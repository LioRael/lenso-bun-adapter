use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    rc::Rc,
    time::Duration as StdDuration,
};

use futures::future::LocalBoxFuture;
use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    ExecutionClassId, PluginInstancePlan,
};
use lenso_auth_sdk::{
    ActorAssertionIssuer, AuthOutcome, CredentialEvidence, Validity, audience,
    authenticate_request, authenticated_response, decode_auth_response,
};
use lenso_bun_adapter::{BunAdapter, BunCapabilityCodec, BunWire};
use lenso_capability_auth::{
    AUTHENTICATE_OPERATION, Auth, AuthEndpoint, AuthError, AuthInvocationError, AuthProvider,
    AuthRequest, AuthResponse, CAPABILITY_ID as AUTH_ID, DESCRIPTOR_VERSION as AUTH_VERSION,
};
use lenso_capability_secure_greeting::{
    CAPABILITY_ID, DESCRIPTOR_VERSION, GREET_OPERATION, GreetError, GreetRequest, GreetResponse,
    SecureGreeting, decode_greet_error, decode_greet_response, encode_greet_request,
};
use lenso_kernel::{
    CancellationToken, DeterministicDriver, ExecutionAdapterCatalog, Kernel, RuntimeFailure,
};
use lenso_native_adapter::{
    NativePluginFactory, NativePluginFactoryContext, NativePluginInstance, NativePluginRegistry,
};
use time::{Duration, OffsetDateTime};

#[derive(Debug)]
struct SecureGreetingCodec;

impl BunCapabilityCodec for SecureGreetingCodec {
    fn capability_id(&self) -> &'static str {
        CAPABILITY_ID
    }
    fn descriptor_version(&self) -> &'static str {
        DESCRIPTOR_VERSION
    }
    fn operations(&self) -> &'static [&'static str] {
        &[GREET_OPERATION]
    }

    fn encode_request(
        &self,
        operation: &str,
        request: &dyn std::any::Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        if operation != GREET_OPERATION {
            return Err(RuntimeFailure::UnknownOperation {
                capability: CAPABILITY_ID,
                operation: operation.to_owned(),
            });
        }
        let request =
            request
                .downcast_ref::<GreetRequest>()
                .ok_or(RuntimeFailure::ProtocolViolation {
                    capability: CAPABILITY_ID,
                })?;
        let wire = encode_greet_request(request).map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to encode secure request: {error}"),
        })?;
        serde_json::from_str(&wire).map_err(|_| RuntimeFailure::ProtocolViolation {
            capability: CAPABILITY_ID,
        })
    }

    fn decode_response(
        &self,
        operation: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn std::any::Any>, RuntimeFailure> {
        if operation != GREET_OPERATION {
            return Err(RuntimeFailure::UnknownOperation {
                capability: CAPABILITY_ID,
                operation: operation.to_owned(),
            });
        }
        let wire = serde_json::to_string(&value).map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to encode secure response: {error}"),
        })?;
        let response =
            decode_greet_response(&wire).map_err(|_| RuntimeFailure::ProtocolViolation {
                capability: CAPABILITY_ID,
            })?;
        Ok(Box::new(response))
    }

    fn decode_domain_error(
        &self,
        operation: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn std::any::Any>, RuntimeFailure> {
        if operation != GREET_OPERATION {
            return Err(RuntimeFailure::UnknownOperation {
                capability: CAPABILITY_ID,
                operation: operation.to_owned(),
            });
        }
        let wire = serde_json::to_string(&value).map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to encode secure Domain Error: {error}"),
        })?;
        let error = decode_greet_error(&wire).map_err(|_| RuntimeFailure::ProtocolViolation {
            capability: CAPABILITY_ID,
        })?;
        Ok(Box::new(error))
    }
}

#[derive(Clone, Debug)]
struct NativeAuthProvider {
    issuer: ActorAssertionIssuer,
}

impl AuthProvider for NativeAuthProvider {
    fn authenticate(
        &self,
        _context: lenso_kernel::InvocationContext,
        request: AuthRequest,
    ) -> LocalBoxFuture<'static, Result<AuthResponse, AuthInvocationError>> {
        let issuer = self.issuer.clone();
        Box::pin(async move {
            let credential = request
                .credential
                .ok_or(AuthInvocationError::Domain(AuthError::Invalid))?;
            if credential.scheme != "bearer" {
                return Err(AuthInvocationError::Domain(AuthError::Unsupported));
            }
            let now = OffsetDateTime::now_utc();
            let (subject, validity) = match credential.value.as_str() {
                "good-token" => (
                    "user-123",
                    Validity::new(now - Duration::seconds(5), now + Duration::minutes(1)),
                ),
                "forbidden-token" => (
                    "forbidden",
                    Validity::new(now - Duration::seconds(5), now + Duration::minutes(1)),
                ),
                "expired-token" => (
                    "user-123",
                    Validity::new(now - Duration::minutes(2), now - Duration::minutes(1)),
                ),
                _ => return Err(AuthInvocationError::Domain(AuthError::Invalid)),
            };
            let assertion = issuer.issue(
                subject,
                "user",
                "fixture",
                [audience(CAPABILITY_ID, GREET_OPERATION)],
                validity.expect("fixture validity is ordered"),
                BTreeMap::new(),
            );
            Ok(authenticated_response(&assertion))
        })
    }
}

#[derive(Debug)]
struct NativeFactory {
    package: &'static str,
    auth: NativeAuthProvider,
}

impl NativePluginFactory for NativeFactory {
    fn package_id(&self) -> &'static str {
        self.package
    }

    fn instantiate(
        &self,
        _context: NativePluginFactoryContext<'_>,
    ) -> Result<NativePluginInstance, RuntimeFailure> {
        let endpoints: Vec<Rc<dyn lenso_kernel::NativeRequestEndpoint>> = match self.package {
            "fixture.auth" => vec![Rc::new(AuthEndpoint::new(self.auth.clone()))],
            "fixture.ingress" => Vec::new(),
            _ => unreachable!("factory package is fixed"),
        };
        Ok(NativePluginInstance::new(endpoints))
    }
}

#[derive(Debug)]
struct HeaderIngressAdapter;

impl HeaderIngressAdapter {
    fn select(&self, authorization: Option<&str>) -> Option<CredentialEvidence> {
        authorization
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(|value| CredentialEvidence::new("bearer", value))
    }
}

fn bun_binary() -> PathBuf {
    std::env::var_os("BUN_BIN").map_or_else(|| PathBuf::from("bun"), PathBuf::from)
}

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/bun/request-provider.ts")
}

fn plan() -> lenso_app_plan::ResolvedAppPlan {
    let ingress = PluginInstancePlan::new("ingress", "fixture.ingress")
        .with_requirement(CapabilityRequirementPlan::one(AUTH_ID, AUTH_VERSION))
        .with_requirement(CapabilityRequirementPlan::one(
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
        ));
    let auth = PluginInstancePlan::new("auth", "fixture.auth").with_capability(
        CapabilityEndpointPlan::new(AUTH_ID, AUTH_VERSION, [AUTHENTICATE_OPERATION]),
    );
    let target = PluginInstancePlan::new("target", "fixture.bun.secure-greeting")
        .with_entrypoint(fixture().to_string_lossy())
        .with_execution_class(ExecutionClassId::bun_child_process())
        .with_capability(CapabilityEndpointPlan::new(
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            [GREET_OPERATION],
        ));
    AppComposition::new(
        vec![ingress, auth, target],
        vec![
            CapabilityBinding::new("ingress", AUTH_ID, AUTH_VERSION, "auth"),
            CapabilityBinding::new("ingress", CAPABILITY_ID, DESCRIPTOR_VERSION, "target"),
        ],
    )
    .resolve()
    .expect("cross-runtime Auth plan resolves")
}

fn run(
    wire: BunWire,
    token: Option<&str>,
) -> Result<Result<GreetResponse, GreetError>, RuntimeFailure> {
    let issuer = ActorAssertionIssuer::new("auth.users", b"shared-auth-key");
    let auth = NativeAuthProvider { issuer };
    let native = NativePluginRegistry::new()
        .with_factory(NativeFactory {
            package: "fixture.ingress",
            auth: auth.clone(),
        })
        .with_factory(NativeFactory {
            package: "fixture.auth",
            auth,
        });
    let bun = BunAdapter::new(bun_binary(), wire).with_codec(SecureGreetingCodec);
    let adapters = ExecutionAdapterCatalog::new()
        .with_adapter(native)
        .and_then(|adapters| adapters.with_adapter(bun))
        .map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to configure adapters: {error:?}"),
        })?;
    let driver = DeterministicDriver::new();
    let app = driver.run(Kernel::start(plan(), driver.clone(), adapters))?;
    let context = if let Some(evidence) = HeaderIngressAdapter.select(token) {
        let response = driver
            .run(app.invoke::<Auth>(
                "ingress",
                AUTHENTICATE_OPERATION,
                authenticate_request(Some(evidence)),
            ))?
            .map_err(|error| RuntimeFailure::Internal {
                detail: format!("credential rejected: {error:?}"),
            })?;
        let AuthOutcome::Authenticated(assertion) =
            decode_auth_response(response).map_err(|error| RuntimeFailure::Internal {
                detail: format!("invalid Auth response: {error:?}"),
            })?
        else {
            return Err(RuntimeFailure::Internal {
                detail: "credential unexpectedly absent".to_owned(),
            });
        };
        Some(
            assertion
                .attach(app.invocation_context(None, CancellationToken::new()))
                .map_err(|error| RuntimeFailure::Internal {
                    detail: format!("failed to attach assertion: {error:?}"),
                })?,
        )
    } else {
        None
    };
    let request = GreetRequest {
        name: "Ada".to_owned(),
    };
    let result = if let Some(context) = context {
        driver.run(app.invoke_with_context::<SecureGreeting>(
            "ingress",
            GREET_OPERATION,
            context,
            request,
        ))
    } else {
        driver.run(app.invoke::<SecureGreeting>("ingress", GREET_OPERATION, request))
    };
    let _ = driver.run(app.shutdown(StdDuration::from_secs(2)));
    result
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn native_auth_and_bun_target_share_one_typed_actor_flow() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        assert_eq!(
            run(wire, Some("Bearer good-token")).expect("cross-runtime invocation succeeds"),
            Ok(GreetResponse {
                message: "Hello from Bun, user-123!".to_owned()
            }),
        );
    }
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn bun_target_returns_declared_actor_authorization_errors() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        assert_eq!(
            run(wire, None).expect("anonymous call reaches target"),
            Err(GreetError::ActorRequired)
        );
        assert_eq!(
            run(wire, Some("Bearer expired-token")).expect("expired call reaches target"),
            Err(GreetError::ActorRequired)
        );
        assert_eq!(
            run(wire, Some("Bearer forbidden-token")).expect("denied call reaches target"),
            Err(GreetError::NotAllowed)
        );
    }
}

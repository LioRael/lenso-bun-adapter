use std::{
    any::Any,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    ExecutionClassId, ModuleInstancePlan,
};
use lenso_bun_adapter::{
    BunAdapter, BunCapabilityCodec, BunProviderDescriptor, BunProviderHandler, BunProviderServer,
    BunRequest, BunResponse, BunWire,
};
use lenso_kernel::{
    CancellationToken, DeterministicDriver, ExecutionAdapterCatalog, InvocationContext, Kernel,
    RequestCapability, RuntimeFailure, SealedInvocationExtension, ShutdownOutcome,
};
use lenso_otel_module::{TraceContext, TraceContextPropagator};
use serde::{Deserialize, Serialize};

const CAPABILITY_ID: &str = "example.trace@1";
const DESCRIPTOR_VERSION: &str = "1.0.0";
const OPERATION: &str = "invoke";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TraceRequest {
    message: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TraceResponse {
    traceparent: String,
    #[serde(default)]
    tracestate: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TraceError {
    message: String,
}

#[derive(Debug)]
struct Trace;

impl RequestCapability for Trace {
    type Request = TraceRequest;
    type Response = TraceResponse;
    type DomainError = TraceError;

    const ID: &'static str = CAPABILITY_ID;
    const DESCRIPTOR_VERSION: &'static str = DESCRIPTOR_VERSION;
}

#[derive(Debug)]
struct TraceCodec;

impl BunCapabilityCodec for TraceCodec {
    fn capability_id(&self) -> &'static str {
        CAPABILITY_ID
    }

    fn descriptor_version(&self) -> &'static str {
        DESCRIPTOR_VERSION
    }

    fn operations(&self) -> &'static [&'static str] {
        &[OPERATION]
    }

    fn encode_request(
        &self,
        operation: &str,
        request: &dyn Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        if operation != OPERATION {
            return Err(RuntimeFailure::UnknownOperation {
                capability: CAPABILITY_ID,
                operation: operation.to_owned(),
            });
        }
        let request =
            request
                .downcast_ref::<TraceRequest>()
                .ok_or(RuntimeFailure::ProtocolViolation {
                    capability: CAPABILITY_ID,
                })?;
        serde_json::to_value(request).map_err(|_| RuntimeFailure::ProtocolViolation {
            capability: CAPABILITY_ID,
        })
    }

    fn decode_response(
        &self,
        operation: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        if operation != OPERATION {
            return Err(RuntimeFailure::UnknownOperation {
                capability: CAPABILITY_ID,
                operation: operation.to_owned(),
            });
        }
        serde_json::from_value::<TraceResponse>(value)
            .map(|response| Box::new(response) as Box<dyn Any>)
            .map_err(|_| RuntimeFailure::ProtocolViolation {
                capability: CAPABILITY_ID,
            })
    }

    fn decode_domain_error(
        &self,
        operation: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        if operation != OPERATION {
            return Err(RuntimeFailure::UnknownOperation {
                capability: CAPABILITY_ID,
                operation: operation.to_owned(),
            });
        }
        serde_json::from_value::<TraceError>(value)
            .map(|error| Box::new(error) as Box<dyn Any>)
            .map_err(|_| RuntimeFailure::ProtocolViolation {
                capability: CAPABILITY_ID,
            })
    }
}

fn bun_binary() -> PathBuf {
    std::env::var_os("BUN_BIN").map_or_else(|| PathBuf::from("bun"), PathBuf::from)
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/bun")
        .join(name)
}

fn plan(script: &Path) -> lenso_app_plan::ResolvedAppPlan {
    let endpoint = CapabilityEndpointPlan::new(CAPABILITY_ID, DESCRIPTOR_VERSION, [OPERATION]);
    let provider = ModuleInstancePlan::new("bun-provider", "fixture.bun.trace")
        .with_entrypoint(script.to_string_lossy())
        .with_execution_class(ExecutionClassId::bun_child_process())
        .with_capability(endpoint);
    let consumer = ModuleInstancePlan::new("bun-consumer", "fixture.bun.trace-consumer")
        .with_entrypoint(script.to_string_lossy())
        .with_execution_class(ExecutionClassId::bun_child_process())
        .with_requirement(CapabilityRequirementPlan::one(
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
        ));
    AppComposition::new(
        vec![provider, consumer],
        vec![CapabilityBinding::new(
            "bun-consumer",
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            "bun-provider",
        )],
    )
    .resolve()
    .expect("Bun OTel trace plan should resolve")
}

fn run(wire: BunWire) -> Result<TraceResponse, RuntimeFailure> {
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire).with_codec(TraceCodec);
    let app = driver.run(Kernel::start(
        plan(&fixture("request-provider.ts")),
        driver.clone(),
        ExecutionAdapterCatalog::single(adapter),
    ))?;
    let propagator = TraceContextPropagator::new("lenso.otel", b"trace-key").map_err(|error| {
        RuntimeFailure::Internal {
            detail: error.to_string(),
        }
    })?;
    let trace = TraceContext::from_traceparent(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        Some("vendor=value"),
    )
    .map_err(|error| RuntimeFailure::Internal {
        detail: error.to_string(),
    })?;
    let context = propagator
        .inject(
            InvocationContext::new(1, None, CancellationToken::new()),
            &trace,
            [format!("{CAPABILITY_ID}:{OPERATION}")],
        )
        .map_err(|error| RuntimeFailure::Internal {
            detail: error.to_string(),
        })?;
    let result = driver.run(app.invoke_with_context::<Trace>(
        "bun-consumer",
        OPERATION,
        context,
        TraceRequest {
            message: "propagate".to_owned(),
        },
    ));
    let shutdown = driver.run(app.shutdown(Duration::from_secs(2)));
    if shutdown != ShutdownOutcome::Clean {
        return Err(RuntimeFailure::Internal {
            detail: format!("Bun trace test shutdown was not clean: {shutdown:?}"),
        });
    }
    result?.map_err(|error| RuntimeFailure::Internal {
        detail: format!("Bun trace provider returned a Domain Error: {error:?}"),
    })
}

#[derive(Debug)]
struct RustTraceProvider {
    propagator: TraceContextPropagator,
}

impl BunProviderHandler for RustTraceProvider {
    fn invoke(&self, request: BunRequest) -> BunResponse {
        let BunRequest {
            request_id,
            capability_id,
            operation,
            payload: _,
            extensions,
            ..
        } = request;
        let Some(extension) = extensions
            .into_iter()
            .find(|extension| extension.key == lenso_otel_module::TRACE_CONTEXT_EXTENSION_KEY)
        else {
            return BunResponse::Runtime(RuntimeFailure::ProtocolViolation {
                capability: CAPABILITY_ID,
            });
        };
        let context = match InvocationContext::new(request_id, None, CancellationToken::new())
            .with_sealed_extension(SealedInvocationExtension::signed(
                extension.key,
                extension.issuer.unwrap_or_default(),
                extension.audience,
                extension.value,
                extension.proof.unwrap_or_default(),
            )) {
            Ok(context) => context,
            Err(_) => {
                return BunResponse::Runtime(RuntimeFailure::ProtocolViolation {
                    capability: CAPABILITY_ID,
                });
            }
        };
        match self
            .propagator
            .extract_for_target(&context, &capability_id, &operation)
        {
            Ok(Some(trace)) => BunResponse::Success(serde_json::json!({
                "traceparent": trace.traceparent(),
                "tracestate": trace.tracestate(),
            })),
            Ok(None) | Err(_) => BunResponse::Runtime(RuntimeFailure::ProtocolViolation {
                capability: CAPABILITY_ID,
            }),
        }
    }
}

fn run_bun_trace_consumer(url: &str) -> serde_json::Value {
    let output = Command::new(bun_binary())
        .arg("run")
        .arg(fixture("otel-trace-consumer.ts"))
        .arg("--")
        .arg("--lenso-url")
        .arg(url)
        .output()
        .expect("Bun trace consumer should start");
    assert!(
        output.status.success(),
        "Bun trace consumer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("Bun trace consumer should emit JSON")
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn bun_and_rust_providers_preserve_and_verify_otel_trace_context() {
    let expected = serde_json::json!({
        "kind": "success",
        "value": {
            "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "tracestate": "vendor=value",
        },
    });
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        let response = run(wire).expect("Bun trace invocation should succeed");
        assert_eq!(
            response,
            TraceResponse {
                traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
                tracestate: Some("vendor=value".to_owned()),
            }
        );
    }

    let server = BunProviderServer::json_rpc(
        BunProviderDescriptor {
            capability_id: CAPABILITY_ID,
            descriptor_version: DESCRIPTOR_VERSION.to_owned(),
            operations: vec![OPERATION.to_owned()],
            stream_operations: Vec::new(),
            event_operations: Vec::new(),
            event_bindings: Vec::new(),
        },
        64 * 1024,
        8,
        RustTraceProvider {
            propagator: TraceContextPropagator::new("lenso.otel", b"trace-key")
                .expect("trace propagator should be configured"),
        },
    )
    .expect("Rust trace provider bridge should start");
    let output = run_bun_trace_consumer(&format!("http://{}", server.address()));
    server.shutdown();
    assert_eq!(output, expected);
}

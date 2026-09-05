use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use lenso_bun_adapter::{BunAuthoringCallback, BunAuthoringHost};
use lenso_kernel::RuntimeFailure;
use lenso_process_protocol::authoring::*;
use serde_json::json;

const STORE_ID: &str = "example.document-store@1";
const SYNC_ID: &str = "example.sync@1";
const VERSION: &str = "1.0.0";
const STORE_DIGEST: &str =
    "sha256:1100000000000000000000000000000000000000000000000000000000000011";
const SYNC_DIGEST: &str = "sha256:2200000000000000000000000000000000000000000000000000000000000022";

#[derive(Debug, Clone)]
struct Callback {
    settlements: Arc<Mutex<Vec<Settlement>>>,
}

impl BunAuthoringCallback for Callback {
    fn call(&self, params: OutboundCallParams) -> Result<OutboundCallResult, RuntimeFailure> {
        assert_eq!(params.requirement_id, "source");
        assert_eq!(params.route_id, "route-0");
        assert_eq!(params.operation, "read");
        Ok(OutboundCallResult {
            session: params.session,
            correlation_id: params.correlation_id,
            outcome: InvocationOutcome::Success {
                value: json!({ "text": "complete object" }),
            },
        })
    }

    fn settled(&self, settlement: Settlement) -> Result<(), RuntimeFailure> {
        self.settlements.lock().unwrap().push(settlement);
        Ok(())
    }
}

#[test]
fn rust_host_and_typescript_child_complete_authenticated_duplex_execution() {
    let settlements = Arc::new(Mutex::new(Vec::new()));
    let initialize = initialization();
    let host = BunAuthoringHost::start(
        bun_binary(),
        fixture("v2-child.ts"),
        initialize.clone(),
        Callback {
            settlements: settlements.clone(),
        },
    )
    .expect("Authoring Host should complete mutual proof");

    let constructed = host
        .construct(ConstructParams {
            session: initialize.identity.session.clone(),
            lifecycle_scope_id: "construct-1".to_owned(),
            remaining_budget_nanos: "10000000000".to_owned(),
        })
        .expect("factory should construct one complete object");
    assert_eq!(constructed.outcome, FactoryOutcome::Constructed);

    let result = host
        .invoke(&InvokeParams {
            session: initialize.identity.session.clone(),
            correlation_id: "40".to_owned(),
            endpoint_id: "endpoint-0".to_owned(),
            capability_id: SYNC_ID.to_owned(),
            descriptor_version: VERSION.to_owned(),
            descriptor_digest: SYNC_DIGEST.to_owned(),
            operation: "sync".to_owned(),
            scope: InvocationScope {
                scope_id: "invoke-40".to_owned(),
                parent_scope_id: None,
                remaining_budget_nanos: "5000000000".to_owned(),
                permissions: Vec::new(),
                extensions: Vec::new(),
            },
            payload: json!({ "document": "guide" }),
        })
        .expect("named dependency callback should complete");
    assert_eq!(
        result.outcome,
        InvocationOutcome::Success {
            value: json!({ "text": "complete object" }),
        }
    );
    assert_eq!(settlements.lock().unwrap().len(), 1);
    assert_eq!(
        settlements.lock().unwrap()[0].state,
        SettlementState::Completed
    );

    let stopped = host
        .stop(StopParams {
            session: initialize.identity.session,
            cleanup_scope_id: "cleanup-1".to_owned(),
            remaining_budget_nanos: "1000000000".to_owned(),
        })
        .expect("child should stop through its reserved control request");
    assert_eq!(stopped.hook, StopHookOutcome::NotDeclared);
}

#[test]
fn cancellation_keeps_noncooperative_execution_admitted_until_host_termination() {
    let settlements = Arc::new(Mutex::new(Vec::new()));
    let initialize = blocking_initialization();
    let host = Arc::new(
        BunAuthoringHost::start(
            bun_binary(),
            fixture("v2-blocking-child.ts"),
            initialize.clone(),
            Callback {
                settlements: settlements.clone(),
            },
        )
        .expect("blocking child should initialize"),
    );
    host.construct(ConstructParams {
        session: initialize.identity.session.clone(),
        lifecycle_scope_id: "construct-1".to_owned(),
        remaining_budget_nanos: "10000000000".to_owned(),
    })
    .expect("blocking child should construct");

    let first_host = host.clone();
    let first = blocking_invocation(&initialize, "50");
    let worker = thread::spawn(move || first_host.invoke(&first));
    thread::sleep(Duration::from_millis(30));
    let cancellation = host
        .cancel(CancelParams {
            session: initialize.identity.session.clone(),
            scope_id: "invoke-50".to_owned(),
            correlation_id: "50".to_owned(),
            reason: "test cancellation".to_owned(),
        })
        .expect("control request should progress beside the blocked invocation");
    assert!(cancellation.accepted);

    let second = host
        .invoke(&blocking_invocation(&initialize, "51"))
        .expect("capacity rejection should be a structured outcome");
    assert!(matches!(
        second.outcome,
        InvocationOutcome::Runtime {
            failure: lenso_process_protocol::authoring::RuntimeFailure::ResourceExhausted { .. }
        }
    ));
    assert_eq!(settlements.lock().unwrap().len(), 1);

    host.terminate();
    assert!(worker.join().unwrap().is_err());
}

fn initialization() -> InitializeParams {
    InitializeParams {
        api_version: AUTHORING_API_VERSION,
        identity: SessionIdentity {
            session: URL_SAFE_NO_PAD.encode([3_u8; 32]),
            plugin_instance: "sync".to_owned(),
            plugin_generation: "1".to_owned(),
            artifact_digest: format!("sha256:{}", "a".repeat(64)),
            contract_digest: format!("sha256:{}", "b".repeat(64)),
            runtime_profile: "lenso.bun-authoring@2".to_owned(),
            value_profile: "lenso-json-value-v1".to_owned(),
        },
        config: json!({}),
        required_declarations: vec![RequirementDeclaration {
            requirement_id: "source".to_owned(),
            capability_id: STORE_ID.to_owned(),
            descriptor_version: VERSION.to_owned(),
            descriptor_digest: STORE_DIGEST.to_owned(),
            cardinality: RequirementCardinality::One,
        }],
        routes: vec![RouteDescriptor {
            route_id: "route-0".to_owned(),
            requirement_id: "source".to_owned(),
            capability_id: STORE_ID.to_owned(),
            descriptor_version: VERSION.to_owned(),
            descriptor_digest: STORE_DIGEST.to_owned(),
            provider_instance: "store".to_owned(),
            provider_order: 0,
        }],
        provided_endpoints: vec![ProvidedEndpoint {
            endpoint_id: "endpoint-0".to_owned(),
            capability_id: SYNC_ID.to_owned(),
            descriptor_version: VERSION.to_owned(),
            descriptor_digest: SYNC_DIGEST.to_owned(),
        }],
        limits: AuthoringLimits {
            max_frame_bytes: 1_048_576,
            max_active_invocations: 4,
            max_active_outbound_calls: 4,
            max_queued_calls: 4,
            max_unfinished_executions: 4,
            max_retired_ids: 16,
        },
    }
}

fn blocking_initialization() -> InitializeParams {
    let mut initialize = initialization();
    "blocked".clone_into(&mut initialize.identity.plugin_instance);
    initialize.required_declarations.clear();
    initialize.routes.clear();
    initialize.provided_endpoints = vec![ProvidedEndpoint {
        endpoint_id: "endpoint-0".to_owned(),
        capability_id: "example.blocking@1".to_owned(),
        descriptor_version: VERSION.to_owned(),
        descriptor_digest:
            "sha256:3300000000000000000000000000000000000000000000000000000000000033".to_owned(),
    }];
    initialize.limits.max_active_invocations = 1;
    initialize
}

fn blocking_invocation(initialize: &InitializeParams, correlation_id: &str) -> InvokeParams {
    let endpoint = &initialize.provided_endpoints[0];
    InvokeParams {
        session: initialize.identity.session.clone(),
        correlation_id: correlation_id.to_owned(),
        endpoint_id: endpoint.endpoint_id.clone(),
        capability_id: endpoint.capability_id.clone(),
        descriptor_version: endpoint.descriptor_version.clone(),
        descriptor_digest: endpoint.descriptor_digest.clone(),
        operation: "block".to_owned(),
        scope: InvocationScope {
            scope_id: format!("invoke-{correlation_id}"),
            parent_scope_id: None,
            remaining_budget_nanos: "5000000000".to_owned(),
            permissions: Vec::new(),
            extensions: Vec::new(),
        },
        payload: json!({}),
    }
}

fn bun_binary() -> PathBuf {
    std::env::var_os("BUN_BIN").map_or_else(|| PathBuf::from("bun"), PathBuf::from)
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/lenso-bun-plugin/test/fixtures")
        .join(name)
}

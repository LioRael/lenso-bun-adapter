use std::{
    any::Any,
    fs,
    path::{Path, PathBuf},
    process::Command,
    rc::Rc,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    ExecutionClassId, PluginInstancePlan,
};
use lenso_bun_adapter::{
    BUN_AUTHORING_RUNTIME_PROFILE, BunAdapter, BunAuthoringCallback, BunAuthoringHost,
};
use lenso_kernel::{
    DeterministicDriver, EventAdmission, EventCapability, ExecutionAdapterCatalog,
    InvocationContext, Kernel, NativeRequestEndpoint, PluginDependencyHandle, RequestCapability,
    RuntimeFailure, StreamCapability, StreamEvent,
};
use lenso_native_adapter::{
    NativePluginFactory, NativePluginFactoryContext, NativePluginInstance, NativePluginRegistry,
};
use lenso_process_protocol::authoring::*;
use lenso_runtime_codec::{
    ArtifactCatalog, ArtifactHandle, JsonCapabilityCodec, JsonHostRequestFuture,
    JsonInvocationOutcome,
};
use serde_json::json;
use sha2::{Digest as _, Sha256};

const STORE_ID: &str = "example.document-store@1";
const SYNC_ID: &str = "example.sync@1";
const VERSION: &str = "1.0.0";
const STORE_DIGEST: &str =
    "sha256:1100000000000000000000000000000000000000000000000000000000000011";
const SYNC_DIGEST: &str = "sha256:2200000000000000000000000000000000000000000000000000000000000022";
const ECHO_ID: &str = "example.echo@1";
const ECHO_DIGEST: &str = "sha256:4400000000000000000000000000000000000000000000000000000000000044";
const CHANNEL_ID: &str = "example.channel@1";
const CHANNEL_DIGEST: &str =
    "sha256:5500000000000000000000000000000000000000000000000000000000000055";

#[derive(Debug)]
struct Echo;

impl RequestCapability for Echo {
    type Request = serde_json::Value;
    type Response = serde_json::Value;
    type DomainError = serde_json::Value;

    const ID: &'static str = ECHO_ID;
    const DESCRIPTOR_VERSION: &'static str = VERSION;
}

#[derive(Debug)]
struct EchoCodec;

impl JsonCapabilityCodec for EchoCodec {
    fn capability_id(&self) -> &'static str {
        ECHO_ID
    }

    fn descriptor_version(&self) -> &'static str {
        VERSION
    }

    fn descriptor_digest(&self) -> &'static str {
        ECHO_DIGEST
    }

    fn request_operations(&self) -> &'static [&'static str] {
        &["echo"]
    }

    fn encode_request(
        &self,
        _: &str,
        request: &dyn Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        request.downcast_ref::<serde_json::Value>().cloned().ok_or(
            RuntimeFailure::ProtocolViolation {
                capability: ECHO_ID,
            },
        )
    }

    fn decode_response(
        &self,
        _: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        Ok(Box::new(value))
    }

    fn decode_domain_error(
        &self,
        _: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        Ok(Box::new(value))
    }
}

#[derive(Debug)]
struct ChannelStream;

impl StreamCapability for ChannelStream {
    type OpenRequest = serde_json::Value;
    type Message = serde_json::Value;
    type DomainError = serde_json::Value;

    const ID: &'static str = CHANNEL_ID;
    const DESCRIPTOR_VERSION: &'static str = VERSION;
}

#[derive(Debug)]
struct ChannelEvent;

impl EventCapability for ChannelEvent {
    type Event = serde_json::Value;

    const ID: &'static str = CHANNEL_ID;
    const DESCRIPTOR_VERSION: &'static str = VERSION;
}

#[derive(Debug)]
struct ChannelCodec;

impl JsonCapabilityCodec for ChannelCodec {
    fn capability_id(&self) -> &'static str {
        CHANNEL_ID
    }
    fn descriptor_version(&self) -> &'static str {
        VERSION
    }
    fn descriptor_digest(&self) -> &'static str {
        CHANNEL_DIGEST
    }
    fn request_operations(&self) -> &'static [&'static str] {
        &[]
    }
    fn stream_operations(&self) -> &'static [&'static str] {
        &["chat"]
    }
    fn event_operations(&self) -> &'static [&'static str] {
        &["notify"]
    }

    fn encode_request(
        &self,
        operation: &str,
        _: &dyn Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        Err(RuntimeFailure::UnknownOperation {
            capability: CHANNEL_ID,
            operation: operation.to_owned(),
        })
    }

    fn decode_response(
        &self,
        operation: &str,
        _: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        Err(RuntimeFailure::UnknownOperation {
            capability: CHANNEL_ID,
            operation: operation.to_owned(),
        })
    }

    fn decode_domain_error(
        &self,
        _: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        Ok(Box::new(value))
    }

    fn encode_event(&self, _: &str, event: &dyn Any) -> Result<serde_json::Value, RuntimeFailure> {
        event.downcast_ref::<serde_json::Value>().cloned().ok_or(
            RuntimeFailure::ProtocolViolation {
                capability: CHANNEL_ID,
            },
        )
    }

    fn encode_stream_open(
        &self,
        _: &str,
        request: &dyn Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        request.downcast_ref::<serde_json::Value>().cloned().ok_or(
            RuntimeFailure::ProtocolViolation {
                capability: CHANNEL_ID,
            },
        )
    }

    fn encode_stream_message(
        &self,
        _: &str,
        message: &dyn Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        message.downcast_ref::<serde_json::Value>().cloned().ok_or(
            RuntimeFailure::ProtocolViolation {
                capability: CHANNEL_ID,
            },
        )
    }

    fn decode_stream_message(
        &self,
        _: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        Ok(Box::new(value))
    }

    fn decode_stream_domain_error(
        &self,
        _: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        Ok(Box::new(value))
    }
}

#[derive(Debug)]
struct EmptyConsumerFactory;

impl NativePluginFactory for EmptyConsumerFactory {
    fn package_id(&self) -> &'static str {
        "test.echo-consumer"
    }

    fn instantiate(
        &self,
        _context: NativePluginFactoryContext<'_>,
    ) -> Result<NativePluginInstance, RuntimeFailure> {
        Ok(NativePluginInstance::default())
    }
}

#[derive(Debug)]
struct Store;

impl RequestCapability for Store {
    type Request = serde_json::Value;
    type Response = serde_json::Value;
    type DomainError = serde_json::Value;

    const ID: &'static str = STORE_ID;
    const DESCRIPTOR_VERSION: &'static str = VERSION;
}

#[derive(Debug)]
struct StoreEndpoint {
    calls: Arc<AtomicUsize>,
}

impl NativeRequestEndpoint for StoreEndpoint {
    fn capability_id(&self) -> &'static str {
        STORE_ID
    }

    fn descriptor_version(&self) -> &'static str {
        VERSION
    }

    fn operations(&self) -> &'static [&'static str] {
        &["read"]
    }

    fn invoke(
        &self,
        operation: &str,
        request: Box<dyn Any>,
        _context: InvocationContext,
    ) -> futures::future::LocalBoxFuture<
        'static,
        Result<Result<Box<dyn Any>, Box<dyn Any>>, RuntimeFailure>,
    > {
        let calls = self.calls.clone();
        let operation = operation.to_owned();
        Box::pin(async move {
            if operation != "read" {
                return Err(RuntimeFailure::UnknownOperation {
                    capability: STORE_ID,
                    operation,
                });
            }
            let request = request.downcast::<serde_json::Value>().map_err(|_| {
                RuntimeFailure::ProtocolViolation {
                    capability: STORE_ID,
                }
            })?;
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(Ok(Box::new(*request) as Box<dyn Any>))
        })
    }
}

#[derive(Debug)]
struct StoreFactory {
    calls: Arc<AtomicUsize>,
}

impl NativePluginFactory for StoreFactory {
    fn package_id(&self) -> &'static str {
        "test.store"
    }

    fn instantiate(
        &self,
        _context: NativePluginFactoryContext<'_>,
    ) -> Result<NativePluginInstance, RuntimeFailure> {
        Ok(NativePluginInstance::new(vec![Rc::new(StoreEndpoint {
            calls: self.calls.clone(),
        })]))
    }
}

#[derive(Debug)]
struct StoreCodec;

impl JsonCapabilityCodec for StoreCodec {
    fn capability_id(&self) -> &'static str {
        STORE_ID
    }

    fn descriptor_version(&self) -> &'static str {
        VERSION
    }

    fn descriptor_digest(&self) -> &'static str {
        STORE_DIGEST
    }

    fn request_operations(&self) -> &'static [&'static str] {
        &["read"]
    }

    fn encode_request(
        &self,
        _: &str,
        request: &dyn Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        request.downcast_ref::<serde_json::Value>().cloned().ok_or(
            RuntimeFailure::ProtocolViolation {
                capability: STORE_ID,
            },
        )
    }

    fn decode_response(
        &self,
        _: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        Ok(Box::new(value))
    }

    fn decode_domain_error(
        &self,
        _: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        Ok(Box::new(value))
    }

    fn invoke_host_request(
        &self,
        dependency: PluginDependencyHandle,
        operation: String,
        request: serde_json::Value,
        context: InvocationContext,
    ) -> JsonHostRequestFuture {
        Box::pin(async move {
            match dependency
                .typed::<Store>()?
                .invoke_with_context(&operation, context, request)
                .await?
            {
                Ok(value) => Ok(JsonInvocationOutcome::Success(value)),
                Err(error) => Ok(JsonInvocationOutcome::DomainError(error)),
            }
        })
    }
}

#[test]
fn create_and_stop_can_call_named_dependencies() {
    let source = fixture("v2-lifecycle-child.ts");
    let bundle = tempfile::tempdir().unwrap();
    let entrypoint = bundle.path().join("plugin.js");
    assert!(
        Command::new(bun_binary())
            .arg("build")
            .arg("--target")
            .arg("bun")
            .arg("--outfile")
            .arg(&entrypoint)
            .arg(source)
            .status()
            .unwrap()
            .success()
    );
    let bytes = fs::read(&entrypoint).unwrap();
    let digest = format!("sha256:{}", hex::encode(Sha256::digest(&bytes)));
    let artifact = ArtifactHandle::open(&entrypoint, &digest, bytes.len() as u64).unwrap();
    let artifacts = ArtifactCatalog::new()
        .with_artifact("lifecycle", artifact)
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let adapters = ExecutionAdapterCatalog::new()
        .with_adapter(NativePluginRegistry::new().with_factory(StoreFactory {
            calls: calls.clone(),
        }))
        .unwrap()
        .with_adapter(
            BunAdapter::production(bun_binary())
                .with_artifacts(artifacts)
                .with_authoring_codec(StoreCodec),
        )
        .unwrap();
    let plan = AppComposition::new(
        vec![
            PluginInstancePlan::new("store", "test.store")
                .with_capability(CapabilityEndpointPlan::new(STORE_ID, VERSION, ["read"])),
            PluginInstancePlan::new("lifecycle", "test.bun-lifecycle")
                .with_authoring(2, BUN_AUTHORING_RUNTIME_PROFILE)
                .with_entrypoint("plugin")
                .with_execution_class(ExecutionClassId::bun_child_process())
                .with_requirement(
                    CapabilityRequirementPlan::one(STORE_ID, VERSION).with_requirement_id("source"),
                ),
        ],
        vec![
            CapabilityBinding::new("lifecycle", STORE_ID, VERSION, "store")
                .with_requirement_id("source"),
        ],
    )
    .resolve()
    .unwrap();
    let driver = DeterministicDriver::new();
    let app = driver
        .run(Kernel::start(plan, driver.clone(), adapters))
        .expect("create dependency should complete through the lifecycle callback pump");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(matches!(
        driver.run(app.shutdown(Duration::from_secs(1))),
        lenso_kernel::ShutdownOutcome::Clean
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[test]
fn execution_adapter_runs_authoring_v2_through_kernel_lifecycle() {
    let source = fixture("v2-echo-child.ts");
    let bundle = tempfile::tempdir().unwrap();
    let entrypoint = bundle.path().join("plugin.js");
    let build = Command::new(bun_binary())
        .arg("build")
        .arg("--target")
        .arg("bun")
        .arg("--outfile")
        .arg(&entrypoint)
        .arg(source)
        .status()
        .expect("Bun should build the admitted execution artifact");
    assert!(build.success());
    let bytes = fs::read(&entrypoint).unwrap();
    let digest = format!("sha256:{}", hex::encode(Sha256::digest(&bytes)));
    let artifact = ArtifactHandle::open(&entrypoint, &digest, bytes.len() as u64).unwrap();
    let artifacts = ArtifactCatalog::new()
        .with_artifact("echo", artifact)
        .unwrap();
    let adapter = BunAdapter::production(bun_binary())
        .with_artifacts(artifacts)
        .with_authoring_codec(EchoCodec);
    let adapters = ExecutionAdapterCatalog::new()
        .with_adapter(NativePluginRegistry::new().with_factory(EmptyConsumerFactory))
        .unwrap()
        .with_adapter(adapter)
        .unwrap();
    let plan = AppComposition::new(
        vec![
            PluginInstancePlan::new("echo", "test.bun-echo")
                .with_authoring(2, BUN_AUTHORING_RUNTIME_PROFILE)
                .with_entrypoint("plugin")
                .with_execution_class(ExecutionClassId::bun_child_process())
                .with_capability(CapabilityEndpointPlan::new(ECHO_ID, VERSION, ["echo"])),
            PluginInstancePlan::new("consumer", "test.echo-consumer")
                .with_requirement(CapabilityRequirementPlan::one(ECHO_ID, VERSION)),
        ],
        vec![CapabilityBinding::new("consumer", ECHO_ID, VERSION, "echo")],
    )
    .resolve()
    .unwrap();
    let driver = DeterministicDriver::new();
    let app = driver
        .run(Kernel::start(plan, driver.clone(), adapters))
        .expect("Bun Authoring V2 App should start");
    let result = driver
        .run(
            app.handle::<Echo>("consumer")
                .unwrap()
                .invoke("echo", json!({"value": "complete object"})),
        )
        .unwrap()
        .unwrap();

    assert_eq!(result, json!({"value": "complete object"}));
    assert!(matches!(
        driver.run(app.shutdown(Duration::from_secs(1))),
        lenso_kernel::ShutdownOutcome::Clean
    ));
}

#[test]
fn execution_adapter_exposes_authoring_v2_stream_and_event_handles() {
    let source = fixture("v2-stream-event-child.ts");
    let bundle = tempfile::tempdir().unwrap();
    let entrypoint = bundle.path().join("plugin.js");
    assert!(
        Command::new(bun_binary())
            .arg("build")
            .arg("--target")
            .arg("bun")
            .arg("--outfile")
            .arg(&entrypoint)
            .arg(source)
            .status()
            .unwrap()
            .success()
    );
    let bytes = fs::read(&entrypoint).unwrap();
    let digest = format!("sha256:{}", hex::encode(Sha256::digest(&bytes)));
    let artifacts = ArtifactCatalog::new()
        .with_artifact(
            "channel",
            ArtifactHandle::open(&entrypoint, &digest, bytes.len() as u64).unwrap(),
        )
        .unwrap();
    let adapters = ExecutionAdapterCatalog::new()
        .with_adapter(NativePluginRegistry::new().with_factory(EmptyConsumerFactory))
        .unwrap()
        .with_adapter(
            BunAdapter::production(bun_binary())
                .with_artifacts(artifacts)
                .with_authoring_codec(ChannelCodec),
        )
        .unwrap();
    let endpoint = CapabilityEndpointPlan::new(CHANNEL_ID, VERSION, ["chat", "notify"])
        .with_stream_operation("chat")
        .with_event_operation("notify")
        .with_event_capacity(4)
        .with_limits(0, 4);
    let plan = AppComposition::new(
        vec![
            PluginInstancePlan::new("channel", "test.bun-channel")
                .with_authoring(2, BUN_AUTHORING_RUNTIME_PROFILE)
                .with_entrypoint("plugin")
                .with_execution_class(ExecutionClassId::bun_child_process())
                .with_capability(endpoint),
            PluginInstancePlan::new("consumer", "test.echo-consumer")
                .with_requirement(CapabilityRequirementPlan::one(CHANNEL_ID, VERSION)),
        ],
        vec![CapabilityBinding::new(
            "consumer", CHANNEL_ID, VERSION, "channel",
        )],
    )
    .resolve()
    .unwrap();
    let driver = DeterministicDriver::new();
    let app = driver
        .run(Kernel::start(plan, driver.clone(), adapters))
        .unwrap();

    let events = driver.run(
        app.event_handle::<ChannelEvent>("consumer")
            .unwrap()
            .publish("notify", json!({"message": "ready"})),
    );
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].admission(), EventAdmission::Accepted);

    let stream = driver
        .run(
            app.stream_handle::<ChannelStream>("consumer")
                .unwrap()
                .open("chat", json!({"room": "general"})),
        )
        .unwrap()
        .unwrap();
    driver.run(stream.send(json!({"text": "hello"}))).unwrap();
    assert!(matches!(
        driver.run(stream.receive()),
        Ok(StreamEvent::Message(message)) if message == json!({"text": "hello"})
    ));
    driver.run(stream.close_send()).unwrap();
    assert!(matches!(
        driver.run(stream.receive()),
        Ok(StreamEvent::Terminal(Ok(())))
    ));
    assert!(matches!(
        driver.run(app.shutdown(Duration::from_secs(1))),
        lenso_kernel::ShutdownOutcome::Clean
    ));
}

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

#[derive(Debug, Clone)]
struct BlockingCallback {
    entered: Arc<(Mutex<bool>, Condvar)>,
    release: Arc<(Mutex<bool>, Condvar)>,
    settlements: Arc<Mutex<Vec<Settlement>>>,
}

impl BunAuthoringCallback for BlockingCallback {
    fn call(&self, params: OutboundCallParams) -> Result<OutboundCallResult, RuntimeFailure> {
        {
            let (entered, changed) = &*self.entered;
            *entered.lock().unwrap() = true;
            changed.notify_all();
        }
        let (released, changed) = &*self.release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        Ok(OutboundCallResult {
            session: params.session,
            correlation_id: params.correlation_id,
            outcome: InvocationOutcome::Success {
                value: json!({ "text": "released" }),
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
fn rust_host_drives_typescript_event_and_stream_providers() {
    let settlements = Arc::new(Mutex::new(Vec::new()));
    let initialize = stream_event_initialization();
    let host = BunAuthoringHost::start(
        bun_binary(),
        fixture("v2-stream-event-child.ts"),
        initialize.clone(),
        Callback {
            settlements: settlements.clone(),
        },
    )
    .expect("Stream/Event child should authenticate");
    host.construct(ConstructParams {
        session: initialize.identity.session.clone(),
        lifecycle_scope_id: "construct-1".to_owned(),
        remaining_budget_nanos: "10000000000".to_owned(),
    })
    .expect("Stream/Event child should construct");

    let event = EventPublishParams {
        session: initialize.identity.session.clone(),
        correlation_id: "60".to_owned(),
        endpoint_id: "endpoint-0".to_owned(),
        capability_id: CHANNEL_ID.to_owned(),
        descriptor_version: VERSION.to_owned(),
        descriptor_digest: CHANNEL_DIGEST.to_owned(),
        operation: "notify".to_owned(),
        scope: test_scope("event-60"),
        event: json!({"message": "ready"}),
    };
    assert_eq!(
        host.publish_event(&event).unwrap().outcome,
        EventPublishOutcome::Accepted
    );

    let open = StreamOpenParams {
        session: initialize.identity.session.clone(),
        correlation_id: "61".to_owned(),
        endpoint_id: "endpoint-0".to_owned(),
        capability_id: CHANNEL_ID.to_owned(),
        descriptor_version: VERSION.to_owned(),
        descriptor_digest: CHANNEL_DIGEST.to_owned(),
        operation: "chat".to_owned(),
        scope: test_scope("stream-61"),
        request: json!({"room": "general"}),
    };
    let stream_id = match host.open_stream(&open).unwrap().outcome {
        StreamOpenOutcome::Opened { stream_id } => stream_id,
        outcome => panic!("expected opened Stream, got {outcome:?}"),
    };
    assert_eq!(settlements.lock().unwrap().len(), 1);
    let send = StreamSendParams {
        session: initialize.identity.session.clone(),
        correlation_id: "62".to_owned(),
        stream_id: stream_id.clone(),
        sequence: "0".to_owned(),
        message: json!({"text": "hello"}),
    };
    assert_eq!(
        host.send_stream(&send).unwrap().outcome,
        StreamActionOutcome::Accepted
    );
    let receive = StreamReceiveParams {
        session: initialize.identity.session.clone(),
        correlation_id: "63".to_owned(),
        stream_id: stream_id.clone(),
    };
    assert!(matches!(
        host.receive_stream(&receive).unwrap().outcome,
        StreamReceiveOutcome::Message { sequence, message }
            if sequence == "0" && message == json!({"text": "hello"})
    ));
    let close = StreamReceiveParams {
        correlation_id: "64".to_owned(),
        ..receive
    };
    assert_eq!(
        host.close_stream_send(&close).unwrap().outcome,
        StreamActionOutcome::Accepted
    );
    let terminal = StreamReceiveParams {
        correlation_id: "65".to_owned(),
        ..close
    };
    assert!(matches!(
        host.receive_stream(&terminal).unwrap().outcome,
        StreamReceiveOutcome::Terminal {
            outcome: StreamTerminalOutcome::Success
        }
    ));
    assert_eq!(
        host.stop(StopParams {
            session: initialize.identity.session,
            cleanup_scope_id: "cleanup-1".to_owned(),
            remaining_budget_nanos: "1000000000".to_owned(),
        })
        .unwrap()
        .hook,
        StopHookOutcome::NotDeclared
    );
}

#[test]
fn settlement_progresses_while_an_unrelated_outbound_callback_is_blocked() {
    let entered = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let settlements = Arc::new(Mutex::new(Vec::new()));
    let initialize = initialization();
    let host = Arc::new(
        BunAuthoringHost::start(
            bun_binary(),
            fixture("v2-child.ts"),
            initialize.clone(),
            BlockingCallback {
                entered: entered.clone(),
                release: release.clone(),
                settlements: settlements.clone(),
            },
        )
        .expect("Authoring Host should complete mutual proof"),
    );
    host.construct(ConstructParams {
        session: initialize.identity.session.clone(),
        lifecycle_scope_id: "construct-1".to_owned(),
        remaining_budget_nanos: "10000000000".to_owned(),
    })
    .expect("child should construct");

    let first_host = host.clone();
    let first_params = invocation(&initialize, "50");
    let first = thread::spawn(move || first_host.invoke(&first_params));
    {
        let (entered, changed) = &*entered;
        let mut entered = entered.lock().unwrap();
        while !*entered {
            entered = changed.wait(entered).unwrap();
        }
    }

    let second = host
        .invoke(&invocation(&initialize, "51"))
        .expect("the overlapping invocation should settle independently");
    assert_eq!(
        second.outcome,
        InvocationOutcome::Domain {
            error: json!("already_running")
        }
    );
    {
        let (released, changed) = &*release;
        *released.lock().unwrap() = true;
        changed.notify_all();
    }
    assert!(first.join().unwrap().is_ok());
    assert_eq!(settlements.lock().unwrap().len(), 2);
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

fn stream_event_initialization() -> InitializeParams {
    let mut initialize = initialization();
    "channel".clone_into(&mut initialize.identity.plugin_instance);
    initialize.required_declarations.clear();
    initialize.routes.clear();
    initialize.provided_endpoints = vec![ProvidedEndpoint {
        endpoint_id: "endpoint-0".to_owned(),
        capability_id: CHANNEL_ID.to_owned(),
        descriptor_version: VERSION.to_owned(),
        descriptor_digest: CHANNEL_DIGEST.to_owned(),
    }];
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

fn invocation(initialize: &InitializeParams, correlation_id: &str) -> InvokeParams {
    let endpoint = &initialize.provided_endpoints[0];
    InvokeParams {
        session: initialize.identity.session.clone(),
        correlation_id: correlation_id.to_owned(),
        endpoint_id: endpoint.endpoint_id.clone(),
        capability_id: endpoint.capability_id.clone(),
        descriptor_version: endpoint.descriptor_version.clone(),
        descriptor_digest: endpoint.descriptor_digest.clone(),
        operation: "sync".to_owned(),
        scope: InvocationScope {
            scope_id: format!("invoke-{correlation_id}"),
            parent_scope_id: None,
            remaining_budget_nanos: "5000000000".to_owned(),
            permissions: Vec::new(),
            extensions: Vec::new(),
        },
        payload: json!({ "document": "guide" }),
    }
}

fn test_scope(scope_id: &str) -> InvocationScope {
    InvocationScope {
        scope_id: scope_id.to_owned(),
        parent_scope_id: None,
        remaining_budget_nanos: "5000000000".to_owned(),
        permissions: Vec::new(),
        extensions: Vec::new(),
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

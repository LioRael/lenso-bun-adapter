use std::{
    any::Any,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use futures::{StreamExt, stream::FuturesUnordered};
use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    ExecutionClassId, ModuleInstancePlan, RestartPolicy,
};
use lenso_capability_greeting::{
    CAPABILITY_ID, DESCRIPTOR_VERSION, GreetError, GreetRequest, GreetResponse, Greeting,
    decode_greet_error, decode_greet_response, encode_greet_request,
};
use lenso_kernel::{
    CancellationToken, DeterministicDriver, ExecutionAdapterCatalog, Kernel, RuntimeDriver,
    RuntimeFailure,
};

use lenso_bun_adapter::{
    BunAdapter, BunAdapterConfig, BunCapabilityCodec, BunProviderDescriptor, BunProviderHandler,
    BunProviderServer, BunRequest, BunResponse, BunWire,
};

#[derive(Debug)]
struct GreetingCodec;

impl BunCapabilityCodec for GreetingCodec {
    fn capability_id(&self) -> &'static str {
        CAPABILITY_ID
    }

    fn descriptor_version(&self) -> &'static str {
        DESCRIPTOR_VERSION
    }

    fn operations(&self) -> &'static [&'static str] {
        &["greet"]
    }

    fn encode_request(
        &self,
        operation: &str,
        request: &dyn Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        if operation != "greet" {
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
        let encoded = encode_greet_request(request).map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to encode Greeting request: {error}"),
        })?;
        serde_json::from_str(&encoded).map_err(|_| RuntimeFailure::ProtocolViolation {
            capability: CAPABILITY_ID,
        })
    }

    fn decode_response(
        &self,
        operation: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        if operation != "greet" {
            return Err(RuntimeFailure::UnknownOperation {
                capability: CAPABILITY_ID,
                operation: operation.to_owned(),
            });
        }
        let wire = serde_json::to_string(&value).map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to encode Greeting response value: {error}"),
        })?;
        Ok(Box::new(decode_greet_response(&wire).map_err(|_| {
            RuntimeFailure::ProtocolViolation {
                capability: CAPABILITY_ID,
            }
        })?))
    }

    fn decode_domain_error(
        &self,
        operation: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        if operation != "greet" {
            return Err(RuntimeFailure::UnknownOperation {
                capability: CAPABILITY_ID,
                operation: operation.to_owned(),
            });
        }
        let wire = serde_json::to_string(&value).map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to encode Greeting Domain Error value: {error}"),
        })?;
        Ok(Box::new(decode_greet_error(&wire).map_err(|_| {
            RuntimeFailure::ProtocolViolation {
                capability: CAPABILITY_ID,
            }
        })?))
    }
}

#[derive(Debug)]
struct RustGreetingProvider;

impl BunProviderHandler for RustGreetingProvider {
    fn invoke(&self, request: BunRequest) -> BunResponse {
        if request.deadline_nanos == Some(0) {
            return BunResponse::Runtime(RuntimeFailure::DeadlineExceeded {
                request_id: request.request_id,
            });
        }
        let request: GreetRequest = match serde_json::from_value(request.payload) {
            Ok(request) => request,
            Err(_) => {
                return BunResponse::Runtime(RuntimeFailure::ProtocolViolation {
                    capability: CAPABILITY_ID,
                });
            }
        };
        let requested_failure = match request.name.as_str() {
            "__runtime_unavailable__" => Some(RuntimeFailure::Unavailable {
                capability: CAPABILITY_ID,
            }),
            "__runtime_ambiguous_binding__" => Some(RuntimeFailure::AmbiguousBinding {
                capability: CAPABILITY_ID,
                providers: 2,
            }),
            "__runtime_missing_module_factory__" => Some(RuntimeFailure::MissingModuleFactory {
                instance: "provider".to_owned(),
                package_id: "fixture.provider".to_owned(),
            }),
            "__runtime_unavailable_execution_class__" => {
                Some(RuntimeFailure::UnavailableExecutionClass {
                    instance_key: "provider".to_owned(),
                    execution_class: "fixture.missing@1".to_owned(),
                })
            }
            "__runtime_invalid_resolved_plan__" => Some(RuntimeFailure::InvalidResolvedPlan {
                detail: "invalid".to_owned(),
            }),
            "__runtime_admission_closed__" => Some(RuntimeFailure::AdmissionClosed),
            "__runtime_internal__" => Some(RuntimeFailure::Internal {
                detail: "internal".to_owned(),
            }),
            "__runtime_module_restart_exhausted__" => {
                Some(RuntimeFailure::ModuleRestartExhausted {
                    instance: "provider".to_owned(),
                    attempts: 3,
                })
            }
            _ => None,
        };
        if let Some(failure) = requested_failure {
            return BunResponse::Runtime(failure);
        }
        if request.name.is_empty() {
            return BunResponse::Domain(serde_json::json!("empty_name"));
        }
        if request.name == "__future_domain__" {
            return BunResponse::Domain(serde_json::json!({
                "code": "future_variant",
                "payload": { "retry_after_ms": 2500 }
            }));
        }
        if request.name == "__crash__" {
            return BunResponse::Runtime(RuntimeFailure::ModuleFailure {
                detail: "Rust provider generation failed".to_owned(),
            });
        }
        if request.name == "__delay__" {
            std::thread::sleep(Duration::from_millis(250));
        }
        BunResponse::Success(
            serde_json::to_value(GreetResponse {
                message: format!("Hello from Rust, {}!", request.name),
            })
            .expect("Greeting response should serialize"),
        )
    }
}

#[derive(Debug)]
struct CancellableRustGreetingProvider;

impl BunProviderHandler for CancellableRustGreetingProvider {
    fn invoke(&self, request: BunRequest) -> BunResponse {
        for _ in 0..200 {
            if request.is_cancelled() {
                return BunResponse::Runtime(RuntimeFailure::Cancelled {
                    request_id: request.request_id,
                });
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        BunResponse::Runtime(RuntimeFailure::Internal {
            detail: "cancellation was not delivered".to_owned(),
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

fn greeting_plan(script: &Path) -> lenso_app_plan::ResolvedAppPlan {
    let endpoint =
        CapabilityEndpointPlan::new(CAPABILITY_ID, DESCRIPTOR_VERSION, ["greet"]).with_limits(1, 1);
    let provider = ModuleInstancePlan::new("bun-provider", "fixture.bun.greeting")
        .with_entrypoint(script.to_string_lossy())
        .with_execution_class(ExecutionClassId::bun_child_process())
        .with_restart_policy(RestartPolicy::on_failure(
            2,
            Duration::from_secs(5),
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        ))
        .with_capability(endpoint);
    let consumer = ModuleInstancePlan::new("bun-consumer", "fixture.bun.consumer")
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
    .expect("Bun cross-runtime plan should resolve")
}

fn run_greeting(
    wire: BunWire,
    name: &str,
) -> Result<Result<GreetResponse, GreetError>, RuntimeFailure> {
    let script = fixture("request-provider.ts");
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire).with_codec(GreetingCodec);
    let app = driver.run(Kernel::start(
        greeting_plan(&script),
        driver.clone(),
        ExecutionAdapterCatalog::single(adapter),
    ))?;
    let result = driver.run(app.invoke::<Greeting>(
        "bun-consumer",
        "greet",
        GreetRequest {
            name: name.to_owned(),
        },
    ));
    let _ = driver.run(app.shutdown(Duration::from_secs(2)));
    result
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn framed_stdio_matches_the_typed_request_contract() {
    let result = run_greeting(BunWire::FramedStdio, "Ada").expect("framed call should succeed");
    assert_eq!(
        result.expect("framed call should not return a Domain Error"),
        GreetResponse {
            message: "Hello from Bun, Ada!".to_owned(),
        }
    );
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn json_rpc_matches_the_typed_request_contract() {
    let result = run_greeting(BunWire::JsonRpcHttp, "Ada").expect("JSON-RPC call should succeed");
    assert_eq!(
        result.expect("JSON-RPC call should not return a Domain Error"),
        GreetResponse {
            message: "Hello from Bun, Ada!".to_owned(),
        }
    );
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn selected_wire_preserves_domain_errors() {
    let result = run_greeting(BunWire::JsonRpcHttp, "").expect("call should reach Bun");
    assert_eq!(result, Err(GreetError::EmptyName));
}

#[derive(Debug, serde::Deserialize)]
struct CorpusCase {
    name: String,
    #[serde(default = "default_corpus_scenario")]
    scenario: String,
    request: CorpusRequest,
    outcome: String,
    #[serde(default)]
    failure_kind: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct CorpusRequest {
    operation: String,
    payload: serde_json::Value,
}

fn default_corpus_scenario() -> String {
    "request".to_owned()
}

fn assert_shared_corpus_contract(corpus: &[CorpusCase]) {
    for (scenario, failure_kind) in [
        ("deadline", "deadline_exceeded"),
        ("cancellation", "cancelled"),
        ("size-boundary", "protocol_violation"),
        ("process-failure", "module_failure"),
        ("overload", "resource_exhausted"),
    ] {
        assert!(
            corpus.iter().any(|case| {
                case.scenario == scenario && case.failure_kind.as_deref() == Some(failure_kind)
            }),
            "shared corpus is missing {scenario}/{failure_kind}"
        );
    }
}

fn run_shared_corpus(wire: BunWire) {
    let corpus: Vec<CorpusCase> = serde_json::from_str(include_str!(
        "../../../fixtures/bun/request-conformance.json"
    ))
    .expect("shared Bun request corpus should decode");
    assert_shared_corpus_contract(&corpus);
    let script = fixture("request-provider.ts");
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire).with_codec(GreetingCodec);
    let app = driver
        .run(Kernel::start(
            greeting_plan(&script),
            driver.clone(),
            ExecutionAdapterCatalog::single(adapter),
        ))
        .expect("Bun App should start");
    for case in corpus.iter().filter(|case| case.scenario == "request") {
        let name = case
            .request
            .payload
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("Ada")
            .to_owned();
        let result = driver.run(app.invoke::<Greeting>(
            "bun-consumer",
            &case.request.operation,
            GreetRequest { name },
        ));
        match case.outcome.as_str() {
            "success" => assert!(
                matches!(result, Ok(Ok(_))),
                "case {}: {result:?}",
                case.name
            ),
            "domain" => assert!(
                matches!(result, Ok(Err(_))),
                "case {}: {result:?}",
                case.name
            ),
            "runtime" => {
                let error = result.as_ref().expect_err(&format!(
                    "case {} should return a Runtime Failure",
                    case.name
                ));
                if let Some(expected) = case.failure_kind.as_deref() {
                    assert_eq!(runtime_failure_kind(error), expected, "case {}", case.name);
                }
            }
            other => panic!("case {} has unknown outcome {other}", case.name),
        }
    }
    let _ = driver.run(app.shutdown(Duration::from_secs(2)));
    for case in corpus.iter().filter(|case| case.scenario != "request") {
        match case.scenario.as_str() {
            "deadline" => assert_deadline_is_enforced_without_replay(wire),
            "cancellation" => assert_dropping_call_cancels(wire),
            "size-boundary" => assert_oversized_request_is_rejected(wire),
            "process-failure" => assert_provider_exit_recreates_generation(wire),
            "overload" => assert_bounded_wire_admission(wire),
            other => panic!("case {} has unknown scenario {other}", case.name),
        }
    }
}

fn runtime_failure_kind(failure: &RuntimeFailure) -> &'static str {
    match failure {
        RuntimeFailure::Unavailable { .. } => "unavailable",
        RuntimeFailure::UnknownOperation { .. } => "unknown_operation",
        RuntimeFailure::AmbiguousBinding { .. } => "ambiguous_binding",
        RuntimeFailure::ProtocolViolation { .. } => "protocol_violation",
        RuntimeFailure::MissingModuleFactory { .. } => "missing_module_factory",
        RuntimeFailure::UnavailableExecutionClass { .. } => "unavailable_execution_class",
        RuntimeFailure::InvalidResolvedPlan { .. } => "invalid_resolved_plan",
        RuntimeFailure::AdmissionClosed => "admission_closed",
        RuntimeFailure::ResourceExhausted { .. } => "resource_exhausted",
        RuntimeFailure::DeadlineExceeded { .. } => "deadline_exceeded",
        RuntimeFailure::Cancelled { .. } => "cancelled",
        RuntimeFailure::Internal { .. } => "internal",
        RuntimeFailure::ModuleFailure { .. } => "module_failure",
        RuntimeFailure::ModuleRestartExhausted { .. } => "module_restart_exhausted",
    }
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn shared_request_corpus_has_the_same_outcomes_on_both_wires() {
    run_shared_corpus(BunWire::FramedStdio);
    run_shared_corpus(BunWire::JsonRpcHttp);
}

fn run_bun_consumer(url: &str, name: &str, operation: &str) -> serde_json::Value {
    run_bun_consumer_with_args(url, name, operation, &[])
}

fn run_bun_consumer_with_args(
    url: &str,
    name: &str,
    operation: &str,
    extra_args: &[&str],
) -> serde_json::Value {
    let mut command = Command::new(bun_binary());
    command
        .arg("run")
        .arg(fixture("request-consumer.ts"))
        .arg("--")
        .arg("--lenso-url")
        .arg(url)
        .arg("--name")
        .arg(name)
        .arg("--operation")
        .arg(operation)
        .args(extra_args);
    let output = command.output().expect("Bun consumer should start");
    assert!(
        output.status.success(),
        "Bun consumer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("Bun consumer should emit JSON")
}

fn run_bun_consumer_with_cancel(url: &str, name: &str, cancel_after_ms: u64) -> serde_json::Value {
    let output = Command::new(bun_binary())
        .arg("run")
        .arg(fixture("request-consumer.ts"))
        .arg("--")
        .arg("--lenso-url")
        .arg(url)
        .arg("--name")
        .arg(name)
        .arg("--cancel-after-ms")
        .arg(cancel_after_ms.to_string())
        .output()
        .expect("Bun consumer should start");
    assert!(
        output.status.success(),
        "Bun consumer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("Bun consumer should emit JSON")
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn bun_consumer_can_call_a_rust_provider_bridge() {
    let server = BunProviderServer::json_rpc(
        BunProviderDescriptor {
            capability_id: CAPABILITY_ID,
            descriptor_version: DESCRIPTOR_VERSION.to_owned(),
            operations: vec!["greet".to_owned()],
        },
        64 * 1024,
        8,
        RustGreetingProvider,
    )
    .expect("Rust provider bridge should start");
    let url = format!("http://{}", server.address());
    let output = run_bun_consumer(&url, "Ada", "greet");
    server.shutdown();
    assert_eq!(
        output,
        serde_json::json!({
            "kind": "success",
            "value": { "message": "Hello from Rust, Ada!" }
        })
    );
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn shared_request_corpus_has_the_same_outcomes_for_a_bun_consumer() {
    let server = BunProviderServer::json_rpc(
        BunProviderDescriptor {
            capability_id: CAPABILITY_ID,
            descriptor_version: DESCRIPTOR_VERSION.to_owned(),
            operations: vec!["greet".to_owned()],
        },
        64 * 1024,
        8,
        RustGreetingProvider,
    )
    .expect("Rust provider bridge should start");
    let url = format!("http://{}", server.address());
    let corpus: Vec<CorpusCase> = serde_json::from_str(include_str!(
        "../../../fixtures/bun/request-conformance.json"
    ))
    .expect("shared Bun request corpus should decode");
    assert_shared_corpus_contract(&corpus);
    for case in corpus
        .iter()
        .filter(|case| !matches!(case.scenario.as_str(), "cancellation" | "overload"))
    {
        let name = case
            .request
            .payload
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("Ada");
        let output = if case.scenario == "deadline" {
            run_bun_consumer_with_args(
                &url,
                name,
                &case.request.operation,
                &["--deadline-nanos", "0"],
            )
        } else {
            run_bun_consumer(&url, name, &case.request.operation)
        };
        let actual = output.get("kind").and_then(serde_json::Value::as_str);
        assert_eq!(actual, Some(case.outcome.as_str()), "case {}", case.name);
        if let Some(expected_failure) = case.failure_kind.as_deref() {
            assert_eq!(
                output["failure"]["kind"].as_str(),
                Some(expected_failure),
                "case {}",
                case.name
            );
        }
    }
    server.shutdown();

    let cancellation_server = BunProviderServer::json_rpc(
        BunProviderDescriptor {
            capability_id: CAPABILITY_ID,
            descriptor_version: DESCRIPTOR_VERSION.to_owned(),
            operations: vec!["greet".to_owned()],
        },
        64 * 1024,
        8,
        CancellableRustGreetingProvider,
    )
    .expect("cancellable Rust provider bridge should start");
    let cancellation = run_bun_consumer_with_cancel(
        &format!("http://{}", cancellation_server.address()),
        "__delay__",
        25,
    );
    cancellation_server.shutdown();
    assert_eq!(cancellation["failure"]["kind"], "cancelled");

    let overload_server = BunProviderServer::json_rpc(
        BunProviderDescriptor {
            capability_id: CAPABILITY_ID,
            descriptor_version: DESCRIPTOR_VERSION.to_owned(),
            operations: vec!["greet".to_owned()],
        },
        64 * 1024,
        1,
        RustGreetingProvider,
    )
    .expect("bounded Rust provider bridge should start");
    let overload = run_bun_consumer_with_args(
        &format!("http://{}", overload_server.address()),
        "__delay__",
        "greet",
        &["--parallel", "16"],
    );
    overload_server.shutdown();
    assert!(
        overload["outcomes"]
            .as_array()
            .is_some_and(|outcomes| outcomes
                .iter()
                .any(|outcome| { outcome["failure"]["kind"] == "resource_exhausted" })),
        "reverse bounded admission should reject overload: {overload}"
    );
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn bun_consumer_can_cancel_a_rust_provider_bridge() {
    let server = BunProviderServer::json_rpc(
        BunProviderDescriptor {
            capability_id: CAPABILITY_ID,
            descriptor_version: DESCRIPTOR_VERSION.to_owned(),
            operations: vec!["greet".to_owned()],
        },
        64 * 1024,
        8,
        CancellableRustGreetingProvider,
    )
    .expect("Rust provider bridge should start");
    let url = format!("http://{}", server.address());
    let output = run_bun_consumer_with_cancel(&url, "Ada", 25);
    server.shutdown();
    assert_eq!(output["kind"], "runtime");
    assert_eq!(output["failure"]["kind"], "cancelled");
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn dropping_a_call_cancels_the_in_flight_bun_request() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        assert_dropping_call_cancels(wire);
    }
}

fn assert_dropping_call_cancels(wire: BunWire) {
    let script = fixture("request-provider.ts");
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire).with_codec(GreetingCodec);
    let app = driver
        .run(Kernel::start(
            greeting_plan(&script),
            driver.clone(),
            ExecutionAdapterCatalog::single(adapter),
        ))
        .expect("Bun App should start");
    let cancellation = CancellationToken::new();
    let context = app.invocation_context(None, cancellation.clone());
    let call = app.invoke_with_context::<Greeting>(
        "bun-consumer",
        "greet",
        context,
        GreetRequest {
            name: "__delay__".to_owned(),
        },
    );
    let cancel = async {
        driver.yield_now().await;
        driver.yield_now().await;
        cancellation.cancel();
    };
    let result = driver.run(async { futures::future::join(call, cancel).await.0 });
    let _ = driver.run(app.shutdown(Duration::from_secs(2)));
    assert!(matches!(result, Err(RuntimeFailure::Cancelled { .. })));
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn bounded_wire_admission_rejects_overload_without_unbounded_growth() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        assert_bounded_wire_admission(wire);
    }
}

fn assert_bounded_wire_admission(wire: BunWire) {
    let script = fixture("request-provider.ts");
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire)
        .with_config(BunAdapterConfig::new(bun_binary(), wire).with_request_queue_capacity(1))
        .with_codec(GreetingCodec);
    let app = driver
        .run(Kernel::start(
            greeting_plan(&script),
            driver.clone(),
            ExecutionAdapterCatalog::single(adapter),
        ))
        .expect("Bun App should start");
    let handle = app
        .handle::<Greeting>("bun-consumer")
        .expect("Greeting binding should be available");
    let calls = (0..16)
        .map(|_| {
            handle.invoke(
                "greet",
                GreetRequest {
                    name: "__delay__".to_owned(),
                },
            )
        })
        .collect::<FuturesUnordered<_>>();
    let results = driver.run(calls.collect::<Vec<_>>());
    let rejected = results
        .iter()
        .filter(|result| matches!(result, Err(RuntimeFailure::ResourceExhausted { .. })))
        .count();
    let _ = driver.run(app.shutdown(Duration::from_secs(2)));
    assert!(rejected > 0, "bounded wire queue should reject overload");
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn both_wires_reject_oversized_requests_as_protocol_violations() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        assert_oversized_request_is_rejected(wire);
    }
}

fn assert_oversized_request_is_rejected(wire: BunWire) {
    let script = fixture("request-provider.ts");
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire)
        .with_config(BunAdapterConfig::new(bun_binary(), wire).with_max_frame_bytes(1024))
        .with_codec(GreetingCodec);
    let app = driver
        .run(Kernel::start(
            greeting_plan(&script),
            driver.clone(),
            ExecutionAdapterCatalog::single(adapter),
        ))
        .expect("Bun App should start");
    let result = driver.run(app.invoke::<Greeting>(
        "bun-consumer",
        "greet",
        GreetRequest {
            name: "x".repeat(4096),
        },
    ));
    let _ = driver.run(app.shutdown(Duration::from_secs(2)));
    assert!(matches!(
        result,
        Err(RuntimeFailure::ProtocolViolation { .. })
    ));
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn deadline_expires_before_the_adapter_can_replay_or_retry_a_request() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        assert_deadline_is_enforced_without_replay(wire);
    }
}

fn assert_deadline_is_enforced_without_replay(wire: BunWire) {
    let script = fixture("request-provider.ts");
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire).with_codec(GreetingCodec);
    let app = driver
        .run(Kernel::start(
            greeting_plan(&script),
            driver.clone(),
            ExecutionAdapterCatalog::single(adapter),
        ))
        .expect("Bun App should start");
    let context = app.invocation_context_after(Duration::from_millis(10), CancellationToken::new());
    let call = app.invoke_with_context::<Greeting>(
        "bun-consumer",
        "greet",
        context,
        GreetRequest {
            name: "__delay__".to_owned(),
        },
    );
    let result = driver.run(async {
        let advance = async {
            driver.yield_now().await;
            driver.advance(Duration::from_millis(10));
        };
        futures::join!(call, advance).0
    });
    let _ = driver.run(app.shutdown(Duration::from_secs(2)));
    assert!(matches!(
        result,
        Err(RuntimeFailure::DeadlineExceeded { .. })
    ));
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn provider_exit_recreates_the_generation_without_replaying_the_request() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        assert_provider_exit_recreates_generation(wire);
    }
}

fn assert_provider_exit_recreates_generation(wire: BunWire) {
    let script = fixture("request-provider.ts");
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire).with_codec(GreetingCodec);
    let app = driver
        .run(Kernel::start(
            greeting_plan(&script),
            driver.clone(),
            ExecutionAdapterCatalog::single(adapter),
        ))
        .expect("Bun App should start");
    let crashed = driver.run(app.invoke::<Greeting>(
        "bun-consumer",
        "greet",
        GreetRequest {
            name: "__crash__".to_owned(),
        },
    ));
    assert!(matches!(crashed, Err(RuntimeFailure::ModuleFailure { .. })));

    let mut restarted = false;
    for _ in 0..100 {
        driver.run(driver.yield_now());
        if app
            .module_generation("bun-provider")
            .is_some_and(|generation| generation >= 2)
        {
            restarted = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        restarted,
        "provider generation should be recreated; generation={:?}, failure={:?}",
        app.module_generation("bun-provider"),
        app.terminal_failure()
    );
    let result = driver.run(app.invoke::<Greeting>(
        "bun-consumer",
        "greet",
        GreetRequest {
            name: "Ada".to_owned(),
        },
    ));
    let _ = driver.run(app.shutdown(Duration::from_secs(2)));
    assert!(matches!(result, Ok(Ok(GreetResponse { .. }))));
}

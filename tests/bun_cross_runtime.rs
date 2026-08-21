use std::{
    any::Any,
    collections::VecDeque,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
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
    CancellationToken, DeterministicDriver, EventAdmission, EventCapability,
    ExecutionAdapterCatalog, Kernel, RuntimeDriver, RuntimeFailure, StreamCapability, StreamEvent,
};

use lenso_bun_adapter::{
    BunAdapter, BunAdapterConfig, BunCapabilityCodec, BunEventAction, BunEventBinding,
    BunProviderDescriptor, BunProviderHandler, BunProviderServer, BunProviderStream, BunRequest,
    BunResponse, BunStreamAction, BunStreamEvent, BunStreamOpenResponse, BunStreamReceive, BunWire,
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

const EVENT_CAPABILITY_ID: &str = "example.notifications@1";
const EVENT_DESCRIPTOR_VERSION: &str = "1.0.0";
const EVENT_OPERATION: &str = "notify";

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct Notification {
    message: String,
    sequence: u64,
}

#[derive(Debug)]
struct Notifications;

impl EventCapability for Notifications {
    type Event = Notification;

    const ID: &'static str = EVENT_CAPABILITY_ID;
    const DESCRIPTOR_VERSION: &'static str = EVENT_DESCRIPTOR_VERSION;
}

#[derive(Debug)]
struct NotificationCodec;

impl NotificationCodec {
    fn unknown(operation: &str) -> RuntimeFailure {
        RuntimeFailure::UnknownOperation {
            capability: EVENT_CAPABILITY_ID,
            operation: operation.to_owned(),
        }
    }
}

impl BunCapabilityCodec for NotificationCodec {
    fn capability_id(&self) -> &'static str {
        EVENT_CAPABILITY_ID
    }

    fn descriptor_version(&self) -> &'static str {
        EVENT_DESCRIPTOR_VERSION
    }

    fn operations(&self) -> &'static [&'static str] {
        &[EVENT_OPERATION]
    }

    fn event_operations(&self) -> &'static [&'static str] {
        &[EVENT_OPERATION]
    }

    fn request_operations(&self) -> &'static [&'static str] {
        &[]
    }

    fn encode_request(
        &self,
        operation: &str,
        _request: &dyn Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        Err(Self::unknown(operation))
    }

    fn encode_event(
        &self,
        operation: &str,
        event: &dyn Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        if operation != EVENT_OPERATION {
            return Err(Self::unknown(operation));
        }
        serde_json::to_value(event.downcast_ref::<Notification>().ok_or(
            RuntimeFailure::ProtocolViolation {
                capability: EVENT_CAPABILITY_ID,
            },
        )?)
        .map_err(|_| RuntimeFailure::ProtocolViolation {
            capability: EVENT_CAPABILITY_ID,
        })
    }

    fn decode_response(
        &self,
        operation: &str,
        _value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        Err(Self::unknown(operation))
    }

    fn decode_domain_error(
        &self,
        operation: &str,
        _value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        Err(Self::unknown(operation))
    }
}

const CHAT_CAPABILITY_ID: &str = "example.chat@1";
const CHAT_DESCRIPTOR_VERSION: &str = "1.0.0";
const CHAT_OPERATION: &str = "chat";

#[derive(Debug)]
struct Chat;

impl StreamCapability for Chat {
    type OpenRequest = ChatOpen;
    type Message = ChatMessage;
    type DomainError = ChatError;

    const ID: &'static str = CHAT_CAPABILITY_ID;
    const DESCRIPTOR_VERSION: &'static str = CHAT_DESCRIPTOR_VERSION;
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ChatOpen {
    room: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ChatMessage {
    text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ChatError {
    RoomClosed,
    Unknown(String),
}

#[derive(Debug)]
struct ChatCodec;

impl ChatCodec {
    fn error_from_value(value: serde_json::Value) -> Result<ChatError, RuntimeFailure> {
        match value {
            serde_json::Value::String(code) if code == "room_closed" => Ok(ChatError::RoomClosed),
            serde_json::Value::String(code) => Ok(ChatError::Unknown(code)),
            serde_json::Value::Object(object) => object
                .get("code")
                .and_then(serde_json::Value::as_str)
                .map(|code| ChatError::Unknown(code.to_owned()))
                .ok_or(RuntimeFailure::ProtocolViolation {
                    capability: CHAT_CAPABILITY_ID,
                }),
            _ => Err(RuntimeFailure::ProtocolViolation {
                capability: CHAT_CAPABILITY_ID,
            }),
        }
    }

    fn json<T: serde::Serialize>(value: &T) -> Result<serde_json::Value, RuntimeFailure> {
        serde_json::to_value(value).map_err(|_| RuntimeFailure::ProtocolViolation {
            capability: CHAT_CAPABILITY_ID,
        })
    }
}

impl BunCapabilityCodec for ChatCodec {
    fn capability_id(&self) -> &'static str {
        CHAT_CAPABILITY_ID
    }

    fn descriptor_version(&self) -> &'static str {
        CHAT_DESCRIPTOR_VERSION
    }

    fn operations(&self) -> &'static [&'static str] {
        &[CHAT_OPERATION]
    }

    fn stream_operations(&self) -> &'static [&'static str] {
        &[CHAT_OPERATION]
    }

    fn request_operations(&self) -> &'static [&'static str] {
        &[]
    }

    fn encode_request(
        &self,
        operation: &str,
        _request: &dyn Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        Err(RuntimeFailure::UnknownOperation {
            capability: CHAT_CAPABILITY_ID,
            operation: operation.to_owned(),
        })
    }

    fn decode_response(
        &self,
        operation: &str,
        _value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        Err(RuntimeFailure::UnknownOperation {
            capability: CHAT_CAPABILITY_ID,
            operation: operation.to_owned(),
        })
    }

    fn decode_domain_error(
        &self,
        operation: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        if operation != CHAT_OPERATION {
            return Err(RuntimeFailure::UnknownOperation {
                capability: CHAT_CAPABILITY_ID,
                operation: operation.to_owned(),
            });
        }
        Ok(Box::new(Self::error_from_value(value)?))
    }

    fn encode_stream_open(
        &self,
        operation: &str,
        request: &dyn Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        if operation != CHAT_OPERATION {
            return Err(RuntimeFailure::UnknownOperation {
                capability: CHAT_CAPABILITY_ID,
                operation: operation.to_owned(),
            });
        }
        let request =
            request
                .downcast_ref::<ChatOpen>()
                .ok_or(RuntimeFailure::ProtocolViolation {
                    capability: CHAT_CAPABILITY_ID,
                })?;
        Self::json(request)
    }

    fn encode_stream_message(
        &self,
        operation: &str,
        message: &dyn Any,
    ) -> Result<serde_json::Value, RuntimeFailure> {
        if operation != CHAT_OPERATION {
            return Err(RuntimeFailure::UnknownOperation {
                capability: CHAT_CAPABILITY_ID,
                operation: operation.to_owned(),
            });
        }
        let message =
            message
                .downcast_ref::<ChatMessage>()
                .ok_or(RuntimeFailure::ProtocolViolation {
                    capability: CHAT_CAPABILITY_ID,
                })?;
        Self::json(message)
    }

    fn decode_stream_message(
        &self,
        operation: &str,
        value: serde_json::Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        if operation != CHAT_OPERATION {
            return Err(RuntimeFailure::UnknownOperation {
                capability: CHAT_CAPABILITY_ID,
                operation: operation.to_owned(),
            });
        }
        serde_json::from_value::<ChatMessage>(value)
            .map(|message| Box::new(message) as Box<dyn Any>)
            .map_err(|_| RuntimeFailure::ProtocolViolation {
                capability: CHAT_CAPABILITY_ID,
            })
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

#[derive(Debug, Clone)]
struct RustNotificationProvider {
    seen: Arc<Mutex<Vec<Notification>>>,
}

impl BunProviderHandler for RustNotificationProvider {
    fn invoke(&self, request: BunRequest) -> BunResponse {
        BunResponse::Runtime(RuntimeFailure::UnknownOperation {
            capability: EVENT_CAPABILITY_ID,
            operation: request.operation,
        })
    }

    fn publish_event(&self, request: BunRequest) -> BunEventAction {
        let Ok(event) = serde_json::from_value::<Notification>(request.payload) else {
            return BunEventAction::Runtime(RuntimeFailure::ProtocolViolation {
                capability: EVENT_CAPABILITY_ID,
            });
        };
        self.seen.lock().expect("event recorder lock").push(event);
        BunEventAction::Accepted
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

#[derive(Debug, Default)]
struct RustChatStreamState {
    events: VecDeque<BunStreamEvent>,
    cancelled: bool,
    provider_closes_first: bool,
}

#[derive(Debug)]
struct RustChatStream {
    state: Arc<Mutex<RustChatStreamState>>,
}

impl BunProviderStream for RustChatStream {
    fn send(&self, payload: serde_json::Value) -> BunStreamAction {
        let message = match serde_json::from_value::<ChatMessage>(payload) {
            Ok(message) => message,
            Err(_) => {
                return BunStreamAction::Runtime(RuntimeFailure::ProtocolViolation {
                    capability: CHAT_CAPABILITY_ID,
                });
            }
        };
        let Ok(mut state) = self.state.lock() else {
            return BunStreamAction::Runtime(RuntimeFailure::Internal {
                detail: "Rust chat stream lock poisoned".to_owned(),
            });
        };
        if state.cancelled {
            return BunStreamAction::Runtime(RuntimeFailure::Cancelled { request_id: 0 });
        }
        if !state.provider_closes_first {
            state.events.push_back(BunStreamEvent::Message(
                serde_json::to_value(ChatMessage {
                    text: format!("Rust echo: {}", message.text),
                })
                .expect("chat message should serialize"),
            ));
        }
        BunStreamAction::Accepted
    }

    fn receive(&self) -> BunStreamReceive {
        let Ok(mut state) = self.state.lock() else {
            return BunStreamReceive::Runtime(RuntimeFailure::Internal {
                detail: "Rust chat stream lock poisoned".to_owned(),
            });
        };
        state.events.pop_front().map_or_else(
            || {
                BunStreamReceive::Runtime(RuntimeFailure::Internal {
                    detail: "Rust chat stream has no queued event".to_owned(),
                })
            },
            BunStreamReceive::Event,
        )
    }

    fn peer_half_closed(&self) -> BunStreamAction {
        let Ok(mut state) = self.state.lock() else {
            return BunStreamAction::Runtime(RuntimeFailure::Internal {
                detail: "Rust chat stream lock poisoned".to_owned(),
            });
        };
        if state.cancelled {
            return BunStreamAction::Runtime(RuntimeFailure::Cancelled { request_id: 0 });
        }
        if !state.provider_closes_first {
            state.events.push_back(BunStreamEvent::PeerHalfClosed);
        }
        state.events.push_back(BunStreamEvent::Terminal(Ok(())));
        BunStreamAction::Accepted
    }

    fn cancel(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.cancelled = true;
            state.events.clear();
        }
    }
}

#[derive(Debug)]
struct RustChatProvider;

impl BunProviderHandler for RustChatProvider {
    fn invoke(&self, _request: BunRequest) -> BunResponse {
        BunResponse::Runtime(RuntimeFailure::UnknownOperation {
            capability: CHAT_CAPABILITY_ID,
            operation: "request".to_owned(),
        })
    }

    fn open_stream(&self, request: BunRequest) -> BunStreamOpenResponse {
        if request.deadline_nanos == Some(0) {
            return BunStreamOpenResponse::Runtime(RuntimeFailure::DeadlineExceeded {
                request_id: request.request_id,
            });
        }
        let request = match serde_json::from_value::<ChatOpen>(request.payload) {
            Ok(request) => request,
            Err(_) => {
                return BunStreamOpenResponse::Runtime(RuntimeFailure::ProtocolViolation {
                    capability: CHAT_CAPABILITY_ID,
                });
            }
        };
        if request.room == "closed" {
            return BunStreamOpenResponse::Domain(serde_json::json!("room_closed"));
        }
        let mut state = RustChatStreamState::default();
        if request.room == "provider-closes-first" {
            state.provider_closes_first = true;
            state.events.push_back(BunStreamEvent::PeerHalfClosed);
        }
        BunStreamOpenResponse::Success(Arc::new(RustChatStream {
            state: Arc::new(Mutex::new(state)),
        }))
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

fn event_plan(accepting_script: &Path, rejecting_script: &Path) -> lenso_app_plan::ResolvedAppPlan {
    let endpoint = CapabilityEndpointPlan::new(
        EVENT_CAPABILITY_ID,
        EVENT_DESCRIPTOR_VERSION,
        [EVENT_OPERATION],
    )
    .with_event_operation(EVENT_OPERATION)
    .with_event_capacity(2);
    let provider_a = ModuleInstancePlan::new("bun-provider-a", "fixture.bun.events")
        .with_entrypoint(accepting_script.to_string_lossy())
        .with_execution_class(ExecutionClassId::bun_child_process())
        .with_restart_policy(RestartPolicy::on_failure(
            2,
            Duration::from_secs(5),
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        ))
        .with_capability(endpoint.clone());
    let provider_b = ModuleInstancePlan::new("bun-provider-b", "fixture.bun.events.reject")
        .with_entrypoint(rejecting_script.to_string_lossy())
        .with_execution_class(ExecutionClassId::bun_child_process())
        .with_restart_policy(RestartPolicy::on_failure(
            2,
            Duration::from_secs(5),
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        ))
        .with_capability(endpoint);
    let consumer = ModuleInstancePlan::new("bun-consumer", "fixture.bun.events.consumer")
        .with_entrypoint(accepting_script.to_string_lossy())
        .with_execution_class(ExecutionClassId::bun_child_process())
        .with_requirement(CapabilityRequirementPlan::many(
            EVENT_CAPABILITY_ID,
            EVENT_DESCRIPTOR_VERSION,
        ));
    AppComposition::new(
        vec![provider_a, provider_b, consumer],
        vec![
            CapabilityBinding::new(
                "bun-consumer",
                EVENT_CAPABILITY_ID,
                EVENT_DESCRIPTOR_VERSION,
                "bun-provider-a",
            ),
            CapabilityBinding::new(
                "bun-consumer",
                EVENT_CAPABILITY_ID,
                EVENT_DESCRIPTOR_VERSION,
                "bun-provider-b",
            ),
        ],
    )
    .resolve()
    .expect("Bun Event Composition should resolve")
}

fn stream_plan(script: &Path) -> lenso_app_plan::ResolvedAppPlan {
    stream_plan_with_concurrency(script, 1)
}

fn stream_plan_with_concurrency(
    script: &Path,
    max_concurrency: usize,
) -> lenso_app_plan::ResolvedAppPlan {
    let endpoint = CapabilityEndpointPlan::new(
        CHAT_CAPABILITY_ID,
        CHAT_DESCRIPTOR_VERSION,
        [CHAT_OPERATION],
    )
    .with_stream_operation(CHAT_OPERATION)
    .with_limits(0, max_concurrency);
    let provider = ModuleInstancePlan::new("bun-provider", "fixture.bun.stream")
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
    let consumer = ModuleInstancePlan::new("bun-consumer", "fixture.bun.stream-consumer")
        .with_entrypoint(script.to_string_lossy())
        .with_execution_class(ExecutionClassId::bun_child_process())
        .with_requirement(CapabilityRequirementPlan::one(
            CHAT_CAPABILITY_ID,
            CHAT_DESCRIPTOR_VERSION,
        ));
    AppComposition::new(
        vec![provider, consumer],
        vec![CapabilityBinding::new(
            "bun-consumer",
            CHAT_CAPABILITY_ID,
            CHAT_DESCRIPTOR_VERSION,
            "bun-provider",
        )],
    )
    .resolve()
    .expect("Bun stream Composition should resolve")
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

fn run_bun_stream(
    wire: BunWire,
    room: &str,
) -> Result<Result<Vec<StreamEvent<ChatMessage, ChatError>>, ChatError>, RuntimeFailure> {
    let script = fixture("stream-provider.ts");
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire).with_codec(ChatCodec);
    let app = driver.run(Kernel::start(
        stream_plan(&script),
        driver.clone(),
        ExecutionAdapterCatalog::single(adapter),
    ))?;
    let handle = app.stream_handle::<Chat>("bun-consumer")?;
    let outcome = match driver.run(handle.open(
        CHAT_OPERATION,
        ChatOpen {
            room: room.to_owned(),
        },
    )) {
        Err(error) => Err(error),
        Ok(Err(error)) => Ok(Err(error)),
        Ok(Ok(stream)) => {
            let result =
                (|| -> Result<Vec<StreamEvent<ChatMessage, ChatError>>, RuntimeFailure> {
                    if room == "provider-closes-first" {
                        let half_closed = driver.run(stream.receive())?;
                        driver.run(stream.send(ChatMessage {
                            text: "accepted after provider half-close".to_owned(),
                        }))?;
                        driver.run(stream.close_send())?;
                        let terminal = driver.run(stream.receive())?;
                        return Ok(vec![half_closed, terminal]);
                    }
                    driver.run(stream.send(ChatMessage {
                        text: "one".to_owned(),
                    }))?;
                    driver.run(stream.send(ChatMessage {
                        text: "two".to_owned(),
                    }))?;
                    let first = driver.run(stream.receive())?;
                    let second = driver.run(stream.receive())?;
                    driver.run(stream.close_send())?;
                    let half_closed = driver.run(stream.receive())?;
                    let terminal = driver.run(stream.receive())?;
                    Ok(vec![first, second, half_closed, terminal])
                })();
            result.map(Ok)
        }
    };
    let _ = driver.run(app.shutdown(Duration::from_secs(2)));
    outcome
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

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn both_wires_report_partial_event_admission_without_short_circuiting_fan_out() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        let accepting_script = fixture("event-provider.ts");
        let rejecting_script = fixture("event-provider-reject.ts");
        let driver = DeterministicDriver::new();
        let adapter = BunAdapter::new(bun_binary(), wire).with_codec(NotificationCodec);
        let app = driver
            .run(Kernel::start(
                event_plan(&accepting_script, &rejecting_script),
                driver.clone(),
                ExecutionAdapterCatalog::single(adapter),
            ))
            .expect("Bun Event App should start");
        let handle = app
            .many_event_handle::<Notifications>("bun-consumer")
            .expect("many Event binding should be materialized");
        let outcomes = driver.run(handle.publish(
            EVENT_OPERATION,
            Notification {
                message: "partial-admission".to_owned(),
                sequence: 1,
            },
        ));
        assert_eq!(
            outcomes
                .iter()
                .map(|outcome| (outcome.subscriber_instance(), outcome.admission()))
                .collect::<Vec<_>>(),
            vec![
                ("bun-provider-a", EventAdmission::Accepted),
                ("bun-provider-b", EventAdmission::Exhausted),
            ],
            "{wire:?} should attempt every explicit subscriber binding"
        );
        let _ = driver.run(app.shutdown(Duration::from_secs(2)));
    }
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn both_wires_bound_each_bun_event_subscriber_queue_independently() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        let accepting_script = fixture("event-provider.ts");
        let rejecting_script = fixture("event-provider-reject.ts");
        let driver = DeterministicDriver::new();
        let adapter = BunAdapter::new(bun_binary(), wire).with_codec(NotificationCodec);
        let app = driver
            .run(Kernel::start(
                event_plan(&accepting_script, &rejecting_script),
                driver.clone(),
                ExecutionAdapterCatalog::single(adapter),
            ))
            .expect("Bun Event App should start");
        let handle = app
            .many_event_handle::<Notifications>("bun-consumer")
            .expect("many Event binding should be materialized");
        let publications = futures::future::join_all((0..8).map(|sequence| {
            handle.publish(
                EVENT_OPERATION,
                Notification {
                    message: "slow".to_owned(),
                    sequence,
                },
            )
        }));
        let outcomes = driver.run(publications);
        assert!(outcomes.iter().all(|outcome| outcome.len() == 2));
        let accepting_statuses: Vec<_> = outcomes
            .iter()
            .map(|outcome| outcome[0].admission())
            .collect();
        assert_eq!(
            accepting_statuses
                .iter()
                .filter(|status| **status == EventAdmission::Accepted)
                .count(),
            2,
            "{wire:?} should admit only the active event and its two-slot volatile queue"
        );
        assert!(
            accepting_statuses
                .iter()
                .any(|status| *status == EventAdmission::Exhausted)
        );
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome[1].admission() == EventAdmission::Exhausted)
        );
        let _ = driver.run(app.shutdown(Duration::from_secs(2)));
    }
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn accepted_event_is_not_replayed_when_a_bun_subscriber_exits_and_recovers() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        let accepting_script = fixture("event-provider.ts");
        let rejecting_script = fixture("event-provider-reject.ts");
        let driver = DeterministicDriver::new();
        let adapter = BunAdapter::new(bun_binary(), wire).with_codec(NotificationCodec);
        let app = driver
            .run(Kernel::start(
                event_plan(&accepting_script, &rejecting_script),
                driver.clone(),
                ExecutionAdapterCatalog::single(adapter),
            ))
            .expect("Bun Event App should start");
        let handle = app
            .many_event_handle::<Notifications>("bun-consumer")
            .expect("many Event binding should be materialized");
        let accepted = driver.run(handle.publish(
            EVENT_OPERATION,
            Notification {
                message: "__crash__".to_owned(),
                sequence: 1,
            },
        ));
        assert_eq!(accepted[0].admission(), EventAdmission::Accepted);

        let mut restarted = false;
        for _ in 0..100 {
            driver.run(driver.yield_now());
            if app
                .module_generation("bun-provider-a")
                .is_some_and(|generation| generation >= 2)
            {
                restarted = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            restarted,
            "{wire:?} Event subscriber should recover; generation={:?}, failure={:?}",
            app.module_generation("bun-provider-a"),
            app.terminal_failure()
        );
        assert_eq!(
            app.module_generation("bun-provider-a"),
            Some(2),
            "{wire:?} should recreate exactly one generation after the crashing Event"
        );
        for _ in 0..25 {
            driver.run(driver.yield_now());
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            app.module_generation("bun-provider-a"),
            Some(2),
            "{wire:?} must not replay the accepted crashing Event into the recovered generation"
        );
        assert!(app.terminal_failure().is_none());

        let after_restart = driver.run(handle.publish(
            EVENT_OPERATION,
            Notification {
                message: "after-restart".to_owned(),
                sequence: 2,
            },
        ));
        assert_eq!(after_restart[0].admission(), EventAdmission::Accepted);
        assert_eq!(after_restart[1].admission(), EventAdmission::Exhausted);
        let _ = driver.run(app.shutdown(Duration::from_secs(2)));
    }
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn both_wires_support_full_duplex_streams_and_independent_half_close() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        let result = run_bun_stream(wire, "room-1").unwrap_or_else(|error| {
            panic!("{wire:?} stream should not fail at runtime: {error:?}")
        });
        let events = result.expect("stream open should not return a Domain Error");
        assert_eq!(
            events,
            vec![
                StreamEvent::Message(ChatMessage {
                    text: "Bun echo: one".to_owned(),
                }),
                StreamEvent::Message(ChatMessage {
                    text: "Bun echo: two".to_owned(),
                }),
                StreamEvent::PeerHalfClosed,
                StreamEvent::Terminal(Ok(())),
            ]
        );

        let domain = run_bun_stream(wire, "closed").expect("stream open should reach Bun");
        assert_eq!(domain, Err(ChatError::RoomClosed));

        let provider_first = run_bun_stream(wire, "provider-closes-first")
            .unwrap_or_else(|error| panic!("{wire:?} provider-first close failed: {error:?}"))
            .expect("provider-first stream open should succeed");
        assert_eq!(
            provider_first,
            vec![StreamEvent::PeerHalfClosed, StreamEvent::Terminal(Ok(()))]
        );
    }
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn stream_provider_restart_terminates_without_replay_and_reopens_the_stable_handle() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        assert_stream_provider_restart(wire);
    }
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn both_wires_bound_stream_admission_and_reject_oversized_messages() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        assert_bounded_stream_admission(wire);
        assert_oversized_stream_message_is_rejected(wire);
    }
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn saturated_stream_send_can_retry_without_losing_sequence() {
    for wire in [BunWire::FramedStdio, BunWire::JsonRpcHttp] {
        assert_saturated_stream_send_can_retry(wire);
    }
}

fn assert_saturated_stream_send_can_retry(wire: BunWire) {
    let script = fixture("stream-provider.ts");
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire).with_codec(ChatCodec);
    let app = driver
        .run(Kernel::start(
            stream_plan(&script),
            driver.clone(),
            ExecutionAdapterCatalog::single(adapter),
        ))
        .expect("Bun stream App should start");
    let handle = app
        .stream_handle::<Chat>("bun-consumer")
        .expect("stream handle should be available");
    let stream = driver
        .run(handle.open(
            CHAT_OPERATION,
            ChatOpen {
                room: "room-1".to_owned(),
            },
        ))
        .expect("stream open should not fail")
        .expect("stream should open");

    for index in 0..16 {
        driver
            .run(stream.send(ChatMessage {
                text: format!("buffered-{index}"),
            }))
            .expect("the bounded provider buffer should admit its advertised credit");
    }
    let overflow = ChatMessage {
        text: "after-saturation".to_owned(),
    };
    let saturated = driver.run(stream.send(overflow.clone()));
    assert!(
        matches!(saturated, Err(RuntimeFailure::ResourceExhausted { .. })),
        "{wire:?} should report bounded saturation, got {saturated:?}"
    );

    assert!(matches!(
        driver.run(stream.receive()),
        Ok(StreamEvent::Message(ChatMessage { text })) if text == "Bun echo: buffered-0"
    ));
    driver
        .run(stream.send(overflow))
        .expect("retry after one receive should preserve the rejected sequence");
    for _ in 1..16 {
        driver
            .run(stream.receive())
            .expect("previously admitted messages should remain ordered");
    }
    assert!(matches!(
        driver.run(stream.receive()),
        Ok(StreamEvent::Message(ChatMessage { text })) if text == "Bun echo: after-saturation"
    ));
    let _ = driver.run(app.shutdown(Duration::from_secs(2)));
}

fn assert_bounded_stream_admission(wire: BunWire) {
    let script = fixture("stream-provider.ts");
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire).with_codec(ChatCodec);
    let app = driver
        .run(Kernel::start(
            stream_plan(&script),
            driver.clone(),
            ExecutionAdapterCatalog::single(adapter),
        ))
        .expect("Bun stream App should start");
    let handle = app
        .stream_handle::<Chat>("bun-consumer")
        .expect("stream handle should be available");
    let first = driver
        .run(handle.open(
            CHAT_OPERATION,
            ChatOpen {
                room: "room-1".to_owned(),
            },
        ))
        .expect("first stream open should not fail")
        .expect("first stream should open");
    assert!(matches!(
        driver.run(handle.open(
            CHAT_OPERATION,
            ChatOpen {
                room: "room-2".to_owned(),
            },
        )),
        Err(RuntimeFailure::ResourceExhausted {
            capability: CHAT_CAPABILITY_ID,
            operation
        }) if operation == CHAT_OPERATION
    ));
    drop(first);
    let _ = driver.run(app.shutdown(Duration::from_secs(2)));
}

fn assert_oversized_stream_message_is_rejected(wire: BunWire) {
    let script = fixture("stream-provider.ts");
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire)
        .with_config(BunAdapterConfig::new(bun_binary(), wire).with_max_frame_bytes(1024))
        .with_codec(ChatCodec);
    let app = driver
        .run(Kernel::start(
            stream_plan(&script),
            driver.clone(),
            ExecutionAdapterCatalog::single(adapter),
        ))
        .expect("Bun stream App should start");
    let handle = app
        .stream_handle::<Chat>("bun-consumer")
        .expect("stream handle should be available");
    let stream = driver
        .run(handle.open(
            CHAT_OPERATION,
            ChatOpen {
                room: "room-1".to_owned(),
            },
        ))
        .expect("stream open should not fail")
        .expect("stream should open");
    let result = driver.run(stream.send(ChatMessage {
        text: "x".repeat(4096),
    }));
    let _ = driver.run(app.shutdown(Duration::from_secs(2)));
    assert!(matches!(
        result,
        Err(RuntimeFailure::ProtocolViolation { .. })
    ));
}

fn assert_stream_provider_restart(wire: BunWire) {
    let script = fixture("stream-provider.ts");
    let driver = DeterministicDriver::new();
    let adapter = BunAdapter::new(bun_binary(), wire).with_codec(ChatCodec);
    let app = driver
        .run(Kernel::start(
            stream_plan_with_concurrency(&script, 2),
            driver.clone(),
            ExecutionAdapterCatalog::single(adapter),
        ))
        .expect("Bun stream App should start");
    let handle = app
        .stream_handle::<Chat>("bun-consumer")
        .expect("stream handle should be available");
    let existing = driver
        .run(handle.open(
            CHAT_OPERATION,
            ChatOpen {
                room: "room-before-restart".to_owned(),
            },
        ))
        .expect("existing stream open should not fail")
        .expect("existing stream should open");
    let crashing = driver
        .run(handle.open(
            CHAT_OPERATION,
            ChatOpen {
                room: "crashing-stream".to_owned(),
            },
        ))
        .expect("crashing stream open should not fail")
        .expect("crashing stream should open");
    let crashed = driver.run(crashing.send(ChatMessage {
        text: "__crash__".to_owned(),
    }));
    assert!(matches!(crashed, Err(RuntimeFailure::ModuleFailure { .. })));
    drop(crashing);

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
        "stream provider generation should be recreated; generation={:?}",
        app.module_generation("bun-provider")
    );
    let existing_result = driver.run(existing.receive());
    assert!(
        matches!(
            existing_result,
            Err(RuntimeFailure::Unavailable {
                capability: CHAT_CAPABILITY_ID
            }) | Err(RuntimeFailure::ModuleFailure { .. })
        ),
        "{wire:?} existing stream should terminate with its old generation, got {existing_result:?}"
    );
    drop(existing);
    let reopened = driver
        .run(handle.open(
            CHAT_OPERATION,
            ChatOpen {
                room: "room-after-restart".to_owned(),
            },
        ))
        .expect("stable stream handle should open after restart")
        .expect("reopened stream should not return a Domain Error");
    drop(reopened);
    let _ = driver.run(app.shutdown(Duration::from_secs(2)));
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

fn run_bun_event_consumer(url: &str, message: &str, sequence: u64) -> serde_json::Value {
    let output = Command::new(bun_binary())
        .arg("run")
        .arg(fixture("event-consumer.ts"))
        .arg("--")
        .arg("--lenso-url")
        .arg(url)
        .arg("--message")
        .arg(message)
        .arg("--sequence")
        .arg(sequence.to_string())
        .output()
        .expect("Bun Event consumer should start");
    assert!(
        output.status.success(),
        "Bun Event consumer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("Bun Event consumer should emit JSON")
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

fn run_bun_stream_consumer(url: &str, room: &str) -> serde_json::Value {
    let output = Command::new(bun_binary())
        .arg("run")
        .arg(fixture("stream-consumer.ts"))
        .arg("--")
        .arg("--lenso-url")
        .arg(url)
        .arg("--room")
        .arg(room)
        .output()
        .expect("Bun stream consumer should start");
    assert!(
        output.status.success(),
        "Bun stream consumer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("Bun stream consumer should emit JSON")
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn bun_consumer_can_call_a_rust_provider_bridge() {
    let server = BunProviderServer::json_rpc(
        BunProviderDescriptor {
            capability_id: CAPABILITY_ID,
            descriptor_version: DESCRIPTOR_VERSION.to_owned(),
            operations: vec!["greet".to_owned()],
            stream_operations: Vec::new(),
            event_operations: Vec::new(),
            event_bindings: Vec::new(),
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
fn bun_consumer_can_publish_events_to_a_rust_provider_bridge() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let server = BunProviderServer::json_rpc(
        BunProviderDescriptor {
            capability_id: EVENT_CAPABILITY_ID,
            descriptor_version: EVENT_DESCRIPTOR_VERSION.to_owned(),
            operations: vec![EVENT_OPERATION.to_owned()],
            stream_operations: Vec::new(),
            event_operations: vec![EVENT_OPERATION.to_owned()],
            event_bindings: vec![BunEventBinding::new("bun-consumer", 8)],
        },
        64 * 1024,
        8,
        RustNotificationProvider { seen: seen.clone() },
    )
    .expect("Rust Event provider bridge should start");
    let url = format!("http://{}", server.address());
    let accepted = run_bun_event_consumer(&url, "from-bun", 1);
    assert_eq!(
        accepted,
        serde_json::json!({ "kind": "success", "value": null })
    );
    for _ in 0..100 {
        if seen.lock().expect("event recorder lock").len() == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    server.shutdown();
    assert_eq!(
        &*seen.lock().expect("event recorder lock"),
        &[Notification {
            message: "from-bun".to_owned(),
            sequence: 1,
        }]
    );
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn bun_consumer_can_open_full_duplex_streams_from_a_rust_provider_bridge() {
    let server = BunProviderServer::json_rpc(
        BunProviderDescriptor {
            capability_id: CHAT_CAPABILITY_ID,
            descriptor_version: CHAT_DESCRIPTOR_VERSION.to_owned(),
            operations: vec![CHAT_OPERATION.to_owned()],
            stream_operations: vec![CHAT_OPERATION.to_owned()],
            event_operations: Vec::new(),
            event_bindings: Vec::new(),
        },
        64 * 1024,
        8,
        RustChatProvider,
    )
    .expect("Rust stream provider bridge should start");
    let url = format!("http://{}", server.address());
    let output = run_bun_stream_consumer(&url, "room-1");
    assert_eq!(
        output,
        serde_json::json!({
            "kind": "success",
            "value": { "text": "Rust echo: hello from Bun" },
            "events": ["peer_half_closed", "success"]
        })
    );
    let domain = run_bun_stream_consumer(&url, "closed");
    assert_eq!(
        domain,
        serde_json::json!({ "kind": "domain", "value": "room_closed" })
    );
    let provider_first = run_bun_stream_consumer(&url, "provider-closes-first");
    assert_eq!(
        provider_first,
        serde_json::json!({
            "kind": "success",
            "events": ["peer_half_closed", "success"]
        })
    );
    server.shutdown();
}

#[test]
#[ignore = "requires Bun; CI runs ignored cross-runtime tests after installing Bun"]
fn shared_request_corpus_has_the_same_outcomes_for_a_bun_consumer() {
    let server = BunProviderServer::json_rpc(
        BunProviderDescriptor {
            capability_id: CAPABILITY_ID,
            descriptor_version: DESCRIPTOR_VERSION.to_owned(),
            operations: vec!["greet".to_owned()],
            stream_operations: Vec::new(),
            event_operations: Vec::new(),
            event_bindings: Vec::new(),
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
            stream_operations: Vec::new(),
            event_operations: Vec::new(),
            event_bindings: Vec::new(),
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
            stream_operations: Vec::new(),
            event_operations: Vec::new(),
            event_bindings: Vec::new(),
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
            stream_operations: Vec::new(),
            event_operations: Vec::new(),
            event_bindings: Vec::new(),
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

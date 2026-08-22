use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver},
    },
    thread,
    time::Duration,
};

use jsonrpsee::{
    RpcModule,
    server::{ServerConfig, ServerHandle},
    types::ErrorObjectOwned,
};
use lenso_kernel::RuntimeFailure;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::protocol::{
    BunInvocationExtension, EndpointDescriptor, Handshake, HandshakeAck, WireEventPublish,
    WireFailure, WireOutcome, WireRequest, WireStreamCall, WireStreamEvent, WireStreamOpen,
    WireStreamOutcome, WireStreamTerminal, handshake_for, to_wire_failure, verify_handshake,
};

const PROVIDER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_CANCELLATION_ENTRIES: usize = 1024;
const MAX_RETIRED_REQUEST_IDS: usize = 1024;
static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// The exact Capability endpoint exposed by an Adapter-owned Rust provider bridge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BunProviderDescriptor {
    /// Stable Capability series identity.
    pub capability_id: &'static str,
    /// Exact generated Descriptor version.
    pub descriptor_version: String,
    /// Exact stable Operation table.
    pub operations: Vec<String>,
    /// Exact stream Operation subset of the Capability.
    pub stream_operations: Vec<String>,
    /// Exact ephemeral Event Operation subset of the Capability.
    pub event_operations: Vec<String>,
    /// Explicit Bun consumer bindings allowed to publish Events to this endpoint.
    pub event_bindings: Vec<BunEventBinding>,
}

/// One explicit Bun consumer-to-Rust Event binding and its volatile capacity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BunEventBinding {
    caller_instance: String,
    capacity: usize,
}

impl BunEventBinding {
    /// Creates one explicit Event binding.
    pub fn new(caller_instance: impl Into<String>, capacity: usize) -> Self {
        Self {
            caller_instance: caller_instance.into(),
            capacity,
        }
    }
}

/// One request received from a Bun consumer after the exact handshake.
#[derive(Clone, Debug)]
pub struct BunRequest {
    /// Kernel-compatible correlation identity supplied by the consumer.
    pub request_id: u64,
    /// Exact Capability series identity.
    pub capability_id: String,
    /// Exact Operation name.
    pub operation: String,
    /// Absolute Driver-monotonic deadline encoded by the consumer, when present.
    pub deadline_nanos: Option<u64>,
    /// Resolved caller Module Instance, when present.
    pub caller_instance: Option<String>,
    /// Generated portable JSON request value.
    pub payload: Value,
    /// Opaque ordinary and sealed Invocation Context extensions.
    pub extensions: Vec<BunInvocationExtension>,
    pub(crate) cancellation: Arc<AtomicBool>,
}

impl BunRequest {
    /// Returns whether the consumer cancelled this request or the bridge is shutting down.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.load(Ordering::Acquire)
    }
}

/// A typed-at-the-wire response returned by a Rust provider bridge.
#[derive(Clone, Debug)]
pub enum BunResponse {
    /// A generated response value.
    Success(Value),
    /// A known or forward-compatible generated Domain Error value.
    Domain(Value),
    /// A Kernel Runtime Failure that the Adapter serializes without widening the Kernel.
    Runtime(RuntimeFailure),
}

/// The admission result returned by a Rust Event subscriber bridge.
#[derive(Clone, Debug)]
pub enum BunEventAction {
    /// The Event entered the subscriber's bounded volatile queue.
    Accepted,
    /// A Runtime Failure prevented admission.
    Runtime(RuntimeFailure),
}

/// One event emitted by a Rust provider stream toward a Bun consumer.
#[derive(Clone, Debug)]
pub enum BunStreamEvent {
    /// One generated portable JSON message.
    Message(Value),
    /// The Rust provider closed only its sending direction.
    PeerHalfClosed,
    /// The one terminal Domain outcome, with `Ok(())` representing success.
    Terminal(Result<(), Value>),
}

/// The result of one provider-side stream action.
#[derive(Clone, Debug)]
pub enum BunStreamAction {
    /// The action was admitted and completed.
    Accepted,
    /// A Runtime Failure prevented the action.
    Runtime(RuntimeFailure),
}

/// The result of reading one provider-side stream event.
#[derive(Clone, Debug)]
pub enum BunStreamReceive {
    /// One ordered event is ready for the Bun consumer.
    Event(BunStreamEvent),
    /// A Runtime Failure prevented delivery.
    Runtime(RuntimeFailure),
}

/// A stream session retained by the Rust provider bridge.
pub trait BunProviderStream: std::fmt::Debug + Send + Sync + 'static {
    /// Delivers one Bun consumer message to the provider.
    fn send(&self, payload: Value) -> BunStreamAction;
    /// Reads one provider-to-consumer message, half-close, or terminal outcome.
    fn receive(&self) -> BunStreamReceive;
    /// Notifies the provider that the Bun consumer closed its sending direction.
    fn peer_half_closed(&self) -> BunStreamAction {
        BunStreamAction::Accepted
    }
    /// Cancels this provider-side session idempotently.
    fn cancel(&self);
}

/// Result returned when a Rust provider opens a stream for a Bun consumer.
#[derive(Clone, Debug)]
pub enum BunStreamOpenResponse {
    /// The stream session is ready.
    Success(Arc<dyn BunProviderStream>),
    /// A Capability-defined Domain Error rejected opening.
    Domain(Value),
    /// A Runtime Failure rejected opening.
    Runtime(RuntimeFailure),
}

/// Host-side provider implementation used by the Bun consumer bridge.
pub trait BunProviderHandler: std::fmt::Debug + Send + Sync + 'static {
    /// Handles one already-framed, already-handshake-validated request.
    ///
    /// A bounded bridge can deliver cancellation while this method is running;
    /// cooperative handlers should poll `BunRequest::is_cancelled` and return
    /// a terminal Runtime Failure instead of retaining the request forever.
    fn invoke(&self, request: BunRequest) -> BunResponse;

    /// Admits one Event into the Rust subscriber's bounded volatile queue.
    fn publish_event(&self, request: BunRequest) -> BunEventAction {
        BunEventAction::Runtime(RuntimeFailure::UnknownOperation {
            capability: "lenso.bun-process@1",
            operation: request.operation,
        })
    }

    /// Opens one bidirectional stream after the exact handshake.
    fn open_stream(&self, request: BunRequest) -> BunStreamOpenResponse {
        BunStreamOpenResponse::Runtime(RuntimeFailure::UnknownOperation {
            capability: "lenso.bun-process@1",
            operation: request.operation,
        })
    }
}

/// Adapter-owned loopback JSON-RPC server for Bun consumer → Rust provider calls.
///
/// JSON-RPC framing, HTTP parsing, body limits, and connection shutdown are
/// delegated to jsonrpsee. This Adapter layer owns only exact handshakes,
/// bounded request admission, cancellation, and wire failure translation.
pub struct BunProviderServer {
    address: SocketAddr,
    state: Arc<ProviderState>,
    handle: Mutex<Option<ServerHandle>>,
    worker: Mutex<Option<ServerWorker>>,
}

struct ServerWorker {
    join: thread::JoinHandle<()>,
    finished: Receiver<()>,
}

#[derive(Debug)]
struct CancellationEntry {
    token: Option<Arc<AtomicBool>>,
    cancelled: bool,
}

type CancellationRegistry = Arc<Mutex<BTreeMap<u64, CancellationEntry>>>;
type RetiredRequestRegistry = Arc<Mutex<BTreeSet<u64>>>;

struct ProviderState {
    capability: &'static str,
    expected: Handshake,
    session: Mutex<Option<String>>,
    cancellations: CancellationRegistry,
    retired: RetiredRequestRegistry,
    admission: Admission,
    event_admissions: BTreeMap<String, Arc<ProviderEventQueue>>,
    handler: Arc<dyn BunProviderHandler>,
    streams: Mutex<BTreeMap<u64, Arc<ProviderStreamEntry>>>,
    retired_streams: Mutex<BTreeSet<u64>>,
}

#[derive(Debug)]
struct ProviderStreamEntry {
    stream: Arc<dyn BunProviderStream>,
    operation: String,
    permit: Mutex<Option<AdmissionPermit>>,
    inbound_credit: AtomicUsize,
    next_inbound_sequence: AtomicU64,
    next_outbound_sequence: AtomicU64,
    send_in_flight: AtomicBool,
    receive_in_flight: AtomicBool,
    peer_half_closed: AtomicBool,
    local_half_closed: AtomicBool,
    terminal_seen: AtomicBool,
    cancelled: AtomicBool,
}

#[derive(Clone, Debug)]
struct Admission {
    active: Arc<AtomicUsize>,
    limit: usize,
}

#[derive(Debug)]
struct AdmissionPermit {
    active: Arc<AtomicUsize>,
}

impl Admission {
    fn new(limit: usize) -> Self {
        Self {
            active: Arc::new(AtomicUsize::new(0)),
            limit: limit.max(1),
        }
    }

    fn try_acquire(&self) -> Option<AdmissionPermit> {
        let mut active = self.active.load(Ordering::Acquire);
        loop {
            if active >= self.limit {
                return None;
            }
            match self.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(AdmissionPermit {
                        active: self.active.clone(),
                    });
                }
                Err(current) => active = current,
            }
        }
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct QueuedEvent {
    request_id: u64,
    request: BunRequest,
}

#[derive(Debug, Default)]
struct ProviderEventQueueState {
    pending: VecDeque<QueuedEvent>,
    admitted: usize,
    draining: bool,
}

#[derive(Debug)]
struct ProviderEventQueue {
    capacity: usize,
    state: Mutex<ProviderEventQueueState>,
}

impl ProviderEventQueue {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            state: Mutex::new(ProviderEventQueueState::default()),
        }
    }

    fn try_enqueue(&self, event: QueuedEvent) -> Result<Option<bool>, RuntimeFailure> {
        let mut state = self.state.lock().map_err(|_| RuntimeFailure::Internal {
            detail: "Bun provider Event queue lock poisoned".to_owned(),
        })?;
        if state.admitted >= self.capacity {
            return Ok(None);
        }
        state.admitted += 1;
        state.pending.push_back(event);
        if state.draining {
            Ok(Some(false))
        } else {
            state.draining = true;
            Ok(Some(true))
        }
    }

    fn pop(&self) -> Result<Option<QueuedEvent>, RuntimeFailure> {
        self.state
            .lock()
            .map_err(|_| RuntimeFailure::Internal {
                detail: "Bun provider Event queue lock poisoned".to_owned(),
            })
            .map(|mut state| state.pending.pop_front())
    }

    fn complete(&self) -> Result<(), RuntimeFailure> {
        let mut state = self.state.lock().map_err(|_| RuntimeFailure::Internal {
            detail: "Bun provider Event queue lock poisoned".to_owned(),
        })?;
        state.admitted = state.admitted.saturating_sub(1);
        if state.pending.is_empty() {
            state.draining = false;
        }
        Ok(())
    }

    fn abort(&self) -> Result<Vec<QueuedEvent>, RuntimeFailure> {
        let mut state = self.state.lock().map_err(|_| RuntimeFailure::Internal {
            detail: "Bun provider Event queue lock poisoned".to_owned(),
        })?;
        state.admitted = 0;
        state.draining = false;
        Ok(state.pending.drain(..).collect())
    }
}

fn spawn_event_worker(
    queue: Arc<ProviderEventQueue>,
    handler: Arc<dyn BunProviderHandler>,
    cancellations: CancellationRegistry,
    retired: RetiredRequestRegistry,
) -> Result<(), RuntimeFailure> {
    thread::Builder::new()
        .name("lenso-bun-provider-event-worker".to_owned())
        .spawn(move || {
            loop {
                let event = match queue.pop() {
                    Ok(Some(event)) => event,
                    Ok(None) | Err(_) => return,
                };
                if !event.request.is_cancelled() {
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        handler.publish_event(event.request.clone())
                    }));
                }
                remove_request(&cancellations, &retired, event.request_id);
                if queue.complete().is_err() {
                    return;
                }
            }
        })
        .map(|_| ())
        .map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to start Bun provider Event worker: {error}"),
        })
}

impl std::fmt::Debug for BunProviderServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BunProviderServer")
            .field("address", &self.address)
            .field(
                "stopped",
                &self.handle.lock().map_or(true, |handle| {
                    handle.as_ref().is_none_or(ServerHandle::is_stopped)
                }),
            )
            .finish_non_exhaustive()
    }
}

impl BunProviderServer {
    /// Starts one bounded loopback JSON-RPC provider endpoint.
    pub fn json_rpc(
        descriptor: BunProviderDescriptor,
        max_frame_bytes: usize,
        queue_capacity: usize,
        handler: impl BunProviderHandler,
    ) -> Result<Self, RuntimeFailure> {
        let BunProviderDescriptor {
            capability_id,
            descriptor_version,
            operations,
            stream_operations,
            event_operations,
            event_bindings,
        } = descriptor;
        let max_frame_bytes = max_frame_bytes.max(1);
        let expected = handshake_for(
            [EndpointDescriptor {
                capability_id: capability_id.to_owned(),
                descriptor_version,
                operations,
                stream_operations,
                event_operations,
            }],
            max_frame_bytes,
        );
        let event_admissions = event_bindings
            .into_iter()
            .map(|binding| {
                (
                    binding.caller_instance,
                    Arc::new(ProviderEventQueue::new(binding.capacity)),
                )
            })
            .collect();
        let state = Arc::new(ProviderState {
            capability: capability_id,
            expected,
            session: Mutex::new(None),
            cancellations: Arc::new(Mutex::new(BTreeMap::new())),
            retired: Arc::new(Mutex::new(BTreeSet::new())),
            admission: Admission::new(queue_capacity),
            event_admissions,
            handler: Arc::new(handler),
            streams: Mutex::new(BTreeMap::new()),
            retired_streams: Mutex::new(BTreeSet::new()),
        });
        let module = provider_module(state.clone())?;
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (finished_sender, finished_receiver) = mpsc::sync_channel(1);
        let startup_result_sender = started_sender.clone();
        let worker = thread::Builder::new()
            .name("lenso-bun-provider-server".to_owned())
            .spawn(move || {
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| RuntimeFailure::Internal {
                        detail: format!("failed to create Bun provider runtime: {error}"),
                    })
                    .and_then(|runtime| {
                        runtime.block_on(async move {
                            let max_frame_bytes =
                                u32::try_from(max_frame_bytes).unwrap_or(u32::MAX);
                            let config = ServerConfig::builder()
                                .max_request_body_size(max_frame_bytes)
                                .max_response_body_size(max_frame_bytes)
                                .http_only()
                                .build();
                            let server = jsonrpsee::server::ServerBuilder::with_config(config)
                                .build(("127.0.0.1", 0))
                                .await
                                .map_err(|error| RuntimeFailure::ModuleFailure {
                                    detail: format!("failed to bind Bun provider bridge: {error}"),
                                })?;
                            let address =
                                server
                                    .local_addr()
                                    .map_err(|error| RuntimeFailure::Internal {
                                        detail: format!(
                                            "failed to read Bun provider bridge address: {error}"
                                        ),
                                    })?;
                            let handle = server.start(module);
                            let waiter = handle.clone();
                            startup_result_sender
                                .send(Ok((address, handle)))
                                .map_err(|_| RuntimeFailure::Internal {
                                    detail: "Bun provider startup receiver closed".to_owned(),
                                })?;
                            waiter.stopped().await;
                            Ok(())
                        })
                    });
                if let Err(error) = result {
                    let _ = started_sender.send(Err(error));
                }
                let _ = finished_sender.send(());
            })
            .map_err(|error| RuntimeFailure::Internal {
                detail: format!("failed to start Bun provider bridge: {error}"),
            })?;
        let (address, handle) =
            started_receiver
                .recv()
                .map_err(|_| RuntimeFailure::Internal {
                    detail: "Bun provider bridge stopped during startup".to_owned(),
                })??;

        Ok(Self {
            address,
            state,
            handle: Mutex::new(Some(handle)),
            worker: Mutex::new(Some(ServerWorker {
                join: worker,
                finished: finished_receiver,
            })),
        })
    }

    /// Returns the loopback address consumed by a Bun client.
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Stops admission, cancels active handlers, and waits for bounded server shutdown.
    pub fn shutdown(&self) {
        let handle = self.handle.lock().ok().and_then(|mut handle| handle.take());
        let Some(handle) = handle else { return };
        cancel_all(&self.state.cancellations);
        cancel_all_streams(&self.state);
        let _ = handle.stop();
        if let Ok(mut worker) = self.worker.lock()
            && let Some(worker) = worker.take()
            && worker
                .finished
                .recv_timeout(PROVIDER_SHUTDOWN_TIMEOUT)
                .is_ok()
        {
            let _ = worker.join.join();
        }
    }
}

impl Drop for BunProviderServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn provider_module(
    state: Arc<ProviderState>,
) -> Result<RpcModule<Arc<ProviderState>>, RuntimeFailure> {
    let mut module = RpcModule::new(state);
    module
        .register_method("lenso.handshake", |params, state, _| {
            let actual = decode_params(params.parse::<Value>()?)?;
            Ok::<_, ErrorObjectOwned>(handle_handshake(&actual, state))
        })
        .map_err(register_failure)?;
    module
        .register_blocking_method("lenso.request", |params, state, _| {
            let request = decode_params(params.parse::<Value>()?)?;
            Ok::<_, ErrorObjectOwned>(handle_request(request, &state))
        })
        .map_err(register_failure)?;
    module
        .register_blocking_method("lenso.event.publish", |params, state, _| {
            let event = decode_params(params.parse::<Value>()?)?;
            Ok::<_, ErrorObjectOwned>(handle_event(event, &state))
        })
        .map_err(register_failure)?;
    module
        .register_blocking_method("lenso.stream.open", |params, state, _| {
            let open = decode_params(params.parse::<Value>()?)?;
            Ok::<_, ErrorObjectOwned>(handle_stream_open(open, &state))
        })
        .map_err(register_failure)?;
    module
        .register_blocking_method("lenso.stream.send", |params, state, _| {
            let call = decode_params(params.parse::<Value>()?)?;
            Ok::<_, ErrorObjectOwned>(handle_stream_call(call, &state))
        })
        .map_err(register_failure)?;
    module
        .register_blocking_method("lenso.stream.receive", |params, state, _| {
            let call = decode_params(params.parse::<Value>()?)?;
            Ok::<_, ErrorObjectOwned>(handle_stream_call(call, &state))
        })
        .map_err(register_failure)?;
    module
        .register_blocking_method("lenso.stream.close_send", |params, state, _| {
            let call = decode_params(params.parse::<Value>()?)?;
            Ok::<_, ErrorObjectOwned>(handle_stream_call(call, &state))
        })
        .map_err(register_failure)?;
    module
        .register_method("lenso.cancel", |params, state, _| {
            let cancel = decode_params(params.parse::<Value>()?)?;
            handle_cancel(&cancel, state)?;
            Ok::<_, ErrorObjectOwned>(true)
        })
        .map_err(register_failure)?;
    module
        .register_method("lenso.stream.cancel", |params, state, _| {
            let cancel = decode_params(params.parse::<Value>()?)?;
            handle_stream_cancel(&cancel, state)?;
            Ok::<_, ErrorObjectOwned>(true)
        })
        .map_err(register_failure)?;
    Ok(module)
}

fn decode_params<T: DeserializeOwned>(params: Value) -> Result<T, ErrorObjectOwned> {
    let params = match params {
        Value::Array(mut values) if values.len() == 1 => values.pop().unwrap_or(Value::Null),
        params => params,
    };
    serde_json::from_value(params)
        .map_err(|error| ErrorObjectOwned::owned(-32602, "Invalid params", Some(error.to_string())))
}

fn register_failure(error: impl std::fmt::Display) -> RuntimeFailure {
    RuntimeFailure::Internal {
        detail: format!("failed to register Bun provider RPC method: {error}"),
    }
}

fn handle_handshake(actual: &Handshake, state: &ProviderState) -> HandshakeAck {
    let candidate = HandshakeAck {
        accepted: true,
        protocol_version: actual.protocol_version,
        value_profile: actual.value_profile.clone(),
        max_frame_bytes: actual.max_frame_bytes,
        endpoints: actual.endpoints.clone(),
        session: None,
    };
    if verify_handshake(&state.expected, &candidate, state.capability).is_err() {
        return HandshakeAck {
            accepted: false,
            ..candidate
        };
    }

    cancel_all_streams(state);
    if let Ok(mut retired_streams) = state.retired_streams.lock() {
        retired_streams.clear();
    }
    let session = format!(
        "lenso-bun-session-{}",
        NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed)
    );
    if let Ok(mut current) = state.session.lock() {
        *current = Some(session.clone());
    }
    if let Ok(mut retired) = state.retired.lock() {
        retired.clear();
    }
    HandshakeAck {
        session: Some(session),
        ..candidate
    }
}

fn handle_request(request: WireRequest, state: &ProviderState) -> WireOutcome {
    let expected_session = state
        .session
        .lock()
        .ok()
        .and_then(|current| current.clone());
    if expected_session.as_deref() != request.session.as_deref() {
        return runtime_outcome(&protocol_failure(state));
    }
    if let Err(error) = request.validate_extensions() {
        return runtime_outcome(&error);
    }
    let Some(endpoint) = state.expected.endpoints.first() else {
        return runtime_outcome(&protocol_failure(state));
    };
    if request.capability_id != endpoint.capability_id
        || !endpoint.operations.contains(&request.operation)
    {
        return WireOutcome::Runtime {
            failure: WireFailure::UnknownOperation {
                operation: request.operation,
            },
        };
    }
    let Some(_permit) = state.admission.try_acquire() else {
        return WireOutcome::Runtime {
            failure: WireFailure::ResourceExhausted {
                operation: request.operation,
            },
        };
    };
    let request_id = request.request_id;
    let cancellation = match register_request(
        &state.cancellations,
        &state.retired,
        request_id,
        state.capability,
    ) {
        Ok(cancellation) => cancellation,
        Err(error) => return runtime_outcome(&error),
    };
    let response = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        state
            .handler
            .invoke(BunRequest::from_wire(request, cancellation))
    }))
    .unwrap_or_else(|_| {
        BunResponse::Runtime(RuntimeFailure::ModuleFailure {
            detail: "Bun provider handler panicked".to_owned(),
        })
    });
    remove_request(&state.cancellations, &state.retired, request_id);
    match response {
        BunResponse::Success(value) => WireOutcome::Success { value },
        BunResponse::Domain(value) => WireOutcome::Domain { value },
        BunResponse::Runtime(error) => runtime_outcome(&error),
    }
}

fn handle_event(event: WireEventPublish, state: &ProviderState) -> WireOutcome {
    if !valid_session(event.session.as_deref(), state) {
        return runtime_outcome(&protocol_failure(state));
    }
    if let Err(error) = event.validate_extensions() {
        return runtime_outcome(&error);
    }
    let Some(endpoint) = state.expected.endpoints.first() else {
        return runtime_outcome(&protocol_failure(state));
    };
    if event.capability_id != endpoint.capability_id
        || !endpoint.event_operations.contains(&event.operation)
    {
        return WireOutcome::Runtime {
            failure: WireFailure::UnknownOperation {
                operation: event.operation,
            },
        };
    }
    let event_queue = match event_queue_for(state, &event) {
        Ok(queue) => queue,
        Err(error) => return runtime_outcome(&error),
    };
    let request_id = event.request_id;
    let operation = event.operation.clone();
    let cancellation = match register_request(
        &state.cancellations,
        &state.retired,
        request_id,
        state.capability,
    ) {
        Ok(cancellation) => cancellation,
        Err(error) => return runtime_outcome(&error),
    };
    let queued = QueuedEvent {
        request_id,
        request: BunRequest::from_wire(event, cancellation),
    };
    let should_start = match event_queue.try_enqueue(queued) {
        Ok(Some(should_start)) => should_start,
        Ok(None) => {
            remove_request(&state.cancellations, &state.retired, request_id);
            return WireOutcome::Runtime {
                failure: WireFailure::ResourceExhausted { operation },
            };
        }
        Err(error) => {
            remove_request(&state.cancellations, &state.retired, request_id);
            return runtime_outcome(&error);
        }
    };
    if should_start
        && let Err(error) = spawn_event_worker(
            event_queue.clone(),
            state.handler.clone(),
            state.cancellations.clone(),
            state.retired.clone(),
        )
    {
        if let Ok(aborted) = event_queue.abort() {
            for event in aborted {
                remove_request(&state.cancellations, &state.retired, event.request_id);
            }
        }
        return runtime_outcome(&error);
    }
    WireOutcome::Success { value: Value::Null }
}

fn event_queue_for(
    state: &ProviderState,
    event: &WireEventPublish,
) -> Result<Arc<ProviderEventQueue>, RuntimeFailure> {
    let caller_instance = event
        .caller_instance
        .as_deref()
        .filter(|caller| !caller.is_empty())
        .ok_or_else(|| RuntimeFailure::ProtocolViolation {
            capability: state.capability,
        })?;
    state
        .event_admissions
        .get(caller_instance)
        .cloned()
        .ok_or(RuntimeFailure::ProtocolViolation {
            capability: state.capability,
        })
}

#[derive(Deserialize)]
struct CancelRequest {
    request_id: u64,
    session: String,
}

fn handle_stream_open(open: WireStreamOpen, state: &ProviderState) -> WireStreamOutcome {
    if !valid_session(open.session.as_deref(), state) {
        return stream_runtime_outcome(&protocol_failure(state));
    }
    if let Err(error) = open.validate_extensions() {
        return stream_runtime_outcome(&error);
    }
    let Some(endpoint) = state.expected.endpoints.first() else {
        return stream_runtime_outcome(&protocol_failure(state));
    };
    if open.capability_id != endpoint.capability_id
        || !endpoint.stream_operations.contains(&open.operation)
        || open.credit == 0
    {
        return stream_runtime_outcome(&RuntimeFailure::UnknownOperation {
            capability: state.capability,
            operation: open.operation,
        });
    }
    let retired = match state.retired_streams.lock() {
        Ok(retired) => retired.contains(&open.stream_id),
        Err(_) => {
            return stream_runtime_outcome(&RuntimeFailure::Internal {
                detail: "Bun provider retired stream lock poisoned".to_owned(),
            });
        }
    };
    let active = match state.streams.lock() {
        Ok(streams) => streams.contains_key(&open.stream_id),
        Err(_) => {
            return stream_runtime_outcome(&RuntimeFailure::Internal {
                detail: "Bun provider stream lock poisoned".to_owned(),
            });
        }
    };
    if retired || active {
        return stream_runtime_outcome(&protocol_failure(state));
    }
    let Some(permit) = state.admission.try_acquire() else {
        return WireStreamOutcome::Runtime {
            failure: WireFailure::ResourceExhausted {
                operation: open.operation,
            },
        };
    };
    let request_id = open.request_id;
    let (stream_id, requested_credit) = (open.stream_id, open.credit);
    let operation = open.operation.clone();
    let cancellation = match register_request(
        &state.cancellations,
        &state.retired,
        request_id,
        state.capability,
    ) {
        Ok(cancellation) => cancellation,
        Err(error) => return stream_runtime_outcome(&error),
    };
    let response = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        state
            .handler
            .open_stream(BunRequest::from_wire(open, cancellation))
    }))
    .unwrap_or_else(|_| {
        BunStreamOpenResponse::Runtime(RuntimeFailure::ModuleFailure {
            detail: "Bun provider stream handler panicked".to_owned(),
        })
    });
    remove_request(&state.cancellations, &state.retired, request_id);
    match response {
        BunStreamOpenResponse::Success(stream) => {
            let credit = usize::try_from(requested_credit)
                .unwrap_or(usize::MAX)
                .min(crate::protocol::DEFAULT_STREAM_CREDIT as usize);
            let entry = Arc::new(ProviderStreamEntry {
                stream,
                operation,
                permit: Mutex::new(Some(permit)),
                inbound_credit: AtomicUsize::new(credit),
                next_inbound_sequence: AtomicU64::new(0),
                next_outbound_sequence: AtomicU64::new(0),
                send_in_flight: AtomicBool::new(false),
                receive_in_flight: AtomicBool::new(false),
                peer_half_closed: AtomicBool::new(false),
                local_half_closed: AtomicBool::new(false),
                terminal_seen: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
            });
            let Ok(mut streams) = state.streams.lock() else {
                entry.stream.cancel();
                return stream_runtime_outcome(&RuntimeFailure::Internal {
                    detail: "Bun provider stream lock poisoned".to_owned(),
                });
            };
            streams.insert(stream_id, entry);
            WireStreamOutcome::Opened {
                stream_id,
                credit: u32::try_from(credit).unwrap_or(u32::MAX),
            }
        }
        BunStreamOpenResponse::Domain(value) => WireStreamOutcome::Domain { value },
        BunStreamOpenResponse::Runtime(error) => stream_runtime_outcome(&error),
    }
}

fn handle_stream_call(call: WireStreamCall, state: &ProviderState) -> WireStreamOutcome {
    let (request_id, stream_id, session) = match &call {
        WireStreamCall::Send {
            request_id,
            stream_id,
            session,
            ..
        }
        | WireStreamCall::Receive {
            request_id,
            stream_id,
            session,
        }
        | WireStreamCall::CloseSend {
            request_id,
            stream_id,
            session,
        } => (*request_id, *stream_id, session.as_str()),
    };
    if !valid_session(Some(session), state) {
        return stream_runtime_outcome(&protocol_failure(state));
    }
    let entry = state
        .streams
        .lock()
        .ok()
        .and_then(|streams| streams.get(&stream_id).cloned());
    let Some(entry) = entry else {
        return stream_runtime_outcome(&protocol_failure(state));
    };
    if entry.cancelled.load(Ordering::Acquire) {
        return stream_runtime_outcome(&RuntimeFailure::Cancelled { request_id });
    }
    match call {
        WireStreamCall::Send {
            sequence, payload, ..
        } => {
            if entry.peer_half_closed.load(Ordering::Acquire)
                || entry.terminal_seen.load(Ordering::Acquire)
                || sequence != entry.next_inbound_sequence.load(Ordering::Acquire)
            {
                return stream_runtime_outcome(&protocol_failure(state));
            }
            if entry
                .send_in_flight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return WireStreamOutcome::Runtime {
                    failure: WireFailure::ResourceExhausted {
                        operation: entry.operation.clone(),
                    },
                };
            }
            let mut credit = entry.inbound_credit.load(Ordering::Acquire);
            loop {
                if credit == 0 {
                    entry.send_in_flight.store(false, Ordering::Release);
                    return WireStreamOutcome::Runtime {
                        failure: WireFailure::ResourceExhausted {
                            operation: entry.operation.clone(),
                        },
                    };
                }
                match entry.inbound_credit.compare_exchange_weak(
                    credit,
                    credit - 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(current) => credit = current,
                }
            }
            let action = entry.stream.send(payload);
            entry.send_in_flight.store(false, Ordering::Release);
            match action {
                BunStreamAction::Accepted => {
                    entry.next_inbound_sequence.fetch_add(1, Ordering::AcqRel);
                    let credit = entry.inbound_credit.fetch_add(1, Ordering::AcqRel) + 1;
                    WireStreamOutcome::Accepted {
                        credit: u32::try_from(credit).unwrap_or(u32::MAX),
                    }
                }
                BunStreamAction::Runtime(error) => {
                    if matches!(&error, RuntimeFailure::ResourceExhausted { .. }) {
                        entry.inbound_credit.fetch_add(1, Ordering::AcqRel);
                    } else {
                        entry.stream.cancel();
                        retire_stream(state, stream_id, &entry);
                    }
                    stream_runtime_outcome(&error)
                }
            }
        }
        WireStreamCall::Receive { .. } => {
            if entry
                .receive_in_flight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return WireStreamOutcome::Runtime {
                    failure: WireFailure::ResourceExhausted {
                        operation: format!("{}.receive", entry.operation),
                    },
                };
            }
            let result = match entry.stream.receive() {
                BunStreamReceive::Runtime(error) => stream_runtime_outcome(&error),
                BunStreamReceive::Event(BunStreamEvent::Message(payload)) => {
                    if entry.local_half_closed.load(Ordering::Acquire)
                        || entry.terminal_seen.load(Ordering::Acquire)
                    {
                        stream_runtime_outcome(&protocol_failure(state))
                    } else {
                        let sequence = entry.next_outbound_sequence.fetch_add(1, Ordering::AcqRel);
                        WireStreamOutcome::Event {
                            event: WireStreamEvent::Message { sequence, payload },
                        }
                    }
                }
                BunStreamReceive::Event(BunStreamEvent::PeerHalfClosed) => {
                    if entry.local_half_closed.swap(true, Ordering::AcqRel) {
                        stream_runtime_outcome(&protocol_failure(state))
                    } else {
                        WireStreamOutcome::Event {
                            event: WireStreamEvent::PeerHalfClosed,
                        }
                    }
                }
                BunStreamReceive::Event(BunStreamEvent::Terminal(outcome)) => {
                    if entry.terminal_seen.swap(true, Ordering::AcqRel) {
                        stream_runtime_outcome(&protocol_failure(state))
                    } else {
                        let outcome = match outcome {
                            Ok(()) => WireStreamTerminal::Success,
                            Err(value) => WireStreamTerminal::Domain { value },
                        };
                        WireStreamOutcome::Event {
                            event: WireStreamEvent::Terminal { outcome },
                        }
                    }
                }
            };
            entry.receive_in_flight.store(false, Ordering::Release);
            let terminal = matches!(
                &result,
                WireStreamOutcome::Event {
                    event: WireStreamEvent::Terminal { .. }
                }
            );
            let fatal_runtime = matches!(
                &result,
                WireStreamOutcome::Runtime { failure }
                    if !matches!(failure, WireFailure::ResourceExhausted { .. })
            );
            if fatal_runtime {
                entry.stream.cancel();
            }
            if terminal || fatal_runtime {
                retire_stream(state, stream_id, &entry);
            }
            result
        }
        WireStreamCall::CloseSend { .. } => {
            if entry.peer_half_closed.swap(true, Ordering::AcqRel)
                || entry.terminal_seen.load(Ordering::Acquire)
            {
                return stream_runtime_outcome(&protocol_failure(state));
            }
            match entry.stream.peer_half_closed() {
                BunStreamAction::Accepted => WireStreamOutcome::Accepted {
                    credit: entry.inbound_credit.load(Ordering::Acquire) as u32,
                },
                BunStreamAction::Runtime(error) => {
                    if matches!(&error, RuntimeFailure::ResourceExhausted { .. }) {
                        entry.peer_half_closed.store(false, Ordering::Release);
                    } else {
                        entry.stream.cancel();
                        retire_stream(state, stream_id, &entry);
                    }
                    stream_runtime_outcome(&error)
                }
            }
        }
    }
}

fn retire_stream(state: &ProviderState, stream_id: u64, entry: &ProviderStreamEntry) {
    entry.cancelled.store(true, Ordering::Release);
    if let Ok(mut streams) = state.streams.lock() {
        streams.remove(&stream_id);
    }
    if let Ok(mut retired) = state.retired_streams.lock() {
        remember_request_id(&mut retired, stream_id, MAX_RETIRED_REQUEST_IDS);
    }
    let _ = entry
        .permit
        .lock()
        .ok()
        .and_then(|mut permit| permit.take());
}

#[derive(Deserialize)]
struct StreamCancelRequest {
    stream_id: u64,
    session: String,
}

fn handle_stream_cancel(
    cancel: &StreamCancelRequest,
    state: &ProviderState,
) -> Result<(), ErrorObjectOwned> {
    if !valid_session(Some(cancel.session.as_str()), state) {
        return Err(invalid_session());
    }
    let entry = state
        .streams
        .lock()
        .ok()
        .and_then(|mut streams| streams.remove(&cancel.stream_id));
    if let Some(entry) = entry {
        entry.cancelled.store(true, Ordering::Release);
        entry.stream.cancel();
        if let Ok(mut retired) = state.retired_streams.lock() {
            remember_request_id(&mut retired, cancel.stream_id, MAX_RETIRED_REQUEST_IDS);
        }
        let _ = entry
            .permit
            .lock()
            .ok()
            .and_then(|mut permit| permit.take());
    }
    Ok(())
}

fn valid_session(session: Option<&str>, state: &ProviderState) -> bool {
    state
        .session
        .lock()
        .ok()
        .and_then(|current| current.clone())
        .as_deref()
        == session
}

fn handle_cancel(cancel: &CancelRequest, state: &ProviderState) -> Result<(), ErrorObjectOwned> {
    let expected_session = state
        .session
        .lock()
        .ok()
        .and_then(|current| current.clone());
    if expected_session.as_deref() != Some(cancel.session.as_str()) {
        return Err(invalid_session());
    }
    cancel_request(&state.cancellations, cancel.request_id);
    Ok(())
}

fn invalid_session() -> ErrorObjectOwned {
    ErrorObjectOwned::owned(-32602, "invalid Bun provider session", None::<Value>)
}

fn runtime_outcome(error: &RuntimeFailure) -> WireOutcome {
    WireOutcome::Runtime {
        failure: to_wire_failure(error),
    }
}

fn stream_runtime_outcome(error: &RuntimeFailure) -> WireStreamOutcome {
    WireStreamOutcome::Runtime {
        failure: to_wire_failure(error),
    }
}

fn register_request(
    registry: &CancellationRegistry,
    retired: &RetiredRequestRegistry,
    request_id: u64,
    capability: &'static str,
) -> Result<Arc<AtomicBool>, RuntimeFailure> {
    let token = Arc::new(AtomicBool::new(false));
    if retired
        .lock()
        .map_err(|_| RuntimeFailure::Internal {
            detail: "Bun provider retired request lock poisoned".to_owned(),
        })?
        .contains(&request_id)
    {
        return Err(RuntimeFailure::ProtocolViolation { capability });
    }
    let mut entries = registry.lock().map_err(|_| RuntimeFailure::Internal {
        detail: "Bun provider cancellation lock poisoned".to_owned(),
    })?;
    if entries
        .get(&request_id)
        .is_some_and(|entry| entry.token.is_some())
    {
        return Err(RuntimeFailure::ProtocolViolation { capability });
    }
    let was_cancelled = entries
        .get(&request_id)
        .is_some_and(|entry| entry.cancelled);
    if was_cancelled {
        token.store(true, Ordering::Release);
    }
    entries.insert(
        request_id,
        CancellationEntry {
            token: Some(token.clone()),
            cancelled: was_cancelled,
        },
    );
    Ok(token)
}

fn remove_request(
    registry: &CancellationRegistry,
    retired: &RetiredRequestRegistry,
    request_id: u64,
) {
    if let Ok(mut entries) = registry.lock() {
        entries.remove(&request_id);
    }
    if let Ok(mut retired) = retired.lock() {
        remember_request_id(&mut retired, request_id, MAX_RETIRED_REQUEST_IDS);
    }
}

fn cancel_request(registry: &CancellationRegistry, request_id: u64) {
    if let Ok(mut entries) = registry.lock() {
        if let Some(entry) = entries.get_mut(&request_id) {
            entry.cancelled = true;
            if let Some(token) = entry.token.as_ref() {
                token.store(true, Ordering::Release);
            }
            return;
        }
        while entries.len() >= MAX_CANCELLATION_ENTRIES {
            let Some(oldest) = entries.keys().next().copied() else {
                break;
            };
            entries.remove(&oldest);
        }
        entries.insert(
            request_id,
            CancellationEntry {
                token: None,
                cancelled: true,
            },
        );
    }
}

fn cancel_all(registry: &CancellationRegistry) {
    if let Ok(entries) = registry.lock() {
        for entry in entries.values() {
            if let Some(token) = entry.token.as_ref() {
                token.store(true, Ordering::Release);
            }
        }
    }
}

fn cancel_all_streams(state: &ProviderState) {
    let streams = state
        .streams
        .lock()
        .ok()
        .map(|mut streams| std::mem::take(&mut *streams))
        .unwrap_or_default();
    for (stream_id, entry) in streams {
        entry.cancelled.store(true, Ordering::Release);
        entry.stream.cancel();
        if let Ok(mut retired) = state.retired_streams.lock() {
            remember_request_id(&mut retired, stream_id, MAX_RETIRED_REQUEST_IDS);
        }
    }
}

fn remember_request_id(ids: &mut BTreeSet<u64>, request_id: u64, limit: usize) {
    while ids.len() >= limit {
        let Some(oldest) = ids.iter().next().copied() else {
            break;
        };
        ids.remove(&oldest);
    }
    ids.insert(request_id);
}

fn protocol_failure(state: &ProviderState) -> RuntimeFailure {
    RuntimeFailure::ProtocolViolation {
        capability: state.capability,
    }
}

#[cfg(test)]
mod tests {
    use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};

    use super::*;
    use crate::protocol::DEFAULT_STREAM_CREDIT;

    #[path = "server_extension_tests.rs"]
    mod extension_tests;

    #[derive(Debug)]
    struct TestHandler;

    #[derive(Debug)]
    struct EventTestHandler;

    impl BunProviderHandler for EventTestHandler {
        fn invoke(&self, request: BunRequest) -> BunResponse {
            BunResponse::Runtime(RuntimeFailure::UnknownOperation {
                capability: "example.notifications@1",
                operation: request.operation,
            })
        }

        fn publish_event(&self, _request: BunRequest) -> BunEventAction {
            BunEventAction::Accepted
        }
    }

    #[derive(Debug)]
    struct StreamTestHandler {
        stream: Arc<dyn BunProviderStream>,
    }

    impl BunProviderHandler for StreamTestHandler {
        fn invoke(&self, _request: BunRequest) -> BunResponse {
            BunResponse::Runtime(RuntimeFailure::UnknownOperation {
                capability: "example.chat@1",
                operation: "request".to_owned(),
            })
        }

        fn open_stream(&self, _request: BunRequest) -> BunStreamOpenResponse {
            BunStreamOpenResponse::Success(self.stream.clone())
        }
    }

    #[derive(Debug)]
    struct RuntimeTerminalStream;

    impl BunProviderStream for RuntimeTerminalStream {
        fn send(&self, _payload: Value) -> BunStreamAction {
            BunStreamAction::Accepted
        }

        fn receive(&self) -> BunStreamReceive {
            BunStreamReceive::Runtime(RuntimeFailure::Internal {
                detail: "terminal stream failure".to_owned(),
            })
        }

        fn cancel(&self) {}
    }

    #[derive(Debug)]
    struct RetryableSendStream {
        reject_once: AtomicBool,
    }

    impl BunProviderStream for RetryableSendStream {
        fn send(&self, _payload: Value) -> BunStreamAction {
            if self.reject_once.swap(false, Ordering::AcqRel) {
                BunStreamAction::Runtime(RuntimeFailure::ResourceExhausted {
                    capability: "example.chat@1",
                    operation: "chat".to_owned(),
                })
            } else {
                BunStreamAction::Accepted
            }
        }

        fn receive(&self) -> BunStreamReceive {
            BunStreamReceive::Event(BunStreamEvent::Terminal(Ok(())))
        }

        fn cancel(&self) {}
    }

    #[derive(Debug)]
    struct CancellationTrackingStream {
        cancelled: Arc<AtomicBool>,
    }

    impl BunProviderStream for CancellationTrackingStream {
        fn send(&self, _payload: Value) -> BunStreamAction {
            BunStreamAction::Accepted
        }

        fn receive(&self) -> BunStreamReceive {
            BunStreamReceive::Runtime(RuntimeFailure::ResourceExhausted {
                capability: "example.chat@1",
                operation: "chat.receive".to_owned(),
            })
        }

        fn cancel(&self) {
            self.cancelled.store(true, Ordering::Release);
        }
    }

    impl BunProviderHandler for TestHandler {
        fn invoke(&self, request: BunRequest) -> BunResponse {
            BunResponse::Success(request.payload)
        }
    }

    fn test_server(queue_capacity: usize) -> BunProviderServer {
        BunProviderServer::json_rpc(
            BunProviderDescriptor {
                capability_id: "example.greeting@1",
                descriptor_version: "1.0.0".to_owned(),
                operations: vec!["greet".to_owned()],
                stream_operations: Vec::new(),
                event_operations: Vec::new(),
                event_bindings: Vec::new(),
            },
            4096,
            queue_capacity,
            TestHandler,
        )
        .expect("server should start")
    }

    fn stream_server(queue_capacity: usize, stream: impl BunProviderStream) -> BunProviderServer {
        BunProviderServer::json_rpc(
            BunProviderDescriptor {
                capability_id: "example.chat@1",
                descriptor_version: "1.0.0".to_owned(),
                operations: vec!["chat".to_owned()],
                stream_operations: vec!["chat".to_owned()],
                event_operations: Vec::new(),
                event_bindings: Vec::new(),
            },
            4096,
            queue_capacity,
            StreamTestHandler {
                stream: Arc::new(stream),
            },
        )
        .expect("stream server should start")
    }

    #[test]
    fn event_publication_rejects_a_caller_absent_from_the_explicit_binding_set() {
        let descriptor = BunProviderDescriptor {
            capability_id: "example.notifications@1",
            descriptor_version: "1.0.0".to_owned(),
            operations: vec!["notify".to_owned()],
            stream_operations: Vec::new(),
            event_operations: vec!["notify".to_owned()],
            event_bindings: vec![BunEventBinding::new("bound-consumer", 1)],
        };
        let expected = handshake_for(
            [EndpointDescriptor {
                capability_id: descriptor.capability_id.to_owned(),
                descriptor_version: descriptor.descriptor_version.clone(),
                operations: descriptor.operations.clone(),
                stream_operations: descriptor.stream_operations.clone(),
                event_operations: descriptor.event_operations.clone(),
            }],
            4096,
        );
        let server = BunProviderServer::json_rpc(descriptor, 4096, 1, EventTestHandler)
            .expect("Event server should start");
        let client = HttpClientBuilder::default()
            .build(format!("http://{}", server.address()))
            .expect("client should build");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let accepted: HandshakeAck = runtime
            .block_on(client.request("lenso.handshake", rpc_params![expected]))
            .expect("exact Event handshake should pass");
        let outcome: WireOutcome = runtime
            .block_on(client.request(
                "lenso.event.publish",
                rpc_params![WireEventPublish {
                    request_id: 1,
                    capability_id: "example.notifications@1".to_owned(),
                    operation: "notify".to_owned(),
                    deadline_nanos: None,
                    caller_instance: Some("unbound-consumer".to_owned()),
                    session: accepted.session,
                    extensions: Vec::new(),
                    payload: serde_json::json!({ "message": "not bound" }),
                }],
            ))
            .expect("Event publication should return a wire outcome");
        server.shutdown();

        assert!(matches!(
            outcome,
            WireOutcome::Runtime {
                failure: WireFailure::ProtocolViolation { .. }
            }
        ));
    }

    fn open_stream(
        runtime: &tokio::runtime::Runtime,
        client: &jsonrpsee::http_client::HttpClient,
        stream_id: u64,
    ) -> String {
        let handshake = handshake_for(
            [EndpointDescriptor {
                capability_id: "example.chat@1".to_owned(),
                descriptor_version: "1.0.0".to_owned(),
                operations: vec!["chat".to_owned()],
                stream_operations: vec!["chat".to_owned()],
                event_operations: Vec::new(),
            }],
            4096,
        );
        let accepted: HandshakeAck = runtime
            .block_on(client.request("lenso.handshake", rpc_params![handshake]))
            .expect("exact stream handshake should pass");
        let session = accepted.session.expect("stream session should be assigned");
        let opened: WireStreamOutcome = runtime
            .block_on(client.request(
                "lenso.stream.open",
                rpc_params![WireStreamOpen {
                    request_id: stream_id,
                    stream_id,
                    capability_id: "example.chat@1".to_owned(),
                    operation: "chat".to_owned(),
                    deadline_nanos: None,
                    caller_instance: None,
                    session: Some(session.clone()),
                    extensions: Vec::new(),
                    credit: DEFAULT_STREAM_CREDIT,
                    payload: serde_json::json!({ "room": "test" }),
                }],
            ))
            .expect("stream should open");
        assert!(matches!(opened, WireStreamOutcome::Opened { .. }));
        session
    }

    #[test]
    fn bridge_allocates_a_loopback_address_and_stops() {
        let server = test_server(1);
        assert_eq!(server.address().ip(), std::net::Ipv4Addr::LOCALHOST);
        server.shutdown();
    }

    #[test]
    fn rejected_handshake_does_not_replace_the_active_session() {
        let state = ProviderState {
            capability: "example.greeting@1",
            expected: handshake_for(
                [EndpointDescriptor {
                    capability_id: "example.greeting@1".to_owned(),
                    descriptor_version: "1.0.0".to_owned(),
                    operations: vec!["greet".to_owned()],
                    stream_operations: Vec::new(),
                    event_operations: Vec::new(),
                }],
                4096,
            ),
            session: Mutex::new(None),
            cancellations: Arc::new(Mutex::new(BTreeMap::new())),
            retired: Arc::new(Mutex::new(BTreeSet::new())),
            admission: Admission::new(1),
            event_admissions: BTreeMap::new(),
            handler: Arc::new(TestHandler),
            streams: Mutex::new(BTreeMap::new()),
            retired_streams: Mutex::new(BTreeSet::new()),
        };
        let accepted = handle_handshake(&state.expected, &state);
        let session = accepted
            .session
            .expect("exact handshake should establish a session");
        let mut rejected = state.expected.clone();
        rejected.protocol_version += 1;
        assert!(!handle_handshake(&rejected, &state).accepted);
        assert_eq!(
            state.session.lock().expect("session lock").as_deref(),
            Some(session.as_str())
        );
    }

    #[test]
    fn malformed_handshake_does_not_corrupt_an_active_rpc_session() {
        let server = test_server(1);
        let client = HttpClientBuilder::default()
            .build(format!("http://{}", server.address()))
            .expect("client should build");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let handshake = handshake_for(
            [EndpointDescriptor {
                capability_id: "example.greeting@1".to_owned(),
                descriptor_version: "1.0.0".to_owned(),
                operations: vec!["greet".to_owned()],
                stream_operations: Vec::new(),
                event_operations: Vec::new(),
            }],
            4096,
        );
        let accepted: HandshakeAck = runtime
            .block_on(client.request("lenso.handshake", rpc_params![handshake]))
            .expect("exact handshake should pass");
        let session = accepted.session.expect("session should be assigned");
        let malformed = runtime.block_on(client.request::<HandshakeAck, _>(
            "lenso.handshake",
            rpc_params![serde_json::json!({ "protocol_version": "bad" })],
        ));
        assert!(malformed.is_err());
        let outcome: WireOutcome = runtime
            .block_on(client.request(
                "lenso.request",
                rpc_params![WireRequest {
                    request_id: 41,
                    capability_id: "example.greeting@1".to_owned(),
                    operation: "greet".to_owned(),
                    deadline_nanos: None,
                    caller_instance: None,
                    session: Some(session),
                    extensions: Vec::new(),
                    payload: serde_json::json!({ "name": "Ada" }),
                }],
            ))
            .expect("the original session should remain active");
        assert!(matches!(outcome, WireOutcome::Success { .. }));
        server.shutdown();
    }

    #[test]
    fn runtime_failure_retires_the_provider_stream() {
        let server = stream_server(1, RuntimeTerminalStream);
        let client = HttpClientBuilder::default()
            .build(format!("http://{}", server.address()))
            .expect("client should build");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let session = open_stream(&runtime, &client, 41);
        let first: WireStreamOutcome = runtime
            .block_on(client.request(
                "lenso.stream.receive",
                rpc_params![WireStreamCall::Receive {
                    request_id: 42,
                    stream_id: 41,
                    session: session.clone(),
                }],
            ))
            .expect("terminal Runtime Failure should be returned");
        assert!(matches!(first, WireStreamOutcome::Runtime { .. }));
        let late: WireStreamOutcome = runtime
            .block_on(client.request(
                "lenso.stream.receive",
                rpc_params![WireStreamCall::Receive {
                    request_id: 43,
                    stream_id: 41,
                    session,
                }],
            ))
            .expect("late receive should be rejected in-band");
        assert!(matches!(
            late,
            WireStreamOutcome::Runtime {
                failure: WireFailure::ProtocolViolation { .. }
            }
        ));
        server.shutdown();
    }

    #[test]
    fn resource_exhausted_send_can_retry_the_same_sequence() {
        let server = stream_server(
            1,
            RetryableSendStream {
                reject_once: AtomicBool::new(true),
            },
        );
        let client = HttpClientBuilder::default()
            .build(format!("http://{}", server.address()))
            .expect("client should build");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let session = open_stream(&runtime, &client, 51);
        let send = |request_id| WireStreamCall::Send {
            request_id,
            stream_id: 51,
            session: session.clone(),
            sequence: 0,
            payload: serde_json::json!({ "text": "retry" }),
        };
        let saturated: WireStreamOutcome = runtime
            .block_on(client.request("lenso.stream.send", rpc_params![send(52)]))
            .expect("saturation should be returned in-band");
        assert!(matches!(
            saturated,
            WireStreamOutcome::Runtime {
                failure: WireFailure::ResourceExhausted { .. }
            }
        ));
        let retried: WireStreamOutcome = runtime
            .block_on(client.request("lenso.stream.send", rpc_params![send(53)]))
            .expect("retry should be returned in-band");
        assert!(matches!(retried, WireStreamOutcome::Accepted { .. }));
        server.shutdown();
    }

    #[test]
    fn consumer_cancellation_retires_the_provider_stream_and_rejects_late_calls() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let server = stream_server(
            1,
            CancellationTrackingStream {
                cancelled: cancelled.clone(),
            },
        );
        let client = HttpClientBuilder::default()
            .build(format!("http://{}", server.address()))
            .expect("client should build");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let session = open_stream(&runtime, &client, 61);
        runtime
            .block_on(client.request::<bool, _>(
                "lenso.stream.cancel",
                rpc_params![serde_json::json!({
                    "stream_id": 61,
                    "session": session.clone(),
                })],
            ))
            .expect("stream cancellation should succeed");
        assert!(cancelled.load(Ordering::Acquire));

        let late: WireStreamOutcome = runtime
            .block_on(client.request(
                "lenso.stream.receive",
                rpc_params![WireStreamCall::Receive {
                    request_id: 62,
                    stream_id: 61,
                    session,
                }],
            ))
            .expect("late receive should be rejected in-band");
        assert!(matches!(
            late,
            WireStreamOutcome::Runtime {
                failure: WireFailure::ProtocolViolation { .. }
            }
        ));
        server.shutdown();
    }

    #[test]
    fn provider_shutdown_cancels_established_streams() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let server = stream_server(
            1,
            CancellationTrackingStream {
                cancelled: cancelled.clone(),
            },
        );
        let client = HttpClientBuilder::default()
            .build(format!("http://{}", server.address()))
            .expect("client should build");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let _session = open_stream(&runtime, &client, 71);

        server.shutdown();

        assert!(cancelled.load(Ordering::Acquire));
    }
}

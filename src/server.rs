use std::{
    collections::{BTreeMap, BTreeSet},
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
    EndpointDescriptor, Handshake, HandshakeAck, WireFailure, WireOutcome, WireRequest,
    handshake_for, to_wire_failure, verify_handshake,
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
    cancellation: Arc<AtomicBool>,
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

/// Host-side provider implementation used by the Bun consumer bridge.
pub trait BunProviderHandler: std::fmt::Debug + Send + Sync + 'static {
    /// Handles one already-framed, already-handshake-validated request.
    ///
    /// A bounded bridge can deliver cancellation while this method is running;
    /// cooperative handlers should poll `BunRequest::is_cancelled` and return
    /// a terminal Runtime Failure instead of retaining the request forever.
    fn invoke(&self, request: BunRequest) -> BunResponse;
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
    handler: Arc<dyn BunProviderHandler>,
}

#[derive(Debug)]
struct Admission {
    active: Arc<AtomicUsize>,
    limit: usize,
}

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
        } = descriptor;
        let max_frame_bytes = max_frame_bytes.max(1);
        let expected = handshake_for(
            [EndpointDescriptor {
                capability_id: capability_id.to_owned(),
                descriptor_version,
                operations,
            }],
            max_frame_bytes,
        );
        let state = Arc::new(ProviderState {
            capability: capability_id,
            expected,
            session: Mutex::new(None),
            cancellations: Arc::new(Mutex::new(BTreeMap::new())),
            retired: Arc::new(Mutex::new(BTreeSet::new())),
            admission: Admission::new(queue_capacity),
            handler: Arc::new(handler),
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
        .register_method("lenso.cancel", |params, state, _| {
            let cancel = decode_params(params.parse::<Value>()?)?;
            handle_cancel(&cancel, state)?;
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
        state.handler.invoke(BunRequest {
            request_id,
            capability_id: request.capability_id,
            operation: request.operation,
            deadline_nanos: request.deadline_nanos,
            caller_instance: request.caller_instance,
            payload: request.payload,
            cancellation,
        })
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

#[derive(Deserialize)]
struct CancelRequest {
    request_id: u64,
    session: String,
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

    #[derive(Debug)]
    struct TestHandler;

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
            },
            4096,
            queue_capacity,
            TestHandler,
        )
        .expect("server should start")
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
                }],
                4096,
            ),
            session: Mutex::new(None),
            cancellations: Arc::new(Mutex::new(BTreeMap::new())),
            retired: Arc::new(Mutex::new(BTreeSet::new())),
            admission: Admission::new(1),
            handler: Arc::new(TestHandler),
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
                    payload: serde_json::json!({ "name": "Ada" }),
                }],
            ))
            .expect("the original session should remain active");
        assert!(matches!(outcome, WireOutcome::Success { .. }));
        server.shutdown();
    }
}

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io::{BufRead, BufReader},
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    process::{Child, ChildStdin, ChildStdout},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError},
    },
    task::{Context, Poll},
    thread,
    time::{Duration, Instant},
};

use futures::{channel::oneshot, future::LocalBoxFuture};
use jsonrpsee::{
    core::client::{ClientT, Error as JsonRpcError},
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};
use lenso_kernel::{NativeStreamItem, NativeStreamSession, RuntimeFailure};
use serde_json::Value;

use crate::protocol::{
    FramedMessage, Handshake, HandshakeAck, WireEventPublish, WireOutcome, WireRequest,
    WireStreamCall, WireStreamOpen, WireStreamOutcome, encode_frame, from_wire_failure,
    protocol_violation, read_frame, verify_handshake, write_frame,
};

const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const PROCESS_STARTUP_TIMEOUT: Duration = Duration::from_secs(2);
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_CANCELLED_REQUEST_IDS: usize = 1024;
const MAX_RETIRED_REQUEST_IDS: usize = 1024;
mod framed;
mod json_rpc;
mod stream_session;

use framed::{FramedTransport, open_framed};
use json_rpc::{JsonRpcTransport, open_json_rpc};
pub(crate) use stream_session::TransportStreamSession;

const MAX_STREAM_CREDIT_WAITERS: usize = 64;
static NEXT_STREAM_CALL_ID: AtomicU64 = AtomicU64::new(1);

/// Wire implementations evaluated by the Bun Adapter evidence spike.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BunWire {
    /// Length-prefixed JSON messages on the child process stdin/stdout pipes.
    FramedStdio,
    /// JSON-RPC 2.0 request/response messages over a loopback HTTP server.
    JsonRpcHttp,
}

impl BunWire {
    pub(crate) const fn argument(self) -> &'static str {
        match self {
            Self::FramedStdio => "framed-stdio",
            Self::JsonRpcHttp => "json-rpc-http",
        }
    }
}

pub(crate) struct ProcessState {
    child: Mutex<Child>,
    capability: &'static str,
    alive: AtomicBool,
    monitor_started: AtomicBool,
    failure: Mutex<Option<RuntimeFailure>>,
    failure_handler: Mutex<Option<FailureHandler>>,
    exit_waiters: Mutex<Vec<oneshot::Sender<()>>>,
}

type FailureHandler = Box<dyn Fn(RuntimeFailure) + Send + Sync>;

impl std::fmt::Debug for ProcessState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessState")
            .field("capability", &self.capability)
            .field("alive", &self.is_alive())
            .field("failure", &self.failure())
            .finish_non_exhaustive()
    }
}

impl ProcessState {
    pub(crate) fn start(child: Child, capability: &'static str) -> Arc<Self> {
        Arc::new(Self {
            child: Mutex::new(child),
            capability,
            alive: AtomicBool::new(true),
            monitor_started: AtomicBool::new(false),
            failure: Mutex::new(None),
            failure_handler: Mutex::new(None),
            exit_waiters: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn start_monitor(self: &Arc<Self>) {
        if self.monitor_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let monitor = Arc::downgrade(self);
        thread::Builder::new()
            .name("lenso-bun-process-monitor".to_owned())
            .spawn(move || {
                loop {
                    let Some(monitor) = monitor.upgrade() else {
                        break;
                    };
                    let result = monitor
                        .child
                        .lock()
                        .map_err(|_| "child process lock poisoned".to_owned())
                        .and_then(|mut child| {
                            child
                                .try_wait()
                                .map_err(|error| format!("Bun process wait failed: {error}"))
                        });
                    match result {
                        Ok(Some(status)) => {
                            monitor.mark_dead(RuntimeFailure::ModuleFailure {
                                detail: format!("Bun process exited with status {status}"),
                            });
                            break;
                        }
                        Ok(None) => thread::sleep(Duration::from_millis(10)),
                        Err(detail) => {
                            monitor.mark_dead(RuntimeFailure::ModuleFailure { detail });
                            break;
                        }
                    }
                }
            })
            .expect("Bun process monitor thread should start");
    }

    pub(crate) fn take_stdin(&self) -> Result<ChildStdin, RuntimeFailure> {
        self.child
            .lock()
            .map_err(|_| RuntimeFailure::Internal {
                detail: "Bun child process lock poisoned".to_owned(),
            })?
            .stdin
            .take()
            .ok_or_else(|| RuntimeFailure::Internal {
                detail: "Bun child process stdin was unavailable".to_owned(),
            })
    }

    pub(crate) fn take_stdout(&self) -> Result<ChildStdout, RuntimeFailure> {
        self.child
            .lock()
            .map_err(|_| RuntimeFailure::Internal {
                detail: "Bun child process lock poisoned".to_owned(),
            })?
            .stdout
            .take()
            .ok_or_else(|| RuntimeFailure::Internal {
                detail: "Bun child process stdout was unavailable".to_owned(),
            })
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    pub(crate) fn failure(&self) -> Option<RuntimeFailure> {
        self.failure.lock().ok().and_then(|failure| failure.clone())
    }

    pub(crate) fn failure_or_exit(&self) -> Option<RuntimeFailure> {
        if let Some(failure) = self.failure() {
            return Some(failure);
        }
        let result = self
            .child
            .lock()
            .map_err(|_| RuntimeFailure::Internal {
                detail: "Bun child process lock poisoned".to_owned(),
            })
            .and_then(|mut child| {
                child
                    .try_wait()
                    .map_err(|error| RuntimeFailure::ModuleFailure {
                        detail: format!("Bun process wait failed: {error}"),
                    })
            });
        match result {
            Ok(Some(status)) => {
                let failure = RuntimeFailure::ModuleFailure {
                    detail: format!("Bun process exited with status {status}"),
                };
                self.mark_dead(failure.clone());
                Some(failure)
            }
            Ok(None) => self.failure(),
            Err(error) => {
                self.mark_dead(error.clone());
                Some(error)
            }
        }
    }

    fn failure_or_exit_within(&self, timeout: Duration) -> Option<RuntimeFailure> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(failure) = self.failure_or_exit() {
                return Some(failure);
            }
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(2));
        }
    }

    pub(crate) fn set_failure_handler(
        &self,
        handler: impl Fn(RuntimeFailure) + Send + Sync + 'static,
    ) {
        let handler: FailureHandler = Box::new(handler);
        if let Ok(mut registered) = self.failure_handler.lock() {
            *registered = Some(handler);
        }
        if !self.is_alive()
            && let (Some(failure), Ok(registered)) = (self.failure(), self.failure_handler.lock())
            && let Some(handler) = registered.as_ref()
        {
            handler(failure);
        }
    }

    pub(crate) fn subscribe_exit(&self) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        if self.is_alive()
            && let Ok(mut waiters) = self.exit_waiters.lock()
            && self.is_alive()
        {
            waiters.push(sender);
            return receiver;
        }
        let _ = sender.send(());
        receiver
    }

    pub(crate) fn stop(&self) {
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
            let deadline = Instant::now() + PROCESS_STOP_TIMEOUT;
            loop {
                match child.try_wait() {
                    Ok(Some(_)) | Err(_) => break,
                    Ok(None) if Instant::now() >= deadline => break,
                    Ok(None) => thread::sleep(Duration::from_millis(10)),
                }
            }
        }
    }

    fn mark_dead(&self, failure: RuntimeFailure) {
        if self.alive.swap(false, Ordering::AcqRel) {
            if let Ok(mut stored) = self.failure.lock() {
                *stored = Some(failure.clone());
            }
            if let Ok(handler) = self.failure_handler.lock()
                && let Some(handler) = handler.as_ref()
            {
                handler(failure);
            }
            if let Ok(mut waiters) = self.exit_waiters.lock() {
                for waiter in waiters.drain(..) {
                    let _ = waiter.send(());
                }
            }
        }
    }
}

impl Drop for ProcessState {
    fn drop(&mut self) {
        self.stop();
    }
}

type WireResult = Result<WireOutcome, RuntimeFailure>;
type StreamWireResult = Result<WireStreamOutcome, RuntimeFailure>;
type PendingResponses = Arc<Mutex<BTreeMap<u64, oneshot::Sender<WireResult>>>>;
type PendingStreamResponses = Arc<Mutex<BTreeMap<u64, oneshot::Sender<StreamWireResult>>>>;
type CapabilityIds = Arc<BTreeMap<String, &'static str>>;

#[derive(Clone, Debug)]
pub(crate) enum TransportClient {
    Framed(Arc<FramedTransport>),
    JsonRpc(Arc<JsonRpcTransport>),
}

impl TransportClient {
    pub(crate) fn session(&self) -> String {
        match self {
            Self::Framed(transport) => transport.session.clone(),
            Self::JsonRpc(transport) => transport.session.clone(),
        }
    }

    pub(crate) fn request(&self, request: WireRequest) -> Result<WireCall, RuntimeFailure> {
        match self {
            Self::Framed(transport) => transport.request(request),
            Self::JsonRpc(transport) => transport.request(request),
        }
    }

    pub(crate) fn publish_event(
        &self,
        mut event: WireEventPublish,
    ) -> Result<WireCall, RuntimeFailure> {
        event.session = Some(self.session());
        match self {
            Self::Framed(transport) => transport.event(event),
            Self::JsonRpc(transport) => transport.event(event),
        }
    }

    pub(crate) fn open_stream(
        &self,
        mut request: WireStreamOpen,
    ) -> Result<StreamCall, RuntimeFailure> {
        request.session = Some(self.session());
        let request_id = request.request_id;
        let stream_id = request.stream_id;
        let session = request.session.clone().unwrap_or_default();
        let capability_name = request.capability_id.clone();
        let operation = request.operation.clone();
        match self {
            Self::Framed(transport) => transport.stream_request(
                FramedMessage::StreamOpen(request),
                request_id,
                stream_id,
                session,
                &capability_name,
                &operation,
            ),
            Self::JsonRpc(transport) => transport.stream_request(
                request_id,
                stream_id,
                session,
                "lenso.stream.open",
                serde_json::to_value(request).map_err(|_| protocol_violation(None))?,
                "open",
            ),
        }
    }

    pub(crate) fn stream_call(
        &self,
        request: WireStreamCall,
        stream_id: u64,
        session: &str,
        capability: &'static str,
        operation: &str,
    ) -> Result<StreamCall, RuntimeFailure> {
        let request_id = match &request {
            WireStreamCall::Send { request_id, .. }
            | WireStreamCall::Receive { request_id, .. }
            | WireStreamCall::CloseSend { request_id, .. } => *request_id,
        };
        match self {
            Self::Framed(transport) => transport.stream_request(
                FramedMessage::StreamCall(request),
                request_id,
                stream_id,
                session.to_owned(),
                capability,
                operation,
            ),
            Self::JsonRpc(transport) => {
                let (method, operation_name) = match &request {
                    WireStreamCall::Send { .. } => ("lenso.stream.send", "send"),
                    WireStreamCall::Receive { .. } => ("lenso.stream.receive", "receive"),
                    WireStreamCall::CloseSend { .. } => ("lenso.stream.close_send", "close_send"),
                };
                transport.stream_request(
                    request_id,
                    stream_id,
                    session.to_owned(),
                    method,
                    serde_json::to_value(request).map_err(|_| protocol_violation(None))?,
                    operation_name,
                )
            }
        }
    }

    pub(crate) fn cancel(&self, request_id: u64) {
        match self {
            Self::Framed(transport) => transport.cancel(request_id),
            Self::JsonRpc(transport) => transport.cancel(request_id),
        }
    }

    pub(crate) fn cancel_stream(&self, stream_id: u64, session: &str) {
        match self {
            Self::Framed(transport) => transport.cancel_stream(stream_id, session),
            Self::JsonRpc(transport) => transport.cancel_stream(stream_id, session),
        }
    }

    pub(crate) fn cancel_stream_call(&self, request_id: u64, stream_id: u64, session: &str) {
        match self {
            Self::Framed(transport) => transport.cancel_stream_call(request_id, stream_id, session),
            Self::JsonRpc(transport) => {
                transport.cancel_stream_call(request_id, stream_id, session);
            }
        }
    }

    pub(crate) fn exit_waiter(&self) -> oneshot::Receiver<()> {
        match self {
            Self::Framed(transport) => transport.process.subscribe_exit(),
            Self::JsonRpc(transport) => transport.process.subscribe_exit(),
        }
    }

    pub(crate) fn shutdown(&self) {
        match self {
            Self::Framed(transport) => transport.shutdown(),
            Self::JsonRpc(transport) => transport.shutdown(),
        }
    }
}

/// A local future whose Drop implementation sends the wire cancellation.
#[derive(Debug)]
pub(crate) struct WireCall {
    request_id: u64,
    transport: TransportClient,
    receiver: Option<oneshot::Receiver<WireResult>>,
}

/// A local stream future whose Drop implementation cancels the stream call.
#[derive(Debug)]
pub(crate) struct StreamCall {
    request_id: u64,
    stream_id: u64,
    session: String,
    transport: TransportClient,
    receiver: Option<oneshot::Receiver<StreamWireResult>>,
}

impl StreamCall {
    fn new(
        request_id: u64,
        stream_id: u64,
        session: String,
        transport: TransportClient,
        receiver: oneshot::Receiver<StreamWireResult>,
    ) -> Self {
        Self {
            request_id,
            stream_id,
            session,
            transport,
            receiver: Some(receiver),
        }
    }
}

impl Future for StreamCall {
    type Output = StreamWireResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let receiver = this
            .receiver
            .as_mut()
            .expect("a stream call receiver is present until completion");
        match Pin::new(receiver).poll(context) {
            Poll::Ready(Ok(result)) => {
                this.receiver.take();
                Poll::Ready(result)
            }
            Poll::Ready(Err(_)) => {
                this.receiver.take();
                Poll::Ready(Err(RuntimeFailure::ModuleFailure {
                    detail: "Bun stream response channel closed".to_owned(),
                }))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for StreamCall {
    fn drop(&mut self) {
        if self.receiver.take().is_some() {
            self.transport
                .cancel_stream_call(self.request_id, self.stream_id, &self.session);
        }
    }
}

impl WireCall {
    fn new(
        request_id: u64,
        transport: TransportClient,
        receiver: oneshot::Receiver<WireResult>,
    ) -> Self {
        Self {
            request_id,
            transport,
            receiver: Some(receiver),
        }
    }
}

impl Future for WireCall {
    type Output = WireResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let receiver = this
            .receiver
            .as_mut()
            .expect("a wire call receiver is present until completion");
        match Pin::new(receiver).poll(context) {
            Poll::Ready(Ok(result)) => {
                this.receiver.take();
                Poll::Ready(result)
            }
            Poll::Ready(Err(_)) => {
                this.receiver.take();
                Poll::Ready(Err(RuntimeFailure::ModuleFailure {
                    detail: "Bun transport response channel closed".to_owned(),
                }))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for WireCall {
    fn drop(&mut self) {
        if self.receiver.take().is_some() {
            self.transport.cancel(self.request_id);
        }
    }
}

fn capability_id(capabilities: &CapabilityIds, value: &str) -> &'static str {
    capabilities
        .get(value)
        .copied()
        .unwrap_or("lenso.bun-process@1")
}

fn remember_request_id(ids: &Mutex<BTreeSet<u64>>, request_id: u64) {
    if let Ok(mut ids) = ids.lock() {
        while ids.len() >= MAX_RETIRED_REQUEST_IDS {
            let Some(oldest) = ids.iter().next().copied() else {
                break;
            };
            ids.remove(&oldest);
        }
        ids.insert(request_id);
    }
}

fn build_json_rpc_client(
    address: SocketAddr,
    max_frame_bytes: usize,
    queue_capacity: usize,
) -> Result<HttpClient, RuntimeFailure> {
    let max_frame_bytes = u32::try_from(max_frame_bytes).unwrap_or(u32::MAX);
    HttpClientBuilder::default()
        .max_request_size(max_frame_bytes)
        .max_response_size(max_frame_bytes)
        .request_timeout(HTTP_CONNECT_TIMEOUT)
        .max_concurrent_requests(queue_capacity.max(1).saturating_add(1))
        .build(format!("http://{address}"))
        .map_err(|error| RuntimeFailure::ModuleFailure {
            detail: format!("failed to build Bun JSON-RPC client: {error}"),
        })
}

fn json_rpc_runtime() -> Result<tokio::runtime::Runtime, RuntimeFailure> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to start Bun JSON-RPC runtime: {error}"),
        })
}

fn json_rpc_failure(
    operation: &str,
    error: &JsonRpcError,
    capability: &'static str,
) -> RuntimeFailure {
    let oversized = match &error {
        JsonRpcError::Transport(error) => error
            .downcast_ref::<jsonrpsee::http_client::transport::Error>()
            .is_some_and(|error| {
                matches!(
                    error,
                    jsonrpsee::http_client::transport::Error::RequestTooLarge
                        | jsonrpsee::http_client::transport::Error::Rejected { status_code: 413 }
                )
            }),
        _ => false,
    };
    if oversized {
        return RuntimeFailure::ProtocolViolation { capability };
    }
    RuntimeFailure::ModuleFailure {
        detail: format!("Bun JSON-RPC {operation} failed: {error}"),
    }
}

pub(crate) fn spawn_process(
    mut command: std::process::Command,
    capability: &'static str,
) -> Result<Arc<ProcessState>, RuntimeFailure> {
    command.stderr(std::process::Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|error| RuntimeFailure::ModuleFailure {
            detail: format!("failed to start Bun child process: {error}"),
        })?;
    if let Some(stderr) = child.stderr.take() {
        thread::Builder::new()
            .name("lenso-bun-process-stderr".to_owned())
            .spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines() {
                    if line.is_err() {
                        break;
                    }
                }
            })
            .expect("Bun stderr drain thread should start");
    }
    Ok(ProcessState::start(child, capability))
}

pub(crate) fn open_transport(
    process: &Arc<ProcessState>,
    wire: BunWire,
    expected: &Handshake,
    queue_capacity: usize,
    event_queue_capacity: usize,
    capability_ids: CapabilityIds,
) -> Result<TransportClient, RuntimeFailure> {
    match wire {
        BunWire::FramedStdio => open_framed(
            process,
            expected,
            queue_capacity,
            event_queue_capacity,
            capability_ids,
        ),
        BunWire::JsonRpcHttp => {
            let stdout = process.take_stdout()?;
            process.start_monitor();
            let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
            thread::Builder::new()
                .name("lenso-bun-json-rpc-readiness".to_owned())
                .spawn(move || {
                    let _ = ready_sender.send(read_ready_address(stdout));
                })
                .map_err(|error| RuntimeFailure::Internal {
                    detail: format!("failed to start Bun readiness reader: {error}"),
                })?;
            let address = match ready_receiver.recv_timeout(PROCESS_STARTUP_TIMEOUT) {
                Ok(Ok(address)) => address,
                Ok(Err(error)) => {
                    return Err(process.failure_or_exit().unwrap_or(error));
                }
                Err(_) => {
                    process.stop();
                    return Err(RuntimeFailure::ModuleFailure {
                        detail: "Bun JSON-RPC process readiness timed out".to_owned(),
                    });
                }
            };
            open_json_rpc(
                process,
                address,
                expected,
                queue_capacity,
                event_queue_capacity,
                capability_ids,
            )
        }
    }
}

fn read_ready_address(stdout: ChildStdout) -> Result<SocketAddr, RuntimeFailure> {
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    for _ in 0..32 {
        line.clear();
        reader
            .read_line(&mut line)
            .map_err(|error| RuntimeFailure::ModuleFailure {
                detail: format!("failed to read Bun JSON-RPC readiness: {error}"),
            })?;
        let Some(port) = line.strip_prefix("LENSO_READY ") else {
            continue;
        };
        let port: u16 = port.trim().parse().map_err(|_| protocol_violation(None))?;
        let address = ("127.0.0.1", port)
            .to_socket_addrs()
            .map_err(|_| RuntimeFailure::ModuleFailure {
                detail: "Bun JSON-RPC readiness address could not be resolved".to_owned(),
            })?
            .next()
            .ok_or_else(|| RuntimeFailure::ModuleFailure {
                detail: "Bun JSON-RPC readiness address was empty".to_owned(),
            })?;
        return Ok(address);
    }
    Err(RuntimeFailure::ModuleFailure {
        detail: "Bun JSON-RPC process did not announce readiness".to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn process_state_reports_exit_to_waiters() {
        let mut command = std::process::Command::new("sh");
        command.args(["-c", "exit 7"]);
        let process = spawn_process(command, "example.greeting@1").expect("process should start");
        process.start_monitor();
        let waiter = process.subscribe_exit();
        std::thread::sleep(Duration::from_millis(20));
        assert!(!process.alive.load(Ordering::Acquire));
        futures::executor::block_on(waiter).expect("exit should wake the waiter");
        assert!(matches!(
            process.failure(),
            Some(RuntimeFailure::ModuleFailure { .. })
        ));
    }

    #[test]
    fn dropping_an_unactivated_transport_stops_its_process() {
        let mut command = std::process::Command::new("sh");
        command.args(["-c", "sleep 30"]);
        let process = spawn_process(command, "example.greeting@1").expect("process should start");
        let (sender, _receiver) = mpsc::sync_channel(1);
        let (control_sender, _control_receiver) = mpsc::sync_channel(2);
        let transport = FramedTransport {
            process: process.clone(),
            sender,
            event_sender: mpsc::sync_channel(1).0,
            control_sender,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            event_pending: Arc::new(Mutex::new(BTreeMap::new())),
            stream_pending: Arc::new(Mutex::new(BTreeMap::new())),
            cancelled: Arc::new(Mutex::new(BTreeSet::new())),
            stream_cancelled: Arc::new(Mutex::new(BTreeSet::new())),
            retired: Arc::new(Mutex::new(BTreeSet::new())),
            stream_retired: Arc::new(Mutex::new(BTreeSet::new())),
            max_frame_bytes: 4096,
            admission_capacity: 1,
            event_admission_capacity: 1,
            stream_admission_capacity: 2,
            session: String::new(),
            capability: "example.greeting@1",
            capability_ids: Arc::new(BTreeMap::from([(
                "example.greeting@1".to_owned(),
                "example.greeting@1",
            )])),
        };

        drop(transport);

        assert!(matches!(
            process.failure_or_exit(),
            Some(RuntimeFailure::ModuleFailure { .. })
        ));
    }
}

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    io::{BufRead, BufReader},
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    process::{Child, ChildStderr, ChildStdin, ChildStdout},
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
const MAX_PROCESS_DIAGNOSTIC_LINES: usize = 32;
const MAX_PROCESS_DIAGNOSTIC_CHARS: usize = 512;
const MAX_PROCESS_DIAGNOSTIC_BYTES: usize = MAX_PROCESS_DIAGNOSTIC_CHARS;
const TRUNCATED_PROCESS_DIAGNOSTIC: &str = "[suppressed truncated child output]";
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
    diagnostics: Mutex<VecDeque<String>>,
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
            diagnostics: Mutex::new(VecDeque::new()),
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
                            monitor.mark_dead(RuntimeFailure::PluginFailure {
                                detail: format!("Bun process exited with status {status}"),
                            });
                            break;
                        }
                        Ok(None) => thread::sleep(Duration::from_millis(10)),
                        Err(detail) => {
                            monitor.mark_dead(RuntimeFailure::PluginFailure { detail });
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

    fn take_stderr(&self) -> Option<ChildStderr> {
        self.child.lock().ok()?.stderr.take()
    }

    fn record_diagnostic(&self, stream: &str, line: &str) {
        let line = redact_diagnostic_line(line);
        let line = line
            .chars()
            .filter(|character| !character.is_control() || *character == '\t')
            .take(MAX_PROCESS_DIAGNOSTIC_CHARS)
            .collect::<String>();
        if line.is_empty() {
            return;
        }
        if let Ok(mut diagnostics) = self.diagnostics.lock() {
            while diagnostics.len() >= MAX_PROCESS_DIAGNOSTIC_LINES {
                diagnostics.pop_front();
            }
            diagnostics.push_back(format!("{stream}: {line}"));
        }
    }

    fn diagnostic_tail(&self) -> String {
        self.diagnostics.lock().map_or_else(
            |_| String::new(),
            |diagnostics| diagnostics.iter().cloned().collect::<Vec<_>>().join(" | "),
        )
    }

    fn decorate_failure(&self, failure: RuntimeFailure) -> RuntimeFailure {
        let RuntimeFailure::PluginFailure { detail } = failure else {
            return failure;
        };
        let diagnostics = self.diagnostic_tail();
        RuntimeFailure::PluginFailure {
            detail: if diagnostics.is_empty() {
                detail
            } else {
                format!("{detail}; child output: {diagnostics}")
            },
        }
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    pub(crate) fn failure(&self) -> Option<RuntimeFailure> {
        let failure = self
            .failure
            .lock()
            .ok()
            .and_then(|failure| failure.clone())?;
        Some(self.decorate_failure(failure))
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
                    .map_err(|error| RuntimeFailure::PluginFailure {
                        detail: format!("Bun process wait failed: {error}"),
                    })
            });
        match result {
            Ok(Some(status)) => {
                self.mark_dead(RuntimeFailure::PluginFailure {
                    detail: format!("Bun process exited with status {status}"),
                });
                self.failure()
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

fn redact_diagnostic_line(line: &str) -> String {
    const SENSITIVE_MARKERS: &[&str] = &[
        "authorization",
        "bearer ",
        "basic ",
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "api-key",
        "apikey",
        "cookie",
        "set-cookie",
    ];
    let lowercase = line.to_ascii_lowercase();
    let url_user_info = lowercase.match_indices("://").any(|(scheme, _)| {
        lowercase[scheme + 3..]
            .split(|character: char| {
                matches!(character, '/' | '?' | '#') || character.is_ascii_whitespace()
            })
            .next()
            .is_some_and(|authority| authority.contains('@'))
    });
    if url_user_info
        || SENSITIVE_MARKERS
            .iter()
            .any(|marker| lowercase.contains(marker))
    {
        "[redacted sensitive child output]".to_owned()
    } else {
        line.to_owned()
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

    pub(crate) fn supports_managed_lifecycle(&self) -> bool {
        matches!(self, Self::JsonRpc(transport) if transport.managed_lifecycle)
    }

    pub(crate) fn request(&self, request: WireRequest) -> Result<WireCall, RuntimeFailure> {
        match self {
            Self::Framed(transport) => transport.request(request),
            Self::JsonRpc(transport) => transport.request(request),
        }
    }

    pub(crate) fn activate(
        &self,
        payload: Value,
    ) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        match self {
            Self::Framed(_) => Box::pin(futures::future::ready(Err(
                RuntimeFailure::InvalidResolvedPlan {
                    detail:
                        "Bun Plugin lifecycle and dependency imports require the json-rpc-http wire"
                            .to_owned(),
                },
            ))),
            Self::JsonRpc(transport) => transport.activate(payload),
        }
    }

    pub(crate) fn deactivate(&self) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        match self {
            Self::JsonRpc(transport) if transport.managed_lifecycle => transport.deactivate(),
            Self::Framed(_) | Self::JsonRpc(_) => Box::pin(futures::future::ready(Ok(()))),
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
                Poll::Ready(Err(RuntimeFailure::PluginFailure {
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
                Poll::Ready(Err(RuntimeFailure::PluginFailure {
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
    max_concurrent_requests: usize,
) -> Result<HttpClient, RuntimeFailure> {
    let max_frame_bytes = u32::try_from(max_frame_bytes).unwrap_or(u32::MAX);
    HttpClientBuilder::default()
        .max_request_size(max_frame_bytes)
        .max_response_size(max_frame_bytes)
        .request_timeout(HTTP_CONNECT_TIMEOUT)
        .max_concurrent_requests(max_concurrent_requests.max(1))
        .build(format!("http://{address}"))
        .map_err(|error| RuntimeFailure::PluginFailure {
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
    RuntimeFailure::PluginFailure {
        detail: format!("Bun JSON-RPC {operation} failed: {error}"),
    }
}

pub(crate) fn spawn_process(
    mut command: std::process::Command,
    capability: &'static str,
) -> Result<Arc<ProcessState>, RuntimeFailure> {
    command.stderr(std::process::Stdio::piped());
    let child = command
        .spawn()
        .map_err(|error| RuntimeFailure::PluginFailure {
            detail: format!("failed to start Bun child process: {error}"),
        })?;
    let process = ProcessState::start(child, capability);
    if let Some(stderr) = process.take_stderr() {
        spawn_output_drain(
            Arc::downgrade(&process),
            "stderr",
            BufReader::new(stderr),
            "lenso-bun-process-stderr",
        );
    }
    Ok(process)
}

fn spawn_output_drain<R: std::io::Read + Send + 'static>(
    process: Weak<ProcessState>,
    stream: &'static str,
    mut reader: BufReader<R>,
    thread_name: &'static str,
) {
    thread::Builder::new()
        .name(thread_name.to_owned())
        .spawn(move || {
            loop {
                let Some(process) = process.upgrade() else {
                    return;
                };
                match read_bounded_line(&mut reader, MAX_PROCESS_DIAGNOSTIC_BYTES) {
                    Ok(Some(line)) => {
                        if line.truncated {
                            process.record_diagnostic(stream, TRUNCATED_PROCESS_DIAGNOSTIC);
                        } else {
                            process.record_diagnostic(stream, &line.text);
                        }
                    }
                    Ok(None) => return,
                    Err(error) => {
                        process.record_diagnostic(stream, &format!("output read failed: {error}"));
                        return;
                    }
                }
            }
        })
        .expect("Bun output drain thread should start");
}

#[derive(Debug)]
struct BoundedLine {
    text: String,
    truncated: bool,
}

fn read_bounded_line<R: std::io::Read>(
    reader: &mut BufReader<R>,
    limit: usize,
) -> std::io::Result<Option<BoundedLine>> {
    let mut retained = Vec::with_capacity(limit);
    let mut truncated = false;
    loop {
        let (consumed, reached_newline) = {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                if retained.is_empty() && !truncated {
                    return Ok(None);
                }
                break;
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let consumed = newline.map_or(available.len(), |position| position + 1);
            let payload_end = newline.unwrap_or(consumed);
            let remaining = limit.saturating_sub(retained.len());
            let copied = payload_end.min(remaining);
            retained.extend_from_slice(&available[..copied]);
            truncated |= copied < payload_end;
            (consumed, newline.is_some())
        };
        reader.consume(consumed);
        if reached_newline {
            break;
        }
    }
    if retained.last() == Some(&b'\r') {
        retained.pop();
    }
    Ok(Some(BoundedLine {
        text: String::from_utf8_lossy(&retained).into_owned(),
        truncated,
    }))
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
            let readiness_process = Arc::clone(process);
            thread::Builder::new()
                .name("lenso-bun-json-rpc-readiness".to_owned())
                .spawn(move || {
                    let _ = ready_sender.send(read_ready_address(stdout, &readiness_process));
                })
                .map_err(|error| RuntimeFailure::Internal {
                    detail: format!("failed to start Bun readiness reader: {error}"),
                })?;
            let (address, stdout) = match ready_receiver.recv_timeout(PROCESS_STARTUP_TIMEOUT) {
                Ok(Ok(ready)) => ready,
                Ok(Err(error)) => {
                    return Err(process
                        .failure_or_exit()
                        .unwrap_or_else(|| process.decorate_failure(error)));
                }
                Err(_) => {
                    process.stop();
                    return Err(process.decorate_failure(RuntimeFailure::PluginFailure {
                        detail: "Bun JSON-RPC process readiness timed out".to_owned(),
                    }));
                }
            };
            spawn_output_drain(
                Arc::downgrade(process),
                "stdout",
                stdout,
                "lenso-bun-json-rpc-stdout",
            );
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

fn read_ready_address(
    stdout: ChildStdout,
    process: &ProcessState,
) -> Result<(SocketAddr, BufReader<ChildStdout>), RuntimeFailure> {
    let mut reader = BufReader::new(stdout);
    for _ in 0..32 {
        let Some(line) =
            read_bounded_line(&mut reader, MAX_PROCESS_DIAGNOSTIC_BYTES).map_err(|error| {
                RuntimeFailure::PluginFailure {
                    detail: format!("failed to read Bun JSON-RPC readiness: {error}"),
                }
            })?
        else {
            break;
        };
        if line.truncated {
            process.record_diagnostic("stdout", TRUNCATED_PROCESS_DIAGNOSTIC);
            continue;
        }
        let Some(port) = line.text.strip_prefix("LENSO_READY ") else {
            process.record_diagnostic("stdout", &line.text);
            continue;
        };
        let port: u16 = port.trim().parse().map_err(|_| protocol_violation(None))?;
        let address = ("127.0.0.1", port)
            .to_socket_addrs()
            .map_err(|_| RuntimeFailure::PluginFailure {
                detail: "Bun JSON-RPC readiness address could not be resolved".to_owned(),
            })?
            .next()
            .ok_or_else(|| RuntimeFailure::PluginFailure {
                detail: "Bun JSON-RPC readiness address was empty".to_owned(),
            })?;
        return Ok((address, reader));
    }
    Err(RuntimeFailure::PluginFailure {
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
            Some(RuntimeFailure::PluginFailure { .. })
        ));
    }

    #[test]
    fn process_state_retains_a_bounded_stderr_tail() {
        let mut command = std::process::Command::new("sh");
        command.args([
            "-c",
            "i=0; while [ $i -lt 40 ]; do echo diagnostic-$i >&2; i=$((i+1)); done; exit 9",
        ]);
        let process = spawn_process(command, "example.greeting@1").expect("process should start");
        process.start_monitor();
        for _ in 0..100 {
            if !process.is_alive() && process.diagnostic_tail().contains("diagnostic-39") {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let diagnostics = process.diagnostic_tail();
        assert!(diagnostics.contains("stderr: diagnostic-39"));
        assert!(!diagnostics.contains("diagnostic-0 |"));
        let failure = format!("{:?}", process.failure());
        assert!(failure.contains("child output"));
        assert!(failure.contains("diagnostic-39"));
    }

    #[test]
    fn readiness_reader_returns_stdout_for_continued_drain() {
        let mut command = std::process::Command::new("sh");
        command
            .args([
                "-c",
                "printf 'LENSO_READY 1234\\n'; sleep 0.02; printf 'after-ready\\n'",
            ])
            .stdout(std::process::Stdio::piped());
        let process = spawn_process(command, "example.greeting@1").expect("process should start");
        let stdout = process.take_stdout().unwrap();
        let (address, reader) = read_ready_address(stdout, &process).unwrap();
        assert_eq!(address, SocketAddr::from(([127, 0, 0, 1], 1234)));
        spawn_output_drain(
            Arc::downgrade(&process),
            "stdout",
            reader,
            "lenso-bun-test-stdout",
        );
        for _ in 0..100 {
            if process.diagnostic_tail().contains("after-ready") {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(process.diagnostic_tail().contains("stdout: after-ready"));
    }

    #[test]
    fn readiness_retains_stdout_emitted_before_the_ready_line() {
        let mut command = std::process::Command::new("sh");
        command
            .args(["-c", "printf 'booting provider\\nLENSO_READY 1234\\n'"])
            .stdout(std::process::Stdio::piped());
        let process = spawn_process(command, "example.greeting@1").expect("process should start");
        let stdout = process.take_stdout().unwrap();

        let (address, _reader) = read_ready_address(stdout, &process).unwrap();

        assert_eq!(address, SocketAddr::from(([127, 0, 0, 1], 1234)));
        assert!(
            process
                .diagnostic_tail()
                .contains("stdout: booting provider")
        );
    }

    #[test]
    fn failure_uses_stderr_that_arrives_after_the_process_is_marked_dead() {
        let mut command = std::process::Command::new("sh");
        command.args(["-c", "sleep 0.02; printf 'late detail\\n' >&2; sleep 0.1"]);
        let process = spawn_process(command, "example.greeting@1").expect("process should start");
        process.mark_dead(RuntimeFailure::PluginFailure {
            detail: "Bun process failed".to_owned(),
        });
        for _ in 0..100 {
            if process.diagnostic_tail().contains("late detail") {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        let failure = format!("{:?}", process.failure());
        assert!(failure.contains("Bun process failed"));
        assert!(failure.contains("stderr: late detail"));
    }

    #[test]
    fn child_output_redacts_common_credentials_before_failure_display() {
        let mut command = std::process::Command::new("sh");
        command.args(["-c", "sleep 30"]);
        let process = spawn_process(command, "example.greeting@1").expect("process should start");
        process.record_diagnostic("stderr", "Authorization: Bearer do-not-leak");
        process.record_diagnostic("stdout", "https://alice:password@example.com/path");
        process.record_diagnostic(
            "stdout",
            "safe https://example.com/path then https://alice:opaque@example.com/private",
        );
        process.mark_dead(RuntimeFailure::PluginFailure {
            detail: "Bun process failed".to_owned(),
        });

        let failure = format!("{:?}", process.failure());
        assert!(failure.contains("[redacted sensitive child output]"));
        assert!(!failure.contains("do-not-leak"));
        assert!(!failure.contains("alice"));
        assert!(!failure.contains("password"));
        assert!(!failure.contains("opaque"));
    }

    #[test]
    fn multi_megabyte_line_is_drained_with_bounded_diagnostics() {
        let mut command = std::process::Command::new("sh");
        command.args([
            "-c",
            "awk 'BEGIN { for (i = 0; i < 2097152; i++) printf \"x\" }' >&2; exit 9",
        ]);
        let process = spawn_process(command, "example.greeting@1").expect("process should start");
        process.start_monitor();
        for _ in 0..400 {
            if !process.is_alive()
                && process
                    .diagnostic_tail()
                    .contains(TRUNCATED_PROCESS_DIAGNOSTIC)
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        let diagnostics = process.diagnostic_tail();
        assert!(diagnostics.contains(TRUNCATED_PROCESS_DIAGNOSTIC));
        assert!(diagnostics.len() <= MAX_PROCESS_DIAGNOSTIC_CHARS + 32);
    }

    #[test]
    fn truncated_child_output_never_retains_a_sensitive_prefix() {
        let mut command = std::process::Command::new("sh");
        command.args([
            "-c",
            "{ printf 'https://alice:opaque'; awk 'BEGIN { for (i = 0; i < 600; i++) printf \"x\" }'; printf '@example.com\\n'; } >&2; exit 9",
        ]);
        let process = spawn_process(command, "example.greeting@1").expect("process should start");
        process.start_monitor();
        for _ in 0..400 {
            if !process.is_alive()
                && process
                    .diagnostic_tail()
                    .contains(TRUNCATED_PROCESS_DIAGNOSTIC)
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        let failure = format!("{:?}", process.failure());
        assert!(failure.contains(TRUNCATED_PROCESS_DIAGNOSTIC));
        assert!(!failure.contains("alice"));
        assert!(!failure.contains("opaque"));
        assert!(!failure.contains("example.com"));
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
            Some(RuntimeFailure::PluginFailure { .. })
        ));
    }
}

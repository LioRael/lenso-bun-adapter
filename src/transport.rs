use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io::{BufRead, BufReader, Write},
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
    FramedMessage, Handshake, HandshakeAck, WireOutcome, WireRequest, WireStreamCall,
    WireStreamOpen, WireStreamOutcome, encode_frame, from_wire_failure, protocol_violation,
    read_frame, verify_handshake, write_frame,
};

const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const PROCESS_STARTUP_TIMEOUT: Duration = Duration::from_secs(2);
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_CANCELLED_REQUEST_IDS: usize = 1024;
const MAX_RETIRED_REQUEST_IDS: usize = 1024;
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
                transport.cancel_stream_call(request_id, stream_id, session)
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

#[derive(Debug)]
pub(crate) struct FramedTransport {
    process: Arc<ProcessState>,
    sender: SyncSender<Vec<u8>>,
    control_sender: SyncSender<Vec<u8>>,
    pending: PendingResponses,
    stream_pending: PendingStreamResponses,
    cancelled: Arc<Mutex<BTreeSet<u64>>>,
    stream_cancelled: Arc<Mutex<BTreeSet<u64>>>,
    retired: Arc<Mutex<BTreeSet<u64>>>,
    stream_retired: Arc<Mutex<BTreeSet<u64>>>,
    max_frame_bytes: usize,
    admission_capacity: usize,
    stream_admission_capacity: usize,
    session: String,
    capability: &'static str,
    capability_ids: CapabilityIds,
}

pub(crate) fn open_framed(
    process: &Arc<ProcessState>,
    expected: &Handshake,
    queue_capacity: usize,
    capability_ids: CapabilityIds,
) -> Result<TransportClient, RuntimeFailure> {
    let mut stdin = process.take_stdin()?;
    let stdout = process.take_stdout()?;
    process.start_monitor();
    write_frame(
        &mut stdin,
        &FramedMessage::Handshake(expected.clone()),
        expected.max_frame_bytes,
    )?;
    let (handshake_sender, handshake_receiver) = mpsc::sync_channel(1);
    let max_frame_bytes = expected.max_frame_bytes;
    let handshake_process = process.clone();
    thread::Builder::new()
        .name("lenso-bun-framed-handshake".to_owned())
        .spawn(move || {
            let mut stdout = stdout;
            let result = match read_frame(&mut stdout, max_frame_bytes) {
                Ok(FramedMessage::HandshakeAck(ack)) => Ok((ack, stdout)),
                Ok(_) => Err(protocol_violation(None)),
                Err(error) => Err(handshake_process.failure_or_exit().unwrap_or(error)),
            };
            let _ = handshake_sender.send(result);
        })
        .map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to start Bun handshake reader: {error}"),
        })?;
    let (actual, stdout) =
        if let Ok(result) = handshake_receiver.recv_timeout(PROCESS_STARTUP_TIMEOUT) {
            result?
        } else {
            process.stop();
            return Err(RuntimeFailure::ModuleFailure {
                detail: "Bun framed-stdio handshake timed out".to_owned(),
            });
        };
    let capability_name = expected
        .endpoints
        .first()
        .map_or("lenso.bun-process@1", |endpoint| {
            endpoint.capability_id.as_str()
        });
    let capability = capability_id(&capability_ids, capability_name);
    verify_handshake(expected, &actual, capability)?;
    let session = actual.session.unwrap_or_default();
    let queue_capacity = queue_capacity.max(1);
    let (sender, receiver) = mpsc::sync_channel(queue_capacity);
    let (control_sender, control_receiver) = mpsc::sync_channel(queue_capacity.saturating_add(1));
    let transport = Arc::new(FramedTransport {
        process: process.clone(),
        sender,
        control_sender,
        pending: Arc::new(Mutex::new(BTreeMap::new())),
        stream_pending: Arc::new(Mutex::new(BTreeMap::new())),
        cancelled: Arc::new(Mutex::new(BTreeSet::new())),
        stream_cancelled: Arc::new(Mutex::new(BTreeSet::new())),
        retired: Arc::new(Mutex::new(BTreeSet::new())),
        stream_retired: Arc::new(Mutex::new(BTreeSet::new())),
        max_frame_bytes: expected.max_frame_bytes,
        admission_capacity: queue_capacity,
        stream_admission_capacity: queue_capacity.max(2),
        session,
        capability,
        capability_ids,
    });
    let failure_transport = Arc::downgrade(&transport);
    process.set_failure_handler(move |failure| {
        if let Some(transport) = failure_transport.upgrade() {
            transport.fail_all(&failure);
        }
    });
    spawn_framed_writer(process.clone(), stdin, receiver, control_receiver);
    spawn_framed_reader(&transport, stdout);
    Ok(TransportClient::Framed(transport))
}

fn spawn_framed_writer(
    process: Arc<ProcessState>,
    mut stdin: ChildStdin,
    receiver: Receiver<Vec<u8>>,
    control_receiver: Receiver<Vec<u8>>,
) {
    thread::Builder::new()
        .name("lenso-bun-framed-writer".to_owned())
        .spawn(move || {
            loop {
                let frame = match control_receiver.try_recv() {
                    Ok(frame) => frame,
                    Err(_) => match receiver.recv_timeout(Duration::from_millis(5)) {
                        Ok(frame) => frame,
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => break,
                    },
                };
                if let Err(error) = stdin.write_all(&frame).and_then(|()| stdin.flush()) {
                    process.mark_dead(RuntimeFailure::ModuleFailure {
                        detail: format!("Bun framed-stdio write failed: {error}"),
                    });
                    break;
                }
            }
        })
        .expect("Bun framed writer thread should start");
}

fn spawn_framed_reader(transport: &Arc<FramedTransport>, mut stdout: ChildStdout) {
    let process = transport.process.clone();
    let pending = transport.pending.clone();
    let stream_pending = transport.stream_pending.clone();
    let cancelled = transport.cancelled.clone();
    let stream_cancelled = transport.stream_cancelled.clone();
    let retired = transport.retired.clone();
    let stream_retired = transport.stream_retired.clone();
    let max_frame_bytes = transport.max_frame_bytes;
    let capability = transport.capability;
    thread::Builder::new()
        .name("lenso-bun-framed-reader".to_owned())
        .spawn(move || {
            loop {
                let message = read_frame(&mut stdout, max_frame_bytes);
                match message {
                    Ok(FramedMessage::Response {
                        request_id,
                        outcome,
                    }) => {
                        let sender = pending
                            .lock()
                            .ok()
                            .and_then(|mut pending| pending.remove(&request_id));
                        let Some(sender) = sender else {
                            let late_cancel = cancelled
                                .lock()
                                .is_ok_and(|mut cancelled| cancelled.remove(&request_id));
                            if late_cancel {
                                remember_request_id(&retired, request_id);
                                continue;
                            }
                            process.mark_dead(protocol_violation(Some(capability)));
                            break;
                        };
                        remember_request_id(&retired, request_id);
                        let _ = sender.send(Ok(outcome));
                    }
                    Ok(FramedMessage::StreamResponse {
                        request_id,
                        response,
                    }) => {
                        let sender = stream_pending
                            .lock()
                            .ok()
                            .and_then(|mut pending| pending.remove(&request_id));
                        let Some(sender) = sender else {
                            let late_cancel = stream_cancelled
                                .lock()
                                .is_ok_and(|mut cancelled| cancelled.remove(&request_id));
                            if late_cancel {
                                remember_request_id(&stream_retired, request_id);
                                continue;
                            }
                            process.mark_dead(protocol_violation(Some(capability)));
                            break;
                        };
                        remember_request_id(&stream_retired, request_id);
                        let _ = sender.send(Ok(response));
                    }
                    Ok(_) => {
                        process.mark_dead(protocol_violation(Some(capability)));
                        break;
                    }
                    Err(error) => {
                        let error = process
                            .failure_or_exit_within(Duration::from_millis(50))
                            .unwrap_or(error);
                        process.mark_dead(error);
                        break;
                    }
                }
            }
        })
        .expect("Bun framed reader thread should start");
}

impl FramedTransport {
    fn request(self: &Arc<Self>, request: WireRequest) -> Result<WireCall, RuntimeFailure> {
        if !self.process.is_alive() {
            return Err(self
                .process
                .failure()
                .unwrap_or(RuntimeFailure::Unavailable {
                    capability: self.capability,
                }));
        }
        let request_id = request.request_id;
        let request_capability = capability_id(&self.capability_ids, &request.capability_id);
        let operation = request.operation.clone();
        let message = FramedMessage::Request(request);
        let frame = encode_frame(&message, self.max_frame_bytes)?;
        let (sender, receiver) = oneshot::channel();
        let mut pending = self.pending.lock().map_err(|_| RuntimeFailure::Internal {
            detail: "Bun pending response lock poisoned".to_owned(),
        })?;
        let cancelled = self
            .cancelled
            .lock()
            .map_err(|_| RuntimeFailure::Internal {
                detail: "Bun cancelled request lock poisoned".to_owned(),
            })?;
        if pending.contains_key(&request_id) || cancelled.contains(&request_id) {
            return Err(protocol_violation(Some(self.capability)));
        }
        if pending.len() >= self.admission_capacity {
            return Err(RuntimeFailure::ResourceExhausted {
                capability: request_capability,
                operation,
            });
        }
        drop(cancelled);
        if self
            .retired
            .lock()
            .map_err(|_| RuntimeFailure::Internal {
                detail: "Bun retired request lock poisoned".to_owned(),
            })?
            .contains(&request_id)
        {
            return Err(protocol_violation(Some(self.capability)));
        }
        pending.insert(request_id, sender);
        drop(pending);
        match self.sender.try_send(frame) {
            Ok(()) => Ok(WireCall::new(
                request_id,
                TransportClient::Framed(self.clone()),
                receiver,
            )),
            Err(TrySendError::Full(_)) => {
                self.pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&request_id));
                Err(RuntimeFailure::ResourceExhausted {
                    capability: request_capability,
                    operation,
                })
            }
            Err(TrySendError::Disconnected(_)) => {
                self.pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&request_id));
                let error =
                    self.process
                        .failure_or_exit()
                        .unwrap_or(RuntimeFailure::ModuleFailure {
                            detail: "Bun framed-stdio writer stopped".to_owned(),
                        });
                self.process.mark_dead(error.clone());
                Err(error)
            }
        }
    }

    fn stream_request(
        self: &Arc<Self>,
        message: FramedMessage,
        request_id: u64,
        stream_id: u64,
        session: String,
        capability_name: &str,
        operation: &str,
    ) -> Result<StreamCall, RuntimeFailure> {
        if !self.process.is_alive() {
            return Err(self
                .process
                .failure()
                .unwrap_or(RuntimeFailure::Unavailable {
                    capability: self.capability,
                }));
        }
        let capability = capability_id(&self.capability_ids, capability_name);
        let frame = encode_frame(&message, self.max_frame_bytes)?;
        let (sender, receiver) = oneshot::channel();
        let mut pending = self
            .stream_pending
            .lock()
            .map_err(|_| RuntimeFailure::Internal {
                detail: "Bun pending stream response lock poisoned".to_owned(),
            })?;
        if pending.contains_key(&request_id)
            || self
                .stream_cancelled
                .lock()
                .map_err(|_| RuntimeFailure::Internal {
                    detail: "Bun cancelled stream request lock poisoned".to_owned(),
                })?
                .contains(&request_id)
            || self
                .stream_retired
                .lock()
                .map_err(|_| RuntimeFailure::Internal {
                    detail: "Bun retired stream request lock poisoned".to_owned(),
                })?
                .contains(&request_id)
        {
            return Err(protocol_violation(Some(capability)));
        }
        if pending.len() >= self.stream_admission_capacity {
            return Err(RuntimeFailure::ResourceExhausted {
                capability,
                operation: operation.to_owned(),
            });
        }
        pending.insert(request_id, sender);
        drop(pending);
        match self.sender.try_send(frame) {
            Ok(()) => Ok(StreamCall::new(
                request_id,
                stream_id,
                session,
                TransportClient::Framed(self.clone()),
                receiver,
            )),
            Err(TrySendError::Full(_)) => {
                self.stream_pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&request_id));
                Err(RuntimeFailure::ResourceExhausted {
                    capability,
                    operation: operation.to_owned(),
                })
            }
            Err(TrySendError::Disconnected(_)) => {
                self.stream_pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&request_id));
                let error =
                    self.process
                        .failure_or_exit()
                        .unwrap_or(RuntimeFailure::ModuleFailure {
                            detail: "Bun framed-stdio writer stopped".to_owned(),
                        });
                self.process.mark_dead(error.clone());
                Err(error)
            }
        }
    }

    fn cancel(&self, request_id: u64) {
        let removed = self
            .pending
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(&request_id));
        if removed.is_none() || !self.process.is_alive() {
            return;
        }
        if let Ok(mut cancelled) = self.cancelled.lock() {
            while cancelled.len() >= MAX_CANCELLED_REQUEST_IDS {
                let Some(oldest) = cancelled.iter().next().copied() else {
                    break;
                };
                cancelled.remove(&oldest);
            }
            cancelled.insert(request_id);
        }
        remember_request_id(&self.retired, request_id);
        if let Ok(frame) = encode_frame(&FramedMessage::Cancel { request_id }, self.max_frame_bytes)
            && self.control_sender.try_send(frame).is_err()
        {
            self.process.mark_dead(RuntimeFailure::ModuleFailure {
                detail: "Bun framed-stdio cancellation channel stopped".to_owned(),
            });
        }
    }

    fn cancel_stream_call(&self, request_id: u64, stream_id: u64, session: &str) {
        let removed = self
            .stream_pending
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(&request_id));
        if removed.is_none() {
            return;
        }
        if let Ok(mut cancelled) = self.stream_cancelled.lock() {
            while cancelled.len() >= MAX_CANCELLED_REQUEST_IDS {
                let Some(oldest) = cancelled.iter().next().copied() else {
                    break;
                };
                cancelled.remove(&oldest);
            }
            cancelled.insert(request_id);
        }
        remember_request_id(&self.stream_retired, request_id);
        self.cancel_stream(stream_id, session);
    }

    fn cancel_stream(&self, stream_id: u64, session: &str) {
        if !self.process.is_alive() {
            return;
        }
        if let Ok(frame) = encode_frame(
            &FramedMessage::StreamCancel {
                stream_id,
                session: session.to_owned(),
            },
            self.max_frame_bytes,
        ) && self.control_sender.try_send(frame).is_err()
        {
            self.process.mark_dead(RuntimeFailure::ModuleFailure {
                detail: "Bun framed-stdio stream cancellation channel stopped".to_owned(),
            });
        }
    }

    fn fail_all(&self, error: &RuntimeFailure) {
        if let Ok(mut pending) = self.pending.lock() {
            let pending = std::mem::take(&mut *pending);
            for (_, sender) in pending {
                let _ = sender.send(Err(error.clone()));
            }
        }
        if let Ok(mut cancelled) = self.cancelled.lock() {
            cancelled.clear();
        }
        if let Ok(mut pending) = self.stream_pending.lock() {
            let pending = std::mem::take(&mut *pending);
            for (_, sender) in pending {
                let _ = sender.send(Err(error.clone()));
            }
        }
        if let Ok(mut cancelled) = self.stream_cancelled.lock() {
            cancelled.clear();
        }
    }

    fn shutdown(&self) {
        if self.process.is_alive()
            && let Ok(frame) = encode_frame(&FramedMessage::Shutdown, self.max_frame_bytes)
        {
            let _ = self.control_sender.try_send(frame);
        }
        self.process.stop();
    }
}

impl Drop for FramedTransport {
    fn drop(&mut self) {
        self.process.stop();
    }
}

#[derive(Debug)]
pub(crate) struct JsonRpcTransport {
    process: Arc<ProcessState>,
    sender: SyncSender<HttpCall>,
    cancel_sender: SyncSender<u64>,
    stream_sender: SyncSender<HttpStreamCall>,
    stream_cancel_sender: SyncSender<StreamCancelCall>,
    pending: PendingResponses,
    stream_pending: PendingStreamResponses,
    cancellations: Arc<Mutex<BTreeMap<u64, Arc<AtomicBool>>>>,
    stream_cancellations: Arc<Mutex<BTreeMap<u64, Arc<AtomicBool>>>>,
    retired: Arc<Mutex<BTreeSet<u64>>>,
    stream_retired: Arc<Mutex<BTreeSet<u64>>>,
    client: Arc<HttpClient>,
    address: SocketAddr,
    max_frame_bytes: usize,
    admission_capacity: usize,
    stream_admission_capacity: usize,
    session: String,
    capability: &'static str,
    capability_ids: CapabilityIds,
}

#[derive(Debug)]
struct HttpCall {
    request: WireRequest,
    cancelled: Arc<AtomicBool>,
}

#[derive(Debug)]
struct HttpStreamCall {
    request_id: u64,
    method: &'static str,
    params: Value,
    cancelled: Arc<AtomicBool>,
}

#[derive(Debug)]
struct StreamCancelCall {
    stream_id: u64,
    session: String,
}

pub(crate) fn open_json_rpc(
    process: &Arc<ProcessState>,
    address: SocketAddr,
    expected: &Handshake,
    queue_capacity: usize,
    capability_ids: CapabilityIds,
) -> Result<TransportClient, RuntimeFailure> {
    let capability_name = expected
        .endpoints
        .first()
        .map_or("lenso.bun-process@1", |endpoint| {
            endpoint.capability_id.as_str()
        });
    let capability = capability_id(&capability_ids, capability_name);
    let client = build_json_rpc_client(address, expected.max_frame_bytes, queue_capacity)?;
    let runtime = json_rpc_runtime()?;
    let actual: HandshakeAck = runtime
        .block_on(client.request("lenso.handshake", rpc_params![expected.clone()]))
        .map_err(|error| json_rpc_failure("handshake", &error, capability))?;
    verify_handshake(expected, &actual, capability)?;
    let session = actual
        .session
        .filter(|session| !session.is_empty())
        .ok_or_else(|| protocol_violation(Some(capability)))?;

    let queue_capacity = queue_capacity.max(1);
    let stream_capacity = queue_capacity.max(2);
    let (sender, receiver) = mpsc::sync_channel(queue_capacity);
    let (cancel_sender, cancel_receiver) = mpsc::sync_channel(queue_capacity.saturating_add(1));
    let (stream_sender, stream_receiver) = mpsc::sync_channel(stream_capacity);
    let (stream_cancel_sender, stream_cancel_receiver) =
        mpsc::sync_channel(queue_capacity.saturating_add(1));
    let transport = Arc::new(JsonRpcTransport {
        process: process.clone(),
        sender,
        cancel_sender,
        stream_sender,
        stream_cancel_sender,
        pending: Arc::new(Mutex::new(BTreeMap::new())),
        stream_pending: Arc::new(Mutex::new(BTreeMap::new())),
        cancellations: Arc::new(Mutex::new(BTreeMap::new())),
        stream_cancellations: Arc::new(Mutex::new(BTreeMap::new())),
        retired: Arc::new(Mutex::new(BTreeSet::new())),
        stream_retired: Arc::new(Mutex::new(BTreeSet::new())),
        client: Arc::new(client),
        address,
        max_frame_bytes: expected.max_frame_bytes,
        admission_capacity: queue_capacity,
        stream_admission_capacity: stream_capacity,
        session,
        capability,
        capability_ids,
    });
    let failure_transport = Arc::downgrade(&transport);
    process.set_failure_handler(move |failure| {
        if let Some(transport) = failure_transport.upgrade() {
            transport.fail_all(&failure);
        }
    });
    spawn_json_rpc_worker(Arc::downgrade(&transport), receiver);
    spawn_json_rpc_cancel_worker(
        process.clone(),
        transport.client.clone(),
        transport.session.clone(),
        cancel_receiver,
    );
    spawn_json_rpc_stream_worker(Arc::downgrade(&transport), stream_receiver, stream_capacity);
    spawn_json_rpc_stream_cancel_worker(
        process.clone(),
        transport.client.clone(),
        stream_cancel_receiver,
    );
    Ok(TransportClient::JsonRpc(transport))
}

fn spawn_json_rpc_worker(transport: Weak<JsonRpcTransport>, receiver: Receiver<HttpCall>) {
    thread::Builder::new()
        .name("lenso-bun-json-rpc-worker".to_owned())
        .spawn(move || {
            let runtime = match json_rpc_runtime() {
                Ok(runtime) => runtime,
                Err(error) => {
                    if let Some(transport) = transport.upgrade() {
                        transport.process.mark_dead(error);
                    }
                    return;
                }
            };
            while let Ok(call) = receiver.recv() {
                let Some(transport) = transport.upgrade() else {
                    break;
                };
                let request_id = call.request.request_id;
                if call.cancelled.load(Ordering::Acquire) {
                    transport.finish(request_id, Err(RuntimeFailure::Cancelled { request_id }));
                    continue;
                }
                let result = if call.cancelled.load(Ordering::Acquire) {
                    Err(RuntimeFailure::Cancelled { request_id })
                } else {
                    runtime
                        .block_on(
                            transport.client.request::<WireOutcome, _>(
                                "lenso.request",
                                rpc_params![call.request],
                            ),
                        )
                        .map_err(|error| json_rpc_failure("request", &error, transport.capability))
                        .and_then(|outcome| {
                            if call.cancelled.load(Ordering::Acquire) {
                                Err(RuntimeFailure::Cancelled { request_id })
                            } else {
                                Ok(outcome)
                            }
                        })
                };
                if let Err(error) = &result
                    && !matches!(error, RuntimeFailure::Cancelled { .. })
                {
                    let failure = transport
                        .process
                        .failure_or_exit()
                        .unwrap_or_else(|| error.clone());
                    transport.process.mark_dead(failure);
                }
                transport.finish(request_id, result);
            }
        })
        .expect("Bun JSON-RPC worker thread should start");
}

fn spawn_json_rpc_cancel_worker(
    process: Arc<ProcessState>,
    client: Arc<HttpClient>,
    session: String,
    receiver: Receiver<u64>,
) {
    thread::Builder::new()
        .name("lenso-bun-json-rpc-cancel-worker".to_owned())
        .spawn(move || {
            let Ok(runtime) = json_rpc_runtime() else {
                return;
            };
            while let Ok(request_id) = receiver.recv() {
                if !process.is_alive() {
                    continue;
                }
                let _ = runtime.block_on(client.request::<bool, _>(
                    "lenso.cancel",
                    rpc_params![serde_json::json!({
                        "request_id": request_id,
                        "session": session,
                    })],
                ));
            }
        })
        .expect("Bun JSON-RPC cancellation worker should start");
}

fn spawn_json_rpc_stream_worker(
    transport: Weak<JsonRpcTransport>,
    receiver: Receiver<HttpStreamCall>,
    worker_count: usize,
) {
    let receiver = Arc::new(Mutex::new(receiver));
    for worker_index in 0..worker_count.max(2) {
        let transport = transport.clone();
        let receiver = receiver.clone();
        thread::Builder::new()
            .name(format!("lenso-bun-json-rpc-stream-worker-{worker_index}"))
            .spawn(move || {
                let runtime = match json_rpc_runtime() {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        if let Some(transport) = transport.upgrade() {
                            transport.process.mark_dead(error);
                        }
                        return;
                    }
                };
                let client = match transport.upgrade() {
                    Some(transport) => match build_json_rpc_client(
                        transport.address,
                        transport.max_frame_bytes,
                        transport.stream_admission_capacity,
                    ) {
                        Ok(client) => client,
                        Err(error) => {
                            transport.process.mark_dead(error);
                            return;
                        }
                    },
                    None => return,
                };
                loop {
                    let call = match receiver.lock() {
                        Ok(receiver) => receiver.recv(),
                        Err(_) => return,
                    };
                    let Ok(call) = call else { break };
                    let Some(transport) = transport.upgrade() else {
                        break;
                    };
                    let request_id = call.request_id;
                    if call.cancelled.load(Ordering::Acquire) {
                        transport.finish_stream(
                            request_id,
                            Err(RuntimeFailure::Cancelled { request_id }),
                        );
                        continue;
                    }
                    let method = call.method;
                    let result = runtime
                        .block_on(
                            client
                                .request::<WireStreamOutcome, _>(method, rpc_params![call.params]),
                        )
                        .map_err(|error| json_rpc_failure(method, &error, transport.capability))
                        .and_then(|outcome| {
                            if call.cancelled.load(Ordering::Acquire) {
                                Err(RuntimeFailure::Cancelled { request_id })
                            } else {
                                Ok(outcome)
                            }
                        });
                    if let Err(error) = &result
                        && !matches!(error, RuntimeFailure::Cancelled { .. })
                    {
                        let failure = transport
                            .process
                            .failure_or_exit()
                            .unwrap_or_else(|| error.clone());
                        transport.process.mark_dead(failure);
                    }
                    transport.finish_stream(request_id, result);
                }
            })
            .expect("Bun JSON-RPC stream worker thread should start");
    }
}

fn spawn_json_rpc_stream_cancel_worker(
    process: Arc<ProcessState>,
    client: Arc<HttpClient>,
    receiver: Receiver<StreamCancelCall>,
) {
    thread::Builder::new()
        .name("lenso-bun-json-rpc-stream-cancel-worker".to_owned())
        .spawn(move || {
            let Ok(runtime) = json_rpc_runtime() else {
                return;
            };
            while let Ok(cancel) = receiver.recv() {
                if !process.is_alive() {
                    continue;
                }
                let _ = runtime.block_on(client.request::<bool, _>(
                    "lenso.stream.cancel",
                    rpc_params![serde_json::json!({
                        "stream_id": cancel.stream_id,
                        "session": cancel.session,
                    })],
                ));
            }
        })
        .expect("Bun JSON-RPC stream cancellation worker thread should start");
}

impl JsonRpcTransport {
    fn stream_request(
        self: &Arc<Self>,
        request_id: u64,
        stream_id: u64,
        session: String,
        method: &'static str,
        params: Value,
        operation: &str,
    ) -> Result<StreamCall, RuntimeFailure> {
        if !self.process.is_alive() {
            return Err(self
                .process
                .failure()
                .unwrap_or(RuntimeFailure::Unavailable {
                    capability: self.capability,
                }));
        }
        let encoded_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": [&params],
        });
        let encoded_size = serde_json::to_vec(&encoded_request)
            .map_err(|_| protocol_violation(Some(self.capability)))?
            .len();
        if encoded_size > self.max_frame_bytes {
            return Err(protocol_violation(Some(self.capability)));
        }
        if self
            .stream_retired
            .lock()
            .map_err(|_| RuntimeFailure::Internal {
                detail: "Bun JSON-RPC retired stream request lock poisoned".to_owned(),
            })?
            .contains(&request_id)
        {
            return Err(protocol_violation(Some(self.capability)));
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = oneshot::channel();
        let mut cancellations =
            self.stream_cancellations
                .lock()
                .map_err(|_| RuntimeFailure::Internal {
                    detail: "Bun JSON-RPC stream cancellation lock poisoned".to_owned(),
                })?;
        if cancellations.contains_key(&request_id) {
            return Err(protocol_violation(Some(self.capability)));
        }
        cancellations.insert(request_id, cancelled.clone());
        drop(cancellations);
        let mut pending = self
            .stream_pending
            .lock()
            .map_err(|_| RuntimeFailure::Internal {
                detail: "Bun JSON-RPC pending stream response lock poisoned".to_owned(),
            })?;
        if pending.contains_key(&request_id) {
            self.stream_cancellations
                .lock()
                .ok()
                .and_then(|mut cancellations| cancellations.remove(&request_id));
            return Err(protocol_violation(Some(self.capability)));
        }
        if pending.len() >= self.stream_admission_capacity {
            self.stream_cancellations
                .lock()
                .ok()
                .and_then(|mut cancellations| cancellations.remove(&request_id));
            return Err(RuntimeFailure::ResourceExhausted {
                capability: self.capability,
                operation: operation.to_owned(),
            });
        }
        pending.insert(request_id, sender);
        drop(pending);
        match self.stream_sender.try_send(HttpStreamCall {
            request_id,
            method,
            params,
            cancelled,
        }) {
            Ok(()) => Ok(StreamCall::new(
                request_id,
                stream_id,
                session,
                TransportClient::JsonRpc(self.clone()),
                receiver,
            )),
            Err(TrySendError::Full(_)) => {
                self.stream_cancellations
                    .lock()
                    .ok()
                    .and_then(|mut cancellations| cancellations.remove(&request_id));
                self.stream_pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&request_id));
                Err(RuntimeFailure::ResourceExhausted {
                    capability: self.capability,
                    operation: operation.to_owned(),
                })
            }
            Err(TrySendError::Disconnected(_)) => {
                self.stream_cancellations
                    .lock()
                    .ok()
                    .and_then(|mut cancellations| cancellations.remove(&request_id));
                self.stream_pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&request_id));
                let error =
                    self.process
                        .failure_or_exit()
                        .unwrap_or(RuntimeFailure::ModuleFailure {
                            detail: "Bun JSON-RPC stream worker stopped".to_owned(),
                        });
                self.process.mark_dead(error.clone());
                Err(error)
            }
        }
    }

    fn request(self: &Arc<Self>, request: WireRequest) -> Result<WireCall, RuntimeFailure> {
        if !self.process.is_alive() {
            return Err(self
                .process
                .failure()
                .unwrap_or(RuntimeFailure::Unavailable {
                    capability: self.capability,
                }));
        }
        let request_id = request.request_id;
        let request_capability = capability_id(&self.capability_ids, &request.capability_id);
        let mut request = request;
        request.session = Some(self.session.clone());
        let encoded_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "lenso.request",
            "params": [&request],
        });
        let encoded_size = serde_json::to_vec(&encoded_request)
            .map_err(|_| protocol_violation(Some(self.capability)))?
            .len();
        if encoded_size > self.max_frame_bytes {
            return Err(protocol_violation(Some(self.capability)));
        }
        if self
            .retired
            .lock()
            .map_err(|_| RuntimeFailure::Internal {
                detail: "Bun JSON-RPC retired request lock poisoned".to_owned(),
            })?
            .contains(&request_id)
        {
            return Err(protocol_violation(Some(self.capability)));
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = oneshot::channel();
        let operation = request.operation.clone();
        let mut cancellations =
            self.cancellations
                .lock()
                .map_err(|_| RuntimeFailure::Internal {
                    detail: "Bun JSON-RPC cancellation lock poisoned".to_owned(),
                })?;
        if cancellations.contains_key(&request_id) {
            return Err(protocol_violation(Some(self.capability)));
        }
        cancellations.insert(request_id, cancelled.clone());
        drop(cancellations);
        let mut pending = self.pending.lock().map_err(|_| RuntimeFailure::Internal {
            detail: "Bun JSON-RPC pending response lock poisoned".to_owned(),
        })?;
        if pending.contains_key(&request_id) {
            self.cancellations
                .lock()
                .ok()
                .and_then(|mut cancellations| cancellations.remove(&request_id));
            return Err(protocol_violation(Some(self.capability)));
        }
        if pending.len() >= self.admission_capacity {
            self.cancellations
                .lock()
                .ok()
                .and_then(|mut cancellations| cancellations.remove(&request_id));
            return Err(RuntimeFailure::ResourceExhausted {
                capability: request_capability,
                operation,
            });
        }
        pending.insert(request_id, sender);
        drop(pending);
        match self.sender.try_send(HttpCall { request, cancelled }) {
            Ok(()) => Ok(WireCall::new(
                request_id,
                TransportClient::JsonRpc(self.clone()),
                receiver,
            )),
            Err(TrySendError::Full(_)) => {
                self.cancellations
                    .lock()
                    .ok()
                    .and_then(|mut cancellations| cancellations.remove(&request_id));
                self.pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&request_id));
                Err(RuntimeFailure::ResourceExhausted {
                    capability: request_capability,
                    operation,
                })
            }
            Err(TrySendError::Disconnected(_)) => {
                self.cancellations
                    .lock()
                    .ok()
                    .and_then(|mut cancellations| cancellations.remove(&request_id));
                self.pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&request_id));
                let error =
                    self.process
                        .failure_or_exit()
                        .unwrap_or(RuntimeFailure::ModuleFailure {
                            detail: "Bun JSON-RPC worker stopped".to_owned(),
                        });
                self.process.mark_dead(error.clone());
                Err(error)
            }
        }
    }

    fn cancel(&self, request_id: u64) {
        let cancelled = self
            .cancellations
            .lock()
            .ok()
            .and_then(|mut cancellations| cancellations.remove(&request_id));
        if let Some(cancelled) = cancelled {
            // Dropping the bounded HTTP call closes the response stream. Bun's
            // handler treats a disconnected request as cancellation; the
            // Kernel still owns the authoritative terminal outcome and never
            // replays it.
            cancelled.store(true, Ordering::Release);
            self.pending
                .lock()
                .ok()
                .and_then(|mut pending| pending.remove(&request_id));
            remember_request_id(&self.retired, request_id);
            if self.cancel_sender.try_send(request_id).is_err() {
                self.process.mark_dead(RuntimeFailure::ModuleFailure {
                    detail: "Bun JSON-RPC cancellation channel stopped".to_owned(),
                });
            }
        }
    }

    fn cancel_stream_call(&self, request_id: u64, stream_id: u64, session: &str) {
        let cancelled = self
            .stream_cancellations
            .lock()
            .ok()
            .and_then(|mut cancellations| cancellations.remove(&request_id));
        if let Some(cancelled) = cancelled {
            cancelled.store(true, Ordering::Release);
            self.stream_pending
                .lock()
                .ok()
                .and_then(|mut pending| pending.remove(&request_id));
            remember_request_id(&self.stream_retired, request_id);
            self.cancel_stream(stream_id, session);
        }
    }

    fn cancel_stream(&self, stream_id: u64, session: &str) {
        if self.process.is_alive()
            && self
                .stream_cancel_sender
                .try_send(StreamCancelCall {
                    stream_id,
                    session: session.to_owned(),
                })
                .is_err()
        {
            self.process.mark_dead(RuntimeFailure::ModuleFailure {
                detail: "Bun JSON-RPC stream cancellation channel stopped".to_owned(),
            });
        }
    }

    fn finish(&self, request_id: u64, result: WireResult) {
        self.cancellations
            .lock()
            .ok()
            .and_then(|mut cancellations| cancellations.remove(&request_id));
        self.pending
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(&request_id))
            .map(|sender| sender.send(result));
        remember_request_id(&self.retired, request_id);
    }

    fn finish_stream(&self, request_id: u64, result: StreamWireResult) {
        self.stream_cancellations
            .lock()
            .ok()
            .and_then(|mut cancellations| cancellations.remove(&request_id));
        self.stream_pending
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(&request_id))
            .map(|sender| sender.send(result));
        remember_request_id(&self.stream_retired, request_id);
    }

    fn fail_all(&self, error: &RuntimeFailure) {
        if let Ok(mut cancellations) = self.cancellations.lock() {
            for cancelled in cancellations.values() {
                cancelled.store(true, Ordering::Release);
            }
            cancellations.clear();
        }
        if let Ok(mut pending) = self.pending.lock() {
            let pending = std::mem::take(&mut *pending);
            for (_, sender) in pending {
                let _ = sender.send(Err(error.clone()));
            }
        }
        if let Ok(mut cancellations) = self.stream_cancellations.lock() {
            for cancelled in cancellations.values() {
                cancelled.store(true, Ordering::Release);
            }
            cancellations.clear();
        }
        if let Ok(mut pending) = self.stream_pending.lock() {
            let pending = std::mem::take(&mut *pending);
            for (_, sender) in pending {
                let _ = sender.send(Err(error.clone()));
            }
        }
    }

    fn shutdown(&self) {
        self.process.stop();
    }
}

impl Drop for JsonRpcTransport {
    fn drop(&mut self) {
        self.process.stop();
    }
}

#[derive(Debug)]
struct TransportStreamState {
    stream_id: u64,
    session: String,
    capability: &'static str,
    operation: String,
    send_credit: AtomicUsize,
    next_send_sequence: AtomicU64,
    next_receive_sequence: AtomicU64,
    receive_in_flight: AtomicBool,
    local_half_closed: AtomicBool,
    peer_half_closed: AtomicBool,
    terminal_seen: AtomicBool,
    cancelled: AtomicBool,
    credit_waiters: Mutex<Vec<oneshot::Sender<()>>>,
}

/// JSON-valued stream session shared by the Bun transport and its codec wrapper.
#[derive(Debug)]
pub(crate) struct TransportStreamSession {
    transport: TransportClient,
    state: Arc<TransportStreamState>,
}

impl TransportStreamSession {
    pub(crate) fn new(
        transport: TransportClient,
        stream_id: u64,
        session: String,
        capability: &'static str,
        operation: String,
        credit: u32,
    ) -> Self {
        Self {
            transport,
            state: Arc::new(TransportStreamState {
                stream_id,
                session,
                capability,
                operation,
                send_credit: AtomicUsize::new(credit as usize),
                next_send_sequence: AtomicU64::new(0),
                next_receive_sequence: AtomicU64::new(0),
                receive_in_flight: AtomicBool::new(false),
                local_half_closed: AtomicBool::new(false),
                peer_half_closed: AtomicBool::new(false),
                terminal_seen: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                credit_waiters: Mutex::new(Vec::new()),
            }),
        }
    }

    fn protocol_violation(&self) -> RuntimeFailure {
        RuntimeFailure::ProtocolViolation {
            capability: self.state.capability,
        }
    }

    fn next_call_id() -> u64 {
        (1_u64 << 52) | NEXT_STREAM_CALL_ID.fetch_add(1, Ordering::Relaxed)
    }

    fn wake_credit_waiters(&self) {
        let waiters = self
            .state
            .credit_waiters
            .lock()
            .map(|mut waiters| std::mem::take(&mut *waiters))
            .unwrap_or_default();
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }

    fn restore_rejected_send(
        transport: &TransportClient,
        state: &Arc<TransportStreamState>,
        sequence: u64,
    ) -> Result<(), RuntimeFailure> {
        if state
            .next_send_sequence
            .compare_exchange(
                sequence.saturating_add(1),
                sequence,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(RuntimeFailure::ProtocolViolation {
                capability: state.capability,
            });
        }
        state.send_credit.fetch_add(1, Ordering::AcqRel);
        Self {
            transport: transport.clone(),
            state: state.clone(),
        }
        .wake_credit_waiters();
        Ok(())
    }

    fn register_credit_waiter(&self) -> Result<Option<oneshot::Receiver<()>>, RuntimeFailure> {
        if self.state.cancelled.load(Ordering::Acquire)
            || self.state.terminal_seen.load(Ordering::Acquire)
        {
            return Err(self.protocol_violation());
        }
        if self.state.send_credit.load(Ordering::Acquire) > 0 {
            return Ok(None);
        }
        let (sender, receiver) = oneshot::channel();
        let mut waiters =
            self.state
                .credit_waiters
                .lock()
                .map_err(|_| RuntimeFailure::Internal {
                    detail: "Bun stream credit waiter lock poisoned".to_owned(),
                })?;
        if self.state.cancelled.load(Ordering::Acquire)
            || self.state.terminal_seen.load(Ordering::Acquire)
        {
            return Err(self.protocol_violation());
        }
        if self.state.send_credit.load(Ordering::Acquire) > 0 {
            return Ok(None);
        }
        if waiters.len() >= MAX_STREAM_CREDIT_WAITERS {
            return Err(RuntimeFailure::ResourceExhausted {
                capability: self.state.capability,
                operation: self.state.operation.clone(),
            });
        }
        waiters.push(sender);
        Ok(Some(receiver))
    }
}

impl NativeStreamSession for TransportStreamSession {
    fn send(&self, message: Box<dyn Any>) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        if self.state.local_half_closed.load(Ordering::Acquire)
            || self.state.terminal_seen.load(Ordering::Acquire)
            || self.state.cancelled.load(Ordering::Acquire)
        {
            return Box::pin(futures::future::ready(Err(self.protocol_violation())));
        }
        let payload = match message.downcast::<Value>() {
            Ok(payload) => *payload,
            Err(_) => {
                return Box::pin(futures::future::ready(Err(self.protocol_violation())));
            }
        };
        let mut credit = self.state.send_credit.load(Ordering::Acquire);
        loop {
            if credit == 0 {
                match self.register_credit_waiter() {
                    Ok(Some(waiter)) => {
                        let session = Self {
                            transport: self.transport.clone(),
                            state: self.state.clone(),
                        };
                        return Box::pin(async move {
                            match waiter.await {
                                Ok(()) => session.send(Box::new(payload)).await,
                                Err(_) => Err(session.protocol_violation()),
                            }
                        });
                    }
                    Ok(None) => {
                        credit = self.state.send_credit.load(Ordering::Acquire);
                        continue;
                    }
                    Err(error) => {
                        return Box::pin(futures::future::ready(Err(error)));
                    }
                }
            }
            match self.state.send_credit.compare_exchange_weak(
                credit,
                credit - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => credit = current,
            }
        }
        let sequence = self.state.next_send_sequence.fetch_add(1, Ordering::AcqRel);
        let request_id = Self::next_call_id();
        let state = self.state.clone();
        let transport = self.transport.clone();
        let call = match transport.stream_call(
            WireStreamCall::Send {
                request_id,
                stream_id: state.stream_id,
                session: state.session.clone(),
                sequence,
                payload,
            },
            state.stream_id,
            &state.session,
            state.capability,
            &state.operation,
        ) {
            Ok(call) => call,
            Err(error) => {
                if matches!(error, RuntimeFailure::ResourceExhausted { .. })
                    && let Err(rollback_error) =
                        Self::restore_rejected_send(&transport, &state, sequence)
                {
                    return Box::pin(futures::future::ready(Err(rollback_error)));
                }
                return Box::pin(futures::future::ready(Err(error)));
            }
        };
        Box::pin(async move {
            match call.await? {
                WireStreamOutcome::Accepted { credit } => {
                    state.send_credit.store(credit as usize, Ordering::Release);
                    let session = Self {
                        transport: transport.clone(),
                        state: state.clone(),
                    };
                    session.wake_credit_waiters();
                    Ok(())
                }
                WireStreamOutcome::Runtime { failure } => {
                    if matches!(
                        failure,
                        crate::protocol::WireFailure::ResourceExhausted { .. }
                    ) {
                        Self::restore_rejected_send(&transport, &state, sequence)?;
                    }
                    Err(from_wire_failure(state.capability, failure))
                }
                _ => Err(RuntimeFailure::ProtocolViolation {
                    capability: state.capability,
                }),
            }
        })
    }

    fn receive(&self) -> LocalBoxFuture<'static, Result<NativeStreamItem, RuntimeFailure>> {
        if self.state.terminal_seen.load(Ordering::Acquire)
            || self.state.cancelled.load(Ordering::Acquire)
        {
            return Box::pin(futures::future::ready(Err(self.protocol_violation())));
        }
        if self
            .state
            .receive_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Box::pin(futures::future::ready(Err(
                RuntimeFailure::ResourceExhausted {
                    capability: self.state.capability,
                    operation: format!("{}.receive", self.state.operation),
                },
            )));
        }
        let request_id = Self::next_call_id();
        let state = self.state.clone();
        let transport = self.transport.clone();
        let call = match transport.stream_call(
            WireStreamCall::Receive {
                request_id,
                stream_id: state.stream_id,
                session: state.session.clone(),
            },
            state.stream_id,
            &state.session,
            state.capability,
            &state.operation,
        ) {
            Ok(call) => call,
            Err(error) => {
                state.receive_in_flight.store(false, Ordering::Release);
                return Box::pin(futures::future::ready(Err(error)));
            }
        };
        Box::pin(async move {
            let result = match call.await {
                Ok(WireStreamOutcome::Event { event }) => match event {
                    crate::protocol::WireStreamEvent::Message { sequence, payload } => {
                        if state.peer_half_closed.load(Ordering::Acquire)
                            || sequence != state.next_receive_sequence.load(Ordering::Acquire)
                        {
                            Err(RuntimeFailure::ProtocolViolation {
                                capability: state.capability,
                            })
                        } else {
                            state.next_receive_sequence.fetch_add(1, Ordering::AcqRel);
                            Ok(NativeStreamItem::Message(Box::new(payload)))
                        }
                    }
                    crate::protocol::WireStreamEvent::PeerHalfClosed => {
                        if state.peer_half_closed.swap(true, Ordering::AcqRel) {
                            Err(RuntimeFailure::ProtocolViolation {
                                capability: state.capability,
                            })
                        } else {
                            Ok(NativeStreamItem::PeerHalfClosed)
                        }
                    }
                    crate::protocol::WireStreamEvent::Terminal { outcome } => {
                        if state.terminal_seen.swap(true, Ordering::AcqRel) {
                            Err(RuntimeFailure::ProtocolViolation {
                                capability: state.capability,
                            })
                        } else {
                            let item = match outcome {
                                crate::protocol::WireStreamTerminal::Success => {
                                    NativeStreamItem::Terminal(Ok(()))
                                }
                                crate::protocol::WireStreamTerminal::Domain { value } => {
                                    NativeStreamItem::Terminal(Err(Box::new(value)))
                                }
                            };
                            let session = Self {
                                transport: transport.clone(),
                                state: state.clone(),
                            };
                            session.wake_credit_waiters();
                            Ok(item)
                        }
                    }
                },
                Ok(WireStreamOutcome::Runtime { failure }) => {
                    Err(from_wire_failure(state.capability, failure))
                }
                Ok(_) => Err(RuntimeFailure::ProtocolViolation {
                    capability: state.capability,
                }),
                Err(error) => Err(error),
            };
            state.receive_in_flight.store(false, Ordering::Release);
            result
        })
    }

    fn close_send(&self) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        if self.state.terminal_seen.load(Ordering::Acquire)
            || self.state.cancelled.load(Ordering::Acquire)
            || self.state.local_half_closed.swap(true, Ordering::AcqRel)
        {
            return Box::pin(futures::future::ready(Err(self.protocol_violation())));
        }
        let request_id = Self::next_call_id();
        let state = self.state.clone();
        let transport = self.transport.clone();
        let call = match transport.stream_call(
            WireStreamCall::CloseSend {
                request_id,
                stream_id: state.stream_id,
                session: state.session.clone(),
            },
            state.stream_id,
            &state.session,
            state.capability,
            &state.operation,
        ) {
            Ok(call) => call,
            Err(error) => {
                if matches!(error, RuntimeFailure::ResourceExhausted { .. }) {
                    state.local_half_closed.store(false, Ordering::Release);
                }
                return Box::pin(futures::future::ready(Err(error)));
            }
        };
        Box::pin(async move {
            let result = match call.await {
                Ok(WireStreamOutcome::Accepted { .. }) => Ok(()),
                Ok(WireStreamOutcome::Runtime { failure }) => {
                    Err(from_wire_failure(state.capability, failure))
                }
                Ok(_) => Err(RuntimeFailure::ProtocolViolation {
                    capability: state.capability,
                }),
                Err(error) => Err(error),
            };
            let resource_exhausted = result
                .as_ref()
                .err()
                .is_some_and(|error| matches!(error, RuntimeFailure::ResourceExhausted { .. }));
            if resource_exhausted {
                state.local_half_closed.store(false, Ordering::Release);
            }
            result
        })
    }

    fn cancel(&self) {
        if !self.state.cancelled.swap(true, Ordering::AcqRel) {
            self.wake_credit_waiters();
            self.transport
                .cancel_stream(self.state.stream_id, &self.state.session);
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
    capability_ids: CapabilityIds,
) -> Result<TransportClient, RuntimeFailure> {
    match wire {
        BunWire::FramedStdio => open_framed(process, expected, queue_capacity, capability_ids),
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
            open_json_rpc(process, address, expected, queue_capacity, capability_ids)
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
            control_sender,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            stream_pending: Arc::new(Mutex::new(BTreeMap::new())),
            cancelled: Arc::new(Mutex::new(BTreeSet::new())),
            stream_cancelled: Arc::new(Mutex::new(BTreeSet::new())),
            retired: Arc::new(Mutex::new(BTreeSet::new())),
            stream_retired: Arc::new(Mutex::new(BTreeSet::new())),
            max_frame_bytes: 4096,
            admission_capacity: 1,
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

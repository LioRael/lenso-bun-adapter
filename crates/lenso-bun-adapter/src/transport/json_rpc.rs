use super::{
    Arc, AtomicBool, BTreeMap, BTreeSet, CapabilityIds, ClientT, Handshake, HandshakeAck,
    HttpClient, Mutex, Ordering, PendingResponses, PendingStreamResponses, ProcessState, Receiver,
    RuntimeFailure, SocketAddr, StreamCall, StreamWireResult, SyncSender, TransportClient,
    TrySendError, Value, Weak, WireCall, WireEventPublish, WireOutcome, WireRequest, WireResult,
    WireStreamOutcome, build_json_rpc_client, capability_id, json_rpc_failure, json_rpc_runtime,
    mpsc, oneshot, protocol_violation, remember_request_id, rpc_params, thread, verify_handshake,
};

#[derive(Debug)]
pub(crate) struct JsonRpcTransport {
    pub(super) process: Arc<ProcessState>,
    sender: SyncSender<HttpCall>,
    event_sender: SyncSender<HttpCall>,
    cancel_sender: SyncSender<u64>,
    stream_sender: SyncSender<HttpStreamCall>,
    stream_cancel_sender: SyncSender<StreamCancelCall>,
    pending: PendingResponses,
    event_pending: PendingResponses,
    stream_pending: PendingStreamResponses,
    cancellations: Arc<Mutex<BTreeMap<u64, Arc<AtomicBool>>>>,
    stream_cancellations: Arc<Mutex<BTreeMap<u64, Arc<AtomicBool>>>>,
    retired: Arc<Mutex<BTreeSet<u64>>>,
    stream_retired: Arc<Mutex<BTreeSet<u64>>>,
    client: Arc<HttpClient>,
    address: SocketAddr,
    max_frame_bytes: usize,
    admission_capacity: usize,
    event_admission_capacity: usize,
    stream_admission_capacity: usize,
    pub(super) session: String,
    capability: &'static str,
    capability_ids: CapabilityIds,
}

struct HttpCall {
    payload: HttpCallPayload,
    cancelled: Arc<AtomicBool>,
}

#[derive(Clone, Debug)]
enum HttpCallPayload {
    Request(WireRequest),
    Event(WireEventPublish),
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
    event_queue_capacity: usize,
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
    let actual: HandshakeAck = thread::scope(|scope| {
        scope
            .spawn(|| {
                let runtime = json_rpc_runtime()?;
                runtime
                    .block_on(client.request("lenso.handshake", rpc_params![expected.clone()]))
                    .map_err(|error| json_rpc_failure("handshake", &error, capability))
            })
            .join()
    })
    .map_err(|_| RuntimeFailure::Internal {
        detail: "Bun JSON-RPC handshake worker panicked".to_owned(),
    })??;
    verify_handshake(expected, &actual, capability)?;
    let session = actual
        .session
        .filter(|session| !session.is_empty())
        .ok_or_else(|| protocol_violation(Some(capability)))?;

    let queue_capacity = queue_capacity.max(1);
    let event_queue_capacity = event_queue_capacity.max(1);
    let stream_capacity = queue_capacity.max(2);
    let (sender, receiver) = mpsc::sync_channel(queue_capacity);
    let (event_sender, event_receiver) = mpsc::sync_channel(event_queue_capacity);
    let (cancel_sender, cancel_receiver) = mpsc::sync_channel(queue_capacity.saturating_add(1));
    let (stream_sender, stream_receiver) = mpsc::sync_channel(stream_capacity);
    let (stream_cancel_sender, stream_cancel_receiver) =
        mpsc::sync_channel(queue_capacity.saturating_add(1));
    let transport = Arc::new(JsonRpcTransport {
        process: process.clone(),
        sender,
        event_sender,
        cancel_sender,
        stream_sender,
        stream_cancel_sender,
        pending: Arc::new(Mutex::new(BTreeMap::new())),
        event_pending: Arc::new(Mutex::new(BTreeMap::new())),
        stream_pending: Arc::new(Mutex::new(BTreeMap::new())),
        cancellations: Arc::new(Mutex::new(BTreeMap::new())),
        stream_cancellations: Arc::new(Mutex::new(BTreeMap::new())),
        retired: Arc::new(Mutex::new(BTreeSet::new())),
        stream_retired: Arc::new(Mutex::new(BTreeSet::new())),
        client: Arc::new(client),
        address,
        max_frame_bytes: expected.max_frame_bytes,
        admission_capacity: queue_capacity,
        event_admission_capacity: event_queue_capacity,
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
    spawn_json_rpc_worker(
        Arc::downgrade(&transport),
        receiver,
        "lenso-bun-json-rpc-worker",
    );
    spawn_json_rpc_worker(
        Arc::downgrade(&transport),
        event_receiver,
        "lenso-bun-json-rpc-event-worker",
    );
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

fn spawn_json_rpc_worker(
    transport: Weak<JsonRpcTransport>,
    receiver: Receiver<HttpCall>,
    thread_name: &'static str,
) {
    thread::Builder::new()
        .name(thread_name.to_owned())
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
                let (request_id, method, operation, params) = match call.payload {
                    HttpCallPayload::Request(request) => (
                        request.request_id,
                        "lenso.request",
                        "request",
                        serde_json::to_value(request).map_err(|_| protocol_violation(None)),
                    ),
                    HttpCallPayload::Event(event) => (
                        event.request_id,
                        "lenso.event.publish",
                        "event",
                        serde_json::to_value(event).map_err(|_| protocol_violation(None)),
                    ),
                };
                let params = match params {
                    Ok(params) => params,
                    Err(error) => {
                        transport.finish(request_id, Err(error));
                        continue;
                    }
                };
                if call.cancelled.load(Ordering::Acquire) {
                    transport.finish(request_id, Err(RuntimeFailure::Cancelled { request_id }));
                    continue;
                }
                let result = if call.cancelled.load(Ordering::Acquire) {
                    Err(RuntimeFailure::Cancelled { request_id })
                } else {
                    runtime
                        .block_on(
                            transport
                                .client
                                .request::<WireOutcome, _>(method, rpc_params![params]),
                        )
                        .map_err(|error| json_rpc_failure(operation, &error, transport.capability))
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
    pub(super) fn stream_request(
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

    pub(super) fn request(
        self: &Arc<Self>,
        request: WireRequest,
    ) -> Result<WireCall, RuntimeFailure> {
        self.call(HttpCallPayload::Request(request))
    }

    pub(super) fn event(
        self: &Arc<Self>,
        event: WireEventPublish,
    ) -> Result<WireCall, RuntimeFailure> {
        self.call(HttpCallPayload::Event(event))
    }

    fn call(self: &Arc<Self>, payload: HttpCallPayload) -> Result<WireCall, RuntimeFailure> {
        if !self.process.is_alive() {
            return Err(self
                .process
                .failure()
                .unwrap_or(RuntimeFailure::Unavailable {
                    capability: self.capability,
                }));
        }
        let is_event = matches!(&payload, HttpCallPayload::Event(_));
        let (payload, request_id, request_capability, operation, method, request_value) =
            match payload {
                HttpCallPayload::Request(mut request) => {
                    request.session = Some(self.session.clone());
                    let request_id = request.request_id;
                    let capability = capability_id(&self.capability_ids, &request.capability_id);
                    let operation = request.operation.clone();
                    let value = serde_json::to_value(&request)
                        .map_err(|_| protocol_violation(Some(self.capability)))?;
                    (
                        HttpCallPayload::Request(request),
                        request_id,
                        capability,
                        operation,
                        "lenso.request",
                        value,
                    )
                }
                HttpCallPayload::Event(mut event) => {
                    event.session = Some(self.session.clone());
                    let request_id = event.request_id;
                    let capability = capability_id(&self.capability_ids, &event.capability_id);
                    let operation = event.operation.clone();
                    let value = serde_json::to_value(&event)
                        .map_err(|_| protocol_violation(Some(self.capability)))?;
                    (
                        HttpCallPayload::Event(event),
                        request_id,
                        capability,
                        operation,
                        "lenso.event.publish",
                        value,
                    )
                }
            };
        let wire_sender = if is_event {
            &self.event_sender
        } else {
            &self.sender
        };
        let pending_registry = if is_event {
            &self.event_pending
        } else {
            &self.pending
        };
        let admission_capacity = if is_event {
            self.event_admission_capacity
        } else {
            self.admission_capacity
        };
        let encoded_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": [&request_value],
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
        let mut pending = pending_registry
            .lock()
            .map_err(|_| RuntimeFailure::Internal {
                detail: "Bun JSON-RPC pending response lock poisoned".to_owned(),
            })?;
        if pending.contains_key(&request_id) {
            self.cancellations
                .lock()
                .ok()
                .and_then(|mut cancellations| cancellations.remove(&request_id));
            return Err(protocol_violation(Some(self.capability)));
        }
        if pending.len() >= admission_capacity {
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
        match wire_sender.try_send(HttpCall { payload, cancelled }) {
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
                pending_registry
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
                pending_registry
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

    pub(super) fn cancel(&self, request_id: u64) {
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
                .and_then(|mut pending| pending.remove(&request_id))
                .or_else(|| {
                    self.event_pending
                        .lock()
                        .ok()
                        .and_then(|mut pending| pending.remove(&request_id))
                });
            remember_request_id(&self.retired, request_id);
            if self.cancel_sender.try_send(request_id).is_err() {
                self.process.mark_dead(RuntimeFailure::ModuleFailure {
                    detail: "Bun JSON-RPC cancellation channel stopped".to_owned(),
                });
            }
        }
    }

    pub(super) fn cancel_stream_call(&self, request_id: u64, stream_id: u64, session: &str) {
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

    pub(super) fn cancel_stream(&self, stream_id: u64, session: &str) {
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
            .or_else(|| {
                self.event_pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&request_id))
            })
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
        if let Ok(mut pending) = self.event_pending.lock() {
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

    pub(super) fn shutdown(&self) {
        self.process.stop();
    }
}

impl Drop for JsonRpcTransport {
    fn drop(&mut self) {
        self.process.stop();
    }
}

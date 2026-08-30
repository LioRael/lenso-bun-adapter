use super::{
    Arc, BTreeMap, BTreeSet, CapabilityIds, ChildStdin, ChildStdout, Duration, FramedMessage,
    Handshake, MAX_CANCELLED_REQUEST_IDS, Mutex, PROCESS_STARTUP_TIMEOUT, PendingResponses,
    PendingStreamResponses, ProcessState, Receiver, RecvTimeoutError, RuntimeFailure, StreamCall,
    SyncSender, TransportClient, TrySendError, WireCall, WireEventPublish, WireRequest,
    capability_id, encode_frame, mpsc, oneshot, protocol_violation, read_frame,
    remember_request_id, thread, verify_handshake, write_frame,
};
use std::io::Write;

#[derive(Debug)]
pub(crate) struct FramedTransport {
    pub(super) process: Arc<ProcessState>,
    pub(super) sender: SyncSender<Vec<u8>>,
    pub(super) event_sender: SyncSender<Vec<u8>>,
    pub(super) control_sender: SyncSender<Vec<u8>>,
    pub(super) pending: PendingResponses,
    pub(super) event_pending: PendingResponses,
    pub(super) stream_pending: PendingStreamResponses,
    pub(super) cancelled: Arc<Mutex<BTreeSet<u64>>>,
    pub(super) stream_cancelled: Arc<Mutex<BTreeSet<u64>>>,
    pub(super) retired: Arc<Mutex<BTreeSet<u64>>>,
    pub(super) stream_retired: Arc<Mutex<BTreeSet<u64>>>,
    pub(super) max_frame_bytes: usize,
    pub(super) admission_capacity: usize,
    pub(super) event_admission_capacity: usize,
    pub(super) stream_admission_capacity: usize,
    pub(super) session: String,
    pub(super) capability: &'static str,
    pub(super) capability_ids: CapabilityIds,
}

pub(crate) fn open_framed(
    process: &Arc<ProcessState>,
    expected: &Handshake,
    queue_capacity: usize,
    event_queue_capacity: usize,
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
            return Err(RuntimeFailure::PluginFailure {
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
    let event_queue_capacity = event_queue_capacity.max(1);
    let (event_sender, event_receiver) = mpsc::sync_channel(event_queue_capacity);
    let (control_sender, control_receiver) = mpsc::sync_channel(queue_capacity.saturating_add(1));
    let transport = Arc::new(FramedTransport {
        process: process.clone(),
        sender,
        event_sender,
        control_sender,
        pending: Arc::new(Mutex::new(BTreeMap::new())),
        event_pending: Arc::new(Mutex::new(BTreeMap::new())),
        stream_pending: Arc::new(Mutex::new(BTreeMap::new())),
        cancelled: Arc::new(Mutex::new(BTreeSet::new())),
        stream_cancelled: Arc::new(Mutex::new(BTreeSet::new())),
        retired: Arc::new(Mutex::new(BTreeSet::new())),
        stream_retired: Arc::new(Mutex::new(BTreeSet::new())),
        max_frame_bytes: expected.max_frame_bytes,
        admission_capacity: queue_capacity,
        event_admission_capacity: event_queue_capacity,
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
    spawn_framed_writer(
        process.clone(),
        stdin,
        receiver,
        event_receiver,
        control_receiver,
    );
    spawn_framed_reader(&transport, stdout);
    Ok(TransportClient::Framed(transport))
}

fn spawn_framed_writer(
    process: Arc<ProcessState>,
    mut stdin: ChildStdin,
    receiver: Receiver<Vec<u8>>,
    event_receiver: Receiver<Vec<u8>>,
    control_receiver: Receiver<Vec<u8>>,
) {
    thread::Builder::new()
        .name("lenso-bun-framed-writer".to_owned())
        .spawn(move || {
            loop {
                let frame = match control_receiver.try_recv() {
                    Ok(frame) => frame,
                    Err(_) => match event_receiver.try_recv() {
                        Ok(frame) => frame,
                        Err(_) => match receiver.try_recv() {
                            Ok(frame) => frame,
                            Err(_) => match receiver.recv_timeout(Duration::from_millis(5)) {
                                Ok(frame) => frame,
                                Err(RecvTimeoutError::Timeout) => continue,
                                Err(RecvTimeoutError::Disconnected) => break,
                            },
                        },
                    },
                };
                if let Err(error) = stdin.write_all(&frame).and_then(|()| stdin.flush()) {
                    process.mark_dead(RuntimeFailure::PluginFailure {
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
    let event_pending = transport.event_pending.clone();
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
                            .and_then(|mut pending| pending.remove(&request_id))
                            .or_else(|| {
                                event_pending
                                    .lock()
                                    .ok()
                                    .and_then(|mut pending| pending.remove(&request_id))
                            });
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
    pub(super) fn request(
        self: &Arc<Self>,
        request: WireRequest,
    ) -> Result<WireCall, RuntimeFailure> {
        let request_id = request.request_id;
        let request_capability = capability_id(&self.capability_ids, &request.capability_id);
        let operation = request.operation.clone();
        self.call_on(
            FramedMessage::Request(request),
            request_id,
            request_capability,
            operation,
            &self.sender,
            &self.pending,
            self.admission_capacity,
        )
    }

    pub(super) fn event(
        self: &Arc<Self>,
        event: WireEventPublish,
    ) -> Result<WireCall, RuntimeFailure> {
        let request_id = event.request_id;
        let request_capability = capability_id(&self.capability_ids, &event.capability_id);
        let operation = event.operation.clone();
        self.call_on(
            FramedMessage::EventPublish(event),
            request_id,
            request_capability,
            operation,
            &self.event_sender,
            &self.event_pending,
            self.event_admission_capacity,
        )
    }

    #[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
    fn call_on(
        self: &Arc<Self>,
        message: FramedMessage,
        request_id: u64,
        request_capability: &'static str,
        operation: String,
        wire_sender: &SyncSender<Vec<u8>>,
        pending_registry: &PendingResponses,
        admission_capacity: usize,
    ) -> Result<WireCall, RuntimeFailure> {
        if !self.process.is_alive() {
            return Err(self
                .process
                .failure()
                .unwrap_or(RuntimeFailure::Unavailable {
                    capability: self.capability,
                }));
        }
        let frame = encode_frame(&message, self.max_frame_bytes)?;
        let (response_sender, receiver) = oneshot::channel();
        let mut pending = pending_registry
            .lock()
            .map_err(|_| RuntimeFailure::Internal {
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
        if pending.len() >= admission_capacity {
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
        pending.insert(request_id, response_sender);
        drop(pending);
        match wire_sender.try_send(frame) {
            Ok(()) => Ok(WireCall::new(
                request_id,
                TransportClient::Framed(self.clone()),
                receiver,
            )),
            Err(TrySendError::Full(_)) => {
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
                pending_registry
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&request_id));
                let error =
                    self.process
                        .failure_or_exit()
                        .unwrap_or(RuntimeFailure::PluginFailure {
                            detail: "Bun framed-stdio writer stopped".to_owned(),
                        });
                self.process.mark_dead(error.clone());
                Err(error)
            }
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(super) fn stream_request(
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
                        .unwrap_or(RuntimeFailure::PluginFailure {
                            detail: "Bun framed-stdio writer stopped".to_owned(),
                        });
                self.process.mark_dead(error.clone());
                Err(error)
            }
        }
    }

    pub(super) fn cancel(&self, request_id: u64) {
        let removed = self
            .pending
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(&request_id))
            .or_else(|| {
                self.event_pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&request_id))
            });
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
            self.process.mark_dead(RuntimeFailure::PluginFailure {
                detail: "Bun framed-stdio cancellation channel stopped".to_owned(),
            });
        }
    }

    pub(super) fn cancel_stream_call(&self, request_id: u64, stream_id: u64, session: &str) {
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

    pub(super) fn cancel_stream(&self, stream_id: u64, session: &str) {
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
            self.process.mark_dead(RuntimeFailure::PluginFailure {
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
        if let Ok(mut pending) = self.event_pending.lock() {
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

    pub(super) fn shutdown(&self) {
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

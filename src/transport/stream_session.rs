use super::{
    Any, Arc, AtomicBool, AtomicU64, AtomicUsize, LocalBoxFuture, MAX_STREAM_CREDIT_WAITERS, Mutex,
    NEXT_STREAM_CALL_ID, NativeStreamItem, NativeStreamSession, Ordering, RuntimeFailure,
    TransportClient, Value, WireStreamCall, WireStreamOutcome, from_wire_failure, oneshot,
};

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

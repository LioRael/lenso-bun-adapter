use std::{
    cell::RefCell,
    collections::BTreeMap,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc as std_mpsc,
    },
    thread,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::{
    FutureExt as _, StreamExt as _,
    channel::{mpsc, oneshot},
    select,
};
use lenso_app_plan::{CapabilityCardinality, PluginInstancePlan};
use lenso_kernel::{
    ActivateContext, DeactivateContext, InvocationContext, ManagedResource, PluginLifecycle,
    PreparedNativePlugin, RuntimeFailure,
};
use lenso_process_protocol::{
    VALUE_PROFILE,
    authoring::{
        AuthoringLimits, CancelParams, ConstructParams, EventPublishOutcome, EventPublishParams,
        EventPublishResult, FactoryOutcome, InitializeParams, InvocationOutcome, InvocationResult,
        InvocationScope, OutboundCallParams, OutboundCallResult, OutboundEventPublishParams,
        OutboundEventPublishResult, OutboundStreamOpenParams, OutboundStreamOpenResult,
        ProvidedEndpoint, RequirementCardinality, RequirementDeclaration, RouteDescriptor,
        RuntimeFailure as WireFailure, SessionIdentity, Settlement, SettlementState,
        StopHookOutcome, StopParams, StreamActionOutcome, StreamActionResult, StreamCancelParams,
        StreamCloseSendParams, StreamOpenOutcome, StreamOpenParams, StreamOpenResult,
        StreamReceiveOutcome, StreamReceiveParams, StreamReceiveResult, StreamSendParams,
        StreamTerminalOutcome,
    },
};
use lenso_runtime_codec::{
    ArtifactCatalog, ArtifactHandle, JsonCapabilityCodec, JsonEventTransport, JsonHostImports,
    JsonInvocationOutcome, JsonRequestTransport, JsonStreamItem, JsonStreamSessionTransport,
    JsonStreamTransport, codecs_for_instance, codecs_for_requirements, json_event_endpoints,
    json_request_endpoints, json_stream_endpoints,
};
use sha2::{Digest as _, Sha256};

use crate::{
    BUN_AUTHORING_RUNTIME_PROFILE, BunAdapterConfig, BunAuthoringCallback, BunAuthoringHost,
};

const EXECUTION_CLASS: &str = "bun-child-process";
static NEXT_BUN_SESSION: AtomicU64 = AtomicU64::new(1);
static NEXT_BUN_STREAM_ACTION: AtomicU64 = AtomicU64::new(1);

type SettlementSenders = Arc<Mutex<BTreeMap<String, oneshot::Sender<Settlement>>>>;
type SharedScopes = Arc<Mutex<BTreeMap<String, InvocationScope>>>;
type CallbackSender = Arc<Mutex<mpsc::Sender<BridgeRequest>>>;

pub(super) fn prepare_instance(
    config: &BunAdapterConfig,
    artifacts: &ArtifactCatalog,
    codecs: &BTreeMap<String, Rc<dyn JsonCapabilityCodec>>,
    instance: &PluginInstancePlan,
) -> Result<PreparedNativePlugin, RuntimeFailure> {
    let provided = codecs_for_instance(instance, codecs)?;
    let required = codecs_for_requirements(instance, codecs)?;
    for codec in provided.iter().chain(&required) {
        validate_digest(codec.descriptor_digest(), codec.capability_id())?;
    }
    let imports = Rc::new(JsonHostImports::new(
        required.clone(),
        config.request_queue_capacity,
    )?);
    let generation = BunGenerationV2::new(
        artifacts.require(instance.instance_key())?.clone(),
        instance.clone(),
        provided.clone(),
        required,
        imports,
        config.clone(),
    )?;
    let requests = json_request_endpoints(generation.clone(), provided.clone());
    let streams = json_stream_endpoints(generation.clone(), provided.clone());
    let events = json_event_endpoints(generation.clone(), provided);
    Ok(PreparedNativePlugin::with_all_endpoints(
        requests,
        streams,
        events,
        BunLifecycleV2 { generation },
    ))
}

struct BunGenerationV2 {
    artifact: ArtifactHandle,
    instance: PluginInstancePlan,
    provided: Vec<Rc<dyn JsonCapabilityCodec>>,
    required: Vec<Rc<dyn JsonCapabilityCodec>>,
    endpoints: BTreeMap<String, EndpointIdentity>,
    imports: Rc<JsonHostImports>,
    config: BunAdapterConfig,
    identity: SessionIdentity,
    initialization: RefCell<Option<InitializeParams>>,
    host: RefCell<Option<Arc<BunAuthoringHost>>>,
    callback_sender: RefCell<Option<CallbackSender>>,
    contexts: Rc<RefCell<BTreeMap<String, InvocationContext>>>,
    outbound_receive_sequences: Rc<RefCell<BTreeMap<u64, u64>>>,
    shared_scopes: SharedScopes,
    settlements: SettlementSenders,
    stop_started: AtomicBool,
}

#[derive(Clone, Debug)]
struct EndpointIdentity {
    endpoint_id: String,
    descriptor_version: String,
    descriptor_digest: String,
}

impl std::fmt::Debug for BunGenerationV2 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BunGenerationV2")
            .field("instance", &self.instance.instance_key())
            .field("started", &self.host.borrow().is_some())
            .finish_non_exhaustive()
    }
}

impl BunGenerationV2 {
    fn new(
        artifact: ArtifactHandle,
        instance: PluginInstancePlan,
        provided: Vec<Rc<dyn JsonCapabilityCodec>>,
        required: Vec<Rc<dyn JsonCapabilityCodec>>,
        imports: Rc<JsonHostImports>,
        config: BunAdapterConfig,
    ) -> Result<Rc<Self>, RuntimeFailure> {
        let generation = NEXT_BUN_SESSION.fetch_add(1, Ordering::Relaxed);
        if generation == 0 {
            return Err(RuntimeFailure::ResourceExhausted {
                capability: EXECUTION_CLASS,
                operation: "session".to_owned(),
            });
        }
        let identity = SessionIdentity {
            session: random_session()?,
            plugin_instance: instance.instance_key().to_owned(),
            plugin_generation: generation.to_string(),
            artifact_digest: artifact.digest().to_owned(),
            contract_digest: contract_digest(&instance, &provided, &required),
            runtime_profile: BUN_AUTHORING_RUNTIME_PROFILE.to_owned(),
            value_profile: VALUE_PROFILE.to_owned(),
        };
        let endpoints = provided
            .iter()
            .enumerate()
            .map(|(index, codec)| {
                (
                    codec.capability_id().to_owned(),
                    EndpointIdentity {
                        endpoint_id: format!("endpoint-{index}"),
                        descriptor_version: codec.descriptor_version().to_owned(),
                        descriptor_digest: codec.descriptor_digest().to_owned(),
                    },
                )
            })
            .collect();
        Ok(Rc::new(Self {
            artifact,
            instance,
            provided,
            required,
            endpoints,
            imports,
            config,
            identity,
            initialization: RefCell::new(None),
            host: RefCell::new(None),
            callback_sender: RefCell::new(None),
            contexts: Rc::new(RefCell::new(BTreeMap::new())),
            outbound_receive_sequences: Rc::new(RefCell::new(BTreeMap::new())),
            shared_scopes: SharedScopes::default(),
            settlements: SettlementSenders::default(),
            stop_started: AtomicBool::new(false),
        }))
    }

    fn initialize(
        &self,
        dependencies: &lenso_kernel::PluginDependencies,
    ) -> Result<InitializeParams, RuntimeFailure> {
        self.imports.activate(dependencies)?;
        let bindings = self.imports.descriptors()?;
        let mut orders = BTreeMap::<String, u32>::new();
        let mut routes = bindings
            .into_iter()
            .map(|binding| {
                let order = orders.entry(binding.requirement_id.clone()).or_default();
                let route = RouteDescriptor {
                    route_id: format!("route-{}", binding.binding_id),
                    requirement_id: binding.requirement_id,
                    capability_id: binding.capability_id,
                    descriptor_version: binding.descriptor_version,
                    descriptor_digest: binding.descriptor_digest,
                    provider_instance: binding.provider_instance,
                    provider_order: *order,
                };
                *order += 1;
                route
            })
            .collect::<Vec<_>>();
        routes.sort_by(|left, right| {
            (
                &left.requirement_id,
                left.provider_order,
                &left.provider_instance,
            )
                .cmp(&(
                    &right.requirement_id,
                    right.provider_order,
                    &right.provider_instance,
                ))
        });
        let mut required_declarations = self
            .instance
            .required_capabilities()
            .iter()
            .map(|requirement| {
                let codec = self
                    .required
                    .iter()
                    .find(|codec| codec.capability_id() == requirement.capability_id())
                    .expect("required codecs were validated during preparation");
                RequirementDeclaration {
                    requirement_id: requirement.requirement_id().to_owned(),
                    capability_id: requirement.capability_id().to_owned(),
                    descriptor_version: requirement.descriptor_version().to_owned(),
                    descriptor_digest: codec.descriptor_digest().to_owned(),
                    cardinality: cardinality(requirement.cardinality()),
                }
            })
            .collect::<Vec<_>>();
        required_declarations.sort_by(|left, right| left.requirement_id.cmp(&right.requirement_id));
        let mut provided_endpoints = self
            .provided
            .iter()
            .map(|codec| {
                let endpoint = &self.endpoints[codec.capability_id()];
                ProvidedEndpoint {
                    endpoint_id: endpoint.endpoint_id.clone(),
                    capability_id: codec.capability_id().to_owned(),
                    descriptor_version: endpoint.descriptor_version.clone(),
                    descriptor_digest: endpoint.descriptor_digest.clone(),
                }
            })
            .collect::<Vec<_>>();
        provided_endpoints.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        let max_pending = u32::try_from(self.config.request_queue_capacity)
            .unwrap_or(u32::MAX)
            .min(1_024);
        let initialization = InitializeParams {
            api_version: lenso_process_protocol::authoring::AUTHORING_API_VERSION,
            identity: self.identity.clone(),
            config: serde_json::from_str(self.instance.configuration()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: format!("invalid Bun Authoring V2 configuration: {error}"),
                }
            })?,
            required_declarations,
            routes,
            provided_endpoints,
            limits: AuthoringLimits {
                max_frame_bytes: self.config.max_frame_bytes as u64,
                max_active_invocations: max_pending,
                max_active_outbound_calls: max_pending,
                max_queued_calls: max_pending,
                max_unfinished_executions: max_pending,
                max_retired_ids: max_pending.saturating_mul(16).max(1),
            },
        };
        initialization
            .validate()
            .map_err(|error| protocol(error.to_string()))?;
        Ok(initialization)
    }

    fn terminate(&self) {
        if let Some(host) = self.host.borrow_mut().take() {
            host.terminate();
        }
        self.settlements.lock().expect("Bun settlements").clear();
        self.shared_scopes.lock().expect("Bun scopes").clear();
        self.contexts.borrow_mut().clear();
        self.outbound_receive_sequences.borrow_mut().clear();
        self.imports.deactivate();
    }
}

impl JsonRequestTransport for BunGenerationV2 {
    #[expect(
        clippy::too_many_lines,
        reason = "the invocation state machine keeps RPC outcome, settlement and cancellation ordering together"
    )]
    fn invoke(
        self: Rc<Self>,
        capability: String,
        operation: String,
        request_json: String,
        context: InvocationContext,
    ) -> futures::future::LocalBoxFuture<'static, Result<JsonInvocationOutcome, RuntimeFailure>>
    {
        Box::pin(async move {
            let initialization = self
                .initialization
                .borrow()
                .clone()
                .ok_or(RuntimeFailure::AdmissionClosed)?;
            let host = self
                .host
                .borrow()
                .clone()
                .ok_or(RuntimeFailure::AdmissionClosed)?;
            let endpoint =
                self.endpoints
                    .get(&capability)
                    .ok_or(RuntimeFailure::ProtocolViolation {
                        capability: EXECUTION_CLASS,
                    })?;
            let correlation_id = context.request_id().to_string();
            let scope = invocation_scope(&context)?;
            let params = lenso_process_protocol::authoring::InvokeParams {
                session: self.identity.session.clone(),
                correlation_id: correlation_id.clone(),
                endpoint_id: endpoint.endpoint_id.clone(),
                capability_id: capability,
                descriptor_version: endpoint.descriptor_version.clone(),
                descriptor_digest: endpoint.descriptor_digest.clone(),
                operation: operation.clone(),
                scope: scope.clone(),
                payload: serde_json::from_str(&request_json).map_err(|_| {
                    RuntimeFailure::ProtocolViolation {
                        capability: EXECUTION_CLASS,
                    }
                })?,
            };
            params
                .validate_against(&initialization)
                .map_err(|error| protocol(error.to_string()))?;

            let (settlement_sender, settlement_receiver) = oneshot::channel();
            {
                let mut settlements = self.settlements.lock().expect("Bun settlements");
                if settlements.len() >= self.config.request_queue_capacity {
                    return Err(RuntimeFailure::ResourceExhausted {
                        capability: EXECUTION_CLASS,
                        operation,
                    });
                }
                if settlements
                    .insert(correlation_id.clone(), settlement_sender)
                    .is_some()
                {
                    return Err(protocol("Bun invocation reused a correlation id"));
                }
            }
            self.contexts
                .borrow_mut()
                .insert(scope.scope_id.clone(), context.clone());
            self.shared_scopes
                .lock()
                .expect("Bun scopes")
                .insert(scope.scope_id.clone(), scope.clone());

            let rpc_params = params.clone();
            let rpc_host = host.clone();
            let mut response = spawn_rpc("invoke", move || rpc_host.invoke(&rpc_params))?
                .boxed_local()
                .fuse();
            let mut settlement = settlement_receiver.boxed_local().fuse();
            let mut cancelled = context.cancellation().cancelled().boxed_local().fuse();
            let mut outcome = None;
            let mut settled = None;
            let mut cancellation_sent = false;
            loop {
                select! {
                    result = response => {
                        match result {
                            Ok(Ok(result)) => outcome = Some(from_wire_outcome(result.outcome)),
                            Ok(Err(error)) => {
                                self.retire_invocation(&correlation_id, &scope.scope_id);
                                return Err(error);
                            }
                            Err(_) => {
                                self.retire_invocation(&correlation_id, &scope.scope_id);
                                return Err(unavailable());
                            }
                        }
                        response = futures::future::pending().boxed_local().fuse();
                    },
                    result = settlement => {
                        let value = result.map_err(|_| unavailable())?;
                        value
                            .validate_for(&self.identity)
                            .map_err(|error| protocol(error.to_string()))?;
                        if value.scope_id != scope.scope_id
                            || value.correlation_id != correlation_id
                        {
                            self.terminate();
                            return Err(protocol("Bun settlement identity mismatch"));
                        }
                        settled = Some(value.state);
                        settlement = futures::future::pending().boxed_local().fuse();
                    },
                    () = cancelled => {
                        cancellation_sent = true;
                        let cancel = CancelParams {
                            session: self.identity.session.clone(),
                            scope_id: scope.scope_id.clone(),
                            correlation_id: correlation_id.clone(),
                            reason: "caller cancelled the invocation".to_owned(),
                        };
                        let cancel_host = host.clone();
                        let identity = cancel.clone();
                        let _ = thread::Builder::new()
                            .name(format!("lenso-bun-v2-cancel-{correlation_id}"))
                            .spawn(move || match cancel_host.cancel(cancel) {
                                Ok(ack) if ack.validate_for(&identity).is_ok() => {}
                                _ => cancel_host.terminate(),
                            });
                        arm_termination(
                            host.clone(),
                            self.settlements.clone(),
                            correlation_id.clone(),
                            self.config.cancellation_settlement_timeout,
                        );
                        cancelled = futures::future::pending().boxed_local().fuse();
                    }
                }
                if let (Some(outcome), Some(state)) = (outcome.take(), settled) {
                    self.retire_invocation(&correlation_id, &scope.scope_id);
                    if cancellation_sent && state != SettlementState::Completed {
                        return Err(RuntimeFailure::Cancelled {
                            request_id: context.request_id(),
                        });
                    }
                    return outcome;
                }
            }
        })
    }
}

impl JsonEventTransport for BunGenerationV2 {
    fn publish(
        self: Rc<Self>,
        capability: String,
        operation: String,
        event_json: String,
        context: InvocationContext,
    ) -> futures::future::LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        Box::pin(async move {
            let initialization = self
                .initialization
                .borrow()
                .clone()
                .ok_or(RuntimeFailure::AdmissionClosed)?;
            let host = self
                .host
                .borrow()
                .clone()
                .ok_or(RuntimeFailure::AdmissionClosed)?;
            let endpoint = self
                .endpoints
                .get(&capability)
                .ok_or_else(|| protocol("unknown Bun Event endpoint"))?;
            let params = EventPublishParams {
                session: self.identity.session.clone(),
                correlation_id: context.request_id().to_string(),
                endpoint_id: endpoint.endpoint_id.clone(),
                capability_id: capability,
                descriptor_version: endpoint.descriptor_version.clone(),
                descriptor_digest: endpoint.descriptor_digest.clone(),
                operation,
                scope: invocation_scope(&context)?,
                event: serde_json::from_str(&event_json)
                    .map_err(|_| protocol("invalid Bun Event payload"))?,
            };
            params
                .validate_against(&initialization)
                .map_err(|error| protocol(error.to_string()))?;
            let scope_id = params.scope.scope_id.clone();
            self.contexts.borrow_mut().insert(scope_id.clone(), context);
            self.shared_scopes
                .lock()
                .expect("Bun scopes")
                .insert(scope_id.clone(), params.scope.clone());
            let request = params.clone();
            let response = spawn_rpc("event-publish", move || host.publish_event(&request))?
                .await
                .map_err(|_| unavailable());
            self.retire_lifecycle(&scope_id);
            let result = response??;
            match result.outcome {
                EventPublishOutcome::Accepted => Ok(()),
                EventPublishOutcome::Runtime { failure } => Err(from_wire_failure(failure)),
            }
        })
    }
}

impl JsonStreamTransport for BunGenerationV2 {
    #[allow(clippy::too_many_lines)]
    fn open(
        self: Rc<Self>,
        capability: String,
        operation: String,
        request_json: String,
        context: InvocationContext,
    ) -> lenso_runtime_codec::JsonStreamOpenFuture {
        Box::pin(async move {
            let initialization = self
                .initialization
                .borrow()
                .clone()
                .ok_or(RuntimeFailure::AdmissionClosed)?;
            let host = self
                .host
                .borrow()
                .clone()
                .ok_or(RuntimeFailure::AdmissionClosed)?;
            let endpoint = self
                .endpoints
                .get(&capability)
                .ok_or_else(|| protocol("unknown Bun Stream endpoint"))?;
            let correlation_id = context.request_id().to_string();
            let scope = invocation_scope(&context)?;
            let params = StreamOpenParams {
                session: self.identity.session.clone(),
                correlation_id: correlation_id.clone(),
                endpoint_id: endpoint.endpoint_id.clone(),
                capability_id: capability.clone(),
                descriptor_version: endpoint.descriptor_version.clone(),
                descriptor_digest: endpoint.descriptor_digest.clone(),
                operation,
                scope: scope.clone(),
                request: serde_json::from_str(&request_json)
                    .map_err(|_| protocol("invalid Bun Stream open payload"))?,
            };
            params
                .validate_against(&initialization)
                .map_err(|error| protocol(error.to_string()))?;
            let (settlement_sender, settlement_receiver) = oneshot::channel();
            {
                let mut settlements = self.settlements.lock().expect("Bun settlements");
                if settlements.len() >= self.config.request_queue_capacity {
                    return Err(RuntimeFailure::ResourceExhausted {
                        capability: EXECUTION_CLASS,
                        operation: "stream.open".to_owned(),
                    });
                }
                if settlements
                    .insert(correlation_id.clone(), settlement_sender)
                    .is_some()
                {
                    return Err(protocol("Bun Stream open reused a correlation id"));
                }
            }
            self.contexts
                .borrow_mut()
                .insert(scope.scope_id.clone(), context.clone());
            self.shared_scopes
                .lock()
                .expect("Bun scopes")
                .insert(scope.scope_id.clone(), scope.clone());
            let request = params.clone();
            let rpc_host = host.clone();
            let mut response = spawn_rpc("stream-open", move || rpc_host.open_stream(&request))?
                .boxed_local()
                .fuse();
            let mut settlement = settlement_receiver.boxed_local().fuse();
            let mut cancelled = context.cancellation().cancelled().boxed_local().fuse();
            let mut outcome = None;
            let mut settled = None;
            let mut cancellation_sent = false;
            loop {
                select! {
                    result = response => {
                        match result {
                            Ok(Ok(result)) => outcome = Some(result.outcome),
                            Ok(Err(error)) => {
                                self.retire_invocation(&correlation_id, &scope.scope_id);
                                return Err(error);
                            }
                            Err(_) => {
                                self.retire_invocation(&correlation_id, &scope.scope_id);
                                return Err(unavailable());
                            }
                        }
                        response = futures::future::pending().boxed_local().fuse();
                    },
                    result = settlement => {
                        let Ok(value) = result else {
                            self.retire_invocation(&correlation_id, &scope.scope_id);
                            return Err(unavailable());
                        };
                        if value.validate_for(&self.identity).is_err()
                            || value.scope_id != scope.scope_id
                            || value.correlation_id != correlation_id
                        {
                            self.retire_invocation(&correlation_id, &scope.scope_id);
                            self.terminate();
                            return Err(protocol("Bun Stream open settlement identity mismatch"));
                        }
                        settled = Some(value.state);
                        settlement = futures::future::pending().boxed_local().fuse();
                    },
                    () = cancelled => {
                        cancellation_sent = true;
                        let cancel = CancelParams {
                            session: self.identity.session.clone(),
                            scope_id: scope.scope_id.clone(),
                            correlation_id: correlation_id.clone(),
                            reason: "caller cancelled the Stream open".to_owned(),
                        };
                        let cancel_host = host.clone();
                        let identity = cancel.clone();
                        let _ = thread::Builder::new()
                            .name(format!("lenso-bun-v2-stream-open-cancel-{correlation_id}"))
                            .spawn(move || match cancel_host.cancel(cancel) {
                                Ok(ack) if ack.validate_for(&identity).is_ok() => {}
                                _ => cancel_host.terminate(),
                            });
                        arm_termination(
                            host.clone(),
                            self.settlements.clone(),
                            correlation_id.clone(),
                            self.config.cancellation_settlement_timeout,
                        );
                        cancelled = futures::future::pending().boxed_local().fuse();
                    }
                }
                let (Some(outcome), Some(state)) = (outcome.take(), settled) else {
                    continue;
                };
                if cancellation_sent && state != SettlementState::Completed {
                    self.retire_invocation(&correlation_id, &scope.scope_id);
                    return Err(RuntimeFailure::Cancelled {
                        request_id: context.request_id(),
                    });
                }
                return match outcome {
                    StreamOpenOutcome::Opened { stream_id } => {
                        self.settlements
                            .lock()
                            .expect("Bun settlements")
                            .remove(&correlation_id);
                        Ok(Ok(Rc::new(BunStreamSessionV2 {
                            host: self
                                .host
                                .borrow()
                                .clone()
                                .ok_or(RuntimeFailure::AdmissionClosed)?,
                            generation: self.clone(),
                            identity: self.identity.clone(),
                            stream_id,
                            scope_id: scope.scope_id,
                            retired: AtomicBool::new(false),
                            next_send_sequence: AtomicU64::new(0),
                        })
                            as Rc<dyn JsonStreamSessionTransport>))
                    }
                    StreamOpenOutcome::Domain { error } => {
                        self.retire_invocation(&correlation_id, &scope.scope_id);
                        Ok(Err(error))
                    }
                    StreamOpenOutcome::Runtime { failure } => {
                        self.retire_invocation(&correlation_id, &scope.scope_id);
                        Err(from_wire_failure(failure))
                    }
                };
            }
        })
    }
}

#[derive(Debug)]
struct BunStreamSessionV2 {
    host: Arc<BunAuthoringHost>,
    generation: Rc<BunGenerationV2>,
    identity: SessionIdentity,
    stream_id: String,
    scope_id: String,
    retired: AtomicBool,
    next_send_sequence: AtomicU64,
}

impl BunStreamSessionV2 {
    fn action() -> String {
        NEXT_BUN_STREAM_ACTION
            .fetch_add(1, Ordering::Relaxed)
            .to_string()
    }

    fn receive_params(&self) -> StreamReceiveParams {
        StreamReceiveParams {
            session: self.identity.session.clone(),
            correlation_id: Self::action(),
            stream_id: self.stream_id.clone(),
        }
    }

    fn retire(&self) {
        if !self.retired.swap(true, Ordering::AcqRel) {
            self.generation.retire_lifecycle(&self.scope_id);
        }
    }
}

impl JsonStreamSessionTransport for BunStreamSessionV2 {
    fn send(
        self: Rc<Self>,
        message_json: String,
    ) -> futures::future::LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        Box::pin(async move {
            let params = StreamSendParams {
                session: self.identity.session.clone(),
                correlation_id: Self::action(),
                stream_id: self.stream_id.clone(),
                sequence: self.next_send_sequence.load(Ordering::Acquire).to_string(),
                message: serde_json::from_str(&message_json)
                    .map_err(|_| protocol("invalid Bun Stream message"))?,
            };
            let host = self.host.clone();
            let result = spawn_rpc("stream-send", move || host.send_stream(&params))?
                .await
                .map_err(|_| unavailable())??;
            match result.outcome {
                StreamActionOutcome::Accepted => {
                    self.next_send_sequence.fetch_add(1, Ordering::Release);
                    Ok(())
                }
                StreamActionOutcome::Runtime { failure } => Err(from_wire_failure(failure)),
            }
        })
    }

    fn receive(
        self: Rc<Self>,
    ) -> futures::future::LocalBoxFuture<'static, Result<JsonStreamItem, RuntimeFailure>> {
        Box::pin(async move {
            let params = self.receive_params();
            let host = self.host.clone();
            let result = spawn_rpc("stream-receive", move || host.receive_stream(&params))?
                .await
                .map_err(|_| unavailable())??;
            match result.outcome {
                StreamReceiveOutcome::Message { message, .. } => {
                    Ok(JsonStreamItem::Message(message))
                }
                StreamReceiveOutcome::PeerHalfClosed => Ok(JsonStreamItem::PeerHalfClosed),
                StreamReceiveOutcome::Terminal {
                    outcome: StreamTerminalOutcome::Success,
                } => {
                    self.retire();
                    Ok(JsonStreamItem::Terminal(Ok(())))
                }
                StreamReceiveOutcome::Terminal {
                    outcome: StreamTerminalOutcome::Domain { error },
                } => {
                    self.retire();
                    Ok(JsonStreamItem::Terminal(Err(error)))
                }
                StreamReceiveOutcome::Runtime { failure } => Err(from_wire_failure(failure)),
            }
        })
    }

    fn close_send(
        self: Rc<Self>,
    ) -> futures::future::LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        Box::pin(async move {
            let params: StreamCloseSendParams = self.receive_params();
            let host = self.host.clone();
            let result = spawn_rpc("stream-close-send", move || host.close_stream_send(&params))?
                .await
                .map_err(|_| unavailable())??;
            match result.outcome {
                StreamActionOutcome::Accepted => Ok(()),
                StreamActionOutcome::Runtime { failure } => Err(from_wire_failure(failure)),
            }
        })
    }

    fn cancel(&self) {
        self.retire();
        let params: StreamCancelParams = self.receive_params();
        let host = self.host.clone();
        let _ = thread::Builder::new().name(format!("lenso-bun-v2-stream-cancel-{}", self.stream_id)).spawn(move || {
            if !matches!(host.cancel_stream(&params), Ok(result) if result.outcome == StreamActionOutcome::Accepted) {
                host.terminate();
            }
        });
    }
}

impl BunGenerationV2 {
    fn retire_invocation(&self, correlation_id: &str, scope_id: &str) {
        self.settlements
            .lock()
            .expect("Bun settlements")
            .remove(correlation_id);
        self.shared_scopes
            .lock()
            .expect("Bun scopes")
            .remove(scope_id);
        self.contexts.borrow_mut().remove(scope_id);
    }

    fn retire_lifecycle(&self, scope_id: &str) {
        self.shared_scopes
            .lock()
            .expect("Bun scopes")
            .remove(scope_id);
        self.contexts.borrow_mut().remove(scope_id);
    }
}

#[derive(Debug)]
struct BunLifecycleV2 {
    generation: Rc<BunGenerationV2>,
}

impl PluginLifecycle for BunLifecycleV2 {
    fn prepare(&self, context: lenso_kernel::PrepareContext) -> lenso_kernel::PluginFuture {
        let resource = BunV2Resource {
            generation: self.generation.clone(),
        };
        Box::pin(async move {
            context
                .resources()
                .register(resource)
                .map_err(|error| RuntimeFailure::Internal {
                    detail: format!("failed to register Bun Authoring V2 process: {error:?}"),
                })?;
            Ok(())
        })
    }

    fn construct(&self, context: ActivateContext) -> lenso_kernel::PluginFuture {
        let generation = self.generation.clone();
        Box::pin(async move {
            let initialization = generation.initialize(context.dependencies())?;
            let (callback_sender, mut callback_receiver) =
                mpsc::channel(generation.config.request_queue_capacity);
            let callback_sender = Arc::new(Mutex::new(callback_sender));
            let callback = HostCallback {
                sender: callback_sender.clone(),
                initialization: initialization.clone(),
                scopes: generation.shared_scopes.clone(),
                settlements: generation.settlements.clone(),
            };
            let host = Arc::new(BunAuthoringHost::start(
                &generation.config.bun_binary,
                generation.artifact.path(),
                initialization.clone(),
                callback,
            )?);
            let exit = host.exit_waiter();
            generation.initialization.replace(Some(initialization));
            generation.host.replace(Some(host.clone()));
            generation.callback_sender.replace(Some(callback_sender));
            let params = ConstructParams {
                session: generation.identity.session.clone(),
                lifecycle_scope_id: "construct-1".to_owned(),
                remaining_budget_nanos: u64::MAX.to_string(),
            };
            let scope =
                lifecycle_scope(&params.lifecycle_scope_id, &params.remaining_budget_nanos)?;
            let invocation_context = context
                .dependencies()
                .invocation_context(None, context.cancellation())?;
            generation
                .contexts
                .borrow_mut()
                .insert(scope.scope_id.clone(), invocation_context);
            generation
                .shared_scopes
                .lock()
                .expect("Bun scopes")
                .insert(scope.scope_id.clone(), scope.clone());
            let request = params.clone();
            let rpc = spawn_rpc("construct", move || host.construct(request))?;
            let result = await_lifecycle_rpc(
                rpc,
                &mut callback_receiver,
                context.cancellation(),
                &generation,
            )
            .await;
            generation.retire_lifecycle(&scope.scope_id);
            let imports = generation.imports.clone();
            let contexts = generation.contexts.clone();
            let outbound_receive_sequences = generation.outbound_receive_sequences.clone();
            let initialization_for_bridge = generation
                .initialization
                .borrow()
                .clone()
                .expect("Bun initialization was installed");
            let callback_cancellation = context.tasks().cancellation();
            context
                .tasks()
                .spawn_local(Box::pin(dispatch_callbacks(
                    callback_receiver,
                    imports,
                    contexts,
                    outbound_receive_sequences,
                    initialization_for_bridge,
                    callback_cancellation,
                    exit,
                )))
                .map_err(|error| RuntimeFailure::Internal {
                    detail: format!("failed to start Bun Authoring callback bridge: {error:?}"),
                })?;
            let result = result?;
            result
                .validate_for(&params)
                .map_err(|error| protocol(error.to_string()))?;
            match result.outcome {
                FactoryOutcome::Constructed => Ok(()),
                FactoryOutcome::Failed { detail } => Err(RuntimeFailure::PluginFailure { detail }),
            }
        })
    }

    fn deactivate(&self, context: DeactivateContext) -> lenso_kernel::PluginFuture {
        let generation = self.generation.clone();
        Box::pin(async move {
            if generation.stop_started.swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            let Some(host) = generation.host.borrow().clone() else {
                generation.terminate();
                return Ok(());
            };
            let params = StopParams {
                session: generation.identity.session.clone(),
                cleanup_scope_id: "cleanup-1".to_owned(),
                remaining_budget_nanos: duration_nanos(context.remaining_budget()),
            };
            let scope = lifecycle_scope(&params.cleanup_scope_id, &params.remaining_budget_nanos)?;
            let invocation_context = context.dependency_invocation_context()?;
            generation
                .contexts
                .borrow_mut()
                .insert(scope.scope_id.clone(), invocation_context);
            generation
                .shared_scopes
                .lock()
                .expect("Bun scopes")
                .insert(scope.scope_id.clone(), scope.clone());
            let (callback_sender, mut callback_receiver) =
                mpsc::channel(generation.config.request_queue_capacity);
            let sender = generation
                .callback_sender
                .borrow()
                .clone()
                .ok_or(RuntimeFailure::AdmissionClosed)?;
            *sender.lock().expect("Bun callback sender") = callback_sender;
            let request = params.clone();
            let rpc = spawn_rpc("stop", move || host.stop(request))?;
            let result = await_lifecycle_rpc(
                rpc,
                &mut callback_receiver,
                context.cancellation(),
                &generation,
            )
            .await;
            generation.retire_lifecycle(&scope.scope_id);
            let result = result?;
            result
                .validate_for(&params)
                .map_err(|error| protocol(error.to_string()))?;
            generation.terminate();
            if result.hook == StopHookOutcome::Failed {
                return Err(RuntimeFailure::PluginFailure {
                    detail: result.diagnostics.first().map_or_else(
                        || "Bun Authoring V2 stop failed".to_owned(),
                        |diagnostic| diagnostic.detail.clone(),
                    ),
                });
            }
            Ok(())
        })
    }
}

#[derive(Debug)]
struct BunV2Resource {
    generation: Rc<BunGenerationV2>,
}

impl ManagedResource for BunV2Resource {
    fn release(&self) -> lenso_kernel::ResourceFuture {
        self.generation.terminate();
        Box::pin(futures::future::ready(Ok(())))
    }
}

enum BridgeRequest {
    Call(
        OutboundCallParams,
        std_mpsc::SyncSender<Result<OutboundCallResult, RuntimeFailure>>,
    ),
    Event(
        OutboundEventPublishParams,
        std_mpsc::SyncSender<Result<OutboundEventPublishResult, RuntimeFailure>>,
    ),
    StreamOpen(
        OutboundStreamOpenParams,
        std_mpsc::SyncSender<Result<OutboundStreamOpenResult, RuntimeFailure>>,
    ),
    StreamSend(
        StreamSendParams,
        std_mpsc::SyncSender<Result<StreamActionResult, RuntimeFailure>>,
    ),
    StreamReceive(
        StreamReceiveParams,
        std_mpsc::SyncSender<Result<StreamReceiveResult, RuntimeFailure>>,
    ),
    StreamClose(
        StreamCloseSendParams,
        std_mpsc::SyncSender<Result<StreamActionResult, RuntimeFailure>>,
    ),
    StreamCancel(
        StreamCancelParams,
        std_mpsc::SyncSender<Result<StreamActionResult, RuntimeFailure>>,
    ),
}

impl std::fmt::Debug for BridgeRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BridgeRequest(..)")
    }
}

#[derive(Debug)]
struct HostCallback {
    sender: CallbackSender,
    initialization: InitializeParams,
    scopes: SharedScopes,
    settlements: SettlementSenders,
}

impl BunAuthoringCallback for HostCallback {
    fn call(&self, params: OutboundCallParams) -> Result<OutboundCallResult, RuntimeFailure> {
        let parent_scope_id = params
            .scope
            .parent_scope_id
            .as_deref()
            .ok_or_else(|| protocol("Bun outbound call has no parent scope"))?;
        let parent = self
            .scopes
            .lock()
            .expect("Bun scopes")
            .get(parent_scope_id)
            .cloned()
            .ok_or_else(|| protocol("Bun outbound call parent is not active"))?;
        params
            .validate_against(&self.initialization, &parent, true)
            .map_err(|error| protocol(error.to_string()))?;
        let (response, result) = std_mpsc::sync_channel(1);
        self.sender
            .lock()
            .expect("Bun callback sender")
            .try_send(BridgeRequest::Call(params, response))
            .map_err(|_| RuntimeFailure::ResourceExhausted {
                capability: EXECUTION_CLASS,
                operation: "lenso.call".to_owned(),
            })?;
        result.recv().map_err(|_| unavailable())?
    }

    fn publish_event(
        &self,
        params: OutboundEventPublishParams,
    ) -> Result<OutboundEventPublishResult, RuntimeFailure> {
        self.validate_parent(&params.scope, |parent| {
            params.validate_against(&self.initialization, parent, true)
        })?;
        send_bridge(
            &self.sender,
            "lenso.event.publish",
            BridgeRequest::Event,
            params,
        )
    }

    fn open_stream(
        &self,
        params: OutboundStreamOpenParams,
    ) -> Result<OutboundStreamOpenResult, RuntimeFailure> {
        self.validate_parent(&params.scope, |parent| {
            params.validate_against(&self.initialization, parent, true)
        })?;
        send_bridge(
            &self.sender,
            "lenso.stream.open",
            BridgeRequest::StreamOpen,
            params,
        )
    }

    fn send_stream(&self, params: StreamSendParams) -> Result<StreamActionResult, RuntimeFailure> {
        params
            .validate_for(&self.initialization.identity)
            .map_err(|error| protocol(error.to_string()))?;
        send_bridge(
            &self.sender,
            "lenso.stream.send",
            BridgeRequest::StreamSend,
            params,
        )
    }

    fn receive_stream(
        &self,
        params: StreamReceiveParams,
    ) -> Result<StreamReceiveResult, RuntimeFailure> {
        params
            .validate_for(&self.initialization.identity)
            .map_err(|error| protocol(error.to_string()))?;
        send_bridge(
            &self.sender,
            "lenso.stream.receive",
            BridgeRequest::StreamReceive,
            params,
        )
    }

    fn close_stream_send(
        &self,
        params: StreamCloseSendParams,
    ) -> Result<StreamActionResult, RuntimeFailure> {
        params
            .validate_for(&self.initialization.identity)
            .map_err(|error| protocol(error.to_string()))?;
        send_bridge(
            &self.sender,
            "lenso.stream.close_send",
            BridgeRequest::StreamClose,
            params,
        )
    }

    fn cancel_stream(
        &self,
        params: StreamCancelParams,
    ) -> Result<StreamActionResult, RuntimeFailure> {
        params
            .validate_for(&self.initialization.identity)
            .map_err(|error| protocol(error.to_string()))?;
        send_bridge(
            &self.sender,
            "lenso.stream.cancel",
            BridgeRequest::StreamCancel,
            params,
        )
    }

    fn settled(&self, settlement: Settlement) -> Result<(), RuntimeFailure> {
        settlement
            .validate_for(&self.initialization.identity)
            .map_err(|error| protocol(error.to_string()))?;
        self.settlements
            .lock()
            .expect("Bun settlements")
            .remove(&settlement.correlation_id)
            .ok_or_else(|| protocol("Bun settled an unknown invocation"))?
            .send(settlement)
            .map_err(|_| unavailable())
    }
}

impl HostCallback {
    fn validate_parent(
        &self,
        scope: &InvocationScope,
        validate: impl FnOnce(&InvocationScope) -> Result<(), lenso_process_protocol::ProtocolError>,
    ) -> Result<(), RuntimeFailure> {
        let parent_scope_id = scope
            .parent_scope_id
            .as_deref()
            .ok_or_else(|| protocol("Bun outbound interaction has no parent scope"))?;
        let parent = self
            .scopes
            .lock()
            .expect("Bun scopes")
            .get(parent_scope_id)
            .cloned()
            .ok_or_else(|| protocol("Bun outbound interaction parent is not active"))?;
        validate(&parent).map_err(|error| protocol(error.to_string()))
    }
}

fn send_bridge<P, R>(
    sender: &CallbackSender,
    operation: &str,
    build: impl FnOnce(P, std_mpsc::SyncSender<Result<R, RuntimeFailure>>) -> BridgeRequest,
    params: P,
) -> Result<R, RuntimeFailure> {
    let (response, result) = std_mpsc::sync_channel(1);
    sender
        .lock()
        .expect("Bun callback sender")
        .try_send(build(params, response))
        .map_err(|_| RuntimeFailure::ResourceExhausted {
            capability: EXECUTION_CLASS,
            operation: operation.to_owned(),
        })?;
    result.recv().map_err(|_| unavailable())?
}

async fn dispatch_callbacks(
    mut receiver: mpsc::Receiver<BridgeRequest>,
    imports: Rc<JsonHostImports>,
    contexts: Rc<RefCell<BTreeMap<String, InvocationContext>>>,
    outbound_receive_sequences: Rc<RefCell<BTreeMap<u64, u64>>>,
    initialization: InitializeParams,
    cancellation: lenso_kernel::CancellationToken,
    exit: oneshot::Receiver<()>,
) {
    let mut exit = exit.fuse();
    loop {
        let request = receiver.next().fuse();
        let cancelled = cancellation.cancelled().fuse();
        futures::pin_mut!(request, cancelled);
        select! {
            request = request => {
                let Some(request) = request else { return };
                dispatch_bridge(
                    &imports,
                    &contexts,
                    &outbound_receive_sequences,
                    &initialization,
                    request,
                ).await;
            },
            () = cancelled => {
                return;
            },
            result = exit => {
                assert!(
                    result.is_err(),
                    "Bun Authoring V2 child exited before deactivation"
                );
                return;
            },
        }
    }
}

async fn dispatch_callback(
    imports: &JsonHostImports,
    contexts: &RefCell<BTreeMap<String, InvocationContext>>,
    initialization: &InitializeParams,
    call: &OutboundCallParams,
) -> Result<OutboundCallResult, RuntimeFailure> {
    let parent_scope = call
        .scope
        .parent_scope_id
        .as_deref()
        .ok_or_else(|| protocol("Bun outbound call has no parent scope"))?;
    let context = contexts
        .borrow()
        .get(parent_scope)
        .cloned()
        .ok_or_else(|| protocol("Bun outbound call parent context is not active"))?;
    let binding_id = route_binding_id(initialization, &call.requirement_id, &call.route_id)?;
    let result = imports
        .invoke(
            binding_id,
            call.operation.clone(),
            call.payload.clone(),
            context,
        )
        .await;
    Ok(InvocationResult {
        session: call.session.clone(),
        correlation_id: call.correlation_id.clone(),
        outcome: wire_outcome(result),
    })
}

#[allow(clippy::too_many_lines)]
async fn dispatch_bridge(
    imports: &Rc<JsonHostImports>,
    contexts: &Rc<RefCell<BTreeMap<String, InvocationContext>>>,
    outbound_receive_sequences: &Rc<RefCell<BTreeMap<u64, u64>>>,
    initialization: &InitializeParams,
    request: BridgeRequest,
) {
    match request {
        BridgeRequest::Call(params, response) => {
            let _ =
                response.send(dispatch_callback(imports, contexts, initialization, &params).await);
        }
        BridgeRequest::Event(params, response) => {
            let result = async {
                let context = parent_context(contexts, &params.scope)?;
                let binding =
                    route_binding_id(initialization, &params.requirement_id, &params.route_id)?;
                let outcome = match imports
                    .publish_event(
                        binding,
                        params.operation.clone(),
                        params.event.clone(),
                        context,
                    )
                    .await
                {
                    Ok(()) => EventPublishOutcome::Accepted,
                    Err(failure) => EventPublishOutcome::Runtime {
                        failure: wire_failure(failure),
                    },
                };
                Ok(EventPublishResult {
                    session: params.session,
                    correlation_id: params.correlation_id,
                    outcome,
                })
            }
            .await;
            let _ = response.send(result);
        }
        BridgeRequest::StreamOpen(params, response) => {
            let result = async {
                let context = parent_context(contexts, &params.scope)?;
                let binding =
                    route_binding_id(initialization, &params.requirement_id, &params.route_id)?;
                let outcome = match Rc::clone(imports)
                    .open_stream(
                        binding,
                        params.operation.clone(),
                        params.request.clone(),
                        context,
                    )
                    .await
                {
                    Ok(Ok(stream_id)) => {
                        outbound_receive_sequences.borrow_mut().insert(stream_id, 0);
                        StreamOpenOutcome::Opened {
                            stream_id: stream_id.to_string(),
                        }
                    }
                    Ok(Err(error)) => StreamOpenOutcome::Domain { error },
                    Err(failure) => StreamOpenOutcome::Runtime {
                        failure: wire_failure(failure),
                    },
                };
                Ok(StreamOpenResult {
                    session: params.session,
                    correlation_id: params.correlation_id,
                    outcome,
                })
            }
            .await;
            let _ = response.send(result);
        }
        BridgeRequest::StreamSend(params, response) => {
            let stream_id = params
                .stream_id
                .parse::<u64>()
                .map_err(|_| protocol("invalid outbound Stream id"));
            let result = match stream_id {
                Ok(stream_id) => imports.send_stream(stream_id, params.message.clone()).await,
                Err(error) => Err(error),
            };
            let outcome = result.map_or_else(
                |failure| StreamActionOutcome::Runtime {
                    failure: wire_failure(failure),
                },
                |()| StreamActionOutcome::Accepted,
            );
            let _ = response.send(Ok(StreamActionResult {
                session: params.session,
                correlation_id: params.correlation_id,
                stream_id: params.stream_id,
                outcome,
            }));
        }
        BridgeRequest::StreamReceive(params, response) => {
            let result = async {
                let stream_id = params
                    .stream_id
                    .parse::<u64>()
                    .map_err(|_| protocol("invalid outbound Stream id"))?;
                let outcome = match Rc::clone(imports).receive_stream(stream_id).await {
                    Ok(JsonStreamItem::Message(message)) => {
                        let sequence = {
                            let mut sequences = outbound_receive_sequences.borrow_mut();
                            let sequence = sequences.entry(stream_id).or_default();
                            let current = *sequence;
                            *sequence = sequence.saturating_add(1);
                            current
                        };
                        StreamReceiveOutcome::Message {
                            sequence: sequence.to_string(),
                            message,
                        }
                    }
                    Ok(JsonStreamItem::PeerHalfClosed) => StreamReceiveOutcome::PeerHalfClosed,
                    Ok(JsonStreamItem::Terminal(Ok(()))) => {
                        outbound_receive_sequences.borrow_mut().remove(&stream_id);
                        StreamReceiveOutcome::Terminal {
                            outcome: StreamTerminalOutcome::Success,
                        }
                    }
                    Ok(JsonStreamItem::Terminal(Err(error))) => {
                        outbound_receive_sequences.borrow_mut().remove(&stream_id);
                        StreamReceiveOutcome::Terminal {
                            outcome: StreamTerminalOutcome::Domain { error },
                        }
                    }
                    Err(failure) => {
                        outbound_receive_sequences.borrow_mut().remove(&stream_id);
                        StreamReceiveOutcome::Runtime {
                            failure: wire_failure(failure),
                        }
                    }
                };
                Ok(StreamReceiveResult {
                    session: params.session,
                    correlation_id: params.correlation_id,
                    stream_id: params.stream_id,
                    outcome,
                })
            }
            .await;
            let _ = response.send(result);
        }
        BridgeRequest::StreamClose(params, response) => {
            let stream_id = params
                .stream_id
                .parse::<u64>()
                .map_err(|_| protocol("invalid outbound Stream id"));
            let result = match stream_id {
                Ok(id) => imports.close_stream_send(id).await,
                Err(error) => Err(error),
            };
            let outcome = result.map_or_else(
                |failure| StreamActionOutcome::Runtime {
                    failure: wire_failure(failure),
                },
                |()| StreamActionOutcome::Accepted,
            );
            let _ = response.send(Ok(StreamActionResult {
                session: params.session,
                correlation_id: params.correlation_id,
                stream_id: params.stream_id,
                outcome,
            }));
        }
        BridgeRequest::StreamCancel(params, response) => {
            let parsed_stream_id = params
                .stream_id
                .parse::<u64>()
                .map_err(|_| protocol("invalid outbound Stream id"));
            if let Ok(stream_id) = &parsed_stream_id {
                outbound_receive_sequences.borrow_mut().remove(stream_id);
            }
            let result = parsed_stream_id.and_then(|id| imports.cancel_stream(id));
            let outcome = result.map_or_else(
                |failure| StreamActionOutcome::Runtime {
                    failure: wire_failure(failure),
                },
                |()| StreamActionOutcome::Accepted,
            );
            let _ = response.send(Ok(StreamActionResult {
                session: params.session,
                correlation_id: params.correlation_id,
                stream_id: params.stream_id,
                outcome,
            }));
        }
    }
}

fn parent_context(
    contexts: &Rc<RefCell<BTreeMap<String, InvocationContext>>>,
    scope: &InvocationScope,
) -> Result<InvocationContext, RuntimeFailure> {
    let parent = scope
        .parent_scope_id
        .as_deref()
        .ok_or_else(|| protocol("Bun outbound interaction has no parent scope"))?;
    contexts
        .borrow()
        .get(parent)
        .cloned()
        .ok_or_else(|| protocol("Bun outbound interaction parent context is not active"))
}

async fn await_lifecycle_rpc<T>(
    receiver: oneshot::Receiver<Result<T, RuntimeFailure>>,
    callbacks: &mut mpsc::Receiver<BridgeRequest>,
    cancellation: lenso_kernel::CancellationToken,
    generation: &BunGenerationV2,
) -> Result<T, RuntimeFailure> {
    let mut response = receiver.fuse();
    let mut cancelled = cancellation.cancelled().fuse();
    loop {
        let callback = callbacks.next().fuse();
        futures::pin_mut!(callback);
        select! {
            response = response => return response.map_err(|_| unavailable())?,
            callback = callback => {
                let Some(callback) = callback else {
                    generation.terminate();
                    return Err(unavailable());
                };
                let initialization = generation
                    .initialization
                    .borrow()
                    .clone()
                    .ok_or(RuntimeFailure::AdmissionClosed)?;
                dispatch_bridge(
                    &generation.imports,
                    &generation.contexts,
                    &generation.outbound_receive_sequences,
                    &initialization,
                    callback,
                ).await;
            },
            () = cancelled => {
                generation.terminate();
                return Err(RuntimeFailure::AdmissionClosed);
            }
        }
    }
}

fn spawn_rpc<T, F>(
    operation: &'static str,
    call: F,
) -> Result<oneshot::Receiver<Result<T, RuntimeFailure>>, RuntimeFailure>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, RuntimeFailure> + Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    thread::Builder::new()
        .name(format!("lenso-bun-v2-{operation}"))
        .spawn(move || {
            let _ = sender.send(call());
        })
        .map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to start Bun Authoring V2 {operation}: {error}"),
        })?;
    Ok(receiver)
}

fn arm_termination(
    host: Arc<BunAuthoringHost>,
    settlements: SettlementSenders,
    correlation_id: String,
    timeout: std::time::Duration,
) {
    let _ = thread::Builder::new()
        .name(format!("lenso-bun-v2-deadline-{correlation_id}"))
        .spawn(move || {
            thread::sleep(timeout);
            if settlements
                .lock()
                .expect("Bun settlements")
                .contains_key(&correlation_id)
            {
                host.terminate();
            }
        });
}

fn invocation_scope(context: &InvocationContext) -> Result<InvocationScope, RuntimeFailure> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let mut extensions = context
        .extensions()
        .map(|extension| lenso_process_protocol::InvocationExtension {
            key: extension.key().to_owned(),
            value: STANDARD.encode(extension.value()),
            issuer: None,
            audience: Vec::new(),
            proof: None,
            sealed: false,
        })
        .chain(context.sealed_extensions().map(|extension| {
            lenso_process_protocol::InvocationExtension {
                key: extension.key().to_owned(),
                value: STANDARD.encode(extension.value()),
                issuer: Some(extension.issuer().to_owned()),
                audience: extension.audience().to_vec(),
                proof: Some(extension.proof().to_owned()),
                sealed: true,
            }
        }))
        .collect::<Vec<_>>();
    extensions.sort_by(|left, right| left.key.cmp(&right.key));
    let scope = InvocationScope {
        scope_id: format!("invoke-{}", context.request_id()),
        parent_scope_id: None,
        remaining_budget_nanos: duration_nanos(context.remaining_budget()),
        permissions: Vec::new(),
        extensions,
    };
    scope
        .validate()
        .map_err(|error| protocol(error.to_string()))?;
    Ok(scope)
}

fn lifecycle_scope(
    scope_id: &str,
    remaining_budget_nanos: &str,
) -> Result<InvocationScope, RuntimeFailure> {
    let scope = InvocationScope {
        scope_id: scope_id.to_owned(),
        parent_scope_id: None,
        remaining_budget_nanos: remaining_budget_nanos.to_owned(),
        permissions: Vec::new(),
        extensions: Vec::new(),
    };
    scope
        .validate()
        .map_err(|error| protocol(error.to_string()))?;
    Ok(scope)
}

fn duration_nanos(duration: Option<std::time::Duration>) -> String {
    duration.map_or_else(
        || u64::MAX.to_string(),
        |value| value.as_nanos().min(u128::from(u64::MAX)).to_string(),
    )
}

fn random_session() -> Result<String, RuntimeFailure> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|error| RuntimeFailure::Internal {
        detail: format!("failed to create Bun Authoring V2 session: {error}"),
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn route_binding_id(
    initialization: &InitializeParams,
    requirement_id: &str,
    route_id: &str,
) -> Result<u32, RuntimeFailure> {
    initialization
        .routes
        .iter()
        .find(|route| route.route_id == route_id && route.requirement_id == requirement_id)
        .ok_or_else(|| protocol("Bun outbound call references an unknown route"))?
        .route_id
        .strip_prefix("route-")
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| protocol("Bun outbound route identity is invalid"))
}

fn cardinality(value: CapabilityCardinality) -> RequirementCardinality {
    match value {
        CapabilityCardinality::One => RequirementCardinality::One,
        CapabilityCardinality::Optional => RequirementCardinality::Optional,
        CapabilityCardinality::Many => RequirementCardinality::Many,
    }
}

fn contract_digest(
    instance: &PluginInstancePlan,
    provided: &[Rc<dyn JsonCapabilityCodec>],
    required: &[Rc<dyn JsonCapabilityCodec>],
) -> String {
    let provided_codecs = provided
        .iter()
        .map(|codec| (codec.capability_id(), codec.as_ref()))
        .collect::<BTreeMap<_, _>>();
    let required_codecs = required
        .iter()
        .map(|codec| (codec.capability_id(), codec.as_ref()))
        .collect::<BTreeMap<_, _>>();
    let mut identities = instance
        .provided_capabilities()
        .iter()
        .map(|endpoint| {
            let codec = provided_codecs[endpoint.capability_id()];
            let mut operations = endpoint.request_operations();
            operations.sort_unstable();
            format!(
                "provide:{}:{}:{}:{}",
                endpoint.capability_id(),
                endpoint.descriptor_version(),
                codec.descriptor_digest(),
                operations.join(",")
            )
        })
        .chain(instance.required_capabilities().iter().map(|requirement| {
            let codec = required_codecs[requirement.capability_id()];
            format!(
                "require:{}:{}:{}:{}:{:?}",
                requirement.requirement_id(),
                requirement.capability_id(),
                requirement.descriptor_version(),
                codec.descriptor_digest(),
                requirement.cardinality()
            )
        }))
        .collect::<Vec<_>>();
    identities.sort();
    format!(
        "sha256:{}",
        hex::encode(Sha256::digest(identities.join("\n").as_bytes()))
    )
}

fn validate_digest(value: &str, capability: &'static str) -> Result<(), RuntimeFailure> {
    let valid = value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    });
    if valid {
        Ok(())
    } else {
        Err(RuntimeFailure::ProtocolViolation { capability })
    }
}

fn wire_outcome(result: Result<JsonInvocationOutcome, RuntimeFailure>) -> InvocationOutcome {
    match result {
        Ok(JsonInvocationOutcome::Success(value)) => InvocationOutcome::Success { value },
        Ok(JsonInvocationOutcome::DomainError(error)) => InvocationOutcome::Domain { error },
        Err(failure) => InvocationOutcome::Runtime {
            failure: wire_failure(failure),
        },
    }
}

fn from_wire_outcome(value: InvocationOutcome) -> Result<JsonInvocationOutcome, RuntimeFailure> {
    match value {
        InvocationOutcome::Success { value } => Ok(JsonInvocationOutcome::Success(value)),
        InvocationOutcome::Domain { error } => Ok(JsonInvocationOutcome::DomainError(error)),
        InvocationOutcome::Runtime { failure } => Err(from_wire_failure(failure)),
    }
}

fn wire_failure(value: RuntimeFailure) -> WireFailure {
    match value {
        RuntimeFailure::Unavailable { capability } => WireFailure::Unavailable {
            capability: capability.to_owned(),
        },
        RuntimeFailure::UnknownOperation {
            capability,
            operation,
        } => WireFailure::UnknownOperation {
            capability: capability.to_owned(),
            operation,
        },
        RuntimeFailure::AmbiguousBinding {
            capability,
            providers,
        } => WireFailure::AmbiguousBinding {
            capability: capability.to_owned(),
            providers: u32::try_from(providers).unwrap_or(u32::MAX),
        },
        RuntimeFailure::ProtocolViolation { capability } => WireFailure::ProtocolViolation {
            capability: capability.to_owned(),
        },
        RuntimeFailure::AdmissionClosed => WireFailure::AdmissionClosed,
        RuntimeFailure::ResourceExhausted {
            capability,
            operation,
        } => WireFailure::ResourceExhausted {
            capability: capability.to_owned(),
            operation,
        },
        RuntimeFailure::DeadlineExceeded { request_id } => WireFailure::DeadlineExceeded {
            request_id: request_id.to_string(),
        },
        RuntimeFailure::Cancelled { request_id } => WireFailure::Cancelled {
            request_id: request_id.to_string(),
        },
        other => WireFailure::Internal {
            detail: format!("{other:?}"),
        },
    }
}

fn from_wire_failure(value: WireFailure) -> RuntimeFailure {
    match value {
        WireFailure::UnknownOperation { operation, .. } => RuntimeFailure::UnknownOperation {
            capability: EXECUTION_CLASS,
            operation,
        },
        WireFailure::AmbiguousBinding { providers, .. } => RuntimeFailure::AmbiguousBinding {
            capability: EXECUTION_CLASS,
            providers: providers as usize,
        },
        WireFailure::ProtocolViolation { .. } => protocol("Bun reported a protocol violation"),
        WireFailure::AdmissionClosed => RuntimeFailure::AdmissionClosed,
        WireFailure::ResourceExhausted { operation, .. } => RuntimeFailure::ResourceExhausted {
            capability: EXECUTION_CLASS,
            operation,
        },
        WireFailure::DeadlineExceeded { request_id } => RuntimeFailure::DeadlineExceeded {
            request_id: request_id.parse().unwrap_or_default(),
        },
        WireFailure::Cancelled { request_id } => RuntimeFailure::Cancelled {
            request_id: request_id.parse().unwrap_or_default(),
        },
        WireFailure::PluginFailure { detail } => RuntimeFailure::PluginFailure { detail },
        other => RuntimeFailure::PluginFailure {
            detail: format!("{other:?}"),
        },
    }
}

fn protocol(detail: impl Into<String>) -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: detail.into(),
    }
}

fn unavailable() -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: "Bun Authoring V2 Plugin is unavailable".to_owned(),
    }
}

impl Drop for BunGenerationV2 {
    fn drop(&mut self) {
        self.terminate();
    }
}

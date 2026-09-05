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
        AuthoringLimits, CancelParams, ConstructParams, FactoryOutcome, InitializeParams,
        InvocationOutcome, InvocationResult, InvocationScope, OutboundCallParams,
        OutboundCallResult, ProvidedEndpoint, RequirementCardinality, RequirementDeclaration,
        RouteDescriptor, RuntimeFailure as WireFailure, SessionIdentity, Settlement,
        SettlementState, StopHookOutcome, StopParams,
    },
};
use lenso_runtime_codec::{
    ArtifactCatalog, ArtifactHandle, JsonCapabilityCodec, JsonHostImports, JsonInvocationOutcome,
    JsonRequestTransport, codecs_for_instance, codecs_for_requirements, json_request_endpoints,
};
use sha2::{Digest as _, Sha256};

use crate::{
    BUN_AUTHORING_RUNTIME_PROFILE, BunAdapterConfig, BunAuthoringCallback, BunAuthoringHost,
};

const EXECUTION_CLASS: &str = "bun-child-process";
static NEXT_BUN_SESSION: AtomicU64 = AtomicU64::new(1);

type SettlementSenders = Arc<Mutex<BTreeMap<String, oneshot::Sender<Settlement>>>>;
type SharedScopes = Arc<Mutex<BTreeMap<String, InvocationScope>>>;
type CallbackSender = Arc<Mutex<mpsc::Sender<BridgeRequest>>>;

pub(super) fn prepare_instance(
    config: &BunAdapterConfig,
    artifacts: &ArtifactCatalog,
    codecs: &BTreeMap<String, Rc<dyn JsonCapabilityCodec>>,
    instance: &PluginInstancePlan,
) -> Result<PreparedNativePlugin, RuntimeFailure> {
    if instance.provided_capabilities().iter().any(|capability| {
        !capability.stream_operations().is_empty() || !capability.event_operations().is_empty()
    }) {
        return invalid("Bun Authoring V2 currently supports Request Capability endpoints");
    }
    let provided = codecs_for_instance(instance, codecs)?;
    let required = codecs_for_requirements(instance, codecs)?;
    for codec in provided.iter().chain(&required) {
        validate_digest(codec.descriptor_digest(), codec.capability_id())?;
    }
    let imports = Rc::new(JsonHostImports::new(required.clone(), 0)?);
    let generation = BunGenerationV2::new(
        artifacts.require(instance.instance_key())?.clone(),
        instance.clone(),
        provided.clone(),
        required,
        imports,
        config.clone(),
    )?;
    let endpoints = json_request_endpoints(generation.clone(), provided);
    Ok(PreparedNativePlugin::with_endpoints(
        endpoints,
        Vec::new(),
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

struct BridgeRequest {
    call: OutboundCallParams,
    response: std_mpsc::SyncSender<Result<OutboundCallResult, RuntimeFailure>>,
}

impl std::fmt::Debug for BridgeRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BridgeRequest")
            .field("correlation_id", &self.call.correlation_id)
            .finish_non_exhaustive()
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
            .try_send(BridgeRequest {
                call: params,
                response,
            })
            .map_err(|_| RuntimeFailure::ResourceExhausted {
                capability: EXECUTION_CLASS,
                operation: "lenso.call".to_owned(),
            })?;
        result.recv().map_err(|_| unavailable())?
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

async fn dispatch_callbacks(
    mut receiver: mpsc::Receiver<BridgeRequest>,
    imports: Rc<JsonHostImports>,
    contexts: Rc<RefCell<BTreeMap<String, InvocationContext>>>,
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
                let result = dispatch_callback(&imports, &contexts, &initialization, &request.call).await;
                let _ = request.response.send(result);
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
    let binding_id = route_binding_id(initialization, call)?;
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
                let result = dispatch_callback(
                    &generation.imports,
                    &generation.contexts,
                    &initialization,
                    &callback.call,
                ).await;
                let _ = callback.response.send(result);
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
    call: &OutboundCallParams,
) -> Result<u32, RuntimeFailure> {
    initialization
        .routes
        .iter()
        .find(|route| route.route_id == call.route_id)
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

fn invalid(detail: impl Into<String>) -> Result<PreparedNativePlugin, RuntimeFailure> {
    Err(RuntimeFailure::InvalidResolvedPlan {
        detail: detail.into(),
    })
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

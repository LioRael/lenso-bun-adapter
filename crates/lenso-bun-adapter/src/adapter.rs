use std::{
    any::Any,
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    rc::Rc,
    sync::Arc,
};

use futures::{FutureExt, channel::oneshot, future::LocalBoxFuture};
use lenso_app_plan::{
    EventAdmissionPlan, ExecutionClassId, PLUGIN_AUTHORING_V2_RUNTIME_PROFILE, PluginInstancePlan,
    ResolvedAppPlan,
};
use lenso_kernel::{
    ActivateContext, DeactivateContext, ManagedResource, NativeEventEndpoint,
    NativeRequestEndpoint, NativeStreamEndpoint, NativeStreamItem, NativeStreamSession,
    PluginFuture, PluginLifecycle, PreparedBinding, PreparedEventBinding, PreparedNativeApp,
    PreparedNativePlugin, PreparedStreamBinding, RuntimeFailure,
};
use lenso_runtime_codec::ArtifactCatalog;
use serde_json::Value;

use crate::{
    host_imports::start_host_imports,
    protocol::{
        EndpointDescriptor, EventBindingDescriptor, WireOutcome, WireStreamOutcome,
        from_wire_failure, handshake_for, wire_event, wire_request, wire_stream_open,
    },
    transport::{
        ProcessState, TransportClient, TransportStreamSession, open_transport, spawn_process,
    },
};

pub use crate::transport::BunWire;

/// Codec bridge generated Capability packages implement at their Adapter edge.
pub trait BunCapabilityCodec: std::fmt::Debug + 'static {
    /// Stable portable Capability series identity.
    fn capability_id(&self) -> &'static str;
    /// Exact generated Descriptor version.
    fn descriptor_version(&self) -> &'static str;
    /// Exact generated Operation table.
    fn operations(&self) -> &'static [&'static str];
    /// Exact stream Operation table; request-only codecs default to no streams.
    fn stream_operations(&self) -> &'static [&'static str] {
        &[]
    }
    /// Exact ephemeral Event Operation table; request-only codecs default to no Events.
    fn event_operations(&self) -> &'static [&'static str] {
        &[]
    }
    /// Exact request Operation table; mixed descriptors can override the default.
    fn request_operations(&self) -> &'static [&'static str] {
        self.operations()
    }
    /// Converts one generated Event value to a validated portable JSON value.
    fn encode_event(&self, operation: &str, event: &dyn Any) -> Result<Value, RuntimeFailure> {
        self.encode_request(operation, event)
    }
    /// Converts one generated request value to a validated portable JSON value.
    fn encode_request(&self, operation: &str, request: &dyn Any) -> Result<Value, RuntimeFailure>;
    /// Converts a successful portable JSON value to the generated response value.
    fn decode_response(
        &self,
        operation: &str,
        value: Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure>;
    /// Converts a Domain Error value to the generated error value.
    fn decode_domain_error(
        &self,
        operation: &str,
        value: Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure>;
    /// Decodes one portable dependency request for its native provider endpoint.
    fn decode_request(
        &self,
        _operation: &str,
        value: Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        Ok(Box::new(value))
    }
    /// Encodes one native dependency success value for the Bun consumer.
    fn encode_response(&self, _operation: &str, value: &dyn Any) -> Result<Value, RuntimeFailure> {
        value
            .downcast_ref::<Value>()
            .cloned()
            .ok_or(RuntimeFailure::ProtocolViolation {
                capability: self.capability_id(),
            })
    }
    /// Encodes one native dependency Domain Error for the Bun consumer.
    fn encode_domain_error(
        &self,
        operation: &str,
        value: &dyn Any,
    ) -> Result<Value, RuntimeFailure> {
        self.encode_response(operation, value)
    }
    /// Converts one stream open value to a validated portable JSON value.
    fn encode_stream_open(
        &self,
        operation: &str,
        request: &dyn Any,
    ) -> Result<Value, RuntimeFailure> {
        self.encode_request(operation, request)
    }
    /// Converts one generated stream message to a validated portable JSON value.
    fn encode_stream_message(
        &self,
        _operation: &str,
        _message: &dyn Any,
    ) -> Result<Value, RuntimeFailure> {
        Err(RuntimeFailure::ProtocolViolation {
            capability: self.capability_id(),
        })
    }
    /// Converts one portable JSON stream message to its generated value.
    fn decode_stream_message(
        &self,
        operation: &str,
        value: Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        self.decode_response(operation, value)
    }
}

/// Configuration owned by one Bun Execution Adapter package.
#[derive(Clone, Debug)]
pub struct BunAdapterConfig {
    bun_binary: PathBuf,
    wire: BunWire,
    working_directory: PathBuf,
    max_frame_bytes: usize,
    request_queue_capacity: usize,
}

impl BunAdapterConfig {
    /// Creates a Bun process configuration with bounded defaults.
    pub fn new(bun_binary: impl Into<PathBuf>, wire: BunWire) -> Self {
        Self {
            bun_binary: bun_binary.into(),
            wire,
            working_directory: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            max_frame_bytes: crate::DEFAULT_MAX_FRAME_BYTES,
            request_queue_capacity: crate::protocol::DEFAULT_REQUEST_QUEUE_CAPACITY,
        }
    }

    /// Selects the directory used to resolve Plan entrypoints.
    #[must_use]
    pub fn with_working_directory(mut self, working_directory: impl Into<PathBuf>) -> Self {
        self.working_directory = working_directory.into();
        self
    }

    /// Sets the hard maximum for one encoded frame or JSON-RPC body.
    #[must_use]
    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes.max(1);
        self
    }

    /// Sets the bounded Adapter-owned request queue.
    #[must_use]
    pub fn with_request_queue_capacity(mut self, capacity: usize) -> Self {
        self.request_queue_capacity = capacity.max(1);
        self
    }

    /// Returns the selected wire implementation.
    pub const fn wire(&self) -> BunWire {
        self.wire
    }
}

/// Bun child-process Execution Adapter.
#[derive(Debug)]
pub struct BunAdapter {
    config: BunAdapterConfig,
    artifacts: Option<ArtifactCatalog>,
    codecs: BTreeMap<String, Rc<dyn BunCapabilityCodec>>,
}

impl BunAdapter {
    /// Creates an Adapter for one Bun binary and one candidate wire.
    pub fn new(bun_binary: impl Into<PathBuf>, wire: BunWire) -> Self {
        Self {
            config: BunAdapterConfig::new(bun_binary, wire),
            artifacts: None,
            codecs: BTreeMap::new(),
        }
    }

    /// Creates the selected production Adapter configuration.
    pub fn production(bun_binary: impl Into<PathBuf>) -> Self {
        Self::new(bun_binary, BunWire::JsonRpcHttp)
    }

    /// Applies adapter-level process and queue settings.
    #[must_use]
    pub fn with_config(mut self, config: BunAdapterConfig) -> Self {
        self.config = config;
        self
    }

    /// Uses exact Host-admitted Artifacts instead of resolving Plan entrypoints
    /// through one shared working directory.
    #[must_use]
    pub fn with_artifacts(mut self, artifacts: ArtifactCatalog) -> Self {
        self.artifacts = Some(artifacts);
        self
    }

    /// Registers the generated codec for one portable Capability.
    #[must_use]
    pub fn with_codec(mut self, codec: impl BunCapabilityCodec) -> Self {
        self.codecs
            .insert(codec.capability_id().to_owned(), Rc::new(codec));
        self
    }

    /// Returns the selected wire implementation.
    pub const fn wire(&self) -> BunWire {
        self.config.wire
    }

    #[allow(clippy::too_many_lines)]
    fn prepare_instance(
        &self,
        plan: &ResolvedAppPlan,
        instance: &PluginInstancePlan,
    ) -> Result<PreparedNativePlugin, RuntimeFailure> {
        if instance.entrypoint() == "default" || instance.entrypoint().is_empty() {
            return Err(RuntimeFailure::InvalidResolvedPlan {
                detail: format!(
                    "Bun Plugin Instance `{}` needs a script entrypoint",
                    instance.instance_key()
                ),
            });
        }
        let mut descriptors = Vec::with_capacity(instance.provided_capabilities().len());
        let mut codecs = Vec::with_capacity(instance.provided_capabilities().len());
        let mut capability_ids = BTreeMap::new();
        for endpoint in instance.provided_capabilities() {
            let codec = self.codecs.get(endpoint.capability_id()).ok_or_else(|| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: format!(
                        "Bun Adapter has no generated codec for Capability `{}`",
                        endpoint.capability_id()
                    ),
                }
            })?;
            let operations: Vec<_> = codec
                .operations()
                .iter()
                .map(|operation| (*operation).to_owned())
                .collect();
            let stream_operations: Vec<_> = codec
                .stream_operations()
                .iter()
                .map(|operation| (*operation).to_owned())
                .collect();
            let event_operations: Vec<_> = codec
                .event_operations()
                .iter()
                .map(|operation| (*operation).to_owned())
                .collect();
            let expected_operations: Vec<_> = endpoint.operations().to_vec();
            let expected_stream_operations: Vec<_> = endpoint
                .stream_operations()
                .iter()
                .map(|operation| (*operation).to_owned())
                .collect();
            let expected_event_operations: Vec<_> = endpoint
                .event_operations()
                .iter()
                .map(|operation| (*operation).to_owned())
                .collect();
            if codec.descriptor_version() != endpoint.descriptor_version()
                || operations != expected_operations
                || stream_operations != expected_stream_operations
                || event_operations != expected_event_operations
            {
                return Err(RuntimeFailure::ProtocolViolation {
                    capability: codec.capability_id(),
                });
            }
            descriptors.push(EndpointDescriptor {
                capability_id: endpoint.capability_id().to_owned(),
                descriptor_version: endpoint.descriptor_version().to_owned(),
                operations,
                stream_operations,
                event_operations,
            });
            capability_ids.insert(endpoint.capability_id().to_owned(), codec.capability_id());
            codecs.push(codec.clone());
        }
        let process_capability = codecs
            .first()
            .map_or("lenso.bun-process@1", |codec| codec.capability_id());
        let mut event_queues_by_capability: BTreeMap<String, BTreeMap<String, Rc<BunEventQueue>>> =
            BTreeMap::new();
        let mut event_bindings = Vec::new();
        for binding in plan
            .capability_bindings()
            .iter()
            .filter(|binding| binding.provider_instance() == instance.instance_key())
        {
            let Some(endpoint) = instance
                .provided_capabilities()
                .iter()
                .find(|endpoint| endpoint.capability_id() == binding.capability_id())
            else {
                continue;
            };
            if !endpoint.event_operations().is_empty() {
                let admission = plan.event_admission_for(binding);
                event_queues_by_capability
                    .entry(endpoint.capability_id().to_owned())
                    .or_default()
                    .insert(
                        binding.consumer_instance().to_owned(),
                        BunEventQueue::new(admission),
                    );
                event_bindings.push(EventBindingDescriptor {
                    capability_id: endpoint.capability_id().to_owned(),
                    caller_instance: binding.consumer_instance().to_owned(),
                    capacity: admission.capacity(),
                });
            }
        }
        let event_queue_capacity = event_queues_by_capability
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
            .max(1);
        let process =
            self.spawn_process(instance, &descriptors, &event_bindings, process_capability)?;
        let handshake = handshake_for(descriptors, self.config.max_frame_bytes);
        let transport = match open_transport(
            &process,
            self.config.wire,
            &handshake,
            self.config.request_queue_capacity,
            event_queue_capacity,
            Arc::new(capability_ids),
        ) {
            Ok(transport) => transport,
            Err(error) => {
                process.stop();
                return Err(error);
            }
        };
        let endpoints = instance
            .provided_capabilities()
            .iter()
            .zip(codecs.iter())
            .filter(|(_, codec)| !codec.request_operations().is_empty())
            .map(|(descriptor, codec)| {
                Rc::new(BunRequestEndpoint {
                    capability: descriptor.capability_id().to_owned(),
                    codec: codec.clone(),
                    transport: transport.clone(),
                }) as Rc<dyn NativeRequestEndpoint>
            })
            .collect();
        let stream_endpoints = instance
            .provided_capabilities()
            .iter()
            .zip(codecs.iter())
            .filter(|(_, codec)| !codec.stream_operations().is_empty())
            .map(|(descriptor, codec)| {
                Rc::new(BunStreamEndpoint {
                    capability: descriptor.capability_id().to_owned(),
                    codec: codec.clone(),
                    transport: transport.clone(),
                }) as Rc<dyn NativeStreamEndpoint>
            })
            .collect();
        let event_endpoints = instance
            .provided_capabilities()
            .iter()
            .zip(codecs.iter())
            .filter(|(_, codec)| !codec.event_operations().is_empty())
            .map(|(descriptor, codec)| {
                Rc::new(BunEventEndpoint {
                    capability: descriptor.capability_id().to_owned(),
                    codec: codec.clone(),
                    transport: transport.clone(),
                    event_queues: event_queues_by_capability
                        .get(descriptor.capability_id())
                        .cloned()
                        .unwrap_or_default(),
                }) as Rc<dyn NativeEventEndpoint>
            })
            .collect();
        Ok(PreparedNativePlugin::with_all_endpoints(
            endpoints,
            stream_endpoints,
            event_endpoints,
            BunPluginLifecycle {
                transport,
                configuration: instance.configuration().to_owned(),
                codecs: self.codecs.clone(),
                authoring_version: instance.authoring_version(),
            },
        ))
    }

    fn spawn_process(
        &self,
        instance: &PluginInstancePlan,
        endpoints: &[EndpointDescriptor],
        event_bindings: &[EventBindingDescriptor],
        capability: &'static str,
    ) -> Result<Arc<ProcessState>, RuntimeFailure> {
        let entrypoint = self.entrypoint_for(instance)?;
        if !entrypoint.is_file() {
            return Err(RuntimeFailure::InvalidResolvedPlan {
                detail: format!(
                    "Bun entrypoint `{}` for Plugin Instance `{}` does not exist",
                    entrypoint.display(),
                    instance.instance_key()
                ),
            });
        }
        let mut command = Command::new(&self.config.bun_binary);
        command
            .arg("run")
            .arg(&entrypoint)
            .arg("--")
            .arg("--lenso-transport")
            .arg(self.config.wire.argument())
            .arg("--lenso-max-frame-bytes")
            .arg(self.config.max_frame_bytes.to_string())
            .arg("--lenso-endpoints-json")
            .arg(
                serde_json::to_string(endpoints).map_err(|error| RuntimeFailure::Internal {
                    detail: format!("failed to encode Bun endpoint descriptors: {error}"),
                })?,
            )
            .arg("--lenso-event-bindings-json")
            .arg(serde_json::to_string(event_bindings).map_err(|error| {
                RuntimeFailure::Internal {
                    detail: format!("failed to encode Bun Event bindings: {error}"),
                }
            })?)
            .arg("--lenso-port")
            .arg("0")
            .current_dir(&self.config.working_directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let process = spawn_process(command, capability)?;
        Ok(process)
    }

    fn entrypoint_for(&self, instance: &PluginInstancePlan) -> Result<PathBuf, RuntimeFailure> {
        if let Some(artifacts) = &self.artifacts {
            Ok(artifacts
                .require(instance.instance_key())?
                .path()
                .to_owned())
        } else {
            let entrypoint = Path::new(instance.entrypoint());
            Ok(if entrypoint.is_absolute() {
                entrypoint.to_owned()
            } else {
                self.config.working_directory.join(entrypoint)
            })
        }
    }
}

impl lenso_kernel::ExecutionAdapter for BunAdapter {
    fn supports_runtime_profile(&self, authoring_version: u32, profile: &str) -> bool {
        (authoring_version == 1 && profile == self.execution_class().as_str())
            || (authoring_version == 2 && profile == PLUGIN_AUTHORING_V2_RUNTIME_PROFILE)
    }

    fn execution_class(&self) -> ExecutionClassId {
        ExecutionClassId::bun_child_process()
    }

    fn prepare(&self, plan: &ResolvedAppPlan) -> Result<PreparedNativeApp, RuntimeFailure> {
        validate_plan(plan)?;
        let mut generations = BTreeMap::new();
        let mut endpoints = BTreeMap::new();
        let mut stream_endpoints = BTreeMap::new();
        let mut event_endpoints = BTreeMap::new();
        for instance in plan
            .plugin_instances()
            .iter()
            .filter(|instance| instance.execution_class() == &ExecutionClassId::bun_child_process())
        {
            let generation = self.prepare_instance(plan, instance)?;
            for endpoint in generation.endpoints() {
                endpoints.insert(
                    (
                        instance.instance_key().to_owned(),
                        endpoint.capability_id().to_owned(),
                    ),
                    endpoint.clone(),
                );
            }
            for endpoint in generation.stream_endpoints() {
                stream_endpoints.insert(
                    (
                        instance.instance_key().to_owned(),
                        endpoint.capability_id().to_owned(),
                    ),
                    endpoint.clone(),
                );
            }
            for endpoint in generation.event_endpoints() {
                event_endpoints.insert(
                    (
                        instance.instance_key().to_owned(),
                        endpoint.capability_id().to_owned(),
                    ),
                    endpoint.clone(),
                );
            }
            generations.insert(instance.instance_key().to_owned(), generation);
        }

        let bindings = plan
            .capability_bindings()
            .iter()
            .filter_map(|binding| {
                endpoints
                    .get(&(
                        binding.provider_instance().to_owned(),
                        binding.capability_id().to_owned(),
                    ))
                    .map(|endpoint| {
                        PreparedBinding::new(
                            binding.consumer_instance(),
                            binding.provider_instance(),
                            endpoint.clone(),
                        )
                        .with_requirement_id(binding.requirement_id())
                    })
            })
            .collect();
        let prepared_stream_bindings = plan
            .capability_bindings()
            .iter()
            .filter_map(|binding| {
                stream_endpoints
                    .get(&(
                        binding.provider_instance().to_owned(),
                        binding.capability_id().to_owned(),
                    ))
                    .map(|endpoint| {
                        PreparedStreamBinding::new(
                            binding.consumer_instance(),
                            binding.provider_instance(),
                            endpoint.clone(),
                        )
                        .with_requirement_id(binding.requirement_id())
                    })
            })
            .collect();
        let prepared_event_bindings = plan
            .capability_bindings()
            .iter()
            .filter_map(|binding| {
                event_endpoints
                    .get(&(
                        binding.provider_instance().to_owned(),
                        binding.capability_id().to_owned(),
                    ))
                    .map(|endpoint| {
                        PreparedEventBinding::new(
                            binding.consumer_instance(),
                            binding.provider_instance(),
                            endpoint.clone(),
                        )
                        .with_requirement_id(binding.requirement_id())
                    })
            })
            .collect();
        Ok(PreparedNativeApp::new(bindings, generations)
            .with_stream_bindings(prepared_stream_bindings)
            .with_event_bindings(prepared_event_bindings))
    }

    fn recreate(
        &self,
        plan: &ResolvedAppPlan,
        instance_key: &str,
    ) -> Result<PreparedNativePlugin, RuntimeFailure> {
        let instance = plan
            .plugin_instances()
            .iter()
            .find(|instance| instance.instance_key() == instance_key)
            .ok_or_else(|| RuntimeFailure::InvalidResolvedPlan {
                detail: format!("unknown Plugin Instance `{instance_key}`"),
            })?;
        self.prepare_instance(plan, instance)
    }
}

fn validate_plan(plan: &ResolvedAppPlan) -> Result<(), RuntimeFailure> {
    plan.validate()
        .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
            detail: error.to_string(),
        })
}

#[derive(Debug)]
struct BunEventQueue {
    capacity: usize,
    state: RefCell<BunEventQueueState>,
}

#[derive(Debug, Default)]
struct BunEventQueueState {
    active: usize,
    next_ticket: u64,
    serving: Option<u64>,
    waiters: VecDeque<(u64, oneshot::Sender<()>)>,
}

impl BunEventQueue {
    fn new(admission: EventAdmissionPlan) -> Rc<Self> {
        Rc::new(Self {
            capacity: admission.capacity(),
            state: RefCell::new(BunEventQueueState::default()),
        })
    }

    fn try_acquire(queue: &Rc<Self>) -> Option<BunEventPermit> {
        let mut state = queue.state.borrow_mut();
        if state.active >= queue.capacity {
            return None;
        }
        state.active += 1;
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.wrapping_add(1);
        let turn = if state.serving.is_none() {
            state.serving = Some(ticket);
            None
        } else {
            let (sender, receiver) = oneshot::channel();
            state.waiters.push_back((ticket, sender));
            Some(receiver)
        };
        Some(BunEventPermit {
            queue: queue.clone(),
            ticket,
            turn,
            started: false,
        })
    }

    fn release(&self, ticket: u64, started: bool) {
        let mut state = self.state.borrow_mut();
        state.active = state.active.saturating_sub(1);
        if started || state.serving == Some(ticket) {
            if state.serving != Some(ticket) {
                return;
            }
            if let Some((next_ticket, sender)) = state.waiters.pop_front() {
                state.serving = Some(next_ticket);
                let _ = sender.send(());
            } else {
                state.serving = None;
            }
        } else if let Some(position) = state
            .waiters
            .iter()
            .position(|(queued_ticket, _)| *queued_ticket == ticket)
        {
            state.waiters.remove(position);
        }
    }
}

#[derive(Debug)]
struct BunEventPermit {
    queue: Rc<BunEventQueue>,
    ticket: u64,
    turn: Option<oneshot::Receiver<()>>,
    started: bool,
}

impl BunEventPermit {
    async fn wait_turn(&mut self) {
        if let Some(turn) = self.turn.take() {
            let _ = turn.await;
        }
        self.started = true;
    }
}

impl Drop for BunEventPermit {
    fn drop(&mut self) {
        self.queue.release(self.ticket, self.started);
    }
}

#[derive(Debug)]
struct BunRequestEndpoint {
    capability: String,
    codec: Rc<dyn BunCapabilityCodec>,
    transport: TransportClient,
}

impl NativeRequestEndpoint for BunRequestEndpoint {
    fn capability_id(&self) -> &'static str {
        self.codec.capability_id()
    }

    fn descriptor_version(&self) -> &'static str {
        self.codec.descriptor_version()
    }

    fn operations(&self) -> &'static [&'static str] {
        self.codec.request_operations()
    }

    fn invoke(
        &self,
        operation: &str,
        request: Box<dyn Any>,
        context: lenso_kernel::InvocationContext,
    ) -> LocalBoxFuture<'static, Result<Result<Box<dyn Any>, Box<dyn Any>>, RuntimeFailure>> {
        if !self.codec.request_operations().contains(&operation) {
            return Box::pin(futures::future::ready(Err(
                RuntimeFailure::UnknownOperation {
                    capability: self.codec.capability_id(),
                    operation: operation.to_owned(),
                },
            )));
        }
        let payload = match self.codec.encode_request(operation, request.as_ref()) {
            Ok(payload) => payload,
            Err(error) => return Box::pin(futures::future::ready(Err(error))),
        };
        let operation = operation.to_owned();
        let wire_request = wire_request(
            &context,
            self.capability.clone(),
            operation.clone(),
            payload,
        );
        let call = match self.transport.request(wire_request) {
            Ok(call) => call,
            Err(error) => return Box::pin(futures::future::ready(Err(error))),
        };
        let codec = self.codec.clone();
        Box::pin(async move {
            match call.await? {
                WireOutcome::Success { value } => codec.decode_response(&operation, value).map(Ok),
                WireOutcome::Domain { value } => {
                    codec.decode_domain_error(&operation, value).map(Err)
                }
                WireOutcome::Runtime { failure } => {
                    Err(from_wire_failure(codec.capability_id(), failure))
                }
            }
        })
    }
}

#[derive(Debug)]
struct BunEventEndpoint {
    capability: String,
    codec: Rc<dyn BunCapabilityCodec>,
    transport: TransportClient,
    event_queues: BTreeMap<String, Rc<BunEventQueue>>,
}

impl NativeEventEndpoint for BunEventEndpoint {
    fn capability_id(&self) -> &'static str {
        self.codec.capability_id()
    }

    fn descriptor_version(&self) -> &'static str {
        self.codec.descriptor_version()
    }

    fn operations(&self) -> &'static [&'static str] {
        self.codec.event_operations()
    }

    fn owns_event_admission(&self) -> bool {
        true
    }

    fn publish(
        &self,
        operation: &str,
        event: Box<dyn Any>,
        context: lenso_kernel::InvocationContext,
    ) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        if !self.codec.event_operations().contains(&operation) {
            return Box::pin(futures::future::ready(Err(
                RuntimeFailure::UnknownOperation {
                    capability: self.codec.capability_id(),
                    operation: operation.to_owned(),
                },
            )));
        }
        let payload = match self.codec.encode_event(operation, event.as_ref()) {
            Ok(payload) => payload,
            Err(error) => return Box::pin(futures::future::ready(Err(error))),
        };
        let caller_instance = context.caller_instance().unwrap_or_default().to_owned();
        let Some(queue) = self.event_queues.get(&caller_instance) else {
            return Box::pin(futures::future::ready(Err(RuntimeFailure::Unavailable {
                capability: self.codec.capability_id(),
            })));
        };
        let Some(permit) = BunEventQueue::try_acquire(queue) else {
            return Box::pin(futures::future::ready(Err(
                RuntimeFailure::ResourceExhausted {
                    capability: self.codec.capability_id(),
                    operation: operation.to_owned(),
                },
            )));
        };
        let wire_event = wire_event(
            &context,
            self.capability.clone(),
            operation.to_owned(),
            payload,
        );
        let capability = self.codec.capability_id();
        let transport = self.transport.clone();
        Box::pin(async move {
            let mut permit = permit;
            permit.wait_turn().await;
            let call = transport.publish_event(wire_event)?;
            match call.await? {
                WireOutcome::Success { .. } => Ok(()),
                WireOutcome::Runtime { failure } => Err(from_wire_failure(capability, failure)),
                WireOutcome::Domain { .. } => Err(RuntimeFailure::ProtocolViolation { capability }),
            }
        })
    }
}

#[derive(Debug)]
struct BunStreamEndpoint {
    capability: String,
    codec: Rc<dyn BunCapabilityCodec>,
    transport: TransportClient,
}

#[derive(Debug)]
struct BunStreamSession {
    inner: Rc<dyn NativeStreamSession>,
    codec: Rc<dyn BunCapabilityCodec>,
    operation: String,
}

impl NativeStreamSession for BunStreamSession {
    fn send(&self, message: Box<dyn Any>) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        let payload = match self
            .codec
            .encode_stream_message(&self.operation, message.as_ref())
        {
            Ok(payload) => payload,
            Err(error) => return Box::pin(futures::future::ready(Err(error))),
        };
        self.inner.send(Box::new(payload))
    }

    fn receive(&self) -> LocalBoxFuture<'static, Result<NativeStreamItem, RuntimeFailure>> {
        let inner = self.inner.clone();
        let codec = self.codec.clone();
        let operation = self.operation.clone();
        Box::pin(async move {
            match inner.receive().await? {
                NativeStreamItem::Message(message) => {
                    let payload = message.downcast::<Value>().map_err(|_| {
                        RuntimeFailure::ProtocolViolation {
                            capability: codec.capability_id(),
                        }
                    })?;
                    codec
                        .decode_stream_message(&operation, *payload)
                        .map(NativeStreamItem::Message)
                }
                NativeStreamItem::PeerHalfClosed => Ok(NativeStreamItem::PeerHalfClosed),
                NativeStreamItem::Terminal(outcome) => match outcome {
                    Ok(()) => Ok(NativeStreamItem::Terminal(Ok(()))),
                    Err(error) => {
                        let payload = error.downcast::<Value>().map_err(|_| {
                            RuntimeFailure::ProtocolViolation {
                                capability: codec.capability_id(),
                            }
                        })?;
                        codec
                            .decode_domain_error(&operation, *payload)
                            .map(|error| NativeStreamItem::Terminal(Err(error)))
                    }
                },
            }
        })
    }

    fn close_send(&self) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        self.inner.close_send()
    }

    fn cancel(&self) {
        self.inner.cancel();
    }
}

impl NativeStreamEndpoint for BunStreamEndpoint {
    fn capability_id(&self) -> &'static str {
        self.codec.capability_id()
    }

    fn descriptor_version(&self) -> &'static str {
        self.codec.descriptor_version()
    }

    fn operations(&self) -> &'static [&'static str] {
        self.codec.stream_operations()
    }

    fn open(
        &self,
        operation: &str,
        request: Box<dyn Any>,
        context: lenso_kernel::InvocationContext,
    ) -> LocalBoxFuture<
        'static,
        Result<Result<Box<dyn NativeStreamSession>, Box<dyn Any>>, RuntimeFailure>,
    > {
        if !self.codec.stream_operations().contains(&operation) {
            return Box::pin(futures::future::ready(Err(
                RuntimeFailure::UnknownOperation {
                    capability: self.codec.capability_id(),
                    operation: operation.to_owned(),
                },
            )));
        }
        let payload = match self.codec.encode_stream_open(operation, request.as_ref()) {
            Ok(payload) => payload,
            Err(error) => return Box::pin(futures::future::ready(Err(error))),
        };
        let operation = operation.to_owned();
        let wire_request = wire_stream_open(
            &context,
            self.capability.clone(),
            operation.clone(),
            payload,
        );
        let call = match self.transport.open_stream(wire_request) {
            Ok(call) => call,
            Err(error) => return Box::pin(futures::future::ready(Err(error))),
        };
        let transport = self.transport.clone();
        let codec = self.codec.clone();
        Box::pin(async move {
            match call.await? {
                WireStreamOutcome::Opened { stream_id, credit } => {
                    let session = TransportStreamSession::new(
                        transport.clone(),
                        stream_id,
                        transport.session(),
                        codec.capability_id(),
                        operation.clone(),
                        credit,
                    );
                    Ok(Ok(Box::new(BunStreamSession {
                        inner: Rc::new(session),
                        codec,
                        operation,
                    }) as Box<dyn NativeStreamSession>))
                }
                WireStreamOutcome::Domain { value } => {
                    codec.decode_domain_error(&operation, value).map(Err)
                }
                WireStreamOutcome::Runtime { failure } => {
                    Err(from_wire_failure(codec.capability_id(), failure))
                }
                _ => Err(RuntimeFailure::ProtocolViolation {
                    capability: codec.capability_id(),
                }),
            }
        })
    }
}

#[derive(Debug)]
struct BunPluginLifecycle {
    transport: TransportClient,
    configuration: String,
    codecs: BTreeMap<String, Rc<dyn BunCapabilityCodec>>,
    authoring_version: u32,
}

impl BunPluginLifecycle {
    fn managed_activation(&self, context: &ActivateContext, required: bool) -> PluginFuture {
        if !self.transport.supports_managed_lifecycle() {
            return Box::pin(futures::future::ready(if required {
                Err(RuntimeFailure::InvalidResolvedPlan {
                    detail: "Bun authoring version 2 requires the managed json-rpc-http lifecycle"
                        .to_owned(),
                })
            } else {
                Ok(())
            }));
        }
        let imports = match start_host_imports(context, &self.codecs) {
            Ok(imports) => imports,
            Err(error) => return Box::pin(futures::future::ready(Err(error))),
        };
        let configuration = match serde_json::from_str::<Value>(&self.configuration) {
            Ok(configuration) => configuration,
            Err(error) => {
                return Box::pin(futures::future::ready(Err(
                    RuntimeFailure::InvalidResolvedPlan {
                        detail: format!("Bun Plugin configuration is not JSON: {error}"),
                    },
                )));
            }
        };
        self.transport.activate(serde_json::json!({
            "configuration": configuration,
            "imports_url": imports.url,
            "imports_token": imports.token,
            "imports": imports.descriptors,
        }))
    }
}

impl PluginLifecycle for BunPluginLifecycle {
    fn prepare(&self, context: lenso_kernel::PrepareContext) -> PluginFuture {
        let resource = BunProcessResource {
            transport: self.transport.clone(),
        };
        Box::pin(async move {
            context
                .resources()
                .register(resource)
                .map_err(|error| RuntimeFailure::Internal {
                    detail: format!("failed to register Bun process resource: {error:?}"),
                })?;
            Ok(())
        })
    }

    fn construct(&self, context: ActivateContext) -> PluginFuture {
        if self.authoring_version == 2 {
            self.managed_activation(&context, true)
        } else {
            Box::pin(futures::future::ready(Ok(())))
        }
    }

    fn activate(&self, context: ActivateContext) -> PluginFuture {
        let exit = self.transport.exit_waiter();
        let cancellation = context.cancellation();
        let admission_closed = context.admission().wait_closed();
        let activation = if self.authoring_version == 1 {
            self.managed_activation(&context, false)
        } else {
            Box::pin(futures::future::ready(Ok(())))
        };
        let task = async move {
            let exit = exit.fuse();
            let cancellation = cancellation.cancelled().fuse();
            let admission_closed = admission_closed.fuse();
            let stop = futures::future::select(cancellation, admission_closed)
                .map(|_| ())
                .fuse();
            futures::pin_mut!(exit, stop);
            match futures::future::select(exit, stop).await {
                futures::future::Either::Left((Ok(()), _)) => {
                    panic!("Bun child process exited; supervision must recreate the generation");
                }
                futures::future::Either::Left((Err(_), _))
                | futures::future::Either::Right(((), _)) => {}
            }
        };
        Box::pin(async move {
            activation.await?;
            context
                .tasks()
                .spawn_local(Box::pin(task))
                .map_err(|error| RuntimeFailure::Internal {
                    detail: format!("failed to monitor Bun child process: {error:?}"),
                })?;
            Ok(())
        })
    }

    fn deactivate(&self, _context: DeactivateContext) -> PluginFuture {
        Box::pin(futures::future::ready(Ok(())))
    }
}

#[derive(Debug)]
struct BunProcessResource {
    transport: TransportClient,
}

impl ManagedResource for BunProcessResource {
    fn release(&self) -> lenso_kernel::ResourceFuture {
        let transport = self.transport.clone();
        Box::pin(async move {
            transport.shutdown();
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use lenso_runtime_codec::ArtifactHandle;
    use sha2::{Digest as _, Sha256};

    use super::*;

    #[derive(Debug)]
    struct TestCodec;

    impl BunCapabilityCodec for TestCodec {
        fn capability_id(&self) -> &'static str {
            "example.greeting@1"
        }

        fn descriptor_version(&self) -> &'static str {
            "1.0.0"
        }

        fn operations(&self) -> &'static [&'static str] {
            &["greet"]
        }

        fn encode_request(
            &self,
            _operation: &str,
            request: &dyn Any,
        ) -> Result<Value, RuntimeFailure> {
            let request =
                request
                    .downcast_ref::<String>()
                    .ok_or(RuntimeFailure::ProtocolViolation {
                        capability: self.capability_id(),
                    })?;
            Ok(Value::String(request.clone()))
        }

        fn decode_response(
            &self,
            _operation: &str,
            value: Value,
        ) -> Result<Box<dyn Any>, RuntimeFailure> {
            Ok(Box::new(value))
        }

        fn decode_domain_error(
            &self,
            _operation: &str,
            value: Value,
        ) -> Result<Box<dyn Any>, RuntimeFailure> {
            Ok(Box::new(value))
        }
    }

    #[test]
    fn production_defaults_to_the_selected_json_rpc_wire() {
        assert_eq!(BunAdapter::production("bun").wire(), BunWire::JsonRpcHttp);
        let adapter = BunAdapter::new("bun", BunWire::FramedStdio).with_codec(TestCodec);
        assert_eq!(adapter.wire(), BunWire::FramedStdio);
    }

    #[test]
    fn admitted_artifact_replaces_shared_plan_entrypoint() {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("selected.js");
        let bytes = b"export default {};\n";
        std::fs::write(&source, bytes).unwrap();
        let digest = format!("sha256:{:x}", Sha256::digest(bytes));
        let artifact = ArtifactHandle::open(&source, &digest, bytes.len() as u64).unwrap();
        let admitted_path = artifact.path().to_owned();
        let artifacts = ArtifactCatalog::new()
            .with_artifact("plugin", artifact)
            .unwrap();
        let instance = PluginInstancePlan::new("plugin", "company.plugin")
            .with_entrypoint("plugin.js")
            .with_execution_class(ExecutionClassId::bun_child_process());
        let adapter = BunAdapter::production("bun").with_artifacts(artifacts);

        assert_eq!(adapter.entrypoint_for(&instance).unwrap(), admitted_path);
        let missing = PluginInstancePlan::new("other", "company.plugin")
            .with_entrypoint("selected.js")
            .with_execution_class(ExecutionClassId::bun_child_process());
        assert!(matches!(
            adapter.entrypoint_for(&missing),
            Err(RuntimeFailure::InvalidResolvedPlan { detail })
                if detail.contains("no admitted Artifact")
        ));
    }
}

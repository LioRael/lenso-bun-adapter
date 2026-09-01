//! Experimental in-process Bun Execution Adapter backed by `libbun`.
//!
//! This crate intentionally defines a separate Execution Class. It does not
//! replace the production Bun child-process Adapter and never falls back to it.

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use futures::{FutureExt, channel::oneshot};
use lenso_app_plan::{ExecutionClassId, PluginInstancePlan, ResolvedAppPlan};
use lenso_bun_adapter::BunCapabilityCodec;
use lenso_kernel::{
    DeactivateContext, ExecutionAdapter, InvocationContext, ManagedResource, NativeRequestEndpoint,
    PluginFuture, PluginLifecycle, PreparedBinding, PreparedNativeApp, PreparedNativePlugin,
    RuntimeFailure,
};
use libbun::{
    BunHost, BunModuleSpec, BunRuntimeConfig, ProviderCallResult, ProviderContractIdentity,
    ProviderDeadline, ProviderDomainClass, ProviderRequest, ProviderSettleOptions,
    SettledProviderReceipt, StructuralValue, dynamic::DynamicBunRuntime,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Experimental trusted in-process Bun execution identity.
pub const EXECUTION_CLASS: &str = "lenso.bun-embedded@1";

/// Host-owned limits and paths for one experimental embedded Bun Adapter.
#[derive(Clone, Debug)]
pub struct LibbunAdapterConfig {
    plugin_path: PathBuf,
    working_directory: PathBuf,
    export_name: String,
    queue_capacity: usize,
    max_request_bytes: usize,
    max_response_bytes: usize,
    max_entrypoint_bytes: u64,
    max_call_duration: Duration,
    environment: BTreeMap<String, String>,
}

impl LibbunAdapterConfig {
    /// Selects one exact replaceable libbun native plugin.
    pub fn new(plugin_path: impl Into<PathBuf>) -> Self {
        Self {
            plugin_path: plugin_path.into(),
            working_directory: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            export_name: "lensoInvoke".to_owned(),
            queue_capacity: 32,
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 1024 * 1024,
            max_entrypoint_bytes: 4 * 1024 * 1024,
            max_call_duration: Duration::from_secs(5),
            environment: BTreeMap::new(),
        }
    }

    /// Selects the directory used for relative Plan entrypoints.
    #[must_use]
    pub fn with_working_directory(mut self, working_directory: impl Into<PathBuf>) -> Self {
        self.working_directory = working_directory.into();
        self
    }

    /// Selects the single provider dispatch export.
    #[must_use]
    pub fn with_export_name(mut self, export_name: impl Into<String>) -> Self {
        self.export_name = export_name.into();
        self
    }

    /// Bounds admitted calls waiting for the affine Bun VM.
    #[must_use]
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity.max(1);
        self
    }

    /// Bounds serialized request and response values.
    #[must_use]
    pub fn with_value_limits(
        mut self,
        max_request_bytes: usize,
        max_response_bytes: usize,
    ) -> Self {
        self.max_request_bytes = max_request_bytes.max(1);
        self.max_response_bytes = max_response_bytes.max(1);
        self
    }

    /// Bounds the selected entrypoint file and each libbun call.
    #[must_use]
    pub fn with_execution_limits(
        mut self,
        max_entrypoint_bytes: u64,
        max_call_duration: Duration,
    ) -> Self {
        self.max_entrypoint_bytes = max_entrypoint_bytes.max(1);
        self.max_call_duration = max_call_duration.max(Duration::from_millis(1));
        self
    }

    /// Supplies an explicit non-secret environment overlay to the embedded runtime.
    #[must_use]
    pub fn with_environment_overlay(
        mut self,
        environment: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.environment = environment
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        self
    }
}

/// One explicitly selected experimental in-process Bun Adapter.
pub struct LibbunAdapter {
    config: LibbunAdapterConfig,
    codecs: BTreeMap<String, Rc<dyn BunCapabilityCodec>>,
    duplicate_codecs: BTreeSet<String>,
    engine_factory: Arc<dyn EngineFactory>,
}

impl std::fmt::Debug for LibbunAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LibbunAdapter")
            .field("config", &self.config)
            .field("capabilities", &self.codecs.keys().collect::<Vec<_>>())
            .field("duplicate_codecs", &self.duplicate_codecs)
            .finish_non_exhaustive()
    }
}

impl LibbunAdapter {
    /// Creates an Adapter that loads the exact configured native plugin.
    pub fn new(config: LibbunAdapterConfig) -> Self {
        Self {
            config,
            codecs: BTreeMap::new(),
            duplicate_codecs: BTreeSet::new(),
            engine_factory: Arc::new(DynamicEngineFactory),
        }
    }

    /// Registers one generated Capability codec at the Adapter edge.
    #[must_use]
    pub fn with_codec(mut self, codec: impl BunCapabilityCodec) -> Self {
        let capability = codec.capability_id().to_owned();
        if self
            .codecs
            .insert(capability.clone(), Rc::new(codec))
            .is_some()
        {
            self.duplicate_codecs.insert(capability);
        }
        self
    }

    fn selected_instances(plan: &ResolvedAppPlan) -> Vec<&PluginInstancePlan> {
        plan.plugin_instances()
            .iter()
            .filter(|instance| instance.execution_class().as_str() == EXECUTION_CLASS)
            .collect()
    }

    fn validate_instance(
        &self,
        instance: &PluginInstancePlan,
    ) -> Result<ValidatedInstance, RuntimeFailure> {
        if !instance.required_capabilities().is_empty() {
            return invalid(format!(
                "embedded Bun Instance `{}` must be a leaf Provider without required Capabilities",
                instance.instance_key()
            ));
        }
        if instance.provided_capabilities().is_empty() {
            return invalid(format!(
                "embedded Bun Instance `{}` provides no request Capability",
                instance.instance_key()
            ));
        }
        if !self.duplicate_codecs.is_empty() {
            return invalid(format!(
                "duplicate generated codecs registered for {:?}",
                self.duplicate_codecs
            ));
        }
        if self.config.export_name.trim().is_empty() {
            return invalid("embedded Bun provider export name is empty");
        }

        let entrypoint = resolve_file(
            &self.config.working_directory,
            instance.entrypoint(),
            "entrypoint",
        )?;
        let entrypoint_size = entrypoint
            .metadata()
            .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
                detail: format!(
                    "failed to inspect embedded Bun entrypoint `{}`: {error}",
                    entrypoint.display()
                ),
            })?
            .len();
        if entrypoint_size > self.config.max_entrypoint_bytes {
            return invalid(format!(
                "embedded Bun entrypoint `{}` exceeds max_entrypoint_bytes",
                entrypoint.display()
            ));
        }

        let configuration: Value =
            serde_json::from_str(instance.configuration()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: format!(
                        "embedded Bun Instance `{}` configuration is not JSON: {error}",
                        instance.instance_key()
                    ),
                }
            })?;
        let mut endpoints = Vec::with_capacity(instance.provided_capabilities().len());
        for descriptor in instance.provided_capabilities() {
            if !descriptor.stream_operations().is_empty()
                || !descriptor.event_operations().is_empty()
            {
                return invalid(format!(
                    "embedded Bun Capability `{}` must contain request Operations only",
                    descriptor.capability_id()
                ));
            }
            let codec = self.codecs.get(descriptor.capability_id()).ok_or_else(|| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: format!(
                        "libbun Adapter has no generated codec for Capability `{}`",
                        descriptor.capability_id()
                    ),
                }
            })?;
            let codec_operations: Vec<_> = codec
                .request_operations()
                .iter()
                .map(|operation| (*operation).to_owned())
                .collect();
            if codec.descriptor_version() != descriptor.descriptor_version()
                || codec_operations != descriptor.operations()
                || !codec.stream_operations().is_empty()
                || !codec.event_operations().is_empty()
            {
                return Err(RuntimeFailure::ProtocolViolation {
                    capability: codec.capability_id(),
                });
            }
            endpoints.push(ValidatedEndpoint {
                codec: codec.clone(),
                contract_fingerprint: contract_fingerprint(
                    descriptor.capability_id(),
                    descriptor.descriptor_version(),
                    descriptor.operations(),
                ),
            });
        }

        Ok(ValidatedInstance {
            instance_key: instance.instance_key().to_owned(),
            package_id: instance.package_id().to_owned(),
            entrypoint,
            configuration,
            endpoints,
        })
    }

    fn prepare_validated(
        &self,
        validated: ValidatedInstance,
    ) -> Result<PreparedNativePlugin, RuntimeFailure> {
        let plugin_path = resolve_file(Path::new("."), &self.config.plugin_path, "native plugin")?;
        let runtime_config = BunRuntimeConfig::new(
            format!("lenso:{}", validated.instance_key),
            &self.config.working_directory,
        )
        .with_environment_overlay(self.config.environment.clone());
        let worker = WorkerControl::start(
            self.engine_factory.clone(),
            plugin_path,
            runtime_config,
            self.config.queue_capacity,
        )?;
        let worker = Arc::new(worker);
        let endpoints = validated
            .endpoints
            .into_iter()
            .map(|endpoint| {
                Rc::new(EmbeddedRequestEndpoint {
                    capability: endpoint.codec.capability_id(),
                    descriptor_version: endpoint.codec.descriptor_version(),
                    operations: endpoint.codec.request_operations(),
                    contract_fingerprint: endpoint.contract_fingerprint,
                    package_id: validated.package_id.clone(),
                    module: BunModuleSpec::Path {
                        path: validated.entrypoint.clone(),
                    },
                    export_name: self.config.export_name.clone(),
                    configuration: validated.configuration.clone(),
                    codec: endpoint.codec,
                    worker: worker.clone(),
                    max_request_bytes: self.config.max_request_bytes,
                    max_response_bytes: self.config.max_response_bytes,
                    max_call_duration: self.config.max_call_duration,
                }) as Rc<dyn NativeRequestEndpoint>
            })
            .collect();
        Ok(PreparedNativePlugin::new(
            endpoints,
            EmbeddedPluginLifecycle { worker },
        ))
    }

    fn prepare_instance(
        &self,
        instance: &PluginInstancePlan,
    ) -> Result<PreparedNativePlugin, RuntimeFailure> {
        let validated = self.validate_instance(instance)?;
        self.prepare_validated(validated)
    }

    #[cfg(test)]
    fn with_engine_factory(mut self, factory: Arc<dyn EngineFactory>) -> Self {
        self.engine_factory = factory;
        self
    }
}

impl ExecutionAdapter for LibbunAdapter {
    fn execution_class(&self) -> ExecutionClassId {
        ExecutionClassId::new(EXECUTION_CLASS)
    }

    fn prepare(&self, plan: &ResolvedAppPlan) -> Result<PreparedNativeApp, RuntimeFailure> {
        plan.validate()
            .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
                detail: error.to_string(),
            })?;
        let instances = Self::selected_instances(plan);
        if instances.len() > 1 {
            return invalid(format!(
                "execution class `{EXECUTION_CLASS}` currently supports one Instance per Plan, found {}",
                instances.len()
            ));
        }
        let Some(instance) = instances.first().copied() else {
            return Ok(PreparedNativeApp::empty());
        };

        // Validate the entire generation before acquiring the process-global Bun runtime lease.
        let validated = self.validate_instance(instance)?;
        let generation = self.prepare_validated(validated)?;
        let mut endpoint_by_capability = BTreeMap::new();
        for endpoint in generation.endpoints() {
            endpoint_by_capability.insert(endpoint.capability_id(), endpoint.clone());
        }
        let bindings = plan
            .capability_bindings()
            .iter()
            .filter(|binding| binding.provider_instance() == instance.instance_key())
            .filter_map(|binding| {
                endpoint_by_capability
                    .get(binding.capability_id())
                    .map(|endpoint| {
                        PreparedBinding::new(
                            binding.consumer_instance(),
                            binding.provider_instance(),
                            endpoint.clone(),
                        )
                    })
            })
            .collect();
        let generations = BTreeMap::from([(instance.instance_key().to_owned(), generation)]);
        Ok(PreparedNativeApp::new(bindings, generations))
    }

    fn recreate(
        &self,
        plan: &ResolvedAppPlan,
        instance_key: &str,
    ) -> Result<PreparedNativePlugin, RuntimeFailure> {
        let instance = plan.plugin_instance(instance_key).ok_or_else(|| {
            RuntimeFailure::InvalidResolvedPlan {
                detail: format!("unknown Plugin Instance `{instance_key}`"),
            }
        })?;
        if instance.execution_class().as_str() != EXECUTION_CLASS {
            return invalid(format!(
                "Plugin Instance `{instance_key}` does not select `{EXECUTION_CLASS}`"
            ));
        }
        self.prepare_instance(instance)
    }
}

struct ValidatedInstance {
    instance_key: String,
    package_id: String,
    entrypoint: PathBuf,
    configuration: Value,
    endpoints: Vec<ValidatedEndpoint>,
}

struct ValidatedEndpoint {
    codec: Rc<dyn BunCapabilityCodec>,
    contract_fingerprint: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EmbeddedInvocation {
    capability: &'static str,
    operation: String,
    request: Value,
    configuration: Value,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum EmbeddedResultEnvelope {
    Ok { value: Value },
    DomainError { value: Value },
}

struct EmbeddedRequestEndpoint {
    capability: &'static str,
    descriptor_version: &'static str,
    operations: &'static [&'static str],
    contract_fingerprint: String,
    package_id: String,
    module: BunModuleSpec,
    export_name: String,
    configuration: Value,
    codec: Rc<dyn BunCapabilityCodec>,
    worker: Arc<WorkerControl>,
    max_request_bytes: usize,
    max_response_bytes: usize,
    max_call_duration: Duration,
}

type ErasedInvocationResult = Result<Result<Box<dyn Any>, Box<dyn Any>>, RuntimeFailure>;

impl std::fmt::Debug for EmbeddedRequestEndpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddedRequestEndpoint")
            .field("capability", &self.capability)
            .field("descriptor_version", &self.descriptor_version)
            .field("operations", &self.operations)
            .field("package_id", &self.package_id)
            .finish_non_exhaustive()
    }
}

impl NativeRequestEndpoint for EmbeddedRequestEndpoint {
    fn capability_id(&self) -> &'static str {
        self.capability
    }

    fn descriptor_version(&self) -> &'static str {
        self.descriptor_version
    }

    fn operations(&self) -> &'static [&'static str] {
        self.operations
    }

    fn invoke(
        &self,
        operation: &str,
        request: Box<dyn Any>,
        context: InvocationContext,
    ) -> futures::future::LocalBoxFuture<'static, ErasedInvocationResult> {
        if !self.operations.contains(&operation) {
            return Box::pin(futures::future::ready(Err(
                RuntimeFailure::UnknownOperation {
                    capability: self.capability,
                    operation: operation.to_owned(),
                },
            )));
        }
        if context.cancellation().is_cancelled() {
            return Box::pin(futures::future::ready(Err(RuntimeFailure::Cancelled {
                request_id: context.request_id(),
            })));
        }
        let call = match self.build_call(operation, request.as_ref(), context.request_id()) {
            Ok(call) => call,
            Err(error) => return Box::pin(futures::future::ready(Err(error))),
        };
        let (response, receiver) = oneshot::channel();
        let command = WorkerCommand::Call(Box::new(WorkerCall {
            request: call.0,
            options: call.1,
            response,
        }));
        match self.worker.commands.try_send(command) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return Box::pin(futures::future::ready(Err(
                    RuntimeFailure::ResourceExhausted {
                        capability: self.capability,
                        operation: operation.to_owned(),
                    },
                )));
            }
            Err(TrySendError::Disconnected(_)) => {
                return Box::pin(futures::future::ready(Err(RuntimeFailure::PluginFailure {
                    detail: "embedded Bun runtime worker is unavailable".to_owned(),
                })));
            }
        }

        let capability = self.capability;
        let codec = self.codec.clone();
        let operation = operation.to_owned();
        let cancellation = context.cancellation();
        let request_id = context.request_id();
        let max_response_bytes = self.max_response_bytes;
        Box::pin(async move {
            let response = receiver.fuse();
            let cancelled = cancellation.cancelled().fuse();
            futures::pin_mut!(response, cancelled);
            let result = match futures::future::select(response, cancelled).await {
                futures::future::Either::Left((Ok(result), _)) => {
                    result.map_err(|error| RuntimeFailure::PluginFailure {
                        detail: bounded_detail(format!(
                            "embedded Bun provider execution failed: {}",
                            error.detail
                        )),
                    })?
                }
                futures::future::Either::Left((Err(_), _)) => {
                    return Err(RuntimeFailure::PluginFailure {
                        detail: "embedded Bun runtime worker stopped before replying".to_owned(),
                    });
                }
                futures::future::Either::Right(((), _)) => {
                    return Err(RuntimeFailure::Cancelled { request_id });
                }
            };
            decode_result(
                result,
                capability,
                &operation,
                max_response_bytes,
                codec.as_ref(),
            )
        })
    }
}

impl EmbeddedRequestEndpoint {
    fn build_call(
        &self,
        operation: &str,
        request: &dyn Any,
        request_id: u64,
    ) -> Result<(ProviderRequest, ProviderSettleOptions), RuntimeFailure> {
        let invocation = EmbeddedInvocation {
            capability: self.capability,
            operation: operation.to_owned(),
            request: self.codec.encode_request(operation, request)?,
            configuration: self.configuration.clone(),
        };
        let input = serde_json::to_value(invocation).map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to encode embedded Bun invocation: {error}"),
        })?;
        if encoded_len(&input) > self.max_request_bytes {
            return Err(RuntimeFailure::ResourceExhausted {
                capability: self.capability,
                operation: operation.to_owned(),
            });
        }
        let request = ProviderRequest {
            contract: ProviderContractIdentity {
                package: self.package_id.clone(),
                capability: self.capability.to_owned(),
                contract_fingerprint: self.contract_fingerprint.clone(),
            },
            domain: ProviderDomainClass::ApplicationIo,
            module: self.module.clone(),
            export: self.export_name.clone(),
            input: StructuralValue(input),
        };
        let options = ProviderSettleOptions::new(ProviderDeadline::after(self.max_call_duration))
            .with_call_id(request_id.to_string());
        Ok((request, options))
    }
}

fn decode_result(
    result: ProviderCallResult,
    capability: &'static str,
    operation: &str,
    max_response_bytes: usize,
    codec: &dyn BunCapabilityCodec,
) -> ErasedInvocationResult {
    let value = match result {
        ProviderCallResult::Ok(StructuralValue(value)) => value,
        ProviderCallResult::Err(error) => {
            return Err(RuntimeFailure::PluginFailure {
                detail: bounded_detail(format!(
                    "embedded Bun provider rejected with `{}`: {}",
                    error.code, error.message
                )),
            });
        }
    };
    if encoded_len(&value) > max_response_bytes {
        return Err(RuntimeFailure::ResourceExhausted {
            capability,
            operation: operation.to_owned(),
        });
    }
    match serde_json::from_value::<EmbeddedResultEnvelope>(value)
        .map_err(|_| RuntimeFailure::ProtocolViolation { capability })?
    {
        EmbeddedResultEnvelope::Ok { value } => codec.decode_response(operation, value).map(Ok),
        EmbeddedResultEnvelope::DomainError { value } => {
            codec.decode_domain_error(operation, value).map(Err)
        }
    }
}

#[derive(Debug)]
struct EmbeddedPluginLifecycle {
    worker: Arc<WorkerControl>,
}

impl PluginLifecycle for EmbeddedPluginLifecycle {
    fn prepare(&self, context: lenso_kernel::PrepareContext) -> PluginFuture {
        let resource = EmbeddedRuntimeResource {
            worker: self.worker.clone(),
        };
        Box::pin(async move {
            context
                .resources()
                .register(resource)
                .map_err(|error| RuntimeFailure::Internal {
                    detail: format!("failed to register embedded Bun runtime resource: {error:?}"),
                })?;
            Ok(())
        })
    }

    fn deactivate(&self, _context: DeactivateContext) -> PluginFuture {
        Box::pin(futures::future::ready(Ok(())))
    }
}

#[derive(Debug)]
struct EmbeddedRuntimeResource {
    worker: Arc<WorkerControl>,
}

impl ManagedResource for EmbeddedRuntimeResource {
    fn release(&self) -> lenso_kernel::ResourceFuture {
        self.worker.release()
    }
}

struct WorkerControl {
    commands: SyncSender<WorkerCommand>,
    shutdown: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<Result<(), EngineFailure>>>>,
}

impl std::fmt::Debug for WorkerControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerControl")
            .field("shutdown", &self.shutdown.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl WorkerControl {
    fn start(
        factory: Arc<dyn EngineFactory>,
        plugin_path: PathBuf,
        config: BunRuntimeConfig,
        queue_capacity: usize,
    ) -> Result<Self, RuntimeFailure> {
        let (commands, receiver) = mpsc::sync_channel(queue_capacity.max(1));
        let (startup, ready) = mpsc::sync_channel(1);
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = shutdown.clone();
        let join = thread::Builder::new()
            .name("lenso-libbun-runtime".to_owned())
            .spawn(move || {
                let mut engine = match factory.open(&plugin_path, config) {
                    Ok(engine) => {
                        let _ = startup.send(Ok(()));
                        engine
                    }
                    Err(error) => {
                        let _ = startup.send(Err(error.clone()));
                        return Err(error);
                    }
                };
                while !worker_shutdown.load(Ordering::Acquire) {
                    match receiver.recv() {
                        Ok(WorkerCommand::Call(call)) => {
                            let WorkerCall {
                                request,
                                options,
                                response,
                            } = *call;
                            let result = engine.call(request, options);
                            let _ = response.send(result);
                        }
                        Ok(WorkerCommand::Shutdown) | Err(_) => break,
                    }
                }
                engine.shutdown()
            })
            .map_err(|error| RuntimeFailure::Internal {
                detail: format!("failed to spawn embedded Bun runtime worker: {error}"),
            })?;
        match ready.recv() {
            Ok(Ok(())) => Ok(Self {
                commands,
                shutdown,
                join: Mutex::new(Some(join)),
            }),
            Ok(Err(error)) => {
                let _ = join.join();
                Err(RuntimeFailure::PluginFailure {
                    detail: bounded_detail(format!(
                        "failed to initialize embedded Bun runtime: {}",
                        error.detail
                    )),
                })
            }
            Err(_) => {
                let _ = join.join();
                Err(RuntimeFailure::PluginFailure {
                    detail: "embedded Bun runtime stopped during initialization".to_owned(),
                })
            }
        }
    }

    fn begin_shutdown(&self) -> Option<JoinHandle<Result<(), EngineFailure>>> {
        if !self.shutdown.swap(true, Ordering::AcqRel) {
            let _ = self.commands.try_send(WorkerCommand::Shutdown);
        }
        self.join.lock().ok().and_then(|mut join| join.take())
    }

    fn release(&self) -> lenso_kernel::ResourceFuture {
        let Some(join) = self.begin_shutdown() else {
            return Box::pin(futures::future::ready(Ok(())));
        };
        let (complete, waiter) = oneshot::channel();
        let spawn = thread::Builder::new()
            .name("lenso-libbun-shutdown".to_owned())
            .spawn(move || {
                let result = join.join();
                let _ = complete.send(result);
            });
        if let Err(error) = spawn {
            return Box::pin(futures::future::ready(Err(RuntimeFailure::Internal {
                detail: format!("failed to join embedded Bun runtime worker: {error}"),
            })));
        }
        Box::pin(async move {
            match waiter.await {
                Ok(Ok(Ok(()))) => Ok(()),
                Ok(Ok(Err(error))) => Err(RuntimeFailure::PluginFailure {
                    detail: bounded_detail(format!(
                        "embedded Bun runtime shutdown failed: {}",
                        error.detail
                    )),
                }),
                Ok(Err(_)) => Err(RuntimeFailure::PluginFailure {
                    detail: "embedded Bun runtime worker panicked during shutdown".to_owned(),
                }),
                Err(_) => Err(RuntimeFailure::Internal {
                    detail: "embedded Bun shutdown joiner stopped without a result".to_owned(),
                }),
            }
        })
    }
}

impl Drop for WorkerControl {
    fn drop(&mut self) {
        if let Some(join) = self.begin_shutdown() {
            let _ = thread::Builder::new()
                .name("lenso-libbun-drop".to_owned())
                .spawn(move || {
                    let _ = join.join();
                });
        }
    }
}

enum WorkerCommand {
    Call(Box<WorkerCall>),
    Shutdown,
}

struct WorkerCall {
    request: ProviderRequest,
    options: ProviderSettleOptions,
    response: oneshot::Sender<Result<ProviderCallResult, EngineFailure>>,
}

#[derive(Clone, Debug)]
struct EngineFailure {
    detail: String,
}

trait ProviderEngine {
    fn call(
        &mut self,
        request: ProviderRequest,
        options: ProviderSettleOptions,
    ) -> Result<ProviderCallResult, EngineFailure>;

    fn shutdown(&mut self) -> Result<(), EngineFailure>;
}

trait EngineFactory: Send + Sync {
    fn open(
        &self,
        plugin_path: &Path,
        config: BunRuntimeConfig,
    ) -> Result<Box<dyn ProviderEngine>, EngineFailure>;
}

struct DynamicEngineFactory;

impl EngineFactory for DynamicEngineFactory {
    fn open(
        &self,
        plugin_path: &Path,
        config: BunRuntimeConfig,
    ) -> Result<Box<dyn ProviderEngine>, EngineFailure> {
        let runtime = DynamicBunRuntime::load(plugin_path, config.clone()).map_err(engine_error)?;
        Ok(Box::new(DynamicProviderEngine {
            host: BunHost::from_runtime(config, runtime),
        }))
    }
}

struct DynamicProviderEngine {
    host: BunHost<DynamicBunRuntime>,
}

impl ProviderEngine for DynamicProviderEngine {
    fn call(
        &mut self,
        request: ProviderRequest,
        options: ProviderSettleOptions,
    ) -> Result<ProviderCallResult, EngineFailure> {
        match self
            .host
            .call_provider_until_settled(request, options)
            .map_err(engine_error)?
        {
            SettledProviderReceipt::Ready { result, .. } => Ok(result),
            SettledProviderReceipt::Failed(failure) => Err(EngineFailure {
                detail: bounded_detail(format!(
                    "libbun {:?} failed for export `{}`: {}",
                    failure.operation,
                    failure.export_name,
                    failure
                        .js_error_message
                        .as_deref()
                        .unwrap_or("JavaScriptCore exposed no error message")
                )),
            }),
        }
    }

    fn shutdown(&mut self) -> Result<(), EngineFailure> {
        self.host.shutdown().map_err(engine_error)
    }
}

fn engine_error(error: impl std::fmt::Display) -> EngineFailure {
    EngineFailure {
        detail: bounded_detail(error.to_string()),
    }
}

fn resolve_file(
    base: &Path,
    selected: impl AsRef<Path>,
    label: &str,
) -> Result<PathBuf, RuntimeFailure> {
    let selected = selected.as_ref();
    let candidate = if selected.is_absolute() {
        selected.to_owned()
    } else {
        base.join(selected)
    };
    candidate
        .canonicalize()
        .ok()
        .filter(|path| path.is_file())
        .ok_or_else(|| RuntimeFailure::InvalidResolvedPlan {
            detail: format!(
                "embedded Bun {label} `{}` is not a file",
                candidate.display()
            ),
        })
}

fn contract_fingerprint(capability: &str, version: &str, operations: &[String]) -> String {
    let mut digest = Sha256::new();
    digest.update(capability.as_bytes());
    digest.update([0]);
    digest.update(version.as_bytes());
    for operation in operations {
        digest.update([0]);
        digest.update(operation.as_bytes());
    }
    format!("sha256:{:x}", digest.finalize())
}

fn encoded_len(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |bytes| bytes.len())
}

fn bounded_detail(detail: impl Into<String>) -> String {
    const MAX_DETAIL_BYTES: usize = 4096;
    let detail = detail.into();
    if detail.len() <= MAX_DETAIL_BYTES {
        return detail;
    }
    let mut end = MAX_DETAIL_BYTES;
    while !detail.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &detail[..end])
}

fn invalid<T>(detail: impl Into<String>) -> Result<T, RuntimeFailure> {
    Err(RuntimeFailure::InvalidResolvedPlan {
        detail: detail.into(),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use lenso_app_plan::{
        CapabilityEndpointPlan, CapabilityRequirementPlan, ExecutionClassId, PluginInstancePlan,
    };
    use lenso_kernel::{CancellationToken, InvocationContext, NativeRequestEndpoint};
    use serde_json::{Value, json};
    use tempfile::TempDir;

    use super::*;

    const CAPABILITY: &str = "example.greeting@1";
    const VERSION: &str = "1.0.0";

    #[derive(Debug)]
    struct GreetingCodec;

    impl BunCapabilityCodec for GreetingCodec {
        fn capability_id(&self) -> &'static str {
            CAPABILITY
        }

        fn descriptor_version(&self) -> &'static str {
            VERSION
        }

        fn operations(&self) -> &'static [&'static str] {
            &["greet"]
        }

        fn encode_request(
            &self,
            operation: &str,
            request: &dyn Any,
        ) -> Result<Value, RuntimeFailure> {
            if operation != "greet" {
                return Err(RuntimeFailure::UnknownOperation {
                    capability: CAPABILITY,
                    operation: operation.to_owned(),
                });
            }
            request
                .downcast_ref::<String>()
                .map(|name| json!({ "name": name }))
                .ok_or(RuntimeFailure::ProtocolViolation {
                    capability: CAPABILITY,
                })
        }

        fn decode_response(
            &self,
            _operation: &str,
            value: Value,
        ) -> Result<Box<dyn Any>, RuntimeFailure> {
            value
                .get("message")
                .and_then(Value::as_str)
                .map(|message| Box::new(message.to_owned()) as Box<dyn Any>)
                .ok_or(RuntimeFailure::ProtocolViolation {
                    capability: CAPABILITY,
                })
        }

        fn decode_domain_error(
            &self,
            _operation: &str,
            value: Value,
        ) -> Result<Box<dyn Any>, RuntimeFailure> {
            value
                .as_str()
                .map(|error| Box::new(error.to_owned()) as Box<dyn Any>)
                .ok_or(RuntimeFailure::ProtocolViolation {
                    capability: CAPABILITY,
                })
        }
    }

    #[derive(Clone, Copy)]
    enum FakeMode {
        Greeting,
        Malformed,
        OpenFailure,
    }

    struct FakeFactory {
        mode: FakeMode,
        opens: Arc<AtomicUsize>,
        shutdowns: Arc<AtomicUsize>,
    }

    impl EngineFactory for FakeFactory {
        fn open(
            &self,
            _plugin_path: &Path,
            _config: BunRuntimeConfig,
        ) -> Result<Box<dyn ProviderEngine>, EngineFailure> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            if matches!(self.mode, FakeMode::OpenFailure) {
                return Err(EngineFailure {
                    detail: "fixture startup failure".to_owned(),
                });
            }
            Ok(Box::new(FakeEngine {
                mode: self.mode,
                shutdowns: self.shutdowns.clone(),
            }))
        }
    }

    struct FakeEngine {
        mode: FakeMode,
        shutdowns: Arc<AtomicUsize>,
    }

    impl ProviderEngine for FakeEngine {
        fn call(
            &mut self,
            request: ProviderRequest,
            _options: ProviderSettleOptions,
        ) -> Result<ProviderCallResult, EngineFailure> {
            if matches!(self.mode, FakeMode::Malformed) {
                return Ok(ProviderCallResult::Ok(StructuralValue(json!({
                    "message": "missing envelope"
                }))));
            }
            let name = request.input.0["request"]["name"]
                .as_str()
                .unwrap_or_default();
            let value = if name.is_empty() {
                json!({ "kind": "domain_error", "value": "empty_name" })
            } else {
                json!({ "kind": "ok", "value": { "message": format!("Hello, {name}") } })
            };
            Ok(ProviderCallResult::Ok(StructuralValue(value)))
        }

        fn shutdown(&mut self) -> Result<(), EngineFailure> {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct Fixture {
        directory: TempDir,
        plugin_path: PathBuf,
        entrypoint: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().expect("fixture directory should be created");
            let plugin_path = directory.path().join("libbun.fixture");
            let entrypoint = directory.path().join("provider.mjs");
            fs::write(&plugin_path, b"fixture").expect("fixture plugin should be written");
            fs::write(&entrypoint, b"export function lensoInvoke() {}")
                .expect("fixture entrypoint should be written");
            Self {
                directory,
                plugin_path,
                entrypoint,
            }
        }

        fn instance(&self) -> PluginInstancePlan {
            PluginInstancePlan::new("provider", "fixture.greeting")
                .with_entrypoint(self.entrypoint.to_string_lossy())
                .with_execution_class(ExecutionClassId::new(EXECUTION_CLASS))
                .with_capability(CapabilityEndpointPlan::new(CAPABILITY, VERSION, ["greet"]))
        }
    }

    fn adapter(
        fixture: &Fixture,
        mode: FakeMode,
    ) -> (LibbunAdapter, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let opens = Arc::new(AtomicUsize::new(0));
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let factory = FakeFactory {
            mode,
            opens: opens.clone(),
            shutdowns: shutdowns.clone(),
        };
        let adapter = LibbunAdapter::new(
            LibbunAdapterConfig::new(&fixture.plugin_path)
                .with_working_directory(fixture.directory.path()),
        )
        .with_codec(GreetingCodec)
        .with_engine_factory(Arc::new(factory));
        (adapter, opens, shutdowns)
    }

    fn invoke(endpoint: &dyn NativeRequestEndpoint, name: &str) -> ErasedInvocationResult {
        futures::executor::block_on(endpoint.invoke(
            "greet",
            Box::new(name.to_owned()),
            InvocationContext::new(7, None, CancellationToken::new()),
        ))
    }

    #[test]
    fn request_success_and_domain_error_remain_distinct() {
        let fixture = Fixture::new();
        let (adapter, opens, _shutdowns) = adapter(&fixture, FakeMode::Greeting);
        let prepared = adapter
            .prepare_instance(&fixture.instance())
            .expect("valid embedded provider should prepare");
        let endpoint = prepared.endpoints()[0].clone();

        let success = invoke(endpoint.as_ref(), "Ada")
            .expect("runtime should succeed")
            .expect("provider should return success");
        assert_eq!(
            *success.downcast::<String>().expect("typed response"),
            "Hello, Ada"
        );

        let domain_error = invoke(endpoint.as_ref(), "")
            .expect("runtime should succeed")
            .expect_err("provider should return a Domain Error");
        assert_eq!(
            *domain_error
                .downcast::<String>()
                .expect("typed Domain Error"),
            "empty_name"
        );
        assert_eq!(opens.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn malformed_result_is_a_protocol_violation() {
        let fixture = Fixture::new();
        let (adapter, _opens, _shutdowns) = adapter(&fixture, FakeMode::Malformed);
        let prepared = adapter
            .prepare_instance(&fixture.instance())
            .expect("valid embedded provider should prepare");
        let error = invoke(prepared.endpoints()[0].as_ref(), "Ada")
            .expect_err("malformed envelope must fail closed");
        assert!(matches!(
            error,
            RuntimeFailure::ProtocolViolation {
                capability: CAPABILITY
            }
        ));
    }

    #[test]
    fn invalid_plan_is_rejected_before_runtime_open() {
        let fixture = Fixture::new();
        let (adapter, opens, _shutdowns) = adapter(&fixture, FakeMode::Greeting);
        let instance = fixture
            .instance()
            .with_requirement(CapabilityRequirementPlan::one(
                "example.dependency@1",
                "1.0.0",
            ));
        let error = adapter
            .prepare_instance(&instance)
            .expect_err("embedded provider with dependencies must be rejected");
        assert!(matches!(error, RuntimeFailure::InvalidResolvedPlan { .. }));
        assert_eq!(opens.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn stream_is_rejected_before_runtime_open() {
        let fixture = Fixture::new();
        let (adapter, opens, _shutdowns) = adapter(&fixture, FakeMode::Greeting);
        let instance = PluginInstancePlan::new("provider", "fixture.greeting")
            .with_entrypoint(fixture.entrypoint.to_string_lossy())
            .with_execution_class(ExecutionClassId::new(EXECUTION_CLASS))
            .with_capability(
                CapabilityEndpointPlan::new(CAPABILITY, VERSION, ["greet"])
                    .with_stream_operation("greet"),
            );
        let error = adapter
            .prepare_instance(&instance)
            .expect_err("stream operation must be rejected");
        assert!(matches!(error, RuntimeFailure::InvalidResolvedPlan { .. }));
        assert_eq!(opens.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn startup_failure_is_a_plugin_failure() {
        let fixture = Fixture::new();
        let (adapter, opens, _shutdowns) = adapter(&fixture, FakeMode::OpenFailure);
        let error = adapter
            .prepare_instance(&fixture.instance())
            .expect_err("runtime startup failure must be visible");
        assert!(matches!(error, RuntimeFailure::PluginFailure { .. }));
        assert_eq!(opens.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn worker_release_shuts_engine_down_once() {
        let fixture = Fixture::new();
        let (_adapter, opens, shutdowns) = adapter(&fixture, FakeMode::Greeting);
        let factory = Arc::new(FakeFactory {
            mode: FakeMode::Greeting,
            opens: opens.clone(),
            shutdowns: shutdowns.clone(),
        });
        let worker = WorkerControl::start(
            factory,
            fixture.plugin_path.clone(),
            BunRuntimeConfig::new("fixture", fixture.directory.path()),
            1,
        )
        .expect("worker should start");
        futures::executor::block_on(worker.release()).expect("worker should stop cleanly");
        futures::executor::block_on(worker.release()).expect("second release should be idempotent");
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn pre_cancelled_request_never_reaches_engine() {
        let fixture = Fixture::new();
        let (adapter, _opens, _shutdowns) = adapter(&fixture, FakeMode::Greeting);
        let prepared = adapter
            .prepare_instance(&fixture.instance())
            .expect("valid embedded provider should prepare");
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = futures::executor::block_on(prepared.endpoints()[0].invoke(
            "greet",
            Box::new("Ada".to_owned()),
            InvocationContext::new(11, None, cancellation),
        ))
        .expect_err("pre-cancelled request must fail");
        assert!(matches!(
            error,
            RuntimeFailure::Cancelled { request_id: 11 }
        ));
    }

    #[test]
    #[ignore = "requires LIBBUN_PLUGIN_PATH to point at an ABI-compatible native plugin"]
    fn real_dynamic_plugin_smoke() {
        let plugin_path = std::env::var_os("LIBBUN_PLUGIN_PATH")
            .map(PathBuf::from)
            .expect("LIBBUN_PLUGIN_PATH must select the native plugin");
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let entrypoint = manifest_dir.join("tests/fixtures/provider.mjs");
        let instance = PluginInstancePlan::new("provider", "fixture.greeting")
            .with_entrypoint(entrypoint.to_string_lossy())
            .with_configuration(r#"{"prefix":"Hello"}"#)
            .with_execution_class(ExecutionClassId::new(EXECUTION_CLASS))
            .with_capability(CapabilityEndpointPlan::new(CAPABILITY, VERSION, ["greet"]));
        let adapter = LibbunAdapter::new(
            LibbunAdapterConfig::new(plugin_path).with_working_directory(&manifest_dir),
        )
        .with_codec(GreetingCodec);
        let prepared = adapter
            .prepare_instance(&instance)
            .expect("real embedded provider should prepare");
        let success = invoke(prepared.endpoints()[0].as_ref(), "Ada")
            .expect("real runtime should succeed")
            .expect("real provider should return success");
        assert_eq!(
            *success.downcast::<String>().expect("typed response"),
            "Hello, Ada"
        );
    }
}

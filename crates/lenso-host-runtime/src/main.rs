use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lenso::host::{self, FileControlStateStore, HostBuilder, KernelGenerationRuntime};
use lenso_bun_adapter::{BunAdapter, BunAdapterConfig, BunCapabilityCodec, BunWire};
use lenso_host_distribution::VerifiedDistribution;
use lenso_kernel::{ExecutionAdapterCatalog, RuntimeFailure};
use lenso_plugin_control_plane::{
    AppGenerationTransitionSpec, CanonicalDocument, CatalogFactory, ControlLifecycle,
    ControlPlaneError, ControlStateStore, ReplacementMode, ResolvedGeneration, RolloutPolicy,
};
use lenso_process_adapter::ProcessAdapter;
use lenso_runtime_codec::JsonCapabilityCodec;
use serde_json::Value;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

fn main() {
    let result = parse_arguments().and_then(|arguments| run(&arguments));
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run(arguments: &Arguments) -> Result<(), Box<dyn std::error::Error>> {
    let distribution_root = arguments
        .distribution_lock
        .parent()
        .and_then(Path::parent)
        .ok_or("distribution lock needs a distribution root")?;
    let expected_lock =
        std::fs::canonicalize(distribution_root.join(".lenso/distribution.lock.json"))?;
    let supplied_lock = std::fs::canonicalize(&arguments.distribution_lock)?;
    if supplied_lock != expected_lock {
        return Err("--distribution must name the distribution's canonical lock".into());
    }
    let distribution = VerifiedDistribution::open(distribution_root)?;
    let identity = distribution.identity().to_owned();
    let app_root = std::fs::canonicalize(&arguments.app_root)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(tokio::task::LocalSet::new().run_until(async move {
        let options = host::control::ControlOptions {
            distribution: identity,
            startup_timeout: arguments.startup_timeout,
            stop_timeout: arguments.stop_timeout,
        };
        host::control::serve(
            options,
            tokio::io::stdin(),
            tokio::io::stdout(),
            move || {
                start_host(
                    distribution,
                    app_root,
                    arguments.startup_timeout,
                    arguments.stop_timeout,
                )
            },
        )
        .await
    }))?;
    Ok(())
}

async fn start_host(
    distribution: VerifiedDistribution,
    app_root: PathBuf,
    startup_timeout: Duration,
    stop_timeout: Duration,
) -> Result<(host::Host<lenso_kernel::NativeApp>, ResolvedGeneration), ControlPlaneError> {
    let app_id = distribution.app_id().to_owned();
    let bun = distribution.root().join("runtime/bun");
    let resolution_distribution = distribution.clone();
    let resolution_root = app_root.clone();
    let prepared =
        tokio::task::spawn_blocking(move || resolution_distribution.resolve(resolution_root))
            .await
            .map_err(host_failure)?
            .map_err(host_failure)?;
    let factory = HostCatalogFactory::new(bun, app_root.clone(), &prepared.generation)?;
    let runtime = KernelGenerationRuntime::new(factory);
    let store = FileControlStateStore::open(app_root.join(".lenso/runtime-control"))?;
    let state = store.load(&app_id)?;
    let candidate = prepared.generation;
    let live = state
        .generations
        .iter()
        .filter(|record| {
            matches!(
                record.lifecycle,
                ControlLifecycle::Staged
                    | ControlLifecycle::Ready
                    | ControlLifecycle::Active
                    | ControlLifecycle::Draining
                    | ControlLifecycle::Standby
            )
        })
        .map(|record| record.generation_spec_digest.as_str())
        .collect::<BTreeSet<_>>();
    let builder = HostBuilder::new(&app_id, runtime, store);
    let host = if live.is_empty() {
        activate_initial(builder.build()?, &candidate, startup_timeout, stop_timeout).await?
    } else if live.iter().all(|digest| *digest == candidate.spec.digest()) {
        let generations = BTreeMap::from([(candidate.spec.digest().to_owned(), candidate.clone())]);
        builder.recover(&generations, unix_nanos()?).await?
    } else if state.host_suspended {
        activate_initial(
            builder.replace_suspended()?,
            &candidate,
            startup_timeout,
            stop_timeout,
        )
        .await?
    } else {
        return Err(host_failure(
            "durable state needs an unavailable previous Generation; cleanly suspend the old Host before replacing its build",
        ));
    };
    Ok((host, candidate))
}

async fn activate_initial(
    mut host: host::Host<lenso_kernel::NativeApp>,
    candidate: &ResolvedGeneration,
    startup_timeout: Duration,
    stop_timeout: Duration,
) -> Result<host::Host<lenso_kernel::NativeApp>, ControlPlaneError> {
    let transition = CanonicalDocument::from_value(
        "lenso-generation-transition.json",
        AppGenerationTransitionSpec {
            schema_version: 1,
            app_id: candidate.spec.value().app_id.clone(),
            from_generation_spec_digest: None,
            to_generation_spec_digest: candidate.spec.digest().to_owned(),
            replacement_mode: ReplacementMode::Initial,
            state_compatibility_receipt_digests: Vec::new(),
            rollout_policy: RolloutPolicy {
                ready_timeout_nanos: startup_timeout.as_nanos().to_string(),
                drain_timeout_nanos: stop_timeout.as_nanos().to_string(),
                rollback_window_nanos: "0".to_owned(),
                automatic_rollback_on_generation_failure: false,
            },
        },
    )?;
    if let Err(error) = host
        .transition(transition, candidate.clone(), BTreeMap::new())
        .await
    {
        return match host.drain_and_suspend(stop_timeout).await {
            Ok(_) => Err(error),
            Err(cleanup) => Err(host_failure(format!(
                "initial Generation activation failed: {error}; cleanup failed: {cleanup}"
            ))),
        };
    }
    Ok(host)
}

#[derive(Clone, Debug)]
struct PortableJsonCodec {
    capability_id: &'static str,
    descriptor_version: &'static str,
    operations: &'static [&'static str],
}

impl PortableJsonCodec {
    fn encode(&self, operation: &str, value: &dyn Any) -> Result<Value, RuntimeFailure> {
        self.require_operation(operation)?;
        value
            .downcast_ref::<Value>()
            .cloned()
            .ok_or(RuntimeFailure::ProtocolViolation {
                capability: self.capability_id,
            })
    }

    fn decode(&self, operation: &str, value: Value) -> Result<Box<dyn Any>, RuntimeFailure> {
        self.require_operation(operation)?;
        Ok(Box::new(value))
    }

    fn require_operation(&self, operation: &str) -> Result<(), RuntimeFailure> {
        if self.operations.contains(&operation) {
            Ok(())
        } else {
            Err(RuntimeFailure::UnknownOperation {
                capability: self.capability_id,
                operation: operation.to_owned(),
            })
        }
    }
}

impl BunCapabilityCodec for PortableJsonCodec {
    fn capability_id(&self) -> &'static str {
        self.capability_id
    }

    fn descriptor_version(&self) -> &'static str {
        self.descriptor_version
    }

    fn operations(&self) -> &'static [&'static str] {
        self.operations
    }

    fn encode_request(&self, operation: &str, request: &dyn Any) -> Result<Value, RuntimeFailure> {
        self.encode(operation, request)
    }

    fn decode_response(
        &self,
        operation: &str,
        value: Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        self.decode(operation, value)
    }

    fn decode_domain_error(
        &self,
        operation: &str,
        value: Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        self.decode(operation, value)
    }
}

impl JsonCapabilityCodec for PortableJsonCodec {
    fn capability_id(&self) -> &'static str {
        self.capability_id
    }

    fn descriptor_version(&self) -> &'static str {
        self.descriptor_version
    }

    fn request_operations(&self) -> &'static [&'static str] {
        self.operations
    }

    fn encode_request(&self, operation: &str, request: &dyn Any) -> Result<Value, RuntimeFailure> {
        self.encode(operation, request)
    }

    fn decode_response(
        &self,
        operation: &str,
        value: Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        self.decode(operation, value)
    }

    fn decode_domain_error(
        &self,
        operation: &str,
        value: Value,
    ) -> Result<Box<dyn Any>, RuntimeFailure> {
        self.decode(operation, value)
    }
}

#[derive(Debug)]
struct HostCatalogFactory {
    bun: PathBuf,
    working_directory: PathBuf,
    codecs: Vec<PortableJsonCodec>,
}

impl HostCatalogFactory {
    fn new(
        bun: PathBuf,
        working_directory: PathBuf,
        generation: &ResolvedGeneration,
    ) -> Result<Self, ControlPlaneError> {
        let mut endpoints = BTreeMap::<String, (String, Vec<String>)>::new();
        for endpoint in generation
            .plan
            .plugin_instances()
            .iter()
            .flat_map(|instance| instance.provided_capabilities().iter())
        {
            if !endpoint.stream_operations().is_empty() || !endpoint.event_operations().is_empty() {
                return Err(host_failure(
                    "TypeScript Host runtime first profile accepts Request Capabilities only",
                ));
            }
            let identity = (
                endpoint.descriptor_version().to_owned(),
                endpoint
                    .operations()
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            );
            if endpoints
                .insert(endpoint.capability_id().to_owned(), identity.clone())
                .is_some_and(|previous| previous != identity)
            {
                return Err(host_failure(format!(
                    "conflicting descriptors for Capability `{}`",
                    endpoint.capability_id()
                )));
            }
        }
        let codecs = endpoints
            .into_iter()
            .map(|(id, (version, operations))| PortableJsonCodec {
                capability_id: intern_string(&id),
                descriptor_version: intern_string(&version),
                operations: intern_operations(&operations),
            })
            .collect();
        Ok(Self {
            bun,
            working_directory,
            codecs,
        })
    }
}

impl CatalogFactory for HostCatalogFactory {
    fn catalog(
        &self,
        generation: &ResolvedGeneration,
    ) -> Result<ExecutionAdapterCatalog, ControlPlaneError> {
        let selected = generation
            .plan
            .plugin_instances()
            .iter()
            .map(|instance| instance.execution_class().as_str())
            .collect::<BTreeSet<_>>();
        let mut catalog = ExecutionAdapterCatalog::new();
        if selected.contains("lenso.bun-process@1") {
            let config = BunAdapterConfig::new(&self.bun, BunWire::JsonRpcHttp)
                .with_working_directory(&self.working_directory);
            let adapter = self.codecs.iter().cloned().fold(
                BunAdapter::production(&self.bun)
                    .with_config(config)
                    .with_artifacts(generation.artifacts.clone()),
                BunAdapter::with_codec,
            );
            catalog = catalog.with_adapter(adapter).map_err(host_failure)?;
        }
        if selected.contains("lenso.process@1") {
            let adapter = self.codecs.iter().cloned().fold(
                ProcessAdapter::new(generation.artifacts.clone()),
                ProcessAdapter::with_codec,
            );
            catalog = catalog.with_adapter(adapter).map_err(host_failure)?;
        }
        if selected
            .iter()
            .any(|class| !matches!(*class, "lenso.bun-process@1" | "lenso.process@1"))
        {
            return Err(host_failure(
                "distribution selects an unsupported execution class",
            ));
        }
        Ok(catalog)
    }
}

fn intern_string(value: &str) -> &'static str {
    static VALUES: OnceLock<Mutex<HashMap<String, &'static str>>> = OnceLock::new();
    let values = VALUES.get_or_init(Mutex::default);
    let mut values = values.lock().expect("runtime string interner lock");
    if let Some(value) = values.get(value) {
        return value;
    }
    let interned = Box::leak(value.to_owned().into_boxed_str());
    values.insert(value.to_owned(), interned);
    interned
}

fn intern_operations(operations: &[String]) -> &'static [&'static str] {
    type OperationSets = HashMap<Vec<String>, &'static [&'static str]>;
    static VALUES: OnceLock<Mutex<OperationSets>> = OnceLock::new();
    let values = VALUES.get_or_init(Mutex::default);
    let mut values = values.lock().expect("runtime operation interner lock");
    if let Some(value) = values.get(operations) {
        return value;
    }
    let interned = Box::leak(
        operations
            .iter()
            .map(|operation| intern_string(operation))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    values.insert(operations.to_vec(), interned);
    interned
}

fn unix_nanos() -> Result<u128, ControlPlaneError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .map_err(host_failure)
}

fn host_failure(error: impl std::fmt::Display) -> ControlPlaneError {
    ControlPlaneError::HostFailure {
        detail: error.to_string(),
    }
}

#[derive(Debug)]
struct Arguments {
    distribution_lock: PathBuf,
    app_root: PathBuf,
    startup_timeout: Duration,
    stop_timeout: Duration,
}

fn parse_arguments() -> Result<Arguments, Box<dyn std::error::Error>> {
    let mut arguments = env::args_os().skip(1);
    let mut distribution_lock = None;
    let mut app_root = None;
    let mut startup_timeout = None;
    let mut stop_timeout = None;
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--distribution") if distribution_lock.is_none() => {
                distribution_lock = arguments.next().map(PathBuf::from);
            }
            Some("--root") if app_root.is_none() => {
                app_root = arguments.next().map(PathBuf::from);
            }
            Some("--startup-ms") if startup_timeout.is_none() => {
                startup_timeout = Some(parse_budget(arguments.next(), "--startup-ms")?);
            }
            Some("--stop-ms") if stop_timeout.is_none() => {
                stop_timeout = Some(parse_budget(arguments.next(), "--stop-ms")?);
            }
            _ => return Err("usage: lenso-host-runtime --distribution LOCK --root ROOT".into()),
        }
    }
    Ok(Arguments {
        distribution_lock: distribution_lock.ok_or("missing --distribution")?,
        app_root: app_root.ok_or("missing --root")?,
        startup_timeout: startup_timeout.unwrap_or(STARTUP_TIMEOUT),
        stop_timeout: stop_timeout.unwrap_or(STOP_TIMEOUT),
    })
}

fn parse_budget(
    value: Option<std::ffi::OsString>,
    name: &str,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let milliseconds = value
        .and_then(|value| value.to_str().and_then(|value| value.parse::<u64>().ok()))
        .ok_or_else(|| format!("{name} requires integer milliseconds"))?;
    if !(1..=60_000).contains(&milliseconds) {
        return Err(format!("{name} must be between 1 and 60000 milliseconds").into());
    }
    Ok(Duration::from_millis(milliseconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interners_reuse_runtime_contract_storage() {
        assert!(std::ptr::eq(
            intern_string("company.capability"),
            intern_string("company.capability")
        ));
        let operations = vec!["get".to_owned(), "put".to_owned()];
        assert!(std::ptr::eq(
            intern_operations(&operations),
            intern_operations(&operations)
        ));
    }
}

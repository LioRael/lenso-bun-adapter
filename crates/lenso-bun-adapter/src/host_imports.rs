use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read as _,
    net::TcpListener,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use futures::{FutureExt as _, StreamExt as _, channel::mpsc as futures_mpsc};
use lenso_kernel::{
    ActivateContext, InvocationContext, ManagedResource, PluginDependencyHandle, RuntimeFailure,
    SealedInvocationExtension,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tiny_http::{Header, Response, Server, StatusCode};

use crate::{
    BunCapabilityCodec,
    protocol::{BunInvocationExtension, WireOutcome, to_wire_failure},
};

const IMPORT_QUEUE_CAPACITY: usize = 32;
const IMPORT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const IMPORT_HTTP_POLL: Duration = Duration::from_millis(100);
static NEXT_IMPORT_SERVER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub(crate) struct HostImportsActivation {
    pub(crate) url: String,
    pub(crate) token: String,
    pub(crate) descriptors: Vec<ImportDescriptor>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ImportDescriptor {
    requirement_id: String,
    capability_id: String,
    descriptor_version: String,
    operations: Vec<String>,
    stream_operations: Vec<String>,
    event_operations: Vec<String>,
}

#[derive(Debug)]
struct HostImportCall {
    request: ImportRequest,
    response: mpsc::SyncSender<WireOutcome>,
}

#[derive(Debug, Deserialize)]
struct ImportEnvelope {
    jsonrpc: String,
    id: Value,
    method: String,
    params: Vec<ImportRequest>,
}

#[derive(Debug, Deserialize)]
struct ImportRequest {
    #[serde(rename = "request_id")]
    _request_id: u64,
    requirement_id: String,
    capability_id: String,
    operation: String,
    #[serde(default)]
    deadline_nanos: Option<u64>,
    #[serde(default)]
    extensions: Vec<BunInvocationExtension>,
    payload: Value,
}

#[derive(Debug)]
struct ImportBinding {
    handle: PluginDependencyHandle,
    codec: Rc<dyn BunCapabilityCodec>,
}

#[derive(Clone, Debug)]
struct HostImportResource {
    shutdown: Arc<AtomicBool>,
}

impl ManagedResource for HostImportResource {
    fn release(&self) -> lenso_kernel::ResourceFuture {
        self.shutdown.store(true, Ordering::Release);
        Box::pin(futures::future::ready(Ok(())))
    }
}

pub(crate) fn start_host_imports(
    context: &ActivateContext,
    codecs: &BTreeMap<String, Rc<dyn BunCapabilityCodec>>,
) -> Result<HostImportsActivation, RuntimeFailure> {
    let mut bindings = BTreeMap::new();
    let mut descriptors = Vec::new();
    let mut seen = BTreeSet::new();
    for dependency in context.dependencies().bindings() {
        if !seen.insert(dependency.requirement_id().to_owned()) {
            return Err(RuntimeFailure::InvalidResolvedPlan {
                detail: format!(
                    "Bun requirement `{}` has multiple providers; many imports are not supported by this runtime profile",
                    dependency.requirement_id()
                ),
            });
        }
        let codec = codecs
            .get(dependency.capability_id())
            .cloned()
            .ok_or_else(|| RuntimeFailure::InvalidResolvedPlan {
                detail: format!(
                    "Bun Adapter has no generated codec for dependency `{}`",
                    dependency.capability_id()
                ),
            })?;
        let handle = dependency
            .handle()
            .ok_or_else(|| RuntimeFailure::Unavailable {
                capability: codec.capability_id(),
            })?;
        descriptors.push(ImportDescriptor {
            requirement_id: dependency.requirement_id().to_owned(),
            capability_id: dependency.capability_id().to_owned(),
            descriptor_version: handle.descriptor_version().to_owned(),
            operations: handle
                .operations()
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            stream_operations: Vec::new(),
            event_operations: Vec::new(),
        });
        bindings.insert(
            dependency.requirement_id().to_owned(),
            ImportBinding { handle, codec },
        );
    }

    let listener = TcpListener::bind("127.0.0.1:0").map_err(|error| RuntimeFailure::Internal {
        detail: format!("failed to bind Bun Host imports: {error}"),
    })?;
    let address = listener
        .local_addr()
        .map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to inspect Bun Host imports address: {error}"),
        })?;
    let server =
        Server::from_listener(listener, None).map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to start Bun Host imports: {error}"),
        })?;
    let token = import_token(context.instance_key());
    let (sender, receiver) = futures_mpsc::channel(IMPORT_QUEUE_CAPACITY);
    let shutdown = Arc::new(AtomicBool::new(false));
    spawn_http_server(server, token.clone(), sender, Arc::clone(&shutdown))?;

    let cancellation = context.cancellation();
    let admission = context.admission();
    context
        .tasks()
        .spawn_local(Box::pin(run_import_dispatch(
            receiver,
            bindings,
            context.dependencies().clone(),
            cancellation,
            admission,
        )))
        .map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to run Bun Host imports: {error:?}"),
        })?;
    context
        .resources()
        .register(HostImportResource { shutdown })
        .map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to register Bun Host imports: {error:?}"),
        })?;

    Ok(HostImportsActivation {
        url: format!("http://{address}"),
        token,
        descriptors,
    })
}

fn spawn_http_server(
    server: Server,
    token: String,
    mut sender: futures_mpsc::Sender<HostImportCall>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), RuntimeFailure> {
    thread::Builder::new()
        .name("lenso-bun-host-imports".to_owned())
        .spawn(move || {
            while !shutdown.load(Ordering::Acquire) {
                let Ok(Some(mut request)) = server.recv_timeout(IMPORT_HTTP_POLL) else {
                    continue;
                };
                let authorized = request.headers().iter().any(|header| {
                    header.field.equiv("authorization")
                        && header.value.as_str() == format!("Bearer {token}")
                });
                if !authorized {
                    let _ = request.respond(Response::empty(StatusCode(401)));
                    continue;
                }
                let mut body = Vec::new();
                let read = request
                    .as_reader()
                    .take(crate::DEFAULT_MAX_FRAME_BYTES as u64 + 1)
                    .read_to_end(&mut body);
                if read.is_err() || body.len() > crate::DEFAULT_MAX_FRAME_BYTES {
                    let _ = request.respond(Response::empty(StatusCode(413)));
                    continue;
                }
                let Ok(envelope) = serde_json::from_slice::<ImportEnvelope>(&body) else {
                    let _ = request.respond(Response::empty(StatusCode(400)));
                    continue;
                };
                if envelope.jsonrpc != "2.0"
                    || envelope.method != "lenso.import"
                    || envelope.params.len() != 1
                {
                    let _ = request.respond(Response::empty(StatusCode(400)));
                    continue;
                }
                let (response_sender, response_receiver) = mpsc::sync_channel(1);
                let call = HostImportCall {
                    request: envelope.params.into_iter().next().expect("length checked"),
                    response: response_sender,
                };
                let outcome = if sender.try_send(call).is_err() {
                    WireOutcome::Runtime {
                        failure: to_wire_failure(&RuntimeFailure::ResourceExhausted {
                            capability: "lenso.bun-host-imports@1",
                            operation: "import".to_owned(),
                        }),
                    }
                } else {
                    response_receiver
                        .recv_timeout(IMPORT_RESPONSE_TIMEOUT)
                        .unwrap_or_else(|_| WireOutcome::Runtime {
                            failure: to_wire_failure(&RuntimeFailure::PluginFailure {
                                detail: "Bun Host import response timed out".to_owned(),
                            }),
                        })
                };
                let body = serde_json::to_vec(&json!({
                    "jsonrpc": "2.0",
                    "id": envelope.id,
                    "result": outcome,
                }))
                .unwrap_or_default();
                let mut response = Response::from_data(body);
                if let Ok(header) = Header::from_bytes("content-type", "application/json") {
                    response.add_header(header);
                }
                let _ = request.respond(response);
            }
        })
        .map(|_| ())
        .map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to start Bun Host imports thread: {error}"),
        })
}

async fn run_import_dispatch(
    mut receiver: futures_mpsc::Receiver<HostImportCall>,
    bindings: BTreeMap<String, ImportBinding>,
    dependencies: lenso_kernel::PluginDependencies,
    cancellation: lenso_kernel::CancellationToken,
    admission: lenso_kernel::AppAdmission,
) {
    let cancelled = cancellation.cancelled().fuse();
    let admission_closed = admission.wait_closed().fuse();
    let stopped = futures::future::select(cancelled, admission_closed)
        .map(|_| ())
        .fuse();
    futures::pin_mut!(stopped);
    loop {
        let next = receiver.next().fuse();
        futures::pin_mut!(next);
        match futures::future::select(next, stopped.as_mut()).await {
            futures::future::Either::Left((Some(call), _)) => {
                let outcome = dispatch_import(
                    &call.request,
                    &bindings,
                    &dependencies,
                    cancellation.clone(),
                )
                .await;
                let _ = call.response.send(outcome);
            }
            futures::future::Either::Left((None, _)) | futures::future::Either::Right(((), _)) => {
                return;
            }
        }
    }
}

async fn dispatch_import(
    request: &ImportRequest,
    bindings: &BTreeMap<String, ImportBinding>,
    dependencies: &lenso_kernel::PluginDependencies,
    cancellation: lenso_kernel::CancellationToken,
) -> WireOutcome {
    let Some(binding) = bindings.get(&request.requirement_id) else {
        return WireOutcome::Runtime {
            failure: to_wire_failure(&RuntimeFailure::Unavailable {
                capability: "lenso.bun-host-imports@1",
            }),
        };
    };
    if binding.codec.capability_id() != request.capability_id {
        return WireOutcome::Runtime {
            failure: to_wire_failure(&RuntimeFailure::ProtocolViolation {
                capability: "lenso.bun-host-imports@1",
            }),
        };
    }
    let mut context = match request.deadline_nanos {
        Some(nanos) => {
            dependencies.invocation_context_after(Duration::from_nanos(nanos), cancellation)
        }
        None => dependencies.invocation_context(None, cancellation),
    };
    for extension in &request.extensions {
        context = context.and_then(|context| attach_extension(context, extension));
    }
    let context = match context {
        Ok(context) => context,
        Err(error) => {
            return WireOutcome::Runtime {
                failure: to_wire_failure(&error),
            };
        }
    };
    let native = match binding
        .codec
        .decode_request(&request.operation, request.payload.clone())
    {
        Ok(native) => native,
        Err(error) => {
            return WireOutcome::Runtime {
                failure: to_wire_failure(&error),
            };
        }
    };
    match binding
        .handle
        .invoke_erased(&request.operation, native, context)
        .await
    {
        Ok(Ok(value)) => match binding
            .codec
            .encode_response(&request.operation, value.as_ref())
        {
            Ok(value) => WireOutcome::Success { value },
            Err(error) => WireOutcome::Runtime {
                failure: to_wire_failure(&error),
            },
        },
        Ok(Err(value)) => match binding
            .codec
            .encode_domain_error(&request.operation, value.as_ref())
        {
            Ok(value) => WireOutcome::Domain { value },
            Err(error) => WireOutcome::Runtime {
                failure: to_wire_failure(&error),
            },
        },
        Err(error) => WireOutcome::Runtime {
            failure: to_wire_failure(&error),
        },
    }
}

fn attach_extension(
    context: InvocationContext,
    extension: &BunInvocationExtension,
) -> Result<InvocationContext, RuntimeFailure> {
    let result = if extension.sealed {
        context.with_sealed_extension(SealedInvocationExtension::signed(
            &extension.key,
            extension.issuer.clone().unwrap_or_default(),
            extension.audience.clone(),
            extension.value.clone(),
            extension.proof.clone().unwrap_or_default(),
        ))
    } else {
        context.with_extension(&extension.key, extension.value.clone())
    };
    result.map_err(|error| RuntimeFailure::Internal {
        detail: format!("invalid Bun Host import Invocation Context: {error}"),
    })
}

fn import_token(instance: &str) -> String {
    let sequence = NEXT_IMPORT_SERVER.fetch_add(1, Ordering::Relaxed);
    let mut digest = Sha256::new();
    digest.update(instance.as_bytes());
    digest.update(std::process::id().to_le_bytes());
    digest.update(sequence.to_le_bytes());
    format!("{:x}", digest.finalize())
}

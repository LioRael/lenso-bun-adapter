use std::{
    io::{BufRead, BufReader, Write as _},
    net::SocketAddr,
    path::Path,
    process::{Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    thread,
};

use axum::{
    Json, Router,
    extract::DefaultBodyLimit,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac as _};
use jsonrpsee::{core::client::ClientT as _, http_client::HttpClient, rpc_params};
use lenso_kernel::RuntimeFailure;
use lenso_process_protocol::{
    AuthoringHandshakeProofInput,
    authoring::{
        CancelAck, CancelParams, ConstructParams, ConstructedResult, InitializeParams,
        InvocationResult, InvokeParams, OutboundCallParams, OutboundCallResult, Settlement,
        StopParams, StoppedResult,
    },
    authoring_callback_proof_message, authoring_child_proof_message,
    authoring_handshake_proof_payload, authoring_host_proof_message,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::sync::oneshot;

use crate::transport::{ProcessState, build_json_rpc_client, json_rpc_runtime, spawn_process};

pub const BUN_AUTHORING_RUNTIME_PROFILE: &str = "lenso.bun-authoring@2";
pub const BUN_AUTHORING_CALLBACK_PROOF_HEADER: &str = "x-lenso-authoring-proof";
const BOOTSTRAP_MAX_BYTES: usize = 4_096;
const STARTUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
type HmacSha256 = Hmac<Sha256>;

/// Host-side dispatch for authenticated child dependency calls and settlement observations.
pub trait BunAuthoringCallback: std::fmt::Debug + Send + Sync + 'static {
    fn call(&self, params: OutboundCallParams) -> Result<OutboundCallResult, RuntimeFailure>;
    fn settled(&self, settlement: Settlement) -> Result<(), RuntimeFailure>;
}

/// One authenticated Bun Authoring V2 child and its Host callback listener.
pub struct BunAuthoringHost {
    process: Arc<ProcessState>,
    client: HttpClient,
    callback: CallbackServer,
    initialize: InitializeParams,
    stopped: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for BunAuthoringHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BunAuthoringHost")
            .field("plugin_instance", &self.initialize.identity.plugin_instance)
            .field("callback", &self.callback.address)
            .field(
                "stopped",
                &self.stopped.load(std::sync::atomic::Ordering::Acquire),
            )
            .finish_non_exhaustive()
    }
}

impl BunAuthoringHost {
    /// Starts a child without secrets in arguments or environment and completes mutual proof.
    pub fn start(
        bun_binary: impl AsRef<Path>,
        entrypoint: impl AsRef<Path>,
        initialize: InitializeParams,
        callback: impl BunAuthoringCallback,
    ) -> Result<Self, RuntimeFailure> {
        initialize
            .validate_for_runtime_profile(BUN_AUTHORING_RUNTIME_PROFILE)
            .map_err(protocol_failure)?;
        let secret = random_32()?;
        let host_nonce = random_32()?;
        let callback = CallbackServer::start(
            secret,
            initialize.identity.session.clone(),
            initialize.limits.max_frame_bytes,
            callback,
        )?;
        let callback_origin = format!("http://{}/", callback.address);
        let mut command = Command::new(bun_binary.as_ref());
        command
            .arg(entrypoint.as_ref())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        let process = match spawn_process(command, "lenso.bun-authoring@2") {
            Ok(process) => process,
            Err(error) => {
                callback.shutdown();
                return Err(error);
            }
        };
        let result = Self::open(
            process.clone(),
            callback,
            secret,
            host_nonce,
            callback_origin,
            initialize,
        );
        if result.is_err() {
            process.stop();
        }
        result
    }

    fn open(
        process: Arc<ProcessState>,
        callback: CallbackServer,
        secret: [u8; 32],
        host_nonce: [u8; 32],
        callback_origin: String,
        initialize: InitializeParams,
    ) -> Result<Self, RuntimeFailure> {
        let bootstrap = Bootstrap {
            callback_origin: callback_origin.clone(),
            bootstrap_secret: URL_SAFE_NO_PAD.encode(secret),
        };
        let wire = serde_json::to_vec(&bootstrap).map_err(internal_encode)?;
        if wire.len() + 1 > BOOTSTRAP_MAX_BYTES {
            return Err(protocol_failure_detail(
                "Bun Authoring bootstrap exceeds its limit",
            ));
        }
        let mut stdin = process.take_stdin()?;
        stdin.write_all(&wire).map_err(child_io)?;
        stdin.write_all(b"\n").map_err(child_io)?;
        stdin.flush().map_err(child_io)?;
        drop(stdin);

        let readiness = read_readiness(&process)?;
        if readiness.len() > BOOTSTRAP_MAX_BYTES {
            return Err(protocol_failure_detail(
                "Bun Authoring readiness exceeds its limit",
            ));
        }
        let readiness: Readiness = serde_json::from_str(readiness.trim_end()).map_err(|error| {
            protocol_failure_detail(format!("invalid Bun Authoring readiness: {error}"))
        })?;
        readiness.validate()?;
        let client = build_json_rpc_client(
            SocketAddr::from(([127, 0, 0, 1], readiness.port)),
            usize::try_from(initialize.limits.max_frame_bytes).unwrap_or(usize::MAX),
            usize::try_from(initialize.limits.max_queued_calls).unwrap_or(usize::MAX),
        )?;
        let payload = authoring_handshake_proof_payload(AuthoringHandshakeProofInput {
            initialize: &initialize,
            callback_origin: &callback_origin,
            host_nonce: &URL_SAFE_NO_PAD.encode(host_nonce),
        })
        .map_err(protocol_failure)?;
        let digest: [u8; 32] = Sha256::digest(payload).into();
        let request = InitializeRequest {
            initialize: initialize.clone(),
            callback_origin,
            host_nonce: URL_SAFE_NO_PAD.encode(host_nonce),
            host_proof: proof(&secret, &authoring_host_proof_message(&digest)),
        };
        let response: InitializedResponse = rpc(&client, "lenso.initialize", request)?;
        initialize
            .validate_initialized(&response.initialized)
            .map_err(protocol_failure)?;
        verify_proof(
            &secret,
            &authoring_child_proof_message(&digest, &response.child_nonce)
                .map_err(protocol_failure)?,
            &response.child_proof,
        )?;
        Ok(Self {
            process,
            client,
            callback,
            initialize,
            stopped: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub fn construct(&self, params: ConstructParams) -> Result<ConstructedResult, RuntimeFailure> {
        params
            .validate_for(&self.initialize.identity)
            .map_err(protocol_failure)?;
        rpc(&self.client, "lenso.construct", params)
    }

    pub fn invoke(&self, params: &InvokeParams) -> Result<InvocationResult, RuntimeFailure> {
        params
            .validate_against(&self.initialize)
            .map_err(protocol_failure)?;
        let result: InvocationResult = rpc(&self.client, "lenso.invoke", params)?;
        result.validate_for(params).map_err(protocol_failure)?;
        Ok(result)
    }

    pub fn cancel(&self, params: CancelParams) -> Result<CancelAck, RuntimeFailure> {
        params
            .validate_for(&self.initialize.identity)
            .map_err(protocol_failure)?;
        rpc(&self.client, "lenso.cancel", params)
    }

    pub fn stop(&self, params: StopParams) -> Result<StoppedResult, RuntimeFailure> {
        params
            .validate_for(&self.initialize.identity)
            .map_err(protocol_failure)?;
        let result = rpc(&self.client, "lenso.stop", params);
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
        self.terminate();
        result
    }

    /// Terminates and reaps a child when graceful settlement cannot be established.
    pub fn terminate(&self) {
        self.callback.shutdown();
        self.process.stop();
    }
}

impl Drop for BunAuthoringHost {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[derive(Serialize)]
struct Bootstrap {
    callback_origin: String,
    bootstrap_secret: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Readiness {
    protocol: String,
    port: u16,
}

impl Readiness {
    fn validate(&self) -> Result<(), RuntimeFailure> {
        if self.protocol != BUN_AUTHORING_RUNTIME_PROFILE || self.port == 0 {
            return Err(protocol_failure_detail(
                "invalid Bun Authoring readiness identity",
            ));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct InitializeRequest {
    initialize: InitializeParams,
    callback_origin: String,
    host_nonce: String,
    host_proof: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InitializedResponse {
    initialized: InitializeParams,
    child_nonce: String,
    child_proof: String,
}

#[derive(Debug)]
struct CallbackState {
    secret: [u8; 32],
    session: String,
    max_frame_bytes: usize,
    handler: Arc<dyn BunAuthoringCallback>,
}

#[derive(Deserialize)]
struct RpcRequest {
    jsonrpc: String,
    id: Value,
    method: String,
    params: Value,
}

#[derive(Serialize)]
struct RpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Serialize)]
struct RpcError {
    code: i32,
    message: String,
}

struct CallbackServer {
    address: SocketAddr,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

impl CallbackServer {
    fn start(
        secret: [u8; 32],
        session: String,
        max_frame_bytes: u64,
        handler: impl BunAuthoringCallback,
    ) -> Result<Self, RuntimeFailure> {
        let state = Arc::new(CallbackState {
            secret,
            session,
            max_frame_bytes: usize::try_from(max_frame_bytes).unwrap_or(usize::MAX),
            handler: Arc::new(handler),
        });
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let startup_result_tx = started_tx.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let worker = thread::Builder::new()
            .name("lenso-bun-authoring-callback".to_owned())
            .spawn(move || {
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| error.to_string())
                    .and_then(|runtime| {
                        runtime.block_on(async move {
                            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                                .await
                                .map_err(|error| error.to_string())?;
                            let address =
                                listener.local_addr().map_err(|error| error.to_string())?;
                            let app = Router::new()
                                .route("/", post(handle_callback))
                                .layer(DefaultBodyLimit::max(
                                    usize::try_from(max_frame_bytes).unwrap_or(usize::MAX),
                                ))
                                .with_state(state);
                            startup_result_tx
                                .send(Ok(address))
                                .map_err(|error| error.to_string())?;
                            axum::serve(listener, app)
                                .with_graceful_shutdown(async {
                                    let _ = shutdown_rx.await;
                                })
                                .await
                                .map_err(|error| error.to_string())
                        })
                    });
                if let Err(error) = result {
                    let _ = started_tx.send(Err(error));
                }
            })
            .map_err(|error| RuntimeFailure::Internal {
                detail: format!("failed to start Bun Authoring callback listener: {error}"),
            })?;
        let address = started_rx
            .recv_timeout(STARTUP_TIMEOUT)
            .map_err(|_| protocol_failure_detail("Bun Authoring callback startup timed out"))?
            .map_err(protocol_failure_detail)?;
        Ok(Self {
            address,
            shutdown: Mutex::new(Some(shutdown_tx)),
            worker: Mutex::new(Some(worker)),
        })
    }

    fn shutdown(&self) {
        if let Ok(mut shutdown) = self.shutdown.lock()
            && let Some(shutdown) = shutdown.take()
        {
            let _ = shutdown.send(());
        }
        if let Ok(mut worker) = self.worker.lock()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

async fn handle_callback(
    State(state): State<Arc<CallbackState>>,
    headers: HeaderMap,
    Json(request): Json<RpcRequest>,
) -> (StatusCode, Json<RpcResponse>) {
    let result = dispatch_callback(&state, &headers, &request);
    match result {
        Ok(result) => (
            StatusCode::OK,
            Json(RpcResponse {
                jsonrpc: "2.0",
                id: request.id,
                result: Some(result),
                error: None,
            }),
        ),
        Err(error) => (
            StatusCode::UNAUTHORIZED,
            Json(RpcResponse {
                jsonrpc: "2.0",
                id: request.id,
                result: None,
                error: Some(RpcError {
                    code: -32602,
                    message: error,
                }),
            }),
        ),
    }
}

fn dispatch_callback(
    state: &CallbackState,
    headers: &HeaderMap,
    request: &RpcRequest,
) -> Result<Value, String> {
    if request.jsonrpc != "2.0" {
        return Err("invalid JSON-RPC version".to_owned());
    }
    let method = match request.method.as_str() {
        "lenso.call" | "lenso.settled" => request.method.as_str(),
        _ => return Err("unsupported Bun Authoring callback method".to_owned()),
    };
    let received = headers
        .get(BUN_AUTHORING_CALLBACK_PROOF_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "missing Bun Authoring callback proof".to_owned())?;
    let message = authoring_callback_proof_message(&state.session, method, &request.params)
        .map_err(|error| error.to_string())?;
    verify_proof(&state.secret, &message, received).map_err(|error| format!("{error:?}"))?;
    match method {
        "lenso.call" => {
            let params: OutboundCallParams = serde_json::from_value(request.params.clone())
                .map_err(|error| error.to_string())?;
            if params.session != state.session {
                return Err("callback session mismatch".to_owned());
            }
            let result = serde_json::to_value(state.handler.call(params).map_err(runtime_detail)?)
                .map_err(|error| error.to_string())?;
            if serde_json::to_vec(&result)
                .map_err(|error| error.to_string())?
                .len()
                > state.max_frame_bytes
            {
                return Err("Bun Authoring callback result exceeds max_frame_bytes".to_owned());
            }
            Ok(result)
        }
        "lenso.settled" => {
            let settlement: Settlement = serde_json::from_value(request.params.clone())
                .map_err(|error| error.to_string())?;
            if settlement.session != state.session {
                return Err("callback session mismatch".to_owned());
            }
            state.handler.settled(settlement).map_err(runtime_detail)?;
            Ok(serde_json::json!({}))
        }
        _ => unreachable!(),
    }
}

fn rpc<T: DeserializeOwned>(
    client: &HttpClient,
    method: &'static str,
    params: impl Serialize,
) -> Result<T, RuntimeFailure> {
    json_rpc_runtime()?
        .block_on(client.request(method, rpc_params![params]))
        .map_err(|error| RuntimeFailure::PluginFailure {
            detail: format!("Bun Authoring RPC {method} failed: {error}"),
        })
}

fn random_32() -> Result<[u8; 32], RuntimeFailure> {
    let mut value = [0_u8; 32];
    getrandom::fill(&mut value).map_err(|error| RuntimeFailure::Internal {
        detail: format!("failed to create Bun Authoring secret: {error}"),
    })?;
    Ok(value)
}

fn read_readiness(process: &Arc<ProcessState>) -> Result<String, RuntimeFailure> {
    let stdout = process.take_stdout()?;
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("lenso-bun-authoring-readiness".to_owned())
        .spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut readiness = String::new();
            let result = reader.read_line(&mut readiness).map(|_| readiness);
            let _ = sender.send(result);
        })
        .map_err(|error| RuntimeFailure::Internal {
            detail: format!("failed to read Bun Authoring readiness: {error}"),
        })?;
    receiver
        .recv_timeout(STARTUP_TIMEOUT)
        .map_err(|_| protocol_failure_detail("Bun Authoring readiness timed out"))?
        .map_err(child_io)
}

fn proof(secret: &[u8; 32], message: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(message);
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

fn verify_proof(secret: &[u8; 32], message: &[u8], candidate: &str) -> Result<(), RuntimeFailure> {
    let candidate = URL_SAFE_NO_PAD
        .decode(candidate)
        .map_err(|_| protocol_failure_detail("invalid Bun Authoring proof encoding"))?;
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(message);
    mac.verify_slice(&candidate)
        .map_err(|_| protocol_failure_detail("Bun Authoring proof did not authenticate"))
}

fn protocol_failure(error: lenso_process_protocol::ProtocolError) -> RuntimeFailure {
    let detail = error.to_string();
    drop(error);
    protocol_failure_detail(detail)
}

fn protocol_failure_detail(detail: impl Into<String>) -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: detail.into(),
    }
}

fn internal_encode(error: serde_json::Error) -> RuntimeFailure {
    let detail = error.to_string();
    drop(error);
    RuntimeFailure::Internal {
        detail: format!("failed to encode Bun Authoring bootstrap: {detail}"),
    }
}

fn child_io(error: std::io::Error) -> RuntimeFailure {
    let detail = error.to_string();
    drop(error);
    RuntimeFailure::PluginFailure {
        detail: format!("Bun Authoring private channel failed: {detail}"),
    }
}

fn runtime_detail(error: RuntimeFailure) -> String {
    let detail = format!("{error:?}");
    drop(error);
    detail
}

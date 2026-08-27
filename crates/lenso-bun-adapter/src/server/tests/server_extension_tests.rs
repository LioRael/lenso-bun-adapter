use std::sync::{Arc, Mutex};

use lenso_kernel::{CancellationToken, InvocationContext, SealedInvocationExtension};
use lenso_otel_plugin::{TraceContext, TraceContextPropagator};

use super::*;

#[derive(Debug)]
struct ExtensionHandler {
    seen: Arc<Mutex<Vec<BunInvocationExtension>>>,
}

impl BunProviderHandler for ExtensionHandler {
    fn invoke(&self, request: BunRequest) -> BunResponse {
        self.seen
            .lock()
            .expect("extension capture lock")
            .extend(request.extensions.iter().cloned());
        BunResponse::Success(request.payload)
    }
}

#[test]
fn provider_preserves_sealed_extensions_and_rejects_duplicate_keys() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let server = BunProviderServer::json_rpc(
        BunProviderDescriptor {
            capability_id: "example.secure-greeting@1",
            descriptor_version: "1.0.0".to_owned(),
            operations: vec!["greet".to_owned()],
            stream_operations: Vec::new(),
            event_operations: Vec::new(),
            event_bindings: Vec::new(),
        },
        4096,
        1,
        ExtensionHandler { seen: seen.clone() },
    )
    .expect("extension server should start");
    let client = HttpClientBuilder::default()
        .build(format!("http://{}", server.address()))
        .expect("client should build");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime should build");
    let expected = handshake_for(
        [EndpointDescriptor {
            capability_id: "example.secure-greeting@1".to_owned(),
            descriptor_version: "1.0.0".to_owned(),
            operations: vec!["greet".to_owned()],
            stream_operations: Vec::new(),
            event_operations: Vec::new(),
        }],
        4096,
    );
    let accepted: HandshakeAck = runtime
        .block_on(client.request("lenso.handshake", rpc_params![expected]))
        .expect("exact handshake should pass");
    let session = accepted.session.expect("session should be assigned");
    let assertion = BunInvocationExtension {
        key: "lenso.auth.actor-assertion".to_owned(),
        value: br#"{"subject":"user-123"}"#.to_vec(),
        issuer: Some("auth.users".to_owned()),
        audience: vec!["example.secure-greeting@1:greet".to_owned()],
        proof: Some("signed-proof".to_owned()),
        sealed: true,
    };
    let accepted_outcome: WireOutcome = runtime
        .block_on(client.request(
            "lenso.request",
            rpc_params![WireRequest {
                request_id: 1,
                capability_id: "example.secure-greeting@1".to_owned(),
                operation: "greet".to_owned(),
                deadline_nanos: None,
                caller_instance: Some("ingress".to_owned()),
                session: Some(session.clone()),
                extensions: vec![assertion.clone()],
                payload: serde_json::json!({"name": "Ada"}),
            }],
        ))
        .expect("sealed extension request should complete");
    assert!(matches!(accepted_outcome, WireOutcome::Success { .. }));
    assert_eq!(
        seen.lock().expect("extension capture lock").as_slice(),
        &[assertion.clone()]
    );

    let duplicate_outcome: WireOutcome = runtime
        .block_on(client.request(
            "lenso.request",
            rpc_params![WireRequest {
                request_id: 2,
                capability_id: "example.secure-greeting@1".to_owned(),
                operation: "greet".to_owned(),
                deadline_nanos: None,
                caller_instance: Some("ingress".to_owned()),
                session: Some(session),
                extensions: vec![assertion.clone(), assertion],
                payload: serde_json::json!({"name": "Ada"}),
            }],
        ))
        .expect("duplicate extensions should be classified in-band");
    server.shutdown();

    assert!(matches!(
        duplicate_outcome,
        WireOutcome::Runtime {
            failure: WireFailure::ProtocolViolation { .. }
        }
    ));
}

#[derive(Debug)]
struct TraceContextHandler {
    propagator: TraceContextPropagator,
    seen: Arc<Mutex<Vec<TraceContext>>>,
}

impl BunProviderHandler for TraceContextHandler {
    fn invoke(&self, request: BunRequest) -> BunResponse {
        let BunRequest {
            request_id,
            capability_id,
            operation,
            payload,
            extensions,
            ..
        } = request;
        if let Some(extension) = extensions
            .into_iter()
            .find(|extension| extension.key == lenso_otel_plugin::TRACE_CONTEXT_EXTENSION_KEY)
        {
            let context = InvocationContext::new(request_id, None, CancellationToken::new())
                .with_sealed_extension(SealedInvocationExtension::signed(
                    extension.key,
                    extension.issuer.expect("sealed extension issuer"),
                    extension.audience,
                    extension.value,
                    extension.proof.expect("sealed extension proof"),
                ))
                .expect("the Bun provider should reconstruct the sealed extension");
            if let Ok(Some(trace)) =
                self.propagator
                    .extract_for_target(&context, &capability_id, &operation)
            {
                self.seen
                    .lock()
                    .expect("trace context capture lock")
                    .push(trace);
            }
        }
        BunResponse::Success(payload)
    }
}

#[test]
fn provider_preserves_and_verifies_otel_trace_context_from_bun_wire() {
    const CAPABILITY_ID: &str = "example.trace@1";
    const OPERATION: &str = "invoke";

    let propagator = TraceContextPropagator::new("lenso.otel", b"trace-key")
        .expect("trace-context propagator should be configured");
    let trace = TraceContext::from_traceparent(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        Some("vendor=value"),
    )
    .expect("the fixture trace context should parse");
    let context = propagator
        .inject(
            InvocationContext::new(7, None, CancellationToken::new()),
            &trace,
            [format!("{CAPABILITY_ID}:{OPERATION}")],
        )
        .expect("the trace context should be sealed for the Bun target");
    let extensions =
        crate::protocol::encode_invocation_extensions(&context, CAPABILITY_ID, OPERATION);
    assert_eq!(extensions.len(), 1);
    assert!(extensions[0].sealed);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let server = BunProviderServer::json_rpc(
        BunProviderDescriptor {
            capability_id: CAPABILITY_ID,
            descriptor_version: "1.0.0".to_owned(),
            operations: vec![OPERATION.to_owned()],
            stream_operations: Vec::new(),
            event_operations: Vec::new(),
            event_bindings: Vec::new(),
        },
        4096,
        1,
        TraceContextHandler {
            propagator,
            seen: seen.clone(),
        },
    )
    .expect("trace-context server should start");
    let client = HttpClientBuilder::default()
        .build(format!("http://{}", server.address()))
        .expect("client should build");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime should build");
    let expected = handshake_for(
        [EndpointDescriptor {
            capability_id: CAPABILITY_ID.to_owned(),
            descriptor_version: "1.0.0".to_owned(),
            operations: vec![OPERATION.to_owned()],
            stream_operations: Vec::new(),
            event_operations: Vec::new(),
        }],
        4096,
    );
    let accepted: HandshakeAck = runtime
        .block_on(client.request("lenso.handshake", rpc_params![expected]))
        .expect("exact trace-context handshake should pass");
    let session = accepted.session.expect("session should be assigned");
    let outcome: WireOutcome = runtime
        .block_on(client.request(
            "lenso.request",
            rpc_params![WireRequest {
                request_id: 7,
                capability_id: CAPABILITY_ID.to_owned(),
                operation: OPERATION.to_owned(),
                deadline_nanos: None,
                caller_instance: Some("bun-caller".to_owned()),
                session: Some(session),
                extensions,
                payload: serde_json::json!({"ok": true}),
            }],
        ))
        .expect("trace-context request should complete");
    server.shutdown();

    assert!(matches!(outcome, WireOutcome::Success { .. }));
    assert_eq!(
        seen.lock().expect("trace context capture lock").as_slice(),
        &[trace]
    );
}

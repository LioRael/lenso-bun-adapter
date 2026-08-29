use std::sync::{Arc, Mutex};

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

use lenso_process_sdk::{ProcessOutcome, ProcessPlugin};
use serde_json::{Value, json};

#[derive(Debug)]
struct Greeting;

impl ProcessPlugin for Greeting {
    fn descriptor(&self) -> Value {
        json!({
            "abi": "lenso.json-request@1",
            "capabilities": [{
                "capability_id": "example.greeting@1",
                "descriptor_version": "1.0.0",
                "request_operations": ["greet"],
            }],
        })
    }

    fn invoke(&self, capability: &str, operation: &str, request: Value) -> ProcessOutcome {
        if capability != "example.greeting@1" || operation != "greet" {
            return ProcessOutcome::DomainError(json!("not_found"));
        }
        let Some(name) = request.get("name").and_then(Value::as_str) else {
            return ProcessOutcome::Failure("Greeting request needs a name".to_owned());
        };
        if name.is_empty() {
            ProcessOutcome::DomainError(json!("empty_name"))
        } else {
            ProcessOutcome::Success(json!({ "message": format!("Hello from Rust, {name}!") }))
        }
    }
}

fn main() {
    lenso_process_sdk::serve(&Greeting).expect("serve Rust Process Plugin fixture");
}

use std::{
    io::{Read, Write},
    time::Duration,
};

use lenso_kernel::RuntimeFailure;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: u32 = 1;
pub const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024;
pub const DEFAULT_REQUEST_QUEUE_CAPACITY: usize = 32;
pub const VALUE_PROFILE: &str = "lenso-json-value-v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct EndpointDescriptor {
    pub capability_id: String,
    pub descriptor_version: String,
    pub operations: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Handshake {
    pub protocol_version: u32,
    pub value_profile: String,
    pub max_frame_bytes: usize,
    pub endpoints: Vec<EndpointDescriptor>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct HandshakeAck {
    pub accepted: bool,
    pub protocol_version: u32,
    pub value_profile: String,
    pub max_frame_bytes: usize,
    pub endpoints: Vec<EndpointDescriptor>,
    #[serde(default)]
    pub session: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WireRequest {
    pub request_id: u64,
    pub capability_id: String,
    pub operation: String,
    pub deadline_nanos: Option<u64>,
    pub caller_instance: Option<String>,
    #[serde(default)]
    pub session: Option<String>,
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum WireOutcome {
    Success { value: Value },
    Domain { value: Value },
    Runtime { failure: WireFailure },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum WireFailure {
    Unavailable,
    UnknownOperation {
        operation: String,
    },
    AmbiguousBinding {
        providers: usize,
    },
    ProtocolViolation {
        detail: Option<String>,
    },
    MissingModuleFactory {
        instance: String,
        package_id: String,
    },
    UnavailableExecutionClass {
        instance_key: String,
        execution_class: String,
    },
    InvalidResolvedPlan {
        detail: String,
    },
    AdmissionClosed,
    ResourceExhausted {
        operation: String,
    },
    DeadlineExceeded {
        request_id: u64,
    },
    Cancelled {
        request_id: u64,
    },
    Internal {
        detail: String,
    },
    ModuleFailure {
        detail: String,
    },
    ModuleRestartExhausted {
        instance: String,
        attempts: usize,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum FramedMessage {
    Handshake(Handshake),
    HandshakeAck(HandshakeAck),
    Request(WireRequest),
    Cancel {
        request_id: u64,
    },
    Response {
        request_id: u64,
        outcome: WireOutcome,
    },
    Shutdown,
}

pub(crate) fn handshake_for(
    endpoints: impl IntoIterator<Item = EndpointDescriptor>,
    max_frame_bytes: usize,
) -> Handshake {
    Handshake {
        protocol_version: PROTOCOL_VERSION,
        value_profile: VALUE_PROFILE.to_owned(),
        max_frame_bytes,
        endpoints: endpoints.into_iter().collect(),
    }
}

pub(crate) fn verify_handshake(
    expected: &Handshake,
    actual: &HandshakeAck,
    capability: &'static str,
) -> Result<(), RuntimeFailure> {
    if actual.accepted
        && actual.protocol_version == expected.protocol_version
        && actual.value_profile == expected.value_profile
        && actual.max_frame_bytes == expected.max_frame_bytes
        && actual.endpoints == expected.endpoints
    {
        return Ok(());
    }
    Err(protocol_violation(Some(capability)))
}

pub(crate) fn protocol_violation(capability: Option<&'static str>) -> RuntimeFailure {
    RuntimeFailure::ProtocolViolation {
        capability: capability.unwrap_or("lenso.bun-process@1"),
    }
}

pub(crate) fn to_wire_failure(error: &RuntimeFailure) -> WireFailure {
    match error {
        RuntimeFailure::Unavailable { .. } => WireFailure::Unavailable,
        RuntimeFailure::UnknownOperation { operation, .. } => WireFailure::UnknownOperation {
            operation: operation.clone(),
        },
        RuntimeFailure::AmbiguousBinding { providers, .. } => WireFailure::AmbiguousBinding {
            providers: *providers,
        },
        RuntimeFailure::ProtocolViolation { capability } => WireFailure::ProtocolViolation {
            detail: Some((*capability).to_owned()),
        },
        RuntimeFailure::MissingModuleFactory {
            instance,
            package_id,
        } => WireFailure::MissingModuleFactory {
            instance: instance.clone(),
            package_id: package_id.clone(),
        },
        RuntimeFailure::UnavailableExecutionClass {
            instance_key,
            execution_class,
        } => WireFailure::UnavailableExecutionClass {
            instance_key: instance_key.clone(),
            execution_class: execution_class.clone(),
        },
        RuntimeFailure::InvalidResolvedPlan { detail } => WireFailure::InvalidResolvedPlan {
            detail: detail.clone(),
        },
        RuntimeFailure::AdmissionClosed => WireFailure::AdmissionClosed,
        RuntimeFailure::ResourceExhausted { operation, .. } => WireFailure::ResourceExhausted {
            operation: operation.clone(),
        },
        RuntimeFailure::DeadlineExceeded { request_id } => WireFailure::DeadlineExceeded {
            request_id: *request_id,
        },
        RuntimeFailure::Cancelled { request_id } => WireFailure::Cancelled {
            request_id: *request_id,
        },
        RuntimeFailure::Internal { detail } => WireFailure::Internal {
            detail: detail.clone(),
        },
        RuntimeFailure::ModuleFailure { detail } => WireFailure::ModuleFailure {
            detail: detail.clone(),
        },
        RuntimeFailure::ModuleRestartExhausted { instance, attempts } => {
            WireFailure::ModuleRestartExhausted {
                instance: instance.clone(),
                attempts: *attempts,
            }
        }
    }
}

pub(crate) fn from_wire_failure(capability: &'static str, failure: WireFailure) -> RuntimeFailure {
    match failure {
        WireFailure::Unavailable => RuntimeFailure::Unavailable { capability },
        WireFailure::UnknownOperation { operation } => RuntimeFailure::UnknownOperation {
            capability,
            operation,
        },
        WireFailure::AmbiguousBinding { providers } => RuntimeFailure::AmbiguousBinding {
            capability,
            providers,
        },
        WireFailure::ProtocolViolation { .. } => RuntimeFailure::ProtocolViolation { capability },
        WireFailure::MissingModuleFactory {
            instance,
            package_id,
        } => RuntimeFailure::MissingModuleFactory {
            instance,
            package_id,
        },
        WireFailure::UnavailableExecutionClass {
            instance_key,
            execution_class,
        } => RuntimeFailure::UnavailableExecutionClass {
            instance_key,
            execution_class,
        },
        WireFailure::InvalidResolvedPlan { detail } => {
            RuntimeFailure::InvalidResolvedPlan { detail }
        }
        WireFailure::AdmissionClosed => RuntimeFailure::AdmissionClosed,
        WireFailure::ResourceExhausted { operation } => RuntimeFailure::ResourceExhausted {
            capability,
            operation,
        },
        WireFailure::DeadlineExceeded { request_id } => {
            RuntimeFailure::DeadlineExceeded { request_id }
        }
        WireFailure::Cancelled { request_id } => RuntimeFailure::Cancelled { request_id },
        WireFailure::Internal { detail } => RuntimeFailure::Internal { detail },
        WireFailure::ModuleFailure { detail } => RuntimeFailure::ModuleFailure { detail },
        WireFailure::ModuleRestartExhausted { instance, attempts } => {
            RuntimeFailure::ModuleRestartExhausted { instance, attempts }
        }
    }
}

pub(crate) fn encode_frame(
    message: &FramedMessage,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, RuntimeFailure> {
    let payload = serde_json::to_vec(message).map_err(|error| RuntimeFailure::Internal {
        detail: format!("failed to encode Bun frame: {error}"),
    })?;
    let length = u32::try_from(payload.len()).map_err(|_| protocol_violation(None))?;
    if payload.len() > max_frame_bytes {
        return Err(protocol_violation(None));
    }
    let mut frame = Vec::with_capacity(payload.len().saturating_add(4));
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub(crate) fn write_frame<W: Write>(
    writer: &mut W,
    message: &FramedMessage,
    max_frame_bytes: usize,
) -> Result<(), RuntimeFailure> {
    writer
        .write_all(&encode_frame(message, max_frame_bytes)?)
        .map_err(|error| RuntimeFailure::ModuleFailure {
            detail: format!("Bun framed-stdio write failed: {error}"),
        })?;
    writer
        .flush()
        .map_err(|error| RuntimeFailure::ModuleFailure {
            detail: format!("Bun framed-stdio flush failed: {error}"),
        })
}

pub(crate) fn read_frame<R: Read>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<FramedMessage, RuntimeFailure> {
    let mut length = [0_u8; 4];
    reader
        .read_exact(&mut length)
        .map_err(|_| RuntimeFailure::ProtocolViolation {
            capability: "lenso.bun-process@1",
        })?;
    let length = usize::try_from(u32::from_be_bytes(length)).unwrap_or(usize::MAX);
    if length > max_frame_bytes {
        return Err(protocol_violation(None));
    }
    let mut payload = vec![0_u8; length];
    reader
        .read_exact(&mut payload)
        .map_err(|_| protocol_violation(None))?;
    serde_json::from_slice(&payload).map_err(|_| protocol_violation(None))
}

pub(crate) fn deadline_nanos(deadline: Option<Duration>) -> Option<u64> {
    deadline
        .map(|value| u64::try_from(value.as_nanos().min(u128::from(u64::MAX))).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn greeting_handshake(max_frame_bytes: usize) -> Handshake {
        handshake_for(
            [EndpointDescriptor {
                capability_id: "example.greeting@1".to_owned(),
                descriptor_version: "1.0.0".to_owned(),
                operations: vec!["greet".to_owned()],
            }],
            max_frame_bytes,
        )
    }

    #[test]
    fn framed_messages_are_length_bounded() {
        let message = FramedMessage::Handshake(greeting_handshake(32));
        let error = encode_frame(&message, 8).expect_err("the handshake should exceed the limit");
        assert!(matches!(error, RuntimeFailure::ProtocolViolation { .. }));
    }

    #[test]
    fn handshake_requires_exact_protocol_and_endpoint_table() {
        let expected = greeting_handshake(128);
        let accepted = HandshakeAck {
            accepted: true,
            protocol_version: PROTOCOL_VERSION,
            value_profile: VALUE_PROFILE.to_owned(),
            max_frame_bytes: 128,
            endpoints: expected.endpoints.clone(),
            session: Some("test-session".to_owned()),
        };
        verify_handshake(&expected, &accepted, "example.greeting@1")
            .expect("the exact handshake should pass");

        let mut rejected = accepted;
        rejected.endpoints[0].operations.push("later".to_owned());
        assert!(matches!(
            verify_handshake(&expected, &rejected, "example.greeting@1"),
            Err(RuntimeFailure::ProtocolViolation { .. })
        ));
    }

    #[test]
    fn wire_outcomes_preserve_the_json_rpc_result_shape() {
        let result = serde_json::to_value(WireOutcome::Success {
            value: serde_json::json!({"message": "Hello"}),
        })
        .expect("wire outcome should encode");
        assert_eq!(result["kind"], "success");
        assert_eq!(result["value"]["message"], "Hello");
    }

    #[test]
    fn every_runtime_failure_has_a_wire_round_trip() {
        let failures = [
            RuntimeFailure::Unavailable {
                capability: "example.greeting@1",
            },
            RuntimeFailure::UnknownOperation {
                capability: "example.greeting@1",
                operation: "missing".to_owned(),
            },
            RuntimeFailure::AmbiguousBinding {
                capability: "example.greeting@1",
                providers: 2,
            },
            RuntimeFailure::ProtocolViolation {
                capability: "example.greeting@1",
            },
            RuntimeFailure::MissingModuleFactory {
                instance: "provider".to_owned(),
                package_id: "package".to_owned(),
            },
            RuntimeFailure::UnavailableExecutionClass {
                instance_key: "provider".to_owned(),
                execution_class: "lenso.bun-process@1".to_owned(),
            },
            RuntimeFailure::InvalidResolvedPlan {
                detail: "invalid".to_owned(),
            },
            RuntimeFailure::AdmissionClosed,
            RuntimeFailure::ResourceExhausted {
                capability: "example.greeting@1",
                operation: "greet".to_owned(),
            },
            RuntimeFailure::DeadlineExceeded { request_id: 7 },
            RuntimeFailure::Cancelled { request_id: 8 },
            RuntimeFailure::Internal {
                detail: "internal".to_owned(),
            },
            RuntimeFailure::ModuleFailure {
                detail: "failed".to_owned(),
            },
            RuntimeFailure::ModuleRestartExhausted {
                instance: "provider".to_owned(),
                attempts: 3,
            },
        ];
        for failure in failures {
            assert_eq!(
                from_wire_failure("example.greeting@1", to_wire_failure(&failure)),
                failure
            );
        }
    }
}

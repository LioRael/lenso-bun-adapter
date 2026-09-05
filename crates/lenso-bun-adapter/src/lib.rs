//! Bun child-process Execution Adapters for portable request, stream, and Event Capabilities.
//!
//! The Kernel only receives the existing typed endpoint and lifecycle seams.
//! Process topology, framing, JSON-RPC, bounded queues, and generated-value
//! codecs live in this crate so that a wire choice cannot become a Kernel
//! contract.

mod adapter;
mod host_imports;
mod authoring_v2;
mod protocol;
mod request;
mod server;
mod transport;

pub use adapter::{BunAdapter, BunAdapterConfig, BunCapabilityCodec, BunWire};
pub use authoring_v2::{
    BUN_AUTHORING_CALLBACK_PROOF_HEADER, BUN_AUTHORING_RUNTIME_PROFILE, BunAuthoringCallback,
    BunAuthoringHost,
};
pub use protocol::{
    BunInvocationExtension, DEFAULT_MAX_FRAME_BYTES, DEFAULT_REQUEST_QUEUE_CAPACITY,
    PROTOCOL_VERSION,
};
pub use server::{
    BunEventAction, BunEventBinding, BunProviderDescriptor, BunProviderHandler, BunProviderServer,
    BunProviderStream, BunRequest, BunResponse, BunStreamAction, BunStreamEvent,
    BunStreamOpenResponse, BunStreamReceive,
};

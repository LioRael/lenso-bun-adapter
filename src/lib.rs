//! Bun child-process Execution Adapters for portable request Capabilities.
//!
//! The Kernel only receives the existing typed endpoint and lifecycle seams.
//! Process topology, framing, JSON-RPC, bounded queues, and generated-value
//! codecs live in this crate so that a wire choice cannot become a Kernel
//! contract.

mod adapter;
mod protocol;
mod server;
mod transport;

pub use adapter::{BunAdapter, BunAdapterConfig, BunCapabilityCodec, BunWire};
pub use protocol::{DEFAULT_MAX_FRAME_BYTES, DEFAULT_REQUEST_QUEUE_CAPACITY, PROTOCOL_VERSION};
pub use server::{
    BunProviderDescriptor, BunProviderHandler, BunProviderServer, BunProviderStream, BunRequest,
    BunResponse, BunStreamAction, BunStreamEvent, BunStreamOpenResponse, BunStreamReceive,
};

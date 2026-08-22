use std::sync::{Arc, atomic::AtomicBool};

use crate::{
    protocol::{WireEventPublish, WireRequest, WireStreamOpen},
    server::BunRequest,
};

pub(crate) trait IntoBunRequest {
    fn into_bun_request(self, cancellation: Arc<AtomicBool>) -> BunRequest;
}

macro_rules! impl_into_bun_request {
    ($wire:ty) => {
        impl IntoBunRequest for $wire {
            fn into_bun_request(self, cancellation: Arc<AtomicBool>) -> BunRequest {
                BunRequest {
                    request_id: self.request_id,
                    capability_id: self.capability_id,
                    operation: self.operation,
                    deadline_nanos: self.deadline_nanos,
                    caller_instance: self.caller_instance,
                    payload: self.payload,
                    extensions: self.extensions,
                    cancellation,
                }
            }
        }
    };
}

impl_into_bun_request!(WireRequest);
impl_into_bun_request!(WireEventPublish);
impl_into_bun_request!(WireStreamOpen);

impl BunRequest {
    pub(crate) fn from_wire<W: IntoBunRequest>(wire: W, cancellation: Arc<AtomicBool>) -> Self {
        wire.into_bun_request(cancellation)
    }
}

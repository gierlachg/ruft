use std::net::SocketAddr;

use bytes::Bytes;
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::relay::protocol::Operation::MapStoreOperation;
use crate::relay::protocol::Request::ReplicateRequest;
use crate::relay::Position;
use crate::Payload;

const REPLICATE_REQUEST_ID: u8 = 1;
const REPLICATE_SUCCESS_RESPONSE_ID: u8 = 2;
const REPLICATE_REDIRECT_RESPONSE_ID: u8 = 3;

const MAP_STORE_OPERATION_ID: u16 = 1;

#[derive(Display, Serialize)]
#[repr(u8)]
pub(crate) enum Request {
    #[display(fmt = "ReplicateRequest {{ position: {:?} }}", position)]
    ReplicateRequest {
        payload: Payload,
        position: Option<Position>,
    } = REPLICATE_REQUEST_ID, // TODO: arbitrary_enum_discriminant not used
}

impl Request {
    pub(crate) fn replicate(payload: Payload, position: Option<Position>) -> Self {
        ReplicateRequest { payload, position }
    }

    pub(crate) fn with_position(self, position: Option<Position>) -> Self {
        match self {
            ReplicateRequest { payload, position: _ } => ReplicateRequest { payload, position },
        }
    }
}

impl Into<Bytes> for &Request {
    fn into(self) -> Bytes {
        Bytes::from(bincode::serialize(self).expect("Unable to serialize"))
    }
}

#[derive(Display, Deserialize)]
#[repr(u8)]
pub(crate) enum Response {
    #[display(fmt = "ReplicateSuccessResponse {{ }}")]
    ReplicateSuccessResponse {} = REPLICATE_SUCCESS_RESPONSE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(
        fmt = "ReplicateRedirectResponse {{ leader_address: {:?}, position: {:?} }}",
        leader_address,
        position
    )]
    ReplicateRedirectResponse {
        leader_address: Option<SocketAddr>,
        position: Option<Position>,
    } = REPLICATE_REDIRECT_RESPONSE_ID, // TODO: arbitrary_enum_discriminant not used
}

impl TryFrom<Bytes> for Response {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes.as_ref()).map_err(|e| e.into())
    }
}

#[derive(Display, Serialize)]
#[repr(u16)]
pub(crate) enum Operation {
    #[display(fmt = "MapStoreOperation {{ }}")]
    MapStoreOperation {
        // TODO: id, key, value...
        payload: Payload,
    } = MAP_STORE_OPERATION_ID, // TODO: arbitrary_enum_discriminant not used
}

impl Operation {
    pub(crate) fn map_store(payload: Payload) -> Self {
        MapStoreOperation { payload }
    }
}

impl Into<Payload> for Operation {
    fn into(self) -> Payload {
        Payload::from(bincode::serialize(&self).expect("Unable to serialize"))
    }
}

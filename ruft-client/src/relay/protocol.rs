use std::net::SocketAddr;

use bytes::Bytes;
use derive_more::Display;
use serde::{Deserialize, Serialize, Serializer};

use crate::relay::protocol::Operation::MapStoreOperation;
use crate::relay::protocol::Request::ReplicateRequest;
use crate::relay::Position;

const REPLICATE_REQUEST_ID: u8 = 1;
const REPLICATE_SUCCESS_RESPONSE_ID: u8 = 2;
const REPLICATE_REDIRECT_RESPONSE_ID: u8 = 3;

const MAP_STORE_OPERATION_ID: u16 = 1;

#[derive(Display, Serialize)]
#[repr(u8)]
pub(crate) enum Request {
    #[display(fmt = "ReplicateRequest {{ payload: {:?}, position: {:?} }}", payload, position)]
    ReplicateRequest {
        payload: Payload,
        position: Option<Position>,
    } = REPLICATE_REQUEST_ID, // TODO: arbitrary_enum_discriminant not used
}

impl Request {
    pub(crate) fn replicate(payload: Bytes, position: Option<Position>) -> Self {
        ReplicateRequest {
            payload: Payload(payload),
            position,
        }
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

// TODO: move it up ???
#[derive(Display, Serialize)]
#[repr(u16)]
pub(crate) enum Operation<'a> {
    _NoOperation, // TODO: should not be needed... arbitrary_enum_discriminant
    #[display(fmt = "MapStoreOperation {{ id: {}, key: {:?}, value: {:?} }}", id, key, value)]
    MapStoreOperation {
        id: &'a str,
        key: Payload,
        value: Payload,
    } = MAP_STORE_OPERATION_ID, // TODO: arbitrary_enum_discriminant not used
}

impl<'a> Operation<'a> {
    pub(crate) fn map_store(id: &'a str, key: Bytes, value: Bytes) -> Self {
        MapStoreOperation {
            id,
            key: Payload(key),
            value: Payload(value),
        }
    }
}

impl<'a> Into<Bytes> for Operation<'a> {
    fn into(self) -> Bytes {
        Bytes::from(bincode::serialize(&self).expect("Unable to serialize"))
    }
}

#[derive(Debug)]
pub(crate) struct Payload(Bytes);

impl Serialize for Payload {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.0.as_ref())
    }
}

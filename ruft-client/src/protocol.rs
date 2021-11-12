use std::net::SocketAddr;

use bytes::Bytes;
use derive_more::Display;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::protocol::Operation::{MapReadOperation, MapWriteOperation};
use crate::protocol::Request::{Read, Write};
use crate::relay::Position;

const WRITE_REQUEST_ID: u8 = 1;
const READ_REQUEST_ID: u8 = 2;

const SUCCESS_RESPONSE_ID: u8 = 101;
const REDIRECT_RESPONSE_ID: u8 = 102;

const MAP_WRITE_OPERATION_ID: u8 = 1;
const MAP_READ_OPERATION_ID: u8 = 2;

#[derive(Display, Serialize)]
#[repr(u8)]
pub(crate) enum Request {
    #[display(fmt = "Write {{ payload: {:?}, position: {:?} }}", payload, position)]
    Write {
        payload: Payload,
        position: Option<Position>,
    } = WRITE_REQUEST_ID, // TODO: arbitrary_enum_discriminant not used
    #[display(fmt = "Read {{ }}")]
    Read { payload: Payload } = READ_REQUEST_ID, // TODO: arbitrary_enum_discriminant not used
}

impl Request {
    pub(crate) fn write(payload: Bytes, position: Option<Position>) -> Self {
        Write {
            payload: Payload(payload),
            position,
        }
    }

    pub(crate) fn read(payload: Bytes) -> Self {
        Read {
            payload: Payload(payload),
        }
    }

    pub(crate) fn with_position(self, position: Option<Position>) -> Self {
        match self {
            Write { payload, position: _ } => Write { payload, position },
            Read { payload } => {
                assert!(position.is_none());
                Read { payload }
            }
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
    // TODO: separate response set for writes and reads (empty & Option) ???
    #[display(fmt = "Success {{ }}")]
    Success { payload: Option<Payload> } = SUCCESS_RESPONSE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(
        fmt = "Redirect {{ leader_address: {:?}, position: {:?} }}",
        leader_address,
        position
    )]
    Redirect {
        leader_address: Option<SocketAddr>,
        position: Option<Position>,
    } = REDIRECT_RESPONSE_ID, // TODO: arbitrary_enum_discriminant not used
}

impl TryFrom<Bytes> for Response {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes.as_ref()).map_err(|e| e.into())
    }
}

#[derive(Display, Serialize)]
#[repr(u8)]
pub(crate) enum Operation<'a> {
    _NoOperation, // TODO: should not be needed... arbitrary_enum_discriminant
    #[display(fmt = "MapWriteOperation {{ id: {}, key: {:?}, value: {:?} }}", id, key, value)]
    MapWriteOperation {
        id: &'a str,
        key: Payload,
        value: Payload,
    } = MAP_WRITE_OPERATION_ID, // TODO: arbitrary_enum_discriminant not used
    #[display(fmt = "MapReadOperation {{ id: {}, key: {:?} }}", id, key)]
    MapReadOperation {
        id: &'a str,
        key: Payload,
    } = MAP_READ_OPERATION_ID, // TODO: arbitrary_enum_discriminant not used
}

impl<'a> Operation<'a> {
    pub(crate) fn map_write(id: &'a str, key: Bytes, value: Bytes) -> Self {
        MapWriteOperation {
            id,
            key: Payload(key),
            value: Payload(value),
        }
    }

    pub(crate) fn map_read(id: &'a str, key: Bytes) -> Self {
        MapReadOperation { id, key: Payload(key) }
    }
}

impl<'a> Into<Bytes> for Operation<'a> {
    fn into(self) -> Bytes {
        Bytes::from(bincode::serialize(&self).expect("Unable to serialize"))
    }
}

#[derive(Debug)]
pub(crate) struct Payload(Bytes);

impl Into<Vec<u8>> for Payload {
    fn into(self) -> Vec<u8> {
        self.0.to_vec() // TODO: avoid copying
    }
}

impl Serialize for Payload {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.0.as_ref())
    }
}

impl<'de> Deserialize<'de> for Payload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // TODO: &[u8] ???
        Vec::<u8>::deserialize(deserializer).map(|bytes| Payload(Bytes::from(bytes)))
    }
}

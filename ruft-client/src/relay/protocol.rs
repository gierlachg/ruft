use std::convert::TryFrom;
use std::net::SocketAddr;

use bytes::Bytes;
use derive_more::Display;
use serde::{Deserialize, Serialize, Serializer};

use crate::relay::protocol::Request::StoreRequest;

const STORE_REQUEST_ID: u16 = 1;
const STORE_SUCCESS_RESPONSE_ID: u16 = 2;
const STORE_REDIRECT_RESPONSE_ID: u16 = 3;

#[derive(Serialize, Display)]
#[repr(u16)]
pub(crate) enum Request {
    #[display(fmt = "StoreRequest {{ }}")]
    StoreRequest { payload: SerializableBytes } = STORE_REQUEST_ID, // TODO: arbitrary_enum_discriminant not used
}

impl Request {
    pub(crate) fn store_request(payload: Bytes) -> Self {
        StoreRequest {
            payload: SerializableBytes(payload),
        }
    }
}

impl Into<Bytes> for &Request {
    fn into(self) -> Bytes {
        Bytes::from(bincode::serialize(self).unwrap()) // TODO: try_into ?
    }
}

#[derive(Deserialize, Display)]
#[repr(u16)]
pub(crate) enum Response {
    #[display(fmt = "StoreSuccessResponse {{ }}")]
    StoreSuccessResponse {} = STORE_SUCCESS_RESPONSE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(fmt = "StoreRedirectResponse {{ leader_address: {:?} }}", leader_address)]
    StoreRedirectResponse { leader_address: Option<SocketAddr> } = STORE_REDIRECT_RESPONSE_ID, // TODO: arbitrary_enum_discriminant not used
}

impl TryFrom<Bytes> for Response {
    type Error = ();

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes.as_ref()).map_err(|_| ()) // TODO: error
    }
}

pub(crate) struct SerializableBytes(Bytes);

impl Serialize for SerializableBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.0.as_ref())
    }
}

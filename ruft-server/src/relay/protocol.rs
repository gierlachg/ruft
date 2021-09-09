use std::convert::TryFrom;
use std::net::SocketAddr;

use bytes::Bytes;
use derive_more::Display;
use serde::{Deserialize, Deserializer, Serialize};

use crate::relay::protocol::Response::{StoreRedirectResponse, StoreSuccessResponse};

const STORE_REQUEST_ID: u16 = 1;
const STORE_SUCCESS_RESPONSE_ID: u16 = 2;
const STORE_REDIRECT_RESPONSE_ID: u16 = 3;

#[derive(Deserialize, Display, Debug)]
#[repr(u16)]
pub(crate) enum Request {
    #[display(fmt = "StoreRequest {{ }}")]
    StoreRequest { payload: SerializableBytes } = STORE_REQUEST_ID, // TODO: arbitrary_enum_discriminant not used
}

impl TryFrom<Bytes> for Request {
    type Error = ();

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes.as_ref()).map_err(|_| ()) // TODO: error
    }
}

#[derive(Serialize, Display)]
#[repr(u16)]
pub(crate) enum Response {
    #[display(fmt = "StoreSuccessResponse {{ }}")]
    StoreSuccessResponse {} = STORE_SUCCESS_RESPONSE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(fmt = "StoreRedirectResponse {{ leader_address: {:?} }}", leader_address)]
    StoreRedirectResponse { leader_address: Option<SocketAddr> } = STORE_REDIRECT_RESPONSE_ID, // TODO: arbitrary_enum_discriminant not used
}

impl Response {
    pub(crate) fn store_success_response() -> Self {
        StoreSuccessResponse {}
    }

    pub(crate) fn store_redirect_response(leader_address: Option<SocketAddr>) -> Self {
        StoreRedirectResponse { leader_address }
    }
}

impl Into<Bytes> for Response {
    fn into(self) -> Bytes {
        Bytes::from(bincode::serialize(&self).unwrap()) // TODO: try_into ?
    }
}

#[derive(Debug)]
pub(crate) struct SerializableBytes(pub(crate) Bytes);

impl<'de> Deserialize<'de> for SerializableBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // TODO: &[u8]
        Vec::<u8>::deserialize(deserializer).map(|bytes| SerializableBytes(Bytes::from(bytes)))
    }
}

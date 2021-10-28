use std::net::SocketAddr;

use bytes::Bytes;
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::relay::protocol::Request::StoreRequest;
use crate::Payload;

const STORE_REQUEST_ID: u16 = 1;
const STORE_SUCCESS_RESPONSE_ID: u16 = 2;
const STORE_REDIRECT_RESPONSE_ID: u16 = 3;

#[derive(Display, Serialize)]
#[repr(u16)]
pub(crate) enum Request {
    #[display(fmt = "StoreRequest {{ position: {:?} }}", position)]
    StoreRequest {
        payload: Payload,
        position: Option<Position>,
    } = STORE_REQUEST_ID, // TODO: arbitrary_enum_discriminant not used
}

impl Request {
    pub(crate) fn store_request(payload: Payload, position: Option<Position>) -> Self {
        StoreRequest { payload, position }
    }

    pub(crate) fn with_position(self, position: Option<Position>) -> Self {
        match self {
            StoreRequest { payload, position: _ } => StoreRequest { payload, position },
        }
    }
}

impl Into<Bytes> for &Request {
    fn into(self) -> Bytes {
        Bytes::from(bincode::serialize(self).expect("Unable to serialize"))
    }
}

#[derive(Display, Deserialize)]
#[repr(u16)]
pub(crate) enum Response {
    #[display(fmt = "StoreSuccessResponse {{ }}")]
    StoreSuccessResponse {} = STORE_SUCCESS_RESPONSE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(
        fmt = "StoreRedirectResponse {{ leader_address: {:?}, position: {:?} }}",
        leader_address,
        position
    )]
    StoreRedirectResponse {
        leader_address: Option<SocketAddr>,
        position: Option<Position>,
    } = STORE_REDIRECT_RESPONSE_ID, // TODO: arbitrary_enum_discriminant not used
}

impl TryFrom<Bytes> for Response {
    type Error = ();

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes.as_ref()).map_err(|_| ()) // TODO: error
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Position(u64, u64);

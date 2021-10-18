use std::convert::TryFrom;
use std::net::SocketAddr;

use bytes::Bytes;
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::relay::protocol::Response::{StoreRedirectResponse, StoreSuccessResponse};
use crate::{Payload, Position};

const STORE_REQUEST_ID: u16 = 1;
const STORE_SUCCESS_RESPONSE_ID: u16 = 2;
const STORE_REDIRECT_RESPONSE_ID: u16 = 3;

#[derive(Display, Debug, Deserialize)]
#[repr(u16)]
pub(crate) enum Request {
    #[display(fmt = "StoreRequest {{ position: {:?} }}", position)]
    StoreRequest {
        payload: Payload,
        position: Option<Position>,
    } = STORE_REQUEST_ID, // TODO: arbitrary_enum_discriminant not used
}

impl TryFrom<Bytes> for Request {
    type Error = ();

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes.as_ref()).map_err(|_| ()) // TODO: error
    }
}

#[derive(Display, Serialize)]
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

impl Response {
    pub(crate) fn store_success_response() -> Self {
        StoreSuccessResponse {}
    }

    pub(crate) fn store_redirect_response(leader_address: Option<SocketAddr>, position: Option<Position>) -> Self {
        StoreRedirectResponse {
            leader_address,
            position,
        }
    }
}

impl Into<Bytes> for Response {
    fn into(self) -> Bytes {
        Bytes::from(bincode::serialize(&self).expect("Unable to serialize"))
    }
}

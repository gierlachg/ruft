use std::net::SocketAddr;

use bytes::Bytes;
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::relay::protocol::Response::{ReplicateRedirectResponse, ReplicateSuccessResponse};
use crate::{Payload, Position};

const REPLICATE_REQUEST_ID: u16 = 1;
const REPLICATE_SUCCESS_RESPONSE_ID: u16 = 2;
const REPLICATE_REDIRECT_RESPONSE_ID: u16 = 3;

#[derive(Display, Debug, Deserialize)]
#[repr(u16)]
pub(crate) enum Request {
    #[display(fmt = "ReplicateRequest {{ position: {:?} }}", position)]
    ReplicateRequest {
        payload: Payload,
        position: Option<Position>,
    } = REPLICATE_REQUEST_ID, // TODO: arbitrary_enum_discriminant not used
}

impl TryFrom<Bytes> for Request {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes.as_ref()).map_err(|e| e.into())
    }
}

#[derive(Display, Serialize)]
#[repr(u16)]
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

impl Response {
    pub(crate) fn replicate_success_response() -> Self {
        ReplicateSuccessResponse {}
    }

    pub(crate) fn replicate_redirect_response(leader_address: Option<SocketAddr>, position: Option<Position>) -> Self {
        ReplicateRedirectResponse {
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

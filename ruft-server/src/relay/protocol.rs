use std::net::SocketAddr;

use bytes::Bytes;
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::relay::protocol::Response::{Redirect, Success};
use crate::{Payload, Position};

const WRITE_REQUEST_ID: u8 = 1;
const READ_REQUEST_ID: u8 = 2;

const SUCCESS_RESPONSE_ID: u8 = 101;
const REDIRECT_RESPONSE_ID: u8 = 102;

#[derive(Display, Debug, Deserialize)]
#[repr(u8)]
pub(crate) enum Request {
    #[display(fmt = "Write {{ position: {:?} }}", position)]
    Write {
        payload: Payload,
        position: Option<Position>,
    } = WRITE_REQUEST_ID, // TODO: arbitrary_enum_discriminant not used
    #[display(fmt = "Read {{ }}")]
    Read { payload: Payload } = READ_REQUEST_ID, // TODO: arbitrary_enum_discriminant not used
}

impl TryFrom<Bytes> for Request {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes.as_ref()).map_err(|e| e.into())
    }
}

#[derive(Display, Serialize)]
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

impl Response {
    pub(crate) fn success(payload: Option<Payload>) -> Self {
        Success { payload }
    }

    pub(crate) fn redirect(leader_address: Option<SocketAddr>, position: Option<Position>) -> Self {
        Redirect {
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

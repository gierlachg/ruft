use std::net::SocketAddr;
use std::num::NonZeroU64;

use bytes::Bytes;
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::automata::protocol::Message::{AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::automata::protocol::Response::{Redirect, Success};
use crate::{Id, Payload, Position};

const APPEND_REQUEST_MESSAGE_ID: u16 = 1;
const APPEND_RESPONSE_MESSAGE_ID: u16 = 2;

const VOTE_REQUEST_MESSAGE_ID: u16 = 3;
const VOTE_RESPONSE_MESSAGE_ID: u16 = 4;

#[derive(PartialEq, Display, Debug, Serialize, Deserialize)]
#[repr(u16)]
pub(crate) enum Message {
    #[display(
        fmt = "AppendRequest {{ leader: {:?}, preceding: {:?}, term: {}, entries_term: {}, committed: {:?} }}",
        leader,
        preceding,
        term,
        entries_term,
        committed
    )]
    AppendRequest {
        leader: Id,
        term: NonZeroU64,
        preceding: Position,
        entries_term: NonZeroU64,
        entries: Vec<Payload>,
        committed: Position,
    } = APPEND_REQUEST_MESSAGE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(
        fmt = "AppendResponse {{ member: {:?}, term: {}, position: {:?} }}",
        member,
        term,
        position
    )]
    AppendResponse {
        member: Id,
        term: NonZeroU64,
        position: Result<Position, Position>,
    } = APPEND_RESPONSE_MESSAGE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(
        fmt = "VoteRequest {{ candidate: {:?}, term: {}, position: {:?} }}",
        candidate,
        term,
        position
    )]
    VoteRequest {
        candidate: Id,
        term: NonZeroU64,
        position: Position,
    } = VOTE_REQUEST_MESSAGE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(
        fmt = "VoteResponse {{ member: {:?}, term: {}, vote_granted: {} }}",
        member,
        term,
        vote_granted
    )]
    VoteResponse {
        member: Id,
        term: NonZeroU64,
        vote_granted: bool,
    } = VOTE_RESPONSE_MESSAGE_ID, // TODO: arbitrary_enum_discriminant not used
}

impl Message {
    pub(crate) fn append_request(
        leader: Id,
        term: NonZeroU64,
        preceding: Position,
        entries_term: NonZeroU64,
        entries: Vec<Payload>,
        committed: Position,
    ) -> Self {
        AppendRequest {
            leader,
            term,
            preceding,
            entries_term,
            entries,
            committed,
        }
    }

    pub(crate) fn append_response(member: Id, term: NonZeroU64, position: Result<Position, Position>) -> Self {
        AppendResponse { member, term, position }
    }

    pub(crate) fn vote_request(candidate: Id, term: NonZeroU64, position: Position) -> Self {
        VoteRequest {
            candidate,
            term,
            position,
        }
    }

    pub(crate) fn vote_response(member: Id, term: NonZeroU64, vote_granted: bool) -> Self {
        VoteResponse {
            member,
            term,
            vote_granted,
        }
    }
}

impl Into<Bytes> for Message {
    fn into(self) -> Bytes {
        Bytes::from(bincode::serialize(&self).expect("Unable to serialize"))
    }
}

impl TryFrom<Bytes> for Message {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes.as_ref()).map_err(|e| e.into())
    }
}

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

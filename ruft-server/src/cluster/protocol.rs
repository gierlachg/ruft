use std::convert::TryFrom;

use bytes::Bytes;
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::cluster::protocol::Message::{AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::{Id, Payload, Position};

const APPEND_REQUEST_MESSAGE_ID: u16 = 1;
const APPEND_RESPONSE_MESSAGE_ID: u16 = 2;
const VOTE_REQUEST_MESSAGE_ID: u16 = 3;
const VOTE_RESPONSE_MESSAGE_ID: u16 = 4;

#[derive(PartialEq, Display, Debug, Serialize, Deserialize)]
#[repr(u16)]
pub(crate) enum Message {
    #[display(
        fmt = "AppendRequest {{ leader: {}, preceding: {:?}, term: {}, entries_term: {}, committed: {:?} }}",
        leader,
        preceding,
        term,
        entries_term,
        committed
    )]
    AppendRequest {
        leader: Id,
        term: u64,
        preceding: Position,
        entries_term: u64,
        entries: Vec<Payload>,
        committed: Position,
    } = APPEND_REQUEST_MESSAGE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(
        fmt = "AppendResponse {{ member: {}, term: {}, success: {},position: {:?} }}",
        member,
        term,
        success,
        position
    )]
    AppendResponse {
        member: Id,
        term: u64,
        success: bool,
        position: Position,
    } = APPEND_RESPONSE_MESSAGE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(
        fmt = "VoteRequest {{ candidate: {}, term: {}, position: {:?} }}",
        candidate,
        term,
        position
    )]
    VoteRequest {
        candidate: Id,
        term: u64,
        position: Position,
    } = VOTE_REQUEST_MESSAGE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(
        fmt = "VoteResponse {{ member: {}, term: {}, vote_granted: {} }}",
        member,
        term,
        vote_granted
    )]
    VoteResponse { member: Id, term: u64, vote_granted: bool } = VOTE_RESPONSE_MESSAGE_ID, // TODO: arbitrary_enum_discriminant not used
}

impl Message {
    pub(crate) fn append_request(
        leader: Id,
        term: u64,
        preceding: Position,
        entries_term: u64,
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

    pub(crate) fn append_response(member: Id, term: u64, success: bool, position: Position) -> Self {
        AppendResponse {
            member,
            term,
            success,
            position,
        }
    }

    pub(crate) fn vote_request(candidate: Id, term: u64, position: Position) -> Self {
        VoteRequest {
            candidate,
            term,
            position,
        }
    }

    pub(crate) fn vote_response(member: Id, term: u64, vote_granted: bool) -> Self {
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
    type Error = ();

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes.as_ref()).map_err(|_| ()) // TODO: error
    }
}

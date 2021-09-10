use std::convert::TryFrom;

use bytes::Bytes;
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::cluster::protocol::Message::{AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::storage::Position;
use crate::{Id, Payload};

const APPEND_REQUEST_MESSAGE_ID: u16 = 1;
const APPEND_RESPONSE_MESSAGE_ID: u16 = 2;
const VOTE_REQUEST_MESSAGE_ID: u16 = 3;
const VOTE_RESPONSE_MESSAGE_ID: u16 = 4;

#[derive(PartialEq, Display, Debug, Serialize, Deserialize)]
#[repr(u16)]
pub(crate) enum Message {
    #[display(
        fmt = "AppendRequest {{ leader_id: {}, preceding position: {:?}, term: {} }}",
        leader_id,
        preceding_position,
        term
    )]
    AppendRequest {
        leader_id: Id,
        preceding_position: Position,
        term: u64,
        entries: Vec<Payload>,
    } = APPEND_REQUEST_MESSAGE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(
        fmt = "AppendResponse {{ member_id: {}, success: {}, position: {:?} }}",
        member_id,
        success,
        position
    )]
    AppendResponse {
        member_id: Id,
        success: bool,
        position: Position,
    } = APPEND_RESPONSE_MESSAGE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(
        fmt = "VoteRequest {{ candidate_id: {}, term: {}, position: {:?} }}",
        candidate_id,
        term,
        position
    )]
    VoteRequest {
        candidate_id: Id,
        term: u64,
        position: Position,
    } = VOTE_REQUEST_MESSAGE_ID, // TODO: arbitrary_enum_discriminant not used

    #[display(fmt = "VoteResponse {{ term: {}, vote_granted: {} }}", vote_granted, term)]
    VoteResponse { vote_granted: bool, term: u64 } = VOTE_RESPONSE_MESSAGE_ID, // TODO: arbitrary_enum_discriminant not used
}

impl Message {
    pub(crate) fn append_request(
        leader_id: Id,
        preceding_position: Position,
        term: u64,
        entries: Vec<Payload>,
    ) -> Self {
        // TODO committed
        AppendRequest {
            leader_id,
            preceding_position,
            term,
            entries,
        }
    }

    pub(crate) fn append_response(member_id: Id, success: bool, position: Position) -> Self {
        AppendResponse {
            member_id,
            success,
            position,
        }
    }

    pub(crate) fn vote_request(candidate_id: Id, term: u64, position: Position) -> Self {
        VoteRequest {
            candidate_id,
            term,
            position,
        }
    }

    pub(crate) fn vote_response(vote_granted: bool, term: u64) -> Self {
        VoteResponse { vote_granted, term }
    }
}

impl Into<Bytes> for Message {
    fn into(self) -> Bytes {
        Bytes::from(bincode::serialize(&self).unwrap()) // TODO: try_into ?
    }
}

impl TryFrom<Bytes> for Message {
    type Error = ();

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(bytes.as_ref()).map_err(|_| ()) // TODO: error
    }
}

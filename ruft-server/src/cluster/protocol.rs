use std::convert::TryInto;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use derive_more::Display;

use crate::cluster::protocol::Message::{AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::storage::Position;
use crate::Id;

const APPEND_REQUEST_MESSAGE_ID: u16 = 1;
const APPEND_RESPONSE_MESSAGE_ID: u16 = 2;
const VOTE_REQUEST_MESSAGE_ID: u16 = 3;
const VOTE_RESPONSE_MESSAGE_ID: u16 = 4;

// TODO: abstract Id inner type

#[derive(PartialEq, Display, Debug)]
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
        entries: Vec<Bytes>,
    },

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
    },

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
    },

    #[display(fmt = "VoteResponse {{ term: {}, vote_granted: {} }}", vote_granted, term)]
    VoteResponse { vote_granted: bool, term: u64 },
}

impl Message {
    pub(crate) fn append_request(leader_id: Id, preceding_position: Position, term: u64, entries: Vec<Bytes>) -> Self {
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
        let mut bytes = BytesMut::new();
        match self {
            AppendRequest {
                term,
                leader_id,
                preceding_position,
                entries,
            } => {
                bytes.put_u16_le(APPEND_REQUEST_MESSAGE_ID);
                bytes.put_u8(*leader_id);
                bytes.put_u64_le(preceding_position.term());
                bytes.put_u64_le(preceding_position.index());
                bytes.put_u64_le(term);
                bytes.put_u32_le(entries.len().try_into().expect("Unable to convert"));
                entries.iter().for_each(|entry| {
                    bytes.put_u32_le(entry.len().try_into().expect("Unable to convert"));
                    bytes.put(entry.as_ref());
                });
            }
            AppendResponse {
                member_id,
                success,
                position,
            } => {
                bytes.put_u16_le(APPEND_RESPONSE_MESSAGE_ID);
                bytes.put_u8(*member_id);
                bytes.put_u8(if success { 1 } else { 0 });
                bytes.put_u64_le(position.term());
                bytes.put_u64_le(position.index());
            }
            VoteRequest {
                candidate_id,
                term,
                position,
            } => {
                bytes.put_u16_le(VOTE_REQUEST_MESSAGE_ID);
                bytes.put_u8(*candidate_id);
                bytes.put_u64_le(term);
                bytes.put_u64_le(position.term());
                bytes.put_u64_le(position.index());
            }
            VoteResponse { term, vote_granted } => {
                bytes.put_u16_le(VOTE_RESPONSE_MESSAGE_ID);
                bytes.put_u8(if vote_granted { 1 } else { 0 });
                bytes.put_u64_le(term);
            }
        }
        bytes.freeze()
    }
}

// TODO: TryFrom
impl From<Bytes> for Message {
    fn from(mut bytes: Bytes) -> Self {
        let r#type = bytes.get_u16_le();
        match r#type {
            APPEND_REQUEST_MESSAGE_ID => {
                let leader_id = bytes.get_u8();
                let preceding_position = Position::of(bytes.get_u64_le(), bytes.get_u64_le());
                let term = bytes.get_u64_le();
                let entries = (0..bytes.get_u32_le())
                    .into_iter()
                    .map(|_| {
                        let len = bytes.get_u32_le().try_into().expect("Unable to convert");
                        bytes.split_to(len)
                    })
                    .collect();
                AppendRequest {
                    leader_id: Id(leader_id),
                    preceding_position,
                    term,
                    entries,
                }
            }
            APPEND_RESPONSE_MESSAGE_ID => AppendResponse {
                member_id: Id(bytes.get_u8()),
                success: bytes.get_u8() == 1,
                position: Position::of(bytes.get_u64_le(), bytes.get_u64_le()),
            },
            VOTE_REQUEST_MESSAGE_ID => VoteRequest {
                candidate_id: Id(bytes.get_u8()),
                term: bytes.get_u64_le(),
                position: Position::of(bytes.get_u64_le(), bytes.get_u64_le()),
            },
            VOTE_RESPONSE_MESSAGE_ID => VoteResponse {
                vote_granted: bytes.get_u8() == 1,
                term: bytes.get_u64_le(),
            },
            r#type => panic!("Unknown message type: {}", r#type),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_request() {
        let bytes: Bytes = AppendRequest {
            leader_id: Id(1),
            preceding_position: Position::of(12, 69),
            term: 128,
            entries: vec![Bytes::from_static(&[1]), Bytes::from_static(&[2])],
        }
        .into();
        assert_eq!(
            Message::from(bytes),
            AppendRequest {
                leader_id: Id(1),
                preceding_position: Position::of(12, 69),
                term: 128,
                entries: vec![Bytes::from_static(&[1]), Bytes::from_static(&[2])],
            }
        );
    }

    #[test]
    fn append_response() {
        let bytes: Bytes = AppendResponse {
            member_id: Id(10),
            success: true,
            position: Position::of(12, 69),
        }
        .into();
        assert_eq!(
            Message::from(bytes),
            AppendResponse {
                member_id: Id(10),
                success: true,
                position: Position::of(12, 69),
            }
        );
    }

    #[test]
    fn vote_request() {
        let bytes: Bytes = VoteRequest {
            candidate_id: Id(1),
            term: 128,
            position: Position::of(10, 10),
        }
        .into();
        assert_eq!(
            Message::from(bytes),
            VoteRequest {
                candidate_id: Id(1),
                term: 128,
                position: Position::of(10, 10),
            }
        );
    }

    #[test]
    fn vote_response() {
        let bytes: Bytes = VoteResponse {
            vote_granted: true,
            term: 128,
        }
        .into();
        assert_eq!(
            Message::from(bytes),
            VoteResponse {
                vote_granted: true,
                term: 128,
            }
        );
    }
}

use bytes::{Buf, BufMut, Bytes, BytesMut};
use derive_more::Display;

use crate::protocol::Message::{AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::Id;

const APPEND_REQUEST_MESSAGE_ID: u16 = 1;
const APPEND_RESPONSE_MESSAGE_ID: u16 = 2;
const VOTE_REQUEST_MESSAGE_ID: u16 = 3;
const VOTE_RESPONSE_MESSAGE_ID: u16 = 4;

#[derive(PartialEq, Display, Debug)]
pub(crate) enum Message {
    #[display(fmt = "AppendRequest {{ term: {}, leader_id: {} }}", term, leader_id)]
    AppendRequest { term: u64, leader_id: Id },

    #[display(fmt = "AppendResponse {{ term: {}, success: {} }}", term, success)]
    AppendResponse { term: u64, success: bool },

    #[display(fmt = "VoteRequest {{ term: {}, candidate_id: {} }}", term, candidate_id)]
    VoteRequest { term: u64, candidate_id: Id },

    #[display(fmt = "VoteResponse {{ term: {}, vote_granted: {} }}", term, vote_granted)]
    VoteResponse { term: u64, vote_granted: bool },
}

impl Message {
    pub(crate) fn append_request(term: u64, leader_id: Id) -> Self {
        AppendRequest { term, leader_id }
    }

    pub(crate) fn append_response(term: u64, success: bool) -> Self {
        AppendResponse { term, success }
    }

    pub(crate) fn vote_request(term: u64, candidate_id: Id) -> Self {
        VoteRequest { term, candidate_id }
    }

    pub(crate) fn vote_response(term: u64, vote_granted: bool) -> Self {
        VoteResponse { term, vote_granted }
    }
}

impl Into<Bytes> for Message {
    fn into(self) -> Bytes {
        let mut bytes = BytesMut::new();
        match self {
            AppendRequest { term, leader_id } => {
                bytes.put_u16_le(APPEND_REQUEST_MESSAGE_ID);
                bytes.put_u64_le(term);
                bytes.put_u8(leader_id);
            }
            AppendResponse { term, success } => {
                bytes.put_u16_le(APPEND_RESPONSE_MESSAGE_ID);
                bytes.put_u64_le(term);
                bytes.put_u8(if success { 1 } else { 0 });
            }
            VoteRequest { term, candidate_id } => {
                bytes.put_u16_le(VOTE_REQUEST_MESSAGE_ID);
                bytes.put_u64_le(term);
                bytes.put_u8(candidate_id);
            }
            VoteResponse { term, vote_granted } => {
                bytes.put_u16_le(VOTE_RESPONSE_MESSAGE_ID);
                bytes.put_u64_le(term);
                bytes.put_u8(if vote_granted { 1 } else { 0 });
            }
        }
        bytes.freeze()
    }
}

impl From<Bytes> for Message {
    fn from(mut bytes: Bytes) -> Self {
        let r#type = bytes.get_u16_le();
        match r#type {
            APPEND_REQUEST_MESSAGE_ID => AppendRequest {
                term: bytes.get_u64_le(),
                leader_id: bytes.get_u8(),
            },
            APPEND_RESPONSE_MESSAGE_ID => AppendResponse {
                term: bytes.get_u64_le(),
                success: bytes.get_u8() == 1,
            },
            VOTE_REQUEST_MESSAGE_ID => VoteRequest {
                term: bytes.get_u64_le(),
                candidate_id: bytes.get_u8(),
            },
            VOTE_RESPONSE_MESSAGE_ID => VoteResponse {
                term: bytes.get_u64_le(),
                vote_granted: bytes.get_u8() == 1,
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
        let bytes: Bytes = AppendRequest { term: 69, leader_id: 1 }.into();
        assert_eq!(Message::from(bytes), AppendRequest { term: 69, leader_id: 1 });
    }

    #[test]
    fn append_response() {
        let bytes: Bytes = AppendResponse {
            term: 69,
            success: true,
        }
        .into();
        assert_eq!(
            Message::from(bytes),
            AppendResponse {
                term: 69,
                success: true
            }
        );
    }

    #[test]
    fn vote_request() {
        let bytes: Bytes = VoteRequest {
            term: 69,
            candidate_id: 1,
        }
        .into();
        assert_eq!(
            Message::from(bytes),
            VoteRequest {
                term: 69,
                candidate_id: 1
            }
        );
    }

    #[test]
    fn vote_response() {
        let bytes: Bytes = VoteResponse {
            term: 69,
            vote_granted: true,
        }
        .into();
        assert_eq!(
            Message::from(bytes),
            VoteResponse {
                term: 69,
                vote_granted: true
            }
        );
    }
}

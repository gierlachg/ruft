use std::convert::TryInto;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use derive_more::Display;

use crate::relay::protocol::Message::{StoreRedirectResponse, StoreRequest, StoreSuccessResponse};

const STORE_REQUEST_MESSAGE_ID: u16 = 1;
const STORE_SUCCESS_RESPONSE_MESSAGE_ID: u16 = 2;
const STORE_REDIRECT_RESPONSE_MESSAGE_ID: u16 = 3;

#[derive(PartialEq, Display, Debug)]
pub(crate) enum Message {
    #[display(fmt = "StoreRequest {{ id: {} }}", id)]
    StoreRequest { id: u64, payload: Bytes },

    #[display(fmt = "StoreSuccessResponse {{ id: {} }}", id)]
    StoreSuccessResponse { id: u64 },

    #[display(fmt = "StoreRedirectResponse {{ id: {} }}", id)]
    StoreRedirectResponse { id: u64 }, // TODO: pass the leader ip/id
}

impl Message {
    pub(crate) fn store_success_response(id: u64) -> Self {
        StoreSuccessResponse { id }
    }

    pub(crate) fn store_redirect_response(id: u64) -> Self {
        StoreRedirectResponse { id }
    }
}

impl Into<Bytes> for Message {
    fn into(self) -> Bytes {
        let mut bytes = BytesMut::new();
        match self {
            StoreSuccessResponse { id } => {
                bytes.put_u16_le(STORE_SUCCESS_RESPONSE_MESSAGE_ID);
                bytes.put_u64_le(id);
            }
            StoreRedirectResponse { id } => {
                bytes.put_u16_le(STORE_REDIRECT_RESPONSE_MESSAGE_ID);
                bytes.put_u64_le(id);
            }
            _ => unreachable!(),
        }
        bytes.freeze()
    }
}

impl From<Bytes> for Message {
    fn from(mut bytes: Bytes) -> Self {
        let r#type = bytes.get_u16_le();
        match r#type {
            STORE_REQUEST_MESSAGE_ID => {
                let id = bytes.get_u64_le();
                let len = bytes.get_u32_le().try_into().expect("Unable to convert");
                let payload = bytes.split_to(len);
                StoreRequest { id, payload }
            }
            r#type => panic!("Unknown message type: {}", r#type),
        }
    }
}

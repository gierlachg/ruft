use std::convert::TryInto;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use derive_more::Display;

use crate::relay::protocol::Message::{StoreRedirectResponse, StoreRequest, StoreSuccessResponse};

const STORE_REQUEST_MESSAGE_ID: u16 = 1;
const STORE_SUCCESS_RESPONSE_MESSAGE_ID: u16 = 2;
const STORE_REDIRECT_RESPONSE_MESSAGE_ID: u16 = 3;

#[derive(PartialEq, Display, Debug)]
pub(crate) enum Message {
    #[display(fmt = "StoreRequest {{ }}")]
    StoreRequest { payload: Bytes },

    #[display(fmt = "StoreSuccessResponse {{ }}")]
    StoreSuccessResponse {},

    #[display(fmt = "StoreRedirectResponse {{ }}")]
    StoreRedirectResponse {}, // TODO: pass the leader ip/id
}

impl Message {
    pub(crate) fn store_request(payload: Bytes) -> Self {
        StoreRequest { payload }
    }
}

impl Into<Bytes> for Message {
    fn into(self) -> Bytes {
        let mut bytes = BytesMut::new();
        match self {
            StoreRequest { payload } => {
                bytes.put_u16_le(STORE_REQUEST_MESSAGE_ID);
                bytes.put_u32_le(payload.len().try_into().expect("Unable to convert"));
                bytes.put(payload.as_ref());
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
            STORE_SUCCESS_RESPONSE_MESSAGE_ID => StoreSuccessResponse {},
            STORE_REDIRECT_RESPONSE_MESSAGE_ID => StoreRedirectResponse {},
            r#type => panic!("Unknown message type: {}", r#type),
        }
    }
}

use std::convert::TryInto;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use derive_more::Display;

use crate::relay::protocol::Request::StoreRequest;
use crate::relay::protocol::Response::{StoreRedirectResponse, StoreSuccessResponse};

const STORE_REQUEST_MESSAGE_ID: u16 = 1;
const STORE_SUCCESS_RESPONSE_MESSAGE_ID: u16 = 2;
const STORE_REDIRECT_RESPONSE_MESSAGE_ID: u16 = 3;

#[derive(PartialEq, Display, Debug)]
pub(crate) enum Request {
    #[display(fmt = "StoreRequest {{ }}")]
    StoreRequest { payload: Bytes },
}

impl From<Bytes> for Request {
    fn from(mut bytes: Bytes) -> Self {
        let r#type = bytes.get_u16_le();
        match r#type {
            STORE_REQUEST_MESSAGE_ID => {
                let len = bytes.get_u32_le().try_into().expect("Unable to convert");
                let payload = bytes.split_to(len);
                StoreRequest { payload }
            }
            r#type => panic!("Unknown message type: {}", r#type),
        }
    }
}

#[derive(PartialEq, Display, Debug)]
pub(crate) enum Response {
    #[display(fmt = "StoreSuccessResponse {{ }}")]
    StoreSuccessResponse {},

    #[display(fmt = "StoreRedirectResponse {{ }}")]
    StoreRedirectResponse {}, // TODO: pass the leader ip/id
}

impl Response {
    pub(crate) fn store_success_response() -> Self {
        StoreSuccessResponse {}
    }

    pub(crate) fn store_redirect_response() -> Self {
        StoreRedirectResponse {}
    }
}

impl Into<Bytes> for Response {
    fn into(self) -> Bytes {
        let mut bytes = BytesMut::new();
        match self {
            StoreSuccessResponse {} => {
                bytes.put_u16_le(STORE_SUCCESS_RESPONSE_MESSAGE_ID);
            }
            StoreRedirectResponse {} => {
                bytes.put_u16_le(STORE_REDIRECT_RESPONSE_MESSAGE_ID);
            }
        }
        bytes.freeze()
    }
}

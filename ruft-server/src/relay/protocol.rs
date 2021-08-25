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

// TODO: TryFrom
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
    StoreRedirectResponse { leader_address: String },
}

impl Response {
    pub(crate) fn store_success_response() -> Self {
        StoreSuccessResponse {}
    }

    pub(crate) fn store_redirect_response(leader_address: &str) -> Self {
        StoreRedirectResponse {
            leader_address: leader_address.to_string(),
        }
    }
}

impl Into<Bytes> for Response {
    fn into(self) -> Bytes {
        let mut bytes = BytesMut::new();
        match self {
            StoreSuccessResponse {} => {
                bytes.put_u16_le(STORE_SUCCESS_RESPONSE_MESSAGE_ID);
            }
            StoreRedirectResponse { leader_address } => {
                bytes.put_u16_le(STORE_REDIRECT_RESPONSE_MESSAGE_ID);
                bytes.put_u32_le(leader_address.len().try_into().expect("Unable to convert"));
                bytes.put(leader_address.as_bytes());
            }
        }
        bytes.freeze()
    }
}

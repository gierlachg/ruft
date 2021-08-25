use std::convert::TryInto;
use std::net::SocketAddr;

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

impl Request {
    pub(crate) fn store_request(payload: Bytes) -> Self {
        StoreRequest { payload }
    }
}

impl Into<Bytes> for &Request {
    fn into(self) -> Bytes {
        let mut bytes = BytesMut::new();
        match self {
            StoreRequest { payload } => {
                bytes.put_u16_le(STORE_REQUEST_MESSAGE_ID);
                bytes.put_u32_le(payload.len().try_into().expect("Unable to convert"));
                bytes.put(payload.as_ref());
            }
        }
        bytes.freeze()
    }
}

#[derive(PartialEq, Display, Debug)]
pub(crate) enum Response {
    #[display(fmt = "StoreSuccessResponse {{ }}")]
    StoreSuccessResponse {},

    #[display(fmt = "StoreRedirectResponse {{ }}")]
    StoreRedirectResponse { leader_address: SocketAddr },
}

// TODO: TryFrom
impl From<Bytes> for Response {
    fn from(mut bytes: Bytes) -> Self {
        let r#type = bytes.get_u16_le();
        match r#type {
            STORE_SUCCESS_RESPONSE_MESSAGE_ID => StoreSuccessResponse {},
            STORE_REDIRECT_RESPONSE_MESSAGE_ID => {
                let len = bytes.get_u32_le().try_into().expect("Unable to convert");
                let payload = bytes.split_to(len);
                StoreRedirectResponse {
                    leader_address: (String::from_utf8_lossy(payload.as_ref()).parse().unwrap()), // TODO:
                }
            }
            r#type => panic!("Unknown message type: {}", r#type),
        }
    }
}

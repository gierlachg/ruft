use std::net::SocketAddr;

use bytes::Bytes;

use crate::relay::protocol::Message;
use crate::relay::tcp::Writer;
use crate::Result;

mod protocol;
mod tcp;

pub(super) struct Relay {
    leader: Option<Writer>,
}

impl Relay {
    pub(super) async fn init<E>(endpoints: E) -> Result<Self>
    where
        E: IntoIterator<Item = SocketAddr>,
    {
        let leader = match endpoints.into_iter().next() {
            Some(endpoint) => Some(Writer::connect(&endpoint).await?),
            None => None,
        };

        Ok(Relay { leader })
    }

    pub(super) async fn store(&mut self, payload: Bytes) {
        if let Some(leader) = self.leader.as_mut() {
            let message = Message::store_request(1, payload);
            leader.write(message.into()).await.unwrap();
        }
    }
}

use std::net::SocketAddr;

use crate::relay::protocol::Message;
use crate::relay::protocol::Message::{StoreRedirectResponse, StoreSuccessResponse};
use crate::relay::tcp::Stream;
use crate::{Result, RuftClientError};

pub(crate) mod protocol;
mod tcp;

pub(super) struct Relay {
    stream: Option<Stream>,
}

impl Relay {
    pub(super) async fn init<E>(endpoints: E) -> Result<Self>
    where
        E: IntoIterator<Item = SocketAddr>,
    {
        let stream = match endpoints.into_iter().next() {
            Some(endpoint) => Some(Stream::connect(&endpoint).await?),
            None => None,
        };

        Ok(Relay { stream })
    }

    pub(super) async fn store(&mut self, message: Message) -> Result<()> {
        if let Some(stream) = self.stream.as_mut() {
            stream.write(message.into()).await?;

            match stream.read().await {
                Some(Ok(message)) => match Message::from(message) {
                    StoreSuccessResponse {} => Ok(()),
                    StoreRedirectResponse {} => Err(RuftClientError::GenericFailure("".into())),
                    _ => unreachable!(),
                },
                _ => Err(RuftClientError::GenericFailure("".into())),
            }
        } else {
            Err(RuftClientError::GenericFailure("".into()))
        }
    }
}

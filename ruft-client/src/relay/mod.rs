use std::net::SocketAddr;

use futures::StreamExt;
use tokio::time::{self, Duration};

use crate::relay::protocol::Message;
use crate::relay::protocol::Message::{StoreRedirectResponse, StoreSuccessResponse};
use crate::relay::tcp::Stream;
use crate::{Result, RuftClientError};

pub(crate) mod protocol;
mod tcp;

// TODO: configurable
const CONNECTION_TIMEOUT_MILLIS: u64 = 5_000;

pub(super) struct Relay {
    stream: Option<Stream>,
}

impl Relay {
    pub(super) async fn init<E>(endpoints: E) -> Result<Self>
    where
        E: IntoIterator<Item = SocketAddr, IntoIter: Clone>,
    {
        time::timeout(
            Duration::from_millis(CONNECTION_TIMEOUT_MILLIS),
            Box::pin(
                tokio_stream::iter(endpoints.into_iter().cycle())
                    .filter_map(|endpoint| async move { Stream::connect(&endpoint).await.ok() }),
            )
            .next(),
        )
        .await
        .map(|stream| Relay { stream })
        .map_err(|_| RuftClientError::GenericFailure("Unable to connect to cluster".into()))
    }

    pub(super) async fn store(&mut self, message: Message) -> Result<()> {
        self.stream.as_mut().unwrap().write(message.into()).await?;

        match self.stream.as_mut().unwrap().read().await {
            Some(Ok(message)) => match Message::from(message) {
                StoreSuccessResponse {} => Ok(()),
                StoreRedirectResponse {} => Err(RuftClientError::GenericFailure("".into())),
                _ => unreachable!(),
            },
            _ => Err(RuftClientError::GenericFailure(
                "Unable to communicate with the cluster".into(),
            )),
        }
    }
}

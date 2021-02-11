use std::net::SocketAddr;

use tokio::time::{self, Duration, Instant};

use crate::relay::protocol::Message;
use crate::relay::protocol::Message::{StoreRedirectResponse, StoreSuccessResponse};
use crate::relay::tcp::Stream;
use crate::{Result, RuftClientError};

pub(crate) mod protocol;
mod tcp;

// TODO: configurable
const CONNECTION_TIMEOUT_MILLIS: u64 = 1_000;

pub(super) struct Relay {
    stream: Stream,
}

impl Relay {
    pub(super) async fn init<E>(endpoints: E) -> Result<Self>
    where
        E: IntoIterator<Item = SocketAddr>,
    {
        match endpoints.into_iter().next() {
            Some(endpoint) => {
                let interval = Duration::from_millis(CONNECTION_TIMEOUT_MILLIS);
                let mut timer = time::interval_at(Instant::now() + interval, interval);
                loop {
                    tokio::select! {
                        _ = timer.tick() => {
                            return Err(RuftClientError::GenericFailure("Unable to connect to cluster".into()))
                        }
                        result = Stream::connect(&endpoint) => {
                            match result {
                                Ok(stream) => return Ok(Relay { stream }),
                                Err(_) => { }
                            }
                        }
                    }
                }
            }
            None => Err(RuftClientError::GenericFailure("Unable to connect to cluster".into())),
        }
    }

    pub(super) async fn store(&mut self, message: Message) -> Result<()> {
        self.stream.write(message.into()).await?;

        match self.stream.read().await {
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

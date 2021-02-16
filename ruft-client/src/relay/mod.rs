use std::net::SocketAddr;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::Mutex;
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
    endpoints: Vec<SocketAddr>,
    stream: Arc<Mutex<Option<Stream>>>,
}

impl Relay {
    pub(super) async fn init(endpoints: Vec<SocketAddr>) -> Result<Self> {
        time::timeout(
            Duration::from_millis(CONNECTION_TIMEOUT_MILLIS),
            Self::connect(endpoints.clone()),
        )
        .await
        .map(|stream| Relay {
            endpoints,
            stream: Arc::new(Mutex::new(stream)),
        })
        .map_err(|_| RuftClientError::GenericFailure("Unable to connect to cluster".into()))
    }

    pub(super) async fn store(&mut self, message: Message) -> Result<()> {
        let mut holder = self.stream.lock().await;
        match holder.as_mut() {
            Some(stream) => {
                match Self::exchange(stream, message).await {
                    Some(message) => match message {
                        StoreSuccessResponse {} => Ok(()),
                        StoreRedirectResponse {} => Err(RuftClientError::GenericFailure("".into())), // TODO: connect to the leader
                        _ => unreachable!(),
                    },
                    None => {
                        *holder = None;
                        Self::reconnect(self.endpoints.clone(), self.stream.clone());
                        Err(RuftClientError::GenericFailure(
                            "Error occurred while communicating with the cluster".into(),
                        ))
                    }
                }
            }
            None => Err(RuftClientError::GenericFailure(
                "Unable to communicate with the cluster".into(),
            )),
        }
    }

    async fn exchange(stream: &mut Stream, message: Message) -> Option<Message> {
        match stream.write(message.into()).await {
            Ok(_) => stream.read().await.and_then(Result::ok).map(Message::from),
            Err(_) => None,
        }
    }

    fn reconnect(endpoints: Vec<SocketAddr>, holder: Arc<Mutex<Option<Stream>>>) {
        tokio::spawn(async move {
            match Self::connect(endpoints).await {
                Some(stream) => holder.lock().await.replace(stream),
                None => unreachable!(),
            };
        });
    }

    async fn connect(endpoints: Vec<SocketAddr>) -> Option<Stream> {
        Box::pin(
            tokio_stream::iter(endpoints.iter().cycle())
                .filter_map(|endpoint| async move { Stream::connect(&endpoint).await.ok() }),
        )
        .next()
        .await
    }
}

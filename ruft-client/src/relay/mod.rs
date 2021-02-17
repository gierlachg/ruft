use std::net::SocketAddr;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{self, Duration};

use crate::relay::protocol::Message;
use crate::relay::protocol::Message::{StoreRedirectResponse, StoreSuccessResponse};
use crate::relay::tcp::Stream;
use crate::{Result, RuftClientError};

pub(crate) mod protocol;
mod tcp;

// TODO: configurable
const CONNECTION_TIMEOUT_MILLIS: u64 = 5_000;
const MAX_NUMBER_OF_IN_FLIGHT_MESSAGES: usize = 1024;

#[derive(Clone)]
pub(super) struct Relay {
    egress: mpsc::Sender<(Message, oneshot::Sender<Result<()>>)>,
}

impl Relay {
    pub(super) async fn init(endpoints: Vec<SocketAddr>) -> Result<Self> {
        let stream = time::timeout(
            Duration::from_millis(CONNECTION_TIMEOUT_MILLIS),
            Self::connect(endpoints.clone()),
        )
        .await
        .map(|stream| Arc::new(Mutex::new(stream)))
        .map_err(|_| RuftClientError::GenericFailure("Unable to connect to the cluster".into()))?;

        let (tx, rx) = mpsc::channel(MAX_NUMBER_OF_IN_FLIGHT_MESSAGES);

        tokio::spawn(async move { Self::run(endpoints, stream, rx).await });

        Ok(Relay { egress: tx })
    }

    pub(super) async fn store(&mut self, message: Message) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.egress
            .send((message, tx))
            .await
            .map_err(|_| RuftClientError::GenericFailure("Slow down cowboy!".into()))?;
        rx.await.expect("Error occurred while receiving response")
    }

    async fn run(
        endpoints: Vec<SocketAddr>,
        stream: Arc<Mutex<Option<Stream>>>,
        mut rx: mpsc::Receiver<(Message, oneshot::Sender<Result<()>>)>,
    ) {
        loop {
            if let Err(_) = match rx.recv().await {
                Some((request, responder)) => {
                    let mut holder = stream.lock().await;
                    match holder.as_mut() {
                        Some(writer) => {
                            match Self::exchange(writer, request).await {
                                Some(response) => match response {
                                    StoreSuccessResponse {} => responder.send(Ok(())),
                                    StoreRedirectResponse {} => {
                                        // TODO: connect to the leader
                                        responder.send(RuftClientError::generic_failure(""))
                                    }
                                    _ => unreachable!(),
                                },
                                None => {
                                    *holder = None;
                                    Self::reconnect(endpoints.clone(), stream.clone());
                                    responder.send(RuftClientError::generic_failure(
                                        "Error occurred while communicating with the cluster",
                                    ))
                                }
                            }
                        }
                        None => responder.send(RuftClientError::generic_failure(
                            "Unable to communicate with the cluster",
                        )),
                    }
                }
                None => break,
            } {
                panic!("Error occurred while sending response")
            }
        }
    }

    async fn exchange(stream: &mut Stream, request: Message) -> Option<Message> {
        match stream.write(request.into()).await {
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

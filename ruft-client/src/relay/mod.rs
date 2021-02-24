use std::collections::VecDeque;
use std::net::SocketAddr;

use futures::{FutureExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};

use crate::relay::protocol::Message;
use crate::relay::protocol::Message::{StoreRedirectResponse, StoreSuccessResponse};
use crate::relay::tcp::Stream;
use crate::relay::State::{DISCONNECTED, DONE};
use crate::{Result, RuftClientError};

pub(crate) mod protocol;
mod tcp;

// TODO: configurable
const CONNECTION_TIMEOUT_MILLIS: u64 = 5_000;
const MAX_NUMBER_OF_IN_FLIGHT_MESSAGES: usize = 1024;

const SENDING_RESPONSE_ERROR: &str = "Error occurred while sending response";

type Responder = oneshot::Sender<Result<()>>;

#[derive(Clone)]
pub(super) struct Relay {
    egress: mpsc::Sender<(Message, Responder)>,
}

impl Relay {
    pub(super) async fn init(endpoints: Vec<SocketAddr>) -> Result<Self> {
        let stream = time::timeout(
            Duration::from_millis(CONNECTION_TIMEOUT_MILLIS),
            Self::connect(endpoints.clone()),
        )
        .await
        .map_err(|_| RuftClientError::GenericFailure("Unable to connect to the cluster".into()))?;

        let (tx, rx) = mpsc::channel(MAX_NUMBER_OF_IN_FLIGHT_MESSAGES);

        tokio::spawn(async move { Self::run(rx, stream, endpoints).await });

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

    async fn run(mut requests: mpsc::Receiver<(Message, Responder)>, stream: Stream, endpoints: Vec<SocketAddr>) {
        let mut stream = stream;
        let mut responders: VecDeque<Responder> = VecDeque::with_capacity(MAX_NUMBER_OF_IN_FLIGHT_MESSAGES);

        loop {
            match Self::service(&mut requests, stream, &mut responders).await {
                DISCONNECTED => responders.drain(..).for_each(|responder| {
                    responder
                        .send(RuftClientError::generic_failure(
                            "Error occurred while communicating with the cluster",
                        ))
                        .expect(SENDING_RESPONSE_ERROR)
                }),
                DONE => break,
            }

            match Self::reconnect(&mut requests, endpoints.clone()).await {
                Some(s) => stream = s,
                None => break,
            }
        }
    }

    async fn service(
        requests: &mut mpsc::Receiver<(Message, Responder)>,
        mut stream: Stream,
        responders: &mut VecDeque<Responder>,
    ) -> State {
        let (mut writer, mut reader) = stream.split();
        loop {
            tokio::select! {
                result = requests.recv() => match result {
                    Some((request, responder)) => {
                        responders.push_front(responder);
                        if let Err(_) = writer.write(request.into()).await {
                            return DISCONNECTED
                        }
                    }
                    None => return DONE,
                },
                result = reader.read() =>  match result.and_then(Result::ok).map(Message::from) {
                    Some(response) => {
                        let responder = responders.pop_back().unwrap();
                        match response {
                            StoreSuccessResponse {} => {
                                responder.send(Ok(())).expect(SENDING_RESPONSE_ERROR);
                            }
                            StoreRedirectResponse {} => {
                                // TODO: connect to the leader
                                responder
                                    .send(RuftClientError::generic_failure(""))
                                    .expect(SENDING_RESPONSE_ERROR);
                            }
                            _ => unreachable!(),
                        }
                    }
                    None => return DISCONNECTED
                }
            }
        }
    }

    async fn reconnect(
        requests: &mut mpsc::Receiver<(Message, Responder)>,
        endpoints: Vec<SocketAddr>,
    ) -> Option<Stream> {
        let mut streams = Box::pin(Self::connect(endpoints).into_stream());
        loop {
            tokio::select! {
                result = requests.recv() => {
                    match result {
                        Some((_, responder)) => {
                            responder.send(RuftClientError::generic_failure(
                                "Unable to communicate with the cluster",
                            )).expect("Error occurred while sending response");
                        }
                        None => break
                    }
                }
                stream = streams.next() => return stream
            }
        }
        None
    }

    async fn connect(endpoints: Vec<SocketAddr>) -> Stream {
        Box::pin(
            tokio_stream::iter(endpoints.iter().cycle())
                .filter_map(|endpoint| async move { Stream::connect(&endpoint).await.ok() }),
        )
        .next()
        .await
        .expect("That really should not happen")
    }
}

enum State {
    DISCONNECTED,
    DONE,
}

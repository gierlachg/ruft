use std::collections::VecDeque;
use std::net::SocketAddr;

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};

use crate::relay::protocol::Message;
use crate::relay::protocol::Message::{StoreRedirectResponse, StoreSuccessResponse};
use crate::relay::tcp::Stream;
use crate::relay::ConnectionState::{BROKEN, CLOSED};
use crate::{Result, RuftClientError};

pub(crate) mod protocol;
mod tcp;

// TODO: configurable
const CONNECTION_TIMEOUT_MILLIS: u64 = 5_000;
const MAX_NUMBER_OF_IN_FLIGHT_MESSAGES: usize = 1024;

#[derive(Clone)]
pub(super) struct Relay {
    egress: mpsc::Sender<(Message, Responder)>,
}

impl Relay {
    pub(super) async fn init(endpoints: Vec<SocketAddr>) -> Result<Self> {
        let stream = time::timeout(
            Duration::from_millis(CONNECTION_TIMEOUT_MILLIS),
            Box::pin(Self::connect(&endpoints)).next(),
        )
        .await
        .unwrap_or(None)
        .ok_or(RuftClientError::generic_failure("Unable to connect to the cluster"))?;

        let (tx, rx) = mpsc::channel(MAX_NUMBER_OF_IN_FLIGHT_MESSAGES);

        tokio::spawn(async move { Self::run(rx, stream, endpoints).await });

        Ok(Relay { egress: tx })
    }

    pub(super) async fn store(&mut self, message: Message) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.egress
            .send((message, Responder(tx)))
            .await
            .map_err(|_| RuftClientError::generic_failure("Slow down cowboy!"))?;
        rx.await.expect("Error occurred while receiving response")
    }

    async fn run(mut requests: mpsc::Receiver<(Message, Responder)>, stream: Stream, endpoints: Vec<SocketAddr>) {
        let mut stream = stream;
        let mut responders = Responders(VecDeque::with_capacity(MAX_NUMBER_OF_IN_FLIGHT_MESSAGES));

        loop {
            match Self::service(&mut requests, stream, &mut responders).await {
                BROKEN => responders.fail_all(),
                CLOSED => break,
            }

            match Self::reconnect(&mut requests, &endpoints).await {
                Some(s) => stream = s,
                None => break,
            }
        }
    }

    async fn service(
        requests: &mut mpsc::Receiver<(Message, Responder)>,
        mut stream: Stream,
        responders: &mut Responders,
    ) -> ConnectionState {
        let (mut writer, mut reader) = stream.split();
        loop {
            tokio::select! {
                result = requests.recv() => match result {
                    Some((request, responder)) => {
                        responders.push(responder);
                        if let Err(_) = writer.write(request.into()).await {
                            return BROKEN
                        }
                    }
                    None => return CLOSED,
                },
                result = reader.read() =>  match result.and_then(Result::ok).map(Message::from) {
                    Some(response) => {
                        match response {
                            StoreSuccessResponse {} =>  responders.pop().respond_with_success(),
                            StoreRedirectResponse {} =>  responders.pop().respond_with_error(""), // TODO: connect to the leader
                            _ => unreachable!(),
                        }
                    }
                    None => return BROKEN
                }
            }
        }
    }

    async fn reconnect(
        requests: &mut mpsc::Receiver<(Message, Responder)>,
        endpoints: &Vec<SocketAddr>,
    ) -> Option<Stream> {
        let mut streams = Box::pin(Self::connect(endpoints));
        loop {
            tokio::select! {
                result = requests.recv() => {
                    match result {
                        Some((_, responder)) => responder.respond_with_error("Unable to communicate with the cluster"),
                        None => return None
                    }
                }
                stream = streams.next() => return stream
            }
        }
    }

    fn connect(endpoints: &Vec<SocketAddr>) -> impl tokio_stream::Stream<Item = Stream> + '_ {
        tokio_stream::iter(endpoints.iter().cycle())
            .filter_map(|endpoint| async move { Stream::connect(endpoint).await.ok() })
    }
}

enum ConnectionState {
    BROKEN,
    CLOSED,
}

struct Responders(VecDeque<Responder>);

impl Responders {
    fn push(&mut self, responder: Responder) {
        self.0.push_front(responder)
    }

    fn pop(&mut self) -> Responder {
        self.0.pop_front().expect("No responder!")
    }

    fn fail_all(&mut self) {
        self.0
            .drain(..)
            .for_each(|responder| responder.respond_with_error("Error occurred while communicating with the cluster"))
    }
}

struct Responder(oneshot::Sender<Result<()>>);

impl Responder {
    fn respond_with_error(self, message: &str) {
        self.0
            .send(Err(RuftClientError::generic_failure(message)))
            .expect("Error occurred while relaying response")
    }

    fn respond_with_success(self) {
        self.0.send(Ok(())).expect("Error occurred while relaying response")
    }
}

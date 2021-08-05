use std::collections::VecDeque;
use std::net::SocketAddr;

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};

use crate::relay::protocol::Request;
use crate::relay::protocol::Response::{self, StoreRedirectResponse, StoreSuccessResponse};
use crate::relay::tcp::Connection;
use crate::relay::ConnectionState::{BROKEN, CLOSED};
use crate::{Result, RuftClientError};

pub(crate) mod protocol;
mod tcp;

const MAX_NUMBER_OF_IN_FLIGHT_MESSAGES: usize = 1024;

#[derive(Clone)]
pub(super) struct Relay {
    egress: mpsc::Sender<(Request, Responder)>,
}

impl Relay {
    pub(super) async fn init(endpoints: Vec<SocketAddr>, connection_timeout_millis: u64) -> Result<Self> {
        let connection = time::timeout(
            Duration::from_millis(connection_timeout_millis),
            Box::pin(Self::connect(&endpoints)).next(),
        )
        .await
        .unwrap_or(None)
        .ok_or(RuftClientError::generic_failure("Unable to connect to the cluster"))?;

        let (tx, rx) = mpsc::channel(MAX_NUMBER_OF_IN_FLIGHT_MESSAGES);

        tokio::spawn(async move { Self::run(rx, connection, endpoints).await });

        Ok(Relay { egress: tx })
    }

    pub(super) async fn send(&mut self, request: Request) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.egress
            .send((request, Responder(tx)))
            .await
            .map_err(|_| RuftClientError::generic_failure("Slow down cowboy!"))?;
        rx.await.expect("Error occurred while receiving response")
    }

    async fn run(
        mut requests: mpsc::Receiver<(Request, Responder)>,
        connection: Connection,
        endpoints: Vec<SocketAddr>,
    ) {
        let mut connection = connection;
        let mut responders = Responders(VecDeque::with_capacity(MAX_NUMBER_OF_IN_FLIGHT_MESSAGES));

        loop {
            match Self::service(&mut requests, connection, &mut responders).await {
                BROKEN => responders.fail_all(),
                CLOSED => break,
            }

            match Self::reconnect(&mut requests, &endpoints).await {
                Some(s) => connection = s,
                None => break,
            }
        }
    }

    async fn service(
        requests: &mut mpsc::Receiver<(Request, Responder)>,
        mut connection: Connection,
        responders: &mut Responders,
    ) -> ConnectionState {
        let (mut writer, mut reader) = connection.split();
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
                result = reader.read() =>  match result.and_then(Result::ok).map(Response::from) {
                    Some(response) => match response {
                        StoreSuccessResponse {} =>  responders.pop().respond_with_success(),
                        StoreRedirectResponse {} =>  responders.pop().respond_with_error(""), // TODO: connect to the leader
                    },
                    None => return BROKEN
                }
            }
        }
    }

    async fn reconnect(
        requests: &mut mpsc::Receiver<(Request, Responder)>,
        endpoints: &Vec<SocketAddr>,
    ) -> Option<Connection> {
        let mut connections = Box::pin(Self::connect(endpoints));
        loop {
            tokio::select! {
                result = requests.recv() => match result {
                     Some((_, responder)) => responder.respond_with_error("Unable to communicate with the cluster"),
                     None => return None
                },
                connection = connections.next() => return connection
            }
        }
    }

    fn connect(endpoints: &Vec<SocketAddr>) -> impl tokio_stream::Stream<Item = Connection> + '_ {
        tokio_stream::iter(endpoints.iter().cycle())
            .filter_map(|endpoint| async move { Connection::connect(endpoint).await.ok() })
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

use std::collections::VecDeque;
use std::net::SocketAddr;

use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};

use crate::relay::protocol::Request;
use crate::relay::protocol::Response::{self, StoreRedirectResponse, StoreSuccessResponse};
use crate::relay::tcp::Connection;
use crate::relay::ConnectionState::{BROKEN, CLOSED, REDIRECT};
use crate::{Result, RuftClientError};

pub(crate) mod protocol;
mod tcp;

const MAX_NUMBER_OF_IN_FLIGHT_REQUESTS: usize = 1024;

#[derive(Clone)]
pub(super) struct Relay {
    requests: mpsc::Sender<(Request, Responder)>,
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

        let (tx, rx) = mpsc::channel(MAX_NUMBER_OF_IN_FLIGHT_REQUESTS);
        tokio::spawn(async move { Self::run(rx, connection, endpoints).await });
        Ok(Relay { requests: tx })
    }

    async fn run(
        mut requests: mpsc::Receiver<(Request, Responder)>,
        connection: Connection,
        endpoints: Vec<SocketAddr>,
    ) {
        let mut exchanges = Exchanges(VecDeque::with_capacity(MAX_NUMBER_OF_IN_FLIGHT_REQUESTS));
        let mut connection = connection;
        let mut endpoints = endpoints;

        loop {
            endpoints = match Self::service(&mut requests, connection, &mut exchanges).await {
                REDIRECT(leader_address) => vec![leader_address], // TODO: ensure on the list ???
                BROKEN => endpoints,
                CLOSED => break,
            };

            connection = match Self::reconnect(&mut requests, &endpoints).await {
                Some(connection) => connection,
                None => break,
            }
        }
    }

    async fn service(
        requests: &mut mpsc::Receiver<(Request, Responder)>,
        mut connection: Connection,
        exchanges: &mut Exchanges,
    ) -> ConnectionState {
        for request in exchanges.requests() {
            if let Err(_) = connection.write(request).await {
                return BROKEN;
            }
        }

        loop {
            tokio::select! {
                result = requests.recv() => match result {
                    Some((request, responder)) => {
                        let request = exchanges.enqueue(request, responder);
                        if let Err(_) = connection.write(request).await {
                            return BROKEN
                        }
                    }
                    None => return CLOSED,
                },
                result = connection.read() =>  match result.and_then(Result::ok).map(Response::from) {
                    Some(response) => match response {
                        StoreSuccessResponse {} =>  exchanges.dequeue().respond_with_success(),
                        StoreRedirectResponse {leader_address} => return REDIRECT(leader_address)
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

    pub(super) async fn send(&mut self, request: Request) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.requests
            .send((request, Responder(tx)))
            .await
            .map_err(|_| RuftClientError::generic_failure("Slow down cowboy!"))?; // TODO: drop latest ???
        rx.await.expect("Error occurred while receiving response")
    }
}

enum ConnectionState {
    REDIRECT(SocketAddr),
    BROKEN,
    CLOSED,
}

struct Exchanges(VecDeque<Exchange>);

impl Exchanges {
    fn enqueue(&mut self, request: Request, responder: Responder) -> Bytes {
        self.0.push_front(Exchange(request.into(), responder));
        self.0.back().unwrap().request()
    }

    fn dequeue(&mut self) -> Responder {
        self.0.pop_front().expect("No exchange!").responder()
    }

    fn requests<'a>(&'a self) -> impl Iterator<Item = Bytes> + 'a {
        self.0.iter().map(|exchange| exchange.request())
    }
}

struct Exchange(Request, Responder);

impl Exchange {
    fn request(&self) -> Bytes {
        (&self.0).into() // TODO: store Bytes ???
    }

    fn responder(self) -> Responder {
        self.1
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

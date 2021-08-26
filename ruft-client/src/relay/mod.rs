use std::collections::VecDeque;
use std::net::SocketAddr;

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

#[derive(Clone)]
pub(super) struct Relay {
    requests: mpsc::UnboundedSender<(Request, Responder)>,
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

        let (tx, rx) = mpsc::unbounded_channel();
        let requests = (tx.clone(), rx);
        tokio::spawn(async move { Self::run(requests, connection, endpoints).await });
        Ok(Relay { requests: tx })
    }

    async fn run(
        mut requests: (
            mpsc::UnboundedSender<(Request, Responder)>,
            mpsc::UnboundedReceiver<(Request, Responder)>,
        ),
        connection: Connection,
        endpoints: Vec<SocketAddr>,
    ) {
        let mut connection = connection;
        let mut endpoints = endpoints;
        let mut exchanges = Exchanges::new();

        loop {
            endpoints = match Self::service(&mut requests.1, &mut connection, &mut exchanges).await {
                REDIRECT(leader_address) => {
                    Self::drain(connection, &mut exchanges, requests.0.clone());
                    vec![leader_address] // TODO: ensure on the list ???
                }
                BROKEN => {
                    exchanges.fail();
                    endpoints
                }
                CLOSED => break,
            };

            connection = match Self::reconnect(&mut requests.1, &endpoints, &mut exchanges).await {
                Some(connection) => connection,
                None => break,
            }
        }
    }

    async fn service(
        requests: &mut mpsc::UnboundedReceiver<(Request, Responder)>,
        connection: &mut Connection,
        exchanges: &mut Exchanges,
    ) -> ConnectionState {
        if let Err(_) = exchanges.write(connection).await {
            return BROKEN;
        }

        loop {
            tokio::select! {
                result = requests.recv() => match result {
                    Some((request, responder)) => {
                        let exchange = exchanges.enqueue(request, responder);
                        if let Err(_) = exchange.write(connection).await {
                            return BROKEN
                        }
                    }
                    None => return CLOSED,
                },
                result = connection.read() =>  match result.and_then(Result::ok).map(Response::from) {
                    Some(response) => match response {
                        StoreSuccessResponse {} =>  exchanges.dequeue().responder().respond_with_success(),
                        StoreRedirectResponse {leader_address} => {
                            let Exchange (request, responder) = exchanges.dequeue();
                            let exchange = exchanges.enqueue(request, responder);
                            if let Err(_) = exchange.write(connection).await {
                                return BROKEN
                            }

                            if let Some(leader_address) = leader_address {
                                if &leader_address != connection.endpoint() {
                                    return REDIRECT(leader_address)
                                }
                            }
                        },
                    },
                    None => return BROKEN
                }
            }
        }
    }

    async fn reconnect(
        requests: &mut mpsc::UnboundedReceiver<(Request, Responder)>,
        endpoints: &Vec<SocketAddr>,
        exchanges: &mut Exchanges,
    ) -> Option<Connection> {
        let mut connections = Box::pin(Self::connect(endpoints));
        loop {
            tokio::select! {
                result = requests.recv() => match result {
                     Some((request, responder)) => {
                        exchanges.enqueue(request, responder);
                     }
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

    fn drain(
        mut connection: Connection,
        exchanges: &mut Exchanges,
        requests: mpsc::UnboundedSender<(Request, Responder)>,
    ) {
        let mut exchanges = exchanges.drain_to();
        tokio::spawn(async move {
            // TODO: stop on shutdown ??? holding onto requests prevents run() from stopping
            loop {
                match connection.read().await.and_then(Result::ok).map(Response::from) {
                    Some(response) => match response {
                        StoreSuccessResponse {} => exchanges.dequeue().responder().respond_with_success(),
                        StoreRedirectResponse { leader_address: _ } => {
                            let Exchange(request, responder) = exchanges.dequeue();
                            requests.send((request, responder)).unwrap_or(())
                        }
                    },
                    None => {
                        exchanges.fail();
                        break;
                    }
                }
            }
        });
    }

    pub(super) async fn send(&mut self, request: Request) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.requests
            .send((request, Responder(tx)))
            .map_err(|_| RuftClientError::generic_failure("This is unexpected!"))?;
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
    fn new() -> Self {
        Exchanges(VecDeque::new())
    }

    fn enqueue(&mut self, request: Request, responder: Responder) -> &Exchange {
        self.0.push_front(Exchange(request, responder));
        self.0.front().unwrap()
    }

    fn dequeue(&mut self) -> Exchange {
        self.0.pop_back().expect("No exchange!")
    }

    fn drain(&mut self) -> impl Iterator<Item = Exchange> + '_ {
        self.0.drain(..)
    }

    fn drain_to(&mut self) -> Exchanges {
        let mut exchanges = Exchanges::new();
        self.drain().for_each(|exchange| exchanges.0.push_front(exchange));
        exchanges
    }

    fn fail(&mut self) {
        self.drain()
            .for_each(|exchange| exchange.responder().respond_with_error());
    }

    async fn write(&self, connection: &mut Connection) -> Result<()> {
        for exchange in self.0.iter() {
            exchange.write(connection).await?
        }
        Ok(())
    }
}

struct Exchange(Request, Responder);

impl Exchange {
    fn responder(self) -> Responder {
        self.1
    }

    async fn write(&self, connection: &mut Connection) -> Result<()> {
        connection.write((&self.0).into()).await
    }
}

struct Responder(oneshot::Sender<Result<()>>);

impl Responder {
    fn respond_with_success(self) {
        self.0.send(Ok(())).unwrap_or(())
    }

    fn respond_with_error(self) {
        self.0
            .send(Err(RuftClientError::generic_failure(
                "Error occurred while communicating with the cluster",
            )))
            .unwrap_or(())
    }
}

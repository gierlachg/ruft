use std::collections::VecDeque;
use std::net::SocketAddr;

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};

use crate::relay::connector::Connector;
use crate::relay::protocol::Request;
use crate::relay::service::Service;
use crate::relay::tcp::Connection;
use crate::relay::State::{CONNECTED, DISCONNECTED, TERMINATED};
use crate::{Result, RuftClientError};

mod connector;
pub(crate) mod protocol;
mod service;
mod tcp;

#[derive(Clone)]
pub(super) struct Relay {
    requests: mpsc::UnboundedSender<(Request, Responder)>,
}

impl Relay {
    pub(super) async fn init(endpoints: Vec<SocketAddr>, connection_timeout_millis: u64) -> Result<Self> {
        let connection = time::timeout(
            Duration::from_millis(connection_timeout_millis),
            Box::pin(connect(&endpoints)).next(),
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
        let mut state = CONNECTED(connection, Exchanges::new());
        loop {
            state = match state {
                CONNECTED(connection, exchanges) => {
                    Service::new(&mut requests, connection, &endpoints, exchanges)
                        .run()
                        .await
                }
                DISCONNECTED(endpoints, exchanges) => Connector::new(&mut requests.1, endpoints, exchanges).run().await,
                TERMINATED => break,
            }
        }
    }

    pub(super) async fn send(&mut self, request: Request) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.requests
            .send((request, Responder(tx)))
            .map_err(|_| RuftClientError::generic_failure("This is unexpected!"))?;
        rx.await.expect("Error occurred while receiving response")
    }
}

fn connect(endpoints: &Vec<SocketAddr>) -> impl tokio_stream::Stream<Item = Connection> + '_ {
    tokio_stream::iter(endpoints.iter().cycle())
        .filter_map(|endpoint| async move { Connection::connect(endpoint).await.ok() })
}

enum State {
    CONNECTED(Connection, Exchanges),
    DISCONNECTED(Vec<SocketAddr>, Exchanges),
    TERMINATED,
}

// TODO: limit size ???
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

    fn requeue(&mut self) -> &Exchange {
        let Exchange(request, responder) = self.dequeue();
        self.enqueue(request, responder)
    }

    fn drain(&mut self) -> impl Iterator<Item = Exchange> + '_ {
        self.0.drain(..)
    }

    fn split_off(&mut self, at: usize) -> Exchanges {
        Exchanges(self.0.split_off(at))
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

use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::relay::protocol::Request;
use crate::relay::protocol::Response::{self, StoreRedirectResponse, StoreSuccessResponse};
use crate::relay::service::ConnectionState::{BROKEN, CLOSED, REDIRECT};
use crate::relay::tcp::Connection;
use crate::relay::State::{DISCONNECTED, TERMINATED};
use crate::relay::{Exchange, Exchanges, Responder, State};

pub(super) struct Service<'a> {
    requests: &'a mut (
        mpsc::UnboundedSender<(Request, Responder)>,
        mpsc::UnboundedReceiver<(Request, Responder)>,
    ),
    connection: Connection,
    endpoints: &'a Vec<SocketAddr>,
    exchanges: Exchanges,
}

impl<'a> Service<'a> {
    pub(super) fn new(
        requests: &'a mut (
            mpsc::UnboundedSender<(Request, Responder)>,
            mpsc::UnboundedReceiver<(Request, Responder)>,
        ),
        connection: Connection,
        endpoints: &'a Vec<SocketAddr>,
        exchanges: Exchanges,
    ) -> Self {
        Service {
            requests,
            connection,
            endpoints,
            exchanges,
        }
    }

    pub(super) async fn run(mut self) -> State {
        match Self::service(&mut self.requests.1, &mut self.connection, &mut self.exchanges).await {
            REDIRECT(leader_address) => {
                Self::drain(self.connection, self.exchanges.split_off(1), self.requests.0.clone());
                DISCONNECTED(vec![leader_address], self.exchanges) // TODO: ensure on the list ???
            }
            BROKEN => {
                self.exchanges.fail();
                DISCONNECTED(self.endpoints.clone(), self.exchanges)
            }
            CLOSED => TERMINATED,
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
                        StoreRedirectResponse {leader_address} => match leader_address {
                            Some(leader_address) if &leader_address != connection.endpoint() => return REDIRECT(leader_address),
                            _ => {
                                let exchange = exchanges.requeue();
                                if let Err(_) = exchange.write(connection).await {
                                    return BROKEN
                                }
                            }
                        }
                    },
                    None => return BROKEN
                }
            }
        }
    }

    fn drain(
        mut connection: Connection,
        mut exchanges: Exchanges,
        requests: mpsc::UnboundedSender<(Request, Responder)>,
    ) {
        tokio::spawn(async move {
            // TODO: stop on shutdown ??? holding onto requests prevents run() from terminating
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
}

enum ConnectionState {
    REDIRECT(SocketAddr),
    BROKEN,
    CLOSED,
}

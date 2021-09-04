use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::relay::protocol::Request;
use crate::relay::protocol::Response::{self, StoreRedirectResponse, StoreSuccessResponse};
use crate::relay::tcp::Connection;
use crate::relay::State::{DISCONNECTED, TERMINATED};
use crate::relay::{Exchange, Exchanges, Responder, State};

pub(super) struct Broker {}

impl Broker {
    pub(super) async fn service<'a>(
        requests: &'a mut (
            mpsc::UnboundedSender<(Request, Responder)>,
            mpsc::UnboundedReceiver<(Request, Responder)>,
        ),
        mut connection: Connection,
        endpoints: &'a Vec<SocketAddr>,
        mut exchanges: Exchanges,
    ) -> State {
        if let Err(_) = exchanges.write(&mut connection).await {
            exchanges.fail();
            return DISCONNECTED(endpoints.clone(), Exchanges::new());
        }

        loop {
            tokio::select! {
                result = requests.1.recv() => match result {
                    Some((request, responder)) => {
                        let exchange = exchanges.enqueue(request, responder);
                        if let Err(_) = exchange.write(&mut connection).await {
                            exchanges.fail();
                            return DISCONNECTED(endpoints.clone(), Exchanges::new())
                        }
                    }
                    None => return TERMINATED
                },
                result = connection.read() => match result.and_then(Result::ok).map(Response::from) {
                    Some(response) => match response {
                        StoreSuccessResponse {} =>  exchanges.dequeue().responder().respond_with_success(),
                        StoreRedirectResponse {leader_address} => match leader_address {
                            Some(leader_address) if &leader_address != connection.endpoint() => {
                                Self::drain(connection, exchanges.split_off(1), requests.0.clone());
                                return DISCONNECTED(vec!(leader_address), exchanges)
                            }
                            _ => {
                                let exchange = exchanges.requeue();
                                if let Err(_) = exchange.write(&mut connection).await {
                                    exchanges.fail();
                                    return DISCONNECTED(endpoints.clone(), Exchanges::new())
                                }
                            }
                        }
                    },
                    None => {
                        exchanges.fail();
                        return DISCONNECTED(endpoints.clone(), Exchanges::new())
                    }
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

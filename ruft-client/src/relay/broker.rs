use std::convert::TryFrom;
use std::net::SocketAddr;

use crate::relay::protocol::Response::{self, StoreRedirectResponse, StoreSuccessResponse};
use crate::relay::tcp::Connection;
use crate::relay::State::{DISCONNECTED, TERMINATED};
use crate::relay::{Exchange, Exchanges, Receiver, Sender, State};

pub(super) struct Broker {}

impl Broker {
    pub(super) async fn service<'a>(
        requests: &'a mut (Sender, Receiver),
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
                            break DISCONNECTED(endpoints.clone(), Exchanges::new())
                        }
                    }
                    None => break TERMINATED
                },
                result = connection.read() => match result.and_then(Result::ok).and_then(|bytes| Response::try_from(bytes).ok()) {
                    Some(response) => match response {
                        StoreSuccessResponse {} =>  exchanges.dequeue().responder().respond_with_success(),
                        StoreRedirectResponse {leader_address, position} => match leader_address {
                            Some(leader_address) if &leader_address != connection.endpoint() => {
                                Self::drain(connection, exchanges.split_off(1), requests.0.clone());
                                exchanges.requeue(position);
                                break DISCONNECTED(vec!(leader_address), exchanges)
                            }
                            _ => {
                                let exchange = exchanges.requeue(position);
                                if let Err(_) = exchange.write(&mut connection).await {
                                    exchanges.fail();
                                    break DISCONNECTED(endpoints.clone(), Exchanges::new())
                                }
                            }
                        }
                    },
                    None => {
                        exchanges.fail();
                        break DISCONNECTED(endpoints.clone(), Exchanges::new())
                    }
                }
            }
        }
    }

    fn drain(mut connection: Connection, mut exchanges: Exchanges, requests: Sender) {
        tokio::spawn(async move {
            // TODO: stop on shutdown ??? holding onto requests prevents Relay from terminating
            loop {
                match connection
                    .read()
                    .await
                    .and_then(Result::ok)
                    .and_then(|bytes| Response::try_from(bytes).ok())
                {
                    Some(response) => match response {
                        StoreSuccessResponse {} => exchanges.dequeue().responder().respond_with_success(),
                        StoreRedirectResponse {
                            leader_address: _,
                            position,
                        } => {
                            let Exchange(request, responder) = exchanges.dequeue();
                            requests
                                .send((request.with_position(position), responder))
                                // safety: client already dropped
                                .unwrap_or(())
                        }
                    },
                    None => break exchanges.fail(),
                }
            }
        });
    }
}

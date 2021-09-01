use std::net::SocketAddr;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::relay::protocol::Request;
use crate::relay::State::{CONNECTED, TERMINATED};
use crate::relay::{connect, Exchanges, Responder, State};

pub(super) struct Connector<'a> {
    requests: &'a mut mpsc::UnboundedReceiver<(Request, Responder)>,
    endpoints: Vec<SocketAddr>,
    exchanges: Exchanges,
}

impl<'a> Connector<'a> {
    pub(super) fn new(
        requests: &'a mut mpsc::UnboundedReceiver<(Request, Responder)>,
        endpoints: Vec<SocketAddr>,
        exchanges: Exchanges,
    ) -> Self {
        Connector {
            requests,
            endpoints,
            exchanges,
        }
    }

    pub(super) async fn run(mut self) -> State {
        let mut connections = Box::pin(connect(&self.endpoints));
        loop {
            tokio::select! {
                result = self.requests.recv() => match result {
                     Some((request, responder)) => {
                        self.exchanges.enqueue(request, responder);
                     }
                     None => return TERMINATED
                },
                connection = connections.next() => match connection {
                    Some(connection) => return CONNECTED(connection, self.exchanges),
                    None => return TERMINATED
                }
            }
        }
    }
}

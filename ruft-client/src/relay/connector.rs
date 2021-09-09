use std::net::SocketAddr;

use futures::StreamExt;

use crate::relay::State::{CONNECTED, TERMINATED};
use crate::relay::{connect, Exchanges, Receiver, State};

pub(super) struct Connector {}

impl Connector {
    pub(super) async fn connect(
        requests: &mut Receiver,
        endpoints: Vec<SocketAddr>,
        mut exchanges: Exchanges,
    ) -> State {
        tokio::pin! {
            let connections = connect(&endpoints);
        }

        loop {
            tokio::select! {
                result = requests.recv() => match result {
                     Some((request, responder)) => {
                        exchanges.enqueue(request, responder);
                     }
                     None => break TERMINATED
                },
                connection = connections.next() => match connection {
                    Some(connection) => break CONNECTED(connection, exchanges),
                    None => break TERMINATED
                }
            }
        }
    }
}

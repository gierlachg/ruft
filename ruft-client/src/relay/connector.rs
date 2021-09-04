use std::net::SocketAddr;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::relay::protocol::Request;
use crate::relay::State::{CONNECTED, TERMINATED};
use crate::relay::{connect, Exchanges, Responder, State};

pub(super) struct Connector {}

impl Connector {
    pub(super) async fn connect(
        requests: &mut mpsc::UnboundedReceiver<(Request, Responder)>,
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
                     None => return TERMINATED
                },
                connection = connections.next() => match connection {
                    Some(connection) => return CONNECTED(connection, exchanges),
                    None => return TERMINATED
                }
            }
        }
    }
}

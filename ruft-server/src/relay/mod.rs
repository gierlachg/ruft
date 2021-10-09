use std::convert::TryFrom;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;

use async_trait::async_trait;
use log::{error, trace};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::relay::protocol::{Request, Response};
use crate::relay::tcp::{Connection, Connections};
use crate::Shutdown;

pub(crate) mod protocol;
mod tcp;

type Error = Box<dyn std::error::Error + Send + Sync>;

type Sender = UnboundedSender<(Request, UnboundedSender<Response>)>;
type Receiver = UnboundedReceiver<(Request, UnboundedSender<Response>)>;

#[async_trait]
pub(crate) trait Relay {
    async fn requests(&mut self) -> Option<(Request, UnboundedSender<Response>)>;
}

pub(crate) struct PhysicalRelay {
    endpoint: SocketAddr,
    requests: Receiver,
}

impl PhysicalRelay {
    pub(crate) async fn init(endpoint: SocketAddr, shutdown: Shutdown) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let connections = Connections::bind(&endpoint).await?;

        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        tokio::spawn(Self::listen(connections, requests_tx, shutdown));
        Ok(PhysicalRelay {
            endpoint,
            requests: requests_rx,
        })
    }

    async fn listen(mut connections: Connections, requests: Sender, mut shutdown: Shutdown) {
        loop {
            tokio::select! {
                result = connections.next() => match result {
                    Ok(connection) => Self::on_connection(connection, requests.clone(), shutdown.clone()),
                    Err(e) => {
                        trace!("Error accepting connection; error = {:?}", e);
                        break
                    }
                },
                _ = shutdown.receive() => break
            }
        }
    }

    fn on_connection(mut connection: Connection, requests: Sender, mut shutdown: Shutdown) {
        tokio::spawn(async move {
            let (tx, mut rx) = mpsc::unbounded_channel();
            loop {
                tokio::select! {
                    result = connection.read() => match result {
                        Some(Ok(bytes)) => match Request::try_from(bytes.freeze()) {
                            Ok(request) => requests.send((request, tx.clone())).expect("This is unexpected!"),
                            Err(e) => break error!("Parsing error; error = {:?}. Closing {} connection.", e, &connection),
                        }
                        Some(Err(e)) => break error!("Communication error; error = {:?}. Closing {} connection.", e, &connection),
                        None => break trace!("{} connection closed by peer.", &connection),
                    },
                    result = rx.recv() => if let Err(e) = connection.write(result.expect("This is unexpected!").into()).await {
                        break error!("Unable to respond to {}; error = {:?}.", &connection, e)
                    },
                    _ = shutdown.receive() => break
                }
            }
        });
    }
}

#[async_trait]
impl Relay for PhysicalRelay {
    async fn requests(&mut self) -> Option<(Request, UnboundedSender<Response>)> {
        self.requests.recv().await
    }
}

impl Display for PhysicalRelay {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.endpoint)
    }
}

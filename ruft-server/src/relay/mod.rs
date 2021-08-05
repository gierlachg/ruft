use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;
use log::{error, trace};
use tokio::signal;
use tokio::sync::{mpsc, watch};

use crate::relay::protocol::{Request, Response};
use crate::relay::tcp::{Connection, Connections};

pub(crate) mod protocol;
mod tcp;

#[async_trait]
pub(crate) trait Relay {
    async fn requests(&mut self) -> Option<(Request, mpsc::UnboundedSender<Response>)>;
}

pub(crate) struct PhysicalRelay {
    endpoint: SocketAddr,
    requests: mpsc::UnboundedReceiver<(Bytes, mpsc::UnboundedSender<Response>)>,
}

impl PhysicalRelay {
    pub(crate) async fn init(endpoint: SocketAddr) -> Result<Self, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        let connections = Connections::bind(&endpoint).await?;

        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(Self::listen(connections, tx));
        Ok(PhysicalRelay { endpoint, requests: rx })
    }

    async fn listen(mut connections: Connections, tx: mpsc::UnboundedSender<(Bytes, mpsc::UnboundedSender<Response>)>) {
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        loop {
            tokio::select! {
                result = connections.next() => match result {
                    Ok(connection) => Self::on_connection(connection, tx.clone(), shutdown_rx.clone()),
                    Err(e) => {
                        trace!("Error accepting connection; error = {:?}", e);
                        break
                    }
                },
                _ = signal::ctrl_c() => break // TODO: dedup with cluster signal
            }
        }
    }

    fn on_connection(
        mut connection: Connection,
        requests: mpsc::UnboundedSender<(Bytes, mpsc::UnboundedSender<Response>)>,
        mut shutdown: watch::Receiver<()>,
    ) {
        tokio::spawn(async move {
            let (tx, mut rx) = mpsc::unbounded_channel();
            loop {
                tokio::select! {
                    result = connection.read() => match result {
                        Some(Ok(request)) => requests.send((request.freeze(), tx.clone())).expect("This is unexpected!"),
                        Some(Err(e)) => {
                            error!("Communication error; error = {:?}. Closing {} connection.", e, &connection.endpoint());
                            break
                        }
                        None => {
                            trace!("{} connection closed by peer.", &connection.endpoint());
                            break
                        }
                    },
                    result = rx.recv() => {
                        if let Err(e) = connection.write(result.expect("This is unexpected!").into()).await {
                            error!("Unable to respond to {}; error = {:?}.", &connection.endpoint(), e);
                            break
                        }
                    },
                    _ = shutdown.changed() => break
                }
            }
        });
    }
}

#[async_trait]
impl Relay for PhysicalRelay {
    async fn requests(&mut self) -> Option<(Request, mpsc::UnboundedSender<Response>)> {
        self.requests
            .recv()
            .await
            .map(|(bytes, responder)| (Request::from(bytes), responder))
    }
}

impl Display for PhysicalRelay {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.endpoint)
    }
}

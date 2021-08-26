use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

use async_trait::async_trait;
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
    requests: mpsc::UnboundedReceiver<(Request, mpsc::UnboundedSender<Response>)>,
}

impl PhysicalRelay {
    pub(crate) async fn init(endpoint: SocketAddr) -> Result<Self, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        let connections = Connections::bind(&endpoint).await?;

        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        tokio::spawn(Self::listen(connections, requests_tx));
        Ok(PhysicalRelay {
            endpoint,
            requests: requests_rx,
        })
    }

    async fn listen(
        mut connections: Connections,
        requests: mpsc::UnboundedSender<(Request, mpsc::UnboundedSender<Response>)>,
    ) {
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        loop {
            tokio::select! {
                result = connections.next() => match result {
                    Ok(connection) => Self::on_connection(connection, requests.clone(), shutdown_rx.clone()),
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
        requests: mpsc::UnboundedSender<(Request, mpsc::UnboundedSender<Response>)>,
        mut shutdown: watch::Receiver<()>,
    ) {
        tokio::spawn(async move {
            let (tx, mut rx) = mpsc::unbounded_channel();
            loop {
                tokio::select! {
                    result = connection.read() => match result {
                        Some(Ok(request)) => {
                            let request = Request::from(request.freeze());
                            requests.send((request, tx.clone())).expect("This is unexpected!");
                        }
                        Some(Err(e)) => {
                            error!("Communication error; error = {:?}. Closing {} connection.", e, &connection);
                            break
                        }
                        None => {
                            trace!("{} connection closed by peer.", &connection.endpoint());
                            break
                        }
                    },
                    result = rx.recv() => if let Err(e) = connection.write(result.expect("This is unexpected!").into()).await {
                        error!("Unable to respond {}; error = {:?}.", &connection, e);
                        break
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
        self.requests.recv().await
    }
}

impl Display for PhysicalRelay {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.endpoint)
    }
}

use std::fmt::{self, Debug, Display, Formatter};
use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{error, trace};

use crate::relay::tcp::{Connection, Connections};
use crate::Shutdown;

mod tcp;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[async_trait]
pub(crate) trait Relay<RQ: TryFrom<Bytes> + Send + Debug + 'static, RS: Into<Bytes> + Send + 'static> {
    async fn requests(&mut self) -> Option<(RQ, UnboundedSender<RS>)>;
}

pub(crate) struct PhysicalRelay<RQ: TryFrom<Bytes> + Send + Debug + 'static, RS: Into<Bytes> + Send + 'static> {
    endpoint: SocketAddr,
    requests: UnboundedReceiver<(RQ, UnboundedSender<RS>)>,
}

impl<RQ: TryFrom<Bytes> + Debug + Send + 'static, RS: Into<Bytes> + Send + 'static> PhysicalRelay<RQ, RS> {
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

    async fn listen(
        mut connections: Connections,
        requests: UnboundedSender<(RQ, UnboundedSender<RS>)>,
        mut shutdown: Shutdown,
    ) {
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

    fn on_connection(
        mut connection: Connection,
        requests: UnboundedSender<(RQ, UnboundedSender<RS>)>,
        mut shutdown: Shutdown,
    ) {
        tokio::spawn(async move {
            let (tx, mut rx) = mpsc::unbounded_channel();
            loop {
                tokio::select! {
                    result = connection.read() => match result {
                        Some(Ok(bytes)) => match RQ::try_from(bytes.freeze()) {
                            Ok(request) => requests.send((request, tx.clone())).expect("This is unexpected!"),
                            Err(_) => break error!("Parsing error. Closing {} connection.", &connection),
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
impl<RQ: TryFrom<Bytes> + Debug + Send, RS: Into<Bytes> + Send> Relay<RQ, RS> for PhysicalRelay<RQ, RS> {
    async fn requests(&mut self) -> Option<(RQ, UnboundedSender<RS>)> {
        self.requests.recv().await
    }
}

impl<RQ: TryFrom<Bytes> + Debug + Send, RS: Into<Bytes> + Send> Display for PhysicalRelay<RQ, RS> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.endpoint)
    }
}

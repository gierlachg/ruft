use std::error::Error;
use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;
use log::{error, trace};
use tokio::signal;
use tokio::sync::{mpsc, watch};

use crate::relay::protocol::Message;
use crate::relay::tcp::{Listener, Stream};
use std::fmt;
use std::fmt::{Display, Formatter};

pub(crate) mod protocol;
mod tcp;

#[async_trait]
pub(crate) trait Relay {
    async fn receive(&mut self) -> Option<(Message, mpsc::UnboundedSender<Message>)>;
}

pub(crate) struct PhysicalRelay {
    endpoint: SocketAddr,
    messages: mpsc::UnboundedReceiver<(Bytes, mpsc::UnboundedSender<Message>)>,
}

impl PhysicalRelay {
    pub(crate) async fn init(endpoint: SocketAddr) -> Result<Self, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        let mut listener = Listener::bind(&endpoint).await?;

        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let (_shutdown_tx, shutdown_rx) = watch::channel(());
            loop {
                tokio::select! {
                    result = listener.next() => {
                        match result {
                            Ok(stream) => Self::on_connection(stream, tx.clone(), shutdown_rx.clone()),
                            Err(e) => {
                                trace!("Error accepting connection; error = {:?}", e);
                                break;
                            }
                        }
                    }
                    _ = signal::ctrl_c() => break // TODO: dedup with cluster signal
                }
            }
        });
        Ok(PhysicalRelay { endpoint, messages: rx })
    }

    fn on_connection(
        mut stream: Stream,
        messages: mpsc::UnboundedSender<(Bytes, mpsc::UnboundedSender<Message>)>,
        mut shutdown: watch::Receiver<()>,
    ) {
        tokio::spawn(async move {
            let (tx, mut rx) = mpsc::unbounded_channel();
            loop {
                tokio::select! {
                    result = stream.read() => {
                        match result {
                            Some(Ok(message)) => messages.send((message.freeze(), tx.clone())).expect("This is unexpected!"),
                            Some(Err(e)) => {
                                error!("Communication error; error = {:?}. Closing {} connection.", e, &stream.endpoint());
                                break;
                            }
                            None => {
                                trace!("{} connection closed by peer.", &stream.endpoint());
                                break;
                            }
                        }
                    }
                    message = rx.recv() => {
                        if let Err(e) = stream.write(message.expect("This is unexpected!").into()).await {
                            error!("Unable to respond to {}; error = {:?}.", &stream.endpoint(), e);
                            break;
                        }
                    }
                    _ = shutdown.changed() => break,
                }
            }
        });
    }
}

#[async_trait]
impl Relay for PhysicalRelay {
    async fn receive(&mut self) -> Option<(Message, mpsc::UnboundedSender<Message>)> {
        self.messages
            .recv()
            .await
            .map(|(bytes, responder)| (Message::from(bytes), responder))
    }
}

impl Display for PhysicalRelay {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.endpoint)
    }
}

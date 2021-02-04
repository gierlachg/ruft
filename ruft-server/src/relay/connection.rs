use crate::relay::tcp::{Listener, Reader};
use bytes::Bytes;
use log::{error, trace};
use std::error::Error;
use std::net::SocketAddr;
use tokio::signal;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, watch};

pub(super) struct Ingress {
    events: UnboundedReceiver<Bytes>,
}

impl Ingress {
    pub(super) async fn bind(endpoint: SocketAddr) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut listener = Listener::bind(&endpoint).await?;

        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let (_shutdown_tx, shutdown_rx) = watch::channel(());
            loop {
                tokio::select! {
                    result = listener.next() => {
                        match result {
                            Ok(reader) => Self::on_connection(reader, tx.clone(), shutdown_rx.clone()),
                            Err(e) => {
                                trace!("Error accepting connection; error = {:?}", e);
                                break;
                            }
                        }
                    }
                    _result = signal::ctrl_c() => break // TODO: dedup with cluster signal
                }
            }
        });
        Ok(Ingress { events: rx })
    }

    fn on_connection(mut reader: Reader, sender: mpsc::UnboundedSender<Bytes>, mut shutdown: watch::Receiver<()>) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result =  reader.read() => {
                        match result {
                            Some(Ok(event)) => sender.send(event.freeze()).expect("This is unexpected!"),
                            Some(Err(e)) => {
                                error!("Communication error; error = {:?}. Closing {} connection.", e, &reader.endpoint());
                                break;
                            }
                            None => {
                                trace!("{} connection closed by peer.", &reader.endpoint());
                                break;
                            }
                        }
                    }
                    _result = shutdown.changed() => break,
                }
            }
        });
    }

    pub(super) async fn next(&mut self) -> Option<Bytes> {
        self.events.recv().await
    }
}

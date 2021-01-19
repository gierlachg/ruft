use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use derive_more::Display;
use log::{error, trace};
use tokio::signal;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time;
use tokio::time::Duration;

use crate::network::tcp::{Listener, Reader, Writer};
use crate::{Endpoint, Id};

// TODO: configurable
const RECONNECT_INTERVAL_MILLIS: u64 = 100;

#[derive(Display)]
#[display(fmt = "{}", endpoint)]
pub(super) struct Egress {
    endpoint: Endpoint,
    writer: Arc<Mutex<Option<Writer>>>,
}

impl Egress {
    pub(super) async fn connect(endpoint: Endpoint) -> Self {
        let writer = match Writer::connect(endpoint.address()).await {
            Ok(writer) => Arc::new(Mutex::new(Some(writer))),
            Err(_) => {
                let writer = Arc::new(Mutex::new(None));
                Self::reconnect(endpoint.clone(), writer.clone());
                writer
            }
        };
        Egress { endpoint, writer }
    }

    pub(super) fn id(&self) -> Id {
        self.endpoint.id()
    }

    pub(super) async fn send(&mut self, message: Bytes) {
        let mut holder = self.writer.lock().await;
        if let Some(writer) = holder.as_mut() {
            if let Err(_) = writer.write(message).await {
                *holder = None;
                Egress::reconnect(self.endpoint.clone(), self.writer.clone());
            }
        }
    }

    fn reconnect(endpoint: Endpoint, holder: Arc<Mutex<Option<Writer>>>) {
        tokio::spawn(async move {
            trace!("Trying reconnect to {}", &endpoint);
            loop {
                if let Ok(writer) = Writer::connect(endpoint.address()).await {
                    trace!("Connected {}", &endpoint);
                    holder.lock().await.replace(writer);
                    break;
                }
                time::sleep(Duration::from_millis(RECONNECT_INTERVAL_MILLIS)).await;
            }
        });
    }
}

#[derive(Display)]
#[display(fmt = "{} this", endpoint)]
pub(super) struct Ingress {
    endpoint: Endpoint,
    messages: UnboundedReceiver<Bytes>,
}

impl Ingress {
    pub(super) async fn bind(endpoint: Endpoint) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut listener = Listener::bind(endpoint.address()).await?;

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
                    _result = signal::ctrl_c() => break
                }
            }
        });
        Ok(Ingress { endpoint, messages: rx })
    }

    fn on_connection(mut reader: Reader, sender: mpsc::UnboundedSender<Bytes>, mut shutdown: watch::Receiver<()>) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result =  reader.read() => {
                        match result {
                            Some(Ok(message)) => sender.send(message.freeze()).expect("That is unexpected!"),
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
        self.messages.recv().await
    }
}

use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use derive_more::Display;
use tokio::sync::{mpsc, Mutex};
use tracing::{error, trace};

use crate::cluster::tcp::{Listener, Reader, Writer};
use crate::{Endpoint, Shutdown};

// TODO: configurable
const RECONNECT_INTERVAL_MILLIS: u64 = 100;

#[derive(Display)]
#[display(fmt = "{:?}", endpoint)]
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

    pub(super) async fn send(&self, message: Bytes) {
        let mut holder = self.writer.lock().await;
        if let Some(writer) = holder.as_mut() {
            if let Err(_) = writer.write(message).await {
                *holder = None;
                Self::reconnect(self.endpoint.clone(), self.writer.clone());
            }
        }
    }

    fn reconnect(endpoint: Endpoint, holder: Arc<Mutex<Option<Writer>>>) {
        tokio::spawn(async move {
            trace!("Trying reconnect to {:?}", endpoint);
            loop {
                if let Ok(writer) = Writer::connect(endpoint.address()).await {
                    trace!("Connected {:?}", endpoint);
                    holder.lock().await.replace(writer);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(RECONNECT_INTERVAL_MILLIS)).await;
            }
        });
    }

    pub(super) fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

#[derive(Display)]
#[display(fmt = "{:?} this", endpoint)]
pub(super) struct Ingress<M: TryFrom<Bytes> + Send + Debug + 'static> {
    endpoint: Endpoint,
    messages: tokio::sync::mpsc::UnboundedReceiver<M>,
}

impl<M: TryFrom<Bytes> + Send + Debug + 'static> Ingress<M> {
    pub(super) async fn bind(endpoint: Endpoint, shutdown: Shutdown) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let listener = Listener::bind(endpoint.address()).await?;

        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(Self::listen(listener, tx, shutdown));
        Ok(Ingress { endpoint, messages: rx })
    }

    async fn listen(mut listener: Listener, messages: mpsc::UnboundedSender<M>, mut shutdown: Shutdown) {
        loop {
            tokio::select! {
                result = listener.next() => match result {
                    Ok(reader) => Self::on_connection(reader, messages.clone(), shutdown.clone()),
                    Err(e) => break trace!("Error accepting connection; error = {:?}", e),
                },
                _ = shutdown.receive() => break
            }
        }
    }

    fn on_connection(mut reader: Reader, messages: mpsc::UnboundedSender<M>, mut shutdown: Shutdown) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = reader.read() => match result {
                        Some(Ok(bytes)) => match M::try_from(bytes.freeze()) {
                            Ok(message) => messages.send(message).expect("This is unexpected!"),
                            Err(_) => break error!("Parsing error. Closing {} connection.", &reader),
                        },
                        Some(Err(e)) => break error!("Communication error; error = {:?}. Closing {} connection.", e, &reader),
                        None => break trace!("{} connection closed by peer.", &reader),
                    },
                    _ = shutdown.receive() => break
                }
            }
        });
    }

    pub(super) async fn next(&mut self) -> Option<M> {
        self.messages.recv().await
    }

    pub(super) fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

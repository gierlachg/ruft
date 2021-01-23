use std::error::Error;
use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use log::{error, trace};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, watch};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[async_trait]
pub(crate) trait Relay {
    async fn receive(&mut self) -> Option<Bytes>;
}

pub(crate) struct PhysicalRelay {
    ingress: Ingress,
}

impl PhysicalRelay {
    pub(crate) async fn init(client_endpoint: SocketAddr) -> Result<Self, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        let ingress = Ingress::bind(client_endpoint).await?;

        Ok(PhysicalRelay { ingress })
    }
}

#[async_trait]
impl Relay for PhysicalRelay {
    async fn receive(&mut self) -> Option<Bytes> {
        self.ingress.next().await
    }
}

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

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;

struct Listener {
    listener: TcpListener,
}

impl Listener {
    async fn bind(endpoint: &SocketAddr) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(&endpoint).await?;
        Ok(Listener { listener })
    }

    async fn next(&mut self) -> Result<Reader, Box<dyn std::error::Error + Send + Sync>> {
        let (stream, endpoint) = self.listener.accept().await?;
        let reader = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_framed(stream);
        Ok(Reader { endpoint, reader })
    }
}

struct Reader {
    endpoint: SocketAddr,
    reader: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Reader {
    fn endpoint(&self) -> &SocketAddr {
        &self.endpoint
    }

    async fn read(&mut self) -> Option<Result<BytesMut, std::io::Error>> {
        self.reader.next().await
    }
}

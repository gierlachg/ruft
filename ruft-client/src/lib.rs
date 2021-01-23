use std::net::SocketAddr;

use bytes::Bytes;
use futures::SinkExt;
use log::info;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const VERSION: &str = env!("CARGO_PKG_VERSION");

type Result<T> = std::result::Result<T, RuftClientError>;

#[derive(Error, Debug)]
pub enum RuftClientError {
    #[error("error")]
    GenericFailure(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("network error")]
    GenericNetworkFailure(#[from] std::io::Error),
}

pub struct RuftClient {
    cluster: Cluster,
}

impl RuftClient {
    pub async fn new<E>(endpoints: E) -> Result<Self>
    where
        E: IntoIterator<Item = SocketAddr>,
    {
        info!("Initializing Ruft client (version: {})", VERSION);
        let cluster = Cluster::init(endpoints).await?;

        Ok(RuftClient { cluster })
    }

    pub async fn store(&mut self, event: Bytes) {
        self.cluster.store(event).await
    }
}

struct Cluster {
    leader: Option<Writer>,
}

impl Cluster {
    async fn init<E>(endpoints: E) -> Result<Self>
    where
        E: IntoIterator<Item = SocketAddr>,
    {
        let leader = match endpoints.into_iter().next() {
            Some(endpoint) => Some(Writer::connect(&endpoint).await?),
            None => None,
        };

        Ok(Cluster { leader })
    }

    async fn store(&mut self, event: Bytes) {
        if let Some(leader) = self.leader.as_mut() {
            leader.write(event).await.unwrap();
        }
    }
}

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;

struct Writer {
    writer: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Writer {
    async fn connect(endpoint: &SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(endpoint).await?;
        let writer = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_framed(stream);

        Ok(Writer { writer })
    }

    async fn write(&mut self, event: Bytes) -> Result<()> {
        Ok(self.writer.send(event).await?)
    }
}

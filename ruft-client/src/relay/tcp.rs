use std::net::SocketAddr;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::Result;

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;

pub(super) struct Connection {
    endpoint: SocketAddr,
    stream: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Connection {
    pub(super) async fn connect(endpoint: &SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(endpoint).await?;
        let stream = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_framed(stream);
        Ok(Connection {
            endpoint: *endpoint,
            stream,
        })
    }
}

impl Connection {
    pub(crate) fn endpoint(&self) -> &SocketAddr {
        &self.endpoint
    }

    pub(crate) async fn write(&mut self, message: Bytes) -> Result<()> {
        Ok(self.stream.send(message).await?)
    }

    pub(crate) async fn read(&mut self) -> Option<Result<Bytes>> {
        self.stream.next().await.map(|result| match result {
            Ok(bytes) => Ok(bytes.freeze()),
            Err(e) => Err(e.into()),
        })
    }
}

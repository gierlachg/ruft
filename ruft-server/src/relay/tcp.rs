use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;

pub(super) struct Listener {
    listener: TcpListener,
}

impl Listener {
    pub(super) async fn bind(endpoint: &SocketAddr) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(&endpoint).await?;
        Ok(Listener { listener })
    }

    pub(super) async fn next(&mut self) -> Result<Stream, Box<dyn std::error::Error + Send + Sync>> {
        let (stream, endpoint) = self.listener.accept().await?;
        let stream = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_framed(stream);
        Ok(Stream { endpoint, stream })
    }
}

pub(super) struct Stream {
    endpoint: SocketAddr,
    stream: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Stream {
    pub(super) fn endpoint(&self) -> &SocketAddr {
        &self.endpoint
    }

    pub(super) async fn write(&mut self, message: Bytes) -> Result<(), std::io::Error> {
        self.stream.send(message).await
    }

    pub(super) async fn read(&mut self) -> Option<Result<BytesMut, std::io::Error>> {
        self.stream.next().await
    }
}

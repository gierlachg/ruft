use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;

pub(crate) struct Writer {
    writer: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Writer {
    pub(crate) async fn connect(endpoint: &SocketAddr) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let stream = TcpStream::connect(endpoint).await?;
        let writer = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_framed(stream);

        Ok(Writer { writer })
    }

    pub(crate) async fn write(&mut self, message: Bytes) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.writer.send(message).await?)
    }
}

pub(crate) struct Listener {
    listener: TcpListener,
}

impl Listener {
    pub(crate) async fn bind(endpoint: &SocketAddr) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(&endpoint).await?;
        Ok(Listener { listener })
    }

    pub(crate) async fn next(&mut self) -> Result<Reader, Box<dyn std::error::Error + Send + Sync>> {
        let (stream, endpoint) = self.listener.accept().await?;
        let reader = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_framed(stream);
        Ok(Reader { endpoint, reader })
    }
}

pub(crate) struct Reader {
    endpoint: SocketAddr,
    reader: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Reader {
    pub(crate) fn endpoint(&self) -> &SocketAddr {
        &self.endpoint
    }

    pub(crate) async fn read(&mut self) -> Option<Result<BytesMut, std::io::Error>> {
        self.reader.next().await
    }
}
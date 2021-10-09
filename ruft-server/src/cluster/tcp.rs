use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;

type Error = Box<dyn std::error::Error + Send + Sync>;

pub(crate) struct Writer {
    writer: FramedWrite<TcpStream, LengthDelimitedCodec>,
}

impl Writer {
    pub(crate) async fn connect(endpoint: &SocketAddr) -> Result<Self, Error> {
        let stream = TcpStream::connect(endpoint).await?;
        let writer = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_write(stream);
        Ok(Writer { writer })
    }

    pub(crate) async fn write(&mut self, message: Bytes) -> Result<(), Error> {
        Ok(self.writer.send(message).await?)
    }
}

pub(crate) struct Listener {
    listener: TcpListener,
}

impl Listener {
    pub(crate) async fn bind(endpoint: &SocketAddr) -> Result<Self, Error> {
        let listener = TcpListener::bind(&endpoint).await?;
        Ok(Listener { listener })
    }

    pub(crate) async fn next(&mut self) -> Result<Reader, Error> {
        let (stream, endpoint) = self.listener.accept().await?;
        let reader = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_read(stream);
        Ok(Reader { endpoint, reader })
    }
}

pub(crate) struct Reader {
    endpoint: SocketAddr,
    reader: FramedRead<TcpStream, LengthDelimitedCodec>,
}

impl Reader {
    pub(crate) async fn read(&mut self) -> Option<Result<BytesMut, std::io::Error>> {
        self.reader.next().await
    }
}

impl Display for &Reader {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.endpoint)
    }
}

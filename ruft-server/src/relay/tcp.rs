use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::relay::Error;

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;

pub(super) struct Connections {
    listener: TcpListener,
}

impl Connections {
    pub(super) async fn bind(endpoint: &SocketAddr) -> Result<Self, Error> {
        let listener = TcpListener::bind(&endpoint).await?;
        Ok(Connections { listener })
    }

    pub(super) async fn next(&mut self) -> Result<Connection, Error> {
        let (stream, endpoint) = self.listener.accept().await?;
        let stream = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_framed(stream);
        Ok(Connection { endpoint, stream })
    }
}

pub(super) struct Connection {
    endpoint: SocketAddr,
    stream: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Connection {
    pub(super) async fn write(&mut self, message: Bytes) -> Result<(), std::io::Error> {
        self.stream.send(message).await
    }

    pub(super) async fn read(&mut self) -> Option<Result<BytesMut, std::io::Error>> {
        self.stream.next().await
    }
}

impl Display for &Connection {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.endpoint)
    }
}

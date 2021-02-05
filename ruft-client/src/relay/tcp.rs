use std::net::SocketAddr;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::Result;

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;

pub(super) struct Stream {
    stream: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Stream {
    pub(super) async fn connect(endpoint: &SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(endpoint).await?;
        let stream = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_framed(stream);

        Ok(Stream { stream })
    }

    pub(super) async fn write(&mut self, message: Bytes) -> Result<()> {
        Ok(self.stream.send(message).await?)
    }

    pub(super) async fn read(&mut self) -> Option<Result<Bytes>> {
        self.stream.next().await.map(|result| match result {
            Ok(bytes) => Ok(bytes.freeze()),
            Err(e) => Err(e.into()),
        })
    }
}

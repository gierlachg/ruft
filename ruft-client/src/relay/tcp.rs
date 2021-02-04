use std::net::SocketAddr;

use bytes::Bytes;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::Result;

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;

pub(super) struct Writer {
    writer: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Writer {
    pub(super) async fn connect(endpoint: &SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(endpoint).await?;
        let writer = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_framed(stream);

        Ok(Writer { writer })
    }

    pub(super) async fn write(&mut self, message: Bytes) -> Result<()> {
        Ok(self.writer.send(message).await?)
    }
}

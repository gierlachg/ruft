use std::net::SocketAddr;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::Result;

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;

pub(super) struct Stream {
    stream: TcpStream,
}

impl Stream {
    pub(super) async fn connect(endpoint: &SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(endpoint).await?;
        Ok(Stream { stream })
    }

    pub(crate) fn split(&mut self) -> (Writer, Reader) {
        let (reader, writer) = self.stream.split();

        let writer = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_write(writer);
        let reader = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .little_endian()
            .new_read(reader);

        (Writer { writer }, Reader { reader })
    }
}

pub(crate) struct Writer<'a> {
    writer: FramedWrite<WriteHalf<'a>, LengthDelimitedCodec>,
}

impl<'a> Writer<'a> {
    pub(crate) async fn write(&mut self, message: Bytes) -> Result<()> {
        Ok(self.writer.send(message).await?)
    }
}

pub(crate) struct Reader<'a> {
    reader: FramedRead<ReadHalf<'a>, LengthDelimitedCodec>,
}

impl<'a> Reader<'a> {
    pub(crate) async fn read(&mut self) -> Option<Result<Bytes>> {
        self.reader.next().await.map(|result| match result {
            Ok(bytes) => Ok(bytes.freeze()),
            Err(e) => Err(e.into()),
        })
    }
}

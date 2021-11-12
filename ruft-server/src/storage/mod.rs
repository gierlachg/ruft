use std::error::Error;
use std::future::Future;
use std::num::NonZeroU64;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use tokio_stream::Stream;

use crate::storage::file::{FileLog, FileState};
use crate::{Payload, Position};

pub(crate) mod file;
pub(crate) mod memory;

pub(crate) async fn init(directory: impl AsRef<Path>) -> Result<(FileState, FileLog), Box<dyn Error + Send + Sync>> {
    match tokio::fs::metadata(directory.as_ref()).await {
        Ok(metadata) if metadata.is_dir() => {} // TODO: check none or both files are there ?
        Ok(_) => panic!("Path is not a directory"),
        Err(_) => tokio::fs::create_dir(&directory).await?,
    }

    let state = FileState::init(&directory);
    let log = FileLog::init(&directory).await?;
    Ok((state, log))
}

#[async_trait]
pub(crate) trait State {
    async fn load(&self) -> Option<u64>;

    async fn store(&mut self, term: u64);
}

#[async_trait]
pub(crate) trait Log: Sync {
    fn head(&self) -> &Position;

    async fn extend(&mut self, term: NonZeroU64, entries: Vec<Payload>) -> Position;

    async fn insert(
        &mut self,
        preceding: &Position,
        term: NonZeroU64,
        entries: Vec<Payload>,
    ) -> Result<Position, Position>;

    async fn at(&self, position: Position) -> Option<(Position, Position, Payload)>;

    async fn next(&self, position: Position) -> Option<(Position, Position, Payload)>;

    fn stream(&self) -> Entries
    where
        Self: Sized,
    {
        Entries {
            log: self,
            future: Some(Box::pin(self.next(Position::initial()))),
        }
    }
}

pub(crate) struct Entries<'a> {
    log: &'a dyn Log,
    future: Option<Pin<Box<(dyn Future<Output = Option<(Position, Position, Payload)>> + Send + 'a)>>>,
}

impl<'a> Stream for Entries<'a> {
    type Item = (Position, Payload);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.future.as_mut() {
            Some(future) => match future.as_mut().poll(cx) {
                Poll::Ready(result) => match result {
                    Some((_, position, entry)) => {
                        let future = Box::pin(self.log.next(position));
                        self.future.replace(future);
                        Poll::Ready(Some((position, entry)))
                    }
                    None => {
                        self.future.take();
                        Poll::Ready(None)
                    }
                },
                Poll::Pending => Poll::Pending,
            },
            None => Poll::Ready(None),
        }
    }
}

impl<'a> Entries<'a> {
    pub(crate) fn skip(mut self, position: &Position) -> Self {
        self.future = Some(Box::pin(self.log.next(*position)));
        self
    }
}

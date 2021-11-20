use std::error::Error;
use std::num::NonZeroU64;
use std::path::Path;

use async_trait::async_trait;
use tokio_stream::Stream;

use crate::storage::file::{FileLog, FileState};
use crate::{Payload, Position};

mod file;
mod memory;

pub(crate) async fn init(directory: impl AsRef<Path>) -> Result<(impl State, impl Log), Box<dyn Error + Send + Sync>> {
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

    fn entries(&self, from: Position) -> Box<dyn Entries + '_>;
}

pub(crate) trait Entries = Stream<Item = (Position, Payload)> + Send;

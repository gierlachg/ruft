use std::io;
use std::ops::Deref;
use std::path::Path;

use async_trait::async_trait;
use tokio_stream::StreamExt;

use crate::storage::file::{FileLog, FileState};
use crate::{Payload, Position};

pub(crate) mod file;
pub(crate) mod memory;

pub(crate) async fn init(directory: impl AsRef<Path>) -> (FileState, FileLog) {
    match tokio::fs::metadata(directory.as_ref()).await {
        Ok(metadata) if metadata.is_dir() => {} // TODO: check none or both files are there ?
        Ok(_) => panic!("Path is not a directory"),
        Err(_) => tokio::fs::create_dir(&directory).await.unwrap(),
    }

    let state = FileState::init(&directory);
    let log = FileLog::init(&directory).await;
    (state, log)
}

#[async_trait]
pub(crate) trait State {
    async fn load(&self) -> Option<u64>;

    async fn store(&mut self, term: u64);
}

#[async_trait]
pub(crate) trait Log {
    fn head(&self) -> &Position;

    async fn extend(&mut self, term: u64, entries: Vec<Payload>) -> Position;

    async fn insert(&mut self, preceding: &Position, term: u64, entries: Vec<Payload>) -> Result<Position, Position>;

    async fn at<'a>(&self, position: &'a Position) -> Option<(Position, &'a Position, Payload)>;

    async fn next<'a>(&self, position: &'a Position) -> Option<(&'a Position, Position, Payload)>;
}

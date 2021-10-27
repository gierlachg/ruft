use async_trait::async_trait;

use crate::{Payload, Position};

pub(crate) mod file;
pub(crate) mod memory;

pub(crate) fn noop_message() -> Payload {
    Payload::from_static(&[])
}

#[async_trait]
pub(crate) trait Log {
    fn head(&self) -> &Position;

    async fn extend(&mut self, term: u64, entries: Vec<Payload>) -> Position;

    async fn insert(&mut self, preceding: &Position, term: u64, entries: Vec<Payload>) -> Result<Position, Position>;

    async fn at<'a>(&self, position: &'a Position) -> Option<(Position, &'a Position, Payload)>;

    async fn next<'a>(&self, position: &'a Position) -> Option<(&'a Position, Position, Payload)>;
}

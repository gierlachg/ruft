use async_trait::async_trait;

use crate::{Id, Payload, Position};

pub(crate) mod file;
pub(crate) mod memory;

#[async_trait]
pub(crate) trait State {
    async fn load(&self) -> (u64, Option<Id>);

    async fn store(&mut self, term: u64, votee: Option<Id>);
}

#[async_trait]
pub(crate) trait Log {
    fn head(&self) -> &Position;

    async fn extend(&mut self, term: u64, entries: Vec<Payload>) -> Position;

    async fn insert(&mut self, preceding: &Position, term: u64, entries: Vec<Payload>) -> Result<Position, Position>;

    async fn at<'a>(&self, position: &'a Position) -> Option<(Position, &'a Position, Payload)>;

    async fn next<'a>(&self, position: &'a Position) -> Option<(&'a Position, Position, Payload)>;
}

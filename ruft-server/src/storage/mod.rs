use std::cmp::Ordering;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub(crate) mod volatile;

pub(crate) fn noop_message() -> Bytes {
    Bytes::from_static(&[])
}

#[async_trait]
pub(crate) trait Storage {
    fn head(&self) -> &Position;

    async fn extend(&mut self, term: u64, entries: Vec<Bytes>) -> Position;

    async fn insert(
        &mut self,
        preceding_position: &Position,
        term: u64,
        entries: Vec<Bytes>,
    ) -> Result<Position, Position>;

    #[allow(clippy::needless_lifetimes)]
    async fn at<'a>(&'a self, position: &Position) -> Option<(&'a Position, &'a Bytes)>;

    #[allow(clippy::needless_lifetimes)]
    async fn next<'a>(&'a self, preceding_position: &Position) -> Option<(&'a Position, &'a Bytes)>;
}

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Position(u64, u64);

impl Position {
    pub(crate) fn of(term: u64, index: u64) -> Self {
        Position(term, index)
    }

    pub(crate) fn term(&self) -> u64 {
        self.0
    }

    pub(crate) fn index(&self) -> u64 {
        self.1
    }

    fn next_in(&self, term: u64) -> Self {
        if self.term() == term {
            Position::of(term, self.index() + 1)
        } else {
            Position::of(term, 0)
        }
    }
}

impl<'a> PartialEq<Position> for &'a Position {
    fn eq(&self, other: &Position) -> bool {
        *self == other
    }
}

impl Ord for Position {
    fn cmp(&self, other: &Self) -> Ordering {
        self.term().cmp(&other.term()).then(self.index().cmp(&other.index()))
    }
}

impl PartialOrd for Position {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

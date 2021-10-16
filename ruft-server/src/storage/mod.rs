use std::cmp::Ordering;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::Payload;

pub(crate) mod volatile;

pub(crate) fn noop_message() -> Payload {
    Payload::from_static(&[])
}

#[async_trait]
pub(crate) trait Storage {
    fn head(&self) -> &Position;

    async fn extend(&mut self, term: u64, entries: Vec<Payload>) -> Position;

    async fn insert(
        &mut self,
        preceding_position: &Position,
        term: u64,
        entries: Vec<Payload>,
    ) -> Result<Position, Position>;

    async fn at(&self, position: &Position) -> Option<(&Position, &Payload)>;

    async fn next(&self, position: &Position) -> Option<(&Position, &Payload)>;
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

    pub(crate) fn next(&self) -> Self {
        Position::of(self.0, self.index() + 1)
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

impl PartialOrd for Position {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Position {
    fn cmp(&self, other: &Self) -> Ordering {
        self.term().cmp(&other.term()).then(self.index().cmp(&other.index()))
    }
}

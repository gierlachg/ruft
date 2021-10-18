use async_trait::async_trait;

use crate::{Payload, Position};

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

    async fn at<'a, 'b>(&'a self, position: &'b Position) -> Option<(&'a Position, &'b Position, &'a Payload)>;

    async fn next<'a, 'b>(&'a self, position: &'b Position) -> Option<(&'b Position, &'a Position, &'a Payload)>;
}

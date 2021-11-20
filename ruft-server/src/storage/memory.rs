use std::collections::BTreeMap;
use std::num::NonZeroU64;

use async_trait::async_trait;

use crate::storage::{Entries, Log};
use crate::{Payload, Position};

pub(crate) struct MemoryLog {
    entries: BTreeMap<Position, Payload>,
}

impl MemoryLog {
    pub(crate) fn _init() -> Self {
        let mut entries = BTreeMap::new();
        entries.insert(Position::initial(), Payload::empty());

        MemoryLog { entries }
    }
}

#[async_trait]
impl Log for MemoryLog {
    fn head(&self) -> &Position {
        match self.entries.iter().next_back() {
            Some((position, _)) => position,
            None => unreachable!("{:?}", self.entries),
        }
    }

    async fn extend(&mut self, term: NonZeroU64, entries: Vec<Payload>) -> Position {
        let mut head = *self.head();
        let mut next;
        for entry in entries {
            next = head.next_in(term);
            self.entries.insert(next, entry);
            head = next;
        }
        head
    }

    async fn insert(
        &mut self,
        preceding: &Position,
        term: NonZeroU64,
        entries: Vec<Payload>,
    ) -> Result<Position, Position> {
        if let Some(position) = self
            .entries
            .range(preceding..)
            .into_iter()
            .skip_while(|(position, _)| position == preceding)
            .next()
            .map(|(position, _)| *position)
        {
            self.entries.split_off(&position);
        }

        let head = self.head();
        if head == preceding {
            Ok(self.extend(term, entries).await)
        } else if head.term() == preceding.term() {
            Err(head.next())
        } else {
            Err(*preceding)
        }
    }

    async fn at(&self, needle: Position) -> Option<(Position, Position, Payload)> {
        self.entries
            .range(..needle)
            .next_back()
            .map(|(position, _)| position)
            .zip(self.entries.get(&needle))
            .map(|(position, entry)| (*position, needle, entry.clone()))
    }

    async fn next(&self, needle: Position) -> Option<(Position, Position, Payload)> {
        self.entries
            .range(needle.next()..)
            .into_iter()
            .next()
            .map(|(position, entry)| (needle, *position, entry.clone()))
    }

    fn entries(&self, from: Position) -> Box<dyn Entries + '_> {
        let iterator = self
            .entries
            .range(from.next()..)
            .into_iter()
            .map(|(position, entry)| (position.clone(), entry.clone()));
        Box::new(tokio_stream::iter(iterator))
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use tokio_stream::StreamExt;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn when_created_then_initialized() {
        let storage = MemoryLog::_init();

        assert_eq!(storage.head(), &Position::of(0, 0));

        assert_eq!(storage.at(Position::of(0, 0)).await, None);
        assert_eq!(storage.at(Position::of(0, 1)).await, None);
        assert_eq!(storage.at(Position::of(1, 0)).await, None);

        assert_eq!(storage.next(Position::of(0, 0)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_empty_entries_appended_then_succeeds() {
        let mut storage = MemoryLog::_init();

        assert_eq!(
            storage.extend(NonZeroU64::new(1).unwrap(), vec![]).await,
            Position::of(0, 0)
        );

        assert_eq!(storage.head(), &Position::of(0, 0));

        assert_eq!(storage.at(Position::of(1, 0)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entries_appended_then_succeeds() {
        let mut storage = MemoryLog::_init();

        assert_eq!(
            storage.extend(NonZeroU64::new(1).unwrap(), entries(1)).await,
            Position::of(1, 0)
        );
        assert_eq!(
            storage.extend(NonZeroU64::new(1).unwrap(), entries(2)).await,
            Position::of(1, 1)
        );
        assert_eq!(
            storage.extend(NonZeroU64::new(2).unwrap(), entries(3)).await,
            Position::of(2, 0)
        );

        assert_eq!(storage.head(), Position::of(2, 0));

        assert_eq!(
            storage.at(Position::of(1, 0)).await,
            Some((Position::of(0, 0), Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            storage.at(Position::of(1, 1)).await,
            Some((Position::of(1, 0), Position::of(1, 1), bytes(2)))
        );
        assert_eq!(storage.at(Position::of(1, 2)).await, None);
        assert_eq!(
            storage.at(Position::of(2, 0)).await,
            Some((Position::of(1, 1), Position::of(2, 0), bytes(3)))
        );
        assert_eq!(storage.at(Position::of(2, 1)).await, None);
        assert_eq!(storage.at(Position::of(3, 0)).await, None);

        assert_eq!(
            storage.next(Position::of(0, 0)).await,
            Some((Position::of(0, 0), Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            storage.next(Position::of(1, 0)).await,
            Some((Position::of(1, 0), Position::of(1, 1), bytes(2)))
        );
        assert_eq!(
            storage.next(Position::of(1, 1)).await,
            Some((Position::of(1, 1), Position::of(2, 0), bytes(3)))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_and_preceding_present_then_succeeds() {
        let mut storage = MemoryLog::_init();

        assert_eq!(
            storage
                .insert(&Position::of(0, 0), NonZeroU64::new(1).unwrap(), entries(1))
                .await,
            Ok(Position::of(1, 0))
        );
        assert_eq!(
            storage
                .insert(&Position::of(1, 0), NonZeroU64::new(1).unwrap(), entries(2))
                .await,
            Ok(Position::of(1, 1))
        );
        assert_eq!(
            storage
                .insert(&Position::of(1, 1), NonZeroU64::new(2).unwrap(), entries(3))
                .await,
            Ok(Position::of(2, 0))
        );

        assert_eq!(storage.head(), &Position::of(2, 0));

        assert_eq!(
            storage.at(Position::of(1, 0)).await,
            Some((Position::of(0, 0), Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            storage.at(Position::of(1, 1)).await,
            Some((Position::of(1, 0), Position::of(1, 1), bytes(2)))
        );
        assert_eq!(storage.at(Position::of(1, 2)).await, None);
        assert_eq!(
            storage.at(Position::of(2, 0)).await,
            Some((Position::of(1, 1), Position::of(2, 0), bytes(3)))
        );
        assert_eq!(storage.at(Position::of(2, 1)).await, None);
        assert_eq!(storage.at(Position::of(3, 0)).await, None);

        assert_eq!(
            storage.next(Position::of(0, 0)).await,
            Some((Position::of(0, 0), Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            storage.next(Position::of(1, 0)).await,
            Some((Position::of(1, 0), Position::of(1, 1), bytes(2)))
        );
        assert_eq!(
            storage.next(Position::of(1, 1)).await,
            Some((Position::of(1, 1), Position::of(2, 0), bytes(3)))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_but_preceding_term_missing_then_fails() {
        let mut storage = MemoryLog::_init();

        assert_eq!(
            storage
                .insert(&Position::of(5, 0), NonZeroU64::new(10).unwrap(), entries(1))
                .await,
            Err(Position::of(5, 0))
        );

        assert_eq!(storage.head(), &Position::of(0, 0));

        assert_eq!(storage.at(Position::of(5, 0)).await, None);
        assert_eq!(storage.at(Position::of(5, 1)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_but_preceding_index_missing_then_fails() {
        let mut storage = MemoryLog::_init();

        storage.extend(NonZeroU64::new(5).unwrap(), entries(1)).await;
        assert_eq!(
            storage
                .insert(&Position::of(5, 5), NonZeroU64::new(5).unwrap(), entries(2))
                .await,
            Err(Position::of(5, 1))
        );

        assert_eq!(storage.head(), &Position::of(5, 0));

        assert_eq!(storage.at(Position::of(5, 1)).await, None);
        assert_eq!(storage.at(Position::of(5, 6)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_in_the_middle_then_subsequent_entries_are_removed() {
        let mut storage = MemoryLog::_init();

        storage
            .extend(
                NonZeroU64::new(5).unwrap(),
                vec![Payload::from(vec![1]), Payload::from(vec![2])],
            )
            .await;
        storage.extend(NonZeroU64::new(10).unwrap(), entries(3)).await;

        assert_eq!(
            storage
                .insert(&Position::of(5, 0), NonZeroU64::new(5).unwrap(), entries(4))
                .await,
            Ok(Position::of(5, 1))
        );

        assert_eq!(storage.head(), &Position::of(5, 1));

        assert_eq!(
            storage.at(Position::of(5, 0)).await,
            Some((Position::of(0, 0), Position::of(5, 0), bytes(1)))
        );
        assert_eq!(
            storage.at(Position::of(5, 1)).await,
            Some((Position::of(5, 0), Position::of(5, 1), bytes(4)))
        );
        assert_eq!(storage.at(Position::of(5, 2)).await, None);
        assert_eq!(storage.at(Position::of(10, 0)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_next() {
        let mut storage = MemoryLog::_init();

        assert_eq!(
            storage.extend(NonZeroU64::new(10).unwrap(), entries(100)).await,
            Position::of(10, 0)
        );

        assert_eq!(
            storage.next(Position::of(0, 0)).await,
            Some((Position::of(0, 0), Position::of(10, 0), bytes(100)))
        );
        assert_eq!(
            storage.next(Position::of(0, 100)).await,
            Some((Position::of(0, 100), Position::of(10, 0), bytes(100)))
        );
        assert_eq!(
            storage.next(Position::of(5, 5)).await,
            Some((Position::of(5, 5), Position::of(10, 0), bytes(100)))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_entries() {
        let mut storage = MemoryLog::_init();

        storage.extend(NonZeroU64::new(1).unwrap(), entries(1)).await;
        storage.extend(NonZeroU64::new(1).unwrap(), entries(2)).await;
        storage.extend(NonZeroU64::new(10).unwrap(), entries(10)).await;

        let entries = Pin::from(storage.entries(Position::of(0, 0))).collect::<Vec<_>>().await;
        assert_eq!(
            entries,
            vec![
                (Position::of(1, 0), bytes(1)),
                (Position::of(1, 1), bytes(2)),
                (Position::of(10, 0), bytes(10))
            ]
        );

        let entries = Pin::from(storage.entries(Position::of(1, 1))).collect::<Vec<_>>().await;
        assert_eq!(entries, vec![(Position::of(10, 0), bytes(10))]);

        let entries = Pin::from(storage.entries(Position::of(100, 100)))
            .collect::<Vec<_>>()
            .await;
        assert!(entries.is_empty());
    }

    fn entries(value: u8) -> Vec<Payload> {
        vec![bytes(value)]
    }

    fn bytes(value: u8) -> Payload {
        Payload::from(vec![value])
    }
}

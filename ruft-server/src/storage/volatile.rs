use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use async_trait::async_trait;

use crate::storage::{noop_message, Position, Storage};
use crate::Payload;

pub(crate) struct VolatileStorage {
    entries: BTreeMap<Position, Payload>,
}

impl VolatileStorage {
    pub(crate) fn init() -> Self {
        let mut entries = BTreeMap::new();
        entries.insert(Position::of(0, 0), noop_message());

        VolatileStorage { entries }
    }
}

#[async_trait]
impl Storage for VolatileStorage {
    fn head(&self) -> &Position {
        match self.entries.iter().next_back() {
            Some((position, _)) => position,
            None => unreachable!("{:?}", self.entries),
        }
    }

    async fn extend(&mut self, term: u64, entries: Vec<Payload>) -> Position {
        assert!(term > 0);

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
        preceding_position: &Position,
        term: u64,
        entries: Vec<Payload>,
    ) -> Result<Position, Position> {
        assert!(term > 0);

        if let Some(position) = self
            .entries
            .range(preceding_position..)
            .into_iter()
            .skip_while(|(position, _)| position == preceding_position)
            .next()
            .map(|(position, _)| position.clone())
        {
            self.entries.split_off(&position);
        }

        let head = self.head();
        if head == preceding_position {
            Ok(self.extend(term, entries).await)
        } else if head.term() == preceding_position.term() {
            Err(head.next())
        } else {
            Err(*preceding_position)
        }
    }

    async fn at(&self, position: &Position) -> Option<(&Position, &Payload)> {
        self.entries
            .range(..position)
            .next_back()
            .map(|(position, _)| position)
            .zip(self.entries.get(position))
    }

    async fn next(&self, position: &Position) -> Option<(&Position, &Payload)> {
        self.entries
            .range(position..)
            .into_iter()
            .skip_while(|(p, _)| p == position)
            .next()
    }
}

impl Display for VolatileStorage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "VOLATILE")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn when_created_then_initialized() {
        let storage = VolatileStorage::init();

        assert_eq!(storage.head(), &Position::of(0, 0));

        assert_eq!(storage.at(&Position::of(0, 0)).await, None);
        assert_eq!(storage.at(&Position::of(0, 1)).await, None);
        assert_eq!(storage.at(&Position::of(1, 0)).await, None);

        assert_eq!(storage.next(&Position::of(0, 0)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_empty_entries_appended_then_succeeds() {
        let mut storage = VolatileStorage::init();

        assert_eq!(storage.extend(1, vec![]).await, Position::of(0, 0));

        assert_eq!(storage.head(), &Position::of(0, 0));

        assert_eq!(storage.at(&Position::of(1, 0)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entries_appended_then_succeeds() {
        let mut storage = VolatileStorage::init();

        assert_eq!(storage.extend(1, entries(1)).await, Position::of(1, 0));
        assert_eq!(storage.extend(1, entries(2)).await, Position::of(1, 1));
        assert_eq!(storage.extend(2, entries(3)).await, Position::of(2, 0));

        assert_eq!(storage.head(), &Position::of(2, 0));

        assert_eq!(
            storage.at(&Position::of(1, 0)).await,
            Some((&Position::of(0, 0), &bytes(1)))
        );
        assert_eq!(
            storage.at(&Position::of(1, 1)).await,
            Some((&Position::of(1, 0), &bytes(2)))
        );
        assert_eq!(storage.at(&Position::of(1, 2)).await, None);
        assert_eq!(
            storage.at(&Position::of(2, 0)).await,
            Some((&Position::of(1, 1), &bytes(3)))
        );
        assert_eq!(storage.at(&Position::of(2, 1)).await, None);
        assert_eq!(storage.at(&Position::of(3, 0)).await, None);

        assert_eq!(
            storage.next(&Position::of(0, 0)).await,
            Some((&Position::of(1, 0), &bytes(1)))
        );
        assert_eq!(
            storage.next(&Position::of(1, 0)).await,
            Some((&Position::of(1, 1), &bytes(2)))
        );
        assert_eq!(
            storage.next(&Position::of(1, 1)).await,
            Some((&Position::of(2, 0), &bytes(3)))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_and_preceding_present_then_succeeds() {
        let mut storage = VolatileStorage::init();

        assert_eq!(
            storage.insert(&Position::of(0, 0), 1, entries(1)).await,
            Ok(Position::of(1, 0))
        );
        assert_eq!(
            storage.insert(&Position::of(1, 0), 1, entries(2)).await,
            Ok(Position::of(1, 1))
        );
        assert_eq!(
            storage.insert(&Position::of(1, 1), 2, entries(3)).await,
            Ok(Position::of(2, 0))
        );

        assert_eq!(storage.head(), &Position::of(2, 0));

        assert_eq!(
            storage.at(&Position::of(1, 0)).await,
            Some((&Position::of(0, 0), &bytes(1)))
        );
        assert_eq!(
            storage.at(&Position::of(1, 1)).await,
            Some((&Position::of(1, 0), &bytes(2)))
        );
        assert_eq!(storage.at(&Position::of(1, 2)).await, None);
        assert_eq!(
            storage.at(&Position::of(2, 0)).await,
            Some((&Position::of(1, 1), &bytes(3)))
        );
        assert_eq!(storage.at(&Position::of(2, 1)).await, None);
        assert_eq!(storage.at(&Position::of(3, 0)).await, None);

        assert_eq!(
            storage.next(&Position::of(0, 0)).await,
            Some((&Position::of(1, 0), &bytes(1)))
        );
        assert_eq!(
            storage.next(&Position::of(1, 0)).await,
            Some((&Position::of(1, 1), &bytes(2)))
        );
        assert_eq!(
            storage.next(&Position::of(1, 1)).await,
            Some((&Position::of(2, 0), &bytes(3)))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_but_preceding_term_missing_then_fails() {
        let mut storage = VolatileStorage::init();

        assert_eq!(
            storage.insert(&Position::of(5, 0), 10, entries(1)).await,
            Err(Position::of(5, 0))
        );

        assert_eq!(storage.head(), &Position::of(0, 0));

        assert_eq!(storage.at(&Position::of(5, 0)).await, None);
        assert_eq!(storage.at(&Position::of(5, 1)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_but_preceding_index_missing_then_fails() {
        let mut storage = VolatileStorage::init();

        storage.extend(5, entries(1)).await;
        assert_eq!(
            storage.insert(&Position::of(5, 5), 5, entries(2)).await,
            Err(Position::of(5, 1))
        );

        assert_eq!(storage.head(), &Position::of(5, 0));

        assert_eq!(storage.at(&Position::of(5, 1)).await, None);
        assert_eq!(storage.at(&Position::of(5, 6)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_in_the_middle_then_subsequent_entries_are_removed() {
        let mut storage = VolatileStorage::init();

        storage
            .extend(5, vec![Payload::from_static(&[1]), Payload::from_static(&[2])])
            .await;
        storage.extend(10, entries(3)).await;

        assert_eq!(
            storage.insert(&Position::of(5, 0), 5, entries(4)).await,
            Ok(Position::of(5, 1))
        );

        assert_eq!(storage.head(), &Position::of(5, 1));

        assert_eq!(
            storage.at(&Position::of(5, 0)).await,
            Some((&Position::of(0, 0), &bytes(1)))
        );
        assert_eq!(
            storage.at(&Position::of(5, 1)).await,
            Some((&Position::of(5, 0), &bytes(4)))
        );
        assert_eq!(storage.at(&Position::of(5, 2)).await, None);
        assert_eq!(storage.at(&Position::of(10, 0)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_next() {
        let mut storage = VolatileStorage::init();

        assert_eq!(storage.extend(10, entries(100)).await, Position::of(10, 0));

        assert_eq!(
            storage.next(&Position::of(0, 0)).await,
            Some((&Position::of(10, 0), &bytes(100)))
        );
        assert_eq!(
            storage.next(&Position::of(0, 100)).await,
            Some((&Position::of(10, 0), &bytes(100)))
        );
        assert_eq!(
            storage.next(&Position::of(5, 5)).await,
            Some((&Position::of(10, 0), &bytes(100)))
        );
    }

    fn entries(value: u8) -> Vec<Payload> {
        vec![bytes(value)]
    }

    fn bytes(value: u8) -> Payload {
        Payload::from(vec![value])
    }
}

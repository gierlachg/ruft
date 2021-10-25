use std::convert::TryFrom;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::SeekFrom;
use std::path::Path;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::storage::{noop_message, Storage};
use crate::{Payload, Position};

pub(crate) struct DurableStorage {
    file: Mutex<SequentialFile>,
    head: Position,
}

impl DurableStorage {
    pub(crate) async fn init(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match tokio::fs::metadata(path.as_ref()).await {
            Ok(metadata) if metadata.is_file() => {
                let mut file = SequentialFile::from(path).await?;
                let (head, _) = file.seek(&Position::terminal()).await?;
                Ok(DurableStorage {
                    file: Mutex::new(file),
                    head: head.unwrap(),
                })
            }
            Ok(_) => panic!("Must be file"), // TODO:
            Err(_) => {
                let mut file = SequentialFile::from(path).await?;
                let head = Position::initial();
                file.append(&head, &noop_message()).await?;
                file.sync().await?;
                Ok(DurableStorage {
                    file: Mutex::new(file),
                    head,
                })
            }
        }
    }
}

#[async_trait]
impl Storage for DurableStorage {
    fn head(&self) -> &Position {
        &self.head
    }

    async fn extend(&mut self, term: u64, entries: Vec<Payload>) -> Position {
        assert!(term > 0 && term >= self.head.term());

        let mut file = self.file.lock().await;
        file.seek(&Position::terminal()).await.unwrap();
        let mut next;
        for entry in entries {
            next = self.head.next_in(term);
            file.append(&next, &entry).await.unwrap();
            self.head = next;
        }
        file.sync().await.unwrap();

        self.head
    }

    async fn insert(&mut self, preceding: &Position, term: u64, entries: Vec<Payload>) -> Result<Position, Position> {
        assert!(term > 0);

        {
            let mut file = self.file.lock().await;
            match file.seek(&preceding.next()).await.unwrap() {
                (Some(position), _) => self.head = position,
                _ => panic!("Missing initial position"),
            }
            file.truncate().await.unwrap();
        }

        if &self.head == preceding {
            Ok(self.extend(term, entries).await)
        } else if self.head.term() == preceding.term() {
            Err(self.head.next())
        } else {
            Err(*preceding)
        }
    }

    async fn at<'a>(&self, position: &'a Position) -> Option<(Position, &'a Position, Payload)> {
        let mut file = self.file.lock().await;
        match file.seek(position).await.unwrap() {
            (Some(preceding), Some(current)) if &current == position => {
                let payload = file.load().await.unwrap();
                Some((preceding, position, payload))
            }
            _ => None,
        }
    }

    async fn next<'a>(&self, position: &'a Position) -> Option<(&'a Position, Position, Payload)> {
        let mut file = self.file.lock().await;
        match file.seek(&position.next()).await.unwrap() {
            (_, Some(next)) => {
                let payload = file.load().await.unwrap();
                Some((position, next, payload))
            }
            _ => None,
        }
    }
}

impl Display for DurableStorage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "DURABLE")
    }
}

struct SequentialFile(tokio::fs::File);

impl SequentialFile {
    async fn from(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .await?;
        Ok(SequentialFile(file))
    }

    async fn seek(&mut self, position: &Position) -> Result<(Option<Position>, Option<Position>), std::io::Error> {
        self.0.seek(SeekFrom::Start(0)).await?;
        let mut offset = 0;
        let size = self.0.metadata().await?.len();
        let mut preceding = None;
        while offset < size {
            let current = Position::of(self.0.read_u64_le().await?, self.0.read_u64_le().await?);
            if &current >= position {
                self.0.seek(SeekFrom::Current(-(8 + 8))).await?;
                return Ok((preceding, Some(current)));
            } else {
                let len = self.0.read_u64_le().await?;
                self.0
                    .seek(SeekFrom::Current(i64::try_from(len).expect("Unable to convert")))
                    .await?;
                offset += 8 + 8 + 8 + len;
                preceding.replace(current);
            }
        }
        Ok((preceding, None))
    }

    async fn load(&mut self) -> Result<Payload, std::io::Error> {
        self.0.seek(SeekFrom::Current(8 + 8)).await?;
        let len = self.0.read_u64_le().await?;
        let mut bytes = vec![0u8; usize::try_from(len).expect("Unable to convert")];
        self.0.read(&mut bytes).await?;
        Ok(Payload::from(bytes))
    }

    async fn append(&mut self, position: &Position, entry: &Payload) -> Result<(), std::io::Error> {
        self.0.write_u64_le(position.term()).await?;
        self.0.write_u64_le(position.index()).await?;
        self.0
            .write_u64_le(u64::try_from(entry.0.len()).expect("Unable to convert"))
            .await?;
        self.0.write_all(entry.0.as_ref()).await?;
        Ok(())
    }

    async fn truncate(&mut self) -> Result<(), std::io::Error> {
        let offset = self.0.stream_position().await?;
        self.0.set_len(offset).await?;
        self.sync().await
    }

    async fn sync(&mut self) -> Result<(), std::io::Error> {
        self.0.sync_all().await
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn when_created_then_initialized() {
        let storage = DurableStorage::init(EphemeralFile::new()).await.unwrap();

        assert_eq!(storage.head(), &Position::of(0, 0));

        assert_eq!(storage.at(&Position::of(0, 0)).await, None);
        assert_eq!(storage.at(&Position::of(0, 1)).await, None);
        assert_eq!(storage.at(&Position::of(1, 0)).await, None);

        assert_eq!(storage.next(&Position::of(0, 0)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_empty_entries_appended_then_succeeds() {
        let mut storage = DurableStorage::init(EphemeralFile::new()).await.unwrap();

        assert_eq!(storage.extend(1, vec![]).await, Position::of(0, 0));

        assert_eq!(storage.head(), &Position::of(0, 0));

        assert_eq!(storage.at(&Position::of(1, 0)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entries_appended_then_succeeds() {
        let mut storage = DurableStorage::init(EphemeralFile::new()).await.unwrap();

        assert_eq!(storage.extend(1, entries(1)).await, Position::of(1, 0));
        assert_eq!(storage.extend(1, entries(2)).await, Position::of(1, 1));
        assert_eq!(storage.extend(2, entries(3)).await, Position::of(2, 0));

        assert_eq!(storage.head(), &Position::of(2, 0));

        assert_eq!(
            storage.at(&Position::of(1, 0)).await,
            Some((Position::of(0, 0), &Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            storage.at(&Position::of(1, 1)).await,
            Some((Position::of(1, 0), &Position::of(1, 1), bytes(2)))
        );
        assert_eq!(storage.at(&Position::of(1, 2)).await, None);
        assert_eq!(
            storage.at(&Position::of(2, 0)).await,
            Some((Position::of(1, 1), &Position::of(2, 0), bytes(3)))
        );
        assert_eq!(storage.at(&Position::of(2, 1)).await, None);
        assert_eq!(storage.at(&Position::of(3, 0)).await, None);

        assert_eq!(
            storage.next(&Position::of(0, 0)).await,
            Some((&Position::of(0, 0), Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            storage.next(&Position::of(1, 0)).await,
            Some((&Position::of(1, 0), Position::of(1, 1), bytes(2)))
        );
        assert_eq!(
            storage.next(&Position::of(1, 1)).await,
            Some((&Position::of(1, 1), Position::of(2, 0), bytes(3)))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_and_preceding_present_then_succeeds() {
        let mut storage = DurableStorage::init(EphemeralFile::new()).await.unwrap();

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
            Some((Position::of(0, 0), &Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            storage.at(&Position::of(1, 1)).await,
            Some((Position::of(1, 0), &Position::of(1, 1), bytes(2)))
        );
        assert_eq!(storage.at(&Position::of(1, 2)).await, None);
        assert_eq!(
            storage.at(&Position::of(2, 0)).await,
            Some((Position::of(1, 1), &Position::of(2, 0), bytes(3)))
        );
        assert_eq!(storage.at(&Position::of(2, 1)).await, None);
        assert_eq!(storage.at(&Position::of(3, 0)).await, None);

        assert_eq!(
            storage.next(&Position::of(0, 0)).await,
            Some((&Position::of(0, 0), Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            storage.next(&Position::of(1, 0)).await,
            Some((&Position::of(1, 0), Position::of(1, 1), bytes(2)))
        );
        assert_eq!(
            storage.next(&Position::of(1, 1)).await,
            Some((&Position::of(1, 1), Position::of(2, 0), bytes(3)))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_but_preceding_term_missing_then_fails() {
        let mut storage = DurableStorage::init(EphemeralFile::new()).await.unwrap();

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
        let mut storage = DurableStorage::init(EphemeralFile::new()).await.unwrap();

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
        let mut storage = DurableStorage::init(EphemeralFile::new()).await.unwrap();

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
            Some((Position::of(0, 0), &Position::of(5, 0), bytes(1)))
        );
        assert_eq!(
            storage.at(&Position::of(5, 1)).await,
            Some((Position::of(5, 0), &Position::of(5, 1), bytes(4)))
        );
        assert_eq!(storage.at(&Position::of(5, 2)).await, None);
        assert_eq!(storage.at(&Position::of(10, 0)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_next() {
        let mut storage = DurableStorage::init(EphemeralFile::new()).await.unwrap();

        assert_eq!(storage.extend(10, entries(100)).await, Position::of(10, 0));

        assert_eq!(
            storage.next(&Position::of(0, 0)).await,
            Some((&Position::of(0, 0), Position::of(10, 0), bytes(100)))
        );
        assert_eq!(
            storage.next(&Position::of(0, 100)).await,
            Some((&Position::of(0, 100), Position::of(10, 0), bytes(100)))
        );
        assert_eq!(
            storage.next(&Position::of(5, 5)).await,
            Some((&Position::of(5, 5), Position::of(10, 0), bytes(100)))
        );
        assert_eq!(storage.next(&Position::of(10, 0)).await, None);
        assert_eq!(storage.next(&Position::of(100, 10)).await, None);
    }

    fn entries(value: u8) -> Vec<Payload> {
        vec![bytes(value)]
    }

    fn bytes(value: u8) -> Payload {
        Payload::from(vec![value])
    }

    struct EphemeralFile(String);

    impl<'a> EphemeralFile {
        fn new() -> Self {
            let mut file = String::from("../target/tmp/ruft-");
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .map(char::from)
                .take(10)
                .for_each(|c| file.push(c));
            file.push_str(".log");
            EphemeralFile(file)
        }
    }

    impl AsRef<Path> for EphemeralFile {
        fn as_ref(&self) -> &Path {
            self.0.as_ref()
        }
    }

    impl Drop for EphemeralFile {
        fn drop(&mut self) {
            std::fs::remove_file(&self.0).expect("Unable to remove file")
        }
    }
}

use std::collections::BTreeMap;
use std::io::{Error, SeekFrom};
use std::mem::size_of;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Buf;
use futures::{StreamExt, TryStreamExt};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_stream::Stream;
use tokio_util::codec::LengthDelimitedCodec;

use crate::storage::{Entries, Log, State};
use crate::{Payload, Position};

pub(crate) struct FileState {
    file: PathBuf,
}

impl FileState {
    pub(super) fn init(directory: impl AsRef<Path>) -> Self {
        let file = directory.as_ref().join(Path::new("state"));
        FileState { file }
    }
}

#[async_trait]
impl State for FileState {
    async fn load(&self) -> Option<u64> {
        match tokio::fs::metadata(self.file.as_path()).await {
            Ok(_) => {
                let mut file = tokio::fs::OpenOptions::new()
                    .create(false)
                    .read(true)
                    .open(self.file.as_path())
                    .await
                    .unwrap();

                Some(file.read_u64_le().await.unwrap())
            }
            Err(_) => None,
        }
    }

    async fn store(&mut self, term: u64) {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(self.file.as_path())
            .await
            .unwrap();

        file.write_u64_le(term).await.unwrap();
    }
}

pub(crate) struct FileLog {
    file: Mutex<SequentialFile>,
    entries: BTreeMap<Position, (u64, Payload)>,
}

impl FileLog {
    pub(super) async fn init(directory: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let file = directory.as_ref().join(Path::new("log"));
        match tokio::fs::metadata(file.as_path()).await {
            Ok(_) => {
                let file = SequentialFile::from(file).await?;

                let mut entries = BTreeMap::new();
                let mut file_entries = file.entries().await?;
                while let Some(result) = file_entries.next().await {
                    let (position, offset, entry) = result?;
                    entries.insert(position, (offset, entry));
                }

                Ok(FileLog {
                    file: Mutex::new(file),
                    entries,
                })
            }
            Err(_) => {
                let position = Position::initial();
                let entry = Payload::empty();

                let mut file = SequentialFile::from(file).await?;
                file.append((&position, &entry)).await?;

                let mut entries = BTreeMap::new();
                entries.insert(position, (0, entry));

                Ok(FileLog {
                    file: Mutex::new(file),
                    entries,
                })
            }
        }
    }
}

#[async_trait]
impl Log for FileLog {
    fn head(&self) -> &Position {
        match self.entries.iter().next_back() {
            Some((position, _)) => position,
            None => unreachable!("{:?}", self.entries),
        }
    }

    async fn extend(&mut self, term: NonZeroU64, entries: Vec<Payload>) -> Position {
        assert!(term.get() >= self.head().term());

        if !entries.is_empty() {
            let mut file = self.file.lock().await;
            let mut position = *self.head();
            for entry in entries {
                position = position.next_in(term);
                let offset = file.append((&position, &entry)).await.unwrap();
                self.entries.insert(position, (offset, entry));
            }
        }
        *self.head()
    }

    async fn insert(
        &mut self,
        preceding: &Position,
        term: NonZeroU64,
        entries: Vec<Payload>,
    ) -> Result<Position, Position> {
        if let Some((position, offset)) = self
            .entries
            .range(preceding.next()..)
            .into_iter()
            .next()
            .map(|(position, (offset, _))| (*position, *offset))
        {
            let mut file = self.file.lock().await;
            file.truncate(offset).await.unwrap();
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

    fn at(&self, needle: Position) -> Option<(Position, Position, Payload)> {
        self.entries
            .range(..needle)
            .next_back()
            .map(|(position, _)| position)
            .zip(self.entries.get(&needle))
            .map(|(position, (_, entry))| (*position, needle, entry.clone()))
    }

    fn next(&self, needle: Position) -> Option<(Position, Position, Payload)> {
        self.entries
            .range(needle.next()..)
            .into_iter()
            .next()
            .map(|(position, (_, entry))| (needle, *position, entry.clone()))
    }

    fn entries(&self, from: Position) -> Box<dyn Entries + '_> {
        let iterator = self
            .entries
            .range(from.next()..)
            .into_iter()
            .map(|(position, (_, entry))| (position.clone(), entry.clone()));
        Box::new(tokio_stream::iter(iterator))
    }
}

struct SequentialFile(tokio::fs::File, u64);

impl SequentialFile {
    async fn from(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .await?;
        let size = file.metadata().await?.len();
        Ok(SequentialFile(file, size))
    }

    async fn append(&mut self, entry: (&Position, &Payload)) -> Result<u64, Error> {
        let offset = self.1;
        let mut bytes = Vec::with_capacity(8 + 8 + 8 + usize::try_from(entry.1.len()).expect("Unable to convert"));
        bytes.extend_from_slice(
            &(u64::try_from(size_of::<u64>()).expect("Unable to convert") * 2 + &entry.1.len()).to_le_bytes(),
        );
        bytes.extend_from_slice(&entry.0.term().to_le_bytes());
        bytes.extend_from_slice(&entry.0.index().to_le_bytes());
        bytes.extend_from_slice(entry.1 .0.as_ref());
        self.0.write_all(&bytes).await?;
        self.0.sync_all().await?; // TODO: batch sync
        self.1 += u64::try_from(bytes.len()).expect("Unable to convert");
        Ok(offset)
    }

    async fn entries(&self) -> Result<impl Stream<Item = Result<(Position, u64, Payload), Error>>, Error> {
        let mut file_offset = 0;
        let entries = LengthDelimitedCodec::builder()
            .length_field_offset(0)
            .length_field_length(size_of::<u64>())
            .little_endian()
            .new_read(self.0.try_clone().await?)
            .map_ok(move |mut bytes| {
                let position = Position::of(bytes.get_u64_le(), bytes.get_u64_le());
                let offset = file_offset;
                let payload = Payload(bytes.freeze());

                file_offset += u64::try_from(size_of::<u64>()).expect("Unable to convert") * 3 + payload.len();

                (position, offset, payload)
            });
        Ok(entries)
    }

    async fn truncate(&mut self, len: u64) -> Result<(), Error> {
        self.0.set_len(len).await?;
        self.0.sync_all().await?;
        self.0.seek(SeekFrom::End(0)).await?;
        self.1 = len;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use rand::Rng;
    use tokio_stream::StreamExt;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn when_created_then_initialized() {
        let log = FileLog::init(EphemeralDirectory::new()).await.unwrap();

        assert_eq!(log.head(), &Position::of(0, 0));

        assert_eq!(log.at(Position::of(0, 0)), None);
        assert_eq!(log.at(Position::of(0, 1)), None);
        assert_eq!(log.at(Position::of(1, 0)), None);

        assert_eq!(log.next(Position::of(0, 0)), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_empty_entries_appended_then_succeeds() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await.unwrap();

        assert_eq!(
            log.extend(NonZeroU64::new(1).unwrap(), vec![]).await,
            Position::of(0, 0)
        );

        assert_eq!(log.head(), &Position::of(0, 0));

        assert_eq!(log.at(Position::of(1, 0)), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entries_appended_then_succeeds() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await.unwrap();

        assert_eq!(
            log.extend(NonZeroU64::new(1).unwrap(), entries(1)).await,
            Position::of(1, 0)
        );
        assert_eq!(
            log.extend(NonZeroU64::new(1).unwrap(), entries(2)).await,
            Position::of(1, 1)
        );
        assert_eq!(
            log.extend(NonZeroU64::new(2).unwrap(), entries(3)).await,
            Position::of(2, 0)
        );

        assert_eq!(log.head(), &Position::of(2, 0));

        assert_eq!(
            log.at(Position::of(1, 0)),
            Some((Position::of(0, 0), Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            log.at(Position::of(1, 1)),
            Some((Position::of(1, 0), Position::of(1, 1), bytes(2)))
        );
        assert_eq!(log.at(Position::of(1, 2)), None);
        assert_eq!(
            log.at(Position::of(2, 0)),
            Some((Position::of(1, 1), Position::of(2, 0), bytes(3)))
        );
        assert_eq!(log.at(Position::of(2, 1)), None);
        assert_eq!(log.at(Position::of(3, 0)), None);

        assert_eq!(
            log.next(Position::of(0, 0)),
            Some((Position::of(0, 0), Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            log.next(Position::of(1, 0)),
            Some((Position::of(1, 0), Position::of(1, 1), bytes(2)))
        );
        assert_eq!(
            log.next(Position::of(1, 1)),
            Some((Position::of(1, 1), Position::of(2, 0), bytes(3)))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_and_preceding_present_then_succeeds() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await.unwrap();

        assert_eq!(
            log.insert(&Position::of(0, 0), NonZeroU64::new(1).unwrap(), entries(1))
                .await,
            Ok(Position::of(1, 0))
        );
        assert_eq!(
            log.insert(&Position::of(1, 0), NonZeroU64::new(1).unwrap(), entries(2))
                .await,
            Ok(Position::of(1, 1))
        );
        assert_eq!(
            log.insert(&Position::of(1, 1), NonZeroU64::new(2).unwrap(), entries(3))
                .await,
            Ok(Position::of(2, 0))
        );

        assert_eq!(log.head(), &Position::of(2, 0));

        assert_eq!(
            log.at(Position::of(1, 0)),
            Some((Position::of(0, 0), Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            log.at(Position::of(1, 1)),
            Some((Position::of(1, 0), Position::of(1, 1), bytes(2)))
        );
        assert_eq!(log.at(Position::of(1, 2)), None);
        assert_eq!(
            log.at(Position::of(2, 0)),
            Some((Position::of(1, 1), Position::of(2, 0), bytes(3)))
        );
        assert_eq!(log.at(Position::of(2, 1)), None);
        assert_eq!(log.at(Position::of(3, 0)), None);

        assert_eq!(
            log.next(Position::of(0, 0)),
            Some((Position::of(0, 0), Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            log.next(Position::of(1, 0)),
            Some((Position::of(1, 0), Position::of(1, 1), bytes(2)))
        );
        assert_eq!(
            log.next(Position::of(1, 1)),
            Some((Position::of(1, 1), Position::of(2, 0), bytes(3)))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_but_preceding_term_missing_then_fails() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await.unwrap();

        assert_eq!(
            log.insert(&Position::of(5, 0), NonZeroU64::new(10).unwrap(), entries(1))
                .await,
            Err(Position::of(5, 0))
        );

        assert_eq!(log.head(), &Position::of(0, 0));

        assert_eq!(log.at(Position::of(5, 0)), None);
        assert_eq!(log.at(Position::of(5, 1)), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_but_preceding_index_missing_then_fails() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await.unwrap();

        log.extend(NonZeroU64::new(5).unwrap(), entries(1)).await;
        assert_eq!(
            log.insert(&Position::of(5, 5), NonZeroU64::new(5).unwrap(), entries(2))
                .await,
            Err(Position::of(5, 1))
        );

        assert_eq!(log.head(), &Position::of(5, 0));

        assert_eq!(log.at(Position::of(5, 1)), None);
        assert_eq!(log.at(Position::of(5, 6)), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_in_the_middle_then_subsequent_entries_are_removed() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await.unwrap();

        log.extend(
            NonZeroU64::new(5).unwrap(),
            vec![Payload::from(vec![1]), Payload::from(vec![2])],
        )
        .await;
        log.extend(NonZeroU64::new(10).unwrap(), entries(3)).await;

        assert_eq!(
            log.insert(&Position::of(5, 0), NonZeroU64::new(5).unwrap(), entries(4))
                .await,
            Ok(Position::of(5, 1))
        );

        assert_eq!(log.head(), &Position::of(5, 1));

        assert_eq!(
            log.at(Position::of(5, 0)),
            Some((Position::of(0, 0), Position::of(5, 0), bytes(1)))
        );
        assert_eq!(
            log.at(Position::of(5, 1)),
            Some((Position::of(5, 0), Position::of(5, 1), bytes(4)))
        );
        assert_eq!(log.at(Position::of(5, 2)), None);
        assert_eq!(log.at(Position::of(10, 0)), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_next() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await.unwrap();

        assert_eq!(
            log.extend(NonZeroU64::new(10).unwrap(), entries(100)).await,
            Position::of(10, 0)
        );

        assert_eq!(
            log.next(Position::of(0, 0)),
            Some((Position::of(0, 0), Position::of(10, 0), bytes(100)))
        );
        assert_eq!(
            log.next(Position::of(0, 100)),
            Some((Position::of(0, 100), Position::of(10, 0), bytes(100)))
        );
        assert_eq!(
            log.next(Position::of(5, 5)),
            Some((Position::of(5, 5), Position::of(10, 0), bytes(100)))
        );
        assert_eq!(log.next(Position::of(10, 0)), None);
        assert_eq!(log.next(Position::of(100, 10)), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_entries() {
        let mut storage = FileLog::init(EphemeralDirectory::new()).await.unwrap();

        storage.extend(NonZeroU64::new(1).unwrap(), entries(1)).await;
        storage.extend(NonZeroU64::new(1).unwrap(), entries(2)).await;
        storage.extend(NonZeroU64::new(10).unwrap(), entries(10)).await;

        let entries = Pin::from(storage.entries(Position::initial()))
            .collect::<Vec<_>>()
            .await;
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

        let entries = Pin::from(storage.entries(Position::of(5, 5))).collect::<Vec<_>>().await;
        assert_eq!(entries, vec![(Position::of(10, 0), bytes(10))]);

        let entries = Pin::from(storage.entries(Position::of(100, 100)))
            .collect::<Vec<_>>()
            .await;
        assert!(entries.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_persistence() {
        let directory = EphemeralDirectory::new();

        {
            let mut storage = FileLog::init(&directory).await.unwrap();

            storage.extend(NonZeroU64::new(1).unwrap(), entries(1)).await;
            storage.extend(NonZeroU64::new(1).unwrap(), entries(2)).await;
            storage.extend(NonZeroU64::new(10).unwrap(), entries(10)).await;
        }

        {
            let storage = FileLog::init(&directory).await.unwrap();

            let entries = Pin::from(storage.entries(Position::initial()))
                .collect::<Vec<_>>()
                .await;
            assert_eq!(
                entries,
                vec![
                    (Position::of(1, 0), bytes(1)),
                    (Position::of(1, 1), bytes(2)),
                    (Position::of(10, 0), bytes(10))
                ]
            );
        }
    }

    fn entries(value: u8) -> Vec<Payload> {
        vec![bytes(value)]
    }

    fn bytes(value: u8) -> Payload {
        Payload::from(vec![value])
    }

    struct EphemeralDirectory(String);

    impl<'a> EphemeralDirectory {
        fn new() -> Self {
            let mut directory = String::from("../target/tmp/ruft-");
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .map(char::from)
                .take(10)
                .for_each(|c| directory.push(c));
            std::fs::create_dir_all(&directory).expect("Unable to create directory");
            EphemeralDirectory(directory)
        }
    }

    impl AsRef<Path> for EphemeralDirectory {
        fn as_ref(&self) -> &Path {
            self.0.as_ref()
        }
    }

    impl Drop for EphemeralDirectory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).expect("Unable to remove directory")
        }
    }
}

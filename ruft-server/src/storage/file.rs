use std::fmt::{self, Display, Formatter};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::storage::{Log, State};
use crate::{Id, Payload, Position};

pub(crate) struct FileState {
    file: PathBuf,
}

impl FileState {
    pub(crate) fn init(directory: impl AsRef<Path>) -> Self {
        let file = directory.as_ref().join(Path::new("state"));
        FileState { file }
    }
}

#[async_trait]
impl State for FileState {
    async fn load(&self) -> (u64, Option<Id>) {
        match tokio::fs::metadata(self.file.as_path()).await {
            Ok(_) => {
                let mut file = tokio::fs::OpenOptions::new()
                    .create(false)
                    .read(true)
                    .open(self.file.as_path())
                    .await
                    .unwrap();

                let term = file.read_u64_le().await.unwrap();
                match file.read_u8().await.unwrap() {
                    0 => (term, None),
                    1 => (term, Some(Id(file.read_u8().await.unwrap()))),
                    _ => panic!("Unexpected value"), // TODO:
                }
            }
            Err(_) => (0, None),
        }
    }

    async fn store(&mut self, term: u64, votee: Option<Id>) {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(self.file.as_path())
            .await
            .unwrap();

        file.write_u64_le(term).await.unwrap();
        match votee {
            Some(votee) => {
                file.write_u8(1).await.unwrap();
                file.write_u8(votee.0).await.unwrap();
            }
            None => file.write_u8(0).await.unwrap(),
        }
        file.sync_all().await.unwrap()
    }
}

pub(crate) struct FileLog {
    file: Mutex<SequentialFile>,
    head: Position,
}

impl FileLog {
    pub(crate) async fn init(directory: impl AsRef<Path>) -> Self {
        let file = directory.as_ref().join(Path::new("log"));
        match tokio::fs::metadata(file.as_path()).await {
            Ok(_) => {
                let mut file = SequentialFile::from(file).await.unwrap();
                let (head, _) = file.seek(&Position::terminal()).await.unwrap();
                FileLog {
                    file: Mutex::new(file),
                    // safety: log is not empty
                    head: head.unwrap(),
                }
            }
            Err(_) => {
                let mut file = SequentialFile::from(file).await.unwrap();
                let head = Position::initial();
                file.append(vec![(head, Payload::empty())].into_iter()).await.unwrap();
                FileLog {
                    file: Mutex::new(file),
                    head,
                }
            }
        }
    }
}

#[async_trait]
impl Log for FileLog {
    fn head(&self) -> &Position {
        &self.head
    }

    async fn extend(&mut self, term: u64, entries: Vec<Payload>) -> Position {
        assert!(term > 0 && term >= self.head.term());

        let mut file = self.file.lock().await;
        file.seek(&Position::terminal()).await.unwrap();
        let entries = entries.into_iter().map(|entry| {
            self.head = self.head.next_in(term);
            (self.head, entry)
        });
        file.append(entries).await.unwrap();

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

impl Display for FileLog {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "FILE")
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

    async fn append(&mut self, entries: impl Iterator<Item = (Position, Payload)>) -> Result<(), std::io::Error> {
        let mut modified = false;
        for (position, entry) in entries {
            self.0.write_u64_le(position.term()).await?;
            self.0.write_u64_le(position.index()).await?;
            self.0
                .write_u64_le(u64::try_from(entry.0.len()).expect("Unable to convert"))
                .await?;
            self.0.write_all(entry.0.as_ref()).await?;
            modified = true;
        }
        if modified {
            self.0.sync_all().await
        } else {
            Ok(())
        }
    }

    async fn truncate(&mut self) -> Result<(), std::io::Error> {
        let offset = self.0.stream_position().await?;
        let size = self.0.metadata().await?.len();
        if offset < size {
            self.0.set_len(offset).await?;
            self.0.sync_all().await
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn when_created_then_initialized() {
        let log = FileLog::init(EphemeralDirectory::new()).await;

        assert_eq!(log.head(), &Position::of(0, 0));

        assert_eq!(log.at(&Position::of(0, 0)).await, None);
        assert_eq!(log.at(&Position::of(0, 1)).await, None);
        assert_eq!(log.at(&Position::of(1, 0)).await, None);

        assert_eq!(log.next(&Position::of(0, 0)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_empty_entries_appended_then_succeeds() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await;

        assert_eq!(log.extend(1, vec![]).await, Position::of(0, 0));

        assert_eq!(log.head(), &Position::of(0, 0));

        assert_eq!(log.at(&Position::of(1, 0)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entries_appended_then_succeeds() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await;

        assert_eq!(log.extend(1, entries(1)).await, Position::of(1, 0));
        assert_eq!(log.extend(1, entries(2)).await, Position::of(1, 1));
        assert_eq!(log.extend(2, entries(3)).await, Position::of(2, 0));

        assert_eq!(log.head(), &Position::of(2, 0));

        assert_eq!(
            log.at(&Position::of(1, 0)).await,
            Some((Position::of(0, 0), &Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            log.at(&Position::of(1, 1)).await,
            Some((Position::of(1, 0), &Position::of(1, 1), bytes(2)))
        );
        assert_eq!(log.at(&Position::of(1, 2)).await, None);
        assert_eq!(
            log.at(&Position::of(2, 0)).await,
            Some((Position::of(1, 1), &Position::of(2, 0), bytes(3)))
        );
        assert_eq!(log.at(&Position::of(2, 1)).await, None);
        assert_eq!(log.at(&Position::of(3, 0)).await, None);

        assert_eq!(
            log.next(&Position::of(0, 0)).await,
            Some((&Position::of(0, 0), Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            log.next(&Position::of(1, 0)).await,
            Some((&Position::of(1, 0), Position::of(1, 1), bytes(2)))
        );
        assert_eq!(
            log.next(&Position::of(1, 1)).await,
            Some((&Position::of(1, 1), Position::of(2, 0), bytes(3)))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_and_preceding_present_then_succeeds() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await;

        assert_eq!(
            log.insert(&Position::of(0, 0), 1, entries(1)).await,
            Ok(Position::of(1, 0))
        );
        assert_eq!(
            log.insert(&Position::of(1, 0), 1, entries(2)).await,
            Ok(Position::of(1, 1))
        );
        assert_eq!(
            log.insert(&Position::of(1, 1), 2, entries(3)).await,
            Ok(Position::of(2, 0))
        );

        assert_eq!(log.head(), &Position::of(2, 0));

        assert_eq!(
            log.at(&Position::of(1, 0)).await,
            Some((Position::of(0, 0), &Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            log.at(&Position::of(1, 1)).await,
            Some((Position::of(1, 0), &Position::of(1, 1), bytes(2)))
        );
        assert_eq!(log.at(&Position::of(1, 2)).await, None);
        assert_eq!(
            log.at(&Position::of(2, 0)).await,
            Some((Position::of(1, 1), &Position::of(2, 0), bytes(3)))
        );
        assert_eq!(log.at(&Position::of(2, 1)).await, None);
        assert_eq!(log.at(&Position::of(3, 0)).await, None);

        assert_eq!(
            log.next(&Position::of(0, 0)).await,
            Some((&Position::of(0, 0), Position::of(1, 0), bytes(1)))
        );
        assert_eq!(
            log.next(&Position::of(1, 0)).await,
            Some((&Position::of(1, 0), Position::of(1, 1), bytes(2)))
        );
        assert_eq!(
            log.next(&Position::of(1, 1)).await,
            Some((&Position::of(1, 1), Position::of(2, 0), bytes(3)))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_but_preceding_term_missing_then_fails() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await;

        assert_eq!(
            log.insert(&Position::of(5, 0), 10, entries(1)).await,
            Err(Position::of(5, 0))
        );

        assert_eq!(log.head(), &Position::of(0, 0));

        assert_eq!(log.at(&Position::of(5, 0)).await, None);
        assert_eq!(log.at(&Position::of(5, 1)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_but_preceding_index_missing_then_fails() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await;

        log.extend(5, entries(1)).await;
        assert_eq!(
            log.insert(&Position::of(5, 5), 5, entries(2)).await,
            Err(Position::of(5, 1))
        );

        assert_eq!(log.head(), &Position::of(5, 0));

        assert_eq!(log.at(&Position::of(5, 1)).await, None);
        assert_eq!(log.at(&Position::of(5, 6)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_entry_inserted_in_the_middle_then_subsequent_entries_are_removed() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await;

        log.extend(5, vec![Payload::from_static(&[1]), Payload::from_static(&[2])])
            .await;
        log.extend(10, entries(3)).await;

        assert_eq!(
            log.insert(&Position::of(5, 0), 5, entries(4)).await,
            Ok(Position::of(5, 1))
        );

        assert_eq!(log.head(), &Position::of(5, 1));

        assert_eq!(
            log.at(&Position::of(5, 0)).await,
            Some((Position::of(0, 0), &Position::of(5, 0), bytes(1)))
        );
        assert_eq!(
            log.at(&Position::of(5, 1)).await,
            Some((Position::of(5, 0), &Position::of(5, 1), bytes(4)))
        );
        assert_eq!(log.at(&Position::of(5, 2)).await, None);
        assert_eq!(log.at(&Position::of(10, 0)).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_next() {
        let mut log = FileLog::init(EphemeralDirectory::new()).await;

        assert_eq!(log.extend(10, entries(100)).await, Position::of(10, 0));

        assert_eq!(
            log.next(&Position::of(0, 0)).await,
            Some((&Position::of(0, 0), Position::of(10, 0), bytes(100)))
        );
        assert_eq!(
            log.next(&Position::of(0, 100)).await,
            Some((&Position::of(0, 100), Position::of(10, 0), bytes(100)))
        );
        assert_eq!(
            log.next(&Position::of(5, 5)).await,
            Some((&Position::of(5, 5), Position::of(10, 0), bytes(100)))
        );
        assert_eq!(log.next(&Position::of(10, 0)).await, None);
        assert_eq!(log.next(&Position::of(100, 10)).await, None);
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

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use derive_more::Display;
use log::info;
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::automaton::candidate::Candidate;
use crate::automaton::follower::Follower;
use crate::automaton::leader::Leader;
use crate::automaton::State::{CANDIDATE, FOLLOWER, LEADER, TERMINATED};
use crate::cluster::Cluster;
use crate::relay::protocol::Response;
use crate::relay::Relay;
use crate::storage::Log;
use crate::{Id, Position};

mod candidate;
mod follower;
mod leader;

pub(super) async fn run<L: Log, C: Cluster, R: Relay>(
    directory: impl AsRef<Path>,
    id: Id,
    heartbeat_interval: Duration,
    election_timeout: Duration,
    mut log: L,
    mut cluster: C,
    mut relay: R,
) {
    let file = directory.as_ref().join(Path::new("state"));

    let (term, votee) = load(file.as_path()).await.unwrap().unwrap_or((0, None));
    let mut state = State::follower(term, votee, None);
    info!("Starting as: {:?}", state);

    loop {
        state = match state {
            FOLLOWER { term, votee, leader } => {
                store(file.as_path(), term, votee).await.unwrap();

                let election_timeout = election_timeout + Duration::from_millis(rand::thread_rng().gen_range(0..=250));
                Follower::init(
                    id,
                    term,
                    &mut log,
                    &mut cluster,
                    &mut relay,
                    votee,
                    leader,
                    election_timeout,
                )
                .run()
                .await
            }
            CANDIDATE { term } => {
                store(file.as_path(), term, None).await.unwrap();

                let election_timeout = election_timeout + Duration::from_millis(rand::thread_rng().gen_range(0..=250));
                Candidate::init(id, term, &mut log, &mut cluster, &mut relay, election_timeout)
                    .run()
                    .await
            }
            LEADER { term } => {
                store(file.as_path(), term, None).await.unwrap();

                Leader::init(id, term, &mut log, &mut cluster, &mut relay, heartbeat_interval)
                    .run()
                    .await
            }
            TERMINATED => break,
        };
        info!("Switching over to: {:?}", state);
    }
}

async fn load(file: impl AsRef<Path>) -> Result<Option<(u64, Option<Id>)>, std::io::Error> {
    match tokio::fs::metadata(file.as_ref()).await {
        Ok(_) => {
            let mut file = tokio::fs::OpenOptions::new()
                .create(false)
                .read(true)
                .open(file)
                .await?;

            let term = file.read_u64_le().await?;
            match file.read_u8().await? {
                0 => Ok(Some((term, None))),
                1 => Ok(Some((term, Some(Id(file.read_u8().await?))))),
                _ => panic!("Unexpected value"), // TODO:
            }
        }
        Err(_) => Ok(None),
    }
}

async fn store(file: impl AsRef<Path>, term: u64, votee: Option<Id>) -> Result<(), std::io::Error> {
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(file)
        .await?;

    file.write_u64_le(term).await?;
    match votee {
        Some(votee) => {
            file.write_u8(1).await?;
            file.write_u8(votee.0).await?;
        }
        None => file.write_u8(0).await?,
    }
    file.sync_all().await.unwrap();
    Ok(())
}

#[derive(PartialEq, Eq, Display, Debug)]
enum State {
    #[display(fmt = "FOLLOWER {{ term: {}, leader: {:?} }}", term, leader)]
    FOLLOWER {
        term: u64,
        votee: Option<Id>,
        leader: Option<Id>,
    },
    #[display(fmt = "CANDIDATE {{ term: {} }}", term)]
    CANDIDATE { term: u64 },
    #[display(fmt = "LEADER {{ term: {} }}", term)]
    LEADER { term: u64 },
    #[display(fmt = "TERMINATED")]
    TERMINATED,
}

impl State {
    fn follower(term: u64, votee: Option<Id>, leader: Option<Id>) -> Self {
        FOLLOWER { term, votee, leader }
    }

    fn candidate(term: u64) -> Self {
        CANDIDATE { term }
    }

    fn leader(term: u64) -> Self {
        LEADER { term }
    }
}

struct Responder(tokio::sync::mpsc::UnboundedSender<Response>);

impl Responder {
    fn respond_with_success(self) {
        // safety: client already disconnected
        self.0.send(Response::store_success_response()).unwrap_or(())
    }

    fn respond_with_redirect(&self, address: Option<SocketAddr>, position: Option<Position>) {
        // safety: client already disconnected
        self.0
            .send(Response::store_redirect_response(address, position))
            .unwrap_or(())
    }
}

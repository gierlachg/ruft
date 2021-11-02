use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use derive_more::Display;
use log::info;
use rand::Rng;

use crate::automaton::candidate::Candidate;
use crate::automaton::follower::Follower;
use crate::automaton::leader::Leader;
use crate::automaton::Transition::{CANDIDATE, FOLLOWER, LEADER, TERMINATED};
use crate::cluster::Cluster;
use crate::relay::protocol::Response;
use crate::relay::Relay;
use crate::storage::file::{FileLog, FileState};
use crate::storage::State;
use crate::{Id, Position};

mod candidate;
mod follower;
mod leader;

pub(super) async fn run<C: Cluster, R: Relay>(
    directory: impl AsRef<Path>,
    id: Id,
    heartbeat_interval: Duration,
    election_timeout: Duration,
    mut cluster: C,
    mut relay: R,
) {
    match tokio::fs::metadata(directory.as_ref()).await {
        Ok(metadata) if metadata.is_dir() => {} // TODO: check empty or both files are there
        Ok(_) => panic!("Path must be a directory"), // TODO:
        Err(_) => tokio::fs::create_dir(&directory).await.unwrap(), // TODO:
    }

    let mut state = FileState::init(&directory);
    let mut transition = Transition::follower(state.load().await.unwrap_or(0), None);

    let mut log = FileLog::init(&directory).await;
    info!("Using {} log", &log);

    loop {
        info!("Switching over to: {:?}", transition);

        transition = match transition {
            FOLLOWER { term, leader } => {
                state.store(term).await;

                let election_timeout = election_timeout + Duration::from_millis(rand::thread_rng().gen_range(0..=250));
                Follower::init(id, term, &mut log, &mut cluster, &mut relay, leader, election_timeout)
                    .run()
                    .await
            }
            CANDIDATE { term } => {
                state.store(term).await;

                let election_timeout = election_timeout + Duration::from_millis(rand::thread_rng().gen_range(0..=250));
                Candidate::init(id, term, &mut log, &mut cluster, &mut relay, election_timeout)
                    .run()
                    .await
            }
            LEADER { term } => {
                state.store(term).await;

                Leader::init(id, term, &mut log, &mut cluster, &mut relay, heartbeat_interval)
                    .run()
                    .await
            }
            TERMINATED => break,
        };
    }
}

#[derive(PartialEq, Eq, Display, Debug)]
enum Transition {
    #[display(fmt = "FOLLOWER {{ term: {}, leader: {:?} }}", term, leader)]
    FOLLOWER { term: u64, leader: Option<Id> },
    #[display(fmt = "CANDIDATE {{ term: {} }}", term)]
    CANDIDATE { term: u64 },
    #[display(fmt = "LEADER {{ term: {} }}", term)]
    LEADER { term: u64 },
    #[display(fmt = "TERMINATED")]
    TERMINATED,
}

impl Transition {
    fn follower(term: u64, leader: Option<Id>) -> Self {
        FOLLOWER { term, leader }
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
        self.0.send(Response::replicate_success_response()).unwrap_or(())
    }

    fn respond_with_redirect(&self, address: Option<SocketAddr>, position: Option<Position>) {
        // safety: client already disconnected
        self.0
            .send(Response::replicate_redirect_response(address, position))
            .unwrap_or(())
    }
}

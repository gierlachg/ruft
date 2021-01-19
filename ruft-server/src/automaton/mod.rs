use std::error::Error;

use log::info;
use rand::Rng;
use tokio::time::Duration;

use crate::automaton::candidate::Candidate;
use crate::automaton::follower::Follower;
use crate::automaton::leader::Leader;
use crate::automaton::State::{CANDIDATE, FOLLOWER, LEADER};
use crate::network::PhysicalCluster;
use crate::storage::volatile::VolatileStorage;
use crate::{Endpoint, Id};

mod candidate;
mod follower;
mod leader;

// TODO: configurable
const HEARTBEAT_INTERVAL_MILLIS: u64 = 20;
const ELECTION_TIMEOUT_BASE_MILLIS: u64 = 250;

pub(super) struct Automaton {}

impl Automaton {
    pub(super) async fn run(
        local_endpoint: Endpoint,
        remote_endpoints: Vec<Endpoint>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let heartbeat_interval = Duration::from_millis(HEARTBEAT_INTERVAL_MILLIS);
        let election_timeout =
            Duration::from_millis(ELECTION_TIMEOUT_BASE_MILLIS + rand::thread_rng().gen_range(0..=250));

        let id = local_endpoint.id();

        let mut storage = VolatileStorage::init();
        info!("Using {} storage", &storage);

        let mut cluster = PhysicalCluster::init(local_endpoint, remote_endpoints).await?;
        info!("{}", &cluster);

        let mut state = State::FOLLOWER {
            id,
            term: 0,
            leader_id: None,
            voted_for: None,
        };
        info!("Starting as: {:?}", state);
        loop {
            state = match match state {
                FOLLOWER {
                    id,
                    term,
                    leader_id,
                    voted_for,
                } => {
                    Follower::init(
                        id,
                        term,
                        leader_id,
                        voted_for,
                        &mut storage,
                        &mut cluster,
                        election_timeout,
                    )
                    .run()
                    .await
                }
                LEADER { id, term } => {
                    Leader::init(id, term, &mut storage, &mut cluster, heartbeat_interval)
                        .run()
                        .await
                }
                CANDIDATE { id, term } => {
                    Candidate::init(id, term, &mut storage, &mut cluster, election_timeout)
                        .run()
                        .await
                }
            } {
                Some(state) => state,
                None => break,
            };
            info!("Switching over to: {:?}", state);
        }
        Ok(())
    }
}

#[derive(Eq, PartialEq, Debug)]
enum State {
    LEADER {
        id: Id,
        term: u64,
    },
    FOLLOWER {
        id: Id,
        term: u64,
        leader_id: Option<Id>,
        voted_for: Option<Id>,
    },
    CANDIDATE {
        id: Id,
        term: u64,
    },
}

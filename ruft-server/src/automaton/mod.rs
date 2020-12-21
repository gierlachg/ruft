use std::error::Error;

use log::info;
use tokio::time::Duration;

use crate::automaton::candidate::Candidate;
use crate::automaton::follower::Follower;
use crate::automaton::leader::Leader;
use crate::automaton::State::{CANDIDATE, FOLLOWER, LEADER};
use crate::network::Cluster;
use crate::{Endpoint, Id};
use rand::Rng;

mod candidate;
mod follower;
mod leader;

// TODO: configurable
const HEARTBEAT_INTERVAL_MILLIS: u64 = 20;
const ELECTION_TIMEOUT_BASE_MILLIS: u64 = 250;

pub(crate) struct Automaton {}

impl Automaton {
    pub(crate) async fn run(
        local_endpoint: Endpoint,
        remote_endpoints: Vec<Endpoint>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let heartbeat_interval = Duration::from_millis(HEARTBEAT_INTERVAL_MILLIS);
        let election_timeout =
            Duration::from_millis(ELECTION_TIMEOUT_BASE_MILLIS + rand::thread_rng().gen_range(0..=250));

        let id = local_endpoint.id();
        let mut cluster = Cluster::init(local_endpoint, remote_endpoints).await?;
        info!("{}", &cluster);

        let mut state = State::FOLLOWER {
            id,
            term: 0,
            leader_id: None,
        };
        info!("Starting as: {:?}", state);
        loop {
            state = match match state {
                FOLLOWER { id, term, leader_id } => {
                    Follower::init(id, term, leader_id, &mut cluster, election_timeout)
                        .run()
                        .await
                }
                LEADER { id, term } => Leader::init(id, term, &mut cluster, heartbeat_interval).run().await,
                CANDIDATE { id, term } => Candidate::init(id, term, &mut cluster, election_timeout).run().await,
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
    LEADER { id: Id, term: u64 },
    FOLLOWER { id: Id, term: u64, leader_id: Option<Id> },
    CANDIDATE { id: Id, term: u64 },
}

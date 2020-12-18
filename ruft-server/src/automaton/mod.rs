use std::error::Error;

use log::info;
use tokio::time::Duration;

use crate::{Endpoint, Id};
use crate::automaton::candidate::Candidate;
use crate::automaton::follower::Follower;
use crate::automaton::leader::Leader;
use crate::automaton::State::{CANDIDATE, FOLLOWER, LEADER};
use crate::network::Cluster;

mod candidate;
mod follower;
mod leader;

// TODO: configurable ?
const LEADER_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(1_500);
const FOLLOWER_HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(2_000);
const CANDIDATE_ELECTION_TIMEOUT: Duration = Duration::from_millis(500);

pub(crate) struct Automaton {}

impl Automaton {
    pub(crate) async fn run(
        local_endpoint: Endpoint,
        remote_endpoints: Vec<Endpoint>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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
                FOLLOWER { id, term, leader_id } =>
                    Follower::init(id, term, leader_id, &mut cluster, FOLLOWER_HEARTBEAT_TIMEOUT)
                        .run()
                        .await,
                LEADER { id, term } =>
                    Leader::init(id, term, &mut cluster, LEADER_HEARTBEAT_INTERVAL)
                        .run()
                        .await,
                CANDIDATE { id, term } =>
                    Candidate::init(id, term, &mut cluster, CANDIDATE_ELECTION_TIMEOUT)
                        .run()
                        .await,
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

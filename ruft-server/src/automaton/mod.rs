use std::net::SocketAddr;
use std::time::Duration;

use derive_more::Display;
use log::info;
use rand::Rng;

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
    id: Id,
    heartbeat_interval: Duration,
    election_timeout: Duration,
    mut log: L,
    mut cluster: C,
    mut relay: R,
) {
    let mut state = if cluster.size() == 1 {
        State::LEADER { term: 1 }
    } else {
        State::FOLLOWER {
            term: 0,
            leader_id: None,
        }
    };
    info!("Starting as: {:?}", state);

    loop {
        state = match state {
            FOLLOWER { term, leader_id } => {
                let election_timeout = election_timeout + Duration::from_millis(rand::thread_rng().gen_range(0..=250));
                Follower::init(
                    id,
                    term,
                    &mut log,
                    &mut cluster,
                    &mut relay,
                    leader_id,
                    election_timeout,
                )
                .run()
                .await
            }
            CANDIDATE { term } => {
                let election_timeout = election_timeout + Duration::from_millis(rand::thread_rng().gen_range(0..=250));
                Candidate::init(id, term, &mut log, &mut cluster, &mut relay, election_timeout)
                    .run()
                    .await
            }
            LEADER { term } => {
                Leader::init(id, term, &mut log, &mut cluster, &mut relay, heartbeat_interval)
                    .run()
                    .await
            }
            TERMINATED => break,
        };
        info!("Switching over to: {:?}", state);
    }
}

#[derive(PartialEq, Eq, Display, Debug)]
enum State {
    #[display(fmt = "LEADER {{ term: {} }}", term)]
    LEADER { term: u64 },
    #[display(fmt = "CANDIDATE {{ term: {} }}", term)]
    CANDIDATE { term: u64 },
    #[display(fmt = "FOLLOWER {{ term: {}, leader id: {:?} }}", term, leader_id)]
    FOLLOWER { term: u64, leader_id: Option<Id> },
    #[display(fmt = "TERMINATED")]
    TERMINATED,
}

impl State {
    fn leader(term: u64) -> Self {
        LEADER { term }
    }

    fn candidate(term: u64) -> Self {
        CANDIDATE { term }
    }

    fn follower(term: u64, leader_id: Option<Id>) -> Self {
        FOLLOWER { term, leader_id }
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

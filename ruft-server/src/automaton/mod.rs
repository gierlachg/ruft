use std::net::SocketAddr;

use derive_more::Display;
use log::info;
use tokio::sync::mpsc;
use tokio::time::Duration;

use crate::automaton::candidate::Candidate;
use crate::automaton::follower::Follower;
use crate::automaton::leader::Leader;
use crate::automaton::State::{CANDIDATE, FOLLOWER, LEADER, TERMINATED};
use crate::cluster::Cluster;
use crate::relay::protocol::Response;
use crate::relay::Relay;
use crate::storage::Storage;
use crate::Id;

mod candidate;
mod follower;
mod leader;

pub(super) async fn run<S: Storage, C: Cluster, R: Relay>(
    id: Id,
    heartbeat_interval: Duration,
    election_timeout: Duration,
    mut storage: S,
    mut cluster: C,
    mut relay: R,
) {
    let mut state = if cluster.size() == 1 {
        State::LEADER { id, term: 1 }
    } else {
        State::FOLLOWER {
            id,
            term: 0,
            leader_id: None,
        }
    };
    info!("Starting as: {}", state);

    loop {
        state = match state {
            FOLLOWER { id, term, leader_id } => {
                Follower::init(
                    id,
                    term,
                    &mut storage,
                    &mut cluster,
                    &mut relay,
                    leader_id,
                    election_timeout,
                )
                .run()
                .await
            }
            CANDIDATE { id, term } => {
                Candidate::init(id, term, &mut storage, &mut cluster, &mut relay, election_timeout)
                    .run()
                    .await
            }
            LEADER { id, term } => {
                Leader::init(id, term, &mut storage, &mut cluster, &mut relay, heartbeat_interval)
                    .run()
                    .await
            }
            TERMINATED => break,
        };
        info!("Switching over to: {}", state);
    }
}

#[derive(PartialEq, Eq, Display, Debug)]
enum State {
    #[display(fmt = "LEADER {{ id: {}, term: {} }}", id, term)]
    LEADER { id: Id, term: u64 },
    #[display(fmt = "CANDIDATE {{ id: {}, term: {} }}", id, term)]
    CANDIDATE { id: Id, term: u64 },
    #[display(fmt = "FOLLOWER {{ id: {}, term: {}, leader id: {:?} }}", id, term, leader_id)]
    FOLLOWER { id: Id, term: u64, leader_id: Option<Id> },
    #[display(fmt = "TERMINATED")]
    TERMINATED,
}

impl State {
    fn leader(id: Id, term: u64) -> Self {
        LEADER { id, term }
    }

    fn follower(id: Id, term: u64, leader_id: Option<Id>) -> Self {
        FOLLOWER { id, term, leader_id }
    }
}

struct Responder(mpsc::UnboundedSender<Response>);

impl Responder {
    fn respond_with_success(self) {
        self.0.send(Response::store_success_response()).unwrap_or(())
    }

    fn respond_with_redirect(&self, address: Option<SocketAddr>) {
        self.0.send(Response::store_redirect_response(address)).unwrap_or(())
    }
}

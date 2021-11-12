use std::fmt::Debug;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::time::Duration;

use derive_more::Display;
use log::info;
use rand::Rng;

use crate::automata::candidate::Candidate;
use crate::automata::follower::Follower;
use crate::automata::fsm::FSM;
use crate::automata::leader::Leader;
use crate::automata::protocol::{Message, Request, Response};
use crate::automata::Transition::{CANDIDATE, FOLLOWER, LEADER, TERMINATED};
use crate::cluster::Cluster;
use crate::relay::Relay;
use crate::storage::{Log, State};
use crate::{Id, Payload, Position};

mod candidate;
mod follower;
mod fsm;
mod leader;
mod protocol;

pub(super) async fn run<S: State, L: Log, C: Cluster<Message>, R: Relay<Request, Response>>(
    id: Id,
    election_timeout: Duration,
    heartbeat_interval: Duration,
    mut state: S,
    mut log: L,
    mut cluster: C,
    mut relay: R,
) {
    let mut fsm = FSM::new();

    let mut transition = FOLLOWER {
        term: state.load().await.unwrap_or(0),
        leader: None,
    };
    info!("Starting as {:?}", transition);
    loop {
        transition = match transition {
            FOLLOWER { term, leader } => {
                state.store(term).await;

                let election_timeout = election_timeout + Duration::from_millis(rand::thread_rng().gen_range(0..=250));
                Follower::init(id, term, &mut log, &mut cluster, &mut relay, leader, election_timeout)
                    .run()
                    .await
            }
            CANDIDATE { term } => {
                state.store(term.get()).await;

                let election_timeout = election_timeout + Duration::from_millis(rand::thread_rng().gen_range(0..=250));
                Candidate::init(id, term, &mut log, &mut cluster, &mut relay, election_timeout)
                    .run()
                    .await
            }
            LEADER { term } => {
                state.store(term.get()).await;

                Leader::init(
                    id,
                    term,
                    &mut log,
                    &mut cluster,
                    &mut relay,
                    &mut fsm,
                    heartbeat_interval,
                )
                .run()
                .await
            }
            TERMINATED => break,
        };
        info!("Switching over to {:?}", transition);
    }
}

#[derive(PartialEq, Eq, Display, Debug)]
enum Transition {
    #[display(fmt = "FOLLOWER {{ term: {}, leader: {:?} }}", term, leader)]
    FOLLOWER { term: u64, leader: Option<Id> },
    #[display(fmt = "CANDIDATE {{ term: {} }}", term)]
    CANDIDATE { term: NonZeroU64 },
    #[display(fmt = "LEADER {{ term: {} }}", term)]
    LEADER { term: NonZeroU64 },
    #[display(fmt = "TERMINATED")]
    TERMINATED,
}

impl Transition {
    fn follower(term: NonZeroU64, leader: Option<Id>) -> Self {
        FOLLOWER {
            term: term.get(),
            leader,
        }
    }

    fn candidate(term: NonZeroU64) -> Self {
        CANDIDATE { term }
    }

    fn leader(term: NonZeroU64) -> Self {
        LEADER { term }
    }
}

struct Responder(tokio::sync::mpsc::UnboundedSender<Response>);

impl Responder {
    fn respond_with_success(self, payload: Option<Payload>) {
        // safety: client already disconnected
        self.0.send(Response::success(payload)).unwrap_or(())
    }

    fn respond_with_redirect(&self, address: Option<SocketAddr>, position: Option<Position>) {
        // safety: client already disconnected
        self.0.send(Response::redirect(address, position)).unwrap_or(())
    }
}

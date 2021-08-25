use std::collections::VecDeque;
use std::error::Error;
use std::net::SocketAddr;

use derive_more::Display;
use log::info;
use rand::Rng;
use tokio::sync::mpsc;
use tokio::time::Duration;

use crate::automaton::candidate::Candidate;
use crate::automaton::follower::Follower;
use crate::automaton::leader::Leader;
use crate::automaton::State::{CANDIDATE, FOLLOWER, LEADER};
use crate::cluster::{Cluster, PhysicalCluster};
use crate::relay::protocol::{Request, Response};
use crate::relay::PhysicalRelay;
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

        let mut cluster = PhysicalCluster::init(local_endpoint.clone(), remote_endpoints).await?;
        info!("{}", &cluster);

        let mut relay = PhysicalRelay::init(local_endpoint.client_address().clone()).await?;
        info!("Listening for client connections on {}", &relay);

        let mut exchanges = Exchanges::new();

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
            state = match match state {
                FOLLOWER { id, term, leader_id } => {
                    Follower::init(
                        id,
                        term,
                        &mut storage,
                        &mut cluster,
                        &mut relay,
                        &mut exchanges,
                        leader_id,
                        election_timeout,
                    )
                    .run()
                    .await
                }
                LEADER { id, term } => {
                    Leader::init(
                        id,
                        term,
                        &mut storage,
                        &mut cluster,
                        &mut relay,
                        &mut exchanges,
                        heartbeat_interval,
                    )
                    .run()
                    .await
                }
                CANDIDATE { id, term } => {
                    Candidate::init(
                        id,
                        term,
                        &mut storage,
                        &mut cluster,
                        &mut relay,
                        &mut exchanges,
                        election_timeout,
                    )
                    .run()
                    .await
                }
            } {
                Some(state) => state,
                None => break,
            };
            info!("Switching over to: {}", state);
        }
        Ok(())
    }
}

#[derive(Eq, PartialEq, Display, Debug)]
enum State {
    #[display(fmt = "LEADER {{ id: {}, term: {} }}", id, term)]
    LEADER { id: Id, term: u64 },
    #[display(fmt = "CANDIDATE {{ id: {}, term: {} }}", id, term)]
    CANDIDATE { id: Id, term: u64 },
    #[display(fmt = "FOLLOWER {{ id: {}, term: {}, leader id: {:?} }}", id, term, leader_id)]
    FOLLOWER { id: Id, term: u64, leader_id: Option<Id> },
}

impl State {
    fn leader(id: Id, term: u64) -> Self {
        LEADER { id, term }
    }

    fn follower(id: Id, term: u64, leader_id: Option<Id>) -> Self {
        FOLLOWER { id, term, leader_id }
    }
}

struct Exchanges(VecDeque<Exchange>);

impl Exchanges {
    fn new() -> Self {
        Exchanges(VecDeque::new()) // TODO: capacity, limit size
    }

    fn enqueue(&mut self, request: Request, responder: Responder) {
        self.0.push_front(Exchange(request, responder))
    }

    fn dequeue(&mut self) -> Option<Exchange> {
        self.0.pop_back()
    }

    fn respond_with_redirect(&mut self, address: &SocketAddr) {
        self.0
            .drain(..)
            .for_each(|exchange| exchange.1.respond_with_redirect(address))
    }
}

struct Exchange(Request, Responder);

struct Responder(mpsc::UnboundedSender<Response>);

impl Responder {
    fn respond_with_success(self) {
        self.0
            .send(Response::store_success_response())
            .expect("This is unexpected!")
    }

    fn respond_with_redirect(&self, address: &SocketAddr) {
        self.0
            .send(Response::store_redirect_response(*address))
            .expect("This is unexpected!")
    }
}

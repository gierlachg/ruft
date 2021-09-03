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
use crate::automaton::State::{CANDIDATE, FOLLOWER, LEADER, TERMINATED};
use crate::cluster::{Cluster, PhysicalCluster};
use crate::relay::protocol::Response;
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

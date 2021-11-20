#![forbid(unsafe_code)]
#![feature(stmt_expr_attributes)]
#![feature(arbitrary_enum_discriminant)]
#![feature(map_entry_replace)]
#![feature(nonzero_ops)]
#![feature(trait_alias)]

use std::cmp::Ordering;
use std::error::Error;
use std::hash::Hash;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tracing::info;

use crate::cluster::PhysicalCluster;
use crate::relay::PhysicalRelay;

mod automata;
mod cluster;
mod relay;
mod storage;

pub async fn run(
    directory: impl AsRef<Path>,
    election_timeout: Duration,
    heartbeat_interval: Duration,
    local: (SocketAddr, SocketAddr),
    remotes: Vec<(SocketAddr, SocketAddr)>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    info!("Initializing Ruft server (version: {})", env!("CARGO_PKG_VERSION"));

    let (local_endpoint, remote_endpoints) = to_endpoints(local, remotes);
    let shutdown = Shutdown::watch();

    let (state, log) = storage::init(directory.as_ref()).await?;

    let cluster = PhysicalCluster::init(local_endpoint.clone(), remote_endpoints, shutdown.clone()).await?;
    info!("{}", &cluster);

    let relay = PhysicalRelay::init(local_endpoint.client_address().clone(), shutdown).await?;
    info!("Listening for client connections on {}", &relay);

    automata::run(
        local_endpoint.id(),
        election_timeout,
        heartbeat_interval,
        state,
        log,
        cluster,
        relay,
    )
    .await;

    info!("Ruft server shut down.");

    Ok(())
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Debug, Serialize, Deserialize)]
struct Id(u8);

impl Id {
    const MAX: u8 = u8::MAX;
}

#[derive(PartialEq, Eq, Clone, Debug)]
struct Endpoint {
    id: Id,
    address: SocketAddr,
    client_address: SocketAddr,
}

impl Endpoint {
    fn new(id: Id, address: SocketAddr, client_address: SocketAddr) -> Self {
        Endpoint {
            id,
            address,
            client_address,
        }
    }

    fn id(&self) -> Id {
        self.id
    }

    fn address(&self) -> &SocketAddr {
        &self.address
    }

    fn client_address(&self) -> &SocketAddr {
        &self.client_address
    }
}

// TODO: better id assignment... ?
fn to_endpoints(local: (SocketAddr, SocketAddr), remotes: Vec<(SocketAddr, SocketAddr)>) -> (Endpoint, Vec<Endpoint>) {
    assert!(
        remotes.len() < usize::from(Id::MAX),
        "Number of members exceeds maximum supported ({})",
        Id::MAX
    );

    let mut endpoints: Vec<(SocketAddr, SocketAddr)> = [vec![local], remotes].concat();

    endpoints.sort_by(|left, right| left.0.cmp(&right.0));
    let mut remote_endpoints = endpoints
        .into_iter()
        .enumerate()
        .map(|(i, endpoint)| Endpoint::new(Id(u8::try_from(i).expect("Unable to convert")), endpoint.0, endpoint.1))
        .collect::<Vec<_>>();
    let local_endpoint_position = remote_endpoints
        .iter()
        .position(|endpoint| endpoint.address == local.0)
        .expect("Where did local endpoint go?!");
    let local_endpoint = remote_endpoints.remove(local_endpoint_position);

    (local_endpoint, remote_endpoints)
}

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Serialize, Deserialize)]
struct Position(u64, u64);

impl Position {
    fn initial() -> Self {
        Position(0, 0)
    }

    fn of(term: u64, index: u64) -> Self {
        Position(term, index)
    }

    fn term(&self) -> u64 {
        self.0
    }

    fn index(&self) -> u64 {
        self.1
    }

    fn next(&self) -> Self {
        Position::of(self.0, self.1 + 1)
    }

    fn next_in(&self, term: NonZeroU64) -> Self {
        if self.0 == term.get() {
            Position::of(term.get(), self.1 + 1)
        } else {
            Position::of(term.get(), 0)
        }
    }
}

impl<'a> PartialEq<Position> for &'a Position {
    fn eq(&self, other: &Position) -> bool {
        *self == other
    }
}

impl PartialOrd for Position {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Position {
    fn cmp(&self, other: &Self) -> Ordering {
        self.term().cmp(&other.term()).then(self.index().cmp(&other.index()))
    }
}

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
struct Payload(Bytes);

impl Payload {
    fn empty() -> Self {
        Payload(Bytes::from(vec![]))
    }

    fn from(bytes: Vec<u8>) -> Self {
        Payload(Bytes::from(bytes))
    }

    fn len(&self) -> u64 {
        u64::try_from(self.0.len()).expect("Unable to convert")
    }
}

impl Serialize for Payload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.0.as_ref())
    }
}

impl<'de> Deserialize<'de> for Payload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // TODO: &[u8] ???
        Vec::<u8>::deserialize(deserializer).map(|bytes| Payload::from(bytes))
    }
}

#[derive(Clone)]
struct Shutdown {
    shutdown: tokio::sync::watch::Receiver<()>,
}

impl Shutdown {
    fn watch() -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        tokio::spawn(async move {
            let _shutdown_tx = shutdown_tx;
            tokio::signal::ctrl_c().await.expect("Failed to listen for event");
        });
        Shutdown { shutdown: shutdown_rx }
    }

    async fn receive(&mut self) -> () {
        // safety: error indicates ctrl-c already received, return anyway
        self.shutdown.changed().await.unwrap_or(())
    }
}

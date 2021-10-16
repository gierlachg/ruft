#![forbid(unsafe_code)]
#![feature(stmt_expr_attributes)]
#![feature(arbitrary_enum_discriminant)]

use std::convert::TryFrom;
use std::error::Error;
use std::hash::Hash;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use derive_more::Display;
use log::info;
use rand::Rng;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::cluster::PhysicalCluster;
use crate::relay::PhysicalRelay;
use crate::storage::volatile::VolatileStorage;

mod automaton;
mod cluster;
mod relay;
mod storage;

// TODO: configurable
const HEARTBEAT_INTERVAL_MILLIS: u64 = 20;
const ELECTION_TIMEOUT_BASE_MILLIS: u64 = 250;

pub async fn run(
    local: (SocketAddr, SocketAddr),
    remotes: Vec<(SocketAddr, SocketAddr)>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    info!("Initializing Ruft server (version: {})", env!("CARGO_PKG_VERSION"));

    let heartbeat_interval = Duration::from_millis(HEARTBEAT_INTERVAL_MILLIS);
    let election_timeout = Duration::from_millis(ELECTION_TIMEOUT_BASE_MILLIS + rand::thread_rng().gen_range(0..=250));

    let (local_endpoint, remote_endpoints) = to_endpoints(local, remotes);
    let shutdown = Shutdown::watch();

    let storage = VolatileStorage::init();
    info!("Using {} storage", &storage);

    let cluster = PhysicalCluster::init(local_endpoint.clone(), remote_endpoints, shutdown.clone()).await?;
    info!("{}", &cluster);

    let relay = PhysicalRelay::init(local_endpoint.client_address().clone(), shutdown).await?;
    info!("Listening for client connections on {}", &relay);

    automaton::run(
        local_endpoint.id(),
        heartbeat_interval,
        election_timeout,
        storage,
        cluster,
        relay,
    )
    .await;

    info!("Ruft server shut down.");

    Ok(())
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Display, Debug, Serialize, Deserialize)]
#[display(fmt = "{:?}", _0)]
struct Id(u8);

impl Id {
    const MAX: u8 = u8::MAX;
}

#[derive(PartialEq, Eq, Clone, Display)]
#[display(fmt = "{{ id: {}, address: {}, client_address: {} }}", id, address, client_address)]
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
        .map(|(i, endpoint)| Endpoint::new(Id(u8::try_from(i).unwrap()), endpoint.0, endpoint.1))
        .collect::<Vec<_>>();
    let local_endpoint_position = remote_endpoints
        .iter()
        .position(|endpoint| endpoint.address == local.0)
        .expect("Where did local endpoint go?!");
    let local_endpoint = remote_endpoints.remove(local_endpoint_position);

    (local_endpoint, remote_endpoints)
}

#[derive(PartialEq, Clone, Debug)]
struct Payload(Bytes);

impl Payload {
    fn from_static(bytes: &'static [u8]) -> Self {
        Payload(Bytes::from_static(bytes))
    }

    fn from(bytes: Vec<u8>) -> Self {
        Payload(Bytes::from(bytes))
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
        // TODO: &[u8]
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

#![forbid(unsafe_code)]
#![feature(stmt_expr_attributes)]

use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::error::Error;
use std::hash::Hash;
use std::net::SocketAddr;
use std::ops::Deref;

use crate::automaton::Automaton;

mod automaton;
mod cluster;
mod relay;
mod storage;

pub struct RuftServer {}

impl RuftServer {
    pub async fn run(
        client_endpoint: SocketAddr,
        local_endpoint: SocketAddr,
        remote_endpoints: Vec<SocketAddr>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (local_endpoint, remote_endpoints) = to_endpoints(local_endpoint, remote_endpoints);
        Automaton::run(client_endpoint, local_endpoint, remote_endpoints).await
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug)]
struct Id(u8);

impl Id {
    pub const MAX: u8 = u8::MAX;
}

impl Deref for Id {
    type Target = u8;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
struct Endpoint {
    id: Id,
    address: SocketAddr,
}

impl Endpoint {
    fn new(id: Id, address: SocketAddr) -> Self {
        Endpoint { id, address }
    }

    fn id(&self) -> Id {
        self.id
    }

    fn address(&self) -> &SocketAddr {
        &self.address
    }
}

// TODO: better id assignment... ?
fn to_endpoints(local_endpoint: SocketAddr, remote_endpoints: Vec<SocketAddr>) -> (Endpoint, Vec<Endpoint>) {
    let mut endpoints = BTreeSet::new();
    endpoints.insert(local_endpoint);
    endpoints.extend(remote_endpoints.into_iter());

    assert!(
        endpoints.len() <= usize::from(Id::MAX),
        "Number of members exceeds maximum supported ({})",
        Id::MAX
    );

    let mut endpoints = endpoints
        .into_iter()
        .enumerate()
        .map(|(i, endpoint)| Endpoint::new(Id(u8::try_from(i).unwrap()), endpoint))
        .collect::<Vec<Endpoint>>();

    let local_endpoint_position = endpoints
        .iter()
        .position(|endpoint| endpoint.address == local_endpoint)
        .expect("Where did local endpoint go?!");
    let local_endpoint = endpoints.remove(local_endpoint_position);

    (local_endpoint, endpoints)
}

#![forbid(unsafe_code)]
#![feature(stmt_expr_attributes)]

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
        local_endpoint: SocketAddr,
        local_client_endpoint: SocketAddr,
        remote_endpoints: Vec<SocketAddr>,
        remote_client_endpoints: Vec<SocketAddr>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        assert_eq!(
            remote_endpoints.len(),
            remote_client_endpoints.len(),
            "Remote endpoints and remote client endpoints lists differ in length"
        );

        let (local_endpoint, remote_endpoints) = to_endpoints(
            local_endpoint,
            local_client_endpoint,
            remote_endpoints,
            remote_client_endpoints,
        );
        Automaton::run(local_endpoint, remote_endpoints).await
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
fn to_endpoints(
    local_endpoint: SocketAddr,
    local_client_endpoint: SocketAddr,
    remote_endpoints: Vec<SocketAddr>,
    remote_client_endpoints: Vec<SocketAddr>,
) -> (Endpoint, Vec<Endpoint>) {
    let mut endpoints: Vec<(SocketAddr, SocketAddr)> = [
        vec![(local_endpoint, local_client_endpoint)],
        remote_endpoints.into_iter().zip(remote_client_endpoints).collect(),
    ]
    .concat();
    assert!(
        endpoints.len() <= usize::from(Id::MAX),
        "Number of members exceeds maximum supported ({})",
        Id::MAX
    );

    endpoints.sort_by(|left, right| left.0.cmp(&right.0));
    let mut remote_endpoints = endpoints
        .into_iter()
        .enumerate()
        .map(|(i, endpoint)| Endpoint::new(Id(u8::try_from(i).unwrap()), endpoint.0, endpoint.1))
        .collect::<Vec<_>>();
    let local_endpoint_position = remote_endpoints
        .iter()
        .position(|endpoint| endpoint.address == local_endpoint)
        .expect("Where did local endpoint go?!");
    let local_endpoint = remote_endpoints.remove(local_endpoint_position);

    (local_endpoint, remote_endpoints)
}

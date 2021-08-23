use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bytes::Bytes;
use portpicker::pick_unused_port;

use ruft_client::RuftClient;
use ruft_server::RuftServer;

#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "Unable to connect to the cluster")]
async fn test_client_connection_timeout() {
    // try to start client
    RuftClient::new(vec![address()], 1).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_successful_store() {
    let client_endpoints = addresses(2);
    let local_endpoints = addresses(2);

    // start 2 node cluster
    spawn_node(client_endpoints[0], local_endpoints[0], vec![local_endpoints[1]]);
    spawn_node(client_endpoints[1], local_endpoints[1], vec![local_endpoints[0]]);

    // start client
    let mut client = RuftClient::new(vec![client_endpoints[0]], 5_000).await.unwrap();

    // store some payload
    let result = client.store(Bytes::from_static(&[1])).await;

    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_successful_store_single_node() {
    let client_endpoint = address();
    let local_endpoint = address();

    // start single node cluster
    spawn_node(client_endpoint, local_endpoint, vec![]);

    // start client
    let mut client = RuftClient::new(vec![client_endpoint], 5_000).await.unwrap();

    // store some payload
    let result = client.store(Bytes::from_static(&[1])).await;

    assert!(result.is_ok());
}

fn spawn_node(client_endpoint: SocketAddr, local_endpoint: SocketAddr, remote_endpoints: Vec<SocketAddr>) {
    tokio::spawn(async move { RuftServer::run(client_endpoint, local_endpoint, remote_endpoints).await });
}

fn addresses(count: usize) -> Vec<SocketAddr> {
    (0..=count).map(|_| address()).collect()
}

fn address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), pick_unused_port().unwrap())
}

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
    let endpoints = addresses(2);
    let client_endpoints = addresses(2);

    // start 2 node cluster
    spawn_node(
        endpoints[0],
        client_endpoints[0],
        vec![endpoints[1]],
        vec![client_endpoints[1]],
    );
    spawn_node(
        endpoints[1],
        client_endpoints[1],
        vec![endpoints[0]],
        vec![client_endpoints[0]],
    );

    // start client
    let mut client = RuftClient::new(vec![client_endpoints[0]], 5_000).await.unwrap();

    // store some payload
    let result = client.store(Bytes::from_static(&[1])).await;

    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_successful_store_single_node() {
    let local_endpoint = address();
    let client_endpoint = address();

    // start single node cluster
    spawn_node(local_endpoint, client_endpoint, vec![], vec![]);

    // start client
    let mut client = RuftClient::new(vec![client_endpoint], 5_000).await.unwrap();

    // store some payload
    let result = client.store(Bytes::from_static(&[1])).await;

    assert!(result.is_ok());
}

fn spawn_node(
    local_endpoint: SocketAddr,
    local_client_endpoint: SocketAddr,
    remote_endpoints: Vec<SocketAddr>,
    remote_client_endpoints: Vec<SocketAddr>,
) {
    tokio::spawn(async move {
        RuftServer::run(
            local_endpoint,
            local_client_endpoint,
            remote_endpoints,
            remote_client_endpoints,
        )
        .await
    });
}

fn addresses(count: usize) -> Vec<SocketAddr> {
    (0..=count).map(|_| address()).collect()
}

fn address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), pick_unused_port().unwrap())
}

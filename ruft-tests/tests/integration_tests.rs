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
    let client_endpoint = address();
    let local_endpoint = address();

    // start single node cluster
    tokio::spawn(async move { RuftServer::run(client_endpoint, local_endpoint, vec![]).await });

    // start client
    let mut client = RuftClient::new(vec![client_endpoint], 5_000).await.unwrap();

    // store some payload
    let result = client.send(Bytes::from_static(&[1])).await;

    assert!(result.is_ok());
}

fn address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), pick_unused_port().unwrap())
}

use bytes::Bytes;

use ruft_client::RuftClient;
use ruft_server::RuftServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successful_store() {
    let client_endpoint = "127.0.0.1:8080".parse().unwrap();
    let local_endpoint = "127.0.0.1:36969".parse().unwrap();

    // start single node cluster
    tokio::spawn(async move { RuftServer::run(client_endpoint, local_endpoint, vec![]).await });

    // start client
    let mut client = RuftClient::new(vec![client_endpoint]).await.unwrap();

    // store some payload
    let result = client.store(Bytes::from_static(&[1])).await;

    assert!(result.is_ok());
}

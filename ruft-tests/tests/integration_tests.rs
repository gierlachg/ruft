use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;

use portpicker::pick_unused_port;
use rand::Rng;

use ruft_client::RuftClient;

#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "Unable to connect to the cluster")]
async fn test_client_connection_timeout() {
    // try to start client
    RuftClient::new(vec![address()], 1).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_map_read() {
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

    // read
    let value = client.read("map", &1u64.to_le_bytes()).await.unwrap();

    assert_eq!(value, None);
}

#[tokio::test(flavor = "current_thread")]
async fn test_map_store() {
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

    // write
    let result = client.write("map", &1u64.to_le_bytes(), "1".as_bytes()).await;
    assert!(result.is_ok());

    let result = client.write("map", &1u64.to_le_bytes(), "2".as_bytes()).await;
    assert!(result.is_ok());

    // read
    let value = client.read("map", &1u64.to_le_bytes()).await.unwrap().unwrap();
    assert_eq!(String::from_utf8(value).unwrap(), "2");

    let value = client.read("map", &2u64.to_le_bytes()).await.unwrap();
    assert_eq!(value, None);
}

#[tokio::test(flavor = "current_thread")]
async fn test_map_store_single_node() {
    let local_endpoint = address();
    let client_endpoint = address();

    // start single node cluster
    spawn_node(local_endpoint, client_endpoint, vec![], vec![]);

    // start client
    let mut client = RuftClient::new(vec![client_endpoint], 5_000).await.unwrap();

    // write
    let result = client.write("map", &1u64.to_le_bytes(), "1".as_bytes()).await;
    assert!(result.is_ok());

    let result = client.write("map", &1u64.to_le_bytes(), "2".as_bytes()).await;
    assert!(result.is_ok());

    // read
    let value = client.read("map", &1u64.to_le_bytes()).await.unwrap().unwrap();

    assert_eq!(String::from_utf8(value).unwrap(), "2");
}

fn spawn_node(
    local_endpoint: SocketAddr,
    local_client_endpoint: SocketAddr,
    remote_endpoints: Vec<SocketAddr>,
    remote_client_endpoints: Vec<SocketAddr>,
) {
    tokio::spawn(async move {
        ruft_server::run(
            EphemeralDirectory::new(),
            (local_endpoint, local_client_endpoint),
            remote_endpoints.into_iter().zip(remote_client_endpoints).collect(),
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

struct EphemeralDirectory(String);

impl<'a> EphemeralDirectory {
    fn new() -> Self {
        let mut directory = String::from("../target/tmp/ruft-");
        rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .map(char::from)
            .take(10)
            .for_each(|c| directory.push(c));
        std::fs::create_dir_all(&directory).expect("Unable to create directory");
        EphemeralDirectory(directory)
    }
}

impl AsRef<Path> for EphemeralDirectory {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

impl Drop for EphemeralDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("Unable to remove directory")
    }
}

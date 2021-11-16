use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use clap::{App, Arg, ArgMatches};
use tracing::Level;

const DIRECTORY: &str = "directory";
const LOCAL_ENDPOINT: &str = "local endpoint";
const LOCAL_CLIENT_ENDPOINT: &str = "local client endpoint";
const REMOTE_ENDPOINTS: &str = "remote endpoints";
const REMOTE_CLIENT_ENDPOINTS: &str = "remote client endpoints";

const ELECTION_TIMEOUT: &str = "election timeout";
const DEFAULT_ELECTION_TIMEOUT: &str = "250";

const HEARTBEAT_INTERVAL: &str = "heartbeat interval";
const DEFAULT_HEARTBEAT_INTERVAL: &str = "20";

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let arguments = parse_arguments();
    let directory = arguments.value_of(DIRECTORY).unwrap();
    let election_timeout = arguments
        .value_of(ELECTION_TIMEOUT)
        .and_then(|election_timeout| u64::from_str(election_timeout).ok())
        .map(Duration::from_millis)
        .unwrap();
    let heartbeat_interval = arguments
        .value_of(HEARTBEAT_INTERVAL)
        .and_then(|heartbeat_interval| u64::from_str(heartbeat_interval).ok())
        .map(Duration::from_millis)
        .unwrap();
    let local_endpoint = arguments
        .value_of(LOCAL_ENDPOINT)
        .map(|local_endpoint| parse_address(local_endpoint))
        .unwrap();
    let local_client_endpoint = arguments
        .value_of(LOCAL_CLIENT_ENDPOINT)
        .map(|client_endpoint| parse_address(client_endpoint))
        .unwrap();
    let remote_endpoints = arguments
        .value_of(REMOTE_ENDPOINTS)
        .into_iter()
        .flat_map(|remote_endpoints| remote_endpoints.split(','))
        .map(|remote_endpoint| parse_address(remote_endpoint))
        .collect::<Vec<_>>();
    let remote_client_endpoints = arguments
        .value_of(REMOTE_CLIENT_ENDPOINTS)
        .into_iter()
        .flat_map(|remote_endpoints| remote_endpoints.split(','))
        .map(|remote_endpoint| parse_address(remote_endpoint))
        .collect::<Vec<_>>();

    init_tracing();

    tokio_uring::start(async {
        ruft_server::run(
            directory,
            election_timeout,
            heartbeat_interval,
            (local_endpoint, local_client_endpoint),
            remote_endpoints.into_iter().zip(remote_client_endpoints).collect(),
        )
        .await
    })
}

fn parse_arguments() -> ArgMatches<'static> {
    App::new(env!("CARGO_PKG_DESCRIPTION"))
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::with_name(DIRECTORY)
                .required(true)
                .long("directory")
                .alias("d")
                .takes_value(true)
                .help("Path to the data directory"),
        )
        .arg(
            Arg::with_name(LOCAL_ENDPOINT)
                .required(true)
                .long("local-endpoint")
                .alias("le")
                .takes_value(true)
                .help("Local endpoint server should bind to"),
        )
        .arg(
            Arg::with_name(LOCAL_CLIENT_ENDPOINT)
                .required(true)
                .long("local-client-endpoint")
                .alias("lce")
                .takes_value(true)
                .help("Local client endpoint clients connect to"),
        )
        .arg(
            Arg::with_name(REMOTE_ENDPOINTS)
                .required(true)
                .long("remote-endpoints")
                .alias("re")
                .takes_value(true)
                .help("Comma separated list of remote endpoints"),
        )
        .arg(
            Arg::with_name(REMOTE_CLIENT_ENDPOINTS)
                .required(true)
                .long("remote-client-endpoints")
                .alias("rce")
                .takes_value(true)
                .help("Comma separated list of remote client endpoints"),
        )
        .arg(
            Arg::with_name(ELECTION_TIMEOUT)
                .required(false)
                .default_value(DEFAULT_ELECTION_TIMEOUT)
                .long("election-timeout")
                .alias("et")
                .takes_value(true)
                .help("Election timeout (in milliseconds)"),
        )
        .arg(
            Arg::with_name(HEARTBEAT_INTERVAL)
                .required(false)
                .default_value(DEFAULT_HEARTBEAT_INTERVAL)
                .long("heartbeat-interval")
                .alias("hi")
                .takes_value(true)
                .help("Heartbeat interval (in milliseconds)"),
        )
        .get_matches()
}

fn parse_address(address: &str) -> SocketAddr {
    address.parse().expect(&format!("Unable to parse '{}'", address))
}

fn init_tracing() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
}

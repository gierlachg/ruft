use std::error::Error;
use std::fs;
use std::net::SocketAddr;

use clap::{App, Arg, ArgMatches};
use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Config, Root};

const DATA_PATH: &str = "data path";
const LOCAL_ENDPOINT: &str = "local endpoint";
const LOCAL_CLIENT_ENDPOINT: &str = "local client endpoint";
const REMOTE_ENDPOINTS: &str = "remote endpoints";
const REMOTE_CLIENT_ENDPOINTS: &str = "remote client endpoints";

const LOGGING_CONFIGURATION_FILE_NAME: &str = "log4rs.yml";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let arguments = parse_arguments();
    let data_path = arguments.value_of(DATA_PATH).unwrap();
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

    init_logging();

    ruft_server::run(
        data_path,
        (local_endpoint, local_client_endpoint),
        remote_endpoints.into_iter().zip(remote_client_endpoints).collect(),
    )
    .await
}

fn parse_arguments() -> ArgMatches<'static> {
    App::new(env!("CARGO_PKG_DESCRIPTION"))
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::with_name(DATA_PATH)
                .required(true)
                .long("data-path")
                .alias("dp")
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
        .get_matches()
}

fn parse_address(address: &str) -> SocketAddr {
    address.parse().expect(&format!("Unable to parse '{}'", address))
}

fn init_logging() {
    match fs::metadata(LOGGING_CONFIGURATION_FILE_NAME) {
        Ok(_) => log4rs::init_file(LOGGING_CONFIGURATION_FILE_NAME, Default::default()).unwrap(),
        Err(_) => {
            let _ = log4rs::init_config(
                Config::builder()
                    .appender(Appender::builder().build("stdout", Box::new(ConsoleAppender::builder().build())))
                    .build(Root::builder().appender("stdout").build(LevelFilter::Info))
                    .unwrap(),
            );
        }
    }
}

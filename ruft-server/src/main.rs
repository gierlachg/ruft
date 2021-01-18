#![feature(map_first_last)]

use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::error::Error;
use std::fs;
use std::net::SocketAddr;

use clap::{App, Arg, ArgMatches};
use derive_more::Display;
use log::{info, LevelFilter};
use log4rs::{
    append::console::ConsoleAppender,
    config::{Appender, Config, Root},
};
use tokio;

use crate::automaton::Automaton;

mod automaton;
mod network;
mod protocol;
mod storage;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const LOCAL_ENDPOINT: &str = "local endpoint";
const REMOTE_ENDPOINTS: &str = "remote endpoint";
const LOGGING_CONFIGURATION_FILE_NAME: &str = "log4rs.yml";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let arguments = parse_arguments();
    let local_endpoint = arguments
        .value_of(LOCAL_ENDPOINT)
        .map(|local_endpoint| parse_address(local_endpoint))
        .unwrap();
    let remote_endpoints = arguments
        .value_of(REMOTE_ENDPOINTS)
        .into_iter()
        .flat_map(|remote_endpoints| remote_endpoints.split(','))
        .map(|remote_endpoint| parse_address(remote_endpoint))
        .collect::<Vec<_>>();
    let (local_endpoint, remote_endpoints) = to_endpoints(local_endpoint, remote_endpoints);

    init_logging();

    info!("Initializing Ruft server (version: {})", VERSION);
    Automaton::run(local_endpoint, remote_endpoints).await?;
    info!("Ruft server shut down.");
    Ok(())
}

fn parse_arguments() -> ArgMatches<'static> {
    App::new(env!("CARGO_PKG_DESCRIPTION"))
        .version(VERSION)
        .arg(
            Arg::with_name(LOCAL_ENDPOINT)
                .required(true)
                .short("le")
                .long("local-endpoint")
                .takes_value(true)
                .help("Local endpoint server should bind to"),
        )
        .arg(
            Arg::with_name(REMOTE_ENDPOINTS)
                .required(true)
                .short("re")
                .long("remote-endpoints")
                .takes_value(true)
                .help("Comma separated list of remote endpoints"),
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

type Id = u8;

#[derive(Eq, PartialEq, Display, Clone)]
#[display(fmt = "Endpoint {{id: {}, address: {}}}", id, address)]
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

// TODO: better id assignment...
fn to_endpoints(local_endpoint: SocketAddr, remote_endpoints: Vec<SocketAddr>) -> (Endpoint, Vec<Endpoint>) {
    let mut endpoints = BTreeSet::new();
    endpoints.insert(local_endpoint);
    endpoints.extend(remote_endpoints.into_iter());

    assert!(
        endpoints.len() < usize::from(u8::MAX),
        "Number of members exceeds maximum supported ({})",
        u8::MAX
    );

    let mut endpoints = endpoints
        .into_iter()
        .enumerate()
        .map(|(i, endpoint)| Endpoint::new(Id::try_from(i).unwrap(), endpoint))
        .collect::<Vec<Endpoint>>();

    let local_endpoint_position = endpoints
        .iter()
        .position(|endpoint| endpoint.address == local_endpoint)
        .expect("Where did local endpoint go?!");
    let local_endpoint = endpoints.remove(local_endpoint_position);

    (local_endpoint, endpoints)
}

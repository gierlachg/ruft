#![forbid(unsafe_code)]

use std::net::SocketAddr;

use bytes::Bytes;
use log::info;
use thiserror::Error;

use crate::relay::protocol::Request;
use crate::relay::Relay;

mod relay;

const VERSION: &str = env!("CARGO_PKG_VERSION");

type Result<T> = std::result::Result<T, RuftClientError>;

#[derive(Clone)]
pub struct RuftClient {
    relay: Relay,
}

impl RuftClient {
    pub async fn new(endpoints: Vec<SocketAddr>, connection_timeout_millis: u64) -> Result<Self> {
        info!("Initializing Ruft client (version: {})", VERSION);
        let relay = Relay::init(endpoints, connection_timeout_millis).await?;
        info!("Ruft client initialized");

        Ok(RuftClient { relay })
    }

    pub async fn send(&mut self, payload: Bytes) -> Result<()> {
        let request = Request::store_request(payload);
        self.relay.send(request).await
    }
}

#[derive(Error, Debug)]
pub enum RuftClientError {
    #[error("error")]
    GenericFailure(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("network error")]
    GenericNetworkFailure(#[from] std::io::Error),
}

impl RuftClientError {
    fn generic_failure(message: &str) -> Self {
        RuftClientError::GenericFailure(message.into())
    }
}

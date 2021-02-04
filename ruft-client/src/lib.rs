use std::net::SocketAddr;

use bytes::Bytes;
use log::info;
use thiserror::Error;

use crate::relay::Relay;

mod relay;

const VERSION: &str = env!("CARGO_PKG_VERSION");

type Result<T> = std::result::Result<T, RuftClientError>;

#[derive(Error, Debug)]
pub enum RuftClientError {
    #[error("error")]
    GenericFailure(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("network error")]
    GenericNetworkFailure(#[from] std::io::Error),
}

pub struct RuftClient {
    relay: Relay,
}

impl RuftClient {
    pub async fn new<E>(endpoints: E) -> Result<Self>
    where
        E: IntoIterator<Item = SocketAddr>,
    {
        info!("Initializing Ruft client (version: {})", VERSION);
        let relay = Relay::init(endpoints).await?;

        Ok(RuftClient { relay })
    }

    pub async fn store(&mut self, payload: Bytes) {
        self.relay.store(payload).await
    }
}
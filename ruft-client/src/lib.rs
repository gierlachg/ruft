#![forbid(unsafe_code)]
#![feature(arbitrary_enum_discriminant)]

use std::net::SocketAddr;

use bytes::Bytes;
use log::info;
use thiserror::Error;

use crate::relay::protocol::{Operation, Payload, Request};
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

    pub async fn write(&mut self, id: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let key = Bytes::from(key.to_vec());
        let value = Bytes::from(value.to_vec());
        let operation = Operation::map_write(id, key, value).into();
        let request = Request::write(operation, None);
        self.relay.send(request).await?;
        Ok(())
    }

    pub async fn read(&mut self, id: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = Bytes::from(key.to_vec());
        let operation = Operation::map_read(id, key).into();
        let request = Request::read(operation);
        self.relay
            .send(request)
            .await
            .map(|payload| payload.map(Payload::inner))
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

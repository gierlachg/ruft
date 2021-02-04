use std::error::Error;
use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;

use crate::relay::connection::Ingress;

mod connection;
mod tcp;

#[async_trait]
pub(crate) trait Relay {
    async fn receive(&mut self) -> Option<Bytes>;
}

pub(crate) struct PhysicalRelay {
    ingress: Ingress,
}

impl PhysicalRelay {
    pub(crate) async fn init(client_endpoint: SocketAddr) -> Result<Self, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        let ingress = Ingress::bind(client_endpoint).await?;

        Ok(PhysicalRelay { ingress })
    }
}

#[async_trait]
impl Relay for PhysicalRelay {
    async fn receive(&mut self) -> Option<Bytes> {
        self.ingress.next().await
    }
}

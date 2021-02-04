use std::error::Error;
use std::net::SocketAddr;

use async_trait::async_trait;

use crate::relay::connection::Ingress;
use crate::relay::protocol::Message;

mod connection;
pub(crate) mod protocol;
mod tcp;

#[async_trait]
pub(crate) trait Relay {
    async fn receive(&mut self) -> Option<Message>;
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
    async fn receive(&mut self) -> Option<Message> {
        self.ingress.next().await.map(Message::from)
    }
}

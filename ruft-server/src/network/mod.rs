use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join_all;

use crate::network::connection::{Egress, Ingress};
use crate::protocol::Message;
use crate::{Endpoint, Id};

mod connection;
mod tcp;

#[async_trait]
pub(crate) trait Relay {
    async fn send(&mut self, member_id: &Id, message: Message);

    async fn broadcast(&mut self, message: Message);

    async fn receive(&mut self) -> Option<Message>;

    fn size(&self) -> usize;
}

pub(crate) struct Cluster {
    egresses: HashMap<Id, Egress>,
    ingress: Ingress,
}

impl Cluster {
    pub(crate) async fn init(
        local_endpoint: Endpoint,
        remote_endpoints: Vec<Endpoint>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        let futures = remote_endpoints.into_iter().map(Egress::connect).collect::<Vec<_>>();
        let egresses = join_all(futures)
            .await
            .into_iter()
            .map(|member| (member.id(), member))
            .collect::<HashMap<Id, Egress>>();

        let ingress = Ingress::bind(local_endpoint).await?;

        Ok(Cluster { egresses, ingress })
    }
}

#[async_trait]
impl Relay for Cluster {
    async fn send(&mut self, member_id: &Id, message: Message) {
        let message = message.into();

        self.egresses.get_mut(member_id).unwrap().send(message).await;
    }

    async fn broadcast(&mut self, message: Message) {
        let message: Bytes = message.into();

        let futures = self
            .egresses
            .values_mut()
            .map(|egress| egress.send(message.clone()))
            .collect::<Vec<_>>();
        join_all(futures).await;
    }

    async fn receive(&mut self) -> Option<Message> {
        self.ingress.next().await.map(Message::from)
    }

    fn size(&self) -> usize {
        self.egresses.len()
    }
}

impl Display for Cluster {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "\n\
        \n\
        Members {{size:{}}} [\n\
        \t{}\n\
        ]\n\
         ",
            self.egresses.len(),
            self.egresses
                .values()
                .map(|egress| egress.to_string())
                .collect::<Vec<String>>()
                .join("\n\t")
        )
    }
}

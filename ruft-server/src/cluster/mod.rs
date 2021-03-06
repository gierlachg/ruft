use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join_all;

use crate::cluster::connection::{Egress, Ingress};
use crate::cluster::protocol::ServerMessage;
use crate::{Endpoint, Id};

mod connection;
pub(crate) mod protocol;
mod tcp;

#[async_trait]
pub(crate) trait Cluster {
    fn member_ids(&self) -> Vec<Id>;

    fn size(&self) -> usize;

    async fn send(&self, member_id: &Id, message: ServerMessage);

    async fn broadcast(&self, message: ServerMessage);

    async fn receive(&mut self) -> Option<ServerMessage>;
}

pub(crate) struct PhysicalCluster {
    egresses: HashMap<Id, Egress>,
    ingress: Ingress,
}

impl PhysicalCluster {
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
            .map(|egress| (egress.id(), egress))
            .collect::<HashMap<Id, Egress>>();

        let ingress = Ingress::bind(local_endpoint).await?;

        Ok(PhysicalCluster { egresses, ingress })
    }
}

#[async_trait]
impl Cluster for PhysicalCluster {
    fn member_ids(&self) -> Vec<Id> {
        self.egresses.keys().map(|id| id.clone()).collect()
    }

    fn size(&self) -> usize {
        self.egresses.len() + 1
    }

    async fn send(&self, member_id: &Id, message: ServerMessage) {
        match self.egresses.get(&member_id) {
            Some(egress) => egress.send(message.into()).await,
            None => panic!("Missing member of id: {}", member_id),
        }
    }

    async fn broadcast(&self, message: ServerMessage) {
        let message: Bytes = message.into();

        let futures = self
            .egresses
            .values()
            .map(|egress| egress.send(message.clone()))
            .collect::<Vec<_>>();
        join_all(futures).await;
    }

    async fn receive(&mut self) -> Option<ServerMessage> {
        self.ingress.next().await.map(ServerMessage::from)
    }
}

impl Display for PhysicalCluster {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let mut endpoints = BTreeSet::new();
        endpoints.insert(self.ingress.to_string());
        endpoints.extend(self.egresses.values().into_iter().map(Egress::to_string));

        write!(
            formatter,
            "\n\
        \n\
        Members {{ size:{} }} [\n\
        \t{}\n\
        ]\n\
         ",
            endpoints.len(),
            endpoints.into_iter().collect::<Vec<_>>().join("\n\t")
        )
    }
}

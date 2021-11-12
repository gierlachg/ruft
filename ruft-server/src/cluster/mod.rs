use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join_all;

use crate::cluster::connection::{Egress, Ingress};
use crate::cluster::protocol::Message;
use crate::{Endpoint, Id, Shutdown};

mod connection;
pub(crate) mod protocol; // TODO: ???
mod tcp;

#[async_trait]
pub(crate) trait Cluster {
    fn members(&self) -> Vec<Id>; // TODO: ???

    fn endpoint(&self, id: &Id) -> &Endpoint; // TODO: ???

    fn is_majority(&self, n: usize) -> bool;

    async fn send(&self, member_id: &Id, message: Message);

    async fn broadcast(&self, message: Message);

    async fn messages(&mut self) -> Option<Message>;
}

pub(crate) struct PhysicalCluster {
    egresses: HashMap<Id, Egress>,
    ingress: Ingress,
}

impl PhysicalCluster {
    pub(crate) async fn init(
        local_endpoint: Endpoint,
        remote_endpoints: Vec<Endpoint>,
        shutdown: Shutdown,
    ) -> Result<Self, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        let futures = remote_endpoints.into_iter().map(Egress::connect).collect::<Vec<_>>();
        let egresses = join_all(futures)
            .await
            .into_iter()
            .map(|egress| (egress.endpoint().id(), egress))
            .collect::<HashMap<Id, Egress>>();

        let ingress = Ingress::bind(local_endpoint, shutdown).await?;

        Ok(PhysicalCluster { egresses, ingress })
    }
}

#[async_trait]
impl Cluster for PhysicalCluster {
    // TODO:
    fn members(&self) -> Vec<Id> {
        self.egresses.keys().map(|id| *id).collect()
    }

    // TODO:
    fn endpoint(&self, id: &Id) -> &Endpoint {
        if *id == self.ingress.endpoint().id() {
            &self.ingress.endpoint()
        } else {
            self.egresses
                .get(id)
                .expect(&format!("Missing member for id: {:?}", id))
                .endpoint()
        }
    }

    fn is_majority(&self, n: usize) -> bool {
        n > (self.egresses.len() + 1) / 2
    }

    async fn send(&self, member_id: &Id, message: Message) {
        match self.egresses.get(&member_id) {
            Some(egress) => egress.send(message.into()).await,
            None => panic!("Missing member of id: {:?}", member_id),
        }
    }

    async fn broadcast(&self, message: Message) {
        let bytes: Bytes = message.into();
        let futures = self
            .egresses
            .values()
            .map(|egress| egress.send(bytes.clone()))
            .collect::<Vec<_>>();
        join_all(futures).await;
    }

    async fn messages(&mut self) -> Option<Message> {
        self.ingress.next().await
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

use bytes::Bytes;
use log::info;
use tokio::time::{self, Duration};

use crate::automaton::State;
use crate::network::Cluster;
use crate::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::storage::{Position, Storage};
use crate::Id;

pub(super) struct Follower<'a, S: Storage, C: Cluster> {
    id: Id,
    term: u64,
    leader_id: Option<Id>,
    storage: &'a mut S,
    cluster: &'a mut C,

    election_timeout: Duration,
}

impl<'a, S: Storage, C: Cluster> Follower<'a, S, C> {
    pub(super) fn init(
        id: Id,
        term: u64,
        leader_id: Option<Id>,
        storage: &'a mut S,
        cluster: &'a mut C,
        election_timeout: Duration,
    ) -> Self {
        Follower {
            id,
            term,
            leader_id,
            storage,
            cluster,
            election_timeout,
        }
    }

    pub(super) async fn run(&mut self) -> Option<State> {
        loop {
            // TODO: replace select! with something that actually works (!sic) here
            tokio::select! {
                _ = time::sleep(self.election_timeout) => {
                    return Some(State::CANDIDATE{id: self.id, term: self.term})
                }
                message = self.cluster.receive() => {
                    match message {
                        Some(message) => {
                            match message {
                                AppendRequest { leader_id, preceding_position, term, entries } => {
                                    self.on_append_request(leader_id, preceding_position, term, entries).await
                                }
                                AppendResponse { member_id: _, success: _, position: _ } => {},
                                VoteRequest {candidate_id, term} => self.on_vote_request(candidate_id, term).await,
                                VoteResponse { vote_granted : _, term: _ } => {},
                            }
                        }
                        None => break
                    }
                }
            }
        }
        None
    }

    async fn on_append_request(&mut self, leader_id: Id, preceding_position: Position, term: u64, entries: Vec<Bytes>) {
        if term >= self.term {
            self.term = term;
            self.leader_id = Some(leader_id);
        }

        match self.storage.insert(&preceding_position, term, entries).await {
            Ok(position) => {
                info!("Accepted: {:?}", position);
                self.cluster
                    .send(&leader_id, Message::append_response(self.id, true, position))
                    .await
            }
            Err(position) => {
                info!("Missing: {:?}", position);
                self.cluster
                    .send(&leader_id, Message::append_response(self.id, false, position))
                    .await
            }
        }
    }

    async fn on_vote_request(&mut self, candidate_id: Id, term: u64) {
        if term > self.term {
            self.term = term;
            self.leader_id = None;

            self.cluster
                .send(&candidate_id, Message::vote_response(true, self.term))
                .await;
        } else {
            self.cluster
                .send(&candidate_id, Message::vote_response(false, self.term))
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::fmt::{Display, Formatter};

    use async_trait::async_trait;
    use bytes::Bytes;
    use mockall::{mock, predicate};
    use predicate::eq;
    use tokio::time::Duration;

    use crate::protocol::Message;
    use crate::storage::Position;
    use crate::Id;

    use super::*;

    const LOCAL_ID: u8 = 1;
    const PEER_1_ID: u8 = 2;

    const LOCAL_TERM: u64 = 10;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_successful_insert_then_update_and_respond() {
        let preceding_position = Position::of(0, 0);

        let mut storage = MockStorage::new();
        storage
            .expect_insert()
            .with(eq(preceding_position), eq(LOCAL_TERM + 1), eq(vec![]))
            .returning(|position, _, _| Ok(position.clone()));
        let mut cluster = MockCluster::new();
        cluster
            .expect_send()
            .with(
                eq(PEER_1_ID),
                eq(Message::append_response(LOCAL_ID, true, preceding_position)),
            )
            .return_const(());
        let mut follower = follower(&mut storage, &mut cluster, None);

        follower
            .on_append_request(PEER_1_ID, preceding_position, LOCAL_TERM + 1, vec![])
            .await;
        assert_eq!(follower.term, LOCAL_TERM + 1);
        assert_eq!(follower.leader_id, Some(PEER_1_ID));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_erroneous_insert_then_update_and_respond() {
        let preceding_position = Position::of(1, 0);

        let mut storage = MockStorage::new();
        storage
            .expect_insert()
            .with(eq(preceding_position), eq(LOCAL_TERM + 1), eq(vec![]))
            .returning(|position, _, _| Err(position.clone()));
        let mut cluster = MockCluster::new();
        cluster
            .expect_send()
            .with(
                eq(PEER_1_ID),
                eq(Message::append_response(LOCAL_ID, false, preceding_position)),
            )
            .return_const(());
        let mut follower = follower(&mut storage, &mut cluster, None);

        follower
            .on_append_request(PEER_1_ID, preceding_position, LOCAL_TERM + 1, vec![])
            .await;
        assert_eq!(follower.term, LOCAL_TERM + 1);
        assert_eq!(follower.leader_id, Some(PEER_1_ID));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_greater_then_respond_and_update() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        cluster
            .expect_send()
            .with(eq(PEER_1_ID), eq(Message::vote_response(true, LOCAL_TERM + 1)))
            .return_const(());
        let mut follower = follower(&mut storage, &mut cluster, None);

        follower.on_vote_request(PEER_1_ID, LOCAL_TERM + 1).await;
        assert_eq!(follower.term, LOCAL_TERM + 1);
        assert_eq!(follower.leader_id, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_equal_then_respond() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        cluster
            .expect_send()
            .with(eq(PEER_1_ID), eq(Message::vote_response(false, LOCAL_TERM)))
            .return_const(());
        let mut follower = follower(&mut storage, &mut cluster, Some(PEER_1_ID));

        follower.on_vote_request(PEER_1_ID, LOCAL_TERM).await;
        assert_eq!(follower.term, LOCAL_TERM);
        assert_eq!(follower.leader_id, Some(PEER_1_ID));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_less_then_respond() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        cluster
            .expect_send()
            .with(eq(PEER_1_ID), eq(Message::vote_response(false, LOCAL_TERM)))
            .return_const(());
        let mut follower = follower(&mut storage, &mut cluster, Some(PEER_1_ID));

        follower.on_vote_request(PEER_1_ID, LOCAL_TERM - 1).await;
        assert_eq!(follower.term, LOCAL_TERM);
        assert_eq!(follower.leader_id, Some(PEER_1_ID));
    }

    fn follower<'a>(
        storage: &'a mut MockStorage,
        cluster: &'a mut MockCluster,
        leader_id: Option<Id>,
    ) -> Follower<'a, MockStorage, MockCluster> {
        Follower::init(
            LOCAL_ID,
            LOCAL_TERM,
            leader_id,
            storage,
            cluster,
            Duration::from_secs(1),
        )
    }

    mock! {
        Storage {}
        #[async_trait]
        trait Storage {
            fn head(&self) -> &Position;
            async fn extend(&mut self, term: u64, entries: Vec<Bytes>) -> Position;
            async fn insert(&mut self, preceding_position: &Position, term: u64, entries: Vec<Bytes>) -> Result<Position, Position>;
            async fn at<'a>(&'a self, position: &Position) -> Option<(&'a Position, &'a Bytes)>;
            async fn next<'a>(&'a self, position: &Position) -> Option<(&'a Position, &'a Bytes)>;
        }
    }

    impl Display for MockStorage {
        fn fmt(&self, _formatter: &mut Formatter<'_>) -> fmt::Result {
            Ok(())
        }
    }

    mock! {
        Cluster {}
        #[async_trait]
        trait Cluster {
            fn ids(&self) ->  Vec<Id>;
            fn size(&self) -> usize;
            async fn send(&mut self, member_id: &Id, message: Message);
            async fn broadcast(&mut self, message: Message);
            async fn receive(&mut self) -> Option<Message>;
        }
    }
}

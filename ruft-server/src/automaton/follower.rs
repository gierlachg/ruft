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
    storage: &'a mut S,
    cluster: &'a mut C,

    leader_id: Option<Id>,
    voted_for: Option<Id>,

    election_timeout: Duration,
}

impl<'a, S: Storage, C: Cluster> Follower<'a, S, C> {
    pub(super) fn init(
        id: Id,
        term: u64,
        storage: &'a mut S,
        cluster: &'a mut C,
        leader_id: Option<Id>,
        voted_for: Option<Id>,
        election_timeout: Duration,
    ) -> Self {
        Follower {
            id,
            term,
            storage,
            cluster,
            leader_id,
            voted_for,
            election_timeout,
        }
    }

    pub(super) async fn run(&mut self) -> Option<State> {
        loop {
            tokio::select! {
                _ = time::sleep(self.election_timeout) => {
                    return Some(State::CANDIDATE { id: self.id, term: self.term })
                }
                message = self.cluster.receive() => {
                    match message {
                        Some(message) => {
                            match message {
                                AppendRequest { leader_id, preceding_position, term, entries } => {
                                    self.on_append_request(leader_id, preceding_position, term, entries).await
                                }
                                AppendResponse { member_id: _, success: _, position: _ } => {},
                                VoteRequest { candidate_id, term, position } => {
                                    self.on_vote_request(candidate_id, term, position).await
                                }
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
        if term > self.term {
            // TODO: possible double vote (in conjunction with VoteRequest actions) ?
            self.term = term;
            self.voted_for = None;
        }
        if term >= self.term {
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

    async fn on_vote_request(&mut self, candidate_id: Id, term: u64, position: Position) {
        if term > self.term && self.voted_for.is_none() && position >= *self.storage.head() {
            // TODO: should it be actually updated ?
            self.term = term;
            self.leader_id = None;
            self.voted_for = Some(candidate_id);

            self.cluster
                .send(&candidate_id, Message::vote_response(true, self.term))
                .await;
        } else if term < self.term {
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

    const ID: u8 = 1;
    const PEER_ID: u8 = 2;

    const TERM: u64 = 10;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_successful_insert_then_update_and_respond() {
        // given
        let position = Position::of(0, 0);
        let entries = vec![Bytes::from(vec![1])];

        let mut storage = MockStorage::new();
        storage
            .expect_insert()
            .with(eq(position), eq(TERM + 1), eq(entries.clone()))
            .returning(|position, _, _| Ok(position.clone()));

        let mut cluster = MockCluster::new();
        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::append_response(ID, true, position)))
            .return_const(());

        let mut follower = follower(&mut storage, &mut cluster, None);

        // when
        follower.on_append_request(PEER_ID, position, TERM + 1, entries).await;

        // then
        assert_eq!(follower.term, TERM + 1);
        assert_eq!(follower.leader_id, Some(PEER_ID));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_erroneous_insert_then_update_and_respond() {
        // given
        let position = Position::of(1, 0);
        let entries = vec![Bytes::from(vec![1])];

        let mut storage = MockStorage::new();
        storage
            .expect_insert()
            .with(eq(position), eq(TERM + 1), eq(entries.clone()))
            .returning(|position, _, _| Err(position.clone()));

        let mut cluster = MockCluster::new();
        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::append_response(ID, false, position)))
            .return_const(());

        let mut follower = follower(&mut storage, &mut cluster, None);

        // when
        follower.on_append_request(PEER_ID, position, TERM + 1, entries).await;

        // then
        assert_eq!(follower.term, TERM + 1);
        assert_eq!(follower.leader_id, Some(PEER_ID));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_greater_log_is_up_to_date_and_did_not_vote_then_update_and_respond() {
        // given
        let position = Position::of(1, 1);

        let mut storage = MockStorage::new();
        storage.expect_head().return_const(position);

        let mut cluster = MockCluster::new();
        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::vote_response(true, TERM + 1)))
            .return_const(());

        let mut follower = follower(&mut storage, &mut cluster, None);

        // when
        follower.on_vote_request(PEER_ID, TERM + 1, position).await;

        // then
        assert_eq!(follower.term, TERM + 1);
        assert_eq!(follower.leader_id, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_greater_but_log_is_not_up_to_date_then_ignore() {
        // given
        let mut storage = MockStorage::new();
        storage.expect_head().return_const(Position::of(1, 1));

        let mut cluster = MockCluster::new();

        let mut follower = follower(&mut storage, &mut cluster, None);

        // when
        follower.on_vote_request(PEER_ID, TERM + 1, Position::of(1, 0)).await;

        // then
        assert_eq!(follower.term, TERM);
        assert_eq!(follower.leader_id, None);
    }

    /*#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_greater_but_already_voted_then_ignore() {
        let current_position = Position::of(1, 1);

        let mut storage = MockStorage::new();
        storage.expect_head().return_const(current_position);
        let mut cluster = MockCluster::new();
        let mut follower = follower(&mut storage, &mut cluster, None);

        follower
            .on_vote_request(PEER_1_ID, LOCAL_TERM + 1, current_position)
            .await;
        assert_eq!(follower.term, LOCAL_TERM);
        assert_eq!(follower.leader_id, None);
    }*/

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_less_then_respond() {
        // given
        let position = Position::of(1, 1);

        let mut storage = MockStorage::new();
        storage.expect_head().return_const(position);

        let mut cluster = MockCluster::new();
        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::vote_response(false, TERM)))
            .return_const(());

        let mut follower = follower(&mut storage, &mut cluster, None);

        // when
        follower.on_vote_request(PEER_ID, TERM - 1, position).await;

        // then
        assert_eq!(follower.term, TERM);
        assert_eq!(follower.leader_id, None);
    }

    fn follower<'a>(
        storage: &'a mut MockStorage,
        cluster: &'a mut MockCluster,
        leader_id: Option<Id>,
    ) -> Follower<'a, MockStorage, MockCluster> {
        Follower::init(ID, TERM, storage, cluster, leader_id, None, Duration::from_secs(1))
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
            fn member_ids(&self) ->  Vec<Id>;
            fn size(&self) -> usize;
            async fn send(&self, member_id: &Id, message: Message);
            async fn broadcast(&self, message: Message);
            async fn receive(&mut self) -> Option<Message>;
        }
    }
}

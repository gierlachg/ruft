use tokio::time::{self, Duration};

use crate::automaton::State;
use crate::network::Cluster;
use crate::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::storage::Storage;
use crate::Id;

pub(super) struct Candidate<'a, S: Storage, C: Cluster> {
    id: Id,
    term: u64,
    granted_votes: usize,
    storage: &'a mut S,
    cluster: &'a mut C,

    election_timeout: Duration,
}

impl<'a, S: Storage, C: Cluster> Candidate<'a, S, C> {
    pub(super) fn init(id: Id, term: u64, storage: &'a mut S, cluster: &'a mut C, election_timeout: Duration) -> Self {
        Candidate {
            id,
            term,
            granted_votes: 0,
            storage,
            cluster,
            election_timeout,
        }
    }

    pub(super) async fn run(&mut self) -> Option<State> {
        let mut election_timer = time::interval(self.election_timeout);
        loop {
            tokio::select! {
                _ = election_timer.tick() => {
                    log::info!("election timeout");
                    self.term += 1;
                    self.granted_votes = 1;
                    self.cluster.broadcast(Message::vote_request(self.id, self.term, *self.storage.head())).await;
                }
                message = self.cluster.receive() => {
                    match message {
                        Some(message) => {
                            log::info!("term: {}, message: {:?}", self.term, message);
                            if let Some (state) = match message {
                                AppendRequest { leader_id, preceding_position: _, term, entries: _ } => {
                                    self.on_append_request(leader_id, term)
                                }
                                AppendResponse { member_id: _, success: _, position: _ } => None,
                                VoteRequest { candidate_id, term, position: _ } => {
                                    self.on_vote_request(candidate_id, term).await
                                }
                                VoteResponse { vote_granted, term } => self.on_vote_response(vote_granted, term),
                            } {
                                return Some(state)
                            }
                        }
                        None => break
                    }
                }
            }
        }
        None
    }

    fn on_append_request(&mut self, leader_id: Id, term: u64) -> Option<State> {
        if term >= self.term {
            Some(State::FOLLOWER {
                id: self.id,
                term,
                leader_id: Some(leader_id),
                voted_for: None,
            })
        } else {
            None
        }
    }

    async fn on_vote_request(&mut self, candidate_id: Id, term: u64) -> Option<State> {
        if term > self.term {
            self.cluster
                .send(&candidate_id, Message::vote_response(true, term))
                .await;

            Some(State::FOLLOWER {
                id: self.id,
                term,
                leader_id: None,
                voted_for: Some(candidate_id),
            })
        } else {
            None
        }
    }

    fn on_vote_response(&mut self, vote_granted: bool, term: u64) -> Option<State> {
        if term > self.term {
            Some(State::FOLLOWER {
                id: self.id,
                term,
                leader_id: None,
                voted_for: None,
            })
        } else if term == self.term && vote_granted {
            self.granted_votes += 1;
            if self.granted_votes > self.cluster.size() / 2 {
                Some(State::LEADER {
                    id: self.id,
                    term: self.term,
                })
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::fmt::{Display, Formatter};

    use async_trait::async_trait;
    use bytes::Bytes;
    use mockall::mock;
    use tokio::time::Duration;

    use crate::protocol::Message;
    use crate::storage::Position;
    use crate::Id;

    use super::*;
    use mockall::predicate::eq;

    const LOCAL_ID: u8 = 1;
    const PEER_ID: u8 = 2;

    const LOCAL_TERM: u64 = 10;

    #[test]
    fn when_append_request_term_greater_then_switch_to_follower() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut storage, &mut cluster);

        let state = candidate.on_append_request(PEER_ID, LOCAL_TERM + 1);
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: Some(PEER_ID),
                voted_for: None,
            })
        );
    }

    #[test]
    fn when_append_request_term_equal_then_switch_to_follower() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut storage, &mut cluster);

        let state = candidate.on_append_request(PEER_ID, LOCAL_TERM);
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM,
                leader_id: Some(PEER_ID),
                voted_for: None,
            })
        );
    }

    #[test]
    fn when_append_request_term_less_then_ignore() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut storage, &mut cluster);

        let state = candidate.on_append_request(PEER_ID, LOCAL_TERM - 1);
        assert_eq!(state, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_greater_then_respond_and_switch_to_follower() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::vote_response(true, LOCAL_TERM + 1)))
            .return_const(());
        let mut candidate = candidate(&mut storage, &mut cluster);

        let state = candidate.on_vote_request(PEER_ID, LOCAL_TERM + 1).await;
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: None,
                voted_for: Some(PEER_ID),
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_equal_then_ignore() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut storage, &mut cluster);

        let state = candidate.on_vote_request(PEER_ID, LOCAL_TERM).await;
        assert_eq!(state, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_less_then_ignore() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut storage, &mut cluster);

        let state = candidate.on_vote_request(PEER_ID, LOCAL_TERM - 1).await;
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_greater_then_switch_to_follower() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut storage, &mut cluster);

        let state = candidate.on_vote_response(false, LOCAL_TERM + 1);
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: None,
                voted_for: None,
            })
        );
    }

    #[test]
    fn when_vote_response_term_equal_but_vote_not_granted_then_ignore() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut storage, &mut cluster);

        let state = candidate.on_vote_response(false, LOCAL_TERM);
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_equal_and_vote_granted_but_quorum_not_reached_then_continue() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        cluster.expect_size().return_const(3usize);
        let mut candidate = candidate(&mut storage, &mut cluster);

        let state = candidate.on_vote_response(true, LOCAL_TERM);
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_equal_vote_granted_and_quorum_reached_then_switch_to_leader() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        cluster.expect_size().times(2).return_const(3usize);
        let mut candidate = candidate(&mut storage, &mut cluster);

        candidate.on_vote_response(true, LOCAL_TERM);
        let state = candidate.on_vote_response(true, LOCAL_TERM);
        assert_eq!(
            state,
            Some(State::LEADER {
                id: LOCAL_ID,
                term: LOCAL_TERM,
            })
        );
    }

    #[test]
    fn when_vote_response_term_less_then_ignore() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut storage, &mut cluster);

        let state = candidate.on_vote_response(true, LOCAL_TERM - 1);
        assert_eq!(state, None);
    }

    fn candidate<'a>(
        storage: &'a mut MockStorage,
        cluster: &'a mut MockCluster,
    ) -> Candidate<'a, MockStorage, MockCluster> {
        Candidate::init(LOCAL_ID, LOCAL_TERM, storage, cluster, Duration::from_secs(1))
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
            async fn send(&mut self, member_id: &Id, message: Message);
            async fn broadcast(&mut self, message: Message);
            async fn receive(&mut self) -> Option<Message>;
        }
    }
}

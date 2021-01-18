use std::collections::HashMap;

use tokio::time::{self, Duration};

use crate::automaton::State;
use crate::network::Cluster;
use crate::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::storage::{noop_message, Position, Storage};
use crate::Id;

// TODO: address liveness issues https://decentralizedthoughts.github.io/2020-12-12-raft-liveness-full-omission/

pub(super) struct Leader<'a, S: Storage, C: Cluster> {
    id: Id,
    term: u64,
    storage: &'a mut S,
    cluster: &'a mut C,

    trackers: HashMap<Id, Tracker>,

    heartbeat_interval: Duration,
}

impl<'a, S: Storage, C: Cluster> Leader<'a, S, C> {
    pub(super) fn init(
        id: Id,
        term: u64,
        storage: &'a mut S,
        cluster: &'a mut C,
        heartbeat_interval: Duration,
    ) -> Self {
        let trackers = cluster
            .ids()
            .into_iter()
            .map(|id| (id, Tracker::new(Position::of(term, 0))))
            .collect::<HashMap<Id, Tracker>>();

        Leader {
            id,
            term,
            storage,
            cluster,
            trackers,
            heartbeat_interval,
        }
    }

    pub(super) async fn run(&mut self) -> Option<State> {
        assert_eq!(
            self.storage.extend(self.term, vec![noop_message()]).await,
            Position::of(self.term, 0)
        );

        let mut ticker = time::interval(self.heartbeat_interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.on_tick().await;
                }
                message = self.cluster.receive() => {
                    match message {
                        Some(message) => {
                            if let Some(state) = match message {
                                AppendRequest { leader_id, preceding_position: _,  term, entries: _ } => {
                                    self.on_append_request(leader_id, term).await
                                }
                                AppendResponse { member_id, success, position } => {
                                    self.on_append_response(member_id, success, position).await
                                }
                                VoteRequest {candidate_id, term} => self.on_vote_request(candidate_id, term).await,
                                VoteResponse { vote_granted: _, term: _ } => None,
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

    async fn on_tick(&mut self) {
        for (id, tracker) in self.trackers.iter_mut() {
            if !tracker.sent {
                match tracker.next_position {
                    Some(position) => match self.storage.at(&position).await {
                        Some((preceding_position, entry)) => {
                            let message = Message::append_request(
                                self.id,
                                *preceding_position,
                                position.term(),
                                vec![entry.clone()],
                            );
                            self.cluster.send(id, message).await; // TODO: join
                        }
                        None => panic!("Missing entry at {:?}", &position),
                    },
                    None => {
                        let message = Message::append_request(self.id, *self.storage.head(), self.term, vec![]);
                        self.cluster.send(id, message).await; // TODO: join
                    }
                }
            }
            tracker.sent = false;
        }
    }

    async fn on_append_request(&mut self, leader_id: Id, term: u64) -> Option<State> {
        if term > self.term {
            Some(State::FOLLOWER {
                id: self.id,
                term,
                leader_id: Some(leader_id),
            })
        } else if term == self.term {
            panic!("Double leader detected - term: {}, leader id: {}", term, leader_id);
        } else {
            self.cluster
                .send(
                    &leader_id,
                    Message::append_response(self.id, true, self.storage.head().clone()),
                )
                .await;
            None
        }
    }

    async fn on_append_response(&mut self, member_id: Id, success: bool, position: Position) -> Option<State> {
        if position.term() > self.term {
            Some(State::FOLLOWER {
                id: self.id,
                term: position.term(),
                leader_id: None,
            })
        } else if success {
            if position == *self.storage.head() {
                match self.trackers.get_mut(&member_id) {
                    Some(tracker) => tracker.next_position = None,
                    None => panic!("Missing member of id: {}", member_id),
                }
                None
            } else {
                match self.trackers.get_mut(&member_id) {
                    Some(tracker) => {
                        let preceding_position = position;
                        match self.storage.next(&preceding_position).await {
                            Some((position, entry)) => {
                                let message = Message::append_request(
                                    self.id,
                                    preceding_position,
                                    position.term(),
                                    vec![entry.clone()],
                                );
                                self.cluster.send(&member_id, message).await;
                            }
                            None => panic!("Missing entry at {:?}", &position),
                        };
                        tracker.next_position = Some(position);
                        tracker.sent = true;
                    }
                    None => panic!("Missing tracker of id: {}", member_id),
                }
                None
            }
        } else {
            match self.trackers.get_mut(&member_id) {
                Some(tracker) => {
                    match self.storage.at(&position).await {
                        Some((preceding_position, entry)) => {
                            let message = Message::append_request(
                                self.id,
                                *preceding_position,
                                position.term(),
                                vec![entry.clone()],
                            );
                            self.cluster.send(&member_id, message).await;
                        }
                        None => panic!("Missing entry at {:?}", &position),
                    };
                    tracker.next_position = Some(position);
                    tracker.sent = true;
                }
                None => panic!("Missing tracker of id: {}", member_id),
            }
            None
        }
    }

    async fn on_vote_request(&mut self, candidate_id: Id, term: u64) -> Option<State> {
        if term > self.term {
            Some(State::FOLLOWER {
                id: self.id,
                term,
                leader_id: None,
            })
        } else {
            self.cluster
                .send(&candidate_id, Message::vote_response(false, self.term))
                .await;
            None
        }
    }
}

struct Tracker {
    next_position: Option<Position>,
    sent: bool, // TODO: naming
}

impl Tracker {
    fn new(next_position: Position) -> Self {
        Tracker {
            next_position: Some(next_position),
            sent: false,
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
    const PEER_ID: u8 = 2;

    const LOCAL_TERM: u64 = 10;

    // TODO: on_tick tests

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_term_greater_then_switch_to_follower() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        cluster.expect_ids().return_const(vec![PEER_ID]);
        let mut leader = leader(&mut storage, &mut cluster);

        let state = leader.on_append_request(PEER_ID, LOCAL_TERM + 1).await;
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: Some(PEER_ID),
            })
        );
    }

    #[should_panic]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_term_equal_then_panics() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        cluster.expect_ids().return_const(vec![PEER_ID]);
        let mut leader = leader(&mut storage, &mut cluster);

        leader.on_append_request(PEER_ID, LOCAL_TERM).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_term_less_then_respond() {
        let current_position: Position = Position::of(LOCAL_TERM, 0);

        let mut storage = MockStorage::new();
        storage.expect_head().return_const(current_position);
        let mut cluster = MockCluster::new();
        cluster.expect_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(
                eq(PEER_ID),
                eq(Message::append_response(LOCAL_ID, true, current_position)),
            )
            .return_const(());
        let mut leader = leader(&mut storage, &mut cluster);

        let state = leader.on_append_request(PEER_ID, LOCAL_TERM - 1).await;
        assert_eq!(state, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_response_term_greater_then_switch_to_follower() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        cluster.expect_ids().return_const(vec![PEER_ID]);
        let mut leader = leader(&mut storage, &mut cluster);

        let state = leader
            .on_append_response(PEER_ID, true, Position::of(LOCAL_TERM + 1, 0))
            .await;
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: None,
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_response_successful_and_position_latest_then_update() {
        let current_position: Position = Position::of(LOCAL_TERM, 0);

        let mut storage = MockStorage::new();
        storage.expect_head().return_const(current_position);
        let mut cluster = MockCluster::new();
        cluster.expect_ids().return_const(vec![PEER_ID]);
        let mut leader = leader(&mut storage, &mut cluster);

        let state = leader.on_append_response(PEER_ID, true, current_position).await;
        assert_eq!(state, None);
    }

    // TODO: remaining on_append_response tests

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_greater_then_switch_to_follower() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        cluster.expect_ids().return_const(vec![PEER_ID]);
        let mut leader = leader(&mut storage, &mut cluster);

        let state = leader.on_vote_request(PEER_ID, LOCAL_TERM + 1).await;
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: None,
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_less_or_equal_then_respond() {
        let mut storage = MockStorage::new();
        let mut cluster = MockCluster::new();
        cluster.expect_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::vote_response(false, LOCAL_TERM)))
            .return_const(());
        let mut leader = leader(&mut storage, &mut cluster);

        let state = leader.on_vote_request(PEER_ID, LOCAL_TERM).await;
        assert_eq!(state, None);
    }

    fn leader<'a>(storage: &'a mut MockStorage, cluster: &'a mut MockCluster) -> Leader<'a, MockStorage, MockCluster> {
        Leader::init(LOCAL_ID, LOCAL_TERM, storage, cluster, Duration::from_secs(1))
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

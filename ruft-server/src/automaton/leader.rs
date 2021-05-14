use std::collections::HashMap;

use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

use crate::automaton::State;
use crate::cluster::protocol::ServerMessage::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::cluster::Cluster;
use crate::relay::protocol::ClientMessage::{self, StoreRequest};
use crate::relay::Relay;
use crate::storage::{noop_message, Position, Storage};
use crate::Id;

// TODO: address liveness issues https://decentralizedthoughts.github.io/2020-12-12-raft-liveness-full-omission/

pub(super) struct Leader<'a, S: Storage, C: Cluster, R: Relay> {
    id: Id,
    term: u64,
    storage: &'a mut S,
    cluster: &'a mut C,
    relay: &'a mut R,

    trackers: HashMap<Id, Tracker>,

    heartbeat_interval: Duration,
}

impl<'a, S: Storage, C: Cluster, R: Relay> Leader<'a, S, C, R> {
    pub(super) fn init(
        id: Id,
        term: u64,
        storage: &'a mut S,
        cluster: &'a mut C,
        relay: &'a mut R,
        heartbeat_interval: Duration,
    ) -> Self {
        let trackers = cluster
            .member_ids()
            .into_iter()
            .map(|id| (id, Tracker::new(Position::of(term, 0))))
            .collect::<HashMap<Id, Tracker>>();

        Leader {
            id,
            term,
            storage,
            cluster,
            relay,
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
                    self.on_tick().await
                },
                result = self.cluster.receive() => match result {
                    Some(message) => {
                        if let Some(state) = match message {
                            AppendRequest { leader_id, preceding_position: _,  term, entries: _ } => {
                                self.on_append_request(leader_id, term).await
                            }
                            AppendResponse { member_id, success, position } => {
                                self.on_append_response(member_id, success, position).await
                            }
                            VoteRequest { candidate_id, term, position: _ } => {
                                self.on_vote_request(candidate_id, term).await
                            }
                            VoteResponse { vote_granted: _, term: _ } => None,
                        } {
                            return Some(state)
                        }
                    }
                    None => return None
                },
                result = self.relay.receive() => match result {
                    Some((message, responder)) => match message {
                        StoreRequest { payload } => self.on_payload(payload, responder).await,
                        _ => unreachable!(),
                    }
                    None => return None
                }
            }
        }
    }

    async fn on_tick(&mut self) {
        {
            let mut futures = self
                .trackers
                .iter()
                .filter(|(_, tracker)| !tracker.was_sent_recently())
                .map(|(member_id, tracker)| self.replicate_or_heartbeat(member_id, tracker.next_position()))
                .collect::<FuturesUnordered<_>>();
            while let Some(_) = futures.next().await {}
        }

        self.trackers
            .iter_mut()
            .for_each(|(_, tracker)| tracker.clear_sent_recently());
    }

    async fn replicate_or_heartbeat(&self, member_id: &Id, position: Option<&Position>) {
        let message = match position {
            Some(position) => match self.storage.at(&position).await {
                Some((preceding_position, entry)) => {
                    ServerMessage::append_request(self.id, *preceding_position, position.term(), vec![entry.clone()])
                }
                None => panic!("Missing entry at {:?}", &position),
            },
            None => ServerMessage::append_request(self.id, *self.storage.head(), self.term, vec![]),
        };
        self.cluster.send(&member_id, message).await;
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
                    ServerMessage::append_response(self.id, true, self.storage.head().clone()),
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
        } else if success && position == *self.storage.head() {
            match self.trackers.get_mut(&member_id) {
                Some(tracker) => tracker.clear_next_position(),
                None => panic!("Missing member of id: {}", member_id),
            }
            None
        } else {
            let tracker = self.trackers.get_mut(&member_id).expect("Missing tracker");
            let (preceding_position, position, entry) = if success {
                self.storage
                    .next(&position)
                    .await
                    .map(|(p, e)| (position, p.clone(), e))
                    .expect("Missing entry")
            } else {
                self.storage
                    .at(&position)
                    .await
                    .map(|(p, e)| (p.clone(), position, e))
                    .expect("Missing entry")
            };

            let message =
                ServerMessage::append_request(self.id, preceding_position, position.term(), vec![entry.clone()]);
            self.cluster.send(&member_id, message).await;

            tracker.update_next_position(position);
            tracker.mark_sent_recently();
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
        } else if term < self.term {
            self.cluster
                .send(&candidate_id, ServerMessage::vote_response(false, self.term))
                .await;
            None
        } else {
            None
        }
    }

    async fn on_payload(&mut self, payload: Bytes, responder: mpsc::UnboundedSender<ClientMessage>) {
        self.storage.extend(self.term, vec![payload]).await; // TODO: replicate, get rid of vec!
        responder
            .send(ClientMessage::store_success_response())
            .expect("This is unexpected!");
    }
}

struct Tracker {
    next_position: Option<Position>,
    sent_recently: bool,
}

impl Tracker {
    fn new(next_position: Position) -> Self {
        Tracker {
            next_position: Some(next_position),
            sent_recently: false,
        }
    }

    fn next_position(&self) -> Option<&Position> {
        self.next_position.as_ref()
    }

    fn clear_next_position(&mut self) {
        self.next_position = None;
    }

    fn update_next_position(&mut self, position: Position) {
        self.next_position = Some(position);
    }

    fn was_sent_recently(&self) -> bool {
        self.sent_recently
    }

    fn clear_sent_recently(&mut self) {
        self.sent_recently = false;
    }

    fn mark_sent_recently(&mut self) {
        self.sent_recently = true;
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bytes::Bytes;
    use lazy_static::lazy_static;
    use mockall::{mock, predicate};
    use predicate::eq;
    use tokio::time::Duration;

    use crate::cluster::protocol::ServerMessage;
    use crate::relay::protocol::ClientMessage;
    use crate::storage::Position;
    use crate::Id;

    use super::*;

    const ID: u8 = 1;
    const PEER_ID: u8 = 2;

    const TERM: u64 = 10;

    lazy_static! {
        static ref PRECEDING_POSITION: Position = Position::of(TERM - 1, 0);
        static ref POSITION: Position = Position::of(TERM, 0);
        static ref ENTRY: Bytes = Bytes::from(vec![1]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_time_comes_and_was_not_sent_recently_heartbeat_is_sent() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_head().return_const(POSITION.clone());

        cluster.expect_member_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(
                eq(PEER_ID),
                eq(ServerMessage::append_request(ID, POSITION.clone(), TERM, vec![])),
            )
            .return_const(());

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);
        leader.trackers.get_mut(&PEER_ID).unwrap().clear_next_position();

        // when
        // then
        leader.on_tick().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_time_comes_but_was_sent_recently_skip() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        cluster.expect_member_ids().return_const(vec![PEER_ID]);

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);
        leader.trackers.get_mut(&PEER_ID).unwrap().mark_sent_recently();

        // when
        // then
        leader.on_tick().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_time_comes_last_non_replicated_entry_is_sent() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_at().returning(|_| Some((&PRECEDING_POSITION, &ENTRY)));

        cluster.expect_member_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(
                eq(PEER_ID),
                eq(ServerMessage::append_request(
                    ID,
                    PRECEDING_POSITION.clone(),
                    POSITION.term(),
                    vec![ENTRY.clone()],
                )),
            )
            .return_const(());

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);

        // when
        // then
        leader.on_tick().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_append_request_term_greater_then_switch_to_follower() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        cluster.expect_member_ids().return_const(vec![PEER_ID]);

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);

        // when
        let state = leader.on_append_request(PEER_ID, TERM + 1).await;

        // then
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: ID,
                term: TERM + 1,
                leader_id: Some(PEER_ID),
            })
        );
    }

    #[should_panic]
    #[tokio::test(flavor = "current_thread")]
    async fn when_append_request_term_equal_then_panics() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        cluster.expect_member_ids().return_const(vec![PEER_ID]);

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);

        // when
        // then
        leader.on_append_request(PEER_ID, TERM).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_append_request_term_less_then_respond() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_head().return_const(POSITION.clone());

        cluster.expect_member_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(
                eq(PEER_ID),
                eq(ServerMessage::append_response(ID, true, POSITION.clone())),
            )
            .return_const(());

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);

        // when
        let state = leader.on_append_request(PEER_ID, TERM - 1).await;

        // then
        assert_eq!(state, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_append_response_term_greater_then_switch_to_follower() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        cluster.expect_member_ids().return_const(vec![PEER_ID]);

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);

        // when
        let state = leader
            .on_append_response(PEER_ID, true, Position::of(TERM + 1, 0))
            .await;

        // then
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: ID,
                term: TERM + 1,
                leader_id: None,
            })
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_append_response_successful_and_position_latest_then_update() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_head().return_const(POSITION.clone());

        cluster.expect_member_ids().return_const(vec![PEER_ID]);

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);

        // when
        let state = leader.on_append_response(PEER_ID, true, POSITION.clone()).await;

        // then
        assert_eq!(state, None);
        assert_eq!(leader.trackers.get(&PEER_ID).unwrap().next_position(), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_append_response_successful_and_position_not_latest_then_replicate_and_update() {
        // given
        let preceding_position = Position::of(TERM - 1, 0);

        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_head().return_const(POSITION.clone());
        storage.expect_next().returning(|_| Some((&POSITION, &ENTRY)));

        cluster.expect_member_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(
                eq(PEER_ID),
                eq(ServerMessage::append_request(
                    ID,
                    preceding_position,
                    POSITION.term(),
                    vec![ENTRY.clone()],
                )),
            )
            .return_const(());

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);

        // when
        let state = leader.on_append_response(PEER_ID, true, preceding_position).await;

        // then
        assert_eq!(state, None);
        assert_eq!(
            leader.trackers.get(&PEER_ID).unwrap().next_position(),
            Some(&POSITION.clone())
        );
        assert!(leader.trackers.get(&PEER_ID).unwrap().was_sent_recently());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_append_response_failed_then_replicate_and_update() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_at().returning(|_| Some((&PRECEDING_POSITION, &ENTRY)));

        cluster.expect_member_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(
                eq(PEER_ID),
                eq(ServerMessage::append_request(
                    ID,
                    PRECEDING_POSITION.clone(),
                    POSITION.term(),
                    vec![ENTRY.clone()],
                )),
            )
            .return_const(());

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);

        // when
        let state = leader.on_append_response(PEER_ID, false, POSITION.clone()).await;

        // then
        assert_eq!(state, None);
        assert_eq!(
            leader.trackers.get(&PEER_ID).unwrap().next_position(),
            Some(&POSITION.clone())
        );
        assert!(leader.trackers.get(&PEER_ID).unwrap().was_sent_recently());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_vote_request_term_greater_then_switch_to_follower() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        cluster.expect_member_ids().return_const(vec![PEER_ID]);

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);

        // when
        let state = leader.on_vote_request(PEER_ID, TERM + 1).await;

        // then
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: ID,
                term: TERM + 1,
                leader_id: None,
            })
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_vote_request_term_equal_then_ignore() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        cluster.expect_member_ids().return_const(vec![PEER_ID]);

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);

        // when
        let state = leader.on_vote_request(PEER_ID, TERM).await;

        // then
        assert_eq!(state, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_vote_request_term_less_then_respond() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        cluster.expect_member_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(ServerMessage::vote_response(false, TERM)))
            .return_const(());

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);

        // when
        let state = leader.on_vote_request(PEER_ID, TERM - 1).await;

        // then
        assert_eq!(state, None);
    }

    fn infrastructure() -> (MockStorage, MockCluster, MockRelay) {
        (MockStorage::new(), MockCluster::new(), MockRelay::new())
    }

    fn leader<'a>(
        storage: &'a mut MockStorage,
        cluster: &'a mut MockCluster,
        relay: &'a mut MockRelay,
    ) -> Leader<'a, MockStorage, MockCluster, MockRelay> {
        Leader::init(ID, TERM, storage, cluster, relay, Duration::from_secs(1))
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

    mock! {
        Cluster {}
        #[async_trait]
        trait Cluster {
            fn member_ids(&self) ->  Vec<Id>;
            fn size(&self) -> usize;
            async fn send(&self, member_id: &Id, message: ServerMessage);
            async fn broadcast(&self, message: ServerMessage);
            async fn receive(&mut self) -> Option<ServerMessage>;
        }
    }

    mock! {
        Relay {}
        #[async_trait]
        trait Relay {
            async fn receive(&mut self) -> Option<(ClientMessage, mpsc::UnboundedSender<ClientMessage>)>;
        }
    }
}

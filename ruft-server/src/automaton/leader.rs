use std::collections::HashMap;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::time::{self, Duration};

use crate::automaton::State::TERMINATED;
use crate::automaton::{Responder, State};
use crate::cluster::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest};
use crate::cluster::Cluster;
use crate::relay::protocol::Request;
use crate::relay::protocol::Request::StoreRequest;
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

    replicator: Replicator,

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
        let replicator = Replicator::new(cluster, Position::of(term, 0));

        Leader {
            id,
            term,
            storage,
            cluster,
            relay,
            replicator,
            heartbeat_interval,
        }
    }

    pub(super) async fn run(&mut self) -> State {
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
                message = self.cluster.messages() => match message {
                    Some(message) => if let Some(state) = self.on_message(message).await {
                        break state
                    },
                    None => break TERMINATED
                },
                request = self.relay.requests() => match request {
                    Some((request, responder)) => self.on_client_request(request, Responder(responder)).await,
                    None => break TERMINATED
                }
            }
        }
    }

    async fn on_tick(&mut self) {
        // TODO: sent recently...
        let mut futures = self
            .replicator
            .next_positions
            .iter()
            .map(|(member_id, position)| self.replicate_or_heartbeat(member_id, position.as_ref()))
            .collect::<FuturesUnordered<_>>();
        while let Some(_) = futures.next().await {}
    }

    async fn replicate_or_heartbeat(&self, member_id: &Id, position: Option<&Position>) {
        let message = match position {
            Some(position) => match self.storage.at(&position).await {
                Some((preceding_position, entry)) => {
                    Message::append_request(self.id, *preceding_position, position.term(), vec![entry.clone()])
                }
                None => panic!("Missing entry at {:?}", &position),
            },
            None => Message::append_request(self.id, *self.storage.head(), self.term, vec![]),
        };
        self.cluster.send(&member_id, message).await;
    }

    async fn on_message(&mut self, message: Message) -> Option<State> {
        #[rustfmt::skip]
        match message {
            AppendRequest { leader_id, preceding_position: _, term, entries: _ } => {
                self.on_append_request(leader_id, term).await
            },
            AppendResponse { member_id, success, position } => {
                self.on_append_response(member_id, success, position).await
            },
            VoteRequest { candidate_id, term, position: _ } => {
                self.on_vote_request(candidate_id, term).await
            },
            _ => None,
        }
    }

    async fn on_append_request(&mut self, leader_id: Id, term: u64) -> Option<State> {
        if term > self.term {
            Some(State::follower(self.id, term, Some(leader_id)))
        } else if term == self.term {
            panic!("Double leader detected - term: {}, leader id: {}", term, leader_id.0);
        } else {
            self.cluster
                .send(
                    &leader_id,
                    Message::append_response(self.id, true, *self.storage.head()),
                )
                .await;
            None
        }
    }

    async fn on_append_response(&mut self, member_id: Id, success: bool, position: Position) -> Option<State> {
        if position.term() > self.term {
            Some(State::follower(self.id, position.term(), None)) // TODO: figure out leader id
        } else if success && position == *self.storage.head() {
            self.replicator.on_success(&member_id, position, None);

            None
        } else {
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
            let message = Message::append_request(self.id, preceding_position, position.term(), vec![entry.clone()]);
            self.cluster.send(&member_id, message).await;

            if success {
                self.replicator
                    .on_success(&member_id, preceding_position, Some(position));
            } else {
                self.replicator.on_failure(&member_id, position);
            }

            None
        }
    }

    async fn on_vote_request(&mut self, candidate_id: Id, term: u64) -> Option<State> {
        if term > self.term {
            Some(State::follower(self.id, term, None)) // TODO: figure out leader id
        } else if term < self.term {
            self.cluster
                .send(&candidate_id, Message::vote_response(false, self.term))
                .await;
            None
        } else {
            None
        }
    }

    async fn on_client_request(&mut self, request: Request, responder: Responder) {
        // TODO: store in exchanges ? leadership lost...
        match request {
            StoreRequest { payload } => {
                let position = self.storage.extend(self.term, vec![payload]).await; // TODO: replicate, get rid of vec!
                self.replicator.on_client_request(position, responder);
            }
        }
    }
}

struct Replicator {
    // TODO: sent recently
    majority: usize,
    next_positions: HashMap<Id, Option<Position>>,
    replicated_positions: HashMap<Id, Option<Position>>,
    responders: HashMap<Position, Responder>,
}

impl Replicator {
    fn new(cluster: &dyn Cluster, init_position: Position) -> Self {
        Replicator {
            majority: cluster.size() / 2, // TODO:
            next_positions: cluster
                .member_ids()
                .into_iter()
                .map(|id| (id, Some(init_position)))
                .collect::<HashMap<_, _>>(),
            replicated_positions: cluster
                .member_ids()
                .into_iter()
                .map(|id| (id, None))
                .collect::<HashMap<_, _>>(),
            responders: HashMap::new(),
        }
    }

    fn on_client_request(&mut self, position: Position, responder: Responder) {
        if self.majority > 0 {
            self.responders.insert(position, responder);
        } else {
            responder.respond_with_success()
        }
    }

    fn on_failure(&mut self, member_id: &Id, missing_position: Position) {
        // TODO:
        *self.next_positions.get_mut(member_id).unwrap() = Some(missing_position);
    }

    fn on_success(&mut self, member_id: &Id, replicated_position: Position, next_position: Option<Position>) {
        *self.replicated_positions.get_mut(member_id).unwrap() = Some(replicated_position); // TODO:
        *self.next_positions.get_mut(member_id).unwrap() = next_position; // TODO:

        // TODO:
        let replication_count = self
            .replicated_positions
            .values()
            .filter_map(|position| position.filter(|position| *position >= replicated_position))
            .count();

        if replication_count >= self.majority {
            // TODO: response is always sent back ...
            if let Some(responder) = self.responders.remove(&replicated_position) {
                responder.respond_with_success()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use lazy_static::lazy_static;
    use mockall::{mock, predicate};
    use predicate::eq;
    use tokio::sync::mpsc;
    use tokio::time::Duration;

    use crate::cluster::protocol::Message;
    use crate::relay::protocol::{Request, Response};
    use crate::storage::Position;
    use crate::{Endpoint, Id, Payload};

    use super::*;

    const ID: Id = Id(1);
    const PEER_ID: Id = Id(2);

    const TERM: u64 = 10;

    lazy_static! {
        static ref PRECEDING_POSITION: Position = Position::of(TERM - 1, 0);
        static ref POSITION: Position = Position::of(TERM, 0);
        static ref ENTRY: Payload = Payload::from_static(&[1]);
    }

    #[ignore]
    #[tokio::test(flavor = "current_thread")]
    async fn when_time_comes_and_was_not_sent_recently_heartbeat_is_sent() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_head().return_const(POSITION.clone());

        cluster.expect_size().return_const(3usize);
        cluster.expect_member_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(
                eq(PEER_ID),
                eq(Message::append_request(ID, POSITION.clone(), TERM, vec![])),
            )
            .return_const(());

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);
        //leader.trackers.get_mut(&PEER_ID).unwrap().clear_next_position();

        // when
        // then
        leader.on_tick().await;
    }

    #[ignore]
    #[tokio::test(flavor = "current_thread")]
    async fn when_time_comes_but_was_sent_recently_skip() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        cluster.expect_size().return_const(3usize);
        cluster.expect_member_ids().return_const(vec![PEER_ID]);

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);
        //leader.trackers.get_mut(&PEER_ID).unwrap().mark_sent_recently();

        // when
        // then
        leader.on_tick().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_time_comes_last_non_replicated_entry_is_sent() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_at().returning(|_| Some((&PRECEDING_POSITION, &ENTRY)));

        cluster.expect_size().return_const(3usize);
        cluster.expect_member_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(
                eq(PEER_ID),
                eq(Message::append_request(
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

        cluster.expect_size().return_const(3usize);
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

        cluster.expect_size().return_const(3usize);
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

        cluster.expect_size().return_const(3usize);
        cluster.expect_member_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::append_response(ID, true, POSITION.clone())))
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

        cluster.expect_size().return_const(3usize);
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

        cluster.expect_size().return_const(3usize);
        cluster.expect_member_ids().return_const(vec![PEER_ID]);

        let mut leader = leader(&mut storage, &mut cluster, &mut relay);

        // when
        let state = leader.on_append_response(PEER_ID, true, POSITION.clone()).await;

        // then
        assert_eq!(state, None);
        assert_eq!(leader.replicator.next_positions.get(&PEER_ID).unwrap().as_ref(), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_append_response_successful_and_position_not_latest_then_replicate_and_update() {
        // given
        let preceding_position = Position::of(TERM - 1, 0);

        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_head().return_const(POSITION.clone());
        storage.expect_next().returning(|_| Some((&POSITION, &ENTRY)));

        cluster.expect_size().return_const(3usize);
        cluster.expect_member_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(
                eq(PEER_ID),
                eq(Message::append_request(
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
            leader.replicator.next_positions.get(&PEER_ID).unwrap().as_ref(),
            Some(&POSITION.clone())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_append_response_failed_then_replicate_and_update() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_at().returning(|_| Some((&PRECEDING_POSITION, &ENTRY)));

        cluster.expect_size().return_const(3usize);
        cluster.expect_member_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(
                eq(PEER_ID),
                eq(Message::append_request(
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
            leader.replicator.next_positions.get(&PEER_ID).unwrap().as_ref(),
            Some(&POSITION.clone())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_vote_request_term_greater_then_switch_to_follower() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        cluster.expect_size().return_const(3usize);
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

        cluster.expect_size().return_const(3usize);
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

        cluster.expect_size().return_const(3usize);
        cluster.expect_member_ids().return_const(vec![PEER_ID]);
        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::vote_response(false, TERM)))
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
            async fn extend(&mut self, term: u64, entries: Vec<Payload>) -> Position;
            async fn insert(&mut self, preceding_position: &Position, term: u64, entries: Vec<Payload>) -> Result<Position, Position>;
            async fn at<'a>(&'a self, position: &Position) -> Option<(&'a Position, &'a Payload)>;
            async fn next<'a>(&'a self, position: &Position) -> Option<(&'a Position, &'a Payload)>;
        }
    }

    mock! {
        Cluster {}
        #[async_trait]
        trait Cluster {
            fn member_ids(&self) ->  Vec<Id>;
            fn endpoint(&self, id: &Id) -> &Endpoint;
            fn size(&self) -> usize;
            async fn send(&self, member_id: &Id, message: Message);
            async fn broadcast(&self, message: Message);
            async fn messages(&mut self) -> Option<Message>;
        }
    }

    mock! {
        Relay {}
        #[async_trait]
        trait Relay {
            async fn requests(&mut self) -> Option<(Request, mpsc::UnboundedSender<Response>)>;
        }
    }
}

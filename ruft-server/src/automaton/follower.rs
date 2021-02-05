use bytes::Bytes;
use log::info;
use tokio::time::{self, Duration};

use crate::automaton::State;
use crate::cluster::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::cluster::Cluster;
use crate::relay::protocol::Message::StoreRequest;
use crate::relay::Relay;
use crate::storage::{Position, Storage};
use crate::{relay, Id};
use tokio::sync::mpsc;

pub(super) struct Follower<'a, S: Storage, C: Cluster, R: Relay> {
    id: Id,
    term: u64,
    storage: &'a mut S,
    cluster: &'a mut C,
    relay: &'a mut R,

    _leader_id: Option<Id>,

    election_timeout: Duration,
}

impl<'a, S: Storage, C: Cluster, R: Relay> Follower<'a, S, C, R> {
    pub(super) fn init(
        id: Id,
        term: u64,
        storage: &'a mut S,
        cluster: &'a mut C,
        relay: &'a mut R,
        leader_id: Option<Id>,
        election_timeout: Duration,
    ) -> Self {
        Follower {
            id,
            term,
            storage,
            cluster,
            relay,
            _leader_id: leader_id,
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
                result = self.relay.receive() => {
                    match result {
                        Some((message, responder)) => match message {
                            StoreRequest { payload: _ } => self.on_payload(responder).await,
                            _ => unreachable!(),
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
        }
        if term >= self.term {
            self._leader_id = Some(leader_id);
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
        if term > self.term && position >= *self.storage.head() {
            // TODO: should it be actually updated ?
            self.term = term;
            self._leader_id = None;

            self.cluster
                .send(&candidate_id, Message::vote_response(true, self.term))
                .await;
        } else if term < self.term {
            self.cluster
                .send(&candidate_id, Message::vote_response(false, self.term))
                .await;
        }
    }

    async fn on_payload(&mut self, responder: mpsc::UnboundedSender<relay::protocol::Message>) {
        responder
            .send(relay::protocol::Message::store_redirect_response())
            .expect("This is unexpected!");
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bytes::Bytes;
    use mockall::{mock, predicate};
    use predicate::eq;
    use tokio::time::Duration;

    use crate::cluster::protocol::Message;
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

        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage
            .expect_insert()
            .with(eq(position), eq(TERM + 1), eq(entries.clone()))
            .returning(|position, _, _| Ok(position.clone()));

        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::append_response(ID, true, position)))
            .return_const(());

        let mut follower = follower(&mut storage, &mut cluster, &mut relay, None);

        // when
        follower.on_append_request(PEER_ID, position, TERM + 1, entries).await;

        // then
        assert_eq!(follower.term, TERM + 1);
        assert_eq!(follower._leader_id, Some(PEER_ID));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_erroneous_insert_then_update_and_respond() {
        // given
        let position = Position::of(1, 0);
        let entries = vec![Bytes::from(vec![1])];

        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage
            .expect_insert()
            .with(eq(position), eq(TERM + 1), eq(entries.clone()))
            .returning(|position, _, _| Err(position.clone()));

        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::append_response(ID, false, position)))
            .return_const(());

        let mut follower = follower(&mut storage, &mut cluster, &mut relay, None);

        // when
        follower.on_append_request(PEER_ID, position, TERM + 1, entries).await;

        // then
        assert_eq!(follower.term, TERM + 1);
        assert_eq!(follower._leader_id, Some(PEER_ID));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_greater_and_log_is_up_to_date_then_update_and_respond() {
        // given
        let position = Position::of(1, 1);

        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_head().return_const(position);

        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::vote_response(true, TERM + 1)))
            .return_const(());

        let mut follower = follower(&mut storage, &mut cluster, &mut relay, None);

        // when
        follower.on_vote_request(PEER_ID, TERM + 1, position).await;

        // then
        assert_eq!(follower.term, TERM + 1);
        assert_eq!(follower._leader_id, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_greater_but_log_is_not_up_to_date_then_ignore() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_head().return_const(Position::of(1, 1));

        let mut follower = follower(&mut storage, &mut cluster, &mut relay, None);

        // when
        follower.on_vote_request(PEER_ID, TERM + 1, Position::of(1, 0)).await;

        // then
        assert_eq!(follower.term, TERM);
        assert_eq!(follower._leader_id, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_less_then_respond() {
        // given
        let position = Position::of(1, 1);

        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_head().return_const(position);

        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::vote_response(false, TERM)))
            .return_const(());

        let mut follower = follower(&mut storage, &mut cluster, &mut relay, None);

        // when
        follower.on_vote_request(PEER_ID, TERM - 1, position).await;

        // then
        assert_eq!(follower.term, TERM);
        assert_eq!(follower._leader_id, None);
    }

    fn infrastructure() -> (MockStorage, MockCluster, MockRelay) {
        (MockStorage::new(), MockCluster::new(), MockRelay::new())
    }

    fn follower<'a>(
        storage: &'a mut MockStorage,
        cluster: &'a mut MockCluster,
        relay: &'a mut MockRelay,
        leader_id: Option<Id>,
    ) -> Follower<'a, MockStorage, MockCluster, MockRelay> {
        Follower::init(ID, TERM, storage, cluster, relay, leader_id, Duration::from_secs(1))
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
            async fn send(&self, member_id: &Id, message: Message);
            async fn broadcast(&self, message: Message);
            async fn receive(&mut self) -> Option<Message>;
        }
    }

    mock! {
        Relay {}
        #[async_trait]
        trait Relay {
            async fn receive(&mut self) -> Option<(relay::protocol::Message, mpsc::UnboundedSender<relay::protocol::Message>)>;
        }
    }
}

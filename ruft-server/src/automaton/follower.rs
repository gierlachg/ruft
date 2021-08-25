use std::net::SocketAddr;

use bytes::Bytes;
use log::info;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

use crate::automaton::State;
use crate::cluster::protocol::Message::{self, AppendRequest, VoteRequest};
use crate::cluster::Cluster;
use crate::relay::protocol::Request::StoreRequest;
use crate::relay::protocol::{Request, Response};
use crate::relay::Relay;
use crate::storage::{Position, Storage};
use crate::Id;

pub(super) struct Follower<'a, S: Storage, C: Cluster, R: Relay> {
    id: Id,
    term: u64,
    storage: &'a mut S,
    cluster: &'a mut C,
    relay: &'a mut R,

    leader_id: Option<Id>,
    queue: Queue,

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
        let queue = Queue::new();

        Follower {
            id,
            term,
            storage,
            cluster,
            relay,
            leader_id,
            queue,
            election_timeout,
        }
    }

    pub(super) async fn run(&mut self) -> Option<State> {
        loop {
            tokio::select! {
                _ = time::sleep(self.election_timeout) => {
                    // TODO:
                    self.queue.redirect(self.cluster.endpoint(&self.id).client_address());

                    return Some(State::CANDIDATE { id: self.id, term: self.term })
                },
                message = self.cluster.messages() => match message {
                    Some(message) => self.on_message(message).await,
                    None => return None
                },
                request = self.relay.requests() => match request {
                    Some((request, responder)) => self.on_client_request(request, responder),
                    None => return None
                }
            }
        }
    }

    async fn on_message(&mut self, message: Message) {
        #[rustfmt::skip]
        match message {
            AppendRequest { leader_id, preceding_position, term, entries } => {
                self.on_append_request(leader_id, preceding_position, term, entries).await
            },
            VoteRequest { candidate_id, term, position } => {
                self.on_vote_request(candidate_id, term, position).await
            },
            _ => {}
        }
    }

    async fn on_append_request(&mut self, leader_id: Id, preceding_position: Position, term: u64, entries: Vec<Bytes>) {
        if term > self.term {
            // TODO: possible double vote (in conjunction with VoteRequest actions) ?
            self.term = term;
        }
        if term >= self.term {
            self.leader_id = Some(leader_id);

            // TODO:
            self.queue.redirect(self.cluster.endpoint(&leader_id).client_address());
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
            self.leader_id = None;

            self.cluster
                .send(&candidate_id, Message::vote_response(true, self.term))
                .await;
        } else if term < self.term {
            self.cluster
                .send(&candidate_id, Message::vote_response(false, self.term))
                .await;
        }
    }

    fn on_client_request(&mut self, request: Request, responder: mpsc::UnboundedSender<Response>) {
        match request {
            StoreRequest { payload: _ } => match self.leader_id {
                Some(ref leader_id) => responder
                    .send(Response::store_redirect_response(
                        &self.cluster.endpoint(leader_id).client_address().to_string(), // TODO:
                    ))
                    .expect("This is unexpected!"),
                None => self.queue.enqueue(responder),
            },
        }
    }
}

// TODO: shared...
#[derive(Clone)]
struct Queue {
    responders: Vec<mpsc::UnboundedSender<Response>>,
}

impl Queue {
    fn new() -> Self {
        Queue { responders: vec![] }
    }

    fn enqueue(&mut self, responder: mpsc::UnboundedSender<Response>) {
        self.responders.push(responder);
    }

    fn redirect(&mut self, address: &SocketAddr) {
        while !self.responders.is_empty() {
            self.responders
                .remove(0)
                .send(Response::store_redirect_response(&address.to_string()))
                .expect("This is unexpected!");
        }
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
    use crate::relay::protocol::{Request, Response};
    use crate::storage::Position;
    use crate::{Endpoint, Id};

    use super::*;

    const ID: Id = Id(1);
    const PEER_ID: Id = Id(2);

    const TERM: u64 = 10;

    #[tokio::test(flavor = "current_thread")]
    async fn when_append_request_successful_insert_then_update_and_respond() {
        // given
        let position = Position::of(0, 0);
        let entries = vec![Bytes::from(vec![1])];

        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage
            .expect_insert()
            .with(eq(position), eq(TERM + 1), eq(entries.clone()))
            .returning(|position, _, _| Ok(position.clone()));

        // TODO:
        cluster.expect_endpoint().with(eq(PEER_ID)).return_const(Endpoint::new(
            PEER_ID,
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
        ));
        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::append_response(ID, true, position)))
            .return_const(());

        let mut follower = follower(&mut storage, &mut cluster, &mut relay, None);

        // when
        follower.on_append_request(PEER_ID, position, TERM + 1, entries).await;

        // then
        assert_eq!(follower.term, TERM + 1);
        assert_eq!(follower.leader_id, Some(PEER_ID));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_append_request_erroneous_insert_then_update_and_respond() {
        // given
        let position = Position::of(1, 0);
        let entries = vec![Bytes::from(vec![1])];

        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage
            .expect_insert()
            .with(eq(position), eq(TERM + 1), eq(entries.clone()))
            .returning(|position, _, _| Err(position.clone()));

        // TODO:
        cluster.expect_endpoint().with(eq(PEER_ID)).return_const(Endpoint::new(
            PEER_ID,
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
        ));
        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::append_response(ID, false, position)))
            .return_const(());

        let mut follower = follower(&mut storage, &mut cluster, &mut relay, None);

        // when
        follower.on_append_request(PEER_ID, position, TERM + 1, entries).await;

        // then
        assert_eq!(follower.term, TERM + 1);
        assert_eq!(follower.leader_id, Some(PEER_ID));
    }

    #[tokio::test(flavor = "current_thread")]
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
        assert_eq!(follower.leader_id, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_vote_request_term_greater_but_log_is_not_up_to_date_then_ignore() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        storage.expect_head().return_const(Position::of(1, 1));

        let mut follower = follower(&mut storage, &mut cluster, &mut relay, None);

        // when
        follower.on_vote_request(PEER_ID, TERM + 1, Position::of(1, 0)).await;

        // then
        assert_eq!(follower.term, TERM);
        assert_eq!(follower.leader_id, None);
    }

    #[tokio::test(flavor = "current_thread")]
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
        assert_eq!(follower.leader_id, None);
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

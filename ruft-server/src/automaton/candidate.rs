use std::time::Duration;

use crate::automaton::State::TERMINATED;
use crate::automaton::{Responder, State};
use crate::cluster::protocol::Message::{self, AppendRequest, VoteRequest, VoteResponse};
use crate::cluster::Cluster;
use crate::relay::protocol::Request;
use crate::relay::Relay;
use crate::storage::Storage;
use crate::Id;

pub(super) struct Candidate<'a, S: Storage, C: Cluster, R: Relay> {
    id: Id,
    term: u64,
    storage: &'a mut S,
    cluster: &'a mut C,
    relay: &'a mut R,

    granted_votes: usize,

    election_timeout: Duration,
}

impl<'a, S: Storage, C: Cluster, R: Relay> Candidate<'a, S, C, R> {
    pub(super) fn init(
        id: Id,
        term: u64,
        storage: &'a mut S,
        cluster: &'a mut C,
        relay: &'a mut R,
        election_timeout: Duration,
    ) -> Self {
        Candidate {
            id,
            term,
            storage,
            cluster,
            relay,
            granted_votes: 0,
            election_timeout,
        }
    }

    pub(super) async fn run(&mut self) -> State {
        self.on_election_timeout().await;

        let mut election_timer = tokio::time::interval_at(
            tokio::time::Instant::now() + self.election_timeout,
            self.election_timeout,
        );
        loop {
            tokio::select! {
                _ = election_timer.tick() => {
                    self.on_election_timeout().await
                },
                message = self.cluster.messages() => match message {
                    Some(message) => if let Some(state) = self.on_message(message).await {
                        break state
                    },
                    None => break TERMINATED
                },
                request = self.relay.requests() => match request {
                    Some((request, responder)) => self.on_client_request(request, Responder(responder)),
                    None => break TERMINATED
                }
            }
        }
    }

    async fn on_election_timeout(&mut self) {
        self.term += 1;
        self.granted_votes = 1;
        self.cluster
            .broadcast(Message::vote_request(self.id, self.term, *self.storage.head()))
            .await
    }

    async fn on_message(&mut self, message: Message) -> Option<State> {
        #[rustfmt::skip]
        match message {
            AppendRequest { leader_id, preceding_position: _, term, entries: _ } => {
                self.on_append_request(leader_id, term)
            },
            VoteRequest { candidate_id, term, position: _ } => {
                self.on_vote_request(candidate_id, term).await
            },
            VoteResponse { vote_granted, term } => {
                self.on_vote_response(vote_granted, term)
            },
            _ => None
        }
    }

    fn on_append_request(&mut self, leader_id: Id, term: u64) -> Option<State> {
        // TODO: strictly higher ?
        if term >= self.term {
            Some(State::follower(self.id, term, Some(leader_id)))
        } else {
            None
        }
    }

    async fn on_vote_request(&mut self, candidate_id: Id, term: u64) -> Option<State> {
        if term > self.term {
            self.cluster
                .send(&candidate_id, Message::vote_response(true, term))
                .await;

            Some(State::follower(self.id, term, None))
        } else {
            None
        }
    }

    fn on_vote_response(&mut self, vote_granted: bool, term: u64) -> Option<State> {
        if term > self.term {
            Some(State::follower(self.id, term, None))
        } else if term == self.term && vote_granted {
            self.granted_votes += 1;
            if self.granted_votes > self.cluster.size() / 2 {
                // TODO: cluster size: dedup with replication
                Some(State::leader(self.id, self.term))
            } else {
                None
            }
        } else {
            None
        }
    }

    fn on_client_request(&mut self, _: Request, responder: Responder) {
        responder.respond_with_redirect(None)
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use mockall::mock;
    use mockall::predicate::eq;
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

    #[test]
    fn when_append_request_term_greater_then_switch_to_follower() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        // TODO:
        cluster.expect_endpoint().with(eq(PEER_ID)).return_const(Endpoint::new(
            PEER_ID,
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
        ));

        let mut candidate = candidate(&mut storage, &mut cluster, &mut relay);

        // when
        let state = candidate.on_append_request(PEER_ID, TERM + 1);

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

    #[test]
    fn when_append_request_term_equal_then_switch_to_follower() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        // TODO:
        cluster.expect_endpoint().with(eq(PEER_ID)).return_const(Endpoint::new(
            PEER_ID,
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
        ));

        let mut candidate = candidate(&mut storage, &mut cluster, &mut relay);

        // when
        let state = candidate.on_append_request(PEER_ID, TERM);

        // then
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: ID,
                term: TERM,
                leader_id: Some(PEER_ID),
            })
        );
    }

    #[test]
    fn when_append_request_term_less_then_ignore() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        let mut candidate = candidate(&mut storage, &mut cluster, &mut relay);

        // when
        let state = candidate.on_append_request(PEER_ID, TERM - 1);

        // then
        assert_eq!(state, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_vote_request_term_greater_then_respond_and_switch_to_follower() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        // TODO:
        cluster.expect_endpoint().with(eq(PEER_ID)).return_const(Endpoint::new(
            PEER_ID,
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
        ));
        cluster
            .expect_send()
            .with(eq(PEER_ID), eq(Message::vote_response(true, TERM + 1)))
            .return_const(());

        let mut candidate = candidate(&mut storage, &mut cluster, &mut relay);

        // when
        let state = candidate.on_vote_request(PEER_ID, TERM + 1).await;

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

        let mut candidate = candidate(&mut storage, &mut cluster, &mut relay);

        // when
        let state = candidate.on_vote_request(PEER_ID, TERM).await;

        // then
        assert_eq!(state, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn when_vote_request_term_less_then_ignore() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        let mut candidate = candidate(&mut storage, &mut cluster, &mut relay);

        // when
        let state = candidate.on_vote_request(PEER_ID, TERM - 1).await;

        // then
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_greater_then_switch_to_follower() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        // TODO:
        cluster.expect_endpoint().with(eq(ID)).return_const(Endpoint::new(
            ID,
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
        ));

        let mut candidate = candidate(&mut storage, &mut cluster, &mut relay);

        // when
        let state = candidate.on_vote_response(false, TERM + 1);

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

    #[test]
    fn when_vote_response_term_equal_but_vote_not_granted_then_ignore() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        let mut candidate = candidate(&mut storage, &mut cluster, &mut relay);

        // when
        let state = candidate.on_vote_response(false, TERM);

        // then
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_equal_and_vote_granted_but_quorum_not_reached_then_continue() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        cluster.expect_size().return_const(3usize);

        let mut candidate = candidate(&mut storage, &mut cluster, &mut relay);

        // when
        let state = candidate.on_vote_response(true, TERM);

        // then
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_equal_vote_granted_and_quorum_reached_then_switch_to_leader() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        cluster.expect_size().times(2).return_const(3usize);
        // TODO:
        cluster.expect_endpoint().with(eq(ID)).return_const(Endpoint::new(
            ID,
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
        ));

        let mut candidate = candidate(&mut storage, &mut cluster, &mut relay);
        candidate.on_vote_response(true, TERM);

        // when
        let state = candidate.on_vote_response(true, TERM);

        // then
        assert_eq!(state, Some(State::LEADER { id: ID, term: TERM }));
    }

    #[test]
    fn when_vote_response_term_less_then_ignore() {
        // given
        let (mut storage, mut cluster, mut relay) = infrastructure();

        let mut candidate = candidate(&mut storage, &mut cluster, &mut relay);

        // when
        let state = candidate.on_vote_response(true, TERM - 1);

        // then
        assert_eq!(state, None);
    }

    fn infrastructure() -> (MockStorage, MockCluster, MockRelay) {
        (MockStorage::new(), MockCluster::new(), MockRelay::new())
    }

    fn candidate<'a>(
        storage: &'a mut MockStorage,
        cluster: &'a mut MockCluster,
        relay: &'a mut MockRelay,
    ) -> Candidate<'a, MockStorage, MockCluster, MockRelay> {
        Candidate::init(ID, TERM, storage, cluster, relay, Duration::from_secs(1))
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

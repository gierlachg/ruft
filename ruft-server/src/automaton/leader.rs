use tokio::time::{self, Duration};

use crate::automaton::State;
use crate::network::Relay;
use crate::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::Id;

// TODO: address liveness issues https://decentralizedthoughts.github.io/2020-12-12-raft-liveness-full-omission/

pub(crate) struct Leader<'a, R: Relay> {
    id: Id,
    term: u64,
    relay: &'a mut R,

    heartbeat_interval: Duration,
}

impl<'a, R: Relay> Leader<'a, R> {
    pub(super) fn init(id: Id, term: u64, relay: &'a mut R, heartbeat_interval: Duration) -> Self {
        Leader {
            id,
            term,
            relay,
            heartbeat_interval,
        }
    }

    pub(super) async fn run(&mut self) -> Option<State> {
        let mut ticker = time::interval(self.heartbeat_interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.relay.broadcast(Message::append_request(self.term, self.id)).await;
                }
                message = self.relay.receive() => {
                    match message {
                        Some(message) => {
                            match message {
                                AppendRequest { term, leader_id } => {
                                    if let Some(status) = self.on_append_request(term, leader_id).await {
                                        return Some(status)
                                    }
                                }
                                AppendResponse { term, success: _ } => {
                                    if let Some(status) = self.on_append_response(term) {
                                        return Some(status)
                                    }
                                }
                                VoteRequest {term, candidate_id} => {
                                    if let Some(status) = self.on_vote_request(term, candidate_id).await {
                                        return Some(status)
                                    }
                                }
                                VoteResponse { term, vote_granted: _ } => {
                                    if let Some(status) = self.on_vote_response(term) {
                                        return Some(status)
                                    }
                                }
                            }
                        }
                        None => break
                    }
                }
            }
        }
        None
    }

    async fn on_append_request(&mut self, term: u64, leader_id: Id) -> Option<State> {
        if term > self.term {
            Some(State::FOLLOWER {
                id: self.id,
                term,
                leader_id: Some(leader_id),
            })
        } else if term == self.term {
            panic!("Double leader detected - term: {}, leader id: {}", term, leader_id);
        } else {
            self.relay
                .send(&leader_id, Message::append_response(self.term, false))
                .await;

            None
        }
    }

    fn on_append_response(&mut self, term: u64) -> Option<State> {
        if term > self.term {
            Some(State::FOLLOWER {
                id: self.id,
                term,
                leader_id: None,
            })
        } else {
            None
        }
    }

    async fn on_vote_request(&mut self, term: u64, candidate_id: Id) -> Option<State> {
        if term > self.term {
            Some(State::FOLLOWER {
                id: self.id,
                term,
                leader_id: None,
            })
        } else {
            self.relay
                .send(&candidate_id, Message::vote_response(self.term, false))
                .await;

            None
        }
    }

    fn on_vote_response(&mut self, term: u64) -> Option<State> {
        if term > self.term {
            Some(State::FOLLOWER {
                id: self.id,
                term,
                leader_id: None,
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use mockall::{mock, predicate};
    use predicate::eq;
    use tokio::time::Duration;

    use crate::protocol::Message;
    use crate::Id;

    use super::*;

    const LOCAL_ID: u8 = 1;
    const PEER_ID: u8 = 2;

    const LOCAL_TERM: u64 = 10;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_term_greater_then_switch_to_follower() {
        let mut relay = MockRelay::new();
        let mut leader = leader(&mut relay);

        let state = leader.on_append_request(LOCAL_TERM + 1, PEER_ID).await;
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: Some(PEER_ID)
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[should_panic]
    async fn when_append_request_term_equal_then_panics() {
        let mut relay = MockRelay::new();
        let mut leader = leader(&mut relay);

        leader.on_append_request(LOCAL_TERM, PEER_ID).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_term_less_then_respond() {
        let mut relay = MockRelay::new();
        relay
            .expect_send()
            .with(eq(PEER_ID), eq(Message::append_response(LOCAL_TERM, false)))
            .return_const(());
        let mut leader = leader(&mut relay);

        let state = leader.on_append_request(LOCAL_TERM - 1, PEER_ID).await;
        assert_eq!(state, None);
    }

    #[test]
    fn when_append_response_term_greater_then_switch_to_follower() {
        let mut relay = MockRelay::new();
        let mut leader = leader(&mut relay);

        let state = leader.on_append_response(LOCAL_TERM + 1);
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: None
            })
        );
    }

    #[test]
    fn when_append_response_term_less_or_equal_then_ignore() {
        let mut relay = MockRelay::new();
        let mut leader = leader(&mut relay);

        let state = leader.on_append_response(LOCAL_TERM);
        assert_eq!(state, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_greater_then_switch_to_follower() {
        let mut relay = MockRelay::new();
        let mut leader = leader(&mut relay);

        let state = leader.on_vote_request(LOCAL_TERM + 1, PEER_ID).await;
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: None
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_less_or_equal_then_respond() {
        let mut relay = MockRelay::new();
        relay
            .expect_send()
            .with(eq(PEER_ID), eq(Message::vote_response(LOCAL_TERM, false)))
            .return_const(());
        let mut leader = leader(&mut relay);

        let state = leader.on_vote_request(LOCAL_TERM, PEER_ID).await;
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_greater_then_switch_to_follower() {
        let mut relay = MockRelay::new();
        let mut leader = leader(&mut relay);

        let state = leader.on_vote_response(LOCAL_TERM + 1);
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: None
            })
        );
    }

    #[test]
    fn when_vote_response_term_less_or_equal_then_ignore() {
        let mut relay = MockRelay::new();
        let mut leader = leader(&mut relay);

        let state = leader.on_vote_response(LOCAL_TERM);
        assert_eq!(state, None);
    }

    fn leader(relay: &mut MockRelay) -> Leader<MockRelay> {
        Leader::init(LOCAL_ID, LOCAL_TERM, relay, Duration::from_secs(1))
    }

    mock! {
        Relay {}
        #[async_trait]
        trait Relay {
            async fn send(&mut self, member_id: &Id, message: Message);
            async fn broadcast(&mut self, message: Message);
            async fn receive(&mut self) -> Option<Message>;
            fn size(&self) -> usize;
        }
    }
}

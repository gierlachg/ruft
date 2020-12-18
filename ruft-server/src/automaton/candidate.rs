use std::ops::Add;

use time::Instant;
use tokio::time::{self, Duration};

use crate::automaton::State;
use crate::Id;
use crate::network::Relay;
use crate::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};

pub(crate) struct Candidate<'a, R: Relay> {
    id: Id,
    term: u64,
    granted_votes: usize,
    relay: &'a mut R,

    election_timeout: Duration,
}

impl<'a, R: Relay> Candidate<'a, R> {
    pub(super) fn init(id: Id, term: u64, relay: &'a mut R, election_timeout: Duration) -> Self {
        Candidate {
            id,
            term,
            granted_votes: 0,
            relay,
            election_timeout,
        }
    }

    pub(super) async fn run(&mut self) -> Option<State> {
        let mut ticker = time::interval_at(Instant::now().add(self.election_timeout), self.election_timeout);
        loop {
            self.term += 1;
            self.granted_votes = 1;
            self.relay.broadcast(Message::vote_request(self.term, self.id)).await;

            tokio::select! {
                _ = ticker.tick() => {
                    // election timed out
                }
                message = self.relay.receive() => {
                    match message {
                        Some(message) => {
                            if let Some (state) = match message {
                                AppendRequest { term, leader_id } => self.on_append_request(term, leader_id).await,
                                AppendResponse { term, success: _ } => self.on_append_response(term),
                                VoteRequest { term, candidate_id } => self.on_vote_request(term, candidate_id).await,
                                VoteResponse { term, vote_granted } => self.on_vote_response(term, vote_granted),
                            } {
                                return Some(state);
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
        if term >= self.term {
            Some(State::FOLLOWER {
                id: self.id,
                term,
                leader_id: Some(leader_id),
            })
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

    fn on_vote_response(&mut self, term: u64, vote_granted: bool) -> Option<State> {
        if term > self.term {
            Some(State::FOLLOWER {
                id: self.id,
                term,
                leader_id: None,
            })
        } else if term == self.term && vote_granted {
            self.granted_votes += 1;
            if self.granted_votes > self.relay.size() / 2 {
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
    use async_trait::async_trait;
    use mockall::{mock, predicate};
    use predicate::eq;
    use tokio::time::Duration;

    use crate::Id;
    use crate::protocol::Message;

    use super::*;

    const LOCAL_ID: u8 = 1;
    const PEER_ID: u8 = 2;

    const LOCAL_TERM: u64 = 10;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_term_greater_or_equal_then_switch_to_follower() {
        let mut relay = MockRelay::new();
        let mut candidate = candidate(&mut relay);

        let state = candidate.on_append_request(LOCAL_TERM, PEER_ID).await;
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM,
                leader_id: Some(PEER_ID),
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_term_less_then_respond() {
        let mut relay = MockRelay::new();
        relay
            .expect_send()
            .with(eq(PEER_ID), eq(Message::append_response(LOCAL_TERM, false)))
            .return_const(());
        let mut candidate = candidate(&mut relay);

        let state = candidate.on_append_request(LOCAL_TERM - 1, PEER_ID).await;
        assert_eq!(state, None);
    }

    #[test]
    fn when_append_response_term_greater_then_switch_to_follower() {
        let mut relay = MockRelay::new();
        let mut candidate = candidate(&mut relay);

        let state = candidate.on_append_response(LOCAL_TERM + 1);
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: None,
            })
        );
    }

    #[test]
    fn when_append_response_term_less_or_equal_then_ignore() {
        let mut relay = MockRelay::new();
        let mut candidate = candidate(&mut relay);

        let state = candidate.on_append_response(LOCAL_TERM);
        assert_eq!(state, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_greater_then_switch_to_follower() {
        let mut relay = MockRelay::new();
        let mut candidate = candidate(&mut relay);

        let state = candidate.on_vote_request(LOCAL_TERM + 1, PEER_ID).await;
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
        let mut relay = MockRelay::new();
        relay
            .expect_send()
            .with(eq(PEER_ID), eq(Message::vote_response(LOCAL_TERM, false)))
            .return_const(());
        let mut candidate = candidate(&mut relay);

        let state = candidate.on_vote_request(LOCAL_TERM, PEER_ID).await;
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_greater_then_switch_to_follower() {
        let mut relay = MockRelay::new();
        let mut candidate = candidate(&mut relay);

        let state = candidate.on_vote_response(LOCAL_TERM + 1, false);
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: None,
            })
        );
    }

    #[test]
    fn when_vote_response_term_equal_but_vote_not_granted_then_ignore() {
        let mut relay = MockRelay::new();
        let mut candidate = candidate(&mut relay);

        let state = candidate.on_vote_response(LOCAL_TERM, false);
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_equal_vote_granted_but_quorum_not_reached_then_continue() {
        let mut relay = MockRelay::new();
        relay.expect_size().return_const(3usize);
        let mut candidate = candidate(&mut relay);

        let state = candidate.on_vote_response(LOCAL_TERM, true);
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_equal_vote_granted_and_quorum_reached_then_switch_to_leader() {
        let mut relay = MockRelay::new();
        relay.expect_size().times(2).return_const(3usize);
        let mut candidate = candidate(&mut relay);

        candidate.on_vote_response(LOCAL_TERM, true);
        let state = candidate.on_vote_response(LOCAL_TERM, true);
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
        let mut relay = MockRelay::new();
        let mut candidate = candidate(&mut relay);

        let state = candidate.on_vote_response(LOCAL_TERM - 1, true);
        assert_eq!(state, None);
    }

    fn candidate(relay: &mut MockRelay) -> Candidate<MockRelay> {
        Candidate::init(LOCAL_ID, LOCAL_TERM, relay, Duration::from_secs(1))
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

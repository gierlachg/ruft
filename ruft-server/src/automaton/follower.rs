use tokio::time::{self, Duration};

use crate::automaton::State;
use crate::network::Relay;
use crate::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::Id;

pub(crate) struct Follower<'a, R: Relay> {
    id: Id,
    term: u64,
    leader_id: Option<Id>,
    relay: &'a mut R,

    heartbeat_timeout: Duration,
}

impl<'a, R: Relay> Follower<'a, R> {
    pub(super) fn init(
        id: Id,
        term: u64,
        leader_id: Option<Id>,
        relay: &'a mut R,
        heartbeat_timeout: Duration,
    ) -> Self {
        Follower {
            id,
            term,
            leader_id,
            relay,
            heartbeat_timeout,
        }
    }

    pub(super) async fn run(&mut self) -> Option<State> {
        loop {
            tokio::select! {
                // TODO: uninterrupted timer for AppendRQ ???
                _ = time::sleep(self.heartbeat_timeout) => {
                    return Some(State::CANDIDATE{id: self.id, term: self.term})
                }
                message = self.relay.receive() => {
                    match message {
                        Some(message) => {
                            match message {
                                AppendRequest { term, leader_id } =>  self.on_append_request(term, leader_id).await,
                                AppendResponse { term, success: _ } => self.on_append_response(term),
                                VoteRequest {term, candidate_id} => self.on_vote_request(term, candidate_id).await,
                                VoteResponse { term, vote_granted : _} =>  self.on_vote_response(term),
                            }
                        }
                        None => break
                    }
                }
            }
        }
        None
    }

    async fn on_append_request(&mut self, term: u64, leader_id: Id) {
        if term >= self.term {
            self.term = term;
            self.leader_id = Some(leader_id);
        } else {
            self.relay
                .send(&leader_id, Message::append_response(self.term, false))
                .await;
        }
    }

    fn on_append_response(&mut self, term: u64) {
        if term > self.term {
            self.term = term;
            self.leader_id = None;
        }
    }

    async fn on_vote_request(&mut self, term: u64, candidate_id: Id) {
        if term > self.term {
            self.term = term;
            self.leader_id = None;

            self.relay
                .send(&candidate_id, Message::vote_response(self.term, true))
                .await;
        } else {
            self.relay
                .send(&candidate_id, Message::vote_response(self.term, false))
                .await;
        }
    }

    fn on_vote_response(&mut self, term: u64) {
        if term > self.term {
            self.term = term;
            self.leader_id = None;
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
    const PEER_1_ID: u8 = 2;
    const PEER_2_ID: u8 = 3;

    const LOCAL_TERM: u64 = 10;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_term_greater_or_equal_then_update() {
        let mut relay = MockRelay::new();
        let mut follower = follower(&mut relay, None);

        follower.on_append_request(LOCAL_TERM, PEER_1_ID).await;
        assert_eq!(follower.term, LOCAL_TERM);
        assert_eq!(follower.leader_id, Some(PEER_1_ID));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_append_request_term_less_then_respond() {
        let mut relay = MockRelay::new();
        relay
            .expect_send()
            .with(eq(PEER_2_ID), eq(Message::append_response(LOCAL_TERM, false)))
            .return_const(());
        let mut follower = follower(&mut relay, Some(PEER_1_ID));

        follower.on_append_request(LOCAL_TERM - 1, PEER_2_ID).await;
        assert_eq!(follower.term, LOCAL_TERM);
        assert_eq!(follower.leader_id, Some(PEER_1_ID));
    }

    #[test]
    fn when_append_response_term_greater_then_update() {
        let mut relay = MockRelay::new();
        let mut follower = follower(&mut relay, Some(PEER_1_ID));

        follower.on_append_response(LOCAL_TERM + 1);
        assert_eq!(follower.term, LOCAL_TERM + 1);
        assert_eq!(follower.leader_id, None);
    }

    #[test]
    fn when_append_response_term_less_or_equal_then_ignore() {
        let mut relay = MockRelay::new();
        let mut follower = follower(&mut relay, Some(PEER_1_ID));

        follower.on_append_response(LOCAL_TERM);
        assert_eq!(follower.term, LOCAL_TERM);
        assert_eq!(follower.leader_id, Some(PEER_1_ID));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_greater_then_respond_and_update() {
        let mut relay = MockRelay::new();
        relay
            .expect_send()
            .with(eq(PEER_1_ID), eq(Message::vote_response(LOCAL_TERM + 1, true)))
            .return_const(());
        let mut follower = follower(&mut relay, None);

        follower.on_vote_request(LOCAL_TERM + 1, PEER_1_ID).await;
        assert_eq!(follower.term, LOCAL_TERM + 1);
        assert_eq!(follower.leader_id, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn when_vote_request_term_less_or_equal_then_respond() {
        let mut relay = MockRelay::new();
        relay
            .expect_send()
            .with(eq(PEER_1_ID), eq(Message::vote_response(LOCAL_TERM, false)))
            .return_const(());
        let mut follower = follower(&mut relay, Some(PEER_1_ID));

        follower.on_vote_request(LOCAL_TERM, PEER_1_ID).await;
        assert_eq!(follower.term, LOCAL_TERM);
        assert_eq!(follower.leader_id, Some(PEER_1_ID));
    }

    #[test]
    fn when_vote_response_term_greater_then_update() {
        let mut relay = MockRelay::new();
        let mut follower = follower(&mut relay, Some(PEER_1_ID));

        follower.on_vote_response(LOCAL_TERM + 1);
        assert_eq!(follower.term, LOCAL_TERM + 1);
        assert_eq!(follower.leader_id, None);
    }

    #[test]
    fn when_vote_response_term_less_or_equal_then_ignore() {
        let mut relay = MockRelay::new();
        let mut follower = follower(&mut relay, Some(PEER_1_ID));

        follower.on_append_response(LOCAL_TERM);
        assert_eq!(follower.term, LOCAL_TERM);
        assert_eq!(follower.leader_id, Some(PEER_1_ID));
    }

    fn follower(relay: &mut MockRelay, leader_id: Option<Id>) -> Follower<MockRelay> {
        Follower::init(LOCAL_ID, LOCAL_TERM, leader_id, relay, Duration::from_secs(1))
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

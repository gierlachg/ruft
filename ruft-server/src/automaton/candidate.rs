use std::ops::Add;

use tokio::time::{self, Duration, Instant};

use crate::automaton::State;
use crate::network::Cluster;
use crate::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::Id;

pub(super) struct Candidate<'a, C: Cluster> {
    id: Id,
    term: u64,
    granted_votes: usize,
    cluster: &'a mut C,

    election_timeout: Duration,
}

impl<'a, C: Cluster> Candidate<'a, C> {
    pub(super) fn init(id: Id, term: u64, cluster: &'a mut C, election_timeout: Duration) -> Self {
        Candidate {
            id,
            term,
            granted_votes: 0,
            cluster,
            election_timeout,
        }
    }

    pub(super) async fn run(&mut self) -> Option<State> {
        let mut ticker = time::interval_at(Instant::now().add(self.election_timeout), self.election_timeout);
        loop {
            self.term += 1;
            self.granted_votes = 1;
            self.cluster.broadcast(Message::vote_request(self.id, self.term)).await;

            // TODO: replace select! with something that actually works (!sic) here
            tokio::select! {
                _ = ticker.tick() => {
                    // election timed out
                }
                message = self.cluster.receive() => {
                    match message {
                        Some(message) => {
                            if let Some (state) = match message {
                                AppendRequest { leader_id, preceding_position: _, term, entries: _ } => {
                                    self.on_append_request(leader_id, term)
                                }
                                AppendResponse { member_id: _, success: _, position: _ } => None,
                                VoteRequest { candidate_id: _, term: _ } => None,
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
    use async_trait::async_trait;
    use mockall::mock;
    use tokio::time::Duration;

    use crate::protocol::Message;
    use crate::Id;

    use super::*;

    const LOCAL_ID: u8 = 1;
    const PEER_ID: u8 = 2;

    const LOCAL_TERM: u64 = 10;

    #[test]
    fn when_append_request_term_greater_then_switch_to_follower() {
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut cluster);

        let state = candidate.on_append_request(PEER_ID, LOCAL_TERM + 1);
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM + 1,
                leader_id: Some(PEER_ID),
            })
        );
    }

    #[test]
    fn when_append_request_term_equal_then_switch_to_follower() {
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut cluster);

        let state = candidate.on_append_request(PEER_ID, LOCAL_TERM);
        assert_eq!(
            state,
            Some(State::FOLLOWER {
                id: LOCAL_ID,
                term: LOCAL_TERM,
                leader_id: Some(PEER_ID),
            })
        );
    }

    #[test]
    fn when_append_request_term_less_then_ignore() {
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut cluster);

        let state = candidate.on_append_request(PEER_ID, LOCAL_TERM - 1);
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_greater_then_switch_to_follower() {
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut cluster);

        let state = candidate.on_vote_response(false, LOCAL_TERM + 1);
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
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut cluster);

        let state = candidate.on_vote_response(false, LOCAL_TERM);
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_equal_and_vote_granted_but_quorum_not_reached_then_ignore() {
        let mut cluster = MockCluster::new();
        cluster.expect_size().return_const(3usize);
        let mut candidate = candidate(&mut cluster);

        let state = candidate.on_vote_response(true, LOCAL_TERM);
        assert_eq!(state, None);
    }

    #[test]
    fn when_vote_response_term_equal_vote_granted_and_quorum_reached_then_switch_to_leader() {
        let mut cluster = MockCluster::new();
        cluster.expect_size().times(2).return_const(3usize);
        let mut candidate = candidate(&mut cluster);

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
        let mut cluster = MockCluster::new();
        let mut candidate = candidate(&mut cluster);

        let state = candidate.on_vote_response(true, LOCAL_TERM - 1);
        assert_eq!(state, None);
    }

    fn candidate(cluster: &mut MockCluster) -> Candidate<MockCluster> {
        Candidate::init(LOCAL_ID, LOCAL_TERM, cluster, Duration::from_secs(1))
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

use std::time::Duration;

use crate::automaton::Responder;
use crate::automaton::State::{self, TERMINATED};
use crate::cluster::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
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
            AppendRequest { leader_id, term, preceding_position: _, entries_term: _, entries: _ } => {
                self.on_append_request(leader_id, term).await
            },
            AppendResponse {member_id: _, term, success: _, position: _} => {
                self.on_append_response(term)
            },
            VoteRequest { candidate_id, term, position: _ } => {
                self.on_vote_request(candidate_id, term).await
            },
            VoteResponse { member_id: _, term, vote_granted } => {
                self.on_vote_response(term, vote_granted)
            },
        }
    }

    async fn on_append_request(&mut self, leader_id: Id, term: u64) -> Option<State> {
        if self.term > term {
            self.cluster
                .send(
                    &leader_id,
                    Message::append_response(self.id, self.term, false, *self.storage.head()),
                )
                .await;
            return None;
        }

        Some(State::follower(term, Some(leader_id)))
    }

    fn on_append_response(&mut self, term: u64) -> Option<State> {
        if term > self.term {
            Some(State::follower(term, None))
        } else {
            None
        }
    }

    async fn on_vote_request(&mut self, candidate_id: Id, term: u64) -> Option<State> {
        if self.term > term {
            self.cluster
                .send(&candidate_id, Message::vote_response(self.id, self.term, false))
                .await;
            return None;
        }

        if term > self.term {
            Some(State::follower(term, None))
        } else {
            None
        }
    }

    fn on_vote_response(&mut self, term: u64, vote_granted: bool) -> Option<State> {
        if term > self.term {
            Some(State::follower(term, None))
        } else if vote_granted {
            self.granted_votes += 1;
            if self.granted_votes > self.cluster.size() / 2 {
                // TODO: cluster size: dedup with replication
                Some(State::leader(self.term))
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

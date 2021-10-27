use std::time::Duration;

use crate::automaton::Responder;
use crate::automaton::State::{self, TERMINATED};
use crate::cluster::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::cluster::Cluster;
use crate::relay::protocol::Request;
use crate::relay::Relay;
use crate::storage::Log;
use crate::{Id, Position};

pub(super) struct Candidate<'a, L: Log, C: Cluster, R: Relay> {
    id: Id,
    term: u64,
    log: &'a mut L,
    cluster: &'a mut C,
    relay: &'a mut R,

    granted_votes: usize,

    election_timeout: Duration,
}

impl<'a, L: Log, C: Cluster, R: Relay> Candidate<'a, L, C, R> {
    pub(super) fn init(
        id: Id,
        term: u64,
        log: &'a mut L,
        cluster: &'a mut C,
        relay: &'a mut R,
        election_timeout: Duration,
    ) -> Self {
        Candidate {
            id,
            term,
            log,
            cluster,
            relay,
            granted_votes: 0,
            election_timeout,
        }
    }

    pub(super) async fn run(mut self) -> State {
        // TODO:
        self.cluster
            .broadcast(Message::vote_request(self.id, self.term, *self.log.head()))
            .await;
        if let Some(state) = self.on_vote_response(self.term, true) {
            return state;
        }

        let mut election_timer = tokio::time::interval_at(
            tokio::time::Instant::now() + self.election_timeout,
            self.election_timeout,
        );
        loop {
            tokio::select! {
                _ = election_timer.tick() => {
                    break State::candidate(self.term + 1)
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

    async fn on_message(&mut self, message: Message) -> Option<State> {
        #[rustfmt::skip]
        match message {
            AppendRequest { leader, term, preceding, entries_term: _, entries: _, committed: _ } => {
                self.on_append_request(leader, term, preceding).await
            },
            AppendResponse { member: _, term, success: _, position: _} => {
                self.on_append_response(term)
            },
            VoteRequest { candidate, term, position: _ } => {
                self.on_vote_request(candidate, term).await
            },
            VoteResponse { member: _, term, vote_granted } => {
                self.on_vote_response(term, vote_granted)
            },
        }
    }

    async fn on_append_request(&mut self, leader: Id, term: u64, preceding: Position) -> Option<State> {
        if self.term > term {
            self.cluster
                .send(&leader, Message::append_response(self.id, self.term, false, preceding))
                .await;
            None
        } else {
            Some(State::follower(term, None, Some(leader)))
        }
    }

    fn on_append_response(&mut self, term: u64) -> Option<State> {
        if self.term >= term {
            None
        } else {
            Some(State::follower(term, None, None))
        }
    }

    async fn on_vote_request(&mut self, candidate: Id, term: u64) -> Option<State> {
        if self.term > term {
            self.cluster
                .send(&candidate, Message::vote_response(self.id, self.term, false))
                .await;
            None
        } else if self.term == term {
            None
        } else {
            Some(State::follower(term, None, None))
        }
    }

    fn on_vote_response(&mut self, term: u64, vote_granted: bool) -> Option<State> {
        if self.term >= term {
            if vote_granted {
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
        } else {
            Some(State::follower(term, None, None))
        }
    }

    fn on_client_request(&mut self, _: Request, responder: Responder) {
        responder.respond_with_redirect(None, None)
    }
}

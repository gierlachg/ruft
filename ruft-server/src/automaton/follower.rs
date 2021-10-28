use std::time::Duration;

use log::info;

use crate::automaton::Responder;
use crate::automaton::State::{self, TERMINATED};
use crate::cluster::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::cluster::Cluster;
use crate::relay::protocol::Request;
use crate::relay::Relay;
use crate::storage::Log;
use crate::{Id, Payload, Position};

pub(super) struct Follower<'a, L: Log, C: Cluster, R: Relay> {
    id: Id,
    term: u64,
    log: &'a mut L,
    cluster: &'a mut C,
    relay: &'a mut R,

    votee: Option<Id>,
    leader: Option<Id>,

    election_timeout: Duration,
}

impl<'a, L: Log, C: Cluster, R: Relay> Follower<'a, L, C, R> {
    pub(super) fn init(
        id: Id,
        term: u64,
        log: &'a mut L,
        cluster: &'a mut C,
        relay: &'a mut R,
        votee: Option<Id>,
        leader: Option<Id>,
        election_timeout: Duration,
    ) -> Self {
        Follower {
            id,
            term,
            log,
            cluster,
            relay,
            votee,
            leader,
            election_timeout,
        }
    }

    pub(super) async fn run(mut self) -> State {
        tokio::pin! {
           let sleep = tokio::time::sleep(self.election_timeout);
        }
        loop {
            tokio::select! {
                _ = &mut sleep => {
                    break State::candidate(self.term + 1)
                },
                message = self.cluster.messages() => match message {
                    Some(message) => match self.on_message(message).await {
                        (_, Some(state)) => break state,
                        (true, None) => sleep.as_mut().reset(tokio::time::Instant::now() + self.election_timeout),
                        _ => {}
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

    async fn on_message(&mut self, message: Message) -> (bool, Option<State>) {
        #[rustfmt::skip]
        match message {
            AppendRequest { leader, term, preceding, entries_term, entries, committed } => {
                self.on_append_request(leader, term, preceding,  entries_term, entries, committed).await
            },
            AppendResponse { member: _, term, position: _} => {
                (false, self.on_append_response(term))
            },
            VoteRequest { candidate, term, position } => {
                (false, self.on_vote_request(candidate, term, position).await)
            },
            VoteResponse { member: _, term, vote_granted: _} => {
                (false, self.on_vote_response(term))
            },
        }
    }

    async fn on_append_request(
        &mut self,
        leader: Id,
        term: u64,
        preceding: Position,
        entries_term: u64,
        entries: Vec<Payload>,
        _committed: Position,
    ) -> (bool, Option<State>) {
        if self.term > term {
            self.cluster
                .send(&leader, Message::append_response(self.id, self.term, Err(preceding)))
                .await;
            (false, None)
        } else if self.term == term {
            self.leader.replace(leader);
            match self.log.insert(&preceding, entries_term, entries).await {
                Ok(position) => {
                    info!("Accepted: {:?}, committed: {:?}", position, _committed);
                    self.cluster
                        .send(&leader, Message::append_response(self.id, self.term, Ok(position)))
                        .await
                }
                Err(position) => {
                    info!("Missing: {:?}", position);
                    self.cluster
                        .send(&leader, Message::append_response(self.id, self.term, Err(position)))
                        .await
                }
            }
            (true, None)
        } else {
            (false, Some(State::follower(term, None, Some(leader))))
        }
    }

    fn on_append_response(&mut self, term: u64) -> Option<State> {
        if self.term >= term {
            None
        } else {
            Some(State::follower(term, None, None))
        }
    }

    async fn on_vote_request(&mut self, candidate: Id, term: u64, position: Position) -> Option<State> {
        if self.term > term {
            self.cluster
                .send(&candidate, Message::vote_response(self.id, self.term, false))
                .await;
            None
        } else if self.term == term {
            if self.votee.is_none() {
                self.cluster
                    .send(&candidate, Message::vote_response(self.id, self.term, true))
                    .await;
                Some(State::follower(term, Some(candidate), None))
            } else {
                None
            }
        } else {
            if position >= *self.log.head() {
                self.cluster
                    .send(&candidate, Message::vote_response(self.id, self.term, true))
                    .await;
                Some(State::follower(term, Some(candidate), None))
            } else {
                Some(State::follower(term, None, None))
            }
        }
    }

    fn on_vote_response(&mut self, term: u64) -> Option<State> {
        if self.term >= term {
            None
        } else {
            Some(State::follower(term, None, None))
        }
    }

    fn on_client_request(&mut self, _: Request, responder: Responder) {
        let leader_address = self
            .leader
            .map(|ref leader| *self.cluster.endpoint(leader).client_address());
        responder.respond_with_redirect(leader_address, None);
    }
}

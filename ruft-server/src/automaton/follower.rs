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
    leader_id: Option<Id>,

    election_timeout: Duration,
}

impl<'a, L: Log, C: Cluster, R: Relay> Follower<'a, L, C, R> {
    pub(super) fn init(
        id: Id,
        term: u64,
        log: &'a mut L,
        cluster: &'a mut C,
        relay: &'a mut R,
        leader_id: Option<Id>,
        election_timeout: Duration,
    ) -> Self {
        Follower {
            id,
            term,
            log,
            cluster,
            relay,
            votee: None,
            leader_id,
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
                    break State::candidate(self.term)
                },
                message = self.cluster.messages() => match message {
                    Some(message) => if self.on_message(message).await {
                        sleep.as_mut().reset(tokio::time::Instant::now() + self.election_timeout);
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

    async fn on_message(&mut self, message: Message) -> bool {
        #[rustfmt::skip]
        match message {
            AppendRequest { leader_id, term, preceding, entries_term, entries, committed } => {
                self.on_append_request(leader_id, term, preceding,  entries_term, entries, committed).await;
                true
            },
            AppendResponse {member_id: _, term, success: _, position: _} => {
                self.on_append_response(term);
                false
            },
            VoteRequest { candidate_id, term, position } => {
                self.on_vote_request(candidate_id, term, position).await;
                false
            },
            VoteResponse {member_id: _, term, vote_granted: _} => {
                self.on_vote_response(term);
                false
            },
        }
    }

    async fn on_append_request(
        &mut self,
        leader_id: Id,
        term: u64,
        preceding: Position,
        entries_term: u64,
        entries: Vec<Payload>,
        _committed: Position,
    ) {
        if self.term > term {
            return self
                .cluster
                .send(
                    &leader_id,
                    Message::append_response(self.id, self.term, false, preceding),
                )
                .await;
        }

        self.term = term;
        self.leader_id.replace(leader_id);
        match self.log.insert(&preceding, entries_term, entries).await {
            Ok(position) => {
                info!("Accepted: {:?}, committed: {:?}", position, _committed);
                self.cluster
                    .send(&leader_id, Message::append_response(self.id, self.term, true, position))
                    .await
            }
            Err(position) => {
                info!("Missing: {:?}", position);
                self.cluster
                    .send(
                        &leader_id,
                        Message::append_response(self.id, self.term, false, position),
                    )
                    .await
            }
        }
    }

    fn on_append_response(&mut self, term: u64) {
        if term > self.term {
            self.term = term;
            self.votee = None;
            self.leader_id = None;
        }
    }

    async fn on_vote_request(&mut self, candidate_id: Id, term: u64, position: Position) {
        if self.term > term {
            return self
                .cluster
                .send(&candidate_id, Message::vote_response(self.id, self.term, false))
                .await;
        }

        if term > self.term {
            if position >= *self.log.head() {
                self.term = term;
                self.votee.replace(candidate_id);
                self.leader_id = None;

                self.cluster
                    .send(&candidate_id, Message::vote_response(self.id, self.term, true))
                    .await
            }
        } else if self.votee.is_none() {
            self.votee.replace(candidate_id);
            self.cluster
                .send(&candidate_id, Message::vote_response(self.id, self.term, true))
                .await
        }
    }

    fn on_vote_response(&mut self, term: u64) {
        if term > self.term {
            self.term = term;
            self.votee = None;
            self.leader_id = None;
        }
    }

    fn on_client_request(&mut self, _: Request, responder: Responder) {
        let leader_address = self
            .leader_id
            .map(|ref leader_id| *self.cluster.endpoint(leader_id).client_address());
        responder.respond_with_redirect(leader_address, None);
    }
}

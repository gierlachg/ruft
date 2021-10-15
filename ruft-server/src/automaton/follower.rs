use std::time::Duration;

use log::info;

use crate::automaton::State::TERMINATED;
use crate::automaton::{Responder, State};
use crate::cluster::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::cluster::Cluster;
use crate::relay::protocol::Request;
use crate::relay::Relay;
use crate::storage::{Position, Storage};
use crate::{Id, Payload};

pub(super) struct Follower<'a, S: Storage, C: Cluster, R: Relay> {
    id: Id,
    term: u64,
    storage: &'a mut S,
    cluster: &'a mut C,
    relay: &'a mut R,

    votee: Option<Id>,
    leader_id: Option<Id>,

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
        Follower {
            id,
            term,
            storage,
            cluster,
            relay,
            votee: None,
            leader_id,
            election_timeout,
        }
    }

    pub(super) async fn run(&mut self) -> State {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.election_timeout) => {
                    break State::candidate(self.term)
                },
                message = self.cluster.messages() => match message {
                    Some(message) => self.on_message(message).await,
                    None => break TERMINATED
                },
                request = self.relay.requests() => match request {
                    Some((request, responder)) => self.on_client_request(request, Responder(responder)),
                    None => break TERMINATED
                }
            }
        }
    }

    async fn on_message(&mut self, message: Message) {
        #[rustfmt::skip]
        match message {
            AppendRequest { leader_id, term, preceding_position, entries } => {
                self.on_append_request(leader_id, preceding_position, term, entries).await
            },
            AppendResponse {member_id: _, term, success: _, position: _} => {
                self.on_append_response(term)
            },
            VoteRequest { candidate_id, term, position } => {
                self.on_vote_request(candidate_id, term, position).await
            },
            VoteResponse {member_id: _, term, vote_granted: _} => {
                self.on_vote_response(term)
            },
        }
    }

    async fn on_append_request(
        &mut self,
        leader_id: Id,
        preceding_position: Position,
        term: u64,
        entries: Vec<Payload>,
    ) {
        if self.term > term {
            return self
                .cluster
                .send(
                    &leader_id,
                    Message::append_response(self.id, self.term, false, *self.storage.head()),
                )
                .await;
        }

        self.term = term;
        self.leader_id.replace(leader_id);
        match self.storage.insert(&preceding_position, term, entries).await {
            Ok(position) => {
                info!("Accepted: {:?}", position);
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
            if position >= *self.storage.head() {
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
        responder.respond_with_redirect(leader_address);
    }
}

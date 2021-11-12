use std::num::NonZeroU64;
use std::time::Duration;

use crate::automata::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::automata::protocol::{Request, Response};
use crate::automata::Responder;
use crate::automata::Transition::{self, TERMINATED};
use crate::cluster::Cluster;
use crate::relay::Relay;
use crate::storage::Log;
use crate::{Id, Payload, Position};

// TODO: address liveness issues https://decentralizedthoughts.github.io/2020-12-12-raft-liveness-full-omission/

pub(super) struct Follower<'a, L: Log, C: Cluster<Message>, R: Relay<Request, Response>> {
    id: Id,
    term: u64,
    log: &'a mut L,
    cluster: &'a mut C,
    relay: &'a mut R,

    leader: Option<Id>,

    election_timeout: Duration,
}

impl<'a, L: Log, C: Cluster<Message>, R: Relay<Request, Response>> Follower<'a, L, C, R> {
    pub(super) fn init(
        id: Id,
        term: u64,
        log: &'a mut L,
        cluster: &'a mut C,
        relay: &'a mut R,
        leader: Option<Id>,
        election_timeout: Duration,
    ) -> Self {
        Follower {
            id,
            term,
            log,
            cluster,
            relay,
            leader,
            election_timeout,
        }
    }

    pub(super) async fn run(mut self) -> Transition {
        tokio::pin! {
           let sleep = tokio::time::sleep(self.election_timeout);
        }
        loop {
            tokio::select! {
                _ = &mut sleep => {
                    // safety: n + 1 > 0
                    break Transition::candidate(NonZeroU64::new(self.term + 1).unwrap())
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

    async fn on_message(&mut self, message: Message) -> (bool, Option<Transition>) {
        #[rustfmt::skip]
        match message {
            AppendRequest { leader, term, preceding, entries_term, entries, committed: _ } => {
                self.on_append_request(leader, term, preceding,  entries_term, entries).await
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
        term: NonZeroU64,
        preceding: Position,
        entries_term: NonZeroU64,
        entries: Vec<Payload>,
    ) -> (bool, Option<Transition>) {
        if self.term > term.get() {
            self.cluster
                .send(
                    &leader,
                    // safety: leader's term > 1 & n + 1 > 0
                    Message::append_response(self.id, NonZeroU64::new(self.term).unwrap(), Err(preceding)),
                )
                .await;
            (false, None)
        } else if self.term == term.get() {
            self.leader.replace(leader);
            let result = self.log.insert(&preceding, entries_term, entries).await;
            self.cluster
                .send(&leader, Message::append_response(self.id, term, result))
                .await;
            (true, None)
        } else {
            (false, Some(Transition::follower(term, Some(leader))))
        }
    }

    fn on_append_response(&mut self, term: NonZeroU64) -> Option<Transition> {
        if self.term >= term.get() {
            None
        } else {
            Some(Transition::follower(term, None))
        }
    }

    async fn on_vote_request(&mut self, candidate: Id, term: NonZeroU64, position: Position) -> Option<Transition> {
        if self.term > term.get() {
            self.cluster
                .send(
                    &candidate,
                    // safety: n + 1 > 0
                    Message::vote_response(self.id, NonZeroU64::new(self.term).unwrap(), false),
                )
                .await;
            None
        } else if self.term == term.get() {
            None
        } else {
            // TODO: persist state before sending the response, otherwise double vote possible
            if position >= *self.log.head() {
                self.cluster
                    .send(&candidate, Message::vote_response(self.id, term, true))
                    .await;
            }
            Some(Transition::follower(term, None))
        }
    }

    fn on_vote_response(&mut self, term: NonZeroU64) -> Option<Transition> {
        if self.term >= term.get() {
            None
        } else {
            Some(Transition::follower(term, None))
        }
    }

    fn on_client_request(&mut self, _: Request, responder: Responder) {
        // TODO: hold onto request until leader is learnt
        let leader_address = self
            .leader
            .map(|ref leader| *self.cluster.endpoint(leader).client_address());
        responder.respond_with_redirect(leader_address, None);
    }
}

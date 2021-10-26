use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use futures::future::join_all;

use crate::automaton::Responder;
use crate::automaton::State::{self, TERMINATED};
use crate::cluster::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::cluster::Cluster;
use crate::relay::protocol::Request::{self, StoreRequest};
use crate::relay::Relay;
use crate::storage::{noop_message, Storage};
use crate::{Id, Position};

// TODO: address liveness issues https://decentralizedthoughts.github.io/2020-12-12-raft-liveness-full-omission/

pub(super) struct Leader<'a, S: Storage, C: Cluster, R: Relay> {
    id: Id,
    term: u64,
    storage: &'a mut S,
    cluster: &'a mut C,
    relay: &'a mut R,

    registry: Registry,

    heartbeat_interval: Duration,
}

impl<'a, S: Storage, C: Cluster, R: Relay> Leader<'a, S, C, R> {
    pub(super) fn init(
        id: Id,
        term: u64,
        storage: &'a mut S,
        cluster: &'a mut C,
        relay: &'a mut R,
        heartbeat_interval: Duration,
    ) -> Self {
        Leader {
            id,
            term,
            storage,
            cluster,
            relay,
            registry: Registry::new(),
            heartbeat_interval,
        }
    }

    pub(super) async fn run(&mut self) -> State {
        self.registry.init(
            self.cluster.member_ids(),
            self.storage.extend(self.term, vec![noop_message()]).await,
        );
        self.on_tick().await;

        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + self.heartbeat_interval,
            self.heartbeat_interval,
        );
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.on_tick().await
                },
                message = self.cluster.messages() => match message {
                    Some(message) => if let Some(state) = self.on_message(message).await {
                        break state
                    },
                    None => break TERMINATED
                },
                request = self.relay.requests() => match request {
                    Some((request, responder)) => self.on_client_request(request, Responder(responder)).await,
                    None => break TERMINATED
                }
            }
        }
    }

    async fn on_tick(&mut self) {
        self.replicate(self.registry.nexts()).await;
    }

    async fn on_message(&mut self, message: Message) -> Option<State> {
        #[rustfmt::skip]
        match message {
            AppendRequest { leader_id, term, preceding, entries_term: _, entries: _, committed: _ } => {
                self.on_append_request(leader_id, term, preceding).await
            },
            AppendResponse { member_id, term, success, position } => {
                self.on_append_response(member_id, term, success, position).await
            },
            VoteRequest { candidate_id, term, position: _ } => {
                self.on_vote_request(candidate_id, term).await
            },
            VoteResponse {member_id: _, term, vote_granted: _} => {
                self.on_vote_response(term).await
            },
        }
    }

    async fn on_append_request(&mut self, leader_id: Id, term: u64, preceding: Position) -> Option<State> {
        if self.term > term {
            self.cluster
                .send(
                    &leader_id,
                    Message::append_response(self.id, self.term, false, preceding),
                )
                .await;
            return None;
        }

        assert!(
            term > self.term,
            "Double leader detected - term: {}, leader id: {}",
            term,
            leader_id
        );
        self.redirect_client_requests(Some(&leader_id)).await;
        Some(State::follower(term, Some(leader_id)))
    }

    async fn on_append_response(
        &mut self,
        member_id: Id,
        term: u64,
        success: bool,
        position: Position,
    ) -> Option<State> {
        if term > self.term {
            self.redirect_client_requests(None).await;
            Some(State::follower(position.term(), None))
        } else if success {
            match self.storage.next(&position).await {
                Some((preceding_position, position, entry)) => {
                    let updated = self.registry.on_success(&member_id, preceding_position, &position);
                    if updated {
                        let message = Message::append_request(
                            self.id,
                            self.term,
                            *preceding_position,
                            position.term(),
                            vec![entry.clone()],
                            *self.registry.committed(),
                        );
                        self.cluster.send(&member_id, message).await;
                    }
                }
                None => {
                    self.registry.on_success(&member_id, &position, &position.next());
                }
            }
            None
        } else {
            let (preceding_position, position, entry) = self.storage.at(&position).await.expect("Missing entry");
            let updated = self.registry.on_failure(&member_id, position);
            if updated {
                let message = Message::append_request(
                    self.id,
                    self.term,
                    preceding_position,
                    position.term(),
                    vec![entry.clone()],
                    *self.registry.committed(),
                );
                self.cluster.send(&member_id, message).await;
            }
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
            self.redirect_client_requests(None).await;
            Some(State::follower(term, None))
        } else {
            None
        }
    }

    async fn on_vote_response(&mut self, term: u64) -> Option<State> {
        if term > self.term {
            self.redirect_client_requests(None).await;
            Some(State::follower(term, None))
        } else {
            None
        }
    }

    async fn on_client_request(&mut self, request: Request, responder: Responder) {
        match request {
            StoreRequest { payload, position } => match position {
                Some(position) if self.storage.at(&position).await.is_some() => {
                    assert!(position.term() < self.term);
                    responder.respond_with_success();
                }
                _ => {
                    let position = self.storage.extend(self.term, vec![payload]).await;
                    self.registry.on_client_request(position, responder);
                    self.replicate(self.registry.nexts_with(position)).await;
                }
            },
        }
    }

    async fn replicate(&self, items: impl Iterator<Item = (&Id, &Position)>) {
        let futures = items
            .map(|(member_id, position)| self.replicate_single(member_id, &position))
            .collect::<Vec<_>>();
        join_all(futures).await;
    }

    async fn replicate_single(&self, member_id: &Id, position: &Position) {
        let message = match self.storage.at(&position).await {
            Some((preceding_position, position, entry)) => Message::append_request(
                self.id,
                self.term,
                preceding_position,
                position.term(),
                vec![entry.clone()],
                *self.registry.committed(),
            ),
            None => Message::append_request(
                self.id,
                self.term,
                *self.storage.head(),
                self.term,
                vec![],
                *self.registry.committed(),
            ),
        };
        self.cluster.send(&member_id, message).await;
    }

    async fn redirect_client_requests(&mut self, leader_id: Option<&Id>) {
        let leader_address = leader_id.map(|leader_id| *self.cluster.endpoint(leader_id).client_address());
        self.registry
            .responders()
            .for_each(|(position, responder)| responder.respond_with_redirect(leader_address, Some(position)))
    }
}

struct Registry {
    records: HashMap<Id, Record>,
    responders: VecDeque<(Position, Responder)>,
    committed: Position,
}

impl Registry {
    fn new() -> Self {
        Registry {
            records: HashMap::new(),
            responders: VecDeque::new(),
            committed: Position::initial(),
        }
    }

    fn init(&mut self, member_ids: Vec<Id>, position: Position) {
        member_ids.into_iter().for_each(|id| {
            self.records.insert(id, Record::new(position));
        });
    }

    fn on_client_request(&mut self, position: Position, responder: Responder) {
        if self.records.len() == 0 {
            self.committed = position;
            responder.respond_with_success()
        } else {
            self.responders.push_front((position, responder));
        }
    }

    fn on_failure(&mut self, member_id: &Id, missing: &Position) -> bool {
        self.records
            .get_mut(member_id)
            .expect("Missing member entry")
            .on_failure(missing)
    }

    fn on_success(&mut self, member_id: &Id, replicated: &Position, next: &Position) -> bool {
        let updated = self
            .records
            .get_mut(member_id)
            .expect("Missing member entry")
            .on_success(replicated, next);
        if updated {
            loop {
                match self.responders.back() {
                    Some((position, _)) if position <= replicated => {
                        if self.replicated_on_majority(&position) {
                            self.committed = *position;
                            // safety: self.responders.back() above guarantees an item exists
                            self.responders.pop_back().unwrap().1.respond_with_success()
                        } else {
                            break;
                        }
                    }
                    _ if replicated > &self.committed => {
                        if self.replicated_on_majority(&replicated) {
                            self.committed = *replicated;
                        }
                        break;
                    }
                    _ => break,
                }
            }
        }
        updated
    }

    fn replicated_on_majority(&self, position: &Position) -> bool {
        self.records
            .values()
            .filter(|record| record.replicated(&position))
            .count()
            + 1
            > self.records.len() / 2
    }

    fn nexts(&self) -> impl Iterator<Item = (&Id, &Position)> {
        self.records.iter().map(|(id, record)| (id, record.next()))
    }

    fn nexts_with(&self, position: Position) -> impl Iterator<Item = (&Id, &Position)> {
        self.records
            .iter()
            .filter(move |(_, record)| record.next() == position)
            .map(|(id, record)| (id, record.next()))
    }

    fn responders(&mut self) -> impl Iterator<Item = (Position, Responder)> {
        self.responders.split_off(0).into_iter()
    }

    fn committed(&self) -> &Position {
        &self.committed
    }
}

struct Record {
    replicated: Position,
    next: Position,
}

impl Record {
    fn new(position: Position) -> Self {
        Record {
            replicated: Position::initial(),
            next: position,
        }
    }

    fn replicated(&self, position: &Position) -> bool {
        self.replicated >= *position
    }

    fn next(&self) -> &Position {
        &self.next
    }

    fn on_failure(&mut self, missing: &Position) -> bool {
        if missing <= &self.replicated {
            log::error!(
                "Missing ({:?}) should already have been replicated ({:?})",
                &missing,
                &self.replicated
            );
            self.replicated = Position::initial();
        }
        if missing < &self.next {
            self.next = *missing;
            true
        } else {
            false
        }
    }

    fn on_success(&mut self, replicated: &Position, next: &Position) -> bool {
        if replicated > &self.replicated {
            self.replicated = *replicated;
            self.next = *next;
            true
        } else {
            false
        }
    }
}

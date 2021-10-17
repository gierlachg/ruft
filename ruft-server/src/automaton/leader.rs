use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use futures::future::join_all;

use crate::automaton::Responder;
use crate::automaton::State::{self, TERMINATED};
use crate::cluster::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::cluster::Cluster;
use crate::relay::protocol::Request::{self, StoreRequest};
use crate::relay::Relay;
use crate::storage::{noop_message, Position, Storage};
use crate::Id;

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
            AppendRequest { leader_id, term, preceding_position: _, entries_term: _, entries: _ } => {
                self.on_append_request(leader_id, term).await
            },
            AppendResponse { member_id, term, success, position } => {
                self.on_append_response(member_id, term, success, position).await
            },
            VoteRequest { candidate_id, term, position: _ } => {
                self.on_vote_request(candidate_id, term).await
            },
            VoteResponse {member_id: _, term, vote_granted: _} => {
                self.on_vote_response(term)
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

        assert!(
            term > self.term,
            "Double leader detected - term: {}, leader id: {}",
            term,
            leader_id
        );
        // TODO: redirect all pending requests
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
            // TODO: redirect all pending requests
            Some(State::follower(position.term(), None))
        } else if success {
            match self
                .storage
                .next(&position)
                .await
                .map(|(p, e)| (position, p.clone(), e))
            {
                Some((preceding_position, position, entry)) => {
                    let message = Message::append_request(
                        self.id,
                        self.term,
                        preceding_position,
                        position.term(),
                        vec![entry.clone()],
                    );
                    self.cluster.send(&member_id, message).await;
                    self.registry.on_success(&member_id, position, preceding_position);
                }
                None => self.registry.on_success(&member_id, position.next(), position),
            }
            None
        } else {
            let (preceding_position, position, entry) = self
                .storage
                .at(&position)
                .await
                .map(|(p, e)| (p.clone(), position, e))
                .expect("Missing entry");
            let message = Message::append_request(
                self.id,
                self.term,
                preceding_position,
                position.term(),
                vec![entry.clone()],
            );
            self.cluster.send(&member_id, message).await;
            self.registry.on_failure(&member_id, position);
            None
        }
    }

    async fn on_vote_request(&mut self, candidate_id: Id, term: u64) -> Option<State> {
        if self.term > term {
            self.cluster
                .send(
                    &candidate_id,
                    Message::append_response(self.id, self.term, false, *self.storage.head()),
                )
                .await;
            return None;
        }

        if term > self.term {
            // TODO: redirect all pending requests
            Some(State::follower(term, None))
        } else {
            None
        }
    }

    fn on_vote_response(&mut self, term: u64) -> Option<State> {
        if term > self.term {
            // TODO: redirect all pending requests
            Some(State::follower(term, None))
        } else {
            None
        }
    }

    async fn on_client_request(&mut self, request: Request, responder: Responder) {
        match request {
            StoreRequest { payload } => {
                let position = self.storage.extend(self.term, vec![payload]).await;
                self.registry.on_client_request(position, responder);
                self.replicate(self.registry.nexts_replicated(position)).await;
            }
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
            Some((preceding_position, entry)) => Message::append_request(
                self.id,
                self.term,
                *preceding_position,
                position.term(),
                vec![entry.clone()],
            ),
            None => Message::append_request(self.id, self.term, *self.storage.head(), self.term, vec![]),
        };
        self.cluster.send(&member_id, message).await;
    }
}

struct Registry {
    records: HashMap<Id, Record>,
    responders: VecDeque<(Position, Responder)>,
}

impl Registry {
    fn new() -> Self {
        Registry {
            records: HashMap::new(),
            responders: VecDeque::new(),
        }
    }

    fn init(&mut self, member_ids: Vec<Id>, position: Position) {
        member_ids.into_iter().for_each(|id| {
            self.records.insert(id, Record::new(position));
        });
    }

    fn on_client_request(&mut self, position: Position, responder: Responder) {
        if self.records.len() == 0 {
            responder.respond_with_success()
        } else {
            self.responders.push_front((position, responder));
        }
    }

    fn on_failure(&mut self, member_id: &Id, missing: Position) {
        self.records
            .get_mut(member_id)
            .expect("Missing member entry")
            .on_failure(missing)
    }

    fn on_success(&mut self, member_id: &Id, next: Position, replicated: Position) {
        self.records
            .get_mut(member_id)
            .expect("Missing member entry")
            .on_success(next, replicated);

        loop {
            match self.responders.back() {
                None => break,
                Some((position, _)) if *position > replicated => break,
                Some((position, _)) => {
                    let replications = self
                        .records
                        .values()
                        .filter(|record| record.already_replicated(&position))
                        .count();
                    if replications >= (self.records.len() + 1) / 2 {
                        // safety: self.responders.back() above guarantees an item exists
                        self.responders.pop_back().unwrap().1.respond_with_success()
                    } else {
                        break;
                    }
                }
            }
        }
    }

    fn nexts(&self) -> impl Iterator<Item = (&Id, &Position)> {
        self.records.iter().map(|(id, record)| (id, record.next()))
    }

    fn nexts_replicated(&self, position: Position) -> impl Iterator<Item = (&Id, &Position)> {
        self.records
            .iter()
            .filter(move |(_, record)| record.next() == position)
            .map(|(id, record)| (id, record.next()))
    }
}

// TODO: sent recently
struct Record {
    next: Position,
    replicated: Option<Position>,
}

impl Record {
    fn new(position: Position) -> Self {
        Record {
            next: position,
            replicated: None,
        }
    }

    fn next(&self) -> &Position {
        &self.next
    }

    fn on_failure(&mut self, missing: Position) {
        self.next = missing
    }

    fn on_success(&mut self, next: Position, replicated: Position) {
        self.next = next;
        self.replicated.replace(replicated);
    }

    fn already_replicated(&self, position: &Position) -> bool {
        self.replicated.map_or(false, |replicated| replicated >= *position)
    }
}

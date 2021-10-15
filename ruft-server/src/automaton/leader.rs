use std::collections::HashMap;
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

    replicator: Replicator,

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
        let replicator = Replicator::new(cluster, Position::of(term, 0));

        Leader {
            id,
            term,
            storage,
            cluster,
            relay,
            replicator,
            heartbeat_interval,
        }
    }

    pub(super) async fn run(&mut self) -> State {
        assert_eq!(
            self.storage.extend(self.term, vec![noop_message()]).await,
            Position::of(self.term, 0)
        );

        let mut ticker = tokio::time::interval(self.heartbeat_interval);
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
        let futures = self
            .replicator
            .next_positions()
            .map(|(member_id, next_position)| self.replicate_or_heartbeat(member_id, next_position))
            .collect::<Vec<_>>();
        join_all(futures).await;
    }

    async fn replicate_or_heartbeat(&self, member_id: &Id, position: Option<&Position>) {
        let message = match position {
            Some(position) => match self.storage.at(&position).await {
                Some((preceding_position, entry)) => Message::append_request(
                    self.id,
                    self.term,
                    *preceding_position,
                    position.term(),
                    vec![entry.clone()],
                ),
                None => panic!("Missing entry at {:?}", &position),
            },
            None => Message::append_request(self.id, self.term, *self.storage.head(), self.term, vec![]),
        };
        self.cluster.send(&member_id, message).await;
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
            Some(State::follower(position.term(), None))
        } else if success {
            if position == *self.storage.head() {
                self.replicator.on_success(&member_id, None, position);
            } else {
                let (preceding_position, position, entry) = self
                    .storage
                    .next(&position)
                    .await
                    .map(|(p, e)| (position, p.clone(), e))
                    .expect("Missing entry");
                let message = Message::append_request(
                    self.id,
                    self.term,
                    preceding_position,
                    position.term(),
                    vec![entry.clone()],
                );
                self.cluster.send(&member_id, message).await;
                self.replicator
                    .on_success(&member_id, Some(position), preceding_position);
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
            self.replicator.on_failure(&member_id, position);
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
            Some(State::follower(term, None))
        } else {
            None
        }
    }

    fn on_vote_response(&mut self, term: u64) -> Option<State> {
        if term > self.term {
            Some(State::follower(term, None))
        } else {
            None
        }
    }

    async fn on_client_request(&mut self, request: Request, responder: Responder) {
        // TODO: store in exchanges ? leadership lost...
        match request {
            StoreRequest { payload } => {
                let position = self.storage.extend(self.term, vec![payload]).await; // TODO: replicate, get rid of vec!
                self.replicator.on_client_request(position, responder);
            }
        }
    }
}

struct Replicator {
    // TODO: sent recently
    views: HashMap<Id, LogView>,
    responders: HashMap<Position, Responder>,
}

impl Replicator {
    fn new(cluster: &dyn Cluster, init_position: Position) -> Self {
        Replicator {
            views: cluster
                .member_ids()
                .into_iter()
                .map(|id| (id, LogView::new(init_position)))
                .collect::<HashMap<_, _>>(),
            responders: HashMap::new(),
        }
    }

    fn next_positions(&self) -> impl Iterator<Item = (&Id, Option<&Position>)> {
        self.views.iter().map(|(id, view)| (id, view.next_position()))
    }

    fn on_client_request(&mut self, position: Position, responder: Responder) {
        if self.views.len() == 0 {
            responder.respond_with_success()
        } else {
            self.responders.insert(position, responder);
        }
    }

    fn on_failure(&mut self, member_id: &Id, missing_position: Position) {
        self.views
            .get_mut(member_id)
            .expect("Missing member entry")
            .on_failure(missing_position)
    }

    fn on_success(&mut self, member_id: &Id, next_position: Option<Position>, replicated_position: Position) {
        self.views
            .get_mut(member_id)
            .expect("Missing member entry")
            .on_success(next_position, replicated_position);

        // TODO:
        let replication_count = self
            .views
            .values()
            .filter(|view| view.already_replicated(&replicated_position))
            .count();

        if replication_count >= (self.views.len() + 1) / 2 {
            // TODO: response is always sent back ...
            if let Some(responder) = self.responders.remove(&replicated_position) {
                responder.respond_with_success()
            }
        }
    }
}

struct LogView {
    next_position: Option<Position>,
    replicated_position: Option<Position>,
}

impl LogView {
    fn new(init_position: Position) -> Self {
        LogView {
            next_position: Some(init_position),
            replicated_position: None,
        }
    }

    fn next_position(&self) -> Option<&Position> {
        self.next_position.as_ref()
    }

    fn on_failure(&mut self, missing_position: Position) {
        self.next_position.replace(missing_position);
    }

    fn on_success(&mut self, next_position: Option<Position>, replicated_position: Position) {
        self.next_position = next_position;
        self.replicated_position.replace(replicated_position);
    }

    fn already_replicated(&self, position: &Position) -> bool {
        self.replicated_position
            .map_or(false, |replicated_position| replicated_position >= *position)
    }
}

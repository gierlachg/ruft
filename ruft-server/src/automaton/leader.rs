use std::collections::HashMap;
use std::time::Duration;

use futures::future::join_all;

use crate::automaton::Responder;
use crate::automaton::State::{self, TERMINATED};
use crate::cluster::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::cluster::Cluster;
use crate::relay::protocol::Request;
use crate::relay::protocol::Request::StoreRequest;
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
            .positions
            .iter()
            .map(|(member_id, (next_position, _))| self.replicate_or_heartbeat(member_id, next_position.as_ref()))
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
                self.replicator.on_success(&member_id, position, None);
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
                    .on_success(&member_id, preceding_position, Some(position));
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
    majority: usize,
    positions: HashMap<Id, (Option<Position>, Option<Position>)>,
    responders: HashMap<Position, Responder>,
}

impl Replicator {
    fn new(cluster: &dyn Cluster, init_position: Position) -> Self {
        Replicator {
            majority: cluster.size() / 2, // TODO:
            positions: cluster
                .member_ids()
                .into_iter()
                .map(|id| (id, (Some(init_position), None)))
                .collect::<HashMap<_, _>>(),
            responders: HashMap::new(),
        }
    }

    fn on_client_request(&mut self, position: Position, responder: Responder) {
        if self.majority > 0 {
            self.responders.insert(position, responder);
        } else {
            responder.respond_with_success()
        }
    }

    fn on_failure(&mut self, member_id: &Id, missing_position: Position) {
        let (next_position, _) = self.positions.get_mut(member_id).expect("Missing member entry");
        next_position.replace(missing_position);
    }

    fn on_success(&mut self, member_id: &Id, replicated_position: Position, next_position: Option<Position>) {
        let (np, rp) = self.positions.get_mut(member_id).expect("Missing member entry");
        next_position
            .and_then(|next_position| np.replace(next_position))
            .or_else(|| np.take());
        rp.replace(replicated_position);

        // TODO:
        let replication_count = self
            .positions
            .values()
            .filter_map(|(_, rp)| rp.filter(|position| *position >= replicated_position))
            .count();

        if replication_count >= self.majority {
            // TODO: response is always sent back ...
            if let Some(responder) = self.responders.remove(&replicated_position) {
                responder.respond_with_success()
            }
        }
    }
}

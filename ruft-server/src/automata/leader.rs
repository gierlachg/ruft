use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use futures::future::join_all;
use tokio_stream::StreamExt;

use crate::automata::fsm::{Operation, FSM};
use crate::automata::Responder;
use crate::automata::Transition::{self, TERMINATED};
use crate::cluster::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::cluster::Cluster;
use crate::relay::protocol::Request::{self, ReplicateRequest};
use crate::relay::Relay;
use crate::storage::Log;
use crate::{Id, Position};

// TODO: address liveness issues https://decentralizedthoughts.github.io/2020-12-12-raft-liveness-full-omission/

pub(super) struct Leader<'a, L: Log, C: Cluster, R: Relay> {
    id: Id,
    term: u64,
    log: &'a mut L,
    cluster: &'a mut C,
    relay: &'a mut R,
    fsm: &'a mut FSM,

    registry: Registry,

    heartbeat_interval: Duration,
}

impl<'a, L: Log, C: Cluster, R: Relay> Leader<'a, L, C, R> {
    pub(super) fn init(
        id: Id,
        term: u64,
        log: &'a mut L,
        cluster: &'a mut C,
        relay: &'a mut R,
        fsm: &'a mut FSM,
        heartbeat_interval: Duration,
    ) -> Self {
        Leader {
            id,
            term,
            log,
            cluster,
            relay,
            fsm,
            registry: Registry::new(),
            heartbeat_interval,
        }
    }

    pub(super) async fn run(mut self) -> Transition {
        // TODO:
        self.registry.init(
            self.cluster.members(),
            // TODO:
            self.log.extend(self.term, vec![Operation::NoOperation.into()]).await,
        );

        let mut ticker = tokio::time::interval(self.heartbeat_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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

    async fn on_message(&mut self, message: Message) -> Option<Transition> {
        #[rustfmt::skip]
        match message {
            AppendRequest { leader, term, preceding, entries_term: _, entries: _, committed: _ } => {
                self.on_append_request(leader, term, preceding).await
            },
            AppendResponse { member, term, position } => {
                self.on_append_response(member, term, position).await
            },
            VoteRequest { candidate, term, position: _ } => {
                self.on_vote_request(candidate, term).await
            },
            VoteResponse { member: _, term, vote_granted: _} => {
                self.on_vote_response(term).await
            },
        }
    }

    async fn on_append_request(&mut self, leader: Id, term: u64, preceding: Position) -> Option<Transition> {
        if self.term > term {
            self.cluster
                .send(&leader, Message::append_response(self.id, self.term, Err(preceding)))
                .await;
            None
        } else if self.term == term {
            panic!("Double leader detected - term: {}, leader id: {:?}", term, leader);
        } else {
            self.redirect_client_requests(Some(&leader)).await;
            Some(Transition::follower(term, Some(leader)))
        }
    }

    async fn on_append_response(
        &mut self,
        member: Id,
        term: u64,
        position: Result<Position, Position>,
    ) -> Option<Transition> {
        if self.term >= term {
            match position {
                Ok(position) => match self.log.next(position).await {
                    Some((preceding_position, position, entry)) => {
                        // TODO:
                        let (updated, committed) = self.registry.on_success(&member, &preceding_position, &position);
                        self.apply(committed).await;
                        if updated {
                            let message = Message::append_request(
                                self.id,
                                self.term,
                                preceding_position,
                                position.term(),
                                vec![entry],
                                *self.registry.committed(),
                            );
                            self.cluster.send(&member, message).await;
                        }
                    }
                    None => {
                        // TODO:
                        let (_, committed) = self.registry.on_success(&member, &position, &position.next());
                        self.apply(committed).await;
                    }
                },
                Err(position) => {
                    let (preceding_position, position, entry) = self.log.at(position).await.expect("Missing entry");
                    if self.registry.on_failure(&member, &position) {
                        let message = Message::append_request(
                            self.id,
                            self.term,
                            preceding_position,
                            position.term(),
                            vec![entry],
                            *self.registry.committed(),
                        );
                        self.cluster.send(&member, message).await;
                    }
                }
            }
            None
        } else {
            self.redirect_client_requests(None).await;
            Some(Transition::follower(term, None))
        }
    }

    // TODO: move to FSM
    async fn apply(&mut self, committed: Position) {
        if self.fsm.applied() < committed {
            while let Some((position, payload)) = self
                .log
                .into_stream()
                .skip_while(|(position, _)| position <= &self.fsm.applied())
                .take_while(|(position, _)| position <= &committed)
                .next()
                .await
            {
                self.fsm.apply(position, payload);
            }
        }
    }

    async fn on_vote_request(&mut self, candidate: Id, term: u64) -> Option<Transition> {
        if self.term > term {
            self.cluster
                .send(&candidate, Message::vote_response(self.id, self.term, false))
                .await;
            None
        } else if self.term == term {
            None
        } else {
            self.redirect_client_requests(None).await;
            Some(Transition::follower(term, None))
        }
    }

    async fn on_vote_response(&mut self, term: u64) -> Option<Transition> {
        if self.term >= term {
            None
        } else {
            self.redirect_client_requests(None).await;
            Some(Transition::follower(term, None))
        }
    }

    async fn on_client_request(&mut self, request: Request, responder: Responder) {
        match request {
            ReplicateRequest { payload, position } => match position {
                Some(position) if self.log.at(position).await.is_some() => {
                    assert!(position.term() < self.term);
                    self.registry.on_client_request(position, responder);
                }
                _ => {
                    let position = self.log.extend(self.term, vec![payload]).await;
                    self.registry.on_client_request(position, responder);
                    self.replicate(self.registry.nexts_with(position)).await;
                }
            },
        }
    }

    async fn replicate(&self, items: impl Iterator<Item = (&Id, &Position)>) {
        let futures = items
            .map(|(member, position)| self.replicate_single(member, *position))
            .collect::<Vec<_>>();
        join_all(futures).await;
    }

    async fn replicate_single(&self, member: &Id, position: Position) {
        let message = match self.log.at(position).await {
            Some((preceding_position, position, entry)) => Message::append_request(
                self.id,
                self.term,
                preceding_position,
                position.term(),
                vec![entry],
                *self.registry.committed(),
            ),
            None => Message::append_request(
                self.id,
                self.term,
                *self.log.head(),
                self.term,
                vec![],
                *self.registry.committed(),
            ),
        };
        self.cluster.send(&member, message).await;
    }

    async fn redirect_client_requests(&mut self, leader: Option<&Id>) {
        let leader_address = leader.map(|leader| *self.cluster.endpoint(leader).client_address());
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

    fn init(&mut self, members: Vec<Id>, position: Position) {
        members.into_iter().for_each(|id| {
            self.records.insert(id, Record::new(position));
        });
    }

    fn on_client_request(&mut self, position: Position, responder: Responder) {
        if self.committed >= position {
            responder.respond_with_success()
        } else if self.records.len() == 0 {
            self.committed = position;
            responder.respond_with_success()
        } else {
            let index = self
                .responders
                .iter()
                .rev()
                .position(|(p, _)| p < &position)
                .map(|p| p + 1)
                .unwrap_or(0);
            self.responders.insert(index, (position, responder));
        }
    }

    fn on_failure(&mut self, member: &Id, missing: &Position) -> bool {
        self.records
            .get_mut(member)
            .expect("Missing member entry")
            .on_failure(missing)
    }

    fn on_success(&mut self, member: &Id, replicated: &Position, next: &Position) -> (bool, Position) {
        let updated = self
            .records
            .get_mut(member)
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
        (updated, self.committed) // TODO: response should be sent back to the client...
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

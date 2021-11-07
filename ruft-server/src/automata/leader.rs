use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use futures::future::join_all;
use tokio_stream::{Stream, StreamExt};

use crate::automata::fsm::{Operation, FSM};
use crate::automata::Responder;
use crate::automata::Transition::{self, TERMINATED};
use crate::cluster::protocol::Message::{self, AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use crate::cluster::Cluster;
use crate::relay::protocol::Request::{self, ReplicateRequest};
use crate::relay::Relay;
use crate::storage::Log;
use crate::{Id, Payload, Position};

pub(super) struct Leader<'a, L: Log, C: Cluster, R: Relay> {
    id: Id,
    term: u64,
    log: &'a mut L,
    cluster: &'a mut C,
    relay: &'a mut R,

    registry: Registry<'a>,

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
        let registry = Registry::new(cluster.members(), Position::of(term, 0), fsm);
        Leader {
            id,
            term,
            log,
            cluster,
            relay,
            registry,
            heartbeat_interval,
        }
    }

    pub(super) async fn run(mut self) -> Transition {
        // TODO:
        self.log.extend(self.term, vec![Operation::NoOperation.into()]).await;

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
        self.replicate(self.registry.nexts(|_| true)).await;
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
            if let Some((preceding, current, entry)) = match position {
                Ok(position) => {
                    let entries = self.log.into_stream();
                    match self.log.next(position).await {
                        Some((preceding, current, entry)) => {
                            if self.registry.on_success(&member, &preceding, &current, entries).await {
                                Some((preceding, current, entry))
                            } else {
                                None
                            }
                        }
                        None => {
                            self.registry
                                .on_success(&member, &position, &position.next(), entries)
                                .await;
                            None
                        }
                    }
                }
                Err(position) => {
                    if self.registry.on_failure(&member, &position) {
                        Some(self.log.at(position).await.expect("Missing entry"))
                    } else {
                        None
                    }
                }
            } {
                let message = Message::append_request(
                    self.id,
                    self.term,
                    preceding,
                    current.term(),
                    vec![entry],
                    *self.registry.committed(),
                );
                self.cluster.send(&member, message).await;
            }
            None
        } else {
            self.redirect_client_requests(None).await;
            Some(Transition::follower(term, None))
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
                    self.replicate(self.registry.nexts(|p| p == position)).await;
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
        let (preceding, term, entries) = self
            .log
            .at(position)
            .await
            .map(|(preceding, current, entry)| (preceding, current.term(), vec![entry]))
            .unwrap_or((*self.log.head(), self.term, vec![]));
        let message = Message::append_request(self.id, self.term, preceding, term, entries, *self.registry.committed());
        self.cluster.send(&member, message).await;
    }

    async fn redirect_client_requests(&mut self, leader: Option<&Id>) {
        // TODO: reads should be redirected without position ???
        let leader_address = leader.map(|leader| *self.cluster.endpoint(leader).client_address());
        self.registry
            .responders()
            .for_each(|(position, responder)| responder.respond_with_redirect(leader_address, Some(position)))
    }
}

struct Registry<'a> {
    records: HashMap<Id, Record>,
    responders: VecDeque<(Position, Responder)>,
    committed: Position,
    fsm: &'a mut FSM,
}

impl<'a> Registry<'a> {
    fn new(members: Vec<Id>, position: Position, fsm: &'a mut FSM) -> Self {
        Registry {
            records: members.into_iter().map(|id| (id, Record::new(position))).collect(),
            responders: VecDeque::new(),
            committed: Position::initial(),
            fsm,
        }
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

    async fn on_success(
        &mut self,
        member: &Id,
        replicated: &Position,
        next: &Position,
        entries: impl Stream<Item = (Position, Payload)>,
    ) -> bool {
        let updated = self
            .records
            .get_mut(member)
            .expect("Missing member entry")
            .on_success(replicated, next);
        if updated {
            let committed = self.committed;
            tokio::pin! {
                // TODO: optimize access
                let entries = entries
                    .skip_while(|(position, _)| position <= &committed)
                    .take_while(|(position, _)| position <= &replicated);
            }
            while let Some((position, entry)) = entries.next().await {
                if !self.replicated_on_majority(&position) {
                    break;
                }

                self.fsm.apply(&entry);
                if let Some((_, responder)) = match self.responders.back() {
                    Some((p, _)) if p == position => self.responders.pop_back(),
                    _ => None,
                } {
                    responder.respond_with_success();
                }

                self.committed = position;
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

    fn nexts<P: Fn(&Position) -> bool>(&self, predicate: P) -> impl Iterator<Item = (&Id, &Position)> {
        self.records
            .iter()
            .filter(move |(_, record)| predicate(record.next()))
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

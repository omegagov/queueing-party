use std::cell::RefCell;
use std::cmp::{max, Eq, Ordering, PartialEq};
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, LogNormal};
use rand_xoshiro::Xoshiro256StarStar;

use crate::args_rets::*;
use crate::lossy_convert::*;

#[derive(Default)]
pub struct WorkerTokenArgs {
    timestamp: u64,
    worker_token: Option<WorkerToken>,
}

#[derive(Default)]
pub struct WorkerTokenReturn {
    proposed_events: Vec<ProposedEvent>,
    worker_tokens_to_restore: Vec<WorkerToken>,
}

pub trait HasWorkerToken {
    fn get_worker_token(&self) -> &Option<WorkerToken>;
    fn get_worker_token_mut(&mut self) -> &mut Option<WorkerToken>;
    fn set_worker_token(&mut self, new_value: Option<WorkerToken>);
}

pub trait HasWorkerTokensToRestore {
    fn get_worker_tokens_to_restore(&self) -> &Vec<WorkerToken>;
    fn get_worker_tokens_to_restore_mut(&mut self) -> &mut Vec<WorkerToken>;
    fn set_worker_tokens_to_restore(&mut self, new_value: Vec<WorkerToken>);
}

impl HasTimestamp for WorkerTokenArgs {
    fn get_timestamp(&self) -> &u64 {
        &self.timestamp
    }
    fn set_timestamp(&mut self, new_value: u64) {
        self.timestamp = new_value
    }
}

impl HasWorkerToken for WorkerTokenArgs {
    fn get_worker_token(&self) -> &Option<WorkerToken> {
        &self.worker_token
    }
    fn get_worker_token_mut(&mut self) -> &mut Option<WorkerToken> {
        &mut self.worker_token
    }
    fn set_worker_token(&mut self, new_value: Option<WorkerToken>) {
        self.worker_token = new_value
    }
}

impl HasProposedEvents for WorkerTokenReturn {
    fn get_proposed_events(&self) -> &Vec<ProposedEvent> {
        &self.proposed_events
    }
    fn get_proposed_events_mut(&mut self) -> &mut Vec<ProposedEvent> {
        &mut self.proposed_events
    }
    fn set_proposed_events(&mut self, new_value: Vec<ProposedEvent>) {
        self.proposed_events = new_value
    }
}

impl HasWorkerTokensToRestore for WorkerTokenReturn {
    fn get_worker_tokens_to_restore(&self) -> &Vec<WorkerToken> {
        &self.worker_tokens_to_restore
    }
    fn get_worker_tokens_to_restore_mut(&mut self) -> &mut Vec<WorkerToken> {
        &mut self.worker_tokens_to_restore
    }
    fn set_worker_tokens_to_restore(&mut self, new_value: Vec<WorkerToken>) {
        self.worker_tokens_to_restore = new_value
    }
}

impl FromLossy<u64> for WorkerTokenArgs {
    fn from_lossy(other: u64) -> WorkerTokenArgs {
        WorkerTokenArgs {
            timestamp: other,
            ..Default::default()
        }
    }
}

impl FromLossy<Vec<ProposedEvent>> for WorkerTokenReturn {
    fn from_lossy(other: Vec<ProposedEvent>) -> WorkerTokenReturn {
        WorkerTokenReturn {
            proposed_events: other,
            ..Default::default()
        }
    }
}

pub struct Queue {
    name: String,
    listening_workers: HashSet<Rc<Worker>>,
    deque: VecDeque<Box<dyn FnOnce(WorkerTokenArgs) -> Vec<ProposedEvent>>>,
    rng: Xoshiro256StarStar,
    //  metrics: ...,
}

impl Queue {
    fn enqueued_handler_inner<Ai, R>(
        &mut self,
        inner_handler: impl FnOnce(Ai) -> R + 'static,
        mut args: Ai,
    ) -> R
    where
        Ai: HasTimestamp + HasWorkerToken,
        R: HasProposedEvents + IntoLossy<Vec<ProposedEvent>> + Default,
        WorkerTokenArgs: IntoLossy<Ai>,
    {
        if self.deque.is_empty() && !self.listening_workers.is_empty() {
            let chosen_worker_rc = Clone::clone(
                self.listening_workers
                    .iter()
                    .nth(self.rng.gen_range(0..self.listening_workers.len()))
                    .unwrap(),
            );

            self.listening_workers.remove(&chosen_worker_rc);

            for other_queue in &chosen_worker_rc.subscribed_queues {
                other_queue
                    .borrow_mut()
                    .listening_workers
                    .remove(&chosen_worker_rc);
            }

            args.set_worker_token(Some(WorkerToken {
                worker: Rc::into_inner(chosen_worker_rc).unwrap(),
                checkout_timestamp: *args.get_timestamp(),
                originating_queue_name: self.name.clone(),
            }));

            // TODO tally metric

            return inner_handler(args);
        } else {
            self.deque.push_back(Box::new(move |args_outer| {
                inner_handler(args_outer.into_lossy()).into_lossy()
            }));
            return Default::default();
        }
    }

    pub fn mk_enqueued_handler<Ao, Ai, R>(
        queue: Rc<RefCell<Queue>>,
        inner_handler: impl FnOnce(Ai) -> R + 'static,
    ) -> impl FnOnce(Ao) -> R
    where
        Ao: HasTimestamp + IntoLossy<Ai>,
        Ai: HasTimestamp + HasWorkerToken,
        R: HasProposedEvents + IntoLossy<Vec<ProposedEvent>> + Default,
        WorkerTokenArgs: IntoLossy<Ai>,
    {
        move |args_outer: Ao| {
            queue
                .borrow_mut()
                .enqueued_handler_inner(inner_handler, args_outer.into_lossy())
        }
    }
}

pub struct Worker {
    id: u64,
    subscribed_queues: Vec<Rc<RefCell<Queue>>>,
    shutting_down: Rc<RefCell<bool>>,
    // shared_cpu_resource: Rc<RefCell<SharedRateResource>>,
    rng: Xoshiro256StarStar,
    // E: ext,
    //  metrics: ...,
}

impl Hash for Worker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for Worker {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Worker {}

pub struct WorkerToken {
    worker: Worker,
    checkout_timestamp: u64,
    originating_queue_name: String,
}

impl WorkerToken {
    pub fn mk_token_restoring_handler<A, Ri, Ro>(
        inner_handler: impl FnOnce(A) -> Ri,
    ) -> impl FnOnce(A) -> Ro
    where
        A: HasTimestamp,
        Ri: HasProposedEvents + HasWorkerTokensToRestore + IntoLossy<Ro>,
        Ro: HasProposedEvents,
    {
        |args: A| {
            let timestamp = *args.get_timestamp();
            // call inner handler
            let mut ret = inner_handler(args);

            // proposed events that follow-on handlers may produce
            let mut followon_proposed_events = Vec::new();

            // handle restored tokens
            for mut token in ret.get_worker_tokens_to_restore_mut().drain(..) {
                assert!(
                    token.checkout_timestamp < timestamp,
                    "Cannot restore WorkerToken until after time period it was checked out",
                );

                let nonempty_queues = Vec::from_iter(
                    token
                        .worker
                        .subscribed_queues
                        .iter()
                        .filter(|q| !q.borrow().deque.is_empty()),
                );
                if nonempty_queues.is_empty() {
                    // return worker to all subscribed queues
                    let worker_rc = Rc::new(token.worker);
                    for queue in &worker_rc.subscribed_queues {
                        queue
                            .borrow_mut()
                            .listening_workers
                            .insert(worker_rc.clone());
                    }
                } else {
                    // this worker picks up a new handler from a nonempty queue

                    // choose a nonempty queue
                    let chosen_queue =
                        SliceRandom::choose(&nonempty_queues[..], &mut token.worker.rng).unwrap();
                    let followon_handler = chosen_queue.borrow_mut().deque.pop_front().unwrap();
                    let chosen_queue_name = chosen_queue.borrow().name.clone();

                    // prepare args
                    let followon_args = WorkerTokenArgs {
                        timestamp: timestamp,
                        worker_token: Some(WorkerToken {
                            originating_queue_name: chosen_queue_name,
                            worker: token.worker,
                            checkout_timestamp: timestamp,
                        }),
                        ..Default::default()
                    };

                    // TODO tally metric

                    // call follow-on handler
                    let mut followon_ret = followon_handler(followon_args);

                    // gather its proposed events
                    followon_proposed_events.append(followon_ret.get_proposed_events_mut());
                }
            }

            // combine proposed events from follow-ons into our ret
            ret.get_proposed_events_mut()
                .append(&mut followon_proposed_events);

            return ret.into_lossy();
        }
    }
}

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
use crate::status::*;

pub struct WorkerTokenArgs<Xw> {
    timestamp: u64,
    worker_token: Option<WorkerToken<Xw>>,
}

pub struct WorkerTokenReturn<Xw> {
    proposed_events: Vec<ProposedEvent>,
    worker_tokens_to_restore: Vec<WorkerToken<Xw>>,
}

// avoid derived Default's unnecessary Default bounds on free generic parameter
impl<Xw> Default for WorkerTokenArgs<Xw> {
    fn default() -> Self {
        WorkerTokenArgs {
            timestamp: Default::default(),
            worker_token: Default::default(),
        }
    }
}

// avoid derived Default's unnecessary Default bounds on free generic parameter
impl<Xw> Default for WorkerTokenReturn<Xw> {
    fn default() -> Self {
        WorkerTokenReturn {
            proposed_events: Default::default(),
            worker_tokens_to_restore: Default::default(),
        }
    }
}

pub trait HasWorkerToken<Xw> {
    fn get_worker_token(&self) -> &Option<WorkerToken<Xw>>;
    fn get_worker_token_mut(&mut self) -> &mut Option<WorkerToken<Xw>>;
    fn set_worker_token(&mut self, new_value: Option<WorkerToken<Xw>>);
}

pub trait HasWorkerTokensToRestore<Xw> {
    fn get_worker_tokens_to_restore(&self) -> &Vec<WorkerToken<Xw>>;
    fn get_worker_tokens_to_restore_mut(&mut self) -> &mut Vec<WorkerToken<Xw>>;
    fn set_worker_tokens_to_restore(&mut self, new_value: Vec<WorkerToken<Xw>>);
}

impl<Xw> HasTimestamp for WorkerTokenArgs<Xw> {
    fn get_timestamp(&self) -> &u64 {
        &self.timestamp
    }
    fn set_timestamp(&mut self, new_value: u64) {
        self.timestamp = new_value
    }
}

impl<Xw> HasWorkerToken<Xw> for WorkerTokenArgs<Xw> {
    fn get_worker_token(&self) -> &Option<WorkerToken<Xw>> {
        &self.worker_token
    }
    fn get_worker_token_mut(&mut self) -> &mut Option<WorkerToken<Xw>> {
        &mut self.worker_token
    }
    fn set_worker_token(&mut self, new_value: Option<WorkerToken<Xw>>) {
        self.worker_token = new_value
    }
}

impl<Xw> HasProposedEvents for WorkerTokenReturn<Xw> {
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

impl<Xw> HasWorkerTokensToRestore<Xw> for WorkerTokenReturn<Xw> {
    fn get_worker_tokens_to_restore(&self) -> &Vec<WorkerToken<Xw>> {
        &self.worker_tokens_to_restore
    }
    fn get_worker_tokens_to_restore_mut(&mut self) -> &mut Vec<WorkerToken<Xw>> {
        &mut self.worker_tokens_to_restore
    }
    fn set_worker_tokens_to_restore(&mut self, new_value: Vec<WorkerToken<Xw>>) {
        self.worker_tokens_to_restore = new_value
    }
}

impl<Xw> FromLossy<u64> for WorkerTokenArgs<Xw> {
    fn from_lossy(other: u64) -> WorkerTokenArgs<Xw> {
        WorkerTokenArgs {
            timestamp: other,
            ..Default::default()
        }
    }
}

impl<Xw> FromLossy<Vec<ProposedEvent>> for WorkerTokenReturn<Xw> {
    fn from_lossy(other: Vec<ProposedEvent>) -> WorkerTokenReturn<Xw> {
        WorkerTokenReturn {
            proposed_events: other,
            ..Default::default()
        }
    }
}

pub struct Queue<Xw> {
    name: String,
    listening_workers: HashSet<Rc<Worker<Xw>>>,
    deque: VecDeque<Box<dyn FnOnce(WorkerTokenArgs<Xw>) -> Vec<ProposedEvent>>>,
    rng: Xoshiro256StarStar,
    //  metrics: ...,
}

impl<Xw> Queue<Xw> {
    fn pick_worker(&mut self) -> Option<Worker<Xw>> {
        while !self.listening_workers.is_empty() {
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

            let chosen_worker = Rc::into_inner(chosen_worker_rc).unwrap();

            if *chosen_worker.status.borrow() != Status::Running {
                chosen_worker.shutdown();
            } else {
                return Some(chosen_worker);
            }
        }

        None
    }

    fn enqueued_handler_inner<Ai, R>(
        &mut self,
        inner_handler: impl FnOnce(Ai) -> R + 'static,
        mut args: Ai,
    ) -> R
    where
        Ai: HasTimestamp + HasWorkerToken<Xw>,
        R: HasProposedEvents + IntoLossy<Vec<ProposedEvent>> + Default,
        WorkerTokenArgs<Xw>: IntoLossy<Ai>,
    {
        if self.deque.is_empty() {
            if let Some(worker) = self.pick_worker() {
                args.set_worker_token(Some(WorkerToken {
                    worker: worker,
                    checkout_timestamp: *args.get_timestamp(),
                    originating_queue_name: self.name.clone(),
                }));

                // TODO tally metric

                return inner_handler(args);
            }
        }

        self.deque.push_back(Box::new(move |args_outer| {
            inner_handler(args_outer.into_lossy()).into_lossy()
        }));

        Default::default()
    }

    pub fn mk_enqueued_handler<Ao, Ai, R>(
        queue: Rc<RefCell<Queue<Xw>>>,
        inner_handler: impl FnOnce(Ai) -> R + 'static,
    ) -> impl FnOnce(Ao) -> R
    where
        Ao: HasTimestamp + IntoLossy<Ai>,
        Ai: HasTimestamp + HasWorkerToken<Xw>,
        R: HasProposedEvents + IntoLossy<Vec<ProposedEvent>> + Default,
        WorkerTokenArgs<Xw>: IntoLossy<Ai>,
    {
        move |args_outer: Ao| {
            queue
                .borrow_mut()
                .enqueued_handler_inner(inner_handler, args_outer.into_lossy())
        }
    }
}

pub struct Worker<Xw> {
    id: u64,
    subscribed_queues: Vec<Rc<RefCell<Queue<Xw>>>>,
    status: Rc<RefCell<Status>>,
    allow_drop: bool,
    rng: Xoshiro256StarStar,
    ext: Xw,
    //  metrics: ...,
}

impl<Xw> Hash for Worker<Xw> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<Xw> PartialEq for Worker<Xw> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<Xw> Eq for Worker<Xw> {}

impl<Xw> Drop for Worker<Xw> {
    fn drop(&mut self) {
        if !self.allow_drop {
            panic!("Worker was dropped without proper shutdown");
        }
    }
}

impl<Xw> Worker<Xw> {
    pub fn shutdown(mut self) {
        self.allow_drop = true;
        // should now drop as method took ownership
    }
}

pub struct WorkerToken<Xw> {
    worker: Worker<Xw>,
    checkout_timestamp: u64,
    originating_queue_name: String,
}

impl<Xw> WorkerToken<Xw> {
    pub fn mk_token_restoring_handler<A, Ri, Ro>(
        inner_handler: impl FnOnce(A) -> Ri,
    ) -> impl FnOnce(A) -> Ro
    where
        A: HasTimestamp,
        Ri: HasProposedEvents + HasWorkerTokensToRestore<Xw> + IntoLossy<Ro>,
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

                if *token.worker.status.borrow() != Status::Running {
                    token.worker.shutdown();
                    continue;
                }

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

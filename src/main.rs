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

pub mod args_rets;
pub mod lossy_convert;
pub mod shared_rate_resource;

use crate::args_rets::*;
use crate::lossy_convert::*;
use crate::shared_rate_resource::*;

struct Queue {
    name: String,
    listening_workers: HashSet<Rc<Worker>>,
    deque: VecDeque<Box<dyn FnOnce(BasicArgs) -> Vec<ProposedEvent>>>,
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
        BasicArgs: IntoLossy<Ai>,
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

    fn mk_enqueued_handler<Ao, Ai, R>(
        queue: Rc<RefCell<Queue>>,
        inner_handler: impl FnOnce(Ai) -> R + 'static,
    ) -> impl FnOnce(Ao) -> R
    where
        Ao: HasTimestamp + IntoLossy<Ai>,
        Ai: HasTimestamp + HasWorkerToken,
        R: HasProposedEvents + IntoLossy<Vec<ProposedEvent>> + Default,
        BasicArgs: IntoLossy<Ai>,
    {
        move |args_outer: Ao| {
            queue
                .borrow_mut()
                .enqueued_handler_inner(inner_handler, args_outer.into_lossy())
        }
    }
}

struct Worker {
    id: u64,
    subscribed_queues: Vec<Rc<RefCell<Queue>>>,
    shutting_down: Rc<RefCell<bool>>,
    shared_cpu_resource: Rc<RefCell<SharedRateResource>>,
    rng: Xoshiro256StarStar,
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

struct WorkerToken {
    worker: Worker,
    checkout_timestamp: u64,
    originating_queue_name: String,
}

impl WorkerToken {
    fn mk_token_restoring_handler<A, Ri, Ro>(
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
                    let followon_args = BasicArgs {
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

trait HasWorkerToken {
    fn get_worker_token(&self) -> &Option<WorkerToken>;
    fn get_worker_token_mut(&mut self) -> &mut Option<WorkerToken>;
    fn set_worker_token(&mut self, new_value: Option<WorkerToken>);
}

trait HasWorkerTokensToRestore {
    fn get_worker_tokens_to_restore(&self) -> &Vec<WorkerToken>;
    fn get_worker_tokens_to_restore_mut(&mut self) -> &mut Vec<WorkerToken>;
    fn set_worker_tokens_to_restore(&mut self, new_value: Vec<WorkerToken>);
}

#[derive(Default)]
struct BasicArgs {
    timestamp: u64,
    worker_token: Option<WorkerToken>,
}

#[derive(Default)]
struct BasicReturn {
    proposed_events: Vec<ProposedEvent>,
    worker_tokens_to_restore: Vec<WorkerToken>,
}

impl HasTimestamp for BasicArgs {
    fn get_timestamp(&self) -> &u64 {
        &self.timestamp
    }
    fn set_timestamp(&mut self, new_value: u64) {
        self.timestamp = new_value
    }
}

impl HasWorkerToken for BasicArgs {
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

impl HasProposedEvents for BasicReturn {
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

impl HasWorkerTokensToRestore for BasicReturn {
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

impl FromLossy<u64> for BasicArgs {
    fn from_lossy(other: u64) -> BasicArgs {
        BasicArgs {
            timestamp: other,
            ..Default::default()
        }
    }
}

impl FromLossy<Vec<ProposedEvent>> for BasicReturn {
    fn from_lossy(other: Vec<ProposedEvent>) -> BasicReturn {
        BasicReturn {
            proposed_events: other,
            ..Default::default()
        }
    }
}

struct AutoscalerWorker {
    shutting_down: Rc<RefCell<bool>>,
}

struct AutoscalerSharedRateResource {
    shutting_down: Rc<RefCell<bool>>,
    workers: HashMap<u64, AutoscalerWorker>,
}

struct Autoscaler {
    name: String,
    shared_rate_resources: HashMap<u64, AutoscalerSharedRateResource>,
    //  metrics: ...
}

struct ScheduledEvent {
    due_time: u64,
    handler: Box<dyn FnOnce(u64) -> Vec<ProposedEvent>>,
}

impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.due_time.cmp(&other.due_time).reverse()
    }
}

impl PartialOrd for ScheduledEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ScheduledEvent {
    fn eq(&self, other: &Self) -> bool {
        self.due_time == other.due_time
    }
}

impl Eq for ScheduledEvent {}

fn main() {
    let mut event_heap: BinaryHeap<ScheduledEvent> = Default::default();
    let mut rng = Xoshiro256StarStar::seed_from_u64(0);

    // ...
    while !event_heap.is_empty() {
        let mut simultaneous_events: Vec<ScheduledEvent> = Default::default();
        while let Some(event) = event_heap.peek() {
            if let Some(ScheduledEvent {
                due_time: existing_time,
                ..
            }) = simultaneous_events.first()
            {
                if *existing_time == event.due_time {
                    break;
                }
            }
            simultaneous_events.push(event_heap.pop().unwrap());
        }

        SliceRandom::shuffle(&mut simultaneous_events[..], &mut rng);
        let mut proposed_events: Vec<ProposedEvent> = simultaneous_events
            .drain(..)
            .flat_map(|event| (event.handler)(event.due_time))
            .collect();

        // TODO more efficient bulk implementation
        for proposed_event in proposed_events.drain(..) {
            event_heap.push(ScheduledEvent {
                due_time: max(1, proposed_event.due_time.sample(&mut rng) as u64),
                handler: proposed_event.handler,
            });
        }
    }
}

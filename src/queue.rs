use std::cell::RefCell;
use std::cmp::{max, Eq, Ordering, PartialEq};
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use prometheus_client::metrics::counter::{Atomic, Counter};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;

use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, LogNormal};
use rand_xoshiro::Xoshiro256StarStar;

use crate::args_rets::*;
use crate::lossy_convert::*;
use crate::simulation::*;
use crate::status::*;

pub trait QueueSimulation: Simulation {
    type WorkerExtension: Default;

    fn get_worker_tokens_checked_out_metric(&self) -> &Family<Vec<(String, String)>, Counter>;
    fn get_worker_token_duration_metric(&self) -> &Family<Vec<(String, String)>, Histogram>;
    fn get_up_metric(&self) -> &Family<Vec<(String, String)>, Gauge>;
}

pub struct Queue<S: QueueSimulation + 'static> {
    name: String,
    listening_workers: HashSet<Rc<Worker<S>>>,
    deque: VecDeque<Box<dyn FnOnce(&'static S, u64, WorkerToken<S>) -> Vec<ProposedEvent<S>>>>,
    rng: Xoshiro256StarStar,
    metric_labels: Vec<(String, String)>,
}

impl<S: QueueSimulation + 'static> Queue<S> {
    fn pick_worker(&mut self, simulation: &'static S) -> Option<Worker<S>> {
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
                chosen_worker.shutdown(simulation);
            } else {
                return Some(chosen_worker);
            }
        }

        None
    }

    fn enqueued_handler_inner(
        &mut self,
        inner_handler: impl FnOnce(&'static S, u64, WorkerToken<S>) -> Vec<ProposedEvent<S>> + 'static,
        simulation: &'static S,
        timestamp: u64,
    ) -> Vec<ProposedEvent<S>> {
        if self.deque.is_empty() {
            if let Some(worker) = self.pick_worker(simulation) {
                let mut token = WorkerToken {
                    metric_labels: worker.metric_labels.clone(),
                    worker: worker,
                    checkout_timestamp: timestamp,
                    originating_queue_name: self.name.clone(),
                };
                token
                    .metric_labels
                    .push(("originating_queue".to_owned(), self.name.clone()));

                simulation
                    .get_worker_tokens_checked_out_metric()
                    .get_or_create(&token.metric_labels)
                    .inc();

                return inner_handler(simulation, timestamp, token);
            }
        }

        self.deque.push_back(Box::new(inner_handler));

        Default::default()
    }

    pub fn mk_enqueued_handler(
        queue: Rc<RefCell<Queue<S>>>,
        inner_handler: impl FnOnce(&'static S, u64, WorkerToken<S>) -> Vec<ProposedEvent<S>> + 'static,
    ) -> impl FnOnce(&'static S, u64) -> Vec<ProposedEvent<S>> {
        move |simulation, timestamp| {
            queue
                .borrow_mut()
                .enqueued_handler_inner(inner_handler, simulation, timestamp)
        }
    }
}

pub struct Worker<S: QueueSimulation + 'static> {
    pub id: u64,
    pub subscribed_queues: Vec<Rc<RefCell<Queue<S>>>>,
    pub status: Rc<RefCell<Status>>,
    pub allow_drop: bool,
    pub rng: Xoshiro256StarStar,
    pub metric_labels: Vec<(String, String)>,
    pub ext: S::WorkerExtension,
}

impl<S: QueueSimulation + 'static> Hash for Worker<S> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<S: QueueSimulation + 'static> PartialEq for Worker<S> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<S: QueueSimulation + 'static> Eq for Worker<S> {}

impl<S: QueueSimulation + 'static> Drop for Worker<S> {
    fn drop(&mut self) {
        if !self.allow_drop {
            panic!("Worker was dropped without proper shutdown");
        }
    }
}

impl<S: QueueSimulation + 'static> Worker<S> {
    pub fn shutdown(mut self, simulation: &'static S) {
        simulation
            .get_up_metric()
            .get_or_create(&self.metric_labels)
            .set(0);
        self.allow_drop = true;
        // should now drop as method took ownership
    }

    pub fn listen(mut self, simulation: &'static S, timestamp: u64) -> Vec<ProposedEvent<S>> {
        simulation
            .get_up_metric()
            .get_or_create(&self.metric_labels)
            .set(1);

        if *self.status.borrow() != Status::Running {
            self.shutdown(simulation);
            return Default::default();
        }

        let nonempty_queues = Vec::from_iter(
            self.subscribed_queues
                .iter()
                .filter(|q| !q.borrow().deque.is_empty()),
        );
        if nonempty_queues.is_empty() {
            // return worker to all subscribed queues
            let worker_rc = Rc::new(self);
            for queue in &worker_rc.subscribed_queues {
                queue
                    .borrow_mut()
                    .listening_workers
                    .insert(worker_rc.clone());
            }
            return Default::default();
        }

        // else this worker picks up a new handler from a nonempty queue

        // choose a nonempty queue
        let chosen_queue = SliceRandom::choose(&nonempty_queues[..], &mut self.rng).unwrap();
        let chosen_queue_name = chosen_queue.borrow().name.clone();
        let followon_handler = chosen_queue.borrow_mut().deque.pop_front().unwrap();
        let mut followon_token = WorkerToken {
            metric_labels: self.metric_labels.clone(),
            originating_queue_name: chosen_queue_name.clone(),
            worker: self,
            checkout_timestamp: timestamp,
        };
        followon_token
            .metric_labels
            .push(("originating_queue".to_owned(), chosen_queue_name));

        // tally metric
        simulation
            .get_worker_tokens_checked_out_metric()
            .get_or_create(&followon_token.metric_labels)
            .inc();

        // call follow-on handler
        followon_handler(simulation, timestamp, followon_token)
    }
}

pub struct WorkerToken<S: QueueSimulation + 'static> {
    worker: Worker<S>,
    checkout_timestamp: u64,
    originating_queue_name: String,
    metric_labels: Vec<(String, String)>,
}

impl<S: QueueSimulation + 'static> WorkerToken<S> {
    pub fn mk_token_restoring_handler(
        inner_handler: impl FnOnce(&'static S, u64) -> (Vec<ProposedEvent<S>>, Vec<WorkerToken<S>>),
    ) -> impl FnOnce(&'static S, u64) -> Vec<ProposedEvent<S>> {
        |simulation, timestamp| {
            // call inner handler
            let (mut proposed_events, mut tokens_to_restore) = inner_handler(simulation, timestamp);

            // proposed events that follow-on handlers may produce
            let mut followon_proposed_events = Vec::new();

            // handle restored tokens
            for mut token in tokens_to_restore.drain(..) {
                assert!(
                    token.checkout_timestamp < timestamp,
                    "Cannot restore WorkerToken until after time period it was checked out",
                );

                simulation
                    .get_worker_token_duration_metric()
                    .get_or_create(&token.metric_labels)
                    .observe((timestamp - token.checkout_timestamp) as f64 / S::TICKS_PER_SECOND);

                followon_proposed_events.append(&mut token.worker.listen(simulation, timestamp));
            }

            // combine proposed events from follow-ons into our ret
            proposed_events.append(&mut followon_proposed_events);

            return proposed_events;
        }
    }
}

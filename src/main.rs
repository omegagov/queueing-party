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
pub mod queue;
pub mod shared_rate_resource;

use crate::args_rets::*;
use crate::lossy_convert::*;
use crate::queue::*;
use crate::shared_rate_resource::*;

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

use std::cmp::{max, Eq, Ordering, PartialEq};
use std::collections::BinaryHeap;

use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, LogNormal};
use rand_xoshiro::Xoshiro256StarStar;

use crate::args_rets::*;
use crate::simulation::*;

struct ScheduledEvent<S: Simulation + 'static> {
    due_time: u64,
    handler: Box<dyn FnOnce(&'static S, u64) -> Vec<ProposedEvent<S>>>,
}

impl<S: Simulation + 'static> Ord for ScheduledEvent<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.due_time.cmp(&other.due_time).reverse()
    }
}

impl<S: Simulation + 'static> PartialOrd for ScheduledEvent<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: Simulation + 'static> PartialEq for ScheduledEvent<S> {
    fn eq(&self, other: &Self) -> bool {
        self.due_time == other.due_time
    }
}

impl<S: Simulation + 'static> Eq for ScheduledEvent<S> {}

pub fn main_loop<S: Simulation + 'static>(
    simulation: &'static S,
    initial_handler: Box<dyn FnOnce(&'static S, u64) -> Vec<ProposedEvent<S>>>,
) {
    let mut event_heap: BinaryHeap<ScheduledEvent<S>> = Default::default();
    let mut simevent_rng = simulation.borrow_rng_mut().clone();
    let mut schedule_rng = simulation.borrow_rng_mut().clone();

    event_heap.push(ScheduledEvent::<S> {
        due_time: 0,
        handler: initial_handler,
    });

    while !event_heap.is_empty() {
        let mut simultaneous_events: Vec<ScheduledEvent<S>> = Default::default();
        while let Some(event) = event_heap.peek() {
            if let Some(ScheduledEvent::<S> {
                due_time: existing_time,
                ..
            }) = simultaneous_events.first()
            {
                if *existing_time != event.due_time {
                    break;
                }
            }
            simultaneous_events.push(event_heap.pop().unwrap());
        }

        let current_timestamp = simultaneous_events.first().unwrap().due_time;

        #[cfg(debug_assertions)]
        std::eprintln!("current_timestamp = {current_timestamp}");

        SliceRandom::shuffle(&mut simultaneous_events[..], &mut simevent_rng);
        let mut proposed_events: Vec<ProposedEvent<S>> = simultaneous_events
            .drain(..)
            .flat_map(|event| {
                let r = (event.handler)(simulation, event.due_time);
                simulation.get_events_dispatched_metric().inc();
                r
            })
            .collect();

        // TODO more efficient bulk implementation
        for proposed_event in proposed_events.drain(..) {
            event_heap.push(ScheduledEvent::<S> {
                due_time: current_timestamp
                    + max(1, proposed_event.due_time.sample(&mut schedule_rng) as u64),
                handler: proposed_event.handler,
            });
        }
    }
}

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
use crate::simulation::*;
use crate::status::*;

pub trait SRRSimulation: Simulation {}

struct SharedRateTenancy<S: SRRSimulation + 'static> {
    due_timer_time: u64,
    handler: Box<dyn FnOnce(&'static S, u64) -> Vec<ProposedEvent<S>>>,
}

impl<S: SRRSimulation + 'static> Ord for SharedRateTenancy<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.due_timer_time.cmp(&other.due_timer_time).reverse()
    }
}

impl<S: SRRSimulation + 'static> PartialOrd for SharedRateTenancy<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: SRRSimulation + 'static> PartialEq for SharedRateTenancy<S> {
    fn eq(&self, other: &Self) -> bool {
        self.due_timer_time == other.due_timer_time
    }
}

impl<S: SRRSimulation + 'static> Eq for SharedRateTenancy<S> {}

pub struct SharedRateResource<S: SRRSimulation + 'static> {
    id: u64,
    partitions: u8,
    resource_timer: u64,
    resource_timer_last_updated_real_time: u64,
    utilization_counter: u64,
    load_counter: u64,
    wakeup_event_memo: VecDeque<u64>,
    status: Rc<RefCell<Status>>,
    tenancies: BinaryHeap<SharedRateTenancy<S>>,
    rng: Xoshiro256StarStar,
}

impl<S: SRRSimulation + 'static> SharedRateResource<S> {
    const MAX_WAKEUP_EVENT_MEMO_LEN: u8 = 8;
    const MIN_RESOURCE_TIMER_RESET_VAL: u64 = (S::TICKS_PER_SECOND * 120.0) as u64;

    fn update_resource_timer(&mut self, current_timestamp: u64) {
        assert!(self.resource_timer_last_updated_real_time <= current_timestamp);

        if self.tenancies.is_empty() {
            // don't reset before we've had a good chance to be observed by metrics
            if self.resource_timer >= Self::MIN_RESOURCE_TIMER_RESET_VAL {
                self.resource_timer = 0;
                self.utilization_counter = 0;
                self.load_counter = 0;

                // no need to worry about adjusting due_timer_time values
                // because there aren't any
            }
        } else if self.resource_timer_last_updated_real_time != current_timestamp {
            let real_time_delta = current_timestamp - self.resource_timer_last_updated_real_time;
            let increment =
                real_time_delta as f64 * self.get_current_resource_timer_rate().unwrap();

            self.resource_timer += increment as u64;

            // should be hard for us to go past our target because float-int
            // conversion rounds towards zero
            assert!(self.resource_timer < self.tenancies.peek().unwrap().due_timer_time);

            self.utilization_counter +=
                u64::min(self.partitions as u64, self.tenancies.len() as u64) * real_time_delta;
            self.load_counter += self.tenancies.len() as u64 * real_time_delta;
        }

        self.resource_timer_last_updated_real_time = current_timestamp;
    }

    fn get_current_resource_timer_rate(&self) -> Option<f64> {
        if self.tenancies.is_empty() {
            None
        } else {
            Some(f64::min(
                1.0,
                self.partitions as f64 / self.tenancies.len() as f64,
            ))
        }
    }

    fn get_next_wakeup_time(&self) -> Option<u64> {
        if self.tenancies.is_empty() {
            None
        } else {
            Some(
                ((self.tenancies.peek().unwrap().due_timer_time - self.resource_timer) as f64
                    / self.get_current_resource_timer_rate().unwrap()) as u64
                    + self.resource_timer_last_updated_real_time,
            )
        }
    }

    fn add_tenancy(
        &mut self,
        current_timestamp: u64,
        required_resource_time: LogNormal<f32>,
        inner_handler: impl FnOnce(&'static S, u64) -> Vec<ProposedEvent<S>> + 'static,
    ) {
        self.update_resource_timer(current_timestamp);
        let actual_req_resource_time = max(1, required_resource_time.sample(&mut self.rng) as u64);
        self.tenancies.push(SharedRateTenancy {
            due_timer_time: self.resource_timer + actual_req_resource_time,
            handler: Box::new(inner_handler),
        });
    }

    fn maybe_generate_wakeup_event(
        shared_rate_resource: Rc<RefCell<Self>>,
    ) -> Option<Vec<ProposedEvent<S>>> {
        let srr = shared_rate_resource;

        if srr.borrow().tenancies.is_empty() {
            return None;
        }

        let t = srr.borrow().get_next_wakeup_time().unwrap();
        if !srr.borrow().wakeup_event_memo.contains(&t) {
            srr.borrow_mut()
                .wakeup_event_memo
                .truncate(Self::MAX_WAKEUP_EVENT_MEMO_LEN as usize - 1);
            srr.borrow_mut().wakeup_event_memo.push_front(t);

            let srrc = srr.clone();
            return Some(Vec::from([ProposedEvent {
                due_time: LogNormal::from_mean_cv(t as f32, 0.0).unwrap(),
                handler: Box::new(move |simulation, timestamp| {
                    let mut handlers = Vec::new();
                    while let Some(wt) = srrc.borrow().get_next_wakeup_time() {
                        if wt != timestamp {
                            break;
                        }
                        handlers.push(srrc.borrow_mut().tenancies.pop().unwrap());
                    }

                    if !handlers.is_empty() {
                        // this was a false wakeup

                        // resource timer doesn't need to be updated as nothing was added
                        // or removed from the heap. if we can avoid doing this we generally
                        // should because it potentially adds precision error every time it
                        // runs.
                        srrc.borrow_mut().update_resource_timer(timestamp);
                    }

                    SliceRandom::shuffle(&mut handlers[..], &mut srrc.borrow_mut().rng);
                    let mut ret: Vec<ProposedEvent<S>> = handlers
                        .drain(..)
                        .flat_map(|tenancy| (tenancy.handler)(simulation, timestamp))
                        .collect();

                    if let Some(mut mwvec) = Self::maybe_generate_wakeup_event(srrc) {
                        ret.append(&mut mwvec);
                    }
                    return ret;
                }),
            }]));

            // TODO runtime destructor guard to ensure resulting event isn't dropped?
        }

        Some(Default::default())
    }

    pub fn mk_shared_rate_event(
        shared_rate_resource: Rc<RefCell<Self>>,
        current_timestamp: u64,
        required_resource_time: LogNormal<f32>,
        inner_handler: impl FnOnce(&'static S, u64) -> Vec<ProposedEvent<S>> + 'static,
    ) -> Vec<ProposedEvent<S>> {
        shared_rate_resource.borrow_mut().add_tenancy(
            current_timestamp,
            required_resource_time,
            inner_handler,
        );

        Self::maybe_generate_wakeup_event(shared_rate_resource).unwrap()
    }
}

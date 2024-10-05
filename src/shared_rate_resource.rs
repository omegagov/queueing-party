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

struct SharedRateTenancy {
    due_timer_time: u64,
    handler: Box<dyn FnOnce(u64) -> Vec<ProposedEvent>>,
}

impl Ord for SharedRateTenancy {
    fn cmp(&self, other: &Self) -> Ordering {
        self.due_timer_time.cmp(&other.due_timer_time).reverse()
    }
}

impl PartialOrd for SharedRateTenancy {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SharedRateTenancy {
    fn eq(&self, other: &Self) -> bool {
        self.due_timer_time == other.due_timer_time
    }
}

impl Eq for SharedRateTenancy {}

pub struct SharedRateResource {
    id: u64,
    partitions: u8,
    resource_timer: u64,
    resource_timer_last_updated_real_time: u64,
    wakeup_event_memo: VecDeque<u64>,
    shutting_down: Rc<RefCell<bool>>,
    tenancies: BinaryHeap<SharedRateTenancy>,
    rng: Xoshiro256StarStar,
    //  metrics: ...,
}

impl SharedRateResource {
    const MAX_WAKEUP_EVENT_MEMO_LEN: u8 = 8;

    fn update_resource_timer(&mut self, current_timestamp: u64) {
        assert!(self.resource_timer_last_updated_real_time <= current_timestamp);

        if self.tenancies.is_empty() {
            self.resource_timer = 0;
        } else if self.resource_timer_last_updated_real_time != current_timestamp {
            let increment = (current_timestamp - self.resource_timer_last_updated_real_time) as f64
                * self.get_current_resource_timer_rate().unwrap();

            self.resource_timer += increment as u64;

            // should be hard for us to go past our target because float-int
            // conversion rounds towards zero
            assert!(self.resource_timer < self.tenancies.peek().unwrap().due_timer_time);
        }

        self.resource_timer_last_updated_real_time = current_timestamp;

        // TODO accumulate timer time for metrics
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

    fn add_tenancy<A, Ri>(
        &mut self,
        current_timestamp: u64,
        required_resource_time: LogNormal<f32>,
        inner_handler: impl FnOnce(A) -> Ri + 'static,
    ) where
        A: HasTimestamp,
        Ri: HasProposedEvents + IntoLossy<Vec<ProposedEvent>>,
        u64: IntoLossy<A>,
    {
        self.update_resource_timer(current_timestamp);
        let actual_req_resource_time = max(1, required_resource_time.sample(&mut self.rng) as u64);
        self.tenancies.push(SharedRateTenancy {
            due_timer_time: self.resource_timer + actual_req_resource_time,
            handler: Box::new(move |timestamp| inner_handler(timestamp.into_lossy()).into_lossy()),
        });
    }

    fn maybe_generate_wakeup_event(
        shared_rate_resource: Rc<RefCell<Self>>,
    ) -> Option<Vec<ProposedEvent>> {
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
                handler: Box::new(move |timestamp| {
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
                    let mut ret: Vec<ProposedEvent> = handlers
                        .drain(..)
                        .flat_map(|tenancy| (tenancy.handler)(timestamp))
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

    pub fn mk_shared_rate_event<A, Ri, Ro>(
        shared_rate_resource: Rc<RefCell<SharedRateResource>>,
        current_timestamp: u64,
        required_resource_time: LogNormal<f32>,
        inner_handler: impl FnOnce(A) -> Ri + 'static,
    ) -> Ro
    where
        A: HasTimestamp,
        Ri: HasProposedEvents + IntoLossy<Vec<ProposedEvent>>,
        Ro: HasProposedEvents + Default,
        u64: IntoLossy<A>,
    {
        shared_rate_resource.borrow_mut().add_tenancy(
            current_timestamp,
            required_resource_time,
            inner_handler,
        );

        let mut ret: Ro = Default::default();
        ret.set_proposed_events(
            SharedRateResource::maybe_generate_wakeup_event(shared_rate_resource).unwrap(),
        );
        ret
    }
}

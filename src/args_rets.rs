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

pub trait HasTimestamp {
    fn get_timestamp(&self) -> &u64;
    fn set_timestamp(&mut self, new_value: u64);
}

impl HasTimestamp for u64 {
    fn get_timestamp(&self) -> &u64 {
        self
    }
    fn set_timestamp(&mut self, new_value: u64) {
        *self = new_value
    }
}

pub struct ProposedEvent {
    pub due_time: LogNormal<f32>,
    pub handler: Box<dyn FnOnce(u64) -> Vec<ProposedEvent>>,
}

pub trait HasProposedEvents {
    fn get_proposed_events(&self) -> &Vec<ProposedEvent>;
    fn get_proposed_events_mut(&mut self) -> &mut Vec<ProposedEvent>;
    fn set_proposed_events(&mut self, new_value: Vec<ProposedEvent>);
}

impl HasProposedEvents for Vec<ProposedEvent> {
    fn get_proposed_events(&self) -> &Vec<ProposedEvent> {
        self
    }
    fn get_proposed_events_mut(&mut self) -> &mut Vec<ProposedEvent> {
        self
    }
    fn set_proposed_events(&mut self, new_value: Vec<ProposedEvent>) {
        *self = new_value
    }
}

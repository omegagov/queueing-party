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

use crate::simulation::*;

pub struct ProposedEvent<S: Simulation + 'static> {
    pub due_time: LogNormal<f32>,
    pub handler: Box<dyn FnOnce(&'static S, u64) -> Vec<Self>>,
}

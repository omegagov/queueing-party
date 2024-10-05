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

pub trait FromLossy<T> {
    fn from_lossy(value: T) -> Self;
}

pub trait IntoLossy<T> {
    fn into_lossy(self) -> T;
}

// reflexive
impl<T> FromLossy<T> for T {
    fn from_lossy(t: T) -> T {
        t
    }
}

// blanket
impl<T, U> IntoLossy<U> for T
where
    U: FromLossy<T>,
{
    fn into_lossy(self) -> U {
        U::from_lossy(self)
    }
}

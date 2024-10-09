use std::cell::{Ref, RefCell, RefMut};
use std::rc::Rc;

use prometheus_client::metrics::counter::{Atomic, Counter};
use prometheus_client::registry::Registry;

use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro256StarStar;

pub trait Simulation {
    const TICKS_PER_SECOND: f64;
    const METRICS_SAMPLING_PERIOD_SECONDS: f64;

    fn get_id(&self) -> u64;

    fn get_events_dispatched_metric(&self) -> &Counter;

    fn borrow_metric_registry(&self) -> Ref<'_, Registry>;
    fn borrow_metric_registry_mut(&self) -> RefMut<'_, Registry>;

    fn borrow_rng_mut(&self) -> RefMut<'_, Xoshiro256StarStar>;

    // TODO events in heap collector
}

pub struct BaseSimulation {
    id: u64,
    events_dispatched_metric: Counter,
    metric_registry: RefCell<Registry>,
    rng: RefCell<Xoshiro256StarStar>,
}

impl BaseSimulation {
    pub fn new(id: u64, metric_registry: Registry) -> Self {
        let r = BaseSimulation {
            id: id,
            events_dispatched_metric: Default::default(),
            metric_registry: metric_registry.into(),
            rng: Xoshiro256StarStar::seed_from_u64(id).into(),
        };
        r.borrow_metric_registry_mut().register(
            "events_dispatched",
            "Number of events dispatched in simulation",
            r.events_dispatched_metric.clone(),
        );

        r
    }
}

impl Simulation for BaseSimulation {
    const TICKS_PER_SECOND: f64 = 1000.0;
    const METRICS_SAMPLING_PERIOD_SECONDS: f64 = 15.0;

    fn get_id(&self) -> u64 {
        self.id
    }

    fn get_events_dispatched_metric(&self) -> &Counter {
        &self.events_dispatched_metric
    }

    fn borrow_metric_registry(&self) -> Ref<'_, Registry> {
        self.metric_registry.borrow()
    }

    fn borrow_metric_registry_mut(&self) -> RefMut<'_, Registry> {
        self.metric_registry.borrow_mut()
    }

    fn borrow_rng_mut(&self) -> RefMut<'_, Xoshiro256StarStar> {
        self.rng.borrow_mut()
    }
}

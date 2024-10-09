use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use rand_distr::{Distribution, LogNormal};

pub mod args_rets;
pub mod lossy_convert;
pub mod main_loop;
pub mod pool_manager;
pub mod queue;
pub mod shared_rate_resource;
pub mod simulation;
pub mod status;

use crate::args_rets::*;
use crate::lossy_convert::*;
use crate::main_loop::*;
use crate::pool_manager::*;
use crate::queue::*;
use crate::shared_rate_resource::*;
use crate::simulation::*;
use crate::status::*;

use std::io::stdout;
use prometheus_client::encoding::text::encode_registry;
use prometheus_client::registry::Registry;

fn metric_collection_event<S: Simulation + 'static>(
    simulation: &'static S,
    timestamp: u64,
) -> Vec<ProposedEvent<S>> {
    let mut outstr: String = Default::default();

    encode_registry(
        &mut outstr,
        &simulation.borrow_metric_registry(),
    ).unwrap();

    std::io::Write::write_all(&mut stdout(), outstr.as_bytes()).unwrap();

    vec![ProposedEvent {
        due_time: LogNormal::from_mean_cv(
            (S::METRICS_SAMPLING_PERIOD_SECONDS * S::TICKS_PER_SECOND) as f32,
            0.0,
        ).unwrap(),
        handler: Box::new(metric_collection_event::<S>),
    }]
}

fn bootstrap<S: Simulation + 'static>(
    simulation: &'static S,
    timestamp: u64,
) -> Vec<ProposedEvent<S>> {
    vec![ProposedEvent {
        due_time: LogNormal::from_mean_cv(
            1.0,
            0.0,
        ).unwrap(),
        handler: Box::new(metric_collection_event::<S>),
    }]
}

fn main() {
    let id: u64 = 1234;
    let mut simulation = Box::new(BaseSimulation::new(
        id,
        Registry::with_labels(vec![
            (
                Cow::from("simulation_id"),
                Cow::from(&*Box::leak(format!("{id:016x}").into_boxed_str())),
            )
        ].into_iter()),
    ));

    main_loop(Box::leak(simulation), Box::new(bootstrap));
}

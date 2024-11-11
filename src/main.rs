use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use rand::Rng;
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

use prometheus_client::encoding::text::encode_registry;
use prometheus_client::registry::Registry;
use std::io::stdout;

fn metric_collection_handler<S: Simulation + 'static>(
    simulation: &'static S,
    timestamp: u64,
) -> Vec<ProposedEvent<S>> {
    let mut outstr: String = Default::default();

    encode_registry(&mut outstr, &simulation.borrow_metric_registry()).unwrap();

    std::io::Write::write_all(&mut stdout(), outstr.as_bytes()).unwrap();

    vec![ProposedEvent {
        due_time: LogNormal::from_mean_cv(
            (S::METRICS_SAMPLING_PERIOD_SECONDS * S::TICKS_PER_SECOND) as f32,
            0.0,
        )
        .unwrap(),
        handler: Box::new(metric_collection_handler::<S>),
    }]
}

fn mk_dummy_autoscaler_handler<S: Simulation + 'static>(
    pool_manager: Rc<RefCell<PoolManager>>,
) -> impl FnOnce(&'static S, u64) -> Vec<ProposedEvent<S>> {
    move |_, _| {
        vec![ProposedEvent {
            due_time: LogNormal::from_mean_cv((60.0 * S::TICKS_PER_SECOND) as f32, 0.0).unwrap(),
            handler: Box::new(mk_dummy_autoscaler_handler::<S>(pool_manager)),
        }]
    }
}

fn mk_foo_handler<S: QueueSimulation + 'static>(
    queue: Rc<RefCell<Queue<S>>>,
) -> impl FnOnce(&'static S, u64) -> Vec<ProposedEvent<S>> {
    move |_, _| {
        vec![ProposedEvent {
            due_time: LogNormal::from_mean_cv((0.1 * S::TICKS_PER_SECOND) as f32, 1.0).unwrap(),
            handler: Box::new(Queue::mk_enqueued_handler(
                queue,
                |simulation, timestamp, worker_token| {
                    std::eprintln!("checked out @ {timestamp}");
                    vec![ProposedEvent {
                        due_time: LogNormal::from_mean_cv((4.0 * S::TICKS_PER_SECOND) as f32, 1.0)
                            .unwrap(),
                        handler: Box::new(WorkerToken::mk_token_restoring_handler(
                            move |simulation, timestamp| {
                                std::eprintln!("restoring @ {timestamp}");
                                (Default::default(), vec![worker_token])
                            },
                        )),
                    }]
                },
            )),
        }]
    }
}

fn bootstrap<S: QueueSimulation + 'static>(
    simulation: &'static S,
    timestamp: u64,
) -> Vec<ProposedEvent<S>> {
    let queue_foo = Rc::new(RefCell::new(Queue::<S> {
        name: "foo".into(),
        listening_workers: Default::default(),
        deque: Default::default(),
        rng: simulation.borrow_rng_mut().clone(),
        metric_labels: vec![("queue_name".into(), "foo".into())],
    }));

    let manager_foo = Rc::new(RefCell::new(PoolManager {
        name: "foo".into(),
        instance_constructor: {
            let queue_foo_clone = queue_foo.clone();

            Box::new(move || {
                let mut worker = {
                    let mut rng = &mut queue_foo_clone.borrow_mut().rng;
                    let id: u64 = rng.gen();
                    Worker {
                        id,
                        status: Rc::new(Status::Running.into()),
                        allow_drop: false,
                        metric_labels: vec![("worker_id".into(), format!("{id:016x}").into())],
                        rng: rng.clone(),
                        ext: Default::default(),
                        subscribed_queues: vec![queue_foo_clone.clone()].into(),
                    }
                };
                let status_clone = worker.status.clone();

                worker.listen(simulation, timestamp);

                Box::new(move || {
                    *status_clone.borrow_mut() = Status::ShuttingDown;
                })
            })
        },
        instances: Default::default(),
        metric_labels: vec![("pool_manager_name".into(), "foo".into())],
    }));
    manager_foo.borrow_mut().set_desired_instances_absolute(2);

    vec![
        ProposedEvent {
            due_time: LogNormal::from_mean_cv(1.0, 0.0).unwrap(),
            handler: Box::new(mk_dummy_autoscaler_handler::<S>(manager_foo)),
        },
        ProposedEvent {
            due_time: LogNormal::from_mean_cv(1.0, 0.0).unwrap(),
            handler: Box::new(metric_collection_handler::<S>),
        },
        ProposedEvent {
            due_time: LogNormal::from_mean_cv((240.0 * S::TICKS_PER_SECOND) as f32, 0.0).unwrap(),
            handler: Box::new(|_, _| {
                std::process::exit(0);
                Default::default()
            }),
        },
        ProposedEvent {
            due_time: LogNormal::from_mean_cv((40.0 * S::TICKS_PER_SECOND) as f32, 0.01).unwrap(),
            handler: Box::new(mk_foo_handler::<S>(queue_foo.clone())),
        },
        ProposedEvent {
            due_time: LogNormal::from_mean_cv((40.1 * S::TICKS_PER_SECOND) as f32, 0.01).unwrap(),
            handler: Box::new(mk_foo_handler::<S>(queue_foo.clone())),
        },
        ProposedEvent {
            due_time: LogNormal::from_mean_cv((40.1 * S::TICKS_PER_SECOND) as f32, 0.01).unwrap(),
            handler: Box::new(mk_foo_handler::<S>(queue_foo.clone())),
        },
    ]
}

fn main() {
    let id: u64 = 1236;
    let mut simulation = Box::new(BaseQueueSimulation::new(
        id,
        Registry::with_labels(
            vec![(
                Cow::from("simulation_id"),
                Cow::from(&*Box::leak(format!("{id:016x}").into_boxed_str())),
            )]
            .into_iter(),
        ),
    ));

    main_loop(Box::leak(simulation), Box::new(bootstrap));
}

use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::collections::HashSet;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::rc::Rc;
use std::hash::{Hash, Hasher};
use std::cmp::{Eq, PartialEq, Ordering, max};

use rand::Rng;
use rand::seq::SliceRandom;
use rand_distr::{Distribution, LogNormal};
use rand_xoshiro::Xoshiro256StarStar;

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

struct SharedRateResource {
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
            let increment = (
                current_timestamp - self.resource_timer_last_updated_real_time
            ) as f64 * self.get_current_resource_timer_rate().unwrap();

            self.resource_timer += increment as u64;

            // should be hard for us to go past our target because float-int
            // conversion rounds towards zero
            assert!(self.resource_timer < self.tenancies.peek().unwrap().due_timer_time);
        }

        self.resource_timer_last_updated_real_time = current_timestamp;
    }

    fn get_current_resource_timer_rate(&self) -> Option<f64> {
        if self.tenancies.is_empty() {
            None
        } else {
            Some(f64::min(1.0, self.partitions as f64 / self.tenancies.len() as f64))
        }
    }

    fn get_next_wakeup_time(&self) -> Option<u64> {
        if self.tenancies.is_empty() {
            None
        } else {
            Some((
                (self.tenancies.peek().unwrap().due_timer_time - self.resource_timer) as f64
                / self.get_current_resource_timer_rate().unwrap()
            ) as u64 + self.resource_timer_last_updated_real_time)
        }
    }

    fn maybe_generate_wakeup_event(shared_rate_resource: Rc<RefCell<Self>>) -> Option<Vec<ProposedEvent>> {
        let srr = shared_rate_resource;

        if srr.borrow().tenancies.is_empty() {
            return None;
        }

        let t = srr.borrow().get_next_wakeup_time().unwrap();
        if !srr.borrow().wakeup_event_memo.contains(&t) {
            srr.borrow_mut().wakeup_event_memo.truncate(Self::MAX_WAKEUP_EVENT_MEMO_LEN as usize - 1);
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

                    SliceRandom::shuffle(
                        &mut handlers[..],
                        &mut srrc.borrow_mut().rng,
                    );
                    let mut ret: Vec<ProposedEvent> = handlers.drain(..).flat_map(|tenancy| {
                        (tenancy.handler)(timestamp)
                    }).collect();

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
}

struct ProposedEvent {
    due_time: LogNormal<f32>,
    handler: Box<dyn FnOnce(u64) -> Vec<ProposedEvent>>,
}

trait HasTimestamp {
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

trait HasProposedEvents {
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

struct Queue {
    name: String,
    listening_workers: HashSet<Rc<Worker>>,
    deque: VecDeque<Box<dyn FnOnce(BasicArgs) -> Vec<ProposedEvent>>>,
    rng: Xoshiro256StarStar,
    //  metrics: ...,
}

struct Worker {
    id: u64,
    subscribed_queues: Vec<Rc<RefCell<Queue>>>,
    shutting_down: Rc<RefCell<bool>>,
    shared_cpu_resource: Rc<RefCell<SharedRateResource>>,
    rng: Xoshiro256StarStar,
    //  metrics: ...,
}

impl Hash for Worker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for Worker {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Worker {}

struct WorkerToken {
    worker: Worker,
    checkout_timestamp: u64,
    originating_queue: Rc<RefCell<Queue>>,
}

trait HasWorkerToken {
    fn get_worker_token(&self) -> &Option<WorkerToken>;
    fn get_worker_token_mut(&mut self) -> &mut Option<WorkerToken>;
    fn set_worker_token(&mut self, new_value: Option<WorkerToken>);
}

trait HasWorkerTokensToRestore {
    fn get_worker_tokens_to_restore(&self) -> &Vec<WorkerToken>;
    fn get_worker_tokens_to_restore_mut(&mut self) -> &mut Vec<WorkerToken>;
    fn set_worker_tokens_to_restore(&mut self, new_value: Vec<WorkerToken>);
}

#[derive(Default)]
struct BasicArgs {
    timestamp: u64,
    worker_token: Option<WorkerToken>,
    proposed_events: Vec<ProposedEvent>,
    worker_tokens_to_restore: Vec<WorkerToken>,
}

impl HasTimestamp for BasicArgs {
    fn get_timestamp(&self) -> &u64 {
        &self.timestamp
    }
    fn set_timestamp(&mut self, new_value: u64) {
        self.timestamp = new_value
    }
}

impl HasWorkerToken for BasicArgs {
    fn get_worker_token(&self) -> &Option<WorkerToken> {
        &self.worker_token
    }
    fn get_worker_token_mut(&mut self) -> &mut Option<WorkerToken> {
        &mut self.worker_token
    }
    fn set_worker_token(&mut self, new_value: Option<WorkerToken>) {
        self.worker_token = new_value
    }
}

impl HasProposedEvents for BasicArgs {
    fn get_proposed_events(&self) -> &Vec<ProposedEvent> {
        &self.proposed_events
    }
    fn get_proposed_events_mut(&mut self) -> &mut Vec<ProposedEvent> {
        &mut self.proposed_events
    }
    fn set_proposed_events(&mut self, new_value: Vec<ProposedEvent>) {
        self.proposed_events = new_value
    }
}

impl HasWorkerTokensToRestore for BasicArgs {
    fn get_worker_tokens_to_restore(&self) -> &Vec<WorkerToken> {
        &self.worker_tokens_to_restore
    }
    fn get_worker_tokens_to_restore_mut(&mut self) -> &mut Vec<WorkerToken> {
        &mut self.worker_tokens_to_restore
    }
    fn set_worker_tokens_to_restore(&mut self, new_value: Vec<WorkerToken>) {
        self.worker_tokens_to_restore = new_value
    }
}

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

impl FromLossy<u64> for BasicArgs {
    fn from_lossy(other: u64) -> BasicArgs {
        BasicArgs {
            timestamp: other,
            ..Default::default()
        }
    }
}

impl FromLossy<Vec<ProposedEvent>> for BasicArgs {
    fn from_lossy(other: Vec<ProposedEvent>) -> BasicArgs {
        BasicArgs {
            proposed_events: other,
            ..Default::default()
        }
    }
}

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

fn mk_enqueued_handler<Ao, Ai, R>(
    queue: Rc<RefCell<Queue>>,
    inner_handler: impl FnOnce(Ai) -> R + 'static,
) -> impl FnOnce(Ao) -> R
where
    Ao: HasTimestamp + IntoLossy<Ai>,
    Ai: HasTimestamp + HasWorkerToken,
    R: HasProposedEvents + IntoLossy<Vec<ProposedEvent>> + Default,
    BasicArgs: IntoLossy<Ai>,
{
    move |args_outer: Ao| {
        if queue.borrow().deque.is_empty() && !queue.borrow().listening_workers.is_empty() {
            let chosen_worker_rc = Clone::clone(queue.borrow_mut().listening_workers.iter().nth(
                queue.borrow_mut().rng.gen_range(0..queue.borrow().listening_workers.len())
            ).unwrap());

            queue.borrow_mut().listening_workers.remove(&chosen_worker_rc);

            for other_queue in &chosen_worker_rc.subscribed_queues {
                other_queue.borrow_mut().listening_workers.remove(&chosen_worker_rc);
            }

            let mut args_inner: Ai = args_outer.into_lossy();
            args_inner.set_worker_token(Some(WorkerToken {
                worker: Rc::into_inner(chosen_worker_rc).unwrap(),
                checkout_timestamp: *args_inner.get_timestamp(),
                originating_queue: queue,
            }));

            // TODO tally metric

            return inner_handler(args_inner);
        } else {
            queue.borrow_mut().deque.push_back(Box::new(move |args| {
                inner_handler(args.into_lossy()).into_lossy()
            }));
            return Default::default();
        }
    }
}

impl WorkerToken {
    fn mk_token_restoring_handler<A, Ri, Ro>(
        inner_handler: impl FnOnce(A) -> Ri,
    ) -> impl FnOnce(A) -> Ro
    where
        A: HasTimestamp,
        Ri: HasProposedEvents + HasWorkerTokensToRestore + IntoLossy<Ro>,
        Ro: HasProposedEvents,
    {
        |args: A| {
            let timestamp = *args.get_timestamp();
            // call inner handler
            let mut ret = inner_handler(args);

            // proposed events that follow-on handlers may produce
            let mut followon_proposed_events = Vec::new();

            // handle restored tokens
            for mut token in ret.get_worker_tokens_to_restore_mut().drain(..) {
                assert!(
                    token.checkout_timestamp < timestamp,
                    "Cannot restore WorkerToken until after time period it was checked out",
                );

                let nonempty_queues = Vec::from_iter(
                    token.worker.subscribed_queues.iter().filter(|q| !q.borrow().deque.is_empty())
                );
                if nonempty_queues.is_empty() {
                    // return worker to all subscribed queues
                    let worker_rc = Rc::new(token.worker);
                    for queue in &worker_rc.subscribed_queues {
                        queue.borrow_mut().listening_workers.insert(worker_rc.clone());
                    }
                } else {
                    // this worker picks up a new handler from a nonempty queue

                    // choose a nonempty queue
                    let chosen_queue = SliceRandom::choose(
                        &nonempty_queues[..],
                        &mut token.worker.rng,
                    ).unwrap();
                    let followon_handler = chosen_queue.borrow_mut().deque.pop_front().unwrap();

                    // prepare args
                    let followon_args = BasicArgs {
                        timestamp: timestamp,
                        worker_token: Some(WorkerToken {
                            originating_queue: (*chosen_queue).clone(),
                            worker: token.worker,
                            checkout_timestamp: timestamp,
                        }),
                        ..Default::default()
                    };

                    // TODO tally metric

                    // call follow-on handler
                    let mut followon_ret = followon_handler(followon_args);

                    // gather its proposed events
                    followon_proposed_events.append(followon_ret.get_proposed_events_mut());
                }
            }

            // combine proposed events from follow-ons into our ret
            ret.get_proposed_events_mut().append(&mut followon_proposed_events);

            return ret.into_lossy();
        }
    }
}

fn mk_shared_rate_event<A, Ri, Ro>(
    current_timestamp: u64,
    shared_rate_resource: Rc<RefCell<SharedRateResource>>,
    required_resource_time: LogNormal<f32>,
    inner_handler: impl FnOnce(A) -> Ri + 'static,
) -> Ro
where
    A: HasTimestamp,
    Ri: HasProposedEvents + IntoLossy<Vec<ProposedEvent>>,
    Ro: HasProposedEvents + Default,
    u64: IntoLossy<A>,
{
    shared_rate_resource.borrow_mut().update_resource_timer(current_timestamp);
    let actual_req_resource_time = max(
        1,
        required_resource_time.sample(&mut shared_rate_resource.borrow_mut().rng) as u64,
    );
    shared_rate_resource.borrow_mut().tenancies.push(SharedRateTenancy {
        due_timer_time: shared_rate_resource.borrow().resource_timer + actual_req_resource_time,
        handler: Box::new(move |timestamp| {
            inner_handler(timestamp.into_lossy()).into_lossy()
        }),
    });

    let mut ret: Ro = Default::default();
    ret.set_proposed_events(
        SharedRateResource::maybe_generate_wakeup_event(shared_rate_resource).unwrap()
    );
    ret
}

// worker_token.worker.metrics. ...

// ... "add worker" routine?

fn main() {
    println!("Hello, world!");
}

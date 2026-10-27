use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::mem::take;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use async_stream::stream;
use bytes::Bytes;
use edgezero_core::{
    BodyStream, Deadline, DispatchBudget, EdgeError, MonotonicClock, MonotonicInstant,
    OutboundFuture,
};
use futures_util::StreamExt as _;
use futures_util::future::poll_fn;

const READ_BYTES: usize = 16 * 1024;

type WaitFactory = Rc<dyn Fn(Deadline, MonotonicClock) -> OutboundFuture<()>>;
type WaitLimit = Rc<dyn Fn() -> Result<Option<Duration>, EdgeError>>;
std::thread_local! {
    static CURRENT_WAIT: RefCell<Option<WaitFactory>> = const { RefCell::new(None) };
    static CURRENT_DRIVER: RefCell<Option<Rc<DriverContext>>> = const { RefCell::new(None) };
}

struct DriverContext {
    limit: WaitLimit,
    parent: Option<Rc<Self>>,
    repoll: Rc<Cell<bool>>,
    waker: Waker,
}

impl DriverContext {
    fn ancestor_limit(&self) -> Result<Option<Duration>, EdgeError> {
        let mut remaining = None;
        let mut ancestor = self.parent.as_ref();
        while let Some(parent) = ancestor {
            if let Some(limit) = (parent.limit)()? {
                remaining = Some(remaining.map_or(limit, |prior: Duration| prior.min(limit)));
            }
            ancestor = parent.parent.as_ref();
        }
        Ok(remaining)
    }

    fn repoll_parent(&self) {
        if let Some(parent) = &self.parent {
            self.repoll.set(true);
            parent.waker.wake_by_ref();
        }
    }
}

struct Timer {
    clock: MonotonicClock,
    deadline: Deadline,
    outcome: Option<Result<(), EdgeError>>,
    previous: MonotonicInstant,
    waker: Option<Waker>,
}

struct TimerLease<T: Transport> {
    id: u64,
    progress: Progress<T>,
}

pub(super) enum HeadPoll<Pending, Head> {
    Pending(Pending),
    Ready(Result<Head, EdgeError>),
}

pub(super) trait Transport {
    type Body;
    type Head;
    type Pending;

    fn body_handle(body: &Self::Body) -> u32;
    fn head_handle(pending: &Self::Pending) -> Option<u32>;
    fn poll_head(
        &mut self,
        pending: Self::Pending,
        budget: DispatchBudget,
        clock: &MonotonicClock,
    ) -> HeadPoll<Self::Pending, Self::Head>;
    fn read_body(
        &mut self,
        body: &mut Self::Body,
        maximum: usize,
        budget: DispatchBudget,
        clock: &MonotonicClock,
    ) -> Result<Bytes, EdgeError>;
    fn select(&mut self, handles: &[u32], timeout_ms: u32) -> Result<Option<usize>, EdgeError>;
}

enum Phase<T: Transport> {
    Body {
        handle: Option<T::Body>,
        next: Option<Result<Bytes, EdgeError>>,
        requested: bool,
    },
    Head(Option<T::Pending>),
    HeadReady(Option<Result<T::Head, EdgeError>>),
}

struct Slot<T: Transport> {
    budget: DispatchBudget,
    expired: bool,
    phase: Phase<T>,
    waker: Option<Waker>,
}

struct State<T: Transport> {
    boundary: Option<Deadline>,
    clock: MonotonicClock,
    cursor: usize,
    opaque_cursor: usize,
    previous: MonotonicInstant,
    slots: Vec<Option<Slot<T>>>,
    timer_id: u64,
    timers: BTreeMap<u64, Timer>,
    transport: T,
    wakes: Vec<Waker>,
}

pub(super) struct Progress<T: Transport> {
    driving: Rc<Cell<bool>>,
    state: Rc<RefCell<State<T>>>,
}

pub(super) struct Lease<T: Transport> {
    index: usize,
    progress: Progress<T>,
}

struct Driving {
    active: Rc<Cell<bool>>,
    previous: bool,
    previous_driver: Option<Rc<DriverContext>>,
    previous_wait: Option<WaitFactory>,
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for Driving {
    fn drop(&mut self) {
        self.active.set(self.previous);
        let retired = CURRENT_WAIT.with(|active| active.replace(self.previous_wait.take()));
        let retired_driver =
            CURRENT_DRIVER.with(|active| active.replace(self.previous_driver.take()));
        drop(retired);
        drop(retired_driver);
    }
}

impl<T: Transport> Clone for Progress<T> {
    fn clone(&self) -> Self {
        Self {
            driving: Rc::clone(&self.driving),
            state: Rc::clone(&self.state),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.driving.clone_from(&source.driving);
        self.state.clone_from(&source.state);
    }
}

impl<T: Transport + 'static> Progress<T> {
    pub(super) fn body(&self, handle: T::Body, budget: DispatchBudget) -> BodyStream {
        let lease = self.register(
            budget,
            Phase::Body {
                handle: Some(handle),
                next: None,
                requested: false,
            },
        );
        let progress = self.clone();
        stream! {
            loop {
                let read = poll_fn(|cx| lease.poll_body(cx));
                let bytes = if progress.driving.get() { read.await } else { progress.drive(read).await? }?;
                if bytes.is_empty() { return; }
                yield Ok(bytes);
            }
        }.boxed_local()
    }

    pub(super) async fn drive<F>(&self, future: F) -> Result<F::Output, EdgeError>
    where
        F: Future,
    {
        futures_util::pin_mut!(future);
        let progress = self.clone();
        let wait: WaitFactory = Rc::new(move |deadline, clock| progress.waiter(deadline, clock));
        poll_fn(|cx| {
            let nested = self.driving.get();
            let parent = CURRENT_DRIVER.with(|active| active.borrow().clone());
            let repoll = parent.as_ref().map_or_else(
                || Rc::new(Cell::new(false)),
                |ancestor| Rc::clone(&ancestor.repoll),
            );
            let state = Rc::clone(&self.state);
            let current = Rc::new(DriverContext {
                limit: Rc::new(move || state.borrow_mut().wait_limit()),
                parent,
                repoll,
                waker: cx.waker().clone(),
            });
            let guard = Driving {
                previous: self.driving.replace(true),
                active: Rc::clone(&self.driving),
                previous_wait: CURRENT_WAIT.with(|active| active.replace(Some(Rc::clone(&wait)))),
                previous_driver: CURRENT_DRIVER
                    .with(|active| active.replace(Some(Rc::clone(&current)))),
            };
            let result = future.as_mut().poll(cx);
            drop(guard);
            if let Poll::Ready(output) = result {
                current.repoll_parent();
                return Poll::Ready(Ok(output));
            }
            if nested {
                return Poll::Pending;
            }
            let (turned, wakes) = {
                let mut shared = self.state.borrow_mut();
                let turned = shared.turn(!current.repoll.get(), || current.ancestor_limit());
                (turned, take(&mut shared.wakes))
            };
            if turned.as_ref().is_ok_and(|made_progress| *made_progress) || turned.is_err() {
                current.repoll.set(true);
                current.repoll_parent();
                cx.waker().wake_by_ref();
            }
            for waiting in wakes {
                waiting.wake();
            }
            turned?;
            Poll::Pending
        })
        .await
    }

    pub(super) fn head(&self, pending: T::Pending, budget: DispatchBudget) -> Lease<T> {
        self.register(budget, Phase::Head(Some(pending)))
    }

    pub(super) fn new(
        transport: T,
        clock: MonotonicClock,
        previous: MonotonicInstant,
        boundary: Option<Deadline>,
    ) -> Self {
        Self {
            driving: Rc::new(Cell::new(false)),
            state: Rc::new(RefCell::new(State {
                boundary,
                clock,
                cursor: 0,
                opaque_cursor: 0,
                previous,
                slots: Vec::new(),
                timer_id: 0,
                timers: BTreeMap::new(),
                transport,
                wakes: Vec::new(),
            })),
        }
    }

    fn register(&self, budget: DispatchBudget, phase: Phase<T>) -> Lease<T> {
        let mut state = self.state.borrow_mut();
        let index = state.slots.len();
        // An operation registers each head/body once. Never reuse an index while a lease can live.
        state.slots.push(Some(Slot {
            budget,
            expired: false,
            phase,
            waker: None,
        }));
        Lease {
            index,
            progress: self.clone(),
        }
    }

    fn waiter(&self, deadline: Deadline, clock: MonotonicClock) -> OutboundFuture<()> {
        let mut state = self.state.borrow_mut();
        let id = state.timer_id;
        let Some(next) = id.checked_add(1) else {
            return Box::pin(async {
                Err(EdgeError::internal(anyhow::anyhow!(
                    "Fastly timer registration exhausted"
                )))
            });
        };
        state.timer_id = next;
        state.timers.insert(
            id,
            Timer {
                previous: clock.now(),
                clock,
                deadline,
                outcome: None,
                waker: None,
            },
        );
        let lease = TimerLease {
            id,
            progress: self.clone(),
        };
        Box::pin(async move {
            poll_fn(|cx| {
                let replacement = cx.waker().clone();
                let (observation, previous) = {
                    let mut shared = lease.progress.state.borrow_mut();
                    let timer = shared.timers.get_mut(&lease.id).ok_or_else(missing_slot)?;
                    let prior = timer.waker.replace(replacement);
                    (
                        timer.outcome.take().map_or(Poll::Pending, Poll::Ready),
                        prior,
                    )
                };
                drop(previous);
                observation
            })
            .await
        })
    }

    pub(super) fn with_current<Output>(&self, operation: impl FnOnce() -> Output) -> Output {
        let progress = self.clone();
        let wait: WaitFactory = Rc::new(move |deadline, clock| progress.waiter(deadline, clock));
        let _guard = Driving {
            previous: self.driving.replace(true),
            active: Rc::clone(&self.driving),
            previous_wait: CURRENT_WAIT.with(|active| active.replace(Some(wait))),
            previous_driver: CURRENT_DRIVER.with(|active| active.borrow().clone()),
        };
        operation()
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl<T: Transport> Drop for TimerLease<T> {
    fn drop(&mut self) {
        let retired = self.progress.state.borrow_mut().timers.remove(&self.id);
        drop(retired);
    }
}

impl<T: Transport + 'static> Lease<T> {
    fn poll_body(&self, cx: &mut Context<'_>) -> Poll<Result<Bytes, EdgeError>> {
        let mut state = self.progress.state.borrow_mut();
        let slot = state
            .slots
            .get_mut(self.index)
            .and_then(Option::as_mut)
            .ok_or_else(missing_slot)?;
        let Phase::Body {
            next, requested, ..
        } = &mut slot.phase
        else {
            return Poll::Ready(Err(missing_slot()));
        };
        if let Some(result) = next.take() {
            return Poll::Ready(result);
        }
        *requested = true;
        slot.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    pub(super) async fn receive(self) -> Result<T::Head, EdgeError> {
        poll_fn(|cx| {
            let mut state = self.progress.state.borrow_mut();
            let slot = state
                .slots
                .get_mut(self.index)
                .and_then(Option::as_mut)
                .ok_or_else(missing_slot)?;
            match &mut slot.phase {
                Phase::HeadReady(result) => Poll::Ready(result.take().ok_or_else(missing_slot)?),
                Phase::Head(_) => {
                    slot.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
                Phase::Body { .. } => Poll::Ready(Err(missing_slot())),
            }
        })
        .await
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl<T: Transport> Drop for Lease<T> {
    fn drop(&mut self) {
        let retired = self
            .progress
            .state
            .borrow_mut()
            .slots
            .get_mut(self.index)
            .and_then(Option::take);
        drop(retired);
    }
}

impl<T: Transport> State<T> {
    fn opaque_head(&self) -> Option<usize> {
        self.slots
            .iter()
            .enumerate()
            .skip(self.opaque_cursor)
            .chain(self.slots.iter().enumerate().take(self.opaque_cursor))
            .find_map(|(index, entry)| {
                entry
                    .as_ref()
                    .is_some_and(|slot| {
                        matches!(&slot.phase,
                            Phase::Head(Some(pending)) if T::head_handle(pending).is_none())
                    })
                    .then_some(index)
            })
    }

    fn poll_head(&mut self, index: usize) -> Result<(), EdgeError> {
        let slot = self
            .slots
            .get_mut(index)
            .and_then(Option::as_mut)
            .ok_or_else(missing_slot)?;
        let Phase::Head(owned) = &mut slot.phase else {
            return Err(missing_slot());
        };
        let pending = owned.take().ok_or_else(missing_slot)?;
        match self.transport.poll_head(pending, slot.budget, &self.clock) {
            HeadPoll::Pending(waiting) => slot.phase = Phase::Head(Some(waiting)),
            HeadPoll::Ready(result) => {
                slot.phase = Phase::HeadReady(Some(result));
                if let Some(waker) = slot.waker.take() {
                    self.wakes.push(waker);
                }
            }
        }
        let now = self.sample()?;
        let polled = self
            .slots
            .get_mut(index)
            .and_then(Option::as_mut)
            .ok_or_else(missing_slot)?;
        if polled.budget.deadline.is_expired_at(now) {
            expire(polled, &mut self.wakes);
        }
        Ok(())
    }

    fn sample(&mut self) -> Result<MonotonicInstant, EdgeError> {
        let now = self.clock.now();
        if now < self.previous {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "outbound clock moved backwards during Fastly progress"
            )));
        }
        self.previous = now;
        Ok(now)
    }

    fn select_ready(
        &mut self,
        remaining: Duration,
        active: &[(usize, u32)],
    ) -> Result<bool, EdgeError> {
        let milliseconds = u32::try_from(remaining.as_millis())
            .unwrap_or(u32::MAX)
            .max(1);
        let handles = active.iter().map(|(_, handle)| *handle).collect::<Vec<_>>();
        let selection = self.transport.select(&handles, milliseconds)?;
        self.sample()?;
        let Some(selected) = selection else {
            return Ok(true);
        };
        let (index, _) = *active.get(selected).ok_or_else(|| {
            EdgeError::internal(anyhow::anyhow!(
                "Fastly readiness returned an invalid handle index"
            ))
        })?;
        self.cursor = index.saturating_add(1);
        self.service(index)?;
        Ok(true)
    }

    fn service(&mut self, index: usize) -> Result<(), EdgeError> {
        let slot = self
            .slots
            .get_mut(index)
            .and_then(Option::as_mut)
            .ok_or_else(missing_slot)?;
        if slot.budget.deadline.is_expired_at(self.previous) {
            expire(slot, &mut self.wakes);
            return Ok(());
        }
        match slot.phase {
            Phase::Head(_) => self.poll_head(index)?,
            Phase::Body { .. } => {
                let Phase::Body {
                    handle,
                    next,
                    requested,
                } = &mut slot.phase
                else {
                    return Err(missing_slot());
                };
                let body = handle.as_mut().ok_or_else(missing_slot)?;
                let result = self
                    .transport
                    .read_body(body, READ_BYTES, slot.budget, &self.clock);
                *requested = false;
                if result.as_ref().map_or(true, Bytes::is_empty) {
                    *handle = None;
                }
                *next = Some(result);
                if let Some(waker) = slot.waker.take() {
                    self.wakes.push(waker);
                }
            }
            Phase::HeadReady(_) => return Err(missing_slot()),
        }
        let now = self.sample()?;
        let serviced = self
            .slots
            .get_mut(index)
            .and_then(Option::as_mut)
            .ok_or_else(missing_slot)?;
        if serviced.budget.deadline.is_expired_at(now) {
            expire(serviced, &mut self.wakes);
        }
        Ok(())
    }

    fn timers(&mut self) -> (bool, Option<Duration>) {
        let mut newly_expired = false;
        let mut timer_remaining = None;
        for timer in self
            .timers
            .values_mut()
            .filter(|timer| timer.outcome.is_none())
        {
            let observed = timer.clock.now();
            let outcome = if observed < timer.previous {
                Some(Err(EdgeError::internal(anyhow::anyhow!(
                    "Fastly timer clock moved backwards"
                ))))
            } else if let Some(remaining) = timer.deadline.remaining_at(observed) {
                timer_remaining =
                    Some(timer_remaining.map_or(remaining, |prior: Duration| prior.min(remaining)));
                None
            } else {
                Some(Ok(()))
            };
            timer.previous = observed;
            if let Some(result) = outcome {
                timer.outcome = Some(result);
                newly_expired = true;
                if let Some(waiting) = timer.waker.take() {
                    self.wakes.push(waiting);
                }
            }
        }
        (newly_expired, timer_remaining)
    }

    fn turn(
        &mut self,
        allow_wait: bool,
        mut ancestor_limit: impl FnMut() -> Result<Option<Duration>, EdgeError>,
    ) -> Result<bool, EdgeError> {
        let now = self.sample()?;
        let (mut newly_expired, timer_remaining) = self.timers();
        for slot in self.slots.iter_mut().flatten() {
            if slot.budget.deadline.is_expired_at(now) {
                newly_expired |= expire(slot, &mut self.wakes);
            }
        }
        // Publish a new timeout promptly, but an occupied mailbox backpressures only its slot.
        if newly_expired {
            return Ok(true);
        }

        // A child turn still permits expiry publication, but never a second host wait.
        if !allow_wait {
            return Ok(false);
        }

        let boundary_remaining = self
            .boundary
            .map(|deadline| deadline.remaining_at(self.previous).unwrap_or_default());
        if boundary_remaining.is_some_and(|remaining| remaining.is_zero()) {
            return Ok(true);
        }
        if self.slots.is_empty() && timer_remaining.is_none() && boundary_remaining.is_none() {
            return Ok(false);
        }
        if ancestor_limit()?.is_some_and(|remaining| remaining.is_zero()) {
            return Ok(true);
        }
        let opaque = self.opaque_head();
        if let Some(index) = opaque {
            self.opaque_cursor = index.saturating_add(1);
            self.poll_head(index)?;
            if matches!(
                self.slots
                    .get(index)
                    .and_then(Option::as_ref)
                    .map(|slot| &slot.phase),
                Some(Phase::HeadReady(_))
            ) {
                return Ok(true);
            }
        }

        let mut earliest = timer_remaining.into_iter().chain(boundary_remaining).min();
        let mut active = Vec::new();
        for (index, entry) in self
            .slots
            .iter()
            .enumerate()
            .skip(self.cursor)
            .chain(self.slots.iter().enumerate().take(self.cursor))
        {
            let Some(slot) = entry else {
                continue;
            };
            if let Some(duration) = slot.budget.deadline.remaining_at(self.previous) {
                earliest = Some(earliest.map_or(duration, |prior: Duration| prior.min(duration)));
            }
            let handle = match &slot.phase {
                Phase::Head(Some(pending)) => T::head_handle(pending),
                Phase::Body {
                    handle: Some(handle),
                    requested: true,
                    next: None,
                } => Some(T::body_handle(handle)),
                Phase::Head(_) | Phase::Body { .. } | Phase::HeadReady(_) => None,
            };
            if let Some(id) = handle {
                active.push((index, id));
            }
        }
        let Some(mut remaining) = earliest else {
            return Ok(false);
        };
        if active.is_empty()
            && opaque.is_none()
            && timer_remaining.is_none()
            && boundary_remaining.is_none()
        {
            return Ok(false);
        }
        // Opaque SDK cache stages expose nonblocking poll, but not their changing wait handle.
        // One shared pulse services them without changing caching policy or sleeping per slot.
        if opaque.is_some() {
            remaining = remaining.min(Duration::from_millis(1));
        }
        if let Some(limit) = ancestor_limit()? {
            if limit.is_zero() {
                return Ok(true);
            }
            remaining = remaining.min(limit);
        }
        if let Some(limit) = self.wait_limit()? {
            if limit.is_zero() {
                return Ok(true);
            }
            remaining = remaining.min(limit);
        }
        self.select_ready(remaining, &active)
    }

    fn wait_limit(&mut self) -> Result<Option<Duration>, EdgeError> {
        let now = self.sample()?;
        let mut remaining = self
            .slots
            .iter()
            .flatten()
            .filter(|slot| {
                !slot.expired
                    && matches!(
                        &slot.phase,
                        Phase::Head(Some(_))
                            | Phase::Body {
                                handle: Some(_),
                                next: None,
                                requested: true
                            }
                    )
            })
            .map(|slot| slot.budget.deadline.remaining_at(now).unwrap_or_default())
            .chain(
                self.boundary
                    .map(|deadline| deadline.remaining_at(now).unwrap_or_default()),
            )
            .min();
        for timer in self
            .timers
            .values_mut()
            .filter(|timer| timer.outcome.is_none())
        {
            let observed = timer.clock.now();
            if observed < timer.previous {
                return Err(EdgeError::internal(anyhow::anyhow!(
                    "Fastly timer clock moved backwards"
                )));
            }
            timer.previous = observed;
            let limit = timer.deadline.remaining_at(observed).unwrap_or_default();
            remaining = Some(remaining.map_or(limit, |prior| prior.min(limit)));
        }
        Ok(remaining)
    }
}

fn expire<T>(slot: &mut Slot<T>, wakes: &mut Vec<Waker>) -> bool
where
    T: Transport,
{
    if slot.expired {
        return false;
    }
    slot.expired = true;
    let detail = match &slot.phase {
        Phase::HeadReady(Some(Err(error)))
        | Phase::Body {
            next: Some(Err(error)),
            ..
        } => error.provider_failure_detail().copied(),
        Phase::Head(_) | Phase::HeadReady(_) | Phase::Body { .. } => None,
    };
    let mut error = super::timeout_error(slot.budget.cause);
    if let EdgeError::GatewayTimeout {
        provider_detail, ..
    } = &mut error
    {
        *provider_detail = detail;
    }
    match &mut slot.phase {
        Phase::Head(_) | Phase::HeadReady(_) => slot.phase = Phase::HeadReady(Some(Err(error))),
        Phase::Body {
            handle,
            next,
            requested,
        } => {
            *handle = None;
            *next = Some(Err(error));
            *requested = false;
        }
    }
    if let Some(waker) = slot.waker.take() {
        wakes.push(waker);
    }
    true
}

fn missing_slot() -> EdgeError {
    EdgeError::internal(anyhow::anyhow!(
        "Fastly progress exchange is no longer owned"
    ))
}

pub(super) fn wait_current(deadline: Deadline, clock: MonotonicClock) -> OutboundFuture<()> {
    let factory = CURRENT_WAIT.with(|active| active.borrow().clone());
    factory.map_or_else(
        || -> OutboundFuture<()> {
            Box::pin(async {
                Err(EdgeError::internal(anyhow::anyhow!(
                    "Fastly timer requires an active outbound progress scope"
                )))
            })
        },
        |wait| wait(deadline, clock),
    )
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    clippy::decimal_literal_representation,
    clippy::panic_in_result_fn,
    clippy::shadow_reuse,
    clippy::unwrap_in_result,
    reason = "deterministic test transport asserts scheduling invariants with small bounded counters and byte sizes"
)]
mod tests {
    use std::io::ErrorKind;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::Wake;

    use edgezero_core::{
        BadGatewayReason, BudgetSource, Deadline, FastlyFailureCause, ProviderFailureDetail,
        ProviderFailurePhase,
    };
    use futures::executor::block_on;
    use futures_util::future::{join, pending};
    use futures_util::task::{AtomicWaker, noop_waker_ref};

    use super::*;

    struct Handle {
        id: u32,
        opaque: bool,
        released: Rc<Cell<usize>>,
        remaining: usize,
    }
    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for Handle {
        fn drop(&mut self) {
            self.released.set(self.released.get() + 1);
        }
    }

    #[derive(Default)]
    struct Observations {
        complete_at: Option<MonotonicInstant>,
        domains: Vec<Vec<u32>>,
        invalid_index: bool,
        permission: Option<u32>,
        polls: Vec<u32>,
        reads: Vec<u32>,
        source_error: bool,
        spurious_heads: usize,
        waits: Vec<u32>,
    }
    struct TestTransport {
        now: Arc<Mutex<MonotonicInstant>>,
        observations: Rc<RefCell<Observations>>,
    }

    struct WakeCount(AtomicUsize);

    #[expect(
        clippy::missing_trait_methods,
        reason = "default wake_by_ref delegates to wake"
    )]
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    std::thread_local! {
        static CANCEL_TIMER: RefCell<Option<OutboundFuture<()>>> = const { RefCell::new(None) };
    }

    struct CancelTimer;

    #[expect(
        clippy::missing_trait_methods,
        reason = "default wake_by_ref delegates to wake"
    )]
    impl Wake for CancelTimer {
        fn wake(self: Arc<Self>) {
            let retired = CANCEL_TIMER.with(|slot| slot.borrow_mut().take());
            drop(retired);
        }
    }
    impl Transport for TestTransport {
        type Body = Handle;
        type Head = u32;
        type Pending = Handle;
        fn body_handle(body: &Handle) -> u32 {
            body.id
        }
        fn head_handle(pending: &Handle) -> Option<u32> {
            (!pending.opaque).then_some(pending.id)
        }
        fn poll_head(
            &mut self,
            pending: Handle,
            _budget: DispatchBudget,
            _clock: &MonotonicClock,
        ) -> HeadPoll<Handle, u32> {
            let mut observed = self.observations.borrow_mut();
            observed.polls.push(pending.id);
            if pending.id == 0 || observed.spurious_heads > 0 {
                observed.spurious_heads = observed.spurious_heads.saturating_sub(1);
                HeadPoll::Pending(pending)
            } else {
                if let Some(at) = observed.complete_at {
                    *self.now.lock().expect("clock") = at;
                }
                HeadPoll::Ready(if observed.source_error {
                    Err(source_failure())
                } else {
                    Ok(pending.id)
                })
            }
        }
        fn read_body(
            &mut self,
            body: &mut Handle,
            maximum: usize,
            _budget: DispatchBudget,
            _clock: &MonotonicClock,
        ) -> Result<Bytes, EdgeError> {
            let mut observed = self.observations.borrow_mut();
            assert_eq!(
                observed.permission.take(),
                Some(body.id),
                "every read needs its own same-slot selection"
            );
            assert_eq!(maximum, 16_384);
            observed.reads.push(body.id);
            if let Some(at) = observed.complete_at {
                *self.now.lock().expect("clock") = at;
            }
            if observed.source_error {
                return Err(source_failure());
            }
            let count = body.remaining.min(maximum);
            body.remaining -= count;
            Ok(Bytes::from(vec![b'x'; count]))
        }
        fn select(&mut self, handles: &[u32], timeout_ms: u32) -> Result<Option<usize>, EdgeError> {
            let mut observed = self.observations.borrow_mut();
            assert!(timeout_ms > 0);
            observed.waits.push(timeout_ms);
            observed.domains.push(handles.to_vec());
            if observed.invalid_index {
                return Ok(Some(handles.len()));
            }
            let selected = handles.iter().position(|handle| *handle != 0);
            if let Some(index) = selected {
                observed.permission = Some(handles[index]);
            } else {
                let mut now = self.now.lock().expect("clock");
                *now = now
                    .checked_add(Duration::from_millis(u64::from(timeout_ms)))
                    .expect("advance");
            }
            Ok(selected)
        }
    }

    fn source_failure() -> EdgeError {
        EdgeError::bad_gateway_with_detail(
            "fixture source failed",
            BadGatewayReason::Transport,
            ProviderFailureDetail::Fastly {
                phase: ProviderFailurePhase::ResponseBodyRead,
                cause: FastlyFailureCause::IoError {
                    kind: ErrorKind::UnexpectedEof,
                },
            },
        )
    }

    fn fixture() -> (
        Progress<TestTransport>,
        Rc<RefCell<Observations>>,
        Arc<Mutex<MonotonicInstant>>,
    ) {
        fixture_at(Arc::new(Mutex::new(MonotonicInstant::now())))
    }

    fn fixture_at(
        now: Arc<Mutex<MonotonicInstant>>,
    ) -> (
        Progress<TestTransport>,
        Rc<RefCell<Observations>>,
        Arc<Mutex<MonotonicInstant>>,
    ) {
        let source = Arc::clone(&now);
        let clock = MonotonicClock::new(move || *source.lock().expect("clock"));
        let observations = Rc::new(RefCell::new(Observations::default()));
        let previous = *now.lock().expect("clock");
        let progress = Progress::new(
            TestTransport {
                observations: Rc::clone(&observations),
                now: Arc::clone(&now),
            },
            clock,
            previous,
            None,
        );
        (progress, observations, now)
    }
    fn budget(
        now: &Arc<Mutex<MonotonicInstant>>,
        millis: u64,
        cause: BudgetSource,
    ) -> DispatchBudget {
        let duration = Duration::from_millis(millis);
        let deadline = Deadline::at_instant(
            now.lock()
                .expect("clock")
                .checked_add(duration)
                .expect("deadline"),
        );
        DispatchBudget {
            duration,
            deadline,
            cause,
        }
    }
    fn handle(id: u32, remaining: usize, opaque: bool, released: &Rc<Cell<usize>>) -> Handle {
        Handle {
            id,
            remaining,
            opaque,
            released: Rc::clone(released),
        }
    }
    async fn collect(mut source: BodyStream) -> Result<usize, EdgeError> {
        let mut bytes = 0;
        while let Some(chunk) = source.next().await {
            bytes += chunk?.len();
        }
        Ok(bytes)
    }

    #[test]
    fn nested_wait_is_bounded_by_all_ancestor_deadlines() {
        let (parent, parent_observed, now) = fixture();
        let (middle, middle_observed, _) = fixture_at(Arc::clone(&now));
        let (child, child_observed, _) = fixture_at(Arc::clone(&now));
        let released = Rc::new(Cell::new(0));
        let selected = budget(&now, 200, BudgetSource::RequestDeadline);
        let parent_head = parent.head(handle(0, 0, false, &released), selected);
        let middle_head = middle.head(
            handle(0, 0, false, &released),
            budget(&now, 1_000, BudgetSource::Default),
        );
        let child_head = child.head(
            handle(0, 0, false, &released),
            budget(&now, 10_000, BudgetSource::Default),
        );
        let mut operation = Box::pin(parent.drive(middle.drive(child.drive(child_head.receive()))));
        assert!(
            operation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(child_observed.borrow().waits, [200]);
        assert!(middle_observed.borrow().waits.is_empty());
        assert!(parent_observed.borrow().waits.is_empty());
        assert_eq!(*now.lock().expect("clock"), selected.deadline.instant());
        drop(operation);
        assert!(matches!(
            block_on(parent_head.receive()),
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::RequestDeadline,
                ..
            })
        ));
        drop(middle_head);
        assert_eq!(released.get(), 3);
    }

    #[test]
    fn custom_pair_upload_boundary_limits_child_without_parent_slots_or_timers() {
        let (parent, parent_observed, now) = fixture();
        let (child, child_observed, _) = fixture_at(Arc::clone(&now));
        let selected = budget(&now, 200, BudgetSource::RequestDeadline);
        parent.state.borrow_mut().boundary = Some(selected.deadline);
        let clock = parent.state.borrow().clock.clone();
        let registered = Arc::new(AtomicWaker::new());
        let time = edgezero_core::OutboundTime::new(clock, move |deadline, timer_clock| {
            let waiter = Arc::clone(&registered);
            Box::pin(poll_fn(move |cx| {
                waiter.register(cx.waker());
                if deadline.is_expired_at(timer_clock.now()) {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }))
        });
        let mut timer = time.wait_until(selected.deadline);
        let released = Rc::new(Cell::new(0));
        let child_head = child.head(
            handle(0, 0, false, &released),
            budget(&now, 10_000, BudgetSource::Default),
        );
        let mut source = Box::pin(child.drive(child_head.receive()));
        let mut operation = Box::pin(parent.drive(poll_fn(|cx| {
            if let Poll::Ready(result) = timer.as_mut().poll(cx) {
                return Poll::Ready(result);
            }
            assert!(source.as_mut().poll(cx).is_pending());
            Poll::Pending
        })));
        let mut context = Context::from_waker(Waker::noop());
        assert!(operation.as_mut().poll(&mut context).is_pending());
        assert!(parent.state.borrow().slots.is_empty());
        assert!(parent.state.borrow().timers.is_empty());
        assert_eq!(child_observed.borrow().waits, [200]);
        assert!(parent_observed.borrow().waits.is_empty());
        assert_eq!(*now.lock().expect("clock"), selected.deadline.instant());
        assert!(matches!(
            operation.as_mut().poll(&mut context),
            Poll::Ready(Ok(Ok(())))
        ));
        drop(operation);
        drop(source);
        assert_eq!(released.get(), 1);
    }

    #[test]
    fn child_turn_and_ready_result_repoll_parent_without_another_selection() {
        let (parent, parent_observed, now) = fixture();
        let (child, child_observed, _) = fixture_at(Arc::clone(&now));
        let released = Rc::new(Cell::new(0));
        let selected = budget(&now, 1_000, BudgetSource::Default);
        let parent_head = parent.head(handle(0, 0, false, &released), selected);
        let child_head = child.head(handle(101, 0, false, &released), selected);
        let wake_count = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake_count));
        let mut context = Context::from_waker(&waker);
        let mut operation = Box::pin(parent.drive(async {
            assert_eq!(
                child.drive(child_head.receive()).await.unwrap().unwrap(),
                101
            );
            pending::<()>().await;
        }));
        assert!(operation.as_mut().poll(&mut context).is_pending());
        assert_eq!(child_observed.borrow().polls, [101]);
        assert!(parent_observed.borrow().waits.is_empty());
        wake_count.0.store(0, Ordering::SeqCst);
        assert!(operation.as_mut().poll(&mut context).is_pending());
        assert!(
            wake_count.0.load(Ordering::SeqCst) > 0,
            "a ready child must wake its parent"
        );
        assert!(parent_observed.borrow().waits.is_empty());
        drop(operation);
        drop(parent_head);
        assert_eq!(released.get(), 2);
        assert!(!parent.driving.get());
        assert!(!child.driving.get());
    }

    #[test]
    fn expired_timer_can_cancel_a_sibling_from_its_waker() {
        let (progress, _, now) = fixture();
        let clock = progress.state.borrow().clock.clone();
        let deadline = budget(&now, 10, BudgetSource::Default).deadline;
        let sibling_deadline = budget(&now, 1_000, BudgetSource::Default).deadline;
        let mut first = progress.with_current(|| super::wait_current(deadline, clock.clone()));
        let sibling = progress.with_current(|| super::wait_current(sibling_deadline, clock));
        CANCEL_TIMER.with(|slot| *slot.borrow_mut() = Some(sibling));
        let cancel_waker = Waker::from(Arc::new(CancelTimer));
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(&cancel_waker))
                .is_pending()
        );
        let mut operation = Box::pin(progress.drive(pending::<()>()));
        let mut context = Context::from_waker(Waker::noop());
        assert!(operation.as_mut().poll(&mut context).is_pending());
        assert!(operation.as_mut().poll(&mut context).is_pending());
        assert!(CANCEL_TIMER.with(|slot| slot.borrow().is_none()));
        assert_eq!(progress.state.borrow().timers.len(), 1);
        assert!(matches!(
            first.as_mut().poll(&mut context),
            Poll::Ready(Ok(()))
        ));
        drop(first);
        drop(operation);
        assert!(progress.state.borrow().timers.is_empty());
    }

    #[test]
    fn nested_live_wait_ignores_unconsumed_ancestor_timeout_mailboxes() {
        let (parent, parent_observed, now) = fixture();
        let (child, child_observed, _) = fixture_at(Arc::clone(&now));
        let released = Rc::new(Cell::new(0));
        let expired = budget(&now, 10, BudgetSource::PerCallTimeout);
        let retained = parent.head(handle(0, 0, false, &released), expired);
        let clock = parent.state.borrow().clock.clone();
        let mut retained_timer =
            parent.with_current(|| super::wait_current(expired.deadline, clock));
        *now.lock().expect("clock") = expired.deadline.instant();
        let mut publisher = Box::pin(parent.drive(pending::<()>()));
        let mut context = Context::from_waker(Waker::noop());
        assert!(publisher.as_mut().poll(&mut context).is_pending());
        drop(publisher);
        let selected = budget(&now, 50, BudgetSource::RequestDeadline);
        let live = parent.head(handle(0, 0, false, &released), selected);
        let child_head = child.head(
            handle(0, 0, false, &released),
            budget(&now, 10_000, BudgetSource::Default),
        );
        let mut operation = Box::pin(parent.drive(child.drive(child_head.receive())));
        assert!(operation.as_mut().poll(&mut context).is_pending());
        assert_eq!(child_observed.borrow().waits, [50]);
        assert!(parent_observed.borrow().waits.is_empty());
        drop(operation);
        assert!(matches!(
            block_on(live.receive()),
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::RequestDeadline,
                ..
            })
        ));
        assert!(matches!(
            block_on(retained.receive()),
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::PerCallTimeout,
                ..
            })
        ));
        assert!(matches!(
            retained_timer.as_mut().poll(&mut context),
            Poll::Ready(Ok(()))
        ));
        drop(retained_timer);
        assert!(parent.state.borrow().timers.is_empty());
        assert_eq!(released.get(), 3);
    }

    #[test]
    fn nested_poll_cost_keeps_original_deadline_and_checks_before_opaque_poll() {
        for elapsed in [199, 200] {
            let (parent, parent_observed, now) = fixture();
            let (child, child_observed, _) = fixture_at(Arc::clone(&now));
            let released = Rc::new(Cell::new(0));
            let selected = budget(&now, 200, BudgetSource::RequestDeadline);
            let after_poll = budget(&now, elapsed, BudgetSource::Default)
                .deadline
                .instant();
            let parent_head = parent.head(handle(0, 0, false, &released), selected);
            let child_head = child.head(
                handle(0, 0, true, &released),
                budget(&now, 10_000, BudgetSource::Default),
            );
            let mut operation = Box::pin(parent.drive(child.drive(async {
                *now.lock().expect("clock") = after_poll;
                child_head.receive().await
            })));
            assert!(
                operation
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            if elapsed == 200 {
                assert!(
                    child_observed.borrow().polls.is_empty(),
                    "expired ancestor must precede opaque host polling"
                );
                assert!(child_observed.borrow().waits.is_empty());
            } else {
                assert_eq!(child_observed.borrow().polls, [0]);
                assert_eq!(child_observed.borrow().waits, [1]);
            }
            assert!(parent_observed.borrow().waits.is_empty());
            assert_eq!(*now.lock().expect("clock"), selected.deadline.instant());
            drop(operation);
            assert!(matches!(
                block_on(parent_head.receive()),
                Err(EdgeError::GatewayTimeout {
                    cause: BudgetSource::RequestDeadline,
                    ..
                })
            ));
            assert_eq!(released.get(), 2);
        }
    }

    #[test]
    fn timer_only_wait_uses_shared_selection_and_unregisters() {
        let (progress, observed, now) = fixture();
        let deadline = budget(&now, 10, BudgetSource::RequestDeadline).deadline;
        let clock = progress.state.borrow().clock.clone();
        block_on(progress.drive(async { super::wait_current(deadline, clock).await }))
            .expect("progress")
            .expect("timer");
        assert_eq!(observed.borrow().waits, [10]);
        assert_eq!(observed.borrow().domains, [Vec::<u32>::new()]);
        assert!(progress.state.borrow().timers.is_empty());
    }

    #[test]
    fn ready_operation_cancels_its_timer_without_host_selection() {
        let (progress, observed, now) = fixture();
        let deadline = budget(&now, 10, BudgetSource::RequestDeadline).deadline;
        let clock = progress.state.borrow().clock.clone();
        block_on(progress.drive(async {
            let mut timer = super::wait_current(deadline, clock);
            assert!(
                timer
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert_eq!(progress.state.borrow().timers.len(), 1);
            drop(timer);
        }))
        .expect("ready operation");
        assert!(observed.borrow().waits.is_empty());
        assert!(progress.state.borrow().timers.is_empty());
    }

    #[test]
    fn nested_timer_is_driven_only_by_the_outer_progress_loop() {
        let (progress, observed, now) = fixture();
        let deadline = budget(&now, 10, BudgetSource::RequestDeadline).deadline;
        let clock = progress.state.borrow().clock.clone();
        block_on(
            progress.drive(progress.drive(async { super::wait_current(deadline, clock).await })),
        )
        .expect("outer")
        .expect("inner")
        .expect("timer");
        assert_eq!(observed.borrow().waits, [10]);
        assert!(progress.state.borrow().timers.is_empty());
        assert!(
            block_on(super::wait_current(
                deadline,
                progress.state.borrow().clock.clone()
            ))
            .is_err(),
            "activation must be restored"
        );
    }

    #[test]
    fn cancelling_driver_releases_pending_timer_registration() {
        let (progress, _observed, now) = fixture();
        let deadline = budget(&now, 10, BudgetSource::RequestDeadline).deadline;
        let clock = progress.state.borrow().clock.clone();
        let mut operation =
            Box::pin(progress.drive(async { super::wait_current(deadline, clock).await }));
        assert!(
            operation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(progress.state.borrow().timers.len(), 1);
        drop(operation);
        assert!(progress.state.borrow().timers.is_empty());
    }

    #[test]
    fn fastly_multiplexer_preserves_sibling_progress() {
        for opaque in [false, true] {
            let (progress, observed, now) = fixture();
            let released = Rc::new(Cell::new(0));
            let selected = budget(&now, 50, BudgetSource::RequestDeadline);
            let pending = progress.head(handle(0, 0, opaque, &released), selected);
            let body = progress.body(handle(101, 32_769, false, &released), selected);
            assert_eq!(
                block_on(progress.drive(collect(body)))
                    .expect("driver")
                    .expect("body"),
                32_769
            );
            assert_eq!(observed.borrow().reads, [101, 101, 101, 101]);
            if opaque {
                assert_eq!(observed.borrow().polls, [0, 0, 0, 0]);
                assert!(observed.borrow().waits.iter().all(|wait| *wait == 1));
            } else {
                assert!(
                    observed
                        .borrow()
                        .domains
                        .iter()
                        .all(|domain| domain.contains(&0) && domain.contains(&101))
                );
            }
            assert_eq!(released.get(), 1);
            drop(pending);
            assert_eq!(released.get(), 2);
        }
    }

    #[test]
    fn continuously_ready_bodies_rotate_and_require_fresh_readiness() {
        let (progress, observed, now) = fixture();
        let released = Rc::new(Cell::new(0));
        let selected = budget(&now, 50, BudgetSource::PerCallTimeout);
        let first = progress.body(handle(101, 32_769, false, &released), selected);
        let second = progress.body(handle(102, 32_769, false, &released), selected);
        let (first, second) =
            block_on(progress.drive(join(collect(first), collect(second)))).expect("driver");
        assert_eq!(first.expect("first"), 32_769);
        assert_eq!(second.expect("second"), 32_769);
        assert_eq!(
            observed.borrow().reads,
            [101, 102, 101, 102, 101, 102, 101, 102]
        );
        assert_eq!(released.get(), 2);
    }

    #[test]
    fn earliest_deadlines_expire_at_equality_without_restarting_siblings() {
        let (progress, observed, now) = fixture();
        let released = Rc::new(Cell::new(0));
        let first = progress.head(
            handle(0, 0, false, &released),
            budget(&now, 10, BudgetSource::RequestDeadline),
        );
        let second = progress.head(
            handle(0, 0, false, &released),
            budget(&now, 50, BudgetSource::BatchCutoff),
        );
        let (first, second) =
            block_on(progress.drive(join(first.receive(), second.receive()))).expect("driver");
        assert!(matches!(
            first,
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::RequestDeadline,
                ..
            })
        ));
        assert!(matches!(
            second,
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::BatchCutoff,
                ..
            })
        ));
        assert_eq!(observed.borrow().waits, [10, 40]);
        assert_eq!(released.get(), 2);
    }

    #[test]
    fn leases_cancel_only_their_slots_and_streams_retain_the_engine() {
        let (progress, observed, now) = fixture();
        let weak = Rc::downgrade(&progress.state);
        let released = Rc::new(Cell::new(0));
        let selected = budget(&now, 50, BudgetSource::PerCallTimeout);
        let abandoned = progress.head(handle(0, 0, false, &released), selected);
        let mut body = progress.body(handle(101, 16_385, false, &released), selected);
        drop(abandoned);
        drop(progress);
        assert!(weak.upgrade().is_some());
        assert_eq!(
            block_on(body.next()).expect("chunk").expect("body").len(),
            16_384
        );
        assert_eq!(observed.borrow().reads, [101]);
        drop(body);
        assert_eq!(released.get(), 2);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn invalid_readiness_index_is_an_internal_driver_error_without_io() {
        let (progress, observed, now) = fixture();
        observed.borrow_mut().invalid_index = true;
        let released = Rc::new(Cell::new(0));
        let source = progress.body(
            handle(101, 10, false, &released),
            budget(&now, 50, BudgetSource::Default),
        );
        let error = block_on(progress.drive(collect(source))).expect_err("invalid selection");
        assert!(matches!(error, EdgeError::Internal { .. }));
        assert!(observed.borrow().reads.is_empty());
        assert_eq!(released.get(), 1);
    }

    #[test]
    fn backwards_clock_fails_before_readiness_or_source_io() {
        let (progress, observed, now) = fixture();
        let released = Rc::new(Cell::new(0));
        let source = progress.body(
            handle(101, 10, false, &released),
            budget(&now, 50, BudgetSource::Default),
        );
        let previous = *now.lock().expect("clock");
        *now.lock().expect("clock") = previous
            .checked_sub(Duration::from_millis(1))
            .expect("earlier");
        assert!(matches!(
            block_on(progress.drive(collect(source))),
            Err(EdgeError::Internal { .. })
        ));
        assert!(observed.borrow().domains.is_empty());
        assert_eq!(released.get(), 1);
    }

    #[test]
    fn original_deadline_outranks_late_io_and_preserves_typed_source_errors() {
        for head in [false, true] {
            for source_error in [false, true] {
                for late in [false, true] {
                    let (progress, observed, now) = fixture();
                    let released = Rc::new(Cell::new(0));
                    let selected = budget(&now, 10, BudgetSource::RequestDeadline);
                    observed.borrow_mut().source_error = source_error;
                    if late {
                        observed.borrow_mut().complete_at = Some(selected.deadline.instant());
                    }
                    let outcome = if head {
                        let lease = progress.head(handle(101, 0, false, &released), selected);
                        block_on(progress.drive(lease.receive()))
                            .expect("driver")
                            .map(|_| ())
                    } else {
                        let source = progress.body(handle(101, 10, false, &released), selected);
                        block_on(progress.drive(collect(source)))
                            .expect("driver")
                            .map(|_| ())
                    };
                    if late {
                        assert!(matches!(
                            outcome,
                            Err(EdgeError::GatewayTimeout {
                                cause: BudgetSource::RequestDeadline,
                                ..
                            })
                        ));
                    } else if source_error {
                        assert!(matches!(
                            outcome,
                            Err(EdgeError::BadGateway {
                                reason: BadGatewayReason::Transport,
                                ..
                            })
                        ));
                    } else {
                        assert!(outcome.is_ok());
                    }
                    if let Err(error) = outcome {
                        assert_eq!(error.provider_failure_detail().is_some(), source_error);
                        if source_error {
                            assert_eq!(
                                error.provider_failure_detail(),
                                source_failure().provider_failure_detail()
                            );
                        }
                    }
                    assert_eq!(released.get(), 1);
                }
            }
        }
    }

    #[test]
    fn spurious_head_readiness_rearms_without_reading_a_body() {
        let (progress, observed, now) = fixture();
        observed.borrow_mut().spurious_heads = 2;
        let released = Rc::new(Cell::new(0));
        let lease = progress.head(
            handle(101, 0, false, &released),
            budget(&now, 50, BudgetSource::Default),
        );
        assert_eq!(
            block_on(progress.drive(lease.receive()))
                .expect("driver")
                .expect("head"),
            101
        );
        assert_eq!(observed.borrow().polls, [101, 101, 101]);
        assert_eq!(observed.borrow().domains, [vec![101], vec![101], vec![101]]);
        assert!(observed.borrow().reads.is_empty());
        assert_eq!(released.get(), 1);
    }

    #[test]
    fn opaque_heads_rotate_with_one_shared_pulse_not_one_wait_per_slot() {
        let (progress, observed, now) = fixture();
        let released = Rc::new(Cell::new(0));
        let selected = budget(&now, 50, BudgetSource::Default);
        let silent = progress.head(handle(0, 0, true, &released), selected);
        let ready = progress.head(handle(102, 0, true, &released), selected);
        assert_eq!(
            block_on(progress.drive(ready.receive()))
                .expect("driver")
                .expect("head"),
            102
        );
        assert_eq!(observed.borrow().polls, [0, 102]);
        assert_eq!(observed.borrow().waits, [1]);
        drop(silent);
        assert_eq!(released.get(), 2);
    }

    #[test]
    fn dropping_unpolled_streams_and_heads_releases_without_host_io() {
        let (progress, observed, now) = fixture();
        let released = Rc::new(Cell::new(0));
        let selected = budget(&now, 50, BudgetSource::Default);
        drop(progress.head(handle(101, 0, false, &released), selected));
        drop(progress.body(handle(102, 10, false, &released), selected));
        assert_eq!(released.get(), 2);
        assert!(observed.borrow().reads.is_empty());
        assert!(observed.borrow().polls.is_empty());
        assert!(observed.borrow().waits.is_empty());
    }

    #[test]
    fn unconsumed_ready_mailbox_does_not_block_a_sibling() {
        let (progress, observed, now) = fixture();
        let released = Rc::new(Cell::new(0));
        let selected = budget(&now, 50, BudgetSource::Default);
        let retained = progress.head(handle(101, 0, false, &released), selected);
        let second = progress.head(handle(102, 0, false, &released), selected);
        let mut driver = Box::pin(progress.drive(second.receive()));
        let mut context = Context::from_waker(noop_waker_ref());
        let mut result = Poll::Pending;
        for _ in 0_u8..4 {
            result = driver.as_mut().poll(&mut context);
            if result.is_ready() {
                break;
            }
        }
        assert!(matches!(result, Poll::Ready(Ok(Ok(102)))));
        assert_eq!(observed.borrow().polls, [101, 102]);
        assert_eq!(block_on(retained.receive()).expect("retained result"), 101);
        drop(driver);
        assert_eq!(released.get(), 2);
    }

    #[test]
    fn unconsumed_timeout_is_published_once_without_starving_a_sibling() {
        let (progress, observed, now) = fixture();
        let released = Rc::new(Cell::new(0));
        let selected = budget(&now, 5, BudgetSource::RequestDeadline);
        let retained = progress.head(handle(0, 0, false, &released), selected);
        let second = progress.head(
            handle(102, 0, false, &released),
            budget(&now, 50, BudgetSource::Default),
        );
        *now.lock().expect("clock") = selected.deadline.instant();
        let mut driver = Box::pin(progress.drive(second.receive()));
        let mut context = Context::from_waker(noop_waker_ref());
        let mut result = Poll::Pending;
        for _ in 0_u8..4 {
            result = driver.as_mut().poll(&mut context);
            if result.is_ready() {
                break;
            }
        }
        assert!(matches!(result, Poll::Ready(Ok(Ok(102)))));
        assert_eq!(observed.borrow().polls, [102]);
        assert!(matches!(
            block_on(retained.receive()),
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::RequestDeadline,
                ..
            })
        ));
        drop(driver);
        assert_eq!(released.get(), 2);
    }
}

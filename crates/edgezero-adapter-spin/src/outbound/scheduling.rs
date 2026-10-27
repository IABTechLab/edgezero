use std::task::Poll;

use edgezero_core::{
    Deadline, DispatchBudget, EdgeError, OutboundBatch, OutboundBatchContext,
    OutboundBatchDriverEvent, OutboundExchangeOwner, OutboundFuture, OutboundPending,
    OutboundResponse, OutboundTask, OutboundTime,
};
use futures::channel::{mpsc, oneshot};
use futures_util::future::{LocalBoxFuture, poll_fn};
use futures_util::stream::{FuturesUnordered, LocalBoxStream, once};
use futures_util::{FutureExt as _, StreamExt as _};

type Slot = (usize, DispatchBudget, OutboundFuture<OutboundResponse>);

fn spawn_owned(
    task_future: LocalBoxFuture<'static, ()>,
    spawn: impl FnOnce(LocalBoxFuture<'static, ()>),
) -> Result<OutboundExchangeOwner, EdgeError> {
    let (task, owner) = OutboundTask::new(task_future)?;
    spawn(task.boxed_local());
    Ok(owner)
}

pub(super) fn schedule_exchange(
    future: OutboundFuture<OutboundResponse>,
    spawn: impl FnOnce(LocalBoxFuture<'static, ()>),
) -> Result<OutboundPending, EdgeError> {
    let (sender, receiver) = oneshot::channel();
    let task = async move {
        drop(sender.send(future.await));
    }
    .boxed_local();
    let owner = spawn_owned(task, spawn)?;
    Ok(OutboundPending::new(
        Box::pin(async move {
            receiver.await.map_err(|_closed| {
                EdgeError::internal(anyhow::anyhow!(
                    "Spin outbound exchange ended without a completion"
                ))
            })?
        }),
        owner,
    ))
}

pub(super) fn schedule_batch(
    slots: Vec<Slot>,
    time: OutboundTime,
    cutoff: Deadline,
    spawn: impl FnOnce(LocalBoxFuture<'static, ()>),
) -> Result<
    (
        LocalBoxStream<'static, OutboundBatchDriverEvent>,
        OutboundExchangeOwner,
    ),
    EdgeError,
> {
    // Each finite input emits at most one observation, plus one terminal event.
    let (sender, receiver) = mpsc::unbounded();
    let task = async move {
        let mut exchanges = slots
            .into_iter()
            .map(|(index, budget, future)| {
                let clock = time.clock().clone();
                async move {
                    let outcome = future.await;
                    (index, budget, clock.now(), outcome)
                }
            })
            .collect::<FuturesUnordered<_>>();
        let mut timer = time.wait_until(cutoff);
        let mut timer_done = false;
        let mut late = false;
        loop {
            let observation = poll_fn(|cx| {
                if !timer_done && let Poll::Ready(result) = timer.as_mut().poll(cx) {
                    timer_done = true;
                    if let Err(error) = result {
                        return Poll::Ready(Err(error));
                    }
                }
                let expired = cutoff.is_expired_at(time.clock().now());
                match exchanges.poll_next_unpin(cx) {
                    Poll::Ready(item) => Poll::Ready(Ok(item)),
                    Poll::Pending if expired => Poll::Ready(Ok(None)),
                    Poll::Pending => Poll::Pending,
                }
            })
            .await;
            match observation {
                Err(error) => {
                    drop(sender.unbounded_send(OutboundBatchDriverEvent::Failed(error)));
                    return;
                }
                Ok(Some((index, budget, completed_at, outcome))) => {
                    let own_timeout = budget.own_deadline_expired_before(cutoff, completed_at);
                    late |= cutoff.is_expired_at(completed_at) && !own_timeout;
                    if sender
                        .unbounded_send(OutboundBatchDriverEvent::Item {
                            index,
                            completed_at,
                            outcome,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(None) => {
                    if late || !exchanges.is_empty() {
                        drop(sender.unbounded_send(OutboundBatchDriverEvent::Cutoff));
                    }
                    return;
                }
            }
        }
    }
    .boxed_local();
    let owner = spawn_owned(task, spawn)?;
    Ok((receiver.boxed_local(), owner))
}

pub(super) fn schedule_prepared_batch(
    context: OutboundBatchContext,
    slots: Vec<Slot>,
    spawn: impl FnOnce(LocalBoxFuture<'static, ()>),
) -> OutboundBatch {
    match schedule_batch(
        slots,
        context.time().clone(),
        context.observation_cutoff(),
        spawn,
    ) {
        Ok((events, owner)) => OutboundBatch::from_driver(context, events, owner),
        Err(error) => OutboundBatch::from_driver(
            context,
            once(async move { OutboundBatchDriverEvent::Failed(error) }),
            OutboundExchangeOwner::new(|| {}),
        ),
    }
}

#[cfg(test)]
mod tests {
    use core::ptr::null;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker};
    use std::time::Duration;

    use bytes::Bytes;
    use edgezero_core::http::{HeaderMap, Method, StatusCode};
    use edgezero_core::outbound::validate_for_dispatch;
    use edgezero_core::outbound::{IndependentOutboundProgress, OutboundProgressRuntime};
    use edgezero_core::{
        Body, BudgetSource, ContentTypeProvenance, Deadline, DispatchBudget, EdgeError, HttpClient,
        MonotonicClock, MonotonicInstant, OutboundBatch, OutboundBatchDriverEvent,
        OutboundBatchTermination, OutboundCollectedSlot, OutboundDispatchRequest,
        OutboundExchangeOwner, OutboundFuture, OutboundHead, OutboundHttpClient, OutboundPending,
        OutboundPreparedBatch, OutboundRequest, OutboundResponse, OutboundScopeOwner,
        OutboundSendMode, OutboundTime,
    };
    use futures::executor::{LocalPool, block_on};
    use futures::task::LocalSpawnExt as _;
    use futures_util::future::{LocalBoxFuture, pending, poll_fn};
    use futures_util::stream::once;
    use futures_util::{FutureExt as _, StreamExt as _};

    use super::{schedule_batch, schedule_exchange, schedule_prepared_batch, spawn_owned};

    #[derive(Default)]
    struct WakerHooks {
        clone: Option<Box<dyn FnOnce()>>,
        drop: Option<Box<dyn FnOnce()>>,
        wake: Option<Box<dyn FnOnce()>>,
    }

    #[derive(Clone, Copy)]
    enum Hook {
        Clone,
        Drop,
        Wake,
    }

    std::thread_local! {
        static WAKER_HOOKS: RefCell<WakerHooks> = const { RefCell::new(WakerHooks { clone: None, drop: None, wake: None }) };
    }

    fn run_hook(hook: Hook) {
        let pending_hook = WAKER_HOOKS.with(|slot| {
            let mut hooks = slot.borrow_mut();
            match hook {
                Hook::Clone => hooks.clone.take(),
                Hook::Drop => hooks.drop.take(),
                Hook::Wake => hooks.wake.take(),
            }
        });
        if let Some(callback) = pending_hook {
            callback();
        }
    }

    fn clone_waker(data: *const ()) -> RawWaker {
        run_hook(Hook::Clone);
        RawWaker::new(data, &WAKER_VTABLE)
    }

    fn drop_waker(_data: *const ()) {
        run_hook(Hook::Drop);
    }
    fn wake_waker(_data: *const ()) {
        run_hook(Hook::Wake);
    }

    const WAKER_VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_waker, wake_waker, wake_waker, drop_waker);

    #[expect(
        unsafe_code,
        reason = "exercise reentrant clone, drop and wake callbacks with a data-free static test waker"
    )]
    fn controlled_waker() -> Waker {
        // SAFETY: every callback ignores the null data; the static vtable owns no allocation.
        // Hooks use only the calling thread's TLS, so moving the waker across threads is safe.
        unsafe { Waker::from_raw(RawWaker::new(null(), &WAKER_VTABLE)) }
    }

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    #[expect(
        clippy::missing_trait_methods,
        reason = "the default wake_by_ref delegates to wake"
    )]
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn stalled_work(
        releases: &Rc<Cell<usize>>,
        polls: &Rc<Cell<usize>>,
    ) -> LocalBoxFuture<'static, ()> {
        let release = Release(Rc::clone(releases));
        let observed = Rc::clone(polls);
        async move {
            let _release = release;
            poll_fn(move |_cx| {
                observed.set(observed.get().saturating_add(1));
                Poll::<()>::Pending
            })
            .await;
        }
        .boxed_local()
    }

    struct Release(Rc<Cell<usize>>);

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_add(1));
        }
    }

    fn response(body: Body) -> OutboundResponse {
        OutboundResponse::new(
            Method::GET,
            StatusCode::OK,
            HeaderMap::new(),
            body,
            MonotonicClock::default(),
            ContentTypeProvenance::Unknown,
        )
    }

    #[test]
    fn begin_progresses_without_collection() {
        let mut executor = LocalPool::new();
        let spawner = executor.spawner();
        let calls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&calls);
        let mut pending = schedule_exchange(
            async move {
                observed.set(observed.get().saturating_add(1));
                Ok(response(Body::from("ready")))
            }
            .boxed_local(),
            move |task| spawner.spawn_local(task).expect("schedule exchange"),
        )
        .expect("schedule exchange");
        executor.run_until_stalled();
        assert_eq!(calls.get(), 1);
        assert!(pending.is_ready());
        assert!(pending.is_ready());
        let response = executor.run_until(pending.collect()).expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn refused_exchange_registration_releases_source_without_spawning() {
        let owner = OutboundScopeOwner::callback();
        owner.close();
        let releases = Rc::new(Cell::new(0_usize));
        let polls = Rc::new(Cell::new(0_usize));
        let release = Release(Rc::clone(&releases));
        let observed = Rc::clone(&polls);
        let spawns = Cell::new(0_usize);
        let outcome = owner.with(|| {
            schedule_exchange(
                async move {
                    let _retained = release;
                    observed.set(observed.get().saturating_add(1));
                    pending().await
                }
                .boxed_local(),
                |_task| spawns.set(spawns.get().saturating_add(1)),
            )
        });
        assert!(matches!(outcome, Err(EdgeError::Internal { .. })));
        assert_eq!(releases.get(), 1);
        assert_eq!(polls.get(), 0);
        assert_eq!(spawns.get(), 0);
    }

    #[test]
    fn refused_batch_registration_releases_sources_without_spawning() {
        let owner = OutboundScopeOwner::callback();
        owner.close();
        let releases = Rc::new(Cell::new(0_usize));
        let release = Release(Rc::clone(&releases));
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let spawns = Cell::new(0_usize);
        let now = MonotonicInstant::now();
        let deadline = Deadline::at_instant(now + Duration::from_secs(1));
        let time = OutboundTime::new(MonotonicClock::new(move || now), |_, _| {
            panic!("refused batch must not register a timer")
        });
        let budget = DispatchBudget {
            cause: BudgetSource::PerCallTimeout,
            deadline,
            duration: Duration::from_secs(1),
        };
        let work = async move {
            let _retained = release;
            observed.set(observed.get().saturating_add(1));
            pending().await
        }
        .boxed_local();
        let outcome = owner.with(|| {
            schedule_batch(vec![(0, budget, work)], time, deadline, |_task| {
                spawns.set(spawns.get().saturating_add(1));
            })
        });
        assert!(matches!(outcome, Err(EdgeError::Internal { .. })));
        assert_eq!(releases.get(), 1);
        assert_eq!(polls.get(), 0);
        assert_eq!(spawns.get(), 0);
    }

    #[test]
    fn cancelling_unpolled_exchange_releases_without_running_it() {
        let mut executor = LocalPool::new();
        let spawner = executor.spawner();
        let releases = Rc::new(Cell::new(0_usize));
        let owner = Release(Rc::clone(&releases));
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let pending = schedule_exchange(
            async move {
                let _owner = owner;
                observed.set(observed.get().saturating_add(1));
                pending().await
            }
            .boxed_local(),
            move |task| spawner.spawn_local(task).expect("schedule exchange"),
        )
        .expect("schedule exchange");
        pending.cancel();
        assert_eq!(releases.get(), 1);
        executor.run_until_stalled();
        assert_eq!(polls.get(), 0);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn cancelling_pending_exchange_releases_its_source_once() {
        let mut executor = LocalPool::new();
        let spawner = executor.spawner();
        let releases = Rc::new(Cell::new(0_usize));
        let owner = Release(Rc::clone(&releases));
        let pending = schedule_exchange(
            async move {
                let _owner = owner;
                pending().await
            }
            .boxed_local(),
            move |task| spawner.spawn_local(task).expect("schedule exchange"),
        )
        .expect("schedule exchange");
        executor.run_until_stalled();
        assert_eq!(releases.get(), 0);
        drop(pending);
        assert_eq!(releases.get(), 1);
        executor.run_until_stalled();
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn cancellation_during_poll_releases_the_active_future_once() {
        let releases = Rc::new(Cell::new(0_usize));
        let release = Release(Rc::clone(&releases));
        let cancellation = Rc::new(RefCell::new(None::<OutboundExchangeOwner>));
        let cancel_from_poll = Rc::clone(&cancellation);
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let mut scheduled = None;
        let owner = spawn_owned(
            async move {
                let _release = release;
                poll_fn(move |_cx| {
                    observed.set(observed.get().saturating_add(1));
                    drop(cancel_from_poll.borrow_mut().take());
                    Poll::<()>::Pending
                })
                .await;
            }
            .boxed_local(),
            |task| scheduled = Some(task),
        )
        .expect("schedule owned task");
        *cancellation.borrow_mut() = Some(owner);
        let mut task = scheduled.expect("scheduled task");
        assert!(
            task.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        assert_eq!(polls.get(), 1);
        assert_eq!(releases.get(), 1);
        drop(task);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn waker_clone_can_cancel_before_the_source_is_polled() {
        let releases = Rc::new(Cell::new(0_usize));
        let polls = Rc::new(Cell::new(0_usize));
        let mut scheduled = None;
        let owner = spawn_owned(stalled_work(&releases, &polls), |task| {
            scheduled = Some(task);
        })
        .expect("schedule owned task");
        WAKER_HOOKS.with(|hooks| hooks.borrow_mut().clone = Some(Box::new(move || drop(owner))));
        let mut task = scheduled.expect("scheduled task");
        let waker = controlled_waker();
        assert!(
            task.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
        assert_eq!(polls.get(), 0);
        assert_eq!(releases.get(), 1);
        drop((task, waker));
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn replacing_a_waker_allows_reentrant_drop_cancellation() {
        let releases = Rc::new(Cell::new(0_usize));
        let polls = Rc::new(Cell::new(0_usize));
        let mut scheduled = None;
        let owner = spawn_owned(stalled_work(&releases, &polls), |task| {
            scheduled = Some(task);
        })
        .expect("schedule owned task");
        let mut task = scheduled.expect("scheduled task");
        let first = controlled_waker();
        assert!(
            task.as_mut()
                .poll(&mut Context::from_waker(&first))
                .is_pending()
        );
        WAKER_HOOKS.with(|hooks| hooks.borrow_mut().drop = Some(Box::new(move || drop(owner))));
        let wakes = Arc::new(WakeCount::default());
        let replacement = Waker::from(Arc::clone(&wakes));
        assert!(
            task.as_mut()
                .poll(&mut Context::from_waker(&replacement))
                .is_ready()
        );
        assert_eq!(
            polls.get(),
            1,
            "waker-drop cancellation must prevent another work poll"
        );
        assert_eq!(releases.get(), 1);
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        assert!(
            task.as_mut()
                .poll(&mut Context::from_waker(&replacement))
                .is_ready()
        );
        assert_eq!(polls.get(), 1);
        drop((task, first, replacement));
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn cancellation_wakeup_can_reentrantly_cancel_a_sibling() {
        let releases = Rc::new(Cell::new(0_usize));
        let polls = Rc::new(Cell::new(0_usize));
        let mut first_task = None;
        let first_owner = spawn_owned(stalled_work(&releases, &polls), |task| {
            first_task = Some(task);
        })
        .expect("schedule owned task");
        let mut sibling_task = None;
        let sibling_owner = spawn_owned(stalled_work(&releases, &polls), |task| {
            sibling_task = Some(task);
        })
        .expect("schedule owned task");
        let mut first = first_task.expect("first task");
        let mut sibling = sibling_task.expect("sibling task");
        let waker = controlled_waker();
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(
            sibling
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        WAKER_HOOKS
            .with(|hooks| hooks.borrow_mut().wake = Some(Box::new(move || drop(sibling_owner))));
        drop(first_owner);
        assert_eq!(releases.get(), 2);
        assert_eq!(polls.get(), 2);
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
        assert!(
            sibling
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
        drop((first, sibling, waker));
        assert_eq!(releases.get(), 2);
    }

    #[test]
    fn executor_exit_revokes_a_retained_pending_exchange() {
        let mut executor = LocalPool::new();
        let spawner = executor.spawner();
        let releases = Rc::new(Cell::new(0_usize));
        let owner = Release(Rc::clone(&releases));
        let mut pending = schedule_exchange(
            async move {
                let _owner = owner;
                pending().await
            }
            .boxed_local(),
            move |task| spawner.spawn_local(task).expect("schedule exchange"),
        )
        .expect("schedule exchange");
        executor.run_until_stalled();
        drop(executor);
        assert_eq!(releases.get(), 1);
        assert!(pending.is_ready());
        let failure = block_on(pending.collect()).expect_err("executor exited");
        assert!(matches!(failure, edgezero_core::EdgeError::Internal { .. }));
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn collected_stream_retains_its_owner_without_reading_payload() {
        let mut executor = LocalPool::new();
        let spawner = executor.spawner();
        let releases = Rc::new(Cell::new(0_usize));
        let owner = Release(Rc::clone(&releases));
        let reads = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&reads);
        let body = Body::from_stream(once(async move {
            let _owner = owner;
            observed.set(observed.get().saturating_add(1));
            Ok(Bytes::from_static(b"body"))
        }));
        let pending = schedule_exchange(
            async move { Ok(response(body)) }.boxed_local(),
            move |task| spawner.spawn_local(task).expect("schedule exchange"),
        )
        .expect("schedule exchange");
        executor.run_until_stalled();
        let response = executor
            .run_until(pending.collect())
            .expect("streamed response");
        assert_eq!(reads.get(), 0);
        assert_eq!(releases.get(), 0);
        let bytes = executor
            .run_until(response.into_bytes_bounded(8))
            .expect("live collected stream");
        assert_eq!(bytes, Bytes::from_static(b"body"));
        assert_eq!(reads.get(), 1);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn unpolled_batch_progresses_and_preserves_explicit_input_indices() {
        let mut executor = LocalPool::new();
        let spawner = executor.spawner();
        let now = MonotonicInstant::now();
        let clock = MonotonicClock::new(move || now);
        let time = OutboundTime::new(clock, |_, _| Box::pin(pending()));
        let deadline =
            Deadline::at_instant(now.checked_add(Duration::from_secs(1)).expect("deadline"));
        let budget = DispatchBudget {
            cause: BudgetSource::PerCallTimeout,
            deadline,
            duration: Duration::from_secs(1),
        };
        let calls = Rc::new(Cell::new(0_usize));
        let slots = [2, 5]
            .into_iter()
            .map(|index| {
                let observed = Rc::clone(&calls);
                (
                    index,
                    budget,
                    async move {
                        observed.set(observed.get().saturating_add(1));
                        Ok(response(Body::empty()))
                    }
                    .boxed_local(),
                )
            })
            .collect();
        let (mut events, owner) = schedule_batch(slots, time, deadline, move |task| {
            spawner.spawn_local(task).expect("schedule batch");
        })
        .expect("schedule batch");
        executor.run_until_stalled();
        assert_eq!(calls.get(), 2);
        for expected in [2, 5] {
            let Some(OutboundBatchDriverEvent::Item {
                index,
                completed_at,
                outcome,
            }) = executor.run_until(events.next())
            else {
                panic!("expected original input index");
            };
            assert_eq!(index, expected);
            assert_eq!(completed_at, now);
            outcome.expect("successful batch response");
        }
        assert!(executor.run_until(events.next()).is_none());
        drop(owner);
    }

    #[test]
    fn cutoff_retains_prior_own_timeout_but_not_equal_deadline() {
        let mut executor = LocalPool::new();
        let spawner = executor.spawner();
        let start = MonotonicInstant::now();
        let end = start
            .checked_add(Duration::from_millis(10))
            .expect("cutoff");
        let now = Arc::new(Mutex::new(start));
        let observed = Arc::clone(&now);
        let clock = MonotonicClock::new(move || *observed.lock().expect("clock"));
        let time = OutboundTime::new(clock, |_, _| Box::pin(pending()));
        let cutoff = Deadline::at_instant(end);
        let own = DispatchBudget {
            cause: BudgetSource::PerCallTimeout,
            deadline: Deadline::at_instant(
                start
                    .checked_add(Duration::from_millis(5))
                    .expect("own deadline"),
            ),
            duration: Duration::from_millis(5),
        };
        let equal = DispatchBudget {
            deadline: cutoff,
            duration: Duration::from_millis(10),
            ..own
        };
        let slots = vec![
            (
                3,
                own,
                async move {
                    *now.lock().expect("clock") = end;
                    Err(edgezero_core::EdgeError::gateway_timeout_caused(
                        "own timeout",
                        own.cause,
                    ))
                }
                .boxed_local(),
            ),
            (7, equal, Box::pin(pending())),
        ];
        let (mut events, owner) = schedule_batch(slots, time, cutoff, move |task| {
            spawner.spawn_local(task).expect("schedule batch");
        })
        .expect("schedule batch");
        executor.run_until_stalled();
        let Some(OutboundBatchDriverEvent::Item {
            index: 3,
            outcome: Err(error),
            ..
        }) = executor.run_until(events.next())
        else {
            panic!("own deadline outcome must precede cutoff");
        };
        assert!(matches!(
            error,
            edgezero_core::EdgeError::GatewayTimeout {
                cause: BudgetSource::PerCallTimeout,
                ..
            }
        ));
        assert!(matches!(
            executor.run_until(events.next()),
            Some(OutboundBatchDriverEvent::Cutoff)
        ));
        assert!(executor.run_until(events.next()).is_none());
        drop(owner);
    }

    #[test]
    fn late_operational_timer_error_completes_at_the_retained_own_deadline() {
        struct Client {
            time: OutboundTime,
            observed: Arc<Mutex<MonotonicInstant>>,
            completed_at: MonotonicInstant,
        }

        impl OutboundHttpClient for Client {
            fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
                Rc::new(IndependentOutboundProgress)
            }

            fn time(&self) -> OutboundTime {
                self.time.clone()
            }

            fn validate_supported_options(
                &self,
                request: &OutboundRequest,
                mode: OutboundSendMode,
            ) -> Result<(), EdgeError> {
                validate_for_dispatch(request, mode)
            }

            fn dispatch_head(
                &self,
                _request: OutboundDispatchRequest,
            ) -> OutboundFuture<OutboundHead> {
                let observed = Arc::clone(&self.observed);
                let completed_at = self.completed_at;
                Box::pin(async move {
                    *observed.lock().expect("clock") = completed_at;
                    Err(EdgeError::internal(anyhow::anyhow!(
                        "operational timer failure"
                    )))
                })
            }

            fn begin(
                &self,
                _request: OutboundDispatchRequest,
            ) -> Result<OutboundPending, EdgeError> {
                Err(EdgeError::internal(anyhow::anyhow!("batch-only fixture")))
            }

            fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
                let (context, requests) = batch.into_slots();
                let slots = requests
                    .into_iter()
                    .map(|(index, request)| {
                        let budget = request.budget();
                        let head = self.dispatch_head(request);
                        (
                            index,
                            budget,
                            async move { head.await?.finish().await }.boxed_local(),
                        )
                    })
                    .collect();
                schedule_prepared_batch(context, slots, block_on)
            }
        }

        let start = MonotonicInstant::now();
        let end = start
            .checked_add(Duration::from_millis(10))
            .expect("cutoff");
        let observed = Arc::new(Mutex::new(start));
        let clock_now = Arc::clone(&observed);
        let time = OutboundTime::new(
            MonotonicClock::new(move || *clock_now.lock().expect("clock")),
            |_, _| Box::pin(pending()),
        );
        let client = HttpClient::with_client(Client {
            time,
            observed,
            completed_at: end,
        });
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .timeout(Duration::from_millis(5));
        let result = block_on(
            client
                .start_batch_until(vec![request], Deadline::at_instant(end))
                .collect(),
        )
        .expect("own timeout completes the batch without a trailing cutoff");
        assert_eq!(result.termination(), OutboundBatchTermination::Completed);
        let [OutboundCollectedSlot::Resolved(slot)] = result.slots() else {
            panic!("one resolved own-deadline timeout");
        };
        assert_eq!(slot.elapsed, Duration::from_millis(5));
        assert!(matches!(
            &slot.outcome,
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::PerCallTimeout,
                ..
            })
        ));
    }
}

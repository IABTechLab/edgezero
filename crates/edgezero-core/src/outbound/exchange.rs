use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::future::poll_fn;

use super::{OutboundFuture, OutboundTime};
use crate::error::EdgeError;
use crate::time::{Deadline, MonotonicClock, MonotonicInstant};

impl OutboundTime {
    /// Returns the clock supplied to both this pair and its timer factory.
    #[must_use]
    #[inline]
    pub fn clock(&self) -> &MonotonicClock {
        &self.clock
    }

    /// Pairs a clock with an owned local timer factory, without adding an executor to core.
    ///
    /// The factory receives the unchanged absolute deadline and the same application clock.
    /// It must register real wakeups, return setup/operational failures as typed errors, and
    /// unregister pending work when its returned future is dropped. Only the shared factory
    /// must be `Send + Sync`; each timer may own local SDK handles or `Rc` values.
    #[must_use]
    #[inline]
    pub fn new<Wait>(clock: MonotonicClock, wait: Wait) -> Self
    where
        Wait: Fn(Deadline, MonotonicClock) -> OutboundFuture<()> + Send + Sync + 'static,
    {
        Self {
            clock,
            wait: Arc::new(wait),
        }
    }

    /// Waits until the paired clock reaches the original absolute deadline.
    ///
    /// A timer's success is only a wakeup: an early wake retires that timer and yields before
    /// rearming the same deadline. At most one timer is created and polled per outer poll.
    /// Dropping the wait cancels its timer registration; a terminal poll also releases the
    /// captured factory and clock even if the completed boxed future is retained.
    ///
    /// # Errors
    /// Returns timer failures unchanged, or typed Internal for an observed clock regression.
    /// Expiry itself is successful wait completion; the caller classifies its governing budget.
    #[must_use]
    #[inline]
    pub fn wait_until(&self, deadline: Deadline) -> OutboundFuture<()> {
        let mut paired = Some(self.clone());
        let mut previous = self.clock.now();
        let mut timer = None;
        Box::pin(poll_fn(move |context| {
            let result = if let Some(time) = &paired {
                poll_wait_until(time, deadline, &mut previous, &mut timer, context)
            } else {
                Poll::Ready(Err(EdgeError::internal(anyhow::anyhow!(
                    "outbound deadline wait polled after completion"
                ))))
            };
            if result.is_ready() {
                drop(timer.take());
                drop(paired.take());
            }
            result
        }))
    }
}

impl fmt::Debug for OutboundTime {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboundTime")
            .field("clock", &self.clock)
            .finish_non_exhaustive()
    }
}

fn checked_now(
    clock: &MonotonicClock,
    previous: &mut MonotonicInstant,
) -> Result<MonotonicInstant, EdgeError> {
    let now = clock.now();
    if now < *previous {
        return Err(EdgeError::internal(anyhow::anyhow!(
            "monotonic clock moved backwards during outbound wait"
        )));
    }
    *previous = now;
    Ok(now)
}

fn poll_wait_until(
    time: &OutboundTime,
    deadline: Deadline,
    previous: &mut MonotonicInstant,
    timer: &mut Option<OutboundFuture<()>>,
    context: &mut Context<'_>,
) -> Poll<Result<(), EdgeError>> {
    let now = checked_now(time.clock(), previous)?;
    if timer.is_none() && deadline.is_expired_at(now) {
        return Poll::Ready(Ok(()));
    }
    // Observe an existing timer's failure even when this poll arrives at/past expiry.
    let pending = timer.get_or_insert_with(|| (time.wait)(deadline, time.clock.clone()));
    let observation = pending.as_mut().poll(context);
    let woke = observation.is_ready();
    if let Poll::Ready(outcome) = observation {
        drop(timer.take());
        outcome?;
    }
    if deadline.is_expired_at(checked_now(time.clock(), previous)?) {
        return Poll::Ready(Ok(()));
    }
    if woke {
        context.waker().wake_by_ref();
    }
    Poll::Pending
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use futures_util::future::pending;
    use futures_util::task::{ArcWake, waker_ref};

    use crate::error::EdgeError;
    use crate::outbound::{OutboundFuture, OutboundTime};
    use crate::time::{Deadline, MonotonicClock, MonotonicInstant};

    #[derive(Clone)]
    struct FakeTime {
        state: Arc<Mutex<FakeTimeState>>,
    }

    struct FakeTimeState {
        deadlines: Vec<MonotonicInstant>,
        drops: usize,
        early_ready: bool,
        now: MonotonicInstant,
        waiters: BTreeMap<usize, (Deadline, Option<Waker>)>,
    }

    struct FakeTimer {
        deadline: Deadline,
        id: usize,
        local_polls: Rc<Cell<usize>>,
        state: Arc<Mutex<FakeTimeState>>,
    }

    struct FaultTimer {
        error: Option<EdgeError>,
        pending_first: bool,
        release: ReleaseCount,
        succeeds_early: bool,
    }

    struct ReleaseCount(Arc<AtomicUsize>);

    #[derive(Debug, thiserror::Error)]
    #[error("fixture timer failure")]
    struct TimerFault {
        identity: Arc<()>,
    }

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl FakeTime {
        fn advance_to(&self, now: MonotonicInstant) {
            let wakers: Vec<_> = {
                let mut state = self.state.lock().expect("fake time");
                state.now = now;
                state
                    .waiters
                    .values_mut()
                    .filter(|(deadline, _)| deadline.is_expired_at(now))
                    .filter_map(|(_, waker)| waker.take())
                    .collect()
            };
            for waker in wakers {
                waker.wake();
            }
        }

        fn clock(&self) -> MonotonicClock {
            let state = Arc::clone(&self.state);
            MonotonicClock::new(move || state.lock().expect("clock").now)
        }

        fn counts(&self) -> (usize, usize, usize) {
            let state = self.state.lock().expect("fake time");
            (state.deadlines.len(), state.waiters.len(), state.drops)
        }

        fn new() -> Self {
            let now = MonotonicInstant::now()
                .checked_add(Duration::from_hours(1))
                .expect("synthetic clock differs from process time on every target");
            Self {
                state: Arc::new(Mutex::new(FakeTimeState {
                    deadlines: Vec::new(),
                    drops: 0,
                    early_ready: false,
                    now,
                    waiters: BTreeMap::new(),
                })),
            }
        }

        fn pair(&self) -> OutboundTime {
            let state = Arc::clone(&self.state);
            OutboundTime::new(self.clock(), move |deadline, clock| {
                let paired_now = clock.now();
                let mut shared = state.lock().expect("timer factory");
                assert_eq!(paired_now, shared.now, "factory receives the app clock");
                assert!(shared.deadlines.len() < 16, "timer creation must not spin");
                if shared.early_ready {
                    assert!(shared.waiters.is_empty(), "retire before rearming");
                }
                let id = shared.deadlines.len();
                shared.deadlines.push(deadline.instant());
                shared.waiters.insert(id, (deadline, None));
                Box::pin(FakeTimer {
                    deadline,
                    id,
                    local_polls: Rc::new(Cell::new(0)),
                    state: Arc::clone(&state),
                })
            })
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for FakeTimer {
        fn drop(&mut self) {
            let mut state = self.state.lock().expect("timer drop");
            assert!(state.waiters.remove(&self.id).is_some(), "release once");
            state.drops = state.drops.saturating_add(1);
        }
    }

    impl Future for FakeTimer {
        type Output = Result<(), EdgeError>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.local_polls
                .set(self.local_polls.get().saturating_add(1));
            assert!(self.local_polls.get() <= 16, "pending timer must not spin");
            let mut state = self.state.lock().expect("timer poll");
            if state.early_ready || self.deadline.is_expired_at(state.now) {
                Poll::Ready(Ok(()))
            } else {
                let (_, waker) = state.waiters.get_mut(&self.id).expect("live registration");
                *waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    impl Future for FaultTimer {
        type Output = Result<(), EdgeError>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let timer = self.get_mut();
            assert!(
                Arc::strong_count(&timer.release.0) > 1,
                "retain the release probe"
            );
            if timer.pending_first {
                timer.pending_first = false;
                cx.waker().wake_by_ref();
                Poll::Pending
            } else if timer.succeeds_early {
                Poll::Ready(Ok(()))
            } else {
                Poll::Ready(Err(timer.error.take().expect("one terminal error")))
            }
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for ReleaseCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl ArcWake for WakeCount {
        fn wake(self: Arc<Self>) {
            Self::wake_by_ref(&self);
        }

        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn poll_wait(
        future: &mut OutboundFuture<()>,
        wake_count: &Arc<WakeCount>,
    ) -> Poll<Result<(), EdgeError>> {
        let waker = waker_ref(wake_count);
        future.as_mut().poll(&mut Context::from_waker(&waker))
    }

    #[test]
    fn outbound_time_advancement_wakes_registered_waker() {
        let fake = FakeTime::new();
        let pair = fake.pair();
        let deadline = pair.clock().deadline_after(Duration::from_millis(100));
        let mut wait = pair.wait_until(deadline);
        drop(pair);
        let old_waker = Arc::new(WakeCount::default());
        let new_waker = Arc::new(WakeCount::default());
        assert!(poll_wait(&mut wait, &old_waker).is_pending());
        assert!(poll_wait(&mut wait, &new_waker).is_pending());
        assert_eq!(fake.counts(), (1, 1, 0), "retain the same pending timer");
        fake.advance_to(
            deadline
                .instant()
                .checked_sub(Duration::from_nanos(1))
                .expect("before deadline"),
        );
        assert_eq!(old_waker.0.load(Ordering::SeqCst), 0);
        assert_eq!(new_waker.0.load(Ordering::SeqCst), 0);
        fake.advance_to(deadline.instant());
        assert_eq!(old_waker.0.load(Ordering::SeqCst), 0);
        assert_eq!(new_waker.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            poll_wait(&mut wait, &new_waker),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(fake.counts(), (1, 0, 1));
    }

    #[test]
    fn outbound_time_early_ready_drops_before_rearming_original_deadline() {
        let fake = FakeTime::new();
        fake.state.lock().expect("fake time").early_ready = true;
        let pair = fake.pair();
        let deadline = pair.clock().deadline_after(Duration::from_millis(100));
        let mut wait = pair.wait_until(deadline);
        let wakes = Arc::new(WakeCount::default());
        assert!(poll_wait(&mut wait, &wakes).is_pending());
        assert_eq!(fake.counts(), (1, 0, 1));
        fake.state.lock().expect("fake time").early_ready = false;
        fake.advance_to(
            deadline
                .instant()
                .checked_sub(Duration::from_millis(20))
                .expect("before deadline"),
        );
        assert!(poll_wait(&mut wait, &wakes).is_pending());
        assert_eq!(fake.counts(), (2, 1, 1));
        assert_eq!(
            fake.state.lock().expect("fake time").deadlines,
            vec![deadline.instant(), deadline.instant()]
        );
        fake.advance_to(deadline.instant());
        assert!(matches!(poll_wait(&mut wait, &wakes), Poll::Ready(Ok(()))));
        assert_eq!(fake.counts(), (2, 0, 2));
    }

    #[test]
    fn outbound_time_frozen_ready_timer_yields_without_spinning() {
        let fake = FakeTime::new();
        fake.state.lock().expect("fake time").early_ready = true;
        let pair = fake.pair();
        let deadline = pair.clock().deadline_after(Duration::from_millis(100));
        let mut wait = pair.wait_until(deadline);
        let wakes = Arc::new(WakeCount::default());
        for count in 1..=3 {
            assert!(poll_wait(&mut wait, &wakes).is_pending());
            assert_eq!(fake.counts(), (count, 0, count));
            assert_eq!(wakes.0.load(Ordering::SeqCst), count);
        }
        fake.advance_to(deadline.instant());
        assert!(matches!(poll_wait(&mut wait, &wakes), Poll::Ready(Ok(()))));
        assert_eq!(fake.counts(), (3, 0, 3));
    }

    #[test]
    fn outbound_time_timer_errors_are_preserved() {
        for mode in 0_u8..4 {
            let fake = FakeTime::new();
            let identity = Arc::new(());
            let captured_identity = Arc::clone(&identity);
            let calls = Arc::new(AtomicUsize::new(0));
            let captured_calls = Arc::clone(&calls);
            let drops = Arc::new(AtomicUsize::new(0));
            let captured_drops = Arc::clone(&drops);
            let pair = OutboundTime::new(fake.clock(), move |_, _| {
                let call = captured_calls.fetch_add(1, Ordering::SeqCst);
                let timer_identity = Arc::clone(&captured_identity);
                assert_eq!(
                    captured_drops.load(Ordering::SeqCst),
                    call,
                    "retire before rearm"
                );
                Box::pin(FaultTimer {
                    error: Some(EdgeError::internal(anyhow::Error::new(TimerFault {
                        identity: timer_identity,
                    }))),
                    pending_first: mode == 1 || mode == 3,
                    release: ReleaseCount(Arc::clone(&captured_drops)),
                    succeeds_early: mode == 2 && call == 0,
                })
            });
            let deadline = pair.clock().deadline_after(Duration::from_secs(1));
            let mut wait = pair.wait_until(deadline);
            let wakes = Arc::new(WakeCount::default());
            if mode != 0 {
                assert!(poll_wait(&mut wait, &wakes).is_pending());
            }
            if mode == 3 {
                fake.advance_to(deadline.instant());
            }
            let Poll::Ready(Err(EdgeError::Internal { source })) = poll_wait(&mut wait, &wakes)
            else {
                panic!("timer failure must remain typed Internal");
            };
            let original = source
                .downcast_ref::<TimerFault>()
                .expect("original source");
            assert!(Arc::ptr_eq(&identity, &original.identity));
            let expected = if mode == 2 { 2 } else { 1 };
            assert_eq!(calls.load(Ordering::SeqCst), expected);
            assert_eq!(drops.load(Ordering::SeqCst), expected);
        }
    }

    #[test]
    fn outbound_time_drop_cancels_registration_once() {
        let fake = FakeTime::new();
        let pair = fake.pair();
        let deadline = pair.clock().deadline_after(Duration::from_secs(1));
        drop(pair.wait_until(deadline));
        assert_eq!(fake.counts(), (0, 0, 0));
        let mut first = pair.wait_until(deadline);
        let mut second = pair.clone().wait_until(deadline);
        let first_wakes = Arc::new(WakeCount::default());
        let second_wakes = Arc::new(WakeCount::default());
        assert!(poll_wait(&mut first, &first_wakes).is_pending());
        assert!(poll_wait(&mut second, &second_wakes).is_pending());
        drop(first);
        assert_eq!(fake.counts(), (2, 1, 1));
        fake.advance_to(deadline.instant());
        assert_eq!(first_wakes.0.load(Ordering::SeqCst), 0);
        assert_eq!(second_wakes.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            poll_wait(&mut second, &second_wakes),
            Poll::Ready(Ok(()))
        ));
        drop(second);
        assert_eq!(fake.counts(), (2, 0, 2));

        fake.state.lock().expect("fake time").early_ready = true;
        let next_deadline = pair.clock().deadline_after(Duration::from_secs(1));
        let mut early = pair.wait_until(next_deadline);
        let early_wakes = Arc::new(WakeCount::default());
        assert!(poll_wait(&mut early, &early_wakes).is_pending());
        assert_eq!(fake.counts(), (3, 0, 3));
        drop(early);
        fake.advance_to(next_deadline.instant());
        assert_eq!(early_wakes.0.load(Ordering::SeqCst), 1);
        assert_eq!(fake.counts(), (3, 0, 3));
    }

    #[test]
    fn outbound_time_terminal_poll_releases_factory_capture() {
        for mode in 0_u8..3 {
            let fake = FakeTime::new();
            let capture = Arc::new(());
            let weak = Arc::downgrade(&capture);
            let state = Arc::clone(&fake.state);
            let pair = OutboundTime::new(fake.clock(), move |deadline, _| {
                let keep_alive = Arc::clone(&capture);
                let timer_state = Arc::clone(&state);
                Box::pin(async move {
                    if mode == 2 {
                        pending::<()>().await;
                    }
                    drop(keep_alive);
                    if mode == 1 {
                        Err(EdgeError::internal(anyhow::anyhow!(
                            "fixture setup failure"
                        )))
                    } else {
                        timer_state.lock().expect("timer clock").now = deadline.instant();
                        Ok(())
                    }
                })
            });
            let mut wait = pair.wait_until(pair.clock().deadline_after(Duration::from_secs(1)));
            drop(pair);
            assert!(weak.upgrade().is_some());
            let result = poll_wait(&mut wait, &Arc::new(WakeCount::default()));
            if mode == 2 {
                assert!(result.is_pending());
                assert!(weak.upgrade().is_some());
            } else {
                assert!(result.is_ready());
                assert!(weak.upgrade().is_none(), "release before boxed future drop");
            }
            drop(wait);
            assert!(weak.upgrade().is_none());
        }
    }

    #[test]
    fn outbound_time_expired_waits_do_not_register_timers() {
        let fake = FakeTime::new();
        let pair = fake.pair();
        let now = pair.clock().now();
        for instant in [
            now,
            now.checked_sub(Duration::from_nanos(1))
                .expect("past instant"),
        ] {
            let mut wait = pair.wait_until(Deadline::at_instant(instant));
            assert!(matches!(
                poll_wait(&mut wait, &Arc::new(WakeCount::default())),
                Poll::Ready(Ok(()))
            ));
        }
        assert_eq!(fake.counts(), (0, 0, 0));
    }

    #[test]
    fn outbound_time_expired_pending_timer_is_cancelled_without_waiting() {
        let fake = FakeTime::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let captured_drops = Arc::clone(&drops);
        let pair = OutboundTime::new(fake.clock(), move |_, _| {
            let guard = ReleaseCount(Arc::clone(&captured_drops));
            Box::pin(async move {
                pending::<()>().await;
                drop(guard);
                Ok(())
            })
        });
        let deadline = pair.clock().deadline_after(Duration::from_secs(1));
        let mut wait = pair.wait_until(deadline);
        let wakes = Arc::new(WakeCount::default());
        assert!(poll_wait(&mut wait, &wakes).is_pending());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        fake.advance_to(deadline.instant());
        assert!(matches!(poll_wait(&mut wait, &wakes), Poll::Ready(Ok(()))));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn outbound_time_backwards_clock_is_internal_and_releases_registration() {
        for registered in [false, true] {
            let fake = FakeTime::new();
            let pair = fake.pair();
            let start = pair.clock().now();
            let mut wait = pair.wait_until(pair.clock().deadline_after(Duration::from_secs(1)));
            let wakes = Arc::new(WakeCount::default());
            if registered {
                assert!(poll_wait(&mut wait, &wakes).is_pending());
            }
            fake.advance_to(
                start
                    .checked_sub(Duration::from_nanos(1))
                    .expect("past instant"),
            );
            assert!(matches!(
                poll_wait(&mut wait, &wakes),
                Poll::Ready(Err(EdgeError::Internal { .. }))
            ));
            let count = usize::from(registered);
            assert_eq!(fake.counts(), (count, 0, count));
        }
    }

    #[test]
    fn outbound_time_rechecks_clock_after_timer_success() {
        for backwards in [false, true] {
            let fake = FakeTime::new();
            let start = fake.clock().now();
            let deadline = fake.clock().deadline_after(Duration::from_secs(1));
            let state = Arc::clone(&fake.state);
            let pair = OutboundTime::new(fake.clock(), move |_, _| {
                let timer_state = Arc::clone(&state);
                Box::pin(async move {
                    timer_state.lock().expect("timer clock").now = if backwards {
                        start
                            .checked_sub(Duration::from_nanos(1))
                            .expect("past instant")
                    } else {
                        deadline.instant()
                    };
                    Ok(())
                })
            });
            let mut wait = pair.wait_until(deadline);
            let result = poll_wait(&mut wait, &Arc::new(WakeCount::default()));
            if backwards {
                assert!(matches!(
                    result,
                    Poll::Ready(Err(EdgeError::Internal { .. }))
                ));
            } else {
                assert!(matches!(result, Poll::Ready(Ok(()))));
            }
        }
    }

    #[test]
    fn outbound_time_is_clone_send_sync_with_owned_local_futures() {
        fn assert_shared<T>()
        where
            T: Clone + Send + Sync,
        {
        }
        assert_shared::<crate::OutboundTime>();
        let fake = FakeTime::new();
        let pair = fake.pair();
        let wait: crate::OutboundFuture<()> =
            pair.wait_until(pair.clock().deadline_after(Duration::from_secs(1)));
        drop(wait);
    }
}

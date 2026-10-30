use edgezero_core::{Deadline, EdgeError, MonotonicClock, MonotonicInstant, OutboundTime};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct TestTime {
    state: Arc<Mutex<TestTimeState>>,
}

struct TestTimeState {
    next_timer: usize,
    now: MonotonicInstant,
    waiters: BTreeMap<usize, std::task::Waker>,
}

struct TestTimer {
    clock: MonotonicClock,
    deadline: Deadline,
    id: usize,
    state: Arc<Mutex<TestTimeState>>,
}

impl Default for TestTime {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(TestTimeState {
                next_timer: 0,
                now: MonotonicInstant::now(),
                waiters: BTreeMap::new(),
            })),
        }
    }
}

impl TestTime {
    pub(crate) fn advance_to(&self, now: MonotonicInstant) {
        let wakers = {
            let mut state = self.state.lock().expect("fake time");
            state.now = now;
            state.waiters.values().cloned().collect::<Vec<_>>()
        };
        for waker in wakers {
            waker.wake();
        }
    }

    pub(crate) fn clock(&self) -> MonotonicClock {
        let state = Arc::clone(&self.state);
        MonotonicClock::new(move || state.lock().expect("clock").now)
    }

    pub(crate) fn pair(&self) -> OutboundTime {
        let state = Arc::clone(&self.state);
        OutboundTime::new(self.clock(), move |deadline, clock| {
            let id = {
                let mut state = state.lock().expect("timer registration");
                let id = state.next_timer;
                state.next_timer = state.next_timer.checked_add(1).expect("timer id");
                id
            };
            Box::pin(TestTimer {
                clock,
                deadline,
                id,
                state: Arc::clone(&state),
            })
        })
    }
}

impl std::future::Future for TestTimer {
    type Output = Result<(), EdgeError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.deadline.is_expired_at(self.clock.now()) {
            std::task::Poll::Ready(Ok(()))
        } else {
            self.state
                .lock()
                .expect("timer waiter")
                .waiters
                .insert(self.id, cx.waker().clone());
            std::task::Poll::Pending
        }
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for TestTimer {
    fn drop(&mut self) {
        self.state
            .lock()
            .expect("timer cancellation")
            .waiters
            .remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};
    use std::time::Duration;

    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn paired_time_wakes_waiters_and_cancels_registrations() {
        let time = TestTime::default();
        let pair = time.pair();
        let deadline = pair.clock().deadline_after(Duration::from_secs(1));
        let count = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&count));
        let mut context = Context::from_waker(&waker);
        let mut completed = pair.wait_until(deadline);
        let mut cancelled = pair.wait_until(deadline);
        assert!(completed.as_mut().poll(&mut context).is_pending());
        assert!(cancelled.as_mut().poll(&mut context).is_pending());
        assert_eq!(time.state.lock().unwrap().waiters.len(), 2);
        drop(cancelled);
        assert_eq!(time.state.lock().unwrap().waiters.len(), 1);
        time.advance_to(deadline.instant());
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            completed.as_mut().poll(&mut context),
            std::task::Poll::Ready(Ok(()))
        ));
        assert!(time.state.lock().unwrap().waiters.is_empty());
    }
}

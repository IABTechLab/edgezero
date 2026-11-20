//! Request-local after-response release and bounded Workers tasks.

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
mod platform {
    use super::{Gate, LocalBoxFuture, ReleaseGuard, run_bounded};
    use std::sync::Arc;
    use std::time::Duration;

    use edgezero_core::after_response::{AfterResponseHandle, AfterResponseScheduler};
    use edgezero_core::error::EdgeError;
    use edgezero_core::time::MonotonicClock;
    use worker::Context;

    use crate::outbound::native_time;

    struct Scheduler {
        clock: MonotonicClock,
        context: Arc<Context>,
        gate: Arc<Gate>,
    }

    impl AfterResponseScheduler for Scheduler {
        fn schedule(
            &self,
            budget: Duration,
            task: LocalBoxFuture<'static, ()>,
        ) -> Result<(), EdgeError> {
            let released = self.gate.register()?;
            let clock = self.clock.clone();
            self.context.wait_until(async move {
                let _released = released.await;
                run_bounded(task, budget, native_time(clock)).await;
            });
            Ok(())
        }
    }

    pub(crate) fn arm(
        context: Arc<Context>,
        clock: MonotonicClock,
    ) -> (AfterResponseHandle, ReleaseGuard) {
        let release = ReleaseGuard::new();
        let handle = AfterResponseHandle::new(Scheduler {
            context,
            gate: Arc::clone(&release.0),
            clock,
        });
        (handle, release)
    }
}

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
pub(crate) use platform::arm;

use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use edgezero_core::error::EdgeError;
use edgezero_core::outbound::{OutboundScopeOwner, OutboundTime};
use futures::channel::oneshot;
use futures_util::future::{Either, FutureExt as _, LocalBoxFuture, select};

const MAX_BUDGET: Duration = Duration::from_secs(30);

#[derive(Default)]
struct Gate {
    waiters: Mutex<Option<Vec<oneshot::Sender<()>>>>,
}

impl Gate {
    fn register(&self) -> Result<oneshot::Receiver<()>, EdgeError> {
        let mut state = self.waiters.lock().unwrap_or_else(PoisonError::into_inner);
        let waiters = state.as_mut().ok_or_else(|| {
            EdgeError::internal(anyhow::anyhow!(
                "after-response tasks can only be registered while the request is handled"
            ))
        })?;
        let (sender, receiver) = oneshot::channel();
        waiters.push(sender);
        Ok(receiver)
    }
}

/// Sole release owner, retained through conversion and the terminal response pump.
pub(crate) struct ReleaseGuard(Arc<Gate>);

impl ReleaseGuard {
    fn new() -> Self {
        Self(Arc::new(Gate {
            waiters: Mutex::new(Some(Vec::new())),
        }))
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "Drop::pin_drop is unstable and conflicts with drop"
)]
impl Drop for ReleaseGuard {
    fn drop(&mut self) {
        let waiters = self
            .0
            .waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        for sender in waiters.unwrap_or_default() {
            let _closed = sender.send(());
        }
    }
}

async fn run_bounded(task: LocalBoxFuture<'static, ()>, budget: Duration, time: OutboundTime) {
    let deadline = time.clock().deadline_after(budget.min(MAX_BUDGET));
    let callback = OutboundScopeOwner::callback();
    let result = callback
        .run(
            AssertUnwindSafe(async move {
                match select(time.wait_until(deadline), task).await {
                    Either::Left((Ok(()), _task)) => {
                        log::warn!("after-response task exceeded its budget and was cancelled");
                    }
                    Either::Left((Err(_error), _task)) => {
                        log::warn!("after-response task timer failed; the task was cancelled");
                    }
                    Either::Right(((), _timer)) => {}
                }
            })
            .catch_unwind(),
        )
        .await;
    if result.is_err() {
        log::error!("after-response task panicked");
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_BUDGET, ReleaseGuard, run_bounded};
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;

    use edgezero_core::error::EdgeError;
    use edgezero_core::outbound::OutboundTime;
    use edgezero_core::time::MonotonicClock;
    use futures::executor::block_on;
    use futures::future::{join_all, pending};
    use futures_util::FutureExt as _;

    #[test]
    fn release_is_request_local_and_closes_registration() {
        let first = ReleaseGuard::new();
        let second = ReleaseGuard::new();
        let retained = Arc::clone(&first.0);
        let first_receiver = first.0.register().unwrap();
        let second_receiver = first.0.register().unwrap();
        let mut other = second.0.register().unwrap();
        assert!(other.try_recv().unwrap().is_none());
        drop(first);
        assert_eq!(block_on(first_receiver), Ok(()));
        assert_eq!(block_on(second_receiver), Ok(()));
        retained.register().unwrap_err();
        assert!(other.try_recv().unwrap().is_none());
        drop(second);
        assert_eq!(block_on(other), Ok(()));
    }

    #[test]
    fn panicking_task_does_not_prevent_a_sibling_from_running() {
        let ran = Rc::new(Cell::new(false));
        let observed = Rc::clone(&ran);
        let time = || OutboundTime::new(MonotonicClock::default(), |_, _| Box::pin(pending()));
        let tasks = [
            run_bounded(
                async { panic!("task failure") }.boxed_local(),
                Duration::from_secs(1),
                time(),
            ),
            run_bounded(
                async move {
                    observed.set(true);
                }
                .boxed_local(),
                Duration::from_secs(1),
                time(),
            ),
        ];
        block_on(join_all(tasks));
        assert!(ran.get());
    }

    #[test]
    fn huge_budget_is_capped_and_timer_failure_drops_local_task_unpolled() {
        struct Probe(Rc<Cell<bool>>);
        #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
        impl Drop for Probe {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        let dropped = Rc::new(Cell::new(false));
        let polled = Rc::new(Cell::new(false));
        let observed = Rc::clone(&polled);
        let probe = Probe(Rc::clone(&dropped));
        let clock = MonotonicClock::default();
        let start = clock.now();
        let time = OutboundTime::new(
            MonotonicClock::new(move || start),
            move |deadline, paired_clock| {
                assert_eq!(deadline.remaining_at(paired_clock.now()), Some(MAX_BUDGET));
                Box::pin(async { Err(EdgeError::internal(anyhow::anyhow!("timer failure"))) })
            },
        );
        block_on(run_bounded(
            async move {
                let _probe = probe;
                observed.set(true);
                pending::<()>().await;
            }
            .boxed_local(),
            Duration::MAX,
            time,
        ));
        assert!(dropped.get());
        assert!(!polled.get());
    }
}

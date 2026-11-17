//! Axum after-response tasks: spawned on the server's `LocalSet`, released when the response
//! body leaves Hyper, bounded by their budget and drained on shutdown.

use std::sync::Arc;
use std::time::Duration;

use edgezero_core::OutboundScopeOwner;
use edgezero_core::after_response::{AfterResponseHandle, AfterResponseScheduler};
use edgezero_core::error::EdgeError;
use futures_util::future::LocalBoxFuture;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::outbound_executor::AxumOutboundExecutorHandle;

/// Counts after-response tasks that were registered but have not yet finished.
#[derive(Clone, Debug)]
pub(crate) struct AfterResponseTracker(Arc<watch::Sender<usize>>);

impl Default for AfterResponseTracker {
    fn default() -> Self {
        Self(Arc::new(watch::Sender::new(0)))
    }
}

impl AfterResponseTracker {
    /// Number of tasks still registered.
    pub(crate) fn pending(&self) -> usize {
        *self.0.borrow()
    }

    fn register(&self) -> TrackedTask {
        self.0.send_modify(|count| *count = count.saturating_add(1));
        TrackedTask(self.clone())
    }

    /// Resolves once every registered task has finished, timed out or been cancelled.
    pub(crate) async fn wait_idle(&self) {
        let mut idle = self.0.subscribe();
        // The sender lives in `self`, so the channel cannot close while this waits.
        let _idle = idle.wait_for(|count| *count == 0).await;
    }
}

/// Decrements the tracker when its task ends for any reason, including spawn failure.
struct TrackedTask(AfterResponseTracker);

#[expect(
    clippy::missing_trait_methods,
    reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
)]
impl Drop for TrackedTask {
    fn drop(&mut self) {
        self.0
            .0
            .send_modify(|count| *count = count.saturating_sub(1));
    }
}

/// Releases a request's tasks when dropped, which happens when Hyper drops the response body.
///
/// The sender is never used to send; only its drop is observed.
pub(crate) struct AfterResponseRelease(
    #[expect(dead_code, reason = "held only to be dropped")] watch::Sender<()>,
);

/// Per-request scheduler installed by the Axum service.
struct AxumAfterResponse {
    delivered: watch::Receiver<()>,
    executor: AxumOutboundExecutorHandle,
    tracker: AfterResponseTracker,
}

impl AfterResponseScheduler for AxumAfterResponse {
    fn schedule(
        &self,
        budget: Duration,
        task: LocalBoxFuture<'static, ()>,
    ) -> Result<(), EdgeError> {
        // The registration window closes when the request's tasks are released.
        if self.delivered.has_changed().is_err() {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "after-response tasks can only be registered while the request is handled"
            )));
        }
        let tracked = self.tracker.register();
        let mut delivered = self.delivered.clone();
        self.executor
            .spawn(Box::pin(async move {
                let _tracked = tracked;
                // Only the sender's drop ends this wait; the request never sends a value.
                let _released = delivered.changed().await;
                let scope = OutboundScopeOwner::callback();
                if timeout(budget, scope.run(task)).await.is_err() {
                    log::warn!(
                        "after-response task exceeded its {budget:?} budget and was cancelled"
                    );
                }
            }))
            .map(|_detached| ())
    }
}

/// Creates the scheduler handle for one request and the release that starts its tasks.
pub(crate) fn request_tasks(
    executor: AxumOutboundExecutorHandle,
    tracker: AfterResponseTracker,
) -> (AfterResponseHandle, AfterResponseRelease) {
    let (release, delivered) = watch::channel(());
    let handle = AfterResponseHandle::new(AxumAfterResponse {
        delivered,
        executor,
        tracker,
    });
    (handle, AfterResponseRelease(release))
}

#[cfg(test)]
mod tests {
    use super::{AfterResponseTracker, request_tasks};
    use crate::outbound_executor::AxumOutboundExecutor;
    use futures_util::future::pending;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Duration;
    use tokio::task::yield_now;
    use tokio::time::sleep;

    struct DropProbe(Rc<Cell<bool>>);

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tasks_wait_for_release_then_run_and_untrack() {
        let mut owner = AxumOutboundExecutor::new();
        let tracker = AfterResponseTracker::default();
        let (handle, release) = request_tasks(owner.handle(), tracker.clone());
        let ran = Rc::new(Cell::new(false));
        let observed = Rc::clone(&ran);
        handle
            .schedule(Duration::from_secs(5), async move { observed.set(true) })
            .unwrap();
        assert_eq!(tracker.pending(), 1);
        owner
            .run_until(async {
                for _ in 0_u8..8 {
                    yield_now().await;
                }
                assert!(!ran.get(), "a task never starts before its release");
                drop(release);
                tracker.wait_idle().await;
            })
            .await;
        assert!(ran.get());
        assert_eq!(tracker.pending(), 0);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn budget_cancels_a_pending_task() {
        let mut owner = AxumOutboundExecutor::new();
        let tracker = AfterResponseTracker::default();
        let (handle, release) = request_tasks(owner.handle(), tracker.clone());
        let dropped = Rc::new(Cell::new(false));
        let started = Rc::new(Cell::new(false));
        let probe = DropProbe(Rc::clone(&dropped));
        let observed = Rc::clone(&started);
        handle
            .schedule(Duration::from_millis(50), async move {
                let _probe = probe;
                observed.set(true);
                pending::<()>().await;
            })
            .unwrap();
        drop(release);
        owner
            .run_until(async {
                sleep(Duration::from_millis(49)).await;
                assert!(started.get(), "the released task starts");
                assert!(!dropped.get(), "the task survives within its budget");
                tracker.wait_idle().await;
            })
            .await;
        assert!(dropped.get(), "the budget drops the pending task");
        assert_eq!(tracker.pending(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn registration_closes_when_tasks_are_released() {
        let owner = AxumOutboundExecutor::new();
        let tracker = AfterResponseTracker::default();
        let (handle, release) = request_tasks(owner.handle(), tracker.clone());
        drop(release);
        let polled = Rc::new(Cell::new(false));
        let observed = Rc::clone(&polled);
        let result = handle.schedule(Duration::from_secs(1), async move { observed.set(true) });
        assert!(result.is_err(), "a released request accepts no more tasks");
        assert_eq!(tracker.pending(), 0);
        assert!(!polled.get());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn budget_starts_at_release_not_at_registration() {
        let mut owner = AxumOutboundExecutor::new();
        let tracker = AfterResponseTracker::default();
        let (handle, release) = request_tasks(owner.handle(), tracker.clone());
        let ran = Rc::new(Cell::new(false));
        let observed = Rc::clone(&ran);
        handle
            .schedule(Duration::from_millis(50), async move {
                sleep(Duration::from_millis(10)).await;
                observed.set(true);
            })
            .unwrap();
        owner
            .run_until(async {
                // A slow response outlasts the budget before the task may start.
                sleep(Duration::from_millis(100)).await;
                drop(release);
                tracker.wait_idle().await;
            })
            .await;
        assert!(ran.get(), "the budget is measured from release");
    }

    #[test]
    fn spawn_failure_untracks_the_task() {
        let owner = AxumOutboundExecutor::new();
        let tracker = AfterResponseTracker::default();
        let (handle, _release) = request_tasks(owner.handle(), tracker.clone());
        // No Tokio runtime is current, so the executor refuses the spawn.
        assert!(handle.schedule(Duration::from_secs(1), async {}).is_err());
        assert_eq!(tracker.pending(), 0);
    }
}

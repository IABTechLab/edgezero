//! Fastly after-response tasks: queued during one request lifecycle, then driven after the
//! response is sent, inside the same callback scope, each bounded by its budget.

use std::cell::RefCell;
use std::time::Duration;

use edgezero_core::after_response::{AfterResponseHandle, AfterResponseScheduler};
use edgezero_core::error::EdgeError;
#[cfg(feature = "fastly")]
use edgezero_core::time::MonotonicClock;
use futures_util::future::LocalBoxFuture;

type Queued = (Duration, LocalBoxFuture<'static, ()>);

thread_local! {
    /// `Some` while a request lifecycle accepts tasks; `None` otherwise.
    static QUEUE: RefCell<Option<Vec<Queued>>> = const { RefCell::new(None) };
}

/// Request-lifecycle guard. Restores the previously armed queue when dropped.
pub(crate) struct ArmedQueue {
    previous: Option<Vec<Queued>>,
}

struct FastlyAfterResponse;

impl ArmedQueue {
    /// Closes registration and returns the queued tasks; the guard still restores on drop.
    #[expect(
        clippy::unused_self,
        reason = "taking the guard by reference ties draining to an armed lifecycle"
    )]
    pub(crate) fn take(&self) -> Vec<Queued> {
        QUEUE.with(|queue| match queue.try_borrow_mut() {
            Ok(mut queued) => queued.take().unwrap_or_default(),
            Err(_borrowed) => {
                log::warn!("after-response queue was busy; queued tasks were dropped");
                Vec::new()
            }
        })
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
)]
impl Drop for ArmedQueue {
    fn drop(&mut self) {
        let previous = self.previous.take();
        QUEUE.with(|queue| {
            if let Ok(mut queued) = queue.try_borrow_mut() {
                *queued = previous;
            }
        });
    }
}

impl AfterResponseScheduler for FastlyAfterResponse {
    fn schedule(
        &self,
        budget: Duration,
        task: LocalBoxFuture<'static, ()>,
    ) -> Result<(), EdgeError> {
        QUEUE.with(|queue| match queue.try_borrow_mut().as_deref_mut() {
            Ok(Some(queued)) => {
                queued.push((budget, task));
                Ok(())
            }
            Ok(None) | Err(_) => Err(EdgeError::internal(anyhow::anyhow!(
                "after-response tasks can only be registered while the request is handled"
            ))),
        })
    }
}

/// Opens a fresh queue for one request lifecycle.
pub(crate) fn arm() -> ArmedQueue {
    let previous = QUEUE.with(|queue| queue.replace(Some(Vec::new())));
    ArmedQueue { previous }
}

/// Returns the request's handle while a lifecycle is armed.
pub(crate) fn handle_if_armed() -> Option<AfterResponseHandle> {
    QUEUE
        .with(|queue| queue.try_borrow().is_ok_and(|queued| queued.is_some()))
        .then(|| AfterResponseHandle::new(FastlyAfterResponse))
}

/// Drives `tasks` to completion or their budgets inside the current callback scope.
#[cfg(feature = "fastly")]
pub(crate) fn drain(tasks: Vec<Queued>, clock: MonotonicClock) {
    use edgezero_core::time::{DEADLINE_FAR_FUTURE, Deadline};
    use futures::executor;
    use futures_util::future::{Either, join_all, select};

    use crate::outbound::{drive_callback, native_time};

    if tasks.is_empty() {
        return;
    }
    let start = clock.now();
    let time = native_time(clock.clone());
    let bounded = tasks.into_iter().map(|(budget, task)| {
        let deadline = Deadline::at_instant(
            start
                .checked_add(budget.min(DEADLINE_FAR_FUTURE))
                .unwrap_or(start),
        );
        let timer = time.wait_until(deadline);
        async move {
            match select(task, timer).await {
                Either::Left(((), _timer)) => {}
                Either::Right((Ok(()), _task)) => log::warn!(
                    "after-response task exceeded its {budget:?} budget and was cancelled"
                ),
                Either::Right((Err(_error), _task)) => {
                    log::warn!("after-response task timer failed; the task was cancelled");
                }
            }
        }
    });
    if executor::block_on(drive_callback(join_all(bounded), clock, start)).is_err() {
        log::warn!("after-response tasks could not be driven and were cancelled");
    }
}

#[cfg(test)]
mod tests {
    use super::{arm, handle_if_armed};
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Duration;

    #[test]
    fn unarmed_requests_get_no_handle() {
        assert!(handle_if_armed().is_none());
    }

    #[test]
    fn armed_queue_collects_tasks_then_closes_registration() {
        let armed = arm();
        let handle = handle_if_armed().expect("armed lifecycle installs a handle");
        let polled = Rc::new(Cell::new(false));
        let observed = Rc::clone(&polled);
        handle
            .schedule(Duration::from_millis(5), async move { observed.set(true) })
            .unwrap();
        let tasks = armed.take();
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks.first().map(|(budget, _task)| *budget),
            Some(Duration::from_millis(5))
        );
        assert!(
            handle.schedule(Duration::from_secs(1), async {}).is_err(),
            "a task cannot register more tasks once the queue is taken"
        );
        assert!(!polled.get());
        drop(armed);
        assert!(handle_if_armed().is_none());
    }

    #[test]
    fn nested_lifecycle_restores_the_outer_queue() {
        let outer = arm();
        let outer_handle = handle_if_armed().expect("outer handle");
        outer_handle
            .schedule(Duration::from_secs(1), async {})
            .unwrap();
        let inner = arm();
        assert!(inner.take().is_empty(), "the inner lifecycle starts empty");
        drop(inner);
        assert_eq!(
            outer.take().len(),
            1,
            "the outer queue survives the inner lifecycle"
        );
    }
}

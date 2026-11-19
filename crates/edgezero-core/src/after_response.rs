//! Portable after-response tasks.
//!
//! A handler registers a task through [`RequestContext::after_response`](crate::context::RequestContext::after_response).
//! The adapter starts it once the request's response attempt has ended at the adapter's terminal
//! egress boundary, and cancels it when its budget runs out. Adapters without support install no
//! handle, so registration fails with [`EdgeError::NotImplemented`].

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::LocalBoxFuture;

use crate::error::EdgeError;

/// Message returned when the adapter installed no after-response scheduler for the request.
pub const AFTER_RESPONSE_UNSUPPORTED: &str =
    "after-response tasks are not supported by this adapter";

/// Adapter-owned scheduler for one request's after-response tasks.
///
/// The scheduler is shared through request extensions, so it is `Send + Sync`. The tasks are local
/// futures; each adapter keeps them on the thread that owns the request.
pub trait AfterResponseScheduler: Send + Sync {
    /// Queues `task` to start after the response attempt ends, cancelled once `budget` elapses.
    ///
    /// # Errors
    /// Returns an error when the adapter cannot accept the task. The task is dropped unpolled.
    fn schedule(
        &self,
        budget: Duration,
        task: LocalBoxFuture<'static, ()>,
    ) -> Result<(), EdgeError>;
}

/// Cloneable handle that adapters insert into request extensions.
#[derive(Clone)]
pub struct AfterResponseHandle(Arc<dyn AfterResponseScheduler>);

impl AfterResponseHandle {
    /// Wraps an adapter scheduler.
    #[must_use]
    #[inline]
    pub fn new<S>(scheduler: S) -> Self
    where
        S: AfterResponseScheduler + 'static,
    {
        Self(Arc::new(scheduler))
    }

    /// Registers `task` with a positive `budget`.
    ///
    /// # Errors
    /// Returns an internal error for a zero budget, or the adapter's scheduling error.
    #[inline]
    pub fn schedule<F>(&self, budget: Duration, task: F) -> Result<(), EdgeError>
    where
        F: Future<Output = ()> + 'static,
    {
        if budget.is_zero() {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "after-response task budget must be positive"
            )));
        }
        self.0.schedule(budget, Box::pin(task))
    }
}

impl fmt::Debug for AfterResponseHandle {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AfterResponseHandle")
            .finish_non_exhaustive()
    }
}

/// Registers `task` through `handle`, or reports the portable capability error.
pub(crate) fn schedule_with<F>(
    handle: Option<&AfterResponseHandle>,
    budget: Duration,
    task: F,
) -> Result<(), EdgeError>
where
    F: Future<Output = ()> + 'static,
{
    handle.map_or_else(
        || Err(EdgeError::not_implemented(AFTER_RESPONSE_UNSUPPORTED)),
        |scheduler| scheduler.schedule(budget, task),
    )
}

#[cfg(test)]
mod tests {
    use super::{AFTER_RESPONSE_UNSUPPORTED, AfterResponseHandle, AfterResponseScheduler};
    use crate::context::RequestContext;
    use crate::error::EdgeError;
    use crate::http::{Method, StatusCode, request_builder};
    use crate::{Body, params::PathParams};
    use futures::executor::block_on;
    use futures_util::future::LocalBoxFuture;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

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

    #[derive(Default)]
    struct Recording {
        budgets: Mutex<Vec<Duration>>,
    }

    thread_local! {
        static RECORDED: RefCell<Vec<LocalBoxFuture<'static, ()>>> =
            const { RefCell::new(Vec::new()) };
    }

    impl AfterResponseScheduler for Arc<Recording> {
        #[inline]
        fn schedule(
            &self,
            budget: Duration,
            task: LocalBoxFuture<'static, ()>,
        ) -> Result<(), EdgeError> {
            if let Ok(mut budgets) = self.budgets.lock() {
                budgets.push(budget);
            }
            RECORDED.with(|recorded| recorded.borrow_mut().push(task));
            Ok(())
        }
    }

    fn context_with(scheduler: Option<AfterResponseHandle>) -> RequestContext {
        let mut request = request_builder()
            .method(Method::GET)
            .uri("/")
            .body(Body::empty())
            .unwrap();
        if let Some(handle) = scheduler {
            request.extensions_mut().insert(handle);
        }
        RequestContext::new(request, PathParams::default())
    }

    #[test]
    fn missing_scheduler_reports_not_implemented_and_never_polls() {
        let ctx = context_with(None);
        let polled = Rc::new(Cell::new(false));
        let dropped = Rc::new(Cell::new(false));
        let probe = DropProbe(Rc::clone(&dropped));
        let observed = Rc::clone(&polled);
        let error = ctx
            .after_response(Duration::from_secs(1), async move {
                let _probe = probe;
                observed.set(true);
            })
            .unwrap_err();
        assert_eq!(error.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(error.message(), AFTER_RESPONSE_UNSUPPORTED);
        assert!(!polled.get(), "an unsupported task is never polled");
        assert!(
            dropped.get(),
            "an unsupported task is dropped at registration"
        );
    }

    #[test]
    fn zero_budget_is_rejected_before_the_scheduler() {
        let recording = Arc::new(Recording::default());
        let ctx = context_with(Some(AfterResponseHandle::new(Arc::clone(&recording))));
        let error = ctx.after_response(Duration::ZERO, async {}).unwrap_err();
        assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(recording.budgets.lock().unwrap().is_empty());
    }

    #[test]
    fn scheduler_receives_the_exact_budget_and_a_runnable_task() {
        let recording = Arc::new(Recording::default());
        let ctx = context_with(Some(AfterResponseHandle::new(Arc::clone(&recording))));
        let ran = Rc::new(Cell::new(false));
        let observed = Rc::clone(&ran);
        ctx.after_response(Duration::from_millis(250), async move {
            observed.set(true);
        })
        .unwrap();
        assert_eq!(
            *recording.budgets.lock().unwrap(),
            [Duration::from_millis(250)]
        );
        assert!(!ran.get(), "registration does not run the task");
        let task = RECORDED
            .with(|recorded| recorded.borrow_mut().pop())
            .unwrap();
        block_on(task);
        assert!(ran.get());
    }
}

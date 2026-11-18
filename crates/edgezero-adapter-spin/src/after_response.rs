//! Spin's after-response capability error.
//!
//! Spin does not run after-response tasks. Every converted request carries a scheduler that
//! rejects them with a `501` naming the Spin adapter, instead of core's generic error.

use std::time::Duration;

use edgezero_core::after_response::{AfterResponseHandle, AfterResponseScheduler};
use edgezero_core::error::EdgeError;
use futures_util::future::LocalBoxFuture;

struct SpinAfterResponseUnsupported;

impl AfterResponseScheduler for SpinAfterResponseUnsupported {
    #[inline]
    fn schedule(
        &self,
        _budget: Duration,
        task: LocalBoxFuture<'static, ()>,
    ) -> Result<(), EdgeError> {
        drop(task);
        Err(EdgeError::not_implemented(
            crate::AFTER_RESPONSE_UNSUPPORTED,
        ))
    }
}

/// Handle inserted into every Spin request so registration fails with the Spin capability error.
pub(crate) fn unsupported_handle() -> AfterResponseHandle {
    AfterResponseHandle::new(SpinAfterResponseUnsupported)
}

#[cfg(test)]
mod tests {
    use super::unsupported_handle;
    use crate::AFTER_RESPONSE_UNSUPPORTED;
    use edgezero_core::http::StatusCode;
    use std::cell::Cell;
    use std::rc::Rc;
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

    #[test]
    fn registration_reports_spin_capability_error_and_drops_task_unpolled() {
        let polled = Rc::new(Cell::new(false));
        let dropped = Rc::new(Cell::new(false));
        let probe = DropProbe(Rc::clone(&dropped));
        let observed = Rc::clone(&polled);

        let error = unsupported_handle()
            .schedule(Duration::from_secs(1), async move {
                let _probe = probe;
                observed.set(true);
            })
            .unwrap_err();

        assert_eq!(error.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(error.message(), AFTER_RESPONSE_UNSUPPORTED);
        assert!(!polled.get(), "a rejected task is never polled");
        assert!(dropped.get(), "a rejected task is dropped at registration");
    }
}

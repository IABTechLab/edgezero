use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures_util::future::LocalBoxFuture;

use super::{IndependentOutboundProgress, OutboundExchangeOwner, OutboundScope, Revocable};
use crate::error::EdgeError;

type Work = Rc<Revocable<LocalBoxFuture<'static, ()>>>;

/// SDK-neutral local runner and cancellation ownership for one outbound producer.
/// Dropping either the runner or its paired owner promptly releases the owned future,
/// even if the other side is retained. Scheduling remains entirely provider-owned.
pub struct OutboundTask {
    work: Option<Work>,
}

impl OutboundTask {
    /// Owns a producer without polling or spawning it, using the shared revocation protocol.
    ///
    /// # Errors
    /// Returns the unchanged scope-registration failure after dropping the input. Providers must
    /// complete this construction before spawning or performing any SDK activity.
    #[inline]
    pub fn new(
        future: LocalBoxFuture<'static, ()>,
    ) -> Result<(Self, OutboundExchangeOwner), EdgeError> {
        let scope = OutboundScope::capture(Rc::new(IndependentOutboundProgress));
        let work = Rc::new(Revocable::new(&scope, future)?);
        let cancellation = Rc::clone(&work);
        let owner = OutboundExchangeOwner::new(move || {
            cancellation.revoke();
        });
        Ok((Self { work: Some(work) }, owner))
    }
}

impl fmt::Debug for OutboundTask {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboundTask")
            .field(
                "active",
                &self.work.as_ref().is_some_and(|work| !work.is_revoked()),
            )
            .finish_non_exhaustive()
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for OutboundTask {
    #[inline]
    fn drop(&mut self) {
        if let Some(work) = self.work.take() {
            work.revoke();
        }
    }
}

impl Future for OutboundTask {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(work) = this.work.as_ref().map(Rc::clone) else {
            return Poll::Ready(());
        };
        match work.with(cx, |future, inner_cx| future.as_mut().poll(inner_cx)) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(())) | Err(_) => {
                work.revoke();
                drop(this.work.take());
                Poll::Ready(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::future::{Future as _, pending, poll_fn};
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::mpsc::{TryRecvError, channel};
    use std::task::{Context, Poll, Wake, Waker};

    use super::OutboundTask;
    use crate::error::EdgeError;
    use crate::outbound::{OutboundExchangeOwner, OutboundScopeOwner};

    struct Release(Rc<Cell<usize>>);

    struct ReentrantWake;

    std::thread_local! {
        static DROP_ACTION: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
        static WAKE_ACTION: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    }

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for ReentrantWake {
        fn drop(&mut self) {
            let action = DROP_ACTION
                .try_with(|stored| stored.borrow_mut().take())
                .ok()
                .flatten();
            if let Some(callback) = action {
                callback();
            }
        }
    }

    impl Wake for ReentrantWake {
        fn wake(self: Arc<Self>) {
            Self::wake_by_ref(&self);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let action = WAKE_ACTION
                .try_with(|stored| stored.borrow_mut().take())
                .ok()
                .flatten();
            if let Some(callback) = action {
                callback();
            }
        }
    }

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_add(1));
        }
    }

    #[test]
    fn executor_drop_releases_unpolled_future_and_sender_even_with_retained_owner() {
        let releases = Rc::new(Cell::new(0_usize));
        let lease = Release(Rc::clone(&releases));
        let (sender, receiver) = channel::<()>();
        let (task, owner) = OutboundTask::new(Box::pin(async move {
            let _owned = (lease, sender);
            pending::<()>().await;
        }))
        .expect("task registration");
        drop(task);
        assert_eq!(releases.get(), 1);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
        drop(owner);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn owner_drop_cancels_before_first_poll_and_runner_remains_terminal() {
        let releases = Rc::new(Cell::new(0_usize));
        let lease = Release(Rc::clone(&releases));
        let (mut task, owner) = OutboundTask::new(Box::pin(async move {
            let _owned = lease;
            panic!("cancelled input must not be polled");
        }))
        .expect("task registration");
        drop(owner);
        assert_eq!(releases.get(), 1);
        for _ in 0_u8..2 {
            assert!(
                Pin::new(&mut task)
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_ready()
            );
        }
        drop(task);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn closed_scope_rejects_registration_before_spawn_and_releases_input() {
        let scope_owner = OutboundScopeOwner::callback();
        scope_owner.close();
        let releases = Rc::new(Cell::new(0_usize));
        let lease = Release(Rc::clone(&releases));
        let (sender, receiver) = channel::<()>();
        let refused = scope_owner.with(|| {
            OutboundTask::new(Box::pin(async move {
                let _owned = (lease, sender);
                panic!("refused task cannot be polled or spawned");
            }))
        });
        assert!(matches!(refused, Err(EdgeError::Internal { .. })));
        assert_eq!(releases.get(), 1);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn completion_releases_input_before_ready_even_with_retained_runner_and_owner() {
        let releases = Rc::new(Cell::new(0_usize));
        let lease = Release(Rc::clone(&releases));
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let (mut task, owner) = OutboundTask::new(Box::pin(poll_fn(move |_| {
            let _retain = &lease;
            observed.set(observed.get().saturating_add(1));
            Poll::Ready(())
        })))
        .expect("task registration");
        for _ in 0_u8..2 {
            assert!(
                Pin::new(&mut task)
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_ready()
            );
            assert_eq!(releases.get(), 1);
        }
        assert_eq!(polls.get(), 1);
        drop((task, owner));
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn cancellation_inside_pending_or_ready_poll_never_restores_the_active_future() {
        for ready in [false, true] {
            let cancellation = Rc::new(RefCell::new(None::<OutboundExchangeOwner>));
            let cancel_from_poll = Rc::downgrade(&cancellation);
            let releases = Rc::new(Cell::new(0_usize));
            let lease = Release(Rc::clone(&releases));
            let polls = Rc::new(Cell::new(0_usize));
            let observed = Rc::clone(&polls);
            let (mut task, owner) = OutboundTask::new(Box::pin(poll_fn(move |_| {
                let _retain = &lease;
                observed.set(observed.get().saturating_add(1));
                let slot = cancel_from_poll.upgrade().expect("cancellation slot");
                let owner_to_drop = slot.borrow_mut().take();
                drop(owner_to_drop);
                if ready {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })))
            .expect("task registration");
            cancellation.replace(Some(owner));
            for _ in 0_u8..2 {
                assert!(
                    Pin::new(&mut task)
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_ready()
                );
            }
            assert_eq!(polls.get(), 1);
            assert_eq!(releases.get(), 1);
        }
    }

    #[test]
    fn cancellation_wake_can_reentrantly_poll_the_retained_runner() {
        let releases = Rc::new(Cell::new(0_usize));
        let lease = Release(Rc::clone(&releases));
        let (task, owner) = OutboundTask::new(Box::pin(async move {
            let _retain = lease;
            pending::<()>().await;
        }))
        .expect("task registration");
        let retained = Rc::new(RefCell::new(task));
        let waiting = Waker::from(Arc::new(ReentrantWake));
        assert!(
            Pin::new(&mut *retained.borrow_mut())
                .poll(&mut Context::from_waker(&waiting))
                .is_pending()
        );
        let target = Rc::downgrade(&retained);
        let wakes = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&wakes);
        WAKE_ACTION.with(|stored| {
            stored.replace(Some(Box::new(move || {
                observed.set(observed.get().saturating_add(1));
                let runner = target.upgrade().expect("retained runner");
                assert!(
                    Pin::new(&mut *runner.borrow_mut())
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_ready()
                );
            })))
        });
        drop(owner);
        assert_eq!(wakes.get(), 1);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn replaced_waker_drop_can_cancel_without_borrowing_or_repolling_input() {
        let releases = Rc::new(Cell::new(0_usize));
        let lease = Release(Rc::clone(&releases));
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let (mut task, owner) = OutboundTask::new(Box::pin(poll_fn(move |_| {
            let _retain = &lease;
            observed.set(observed.get().saturating_add(1));
            Poll::Pending
        })))
        .expect("task registration");
        let waiting = Waker::from(Arc::new(ReentrantWake));
        assert!(
            Pin::new(&mut task)
                .poll(&mut Context::from_waker(&waiting))
                .is_pending()
        );
        drop(waiting);
        DROP_ACTION.with(|stored| stored.replace(Some(Box::new(move || drop(owner)))));
        assert!(
            Pin::new(&mut task)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        assert_eq!(polls.get(), 1);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn scope_close_cancels_unpolled_task_while_both_sdk_handles_are_retained() {
        let scope_owner = OutboundScopeOwner::callback();
        let releases = Rc::new(Cell::new(0_usize));
        let lease = Release(Rc::clone(&releases));
        let (mut task, owner) = scope_owner
            .with(|| {
                OutboundTask::new(Box::pin(async move {
                    let _retain = lease;
                    pending::<()>().await;
                }))
            })
            .expect("task registration");
        scope_owner.close();
        assert_eq!(releases.get(), 1);
        assert!(
            Pin::new(&mut task)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        drop((task, owner));
        assert_eq!(releases.get(), 1);
    }
}

use edgezero_core::{
    EdgeError, MonotonicInstant, OutboundProgressRuntime, OutboundScope, OutboundTime,
};
use futures_util::future::LocalBoxFuture;
use std::cell::RefCell;
use std::future::Future;
use std::rc::{Rc, Weak};
use std::sync::{Arc, Weak as SyncWeak};
use std::task::{Context, Poll};
use std::thread::{ThreadId, current};
use std::thread_local;
use tokio::runtime::Handle;
use tokio::task::{JoinHandle, LocalSet};

thread_local! {
    // Only weak references cross the client/owner boundary; clients cannot keep tasks alive.
    static OWNERS: RefCell<Vec<(SyncWeak<()>, Weak<LocalSet>)>> = const { RefCell::new(Vec::new()) };
}

/// Owns the single local task set used by a native outbound client.
///
/// Drive this owner on its creating thread inside a Tokio runtime with I/O and
/// time enabled. Dropping it cancels its spawned work and revokes every handle.
pub struct AxumOutboundExecutor {
    identity: Arc<()>,
    local: Rc<LocalSet>,
}

/// Sendable identity of a live, thread-affine outbound executor.
///
/// Moving a client does not move its executor: use from another thread fails
/// before polling request work. A handle never extends the executor's lifetime.
#[derive(Clone)]
pub struct AxumOutboundExecutorHandle {
    identity: Arc<()>,
    thread: ThreadId,
}

impl AxumOutboundExecutor {
    /// Returns a revocable handle to this executor.
    #[must_use]
    #[inline]
    pub fn handle(&self) -> AxumOutboundExecutorHandle {
        AxumOutboundExecutorHandle {
            identity: Arc::clone(&self.identity),
            thread: current().id(),
        }
    }

    /// Creates an owner without creating a runtime or spawning background work.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        let identity = Arc::new(());
        let local = Rc::new(LocalSet::new());
        OWNERS.with(|owners| {
            owners
                .borrow_mut()
                .push((Arc::downgrade(&identity), Rc::downgrade(&local)));
        });
        Self { identity, local }
    }

    /// Drives this task set until `future` completes; queued tasks remain owned
    /// until driven again or this owner is dropped.
    #[inline]
    pub async fn run_until<F>(&mut self, future: F) -> F::Output
    where
        F: Future,
    {
        self.local.run_until(future).await
    }
}

impl Default for AxumOutboundExecutor {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "Drop::pin_drop is unstable and conflicts with drop"
)]
impl Drop for AxumOutboundExecutor {
    #[inline]
    fn drop(&mut self) {
        let _unavailable = OWNERS.try_with(|owners| {
            if let Ok(mut entries) = owners.try_borrow_mut() {
                entries
                    .retain(|(identity, _local)| !identity.ptr_eq(&Arc::downgrade(&self.identity)));
            }
        });
        // No registry borrow survives destruction of LocalSet and its task sources.
    }
}

impl AxumOutboundExecutorHandle {
    pub(crate) fn check(&self) -> Result<(), EdgeError> {
        self.resolve().map(|_local| ())
    }

    fn resolve(&self) -> Result<Rc<LocalSet>, EdgeError> {
        if current().id() != self.thread {
            return Err(unavailable());
        }
        OWNERS
            .try_with(|owners| {
                let entries = owners.try_borrow().map_err(|_borrowed| unavailable())?;
                entries
                    .iter()
                    .find_map(|(identity, local)| {
                        identity
                            .ptr_eq(&Arc::downgrade(&self.identity))
                            .then(|| local.upgrade())
                            .flatten()
                    })
                    .ok_or_else(unavailable)
            })
            .map_err(|_closed| unavailable())?
    }

    pub(crate) fn spawn(
        &self,
        work: LocalBoxFuture<'static, ()>,
    ) -> Result<JoinHandle<()>, EdgeError> {
        let local = self.resolve()?;
        Handle::try_current().map_err(|_missing| unavailable())?;
        Ok(local.spawn_local(work))
    }
}

impl OutboundProgressRuntime for AxumOutboundExecutorHandle {
    #[inline]
    fn poll(
        &self,
        _scope: &OutboundScope,
        _time: &OutboundTime,
        _entered_at: MonotonicInstant,
        cx: &mut Context<'_>,
        inner: &mut dyn FnMut(&mut Context<'_>) -> Poll<()>,
    ) -> Poll<Result<(), EdgeError>> {
        if let Err(error) = self.check() {
            return Poll::Ready(Err(error));
        }
        inner(cx).map(Ok)
    }
}

fn unavailable() -> EdgeError {
    EdgeError::internal(anyhow::anyhow!(
        "outbound executor is unavailable on this thread"
    ))
}

#[cfg(test)]
mod tests {
    use super::AxumOutboundExecutor;
    use edgezero_core::EdgeError;
    use futures_util::future::{FutureExt as _, pending};
    use std::cell::Cell;
    use std::rc::Rc;
    use std::thread::spawn;
    use tokio::task::yield_now;

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_owner_schedules_without_an_ambient_local_set() {
        let mut owner = AxumOutboundExecutor::new();
        let handle = owner.handle();
        let polled = Rc::new(Cell::new(false));
        let observed = Rc::clone(&polled);
        let task = handle
            .spawn(Box::pin(async move { observed.set(true) }))
            .unwrap();
        assert!(!polled.get());
        owner.run_until(task).await.unwrap();
        assert!(polled.get());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn owner_drop_revokes_handles_and_releases_spawned_work() {
        let mut owner = AxumOutboundExecutor::new();
        let handle = owner.handle();
        let source = Rc::new(());
        let released = Rc::downgrade(&source);
        let task = handle
            .spawn(Box::pin(async move {
                assert_eq!(Rc::strong_count(&source), 1);
                pending::<()>().await;
            }))
            .unwrap();
        owner.run_until(yield_now()).await;
        assert!(released.upgrade().is_some());
        drop(owner);
        assert!(released.upgrade().is_none());
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(matches!(handle.check(), Err(EdgeError::Internal { .. })));
    }

    #[test]
    fn foreign_thread_cannot_resolve_a_live_owner() {
        let owner = AxumOutboundExecutor::new();
        let handle = owner.handle();
        spawn(move || {
            assert!(matches!(handle.check(), Err(EdgeError::Internal { .. })));
        })
        .join()
        .unwrap();
        owner.handle().check().unwrap();
    }

    #[test]
    fn live_owner_without_a_runtime_releases_work_without_polling() {
        let owner = AxumOutboundExecutor::new();
        let source = Rc::new(());
        let released = Rc::downgrade(&source);
        owner
            .handle()
            .spawn(Box::pin(async move {
                assert_eq!(Rc::strong_count(&source), 1);
                panic!("missing runtime must not poll work");
            }))
            .unwrap_err();
        assert!(released.upgrade().is_none());
    }

    #[test]
    fn stale_handle_cannot_resolve_another_owner() {
        let stale = AxumOutboundExecutor::new().handle();
        let live = AxumOutboundExecutor::new();
        assert!(stale.check().is_err());
        live.handle().check().unwrap();
    }

    #[test]
    fn invalid_owner_rejects_and_drops_work_without_polling() {
        let handle = AxumOutboundExecutor::new().handle();
        let source = Rc::new(());
        let released = Rc::downgrade(&source);
        let task = async move {
            assert_eq!(Rc::strong_count(&source), 1);
            panic!("invalid ownership must not poll work");
        }
        .boxed_local();
        handle.spawn(task).unwrap_err();
        assert!(released.upgrade().is_none());
    }
}

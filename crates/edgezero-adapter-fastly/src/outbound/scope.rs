//! Revocable ownership of complete batch drivers, independent of transport scheduling.
//! Registration happens at batch creation; polling never reparents callback work into egress.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::mem;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll, Waker};

use edgezero_core::{EdgeError, OutboundBatchDriverEvent};
use futures_util::StreamExt as _;
use futures_util::stream::{LocalBoxStream, Stream, once};

std::thread_local! {
    static ACTIVE: RefCell<Option<Weak<Scope>>> = const { RefCell::new(None) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Owner {
    Callback,
    Response,
}

#[derive(Debug, thiserror::Error)]
#[error("Fastly outbound {owner:?} scope closed")]
struct ScopeClosed {
    owner: Owner,
}

struct Scope {
    closed: Cell<bool>,
    drivers: RefCell<BTreeMap<u64, Weak<RefCell<Driver>>>>,
    next_id: Cell<u64>,
    owner: Owner,
}

struct Driver {
    cancelled: Option<Owner>,
    stream: Option<LocalBoxStream<'static, OutboundBatchDriverEvent>>,
    waker: Option<Waker>,
}

pub(crate) struct Guard {
    previous: Option<Weak<Scope>>,
    scope: Rc<Scope>,
}

struct RestoreActive(Option<Weak<Scope>>);

struct ScopedBatch {
    driver: Rc<RefCell<Driver>>,
    registration: Option<(Weak<Scope>, u64)>,
}

impl Guard {
    pub(crate) fn callback() -> Self {
        Self::enter(Owner::Callback)
    }

    fn enter(owner: Owner) -> Self {
        let closed = ACTIVE.with(|active| {
            active
                .borrow()
                .as_ref()
                .and_then(Weak::upgrade)
                .is_some_and(|scope| scope.closed.get())
        });
        let scope = Rc::new(Scope {
            closed: Cell::new(closed),
            drivers: RefCell::new(BTreeMap::new()),
            next_id: Cell::new(0),
            owner,
        });
        let previous = ACTIVE.with(|active| active.replace(Some(Rc::downgrade(&scope))));
        Self { previous, scope }
    }

    pub(crate) fn fallback() -> Option<Self> {
        let active = ACTIVE.with(|current| current.borrow().as_ref().and_then(Weak::upgrade));
        active.is_none().then(Self::callback)
    }

    pub(crate) fn response() -> Self {
        Self::enter(Owner::Response)
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for Guard {
    fn drop(&mut self) {
        let _restore = RestoreActive(self.previous.take());
        self.scope.closed.set(true);
        // Revoke every root before any destructor/wakeup can observe a sibling or start work.
        let drivers = mem::take(&mut *self.scope.drivers.borrow_mut());
        let mut cleanup = Vec::with_capacity(drivers.len());
        for driver in drivers.into_values().filter_map(|driver| driver.upgrade()) {
            let mut root = driver.borrow_mut();
            root.cancelled = Some(self.scope.owner);
            cleanup.push((root.stream.take(), root.waker.take()));
        }
        for (stream, waiting) in cleanup {
            drop(stream);
            if let Some(waker) = waiting {
                waker.wake();
            }
        }
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for RestoreActive {
    fn drop(&mut self) {
        ACTIVE.with(|active| active.replace(self.0.take()));
    }
}

impl ScopedBatch {
    fn unregister(&mut self) {
        if let Some((weak, id)) = self.registration.take()
            && let Some(scope) = weak.upgrade()
        {
            scope.drivers.borrow_mut().remove(&id);
        }
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for ScopedBatch {
    fn drop(&mut self) {
        self.unregister();
    }
}

impl Stream for ScopedBatch {
    type Item = OutboundBatchDriverEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let observation = {
            let mut driver = this.driver.borrow_mut();
            if let Some(owner) = driver.cancelled.take() {
                Poll::Ready(Some(OutboundBatchDriverEvent::Failed(EdgeError::internal(
                    ScopeClosed { owner },
                ))))
            } else if let Some(stream) = &mut driver.stream {
                stream.as_mut().poll_next(cx)
            } else {
                Poll::Ready(None)
            }
        };
        let terminal = matches!(
            observation,
            Poll::Ready(
                None | Some(OutboundBatchDriverEvent::Cutoff | OutboundBatchDriverEvent::Failed(_))
            )
        );
        if terminal {
            this.unregister();
            let stream = this.driver.borrow_mut().stream.take();
            drop(stream);
        }
        let mut replacement = observation.is_pending().then(|| cx.waker().clone());
        let previous = {
            let mut driver = this.driver.borrow_mut();
            if driver.cancelled.is_some() || driver.stream.is_none() {
                driver.waker.take()
            } else {
                mem::replace(&mut driver.waker, replacement.take())
            }
        };
        drop(previous);
        drop(replacement);
        // Waker cloning/destruction can close the scope before registering a wakeup.
        if observation.is_pending() {
            let cancelled = this.driver.borrow_mut().cancelled.take();
            if let Some(owner) = cancelled {
                this.unregister();
                return Poll::Ready(Some(OutboundBatchDriverEvent::Failed(EdgeError::internal(
                    ScopeClosed { owner },
                ))));
            }
        }
        observation
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

pub(crate) fn bind_batch<DriverStream>(
    stream: DriverStream,
) -> LocalBoxStream<'static, OutboundBatchDriverEvent>
where
    DriverStream: Stream<Item = OutboundBatchDriverEvent> + 'static,
{
    let Some(scope) = ACTIVE.with(|active| active.borrow().as_ref().and_then(Weak::upgrade)) else {
        return stream.boxed_local();
    };
    if scope.closed.get() {
        drop(stream);
        let owner = scope.owner;
        return once(async move {
            OutboundBatchDriverEvent::Failed(EdgeError::internal(ScopeClosed { owner }))
        })
        .boxed_local();
    }
    let id = scope.next_id.get();
    let Some(next_id) = id.checked_add(1) else {
        drop(stream);
        return once(async {
            OutboundBatchDriverEvent::Failed(EdgeError::internal(anyhow::anyhow!(
                "Fastly outbound scope registration exhausted"
            )))
        })
        .boxed_local();
    };
    scope.next_id.set(next_id);
    let driver = Rc::new(RefCell::new(Driver {
        cancelled: None,
        stream: Some(stream.boxed_local()),
        waker: None,
    }));
    scope
        .drivers
        .borrow_mut()
        .insert(id, Rc::downgrade(&driver));
    ScopedBatch {
        driver,
        registration: Some((Rc::downgrade(&scope), id)),
    }
    .boxed_local()
}

pub(super) fn ensure_open() -> Result<(), EdgeError> {
    let active = ACTIVE.with(|current| current.borrow().as_ref().and_then(Weak::upgrade));
    if let Some(scope) = active
        && scope.closed.get()
    {
        return Err(EdgeError::internal(ScopeClosed { owner: scope.owner }));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::future::Future as _;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use async_stream::stream;
    use edgezero_core::{
        EdgeError, OutboundBatch, OutboundBatchDriverEvent, OutboundBatchItem, OutboundBatchNext,
        OutboundBatchTermination, OutboundSlotResult,
    };
    use futures::executor::block_on;
    use futures_util::StreamExt as _;
    use futures_util::future::pending;
    use futures_util::stream::{LocalBoxStream, iter, pending as pending_stream, poll_fn};
    use futures_util::task::{ArcWake, waker, waker_ref};

    use super::{Guard, Owner, ScopeClosed, bind_batch, ensure_open};

    struct Release(Rc<Cell<usize>>);

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    std::thread_local! {
        static CLOSE_ON_WAKER_DROP: RefCell<Option<Guard>> = const { RefCell::new(None) };
    }

    struct CloseOnDrop;

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for CloseOnDrop {
        fn drop(&mut self) {
            let scope = CLOSE_ON_WAKER_DROP.with(|saved| saved.borrow_mut().take());
            drop(scope);
        }
    }

    impl ArcWake for CloseOnDrop {
        fn wake(self: Arc<Self>) {
            Self::wake_by_ref(&self);
        }

        fn wake_by_ref(_arc_self: &Arc<Self>) {}
    }

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_add(1));
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

    fn closed(error: &EdgeError, expected: Owner) {
        let EdgeError::Internal { source, .. } = error else {
            panic!("expected internal scope closure, got {error:?}");
        };
        let reason = source.downcast_ref::<ScopeClosed>().expect("typed closure");
        assert_eq!(reason.owner, expected);
    }

    fn pending_driver(
        released: Rc<Cell<usize>>,
    ) -> LocalBoxStream<'static, OutboundBatchDriverEvent> {
        let owner = Release(released);
        stream! {
            let _owner = owner;
            yield pending::<OutboundBatchDriverEvent>().await;
        }
        .boxed_local()
    }

    fn item(index: usize) -> OutboundBatchDriverEvent {
        OutboundBatchDriverEvent::Item(OutboundBatchItem::new(
            index,
            OutboundSlotResult::new(Duration::ZERO, Err(EdgeError::bad_request("slot fixture"))),
        ))
    }

    #[test]
    fn closing_scope_releases_unpolled_driver_without_polling() {
        let released = Rc::new(Cell::new(0));
        let scope = Guard::callback();
        let mut driver = bind_batch(pending_driver(Rc::clone(&released)));
        assert_eq!(scope.scope.drivers.borrow().len(), 1);
        drop(scope);
        assert_eq!(released.get(), 1);
        let Some(OutboundBatchDriverEvent::Failed(error)) = block_on(driver.next()) else {
            panic!("scope closure must not become cutoff or EOF");
        };
        closed(&error, Owner::Callback);
        assert!(block_on(driver.next()).is_none());
        drop(driver);
        assert_eq!(released.get(), 1);
    }

    #[test]
    fn closing_scope_wakes_pending_consumer_after_releasing_driver() {
        let released = Rc::new(Cell::new(0));
        let scope = Guard::callback();
        let mut driver = bind_batch(pending_driver(Rc::clone(&released)));
        let counter = Arc::new(WakeCount::default());
        let waker = waker_ref(&counter);
        assert!(
            driver
                .as_mut()
                .poll_next(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(scope);
        assert_eq!(released.get(), 1);
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            block_on(driver.next()),
            Some(OutboundBatchDriverEvent::Failed(_))
        ));
    }

    #[test]
    fn replacing_waker_allows_reentrant_scope_close() {
        let released = Rc::new(Cell::new(0));
        let scope = Guard::callback();
        CLOSE_ON_WAKER_DROP.with(|saved| *saved.borrow_mut() = Some(scope));
        let mut driver = bind_batch(pending_driver(Rc::clone(&released)));
        let original = waker(Arc::new(CloseOnDrop));
        assert!(
            driver
                .as_mut()
                .poll_next(&mut Context::from_waker(&original))
                .is_pending()
        );
        drop(original);
        let counter = Arc::new(WakeCount::default());
        let replacement = waker_ref(&counter);
        assert!(matches!(
            driver
                .as_mut()
                .poll_next(&mut Context::from_waker(&replacement)),
            Poll::Ready(Some(OutboundBatchDriverEvent::Failed(_)))
        ));
        assert_eq!(released.get(), 1);
        assert!(block_on(driver.next()).is_none());
    }

    #[test]
    fn closing_scope_revokes_siblings_before_reentrant_cleanup() {
        struct Cleanup {
            created: Rc<RefCell<Option<LocalBoxStream<'static, OutboundBatchDriverEvent>>>>,
            rejected: Rc<Cell<bool>>,
            released: Rc<Cell<usize>>,
            sibling: Rc<RefCell<Option<LocalBoxStream<'static, OutboundBatchDriverEvent>>>>,
        }

        #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
        impl Drop for Cleanup {
            fn drop(&mut self) {
                self.rejected.set(ensure_open().is_err());
                let mut saved = self.sibling.borrow_mut().take().expect("sibling driver");
                drop(block_on(saved.next()));
                *self.created.borrow_mut() =
                    Some(bind_batch(pending_driver(Rc::clone(&self.released))));
            }
        }

        let scope = Guard::callback();
        let created = Rc::new(RefCell::new(None));
        let sibling = Rc::new(RefCell::new(None));
        let released = Rc::new(Cell::new(0));
        let rejected = Rc::new(Cell::new(false));
        let cleanup = Cleanup {
            created: Rc::clone(&created),
            rejected: Rc::clone(&rejected),
            released: Rc::clone(&released),
            sibling: Rc::clone(&sibling),
        };
        let _first = bind_batch(poll_fn(move |_cx| {
            let _keep_cleanup = &cleanup;
            Poll::<Option<OutboundBatchDriverEvent>>::Pending
        }));
        let reads = Rc::new(Cell::new(0_usize));
        let captured_reads = Rc::clone(&reads);
        *sibling.borrow_mut() = Some(bind_batch(poll_fn(move |_cx| {
            captured_reads.set(captured_reads.get().saturating_add(1));
            Poll::Ready(Some(item(0)))
        })));
        drop(scope);
        assert!(
            rejected.get(),
            "dispatch must reject before preparation or I/O"
        );
        assert_eq!(reads.get(), 0, "cleanup cannot poll a sibling's source");
        assert_eq!(released.get(), 1, "cleanup-created work cannot escape");
        let mut retained = created.borrow_mut().take().expect("reentrant driver");
        assert!(matches!(
            block_on(retained.next()),
            Some(OutboundBatchDriverEvent::Failed(_))
        ));
    }

    #[test]
    fn driver_drop_and_terminal_unregister_without_accumulating_entries() {
        let scope = Guard::callback();
        for _ in 0_u8..32 {
            let released = Rc::new(Cell::new(0));
            let driver = bind_batch(pending_driver(Rc::clone(&released)));
            assert_eq!(scope.scope.drivers.borrow().len(), 1);
            drop(driver);
            assert_eq!(released.get(), 1);
            assert!(scope.scope.drivers.borrow().is_empty());
        }
        let batch = OutboundBatch::from_driver(1, bind_batch(iter([item(0)])));
        assert_eq!(
            block_on(batch.collect()).unwrap().termination,
            OutboundBatchTermination::Completed
        );
        assert!(scope.scope.drivers.borrow().is_empty());
    }

    #[test]
    fn closure_after_last_item_requires_failure_instead_of_completed() {
        let scope = Guard::callback();
        let mut batch = OutboundBatch::from_driver(1, bind_batch(iter([item(0)])));
        assert!(matches!(
            block_on(batch.next()).unwrap(),
            OutboundBatchNext::Item(_)
        ));
        drop(scope);
        closed(&block_on(batch.next()).unwrap_err(), Owner::Callback);
        assert!(matches!(
            block_on(batch.next()).unwrap_err(),
            EdgeError::Internal { .. }
        ));
    }

    #[test]
    fn failed_collection_preserves_results_observed_before_scope_closure() {
        let scope = Guard::callback();
        let source = iter([item(0)]).chain(pending_stream());
        let batch = OutboundBatch::from_driver(2, bind_batch(source));
        let mut collection = Box::pin(batch.collect());
        let counter = Arc::new(WakeCount::default());
        let waker = waker_ref(&counter);
        assert!(
            collection
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(scope);
        let failure = block_on(collection).unwrap_err();
        closed(&failure.error, Owner::Callback);
        assert!(matches!(failure.slots.first(), Some(Some(_))));
        assert!(matches!(failure.slots.get(1), Some(None)));
    }

    #[test]
    fn response_scope_closes_only_response_created_work() {
        let callback = Guard::callback();
        let callback_release = Rc::new(Cell::new(0));
        let mut callback_driver = bind_batch(pending_driver(Rc::clone(&callback_release)));
        let response_release = Rc::new(Cell::new(0));
        let mut response_driver = {
            let response = Guard::response();
            assert!(Guard::fallback().is_none(), "helper reuses response scope");
            let driver = bind_batch(pending_driver(Rc::clone(&response_release)));
            drop(response);
            driver
        };
        assert_eq!(response_release.get(), 1);
        assert_eq!(callback_release.get(), 0);
        let Some(OutboundBatchDriverEvent::Failed(error)) = block_on(response_driver.next()) else {
            panic!("closed response");
        };
        closed(&error, Owner::Response);
        let later_release = Rc::new(Cell::new(0));
        let _later_driver = bind_batch(pending_driver(Rc::clone(&later_release)));
        assert!(Guard::fallback().is_none(), "helper reuses outer callback");
        assert_eq!(later_release.get(), 0);
        drop(callback);
        assert_eq!(callback_release.get(), 1);
        assert_eq!(later_release.get(), 1);
        assert!(matches!(
            block_on(callback_driver.next()),
            Some(OutboundBatchDriverEvent::Failed(_))
        ));
    }

    #[test]
    fn standalone_helper_scope_ends_at_helper_return() {
        let released = Rc::new(Cell::new(0));
        let mut driver = {
            let _helper = Guard::fallback().expect("standalone helper needs an owner");
            bind_batch(pending_driver(Rc::clone(&released)))
        };
        assert_eq!(released.get(), 1);
        assert!(matches!(
            block_on(driver.next()),
            Some(OutboundBatchDriverEvent::Failed(_))
        ));
    }

    #[test]
    fn unscoped_direct_driver_remains_owned_by_its_consumer() {
        let released = Rc::new(Cell::new(0));
        let driver = bind_batch(pending_driver(Rc::clone(&released)));
        assert_eq!(released.get(), 0);
        drop(driver);
        assert_eq!(released.get(), 1);
    }

    #[test]
    fn terminal_cutoff_and_driver_failure_are_not_rewritten_by_scope_exit() {
        for cutoff in [false, true] {
            let scope = Guard::callback();
            let original = if cutoff {
                OutboundBatchDriverEvent::Cutoff
            } else {
                OutboundBatchDriverEvent::Failed(EdgeError::bad_request("original driver error"))
            };
            let mut driver = bind_batch(iter([original]));
            let terminal = block_on(driver.next()).expect("original terminal");
            assert!(scope.scope.drivers.borrow().is_empty());
            drop(scope);
            match terminal {
                OutboundBatchDriverEvent::Cutoff => assert!(cutoff),
                OutboundBatchDriverEvent::Failed(error) => {
                    assert!(!cutoff);
                    assert_eq!(error.message(), "original driver error");
                    assert!(matches!(error, EdgeError::BadRequest { .. }));
                }
                OutboundBatchDriverEvent::Item(_) => panic!("unexpected item"),
                _ => panic!("unexpected terminal"),
            }
            assert!(block_on(driver.next()).is_none());
        }
    }

    #[test]
    fn completed_batch_remains_completed_after_scope_exit() {
        let scope = Guard::callback();
        let mut batch = OutboundBatch::from_driver(1, bind_batch(iter([item(0)])));
        assert!(matches!(
            block_on(batch.next()).unwrap(),
            OutboundBatchNext::Item(_)
        ));
        assert!(matches!(
            block_on(batch.next()).unwrap(),
            OutboundBatchNext::Finished(OutboundBatchTermination::Completed)
        ));
        drop(scope);
        assert!(matches!(
            block_on(batch.next()).unwrap(),
            OutboundBatchNext::Finished(OutboundBatchTermination::Completed)
        ));
    }
}

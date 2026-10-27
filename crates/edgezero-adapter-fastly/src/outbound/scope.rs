//! Cooperative provider scheduling; core owns every callback/body lifetime.

use core::convert::identity;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::mem;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll, Waker};

use super::progress::{Progress, Transport};
use edgezero_core::{
    EdgeError, MonotonicInstant, OutboundBatchDriverEvent, OutboundExchangeOwner, OutboundFuture,
    OutboundPending, OutboundProgressRuntime, OutboundResponse, OutboundScope, OutboundTime,
};
use futures_util::StreamExt as _;
use futures_util::stream::{LocalBoxStream, Stream, iter, once, poll_fn};

#[derive(Default)]
struct Drivers {
    active: RefCell<BTreeMap<u64, Weak<RefCell<Driver>>>>,
    next_id: Cell<u64>,
    pumping: Cell<bool>,
}

struct Driver {
    abandoned: bool,
    polling: bool,
    ready: VecDeque<OutboundBatchDriverEvent>,
    stream: Option<LocalBoxStream<'static, OutboundBatchDriverEvent>>,
    waker: Option<Waker>,
}

struct Scheduled {
    driver: Rc<RefCell<Driver>>,
    registration: Option<(Rc<Drivers>, u64)>,
}

struct Pumping(Rc<Drivers>);

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for Pumping {
    fn drop(&mut self) {
        self.0.pumping.set(false);
    }
}

impl Scheduled {
    fn unregister(&mut self) {
        if let Some((drivers, id)) = self.registration.take() {
            drivers.active.borrow_mut().remove(&id);
        }
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for Scheduled {
    fn drop(&mut self) {
        self.unregister();
        let cleanup = {
            let mut driver = self.driver.borrow_mut();
            driver.abandoned = true;
            (
                driver.stream.take(),
                mem::take(&mut driver.ready),
                driver.waker.take(),
            )
        };
        drop(cleanup);
    }
}

impl Stream for Scheduled {
    type Item = OutboundBatchDriverEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let observation = {
            let mut driver = this.driver.borrow_mut();
            if let Some(event) = driver.ready.pop_front() {
                Poll::Ready(Some(event))
            } else if driver.stream.is_none() && !driver.polling {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        };
        let terminal = matches!(
            &observation,
            Poll::Ready(
                None | Some(OutboundBatchDriverEvent::Cutoff | OutboundBatchDriverEvent::Failed(_))
            )
        );
        if terminal {
            this.unregister();
        }
        let previous = {
            let mut driver = this.driver.borrow_mut();
            mem::replace(
                &mut driver.waker,
                observation.is_pending().then(|| cx.waker().clone()),
            )
        };
        drop(previous);
        observation
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

pub(super) fn begin<T>(
    scope: &OutboundScope,
    future: OutboundFuture<OutboundResponse>,
    progress: &Progress<T>,
    time: &OutboundTime,
    entered: MonotonicInstant,
) -> Result<OutboundPending, EdgeError>
where
    T: Transport + 'static,
{
    scope.check()?;
    let driver = progress.clone();
    let clock = progress.clock();
    let source = async_stream::stream! {
        let outcome = driver.drive(future).await.and_then(identity);
        yield OutboundBatchDriverEvent::Item { index: 0, completed_at: clock.now(), outcome };
    };
    let mut receiver = schedule_batch(scope, source, progress, time, entered);
    Ok(OutboundPending::new(
        Box::pin(async move {
            match receiver.next().await {
                Some(OutboundBatchDriverEvent::Item { outcome, .. }) => outcome,
                Some(OutboundBatchDriverEvent::Failed(error)) => Err(error),
                _ => Err(EdgeError::internal(anyhow::anyhow!(
                    "Fastly begun exchange ended without a result"
                ))),
            }
        }),
        OutboundExchangeOwner::new(|| {}),
    ))
}

pub(crate) fn bind_batch<S>(
    scope: &OutboundScope,
    stream: S,
) -> LocalBoxStream<'static, OutboundBatchDriverEvent>
where
    S: Stream<Item = OutboundBatchDriverEvent> + 'static,
{
    let captured = scope.clone();
    let mut pending = stream.boxed_local();
    let activated = poll_fn(move |cx| {
        captured
            .with(|| pending.as_mut().poll_next(cx))
            .unwrap_or_else(|error| Poll::Ready(Some(OutboundBatchDriverEvent::Failed(error))))
    });
    let registered = scope.provider_state(Drivers::default).and_then(|drivers| {
        let id = drivers.next_id.get();
        let next = id.checked_add(1).ok_or_else(|| {
            EdgeError::internal(anyhow::anyhow!(
                "Fastly outbound scheduling registration exhausted"
            ))
        })?;
        drivers.next_id.set(next);
        let driver = Rc::new(RefCell::new(Driver {
            abandoned: false,
            polling: false,
            ready: VecDeque::new(),
            stream: Some(activated.boxed_local()),
            waker: None,
        }));
        drivers
            .active
            .borrow_mut()
            .insert(id, Rc::downgrade(&driver));
        Ok(Scheduled {
            driver,
            registration: Some((drivers, id)),
        }
        .boxed_local())
    });
    registered.unwrap_or_else(|error| {
        once(async move { OutboundBatchDriverEvent::Failed(error) }).boxed_local()
    })
}

pub(crate) fn capture() -> OutboundScope {
    OutboundScope::capture(runtime())
}

#[cfg(feature = "fastly")]
pub(super) fn native_progress(
    scope: &OutboundScope,
    time: &OutboundTime,
    entered: MonotonicInstant,
) -> Result<super::fastly_impl::NativeProgress, EdgeError> {
    use super::fastly_impl::{NativeProgress, NativeTransport};
    scope
        .provider_state(|| {
            NativeProgress::new(NativeTransport, time.clock().clone(), entered, None)
        })
        .map(|progress| progress.with_clock(time.clock().clone(), entered))
}

pub(crate) fn poll_current(cx: &mut Context<'_>) {
    let Ok(drivers) = capture().provider_state(Drivers::default) else {
        return;
    };
    if drivers.pumping.replace(true) {
        return;
    }
    let _guard = Pumping(Rc::clone(&drivers));
    let active: Vec<_> = drivers
        .active
        .borrow()
        .values()
        .filter_map(Weak::upgrade)
        .collect();
    for driver in active {
        pump(&driver, cx);
    }
}

fn pump(driver: &Rc<RefCell<Driver>>, cx: &mut Context<'_>) {
    let owned = {
        let mut root = driver.borrow_mut();
        if root.polling || root.abandoned {
            return;
        }
        let source = root.stream.take();
        root.polling = source.is_some();
        source
    };
    let Some(mut source) = owned else {
        return;
    };
    let observation = source.as_mut().poll_next(cx);
    let mut root = driver.borrow_mut();
    root.polling = false;
    if root.abandoned {
        let ready = mem::take(&mut root.ready);
        drop(root);
        drop(ready);
        return;
    }
    let terminal = matches!(
        &observation,
        Poll::Ready(
            None | Some(OutboundBatchDriverEvent::Cutoff | OutboundBatchDriverEvent::Failed(_))
        )
    );
    let waiting = if observation.is_ready() {
        root.waker.take()
    } else {
        None
    };
    if let Poll::Ready(Some(event)) = observation {
        root.ready.push_back(event);
    }
    if !terminal {
        root.stream = Some(source);
    }
    drop(root);
    if let Some(waker) = waiting {
        waker.wake();
    }
}

pub(super) fn runtime() -> Rc<dyn OutboundProgressRuntime> {
    #[cfg(feature = "fastly")]
    {
        Rc::new(super::fastly_impl::NativeRuntime)
    }
    #[cfg(not(feature = "fastly"))]
    {
        Rc::new(edgezero_core::IndependentOutboundProgress)
    }
}

pub(super) fn schedule_batch<S, T>(
    scope: &OutboundScope,
    source: S,
    progress: &Progress<T>,
    time: &OutboundTime,
    entered: MonotonicInstant,
) -> LocalBoxStream<'static, OutboundBatchDriverEvent>
where
    S: Stream<Item = OutboundBatchDriverEvent> + 'static,
    T: Transport + 'static,
{
    let mut pending = source.boxed_local();
    let driver = progress.clone();
    let captured = scope.clone();
    let paired = time.clone();
    let polling = poll_fn(move |cx| {
        let next = pending.next();
        futures_util::pin_mut!(next);
        driver
            .with_current(|| captured.poll(&paired, entered, cx, next.as_mut()))
            .map(|result| match result {
                Ok(event) => event,
                Err(error) => Some(OutboundBatchDriverEvent::Failed(error)),
            })
    });
    let mut receiver = bind_batch(scope, polling);
    let observation = progress.with_current(|| {
        scope
            .with(|| {
                let mut cx = Context::from_waker(Waker::noop());
                poll_current(&mut cx);
                receiver.as_mut().poll_next(&mut cx)
            })
            .unwrap_or_else(|error| Poll::Ready(Some(OutboundBatchDriverEvent::Failed(error))))
    });
    if let Poll::Ready(Some(event)) = observation {
        iter([event]).chain(receiver).boxed_local()
    } else {
        receiver
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing;
    use super::{Drivers, bind_batch, capture};
    use crate::lifecycle::with_callback;
    use std::cell::{Cell, RefCell};
    use std::future::Future as _;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    use edgezero_core::{
        EdgeError, OutboundBatch, OutboundBatchCheck, OutboundBatchDriverEvent, OutboundBatchNext,
        OutboundBatchTermination, OutboundCollectedSlot, OutboundScope, OutboundScopeKind,
        OutboundScopeOwner,
    };
    use futures::executor::block_on;
    use futures_util::StreamExt as _;
    use futures_util::stream::{LocalBoxStream, Stream, iter, pending as pending_stream, poll_fn};
    use futures_util::task::{ArcWake, waker, waker_ref};

    struct Release(Rc<Cell<usize>>);
    #[derive(Default)]
    struct WakeCount(AtomicUsize);
    struct CloseOnDrop;

    #[test]
    #[cfg(feature = "fastly")]
    fn family_progress_preserves_each_explicit_client_clock() {
        use edgezero_core::{MonotonicClock, MonotonicInstant, OutboundTime};
        use std::time::Duration;

        let callback = OutboundScopeOwner::callback();
        let captured = callback.with(capture);
        let first = MonotonicInstant::now();
        let second = first.checked_add(Duration::from_secs(3600)).unwrap();
        let first_time = OutboundTime::new(
            MonotonicClock::new(move || first),
            super::super::progress::wait_current,
        );
        let second_time = OutboundTime::new(
            MonotonicClock::new(move || second),
            super::super::progress::wait_current,
        );
        let first_progress = super::native_progress(&captured, &first_time, first).unwrap();
        let second_progress = super::native_progress(&captured, &second_time, second).unwrap();
        assert_eq!(first_progress.clock().now(), first);
        assert_eq!(second_progress.clock().now(), second);
    }

    std::thread_local! {
        static CLOSE_ON_WAKER_DROP: RefCell<Option<OutboundScopeOwner>> = const { RefCell::new(None) };
    }

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_add(1));
        }
    }
    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for CloseOnDrop {
        fn drop(&mut self) {
            drop(CLOSE_ON_WAKER_DROP.with(|saved| saved.borrow_mut().take()));
        }
    }
    impl ArcWake for CloseOnDrop {
        fn wake(self: Arc<Self>) {
            Self::wake_by_ref(&self);
        }
        fn wake_by_ref(_arc_self: &Arc<Self>) {}
    }
    impl ArcWake for WakeCount {
        fn wake(self: Arc<Self>) {
            Self::wake_by_ref(&self);
        }
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn scheduled_receiver_observation_never_polls_producer() {
        let callback = OutboundScopeOwner::callback();
        let polls = Rc::new(Cell::new(0_usize));
        let counted = Rc::clone(&polls);
        let mut events = [OutboundBatchDriverEvent::Item {
            index: 0,
            completed_at: testing::instant(),
            outcome: Err(EdgeError::bad_gateway("queued fixture")),
        }]
        .into_iter();
        let source = poll_fn(move |_cx| {
            counted.set(counted.get().saturating_add(1));
            Poll::Ready(events.next())
        });
        let mut receiver = callback.with(|| bind_batch(&callback.scope(), source));
        let mut context = Context::from_waker(Waker::noop());
        assert!(receiver.as_mut().poll_next(&mut context).is_pending());
        assert_eq!(
            polls.get(),
            0,
            "receiver observation cannot poll the producer"
        );
        callback.with(|| super::poll_current(&mut context));
        assert_eq!(polls.get(), 1);
        assert!(matches!(
            receiver.as_mut().poll_next(&mut context),
            Poll::Ready(Some(OutboundBatchDriverEvent::Item { index: 0, .. }))
        ));
        assert!(receiver.as_mut().poll_next(&mut context).is_pending());
        assert_eq!(polls.get(), 1, "empty receiver cannot resume the producer");
        callback.with(|| super::poll_current(&mut context));
        assert!(matches!(
            receiver.as_mut().poll_next(&mut context),
            Poll::Ready(None)
        ));
        assert_eq!(polls.get(), 2);
    }

    fn closed(error: &EdgeError, kind: OutboundScopeKind) {
        let EdgeError::Internal { source, .. } = error else {
            panic!("scope closure must stay Internal: {error:?}");
        };
        assert_eq!(
            source.to_string(),
            format!("outbound {kind:?} scope closed")
        );
    }

    fn pending_driver(
        released: Rc<Cell<usize>>,
    ) -> LocalBoxStream<'static, OutboundBatchDriverEvent> {
        let owner = Release(released);
        poll_fn(move |_cx| {
            let _owner = &owner;
            Poll::Pending
        })
        .boxed_local()
    }

    fn batch<S>(slots: usize, source: S) -> OutboundBatch
    where
        S: Stream<Item = OutboundBatchDriverEvent> + 'static,
    {
        let context = testing::batch_context(slots);
        let receiver = bind_batch(context.scope(), source);
        OutboundBatch::from_driver(context, receiver, testing::owner())
    }

    fn item(index: usize) -> OutboundBatchDriverEvent {
        OutboundBatchDriverEvent::Item {
            index,
            completed_at: testing::instant(),
            outcome: Err(EdgeError::bad_request("slot fixture")),
        }
    }

    #[test]
    fn callback_progress_finishes_pipeline_without_consumer_polling() {
        let owner = OutboundScopeOwner::callback();
        let ready = Rc::new(Cell::new(false));
        let completed = Rc::new(Cell::new(0_usize));
        let signal = Rc::clone(&ready);
        let terminal = Rc::clone(&completed);
        let driver = owner.with(|| {
            batch(
                1,
                poll_fn(move |_cx| {
                    if signal.get() {
                        if terminal.replace(1) == 0 {
                            Poll::Ready(Some(item(0)))
                        } else {
                            Poll::Ready(None)
                        }
                    } else {
                        Poll::Pending
                    }
                }),
            )
        });
        ready.set(true);
        owner.with(|| super::poll_current(&mut Context::from_waker(Waker::noop())));
        assert_eq!(
            completed.get(),
            1,
            "pipeline must run while its consumer is idle"
        );
        owner.with(|| super::poll_current(&mut Context::from_waker(Waker::noop())));
        assert_eq!(
            block_on(driver.collect()).unwrap().termination(),
            OutboundBatchTermination::Completed
        );
    }

    #[test]
    fn callback_progress_wakes_idle_completion_consumer() {
        let owner = OutboundScopeOwner::callback();
        let ready = Rc::new(Cell::new(false));
        let signal = Rc::clone(&ready);
        let mut emitted = false;
        let mut retained = owner.with(|| {
            batch(
                1,
                poll_fn(move |_cx| {
                    if signal.get() && !emitted {
                        emitted = true;
                        Poll::Ready(Some(item(0)))
                    } else if emitted {
                        Poll::Ready(None)
                    } else {
                        Poll::Pending
                    }
                }),
            )
        });
        let counter = Arc::new(WakeCount::default());
        let waiting = waker_ref(&counter);
        let mut receive = Box::pin(retained.next());
        assert!(
            receive
                .as_mut()
                .poll(&mut Context::from_waker(&waiting))
                .is_pending()
        );
        drop(receive);
        ready.set(true);
        owner.with(|| super::poll_current(&mut Context::from_waker(Waker::noop())));
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "producer completion wakes the original idle consumer"
        );
        assert!(matches!(
            block_on(retained.next()).unwrap(),
            OutboundBatchNext::Item(_)
        ));
    }

    #[test]
    fn captured_root_keeps_invocation_scope_across_response_polling() {
        let callback = OutboundScopeOwner::callback();
        let captured = callback.with(capture);
        let released = Rc::new(Cell::new(0));
        let mut retained = callback.with(|| batch(1, pending_driver(Rc::clone(&released))));
        let response = callback.with(OutboundScopeOwner::response_body);
        let readiness = response.with(|| retained.check()).unwrap();
        assert!(matches!(readiness, OutboundBatchCheck::Pending));
        response.close();
        assert_eq!(
            released.get(),
            0,
            "foreign response cannot reparent callback roots"
        );
        callback.close();
        assert_eq!(
            released.get(),
            1,
            "callback exit revokes an unpolled producer"
        );
        closed(&captured.check().unwrap_err(), OutboundScopeKind::Callback);
        let later = OutboundScopeOwner::callback();
        let failure = later.with(|| block_on(retained.collect()).unwrap_err());
        closed(failure.error(), OutboundScopeKind::Callback);
        assert_eq!(released.get(), 1);
    }

    #[test]
    fn closing_scope_releases_unpolled_driver_without_polling() {
        let owner = OutboundScopeOwner::callback();
        let released = Rc::new(Cell::new(0));
        let retained = owner.with(|| batch(1, pending_driver(Rc::clone(&released))));
        owner.close();
        assert_eq!(released.get(), 1);
        closed(
            block_on(retained.collect()).unwrap_err().error(),
            OutboundScopeKind::Callback,
        );
        assert_eq!(released.get(), 1);
    }

    #[test]
    fn closing_scope_wakes_pending_consumer_after_releasing_driver() {
        let owner = OutboundScopeOwner::callback();
        let released = Rc::new(Cell::new(0));
        let retained = owner.with(|| batch(1, pending_driver(Rc::clone(&released))));
        let counter = Arc::new(WakeCount::default());
        let waiting = waker_ref(&counter);
        let mut receive = Box::pin(retained.collect());
        assert!(
            receive
                .as_mut()
                .poll(&mut Context::from_waker(&waiting))
                .is_pending()
        );
        owner.close();
        assert_eq!(released.get(), 1);
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        closed(
            block_on(receive).unwrap_err().error(),
            OutboundScopeKind::Callback,
        );
    }

    #[test]
    fn replacing_waker_allows_reentrant_scope_close() {
        let owner = OutboundScopeOwner::callback();
        let released = Rc::new(Cell::new(0));
        let mut retained = owner.with(|| batch(1, pending_driver(Rc::clone(&released))));
        CLOSE_ON_WAKER_DROP.with(|saved| *saved.borrow_mut() = Some(owner));
        let original = waker(Arc::new(CloseOnDrop));
        let mut next = Box::pin(retained.next());
        assert!(
            next.as_mut()
                .poll(&mut Context::from_waker(&original))
                .is_pending()
        );
        drop(next);
        drop(original);
        let replacement = waker(Arc::new(WakeCount::default()));
        let result = {
            let mut replacement_wait = Box::pin(retained.next());
            replacement_wait
                .as_mut()
                .poll(&mut Context::from_waker(&replacement))
        };
        assert!(matches!(
            result,
            Poll::Ready(Err(EdgeError::Internal { .. }))
        ));
        assert_eq!(released.get(), 1);
    }

    #[test]
    fn closing_scope_revokes_siblings_before_reentrant_cleanup() {
        struct Cleanup {
            rejected: Rc<Cell<bool>>,
            released: Rc<Cell<usize>>,
            scope: OutboundScope,
            sibling: Rc<RefCell<Option<OutboundBatch>>>,
        }
        #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
        impl Drop for Cleanup {
            fn drop(&mut self) {
                self.rejected.set(self.scope.check().is_err());
                let sibling = self.sibling.borrow_mut().take().expect("sibling");
                closed(
                    block_on(sibling.collect()).unwrap_err().error(),
                    OutboundScopeKind::Callback,
                );
                let source = pending_driver(Rc::clone(&self.released));
                let error = self
                    .scope
                    .with(move || batch(1, source))
                    .expect_err("closed scope rejects cleanup-created work");
                closed(&error, OutboundScopeKind::Callback);
            }
        }
        let owner = OutboundScopeOwner::callback();
        let sibling = Rc::new(RefCell::new(None));
        let released = Rc::new(Cell::new(0));
        let rejected = Rc::new(Cell::new(false));
        let cleanup = Cleanup {
            scope: owner.with(capture),
            rejected: Rc::clone(&rejected),
            released: Rc::clone(&released),
            sibling: Rc::clone(&sibling),
        };
        let first = owner.with(|| {
            batch(
                1,
                poll_fn(move |_cx| {
                    let _cleanup = &cleanup;
                    Poll::Pending
                }),
            )
        });
        let reads = Rc::new(Cell::new(0_usize));
        let source_reads = Rc::clone(&reads);
        *sibling.borrow_mut() = Some(owner.with(|| {
            batch(
                1,
                poll_fn(move |_cx| {
                    source_reads.set(source_reads.get().saturating_add(1));
                    Poll::Ready(Some(item(0)))
                }),
            )
        }));
        owner.close();
        assert!(rejected.get());
        assert_eq!(reads.get(), 0, "cleanup cannot poll a sibling source");
        assert_eq!(released.get(), 1, "cleanup-created work cannot escape");
        drop(first);
    }

    #[test]
    fn driver_drop_and_terminal_unregister_without_accumulating_entries() {
        let owner = OutboundScopeOwner::callback();
        let drivers = owner
            .with(capture)
            .provider_state(Drivers::default)
            .unwrap();
        for _ in 0_u8..32 {
            let released = Rc::new(Cell::new(0));
            let retained = owner.with(|| batch(1, pending_driver(Rc::clone(&released))));
            assert_eq!(drivers.active.borrow().len(), 1);
            drop(retained);
            assert_eq!(released.get(), 1);
            assert!(drivers.active.borrow().is_empty());
        }
        let retained = owner.with(|| batch(1, iter([item(0)])));
        owner.with(|| super::poll_current(&mut Context::from_waker(Waker::noop())));
        owner.with(|| super::poll_current(&mut Context::from_waker(Waker::noop())));
        assert_eq!(
            block_on(retained.collect()).unwrap().termination(),
            OutboundBatchTermination::Completed
        );
        assert!(drivers.active.borrow().is_empty());
    }

    #[test]
    fn closure_after_last_item_requires_failure_instead_of_completed() {
        let owner = OutboundScopeOwner::callback();
        let mut retained = owner.with(|| batch(1, iter([item(0)]).chain(pending_stream())));
        owner.with(|| super::poll_current(&mut Context::from_waker(Waker::noop())));
        assert!(matches!(
            block_on(retained.next()).unwrap(),
            OutboundBatchNext::Item(_)
        ));
        owner.close();
        closed(
            &block_on(retained.next()).unwrap_err(),
            OutboundScopeKind::Callback,
        );
        assert!(matches!(
            block_on(retained.next()).unwrap_err(),
            EdgeError::Internal { .. }
        ));
    }

    #[test]
    fn failed_collection_preserves_results_observed_before_scope_closure() {
        let owner = OutboundScopeOwner::callback();
        let retained = owner.with(|| batch(2, iter([item(0)]).chain(pending_stream())));
        owner.with(|| super::poll_current(&mut Context::from_waker(Waker::noop())));
        let mut receive = Box::pin(retained.collect());
        assert!(
            receive
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        owner.close();
        let failure = block_on(receive).unwrap_err();
        closed(failure.error(), OutboundScopeKind::Callback);
        assert!(matches!(
            failure.slots().first(),
            Some(OutboundCollectedSlot::Resolved(_))
        ));
        assert!(matches!(
            failure.slots().get(1),
            Some(OutboundCollectedSlot::Unresolved)
        ));
    }

    #[test]
    fn response_scope_closes_only_response_created_work() {
        let callback = OutboundScopeOwner::callback();
        let callback_release = Rc::new(Cell::new(0));
        let callback_batch =
            callback.with(|| batch(1, pending_driver(Rc::clone(&callback_release))));
        let body = callback.with(OutboundScopeOwner::response_body);
        let body_release = Rc::new(Cell::new(0));
        let body_batch = body.with(|| batch(1, pending_driver(Rc::clone(&body_release))));
        body.close();
        assert_eq!(body_release.get(), 1);
        assert_eq!(callback_release.get(), 0);
        closed(
            block_on(body_batch.collect()).unwrap_err().error(),
            OutboundScopeKind::ResponseBody,
        );
        let later_release = Rc::new(Cell::new(0));
        let later = callback.with(|| batch(1, pending_driver(Rc::clone(&later_release))));
        callback.close();
        assert_eq!(callback_release.get(), 1);
        assert_eq!(later_release.get(), 1);
        closed(
            block_on(callback_batch.collect()).unwrap_err().error(),
            OutboundScopeKind::Callback,
        );
        drop(later);
    }

    #[test]
    fn standalone_helper_scope_ends_at_helper_return() {
        let released = Rc::new(Cell::new(0));
        let retained = with_callback(|| batch(1, pending_driver(Rc::clone(&released))));
        assert_eq!(released.get(), 1);
        closed(
            block_on(retained.collect()).unwrap_err().error(),
            OutboundScopeKind::Callback,
        );
    }

    #[test]
    fn unscoped_direct_driver_remains_owned_by_its_consumer() {
        let released = Rc::new(Cell::new(0));
        let retained = batch(1, pending_driver(Rc::clone(&released)));
        assert_eq!(released.get(), 0);
        drop(retained);
        assert_eq!(released.get(), 1);
    }

    #[test]
    fn terminal_cutoff_and_driver_failure_are_not_rewritten_by_scope_exit() {
        for cutoff in [false, true] {
            let owner = OutboundScopeOwner::callback();
            let scope = owner.with(capture);
            let original = if cutoff {
                OutboundBatchDriverEvent::Cutoff
            } else {
                OutboundBatchDriverEvent::Failed(EdgeError::bad_request("original driver error"))
            };
            let mut driver = bind_batch(&scope, iter([original]));
            owner.with(|| super::poll_current(&mut Context::from_waker(Waker::noop())));
            let terminal = block_on(driver.next()).expect("original terminal");
            owner.close();
            match terminal {
                OutboundBatchDriverEvent::Cutoff => assert!(cutoff),
                OutboundBatchDriverEvent::Failed(error) => {
                    assert!(!cutoff);
                    assert!(matches!(error, EdgeError::BadRequest { .. }));
                    assert_eq!(error.message(), "original driver error");
                }
                OutboundBatchDriverEvent::Item { .. } => panic!("unexpected item"),
            }
            assert!(block_on(driver.next()).is_none());
        }
    }

    #[test]
    fn completed_batch_remains_completed_after_scope_exit() {
        let owner = OutboundScopeOwner::callback();
        let mut retained = owner.with(|| batch(1, iter([item(0)])));
        owner.with(|| super::poll_current(&mut Context::from_waker(Waker::noop())));
        owner.with(|| super::poll_current(&mut Context::from_waker(Waker::noop())));
        assert!(matches!(
            block_on(retained.next()).unwrap(),
            OutboundBatchNext::Item(_)
        ));
        assert!(matches!(
            block_on(retained.next()).unwrap(),
            OutboundBatchNext::Finished(OutboundBatchTermination::Completed)
        ));
        owner.close();
        assert!(matches!(
            block_on(retained.next()).unwrap(),
            OutboundBatchNext::Finished(OutboundBatchTermination::Completed)
        ));
    }
}

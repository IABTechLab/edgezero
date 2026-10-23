use std::fmt;
use std::task::{Context, Poll, Waker};

use futures_util::future::poll_fn;

use super::{OutboundExchangeOwner, OutboundFuture, OutboundPending, OutboundResponse};
use crate::error::EdgeError;

impl OutboundExchangeOwner {
    fn disarm(mut self) {
        drop(self.release.take());
    }

    /// Owns one nonblocking release operation, run when this owner is dropped.
    /// Pending collection disarms its cancellation callback when success transfers ownership.
    #[inline]
    pub fn new<Release>(release: Release) -> Self
    where
        Release: FnOnce() + 'static,
    {
        Self {
            release: Some(Box::new(release)),
        }
    }
}

impl fmt::Debug for OutboundExchangeOwner {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboundExchangeOwner")
            .finish_non_exhaustive()
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
)]
impl Drop for OutboundExchangeOwner {
    #[inline]
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

impl OutboundPending {
    /// Cancels scheduled work and drops any cached response without polling its body.
    #[inline]
    pub fn cancel(self) {
        drop(self);
    }

    /// Collects the original completion, transferring a successful live response unchanged.
    /// Dropping this future, including before its first poll, cancels remaining work.
    ///
    /// # Errors
    /// Returns the original completion error unchanged.
    #[inline]
    pub fn collect(mut self) -> OutboundFuture<OutboundResponse> {
        Box::pin(async move {
            poll_fn(|context| self.observe(context)).await;
            let outcome = self.outcome.take().ok_or_else(|| {
                EdgeError::internal(anyhow::anyhow!("outbound completion missing its result"))
            })?;
            if outcome.is_ok()
                && let Some(owner) = self.owner.take()
            {
                owner.disarm();
            }
            drop(self);
            outcome
        })
    }

    /// Checks the completion receiver once without waiting, consuming a result or polling a body.
    /// Success and failure are both ready; repeated checks use the cached original result.
    #[must_use]
    #[inline]
    pub fn is_ready(&mut self) -> bool {
        self.observe(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    }

    /// Wraps a receiver for already-issued work, not a lazy transport future.
    /// The owner cancels scheduled work on failure/cancellation and is disarmed on successful
    /// collection. Completed buffered responses must already have released transport resources;
    /// streamed responses must already own their transport cleanup before being published.
    #[inline]
    pub fn new(
        already_scheduled: OutboundFuture<OutboundResponse>,
        owner: OutboundExchangeOwner,
    ) -> Self {
        Self {
            completion: Some(already_scheduled),
            outcome: None,
            owner: Some(owner),
        }
    }

    fn observe(&mut self, context: &mut Context<'_>) -> Poll<()> {
        if self.outcome.is_some() {
            return Poll::Ready(());
        }
        let outcome = if let Some(completion) = &mut self.completion {
            let Poll::Ready(outcome) = completion.as_mut().poll(context) else {
                return Poll::Pending;
            };
            outcome
        } else {
            Err(EdgeError::internal(anyhow::anyhow!(
                "outbound completion receiver missing"
            )))
        };
        let failed = outcome.is_err();
        self.outcome = Some(outcome);
        drop(self.completion.take());
        if failed {
            drop(self.owner.take());
        }
        Poll::Ready(())
    }
}

impl fmt::Debug for OutboundPending {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboundPending")
            .field("ready", &self.outcome.is_some())
            .finish_non_exhaustive()
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
)]
impl Drop for OutboundPending {
    #[inline]
    fn drop(&mut self) {
        // Stop scheduled work before releasing its receiver or an uncollected response.
        drop(self.owner.take());
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    use bytes::Bytes;
    use futures::channel::oneshot;
    use futures::executor::block_on;
    use futures_util::future::pending;
    use futures_util::stream;
    use futures_util::task::{ArcWake, waker_ref};

    use crate::body::Body;
    use crate::error::EdgeError;
    use crate::http::{HeaderMap, Method, StatusCode};
    use crate::outbound::{
        ContentTypeProvenance, OutboundExchangeOwner, OutboundFuture, OutboundPending,
        OutboundResponse,
    };
    use crate::time::MonotonicClock;

    struct Completion {
        counts: CompletionCounts,
        outcome: Option<Result<OutboundResponse, EdgeError>>,
        pending_first: bool,
    }

    #[derive(Clone, Default)]
    struct CompletionCounts {
        drops: Rc<Cell<usize>>,
        polls: Rc<Cell<usize>>,
    }

    struct DropCount(Rc<Cell<usize>>);

    #[derive(Debug, thiserror::Error)]
    #[error("completion fixture failure")]
    struct IdentityError(Arc<()>);

    struct OrderedDrop {
        events: Rc<RefCell<Vec<&'static str>>>,
        name: &'static str,
    }

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Future for Completion {
        type Output = Result<OutboundResponse, EdgeError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.counts
                .polls
                .set(self.counts.polls.get().saturating_add(1));
            if self.pending_first {
                self.pending_first = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(self.outcome.take().expect("completion polled only once"))
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for Completion {
        fn drop(&mut self) {
            self.counts
                .drops
                .set(self.counts.drops.get().saturating_add(1));
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_add(1));
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for OrderedDrop {
        fn drop(&mut self) {
            self.events.borrow_mut().push(self.name);
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

    fn completion(
        outcome: Result<OutboundResponse, EdgeError>,
        counts: &CompletionCounts,
        pending_first: bool,
    ) -> OutboundFuture<OutboundResponse> {
        Box::pin(Completion {
            counts: counts.clone(),
            outcome: Some(outcome),
            pending_first,
        })
    }

    fn owner(calls: &Rc<Cell<usize>>) -> OutboundExchangeOwner {
        let captured_calls = Rc::clone(calls);
        OutboundExchangeOwner::new(move || {
            captured_calls.set(captured_calls.get().saturating_add(1));
        })
    }

    fn poll_outcome(
        future: &mut OutboundFuture<OutboundResponse>,
    ) -> Poll<Result<OutboundResponse, EdgeError>> {
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    }

    fn response(body: Body) -> OutboundResponse {
        OutboundResponse::new(
            Method::GET,
            StatusCode::OK,
            HeaderMap::new(),
            body,
            MonotonicClock::default(),
            ContentTypeProvenance::Unknown,
        )
    }

    #[test]
    fn exchange_owner_releases_local_capture_once() {
        let calls = Rc::new(Cell::new(0_usize));
        let capture_drops = Rc::new(Cell::new(0));
        let captured = DropCount(Rc::clone(&capture_drops));
        let observed = Rc::clone(&calls);
        let owner = crate::OutboundExchangeOwner::new(move || {
            observed.set(observed.get().saturating_add(1));
            drop(captured);
        });
        assert!(format!("{owner:?}").contains("OutboundExchangeOwner"));
        assert_eq!(calls.get(), 0);
        assert_eq!(capture_drops.get(), 0);
        drop(owner);
        assert_eq!(calls.get(), 1);
        assert_eq!(capture_drops.get(), 1);
    }

    #[test]
    fn pending_construction_does_not_poll_and_cancel_drops_without_polling() {
        for cancel in [false, true] {
            let calls = Rc::new(Cell::new(0));
            let counts = CompletionCounts::default();
            let pending = crate::OutboundPending::new(
                completion(Ok(response(Body::empty())), &counts, false),
                owner(&calls),
            );
            assert_eq!(counts.polls.get(), 0);
            if cancel {
                pending.cancel();
            } else {
                drop(pending);
            }
            assert_eq!(counts.polls.get(), 0);
            assert_eq!(counts.drops.get(), 1);
            assert_eq!(calls.get(), 1);
        }
    }

    #[test]
    fn pending_readiness_caches_success_and_releases_completed_receiver() {
        let calls = Rc::new(Cell::new(0_usize));
        let capture_drops = Rc::new(Cell::new(0));
        let captured = DropCount(Rc::clone(&capture_drops));
        let counted = Rc::clone(&calls);
        let counts = CompletionCounts::default();
        let mut pending = OutboundPending::new(
            completion(Ok(response(Body::from("answer"))), &counts, true),
            OutboundExchangeOwner::new(move || {
                counted.set(counted.get().saturating_add(1));
                drop(captured);
            }),
        );
        assert!(format!("{pending:?}").contains("ready: false"));
        assert_eq!(counts.polls.get(), 0, "Debug never polls the receiver");
        assert!(!pending.is_ready());
        assert_eq!(counts.polls.get(), 1);
        assert!(pending.is_ready());
        assert_eq!(counts.polls.get(), 2);
        assert_eq!(counts.drops.get(), 1);
        let ready_debug = format!("{pending:?}");
        assert!(ready_debug.contains("ready: true"));
        assert!(
            !ready_debug.contains("answer"),
            "Debug excludes cached payloads"
        );
        for _ in 0_usize..3 {
            assert!(pending.is_ready());
        }
        assert_eq!(counts.polls.get(), 2);
        let mut collect: crate::OutboundFuture<crate::OutboundResponse> = pending.collect();
        let Poll::Ready(Ok(response)) = poll_outcome(&mut collect) else {
            panic!("cached response must be ready");
        };
        assert_eq!(response.body().as_bytes(), Some(b"answer".as_slice()));
        assert_eq!(counts.polls.get(), 2);
        assert_eq!(counts.drops.get(), 1);
        assert_eq!(calls.get(), 0, "successful collection disarms cancellation");
        assert_eq!(capture_drops.get(), 1, "completed future retains no owner");
        drop(collect);
        assert_eq!(calls.get(), 0);
        assert_eq!(capture_drops.get(), 1);
    }

    #[test]
    fn pending_readiness_preserves_original_nonclone_error() {
        use crate::error_detail::{ProviderFailureDetail, ProviderFailurePhase, SpinFailureCause};

        let detail = ProviderFailureDetail::Spin {
            phase: ProviderFailurePhase::ResponseHeaders,
            cause: SpinFailureCause::ConnectionRefused,
        };
        for checked in [false, true] {
            let identity = Arc::new(());
            let counts = CompletionCounts::default();
            let calls = Rc::new(Cell::new(0));
            let mut pending = OutboundPending::new(
                completion(
                    Err(EdgeError::internal_with_detail(
                        IdentityError(Arc::clone(&identity)),
                        detail,
                    )),
                    &counts,
                    false,
                ),
                owner(&calls),
            );
            if checked {
                assert!(pending.is_ready(), "failure is also a ready result");
                assert_eq!(calls.get(), 1, "failed work is released on observation");
                assert_eq!(counts.drops.get(), 1);
                assert!(pending.is_ready());
            }
            let mut collect = pending.collect();
            let Poll::Ready(Err(error)) = poll_outcome(&mut collect) else {
                panic!("original error must be ready");
            };
            let original = error
                .inner()
                .expect("internal error")
                .downcast_ref::<IdentityError>()
                .expect("original fixture identity");
            assert!(Arc::ptr_eq(&original.0, &identity));
            assert_eq!(error.provider_failure_detail(), Some(&detail));
            assert_eq!(counts.polls.get(), 1);
            assert_eq!(counts.drops.get(), 1);
            assert_eq!(calls.get(), 1);
            drop(collect);
            assert_eq!(calls.get(), 1);
        }
    }

    #[test]
    fn pending_collection_cancellation_releases_unpolled_and_waiting_work() {
        for polled in [false, true] {
            let calls = Rc::new(Cell::new(0));
            let future_drops = Rc::new(Cell::new(0));
            let captured = DropCount(Rc::clone(&future_drops));
            let future = Box::pin(async move {
                pending::<()>().await;
                drop(captured);
                Ok(response(Body::empty()))
            });
            let pending = OutboundPending::new(future, owner(&calls));
            let mut collect = pending.collect();
            if polled {
                assert!(poll_outcome(&mut collect).is_pending());
            }
            assert_eq!(calls.get(), 0);
            assert_eq!(future_drops.get(), 0);
            drop(collect);
            assert_eq!(calls.get(), 1);
            assert_eq!(future_drops.get(), 1);
        }
    }

    #[test]
    fn pending_cancellation_precedes_receiver_and_cached_body_release() {
        for checked in [false, true] {
            for cancel in [false, true] {
                let events = Rc::new(RefCell::new(Vec::new()));
                let body_drop = OrderedDrop {
                    events: Rc::clone(&events),
                    name: "body",
                };
                let body = Body::from_stream(stream::poll_fn(move |_| {
                    let _retain = &body_drop;
                    panic!("cancelled response must not be polled");
                }));
                let receiver_drop = OrderedDrop {
                    events: Rc::clone(&events),
                    name: "receiver",
                };
                let cancel_events = Rc::clone(&events);
                let mut pending = OutboundPending::new(
                    Box::pin(async move {
                        drop(receiver_drop);
                        Ok(response(body))
                    }),
                    OutboundExchangeOwner::new(move || cancel_events.borrow_mut().push("cancel")),
                );
                if checked {
                    assert!(pending.is_ready());
                    assert_eq!(*events.borrow(), ["receiver"]);
                }
                if cancel {
                    pending.cancel();
                } else {
                    drop(pending);
                }
                let observed = events.borrow();
                assert_eq!(observed.len(), 3);
                if checked {
                    assert_eq!(*observed, ["receiver", "cancel", "body"]);
                } else {
                    assert_eq!(observed.first(), Some(&"cancel"));
                    for name in ["receiver", "body"] {
                        assert_eq!(observed.iter().filter(|event| **event == name).count(), 1);
                    }
                }
            }
        }
    }

    #[test]
    fn collected_stream_stays_live_after_pending_handoff() {
        for checked in [false, true] {
            let aborted = Rc::new(Cell::new(false));
            let body_drops = Rc::new(Cell::new(0));
            let body_polls = Rc::new(Cell::new(0_usize));
            let release_body = owner(&body_drops);
            let body_aborted = Rc::clone(&aborted);
            let counted = Rc::clone(&body_polls);
            let mut chunk = Some(Bytes::from_static(b"live"));
            let body = Body::from_stream(stream::poll_fn(move |_| {
                let _retain_owner = &release_body;
                counted.set(counted.get().saturating_add(1));
                assert!(!body_aborted.get(), "pending owner must not abort body");
                Poll::Ready(chunk.take().map(Ok))
            }));
            let cancel_aborted = Rc::clone(&aborted);
            let counts = CompletionCounts::default();
            let mut pending = OutboundPending::new(
                completion(Ok(response(body)), &counts, false),
                OutboundExchangeOwner::new(move || cancel_aborted.set(true)),
            );
            if checked {
                assert!(pending.is_ready());
            }
            assert_eq!(body_polls.get(), 0);
            let collected = block_on(pending.collect()).expect("collected stream");
            assert_eq!(body_polls.get(), 0, "collection never drains a stream");
            assert_eq!(body_drops.get(), 0);
            assert!(!aborted.get());
            let bytes = block_on(collected.into_bytes_bounded(4)).expect("live body");
            assert_eq!(bytes, Bytes::from_static(b"live"));
            assert_eq!(body_polls.get(), 2);
            assert_eq!(body_drops.get(), 1);
            assert!(!aborted.get());
        }
    }

    #[test]
    fn pending_drop_or_cancel_releases_cached_response_body_once() {
        for checked in [false, true] {
            for cancel in [false, true] {
                let calls = Rc::new(Cell::new(0));
                let body_drops = Rc::new(Cell::new(0));
                let release_body = owner(&body_drops);
                let body = Body::from_stream(stream::poll_fn(move |_| {
                    let _retain_owner = &release_body;
                    panic!("dropped response body must never be polled");
                }));
                let counts = CompletionCounts::default();
                let mut pending = OutboundPending::new(
                    completion(Ok(response(body)), &counts, false),
                    owner(&calls),
                );
                if checked {
                    assert!(pending.is_ready());
                }
                if cancel {
                    pending.cancel();
                } else {
                    drop(pending);
                }
                assert_eq!(counts.polls.get(), usize::from(checked));
                assert_eq!(counts.drops.get(), 1);
                assert_eq!(body_drops.get(), 1);
                assert_eq!(calls.get(), 1);
            }
        }
    }

    #[test]
    fn pending_collection_registers_real_waker_after_readiness_probe() {
        let (sender, receiver) = oneshot::channel();
        let calls = Rc::new(Cell::new(0));
        let mut pending = OutboundPending::new(
            Box::pin(async move { receiver.await.map_err(EdgeError::internal) }),
            owner(&calls),
        );
        assert!(!pending.is_ready());
        let mut collect = pending.collect();
        let count = Arc::new(WakeCount::default());
        let waker = waker_ref(&count);
        let mut context = Context::from_waker(&waker);
        assert!(collect.as_mut().poll(&mut context).is_pending());
        sender
            .send(response(Body::from("scheduled")))
            .expect("receiver alive");
        assert!(count.0.load(Ordering::SeqCst) > 0);
        let Poll::Ready(Ok(response)) = collect.as_mut().poll(&mut context) else {
            panic!("scheduled completion must wake collector");
        };
        assert_eq!(response.body().as_bytes(), Some(b"scheduled".as_slice()));
        assert_eq!(calls.get(), 0);
    }
}

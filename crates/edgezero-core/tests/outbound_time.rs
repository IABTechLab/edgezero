#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use edgezero_core::{
        EdgeError, HttpClient, IndependentOutboundProgress, MonotonicClock, MonotonicInstant,
        OutboundBatch, OutboundDispatchRequest, OutboundFuture, OutboundHead, OutboundHttpClient,
        OutboundPending, OutboundPreparedBatch, OutboundProgressRuntime, OutboundRequest,
        OutboundSendMode, OutboundTime,
    };
    use futures::executor::block_on;
    use futures_util::future::{poll_fn, ready};

    struct Client(OutboundTime);
    struct DropCount(Arc<AtomicUsize>);

    impl OutboundHttpClient for Client {
        fn begin(&self, _request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
            Err(EdgeError::bad_request("unused fixture dispatch"))
        }
        fn dispatch_head(&self, _request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
            Box::pin(ready(Err(EdgeError::bad_request(
                "unused fixture dispatch",
            ))))
        }
        fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
            Rc::new(IndependentOutboundProgress)
        }
        fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
            OutboundBatch::cutoff(batch.into_slots().0)
        }
        fn time(&self) -> OutboundTime {
            self.0.clone()
        }
        fn validate_supported_options(
            &self,
            _request: &OutboundRequest,
            _mode: OutboundSendMode,
        ) -> Result<(), EdgeError> {
            Ok(())
        }
    }

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn facade_time_preserves_custom_clock_factory_and_cancellation_without_eager_work() {
        let start = MonotonicInstant::now();
        let samples = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&samples);
        let clock = MonotonicClock::new(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            start
        });
        let deadline = clock.deadline_after(Duration::from_secs(1));
        samples.store(0, Ordering::SeqCst);
        let registrations = Arc::new(AtomicUsize::new(0));
        let cancellations = Arc::new(AtomicUsize::new(0));
        let registered = Arc::clone(&registrations);
        let cancelled = Arc::clone(&cancellations);
        let time = OutboundTime::new(clock, move |actual, paired_clock| {
            assert_eq!(actual.instant(), deadline.instant());
            assert_eq!(paired_clock.now(), start);
            registered.fetch_add(1, Ordering::SeqCst);
            let guard = DropCount(Arc::clone(&cancelled));
            Box::pin(poll_fn(move |_cx| {
                let _retained = &guard;
                Poll::Pending
            }))
        });
        let client = HttpClient::with_client(Client(time));
        let pair = client.time();
        assert_eq!(samples.load(Ordering::SeqCst), 0);
        assert_eq!(registrations.load(Ordering::SeqCst), 0);
        assert_eq!(pair.clock().now(), start);
        let mut wait = pair.wait_until(deadline);
        assert_eq!(registrations.load(Ordering::SeqCst), 0);
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(registrations.load(Ordering::SeqCst), 1);
        drop(wait);
        assert_eq!(cancellations.load(Ordering::SeqCst), 1);
        assert!(samples.load(Ordering::SeqCst) > 1);
    }

    #[test]
    fn facade_time_preserves_timer_error_and_expired_wait_behavior() {
        let start = MonotonicInstant::now();
        let calls = Arc::new(AtomicUsize::new(0));
        let captured = Arc::clone(&calls);
        let time = OutboundTime::new(MonotonicClock::new(move || start), move |_, _| {
            captured.fetch_add(1, Ordering::SeqCst);
            Box::pin(ready(Err(EdgeError::bad_request("fixture timer failure"))))
        });
        let client = HttpClient::with_client(Client(time));
        let pair = client.time();
        let expired = edgezero_core::Deadline::at_instant(start);
        block_on(pair.wait_until(expired)).expect("already expired wait");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let future = pair.clock().deadline_after(Duration::from_secs(1));
        let error = block_on(pair.wait_until(future)).expect_err("timer error");
        assert_eq!(error.message(), "fixture timer failure");
        assert_eq!(error.status().as_u16(), 400);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

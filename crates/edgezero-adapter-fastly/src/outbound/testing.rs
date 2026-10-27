//! Obtain opaque test inputs through the real facade, never by reconstructing core policies.

use futures_util::future::pending;
use std::cell::RefCell;
use std::iter::repeat_with;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "fastly")]
use std::task::{Context, Waker};
use std::time::Duration;

use edgezero_core::{
    Deadline, EdgeError, HttpClient, IndependentOutboundProgress, MonotonicClock, MonotonicInstant,
    OutboundBatch, OutboundBatchContext, OutboundDispatchRequest, OutboundExchangeOwner,
    OutboundFuture, OutboundHead, OutboundHttpClient, OutboundPending, OutboundPreparedBatch,
    OutboundProgressRuntime, OutboundRequest, OutboundSendMode, OutboundTime,
};

std::thread_local! {
    static DISPATCH: RefCell<Option<OutboundDispatchRequest>> = const { RefCell::new(None) };
    static BATCH: RefCell<Option<OutboundBatchContext>> = const { RefCell::new(None) };
}

struct Capture(OutboundTime, bool);

impl OutboundHttpClient for Capture {
    fn begin(&self, _: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        Err(EdgeError::internal(anyhow::anyhow!(
            "capture fixture does not begin exchanges"
        )))
    }

    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        DISPATCH.with(|saved| saved.replace(Some(request)));
        Box::pin(pending())
    }

    fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        Rc::new(IndependentOutboundProgress)
    }

    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        let (context, _) = batch.into_slots();
        if !self.1 {
            return OutboundBatch::cutoff(context);
        }
        let cutoff = context.observation_cutoff();
        BATCH.with(|saved| saved.replace(Some(context)));
        HttpClient::with_client(Capture(self.0.clone(), false))
            .start_batch_until(Vec::new(), cutoff)
    }

    fn time(&self) -> OutboundTime {
        self.0.clone()
    }

    fn validate_supported_options(
        &self,
        _: &OutboundRequest,
        _: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        Ok(())
    }
}

#[cfg(feature = "fastly")]
pub(super) fn dispatch(request: OutboundRequest, time: OutboundTime) -> OutboundDispatchRequest {
    let mut future = HttpClient::with_client(Capture(time, false)).send(request);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    DISPATCH.with(|saved| {
        saved
            .borrow_mut()
            .take()
            .expect("facade dispatched test request")
    })
}

pub(crate) fn batch_context(slot_count: usize) -> OutboundBatchContext {
    batch_context_at(slot_count, instant(), Duration::from_secs(60))
}

pub(super) fn batch_context_at(
    slot_count: usize,
    start: MonotonicInstant,
    duration: Duration,
) -> OutboundBatchContext {
    let first = AtomicBool::new(true);
    let observed = start.checked_add(duration.checked_div(2).unwrap()).unwrap();
    let time = OutboundTime::new(
        MonotonicClock::new(move || {
            if first.swap(false, Ordering::SeqCst) {
                start
            } else {
                observed
            }
        }),
        |_, _| Box::pin(pending()),
    );
    let client = HttpClient::with_client(Capture(time, true));
    let requests = repeat_with(|| {
        OutboundRequest::get("https://example.com/")
            .unwrap()
            .timeout(duration)
    })
    .take(slot_count)
    .collect();
    drop(client.start_batch_until(
        requests,
        Deadline::at_instant(start.checked_add(duration).unwrap()),
    ));
    BATCH.with(|saved| {
        saved
            .borrow_mut()
            .take()
            .expect("facade prepared batch context")
    })
}

#[cfg(feature = "fastly")]
pub(crate) fn batch_context_using(
    slot_count: usize,
    time: OutboundTime,
    observation_cutoff: Deadline,
) -> OutboundBatchContext {
    let requests = repeat_with(|| {
        OutboundRequest::get("https://example.com/")
            .unwrap()
            .timeout(Duration::from_secs(60))
    })
    .take(slot_count)
    .collect();
    drop(
        HttpClient::with_client(Capture(time, true))
            .start_batch_until(requests, observation_cutoff),
    );
    BATCH.with(|saved| {
        saved
            .borrow_mut()
            .take()
            .expect("facade prepared timed context")
    })
}

#[cfg(not(feature = "fastly"))]
pub(crate) fn client(time: OutboundTime) -> HttpClient {
    HttpClient::with_client(Capture(time, false))
}

pub(crate) fn owner() -> OutboundExchangeOwner {
    OutboundExchangeOwner::new(|| {})
}

std::thread_local! { static START: MonotonicInstant = MonotonicInstant::now(); }
pub(crate) fn instant() -> MonotonicInstant {
    START.with(|start| *start)
}

#[cfg(feature = "fastly")]
pub(super) fn dispatch_at(
    request: OutboundRequest,
    time: OutboundTime,
    start: MonotonicInstant,
) -> OutboundDispatchRequest {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let capturing = Arc::new(AtomicBool::new(true));
    let during_preparation = Arc::clone(&capturing);
    let original = time.clock().clone();
    let clock = MonotonicClock::new(move || {
        if during_preparation.load(Ordering::SeqCst) {
            start
        } else {
            original.now()
        }
    });
    let prepared = dispatch(
        request,
        OutboundTime::new(clock, move |deadline, _| time.wait_until(deadline)),
    );
    capturing.store(false, Ordering::SeqCst);
    prepared
}

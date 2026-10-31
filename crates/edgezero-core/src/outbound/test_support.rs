//! Conformance checks over real adapters, enabled only for test consumers.

use super::*;
use crate::error::BudgetSource;
use futures_util::future::poll_fn;
use futures_util::stream::{once, poll_fn as stream_poll_fn};
use std::future::Future;
use std::io::Error as IoError;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

#[derive(Default)]
struct Calls {
    bodies_dropped: AtomicUsize,
    bodies_polled: AtomicUsize,
    dispatches: AtomicUsize,
    timers: AtomicUsize,
}

struct Boundary<Client> {
    calls: Arc<Calls>,
    native: Client,
}

struct SourceDrop(Arc<Calls>);

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for SourceDrop {
    fn drop(&mut self) {
        self.0.bodies_dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl<Client: OutboundHttpClient> OutboundHttpClient for Boundary<Client> {
    fn begin(&self, _request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        self.calls.dispatches.fetch_add(1, Ordering::SeqCst);
        Err(invalid())
    }

    fn dispatch_head(&self, _request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        self.calls.dispatches.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(invalid()) })
    }

    fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        self.native.progress()
    }

    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        let (context, slots) = batch.into_slots();
        if !slots.is_empty() {
            self.calls
                .dispatches
                .fetch_add(slots.len(), Ordering::SeqCst);
            return OutboundBatch::from_driver(
                context,
                once(async { OutboundBatchDriverEvent::Failed(invalid()) }),
                OutboundExchangeOwner::new(|| {}),
            );
        }
        self.native
            .start_batch_until(OutboundPreparedBatch::new(context, slots))
    }

    fn time(&self) -> OutboundTime {
        self.native.time()
    }

    fn validate_supported_options(
        &self,
        request: &OutboundRequest,
        mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        self.native.validate_supported_options(request, mode)
    }
}

fn invalid() -> EdgeError {
    EdgeError::internal(IoError::other("outbound preflight conformance failed"))
}

fn ensure(condition: bool) -> Result<(), EdgeError> {
    if condition { Ok(()) } else { Err(invalid()) }
}

async fn bounded_error<Operation>(operation: Operation) -> Result<EdgeError, EdgeError>
where
    Operation: Future<Output = Result<EdgeError, EdgeError>>,
{
    let mut pending = Box::pin(operation);
    let mut remaining = 16_u8;
    // A validation-order regression must fail, not hang on the pending source.
    poll_fn(move |context| {
        if remaining == 0 {
            return Poll::Ready(Err(invalid()));
        }
        remaining = remaining.saturating_sub(1);
        match pending.as_mut().poll(context) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => {
                context.waker().wake_by_ref();
                Poll::Pending
            }
        }
    })
    .await
}

async fn error_for(
    client: &HttpClient,
    mode: OutboundSendMode,
    request: OutboundRequest,
    cutoff: Deadline,
) -> Result<EdgeError, EdgeError> {
    match mode {
        OutboundSendMode::Send => client.send(request).await.err().ok_or_else(invalid),
        OutboundSendMode::Begin => client.begin(request).await.err().ok_or_else(invalid),
        OutboundSendMode::Follow => {
            let policies = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&policies);
            let result = client
                .follow(request, OutboundRedirectLimit::new(1)?, move |_| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Err::<OutboundRedirectAction, _>(())
                })
                .await;
            ensure(policies.load(Ordering::SeqCst) == 0)?;
            match result {
                Err(OutboundFollowFailure::Hop { index: 0, error }) => Ok(error),
                _ => Err(invalid()),
            }
        }
        OutboundSendMode::Batch => {
            let result = client
                .start_batch_until(vec![request], cutoff)
                .collect()
                .await
                .map_err(|failure| failure.into_parts().0)?;
            ensure(result.termination() == OutboundBatchTermination::Completed)?;
            let (mut slots, _) = result.into_parts();
            ensure(slots.len() == 1)?;
            let Some(OutboundCollectedSlot::Resolved(slot)) = slots.pop() else {
                return Err(invalid());
            };
            ensure(slot.elapsed.is_zero())?;
            slot.outcome.err().ok_or_else(invalid)
        }
    }
}

/// Checks unsupported-option precedence without entering transport or polling a body.
///
/// # Errors
/// Returns an internal error if the adapter violates the supplied unsupported-mode matrix.
#[inline]
pub async fn check_unsupported_image_optimizer_preflight<Client, Factory>(
    factory: Factory,
    adapter: &str,
    modes: &[OutboundSendMode],
) -> Result<(), EdgeError>
where
    Client: OutboundHttpClient + 'static,
    Factory: FnOnce(OutboundTime) -> Client,
{
    let expected: &[OutboundSendMode] = match adapter {
        "fastly" => &[OutboundSendMode::Begin, OutboundSendMode::Batch],
        "axum" | "cloudflare" | "spin" => &[
            OutboundSendMode::Send,
            OutboundSendMode::Follow,
            OutboundSendMode::Begin,
            OutboundSendMode::Batch,
        ],
        _ => return Err(invalid()),
    };
    ensure(modes == expected)?;
    let now = MonotonicInstant::now();
    let calls = Arc::new(Calls::default());
    let timer_calls = Arc::clone(&calls);
    let clock = MonotonicClock::new(move || now);
    let cutoff = clock.deadline_after(Duration::from_secs(1));
    let time = OutboundTime::new(clock, move |_, _| {
        timer_calls.timers.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(invalid()) })
    });
    let client = HttpClient::with_client(Boundary {
        calls: Arc::clone(&calls),
        native: factory(time),
    });
    for (index, mode) in modes.iter().copied().enumerate() {
        for variant in 0_u8..3 {
            let mut request = OutboundRequest::get("http://127.0.0.1:9/image")?
                .deadline(Deadline::at_instant(now));
            if variant != 1 {
                request = request.image_optimizer(
                    OutboundImageOptimizerOptions::new()
                        .region(OutboundImageOptimizerRegion::UsEast),
                );
            }
            if variant == 2 {
                let observed = Arc::clone(&calls);
                let source = SourceDrop(Arc::clone(&calls));
                request = request.body(Body::from_stream(stream_poll_fn(move |_| {
                    let _keep_alive = &source;
                    observed.bodies_polled.fetch_add(1, Ordering::SeqCst);
                    Poll::<Option<Result<Bytes, EdgeError>>>::Pending
                })));
            }
            let error = bounded_error(error_for(&client, mode, request, cutoff)).await?;
            ensure(if variant == 1 {
                matches!(
                    error,
                    EdgeError::GatewayTimeout {
                        cause: BudgetSource::RequestDeadline,
                        ..
                    }
                )
            } else {
                matches!(error, EdgeError::BadRequest { .. })
                    && error.message().contains("image_optimizer")
                    && error.message().contains(adapter)
            })?;
            ensure(
                calls.dispatches.load(Ordering::SeqCst) == 0
                    && calls.timers.load(Ordering::SeqCst) == 0
                    && calls.bodies_polled.load(Ordering::SeqCst) == 0,
            )?;
        }
        ensure(calls.bodies_dropped.load(Ordering::SeqCst) == index.saturating_add(1))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::cell::Cell;

    #[test]
    fn pending_preflight_has_a_finite_poll_bound() {
        let polls = Cell::new(0_usize);
        let pending = poll_fn(|_| {
            polls.set(polls.get().saturating_add(1));
            Poll::<Result<EdgeError, EdgeError>>::Pending
        });
        let error = block_on(bounded_error(pending)).expect_err("pending preflight must fail");
        assert!(matches!(error, EdgeError::Internal { .. }));
        assert_eq!(polls.get(), 16);
    }
}

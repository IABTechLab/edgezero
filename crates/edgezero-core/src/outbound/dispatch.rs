use std::mem::take;
use std::task::Poll;

use futures_util::StreamExt as _;
use futures_util::future::poll_fn;

use super::{
    HttpClient, Method, OutboundDispatchRequest, OutboundRequest, OutboundRequestParts,
    OutboundResponseContext, OutboundScope, OutboundSendMode, OutboundTime, ResponseBodyPolicy,
    normalize_for_dispatch, validate_for_dispatch,
};
use crate::error::EdgeError;
use crate::http::StatusCode;
use crate::time::{
    Deadline, DispatchBudget, MonotonicClock, MonotonicInstant, dispatch_budget_inputs,
};

#[derive(Clone, Copy, Default)]
pub(super) struct DispatchBounds {
    pub observation_cutoff: Option<Deadline>,
    pub original_chain_deadline: Option<Deadline>,
}

/// Private forwarding metadata, not a second body or a public policy override.
#[derive(Clone)]
pub(crate) struct ForwardedReadDeadline(Deadline);

impl ForwardedReadDeadline {
    pub(crate) fn deadline(&self) -> Deadline {
        self.0
    }
    pub(crate) fn new(deadline: Deadline) -> Self {
        Self(deadline)
    }
}

impl OutboundDispatchRequest {
    #[must_use]
    #[inline]
    pub fn budget(&self) -> DispatchBudget {
        self.context.budget
    }

    #[must_use]
    #[inline]
    pub fn clock(&self) -> &MonotonicClock {
        self.context.time.clock()
    }

    #[must_use]
    #[inline]
    pub fn entered_at(&self) -> MonotonicInstant {
        self.context.entered_at
    }

    #[must_use]
    #[inline]
    pub fn into_parts(self) -> (OutboundRequestParts, OutboundResponseContext) {
        (self.request.into_parts(), self.context)
    }

    #[must_use]
    #[inline]
    pub fn request(&self) -> &OutboundRequest {
        &self.request
    }

    #[must_use]
    #[inline]
    pub fn scope(&self) -> &OutboundScope {
        &self.context.scope
    }

    #[must_use]
    #[inline]
    pub fn time(&self) -> &OutboundTime {
        &self.context.time
    }
}

pub(super) fn prepare(
    client: &HttpClient,
    mut request: OutboundRequest,
    mode: OutboundSendMode,
    time: OutboundTime,
    entered_at: MonotonicInstant,
    bounds: DispatchBounds,
    scope: OutboundScope,
) -> Result<OutboundDispatchRequest, EdgeError> {
    scope.check()?;
    client.inner.validate_supported_options(&request, mode)?;
    scope.check()?;
    validate_for_dispatch(&request, mode)?;
    let budget = dispatch_budget_inputs(
        request.budget_inputs(),
        entered_at,
        bounds.observation_cutoff,
        bounds.original_chain_deadline,
    )?;
    request = client.resolve_proxy_marker(request);
    normalize_for_dispatch(&mut request)?;
    request.body = take(&mut request.body).bind_outbound_scope(&scope)?;
    scope.check()?;
    let body_policy = ResponseBodyPolicy {
        max_brotli_window_bits: request.max_brotli_window_bits,
        max_chunk_bytes: request.max_chunk_bytes,
        max_decoded_response_bytes: request.max_decoded_response_bytes,
        max_decoder_bytes: request.max_decoder_bytes,
        max_encoded_response_bytes: request.max_encoded_response_bytes,
        response_decoding: request.response_decoding,
        response_mode: request.response_mode,
        status: StatusCode::OK,
    };
    let context = OutboundResponseContext {
        body_policy,
        budget,
        entered_at,
        max_response_header_bytes: request.max_response_header_bytes,
        max_response_header_count: request.max_response_header_count,
        mode,
        original_chain_deadline: bounds.original_chain_deadline.or(request.deadline),
        proxy_marker: request.proxy_marker,
        request_method: request.method.clone(),
        scope,
        time,
    };
    Ok(OutboundDispatchRequest { context, request })
}

pub(super) async fn settle(
    mut prepared: OutboundDispatchRequest,
) -> Result<OutboundDispatchRequest, EdgeError> {
    let scope = prepared.scope().clone();
    let time = prepared.time().clone();
    let mut previous = prepared.entered_at();
    let entry_now = settlement_now(&time, &scope, &mut previous)?;
    if !matches!(prepared.request.method, Method::GET | Method::HEAD)
        || !prepared.request.body.is_stream()
    {
        if prepared.budget().deadline.is_expired_at(entry_now) {
            return Err(EdgeError::gateway_timeout_caused(
                "outbound request deadline exceeded",
                prepared.budget().cause,
            ));
        }
        return Ok(prepared);
    }
    let outbound = prepared.budget();
    let inbound = prepared.request.inbound_read_deadline;
    let deadline = inbound
        .filter(|bound| bound.instant() <= outbound.deadline.instant())
        .unwrap_or(outbound.deadline);
    let mut timer = time.wait_until(deadline);
    let body = take(&mut prepared.request.body);
    let mut source = body.into_stream().map_err(|unexpected_body| {
        drop(unexpected_body);
        EdgeError::internal(anyhow::anyhow!(
            "outbound settlement lost its streamed body"
        ))
    })?;
    poll_fn(|cx| {
        settlement_now(&time, &scope, &mut previous)?;
        // The paired waiter preserves registered faults even at expiry. An expired unarmed
        // wait completes without creating a timer, and the body is still never polled.
        match timer.as_mut().poll(cx) {
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {
                return Poll::Ready(Err(settlement_expiry(
                    inbound,
                    outbound,
                    time.clock().now(),
                )
                .unwrap_or_else(|| {
                    EdgeError::internal(anyhow::anyhow!(
                        "settlement timer completed before its deadline"
                    ))
                })));
            }
            Poll::Pending => {}
        }
        scope.check()?;
        let polled = source.poll_next_unpin(cx);
        let after = settlement_now(&time, &scope, &mut previous)?;
        if let Some(error) = settlement_expiry(inbound, outbound, after) {
            return Poll::Ready(Err(error));
        }
        match polled {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Err(error)),
            Poll::Ready(Some(Ok(bytes))) if !bytes.is_empty() => Poll::Ready(Err(
                EdgeError::bad_request("GET/HEAD request must not carry a body"),
            )),
            Poll::Ready(Some(Ok(_))) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    })
    .await?;
    drop(source);
    drop(timer);
    let released_at = settlement_now(&time, &scope, &mut previous)?;
    if outbound.deadline.is_expired_at(released_at) {
        return Err(EdgeError::gateway_timeout_caused(
            "outbound request deadline exceeded",
            outbound.cause,
        ));
    }
    Ok(prepared)
}

fn settlement_expiry(
    inbound: Option<Deadline>,
    outbound: DispatchBudget,
    now: MonotonicInstant,
) -> Option<EdgeError> {
    if inbound.is_some_and(|bound| {
        bound.instant() <= outbound.deadline.instant() && bound.is_expired_at(now)
    }) {
        Some(EdgeError::request_timeout(
            "inbound body read deadline exceeded",
        ))
    } else if outbound.deadline.is_expired_at(now) {
        Some(EdgeError::gateway_timeout_caused(
            "outbound request deadline exceeded",
            outbound.cause,
        ))
    } else {
        None
    }
}

fn settlement_now(
    time: &OutboundTime,
    scope: &OutboundScope,
    previous: &mut MonotonicInstant,
) -> Result<MonotonicInstant, EdgeError> {
    scope.check()?;
    let now = time.clock().now();
    scope.check()?;
    if now < *previous {
        return Err(EdgeError::internal(anyhow::anyhow!(
            "monotonic clock moved backwards during outbound settlement"
        )));
    }
    *previous = now;
    Ok(now)
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use bytes::Bytes;
    use futures::executor::block_on;
    use futures_util::future::poll_fn;
    use futures_util::{FutureExt as _, StreamExt as _, stream};

    use crate::http::{HeaderMap, Method, StatusCode, Uri, request_builder};
    use crate::{
        Body, Deadline, EdgeError, HttpClient, IndependentOutboundProgress, MonotonicClock,
        MonotonicInstant, OutboundBatch, OutboundBatchDriverEvent, OutboundDispatchRequest,
        OutboundExchangeOwner, OutboundFuture, OutboundHead, OutboundHeaderFidelity,
        OutboundHeaderInput, OutboundHttpClient, OutboundPending, OutboundPreparedBatch,
        OutboundProgressRuntime, OutboundRequest, OutboundScope, OutboundScopeOwner,
        OutboundSendMode, OutboundTime,
    };

    #[derive(Clone)]
    struct Fake {
        dispatches: Arc<AtomicUsize>,
        state: Arc<Mutex<(MonotonicInstant, Option<Waker>)>>,
        timers: Arc<AtomicUsize>,
        uploads: Arc<Mutex<Vec<Upload>>>,
    }

    struct Client {
        fake: Fake,
        refuse: bool,
    }

    #[derive(Debug, Eq, PartialEq)]
    struct Upload {
        body: Bytes,
        method: Method,
        mode: OutboundSendMode,
        streamed: bool,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("original operational timer fault")]
    struct OperationalTimerFault(Arc<()>);

    impl Fake {
        fn advance(&self, duration: Duration) {
            let wake = {
                let mut state = self.state.lock().expect("clock");
                state.0 = state.0.checked_add(duration).expect("fake advancement");
                state.1.take()
            };
            if let Some(waker) = wake {
                waker.wake();
            }
        }

        fn client(&self, refuse: bool) -> HttpClient {
            HttpClient::with_client(Client {
                fake: self.clone(),
                refuse,
            })
        }

        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new((MonotonicInstant::now(), None))),
                dispatches: Arc::new(AtomicUsize::new(0)),
                timers: Arc::new(AtomicUsize::new(0)),
                uploads: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn pair(&self) -> OutboundTime {
            let clock_state = Arc::clone(&self.state);
            let clock = MonotonicClock::new(move || clock_state.lock().expect("clock").0);
            let wait_state = Arc::clone(&self.state);
            let timers = Arc::clone(&self.timers);
            OutboundTime::new(clock, move |deadline, _| {
                timers.fetch_add(1, Ordering::SeqCst);
                let state = Arc::clone(&wait_state);
                Box::pin(poll_fn(move |cx| {
                    let mut current = state.lock().expect("timer");
                    if deadline.is_expired_at(current.0) {
                        Poll::Ready(Ok(()))
                    } else {
                        current.1 = Some(cx.waker().clone());
                        Poll::Pending
                    }
                }))
            })
        }
    }

    impl OutboundHttpClient for Client {
        fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
            let head = block_on(self.dispatch_head(request))?;
            let outcome = block_on(head.finish());
            Ok(OutboundPending::new(
                Box::pin(async move { outcome }),
                OutboundExchangeOwner::new(|| {}),
            ))
        }

        fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
            self.fake.dispatches.fetch_add(1, Ordering::SeqCst);
            let (parts, context) = request.into_parts();
            let uploads = Arc::clone(&self.fake.uploads);
            Box::pin(async move {
                let streamed = parts.body.is_stream();
                let bytes = parts.body.into_bytes_bounded(64).await?;
                uploads.lock().expect("upload recording").push(Upload {
                    body: bytes,
                    method: parts.method,
                    mode: context.mode(),
                    streamed,
                });
                OutboundHead::new(
                    StatusCode::OK,
                    OutboundHeaderInput::new(
                        "fixture",
                        HeaderMap::new(),
                        OutboundHeaderFidelity::AdapterVisibleFields,
                    ),
                    stream::empty().boxed_local(),
                    context,
                    OutboundExchangeOwner::new(|| {}),
                )
            })
        }

        fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
            Rc::new(IndependentOutboundProgress)
        }

        fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
            let (context, slots) = batch.into_slots();
            if slots.is_empty() {
                return OutboundBatch::cutoff(context);
            }
            let events = slots
                .into_iter()
                .map(|(index, request)| {
                    let outcome = block_on(self.dispatch_head(request))
                        .and_then(|head| block_on(head.finish()));
                    OutboundBatchDriverEvent::Item {
                        index,
                        completed_at: context.clock().now(),
                        outcome,
                    }
                })
                .collect::<Vec<_>>();
            OutboundBatch::from_driver(
                context,
                stream::iter(events),
                OutboundExchangeOwner::new(|| {}),
            )
        }

        fn time(&self) -> OutboundTime {
            self.fake.pair()
        }

        fn validate_supported_options(
            &self,
            _: &OutboundRequest,
            _: OutboundSendMode,
        ) -> Result<(), EdgeError> {
            if self.refuse {
                Err(EdgeError::bad_request("unsupported fixture option"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn prepared_upload_revokes_a_reentrant_chunk_before_the_transport_can_write() {
        let fake = Fake::new();
        let owner = Rc::new(OutboundScopeOwner::callback());
        let close_from_source = Rc::clone(&owner);
        let request = OutboundRequest::post("https://example.com/")
            .expect("request")
            .body(Body::from_stream(stream::poll_fn(move |_| {
                close_from_source.close();
                Poll::Ready(Some(Ok(Bytes::from_static(b"must not be written"))))
            })));
        let time = fake.pair();
        let prepared = owner
            .with(|| {
                super::prepare(
                    &fake.client(false),
                    request,
                    OutboundSendMode::Send,
                    time.clone(),
                    time.clock().now(),
                    super::DispatchBounds::default(),
                    OutboundScope::capture(Rc::new(IndependentOutboundProgress)),
                )
            })
            .expect("prepared upload");
        let (parts, _context) = prepared.into_parts();
        let mut source = parts.body.into_stream().expect("streamed upload");
        let observed = block_on(source.next());
        assert!(
            matches!(observed, Some(Err(EdgeError::Internal { .. }))),
            "reentrant closure must suppress the source chunk before a provider write"
        );
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn fixture_expired_batch_preserves_cutoff_without_dispatch() {
        use crate::{OutboundBatchTermination, OutboundCollectedSlot};

        let fake = Fake::new();
        let observation_cutoff = Deadline::at_instant(fake.pair().clock().now());
        let results = block_on(
            fake.client(false)
                .start_batch_until(
                    vec![OutboundRequest::get("https://example.com/").expect("request")],
                    observation_cutoff,
                )
                .collect(),
        )
        .expect("an expired observation cutoff is not driver EOF");
        assert_eq!(results.termination(), OutboundBatchTermination::Cutoff);
        assert!(matches!(
            results.slots(),
            [OutboundCollectedSlot::Unresolved]
        ));
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 0);
        assert_eq!(fake.timers.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn invocation_budget_is_not_restarted_before_first_poll() {
        let fake = Fake::new();
        let send = fake.client(false).send(
            OutboundRequest::get("https://example.com/")
                .expect("request")
                .timeout(Duration::from_secs(1)),
        );
        fake.advance(Duration::from_secs(2));
        assert!(matches!(
            block_on(send),
            Err(EdgeError::GatewayTimeout { .. })
        ));
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn settlement_timer_wakes_without_body_data() {
        let fake = Fake::new();
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .body(Body::from_stream(stream::pending()))
            .timeout(Duration::from_secs(1));
        let mut send = fake.client(false).send(request);
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(fake.timers.load(Ordering::SeqCst), 1);
        fake.advance(Duration::from_secs(1));
        assert!(matches!(
            block_on(send),
            Err(EdgeError::GatewayTimeout { .. })
        ));
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn adapter_preflight_precedes_pending_settlement() {
        let fake = Fake::new();
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .body(Body::from_stream(stream::poll_fn(|_| {
                panic!("must not read")
            })))
            .deadline(Deadline::at_instant(fake.pair().clock().now()));
        assert!(matches!(
            block_on(fake.client(true).send(request)),
            Err(EdgeError::BadRequest { .. })
        ));
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 0);
        assert_eq!(fake.timers.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn get_head_first_nonempty_byte_is_rejected_before_dispatch() {
        let fake = Fake::new();
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .body(Body::from_stream(stream::iter([
                Ok(bytes::Bytes::new()),
                Ok(bytes::Bytes::from_static(b"x")),
            ])));
        assert!(matches!(
            block_on(fake.client(false).send(request)),
            Err(EdgeError::BadRequest { .. })
        ));
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn get_head_stream_eof_dispatches_certified_empty_body() {
        let fake = Fake::new();
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .body(Body::from_stream(stream::empty()));
        block_on(fake.client(false).send(request)).expect("empty streamed forwarding");
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn facade_upload_rules_preserve_forwarded_begin_and_streamed_send() {
        let fake = Fake::new();
        let client = fake.client(false);
        let rejected_polls = Arc::new(AtomicUsize::new(0));
        let rejected_source = Arc::clone(&rejected_polls);
        let rejected = OutboundRequest::post("https://example.com/")
            .expect("request")
            .body(Body::from_stream(stream::poll_fn(move |_| {
                rejected_source.fetch_add(1, Ordering::SeqCst);
                Poll::Pending
            })));
        let Err(error) = block_on(client.begin(rejected)) else {
            panic!("streamed begin must be refused");
        };
        assert!(matches!(error, EdgeError::BadRequest { .. }));
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(rejected_polls.load(Ordering::SeqCst), 0);
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 0);
        assert!(fake.uploads.lock().expect("uploads").is_empty());

        let get_polls = Arc::new(AtomicUsize::new(0));
        let empty_source = Arc::clone(&get_polls);
        let inbound = request_builder()
            .method(Method::GET)
            .uri("/inbound")
            .body(Body::from_stream(stream::poll_fn(move |_| {
                empty_source.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(None)
            })))
            .expect("inbound request");
        let forwarded =
            OutboundRequest::from_request(inbound, Uri::from_static("https://example.com/"))
                .expect("forward request");
        assert_eq!(
            get_polls.load(Ordering::SeqCst),
            0,
            "conversion cannot read the source"
        );
        let pending = client
            .begin(forwarded)
            .now_or_never()
            .expect("ready begin")
            .expect("empty forwarded GET");
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(
            fake.uploads.lock().expect("uploads").len(),
            1,
            "transport records before pending collection"
        );
        block_on(pending.collect()).expect("begun response");
        assert_eq!(get_polls.load(Ordering::SeqCst), 1);

        let sent_chunks = Arc::new(AtomicUsize::new(0));
        let sent_source = Arc::clone(&sent_chunks);
        let source =
            stream::iter([b"a".as_slice(), b"".as_slice(), b"bc".as_slice()]).map(move |bytes| {
                sent_source.fetch_add(1, Ordering::SeqCst);
                Ok(Bytes::from_static(bytes))
            });
        block_on(
            client.send(
                OutboundRequest::post("https://example.com/")
                    .expect("request")
                    .body(Body::from_stream(source)),
            ),
        )
        .expect("streamed POST send");
        assert_eq!(sent_chunks.load(Ordering::SeqCst), 3);
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 2);
        assert_eq!(
            *fake.uploads.lock().expect("uploads"),
            vec![
                Upload {
                    body: Bytes::new(),
                    method: Method::GET,
                    mode: OutboundSendMode::Begin,
                    streamed: false
                },
                Upload {
                    body: Bytes::from_static(b"abc"),
                    method: Method::POST,
                    mode: OutboundSendMode::Send,
                    streamed: true
                },
            ]
        );
    }

    #[test]
    fn forwarded_inbound_deadline_wins_earlier_and_equal_limits() {
        use super::ForwardedReadDeadline;
        use crate::http::request_builder;

        for inbound_ms in [50, 100] {
            for round_trip in [false, true] {
                for late_first_poll in [false, true] {
                    let fake = Fake::new();
                    let deadline = fake
                        .pair()
                        .clock()
                        .deadline_after(Duration::from_millis(inbound_ms));
                    let mut inbound = request_builder()
                        .method("HEAD")
                        .uri("/")
                        .body(Body::from_stream(stream::pending()))
                        .expect("inbound");
                    inbound
                        .extensions_mut()
                        .insert(ForwardedReadDeadline::new(deadline));
                    let forwarded = OutboundRequest::from_request(
                        inbound,
                        "https://example.com/".parse().expect("target"),
                    )
                    .expect("forward")
                    .timeout(Duration::from_millis(100));
                    let request = if round_trip {
                        OutboundRequest::from_parts(forwarded.into_parts()).expect("round trip")
                    } else {
                        forwarded
                    };
                    let mut send = fake.client(false).send(request);
                    if late_first_poll {
                        fake.advance(Duration::from_millis(200));
                    } else {
                        assert!(
                            send.as_mut()
                                .poll(&mut Context::from_waker(Waker::noop()))
                                .is_pending()
                        );
                        fake.advance(Duration::from_millis(inbound_ms));
                    }
                    assert!(matches!(
                        block_on(send),
                        Err(EdgeError::RequestTimeout { .. })
                    ));
                    assert_eq!(fake.dispatches.load(Ordering::SeqCst), 0);
                    if late_first_poll {
                        assert_eq!(fake.timers.load(Ordering::SeqCst), 0);
                    }
                }
            }
        }
    }

    #[test]
    fn earlier_outbound_settlement_deadline_retains_selected_cause() {
        use super::ForwardedReadDeadline;
        use crate::error::BudgetSource;
        use crate::http::request_builder;

        let fake = Fake::new();
        let mut inbound = request_builder()
            .method("GET")
            .uri("/")
            .body(Body::from_stream(stream::pending()))
            .expect("inbound");
        inbound.extensions_mut().insert(ForwardedReadDeadline::new(
            fake.pair().clock().deadline_after(Duration::from_secs(10)),
        ));
        let request =
            OutboundRequest::from_request(inbound, "https://example.com/".parse().expect("target"))
                .expect("forward")
                .timeout(Duration::from_millis(100));
        let mut begin = fake.client(false).begin(request);
        assert!(
            begin
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        fake.advance(Duration::from_millis(100));
        assert!(matches!(
            block_on(begin),
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::PerCallTimeout,
                ..
            })
        ));
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn settlement_preserves_original_nonclone_source_error() {
        #[derive(Debug, thiserror::Error)]
        #[error("fixture source failure")]
        struct Original(Arc<()>);

        let fake = Fake::new();
        let identity = Arc::new(());
        let error = EdgeError::internal(anyhow::Error::new(Original(Arc::clone(&identity))));
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .body(Body::from_stream(stream::iter([Err(error)])));
        let observed = block_on(fake.client(false).send(request)).expect_err("source failure");
        let original = observed
            .inner()
            .expect("source")
            .downcast_ref::<Original>()
            .expect("identity");
        assert!(Arc::ptr_eq(&identity, &original.0));
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn settlement_registered_timer_fault_survives_deadline_expiry() {
        let fake = Fake::new();
        let identity = Arc::new(());
        let timer_identity = Arc::clone(&identity);
        let timer_polls = Arc::new(AtomicUsize::new(0));
        let captured_polls = Arc::clone(&timer_polls);
        let timer_state = Arc::clone(&fake.state);
        let time = OutboundTime::new(fake.pair().clock().clone(), move |_, _| {
            let mut fault = Some(EdgeError::internal(OperationalTimerFault(Arc::clone(
                &timer_identity,
            ))));
            let polls = Arc::clone(&captured_polls);
            let state = Arc::clone(&timer_state);
            let mut armed = false;
            Box::pin(poll_fn(move |cx| {
                polls.fetch_add(1, Ordering::SeqCst);
                if armed {
                    Poll::Ready(Err(fault.take().expect("single operational fault")))
                } else {
                    armed = true;
                    state.lock().expect("timer registration").1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            }))
        });
        let body_polls = Arc::new(AtomicUsize::new(0));
        let captured_body_polls = Arc::clone(&body_polls);
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .timeout(Duration::from_millis(100))
            .body(Body::from_stream(stream::poll_fn(move |_| {
                captured_body_polls.fetch_add(1, Ordering::SeqCst);
                Poll::Pending
            })));
        let entered_at = time.clock().now();
        let prepared = super::prepare(
            &fake.client(false),
            request,
            OutboundSendMode::Send,
            time,
            entered_at,
            super::DispatchBounds::default(),
            OutboundScope::capture(Rc::new(IndependentOutboundProgress)),
        )
        .expect("prepared");
        let mut settlement = Box::pin(super::settle(prepared));
        assert!(
            settlement
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(timer_polls.load(Ordering::SeqCst), 1);
        assert_eq!(body_polls.load(Ordering::SeqCst), 1);
        fake.advance(Duration::from_millis(100));
        let error = block_on(settlement).expect_err("operational timer failure");
        let EdgeError::Internal { source, .. } = error else {
            panic!("registered timer fault must win over expiry, received {error:?}");
        };
        assert!(Arc::ptr_eq(
            &source
                .downcast_ref::<OperationalTimerFault>()
                .expect("original timer fault")
                .0,
            &identity
        ));
        assert_eq!(timer_polls.load(Ordering::SeqCst), 2);
        assert_eq!(body_polls.load(Ordering::SeqCst), 1);
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn settlement_expired_before_first_poll_reads_no_body_and_registers_no_timer() {
        let fake = Fake::new();
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .timeout(Duration::from_millis(100))
            .body(Body::from_stream(stream::poll_fn(|_| {
                panic!("expired settlement must not read")
            })));
        let send = fake.client(false).send(request);
        fake.advance(Duration::from_millis(100));
        assert!(matches!(
            block_on(send),
            Err(EdgeError::GatewayTimeout { .. })
        ));
        assert_eq!(fake.timers.load(Ordering::SeqCst), 0);
        assert_eq!(fake.dispatches.load(Ordering::SeqCst), 0);
    }
}

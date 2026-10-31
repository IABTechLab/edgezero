use bytes::Bytes;

use crate::context::{BodyKind, FallbackDrainOutcome, RequestContext};
use crate::error::EdgeError;
use crate::http::{Extensions, HeaderMap, Method, Request, RequestParts, Uri, Version};
use crate::ingress::AdmittedIngress;
use crate::params::PathParams;
use crate::router::RouteMetadata;

/// An admitted request view that keeps input ownership and its original policy private.
pub struct PreDispatchRequest {
    context: RequestContext,
}

impl PreDispatchRequest {
    /// Buffers the current body under `max` and the original raw input policy.
    ///
    /// # Errors
    /// Returns the existing sticky read error, 413 for overflow, or 408 for input expiry.
    #[inline]
    pub async fn body_bytes(&self, max: usize) -> Result<Bytes, EdgeError> {
        self.context.body_bytes(max).await
    }

    #[must_use]
    #[inline]
    pub fn body_kind(&self) -> BodyKind {
        self.context.body_kind()
    }

    pub(crate) async fn discard(self) -> Result<FallbackDrainOutcome, EdgeError> {
        self.context.discard_pre_dispatch_body().await
    }

    #[must_use]
    #[inline]
    pub fn extensions(&self) -> &Extensions {
        self.context.extensions()
    }

    #[inline]
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        self.context.extensions_mut()
    }

    pub(crate) fn fixed_outcome(&self) -> Option<FallbackDrainOutcome> {
        self.context.fixed_input_outcome()
    }

    #[must_use]
    #[inline]
    pub fn headers(&self) -> &HeaderMap {
        self.context.headers()
    }

    #[inline]
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        self.context.headers_mut()
    }

    pub(crate) fn into_context(
        self,
        params: PathParams,
        metadata: Option<RouteMetadata>,
        ingress: Option<AdmittedIngress>,
    ) -> RequestContext {
        self.context.finish_pre_dispatch(params, metadata, ingress)
    }

    /// Maps each input chunk synchronously while bounding total emitted bytes.
    ///
    /// # Errors
    /// Returns an existing body error or a buffered mapper error. Streaming failures are
    /// returned by the next reader; they terminate and release the original source.
    #[inline]
    pub fn map_body<F>(&mut self, max_output: usize, map: F) -> Result<(), EdgeError>
    where
        F: FnMut(Bytes) -> Result<Bytes, EdgeError> + 'static,
    {
        self.context.map_pre_dispatch_body(max_output, map)
    }

    #[must_use]
    #[inline]
    pub fn method(&self) -> &Method {
        self.context.method()
    }

    #[inline]
    pub fn method_mut(&mut self) -> &mut Method {
        &mut self.context.parts_mut().method
    }
    pub(crate) fn new(request: Request, ingress: Option<&AdmittedIngress>) -> Self {
        Self {
            context: RequestContext::new_pre_dispatch(request, ingress),
        }
    }

    #[must_use]
    #[inline]
    pub fn parts(&self) -> &RequestParts {
        self.context.parts()
    }

    #[inline]
    pub fn parts_mut(&mut self) -> &mut RequestParts {
        self.context.parts_mut()
    }

    /// Buffers bounded input and installs bounded, synchronously transformed bytes.
    ///
    /// # Errors
    /// Read, transform, and output overflow errors leave the shared body cell poisoned.
    #[inline]
    pub async fn transform_body<F>(
        &mut self,
        max_input: usize,
        max_output: usize,
        transform: F,
    ) -> Result<(), EdgeError>
    where
        F: FnOnce(Bytes) -> Result<Bytes, EdgeError>,
    {
        self.context
            .transform_pre_dispatch_body(max_input, max_output, transform)
            .await
    }

    #[must_use]
    #[inline]
    pub fn uri(&self) -> &Uri {
        self.context.uri()
    }

    #[inline]
    pub fn uri_mut(&mut self) -> &mut Uri {
        &mut self.context.parts_mut().uri
    }

    #[must_use]
    #[inline]
    pub fn version(&self) -> Version {
        self.context.version()
    }

    #[inline]
    pub fn version_mut(&mut self) -> &mut Version {
        &mut self.context.parts_mut().version
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::App;
    use crate::body::Body;
    use crate::error::{BadGatewayReason, BudgetSource};
    use crate::error_detail::{FastlyFailureCause, ProviderFailureDetail, ProviderFailurePhase};
    use crate::http::{Method, StatusCode, request_builder};
    use crate::ingress::{
        AdmissionDecision, BufferedIngressResponse, IngressAdmissionOutcome, IngressFraming,
        IngressGrant, IngressHeadAccounting, IngressHeadParts,
    };
    use crate::outbound::ForwardedReadDeadline;
    use crate::response_egress::ResponseEgressCompletion;
    use crate::router::{RouteResolution, RouterService};
    use crate::time::{MonotonicClock, MonotonicInstant};
    use futures::executor::block_on;
    use futures::stream;
    use futures::task::noop_waker_ref;
    use futures_util::StreamExt as _;
    use std::cell::Cell;
    use std::future::Future as _;
    use std::io::Error as IoError;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;

    fn request(body: Body) -> Request {
        request_builder()
            .method(Method::POST)
            .uri("/missing")
            .body(body)
            .expect("request")
    }

    fn assert_source_category(error: &EdgeError, case: usize) {
        match case {
            0 => assert!(matches!(error, EdgeError::BadRequest { .. })),
            1 => assert!(matches!(
                error,
                EdgeError::BadGateway {
                    reason: BadGatewayReason::Transport,
                    ..
                }
            )),
            2 => assert!(matches!(
                error,
                EdgeError::GatewayTimeout {
                    cause: BudgetSource::PerCallTimeout,
                    ..
                }
            )),
            3 => assert!(matches!(error, EdgeError::Internal { .. })),
            _ => {
                assert_eq!(case, 4, "five source categories");
                assert!(matches!(error, EdgeError::RequestTimeout { .. }));
            }
        }
    }

    fn assert_original_stream_errors(mapper_error: bool, map_depth: usize) {
        let detail = ProviderFailureDetail::Fastly {
            phase: ProviderFailurePhase::ResponseBodyRead,
            cause: FastlyFailureCause::ConnectionRefused,
        };
        for forwarded in [false, true] {
            for (case, error) in [
                EdgeError::bad_request_with_detail("source rejected", detail),
                EdgeError::bad_gateway_with_detail(
                    "source failed",
                    BadGatewayReason::Transport,
                    detail,
                ),
                EdgeError::gateway_timeout_with_detail(
                    "source timed out",
                    BudgetSource::PerCallTimeout,
                    detail,
                ),
                EdgeError::internal_with_detail(IoError::from_raw_os_error(42), detail),
                EdgeError::request_timeout("adapter source timed out"),
            ]
            .into_iter()
            .enumerate()
            {
                let expected_display = error.to_string();
                let expected_message = error.message();
                let expected_status = error.status();
                let expected_detail = error.provider_failure_detail().copied();
                let assert_fields = |observed: &EdgeError| {
                    assert_source_category(observed, case);
                    assert_eq!(observed.to_string(), expected_display);
                    assert_eq!(observed.message(), expected_message);
                    assert_eq!(observed.status(), expected_status);
                    assert_eq!(observed.provider_failure_detail(), expected_detail.as_ref());
                };
                let mut pending_error = Some(error);
                let mut wrapped = if mapper_error {
                    let body = Body::from_stream(stream::iter([Ok(Bytes::from_static(b"a"))]));
                    let mut wrapped = PreDispatchRequest::new(request(body), None);
                    wrapped
                        .map_body(4, move |_| Err(pending_error.take().expect("mapper error")))
                        .expect("install failing mapper");
                    wrapped
                } else {
                    let body = Body::from_stream(stream::poll_fn(move |_| {
                        Poll::Ready(pending_error.take().map(Err))
                    }));
                    PreDispatchRequest::new(request(body), None)
                };
                for _ in 0..map_depth {
                    wrapped.map_body(4, Ok).expect("install identity mapper");
                }
                let context = wrapped.into_context(PathParams::default(), None, None);
                let (body, retained_context) = if forwarded {
                    (
                        context.into_request().expect("reassemble").into_body(),
                        None,
                    )
                } else {
                    (context.take_body().expect("take body"), Some(context))
                };
                let mut source = body.into_stream().expect("streaming body");
                let first = block_on(source.next())
                    .expect("first item")
                    .expect_err("source error");
                assert_fields(&first);
                if case == 3 {
                    assert_eq!(
                        first
                            .inner()
                            .expect("internal source")
                            .downcast_ref::<IoError>()
                            .expect("original concrete cause")
                            .raw_os_error(),
                        Some(42_i32)
                    );
                }
                assert!(block_on(source.next()).is_none(), "source closes on error");
                if let Some(retained) = retained_context {
                    if !mapper_error && case == 4 {
                        assert!(matches!(
                            retained.fixed_input_outcome(),
                            Some(FallbackDrainOutcome::TimedOut)
                        ));
                    }
                    assert_fields(&retained.take_body().expect_err("sticky taken body"));
                    assert_fields(&retained.into_request().expect_err("sticky reassembly"));
                }
            }
        }
    }

    #[test]
    fn protected_stream_preserves_first_source_error() {
        assert_original_stream_errors(false, 0);
    }

    #[test]
    fn protected_nested_maps_preserve_first_source_error() {
        assert_original_stream_errors(false, 2);
    }

    #[test]
    fn protected_nested_maps_preserve_first_mapper_error() {
        assert_original_stream_errors(true, 1);
    }

    fn admitted(max: usize, clock: MonotonicClock) -> AdmittedIngress {
        let mut app = App::new(RouterService::builder().build());
        app.set_monotonic_clock(clock);
        app.set_ingress_admission_policy(move |head| AdmissionDecision::ReadBodyBeforeFallback {
            completion: ResponseEgressCompletion::empty(),
            grant: IngressGrant::empty(),
            max_body_bytes: max,
            read_deadline: head.read_deadline_after(Duration::from_secs(1)),
            on_exceeded: BufferedIngressResponse::text(StatusCode::BAD_REQUEST, "exceeded"),
            on_timeout: BufferedIngressResponse::text(StatusCode::REQUEST_TIMEOUT, "expired"),
        });
        let head = IngressHeadParts::from_request(
            &request(Body::empty()),
            IngressHeadAccounting::HostManaged,
            IngressFraming::HostManaged,
        )
        .into_head(app.monotonic_now(), RouteResolution::NotFound);
        let IngressAdmissionOutcome::Admitted { ingress, .. } =
            app.admit_ingress(&head).expect("admission")
        else {
            panic!("admitted ingress");
        };
        ingress
    }

    #[test]
    fn reassembly_preserves_unread_empty_and_pending_bodies_after_expiry() {
        for buffered in [false, true] {
            let start = MonotonicInstant::now();
            let now = Arc::new(Mutex::new(start));
            let observed = Arc::clone(&now);
            let ingress = admitted(
                4,
                MonotonicClock::new(move || *observed.lock().expect("clock")),
            );
            let polls = Rc::new(Cell::new(0_usize));
            let observed_polls = Rc::clone(&polls);
            let body = if buffered {
                Body::empty()
            } else {
                Body::from_stream(stream::poll_fn(move |_| {
                    observed_polls.set(observed_polls.get() + 1);
                    Poll::Pending
                }))
            };
            let wrapped = PreDispatchRequest::new(request(body), Some(&ingress));
            *now.lock().expect("clock") =
                start.checked_add(Duration::from_secs(2)).expect("advance");
            let context = wrapped.into_context(PathParams::default(), None, Some(ingress));
            let forwarded = context.into_request().expect("reassembly is not a read");
            assert!(
                forwarded
                    .extensions()
                    .get::<ForwardedReadDeadline>()
                    .is_some()
            );
            assert_eq!(forwarded.body().is_stream(), !buffered);
            assert_eq!(polls.get(), 0);
        }
    }

    #[test]
    fn expired_lazy_map_installation_does_not_read_or_record_timeout() {
        let start = MonotonicInstant::now();
        let now = Arc::new(Mutex::new(start));
        let observed = Arc::clone(&now);
        let ingress = admitted(
            4,
            MonotonicClock::new(move || *observed.lock().expect("clock")),
        );
        let polls = Rc::new(Cell::new(0_usize));
        let observed_polls = Rc::clone(&polls);
        let body = Body::from_stream(stream::poll_fn(move |_| {
            observed_polls.set(observed_polls.get() + 1);
            Poll::Pending
        }));
        let mut wrapped = PreDispatchRequest::new(request(body), Some(&ingress));
        *now.lock().expect("clock") = start.checked_add(Duration::from_secs(2)).expect("advance");
        wrapped.map_body(4, Ok).expect("installation is not a read");
        assert!(wrapped.fixed_outcome().is_none());
        assert_eq!(polls.get(), 0);
        let context = wrapped.into_context(PathParams::default(), None, Some(ingress));
        let _body = context.take_body().expect("ownership is not a read");
        assert_eq!(polls.get(), 0);
    }

    #[test]
    fn advancing_clock_records_timeout_in_the_original_guard() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let start = MonotonicInstant::now();
        let expired = start.checked_add(Duration::from_secs(2)).expect("expiry");
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&reads);
        let ingress = admitted(
            4,
            MonotonicClock::new(move || {
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    start
                } else {
                    expired
                }
            }),
        );
        let wrapped = PreDispatchRequest::new(request(Body::from("ok")), Some(&ingress));
        reads.store(0, Ordering::SeqCst);
        assert_eq!(
            block_on(wrapped.body_bytes(4))
                .expect_err("expired during read")
                .status(),
            StatusCode::REQUEST_TIMEOUT
        );
        assert!(matches!(
            wrapped.fixed_outcome(),
            Some(FallbackDrainOutcome::TimedOut)
        ));
        assert!(matches!(
            block_on(wrapped.discard()).expect("fixed timeout"),
            FallbackDrainOutcome::TimedOut
        ));
    }

    #[test]
    fn protected_read_does_not_preflight_mutated_content_length() {
        let mut wrapped = PreDispatchRequest::new(request(Body::from("ok")), None);
        wrapped
            .headers_mut()
            .insert("content-length", "100".parse().expect("length"));
        assert_eq!(
            block_on(wrapped.body_bytes(2)).expect("actual bytes"),
            Bytes::from_static(b"ok")
        );
    }

    #[test]
    fn fixed_raw_cap_survives_header_mutation_and_shrinking_map() {
        let ingress = admitted(4, MonotonicClock::default());
        let body = Body::from_stream(stream::iter([Ok(Bytes::from_static(b"hello"))]));
        let mut wrapped = PreDispatchRequest::new(request(body), Some(&ingress));
        wrapped
            .headers_mut()
            .insert("content-length", "0".parse().expect("length"));
        wrapped
            .map_body(1, |_| Ok(Bytes::new()))
            .expect("install mapper");
        assert_eq!(
            block_on(wrapped.body_bytes(32))
                .expect_err("raw cap")
                .status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        wrapped.extensions_mut().clear();
        assert!(matches!(
            wrapped.fixed_outcome(),
            Some(FallbackDrainOutcome::Exceeded)
        ));
        assert!(matches!(
            block_on(wrapped.discard()).expect("sticky outcome"),
            FallbackDrainOutcome::Exceeded
        ));
    }

    #[test]
    fn original_declared_length_cannot_be_erased_before_reading() {
        let ingress = admitted(4, MonotonicClock::default());
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let mut original = request(Body::from_stream(stream::poll_fn(move |_| {
            observed.set(observed.get() + 1);
            Poll::Ready(None)
        })));
        original
            .headers_mut()
            .insert("content-length", "5".parse().expect("length"));
        let mut wrapped = PreDispatchRequest::new(original, Some(&ingress));
        wrapped.headers_mut().clear();
        block_on(wrapped.body_bytes(32)).expect_err("original declared overflow");
        assert_eq!(polls.get(), 0);
        assert!(matches!(
            wrapped.fixed_outcome(),
            Some(FallbackDrainOutcome::Exceeded)
        ));
    }

    #[test]
    fn mapped_output_limit_is_cumulative_and_error_is_sticky() {
        let body = Body::from_stream(stream::iter([
            Ok(Bytes::from_static(b"a")),
            Ok(Bytes::from_static(b"b")),
        ]));
        let mut wrapped = PreDispatchRequest::new(request(body), None);
        wrapped
            .map_body(3, |_| Ok(Bytes::from_static(b"xx")))
            .expect("mapper");
        let first = block_on(wrapped.body_bytes(32)).expect_err("aggregate expansion");
        let ctx = wrapped.into_context(PathParams::default(), None, None);
        assert_eq!(ctx.body_kind(), BodyKind::Poisoned);
        assert_eq!(
            block_on(ctx.body_bytes(usize::MAX))
                .expect_err("sticky")
                .status(),
            first.status()
        );
        assert_eq!(
            ctx.take_body().expect_err("no escape").status(),
            first.status()
        );
    }

    #[test]
    fn buffered_transform_preserves_cache_and_updates_framing() {
        let mut original = request(Body::from("input"));
        original
            .headers_mut()
            .insert("content-length", "5".parse().expect("length"));
        let mut wrapped = PreDispatchRequest::new(original, None);
        block_on(wrapped.transform_body(5, 2, |_| Ok(Bytes::from_static(b"ok"))))
            .expect("transform");
        assert_eq!(wrapped.body_kind(), BodyKind::Cached { len: 2 });
        assert_eq!(wrapped.headers()["content-length"], "2");
        let ctx = wrapped.into_context(PathParams::default(), None, None);
        assert_eq!(ctx.body_kind(), BodyKind::Cached { len: 2 });
        assert_eq!(
            block_on(ctx.body_bytes(2)).expect("same cache"),
            Bytes::from_static(b"ok")
        );
    }

    #[test]
    fn cached_transform_input_cap_error_poison_is_sticky() {
        let mut wrapped = PreDispatchRequest::new(request(Body::from("input")), None);
        block_on(wrapped.body_bytes(5)).expect("cached input");
        assert_eq!(wrapped.body_kind(), BodyKind::Cached { len: 5 });
        let error = block_on(wrapped.transform_body(1, 5, Ok)).expect_err("transform input cap");
        assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(wrapped.body_kind(), BodyKind::Poisoned);
        assert_eq!(
            block_on(wrapped.body_bytes(5))
                .expect_err("sticky transform error")
                .status(),
            error.status()
        );
        let context = wrapped.into_context(PathParams::default(), None, None);
        assert_eq!(
            context
                .into_request()
                .expect_err("poisoned reassembly")
                .status(),
            error.status()
        );
    }

    #[test]
    fn buffered_transform_failure_cannot_restore_original_body() {
        let mut wrapped = PreDispatchRequest::new(request(Body::from("input")), None);
        let first =
            block_on(wrapped.transform_body(5, 1, |_| Ok(Bytes::from_static(b"too large"))))
                .expect_err("output cap");
        assert_eq!(first.status(), StatusCode::PAYLOAD_TOO_LARGE);
        block_on(wrapped.body_bytes(32)).expect_err("sticky transform overflow");
        wrapped
            .map_body(32, Ok)
            .expect_err("mapper cannot clear poison");
    }

    #[test]
    fn mapper_installation_is_lazy_and_retains_pending_source_waker() {
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let body = Body::from_stream(stream::poll_fn(move |cx| {
            observed.set(observed.get() + 1);
            if observed.get() == 1 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else if observed.get() == 2 {
                Poll::Ready(Some(Ok(Bytes::from_static(b"a"))))
            } else {
                Poll::Ready(None)
            }
        }));
        let mut wrapped = PreDispatchRequest::new(request(body), None);
        wrapped.map_body(1, Ok).expect("mapper");
        assert_eq!(polls.get(), 0);
        let ctx = wrapped.into_context(PathParams::default(), None, None);
        let mut source = ctx
            .take_body()
            .expect("body")
            .into_stream()
            .expect("streaming");
        assert_eq!(
            block_on(source.next()).expect("chunk").expect("mapped"),
            Bytes::from_static(b"a")
        );
        assert!(block_on(source.next()).is_none());
        assert_eq!(polls.get(), 3);
    }

    #[test]
    fn cancelled_buffering_keeps_same_poisoned_cell() {
        let body = Body::from_stream(stream::poll_fn(|_| Poll::Pending));
        let wrapped = PreDispatchRequest::new(request(body), None);
        let mut read = Box::pin(wrapped.body_bytes(16));
        let mut task = Context::from_waker(noop_waker_ref());
        assert!(read.as_mut().poll(&mut task).is_pending());
        drop(read);
        let ctx = wrapped.into_context(PathParams::default(), None, None);
        assert_eq!(ctx.body_kind(), BodyKind::Poisoned);
        assert!(
            block_on(ctx.body_bytes(16))
                .expect_err("cancelled")
                .message()
                .contains("cancelled")
        );
    }

    #[test]
    fn cached_input_does_not_acquire_a_new_deadline_at_handoff_or_discard() {
        let start = MonotonicInstant::now();
        let now = Arc::new(Mutex::new(start));
        let observed = Arc::clone(&now);
        let ingress = admitted(
            4,
            MonotonicClock::new(move || *observed.lock().expect("clock")),
        );
        let wrapped = PreDispatchRequest::new(request(Body::from("body")), Some(&ingress));
        assert_eq!(block_on(wrapped.body_bytes(4)).expect("read").len(), 4);
        *now.lock().expect("clock") = start.checked_add(Duration::from_secs(2)).expect("advance");
        assert!(wrapped.fixed_outcome().is_none());
        assert_eq!(block_on(wrapped.body_bytes(4)).expect("cache").len(), 4);
        assert!(matches!(
            block_on(wrapped.discard()).expect("already read"),
            FallbackDrainOutcome::Complete
        ));
    }

    #[test]
    fn unread_expired_terminal_snapshot_does_not_read_or_poison() {
        let start = MonotonicInstant::now();
        let now = Arc::new(Mutex::new(start));
        let observed = Arc::clone(&now);
        let ingress = admitted(
            0,
            MonotonicClock::new(move || *observed.lock().expect("clock")),
        );
        let wrapped = PreDispatchRequest::new(request(Body::empty()), Some(&ingress));
        *now.lock().expect("clock") = start.checked_add(Duration::from_secs(2)).expect("advance");
        assert!(wrapped.fixed_outcome().is_none());
        assert_eq!(wrapped.body_kind(), BodyKind::Initial);
        drop(wrapped);
    }

    #[test]
    fn timeout_seen_after_a_pending_read_remains_fixed_after_extensions_clear() {
        let start = MonotonicInstant::now();
        let now = Arc::new(Mutex::new(start));
        let observed = Arc::clone(&now);
        let ingress = admitted(
            4,
            MonotonicClock::new(move || *observed.lock().expect("clock")),
        );
        let advanced = Arc::clone(&now);
        let body = Body::from_stream(stream::poll_fn(move |_| {
            *advanced.lock().expect("clock") =
                start.checked_add(Duration::from_secs(2)).expect("advance");
            Poll::Ready(Some(Ok(Bytes::from_static(b"a"))))
        }));
        let mut wrapped = PreDispatchRequest::new(request(body), Some(&ingress));
        assert_eq!(
            block_on(wrapped.body_bytes(4))
                .expect_err("expired")
                .status(),
            StatusCode::REQUEST_TIMEOUT
        );
        wrapped.extensions_mut().clear();
        assert!(matches!(
            wrapped.fixed_outcome(),
            Some(FallbackDrainOutcome::TimedOut)
        ));
    }
}

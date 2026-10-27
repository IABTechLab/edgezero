use std::fmt;
use std::pin::Pin;
use std::str;
use std::task::{Context, Poll};

use async_stream::stream;
use bytes::Bytes;
use futures_util::future::poll_fn;
use futures_util::{Stream, StreamExt as _};

use super::{
    ContentTypeProvenance, OutboundExchangeOwner, OutboundFuture, OutboundHead,
    OutboundHeaderFidelity, OutboundHeaderInput, OutboundLocationEvidence, OutboundProxyMarker,
    OutboundResponse, OutboundResponseContext, OutboundScope, OutboundScopeKind,
    OutboundScopeOwner, OutboundSendMode, OutboundTime, ResponseBodyDisposition,
    ResponseBodyPolicy, ResponseHeadNormalization, ResponseHeaderLimiter, ResponseMode,
    apply_response_body_policy, collect_response_stream, insert_proxy_header,
    normalize_response_headers,
};
use crate::body::{Body, BodyPayload, BodyStream};
use crate::error::{BudgetSource, EdgeError};
use crate::http::header::LOCATION;
use crate::http::{HeaderMap, HeaderValue, Method, StatusCode};
use crate::time::{Deadline, DispatchBudget, MonotonicClock, MonotonicInstant};

struct OwnedPayload {
    cause: BudgetSource,
    deadline: Deadline,
    entered_at: MonotonicInstant,
    owner: Option<OutboundExchangeOwner>,
    previous: MonotonicInstant,
    scope: Option<OutboundScope>,
    source: Option<BodyStream>,
    time: Option<OutboundTime>,
    timer: Option<OutboundFuture<()>>,
}

impl OutboundResponseContext {
    #[must_use]
    #[inline]
    pub fn budget(&self) -> DispatchBudget {
        self.budget
    }

    #[must_use]
    #[inline]
    pub fn clock(&self) -> &MonotonicClock {
        self.time.clock()
    }

    #[must_use]
    #[inline]
    pub fn entered_at(&self) -> MonotonicInstant {
        self.entered_at
    }

    #[must_use]
    #[inline]
    pub fn mode(&self) -> OutboundSendMode {
        self.mode
    }

    #[must_use]
    #[inline]
    pub fn original_chain_deadline(&self) -> Option<Deadline> {
        self.original_chain_deadline
    }

    #[must_use]
    #[inline]
    pub fn request_method(&self) -> &Method {
        &self.request_method
    }

    #[must_use]
    #[inline]
    pub fn response_body_policy(&self) -> &ResponseBodyPolicy {
        &self.body_policy
    }

    #[must_use]
    #[inline]
    pub fn scope(&self) -> &OutboundScope {
        &self.scope
    }

    #[must_use]
    #[inline]
    pub fn time(&self) -> &OutboundTime {
        &self.time
    }
}

impl OutboundHeaderInput {
    #[must_use]
    #[inline]
    pub fn new(
        adapter: &'static str,
        headers: HeaderMap,
        fidelity: OutboundHeaderFidelity,
    ) -> Self {
        Self {
            adapter,
            fidelity,
            headers,
        }
    }

    /// Charges original fields, captures their evidence, normalizes, then charges the marker.
    ///
    /// Pass a limiter that has not yet charged this field section. Body disposition must be
    /// retained: normalizing a 205 a second time would lose
    /// whether its original declared length requires immediate body suppression.
    /// Unknown provider field boundaries never certify Content-Type provenance.
    ///
    /// # Errors
    /// Returns typed header-limit, protocol-framing or invalid adapter-identity errors.
    ///
    /// ```
    /// use edgezero_core::http::{HeaderMap, HeaderValue, Method, StatusCode};
    /// use edgezero_core::{OutboundHeaderFidelity, OutboundHeaderInput,
    ///     OutboundProxyMarker, ResponseHeaderLimiter};
    /// let mut headers = HeaderMap::new();
    /// headers.insert("content-type", HeaderValue::from_static("application/json"));
    /// let (headers, facts, _) = OutboundHeaderInput::new(
    ///     "custom", headers, OutboundHeaderFidelity::AdapterVisibleFields,
    /// ).normalize(
    ///     ResponseHeaderLimiter::new(Some(1024), Some(8)),
    ///     &Method::GET, StatusCode::OK, OutboundProxyMarker::Off,
    /// )?;
    /// assert_eq!(headers["content-type"], "application/json");
    /// assert_eq!(facts.content_type_provenance,
    ///     edgezero_core::ContentTypeProvenance::SingleTextUnchanged);
    /// # Ok::<(), edgezero_core::EdgeError>(())
    /// ```
    ///
    /// ```compile_fail
    /// use edgezero_core::normalize_response_headers;
    /// ```
    /// ```compile_fail
    /// use edgezero_core::outbound::insert_proxy_header;
    /// ```
    #[inline]
    pub fn normalize(
        self,
        mut limiter: ResponseHeaderLimiter,
        method: &Method,
        status: StatusCode,
        marker: OutboundProxyMarker,
    ) -> Result<
        (
            HeaderMap,
            ResponseHeadNormalization,
            OutboundLocationEvidence,
        ),
        EdgeError,
    > {
        let Self {
            adapter,
            mut headers,
            fidelity,
        } = self;
        limiter.observe(&headers)?;
        let mut location = original_location_evidence(&headers, fidelity);
        let mut normalized = normalize_response_headers(method, status, &mut headers)?;
        location.retained_singleton = location.observed_count == 1
            && !location.has_non_text
            && headers.contains_key(LOCATION);
        if fidelity == OutboundHeaderFidelity::ProviderFieldBoundariesUnknown {
            normalized.content_type_provenance = ContentTypeProvenance::Unknown;
        }
        let adapter_value = HeaderValue::from_maybe_shared(Bytes::from_static(adapter.as_bytes()))
            .map_err(|_error| {
                EdgeError::internal(anyhow::anyhow!("invalid outbound adapter identity"))
            })?;
        insert_proxy_header(&mut headers, &mut limiter, marker, adapter_value)?;
        Ok((headers, normalized, location))
    }
}

impl OutboundLocationEvidence {
    #[must_use]
    #[inline]
    pub fn fidelity(&self) -> OutboundHeaderFidelity {
        self.fidelity
    }

    #[must_use]
    #[inline]
    pub fn has_non_text(&self) -> bool {
        self.has_non_text
    }

    #[must_use]
    #[inline]
    pub fn is_missing_or_empty(&self) -> bool {
        self.missing_or_empty
    }

    #[must_use]
    #[inline]
    pub fn observed_count(&self) -> usize {
        self.observed_count
    }

    #[must_use]
    #[inline]
    pub fn retained_singleton(&self) -> bool {
        self.retained_singleton
    }
}

impl OutboundHead {
    /// Releases this exchange without reading or validating its payload.
    #[inline]
    pub fn discard(self) {
        drop(self);
    }

    /// Applies final-response policies once, within the captured absolute budget.
    ///
    /// # Errors
    /// Returns original source/timer failures, typed policy limits or the selected timeout.
    #[inline]
    #[must_use]
    pub fn finish(self) -> OutboundFuture<OutboundResponse> {
        let scope = self.context.scope.clone();
        let time = self.context.time.clone();
        let entered_at = self.context.entered_at;
        super::scope::bind_future(
            scope,
            time,
            entered_at,
            Box::pin(async move {
                let Self {
                    owner,
                    context,
                    encoded,
                    mut headers,
                    normalization,
                    status,
                    ..
                } = self;
                let clock = context.clock().clone();
                let (deadline, cause) = effective_deadline(&context);
                let mut previous = context.entered_at;
                checked_live(&clock, &mut previous, deadline, cause)?;
                let mode = context.body_policy.response_mode;
                let body = match normalization.body_disposition {
                    ResponseBodyDisposition::FramingBodyless
                    | ResponseBodyDisposition::ResetContent {
                        declared_body: true,
                    } => {
                        drop(owner);
                        drop(encoded);
                        Body::empty()
                    }
                    ResponseBodyDisposition::ResetContent {
                        declared_body: false,
                    } => {
                        let mut source = OwnedPayload::new(encoded, owner, &context, previous);
                        if let Some(item) = source.next().await {
                            item?;
                        }
                        drop(source);
                        Body::empty()
                    }
                    ResponseBodyDisposition::Payload => {
                        let shaped = apply_response_body_policy(
                            cooperative_encoded_stream(encoded),
                            &mut headers,
                            context.body_policy,
                        )?;
                        let mut source = OwnedPayload::new(shaped, owner, &context, previous);
                        match mode {
                            ResponseMode::Buffered { max_bytes } => Body::from(
                                collect_response_stream(source.boxed_local(), max_bytes).await?,
                            ),
                            ResponseMode::Streamed => {
                                let (body_scope, body_owner) = source.bind_body_scope(&context)?;
                                Body::from_parts(
                                    BodyPayload::Stream(source.boxed_local()),
                                    body_owner,
                                )
                                .bind_outbound_scope(&body_scope)?
                            }
                        }
                    }
                };
                checked_live(&clock, &mut previous, deadline, cause)?;
                Ok(OutboundResponse::new(
                    context.request_method,
                    status,
                    headers,
                    body,
                    clock,
                    normalization.content_type_provenance,
                ))
            }),
        )
    }

    #[must_use]
    #[inline]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    #[must_use]
    #[inline]
    pub fn location_evidence(&self) -> &OutboundLocationEvidence {
        &self.location_evidence
    }

    /// Captures original metadata and applies header caps before exposing a lazy body.
    ///
    /// # Errors
    /// Returns typed header-limit, protocol-framing or invalid adapter-identity errors.
    #[inline]
    pub fn new(
        status: StatusCode,
        input: OutboundHeaderInput,
        encoded: BodyStream,
        mut context: OutboundResponseContext,
        owner: OutboundExchangeOwner,
    ) -> Result<Self, EdgeError> {
        let (headers, normalization, location_evidence) = input.normalize(
            ResponseHeaderLimiter::new(
                context.max_response_header_bytes,
                context.max_response_header_count,
            ),
            &context.request_method,
            status,
            context.proxy_marker,
        )?;
        context.body_policy.status = status;
        Ok(Self {
            context,
            encoded,
            headers,
            location_evidence,
            normalization,
            owner,
            status,
        })
    }

    #[must_use]
    #[inline]
    pub fn status(&self) -> StatusCode {
        self.status
    }
}

impl fmt::Debug for OutboundHead {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboundHead")
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

impl OwnedPayload {
    fn bind_body_scope(
        &mut self,
        context: &OutboundResponseContext,
    ) -> Result<(OutboundScope, Option<OutboundScopeOwner>), EdgeError> {
        if context.scope.kind() == Some(OutboundScopeKind::ResponseBody) {
            context.scope.check()?;
            self.scope = Some(context.scope.clone());
            return Ok((context.scope.clone(), None));
        }
        let owner = OutboundScopeOwner::response_body();
        owner.accept_body_handoff()?;
        let scope = owner.with(|| OutboundScope::capture(context.scope.progress()));
        self.scope = Some(scope.clone());
        Ok((scope, Some(owner)))
    }

    fn new(
        source: BodyStream,
        owner: OutboundExchangeOwner,
        context: &OutboundResponseContext,
        previous: MonotonicInstant,
    ) -> Self {
        let (deadline, cause) = effective_deadline(context);
        Self {
            entered_at: context.entered_at,
            owner: Some(owner),
            source: Some(source),
            timer: None,
            time: Some(context.time.clone()),
            cause,
            deadline,
            previous,
            scope: None,
        }
    }

    fn poll_active(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes, EdgeError>>> {
        let Some(time) = &self.time else {
            return Poll::Ready(None);
        };
        checked_sample(time.clock(), &mut self.previous)?;
        let timer = self
            .timer
            .get_or_insert_with(|| time.wait_until(self.deadline));
        if let Poll::Ready(result) = timer.as_mut().poll(cx) {
            result?;
            return Poll::Ready(Some(Err(timeout_error(self.cause))));
        }
        let Some(source) = &mut self.source else {
            return Poll::Ready(None);
        };
        let next = source.as_mut().poll_next(cx);
        checked_live(time.clock(), &mut self.previous, self.deadline, self.cause).map_err(
            |mut selected| {
                if let EdgeError::GatewayTimeout {
                    provider_detail, ..
                } = &mut selected
                    && let Poll::Ready(Some(Err(error))) = &next
                {
                    *provider_detail = error.provider_failure_detail().copied();
                }
                selected
            },
        )?;
        next
    }

    fn release(&mut self) {
        if let Some(scope) = self.scope.take() {
            scope.with_cleanup(|| self.release_transport());
        } else {
            self.release_transport();
        }
    }

    fn release_transport(&mut self) {
        drop(self.owner.take());
        drop(self.source.take());
        drop(self.timer.take());
        drop(self.time.take());
        drop(self.scope.take());
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for OwnedPayload {
    fn drop(&mut self) {
        self.release();
    }
}

impl Stream for OwnedPayload {
    type Item = Result<Bytes, EdgeError>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = if let (Some(scope), Some(time)) = (self.scope.clone(), self.time.clone()) {
            let entered_at = self.entered_at;
            let mut read = poll_fn(|context| self.poll_active(context));
            scope
                .poll(&time, entered_at, cx, Pin::new(&mut read))
                .map(|outcome| match outcome {
                    Ok(item) => item,
                    Err(error) => Some(Err(error)),
                })
        } else {
            self.poll_active(cx)
        };
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
            self.release();
        }
        result
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.source.is_none() {
            (0, Some(0))
        } else {
            (0, None)
        }
    }
}

fn checked_live(
    clock: &MonotonicClock,
    previous: &mut MonotonicInstant,
    deadline: Deadline,
    cause: BudgetSource,
) -> Result<(), EdgeError> {
    let now = checked_sample(clock, previous)?;
    if deadline.is_expired_at(now) {
        Err(timeout_error(cause))
    } else {
        Ok(())
    }
}

fn checked_sample(
    clock: &MonotonicClock,
    previous: &mut MonotonicInstant,
) -> Result<MonotonicInstant, EdgeError> {
    let now = clock.now();
    if now < *previous {
        return Err(EdgeError::internal(anyhow::anyhow!(
            "monotonic clock moved backwards during outbound response"
        )));
    }
    *previous = now;
    Ok(now)
}

fn cooperative_encoded_stream(mut source: BodyStream) -> BodyStream {
    stream! {
        while let Some(item) = source.next().await {
            yield item;
            // A decoder may consume ready chunks without producing output. Return control
            // to the outer deadline race between reads, including empty/no-output input.
            let mut yielded = false;
            poll_fn(|cx| {
                if yielded { Poll::Ready(()) } else {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }).await;
        }
    }
    .boxed_local()
}

fn effective_deadline(context: &OutboundResponseContext) -> (Deadline, BudgetSource) {
    if let Some(chain) = context.original_chain_deadline
        && chain.instant() < context.budget.deadline.instant()
    {
        return (chain, BudgetSource::RequestDeadline);
    }
    (context.budget.deadline, context.budget.cause)
}

fn original_location_evidence(
    headers: &HeaderMap,
    fidelity: OutboundHeaderFidelity,
) -> OutboundLocationEvidence {
    let mut values = headers.get_all(LOCATION).iter();
    let first = values.next();
    let observed_count = headers.get_all(LOCATION).iter().count();
    OutboundLocationEvidence {
        observed_count,
        has_non_text: headers
            .get_all(LOCATION)
            .iter()
            .any(|value| str::from_utf8(value.as_bytes()).is_err()),
        retained_singleton: false,
        missing_or_empty: observed_count == 0
            || (observed_count == 1 && first.is_some_and(|value| value.as_bytes().is_empty())),
        fidelity,
    }
}

fn timeout_error(cause: BudgetSource) -> EdgeError {
    EdgeError::gateway_timeout_caused("outbound response deadline expired", cause)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::io::Write as _;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use bytes::Bytes;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use futures::executor::block_on;
    use futures::future::pending;
    use futures_util::future::poll_fn;
    use futures_util::task::{ArcWake, waker_ref};
    use futures_util::{StreamExt as _, stream};

    use crate::http::{HeaderMap, HeaderValue, Method, StatusCode};
    use crate::{
        BodyStream, BudgetSource, ContentTypeProvenance, DispatchBudget, EdgeError, MonotonicClock,
        OutboundExchangeOwner, OutboundHead, OutboundHeaderFidelity, OutboundHeaderInput,
        OutboundProxyMarker, OutboundResponseContext, OutboundSendMode, OutboundTime,
        ResponseBodyDisposition, ResponseBodyPolicy, ResponseDecoding, ResponseHeaderLimiter,
        ResponseLimitReason, ResponseMode,
    };

    #[derive(Clone)]
    struct FakeTime(Arc<Mutex<FakeState>>);

    struct FakeState {
        now: crate::MonotonicInstant,
        registrations: usize,
        releases: usize,
        waker: Option<Waker>,
    }

    struct TimerLease(Arc<Mutex<FakeState>>);

    struct SourceLease(Rc<Cell<usize>>);

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    #[derive(Debug, thiserror::Error)]
    #[error("fixture identity")]
    struct IdentityError(Arc<()>);

    impl FakeTime {
        fn advance(&self, duration: Duration) {
            let wake = {
                let mut state = self.0.lock().expect("time");
                state.now = state.now.checked_add(duration).expect("advance");
                state.waker.take()
            };
            if let Some(waker) = wake {
                waker.wake();
            }
        }

        fn clock(&self) -> MonotonicClock {
            let state = Arc::clone(&self.0);
            MonotonicClock::new(move || state.lock().expect("clock").now)
        }

        fn new() -> Self {
            Self(Arc::new(Mutex::new(FakeState {
                now: crate::MonotonicInstant::now()
                    .checked_add(Duration::from_hours(1))
                    .expect("future fake domain"),
                registrations: 0,
                releases: 0,
                waker: None,
            })))
        }

        fn pair(&self) -> OutboundTime {
            let shared = Arc::clone(&self.0);
            OutboundTime::new(self.clock(), move |deadline, clock| {
                let timer_state = Arc::clone(&shared);
                let paired_now = clock.now();
                let lease = {
                    let mut state = shared.lock().expect("registration");
                    assert_eq!(paired_now, state.now);
                    state.registrations = state.registrations.saturating_add(1);
                    TimerLease(Arc::clone(&shared))
                };
                Box::pin(async move {
                    let _lease = lease;
                    poll_fn(move |cx| {
                        let mut state = timer_state.lock().expect("timer");
                        if deadline.is_expired_at(state.now) {
                            Poll::Ready(Ok(()))
                        } else {
                            state.waker = Some(cx.waker().clone());
                            Poll::Pending
                        }
                    })
                    .await
                })
            })
        }

        fn response_context(&self) -> OutboundResponseContext {
            let mut captured = context(ResponseMode::Buffered { max_bytes: 16 });
            captured.time = self.pair();
            captured.entered_at = self.clock().now();
            captured.budget = DispatchBudget {
                cause: BudgetSource::PerCallTimeout,
                deadline: self.clock().deadline_after(Duration::from_millis(20)),
                duration: Duration::from_millis(20),
            };
            captured
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "Drop::pin_drop is unstable and conflicts with drop"
    )]
    impl Drop for TimerLease {
        fn drop(&mut self) {
            let mut state = self.0.lock().expect("unregister");
            state.releases = state.releases.saturating_add(1);
            state.waker = None;
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "Drop::pin_drop is unstable and conflicts with drop"
    )]
    impl Drop for SourceLease {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_add(1));
        }
    }

    impl ArcWake for WakeCount {
        fn wake(self: Arc<Self>) {
            Self::wake_by_ref(&self);
        }

        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn context(mode: ResponseMode) -> OutboundResponseContext {
        let clock = MonotonicClock::default();
        OutboundResponseContext {
            entered_at: clock.now(),
            budget: DispatchBudget {
                cause: BudgetSource::PerCallTimeout,
                deadline: clock.deadline_after(Duration::from_secs(60)),
                duration: Duration::from_secs(60),
            },
            time: OutboundTime::new(clock, |_, _| Box::pin(pending())),
            mode: OutboundSendMode::Send,
            request_method: Method::GET,
            body_policy: ResponseBodyPolicy {
                max_brotli_window_bits: 24,
                max_chunk_bytes: None,
                max_decoded_response_bytes: Some(16),
                max_decoder_bytes: crate::DEFAULT_MAX_DECODER_BYTES,
                max_encoded_response_bytes: Some(32),
                response_decoding: ResponseDecoding::Decode,
                response_mode: mode,
                status: StatusCode::OK,
            },
            max_response_header_bytes: None,
            max_response_header_count: None,
            proxy_marker: OutboundProxyMarker::On,
            original_chain_deadline: None,
            scope: crate::OutboundScope::capture(Rc::new(crate::IndependentOutboundProgress)),
        }
    }

    fn owner(releases: &Rc<Cell<usize>>) -> OutboundExchangeOwner {
        let counted = Rc::clone(releases);
        OutboundExchangeOwner::new(move || counted.set(counted.get().saturating_add(1)))
    }

    fn input(headers: HeaderMap) -> OutboundHeaderInput {
        OutboundHeaderInput::new(
            "test",
            headers,
            OutboundHeaderFidelity::AdapterVisibleFields,
        )
    }

    #[test]
    fn shared_header_policy_preserves_original_evidence_before_filtering() {
        for fidelity in [
            OutboundHeaderFidelity::AdapterVisibleFields,
            OutboundHeaderFidelity::ProviderFieldBoundariesUnknown,
        ] {
            let mut headers = HeaderMap::new();
            headers.append("content-type", HeaderValue::from_static("application/json"));
            headers.append(
                "content-type",
                HeaderValue::from_bytes(&[0xff]).expect("opaque"),
            );
            headers.append("location", HeaderValue::from_static("/next"));
            headers.append(
                "location",
                HeaderValue::from_bytes(&[0xff]).expect("opaque"),
            );
            let (retained, normalized, location) =
                OutboundHeaderInput::new("test", headers, fidelity)
                    .normalize(
                        ResponseHeaderLimiter::new(None, Some(4)),
                        &Method::GET,
                        StatusCode::FOUND,
                        OutboundProxyMarker::Off,
                    )
                    .expect("all four original fields fit");
            assert_eq!(retained.get_all("content-type").iter().count(), 1);
            assert_eq!(retained.get_all("location").iter().count(), 1);
            assert_eq!(location.observed_count(), 2);
            assert!(location.has_non_text());
            assert!(!location.retained_singleton());
            assert!(!location.is_missing_or_empty());
            assert_eq!(location.fidelity(), fidelity);
            assert_eq!(
                normalized.content_type_provenance,
                if fidelity == OutboundHeaderFidelity::AdapterVisibleFields {
                    ContentTypeProvenance::Ineligible(
                        crate::ContentTypeIneligibility::MultipleValues,
                    )
                } else {
                    ContentTypeProvenance::Unknown
                }
            );
        }
    }

    #[test]
    fn shared_header_policy_charges_filtered_fields_and_marker_once() {
        for marker in [
            OutboundProxyMarker::Off,
            OutboundProxyMarker::On,
            OutboundProxyMarker::Inherit,
        ] {
            let mut headers = HeaderMap::new();
            headers.append(
                "x-filtered",
                HeaderValue::from_bytes(&[0xff]).expect("opaque"),
            );
            let marker_enabled = marker != OutboundProxyMarker::Off;
            let count = if marker_enabled { 2 } else { 1 };
            let bytes = if marker_enabled { 31 } else { 11 };
            for (max_bytes, max_count, expected) in [
                (bytes, count, None),
                (bytes - 1, count, Some(ResponseLimitReason::HeaderBytes)),
                (bytes, count - 1, Some(ResponseLimitReason::HeaderCount)),
            ] {
                let result = input(headers.clone()).normalize(
                    ResponseHeaderLimiter::new(Some(max_bytes), Some(max_count)),
                    &Method::GET,
                    StatusCode::OK,
                    marker,
                );
                if let Some(reason) = expected {
                    assert!(
                        matches!(result, Err(EdgeError::ResponseTooLarge { reason: actual, .. }) if actual == reason)
                    );
                } else {
                    let (normalized, _, _) = result.expect("exact byte/count cap");
                    assert!(!normalized.contains_key("x-filtered"));
                    assert_eq!(normalized.contains_key("x-edgezero-proxy"), marker_enabled);
                }
            }
        }
    }

    #[test]
    fn shared_header_policy_retains_reset_and_marker_off_semantics() {
        let mut headers = HeaderMap::new();
        headers.append("content-length", HeaderValue::from_static("9"));
        headers.append("x-edgezero-proxy", HeaderValue::from_static("upstream"));
        let (retained, normalized, _) = input(headers)
            .normalize(
                ResponseHeaderLimiter::new(None, Some(2)),
                &Method::GET,
                StatusCode::RESET_CONTENT,
                OutboundProxyMarker::Off,
            )
            .expect("marker-off does not charge or replace the upstream field");
        assert_eq!(retained["x-edgezero-proxy"], "upstream");
        assert_eq!(retained["content-length"], "0");
        assert_eq!(
            normalized.body_disposition,
            ResponseBodyDisposition::ResetContent {
                declared_body: true
            }
        );
    }

    #[test]
    fn head_disposition_precedes_payload_processing() {
        for case in 0_u8..4 {
            let polls = Rc::new(Cell::new(0_usize));
            let counted = Rc::clone(&polls);
            let body: BodyStream = stream::poll_fn(move |_| {
                counted.set(counted.get().saturating_add(1));
                match case {
                    0 => Poll::Pending,
                    1 => Poll::Ready(Some(Err(EdgeError::bad_gateway("source")))),
                    2 => Poll::Ready(None),
                    _ => Poll::Ready(Some(Ok(Bytes::from_static(b"oversized")))),
                }
            })
            .boxed_local();
            let mut headers = HeaderMap::new();
            headers.insert("location", HeaderValue::from_static("/next"));
            headers.insert("content-length", HeaderValue::from_static("10485760"));
            headers.insert("content-encoding", HeaderValue::from_static("br"));
            let releases = Rc::new(Cell::new(0));
            let head = OutboundHead::new(
                StatusCode::FOUND,
                input(headers),
                body,
                context(ResponseMode::Buffered { max_bytes: 16 }),
                owner(&releases),
            )
            .expect("header construction never applies payload caps or decoding");
            assert_eq!(head.status(), StatusCode::FOUND);
            assert_eq!(head.headers()["content-length"], "10485760");
            assert_eq!(polls.get(), 0);
            head.discard();
            assert_eq!(
                polls.get(),
                0,
                "discard must never poll an intermediate body"
            );
            assert_eq!(releases.get(), 1);
        }
    }

    #[test]
    fn final_head_applies_declared_payload_limits_without_polling() {
        let polls = Rc::new(Cell::new(0_usize));
        let counted = Rc::clone(&polls);
        let body = stream::poll_fn(move |_| {
            counted.set(counted.get().saturating_add(1));
            Poll::Pending
        })
        .boxed_local();
        let mut headers = HeaderMap::new();
        headers.insert("content-length", HeaderValue::from_static("33"));
        let releases = Rc::new(Cell::new(0));
        let head = OutboundHead::new(
            StatusCode::FOUND,
            input(headers),
            body,
            context(ResponseMode::Buffered { max_bytes: 16 }),
            owner(&releases),
        )
        .expect("head");
        let error = block_on(head.finish()).expect_err("final payload cap");
        assert!(matches!(
            error,
            EdgeError::ResponseTooLarge {
                reason: ResponseLimitReason::EncodedBody,
                ..
            }
        ));
        assert_eq!(polls.get(), 0);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn location_evidence_survives_header_filtering() {
        let mut headers = HeaderMap::new();
        headers.append("location", HeaderValue::from_static("/next"));
        headers.append(
            "location",
            HeaderValue::from_bytes(&[0xff]).expect("opaque"),
        );
        let releases = Rc::new(Cell::new(0));
        let head = OutboundHead::new(
            StatusCode::FOUND,
            input(headers),
            stream::pending().boxed_local(),
            context(ResponseMode::Streamed),
            owner(&releases),
        )
        .expect("invalid location is not rejected ahead of redirect stop rules");
        assert_eq!(head.headers().get_all("location").iter().count(), 1);
        let evidence = head.location_evidence();
        assert_eq!(evidence.observed_count(), 2);
        assert!(evidence.has_non_text());
        assert!(!evidence.retained_singleton());
        assert!(!evidence.is_missing_or_empty());
        assert_eq!(
            evidence.fidelity(),
            OutboundHeaderFidelity::AdapterVisibleFields
        );
        head.discard();
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn head_finalization_releases_buffered_owner_and_transfers_streamed_owner() {
        for mode in [
            ResponseMode::Buffered { max_bytes: 16 },
            ResponseMode::Streamed,
        ] {
            let releases = Rc::new(Cell::new(0));
            let head = OutboundHead::new(
                StatusCode::OK,
                input(HeaderMap::new()),
                stream::iter([Ok(Bytes::from_static(b"body"))]).boxed_local(),
                context(mode),
                owner(&releases),
            )
            .expect("head");
            let response = block_on(head.finish()).expect("final response");
            assert_eq!(
                response.content_type_provenance(),
                ContentTypeProvenance::Ineligible(crate::ContentTypeIneligibility::Missing)
            );
            assert_eq!(
                releases.get(),
                usize::from(matches!(mode, ResponseMode::Buffered { .. }))
            );
            assert_eq!(
                block_on(response.into_bytes_bounded(16)).expect("body"),
                b"body".as_slice()
            );
            assert_eq!(releases.get(), 1);
        }
    }

    #[test]
    fn empty_response_chunks_yield_without_starving_the_deadline() {
        let polls = Rc::new(Cell::new(0_usize));
        let counted = Rc::clone(&polls);
        let body = stream::poll_fn(move |_| {
            counted.set(counted.get().saturating_add(1));
            assert!(
                counted.get() < 8,
                "ready empty chunks must not monopolize one poll"
            );
            Poll::Ready(Some(Ok(Bytes::new())))
        })
        .boxed_local();
        let releases = Rc::new(Cell::new(0));
        let head = OutboundHead::new(
            StatusCode::OK,
            input(HeaderMap::new()),
            body,
            context(ResponseMode::Buffered { max_bytes: 16 }),
            owner(&releases),
        )
        .expect("head");
        let mut finishing = head.finish();
        assert!(
            finishing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(polls.get(), 1);
        drop(finishing);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn pending_final_body_wakes_in_paired_time_and_releases_every_resource() {
        let time = FakeTime::new();
        let releases = Rc::new(Cell::new(0));
        let source_drops = Rc::new(Cell::new(0));
        let lease = SourceLease(Rc::clone(&source_drops));
        let body = stream::poll_fn(move |_| {
            let _retained = &lease;
            Poll::Pending
        })
        .boxed_local();
        let head = OutboundHead::new(
            StatusCode::OK,
            input(HeaderMap::new()),
            body,
            time.response_context(),
            owner(&releases),
        )
        .expect("head");
        let wake = Arc::new(WakeCount::default());
        let waker = waker_ref(&wake);
        let mut cx = Context::from_waker(&waker);
        let mut finishing = head.finish();
        assert!(finishing.as_mut().poll(&mut cx).is_pending());
        assert_eq!(time.0.lock().expect("time").registrations, 1);
        time.advance(Duration::from_millis(20));
        assert_eq!(wake.0.load(Ordering::Relaxed), 1);
        let Poll::Ready(Err(error)) = finishing.as_mut().poll(&mut cx) else {
            panic!("woken deadline");
        };
        assert!(matches!(
            error,
            EdgeError::GatewayTimeout {
                cause: BudgetSource::PerCallTimeout,
                ..
            }
        ));
        assert_eq!(releases.get(), 1);
        assert_eq!(source_drops.get(), 1);
        assert_eq!(time.0.lock().expect("time").releases, 1);
        drop(finishing);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn finalizer_timer_setup_error_precedes_body_poll_and_preserves_identity() {
        let identity = Arc::new(());
        let timer_identity = Arc::clone(&identity);
        let mut captured = context(ResponseMode::Buffered { max_bytes: 16 });
        captured.time = OutboundTime::new(captured.clock().clone(), move |_, _| {
            let error = EdgeError::internal(IdentityError(Arc::clone(&timer_identity)));
            Box::pin(async move { Err(error) })
        });
        let polls = Rc::new(Cell::new(0_usize));
        let counted = Rc::clone(&polls);
        let body = stream::poll_fn(move |_| {
            counted.set(counted.get().saturating_add(1));
            Poll::Pending
        })
        .boxed_local();
        let releases = Rc::new(Cell::new(0));
        let head = OutboundHead::new(
            StatusCode::OK,
            input(HeaderMap::new()),
            body,
            captured,
            owner(&releases),
        )
        .expect("head");
        let error = block_on(head.finish()).expect_err("timer setup");
        let EdgeError::Internal { source, .. } = error else {
            panic!("original internal");
        };
        assert!(Arc::ptr_eq(
            &source.downcast_ref::<IdentityError>().expect("identity").0,
            &identity
        ));
        assert_eq!(polls.get(), 0);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn head_drop_and_unpolled_finalization_release_without_body_polling() {
        for finish in [false, true] {
            let releases = Rc::new(Cell::new(0));
            let source_drops = Rc::new(Cell::new(0));
            let lease = SourceLease(Rc::clone(&source_drops));
            let body = stream::poll_fn(move |_| {
                let _retained = &lease;
                panic!("no body polling");
            })
            .boxed_local();
            let head = OutboundHead::new(
                StatusCode::OK,
                input(HeaderMap::new()),
                body,
                context(ResponseMode::Streamed),
                owner(&releases),
            )
            .expect("head");
            if finish {
                drop(head.finish());
            } else {
                drop(head);
            }
            assert_eq!(releases.get(), 1);
            assert_eq!(source_drops.get(), 1);
        }
    }

    #[test]
    fn finalizer_cancels_pending_read_and_timer_on_drop() {
        let time = FakeTime::new();
        let releases = Rc::new(Cell::new(0));
        let head = OutboundHead::new(
            StatusCode::OK,
            input(HeaderMap::new()),
            stream::pending().boxed_local(),
            time.response_context(),
            owner(&releases),
        )
        .expect("head");
        let mut finishing = head.finish();
        assert!(
            finishing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(finishing);
        assert_eq!(releases.get(), 1);
        let state = time.0.lock().expect("time");
        assert_eq!(state.registrations, 1);
        assert_eq!(state.releases, 1);
        assert!(state.waker.is_none());
    }

    #[test]
    fn ready_body_at_deadline_never_becomes_late_success() {
        let time = FakeTime::new();
        let advanced = time.clone();
        let body = stream::poll_fn(move |_| {
            advanced.advance(Duration::from_millis(20));
            Poll::Ready(Some(Ok(Bytes::from_static(b"late"))))
        })
        .boxed_local();
        let releases = Rc::new(Cell::new(0));
        let head = OutboundHead::new(
            StatusCode::OK,
            input(HeaderMap::new()),
            body,
            time.response_context(),
            owner(&releases),
        )
        .expect("head");
        assert!(matches!(
            block_on(head.finish()),
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::PerCallTimeout,
                ..
            })
        ));
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn original_chain_deadline_remains_binding_through_final_body() {
        let time = FakeTime::new();
        let mut captured = time.response_context();
        captured.original_chain_deadline =
            Some(time.clock().deadline_after(Duration::from_millis(5)));
        let releases = Rc::new(Cell::new(0));
        let head = OutboundHead::new(
            StatusCode::OK,
            input(HeaderMap::new()),
            stream::pending().boxed_local(),
            captured,
            owner(&releases),
        )
        .expect("head");
        let mut finishing = head.finish();
        let mut cx = Context::from_waker(Waker::noop());
        assert!(finishing.as_mut().poll(&mut cx).is_pending());
        time.advance(Duration::from_millis(5));
        let Poll::Ready(Err(EdgeError::GatewayTimeout {
            cause: BudgetSource::RequestDeadline,
            ..
        })) = finishing.as_mut().poll(&mut cx)
        else {
            panic!("original chain deadline");
        };
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn bodyless_and_declared_reset_bodies_bypass_payload_processing() {
        for (method, status) in [
            (Method::HEAD, StatusCode::OK),
            (Method::GET, StatusCode::NO_CONTENT),
            (Method::GET, StatusCode::NOT_MODIFIED),
            (Method::GET, StatusCode::RESET_CONTENT),
        ] {
            let mut captured = context(ResponseMode::Buffered { max_bytes: 0 });
            captured.request_method = method;
            let mut headers = HeaderMap::new();
            headers.insert("content-length", HeaderValue::from_static("99"));
            headers.insert("content-encoding", HeaderValue::from_static("br"));
            let body = stream::poll_fn(|_| panic!("suppression never reads payload")).boxed_local();
            let releases = Rc::new(Cell::new(0));
            let head = OutboundHead::new(status, input(headers), body, captured, owner(&releases))
                .expect("head");
            if status == StatusCode::RESET_CONTENT {
                assert_eq!(head.headers()["content-length"], "0");
            }
            let response = block_on(head.finish()).expect("suppressed");
            assert!(
                block_on(response.into_bytes_bounded(0))
                    .expect("empty")
                    .is_empty()
            );
            assert_eq!(releases.get(), 1);
        }
    }

    #[test]
    fn location_evidence_distinguishes_absence_empty_removed_and_unknown() {
        for (values, nominated, count, empty, retained) in [
            (vec![], false, 0, true, false),
            (vec![""], false, 1, true, true),
            (vec!["/next"], false, 1, false, true),
            (vec!["/next"], true, 1, false, false),
            (vec!["", ""], false, 2, false, false),
            (vec!["/a, /b"], false, 1, false, true),
        ] {
            for fidelity in [
                OutboundHeaderFidelity::AdapterVisibleFields,
                OutboundHeaderFidelity::ProviderFieldBoundariesUnknown,
            ] {
                let mut headers = HeaderMap::new();
                for value in &values {
                    headers.append("location", HeaderValue::from_str(value).expect("location"));
                }
                headers.append("content-type", HeaderValue::from_static("application/json"));
                if nominated {
                    headers.append("connection", HeaderValue::from_static("location"));
                }
                let releases = Rc::new(Cell::new(0));
                let head = OutboundHead::new(
                    StatusCode::FOUND,
                    OutboundHeaderInput::new("test", headers, fidelity),
                    stream::empty().boxed_local(),
                    context(ResponseMode::Streamed),
                    owner(&releases),
                )
                .expect("head");
                let evidence = head.location_evidence();
                assert_eq!(evidence.observed_count(), count);
                assert_eq!(evidence.is_missing_or_empty(), empty);
                assert_eq!(evidence.retained_singleton(), retained);
                assert_eq!(evidence.fidelity(), fidelity);
                let response = block_on(head.finish()).expect("final");
                assert_eq!(
                    response.content_type_provenance(),
                    if fidelity == OutboundHeaderFidelity::AdapterVisibleFields {
                        ContentTypeProvenance::SingleTextUnchanged
                    } else {
                        ContentTypeProvenance::Unknown
                    }
                );
                drop(response);
                assert_eq!(releases.get(), 1);
            }
        }
    }

    #[test]
    fn head_caps_charge_original_fields_and_synthetic_marker_before_payload() {
        for (cap, marker, values, expected) in [
            (1, OutboundProxyMarker::On, 1_u8, true),
            (1, OutboundProxyMarker::Off, 1, false),
            (1, OutboundProxyMarker::Off, 2, true),
        ] {
            let mut captured = context(ResponseMode::Streamed);
            captured.max_response_header_count = Some(cap);
            captured.proxy_marker = marker;
            let mut headers = HeaderMap::new();
            for _ in 0..values {
                headers.append(
                    "x-filtered",
                    HeaderValue::from_bytes(&[0xff]).expect("opaque"),
                );
            }
            let releases = Rc::new(Cell::new(0));
            let result = OutboundHead::new(
                StatusCode::OK,
                input(headers),
                stream::poll_fn(|_| panic!("no body reads")).boxed_local(),
                captured,
                owner(&releases),
            );
            if expected {
                assert!(matches!(
                    result,
                    Err(EdgeError::ResponseTooLarge {
                        reason: ResponseLimitReason::HeaderCount,
                        ..
                    })
                ));
            } else {
                result.expect("within cap").discard();
            }
            assert_eq!(releases.get(), 1);
        }
    }

    #[test]
    fn streamed_source_failure_releases_on_terminal_poll_without_rebuilding_error() {
        let identity = Arc::new(());
        let source_identity = Arc::clone(&identity);
        let releases = Rc::new(Cell::new(0));
        let head = OutboundHead::new(
            StatusCode::OK,
            input(HeaderMap::new()),
            stream::iter([Err(EdgeError::internal(IdentityError(source_identity)))]).boxed_local(),
            context(ResponseMode::Streamed),
            owner(&releases),
        )
        .expect("head");
        let response = block_on(head.finish()).expect("stream is lazy");
        assert_eq!(releases.get(), 0);
        let mut body = response.into_body().into_stream().expect("streamed body");
        let error = block_on(body.next())
            .expect("terminal item")
            .expect_err("source");
        let EdgeError::Internal { source, .. } = error else {
            panic!("original error");
        };
        assert!(Arc::ptr_eq(
            &source.downcast_ref::<IdentityError>().expect("identity").0,
            &identity
        ));
        assert_eq!(
            releases.get(),
            1,
            "retaining the terminal stream retains no SDK owner"
        );
        assert!(block_on(body.next()).is_none());
        drop(body);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn final_head_preserves_independent_observed_payload_caps() {
        for (reason, encoded_cap, decoded_cap, final_cap) in [
            (ResponseLimitReason::EncodedBody, 8, 16, 16),
            (ResponseLimitReason::DecodedBody, 32, 8, 16),
            (ResponseLimitReason::BufferedBody, 32, 16, 8),
        ] {
            let mut captured = context(ResponseMode::Buffered {
                max_bytes: final_cap,
            });
            captured.body_policy.max_encoded_response_bytes = Some(encoded_cap);
            captured.body_policy.max_decoded_response_bytes = Some(decoded_cap);
            let releases = Rc::new(Cell::new(0));
            let head = OutboundHead::new(
                StatusCode::OK,
                input(HeaderMap::new()),
                stream::iter([Ok(Bytes::from_static(b"123456789"))]).boxed_local(),
                captured,
                owner(&releases),
            )
            .expect("head");
            assert!(
                matches!(block_on(head.finish()), Err(EdgeError::ResponseTooLarge { reason: actual, .. }) if actual == reason)
            );
            assert_eq!(releases.get(), 1);
        }
    }

    #[test]
    fn head_preserves_normalization_and_unknown_location_facts() {
        for (value, non_text) in [
            (HeaderValue::from_bytes(&[0xff]).expect("opaque"), true),
            (
                HeaderValue::from_bytes("caf\u{e9}".as_bytes()).expect("UTF-8"),
                false,
            ),
        ] {
            let mut headers = HeaderMap::new();
            headers.append("location", value);
            headers.append("content-type", HeaderValue::from_static("application/json"));
            headers.append("content-length", HeaderValue::from_static("malformed"));
            headers.append("content-encoding", HeaderValue::from_static("gzip"));
            headers.append(
                "connection",
                HeaderValue::from_static("content-type, content-length, content-encoding"),
            );
            let releases = Rc::new(Cell::new(0));
            let head = OutboundHead::new(
                StatusCode::FOUND,
                input(headers),
                stream::empty().boxed_local(),
                context(ResponseMode::Streamed),
                owner(&releases),
            )
            .expect("nominations precede framing");
            assert_eq!(head.location_evidence().observed_count(), 1);
            assert_eq!(head.location_evidence().has_non_text(), non_text);
            assert!(!head.location_evidence().is_missing_or_empty());
            assert_eq!(head.location_evidence().retained_singleton(), !non_text);
            assert!(!head.headers().contains_key("content-length"));
            assert!(!head.headers().contains_key("content-encoding"));
            let response = block_on(head.finish()).expect("final");
            assert_eq!(
                response.content_type_provenance(),
                ContentTypeProvenance::Ineligible(crate::ContentTypeIneligibility::Removed)
            );
            drop(response);
            assert_eq!(releases.get(), 1);
        }
    }

    #[test]
    fn response_context_readback_preserves_captured_policies_and_clock() {
        let time = FakeTime::new();
        let captured = time.response_context();
        assert_eq!(captured.clock().now(), time.clock().now());
        assert_eq!(captured.time().clock().now(), time.clock().now());
        assert_eq!(captured.entered_at(), time.clock().now());
        assert_eq!(captured.budget().cause, BudgetSource::PerCallTimeout);
        assert_eq!(captured.budget().duration, Duration::from_millis(20));
        assert_eq!(captured.mode(), OutboundSendMode::Send);
        assert_eq!(captured.request_method(), Method::GET);
        assert!(captured.original_chain_deadline().is_none());
        assert_eq!(
            captured.response_body_policy().max_decoded_response_bytes,
            Some(16)
        );
    }

    #[test]
    fn compressed_empty_reads_yield_before_the_decoder_can_starve_time() {
        for coding in ["br", "gzip", "deflate"] {
            let polls = Rc::new(Cell::new(0_usize));
            let counted = Rc::clone(&polls);
            let body = stream::poll_fn(move |_| {
                counted.set(counted.get().saturating_add(1));
                assert!(
                    counted.get() < 8,
                    "decoder must yield across ready empty input"
                );
                Poll::Ready(Some(Ok(Bytes::new())))
            })
            .boxed_local();
            let mut headers = HeaderMap::new();
            headers.insert(
                "content-encoding",
                HeaderValue::from_str(coding).expect("coding"),
            );
            let time = FakeTime::new();
            let releases = Rc::new(Cell::new(0));
            let head = OutboundHead::new(
                StatusCode::OK,
                input(headers),
                body,
                time.response_context(),
                owner(&releases),
            )
            .expect("head");
            let mut finishing = head.finish();
            let mut cx = Context::from_waker(Waker::noop());
            assert!(finishing.as_mut().poll(&mut cx).is_pending());
            assert_eq!(polls.get(), 1);
            time.advance(Duration::from_millis(20));
            assert!(matches!(
                finishing.as_mut().poll(&mut cx),
                Poll::Ready(Err(EdgeError::GatewayTimeout {
                    cause: BudgetSource::PerCallTimeout,
                    ..
                }))
            ));
            assert_eq!(releases.get(), 1);
        }
    }

    #[test]
    fn equal_chain_deadline_keeps_the_selected_budget_cause() {
        let time = FakeTime::new();
        let mut captured = time.response_context();
        captured.original_chain_deadline = Some(captured.budget.deadline);
        let releases = Rc::new(Cell::new(0));
        let head = OutboundHead::new(
            StatusCode::OK,
            input(HeaderMap::new()),
            stream::empty().boxed_local(),
            captured,
            owner(&releases),
        )
        .expect("head");
        time.advance(Duration::from_millis(20));
        assert!(matches!(
            block_on(head.finish()),
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::PerCallTimeout,
                ..
            })
        ));
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn head_finalization_retains_coding_choices_and_partial_response_rules() {
        let mut compressor = GzEncoder::new(Vec::new(), Compression::fast());
        compressor.write_all(b"body").expect("encode");
        let encoded = Bytes::from(compressor.finish().expect("gzip"));
        for decoding in [ResponseDecoding::Decode, ResponseDecoding::Original] {
            for status in [StatusCode::OK, StatusCode::PARTIAL_CONTENT] {
                for mode in [
                    ResponseMode::Buffered { max_bytes: 32 },
                    ResponseMode::Streamed,
                ] {
                    let mut captured = context(mode);
                    captured.body_policy.response_decoding = decoding;
                    let mut headers = HeaderMap::new();
                    headers.insert("content-encoding", HeaderValue::from_static("gzip"));
                    headers.insert(
                        "content-length",
                        HeaderValue::from_str(&encoded.len().to_string()).expect("length"),
                    );
                    let releases = Rc::new(Cell::new(0));
                    let head = OutboundHead::new(
                        status,
                        input(headers),
                        stream::iter([Ok(encoded.clone())]).boxed_local(),
                        captured,
                        owner(&releases),
                    )
                    .expect("head");
                    let response = block_on(head.finish()).expect("finalized");
                    let original = decoding == ResponseDecoding::Original
                        || status == StatusCode::PARTIAL_CONTENT;
                    assert_eq!(
                        response.headers().contains_key("content-encoding"),
                        original
                    );
                    assert_eq!(response.headers().contains_key("content-length"), original);
                    let body = block_on(response.into_bytes_bounded(32)).expect("body");
                    assert_eq!(
                        body.as_ref(),
                        if original { encoded.as_ref() } else { b"body" }
                    );
                    assert_eq!(releases.get(), 1);
                }
            }
        }
    }

    #[test]
    fn head_header_errors_release_before_any_payload_poll() {
        for malformed_connection in [false, true] {
            let mut captured = context(ResponseMode::Streamed);
            let mut headers = HeaderMap::new();
            if malformed_connection {
                headers.insert("connection", HeaderValue::from_static("bad name"));
            } else {
                headers.insert("x", HeaderValue::from_static("y"));
                captured.max_response_header_bytes = Some(1);
            }
            let releases = Rc::new(Cell::new(0));
            let result = OutboundHead::new(
                StatusCode::FOUND,
                input(headers),
                stream::poll_fn(|_| panic!("header failures never read payloads")).boxed_local(),
                captured,
                owner(&releases),
            );
            if malformed_connection {
                assert!(matches!(
                    result,
                    Err(EdgeError::BadGateway {
                        reason: crate::BadGatewayReason::Protocol,
                        ..
                    })
                ));
            } else {
                assert!(matches!(
                    result,
                    Err(EdgeError::ResponseTooLarge {
                        reason: ResponseLimitReason::HeaderBytes,
                        ..
                    })
                ));
            }
            assert_eq!(releases.get(), 1);
        }
    }
}

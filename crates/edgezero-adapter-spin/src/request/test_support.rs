//! Narrow request-conversion and timer bridges.
use super::*;
use bytes::Bytes;
use edgezero_core::http::Uri;
use edgezero_core::http::request_builder;
use std::future::pending;

#[doc(hidden)]
#[inline]
pub fn deadline_body<Source, SourceError, Timer, MakeTimer>(
    source: Source,
    deadline: Deadline,
    clock: MonotonicClock,
    make_timer: MakeTimer,
) -> Body
where
    Source: futures_util::Stream<Item = Result<Bytes, SourceError>> + 'static,
    SourceError: Into<anyhow::Error> + 'static,
    Timer: Future<Output = ()> + 'static,
    MakeTimer: Fn(Duration) -> Timer + Clone + 'static,
{
    super::deadline_body_with_timer(source, deadline, clock, make_timer)
}

/// Dispatches an observable source through the production ingress body wrapper without a
/// provider timer. Tests that exercise timer expiry use
/// [`dispatch_ingress_stream_with_timer`] instead.
#[doc(hidden)]
#[inline]
pub async fn dispatch_ingress_stream<Source, SourceError>(
    app: &App,
    method: Method,
    uri: Uri,
    source: Source,
) -> anyhow::Result<ResponseEgressEnvelope>
where
    Source: futures_util::Stream<Item = Result<Bytes, SourceError>> + 'static,
    SourceError: Into<anyhow::Error> + 'static,
{
    let request_start = app.monotonic_now();
    let core_request = request_builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .map_err(EdgeError::internal)?;
    dispatch_ingress_stream_with_timer_and(
        app,
        core_request,
        Stores::default(),
        request_start,
        move || Ok(source),
        |_| pending::<()>(),
        Ok,
    )
    .await
}

/// Exercises admission ordering with observable source construction and delivery closures.
#[doc(hidden)]
#[inline]
pub async fn dispatch_ingress_with<Source, SourceError, MakeSource, Output, Deliver>(
    app: &App,
    method: Method,
    uri: Uri,
    make_source: MakeSource,
    deliver: Deliver,
) -> anyhow::Result<Output>
where
    Source: futures_util::Stream<Item = Result<bytes::Bytes, SourceError>> + 'static,
    SourceError: Into<anyhow::Error> + 'static,
    MakeSource: FnOnce() -> Result<Source, EdgeError>,
    Deliver: FnOnce(ResponseEgressEnvelope) -> Result<Output, EdgeError>,
{
    let request_start = app.monotonic_now();
    let core_request = request_builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .map_err(EdgeError::internal)?;
    dispatch_ingress_stream_with_timer_and(
        app,
        core_request,
        Stores::default(),
        request_start,
        make_source,
        |_| pending::<()>(),
        deliver,
    )
    .await
}

/// Dispatches an observable source through the production ingress deadline race.
#[doc(hidden)]
#[inline]
pub async fn dispatch_ingress_stream_with_timer<Source, SourceError, Timer, MakeTimer>(
    app: &App,
    method: Method,
    uri: Uri,
    source: Source,
    make_timer: MakeTimer,
) -> anyhow::Result<ResponseEgressEnvelope>
where
    Source: futures_util::Stream<Item = Result<Bytes, SourceError>> + 'static,
    SourceError: Into<anyhow::Error> + 'static,
    Timer: Future<Output = ()> + 'static,
    MakeTimer: Fn(Duration) -> Timer + Clone + 'static,
{
    let request_start = app.monotonic_now();
    let core_request = request_builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .map_err(EdgeError::internal)?;
    dispatch_ingress_stream_with_timer_and(
        app,
        core_request,
        Stores::default(),
        request_start,
        move || Ok(source),
        make_timer,
        Ok,
    )
    .await
}

/// Dispatches a native Spin request through the production outer request seam.
///
/// # Errors
/// Returns the same request-conversion, routing, or response-conversion error as standard dispatch.
#[doc(hidden)]
#[inline]
pub async fn dispatch_request(
    app: &App,
    req: SpinRequest,
) -> anyhow::Result<ResponseEgressEnvelope> {
    dispatch_with_handles_and(app, req, Stores::default(), app.monotonic_now(), Ok).await
}

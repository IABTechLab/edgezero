//! Narrow request-conversion and timer bridges.
use super::*;
use bytes::Bytes;
use edgezero_core::config_store::BoundedStoreRead;
use edgezero_core::http::Uri;
use edgezero_core::http::request_builder;
use edgezero_core::secret_store::{SecretError, SecretStore, finish_bounded_secret_read};
use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) struct GeoSecret(Option<Bytes>, Option<Arc<AtomicUsize>>);

#[cfg(test)]
impl GeoSecret {
    pub(super) fn missing() -> Self {
        Self(None, None)
    }
}

#[async_trait::async_trait(?Send)]
impl SecretStore for GeoSecret {
    async fn get_bytes(&self, _store: &str, _key: &str) -> Result<Option<Bytes>, SecretError> {
        if let Some(reads) = &self.1 {
            reads.fetch_add(1, Ordering::SeqCst);
        }
        Ok(self.0.clone())
    }

    async fn get_bytes_bounded(
        &self,
        store_name: &str,
        key: &str,
        clock: &MonotonicClock,
        deadline: Deadline,
        max_backend_bytes: u64,
        max_value_bytes: u64,
    ) -> Result<BoundedStoreRead<Bytes>, SecretError> {
        if self.1.is_some() {
            assert_eq!(
                max_backend_bytes, 128,
                "geo lookup must bound backend credential bytes"
            );
            assert_eq!(
                max_value_bytes, 128,
                "geo lookup must bound returned credential bytes"
            );
            assert!(
                !deadline.is_expired_at(clock.now()),
                "admitted lookup must receive a live request deadline"
            );
        }
        finish_bounded_secret_read(
            self.get_bytes(store_name, key).await,
            clock,
            deadline,
            max_backend_bytes,
            max_value_bytes,
        )
    }
}

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
        &GeoSecret(None, None),
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
        &GeoSecret(None, None),
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
        &GeoSecret(None, None),
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
    dispatch_request_with_geo_secret(app, req, None, None).await
}

/// Uses the production converter and managed ingress with an in-memory credential backend.
/// The actual Spin variable provider is qualified by the Spin runtime fixture.
///
/// # Errors
/// Returns the managed request's admission, dispatch, or delivery error.
#[doc(hidden)]
#[inline]
pub async fn dispatch_request_with_geo_secret(
    app: &App,
    req: SpinRequest,
    secret: Option<Bytes>,
    reads: Option<Arc<AtomicUsize>>,
) -> anyhow::Result<ResponseEgressEnvelope> {
    dispatch_request_with_geo_stores(app, req, secret, reads, None).await
}

/// Managed ingress with explicit test-only credential and KV registry injection.
///
/// # Errors
/// Returns the managed admission, dispatch or delivery error.
#[doc(hidden)]
#[inline]
pub async fn dispatch_request_with_geo_stores(
    app: &App,
    req: SpinRequest,
    secret: Option<Bytes>,
    reads: Option<Arc<AtomicUsize>>,
    kv_registry: Option<KvRegistry>,
) -> anyhow::Result<ResponseEgressEnvelope> {
    let request_start = app.monotonic_now();
    let (parts, native_body) = req.into_parts();
    let core_request = into_core_request_head_for_app(parts, app);
    dispatch_ingress_stream_with_timer_and(
        app,
        core_request,
        Stores {
            geo: app.state::<SpinKvGeo>().cloned(),
            kv_registry,
            ..Stores::default()
        },
        request_start,
        move || Ok(native_body.stream()),
        sleep,
        Ok,
        &GeoSecret(secret, reads),
    )
    .await
}

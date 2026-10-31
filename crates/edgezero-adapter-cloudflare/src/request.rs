use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
#[cfg(feature = "test-utils")]
use std::{
    io,
    sync::atomic::{AtomicUsize, Ordering},
    task::Poll,
};

#[cfg(feature = "test-utils")]
use bytes::Bytes;
use edgezero_core::OutboundScopeOwner;
use edgezero_core::app::{App, StoreMetadata};
use edgezero_core::body::Body;
use edgezero_core::config_store::{ConfigStoreHandle, ConfigStoreOpenFailure};
use edgezero_core::context::RuntimeVariables;
use edgezero_core::env_config::EnvConfig;
use edgezero_core::error::EdgeError;
use edgezero_core::http::{HeaderValue, Method as CoreMethod, Request, Uri, request_builder};
use edgezero_core::ingress::{
    CapturedTarget, HeaderFidelity, InboundOrigin, IngressBeginOutcome, IngressDispatchOutcome,
    IngressFraming, IngressHeadAccounting, IngressHeadParts, OriginSource, PreparedIngress,
    Preservation, RequestIngress, TargetSource,
};
use edgezero_core::key_value_store::KvHandle;
use edgezero_core::outbound::HttpClient;
use edgezero_core::response_egress::ResponseEgressEnvelope;
use edgezero_core::secret_store::SecretHandle;
use edgezero_core::store_registry::{
    BoundSecretStore, ConfigRegistry, ConfigStoreBinding, KvRegistry, SecretRegistry, StoreRegistry,
};
use edgezero_core::time::{Deadline, MonotonicClock, MonotonicInstant};
#[cfg(feature = "test-utils")]
use futures::executor::block_on;
use futures_util::future::{Either, select};
#[cfg(feature = "test-utils")]
use futures_util::stream::Empty;
#[cfg(feature = "test-utils")]
use futures_util::stream::poll_fn;
use futures_util::stream::{LocalBoxStream, unfold};
use futures_util::{StreamExt as _, TryStreamExt as _};
use worker::{
    Context, Delay, Env, Error as WorkerError, Method, Request as CfRequest, Response as CfResponse,
};

use crate::config_store::CloudflareConfigStore;
use crate::context::CloudflareRequestContext;
use crate::key_value_store::CloudflareKvStore;
use crate::outbound::{CloudflareOutboundClient, native_time};
use crate::response::{CloudflareEgressRuntime, from_egress_response};
use crate::secret_store::CloudflareSecretStore;

/// Groups the optional per-request store handles injected at dispatch time.
///
/// Use `..Default::default()` for fields you do not need:
///
/// ```rust,ignore
/// let stores = Stores { kv: Some(kv_handle), ..Default::default() };
/// ```
#[derive(Default)]
pub(crate) struct Stores {
    config_registry: Option<ConfigRegistry>,
    config_store: Option<ConfigStoreHandle>,
    kv: Option<KvHandle>,
    kv_registry: Option<KvRegistry>,
    runtime_variables: Option<RuntimeVariables>,
    secret_registry: Option<SecretRegistry>,
    secrets: Option<SecretHandle>,
}

/// Cloudflare per-request dispatch service.
///
/// Builds a Worker invocation with the stores the operator wants
/// injected into request extensions, then dispatches one request
/// against the wrapped `App`. The store wiring is a per-Service
/// decision; on Cloudflare Workers that means per-request (the
/// runtime invokes the entrypoint per HTTP request), but the
/// Service type itself is cheap to build.
///
/// Replaces the prior `dispatch_with_*` variant fan-out. Each
/// builder method is independent: enable any combination of KV,
/// config, and secret stores by chaining the relevant `with_*` /
/// `require_*` calls. The manifest-driven `run_app` is still the
/// recommended entrypoint for normal flows -- the Service builder
/// is for manual / no-manifest deployments.
///
/// ```rust,ignore
/// CloudflareService::new(&app)
///     .with_kv("sessions").require_kv()
///     .with_config("app_config")
///     .with_secrets()
///     .dispatch(req, env, ctx).await
/// ```
pub struct CloudflareService<'app> {
    app: &'app App,
    config: ConfigSource,
    kv: Option<KvSource>,
    secrets: SecretSource,
}

enum ConfigSource {
    Binding(String),
    Handle(ConfigStoreHandle),
    None,
}

struct KvSource {
    binding: String,
    required: bool,
}

enum SecretSource {
    Off,
    On { required: bool },
}

impl<'app> CloudflareService<'app> {
    /// Resolve every wired store at request time and dispatch
    /// against the wrapped `App`. `env` and `ctx` come from the
    /// Worker runtime per request, NOT the Service builder.
    /// Consumes the service so a builder can't be reused with stale
    /// wiring.
    ///
    /// # Errors
    /// Returns [`worker::Error`] if a required store binding cannot be
    /// opened, the core request cannot be built, or the inner router
    /// dispatch fails.
    #[inline]
    pub async fn dispatch(
        self,
        req: CfRequest,
        env: Env,
        ctx: Context,
    ) -> Result<CfResponse, WorkerError> {
        with_callback_scope(self.dispatch_scoped(req, env, ctx)).await
    }

    async fn dispatch_scoped(
        self,
        req: CfRequest,
        env: Env,
        ctx: Context,
    ) -> Result<CfResponse, WorkerError> {
        let request_start = self.app.monotonic_now();
        let config_store = match self.config {
            ConfigSource::Binding(binding) => Some(open_config_handle(&env, &binding)),
            ConfigSource::Handle(handle) => Some(handle),
            ConfigSource::None => None,
        };
        let kv = match self.kv {
            Some(source) => resolve_kv_handle(&env, &source.binding, source.required)?,
            None => None,
        };
        let secrets = match self.secrets {
            SecretSource::Off => None,
            // Always Some — post-PR-269 fix; `required=false`
            // (set by `.with_secrets()` without
            // `.require_secrets()`) no longer suppresses the
            // handle. See `resolve_secret_handle`.
            SecretSource::On { required } => Some(resolve_secret_handle(&env, required)),
        };
        dispatch_with_handles(
            self.app,
            req,
            env,
            ctx,
            Stores {
                config_store,
                kv,
                secrets,
                ..Default::default()
            },
            request_start,
        )
        .await
    }

    /// Build a new service that dispatches against `app` with NO
    /// stores wired. Chain `.with_*` / `.require_*` to add stores.
    #[must_use]
    #[inline]
    pub fn new(app: &'app App) -> Self {
        Self {
            app,
            config: ConfigSource::None,
            kv: None,
            secrets: SecretSource::Off,
        }
    }

    /// Promote the previously-wired KV binding to required: an
    /// unavailable namespace causes dispatch to return an error.
    /// No-op when `with_kv` wasn't called.
    #[must_use]
    #[inline]
    pub fn require_kv(mut self) -> Self {
        if let Some(kv) = self.kv.as_mut() {
            kv.required = true;
        }
        self
    }

    /// Promote the previously-wired secret store to required.
    /// No-op when `with_secrets` wasn't called.
    #[must_use]
    #[inline]
    pub fn require_secrets(mut self) -> Self {
        if let SecretSource::On { ref mut required } = self.secrets {
            *required = true;
        }
        self
    }

    /// Open the KV namespace bound as `binding` (per `wrangler.toml`)
    /// as a Cloudflare config store and inject its handle. If opening
    /// fails, the dispatcher logs once and retains a failing handle
    /// whose reads return
    /// [`ConfigStoreError::Unavailable`](edgezero_core::config_store::ConfigStoreError::Unavailable).
    /// Only an unconfigured service has no config-store handle.
    #[must_use]
    #[inline]
    pub fn with_config<S>(mut self, binding: S) -> Self
    where
        S: Into<String>,
    {
        self.config = ConfigSource::Binding(binding.into());
        self
    }

    /// Inject a pre-built `ConfigStoreHandle`. Use this when the
    /// caller has already opened (or mocked) the backend. Mutually
    /// exclusive with `with_config(binding)` -- the last call wins.
    #[must_use]
    #[inline]
    pub fn with_config_handle(mut self, handle: ConfigStoreHandle) -> Self {
        self.config = ConfigSource::Handle(handle);
        self
    }

    /// Open the KV namespace bound as `binding` and inject its
    /// handle. Non-required by default: an absent binding logs
    /// once and dispatch continues. Pair with `require_kv()` when
    /// the manifest declares `[stores.kv]`.
    #[must_use]
    #[inline]
    pub fn with_kv<S>(mut self, binding: S) -> Self
    where
        S: Into<String>,
    {
        self.kv = Some(KvSource {
            binding: binding.into(),
            required: false,
        });
        self
    }

    /// Enable Cloudflare Worker secrets and inject the secret-store
    /// handle. Worker secrets have no namespace concept, so no
    /// name is needed. Non-required by default; pair with
    /// `require_secrets()` when the manifest declares
    /// `[stores.secrets]`. Individual missing secrets surface as
    /// `SecretError::NotFound` at access time.
    #[must_use]
    #[inline]
    pub fn with_secrets(mut self) -> Self {
        self.secrets = SecretSource::On { required: false };
        self
    }
}

/// Groups the multi-id store metadata + env config inputs threaded into
/// the registry-based dispatcher. Carved out so `dispatch_with_registries`
/// stays under the `too_many_arguments` ceiling.
pub(crate) struct RegistryInputs<'env> {
    pub config_meta: Option<StoreMetadata>,
    pub env_config: &'env EnvConfig,
    pub kv_meta: Option<StoreMetadata>,
    pub runtime_variables: RuntimeVariables,
    pub secret_meta: Option<StoreMetadata>,
}

#[cfg(feature = "test-utils")]
struct DropSignal(Arc<AtomicUsize>);

#[cfg(feature = "test-utils")]
#[expect(
    clippy::missing_trait_methods,
    reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
)]
impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn detached_error_envelope(
    app: &App,
    error: EdgeError,
    request_method: CoreMethod,
    request_start: MonotonicInstant,
) -> Result<ResponseEgressEnvelope, WorkerError> {
    match app.detached_ingress_error_egress(error, request_method, request_start) {
        IngressDispatchOutcome::Response(envelope) => Ok(*envelope),
        IngressDispatchOutcome::Aborted | _ => Err(WorkerError::RustError(
            "ingress admission aborted".to_owned(),
        )),
    }
}

/// Convert a Cloudflare Worker request into an `EdgeZero` core request.
///
/// # Errors
/// Returns [`EdgeError::bad_request`] for invalid runtime methods, header values,
/// URLs or URIs,
/// and [`EdgeError::internal`] if the body cannot be read or the core
/// request cannot be built.
#[inline]
#[expect(
    clippy::unused_async,
    reason = "the public converter retains its established async API while request bodies remain lazy"
)]
pub async fn into_core_request(
    req: CfRequest,
    env: Env,
    ctx: Context,
) -> Result<Request, EdgeError> {
    let mut request = into_core_request_head(&req, env, Arc::new(ctx), MonotonicClock::default())?;
    *request.body_mut() =
        cloudflare_body_stream(req)?.map_or_else(Body::empty, Body::from_external_stream);
    Ok(request)
}

fn into_core_request_head(
    req: &CfRequest,
    env: Env,
    ctx: Arc<Context>,
    outbound_clock: MonotonicClock,
) -> Result<Request, EdgeError> {
    let method = CoreMethod::from_bytes(req.inner().method().as_bytes())
        .map_err(|_error| EdgeError::bad_request("invalid runtime method"))?;
    let runtime_url = req.inner().url();
    let target = CapturedTarget::capture(
        &runtime_url,
        TargetSource::RuntimeUrl,
        Preservation::Unknown,
    );
    let url: worker::Url = runtime_url
        .parse()
        .map_err(|_error| EdgeError::bad_request("invalid runtime URL"))?;
    let uri: Uri = url
        .as_str()
        .parse()
        .map_err(|_error| EdgeError::bad_request("invalid runtime URI"))?;
    let origin = uri
        .scheme_str()
        .zip(uri.authority())
        .and_then(|(scheme, authority)| {
            InboundOrigin::parse(scheme, authority.as_str(), OriginSource::RuntimeUri).ok()
        });
    let fidelity = HeaderFidelity::RUNTIME_HEADER_MAP;
    let ingress = RequestIngress::new(target, origin, fidelity);

    let mut builder = request_builder().method(method).uri(uri);
    let headers = req.headers();
    for (name, value) in headers.entries() {
        let header_value = HeaderValue::from_bytes(value.as_bytes())
            .map_err(|_error| EdgeError::bad_request("invalid runtime header value"))?;
        builder = builder.header(name.as_str(), header_value);
    }

    let mut request = builder.body(Body::empty()).map_err(EdgeError::internal)?;

    request.extensions_mut().insert(ingress);
    CloudflareRequestContext::insert(&mut request, env, ctx);
    request
        .extensions_mut()
        .insert(outbound_client(outbound_clock));
    Ok(request)
}

fn outbound_client(clock: MonotonicClock) -> HttpClient {
    HttpClient::with_client(CloudflareOutboundClient::with_time(native_time(clock)))
}

fn cloudflare_body_stream(
    mut req: CfRequest,
) -> Result<Option<LocalBoxStream<'static, Result<bytes::Bytes, WorkerError>>>, EdgeError> {
    if req.inner().body().is_none() {
        return Ok(None);
    }
    Ok(Some(
        req.stream()
            .map_err(EdgeError::internal)?
            .map_ok(bytes::Bytes::from)
            .boxed_local(),
    ))
}

fn cloudflare_deadline_body<Source, SourceError>(
    source: Source,
    deadline: Deadline,
    monotonic_clock: MonotonicClock,
) -> Body
where
    Source: futures_util::Stream<Item = Result<bytes::Bytes, SourceError>> + 'static,
    SourceError: Into<anyhow::Error> + 'static,
{
    let boxed_stream = source.map_err(Into::into).boxed_local();
    let stream = unfold(Some(boxed_stream), move |stream_state| {
        let clock = monotonic_clock.clone();
        async move {
            let mut body_stream = stream_state?;
            if deadline.is_expired_at(clock.now()) {
                return Some((
                    Err(EdgeError::request_timeout(
                        "inbound body read deadline exceeded",
                    )),
                    None,
                ));
            }

            Delay::from(Duration::ZERO).await;

            let item = {
                let Some(remaining) = deadline.remaining_at(clock.now()) else {
                    return Some((
                        Err(EdgeError::request_timeout(
                            "inbound body read deadline exceeded",
                        )),
                        None,
                    ));
                };
                let timer = Delay::from(remaining);
                let next = body_stream.next();
                match select(timer, next).await {
                    Either::Left(((), _)) => {
                        return Some((
                            Err(EdgeError::request_timeout(
                                "inbound body read deadline exceeded",
                            )),
                            None,
                        ));
                    }
                    Either::Right((item, _)) => item,
                }
            };
            if deadline.is_expired_at(clock.now()) {
                return Some((
                    Err(EdgeError::request_timeout(
                        "inbound body read deadline exceeded",
                    )),
                    None,
                ));
            }
            match item {
                Some(Ok(bytes)) => Some((Ok(bytes), Some(body_stream))),
                Some(Err(error)) => Some((Err(EdgeError::internal(error)), None)),
                None => None,
            }
        }
    });
    Body::from_stream(stream)
}

/// Runs the terminal source-release probe used by the WASM contract suite.
#[cfg(feature = "test-utils")]
#[must_use]
#[inline]
pub fn deadline_body_releases_source_for_test() -> bool {
    let dropped = Arc::new(AtomicUsize::new(0));
    let signal = DropSignal(Arc::clone(&dropped));
    let source = poll_fn(move |_cx| {
        let _keep_alive = &signal;
        Poll::<Option<Result<Bytes, io::Error>>>::Pending
    });
    let start = MonotonicInstant::now();
    let clock = MonotonicClock::new(move || start);
    let body = cloudflare_deadline_body(source, Deadline::at_instant(start), clock);
    let Ok(mut body_stream) = body.into_stream() else {
        return false;
    };
    let Some(Err(error)) = block_on(body_stream.next()) else {
        return false;
    };
    matches!(error, EdgeError::RequestTimeout { .. }) && dropped.load(Ordering::SeqCst) == 1
}

/// Dispatches an observable source through the production ingress body wrapper.
#[cfg(feature = "test-utils")]
#[doc(hidden)]
#[inline]
pub async fn dispatch_ingress_stream_for_test<Source, SourceError>(
    app: &App,
    method: CoreMethod,
    uri: Uri,
    source: Source,
) -> Result<ResponseEgressEnvelope, WorkerError>
where
    Source: futures_util::Stream<Item = Result<Bytes, SourceError>> + 'static,
    SourceError: Into<anyhow::Error> + 'static,
{
    let request_start = app.monotonic_now();
    let mut core_request = request_builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .map_err(|error| edge_error_to_worker(&EdgeError::internal(error)))?;
    core_request
        .extensions_mut()
        .insert(outbound_client(app.monotonic_clock()));
    with_callback_scope(dispatch_ingress_stream_and(
        app,
        core_request,
        Stores::default(),
        request_start,
        move || Ok(Some(source)),
        Ok,
    ))
    .await
}

/// Exercises admission ordering with observable source construction and delivery closures.
#[cfg(feature = "test-utils")]
#[doc(hidden)]
#[inline]
pub async fn dispatch_ingress_with_for_test<Source, SourceError, MakeSource, Output, Deliver>(
    app: &App,
    method: CoreMethod,
    uri: Uri,
    make_source: MakeSource,
    deliver: Deliver,
) -> Result<Output, WorkerError>
where
    Source: futures_util::Stream<Item = Result<Bytes, SourceError>> + 'static,
    SourceError: Into<anyhow::Error> + 'static,
    MakeSource: FnOnce() -> Result<Source, EdgeError>,
    Deliver: FnOnce(ResponseEgressEnvelope) -> Result<Output, EdgeError>,
{
    let request_start = app.monotonic_now();
    let core_request = request_builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .map_err(|error| edge_error_to_worker(&EdgeError::internal(error)))?;
    with_callback_scope(dispatch_ingress_stream_and(
        app,
        core_request,
        Stores::default(),
        request_start,
        move || make_source().map(Some),
        deliver,
    ))
    .await
}

/// Exercises a source-construction failure after admission through the production seam.
#[cfg(feature = "test-utils")]
#[doc(hidden)]
#[inline]
pub async fn dispatch_ingress_source_error_for_test(
    app: &App,
    method: CoreMethod,
    uri: Uri,
) -> Result<ResponseEgressEnvelope, WorkerError> {
    let request_start = app.monotonic_now();
    let core_request = request_builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .map_err(|error| edge_error_to_worker(&EdgeError::internal(error)))?;
    with_callback_scope(dispatch_ingress_stream_and(
        app,
        core_request,
        Stores::default(),
        request_start,
        || -> Result<Option<Empty<Result<Bytes, io::Error>>>, EdgeError> {
            Err(EdgeError::internal(anyhow::anyhow!(
                "cloudflare source construction failed"
            )))
        },
        Ok,
    ))
    .await
}

pub(crate) async fn with_callback_scope<F>(future: F) -> F::Output
where
    F: Future,
{
    match OutboundScopeOwner::callback_if_absent() {
        Some(callback) => callback.run(future).await,
        None => future.await,
    }
}

pub(crate) async fn dispatch_with_handles(
    app: &App,
    req: CfRequest,
    env: Env,
    ctx: Context,
    stores: Stores,
    request_start: MonotonicInstant,
) -> Result<CfResponse, WorkerError> {
    let signal = req.inner().signal();
    let request_method =
        CoreMethod::from_bytes(req.inner().method().as_bytes()).unwrap_or(CoreMethod::GET);
    let context = Arc::new(ctx);
    let runtime = CloudflareEgressRuntime::new(signal, Arc::clone(&context));
    let head_request = match into_core_request_head(&req, env, context, app.monotonic_clock()) {
        Ok(request) => request,
        Err(error) => {
            drop(req);
            let egress = detached_error_envelope(app, error, request_method, request_start)?;
            return from_egress_response(egress, runtime)
                .map_err(|delivery_error| edge_error_to_worker(&delivery_error));
        }
    };
    dispatch_ingress_stream_and(
        app,
        head_request,
        stores,
        request_start,
        move || cloudflare_body_stream(req),
        move |egress| from_egress_response(egress, runtime),
    )
    .await
}

async fn dispatch_ingress_stream_and<Source, SourceError, MakeSource, Output, Deliver>(
    app: &App,
    mut head_request: Request,
    stores: Stores,
    request_start: MonotonicInstant,
    make_source: MakeSource,
    deliver: Deliver,
) -> Result<Output, WorkerError>
where
    Source: futures_util::Stream<Item = Result<bytes::Bytes, SourceError>> + 'static,
    SourceError: Into<anyhow::Error> + 'static,
    MakeSource: FnOnce() -> Result<Option<Source>, EdgeError>,
    Deliver: FnOnce(ResponseEgressEnvelope) -> Result<Output, EdgeError>,
{
    let request_method = head_request.method().clone();
    let head_parts = IngressHeadParts::from_request(
        &head_request,
        IngressHeadAccounting::HostManaged,
        IngressFraming::HostManaged,
    );
    if let Err(validation_error) = head_parts.validate_normalized(app.ingress_head_limits()) {
        let egress = detached_error_envelope(app, validation_error, request_method, request_start)?;
        return deliver(egress).map_err(|delivery_error| edge_error_to_worker(&delivery_error));
    }
    let prepared = match app.begin_ingress(head_parts, request_start) {
        Ok(outcome) => match outcome {
            IngressBeginOutcome::Admitted(prepared) => prepared,
            IngressBeginOutcome::Refused(response) => {
                return deliver(response).map_err(|error| edge_error_to_worker(&error));
            }
            IngressBeginOutcome::Aborted => {
                return Err(WorkerError::RustError(
                    "ingress admission aborted".to_owned(),
                ));
            }
            _ => {
                let admission_error =
                    EdgeError::internal(anyhow::anyhow!("unsupported ingress admission outcome"));
                let egress =
                    detached_error_envelope(app, admission_error, request_method, request_start)?;
                return deliver(egress)
                    .map_err(|delivery_error| edge_error_to_worker(&delivery_error));
            }
        },
        Err(admission_error) => {
            let egress =
                detached_error_envelope(app, admission_error, request_method, request_start)?;
            return deliver(egress).map_err(|delivery_error| edge_error_to_worker(&delivery_error));
        }
    };
    let source = match make_source() {
        Ok(source) => source,
        Err(source_error) => {
            return deliver(app.admitted_error_egress(prepared, source_error))
                .map_err(|delivery_error| edge_error_to_worker(&delivery_error));
        }
    };
    *head_request.body_mut() = source.map_or_else(Body::empty, |stream| {
        cloudflare_deadline_body(stream, prepared.read_deadline(), prepared.monotonic_clock())
    });
    dispatch_core_request(app, head_request, stores, prepared, deliver).await
}

/// Dispatch with per-id store registries built from baked metadata.
///
/// Cloudflare capability map:
/// - KV (Multi): each declared id binds its KV namespace lazily via
///   `EDGEZERO__STORES__KV__<ID>__NAME` (default = id). Opening occurs on
///   first use, with a typed failure retained for that request only.
/// - Config (Multi): each declared id opens its own KV namespace via
///   `EDGEZERO__STORES__CONFIG__<ID>__NAME`, read asynchronously.
/// - Secrets (Single): one shared [`CloudflareSecretStore`] is registered
///   under every declared id.
pub(crate) async fn dispatch_with_registries(
    app: &App,
    req: CfRequest,
    env: Env,
    ctx: Context,
    inputs: RegistryInputs<'_>,
) -> Result<CfResponse, WorkerError> {
    let request_start = app.monotonic_now();
    let kv_registry = build_kv_registry(&env, inputs.kv_meta, inputs.env_config);
    let config_registry = build_config_registry(&env, inputs.config_meta, inputs.env_config);
    let secret_registry = build_secret_registry(&env, inputs.secret_meta, inputs.env_config);
    dispatch_with_handles(
        app,
        req,
        env,
        ctx,
        Stores {
            config_registry,
            kv_registry,
            secret_registry,
            runtime_variables: Some(inputs.runtime_variables),
            ..Default::default()
        },
        request_start,
    )
    .await
}

pub(crate) fn resolve_kv_handle(
    env: &Env,
    kv_binding: &str,
    kv_required: bool,
) -> Result<Option<KvHandle>, WorkerError> {
    match CloudflareKvStore::from_env(env, kv_binding) {
        Ok(store) => Ok(Some(KvHandle::new(Arc::new(store)))),
        Err(err) => {
            if kv_required {
                return Err(WorkerError::RustError(err.to_string()));
            }
            warn_missing_kv_binding_once(kv_binding, &err);
            Ok(None)
        }
    }
}

/// Construct the Cloudflare secret-store handle. Called from the
/// `SecretSource::On { .. }` arm of `dispatch`, so it always builds
/// the handle — the `_required` parameter is preserved (and ignored)
/// for symmetry with the kv path's `resolve_kv_handle(_, _, required)`
/// signature, where `required` decides whether a runtime open
/// failure is fatal or silently degrades. `CloudflareSecretStore` is
/// a thin `env`-wrapper whose construction can't fail, so there's
/// nothing for `required` to gate at handle-construction time —
/// `_required` is reserved for whichever future per-secret-lookup
/// availability policy we add.
///
/// Pre-fix, this short-circuited to `None` when `!_required`, which
/// silently swallowed `.with_secrets()` (which sets `required:
/// false`): handlers ran without a `SecretRegistry` even though the
/// builder claimed to inject one.
pub(crate) fn resolve_secret_handle(env: &Env, _required: bool) -> SecretHandle {
    let secret_store = CloudflareSecretStore::from_env(env.clone());
    SecretHandle::new(Arc::new(secret_store))
}

fn build_config_registry(
    env: &Env,
    config_meta: Option<StoreMetadata>,
    env_config: &EnvConfig,
) -> Option<ConfigRegistry> {
    let meta = config_meta?;
    let mut by_id: BTreeMap<String, ConfigStoreBinding> = BTreeMap::new();
    for id in meta.ids {
        let binding_name = env_config.store_name("config", id);
        by_id.insert(
            (*id).to_owned(),
            ConfigStoreBinding {
                handle: open_config_handle(env, &binding_name),
                default_key: env_config.store_key("config", id),
            },
        );
    }
    StoreRegistry::from_parts(by_id, meta.default.to_owned())
}

fn build_kv_registry(
    env: &Env,
    kv_meta: Option<StoreMetadata>,
    env_config: &EnvConfig,
) -> Option<KvRegistry> {
    let meta = kv_meta?;
    let mut by_id: BTreeMap<String, KvHandle> = BTreeMap::new();
    for id in meta.ids {
        let binding = env_config.store_name("kv", id);
        by_id.insert(
            (*id).to_owned(),
            KvHandle::new(Arc::new(CloudflareKvStore::declared(
                env.clone(),
                id,
                binding,
            ))),
        );
    }
    StoreRegistry::from_parts(by_id, meta.default.to_owned())
}

fn build_secret_registry(
    env: &Env,
    secret_meta: Option<StoreMetadata>,
    env_config: &EnvConfig,
) -> Option<SecretRegistry> {
    let meta = secret_meta?;
    // Cloudflare is `Single` for secrets — one shared handle binds every id.
    // `CloudflareSecretStore::get_bytes` ignores `store_name` (worker
    // secrets are a flat namespace), so the per-id bound name is
    // observable only via [`BoundSecretStore::store_name`].
    let handle = SecretHandle::new(Arc::new(CloudflareSecretStore::from_env(env.clone())));
    let mut by_id: BTreeMap<String, BoundSecretStore> = BTreeMap::new();
    for id in meta.ids {
        let store_name = env_config.store_name("secrets", id);
        by_id.insert(
            (*id).to_owned(),
            BoundSecretStore::new(handle.clone(), store_name),
        );
    }
    // Cloudflare secret handles are infallible to construct; `from_parts`
    // keeps the API symmetric with the KV / config builders.
    StoreRegistry::from_parts(by_id, meta.default.to_owned())
}

async fn dispatch_core_request<Output, Deliver>(
    app: &App,
    mut core_request: Request,
    mut stores: Stores,
    prepared: PreparedIngress,
    deliver: Deliver,
) -> Result<Output, WorkerError>
where
    Deliver: FnOnce(ResponseEgressEnvelope) -> Result<Output, EdgeError>,
{
    // Hard-cutoff: see fastly's `dispatch_core_request`
    // for the rationale. Only registries go into extensions —
    // legacy bare handles are synthesised into a one-id registry
    // at the dispatch boundary.
    if let Some(variables) = stores.runtime_variables.take() {
        core_request.extensions_mut().insert(variables);
    }
    let (config_registry, kv_registry, secret_registry) = synthesise_store_registries(stores);
    if let Some(registry) = config_registry {
        core_request.extensions_mut().insert(registry);
    }
    if let Some(registry) = kv_registry {
        core_request.extensions_mut().insert(registry);
    }
    if let Some(registry) = secret_registry {
        core_request.extensions_mut().insert(registry);
    }
    let response = app.dispatch_admitted(prepared, core_request).await;
    deliver(response).map_err(|err| edge_error_to_worker(&err))
}

fn edge_error_to_worker(err: &EdgeError) -> WorkerError {
    WorkerError::RustError(err.to_string())
}

fn into_core_method(method: &Method) -> CoreMethod {
    let bytes = method.as_ref().as_bytes();
    CoreMethod::from_bytes(bytes).unwrap_or_else(|_| {
        log::warn!(
            "unknown HTTP method {:?}, defaulting to GET",
            method.as_ref()
        );
        CoreMethod::GET
    })
}

/// Begins canonical detached delivery of an assembly error without a configured app.
///
/// Releases the unread request after capturing its method and abort signal. The caller supplies
/// the pre-assembly start and clock; no app callbacks, stores or configured headers are used.
///
/// # Errors
/// Returns a category-safe delivery error if conversion and its bounded fallback both fail.
#[inline]
pub fn send_assembly_error(
    req: CfRequest,
    error: EdgeError,
    ctx: Context,
    request_start: MonotonicInstant,
    clock: MonotonicClock,
) -> Result<CfResponse, WorkerError> {
    let method = into_core_method(&req.method());
    let runtime = CloudflareEgressRuntime::new(req.inner().signal(), Arc::new(ctx));
    drop(req);
    from_egress_response(
        App::assembly_error_egress(error, method, request_start, clock),
        runtime,
    )
    .map_err(|delivery_error| edge_error_to_worker(&delivery_error))
}

fn open_config_handle(env: &Env, binding_name: &str) -> ConfigStoreHandle {
    match CloudflareConfigStore::from_env(env, binding_name) {
        Ok(store) => ConfigStoreHandle::new(Arc::new(store)),
        Err(err) => {
            warn_missing_config_binding_once(binding_name, &err.to_string());
            ConfigStoreHandle::failed_open(ConfigStoreOpenFailure::Unavailable)
        }
    }
}

/// Pure synthesis: collapse a `Stores` (which may carry both a
/// wired multi-id registry AND a legacy bare handle) into the
/// three registries that go into request extensions. Precedence
/// is "registry wins": a wired registry is taken verbatim; only
/// in its absence is a bare handle wrapped into a one-id registry
/// keyed under `"default"`. The bare handle is never merged in,
/// never used as a fallback for ids the registry doesn't define.
/// Pulled out as a pure function so the precedence contract is
/// unit-testable without spinning up a real `Request` and async
/// dispatcher.
fn synthesise_store_registries(
    stores: Stores,
) -> (
    Option<ConfigRegistry>,
    Option<KvRegistry>,
    Option<SecretRegistry>,
) {
    let config_registry = stores.config_registry.or_else(|| {
        stores.config_store.map(|handle| {
            ConfigRegistry::single_id(
                "default".to_owned(),
                ConfigStoreBinding {
                    handle,
                    default_key: "default".to_owned(),
                },
            )
        })
    });
    let kv_registry = stores.kv_registry.or_else(|| {
        stores
            .kv
            .map(|handle| KvRegistry::single_id("default".to_owned(), handle))
    });
    let secret_registry = stores.secret_registry.or_else(|| {
        stores.secrets.map(|handle| {
            SecretRegistry::single_id(
                "default".to_owned(),
                BoundSecretStore::new(handle, "default".to_owned()),
            )
        })
    });
    (config_registry, kv_registry, secret_registry)
}

fn warn_missing_config_binding_once(binding: &str, error: &impl Display) {
    static WARNED_BINDINGS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();
    let warned_bindings = WARNED_BINDINGS.get_or_init(|| Mutex::new(BTreeSet::new()));

    match warned_bindings.lock() {
        Ok(mut guard) => {
            if !guard.insert(binding.to_owned()) {
                return;
            }
            log::warn!("config KV binding '{binding}' not available: {error}");
        }
        Err(_) => {
            log::warn!("config KV binding '{binding}' not available: {error}");
        }
    }
}

fn warn_missing_kv_binding_once(kv_binding: &str, error: &impl Display) {
    static WARNED_BINDINGS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();
    let warned_bindings = WARNED_BINDINGS.get_or_init(|| Mutex::new(BTreeSet::new()));

    match warned_bindings.lock() {
        Ok(mut guard) => {
            if !guard.insert(kv_binding.to_owned()) {
                return;
            }
            log::warn!("KV binding '{kv_binding}' not available: {error}");
        }
        Err(_) => {
            log::warn!("KV binding '{kv_binding}' not available: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::cell::RefCell;
    use std::rc::{Rc, Weak};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;
    use std::time::Duration;

    use super::*;
    use crate::test_runtime::NumericTimeoutGuard;
    use async_stream::stream;
    use edgezero_core::app::Hooks;
    use edgezero_core::context::{BodyKind, RequestContext};
    use edgezero_core::http::{Response, StatusCode, response_builder};
    use edgezero_core::ingress::{AdmissionDecision, IngressGrant};
    use edgezero_core::outbound::{
        OutboundBatch, OutboundCollectedSlot, OutboundFuture, OutboundPending, OutboundRequest,
        OutboundResponse,
    };
    use edgezero_core::response_egress::{
        ResponseEgressCompletion, ResponseEgressDeadline, ResponseEgressObserver,
        ResponseEgressOutcome, ResponseEgressReport,
    };
    use edgezero_core::router::RouterService;
    use futures::channel::oneshot;
    use futures_util::FutureExt as _;
    use futures_util::future::join;
    #[cfg(feature = "test-utils")]
    use futures_util::stream::empty;
    use futures_util::stream::poll_fn;
    use wasm_bindgen_test::wasm_bindgen_test;
    use worker::js_sys::{Function, Object, Reflect, Uint8Array};
    use worker::wasm_bindgen::JsCast as _;
    use worker::wasm_bindgen::JsValue;
    use worker::wasm_bindgen_futures::JsFuture;
    use worker::worker_sys::Context as WorkerSysContext;

    thread_local! {
        static RETAINED_SEND: RefCell<Option<OutboundFuture<OutboundResponse>>> = const { RefCell::new(None) };
        static RETAINED_UPLOAD: RefCell<Weak<()>> = const { RefCell::new(Weak::new()) };
        static RETAINED_PROGRESS: RefCell<Option<(OutboundPending, OutboundBatch)>> = const { RefCell::new(None) };
    }

    struct FailingScopeAssembly;

    struct ScopeFetchProbe {
        probe: JsValue,
        restore: Function,
    }

    struct ScopeRoutes;

    struct ScopeTerminalObserver(
        Mutex<Option<oneshot::Sender<ResponseEgressReport>>>,
        Arc<AtomicUsize>,
    );

    struct ScopeSourceDrop(Arc<AtomicUsize>);
    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for ScopeSourceDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl ResponseEgressObserver for ScopeTerminalObserver {
        fn complete(&self, report: &ResponseEgressReport) {
            self.1.fetch_add(1, Ordering::SeqCst);
            let sender = self
                .0
                .lock()
                .expect("terminal sender")
                .take()
                .expect("one terminal report");
            sender.send(report.clone()).expect("terminal receiver");
        }
    }

    impl ScopeFetchProbe {
        fn count(&self, field: &str) -> Option<f64> {
            Reflect::get(&self.probe, &JsValue::from_str(field))
                .expect("counter")
                .as_f64()
        }

        fn install() -> Self {
            let probe = Function::new_no_args("const original = globalThis.fetch; const probe = {calls: 0, aborts: 0}; globalThis.fetch = (request, init) => { probe.calls++; if (init.encodeResponseBody !== 'manual' || request.redirect !== 'manual') throw new Error('manual policy lost'); init.signal.addEventListener('abort', () => { probe.aborts++; }, {once: true}); return new Promise(() => {}); }; probe.restore = () => {globalThis.fetch = original;}; return probe;")
                .call0(&JsValue::UNDEFINED).expect("pending fetch probe");
            let restore = Reflect::get(&probe, &JsValue::from_str("restore"))
                .expect("restore")
                .dyn_into::<Function>()
                .expect("restore function");
            Self { probe, restore }
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "Drop::pin_drop is unstable and conflicts with drop"
    )]
    impl Drop for ScopeFetchProbe {
        fn drop(&mut self) {
            self.restore
                .call0(&JsValue::UNDEFINED)
                .expect("restore fetch");
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "exercise default application metadata through both native dispatch entrypoints"
    )]
    impl Hooks for ScopeRoutes {
        fn routes() -> Result<RouterService, EdgeError> {
            Ok(RouterService::builder()
                .get("/", retain_progress_handler)
                .build())
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "exercise defaults before typed route-assembly failure"
    )]
    impl Hooks for FailingScopeAssembly {
        fn routes() -> Result<RouterService, EdgeError> {
            let client = outbound_client(MonotonicClock::default());
            let send = client.send(
                OutboundRequest::post("https://fixture.invalid/")?.body(pending_scope_upload()),
            );
            RETAINED_SEND.with(|retained| *retained.borrow_mut() = Some(send));
            Err(EdgeError::service_unavailable("assembly fixture"))
        }
    }

    fn pending_scope_upload() -> Body {
        let source = Rc::new(());
        RETAINED_UPLOAD.with(|retained| *retained.borrow_mut() = Rc::downgrade(&source));
        Body::from_stream(poll_fn(move |_| {
            assert_eq!(Rc::strong_count(&source), 1);
            Poll::<Option<Result<Bytes, EdgeError>>>::Pending
        }))
    }

    async fn retain_progress(client: &HttpClient) -> Result<(), EdgeError> {
        let pending = client
            .begin(OutboundRequest::get("https://fixture.invalid/begin")?)
            .await?;
        let batch = client.start_batch_until(
            vec![
                OutboundRequest::get("https://fixture.invalid/first")?,
                OutboundRequest::get("https://fixture.invalid/second")?,
            ],
            Deadline::after(Duration::from_secs(10)),
        );
        RETAINED_PROGRESS.with(|retained| *retained.borrow_mut() = Some((pending, batch)));
        Ok(())
    }

    fn assert_retained_fetch_progress_closed(probe: &ScopeFetchProbe) {
        assert_eq!(
            probe.count("aborts"),
            Some(3.0_f64),
            "all fetch aborts precede consuming retained handles"
        );
        let (origin, batch) = RETAINED_PROGRESS
            .with(|slot| slot.borrow_mut().take())
            .expect("retained original handles");
        assert!(matches!(
            origin
                .collect()
                .now_or_never()
                .expect("closed begun receiver ready"),
            Err(EdgeError::Internal { .. })
        ));
        let failure = batch
            .collect()
            .now_or_never()
            .expect("closed original batch ready")
            .expect_err("closed batch");
        assert!(matches!(failure.error(), EdgeError::Internal { .. }));
        assert_eq!(failure.slots().len(), 2);
        assert!(
            failure
                .slots()
                .iter()
                .all(|slot| matches!(slot, OutboundCollectedSlot::Unresolved))
        );
        assert_eq!(
            probe.count("aborts"),
            Some(3.0_f64),
            "later handle consumption cannot duplicate aborts"
        );
    }

    async fn retain_progress_handler(ctx: RequestContext) -> Result<Response, EdgeError> {
        retain_progress(&ctx.http_client().expect("request HTTP client")).await?;
        Delay::from(Duration::from_millis(10)).await;
        response_builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .map_err(EdgeError::internal)
    }

    #[wasm_bindgen_test]
    async fn canonical_dispatch_handoff_aborts_retained_begin_and_batch_fetches() {
        let _timers = NumericTimeoutGuard::install();
        for manifest_dispatch in [false, true] {
            let probe = ScopeFetchProbe::install();
            let app = App::build::<ScopeRoutes>(crate::CLOUDFLARE_PLATFORM).expect("application");
            let (env, context) = env_context();
            let req = native_request(Method::Get, "/", None);
            let response = if manifest_dispatch {
                crate::dispatch_app::<ScopeRoutes>(&app, req, env, context).await
            } else {
                CloudflareService::new(&app)
                    .dispatch(req, env, context)
                    .await
            }
            .expect("native handoff");
            let aborts_at_handoff = probe.count("aborts");
            let retained = RETAINED_PROGRESS.with(|retained| retained.borrow_mut().take());
            assert!(
                retained.is_some(),
                "both progress handles remain caller-retained"
            );
            drop(retained);
            assert_eq!(response.status_code(), StatusCode::NO_CONTENT.as_u16());
            assert_eq!(probe.count("calls"), Some(3.0_f64));
            assert_eq!(
                aborts_at_handoff,
                Some(3.0_f64),
                "scope closes before handoff returns"
            );
            assert_eq!(
                probe.count("aborts"),
                Some(3.0_f64),
                "no duplicate abort on handle drop"
            );
        }
    }

    #[wasm_bindgen_test]
    #[cfg(feature = "test-utils")]
    async fn nested_dispatch_return_keeps_outer_callback_work_live() {
        let _timers = NumericTimeoutGuard::install();
        for helper in [0_u8, 1, 2] {
            let probe = ScopeFetchProbe::install();
            let outer = OutboundScopeOwner::callback();
            let app = App::build::<ScopeRoutes>(crate::CLOUDFLARE_PLATFORM).expect("application");
            outer
                .run(async {
                    if helper == 2 {
                        drop(
                            dispatch_ingress_stream_for_test(
                                &app,
                                CoreMethod::GET,
                                "/".parse().expect("URI"),
                                empty::<Result<Bytes, EdgeError>>(),
                            )
                            .await
                            .expect("test dispatch helper"),
                        );
                    } else {
                        let (env, context) = env_context();
                        let req = native_request(Method::Get, "/", None);
                        let response = if helper == 1 {
                            crate::dispatch_app::<ScopeRoutes>(&app, req, env, context).await
                        } else {
                            CloudflareService::new(&app)
                                .dispatch(req, env, context)
                                .await
                        }
                        .expect("nested dispatch helper");
                        assert_eq!(response.status_code(), StatusCode::NO_CONTENT.as_u16());
                    }
                })
                .await;
            let aborts_at_helper_return = probe.count("aborts");
            drop(outer);
            let aborts_at_outer_exit = probe.count("aborts");
            let retained = RETAINED_PROGRESS.with(|retained| retained.borrow_mut().take());
            assert!(retained.is_some(), "outer caller retains both handles");
            drop(retained);
            assert_eq!(probe.count("calls"), Some(3.0_f64));
            assert_eq!(
                aborts_at_helper_return,
                Some(0.0_f64),
                "helper return is not callback return"
            );
            assert_eq!(
                aborts_at_outer_exit,
                Some(3.0_f64),
                "only the outer callback closes work"
            );
            assert_eq!(probe.count("aborts"), Some(3.0_f64), "no duplicate cleanup");
        }
    }

    #[wasm_bindgen_test]
    async fn closed_nested_dispatch_cannot_create_a_fresh_callback_scope() {
        let _timers = NumericTimeoutGuard::install();
        let probe = ScopeFetchProbe::install();
        let outer = OutboundScopeOwner::callback();
        let application_status = Arc::new(Mutex::new(None));
        let observed = Arc::clone(&application_status);
        let router = RouterService::builder()
            .get("/", move |ctx: RequestContext| {
                let recorded = Arc::clone(&observed);
                async move {
                    let client = ctx.http_client().expect("request HTTP client");
                    let error = client
                        .send(
                            OutboundRequest::get("https://fixture.invalid/")?
                                .timeout(Duration::ZERO),
                        )
                        .await
                        .expect_err("closed scope refuses outbound dispatch");
                    if !matches!(error, EdgeError::Internal { .. }) {
                        return Err(error);
                    }
                    let response = response_builder()
                        .status(StatusCode::NO_CONTENT)
                        .body(Body::empty())
                        .map_err(EdgeError::internal)?;
                    *recorded.lock().expect("application status") = Some(response.status());
                    Ok(response)
                }
            })
            .build();
        let app = App::new(router);
        let (env, context) = env_context();
        outer.close();
        let error = outer
            .run(CloudflareService::new(&app).dispatch(
                native_request(Method::Get, "/", None),
                env,
                context,
            ))
            .await
            .expect_err("closed ambient scope cannot create a new egress lifetime");
        assert_eq!(
            *application_status.lock().expect("application status"),
            Some(StatusCode::NO_CONTENT),
            "helper must preserve the closed ambient scope"
        );
        assert!(
            matches!(error, WorkerError::RustError(message) if message == "internal error: response conversion failed")
        );
        assert_eq!(
            probe.count("calls"),
            Some(0.0_f64),
            "closed dispatch performs no SDK I/O"
        );
    }

    #[wasm_bindgen_test]
    async fn attached_body_owner_keeps_pre_head_fetches_live_without_changing_framing() {
        let _timers = NumericTimeoutGuard::install();
        for streamed in [false, true] {
            let probe = ScopeFetchProbe::install();
            let router = RouterService::builder()
                .get("/", move |ctx: RequestContext| async move {
                    let owner = OutboundScopeOwner::response_body();
                    owner
                        .run(retain_progress(&ctx.http_client().expect("HTTP client")))
                        .await?;
                    let body = if streamed {
                        Body::from_stream(stream! {
                            yield Ok(Bytes::from_static(b"live"));
                        })
                    } else {
                        Body::from(Bytes::from_static(b"live"))
                    };
                    owner.attach(Response::new(body))
                })
                .build();
            let mut app = App::new(router);
            let (published, terminal) = oneshot::channel();
            app.set_response_egress_observer(ScopeTerminalObserver(
                Mutex::new(Some(published)),
                Arc::new(AtomicUsize::new(0)),
            ));
            let (env, context) = env_context();
            let mut response = CloudflareService::new(&app)
                .dispatch(native_request(Method::Get, "/", None), env, context)
                .await
                .expect("native handoff before origin heads");
            assert_eq!(response.status_code(), StatusCode::OK.as_u16());
            let declared = response
                .headers()
                .get("content-length")
                .expect("length header");
            assert_eq!(declared.as_deref(), if streamed { None } else { Some("4") });
            Delay::from(Duration::from_millis(10)).await;
            assert_eq!(probe.count("calls"), Some(3.0_f64));
            assert_eq!(
                probe.count("aborts"),
                Some(0.0_f64),
                "attached jobs survive callback return"
            );
            assert_eq!(response.bytes().await.expect("body bytes"), b"live");
            let _report = terminal.await.expect("canonical terminal report");
            let aborts_at_terminal = probe.count("aborts");
            let retained = RETAINED_PROGRESS.with(|retained| retained.borrow_mut().take());
            assert!(
                retained.is_some(),
                "handles remain retained after body completion"
            );
            drop(retained);
            assert_eq!(
                aborts_at_terminal,
                Some(3.0_f64),
                "exact attached owner closes at terminal"
            );
            assert_eq!(
                probe.count("aborts"),
                Some(3.0_f64),
                "each fetch aborts once"
            );
        }
    }

    #[wasm_bindgen_test]
    async fn response_body_scope_keeps_fetches_live_until_eof_or_source_error() {
        let _timers = NumericTimeoutGuard::install();
        for source_error in [false, true] {
            let probe = ScopeFetchProbe::install();
            let (finish, completion_rx) = oneshot::channel::<()>();
            let shared_receiver = Arc::new(Mutex::new(Some(completion_rx)));
            let router = RouterService::builder()
                .get("/", move |ctx: RequestContext| {
                    let complete_body = shared_receiver
                        .lock()
                        .expect("finish receiver")
                        .take()
                        .expect("one body");
                    async move {
                        let client = ctx.http_client().expect("request HTTP client");
                        let source = stream! {
                            retain_progress(&client).await.expect("body progress");
                            yield Ok(Bytes::from_static(b"live"));
                            complete_body.await.expect("body release");
                            if source_error {
                                yield Err(EdgeError::bad_gateway("body fixture"));
                            }
                        };
                        Ok::<_, EdgeError>(Response::new(Body::from_stream(source)))
                    }
                })
                .build();
            let mut app = App::new(router);
            let (published, terminal) = oneshot::channel();
            app.set_response_egress_observer(ScopeTerminalObserver(
                Mutex::new(Some(published)),
                Arc::new(AtomicUsize::new(0)),
            ));
            let (env, context) = env_context();
            let mut response = CloudflareService::new(&app)
                .dispatch(native_request(Method::Get, "/", None), env, context)
                .await
                .expect("streaming native handoff");
            let release = async {
                Delay::from(Duration::from_millis(10)).await;
                assert_eq!(probe.count("calls"), Some(3.0_f64));
                assert_eq!(
                    probe.count("aborts"),
                    Some(0.0_f64),
                    "body work survives callback handoff"
                );
                finish.send(()).expect("release body");
            };
            let (received, ()) = join(response.bytes(), release).await;
            if source_error {
                assert!(received.is_err());
            } else {
                assert_eq!(received.expect("body bytes"), b"live");
            }
            let _report = terminal.await.expect("canonical egress terminal report");
            let aborts_at_terminal = probe.count("aborts");
            let retained = RETAINED_PROGRESS.with(|retained| retained.borrow_mut().take());
            assert!(
                retained.is_some(),
                "body still retains both progress handles"
            );
            drop(retained);
            assert_eq!(
                aborts_at_terminal,
                Some(3.0_f64),
                "body terminal closes its scope"
            );
            assert_eq!(
                probe.count("aborts"),
                Some(3.0_f64),
                "each native fetch aborts once"
            );
        }
    }

    fn scope_terminal_router(
        mode: &'static str,
        body_wait: oneshot::Receiver<()>,
        tail_notice: oneshot::Sender<()>,
        source_drops: Arc<AtomicUsize>,
        source_polls: Arc<AtomicUsize>,
    ) -> RouterService {
        let gate_slot = Arc::new(Mutex::new(Some(body_wait)));
        let tail_slot = Arc::new(Mutex::new(Some(tail_notice)));
        RouterService::builder().get("/",move |ctx:RequestContext| {
            let gate=gate_slot.lock().expect("source gate").take().expect("one request");
            let tail=tail_slot.lock().expect("tail signal").take().expect("one request");
            let guard=ScopeSourceDrop(Arc::clone(&source_drops));
            let observed_polls=Arc::clone(&source_polls);
            async move {
                let owner=OutboundScopeOwner::response_body();
                owner.run(retain_progress(&ctx.http_client().expect("HTTP client"))).await?;
                let source=stream! {
                    let _guard=guard;
                    observed_polls.fetch_add(1,Ordering::SeqCst);
                    yield Ok(Bytes::from_static(b"live"));
                    observed_polls.fetch_add(1,Ordering::SeqCst);
                    gate.await.expect("source release");
                    observed_polls.fetch_add(1,Ordering::SeqCst);
                    if mode=="source" {yield Err(EdgeError::bad_gateway("controlled source failure"));}
                    if mode=="transport" {tail.send(()).expect("tail observer");yield Ok(Bytes::from_static(b"tail"));}
                };
                let mut response=Response::new(Body::from_stream(source));
                if mode=="deadline" {response.extensions_mut().insert(ResponseEgressDeadline::after(Duration::from_secs(1)));}
                owner.attach(response)
            }
        }).build()
    }

    async fn trigger_scope_terminal(
        mode: &str,
        release: oneshot::Sender<()>,
        tail_started: oneshot::Receiver<()>,
        controller: &web_sys::AbortController,
        reader: &web_sys::ReadableStreamDefaultReader,
    ) -> Option<oneshot::Sender<()>> {
        let mut pending_release = Some(release);
        if matches!(mode, "eof" | "source" | "transport") {
            pending_release
                .take()
                .expect("gate")
                .send(())
                .expect("source pending");
        }
        if mode == "cancel" {
            controller.abort();
        }
        if mode == "transport" {
            tail_started
                .await
                .expect("second chunk produced without another read");
            JsFuture::from(reader.cancel())
                .await
                .expect("cancel response reader");
        } else {
            let next = JsFuture::from(reader.read()).await;
            if mode == "eof" {
                assert_eq!(
                    Reflect::get(&next.expect("EOF"), &JsValue::from_str("done")).expect("done"),
                    JsValue::TRUE
                );
            } else {
                assert!(next.is_err(), "terminal failure rejects the reader");
            }
        }
        pending_release
    }

    #[wasm_bindgen_test]
    async fn attached_progress_closes_on_worker_terminal_paths_before_retained_handles() {
        let _timers = NumericTimeoutGuard::install();
        for (mode, expected) in [
            ("eof", ResponseEgressOutcome::HostHandoff),
            ("source", ResponseEgressOutcome::SourceError),
            ("deadline", ResponseEgressOutcome::DeadlineExceeded),
            ("cancel", ResponseEgressOutcome::RequestCancelled),
            ("transport", ResponseEgressOutcome::TransportError),
        ] {
            let probe = ScopeFetchProbe::install();
            let (release, wait) = oneshot::channel::<()>();
            let (tail_ready, tail_started) = oneshot::channel::<()>();
            let drops = Arc::new(AtomicUsize::new(0));
            let polls = Arc::new(AtomicUsize::new(0));
            let router = scope_terminal_router(
                mode,
                wait,
                tail_ready,
                Arc::clone(&drops),
                Arc::clone(&polls),
            );
            let mut app = App::new(router);
            let (published, terminal) = oneshot::channel();
            let reports = Arc::new(AtomicUsize::new(0));
            app.set_response_egress_observer(ScopeTerminalObserver(
                Mutex::new(Some(published)),
                Arc::clone(&reports),
            ));
            let (env, context) = env_context();
            let controller = web_sys::AbortController::new().expect("abort controller");
            let init = web_sys::RequestInit::new();
            init.set_signal(Some(&controller.signal()));
            let request = web_sys::Request::new_with_str_and_init("https://example.com/", &init)
                .expect("native request");
            let response = CloudflareService::new(&app)
                .dispatch(CfRequest::from(request), env, context)
                .await
                .expect("response handoff");
            assert_eq!(response.status_code(), 200);
            let (_, worker::ResponseBody::Stream(readable)) = response.into_parts() else {
                panic!("native response stream");
            };
            let reader = readable
                .get_reader()
                .dyn_into::<web_sys::ReadableStreamDefaultReader>()
                .expect("stream reader");
            let prefix = JsFuture::from(reader.read()).await.expect("prefix read");
            assert_eq!(
                Reflect::get(&prefix, &JsValue::from_str("done")).expect("done"),
                JsValue::FALSE
            );
            let value = Reflect::get(&prefix, &JsValue::from_str("value")).expect("prefix value");
            assert_eq!(Uint8Array::new(&value).to_vec(), b"live");
            assert_eq!(probe.count("calls"), Some(3.0_f64));
            assert_eq!(probe.count("aborts"), Some(0.0_f64));
            RETAINED_PROGRESS.with(|storage| {
                let mut state = storage.borrow_mut();
                let (origin, batch) = state.as_mut().expect("original handles");
                assert!(!origin.is_ready());
                assert!(matches!(
                    batch.check().expect("batch pending"),
                    edgezero_core::OutboundBatchCheck::Pending
                ));
            });
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            assert_eq!(reports.load(Ordering::SeqCst), 0);
            let pending_release =
                trigger_scope_terminal(mode, release, tail_started, &controller, &reader).await;
            let report = terminal.await.expect("canonical terminal");
            assert_eq!(report.outcome, expected);
            assert_eq!(report.bytes_written, 4);
            assert_eq!(
                drops.load(Ordering::SeqCst),
                1,
                "source released before reader and handles drop"
            );
            if let Some(pending_sender) = pending_release {
                assert!(
                    pending_sender.send(()).is_err(),
                    "terminal source no longer owns its receiver"
                );
            }
            assert_retained_fetch_progress_closed(&probe);
            let terminal_polls = polls.load(Ordering::SeqCst);
            drop(reader);
            drop(readable);
            Delay::from(Duration::ZERO).await;
            assert_eq!(polls.load(Ordering::SeqCst), terminal_polls);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(reports.load(Ordering::SeqCst), 1);
            assert_eq!(probe.count("calls"), Some(3.0_f64));
            assert_eq!(probe.count("aborts"), Some(3.0_f64));
        }
    }

    #[wasm_bindgen_test]
    async fn run_app_scope_covers_assembly_error_and_native_handoff() {
        let _timers = NumericTimeoutGuard::install();
        let (env, context) = env_context();
        let mut response = crate::run_app::<FailingScopeAssembly>(
            native_request(Method::Get, "/", None),
            env,
            context,
        )
        .await
        .expect("assembly-error native handoff");
        let released_at_handoff =
            RETAINED_UPLOAD.with(|retained| retained.borrow().upgrade().is_none());
        let retained = RETAINED_SEND.with(|slot| slot.borrow_mut().take());
        assert!(
            retained.is_some(),
            "assembly hook retains its unpolled send"
        );
        drop(retained);
        assert!(
            released_at_handoff,
            "assembly failure closes the outer callback"
        );
        assert_eq!(
            response.status_code(),
            StatusCode::SERVICE_UNAVAILABLE.as_u16()
        );
        assert!(
            !response
                .bytes()
                .await
                .expect("assembly-error body")
                .is_empty()
        );
    }

    #[wasm_bindgen_test]
    async fn callback_handoff_releases_retained_unpolled_send_without_polling_upload() {
        let polls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&polls);
        let router = RouterService::builder()
            .get("/", move |ctx: RequestContext| {
                let observed = Arc::clone(&counted);
                async move {
                    let source = Rc::new(());
                    RETAINED_UPLOAD
                        .with(|retained| *retained.borrow_mut() = Rc::downgrade(&source));
                    let body = Body::from_stream(poll_fn(move |_| {
                        assert_eq!(Rc::strong_count(&source), 1);
                        observed.fetch_add(1, Ordering::SeqCst);
                        Poll::<Option<Result<Bytes, EdgeError>>>::Pending
                    }));
                    let client = ctx.http_client().expect("request HTTP client");
                    let send = client.send(
                        OutboundRequest::post("https://fixture.invalid/")
                            .expect("request")
                            .body(body),
                    );
                    RETAINED_SEND.with(|retained| *retained.borrow_mut() = Some(send));
                    response_builder()
                        .status(StatusCode::NO_CONTENT)
                        .body(Body::empty())
                        .map_err(EdgeError::internal)
                }
            })
            .build();
        let app = App::new(router);
        let (env, context) = env_context();
        let response = CloudflareService::new(&app)
            .dispatch(native_request(Method::Get, "/", None), env, context)
            .await
            .expect("native handoff");
        let released_at_handoff =
            RETAINED_UPLOAD.with(|retained| retained.borrow().upgrade().is_none());
        let retained = RETAINED_SEND.with(|slot| slot.borrow_mut().take());
        assert!(retained.is_some(), "caller still retains the unpolled send");
        drop(retained);
        drop(response);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert!(released_at_handoff, "callback closes at native handoff");
        assert!(RETAINED_UPLOAD.with(|upload_ref| upload_ref.borrow().upgrade().is_none()));
    }

    fn native_request(method: Method, path: &str, body: Option<&[u8]>) -> CfRequest {
        let mut init = worker::RequestInit::new();
        init.with_method(method);
        if let Some(bytes) = body {
            init.with_body(Some(Uint8Array::from(bytes).into()));
        }
        CfRequest::new_with_init(&format!("https://example.com{path}"), &init)
            .expect("native request")
    }

    fn env_context() -> (Env, Context) {
        Function::new_no_args(include_str!("../tests/worker_runtime.js"))
            .call0(&JsValue::UNDEFINED)
            .expect("guest Workers stream fixture");
        let context = Object::new();
        Reflect::set(
            &context,
            &JsValue::from_str("waitUntil"),
            Function::new_with_args("_promise", "return undefined;").as_ref(),
        )
        .expect("guest waitUntil");
        (
            Object::new().unchecked_into::<Env>(),
            Context::new(context.unchecked_into::<WorkerSysContext>()),
        )
    }

    async fn native_ingress(
        app: &App,
        req: CfRequest,
    ) -> Result<ResponseEgressEnvelope, WorkerError> {
        let (env, context) = env_context();
        let head = into_core_request_head(&req, env, Arc::new(context), app.monotonic_clock())
            .expect("request head");
        dispatch_ingress_stream_and(
            app,
            head,
            Stores::default(),
            app.monotonic_now(),
            move || cloudflare_body_stream(req),
            Ok,
        )
        .await
    }

    #[wasm_bindgen_test]
    async fn native_null_body_stays_buffered_without_reading() {
        for method in [Method::Get, Method::Head, Method::Post] {
            let req = native_request(method, "/", None);
            assert!(req.inner().body().is_none());
            let tripwire = Function::new_no_args("throw new Error('unexpected body read');");
            assert!(
                Reflect::set(req.inner(), &JsValue::from_str("arrayBuffer"), &tripwire)
                    .expect("install read tripwire")
            );
            let (env, context) = env_context();
            let request = into_core_request(req, env, context).await.expect("request");
            assert_eq!(request.body().as_bytes(), Some(&[][..]));
        }
    }

    #[wasm_bindgen_test]
    async fn native_present_body_remains_a_lazy_stream_even_when_empty() {
        for bytes in [b"".as_slice(), b"data".as_slice()] {
            for value in [
                JsValue::from(Uint8Array::from(bytes)),
                Uint8Array::from(bytes).buffer().into(),
            ] {
                let mut init = worker::RequestInit::new();
                init.with_method(Method::Post).with_body(Some(value));
                let mut req =
                    CfRequest::new_with_init("https://example.com/", &init).expect("request");
                req.headers_mut()
                    .expect("mutable headers")
                    .set("content-length", "0")
                    .expect("header");
                assert!(req.inner().body().is_some());
                let (env, context) = env_context();
                let request = into_core_request(req, env, context).await.expect("request");
                assert!(request.body().is_stream());
                assert_eq!(
                    request
                        .into_body()
                        .into_bytes_bounded(4)
                        .await
                        .expect("body")
                        .as_ref(),
                    bytes
                );
            }
        }
    }

    #[wasm_bindgen_test]
    async fn native_absence_does_not_depend_on_sdk_read_once_bookkeeping() {
        for body in [None, Some(b"data".as_slice())] {
            let mut req = native_request(Method::Post, "/", body);
            assert_eq!(
                req.bytes().await.expect("original read").as_slice(),
                body.unwrap_or_default()
            );
            let (env, context) = env_context();
            let outcome = into_core_request(req, env, context).await;
            if body.is_none() {
                assert_eq!(
                    outcome.expect("absent body").body().as_bytes(),
                    Some(&[][..])
                );
            } else {
                assert!(matches!(outcome, Err(EdgeError::Internal { .. })));
            }
        }
    }

    #[wasm_bindgen_test]
    async fn admitted_null_body_transfers_but_first_read_keeps_expired_deadline() {
        #[edgezero_core::action]
        async fn transfer(ctx: RequestContext) -> Result<Response, EdgeError> {
            assert_eq!(ctx.body_kind(), BodyKind::Initial);
            let request = ctx.into_request()?;
            response_builder()
                .header(
                    "x-body-buffered",
                    if request.body().is_stream() {
                        "no"
                    } else {
                        "yes"
                    },
                )
                .body(Body::empty())
                .map_err(EdgeError::internal)
        }

        #[edgezero_core::action]
        async fn read(ctx: RequestContext) -> Result<Response, EdgeError> {
            assert_eq!(ctx.body_kind(), BodyKind::Initial);
            let error = ctx.body_bytes(4).await.expect_err("expired first read");
            assert_eq!(error.status(), StatusCode::REQUEST_TIMEOUT);
            assert_eq!(ctx.body_kind(), BodyKind::Poisoned);
            assert_eq!(
                ctx.take_body().expect_err("sticky timeout").status(),
                error.status()
            );
            Err(error)
        }

        let start = MonotonicInstant::now();
        let mut app = App::new(
            RouterService::builder()
                .post("/transfer", transfer)
                .post("/read", read)
                .build(),
        );
        app.set_monotonic_clock(MonotonicClock::new(move || start));
        app.set_ingress_admission_policy(|head| {
            assert_eq!(head.framing(), IngressFraming::HostManaged);
            AdmissionDecision::Admit {
                completion: ResponseEgressCompletion::empty(),
                grant: IngressGrant::empty(),
                read_deadline: Deadline::at_instant(head.request_start()),
            }
        });
        for body in [None, Some(b"".as_slice()), Some(b"data".as_slice())] {
            for path in ["/transfer", "/read"] {
                let mut egress = native_ingress(&app, native_request(Method::Post, path, body))
                    .await
                    .expect("response");
                let response = egress.response_mut();
                if path == "/read" {
                    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
                } else {
                    assert_eq!(response.status(), StatusCode::OK);
                    assert_eq!(
                        response.headers().get("x-body-buffered").expect("header"),
                        if body.is_none() { "yes" } else { "no" }
                    );
                }
            }
        }
    }

    #[wasm_bindgen_test]
    async fn native_source_is_not_inspected_on_refusal_or_abort() {
        for abort in [false, true] {
            let mut app = App::new(RouterService::builder().build());
            app.set_ingress_admission_policy(move |_head| {
                if abort {
                    AdmissionDecision::Abort
                } else {
                    AdmissionDecision::Refuse {
                        completion: ResponseEgressCompletion::empty(),
                        response: response_builder()
                            .status(StatusCode::SERVICE_UNAVAILABLE)
                            .body(Body::empty())
                            .expect("refusal"),
                    }
                }
            });
            let req = native_request(Method::Post, "/", Some(b"data"));
            Function::new_with_args("request",
                "Object.defineProperty(request, 'body', {get() {throw new Error('unexpected source inspection');}});")
                .call1(&JsValue::UNDEFINED, req.inner()).expect("install source tripwire");
            let outcome = native_ingress(&app, req).await;
            if abort {
                assert!(outcome.is_err());
            } else {
                assert_eq!(
                    outcome.expect("refusal").response_mut().status(),
                    StatusCode::SERVICE_UNAVAILABLE
                );
            }
        }
    }

    #[wasm_bindgen_test]
    fn into_http_method_defaults_unknown_to_get() {
        let method = Method::from("FOO".to_owned());
        assert_eq!(into_core_method(&method), CoreMethod::GET);
    }

    #[wasm_bindgen_test]
    fn into_http_method_maps_known_methods() {
        assert_eq!(into_core_method(&Method::Get), CoreMethod::GET);
        assert_eq!(into_core_method(&Method::Post), CoreMethod::POST);
        assert_eq!(into_core_method(&Method::Put), CoreMethod::PUT);
        assert_eq!(into_core_method(&Method::Delete), CoreMethod::DELETE);
    }

    #[wasm_bindgen_test]
    async fn router_state_client_overrides_adapter_default() {
        use edgezero_core::OutboundTime;

        let _timers = NumericTimeoutGuard::install();
        for state in [true, false] {
            let start = MonotonicInstant::now();
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&calls);
            let clock = MonotonicClock::new(move || {
                observed.fetch_add(1, Ordering::SeqCst);
                start
            });
            let supplied = HttpClient::with_client(CloudflareOutboundClient::with_time(
                OutboundTime::new(clock, |_, _| {
                    panic!("invalid request must not register a timer")
                }),
            ))
            .with_proxy_marker(false);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let router =
                RouterService::builder().get("/client", move |ctx: RequestContext| async move {
                    let client = ctx.http_client().expect("selected HTTP client");
                    assert_eq!(client.proxy_marker_enabled(), !state);
                    let error = client
                        .send(
                            OutboundRequest::get("https://example.com/")?
                                .body(Body::from("invalid GET body")),
                        )
                        .await
                        .expect_err("preflight rejection");
                    assert!(matches!(error, EdgeError::BadRequest { .. }));
                    assert_eq!(error.message(), "GET/HEAD request must not carry a body");
                    Ok::<_, EdgeError>("selected")
                });
            let configured = if state {
                router.with_state(supplied)
            } else {
                router
            };
            let app = App::new(configured.build());
            let (env, context) = env_context();
            let mut response = CloudflareService::new(&app)
                .dispatch(native_request(Method::Get, "/client", None), env, context)
                .await
                .expect("native dispatch");
            assert_eq!(response.status_code(), StatusCode::OK.as_u16());
            assert_eq!(response.bytes().await.expect("response body"), b"selected");
            assert_eq!(calls.load(Ordering::SeqCst) > 0, state);
        }
    }

    #[wasm_bindgen_test]
    async fn standard_service_installs_the_exact_application_outbound_clock() {
        async fn elapsed(ctx: RequestContext) -> Result<Response, EdgeError> {
            let client = ctx
                .http_client()
                .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("missing HTTP client")))?;
            let request =
                OutboundRequest::get("https://example.com/")?.body(Body::from("invalid GET body"));
            let results = client
                .send_all_until(vec![request], Deadline::after(Duration::from_secs(1)))
                .await
                .map_err(|failure| failure.into_parts().0)?;
            let observed_slot = results
                .slots()
                .first()
                .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("missing outbound result")))?;
            let edgezero_core::OutboundCollectedSlot::Resolved(result) = observed_slot else {
                return Err(EdgeError::internal(anyhow::anyhow!(
                    "unresolved outbound result"
                )));
            };
            if !matches!(&result.outcome, Err(EdgeError::BadRequest { .. })) {
                return Err(EdgeError::internal(anyhow::anyhow!(
                    "clock probe did not fail during batch preflight"
                )));
            }
            response_builder()
                .status(StatusCode::NO_CONTENT)
                .header(
                    "x-edgezero-elapsed-ms",
                    result.elapsed.as_millis().to_string(),
                )
                .body(Body::empty())
                .map_err(EdgeError::internal)
        }

        let start = MonotonicInstant::now();
        let completed = start
            .checked_add(Duration::from_millis(7))
            .expect("completed instant");
        let observations = Arc::new(AtomicUsize::new(0));
        let clock_observations = Arc::clone(&observations);
        let mut app = App::new(RouterService::builder().get("/clock", elapsed).build());
        app.set_monotonic_clock(MonotonicClock::new(move || {
            if clock_observations.fetch_add(1, Ordering::SeqCst) < 2 {
                start
            } else {
                completed
            }
        }));
        let init = worker::RequestInit::new();
        let request = CfRequest::new_with_init("https://example.com/clock", &init)
            .expect("Cloudflare request");
        let env = Object::new().unchecked_into::<Env>();
        let js_context = Object::new().unchecked_into::<WorkerSysContext>();

        let response = CloudflareService::new(&app)
            .dispatch(request, env, Context::new(js_context))
            .await
            .expect("Cloudflare response");

        assert_eq!(response.status_code(), StatusCode::NO_CONTENT.as_u16());
        assert_eq!(
            response
                .headers()
                .get("x-edgezero-elapsed-ms")
                .expect("elapsed header"),
            Some("7".to_owned())
        );
        assert!(observations.load(Ordering::SeqCst) >= 3);
    }
}

#[cfg(test)]
mod synthesis_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use edgezero_core::config_store::{
        BoundedStoreRead, ConfigStore, ConfigStoreError, ConfigStoreHandle,
        finish_bounded_config_read,
    };
    use edgezero_core::key_value_store::{KvStore, NoopKvStore};
    use edgezero_core::secret_store::{NoopSecretStore, SecretHandle};

    use super::*;

    struct StubConfig;
    #[async_trait::async_trait(?Send)]
    impl ConfigStore for StubConfig {
        async fn get(
            &self,
            _key: &str,
        ) -> Result<Option<edgezero_core::ConfigValue>, ConfigStoreError> {
            Ok(None)
        }

        async fn get_bounded(
            &self,
            key: &str,
            clock: &MonotonicClock,
            deadline: Deadline,
            max_backend_bytes: u64,
            max_value_bytes: u64,
        ) -> Result<BoundedStoreRead<edgezero_core::ConfigValue>, ConfigStoreError> {
            if deadline.is_expired_at(clock.now()) {
                return Err(ConfigStoreError::DeadlineExceeded);
            }
            finish_bounded_config_read(
                self.get(key).await,
                clock,
                deadline,
                max_backend_bytes,
                max_value_bytes,
            )
        }
    }

    fn config_handle() -> ConfigStoreHandle {
        ConfigStoreHandle::new(Arc::new(StubConfig))
    }

    fn kv_handle() -> KvHandle {
        let store: Arc<dyn KvStore> = Arc::new(NoopKvStore);
        KvHandle::new(store)
    }

    fn secret_handle() -> SecretHandle {
        SecretHandle::new(Arc::new(NoopSecretStore))
    }

    #[test]
    fn synthesis_handles_config_and_secret_bare_handles_symmetrically() {
        let stores = Stores {
            config_store: Some(config_handle()),
            secrets: Some(secret_handle()),
            ..Default::default()
        };
        let (config, _, secret) = synthesise_store_registries(stores);
        assert_eq!(config.expect("config").default_id(), "default");
        let secret_registry = secret.expect("secret");
        assert_eq!(secret_registry.default_id(), "default");
        // BoundSecretStore binds the synthesised secret to platform
        // store name "default". A handler reading via
        // `ctx.secret_store_default()?.require_str(key)` resolves
        // the cloudflare Worker Secret literally named "default";
        // if the operator's wrangler.toml uses a different name,
        // the runtime require_str() surfaces a clear store-name
        // error rather than a silent miss.
        assert_eq!(
            secret_registry.default().expect("bound").store_name(),
            "default"
        );
    }

    #[test]
    fn synthesis_registry_wins_over_bare_handle_when_both_wired() {
        let mut by_id: BTreeMap<String, KvHandle> = BTreeMap::new();
        by_id.insert("sessions".to_owned(), kv_handle());
        let registry = KvRegistry::new(by_id, "sessions".to_owned());
        let stores = Stores {
            kv: Some(kv_handle()),
            kv_registry: Some(registry),
            ..Default::default()
        };
        let (_, kv, _) = synthesise_store_registries(stores);
        let kv_registry = kv.expect("registry survives");
        assert_eq!(kv_registry.default_id(), "sessions");
        assert!(
            kv_registry.named("default").is_none(),
            "bare handle's `default` synth NOT merged in"
        );
    }

    #[test]
    fn synthesis_returns_none_for_each_kind_with_no_wiring() {
        let (config, kv, secret) = synthesise_store_registries(Stores::default());
        assert!(config.is_none() && kv.is_none() && secret.is_none());
    }

    #[test]
    fn synthesis_wraps_bare_kv_handle_under_default_when_no_registry() {
        let stores = Stores {
            kv: Some(kv_handle()),
            ..Default::default()
        };
        let (config, kv, secret) = synthesise_store_registries(stores);
        assert!(config.is_none());
        assert!(secret.is_none());
        let kv_registry = kv.expect("kv registry synthesised");
        assert_eq!(kv_registry.default_id(), "default");
        assert!(kv_registry.named("other").is_none());
    }

    /// Spec 12.7 / plan line 1526: `EDGEZERO__STORES__CONFIG__<ID>__KEY`
    /// must surface as `ConfigStoreBinding.default_key`.
    ///
    /// `build_config_registry` requires a live `worker::Env` (Cloudflare
    /// runtime type) so cannot be unit-tested here; this test exercises the
    /// env-resolution layer that `build_config_registry` reads from.
    /// Platform-integration coverage relies on the E2 smoke scripts.
    #[test]
    fn config_default_key_env_override_resolved() {
        let env = EnvConfig::from_vars([(
            "EDGEZERO__STORES__CONFIG__APP_CONFIG__KEY",
            "app_config_staging",
        )]);
        assert_eq!(
            env.store_key("config", "app_config"),
            "app_config_staging",
            "env override must propagate to the key resolved by build_config_registry"
        );
    }
}

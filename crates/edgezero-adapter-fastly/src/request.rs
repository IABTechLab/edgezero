use std::collections::{HashSet, VecDeque};
use std::fmt::Display;
#[cfg(all(feature = "test-utils", target_arch = "wasm32"))]
use std::io::Cursor;
use std::io::Read;
use std::str::from_utf8;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::task::Poll;

use bytes::Bytes;
use edgezero_core::app::{App, StoreMetadata, StoresMetadata};
use edgezero_core::body::Body;
use edgezero_core::config_store::{ConfigStoreHandle, ConfigStoreOpenFailure};
use edgezero_core::error::EdgeError;
use edgezero_core::http::header::HOST;
use edgezero_core::http::{
    Extensions, HeaderValue, Method, Request, Response, Uri, request_builder,
};
use edgezero_core::ingress::{
    CapturedTarget, HeaderFidelity, InboundOrigin, IngressBeginOutcome, IngressDispatchOutcome,
    IngressFraming, IngressHeadAccounting, IngressHeadParts, OriginSource, PreparedIngress,
    RequestIngress, TargetUnavailable,
};
use edgezero_core::key_value_store::{KvError, KvHandle, KvStoreOpenFailure};
use edgezero_core::outbound::HttpClient;
use edgezero_core::response_egress::ResponseEgressEnvelope;
use edgezero_core::secret_store::SecretHandle;
use edgezero_core::store_registry::{
    BoundSecretStore, ConfigRegistry, ConfigStoreBinding, KvRegistry, SecretRegistry, StoreRegistry,
};
use edgezero_core::time::{Deadline, MonotonicClock, MonotonicInstant};
use fastly::{Error as FastlyError, Request as FastlyRequest};
use futures::executor;
use futures_util::stream;
use std::collections::BTreeMap;

use crate::config_store::FastlyConfigStore;
use crate::context::{FastlyOutboundObservation, FastlyRequestContext};
use crate::delivery::{
    DispatchOutcome, FastlyNoAttempt, FastlyNoAttemptReason, FastlyRequestDelivery,
    FastlyRequestHeadFailure, FastlyResponseDelivery, finish_request,
};
use crate::init_fastly_abi;
use crate::key_value_store::FastlyKvStore;
use crate::lifecycle::with_callback;
use crate::outbound::drive_callback;
use crate::outbound::{FastlyOutboundClient, native_time};
use crate::response::{parse_uri, send_egress_response};
use crate::secret_store::FastlySecretStore;

const WARNED_STORE_CACHE_LIMIT: usize = 64;

#[derive(Default)]
struct RecentStringSet {
    keys: HashSet<String>,
    order: VecDeque<String>,
}

impl RecentStringSet {
    fn insert(&mut self, key: &str, limit: usize) -> bool {
        let owned = key.to_owned();
        if !self.keys.insert(owned.clone()) {
            return false;
        }
        self.order.push_back(owned);
        while limit > 0 && self.order.len() > limit {
            if let Some(oldest) = self.order.pop_front() {
                self.keys.remove(&oldest);
            }
        }
        true
    }
}

/// Groups the optional per-request store handles injected at dispatch time.
///
/// Use `..Default::default()` for fields you do not need:
///
/// ```rust,ignore
/// let stores = Stores { kv: Some(kv_handle), ..Default::default() };
/// ```
#[derive(Default)]
struct Stores {
    config_registry: Option<ConfigRegistry>,
    config_store: Option<ConfigStoreHandle>,
    kv: Option<KvHandle>,
    kv_registry: Option<KvRegistry>,
    secret_registry: Option<SecretRegistry>,
    secrets: Option<SecretHandle>,
}

enum ConfigSource {
    Handle(ConfigStoreHandle),
    Name(String),
    None,
}

/// Fastly per-request dispatch service.
///
/// Builds a router invocation with the stores the operator wants
/// injected into request extensions, then dispatches one request
/// against the wrapped `App`. The store wiring is a per-Service
/// decision; on Fastly Compute that means per-request (the worker
/// model invokes the entrypoint per HTTP request), but the Service
/// type itself is cheap to build.
///
/// Replaces the prior `dispatch_with_*` variant fan-out. Each
/// builder method is independent: enable any combination of KV,
/// config, and secret stores by chaining the relevant `with_*` /
/// `require_*` calls. The manifest-driven `run_app` is still the
/// recommended entrypoint for normal flows -- the Service builder
/// is for manual / no-manifest deployments.
///
/// ```rust,ignore
/// FastlyService::new(&app)
///     .with_kv("sessions").require_kv()
///     .with_config("app_config")
///     .with_secrets()
///     .send()
/// ```
pub struct FastlyService<'app> {
    app: &'app App,
    config: ConfigSource,
    kv: Option<KvSource>,
    secrets: SecretSource,
}

struct KvSource {
    name: String,
    required: bool,
}

struct HeadConversionFailure {
    cause: FastlyRequestHeadFailure,
    error: EdgeError,
}

enum SecretSource {
    Off,
    On { required: bool },
}

impl DispatchOutcome {
    #[cfg(any(test, feature = "test-utils"))]
    fn into_envelope(self) -> Result<ResponseEgressEnvelope, FastlyError> {
        match self {
            Self::Detached(envelope) | Self::Routed(envelope) => Ok(envelope),
            Self::Aborted => Err(FastlyError::msg("ingress admission aborted")),
        }
    }
}

impl<'app> FastlyService<'app> {
    /// Test-only request-in/egress-envelope dispatch seam.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub fn capture_egress_for_test(
        self,
        req: FastlyRequest,
    ) -> Result<ResponseEgressEnvelope, FastlyError> {
        self.dispatch_with(req, |_req, _extensions| {})
            .map_err(|reason| FastlyError::msg(reason.message()))
            .and_then(DispatchOutcome::into_envelope)
    }

    fn dispatch_with<Prepare>(
        self,
        req: FastlyRequest,
        prepare: Prepare,
    ) -> Result<DispatchOutcome, FastlyNoAttempt>
    where
        Prepare: FnOnce(&mut FastlyRequest, &mut Extensions),
    {
        let request_start = self.app.monotonic_now();
        let config_store = match self.config {
            ConfigSource::Handle(handle) => Some(handle),
            ConfigSource::Name(name) => Some(open_config_handle(&name)),
            ConfigSource::None => None,
        };
        let kv = match self.kv {
            Some(source) => resolve_kv_handle(&source.name, source.required)?,
            None => None,
        };
        let secrets = match self.secrets {
            SecretSource::Off => None,
            SecretSource::On { required } => Some(resolve_secret_handle(required)),
        };
        dispatch_with_handles(
            self.app,
            req,
            Stores {
                config_store,
                kv,
                secrets,
                ..Default::default()
            },
            request_start,
            prepare,
        )
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

    /// Promote the previously-wired KV store to required: an
    /// unavailable store causes dispatch to return an error
    /// instead of silently degrading. No-op when `with_kv` wasn't
    /// called.
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

    fn run_request_with_delivery<State, Prepare, Finalize, Deliver>(
        self,
        req: FastlyRequest,
        prepare: Prepare,
        finalize: Finalize,
        deliver: Deliver,
    ) -> FastlyRequestDelivery<State>
    where
        Prepare: FnOnce(&mut FastlyRequest, &mut Extensions),
        Finalize: FnOnce(&mut Response) -> State,
        Deliver: FnOnce(ResponseEgressEnvelope) -> FastlyResponseDelivery,
    {
        with_callback(|| {
            init_fastly_abi();
            let outcome = self.dispatch_with(req, prepare);
            finish_request(outcome, finalize, deliver)
        })
    }

    /// Exercises the closed request/response lifecycle with an injected delivery boundary.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    #[inline]
    pub fn run_request_with_hooks_for_test<State, Prepare, Finalize, Deliver>(
        self,
        req: FastlyRequest,
        prepare: Prepare,
        finalize: Finalize,
        deliver: Deliver,
    ) -> FastlyRequestDelivery<State>
    where
        Prepare: FnOnce(&mut FastlyRequest, &mut Extensions),
        Finalize: FnOnce(&mut Response) -> State,
        Deliver: FnOnce(ResponseEgressEnvelope) -> FastlyResponseDelivery,
    {
        self.run_request_with_delivery(req, prepare, finalize, deliver)
    }

    /// Resolve every wired store, receive the client request, and transmit its response.
    ///
    /// # Errors
    /// Returns an error if a required store cannot be opened or
    /// the underlying handler returns an error.
    #[inline]
    pub fn send(self) -> Result<(), FastlyError> {
        with_callback(|| {
            init_fastly_abi();
            let req = FastlyRequest::from_client();
            self.send_request_with_hooks(req, |_req, _extensions| {}, |_response| ())
                .into_standard_result()
                .map(|_state| ())
        })
    }

    /// Runs an already-received request through the closed `EdgeZero` lifecycle.
    ///
    /// `prepare` may mutate the borrowed raw request and add portable extensions before conversion and
    /// ingress validation. Insert a validated `SynthesizedResponseHeaders` to replace the app's
    /// generated-error/fallback headers for this request, including an explicitly empty set.
    /// `finalize` may mutate only a routed response before egress policy/framing. The
    /// adapter retains response ownership and returns its disposition, last-body commitment and
    /// exact terminal report. Routed hook state survives failed delivery and fallback. Detached
    /// responses skip `finalize`. Inspect replacement permission before sending a native response.
    #[inline]
    pub fn send_request_with_hooks<State, Prepare, Finalize>(
        self,
        req: FastlyRequest,
        prepare: Prepare,
        finalize: Finalize,
    ) -> FastlyRequestDelivery<State>
    where
        Prepare: FnOnce(&mut FastlyRequest, &mut Extensions),
        Finalize: FnOnce(&mut Response) -> State,
    {
        self.run_request_with_delivery(req, prepare, finalize, send_egress_response)
    }

    /// Open the Fastly Config Store named `name` and inject its
    /// handle into request extensions. If the store is unavailable
    /// at request time, the dispatcher logs the warning once and
    /// proceeds without it.
    ///
    /// This bare-handle path binds the config registry's default key to
    /// `"default"`. Manifest-driven [`run_app`](crate::run_app) instead opens
    /// the logical resource-link alias and uses its deterministic target key.
    #[must_use]
    #[inline]
    pub fn with_config<S>(mut self, name: S) -> Self
    where
        S: Into<String>,
    {
        self.config = ConfigSource::Name(name.into());
        self
    }

    /// Inject a pre-built `ConfigStoreHandle`. Use this when the
    /// caller has already opened (or mocked) the backend. Mutually
    /// exclusive with `with_config(name)` -- the last call wins.
    /// Like [`Self::with_config`], this binds the config registry's default key
    /// to `"default"`. Manifest-driven [`run_app`](crate::run_app) derives its
    /// binding from baked store metadata instead.
    #[must_use]
    #[inline]
    pub fn with_config_handle(mut self, handle: ConfigStoreHandle) -> Self {
        self.config = ConfigSource::Handle(handle);
        self
    }

    /// Open a Fastly KV Store by `name` and inject its handle.
    /// Non-required by default: an absent store logs once and
    /// dispatch continues. Pair with `require_kv()` to explicitly fail
    /// before dispatch. Canonical metadata-driven declarations instead
    /// open lazily and return typed failures only when used.
    #[must_use]
    #[inline]
    pub fn with_kv<S>(mut self, name: S) -> Self
    where
        S: Into<String>,
    {
        self.kv = Some(KvSource {
            name: name.into(),
            required: false,
        });
        self
    }

    /// Enable the Fastly Secret Store and inject its handle.
    /// Non-required by default: an absent store leaves no secret
    /// handle in extensions and dispatch continues. Pair with
    /// `require_secrets()` when the manifest declares
    /// `[stores.secrets]`.
    ///
    /// Platform-name binding: the synthesised `SecretRegistry`
    /// binds the handle to platform store name `"default"`.
    /// Handlers reading `ctx.secret_store_default()?.require_str(key)`
    /// open a Fastly Secret Store literally named `"default"`. Use
    /// the manifest-aware `run_app` if your account uses a
    /// different store name -- it opens each `[stores.secrets]` id
    /// baked into the app as a resource-link alias, and a managed
    /// deploy links the selected physical store under that alias.
    #[must_use]
    #[inline]
    pub fn with_secrets(mut self) -> Self {
        self.secrets = SecretSource::On { required: false };
        self
    }
}

fn detached_error_outcome(
    app: &App,
    error: EdgeError,
    request_method: Method,
    request_start: MonotonicInstant,
) -> DispatchOutcome {
    match app.detached_ingress_error_egress(error, request_method, request_start) {
        IngressDispatchOutcome::Response(envelope) => DispatchOutcome::Detached(*envelope),
        IngressDispatchOutcome::Aborted | _ => DispatchOutcome::Aborted,
    }
}

fn dispatch_core_request(
    app: &App,
    mut core_request: Request,
    stores: Stores,
    prepared: PreparedIngress,
) -> DispatchOutcome {
    // Hard-cutoff: legacy bare handles are no longer
    // inserted into request extensions. `with_config_handle`
    // still accepts a `ConfigStoreHandle`, but the dispatcher
    // synthesises a one-id `<kind>Registry` from any wired handle
    // and only the registry goes into extensions. The
    // `ctx.{config,kv,secret}_handle()` accessors are gone; handlers
    // use `ctx.{config,kv,secret}_store_default()` or the
    // `Kv` / `Config` / `Secrets` extractors.
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
    let clock = prepared.monotonic_clock();
    let entered = prepared.request_start();
    let method = prepared.request_method().clone();
    match executor::block_on(drive_callback(
        Box::pin(app.dispatch_admitted(prepared, core_request)),
        clock,
        entered,
    )) {
        Ok(response) => DispatchOutcome::Routed(response),
        Err(error) => detached_error_outcome(app, error, method, entered),
    }
}

/// Run an app-provided closure against the borrowed raw request and a scratch extension bag before
/// conversion consumes the request.
fn apply_request_prepare<Prepare>(req: &mut FastlyRequest, prepare: Prepare) -> Extensions
where
    Prepare: FnOnce(&mut FastlyRequest, &mut Extensions),
{
    let mut scratch = Extensions::default();
    if let Some(observation) = FastlyOutboundObservation::current() {
        scratch.insert(observation);
    }
    prepare(req, &mut scratch);
    // Preparation may supply a client, but cannot substitute another callback's accounting.
    scratch.remove::<FastlyOutboundObservation>();
    if let Some(observation) = FastlyOutboundObservation::current() {
        scratch.insert(observation);
    }
    scratch
}

fn dispatch_with_handles<Prepare>(
    app: &App,
    mut req: FastlyRequest,
    stores: Stores,
    request_start: MonotonicInstant,
    prepare: Prepare,
) -> Result<DispatchOutcome, FastlyNoAttempt>
where
    Prepare: FnOnce(&mut FastlyRequest, &mut Extensions),
{
    let ingress = capture_request_ingress(&req);
    let mut scratch = apply_request_prepare(&mut req, prepare);
    scratch.remove::<RequestIngress>();
    let synthesized_headers = scratch.remove::<edgezero_core::SynthesizedResponseHeaders>();
    let mut head_request =
        into_core_request_head(&req, app.monotonic_clock(), ingress).map_err(|failure| {
            FastlyNoAttempt::new(
                FastlyNoAttemptReason::RequestHeadConversion(failure.cause),
                map_edge_error(&failure.error),
            )
        })?;
    head_request.extensions_mut().extend(scratch);
    let outcome = dispatch_ingress_reader(app, head_request, stores, request_start, move || {
        Ok(req.try_take_body())
    });
    Ok(match (outcome, synthesized_headers) {
        (DispatchOutcome::Detached(envelope), Some(headers)) => {
            DispatchOutcome::Detached(envelope.with_synthesized_response_headers(headers))
        }
        (DispatchOutcome::Routed(envelope), Some(headers)) => {
            DispatchOutcome::Routed(envelope.with_synthesized_response_headers(headers))
        }
        (unchanged, _) => unchanged,
    })
}

fn dispatch_ingress_reader<Source, MakeSource>(
    app: &App,
    mut head_request: Request,
    stores: Stores,
    request_start: MonotonicInstant,
    make_source: MakeSource,
) -> DispatchOutcome
where
    Source: Read + 'static,
    MakeSource: FnOnce() -> Result<Option<Source>, EdgeError>,
{
    let request_method = head_request.method().clone();
    let head_parts = IngressHeadParts::from_request(
        &head_request,
        IngressHeadAccounting::HostManaged,
        IngressFraming::HostManaged,
    );
    if let Err(error) = head_parts.validate_normalized(app.ingress_head_limits()) {
        return detached_error_outcome(app, error, request_method, request_start);
    }
    let prepared = match app.begin_ingress(head_parts, request_start) {
        Ok(outcome) => match outcome {
            IngressBeginOutcome::Admitted(prepared) => prepared,
            IngressBeginOutcome::Refused(response) => {
                return DispatchOutcome::Detached(response);
            }
            IngressBeginOutcome::Aborted => return DispatchOutcome::Aborted,
            _ => {
                return detached_error_outcome(
                    app,
                    EdgeError::internal(anyhow::anyhow!("unsupported ingress admission outcome")),
                    request_method,
                    request_start,
                );
            }
        },
        Err(error) => {
            return detached_error_outcome(app, error, request_method, request_start);
        }
    };
    let source = match make_source() {
        Ok(source) => source,
        Err(error) => {
            return DispatchOutcome::Detached(app.admitted_error_egress(prepared, error));
        }
    };
    *head_request.body_mut() = fastly_deadline_body(
        source,
        Some((prepared.read_deadline(), prepared.monotonic_clock())),
    );
    dispatch_core_request(app, head_request, stores, prepared)
}

/// Dispatch with per-id store registries built from baked metadata and closed lifecycle hooks.
///
/// Fastly is `Multi` for all three kinds. Each declared ID is the stable
/// resource-link alias opened by the runtime. Config always uses the logical ID
/// as its entry key. A custom entry point gets full parity with `run_app` by
/// passing the baked store metadata:
///
/// ```rust,ignore
/// let stores = MyHooks::stores();
/// send_request_with_registries_and_hooks(&app, stores, req, |_req, _extensions| {}, |_response| ())
/// ```
///
/// [`FastlyService`]'s bare-handle path binds `default_key: "default"` instead
/// of the logical ID.
///
/// Declared KV bindings open on first use, at most once per request, without blocking
/// preparation or routes that ignore KV. Missing/invalid bindings fail at use with 500;
/// transient open failures fail with 503. Explicit `FastlyService::require_kv()` still
/// fails before dispatch. Failed declared config bindings remain typed failing handles, and secret
/// bindings open their logical alias when extraction requests the value.
///
/// Reports no-attempt, policy abort, detached or routed delivery. Declared KV opening errors are
/// handled through the application's normal error response path when the store is used.
#[inline]
pub fn send_with_registries_and_hooks<State, Prepare, Finalize>(
    app: &App,
    stores: StoresMetadata,
    prepare: Prepare,
    finalize: Finalize,
) -> FastlyRequestDelivery<State>
where
    Prepare: FnOnce(&mut FastlyRequest, &mut Extensions),
    Finalize: FnOnce(&mut Response) -> State,
{
    with_callback(|| {
        init_fastly_abi();
        let req = FastlyRequest::from_client();
        send_request_with_registries_and_hooks(app, stores, req, prepare, finalize)
    })
}

/// Deliver an already-received request through canonical admission and owned egress.
///
/// Use this inside a retained serving loop; it never receives another client request.
/// Request registries, preparation, finalization, and body handles remain request-local.
///
/// Returns an owned disposition and delivery report without sending a second response.
/// Routed finalization state is retained even when delivery fails or a fallback is delivered.
#[inline]
pub fn send_request_with_registries_and_hooks<State, Prepare, Finalize>(
    app: &App,
    stores: StoresMetadata,
    req: FastlyRequest,
    prepare: Prepare,
    finalize: Finalize,
) -> FastlyRequestDelivery<State>
where
    Prepare: FnOnce(&mut FastlyRequest, &mut Extensions),
    Finalize: FnOnce(&mut Response) -> State,
{
    with_callback(|| {
        init_fastly_abi();
        let request_start = app.monotonic_now();
        let kv_registry = build_kv_registry(stores.kv);
        let config_registry = build_config_registry(stores.config);
        let secret_registry = build_secret_registry(stores.secrets);
        let outcome = dispatch_with_handles(
            app,
            req,
            Stores {
                config_registry,
                kv_registry,
                secret_registry,
                ..Default::default()
            },
            request_start,
            prepare,
        );
        finish_request(outcome, finalize, send_egress_response)
    })
}

/// Delivers an assembly failure for an already-received request without an application.
///
/// Drops the unread request and uses the default redacted renderer and finite write deadline.
/// No preparation, finalization, store opening or app observer runs. The returned detached
/// delivery owns the terminal report. Inspect commitment before an explicit native replacement;
/// this helper never resends, and unknown or committed delivery never permits replacement.
#[inline]
pub fn send_assembly_error(
    req: FastlyRequest,
    error: EdgeError,
    request_start: MonotonicInstant,
    clock: MonotonicClock,
) -> FastlyRequestDelivery<()> {
    init_fastly_abi();
    let method = req.get_method().clone();
    drop(req);
    let envelope = App::assembly_error_egress(error, method, request_start, clock);
    FastlyRequestDelivery::Detached(send_egress_response(envelope))
}

/// Pure synthesis: collapse a `Stores` (which may carry both a
/// wired multi-id registry AND a legacy bare handle) into the
/// three registries that go into request extensions. Precedence
/// is "registry wins": a wired registry is taken verbatim; only
/// in its absence is a bare handle wrapped into a one-id registry
/// keyed under `"default"`. The bare handle is never merged
/// in, never used as a fallback for ids the registry doesn't
/// define. Pulled out as a pure function so the precedence
/// contract is unit-testable without spinning up a real
/// `Request` and async dispatcher.
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

fn build_kv_registry(kv_meta: Option<StoreMetadata>) -> Option<KvRegistry> {
    let meta = kv_meta?;
    let mut by_id: BTreeMap<String, KvHandle> = BTreeMap::new();
    for id in meta.ids {
        by_id.insert(
            (*id).to_owned(),
            KvHandle::new(Arc::new(FastlyKvStore::declared(id))),
        );
    }
    let default_id = meta.default.to_owned();
    StoreRegistry::from_parts(by_id, default_id)
}

fn build_config_registry(config_meta: Option<StoreMetadata>) -> Option<ConfigRegistry> {
    let meta = config_meta?;
    let mut by_id: BTreeMap<String, ConfigStoreBinding> = BTreeMap::new();
    for id in meta.ids {
        by_id.insert(
            (*id).to_owned(),
            ConfigStoreBinding {
                handle: open_config_handle(id),
                default_key: (*id).to_owned(),
            },
        );
    }
    StoreRegistry::from_parts(by_id, meta.default.to_owned())
}

fn open_config_handle(store_name: &str) -> ConfigStoreHandle {
    match FastlyConfigStore::try_open(store_name) {
        Ok(store) => ConfigStoreHandle::new(Arc::new(store)),
        Err(err) => {
            warn_missing_store_once(store_name, &err.to_string());
            ConfigStoreHandle::failed_open(ConfigStoreOpenFailure::InvalidConfiguration)
        }
    }
}

fn build_secret_registry(secret_meta: Option<StoreMetadata>) -> Option<SecretRegistry> {
    let meta = secret_meta?;
    // Fastly is `Multi` for secrets. The provider trait is stateless —
    // `FastlySecretStore::get_bytes(store_name, key)` opens the named Fastly
    // Secret Store per call — so we share one provider handle across all
    // bindings, then capture the logical resource-link alias in the bound
    // wrapper.
    let handle = SecretHandle::new(Arc::new(FastlySecretStore));
    let mut by_id: BTreeMap<String, BoundSecretStore> = BTreeMap::new();
    for id in meta.ids {
        by_id.insert(
            (*id).to_owned(),
            BoundSecretStore::new(handle.clone(), (*id).to_owned()),
        );
    }
    // Fastly's secret-store handle wrappers are infallible to construct;
    // `from_parts` keeps the API symmetric with the KV / config builders.
    StoreRegistry::from_parts(by_id, meta.default.to_owned())
}

/// Capture available ingress facts without consuming the native request.
///
/// Fastly SDK 0.13.1 does not expose a safe borrowed raw target accessor.
/// Record target unavailability before reading the SDK URL for the canonical
/// origin. That URL may normalize the path and is never used as an original
/// target. Client requests require exactly one validated Host matching the
/// runtime URL authority. No forwarded headers or missing TLS facts supply a
/// scheme. This function does not clone handles or change client provenance.
///
/// Original header names/count APIs expose partial ordering and aggregate count,
/// but do not associate original fields with converted values. They cannot prove
/// per-name multiplicity or full global order. Fidelity remains unknown pending
/// production evidence; aggregate count equality is not proof of preservation.
/// Unlike the other adapters, Fastly exposes partial original-header metadata,
/// so unresolved native capability is intentionally `Unknown` rather than
/// `Unavailable`. This extension does not supply original global order to hooks.
/// Consumers requiring it must accept only `Preserved`; both `Unknown` and
/// `Unavailable` are insufficient.
///
/// # Panics
/// The SDK URL accessor can panic if its runtime URI cannot be parsed.
#[inline]
#[must_use]
pub fn capture_request_ingress(req: &FastlyRequest) -> RequestIngress {
    let target = CapturedTarget::Unavailable(TargetUnavailable::NotExposed);
    let origin = if req.is_from_client() {
        parse_uri(req.get_url_str()).ok().and_then(|uri| {
            origin_from_runtime_uri(&uri, req.get_header_all(HOST).map(HeaderValue::as_bytes))
        })
    } else {
        None
    };
    RequestIngress::new(target, origin, HeaderFidelity::default())
}

fn origin_from_runtime_uri<'header>(
    uri: &Uri,
    mut hosts: impl Iterator<Item = &'header [u8]>,
) -> Option<InboundOrigin> {
    let host = from_utf8(hosts.next()?).ok()?;
    if hosts.next().is_some() {
        return None;
    }
    let scheme = uri.scheme_str()?;
    let runtime_origin =
        InboundOrigin::parse(scheme, uri.authority()?.as_str(), OriginSource::RuntimeUri).ok()?;
    let origin = InboundOrigin::parse(scheme, host, OriginSource::RuntimeUri).ok()?;
    runtime_origin
        .same_authority(&origin)
        .then_some(runtime_origin)
}

/// Converts a request without eagerly reading its body.
///
/// # Errors
/// Returns a typed error for invalid request-head conversion.
#[inline]
pub fn into_core_request(mut req: FastlyRequest) -> Result<Request, EdgeError> {
    let ingress = capture_request_ingress(&req);
    let request = into_core_request_head(&req, MonotonicClock::default(), ingress)
        .map_err(|failure| failure.error)?;
    Ok(attach_core_body(&mut req, request, None))
}

fn into_core_request_head(
    req: &FastlyRequest,
    outbound_clock: MonotonicClock,
    ingress: RequestIngress,
) -> Result<Request, HeadConversionFailure> {
    let method = req.get_method().clone();
    let uri = parse_uri(req.get_url_str()).map_err(|error| HeadConversionFailure {
        cause: FastlyRequestHeadFailure::InvalidUri,
        error,
    })?;

    let mut builder = request_builder().method(method).uri(uri);
    for (name, value) in req.get_headers() {
        builder = builder.header(name.as_str(), value.as_bytes());
    }

    let mut request = builder
        .body(Body::empty())
        .map_err(|error| HeadConversionFailure {
            cause: FastlyRequestHeadFailure::RequestBuilder,
            error: EdgeError::internal(error),
        })?;

    let context = FastlyRequestContext {
        client_ip: req.get_client_ip_addr(),
    };
    FastlyRequestContext::insert(&mut request, context);
    if let Some(observation) = FastlyOutboundObservation::current() {
        request.extensions_mut().insert(observation);
    }
    request
        .extensions_mut()
        .insert(outbound_client(outbound_clock));
    request.extensions_mut().insert(ingress);

    Ok(request)
}

fn outbound_client(clock: MonotonicClock) -> HttpClient {
    HttpClient::with_client(FastlyOutboundClient::with_time(native_time(clock)))
}

fn attach_core_body(
    req: &mut FastlyRequest,
    mut request: Request,
    read_lifetime: Option<(Deadline, MonotonicClock)>,
) -> Request {
    *request.body_mut() = fastly_deadline_body(req.try_take_body(), read_lifetime);
    request
}

fn fastly_deadline_body<Source>(
    source: Option<Source>,
    read_lifetime: Option<(Deadline, MonotonicClock)>,
) -> Body
where
    Source: Read + 'static,
{
    // SDK host length omits unread/read-ahead and pending-write buffers; only absence is enough.
    let Some(reader) = source else {
        return Body::empty();
    };
    let mut native_body = Some(reader);
    let mut terminal = false;
    let stream = stream::poll_fn(move |_cx| {
        if terminal {
            return Poll::Ready(None);
        }
        if read_lifetime
            .as_ref()
            .is_some_and(|(deadline, clock)| deadline.is_expired_at(clock.now()))
        {
            terminal = true;
            native_body.take();
            return Poll::Ready(Some(Err(EdgeError::request_timeout(
                "inbound body read deadline exceeded",
            ))));
        }

        let Some(body) = native_body.as_mut() else {
            terminal = true;
            return Poll::Ready(None);
        };
        let mut chunk = vec![0_u8; 16 * 1024];
        let result = body.read(&mut chunk);
        if read_lifetime
            .as_ref()
            .is_some_and(|(deadline, clock)| deadline.is_expired_at(clock.now()))
        {
            terminal = true;
            native_body.take();
            return Poll::Ready(Some(Err(EdgeError::request_timeout(
                "inbound body read deadline exceeded",
            ))));
        }
        match result {
            Ok(0) => {
                terminal = true;
                native_body.take();
                Poll::Ready(None)
            }
            Ok(read) => {
                chunk.truncate(read);
                Poll::Ready(Some(Ok(Bytes::from(chunk))))
            }
            Err(error) => {
                terminal = true;
                native_body.take();
                Poll::Ready(Some(Err(EdgeError::internal(error))))
            }
        }
    });
    Body::from_stream(stream)
}

/// Dispatches an observable reader through the production ingress body wrapper.
#[cfg(all(feature = "test-utils", target_arch = "wasm32"))]
#[doc(hidden)]
#[inline]
pub fn dispatch_ingress_reader_for_test<Source>(
    app: &App,
    method: Method,
    uri: Uri,
    source: Source,
) -> Result<ResponseEgressEnvelope, FastlyError>
where
    Source: Read + 'static,
{
    let request_start = app.monotonic_now();
    let mut core_request = request_builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .map_err(|error| map_edge_error(&EdgeError::internal(error)))?;
    core_request
        .extensions_mut()
        .insert(outbound_client(app.monotonic_clock()));
    dispatch_ingress_reader(
        app,
        core_request,
        Stores::default(),
        request_start,
        move || Ok(Some(source)),
    )
    .into_envelope()
}

/// Exercises admission ordering with observable reader construction and delivery closures.
#[cfg(all(feature = "test-utils", target_arch = "wasm32"))]
#[doc(hidden)]
#[inline]
pub fn dispatch_ingress_with_for_test<Source, MakeSource, Deliver>(
    app: &App,
    method: Method,
    uri: Uri,
    make_source: MakeSource,
    deliver: Deliver,
) -> Result<(), FastlyError>
where
    Source: Read + 'static,
    MakeSource: FnOnce() -> Result<Source, EdgeError>,
    Deliver: FnOnce(ResponseEgressEnvelope) -> Result<(), FastlyError>,
{
    let request_start = app.monotonic_now();
    let mut core_request = request_builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .map_err(|error| map_edge_error(&EdgeError::internal(error)))?;
    core_request
        .extensions_mut()
        .insert(outbound_client(app.monotonic_clock()));
    let outcome = dispatch_ingress_reader(
        app,
        core_request,
        Stores::default(),
        request_start,
        move || make_source().map(Some),
    );
    match outcome {
        DispatchOutcome::Aborted => Err(FastlyError::msg("ingress admission aborted")),
        DispatchOutcome::Detached(envelope) | DispatchOutcome::Routed(envelope) => {
            deliver(envelope)
        }
    }
}

/// Exercises a reader-construction failure after admission through the production seam.
#[cfg(all(feature = "test-utils", target_arch = "wasm32"))]
#[doc(hidden)]
#[inline]
pub fn dispatch_ingress_source_error_for_test(
    app: &App,
    method: Method,
    uri: Uri,
) -> Result<ResponseEgressEnvelope, FastlyError> {
    let request_start = app.monotonic_now();
    let mut core_request = request_builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .map_err(|error| map_edge_error(&EdgeError::internal(error)))?;
    core_request
        .extensions_mut()
        .insert(outbound_client(app.monotonic_clock()));
    dispatch_ingress_reader(
        app,
        core_request,
        Stores::default(),
        request_start,
        || -> Result<Option<Cursor<Vec<u8>>>, EdgeError> {
            Err(EdgeError::internal(anyhow::anyhow!(
                "fastly source construction failed"
            )))
        },
    )
    .into_envelope()
}

fn map_edge_error(err: &EdgeError) -> FastlyError {
    FastlyError::msg(err.to_string())
}

fn resolve_kv_handle(
    kv_store_name: &str,
    kv_required: bool,
) -> Result<Option<KvHandle>, FastlyNoAttempt> {
    match FastlyKvStore::open(kv_store_name) {
        Ok(store) => Ok(Some(KvHandle::new(Arc::new(store)))),
        Err(err) => {
            if kv_required {
                let reason = if let KvError::OpenFailed { reason } = &err {
                    *reason
                } else {
                    KvStoreOpenFailure::Unavailable
                };
                return Err(FastlyNoAttempt::new(
                    FastlyNoAttemptReason::RequiredStore(reason),
                    FastlyError::new(err),
                ));
            }
            warn_missing_kv_store_once(kv_store_name, &err);
            Ok(None)
        }
    }
}

/// Construct the Fastly secret-store handle. Called from the
/// `SecretSource::On { .. }` arm of `dispatch`. The `_required`
/// parameter is preserved (and ignored) for symmetry with the kv
/// path's `resolve_kv_handle(_, _required)`, where `required`
/// decides whether a runtime open failure is fatal or silently
/// degrades. `FastlySecretStore` is a unit struct whose
/// construction can't fail, so there's nothing for `required` to
/// gate here — and `clippy::unnecessary_wraps` would flag an
/// `Option<SecretHandle>` return on a function that never returns
/// None. The caller wraps the result in `Some(...)` so the
/// `SecretSource::Off => None` branch still produces the right
/// `Option`.
///
/// Pre-fix, the return type was `Option<SecretHandle>` and the
/// body short-circuited to `None` when `!_required`, which
/// silently swallowed `.with_secrets()` (which sets `required:
/// false`): handlers ran without a `SecretRegistry` even though the
/// builder claimed to inject one.
fn resolve_secret_handle(_required: bool) -> SecretHandle {
    SecretHandle::new(Arc::new(FastlySecretStore))
}

fn warn_missing_kv_store_once(kv_store_name: &str, error: &impl Display) {
    static WARNED_KV_STORES: OnceLock<Mutex<RecentStringSet>> = OnceLock::new();
    warn_missing_once(&WARNED_KV_STORES, "KV store", kv_store_name, error);
}

fn warn_missing_once(
    cache: &'static OnceLock<Mutex<RecentStringSet>>,
    item_type: &str,
    name: &str,
    detail: &impl Display,
) {
    let set = cache.get_or_init(|| Mutex::new(RecentStringSet::default()));
    let mut guard = set.lock().unwrap_or_else(PoisonError::into_inner);
    if guard.insert(name, WARNED_STORE_CACHE_LIMIT) {
        log::warn!("{item_type} '{name}' not available: {detail}");
    }
}

fn warn_missing_store_once(store_name: &str, detail: &str) {
    static WARNED_STORES: OnceLock<Mutex<RecentStringSet>> = OnceLock::new();
    warn_missing_once(
        &WARNED_STORES,
        "configured Fastly config store",
        store_name,
        &format!("{detail}; skipping config-store injection"),
    );
}

#[cfg(test)]
mod synthesis_tests {
    use super::*;
    use edgezero_core::config_store::{
        BoundedStoreRead, ConfigStore, ConfigStoreError, ConfigStoreHandle,
        finish_bounded_config_read,
    };
    use edgezero_core::context::RequestContext;
    use edgezero_core::key_value_store::{KvStore, NoopKvStore};
    use edgezero_core::response_egress_framing::ResponseEgressFramingMode;
    use edgezero_core::router::RouterService;
    use edgezero_core::secret_store::{NoopSecretStore, SecretHandle};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

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

    fn kv_handle() -> KvHandle {
        let store: Arc<dyn KvStore> = Arc::new(NoopKvStore);
        KvHandle::new(store)
    }

    fn config_handle() -> ConfigStoreHandle {
        ConfigStoreHandle::new(Arc::new(StubConfig))
    }

    fn secret_handle() -> SecretHandle {
        SecretHandle::new(Arc::new(NoopSecretStore))
    }

    #[test]
    fn declared_kv_registry_construction_never_opens_a_provider() {
        let registry = build_kv_registry(Some(StoreMetadata {
            default: "unlinked_f8_fixture",
            ids: &["unlinked_f8_fixture"],
        }))
        .expect("declared registry");
        assert!(registry.named("unlinked_f8_fixture").is_some());
        assert!(registry.default().is_some());
        assert!(build_kv_registry(None).is_none());
    }

    #[cfg(target_arch = "wasm32")]
    #[test]
    fn declared_kv_open_failure_retains_binding_and_recovers() {
        use edgezero_core::http::StatusCode;

        let metadata = StoreMetadata {
            default: "unlinked_f8_fixture",
            ids: &["unlinked_f8_fixture"],
        };
        let registry = build_kv_registry(Some(metadata)).expect("declared registry");
        assert!(registry.named("unlinked_f8_fixture").is_some());
        assert!(registry.named("undeclared").is_none());
        let handle = registry.default().expect("declared default binding");
        let first_error =
            executor::block_on(handle.get_bytes("marker")).expect_err("unlinked store");
        assert_eq!(
            EdgeError::from(first_error).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        for error in [
            executor::block_on(handle.exists("marker")).unwrap_err(),
            executor::block_on(handle.put_bytes("marker", Bytes::from_static(b"value")))
                .unwrap_err(),
            executor::block_on(handle.put_bytes_with_ttl(
                "marker",
                Bytes::from_static(b"value"),
                Duration::from_mins(1),
            ))
            .unwrap_err(),
            executor::block_on(handle.delete("marker")).unwrap_err(),
            executor::block_on(handle.list_keys_page("", None, 1)).unwrap_err(),
        ] {
            assert_eq!(
                EdgeError::from(error).status(),
                StatusCode::INTERNAL_SERVER_ERROR
            );
        }
    }

    #[cfg(target_arch = "wasm32")]
    #[test]
    fn explicitly_required_kv_failure_precedes_prepare_and_delivery() {
        let app = App::new(RouterService::builder().build());
        let prepared = AtomicUsize::new(0);
        let delivered = AtomicUsize::new(0);
        let result = FastlyService::new(&app)
            .with_kv("unlinked_f8_fixture")
            .require_kv()
            .run_request_with_hooks_for_test(
                FastlyRequest::get("https://example.com/"),
                |_, _| {
                    prepared.fetch_add(1, Ordering::SeqCst);
                },
                |_| (),
                |_| {
                    delivered.fetch_add(1, Ordering::SeqCst);
                    panic!("required store failure must not deliver")
                },
            );
        let FastlyRequestDelivery::NoAttempt(reason) = result else {
            panic!("explicit required store must fail before dispatch");
        };
        assert!(matches!(
            reason.reason(),
            FastlyNoAttemptReason::RequiredStore(_)
        ));
        assert!(!reason.message().contains("unlinked_f8_fixture"));
        assert_eq!(prepared.load(Ordering::SeqCst), 0);
        assert_eq!(delivered.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn apply_request_prepare_populates_scratch_from_raw_request() {
        use edgezero_core::http::Method;

        #[derive(Clone, Debug, PartialEq)]
        struct Ja4(String);

        let mut raw = FastlyRequest::new(Method::GET, "http://example.test/");
        let scratch = apply_request_prepare(&mut raw, |req, extensions| {
            // A real closure would call req.get_tls_ja4(); deriving from the URL
            // keeps the assertion deterministic under Viceroy.
            let marker = req.get_url_str().to_owned();
            extensions.insert(Ja4(marker));
        });

        assert_eq!(
            scratch.get::<Ja4>(),
            Some(&Ja4("http://example.test/".to_owned()))
        );
    }

    #[test]
    fn registration_observation_is_canonical_before_prepare_and_in_handler() {
        use crate::context::{FastlyOutboundObservation, observation};
        use edgezero_core::http::StatusCode;

        let foreign = with_callback(|| {
            observation::current()
                .unwrap()
                .configuration("foreign".into());
            FastlyOutboundObservation::current().unwrap()
        });
        let router = RouterService::builder()
            .get("/observation", |ctx: RequestContext| async move {
                let handle = ctx
                    .extensions()
                    .get::<FastlyOutboundObservation>()
                    .expect("canonical extension");
                assert_eq!(
                    handle.snapshot(),
                    FastlyOutboundObservation::current().unwrap().snapshot()
                );
                assert_eq!(handle.snapshot().registration_count(), 0);
                assert_eq!(handle.snapshot().configuration_count(), 0);
                observation::current()
                    .unwrap()
                    .configuration("handler".into());
                assert_eq!(
                    handle.snapshot().configuration_count(),
                    1,
                    "handler must share the live canonical record"
                );
                Ok::<_, EdgeError>("observed")
            })
            .build();
        let app = App::new(router);
        let outcome = with_callback(|| {
            FastlyService::new(&app).dispatch_with(
                FastlyRequest::get("http://example.test/observation"),
                |_, extensions| {
                    assert!(
                        extensions.get::<FastlyOutboundObservation>().is_some(),
                        "installed before preparation"
                    );
                    extensions.insert(foreign);
                },
            )
        })
        .expect("native dispatch");
        let envelope = outcome.into_envelope().expect("response envelope");
        let Ok((prepared, _, mut attempt, clock)) =
            envelope.begin(ResponseEgressFramingMode::HostAutomatic)
        else {
            panic!("begin egress");
        };
        assert!(attempt.begin_writing());
        assert!(attempt.complete(clock.now()));
        let response = prepared.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body().as_bytes(), Some(b"observed".as_slice()));
    }

    #[test]
    fn router_state_client_precedes_prepare_and_adapter_default() {
        use edgezero_core::http::StatusCode;
        use edgezero_core::outbound::{OutboundRequest, OutboundTime};

        for (state, prepare) in [(true, true), (false, true), (false, false)] {
            let start = MonotonicInstant::now();
            let state_calls = Arc::new(AtomicUsize::new(0));
            let prepare_calls = Arc::new(AtomicUsize::new(0));
            let supplied = |calls: &Arc<AtomicUsize>| {
                let observed_calls = Arc::clone(calls);
                let clock = MonotonicClock::new(move || {
                    observed_calls.fetch_add(1, Ordering::SeqCst);
                    start
                });
                HttpClient::with_client(FastlyOutboundClient::with_time(OutboundTime::new(
                    clock,
                    |_, _| panic!("invalid request must not register a timer"),
                )))
                .with_proxy_marker(false)
            };
            let state_client = supplied(&state_calls);
            let prepare_client = supplied(&prepare_calls);
            assert_eq!(state_calls.load(Ordering::SeqCst), 0);
            assert_eq!(prepare_calls.load(Ordering::SeqCst), 0);
            let router =
                RouterService::builder().get("/client", move |ctx: RequestContext| async move {
                    let client = ctx.http_client().expect("selected HTTP client");
                    assert_eq!(client.proxy_marker_enabled(), !state && !prepare);
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
                router.with_state(state_client)
            } else {
                router
            };
            let app = App::new(configured.build());
            let envelope = FastlyService::new(&app)
                .dispatch_with(
                    FastlyRequest::get("http://example.test/client"),
                    move |_, extensions| {
                        if prepare {
                            extensions.insert(prepare_client);
                        }
                    },
                )
                .expect("native dispatch")
                .into_envelope()
                .expect("response envelope");
            let Ok((prepared, _, mut attempt, clock)) =
                envelope.begin(ResponseEgressFramingMode::HostAutomatic)
            else {
                panic!("begin egress");
            };
            assert!(attempt.begin_writing());
            assert!(attempt.complete(clock.now()));
            let response = prepared.into_response();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.body().as_bytes(), Some(b"selected".as_slice()));
            assert_eq!(state_calls.load(Ordering::SeqCst) > 0, state);
            assert_eq!(prepare_calls.load(Ordering::SeqCst) > 0, prepare && !state);
        }
    }

    #[test]
    fn standard_service_installs_the_exact_application_outbound_clock() {
        use edgezero_core::http::Method;

        async fn elapsed(ctx: RequestContext) -> Result<String, EdgeError> {
            let client = ctx
                .http_client()
                .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("missing HTTP client")))?;
            let request =
                edgezero_core::OutboundRequest::get("https://example.com/")?.stream_response();
            let results = client
                .send_all_until(vec![request], Deadline::after(Duration::from_secs(1)))
                .await
                .map_err(|failure| failure.into_parts().0)?;
            let elapsed = results
                .slots()
                .first()
                .and_then(|slot| match slot {
                    edgezero_core::OutboundCollectedSlot::Resolved(result) => Some(result),
                    edgezero_core::OutboundCollectedSlot::AlreadyDelivered
                    | edgezero_core::OutboundCollectedSlot::Unresolved => None,
                })
                .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("missing batch slot")))?
                .elapsed;
            Ok(elapsed.as_millis().to_string())
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
        let request = FastlyRequest::new(Method::GET, "http://example.test/clock");

        let envelope = FastlyService::new(&app)
            .dispatch_with(request, |_request, _extensions| {})
            .expect("dispatch")
            .into_envelope()
            .expect("response envelope");
        let Ok((prepared, _, mut attempt, egress_clock)) =
            envelope.begin(ResponseEgressFramingMode::HostAutomatic)
        else {
            panic!("begin response egress");
        };
        assert!(attempt.begin_writing());
        assert!(attempt.complete(egress_clock.now()));
        let response = prepared.into_response();

        assert_eq!(response.body().as_bytes(), Some(b"7".as_slice()));
        assert!(observations.load(Ordering::SeqCst) >= 3);
    }

    #[test]
    fn extended_request_extensions_are_visible_to_handler() {
        use edgezero_core::body::Body;
        use edgezero_core::context::RequestContext;
        use edgezero_core::http::{Method, StatusCode, request_builder};
        use edgezero_core::router::RouterService;
        use futures::executor::block_on;

        #[derive(Clone)]
        struct Ja4(String);

        async fn handler(ctx: RequestContext) -> Result<String, EdgeError> {
            let ja4 = ctx
                .extensions()
                .get::<Ja4>()
                .map_or_else(|| "missing".to_owned(), |value| value.0.clone());
            Ok(ja4)
        }

        // Mirror what `dispatch_with_handles` does: a scratch bag built from the
        // raw request is `extend`ed into the core request before dispatch.
        let mut scratch = Extensions::default();
        scratch.insert(Ja4("t13d1516h2".to_owned()));

        let mut core_request = request_builder()
            .method(Method::GET)
            .uri("/ja4")
            .body(Body::empty())
            .expect("request");
        core_request.extensions_mut().extend(scratch);

        let service = RouterService::builder().get("/ja4", handler).build();
        let response = block_on(service.oneshot(core_request)).expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body().as_bytes().expect("buffered"), b"t13d1516h2");
    }

    #[test]
    fn unsupported_optimizer_precedes_expiry_without_transport() {
        use edgezero_core::outbound::OutboundSendMode;
        use edgezero_core::outbound::test_support::check_unsupported_image_optimizer_preflight;

        with_callback(|| {
            executor::block_on(check_unsupported_image_optimizer_preflight(
                FastlyOutboundClient::with_time,
                "fastly",
                &[OutboundSendMode::Begin, OutboundSendMode::Batch],
            ))
            .expect("unsupported-mode conformance");
            let stats = FastlyOutboundObservation::current().unwrap().snapshot();
            assert_eq!(stats.registration_count(), 0);
            assert_eq!(stats.configuration_count(), 0);
        });
    }

    #[test]
    fn synthesis_wraps_bare_kv_handle_under_default_when_no_registry() {
        let stores = Stores {
            kv: Some(kv_handle()),
            ..Default::default()
        };
        let (config_out, kv_out, secret_out) = synthesise_store_registries(stores);
        assert!(
            config_out.is_none(),
            "no config wiring -> no config registry"
        );
        assert!(
            secret_out.is_none(),
            "no secret wiring -> no secret registry"
        );
        let kv_reg = kv_out.expect("kv registry synthesised from bare handle");
        assert_eq!(
            kv_reg.default_id(),
            "default",
            "synthesised id is `default`"
        );
        assert!(kv_reg.named("default").is_some());
        assert!(
            kv_reg.named("other").is_none(),
            "synthesised registry only knows the `default` id"
        );
    }

    #[test]
    fn synthesis_registry_wins_over_bare_handle_when_both_wired() {
        // Multi-id registry declaring only `sessions` paired with a
        // bare handle that would otherwise synthesise to a
        // `default`-keyed entry. Precedence rule: the bare handle
        // is dropped entirely; the registry stands alone with no
        // `default` id.
        let mut by_id: BTreeMap<String, KvHandle> = BTreeMap::new();
        by_id.insert("sessions".to_owned(), kv_handle());
        let registry = KvRegistry::new(by_id, "sessions".to_owned());
        let stores = Stores {
            kv: Some(kv_handle()),
            kv_registry: Some(registry),
            ..Default::default()
        };
        let (_, kv_out, _) = synthesise_store_registries(stores);
        let kv_reg = kv_out.expect("registry survives synthesis");
        assert_eq!(kv_reg.default_id(), "sessions");
        assert!(
            kv_reg.named("default").is_none(),
            "bare handle's `default` synth NOT merged in"
        );
    }

    #[test]
    fn synthesis_returns_none_for_each_kind_with_no_wiring() {
        let (config, kv, secret) = synthesise_store_registries(Stores::default());
        assert!(config.is_none() && kv.is_none() && secret.is_none());
    }

    #[test]
    fn synthesis_handles_config_and_secret_bare_handles_symmetrically() {
        let stores = Stores {
            config_store: Some(config_handle()),
            secrets: Some(secret_handle()),
            ..Default::default()
        };
        let (config_out, _, secret_out) = synthesise_store_registries(stores);
        let config_reg = config_out.expect("config wrapped");
        assert_eq!(config_reg.default_id(), "default");
        let secret_reg = secret_out.expect("secret wrapped");
        assert_eq!(secret_reg.default_id(), "default");
        // BoundSecretStore binds the synthesised secret to platform
        // store name "default" -- if the underlying Fastly account
        // has no Secret Store literally named "default", the
        // require_str() call from a handler will fail with a clear
        // store-name error rather than silent miss.
        assert_eq!(
            secret_reg.default().expect("default bound").store_name(),
            "default"
        );
    }

    // Regression for the `.with_secrets()` bug — the pre-fix
    // `resolve_secret_handle(false)` short-circuited to `None`, so the
    // documented `.with_secrets().send()` path silently ran
    // handlers without a `SecretRegistry`. After the fix, the handle
    // is always built when `SecretSource::On` is selected; `_required`
    // is reserved for whichever future per-secret-lookup availability
    // policy lands.

    #[test]
    fn resolve_secret_handle_builds_handle_when_required_false_matches_with_secrets_default() {
        // The return type is unconditionally `SecretHandle` (post-
        // clippy::unnecessary_wraps cleanup); just exercise the
        // call to lock in that `.with_secrets()`-shaped paths
        // (required=false) still build a handle without panicking.
        let _handle = resolve_secret_handle(false);
    }

    #[test]
    fn resolve_secret_handle_builds_handle_when_required_true_matches_require_secrets() {
        let _handle = resolve_secret_handle(true);
    }
}

#[cfg(test)]
mod ingress_tests {
    use super::{
        Stores, capture_request_ingress, dispatch_with_handles, into_core_request_head,
        origin_from_runtime_uri,
    };
    use crate::context::FastlyRequestContext;
    use crate::delivery::DispatchOutcome;
    use async_trait::async_trait;
    use edgezero_core::app::App;
    use edgezero_core::body::Body;
    use edgezero_core::error::EdgeError;
    use edgezero_core::http::header::HOST;
    use edgezero_core::http::{HeaderValue, Response, Uri, response_builder};
    use edgezero_core::ingress::{
        CapturedTarget, HeaderFidelity, OriginSource, Preservation, RequestIngress, TargetSource,
        TargetUnavailable,
    };
    use edgezero_core::router::{PreDispatchHook, RouterService};
    use edgezero_core::{HttpClient, MonotonicClock, PreDispatchRequest};
    use fastly::Request as FastlyRequest;
    use std::sync::Arc;

    #[derive(Clone)]
    struct ScratchMarker;

    struct VerifySnapshot {
        scratch_expected: bool,
    }

    #[async_trait(?Send)]
    impl PreDispatchHook for VerifySnapshot {
        async fn handle(
            &self,
            request: &mut PreDispatchRequest,
        ) -> Result<Option<Response>, EdgeError> {
            assert_eq!(
                request.extensions().get::<ScratchMarker>().is_some(),
                self.scratch_expected
            );
            let ingress = request
                .extensions()
                .get::<RequestIngress>()
                .expect("should expose chosen snapshot");
            assert!(matches!(
                ingress.target(),
                CapturedTarget::Unavailable(TargetUnavailable::NotExposed)
            ));
            Ok(Some(
                response_builder()
                    .status(204)
                    .body(Body::empty())
                    .map_err(EdgeError::internal)?,
            ))
        }
    }

    #[test]
    fn ingress_runtime_origin_accepts_one_matching_authority() {
        for (url, host, expected_scheme) in [
            ("http://example.com/reserved", "EXAMPLE.com:80", "http"),
            ("https://example.com/reserved", "example.com:443", "https"),
            (
                "https://example.com:8443/reserved",
                "example.com:8443",
                "https",
            ),
            ("https://[::1]/reserved", "[::1]:443", "https"),
        ] {
            let uri: Uri = url.parse().expect("should parse runtime URI");
            let origin = origin_from_runtime_uri(&uri, [host.as_bytes()].into_iter())
                .expect("should recover the matching runtime origin");
            assert_eq!(
                origin.scheme(),
                expected_scheme,
                "should use runtime scheme"
            );
            assert_eq!(
                origin.authority(),
                uri.authority()
                    .expect("should have runtime authority")
                    .as_str(),
                "should retain runtime authority rather than Host spelling"
            );
            assert_eq!(
                origin.source(),
                OriginSource::RuntimeUri,
                "should identify runtime provenance"
            );
        }
    }

    #[test]
    fn ingress_runtime_origin_rejects_missing_duplicate_and_conflicting_hosts() {
        let uri: Uri = "https://example.com/reserved"
            .parse()
            .expect("should parse URI");
        for hosts in [
            vec![],
            vec![b"example.com".as_slice(), b"example.com".as_slice()],
            vec![b"other.example.com".as_slice()],
            vec![b"example.com:80".as_slice()],
            vec![b"example.com, other.example.com".as_slice()],
            vec![b"example.com:invalid".as_slice()],
            vec![b"a=\xff".as_slice()],
        ] {
            assert!(
                origin_from_runtime_uri(&uri, hosts.into_iter()).is_none(),
                "should not invent an origin from unavailable or inconsistent authority"
            );
        }
    }

    #[test]
    fn ingress_runtime_origin_rejects_invalid_runtime_authorities() {
        for url in [
            "https://user@example.com/reserved",
            "https://example.com:invalid/reserved",
            "https://example.com:65536/reserved",
        ] {
            let uri: Uri = url
                .parse()
                .expect("should represent invalid runtime authority");
            assert!(
                origin_from_runtime_uri(&uri, [b"example.com".as_slice()].into_iter()).is_none(),
                "should validate the complete runtime authority before comparing host or default port"
            );
        }
    }

    fn snapshot() -> RequestIngress {
        RequestIngress::new(
            CapturedTarget::capture(
                "/reserved/%2e",
                TargetSource::TransportRequestTarget,
                Preservation::Unknown,
            ),
            None,
            HeaderFidelity::default(),
        )
    }

    #[test]
    fn ingress_current_sdk_reports_missing_target_and_no_synthetic_origin() {
        let native = FastlyRequest::get("https://example.com/reserved");
        let ingress = capture_request_ingress(&native);
        assert_eq!(
            ingress.header_fidelity(&HOST).global_field_order(),
            Preservation::Unknown
        );
        assert!(matches!(
            ingress.target(),
            CapturedTarget::Unavailable(TargetUnavailable::NotExposed)
        ));
        assert!(ingress.origin().is_none());
    }

    #[test]
    fn ingress_snapshot_and_header_bytes_survive_conversion() {
        let mut native = FastlyRequest::new("EXAMPLE-METHOD", "https://example.com/later");
        native.append_header("cookie", b"a=\xff".as_slice());
        native.append_header("cookie", "b=2");
        let client_ip = native.get_client_ip_addr();
        let core = into_core_request_head(&native, MonotonicClock::default(), snapshot())
            .map_err(|failure| failure.error)
            .expect("should convert snapshot");
        assert_eq!(
            FastlyRequestContext::get(&core)
                .expect("should preserve context")
                .client_ip,
            client_ip
        );
        assert!(core.extensions().get::<HttpClient>().is_some());
        assert_eq!(core.method().as_str(), "EXAMPLE-METHOD");
        let values: Vec<_> = core
            .headers()
            .get_all("cookie")
            .iter()
            .map(HeaderValue::as_bytes)
            .collect();
        assert_eq!(values, [b"a=\xff".as_slice(), b"b=2".as_slice()]);
        let CapturedTarget::Complete(target) = core
            .extensions()
            .get::<RequestIngress>()
            .expect("should carry snapshot")
            .target()
        else {
            panic!("should retain target")
        };
        assert_eq!(target.value(), "/reserved/%2e");
    }

    #[test]
    fn ingress_registry_dispatch_wins_scratch_collision() {
        let app = App::new(
            RouterService::builder()
                .pre_dispatch_hook_arc(Arc::new(VerifySnapshot {
                    scratch_expected: true,
                }))
                .build(),
        );
        let outcome = dispatch_with_handles(
            &app,
            FastlyRequest::get("https://example.com/later"),
            Stores::default(),
            app.monotonic_now(),
            |native, scratch| {
                native.set_url("https://spoof.example.com/changed");
                scratch.insert(snapshot());
                scratch.insert(ScratchMarker);
            },
        )
        .expect("should dispatch with chosen snapshot");
        let DispatchOutcome::Routed(mut envelope) = outcome else {
            panic!("expected routed hook response")
        };
        assert_eq!(envelope.response_mut().status().as_u16(), 204);
    }
}

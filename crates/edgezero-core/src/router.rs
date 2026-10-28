use std::collections::HashMap;
use std::io::Error as IoError;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use matchit::Router as PathRouter;
use tower_service::Service;

use crate::context::{FallbackDrainOutcome, RequestContext, drain_body_discard};
use crate::error::EdgeError;
use crate::handler::{BoxHandler, IntoHandler, IntrospectionNeeds};
use crate::http::{Extensions, HandlerFuture, Method, Request, Response};
use crate::ingress::AdmittedIngress;
use crate::introspection::{ManifestJson, RouteTable};
use crate::middleware::{BoxMiddleware, Middleware, Next};
use crate::params::PathParams;
use crate::response::IntoResponse as _;

/// Shared optional interception performed before router lookup and state injection.
pub type BoxPreDispatchHook = Arc<dyn PreDispatchHook>;

/// Inspect or terminate an admitted request before handler dispatch and route middleware.
///
/// Adapter admission may resolve route identity first. The hook cannot bypass admission,
/// inbound read deadlines, or the response's owned egress lifecycle.
///
/// The hook receives adapter extensions but no router-injected state or
/// introspection. It may capture application state in its own implementation.
/// Futures deliberately do not require `Send` so WASM runtimes remain supported.
#[async_trait(?Send)]
pub trait PreDispatchHook: Send + Sync + 'static {
    /// Return a terminal response, or continue with the mutated request.
    /// Continuing hooks may change headers, extensions, the body and URI query,
    /// but must preserve the method and URI path. A violation is an internal error.
    ///
    /// # Errors
    ///
    /// Errors prevent dispatch. [`Service::call`] propagates them, while
    /// [`RouterService::oneshot`] uses the existing error-response rendering.
    async fn handle(&self, request: &mut Request) -> Result<Option<Response>, EdgeError>;
}

struct RouteEntry {
    handler: BoxHandler,
    introspection_needs: IntrospectionNeeds,
    metadata: RouteMetadata,
}

impl Clone for RouteEntry {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
            introspection_needs: self.introspection_needs,
            metadata: self.metadata.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.handler = Arc::clone(&source.handler);
        self.introspection_needs = source.introspection_needs;
        self.metadata.clone_from(&source.metadata);
    }
}

/// Stable identity for one registered route.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RouteId {
    method: Method,
    pattern: String,
}

impl RouteId {
    #[must_use]
    #[inline]
    pub fn method(&self) -> &Method {
        &self.method
    }

    fn new<S>(method: Method, pattern: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            method,
            pattern: pattern.into(),
        }
    }

    #[must_use]
    #[inline]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }
}

/// Canonical metadata for one registered route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteMetadata {
    class: Option<Arc<str>>,
    id: RouteId,
}

impl RouteMetadata {
    /// Opaque manifest-sourced route class used by admission policy.
    #[must_use]
    #[inline]
    pub fn class(&self) -> Option<&str> {
        self.class.as_deref()
    }

    #[must_use]
    #[inline]
    pub fn id(&self) -> &RouteId {
        &self.id
    }

    #[must_use]
    #[inline]
    pub fn method(&self) -> &Method {
        self.id.method()
    }

    #[inline]
    pub fn new<S>(method: Method, pattern: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            class: None,
            id: RouteId::new(method, pattern),
        }
    }

    fn new_with_class<S, C>(method: Method, pattern: S, class: C) -> Self
    where
        C: Into<Arc<str>>,
        S: Into<String>,
    {
        Self {
            class: Some(class.into()),
            id: RouteId::new(method, pattern),
        }
    }

    /// Registered route pattern. Preserved for existing route-introspection consumers.
    #[must_use]
    #[inline]
    pub fn path(&self) -> &str {
        self.pattern()
    }

    #[must_use]
    #[inline]
    pub fn pattern(&self) -> &str {
        self.id.pattern()
    }
}

/// Backward-compatible name for route-table entries; the value is canonical route metadata.
pub type RouteInfo = RouteMetadata;

/// Stable result exposed to ingress admission before route dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RouteResolution {
    Matched(RouteMetadata),
    MethodNotAllowed { allowed: Arc<[RouteMetadata]> },
    NotFound,
}

enum ResolvedTarget {
    Found(RouteEntry, PathParams),
    MethodNotAllowed(Vec<RouteMetadata>),
    NotFound,
}

/// Opaque route-match token bound to the router that created it.
pub struct ResolvedDispatch {
    method: Method,
    owner: Arc<RouterInner>,
    path: String,
    resolution: RouteResolution,
    target: ResolvedTarget,
}

impl ResolvedDispatch {
    #[must_use]
    #[inline]
    pub(crate) fn method(&self) -> &Method {
        &self.method
    }

    #[must_use]
    #[inline]
    pub fn resolution(&self) -> &RouteResolution {
        &self.resolution
    }
}

enum RouteMatch<'route> {
    Found(&'route RouteEntry, PathParams),
    MethodNotAllowed(Vec<RouteMetadata>),
    NotFound,
}

#[derive(Default)]
pub struct RouterBuilder {
    manifest_json: Option<Arc<str>>,
    middlewares: Vec<BoxMiddleware>,
    pre_dispatch_hook: Option<BoxPreDispatchHook>,
    route_info: Vec<RouteInfo>,
    routes: HashMap<Method, PathRouter<RouteEntry>>,
    /// App state registered via [`RouterBuilder::with_state`], keyed by type.
    /// Cloned into every request's extensions at dispatch.
    state_extensions: Extensions,
}

impl RouterBuilder {
    #[expect(
        clippy::panic,
        reason = "duplicate route is a build-time programmer error, not a runtime condition"
    )]
    fn add_route<H>(&mut self, path: &str, method: Method, class: Option<Arc<str>>, handler: H)
    where
        H: IntoHandler,
    {
        let metadata = class.map_or_else(
            || RouteMetadata::new(method.clone(), path),
            |route_class| RouteMetadata::new_with_class(method.clone(), path, route_class),
        );
        let router = self.routes.entry(method).or_default();

        // The handler reports which introspection payloads its route needs; the
        // flag is read once here and consulted per request in `dispatch`.
        let boxed = handler.into_handler();
        let introspection_needs = boxed.introspection_needs();

        router
            .insert(
                path,
                RouteEntry {
                    handler: boxed,
                    introspection_needs,
                    metadata: metadata.clone(),
                },
            )
            .unwrap_or_else(|err| panic!("duplicate route definition for {path}: {err}"));

        self.route_info.push(metadata);
    }

    #[must_use]
    #[inline]
    pub fn build(self) -> RouterService {
        let route_index: Arc<[RouteInfo]> = Arc::from(self.route_info);

        RouterService::new(
            self.routes,
            self.middlewares,
            route_index,
            self.manifest_json,
            self.state_extensions,
            self.pre_dispatch_hook,
        )
    }

    #[must_use]
    #[inline]
    pub fn delete<H>(self, path: &str, handler: H) -> Self
    where
        H: IntoHandler,
    {
        self.route(path, Method::DELETE, handler)
    }

    #[must_use]
    #[inline]
    pub fn get<H>(self, path: &str, handler: H) -> Self
    where
        H: IntoHandler,
    {
        self.route(path, Method::GET, handler)
    }

    #[must_use]
    #[inline]
    pub fn middleware<M>(mut self, middleware: M) -> Self
    where
        M: Middleware,
    {
        self.middlewares.push(Arc::new(middleware));
        self
    }

    #[must_use]
    #[inline]
    pub fn middleware_arc(mut self, middleware: BoxMiddleware) -> Self {
        self.middlewares.push(middleware);
        self
    }

    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    #[inline]
    pub fn post<H>(self, path: &str, handler: H) -> Self
    where
        H: IntoHandler,
    {
        self.route(path, Method::POST, handler)
    }

    /// Install one hook before route lookup, state injection and middleware.
    ///
    /// Router clones share the same instance. None of the normal route lifecycle
    /// runs after a terminal response; outer adapter/application finalizers may run.
    ///
    /// # Panics
    /// Panics if a hook is already registered.
    #[must_use]
    #[inline]
    pub fn pre_dispatch_hook<H>(self, hook: H) -> Self
    where
        H: PreDispatchHook,
    {
        self.pre_dispatch_hook_arc(Arc::new(hook))
    }

    /// Install a shared hook with the same guarantees as [`Self::pre_dispatch_hook`].
    ///
    /// # Panics
    /// Panics if a hook is already registered.
    #[must_use]
    #[inline]
    pub fn pre_dispatch_hook_arc(mut self, hook: BoxPreDispatchHook) -> Self {
        assert!(
            self.pre_dispatch_hook.is_none(),
            "pre-dispatch hook already registered"
        );
        self.pre_dispatch_hook = Some(hook);
        self
    }

    #[must_use]
    #[inline]
    pub fn put<H>(self, path: &str, handler: H) -> Self
    where
        H: IntoHandler,
    {
        self.route(path, Method::PUT, handler)
    }

    #[must_use]
    #[inline]
    pub fn route<H>(mut self, path: &str, method: Method, handler: H) -> Self
    where
        H: IntoHandler,
    {
        self.add_route(path, method, None, handler);
        self
    }

    /// Registers a route with opaque admission metadata from the manifest.
    #[must_use]
    #[inline]
    pub fn route_with_class<H, C>(
        mut self,
        path: &str,
        method: Method,
        class: C,
        handler: H,
    ) -> Self
    where
        C: Into<Arc<str>>,
        H: IntoHandler,
    {
        self.add_route(path, method, Some(class.into()), handler);
        self
    }

    #[must_use]
    #[inline]
    pub fn with_manifest_json<S>(mut self, json: S) -> Self
    where
        S: Into<Arc<str>>,
    {
        self.manifest_json = Some(json.into());
        self
    }

    /// Register a value cloned into every request's extensions before
    /// dispatch, making it available to the [`State<T>`] extractor and to
    /// `RequestContext`-based handlers.
    ///
    /// Typically `T = Arc<AppState>`. Registering the same `T` twice is
    /// last-write-wins. Cost is one `T::clone` (an `Arc` bump for
    /// `Arc<AppState>`) per registered state per request.
    ///
    /// [`State<T>`]: crate::extractor::State
    #[must_use]
    #[inline]
    pub fn with_state<T>(mut self, value: T) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        self.state_extensions.insert(value);
        self
    }
}

struct RouterInner {
    manifest_json: Option<Arc<str>>,
    middlewares: Vec<BoxMiddleware>,
    pre_dispatch_hook: Option<BoxPreDispatchHook>,
    route_index: Arc<[RouteInfo]>,
    routes: HashMap<Method, PathRouter<RouteEntry>>,
    state_extensions: Extensions,
}

impl RouterInner {
    async fn dispatch(&self, mut request: Request) -> Result<Response, EdgeError> {
        if let Some(response) = self.pre_dispatch(&mut request).await? {
            return Ok(response);
        }
        let method = request.method().clone();
        let path = request.uri().path().to_owned();

        match self.find_route(&method, &path) {
            RouteMatch::Found(entry, params) => {
                self.dispatch_found(request, entry, params, None).await
            }
            RouteMatch::MethodNotAllowed(allowed) => {
                let methods = allowed
                    .iter()
                    .map(|metadata| metadata.method().clone())
                    .collect::<Vec<_>>();
                Err(EdgeError::method_not_allowed(&method, &methods))
            }
            RouteMatch::NotFound => Err(EdgeError::not_found(path)),
        }
    }

    async fn dispatch_found(
        &self,
        mut request: Request,
        entry: &RouteEntry,
        params: PathParams,
        ingress: Option<AdmittedIngress>,
    ) -> Result<Response, EdgeError> {
        // Inject only the introspection payloads this route asked for —
        // nothing for the vast majority of routes that need none.
        let needs = entry.introspection_needs;
        if needs.manifest
            && let Some(json) = &self.manifest_json
        {
            request
                .extensions_mut()
                .insert(ManifestJson(Arc::clone(json)));
        }
        if needs.routes {
            request
                .extensions_mut()
                .insert(RouteTable(Arc::clone(&self.route_index)));
        }
        // App-owned state registered via RouterBuilder::with_state.
        // Runs after introspection inserts; `extend` overwrites by TypeId, so app state wins.
        request
            .extensions_mut()
            .extend(self.state_extensions.clone());
        let ctx = match ingress {
            Some(admitted) => {
                RequestContext::new_routed(request, params, entry.metadata.clone(), admitted)
            }
            None => RequestContext::new(request, params),
        };
        let next = Next::new(&self.middlewares, entry.handler.as_ref());
        next.run(ctx).await
    }

    fn find_route(&self, method: &Method, path: &str) -> RouteMatch<'_> {
        if let Some(router) = self.routes.get(method)
            && let Ok(matched) = router.at(path)
        {
            let params = PathParams::new(
                matched
                    .params
                    .iter()
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .collect(),
            );
            return RouteMatch::Found(matched.value, params);
        }

        let mut allowed: Vec<RouteMetadata> = self
            .routes
            .values()
            .filter_map(|router| router.at(path).ok())
            .map(|matched| matched.value.metadata.clone())
            .collect();

        allowed.sort_by(|left, right| {
            left.method()
                .as_str()
                .cmp(right.method().as_str())
                .then_with(|| left.pattern().cmp(right.pattern()))
        });

        if allowed.is_empty() {
            RouteMatch::NotFound
        } else {
            RouteMatch::MethodNotAllowed(allowed)
        }
    }
    async fn pre_dispatch(&self, request: &mut Request) -> Result<Option<Response>, EdgeError> {
        let method = request.method().clone();
        let path = request.uri().path().to_owned();
        if let Some(hook) = &self.pre_dispatch_hook {
            if let Some(response) = hook.handle(request).await? {
                return Ok(Some(response));
            }
            if request.method() != method || request.uri().path() != path {
                return Err(EdgeError::internal(IoError::other(
                    "pre-dispatch hook changed the request method or path",
                )));
            }
        }
        Ok(None)
    }
}

#[derive(Clone)]
pub struct RouterService {
    inner: Arc<RouterInner>,
}

impl Service<Request> for RouterService {
    type Error = EdgeError;
    type Future = HandlerFuture;
    type Response = Response;

    #[inline]
    fn call(&mut self, req: Request) -> Self::Future {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.dispatch(req).await })
    }

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl RouterService {
    #[must_use]
    #[inline]
    pub fn builder() -> RouterBuilder {
        RouterBuilder::new()
    }

    /// Dispatches the exact route selected before ingress admission without rematching.
    ///
    /// # Errors
    /// Returns [`EdgeError::Internal`] if the token belongs to another router or the request
    /// method/path changed after resolution. Normal handler and routing errors are preserved.
    #[inline]
    pub async fn dispatch_resolved(
        &self,
        resolved: ResolvedDispatch,
        mut request: Request,
        ingress: AdmittedIngress,
    ) -> Result<Response, EdgeError> {
        if !Arc::ptr_eq(&self.inner, &resolved.owner) {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "resolved dispatch token belongs to another router"
            )));
        }
        if request.method() != resolved.method || request.uri().path() != resolved.path {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "request method or path changed after ingress route resolution"
            )));
        }
        if let Some(response) = self.inner.pre_dispatch(&mut request).await? {
            return Ok(response);
        }

        match resolved.target {
            ResolvedTarget::Found(entry, params) => {
                if ingress.is_fallback() {
                    return Err(EdgeError::internal(anyhow::anyhow!(
                        "fallback body policy cannot dispatch a matched route"
                    )));
                }
                self.inner
                    .dispatch_found(request, &entry, params, Some(ingress))
                    .await
            }
            ResolvedTarget::MethodNotAllowed(allowed) => {
                if let Some(response) = fallback_terminal_response(request, ingress).await? {
                    return Ok(response);
                }
                let methods = allowed
                    .iter()
                    .map(|metadata| metadata.method().clone())
                    .collect::<Vec<_>>();
                Err(EdgeError::method_not_allowed(&resolved.method, &methods))
            }
            ResolvedTarget::NotFound => {
                if let Some(response) = fallback_terminal_response(request, ingress).await? {
                    return Ok(response);
                }
                Err(EdgeError::not_found(resolved.path))
            }
        }
    }

    fn new(
        routes: HashMap<Method, PathRouter<RouteEntry>>,
        middlewares: Vec<BoxMiddleware>,
        route_index: Arc<[RouteInfo]>,
        manifest_json: Option<Arc<str>>,
        state_extensions: Extensions,
        pre_dispatch_hook: Option<BoxPreDispatchHook>,
    ) -> Self {
        Self {
            inner: Arc::new(RouterInner {
                manifest_json,
                middlewares,
                pre_dispatch_hook,
                route_index,
                routes,
                state_extensions,
            }),
        }
    }

    /// # Errors
    /// Returns [`EdgeError`] if the dispatched handler errors AND the error
    /// itself fails to render as a response.
    #[inline]
    pub async fn oneshot(&self, request: Request) -> Result<Response, EdgeError> {
        let mut service = self.clone();
        match service.call(request).await {
            Ok(response) => Ok(response),
            Err(err) => err.into_response(),
        }
    }

    /// Resolves a route without invoking middleware, handlers, or request-body code.
    #[must_use]
    #[inline]
    pub fn resolve(&self, method: &Method, path: &str) -> ResolvedDispatch {
        let (resolution, target) = match self.inner.find_route(method, path) {
            RouteMatch::Found(entry, params) => (
                RouteResolution::Matched(entry.metadata.clone()),
                ResolvedTarget::Found(entry.clone(), params),
            ),
            RouteMatch::MethodNotAllowed(allowed) => (
                RouteResolution::MethodNotAllowed {
                    allowed: Arc::from(allowed.clone()),
                },
                ResolvedTarget::MethodNotAllowed(allowed),
            ),
            RouteMatch::NotFound => (RouteResolution::NotFound, ResolvedTarget::NotFound),
        };

        ResolvedDispatch {
            method: method.clone(),
            owner: Arc::clone(&self.inner),
            path: path.to_owned(),
            resolution,
            target,
        }
    }

    #[must_use]
    #[inline]
    pub fn routes(&self) -> Vec<RouteInfo> {
        self.inner.route_index.to_vec()
    }

    /// Borrows state registered with [`RouterBuilder::with_state`] without cloning or rebuilding it.
    #[must_use]
    #[inline]
    pub fn state<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.inner.state_extensions.get::<T>()
    }
}

async fn fallback_terminal_response(
    request: Request,
    ingress: AdmittedIngress,
) -> Result<Option<Response>, EdgeError> {
    let Some(fallback) = ingress.into_fallback() else {
        return Ok(None);
    };
    let (grant, max_body_bytes, read_deadline, monotonic_clock, on_exceeded, on_timeout) =
        fallback.into_parts();
    let outcome = drain_body_discard(
        request.into_body(),
        max_body_bytes,
        read_deadline,
        &monotonic_clock,
    )
    .await;
    drop(grant);

    match outcome? {
        FallbackDrainOutcome::Complete => Ok(None),
        FallbackDrainOutcome::Exceeded => Ok(Some(on_exceeded.into_response())),
        FallbackDrainOutcome::TimedOut => Ok(Some(on_timeout.into_response())),
    }
}

#[cfg(test)]
mod tests {
    /// Per-capability introspection injection: a route receives exactly the
    /// payloads its handler opted into via `#[action(manifest|routes)]`.
    mod introspection_gating {
        use super::*;
        use crate::handler::DynHandler;

        /// A handler that records which introspection payloads its request
        /// carried, as `(manifest_present, routes_present)`, and reports `needs`.
        struct CapProbe {
            needs: IntrospectionNeeds,
            seen: Arc<Mutex<Option<(bool, bool)>>>,
        }

        impl DynHandler for CapProbe {
            fn call(&self, ctx: RequestContext) -> HandlerFuture {
                let seen = Arc::clone(&self.seen);
                Box::pin(async move {
                    *seen.lock().unwrap() = Some((
                        ctx.extension::<ManifestJson>().is_some(),
                        ctx.extension::<RouteTable>().is_some(),
                    ));
                    response_with_body(StatusCode::OK, Body::empty())
                })
            }
            fn introspection_needs(&self) -> IntrospectionNeeds {
                self.needs
            }
        }

        #[test]
        fn manifest_and_routes_route_injects_both() {
            // Combined `#[action(manifest, routes)]` → both payloads injected.
            let seen = run_probe(
                RouterService::builder()
                    .with_manifest_json(serde_json::json!({"app": {"name": "t"}}).to_string()),
                IntrospectionNeeds {
                    manifest: true,
                    routes: true,
                },
            );
            assert_eq!(seen, (true, true));
        }

        #[test]
        fn manifest_route_injects_only_manifest() {
            // Manifest available AND requested → ManifestJson present, RouteTable absent.
            let seen = run_probe(
                RouterService::builder()
                    .with_manifest_json(serde_json::json!({"app": {"name": "t"}}).to_string()),
                IntrospectionNeeds {
                    manifest: true,
                    routes: false,
                },
            );
            assert_eq!(seen, (true, false));
        }

        #[test]
        fn middleware_sees_injected_manifest() {
            // Injection happens before the middleware chain, so a middleware on a
            // manifest-flagged route sees the payload.
            struct Probe(Arc<Mutex<Option<bool>>>);
            #[async_trait::async_trait(?Send)]
            impl Middleware for Probe {
                async fn handle(
                    &self,
                    ctx: RequestContext,
                    next: Next<'_>,
                ) -> Result<Response, EdgeError> {
                    *self.0.lock().unwrap() = Some(ctx.extension::<ManifestJson>().is_some());
                    next.run(ctx).await
                }
            }

            let saw: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
            let router = RouterService::builder()
                .with_manifest_json(serde_json::json!({"app": {"name": "t"}}).to_string())
                .middleware(Probe(Arc::clone(&saw)))
                .get(
                    "/",
                    CapProbe {
                        needs: IntrospectionNeeds {
                            manifest: true,
                            routes: false,
                        },
                        seen: Arc::new(Mutex::new(None)),
                    },
                )
                .build();
            let request = request_builder()
                .method(Method::GET)
                .uri("/")
                .body(Body::empty())
                .unwrap();
            block_on(router.oneshot(request)).unwrap();
            assert_eq!(*saw.lock().unwrap(), Some(true));
        }

        #[test]
        fn plain_route_injects_neither() {
            // Manifest IS baked but the route requested nothing → neither injected.
            let seen = run_probe(
                RouterService::builder()
                    .with_manifest_json(serde_json::json!({"app": {"name": "t"}}).to_string()),
                IntrospectionNeeds::default(),
            );
            assert_eq!(seen, (false, false));
        }

        #[test]
        fn routes_route_injects_only_routes() {
            // Only `routes` requested → RouteTable present (from the always-available
            // route index), ManifestJson absent (not requested; none baked either).
            let seen = run_probe(
                RouterService::builder(),
                IntrospectionNeeds {
                    manifest: false,
                    routes: true,
                },
            );
            assert_eq!(seen, (false, true));
        }

        fn run_probe(builder: RouterBuilder, needs: IntrospectionNeeds) -> (bool, bool) {
            let seen = Arc::new(Mutex::new(None));
            let router = builder
                .get(
                    "/",
                    CapProbe {
                        needs,
                        seen: Arc::clone(&seen),
                    },
                )
                .build();
            let request = request_builder()
                .method(Method::GET)
                .uri("/")
                .body(Body::empty())
                .unwrap();
            block_on(router.oneshot(request)).unwrap();
            let observed = *seen.lock().unwrap();
            observed.expect("handler ran")
        }
    }

    use super::*;
    use crate::app::App;
    use crate::body::Body;
    use crate::context::RequestContext;
    use crate::error::EdgeError;
    use crate::http::{Method, Request, Response, StatusCode, request_builder};
    use crate::ingress::{
        AdmissionDecision, IngressAdmissionOutcome, IngressFraming, IngressGrant, IngressHead,
        IngressHeadAccounting,
    };
    use crate::params::PathParams;
    use crate::response::response_with_body;
    use crate::response_egress::ResponseEgressCompletion;
    use crate::time::{Deadline, MonotonicInstant};
    use futures::executor::block_on;
    use futures::task::noop_waker_ref;
    use serde::Deserialize;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;

    #[test]
    fn resolve_exposes_stable_route_identity_and_sorted_method_candidates() {
        async fn handler(_ctx: RequestContext) -> Result<Response, EdgeError> {
            response_with_body(StatusCode::OK, Body::empty())
        }

        let router = RouterService::builder()
            .post("/users/{id}", handler)
            .route_with_class("/users/{id}", Method::GET, "auction", handler)
            .build();

        let matched = router.resolve(&Method::GET, "/users/42");
        let RouteResolution::Matched(metadata) = matched.resolution() else {
            panic!("expected matched route");
        };
        assert_eq!(metadata.method(), Method::GET);
        assert_eq!(metadata.pattern(), "/users/{id}");
        assert_eq!(metadata.id().method(), Method::GET);
        assert_eq!(metadata.id().pattern(), "/users/{id}");
        assert_eq!(metadata.class(), Some("auction"));

        let rejected = router.resolve(&Method::DELETE, "/users/99");
        let RouteResolution::MethodNotAllowed { allowed } = rejected.resolution() else {
            panic!("expected method-not-allowed route");
        };
        assert_eq!(allowed.len(), 2);
        assert_eq!(allowed[0].method(), Method::GET);
        assert_eq!(allowed[1].method(), Method::POST);
        assert_eq!(allowed[0].class(), Some("auction"));
        assert_eq!(allowed[1].class(), None);

        assert!(matches!(
            router.resolve(&Method::GET, "/missing").resolution(),
            RouteResolution::NotFound
        ));
    }

    fn admitted_for(resolved: &ResolvedDispatch, method: Method, path: &str) -> AdmittedIngress {
        let app = App::new(RouterService::builder().build());
        let request = request_builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .expect("request");
        let head = IngressHead::from_request(
            &request,
            MonotonicInstant::now(),
            resolved.resolution().clone(),
            IngressHeadAccounting::HostManaged,
            IngressFraming::HostManaged,
        );
        let IngressAdmissionOutcome::Admitted {
            completion,
            ingress: admitted,
        } = app.admit_ingress(&head).expect("admission")
        else {
            panic!("expected admission");
        };
        drop(completion);
        admitted
    }

    #[test]
    fn dispatch_resolved_preserves_metadata_and_grant() {
        async fn handler(ctx: RequestContext) -> Result<String, EdgeError> {
            let metadata = ctx.route_metadata().expect("route metadata");
            let grant = ctx.take_ingress_grant().expect("ingress grant");
            assert!(ctx.take_ingress_grant().is_none());
            let lease = grant.downcast::<String>().expect("lease type");
            Ok(format!(
                "{}:{}:{lease}",
                metadata.pattern(),
                metadata.class().expect("route class")
            ))
        }

        let router = RouterService::builder()
            .route_with_class("/users/{id}", Method::GET, "auction", handler)
            .build();
        let resolved = router.resolve(&Method::GET, "/users/42");
        let start = MonotonicInstant::now();
        let request = request_builder()
            .method(Method::GET)
            .uri("/users/42")
            .body(Body::empty())
            .expect("request");
        let head = IngressHead::from_request(
            &request,
            start,
            resolved.resolution().clone(),
            IngressHeadAccounting::HostManaged,
            IngressFraming::HostManaged,
        );
        let mut app = App::new(router.clone());
        app.set_ingress_admission_policy(|_| AdmissionDecision::Admit {
            completion: ResponseEgressCompletion::empty(),
            grant: IngressGrant::new(String::from("lease")),
            read_deadline: Deadline::after(Duration::from_secs(1)),
        });
        let IngressAdmissionOutcome::Admitted {
            completion,
            ingress: admitted,
        } = app.admit_ingress(&head).expect("admission")
        else {
            panic!("expected admission");
        };
        drop(completion);
        assert_eq!(admitted.request_start(), start);
        let response = block_on(router.dispatch_resolved(resolved, request, admitted))
            .expect("resolved response");
        assert_eq!(
            response.body().as_bytes().expect("buffered"),
            b"/users/{id}:auction:lease"
        );
    }

    #[test]
    fn dispatch_resolved_rejects_foreign_router_token() {
        let router = RouterService::builder()
            .get("/users/{id}", ok_handler)
            .build();
        let resolved = router.resolve(&Method::GET, "/users/42");
        let admitted = admitted_for(&resolved, Method::GET, "/users/42");
        let request = request_builder()
            .method(Method::GET)
            .uri("/users/42")
            .body(Body::empty())
            .expect("request");
        let foreign = RouterService::builder().build();
        assert!(matches!(
            block_on(foreign.dispatch_resolved(resolved, request, admitted)),
            Err(EdgeError::Internal { .. })
        ));
    }

    #[test]
    fn dispatch_resolved_rejects_mutated_request_path() {
        let router = RouterService::builder()
            .get("/users/{id}", ok_handler)
            .build();
        let resolved = router.resolve(&Method::GET, "/users/42");
        let admitted = admitted_for(&resolved, Method::GET, "/users/42");
        let request = request_builder()
            .method(Method::GET)
            .uri("/users/43")
            .body(Body::empty())
            .expect("request");
        assert!(matches!(
            block_on(router.dispatch_resolved(resolved, request, admitted)),
            Err(EdgeError::Internal { .. })
        ));
    }

    async fn ok_handler(_ctx: RequestContext) -> Result<Response, EdgeError> {
        response_with_body(StatusCode::OK, Body::empty())
    }

    #[test]
    fn builder_accepts_middleware_and_middleware_arc() {
        struct RecordingMiddleware {
            log: Arc<Mutex<Vec<&'static str>>>,
            name: &'static str,
        }

        #[async_trait::async_trait(?Send)]
        impl Middleware for RecordingMiddleware {
            async fn handle(
                &self,
                ctx: RequestContext,
                next: Next<'_>,
            ) -> Result<Response, EdgeError> {
                self.log.lock().unwrap().push(self.name);
                next.run(ctx).await
            }
        }

        let log = Arc::new(Mutex::new(Vec::new()));
        let first = RecordingMiddleware {
            log: Arc::clone(&log),
            name: "first",
        };
        let second = RecordingMiddleware {
            log: Arc::clone(&log),
            name: "second",
        };

        let service = RouterService::builder()
            .middleware(first)
            .middleware_arc({
                let arc: BoxMiddleware = Arc::new(second);
                arc
            })
            .get("/test", ok_handler)
            .build();

        let request = request_builder()
            .method(Method::GET)
            .uri("/test")
            .body(Body::empty())
            .expect("request");
        let response = block_on(service.clone().call(request)).expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let entries = log.lock().unwrap().clone();
        assert_eq!(entries, vec!["first", "second"]);
    }

    #[test]
    fn builder_supports_put_and_delete_routes() {
        let service = RouterService::builder()
            .put("/items", ok_handler)
            .delete("/items", ok_handler)
            .build();

        let put_request = request_builder()
            .method(Method::PUT)
            .uri("/items")
            .body(Body::empty())
            .expect("request");
        let put_response = block_on(service.clone().call(put_request)).expect("response");
        assert_eq!(put_response.status(), StatusCode::OK);

        let delete_request = request_builder()
            .method(Method::DELETE)
            .uri("/items")
            .body(Body::empty())
            .expect("request");
        let delete_response = block_on(service.clone().call(delete_request)).expect("response");
        assert_eq!(delete_response.status(), StatusCode::OK);
    }

    #[test]
    #[should_panic(expected = "duplicate route definition")]
    fn duplicate_route_definition_panics() {
        let _service = RouterService::builder()
            .get("/dup", ok_handler)
            .get("/dup", ok_handler)
            .build();
    }

    #[test]
    fn handler_returns_bad_request_for_invalid_path_params() {
        #[derive(Deserialize)]
        struct Params {
            id: String,
        }

        async fn handler(ctx: RequestContext) -> Result<String, EdgeError> {
            let params: Params = ctx.path()?;
            let id = params
                .id
                .parse::<u32>()
                .map_err(|_e| EdgeError::bad_request("invalid id"))?;
            Ok(format!("hello {id}"))
        }

        let service = RouterService::builder().get("/items/{id}", handler).build();
        let ok_request = request_builder()
            .method(Method::GET)
            .uri("/items/42")
            .body(Body::empty())
            .expect("request");
        let ok_response = block_on(service.clone().call(ok_request)).expect("response");
        assert_eq!(ok_response.status(), StatusCode::OK);
        assert_eq!(
            ok_response.body().as_bytes().expect("buffered"),
            b"hello 42"
        );

        let request = request_builder()
            .method(Method::GET)
            .uri("/items/abc")
            .body(Body::empty())
            .expect("request");

        let error = block_on(service.clone().call(request)).expect_err("error");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn oneshot_returns_error_response() {
        let service = RouterService::builder().build();
        let request = request_builder()
            .method(Method::GET)
            .uri("/missing")
            .body(Body::empty())
            .expect("request");

        let response = block_on(service.oneshot(request)).expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn oneshot_returns_success_response() {
        let service = RouterService::builder().get("/ok", ok_handler).build();
        let request = request_builder()
            .method(Method::GET)
            .uri("/ok")
            .body(Body::empty())
            .expect("request");

        let response = block_on(service.oneshot(request)).expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn returns_method_not_allowed() {
        let service = RouterService::builder().post("/submit", ok_handler).build();

        let request = request_builder()
            .method(Method::GET)
            .uri("/submit")
            .body(Body::empty())
            .expect("request");

        let error = block_on(service.clone().call(request)).expect_err("error");
        assert_eq!(error.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[test]
    fn returns_method_not_allowed_with_multiple_methods() {
        let service = RouterService::builder()
            .get("/submit", ok_handler)
            .post("/submit", ok_handler)
            .build();

        let request = request_builder()
            .method(Method::PUT)
            .uri("/submit")
            .body(Body::empty())
            .expect("request");

        let error = block_on(service.clone().call(request)).expect_err("error");
        assert_eq!(error.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[test]
    fn returns_not_found() {
        let service = RouterService::builder().get("/known", ok_handler).build();
        let request = request_builder()
            .method(Method::GET)
            .uri("/missing")
            .body(Body::empty())
            .expect("request");

        let error = block_on(service.clone().call(request)).expect_err("error");
        assert_eq!(error.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn route_entry_clone_copies_handler() {
        let entry = RouteEntry {
            handler: ok_handler.into_handler(),
            introspection_needs: IntrospectionNeeds::default(),
            metadata: RouteMetadata::new(Method::GET, "/test"),
        };
        let cloned = entry.clone();

        let request = request_builder()
            .method(Method::GET)
            .uri("/test")
            .body(Body::empty())
            .expect("request");
        let ctx = RequestContext::new(request, PathParams::default());
        let response = block_on(cloned.handler.call(ctx)).expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn route_matches_path_params() {
        #[derive(Deserialize)]
        struct Params {
            id: String,
        }

        async fn handler(ctx: RequestContext) -> Result<String, EdgeError> {
            let params: Params = ctx.path()?;
            Ok(format!("hello {}", params.id))
        }

        let service = RouterService::builder().get("/hello/{id}", handler).build();

        let request = request_builder()
            .method(Method::GET)
            .uri("/hello/world")
            .body(Body::empty())
            .expect("request");

        let response = block_on(service.clone().call(request)).expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.body().as_bytes().expect("buffered"),
            b"hello world"
        );
    }

    #[test]
    fn service_poll_ready_reports_ready() {
        let mut service = RouterService::builder().build();
        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);
        let ready = Service::<Request>::poll_ready(&mut service, &mut cx);
        assert!(matches!(ready, Poll::Ready(Ok(()))));
    }

    #[test]
    fn streams_body_through_router() {
        use bytes::Bytes;
        use futures_util::StreamExt as _;
        use futures_util::stream;

        async fn handler(_ctx: RequestContext) -> Result<Response, EdgeError> {
            let chunks = stream::iter(vec![
                Bytes::from_static(b"chunk-one\n"),
                Bytes::from_static(b"chunk-two\n"),
            ]);

            (StatusCode::OK, Body::stream(chunks)).into_response()
        }

        let service = RouterService::builder().get("/stream", handler).build();

        let request = request_builder()
            .method(Method::GET)
            .uri("/stream")
            .body(Body::empty())
            .expect("request");

        let response = block_on(service.clone().call(request)).expect("response");
        let mut stream = response.into_body().into_stream().expect("stream body");
        let collected = block_on(async {
            let mut acc = Vec::new();
            while let Some(result) = stream.next().await {
                let chunk = result.expect("chunk");
                acc.extend_from_slice(&chunk);
            }
            acc
        });
        assert_eq!(collected, b"chunk-one\nchunk-two\n");
    }

    #[test]
    fn with_state_exposes_value_to_handler() {
        use crate::extractor::{FromRequest as _, State};

        #[derive(Clone)]
        struct Counter(u32);

        async fn handler(ctx: RequestContext) -> Result<String, EdgeError> {
            let State(counter) = State::<Counter>::from_request(&ctx).await?;
            Ok(format!("count={}", counter.0))
        }

        let service = RouterService::builder()
            .with_state(Counter(9))
            .get("/count", handler)
            .build();

        let request = request_builder()
            .method(Method::GET)
            .uri("/count")
            .body(Body::empty())
            .expect("request");

        let response = block_on(service.oneshot(request)).expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body().as_bytes().expect("buffered"), b"count=9");
    }

    #[test]
    fn with_state_last_write_wins_for_same_type() {
        use crate::extractor::{FromRequest as _, State};

        #[derive(Clone)]
        struct Counter(u32);

        async fn handler(ctx: RequestContext) -> Result<String, EdgeError> {
            let State(counter) = State::<Counter>::from_request(&ctx).await?;
            Ok(format!("count={}", counter.0))
        }

        let service = RouterService::builder()
            .with_state(Counter(1))
            .with_state(Counter(2))
            .get("/c", handler)
            .build();

        let request = request_builder()
            .method(Method::GET)
            .uri("/c")
            .body(Body::empty())
            .expect("request");

        let response = block_on(service.oneshot(request)).expect("response");
        assert_eq!(response.body().as_bytes().expect("buffered"), b"count=2");
    }

    #[test]
    fn retained_router_separates_overlapping_requests() {
        use futures::channel::oneshot;
        use std::collections::VecDeque;
        use std::future::Future as _;
        use std::str::from_utf8;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone)]
        struct Tag(&'static str);
        #[derive(Clone)]
        struct RequestToken(&'static str);
        struct Barrier(Mutex<VecDeque<oneshot::Receiver<()>>>);
        #[async_trait::async_trait(?Send)]
        impl Middleware for Barrier {
            async fn handle(
                &self,
                ctx: RequestContext,
                next: Next<'_>,
            ) -> Result<Response, EdgeError> {
                let receiver = self.0.lock().unwrap().pop_front().unwrap();
                receiver.await.unwrap();
                next.run(ctx).await
            }
        }
        #[crate::action]
        async fn probe(ctx: RequestContext) -> Result<String, EdgeError> {
            let body = ctx.body_bytes(1024).await?;
            let counter = ctx.extensions().get::<Arc<AtomicUsize>>().unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(format!(
                "{}:{}:{}:{}:{}",
                ctx.path_params().get("id").unwrap(),
                ctx.headers()["x-request-token"].to_str().unwrap(),
                from_utf8(&body).unwrap(),
                ctx.extensions().get::<RequestToken>().unwrap().0,
                ctx.extensions().get::<Tag>().unwrap().0
            ))
        }
        let (send1, recv1) = oneshot::channel();
        let (send2, recv2) = oneshot::channel();
        let counter = Arc::new(AtomicUsize::new(0));
        let router = RouterService::builder()
            .with_state(Tag("app"))
            .with_state(Arc::clone(&counter))
            .middleware(Barrier(Mutex::new([recv1, recv2].into())))
            .get("/probe/{id}", probe)
            .build();
        let make_request = |token: &'static str| {
            let mut req = request_builder()
                .uri(format!("/probe/{token}"))
                .header("x-request-token", token)
                .body(Body::from(token))
                .unwrap();
            req.extensions_mut().insert(RequestToken(token));
            req.extensions_mut().insert(Tag("request"));
            req
        };
        let mut first = Box::pin(router.oneshot(make_request("first")));
        let mut second = Box::pin(router.oneshot(make_request("second")));
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        send1.send(()).unwrap();
        send2.send(()).unwrap();
        let first_response = block_on(first).unwrap();
        let second_response = block_on(second).unwrap();
        assert_eq!(
            first_response.body().as_bytes().unwrap(),
            b"first:first:first:first:app"
        );
        assert_eq!(
            second_response.body().as_bytes().unwrap(),
            b"second:second:second:second:app"
        );
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn retained_router_releases_cancelled_request_state() {
        use std::future::{Future as _, pending};

        #[derive(Clone)]
        struct RequestResource(Arc<()>);

        #[crate::action]
        async fn wait(ctx: RequestContext) -> Result<String, EdgeError> {
            // Keep the request alive across suspension, as an awaiting handler does.
            pending::<()>().await;
            Ok(ctx.path_params().get("id").unwrap().to_owned())
        }

        #[crate::action]
        async fn probe(ctx: RequestContext) -> Result<String, EdgeError> {
            assert!(ctx.extensions().get::<RequestResource>().is_none());
            Ok(ctx.path_params().get("id").unwrap().to_owned())
        }

        let shared = Arc::new(());
        let router = RouterService::builder()
            .with_state(Arc::clone(&shared))
            .get("/wait/{id}", wait)
            .get("/probe/{id}", probe)
            .build();
        let resource = RequestResource(Arc::new(()));
        let released = Arc::downgrade(&resource.0);
        let mut request = request_builder()
            .uri("/wait/cancelled")
            .body(Body::from("private request body"))
            .unwrap();
        request.extensions_mut().insert(resource);
        let mut suspended = Box::pin(router.oneshot(request));
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(suspended.as_mut().poll(&mut cx).is_pending());
        assert!(released.upgrade().is_some());
        drop(suspended);
        assert!(released.upgrade().is_none());

        let response = block_on(
            router.oneshot(
                request_builder()
                    .uri("/probe/fresh")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .unwrap();
        assert_eq!(response.body().as_bytes().unwrap(), b"fresh");
        assert_eq!(Arc::strong_count(&shared), 2);
        drop(router);
        assert_eq!(Arc::strong_count(&shared), 1);
    }

    #[test]
    fn with_state_no_cross_request_bleed() {
        use crate::extractor::{FromRequest as _, State};
        use std::future::Future as _;

        #[derive(Clone)]
        struct Tag(&'static str);

        async fn handler(ctx: RequestContext) -> Result<String, EdgeError> {
            let State(tag) = State::<Tag>::from_request(&ctx).await?;
            Ok(tag.0.to_owned())
        }

        let service = RouterService::builder()
            .with_state(Tag("shared"))
            .get("/t", handler)
            .build();

        let req1 = request_builder()
            .method(Method::GET)
            .uri("/t")
            .body(Body::empty())
            .expect("req1");
        let req2 = request_builder()
            .method(Method::GET)
            .uri("/t")
            .body(Body::empty())
            .expect("req2");

        // Two independent in-flight requests, polled interleaved on one thread.
        let mut f1 = Box::pin(service.oneshot(req1));
        let mut f2 = Box::pin(service.oneshot(req2));
        let mut cx = Context::from_waker(noop_waker_ref());

        let mut r1 = None;
        let mut r2 = None;
        while r1.is_none() || r2.is_none() {
            if r1.is_none()
                && let Poll::Ready(value) = f1.as_mut().poll(&mut cx)
            {
                r1 = Some(value);
            }
            if r2.is_none()
                && let Poll::Ready(value) = f2.as_mut().poll(&mut cx)
            {
                r2 = Some(value);
            }
        }

        let resp1 = r1.unwrap().expect("resp1");
        let resp2 = r2.unwrap().expect("resp2");
        assert_eq!(resp1.body().as_bytes().expect("buffered"), b"shared");
        assert_eq!(resp2.body().as_bytes().expect("buffered"), b"shared");
    }

    #[test]
    fn with_state_supports_multiple_distinct_types() {
        use crate::extractor::{FromRequest as _, State};

        #[derive(Clone)]
        struct First(u32);
        #[derive(Clone)]
        struct Second(&'static str);

        async fn handler(ctx: RequestContext) -> Result<String, EdgeError> {
            let State(first) = State::<First>::from_request(&ctx).await?;
            let State(second) = State::<Second>::from_request(&ctx).await?;
            Ok(format!("{}-{}", first.0, second.0))
        }

        let service = RouterService::builder()
            .with_state(First(7))
            .with_state(Second("hi"))
            .get("/both", handler)
            .build();

        let request = request_builder()
            .method(Method::GET)
            .uri("/both")
            .body(Body::empty())
            .expect("request");

        let response = block_on(service.oneshot(request)).expect("response");
        assert_eq!(response.body().as_bytes().expect("buffered"), b"7-hi");
    }
}

#[cfg(test)]
mod pre_dispatch_tests {
    mod admitted {
        use super::*;
        use std::task::Poll;
        use std::time::Duration;

        use crate::app::App;
        use crate::ingress::{
            AdmissionDecision, BufferedIngressResponse, IngressBeginOutcome,
            IngressDispatchOutcome, IngressFraming, IngressGrant, IngressHeadAccounting,
            IngressHeadParts,
        };
        use crate::response_egress::{
            ResponseEgressCompletion, ResponseEgressEnvelope, ResponseEgressOutcome,
        };
        use crate::response_egress_framing::ResponseEgressFramingMode;

        struct CountedContinueHook {
            calls: Arc<AtomicUsize>,
            mutation: Option<bool>,
        }

        struct DropCount(Arc<AtomicUsize>);

        #[async_trait(?Send)]
        impl PreDispatchHook for CountedContinueHook {
            async fn handle(&self, request: &mut Request) -> Result<Option<Response>, EdgeError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if let Some(method) = self.mutation {
                    RoutingMutationHook {
                        method,
                        terminal: false,
                    }
                    .handle(request)
                    .await
                } else {
                    ContinueHook.handle(request).await
                }
            }
        }

        #[expect(
            clippy::missing_trait_methods,
            reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
        )]
        impl Drop for DropCount {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        fn complete(envelope: ResponseEgressEnvelope) -> Response {
            let Ok((prepared, _, mut attempt, clock)) =
                envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength)
            else {
                panic!("response egress begin");
            };
            assert!(attempt.begin_writing());
            assert!(attempt.complete(clock.now()));
            prepared.into_response()
        }

        #[test]
        fn continuation_preserves_the_admitted_route_and_rejects_routing_mutations() {
            for mutation in [None, Some(false), Some(true)] {
                let calls = Arc::new(AtomicUsize::new(0));
                let clones = Arc::new(AtomicUsize::new(0));
                let middleware = Arc::new(AtomicUsize::new(0));
                let app = App::new(
                    RouterService::builder()
                        .with_state(CountedState {
                            clones: Arc::clone(&clones),
                        })
                        .middleware(CountingMiddleware {
                            calls: Arc::clone(&middleware),
                        })
                        .pre_dispatch_hook(CountedContinueHook {
                            calls: Arc::clone(&calls),
                            mutation,
                        })
                        .get("/before", unreachable_handler)
                        .post("/after", after_handler)
                        .build(),
                );
                let (method, path) = if mutation.is_none() {
                    (Method::POST, "/after")
                } else {
                    (Method::GET, "/before")
                };
                let response =
                    complete(envelope(dispatch(&app, input(method, path, Body::empty()))));
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                if mutation.is_none() {
                    assert_eq!(response.status(), StatusCode::OK);
                    assert_eq!(response.body().as_bytes(), Some(b"continued".as_slice()));
                    assert_eq!(clones.load(Ordering::SeqCst), 1);
                    assert_eq!(middleware.load(Ordering::SeqCst), 1);
                } else {
                    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
                    assert_eq!(clones.load(Ordering::SeqCst), 0);
                    assert_eq!(middleware.load(Ordering::SeqCst), 0);
                }
            }
        }

        fn dispatch(app: &App, request: Request) -> IngressDispatchOutcome {
            block_on(app.dispatch_ingress(
                request,
                app.monotonic_now(),
                IngressHeadAccounting::HostManaged,
                IngressFraming::HostManaged,
            ))
            .expect("canonical ingress")
        }

        fn envelope(outcome: IngressDispatchOutcome) -> ResponseEgressEnvelope {
            let IngressDispatchOutcome::Response(envelope) = outcome else {
                panic!("expected response");
            };
            *envelope
        }

        #[test]
        fn refusal_and_abort_precede_hooks_without_reading_the_request_body() {
            for abort in [false, true] {
                let calls = Arc::new(AtomicUsize::new(0));
                let polls = Arc::new(AtomicUsize::new(0));
                let drops = Arc::new(AtomicUsize::new(0));
                let mut app = App::new(
                    RouterService::builder()
                        .pre_dispatch_hook(TerminalHook {
                            calls: Arc::clone(&calls),
                            fail: false,
                        })
                        .post("/after", unreachable_handler)
                        .build(),
                );
                app.set_ingress_admission_policy(move |_| {
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
                let outcome = dispatch(
                    &app,
                    input(Method::POST, "/after", unread_body(&polls, &drops)),
                );
                if abort {
                    assert!(matches!(outcome, IngressDispatchOutcome::Aborted));
                } else {
                    assert_eq!(
                        complete(envelope(outcome)).status(),
                        StatusCode::SERVICE_UNAVAILABLE
                    );
                }
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                assert_eq!(polls.load(Ordering::SeqCst), 0);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            }
        }

        #[test]
        fn terminal_hook_and_error_release_ingress_but_retain_completion_through_attempt() {
            for fail in [false, true] {
                let calls = Arc::new(AtomicUsize::new(0));
                let polls = Arc::new(AtomicUsize::new(0));
                let body_drops = Arc::new(AtomicUsize::new(0));
                let grant_drops = Arc::new(AtomicUsize::new(0));
                let completions = Arc::new(AtomicUsize::new(0));
                let completion_drops = Arc::new(AtomicUsize::new(0));
                let mut app = App::new(
                    RouterService::builder()
                        .pre_dispatch_hook(TerminalHook {
                            calls: Arc::clone(&calls),
                            fail,
                        })
                        .post("/after", unreachable_handler)
                        .build(),
                );
                let grants = Arc::clone(&grant_drops);
                let completed = Arc::clone(&completions);
                let retained = Arc::clone(&completion_drops);
                app.set_ingress_admission_policy(move |head| {
                    let owned = DropCount(Arc::clone(&retained));
                    let observed_completions = Arc::clone(&completed);
                    AdmissionDecision::Admit {
                        completion: ResponseEgressCompletion::new(move |report| {
                            assert_eq!(report.outcome, ResponseEgressOutcome::Completed);
                            observed_completions.fetch_add(1, Ordering::SeqCst);
                            drop(owned);
                        }),
                        grant: IngressGrant::new(DropCount(Arc::clone(&grants))),
                        read_deadline: head.read_deadline_after(Duration::from_secs(1)),
                    }
                });
                let request = input(Method::POST, "/after", unread_body(&polls, &body_drops));
                let head = IngressHeadParts::from_request(
                    &request,
                    IngressHeadAccounting::HostManaged,
                    IngressFraming::HostManaged,
                );
                let IngressBeginOutcome::Admitted(prepared) = app
                    .begin_ingress(head, app.monotonic_now())
                    .expect("admission")
                else {
                    panic!("expected admitted request");
                };
                let mut envelope = block_on(app.dispatch_admitted(prepared, request));
                assert_eq!(
                    envelope.response_mut().status(),
                    if fail {
                        StatusCode::BAD_REQUEST
                    } else {
                        StatusCode::IM_A_TEAPOT
                    }
                );
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(polls.load(Ordering::SeqCst), 0);
                assert_eq!(body_drops.load(Ordering::SeqCst), 1);
                assert_eq!(grant_drops.load(Ordering::SeqCst), 1);
                assert_eq!(completions.load(Ordering::SeqCst), 0);
                assert_eq!(completion_drops.load(Ordering::SeqCst), 0);
                let Ok((prepared_response, _, mut attempt, clock)) =
                    envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength)
                else {
                    panic!("response egress begin");
                };
                drop(prepared_response);
                assert!(attempt.begin_writing());
                assert_eq!(completions.load(Ordering::SeqCst), 0);
                assert_eq!(completion_drops.load(Ordering::SeqCst), 0);
                assert!(attempt.complete(clock.now()));
                assert!(!attempt.complete(clock.now()));
                assert_eq!(completions.load(Ordering::SeqCst), 1);
                assert_eq!(completion_drops.load(Ordering::SeqCst), 1);
                drop(attempt);
                assert_eq!(completions.load(Ordering::SeqCst), 1);
                assert_eq!(completion_drops.load(Ordering::SeqCst), 1);
            }
        }

        #[test]
        fn terminal_hook_runs_once_before_matched_missing_method_and_fallback_dispatch() {
            for (method, path, fallback) in [
                (Method::POST, "/after", false),
                (Method::GET, "/after", false),
                (Method::GET, "/missing", false),
                (Method::GET, "/after", true),
                (Method::GET, "/missing", true),
            ] {
                let calls = Arc::new(AtomicUsize::new(0));
                let clones = Arc::new(AtomicUsize::new(0));
                let middleware = Arc::new(AtomicUsize::new(0));
                let polls = Arc::new(AtomicUsize::new(0));
                let drops = Arc::new(AtomicUsize::new(0));
                let admissions = Arc::new(AtomicUsize::new(0));
                let mut app = App::new(
                    RouterService::builder()
                        .with_state(CountedState {
                            clones: Arc::clone(&clones),
                        })
                        .with_manifest_json("{}")
                        .middleware(CountingMiddleware {
                            calls: Arc::clone(&middleware),
                        })
                        .pre_dispatch_hook(TerminalHook {
                            calls: Arc::clone(&calls),
                            fail: false,
                        })
                        .post("/after", unreachable_handler)
                        .build(),
                );
                let admitted = Arc::clone(&admissions);
                app.set_ingress_admission_policy(move |head| {
                    admitted.fetch_add(1, Ordering::SeqCst);
                    let read_deadline = head.read_deadline_after(Duration::from_secs(1));
                    if fallback {
                        AdmissionDecision::ReadBodyBeforeFallback {
                            completion: ResponseEgressCompletion::empty(),
                            grant: IngressGrant::empty(),
                            max_body_bytes: 0,
                            read_deadline,
                            on_exceeded: BufferedIngressResponse::text(
                                StatusCode::PAYLOAD_TOO_LARGE,
                                "too large",
                            ),
                            on_timeout: BufferedIngressResponse::text(
                                StatusCode::REQUEST_TIMEOUT,
                                "late",
                            ),
                        }
                    } else {
                        AdmissionDecision::Admit {
                            completion: ResponseEgressCompletion::empty(),
                            grant: IngressGrant::empty(),
                            read_deadline,
                        }
                    }
                });
                let response = complete(envelope(dispatch(
                    &app,
                    input(method, path, unread_body(&polls, &drops)),
                )));
                assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
                assert_eq!(response.headers()["allow"], "POST");
                assert_eq!(response.headers()["www-authenticate"], "Basic");
                assert_eq!(response.body().as_bytes(), Some(b"local".as_slice()));
                assert_eq!(admissions.load(Ordering::SeqCst), 1);
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(clones.load(Ordering::SeqCst), 0);
                assert_eq!(middleware.load(Ordering::SeqCst), 0);
                assert_eq!(polls.load(Ordering::SeqCst), 0);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            }
        }

        fn unread_body(polls: &Arc<AtomicUsize>, drops: &Arc<AtomicUsize>) -> Body {
            let observed_polls = Arc::clone(polls);
            let owned = DropCount(Arc::clone(drops));
            Body::from_stream(stream::poll_fn(move |_| {
                let _retained = &owned;
                observed_polls.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(None::<Result<Bytes, EdgeError>>)
            }))
        }
    }

    use std::io::Error as IoError;
    use std::mem::take;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::executor::block_on;
    use futures::future::ready;
    use futures::stream;
    use tower_service::Service as _;

    use crate::action;
    use crate::body::Body;
    use crate::context::RequestContext;
    use crate::error::EdgeError;
    use crate::http::{Method, Request, Response, StatusCode, request_builder, response_builder};
    use crate::introspection::{ManifestJson, RouteTable};
    use crate::middleware::{Middleware, Next};
    use crate::router::{PreDispatchHook, RouterService};

    struct CountedState {
        clones: Arc<AtomicUsize>,
    }

    impl Clone for CountedState {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::SeqCst);
            Self {
                clones: Arc::clone(&self.clones),
            }
        }

        fn clone_from(&mut self, source: &Self) {
            *self = source.clone();
        }
    }

    struct CountingMiddleware {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait(?Send)]
    impl Middleware for CountingMiddleware {
        async fn handle(&self, ctx: RequestContext, next: Next<'_>) -> Result<Response, EdgeError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            next.run(ctx).await
        }
    }

    #[derive(Clone)]
    struct HookMarker;

    struct InspectHook;

    #[async_trait(?Send)]
    impl PreDispatchHook for InspectHook {
        async fn handle(&self, request: &mut Request) -> Result<Option<Response>, EdgeError> {
            let local = Rc::new(7_u32);
            let body = take(request.body_mut());
            body.into_bytes_bounded(0).await?;
            ready(()).await;
            assert_eq!(*local, 7_u32, "should support a non-Send hook future");
            Ok(Some(
                response_builder()
                    .status(204)
                    .body(Body::empty())
                    .map_err(EdgeError::internal)?,
            ))
        }
    }

    struct RoutingMutationHook {
        method: bool,
        terminal: bool,
    }

    #[async_trait(?Send)]
    impl PreDispatchHook for RoutingMutationHook {
        async fn handle(&self, request: &mut Request) -> Result<Option<Response>, EdgeError> {
            if self.method {
                *request.method_mut() = Method::POST;
            } else {
                *request.uri_mut() = "/after".parse().expect("fixed URI");
            }
            if self.terminal {
                return response_builder()
                    .status(204)
                    .body(Body::empty())
                    .map(Some)
                    .map_err(EdgeError::internal);
            }
            Ok(None)
        }
    }

    struct ContinueHook;

    #[async_trait(?Send)]
    impl PreDispatchHook for ContinueHook {
        async fn handle(&self, request: &mut Request) -> Result<Option<Response>, EdgeError> {
            assert!(request.extensions().get::<CountedState>().is_none());
            *request.uri_mut() = "/after?hook=present"
                .parse()
                .expect("should parse fixed URI");
            request.headers_mut().insert(
                "x-hook",
                "present".parse().expect("should parse fixed header"),
            );
            request.extensions_mut().insert(HookMarker);
            *request.body_mut() = Body::text("changed");
            Ok(None)
        }
    }

    struct TerminalHook {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    #[async_trait(?Send)]
    impl PreDispatchHook for TerminalHook {
        async fn handle(&self, request: &mut Request) -> Result<Option<Response>, EdgeError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(request.extensions().get::<CountedState>().is_none());
            assert!(request.extensions().get::<ManifestJson>().is_none());
            assert!(request.extensions().get::<RouteTable>().is_none());
            if self.fail {
                return Err(EdgeError::bad_request("hook rejected request"));
            }
            Ok(Some(
                response_builder()
                    .status(418)
                    .header("allow", "POST")
                    .header("www-authenticate", "Basic")
                    .body(Body::text("local"))
                    .map_err(EdgeError::internal)?,
            ))
        }
    }

    #[action]
    async fn after_handler(context: RequestContext) -> Result<Response, EdgeError> {
        assert!(context.extensions().get::<HookMarker>().is_some());
        assert!(context.extensions().get::<CountedState>().is_some());
        assert_eq!(context.headers()["x-hook"], "present");
        assert_eq!(context.uri().query(), Some("hook=present"));
        assert_eq!(
            context.body_bytes(16).await.expect("changed body").as_ref(),
            b"changed"
        );
        response_builder()
            .body(Body::text("continued"))
            .map_err(EdgeError::internal)
    }

    #[action(manifest, routes)]
    async fn unreachable_handler() -> &'static str {
        panic!("should short-circuit before handler");
    }

    fn input(method: Method, path: &str, body: Body) -> Request {
        request_builder()
            .method(method)
            .uri(path)
            .body(body)
            .expect("should build hook input")
    }

    #[test]
    fn pre_dispatch_continuation_uses_mutated_request() {
        let clones = Arc::new(AtomicUsize::new(0));
        let router = RouterService::builder()
            .with_state(CountedState {
                clones: Arc::clone(&clones),
            })
            .pre_dispatch_hook(ContinueHook)
            .post("/after", after_handler)
            .build();
        let response = block_on(router.oneshot(input(Method::POST, "/after", Body::empty())))
            .expect("should dispatch continuing request");
        assert_eq!(response.body().as_bytes(), Some(b"continued".as_slice()));
        assert_eq!(clones.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn pre_dispatch_rejects_continued_method_or_path_changes() {
        for method in [false, true] {
            let clones = Arc::new(AtomicUsize::new(0));
            let middleware_calls = Arc::new(AtomicUsize::new(0));
            let router = RouterService::builder()
                .with_state(CountedState {
                    clones: Arc::clone(&clones),
                })
                .middleware(CountingMiddleware {
                    calls: Arc::clone(&middleware_calls),
                })
                .pre_dispatch_hook(RoutingMutationHook {
                    method,
                    terminal: false,
                })
                .post("/before", unreachable_handler)
                .get("/after", unreachable_handler)
                .build();
            let error = block_on(
                router
                    .clone()
                    .call(input(Method::GET, "/before", Body::empty())),
            )
            .expect_err("routing mutation must fail before lookup");
            assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert!(
                error
                    .to_string()
                    .contains("pre-dispatch hook changed the request method or path")
            );
            let response = block_on(router.oneshot(input(Method::GET, "/before", Body::empty())))
                .expect("oneshot renders routing mutation error");
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(clones.load(Ordering::SeqCst), 0);
            assert_eq!(middleware_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn pre_dispatch_terminal_response_allows_method_or_path_changes() {
        for method in [false, true] {
            let router = RouterService::builder()
                .pre_dispatch_hook(RoutingMutationHook {
                    method,
                    terminal: true,
                })
                .build();
            let response = block_on(router.clone().call(input(
                Method::GET,
                "/before",
                Body::empty(),
            )))
            .expect("terminal response precedes mutation guard");
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
            let oneshot_response =
                block_on(router.oneshot(input(Method::GET, "/before", Body::empty())))
                    .expect("terminal response through oneshot");
            assert_eq!(oneshot_response.status(), StatusCode::NO_CONTENT);
        }
    }

    #[test]
    fn pre_dispatch_errors_stop_routing_in_both_service_apis() {
        let clones = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let router = RouterService::builder()
            .with_state(CountedState {
                clones: Arc::clone(&clones),
            })
            .pre_dispatch_hook_arc(Arc::new(TerminalHook {
                calls: Arc::clone(&calls),
                fail: true,
            }))
            .post("/after", after_handler)
            .build();
        let error = block_on(
            router
                .clone()
                .call(input(Method::POST, "/after", Body::empty())),
        )
        .expect_err("should propagate hook error from Service");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        let response = block_on(router.oneshot(input(Method::POST, "/after", Body::empty())))
            .expect("should render hook error from oneshot");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(clones.load(Ordering::SeqCst), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn pre_dispatch_hook_is_shared_across_router_clones() {
        let calls = Arc::new(AtomicUsize::new(0));
        let router = RouterService::builder()
            .pre_dispatch_hook_arc(Arc::new(TerminalHook {
                calls: Arc::clone(&calls),
                fail: false,
            }))
            .build();
        for service in [router.clone(), router] {
            let response = block_on(service.oneshot(input(Method::GET, "/missing", Body::empty())))
                .expect("should reuse shared hook");
            assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn pre_dispatch_inspects_empty_stream_without_send_bound() {
        let router = RouterService::builder()
            .pre_dispatch_hook_arc(Arc::new(InspectHook))
            .build();
        let chunks = stream::iter([Ok::<Bytes, EdgeError>(Bytes::new()), Ok(Bytes::new())]);
        let response =
            block_on(router.oneshot(input(Method::POST, "/unknown", Body::from_stream(chunks))))
                .expect("should inspect clean streamed EOF");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[test]
    #[should_panic(expected = "pre-dispatch hook already registered")]
    fn pre_dispatch_registration_rejects_duplicate_hook() {
        let _builder = RouterService::builder()
            .pre_dispatch_hook_arc(Arc::new(InspectHook))
            .pre_dispatch_hook(ContinueHook);
    }

    #[test]
    fn pre_dispatch_rejects_nonempty_or_failed_streams() {
        let router = RouterService::builder()
            .pre_dispatch_hook_arc(Arc::new(InspectHook))
            .build();
        let chunks = stream::iter([Ok::<Bytes, EdgeError>(Bytes::from_static(b"x"))]);
        let response =
            block_on(router.oneshot(input(Method::POST, "/unknown", Body::from_stream(chunks))))
                .expect("should render nonempty body rejection");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let failure = stream::iter([Err::<Bytes, EdgeError>(EdgeError::internal(
            IoError::other("fixture failure"),
        ))]);
        let failure_response =
            block_on(router.oneshot(input(Method::POST, "/unknown", Body::from_stream(failure))))
                .expect("should render stream failure");
        assert_eq!(failure_response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn pre_dispatch_terminal_response_precedes_method_path_and_lifecycle() {
        let calls = Arc::new(AtomicUsize::new(0));
        let clones = Arc::new(AtomicUsize::new(0));
        let middleware_calls = Arc::new(AtomicUsize::new(0));
        let router = RouterService::builder()
            .with_state(CountedState {
                clones: Arc::clone(&clones),
            })
            .with_manifest_json("{}")
            .middleware(CountingMiddleware {
                calls: Arc::clone(&middleware_calls),
            })
            .pre_dispatch_hook_arc(Arc::new(TerminalHook {
                calls: Arc::clone(&calls),
                fail: false,
            }))
            .post("/after", unreachable_handler)
            .build();
        for (method, path) in [
            (Method::POST, "/after"),
            (Method::GET, "/after"),
            (
                Method::from_bytes(b"EXAMPLE-METHOD").expect("should parse extension method"),
                "/after",
            ),
            (Method::GET, "/missing"),
        ] {
            let response = block_on(router.oneshot(input(method, path, Body::empty())))
                .expect("should intercept before method/path lookup");
            assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
            assert_eq!(response.headers()["allow"], "POST");
            assert_eq!(response.headers()["www-authenticate"], "Basic");
            assert_eq!(response.body().as_bytes(), Some(b"local".as_slice()));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(clones.load(Ordering::SeqCst), 0);
        assert_eq!(middleware_calls.load(Ordering::SeqCst), 0);
    }
}

# Routing

EdgeZero uses `matchit` 0.8+ for high-performance path matching with support for parameters and catch-all segments.

## Defining Routes

Routes are typically defined in your `edgezero.toml` manifest and wired automatically via the `app!` macro:

```toml
[[triggers.http]]
id = "hello"
path = "/hello"
methods = ["GET"]
handler = "my_app_core::handlers::hello"

[[triggers.http]]
id = "echo"
class = "interactive"
path = "/echo/{name}"
methods = ["GET", "POST"]
handler = "my_app_core::handlers::echo"
```

An optional `class` is opaque application metadata for ingress admission. It is available through
`RouteMetadata::class()` for matched and method-not-allowed resolutions and does not change the
route's stable identity.

You can also build routes programmatically using convenience methods:

```rust
use edgezero_core::router::RouterService;

let router = RouterService::builder()
    .get("/hello", hello_handler)
    .get("/echo/{name}", echo_handler)
    .post("/echo", echo_json_handler)
    .build();
```

Or with explicit method specification:

```rust
use edgezero_core::router::RouterService;
use edgezero_core::http::Method;

let router = RouterService::builder()
    .route("/hello", Method::GET, hello_handler)
    .route_with_class("/echo/{name}", Method::GET, "interactive", echo_handler)
    .route("/echo", Method::POST, echo_json_handler)
    .build();
```

## Path Parameters

Define parameters with `{name}` segments:

```rust
use edgezero_core::action;
use edgezero_core::extractor::Path;
use edgezero_core::response::Text;

#[action]
async fn greet(Path(name): Path<String>) -> Text<String> {
    Text::new(format!("Hello, {}!", name))
}
```

For routes like `/users/{id}/posts/{post_id}`, extract multiple parameters:

```rust
#[derive(serde::Deserialize)]
struct PostParams {
    id: u64,
    post_id: u64,
}

#[action]
async fn get_post(Path(params): Path<PostParams>) -> Text<String> {
    Text::new(format!("User {} Post {}", params.id, params.post_id))
}
```

## Catch-All Segments

Use `{*rest}` for catch-all routes that match any remaining path:

```rust
// Route: /files/{*path}
// Matches: /files/docs/readme.md -> path = "docs/readme.md"

#[action]
async fn serve_file(Path(path): Path<String>) -> Text<String> {
    Text::new(format!("Serving: {}", path))
}
```

## HTTP Methods

Specify allowed methods in your route definition:

```toml
[[triggers.http]]
path = "/resource"
methods = ["GET", "POST", "PUT", "DELETE"]
handler = "my_app_core::handlers::resource"
```

Or programmatically:

```rust
RouterService::builder()
    .get("/resource", get_resource)
    .post("/resource", create_resource)
    .put("/resource/{id}", update_resource)
    .delete("/resource/{id}", delete_resource)
    .build()
```

EdgeZero automatically returns `405 Method Not Allowed` for requests that match a path but use an
unsupported method. The response includes an `Allow` header containing the sorted, deduplicated
method set registered for that path.

## Introspection Routes

EdgeZero provides three bindable handlers in `edgezero_core::introspection` for debugging and runtime inspection:

- **`manifest`**: Returns the full manifest JSON with secret values redacted.
- **`config`**: Returns the effective app config from the default config store, with secret fields appearing as unresolved key-name references (secret-safe).
- **`routes`**: Returns the registered route table as `[{method, path}]`.

Bind them in your manifest's `[[triggers.http]]` like any handler. By default, generated apps and app-demo mount them under `/_<app-name>/{manifest,config,routes}`:

```toml
[[triggers.http]]
id = "manifest"
path = "/_my-app/manifest"
methods = ["GET"]
handler = "edgezero_core::introspection::manifest"

[[triggers.http]]
id = "config"
path = "/_my-app/config"
methods = ["GET"]
handler = "edgezero_core::introspection::config"

[[triggers.http]]
id = "routes"
path = "/_my-app/routes"
methods = ["GET"]
handler = "edgezero_core::introspection::routes"
```

::: warning Security
These endpoints are unauthenticated wherever they are bound — restrict access at the network or middleware layer before exposing them publicly. Note that `/manifest` emits `environment.variables[].value` verbatim; only `environment.secrets` values are redacted. Do not store secrets in `[environment.variables]`.
:::

The `routes` handler returns JSON like:

```json
[
  { "method": "GET", "path": "/_my-app/manifest" },
  { "method": "GET", "path": "/_my-app/config" },
  { "method": "GET", "path": "/_my-app/routes" }
]
```

## Path Syntax

EdgeZero uses matchit's path syntax:

| Pattern     | Example          | Matches                      |
| ----------- | ---------------- | ---------------------------- |
| `/static`   | `/static`        | Exact match only             |
| `/{param}`  | `/users/{id}`    | Single segment: `/users/123` |
| `/{*catch}` | `/files/{*path}` | Rest of path: `/files/a/b/c` |

::: warning Legacy Syntax
Axum-style `:name` parameters are **not supported**. Use `{name}` instead.
:::

## Route Priority

Routes are matched by specificity (static segments first, then parameters, then catch-alls).
Registering two routes that conflict for the same method (for example, two routes that both look
like `/users/{id}`) panics while the routes are registered (inside `App::build::<A>(platform)`) with
`duplicate route definition for <path>`.
When that happens depends on the adapter: Axum builds the router once at startup, so the conflict
surfaces before any traffic, but the Fastly, Cloudflare, and Spin entry points call
`App::build::<A>(platform)` per request, so a conflicting route deploys successfully and then panics on
the first request.
Run the app under the Axum dev server before deploying to an edge target.

## Intercepting before dispatch

Use an optional `PreDispatchHook` to intercept an admitted request before
handler, middleware or router-generated 404/405 fallback execution. Canonical
adapter dispatch resolves the route during admission, before the hook. The hook
cannot bypass admission or change that route selection. Direct
`RouterService::call` and `RouterService::oneshot` instead perform their route
lookup after the hook; they are not substitutes for canonical adapter admission.
The hook receives a protected `PreDispatchRequest` before router state and
introspection injection. Its future can be non-`Send`.

```rust
use async_trait::async_trait;
use edgezero_core::body::Body;
use edgezero_core::error::EdgeError;
use edgezero_core::http::{Response, response_builder};
use edgezero_core::PreDispatchRequest;
use edgezero_core::router::{PreDispatchHook, RouterService};

struct ExampleHook;

#[async_trait(?Send)]
impl PreDispatchHook for ExampleHook {
    async fn handle(&self, request: &mut PreDispatchRequest) -> Result<Option<Response>, EdgeError> {
        if request.headers().contains_key("x-example-stop") {
            return Ok(Some(
                response_builder()
                    .status(204)
                    .body(Body::empty())
                    .map_err(EdgeError::internal)?,
            ));
        }
        Ok(None)
    }
}

let router = RouterService::builder()
    .pre_dispatch_hook(ExampleHook)
    .build();
```

`Ok(None)` continues with modified headers, extensions, bounded body and URI
query. The method and URI path must remain unchanged; changing either returns
an internal error before handler, middleware or fallback execution; canonical
admission has already selected the route. Modified heads are checked again against
the original ingress limits and normalized framing rules. Use `body_bytes(max)`
for cached bounded reads, `transform_body(input_max, output_max, transform)` for
buffered transformations, or `map_body(output_max, transform)` for synchronous
per-chunk transformations. Raw body replacement is not exposed. Original input
limits, read deadlines and sticky failures survive transformations, header and
extension edits. A fallback input overflow or timeout retains its configured response
even if the hook ignores the read error. `Ok(Some(response))` skips router state,
introspection, middleware, handlers and fallback draining when no body-policy failure
has occurred. An unread terminal hook never polls the body. In canonical dispatch,
the response still uses the admitted response-egress policy and coordinator.
Return a complete response when exact challenge or hardening headers matter.
`Err(error)` stops dispatch; direct `Service::call` propagates it,
while `RouterService::oneshot` renders it through the existing error response
conversion.

Register a value with `pre_dispatch_hook`, or an existing shared hook with
`pre_dispatch_hook_arc`. A second registration panics during router construction.
Router clones share the same hook instance.
Without a hook, existing routing is unchanged. Admission, lazy request-body
ownership, capability bootstrap and response egress retain their adapter-owned
boundaries; the hook does not replace those layers.

The hook value must be `Send + Sync + 'static`; it cannot hold `Rc` state,
although its returned future may be non-`Send`. Adapter extensions such as
`RequestIngress`, platform context and store registries are available. Router
state, introspection and `RequestContext` have not been injected yet:

```rust
let ingress = request.extensions()
    .get::<edgezero_core::ingress::RequestIngress>();
```

If the hook reads the body and returns `Ok(None)`, the handler receives the shared
cached bytes, or the bounded transformed body. Sticky read or transform failures
cannot be cleared by continuing. Generated `app!` applications
cannot install a hook through the macro or modify the built router. A
hand-written `Hooks` implementation does not inherit the generated metadata.
Move your existing `app!` invocation **unchanged, including all arguments**
(`owns_logging = …`, `state = …`), into a separate module. Delegate the baked
manifest, stores, logging, name and application configuration. For example, an application whose
existing invocation uses `owns_logging = true, state = 42_u32` would keep them.
If the invocation has no app name, scaffolded projects use
`app!("../../edgezero.toml")`, which defines `App`; import it as
`use manifest_app::App as ManifestApp;` instead of the import below.

When combining this with the hook example above, omit its illustrative
`let router = …build();` statement and duplicate `RouterService` import:

```rust
use edgezero_core::app::{App, Hooks, StoresMetadata};
use edgezero_core::manifest::{BakedManifest, ResolvedLoggingConfig};
use edgezero_core::router::RouterService;
use std::sync::{Arc, LazyLock};

mod manifest_app {
    edgezero_core::app!(
        "edgezero.toml", ManifestApp,
        owns_logging = true, state = 42_u32
    );
}
use manifest_app::ManifestApp;

static MANIFEST_JSON: LazyLock<Arc<str>> = LazyLock::new(|| {
    Arc::from(ManifestApp::manifest_json().expect("manifest application"))
});

pub struct HookedApp;

impl Hooks for HookedApp {
    fn routes() -> Result<RouterService, EdgeError> {
        Ok(RouterService::builder()
            .pre_dispatch_hook(ExampleHook)
            .with_manifest_json(Arc::clone(&MANIFEST_JSON))
            .with_state(42_u32)
            // Re-register every manifest route and middleware here, in order.
            .build())
    }

    fn manifest() -> BakedManifest { ManifestApp::manifest() }
    fn manifest_json() -> Option<&'static str> { ManifestApp::manifest_json() }
    fn stores() -> StoresMetadata { ManifestApp::stores() }
    fn logging_for(adapter: &str) -> ResolvedLoggingConfig {
        ManifestApp::logging_for(adapter)
    }
    fn owns_logging() -> bool { ManifestApp::owns_logging() }
    fn name() -> &'static str { ManifestApp::name() }
    fn configure(app: &mut App) -> Result<(), EdgeError> {
        ManifestApp::configure(app)
    }
}
```

This outline uses `ExampleHook` from above, with code in `src/lib.rs` and
`edgezero.toml` at the crate root. Add a direct `async-trait` dependency:
core does not re-export that crate. The `app!` path is relative to the crate
root. The wrapper delegates `manifest()` and `manifest_json()` so adapter
environment resolution and introspection keep the original baked contract.
Import any names used by the original arguments into the nested module (for
example, `use super::app_state;` for `state = app_state()`).
Point every adapter entry point (`run_app::<…>`, `serve_app::<…>`,
`dispatch_app::<…>`, their `_with_*` variants, and any
`App::build::<…>(platform)` call inside a custom `serve_custom`/`run_custom`
callback) at `HookedApp`. Construct manual applications with
`App::build::<HookedApp>(platform)?`, preserving fallible assembly and platform
validation. Its `routes()` must explicitly
re-register **all routes, manifest middleware and configured state**; delegating
metadata alone does not preserve them. `with_manifest_json` above preserves
manifest introspection. The lazy static wraps the compile-time manifest JSON
in an `Arc` once per runtime instance, without runtime TOML parsing.
Cloudflare isolates and reused Spin P3 instances share this cached JSON. Axum's `run_app` and Fastly's `serve_app` build the application once, so
`routes()` already runs once there. Classic Fastly Compute starts a fresh
instance per request and pays the initialization cost each time. EdgeZero's
spin-sdk 7 `#[http_service]` entry points export WASI P3 HTTP components.
[Spin 4.1 defaults](https://github.com/spinframework/spin/blob/v4.1.0/crates/trigger-http/src/lib.rs)
allow up to 128 requests per instance and 16 concurrent requests, controlled by
`--max-instance-reuse-count` and `--max-instance-concurrent-reuse-count`.
Fresh instances still pay the initialization cost; reuse is not guaranteed.
The macro embeds its JSON at compile time and exposes it through
`Hooks::manifest_json()`. Registering hooks directly through `app!` is tracked in
[issue #434](https://github.com/IABTechLab/edgezero/issues/434).
This manual duplication must stay synchronized with `app!` and the manifest,
including matching `.with_state(...)` to the original macro's state argument.
The complete executable example and regression tests are in
`crates/edgezero-macros/tests/hooked_app.rs`. The store-free ingress fixture is
not a metadata-preserving example for manifest applications.

Consult [adapter ingress capabilities](./adapters/overview#inbound-request-fidelity)
before classifying a request using its target or duplicate headers.

## Next Steps

- Learn about [Handlers & Extractors](/guide/handlers) for processing requests
- Explore [Middleware](/guide/middleware) for cross-cutting concerns

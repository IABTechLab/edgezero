# Adapter Overview

Adapters bridge provider-specific HTTP primitives into EdgeZero's portable model. This document defines the contract that all adapters must fulfill.

## Goals

Adapters translate provider-specific HTTP primitives into the portable `App` in `edgezero-core`. They must:

- Preserve request semantics
- Stream responses without buffering where the provider supports it
- Expose provider context
- Inject the portable outbound HTTP client without exposing provider SDK types to handlers

## Request Conversion

Canonical adapter dispatch normalizes the provider request head, performs admission,
and only then consumes the body under the admitted limits and deadline. Public raw
conversion helpers are not a common adapter contract or a substitute for this lifecycle.
The admitted conversion must:

- **Preserve the HTTP method** exactly (`GET`, `POST`, etc.)
- **Parse the full URI** (path and query string) into an `http::Uri`. Reject invalid URIs with `EdgeError::bad_request`
- **Copy all headers** into the core request. Provider-specific headers may be filtered only when they clash with platform defaults
- **Consume the request body** into an `edgezero_core::body::Body` only after admission, enforcing the selected size limit and absolute read deadline
- **Insert a provider context struct** (e.g., `FastlyRequestContext`) into the request extensions. The context should expose metadata such as client IP addresses or environment handles so handlers can reach platform APIs

## Response Egress

Adapters consume a `ResponseEgressEnvelope` and own delivery through the strongest platform
transport boundary available. Implementations must:

- **Map HTTP status codes** verbatim
- **Copy headers**, respecting casing rules enforced by the provider
- **Preserve lazy body production** and platform backpressure without whole-response collection
- **Use the application clock and one absolute write deadline** through terminal observation
- **Account accepted payload bytes** at the adapter's documented host boundary
- **Abort or close owned transport resources** and deliver exactly one terminal report
- **Use the bounded precommit fallback** without replacing a response after commit

## Dispatch Helper

Adapters surface a send-owning runner that bridges from the provider event loop into the shared
router. It should:

1. Normalize the incoming provider request head and run the app's admission policy
2. Consume the admitted body and await canonical app dispatch, or deliver the detached admission response
3. Carry the returned egress envelope into the adapter coordinator
4. Own body delivery, deadline/abort handling, and terminal observation
5. Map precommit failures to the bounded fallback and expose only category-safe diagnostics

Generated and demo entrypoints call only the canonical runner for their adapter.

## Store Registry Resolution

All four adapters resolve KV, config, and secret stores from the portable
`Hooks::stores()` metadata baked by the `app!` macro plus `EDGEZERO__*`
environment variables (see [the migration guide](../manifest-store-migration.md)
for the schema change). Each adapter builds a per-request
`StoreRegistry<H>` keyed by logical id; handlers reach a bound store via
the id-keyed `Kv` / `Secrets` / `Config` extractors or the matching
`ctx.kv_store(id)` / `ctx.config_store(id)` / `ctx.secret_store(id)`
accessors. The pre-rewrite `Hooks::config_store()` hook is gone.

## Outbound HTTP Integration

Adapters implement `edgezero_core::outbound::OutboundHttpClient` and inject an `HttpClient` into
each core request. Implementations must:

- Implement `time`, synchronous `validate_supported_options`, `dispatch_head`, eager `begin`,
  and prepared `start_batch_until`. Core owns application-facing `send` and ordered collection.
- Consume `OutboundDispatchRequest` without restarting its captured budget. Return an
  `OutboundHead` with original adapter-visible headers, lazy encoded bytes and a teardown owner;
  core owns normalization, provenance, decoding, body disposition and bounded finalization.
- Schedule complete begin/batch exchanges before returning their receivers. Driver events report
  original indices, actual terminal observations and outcomes, not consumer-created receipts.
- Enforce independent request, encoded response, decoded response, final buffer, header, Brotli, and chunk-shape controls
- Map typed cache-bypass and validated wire-authority policy without changing URI-owned routing,
  TLS SNI, or certificate identity
- Preserve typed deadline, transport, protocol, codec, and resource failures without message matching
- Preserve the prepared context's paired clock/timer and exactly-once ownership through response
  handoff. Successful streamed collection must not cancel its live payload.
- Attach `x-edgezero-proxy: <adapter>` by default; configure relay handles with
  `HttpClient::with_proxy_marker(false)` or override an individual request with
  `OutboundProxyMarker::Off`. An omitted synthetic marker consumes no header budget and does not
  overwrite an upstream marker.
- Publish an exact static capability level for every outbound capability

Provider limitations are part of the contract rather than hidden implementation details. See
[Capabilities](/guide/capabilities) for the support matrix, timing semantics, and accounting
exclusions. Consumers upgrading platform resource or response construction APIs should use the
[Unreleased migration table](https://github.com/IABTechLab/edgezero/blob/main/CHANGELOG.md#platform-resource-metadata).

## Logging Initialisation

Fastly wires `log_fastly`, and Axum uses `simple_logger`. Their runners initialize logging
before application configuration. Cloudflare and Spin `init_logger` helpers are no-ops: host
console or stdout/stderr capture does not install a Rust `log` backend.

`owns_logging = true` skips adapter logger setup completely. Install your backend **before**
entering the adapter runner, apply the configured runtime filter to that backend, and set the
facade maximum high enough for all enabled targets. `edgezero_core::resolve_logging_level(&env)`
resolves the supplied platform environment's runtime level (invalid or absent means `Info`);
it neither installs a logger nor reads manifest defaults implicitly.

Managed Fastly and Axum backends filter `edgezero_core::BOOT_LOG_TARGET` (`edgezero::boot`) at
`BOOT_LOG_LEVEL` (`Warn`) independently of ordinary records. Consequently boot warnings and
errors still appear with ordinary logging set to `Error` or `Off`. Boot `Info`/`Debug` messages
do not appear. The target is not a physical Fastly endpoint. Application-owned loggers choose
their own boot policy; raising `log::set_max_level` alone cannot restore backend-filtered records.

## Contract Tests

To keep the contract enforceable, each adapter includes tests that validate request conversion,
response framing, and the owned delivery coordinator:

- canonical admission and request conversion for method, URI, header, body, and context propagation
- response-head commit and lazy streamed body writes
- absolute deadlines, accepted-byte accounting, abort/close, and exactly-once terminal reports
- routing, admission refusal, fallback drain, handler, middleware, and fallback response paths

### Fastly Tests

Because the Fastly SDK links against the Compute@Edge host functions, the contract tests compile only for `wasm32-wasip1`. Run them with:

```bash
rustup target add wasm32-wasip1
cargo install viceroy --locked
export CARGO_TARGET_WASM32_WASIP1_RUNNER="viceroy run"
cargo test -p edgezero-adapter-fastly --features fastly --target wasm32-wasip1 --test contract
```

Fastly's SDK-linked test binaries need Viceroy for execution; plain Wasmtime
does not provide the required `fastly_*` host imports.

### Cloudflare Tests

Cloudflare's adapter relies on `wasm32-unknown-unknown`. Its contract and
outbound runtime unit suites use `wasm-bindgen-test` under Node with the Workers
runtime shims:

```bash
rustup target add wasm32-unknown-unknown
export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner
cargo test -p edgezero-adapter-cloudflare --features cloudflare --target wasm32-unknown-unknown --test contract
```

Install a `wasm-bindgen-cli` version that matches the workspace's `wasm-bindgen`
entry in `Cargo.lock` before running the Cloudflare tests. Browser ingress
conversion tests are a separate target, not a browser mode for the Node suites:

```bash
HEADLESS=1 cargo test -p edgezero-adapter-cloudflare --features cloudflare,test-utils --target wasm32-unknown-unknown --test ingress_fidelity
```

CI pins Chrome and its matching driver and configures the browser binary through
`WASM_BINDGEN_TEST_WEBDRIVER_JSON`. Local workerd transport probes remain
separate from both suites.

### Spin Tests

Spin's adapter targets `wasm32-wasip2` and its contract suite runs under Wasmtime:

```bash
rustup target add wasm32-wasip2
export CARGO_TARGET_WASM32_WASIP2_RUNNER="wasmtime run -W component-model-async=y -S p3=y -S http=y"
cargo test -p edgezero-adapter-spin --features spin --target wasm32-wasip2 --test contract
```

The `wasmtime` version CI uses is pinned in `.tool-versions`.

## Onboarding New Adapters

When bringing up another adapter:

1. **Implement request conversion and an owned response coordinator** that follow the rules above
2. **Provide a context type** exposing the adapter's metadata and insert it during admitted request conversion
3. **Implement a send-owning runner** plus logging helper
4. **Wire up an `OutboundHttpClient`** with limits, deadlines, batching, and typed errors
5. **Copy the contract test suite**, swapping in the new adapter types. Ensure the tests are gated to the target architecture if the adapter SDK does not compile for native hosts
6. **Register the adapter** with `edgezero-adapter::register_adapter` (typically in a `cli` module using the `ctor` crate) so the CLI can discover it dynamically. To take part in `provision` and `config push`, override the relevant `Adapter` trait hooks: `single_store_kinds` and `merged_id_kinds` declare the platform's store shape, `validate_adapter_manifest` checks the adapter's own manifest, `provision` creates platform resources, and `push_config_entries` plus `read_config_entry` move config blobs. `single_store_kinds` and `merged_id_kinds` default to empty, `validate_adapter_manifest` defaults to accepting, `provision` defaults to a no-op, and the config push and read hooks default to reporting the operation as unsupported

Adapters that fulfil these steps can be dropped into the EdgeZero CLI without requiring changes to application code.

## Available Adapters

| Adapter                                  | Platform            | Target                   | Status |
| ---------------------------------------- | ------------------- | ------------------------ | ------ |
| [Fastly](/guide/adapters/fastly)         | Fastly Compute@Edge | `wasm32-wasip1`          | Stable |
| [Cloudflare](/guide/adapters/cloudflare) | Cloudflare Workers  | `wasm32-unknown-unknown` | Stable |
| [Spin](/guide/adapters/spin)             | Fermyon Spin        | `wasm32-wasip2`          | Stable |
| [Axum](/guide/adapters/axum)             | Native (Tokio)      | Host                     | Stable |

### Store Capabilities

`single_store_kinds` lists the kinds that allow only one declared id; `merged_id_kinds`
lists the kinds that share one underlying platform resource, so declaring the same
logical id under both is a collision that `config validate` rejects.

| Adapter    | Single-store kinds | Merged kinds   | Config GC | Staging lifecycle |
| ---------- | ------------------ | -------------- | --------- | ----------------- |
| Fastly     | none               | none           | Yes       | Yes               |
| Cloudflare | `secrets`          | `kv`, `config` | No        | No                |
| Spin       | `secrets`          | `kv`, `config` | No        | No                |
| Axum       | `secrets`          | none           | No        | No                |

Fastly is the only adapter implementing `gc_config_entries` and the staging lifecycle
actions (`DeployStaging`, `EmitVersion`, `Healthcheck`, `Rollback`); the others return
an unsupported error for those.

## Inbound request fidelity

Adapters insert `edgezero_core::ingress::RequestIngress` into request extensions.
It records runtime-visible target and adapter-supplied origin facts separately from the
primary URI and headers. Missing metadata means unknown capability.

`CapturedTarget::Complete` holds the whole captured string, its acquisition
source and preservation status. Capture has a 16 KiB UTF-8 byte bound; oversize
input becomes `Unavailable(TooLarge)`, without retaining a prefix. Fastly's
current safe API reports `Unavailable(NotExposed)`. Metadata has borrowed
getters and intentionally has no value-bearing `Debug`, `Display` or
serialization implementation. Do not log or serialize captured values.

`InboundOrigin` validates an HTTP(S) scheme and an authority of at most 1 KiB,
including IPv6 and port syntax. Parsing validates syntax; the adapter still
establishes provenance. Runtime URI facts use `RuntimeUri`; Axum's plain HTTP
listener uses `TransportBinding` plus one consistent Host. Origin-form Axum
conversion without that binding has no established origin. Ordinary Origin,
Forwarded and Spin special headers do not establish ingress origin.
Authorities retain source-provided spelling, including host case and explicit
default ports and leading-zero ports when exposed. Syntax validation accepts
port zero without establishing a usable destination. Runtime authority usually
reflects the client's Host or :authority; adapter provenance does not establish
operator approval, ownership or an allowlist. Compare configured origins after
normalizing host case and effective ports.

Fastly records target unavailability before reading its SDK runtime URL for
origin, and requires a client request with one consistent validated Host.
This recovers the validated runtime origin without claiming an original path.

`HeaderFidelity` has separate octet, field-multiplicity, same-name-order and
global-order axes, with per-name overrides. They concern original received
fields, excluding HTTP framing and header-name spelling. Faithful copying from
a runtime HeaderMap does not establish original-wire preservation. Unproved
axes remain `Unknown`. Axum, Cloudflare and Spin report
`HeaderFidelity::RUNTIME_HEADER_MAP`: octets, multiplicity and same-name order
are `Unknown`, and global order is `Unavailable`. Fastly reports all four axes
as `Unknown`, with no per-name overrides. Its original-header names/count APIs
expose partial evidence, but do not associate original fields with converted
values. Aggregate count equality cannot prove per-name multiplicity, and full
global order is not established. Production verification remains pending.
The matrix below shows observed runtime behavior, not `RequestIngress` guarantees.

The table records the historical observations imported from main
`87456c70`, checked into
`crates/edgezero-test-harness/fixtures/request-fidelity/observations.json`;
`./scripts/test_integration.sh --adapter all --suite ingress-fidelity` compares against those expectations
exactly. The versions below identify that historical record, not a run of the
merged candidate. Rerun the probes after integration and record the actual
dependency sources and runtime identities before citing new evidence. Neither
the historical record nor a local regression run establishes production-edge
behavior.

| Surface                             | Fastly 0.13.1 / Viceroy 0.21.0                                       | worker 0.8.5 / workerd 1.20260415.1                                                       | Axum / Hyper 1.11.1                                                  | Spin SDK 7.0.0 / Spin 4.1.0                                          |
| ----------------------------------- | -------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Pre-dispatch hook                   | Executed                                                             | Executed for supported incoming methods                                                   | Executed                                                             | Executed                                                             |
| Runtime extension method conversion | Byte method preserved                                                | Web Request converter preserves tokens; local wire parser returns 501 for unknown methods | Preserved in tested TCP fixture                                      | Preserved in tested HTTP fixture                                     |
| Target metadata                     | Not exposed by safe SDK API                                          | Runtime URL; literal and encoded dot spellings normalize                                  | Runtime URI; tested spelling retained                                | Runtime URI; tested spelling retained                                |
| High-byte Cookie                    | FF retained in tested fixture                                        | Local wire FF becomes U+FFFD before Rust and is copied as runtime UTF-8 EF BF BD          | FF retained in tested TCP fixture                                    | Runtime returns 500 before component for invalid UTF-8               |
| Repeated control fields             | Tested ordinary duplicates retained; identical Content-Length folded | Fetch combines fields; original counts cannot be recovered; common fidelity Unknown       | Tested ordinary duplicates retained; identical Content-Length folded | Tested ordinary duplicates retained; identical Content-Length folded |
| Global wire order                   | Not established                                                      | Unavailable                                                                               | Unavailable                                                          | Unavailable                                                          |

Run `./scripts/test_integration.sh --adapter all --suite ingress-fidelity` for local transport probes. They
assert pinned observations, including unsupported outcomes; a green regression
run does not turn an unsupported capability into a preservation guarantee.
Fastly raw-target support requires a safe borrowed SDK accessor and new ingress
evidence. Consumers that require original dot spellings or duplicate fields
must separately resolve Fetch normalization/coalescing and platform rejection
limits. Consumer authentication and cookie policy remain application responsibilities.

Canonical adapter admission resolves the route before the
[pre-dispatch hook](../routing#intercepting-before-dispatch). The hook then runs
before router state, introspection, request-context injection, middleware,
handlers and 404/405 fallback execution. Request-body access remains lazy under
the admitted limits and deadline, and terminal hook responses retain canonical
response-egress ownership. Store/capability bootstrap remains adapter-owned.
Direct router APIs instead perform route lookup after the hook; they do not
perform canonical adapter admission.

## Opt-in application retention

Use retention when measurements show that constructing the app is expensive.
Keep per-request construction when `configure` or `app_state` reads values that
can change and no refresh policy has been defined.

Application ownership and request scheduling are separate. An `App` can be
retained by its caller; each adapter still controls how requests arrive:

| Adapter    | Existing default                         | Explicit retention                                                                         |
| ---------- | ---------------------------------------- | ------------------------------------------------------------------------------------------ |
| Fastly     | Build for each single-request invocation | `serve_app` / `serve_app_with_hooks` with SDK `Serve`, backed by `lifecycle::Sandbox<App>` |
| Cloudflare | Build on each fetch                      | Concrete application-owned cache and `dispatch_app`                                        |
| Spin       | Build on each invocation                 | Concrete cache and `dispatch_app` on a compatible host                                     |
| Axum       | Build once at server startup             | Already retains the app and router                                                         |

Hook-based assembly uses `App::build::<A>(PLATFORM) -> Result<App, EdgeError>`.
`Hooks::routes` is fallible: its error skips configuration and is returned unchanged.
Applications that have built their own router use `App::from_router::<A>(router, PLATFORM)`
to apply the same name, configuration and platform check, without rebuilding routes.
Both paths install platform metadata before calling fallible `Hooks::configure` and reject
successful configuration that changes that metadata. `App::state::<T>()` borrows the
router's registered state, also available during configuration; retrieve an `Arc<S>`
registration as that exact type. Retain only a successfully
configured app; never cache an initialization error or publish a partial app.
Macro state expressions can use `state = load_state()?` under the same fallible assembly contract.
Serverless standard entrypoints and custom `request::send_assembly_error` helpers use canonical
detached delivery for failed assembly, preserving typed status without using any partially
configured app callbacks. Fastly retained serving retries after successful error delivery;
Axum instead fails startup before binding. See [assembly failures](/guide/handlers#configuring-the-application-lifecycle).
Cloudflare uses `dispatch_app::<A>(&app, req, env, ctx)`; Spin uses
`dispatch_app::<A>(&app, req)` and returns a raw `SpinResponse`. Neither builds an
app nor initializes logging. The caller must pair the app with its construction
`Hooks` type and selected platform.

Retain owned settings, parsed objects, and bounded caches only when their
staleness policy is acceptable. Keep native request handles, bodies, store
registries, pending operations, and response effects request-local. `Send + Sync`
and cloning do not prove host-resource validity. Shared `Arc` interiors remain
shared; the framework does not reset them between requests. Registered app state
continues to overwrite request extensions of the same type.

Every dispatch creates fresh admission state and response-egress ownership in
addition to fresh registries. Fastly's preparation/finalization hooks borrow
request-local values; the adapter owns sending and lazy streaming. Retention does
not expose a response-returning Fastly dispatch path or add response buffering.

Applications own refresh, invalidation, key rotation, initialization-failure
policy, and workload benchmarks. Publish complete snapshots; overlapping requests
keep the snapshot they acquired. Never put an unkeyed static in a generic cache
function: that static would be shared between application types. Existing macros,
manifest settings, and generated entry points retain their lifecycle defaults.
The Cloudflare response-header correction applies to both default and retained apps.

### Preparing an application for retention

Audit the values captured by handlers, middleware, and registered state before
opting in. The framework creates fresh request resources but cannot inspect or
clear application-owned interiors.

| Risk                                       | Application mitigation                                                                                                                                                                                                                             |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Request data survives into another request | Store identity, authorization results, bodies, and correlation IDs in request-local values or extensions. Use distinct types for request data and registered state; registered state wins on a type collision.                                     |
| Settings or parsed keys become stale       | Keep them request-scoped until a refresh policy is defined. For retained values, build and validate a complete replacement snapshot, then publish it atomically. Decide whether refresh failure keeps the last valid snapshot or rejects requests. |
| Caches grow without bound                  | Limit entries and retained bytes, expire or evict entries, and bound origin diversity. A sandbox request limit cannot bound allocation within one request.                                                                                         |
| Concurrent requests mutate shared state    | Synchronize mutations and acquire an immutable snapshot per request. Release locks before awaiting provider work. Rust's `Send + Sync` bounds do not establish application-level isolation.                                                        |
| Initialization or required bindings fail   | Surface the error and monitor repeated fresh-instance failures. In custom dispatch, convert a recoverable failure into one response only when the application defines that recovery policy. Bound retries, e.g. with Fastly `Serve` limits.        |
| Logging is installed twice                 | Choose one owner. With the Fastly helper, set `Hooks::owns_logging()` when application code installs the logger. First-snapshot logging is intentionally fixed; use custom dispatch for another policy.                                            |
| A stream fails after commitment            | Finish or drop the streaming writer and settle request-owned pending work. Log the partial-response failure; do not attempt a replacement response after sending headers.                                                                          |

Exercise cancellation as well as successful requests. The core regression tests
drop a suspended request, verify its extension resources are released, and serve
a fresh request through the same router. This does not cancel application-spawned
background tasks or prove cleanup after a terminating provider trap.

Roll out retention only after the application's isolation, refresh, and bounded
memory workload checks pass. Provider eviction can force initialization on any
request, so correctness must not depend on reaching the configured reuse limit.

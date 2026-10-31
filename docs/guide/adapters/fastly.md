# Fastly Compute@Edge

Deploy EdgeZero applications to Fastly's Compute@Edge platform using WebAssembly.

## Prerequisites

- [Fastly CLI](https://developer.fastly.com/learning/compute/#install-the-fastly-cli)
- Rust `wasm32-wasip1` target: `rustup target add wasm32-wasip1`
- [Viceroy](https://github.com/fastly/Viceroy) 0.21.0 for local execution and testing

The adapter pins the Fastly SDK to 0.13.1. Older Viceroy releases may reject its
host imports before application code executes; use the repository's pinned runtime.

## Project Setup

When scaffolding with `edgezero new my-app`, the Fastly adapter includes:

```
crates/my-app-adapter-fastly/
├── Cargo.toml
├── fastly.toml
└── src/
    └── main.rs
```

### fastly.toml

The Fastly manifest configures your service:

```toml
manifest_version = 3
name = "my-app"
language = "rust"
authors = ["you@example.com"]

[local_server]
  [local_server.backends]
    [local_server.backends."origin"]
    url = "https://your-origin.example.com"
```

`edgezero provision --adapter fastly` creates each declared store and writes a
`[setup.kv_stores]`, `[setup.secret_stores]` or `[setup.config_stores]` entry
into `fastly.toml`, keyed by the store's logical ID. It writes that entry only
when the physical store name equals the logical ID. A different physical name
needs a selected service (`service_id` in `fastly.toml` or `FASTLY_SERVICE_ID`),
and provision leaves `fastly.toml` unchanged for that store. Whenever a service
is selected, provision prints the `fastly service resource-link create` command
that links each new store under its logical ID, because an existing service does
not re-run `[setup]`. Provision leaves the Viceroy-only `[local_server.*]` tables
alone. The config-store stanzas there are written by your generated app CLI,
`<app-cli> config push --adapter fastly --local` (the bundled `edgezero` binary
has no typed app config and exits 2), and KV / secret local-server seeding is
hand-edited.

### Entrypoint

The Fastly entrypoint wires the adapter:

```rust
use my_app_core::App;

pub fn main() -> Result<(), fastly::Error> {
    edgezero_adapter_fastly::run_app::<App>()
}
```

Do not add Fastly's response-returning entrypoint attribute. EdgeZero initializes the ABI,
receives the client request, and owns `stream_to_client` through final body-handle close.

`run_app` builds per-id `KV` / `Config` / `Secret` registries from the portable
store metadata baked into `App` by the `app!` macro. Logging settings are baked
in at the same time. Managed deployments resolve `EDGEZERO__*` store selectors
and link each physical resource under its logical ID; neither `edgezero.toml`
nor the deployment environment is read by the runtime. See
[the migration guide](../manifest-store-migration.md).
If route/state assembly or `Hooks::configure` fails, `run_app`, `run_app_with_hooks` and
`run_app_with_config` deliver the error's status and standard JSON body through canonical
detached egress. They release the unread request, skip preparation/finalization and registries,
and log the full failure once on `edgezero::boot`. Internal details stay off the wire. No failed
app's renderer, headers or callbacks apply; the finite default write deadline starts before assembly.

For fully manual wiring, build `FastlyService`, attach stores as needed, and call its send-owning
`send()` method. Prefer `run_app` for manifest-driven store resolution.

Fastly store `__NAME` selectors are deployment inputs. The runtime opens logical
aliases directly, and Config Stores use the logical ID as the entry key. Remove
runtime selector-store reads and Fastly `__KEY` overrides when migrating a custom
entrypoint; use the canonical send-owning helper below. Logging endpoint, level
and stdout behavior come from `Hooks::logging_for("fastly")` and cannot vary
between staging and production for the same immutable application release.

### Capturing raw-request signals (JA4, H2 fingerprint)

`run_app` receives and converts the `fastly::Request` into a neutral core request before
dispatch. The client IP is carried across automatically; read it via
`FastlyRequestContext` (see [Context Access](#context-access) below). Other
Fastly-only signals that are readable only on the raw request
(`get_tls_ja4()`, `get_client_h2_fingerprint()`) aren't reachable from handlers
by default. Use `run_app_with_hooks`, which runs a request closure against a
scratch `Extensions` **before** conversion and merges the values into the core
request. Its response closure runs only for routed responses before egress
policy and framing; EdgeZero retains delivery ownership:

```rust
#[derive(Clone)]
struct Ja4(String);

pub fn main() -> Result<(), fastly::Error> {
    edgezero_adapter_fastly::run_app_with_hooks::<App, _, _, ()>(
        |raw, ext| {
            if let Some(ja4) = raw.get_tls_ja4() {
                ext.insert(Ja4(ja4.to_owned()));
            }
        },
        |_response| (),
    )
    .map(|_post_send_state| ())
}
```

The request closure runs once before admission for each request that reaches
preparation; insert whatever typed values your handlers need, then read them via a custom extractor or
`ctx.extensions().get::<Ja4>()`. The optional state returned by
`run_app_with_hooks` is available only after EdgeZero reaches its strongest
terminal delivery boundary. Detached admission responses skip the response
closure and return `None`.

For fully manual wiring, `FastlyService::send_request_with_hooks` provides the
same closed lifecycle for an already-received request. It does not expose a
response-returning dispatch operation.

### Owning your own logging

By default `run_app` initializes the Fastly logger when the baked
`[adapters.fastly.logging]` table sets an `endpoint`. If your app already installs
a `log` backend, opt out with the platform-neutral `Hooks::owns_logging()` flag —
via the `app!` macro:

```rust
edgezero_core::app!("edgezero.toml", owns_logging = true);
```

or on a hand-written `Hooks` impl (`fn owns_logging() -> bool { true }`). Every
adapter's `run_app` honors it. The app must install its backend **before** entering the runner,
apply `Hooks::logging_for("fastly").level`, and set both backend and facade filters. Raising only
`log::set_max_level` does not override `log_fastly`'s internal filter.

## Building

Build for Fastly's Wasm target:

```bash
# Using the CLI
edgezero build --adapter fastly

# Or directly
fastly compute build -C crates/my-app-adapter-fastly
```

The compiled Wasm binary is placed in `target/wasm32-wasip1/release/`.

## Local Development

Run locally with Viceroy (Fastly's local simulator):

```bash
# Using the CLI
edgezero serve --adapter fastly

# Or directly
fastly compute serve -C crates/my-app-adapter-fastly
```

This starts a local server at `http://127.0.0.1:7676`.

## Deployment

Deploy a verified application release with the adapter-managed lifecycle:

```bash
EDGEZERO_MANIFEST="$RELEASE_ROOT/edgezero.toml" \
edgezero deploy --adapter fastly \
  --service-id "$FASTLY_SERVICE_ID" \
  --application-release "$RELEASE_ROOT"
```

The release fixes the package, `edgezero.toml`, and its referenced Fastly
manifest before deployment selects physical resource links. A bare
`edgezero deploy --adapter fastly` supports store-free production deployments
using a manifest command. Staging and any deployment that
declares a Config, KV, or Secret Store require the verified release-backed
managed command above.

## Ingress Body Conversion

Direct request conversion and admitted dispatch preserve actual SDK body absence:
`try_take_body() == None` becomes core `Body::empty()` without constructing an SDK body.
Every `Some(body)` stays on the existing lazy stream/read-deadline path, even when
`known_length()` reports zero. The admitted path transfers the body after the prepare hook,
preserving SDK-prefetched read bytes and pending writes. Conversion does not infer emptiness
from headers or hints, repack the SDK body, read it or flush it.

Actual WASI SDK tests demonstrate that prefetched read bytes and pending writes can contain
payload despite zero host length. Host length is not EOF even beyond SDK buffering: Viceroy
uses Hyper exact size hints, not a terminal-body signal. A present body is never discarded
on that basis. An absent body remains Initial, not Cached, in the request context; core's first
`body_bytes` read-deadline check is unchanged and can still return 408. Ingress framing remains
`HostManaged`.

Normal `from_client()` requests carry a body handle even with absent framing or `Content-Length: 0`.
The shared facade now settles those present GET/HEAD sources to EOF or the first nonempty byte
before backend setup. It preserves the original outbound budget and inbound 408 precedence.
The synchronous inbound host read remains non-preemptible, so its read deadline is BestEffort;
post-return checks are not prompt interruption. Demo and generated proxy handlers share this path.
No capability cell or SDK/runtime pin is promoted.

## Backends

Outbound HTTP uses deterministic **dynamic backends** derived from the canonical target,
TLS identity, and provider timer budget. You do not predeclare those destinations in
`fastly.toml`, but dynamic backends must be enabled on the deployed Fastly service. The local CLI
cannot prove that service entitlement, so `outbound-http` is BestEffort. A disabled service
returns a typed 502 with an enablement diagnostic.

`context::FastlyOutboundObservation::current()` exposes the current Fastly serving callback's
registration statistics. The same handle is installed in request extensions before `prepare`;
preparation cannot replace it with a handle from another request. Read `snapshot()` for actual
registration attempts, distinct eligible configurations, total registration duration and longest
registration duration. Cache hits count as configurations, not registrations. Failed and late
registration attempts count; unsupported or already-expired preflight does not.

Measurements bracket the provider registration call using the client's paired application clock.
Calls lasting at least 50 ms emit a fixed warning without URLs, keys or provider diagnostics.
A backwards clock preserves the transport outcome and emits a fixed warning instead of inventing
a duration; a frozen clock contributes zero elapsed time. These observations do not bound host
blocking or certify registration latency.

The numeric handle is `Clone + Send + Sync`; snapshots are immutable and `Copy`. Retaining a
handle does not retain requests, configuration keys, scopes or SDK work. Callback exit freezes
its record, even if an accepted response body survives. Nested helpers and response cleanup do
not freeze an active outer callback; a standalone helper owns only its fallback record.
Outside EdgeZero's Fastly callback/helper lifecycle, `current()` returns `None`.

Fastly also has documented deadline, upload, elastic-budget, batch-isolation, and lazy downstream
streaming limitations. EdgeZero owns the low-level `stream_to_client` lifetime and writes portable
response chunks without whole-body collection. Synchronous source polls and hostcalls cannot be
preempted, so downstream streaming and response-egress guarantees remain BestEffort. See
[Capabilities](/guide/capabilities) before marking a capability required.

## Logging

Fastly uses endpoint-based logging. Configure logging in `edgezero.toml`:

```toml
[adapters.fastly.logging]
endpoint = "stdout"
level = "info"
echo_stdout = true
```

Managed Fastly logging keeps the `edgezero::boot` target at `Warn` while ordinary records use
the configured level. Both the SDK and facade allow boot warnings even at ordinary `Off`;
raising the facade maximum alone is insufficient. `edgezero::boot` is a logical target, not a
Fastly endpoint name. Initialize logging before application configuration.

To initialize logging manually, call `init_logger` with a provisioned endpoint:

```rust
use edgezero_adapter_fastly::init_logger;
use log::LevelFilter;

fn main() {
    init_logger("edgezero-logs", LevelFilter::Info, true).expect("init logger");
}
```

::: tip Logging status
Fastly logging is wired when you call `init_logger`, or when `run_app` finds an `endpoint` in the baked `[adapters.fastly.logging]` table and `owns_logging()` is `false`; otherwise no logger is installed.
:::

## Store selection and deployment

Fastly Compute applications open Config, KV, and Secret Stores by resource-link
name. EdgeZero bakes the logical IDs declared in `edgezero.toml` into the package.
A managed deployment resolves these optional deployment selectors:

```text
EDGEZERO__STORES__CONFIG__<ID>__NAME
EDGEZERO__STORES__KV__<ID>__NAME
EDGEZERO__STORES__SECRETS__<ID>__NAME
```

Each selected physical store is linked to the unpublished target version under
the stable logical `<ID>` alias. An absent `__NAME` defaults to `<ID>`; a present
blank or invalid value fails before provider mutation. The same logical ID can be
used independently by Config, KV, and Secret Stores because link identity is the
pair `(resource kind, logical ID)`.

Config keys are deterministic on Fastly: production, staging, and local Viceroy
all read `<ID>`. The selected Environment chooses the physical store through
`__NAME`; the same name shares config and different names isolate it. A
conflicting `__KEY` or `--key` fails before a write. Logging is resolved from
`[adapters.fastly.logging]` when the package is built.

Before publication, EdgeZero:

1. verifies the immutable release package and manifests;
2. resolves complete Config, KV, and Secret Store inventories;
3. selects the exact active, staged, or initialized-draft source;
4. uploads the verified package to an unreachable draft;
5. replaces declared links whose selected physical resource changed and creates
   missing declared links under their logical aliases;
6. preserves links not declared by the application;
7. re-reads the exact links, source state, draft state, and provider-visible
   package identity; and
8. stages or activates the prepared version without another EdgeZero mutation.

Production and staging can select different physical resources while deploying
identical package bytes. Secret Stores remain optional. Links the application
does not declare are preserved.

### Declaring and using stores

Declare portable logical IDs in `edgezero.toml`:

```toml
[stores.config]
ids = ["app_config"]

[stores.kv]
ids = ["cache"]

# Optional: omit this table when the app uses no Secret Store.
[stores.secrets]
ids = ["credentials"]
```

For local Viceroy tests, expose the Config Store under its logical ID and
write the production key under that store. Local Viceroy uses the production
key because it has no Fastly staging publication state:

```toml
[local_server.config_stores.app_config]
format = "inline-toml"

[local_server.config_stores.app_config.contents]
greeting = "hello from config store"
```

Handlers read values through the `Config` extractor or
`ctx.config_store(id)`:

```rust
async fn handler(config: Config) -> Result<Response, EdgeError> {
    let store = config
        .named("app_config")
        .ok_or_else(|| EdgeError::service_unavailable("no `app_config`"))?;
    let greeting = store.get("greeting").await?.unwrap_or_default();
    // …
}
```

If a configured store link is missing, the adapter logs a one-time warning
and retains a typed failing declared binding, rather than reporting intentional absence.
Raw values are shared `ConfigValue` UTF-8 payloads; use `as_str()` to borrow them.
Migrating from `name`/`adapters.*`?
See [the migration guide](../manifest-store-migration.md).
See [the GitHub Actions guide](../deploy-github-actions.md) for the immutable-release workflow.

## Context Access

Access Fastly-specific APIs via the request context extensions:

```rust
use edgezero_core::context::RequestContext;
use edgezero_adapter_fastly::context::FastlyRequestContext;

async fn handler(ctx: RequestContext) -> Result<Response, EdgeError> {
    // Access Fastly context from extensions
    if let Some(fastly_ctx) = ctx.extensions().get::<FastlyRequestContext>() {
        let client_ip = fastly_ctx.client_ip;
        // ...
    }

    // ...
}
```

## Streaming

Fastly provides `stream_to_client`, but that API is incompatible with the standard
response-returning SDK entrypoint. Generated applications use EdgeZero's undecorated, send-owning
entrypoint instead. EdgeZero commits the response head through the raw Fastly ABI, writes each
portable body chunk to the streaming body handle with short-write accounting, and closes or
abandons that handle exactly once. A blocked synchronous source poll or hostcall cannot be
preempted, and a failed consuming `finish` call leaves no handle to abandon; those limits keep the
response-egress capabilities at BestEffort.

See the [Streaming guide](/guide/streaming) and
[capability matrix](/guide/capabilities#outbound-matrix) for the exact boundary.

## Testing

Run contract tests for the Fastly adapter:

```bash
cargo install viceroy --version 0.21.0 --locked
export CARGO_TARGET_WASM32_WASIP1_RUNNER="viceroy run"

# Run tests
cargo test -p edgezero-adapter-fastly --features fastly --target wasm32-wasip1 --test contract
```

Fastly SDK-linked Wasm binaries require Viceroy for execution; plain Wasmtime
does not provide the `fastly_*` host imports needed by the adapter tests.

::: tip Local Execution
If Viceroy reports native certificate or keychain errors on macOS, use `--no-run`
locally and rely on Linux CI for execution.
:::

## Manifest Configuration

Configure the Fastly adapter in `edgezero.toml`. See [Configuration](/guide/configuration) for the full manifest reference.

## Reusing a sandbox and retaining an app

Opt in with an ordinary `main` in place of the single-request `run_app` call:

```rust
use edgezero_adapter_fastly::{Serve, serve_app};
use std::time::Duration;

fn main() -> Result<(), fastly::Error> {
    serve_app::<MyApp>(
        Serve::new()
            .with_max_requests(10)
            .with_timeout(Duration::from_millis(500)),
    ).into_result()
}
```

`serve_app_with_hooks` additionally accepts a preparation hook
`FnMut(&mut fastly::Request, &mut Extensions)` and a finalization hook
`FnMut(&mut edgezero_core::Response) -> State`. Preparation runs before admission;
finalization runs only for routed responses, before egress policy and framing.
Detached admission responses skip finalization. The retained helper discards the
finalizer's state after delivery; use the request-level send helper in a custom
callback when post-send work needs that state.

The first request initializes logging and attempts to build the app through
`App::build::<MyApp>(FASTLY_PLATFORM) -> Result<App, EdgeError>`. Each request constructs
fresh store registries using deployed logical aliases. Successful app construction
and `configure` execute once per sandbox; explicit work elsewhere still executes
whenever the application calls it. Both hooks and their captures are retained for
the whole serving loop; the supplied native request, extensions, and response are
request-local. Create request-specific mutable data inside the preparation hook
or store it in those extensions.

Failed route/state or configuration attempts leave the retained slot empty. `serve_app` and
`serve_app_with_hooks` deliver the same detached error response as the single-request helpers;
successful failure-response delivery keeps serving, and the next request retries assembly.
They never retain an error-response router. A custom `serve_custom` callback can use
`request::send_assembly_error(req, error, request_start, clock)` without an App and inspect its
owned `FastlyRequestDelivery::Detached` report. Capture the start with that clock before assembly.
The helper never invokes preparation/finalization, configured rendering, headers, policy or an
app observer. A terminal delivery failure retains the ordinary stop/error behavior; it does
not authorize an automatic native resend.

Logging uses the immutable baked settings, unless the app owns logging. Setup
completes once per sandbox and does not reconfigure filters on later callbacks.
Applications requiring another initialization policy should use
`lifecycle::serve_custom` and `setup_once`. Store-open diagnostics use the bounded
boot channel after logger setup; an absent warning is not proof of a healthy binding.

SDK 0.13.1 exposes `with_max_requests`, `with_timeout`, `with_max_lifetime`, and
`with_max_memory`. A value of zero disables the request-count or memory limit;
it does not request zero callbacks or zero memory use. The lifetime limit is
measured from `Serve` construction, including time before the first callback.
`with_timeout` bounds only the idle wait for the next request; it does not bound
handler or backend time. If your service is billed by memory × wall-clock time,
idle waiting is billed too. Limits do not guarantee reuse; any request may start a
fresh sandbox. The fixtures pin standalone Viceroy 0.21.0; any observed per-guest
request ceiling belongs to that runtime and run, not to the SDK request limit.
`edgezero serve` runs `fastly compute serve`, whose bundled Viceroy can differ.
Record the executable and observed reuse; recheck local ceilings when pins
change. Neither local runtime establishes deployed behavior.
Lifetime and memory checks occur between callbacks and cannot interrupt
blocked application work. Before using a local memory limit, verify that the
guest's memory-snapshot call succeeds: an unsupported snapshot conservatively
ends the SDK loop. CPU clock observations are not reliable cross-run benchmarks.

`ServeSummary::requests()` counts attempted callbacks, including a failed one;
a crash can prevent a final summary. Handler `EdgeError`s render as responses
and serving continues when adapter delivery succeeds. A returned setup, required-store,
admission-abort, or terminal-delivery error ends the loop and remains in its summary.
The lifecycle wrapper records the callback result without asking the SDK to send a
synthetic 500, including when a response was already committed. Ingress and egress
policies may produce their own error responses; an escaping error is not proof that
any response was sent. Constructor panics remain sandbox failures.

### Custom dispatch and streaming

The standard helpers preserve core response streams without whole-body collection.
For custom initialization or post-send work, use `lifecycle::serve_custom` with a
configured `Serve` builder and a callback
`FnMut(fastly::Request, &mut lifecycle::Sandbox<T>) -> Result<(), E>`. EdgeZero owns
the retained slot and successful-only initialization; your callback chooses the
application-owned `T`. Initialize lazily so health checks can bypass expensive
construction.

For an EdgeZero response, call `request::send_request_with_registries_and_hooks`
with the app, store metadata, received request, preparation hook,
and finalization hook. It runs canonical admission, injects fresh `Kv`, `Config`,
`Secrets`, and `AppConfig` registries, then owns framing, streaming, deadlines, and
terminal delivery. Preparation borrows a mutable native request and extension bag;
finalization borrows a mutable routed core response. Neither hook returns a
response for the callback to send separately. The hard-cut result is
`delivery::FastlyRequestDelivery<State>`; inspect its disposition and commitment
before deciding whether to send a native response or perform post-send work.
The same result is returned by `send_with_registries_and_hooks` and
`FastlyService::send_request_with_hooks`. Standard `run_app_with_hooks`,
`serve_app_with_hooks`, and `FastlyService::send` keep their existing result and
stop-on-error policy; successful fallback delivery remains successful there.

- `NoAttempt(failure)`: no egress attempt, completion, observer or host send occurred.
  `failure.reason()` distinguishes typed required-store failures from request-head
  conversion (`InvalidUri` or `RequestBuilder`). Public messages and Debug output
  contain no request, store or provider text. Standard serving privately retains
  its original error and source.
- `Aborted`: application policy intentionally selected no response. Never replace it.
- `Detached(delivery)`: an attempted framework/admission response skipped finalization.
- `Routed { state, delivery }`: retains finalizer state even when framing, fallback,
  source or transport delivery fails. Terminal delivery is not necessarily successful.

`FastlyResponseDelivery::report()` is the exact terminal `ResponseEgressReport`
already supplied to completion and any applicable observer. Its body kind, source
and framing observations, outcome, and fallback disposition describe the attempt.
`error()` separately identifies the last delivery failure: a successful fallback
can retain the original conversion/deadline cause in the report without a delivery
error. `commitment()` describes the **last selected body**, not confirmed client
delivery: `NotCommitted` means its head never reached the host send; `Unknown`
means the host rejected the send; `Committed` means the host accepted the head.

`may_send_native_response()` permits a replacement only for `NoAttempt` or confirmed
`NotCommitted`, never policy abort, unknown commitment or committed failure. For an
attempted response, its terminal notification has already happened; any native
replacement is outside EdgeZero accounting. Pre-admission rejections and refusals
exclude the global observer; an admitted source-error response also reports
`Detached` but retains the application's observer. These reports do not strengthen
provider capabilities.

The manual response lifetime remains adapter-owned: append every header value,
commit once through `stream_to_client`, account accepted body bytes, and finish or
abandon the body handle exactly once. A native response constructed directly by
your callback, such as a health reply, must instead be explicitly sent there;
manual native streams must be pumped, flushed, and finished before returning.
The lifecycle wrapper never supplies an extra response. Returning `Err` terminates
serving without a synthetic send, even after commitment. Return `Ok(())` only when
your policy permits continued serving after cleanup. Complete pending backend and
post-send operations before returning. Do not cache native store handles with the app.

Use `Request::get_client_request_id()` for request correlation. `FASTLY_TRACE_ID`
describes the sandbox and must not be treated as a unique request ID. If a native
ID is unavailable, generate a request-local fallback and label its source. Do
not retain correlation fields in app state or global logger configuration.

### Custom lifecycle contract

The custom path uses public APIs: `lifecycle::{Sandbox, serve_custom, run_custom}`
and `request::send_request_with_registries_and_hooks`. EdgeZero owns the
successful-only retained slot and delegates request reception and limits to the
SDK. The callback owns delivery: it either explicitly sends a native response or
calls the adapter's closed send helper. The serving wrapper passes only the
terminal `Result<(), E>` to the SDK without a synthetic response. Applications
own setup, state, refresh policy, hooks, and post-send work. There is no
response-returning conversion or whole-body compatibility path.

Follow this order inside a callback:

1. Explicitly send lightweight health responses before expensive initialization.
2. Resolve baked logging with `FastlyLogging::from(MyApp::logging_for("fastly"))`.
   Use `sandbox.setup_once` to guard successful logger setup independently from
   app construction. Failed setup leaves the guard unset; partial side effects
   are not rolled back, so retries must be safe. The closed send helper installs fresh,
   request-local logical KV bindings after logger setup, without opening unused stores.
   First use opens once per request; failed declarations retain a typed handle rather than
   failing KV-free routes. Explicit `FastlyService::require_kv()` remains fail-before-dispatch.
3. Call `sandbox.initialize` with `App::build::<MyApp>(FASTLY_PLATFORM)` for a core
   app, mapping its typed error at the adapter boundary. Success fills the slot;
   failure leaves it empty and returns the error unchanged. Read the value
   with `state()`. A handled failure may send a 503 and return `Ok(())` to permit
   a later callback to retry. Propagating it with `?` ends the loop without a synthetic send.
4. Pass the received request to `send_request_with_registries_and_hooks`. Its
   `FnOnce` preparation hook may mutate the native request and portable extensions
   before admission; its `FnOnce` finalizer may inspect response extensions and
   mutate a routed response before egress. Keep bodies, extensions, registries,
   and native handles request-local. Registered app state still overwrites
   extensions of the same type.
5. Inspect the owned terminal result before using finalizer state or starting post-send
   work. Detached responses skip finalization; routed state survives delivery failure.
   Schedule application follow-up only for an application-body result selected by your
   policy, not a delivered fallback. A committed application failure can still carry
   the request-local client needed for follow-up; complete that work before returning.
   Never send the core response again. A delivery error alone does not authorize replacement.
6. Complete pending backend and post-send work before returning. Constructors
   and callbacks that panic remain sandbox failures.

For example, after health checks, this policy permits an in-sandbox setup retry:

```rust
if let Err(error) = sandbox.setup_once(|| install_application_logger()) {
    eprintln!("logger setup failed: {error}");
    fastly::Response::from_status(503).send_to_client();
    return Ok(());
}
sandbox.initialize(|| App::build::<MyApp>(FASTLY_PLATFORM).map_err(fastly::Error::from))?;
// Read sandbox.state() and call the closed send helper; do not send its response again.
// The ? above deliberately terminates this sandbox if application construction fails.
```

A complete callback looks like this. `serve_custom` returns a `ServeSummary`
that you inspect or convert with `into_result()`. `App::build` is fallible and
installs `FASTLY_PLATFORM` before application configuration:

```rust
use edgezero_adapter_fastly::lifecycle::{self, Sandbox};
use edgezero_adapter_fastly::delivery::{FastlyRequestDelivery, FastlyResponseCommitState};
use edgezero_adapter_fastly::{
    FASTLY_PLATFORM, FastlyLogging, Serve, init_logger, request,
};
use edgezero_core::app::{App, Hooks};
use my_app_core::App as MyApp;

fn main() -> Result<(), fastly::Error> {
    lifecycle::serve_custom(Serve::new().with_max_requests(10), handle).into_result()
}

fn handle(req: fastly::Request, sandbox: &mut Sandbox<App>) -> Result<(), fastly::Error> {
    if req.get_path() == "/health" {
        fastly::Response::from_status(200).send_to_client();
        return Ok(());
    }
    let stores = MyApp::stores();
    sandbox.setup_once(|| {
        let logging = FastlyLogging::from(MyApp::logging_for("fastly"));
        if logging.use_fastly_logger && !MyApp::owns_logging() {
            init_logger(
                logging.endpoint.as_deref().expect("configured endpoint"),
                logging.level,
                logging.echo_stdout,
            )?;
        }
        Ok::<(), fastly::Error>(())
    })?;
    sandbox.initialize(|| App::build::<MyApp>(FASTLY_PLATFORM).map_err(fastly::Error::from))?;
    let app = sandbox.state().expect("initialized above");
    let result = request::send_request_with_registries_and_hooks(
        app,
        stores,
        req,
        |_raw, _extensions| {},
        |response| response.status(),
    );
    // Custom reuse policy: keep serving after a committed failure, without replacement.
    match result {
        FastlyRequestDelivery::NoAttempt(failure) => {
            // Permitted native replacement: no EdgeZero response was attempted.
            fastly::Response::from_status(503).send_to_client();
            Err(fastly::Error::msg(failure.message()))
        }
        FastlyRequestDelivery::Aborted => Err(fastly::Error::msg("admission aborted")),
        FastlyRequestDelivery::Detached(delivery)
        | FastlyRequestDelivery::Routed { delivery, .. } => {
            if let Some(error) = delivery.error() {
                log::error!("{}", error.message());
                if delivery.commitment() == FastlyResponseCommitState::Committed {
                    return Ok(());
                }
                // Never replace Unknown or Committed, including a failed body stream.
                if delivery.commitment() == FastlyResponseCommitState::NotCommitted {
                    fastly::Response::from_status(503).send_to_client();
                }
                return Err(fastly::Error::msg(error.message()));
            }
            Ok(())
        }
        _ => Err(fastly::Error::msg("unsupported delivery disposition")),
    }
}
```

The fixture's [`custom_dispatch`](https://github.com/IABTechLab/edgezero/blob/main/crates/edgezero-test-harness/fixtures/reusable-app/crates/fixture-fastly/src/lib.rs)
is a fuller reference covering logger setup, handled initialization failure,
response finalization, adapter-owned progressive streaming and post-send work.
It verifies retained-client follow-up after a committed application-source failure
and suppresses application follow-up when a framing rejection selects fallback.
The `test-utils` module `delivery::testing` can supply synthetic dispatch/delivery
outcomes to the shared hook finisher; it does not simulate SDK head conversion,
host commitment or transport. Runtime fixtures establish those separate boundaries.

`Sandbox::requests()` counts attempted callbacks, including early health responses
and failed ones. `initialization_attempts()` counts only actual builder calls.
These counters describe this sandbox and are part of the public lifecycle API.
They do not establish client completion or provider reuse.

Use `lifecycle::run_custom(Request::from_client(), callback)` for the single-request
branch, initializing the Fastly ABI before receiving that request. It creates
fresh state without entering `Serve` and returns the callback's `Result<(), E>`
directly, without asking the SDK to render an error. Use an ordinary `fn main`;
the response-returning SDK entrypoint attribute does not own this delivery lifecycle.

Keep adapter, core and SDK versions aligned with the pinned checkout. Run the
lifecycle fixtures against the checkout being adopted:

```sh
./scripts/test_integration.sh --adapter fastly --suite smoke
```

See the [fixture README](https://github.com/IABTechLab/edgezero/tree/main/crates/edgezero-test-harness/fixtures/reusable-app)
for assertions, runtime requirements and evidence files. An unavailable runtime
or missing reuse is unverified and exits 2. Local fixtures do not guarantee
provider reuse, deployed resource lifetimes or application workload gains.
Applications still own refresh, key rotation and bounded caches.

Warning caches persist too. Their bounded recent-name sets can evict entries,
so warnings can recur; the number of warnings logged is not a failure count.
Dynamic-backend capacity is service-wide, and registrations may wait for capacity
(see Fastly's [dynamic-backend limit behavior](https://www.fastly.com/documentation/reference/compute/errors)).
A sandbox request limit alone does not bound origin diversity or request fan-out.

The local fixtures are in `crates/edgezero-test-harness/fixtures/reusable-app`. Local reuse and streaming
results do not establish deployed eviction frequency, endpoint-handle validity,
resource accounting, or performance. Named endpoint delivery must be verified
separately from echoed stdout. Roll back by restoring the original single-request
entry point and removing retained state, not merely setting a request limit of one.

## Early ingress snapshots

Call `request::capture_request_ingress(&req)` immediately after acquiring the
native request and before URL-based shortcuts or native mutation. It borrows
the same request without round-tripping handles. With Fastly SDK 0.13.1,
safe raw-target acquisition is unavailable: the snapshot records `NotExposed`
first, then uses the SDK runtime URL only to recover the canonical origin.
This URL access can normalize the path; it never establishes original-target
preservation. Client requests need one valid Host consistent with the runtime
authority, including equivalent default ports. Origin provenance is
`RuntimeUri`; absent TLS metadata does not establish an HTTP scheme. Synthetic
requests and missing, duplicate, invalid or inconsistent Host have no origin.

The canonical send-owning path captures ingress before its raw-request
preparation closure. Use `run_app_with_hooks` or
`request::send_request_with_registries_and_hooks` for native preparation without
losing that earlier snapshot. The latter preserves manifest store IDs through
logical resource-link aliases and uses each logical config store ID as its
entry key. Scratch extensions cannot replace the captured ingress metadata.
A standalone snapshot does not add a response-returning dispatch API or
independently establish the provenance of caller-supplied metadata.

The historical Viceroy observations imported from main `87456c70` retain tested
ordinary repeated fields and FF header octets. Rerun the ingress fixture on the
merged candidate before treating these observations as current-run evidence.
Identical repeated Content-Length fields fold into one; conflicting lengths are
rejected with 400 before application dispatch. These parser observations do not
establish preservation for every incoming request. The probes also
demonstrate that a normalized native shortcut can receive
`/reserved/../native` as `/native`. The router hook cannot intercept a shortcut
taken earlier. Original-target support and safe shortcut exclusion remain
blocked on the SDK accessor; client/TLS preservation must be proved on the
appropriate ingress. See [the capability matrix](./overview#inbound-request-fidelity).

Fastly exposes `get_original_header_names()` and `get_original_header_count()`.
These provide original-name ordering and a total count, but cannot associate
original fields with converted values. Local Viceroy can lowercase and deduplicate
names, so global order is `Unknown`, pending production verification. Aggregate
count equality cannot prove per-header multiplicity; no per-header guarantees
are inferred from it.

## Next Steps

- Learn about [Cloudflare Workers](/guide/adapters/cloudflare) as an alternative deployment target
- Explore [Configuration](/guide/configuration) for manifest details

# Fermyon Spin

Run EdgeZero applications on [Fermyon Spin](https://spinframework.dev/),
a WebAssembly-first application platform with a `wasm32-wasip2` target and
component-scoped KV / variable stores.

## Prerequisites

- Rust toolchain with `wasm32-wasip2` target (`rustup target add wasm32-wasip2`)
- Spin CLI **4.1 or newer** ([install](https://spinframework.dev/install)).
  The workspace's `spin-sdk` imports `wasi:http/types@0.3.0`, which Spin
  4.0.x does not provide. On an older runtime the component builds and its
  tests pass, and only `spin up` fails:

  ```
  Error: component imports instance `wasi:http/types@0.3.0`, but a
  matching implementation was not found in the linker
  ```

## Project Setup

When scaffolding with `edgezero new my-app`, the Spin adapter includes:

```
crates/my-app-adapter-spin/
├── Cargo.toml
├── spin.toml
└── src/
    └── lib.rs
```

### Entrypoint

The Spin entrypoint wires the adapter via `#[http_service]`:

```rust
use spin_sdk::{http::Request, http_service};
use my_app_core::App;

#[http_service]
async fn handle(req: Request) -> anyhow::Result<edgezero_adapter_spin::SpinResponse> {
    edgezero_adapter_spin::run_app::<App>(req).await
}
```

`run_app` reads the portable store metadata baked into `App` by the `app!`
macro plus `EDGEZERO__*` environment variables; it does not require an
`edgezero.toml` to be present at runtime.
If route/state assembly or `Hooks::configure` fails, `run_app` and `run_app_with_platform`
deliver the typed error's status and standard JSON body through canonical detached egress.
They release the unread request, use no failed app's policies or headers, and log the full failure
once on `edgezero::boot`. Internal details stay off the wire. The finite default write deadline
starts before assembly; see [application assembly](/guide/handlers#configuring-the-application-lifecycle).

## Building

Build the Spin component:

```bash
# Using the CLI
edgezero build --adapter spin

# Or directly
cargo build --target wasm32-wasip2 --release -p my-app-adapter-spin
```

## Local Development

```bash
# Using the CLI
edgezero serve --adapter spin

# Or directly
spin up --from crates/my-app-adapter-spin
```

## Deployment

```bash
# Using the CLI
edgezero deploy --adapter spin

# Or directly
spin deploy --from crates/my-app-adapter-spin
```

## Outbound HTTP

`SpinOutboundClient` uses the WASI HTTP 0.3 interfaces directly so request-body production,
response completion, limits, and the guest-visible deadline race share one owner. Generated
`spin.toml` files default `allowed_outbound_hosts` to `https://*:*`; cleartext requires an explicit
manifest declaration.

Spin exposes raw header bytes and drives isolated batch slots in completion order. Cache bypass
adds no synthetic origin header because the adapter owns no intermediary cache. A wire-authority
override is rejected during preflight: WASI HTTP uses the same authority for connection routing,
so Spin cannot preserve URI-owned routing and TLS identity while changing only the HTTP authority.
Unsupported Image Optimizer settings are rejected first, then an unsupported authority override,
before portable method/body validation and budget selection. A pending GET/HEAD source or invalid
response policy cannot mask the authority diagnostic or trigger a body read. The shared facade
prepares contexts and settles unknown GET/HEAD sources before dispatch. Begin and batches schedule
complete exchanges with the WASIp3 executor before returning their completion receivers; later
collection does not start transport work or resample the batch start.
Deadline, batch cancellation, and streamed-upload cancellation remain BestEffort until host
teardown has a documented observed bound, and provider phase-timer defaults may prevent a fully
elastic budget. Downstream response
delivery owns the raw WASI response, body, and result writers in one coordinator and preserves
lazy body production under host backpressure. Successful close is a host handoff, not proof of
client receipt, so response-egress capabilities also remain BestEffort. See
[Capabilities](/guide/capabilities).

## KV Storage

Spin KV is **label-backed and multi-store** — each logical id in
`[stores.kv].ids` maps to a Spin store label declared in `spin.toml`.
Override the label per id with `EDGEZERO__STORES__KV__<ID>__NAME`; with the
variable unset the label defaults to the logical id.

```toml
# edgezero.toml
[stores.kv]
ids     = ["sessions", "cache"]
default = "sessions"
```

```toml
# spin.toml
[component.my-app]
key_value_stores = ["sessions", "cache"]
```

Two Spin-specific KV constraints (see §6.7 of the design spec for the
full rationale):

- **TTL is unsupported.** Spin's `key_value::Store::set` accepts no
  expiry. `put_bytes_with_ttl` returns
  `KvError::Unsupported { operation: "put_bytes_with_ttl" }` (mapped to
  HTTP 501) after a successful open; a failed declared binding returns its typed
  `OpenFailed` reason first. It never silently strips the TTL.
- **Listing is capped.** `Store::get_keys()` is unbounded, so the
  adapter materialises the key list, filters by prefix, sorts, and pages
  client-side. A `max_list_keys` cap (default `1000`, override via
  `EDGEZERO__STORES__KV__<ID>__MAX_LIST_KEYS`) guards against runaway
  lists and yields `KvError::LimitExceeded` (HTTP 503) when exceeded.

## Config Store

Spin config is **KV-backed and multi-store** — each logical id in
`[stores.config].ids` opens a separate `spin_sdk::key_value::Store` at
runtime. The store accepts arbitrary UTF-8 keys, so the canonical dotted
key (`service.timeout_ms`) is read back verbatim — no key translation.
Override the label per id with `EDGEZERO__STORES__CONFIG__<ID>__NAME`;
with the variable unset the label defaults to the logical id.

```toml
# edgezero.toml
[stores.config]
ids     = ["app_config", "feature_flags"]
default = "app_config"
```

```toml
# spin.toml — declare every label in the component's `key_value_stores`
[component.my-app]
key_value_stores = ["app_config", "feature_flags"]
```

```toml
# runtime-config.toml — register each custom label with a backend
# (the default `default` label is auto-provided by Spin; everything
# else needs an entry here, or `spin up` errors with
# "unknown key_value_stores label <name>").
[key_value_store.app_config]
type = "spin"

[key_value_store.feature_flags]
type = "spin"
```

`edgezero new --adapter spin` scaffolds both files; `edgezero serve
--adapter spin` runs `spin up --runtime-config-file runtime-config.toml`
so locally-declared labels resolve to the SQLite-backed Spin KV
implementation. For production, swap `type = "spin"` for a managed
backend (`type = "azure_cosmos"`, `type = "redis"`, …) per the
[Spin runtime-config docs](https://spinframework.dev/v3/dynamic-configuration#key-value-store-runtime-configuration).

`provision` writes the `[component.<id>].key_value_stores` array for
you (it does NOT touch `runtime-config.toml` — keep that one
hand-edited).

### Seeding the store

`<your-cli> config push --adapter spin` reads `runtime-config.toml` and
dispatches to the right per-backend writer — no embedded HTTP endpoint,
no token to manage. (Typed push runs from your downstream app CLI; the
bundled `edgezero config push` errors with a pointer to it, since the
blob model needs the typed `AppConfig<C>`.) Resolution order:

1. **`--local` set**: forces SQLite-direct against
   `<spin.toml dir>/.spin/sqlite_key_value.db` (Spin's local KV file).
   Useful for poking values in your local dev loop without
   authenticating against Fermyon Cloud. Even under `--local`, every
   non-`default` label MUST be declared in `runtime-config.toml`
   (see point 4 below) — without the stanza, `spin up` errors with
   `unknown key_value_stores label <name>` and the file you just
   wrote is unreadable from the running app, so the dispatcher
   refuses the push and tells you exactly which stanza to add.
2. **Manifest's `deploy` command targets Fermyon Cloud** (auto-detected
   from `[adapters.spin.commands].deploy` containing `spin deploy` or
   `spin cloud deploy`): one batched shellout per ≤96 KiB chunk of
   `spin cloud key-value set --app <APP> --label <LABEL>
KEY=VALUE [KEY=VALUE …]`. `<APP>` comes from
   `[application].name` in `spin.toml`; `<LABEL>` is the env-resolved
   platform label (your `[stores.config].id` after the
   `EDGEZERO__STORES__CONFIG__<ID>__NAME` overlay). EdgeZero uses
   Fermyon's [app-scoped label model](https://developer.fermyon.com/cloud/linking-applications-to-resources-using-labels),
   so the operator pre-links the label to a cloud KV store via
   `spin cloud link key-value` (or the Fermyon dashboard) before
   the first push; an unlinked label produces an actionable error
   suggesting the link command. Authentication comes from
   `spin cloud login` — EdgeZero does not store cloud credentials.
3. **`runtime-config.toml` declares this label's backend**:
   - `type = "spin"` → SQLite-direct (honours an explicit `path` field
     if set; otherwise the default `.spin/sqlite_key_value.db`).
   - `type = "redis"` / `azure_cosmos` / unknown → clear error
     pointing at the backend's native CLI (e.g. `redis-cli -u <url>
SET <key> <value>`). Native-CLI dispatch for redis / azure is
     planned for a follow-up release.
4. **Default**: only the `default` label falls through to SQLite
   without a runtime-config stanza (Spin auto-provides `default`).
   Any other label MUST have a `[key_value_store.<label>]` entry —
   absent it, the dispatcher errors before any write.

Schema-coupling note: the SQLite writer uses the exact `spin_key_value`
schema and `INSERT … ON CONFLICT DO UPDATE` statement vendored from
spinframework/spin's `crates/key-value-spin/src/store.rs`. A contract
test in `edgezero-adapter-spin/src/cli/push_sqlite.rs` asserts
byte-equality against the upstream Spin 4.1.0 strings. The workspace's
exact SDK 7 Git revision, shared with the demo, reusable fixture and generator,
makes every SDK upgrade explicit. The
[dependency audit](../../audits/2026-10-27-spin-sdk-eager-progress.md) records the executor
repair and upstream PR; callers use the SDK's `wasip3::spawn_local` re-export.
The CLI warns when the installed Spin runtime major is outside the
schema-verified 2.x through 4.x range.

```bash
# Local dev: writes through to .spin/sqlite_key_value.db.
<your-cli> config push --adapter spin --local

# Production (Fermyon Cloud): shells `spin cloud key-value set`.
spin cloud login    # one-time
<your-cli> config push --adapter spin
```

## Secret Store

Spin secrets use `spin_sdk::variables`, which exposes a **single flat
variable namespace** per component (no notion of multiple named secret
stores). `[stores.secrets].ids.len() > 1` while targeting Spin is caught
by `config validate --strict`. Secret variables are declared manually in
`spin.toml` with `secret = true`:

```toml
# spin.toml
[variables]
api_token = { required = true, secret = true }

[component.my-app.variables]
api_token = "{{ api_token }}"
```

`required = true` blocks `spin up` until the variable is supplied by a provider or
`SPIN_VARIABLE_<NAME>`. For local development, declare `default = ""` with
`secret = true` instead so the component starts without one; that is the form the
demo's `spin.toml` uses.

`config validate` runs a within-secrets canonicalisation check: each
`#[secret]` field value is lowercased to mirror the runtime
`SpinSecretStore::get_bytes` lookup, must be a valid Spin variable name
(`^[a-z][a-z0-9_]*$`), and must not collide with another `#[secret]`
value that lowercases to the same form.

## Spin component discovery

`provision` and `config push` (Stages 6 and 7) write `[component.<id>.*]`
blocks to `spin.toml`, which requires knowing the component id. Resolution:

- The CLI parses `spin.toml` and enumerates `[component.*]` ids.
- If exactly one component exists, it is used.
- If more than one exists, `[adapters.spin.adapter]` must carry an
  explicit `component = "<id>"` field; otherwise the command errors.

`config validate --strict` performs this resolution as part of its
adapter-set checks when `spin` is in the target list, so the failure
surfaces before `provision` / `config push` run.

## Logging

`edgezero_adapter_spin::init_logger()` is a no-op; `run_app` calls it before application
configuration unless your app sets `owns_logging = true`. An
`[adapters.spin.logging]` table in `edgezero.toml` is not consumed by the Spin runtime.
View output through `spin up` or your Spin host's log files.

::: tip Logging status
Host stdout/stderr capture does not install a Rust `log` backend. Install yours before the
adapter entrypoint and set `owns_logging = true` if you need facade output. Resolve ordinary
levels with `edgezero_core::resolve_logging_level(&EnvConfig)` and apply both backend and facade
filters; the adapter does not promise boot-target visibility without that backend.
:::

## Context Access

Access Spin-specific request metadata via the request context extensions:

```rust
use edgezero_core::context::RequestContext;
use edgezero_adapter_spin::context::SpinRequestContext;

async fn handler(ctx: RequestContext) -> Result<Response, EdgeError> {
    if let Some(spin_ctx) = ctx.extensions().get::<SpinRequestContext>() {
        let client_addr = spin_ctx.client_addr;
        let full_url = spin_ctx.full_url.as_deref();
        // ...
    }

    // ...
}
```

Spin exposes this data through the `spin-client-addr` and `spin-full-url` headers
rather than a separate runtime object.

## Testing

Run contract tests for the Spin adapter:

```bash
rustup target add wasm32-wasip2
export CARGO_TARGET_WASM32_WASIP2_RUNNER="wasmtime run -W component-model-async=y -S p3=y -S http=y"
cargo test -p edgezero-adapter-spin --features spin --target wasm32-wasip2 --test contract
```

These contracts execute under Wasmtime. Incoming conversion is additionally exercised by the actual Spin HTTP fixture. The
`wasmtime` version CI uses is pinned in `.tool-versions`.

## Manifest Configuration

Configure the Spin adapter in `edgezero.toml`. See
[Configuration](/guide/configuration) for the full manifest reference.

## Retaining an application

Use a concrete application-owned `OnceLock<App>` and call
`edgezero_adapter_spin::dispatch_app::<MyApp>(app, req).await` on each
invocation. This resolves fresh request resources from `MyApp::stores()` and does
not install logging, construct an app, or cache native resources. Initialize any
application logging before the cache's synchronous app constructor. Do not use a
generic unkeyed static or hold a lock across an await.

```rust
use edgezero_adapter_spin::{SPIN_PLATFORM, SpinResponse};
use edgezero_core::{app::App, EdgeError, MonotonicClock};
use my_app_core::App as MyApp;
use spin_sdk::{http::Request, http_service};
use std::sync::OnceLock;

static APP: OnceLock<App> = OnceLock::new();

fn retained_app() -> Result<&'static App, EdgeError> {
    if let Some(app) = APP.get() {
        return Ok(app);
    }
    let app = App::build::<MyApp>(SPIN_PLATFORM)?;
    Ok(APP.get_or_init(|| app))
}

#[http_service]
async fn handle(req: Request) -> anyhow::Result<SpinResponse> {
    // Install application-owned logging before attempting assembly.
    let clock = MonotonicClock::default();
    let start = clock.now();
    let app = match retained_app() {
        Ok(app) => app,
        Err(error) => return Ok(edgezero_adapter_spin::request::send_assembly_error(
            req, error, start, clock,
        )?),
    };
    edgezero_adapter_spin::dispatch_app::<MyApp>(app, req).await
}
```

`App::build::<MyApp>(SPIN_PLATFORM)` returns `Result<App, EdgeError>` and invokes
fallible `Hooks::configure` after installing platform metadata. For Akamai Functions,
select `AKAMAI_FUNCTIONS_PLATFORM`; other hosts can supply metadata matching their
configured limits. Only a successful app is cached, so a failed build can be retried.
Concurrent construction attempts may build multiple candidates; only one is retained.
The app must be built from the same `Hooks` type used by dispatch.

`dispatch_app<A: Hooks>(&App, SpinRequest) -> anyhow::Result<SpinResponse>` returns
the raw WASI response, not a collected SDK response. Each invocation resolves fresh
registries and runs admission and response egress; the adapter's coordinator owns
the body writer and transmission-result observation through terminal delivery.

The pinned SDK 7 macro exports a P3 HTTP interface. The `wasm32-wasip2` Rust target
name does not establish the component's HTTP lifecycle. Verify the emitted
interface and host together. SDK 7 requires Spin 4.1 or later. See Spin's
[instance-reuse documentation](https://spinframework.dev/v4/http-trigger#controlling-instance-reuse)
for the reuse and concurrency controls your host version supports.

Test both sequential reuse and overlapping invocations in the same instance.
Different guest instances are not evidence of concurrent isolation. Keep
request/response bodies, admission resources, delivery coordinators, and pending
operations invocation-local. Standard response delivery remains lazy under host
backpressure; retention adds no whole-response buffer or collection cap. Restore `run_app`
and remove the cache to roll back; default generated entry points are unchanged.

## Runtime ingress metadata

Conversion captures SDK URI parts before rebuilding the core request. Origin
comes from HTTP(S) URI scheme/authority, with runtime provenance.
`spin-full-url`, `spin-client-addr`, Origin and forwarding headers do not become
adapter-supplied ingress-origin facts. Runtime URI authority still identifies the client-requested host; it does not establish operator approval or ownership. Existing context fields and store/body behavior
remain unchanged.

The historical Spin 4.1 HTTP fixture observations imported from main
`87456c70` cover methods, tested target spelling, duplicates and hook ordering
through the converter. They are not evidence of a run with the merged
candidate's pinned SDK source; rerun the fixture to establish that result.
In that recorded runtime, request
preparation requires UTF-8 header values; a Cookie containing FF returns 500
before the component starts. This needs an upstream runtime fix and cannot be
restored inside EdgeZero. Same-name runtime values are copied byte-for-byte,
while original-wire fidelity remains conservative and global order unavailable.
Spin 4.1 consumes chunked framing and removes `Transfer-Encoding` before
component invocation: the `chunked` and `te-cl` probes expose no such field to
the hook. The component cannot reconstruct that transport framing.

Pure metadata tests execute on the host. The WASM contract suite runs with
`wasmtime run -W component-model-async=y -S p3=y -S http=y`.
The full WASM library test artifact
also imports Spin-specific host interfaces, so plain Wasmtime cannot execute
that artifact. The real Spin HTTP fixture supplies runtime conversion evidence.
See [the capability matrix](./overview#inbound-request-fidelity).

## Next Steps

- [Migration guide](/guide/manifest-store-migration) — moving from the
  pre-rewrite store schema
- [Adapters overview](/guide/adapters/overview) — cross-adapter contracts
- [Configuration](/guide/configuration) — full manifest reference

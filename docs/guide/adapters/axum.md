# Axum (Native)

Run EdgeZero applications natively using Axum and Tokio for local development, testing, and container deployments.

## Overview

The Axum adapter provides:

- **Local development server** - Fast iteration without Wasm compilation
- **Native testing** - Run tests with standard `cargo test`
- **Container deployments** - Deploy to any platform supporting native binaries

## Project Setup

When scaffolding with `edgezero new my-app`, the Axum adapter includes:

```
crates/my-app-adapter-axum/
├── Cargo.toml
├── axum.toml
└── src/
    └── main.rs
```

### Entrypoint

The Axum entrypoint wires the adapter:

```rust
use edgezero_adapter_axum::dev_server::run_app;
use my_app_core::App;

fn main() -> anyhow::Result<()> {
    run_app::<App>()
}
```

`run_app` installs `simple_logger` unless `owns_logging = true`, builds the app, and reads bind,
store, and logging config at runtime from `EDGEZERO__*` environment
variables. See [the migration guide](../manifest-store-migration.md).

Incoming proxy trust defaults to no trusted networks. Managed hosting strips raw forwarding
headers before admission and exposes checked ingress metadata instead. See
[Axum diagnostics and trusted proxies](/guide/axum-diagnostics) for configuration, migration,
request records, and work snapshots.

The portable store metadata baked into `App` by the `app!` macro
drives which logical stores are exposed; no `edgezero.toml` needs to
be loaded by the runtime.
If `Hooks::configure` fails, `run_app` returns the source-preserving
`application configuration failed` error before binding the listener.

## Development Server

Run your project locally on the Axum adapter:

```bash
edgezero serve --adapter axum
```

This starts a server at `http://127.0.0.1:8787` with standard logging to stdout.

### Manual Start

Run the Axum entrypoint directly:

```bash
# Using the CLI
edgezero serve --adapter axum

# Or directly with cargo
cargo run -p my-app-adapter-axum
```

## Building

Build a native release binary:

```bash
# Using the CLI
edgezero build --adapter axum

# Or directly
cargo build -p my-app-adapter-axum --release
```

The binary is placed in `target/release/my-app-adapter-axum`.

## Bounded HTTP/1 Ingress

The standard server entrypoints own a bounded pool of HTTP/1 connections. These limits apply
before application admission, including to sockets that never finish a request head. No custom
listener or router is needed. HTTP/2 is not enabled, and protocol upgrades cannot transfer the
socket out of this pool.

`run_app` resolves these runtime variables once, before application configuration or binding:

| Variable (`EDGEZERO__ADAPTER__INGRESS__` prefix) | Default | Supported range |
| ------------------------------------------------ | ------- | --------------- |
| `MAX_CONNECTIONS`                                | 256     | 1–4096          |
| `MAX_RAW_HEAD_BYTES`                             | 65536   | 8192–1048576    |
| `MAX_HEADER_COUNT`                               | 100     | 1–1024          |
| `HEAD_READ_TIMEOUT_MS`                           | 10000   | 1–120000        |

Only missing settings use defaults. Explicit values must be unsigned decimal integers within
the supported ranges; empty, signed, malformed, overflowing, and zero values fail startup.
Diagnostics identify the setting, never its supplied value. The effective validated
`AxumIngressConfig` is logged at `Info`. Applications can resolve the same configuration with
`AxumIngressConfig::from_env(&EnvConfig::from_env())` and inspect its getters. Programmatic
servers install the validated value in `AxumDevServerConfig::ingress`:

```rust
use std::time::Duration;
use edgezero_adapter_axum::{ingress_config::AxumIngressConfig, dev_server::AxumDevServerConfig};

let config = AxumDevServerConfig {
    ingress: AxumIngressConfig::new(64, 16384, 64, Duration::from_secs(5))?,
    ..AxumDevServerConfig::default()
};
```

Manifest defaults can be supplied through adapter-filtered `[[environment.variables]]` entries.
The Axum CLI forwards the selected manifest's defaults to its Cargo child without mutating the
parent environment. An existing parent variable, including an empty one, takes precedence;
duplicate applicable defaults retain declaration order, with the last taking precedence.
Running the binary directly requires setting the runtime variables directly.

### Connection Ownership And Deadlines

Let `C` be `max_connections()`. At most `C` connection futures own parser state, sockets, and
application work, including keep-alive connections. There is no per-connection spawned task or
permit-waiter queue. The accept loop may own **one additional transient accepted socket**; if
capacity is exhausted it closes that socket immediately, without a parser, response, or
background task. Completed connections are reaped before accepting more work. Disconnect,
parser failure, timeout, cancellation, and shutdown drop the single connection owner and release
capacity; unwinding handler failures are isolated to their connection. Shutdown closes the
owned connections rather than waiting indefinitely for a graceful drain.

The first absolute idle-plus-header deadline starts when the accepted socket enters the pool,
before its future is first polled. Each subsequent deadline starts when Hyper first polls the
next request head after the previous exchange permits it. Idle keep-alive time and incomplete-head
time share this budget; a byte arriving does **not** reset it. Pipelined bytes may already be
buffered, so this is not a deadline starting at the network arrival of each pipelined request.
An independent timer wakes without incoming data, and ready I/O and completed heads also check
expiry before admission. Equality is expired. Slow/trickled or idle connections close when the
deadline expires; an HTTP timeout response is not promised.

After a complete head is accepted, the header clock is disarmed for application work. Existing
application body-read and response-write deadlines remain in force. Infrastructure timers use
the native Tokio monotonic clock, independently of an application's injectable clock, so a
frozen application clock cannot disable pre-admission expiry. These timers are cooperatively
scheduled: blocking application code can delay all work on the connection executor.

### Wire Limits And Parser Storage

Let `H = max_raw_head_bytes()` and `N = max_header_count()`. The pinned Hyper 1.12.0 parser
rejects a complete raw head above `H` bytes or above `N` field lines **before** constructing an
admitted request. The byte budget includes the entire request line, field names, colons,
whitespace, line endings, and the final empty line. Duplicate field lines count separately.
Request-line and header bytes share one combined budget, not independent allowances. Exactly
`H` bytes and `N` fields pass the size/count checks; other protocol, type, or application validation
can still reject an in-budget head. Hyper additionally limits request targets to 65534 bytes
(414). Overflow closes the connection and Hyper attempts a bounded 431 response; other malformed
syntax may receive a bounded 400 response. Expiry or disconnect can prevent response delivery.
Application admission and rendering hooks do not run for raw parser rejections.

`parser_buffer_bytes()` is Hyper's logical threshold `H`, **not** a hard allocation ceiling.
The receive scratch audit pins Hyper 1.12.0, bytes 1.12.1, http 1.4.2, and Rust 1.95.0:

- `parser_read_allocation_bytes()` reports the conservative requested-capacity bound `R = 4H`
  for each receive backing allocation. Adaptive reads expose spare capacity and shared-buffer
  splits can cause a larger replacement allocation than the logical threshold.
- `parser_trailer_allocation_bytes()` reports `T = next_power_of_two(H)` for the separate raw
  chunked-trailer buffer. Hyper applies its byte budget separately to trailers, whose completed
  raw field section must be **strictly smaller** than `H`; this is not a combined head-plus-trailer
  allowance. The trailer field count remains at most `N`.
- Conservative simultaneous receive scratch, including an outstanding channel frame, replacement
  allocation, raw trailer growth, and copied field bytes, is `3R + 2T + H`, **plus** parser/header
  metadata, URI/method storage, and fixed connection state. These are requested capacities, not
  allocator-resident bytes or a complete per-request memory certificate.
- `parser_header_map_allocation_bytes()` reports `M`, a conservative per-map requested-capacity
  charge covering both ordinary and adversarial collision-driven growth, duplicate-value storage,
  and capacity retained across clear/reuse. For `P = max(100, N) + 1`, raw index capacity is bounded
  by the largest power of two at most `10P`, bucket capacity by three quarters of that number,
  and duplicate capacity by `next_power_of_two(max(4, P - 1))`. The getter uses public header-type
  sizes plus conservative machine-word padding for private map nodes. Charge up to `1.5M` for
  one map's reallocating peak, or `3.5M` for overlapping request, response/cache, and trailer maps.
  An application-retained request map or admission's temporary header clone is an additional
  owner, not included in those three maps.
- Raw parse workspaces additionally retain the default 100-field inline arrays and, above that,
  bounded heap arrays. On 64-bit native builds a conservative payload charge for these two
  workspaces is `64 * max(100, N)` bytes, plus their fixed container state. Parsed URI/method data
  and field-name/value backing bytes are separate charges bounded by the raw input sizes.

Response header maps become reusable request-parser storage in Hyper. Before handoff, EdgeZero
therefore bounds application response fields to `max_response_header_count() = max(100, N)`
and `max_response_header_bytes() = H`. Field-byte accounting is the sum of name bytes, value
bytes, and four bytes per field (`: ` and CRLF), including duplicates. EdgeZero copies the map
and field data into fresh storage, discarding application-supplied spare capacity and large
shared backing buffers; duplicate values and sensitive flags are preserved. Custom Hyper
reason phrases are removed in favor of canonical status phrases. Hyper's own status line,
framing, connection, and date fields need additional fixed overhead. An oversized response head
closes the connection and reports a transport error to any active egress attempt; it does not
allocate a replacement error response.

The pool bounds retention of receive scratch, parser-generated error responses, and parser
metadata, not arbitrary application allocations. A budget must additionally charge parsed
request/response metadata and copies, application-held body frames, response buffers, handler
state, stores, outbound exchanges, and allocator overhead. Kernel socket buffers, the OS listen
backlog, and resources outside EdgeZero are excluded. The connection bound is not a published
platform memory ceiling or a whole-process RSS guarantee.

This combined HTTP/1 resource policy does not implement the stricter independent raw-target /
field-section accounting or raw framing-rejection policy. Admission still receives
`IngressHeadAccounting::HostManaged` and `IngressFraming::HostManaged`; both raw-ingress
capabilities remain `Unsupported`. See [Capabilities](/guide/capabilities).

## Buffered JSON body limits

Every standard Axum hosting path defaults framework-managed JSON buffering to 2 MiB,
exactly 2,097,152 bytes. Valid JSON at the effective limit succeeds; one byte over returns
HTTP 413. Malformed JSON within the limit remains HTTP 400.

For standalone development or production, select decimal integer bytes at startup:

```bash
EDGEZERO__ADAPTER__JSON_BODY_LIMIT_BYTES=4194304 cargo run -p my-app-adapter-axum
```

The value must be positive and representable by the collector. Zero, signs, whitespace,
suffixes, fractions, overflowing values, and non-Unicode values fail startup before application
configuration or readiness. There is no unlimited sentinel, CLI flag, or manifest setting.
The selected value is captured once per run.

For explicit production embedding or a router-only server, use the fallible builder:

```rust
use edgezero_adapter_axum::dev_server::{AxumDevServer, AxumRunOptions};

let options = AxumRunOptions::new("127.0.0.1:8787".parse()?)?
    .with_json_body_limit_bytes(4 * 1024 * 1024)?;
let server = AxumDevServer::new(router).with_json_body_limit_bytes(4 * 1024 * 1024)?;
```

Explicit options and router builders do not merge ambient settings. `AxumRunOptions::from_env`
is the deliberate environment-driven alternative. The low-level `into_core_request` converter
attaches the finite default without reading this environment setting.

Ordinary `Json` and `ValidatedJson` use the selected ceiling, including overrides above the
portable 8 MiB fallback. Explicit JSON caps take the minimum of the caller cap and native ceiling.
JSON helpers enforce it regardless of Content-Type; generic buffered helpers enforce it for the
original `application/json` or `application/*+json` ingress classification. Ordinary Form retains
its 1 MiB caller cap. Header/extension mutation and context reconstruction cannot loosen policy.

Native-managed JSON deserialization failures return HTTP 400 with the fixed text
`invalid JSON payload`. Custom renderers receive that same fixed error, not serde detail.
Legacy/unmanaged JSON errors and application-authored `EdgeError` messages retain their
existing behavior. See [privacy limits](/guide/axum-diagnostics#timing-failures-and-privacy-limits).

Taken streams and proxy forwarding receive no new aggregate size cap. Manual body collection
keeps its supplied cap and existing 400 overflow behavior. The ceiling bounds logical buffered
body bytes, not parsed JSON allocations, host buffers, concurrent requests, or process memory.
It does not promise client receipt or keep-alive reuse after rejection.

## Outbound HTTP

The Axum adapter injects `AxumOutboundClient`, backed by `reqwest`. Application handlers use the
portable client from `RequestContext::http_client()`; direct wiring and tests can construct the
adapter client with `AxumOutboundClient::try_new()`.

Axum provides native total deadlines, completion-order batching, pending-future cancellation,
batch slot isolation, cache bypass, wire-authority override, elastic phase budgeting, header
fidelity, and streamed upload cancellation. Downstream responses run on a connection-local Hyper
HTTP/1 executor, so portable non-`Send` streams remain lazy and are polled under Hyper demand.
The connection supervisor enforces the absolute response-write deadline, but frame acceptance is
not proof of socket or client receipt; response-egress capabilities therefore remain BestEffort.
See [Capabilities](/guide/capabilities).

For streamed responses with `Content-Length`, Axum retains only the final chunk until source EOF
validates, because Hyper stops polling once the declared length is satisfied. A bad tail is not
sent, and a pending tail remains subject to the write deadline. A declared zero-length stream is
validated before handoff. Suppressed HEAD/status bodies are dropped without polling. Clean source
EOF remains `HostHandoff`, not proof of socket flush or client receipt.

## Logging

The Axum adapter's `run_app` helper installs `simple_logger` at the level read from
`EDGEZERO__LOGGING__LEVEL`, falling back to `info` when the variable is unset or
unparseable. It does not read `edgezero.toml`, and `echo_stdout` has no effect on
the runtime. To install a different logger, set `owns_logging = true` on your `app!`
declaration so `run_app` skips its own logger, then install yours in `main` **before** `run_app`.
Logger setup now precedes application configuration, and setup failures are returned rather than
ignored. For embedding startup validation, `run_app_with_preflight::<App, _>(check)` runs `check`
after logging is ready and before configuration or listener binding. The bundled `edgezero demo`
uses this for its capability gate instead of preinstalling a second CLI logger.

Managed boot warnings use `edgezero::boot` at `Warn`, even when runtime logging is `Off`.
Application-owned loggers can reuse the shared resolver and constants:

```rust
use edgezero_core::{BOOT_LOG_LEVEL, BOOT_LOG_TARGET, resolve_logging_level};
use edgezero_core::env_config::EnvConfig;

let env = EnvConfig::from_env();
simple_logger::SimpleLogger::new()
    .with_level(resolve_logging_level(&env))
    .with_module_level(BOOT_LOG_TARGET, BOOT_LOG_LEVEL)
    .init()?;
// Then call run_app::<App>() with owns_logging = true.
```

Wiring
`edgezero_core::app::App::build::<App>(edgezero_adapter_axum::AXUM_PLATFORM)` and `AxumDevServer` by hand remains the fallback if you also need
to control the bind address or store setup.

Production `AxumRunOptions` rejects an invalid supplied logging level.

To install your own logger, set `owns_logging = true` in the `app!` declaration and initialize
it once in `main` before entering the runner. Ownership controls installation, not framework
facade output. Safe native request and lifecycle records use target `edgezero::native`.

Request records are on by default. `EDGEZERO__LOGGING__REQUEST_RECORDS` accepts exactly
`true` or `false`; invalid supplied values fail startup in development and production.
`AxumRunOptions::with_request_records(false)` and `AxumDevServer::with_request_records(false)`
suppress only default native request logs. Application/native observers, lifecycle records,
counts, and snapshots remain active. Explicit builders do not merge ambient record settings.

Use `with_diagnostics` to attach a `NativeDiagnosticsHandle` for snapshots or an optional
native request observer. No exporter or public metrics endpoint is installed. See
[logger ownership](/guide/axum-diagnostics#records-and-logger-ownership) and
[snapshots and metrics](/guide/axum-diagnostics#snapshots-and-optional-metrics) for examples
and the synchronous sink limitations.

## Testing

The Axum adapter enables standard Rust testing:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::app::App as EdgeZeroApp;
    use edgezero_core::http::{Request, Method};

    #[tokio::test]
    async fn test_handler() {
        let app = EdgeZeroApp::build::<App>(edgezero_adapter_axum::AXUM_PLATFORM)
            .expect("configured app");
        let router = app.router();

        let request = Request::builder()
            .method(Method::GET)
            .uri("/hello")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);
    }
}
```

Run tests:

```bash
cargo test -p my-app-core
cargo test -p my-app-adapter-axum
```

## KV Storage

Each declared `[stores.kv]` id resolves to a `redb`-backed store on disk under
`.edgezero/`, so values persist across dev-server restarts. The file name is
derived from the platform store name, which comes from
`EDGEZERO__STORES__KV__<ID>__NAME` or defaults to the logical id:

```
.edgezero/kv-<slug>-<hash>.redb
```

The database file grows over time and does not shrink after deletions. To reclaim
space, delete the file in `.edgezero/`; the data is lost. See [KV Storage](/guide/kv)
for the portable API.

## Native store providers

Axum keeps the existing local defaults, but native applications can select a provider
per logical store ID. The portable manifest still declares IDs and defaults only. A
separate, strict native bindings file selects supported providers; it does not change
store metadata or add AWS dependencies to Fastly, Cloudflare, or Spin builds.

Without a bindings file, config uses local `.edgezero/local-config-<id>.json`
snapshots, secrets use environment variables, and KV uses the existing local backend.
With explicit AWS bindings, the runner fetches mapped values before its initializer
and before it accepts requests. It does not fall back to a local provider when an AWS
lookup or preparation fails. See the [native AWS stores guide](../native-aws-stores.md)
for the schema, feature selection, permissions, and lifecycle limits.

The native binding file path must be absolute. `EDGEZERO__STORE_BINDINGS_FILE` is
read only by the environment-loading runner. Explicit `AxumRunOptions` selection
uses only values supplied in code. Programmatic applications may also inject existing
handles or deferred setup through `run_app_with_native_stores`; deferred setup is
polled after source validation, in the current native runtime. Its construction must
not perform provider work.

A declared `[stores.secrets]` ID still defaults to an environment-backed store when
not explicitly bound. It is no longer limited to one ID by Axum. Existing
`EDGEZERO__STORES__SECRETS__<ID>__NAME` and config/KV local overrides retain their
meaning for implicit local bindings; they cannot override an explicitly bound ID.

`config push --adapter axum` remains a local-file writer. It does not publish to
AppConfig. If passed an explicit `--store-bindings` file that binds the selected ID
to AWS, the guard rejects the operation before writing anything. A push
without that explicit file still only updates the local snapshot file; it does not
update an AWS-bound deployment.

```toml
[stores.config]
ids     = ["app_config"]
# default = "app_config"   # required when ids.len() > 1
```

```jsonc
// .edgezero/local-config-app_config.json — what `<your-cli> config push` writes.
// The outer object maps the selected config key (defaults to the logical store
// id; override with `--key`, e.g. for a `staging` blob) to ONE BlobEnvelope,
// JSON-encoded as a string (see the blob migration guide for the envelope's
// fields). `data` holds the typed config verbatim; `#[secret]` fields store
// their key NAMES, which the runtime resolves at request time — never the
// secret values.
{
  "app_config": "{\"version\":1,\"generated_at\":\"…\",\"sha256\":\"…\",\"data\":{\"greeting\":\"hello\",\"feature\":{\"new_checkout\":false},\"service\":{\"timeout_ms\":1500},\"api_token\":\"demo_api_token\",\"vault\":\"default\"}}",
}
```

Typed apps read the whole config in one shot with the `AppConfig<C>` extractor,
which parses the envelope and resolves `#[secret]` fields before handing you `cfg`:

```rust
async fn handler(AppConfig(cfg): AppConfig<MyConfig>) -> Result<Response, EdgeError> {
    let greeting = &cfg.greeting;
    // …
}
```

The lower-level `Config` extractor / `ctx.config_store(id)` exposes the raw
key/value store — `store.get("app_config")` returns the envelope string, and a
hand-seeded flat file returns individual values. Do not pass raw user input
straight to `store.get(…)` in production handlers; validate or allowlist keys
first.

Seed the per-id files with `<your-cli> config push --adapter axum` (the typed
flow — the bundled `edgezero config push` errors), which writes the same
`.edgezero/local-config-<id>.json` files the runtime reads — no shell-out, no
server to authenticate against.

### Snapshot Allocation Limits

Set `EDGEZERO__ADAPTER__CONFIG_STORE__<SETTING>` through the normal environment
configuration/startup path. Absent settings use finite defaults; explicit invalid,
zero, overflowing or inconsistent settings fail before listener binding.

| Setting              | Default | Supported Range / Scope                                                    |
| -------------------- | ------- | -------------------------------------------------------------------------- |
| `MAX_FILE_BYTES`     | 16 MiB  | 1 byte to 256 MiB per file, including JSON syntax and escaping             |
| `MAX_ENTRIES`        | 1024    | 1 to 65536 entries per file, including duplicate-key attempts              |
| `MAX_KEY_BYTES`      | 1024    | 1 byte to 64 KiB decoded UTF-8 per key, no larger than the file cap        |
| `MAX_VALUE_BYTES`    | 8 MiB   | 1 byte to 256 MiB decoded UTF-8 per value, no larger than the file cap     |
| `MAX_RESIDENT_BYTES` | 32 MiB  | 1 byte to 256 MiB across every config snapshot in the startup registry     |
| `MAX_STARTUP_BYTES`  | 112 MiB | 1 byte to 1 GiB across resident snapshots plus one file's reserved staging |

These are requested-allocation charges, not an RSS ceiling. Stores load sequentially.
For wire cap `W`, staging reserves `5 * max(W, 32)` bytes: input growth, escaped-string
decoder growth (including old/new allocation overlap), and temporary decoded key/value
strings. A separate 4096-byte read buffer lives on the stack. The startup limit must
cover staging; every index/payload reservation also checks residency plus staging.
This decoder allowance is audited against pinned `serde_json 1.0.150` and Rust 1.95.0;
changing those allocation implementations requires repeating the audit.

The sorted snapshot index reserves `MAX_ENTRIES * size_of::<(String, ConfigValue)>()`
bytes per store. Each entry additionally charges key UTF-8 bytes, value UTF-8 bytes,
two `Arc` reference counts and up to `size_of::<usize>() - 1` alignment bytes. Duplicate
keys are rejected. Keys/values are size-checked before their owned copies; actual reads
detect file growth and reject the first excess byte without appending it. Non-string
values are rejected without constructing a JSON tree. Failed loads drop staging and
partial snapshots. Diagnostics never contain file contents.

`get` and `get_bounded` return `ConfigValue`, an independently owned, shared UTF-8
payload. Clones allocate no payload and never retain the whole file. Bounded reads
check both extraction caps and the injected deadline before returning an `Arc` clone;
there is no request-time file read. Borrow with `as_str()` or `as_ref()`; `to_string()`
is an explicit allocating boundary. Already-owned `from_map` inputs are caller allocations
outside these startup-reader charges.

Budget fixed manifest/registry/handle metadata separately, as well as allocator overhead,
environment-backed secret reads, concurrently parsed config trees, typed outputs and
application retention of returned handles. Canonical hashing streams into SHA-256 rather
than staging a second full JSON string. The extractor drops its raw handle before resolving
secrets. Kernel buffers, filesystem page cache, allocator fragmentation and total RSS are
not guaranteed. Local file reads are synchronous; no finite startup I/O interruption is
claimed. The broad `config-read-allocation-bounds` capability remains `Unsupported`;
this bounded local config-snapshot path does not certify every secret/provider allocation.

## Optional AWS features

Generated native targets opt into `aws-appconfig-agent` and
`aws-secrets-manager` independently, or use the `aws-stores` alias for both. These
features are separate from the CLI binary's own dependencies. A bindings file cannot
enable a provider that was not compiled into the application. The AWS SDK and service
clients stay out of default native and WASM target graphs.

The native AWS SDK can emit diagnostic fields at dependency `DEBUG`/`TRACE` levels.
Those events may include secret ARNs, credential-source metadata, or response details
when sensitive-body logging is enabled. EdgeZero redacts its own errors and diagnostics,
but the first version does not suppress dependency events because scoped suppression
can break an application's tracing-to-log fallback. Do not enable dependency
`DEBUG`/`TRACE` in production unless the process logging policy has been reviewed.
This limitation must be disclosed in release and PR review notes.

## Container packaging

`edgezero new` generates a workspace-root `Dockerfile` and `.dockerignore` when
Axum is registered in the CLI build. The recipe builds only the native Axum
application binary with `--release --locked`, using a GNU/Linux Rust builder and
compatible Debian-slim runtime. The base images have reviewed digest pins;
package installation still needs network access and is not a byte-reproducible
build claim.

### Prepare the application

Generated EdgeZero dependencies may point into the developer's local checkout.
Those paths are not portable Docker inputs. Before building, edit the root
`[workspace.dependencies]` entries to use the same approved release tag or
commit for every EdgeZero crate. For example:

```toml
# Replace the placeholder with a reviewed release compatible with this app.
edgezero-core = { git = "https://github.com/stackpop/edgezero.git", tag = "<approved-release>", default-features = false }
edgezero-adapter-axum = { git = "https://github.com/stackpop/edgezero.git", tag = "<approved-release>", default-features = false }
edgezero-cli = { git = "https://github.com/stackpop/edgezero.git", tag = "<approved-release>" }
```

Apply that ref to the other generated EdgeZero adapter entries too. Preserve
member feature settings, including Axum's `axum` feature and the operator CLI's
default features. Keep relative paths between the application's own crates.
There is no new dependency-selection flag; this is ordinary manifest preparation.

```sh
cargo generate-lockfile
cargo metadata --locked --format-version 1
# Review dependency sources, then commit the app manifests and Cargo.lock.
```

Do not copy EdgeZero's workspace lockfile. Scaffolding stays offline; these
explicit preparation commands may fetch dependencies. Docker requires a present,
current application lockfile and never repairs dependencies during the build.

The builder compiler must match the exact Rust pin in the generated
`.tool-versions`. An optional `rust-toolchain.toml` must have a matching quoted
`channel = "X.Y.Z"` under `[toolchain]`. The small container check supports that
conventional format, not arbitrary TOML/channel syntax; legacy `rust-toolchain`
files are rejected. Update the declarations, Docker Rust version and reviewed
builder digest together when upgrading. The image uses the installed compiler
without downloading toolchain-file targets/components. It selects the native
host target explicitly rather than inheriting a consumer's WASM build target.

### Build and run

Run from the application workspace root, which must retain all workspace member
manifests, `edgezero.toml` and required build-time sources/assets:

```sh
docker build -t my-app:local .
docker run --rm --read-only --cap-drop=ALL \
  --security-opt=no-new-privileges:true \
  -p 127.0.0.1:8787:8787 my-app:local
# From another terminal:
curl --fail http://127.0.0.1:8787/
```

The runtime contains the binary, system CA trust and required native libraries,
not the source workspace, Rust, Node or the operator CLI. It runs as UID/GID
10001:10001 with exec startup and binds `0.0.0.0:8787` inside the container. The
publication above is localhost-only. EXPOSE does not publish ports, and the
read-only/capability settings are Docker run options, not Dockerfile guarantees.
To change the internal port, set `EDGEZERO__ADAPTER__PORT` and adjust the published
port mapping. Local development still defaults to localhost.

The default generated app declares no stores and needs no writable mount. Apps
using current embedded stores must provide `/app/.edgezero`, owned/readable as
needed by UID 10001. Provision permissions outside startup; do not run a root
chown wrapper or use world-writable state. Config-only mounts can be read-only.
Embedded redb needs one writer; a shared volume does not establish multi-replica
safety. Runtime configuration, secrets, signing keys and mutable state remain
external inputs. The image does not generate them.

Review `.dockerignore` before building. It excludes common credentials, local
state, Git and host build output, but cannot identify custom secret filenames.
Keep required manifests and assets. Private build credentials need an explicit
app-owned BuildKit secret/SSH setup, never COPY, ARG or ENV.

### Optional application asset builds

Node is not required for the generic template. Applications that embed JS during
Rust compilation can add a separate pinned asset stage before the Rust builder.
Trusted Server is one such consumer. Its app-owned extension can start with:

```dockerfile
FROM node:24.12.0-bookworm-slim AS assets
WORKDIR /assets/lib
COPY crates/trusted-server-js/lib/ ./
RUN npm ci && npm run build


# In the existing Rust builder, before Cargo compilation:
# COPY --from=assets /assets/dist /build/crates/trusted-server-js/dist
# ENV TSJS_SKIP_BUILD=1
```

Choose Node from the consumer's declared pin, retain its npm lockfile, and build
fresh bundles rather than reuse host dist output. Trusted Server's build.rs
embeds those bundles; after the explicit asset build, `TSJS_SKIP_BUILD=1` avoids
an implicit npm retry. Select its native package `trusted-server-adapter-axum`
and binary `trusted-server-axum`, not its Fastly default member or operator CLI.
Check bundle completeness and embedded bytes. This customization is guidance,
not a claim that Trusted Server's bind/config/health behavior or image has been
validated. Node stays out of the final runtime.

### Packaging verification and limits

From an EdgeZero checkout, the opt-in smoke accepts a **disposable generated
workspace** after the preparation above:

```sh
./scripts/test_generated_axum_container.sh /path/to/container-probe
```

It requires Docker, Node.js 24.12.0, jq, curl, OpenSSL, readelf and preparation network access.
It tests actual context exclusions, final filesystem/native libraries, UID,
read-only root, dropped capabilities, explicit mount permissions, published-port
HTTP and the real proxy's controlled TLS trust and hostname rejection. It uses
private test CA material, not production secrets or public upstreams, and cleans
up its containers/network/image. Failure diagnostics are retained in a temporary
directory, with private test keys removed.

For unmerged framework changes, a separate disposable fixture can explicitly
stage EdgeZero sources and point test-only dependencies within the context.
Include all needed workspace manifests and the CLI's optional app-demo source
path. Resolve that fixture's own lockfile. This local-source evidence is not a
substitute for a standalone consumer with portable Git references.

These are packaging checks, not production certification. The current runtime
still needs accepted PR #275 integration and the lifecycle/config work tracked
by #392/#396 for readiness, required-config failure, durable restart and bounded
SIGTERM draining. A response from `/` is not application readiness, and exec
startup alone does not implement signal handling. #399 owns native amd64/arm64
CI and publishing guidance; #400 owns Docker/Kubernetes/cloud operations.
Application owners publish their own images. No public EdgeZero image is needed.

See [Publishing Axum containers](../publishing-axum-containers.md) for native
architecture CI, artifact identity, the strict scan gate and the app-owned GHCR
recipe. [Deploying Axum containers](../deploying-axum-containers.md) covers the
no-store Docker/Compose example and the remaining runtime, Kubernetes and
ECS/Fargate acceptance gates.

## Configuration

Configure the Axum adapter in `edgezero.toml`. See [Configuration](/guide/configuration) for the full
manifest reference.

The `axum.toml` file is used by the Axum CLI helper to locate the crate and carry a
default port. `edgezero serve --adapter axum` resolves the bind address with this
precedence, highest first: the `EDGEZERO__ADAPTER__HOST` / `EDGEZERO__ADAPTER__PORT`
environment variables, then `[adapters.axum.adapter]` in `edgezero.toml`, then
`axum.toml`, then `127.0.0.1:8787`. The CLI passes the resolved address to the child
process as `EDGEZERO__ADAPTER__HOST` / `EDGEZERO__ADAPTER__PORT`. Running the binary
directly bypasses that resolution: it reads only those two environment variables and
otherwise falls back to `127.0.0.1:8787`.

## Development Workflow

A typical development workflow:

1. **Run locally**: `edgezero serve --adapter axum`
2. **Make changes** to handlers in `my-app-core`
3. **Test locally** with curl or browser
4. **Run tests**: `cargo test`
5. **Build for edge**: `edgezero build --adapter fastly`
6. **Deploy**: `edgezero deploy --adapter fastly`

## Differences from Edge Adapters

| Aspect      | Axum                            | Fastly/Cloudflare |
| ----------- | ------------------------------- | ----------------- |
| Compilation | Native                          | Wasm              |
| Cold start  | ~0ms                            | ~0ms (Wasm)       |
| Memory      | Operator configured             | Provider quota    |
| Filesystem  | Full access                     | Sandboxed         |
| Network     | Direct                          | Backend/fetch     |
| Concurrency | Bounded, connection-local async | Runtime dependent |

::: tip Development Parity
While Axum provides a convenient development environment, always test on actual edge platforms before deploying. Provider-specific behaviour such as store backends and request context differs on the real targets.
:::

## Application lifetime

`dev_server::run_app` already constructs one app per server startup and serves
clones of its router. No additional reuse option is needed. Retained application
state must support concurrent requests; each request gets its own metadata,
extensions, and body. Restarting the server creates a fresh app. Native Axum
measurements are a useful ownership reference but do not establish WASM runtime
performance or resource lifetimes.

## Next Steps

- Configure [native diagnostics and trusted proxies](/guide/axum-diagnostics)
- Deploy to [Fastly Compute](/guide/adapters/fastly) for production
- Deploy to [Cloudflare Workers](/guide/adapters/cloudflare) as an alternative
- Explore [Configuration](/guide/configuration) for manifest options

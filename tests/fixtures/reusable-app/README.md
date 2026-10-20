# Reusable application lifecycle fixtures

This standalone Cargo workspace tests provider HTTP behavior without changing
normal generated entry points. Run from the repository root:

```sh
cargo test --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-harness -p fixture-core
./scripts/smoke_test_reusable_app.sh --adapter fastly --suite smoke
./scripts/smoke_test_reusable_app.sh --adapter cloudflare --suite smoke
./scripts/smoke_test_reusable_app.sh --adapter spin --suite smoke
./scripts/smoke_test_reusable_app.sh --adapter axum --suite smoke
```

The driver is the native Rust `fixture-harness` crate. Its HTTP clients, controlled
loopback backend, process management, and evidence checks run outside the WASM
applications. The shell script only launches it through Cargo.

## Outbound Cutover Gates

The acceptance inventory is `outbound-cutover-cases.json`. It records acceptance criteria,
including the unnumbered lists and consumer end-to-end prose checks, expanded into individual
environment legs. Source and criterion SHA-256 digests require explicit review when the
requirements change. A split criterion retains its source ID and adds a letter to its case ID;
cache parity and fresh-execution pooling are separate cases, not one passing result.
The checker's reviewed leg-set digest also rejects deleted environment/split legs. Supplemental
template/demo checks cite normative EZ-24.12 directly rather than reusing a consumer assertion.

```sh
node --test scripts/check_outbound_cutover_evidence.test.mjs
node scripts/check_outbound_cutover_evidence.mjs --manifest
node scripts/check_outbound_cutover_evidence.mjs --help
```

Run these commands from the repository root. Manifest validation accepts pending decisions and
planned commands; it is not qualification. Per-case sentinels and `outbound-cutover` commands are
implementation targets, not existing passing tests. `consumer-runbook` and `hosted-runbook` denote
external execution recipes whose owners must replace them with exact runnable commands before
setting a leg to `ready`. No staging deployment or approval is inferred from this inventory.

Release validation is a different, fail-closed command:

```sh
node scripts/check_outbound_cutover_evidence.mjs --release \
  --candidate <40-character-source-SHA> --record RECORD.json --artifacts EVIDENCE_DIRECTORY
```

The record must contain the same candidate SHA in every required result, exact command/sentinel/
source digest/artifact path, positive sample counts, passing assertions and numeric measurement
bounds. Each artifact is checked against its SHA-256 under the evidence directory. An explicit
applicability entry is required for every leg. Deferred follow-ups name the unmet condition or an
attributed, leg-scoped approval; a sibling approval never adopts an entire requirement range.
Blocking work cannot be skipped because it is unimplemented or lacks an entitlement.

The record also names the pinned toolchain, workerd revision, runner image and baseline/candidate
comparison identities where required. Retention must be a non-expiring repository or release asset
for the lifetime of the pin, not a CI artifact. This validator checks recorded facts and hashes;
it does not authenticate telemetry, approvers, deployment identities or continued remote retention.
Independent owner review remains necessary.

### Fastly Readiness Diagnostic

```sh
scripts/smoke_test_reusable_app.sh --adapter fastly --suite outbound-readiness
```

This opt-in suite uses direct pinned SDK/ABI calls only in the diagnostic fixture. It runs three
pairs against a controlled, truncated loopback response. Each pair compares a direct one-byte
read control with a timed readiness check before every one-byte read. It records head waits,
readiness waits, read outcomes and timings; zero samples or an unbounded readiness timeout fail.
The direct control intentionally uses a blocking read against an origin that closes after 50 ms;
it is never a proposed production fallback. A stuck experiment is terminated by the harness's
finite client timeout and runtime process-group cleanup, not counted as successful cancellation.

Pinned Viceroy 0.21.0 currently loses the body error on the readiness path, so this diagnostic
intentionally exits 1. It is not part of normal smoke or unit-test gates. Even if the paired control
later passes, this initial suite exits 2 until the remaining G2 scenarios are implemented and run.
Working-tree characterization is labelled as such, never as exact-commit release evidence.
See the [feasibility report](../../../docs/audits/2026-10-18-outbound-feasibility.md).

### Scheduling and Empty-Body Characterization

```sh
scripts/smoke_test_reusable_app.sh --adapter all --suite outbound-progress
scripts/smoke_test_reusable_app.sh --adapter all --suite smoke
```

The default-off `progress` feature selects fixture-only SDK probes, independent of the hosted
`qualification` application. Smoke also builds and runs these probes after its existing cases;
their artifacts are staged separately. Eager single/batch SDK starts are compared with the
existing lazy public single/batch APIs during a measured 100 ms unrelated wait. Origin-local
receipt timestamps and consumer poll counts are checked without comparing guest/host clock
epochs. A response-owned stream must reach the client before the gated origin can complete.
Fastly's synchronous SDK dispatch and guest sleep are not a background guest executor proof.

Five raw HTTP/1 inputs exercise absent framing, explicit zero length, terminating chunks, and
nonempty fixed/chunked controls. Host snapshots do not consume or replace body data. Native raw
signals are unavailable at the existing entrypoint; Spin hints are explicitly not raw framing
evidence. HTTP/2/3, hosted behavior and provider scheduler-context rejection remain unverified.

The initial local run passed on native/Viceroy but exposed a Spin response-owned egress defect.
Separate ready/timer-only adapter streams and SDK-native controls isolated early successful
host observation being mistaken for transport failure. After the internal fix, Spin passes
all five progress, five framing and four comparison cases. Token-correlated SDK terminal evidence
is required from canonical component stderr; missing, duplicated, failed or foreign completions
and guest panic records fail the check. It is revalidated after the finite pre-shutdown hold.
Host worker-panic text before controlled shutdown fails; post-shutdown observations are recorded
separately and are not proof of panic-free teardown. The original failed evidence remains retained;
these local cases do not qualify hosted Spin or weaken G2. A subsequent Cloudflare run with the
exact matching wasm-bindgen CLI passed 24 coding, 46 marker, five scheduling and five framing
checks on local workerd 2026-09-10. The framing checks require a pre-conversion boolean body
signal, not just matching byte counts. Coding/marker failures do not skip the independent progress
group; any failed group still fails the run. Compilation is not runtime proof. See the
[feasibility report](../../../docs/audits/2026-10-18-outbound-feasibility.md) for exact versions,
source identities, retained evidence and remaining hosted/relay limitations.

Cloudflare builds require explicit absolute `WASM_BINDGEN_BIN`, `ESBUILD_BIN` and `WASM_OPT_BIN`
executable overrides. The wasm-bindgen version must match the locked crate. Preflight refuses
implicit tool downloads or dependency repins; missing tools produce unverified exit 2, not pass.
Actual failed cases produce exit 1, including when another adapter also has unavailable tooling.
The retained local run uses `WRANGLER_LOG_PATH` in its temporary tool directory and
`WRANGLER_SEND_METRICS=false`; Wrangler runs `dev --local` with loopback-only listening and local
fixture bindings. It neither deploys a Worker nor accesses production bindings.

### Encoded Downstream Relay

The same `outbound-progress` suite now adds 24 `relay-*` cases per adapter. They share the coding
probe's validated loopback target and outbound caps, disable only synthetic marker insertion on
the cloned client, and return `OutboundResponse::into_response()` directly. No body collection or
JSON report substitutes for downstream transmission. The TCP reader captures binary payloads
without UTF-8 conversion or content decoding, with a 256-byte capture cap. Tests cover exact-cap
and overflow for fixed, chunked and close-delimited payloads, plus fixed/chunked truncation.

The matrix crosses gzip/deflate/Brotli, buffered/streamed output, Original/200, Original/206,
Decode/206 and Decode/200. The 18 raw cases retain original bytes and surviving representation
fields, including range metadata; six decode controls verify plain bytes. Wire framing cannot
contain conflicting Content-Length/Transfer-Encoding. Host chunking is allowed, and any observed
Content-Length must match the delivered bytes.

Each runtime now runs 104 base checks and asserts 113 base origin receipts. Spin additionally runs
two SDK header controls and four response-stream comparison controls, producing 110 client
completions. Relay coverage retains its exact 24 ordered origin receipts. All 16 group-failure
combinations preserve independent execution and overall failure. A reader error remains fatal and
group-labelled, but may precede a per-case relay record. These local cases do not prove inbound
GET/HEAD forwarding, new eager/head-first APIs, hosted qualification or Fastly readiness.

## Opt-in Hosted Response Qualification

The default-off `qualification` feature selects a synthetic application through the standard
entrypoints. Use only a designated isolated nonproduction service with complete unsampled logging
at `Info` or above, including the `edgezero_qualification` target. No production stores, traffic,
credentials or log exports should be reused. Without this feature, normal fixture applications
remain selected; smoke separately builds the `progress` probes described above.

Compile from a clean committed checkout with recorded build identity:

```sh
export EDGEZERO_QUALIFICATION_REVISION="$(git rev-parse HEAD)"
export EDGEZERO_QUALIFICATION_ARTIFACT="$(shasum -a 256 tests/fixtures/reusable-app/Cargo.lock | awk '{print $1}')"
cargo build --locked --release --manifest-path tests/fixtures/reusable-app/Cargo.toml \
  -p fixture-cloudflare --features qualification --target wasm32-unknown-unknown
cargo build --locked --release --manifest-path tests/fixtures/reusable-app/Cargo.toml \
  -p fixture-fastly --features qualification --bin fixture-fastly-qualification --target wasm32-wasip1
cargo build --locked --release --manifest-path tests/fixtures/reusable-app/Cargo.toml \
  -p fixture-spin --features qualification --target wasm32-wasip2
cargo build --locked --release --manifest-path tests/fixtures/reusable-app/Cargo.toml \
  -p fixture-axum --features qualification
```

Deploy the selected artifact through the platform's normal isolated-service path. Record the
actual binary SHA-256 and deployment identity after build. `artifact` below is the compiled
lockfile/build-input digest, not the binary hash: a binary cannot embed its own hash. Record an
unavailable runtime revision as a reasoned unknown, not an invented version.

Prepare a target JSON object using actual recorded identities:

```json
{
  "origin": "https://isolated-qualification.example.com",
  "allowed_origin": "https://isolated-qualification.example.com",
  "isolated_nonproduction": true,
  "run": "qualification-1",
  "adapter": "cloudflare",
  "revision": "<40-character committed source SHA>",
  "artifact": "<64-character compiled build-input SHA-256>",
  "binary_sha256": "<64-character deployed artifact SHA-256>",
  "sdk": "worker 0.8.5",
  "runtime": { "unknown": "provider-unpublished" },
  "compatibility": { "date": "<actual deployed date>", "flags": ["enable_request_signal", "enable_weak_ref"] },
  "deployment": "<independently recorded isolated deployment identity>",
  "max_probe_ms": 2000
}
```

Other adapters use their actual SDK name/version and a compatibility object with a `flags`
array. Fastly additionally requires `execution_ceiling_ms` (1..60000), enforced independently by
the host: guest checks cannot wake a permanently pending synchronous source. The runner waits
for that ceiling before recovery where necessary. The server response budget is 500 ms; the
client budget must be 1000..10000 ms. Thirteen scenarios plus a healthy request after each run
sequentially, without redirects/retries, with a 64 MiB observed-transfer cap (plus one Node stream
chunk on crossing). No outbound backend is contacted.

```sh
node scripts/qualify_response_egress.mjs --execute-hosted TARGET.json CLIENTS.json
node scripts/qualify_response_egress.mjs --validate CLIENTS.json EVENTS.json
```

Client collection intentionally exits 2: it is not qualification by itself. Export the complete
unsampled events as a JSON array in `EVENTS.json`, preserving all records/sequences and excluding
other runs; independently confirm completeness before setting the recorded manifest's
`telemetry_complete` to true. Missing/duplicate/conflicting records remain unverified. Evidence
files are capped at 4 MiB and output uses private file permissions. The validator does not
authenticate fabricated telemetry or prove the supplied deployment identity.

Each probe emits source-drop, terminal, resource-drop and final counters for one late-bound
completion resource. `HostHandoff` remains distinct from delivery. Success qualifies ownership
only for the scheduled probes, not full Q4: instance identity is `unobserved`, so a healthy next
request does not prove same-isolate capacity recovery. Actual transport failures, host teardown,
raw parser behavior and provider memory need separate evidence. No capability is promoted by
these tools, local tests or successful compilation. The response-egress specification maintains
the per-target evidence matrix and limitations.

## Understand the change in five minutes

The opt-in `outbound-progress` suite also runs 24 controlled-origin coding checks per target:
gzip, deflate and Brotli; buffered and streamed responses; Original/200, Original/206, Decode/206
and Decode/200 controls. It compares exact delivered bytes and surviving coding/length metadata
through each actual adapter client. Original and 206 use decoder budget zero and decoded cap one.
These JSON-report probes characterize outbound delivery, not encoded downstream relay framing or
hosted qualification. Cloudflare still requires the exact explicitly supplied build tools.

It also runs 46 marker-policy checks: inherited-off/explicit-off/explicit-on crossed with absent,
upstream and Connection-nominated markers on GET/200, GET/204, HEAD/200, GET/304 and GET/302, plus
a mixed-policy batch. Returned fields must agree with the client inspector. Spin adds two direct
SDK header snapshots before EdgeZero conversion. If the host hides Connection while retaining the
nominated marker, the record labels the affected cases `provider_hidden_nomination`; those passes
characterize adapter-visible fields, not raw-header fidelity. No provider workaround reconstructs
missing fields, and these cases do not qualify header allocation or hosted cancellation bounds.

Think of the app as the route table plus any state you attach to it. A request
still brings its own headers, body, extensions, and provider resources.

```text
Existing Fastly entry point:
  request 1 → initialize → build app → dispatch → sandbox ends
  request 2 → initialize → build app → dispatch → sandbox ends

Opt-in retained Fastly app, when the host reuses the sandbox:
  request 1 → initialize → build app → fresh request setup → dispatch
  request 2 →                        fresh request setup → dispatch
  request 3 →                        fresh request setup → dispatch
```

Responses should behave the same. The visible difference is fewer construction
calls, which can reduce latency when initialization is expensive. Reuse is a host
decision: the next request may always need to initialize again.

Start with these entry points in `crates/fixture-fastly/src/bin/`:

| Experiment | Source                   | What survives between callbacks?   |
| ---------- | ------------------------ | ---------------------------------- |
| A          | `single_request.rs`      | Nothing from the previous sandbox  |
| B          | `rebuild_per_request.rs` | Sandbox globals; app is rebuilt    |
| C          | `retained_app.rs`        | Sandbox globals and the app/router |

The `custom_` versions use the same three lifetimes with request preparation,
response finalization, adapter-owned streaming, and post-send work. A/B/C remain
short labels in experiment reports only.

With the pinned Viceroy installed, run a small synthetic comparison:

```sh
./scripts/smoke_test_reusable_app.sh --adapter fastly --suite benchmark \
  --requests 20 --repetitions 1 --construction-rounds 10000
```

The reported evidence directory contains `events.jsonl` and `summary.json`.
For matched probe responses, inspect `guest.instance`, `guest.ordinal`,
`guest.builds`, and `guest.shared`:

- A: a new instance, ordinal 1, and one build on every request.
- B: the same instance can have ordinal 2 and two builds.
- C: the same instance can have ordinal 2 while builds stays at one.

Compare B and C's reused-request latency to see the construction cost being
removed. The extra JSON parsing deliberately makes that cost visible; it is not
a prediction of an application's production performance. Token, path, and body
checks confirm that each request still sees its own data. Shared state equals the
ordinal in retained probe sequences and one in per-request modes.

For production code, read `serve_app` and `serve_app_with_hooks` in the Fastly
adapter: they retain one successfully configured app and perform fresh request
setup inside each SDK callback. Construction uses
`App::build::<A>(FASTLY_PLATFORM) -> Result<App, EdgeError>`, not an infallible hook builder.
Cloudflare and Spin instead expose `dispatch_app` and let the caller retain a
concrete app. Axum already retains its app. Existing entry points keep their
lifecycle defaults until an application explicitly adopts retention.

Builds use `$CARGO` (falling back to `cargo`) and pin `CARGO_TARGET_DIR` to this
workspace's `target/`, regardless of external Cargo target-dir settings. Update
this workspace's lockfile alongside the root and app-demo lockfiles when bumping
shared dependencies, and keep its Fastly SDK requirement aligned with the root.

Select executables with `VICEROY_BIN`, `WORKER_BUILD_BIN`, `WRANGLER_BIN`, and
`SPIN_BIN`. Install tools explicitly; the runner never deploys or installs them.
The PR pins Fastly SDK 0.13.1, Viceroy 0.21.0, `worker` 0.8.5, and `spin-sdk` 7.0.0
using its `wasip3` re-export; the repository pins Spin CLI 4.1.0. Use a worker-build
version compatible with the locked worker/wasm-bindgen graph rather than assuming
an older builder remains compatible. The runner builds
once with the selected `WORKER_BUILD_BIN`, then starts Wrangler with a snapshot
that disables automatic rebuilding. Axum's executable path is read from Cargo's
artifact output, so configured build targets cannot select a stale default binary.

The runner binds loopback servers, uses synthetic values, seeds only local stores,
and creates an ignored `.runs/<unique-id>` directory. An explicit `--output` must
be empty to prevent evidence loss. Generated provider artifacts are ignored. Do
not expose these diagnostic applications publicly. Their synthetic request tokens
are fixture identities, not a production request-ID generation scheme.

Exit 0 means the requested implemented assertions passed; 1 means a build or
assertion failed; 2 means required runtime evidence is unsupported/unverified.
Raw JSONL, runtime logs, and a summary are retained. The summary includes only
matched probe samples, separated into cold/reused cohorts. SDK attempts, guest
completion, and client completion are distinct. Missing final summaries after a
crash are not zero attempted requests.

## Fastly comparisons

The provider/application boundary is documented in
[`Custom lifecycle contract`](../../../docs/guide/adapters/fastly.md#custom-lifecycle-contract).
This workspace uses path dependencies on the checkout under test. The Fastly
smoke suite additionally runs a fresh custom guest through health, a handled
initialization failure, another health request, successful initialization, and
retained reuse. The sequence must share one guest to establish recovery; early
guest retirement is reported as unverified. Router-produced finalization metadata
carries each request's token, so finalization also checks response-extension
preservation and isolation rather than merely setting a constant header.

- A: original single-request helper.
- B: `Serve`, once-only logging, app construction on each callback.
- C: production retained-app helper.
- Custom A/B/C: EdgeZero `lifecycle::run_custom` / `serve_custom` and `Sandbox<App>`
  own the serving boundary and successful-only initialization. Per-request mode uses a fresh
  state slot per callback; C uses the retained slot. Callbacks return `Result<(), E>`
  and own delivery by explicitly sending native replies or invoking
  `send_request_with_registries_and_hooks(app, stores, req, &runtime.env, prepare, finalize)`.
  The preparation hook mutates the native request and extensions; the routed-response
  finalizer runs before egress policy and framing. The adapter owns appended headers,
  progressive body writes, and terminal close/abandon without whole-body collection.
  Routed finalizer state is available as `Some` only after terminal delivery;
  detached admission responses skip finalization and return `None`. Complete
  post-send work before returning. Neither wrapper requests a synthetic SDK send;
  `run_custom` returns the callback result directly.

Callbacks that dispatch the app use baked `Hooks::logging_for("fastly")` settings
for logger setup and reopen stores under their stable logical aliases per request.
Deployment selectors choose physical links, not runtime configuration. Native request bodies, extensions, store handles,
and pending work are not retained with the app.

```sh
./scripts/smoke_test_reusable_app.sh --adapter fastly --suite benchmark
./scripts/smoke_test_reusable_app.sh --adapter fastly --suite benchmark --construction-rounds 10000
```

Defaults are three repetitions of 100 probes per variant, with randomized order
and a recorded seed. `--construction-rounds` adds deterministic JSON parsing
inside app construction; it models expensive initialization without embedding
application business logic. This synthetic workload cannot predict adoption gains.
Initialization events record wall time and supported SDK CPU/heap observations.
SDK CPU phase readings are not valid cross-run benchmarks. Heap snapshots include
host resources and are rounded MiB values; flat readings do not rule out small
leaks. Latency quantiles use nearest rank and retain sample counts.

The smoke checks stable guest identity, increasing ordinals, build counts,
request-local values, named/default binding reads, duplicate cookies, ordinary
rendered errors, idle reinitialization, supported request/lifetime/memory limits,
progressive chunks, interrupted streams, finite distinct loopback origins, and
exclusion of the terminated guest after an injected panic. Logging checks run on
B, C, custom C and the negative control. Custom C installs its logger with
`Sandbox::setup_once`, so they also imply setup runs once per guest across
retained callbacks. They derive service-scoped keys from an observed guest service
ID and distinguish `fixture-logs ::` endpoint records from echoed stdout. The
negative control verifies that naively wrapping `run_app` with an enabled global
logger fails on its second installation.

The interrupted-upstream case waits until the client observes its partial body before
releasing the backend abort, proving physical progressive delivery. The immediate
zero-byte source-failure case instead requires exactly one response-scoped terminal
report (`SourceError`, application body, zero accepted bytes, no fallback) and terminal
guest eviction. The host may reset before its head reaches the client; only EOF/reset
with no observed status is accepted, not timeouts, malformed responses, or replacement
error responses. Neither proof upgrades the documented egress capability guarantees.

## Runtime and measurement limits

Raising `--max-requests` does not guarantee a longer guest lifetime. Any observed
Viceroy per-guest ceiling is local evidence, not a deployed guarantee or a promise
that Viceroy 0.21.0 reproduces an older runtime's ceiling. Verify deployed eviction
and long-lived memory behavior separately. The runner checks the Viceroy version against
`.tool-versions` and reports a mismatch as unverified.

Building with `spin-sdk` 7.0.0 emits the SDK's `wasip3` WASI HTTP interface. A
successful build does not prove the selected Spin CLI 4.1.0 host can execute that
component. Retained dispatch also depends on provider-specific runtime support;
an unavailable host, interface mismatch, or missing HTTP/reuse evidence cannot
count as a passing lifecycle assertion. Do not substitute older SDKs or response
converters to make a provider check appear supported. Build/startup failures remain
failures; tools or runtime evidence the runner explicitly marks unsupported or
unverified remain exit 2.

Cloudflare and Spin may select different guests during overlap or recovery tests;
those observations are unverified. A success requires the same instance and the
specific injected failure. Overlap checks use each request's `before.inflight`
observation for the tested pair.

Spin's `/bindings` config check reads a value the same request seeds, so it proves
only that the store label resolves. Cloudflare (`wrangler kv key put --local`) and
Axum (a seeded file) read values written before the request.

Quantiles summarize successful matched probes only. Health replies and injected
errors are excluded. Each cohort includes per-repetition sample counts and
quantiles so changes between repetitions remain visible. Pooled quantiles alone
are not a variance estimate or evidence of production gains. Rounded memory
snapshots over short guest lifetimes cannot establish absence of small leaks.

## Failure and measurement evidence

The smoke suite exercises actual adapter failures with controlled inputs:

These cases describe fixture coverage, not a recorded pass for the current pinned
SDK/runtime graph. Only correlated evidence from a run of this checkout establishes
an assertion; results from older runtimes do not certify the migrated entry points.

- Panic inside `MeasuredApp::configure` during `App::build`, followed by a request
  in another guest.
- A truncated upstream body used as the inbound body, exercising the canonical
  ingress read/error-response path rather than a public conversion helper.
- An erroring core body stream, exercising terminal response-egress failure and
  close/abandon without collecting the whole response first.
- A missing required KV selector terminating the SDK loop, with an attempted
  request summary. A separate custom policy catches that error and proves
  successful bindings and retained app state on the following request.
- Invalid HTTP framing rejected by the host before dispatch, distinguished from
  the observed adapter conversion failure.
- A response with headers and partial body followed by stream interruption and a
  correlated guest error. Connection failure alone cannot satisfy this check.
- Progressive 256-KiB streams, correlated post-send work, repeated and distinct
  loopback origins, and an elapsed lifetime limit during initialization.

`summary.json` groups observations by variant, repetition, and guest. It reports
initialization phases, SDK attempts, recorded client completion, CPU deltas,
and memory change/peak/slope/last-three-sample plateau over actual ordinals.
Missing phase events remain unknown. Historical event labels such as
`conversion_completed` identify only the instrumented resource lifetime; they
are not evidence of whole-body collection, exact response commitment, or complete
cleanup. Custom callbacks additionally record terminal delivery and guest completion
where instrumented. These are different observation points. A standard request-start sample occurs after
initialization, while the custom callback sample occurs before it.

Each run saves its lockfile, configuration, compiler/runtime identity, and binary
fingerprints. Built artifacts are copied into `<run>/artifacts/` right after the
build; runtimes serve and fingerprint those copies. Fingerprints identify artifacts; they are not security checksums.
A larger, still bounded observation run can use:

```sh
./scripts/smoke_test_reusable_app.sh --adapter fastly --suite benchmark \
  --requests 300 --repetitions 1 --max-requests 100
```

`--max-requests` defaults to 10 and accepts 1–1000. Host eviction can shorten any
observed guest lifetime. Rounded MiB snapshots and a short plateau cannot prove
absence of small leaks or bounded growth for an arbitrary application.

## Evidence requiring a different environment or API

These are explicitly unverified, not assertions counted as passed:

| Case                                                                                   | Available evidence / limitation                                                                                                                                                                                                         |
| -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Optional runtime-store open fails, then recovers in the same guest                     | Host tests inject degraded then successful configuration snapshots; required-registry recovery is exercised above. Local runtimes cannot make the fixed optional store fail transiently.                                                |
| Spin WASI HTTP interface or provider runtime unavailable                               | SDK compilation alone does not establish host support or reuse. Preserve build/startup failures and explicitly unsupported/unverified records; no older-component compatibility path is used.                                           |
| `EdgeError` itself fails to render                                                     | The renderer builds fixed valid statuses/headers from JSON values; no public input or injection hook triggers a returned rendering error.                                                                                               |
| Unavailable heap hostcall                                                              | Only the preflight is implemented. Under a memory limit, a failed heap snapshot makes the SDK stop after the first request. Local runtimes supply the hostcall and cannot disable it.                                                   |
| Full standard callback/send/cleanup timing                                             | The public retained helper has no post-send callback; its finalizer runs before delivery and its state is discarded. Instrumented resource observations and SDK summary timing are labeled separately; unavailable phases stay unknown. |
| Deployed eviction, endpoint handles, resource accounting, capacity, and workload gains | Require separately authorized deployment and application telemetry. Local CPU samples are not cross-run CPU benchmarks; finite origin tests do not establish service-wide capacity.                                                     |

No deployment is performed by these fixtures.

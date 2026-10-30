# Reusable application lifecycle fixtures

This standalone Cargo workspace tests provider HTTP behavior without changing
normal generated entry points. Run from the repository root:

```sh
cargo test --locked --manifest-path crates/edgezero-test-harness/Cargo.toml
cargo test --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-core
./scripts/smoke_test_reusable_app.sh --adapter fastly --suite smoke
./scripts/smoke_test_reusable_app.sh --adapter cloudflare --suite smoke
./scripts/smoke_test_reusable_app.sh --adapter spin --suite smoke
./scripts/smoke_test_reusable_app.sh --adapter axum --suite smoke
```

The driver is the standalone native Rust `edgezero-test-harness` crate under `crates/`.
It has its own workspace and lockfile, separate from the runtime and fixture applications.
Its HTTP clients, controlled
loopback backend, process management, and evidence checks run outside the WASM
applications. The shell script only launches it through Cargo.

The runner now treats TCP readiness as provisional and requires responder identity on
the first prescribed response, with bounded startup and capture. See the
[startup contract](../../../crates/edgezero-test-harness/README.md#bounded-startup).
Headerless canonical Fastly assembly-error responses cannot currently establish that
identity; those modes remain unverified and do not receive case-evidence credit.
This does not weaken their functional assertions or replace the standard entrypoints.

## Outbound Cutover Gates

### Dispatch-Slack Retirement (2026-10-27)

The requester authorized removing the fixed 25 ms dispatch-slack guard and its test injection
helper. This supersedes the older slack-unchanged and slack-pending statements below, not their
historical measurements or failed observations. Original absolute deadlines, timeout attribution,
late-result rejection, B31 backend/timer policy and resource limits remain in force. Synchronous
SDK host calls remain non-preemptible; no capability level, blocking horizon or qualification is
promoted. EZ-3 inventory legs remain planned until real commands and sentinels exercise their full
assertions. Criterion text, source digests and historical evidence are unchanged.

### Scoped Timing and Integration Override (2026-10-24)

The requester's [scoped approval](../../../docs/audits/2026-10-18-outbound-api-approval.md#scoped-timing-and-native-integration-amendment)
permits the [D1d Send-first checkpoint](../../../docs/superpowers/plans/2026-10-18-trusted-server-outbound-03-drivers.md#send-first-checkpoint-2026-10-24),
not a qualification pass. The +5 ms diagnostic windows/tests remain measured qualification
targets, not hard scheduler guarantees. Keep every late/error observation and historical exit
unchanged; timing-only G2 failure no longer prohibits this scoped implementation. The optimizer
synchronous blocking horizon is explicitly **UNKNOWN** until provider evidence, not inferred from
timers or a Viceroy rejection. Hosted/performance qualification stays blocked.

The checkpoint uses the current trait and existing `execute`/`process_response`, leaving the
ordinary SDK send branch unchanged. Full EZ-1 settlement, Follow, engine and atomic trait migration
remain open. The later [scoped B31 approval](../../../docs/audits/2026-10-18-outbound-api-approval.md#scoped-b31-implementation-approval)
implements only the existing aggregate backend identity/timer policy; slack, readiness and resource gates remain open;
typed body-error preservation and fresh-readiness/subsequent-read safety remain requirements.
This override governs older implementation-stop wording below without changing diagnostic
acceptance tests, 255 criteria/511 legs, IDs, statuses, applicability or comparison requirements.

### Focused Assembly Checks

```sh
scripts/smoke_test_reusable_app.sh --adapter fastly --suite assembly
scripts/smoke_test_reusable_app.sh --adapter cloudflare --suite assembly
scripts/smoke_test_reusable_app.sh --adapter spin --suite assembly
```

This local-only suite exercises the standard entrypoints and the same app-free canonical
error delivery used by custom retention recipes. It checks typed JSON/status/retry headers,
HEAD suppression, internal redaction and one full boot log per failed attempt. Retained
503-to-200-to-200 cases require two builds, shared instance identity and persistent successful
state. Fastly additionally checks configure-failure state release and detached commitment
reports. Instance turnover never counts as a retry pass. Axum's distinct
fail-before-bind behavior and boot diagnostics are covered by its native startup tests.

Use the explicit Cloudflare tool overrides documented below. Runtime processes, compiled
artifacts and JSONL evidence are scoped to the run directory. Results are working-tree
characterization, not hosted qualification; this suite neither changes capability levels
nor closes the separate Fastly readiness/scheduling gate.

### Qualification Inventory

Start with the implementation plan's
[current delivery dashboard](../../../docs/superpowers/plans/2026-10-18-trusted-server-outbound-implementation.md#current-delivery-dashboard-2026-10-27)
for shipped checkpoints, unfinished implementation and blockers. Its requirement table tracks
task-level progress; this inventory tracks criterion/environment execution readiness and the
qualification checker validates separate evidence. These are not interchangeable completion claims.

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

Specifically, `implementation: planned | ready` means the declared leg's command/sentinel is
planned or implemented and executable with its full assertions. It does not mean the corresponding
feature is absent or qualified. EZ-23 local criteria 01-12 bind guarded Cargo tests and the
existing Axum CI job. EZ-9 core criteria 01-15 bind all 51 redirect tests, and EZ-6 core criterion
22 binds five mixed-observation/receipt-recovery tests through the same inventory guard.
EZ-6 core criteria 13/15/17 bind facade upload rules, cutoff-equality and recorded-undelivered
cancellation. EZ-7 core criteria 01/02/03 bind marker limits, same-handle inheritance and
upstream-marker preservation. EZ-4 core criterion 01 binds connect-setting carriage; EZ-5 core
criteria 01/02/03/10/11 bind original coding, encoded/final limits, exact partial ranges, identity
limits and shared-head normalization/body disposition. EZ-1 core criterion 07 binds forwarding
with empty chunks, expired budgets, dispatch-lazy POST, buffered/taken and failed-read controls.
EZ-3 core criterion 04 binds the literal two-slot completion/input-order clause, not Fastly's
known-origin registration SHOULD. EZ-9 runtime criteria 16/17 bind the complete four-adapter wire
matrix. EZ-1 Axum HTTP/1 criteria 01/04/08 bind actual forwarding, five-byte rejection with
pipeline safety, and certified-empty forwarding after the inbound deadline. Each adapter's
EZ-5 unit criterion 09 binds its native head/source bodyless matrix; Axum, Cloudflare and Fastly
also bind unit criteria 10/11. Spin unit criterion 10 binds its independent-cap host-wire matrix.
There are 62 ready legs and 449 planned. Spin unit criterion 11,
EZ-6 criterion 22's four adapter legs and EZ-23 criterion 13 stay planned
for their own runtime and consumer-owned gaps. G1/Q3 must still reconcile the other bindings.
A `ready` leg also needs applicable, passing exact-candidate evidence before release credit;
do not promote it solely from a checkpoint test count or green CI.

The EZ-23 ready legs share one full Axum suite command, including ignored tests:

```sh
scripts/run_test_nonzero.sh --include-ignored --inventory axum/native-tcp,unit/axum,axum/http1 cargo test --offline --locked -p edgezero-adapter-axum --no-default-features --features axum,test-utils --all-targets
```

The core legs discover all 80 bound primary/additional sentinels before running one outbound suite:

```sh
scripts/run_test_nonzero.sh --inventory unit/core cargo test --offline --locked -p edgezero-core --features test-utils --lib outbound::
```

The core policy bindings cover actual facade calls, not only validation helpers. A one-second
connect setting retains its full aggregate budget through parts rebuilding and redirect hops.
Upload controls observe rejected Begin source polls and exact transport payloads; the private
recorder's 64-byte drain is only a fixture bound. The off-configured marker handle is exercised
through Send, Begin and exactly two inherited batch slots. Coding bundles preserve exact range
headers and independently exercise encoded, decoded and final caps. These bindings do not close
their provider legs or authorize pending scripted-client or representation-metadata changes.

Shared-head controls inject provider-inexpressible fields before normalization; they do not
certify provider raw-header fidelity. Both decoding choices and response modes retain normalization
precedence, opaque coding passthrough and identity decoded caps. HEAD/304/204 and both 205
dispositions preserve suppression and release source/owner resources before collection. Conditional
metadata criteria 11b/12 remain planned, independently of the core criterion 11 binding.

The native adapter matrices use the production head conversion and SDK body reader, under both
decoding choices and response modes. Spin binds the bodyless matrix through the eager component
and independent-cap criterion 10 through the real host-wire matrix. A generous reporting collector cannot manufacture the
tested final-buffer overflow. Fastly's test-only lease witnesses prove release of EdgeZero's
encoded stream, not host-side interruption or delivery. Provider-inexpressible fields are injected
only at the explicitly labelled shared-head boundary; they do not establish raw-header fidelity.
Cloudflare and Fastly use one inventory-guarded library suite each. Spin's existing WASI component
requires the normal build prerequisite and emits each criterion marker only after its matrix passes:

```sh
cargo build --offline --locked -p edgezero-adapter-spin --no-default-features --features spin,test-utils --target wasm32-wasip2 --example outbound-eager
wasmtime run -W component-model-async=y -S p3=y -S http=y target/wasm32-wasip2/debug/examples/outbound_eager.wasm
```

CI checks every ready `unit/spin` marker against the fresh eager-component and host-wire logs,
retained under `outbound/unit/spin/`. The synthetic
full-body probe hit Wasmtime 48.0.2's unsupported intra-component non-numeric future boundary;
its unqualified failure log is `/private/tmp/edgezero-spin-policy-intra-component-failure.log`.
The unusable probe is removed. Criterion 10 now uses 26 serialized actual-client wire cases:
identity with decoded cap 10 and encoded/final caps 20 under both decoding choices and modes,
declared and observed 10/11-byte bodies, plus original gzip with zero decoder budget and independent
23/22-byte transport/final boundaries. A test-only probe requires actual native-head attachment,
counts source polls and witnesses local source release without retaining the SDK reader. Declared
rejection must occur during send with zero polls; observed streamed overflow occurs during body
collection. The reporter's 1024-byte bound cannot manufacture the expected cap failures.
The marker is emitted only after all 26 distinct cases, their exact origin receipts and runtime/origin
cleanup pass. Criterion 11 still requires extending the existing real host-wire fixture,
not bypassing trailer or completion acknowledgement. A host library test is not a
substitute for that SDK execution. Grouped inventory selection requires at least one
ready sentinel in every selected environment and deduplicates names before the one suite run.
This binds local execution only, without adopting optional metadata or qualifying hosted behavior.

Core criterion 22 uses exactly three slots under Completed, Cutoff and an original typed driver
failure, with both fresh collection and check/next/collect. Terminal production is held until
both mixed receipts transfer. Assertions distinguish `AlreadyDelivered` from unresolved, merge
the original receipts without consuming bodies, preserve error identity and release each body
owner once. Companion tests cover foreign/missing receipts and duplicate/conflicting inputs.
The shared raw Cargo output path is `outbound/unit/core/raw.log`; old redirect-only logs are not
relabelled as evidence for this broadened command. Readiness is not exact-candidate qualification.

Runtime commands select `--suite outbound-wire` and write their JSONL to
`.runs/outbound/redirect/{environment}/events.jsonl`. Each adapter must complete 68 exchanges and
84 ordered origin receipts. Spin additionally records a separate `spin-policy-inventory` with
26 policy exchanges and 26 correlated receipts, without changing the redirect counters or connection
limits. Idle-body policy remains an unadopted conditional leg; these bindings
do not qualify that policy, native optimizer transformation or consumer/hosted behavior.
Axum, Fastly and Spin execute these bindings in their existing CI jobs. Workerd's command is
locally executable; automatic CI runtime activation remains pending explicit setup of its
matching build tools and selected runtime. Green CI alone does not cover that workerd leg.

The existing guard asks the evidence checker for every ready sentinel in that environment and
requires exact discovery of all twelve names before executing the suite once. Criterion 10's
`additional_sentinels` binds its factory test alongside its primary TCP test; no separate CI list
or invocation is needed. Missing tests or an empty ready selection fail before execution; test
failures retain their exit status. Their inventory `.log` paths are future raw Cargo output artifacts,
not JSON result exports. At the frozen candidate, capture stdout and stderr, retain the guarded
exit status and bind the source/build identities through Q3's record assembly. Current local/CI
runs are not a qualification record, and executable follow-ups are not automatically promoted or
confirmed present. Other `outbound-cutover` placeholders remain implementation targets.

The [TS qualification runbook source template](ts-outbound-qualification-runbook.md) separates
implemented preparation/egress commands from missing full-outbound execution/export bindings.
It is unaccepted and unqualified. Q3 must freeze the candidate before Q4a fills an external copy;
actual TS ownership acknowledgement, staging authorization and package acceptance remain open.

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

#### Mandatory Forwarding Example Doctest

Blocking EZ-1.14 has a supplementary compile-only prerequisite owned by Q1 (checker) and
Q2 (workspace CI). Run from the repository root:

```sh
node --test scripts/check_forwarding_example.test.mjs
node scripts/check_forwarding_example.mjs
```

The checker extracts the actual `Forward An Inbound Request` fence in `docs/guide/proxying.md`,
builds core offline/locked and requires exactly one listed and passed non-ignored rustdoc test.
It compiles item definitions with an empty entrypoint; the network handler is never invoked.
Missing/duplicate fences, weakening annotations, zero tests and compile failures fail the gate.
Regression tests include the missing `into_request()?` mutation and embedded-fence bypasses
through real compilation.

This CI prerequisite is outside the reviewed 511 environment legs. It grants no runtime
forwarding acceptance and does not adopt optional EZ-25.42's broader guide checker. P2's real
GET/HEAD/POST behavior tests and provider/consumer qualification remain separate requirements.

### Pre-Freeze Evidence Assembly

Q3 preparation uses the existing checker rather than a second exporter:

```sh
node scripts/check_outbound_cutover_evidence.mjs --assemble \
  --candidate SOURCE_SHA --input OBSERVATIONS.json --artifacts EVIDENCE_DIRECTORY \
  --output NEW_DRAFT.json
node scripts/check_outbound_cutover_evidence.mjs --report \
  --candidate SOURCE_SHA --record NEW_DRAFT.json --artifacts EVIDENCE_DIRECTORY
scripts/run_test_nonzero.sh outbound_evidence_assembly_retains_case_counts_and_hashes cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml outbound_evidence_assembly_retains_case_counts_and_hashes
```

Both assembly and report exit nonzero while release gates are blocked. Successful draft creation
is identified by `record_output` in the report and a new output file; malformed input or an
existing output path produces an error instead. Assembly never overwrites evidence.

Inputs use schema 1 with an explicit `candidate_sha`, `source_state` (`clean` or `working-tree`),
nonempty `builds` and nonempty `results`. Each build supplies its candidate SHA, relative
`artifact_path` and `build_id`, the SHA-256 of that file. Each result supplies its exact inventory
case/environment, candidate SHA, linked `build_id`, source digest, command, sentinel, artifact
path, status (`passed`, `failed` or `pending`), sample count, assertions and measurements.
The tool verifies build hashes and computes raw artifact hashes; supplied hashes must agree.
It preserves observation statuses and sample counts, including failed or pending observations,
without filling absent results, changing applicability or approving a deferred part.

Drafts always have `stage: "pre-freeze"` and `qualified: false`. Runner, toolchain, retention,
comparison and applicability metadata is copied only if supplied; missing fields remain gates.
Working-tree records and explicit `qualification: false` cannot pass release validation.
Release records require `stage: "frozen-candidate"`, `source_state: "clean"` and linked build
identities. These are declared provenance, not proof that a binary was built from the claimed
SHA. Candidate freeze, runtime mapping, approvals and independent evidence review remain required.
Changing a draft's label does not perform those steps.

For a leg marked `comparison: true`, release/report validation requires comparison build
identities and a nonempty valid measurement list. The hosted Image Optimizer leg is marked this
way. This is a basic evidence-presence check, not proof that the required controls, groups,
sampling or acceptance rules were approved or completely exercised. Those still require the
paired native-path record and independent review; arbitrary measurement names do not certify parity.

`--report` lists independently evaluable blockers by leg/rule using the same checks as
`--release`, which still fails closed. Invalid or ambiguous entries receive no acceptance credit
without hiding unrelated observations. Assembly does not interpret generic smoke JSONL as
qualification results or substitute FNV fingerprints for SHA-256. All 511 reviewed inventory
legs retain their existing state. No source freeze or hosted qualification is performed here.

Use stable artifact snapshots: the tool rejects absolute/traversing paths, symlink escapes,
directories and nonregular files. Artifacts are capped at 256 MiB and hashed with a 64 KiB buffer;
JSON inputs/outputs are capped at 16 MiB. It is not a defense against concurrently mutating a
filesystem, and neither verifies telemetry authenticity nor publishes artifacts.

### Fastly Client Progress

```sh
VICEROY_BIN=/absolute/path/to/repaired/viceroy scripts/smoke_test_reusable_app.sh --adapter fastly --suite outbound-client-progress
```

This functional CI suite exercises the actual `FastlyOutboundClient` through `HttpClient`, using
the same guest, bounded origins and validators as the full diagnostic below. Four cases run
three times: pending-head/ready-body, pending-body/delayed-head, and buffered/streamed multi-buffer
truncation. Require twelve probes and eighteen correlated origin requests, exact payload bytes
and checksums, preserved typed source errors, and the ready sibling observed before its cutoff.
Preflight failures cannot count as exercised receive paths.

The same suite also requires a separate P5 inventory: twenty-three redirect probes and
forty-three correlated origin requests. Plain Send stays unfollowed; Follow reaches the second
origin. Buffered and streamed no-redirect controls compare identity, decoded gzip and unsupported-
token passthrough under normal decoding, including exact-cap, overflow and declared-length cases.
A gated pending oversized compressed redirect progresses without origin body data; stopping at
that head applies the ordinary typed cap. Native optimizer Send, initial Follow and a policy-
supplied later hop reach Viceroy's typed unavailable boundary. These are compositional dispatch
controls, not successful optimized-first-hop redirect, transformation or performance evidence.

Both receive suites require `--adapter fastly` and the pinned runtime version. Select the
[tracked Viceroy repair](../../../patches/viceroy/README.md) explicitly; an installed stock binary
is not silently replaced. CI requires exactly exit 2, both passing complete client/P5 inventories,
an explicit unverified disposition and no failure records. Exit 0 or an unavailable runtime
does not satisfy that gate. The suite deliberately excludes SDK timing probes and timer controls.
It does not establish callback-wide progress, host cancellation, memory bounds or qualification.

### Fastly Readiness Diagnostic

```sh
scripts/smoke_test_reusable_app.sh --adapter fastly --suite outbound-readiness
```

The SDK portion of this opt-in suite uses direct pinned SDK/ABI calls in the diagnostic fixture. Eleven cases
cover silent heads, pending/trickled bodies, sibling readiness, exhausted bodies, ordinary/empty
EOF, truncation, malformed heads/chunks and connection closure before a response. Each runs with
buffered and pull-streamed consumption, read capacities 1 and 16384, and three repetitions.
Three additional finite direct-read truncation controls produce 135 probes and 147 origin requests.
Those capacities are diagnostic variations, not an approved production read maximum.

The full command also runs the twelve actual-client cases above. In total, its HTTP inventory
contains 165 probes and 195 origin requests; the SDK baseline and supplementary probes remain
separately validated. Ordinary CI gates only the explicit functional suite. The full diagnostic
retains its strict timing assertions and exit behavior; timing misses remain failures, not a
passing SDK result. It remains required evidence for applicable qualification work.

The same command appends 18 supplementary probes and 30 origin requests, separately validated
from the unchanged historical matrix. `mixed-head-body` completes a ready body while a sibling
never returns a head; `mixed-body-head` observes a delayed head while another body is pending.
`full-body-error` closes after 32769 of 32770 declared bytes. Both consumers run three repetitions
at capacity 16384. Checks require actual mixed selection domains, at least one full-capacity read,
exact payload checksums and a fresh matching selection for every read, including terminal errors.
This tests the candidate capacity with a multi-buffer payload, not general fairness when several
sources remain continuously ready or approval of a production maximum. Baseline and supplementary
failures are both collected before the run's result; known stock errors cannot skip the new cases.

Before the HTTP matrix, timer-only controls run eight repetitions at 1/100/500 ms using
native thread sleep, guest WASI sleep and empty-handle timed ABI select: 72 observations.
Three one-millisecond warm-ups are recorded separately and excluded from those counts.
These controls send no outbound HTTP requests. Lateness flags are derived from measured
elapsed time and retained even when false; they do not change the 135-probe acceptance rules.
Timer-only measurements end at primitive return, before status formatting. Declared ABI
endpoints must agree with that timestamp and cannot report less than the positive timeout.
The two guest controls share Viceroy's asynchronous execution/timer machinery; native sleep
does not exercise its reactor. Differences narrow hypotheses, not prove a scheduling cause.

Raw ABI entry/return timestamps are captured immediately around select, before status formatting.
The outer operation and whole-exchange intervals still include guest overhead and enforce the
same deadline. Paired raw timestamps must lie within their enclosing operation. Earlier retained
traces do not contain this additional instrumentation and are not rewritten.

Every head is selected with a positive remaining timeout, then nonblocking-polled. Every selected
body read requires fresh same-slot readiness. One original 500 ms absolute deadline survives the
stream handoff; trickle never resets it. Traces must finish within +5 ms, retain exact EOF/error
outcomes, and cover the complete inventory with unique correlated origin receipts. Streamed
consumption discards each pulled chunk; buffered consumption retains it under a finite byte cap.
The direct control alone bypasses body readiness against a source that closes after 50 ms;
it is never a production fallback. Backend phase timers are fixed at two seconds, outside the
measured exchange window; backend registration is not timed by this proof.

Each origin owns at most two socket workers, a 4096-byte request-head cap and a three-second
watchdog. Finish/Drop wakes, closes and joins its workers, retaining errors and cleanup reasons.
Harness-forced cleanup is not SDK cancellation evidence. A stuck guest is contained by the
12-second client timeout and runtime process-group cleanup, not counted as successful cancellation.

Pinned Viceroy 0.21.0 loses source errors on the readiness path and fails validation. The suite
is separate from normal smoke/unit gates. Even a complete primitive matrix exits 2: migrated
production paths, exact-version review, backend policy and hosted qualification remain separate.
Working-tree characterization is labelled as such, never as exact-commit release evidence.
See the [feasibility report](../../../docs/audits/2026-10-18-outbound-feasibility.md).

PR #275 also carries an [exact-version local Viceroy repair](../../../patches/viceroy/README.md).
Select its built executable explicitly with `VICEROY_BIN`; the default installed runtime and
version pins are not changed. The separate repair record retains three failing stock controls
and three repaired comparisons preserving UnexpectedEof with identical guest bytes. A repaired
run still exits 2 because full G2/hosted proof is pending; do not treat it as a full gate pass.

### Scheduling and Empty-Body Characterization

```sh
scripts/smoke_test_reusable_app.sh --adapter all --suite outbound-progress
scripts/smoke_test_reusable_app.sh --adapter all --suite smoke
```

### Buffered Response Framing

```sh
scripts/smoke_test_reusable_app.sh --adapter all --suite response-framing
```

This separate suite reuses the progress artifacts and production entrypoints without changing
the outbound scheduling/coding case counts. It requires curl; Fastly additionally requires an
HTTP/2-enabled build for prior-knowledge h2c. Bounded captures record protocol, headers and body
artifacts for literal buffered, empty, explicit-length, stream, suppressed and fallback sources.
Fastly compares framing against a direct pinned-SDK Automatic reference for the same buffered
sources over HTTP/1.1 and HTTP/2. The expired case uses a response-owned absolute deadline.
Each case is attempted independently; a failed case does not hide later observations.
HEAD captures are header-only and report payload observation as unavailable, not a measured empty
body. Failed curl requests retain bounded diagnostics and partial capture artifacts. Short sources
require a terminal HTTP/1 partial-transfer/empty-reply or HTTP/2 stream-error witness; a client
timeout is not a passing abort. Both transfers must succeed before an SDK parity case can pass.

The pinned local SDK's bodyless controls currently fail: restoring a known HEAD body breaks
HTTP/2, forbidden-status SDK payloads are not consistently suppressed, and representation framing
differs from strict suppression. Fastly and `--adapter all` therefore remain nonzero. These failed
controls are retained as open parity gates, not normalized away or inferred to be hosted behavior.

These are working-tree local characterization runs. They do not qualify hosted Fastly compression,
deployed Workers header delivery, consumer response parity, or Fastly outbound G2/P1. Hosted
compression still needs encoded wire counts, coding and decompressed source hashes for buffered
and declared-stream sources on both HTTP versions; Viceroy does not supply that transformation.

### Bounded Synthesized Headers

```sh
scripts/smoke_test_reusable_app.sh --adapter all --suite synthesized-headers
```

This independent suite uses the same production progress entrypoints with suite-only application
configuration. It delivers an entire 32-value/4096-byte configured set: one fixture marker and
31 distinct `Set-Cookie` values, verified in order without comma splitting. GET/HEAD fallbacks
retain the canonical 500 body/suppression behavior. Canonical 404/405 responses are decorated;
matched successes/errors and actual admission refusals are not. The 405 retains its single `Allow`.

Viceroy additionally receives an absolute target over 9 KiB and about 72 KiB of headers in 60 added
lines, below its parser boundaries but above EdgeZero's normalized limits. A correlated detached
decision and exactly one terminal completion prove guest-side 414/431 generation, with no
admission, handler or finalize invocation. Fastly preparation controls replace/clear the app set
or reject a 4097-byte dynamic set without losing the valid app policy. Final logs are rechecked
after runtime teardown; fallback reports must identify conversion failure, the fallback body,
completed disposition and exact GET/HEAD byte counts.

Raw HTTP/1 captures require a complete head and clean EOF, not a timeout or reset. HEAD checks
all post-header octets, including bytes sent with the head or in a later write. The client uses
one absolute five-second exchange deadline and a 16 KiB head/4 KiB body capture envelope.
Cloudflare's local frontend requires a client write-half-close to end this controlled exchange;
the other three use `Connection: close` without an early FIN, which their parsers treat as
cancellation. The selected close mode is recorded. No production transport setting is changed.
Capture/artifact failures are recorded without skipping later cases.

The [local record](../../../docs/audits/evidence/2026-10-23-synthesized-response-headers.json)
contains fingerprints for the passing matrix and retained failed diagnostic runs.
These are local HTTP/1 working-tree observations, not hosted/HTTP2/HTTP3 or frozen-candidate
qualification. The separate `assembly` suite installs policy before deliberate failure and checks
GET/HEAD exclusion, successful recovered-policy 404 delivery and correlated lifecycle evidence.
Axum instead proves its canonical pre-listener startup failure, without constructing a response.
The [assembly record](../../../docs/audits/evidence/2026-10-23-assembly-response-headers.json)
includes the repeated header matrix and final request-correlated assembly evidence.
Consumer-owned header integration remains open. Neither suite reruns nor waives the failed
Fastly SDK parity controls or G2, and does not establish Cloudflare overflow recovery.

The default-off `progress` feature selects fixture-only SDK probes, independent of the hosted
`qualification` application. Smoke also builds and runs these probes after its existing cases;
their artifacts are staged separately. Eager single/batch SDK starts are compared with the
existing lazy public single/batch APIs during a measured 100 ms unrelated wait. Origin-local
receipt timestamps and consumer poll counts are checked without comparing guest/host clock
epochs. A response-owned stream must reach the client before the gated origin can complete.
Fastly's synchronous SDK dispatch and guest sleep are not a background guest executor proof.

The Spin progress suite also runs `/progress/sdk-source-diagnostics`: eight SDK source-race
cases check EOF, pre-expiry, ready errors with/without provider detail and deadline equality.
It awaits the production I/O helper inside the existing async HTTP export; the synchronous WASI
test export cannot wait on SDK timers. The probe creates no downstream writer and does not qualify
hosted delivery or change capability claims. Its test utilities are enabled only by `progress`.

`/progress/sdk-connect-fallback` uses that same async export for five Connect setter outcomes
and four aggregate budget sources. Its 60 checks cover ready success, a non-self-waking operation
released by the real SDK timer, and expired-entry rejection without polling. The captured guest
owner is released on timeout; no refusal detail becomes the later timeout's cause. The injected
timestamp stays frozen during the positive-remaining test, so a clock jump cannot supply progress.
This is guest-race evidence, not independently configured connect enforcement or host teardown.

Each progress target also runs `/progress/framing-trailers` through its ordinary App/converter.
The client must receive the unchanged 500 fallback with no sentinel leakage in headers or body.
On Viceroy the harness additionally requires one `UnsupportedTrailers` terminal report and the
matching error-level fixed-rule log, rejecting leaked sentinel text anywhere in captured logs.
This is local framing-diagnostic evidence, not hosted response-lifetime qualification.

Five raw HTTP/1 inputs exercise absent framing, explicit zero length, terminating chunks, and
nonempty fixed/chunked controls. Host snapshots do not consume or replace body data. Native raw
signals are unavailable at the existing entrypoint; Spin hints are explicitly not raw framing
evidence. HTTP/2/3, hosted behavior and provider scheduler-context rejection remain unverified.

The empty probe transfers the Initial context body with `into_request()` before bounded
collection and reports `body_is_stream` separately from context state. Cloudflare must preserve
the original pre-conversion null-body snapshot as buffered empty for absent/zero-length framing,
and preserve every present source as streamed for chunked-empty and nonempty controls. The
validator rejects missing, mistyped or contradictory storage evidence; byte counts alone cannot
prove this property. First-`body_bytes` deadline precedence has separate adapter regressions.

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

### Request Overflow Characterization

```sh
scripts/smoke_test_reusable_app.sh --adapter all --suite outbound-overflow
```

This separate opt-in suite reuses the `progress` artifacts and standard entrypoints. Each target
attempts eleven cases: chunked ingress at 1023/1024/1025 bytes under a 1024-byte read cap,
buffered and streamed uploads at 7/8/9 bytes under an 8-byte cap, an ordinary upstream 413,
then the exact 2048-byte chunked ingress case required by EZ-10. The original ten-case order is
unchanged, including the overflow-to-next-upload recovery check; earlier failures do not skip
the appended case.
Uploads use the injected production client, not the scheduling probes' direct SDK requests.
Only response heads are observed; bodies are dropped unpolled. This neither enters Fastly's
failed readiness path nor qualifies downstream relay or hosted behavior.

Generated overflow must return the fixed 413 JSON category without Retry-After; an ordinary
upstream 413 must retain its status and `Retry-After: 120` in the head report. Buffered overflow
must not contact the origin on any adapter; streamed overflow must not contact it on Axum or
Cloudflare. Fastly/Spin may instead close an incomplete upload. An explicitly observed zero-byte,
pre-head EOF/reset is also recorded as TCP contact, not "no contact"; a missing receipt or partial
head is not equivalent. Complete request framing, malformed fields, timeout, duplicate contact
or over-cap bytes cannot pass as an abort.

The dedicated origin has one joined worker, two retained connection receipts plus one overflow
witness, a 256-byte body capture plus one witness, 16 KiB combined head/chunk/trailer metadata
and 100 head fields. Each accepted socket has a two-second absolute read/write budget.
Collection observes a further 100 ms before stopping new accepts and joining the worker; up to
two already accepted socket budgets may remain. This is a finite observation, not proof that
provider work can never resume. Client captures are bounded to 4 KiB and reads to six seconds.
First-byte timing is explicitly unmeasured; header/completion times use the client clock.

The retained local matrix passes all eleven cases on Axum, Viceroy and Spin. The exact 2048-byte
ingress case passes on all four targets. On pinned local
Wrangler/Miniflare/workerd, ingress overflow returns 413, but the following valid POST returns a
proxy-generated 500 before fixture entry. A JavaScript-only body-cancellation control reproduces
the follow-on failure without EdgeZero; the native connection-pool mechanism remains unproven.
The suite retains the failure and exits 1, without retries, recovery sleeps, drains or capability
promotion. See [the original evidence](../../../docs/audits/evidence/2026-10-21-outbound-overflow.json)
and [the boundary/recovery checkpoint](../../../docs/audits/evidence/2026-10-21-overflow-boundaries.json).
Omitting `Connection: close` does not eliminate the failure. Diagnostic requests sent directly
to Wrangler's own user-worker listener recover for both the JavaScript control and the actual
EdgeZero artifact. This narrows the failing boundary to the front-proxy path, not a proven native
connection-pool mechanism. That diagnostic topology is not a replacement acceptance entrypoint.
The collector's Drop test gates worker exit and mutation-checks that Drop waits for its join;
it does not certify provider cancellation after the finite observation window.
Cloudflare recovery, hosted HTTP/2 and end-to-end qualification remain open. The separate
[generated-app/demo TCP proof](../../../docs/audits/evidence/2026-10-21-generated-fallback.json)
passes through their unchanged Axum entrypoints: fixed/chunked 4 KiB boundaries on both fallback
routes, exact 413/no-retry output, 405 Allow, the five-second pending-read 408, and a request-looking
body tail sent only after receiving the 413, followed by genuine health/recovery requests.
Its bounded buffered-response reader uses `httparse`; malformed, ambiguous or incomplete framing
cannot pass as connection-close recovery. These are local HTTP/1 observations, not hosted lifecycle
or full resource certification. The existing generated-workspace and bundled-demo CI steps run them.

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
  "compatibility": {
    "date": "<actual deployed date>",
    "flags": ["enable_request_signal", "enable_weak_ref"]
  },
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

Custom callbacks retain the actual dispatch-selected HTTP client for post-send work, rather
than constructing a second client or overriding request injection in preparation. A request-local
capture is filled before middleware awaits the handler, so routed handler errors still retain
the selected client. State-client precedence is unchanged. Detached/fallback delivery keeps its
existing post-send restrictions; a missing capture never creates a substitute transport.

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
from `stackpop/spin-rust-sdk` at `6c59ed5b4fcb377072763aee0b27a3abdd8edb72`, using its
`wasip3 0.10.0` / `wit-bindgen 0.63.0` re-export and `spawn_local`. See the
[dependency audit](../../../docs/audits/2026-10-27-spin-sdk-eager-progress.md).
The repository pins Spin CLI 4.1.0. Use a worker-build
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
  `send_request_with_registries_and_hooks(app, stores, req, prepare, finalize)`.
  The preparation hook mutates the native request and extensions; the routed-response
  finalizer runs before egress policy and framing. The adapter owns appended headers,
  progressive body writes, and terminal close/abandon without whole-body collection.
  Its owned `FastlyRequestDelivery<State>` distinguishes NoAttempt, policy Aborted,
  Detached and Routed. Routed state survives delivery failure; detached responses
  skip finalization. Attempted results carry the exact completion report and the
  last selected body's NotCommitted/Unknown/Committed state. Only NoAttempt or
  confirmed NotCommitted permit native replacement; completed attempt accounting
  never includes that replacement. Complete
  post-send work before returning. Neither wrapper requests a synthetic SDK send;
  `run_custom` returns the callback result directly.

The custom stream checks retain the finalizer's request-local client and complete
post-send work after a committed application-source failure. A separate framing
rejection delivers the canonical fallback and must not start application follow-up.
Each attempted result must match exactly one correlated egress terminal event;
required-store NoAttempt results must produce none. Host acceptance is not client
delivery, and local Viceroy evidence is not hosted qualification. Synthetic
`delivery::testing` fixtures exercise the shared finisher, not SDK head conversion
or host transport.

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

The retained serverless smoke fixtures also declare an unlinked KV ID alongside the
healthy default. `/declared-kv/free`, two `/declared-kv/missing/read` requests, then
`/declared-kv/free` must produce 200/500/500/200 in one observed instance, with warning
counts 0/1/1/1. A separate linked-store write/read/list/delete sequence checks continuity.
Failed write/delete/list requests follow that unchanged four-request prefix. The linked-store
sequence also reads and lists after delete, requiring `null` and an empty list. All 13 requests
per serverless adapter require exactly one actual egress terminal. Operation handlers emit one
entry and one typed-result receipt derived from the actual `KvError`, not the rendered status;
the KV-free requests must emit neither. Start, entry, result and terminal events must be ordered;
the post-delete read must contain an explicit `kv_outcome: null`, not merely omit that field.
Missing bindings must retain the exact sanitized 500 JSON.
Every token, instance, typed result and terminal count is checked again after runtime shutdown,
including when an earlier smoke check failed. Fastly endpoint receipts are distinguished from stdout echoes;
Cloudflare/Spin use a fixture-owned logger with instance-tagged structured receipts.
Pinned Viceroy 0.21.0 accepts at most five follow-up requests in one guest, so the additional
operations may naturally cross guests. The original four-request prefix still requires one
instance; the extension records every observed instance without restarting or retrying that prefix.
Cloudflare/Spin require one warning per missing-operation instance and none for healthy-only
instances. Fastly's named-endpoint warnings have no instance tag: after each request and shutdown,
their aggregate count must equal the number of observed missing-operation instances. That checks
sequential totals, not independent attribution of late warnings across different instances.
This is local host characterization, not a hosted reuse guarantee.
Fastly reports `Completed`, Cloudflare/Spin `HostHandoff`; neither qualifies peer delivery.
SDK/source-harness tests separately cover all failed operations, typed reasons, and
fresh-request recovery. Linked-store continuity is not a provider outage/restoration proof.

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

The adapter's local Spin wire controls run in the existing Rust harness, using the unchanged
guest and pinned Spin. The runner builds and stages that guest; there is no separate host-language
runner or alternate outbound client:

```sh
cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml
./scripts/smoke_test_reusable_app.sh --adapter spin --suite outbound-wire
```

These controls preserve raw request framing, scope/handoff checks and 68 redirect exchanges
with 84 correlated origin requests. Spin also runs the separate 26-case independent-cap matrix
described above. They establish local SDK behavior, not hosted qualification.

The same Rust matrix also runs Cloudflare's production client through the standard application
entrypoint and local workerd, without replacing `fetch`:

```sh
./scripts/smoke_test_reusable_app.sh --adapter cloudflare --suite outbound-wire
```

Set the explicit absolute Cloudflare build-tool overrides listed above and
`MINIFLARE_WORKERD_PATH` to the local workerd executable. The runner records that executable's
identity and version and the effective compatibility settings; it uses loopback origins,
`dev --local`, synthetic local bindings and disabled metrics. Missing tools are unsupported
exit 2, never a passing inventory. No implicit download, deployment or remote binding is used.
Require all 68 exchanges and 84 ordered origin requests, exact bytes, encoding/cap parity,
unread intermediate-body progress, final stream behavior and stop/hop metadata. Truncated-body
errors must be Cloudflare `Transport` at `ResponseBodyRead`, not Spin's `Protocol` classification.
Both runners share cleanup, including pre-startup failures, and publish success only afterward.
The local workerd run is distinct from Node guest fixtures and from hosted cancellation evidence.

## Begin And Batch Lifetime Probes

```sh
./scripts/smoke_test_reusable_app.sh --adapter axum --suite outbound-lifetimes
```

The same suite accepts `fastly`, `cloudflare` or `spin` with the explicit local tooling above.
It exercises EZ-6 scenarios 1, 2, 4 and 5 through the injected production client: slow origin,
fast origin with a silent mediator, Begin followed by an unrelated 100 ms timer, and an
unobserved batch during a separate 300 ms send. Two additional probes exercise a waiting
`next()` against the original request-start-relative 200 ms cutoff, requiring `Cutoff` by
205 ms and all three slots unresolved. One origin withholds headers; the other writes a head
declaring one body byte but withholds both that byte and EOF. A seventh case exercises
CPU-stretch scenario 3: the guest consumes A before synchronous work from 520 to 560 ms,
while B is scheduled at 530 ms and C after the original 534 ms cutoff. B is either delivered
with an on-time recorded arrival or remains unresolved; a delivered elapsed time of 500 ms
or more is always rejected. The eighth case extends the slow-origin scenario with a fourth
bidder D whose per-call timeout is 300 ms and whose origin sends no response. Late checks must
return indices 0, 1 and 3 with elapsed 146, 286 and 300 ms, preserving D's typed `PerCallTimeout`
504 before `Cutoff`. C stays unresolved; the other three positions are `AlreadyDelivered`.
The original three-slot slow-origin case remains separate. D's source closes silently at
scenario 1500 ms; this bounded fixture cleanup is not evidence of SDK cancellation.
A ninth case, `caps-while-waiting`, keeps the three-slot 34/534 ms batch. A receives a 128 KiB
decoded gzip response over 300 ms, with a 1 MiB decoded cap, 64 KiB buffered cap and 256 KiB
encoded cap. B/C return one byte after 20/40 ms. Collection must preserve both healthy slots
and A's original `BufferedBody`/502 slot error before the source can complete.
A tenth case, `response-handoff` (EZ-6 criterion 20), applies only to Axum, workerd and Spin.
It starts the original streamed origin and three-slot batch under one provisional response-body
owner, confirms both are pending, and attaches that exact owner to a streamed NDJSON response.
The body collects the same handles; it does not replay requests. Before any origin head, data
or EOF is released, the native client must validate the complete canonical prelude, response
status/media type, token, pending flags, closed callback scope and live body scope. Cloudflare
and Spin also wait for a fixture-only wakeable signal set after EdgeZero dispatch returns;
Axum uses its native handoff and closed callback scope. This names the dispatch boundary, not
literal exported callback return or confirmed hosted client delivery.

The prelude is at most 512 bytes including newline; the final report is at most 16 KiB including
newline, with exactly two records. The existing bounded HTTP reader observes the prefix across
fixed, chunked and close-delimited responses, before awaiting a larger chunk or gated tail.
Ambiguous lengths, CL+TE and unsupported/duplicate transfer codings fail before notification.
Only a validated prelude opens the shared source latch. Four original roles return O/A/B/C;
the source records 22 events, plus the runner's complete-response boundary, within the existing
32-event/eight-active/128-cumulative limits. Successful recorded arrivals must share a batch
epoch after the pending check and strictly before the original cutoff. Later collection does
not invalidate on-time arrivals. Unit tests cover both native and signalled handoff, existing
resource retention, single consumption and exactly-once terminal family release.

Fastly records this criterion as inapplicable and still runs all original nine cases. The suite
does not yet implement the remaining cleanup and reusable-sandbox acceptance scenarios.

Fastly adds three cleanup cases for the local isolation part of EZ-6 criterion 21:
`response-cleanup`, `response-cleanup-source-error`, and `response-cleanup-write-deadline`.
All use the existing cleanup route and helper; a required private `x-fixture-cleanup-mode`
header selects exactly `complete`, `source-error`, or `write-deadline` without a compatibility
default. Fastly runs twelve timeline/response-cleanup cases plus the eight callback cases below;
the other adapters retain ten including response handoff.
One real client starts a callback-owned begun request before a response-owned Begin and original
three-slot batch. Public handles remain outside the streamed response while canonical egress
completes. A sixth, bounded same-client `ready` Send returns only after the source has observed
all five original requests waiting; it is a causal setup barrier, not an application workaround.
The native client validates the canonical JSON prefix and response head. Failure cases acknowledge
only a separate prefix gate, leaving all original work pending. A bounded same-client `ack` Send
then lets the body produce an intentional source error, or genuinely poll the retained original
batch to Pending under a one-second egress-start-relative write deadline. The original batch
cutoff and per-request timeouts remain three seconds without renewal. The guest requires the
matching typed terminal error/outcome, exact accepted prefix bytes, one report, no framing/fallback
failure, and the intentional source-error or actual pending-poll witness. The deadline case must
finish before the original batch cutoff. ACK failures cannot satisfy the source-error witness.

Normal delivery must complete; failure cases require premature EOF or recognized peer closure
after the exact prefix. Reader timeouts, malformed framing, capture overflow, rejected observations,
missing prefixes and ordinary successful HTTP completion cannot stand in for failed delivery.
Only validated wire termination plus the canonical `cleaned` terminal record opens the original
source gate. The body scope is closed, its retained handles return typed scope errors, and the
callback sibling remains pending. That sibling subsequently returns K, the same client sends
fresh post-delivery work returning P, and callback return closes its scope.

The private fixture protocol uses three ordered, newline-terminated records: `cleaned`,
`continued`, `returned`. Records are at most 2048 bytes, and the native log reader is capped at
1 MiB across all cases, not per case. One cursor retains partial lines and rejects observed log
shrinkage, including truncation above its consumed offset. A bounded final-tail check after runtime
shutdown catches late duplicate/foreign records, fragments and overflow from the last case.
Wrong/reordered records cannot open the source gate. Seven normal-case roles produce 25 events;
the failure cases add the eighth ACK role and its separate acknowledgment for 29 events. Each has
one additional native response/terminal-observation boundary, below the unchanged 32-event,
eight-active and 128-cumulative connection limits. Native
write-start timestamps prove the successful callback and post-delivery replies follow release.
The four body-owned source sockets close by explicit fixture release; this is not observed host
cancellation or a count of SDK resource drops.

A companion actual-route unit keeps four gated resource owners and the response attempt/body
alive across injected terminal outcomes, proving immediate release, receiver cancellation,
frozen producer polls and exactly one report before later wrapper drops. Attempt drop has its
own `TransportError` row. These are core terminal-injection and mock-owner proofs, not evidence
that an adapter detects disconnect or explicit cancellation, or that SDK/host teardown is certified.
The local Fastly source/deadline cases add adapter evidence, not hosted qualification. Full
criterion 21, criterion 23 and hosted qualification remain
open. No production API, executor, transport or runtime pin changes.

The same `outbound-lifetimes` command also exercises Fastly callback exit through both existing
`custom-single-request` and `custom-rebuild-per-request` entrypoints. Internal build selectors
choose those binaries, not another transport or entrypoint implementation. Each SDK serving loop
is limited to one request: this does not adopt or prove sandbox reuse. One shared guest probe
tests normal post-delivery return, post-delivery error, early error and setup failure in each
wrapper, retaining the original unpolled Send, begun origin and three-slot batch outside the
callback. Four pending source receipts precede readiness. After-delivery cases additionally start
and retain post-delivery work on the same client, then confirm its receipt before callback exit.
The fixture propagates the original callback/setup error from `main` after verifying cleanup;
it must not consume an expected error and accidentally return success.

Before consuming any retained handle, require closed callback scope, one request-data release,
zero unpolled-source polls and release of every registered fixture-wrapped native timer. Original
Begin/Send and optional post-delivery handles return typed Internal; the original batch keeps
three unresolved slots. Counters remain unchanged after those handles are consumed. These are
request-data/timer witnesses, not counts of SDK handle drops or proof of frozen SDK polling.
The probe uses three canonical records (`pending`, `returning`, `closed`), the same shared bounded
protocol reader, per-runtime total 1 MiB cursors, and post-shutdown tail checks. Original work is
released only after complete wire evidence plus all three records validate. Early/after-delivery
cases have 16/22 source events plus one native boundary, below the existing limits.

After delivery, require the unchanged complete canonical response. Before delivery, a typed
peer close establishes absence of a response head, but an HTTP 500 alone cannot establish its
origin. Such responses are labelled `UnverifiedHttp500` with
`no_replacement_response_proven: false`, not accepted as proof that the wrapper sent no replacement.
Reader timeouts and malformed replies fail. All eight cases are local guest-cleanup
characterization; full criterion 23 remains planned for its missing SDK/poll/provenance evidence.
Parent diagnostics preserve simultaneous timeline, response-log and callback failures rather
than hiding the later failures behind an earlier timing miss.

Adapter-local terminal matrices complement these runtime cases. Axum retains real pending
origin/batch TCP exchanges through EOF, source failure, connection-deadline signalling and
transport failure. Its unit driver waits on the actual registered connection deadline, not a
safety timeout, and checks the terminal report before body/handle drops. Cloudflare's SDK/JS
matrix consumes a prefix before EOF, source failure, an actual one-second write deadline,
request AbortSignal or readable cancellation when a further write is eligible. It observes three
fetch-abort notifications and typed scope failures before consuming the retained handles, with
stable source/report counts after later drops. This does not prove immediate disconnect
detection while a source read remains pending or hosted transport teardown.

Spin extends its existing mock-committer matrix with an original pending begun request and
three-slot batch, in addition to the retained unpolled Send. Four mock resource guards stay live
through HEAD/EOF until SDK-terminal simulation, or close immediately when fallback is selected.
Completion observes all four releases and both cancellation notifications before public-handle
consumption. Later consumption cannot poll the driver or duplicate releases. Source/deadline/
write/finish failures and coordinator cancellation at each await use the same helper; these are
core revocation/adapter coordinator units, not real Spin SDK-drop or scheduling evidence.
Full criterion 21/23 and exact-candidate qualification remain open.

The caps source uses the pinned native harness encoder, not a new guest dependency. Eight
flushed 16 KiB plaintext pieces produce one gzip member, with a logical encoded size at most
129 KiB. Actual payload remains in the final scheduled write. Counted writes retain partial
progress; only recognized peer-close errors after a prefix decoding beyond 64 KiB may be
classified as expected. Source cadence must meet its unchanged 5 ms ceiling, without early
writes. Unknown errors, insufficient prefixes and inconsistent counts fail. Existing worker,
connection and 32-event limits remain unchanged; the caps case records at most twenty events.
These fixture bounds are not a guest/provider allocation or RSS certificate.

The source's exact stored host anchor also timestamps the complete guest-response return.
The validator requires an observed over-cap prefix and both healthy payloads before that
return. The cap deadline applies to the original slot's terminal observation, not delivery
of the diagnostic HTTP response. Each original request stamp and host receipt bounds the
unknown host-minus-guest offset from above. Original slot elapsed values must permit one common
epoch inside the measured batch invocation, with no arrival after collection. Payload write
starts and native response return constrain that same host epoch for all three slots.
The worst-case cap terminal upper bound must be strictly before the original 300 ms ending;
equality, overlap and contradictory bounds fail. Round outward by one microsecond at invocation
and collection, with the existing additional 1 ms clock guard. Never use a midpoint, assumed
zero transit, a later source closure or a separately convenient offset per slot.

Correlation records publish the bounds and native HTTP timing with `same_rate_assumed: true`
and `clock_sync_proven: false`. Unknown clock origins and transit are allowed; arbitrary unequal
or paused clock rates are not proved by monotonicity. This is conditional local timing evidence,
not hosted qualification. A late diagnostic response stays recorded and does not redefine when
the slot failed. Socket shutdown may follow return. Each
reported item records `slot_error` before bounded report-body consumption, so the reporter's
own limit cannot masquerade as a production slot failure.

Each case records actual guest operation entry/duration, per-slot arrival elapsed, exact
collection states, typed errors, and independently observed host receipts/head/chunk writes.
Batched operation endpoints are captured before report formatting; microsecond offsets retain
the measured endpoint rather than separately truncating the duration. Pending probes capture
the host's complete guest-response return before explicitly releasing origins. Every receipt,
head write and waiting phase must precede that boundary, and closure must follow release.
Equal timestamps do not override the required event-index ordering.

The pending-body record explicitly reports `body_phase_evidence: host-head-written-only`.
Writing the host's head does not prove the SDK consumed it or entered a body read before the
cutoff. These probes do not close the complete scenario 16 acceptance leg or certify host
cancellation. Release ends the fixture's hold; it is not evidence of provider teardown.
Report-body reads are capped at 64 bytes; the caps slot has the separate limits above.
The existing bounded origin collector/parser and joined worker
ownership are reused; registrations, roles and event counts are finite, and unexpected or late
worker failures prevent a passing inventory. The suite runs every applicable case even when an
earlier case fails, but publishes success only after runtime and origin cleanup succeed.
Drain errors are recorded separately from primary client failures, including interrupted prefix
captures. Shared teardown records client completions and runtime logs even on failure.

Host request-send and guest request-start have different origins, particularly on a cold guest.
The slow/fast timeline cases, including the fourth-slot variant, record a scheduling alignment derived from the first
origin receipt and its guest send timestamp. Transit uncertainty remains unknown and
`clock_sync_proven` remains false. Scheduled timestamps never replace actual writes or guest
observations. The frozen +/-5 ms timeline targets remain checked; misses return exit 1, not a
qualified pass. The fourth-slot case also records `timeout_contract` with its own status/error,
even when an earlier source-timing check fails. That diagnostic subresult cannot pass the whole
case, close its acceptance leg or prove prompt native timer wakeup.
For quiet-window proofs, a head-write-start/guest-head-observation causal bound
conservatively compares receipts without pretending the clocks are synchronized. The 5 ms
measurement guard makes that proof stricter, and is not an extra deadline allowance.

The CPU case makes exactly one bounded calibration Send through the same injected client after
consuming the origin. It carries the actual origin-head observation and request-send samples,
uses a 64-byte buffered cap, a 1-second timeout and an absolute 520 ms safety deadline, and
expects a bodyless `204`. The source publishes calibration before waiting to send that reply;
A/B/C cannot fall back to the initial cold scheduling anchor. The reply is held to `450 ms + U`,
where `U` is the causal offset upper bound, leaving the existing timer to reach the unchanged
500 ms observation target. A's whole receipt interval must lie inside this EdgeZero Send.
Five fixed roles produce at most 19 events within the existing 32-event bound and owned worker
limits. Missing or contradictory calibration fails; no extra transport or production hook is used.

The CPU case records a causal interval for `host - guest` from the original and calibration
request-send/receipt boundaries and the actual origin-head write/observation. Calibration receipt
cannot precede the origin payload's pre-write sample; a post-flush timestamp cannot safely prove
that ordering. The midpoint is only a source scheduling estimate for A180/B530/C900, not proof.
Its 1 ms clock-rounding guard expands uncertainty, not the
accepted timing targets. The complete actual B write must fit the busy interval and 530 +/-5 ms
for every offset in that interval; choosing a convenient point estimate is not proof. Receipt
arrival still must be strictly before the original 534 ms cutoff. The report explicitly records
the equal-rate assumption and does not claim synchronized clocks. Receipt reconstruction brackets
core's batch epoch by the complete measured invocation, not its pre-call timestamp; every
possible arrival must satisfy the cutoff and delivery boundaries. A cold-connection interval
that is too wide fails the probe, even when B is correctly unresolved.
Same-client calibration does not promise connection reuse or a sufficiently narrow interval.
The shared test origin uses harness-only Mio 1.2.1 readiness rather than a 5 ms accept sleep.
Both listeners and their single [wakers](https://docs.rs/mio/1.2.1/mio/struct.Waker.html) are
registered before either collector starts. Collectors drain accepts through `WouldBlock` and
recheck sticky stop under the accounting mutex before admitting work. Stop releases gates and
wakes both collectors outside that mutex; each collector retains its wake owner until joined.
A 50 ms poll timeout backs up failed/missed stop wakes, not normal socket readiness. It is not
a realtime or whole-shutdown guarantee: workers retain their existing 5-second I/O and 6-second
gate budgets. The 128 cumulative/eight-active connection limits remain global to both listeners,
and shutdown wakes create no TCP requests. These are test-origin bounds, not deployed-app claims.
Source timing and hosted qualification remain open when actual write/receipt bounds do not fit.
The validators also require every head and chunk write to start at or after its original schedule,
finish after its own start, and begin payload only after head completion. Receipt lower bounds
use actual payload production, not head production. `baseline-batch` is eager on all four adapters;
only `lazy-single` is the lazy negative control. These checks retain the original +5 ms boundary.

CPU work is synchronous and bounded by ten million iterations, with rejection on backwards,
frozen or already-expired windows. The iteration cap is a finite work bound, not a wall-clock
guarantee. This is a local runtime probe only: [deployed Workers clocks advance after I/O](https://developers.cloudflare.com/workers/runtime-apis/performance/),
so they must not be used to drive an unbounded CPU loop. No hosted CPU timing is certified.

These are local characterization/acceptance probes, not a hosted evidence package. Until the
complete declared criteria and timing evidence pass, the inventory bindings stay planned.
No candidate freeze, provider capability promotion, deployment, or optional idle-body adoption
is implied. Run artifacts preserve failures as well as successes.

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

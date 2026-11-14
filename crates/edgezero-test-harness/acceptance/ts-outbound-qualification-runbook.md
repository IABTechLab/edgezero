# Trusted Server Outbound Qualification Runbook Template

**Source template only: unaccepted, unqualified and not execution authorization.**
Fill an external copy after Q3 freezes the candidate. Do not fill candidate identities by
editing this source after freeze. This document changes no outbound contract or capability.

## Authorities and Entry Gates

**Qualification amendment, 2026-11-08:** AG approved adapter/deployment-specific HTTP/3
qualification and strict bodiless suppression instead of Fastly bodiless SDK parity. Keep all
511 existing leg identities and three appended HTTP/3 legs, for 255 criteria / 514 legs. These
are coverage identities, not passing executions. The nine historical SDK-reference failures
remain unchanged; all twelve current bodiless SDK controls are reference observations. Six
content-bearing comparisons retain strict parity. Hosted suppression/completion, compression
and consumer content-bearing parity remain open.

**Dispatch-slack retirement, 2026-10-27:** the requester authorized removing the fixed 25 ms
dispatch-slack guard and its test injection helper. This supersedes older statements below that
slack remains unchanged. Original absolute deadlines, timeout causes, late-result rejection, B31
backend/timer policy and resource limits remain unchanged. Synchronous SDK host calls are still
non-preemptible; no finite blocking horizon, Native capability or hosted qualification follows.
Keep historical hashes, measurements and failed probes intact. EZ-3 legs remain planned until
real executable commands and sentinels are bound; this authorization is not acceptance evidence.

**Current scoped override, 2026-10-24:** the requester's [timing and native integration approval](../../../docs/audits/2026-10-18-outbound-api-approval.md#scoped-timing-and-native-integration-amendment)
permits a Send-first implementation checkpoint, not qualification/deployment. It supersedes the
implementation-blocking interpretation of the earlier G2/H(K) statements below: +5 ms stays a
measured qualification target with unchanged tests; the synchronous optimizer blocking horizon
is explicitly **UNKNOWN** until provider evidence. Original deadlines/sources and late-result
rejection remain. No old failure becomes a pass, and no numerical horizon is approved here.

The shared head-first pipeline, explicit Follow, B31 timers, original deadlines and resource
constraints are implemented. The preceding approvals describe historical sequencing, not a
second implementation or a compatibility path. Typed body errors and fresh-readiness/read safety
remain required. The fixed dispatch-slack guard has been retired under the approval above.
The external numerical-policy/H(K), hosted evidence and performance-comparison gates below
still block qualification; an unknown horizon cannot satisfy their finite-bound assertions.

- [Requirements](../../../docs/superpowers/specs/2026-10-17-edgezero-pr275-requirements.md)
  define acceptance; [G1 inventory](outbound-cutover-cases.json) maps criteria to environments,
  sentinels, owners and applicability decisions. Its 255 criteria / 514 legs are a
  coverage domain, **not 514 passing tests**. Use `acceptance --coverage` for current readiness.
- [G2 feasibility](../../../docs/audits/2026-10-18-outbound-feasibility.md) remains failed.
  Stock pinned Viceroy timed readiness loses body errors. No blocking-read fallback,
  compatibility path or pin change is authorized by this runbook.
  A separately identified [local runtime repair](../../../patches/viceroy/README.md) fixes the
  measured error loss in a patched executable. It does not qualify stock 0.21.0, the full G2
  scenario set or hosted Fastly; remaining exact-version/policy review stays open.
  The [expanded local matrix](../../../docs/audits/evidence/2026-10-23-fastly-readiness-matrix.json)
  executes all eleven diagnostic scenarios but retains a repaired-runtime result beyond +5 ms.
  Error retention is repaired locally; timing acceptance and qualification remain failed/open.
- [G4 approval record](../../../docs/audits/2026-10-18-outbound-api-approval.md) records AG's
  API-only approval of Interfaces SHA-256
  `c623e7fc5b189e2d9122756f29297a92031a9936b13c6cc475484d4e028a4a7a`.
  Fastly primitive/policy including literal B, numerical H(K), applicability and measurement
  approvals remain separate pending decisions. Historical requirements/inventory hashes in
  that record are not refreshed approvals of later source changes.
- [Qualification plan](../../../docs/superpowers/plans/2026-10-18-trusted-server-outbound-05-qualification.md)
  owns Q2 source preparation, Q3 freeze/local acceptance, Q4a package acceptance, Q4b execution
  and Q5 publication. No candidate has been frozen or package accepted by this template.
- The single-request Fastly qualification build and frozen-candidate exporter are implemented
  below. Actual frozen/hosted execution and qualification remain open. Historical SDK bodyless
  failures are not relabeled; their current obligation is strict suppression under the approved
  amendment. Cloudflare front-proxy overflow-recovery failures are not waived.

Do not start qualification until all prerequisite implementation, feasibility, policy and
source-producing work is complete. Deployment additionally requires the actual execution
owner's acceptance, explicit staging authorization and required entitlements below.

## External Candidate and Acceptance Sheet

Every `UNSET` blocks the dependent step; an operator must supply and review facts, not infer
them from a successful command. Preserve separate source, build-input, binary and deployment
identities. Record unavailable provider revisions as typed, reasoned unknowns.

| Field                          | Required External Record                                                                                                                                 |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Q3 freeze and local acceptance | UNSET: freeze reference, clean 40-character candidate SHA, authoritative per-leg local evidence                                                          |
| Baseline and prerequisite      | UNSET: full immutable identities resolving TS baseline `a9037770a` and prerequisite PR 1123 `4c5ca8ae5`                                                  |
| Reviewed source subjects       | UNSET: requirements/inventory digests, approved Interfaces digest, reviewed changes since historical approval packet                                     |
| G4 decisions                   | UNSET: separate API, primitive/B, H(K), applicability and measurement decision IDs, scope, exact subject digest, approver, date and evidence reference   |
| Build identities               | UNSET: lockfiles/build-input SHA-256, commands/features/target, runner image, binary and packaged/deployed artifact SHA-256                              |
| Runtime identities             | UNSET: actual SDK/CLI/runtime revisions, compatibility date/flags, deployment/service identity and POP where observable                                  |
| Execution ownership            | UNSET: named EdgeZero preparer and actual TS maintainer/operator, dated ownership acknowledgement, reachable runbook/work-item link                      |
| Package acceptance             | UNSET: TS acceptance date, accepted external runbook/package digest and candidate/build identities                                                       |
| Staging authorization          | UNSET: issuer, date, evidence reference, isolated service and permitted actions; no production traffic/stores                                            |
| Entitlements and origins       | UNSET: required feature entitlements, controlled origins, approved delays/failure modes and non-secret settings references                               |
| Execution mapping              | UNSET: each G1 leg, actual command/cwd, unique sentinel, expected assertions/case counts, samples/thresholds where required, artifact and owner          |
| Measurement approvals          | UNSET: M1/M2/M5/M7 comparison definitions, thresholds, sampling/randomization, 14b control and H(K) numerical rule/domain/maximum/tolerance              |
| Image performance comparison   | UNSET: matched TS/SDK-only controls, asset/profile corpus, cache classification, GET/HEAD/S3 groups, instrumentation, samples and acceptance/noise rules |
| Handoff and retention          | UNSET: sanitized artifact manifest, independent review, durable non-expiring retention and consumer confirmation                                         |

Use the frozen values from the external record; never substitute the current `HEAD`:

```sh
set -eu
: "${CANDIDATE_SHA:?Q3 external freeze record required}"
: "${BUILD_INPUT_SHA256:?reviewed build-input identity required}"
actual_sha=$(git rev-parse --verify HEAD)
test "$actual_sha" = "$CANDIDATE_SHA"
source_status=$(git status --porcelain --untracked-files=all)
test -z "$source_status"
```

Run from the candidate checkout root. Record the full command/environment and verify exact
SHA/digest syntax and source-to-build provenance independently. A source change after freeze
requires a new freeze and **all required local and hosted legs rerun**, not affected-only tests.
Freeze, package acceptance and release/push authorization are different decisions.

## Implemented Preparation Commands

These commands are runnable now from the repository root; their results are development checks,
not exact-candidate acceptance. Install the worktree's `.tool-versions` and fetch the required
locked Cargo workspaces as described in Q2 before offline checks.

```sh
cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml evidence::cutover
scripts/test_integration.sh acceptance --manifest
scripts/test_integration.sh check
cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml checks::forwarding::
scripts/run_test_nonzero.sh outbound_evidence_assembly_retains_case_counts_and_hashes cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml outbound_evidence_assembly_retains_case_counts_and_hashes
```

Expected: nonzero Rust test listings with zero failures, exactly one passing non-ignored
forwarding doctest, exactly one passing named Rust sentinel, and inventory totals of
255 criteria / 514 legs / `qualified:false`. Record actual test counts for the frozen
candidate. A manifest pass deliberately permits pending decisions and planned commands.

Also run Q2's five primary Rust gates, excluded demo/generated/fixture checks, seven adapter
feature matrices and actual target sentinels. Those exact commands remain in the qualification
plan; compilation, routing sentinels and local smoke are not hosted outbound acceptance.

Worktree tool pins are Rust 1.98.1, Node 24.20.0, Fastly CLI 16.0.0, Spin 4.1.0,
standalone Viceroy 0.21.0 and Wasmtime 48.0.2. SDK requirements are Fastly 0.13.1,
worker 0.8.5 and registry spin-sdk 8.0.0 (`wasip3 0.10.0` / `wit-bindgen 0.63.0`).
Record actual executable identities rather than borrowing
historical paths. Local fixture runners accept `VICEROY_BIN`, `SPIN_BIN`, `WRANGLER_BIN` and
`WORKER_BUILD_BIN`; Cloudflare also requires explicit absolute `WASM_BINDGEN_BIN`,
`ESBUILD_BIN` and `WASM_OPT_BIN`. Preserve `WRANGLER_SEND_METRICS=false` and a scoped
`WRANGLER_LOG_PATH`. See the [suite reference](../SUITES.md) for runtime preflight and limitations.

## Qualification Builds

Fastly selects the single-request `run_custom` qualification package, including the existing
wire/lifetime controllers and the hosted profiles below. Other targets select their existing
response-egress fixture, not this Fastly outbound package. These commands may be checked locally before freeze using
explicit characterization identities; those builds cannot be relabelled frozen evidence.
For accepted execution use only the Q3 sheet's clean candidate and reviewed build inputs.

```sh
export EDGEZERO_QUALIFICATION_REVISION="$CANDIDATE_SHA"
export EDGEZERO_QUALIFICATION_ARTIFACT="$BUILD_INPUT_SHA256"
export EDGEZERO_QUALIFICATION_ORIGINS='["https://controlled-origin.example","http://controlled-drop.example:8080"]'
cargo build --offline --locked --release --manifest-path crates/edgezero-test-harness/fixtures/reusable-app/Cargo.toml -p fixture-cloudflare --features qualification --target wasm32-unknown-unknown
cargo build --offline --locked --release --manifest-path crates/edgezero-test-harness/fixtures/reusable-app/Cargo.toml -p fixture-fastly --features qualification --bin fixture-fastly-qualification --target wasm32-wasip1
cargo build --offline --locked --release --manifest-path crates/edgezero-test-harness/fixtures/reusable-app/Cargo.toml -p fixture-spin --features qualification --target wasm32-wasip2
cargo build --offline --locked --release --manifest-path crates/edgezero-test-harness/fixtures/reusable-app/Cargo.toml -p fixture-axum --features qualification
```

`EDGEZERO_QUALIFICATION_ARTIFACT` is a lockfile/build-input digest, **not a binary hash**.
After building, record actual binary/package SHA-256 separately. Cloudflare's Cargo output
alone is not its deployable worker package; record the generated shim, WASM and configuration
identities. TS must confirm the actual isolated-service packaging/deployment command and
settings. This template supplies no deployment authorization or invented deployment recipe.

Only after accepted target configuration and explicit staging authorization:

```sh
node scripts/qualify_response_egress.mjs --execute-hosted TARGET.json CLIENTS.json
scripts/test_integration.sh response-egress --validate CLIENTS.json EVENTS.json
```

Use the README's strict target JSON shape with actual allowlisted HTTPS origin, SDK/runtime,
compatibility, candidate, artifact and deployment facts. Thirteen scheduled scenarios plus
thirteen healthy recovery requests produce **26 client observations**; a complete fixture run
requires four lifecycle events per request, **104 events**. These are not the 514 G1 legs or
measurement sample counts. No outbound backend is contacted by the `/qualification/*` routes.

Collection intentionally exits **2**, not a qualification pass. Export complete unsampled
events independently; only an independently confirmed complete export permits setting
`telemetry_complete`. Do not discard failures or duplicate/conflicting events. Validation can
qualify ownership for the scheduled probes only: `HostHandoff` is not client delivery, instance
identity is unobserved, and recovery does not prove same-isolate capacity recovery. Missing
transport/parser/allocation evidence remains separate. Fastly requires an independently enforced
host execution ceiling of 1..60000 ms; client budget is 1000..10000 ms and the fixture server
response budget is 500 ms. Before/after checks cannot interrupt a blocking host call.

## Fastly Outbound Qualification Package

G1 strings containing `outbound-cutover`, `--case`, `hosted-runbook` or `consumer-runbook`
are planned recipes, not commands to copy into a terminal. Q3/Q4a must replace them with
owner-confirmed runnable invocations before a leg is ready. The Fastly package below supplies
the outbound implementation; it does not invent those external bindings. Frozen-record export requires
an independently frozen candidate, reviewed build attestations and new frozen-run captures.

### Build And Origin Safety

Replace the sample origin list above with independently controlled origins before building.
`EDGEZERO_QUALIFICATION_ORIGINS` is an embedded JSON array of at most 16 exact HTTP(S)
scheme/authority pairs (16 KiB total, 2048 bytes per entry). Credentials, wildcard hosts,
paths, queries, fragments, zero/invalid ports and duplicates are rejected. An absent list
permits only HTTP `localhost`/`127.0.0.1` with explicit positive ports. No request can extend
the list. The fixture checks initial and prepared redirect targets; HTTPS retains certificate
verification and URI-host SNI. Keep this diagnostic build isolated, not on production traffic.

`qualification` includes the existing `progress` feature. Both Fastly binaries share one
dispatcher; the qualification binary uses `run_custom` for one request, not reusable
`serve_app`. The existing wire and lifetime harness suites build and execute this actual binary:

```sh
scripts/test_integration.sh --adapter fastly --suite outbound-wire --output NEW_WIRE_DIRECTORY
scripts/test_integration.sh --adapter fastly --suite outbound-lifetimes --output NEW_LIFETIME_DIRECTORY
```

Local runs embed the actual source revision and harness build-input identity; they are labelled
working-tree characterization, not clean frozen-candidate evidence. The wire suite additionally
checks eight hosted-profile routing/settings controls and zero-I/O invalid/duplicate inputs.
It does not pretend the local no-store origin proves hosted cache hits, TLS pooling or an 80 s delay.

### Hosted Profiles

After independent packaging and staging authorization, invoke
`/hosted/candidate/PROFILE` with `x-request-token` (1..64 ASCII alphanumeric/hyphen) and
`x-qualification-target` (allowlisted URI, at most 2048 bytes). Image profiles additionally
require `x-qualification-region` (the typed SDK region token). GET and HEAD are accepted;
duplicates, invalid controls and unknown profiles return a fixed 400 without dispatch or input echo.
Only the explicit token/target/region/Accept controls are consumed, not arbitrary inbound headers.

| Profile                                       | Actual Request Policy                                                               | Independently Controlled Origin / Required Observation                                                                                        |
| --------------------------------------------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `head-timeout`                                | 100 ms timeout                                                                      | Delay head 500 ms; preserve the 100..105 ms measured target and any failure.                                                                  |
| `body-buffered`, `body-streamed`              | 500 ms total timeout, corresponding response mode                                   | Immediate head, one byte per 300 ms; retain actual terminal time and typed cause.                                                             |
| `long-budget`                                 | 90 s configured timeout                                                             | Head at 80 s; local immediate-origin routing is not this proof.                                                                               |
| `connect`                                     | 200 ms connect, 2 s total                                                           | Enforced silent-drop endpoint with independent capture, not a documentation IP.                                                               |
| `connect-default`                             | No connect override, 2 s total                                                      | Same silent-drop endpoint; distinguish aggregate and phase observations.                                                                      |
| `refused`                                     | 200 ms connect, 2 s total                                                           | Separate reachable refused port; require typed `Unreachable`.                                                                                 |
| `cache-send`, `cache-batch`                   | Two sequential Send requests or two real one-slot buffered batches; 1 s per request | Identical URL/headers within a pair; distinct cold keys between profiles. Require one origin receipt per pair plus actual cache-hit evidence. |
| `reuse`                                       | One request per execution, cache bypass, 1 s                                        | Persistent HTTPS origin with independently recorded TCP/TLS and connection IDs; repeat fresh executions for both roles.                       |
| `image`                                       | Typed native width 100 / WebP / region, streamed response, 1 s                      | Actual optimized GET and HEAD, source-copy/host-call counts and matched native control. Also exercise timely head with pending body.          |
| `image-query-on`, `image-query-off`           | Same typed options; preserve-query true / false                                     | Target must have no query; fixture adds only `width=100&unrelated=qualification`. Verify the origin URI independently.                        |
| `image-short-deadline`                        | 1 s configured timeout and original 250 ms absolute deadline                        | Silent/delayed head and pending body; actual host-call entry is required, not merely pre-dispatch expiry.                                     |
| `image-deadline-1ms`, `image-deadline-1025ms` | Deadline-only, no configured timeout; original 1 / 1025 ms deadline                 | Exercise ladder boundaries with actual host entry where feasible; report preflight expiry honestly.                                           |

The candidate returns bounded JSON for ordinary probes: effective settings, input/build identity,
preparation/original deadline and cause, head/terminal times, typed outcome, body length/digest
and registration counts. Selected header evidence is at most 16 fields; values over 256 bytes
or invalid UTF-8 are unknown (`null`), not rewritten authoritative headers. Images are streamed
directly with `x-qualification-report` plus terminal lifecycle telemetry; no extra fetch, image
hash/collection or serialized optimizer options are introduced. A successful HEAD has no payload.
The test-only native witness measures the actual synchronous optimizer SDK call, before/after
that call, using its prepared clock; an unavailable timestamp is unknown, never fabricated.

`/hosted/native/reuse` and `/hosted/native/IMAGE_PROFILE` are independent, direct SDK-only
controls with matched authority, strict TLS/SNI, phase/connect/cache policy, negotiation and
typed image options. They send exactly once; no EdgeZero outbound transport is used. Their SDK
call timings are not a promise of portable logical-deadline enforcement. For 14b run one request
per fresh execution for each role with the same independently controlled persistent TLS origin;
the fixture cannot manufacture provider connection identity or certify pooling from registrations.
For cache 14a use the candidate pairs above, not this non-caching local SDK control.

The same package retains `/progress/forward/{send,late,outbound-deadline,inbound-deadline}`
for EZ-1: `x-origin` is the allowlisted `/forward/send/TOKEN` target. The last two set a
200 ms outbound timeout; `inbound-deadline` also admits a 100 ms body-read budget, while
`late` admits an already expired read deadline. Use real absent/CL:0/chunked-empty bodies,
never-ready chunked GETs and actual HTTP/1/2 clients;
the report retains observed duration without upgrading Fastly's non-preemptible read semantics.
All original forwarding checks remain in the wire runner.

### Per-Adapter HTTP/3 Receive Qualification

EZ-1 acceptance 02 keeps the original Fastly staging HTTP/1.1 and HTTP/2 legs. Its four
HTTP/3 slots are `hosted-{axum,cloudflare,fastly,spin}/http3`, each bound to one explicitly
selected deployment/runtime/configuration, not to a consumer's staging setup. All four
pending dispositions are required. The direct Axum listener is HTTP/1-only; selecting a
reverse proxy means naming and qualifying that separate receive boundary. Local workerd,
Viceroy and Spin HTTP/1 fixtures do not prove hosted HTTP/3 support or lack of support.

For a supported selected deployment, record a real client's negotiated HTTP/3 version,
adapter/SDK/runtime/build/service/configuration identities, the existing bodiless GET/HEAD
and chunked-empty-equivalent assertions, token-correlated origin receipts and original
read/outbound deadline observations. For example, an HTTP/3-enabled curl can record the
actual version without silently falling back:

```sh
curl --http3-only --silent --show-error --max-time 5 --dump-header get.headers --output get.body --write-out '%{http_version}\n' "$QUALIFICATION_URL/progress/forward/send"
curl --http3-only --silent --show-error --max-time 5 --head --dump-header head.headers --output head.headers-only --write-out '%{http_version}\n' "$QUALIFICATION_URL/progress/forward/late"
```

These are client invocation examples, not complete passing acceptance commands: supply the
existing allowlisted origin/token inputs and deadline scenarios, verify the recorded version
is HTTP/3, and retain server-side receive/host reports where available. A HEAD `--head` capture
alone does not prove zero DATA bytes. Use a protocol-aware full-stream capture for that proof.
The Node HTTPS egress collector, a proxy's upstream HTTP version and the guest's normalized
version are not HTTP/3 receive proof.

For proved unsupported deployments, retain the exact boundary/configuration, reason and
evidence in a separately attributed `requirement-change` decision with a scope containing
only that leg. Its `approved-deferred` mapping records N/A, not a passed execution. The
requirements amendment does not approve these four decisions in advance. Unknown support,
a disabled service option, inaccessible deployment or unavailable HTTP/3 client is unresolved,
not unsupported; ordinary `defer` cannot waive these blocking dispositions. Qualifying a
different deployment requires fresh applicability and execution evidence.

EZ-6 uses the same existing `/progress/lifetimes/{slow-origin,fast-origin,body-read-arrivals,trickling-slot,send-during-batch,batch-write-deadline}`
controllers and independent `/lifetimes/TOKEN/CASE` origins as the lifetime suite. Admission
observations are enabled by valid qualification build identity, not a different suite-only
compile flag. Preserve their original schedules, slot sets, receipts and assertions; the package
does not substitute a simpler implementation. Use separate old/new builds and retain build /
registration identities for EZ-2.13b. S3 preflight remains application owned.

No profile supplies an approved finite H(K), product threshold, provider entitlement, frozen
record or hosted result. Preserve all negative observations and the UNKNOWN blocking horizon.

For every applicable leg, the accepted external package must bind: case/environment/source
digest, unique sentinel, command/cwd, owner, candidate/build identity, expected assertions and
case count, pre-approved sample count/threshold where required, raw artifact and export path. Include explicit
approved-deferred mappings without counting them as passed executions. Do not substitute a
positive sample number for a complete expected-case count or invent absent TS commands.

Required hosted coverage includes EZ-Q1.4's EZ-1 body/deadline cases, EZ-2 criteria
11/12/13b/14a/14b, EZ-4 criterion 8, EZ-6 scenarios 1/2/6/7/8/11 and Image Optimizer.
TS confirms canonical `run_custom` plus `send_request_with_registries_and_hooks` composition,
single-request mode by default, and its deployment/browser/response-diff/export commands.
Its known integration command, **from the TS checkout**, is:

```sh
cargo test --manifest-path crates/trusted-server-integration-tests/Cargo.toml --test integration -- --include-ignored
```

Preserve these distinct measurement obligations from the requirements and Q4b:

- Confirm M1/M2/M5/M7 product comparisons, samples, settings and thresholds before replay.
  M3's registration-count bound is a fixed pass/fail invariant; its timing distribution is
  report-only, not subject to a product threshold. M6 keeps criteria 4/5's fixed deadline
  windows. Preserve ordinary deadline plus 5 ms acceptance windows and original on-time-success
  rules; these do not become negotiable product thresholds.
- Confirm EZ-8.12's native-path comparison separately: identical region/options/image bytes,
  negotiation and transport/cache settings; verified cache-hit/miss groups; GET/HEAD and
  application-owned S3 preflight groups; matched client rate/concurrency and randomized build
  order. Record latency distributions, throughput, allocation/copy/buffering and host-call counts,
  plus origin receipts/connection identities where observable. Disclose unavailable counters and
  agree how they will be evidenced before replay. Neither SDK-shaped code, Viceroy rejection nor
  an unmeasured/missing control proves performance parity. Do not reuse bidder-latency tolerance.
- For 14b, both SDK-only control and candidate need positive cross-execution origin connection
  IDs across fresh single-request executions. Registration counts, POP or TLS resumption alone
  do not prove pooling. A negative/inconclusive control blocks qualification. Separately deferred
  M4/reused-sandbox work does not waive 14b or approve reuse.
- Pre-approve literal B/domain and numerical H(K) rule/domain/maximum/tolerance with exact
  subject digests. Record original entry t0, original D/BudgetSource, preparation, host-call and
  total timings. Exercise the actual optimizer host call and timely-head/pending-body case;
  pre-dispatch expiry is not a host-call proof. Validate both original-entry and host-entry
  inequalities; never select a horizon or inflate thresholds after seeing results.

## Draft And Frozen Records

The existing tool consumes explicit observations, not generic smoke JSONL/FNV summaries:

```sh
scripts/test_integration.sh acceptance --assemble --candidate "$CANDIDATE_SHA" --input OBSERVATIONS.json --artifacts EVIDENCE_DIRECTORY --output NEW_DRAFT.json
scripts/test_integration.sh acceptance --report --candidate "$CANDIDATE_SHA" --record NEW_DRAFT.json --artifacts EVIDENCE_DIRECTORY
```

`--assemble` preserves supplied counts/outcomes and verifies linked build/raw-artifact SHA-256;
it writes a new `stage:"pre-freeze", qualified:false` draft and exits nonzero for open gates.
The draft report is blocked, even if every supplied observation passed. A draft cannot be
promoted by changing its stage. After Q3 and separately authorized capture, the build/capture
producer supplies `stage:"frozen-capture", source_state:"clean"` observations and a receipt:

```json
{
  "schema": 1,
  "freeze_ref": "UNSET: independent Q3 approval",
  "capture_ref": "UNSET: frozen execution identity",
  "candidate_sha": "UNSET: 40 lowercase hexadecimal characters",
  "candidate_tree": "UNSET: candidate Git tree",
  "source_digest": "UNSET: requirements byte SHA-256",
  "inventory_sha256": "UNSET: committed inventory byte SHA-256",
  "observations": {
    "artifact_path": "observations.json",
    "artifact_sha256": "UNSET: producer capture byte SHA-256"
  },
  "builds": [
    {
      "build_id": "UNSET: binary SHA-256",
      "source_tree": "UNSET: candidate Git tree",
      "recipe_path": "build/recipe.json",
      "recipe_sha256": "UNSET: reviewed build-input byte SHA-256"
    }
  ]
}
```

Each recipe is JSON with `schema:1`, `candidate_sha`, `source_tree`, the exact build `argv`
array and its explicit non-secret `env` object. Bind every binary, dependency/configuration input
and capture to the independent freeze sheet; do not mint a receipt to legitimize historical logs.
Each observation already supplies its binary id and raw artifact SHA-256. The receipt binds the
complete observation bytes, including failures, pending results, measurements and applicability.
Missing results remain missing. The **reviewed receipt hash must come from independent review**,
not from a record or an exporter-generated approval.

```sh
scripts/test_integration.sh acceptance --export-frozen --candidate "$CANDIDATE_SHA" --candidate-root CLEAN_CHECKOUT --provenance provenance.json --provenance-sha256 "$REVIEWED_RECEIPT_SHA256" --input EVIDENCE_DIRECTORY/observations.json --artifacts EVIDENCE_DIRECTORY --output NEW_FROZEN_RECORD.json
scripts/test_integration.sh acceptance --report --candidate "$CANDIDATE_SHA" --candidate-root CLEAN_CHECKOUT --provenance provenance.json --provenance-sha256 "$REVIEWED_RECEIPT_SHA256" --record NEW_FROZEN_RECORD.json --artifacts EVIDENCE_DIRECTORY
scripts/test_integration.sh acceptance --release --candidate "$CANDIDATE_SHA" --candidate-root CLEAN_CHECKOUT --provenance provenance.json --provenance-sha256 "$REVIEWED_RECEIPT_SHA256" --record NEW_FROZEN_RECORD.json --artifacts EVIDENCE_DIRECTORY
```

Provenance paths are relative to the evidence directory. Use a clean candidate-root checkout,
not a subdirectory, with exact `HEAD`/tree and matching tracked requirements/inventory. Staged,
unstaged or untracked changes, hidden-index flags and changed submodules are rejected. Git checks
disable fsmonitor and optional index updates. Store capture/output files outside that checkout.
The exporter validates the source/build/capture chain and writes only a new record. It performs
no build, freeze, deployment, signing, publication or approval. Report/release revalidate the same
receipt and exact exported content; a modified record cannot retain credit from that receipt.
Valid provenance with failed, missing or pending legs still writes a **blocked** record and exits
nonzero. Invalid provenance writes nothing. Neither mode clears characterization flags or marks
`qualified:true`; empty frozen captures remain blocked rather than inventing observations.

`--report` and `--release` share release rules. A successful check does **not** authenticate
telemetry, approvers or attestation producers, independently reproduce a binary, authorize
execution/publication, establish criterion-specific H(K)/pooling thresholds, or grant qualification.
Their output's `qualified:false` is not changed by success.

Use stable sanitized file snapshots: checker JSON is capped at 16 MiB; raw artifacts at 256 MiB,
hashed with 64 KiB scratch. Provenance JSON is parsed from the same bounded bytes whose hash is
verified, not from a later read. The checker rejects artifact symlink/path escapes and refuses output
overwrites. The separate hosted-egress tool caps evidence inputs at 4 MiB and also refuses existing
output files; use operator-controlled paths. Exclude credentials, signing material, raw application
payloads and diagnostic URLs. The tools do not protect against fabricated telemetry or authenticate
provider identity; the checker also requires files not be mutated concurrently. Review independently.

Q5 additionally requires every required leg and pre-approved measurement to pass, applicability
and consumer confirmation, exact unchanged candidate/build identities, and separately authorized
publication to a reachable immutable tag with durable non-expiring artifacts. Verify remote
hashes and retention after publication. Missing command, owner, authorization, entitlement,
approval or evidence leaves qualification blocked; neither a green CI run nor delivery of this
template closes Q4/Q5 or promotes BestEffort/Unsupported guarantees.

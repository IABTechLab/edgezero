# Trusted Server Outbound Qualification Runbook Template

**Source template only: unaccepted, unqualified and not execution authorization.**
Fill an external copy after Q3 freezes the candidate. Do not fill candidate identities by
editing this source after freeze. This document changes no outbound contract or capability.

## Authorities and Entry Gates

- [Requirements](../../../docs/superpowers/specs/2026-10-17-edgezero-pr275-requirements.md)
  define acceptance; [G1 inventory](outbound-cutover-cases.json) maps criteria to environments,
  sentinels, owners and applicability decisions. Its 255 criteria / 511 planned legs are a
  coverage domain, **not 511 passing tests**.
- [G2 feasibility](../../../docs/audits/2026-10-18-outbound-feasibility.md) remains failed.
  Fastly timed readiness loses body errors in pinned Viceroy. The atomic outbound migration
  remains blocked; no blocking-read fallback, compatibility path or pin change is authorized.
- [G4 approval record](../../../docs/audits/2026-10-18-outbound-api-approval.md) records AG's
  API-only approval of Interfaces SHA-256
  `c623e7fc5b189e2d9122756f29297a92031a9936b13c6cc475484d4e028a4a7a`.
  Fastly primitive/policy including literal B, numerical H(K), applicability and measurement
  approvals remain separate pending decisions. Historical requirements/inventory hashes in
  that record are not refreshed approvals of later source changes.
- [Qualification plan](../../../docs/superpowers/plans/2026-10-18-trusted-server-outbound-05-qualification.md)
  owns Q2 source preparation, Q3 freeze/local acceptance, Q4a package acceptance, Q4b execution
  and Q5 publication. No candidate has been frozen or package accepted by this template.
- Full outbound case execution and frozen-candidate export are not implemented. Existing
  response-egress probes do not replace them. Recorded Fastly SDK framing/bodyless parity
  failures and Cloudflare front-proxy overflow-recovery failures are not waived.

Do not start qualification until all prerequisite implementation, feasibility, policy and
source-producing work is complete. Deployment additionally requires the actual execution
owner's acceptance, explicit staging authorization and required entitlements below.

## External Candidate and Acceptance Sheet

Every `UNSET` blocks the dependent step; an operator must supply and review facts, not infer
them from a successful command. Preserve separate source, build-input, binary and deployment
identities. Record unavailable provider revisions as typed, reasoned unknowns.

| Field                          | Required External Record                                                                                                                               |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Q3 freeze and local acceptance | UNSET: freeze reference, clean 40-character candidate SHA, authoritative per-leg local evidence                                                        |
| Baseline and prerequisite      | UNSET: full immutable identities resolving TS baseline `a9037770a` and prerequisite PR 1123 `4c5ca8ae5`                                                |
| Reviewed source subjects       | UNSET: requirements/inventory digests, approved Interfaces digest, reviewed changes since historical approval packet                                   |
| G4 decisions                   | UNSET: separate API, primitive/B, H(K), applicability and measurement decision IDs, scope, exact subject digest, approver, date and evidence reference |
| Build identities               | UNSET: lockfiles/build-input SHA-256, commands/features/target, runner image, binary and packaged/deployed artifact SHA-256                            |
| Runtime identities             | UNSET: actual SDK/CLI/runtime revisions, compatibility date/flags, deployment/service identity and POP where observable                                |
| Execution ownership            | UNSET: named EdgeZero preparer and actual TS maintainer/operator, dated ownership acknowledgement, reachable runbook/work-item link                    |
| Package acceptance             | UNSET: TS acceptance date, accepted external runbook/package digest and candidate/build identities                                                     |
| Staging authorization          | UNSET: issuer, date, evidence reference, isolated service and permitted actions; no production traffic/stores                                          |
| Entitlements and origins       | UNSET: required feature entitlements, controlled origins, approved delays/failure modes and non-secret settings references                             |
| Execution mapping              | UNSET: each G1 leg, actual command/cwd, unique sentinel, expected assertions/case counts, samples/thresholds where required, artifact and owner        |
| Measurement approvals          | UNSET: M1/M2/M5/M7 comparison definitions, thresholds, sampling/randomization, 14b control and H(K) numerical rule/domain/maximum/tolerance            |
| Handoff and retention          | UNSET: sanitized artifact manifest, independent review, durable non-expiring retention and consumer confirmation                                       |

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
not exact-candidate acceptance. Install the worktree's `.tool-versions` and fetch all three
locked Cargo workspaces as described in Q2 before offline checks.

```sh
node --test scripts/check_outbound_cutover_evidence.test.mjs
node scripts/check_outbound_cutover_evidence.mjs --manifest
node --test scripts/check_forwarding_example.test.mjs
node scripts/check_forwarding_example.mjs
scripts/run_test_nonzero.sh outbound_evidence_assembly_retains_case_counts_and_hashes cargo test --offline --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-harness outbound_evidence_assembly_retains_case_counts_and_hashes
```

Expected: nonzero Node test listings with zero failures, exactly one passing non-ignored
forwarding doctest, exactly one passing named Rust sentinel, and inventory totals of
255 criteria / 511 planned legs / `qualified:false`. Record actual test counts for the frozen
candidate. A manifest pass deliberately permits pending decisions and planned commands.

Also run Q2's five primary Rust gates, excluded demo/generated/fixture checks, seven adapter
feature matrices and actual target sentinels. Those exact commands remain in the qualification
plan; compilation, routing sentinels and local smoke are not hosted outbound acceptance.

Worktree tool pins are Rust 1.98.1, Node 24.20.0, Fastly CLI 16.0.0, Spin 4.1.0,
standalone Viceroy 0.21.0 and Wasmtime 48.0.2. SDK requirements are Fastly 0.13.1,
worker 0.8.5 and spin-sdk 7.0.0. Record actual executable identities rather than borrowing
historical paths. Local fixture runners accept `VICEROY_BIN`, `SPIN_BIN`, `WRANGLER_BIN` and
`WORKER_BUILD_BIN`; Cloudflare also requires explicit absolute `WASM_BINDGEN_BIN`,
`ESBUILD_BIN` and `WASM_OPT_BIN`. Preserve `WRANGLER_SEND_METRICS=false` and a scoped
`WRANGLER_LOG_PATH`. See the [fixture README](README.md) for runtime preflight and limitations.

## Existing Response-Egress Fixture Only

The following builds select the implemented lifecycle fixture, **not the planned full
outbound qualification package**. These commands may be checked locally before freeze using
explicit characterization identities; those builds cannot be relabelled frozen evidence.
For accepted execution use only the Q3 sheet's clean candidate and reviewed build inputs.

```sh
export EDGEZERO_QUALIFICATION_REVISION="$CANDIDATE_SHA"
export EDGEZERO_QUALIFICATION_ARTIFACT="$BUILD_INPUT_SHA256"
cargo build --offline --locked --release --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-cloudflare --features qualification --target wasm32-unknown-unknown
cargo build --offline --locked --release --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-fastly --features qualification --bin fixture-fastly-qualification --target wasm32-wasip1
cargo build --offline --locked --release --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-spin --features qualification --target wasm32-wasip2
cargo build --offline --locked --release --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-axum --features qualification
```

`EDGEZERO_QUALIFICATION_ARTIFACT` is a lockfile/build-input digest, **not a binary hash**.
After building, record actual binary/package SHA-256 separately. Cloudflare's Cargo output
alone is not its deployable worker package; record the generated shim, WASM and configuration
identities. TS must confirm the actual isolated-service packaging/deployment command and
settings. This template supplies no deployment authorization or invented deployment recipe.

Only after accepted target configuration and explicit staging authorization:

```sh
node scripts/qualify_response_egress.mjs --execute-hosted TARGET.json CLIENTS.json
node scripts/qualify_response_egress.mjs --validate CLIENTS.json EVENTS.json
```

Use the README's strict target JSON shape with actual allowlisted HTTPS origin, SDK/runtime,
compatibility, candidate, artifact and deployment facts. Thirteen scheduled scenarios plus
thirteen healthy recovery requests produce **26 client observations**; a complete fixture run
requires four lifecycle events per request, **104 events**. These are not the 511 G1 legs or
measurement sample counts. No outbound backend is contacted by this fixture.

Collection intentionally exits **2**, not a qualification pass. Export complete unsampled
events independently; only an independently confirmed complete export permits setting
`telemetry_complete`. Do not discard failures or duplicate/conflicting events. Validation can
qualify ownership for the scheduled probes only: `HostHandoff` is not client delivery, instance
identity is unobserved, and recovery does not prove same-isolate capacity recovery. Missing
transport/parser/allocation evidence remains separate. Fastly requires an independently enforced
host execution ceiling of 1..60000 ms; client budget is 1000..10000 ms and the fixture server
response budget is 500 ms. Before/after checks cannot interrupt a blocking host call.

## Full Outbound Execution Bindings: Not Implemented

G1 strings containing `outbound-cutover`, `--case`, `hosted-runbook` or `consumer-runbook`
are planned recipes, not commands to copy into a terminal. Q3/Q4a must replace them with
owner-confirmed runnable invocations before a leg is ready. No full outbound probe or
frozen-record export invocation is available at this checkpoint.

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
- For 14b, both SDK-only control and candidate need positive cross-execution origin connection
  IDs across fresh single-request executions. Registration counts, POP or TLS resumption alone
  do not prove pooling. A negative/inconclusive control blocks qualification. Separately deferred
  M4/reused-sandbox work does not waive 14b or approve reuse.
- Pre-approve literal B/domain and numerical H(K) rule/domain/maximum/tolerance with exact
  subject digests. Record original entry t0, original D/BudgetSource, preparation, host-call and
  total timings. Exercise the actual optimizer host call and timely-head/pending-body case;
  pre-dispatch expiry is not a host-call proof. Validate both original-entry and host-entry
  inequalities; never select a horizon or inflate thresholds after seeing results.

## Draft Assembly, Validation and Release

The existing tool consumes explicit observations, not generic smoke JSONL/FNV summaries:

```sh
node scripts/check_outbound_cutover_evidence.mjs --assemble --candidate "$CANDIDATE_SHA" --input OBSERVATIONS.json --artifacts EVIDENCE_DIRECTORY --output NEW_DRAFT.json
node scripts/check_outbound_cutover_evidence.mjs --report --candidate "$CANDIDATE_SHA" --record NEW_DRAFT.json --artifacts EVIDENCE_DIRECTORY
node scripts/check_outbound_cutover_evidence.mjs --release --candidate "$CANDIDATE_SHA" --record FROZEN_RECORD.json --artifacts EVIDENCE_DIRECTORY
```

`--assemble` preserves supplied counts/outcomes and verifies linked build/raw-artifact SHA-256;
it writes a new `stage:"pre-freeze", qualified:false` draft and exits nonzero for open gates.
`--report` and `--release` share release rules. A successful check does **not** authenticate
telemetry/approvers, prove source-to-binary provenance, authorize execution/publication, establish
criterion-specific H(K)/pooling thresholds, or independently grant qualification. Their output's
`qualified:false` is not changed by success. Frozen-candidate export/mapping remains unimplemented;
changing a draft label does not perform freeze, execution, approval or export.

Use stable sanitized file snapshots: checker JSON is capped at 16 MiB; raw artifacts at 256 MiB,
hashed with 64 KiB scratch. The checker rejects artifact symlink/path escapes and refuses output
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

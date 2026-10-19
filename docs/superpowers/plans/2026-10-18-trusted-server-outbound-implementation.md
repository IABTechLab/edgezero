# Trusted Server Outbound Adoption Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Trusted Server adoption requirements without regressing the outbound work already implemented in PR 275.

**Architecture:** Extend the existing portable outbound policies and adapter-owned exchanges; do not introduce a second transport. Use one head-first dispatch boundary for plain sends, eager exchanges, and explicit redirect following, with independent callback and response-body ownership. Prove the pinned Fastly timed-read boundary before replacing its dispatch and harvest engine.

**Tech Stack:** Rust 1.98.1, existing Cargo features, Fastly SDK 0.13.1 and Viceroy 0.21.0, worker 0.8.5/workerd, Spin SDK 7.0.0/WASI HTTP 0.3 and Spin 4.1, reqwest 0.13.4, existing native Axum/Hyper runtime.

---

## Status and Authority

Planning only. No runtime tasks below are complete. Baseline is PR 275 at
`f6c708ccb5be19ba9fa92575db3c08c0f17e0d72`, called F in the
[requirements](../specs/2026-10-17-edgezero-pr275-requirements.md).
That requirements document supplies observable behavior, priorities, exceptions, acceptance
criteria, and evidence ownership. This series supplies implementation order and file ownership.
Historical outbound plans are implementation history, not instructions to recreate removed APIs.

The requirements' EZ-14.6 correction makes an expired application absolute deadline take
precedence over idle expiry, including a tie and late observation of both expiries. It changes
timeout attribution, not the deadline or the amount of permitted work.

EZ-8.10's draft revision replaces an impossible universal twice-effective-budget promise for the
synchronous Image Optimizer call with an explicitly approved finite backend-policy horizon H(K).
It preserves the original deadline/source and forbids late success. G4 approval and hosted proof
are still required; ordinary response waits and body reads keep their existing deadline +5 ms bound.

## Non-Negotiable Baseline

- Keep `OutboundRequest`, `HttpClient`, positional slot identity, input-ordered collection,
  completion-ordered observation, per-slot elapsed time, typed termination, driver poisoning,
  failure-with-partial-results, and explicit cancellation. Preserve validation of trailing driver
  events and the extra observation required for `Finished(Completed)`.
- Keep independent encoded, decoded, buffered, header, decoder-memory, Brotli-window, and chunk
  controls; pre-append enforcement; gzip native completion/multiple members; deflate; typed
  passthrough reasons; and typed decode/transport failures. Rechunking is not an allocation bound.
- Keep the app's injected monotonic clock, immutable Content-Type provenance, sanitized public
  errors, credential-safe diagnostics, fallible configuration, response completion composition,
  detached `Send | Abort`, relative/absolute egress deadlines, and existing bounded ingress/config
  behavior. Unknown platform facts stay unknown.
- Hard migration is allowed where the requirements change a contract. Migrate all adapters,
  implementations, tests, demo, and generator together. No aliases, old proxy modules, SDK escape
  clients, fallback to the old transport, or backward-compatibility branches.
- Keep core free of provider SDKs, Tokio, threads, and production test utilities. Reuse the native
  adapter's existing runtime; do not add a runtime dependency to another adapter or core.
- Do not edit the historical normative specs to make a failing implementation appear compliant.
  Document explicitly superseded behavior in the active guides and CHANGELOG.

## Execution Order

Each linked plan is an independently reviewable work package. Within a package, execute its
checkbox steps in order. D1, F3/F6/F7/F9/F10 and Q4 are task groups: their lettered tasks have separate
files, dependencies and red/green exits, and the parent closes only when every applicable child
closes. Do not parallel-edit shared `outbound.rs`, `error.rs`, or `time.rs`.

Within qualification, Q1/Q2 and Q3's preparation finish every source change, including package
tooling and the runbook template. Q3 then freezes the candidate before authoritative local runs;
Q4a binds/accepts the external package, Q4b returns hosted results, and Q5 only validates/publishes
that same SHA. A changed source commit returns to the freeze checkpoint and reruns all required
legs; publication must not create a new source revision after qualification.

| Order | Plan                                                                                  | Scope                                                            | Exit Gate                                                                                               |
| ----- | ------------------------------------------------------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| 1     | [Feasibility and evidence](2026-10-18-trusted-server-outbound-01-gates.md)            | Pinned host probes, acceptance inventory, priority decisions     | Feasibility artifacts, explicit pass/fail/pending dispositions, applicability and API approval recorded |
| 2     | [Portable policies](2026-10-18-trusted-server-outbound-02-policies.md)                | EZ-1, EZ-4/5/7/8/9; shared validation; minimal EZ-24 if promoted | One head-first implementer protocol; portable tests; no default redirects                               |
| 3     | [Adapter progress](2026-10-18-trusted-server-outbound-03-drivers.md)                  | EZ-2/3/6 and adapter integration of blocking policies            | Eager begin/batches, bounded multiplexing, correct ownership and arrival/cutoff behavior                |
| 4     | [Follow-up contracts](2026-10-18-trusted-server-outbound-04-followups.md)             | Deferred reuse, EZ-10 through EZ-24 remaining parts              | Each selected part passes its own acceptance; no implicit priority promotion                            |
| 5     | [Qualification and migration](2026-10-18-trusted-server-outbound-05-qualification.md) | EZ-25, demo/templates, CI, EZ-Q1                                 | Durable exact-commit evidence, consumer comparison, reachable pin                                       |

Portable changes and each adapter migration form a buildable hard-cut series. Do not merge an
intermediate revision that violates the implemented outbound baseline. Unchanged behavior keeps
its existing tests; replacing a test requires an explicit changed requirement and an equally
strong replacement assertion.

## Requirement Coverage

| Requirement | Implementation Tasks                      | Production Applicability                                               |
| ----------- | ----------------------------------------- | ---------------------------------------------------------------------- |
| EZ-1        | Policies P2; drivers D1-D4                | Blocking, including actual ingress forwarding                          |
| EZ-2        | Gates G2/G4; drivers D1/D5; follow-ups F1 | Blocking ranges exactly as spec; reuse-only ranges deferred            |
| EZ-3        | Drivers D1/D5                             | Blocking; remove slack only with verified bounded waits                |
| EZ-4        | Policies P1; drivers D1-D4                | Blocking; Native/BestEffort distinctions retained                      |
| EZ-5        | Policies P3; drivers D1-D4                | Blocking; Spin conditional host result recorded                        |
| EZ-6        | Policies P4; drivers D1-D6                | Blocking except EZ-6.31 reuse-only                                     |
| EZ-7        | Policies P3; drivers D1-D4                | Blocking                                                               |
| EZ-8        | Policies P1; drivers D1/D5                | Blocking; Fastly single send only                                      |
| EZ-9        | Policies P1/P5; drivers D1-D5             | Blocking; supplied clients use the same dispatch boundary              |
| EZ-10       | Follow-ups F2                             | Follow-up unless approved as promoted/present                          |
| EZ-11/12/13 | Follow-ups F3                             | Follow-up; EZ-13 closes with Unsupported evidence                      |
| EZ-14       | Follow-ups F4                             | Follow-up; deadline-first classification includes new acceptance 10/11 |
| EZ-15       | Follow-ups F5                             | Follow-up; reuse legs separately conditional                           |
| EZ-16/17/21 | Follow-ups F6                             | Follow-up                                                              |
| EZ-18/19    | Follow-ups F7                             | Follow-up                                                              |
| EZ-20       | Follow-ups F8                             | Follow-up                                                              |
| EZ-22/23    | Follow-ups F9                             | Follow-up; individual SHOULD parts require applicability decisions     |
| EZ-24       | Policies P6; follow-ups F10               | Proposed host/WASI core promotion is a decision, not assumed approved  |
| EZ-25       | Qualification Q1/Q2                       | Documentation updates accompany each affected behavior                 |
| EZ-Q1       | Gates G1; qualification Q3-Q5             | Required blocking, approved promoted, and confirmed-present parts only |

This summary inherits the normative-range-to-leaf ownership table in
[plan 04](2026-10-18-trusted-server-outbound-04-followups.md#explicit-requirement-ownership).
In particular EZ-11/12/13 close through F3a/F3b/F3c respectively, EZ-16/17/21 through
F6a/F6b/F6c, EZ-18/19 through F7a/F7b, and EZ-22/23 through F9a/F9b. EZ-24.11/.12/JS
remain separately selected F10a/F10b/F10c tasks. Fastly D1a/D1b/D1c/D1d own progress/backend/
connect/Image Optimizer work respectively; a dependency names the required children, not all
of D1 unless the entire driver really is required. Hosted qualification explicitly comprises
Q4a's accepted package and Q4b's returned TS-executed results. A sibling's passing test, package
delivery alone or a group heading never closes another leaf's requirement.

## Decisions Before Runtime Changes

- [ ] G1 records the decision on EZ-24.1-24.10 host/WASI promotion. The default remains Follow-up;
      no promotion is required to start EZ-1-EZ-9. Until an explicit promotion is approved, use a
      crate-private recording harness. P6 follows the shared production semantics, never precedes them.
- [ ] Confirm EZ-2 M1-M5/M7 product thresholds and all open questions that change acceptance.
      The written normative defaults remain the proposed implementation; do not silently adjust
      thresholds or skip required legs.
- [ ] G4 obtains maintainer approval for the Interfaces blocks in plan 02 before P1 changes public
      traits. Separately approve G2's Fastly primitive and literal backend-policy bound before D1.
      Image Optimizer integration additionally requires the revised horizon contract, numerical
      H(K) rule/maximum and tolerance approved by both maintainers, not inferred from timer rounding.
      A failed Fastly feasibility leg does not prohibit approved portable or other-adapter work.
      An absent or rejected API approval stops P1; it cannot be inferred from test success.
- [ ] Do not start dependent Fastly engine work until G2 passes locally and the hosted leg is
      verified or explicitly retained as a production blocker. A safe 500 before an unbounded read
      is not qualification success.

## Shared Verification and Commit Discipline

For every runtime Rust task: add a regression test, demonstrate the intended failure, implement
the smallest change, rerun scoped tests, and review the diff. `scripts/run_test_nonzero.sh` must
observe the named new sentinel; an empty test selection is failure, not green evidence. The helper
parses Rust test listings and must not wrap Node tests or host-probe CLIs. Node tasks name their
test file/cases and assert nonzero execution; probe tasks additionally record nonzero real case
counts and measured outcomes. Approval/handoff tasks close with attributed decisions/results,
not an invented Rust test. Commit boundaries
are suggested in each task; commit/push only when authorized and after required checks pass.

Run these gates before a runtime series is declared ready:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets
cargo check --workspace --all-targets --features "fastly cloudflare spin"
cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin
node scripts/check_outbound_docs_contract.mjs
bash scripts/check_outbound_legacy_api.sh
node scripts/check_brotli_dependency_contract.mjs
node scripts/check_flate_dependency_contract.mjs
node scripts/check_serde_json_map_contract.mjs
```

Q2 adds the existing target feature matrices, real SDK execution sentinels, fixture workspace,
generated-project tests, and excluded demo workspace. Fetch all three lockfile workspaces before
offline audits. A local contract pass does not promote a hosted capability.
Q2 lists the seven exact `bash scripts/check_adapter_feature_matrix.sh <adapter> <target>`
invocations (four native, three WASM), each compiling eight no-default-feature combinations.
They are required compile checks, not substitutes for target clippy or real runtime sentinels.
Every phase exit links back to this shared gate card and its task-specific evidence. The gates
remain centralized rather than diverging into five slightly different copies.

### Per-Task Red/Green and Lint Procedure

Each runtime task includes this procedure, not merely the final workspace gate. Run the named
test with `scripts/run_test_nonzero.sh` before implementation; Expected: a real failing assertion
or missing new API, never an empty selection. After implementation, Expected: the named sentinel
executes and passes, then scoped tests pass, rustfmt is clean, and clippy reports zero warnings.
For a core task the exact commands are:

```sh
cargo test -p edgezero-core --features test-utils
cargo fmt --all -- --check
cargo clippy -p edgezero-core --all-targets --all-features -- -D warnings
```

For an adapter task replace the package with the changed adapter package and run its native and
WASM commands in plan 03/Q2. For fixture changes use `--manifest-path tests/fixtures/reusable-app/Cargo.toml`;
format and lint that excluded workspace separately. Demo changes similarly use its own manifest.
Do not commit a public trait cut until every affected implementation and generated fixture builds.

Use these native test feature selections for every named adapter sentinel; replace only the
sentinel name and test target when the owning task specifies one:

```sh
scripts/run_test_nonzero.sh batch_preflight_precedence_and_indices cargo test --offline --locked -p edgezero-adapter-axum --no-default-features --features axum,test-utils --test contract
scripts/run_test_nonzero.sh batch_preflight_precedence_and_indices cargo test --offline --locked -p edgezero-adapter-cloudflare --no-default-features --features test-utils --test contract
scripts/run_test_nonzero.sh batch_preflight_rejects_streamed_slots_without_poisoning_siblings cargo test --offline --locked -p edgezero-adapter-fastly --no-default-features --features test-utils --test contract
scripts/run_test_nonzero.sh batch_preflight_precedence_and_indices cargo test --offline --locked -p edgezero-adapter-spin --no-default-features --features test-utils --test contract
```

These existing cases are regression controls, not the new task's red test. Native Fastly tests
must not enable `fastly`: SDK host symbols cannot link on the native target. Real Fastly/Spin/
Cloudflare SDK tests instead use Q2's explicit WASM target, platform feature and pinned runner.
Native all-features clippy/check is allowed because it does not link/run SDK test binaries.

### Clippy Briefing

`Cargo.toml` enables `restriction = deny` and `pedantic`; `clippy.toml` relaxes selected idioms only
in tests. Public functions need `#[inline]`, appropriate `#[must_use]`, and error/panic documentation.
Follow existing alphabetical item/variant ordering; keep types before helper functions where the
lint requires it. Production code must not gain `unwrap`, `expect`, `panic!`, unchecked indexing,
unsafe Send wrappers, blanket lint suppressions, or dead feature-only helpers. Run each adapter's
CLI-only/no-default/target feature matrix: native all-features clippy does not cover WASM-only code.

## Completion Definition

- [ ] Every required acceptance/environment leg has an executable case and a passing result.
- [ ] Every deferred leg cites its approved deferral or unmet explicit condition under EZ-Q1.8.
- [ ] No outbound regression, compatibility branch, stale generator example, or unreviewed
      capability promotion is present.
- [ ] Q5 identifies a reachable candidate SHA and a durable passing record for that exact SHA.

This turn creates the plan and fixes the requirements wording only. Runtime execution, deployment,
evidence production, consumer migration, commits, and pushes are separate actions.

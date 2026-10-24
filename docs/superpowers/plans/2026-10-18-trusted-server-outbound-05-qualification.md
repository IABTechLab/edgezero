# Outbound Qualification and Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep active docs, generated projects, and the demo aligned with the hard-cut implementation, then produce durable qualification evidence for the exact consumer pin.

**Architecture:** Extend existing documentation checks, runtime fixtures, and CI feature/target gates. Compare the approved blocking and confirmed-present follow-up behaviors through real adapters and the consumer's canonical composition; never substitute local mocks for hosted evidence. Store the result as an immutable release asset for a reachable candidate SHA.

**Tech Stack:** Existing Cargo workspaces and lockfiles, Node test runner, VitePress/Prettier/ESLint, existing WASM runners, fixture harness, protected staging environments.

---

Dependencies: execute documentation/test updates with each behavior change, not only at the end.
Final qualification requires all approved blocking work. All commands below are future execution
steps; none constitutes a claimed passing run at plan creation.
Every implementation increment uses the index's per-task red/green, rustfmt, and strict scoped
clippy card. G4's approved interface digest and G2's recorded feasibility dispositions are inputs;
qualification cannot silently approve an API change or convert pending/failed provider evidence
into a waiver.

## Task Q1: Active Guides, CHANGELOG, and Anti-Drift Checks

**Files:** Modify `docs/guide/proxying.md`, `streaming.md`, `capabilities.md`, `handlers.md`,
`middleware.md`, `architecture.md`, `routing.md`, `getting-started.md`, `configuration.md`,
`manifest-store-migration.md`, `blob-app-config-migration.md`, `dependency-majors-migration.md`,
`cli-reference.md`, `deploy-action-adoption.md`, four `docs/guide/adapters/*.md`, `CHANGELOG.md`,
and `tests/fixtures/reusable-app/README.md`; update `scripts/check_outbound_docs_contract.mjs`.
Create `scripts/check_forwarding_example.mjs` and `scripts/check_forwarding_example.test.mjs`
for the mandatory EZ-1.14 doctest. Conditionally add the broader Node regression tests and
guide-snippet checker/tests below; Q2 owns CI activation, with Q1 supplying exact invocations.

- [ ] Convert EZ-25.1-25.44 into a checked source-to-fix list in G1's inventory. First add failing
      documentation-contract cases for incorrect helper signatures, result shape, marker accounting,
      current pin text, and false streaming API names. Keep historical old-version columns and audit
      provenance outside current-state assertions.
- [ ] Document typed AlreadyDelivered/Resolved/Unresolved collection states on both success and
      failure, the batch-bound accumulator recipe after mixed check/next/collect, and one-transfer
      ownership. Demonstrate invalid merge inputs retain ownership. Do not publish the old Option
      slot shape as current or turn AlreadyDelivered into a timeout. Show read-only item/collection
      getters and merge-before-consuming `into_parts`; migrate old public-field access instead of
      leaving mutable-field examples or an alternate receipt constructor in the guides.
- [ ] Correct each active snippet against the actual API, without adding root re-exports just to
      make an old example compile. The relay example uses `ctx.into_request()?`, forwards actual
      GET/HEAD/POST ingress, requests original coding, and suppresses only EdgeZero's marker. Show
      explicit follow policy, credential/target restrictions, scheduler requirements, budgets/idle
      precedence, and cancellation ownership. Do not show GET/HEAD body dropping as a workaround.
- [x] Unconditionally compile the actual forwarding example for blocking EZ-1.14, independently
      of EZ-25.42 adoption. The narrow checker extracts the single Rust fence under
      `Forward An Inbound Request` in `docs/guide/proxying.md`, without maintaining a copied example.
      Build core with `cargo build --offline --locked -p edgezero-core --lib --message-format=json`;
      parse Cargo's artifact records to locate its rlib and dependency directory, then invoke
      rustdoc on the extracted Markdown with that extern, the workspace edition and dependencies.
      Require exactly one non-ignored doctest in its listing and run it; never invoke the example's
      network handler. Missing/duplicate fences, `ignore`, `no_run`, `compile_fail`, zero tests or
      compile failure fail the gate. Add negative fixtures for each case, including the missing
      `into_request()?`. Run `node --test scripts/check_forwarding_example.test.mjs` and
      `node scripts/check_forwarding_example.mjs` before/after the example correction.
      The example was corrected by an earlier checkpoint. The checker now rejects a real mutation
      removing `into_request()?` and passes the unchanged actual example in a nonzero rustdoc run.
      Its empty entrypoint compiles item definitions without invoking the network handler.
      This does not substitute for P2's real GET/HEAD/POST forwarding tests.
- [ ] Explain that original coding preserves only representation fields surviving ordinary
      normalization, never stripped hop-by-hop/nominated or filtered fields. Document cause-based
      generated-overflow Retry-After suppression and its precedence over synthesized headers,
      without changing ordinary upstream 413 or the effective config-503 retry policy.
- [ ] Publish only G4's approved Image Optimizer horizon rule/maximum and tolerance, with original
      entry/preparation/host-call measurements and the unchanged post-head deadline. Reject current-
      state guide claims of a universal twice-effective-budget bound; historical audit text remains
      provenance, not a promise. Document callback-scope exit separately from helper/egress completion.
- [ ] Align capabilities in core `src/manifest.rs`, four adapter `src/cli.rs`, guide tables and
      tests. Introduce only requirement-owned rows with documented Native/BestEffort/Unsupported
      semantics. Existing raw ingress, allocation, cancellation and delivery limitations stay explicit.
      Correct guides that still describe F's superseded behavior without rewriting historical specs.
- [ ] List every changed public signature and removed symbol in CHANGELOG, including external
      dispatch implementers, eager APIs, new options, assembly/custom-lifecycle changes if adopted,
      and generator updates. Keep the hard migration explicit and document client-level marker
      configuration and manual native-client scheduler setup. Include clock-only constructor removal,
      `with_time(OutboundTime)`, app-clock/timer propagation, and deterministic fake-time wakeups;
      do not publish a fake clock alone as sufficient for deadline conformance.
      Include the Fastly-local observation/snapshot API, extension and current-callback access,
      counting rules and frozen-handle lifetime in the Fastly guide and CHANGELOG; do not describe
      its numbers as client-lifetime totals or provider-wide pooling statistics.
- [ ] Record separate maintainer-attributed decisions for EZ-25.42 and EZ-25.43 through G1/G4;
      Q1 proposes and implements, Q2 owns CI execution. Neither is implicitly adopted by the other.
- [ ] If EZ-25.42 is adopted, add `scripts/check_guide_rust_snippets.mjs` and
      `scripts/check_guide_rust_snippets.test.mjs` for
      extracting complete items/wrapped fragments versus reasoned ignored examples. Compile against
      workspace dependencies in a temporary crate; do not fetch arbitrary new dependencies or run
      network examples. Run `node --test scripts/check_guide_rust_snippets.test.mjs` before/after
      implementation, then `node scripts/check_guide_rust_snippets.mjs` in a Rust-capable CI job
      after locked dependency preparation. Expected: broken complete items fail, valid items and
      wrapped fragments compile, ignored fragments require reasons. Record any approved partial
      guide scope explicitly instead of claiming the whole SHOULD. Q2 wires both commands into CI.
      Reuse the mandatory forwarding checker when that section is selected; do not duplicate its
      extraction/compilation machinery or make its gate conditional on this broader adoption.
- [ ] If EZ-25.43 is adopted, extend the existing docs-contract checker and add
      `scripts/check_outbound_docs_contract.test.mjs`. Run
      `node --test scripts/check_outbound_docs_contract.test.mjs` before/after implementation, then
      the production checker below. Expected: negative current-state drift fixtures fail, while
      old-version table columns, audit records and explanatory history pass. Q2 keeps existing
      production CI invocations and adds the regression-test command; do not create a duplicate checker.
- [ ] Run `node scripts/check_outbound_docs_contract.mjs`, `bash scripts/check_outbound_legacy_api.sh`,
      `npm --prefix docs run format`, `npm --prefix docs run lint`, and `npm --prefix docs run build`.
      A compile-only guide test is not runtime forwarding proof. Suggested commit with each behavior:
      `docs: align outbound adoption and hard-cut migration guidance`.

## Task Q2: Demo, Generator, and CI Execution Matrix

**Files:** Modify CLI `src/templates/core/src/handlers.rs.hbs`, `core/src/lib.rs.hbs`, root
`edgezero.toml.hbs`, root `README.md.hbs`, adapter templates as signatures require, `generator.rs`,
and `tests/generated_project_builds.rs`; corresponding `examples/app-demo/` files;
`.github/workflows/test.yml`, `format.yml`, and `scripts/run_test_nonzero.sh` only if necessary.
Create `tests/fixtures/reusable-app/ts-outbound-qualification-runbook.md` as the source template
for Q4a. Qualification binaries/tooling remain owned by G2/Q3, not duplicated here.

- [ ] Add behavior tests to both demo and generated core for empty GET/HEAD and POST forwarding,
      coding/marker options, begin+batch overlap, strict cutoff, redirect policy and error preservation.
      Include identity decoded-cap enforcement in original mode, pre-filter Location evidence,
      new connect/idle BudgetSource variants, and mixed observation/collection ownership.
      Exercise support rejection before settlement and a pending settlement read woken by its
      shorter outbound timer, using paired fake time in unit clients and actual target timers.
      Add original-coding normalization controls through the shared finalizer. Where F2/F7 are
      present, prove header decoration cannot reintroduce Retry-After on a generated overflow and
      does not suppress upstream-413/config-503 retry behavior. Templates call shared APIs; they do
      not duplicate filtering, retry-header suppression, callback guards or horizon calculations.
      Request assertions must include every effective option; keep tests independent of network or
      credentials unless explicitly opt-in runtime integration. Use the published double only if
      EZ-24.12 is adopted; otherwise migrate existing recording clients to the new protocol.
- [ ] Keep reusable policy/transport implementation in core/adapters. Templates wire entrypoints,
      choose application settings and expose examples; they do not contain another driver, decoder,
      redirect loop, body buffer, error taxonomy, or response-delivery implementation. Delete obsolete
      helper code and migrate calls directly rather than adding compatibility aliases.
- [ ] Fetch all lockfile workspaces before offline checks:

  ```sh
  cargo fetch --locked
  cargo fetch --locked --manifest-path examples/app-demo/Cargo.toml
  cargo fetch --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml
  ```

- [ ] Run `cargo test -p edgezero-cli` and
      `scripts/run_test_nonzero.sh --ignored generated_workspace_compiles cargo test --offline --locked -p edgezero-cli --test generated_project_builds`.
      Preserve this existing integration test's nonzero generated-core listing guard and actual
      core-test execution; it is not compile-only despite its name. Extend its assertions for the
      new protocol, not a duplicate wrapper. The app-demo CI test job also already exists; run
      `cargo test --locked --manifest-path examples/app-demo/Cargo.toml --workspace --all-targets`
      and equivalent demo format/clippy checks.
- [ ] Preserve all existing feature combinations in `.github/workflows/format.yml` and
      `test.yml`: default/no-default, CLI-only Fastly, native `test-utils`, and each real target with
      and without `test-utils`. Check zero-test detection. Execute real SDK paths with the existing
      pinned runners, for example from the workspace root:

  After the locked fetches above and installation of the three WASM targets, execute all seven
  existing matrix invocations. Each must compile eight feature combinations with exit zero:

  ```sh
  bash scripts/check_adapter_feature_matrix.sh axum native
  bash scripts/check_adapter_feature_matrix.sh cloudflare native
  bash scripts/check_adapter_feature_matrix.sh fastly native
  bash scripts/check_adapter_feature_matrix.sh spin native
  bash scripts/check_adapter_feature_matrix.sh cloudflare wasm32-unknown-unknown
  bash scripts/check_adapter_feature_matrix.sh fastly wasm32-wasip1
  bash scripts/check_adapter_feature_matrix.sh spin wasm32-wasip2
  ```

  These are offline/locked compilation checks. Preserve their current CI jobs; no Axum WASM
  target is supported, and passing matrices replace neither target clippy nor actual SDK execution.
  Then execute the target sentinels, for example:

  ```sh
  CARGO_TARGET_WASM32_WASIP1_RUNNER='viceroy run' scripts/run_test_nonzero.sh dispatch_runs_router_and_returns_response cargo test --locked -p edgezero-adapter-fastly --no-default-features --features fastly,test-utils --target wasm32-wasip1 --test contract
  CARGO_TARGET_WASM32_WASIP2_RUNNER='wasmtime run -W component-model-async=y -S p3=y -S http=y' scripts/run_test_nonzero.sh spin_error_code_table_is_exhaustive cargo test --locked -p edgezero-adapter-spin --no-default-features --features spin,test-utils --target wasm32-wasip2 --test sdk_resources
  CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner scripts/run_test_nonzero.sh dispatch_runs_router_and_returns_response cargo test --locked -p edgezero-adapter-cloudflare --no-default-features --features cloudflare,test-utils --target wasm32-unknown-unknown --test contract
  ```

  These runner contracts do not replace actual workerd/Spin HTTP fixtures. Add new nonzero
  outbound sentinels for this work instead of relying only on routing sentinels.

- [x] Run `node --test scripts/check_forwarding_example.test.mjs` and
      `node scripts/check_forwarding_example.mjs` unconditionally in the existing Rust-capable
      workspace CI job after locked dependency preparation. Preserve the one-doctest assertion;
      deferring EZ-25.42 does not skip this blocking EZ-1.14 gate. Do not pass Node through
      `run_test_nonzero.sh`, which parses Rust listings.
      Implemented in the workspace host-test leg with the repository-pinned Node toolchain;
      local execution is separate from a hosted CI result and frozen-candidate qualification.
- [ ] Activate each adopted conditional branch, not just its compilation: P3's individual metadata
      sentinels, F10a cancellation on selected targets, F10b's new assertions in the existing generated/
      demo test gates and F10c's
      explicit JS runner when adopted. For Q1's adopted EZ-25.42/43 run the exact checker and Node
      test commands above in CI with the proper toolchains/dependencies. G1 records job and command
      per selected part; deferred parts retain a documented decision, not a green skipped test.
- [ ] Run all five primary gates explicitly, including the independent Spin compilation check:

  ```sh
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  cargo test --workspace --all-targets
  cargo check --workspace --all-targets --features "fastly cloudflare spin"
  cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin
  ```

  Expected: all commands exit zero; gate 5 compiles Spin's real target path after its scheduler
  rewrite. Neither a native feature build nor a passing SDK stub replaces that target check.

- [ ] Compile/check all demo target adapters using their current package names and features in
      `test.yml`. Run the root verification list from the index plus fixture workspace tests/format/
      clippy. No green root-workspace result substitutes for the excluded workspaces. Suggested
      commit: `test: align demo generator and target gates with outbound adoption`.
- [ ] Prepare and commit the runbook source template for Q4a, with the G1 case map, G4 approval
      references, exact build/probe/export/validation commands and expected counts. Use explicit
      candidate/build placeholders here; Q4a fills them in its external copy after Q3 freezes.
      Finish all documentation/package-template source edits before that freeze. Runtime ownership
      acknowledgement, staging authorization and candidate-specific package acceptance remain Q4a
      gates, not implied by committing this template.

## Task Q3: Evidence Preparation, Candidate Freeze and Local Acceptance

**Files:** Extend G1's inventory/checker, fixture harness `net.rs`, `runners.rs`, `evidence.rs`,
`main.rs`, fixture-core qualification, and adapter fixtures. No consumer production code is owned
by this plan.

Finish the source-producing steps below and Q2's runbook/template preparation before freezing.
Pre-freeze runs are development checks, not the authoritative EZ-Q1 record. After the freeze
checkpoint, Q3/Q4 collect evidence externally without editing candidate source.

- [x] Add `outbound_evidence_assembly_retains_case_counts_and_hashes` to fixture-harness tests and
      run `scripts/run_test_nonzero.sh outbound_evidence_assembly_retains_case_counts_and_hashes cargo test --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-harness`.
      Expected: red for incomplete record assembly, then nonzero case counts, exact candidate/build
      identities and raw artifact hashes survive export. Run the Node evidence-validator test file
      below separately, asserting its cases actually execute; do not pass Node through the Rust
      listing helper. Host runs still own measured outcomes, not these synthetic unit cases.

- [ ] Supply controlled origins for every blocking case and confirmed-present follow-up. Include
      simultaneous requests, trickled headers/bodies, pending reads, encoded ranges, invalid body
      completion, redirects with unreadable bodies, capacity exhaustion/recovery, and host-cache
      behavior where applicable. Each case asserts observable counts, bytes, typed outcomes, ordering,
      deadlines, and owner release rather than just HTTP status.
- [ ] Add a record-writer mode and release-validation invocation to the G1 checker. Normalize
      results into explicit per-leg pass/fail/approved-deferred status with artifact hashes. Include
      confirmed-present follow-up mapping from CHANGELOG plus consumer confirmation. A missing
      blocking part is a failure, not an applicability exception.
- [ ] Run `node --test scripts/check_outbound_cutover_evidence.test.mjs` with synthetic records
      proving wrong-SHA, missing-leg, wrong-environment, wrong-version, empty measurement, failing
      threshold and unapproved-deferral records are rejected. Provide human-readable output with
      the exact missing gates. Map new normalization, callback-boundary and combined overflow/
      decoration cases to their actual environment/conditional legs. Reject an Image Optimizer
      record lacking the pre-approved H(K) rule/maximum/digest or original-entry timing, or one
      substituting pre-dispatch expiry for an executed host-call probe. Do not compute the horizon
      retrospectively from the run. Validate both the host-call-entry and original-method-entry
      inequalities, so unused preparation budget cannot conceal a host-call horizon violation.
      Reject a reuse record based only on registration counts, POP, or TLS
      resumption rather than the approved 14b cross-execution origin connection proof. Missing
      entitlement/hosted execution is pending, never a passing Image Optimizer result. Suggested
      commit: `test: assemble outbound acceptance evidence`.
- [ ] Freeze the candidate only after all implementation, test, checker, documentation and
      qualification-package source changes, including Q2's runbook template and G2/Q3 tooling.
      Record the clean source SHA and build identities externally. Any later source commit
      invalidates exact-commit qualification: freeze the new SHA and rerun all required legs,
      not only tests thought affected. Candidate freezing does not authorize deployment or release.
- [ ] At that frozen SHA, run the G1 inventory leg by leg under unit, native Axum TCP,
      Viceroy 0.21.0, workerd and Spin 4.1. Record Rust/SDK/runtime versions, exact source SHA,
      runner image, case counts, seed, measurements and sample counts. An offline compile,
      successful stub, skipped case, or safe refusal of a required bounded operation cannot
      satisfy an acceptance leg. These are the authoritative local results Q4a/Q5 consume.

### Q3 Pre-Freeze Assembly Checkpoint (2026-10-23)

- [x] Keep assembly and artifact validation in the existing Node checker. Add `--assemble` for
      explicit per-leg observations and `--report` for all independently evaluable release gates.
      The fixture Rust sentinel exercises that actual CLI; no duplicate Rust validator or JSONL
      conversion is added.
- [x] Preserve status/sample/assertion/measurement values, candidate SHA and linked SHA-256 build
      identity. Compute raw hashes from bounded regular files, reject escapes and refuse overwrites.
      Drafts remain pre-freeze/unqualified and release validation rejects dirty/characterization
      evidence. No missing metadata, result, applicability decision or approval is invented.
- [x] Share release checks and artifact hashing with the diagnostic collector. Invalid catalog
      entries do not erase unrelated evidence. Accepted totals exclude invalid artifacts/deferrals;
      planned-command and comparison gates are evaluated even when results are missing.
- [x] Activate Node regressions and the nonzero Rust sentinel in the existing workspace CI leg.
- [ ] Finish controlled origins, frozen-candidate export/mapping, approved H(K)/pooling validation,
      the runbook, candidate freeze and all local/hosted/consumer qualification before closing Q3.

This is preparatory tooling only. Its synthetic tests do not establish runtime measurements,
source-to-build provenance, maintainer approvals or hosted delivery. The full record-writer/export
step above remains open for frozen-candidate integration. The reviewed 511-leg set, frozen APIs,
transport behavior and failed Fastly G2 are unchanged.

## Q4: Hosted Qualification Handoff Task Group

Q4a delivers a runnable package; Q4b is TS-owned execution and result return. Delivery alone
cannot close Q4 or satisfy the hosted acceptance gates. Deployment requires separately recorded
authorization to the consumer-owned staging service, never inferred from this plan.

### Task Q4a: Qualification Package and Accepted Execution Runbook

**Files:** Read the committed qualification binaries/tooling and fixture README from G2/Q3,
Q2's `tests/fixtures/reusable-app/ts-outbound-qualification-runbook.md` source template, and
G1's checker/inventory. Produce an external candidate-specific package/runbook and acceptance
record, not another source commit. No accepted runbook exists at plan creation.

**Dependencies:** Q3's frozen candidate and authoritative local acceptance, the committed Q2
runbook template and G2/Q3 package sources, and G2/G4 approvals. Bind candidate/build hashes in
the external execution package/evidence record without another source commit. EdgeZero prepares the
package; G1 must record the actual TS maintainer/operator accepting
execution ownership and a reachable runbook/work-item link before Q4b starts.

- [ ] EdgeZero supplies the exact candidate qualification build for all EZ-Q1.4-listed legs,
      including EZ-1 body/deadline cases, EZ-2 criteria 11/12/13b/14a/14b, EZ-4 criterion 8, hosted EZ-6
      scenarios 1/2/6/7/8/11 and hosted Image Optimizer behavior. Include version and candidate SHA
      in an authenticated/non-secret fixture identity, never provider diagnostics in public errors.
- [ ] Populate the external runbook copy with candidate/baseline SHAs and build hashes, G1 case
      map, G4 API/B/H(K) and threshold approvals, exact SDK/runtime versions, settings, entitlements,
      controlled origins, sampling plan and canonical lifecycle composition. List the build, probe,
      export and evidence-validation invocations and expected case counts per leg. Require TS to
      confirm its deployment/browser/response-diff commands rather than inventing commands for its
      repository. Include its known integration command, run from the TS checkout:

  ```sh
  cargo test --manifest-path crates/trusted-server-integration-tests/Cargo.toml --test integration -- --include-ignored
  ```

- [ ] Review package identities and runbook invocations against G1; execute local probe/export
      paths and the evidence-checker regression tests. Expected: exact-commit package, no zero-case
      acceptance, and TS acknowledgement of inputs/ownership. Missing operator, missing command,
      entitlement or deployment authorization leaves execution pending; do not fabricate acceptance.
      A run exposing an implementation defect requires a new reviewed/frozen source SHA and
      rerunning all required local and hosted qualification legs; do not relabel earlier evidence.

### Task Q4b: TS-Executed Hosted and Composition Evidence

**Files:** Q4a's accepted runbook/package and G1's evidence artifacts. TS owns its checkout,
deployment and actual hosted runs; EdgeZero owns validating returned records in Q3/Q5.

**Dependencies:** Q4a acceptance, explicit staging authorization, required entitlements and
pre-approved measurement definitions. None can be replaced with local CI success.

- [ ] TS runs its own checks and remaining hosted results in canonical `run_custom` plus
      `send_request_with_registries_and_hooks`, single-request mode by default. Its baseline is
      `a9037770a` plus prerequisite PR 1123 at `4c5ca8ae5`; record immutable identities for both
      baseline and candidate, service compatibility settings, entitlements, origin delays, and POP.
- [ ] Run EZ-2 M1, M2, M3, M5 and M6, plus EZ-3 M7, using their approved pass conditions,
      randomized build order,
      fixed page mix/rate and sample counts. Use a controlled silent-drop connect destination, not
      TEST-NET assumptions; keep refused connection separate. Probe heterogeneous hosts/budgets,
      rolling versions, origin reuse, and Image Optimizer transformation/bounded-stall/query behavior.
      M4 applies only to separately applicable reusable-sandbox qualification before enabling reuse;
      record its exclusion from single-request qualification under EZ-Q1.5/Q1.8. Blocking 14b
      cross-execution pooling evidence remains required and does not promote M4.
- [ ] For Image Optimizer, use the separately approved EZ-8.10 horizon contract, numerical H(K)
      rule/maximum/domain and tolerance. Run 1 s and shortened-absolute-deadline cases with the same
      per-call timeout/backend, plus representative policy boundaries. Record preparation, host call
      and total time from original entry; require original-source 504 on late success/error. Probe
      a timely head with a pending body against the unchanged ordinary deadline. Deterministic
      nanosecond/millisecond boundary proofs complement hosted executions; an expired-before-send
      sample is not a hosted horizon pass. Violations fail qualification, not trigger threshold inflation.
- [ ] For 14b, run both the SDK-only control and candidate across fresh single-request executions
      with approved sample counts and origin connection/execution IDs. Require positive cross-
      execution connection reuse in both plus M1's connection/full-TLS/resumed-TLS counts and
      thresholds. A negative/inconclusive control blocks qualification; it does not make pooling
      vacuously pass. This proof does not promote M4 or reuse-mode follow-ups. Read the approved
      literal B and domain from G4; registration cardinality must meet that bound without weakening
      live-budget, connect, or Image Optimizer constraints.
- [ ] TS runs the specified Viceroy/workerd/Axum integration scenarios with `--include-ignored`,
      browser suite, Spin smoke and Fastly response diff. Approved deferred behavior has explicit
      differences, not transport workarounds. Record any failing criterion as a production blocker;
      do not quietly demote a blocking deadline requirement to fit provider behavior.
- [ ] Add hosted results to the exact-commit record without committing credentials, raw payloads,
      signing material or diagnostic URLs. Reuse-only legs remain gated until reuse is enabled.
      Local success cannot satisfy these steps; report absent credentials/authorization as pending.
- [ ] Return to Q3/Q5 the per-leg result, case/sample counts, timings, sanitized raw artifacts and
      hashes, candidate/build/deployment identities, failures/pending reasons, and TS acknowledgement.
      Validate using G1's release-mode invocation from the accepted runbook. Expected: every required
      leg passes its pre-approved threshold, or qualification remains blocked with the exact cause.

## Task Q5: Release Record and Reachable Pin

**Files:** Read G1 inventory/checker, `CHANGELOG.md` and fixture README; publish release asset and immutable tag
for the reviewed candidate, created only when release/push is authorized.

- [ ] Verify the reviewed source/build identities still match Q3's frozen candidate and Q3/Q4's
      local/hosted records. Do not freeze a new commit or edit source during publication. Any source
      change returns execution to Q3's freeze checkpoint and reruns all required legs for the new SHA.
- [ ] Validate EZ-Q1.1-Q1.8 against the assembled record. Required results all pass; approved
      partial promotions/deferrals are explicit. Preserve failures as evidence but never as a passing
      record. Review body-read readiness, cancellation, raw-ingress and allocation caveats unchanged.
- [ ] Publish the passing record and supporting artifacts as a durable release asset attached to
      a reachable immutable tag for that SHA. A 14-day or 90-day expiring CI artifact is insufficient;
      the record remains retrievable while TS pins the commit. Verify URLs and hashes after publishing.
- [ ] Obtain consumer confirmation of implemented follow-up applicability and acceptance of the
      candidate. Only then recommend repinning. Do not report production readiness merely because
      CI is green or a PR is mergeable.

## Completion

- [ ] Execute the index's shared gates, Q1/Q2's concrete checks and selected target commands, and
      G1's documented release-mode validation on Q3/Q4's returned record. Expected: nonzero passing
      applicable tests and a record for the exact SHA; approval/handoff gates remain separate checks.
- [ ] All baseline outbound guarantees remain enforced and no compatibility implementation remains.
- [ ] Every required actual adapter, generated core, demo, docs and target gate passed nonzero.
- [ ] Hosted evidence and consumer qualification passed for the exact reachable candidate.
- [ ] No pending production gate is described as implementation or certification complete.

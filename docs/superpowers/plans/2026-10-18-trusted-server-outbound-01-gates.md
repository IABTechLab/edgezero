# Outbound Feasibility and Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the host primitives and acceptance inventory needed to implement the blocking cutover without weakening outbound requirements.

**Architecture:** Extend the existing reusable-app fixture and harness, not the application transport. Keep characterization results separate from production implementations and from capability promotion. Validate the exact pinned SDK/runtime graph before depending on readiness or executor behavior.

**Tech Stack:** Existing fixture Cargo workspace, JSON evidence, Fastly SDK 0.13.1, Viceroy 0.21.0, workerd, Spin 4.1, native Axum.

---

Read the [index](2026-10-18-trusted-server-outbound-implementation.md) and its baseline invariants.
This phase does not claim any requirement implemented merely because an SDK declares an API.
Apply the index's per-task test/format/clippy procedure after every eventual code change.

## Execution Record

G1's draft inventory and fail-closed checker are implemented: 255 source acceptance cases,
including two consumer prose checks, plus supplemental template/demo checks produce 511
reviewed split/environment legs. Individual
execution commands remain planned until their task owners implement and activate them; no
applicability or product approval is fabricated. Scope lock awaits those owner decisions.

G2's first measured error-preservation control **failed** on pinned Viceroy 0.21.0. The opt-in
`outbound-readiness` suite and its nonzero measurement guard are implemented. Three direct reads
report `UnexpectedEof`; the corresponding timed-readiness paths report clean EOF. See the
[feasibility report](../../audits/2026-10-18-outbound-feasibility.md). The remaining scenarios below
are not passing or complete. D1's dependent proposal is cancelled pending a reviewed primitive;
G3 probes record native/Viceroy local observations and Cloudflare's tooling block. The initial
Spin failure exposed and now regression-tests an internal egress early-success bug; the fixed
local cases pass. G3's remaining legs and Fastly-policy approvals stay pending. A later scoped
conversation reply authorizes the exact G4 API digest, with maintainer attribution supplied as AG.
P1's core clock/timer foundation has started without changing client traits, adapter transports
or constructors. The response-egress fix itself did not approve the new begin API.

## Task G1: Acceptance Inventory and Scope Lock

**Files:** Create `tests/fixtures/reusable-app/outbound-cutover-cases.json` and
`scripts/check_outbound_cutover_evidence.mjs`; test in
`scripts/check_outbound_cutover_evidence.test.mjs`; document the invocation in
`tests/fixtures/reusable-app/README.md`.

- [x] Write Node tests rejecting a missing blocking leg, wrong candidate SHA, zero samples,
      expired-only artifact, wrong environment, failed measurement, and unjustified deferral.
      Run `node --test scripts/check_outbound_cutover_evidence.test.mjs`; demonstrate failure.
- [ ] Inventory every acceptance criterion of EZ-1-EZ-25 and EZ-Q1, including unnumbered EZ-9
      cases. Give each an immutable case ID such as `EZ-9/acceptance/09`, source-text digest, and
      separate environment legs. JSON fields are `case_id`, `requirement_ids`, `source_digest`,
      `environment`, `applicability`, `approval`, `expected`, and `owner`. Digest mismatch requires
      review, not automatic acceptance. Build fixtures with `json!` or structured serialization.
      Add owning task/substep, named sentinel, exact command, expected assertion, and artifact path
      for every leg. Use the explicit follow-up traceability table in plan 04; a grouped title alone
      does not establish coverage. Split EZ-2 criterion 14a caching and 14b fresh-execution pooling.
- [ ] Implement fail-closed applicability checks for blocking parts, approved promotions,
      confirmed-present follow-ups, and explicit conditions. Split mixed criteria. The default
      pending status cannot satisfy a required leg; approved deferred JS/reuse legs do not fail an
      unrelated host/WASI promotion. Record unresolved priority/product decisions for maintainers.
      Record Image Optimizer staging entitlement/owner availability explicitly: EZ-8 remains
      blocking unless maintainers approve a written requirement change. CI's lack of an entitlement
      is not a passing result or an automatic demotion. EZ-24 remains Follow-up unless promoted.
- [ ] Record these conditional decisions individually. The task owner prepares the proposal;
      the attributed EdgeZero maintainer approves adoption, with TS approval for any change to its
      blocking/applicability set. Name the actual approver, date, exact part/target, implementation
      owner, sentinel/command, CI owner and evidence in G1/G4. Pending is not adoption, and an
      approved sibling never adopts the rest of a range:

  | Conditional Part              | Proposal / Implementation Owner | Execution After Approval                                                               |
  | ----------------------------- | ------------------------------- | -------------------------------------------------------------------------------------- |
  | EZ-5.9 and EZ-5.10 separately | P3                              | Shared finalizer plus actual adapter metadata controls; Q2 activates adopted sentinels |
  | EZ-24.11                      | F10a                            | Published-double abandonment/cancellation on each adopted target                       |
  | EZ-24.12                      | F10b                            | Delete bespoke demo/template doubles and execute generated/demo tests                  |
  | EZ-24.2 JavaScript target     | F10c                            | Explicit JS feature/runtime tests, independent of host/WASI promotion                  |
  | EZ-25.42                      | Q1                              | Snippet checker, Node tests and compile invocation in Rust-capable CI                  |
  | EZ-25.43                      | Q1                              | Existing docs-checker drift regressions and CI execution                               |

- [x] Run the Node tests and `node scripts/check_outbound_cutover_evidence.mjs --help`.
      Add a manifest-validation mode that passes with pending results and a separate release mode
      that fails until all applicable results pass. Review the inventory against EZ-Q1.1-Q1.8.
      Suggested commit: `test: define outbound cutover evidence inventory`.

## Task G2: Fastly Timed Header and Body Readiness Proof

**Files:** Create `tests/fixtures/reusable-app/crates/fixture-fastly/src/bin/outbound_readiness.rs`
and register the binary in its `Cargo.toml`; add controlled origin cases in
`tests/fixtures/reusable-app/crates/fixture-harness/src/net.rs`; add execution/evidence in
`runners.rs`, `evidence.rs`, and `main.rs` in the same harness; update fixture `README.md`.
Create the report `docs/audits/2026-10-18-outbound-feasibility.md` as this task's output, not as a
claim that feasibility has already been established.

- [x] First inspect the exact Fastly `fastly-sys` async I/O declarations, normal-body readiness,
      pending-response readiness, read semantics, and Viceroy implementation cited in EZ-2 open
      question 6. Record what read size and readiness state make a subsequent read bounded. A
      readiness indication followed by an unconstrained blocking read does not establish the gate.
- [ ] Add loopback origin scenarios: silent head; head now/body permanently pending; one byte
      every 300 ms under a 500 ms deadline; two pending bodies with only one ready; one ready body
      becoming exhausted between reads; EOF, malformed framing, and transport failure. Prove both
      buffered and streamed reads return by deadline +5 ms and preserve typed source errors.
- [x] Add an `outbound-readiness` fixture suite and a nonzero case-count assertion. Run
      `scripts/smoke_test_reusable_app.sh --adapter fastly --suite outbound-readiness` after adding
      support for that suite. Capture per-read start/end times, readiness outcome, and candidate SHA.
      This command is a planned extension, not an existing passing suite at F.
- [x] Name the harness guard `outbound_readiness_rejects_empty_or_unbounded_measurements` and run
      `scripts/run_test_nonzero.sh outbound_readiness_rejects_empty_or_unbounded_measurements cargo test --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-harness outbound_readiness_rejects_empty_or_unbounded_measurements`
      before/after adding the suite and evidence assertions. Expected: red for missing case/measurement
      validation, then a nonzero pass rejecting empty counts or missing read/timer samples. Execute
      the actual `outbound-readiness` command above afterward; its measured host reads, not the
      harness unit test, establish feasibility. Apply the fixture task fmt/clippy card.
- [ ] Provide the same fixture build and invocation contract for an authorized hosted Fastly
      staging run. TS owns hosted execution under EZ-Q1.4. Store SDK, runtime/service settings,
      source SHA, and timings. If proof fails, stop dependent Fastly qualification; report the
      precise provider boundary. Never add the rejected blocking-body fallback or widen +5 ms.
      Suggested commit after local proof: `test: probe deadline-bounded Fastly body readiness`.
- [ ] Write the feasibility report with exact source/SDK/runtime identities, raw result hashes,
      response-head wait operation, body readiness operation, maximum read size after readiness,
      timer granularity, EOF/error behavior, cancellation ownership, sibling-progress result, and
      proposed finite deadline-only backend-policy bound. Classify each local and hosted leg as
      Passed, Failed, or PendingExternal; include the exact callable internal interface and an
      example of one timeout/read transition. D1 must read this artifact before selecting its primitive.
      B is a proposed measured/cardinality-proved integer here; only G4 can approve it. The report
      must not impose a minimum budget or silently omit a budget domain. Include EZ-8.10's proposed
      finite numerical horizon rule H(K), maximum and full policy domain separately from B. Account
      for quantization, phase composition, retry/provider work and unchanged eligible settings for
      equal per-call timeouts with differing deadlines. SDK timer arithmetic alone does not prove a
      synchronous host-call bound. Record unknown/failed proof explicitly, never invent a finite horizon.
- [ ] If a local leg fails, cancel D1's proposed Fastly multiplexer/slack-removal implementation
      and defer its dependent Fastly D5/D6 qualification legs. P1-P5 portable work and D2-D4 may
      proceed independently, but TS production adoption stays blocked. If only hosted execution is
      unavailable, mark those legs PendingExternal; locally proved work may proceed at risk without
      claiming qualification. If hosted behavior contradicts local proof, cancel provider promotion,
      stop release/repinning, and return to a reviewed provider/API or requirement decision. The
      report must list runnable, cancelled, and deferred tasks by ID; no legacy client, blocking-read
      fallback, cutoff widening, or automatic BestEffort waiver is an alternative branch.

### G2 Local Runtime Repair Checkpoint (2026-10-23)

- [x] Carry an exact-source Viceroy 0.21.0 patch and co-located tests in PR #275, with explicit
      apply/build/runtime-override instructions. No SDK/runtime version pin or installed binary changes.
- [x] Reproduce nine failing new tests before the repair; pass all ten afterward and all 135
      upstream library tests. Preserve owned typed errors, empty-frame safety, cancellation/append
      ownership, first-poll read bounds and cooperative decoder progress.
- [x] Retain separately identified stock/repaired guest runs using identical guest bytes: three
      stock comparisons fail, three repaired comparisons preserve UnexpectedEof. Patched probe
      exit 2 remains unverified overall; old failed evidence is unchanged.
- [ ] Complete the remaining full G2 ABI scenarios and exact-version review, then obtain the
      separate B/H(K) and hosted evidence/approval gates before dependent qualification.

### G2 Expanded Diagnostic Checkpoint (2026-10-23)

- [x] Implement eleven owned loopback scenarios with bounded sockets, request heads, workers,
      lifetime and joined cleanup; remove the superseded shared-origin truncation shortcut.
- [x] Run a fixed 135-probe / 147-request inventory: buffered and pull-streamed consumption,
      one-byte and 16 KiB reads, three repetitions and finite direct truncation controls.
- [x] Reject incomplete/duplicate identities, contradictory reads, missing fresh readiness,
      changed clocks/deadlines, invented terminal outcomes, mismatched errors and late operations.
      Persist completed samples incrementally so failed guests retain their preceding trace.
- [ ] Establish the fixed +5 ms bound. The first complete repaired run retained a 507.794 ms
      pending-body result, so local timing acceptance remains failed. Do not widen or waive it.
- [ ] Complete production-path and exact-version review, B/H(K) decisions and hosted evidence.

The [expanded diagnostic record](../../audits/evidence/2026-10-23-fastly-readiness-matrix.json)
is working-tree characterization, not frozen-candidate qualification. Earlier failed stock and
repair evidence remains unchanged. Implemented cases do not imply a passing G2 gate.

See the [repair record](../../audits/evidence/2026-10-23-viceroy-readiness-repair.json) and
[source patch](../../../patches/viceroy/README.md). This fixes the measured local runtime defect,
not the unmodified runtime or hosted service. Strict upstream clippy reports the same stock and
repaired diagnostics under Rust 1.98.1; no unrelated lint changes/suppressions are included.

### G2 Timer-Control Checkpoint (2026-10-23)

- [x] Capture raw ABI entry/return before formatting while retaining the outer operation and
      whole-exchange +5 ms checks. Reject missing/reversed/outside raw timestamp pairs.
- [x] Run identical guest bytes on stock/repaired runtimes with 72 timer-only controls each:
      eight repetitions, 1/100/500 ms, native sleep, guest WASI sleep and empty select. Exclude
      three recorded warm-ups and repeat the unchanged 135-probe / 147-request HTTP inventory.
- [x] Retain both measurement phases: native lateness in the first pair and, after review, one
      100 ms stock WASI sleep returning at 106.952 ms without HTTP body handles. Preserve locally
      passing repaired matrices, stock source-error failures and earlier failed evidence.
- [x] Exclude status formatting from timer-only comparisons; enforce positive raw wait duration
      and the declared primitive-return endpoint. Keep the G2 outer exchange deadline unchanged.
- [ ] Identify or establish a bound for the original late select. Source inspection and these
      samples narrow hypotheses, not prove a scheduling cause, fix or deterministic +5 ms bound.

The [timer-control record](../../audits/evidence/2026-10-23-fastly-timer-controls.json) is unqualified.
G2 remains Failed. Do not shorten budgets, add margins or promote the pending Fastly engine/policy.
Approved portable work can proceed independently of this failed provider gate.

## Task G3: Eager Scheduling and Empty-Body Host Premises

**Files:** Modify `tests/fixtures/reusable-app/crates/fixture-core/src/qualification.rs`, the
four fixture entrypoints, and harness `net.rs`/`runners.rs`; add adapter-local tests in each
`crates/edgezero-adapter-*/tests/contract.rs` and Spin `tests/sdk_resources.rs` where applicable.

This is host/runtime characterization before the new public begin API exists. Use direct SDK
operations only inside the opt-in qualification fixture to establish scheduler feasibility; do
not add another production client. D6 repeats the scenarios through the implemented public APIs.

**Execution update, 2026-10-19:** default-off `progress` fixtures and `outbound-progress` suite
are implemented and included after the ordinary smoke cases. The origin-receipt sentinel passed
after rejecting empty, foreign, duplicate, wrong-protocol, unknown-adapter and unmeasured-wait
traces. Separate regressions cover requested case/token identity, cross-adapter build fingerprints
and a response gate released only by the first client byte. Fixture tests: 33 harness and 10 core
passes, plus the native absent-runtime control. The all-adapter smoke run exited 1: native and
Viceroy each passed five progress and five HTTP/1 framing cases; Spin passed four progress and
five framing cases, but response-owned execution failed. Cloudflare's CLI mismatch prevented
runtime execution without repinning/downloading. See the retained G3 record linked in the
[feasibility report](../../audits/2026-10-18-outbound-feasibility.md). The checkboxes below remain
open because their full protocol, scheduler-context, hosted and callback-return proofs are not
complete. G4 approval is not inferred from partial local evidence.

**Subsequent repair:** isolated ready/timer-only responses failed through Spin's adapter while
equivalent SDK-native body-writer controls passed. The response-result future could return success
before source/writes completed; the adapter incorrectly aborted on it. It now latches that success
once, retains the same operation and deadline, and closes/publishes the body before terminal success.
The native helper regression was red before this fix; deadline/error precedence and resumption of
the same pinned operation are covered. Spin now passes all five progress cases, five framing cases
and four comparison controls, with 18 origin receipts and two token-correlated SDK terminals.
Native unpolled-worker cleanup and unconsumed-response job drops are also covered. Canonical guest
diagnostics are revalidated after the finite pre-shutdown observation hold. The latest all-adapter
smoke exits 2 for unavailable/unverified legs, not a Spin request-serving failure. Historical failed
evidence remains retained; see the newer repair record in the feasibility report. No SDK pin,
capability, public API or approval was changed.

**Cloudflare update, 2026-10-20:** an authorized temporary tool install supplied the exact locked
wasm-bindgen CLI without repinning. Local workerd 2026-09-10 now passes 24 coding, 46 marker,
five scheduling and five HTTP/1 framing checks, with 89 asserted origin receipts. The strengthened
harness requires original Workers body signals and keeps independent groups running after policy
failures. See the additive Cloudflare record in the feasibility report for source/artifact/runtime
hashes and exact compatibility settings. This supersedes the old tool gap, not the failed Fastly
gate or the remaining hosted/protocol/context/relay proofs. G3's complete acceptance stays open.

- [ ] Write a probe that starts a request, then awaits an unrelated timer/KV operation with no
      outbound polling for 100 ms. Record when the origin received it. Repeat with a never-polled
      batch and with callback return followed by response-owned streaming. Run current probes to
      demonstrate the lazy-dispatch gap; count actual requests rather than just future creation.
- [ ] Map supported scheduler installation: the existing native Axum `LocalSet`, workerd local
      task scheduling, Spin WASIp3 spawning, and Fastly cooperative timed I/O multiplexing. Prove
      missing scheduler context is detected before dispatch without a `spawn_local` panic. Keep
      provider dependencies in their adapters; no runtime enters core.
- [ ] Exercise bodiless HTTP/1, explicit zero length, terminating chunk, and supported HTTP/2/3
      cases through actual ingress. Verify workerd's absent-body signal without reproducing the
      browser body-loss bug. Establish the Spin zero-length premise before taking its fast path;
      header-derived size hints alone are not raw framing evidence. Record any unavailable protocol
      leg exactly as the requirement permits, not as a successful test.
- [ ] Run the native fixture harness tests with
      `cargo test --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-harness -p fixture-core --features fixture-core/qualification`.
      Record local and hosted work separately; no new Native claim follows from a mock.
      Suggested commit: `test: characterize outbound progress and empty-body signals`.
- [ ] Add `outbound_progress_probe_requires_origin_receipts` to the fixture harness and run
      `scripts/run_test_nonzero.sh outbound_progress_probe_requires_origin_receipts cargo test --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-harness`.
      Expected: red for missing receipt accounting, then zero-origin-receipt and unsupported-protocol
      samples cannot masquerade as successful observations. Include the actual SDK probes above in
      the smoke qualification routes and run
      `scripts/smoke_test_reusable_app.sh --adapter all --suite smoke` with nonzero probe counts per
      adapter. Record missing/failed runtime legs rather than inferring Native from the unit test;
      apply the fixture task fmt/clippy card.

## Task G4: Maintainer API and Feasibility Approval

**Files:** Create `docs/audits/2026-10-18-outbound-api-approval.md` as a future approval record;
input is plan 02's Interfaces block and G2's feasibility report, plus G1's applicability decisions.

**Preparation update, 2026-10-19:** the [review packet](../../audits/2026-10-18-outbound-api-approval.md)
now pins the complete Interfaces section and its input identities, inventories all eight current
implementations and their callers, and separates API, Fastly policy, optimizer horizon,
applicability and measurement decisions. The requester subsequently replied `continue` to the
question naming the exact API digest, then supplied AG as the approving maintainer. The packet
records this API-only approval, not any Fastly-policy approval. P1's paired-time foundation starts
under that instruction. The remaining G4 decisions stay open.

**Scoped amendment, 2026-10-24:** the requester approved replacing the private string-backed
region with a public seven-variant enum, retaining `parse`/`as_str` at string boundaries and
removing the wrapper. The [approval record](../../audits/2026-10-18-outbound-api-approval.md#scoped-region-enum-amendment)
preserves the original exact-digest approval and records this amendment separately. No other
G4 decision is approved by that instruction.

- [x] The plan owner presents the exact Interfaces block, its SHA-256 digest, the old/new
      implementer migration table, mixed-collection semantics, marker default, and timeout cause
      variants to the EdgeZero maintainer. Include synchronous pre-settlement support rejection and
      paired app-clock/adapter-timer provisioning with constructor migration. Digest the complete
      Interfaces section, including supporting-type recovery/readback signatures, immutable batch
      receipts, Image Optimizer mode matrix and ownership rules, not just the main trait block.
      Include batch producer constructors, retained facade constructors, request option/readback
      APIs and effective marker inspection, and the complete explicit-follow policy/result/error
      surface. Include the Fastly-local observation handle, immutable snapshot/getters, extension
      access/current-scope binding and lifetime/counting semantics for blocking EZ-2.13; D1b cannot
      invent a different public getter after this freeze. The freeze covers blocking APIs and the
      shared producer contract; public follow-up extensions require their own attributed review
      before implementation, not an implicit change
      to this digest or compatibility defaults.
      Include all implementer and caller files; facade-only preparation is intentional and external
      integration tests must not require an unchecked prepared-batch constructor. Record Approved,
      Rejected, or Pending with reviewer/date
      and the approved interface digest. An implementer cannot self-approve by checking this box.
- [ ] The EdgeZero and TS maintainers approve the literal integer B, fixed transport-settings
      scope, complete budget domain and rounding/cardinality argument from G2, or record why that
      Fastly policy is rejected/pending. Link raw evidence by hash and exact source/SDK identities.
      No number has been approved by this plan; lack of an approved number blocks D1.
- [ ] Separately approve the revised EZ-8.10 contract: original t0/D/source, finite numerical H(K)
      rule and literal maximum/domain, and proposed 5 ms observation tolerance. Bind approval to the
      source/SDK/policy digests before testing; reject a horizon justified only by an observed maximum,
      runtime termination or a two-timer sum. No H(K) value is approved or proven by this plan.
      Image Optimizer integration stops without this approval; production remains blocked without
      hosted proof. This does not widen ordinary outbound deadlines or permit late Ok responses.
- [ ] Record decisions on EZ-24 promotion (Follow-up by default), EZ-8 entitlement availability
      (Blocking remains), and M1/M2/M5/M7 sample/threshold definitions. Record zero-baseline and
      inconclusive-control rules before replay. M3's count invariant is separate from its report-only
      timing distribution; M4 reuse-mode capacity characterization stays deferred.
- [ ] Emit separate API and Fastly-policy decisions. P1 starts only with the approved API digest;
      D1 additionally requires passing local G2 proof and an approved literal B; its Image Optimizer
      integration also requires the separately approved EZ-8.10 horizon contract. Hosted pending
      status is not production approval. A rejected primitive cancels its dependent implementation,
      not the requirement; resumption needs a new report and approval. Review any negative result
      with maintainers before changing an SDK pin or the blocking set.

Expected: a signed/attributed decision record with no implicit approvals. No runtime API or
capability changes are made by this task. P1 must stop rather than invent an alternate signature
if its approval is absent or stale. This is the owning task for the index's API approval checkbox.

## Phase Exit

- [ ] Run `node --test scripts/check_outbound_cutover_evidence.test.mjs` and the checker's
      manifest-validation invocation documented by G1; Expected: nonzero passing tests and a valid
      case/owner/command inventory, with pending external results still visibly pending. Use G2/G3's
      exact fixture commands above for feasibility. Command success does not supply G4 approval.
- [ ] Inventory covers all requirements without promoting deferred scope.
- [ ] G2 records passing proof or an explicit negative/pending disposition. Only passing local
      proof plus G4's Fastly policy approval enables D1; a negative Fastly result does not prohibit
      separately approved portable or other-adapter work. Hosted proof passes or visibly blocks
      the production pin.
- [ ] G4 records API approval separately from the Fastly feasibility/policy decision. Missing API
      approval stops P1; missing literal backend-policy bound stops D1. Missing H(K) approval
      stops Image Optimizer integration, not separately approved ordinary multiplexer work.
- [ ] Executor and known-empty integration assumptions are recorded for phase 3.

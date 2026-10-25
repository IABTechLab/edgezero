# Outbound Follow-Up Contracts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the remaining adoption requirements in scoped, separately verifiable changes without silently adding them to the production blocking set.

**Architecture:** Extend the existing request policies, body readers, application assembly, and egress coordinator. Reuse phase 2's head-first boundary and phase 3's owned progress engines. Keep response decoration/failure rendering bounded and keep provider diagnostics out of wire errors.

**Tech Stack:** Existing core/adapters/macros, portable timers, Fastly lifecycle and response ABI, native Axum local server, existing test utilities and fixture workspace.

---

Dependencies are task-specific. Scheduling a task here does not approve its promotion. When a part
lands, record confirmed-present parts and their environment legs under EZ-Q1.6/Q1.8. Conditional
SHOULD work, JS targets, and reusable-sandbox behavior retain their explicit applicability.

Task numbers group ownership, not an instruction to ignore dependencies. Use the child-specific
prerequisites below: ordinary cache work needs D1b, while callback/egress ownership integration
needs D1a, not D1d's separately approved Image Optimizer work. Diagnostic types and independent
application policies may land before their adapter integration. For lifecycle integration execute
F8 (KV) and F9a (assembly) before F6b's final custom-send report tests; F9b is not a prerequisite.
F10 requires the published P6 utility and includes
F4/F5 cases only when those parts are adopted. Every task retains a buildable hard-cut boundary.

## Explicit Requirement Ownership

Every row's full normative range and acceptance cases belong to the named substep. G1 adds an
individual sentinel/command/assertion/environment/artifact entry for every applicable case; a
single test of the first requirement in a grouped task cannot close the later requirements.

| Requirement     | Owning Substep | Required Acceptance Coverage                                                                  |
| --------------- | -------------- | --------------------------------------------------------------------------------------------- |
| EZ-10           | F2             | All request readers/extractors, sticky/early errors, every upload mode, renderer and fallback |
| EZ-11           | F3a            | Tags, validation/deduplication, freshness/no-store/private, mode and adapter matrix           |
| EZ-12           | F3b            | TLS policy/readback, trusted-chain wrong-name, transport-key isolation, HTTP no-op            |
| EZ-13           | F3c            | Spin one-authority evidence, preflight Unsupported and documented config rejection            |
| EZ-14           | F4             | All idle/absolute-deadline cases, encoded progress, paused application and classification     |
| EZ-15           | F5             | Explicit ownership, 64 KiB response ceiling, capacity recovery and provider lifetime          |
| EZ-16           | F6a            | Every typed provider detail path, bounded diagnostics, retained errors and wire invariance    |
| EZ-17           | F6b            | Attempt/commit/body/state report, no-attempt categories, native replacement and Abort         |
| EZ-18           | F7a            | Buffered/bodyless/streamed framing plus SDK and host-compression parity                       |
| EZ-19           | F7b            | Bounded app/dynamic synthesized headers on detached, error and fallback responses             |
| EZ-20           | F8             | Declared failure handles, require_kv, all KV operations and recovery without retention        |
| EZ-21           | F6c            | All six framing reasons across four adapters and routed/detached/fallback failure paths       |
| EZ-22           | F9a            | Fallible routes/configure, typed retained state, retry and all selected entrypoint cases      |
| EZ-23           | F9b            | All 13 policy-aware Axum server/TCP testing acceptance criteria                               |
| EZ-24 remaining | F10            | Adopted cancellation, JS and generator parts only                                             |

After each implementation increment run scoped `cargo test`, root `cargo fmt --all -- --check`,
and scoped all-target/all-feature clippy from the index. Native success never replaces the
corresponding adapter-target checks. Expected: real named cases execute, then zero lint warnings.

## Task F1: Reusable Fastly Backend Retirement

**Files:** Fastly `src/outbound/backend.rs` from D1, `src/lifecycle.rs`, `src/lib.rs`,
`src/request.rs`, and `tests/contract.rs`; fixture Fastly reusable entrypoints.

- [ ] Add `backend_retirement_rejects_two_hundred_and_first_configuration`; run
      `scripts/run_test_nonzero.sh backend_retirement_rejects_two_hundred_and_first_configuration cargo test --offline --locked -p edgezero-adapter-fastly --no-default-features --features test-utils --lib`.
      Expected: red for missing capacity/retirement policy, then the 201st configuration sends
      nothing, emits typed 503, retires after the current request and retains request count.
      Repeat real WASI/reuse cases below and apply the task lint card; single-request adoption
      does not depend on a deferred reuse leg.

- [ ] Add registry-growth tests across clients/callbacks, equal origins under jitter, and the
      200-configurations-plus-one case. Fail before a blocking 201st registration with 503 and no send.
- [ ] Implement threshold/default and application-requested retirement after the current request.
      Preserve serve-summary request count, distinguish retirement from error, and do not emit a
      synthetic SDK error response. Keep request exchange state outside retained backend metadata.
- [ ] Test cold-registration failure recovery, successful late registration reuse, and abandoned
      exchanges across six Viceroy requests and the separate hosted 1,001-request case. Never let an
      old request's result enter a new request's batch.
- [ ] Run Fastly native/WASI contracts and reuse fixture legs. These results gate reuse enablement,
      not single-request adoption. Suggested commit: `feat: bound reusable backend registration growth`.

## Task F2: Typed Request Overflow and Reader Precedence

**Files:** Core `src/error.rs`, `body.rs`, `context.rs`, `extractor.rs`, `app.rs`; four adapter
`src/outbound.rs`; demo `crates/app-demo-core/src/lib.rs`; CLI template `core/src/lib.rs.hbs`.

- [ ] Add cap-minus-one/exact/plus-one cases for every reader/extractor/upload; distinguish host
      framing rejection from host-delivered bytes. Test sticky consumed overflow, reusable early/cached
      cap rejection, already-expired inbound deadline, and unchanged malformed JSON/form errors.
- [x] Add a stable 413 category and typed inbound/upload origin, preserving public diagnostics and
      sticky body state. Early normalized Content-Length refusal must not poison a later larger-cap
      read; 408 still wins if already expired. Update explicit demo/template fallback overflow responses.
- [ ] Reject every buffered over-cap upload before dispatch. Keep Axum/Cloudflare bounded
      prebuffering; Fastly/Spin may send a partial upload but must abort it, never complete a truncated
      request. Preserve ordinary upstream 413 responses and their Retry-After; generated cap errors
      carry none even after custom rendering and synthesized-header composition. Keep an internal
      typed overflow-origin marker through those stages rather than classifying by status alone.
      The suppression takes precedence over F7's header decoration, never over a config-503 retry policy.
- [ ] Run `cargo test -p edgezero-core`, four adapter contracts, and real ingress/pipelining cases.
      Add hosted HTTP/2 overflow evidence where required. Suggested commit: `feat: classify request overflow as payload too large`.

### F2 Implementation Checkpoint (2026-10-21)

- [x] Implement `PayloadTooLarge { origin }` and `RequestBodyOverflowOrigin::{Inbound,
  OutboundUpload}` with fixed public text, typed access and sticky replay. Keep malformed
      JSON/form at 400, admitted expiry at 408, and strict cached-cap rejection reusable.
- [x] Refuse an oversized single normalized Content-Length without source polling or poisoning;
      match admission's whitespace normalization and let an already-expired admitted deadline win.
      Preserve source-error origins through uploads.
- [x] Reject oversized buffered uploads in shared preflight on all four adapters. Replace the
      adapters' own streamed-cap errors without changing their existing prebuffer/abort lifecycles.
- [x] Keep a private overflow-origin marker through default/custom rendering and final egress
      preparation. Suppress retry headers only for that typed cause; preserve config-503 and
      unmarked upstream-413 retry policies. The original error overrides any marker returned by
      a custom renderer, including when restoring config retry policy. Demo/scaffold fallback
      defaults now select 413.
- [x] Observe red/green core, Axum, Cloudflare Node/WASM and Fastly Viceroy/WASI tests. Exercise
      cap-minus-one/exact/plus-one readers, extractors, preflight and upload seams. Verify Spin's
      native exchange arbitration preserves the typed overflow. These are unit/SDK-seam results,
      not actual hosted ingress/upload qualification.
- [x] Run actual Axum chunked ingress plus a following pipelined request and custom renderer;
      prove 413/no retry and correct reuse or connection close. Prove no origin contact for local
      buffered/streamed overflow, and an ordinary upstream 413 retaining `Retry-After: 120`.
- [x] Pass all five root Rust gates (793 core tests), all three real-target WASM clippy checks,
      30 Cloudflare Node/WASM library tests, the targeted Fastly Viceroy upload test, demo and
      generator tests, docs format/lint/build, and the legacy/docs/dependency/evidence guards.
      The workspace TCP suite requires loopback permission; its approved rerun passes.
- [x] Add the isolated `outbound-overflow` suite through all four actual local entrypoints,
      with ten independently attempted cases each, bounded TCP origin receipts and no retries.
      Axum, Viceroy and Spin pass all ten. Fastly/Spin streamed overflow closes a TCP connection
      before any HTTP head bytes; retain that explicit witness, not a claim of zero contact.
- [x] Reject malformed/truncated metadata, missing head-state evidence, duplicate connections,
      clean truncated uploads and collector errors in the evidence validator. Observe regression
      failures before the fixes; 51 harness and 17 core fixture tests pass afterward.
- [x] Append the exact 2048-byte chunked POST against a 1024-byte cap after the original ten-case
      sequence. Both expanded local runs execute eleven cases per target: Axum, Viceroy and Spin
      pass eleven; Cloudflare passes the exact-size case but retains the original next-POST failure.
      Preserve the old failed evidence. Extend Axum's TCP pipeline test to cover both its original
      9/8-byte boundary and the exact 2048/1024-byte case. A separate built-in `Json` extractor TCP
      test exceeds its existing 8 MiB cap by one byte, reaches custom rendering as typed inbound
      overflow, and returns 413 without Retry-After. Ordinary JSON is constructed with `json!`.
- [x] Prove collector Drop signals stop and waits for a gated worker, then releases and joins
      cleanly before assertions. Removing the join makes the new regression fail; restoring it
      passes. This is SDK-owned harness cleanup, not a claim about provider resources.
- [ ] Close the local Cloudflare recovery failure: three matrix runs return the ingress 413
      followed by a proxy-generated 500 before the next request enters the fixture; the origin
      receives no connection. An isolated JavaScript-only cancellation control reproduces the
      follow-on failure without EdgeZero/Rust/WASM. This local runtime boundary is not repaired,
      waived, silently retried or presented as hosted evidence. The precise native mechanism
      remains unproven. Retain the failed runs and control inputs/results in the
      [evidence record](../../audits/evidence/2026-10-21-outbound-overflow.json).
      Subsequent expanded runs retain that failure. Controlled front-proxy requests fail after
      cancellation both with and without `Connection: close`; direct requests to Wrangler's own
      user-worker listener recover for both JavaScript-only and actual EdgeZero artifacts.
      This narrows the observed boundary, not the native cause. The direct listener is diagnostic
      only and cannot replace the standard gate. Retain inputs, failed diagnostic setup attempts
      and results in the [boundary record](../../audits/evidence/2026-10-21-overflow-boundaries.json).
- [x] Verify the generated-app and demo TCP proof through their unchanged standard Axum
      entrypoints. The generated proof reuses the existing ignored workspace test, discovers
      the native executable from Cargo JSON and canonicalizes manifest aliases. A shared
      bounded client checks fixed/chunked 4095/4096/4097-byte bodies on both 404/405 paths,
      exact 413/no-retry output, 405 Allow, the five-second pending-read 408, and a body tail
      sent only after receiving the complete 413, followed by genuine health/recovery requests.
      Response completion is sampled separately from socket closure. Parser failures and
      child-process exit cannot count as success. Both applications pass all 16 observations;
      the generated workspace also passes its 22 core tests, strict host clippy and three WASM
      compilation checks. Retain the first path-alias failure, successful raw logs, generated
      source archive and native binaries in the [evidence record](../../audits/evidence/2026-10-21-generated-fallback.json).
      This closes EZ-10's local generated/demo fallback leg, not F2 overall or hosted qualification.
- [ ] Complete hosted Fastly HTTP/2, TS end-to-end and later F7b composition qualification.
      Until those environment legs land, F2 and EZ-10 overall remain incomplete. Its implemented
      parts are confirmed present, not silently promoted; no other Follow-up becomes blocking.

Decision log: independent upload and rendering investigations selected shared buffered preflight
plus localized cap-error changes, rather than rewriting provider transports. Separate SDK/runtime
investigations confirmed Fastly readiness still loses source failures, including through
`is_ready`; the failed G2 gate and cancelled dependent driver migration are unchanged.
Independent harness and platform reviews additionally selected strict framing/correlation
validation and a provider-only diagnostic control over transport changes or recovery workarounds.
The local upload suite observes streamed response heads only and drops their bodies unpolled.
The generator and application-lifecycle investigations selected the existing build gate plus
one shared test-only HTTP parser/client rather than another generated application or custom
server. `httparse = 1.10.1` is a CLI dev dependency only; no normal dependency or audited SDK/
decoder requirement changes. Existing CI steps execute the new checks without adding job names.
The demo step requires the fallback sentinel and forwards `--include-ignored` to both test
listing and execution, preventing a later ignore annotation from silently skipping the proof.
Seven guard regressions cover all selection modes, missing/zero tests and failure propagation.
Independent acceptance and platform investigations selected the appended exact-size probe and
gated Drop test over changing transports or SDK versions. The topology control reads only the
generated loopback user-worker address from the private local runtime log; no proxy secret is
published. Omitting a connection header, bypassing the proxy, delaying recovery, draining bodies
or replaying POST is not adopted as a repair. Fastly G2 and the approved Interfaces digest remain
unchanged.

The boundary checkpoint passes all five root Rust gates, 53 harness and 17 qualification-enabled
core fixture tests, fixture clippy, 14 Axum connection tests and docs format/lint/build. The 25
nonzero/evidence guard tests and dependency/docs/legacy checks pass; the inventory remains
unqualified. Independent acceptance, quality and evidence reviews found no remaining scoped
defect. Both expanded local runtime matrices deliberately exit 1 for Cloudflare recovery.

Generated/demo TCP commands (loopback permission required; generated dependencies must be cached
for offline execution):

```sh
CARGO_NET_OFFLINE=true scripts/run_test_nonzero.sh --ignored generated_workspace_compiles cargo test --offline --locked -p edgezero-cli --test generated_project_builds
scripts/run_test_nonzero.sh --include-ignored demo_fallbacks_enforce_body_cap_over_tcp cargo test --offline --locked -p edgezero-cli --features demo-example --test demo_logging
```

## F3: Cache, TLS and Authority Task Group

F3a-F3c are independently closable tasks, not inline labels. None closes another requirement.

### Task F3a: Cache Tags

**Files:** Core `src/outbound.rs` and `outbound/image_optimizer.rs`; Fastly `src/outbound.rs`;
four adapter preflight/capability modules and `tests/contract.rs`; cache qualification fixture.

**Dependencies:** P1/P3/P5 and ordinary Fastly cache integration D1b (including D1a); D2/D3/D4
for their respective mode-parity legs. No D1c/D1d prerequisite. F5 parity applies only when adopted.

- [ ] Add typed tag validation/deduplication and cache behavior tests. Empty/no-tags behavior,
      bypass combinations, and Image Optimizer conflicts must follow EZ-11/EZ-8. Never send tags as
      origin headers. Preserve default freshness and negative private/no-store caching behavior.
- [ ] Implement Fastly tagging with the supported SDK cache interface. Axum/Spin accept tags as
      documented no-ops; Cloudflare rejects before dispatch. Run send/begin/batch/redirect and later
      fire-and-forget cases, with actual cache probes where the requirement names them.
- [ ] Name the shared rejection case `cache_tags_preserve_mode_and_option_precedence` and run
      `scripts/run_test_nonzero.sh cache_tags_preserve_mode_and_option_precedence cargo test -p edgezero-core --features test-utils`
      before/after implementation. Expected red for missing policy, then exact effective tags,
      rejections and sibling isolation pass. Run each adapter contract and required hosted cache
      probes, then the task lint card. Suggested commit: `feat: expose portable outbound cache tags`.

### Task F3b: Fastly TLS Name-Check Policy

**Files:** Core `src/outbound.rs`; Fastly `src/outbound.rs`/`outbound/backend.rs`;
four adapter preflight/capability modules and contracts; controlled TLS qualification fixture.

**Dependencies:** P1/P5 and D1b; independent of F3a.

- [ ] Add default strict and Fastly-only chain-valid/name-unchecked TLS modes, with typed getter
      and parts round-trip. Key backend reuse by TLS settings. Keep HTTP no-op semantics and reject
      unsupported adapters without disabling verification globally. Test a trusted-chain/wrong-host
      fixture, not an unauthenticated internet endpoint.
- [ ] Add `tls_name_policy_isolates_backend_identity`; run it before/after implementation via
      `scripts/run_test_nonzero.sh tls_name_policy_isolates_backend_identity cargo test -p edgezero-adapter-fastly --features test-utils`.
      Expected red for missing policy, then strict/no-name keys differ and chain validation remains.
      Execute the actual trusted-chain/wrong-name TLS fixture and other adapters' preflight controls;
      apply the task lint card. Suggested commit: `feat: add explicit Fastly TLS name-check policy`.

### Task F3c: Spin Authority Limitation Evidence

**Files:** Spin `src/outbound.rs`, `tests/contract.rs`, `tests/sdk_resources.rs`;
fixture Spin entrypoint and `docs/guide/adapters/spin.md`/`capabilities.md`.

**Dependencies:** P1's support hook and D4; independent of F3a/F3b.

- [ ] Keep Spin authority override Unsupported. Record the one-routing-and-Host authority proof
      from pinned WASI 0.3 and a Spin fixture; document the final limitation rather than adding a shim.
- [ ] Add `spin_authority_override_rejects_before_pending_body`; run it with
      `scripts/run_test_nonzero.sh spin_authority_override_rejects_before_pending_body cargo test -p edgezero-adapter-spin --no-default-features --features test-utils`.
      Expected: zero body polls, timer registrations and provider sends, including under expired
      budget. Existing Unsupported cases may already pass; the new ordering assertion is the red/green
      target. Repeat actual SDK/Spin fixture legs from D4 and apply the task lint card.
      Suggested commit: `docs: qualify Spin authority override limitation`.

## Task F4: Stream Idle Policy and Deadline-First Classification

**Files:** Core `src/outbound.rs`, `outbound/progress.rs`, `time.rs`, `error.rs`; four adapters'
raw response streams and request timeout setup; `outbound/testing.rs` if published.

- [ ] Add `stream_idle_keeps_absolute_deadline_precedence`; run
      `scripts/run_test_nonzero.sh stream_idle_keeps_absolute_deadline_precedence cargo test -p edgezero-core --features test-utils --lib`.
      Expected: red for missing idle mode, then absolute-only, idle-only, equality and late samples
      retain the required source, and only nonempty encoded progress resets idle waiting. Repeat
      the named controls in native/SDK contracts with the index/Q2 cards and actual slow-origin
      legs from EZ-14; apply the task lint card. A unit pass does not upgrade provider support.

- [ ] Add zero/invalid-mode tests, getters and parts round-trip. Add a classifier truth table:
      idle only -> idle cause; absolute only -> RequestDeadline; both expired or equal -> RequestDeadline.
      In unit clocks, a read at 600 ms with idle 400 ms/deadline 1,000 ms must choose the deadline;
      delayed platform failure after both limits must do likewise. No timer extends another budget.
- [ ] Keep the original aggregate budget through headers. In opted-in streamed body mode remove
      per-call/default body timeout, retain independent application/chain absolute deadlines, and start
      idle waiting only when more nonempty encoded input is needed. Reset on encoded progress, not
      decoded output or empty chunks; application pauses do not consume idle time.
- [ ] Integrate per-adapter raw-read races and safe platform timer settings. Remove reqwest's
      body-inclusive total timeout only for idle mode, replacing header/absolute enforcement rather
      than dropping it. Decoder/rechunk buffering may span idle intervals without false expiry or
      invented empty output. Default streamed behavior remains unchanged.
- [ ] Run core/adapter classifier tests and EZ-14 acceptance 1-11, including gzip empty DEFLATE
      progress, slow reader, longer-than-call-timeout idle setting, and source release. Hosted Fastly
      and CF/Spin cancellation limits stay visible. Update scripts to model header/body budgets
      separately. Suggested commit: `feat: bound streamed bodies by idle time and absolute deadline`.

## Task F5: Explicit Fire-and-Forget Ownership

**Files:** Core `src/outbound.rs`; four adapter `src/outbound.rs` and lifecycle/entrypoint modules;
fixture-core qualification cases and adapter fixtures.

- [ ] Add `fire_and_forget_capacity_recovers_after_every_terminal_path`; run
      `scripts/run_test_nonzero.sh fire_and_forget_capacity_recovers_after_every_terminal_path cargo test -p edgezero-core --features test-utils --lib`.
      Expected: red for missing explicit ownership, then capacity two rejects a third send without
      dispatch, every terminal path releases once and the independent 64 KiB discard ceiling holds.
      Repeat the named accounting test via native/SDK cards and G1's callback/reuse runtime legs;
      deferred provider lifetime evidence remains pending. Apply the task lint card.

- [ ] Add dispatch-without-observation cases and a per-instance capacity-two test across callbacks.
      Saturation rejects before send; capacity recovers after completion/failure/expiry exactly once.
      Image Optimizer and redirect following remain forbidden; all allowed request options propagate.
- [ ] Add a dedicated explicit operation and bounded owner/capacity accounting, separate from
      dropping an ordinary request. Enforce EZ-15.5's independent 64 KiB response-retention and
      discard-read ceiling in addition to tighter request caps and deadlines. Stop reading and
      release the response when that ceiling is reached, even if a request permits a larger or
      unlimited response. Keep provider-specific post-callback lifetime exactly as EZ-15 permits.
- [ ] Test callback end, reused-instance cross-request work, cancellation, body errors and permanently
      pending sources. A completion must never be delivered into another request's batch. Do not
      publish a background lifetime guarantee inferred from local handle ownership alone. Test a
      response larger than 64 KiB under looser request caps: retention and discard reads remain
      bounded, response ownership is released promptly, and capacity recovers exactly once.
- [ ] Run core, adapter, and EZ-15 applicable fixture legs. Document the operation's dispatch
      receipt/error semantics and explicit limitations. Suggested commit: `feat: add bounded fire-and-forget outbound ownership`.

## F6: Diagnostics and Delivery Task Group

Execute F6a before F6b/F6c. Their acceptance and commits are independent; F6b's final lifecycle
tests wait for F8/F9a, not for unrelated F9b TCP helpers.

### Task F6a: Log-Only Provider Diagnostics

**Files:** Core `src/error.rs`, `context.rs`, `response_egress.rs`;
four adapter error/fallback paths and contracts.

**Dependencies:** P1 and existing typed failure boundaries; D1a/D2/D3/D4 only for integration at
the boundaries they change. Diagnostic types can land earlier. No D1b-D1d or F8/F9a prerequisite.

#### F6a Foundation Checkpoint (2026-10-21)

- [x] Add the text-free, fixed-size `ProviderFailureDetail` catalogs and safe numeric payloads
      for the pinned SDKs. Adapter identity and cause share one enum; arbitrary provider strings
      cannot be stored. Spin's documented six-value DNS allowlist projects other text to Unknown.
- [x] Attach optional detail directly to gateway, timeout and internal errors; retain their
      existing Display/message/status/kind/reason/wire contracts and original internal source.
      Hard-migrate exact field patterns, without an error wrapper or compatibility variant.
- [x] Demonstrate the retained-body diagnostic-loss regression, then preserve detail during sticky
      error reconstruction. Cover codec/limit/rechunk/collection, batch partial failures and owned
      pending readiness without changing existing deadline precedence or transport behavior.
- [ ] Populate provider boundaries and observers, add actual SDK/token-bearing adapter regression
      fixtures, then run the remaining full F6a acceptance legs and capability/evidence work below.

The first three items are an independent core foundation, not completion of F6a/EZ-16. No adapter
mapper or diagnostic capability is changed. Context replay retains the existing stringified
internal source behavior; direct body/pending paths retain the original concrete error. Full P1
hard migration, failed G2, F6c framing detail and hosted qualification remain separate gates.

Checkpoint validation passed all five required Rust gates (27 workspace binaries, 2,103 tests,
including 800 core tests), four compile-fail doctests, three actual-target WASM clippy checks,
and native outbound contracts on all adapters. Viceroy passed 25 adapter contracts plus four
core diagnostic tests; pinned Wasmtime 48.0.2 passed 35 Spin contracts. Cloudflare passed 32
Node/WASM unit tests. Its browser contract attempts failed before test execution during local
Safari/Firefox WebDriver initialization; that coverage is unverified, not a passing or hosted leg.
Initial Fastly/Spin invocations lacked explicit runners; the correctly configured reruns above
passed. Demo-core and reusable-fixture-core build checks, docs format/lint/build, dependency and
legacy guards passed. The inventory remains 255 criteria / 511 planned legs, `qualified: false`.

Independent SDK-catalog and core-flow investigations/proposals selected optional fields on the
original variants, not a wrapper or a reconstructed pipeline error. Spec/quality reviews found no
remaining foundation defect; their explicit getter assertions and outbound-collector coverage
improvements were adopted. No SDK/dependency pins or capability claims changed.

#### F6a SDK Boundary Checkpoint (2026-10-21)

- [x] Project all 31 known pinned Fastly send causes and nine backend causes, including optional
      numeric statuses/codes and I/O kinds, without changing the existing classification tables.
      Dispatch initiation and pending-header completion are the same Send operation: no response
      head is available on failure. Upload writes/flush/finish and body reads carry separate phases.
- [x] Project all 39 Spin ErrorCode variants without retaining DNS text, TLS messages, field names,
      coding strings or internal descriptions. Distinguish send and request-completion callback
      phases without changing exchange arbitration, polling or drop order.
- [x] Extend BadRequest with optional detail and sticky replay for the existing host-rejection
      cases. Migrate exact patterns and the Spin test seam without compatibility wrappers.
- [x] Populate Axum reqwest predicate/I/O-source evidence and Cloudflare closed exception-name
      evidence at the existing send/body boundaries. Preserve original upload-source errors.
- [x] Demonstrate and repair metadata loss during late backend/upload/pending results, adapter
      post-ready checks, Cloudflare host-event/205 arbitration and shared bounded collection.
      Keep selected timeout text/cause and every existing clock check; late Ok/EOF and timer-only
      outcomes carry no invented detail. Spin uses an explicit private result projector in its
      existing race helper, not a second transport or a new public client contract.
- [ ] Finish preparation/header-option enrichment, observer/framing integration and diagnostic
      capability publication with the remaining F6a/F6c acceptance and hosted evidence.

Local SDK-value/Node/WASI fixtures are not hosted cause provenance. Standalone Spin library tests
cannot link the Spin KV interface in plain Wasmtime; the executable SDK resource binary exercises
the actual deadline helper through a test-only seam instead. Browser coverage remains unverified
after the previous WebDriver failures; no new browser/hosted success or G2 waiver is claimed.

Validation passed the five required Rust gates (801 core tests; 27 workspace binaries and 2,108
tests), core doctests, all three actual-target WASM clippy checks, and all four native outbound
contract suites. Local Viceroy passed 171 adapter library tests and 25 contracts; pinned Wasmtime
passed 35 Spin contracts and nine SDK resource tests; Cloudflare Node/WASM passed 37 library tests.
The final Axum ordering cleanup was followed by its full unit/contract suite and workspace clippy.
Docs format/lint/build, dependency/legacy guards and the 18 evidence-checker tests passed; inventory
validation still reports 255 criteria / 511 planned legs, `qualified: false`. No dependency pin or
capability changed.

Independent SDK and Axum/Cloudflare reviews found and closed the timeout-replacement losses,
including the shared collector; classification now precedes additional Axum diagnostic inspection.
Independent demo-core (48 tests) and qualification-enabled fixture-core (17 tests) checks passed.
Remaining nonblocking test gaps include signed Fastly status extrema and explicit Spin timer-won/
raw-trailer projection cases. The refused-port fixture retains a small port-reuse risk: a bound
non-listening socket timed out on macOS, and port zero returned AddrNotAvailable, so neither
proposed replacement can prove the required ConnectionRefused witness. This is a local harness
limitation, not a transport behavior or hosted-qualification claim.

#### F6a Preparation Checkpoint (2026-10-21)

- [x] Record Cloudflare request-header and constructor errors without replacing the retained
      worker error. Keep SDK-erased constructor types Unknown; project caught raw Fetch setup
      exceptions while retaining their exact fixed internal source. Do not enrich local UTF-8
      validation, nonthrowing runtime refusals or the shared response-header sink.
- [x] Record all five pinned Spin HeaderError variants, timeout-option identity and rejection
      category, and component-specific unit setter rejection. Preserve 400/500 classification,
      fixed messages, advisory NotSupported success and existing preparation/dispatch order.
- [x] Introduce RequestPreparation for these SDK resource boundaries. Do not infer transport
      progress from phase names or relabel errors observed at existing SDK send/body boundaries.
      Preserve the outbound contract, dependency pins, approved API digest and failed Fastly G2.
- [x] Finish independent review and current-tree Rust/WASM/docs verification before committing.
- [x] Repair the feature-matrix-discovered Fastly lifecycle helper gate: co-located tests can use
      it without enabling test-utils. Keep the unit test active; production builds are unchanged.
- [ ] Complete observer/framing integration, diagnostic capability publication and the remaining
      provider/hosted acceptance legs. Preparation fixtures do not close F6a/EZ-16 by themselves.

Dirac investigated Cloudflare source preservation and its shared request/response header sink;
Banach verified the actual Spin SDK's five header variants and unit-returning setter provenance.
Their proposals favored existing-boundary enrichment over a new request construction path or
transport-error remapping. The selected design uses a distinct preparation phase, retains source
objects where they already existed, and never reconstructs a missing cause from provider text.
Implementation is centralized. No application-specific policy or compatibility alias is added.

Cloudflare fixtures use actual SDK header/constructor failures and scoped, restored Node globals
for setup exceptions and a nonthrowing refusal control. Spin exercises an actual forbidden field
through the production field-construction helper; normal dispatch strips that field beforehand,
so this is not an end-to-end public-client failure claim. Synthetic enum/unit outcomes cover
otherwise unforced setter branches, including secret-bearing Other payloads. They are projection
tests, not hosted cause or runtime-limit qualification. Tests assert exact existing public error
behavior, typed detail and its text-free representation. Local validation and advisory success
controls remain separate from provider failures.

Spec review passed. Quality review tightened Cloudflare's setup fixtures so assertions run only
after global restoration and added a direct retained-source identity/payload witness; the revised
review passed. No panic-abort or trap cleanup guarantee is inferred from the test guard.

Validation passed all five required Rust gates (801 core tests; 27 workspace binaries and 2,108
tests), core doctests and the nonzero public-error diagnostic sentinel, all three actual-target
WASM clippy checks, and the four native outbound contract suites (17/4/2/3 tests). Cloudflare's
Node/WASM suite passed 40 tests; pinned Wasmtime passed 35 Spin contracts and 12 SDK resource tests;
Viceroy passed 171 Fastly library tests and 25 contracts. The plain-fastly lifecycle sentinel also
passed without test-utils after reproducing its missing-helper compile error. All seven adapter
feature matrices passed eight legs each. Independent demo-core tests passed 48 cases. Docs
format/lint/build, dependency/legacy guards and all 18 evidence-checker tests passed. Inventory
validation remains 255 criteria / 511 planned legs, qualified false. No dependency pin, capability,
frozen API file or approved Interfaces digest changed. Hosted/browser acceptance, full P1 and
the failed Fastly G2 remain open.

#### F6a Source-Observer Checkpoint (2026-10-21)

- [x] Retain a fixed-size, text-free first-observed body-source failure in the terminal report.
      Copy the existing category and optional provider detail; preserve observation-time body kind
      through fallback. Missing detail is meaningful and later observations cannot replace it.
- [x] Wire all four converters before post-ready deadline/cancellation arbitration, including
      Axum's zero-length validation and final EOF lookahead and the existing Cloudflare/Spin
      source-read races. Preserve polling, clocks, terminal precedence, bytes and resource ownership.
- [x] Deliver the same snapshot to completion and observer; reject post-terminal recording.
      Keep rendered application errors distinct from body-source errors and invent no provider
      evidence for EOF, timer-only, conversion-only or transport-only failures.
- [x] Add native converter regressions, core state/clock controls, real Cloudflare Node/WASM I/O
      checks, and a Spin SDK async HTTP check in the existing local progress fixture.
- [x] Finish independent quality review and broad current-tree Rust/WASM/docs verification before
      committing this checkpoint.
- [ ] Complete typed framing reasons, diagnostic capability publication, full F6a acceptance and
      hosted qualification. This checkpoint does not complete F6a/EZ-16, P1, or the failed Fastly G2.

Turing reviewed core state/report ownership and Axum; Heisenberg reviewed converter boundaries
and the real SDK test paths. Their proposals favored one bounded observation on the existing
attempt over wrapping streams, retaining source errors or changing delivery-error types.
Implementation is centralized. The report field is a hard cut, not a compatibility wrapper.
No production application renderer changes: its existing callback already receives the original
error, and delivering that rendered response successfully remains a successful delivery.

Native regressions first failed on missing source observations, then passed after wiring. Controls
cover detail present/absent, first-None/later-Some, fallback attribution, no recording-time clock or
callback activity, drop classification, terminal rejection and successful rendered-error delivery.
Cloudflare's actual source race covers pre-expired/pre-aborted, EOF, ready error and ready
error followed by deadline equality or cancellation. Its existing scoped Node timer shim is
shared rather than duplicated. Spin's synchronous WASI test export cannot wait on SDK timers:
both generic and SDK executor attempts are retained as failed harness diagnostics, not evidence.
The check instead runs directly under the existing async HTTP export with eight cases on local
Spin 4.1.0, without timer substitutions. Neither local runtime establishes hosted client delivery.

The actual Cloudflare/Spin races failed when only their inner observation hooks were temporarily
disabled, then passed after restoration; recording only in the outer pump loses those ready
errors. Local Spin progress evidence is retained under `.runs/18e0bf43a59d4170-f275-0`, including
the eight-case receipt, all four passing progress groups and no pre-shutdown worker panic.
The mutation failure remains under `.runs/18e0bf38c867ad68-eb61-0` with its missing-observation
diagnostic; neither failed run is rewritten as passing. These are local characterization runs,
not the frozen acceptance inventory or hosted qualification.

Independent spec and quality reviews passed after tightening exact provider assertions and adding
terminal-without-observation and successful-rendered-error controls. All five Rust gates passed:
27 workspace binaries, 2,116 tests, zero failures and one existing ignored test, including 804 core
tests. All three actual-target WASM clippy checks passed. Node/WASM passed 41 Cloudflare tests;
Viceroy passed 172 Fastly library tests and 25 contracts; pinned Wasmtime passed 35 Spin contracts
and 12 SDK resource tests. The four native outbound contract suites passed 17/4/2/3 tests. Core
doctests, the nonzero public-error sentinel, all seven eight-leg feature matrices, 48 demo-core
tests, 56 fixture-harness tests, fixture clippy and docs format/lint/build passed. Dependency and
legacy guards and all 18 evidence-checker tests passed. Inventory validation remains 255 criteria /
511 planned legs, qualified false. SDK/decoder pins, frozen policies, the approved Interfaces digest
and capability claims are unchanged. Typed framing reasons, full P1/F6a acceptance, browser/hosted
evidence and the failed Fastly G2 remain open.

- [ ] Add token-bearing URLs/provider messages to regression fixtures. Assert wire bytes and
      existing Display/status/reasons do not change; retained body failures preserve diagnostic detail
      internally. Add framing-reason cases independent of provider text and policy-panic/deadline cases.
- [ ] Carry bounded typed provider detail, safe numeric codes, and framing reason to logging/
      observer paths without cloning secret-bearing strings into public responses. Keep configuration
      contents and raw request headers out. Missing Image Optimizer entitlement remains typed even
      if broader diagnostics are deferred.
- [ ] Add `provider_diagnostics_never_change_public_errors`; run it via
      `scripts/run_test_nonzero.sh provider_diagnostics_never_change_public_errors cargo test -p edgezero-core --features test-utils`.
      Expected red for unavailable retained detail; after implementation, typed internal detail is
      present while token-bearing public wire/Display/status snapshots remain unchanged. Run four
      adapter error contracts and the task lint card. Suggested commit: `feat: retain bounded log-only provider diagnostics`.

#### F6a Capability Publication Checkpoint (2026-10-22)

- [x] Add `OutboundFailureDiagnostics` / `outbound-failure-diagnostics` to the canonical
      manifest enum, serialization and existing four adapter capability tables. Publish Native
      for Fastly/Spin SDK-cause projection and BestEffort for Axum/Cloudflare's limited visibility.
- [x] Add manifest round-trip, required/optional gate and actual registered-adapter/document-row
      regressions. Reproduce unknown-variant, missing-row and Unsupported-versus-BestEffort
      failures before filling their implementation. Keep future capabilities fail-closed.
- [x] Align the public guide, proxying guide, normative design, matrix checker and changelog.
      Document closed catalog ownership, Unknown/absent codes, API-boundary phase semantics,
      wire privacy and the distinction between diagnostic projection and hosted cause evidence.
- [x] Complete current-tree Rust/WASM/docs verification and independent review before committing.
- [ ] Complete the remaining full EZ-16 acceptance and consumer/hosted qualification separately.
      Keep the full P1 migration and failed Fastly G2 open.

Erdos mapped the canonical enum, existing generic gate and adapter tables; Beauvoir reviewed
catalog visibility and publication scope. Both proposals selected the existing capability
mechanism rather than a new runtime query or diagnostic validator. Implementation is centralized.
Native means retaining available pinned-SDK causes/codes, not recovering Viceroy-hidden causes
or promising client delivery. Local SDK/Node/WASI fixtures remain local evidence. Earlier dated
checkpoints retain their historical capability-publication status; this checkpoint supersedes
that status only. No transport behavior, SDK/decoder pins, frozen policy or approved Interfaces
digest changes, and no compatibility API is added.

All five required Rust gates passed: 27 workspace binaries, 2,130 passing tests, zero failures
and one existing ignored generated-workspace test, including 812 core tests. All three real-target
WASM clippy checks, Fastly CLI-only/no-default clippy, four core compile-fail doctests and 48
demo-core tests passed. The actual registered-adapter/doc-row and required/optional gate tests
executed. Docs format/lint/build, dependency/legacy guards and the 18 evidence-checker tests
passed; inventory validation remains 255 criteria / 511 planned legs, qualified false.
The first workspace run could not bind TCP listeners inside the sandbox; the complete rerun
with local socket access passed. A wrong initial Axum test filter selected zero tests and was
not accepted as evidence; the correctly named nonzero regression proved the missing support arm.
Spec review found one stale numeric capability count in the active implementation index; it now
references matrix rows, with a regression guard. Revised spec and quality reviews passed.
No new SDK/runtime/hosted probes or generated-workspace execution are claimed for this metadata
publication; earlier diagnostic projection evidence retains its own revision and scope.

### Task F6b: Fastly Delivery Reports and Safe Replacement

**Files:** Core `src/response_egress.rs`; Fastly `src/request.rs`, `response.rs`, `lifecycle.rs`,
`lib.rs`, and contracts; affected macros, demo and CLI templates when signatures change.

**Dependencies:** F6a, F8 and F9a for final assembly/KV failure-path tests, plus D1a callback ownership.

- [ ] Hard-migrate Fastly custom-send helpers to an owned delivery report that preserves finalize
      state and reports actual disposition/commit/body/outcome. Preserve exactly-once completion.
      Policy Abort is intentional and never NoAttempt; it cannot authorize a replacement send. A
      committed partial response cannot be replaced by another response. Cover declared KV/assembly
      pre-delivery failures and standard lifecycle behavior explicitly.
- [ ] Run core and Fastly contract tests, including actual canonical hook composition and safe
      native replacement in both permitted cases: no response was attempted, or an attempted
      response is confirmed not committed. In the second case, its original terminal report has
      already reached completion and any applicable observer; the native replacement is outside
      EdgeZero accounting. Prohibit replacement after policy Abort, unknown commitment, or
      committed delivery. Update macro/demo/template callers in the same series if signatures
      change. Add `fastly_delivery_report_prevents_unsafe_replacement` and run it with
      `scripts/run_test_nonzero.sh fastly_delivery_report_prevents_unsafe_replacement cargo test -p edgezero-adapter-fastly --features test-utils`.
      Expected red for missing report, then every disposition/commit state and exactly-once release
      passes. Repeat canonical Viceroy composition, then the task lint card. Suggested commit:
      `feat: report Fastly delivery disposition without unsafe replacement`.

#### F6b Delivery Report Checkpoint (2026-10-22)

- [x] Hard-cut all three custom-send helpers to owned NoAttempt/Aborted/Detached/Routed
      results. Preserve routed finalizer state on failure and exact terminal report identity,
      last-body commitment and separate delivery errors. Keep standard serving policy unchanged.
- [x] Consume the core attempt for terminal metadata using existing live Drop semantics.
      Already-terminal extraction samples no clock and cannot notify twice. Shared transition
      publication installs terminal state before completion and observer callbacks.
- [x] Keep safe typed pre-attempt detail separate from privately retained standard errors.
      Head conversion names URI/builder failures; required stores retain their typed open reason.
      Public Debug/messages exclude original diagnostic text; standard errors retain their source.
- [x] Migrate custom fixtures and test utilities without an old-signature wrapper. Standard
      macro/demo/template entrypoints require no source migration. Local Viceroy smoke/progress
      covers retained-client follow-up after committed source failure and no follow-up after
      framing fallback; attempted reports match one terminal event and NoAttempt matches none.
- [x] Finish final quality review and broad verification before committing this checkpoint.
- [ ] Complete F9a assembly-failure integration, consumer acceptance and separately required
      hosted qualification. This checkpoint does not close full F6b/EZ-17 or unblock Fastly G2.

Schrodinger reviewed the custom-helper ownership contract and found two initial regressions:
head-conversion detail was too coarse and standard pre-attempt errors lost their original source.
Both now have typed-safe report projections plus exact standard-error preservation; the focused
test failed before the fix and passes afterward. Herschel reviewed consuming core extraction and
the final cross-crate changes. Synthetic fixtures share the production finisher but do not claim
to execute SDK conversion or host commitment; separate normalizer/committer and Viceroy tests
cover those boundaries. No submitted Interfaces, frozen policy, SDK/decoder pin, outbound
algorithm or provider capability changes.

Quality review also corrected two fixture weaknesses: the finalizer now retains the exact
request-injected client handle, and required-store evidence checks the closed NoAttempt category.
The wrong-category control failed before validator enforcement and passes afterward. Documentation
distinguishes unobserved pre-admission/refusal responses from admitted source-error Detached
responses, which retain the application observer. Revised spec and quality reviews passed.

All five required Rust gates passed: 27 workspace binaries, 2,136 passing tests, zero failures and
one existing ignored test, including 813 core tests. The separately executed generated-workspace
gate passed its native/three-WASM builds and real fallback TCP checks. Demo-core's 48 tests and
its actual Fastly-target build passed. All three actual-target WASM clippy checks, Fastly CLI-only
clippy, both eight-leg Fastly feature matrices, strict public rustdoc, four core compile-fail
doctests, the nonzero replacement and public-wire sentinels, 58 harness tests and 17 qualification
core tests passed. The excluded Fastly fixture and harness clippy checks passed. Dependency,
shared-pin, legacy and docs guards passed, as did docs format/lint/build and 18 evidence-checker
tests. Inventory remains 255 criteria / 511 planned legs, qualified false.

Viceroy 0.21.0 passed 178 Fastly library tests and 25 contracts. Final local smoke/progress
evidence is retained in `.runs/18e0ea8c3a158010-a4bb-0`; it includes committed partial-source
failure, retained-client post-send completion, framing fallback with no follow-up, and typed
required-store NoAttempt with a native response. The earlier `.runs/18e0ea1e943e5d78-52b2-0`
failure remains retained: its harness expected the retired generic adapter-error event instead
of the committed-source event. A WASM-only test shadow-binding lint also failed before renaming.
These are working-tree local characterization runs, not frozen consumer or hosted acceptance.
F9a integration, consumer tests, hosted qualification, full EZ-17/P1 and failed Fastly G2 remain
open; standard entrypoints, outbound transport algorithms and all pinned inputs are unchanged.

### Task F6c: Typed Response Framing Diagnostics

**Files:** Core `src/error.rs`, `response_egress.rs`, `response_egress_framing.rs`;
four adapter normalizer/converter/fallback paths and contracts.

**Dependencies:** F6a and existing response framing policy; independent of F6b.

- [x] Add one exact assertion per existing `ResponseEgressFramingError` variant:
      `ContentLengthMismatch`, `InvalidConnectionHeader`, `InvalidContentLength`, `UnsupportedStatus`,
      `UnsupportedTrailers`, and `UnsupportedTunnel`, rather than string matching. Drive each through the actual
      normalizer/converter on Axum, Cloudflare, Fastly and Spin, and routed/detached/fallback paths
      wherever the rule is observable. Preserve source-category and late failure/commit distinctions;
      observer/log evidence contains the typed rule but response status/body/Display remain unchanged.
      Add `framing_observer_preserves_each_typed_reason` to core and adapter contracts; run it via
      `scripts/run_test_nonzero.sh framing_observer_preserves_each_typed_reason cargo test -p edgezero-core --features test-utils`
      and the corresponding adapter package/target commands, then each response contract suite
      and task lint card. Expected: every constructible rule is observed under the
      correct category, never flattened into generic transport/conversion or policy-panic failure.
      Existing framing rejection stays a green control; newly retained typed observation must fail
      before implementation and pass afterward. Suggested commit: `feat: preserve typed egress framing diagnostics`.

#### F6c Framing Checkpoint (2026-10-21)

- [x] Reuse the existing six-rule enum in a fixed, two-slot report carrier. Preserve the first
      application and fallback reasons independently, without altering source diagnostics.
- [x] Route attempt-owned normalization through one recorder on core begin, all four fallback
      preparers and Axum empty-stream revalidation. Log only fixed enum text and body kind at
      error level; preserve Display, status, fallback bytes, clocks and source/terminal ownership.
- [x] Publish the existing enum at the crate root and a downcast-only `EdgeError` accessor.
      Hard-migrate exact report literals to the new carrier; add no legacy report or error shape.
- [x] Reproduce missing typed observations in core and each native converter, then pass all six
      rules through actual routed/refused App dispatch and existing fallback coordinators.
      Preserve exactly-once completion, routed observation and detached observer exclusion.
- [x] Verify the local Viceroy routed-trailers case has the fixed error-level log, typed terminal
      report and unchanged 500 fallback. Strengthen the probe against additional leaking logs and
      response headers, not merely absence of leakage in the matching safe event.
- [x] Finish quality review, negative controls and broad current-tree Rust/WASM/docs verification
      before committing this checkpoint.
- [ ] Complete consumer Viceroy acceptance and any separately required hosted qualification.
      Do not promote capabilities or treat this checkpoint as full outbound-cutover completion.

Turing mapped the shared coordinator's enum loss and reviewed the bounded carrier/accessor;
Heisenberg mapped converter test seams and fallback reachability. Their proposals favored one
attempt-owned wrapper over per-adapter duplicated recording or a global first-error slot, which
would hide a fallback's independent rule. Implementation remains centralized. No approved
Interfaces, frozen policies, SDK/decoder pins or capability claims change.

The six-rule matrix uses the unchanged normalizer through actual App dispatch. Fastly and Axum
exercise their converters directly; Cloudflare and Spin exercise failed `begin()` followed by
their real fallback coordinator with existing mock committers. Detached refusals retain the
completion but deliberately invoke no global observer. Stock 500/504 fallbacks cannot currently
violate these framing rules, so all-six fallback-tagged core normalizer controls establish
attribution only, not an observed adapter-invalid-fallback failure. Postcommit streamed length
errors remain their original source categories and are not relabeled as precommit framing rules.

Core controls retain simultaneous application/fallback observations, first-per-slot behavior,
terminal rejection, independent source evidence and no recording-time clock/callback activity.
Accessor fixtures compare unchanged Display, status, headers and exact body bytes for all six
rules, and reject postcommit length-error misclassification. Policy, deadline, source and SDK
head-preparation controls must not invent framing observations. The initial expired-policy test
incorrectly assumed normalization rejects expired deadlines; that fixture was corrected to the
existing valid-expired policy contract without changing production precedence.

The local progress fixture adds one routed `Trailer` rejection on all four targets. Viceroy's
fixture logger exposes the actual core error-level event; its global observer records only the
bounded typed framing snapshot and terminal metadata. Wire and log validators reject the private
sentinel anywhere in the captured response or diagnostics. Mutation fixtures cover an extra
leaking event, a leaking response header, missing/duplicate reports and changed outcomes. This is
local characterization, not hosted delivery or resource-accounting certification.

Quality review reproduced and corrected an initial-only boundary omission in the new recorder:
normalization after writing had begun could manufacture a precommit observation. The failing
writing-state regression now passes; capture and logging are initial-only, while normalization
still runs and returns the same result. Both independent quality reviews passed after this fix,
the exact wire comparisons, negative controls and strengthened leak validators.

All five Rust gates passed: 27 workspace binaries, 2,126 tests, zero failures and one existing
ignored test, including 811 core tests. All three actual-target WASM clippy checks passed.
Cloudflare's Node/WASM suite passed 42 tests including the nonzero framing sentinel; Viceroy
passed 173 Fastly library tests and 25 contracts; pinned Wasmtime passed 35 Spin contracts and
12 SDK resource tests. All four native outbound contract suites passed (17/4/2/3 tests). Core
doctests, the public-error nonzero sentinel, all seven eight-leg feature matrices, 48 demo-core
tests, 57 harness tests, 17 qualification-core tests and fixture clippy passed. Docs format,
lint/build, dependency/legacy guards and all 18 evidence-checker tests passed. Inventory remains
255 criteria / 511 planned legs, qualified false.

The hardened local progress suites passed on all four runtimes. Viceroy evidence is retained in
`.runs/18e0c3d1b2fa2300-154c5-0`; Axum and Spin in `.runs/18e0c3e91b527738-16686-0` and
`.runs/18e0c3d1fe532360-155d1-0`. Cloudflare's first attempt retained an unsupported tool-version
record (`.runs/18e0c3e91a822ee8-166f3-0`); the next retained esbuild 0.21.5's import-syntax failure
(`.runs/18e0c41c99850f20-f5a-0`). The successful `.runs/18e0c58f04ba70a0-6123-0` uses existing
cached wasm-bindgen 0.2.129, esbuild 0.28.2, wasm-opt 130 and Wrangler 4.131.0, with explicit
binary overrides and automatic installation disabled. No dependency repin, generated JavaScript
rewrite, alternate entrypoint or hosted deployment was used. Historical failures remain failures.
These are working-tree characterization runs, not a frozen acceptance claim. Consumer Viceroy
acceptance, hosted qualification, diagnostic capability publication, full P1 and failed Fastly G2
remain open; the approved Interfaces and frozen policies remain unchanged.

## F7: Response Framing and Header Task Group

### Task F7a: Buffered Response Framing

**Files:** Core `src/response_egress.rs`, `response_egress_framing.rs`; four adapter
`src/response.rs`; Fastly `src/fastly_response_abi.rs`; fixture response cases.

**Dependencies:** Existing egress coordinator; D1a/D2/D3/D4 for ownership-specific integration.
Independent of F7b and D1b-D1d.

- [ ] Add exact Content-Length cases for buffered data, explicit values, HEAD/304, mismatched
      streamed length, suppressed bodies, and platform compression. Compare Fastly SDK Automatic
      framing on HTTP/1.1 and HTTP/2 rather than assuming raw guest length is transformed wire length.
- [ ] Derive a length only for eligible buffered untransformed payloads. Preserve representation
      lengths on bodyless responses and actual-body gating of manual framing. Host transforms may
      recompute/omit length; never force a pretransform count. Verify declared-length compressed
      streams by recording raw/wire sizes and decompressed body hash.
- [ ] Add `buffered_framing_respects_suppression_and_host_transform`; run it using
      `scripts/run_test_nonzero.sh buffered_framing_respects_suppression_and_host_transform cargo test -p edgezero-core --features test-utils`.
      Expected red for missing derived length, then exact length/bodyless controls pass. Run all
      adapter response contracts and required Fastly SDK/host-compression legs, then the task lint
      card. Suggested commit: `fix: align buffered response framing across adapters`.

### F7a Implementation Checkpoint (2026-10-23)

This is an implementation checkpoint, not full EZ-18 acceptance. The original task checklist stays
open because hosted compression, frozen SDK parity and consumer response-diff evidence are still
required. The approved outbound Interfaces and frozen policies are unchanged.

- [x] Derive exact transmitted buffered lengths, including empty and framework fallback bodies,
      after common header normalization on Axum, Cloudflare and Spin. Continue rejecting explicit
      buffered mismatches and incrementally enforcing declared streamed source lengths.
- [x] Hard-cut `ResponseEgressEnvelope::begin` and both preparation APIs to require an explicit
      `ResponseEgressFramingMode`; migrate all adapters, demo tests and generated scaffolds without
      compatibility wrappers.
- [x] Preload transmitted Fastly buffers before one non-streaming Automatic handoff. Check the
      injected deadline before/after each staging write, retain an observed timeout through a later
      backwards clock sample, and count source bytes once only after successful bulk handoff.
      Never pump, finish or abandon a body consumed by that handoff.
- [x] Preserve strict HEAD/204/205/304 suppression. Original buffers and streams are dropped
      without polling. Suppressed Fastly responses use an empty streaming handoff, preventing the
      local HTTP/2 host from deriving a false zero-length representation. No unused HEAD restoration
      field or accessor remains. Hint-bearing declared streams delegate wire framing to Automatic
      while retaining source-length enforcement.
- [x] Add the capability row, deterministic red/green controls, separate `response-framing`
      suite, bounded failure captures, actual SDK reference controls and HTTP/2 client observations.
      Preserve the existing scheduling/coding suite counts and failed Fastly G2 gate.
- [ ] Freeze and qualify hosted Fastly HTTP/1.1/HTTP/2 compression and SDK parity, including encoded
      wire counts and decompressed source hashes. Deployed Workers, frozen Spin evidence and the
      consumer response-diff acceptance remain separate required legs.

Nash investigated ownership, native preload/send and clock ordering. Russell investigated fixture
reuse, wire evidence and false-positive assertions. The selected implementation separates explicit
buffered framing from host-owned Automatic framing and keeps actual streaming progressive. Review
found and fixed avoidable staging writes after deadline, loss of typed timeout provenance, and
fixture predicates that could mistake a timeout or failed transfer for passing evidence.

The first real Viceroy run invalidated the proposed HEAD-buffer restoration: HTTP/2 SDK controls
with nonempty HEAD/204/304 bodies fail, and the SDK's 205 control transmits forbidden payload. The
requester explicitly chose strict suppression and unqualified parity rather than reproducing that
behavior. Those controls remain failed; this decision does not waive EZ-18.4 or infer hosted behavior.

Local characterization records 80/80 production cases: 32 on Viceroy 0.21.0 across HTTP/1.1 and
HTTP/2, plus 16 each on local Workers, Spin 4.1.0 and Axum. Nine of 18 Fastly SDK reference controls
fail; `--adapter fastly` and `--adapter all` intentionally return nonzero. HEAD captures report
payload observation as unavailable. Short-source cases require concrete terminal transfer failures,
not client timeouts. The capability remains Native on Axum and BestEffort on all serverless targets.
See [working-tree evidence](../../audits/evidence/2026-10-23-buffered-response-framing.json).

Verification passes all five required Rust gates, 2,181 tests across 28 workspace binaries
(823 core; one existing ignored test exercised separately by the generated-project gate), all
56 native/WASM feature-compilation combinations, the target clippy variants, native adapter
contracts, the demo and generated application checks, and documentation formatting/lint/build.
The independent build-validator also passes the default native build and all three actual WASM
builds. Its additional native all-features link check fails on Fastly SDK hostcalls. A clean
detached build of pushed baseline `fe0fd4c9` fails with the identical 83 undefined symbols, so this
is recorded as a pre-existing target-link limitation, not converted into a passing check or
"fixed" with native ABI stubs. The temporary baseline worktree was removed without changing it.

### Task F7b: Bounded Synthesized Response Headers

**Files:** Core `src/app.rs`, `response_egress.rs`; four adapter `src/response.rs` and contracts;
Fastly request preparation; fixture detached/error/fallback responses.

**Dependencies:** Existing renderer/egress decisions; D1a/D2/D3/D4 for ownership-specific
integration. F2 adds only the conditional combined overflow test; no F7a or D1b-D1d prerequisite.

- [x] Add bounded configured header sets for detached/error/fallback paths. Validate app-level
      sets at configuration; dynamic sets during normal prepare before installation. Invalid dynamic
      headers fail with the typed error while retaining the already-valid app set. Fallback executes
      no new application code and preserves response-scoped completion/deadline/abort decisions.
- [x] Where F2 is present, add `generated_overflow_retry_after_precedes_synthesized_headers`
      covering renderer plus app-level and Fastly per-request sets. Final generated cap errors omit
      Retry-After and retain unrelated headers; ordinary upstream 413 retains its surviving value;
      config 503 retains its effective retry value (default 60). Do not globally ban the header from
      validated sets. Run `scripts/run_test_nonzero.sh generated_overflow_retry_after_precedes_synthesized_headers cargo test -p edgezero-core --lib`
      plus each adapter response contract; Expected: real red/green assertions, not a skipped
      cross-feature case. Record the combined leg only when both follow-up parts are adopted.
- [x] Run core and four adapter response contracts; compare normal, fallback, admission refusal,
      normalized rejection, assembly failure, and postcommit failure paths. Preserve canonical Allow
      and effective config Retry-After policy. Add `synthesized_headers_preserve_detached_ownership`
      and run it with `scripts/run_test_nonzero.sh synthesized_headers_preserve_detached_ownership cargo test -p edgezero-core --features test-utils`.
      Expected red for missing policy, then bounded validation, preserved valid app headers and
      exactly-once completion pass. Apply the task lint card. Suggested commit:
      `feat: bound synthesized headers on every response path`.

### F7b Implementation Checkpoint (2026-10-23)

This checkpoint implements the guest-side policy, not full EZ-19 acceptance. The task's complete
runtime/consumer checklist remains open, and no frozen Interfaces, policies, SDK pins or capability
claims change.

- [x] Add immutable typed sets with inclusive 32-value/4096-byte bounds, forbidden-field and portable
      ASCII validation. Count repeated names and wire delimiters; compact retained map/backing
      storage. Failed installation preserves the previous valid set.
- [x] Snapshot the selected policy on envelopes and attempts. Decorate detached/admitted-source
      and canonical 404/405 synthesis, not matched handler results/errors or admission refusals.
      Preserve completion/deadline/abort ownership and omit configured policy on assembly failure.
- [x] Apply the snapshot through shared fallback preparation and Axum's secondary detached fallback,
      without application callbacks, repeat validation or a different fallback body/budget.
- [x] Consume a validated Fastly `prepare` replacement before conversion/head checks and install it
      on the returned envelope before egress begins. An invalid construction installs nothing;
      an empty validated set explicitly replaces the app set.
- [x] Add ownership/exclusion, field collision/order, repeated-value byte boundaries and typed
      overflow/config-retry tests. Exercise maximum-size sets through shared adapter fallback
      fixtures and align demo/template configuration with a framework-error version header.
- [x] Finish all required shared/target checks and independent final review for this checkpoint.
- [x] Record actual local normalized 414/431, GET/HEAD fallback, maximum-size provider delivery and
      dynamic replacement runtime evidence through the canonical entrypoints.
- [x] Exercise partial-configuration header exclusion and successful retained-policy recovery
      through canonical serverless assembly delivery, plus Axum's pre-listener failure boundary.
- [ ] Complete consumer-owned header integration and hosted/frozen-candidate qualification.
      Local wire tests do not close these legs.

Hilbert mapped the four delivery paths and recommended deferred envelope decoration over ingress
API churn, identifying routed-error exclusions and Axum's secondary builder. Aquinas mapped
acceptance and fixture seams and found weak completion/collision/precedence controls; the affected
assertions were strengthened. The selected path uses one core merge implementation and validated
snapshots, with no compatibility wrappers or duplicate transport. Fastly's prepare override had a
real failing Viceroy control before dispatch integration and passes after integration. Failed G2,
the full P1 hard cut, unresolved Cloudflare F2 recovery and the prior framing parity gates remain
open; this checkpoint does not waive them.

Final static review found no remaining objection after immediate completion/resource-release,
refusal-witness, complete header-set capture and both Fastly dispatch-arm controls were added.
The maximum-size captures verify all 32 configured values and their 4096-byte field-section
charge, not just one marker. Fastly's two prepare contracts pass under Viceroy. Its admitted
source-error/request-replacement composition is checked through the actual coordinator; this is
not a claim that the canonical SDK `take_body` path produced a source-construction failure.

All 12 native Spin response controls pass with `spin,test-utils`. The additional WASI library-unit
attempt cannot start under bare pinned Wasmtime 48.0.2 because the binary imports
`spin:key-value/key-value@3.0.0`, which that runner does not implement. That result remains an
unexecuted-host boundary, not a passing target test. Target Clippy passes; no SDK import shim,
feature suppression or hosted-evidence waiver was introduced to force execution.

Verification passes the five required Rust gates: 2,193 tests across 28 workspace binaries,
including 831 core tests, zero failures and one existing ignored generated-project gate. That
ignored gate was run explicitly and passed: generated formatting, strict lint, target checks,
23 generated-core tests and the actual Axum fallback TCP probe. Demo tests pass with 50 core
tests. All three actual-target SDK Clippy runs and Fastly's native CLI-only variant pass, as do
both required nonzero sentinels and documentation format/lint/build/contract checks. The generator
initially exposed template formatting/import mistakes, which were corrected. Its nested Cargo
commands require inherited `CARGO_NET_OFFLINE=true` in the restricted environment; the final run
also has localhost socket access for the TCP probe. No failed environment run was counted as
passing, and the separate bare-Wasmtime limitation above remains recorded.

### F7b Local Wire Checkpoint (2026-10-23)

The separate `synthesized-headers` fixture suite passes 55 actual HTTP/1 cases: Viceroy 0.21.0
22/22, local Wrangler 4.131.0/workerd 2026-09-10 11/11, Spin 4.1.0 11/11 and native Axum 11/11.
All configured values are checked, including 31 ordered cookies and the exact 4096-byte charge.
Canonical 404/405, matched success/error exclusions and real admission refusals are exercised.
Fastly's normalized 414/431 retain the set, while correlated detached and terminal events prove
zero admission/handler/finalize calls. Its prepare replacement, invalid byte-overflow and empty
replacement paths cover both early rejection and GET/HEAD framing fallback.

Raw captures observe clean EOF and actual HEAD payload absence; GET fallback stays exactly
`internal server error`. Final Fastly logs are rechecked after teardown for terminal count,
outcome, body kind, fallback disposition and byte count. Mutation and TCP controls reject lost,
reordered/changed/duplicate fields, payload in the header packet or a later write, changed/late
terminal records and capture failures that might otherwise skip later cases. Ramanujan reviewed
fixture design/bounds and target builds; Turing reviewed evidence assertions and identified the
final-log and terminal-content controls. Implementation remains centralized.

Earlier failed capture runs remain retained. Cloudflare's frontend keeps connections open despite
the close field; a client write-half-close produces observable EOF. Applying that FIN to the
other three frontends cancels parsing before dispatch, so the harness explicitly records the
per-target close mode. A macOS timeout-option failure after FIN was removed by nonblocking reads
under the unchanged absolute five-second budget. These are harness corrections, not production
transport changes, timeout-as-success, alternate listeners or a Cloudflare overflow-recovery fix.

The [local record](../../audits/evidence/2026-10-23-synthesized-response-headers.json) is working-tree
characterization, not exact-candidate qualification. It does not
close consumer-owned headers, assembly-failure runtime exclusion, hosted delivery, HTTP2/HTTP3,
Fastly's nine failed SDK parity controls or failed G2, nor the full P1 hard cut.

### F7b Assembly Exclusion Checkpoint (2026-10-23)

The subsequent `assembly` suite passes 40 HTTP/1 exchanges: 18 Viceroy, 11 local workerd and
11 Spin, plus one native Axum startup control. A validated two-field set is installed before
configuration deliberately fails. Both configured names are absent from canonical failure GET
and HEAD responses; config 503 retains its single retry field, and HEAD transmits no payload.
The existing three-request retained recovery sequence is followed by a canonical 404 carrying
only generation-two headers, proving that exclusion was not an inert configuration fixture.

One bounded TCP capture implementation now serves both suites. It verifies exact GET framing,
retains actual HEAD bytes through clean EOF and preserves failed captures. The superseded HEAD
reader was removed, with its regression migrated to this path. Fastly's custom delivery reports
match all four distinct request tokens. Failed-generation state release is observed before the
recovery request starts; counts, generation/instance identity, reports and boot logs are rechecked
after teardown. Negative controls cover malformed/competing fields, stale recovered values,
missing/duplicate installation witnesses, count-preserving report duplication and delayed release.

Axum uses its canonical startup function with the selected port already occupied. The partial
installation witness, exact configuration-error boundary and nonzero exit establish configuration
failure precedence over binding; no HTTP response or native response-delivery guarantee is claimed.
Banach reviewed provider routing/ownership and target builds; Fermat identified the framing,
per-request report correlation and immediate-release controls. Both reviews were addressed without
production API, SDK/pin, frozen-policy or transport changes.

This closes the local assembly-exclusion leg left open above, not consumer integration, hosted or
frozen-candidate qualification. Fastly G2, the nine failed SDK parity controls, Cloudflare overflow
recovery and the broader outbound migration remain open. The
[local evidence record](../../audits/evidence/2026-10-23-assembly-response-headers.json) preserves
both the earlier successful run and the stronger final controls; it is not a release certificate.

## Task F8: Declared KV Failure Bindings

**Files:** Fastly `src/request.rs`/`key_value_store.rs`, Cloudflare and Spin corresponding modules,
core `src/key_value_store.rs` and `store_registry.rs`; adapter tests and fixture routes.

- [ ] Add `declared_kv_open_failure_retains_binding_and_recovers` to core/adapter tests; run
      `scripts/run_test_nonzero.sh declared_kv_open_failure_retains_binding_and_recovers cargo test -p edgezero-core --features test-utils --lib`
      and the index/Q2's native/SDK cards using that sentinel. Expected: red when a failed declared
      store disappears; after implementation the typed failure binding remains, status dispatch
      still works unless require_kv applies, and a later restored binding recovers. Include these
      cases in the smoke qualification routes, then execute
      `scripts/smoke_test_reusable_app.sh --adapter all --suite smoke` with nonzero case IDs and
      actual read/write/delete/list results. Apply the task lint card.

- [x] Add missing declaration versus declared-open-failure cases, provider StoreNotFound versus
      transient error, and unaffected status-route dispatch. Honor explicit `require_kv` where requested.
- [x] Preserve failed declarations as typed handles; defer opening/failure to actual use where
      required. Do not retain transient availability errors as permanent absence or indiscriminately
      fail every request. Keep bounded extraction/secret/config behavior from the outbound baseline.
- [ ] Test failed read/write/delete/list behavior and recovery on a later request with the binding
      restored. Source/provider error payloads remain off the wire and response completion still runs.
- [ ] Run changed adapter unit/contract tests and required real runtime legs. Suggested commit:
      `fix: preserve declared KV bindings through provider open failure`.

### F8 Implementation Checkpoint (2026-10-21)

The requester explicitly adopted Cloudflare and Spin alongside Fastly. All three canonical
registry paths now install declarations without provider opens. The first operation retains a
settled result only in its request-local binding. `KvStoreOpenFailure` separates missing binding,
invalid configuration and provider unavailability without storing provider text or resolved names;
rendering is 500/500/503. Named/default declarations remain resolvable; unknown IDs remain absent.
Explicit required-store failure still precedes prepare/delivery, and Axum startup is unchanged.

Lovelace investigated SDK classification and ownership, proposing request-local initialization
rather than per-operation opening because Fastly host handles have no Rust close operation.
Ampere investigated async cancellation and runtime evidence. The selected Spin async mutex
serializes only initialization; operations clone the owned handle and release that lock before I/O.
Cancellation before settlement leaves the cell empty. Cloudflare checks callable namespace methods
before the SDK's unchecked casts. Only warning membership for compiled IDs is global: no eviction,
runtime labels, handles, or availability state survives requests. Implementation was centralized.

Core fake-binding tests and SDK/source-harness tests distinguish scripted fresh-request recovery
from real provider restoration. The local smoke fixtures retain the same strict
`free/read/read/free` sequence, explicit instance identity, 0/1/1/1 warning receipts and actual
completion reports, followed by separately attributed healthy-store CRUD. Each linked operation
also needs one terminal report, and warning counts are rechecked after runtime shutdown.
The sequential Cloudflare/Spin proof precedes overlap, which can create multiple runtime instances;
it does not disable or relax the overlap checks. Retained failed runs remain evidence, not waivers.

Verification passed all five root Rust gates (794 core tests), actual Cloudflare JS/WASM acquisition
tests, Fastly WASI contracts, Spin source-harness cancellation/classification tests, fixture tests,
and real-target clippy. The final four-runtime smoke run passes all 24 F8 request cases and all
three post-shutdown warning checks, with no failed case. The overall smoke result is still exit 2:
the separate Spin overlap observations remain unverified (per-request and retained in final C).
Smoke probes after the
new retained-prefix checks are warm, not cold-start benchmark evidence; benchmark ordering is unchanged.

The checkpoint evidence records exact commands, source/tool/artifact hashes and outcomes in
`docs/audits/evidence/2026-10-21-declared-kv.json`. Consumer acceptance criterion 5, actual provider
outage/restoration and hosted qualification remain open. The untouched G2 failure still blocks
dependent Fastly migration; no outbound API, SDK/decoder pin, capability or approved API digest
changes in this independent follow-up.
All 255 acceptance criteria and the reviewed leg set remain unchanged. The inventory records
the implementation adoption separately; its formal release approvals and planned cutover commands
are not promoted by these local smoke results.

### F8 Operation Delivery Checkpoint (2026-10-23)

- [x] Preserve the original same-instance `free/read/read/free` sequence and 0/1/1/1 warning
      assertions. Append failed write/delete/list operations through each canonical local serverless
      entrypoint, with exact sanitized 500 JSON and actual typed `KvError::OpenFailed` receipts.
- [x] Require one operation-handler entry/result pair per operation and none for KV-free routes.
      Match request token, observed instance, store selector and operation; reject a generic internal
      error masquerading as the expected missing-binding failure. Enforce start/entry/result/terminal
      order, including after shutdown; reject a terminal notification before the operation result.
- [x] Retain healthy linked-store write/read/list/delete checks, then require an explicit null read
      result and empty list after delete. This is continuity of a different, linked declaration,
      not restoration of the missing declaration.
- [x] Revalidate all 13 request tokens per adapter after runtime shutdown, including when an earlier
      smoke check failed. Check exactly-once terminal events and warning counts across the observed
      instances; preserve the original error alongside any separately recorded teardown failure.
      Cloudflare/Spin warning attribution is per instance; Fastly checks sequential aggregate
      endpoint totals because those warning lines do not carry an instance tag.
- [ ] Complete consumer acceptance, actual missing-binding outage/restoration and frozen/hosted
      qualification before closing F8.

The [operation-delivery record](../../audits/evidence/2026-10-23-declared-kv-operations.json)
retains 39 passing request cases and three passing post-shutdown validations. The complete
four-adapter smoke run exits 2, not 0: Spin retained-mode overlap remains unverified. The final run
observed per-request overlap; preceding runs left both modes unverified. Neither closes hosted evidence.
This checkpoint changes fixture instrumentation and validation only. Production store adapters,
outbound behavior, capabilities, SDK/decoder pins and the approved interface contract are unchanged.

The first expanded run correctly failed its new, overbroad seven-request same-instance assertion.
Pinned Viceroy 0.21.0 has `NEXT_REQ_ACCEPT_MAX = 5`, limiting a guest to its initial request plus
five follow-ups. The final test keeps the original four-request proof unchanged and permits natural
turnover only in the added operations. Cloudflare/Spin require one warning per missing-operation
instance and none for healthy-only instances; Fastly checks its endpoint total after each request
against the number of observed missing-operation instances. The latter cannot independently
attribute late warnings across different instances. The test does not restart the prefix, retry a
failed observation, alter production serving settings or claim a hosted reuse guarantee. The failed
run remains in the evidence record. Original SDK/source-harness recovery tests remain separate.

## F9: Application Assembly and Axum Task Group

### Task F9a: Fallible Retained Application Assembly

**Files:** Core `src/app.rs`/`router.rs`; macros `src/app.rs`; four adapter `src/lib.rs` and
entrypoint/lifecycle modules; fixture apps and affected demo/template callers.

**Dependencies:** Existing fallible assembly baseline and P1's client contract; D1a/D2/D3/D4 for
affected lifecycle integration. F8 applies to declared-KV assembly-failure integration only.
No F9b or D1b-D1d prerequisite. These scheduling scopes do not change the release blocking set.

- [x] Add failures-before-configure, registered-state identity/access, configure-from-state,
      manually-built-router policy/platform parity, success-only retention, and retry-after-failed-build
      tests. Hard-migrate route assembly to Result; no expect-based configuration wrapper.
- [x] Expose typed assembled-app state access and one manual-router assembly path that applies
      name/configure/platform checks exactly once. Retain only complete successful apps, safe under
      overlapping requests. Standard Fastly serving answers/retries assembly failures through canonical
      detached delivery; custom callbacks can do so without an assembled app. Log once on boot target.
- [x] Implement the adopted macro fallible-state and standard serverless failure-response
      SHOULDs. Preserve Axum fail-before-bind behavior; do not
      retain the temporary failure router or require async route-building inside EdgeZero.
- [x] Add `failed_app_assembly_retries_without_retaining_partial_state`; run it before/after the
      hard cut with `scripts/run_test_nonzero.sh failed_app_assembly_retries_without_retaining_partial_state cargo test -p edgezero-adapter-fastly --no-default-features --features test-utils --lib`.
      Expected red for missing fallible/retained-state contract, then retry and successful-state
      identity pass. Run macros and four adapter assembly contracts plus applicable retained-instance
      fixtures and the task lint card. Suggested commit: `feat: assemble and retain only complete applications`.
- [ ] Close the remaining consumer acceptance and exact-candidate qualification record.
      The implementation and local runtime checks below do not substitute for those external legs.

#### F9a Assembly Foundation Checkpoint (2026-10-22)

- [x] Hard-cut `Hooks::routes` and macro-generated `build_router` to Result. Return route errors
      unchanged and skip configuration. Migrate every handwritten implementation and direct
      generated-router caller, including the excluded demo and reusable-fixture workspaces.
- [x] Share one configured assembly path through `App::from_router::<A>(router, platform)`.
      Apply name/configure/platform checks once, without rebuilding the supplied router, while
      preserving configuration-error precedence and the complete metadata comparison.
- [x] Borrow typed state from the router's existing registry through `App::state` and
      `RouterService::state`. Preserve exact-type lookup, shared interiors and last-write-wins.
      Prove configuration-derived head limits and canonical State extraction use that source.
- [x] Exercise real `Sandbox<App>` route failure, configuration failure, partial-state release,
      subsequent success and retained state identity. Keep the existing success-only mechanism.
- [x] Finish scoped spec/quality reviews and all root, actual-target, demo/generator/fixture,
      documentation and dependency verification before committing the foundation.
- [x] Implement app-free canonical Fastly assembly-error delivery and standard retained retry
      integration in the subsequent unified checkpoint below. The original foundation alone
      did not change entrypoint responses; full F9a/EZ-22 consumer acceptance remains separate.
- [x] The requester adopted EZ-22.8 and EZ-22.10 on 2026-10-22: one fallible assembly and
      error-delivery contract, including macro state expressions and all single-request Fastly,
      Cloudflare and Spin entrypoints. These are part of this hard cut, not compatibility modes.

#### F9a Unified Failure Delivery Work

- [x] Construct assembly-error egress without an App or temporary error router. Share the
      existing default renderer, preserve typed status and redaction, log the failure once on
      the boot target, and carry the caller's clock and captured request start into egress.
      No successfully configured policies, completion factory, observer or application headers
      exist on this path; use the finite default response-write policy.
- [x] Reuse that envelope through the existing adapter converters on Fastly, Cloudflare and
      Spin. Remove superseded host-generic assembly-error mappings and their test-only wrappers.
      Keep Axum startup failure before binding; keep transport failures distinct from assembly.
- [x] Let Fastly custom callbacks deliver the error without an App. Standard retained serving
      delivers the same detached response and retries assembly on the next request, retaining
      only complete success. Preserve commitment-safe delivery and no automatic native resend.
- [x] Exercise fallible macro state, typed/redacted wire failures, HEAD suppression, supplied
      clocks, partial-state release, and standard/custom runtime failure-to-success recovery.
      Align the demo, generator, active guides and changelog; record local evidence separately
      from consumer integration and hosted qualification. Fastly G2 remains a separate gate.

Rawls mapped macro emission and callers; Herschel mapped retained assembly and provider delivery.
Both proposed one shared configured core path and state access through existing Extensions.
The chosen macro helper itself returns Result rather than keeping an infallible helper wrapped
only at the trait boundary. Existing low-level App constructors remain tools, not compatibility
wrappers. SDK/decoder pins, the approved Interfaces, frozen policies and outbound transports are
unchanged. Fastly G2, P1 cutover and consumer/hosted qualification retain their separate gates.

Both scoped reviews passed. Quality review identified two additional test gaps, now covered:
the original internal error object is retained across route and both configuration paths, and
all adapter assembly boundaries reject a failed route build before configuration (Axum also
preserves its pre-bind boundary). Real Sandbox tests prove partial-state release and successful
state identity, not HTTP serving-loop recovery. Rawls' verification handoff found incomplete
generated-project output and missing standalone receipts; the generated gate subsequently
completed, fresh demo tests/lints ran, and exact commands/exit results are recorded below.

Local verification used Rust 1.98.1, with no SDK/decoder repin. Relative to base
`5968cac4e0d18964adebb5e261595431e613cd2a`, the 22 changed Rust/template files have aggregate
SHA-256 `f406cc0fcb7c15b9dd612fb1436ef48890d79875146b092db52a7891e3b4efd0`.
The aggregate hashes the concatenation of sorted `SHA256(file)`, two spaces, repository-relative
path and LF for each changed `.rs`/`.hbs` file. This identifies local implementation inputs,
not a hosted qualification artifact or a release approval. Frozen policy and lockfiles are unchanged.

All five root gates completed with exit 0: formatting, strict workspace clippy, workspace tests,
feature compilation and Spin target compilation. Workspace tests passed 2,147 cases across 27
binaries, zero failures and one existing ignored generated-workspace case; core passed 818.
That ignored gate was separately executed and passed native/three-WASM compilation, generated-core
tests and fallback TCP checks. All three actual-target WASM clippy checks passed. Viceroy 0.21.0
passed 180 library tests and 25 contracts, including the real assembled-app retention case.
Demo-core passed 48 tests and strict clippy; all three demo WASM checks passed. Fixture-core's
17 qualification tests, fixture host/Axum/Fastly clippy and all four fixture target checks passed.
Strict public rustdoc, four core compile-fail doctests, docs format/lint/build, dependency/shared-pin,
legacy/docs guards and 18 evidence-checker tests passed. Inventory remains 255 criteria / 511
planned legs, qualified false. Early type/import/lint failures remain in the local logs and are
superseded by explicit successful reruns, not treated as passing evidence.

Exact primary commands below completed with exit 0; `--offline --locked` preserves the selected
workspace inputs. Native TCP and Viceroy commands ran with local socket permission. These are
foundation regression/characterization results only; no new hosted probe or capability is claimed.

```sh
cargo fmt --all -- --check
cargo clippy --offline --locked --workspace --all-targets --all-features -- -D warnings
cargo test --offline --locked --workspace --all-targets
cargo check --offline --locked --workspace --all-targets --features 'fastly cloudflare spin'
cargo check --offline --locked -p edgezero-adapter-spin --target wasm32-wasip2 --features spin
cargo clippy --offline --locked -p edgezero-adapter-fastly --target wasm32-wasip1 --all-targets --features fastly,cli,test-utils -- -D warnings
cargo clippy --offline --locked -p edgezero-adapter-cloudflare --target wasm32-unknown-unknown --all-targets --features cloudflare,cli,test-utils -- -D warnings
cargo clippy --offline --locked -p edgezero-adapter-spin --target wasm32-wasip2 --all-targets --features spin,cli,test-utils -- -D warnings
cargo clippy --offline --locked -p edgezero-adapter-fastly --no-default-features --features cli --all-targets -- -D warnings
SSL_CERT_FILE=/etc/ssl/cert.pem CARGO_TARGET_WASM32_WASIP1_RUNNER='viceroy run' cargo test --offline --locked -p edgezero-adapter-fastly --target wasm32-wasip1 --features fastly,test-utils --lib --test contract
scripts/run_test_nonzero.sh --ignored generated_workspace_compiles cargo test --offline --locked -p edgezero-cli --test generated_project_builds
scripts/run_test_nonzero.sh assembly_errors_preserve_the_original_internal_source cargo test --offline --locked -p edgezero-core --lib
scripts/run_test_nonzero.sh failed_app_assembly_retries_without_retaining_partial_state cargo test --offline --locked -p edgezero-adapter-fastly --no-default-features --features test-utils --lib
cargo test --offline --locked --manifest-path examples/app-demo/Cargo.toml -p app-demo-core
cargo clippy --offline --locked --manifest-path examples/app-demo/Cargo.toml -p app-demo-core --all-targets --all-features -- -D warnings
cargo fmt --all --manifest-path examples/app-demo/Cargo.toml -- --check
cargo fmt --all --manifest-path tests/fixtures/reusable-app/Cargo.toml -- --check
cargo clippy --offline --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-core -p fixture-harness --all-targets --features fixture-core/qualification -- -D warnings
cargo clippy --offline --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-axum --all-targets -- -D warnings
cargo clippy --offline --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-fastly --target wasm32-wasip1 --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --offline --locked --workspace --all-features --no-deps
node scripts/check_outbound_docs_contract.mjs
bash scripts/check_outbound_legacy_api.sh
node scripts/check_serde_json_map_contract.mjs
```

#### F9a Unified Failure Delivery Checkpoint (2026-10-22)

The requester adopted both SHOULDs as one hard-cut contract. One app-free core factory now
shares the default renderer and detached envelope; all three serverless adapters release the
unread native request and use their existing converter once. Failed configuration contributes
no callbacks, headers, observer or retained App. The 30-second default write deadline is
anchored before assembly in the supplied clock domain; synchronous initialization itself is
not preempted. Fastly standard retained serving answers/retries, and custom callbacks receive
the detached owned report without native resend. Axum logs the original failure once but
still returns its source-preserving startup error before binding, without an HTTP response.

Ramanujan proposed/reviewed Fastly ownership and the focused fixture; Linnaeus mapped the
Cloudflare/Spin converters and reviewed quality. Both approved the shared factory and deletion
of superseded host-generic wrappers; both reviewed the final Axum diagnostic delta. Central
implementation kept transport code, dependency pins, lockfiles, the approved Interfaces and
frozen policies unchanged. Demo state identity and generated manual-router policy tests use
the same assembler; normal generated entrypoints need no extra assembly plumbing.

Relative to `e07a30d23d37e0f640bdbe6a181a619cc70530bf`, 25 changed Rust/template files have
aggregate SHA-256 `872ddd60aff29ac208dfbbac7570a3326e9cc480b5fad4833124230bee075ef3`, using
the sorted source-file hashing recipe above. This is local working-tree characterization,
not an exact-commit release qualification or approval of unresolved gates.

Local Rust 1.98.1 verification completed with exit 0: all five root gates; 2,151 workspace
tests across 28 binaries, zero failures and one separately executed ignored generator gate;
820 core tests and the once-only full-log/redacted-wire regression; seven macro integration
tests; all three actual-target WASM clippy checks; 180 Fastly WASI library tests and 25 contracts;
49 demo-core tests, demo clippy/three WASM checks, fresh generated native/three-WASM builds,
22 generated-core tests and fallback TCP checks; 18 fixture-core tests and 61 harness tests;
fixture host/three-WASM clippy and Axum compilation; strict public rustdoc; docs format/lint/build;
decoder/shared-pin/map/legacy/docs guards and 18 evidence-checker tests. The inventory still
contains 255 criteria / 511 planned legs and reports qualified false.

The new focused `--suite assembly` completed with exit 0 on Viceroy 0.21.0, local workerd
2026-09-10 through Wrangler 4.131.0, and Spin 4.1.0. Seven Fastly cases cover standard/custom
503-to-200-to-200 recovery, successful-state retention, partial-state release, all standard
single-request helpers, exact configuration retry metadata, HEAD suppression and internal-error
redaction. Cloudflare and Spin each pass per-request and retained cases, including actual
same-instance recovery. Boot logs are checked once per failed request, including HEAD. The
existing full Fastly smoke suite also passes after replacing its native initialization response.

Local events (each with zero failed/unverified/unsupported records):

| Adapter    | Events Path                                                   | Events SHA-256                                                     |
| ---------- | ------------------------------------------------------------- | ------------------------------------------------------------------ |
| Fastly     | `/private/tmp/edgezero-f9a-unified-fastly-final/events.jsonl` | `f6a9611381ca4269eb4d5d1139bc511b25977d021403c5ebf267e8241216e7aa` |
| Cloudflare | `/private/tmp/edgezero-f9a-unified-cloudflare-b/events.jsonl` | `04fa354bd903b9aa9579df7879a6b8bac9720677eda0d2d81eb67d660af68a99` |
| Spin       | `/private/tmp/edgezero-f9a-unified-spin-final/events.jsonl`   | `22e8b2f6d195765314fbdbc1fa5e551cde89be7ae807c098d8299bf1942756a2` |

Earlier fixture/validator failures are retained locally, not counted as passing evidence:
the HEAD capture test incorrectly assumed one TCP read contained the method, the boot validator
counted two separate GET/HEAD failures as one, and fixture-only `std::time::SystemTime` failed
on Workers. The fixes use complete request-line reads, per-failure counts and EdgeZero's portable
clock. Fresh successful reruns supersede those failures. Template formatting and strict target
lint failures were likewise fixed and rerun, without suppressions or weakened assertions.

Consumer integration (including acceptance criteria 9 and 10), durable exact-candidate/hosted
qualification, full EZ-17/EZ-22 acceptance, P1 cutover and failed Fastly G2 remain open. No
provider capability is promoted, no waiver is inferred, and no temporary failure app is retained.

### Task F9b: Policy-Aware Axum Servers and Test Helpers

**Files:** Axum `src/dev_server.rs`, `service.rs`, `test_utils.rs`, `src/lib.rs`, TCP/contract tests;
core/macros callers only where required to consume F9a's assembled app; fixture apps.

**Dependencies:** F9a and D2's local executor; independent of F6b's delivery report.

- [x] Make programmatic Axum servers accept the assembled app through the same bounded ingress,
      local executor and egress path as `run_app`. Provide policy-aware TCP/test helpers, not a public
      alternate router service that bypasses admission. A supplied HttpClient avoids constructing an
      unused default transport. Run core/macros, Axum TCP, and required retained-instance fixtures.
      Suggested commit: `feat: serve assembled app policies through Axum test helpers`.

F9b is its own implementation/test slice, not a rename of `router.oneshot`. For each of EZ-23's
13 acceptance criteria, G1 records its own test target, command and assertion before F9b closes.
Implement in this order: assembled-app server construction; every configured policy through real
TCP; default-client avoidance when state supplies a client; policy-aware test helper ownership and
shutdown; method/path extraction plus state/store parity; and retained app/policy identity across
requests. Keep the existing ingress connection/parser/deadline tests unchanged. Add explicit tests
for admission Abort/refusal, head limits, custom renderer, response policy/completion, and the
production local executor. Run:

```sh
cargo test -p edgezero-adapter-axum --features axum,test-utils
cargo fmt --all -- --check
cargo clippy -p edgezero-adapter-axum --all-targets --all-features -- -D warnings
```

Expected: all existing TCP tests and the 13 mapped acceptance cases execute and pass. A thin
router-only response test, compile-only helper, or a new service bypassing production ingress is
not equivalent evidence.

F9b's sentinel map (current execution/readiness is recorded in the checkpoints below):

| EZ-23 Criterion | Sentinel / Owner                                   | Expected Assertion                                                                                     |
| --------------- | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| 1               | `configured_target_limit_applies_on_tcp`           | Both programmatic and test server reject 100-byte target under configured cap 64 with 414              |
| 2               | `configured_renderer_applies_to_not_found`         | Unmatched 404 carries the configured marker                                                            |
| 3               | `admission_abort_prevents_response_and_dispatch`   | Zero response bytes and zero handler/middleware calls                                                  |
| 4               | `test_server_preserves_upload_and_read_deadline`   | Full 1 MiB chunked upload, and a stalled upload gets 408                                               |
| 5               | `test_server_streams_before_source_completion`     | First of three timed chunks arrives before third is produced                                           |
| 6               | `test_server_buffers_with_exact_content_length`    | Five-byte buffered response has Content-Length 5                                                       |
| 7               | `test_server_suppresses_head_payload`              | HEAD has headers and no payload bytes                                                                  |
| 8               | `raw_head_limit_precedes_admission`                | Five fields under raw max four are rejected before admission; server recovers                          |
| 9               | `test_server_shutdown_releases_live_source`        | Mid-stream shutdown drops source and returns successful serving result                                 |
| 10              | `state_client_skips_real_transport_initialization` | Supplied client records send; no real target socket or transport init                                  |
| 11              | `foreign_platform_app_keeps_configured_policies`   | Cloudflare-platform app's configured policies still apply under Axum                                   |
| 12              | `plain_tokio_test_runs_local_handler_work`         | Local spawned work and begun-request-fed stream complete without executor panic                        |
| 13              | TS-owned route/parity suites using the new helper  | Production settings give 414 for 9000-byte target and empty GET reaches origin on all host-parity apps |

Run each upstream sentinel with `scripts/run_test_nonzero.sh <name> cargo test -p edgezero-adapter-axum --features axum,test-utils`.
Expected red: at least the configured-app/client-construction cases expose F's gap. Existing
working ingress behaviors are regression controls, not forced failures. TS provides criterion 13's
integration result; EdgeZero does not add a dependency on the consumer's crates to fake it.

#### F9b Assembled-App TCP Checkpoint (2026-10-22)

- [x] Hard-cut `AxumDevServer::new` and `with_config` to consume `App`; remove implicit default
      reassembly. `run_app` uses the same server constructor/serving path after startup setup.
- [x] Add `serve(listener, shutdown)` and `test-utils::AxumTestServer::{start, address, shutdown}`.
      Both use the existing bounded HTTP/1 supervisor, current-thread runtime and LocalSet.
      Explicit shutdown signals and awaits the same worker; drop signals without blocking.
- [x] Prefer the registered `HttpClient` lazily, without invoking the default factory. Preserve
      its complete policy handle; construct the ordinary fallback once with the application clock.
      Remove the obsolete transport-only conversion helper, not retain a compatibility path.
- [x] Exercise the twelve upstream sentinels through TCP or the zero-initialization factory seam,
      plus refusal/recovery, detached Send/Abort, egress clock/observer/deadline, explicit store/state
      parity, drop cancellation and serving-error controls. Foreign metadata uses the actual
      Cloudflare export through a dev-only dependency, not a copied platform constant.
- [x] Update the Axum guide, hard-migration changelog and documentation contract guard. Existing
      demo, generated and fixture entrypoints use `run_app` and inherit the same production path.
- [x] Finish final independent spec/quality reviews and broad Rust/docs/demo/generated verification.
- [ ] Close criterion 13 with consumer-owned route/parity results, and criterion 12's conditional
      begun-request leg after the separate head-first/eager hard cut. Complete exact-candidate
      qualification separately; the whole F9b/EZ-23 task is not closed by this local checkpoint.

Aristotle investigated constructor callers and worker ownership; Franklin mapped TCP acceptance
and local-executor coverage. Both proposed consuming the complete app and reusing the bounded
serving path, with no public router-only service. Central implementation follows that design.
The initial tests failed on the missing TCP helper and router-only constructor signatures; the
client test also failed on the absent lazy factory seam. An initial sandboxed run denied TCP bind;
real loopback reruns passed. No test assertion or ingress resource bound was weakened.

The helper deliberately skips logger setup, startup preflight and metadata-driven store opening;
callers attach explicit handles/registries. Foreign-platform apps receive Axum context, not their
provider's request context. Shutdown cancels rather than drains active responses. Local spawning
is covered, but the not-yet-integrated begun-request API is not emulated by ordinary `send`.
Fastly G2, P1 cutover, hosted qualification and all capability assignments remain unchanged.

Spec review found missing actual Cloudflare metadata and refusal TCP controls; both are now
covered. Quality review strengthened HEAD validation to reject every byte after the first header
terminator, including a transport-generated chunk terminator. The negative control demonstrates
the old suffix assertion accepted that illegal tail. Both reviewers signed off this checkpoint.
The fabricated serving-task failure checks helper error forwarding, not an actual worker startup
failure or JoinError. Cancelled-shutdown cleanup additionally relies on inspection of the existing
drop-guard ownership; explicit shutdown and handle-drop cleanup have real TCP controls.

Final source verification uses base `f29fd9f2910f3fe192b751b379507fa553209d47` and six changed
Rust files, aggregate SHA-256 `b419afa3b3085ce19af0769042dc70d0725d26fe0aa879a6941be196a816e198`
with F9a's sorted file-digest/path convention. The requirements, approved Interfaces, core
outbound/compression modules and frozen policies remain unchanged; the policies digest remains
`c52b4aea6b8d2793f30784e88016a3676088d344c0a5607774a5d9f1a7ffae80`.
The sole lockfile change records the dev-only Cloudflare metadata dependency; no pins changed.

All five root gates pass on Rust 1.98.1, including 2,171 workspace tests across 28 binaries with
one existing ignored gate, separately executed successfully. Axum passes 233 unit and 17 contract
tests; all twelve required upstream sentinel invocations list and execute their named tests.
Seven eight-combination adapter matrices, three real WASM-target strict clippy runs, public
rustdoc, dependency/legacy/docs guards, docs format/lint/build, 49 demo tests, the generated
workspace gate (including 22 generated-core tests and TCP fallback checks), 18 fixture-core and
61 harness tests, and all 18 evidence-checker tests pass. The ordinary Axum smoke suite observes
one cold and four reused requests. Local logs and smoke evidence are under
`/private/tmp/edgezero-f9b-*`; they are working-tree characterization, not durable release evidence.
The acceptance inventory remains 255 criteria / 511 planned legs with `qualified: false`.

The later [G1/F9b binding checkpoint](2026-10-18-trusted-server-outbound-implementation.md#g1f9b-local-acceptance-binding-checkpoint)
activates criteria 01-11 using the actual tests in the map above, strengthens criterion 01's
unconfigured success control, and runs the exact guarded commands in the existing Axum CI job.
The original binding checkpoint selected both criterion 10 tests with a separate factory CI guard.
The subsequent [inventory-driven consolidation](2026-10-18-trusted-server-outbound-implementation.md#g1f9b-inventory-driven-suite-consolidation-2026-10-25) replaces those invocations with one full
Axum suite and requires both primary and additional factory sentinels before execution.
Criteria 12/13 remain planned for their outstanding begun-request/consumer portions. The earlier
511-planned count is historical; current bindings have eleven ready and 500 planned legs without
changing source criteria, applicability or qualification. The local execution record above remains
characterization of its original source, not passing evidence for a later candidate.

## F10: Conditional Scripted-Client Task Group

G1/G4 records separate maintainer-attributed decisions for EZ-24.11, EZ-24.12 and the JavaScript
part of EZ-24.2. F10 prepares proposals, not its own approval. Each child closes only for its
selected parts/targets; publishing P6's host/WASI core does not adopt these follow-ups.

### Task F10a: Abandoned Script and Cancellation Accounting

**Files:** Core `src/outbound/testing.rs` and colocated tests; CI target test activation in Q2.

**Dependencies:** Published P6 and explicit EZ-24.11 adoption/confirmed-present disposition.

- [ ] Add `abandoned_script_releases_once_without_unconsumed_report` covering begun-request and
      batch drop/cancel, recorded-but-undelivered outcomes, dispatched versus unscripted requests,
      and source release. Run it before/after implementation with
      `scripts/run_test_nonzero.sh abandoned_script_releases_once_without_unconsumed_report cargo test -p edgezero-core --features test-utils`.
      Expected red for missing accounting, then exactly-once release and no false unconsumed-script
      report. Unscripted requests still fail loudly without panic.
- [ ] Run the same nonzero sentinel on the adopted WASI target using P6's pinned runner and on
      JS only if F10c is selected. Q2 activates those tests in CI; confirm no wall-clock sleeps,
      threads or provider emulation. Apply the task lint card. Suggested commit:
      `test: account for abandoned scripted exchanges`.

### Task F10b: Shared Demo and Generator Test Client

**Files:** CLI `src/templates/core/src/handlers.rs.hbs`, generator tests and
`tests/generated_project_builds.rs`; demo `crates/app-demo-core/src/handlers.rs`; Q2 CI activation.

**Dependencies:** Published P6 and explicit EZ-24.12 adoption/confirmed-present disposition;
independent of F10a, with cancellation tests selected only when that part is present.

- [ ] Replace demo/template bespoke doubles with the published utility,
      delete the duplicated implementations, and keep examples thin. Test request option recording,
      response policies, arrival/cutoff semantics, redirects, and typed errors rather than empty stubs.
- [ ] Extend the existing `generated_workspace_compiles` integration test rather than adding a
      duplicate execution wrapper. It already lists `generated_outbound_http_smoke` exactly once
      and runs the generated core tests. Run it before/after migration with
      `scripts/run_test_nonzero.sh --ignored generated_workspace_compiles cargo test --offline --locked -p edgezero-cli --test generated_project_builds`.
      Expected red for assertions using the unadopted shared double, then actual generated tests
      assert its recordings and behavior. Preserve the existing nonzero-listing guard, generated
      test execution and demo CI job. Run core test-utils, CLI and excluded demo tests from Q2;
      extend their existing CI execution with the adopted cases. Apply the task lint card.
      Suggested commit: `test: align generated outbound examples with shared scripts`.

### Task F10c: Published Scripted Client on JavaScript

**Files:** Core `Cargo.toml`, test-utils feature gates and `src/outbound/testing.rs`; WASM test runner
setup and CI commands owned by Q2.

**Dependencies:** Published P6 and explicit JavaScript EZ-24.2 adoption; independent of F10b.

- [ ] Add a target-specific `wasm-bindgen-test` dev-dependency under core's
      `cfg(all(target_arch = "wasm32", target_os = "unknown"))` test configuration, aligned with the pinned runner,
      and annotate the JS sentinel with `#[wasm_bindgen_test]`. Keep it out of production
      dependencies and the core runtime. Q2 owns installation and CI invocation of the matching
      `wasm-bindgen-test-runner`; the native Rust test attribute alone is not a JS execution proof.

- [ ] Add a WASM-gated `scripted_client_uses_paired_js_fake_time` test for owned local futures,
      wakeable fake time, head/chunk timing and strict cutoff without provider emulation. Run the
      before/after command:
      `CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner scripts/run_test_nonzero.sh scripted_client_uses_paired_js_fake_time cargo test -p edgezero-core --features test-utils --target wasm32-unknown-unknown --lib scripted_client_uses_paired_js_fake_time`.
      Keep the explicit library/filter selection for target isolation; P1's external API contracts
      run separately with their native commands.
      Expected red for missing target support, then a nonzero pass without wall-clock sleeps or
      extra runtime/thread requirements. Include F10a only when separately adopted, and activate
      the selected tests in Q2 CI. Apply the task lint card. Suggested commit:
      `test: qualify portable outbound scripts on JavaScript`.

## Phase Exit

- [ ] Execute the index's shared gates and each selected lettered task's named sentinel/target
      commands. Use Q2's concrete feature matrices for the changed adapters and G1's acceptance
      manifest to check every applicable case; Expected: nonzero passes and no uncovered child.
- [ ] Each implemented part is marked present with matching local/hosted evidence requirements.
- [ ] Deferred parts retain explicit interim behavior and never silently become production gates.
- [ ] New code replaces obsolete code directly; no stale transport, duplicate helper, or compatibility
      API remains after the hard migration.

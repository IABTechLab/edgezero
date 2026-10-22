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

### Task F7b: Bounded Synthesized Response Headers

**Files:** Core `src/app.rs`, `response_egress.rs`; four adapter `src/response.rs` and contracts;
Fastly request preparation; fixture detached/error/fallback responses.

**Dependencies:** Existing renderer/egress decisions; D1a/D2/D3/D4 for ownership-specific
integration. F2 adds only the conditional combined overflow test; no F7a or D1b-D1d prerequisite.

- [ ] Add bounded configured header sets for detached/error/fallback paths. Validate app-level
      sets at configuration; dynamic sets during normal prepare before installation. Invalid dynamic
      headers fail with the typed error while retaining the already-valid app set. Fallback executes
      no new application code and preserves response-scoped completion/deadline/abort decisions.
- [ ] Where F2 is present, add `generated_overflow_retry_after_precedes_synthesized_headers`
      covering renderer plus app-level and Fastly per-request sets. Final generated cap errors omit
      Retry-After and retain unrelated headers; ordinary upstream 413 retains its surviving value;
      config 503 retains its effective retry value (default 60). Do not globally ban the header from
      validated sets. Run `scripts/run_test_nonzero.sh generated_overflow_retry_after_precedes_synthesized_headers cargo test -p edgezero-core --lib`
      plus each adapter response contract; Expected: real red/green assertions, not a skipped
      cross-feature case. Record the combined leg only when both follow-up parts are adopted.
- [ ] Run core and four adapter response contracts; compare normal, fallback, admission refusal,
      normalized rejection, assembly failure, and postcommit failure paths. Preserve canonical Allow
      and effective config Retry-After policy. Add `synthesized_headers_preserve_detached_ownership`
      and run it with `scripts/run_test_nonzero.sh synthesized_headers_preserve_detached_ownership cargo test -p edgezero-core --features test-utils`.
      Expected red for missing policy, then bounded validation, preserved valid app headers and
      exactly-once completion pass. Apply the task lint card. Suggested commit:
      `feat: bound synthesized headers on every response path`.

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

## F9: Application Assembly and Axum Task Group

### Task F9a: Fallible Retained Application Assembly

**Files:** Core `src/app.rs`/`router.rs`; macros `src/app.rs`; four adapter `src/lib.rs` and
entrypoint/lifecycle modules; fixture apps and affected demo/template callers.

**Dependencies:** Existing fallible assembly baseline and P1's client contract; D1a/D2/D3/D4 for
affected lifecycle integration. F8 applies to declared-KV assembly-failure integration only.
No F9b or D1b-D1d prerequisite. These scheduling scopes do not change the release blocking set.

- [ ] Add failures-before-configure, registered-state identity/access, configure-from-state,
      manually-built-router policy/platform parity, success-only retention, and retry-after-failed-build
      tests. Hard-migrate route assembly to Result; no expect-based configuration wrapper.
- [ ] Expose typed assembled-app state access and one manual-router assembly path that applies
      name/configure/platform checks exactly once. Retain only complete successful apps, safe under
      overlapping requests. Standard Fastly serving answers/retries assembly failures through canonical
      detached delivery; custom callbacks can do so without an assembled app. Log once on boot target.
- [ ] Implement separately adopted macro fallible-state and standard CF/Spin failure-response
      SHOULDs only after applicability is recorded. Preserve Axum fail-before-bind behavior; do not
      retain the temporary failure router or require async route-building inside EdgeZero.
- [ ] Add `failed_app_assembly_retries_without_retaining_partial_state`; run it before/after the
      hard cut with `scripts/run_test_nonzero.sh failed_app_assembly_retries_without_retaining_partial_state cargo test -p edgezero-core --features test-utils`.
      Expected red for missing fallible/retained-state contract, then retry and successful-state
      identity pass. Run macros and four adapter assembly contracts plus applicable retained-instance
      fixtures and the task lint card. Suggested commit: `feat: assemble and retain only complete applications`.

### Task F9b: Policy-Aware Axum Servers and Test Helpers

**Files:** Axum `src/dev_server.rs`, `service.rs`, `test_utils.rs`, `src/lib.rs`, TCP/contract tests;
core/macros callers only where required to consume F9a's assembled app; fixture apps.

**Dependencies:** F9a and D2's local executor; independent of F6b's delivery report.

- [ ] Make programmatic Axum servers accept the assembled app through the same bounded ingress,
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

F9b's required new sentinel map (names are targets to add, not existing passing tests):

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

# Outbound Follow-Up Contracts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the remaining adoption requirements in scoped, separately verifiable changes without silently adding them to the production blocking set.

**Architecture:** Extend the existing request policies, body readers, application assembly, and egress coordinator. Reuse phase 2's head-first boundary and phase 3's owned progress engines. Keep response decoration/failure rendering bounded and keep provider diagnostics out of wire errors.

**Tech Stack:** Existing core/adapters/macros, portable timers, Fastly lifecycle and response ABI, native Axum local server, existing test utilities and fixture workspace.

---

Dependencies are task-specific. Scheduling a task here does not approve its promotion. When a part
lands, record confirmed-present parts and their environment legs under EZ-Q1.6/Q1.8. Conditional
SHOULD work, JS targets, and reusable-sandbox behavior retain their explicit applicability.

Task numbers group ownership, not an instruction to ignore dependencies. F1-F5 depend on their
phase 2 policies and phase 3 drivers; F7-F9 may proceed independently after those foundations.
For lifecycle integration execute F8 (KV) and F9 (assembly) before F6's final custom-send report
tests; F6's diagnostic types can land earlier. F10 requires the published P6 utility and includes
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
- [ ] Add a stable 413 category and typed inbound/upload origin, preserving public diagnostics and
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

## F3: Cache, TLS and Authority Task Group

F3a-F3c are independently closable tasks, not inline labels. None closes another requirement.

### Task F3a: Cache Tags

**Files:** Core `src/outbound.rs` and `outbound/image_optimizer.rs`; Fastly `src/outbound.rs`;
four adapter preflight/capability modules and `tests/contract.rs`; cache qualification fixture.

**Dependencies:** P1/P3/P5 and D1-D4; F5 parity only when fire-and-forget is adopted.

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

**Dependencies:** P1 and D1-D4 typed failure boundaries; does not require F8/F9a.

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

**Dependencies:** F6a, F8 and F9a for final assembly/KV failure-path tests, plus D1 callback ownership.

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

- [ ] Add one exact assertion per existing `ResponseEgressFramingError` variant:
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

## F7: Response Framing and Header Task Group

### Task F7a: Buffered Response Framing

**Files:** Core `src/response_egress.rs`, `response_egress_framing.rs`; four adapter
`src/response.rs`; Fastly `src/fastly_response_abi.rs`; fixture response cases.

**Dependencies:** Existing egress coordinator and D1-D4 ownership integration; independent of F7b.

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

**Dependencies:** Existing renderer/egress decisions and D1-D4. F2 adds only the conditional
combined overflow test; F7b does not require F7a.

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

- [ ] Add missing declaration versus declared-open-failure cases, provider StoreNotFound versus
      transient error, and unaffected status-route dispatch. Honor explicit `require_kv` where requested.
- [ ] Preserve failed declarations as typed handles; defer opening/failure to actual use where
      required. Do not retain transient availability errors as permanent absence or indiscriminately
      fail every request. Keep bounded extraction/secret/config behavior from the outbound baseline.
- [ ] Test failed read/write/delete/list behavior and recovery on a later request with the binding
      restored. Source/provider error payloads remain off the wire and response completion still runs.
- [ ] Run changed adapter unit/contract tests and required real runtime legs. Suggested commit:
      `fix: preserve declared KV bindings through provider open failure`.

## F9: Application Assembly and Axum Task Group

### Task F9a: Fallible Retained Application Assembly

**Files:** Core `src/app.rs`/`router.rs`; macros `src/app.rs`; four adapter `src/lib.rs` and
entrypoint/lifecycle modules; fixture apps and affected demo/template callers.

**Dependencies:** Existing fallible assembly baseline, P1's client contract and D1-D4 lifecycle
ownership. F8 is required for declared-KV assembly-failure integration only.

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
- [ ] Add `generated_core_tests_execute` to the ignored generated-project integration suite and
      run it before/after migration with
      `scripts/run_test_nonzero.sh --ignored generated_core_tests_execute cargo test --offline --locked -p edgezero-cli --test generated_project_builds`.
      Expected: generated core tests actually execute and assert the adopted API; a compile-only
      workspace pass is insufficient. Run core test-utils, CLI tests and the excluded demo tests
      from Q2; Q2 wires both generated and demo execution into CI. Apply the task lint card.
      Suggested commit: `test: align generated outbound examples with shared scripts`.

### Task F10c: Published Scripted Client on JavaScript

**Files:** Core test-utils Cargo feature gates and `src/outbound/testing.rs`; WASM test runner
setup and CI commands owned by Q2.

**Dependencies:** Published P6 and explicit JavaScript EZ-24.2 adoption; independent of F10b.

- [ ] Add a WASM-gated `scripted_client_uses_paired_js_fake_time` test for owned local futures,
      wakeable fake time, head/chunk timing and strict cutoff without provider emulation. Run the
      before/after command:
      `CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner scripts/run_test_nonzero.sh scripted_client_uses_paired_js_fake_time cargo test -p edgezero-core --features test-utils --target wasm32-unknown-unknown`.
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

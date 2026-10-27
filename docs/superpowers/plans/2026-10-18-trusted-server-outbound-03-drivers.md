# Adapter Outbound Progress Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make begun requests and batches progress eagerly, enforce bounded Fastly waits, and preserve arrival-time results and resource ownership on all four adapters.

**Architecture:** Use each adapter's existing transport and scheduling model with the shared policies/ledger from phase 2. Fastly gets one cooperative timed multiplexer; native Axum uses its established local executor, workerd uses host-local scheduling, and Spin uses WASIp3 spawning. Callback and response-owned exchanges have separate cancellation owners even when they share a client.

**Tech Stack:** Existing adapter features and target runners, Fastly async I/O handles, reqwest/Axum local runtime, worker fetch/AbortController, Spin SDK/WASI HTTP 0.3.

---

Dependencies: P1/P4 contracts; P2/P3/P5 integration accompanies each driver. G2 must pass before
depending on Fastly timed reads. Do not introduce a background reader that buffers outside caps.
Apply the index's scoped test/fmt/clippy card after each implementation increment, not only at
phase completion. The target checks below supplement, not replace, all five root CI gates.

**Scoped precedence, 2026-10-24:** the requester's [timing and native integration amendment](../../audits/2026-10-18-outbound-api-approval.md#scoped-timing-and-native-integration-amendment)
permits the Send-first checkpoint below before full D1a-D1d sequencing. Missing finite H(K) and
timing-only G2 failures do not block that subset. Original deadlines and late-result rejection,
+5 ms measured targets/tests, typed body errors and fresh-readiness/subsequent-read safety remain.
The optimizer blocking horizon is **UNKNOWN** until provider evidence. At that checkpoint,
B/backend policy and timers were unapproved. The later scoped B31 approval permits the existing
aggregate identity/timer subset described in D1b below; slack, resources and the readiness engine
retain their separate gates.
The current trait remains unchanged until the later atomic head-first/full-trait migration.

Each D1-D4 migration implements plan 02's synchronous `validate_supported_options` and paired
`time()` hooks as well as dispatch. Replace clock-only constructors per plan 02's migration table,
preserving Axum's fallible `try_with_time` and transport-sharing setup, and migrate request-context
injection. Reuse the adapter's own timer facility (native runtime,
Workers Delay, Spin sleep, or G2's verified Fastly cooperative wait). Test unsupported options with
a pending inbound body and zero timer/body/transport activity; on Axum/workerd/Spin also test a
200 ms outbound settlement timeout against an otherwise permanently pending 10 s inbound read.
Fastly's EZ-1.5 blocked-inbound-read exception stays explicit; no inert/default timer or core runtime
is an alternative. Fake-clock tests install a paired wakeable timer, not only a timestamp callback.

## D1: Fastly Driver Task Group

Execute D1a-D1d serially; D1 closes only when all four close. Each task has its own red/green
cycle and buildable commit. The public trait migration remains atomic across all adapters; these
internal slices do not authorize merging an incomplete public implementation or keeping old APIs.

### Task D1a: Bounded Multiplexer and Callback Ownership

#### Current-Operation Implementation Slice (2026-10-25)

The requester's [scoped multiplexer approval](../../audits/2026-10-18-outbound-api-approval.md#scoped-fastly-current-operation-multiplexer-approval-2026-10-25)
authorizes the existing Send/Batch receive-path replacement, fresh matching readiness, a 16 KiB
raw read maximum and rotating slot service under B31 and the original deadlines/cutoffs/caps.
Local/CI explicitly selects the tracked Viceroy repair; installed pins and hosted promotion stay
unchanged. Keep cached Send heads opaque rather than converting them to raw handles and changing
cache policy. Poll them nonblockingly with one shared positive 1 ms pulse.

Native deterministic scheduling tests and actual-client fixtures accompany this slice. D1a stays
open: this per-operation engine does not yet supply callback-wide ownership, progress during
unrelated operations or the eager public modes. Full P1/P4 and D1c/D1d integration, slack retirement,
resource evidence and hosted qualification remain separately tracked. No public trait or frozen
Interfaces/requirements change, acceptance-leg promotion or capability upgrade is implied.

#### Existing-Batch Scope Checkpoint (2026-10-26)

The [live batch ownership slice](2026-10-18-trusted-server-outbound-implementation.md#existing-batch-scope-checkpoint-2026-10-26)
adds fresh callback guards, helper fallback/reuse and separate transmission scopes. Private weak
registrations revoke whole current batch drivers, not only their progress slots. Retained handles
fail with typed Internal closure and preserve already-collected results; they do not become Cutoff.
Registry cleanup does not poll or drain work. D1a remains open for retained Send roots, transfer
ownership, callback-wide scheduling and the atomic eager/head-first modes. Current-operation
progress, B31, slack, original budgets, pins and capability disclosures are unchanged. No acceptance
leg or hosted qualification is promoted by the new local ownership regressions.

#### Mixed-Phase Prerequisite Checkpoint (2026-10-25)

The existing SDK/ABI diagnostic now appends eighteen separately validated probes: a ready
multi-buffer body alongside a permanently pending head, a delayed head alongside a pending body,
and a truncated multi-buffer body. Buffered and pull-streamed consumers run three repetitions at
16 KiB capacity. Require actual mixed domains, full-capacity reads, original deadlines, exact
payload checksums, fresh matching readiness and terminal source errors. Preserve the original
135-probe matrix and its failed historical replays. No production multiplexer, read maximum,
runtime-pin change or capability is approved by this experiment; continuously-ready multi-source
fairness and full D1a integration remain open.

**Files:** Modify `crates/edgezero-adapter-fastly/src/outbound.rs`; create
`src/outbound/progress.rs` and `src/outbound/backend.rs` only for the progress engine and backend
registry extracted by this change; update `src/lib.rs`, `src/request.rs`, `src/response.rs`,
`src/lifecycle.rs`, `tests/contract.rs`, and colocated lifecycle tests. The existing-batch scope
checkpoint extracts only its private ownership registry into `src/outbound/scope.rs`.
Callback ownership is
blocking EZ-6 work here, not deferred until F1/F6 reuse or delivery-report changes.

**Dependencies:** P1/P4, passing local G2, G3 and G4's API/Fastly-policy approval. Keep existing
backend identity and slack until D1b replaces them together; no new blocking-read fallback.

- [ ] Read G2's feasibility report and G4's API/Fastly-policy approval. Check source/SDK/interface
      digests and the literal configuration bound B. Adopt exactly the reported timed-read sequence,
      readiness predicates and maximum read size. If any dependency is failed/missing/stale, follow
      G2's cancelled/deferred map; do not implement a guessed host primitive or remove slack.
- [ ] Add tests for one active slot plus a trickling sibling, two ready bodies, earliest slot
      deadline, cutoff with a permanently pending body, handle reassociation failures, and cleanup.
      Assert exact indices/reasons and preserved partial results; do not use preflight-only cases as
      harvest coverage. Run the Fastly native `test-utils` contract suite to demonstrate failures.
      Name the sibling case `fastly_multiplexer_preserves_sibling_progress` and run it with
      `scripts/run_test_nonzero.sh fastly_multiplexer_preserves_sibling_progress cargo test -p edgezero-adapter-fastly --features test-utils`.
      Expected red before implementation; after implementation, real indexed outcomes and owner
      counts pass. Repeat the timed-read fixture via G2, then run the index's task lint card.
- [ ] Use G2's verified timed-read primitives for header readiness, body readiness, and bounded
      reads. Wait on all active request-lifetime exchanges and the earliest relevant timer. Bound
      bytes/work per turn; yield between ready sources. While an origin read or ordinary send waits,
      keep harvesting bidder progress; never enter an uninterruptible response-body read.
- [ ] Keep transport resources in an owned registry with response/callback ownership tokens;
      no path/query/header/body survives request lifetime. A response body retains its exchanges
      after handoff; callback-owned work survives earlier response cleanup. Drop/cancel removes
      only the corresponding exchanges and retains precise SDK-selection failures as driver errors.
- [ ] Install one fresh adapter-private RAII callback guard in `Sandbox::handle`, immediately
      around the handler invocation shared by `run_custom` and every `serve_custom` callback.
      Bind scope membership per operation, not at client construction; saved clients must resolve
      the current scope. Callback exit (Ok, Err, or supported unwinding) releases callback-owned
      work exactly once even when clients/handles remain retained elsewhere. Do not retain the
      guard in sandbox state or wait for sandbox destruction. Preserve the original return/error
      and existing no-synthetic-send behavior. Response-owned work has its separate egress owner.
- [ ] Standalone canonical send helpers and `FastlyService::send_request_with_hooks` establish
      a helper-local fallback scope only if no outer callback scope exists. Under custom wrappers
      they reuse the outer guard: helper return and response completion cannot cancel post-delivery
      callback work. `response.rs` ends only the response-owned scope. New exchanges and completed
      streamed-body transfers keep the existing exactly-once teardown owner.
- [ ] Add `callback_scope_exit_releases_retained_exchange` and
      `retained_client_resolves_fresh_callback_scope` in lifecycle/contract tests. Cover retained
      pending handles, Ok/Err/pre-egress setup failure, supported unwinding, response cleanup while
      same-client callback work is live, and post-delivery new work. Unit-simulate two invocations
      on the same sandbox/client even when hosted reuse is disabled; previous scope handles cannot
      restart or deliver old work. Actual next-request reuse qualification remains conditional.
      Run `scripts/run_test_nonzero.sh callback_scope_exit_releases_retained_exchange cargo test -p edgezero-adapter-fastly --lib`
      and the actual Viceroy sentinel; Expected: no early cancellation, no extra send, exactly one
      release at callback exit, and no reuse of a stale scope. Apply the task fmt/clippy card.
      Suggested commit: `feat: multiplex bounded Fastly exchanges with scoped ownership`.

### Task D1b: Stable Backend Identity and Slack Removal

**Files:** Fastly `src/outbound.rs`, `src/outbound/backend.rs`, `src/context.rs`,
`src/request.rs`, `src/lifecycle.rs`, `tests/contract.rs`, and fixture Fastly qualification cases;
create `tests/outbound_public_api.rs` for the external observation compile contract.

**Dependencies:** D1a's verified bounded waits and G4's literal B/domain approval.

#### Existing-Path B31 Checkpoint (2026-10-25)

The [scoped B31 approval](../../audits/2026-10-18-outbound-api-approval.md#scoped-b31-implementation-approval)
permits the aggregate-policy backend portion on the current Send/Batch transport before D1a.
Configured timeout, not shortened remaining budget, selects the clamped B31 anchor. All three
provider timers use the same ceiling-rounded value; the original logical budget stays unchanged.
Replace per-client namespaces/caches with one verified guest-instance registry and versioned
configuration names. Compare full identities, serialize lookup/registration/insertion, retain late
successful registrations and fail closed on unknown `NameInUse`. No public API or body clone is
needed; request parts carry the existing timeout through the same preparation path.

The real WASI regressions cover all 93 boundary samples, equal timeouts under different deadlines
and cutoffs, maximum clamps, native cross-client reuse, collisions, failed retries and late-cache
retention. This implements only the current aggregate identity/timer subset, not all D1b modes or
acceptance criteria. Registration observation, explicit Connect/idle mapping, retirement, the
readiness engine, slack removal and hosted/resource qualification remain open. The existing two
dispatch guards and their tests remain, not an alternate old backend policy.

- [ ] Add `backend_identity_ignores_remaining_budget` for equal per-call timeouts under different
      absolute deadlines, with negative controls for connect/TLS/authority changes. Run
      `scripts/run_test_nonzero.sh backend_identity_ignores_remaining_budget cargo test -p edgezero-adapter-fastly --features test-utils`.
      Expected red for exact-remaining-budget identity at F; after implementation the equal keys
      and distinct controls pass without changing logical deadlines or the approved policy domain.
- [ ] Replace client/exact-budget backend names with deterministic versioned configuration keys.
      Include origin, authority and TLS/phase settings; omit request/client/sandbox identifiers.
      Equal per-call timeouts remain eligible under differing remaining budgets. Deadline-only
      configurations use the G4-approved policy and literal maximum B; its documented timer
      boundaries/cardinality proof must cover all valid budgets without shortening a deadline.
      Keep explicit connect/idle limits distinct from aggregate deadlines; Image Optimizer is not
      an identity discriminator. Detect inconsistent name reuse; never trust `NameInUse` blindly.
- [ ] Add request-local registration count/config-count/total/max duration and >=50 ms warning.
      Implement plan 02's G4-approved `FastlyOutboundObservation` and `FastlyOutboundStats` contract.
      Install the extension handle before prepare, bind registration accounting per operation to
      D1a's scope, and keep the read-only handle independent of cleanup ownership. Freeze numeric
      accounting on callback/helper-fallback exit, not response completion or client drop.
      Charge deadline from original call entry, retain a successfully registered backend even if
      registration returns late, and return attributed 504 without sending an expired slot. Remove
      slack guards, constant, hook, and tests only together with verified bounded waits. Preserve
      router-state > prepare-hook > default-client precedence and cross-request Axum pooling.
- [ ] Add `registration_observation_public_api` in the new external test file; compile every
      getter plus Clone/Send/Sync handle bounds and Copy/equality snapshot bounds without private
      constructors or a Fastly SDK dependency. Run
      `scripts/run_test_nonzero.sh registration_observation_public_api cargo test --offline --locked -p edgezero-adapter-fastly --no-default-features --features test-utils --test outbound_public_api`.
      Expected: missing/unusable declarations fail compilation, then a nonzero native pass.
- [ ] Add `request_registration_observation_follows_scope` to native contract tests and repeat
      real provider accounting on the Q2 WASI/fixture legs. Run the sentinel using the index's
      exact Fastly native feature card. Expected: default, router-state and prepare injection see
      the same request's counters; same-key cache hits, failed/late registrations and retries keep
      the specified count/durations. Check zero-activity preflight, post-delivery work, helper reuse,
      Ok/Err/unwind exit and retained handle/client isolation across two callbacks. Retaining a
      handle retains only its frozen numbers, not an old exchange or configuration-key set.
- [ ] Add `registration_warning_obeys_fifty_millisecond_boundary`; run
      `scripts/run_test_nonzero.sh registration_warning_obeys_fifty_millisecond_boundary cargo test --offline --locked -p edgezero-adapter-fastly --no-default-features --features test-utils --lib`.
      Use an injected clock and recording logger for just below 50 ms, exactly 50 ms and above,
      including a failed registration. Expected: no warning below the boundary, one warn record
      at/above it, correct request-local count/total/max observations, and unchanged wire errors.
      This is EZ-2.13's acceptance proof, not a hosted registration-latency threshold.
- [ ] Run native backend/registration contracts and the real G2 readiness suite after removing
      slack; deadline, cutoff and sibling-progress sentinels must still execute nonzero. Apply the
      task fmt/clippy card. Suggested commit: `feat: stabilize Fastly backend identity and remove proved slack`.

### Task D1c: Connect Timeout Mapping

**Files:** Fastly `src/outbound.rs`, `src/outbound/backend.rs`, `tests/contract.rs`,
and fixture qualification/hosted probe cases from G2.

**Dependencies:** D1b and P1/P5; independent of Image Optimizer horizon/entitlement approval.

- [ ] Add `connect_timeout_reaches_provider_settings` and
      `connect_expiry_preserves_governing_budget_precedence` from plan 02. Exercise Send/Begin/Batch
      and permitted Follow hops; observe the actual platform mapping, full response-head budget,
      ConnectTimeout before the governing expiry, and its original source at equality/late observation.
      Run each via `scripts/run_test_nonzero.sh <sentinel> cargo test -p edgezero-adapter-fastly --features test-utils`.
      Expected red for F's split/missing option, then nonzero pass. Hosted EZ-4 criterion 8 separately
      proves real expiry; Viceroy setter/classifier tests do not establish that provider guarantee.
- [ ] Implement Fastly connect timeout without a fixed connect/first-byte split. Run native contracts
      and `CARGO_TARGET_WASM32_WASIP1_RUNNER='viceroy run' scripts/run_test_nonzero.sh connect_timeout_reaches_provider_settings cargo test --locked -p edgezero-adapter-fastly --no-default-features --features fastly,test-utils --target wasm32-wasip1 --lib`.
      Expected: actual mapping assertions execute and the 900 ms head under a 1 s budget succeeds.
      Keep hosted connect expiry pending until Q4 returns evidence. Apply the task lint card.
      Suggested commit: `feat: map explicit Fastly connect timeouts`.

### Task D1d: Image Optimizer Single-Send Mapping and Horizon

**Files:** Fastly `src/outbound.rs`, `src/outbound/backend.rs`, `tests/contract.rs`,
and fixture qualification/hosted probe cases from G2.

#### Send-First Checkpoint (2026-10-24)

This is an implementation sequencing decision under the [scoped integration authorization](../../audits/2026-10-18-outbound-api-approval.md#scoped-timing-and-native-integration-amendment),
not a claim that the requester explicitly approved different signatures. Its scope is bounded;
the synchronous optimizer wall time is not. Do not wait for full D1c/P3/P5, finite H(K) or a
timing-only G2 pass to implement this subset. Do not mark full D1d or its acceptance legs complete.

**Timing anchor:** the current trait's production async `send` selects its budget at existing
adapter method entry on first poll, not at facade invocation. Preserve that selected budget before
SDK preparation/registration and across the native call. Facade-invocation capture remains pending
with P1's later atomic head-first/full-trait migration; this checkpoint does not complete P1.

**Checkpoint files:** core `outbound.rs`, `outbound/image_optimizer.rs` and the external request
API tests; each adapter's existing outbound validation/dispatch and contract tests; Fastly's
direct SDK mapping. Do not change `src/outbound/backend.rs`, backend policy, phase timers or slack.

- [x] Attach existing typed optimizer values to `OutboundRequest`/parts with builder/readback and
      lossless `into_parts`/`from_parts` round trips. Keep `OutboundHttpClient`'s current trait
      unchanged. Add no default, alias, compatibility helper or alternative transport.
- [x] Validate support first in all four adapters before budget rejection, body polling, backend
      registration or dispatch. Axum/Cloudflare/Spin reject options; Fastly Batch rejects only its
      option-bearing slot and allows siblings to proceed. Only Fastly single Send is integrated
      here; Begin, Follow and fire-and-forget remain outside this checkpoint, not silently enabled.
      Preserve method/body/default-cache restrictions; do not claim full EZ-1 settlement is done.
- [x] Map typed values directly to native Fastly SDK options in the existing `execute` single-Send
      branch and use existing `process_response`. Leave the ordinary SDK send branch unchanged.
      Preserve built targets/signature-bearing headers, header/body/decoder limits, lazy streamed
      bodies and exactly-once cleanup. No extra fetch, option serialization layer or prebuffering.
- [x] Keep the budget selected at existing adapter method entry on first poll, including its
      `t0`, `D` and `BudgetSource`, through SDK preparation/registration and host return. Do not
      claim facade-invocation capture. Reject
      observed pre-send expiry without dispatch. At/after `D`, discard host success and classify
      host failure as the original attributed 504. Before `D`, map SDK
      `SendErrorCause::ImageOptimizerUnsupported` to a 502 with
      `BadGatewayReason::ImageOptimizerUnavailable` on core's non-exhaustive reason enum. Preserve
      the existing message `Fastly outbound request failed` exactly; only the typed reason and
      provider detail distinguish this cause, without provider-text parsing. State blocking horizon
      **UNKNOWN**; infer no finite bound from SDK timers or an unsupported Viceroy response.
- [x] Demonstrate nonzero red/green tests for attachment/parts round trips, support-first rejection
      with expired budgets and unread bodies, batch sibling isolation, direct typed SDK mapping,
      `BadGatewayReason::ImageOptimizerUnavailable` 502 versus late-504 precedence, retained provider
      detail, lazy response limits and exact cleanup. Run the
      index's scoped test/fmt/clippy card and actual applicable WASM contracts. Typed body-error
      preservation and fresh-readiness/subsequent-read safety remain requirements; no lost-error
      EOF conversion or blocking-body fallback is permitted. Retain +5 ms targets and failed traces.

**Local evidence boundary:** core request/parts option readbacks and exports are stable. Static
inspection confirms that optimizer options select native SDK `.send`, not `.send_async`; this is
not runtime host-entry evidence. `image_optimizer_unavailable_is_typed_on_viceroy` proves the option
is not dropped and unavailable errors are typed, but cannot prove host entry: SDK `.send_async`
can return the same `ImageOptimizerUnsupported` without entering the host. Successful shared-response
cap/laziness checks use synthetic successful SDK responses, not hosted image transformation.
Neither test category proves runtime optimizer host entry or hosted transformation; broader
verification and qualification remain separate.

Full EZ-1 settlement, Follow, B/backend policy, the new engine and head-first/full-trait migration
remain open. The later trait cut still migrates every implementer/caller atomically, not through
compatibility paths. Hosted/performance qualification, numerical horizon/policy approval and
deployment remain blocked. This checkpoint asserts no optimized hosted result or performance parity.

#### Remaining Full D1d Work

The dependencies and earlier prerequisite statements below apply to completing full D1d, not to
the Send-first checkpoint. Read finite-horizon and G2 timing language under the scoped override;
provider evidence/numerical approval remain qualification gates, while B and read-safety gates persist.

**Dependencies:** D1c, P3/P5 and G4's separately approved EZ-8.10 horizon. Missing approval stops
this task; missing hosted entitlement leaves its production qualification blocking, not passed.

**Native-parity scope approved by the requester, 2026-10-24:** use generic typed request options
and Fastly's native optimizer, retaining the existing outbound pipeline. The source inspection
used TS `f9281917a7260b723f5f22175911c6d2147ef650`; its optimizer DTO/profile/adapter files match
the `a9037770a` baseline. This inspection is not a new qualification baseline or a passing run.
The approval supplies neither numerical H(K)/B nor performance thresholds, entitlement or
deployment authorization. Runtime integration still waits for D1c/P3/P5, G2 and numerical B/H(K)
approval. Measurement approval and staging authorization separately gate hosted comparison.

- [ ] Recheck D1c/P3/P5, the current attributed API approval plus scoped amendments, passing G2
      and the separately approved numerical H(K)/B records. Stop before production changes if
      any prerequisite is missing. Preserve all baseline outbound fields and behavior; no old-trait
      helper, separate image client, ignored options or software-optimization fallback is allowed.
- [ ] At the atomic request/driver cut, add `image_optimizer_options_round_trip` in core
      `outbound/image_optimizer.rs` and the external public API test. Exercise all selectors,
      query preservation, replacement, `into_parts`/`from_parts` and prepared redirect transfer.
      Run `scripts/run_test_nonzero.sh image_optimizer_options_round_trip cargo test --locked -p edgezero-core`.
      Expected: missing request attachment is red, then exact option readback passes without
      changing signed targets/headers. Leave query/profile parsing and aliases in application
      configuration; validate a missing required region before SDK serialization rather than
      inventing a default or letting an SDK-built invalid option reach the host.
- [ ] Put the direct SDK value mapping in Fastly `src/outbound/image_optimizer.rs`, called from
      the existing Send/Follow dispatch, not a second transport. Add colocated
      `image_optimizer_native_mapping_matches_sdk` cases for seven regions, eight formats,
      five filters, quality boundaries, pixel dimensions, all crop modes and explicit false/true
      query preservation. Run
      `scripts/run_test_nonzero.sh image_optimizer_native_mapping_matches_sdk cargo test --locked -p edgezero-adapter-fastly --features fastly,test-utils`.
      Expected: missing mapping is red, then exact SDK-field assertions pass. Enum matches and
      numeric widening must not allocate a string/JSON/query intermediate; SDK-internal
      serialization remains the SDK's work. The non-optimizer dispatch branch does not construct
      or clone optimizer options.
- [ ] Add `image_optimizer_stream_is_lazy_and_releases_once` and
      `image_optimizer_backend_identity_matches_plain_send` at the scripted provider boundary.
      Before first application body polling, assert zero payload reads, one send and unchanged
      transport configuration identity; after polling, assert bounded chunks, existing caps,
      typed errors and exactly-once release on completion/drop/failure. Cover HEAD/204/205/304
      suppression and a permanently pending/truncated stream. Run each via
      `scripts/run_test_nonzero.sh <sentinel> cargo test --locked -p edgezero-adapter-fastly --features fastly,test-utils`.
      Scripted identity/count proofs are not hosted pooling or performance evidence.
- [ ] Keep TS-specific policy outside core: profiles, SVG selection, query stripping before
      signing, SigV4 and conditional S3 HEAD/raw-error GET stay with the application. Add no
      automatic HEAD/retry/fetch. Update the generic attachment/streaming guide and applicable
      demo/generated examples only with the working request API, and compile them in the same
      hard migration. Do not present an unsupported target as serving optimized images.

- [ ] Add `image_optimizer_follow_uses_single_send_mapping` at the Fastly boundary and run it
      with `scripts/run_test_nonzero.sh image_optimizer_follow_uses_single_send_mapping cargo test -p edgezero-adapter-fastly --features test-utils`.
      Expected red for missing mapping, then Send/Follow preserve options and Begin/Batch reject
      before provider/body work. Implement Image Optimizer single-send mapping. Use the typed
      entitlement reason even if EZ-16 is deferred. Preserve the exact built URI/signature-bearing
      headers at the provider boundary; host query transformation is separately characterized.
      Image Optimizer body handling remains lazy and uses shared policies after its response head.
- [ ] Before Image Optimizer integration, read G4's separate approved EZ-8.10 horizon contract.
      Implement H(K) lookup from the actual stable backend policy, not remaining-budget-derived
      identity or an assumed two-timer sum. Add `image_optimizer_horizon_preserves_original_deadline`
      with nanosecond/millisecond boundaries, equal timeout/different deadlines, late preparation,
      host success/error at D, and post-head pending reads. Report original-entry, preparation and
      host-call timings independently; a pre-dispatch timeout is not evidence of host-call boundedness.
      Run the named sentinel under native scripted-host tests and actual hosted qualification;
      Viceroy's entitlement rejection is not a successful Image Optimizer horizon test.
- [ ] Run native contracts and the actual WASI contracts:
      `CARGO_TARGET_WASM32_WASIP1_RUNNER='viceroy run' scripts/run_test_nonzero.sh backend_creation_error_table_is_exhaustive cargo test --locked -p edgezero-adapter-fastly --no-default-features --features fastly,test-utils --target wasm32-wasip1 --lib`.
      Add equivalent nonzero sentinels for successful dispatch/harvest and `outbound-readiness`.
      Complete EZ-2/3/4/6/8 loopback legs; hosted gates remain explicitly pending until run.
      Hosted EZ-2 criterion 14b must prove fresh-execution connection reuse using origin connection
      IDs and an SDK-only control. Registration counts, same POP and resumed TLS alone do not prove
      pooling. Keep existing M1 handshake-rate measurement and M4's separate reuse-mode scope.
      Apply the task fmt/clippy card. Suggested commit: `feat: map Fastly optimized single sends with approved horizon`.

## Task D2: Native Axum Eager Exchanges

**Files:** Modify `crates/edgezero-adapter-axum/src/outbound.rs`, `request.rs`, `service.rs`,
`response.rs`, `dev_server.rs`; tests in `tests/contract.rs` and colocated modules.

- [ ] Add `begin_dispatches_without_collection` and `unpolled_batch_dispatches_all_slots` using
      loopback origins plus the existing local runtime. Include a missing-runtime-context case that
      returns a typed setup error before network activity rather than panicking.
- [ ] Install an adapter-owned local scheduling context through the existing `LocalSet` serving
      path and explicit manual-client construction. Spawn local exchange work before returning a
      batch/begun handle; do not use `tokio::spawn` with core's `!Send` body/futures or unsafe Send
      wrappers. Return response data through bounded local state while preserving shared reqwest
      cross-request pooling and caller-supplied client precedence.
- [ ] Record buffered completion/cap/error at the actual observed terminal event and streamed
      begin at the head. Race reads against deadlines, retain successful early arrivals for late
      collection, enforce strict cutoff equality, and use shared head-first disposition. Dropping a
      handle cancels its task and transport chain; detached runtime work must not retain a client cycle.
- [ ] Integrate distinct callback/body cleanup with `AxumEgressBody` and disconnect/write/source
      terminal paths. Run `cargo test -p edgezero-adapter-axum --features axum,test-utils` plus TCP
      forwarding/cancellation cases. Assert healthy recovery and zero retained exchange owners.
      Suggested commit: `feat: drive native outbound exchanges eagerly`.

## Task D3: Cloudflare Eager Exchanges

**Files:** Modify `crates/edgezero-adapter-cloudflare/src/outbound.rs`, `request.rs`, `response.rs`;
tests in `tests/contract.rs` and existing WASM-gated module tests; workerd cases belong in
`tests/fixtures/reusable-app/crates/fixture-cloudflare/src/lib.rs`, fixture-core qualification,
and fixture-harness `net.rs`/`runners.rs`.

- [ ] Add workerd cases for begin/batch dispatch during an unrelated awaited timer, arrival during
      origin streaming, and a buffered cap failure while no result is being collected. Demonstrate
      failures before replacing lazy futures. Keep browser-only body behavior separate from workerd.
- [ ] Spawn exchange work with the provider's local scheduling primitive and keep one abort owner
      from setup through streamed body lifetime. Preserve manual redirects, raw response encoding,
      multi-value headers, token-safe diagnostics, frozen-clock fairness, and event-loop yield quotas.
      Do not treat handing a future to a Rust container as proof it is executing.
- [ ] Hand response-owned exchanges through the existing egress coordinator. Clean transport/body
      completion disarms the abort guard and releases its ownership exactly once without aborting
      completed work. Disconnect, deadline, source/conversion failure, cancellation, or drop before
      clean completion stops guest polling and aborts still-armed unfinished work at most once,
      then releases ownership exactly once. A previously aborted/disarmed guard cannot abort again;
      retained callback work has a separate owner. Test each terminal path, including setup failure
      after controller creation and streamed-body ownership transfer. Track arrival before collection
      and keep the shared driver validator. Local tests do not upgrade provider guarantees.
- [ ] Run native `test-utils` contracts and the repository's pinned browser/WASM contract command
      from `.github/workflows/test.yml`, with nonzero new sentinels. Run
      `scripts/smoke_test_reusable_app.sh --adapter cloudflare --suite smoke` after extending the
      qualification fixture cases above. That Wrangler-local/workerd path, not the browser runner,
      supplies the required Workers scheduling/lifetime evidence.
      Suggested commit: `feat: drive Workers outbound exchanges eagerly`.

## Task D4: Spin Eager Exchanges

**Files:** Modify `crates/edgezero-adapter-spin/src/outbound.rs`, `request.rs`, `response.rs`;
tests in `tests/contract.rs` and `tests/sdk_resources.rs`.

**Existing-path checkpoint, 2026-10-25:** Connect setter refusals now remain advisory under the
unchanged aggregate race. The SDK matrix covers every outcome; the async local Spin progress
fixture proves ready success, actual pending timer wakeup, expired-entry zero polling and owner
release across all existing budget sources. First-byte/between-bytes typed setup failures remain.
This is only the refusal-handling part of the third checkbox below, not configured connect
mapping, eager/head-first integration, hosted expiry proof or completion of D4. See the
[master checkpoint](2026-10-18-trusted-server-outbound-implementation.md#spin-connect-refusal-checkpoint-2026-10-25).

- [ ] Add real SDK-resource cases for unpolled begin/batch dispatch, pending header/body deadline,
      original coding, and response handoff. Run the pinned SDK-resource runner, not a compile-only
      `wasm32-wasip2` check or a native test-double substitute.
- [ ] Use `spin_sdk::wasip3::spawn` for eager exchange progress. Preserve the upload/send state
      machine, response-completion future, and distinction between clean EOF and truncated transport.
      Use the existing timer race for permanently pending operations. Preserve typed provider errors
      and source release under every cancellation arm.
- [ ] Pass configured connect timeout independently and accept all documented setter refusals as
      BestEffort fallback to the aggregate budget. Do not modify authority to implement the unsupported
      override. Integrate body/callback owner cleanup, head-first response policy, shared arrival ledger,
      and actual ingress GET/HEAD settlement from P2.
      Add `connect_timeout_reaches_provider_settings` with a recording setter and every refusal
      variant, and execute it through `run_test_nonzero.sh` in the SDK-resource suite. The recorded
      setting uses a configured timeout shorter than the aggregate budget, not a split of that
      budget. Add an unset/longer-than-budget control following EZ-4.5; refusal keeps aggregate
      enforcement and its typed source. Repeat the controlled-drop runtime case from EZ-4.
- [ ] Run `cargo test -p edgezero-adapter-spin --no-default-features --features test-utils` and
      `CARGO_TARGET_WASM32_WASIP2_RUNNER='wasmtime run -W component-model-async=y -S p3=y -S http=y' scripts/run_test_nonzero.sh spin_error_code_table_is_exhaustive cargo test --locked -p edgezero-adapter-spin --no-default-features --features spin,test-utils --target wasm32-wasip2 --test sdk_resources`.
      Extend named sentinels for the new paths and run the Spin smoke. Keep teardown BestEffort
      unless authorized hosted evidence establishes more. Suggested commit: `feat: drive Spin outbound exchanges eagerly`.
      Also run CI gate 5 explicitly:
      `cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin`.
      Expected: target compilation succeeds with the real Spin dispatch/spawn path enabled.

## Task D5: Cross-Adapter Precedence and Send-Mode Parity

**Files:** Four adapter `tests/contract.rs`, core `src/outbound.rs` tests, and the qualification
fixture. Fix only the corresponding adapter implementation when a parity case fails.

- [ ] Add `send_modes_preserve_options_and_failure_precedence` to each native adapter contract.
      Run it with `scripts/run_test_nonzero.sh` using the index's four exact native feature cards,
      replacing the existing sentinel name only. Expected: red for missing mode propagation, then
      identical effective options/typed outcomes and zero activity on unsupported preflight.
      Repeat the same named cases under Q2's SDK target cards and required real-runtime fixtures;
      a passing native double cannot close a target leg. Apply the task lint card.

- [ ] Run each changed option through single send, begun request, batch slot, and redirect hops
      where permitted; later include fire-and-forget only if implemented. Exercise client marker defaults
      and overrides, all caps, connect limits, coding choices, and Image Optimizer rejections.
- [ ] Assert one unsupported-option category wins before expired budget, GET/HEAD body reads,
      and backend/network work. Valid siblings proceed. A timeout cause alone never abandons a batch;
      strict observation cutoff and actual clock determine termination.
- [ ] Compare one-slot batch and single send cache/validation/error behavior. Prove no-redirect
      following parity for exact/overflow identity, compressed, and original-coded responses. Run the
      header-first followed-body versus final-body cases through actual normalization and supplied clients.
- [ ] Run core plus all native adapter contracts, real target contracts, dependency audits, and
      docs/legacy gates. Do not merge until all required no-regression sentinels execute nonzero.
      Suggested commit: `test: enforce outbound send-mode and cutoff parity`.

## Task D6: Lifetime and Late-Collection Qualification

**Files:** Fixture-core qualification cases, four fixture entrypoints, fixture-harness `net.rs`,
`runners.rs`, `evidence.rs`, `main.rs` and suite documentation; adapter-local owner tests.

- [ ] Add `late_collection_preserves_arrivals_and_response_ownership` to core and applicable
      adapter contracts. Run
      `scripts/run_test_nonzero.sh late_collection_preserves_arrivals_and_response_ownership cargo test -p edgezero-core --features test-utils --lib`
      and the index/Q2's native/SDK cards with that sentinel. Expected: red for late-observation loss,
      then on-time outcomes survive late collection, cutoff equality stays unresolved and response/
      callback ownership releases exactly once at its own boundary. Apply the task lint card.
- [ ] Add an `outbound-lifetimes` suite to the harness before invoking
      `scripts/smoke_test_reusable_app.sh --adapter all --suite outbound-lifetimes`.
      Expected: nonzero G1-mapped applicable EZ-6 cases per adapter, with exact outcomes/timings and
      owner counts below. Reuse/hosted conditions stay separately pending until their own runs;
      the suite does not skip a required Fastly case merely because G2 failed.

- [ ] Implement all EZ-6 scenarios 1-23, with reuse scenario 10 and scenario 23's actual next-request
      reuse leg separately conditional. Include
      origin head 700 ms, bids 180/320/900 ms, batch start 34 ms and cutoff 534 ms; retain the first
      two bids and report the third unresolved. Use injected clock for exact unit assertions.
- [ ] Check origin flow while a bidder body is pending; cutoff returns by 539 ms rather than
      waiting to 600 ms. Cover own timeout before cutoff, CPU-stretch observation, late collection,
      repeated termination, and cancellation reporting recorded-but-undelivered indices.
- [ ] Prove asynchronous response handoff retains body-owned work, egress end releases it once,
      and an already-pending callback-owned exchange on the same client survives that cleanup.
      Preserve Fastly post-delivery work and every documented host-cancellation limitation.
- [ ] Exercise both Fastly lifecycle wrappers with the callback-scope return/error cases, not
      merely a manually dropped pending handle. Observe real cancellation/release with a retained
      handle and assert that helper return does not end the outer callback scope.
- [ ] Run applicable local and hosted legs under the G1 inventory. Runtime assertions must count
      real progress and terminal notifications; do not use sleeps alone as evidence of ownership.
      Suggested commit: `test: qualify outbound arrival and lifetime contracts`.

## Phase Exit

- [ ] Execute the index's shared gates plus Q2's explicit native/WASM feature matrices and the
      D1-D6 named runtime commands. Record nonzero test counts and the G1 inventory entries; a
      passing command cannot substitute for G2/G4 approvals or missing hosted legs.
- [ ] Existing outbound semantics remain green; no lazy adapter path remains behind eager APIs.
- [ ] Fastly timed readiness is proved, not inferred; registration/upload/settlement/Image Optimizer
      exceptions stay narrowly documented, never widened to response-body reads.
- [ ] Every required mode has one transport, one response policy, and exactly one teardown owner.

# Trusted Server Outbound Adoption Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Trusted Server adoption requirements without regressing the outbound work already implemented in PR 275.

**Architecture:** Extend the existing portable outbound policies and adapter-owned exchanges; do not introduce a second transport. Use one head-first dispatch boundary for plain sends, eager exchanges, and explicit redirect following, with independent callback and response-body ownership. Prove the pinned Fastly timed-read boundary before replacing its dispatch and harvest engine.

**Tech Stack:** Rust 1.98.1, existing Cargo features, Fastly SDK 0.13.1 and Viceroy 0.21.0, worker 0.8.5/workerd, Spin SDK 7.0.0/WASI HTTP 0.3 and Spin 4.1, reqwest 0.13.4, existing native Axum/Hyper runtime.

---

## Status and Authority

### Scoped Timing and Send-First Override (2026-10-24)

The requester's [scoped approval](../../audits/2026-10-18-outbound-api-approval.md#scoped-timing-and-native-integration-amendment)
supersedes earlier preimplementation finite-H(K) and timing-only G2 blockers below. Original
logical deadlines, sources and late-result rejection remain exact; +5 ms stays a measured
qualification target with unchanged tests and negative evidence, not a hard scheduler guarantee.
The synchronous optimizer blocking horizon is explicitly **UNKNOWN** until provider evidence.
Hosted/performance qualification remains blocked; no historical experiment is relabeled.

The [D1d Send-first checkpoint](2026-10-18-trusted-server-outbound-03-drivers.md#send-first-checkpoint-2026-10-24)
is an implementation sequencing decision under that authorization, not approval of different
signatures. It attaches typed options to requests/parts, validates support first on all four
adapters, and maps Fastly single Send directly through existing `execute`/`process_response`.
Keep the ordinary SDK send branch and current trait unchanged; introduce no separate transport,
compatibility helper, default or alias. Typed optimizer-unavailable 502 and post-return original
attributed 504 are required, with existing limits, lazy streams, cleanup and target/header fidelity.

Budget selection still occurs at existing adapter async `send` method entry on first poll, before
SDK preparation/registration; the checkpoint preserves that selected budget across the native call.
It does not introduce facade-invocation capture or complete P1. Invocation-time capture remains
pending with the later atomic head-first/full-trait cut, alongside the existing open engine work.

Full EZ-1 settlement, Follow, B/backend policy, the new engine and atomic head-first/full-trait
migration remain open. This subset does not depend on completing D1c/P3/P5 or full G2 first,
but does not complete those tasks. B/domain/cardinality/resource approval, backend policy,
timers and slack are unchanged; no backend-policy implementation is enabled. Typed body-error
preservation and fresh-readiness/subsequent-read safety remain requirements. Earlier checkpoint
prerequisite lists, the EZ-8.10 draft summary and Decisions Before Runtime Changes are read with
this override. The historical execution/checkpoint records follow unchanged.

### Send-First Implementation Checkpoint

- [x] Attach/read back typed optimizer settings through request parts without changing targets
      or signature-bearing headers. Demonstrate missing-API red and passing round trips.
- [x] Reject unsupported adapters/batch modes before ordinary validation, body polling or budget
      rejection. Prove a rejected Axum image slot leaves its live plain TCP sibling eligible.
- [x] Map every portable selector directly to pinned SDK fields; use native synchronous Send only
      for attached options and retain the ordinary async branch and existing backend policy.
- [x] Preserve selected deadlines through native return and shared response handoff, including
      equality, late-error detail and release of late successful resources. Retain exact public
      error text while exposing typed optimizer unavailability.
- [x] Retain shared body/header limits and lazy streaming; align the Fastly-only demo/scaffold
      route and capability publication. Keep all historical failed qualification evidence unchanged.

Local verification passes 2,233 workspace tests (one existing ignored build test), all five root
Rust gates, target lints, all 193 Fastly WASI unit cases and two focused optimizer contract cases,
384 native Fastly `cli,test-utils` tests, generated-project builds/tests, demo tests, fixture
replays and documentation guards. These are implementation checks, not hosted transformation,
deadline-tail or performance certification. The source-selection proof is static; the Viceroy
unavailable result alone cannot prove optimizer host entry. The full P1/D1d cut remains open.

Two read-only investigations compared Send-first integration with another disconnected mapping
foundation. The selected path keeps one existing transport and preserves the pending atomic
trait replacement. An independent final review checked mapping, unsupported-option precedence,
deadlines, ownership, caps and generated routing; its message/provenance corrections were applied.
The extra native `cli,test-utils` lint leg exposed an existing no-attempt constructor conditional;
owning both inputs before selecting the source field removes that redundant conditional drop
without allocating, cloning or changing source ownership. That lint combination now passes too.

### P2 Axum Host-Known-Empty Checkpoint

This implements P2's existing known-empty conversion subtask as an engineering sequencing
clarification, not a new user approval or an alternative public contract. Outbound settlement
and the support-first/paired-time/head hooks remain with atomic P1. Preserving an existing
transport fact at ingress does not require those hooks or change the frozen Interfaces.

- [x] After admission, preserve Axum `is_end_stream()` bodies as `Body::empty()` for any method,
      without polling or inferring emptiness from headers or size hints. Keep Initial rather
      than Cached state, `IngressFraming::HostManaged`, context metadata and supplied clients.
- [x] Keep unknown bodies on the existing lazy stream/read-deadline path; retain source bytes,
      empty chunks, errors and ownership. Core first-`body_bytes` deadline precedence is unchanged:
      an initial buffered-empty body can still produce 408 through that accessor after expiry.
- [x] Pass actual HTTP/1 TCP GET/HEAD forwarding checks for absent framing and explicit zero
      length through `into_request` despite an expired inbound read deadline, with no outbound
      body bytes, Content-Length or Transfer-Encoding. Chunked-empty and nonempty GET controls
      still return 400 with no origin request and preserve unread-body connection boundaries.
- [x] Update the active guides and changelog without new capability rows or promotions. Demo
      and generated proxy handlers benefit from the same Axum converter with unchanged APIs.
- [ ] Complete general streamed GET/HEAD settlement, support-first preparation, paired outbound
      timers, eager/follow integration and the remaining adapter/runtime qualification with the
      atomic migration. This checkpoint does not complete P2 or EZ-1.

Fastly, Cloudflare and Spin conversion is unchanged. No core deadline precedence, public API,
backend policy, B/H(K), phase timer, slack, resource limit or SDK/runtime pin changes. The
checkpoint supplies no new approval or hosted/capability qualification; historical evidence and
the frozen Interfaces remain unchanged.

Verification passes all five root Rust gates, 2,241 workspace tests (one existing ignored),
245 Axum unit tests plus 17 contracts, 57 demo tests, and the explicit generated-project native/
three-WASM build and TCP fallback gate. Documentation formatting, lint, build, forwarding doctest
and its 15 checker tests, docs/legacy guards and approval-identity verification pass. Converter
and TCP regressions were red before the fix. Independent protocol and implementation reviews
selected this existing ingress fact over partial outbound hooks, and tightened the pipeline
guard to reject extra/wrong responses and the origin recorder to distinguish connection setup
from HTTP receipt. This remains local implementation evidence, not hosted qualification.

### P2 Fastly SDK-Body-Absence Checkpoint

This selects only preservation of actual SDK body absence as an engineering sequencing decision,
not new user approval, a new public contract or full P2 completion. The preceding Axum checkpoint
and its verification remain historical. Outbound settlement and support-first/paired-time/head
hooks remain with atomic P1; the frozen Interfaces, plan 02, requirements and approval record
are unchanged.

- [x] In direct conversion and admitted dispatch, use `try_take_body()` to preserve `None` as
      core `Body::empty()` without creating an SDK body. Acquire the admitted source only after
      admission and transfer the final SDK body after the prepare hook.
- [x] Keep every `Some(body)` on the existing lazy stream/read-deadline path, even when host
      known length is zero. Preserve prefetched read bytes and pending writes without header/
      hint inference, repacking, reads or flushes during conversion. Generic test-reader seams
      remain present sources, not SDK-absence evidence.
- [x] Cover actual WASI SDK bodies with zero host length but payload remaining in prefetched
      read bytes and pending writes. Host length is not EOF even beyond SDK buffering: Viceroy
      uses Hyper exact size hints, not a terminal-body signal.
- [x] Preserve Initial rather than Cached context state and `IngressFraming::HostManaged`.
      Core's first `body_bytes` read-deadline check remains unchanged and can still return 408.
- [ ] Fix ordinary Fastly bodiless forwarding and complete general settlement and the remaining
      P2/EZ-1 acceptance through the separately gated atomic migration.

G3's actual `from_client()` requests always supplied `Some(body)`, including absent framing and
CL0. This checkpoint therefore does not establish ordinary Fastly GET/HEAD forwarding success.
Demo and generated proxy handlers automatically share the converter with unchanged APIs; this
does not mean raw Fastly GET proxying works. No Native claim, new capability row or promotion,
B/backend-policy, slack, timer, resource-limit or SDK/runtime-pin change is enabled. Historical
failures and qualification gates remain; this is not hosted or full Fastly G2 qualification.

Verification passes all five root Rust gates, 2,241 workspace tests (one existing ignored),
193 Fastly WASI unit tests, 35 WASI contracts including six new SDK-body cases, target clippy,
57 demo tests and the generated-project native/three-WASM/TCP gate. All four existing local
Viceroy progress groups pass, including 24 relay cases, as working-tree characterization only.
Docs formatting/lint/build, forwarding-doctest, docs/legacy and approval-identity guards pass.
The absence test was red before implementation. Independent source and runtime investigations
rejected zero-host-length inference; real SDK tests demonstrate both buffer counterexamples.
Independent review found no remaining scoped defect after replacing a wall-clock-sensitive
prepare test with the injected app clock. No historical evidence or protected API identity changed.

### Prior Execution Status

Execution started with G1's inventory/checker and G2's opt-in host diagnostic. No production
runtime migration below is complete. The local G2 control failed: timed body readiness consumes a
source error on pinned Viceroy. See the [feasibility report](../../audits/2026-10-18-outbound-feasibility.md)
for retained evidence and cancelled/deferred Fastly work. G3's local probes are implemented:
native and Viceroy passed the exercised cases. Spin's initial response-owned failure exposed an
existing egress bug: early successful host observation aborted pending body work. That internal
bug is fixed without changing the outbound/public APIs; all local Spin cases and isolated controls
now pass. Cloudflare's subsequent local workerd run passes the exercised coding, marker,
scheduling and framing cases. A subsequent local relay run also passes on all four entrypoints.
Protocol/hosted/context legs and
Fastly-policy approvals remain pending. The requester's scoped reply authorizes the exact G4 API
digest, with approving maintainer name supplied as AG. P1's core paired-time and ownership foundations have started, not
the full client/adapter hard migration. Baseline is PR 275 at
`f6c708ccb5be19ba9fa92575db3c08c0f17e0d72`, called F in the
[requirements](../specs/2026-10-17-edgezero-pr275-requirements.md).
That requirements document supplies observable behavior, priorities, exceptions, acceptance
criteria, and evidence ownership. This series supplies implementation order and file ownership.
Historical outbound plans are implementation history, not instructions to recreate removed APIs.

The [G4 review packet](../../audits/2026-10-18-outbound-api-approval.md) presents the exact API digest,
hard-migration inventory and independent decisions, including the scoped API-only conversation
approval attributed to AG. Dependent Fastly implementation still needs a
new preserving primitive/report and its separately approved policy. No failed gate was waived.

### F2 Overflow Checkpoint

The independent [F2 checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f2-implementation-checkpoint-2026-10-21)
implements typed 413 readers/uploads, zero-read declared-length rejection, retry-header
suppression and aligned demo/scaffold fallback defaults. The isolated local overflow suite now
passes eleven cases each on Axum, Viceroy and Spin, including the exact 2 KiB chunked request
against a 1 KiB cap. That exact case also passes on Cloudflare, which remains failed: after the ingress
413, the next valid POST receives a proxy-generated 500 before fixture entry, also reproduced by
a JavaScript-only control. Later topology diagnostics narrow the failure to Wrangler's front-proxy
path; directly contacting its user-worker listener is not an acceptance replacement or a repair.
Retained failures are not waived by the passing cases.
The generated-app/demo Axum fallback leg also passes real TCP boundary, timeout and deferred
unread-body tests. Hosted and end-to-end legs remain open. This does not waive G2 or change the
exact approved P1 API migration; local upload response heads are observed without polling Fastly
response bodies.

### F8 Declared KV Checkpoint

The requester adopted Fastly, Cloudflare and Spin together. The independent
[F8 checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f8-implementation-checkpoint-2026-10-21)
preserves declared KV handles without opening unused stores, provides payload-free typed
500/503 opening failures and retries initialization on later requests. Only warning membership
for compiled IDs survives requests. Explicit required-store and Axum startup policies remain.
Local unit/runtime evidence is separate from consumer integration and hosted restoration.
This follow-up does not unblock G2 or change the frozen P1 API migration.

The [operation-delivery checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f8-operation-delivery-checkpoint-2026-10-23)
adds real local failed write/delete/list cases, post-delete state checks and post-shutdown
validation of all 39 request lifecycles. The original four-request same-instance warning proof
remains unchanged; Viceroy's six-request guest ceiling permits natural turnover in the added
operations. The final smoke run still exits 2 for the separately unverified Spin retained-mode overlap.
Consumer integration and actual provider outage/restoration remain open.

### F6a Diagnostic Foundation Checkpoint

The independent [F6a foundation](2026-10-18-trusted-server-outbound-04-followups.md#f6a-foundation-checkpoint-2026-10-21)
adds fixed-size, text-free provider detail to existing gateway, timeout and internal errors,
preserves public error behavior, and retains that detail through body policies, batches, pending
handles and sticky body replay. Exact field patterns migrate without compatibility wrappers.
The subsequent [SDK boundary checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f6a-sdk-boundary-checkpoint-2026-10-21)
populates the existing send/body boundaries on all four adapters and retains detail through ready
error/deadline arbitration. The [preparation checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f6a-preparation-checkpoint-2026-10-21)
adds request-only Cloudflare and Spin resource-setter diagnostics. The
[source-observer checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f6a-source-observer-checkpoint-2026-10-21)
retains the first observed body-source failure in completion/observer reports without changing
terminal precedence. Capability claims and hosted acceptance remain open.
This does not change transport behavior, unblock Fastly G2 or complete P1/F6a.

### F6c Framing Checkpoint

The independent [typed framing checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f6c-framing-checkpoint-2026-10-21)
retains the existing framing rules in separate application/fallback report slots and fixed-text
error-level logs. Shared attempt-owned normalization covers all four adapters without changing
wire bytes or postcommit source categories. This is not consumer or hosted qualification and
does not unblock Fastly G2 or the remaining outbound cutover.

### F6a Capability Publication Checkpoint

The independent [capability publication checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f6a-capability-publication-checkpoint-2026-10-22)
adds `outbound-failure-diagnostics` to manifest parsing, existing gates and four adapter tables.
Fastly/Spin are Native for available typed SDK-cause projection; Axum/Cloudflare are BestEffort
because provider visibility is incomplete. Published matrices are checked against the registered
adapters. This does not recover host-hidden causes or close full EZ-16/P1, consumer/hosted
qualification or failed Fastly G2. All transport, deadline and delivery guarantees remain unchanged.

### F6b Delivery Report Checkpoint

The [Fastly delivery-report checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f6b-delivery-report-checkpoint-2026-10-22)
hard-migrates custom-send results while preserving standard serving policy. It retains finalizer
state, exact terminal reports, last-body commitment and safe pre-attempt diagnostics. Native
replacement is permitted only before any attempt or after confirmed non-commitment. Local
Viceroy characterization is separate from hosted qualification. F9a assembly-failure integration
is now implemented below; consumer acceptance, full EZ-17 and failed Fastly G2 remain open.

### F9a Assembly Foundation Checkpoint

The independent [F9a assembly foundation](2026-10-18-trusted-server-outbound-04-followups.md#f9a-assembly-foundation-checkpoint-2026-10-22)
hard-cuts route assembly to Result, shares configured manual-router assembly, and exposes state
from the existing router registry. That foundation did not change outbound transport or entrypoint
failure responses. The subsequent [unified failure-delivery checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f9a-unified-failure-delivery-checkpoint-2026-10-22)
implements the explicitly adopted macro/serverless SHOULDs, canonical assembly-error delivery
and Fastly retained retry. All three local serverless runtimes pass focused assembly cases;
Axum retains pre-bind startup failure with a once-only boot diagnostic. Consumer acceptance,
durable hosted qualification and full EZ-22 closure remain separate from those local results.

### F9b Assembled-App TCP Checkpoint

The independent [F9b assembled-app TCP checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f9b-assembled-app-tcp-checkpoint-2026-10-22)
hard-cuts programmatic Axum servers to consume configured apps and adds an owned production-path
TCP test helper. Supplied clients avoid default transport initialization. Upstream local policy,
streaming and cleanup controls pass the root gates, all seven feature matrices, target lint,
demo/generated checks and local TCP smoke. Independent spec/quality reviews passed. Consumer
parity, the conditional begun-request leg and exact-candidate qualification remain open.

### F7a Buffered Framing Checkpoint

The independent [F7a checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f7a-implementation-checkpoint-2026-10-23)
derives eligible buffered lengths and hands transmitted Fastly buffers to the host before any
stream writer can truncate them. Core preparation requires an explicit framing mode; demos and
scaffolds migrate directly. Strict body suppression takes precedence over the pinned local SDK's
failing bodyless reference controls. All 80 local production cases pass, while nine SDK parity
controls remain failed and keep the full framing suite nonzero. Hosted compression, frozen
candidate qualification and consumer comparisons remain open. No serverless capability is
promoted, Fastly G2 remains failed, and neither full F7a nor P1 is complete.

### F7b Synthesized Header Checkpoint

The [F7b checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#f7b-implementation-checkpoint-2026-10-23)
adds bounded prevalidated framework-error/fallback header sets and Fastly per-request replacement,
without decorating matched routed responses/errors or application refusals. Envelope/attempt
snapshots retain the existing ownership and fallback budget; demos and scaffolds use the same core
API. Required shared/target checks and independent review pass, including the generated app's
executed tests and TCP probe. The separate local HTTP/1 suite passes 55 cases across all four
runtimes, including complete maximum-size sets, GET/HEAD fallbacks and Fastly's normalized
rejection/request-replacement paths. A further 40 serverless HTTP exchanges and one native startup
control prove partial-configuration exclusion and recovered-policy delivery. Consumer integration
and hosted/frozen-candidate acceptance remain gates. This does not complete P1 or waive failed Fastly G2.

### P1 Foundation Checkpoint

- [x] Verify the exact submitted Interfaces digest before implementation and record the scoped
      API-only approval with the supplied maintainer attribution, AG.
- [x] Add `OutboundFuture` and `OutboundTime` with the existing clock and an owned local wakeup
      factory. Preserve the absolute deadline, retain pending registrations, yield before early
      rearming, preserve original timer errors and release terminal/cancelled state exactly once.
- [x] Demonstrate missing-type/export red and the late timer-error regression, then eleven
      deterministic passing time tests, including
      fake-clock wakeups, frozen time, original error identity, resource release and clock regression.
- [ ] Integrate paired time with support-first preparation and timer-before-body settlement.
- [ ] Hard-migrate the shared dispatch trait, every current implementation, adapter injectors,
      behavioral tests, demo, generator and guards together. No old-signature defaults or aliases.

The foundation does not change transport behavior or qualify a runtime. G1 applicability, remaining
G3 premises, Fastly B/H(K), and hosted results retain their own open gates.

### P1 Head Foundation Checkpoint

- [x] Reverify the frozen Interfaces and policies-file digests; retain AG's scoped API approval.
- [x] Add the approved head/header/fidelity/Location-evidence types, response-context readbacks
      and send-mode enum, with no public context constructor, Clone, Default or mutable fields.
- [x] Charge original headers and the effective marker before exposing a head. Capture original
      Location facts before filtering, retaining provider-unknown fidelity and Content-Type
      uncertainty without a second raw header/value copy.
- [x] Reuse existing normalization unchanged. Body-independent framing validation, including
      malformed/conflicting lengths, still runs at construction; payload-length cap checks,
      decoding and collection wait for final disposition. Retain the original 205 declared-body flag.
- [x] Implement zero-read discard and finalization with the captured clock/budget, one teardown
      owner and live streamed handoff. Release terminal/cancelled reads and timer registrations.
      Yield between encoded reads, before decoding, so ready empty/no-output input cannot prevent
      the outer timer from waking. Strictly earlier original chain deadlines remain binding;
      equal deadlines retain the already-selected budget cause.
- [x] Establish missing-type/export red, empty-input starvation red, and both independent review
      regressions red/green. Add 22 deterministic head tests, including pending fake-clock wakeup,
      timer setup before source polling, typed error identity, cancellation, bodyless/205,
      header/body caps, original/partial coding and retained Location facts. Add privacy doctests.
- [x] Complete broad verification and independent final review for this checkpoint.
- [ ] Integrate the validated facade producer, progress-scope token and all client/adapter
      implementations in the atomic hard cut. Complete external supplied-client conformance,
      eager progress, settlement, batch-ledger migration and redirect following separately.

This is a core-internal execution foundation, not live head-first dispatch. Unit tests construct
private contexts; external implementations cannot manufacture them. The existing facade/trait and
adapter constructors have not changed, so demo, generator and fixtures keep their current valid
implementations until the atomic migration. No compatibility alias, silent option default or second
transport was added. The current inventory is eleven implementation blocks, including the fully
qualified qualification-fixture client; rediscover all callers before that cut.

Two read-only investigations compared this buildable foundation against an atomic full cut and
selected the former. API approval does not waive Fastly G2, B/H(K), the remaining G3 premises or
hosted/frozen-candidate qualification. Existing evidence records and the frozen Interfaces are unchanged.

All five workspace Rust gates passed on the final source snapshot, including 853 core unit tests;
core doctests and direct core builds for wasm32-unknown-unknown and wasm32-wasip1 passed too.
Docs format/lint/build, outbound documentation/legacy guards and 29 evidence-checker tests passed.
The inventory remains 255 criteria / 511 planned legs with `qualified: false`. Independent spec
review found and rechecked the encoded-read starvation and equal-deadline attribution fixes;
final quality review found no actionable defect in this subset. Broader terminal-race, delayed
timer-error/clock-regression and streamed pending-cancellation matrices remain integration coverage,
not provider guarantees inferred from these foundation tests.

### P1 Image Optimizer Values Checkpoint

Historical checkpoint at `08bfb89d`; the region-enum amendment below supersedes only the region
representation and its privacy doctest.

- [x] Reverify the frozen Interfaces digest and the seven region tokens against the installed
      Fastly SDK 0.13.1 source, without adding that SDK to core.
- [x] Add the approved selector enums, private region/options types and exact consuming
      builders/readbacks. Leave every selector unset and query preservation false by default.
- [x] Validate quality 0..=100, nonzero dimensions/ratio components and both percentage coordinates
      before values can be retained. The focus enum makes partial/conflicting coordinates
      unrepresentable; public Percent payloads are still checked by the crop builder.
- [x] Establish missing-type/export failures in both colocated and external tests, followed by
      eight passing unit tests and `public_image_optimizer_value_readbacks_compile_contract`.
      Include privacy/invalid-focus compile-fail doctests, fixed invalid-input diagnostics,
      selector/default/clone equality, full quality-domain boundaries and replacement semantics.
- [x] Complete broad verification and independent spec/quality review for this checkpoint.
- [ ] Attach values to requests/parts and prepared redirects with the required synchronous support
      hook in the atomic migration. Implement permitted Fastly Send/Follow mapping only after
      its separate prerequisites and numerical host-call horizon are approved.

Both read-only investigations selected standalone validation over premature request attachment.
Applications can construct, validate, clone, compare and inspect these values, not send optimized
requests yet. The facade, request/parts, adapters, demo and generator remain on their current
working contracts. No compatibility alias, ignored option, SDK dependency, provider pin or capability
claim was added. This advances the portable construction portion of P1/EZ-8.3, not all of P1 or EZ-8.
Fastly G2, B/H(K), the live hard cut and hosted/frozen-candidate qualification remain open.

All five workspace Rust gates passed on the unchanged final source snapshot: 2,224 tests passed
and one existing generated-project test remained ignored. Core doctests (including the four new
compile-fail examples), core no-default-features builds for wasm32-unknown-unknown/wasm32-wasip1,
and the Spin wasm32-wasip2 gate passed. Docs format/lint/build, outbound documentation/legacy
guards and 29 evidence-checker tests passed. Independent spec and quality reviews found no issue
in this subset. The inventory remains 255 criteria / 511 planned legs, `qualified: false`.

### P1 Region Enum Amendment

- [x] Apply the requester's 2026-10-24 scoped approval: replace the string-backed region with
      the public Copy/Clone/Debug/PartialEq/Eq enum Asia/Australia/EuCentral/EuWest/UsCentral/
      UsEast/UsWest. Keep canonical `parse`/`as_str` conversions without a compatibility wrapper.
- [x] Update the current Interfaces and retain the original exact-digest approval as history
      in the [approval record](../../audits/2026-10-18-outbound-api-approval.md#scoped-region-enum-amendment).
- [x] Demonstrate missing-variant red in the external API test; cover exhaustive external
      matching, direct construction, Copy readback and exact token/variant pairs. Replace the
      obsolete privacy doctest with a typed construction example; retain invalid-input tests.
- [x] Complete broad verification and independent review for the amendment.

Only the standalone region value changes. Request attachment, adapters, demo and generator do
not yet consume these optimizer settings; their existing outbound contracts stay unchanged.
Fastly feasibility/policy/horizon and hosted qualification remain open.

All five workspace Rust gates passed: 2,225 tests passed across 29 binaries, zero failed and one
existing test remained ignored. The 861 core unit tests, both external API tests and 11 core
doctests passed; core no-default-features checks for wasm32-unknown-unknown/wasm32-wasip1 and
the Spin wasm32-wasip2 gate passed. Docs format/lint/build, outbound documentation/legacy guards,
29 evidence-checker tests and the historical/current API identity verifier passed. Independent
code and documentation reviews found no actionable issue. The inventory remains 255 criteria /
511 planned legs, `qualified: false`; these checks do not qualify the full outbound migration.

### Native Image Optimizer Parity Preparation

- [x] Inspect the native TS path and compare the optimizer DTO/profile/adapter sources at
      `f9281917a7260b723f5f22175911c6d2147ef650` with baseline `a9037770a`; these files match.
      Confirm direct native dispatch and lazy response handling, with application-owned profiles,
      SVG exclusion, query stripping and conditional S3 signing/HEAD/raw-error sends.
- [x] Record the requester's 2026-10-24 scope approval separately from numerical Fastly-policy,
      measurement and deployment decisions. Keep the current Interfaces unchanged.
- [x] Strengthen EZ-8.12 and its existing hosted criterion with native-path performance parity;
      synchronize that one inventory leg and the source digest without changing applicability.
- [x] Expand D1d with exact mapping/request-round-trip/laziness/identity sentinels and document
      the paired TS/SDK-only hosted comparison in Q4b and the existing runbook template.
- [x] Complete independent review and preparation checks.
- [ ] Resume runtime integration only after its existing D1c/P3/P5, G2 and numerical B/H(K)
      prerequisites. No request attachment, SDK mapping, optimizer dispatch or new examples are
      implemented by this preparation checkpoint, and no compatibility path is added.
- [ ] Obtain measurement definitions/acceptance rules, explicit staging authorization and an
      entitled service; implement and execute the paired comparison. No performance result is
      claimed from local validation or Viceroy's unsupported response.

Two bounded investigations selected direct native mapping through the existing pipeline over
extra serialization or a separate image client. The approved scope retains every existing
outbound deadline, limit, error, ownership and batch contract. The finite Image Optimizer host-call
horizon remains unproved/unapproved; an instruction to continue is not its numerical approval.
Demo and generator remain on their working request API until the atomic migration; do not add
examples that compile yet cannot optimize. The inventory remains planned, not qualified.

All five root Rust gates passed on the unchanged runtime: 2,225 tests passed across 29 binaries,
zero failed and one existing test remained ignored. Docs format/lint/build, outbound documentation
and legacy guards, 29 evidence-checker tests and manifest/API identity checks passed. Independent
plan/spec and mechanical evidence reviews found no actionable issue. The inventory still contains
255 criteria / 511 planned legs and `qualified: false`. No hosted request or deployment was made.

### Local-Only Recheck And Comparison Guard

The requester instructed continuation without deployment on 2026-10-24. No service,
entitlement or deployment authorization is inferred from that instruction.

- [x] Recheck the stock/repaired executable hashes and run the existing offline/loopback G2
      suite on identical guest bytes from clean `98a1d0e8`; do not replace either runtime or pin.
- [x] Retain both failed runs and 414 canonical control/probe records separately. Repaired
      preserves source errors but completes one probe at 505.158 ms; its terminal release window
      crosses the unchanged 505 ms bound after a within-bound ABI return. Stock still loses 24
      selected source errors. Do not backdate, omit cleanup or infer its precise latency cause.
- [x] Add a co-located fixture replay guard for those complete inventories, retained late timer
      controls, distinct failure counts and the repaired cleanup/completion boundary.
- [x] Reproduce the evidence checker's acceptance of empty comparison measurements and the
      missing hosted optimizer comparison flag, then reject both with the existing schema. Keep
      this as a presence check, not a substitute for approved metrics, controls or review.
- [x] Complete independent review and all preparation checks before committing.

See the [local recheck record](../../audits/evidence/2026-10-24-fastly-local-recheck.json).
No production client, optimizer mapping, deadline, batch behavior, capability or current
Interfaces change. G2 remains Failed and B/H(K)/hosted gates stay open. This is local diagnostic
and evidence-tool progress, not implementation or performance qualification of Image Optimizer.

All five root Rust gates passed: 2,225 tests passed across 29 binaries, zero failed and one
existing test remained ignored. All 119 fixture-harness tests, fixture fmt/clippy, 31 evidence-tool
tests, docs format/lint/build, script formatting and outbound docs/legacy guards passed. The first
sandboxed fixture run denied existing TCP binds; the unchanged loopback-enabled rerun passed.
Independent reviews verified the evidence identities/replay and comparison-presence fix without
finding an actionable issue. Documentation-only formatting options also fail on the baseline
script files; their existing local formatting passes and is retained. All 511 inventory legs
remain planned and unqualified. The live local G2 diagnostic exits 1 on both runtimes, unchanged
by these passing development checks; no deployment was made.

### P1/P4 Ownership Checkpoint

- [x] Add the approved local `OutboundExchangeOwner` and `OutboundPending` types with private
      ownership state and root exports. Retain the exact submitted Interfaces digest.
- [x] Use one receiver-observation path for readiness and collection; cache the original outcome,
      release a completed receiver immediately and release failed work on observation. Do not poll
      a response body, resample deadlines or reconstruct typed errors during handoff.
- [x] Cancel on handle/collection-future drop, including an unpolled future; disarm pending
      cancellation only before returning success. Keep streamed-body cleanup separately owned.
- [x] Demonstrate missing-type/export red, then nine deterministic passing ownership tests,
      including live-stream handoff, repeated readiness, exact non-Clone error identity, cached
      response abandonment, ordered cancellation before receiver/body release and real-waker
      registration after a nonblocking check. Debug inspection neither polls nor exposes cached payloads.
- [x] Finish broad verification and independent spec/quality review for this checkpoint.
- [ ] Integrate these primitives with head-first dispatch, verified eager scheduling, scope owners
      and the arrival ledger during the full hard cut. No lazy compatibility begin is supplied.

Both investigation/proposal reviews selected the same minimal design: retain cancellation on cached
success, release it on cached error, and privately disarm it on successful collection. Dropping a
disarmed callback still destroys its captures: pending producers must use passively droppable
cancellation handles, not armed abort-on-drop SDK guards. Buffered publication follows transport
cleanup; streamed publication follows transfer to a distinct body owner. These are producer
obligations, not guarantees inferred by polling a supplied future. No adapter, runtime scheduling
semantics or capability is changed by this standalone foundation. At that checkpoint Fastly G2
remained failed and Cloudflare tooling was unavailable; portable tests waived neither. The later
Cloudflare checkpoint below supersedes only that local tool gap.

Checkpoint verification passed: all five workspace Rust gates (782 core tests), core doctests,
all seven eight-combination adapter matrices rerun after the final test additions, three WASM
target clippy runs, demo tests and the generated-workspace gate with 22 executed core tests.
Docs checks, dependency audits, the legacy guard and the 18 evidence-checker tests passed; the
inventory still reports 255 criteria / 511 planned legs with `qualified: false`. Independent spec
and quality reviews found no correctness defect in this subset; the suggested ordered-cleanup
regression was added and reviewed. No new trait implementation or compatibility path was introduced.

The nine ownership tests also executed successfully under pinned Viceroy on wasm32-wasip1:

```sh
SSL_CERT_FILE=/etc/ssl/cert.pem CARGO_TARGET_WASM32_WASIP1_RUNNER='viceroy run' scripts/run_test_nonzero.sh pending_cancellation_precedes_receiver_and_cached_body_release cargo test --offline --locked -p edgezero-core --target wasm32-wasip1 --lib outbound::progress::tests
```

The initial local runner exited before test discovery because its macOS keychain was unavailable.
Using the existing system CA bundle through the documented native-certificate override fixed
runner startup without changing code, disabling verification or accessing the keychain. These are
portable ownership tests, not Fastly SDK scheduling/readiness or hosted qualification evidence.
The previously retained runtime characterization records remain historical source snapshots.

### P1/P3 Original-Coding Checkpoint

- [x] Add the approved Decode/Original request choice, default/readback/parts preservation and
      enforced propagation through all four current adapter transports, including Fastly batch metadata.
- [x] Centralize original and mandatory 206 delivery in the existing shared body policy; preserve
      normalization precedence, identity decoded caps, transport/final caps and decoder bypass.
- [x] Demonstrate failing decoder/header/buffer-cap regressions, then passing core and actual Axum
      transport tests, including mixed batch choices and zero-poll bodyless controls.
- [x] Exercise gzip/deflate/Brotli through pinned local Viceroy and Spin origins: 24 checks per target,
      both response modes, Original/200, Original/206, Decode/206 and Decode/200 controls.
- [x] Align relay demo/template requests with Original while keeping JSON fanout on Decode.
- [x] Exercise actual encoded downstream relay through all four local entrypoints, including
      workerd manual egress, both response modes and Decode controls; retain wire bytes and metadata.
- [ ] Complete the head-first hard cut, full adapter normalization/limit matrices, Fastly mixed-batch
      host proof and hosted qualification before closing P1/P3.

This original-coding slice preceded the marker checkpoint below and does not implement eager dispatch or redirect following. It
does not adopt EZ-5.9/5.10's optional metadata changes or bypass the failed Fastly readiness gate.
The approved Interfaces text and its digest remain unchanged.

Checkpoint verification passed: all five workspace Rust gates (768 core tests), all seven
eight-combination adapter matrices, three real-target WASM clippy runs, demo format/clippy/tests,
and the generated-workspace gate including 22 executed generated-core tests. Docs checks, dependency
audits, both nonzero original-coding sentinels and the fixture's tests/clippy passed. Two independent
read-only reviews found no blocking defect; their additional declared/observed cap controls were
added and verified. Local coding results are retained in the feasibility report, not claimed as
release evidence. The forwarding-example checker was delivered in the separate Q1/Q2 checkpoint below.

### Q1/Q2 Forwarding Documentation Checkpoint

The mandatory EZ-1.14 checker extracts the actual forwarding fence from `docs/guide/proxying.md`,
builds core offline/locked and discovers its rlib, dependency directory and edition through Cargo
JSON. It requires exactly one listed and passed non-ignored rustdoc test. A checker-owned empty
entrypoint prevents handler invocation; a delimiter longer than every source backtick run keeps
embedded fences from bypassing compilation or the empty entrypoint. Fifteen passing Node cases
cover missing/duplicate fences, weakened annotations, zero tests and actual compilation failures,
including removal of `into_request()?` and both independently reproduced embedded-fence failures.
The existing workspace CI leg runs both Node commands after locked fetches with pinned Node.
This is supplementary compile-only coverage outside the reviewed 511 runtime/environment legs:
it does not adopt optional EZ-25.42, prove GET/HEAD/POST forwarding or close P2 or failed Fastly G2.

Local checkpoint verification passes all five Rust gates (2,193 workspace tests, zero failures,
one existing ignored test), the 15 checker cases, 18 inventory tests and docs format/lint/build.
Independent review reproduced the embedded-fence bug before its fix and cleared the final checker.
Inventory validation still reports 255 criteria and 511 planned, unqualified environment legs.
These results are local source verification, not hosted CI or frozen-candidate qualification.

### G2 Timer-Control Checkpoint

The [timer-control investigation](../../audits/evidence/2026-10-23-fastly-timer-controls.json)
adds raw ABI timestamps and 72 controls per stock/repaired comparison without changing production
code or runtime pins. Native sleep exceeded +5 ms in the initial pair. After excluding formatting
from timer-only measurements, one stock 100 ms WASI sleep returned at 106.952 ms without body
handles. Repaired HTTP matrices passed locally, while stock still lost 24 source errors.
Earlier repaired timing failures remain retained/replayed; these samples do not identify their
cause or establish a bound. G2 stays Failed, with dependent Fastly work gated and approved
portable work independently eligible. B/H(K) and hosted qualification remain separate decisions.

### G2 Expanded Diagnostic Checkpoint

The opt-in SDK/ABI fixture now executes eleven controlled origins with buffered and pull-streamed
consumption, capacities 1/16384 and three repetitions, plus finite direct controls: 135 probes and
147 origin requests. Every selected read needs fresh matching readiness and the original absolute
deadline. Origins own bounded workers/sockets and join them on every exit; partial traces survive
ABI failures. Strict mutation tests reject missing inventories, malformed outcomes and error loss.

The first complete repaired run returned one pending-body probe at 507.794 ms and failed the
unchanged 505 ms boundary. Stock also loses source errors. The separate
[matrix record](../../audits/evidence/2026-10-23-fastly-readiness-matrix.json) retains negative
observations. No capability, SDK/runtime pin, production API, inventory leg or G4 approval changes.
Full G2, B/H(K), the atomic production migration and hosted qualification remain open.

### G2 Local Runtime Repair Checkpoint

The [tracked runtime patch](../../../patches/viceroy/README.md) repairs the measured Viceroy
body-readiness error loss and empty-frame hazard without a blocking-read fallback or SDK/runtime
version change. Two independent investigations selected owned queue-based error retention over an
adapter workaround that cannot recover a consumed host error. Ten co-located regressions went red
(nine failures) and then green; all 135 upstream library tests pass. Stock and repaired guest
controls use identical Wasm: three stock comparisons fail, three repaired comparisons preserve
UnexpectedEof. The separate [record](../../audits/evidence/2026-10-23-viceroy-readiness-repair.json)
binds upstream/patch/host/guest identities and retains both event streams. Strict upstream clippy
has identical existing diagnostics on stock/repaired source; no suppression is introduced.
The repaired probe exits 2: full G2 ABI coverage/review, B/H(K), hosted execution, candidate freeze
and production qualification remain open. Historical failure records and all 511 legs are unchanged.

### Q2 Runbook Source Preparation Checkpoint

The [runbook source template](../../../tests/fixtures/reusable-app/ts-outbound-qualification-runbook.md)
records implemented preparation/lifecycle-egress commands separately from unimplemented full
outbound probe/export bindings. It includes G1/G4 references, external candidate/build placeholders,
count domains and separate owner acceptance, staging authorization, entitlements and approval fields.
No new checker or second qualification system is introduced. G1's stale G4 reason now acknowledges
AG's API-only approval; all 511 legs and pending policy decisions remain unchanged. Exact runnable
bindings and the Q2/Q4a acceptance gates stay open. No candidate freeze, runtime pin, transport,
capability or failed Fastly G2 disposition changes.

Local checks pass all five Rust gates (2,193 tests, 28 binaries, one existing ignored), 34 evidence/
egress Node cases, 15 forwarding-checker cases, the actual one-test doctest and exact Rust assembly
sentinel. Docs format/lint/build, documentation/legacy guards and all six runbook links pass.
The complete TCP-enabled test rerun replaces the sandbox-denied attempt. These checks do not
establish hosted execution or frozen-candidate acceptance.

Independent command-map and approval reviews favored a static unaccepted template over a second
checker. Spec review corrected product-threshold scope; operator-safety review caught a fail-open
Git status check. Separate assignments now preserve Git failures and explicitly include untracked
files. Negative shell probes cover both errors, wrong SHA, tracked/untracked changes and a clean
control. Final scoped reviews are clear; they do not close full Q2 or Q4 acceptance.

### Q3 Evidence Preparation Checkpoint

The [pre-freeze assembly checkpoint](2026-10-18-trusted-server-outbound-05-qualification.md#q3-pre-freeze-assembly-checkpoint-2026-10-23)
adds one Node-owned draft assembler and shared release/report validation. A Rust fixture test
invokes the actual CLI and checks preserved case/sample counts, candidate/build identities,
raw SHA-256 hashes, non-overwrite behavior and release refusal. Draft assembly is always
unqualified, not an automatic conversion of working-tree smoke evidence. Reports retain
independent blockers without accepting ambiguous rows or counting invalid artifacts as passed.
All 511 reviewed legs and applicability decisions remain unchanged. Frozen export, controlled
origins, approvals, source freeze and local/hosted/consumer qualification remain open; this does
not change production APIs, complete P1 or waive failed Fastly G2.

Independent architecture/coverage proposals selected Node-owned assembly with an actual Rust CLI
test instead of a second constructor/validator. Spec and quality review reproduced and closed
acceptance-count and catalog-isolation errors; final reviews found no remaining scoped issue.
Local verification passes all five Rust gates (2,193 tests, 28 binaries, one existing ignored),
85 harness and 20 qualification-core tests, the exact nonzero CI sentinel, 29 evidence-tool tests,
15 forwarding-checker tests and the one actual doctest. Fixture strict clippy/format, docs
format/lint/build and documentation/legacy guards pass. Complete TCP-enabled reruns replace the
initial sandbox-denied attempts. Inventory remains 255 criteria / 511 planned legs, unqualified.

### P1/P3 Marker Checkpoint

- [x] Add the approved client default, effective inspector and per-request Inherit/On/Off APIs;
      preserve readback/parts settings and resolve policy before supplied-client send/batch delegation.
- [x] Wire the shared insertion/accounting policy through all four adapters and Fastly pending slots;
      retain default-on behavior and ordinary pre-normalization upstream accounting.
- [x] Test exact byte/count budgets, duplicate upstream values, nomination precedence, overwrite
      behavior, clone isolation and a non-polling inspector, plus real Axum disposition/batch cases.
- [x] Configure only demo/template relay handles off; keep JSON fanout default on and test both.
- [x] Exercise 46 marker checks each on native Axum, pinned Viceroy and Spin, including mixed-policy
      batches; retain working-tree source/artifact identities and the initial Spin failure.
- [x] Isolate Spin's host-hidden Connection nomination with direct SDK controls and demote its
      header fidelity to BestEffort without claiming raw-field normalization.
- [ ] Complete hosted marker evidence, plus the larger head-first/eager/follow
      migration before closing P1/P3. Future send modes must use the same policy preparation.

The [marker record](../../audits/evidence/2026-10-20-outbound-marker.json) characterizes visible-field
policy, not a frozen candidate or raw-header fidelity. Fifteen Spin cases explicitly record hidden
nomination rather than count it as successful raw normalization. The same three runs also passed
24 coding controls each. At that checkpoint Cloudflare lacked the exact locked wasm-bindgen CLI;
no download or repin had been attempted. The subsequent Cloudflare checkpoint records its separate
authorized tool install. Fastly's failed readiness gate and existing dispatch-slack guard remain unchanged.

Checkpoint verification passed: all five workspace Rust gates (773 core tests), all seven
eight-combination adapter matrices, three real-target WASM clippy runs, demo tests/clippy and the
generated-workspace gate including 22 generated-core tests. Independent adapter and core/template
reviews found no remaining marker-policy defect; the template formatting finding and stale Spin
capability prose were corrected and rechecked. The new test-only MarkerClient adds one implementer
to rediscover during the full P1 hard cut; the frozen eight-implementation approval inventory remains
submission-time history and its Interfaces digest is unchanged.

### Cloudflare Runtime Checkpoint

- [x] Provision the exact locked wasm-bindgen CLI in an authorized temporary directory without
      repinning dependencies or changing global tools. Run only local workerd with local bindings.
- [x] Execute 24 coding, 46 marker, five SDK/public scheduling and five HTTP/1 framing checks;
      assert 89 origin receipts and retain the 18 individual scheduling receipts.
- [x] Strengthen framing validation against missing, mistyped or contradictory pre-conversion
      Workers signals. Prove all eight independent-group failure combinations retain overall
      failure and still execute scheduling/framing; keep Spin terminal diagnostics intact.
- [x] Retain separate initial and strengthened source snapshots, raw events, JS/WASM/runtime/tool
      hashes and effective compatibility settings in the
      [new record](../../audits/evidence/2026-10-20-cloudflare-outbound.json).
- [x] Complete the independent local downstream encoded relay proof in the checkpoint below.
- [ ] Complete full head-first/eager/follow integration and
      hosted/protocol/context/cancellation qualification before closing their parent tasks.

This dated checkpoint supersedes earlier Cloudflare tool-unavailable status only for the local
cases exercised. No production source, dependency pin, capability assignment, Fastly readiness
gate or approved Interfaces digest changes. Historical evidence is not rewritten as passing.

Checkpoint verification passed: all five workspace Rust gates, the three real-WASM-target clippy
runs, 37 harness tests, 12 fixture-core tests, fixture format/clippy and docs format/lint/build.
The 18 evidence-checker tests, inventory validation, docs/legacy guards and decoder/map dependency
audits passed. Source, embedded-content, artifact and tool hashes were recomputed successfully.
Two read-only investigations selected private framing/phase helpers over a new runner framework:
the runtime review found no need for URL/header workarounds, while the evidence review identified
the missing host-signal assertion and independent-phase coverage gap. The subsequent scoped spec
review found no defect in the strengthened harness or final local observations. Independent quality
review verified 27 file hashes, 16 embedded payload hashes, 23 final source hashes and the unchanged
approval/requirements digests; it found no remaining defect in this checkpoint. It did not rerun
the broader CI commands, whose results above are from the central verification run.

### P3 Downstream Relay Checkpoint

- [x] Reuse the validated coding fixture and current transport; return `into_response()` before
      collection. Keep coding JSON reports unchanged and disable markers only on the relay clone.
- [x] Prove zero source polls before streamed handoff, exact buffered/streamed bytes, request
      policy parity and isolated target rejection with a private recording client.
- [x] Capture binary TCP payloads without UTF-8 conversion or decoding, enforcing a 256-byte cap
      before append and preserving truncation as failure. Run negative wire assertions for changed
      bytes, missing metadata, stale coding, invalid lengths and conflicting framing.
- [x] Run 24 actual downstream relay cases on each local runtime, keeping the existing 80 base
      cases. Retain 24 ordered relay receipts per target and all four group results.
- [x] Retain the [relay evidence](../../audits/evidence/2026-10-20-outbound-relay.json), including
      all 96 relay observations, selected source/artifact/runtime hashes and compatibility settings.
- [ ] Finish head-first/eager/follow integration, full normalization/limit matrices and exact-commit
      hosted qualification before closing P3 or its parent phase.

The final all-runtime command exited zero: 104 base checks and 113 asserted base origin receipts per
adapter. Spin additionally retains its two SDK header and four response-stream controls. This is a
working-tree characterization at base `0250edc9`, not qualification of that commit. No production
adapter or API changed, no compatibility path was added, and Fastly G2 remains failed. Local relay
does not prove forwarding actual inbound GET/HEAD bodies or the new eager APIs.

Both read-only investigations kept P2's outbound settlement and support-first/time/head hooks
with atomic P1: current clock-only constructors and lazy implementer sends cannot satisfy that
boundary by partial hooks. The independent P3 proof was selected instead of adding unused
foundations or weakening that cut. The later Axum host-known-empty checkpoint independently
preserves an existing transport fact during ingress conversion; it does not implement settlement,
add partial hooks or change a public API.
Future P1 rediscovery must include the new fully qualified test implementation
`impl edgezero_core::OutboundHttpClient for RelayClient`; the frozen approval inventory remains
submission-time history. Scoped spec and reader/validator reviews found no blocking defect. The
remaining nonblocking diagnostic gap is explicit: reader failures are fatal and group-labelled
but can precede per-case relay records.

Checkpoint verification passed: all five root Rust gates, 14 fixture-core and 41 harness tests,
native fixture clippy/format, and progress-enabled fixture clippy on all three WASM targets. Docs
format/lint/build, the docs/legacy guards, decoder/map dependency audits, 18 evidence-checker tests
and inventory validation passed. The legacy guard first caught a test-double parameter retaining
the old cutoff spelling; it was renamed and the scoped tests, guard and complete four-runtime
suite were rerun. Evidence now points to that final source/run; the earlier passing runtime record
is retained only as diagnostic history. Production sources and lockfiles remain unchanged.
Independent evidence review verified all 96 relay passes, 96 ordered receipts, 42 selected source
hashes and eight embedded texts. Its documentation nit was corrected: truncation tests cover
fixed/chunked bodies; exact-cap and overflow tests cover all three framing modes.

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
- [ ] Complete G4's remaining policy decisions. AG approved the exact Interfaces digest for API
      implementation; separately approve G2's Fastly primitive and literal backend-policy bound before D1.
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

Deployment, consumer migration, commits and pushes remain separately authorized actions. A
negative feasibility experiment is not completion of the runtime series or approval to repin.

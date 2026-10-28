# Trusted Server Outbound Adoption Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Trusted Server adoption requirements without regressing the outbound work already implemented in PR 275.

**Architecture:** Extend the existing portable outbound policies and adapter-owned exchanges; do not introduce a second transport. Use one head-first dispatch boundary for plain sends, eager exchanges, and explicit redirect following, with independent callback and response-body ownership. Prove the pinned Fastly timed-read boundary before replacing its dispatch and harvest engine.

**Tech Stack:** Rust 1.98.1, existing Cargo features, Fastly SDK 0.13.1 and Viceroy 0.21.0, worker 0.8.5/workerd, Spin SDK 7.0.0/WASI HTTP 0.3 and Spin 4.1, reqwest 0.13.4, existing native Axum/Hyper runtime.

---

## Status and Authority

### Current Delivery Dashboard (2026-10-27)

Reconciled against the current hard-cut working tree above `f4d660b1`, including live prepared
dispatch/head finalization, GET/HEAD settlement, eager drivers and identity-bound batch receipts.
All five primary Rust gates pass locally after the current scope hard cut. The requester reopened the owned
interfaces for a shared typed lifecycle and separately approved dispatch-slack retirement.
Core now owns callback/body scopes and revocable operations; provider hooks supply progress only.
Explicit early body ownership is attached unchanged and accepted at canonical egress, while
unaccepted responses remain callback-bound. The retained 25 ms rejection and injection APIs are
removed; original deadlines, B31 and every portable cap remain enforced. The three requested
implementation slices (shared pipeline, bounded GET/HEAD settlement, eager Begin/Batch ownership)
are locally verified. The subsequent P5 checkpoints below add explicit same-client Follow and
provider-boundary controls; all-target acceptance binding and hosted qualification remain open.
This is the current task-level implementation summary, not qualification of that SHA. The
recorded [local acceptance binding checkpoint](#g1f9b-local-acceptance-binding-checkpoint)
activates existing criteria without changing the production implementation snapshot; the
standalone-runner checkpoint below adds the complete local redirect bindings.
The checkpoint sections below retain their historical commands, results and limitations. The
[Requirement Coverage](#requirement-coverage) table maps every EZ requirement group to its owning
tasks, current implementation/checkpoint and remaining closure work.

The six approved consolidation commitments are **not collectively complete**:

| Commitment                                                            | Current Implementation                                                                                                                                                                                              | Remaining Work                                                                                                                                                                                               |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1. Shared preparation, validation, normalization, decoding and limits | **Hard-cut implementation locally verified.** The facade captures invocation ownership/time and prepared contexts; all four adapters return raw heads to shared normalization, disposition, decoding and bounded collection. | Bind applicable exact-candidate acceptance legs (P1-P3, D1-D4). |
| 2. One pipeline for send, begin, batches and redirects | **Send/Begin/Batch plus explicit Follow implemented.** The shared pipeline includes eager drivers, ownership, raw observations, observation-only checks, mixed collection, lossless identity-bound recovery and head-first redirect policy. | Complete all-target redirect runtime parity and acceptance/qualification (P5, D1-D6, Q3-Q5). |
| 3. Remove superseded implementations and APIs without compatibility | **Hard cut locally verified.** Old implementer send/raw-vector batches, `from_futures`, `finish_batch_item`, public receipt construction, Fastly's separate lifetime registry, copied task/deadline policies and old Body enum API are removed; no aliases remain. Follow uses those same primitives. | Complete applicable acceptance bindings before closing all P1-P5 work. |
| 4. Preserve direct native Image Optimizer | **Send/Follow share native dispatch.** The native SDK mapping hands its head to the common finalizer without another fetch or transformation pipeline; prepared redirects retain optimizer options. Local actual-client Send/Follow controls verify native unsupported boundaries. | Qualify successful optimizer-first-hop redirects and hosted transformation/performance. The synchronous blocking horizon remains UNKNOWN (D1d/Q4b). |
| 5. Migrate demo, templates, clients and docs together | **Hard-cut callers migrated.** Supplied clients, demo, generator, fixtures, guides and guards use prepared contexts, shared ownership/progress, raw events and explicit collection states; demo/scaffold add explicit allowlisted Follow. | Complete remaining acceptance and qualification (P5/Q1-Q5). |
| 6. Document the TS hard-cut ownership boundary                        | **Documented** in the [adoption guide](../../guide/proxying.md#hard-cut-adoption). Transport stays upstream; application policies stay in the consumer.                                                             | Implement the remaining EdgeZero prerequisites and obtain consumer composition/qualification evidence before cutover. Do not delegate body dropping, buffering or transport workarounds to TS (P1-P5/Q4/Q5). |

Keep three different status questions separate:

- **Implementation:** this dashboard and the requirement table describe code present at the
  snapshot. A foundation or a local checkpoint does not complete its parent task. No full P1-P6
  or D1-D6 migration task is closed by the checkpoints recorded so far.
- **Executable acceptance coverage:** G1's
  [inventory](../../../tests/fixtures/reusable-app/outbound-cutover-cases.json) covers 255 source
  criteria and 511 split/environment legs. Its `implementation: planned | ready` field describes
  readiness of that leg's declared command/sentinel, not overall feature implementation.
  EZ-23 local criteria 01-11 and EZ-9 core criteria 01-15 plus local runtime criteria 16/17 are
  `ready`: 32 legs, with 479 still `planned`. G1/Q3 binding work remains unfinished, not evidence
  that 479 features are absent. Only the explicitly bound
  commands count as executable legs; unrelated checkpoint tests do not activate another leg.
- **Qualification:** Q3 has not frozen a candidate; Q4 has no accepted candidate-specific package
  or returned hosted/consumer qualification. `ready` commands and green CI cannot substitute for
  passing, exact-candidate evidence. The inventory check remains `qualified: false`.

Local review repaired expired-cutoff handoff on Fastly and Spin: earlier own-deadline outcomes
remain resolved at logical expiry, with no body, timer or provider work. Demo, template and
supplied fixtures preserve exact-cutoff `Unresolved` outcomes rather than emitting premature EOF.
Fastly's pending response-source read now races its absolute write deadline, reports write expiry
and releases response-owned work without cancelling callback siblings. Registered timer failures
remain typed source failures; these guest fixes do not certify interruption of a blocking hostcall.

Prior shared-scope checkpoint verification includes 984 core unit tests, six external prepared-client tests, 262 repaired-Viceroy
unit tests and 36 Fastly contracts with no filtered readiness cases, 74 Cloudflare guest unit tests
and 29 contracts, and 21 Spin SDK tests plus 35 WASI contracts. The consolidated native TCP gate
passes 261 library cases plus 25 contracts, including all eleven bound sentinels. Refreshed demo
default-core (55), rendered scaffold (28) and qualification-enabled reusable fixture (29) tests
pass. Strict native/provider Clippy and feature builds pass. The forwarding example compiles, docs lint/format/build
and legacy/dependency/evidence guards pass. These are working-tree observations, not a frozen
candidate or hosted qualification. Subsequent interface and slack authorizations are recorded in the
[approval record](../../audits/2026-10-18-outbound-api-approval.md#original-exact-api-subject).

CI at `2be1dd87` exposed a local verification gap: Spin's `spin`-only WASM lib-test build
included two helpers called only by `test-utils`. Both red jobs report that same compile
failure, not a guest contract failure. Narrow those helper gates and reproduce both
`spin` and `spin,test-utils` Clippy all-targets commands plus the complete existing
feature matrix; native all-features checks cannot stand in for these combinations.

Independent core/protocol and caller reviewers identified timer-error precedence, future terminal
observations, cutoff handoff and fixture EOF defects. Adapter reviewers/validators covered the
Axum/Cloudflare pair, Fastly and Spin independently; caller validation covered actual demo,
generated and reusable workspaces. The selected fixes preserve the strict core driver protocol,
shared native pools, original budgets and non-cloneable resource ownership rather than weakening
tests or adding compatibility paths. The parent reran workspace gates and guest contracts after
those earlier fixes. Scope/slack changes require their own fresh verification; no passing check
supplies hosted qualification or additional runtime-policy approval.

The implemented outbound baseline is protected by [Non-Negotiable Baseline](#non-negotiable-baseline).
The tables track the additional adoption work; they do not authorize replacing or deleting the
existing batch/cutoff/cancellation, timing, cap, encoding, provenance or lifecycle guarantees.
Normative requirements, attributed approval amendments and explicit applicability decisions
remain authoritative over this summary. Pending follow-ups are not silently promoted or waived.

### P5 Explicit Redirect Policy Checkpoint (2026-10-27)

The approved `HttpClient::follow` surface is implemented through the same prepared dispatch and
head finalizer as Send, including supplied clients. A synchronous application policy and a
validated 1..=20 limit are required. The implementation preserves the exact decision order,
original Location evidence, byte-sharing replay, method/header/credential rules, unchanged
options and arbitrary policy errors. Followed heads are dropped without polling or decoding;
final heads keep normal body policy. The original absolute deadline survives replacement
requests and final streamed consumption, while per-hop timeouts/defaults remain per-hop.

Core verification passes 1,044 unit tests and fourteen integration tests, including fifty redirect
tests, the complete status/method table and the mandatory supplied-client/original-deadline
sentinel. Real-TCP Axum controls cover plain Send remaining unfollowed, two-origin following,
pending oversized compressed intermediate bodies and no-redirect coding/limit parity. The
fixture fixes `Date` explicitly so comparing independent responses does not depend on wall time.

Independent review reproduced reentrant scope closure during policy, unread-head disposal,
GET/HEAD settlement, upload polling and finalizer clock/timer callbacks. Shared preparation,
source binding and finalization now check their captured scope before advancing; there are no
adapter-specific cancellation copies. Finalization retains read-time clock history through
cleanup. Terminal source disposal runs inside the existing revocation check, so a cleanup callback
cannot turn scope closure into clean EOF. The existing private collector accepts a borrowed
payload, avoiding another box without changing public signatures, caps or error precedence.
All reproduced red cases now pass.

Demo and generated `/follow` handlers use the existing bounded request policy, an app-clock
absolute deadline and initial/per-hop same-origin authorization. Canonical equivalent authorities
are covered; existing `/proxy` and fanout behavior is unchanged. The public capability and guide
describe portable redirect policy separately from provider transport guarantees. The CI pipeline
runs the core sentinel and the existing Spin eager component rather than merely compiling it.

Final verification after the terminal-cleanup fix passes all five root gates, the consolidated
Axum suite (261 library tests and 27 contracts), sixty demo-core tests, thirty-three freshly
generated core tests and strict clippy for the generated workspace. All eight WASM feature
clippy configurations pass. Runtime suites execute without ignored or filtered cases:
Cloudflare 74 library tests plus 29 contracts and nine Node fixture tests; Spin 35 contracts
plus 21 SDK-resource tests and the eager component; Fastly 262 library tests plus 36 contracts.
Fastly uses the already-approved repaired Viceroy with `SSL_CERT_FILE=/etc/ssl/cert.pem`,
not disabled TLS verification or a promoted runtime pin. The initial sandboxed native run
could not bind sockets; the full workspace and TCP suites pass with local-network permission.
The expensive ignored full generated-workspace gate was not rerun locally; the fresh generated
core tests and workspace clippy above are not a claim that this separate CI gate ran.

This checkpoint does **not** close P5's complete acceptance matrix. Other provider-runtime Follow
parity, actual Fastly optimizer-hop mapping, consumer composition, criterion-level binding and
hosted qualification remain open. The inventory is unchanged: eleven ready legs, 500 planned,
`qualified: false`. Native redirect policy is not a promotion of deadline, cancellation, framing,
resource-accounting or delivery guarantees. Normative requirements and approved Interfaces
remain unchanged; the SDK/decoder/runtime pins and scoped Viceroy qualification boundary remain
unchanged.

### Fastly Cooperative-Wake CI Repair (2026-10-27)

CI at `e02c2360` passes the guest unit/contract suites but fails the reusable HTTP smoke on
variant `b`'s first real outbound `/origin/repeated` request. The recorded socket error is
Linux `WouldBlock` (11), reproduced locally as macOS `WouldBlock` (35); the dependency's
future-incompatibility warning is not the failure. Fixed-phase local tracing shows the
cooperative encoded-body stream waking and yielding after its first chunk, followed by
Fastly entering an empty-handle host selection until the still-live 30-second deadline.
The unchanged smoke's client times out first. No timeout or smoke assertion is relaxed.

The existing progress engine now forwards source wakes through a per-poll waker and shares
only a notification flag across its family. Same-family nesting never clears that flag;
older retained wakers can notify the current caller. A notified turn still publishes expiry,
but does not enter another host wait. Later non-waking pending polls resume ordinary waits.
The progress state retains no parent task waker, and all temporary tracing is removed.

The direct/nested-yield and retained-waker regressions first fail deterministic assertions
without sleeping, then pass with the fix. A third control covers deadline expiry during the
yield and restoration of normal waits afterward. All thirty progress tests pass. The exact
`--adapter fastly --suite smoke` command passes locally with exit 0 under the approved repaired
Viceroy, including every existing variant and assertion. This is local runtime evidence, not
a frozen candidate, hosted qualification, a changed runtime pin or capability promotion.
Provider-runtime redirect acceptance work remains open while this CI repair takes priority.

Final repeat passes all five root gates, all four Fastly WASM clippy configurations, 265 guest
library tests and 36 guest contracts without ignored or filtered cases, plus docs/legacy guards.
The production-client progress suite passes all twelve probes with eighteen origin requests;
the unchanged CI assertions pass. Its expected exit 2 retains `qualification: false`, not a
qualification claim. The requirement inventory and frozen requirements/Interfaces are unchanged.

### P5 Provider-Boundary Controls (2026-10-27)

This test-only checkpoint exercises the existing actual provider clients through the shared
Send/Follow pipeline. It adds no transport, implementer API, provider setting or compatibility
path. Production behavior, SDK/decoder/runtime pins and every existing outbound guarantee remain
unchanged. Local validation uses Rust 1.98.1, Node 24.20.0, Spin 4.1.0 and the explicitly selected
approved Viceroy repair; it does not deploy a qualification build.

Cloudflare's eight new guest tests run `CloudflareOutboundClient` against a manual-fetch fixture,
not a supplied-client substitute. They compare declared and undeclared identity, gzip, deflate
and unsupported-coding responses at the final buffer cap; cover unread pending, erroneous, empty
Brotli and oversized intermediate heads; retain exact stopped/limit-reached failures and hop
indices; and check streamed final parity, original deadlines, replacement requests and drop
cancellation. Fixture teardown restores globals and settles pending fetch/body callbacks.
The complete guest suites pass 82 library tests, 29 contracts and twelve Node fixture tests.
Abort signals and zero source pulls are guest observations, not workerd/origin cancellation proof.
Duplicate or invalid-byte Location evidence unavailable through JS normalization remains covered
only at the portable observable boundary, not certified as original Cloudflare wire evidence.

Spin's existing local wire component now performs 68 P5 exchanges with 84 correlated real HTTP
requests. Buffered/streamed no-redirect identity, gzip and unsupported-token passthrough controls
compare exact bytes/headers and typed final, decoded and encoded caps. Actual followed pending,
source-error, empty-Brotli and 10 MiB declared heads progress without intermediate body data;
plain Send, policy stop and limit-reached controls retain their ordinary errors. Two origins,
stop metadata and streamed final parity are observed through Spin's SDK. Existing bodyless,
upload, canonical-scope and attached Begin/Batch handoff controls still pass. Failure cleanup
releases pending gates, stops Spin and joins both origin servers and their request handlers.
Queue/failure checks run only after those joins, and an unexpected runtime exit fails the gate
without replacing an earlier test failure. Injected late-worker and unexpected-exit controls
first reproduce false-green outcomes, then are rejected after the fix; the normal suite passes.
The existing Spin CI job now executes this harness with the checksum-verified pinned CLI;
local macOS execution is not evidence of Ubuntu execution or hosted cancellation.

Fastly's existing production-client progress suite preserves its twelve probes and eighteen
origin receipts, and adds a separate twenty-three-probe P5 inventory with forty-three receipts.
One case inventory feeds execution and validation. Real unsupported-token passthrough uses normal
Decode, not Original mode. Validators require exact typed cap causes and reject incomplete,
duplicate, foreign-token, wrong-mode and substituted evidence. Two-origin Send/Follow and a gated
pending oversized compressed head pass. Native optimizer Send, initial Follow and a policy-
supplied later hop reach Viceroy's typed unavailable boundary; these are compositional dispatch
controls, not a successful optimized-first-hop redirect, transformation or performance proof.
The existing CI predicate requires both inventories, rejects missing/failed/mismatched P5
evidence and retains the expected unqualified exit 2. No prior assertion is relaxed.

Independent specification and quality review tightened cap attribution, Follow terminal metadata
and fixture cleanup. A negative regression first accepts an invalid overflow hop and fails;
the fixed validator separately requires correlated completed-Follow metadata or the exact
head/finalization-failure index, without contradictory fields. All eight validator tests,
149 harness tests and 29 qualification-enabled fixture-core tests pass. The parent reran the
actual Spin and Fastly suites, all five root Rust gates, affected WASM checks and the full
Cloudflare suites. Docs lint/format/build, workflow lint and dependency/evidence guards pass.
Remaining P5 work includes workerd/origin parity,
successful native optimizer redirects, conditional idle-policy cases, complete criterion-level
acceptance binding and consumer/hosted qualification. The G1 inventory remains eleven ready legs,
500 planned and `qualified: false`; these controls do not activate incomplete declared legs or
freeze an exact candidate. The normative requirements and approved Interfaces are unchanged.

### P5 Local Workerd Follow Checkpoint (2026-10-27)

The existing Rust `outbound-wire` suite now also selects Cloudflare. Its fixture-only
`/progress/redirect` action uses the canonical request-context client, without a replacement Fetch
or another outbound transport. One matrix and the same bounded origins exercise both providers.
Local workerd 2026-09-10 with Wrangler 4.131.0, worker 0.8.5 and the existing compatibility settings
passes 68 exchanges/84 ordered origin requests. Spin retains the same 68/84 inventory and its four
bodyless, upload, scope and SDK-terminal handoff controls. Exact coding/cap parity, unread
intermediate-head progress, two-origin behavior, stopped/limited final bodies and streamed final
responses are checked. Truncated reads retain exact provider-specific reasons; workerd's observed
sanitized cause is pinned by negative validation controls rather than accepted as arbitrary text.

Startup and teardown use the existing staged build, runtime and process supervisor. Cleanup begins
before local binding setup and handles an absent runtime/origin without discarding the primary
failure. Controlled Rust regressions reject premature exits, including 143; a wrapper's 143 is
accepted only following a confirmed sent TERM. Missing tools cannot publish a passing inventory.
The matching locked wasm-bindgen CLI was rebuilt offline in a temporary directory, not installed
globally or repinned. No deployment, SDK/runtime pin change, capability promotion, candidate freeze
or hosted qualification occurs. Successful native optimizer redirects, conditional idle-policy
coverage, complete criterion-level binding and consumer/hosted acceptance remain open. The G1
inventory remains eleven ready legs, 500 planned and `qualified: false`.

### Standalone Test Harness and Local Redirect Bindings (2026-10-27)

The shared native runner now lives at `crates/edgezero-test-harness`, with its own unpublished
workspace and Cargo lockfile. The old runner package and source directory are removed without an
alias. Unit/contract tests remain in their owning crates, and deployable applications remain in
the fixture workspace. Fixture cwd, fixed build target, staging and `.runs` locations are unchanged;
the launcher resolves the repository before selecting its toolchain. Both fixture and harness
lock identities are recorded separately. The fixture lock only loses packages made unused by
the move; existing dependency versions, decoder/SDK/runtime pins and frozen documents are unchanged.

All four adapters now execute the same 68-exchange/84-receipt redirect matrix. Spin retains its
bodyless, upload, scope and SDK-terminal controls; Fastly retains its separate progress and native
optimizer-unavailable inventories. Exact provider-specific failures remain enforced. The existing
Rust tests add the literal two-hop query-redaction and signed-date-header controls, plus first-hop
cancellation, without replacing the older controls. EZ-9 unit criteria 01-15 bind all 51 guarded
tests, and local runtime 16/17 bind six environment legs. Together with EZ-23's eleven legs, the
inventory has 32 ready and 479 planned; no applicability approval, candidate freeze, deployment,
provider capability promotion or qualification is inferred. EZ-14 remains an unadopted follow-up.

CI and active guides use the new manifest and separately fetch, format, lint and test each
workspace. Historical commands and audit paths remain revision-specific records. Axum/Fastly/Spin
runtime jobs use the declared redirect output paths; workerd runtime CI activation remains pending
its explicit tool/runtime setup, separately from the passing local command. Remaining lifetime,
registration, consumer and hosted acceptance work stays open in the owning tasks;
this relocation does not mark the complete adoption plan finished.

### Begin/Batch Lifetime Probe Checkpoint (2026-10-27)

The standalone Rust harness now has `--suite outbound-lifetimes`, using the same bounded
origin collector, parser, process supervision and production-client guests as the redirect
suite. It implements the four fixed EZ-6 scenarios 1, 2, 4 and 5, with independently measured
receipts and writes, guest entry/completion times, original cutoffs, typed mediator failures
and exact mixed collection states. CPU-stretch and the other lifetime scenarios remain open.
Clock alignment is explicit, transit uncertainty remains unknown, and schedule timestamps
never substitute for actual observations. Quiet-window proofs use a conservative causal bound,
not an assumed common host/guest clock origin. Cleanup failures prevent passing inventories.

Local characterization above `b5f94c70` found that all four adapters pass the Begin/unobserved
batch progress cases. Slow/fast timeline runs do **not** pass their strict timing targets;
those misses remain failures. Spin additionally loses the slow-origin batch: the producer's
first poll occurs only after the origin head, after the original cutoff, so no bidder request
is sent. Temporary instrumentation reproduced the cause at the existing SDK boundary:
`wit-bindgen 0.57.1` drains newly spawned tasks after returning Pending without arranging
another poll before waiting on the old host operation. The temporary prints were removed.
Current upstream code drains that work before sleeping; integration of a compatible audited
fix is a separate dependency decision, not a reason to drive producers from observers,
add another executor, extend deadlines or weaken the outbound requirements.

The existing redirect regression and fixture/harness unit checks remain separate from these
failed acceptance runs. No new leg is marked ready: the inventory remains 32 ready and 479
planned, with qualification false. Candidate freeze, hosted evidence, provider capability
promotion and optional idle-body adoption remain unchanged. The SDK repair and exact timing
evidence are required before these scenarios can close their owning D6/Q3 work.

### Spin SDK Eager Progress Dependency Checkpoint (2026-10-27)

The user requested a Stackpop SDK fork and upstream contribution rather than vendoring.
The coordinated dependency is now SDK 7 from `stackpop/spin-rust-sdk` at signed revision
`e65ff90dadf4173b08b14194e878ad3acd555337`; the proposed upstream repair is
[spin-rust-sdk#169](https://github.com/spinframework/spin-rust-sdk/pull/169).
The [dependency audit](../../audits/2026-10-27-spin-sdk-eager-progress.md) records its exact
scope, regression and verification. WASIp3 0.9.0 retains WASI 0.3.0 interfaces and selects the
corrected wit-bindgen 0.62.0 executor. The task queue makes progress without observer-driven
producers, another executor, compatibility aliases or a transport workaround.

Root, demo, reusable fixture and scaffold pins use the same Git source/revision; all three
application lockfiles resolve that SDK and macro. Active calls migrate to `spawn_local`.
Generator guards compare source and revision as well as package version. SDK validation
reproduces the exact regression failing on the old dependency chain, then passing with the
upgrade; its workspace tests, doctests and WASM builds pass. Upstream Rust 1.94 CI remains
separate from local Rust 1.98.1 verification, and generated gRPC Clippy warnings reproduce on
unchanged upstream. The private task-context guard does not dereference executor internals.

Local candidate characterization removes the slow-origin batch starvation and retains the
existing Spin wire and eager ownership controls. Strict timeline target misses remain failures;
this checkpoint does not close D6/Q3 or promote hosted cancellation, scheduler guarantees or
resource accounting. Original deadlines/cutoffs and all outbound requirements remain unchanged.
The frozen requirements/Interfaces and inventory are unchanged: 32 ready, 479 planned,
qualification false. No deployment is authorized by this dependency repair.

Final adoption checks pass all five root Rust gates, both affected Spin WASM Clippy combinations,
35 contracts, 21 SDK-resource cases and the eager component on exact Wasmtime 48.0.2. Demo core
(60), fixture core (42), generated core (33), generated Spin build/strict Clippy, dependency and
documentation guards pass. The full ignored generated-workspace matrix was not rerun locally.
Spin 4.1 wire checks pass 68 exchanges/84 receipts; the four lifetime cases remain two passes
and two strict timing failures. A pre-existing trailing worker diagnostic was reproduced after
requested shutdown with identical guest artifacts; retain that caveat, not a clean-hosted-
shutdown claim. These checks do not change any acceptance binding or qualification gate.

### Main Integration Checkpoint (2026-10-28)

Merge main `87456c70` into the signed-SDK checkpoint `7b68f737`, preserving the existing
outbound implementation and hard-cut APIs. Main's request-fidelity types remain available
under `edgezero_core::ingress`, alongside route-aware admission. Canonical adapters admit
before running the pre-dispatch hook exactly once, before state, middleware, handlers or
fallback draining. Refusal/abort remain zero-read paths without hook invocation. Direct
router dispatch runs the hook before lookup; method/path continuation guards remain enforced.

All four adapters capture metadata on their existing lazy conversion paths. Cloudflare head
conversion failures use detached Send/Abort policy and owned completion, sharing one runtime
context. Migrate main's fixture/macro callers to fallible assembly and adapter-owned egress;
extend the structured SDK guard to the fourth application lockfile. Keep main's independent
docs-tool updates and all existing outbound CI gates, separating browser ingress tests from
Node contracts rather than replacing either suite. Historical fidelity observations are not
candidate evidence. No outbound, compression or signed dependency pin changes are authorized
by this integration.

The unfinished D6 pending-head/body probes are parked separately during this merge. They are
not part of the merge checkpoint and must retain their unresolved timing/evidence regressions
when resumed. The frozen requirements, interfaces and acceptance inventory remain unchanged;
hosted qualification, runtime promotion, candidate freeze and optional EZ-14 stay open.

Local verification passes the five primary Rust gates (1,067 core tests), strict Clippy on
all three provider WASM targets, strict public Rustdoc, the demo's 60 core tests, and the
retained shared harness (203 tests, one ignored). Axum's consolidated real-TCP suites pass;
provider contracts pass Cloudflare 30, Fastly 36 and Spin 35, with 271 Fastly library cases.
The five Cloudflare browser regressions pass separately from Node contracts. Structured
dependency, decoder/map, docs/legacy/evidence guards and docs lint/format/build pass. Changed
CI workflows pass actionlint; two existing shellcheck warnings in untouched deploy-docs remain.

All existing request-fidelity observations compare unchanged on local repaired Viceroy,
Wrangler/workerd and Spin. The fixture explicitly drains the two known-empty framing bodies
under a zero-byte cap, preserving failure on nonempty data. This avoids a reproduced Wrangler
proxy 503 sequence after unread cancellation; it does not restore eager production reads,
accept 503 as a baseline, or claim unread-body cancellation evidence. The full ignored
generated-project matrix and hosted qualification were not rerun by this merge checkpoint.

### D6 Pending Cutoff Probe Checkpoint (2026-10-28)

Resume the parked lifetime-probe work above main-integration commit `f822e408`. The existing
Rust harness and production-client fixture now run six cases, adding pending response heads
and host-written heads with permanently withheld bodies. Both keep the original request-start
200 ms cutoff, require a genuinely Pending `next()` poll and `Cutoff` by 205 ms, and preserve
three unresolved slots. Origins remain held until the complete guest response returns to the
host runner, then release and join through the existing bounded collector.

Two independently reproduced red regressions are repaired: host validation no longer accepts
an origin phase reached only after guest-response return but before later fixture release;
guest timing captures operation return before result formatting. The one private Timeline API
uses explicit endpoints throughout, including fractional-microsecond reconstruction. Negative
tests cover missing/invalid boundaries, every role and both cases, whole phases shifted past
return, premature release, and tied timestamps with reordered events. Existing strict bounds
and the original four case assertions remain unchanged.

One local characterization per adapter records these guest return offsets from request start:

| Local Runtime | Pending Head | Pending Body |
| ------------- | ------------ | ------------ |
| Axum | 202.668 ms | 201.179 ms |
| Repaired Viceroy 0.21.0 | 201.182 ms | 201.194 ms |
| Spin 4.1.0 / signed SDK revision | 202.133 ms | 202.983 ms |
| workerd 2026-09-10 / Wrangler 4.131.0 | 202 ms | 204 ms |

Every run has four passing cases and two failing original slow/fast timeline cases; all four
suite exits remain 1. These are not passing six-case inventories. Complete local artifacts are
under `/private/tmp/edgezero-d6-return-boundaries-20261028/{axum,viceroy,spin,workerd}`. Host
response-return and release offsets stay in their own clock domain. Unit tests pass 212 shared
harness cases (one ignored) and 43 qualification-enabled fixture cases; both scoped strict
Clippy checks pass. Existing CI already executes those test groups; no per-case jobs are added.
Final verification also passes the five primary Rust gates, fixture strict Clippy on all three
WASM targets, formatting across all affected workspaces, and the legacy/docs/inventory guards.

Pending-body evidence stays explicitly `host-head-written-only`, not proof that the SDK
received the head or entered a body read. Full criterion 16, the other D6 scenarios and strict
timeline closure remain open. No production outbound implementation, dependency pin, frozen
requirement/interface or acceptance binding changes; the inventory stays 32 ready and 479
planned, unqualified. No deployment, candidate freeze or capability promotion is implied.

### D6 CPU Stretch Characterization And Calibration Gate (2026-10-28)

Add the seventh local lifetime case for existing EZ-6 scenario 3, above `26e9e6b7`. It keeps
the original batch start/cutoff (34/534 ms), consumes A before bounded synchronous CPU work
from 520 to 560 ms, then collects. The work has no awaits or I/O, a ten-million-iteration cap,
and explicit rejection of frozen, backwards or expired clock windows. It uses the application's
existing clock/client and the same source ownership; no public API or production hook is added.

The validator derives a causal interval for `host - guest`, with an explicit equal-rate
assumption and 1 ms rounding guard. Every possible offset must place B's complete actual write
inside both CPU work and the unchanged 530 +/-5 ms target. Scheduled timestamps and a convenient
point estimate cannot supply that proof. Separately, A retains its 146 +/-5 ms elapsed check;
delivered B retains 496 +/-5 ms, elapsed strictly below 500 ms and absolute arrival strictly
before 534 ms. An unresolved B is permitted only with the complete source precondition proved.
C's complete/partial peer-close paths reject early bodies hidden behind a late connection close.
Receipt reconstruction brackets core's batch epoch by the full measured call interval; every
possible arrival must meet the cutoff and delivery boundaries. The pre-call sample is not
treated as core's exact epoch.

Red/green regressions cover both permitted B branches, expired work windows, receipt chronology,
collection-stamped A, early C writes, interval extrema, cold epoch shifts, invocation-epoch
uncertainty and uncertain source timing. Shared harness tests pass 223 cases (one ignored);
qualification-enabled fixture tests pass 47. Scoped strict Clippy passes natively and on all
three WASM fixture targets.
All five primary Rust gates pass, including 2,568 workspace tests (one ignored), after rerunning
TCP/process tests with their required local permissions. Formatting, docs lint and the
legacy/docs/inventory guards pass; the 36 evidence-checker regressions also pass.

The first local CPU characterizations remain **failed/unqualified**. The source's initial
cold-connection alignment is too wide/late to prove the required B write:

| Local Runtime | Mapped B Write Interval | CPU Interval | B Outcome |
| ------------- | ----------------------- | ------------ | --------- |
| Axum | 533.322..542.720 ms | 523.207..560 ms | Unresolved |
| Repaired Viceroy 0.21.0 | 532.918..542.897 ms | 520.164..560 ms | Unresolved |
| Spin 4.1.0 | 529.763..537.807 ms | 521.943..560 ms | Unresolved |
| workerd 2026-09-10 | 532.657..539.969 ms | 521..560 ms | Unresolved |

These failed preconditions do not establish a production batch defect. Fastly also records A
only at the later observation after the fixture's unrelated wait; a CPU acceptance setup must
keep the pre-CPU wait inside an EdgeZero operation if it requires arrival recording there.
All four full seven-case runs exit 1: the earlier slow/fast misses remain, four controls pass,
and CPU source timing fails. Artifacts are under
`/private/tmp/edgezero-cpu-stretch-20261028/{axum,viceroy,spin,workerd}`.

A bounded fixture-only calibration send through the same client has been proposed in the
conversation; it is **not implemented or approved by this checkpoint**. It can tighten the
causal bracket and provide the pre-CPU EdgeZero wait without resetting the batch's deadline.
Keep the approval/measurement-definition gate explicit rather than relaxing targets. The native
SDK body-entry evidence gap also remains open: no uniform existing observation seam was found,
and host-written heads still do not close criterion 16.

No frozen requirement, interface, dependency pin, capability or acceptance binding changes.
Inventory remains 32 ready/479 planned and unqualified. No deployment, candidate freeze or
hosted qualification is authorized by these local probes.

### D6 Same-Client Calibration Checkpoint (2026-10-28)

The requester approved one bounded fixture-only calibration Send after the `44baaf3b`
checkpoint. This approval does not change the original scenario targets, frozen requirements,
interfaces, G4 measurement-approval status, runtime pins or qualification gates.

The CPU guest now consumes its origin, then sends one request through the same injected client.
It carries the captured origin-head return sample and its own earlier request-construction
sample, keeps the 64-byte buffered cap and 1-second timeout, and adds an absolute
request-start-relative 520 ms safety deadline. The bodyless `204` completion is sampled before reporting work.
The source publishes a separate calibrated scheduling anchor before holding that response,
and A/B/C boundedly await it rather than falling back to the cold anchor. Five fixed roles
produce at most 19 events under the existing worker, connection and 32-event limits.

The causal upper bound intersects the original and calibration receipts; the lower bound uses
the actual origin-head write and captured guest observation. The midpoint only schedules
A180/B530/C900. Every possible offset must still place B's complete actual write inside the
CPU interval and the original 530 +/-5 ms target. Receipt reconstruction retains the complete
batch-invocation epoch bracket, strict arrival before 534 ms, and elapsed below 500 ms.

The initial calibration response hold to `500 ms + U` shifted the original next500 target
on three runtimes. Keep those failed runs under `/private/tmp/edgezero-calibration-20261028`.
The corrected setup holds calibration to `450 ms + U`, proves A's complete arrival interval
inside that Send, then uses the existing timer to reach the unchanged next500 target. No
prescribed completion occurs during this unrelated wait before B530. CPU520..560, the
34/534 ms batch boundaries and all original receipt tolerances remain unchanged.

Independent review reproduced a further false positive: calibration receipt could precede
the origin payload write. Source and validator now require native ordering against a pre-write
chunk sample. A post-flush timestamp is unsuitable because source-thread preemption could put
that sample after a legitimate receipt. This is necessary ordering, not proof of peer receipt.
Red/green tests cover the false positive and the earlier-control refinement. The 229 harness
tests (one ignored) and 48 qualification-enabled guest tests pass; narrow hostile-evidence
cases retain missing/duplicate/mismatched samples, exact release boundaries, wide clock bounds,
strict cutoff equality and the finite CPU-work checks.
All five root Rust gates pass, including 2,568 workspace tests (one ignored). Strict native and
three-target fixture Clippy, formatting, docs lint, legacy/docs/inventory guards, and the 36
evidence-checker regressions pass. Independent guest, host and documentation reviews are clear.

Final local working-tree characterizations remain separate from qualification:

| Local Runtime | B Write Interval In Guest Time | CPU Case |
| ------------- | ------------------------------ | -------- |
| Axum | 531.190..537.288 ms | Fail: source window not proved |
| Repaired Viceroy 0.21.0 | 530.010..537.326 ms | Fail: source window not proved |
| Spin 4.1.0 | 530.770..537.093 ms | Fail: source window not proved |
| workerd 2026-09-10 | 528.539..532.639 ms | Pass, unqualified |

Artifacts: `/private/tmp/edgezero-calibration-final-20261028/{axum,viceroy,spin,workerd}`.
All four full seven-case suites exit 1. The four controls pass on each target; the previous
slow/fast timing misses remain, independently of CPU results. All B slots remain unresolved.
An earlier intermediate Axum CPU pass is retained, not substituted for its final failed run.
Neither a passing local case nor a failed fixture timing precondition establishes hosted
qualification or a production outbound defect.

The shared origin still polls accept with a 5 ms delay, contributing fixture-side uncertainty.
A bounded readiness/wake replacement has been proposed separately and is not implemented or
approved by this checkpoint. Same-client use does not guarantee connection reuse or sufficient
precision. Keep that fixture gap and native SDK body-entry evidence open. Inventory remains
32 ready / 479 planned, unqualified; no deployment, candidate freeze or capability promotion.

### Current Work Queue and Gates

The Spin host wire suite now uses the existing Rust fixture harness's explicit
`--adapter spin --suite outbound-wire` entrypoint. The former Python runner is removed, without
compatibility wrappers or another process supervisor.
The unchanged guest retains 68 exchanges/84 correlated requests, raw framing and scope/handoff
assertions. Colocated Rust tests cover capture boundaries, duplicate fields, exact bytes, late
worker failures and process-group shutdown. The existing Spin CI job executes the shared harness
and checks its complete inventory. This tooling migration does not change production outbound
behavior, frozen requirements, provider capabilities or qualification status.

1. **G1/Q3 acceptance binding:** replace planned placeholder commands with actual, nonzero
   sentinels and environment-specific assertions/artifacts as each owning task lands. Reconcile
   applicability/approval records separately; mark a leg `ready` only when its complete declared
   command/assertions are implemented. Historical characterization is not exact-candidate acceptance.
2. **P1-P5 and D2-D4 integration:** bind mode/lifetime acceptance for the locally verified shared
   invocation scope and canonical body handoff. Complete all-target runtime acceptance of the
   implemented explicit same-client Follow policy.
   Do not restore lazy compatibility implementations, duplicate schedulers or unused foundations.
3. **Fastly D1 prerequisites:** the stock readiness/error-preservation failure still gates its
   hosted/runtime promotion. The scoped B31 policy and current-operation multiplexer are approved
   for implementation; local/CI receive validation explicitly uses the tracked preserving repair.
   The isolated runtime repair is not a changed production pin or resource qualification.
   Callback-wide scheduling and shared Send/Begin/Batch ownership are integrated; verify the body-first
   progress family and the approved dispatch-slack retirement. G2 does not
   globally prohibit approved portable or other-adapter work; no lost-error fallback is allowed.
4. **Q1/Q2 then Q3-Q5:** update examples/docs/checks with each implementation change. Finish source
   work before freezing and qualifying a candidate. Hosted deployment remains unauthorized;
   entitlement, operator acceptance, measurement definitions and provider evidence remain open.

For every subsequent behavior change, update the affected requirement row and checkpoint in the
same commit, naming its implemented scope, nonzero tests/evidence and next unclosed step. Reconcile
its inventory bindings rather than reporting a percentage from checkbox or test counts. Close a
parent task only after every applicable child and acceptance leg closes. Requirements/digest
changes need explicit review; neither a dashboard edit nor a passing checker supplies approval.

### Shared Ownership Hard Cut (2026-10-27)

- [x] Share validation, GET/HEAD settlement, raw-head normalization, decoding and limits across
      Send/Begin/Batch; keep direct native Image Optimizer dispatch unchanged.
- [x] Capture complete unpolled operations at invocation. Revoke retained handles through one
      core implementation without discarding recorded batch partials, receipts or typed errors.
- [x] Transfer exact provisional Body ownership before egress policy/framing. Keep HEAD/bodyless
      work until terminal; cancel a failed application's work before fallback while holding
      response completion through fallback's SDK terminal. EOF alone does not complete egress.
- [x] Use one Body source binder and fallible local task runner, including executor exit,
      reentrant cancellation and supported cleanup unwind. No copied provider lifetime state.
- [x] Keep readiness probes observation-only and the literal 2/34/120/134/260 ms timeline;
      preserve 146/286/366 ms receipts and the original mediator deadline without restart.
- [x] Retire Fastly's slack rejection under the separate approval. Share the earlier-own-deadline
      cutoff predicate; retain B31, equality expiry and backwards-clock rejection.
- [x] Migrate and verify supplied clients, all four adapters, demo, generator, fixtures, guides,
      changelog and drift guards. No compatibility alias or second transport is retained.

Read-only reviews covered core/protocol, Axum/Cloudflare, Fastly, Spin and callers separately.
They found body-handoff, cleanup identity/unwind, nonblocking SDK-turn and duplicated task/timer
paths; regression proofs and the shared fixes above close those implementation findings.
Local parser/TCP probes required listener permission; the permission-denied sandbox run is not
passing evidence. The authorized loopback rerun passes. None of these results freezes a candidate,
promotes capabilities, closes the failed historical SDK parity controls, authorizes deployment or
changes the inventory's 500 planned legs / `qualified: false`. Follow and Q3-Q5 remain open.

### Post-Merge Guest CI Repair (2026-10-26)

At merge `b7a288d2`, the primary workspace gates passed but two additional CI jobs failed.
The Fastly CLI-only lint configuration exposed declaration ordering hidden by the runtime
feature's ordering exception. Reorder declarations without changing their bodies and apply
the existing conflicting module-file lint exception to the matching native test configuration.
Do not weaken ordering checks or rewrite the earlier source-hashed multiplexer evidence.

Cloudflare's browser fixture lacked Workers' `FixedLengthStream`; its native request-body
presence check also failed before entering adapter conversion. Run the guest contracts on
the pinned Node host already used by the library tests, retaining the native body-presence
tripwire and sharing the existing scoped numeric-timer guard. A test-only fixed-length stream
model exercises the response writer without a production fallback, bodyless error renderer
or request-body polyfill. Its nine JS tests cover exact/zero/bigint lengths, both length errors,
backpressure, reader cancellation, writer abort, invalid inputs and existing-host preservation.
CI runs that suite alongside the existing nonzero WASM contract sentinel.

Keep exact response-header map comparisons and explicitly expect canonical buffered lengths
on overflow, timeout and admission-refusal responses. Keep exact payload, status, admission,
source-release and completion assertions. Local Node execution passes all 29 contract cases;
the prior Firefox run and its body-presence failure are diagnostic evidence, not passing
provider qualification. No production response-conversion, SDK pin, capability or frozen
requirements/interface change is introduced. Full head-first/callback ownership integration,
dispatch-slack retirement, candidate freeze and hosted qualification remain open.

Local repair verification passes all five primary Rust gates (2,257 workspace tests, one
existing ignored test), Fastly CLI-only/default/test-utils Clippy, both Cloudflare WASM Clippy
configurations, 210 Fastly WASI unit tests, 29 Cloudflare contracts, 48 Cloudflare library guest
tests, all nine JS fixture tests, docs format/lint/build and the documentation/dependency guards.
The actual-client Fastly suite again records twelve passing probes and eighteen origin requests,
with the required unqualified exit 2; this does not replace the earlier failed SDK timing results.

### Fixture Client Selection Checkpoint (2026-10-26)

Remove the Fastly custom fixture's manual clock-only client construction and prepare-time
HTTP-client override. Its request-local capture retains the actual `RequestContext::http_client()`
before the observation middleware awaits `Next`; routed handler errors therefore keep the client
even though the framework renders their response outside that middleware. The finalizer transfers
the captured handle through the existing owned delivery result. Post-send still requires committed
application delivery and the existing local-origin policy, and a missing capture fails explicitly
instead of constructing another client. Detached, no-attempt, abort and fallback gates are unchanged.

Use the existing typed request-extension payload `Arc<Mutex<Option<HttpClient>>>`, not another
transport, public factory or compatibility API. Each callback allocates its own capture and releases
the lock before awaiting. App-state client precedence remains unchanged. Tests cover success/error
rendering, effective request/state client selection, one consuming transfer, two-request isolation,
and absent/unrouted clients without a fallback transport. The capture regression failed before
middleware integration and passes afterward. The actual local Fastly smoke suite preserves
progressive/large delivery, source-error reporting, post-send work, fallback exclusion and retained
callback behavior. This is fixture preparation for the paired-time/full-protocol migration, not
completion of P1/D1 or promotion of any acceptance/hosted qualification leg. Production APIs,
default clients, clocks, caps, direct image optimization and frozen digests are unchanged.

Verification passes all five primary Rust gates (2,272 workspace tests, one existing ignored),
all 23 fixture-core tests with qualification enabled, fixture-core/Fastly guest Clippy, and
all four fixture target builds. The real local Fastly smoke suite exits 0 with its existing
progressive/source-error/post-send/fallback assertions intact. Docs format/lint/build and
the outbound documentation/inventory guards pass; the inventory remains unqualified. Two
independent read-only reviews found no actionable issue in the fixture implementation.

### Existing-Batch Scope Checkpoint (2026-10-26)

Implement the private D1a ownership boundary for complete current batch drivers, without
splitting the frozen public protocol. `Sandbox::handle` installs a fresh callback guard for
both custom wrappers. Canonical send helpers install a fallback only outside an existing
scope. Each response transmission has an independent owner for work created during body
production, restored to the active callback at transmission exit. Saved clients resolve
scope at batch creation, not client construction or later driver polling.

Weak registrations track only live driver roots and unregister on terminal/drop paths.
Scope exit first revokes every root, then drops whole streams outside registry/driver borrows,
including queued results, pending processing and decoder state, and wakes waiting consumers.
The closing scope stays active until cleanup ends, rejecting reentrant Send/Batch starts before
clock/preflight/provider work; nested cleanup scopes cannot reopen it. Waker cloning/destruction
runs outside driver borrows, with a final closure check preventing a stranded Pending result.
A retained
handle emits one typed Internal scope-closure failure instead of Cutoff or clean EOF. Core's
existing poison and partial-failure collection rules remain unchanged. Closure after the last
item but before validated EOF is still a failure, never inferred completion.

The actual Fastly regression `callback_scope_exit_releases_retained_batch_driver` first failed
because the unpolled driver's captured clock owner survived callback exit, then passed after
integration. It uses queued preflight results to avoid claiming SDK-harvest coverage. The
native response regression `body_created_batches_end_at_egress_without_closing_callback_work`
likewise fails without the transmission guard, then passes for completion, source/write/finish
failure while callback/post-delivery work stays live. The callback batch is polled inside body
production without being reparented. Colocated tests additionally cover
Ok/Err/supported unwind, partial collection, last-item/EOF, wakeups, unregistering, helper-scope
reuse and the real-client `retained_client_resolves_fresh_callback_scope` across two invocations.
The shared production/test helper lifecycle has a WASI fallback/reuse contract. Reentrant
destructor and waker-drop regressions were observed failing before the cleanup-order fixes.

This does not complete D1a, P1/P4 or an acceptance/qualification leg. Retained Send roots,
response ownership transfers, callback-wide scheduling, unpolled eager modes, arrival recording,
paired-time/trait migration and dispatch-slack retirement remain open. Neither dropping an SDK
handle nor local cleanup proves host cancellation. No public API, SDK/runtime pin, B31 policy,
Image Optimizer path, frozen requirement/interface digest or capability level changes.
Demo/templates need no API migration for this adapter-private checkpoint; their forthcoming
head-first migration remains atomic with all implementers/callers. Hosted deployment is not run.

Verification passes all five primary Rust gates (2,272 workspace tests, one existing ignored
test), Fastly CLI-only Clippy, WASI Clippy with/without test-utils, 226 Fastly WASI unit tests
and 36 WASI contracts. Docs format/lint/build, legacy/API/docs/decoder guards and unchanged
frozen requirement/Interfaces-plan hashes pass. The actual-client functional suite again records
twelve passing probes and eighteen origin requests with the expected unqualified exit 2.
Independent review reproduced the reentrant failures before correction and cleared the focused
scope implementation afterward. Existing SDK timing failures and hosted gates remain unwaived.

### Current-Operation Multiplexer Checkpoint (2026-10-25)

Implement the requester's [scoped approval](../../audits/2026-10-18-outbound-api-approval.md#scoped-fastly-current-operation-multiplexer-approval-2026-10-25)
in the live Fastly Send/Batch paths. A single private per-operation engine services raw batch
heads and demanded response bodies using validated borrowed-handle timed selection. Each raw
body read consumes fresh matching readiness and reads at most 16 KiB. Rotate slots, retain only
one demanded chunk per body and treat occupied result mailboxes as per-slot backpressure.
Cached SDK Send heads remain opaque and use nonblocking polling with one shared positive 1 ms
pulse. Remove the old blocking head waits, selected-body drains and ownership-reassociation code.

Preserve B31, original logical deadlines and causes, observation cutoffs, partial batch results,
application clocks, all payload/header/codec limits, provenance and direct native Image Optimizer.
Sample the clock after I/O and reject late success while preserving typed source-error details.
Local stream/lease ownership drops guest-side SDK owners on terminal/drop paths without a registry cycle.
Core drops batch drivers before returning terminal/poisoned observations, including retained
batches. The outer Fastly deadline guard retains typed source details and drops the source before
yielding terminal errors. Dropping guest owners does not prove that pending host work is cancelled.
SDK cleanup, cold registration, streamed writes, blocked inbound pulls and the native optimizer
retain their separately disclosed non-preemptible boundaries.

The real-client mixed-body/head regression failed against the prior serial drain. Deterministic
transport tests exercise fairness, per-read permissions, deadline equality, clock regression,
spurious readiness and cancellation. Review additionally found an occupied-mailbox starvation
bug; a bounded-poll red/green test now covers it. The runtime fixture also checks exact payloads,
typed multi-buffer source errors and request-correlated bounded origin receipts. Local timing
or preflight failures remain failures, not evidence of passing progress.

CI builds the exact Viceroy source plus tracked repair in isolation, verifies source/lock/patch
identities and upstream readiness tests, then selects its absolute executable explicitly.
The stock installation and dependency/runtime pins remain unchanged. Ordinary CI selects the
explicit functional-only suite and requires its complete passing client inventory, no failure
records and exactly the exit-2 unqualified disposition. The full SDK timing diagnostic stays
separately available with its strict assertions and exit behavior unchanged.

This does not complete D1a, P1 or any acceptance/qualification leg. Callback-wide scheduling,
independent callback/body cancellation owners, eager Begin, prepared head-first dispatch and the
atomic four-adapter public trait/caller migration remain open. So do dispatch-slack retirement,
total resource accounting and hosted qualification. No compatibility branch, alternative
transport, capability upgrade or frozen Interfaces/requirements change is introduced.

Verification on 2026-10-26 passes all five primary Rust gates: 2,257 workspace tests with one
existing ignored test, strict Clippy and all required feature/Spin target checks. Fastly passes
210 WASI unit tests and 35 runtime contracts under the repaired runtime. The standalone harness
passes 135 tests and strict Clippy; docs format/lint/build, docs/legacy guards, decoder/map audits
and the 34 cutover-evidence tests pass. The requirements and complete frozen Plan 02 hashes are
unchanged.

The [new local record](../../audits/evidence/2026-10-26-fastly-cooperative-progress.json) retains
all five runs, including the serial-drain red result and failed integration attempts. Both final
unchanged-build runs pass all twelve actual-client cases with eighteen correlated origin
requests, preserving exact bytes and typed buffered/streamed truncation errors. However, the
full 153-probe SDK diagnostic exits 1 in both runs: the first misses two +5 ms timing targets;
the replication misses twelve. These are not passing full-matrix or qualification results.
The requester directed continuation after the recommended
[functional-versus-timing CI split](../../audits/2026-10-18-outbound-api-approval.md#functional-ci-validation-decision-2026-10-26).
The new Fastly-only `outbound-client-progress` suite reuses the same client probes, validators,
guest and bounded origins, returning before SDK probes or timer controls run. It does not make
the failed full diagnostic pass: no validator is weakened and no failure is relabeled as success.
Four additional parser/aggregation tests cover suite selection, adapter exclusion and retained
failures; all 139 harness tests and strict Clippy pass. The missing temporary repaired runtime
was rebuilt from the exact upstream/lock/patch inputs in an isolated worktree target cache.
Its ten readiness tests, 24 body/collector tests, 210 Fastly WASI unit tests and 35 runtime
contracts pass, including the exact no-config runner form used by CI.

The functional suite passes on that repaired runtime with twelve correlated probes, eighteen
origin requests, no SDK/timer probes or inventories and exactly exit 2. The identical guest on
the unchanged stock runtime loses all six typed truncation errors and exits 1. The exact CI
guard accepts repaired evidence and rejects both stock evidence and an unavailable-runtime
exit-2 run with no inventory. The [same audit record](../../audits/evidence/2026-10-26-fastly-cooperative-progress.json)
retains these controls and all earlier failed full-matrix runs; its 48 client observations do
not qualify timing, callback-wide ownership or hosted behavior.

### Mixed-Readiness Diagnostic Checkpoint (2026-10-25)

Extend the existing diagnostic, not production dispatch: eighteen supplementary probes cover
pending-head/ready-body, pending-body/delayed-head and multi-buffer truncation. Both consumers run
three repetitions at 16 KiB capacity, requiring a real full-sized return. Shared trace validation
checks every live mixed handle, fresh matching readiness, exact payloads/errors and bounded joined
origins with the recorded delayed-head control. Keep the original 135-probe matrix and historical
negative replays unchanged. The [new record](../../audits/evidence/2026-10-25-fastly-mixed-readiness.json)
retains all three runs, including the first repaired timing failure.

The formatted stock/repaired pair has identical guest bytes. Stock loses thirty source errors
(six new and twenty-four baseline); repaired preserves all and passes 153 probes/177 origin
requests, but exits 2 unverified. Its earlier two timing failures remain failures. Every new probe
records a full 16 KiB read; mixed cases complete the ready sibling before the pending slot expires.
This closes narrow local proof gaps only: continuously-ready multi-source fairness, production read
maximum/primitive approval, runtime promotion, resource accounting and hosted qualification remain
open. No full D1a task, acceptance leg or capability is promoted. The public APIs, B31 production
policy, original logical deadlines, all outbound guarantees and frozen requirements are unchanged.

Verification also exposed the existing Axum partial-collection test's 25 ms wall-clock race.
Replace sleep-based ordering with an explicitly gated slow origin and coordinated injected clock:
the actual adapter's fast item reaches `collect()` before time advances to the same logical cutoff.
Paused native timers avoid unrelated real-time expiry; an independent five-second real-clock
watchdog bounds the test's self-waking I/O loop. Preserve the successful first slot and unresolved
second slot assertions. This is a test-only repair, not a new timer API or production behavior.

All five primary Rust gates pass on the stable source, including 2,248 workspace tests and one
existing ignored test. The standalone fixtures pass 128 harness and six core tests; strict harness
and actual Fastly WASI diagnostic Clippy pass. Docs format/lint/build, docs/legacy guards, 34
cutover-evidence tests and frozen approval identity verification pass. The acceptance inventory
remains 255 criteria/511 legs, eleven ready and 500 planned, with `qualified: false`. Independent
reviews cover the trace/receipt changes and the real-adapter partial-collection test separately.

### Existing-Path B31 Implementation Checkpoint (2026-10-25)

The requester explicitly approved scoped B31 production implementation in the
[approval record](../../audits/2026-10-18-outbound-api-approval.md#scoped-b31-implementation-approval).
The existing Fastly Send/Batch path now selects coupled provider timers from the clamped
configured timeout, or its original selected duration when no timeout is configured. True
ceiling-to-millisecond rounding followed by the 31-member ladder never rounds the logical
deadline. Method-entry capture, cutoff eligibility, typed errors and all payload/header limits
remain unchanged. Facade-invocation capture is still part of the later atomic migration.

Remove client namespaces, exact-budget configuration names, per-client caches and the rigid
quarter/three-quarter split. A single verified guest-instance registry serializes registration,
compares complete identities, rejects unknown name collisions and retains a successful late
registration before returning the original timeout. Versioned names hash canonical transport
fields and the chosen timer. The registry retains no request clock, body, callback or exchange.
Request parts move once through preparation; no new public getter, reconstruction, body copy,
transport or compatibility branch is introduced. Direct native optimizer dispatch is untouched.

Behavioral regressions first fail against the old policy, then pass under the real pinned WASI
runtime: equal-timeout/different-deadline identity, 93 boundary samples/31 configurations, coupled
phases and cross-client SDK registration reuse. Controls cover observation cutoffs, maximum
timeouts, preserved parts, collision/failure retry and late-success retention with expired cache
access rejection. Actual SDK readbacks preserve target host/port, authority and TLS for DNS,
IPv4 and IPv6; timing getters remain unavailable. All 202 Fastly WASI unit tests and 35 runtime
contracts pass in the current checkpoint.

Both investigations selected one shared authoritative registry over duplicate client caches.
Separate reviews preserve the scope limits: no readiness, runtime-pin, public protocol, dispatch
slack or capability change. Explicit Connect/idle settings, registration observation, F1 retirement,
total resource accounting, hosted pooling and qualification remain open. No full D1b/EZ-2 task
or acceptance leg is closed by this subset; the original proposal and negative evidence stay
historical, unchanged records.

Verification passes all five primary Rust gates (2,248 workspace tests; one existing ignored),
strict Fastly WASI Clippy, and the Fastly demo target build. Native count is one lower because
the obsolete namespace-isolation test was replaced by the WASI shared-registry proofs. The docs
checker now rejects the two stale public exact-budget/phase-split descriptions; it failed against
the stale footnote and passes after correction. No acceptance leg is promoted by these checks.
The existing Fastly loopback serving smoke also passes at
`/private/tmp/edgezero-fastly-b31-smoke-20261025`. This is serving/lifecycle regression coverage,
not a hosted backend-pooling, interruption or finite-resource qualification result. Demo code
builds unchanged and generator tests pass through the workspace suite; no public API migration
or caller compatibility wrapper is needed for this private policy change.

### G2/G4 Policy Review Packet (2026-10-25)

Historical proposal checkpoint; the scoped implementation status above supersedes only its
implementation-approval gap, not its remaining primitive/resource/qualification decisions.

The [review packet](../../audits/2026-10-25-fastly-backend-policy-proposal.md) proposes literal
B31, selected by the requester for final review on 2026-10-25, with a B1 comparison, full positive
selected-budget/timeout-anchor domains, upward integer rounding and complete coupled timer-vector
counting. Its executable proposal model passes
93 boundary samples and 16 timeout-anchor controls, including equal timeouts across different
deadline-only buckets. This is not production code, SDK setter evidence or policy approval.

Two read-only investigations/proposals reviewed timer/resource tradeoffs and the exact repaired
primitive separately. The packet preserves stock error loss, repaired negative timing traces,
untested full-sized-copy and mixed-head/body fairness domains, SDK/provider distinctions and
Viceroy's unavailable timer getters. No production API, backend, timer, slack, resource limit,
dependency or runtime pin changes. G2's gate is not passed, no acceptance leg is promoted, and
no conditional follow-up is adopted. The next step is explicit candidate/resource/primitive
review in G4, followed by its still-required evidence and the approved atomic migration.

### G2/G4 SDK Registration Checkpoint (2026-10-25)

Add the WASI-only `proposed_b31_timer_vectors_register` to the existing Fastly outbound unit
module, not a production policy helper or alternate backend path. All 31 candidate vectors
register with coupled Connect and with explicit 200 ms Connect (62 registrations); three
independent oversized-duration controls preserve the exact SDK error and input. Removing each
setter makes the corresponding assertion fail, then the restored guarded test passes. The
existing Fastly WASI CI suite includes it without another filtered command or job.

The [feasibility checkpoint](../../audits/2026-10-18-outbound-feasibility.md#b31-sdk-registration-checkpoint-2026-10-25)
records the SDK/runtime/source/guest identities and exact command. This adds real pinned-SDK
serialization/local-registration evidence to the arithmetic proposal, not hosted timer acceptance,
Connect enforcement, pooling, resource certification, readiness/error-preservation or production
policy approval. No production implementation, public API, deadline, source pin or capability
changes; G2/G4, the atomic migration and all acceptance-leg statuses remain open as before.

Verification passes all five primary Rust gates (2,249 workspace tests; one existing ignored),
strict Fastly WASI Clippy, all 194 real Fastly WASI unit tests including the guarded proposal
probe, docs format/lint/build and frozen API identity verification. The selected production-policy
implementation decision remains separate; successful local registration does not authorize it.

### G1/F9b Inventory-Driven Suite Consolidation (2026-10-25)

Replace the binding checkpoint's eleven filtered commands and separate factory guard with one
`--all-targets --features axum,test-utils` suite execution in the existing Axum CI job. The existing
nonzero guard's `--inventory axum/native-tcp` mode obtains ready sentinel names from the existing
evidence checker, validates every exact name in the listing, and executes once with
`--include-ignored`. Criterion 10 records its factory test in `additional_sentinels`; there is no
second CI whitelist or command runner. All host CI legs explicitly install the pinned Node
version because their existing guards already use Node.

Regression tests first failed on the missing inventory mode/schema, then passed after the minimal
extension. Deleting any of the twelve required names rejects execution. Empty ready selections,
malformed additional names and suite failures remain errors. The historical execution and
verification record below is unchanged; the shared command is now the inventory's current binding.
The 255 criteria / 511 legs, eleven ready / 500 planned, applicability, approvals and
`qualified: false` remain unchanged. This consolidation changes tooling and tracking, not outbound
runtime behavior or qualification status.

Verification passes the consolidated 245-unit/17-contract Axum suite, all 44 guard/evidence
regressions, and all five root Rust gates. Independent inventory review confirmed preserved
bindings; portability review found no code defect. Its documentation cross-reference correction
is applied. These are local checks, not new release or hosted evidence.

### G1/F9b Local Acceptance Binding Checkpoint

- [x] Bind EZ-23 criteria 01-11 at `axum/native-tcp` to the existing guarded Cargo tests,
      with explicit filters and `--include-ignored` during both discovery and execution.
      Preserve all 511 case/environment identities, source text/digests, applicability and approvals.
- [x] Strengthen criterion 01 with a 100-byte target accepted by the same unconfigured router.
      Observe the new control fail before adding the test-only route, then pass with configured
      414 through both server paths and unconfigured 200. This is a current positive control,
      not a newly executed historical-F comparison.
- [x] Run all eleven inventory commands: twelve distinct tests pass, including both the supplied-
      client TCP and default-factory failure-seam tests selected by criterion 10. Guard the factory
      sentinel separately in the existing Axum CI job so deleting it cannot silently reduce coverage.
- [x] Observe the inventory-binding regression fail while the legs are still planned, then pass
      after wiring the exact commands into the existing CI job. No new checker, execution framework,
      capability or job name is introduced.
- [ ] Implement criterion 12's begun-request branch after the eager API migration and obtain
      criterion 13's consumer-owned route/parity evidence; both legs remain `planned`.
- [ ] Reconcile the remaining 500 executable bindings and all required applicability decisions;
      freeze and qualify a candidate separately through Q3-Q5.

The inventory now reports 255 criteria, 511 legs, 500 planned and `qualified: false`.
The eleven ready legs point to future raw `.log` artifacts rather than nonexistent JSON exports.
Frozen-candidate execution must capture stdout/stderr there, preserve the guarded exit status
and bind the actual source/build identity through Q3. The local runs are not that record and
neither readiness nor CI supplies promotion/confirmed-present approval for these follow-ups.
Criterion 10 proves avoidance of automatic default transport initialization; its separate factory
fixture may itself construct a client. No network restriction or hosted behavior is inferred.

Only acceptance bindings, tests, CI and tracking documentation change. The production APIs,
transport paths, SDK/runtime pins, frozen Interfaces, requirements and retained failed evidence
remain unchanged. Full P1-P5/D1-D6 integration and every existing Fastly/provider gate remain open.

Verification passes all five primary Rust gates, 2,244 workspace tests (one existing ignored),
245 scoped Axum unit tests and 17 contracts, 32 evidence-checker and seven nonzero-guard tests,
docs format/lint/build, docs/legacy guards and the approval-identity verifier. Independent
inventory and criterion-coverage reviews selected the existing tests/CI path plus the missing
positive control, not a duplicate command registry or execution framework. Subsequent spec and
quality reviews found no remaining scoped defect. Generated-project and hosted qualification
were not executed or credited by this checkpoint.

### Spin Existing-Path Support Precedence Checkpoint (2026-10-25)

The live Spin Send/Batch validators now reject unsupported authority overrides before portable
method/body/response-policy errors, retaining optimizer-first ordering and unchanged diagnostics.
Native red/green coverage proves a pending GET stream previously masked the authority error.
The regression covers GET/HEAD/CONNECT/POST, invalid Brotli settings and zero timeout with no
body polls; the actual SDK test exercises both Send and Batch. See
[F3c's scoped checkpoint](2026-10-18-trusted-server-outbound-04-followups.md#existing-path-support-precedence-checkpoint-2026-10-25).

That ordering repair changes no public signature, client constructor, timer, transport, runtime
pin, cutoff rule or capability. The separately approved core helper change is recorded below;
neither slice completes shared preparation, P1/F3c/D4, or qualification.

### Spin Connect-Refusal Checkpoint (2026-10-25)

Complete the independent D4/EZ-4.8 refusal-handling subtask on the current live transport:
Connect `NotSupported`, `Immutable` and `Other` are advisory, without provider text or fabricated
timeout evidence. Keep the first-byte/between-bytes typed error policy, all configured timer values,
the absolute budget, outer race and exchange ownership unchanged. This adds no request option,
compatibility path, new transport or capability claim; configured connect mapping and full D4
remain unfinished.

The SDK refusal matrix failed red on Connect `Immutable`, then passes all five outcomes while
retaining exact nonconnect diagnostics. A separate test-utils probe executes under the existing
local Spin progress fixture's async service export: five outcomes by four budget sources by three
cases (ready success, pending timer wakeup, expired entry), 60 cases with captured-owner release
and no inherited refusal detail. Pending operations never self-wake; the SDK timer must make
progress even with the injected timestamp frozen. The synchronous SDK command runner cannot block
on an async import, confirmed with an isolated sleep control, so it is not credited with pending
timer evidence and no executor workaround remains in its tests.

`scripts/smoke_test_reusable_app.sh --adapter spin --suite outbound-progress --output /private/tmp/edgezero-spin-connect-fallback-20261025-r3`
passes under Spin 4.1.0 with SDK 7.0.0. The retained events identify this as working-tree
characterization based on `622f996a`, not frozen-candidate qualification. No worker panic occurred
before shutdown; the existing post-shutdown runtime panic remains recorded, not relabeled as
bounded host teardown. All 15 SDK-resource tests and 119 fixture-harness tests pass. All five
primary Rust gates, both eight-feature Spin matrices, strict WASM Clippy with and without
test-utils, the demo check, docs lint/build, docs/legacy guards and approval-identity verification
pass. Independent spec and quality reviews found no remaining scoped blocker. No acceptance leg
is promoted; the full atomic migration and Fastly/hosted gates remain open.

### Response Handoff Deadline Checkpoint (2026-10-25)

The current live Axum, Cloudflare and Spin converters checked the budget on entry, but could
return a successful bodyless response or lazy payload head after normalization without a terminal
sample. Fastly already checked those success branches. Direct converter regressions reproduced
late success; the new checks reject equality and later observations with the original selected
BudgetSource. This repairs the protected baseline rather than changing the approved public API.

All four converters now build the disposition-selected body and use one successful epilogue with
one final budget check and one response constructor. Remove their eight duplicated early-success
constructors, not keep another finalization path. Source/header/cap errors retain their existing
early error exits and diagnostic handling. Preserve each provider's native-body construction and
ownership points, raw read timers, bodyless/205 suppression, lazy payloads and independent caps.
Core header and payload policies remain shared; this is not full head-first finalizer integration.

Native Axum and Cloudflare Node/WASM tests first failed on late Ok, then passed. Matrices cover
HEAD/200, 204, 304, declared 205 and streamed payload heads, before/equal/after expiry and four
budget causes. Bodyless cases run both buffered and streamed modes. Axum proves zero source polls
and source release; Cloudflare tracks a high-water-mark-zero native stream for its status-200
cases, plus abort ownership and unchanged known-empty 205 disarming at late handoff. Its 204/304
controls use actual empty native responses, not tracked-source evidence. Preserve Fastly's existing
late-handoff control unchanged.

Spin's ordinary library test binary imports its KV service and cannot link under plain Wasmtime;
that attempt is not a passing test. Reuse the existing SDK-resources binary and existing test-only
clock seam to execute the private conversion matrix without another public API. The actual SDK
case fails before the Spin repair and passes afterward. It checks native consumer release through
peer write rejection while producer/completion handles are still held, not by dropping the fixture
first. The same private matrix remains colocated with conversion for a full Spin test host.

Two read-only investigations compared adding repeated checks or another public finalizer helper
with this single-exit repair. The selected path removes duplicate success handling without adding
an unapproved context constructor or bypassing the pending atomic migration. Demo/template APIs,
batch eligibility/arrival/cutoff rules, SDK/runtime pins, capability levels, frozen Interfaces and
requirements remain unchanged. Full P1/P4/P5/D1-D6 and resource/hosted qualification stay open;
no inventory leg becomes ready or qualified from this checkpoint alone.

Verification passes all five primary Rust gates, three WASM target lint scopes plus Spin's
non-test-utils scope, the inventory-bound 246-unit/17-contract Axum suite, 193 Fastly SDK unit
tests and 35 contracts, 48 Cloudflare Node/WASM tests, and 14 Spin SDK tests. The demo workspace
all-target/all-feature check, forwarding doctest, docs format/lint/build, docs/legacy guards and
approval-identity verification pass. Final spec and quality reviews found no remaining scoped
blocker after correcting the Spin test-only import. No new browser or hosted qualification is
claimed; the full Spin KV-hosted unit suite was not executed by the SDK-only runner.

### Shared Live-Validation Checkpoint (2026-10-25)

The requester's scoped approval hard-cuts the public helper to
`validate_for_dispatch(&request, OutboundSendMode)`. All four live Send/Batch paths now use the
same common validator; the four copied buffered-batch checks are removed. All old in-tree calls
are migrated directly, including the external compile contract and optimizer fixtures. There
is no one-argument wrapper. Target-specific unsupported checks still run first, including
Fastly's existing optimizer Batch diagnostic and Spin's optimizer/authority ordering.

Core regressions cover Send/Follow/Begin/Batch validation without body polling, method and upload
cap precedence, and unsupported optimizer modes before ordinary validation. Begin does not inherit
Batch's buffered-response restriction, but rejects streamed uploads as EZ-6.3 requires. The
corrected upload assertion failed before adding the Begin guard. The new external contract first failed to compile against
the old signature. The scoped core run passes 869 unit tests and four public API tests. Adapter
and target checks verify the actual live paths separately; these tests do not implement Begin,
Follow, paired invocation-time preparation, an eager scheduler or new arrival semantics.

The original frozen Interfaces and requirements, Fastly G2/backend-policy gates, runtime pins,
cutoff short circuit, existing transport behavior and capability levels remain unchanged. Demo
and templates do not call this adapter helper; their application request API needs no shim or
cosmetic migration. The full atomic client/driver migration remains P1/P4/P5 and D1-D6.

Local verification passes all five primary Rust gates, all three scoped WASM Clippy checks,
seven eight-feature adapter/target matrices, the native adapter contracts, 35 Fastly/Viceroy
contracts, 13 Spin WASI SDK tests and 47 Cloudflare Node/WASM unit tests. The excluded demo
workspace passes its all-target/all-feature check. Docs format/lint/build, the forwarding doctest,
docs/legacy guards and approval-identity verifier pass. Independent source-contract and code-quality
reviews found and corrected the Begin upload assertion, then found no remaining scoped blocker.
The Cloudflare browser-contract attempt failed in the local Safari WebDriver (HTTP 500/SIGKILL),
so it is not credited as a passing test or replaced by the Node results. Consumer suites and
hosted qualification were not run. No acceptance leg is newly ready or qualified.

### Consumer Contract Retention Audit (2026-10-25)

Reviewed external consumer PR #21 at exact source commit
`1626f72fca8d837dba697977eaa139dd4694ffd3`, using immutable Git objects rather than its working
tree. Its six workspace declarations and eight resolved SDK packages pin
`bc300ace5ca1ffe7060594a7bdb01c1da80de305`, not the older revision still described in its PR body.
This is a source-level integration/requirements audit, not execution of that application's
tests, proof of its resource arithmetic, or candidate qualification. Keep the Non-Negotiable
Baseline above every TS consolidation decision. Do not preserve old APIs for source compatibility
or drop a consumer contract to make the new pipeline simpler.

| Surface                                     | Current Contract And Hard-Cut Path                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Buffered wave dispatch                      | The consumer submits buffered requests through `HttpClient::start_batch_until`, observes indexed completions and distinguishes Completed/Cutoff/fault. Preserve positional identity, trailing-event validation, partial failures and cancellation. Eager implementation replaces the driver protocol, not these semantics.                                                                                                                                                                           |
| Independent limits and cache policy         | The consumer supplies absolute call deadlines, Bypass, upload/encoded/decoded/final/header/decoder/window/chunk caps. Keep each independent. Original coding for proxy use stays explicit; ordinary decoded responses and typed passthrough reasons remain available. Application wave sizing and protocol classification stay application policy.                                                                                                                                                   |
| Timing and clocks                           | Keep the application clock domain, one terminal per-slot elapsed sample and no later sibling-based reinterpretation. The paired-time cutover must migrate native and scripted clients together without reanchoring absolute call deadlines or inventing Native guarantees.                                                                                                                                                                                                                           |
| Header provenance                           | The consumer's source uses the retired public normalization helper in tests. Migrate those fixtures to `OutboundHeaderInput::normalize`, supplying actual header fidelity and policy. Preserve original Content-Type evidence through filtering and body lifting; never reconstruct eligibility from surviving text headers. No compatibility export or extra raw-header copy.                                                                                                                       |
| Implementer and collection APIs             | Scripted/test clients currently implement send/raw-request batches and construct indexed events. P1 migrates them to prepared contexts, raw terminal observations and one exchange owner. Replace mutable receipt/collection field access with the approved consuming accessors and explicit AlreadyDelivered/Resolved/Unresolved states; preserve errors and owned partial results. Do not retain old constructors, lazy defaults or raw-count overloads.                                           |
| Configuration and request/response lifetime | Keep typed declared-store failures, bounded extraction/structural limits, admission/fallback grants, detached Send/Abort, completion join/late binding, and absolute/relative write policy. Startup registry, credentials, catalog/schema residency and application admission sizes remain application-owned. Adapter cancellation/allocation evidence remains capability-scoped.                                                                                                                    |
| Axum resolved resource facts                | **Open implementation gap:** the runner enforces configured connection/parser limits but assembly still receives static unknown `AXUM_PLATFORM`. A scoped follow-up must derive metadata from the same validated runner configuration before assembly, accept only explicit operator ceiling/scope declarations with provenance, and test independent ceiling/population changes, invalid startup settings and unchanged unknown facts. Do not publish an application arithmetic cap as an OS quota. |
| Hosted ingress resource facts               | **Open enforcement/evidence gaps:** Cloudflare has no justified per-instance inbound-population bound; all-target host-ingress accounting remains unknown. A guest gate must precede the allocation it claims to bound, and cannot certify earlier provider-created storage. Keep Unknown/Indeterminate until enforced bounds or provider evidence exist; outbound connection limits are not inbound limits.                                                                                         |
| Deployment and qualification                | Hosted versus generic Spin selection and application stack requirements need deployment/application evidence. Raw ingress, egress, deadlines and total provider allocations retain their actual capability levels. No full resource certification, hosted approval or consumer repin is implied by green EdgeZero unit tests.                                                                                                                                                                        |

Required migration controls, not claimed executions:

- A buffered successful slot must already own a complete, finalized body under every effective
  cap. The consumer's subsequent collection uses an unbounded numeric sentinel in reliance on
  that contract; head-first dispatch must not substitute an unfinished stream for buffered success.
- Preserve decoded-by-default bidder responses. Original coding is the explicit proxy policy,
  not a setting used by the audited buffered client. Compressed 206 is a separate migration case:
  current EdgeZero preserves partial-representation coding, whereas the pinned consumer's cap-row
  classifier assumes every supported coding is already decoded. Require status/coding-aware
  application handling and a regression; do not undo 206 preservation or silently decode it upstream.
- Bind captured extraction funding before the read, retained configuration through rejection and
  terminal egress, completed response storage outliving stream leases, and boot/strict resource
  parity to the existing consumer test targets `config_funding`, `auction_config_retention`,
  `response_retention` and `boot_strict_parity`. Rerun them with the selected pin and migrated
  fixtures; their source presence is not a passing execution record.
- Preserve upstream terminal timing without a later sibling-based recheck, while retaining the
  consumer's separate deadline/elapsed accounting for its own settlement work. Driver failures
  retain valid upstream partials; application policy may still reject the entire operation.
- The older TS companion draft's retained application transport, universal ingress buffering and
  custom lifecycle exceptions are superseded by the approved requirements here. They are not a
  fallback plan or permission to retain the plumbing this migration removes.

The Axum resolved-fact and hosted-resource follow-ups are recorded requirements/gaps, not added
runtime implementation, approved policy changes or newly ready legs in the existing 511-leg
inventory. They need their own scoped design/acceptance before implementation. Keep their eventual
consumer repin atomic with source and fixture migrations; never solve them with a second transport,
unknown-as-zero accounting, or removal of the existing buffered fanout behavior.

### P2 Cloudflare Native-Body-Absence Checkpoint (2026-10-25)

Continue P2's existing known-empty ingress subtask without changing the frozen Interfaces,
requirements, current public client protocol, SDK/runtime pins or provider capability claims.
Both direct conversion and admitted dispatch preserve native Fetch `body == null` as
`Body::empty()`. Remove the unsupported ArrayBuffer fallback and unused attachment helper.
Every present source retains the existing lazy reader/deadline/error path, including empty
ArrayBuffer/Uint8Array bodies. Generic source-test seams still represent present streams.

The two source investigations confirmed that worker 0.8.5 keeps no alternative payload buffer:
its native-null HTTP conversion already selects empty. A prior SDK read can change private
bookkeeping without creating a source; it cannot justify reading a null body. Consumed present
bodies keep their construction error. The smallest implementation preserves absence internally
with `Option`, without another transport, public API, header inference or eager collection.

Native Node/WASM regressions first demonstrated streamed null bodies on direct and admitted
paths. They cover GET/HEAD/POST absence, present empty/nonempty buffers, expired first reads,
sticky poisoning, unchanged Initial/HostManaged state and refusal/abort before source inspection.
The test delivery seam inspects ingress envelopes; it is not a hosted egress proof. The existing
workerd empty probe separately records the original native snapshot and pre-collection body
storage through production dispatch. Its fixture/validator regressions reject contradictory,
missing or mistyped source evidence. General settlement, support-first paired timing, full
P1-P5/D1-D6 migration and Q3-Q5 qualification remain open. No acceptance leg is promoted here.

Local Node/WASM verification passes 47 adapter tests; the fixture passes 21 core and 119 harness
tests. Actual local workerd 1.20260910.1 with Wrangler 4.131.0 passes all five source-presence cases
and the existing coding, marker, relay and progress/framing groups. The 223-event log is
`/private/tmp/edgezero-cf-null-body-20261025-c/events.jsonl`, SHA-256
`5b035c59b03f83c696f378629da8fe2a52c0bee20221c2395becd16142ec46cd`, recorded as working-tree
characterization against base `a0d31519`, not frozen-candidate evidence. The preceding unavailable-
CLI and asdf-shim failures remain in the `20261025` and `20261025-b` run directories. Version-matched
isolated tools and an absolute installed Wrangler path repaired tooling without changing pins.
All five root Rust gates, target/fixture strict clippy, docs formatting/lint/build, forwarding
doctest, legacy/docs guards and approval-identity verification pass. Independent specification
and implementation reviews found no remaining scoped issue.

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

### Shared Header-Policy Consolidation Checkpoint

The requester approved consolidation around one EdgeZero implementation and a documented
hard-cut consumer migration, then explicitly required preservation of existing consumer
contracts. Two read-only solution reviews selected a permanent shared header operation rather
than a second transport, a public response-context constructor or premature dispatch replacement.
This checkpoint does not change the frozen Interfaces or claim a new exact-interface approval.

- [x] Extract `OutboundHeaderInput::normalize` from `OutboundHead::new` and use it immediately
      in all four adapter response paths. Preserve pre-filter field accounting, immutable
      Content-Type/Location evidence, unknown provider fidelity, marker accounting and original
      body disposition. Retain static marker bytes without allocating or copying raw headers.
- [x] Privatize the separate normalization/marker helpers and remove their root exports;
      retain private defensive response normalization without a second upstream charge or marker.
- [x] Establish missing-operation compile red, then cover exact count/byte boundaries, filtered
      duplicates, unknown fidelity, original positive-205 disposition and marker-off retention.
      Add public usage and retired-helper compile-fail doctests.
- [x] Align the capability guide, outbound adoption path and changelog. Handler/demo/template
      APIs remain unchanged; their atomic implementer migration remains with P1.
- [x] Complete independent spec/quality reviews, all five root Rust gates, three WASM lint
      legs, Fastly and Spin contract execution, core doctests and documentation checks.
- [x] Complete the fresh generated-project native/three-WASM build, 24 generated core cases
      and actual TCP fallback gate. An initial sandboxed run denied loopback; the unchanged
      loopback-enabled rerun passes.
- [ ] Complete the genuine prepared-context, paired-time, eager scheduling, arrival-ledger and
      Follow migration; remove superseded implementer APIs atomically, not through compatibility
      adapters or lazy scheduling defaults. Existing positional/partial results, cutoff/cancellation,
      timing, typed failures, all caps, encoding/provenance and ingress/egress ownership remain required.

Payload decoding/collection, SDK resources and abort/EOF acknowledgement, deadline races,
native Image Optimizer, backend identity/timers/slack and all capability levels are unchanged.
Fastly readiness/error-preservation and B/backend-policy constraints still gate their affected
engine work; no numerical policy, provider guarantee or hosted qualification is inferred.
This is a header-only consolidation, not completion of P1/P2 or the full approved scope.

The scoped workspace run passes 2,244 tests across 29 binaries (one existing ignored), including
866 core cases. Fastly passes 193 WASI unit cases and 35 contracts; Spin passes all 35 contracts
on pinned Wasmtime 48.0.2 after correcting a baseline-proven stale exact-header expectation:
explicit buffered framing adds Content-Length. The corrected fixture still asserts the exact
header set, body, status and lifecycle counts. All 57 demo tests and the approval-identity verifier
pass. The shared-header documentation guard includes nine positive/negative self-checks.

Cloudflare target compilation/lint passes. Local browser execution is unverified: the installed
0.2.121 runner first mismatched the locked 0.2.128 schema; an isolated exact-version runner then
encountered OS-killed Safari/Firefox WebDrivers. No repository dependency or installed-tool pin
was changed, no browser result is called passing, and no hosted qualification is inferred.

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

The initial foundation did not change transport behavior or qualify a runtime. The approved
paired-time integration below now replaces clock-only constructors, while the dispatch-trait
hard cut remains open. G1 applicability, remaining
G3 premises, Fastly B/H(K), and hosted results retain their own open gates.

### P1 Paired-Time Integration Checkpoint

The requester explicitly approved moving this integration ahead of the atomic dispatch cut on
2026-10-26; see the scoped approval audit. The frozen Interfaces and Plan 02 are unchanged.

- [x] Hard-migrate all four live clients, request injectors and affected fixtures to `with_time`
      / `try_with_time`, pairing standard entrypoints with the exact application clock. Remove
      the clock-only constructors without aliases; leave the public dispatch trait unchanged.
- [x] Use the supplied timer for upload/head/body deadline races, check it before source I/O,
      retain pending registrations, preserve typed failures and recheck original deadlines after
      ready I/O. Native reqwest total timeouts no longer introduce a second clock domain.
- [x] Register Fastly timers in the existing progress multiplexer, including timer-only waits,
      without per-slot sleep/select. Check Workers scheduling and cancellation before fetch;
      retain a real host-event yield for frozen-clock fairness.
- [x] Complete cross-target regression checks, deterministic wake/cancellation proofs, strict
      lint and independent ownership review; record the result in the approval audit.

This is live timer integration, not completion of P1. Support-first settlement/context issuance,
the atomic trait and batch-receipt migration, external-client conformance, redirect following and
hosted qualification retain their separate gates. Demo and generator use standard injection,
not removed custom constructors; no alternate client API is added for them.

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
private contexts; external implementations cannot manufacture them. The existing facade/trait
has not changed. Adapter constructors migrate in the separately approved paired-time checkpoint;
demo, generator and fixtures keep their current valid
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

This is the current implementation/next-gap view at the dashboard's snapshot, not a list of
qualified requirements. **All rows retain open acceptance/qualification work.** Local checkpoint
links identify the implemented scope and its test/evidence record; they do not close another
mode, adapter, conditional part or hosted leg. The inventory remains the criterion-level coverage
map; completing its executable bindings is an explicit open task, not hidden by this table.

| Requirement | Implementation Tasks | Production Applicability                                                  | Current Implementation / Checkpoint                                                                                                                                                                                                                                               | Next Unclosed Work                                                                                                                                         |
| ----------- | -------------------- | ------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| EZ-1 | P2; D1-D4 | Blocking, including actual ingress forwarding | **Shared settlement implemented in the worktree.** Certified-empty conversion remains zero-read; unknown GET/HEAD sources settle before dispatch with paired 408/504 deadlines, preserved source errors and hidden deadline provenance across parts. Fastly's approved slack retirement removes the later false rejection. | Complete all-target/runtime acceptance bindings and hosted forwarding evidence. Fastly synchronous inbound reads remain BestEffort. |
| EZ-2        | G2/G4; D1/D5; F1     | Blocking ranges exactly as spec; reuse-only ranges deferred               | **Partial implementation:** [B31 existing-path identity/timers](#existing-path-b31-implementation-checkpoint-2026-10-25); [current-operation multiplexer](#current-operation-multiplexer-checkpoint-2026-10-25). Historical mixed/full-buffer readiness failures remain retained. | Callback-wide ownership, remaining modes and registration observations. F1 retirement, hosted pooling and resource qualification remain separate.          |
| EZ-3 | D1/D5 | Blocking; bounded waits and original budgets retained | **Approved retirement locally verified.** Remove the 25 ms rejection, exported constant and injection hook. A 26 ms settlement no longer rejects a live budget; dispatch/registration still reject actual expiry and backwards clocks. | Bind bounded-wait acceptance and run the M7 comparison. Preserve original deadlines and B31; hosted/runtime promotion remains separate. |
| EZ-4 | P1; D1-D4 | Blocking; Native/BestEffort distinctions retained | **Configured mapping implemented locally.** Connect setting/readback/round-trip, zero rejection, Fastly stable backend mapping, Spin advisory setter/refusal and configured-vs-unconfigured timeout causes are covered. Axum preserves its shared pool and accepts BestEffort aggregate enforcement; Cloudflare does likewise. Follow retains those settings. | Bind the complete mode/adapter matrices, update capability rows only with evidence, and obtain controlled hosted connect-expiry proof. |
| EZ-5 | P3; D1-D4 | Blocking; Spin conditional host result recorded | **Integrated through the shared head finalizer.** Send/Begin/Batch and final Follow heads retain original-coding policy, body disposition, provenance and independent limits; followed heads are discarded unread. | Complete remaining all-target normalization/limit/runtime acceptance bindings and separately adopted metadata legs. |
| EZ-6 | P4; D1-D6 | Blocking except EZ-6.31 reuse-only | **Shared scope integration locally verified.** Eager drivers feed the arrival ledger, observation-only checks and lossless mixed collection. Core captures unpolled operations including Follow, revokes producer roots without dropping recorded results, and accepts exact provisional body owners before egress policy/framing. Provider family leases preserve early body progress without retaining callback roots. | Bind all applicable mode/lifetime runtime acceptance and qualify providers; local ownership proofs do not close parent qualification. |
| EZ-7 | P3; D1-D4 | Blocking | **Integrated through shared preparation and head finalization.** The facade resolves marker policy before dispatch; original fields and the optional marker are charged once across Send/Begin/Batch/Follow. | Complete remaining all-target marker/normalization acceptance bindings. |
| EZ-8 | P1; D1d/D5 | Blocking; Fastly single-request Send/Follow only | **Send/Follow use the shared pipeline.** Direct native optimizer mapping is preserved without another fetch, buffering or serialization; unsupported modes/adapters reject before settlement. Actual-client Send/initial-Follow/later-hop controls reach Viceroy's native unavailable boundary. | Obtain successful optimized-first-hop redirect and hosted transformation/performance evidence. Blocking horizon remains UNKNOWN. |
| EZ-9 | P1/P5; D1-D5 | Blocking; supplied clients use the same dispatch boundary | **Explicit Follow implemented.** Core owns the decision table, per-hop policy, immutable initial chain deadline, replay and unread disposal. Supplied-client, real-TCP Axum, actual local Spin/Viceroy and Cloudflare guest-boundary controls pass. | Bind full criterion-level acceptance; complete workerd/origin, successful optimizer redirect and consumer/hosted evidence. No compatibility transport. |
| EZ-10       | F2                   | Follow-up unless approved as promoted/present                             | **Local implementation with a known failure:** [typed overflow/uploads/rendering](#f2-overflow-checkpoint); generated/demo TCP proof exists.                                                                                                                                      | Cloudflare front-proxy recovery failure, required hosted HTTP/2 and consumer end-to-end acceptance. Passing sibling cases do not waive the failure.        |
| EZ-11       | F3a                  | Follow-up                                                                 | **Not implemented:** cache tags.                                                                                                                                                                                                                                                  | Tag validation/deduplication, provider mapping, mode/support matrix and actual cache probes.                                                               |
| EZ-12       | F3b                  | Follow-up                                                                 | **Not implemented:** TLS name-check policy.                                                                                                                                                                                                                                       | Typed setting/readback, backend-key isolation and trusted-chain/wrong-name fixture; no global verification bypass.                                         |
| EZ-13 | F3c | Follow-up; closes with Unsupported evidence | **Unsupported retained with the required synchronous support hook integrated.** Spin authority and unsupported optimizer checks precede ordinary validation, timers and body polling. | Bind the complete activity/SDK limitation and consumer configuration-rejection evidence; do not promote support. |
| EZ-14       | F4                   | Follow-up; includes acceptance 10/11                                      | **Not implemented:** opted-in idle-body policy.                                                                                                                                                                                                                                   | New mode, encoded-progress resets, absolute-deadline precedence and actual adapter read races; existing total deadlines are not idle-mode support.         |
| EZ-15       | F5                   | Follow-up; reuse legs separately conditional                              | **Not implemented:** explicit bounded fire-and-forget operation.                                                                                                                                                                                                                  | Separate capacity/ownership, 64 KiB response ceiling, cancellation/recovery and provider-lifetime proof.                                                   |
| EZ-16       | F6a                  | Follow-up                                                                 | **Diagnostic checkpoints implemented:** [SDK/source details and capability publication](#f6a-capability-publication-checkpoint).                                                                                                                                                  | Full token-safe acceptance matrix, remaining integration cases and consumer/provider qualification. Capability publication itself has landed.              |
| EZ-17       | F6b                  | Follow-up                                                                 | **Local implementation:** [owned delivery reports/safe replacement](#f6b-delivery-report-checkpoint); subsequent F9a assembly integration has landed.                                                                                                                             | Consumer canonical-lifecycle acceptance and full required qualification, not another assembly integration implementation.                                  |
| EZ-18       | F7a                  | Follow-up                                                                 | **Local implementation:** [buffered framing](#f7a-buffered-framing-checkpoint); 80 production cases passed, nine SDK reference controls failed.                                                                                                                                   | Resolve/qualify parity and hosted compression while preserving strict suppression. The complete local suite remains nonzero.                               |
| EZ-19       | F7b                  | Follow-up                                                                 | **Local implementation:** [bounded synthesized headers](#f7b-synthesized-header-checkpoint), wire and assembly-exclusion checkpoints.                                                                                                                                             | Consumer-owned header integration and hosted/frozen-candidate qualification; local assembly exclusion is not still missing.                                |
| EZ-20       | F8                   | Follow-up                                                                 | **Local implementation:** [declared KV bindings and operation delivery](#f8-declared-kv-checkpoint) on all three adopted serverless adapters.                                                                                                                                     | Actual provider outage/restoration, consumer acceptance and qualification; the separate Spin retained-overlap uncertainty remains disclosed.               |
| EZ-21       | F6c                  | Follow-up                                                                 | **Local implementation:** [six typed framing reasons](#f6c-framing-checkpoint), observer/log controls.                                                                                                                                                                            | Consumer Viceroy acceptance and separately required hosted evidence across applicable failure paths.                                                       |
| EZ-22       | F9a                  | Follow-up; individual SHOULD decisions explicit                           | **Local implementation:** [unified fallible assembly/failure delivery](#f9a-assembly-foundation-checkpoint), including adopted macro/serverless parts.                                                                                                                            | Consumer acceptance and exact-candidate qualification; no compatibility assembly path to add.                                                              |
| EZ-23       | F9b                  | Follow-up                                                                 | **Local implementation:** [assembled-App Axum/TCP helpers](#f9b-assembled-app-tcp-checkpoint), twelve upstream sentinels.                                                                                                                                                         | Criterion 12's conditional begun-request portion and criterion 13's consumer route/parity evidence.                                                        |
| EZ-24       | P6; F10a/F10b/F10c   | Host/WASI publication and each optional extension need separate decisions | **Not implemented as the proposed published utility; adoption pending.** Existing private doubles are not P6.                                                                                                                                                                     | Attributed applicability decisions, P1-P5 semantics, then selected published-client/cancellation/demo/JS work. Pending is not approved deferral.           |
| EZ-25 | Q1/Q2 | Documentation updates accompany each affected behavior | **Hard-cut caller migration locally verified.** Supplied clients, demo, generator and fixtures use prepared contexts, raw events, explicit states, shared progress and the Body payload/owner split. Guides, changelog and guards track the same implementation, including explicit Follow. | Complete applicable acceptance bindings, then qualify the frozen candidate. |
| EZ-Q1       | G1; Q3-Q5            | Required blocking, approved promoted, and confirmed-present parts only    | **Preparation only:** inventory/checker, [evidence assembly](#q3-evidence-preparation-checkpoint), [runbook source](#q2-runbook-source-preparation-checkpoint).                                                                                                                   | Executable leg bindings and scope decisions, remaining sources, candidate freeze, accepted Q4a package, authorized Q4b runs and durable passing Q5 record. |

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
      implementation, and the requester separately approved the existing-path B31 subset above.
      The Fastly readiness primitive and full engine/resource policy remain separately gated.
      Numerical H(K) and its hosted evidence still gate qualification, not the permitted native
      Send-first integration. Neither approval is inferred from timer rounding.
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

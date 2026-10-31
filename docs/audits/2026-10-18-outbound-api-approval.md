# Outbound API Review and Approval Record

## Decision Status

Prepared on 2026-10-19 for [task G4](https://github.com/IABTechLab/edgezero/blob/7cccaf10734914161803317aec6c55c22c2bccc9/docs/superpowers/plans/2026-10-18-trusted-server-outbound-01-gates.md#task-g4-maintainer-api-and-feasibility-approval).
**API-only approval: AG, 2026-10-19.** The requester replied `continue` to the conversation
question naming the exact Interfaces digest below, then supplied `AG` as the approving maintainer.
That scoped response authorizes API implementation, not Fastly policy, capability promotion,
requirement changes, dependency repinning or production adoption. The other decisions remain
pending. Preparing this record or passing tests does not supply their approval.

The [2026-10-24 region-enum amendment](#scoped-region-enum-amendment) records the requester's
later, narrower approval separately. The [scoped timing and native integration amendment](#scoped-timing-and-native-integration-amendment)
records the subsequent implementation authorization. Neither supplies a newly approved
whole-contract digest or changes the frozen Interfaces signatures.

The inspected checkout is PR 275 at `7cccaf10734914161803317aec6c55c22c2bccc9` plus the uncommitted
G1-G3 work and Spin response-egress repair. Its local characterization does not qualify that commit.
The [implementation index](https://github.com/IABTechLab/edgezero/blob/7cccaf10734914161803317aec6c55c22c2bccc9/docs/superpowers/plans/2026-10-18-trusted-server-outbound-implementation.md)
continues to require a buildable hard migration preserving the implemented outbound baseline.
There is no compatibility branch, alternate transport or lazy default proposed here.

| Decision ID          | Decision                                 | Approver                                   | Decision Date | Approved Subject Digest                                             |
| -------------------- | ---------------------------------------- | ------------------------------------------ | ------------- | ------------------------------------------------------------------- |
| G4-API               | Approved, API only                       | AG; scoped exact-digest conversation reply | 2026-10-19    | `c623e7fc5b189e2d9122756f29297a92031a9936b13c6cc475484d4e028a4a7a`  |
| G4-SCOPE-CAPTURE     | Authorized scoped architectural redesign | Requester; explicit conversation reply     | 2026-10-27    | Structured ownership design below; not the earlier closure proposal |
| G4-FASTLY-SLACK      | Approved, scoped retirement              | Requester; explicit conversation reply     | 2026-10-27    | Remove slack rejection; retain original deadlines, B31 and limits   |
| G4-FASTLY-POLICY     | Pending; local primitive failed          | Unset                                      | Unset         | Unset                                                               |
| G4-FASTLY-B31        | Approved, scoped implementation only     | Requester; scoped conversation reply       | 2026-10-25    | Proposal referenced below; not new whole-contract digest approval   |
| G4-OPTIMIZER-HORIZON | Pending; finite rule unproved            | Unset                                      | Unset         | Unset                                                               |
| G4-APPLICABILITY     | Pending; G1 scope lock open              | Unset                                      | Unset         | Unset                                                               |
| G4-MEASUREMENTS      | Pending; replay definitions unconfirmed  | Unset                                      | Unset         | Unset                                                               |

The [2026-10-25 policy/readiness packet](2026-10-25-fastly-backend-policy-proposal.md) now proposes
B31 and a B1 comparison with complete aggregate-budget arithmetic. On 2026-10-25 the requester
selected `B = 31 bounded ladder (Recommended)` as the proposal to finalize for review. The question
explicitly excluded production integration, runtime changes and hosted qualification.
That selection alone did not authorize implementation. The subsequent scoped approval below
authorizes B31 production policy changes; the full G4-FASTLY-POLICY resource/primitive decision
remains pending.
Review resource exposure, emitted settings, anchor rules, provider maxima and the repaired-versus-
stock primitive disposition separately. Preparing or selecting a proposal does not authorize
runtime promotion or hosted execution beyond the separately approved scope.

The scoped API response and its exact digest are attributed to the maintainer name supplied in
the conversation, not inferred from Git. This does not close G4 as a whole. A completed
decision must retain its maintainer, date, exact subject digest and scope. Proposal hashes do not
approve the other subjects; missing, rejected or stale approval blocks their dependent work.

## Scoped B31 Implementation Approval

On 2026-10-25 the requester answered `Approve scoped B31 implementation` to this question:

> May I implement the B31 aggregate-budget backend policy from the review packet in PR #275,
> including stable backend identity and ceiling-rounded timers, while preserving original logical
> deadlines? This would authorize those production policy changes only; runtime-pin changes, the
> readiness engine, and hosted qualification would remain separate.

The referenced [proposal](2026-10-25-fastly-backend-policy-proposal.md), SHA-256
`011b090e21c7e8e6424b53303c55b1b2eacb7c896e51a81b6492c13fa65ce3e1`, remains the historical
submission. This is an attributed scoped authorization, not a new exact-digest API approval or
approval of every resource/primitive decision listed in that proposal. It supersedes the D1a/G2
sequencing dependency only for the existing aggregate-policy Send/Batch backend identity and timer
implementation. Keep current dispatch guards, pending selection, body reads and error handling;
do not remove slack or change runtime pins/readiness as part of this authorization.

Configured per-call timeouts select a stable, seven-day-clamped anchor even when a caller deadline
or observation cutoff is earlier. Deadline-only/default work uses the originally selected duration,
not the later remainder. Ceiling-millisecond rounding then selects one of 31 coupled provider
timer vectors. Names are versioned, configuration-derived and client-independent. A shared verified
registry must compare full identities, serialize registration, retain successful late registrations
before reporting the original timeout, and reject unknown `NameInUse` rather than trusting it.

At that checkpoint the transport still captured its budget at async method entry, as documented
by the Send-first checkpoint; that approval did not claim facade-invocation capture or the atomic
trait migration. The subsequent scope amendment and implementation dashboard track that migration.
Explicit Connect/idle options, retirement/total resource bounds, host enforcement, pooling,
optimizer horizon and hosted qualification remain separate work. No public signature, application
policy, portable cap, timeout cause or logical deadline changes under this scope.

## Original Exact API Subject

### Reopened Invocation-Scope Interface (2026-10-27)

The atomic implementation review found that the approved response context requires an
invocation-time local progress-scope token, but the frozen implementer surface provides no way
to capture/install it across crate boundaries. Capturing at dispatch, timer polling or collection
is too late for a retained, never-polled Send future; binding it at client construction would
incorrectly retain a previous request's scope. This is an implementation-interface gap, not a
provider impossibility or permission to weaken request-lifetime cleanup.

The initial narrow proposal added a required `capture_scope` hook returning an opaque local
`OutboundScope`, read-only scope access on prepared contexts, and provider-facing construction,
activation and cancellation-registration operations. Activation restores the previous scope on
drop and cannot create, close or reparent a captured lifetime. Check/register alone is insufficient:
Begin or Batch work later polled inside another response must retain its original provider
runtime/progress identity. Pending/group registrations survive completion of the outer Begin
future; successful streamed handoff changes cleanup ownership without resetting deadlines.

The requester answered that EdgeZero owns this interface, requested an ideal architecture with
"No hacks!", and explicitly replied `ok to reopen the interface!`. The earlier closure-based
proposal is not a constraint to preserve. Review shared typed scope ownership against a
provider-owned protocol, record the chosen contract, and hard-migrate its implementation and
callers. No compatibility default, clock/timer side channel or retained obsolete path is authorized.
Reopening is explicit, not inferred from the current implementation or passing tests.
Canonical callback/body owners, unpolled-root revocation and retained-handle lifetime coverage
remain open in the current dashboard in
`docs/superpowers/plans/2026-10-18-trusted-server-outbound-implementation.md` (2026-10-27).
The original Interfaces and requirements digests below remain historical, not restrictions on
this authorized redesign. Record new contract bytes and scope separately rather than silently
refreshing the original approval subject. Runtime promotion and hosted qualification remain separate.

The selected architecture uses one core `OutboundScopeOwner`/`OutboundScope` implementation and
private revocable operation roots. The required client `progress` hook supplies provider polling,
not cancellation registration or a second scope registry. Independent adapters share the core
progress bridge; Fastly retains only cooperative SDK scheduling and the existing multiplexer.
There is no `capture_scope` callback interface, compatibility default or alternate transport.

For early body work, `response_body()` creates a provisional owner, `with`/`run` starts work in
that owner, and consuming `attach(response)` carries it in local body metadata. Attachment alone
cannot escape callback cleanup. Canonical egress accepts the exact owner before policy, framing
or suppression; successful outbound streamed collection likewise accepts its body lifetime before
handoff. Provider progress is shared through a request-family cache while callback and body roots
remain independently revocable. EOF is not terminal egress. Failed application body work closes
before bounded fallback delivery. Buffered representation and payload bytes are unchanged.

`Body` becomes a private carrier with a separate `BodyPayload` transport representation; all four
adapters, supplied clients, demo and templates hard-migrate together. Local ownership does not
enter HTTP extensions, shared clients or App state. Original deadlines, timeout causes, partial
batch/receipt semantics, coding/provenance, independent limits and native Image Optimizer behavior
remain mandatory. This is the requester's scoped architecture authorization, not a claim of a new
exact-digest approval or hosted evidence.

The same core revocation protocol backs the fallible `OutboundTask` runner/owner pair consumed
by Axum, Cloudflare and Spin. Executor drop cannot retain producer futures through another public
handle, and failed registration returns before provider spawning. One Body source-binding path
serves outbound and downstream egress; one core predicate handles earlier-own-deadline cutoff
classification. Nonblocking completion observation does not drive SDK progress. These are
consolidations of the selected architecture, not independent compatibility implementations.

| Scope Amendment Identity (2026-10-27)    | Value                                                              |
| ---------------------------------------- | ------------------------------------------------------------------ |
| Interfaces SHA-256                       | `b69a75ba96b69c0979c05b8711f9cacbaad0abef6a6caa053ed77d21916bbba4` |
| Complete plan SHA-256                    | `10776c6db1bfacaecca91a32c9ad955ad13f696ef8d078312e5b566b43f6205b` |
| Interfaces size                          | 856 LF-terminated lines, 53,795 UTF-8 bytes                        |
| Unchanged normative requirements SHA-256 | `a3d58bcabe7e9e556d21227de973c660794d25606b7ee3365bd7808d3d101c70` |

These identify the implemented scope proposal; they do not overwrite the original AG approval
subject or convert scoped conversation authorization into a new exact-digest review.

### Approved Dispatch-Slack Retirement (2026-10-27)

Code inspection found that Fastly's retained dispatch-slack guard measures from the invocation
snapshot, including bounded GET/HEAD settlement. A body that settles after 25 ms can therefore
encounter a dispatch Internal error even while its original logical deadline remains live.
Resetting the budget or silently changing the guard's anchor would not satisfy the approved
contract. The requester explicitly answered `Approve dispatch-slack retirement` to the scoped
question authorizing removal while preserving original absolute deadlines, B31, typed timeout
causes and resource limits.

The implementation removes the elapsed-time rejection, its public constant and test-injection
hook instead of retaining aliases. Absolute-expiry and backwards-clock checks remain at dispatch
and registration boundaries. Regression and target verification are required before completing
this checkpoint. This approval neither authorizes a runtime-pin change nor supplies hosted
qualification, a hard interruption guarantee or a capability promotion.

### Frozen Original Subject

Review the complete [Interfaces: Proposed Hard-Cut Contract](https://github.com/IABTechLab/edgezero/blob/7cccaf10734914161803317aec6c55c22c2bccc9/docs/superpowers/plans/2026-10-18-trusted-server-outbound-02-policies.md#interfaces-proposed-hard-cut-contract),
not just its opening trait declaration or the summaries in this packet. The subject includes every
signature, supporting type, ownership/default rule, migration table and acceptance sentinel in
that section, ending immediately before `## Task P1:`. It is referenced once rather than copied
into a second potentially conflicting contract.
These source links use the immutable submission revision because internal plans are excluded from
the published documentation site. The identities below are historical, not the amended local file.

| Property                       | Submitted Value                                                            |
| ------------------------------ | -------------------------------------------------------------------------- |
| Source                         | `docs/superpowers/plans/2026-10-18-trusted-server-outbound-02-policies.md` |
| Section                        | Lines 18-696 inclusive, 679 LF-terminated lines, 42,166 UTF-8 bytes        |
| Section SHA-256                | `c623e7fc5b189e2d9122756f29297a92031a9936b13c6cc475484d4e028a4a7a`         |
| Complete source-file SHA-256   | `c52b4aea6b8d2793f30784e88016a3676088d344c0a5607774a5d9f1a7ffae80`         |
| Normative requirements SHA-256 | `5d9833ebb13bf191377eeec6f9ada89d236044546701d01478c7f12f65b175ec`         |

Run from the repository/worktree root to verify the original subject and historical region amendment. The extraction includes
the trailing blank lines before the next task; it does not normalize whitespace or line endings.
Any changed Interfaces byte needs a new digest and review. Changes elsewhere in the plan or in
the requirements also require reviewing their effect, not silently refreshing these identities.

```sh
node --input-type=module <<'JS'
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

const path = 'docs/superpowers/plans/2026-10-18-trusted-server-outbound-02-policies.md';
const revision = '7cccaf10734914161803317aec6c55c22c2bccc9';
const source = execFileSync('git', ['show', `${revision}:${path}`], { encoding: 'utf8' });
const start = source.indexOf('## Interfaces: Proposed Hard-Cut Contract\n');
assert.notEqual(start, -1);
const end = source.indexOf('## Task P1:', start);
assert.ok(end > start);
const section = source.slice(start, end);
const digest = value => createHash('sha256').update(value).digest('hex');
assert.equal(Buffer.byteLength(section), 42166);
assert.equal(section.split('\n').length - 1, 679);
assert.equal(digest(section), 'c623e7fc5b189e2d9122756f29297a92031a9936b13c6cc475484d4e028a4a7a');
assert.equal(digest(source), 'c52b4aea6b8d2793f30784e88016a3676088d344c0a5607774a5d9f1a7ffae80');
const requirementsPath = 'docs/superpowers/specs/2026-10-17-edgezero-pr275-requirements.md';
const requirements = execFileSync('git', ['show', `${revision}:${requirementsPath}`]);
assert.equal(digest(requirements), '5d9833ebb13bf191377eeec6f9ada89d236044546701d01478c7f12f65b175ec');
const regional = execFileSync('git', ['show', `f4d660b1054a195ec923d78c1375358dc7b8b9fe:${path}`], { encoding: 'utf8' });
const regionalStart = regional.indexOf('## Interfaces: Proposed Hard-Cut Contract\n');
assert.notEqual(regionalStart, -1);
const regionalEnd = regional.indexOf('## Task P1:', regionalStart);
assert.ok(regionalEnd > regionalStart);
assert.equal(digest(regional.slice(regionalStart, regionalEnd)), '0b1b2e132dc3e294e117a1a5d2bdb65c49693eab47b93e7926220612fd595404');
assert.equal(digest(regional), '39c2ae866be8775b6b34879cbabff92a97f1c9f7010d76f275027690c0b6dd51');
const current = readFileSync(path, 'utf8');
const currentStart = current.indexOf('## Interfaces: Proposed Hard-Cut Contract\n');
const currentEnd = current.indexOf('## Task P1:', currentStart);
assert.ok(currentStart >= 0 && currentEnd > currentStart);
assert.equal(digest(current.slice(currentStart, currentEnd)), 'b69a75ba96b69c0979c05b8711f9cacbaad0abef6a6caa053ed77d21916bbba4');
assert.equal(digest(current), '10776c6db1bfacaecca91a32c9ad955ad13f696ef8d078312e5b566b43f6205b');
assert.equal(digest(readFileSync(requirementsPath)), 'a3d58bcabe7e9e556d21227de973c660794d25606b7ee3365bd7808d3d101c70');
console.log('Historical and scope-amendment identities match; authorization remains separately scoped.');
JS
```

### Scoped Region Enum Amendment

On 2026-10-24 the requester explicitly asked for an enum and replied `ok` to the proposal for a
public, core-owned seven-variant region enum, typed `parse`/`as_str` conversions, corresponding
spec/tests and no compatibility wrapper. This is approval of that described API change, not a
claim that the requester reviewed the new whole-section digest computed afterward.

The regional [Interfaces contract](https://github.com/IABTechLab/edgezero/blob/f4d660b1054a195ec923d78c1375358dc7b8b9fe/docs/superpowers/plans/2026-10-18-trusted-server-outbound-02-policies.md#interfaces-proposed-hard-cut-contract)
differs from the original only by declaring `OutboundImageOptimizerRegion::{Asia, Australia,
EuCentral, EuWest, UsCentral, UsEast, UsWest}` and specifying its Copy/Clone/Debug/PartialEq/Eq,
direct construction and exhaustive matching semantics. Options alone retain private fields;
existing getters and canonical token/error behavior are unchanged. No other interface or gate
is amended.

| Historical Regional Identity | Value                                                              |
| ---------------------------- | ------------------------------------------------------------------ |
| Interfaces SHA-256           | `0b1b2e132dc3e294e117a1a5d2bdb65c49693eab47b93e7926220612fd595404` |
| Complete plan SHA-256        | `39c2ae866be8775b6b34879cbabff92a97f1c9f7010d76f275027690c0b6dd51` |
| Interfaces size              | 683 LF-terminated lines, 42,364 UTF-8 bytes                        |

These are verification identities, not fresh exact-digest approval evidence. The original
AG approval remains attributed as recorded above; this amendment is attributed to the requester's
explicit conversation approval. The requirements file had already changed since the original
submission; the historical requirements digest above is not asserted against today's file.
This amendment does not change requirements or approve their intervening changes. Fastly policy,
optimizer horizon, applicability, measurement and hosted-qualification decisions stay open.

The current complete-plan identity also includes the 2026-10-24 Task P2 sequencing
clarification outside Interfaces: Axum can preserve its existing transport-certified empty
body during ingress conversion independently of outbound settlement. This implements an
existing subtask without changing public signatures, core deadline precedence or any provider
policy. Interfaces and normative requirements are unchanged by this checkpoint. This maintenance
identity is not new API, feasibility or policy approval; the original approval identities above
remain historical.

### Scoped Native Optimizer Parity Instruction

On 2026-10-24 the requester approved generic typed request options mapped directly to the native
Fastly optimizer, retaining streaming, cache/backend reuse, exact targets/signature-bearing headers,
all outbound limits and cleanup, without an extra fetch or representation layer. Profiles, SVG
selection, query stripping and S3 signing/preflight remain application policy. The instruction also
requires a measured performance comparison against the native application path and an SDK-only
control rather than inferring parity from API shape.

This scope is recorded in requirements EZ-8.12 and the existing EZ-8 hosted criterion, plan D1d,
and qualification Q4b/runbook. It does not change the approved Interfaces or supply a numerical
B/H(K), performance thresholds, a staging service/entitlement, deployment authorization or passing
evidence. G2 and the corresponding implementation/qualification gates remain open. No production
optimizer path was implemented under this instruction while those prerequisites are missing.

### Included Contract Decisions

The full migration decisions below remain the target contract. The
[scoped amendment](#scoped-timing-and-native-integration-amendment) permits a Send-first checkpoint
on the current trait before that atomic migration; it supersedes only the stated implementation
prerequisites, not these signatures or the remaining acceptance requirements.

- One required `dispatch_head` network primitive, synchronous support validation and paired
  `time()`, plus mandatory eager `begin`/batch scheduling. Supplied implementations use the same
  protocol. `OutboundHttpClient` stays object-safe and `Send + Sync`; owned futures, heads and
  cleanup owners may be local and capture `Rc`. No unsafe `Send` wrapper or core runtime is added.
- Facade invocation captures the original entry time. Unsupported options reject before budget
  rejection, timer registration, inbound settlement or network activity. `OutboundTime` pairs the
  application clock with the adapter's real wakeup factory; clock-only constructors hard-migrate.
  Early/frozen-clock wakeups recheck time and yield/rearm without extending the captured deadline.
- Request builders and readbacks cover connect timeout, decode/original response delivery,
  marker override and the complete typed Image Optimizer subset. Marker default remains **on**;
  client off and per-request Inherit/On/Off resolve into one effective policy. Decode stays default;
  original identity responses still enforce their decoded limit. Existing independent caps,
  pre-normalization header accounting, Content-Type provenance and typed errors remain intact.
- Heads own encoded bodies and one SDK cleanup owner. Header limits and immutable Location
  evidence precede filtering; body disposition precedes length checks, decoding and collection.
  Discard never polls payloads. Collected streams retain ownership instead of being cancelled
  when their pending handle completes. No SDK resource receives two independent release callbacks.
- Prepared batches and their contexts retain original indices, start, paired time, cutoff,
  selected budgets and exact preflight outcomes. External producers receive these through the
  facade, not unchecked public constructors. Drivers emit raw timestamped observations; only core
  creates immutable consumer receipts and derives elapsed/cutoff eligibility.
- Mixed collection uses `AlreadyDelivered`, `Resolved` and `Unresolved`, not ambiguous `Option`
  slots. Receipts and collections have read-only access and consuming decomposition. A batch-bound
  accumulator preserves ownership on rejected insert/merge, including the distinct original
  driver error. EOF/trailing validation, poisoning, exact-once release and the extra poll for
  `Finished(Completed)` remain. Cancel returns all never-delivered indices, including recorded
  but unconsumed results; consumers check first to retain those results.
- Explicit follow uses the same client's head pipeline. The complete redirect limit/context,
  prepared-request transfer, actions, generic non-Clone/non-Send policy error, stop reasons and
  success/error readbacks are part of this submission. The initial absolute chain deadline survives
  replacements; each hop captures its own entry and uses its selected request's timeout. Followed
  heads are discarded without payload reads.
- `BudgetSource::ConnectTimeout` and `IdleTimeout` extend the existing non-exhaustive enum;
  aggregate budget selection and public wire text do not gain a second cause field. Governing
  expiry wins connect ties; expired application/chain absolute deadlines win idle ties and late
  observation, as the requirements specify.
- Fastly's request-scoped observation handle, immutable snapshot/getters, extension access and
  `current()` scope are included in the API subject. Failed/late registration attempts count;
  retained snapshots do not retain work or attach to the next request. This is not approval of
  an unproved Fastly scheduling/backend implementation.

Arrival recording is subject to EZ-6.11's actual scheduling opportunities: Fastly observes during
EdgeZero operations; the other adapters also observe when the application yields to their
executor. CPU-only work or synchronously ready awaits do not promise background observation or
allow backdating a terminal instant. No summary here expands that normative contract.

The optimizer mode matrix is also fixed: Fastly Send/Follow only, subject to entitlement and
horizon gates; Fastly Begin/Batch and all modes on other adapters reject it synchronously.

### Scoped Timing and Native Integration Amendment

**Decision G4-TIMING-INTEGRATION: approved by the requester in the conversation on 2026-10-24.**
The requester answered `Approve scoped change and continue implementation` to this question:

> May I make a scoped requirements change to unblock implementation: preserve original logical
> deadlines and late-result rejection, treat +5ms as a measured qualification target rather than
> a hard scheduler guarantee, and allow native Image Optimizer integration with its blocking
> horizon explicitly unknown until provider evidence exists? Hosted qualification and performance
> claims would remain blocked.

This is an attributed scoped authorization, not numerical H(K)/B approval, approval of different
signatures, or a claim that a new whole-contract digest was reviewed. The original API approval,
historical submitted digests and region-enum amendment retain their separate scopes. The current
Interfaces bytes are unchanged; a refreshed complete-plan digest verifies only edits outside them.

This section takes precedence over earlier implementation-blocking G2 timing and optimizer-horizon
wording in this packet, the requirements, plans, feasibility index and fixture documentation:

- Preserve original entry `t0`, absolute deadline `D` and `BudgetSource`. Do not extend or round
  the logical deadline, restart the budget after preparation, accept a late result, or send after
  observed expiry. A synchronous optimizer return at or after `D`, success or failure, becomes the
  original attributed 504. Before expiry, missing optimizer entitlement remains a typed 502 with
  the unchanged message `Fastly outbound request failed`; its typed reason/provider detail carry
  the distinction, not new display text.
- Keep +5 ms as the measured qualification target and retain its tests, raw timings, cleanup
  endpoints and negative results. It is not a deterministic scheduler-resumption guarantee.
  Timing-only G2 failures no longer prohibit the scoped integration; they are not relabeled passes.
- The synchronous optimizer blocking horizon is explicitly **UNKNOWN** until provider evidence.
  Finite numerical H(K) is no longer a preimplementation prerequisite. Do not infer it from SDK
  timers, observed maxima, runtime termination or a fabricated policy. Horizon approval and hosted
  evidence still gate qualification; an unknown horizon cannot pass a finite-bound assertion.
- Under this 2026-10-24 amendment, backend B/domain/cardinality and resource-policy approval
  remained pending. The subsequent scoped B31 approval above supersedes that implementation
  restriction only for aggregate backend identity/timers. No dispatch slack, resource limit or
  SDK/runtime pin changes are authorized.
  Typed body-error preservation and fresh-readiness/subsequent-read safety remain requirements;
  no blocking-body fallback or lost-error-to-EOF conversion is permitted.

The [D1d Send-first checkpoint](https://github.com/IABTechLab/edgezero/blob/docs/outbound-http-spec/docs/superpowers/plans/2026-10-18-trusted-server-outbound-03-drivers.md#send-first-checkpoint-2026-10-24)
is an implementation sequencing decision under this integration authorization. Attach typed
options to requests/parts, validate support first on all four adapters, and map Fastly single
Send directly to SDK options inside existing `execute` and `process_response`. Keep the ordinary
SDK send branch and current `OutboundHttpClient` trait unchanged. No separate transport,
compatibility helper, default implementation or alias is introduced. This does not claim the
requester explicitly approved alternative signatures.

For this checkpoint, budget selection remains at existing adapter async `send` method entry
on first poll, before SDK preparation/registration. Preserve that selected budget across the native
call; this is not a new facade-invocation snapshot or completion of P1. Invocation-time capture
remains pending with the later atomic head-first/full-trait migration. The full contract's
invocation-time target is unchanged, not asserted as implemented by this subset.

Full EZ-1 settlement, Follow, new-mode backend integration, the new engine and the head-first/full-trait
migration remain open; the latter still migrates all implementers/callers atomically. The
checkpoint does not complete D1d or any full criterion. No deployment, Native capability promotion,
hosted qualification or performance-parity claim is authorized. Existing failed evidence,
255 acceptance criteria and 511 legs retain their IDs, statuses, applicability and comparison gates.

### Paired-Time Sequencing Approval (2026-10-26)

The requester answered `Approve this sequencing change` in this conversation to:

> May I move paired clock/timer integration ahead of the full trait migration? This would
> hard-migrate all four adapters to the already-approved `with_time`/`try_with_time`
> constructors, wire `OutboundTime` into live deadline handling, and migrate every caller
> without aliases. Public trait, batch-receipt and head-finalizer changes would remain one
> later atomic cut.

This supersedes only the earlier constructor/paired-time sequencing dependency. Use the
existing approved names and one live paired time source per client, including request injection
and real wakeups. Remove clock-only constructors; add no compatibility aliases, inert timers,
public factory helpers or standalone trait `time()` transition. Preserve support-first checks,
original deadlines, typed timer failures, source/transport cleanup and provider diagnostics.
Fastly timers participate in its cooperative multiplexer rather than blocking before source
polling. Keep application-clock/frozen-clock checks and host-event yields on the other targets.

The public trait, prepared contexts, batch identity/receipts, head finalization and caller
protocol still migrate together later. The frozen Interfaces and requirements are unchanged.
No SDK/runtime pin, B31/slack policy, capability level or hosted qualification is approved here.

#### Paired-Time Verification

The live checkpoint removes clock-only constructors from all four adapters and every caller;
standard injectors retain the exact application clock and pair it with a crate-private runtime
factory. The default constructors use real clocks and timers, not inert waits. Timer setup
precedes source/network polling, supplied failures retain their type and detail, and clock
high-water observations persist across chunks and cooperative yields. Terminal streams release
their source, timer and factory capture before yielding an error, even if the wrapper is retained.

Fastly's existing progress engine owns its timer registrations and original operation boundary.
Nested engines respect all ancestor deadlines, including custom-pair uploads without pending
handles; child progress causes parent repoll before another host wait. Expired/unconsumed
mailboxes do not block live siblings. Wakes and removed timer state are released outside state
borrows. No extra transport, per-slot blocking timer or public factory API was added.

Two independent review passes found cross-chunk clock-regression, timer-diagnostic and nested
progress/cancellation defects; deterministic regressions reproduced them and pass after repair.
Fresh local verification passes all five required Rust gates: formatting, workspace strict
Clippy, 2,293 workspace tests (one existing ignored test), all-target adapter feature compilation
and the Spin WASM check. Strict Clippy also passes for the three provider WASM targets. Runtime
tests pass: Cloudflare 53 library/29 contract cases, Fastly 238 library/36 contract cases and
Spin 15 SDK-resource cases on Wasmtime 48.0.2. The deterministic Spin helper uses its controlled
wakeable pair rather than polling a WASI 0.3 host timer under a non-WASI test executor.

Hosted CI additionally exposed test-helper declaration order in the isolated `cloudflare`
feature build, masked in `cloudflare,test-utils` by the existing module grouping expectation.
Move the helper after its fixtures rather than adding a suppression. Both isolated feature
combinations now pass strict WASM Clippy; the bare-feature contract also passes its 12 cases.

Demo workspace tests, reusable fixture tests, dependency/legacy/documentation guards, the
evidence checker tests and docs format/lint/build pass. The final actual-client Fastly probe
suite passes its receive cases on the tracked repaired Viceroy; Spin's local component smoke
passes functional cases. Both correctly exit 2 with qualification still open. They do not prove
hosted lifecycle timing, provider allocation bounds, callback-wide progress or end-client receipt.
No deployment occurred. The requirements and frozen Plan 02 hashes remain unchanged.

This completes only the approved paired-time sequencing checkpoint. Shared support-first
settlement/context issuance, the public trait and batch-receipt hard cut, eager Begin,
head finalization integration, redirects and hosted qualification remain separate work.

## Hard-Migration Inventory

This is the submission-time worktree inventory, including untracked G3 callers, not a claim that the
migration is implemented. Repeat the discovery commands before editing; new callers must join
the same cut. A separately approved portable API does not authorize a broken Fastly build,
stubbed transport, compatibility trait or removed baseline regression test.

| Existing Surface                                                  | Submitted Replacement                                                                                                                 |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Implementer `async send(OutboundRequest)`                         | Required `dispatch_head(OutboundDispatchRequest)`, `time()` and synchronous support validation                                        |
| Implementer raw request-vector batch                              | `start_batch_until(OutboundPreparedBatch)` and required eager `begin(OutboundDispatchRequest)`                                        |
| Borrowing facade send/collection futures                          | Owned local `'static` futures with invocation-time capture; `new`/`with_client` keep their signatures                                 |
| Axum `try_with_clock` / internal `with_transport_and_clock`       | `try_with_time` / `with_transport_and_time`; `try_new` stays fallible                                                                 |
| Fastly/Cloudflare/Spin `with_clock`                               | `with_time(OutboundTime)`; default constructors pair real clocks and timers                                                           |
| Raw-count batch construction, `from_futures`, `finish_batch_item` | Context-consuming `cutoff`/`from_driver`, raw driver observations and a group cancellation owner; remove superseded paths             |
| Public item/result/failure fields and receipt constructor         | Private identity with inspection/decomposition and lossless accumulator recovery; no reconstruction or mutable compatibility accessor |
| Direct adapter behavioral calls in tests                          | Facade calls through `HttpClient::with_client`; generic implementer assertions remain trait assertions                                |

### Eight Implementations

| Current File                                                     | Implementation / Migration Surface                                                              |
| ---------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `crates/edgezero-adapter-axum/src/outbound.rs:174`               | `AxumOutboundClient`; constructors 161/165, future factory 212, clock tests 521/544             |
| `crates/edgezero-adapter-fastly/src/outbound.rs:562`             | `FastlyOutboundClient`; constructors 300/307, completion helper 170, raw driver 653             |
| `crates/edgezero-adapter-cloudflare/src/outbound.rs:363`         | `CloudflareOutboundClient`; constructors 331/338, future factory 401, clock tests 1097/1126     |
| `crates/edgezero-adapter-spin/src/outbound.rs:408`               | `SpinOutboundClient`; constructors 296/303, future factory 446, clock tests 1049/1079           |
| `crates/edgezero-core/src/outbound.rs:1982`                      | `MockClient`; trait 419, facade 567, batch fields 124, factories 254/278 and helper 1307        |
| `crates/edgezero-core/src/context.rs:768`                        | `DummyOutboundClient`; raw driver 786 and context-client injection 1165; accessor 372 stays     |
| `examples/app-demo/crates/app-demo-core/src/handlers.rs:648`     | `TestOutboundClient`; application fields 196-199, item 683, driver 691 and injections 1300/1425 |
| `crates/edgezero-cli/src/templates/core/src/handlers.rs.hbs:338` | Template client; application fields 156-159, item 368, driver 376 and injections 595/643        |

### Callers, Injectors and Guards

| Files                                                                                                                                                                                                         | Migration Responsibility                                                                                                                                                                            |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/edgezero-adapter-axum/src/request.rs:69`                                                                                                                                                              | Clock-only constructor branches 69/70 and injection 75; preserve supplied-client precedence                                                                                                         |
| `crates/edgezero-adapter-fastly/src/request.rs:732`                                                                                                                                                           | Clock-only constructor/injector 732/733, helper paths 727/826/861/889 and batch test 1068                                                                                                           |
| `crates/edgezero-adapter-cloudflare/src/request.rs:351`                                                                                                                                                       | Clock-only constructor/injector 351/352, paths 347/498 and batch test 982                                                                                                                           |
| `crates/edgezero-adapter-spin/src/request.rs:152`                                                                                                                                                             | Clock-only constructor/injector 152/153, paths 143/535 and batch test 907                                                                                                                           |
| `crates/edgezero-adapter-axum/src/service.rs`, `dev_server.rs`, `response.rs`                                                                                                                                 | Preserve shared transport and installed LocalSet; integrate independent callback/body owners under D2                                                                                               |
| Cloudflare and Spin `src/response.rs`; Fastly `src/context.rs` and serving scope                                                                                                                              | Adapter-specific lifecycle/scope integration under D1/D3/D4; retain the current Spin repair                                                                                                         |
| `crates/edgezero-adapter-axum/tests/contract.rs:67`                                                                                                                                                           | Direct sends/batches at 67/98/256 and collection/item fields; runtime evidence remains D2/D5 work                                                                                                   |
| `crates/edgezero-adapter-fastly/tests/contract.rs:1303`                                                                                                                                                       | Direct batches 1303/1332 and clock constructor 1327; runtime evidence remains D1/D5 work                                                                                                            |
| `crates/edgezero-adapter-cloudflare/tests/contract.rs:482`                                                                                                                                                    | Clock constructors 482/517/547/566, direct batches 492/522/551/578 and collection 632                                                                                                               |
| `crates/edgezero-adapter-spin/tests/contract.rs:662`                                                                                                                                                          | Clock constructors 662/695/723/743, direct batches 673/701/728/756 and collection 822                                                                                                               |
| `crates/edgezero-adapter-spin/tests/sdk_resources.rs:24`                                                                                                                                                      | Retain trait assertion 24; migrate clock constructor 43, batch 56 and slot access 61                                                                                                                |
| `crates/edgezero-cli/src/generator.rs:1783`                                                                                                                                                                   | Literal assertions 1783/1786-1791 for `failure.error`, elapsed, clocks/imports and driver construction change with the template, not later                                                          |
| `tests/fixtures/reusable-app/crates/fixture-fastly/src/lib.rs:251`                                                                                                                                            | Migrate manually constructed clock-only client 251; facade send 253 remains                                                                                                                         |
| `tests/fixtures/reusable-app/crates/fixture-core/src/lib.rs:208`                                                                                                                                              | Preserve context-client send behavior and validate changed preparation                                                                                                                              |
| `tests/fixtures/reusable-app/crates/fixture-core/src/qualification/progress.rs:157`                                                                                                                           | Untracked G3 caller: facade send 176, batch 181 and Option-slot consumption 184-188; do not rewrite historical evidence                                                                             |
| `crates/edgezero-core/src/lib.rs`, `src/error.rs`, `src/time.rs`                                                                                                                                              | Export reviewed supporting types/causes; preserve aggregate budget selection and original clock domain                                                                                              |
| New `crates/edgezero-core/src/outbound/exchange.rs`, `image_optimizer.rs`, `progress.rs`, `redirect.rs`                                                                                                       | P1-P5 implementation modules, not alternative transports                                                                                                                                            |
| New `crates/edgezero-core/tests/outbound_public_api.rs`                                                                                                                                                       | External object-safety/local ownership, option/follow, and context-backed producer compile contracts                                                                                                |
| `docs/guide/proxying.md`, `streaming.md`, `capabilities.md`, `handlers.md`, `architecture.md`, `adapters/overview.md`, `adapters/axum.md`, `adapters/cloudflare.md`, `adapters/spin.md`, `adapters/fastly.md` | All paths use the `docs/guide/` prefix. Update affected contracts/examples; Fastly's current backend section at line 194 also needs the reviewed observation/getter/counting/frozen-handle contract |
| `scripts/check_outbound_docs_contract.mjs`, `check_outbound_legacy_api.sh`, `CHANGELOG.md`                                                                                                                    | Replace superseded active-signature assertions with equally strong reviewed contracts; retain retired-API guards and record the hard cut                                                            |

The historical `docs/superpowers/specs/2026-05-21-outbound-http-design.md` describes the implemented
baseline. It must not be rewritten to make a failed cutover pass. Explain supersession in active
guides and CHANGELOG as directed by Q1; the new requirements and approved Interfaces govern the cut.
Demo, generated projects and the excluded fixture workspace must build in the same series.
Source locations above are submission-time review anchors, not stable line-number APIs. Axum's
`src/service.rs:73/282` owns shared transport setup and `:607/614` its injection assertions.
Active guide consumers include `docs/guide/proxying.md:120/144/158` and
`docs/guide/adapters/overview.md:69`; active-source signature guards include
`scripts/check_outbound_docs_contract.mjs:744/1735`. Its `:1216` producer-signature check reads
the historical spec, not active source; review those assertions independently when implementing
the cut.

```sh
rg -n 'impl OutboundHttpClient|OutboundHttpClient|start_batch_until' crates examples tests
rg -n 'OutboundBatch(Item|Results|Failure)|finish_batch_item|OutboundBatch::(cutoff|from_driver|from_futures)' crates examples tests
rg -n 'with_clock|try_with_clock|with_transport_and_clock|OutboundResponse::new' crates examples tests
rg -n 'failure\.error|slot\.elapsed|handlers\.contains' crates/edgezero-cli/src/generator.rs
```

## Evidence Inputs and Outstanding Decisions

The [feasibility report](2026-10-18-outbound-feasibility.md) separates local results, provider
unknowns and cancelled/deferred work. These hashes identify submitted inputs, not passing gates:

| Input                                                                               | SHA-256                                                            | Disposition                                                               |
| ----------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------- |
| [G1 draft inventory](../../tests/fixtures/reusable-app/outbound-cutover-cases.json) | `232798a4a7e12a727e937d4ef5064c2c46c7420ded02cbbe6ce3534259f59819` | 255 acceptance criteria, 511 planned legs; scope/owner approval pending   |
| [G2 readiness evidence](evidence/2026-10-19-fastly-readiness.json)                  | `bf0911474b9f2dd50a5c0b1d1927edb1eafc865d7ff816e39bc686c379a88ac3` | Failed error preservation; hosted PendingExternal                         |
| [Initial G3 evidence](evidence/2026-10-19-outbound-progress.json)                   | `46effcfd323756d861efef16f46d4652741692db44df595dfcdc5cf8e93a9a9c` | Historical Spin failure, Cloudflare unavailable                           |
| [Repaired Spin/G3 evidence](evidence/2026-10-19-spin-response-egress.json)          | `59006383b01abbde4ad5a97a04841ccfe2cc4cb4627d14b5e13ad80febcc20f6` | Native/Viceroy/Spin exercised cases pass; smoke exit 2, not qualification |
| [G2/G4 policy/readiness proposal](2026-10-25-fastly-backend-policy-proposal.md)     | `011b090e21c7e8e6424b53303c55b1b2eacb7c896e51a81b6492c13fa65ce3e1` | B31 selected for review; resource/primitive/read-cap approvals pending    |

G3 does not yet prove Cloudflare execution, every protocol, missing active scheduler detection,
callback/export-return lifetime, all cancellation interleavings or the future public begin API.
Fastly's empty-response progress controls do not repair G2's body-readiness error loss. Spin's
header-derived zero-length hints are not raw framing evidence; no known-empty fast path is approved.

### Independent Decision Requirements

Apply the [scoped amendment](#scoped-timing-and-native-integration-amendment) to the original
task effects below: H(K) and timing-only G2 proof no longer block the Send-first checkpoint.
G4-FASTLY-POLICY still gates the full backend/readiness engine and resource qualification; the
separate scoped B31 approval authorizes the existing aggregate backend implementation only.
G4-OPTIMIZER-HORIZON remains pending for numerical horizon approval and qualification, not for
the permitted Send-first integration.

| Decision             | Required Attributed Review                                                                                                                                                                                                                                            | Task Effect                                                                                                                                                          |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| G4-API               | EdgeZero maintainer reviews the entire submitted Interfaces digest, migration/readback/recovery rules, defaults, causes, timer pairing and external producer contract                                                                                                 | Required before P1; does not approve Fastly feasibility, runtime promotion or production adoption                                                                    |
| G4-FASTLY-POLICY     | EdgeZero and TS maintainers review a new preserving primitive/report plus literal positive B, fixed origin/authority/TLS/connect-settings scope, complete budget domain, rounding/cardinality proof and boundary evidence                                             | D1 needs passing local G2 plus this approval; the currently tested primitive remains failed, not waived                                                              |
| G4-OPTIMIZER-HORIZON | Both maintainers separately approve original t0/D/source, actual backend-policy digest, finite numerical H(K) rule, literal finite maximum/domain and proposed 5 ms observation tolerance, accounting for every phase/retry/provider step                             | Blocks optimizer integration until approved and production qualification until hosted proof; never widens ordinary body/head deadlines                               |
| G4-APPLICABILITY     | EdgeZero maintainer approves exact part/target/leg adoption decisions; changes to TS's blocking/applicability set also require TS approval. Record basis, implementation/execution/CI owners and evidence; confirmed-present parts need CHANGELOG and TS confirmation | Locks G1 scope; EZ-24 remains Follow-up unless explicitly promoted; missing Image Optimizer entitlement does not demote blocking EZ-8                                |
| G4-MEASUREMENTS      | Before replay, confirm M1/M2/M5/M7 samples, warm-up, settings, thresholds, zero-baseline and negative/inconclusive-control rules against identified builds                                                                                                            | Enables those comparison runs, not an automatic passing result; M3's count invariant remains pass/fail, its timing distribution report-only; M4 reuse stays deferred |

The submitted product thresholds are unchanged from EZ-2/EZ-3 and still require confirmation
before replay. Sample counts, runner/build identities and zero-baseline/control rules are not
approved by listing these proposed values:

| Measurement | Submitted Bound                                                                                                                                                                     |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| M1          | After five-minute warm-up, new handshakes per request to the origin and provider are at most 10 percent above baseline                                                              |
| M2          | Each bidder's p95 slot and response-head latency is at most 10 ms above baseline                                                                                                    |
| M3          | Registrations per inbound request do not exceed its distinct origin configurations; duration distribution is report-only                                                            |
| M5          | Gateway-failure share is at most 10 percent above baseline share or 0.1 percentage points above it, whichever is larger; apply the connection-limit subcategory if EZ-16 is present |
| M7          | Zero pre-send internal-error slots/sends, reported per 1000 auctions by bidder count; per-slot 504 rate at most one percentage point above baseline, both for a 12-bidder profile   |

M4 reuse-mode capacity characterization remains deferred. Fixed deadline acceptance windows and
required negative/inconclusive-control handling do not become optional product thresholds.

Applicability decisions must name EZ-5.9 and EZ-5.10 separately; EZ-24 host/WASI promotion,
EZ-24.11 abandonment, EZ-24.12 demo/template replacement and the JavaScript target separately;
and EZ-25.42/.43 separately. Preserve all other individually scoped pending decisions in the G1
inventory. Approving one sibling does not adopt a range. Scoped approval of portable work does
not cancel the blocking Fastly requirements or permit an incomplete adapter migration.

For each completed decision, record the exact requirement/target/leg scope; approved subject
digest; actual approver(s)/date; implementation, execution and CI owners; source/SDK/runtime/settings
identities and evidence hashes; unresolved conditions; and explicitly blocked/unblocked tasks.
Keep unknown B, H(K), owner assignments and pending approvals unset rather than manufacturing values.

## Review Exit

- [x] AG approves the exact full Interfaces digest for API implementation only, on 2026-10-19.
- [ ] G1's required applicability/owner decisions and G3's remaining prerequisites are resolved
      for the work selected to proceed; portable progress is not mistaken for production readiness.
- [ ] Fastly primitive/B and optimizer horizon decisions have their own attributed dispositions;
      a negative or unavailable result remains a visible block for dependent work.

No checkbox is satisfied merely by creating this packet, passing a unit test or receiving an
unscoped instruction to continue. The scoped exact-digest reply and supplied maintainer name close
the API approval entry only; the remaining decisions still need their own approvals.

**Downstream P1 obligation, not a G4 review exit:** after approval, P1 rereads the decisions,
verifies their digests and hard-migrates every affected implementation and caller with nonzero
red/green sentinels and the index's test/fmt/clippy card. Closing API review does not complete P1.

## P1 Foundation Checkpoint

The scoped reply starts the approved clock/timer foundation only: `OutboundFuture` and
`OutboundTime` are declared in core outbound, re-exported at the crate root, and implemented in
private `outbound/exchange.rs`. Named generic syntax for the factory uses the same approved
`Fn(Deadline, MonotonicClock) -> OutboundFuture<()> + Send + Sync + 'static` bounds.
No new executor, default/inert timer, adapter scheduling fallback or transport was added.

Eleven co-located deterministic tests cover the foundation. Initial compilation failed on the
missing public types/exports; a later ready-error-at-expiry regression failed before its fix.
They cover paired-clock wakeups and waker replacement, retaining pending registrations,
early/frozen-ready rearming with the original deadline, typed original errors, equality/expiry,
clock regression, clone/local-future contracts, and exact release on cancellation and terminal
polls. A terminal wait releases captured factory state even if its completed boxed future remains
allocated. Ready factories under frozen time yield once per poll; real pending wakeups remain the
provider factory's obligation, not a core scheduling guarantee.

A retained timer is polled once even when a later outer poll arrives at/past expiry, preserving
its original ready error instead of discarding it. A still-pending timer is cancelled when the
paired clock expires; no new timer is constructed for an already-expired wait. Error-path probes
remain fields of the timer after `Ready`, proving actual timer destruction rather than an
explicit test-local cleanup. Fake time is seeded ahead of process time so it is representable
on the JavaScript target as well as native/WASI.

This does not establish settlement's timer-before-body ordering or 408/504 precedence, eager
public begin/batches, nor any adapter/runtime qualification. Existing client traits/constructors,
request options, demo and generator still use the current implemented contract. They must migrate
together in the later atomic dispatch cut; no compatibility alias was introduced to defer it.
Historical G2/G3 source/artifact records have not been relabeled as evidence for this later work.

That constructor status describes the initial foundation only. The subsequently approved
2026-10-26 paired-time sequencing checkpoint hard-migrates constructors and live timer races;
the dispatch trait and head/batch protocol remain reserved for their later atomic cut.

Verification for this foundation passed all five workspace Rust CI commands, 761 core unit
tests (including the eleven time tests), the nonzero timer sentinel, core compilation for
`wasm32-unknown-unknown` and `wasm32-wasip1`, and the Spin `wasm32-wasip2` check. The full workspace
suite had no failures and one existing ignored generated-project test. Strict clippy, decoder/map
dependency audits, outbound docs/legacy guards, the 18 evidence-checker tests and docs format,
lint and build passed. These are local checks, not hosted qualification or acceptance of G1's
511 still-planned legs.

## Scoped Live-Validation Helper Approval (2026-10-25)

The requester approved the exact checkpoint question in this conversation with
**"Approve hard-cut helper change"**: change the existing public helper to
`validate_for_dispatch(&request, OutboundSendMode)` and migrate every caller without a
compatibility wrapper. Consolidate live Send/Batch validation and preserve existing outbound
requirements. This is a separate helper-signature approval, not an interpretation of the
original exact Interfaces digest as covering a signature it did not declare.

The frozen head-first Interfaces and their identities remain unchanged. This approval does not
authorize alternative implementer traits, new transport paths, a Fastly backend policy/bound,
runtime or SDK repins, weaker timer/slack/read-safety requirements, deployment or qualification.

## Scoped Fastly Current-Operation Multiplexer Approval (2026-10-25)

The requester approved the exact checkpoint question in this conversation with
**"Approve scoped implementation"**: one cooperative Fastly multiplexer with fresh,
error-preserving readiness, at most 16 KiB per raw read, and rotating slot service, preserving
B31, original logical deadlines, observation cutoffs and limits. Local/CI validation explicitly
uses the tracked Viceroy repair; deployment, hosted runtime promotion and resource qualification
remain separately gated. No dependency or installed runtime pin changes.

This implementation slice shares one private engine across the existing Send and Batch receive
paths. It replaces blocking selected-body drains, not the frozen public implementer protocol.
SDK-owned cached Send heads remain opaque and use documented nonblocking polling with one shared
positive 1 ms pulse; raw batch heads and response bodies use borrowed-handle timed selection.
Native Image Optimizer remains the direct synchronous SDK operation. Registration, upload writes,
blocked inbound source pulls and SDK cleanup retain their disclosed limitations.

Callback-wide scheduling/ownership, unpolled batch progress during unrelated operations, eager
Begin, the atomic four-adapter public API migration, dispatch-slack retirement, and hosted
qualification are not completed or waived by this scoped approval. The full Interfaces digest,
requirements digest, historical policy proposal and negative evidence remain unchanged.

## Functional CI Validation Decision (2026-10-26)

After the recommendation to gate functional client cases separately from the strict SDK timing
diagnostic, the requester directed **"continue"**. Implement that CI split only: the explicit
`outbound-client-progress` harness suite reuses the actual client cases, artifact, origin bounds
and validators. CI requires exactly exit 2, the complete passing twelve-probe/eighteen-request
inventory, no failures and an explicit unverified disposition. It is not a qualification pass.

Keep `outbound-readiness` and its full SDK matrix, strict timing assertions, exit semantics and
historical failures intact. Excluding those broader diagnostic cases from ordinary CI is a
coverage choice, not proof that every observed failure is environmental or permission to waive
candidate-specific qualification. The original +5 ms target, logical deadlines, typed-error/read
safety requirements, public protocol and frozen requirements/Interfaces remain unchanged.

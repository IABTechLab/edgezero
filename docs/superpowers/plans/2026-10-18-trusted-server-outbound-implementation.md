# Trusted Server Outbound Adoption Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Trusted Server adoption requirements without regressing the outbound work already implemented in PR 275.

**Architecture:** Extend the existing portable outbound policies and adapter-owned exchanges; do not introduce a second transport. Use one head-first dispatch boundary for plain sends, eager exchanges, and explicit redirect following, with independent callback and response-body ownership. Prove the pinned Fastly timed-read boundary before replacing its dispatch and harvest engine.

**Tech Stack:** Rust 1.98.1, existing Cargo features, Fastly SDK 0.13.1 and Viceroy 0.21.0, worker 0.8.5/workerd, Spin SDK 7.0.0/WASI HTTP 0.3 and Spin 4.1, reqwest 0.13.4, existing native Axum/Hyper runtime.

---

## Status and Authority

The current repair replaces recurring Workers response-egress abort polling with an owned
listener inside the existing operation race. See the
[event-driven cancellation checkpoint](../../audits/2026-11-04-native-timing-and-disconnect.md#event-driven-response-cancellation-2026-11-06).
Original deadlines, accepted-byte accounting, source ownership and capability levels remain
unchanged. The preceding clean `6e5ce765` run still fails two timing scenarios and ordinary-close
cleanup; it is not evidence for the listener repair or final qualification.

The preceding repair is the
[absolute Workers response-write wait](../../audits/2026-11-04-native-timing-and-disconnect.md#absolute-response-write-wait-repair-2026-11-05).
It reuses the existing timer and race, preserves accepted native bytes and rejects expired source
polls. The preceding clean merged-head native characterization retains four failed scenarios and
the tail failure; it does not qualify the repair or supersede the original timing/cleanup gates.

The preceding runtime repair is the
[independent Workers batch cutoff](../../audits/2026-11-04-native-timing-and-disconnect.md#independent-batch-cutoff-2026-11-05).
Its native run is working-tree characterization, not a replacement exact-candidate record.
Cold-entry, fast-origin timing, ordinary-close cleanup and qualification gates remain open.

Current exact-head evidence disposition is recorded in the
[2026-11-05 mapping checkpoint](2026-10-18-trusted-server-outbound-05-qualification.md#exact-head-mapping-checkpoint-2026-11-05).
Binding readiness and successful PR checks do not establish exact-head execution or close
failed timing/cancellation, hosted, freeze or durable-publication gates.

### Split Completion CI Guard (2026-11-04)

At `55e07f69`, 29 checks pass and Fastly's WASM job fails after reporting 309 passed/zero failed.
The exact raw artifact reproduces the guard failure: a named test is interrupted by recognized
diagnostics, then its split `ok` is immediately followed by another diagnostic. Existing code
accepts the prefix and suffix forms separately, but not their composition.

- [x] Retain the failed job and downloaded raw artifact; reproduce failure by parser replay.
- [x] Add failing combined-output regressions, then share the existing exact completion predicate
      between named and split result handling. Retain pending-name/interruption requirements.
- [x] Cover ANSI/CRLF and should-panic forms, malformed suffixes, missing/failed/zero summaries,
      invalidated associations, all four exact-name inventory groups and nonzero command exits.
      Preserve the existing stream-exit negative control.
- [x] Replay the same raw artifact successfully with no altered test output or positive summary.

Artifact: `/private/tmp/pr275-55e-fastly-ci-artifact/`; red/green replay logs are under
`/private/tmp/pr275-split-suffix-artifact-*.log`. Replay does not certify the failed CI job or
replace fresh execution. Both independent proposals favor sharing the completion predicate over
unproven stream separation; results written to stderr remain supported. No workflow, inventory,
production API/adapter, provider pin, deadline, capability or acceptance change. The remaining
timing, ordinary-close cancellation, candidate freeze and hosted requirements stay open.

All five root gates pass: 2,666 tests across 33 binaries with one existing ignored, formatting,
strict Clippy and both feature/Spin target checks. All 301 tooling regressions pass, including
combined-output and failure-association controls. Bash/Node syntax, script formatting, docs
format/lint/build and docs/legacy contracts pass. Independent code/evidence reviews find no
remaining repair issues. The old failed job remains failed; new-head CI must execute separately.

### Admission Boundary Observation (2026-11-04)

All 30 GitHub checks pass at `9f0a3984`. Continuing the existing timing investigation adds one
request-owned fixture sample, not a public API, production timing hook or alternate transport.

- [x] Extend the actual-route regression first: admit at 3 ms, dispatch at 9 ms and require the
      original request epoch and Begin at 9 ms. Observe failure for the missing admission sample.
- [x] Capture the app clock after clock configuration; record one sample after the fixed admission
      path predicate for slow-origin, timeout-before-cutoff and response-handoff. Carry the sample
      and epoch through a private grant, preserving batch-write/cleanup grants and completions.
- [x] Reject missing, empty, foreign and wrong-epoch grants before outbound work; verify foreign
      resource release exactly once. Check raw ordering before truncation, equal samples, null
      unknown/pre-epoch values and reporting without clock reads. Forged metadata cannot rescue
      the original late-Begin failure.
- [x] Run the existing complete Workers lifecycle suite with unchanged budgets and acceptance.

Capture: `/private/tmp/pr275-admission-phase-workers-v1/`, working-tree build identity
`31d3269bb94ac37958658aa8e767d823535082edc7324eaf8979d6aab95a6bcc`. Cold slow-origin records
admission **4 ms**, handler **8 ms**, wait **8/9 ms** and Begin **9 ms** with **2 ms** call duration.
Comparison admission/handler/wait-return is **0/0/4 ms**; handoff records **0/0/3 ms**. These split
observations do not isolate conversion, routing, JIT or scheduling. The sample is after selecting
its path, not literally policy entry. No default-clock fallback or observer-cost subtraction.

The complete suite reports **17 pass/two fail, exit 1**, plus a final-log-tail failure. Cold Begin
and ordinary-close cancellation still fail. Pending-body cutoff returns at 205 ms here; its
historical misses remain recorded. No requirement, API, adapter, capability, provider pin, demo,
template, suite, invocation list or CI job changes. Exact-candidate and hosted proof remain gated.
Both read-only design proposals favor the existing grant over a new public hook or shared map.

Verification passes all five root gates (2,666 tests/33 binaries, one existing ignored), 88 fixture
tests with native witnesses and 72 without, 151 owning lifecycle-harness tests, strict fixture and
harness Clippy, all three fixture WASM checks, all three workspace format checks, 284 tooling
tests, docs format/lint/build and docs/legacy contracts. Initial sandboxed workspace/harness runs
fail at denied TCP binds; reruns with owned-loopback access pass. Independent code and evidence
reviews have no findings. These results do not qualify the retained failed runtime cases.

### Cold Entry Phase Observation (2026-11-03)

All 30 GitHub checks pass at `f82e4025`. Continuing the remaining timing investigation adds
bounded diagnostic samples to the existing fixture, not production behavior or acceptance.

- [x] Fail first on an actual-route late-entry test, then capture three original-clock instants:
      fixture-handler-body entry and the original 2 ms wait's entry/return. Reuse the existing
      Begin entry instant. Keep fixed typed storage and format only after operation timestamps.
- [x] Share the two existing origin wait/request preparation sequences privately, preserving
      `?` propagation, request policies and handoff's original `owner.with` dispatch boundary.
- [x] Prove on-time and 9 ms late handler entry keep the original epoch; the late Begin still
      fails unchanged validation even with forged diagnostic metadata. Preserve null for unreachable
      or pre-epoch samples, raw ordering before truncation, and reporting without clock reads.
- [x] Execute the existing complete local Workers suite without a warmup or changed timing bound.

Capture: `/private/tmp/pr275-entry-phase-workers-v1/`, working-tree build identity
`2eef23c779117db5a410d3930daf082a12deb992158cf7300d171f32471ef45c`. The cold case records
handler/wait-entry/wait-return **8/8/8 ms**, followed by Begin **9 ms** with **1 ms** duration.
The original 2 ms target is already expired, so the wait is skipped. Comparison **0/0/3 ms**
precedes Begin at **3 ms**. This places the observed cold delay before fixture-handler entry;
it does not attribute it to URL parsing, conversion, routing, JIT or scheduling. Equal/coarse
samples do not prove zero work or CPU duration; local behavior must not be inferred from deployed
clock freezing. See the [audit and primary clock references](../../audits/2026-10-18-outbound-feasibility.md#cold-entry-phase-observation-2026-11-03).

The full suite remains **16 pass/three fail, exit 1**, plus a final-log-tail failure. Cold Begin,
pending-body cutoff at 207 ms and ordinary cancellation remain failed; historical negatives are
not retried for credit or erased. The diagnostics do not measure actual timer delivery or abort
duration. Both independent designs favor the minimal three new samples over six boundaries or
a new diagnostic API. Production APIs/adapters, SDK/runtime pins, deadlines, caps, acceptance
identities, capabilities, demo and templates are unchanged. Candidate freeze and hosted proof
remain separately gated. No new script, runtime, suite, invocation list or CI job is introduced.

Verification passes all five root gates (2,666 tests/33 binaries, one existing ignored), 87 fixture
tests with native witnesses and 71 without, 151 owning lifecycle-harness tests, strict fixture and
harness Clippy, all three fixture WASM checks, all three workspace format checks, 284 tooling
tests, docs format/lint/build and docs/legacy contracts. Independent code and evidence reviews
have no remaining findings. These results do not qualify the retained failed runtime cases.

### CI Completed-Result Diagnostic Suffix (2026-11-03)

At `158b0c0e`, 29 GitHub checks pass and the [Fastly job](https://github.com/IABTechLab/edgezero/actions/runs/41777623478/job/126401545527)
fails after its library reports 309 passed/zero failed. One complete named `ok` is immediately
followed by a timestamped Viceroy backend diagnostic without a newline. The guard recognizes
that diagnostic before a split completion, but previously rejected it after a completed result.

The downloaded `fastly-lifecycle-evidence` artifact is retained under
`/private/tmp/pr275-158b-fastly-ci-artifact/`. Replaying its exact `runtime-unit-tests.log` reproduces
the missing-sentinel error. The co-located guard tests first fail six expected positive cases:
two result formats and exact inventory names in four environments. All negative controls pass.

The minimal repair extends only the exact completion condition: `ok` may be immediately followed
by the existing anchored host-diagnostic predicate. No generic success-prefix match is added.
The same artifact then passes the repaired guard; it still contains the original guard-error text,
so this is parser replay, not fresh runtime execution or a claim that the old CI job succeeded.
Malformed/missing timestamps, unrelated suffixes, failed/ignored statuses, wrong names, missing
sentinels, failed/zero summaries and nonzero command/stream exits remain rejected. Two read-only
reviews favor this path over separating log streams, which would change stderr-result support
without necessarily repairing upstream interleaving. No workflow, command, inventory identity,
production API, provider pin, deadline, capability or remaining qualification gate changes.

All five root gates pass again: 2,666 tests across 33 binaries with one existing ignored test,
formatting, strict Clippy and both feature/Spin target checks. All 284 tooling regressions pass,
including the 109 guard tests; Bash syntax, script formatting, docs format/lint/build and
docs/legacy contracts also pass. Independent parser and scope reviews have no remaining findings.
The failed hosted job remains failed; new-head CI must run separately after this repair is pushed.

### Deferred Body Dispatch Timer (2026-11-03)

All 30 GitHub checks pass at `8fbc4f83`. Continuing the unchanged timing investigation identifies
one deterministic fixture defect, not a new production transport defect: batch-write computes
a relative delay before returning its response, while the provider starts that delay only when
the deferred body is first polled. A 7 ms first poll therefore dispatches at 41 ms instead of
waiting for the original 34 ms target.

- [x] Reproduce the defect in the existing actual-route test before changing implementation.
      Preserve the request epoch and one-second egress policy; require no transport calls before
      body polling or through 33.999 ms.
- [x] Use the existing native time pair's absolute 34 ms wait inside the same deferred body.
      Initial polling at 0/7 ms dispatches at 34 ms. Already-late polling at 40 ms dispatches at
      actual 40 ms, without backdating or renewed sleep; existing strict validation remains unchanged.
- [x] Execute the same complete local Workers suite. The batch-write case passes with dispatch
      at 34 ms, original per-call deadlines at 1,034 ms and `DeadlineExceeded` at 1,001 ms.
      The original egress owner is installed before batch dispatch and remains exactly once.

Capture: `/private/tmp/pr275-deferred-batch-absolute-native-v2/`, working-tree build identity
`9b8eda9506db81eab84027a2e2b456a9aba2324fef3f0375b4604d16e9a50b47`. The first capture
without the required explicit workerd executable stops at preflight with unverified exit 2.
The full native suite remains 16 pass/three fail, exit 1, plus a final-log-tail failure after the
failed cancellation case. Cold Begin enters at 9 ms, pending-body cutoff returns at 206 ms and
ordinary cancellation lacks proof. Pending-head cutoff returns at 204 ms in this run; historical
failed observations remain failures. No retry-for-credit or tolerance change is made.

Two read-only investigations and minimal-fix reviews agree on the absolute paired wait rather
than another timer mechanism or added diagnostic API. Saved cold Begin/handoff timestamps place
their lateness before API entry; call durations are 0-1 ms at recorded resolution. That does not
attribute the pre-entry delay. The cutoff traces still lack actual timer-wake and abort-call
brackets; SDK head-operation release precedes the outer abort guard, so it cannot establish abort
completion. Production APIs/adapters, pins, deadlines, caps, acceptance identities, capabilities,
demo and templates are unchanged. Candidate freeze, hosted proof and remaining timing/cancellation
requirements stay open. See the [audit](../../audits/2026-10-18-outbound-feasibility.md#deferred-body-dispatch-timer-2026-11-03).

Verification passes all five root gates (2,666 tests/33 binaries, one existing ignored test),
85 fixture tests with native witnesses and 69 without, 150 owning lifecycle-harness tests,
strict fixture Clippy, all three fixture WASM checks, formatting, 267 tooling guards, docs
format/lint/build and the docs/legacy contracts. These checks validate the fixture correction,
not the remaining failed runtime or hosted requirements. No additional suite or CI job is added.

### Isolated Reset Diagnostic (2026-11-03)

The scoped continuation adds one uncredited control, not an alternative cancellation contract.
The existing Rust client now explicitly selects ordinary close or reset on prefix rejection.
All original callers retain close. Only typed observer rejection in the reset control arms zero
linger on the exchange's sole socket owner before drop; it performs no shutdown or additional I/O.
Socket-option failure retains the original partial response and rejection reason with a distinct
typed failure. A real TCP test first observes EOF where reset is expected, then passes with exact
peer `ConnectionReset`. Ordinary close, successful notification and non-rejection premature EOF
retain peer EOF. The test peer sends no unread tail or subsequent write that could itself cause
a reset. Its accept and read waits are finite.

After the original cancellation case fails, the existing runner finishes its runtime and strict
tail before starting the same pending-source fixture in a fresh runtime. The existing complete
empty-body control first attests that launch, before the case and its coordination deadline begin.
An initial capture without that prerequisite is retained as failed: intentional interruption
cannot attest its own first response, and the resulting startup rejection invalidates the survivor.
No startup check is relaxed. Separate tokens, cursor,
log and `<adapter>-reset-diagnostic/events.jsonl` prevent either run's late records from feeding
the other's proof. The original result takes precedence over diagnostic-recording errors. The
control keeps the original two-second coordination and all request/source budgets. It does not
change the primary case count, acceptance sentinel or capability level. Passing runs do not start
the extra runtime; no selector, CI job, transport or executor is added. The diagnostic's own log
tail is checked after shutdown. Source tests and execution-owner mutation guards distinguish the
diagnostic from every original gate and reject bypassed primary errors or reversed error precedence.

Two read-only reviews compare policy selection with a separate helper and runtime isolation with
same-runtime cursor partitioning. The selected path reuses the sole HTTP implementation and
existing runner, avoiding additional entrypoints and cross-run log ambiguity. Review fixes bound
test accept, make diagnostic directories adapter-specific and preserve primary-error precedence.
No original close/cancel failure, strict timing miss, hosted gate or provider limitation is waived.

Native Workers execution with that setup records all 19 ordered reset-control stages, a
`RequestCancelled` victim terminal, exactly-once SDK/source release and healthy survivor work.
Its diagnostic tail passes, but earns no acceptance credit. Ordinary close still fails: eight
validated setup/Pending stages and eleven canonical diagnostic rows contain no victim terminal
through the original two-second window. The primary suite remains 15 pass/four fail, exit 1:
cold Begin is 8 ms against 2 +/- 5 ms, both pending-cutoff controls miss the original 205 ms
bound, and ordinary cancellation remains unproved. The separate reset result does not alter those
counts. Capture: `/private/tmp/pr275-reset-workers-lifetimes-v2/`; the invalid initial diagnostic
remains in `/private/tmp/pr275-reset-workers-lifetimes-v1/`.

Spin's ordinary-close and reset captures each retain eight validated setup/Pending stages and
eleven canonical diagnostic rows without victim cleanup. Both remain failed, with no provider
impossibility claim. The primary macOS run is 17 pass/two fail plus the Linux-only connect case
unverified, exit 1: slow-origin slot elapsed is 153,575 us against 146,000 +/- 5,000 us, and ordinary
cancellation lacks proof. Capture: `/private/tmp/pr275-reset-spin-lifetimes-v1/`. Neither target
changes SDK/runtime pins, capabilities, accepted identities or budgets. Source inputs stay fixed
across both executions; this is working-tree characterization, not frozen-candidate qualification.

Final verification passes all five root gates (2,666 tests across 33 binaries, one existing ignored),
585 serialized harness tests (two existing ignored), strict harness lint, 165 evidence/workflow
guard tests, docs format/lint/build and the legacy-name guard. The initial concurrent harness run
retains one existing process-cancellation test's three-second timing miss; its unchanged isolated
and final serialized runs pass. No test timeout or acceptance tolerance is widened. Reviews of
the socket ownership, diagnostic isolation, startup prerequisite and retained evidence have no
remaining findings. These checks validate the diagnostic tooling, not the failing runtime gates.

#### Native Spin Signal Follow-Up

The [retained native record](../../audits/evidence/2026-11-03-spin-response-result-local.json)
and [source analysis](../../audits/2026-10-18-outbound-feasibility.md#native-spin-result-before-eof-2026-11-03)
narrow the remaining signal question without a production repair. The existing raw SDK controls
retain their original ready/timer-only cases and 100 ms wait. One sample starts the native read;
the timer control samples again only if initially Pending, after its existing wait. No third
pre-EOF poll, new timer/write callsite, route, transport, executor or CI job is added. Completed
values are cached permanently; the original post-EOF await remains only for still-Pending work.
Bounded case-specific transcripts, ownership premises and matching component terminals are
validated with fail-first and mutation controls. No compatibility schema is retained.

The ready control observes Pending, which is inconclusive. The timer-only control observes
Pending then `Ready(Ok(()))` across 102.1215 ms, while its writer is still owned and its body result
unsent, before stream EOF. The precise native completion time is unknown. Its one-shot result
cannot thereafter signal another error; it is not an ongoing disconnect watcher or delivery proof.
All four comparison controls and the complete progress suite pass. The canonical component log
has two successful correlated native terminals. Runtime panic text after controlled shutdown is
retained separately, not hidden by that success. Neither this raw unbounded-response behavior nor
the pinned host's request-I/O wiring proves universal provider impossibility or a remaining
EdgeZero defect. The existing coordinator must preserve body work after early host success.

Source review finds a write-side/read-side distinction in Workers' pinned native HTTP boundary,
consistent with the close/reset asymmetry. Actual packet attribution remains unproved because
local BPF capture needs administrator access; no traffic was captured or permission bypass tried.
Original cancellation/timing gates, scope, pins and capabilities remain unchanged. No upstream
issue is posted, candidate frozen or hosted deployment authorized by this investigation.

Verification: all five workspace gates pass, including 2,666 tests across 33 binaries with one
existing ignored test. The harness passes 586 tests with two existing ignored tests; fixture core
passes 84 tests. Three feature-enabled Spin response-race model tests pass under the nonzero-test
guard; those model results are not native cancellation evidence. Harness and Spin fixture WASM
strict Clippy, all three workspace format checks, 165 tooling guards, legacy-API checks and docs
format/lint/build pass. Independent fixture, validator and artifact reviews find no remaining
issues in this checkpoint; no hosted or original failing lifecycle gate is credited by these checks.

### Runtime Boundary Investigation (2026-11-03)

All 30 GitHub checks pass at `f036425b`. The following changes and executions are earlier
working-tree characterization, not frozen-candidate or hosted qualification. Outbound API,
provider pins, deadlines, caps, accepted identities and capability classifications are unchanged.

The cancellation controller previously stopped its ordered scan when the survivor's independent
one-second acknowledgment request timed out, releasing held sources around 1.01 seconds instead
of observing the remaining original two-second coordination window. A failure-only observer now
keeps the strict error, validated cursor and native-proof credit unchanged while collecting a
separate bounded canonical trace until that same deadline. It reuses the existing parser and
1 MiB log/2 KiB record limits, retains arrival order and duplicates under the original row cap,
and flags malformed/incomplete/truncated/late or failed reads. It runs only after an intentional
prefix-bound cancellation has actually unwound the client exchange, before source release.
Successful cases and all request/source budgets are unchanged. Fail-first and mutation tests
pass; final serialized harness verification passes 583 tests with two existing ignored helpers.

Miniflare's entry worker has different compatibility flags from the application worker and forwards
through a service binding. The existing fixture launcher now selects the supported native default
user-worker socket directly, without an entry-listener fallback, preserving guest flags, modules,
bindings, persistence and disposal. Miniflare's fixed additional entry/control listener remains
on a distinct ephemeral loopback port, unused by the client. The existing Rust-controlled launcher
test first fails against entry readiness, then passes direct selection, address/API rejection and
shutdown controls. This corrects a test boundary; it is not proof that entry forwarding caused the
cancellation failure. Wrangler serve/deploy paths and production adapter code are unchanged.
Fresh and retained Workers smoke modes also pass with KV-backed fixture behavior. Axum's
unchanged lifecycle suite passes all 19 cases; all five cleanup paths retain native proof and
leave the new diagnostic field empty, so successful observation does not enter the failure drain.

Actual Workers direct-socket execution passes the complete wire suite, including all 97 paired
Send/Begin vectors, and all ingress-overflow/recovery controls. Its lifecycle run passes 15 cases
and fails four: cold Begin is 10 ms against 2 +/- 5 ms, response-handoff batch entry is 58 ms
against 34-39 ms, batch-write entry misses that same original window, and pending-source client
cancellation lacks terminal/source proof. Head/body cutoff controls pass at 204/205 ms in this
run; that does not erase earlier misses or establish their cause. Spin on macOS passes 18 cases,
retains the cancellation failure and reports the Linux-only connect case unverified.

Both cancellation captures now retain eleven canonical records through the original deadline
with all diagnostic-error flags false. They show eight valid setup/Pending stages, followed by
the survivor's three failure stages, but no victim cleanup. Controller source release occurs at
about 2.000 seconds, not 1.01 seconds. That release is not SDK release or a guest source-drop
observation. The negative is now better bounded, but does not establish provider impossibility
or a missed EdgeZero race. The client uses its default socket-close policy, not an explicit TCP
reset; any discriminating reset control must remain separately labelled without replacing this
original failing gate. Raw captures remain under `/private/tmp/pr275-direct-guest-cancellation-v1/`
and `/private/tmp/pr275-cancel-diagnostics-spin-v1/`.

The [retained Linux connect record](../../audits/evidence/2026-11-03-spin-silent-connect-local.json)
closes the previously unavailable local execution observation for existing `EZ-4/acceptance/06`
on Spin. Linux/aarch64, Rust 1.98.1 and checksum-verified Spin 4.1.0 execute SDK 7.0.0 at the
unchanged pinned revision. The controlled saturated backlog yields 26 exact Spin-owned
`SYN_SENT` observations for PID 3321/inode 4448928, then typed `ConnectTimeout`/504 at 202,813 us
under the unchanged 200 ms connect and 2 s aggregate settings. Operation acquisition/release is
1/1 with three polls and zero heads/sources; same-client recovery returns 200/`ok`. It proves the
phase-timeout branch, not aggregate expiry. The complete Linux suite remains 9 pass/11 fail,
exit 1; all timing/protocol failures are preserved. No new case, leg, selector or CI job is added.
Container/runtime cleanup completes. The source build identity is working-tree, not candidate.

All five root gates pass, including 2,666 tests across 33 binaries (one existing ignored test).
An initial sandboxed run fails local socket permission checks; the unchanged unrestricted run
passes, and both logs are retained. Strict harness lint and formatting pass. All 217 required-local
bindings remain executable with `qualified: false`; hosted execution, candidate freeze and
capability promotion remain outside this work. Cancellation and remaining strict timing proof
are still open; the diagnostic and direct-socket corrections do not waive those requirements.

### CI Follow-Up (2026-11-02)

GitHub checks at `f8f2dc56` are not green. Workspace fails the legacy-name guard, docs/static
checks fail a newly added link to an unpublished internal plan, and Axum's wire phase lacks its
second receipt for `pair-original-absent-declared-s`. WASM tests and adapter lint checks pass.
The earlier local reports below do not replace these hosted CI results.

The minimal repair preserves the guard and deadlines: rename new Rust batch variables to
`observation_cutoff`, link the plan's repository source, and make the one-response paired origin's
connection lifetime explicit. A deterministic raw-TCP regression fails before the latter repair;
the full Axum CI owner then passes. Production retries and pooling are unchanged. Exact-header
fixtures charge the additional closing field where visible; Spin's documented hidden Connection
field retains its original adapter-visible boundary. Marker-on must still exceed the exact cap,
marker-off must still fit, and byte limits charge every visible field. Case/receipt identities,
production limits and acceptance tolerances are unchanged. All four full wire suites now pass,
including all 97 paired Send/Begin vectors per adapter. Axum's complete CI owner also passes its
native tests and fixture check. These runs are local working-tree evidence, not new hosted CI
results or frozen-candidate qualification.

The pending-source cancellation failures also have a diagnostic confound: the survivor's original
one-second `ack` request times out before the controller's two-second coordination bound. Its
terminal record ends ordered observation early. This explains the parser error, not the victim's
missing cancellation proof. Retain both original failures; they are not complete two-second
observations or evidence that the providers cannot expose cancellation. Preserve the survivor
budget and all protocol validation. The scoped Cloudflare repair now observes one cached
writer-closed promise during source waits, retaining abort/deadline precedence and byte accounting.
Its regression first reports DeadlineExceeded instead of TransportError, then passes with 111 WASM
SDK and 43 host tests, strict WASM Clippy and independent ownership review. The FixedLengthStream
test explicitly uses a guest shim; raw HTTP and hosted propagation stay open.

The unpolled-batch probe additionally separates its actual paired 300 ms timer from completion of
the unchanged auxiliary HTTP Send. The previous whole-call measurements include transport and
resumption overhead. The timer keeps its 300-305 ms interval; the Send keeps its original request
budget, source receipts and causal clock-calibration role. No batch observation occurs until both
complete. Other injected-client probes remain unchanged, and the native client, timer and provider
progress come from one existing private factory, without another public accessor or executor.

Fresh verification passes all five root gates (2,666 tests across 33 binaries, zero failures and
one existing ignored test), 576 serialized harness tests (two existing ignored helpers), 84
qualification-fixture tests, strict harness/fixture Clippy, all three workspace format checks,
131 evidence-checker tests, 34 workflow guards, the legacy-name guard and docs build/lint/format.
Independent review covers source ownership, header-cap calibration and completion ordering; the
HTTP-before-timer false-negative is fixed with mutation tests for both chronological orders.
All four runtime quiet-window cases pass without relaxing their original 300-305 ms timer bound
or replacing the unrelated HTTP Send. The new local captures are retained under
`/private/tmp/pr275-ci-repair-{adapter}-{wire,lifetimes}-final/`; Axum's full CI capture is
`/private/tmp/pr275-ci-repair-axum-ci-final/`. Earlier failures remain retained unchanged.

| Lifecycle Run    | Result                                | Remaining Observation                                                                                                                                                                       |
| ---------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Axum             | 19 pass, exit 0                       | All cleanup and timing cases pass in this run.                                                                                                                                              |
| Repaired Viceroy | 33 pass, 1 recorded row, exit 0       | Budget, callback, cleanup and protocol-tail checks pass. The recorded batch-write row is not another passing runtime case.                                                                  |
| Spin             | 18 pass, 1 fail, 1 unverified, exit 1 | Pending-source client cancellation lacks the required terminal/source proof; silent-connect qualification requires Linux.                                                                   |
| Workers          | 14 pass, 5 fail, exit 1               | Cold Begin is 10 ms against 2 +/- 5 ms; cutoff returns are 206/228 ms against 205 ms; batch entry is 40 ms against 34-39 ms. Pending-source cancellation still lacks terminal/source proof. |

The cached writer-closed repair closes the SDK-level observation gap, not the remaining HTTP
propagation proof. Both pending-source cancellation runs still fail the original protocol and
retain their tail failures; the survivor timeout confound above remains relevant. All observed
native operations and bodies in the Workers cutoff cases release once before collection, but
late release/return samples still fail their unchanged timing checks. No provider impossibility,
capability promotion, deployment, candidate freeze or release qualification is inferred. All 217
required-local bindings remain executable with `qualified: false`; external and conditional
evidence stays open. New GitHub checks must be evaluated at the pushed repair commit.

### Required Local Acceptance Closeout (2026-11-02)

The requester approved completing required local acceptance coverage and inventory reconciliation
without another checkpoint handoff. Hosted execution, candidate freeze and release qualification
remain outside this authorization. Preserve all outbound contracts, reviewed identities and
original tolerances; do not convert unknown scope or failed measurements into approved deferrals.

Use `scripts/test_integration.sh acceptance --coverage` for current binding counts and
scope. Counts in earlier checkpoints below describe those historical checkpoints, not the current
inventory. Ready bindings are implementation coverage; passing execution and frozen qualification
are separate requirements. The first reviewed closeout update activates twelve additional local
bindings without changing acceptance identities, approvals or any external/conditional leg.

The latest inventory binds **all 217 required-local legs** (246 ready/265 planned overall).
AG approved the [scoped TLS baseline clarification](../../audits/2026-10-18-outbound-api-approval.md#scoped-tls-baseline-clarification)
on 2026-11-02: test every currently configurable identity dimension and fixed strict-TLS native
readback; independently configurable verification remains the unpromoted EZ-12 follow-up. This
changes only the approved wording and its affected digests, not the 255 criteria, 511 leg identities,
applicability or qualification requirements. All 44 required-external, 50 conditional and 200
unpromoted follow-up legs retain their original category. No other approval or deferral is inferred.
The final 217-leg checker passes binding completeness, always reporting `qualified: false`.

The current implementation adds actual Fastly known-first SDK dispatch, all-adapter native
Send/Begin parity, response-owned operation cleanup, and both callback-wrapper runtime proofs.
Unit callback evidence requires the five-test adapter ownership companion; its three fixture
counter tests run in the existing Fastly fixture-check phase. Those counter/model tests are not
runtime SDK-release or host-cancellation evidence. Spin's native phase now executes its existing
response-coordinator models. No new job, selector, transport, executor or production public API is
introduced; native instrumentation remains test-only.

The reviewed private batch fixture now obtains one actual native client from the existing factory
and wraps that same handle in the facade, retaining its paired time/progress under the response
owner. No public batch-context accessor is added. It requires genuine pending SDK evidence and
completed buffering before cancellation; those observations cannot substitute for one another.

One isolated Linux Spin run with the immutable guest
`20402794e0799eff2fd3f20f301b4cb91147c6a5d35b44e749f152fca93a8bdc` and official Spin 4.1.0
returns `204 -> 504 ConnectTimeout -> 200 ok`: 202.600 ms with unchanged 200 ms/2 s settings.
Owned PID/inode SYN_SENT observations are bounded by causal start/recovery requests, not a guessed
clock alignment. The aggregate-timeout branch is not exercised. The container/process/socket
cleanup record and raw samples are retained under
`/private/tmp/ez4-syn-sidecar-20261102/final-three-step/`; this is characterization, not frozen
candidate or hosted proof. The canonical Linux qualification owner is registered independently.

Fresh root gates pass formatting, strict all-feature Clippy, both feature/Spin WASM checks and
2,665 workspace tests across 33 binaries (zero failures, one existing ignored test). Existing Node
workflow guards pass 34 tests; nonzero/legacy/egress guards pass 102. Passing complete local runtime
qualification remains open; retain failed timing controls rather than retrying for credit.

Final serialized harness verification passes 573 tests with zero failures and two existing ignored
helpers; strict harness Clippy and all three workspace-format checks pass. Fixture-core passes 82,
the selected Viceroy executes all 309 Fastly SDK tests and 13 fixture tests, and the Node evidence
and workflow guards pass 131 and 34 tests respectively. Docs build/lint/format pass. Safe native
Unix PID/group signalling replaces the helper subprocess; injected probe errors are retained until
cleanup/reaping is attempted, and only ESRCH establishes absence. The original helper-timeout
failures remain recorded; their exact startup-delay cause is unknown. Four additional fail-first
controls prove failed Fastly wire/protocol validation releases held sources before event collection
without advancing the protocol cursor or replacing the original error.

The final frozen-source working-tree runs are retained under
`crates/edgezero-test-harness/fixtures/reusable-app/.runs/outbound/lifetimes/*-final-v4/`.
All four commands exit 1; no failed window or missing observation was waived or rerun for credit.
These are not frozen-candidate or hosted qualification:

| Run                | Recorded Result                 | Remaining Observation                                                                                                                                                                                                                    |
| ------------------ | ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `axum-final-v4`    | 17 pass, 2 fail                 | Fast-origin head is 126.620 ms against 120 +/- 5 ms; the quiet-window interval also fails. All five response-cleanup cases and repaired batch cancellation pass.                                                                         |
| `viceroy-final-v4` | 30 pass, 3 fail, 1 recorded row | Three original timing checks fail. All six budget cases, eight callback cases, response-cleanup cases and repaired batch cancellation pass; all log tails pass. The recorded batch-write row is not an additional passing runtime case.  |
| `spin-final-v4`    | 16 pass, 3 fail, 1 unverified   | Cold Begin is 11.218 ms against 2 +/- 5 ms; pending-body cutoff returns at 207.865 ms against the 205 ms upper target. Pending-source client cancellation lacks terminal/source proof; Linux silent-connect remains unverified on macOS. |
| `workerd-final-v4` | 14 pass, 5 fail                 | Four original timing checks fail. Pending-source client cancellation lacks terminal/source proof; complete, disconnect, deadline and source-error cleanup pass.                                                                          |

No run records process/origin/state teardown failure. The actual worker/native-source records
remain mandatory: missing pending-source cancellation evidence is not inferred from a client close,
substituted with model coverage, or relabelled a pass. No provider impossibility is claimed.
All 217 local bindings remain executable and `qualified: false`; neither these failures nor a
passing counter/model test changes applicability or approves external/conditional scope.

Independent review confirms Spin's original cutoff stays 200 ms and pending SDK bodies release
once before the 207.865 ms return. This is not evidence of deadline renewal; the observation alone
cannot distinguish timer, scheduling and cancellation overhead. The cancellation probes reach a
validated prefix, actual post-prefix source Pending and intentional client socket close, with no
further source writes. Spin then watches the response-result future and Workers the request abort
signal. Neither run supplies the required combined terminal/cleanup/source-drop proof within the
shared two-second window. Missing that combined record does not prove every callback was absent
or that either provider cannot expose cancellation. Original 30-second cancel-case write budgets
and the separate explicit-deadline cases remain unchanged.

The earlier expanded runtime observations remain retained under
`crates/edgezero-test-harness/fixtures/reusable-app/.runs/outbound/lifetimes/`. These are working-tree
observations from before the final fixture/controller repairs, not frozen-candidate evidence:

| Run                     | Recorded Result               | Remaining Observation                                                                                                                                                              |
| ----------------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `axum-after-ledger-fix` | 19/19 pass                    | Earlier source-scheduling failure remains retained in `axum`; no tolerance was changed.                                                                                            |
| `workerd`               | 15/19 pass                    | Cold Begin misses its existing interval; deadline/source-error cleanup has a clean wire EOF; cancellation is obscured by the stale cleanup cursor.                                 |
| `spin`                  | 17 pass, 2 fail, 1 unverified | Cancellation snapshots A before SDK EOF; pending-source client cancellation lacks a terminal report. Silent-connect proof requires Linux.                                          |
| `viceroy`               | Failing expanded matrix       | Pending native head handles need readiness-wait evidence rather than fabricated SDK polls; cleanup failures cascade into bounded-origin saturation and callback identity failures. |

Two SDK-only workerd controls retain the same clean HTTP ending for `writer.abort(undefined)`
and an explicit fixed `Error`, including one direct-worker TCP socket control. The original
`/private/tmp/edgezero-native-abort-control.LRzJqQ/` and
`/private/tmp/edgezero-native-direct-abort.VyvsQd/` captures include the runtime hashes, compatibility
settings, terminal chunk, curl exit and reaped-process evidence. They do not establish universal
provider impossibility or justify accepting wire cancellation as proven. No speculative production
abort change is made. Cleanup verdicts now collect SDK proof even if the wire verdict fails;
only complete validated protocol advances its cursor. The final runs above execute that repair;
the old failed observations remain unchanged.
The cleanup acceptance criterion requires observable ownership release, not a TCP reset. Workers'
exact prefix-only clean ending is therefore retained as its actual wire shape only for the
deadline/source-error paths, while their error terminal and all cleanup/isolation checks remain
mandatory. This supplies no cancellation proof or capability promotion. Prefix rejection cannot
stand in for an intentional abort. Partial stages are retained, and a single two-second absolute
coordination deadline bounds captures, channel waits and native protocol scans without changing
guest deadlines, source limits or qualification intervals.

Current fixture repairs retain the original 34/534 ms batch defaults and all source, protocol,
resource and timing bounds. A completed server write is not an SDK-completed buffer. An actual
native head-readiness timeout is not an SDK future poll or a body-readiness wait. Counter/model
tests, native resource release, wire termination and host cancellation remain distinct evidence.

**Earlier closeout checkpoints:**

The subsequent reviewed binding update adds 39 existing lifetime legs and four native Fastly
transport legs. Required-local coverage is now 179 ready and 38 planned; all 44 external,
50 conditional and 200 unpromoted follow-up legs retain their scope. Runtime acceptance markers
require every scenario named by a criterion, including both pending phases for cutoff and every
observable response-cleanup path. Three fail-first marker controls and all 66 lifetime validator
tests pass. The latter's first sandboxed run retains one denied TCP bind; the authorized local
TCP run passes unchanged. Strict workspace and harness Clippy, formatting and frozen identity
reproduction pass at this working-tree checkpoint. No hosted or release qualification is implied.

The guarded Axum group passes 291 library and 28 contract tests; core passes 1,125 tests and
the private buffered-send extraction passes all 304 Fastly WASI library tests. The native
workerd and Spin wire runs retain their original 68/84 matrices and pass 19 native transport
controls with 30 receipts each. These are working-tree observations, not frozen-candidate proof.
The broader harness run retains four process-signal-helper failures (473 passing, two ignored);
the isolated command-capture and five cancellation controls then pass without timing changes.
The cause of the broad-run helper timeouts is not established. A subsequent workerd lifetime
attempt correctly rejects build identity drift while source writers are active: its expected
`2859b448...` identity is absent from the staged WASM, which embeds `9ce7092e...` instead.
It supplies no acceptance credit. Final runtime runs require paused source/metadata writers.

- [x] Partition the initial 511 legs: 217 blocking-local (124 ready, 93 planned), 44 required
      external/provider/consumer legs, 50 unconfirmed conditional legs, and 200 unpromoted
      follow-up legs (29 ready). No attributed applicability decision is newly inferred.
- [x] Add an inventory-derived coverage report to the existing checker, with fail-closed local
      binding completeness, explicit external/conditional/follow-up categories and qualification
      always false. Add approval, identity, CLI and per-leg partition controls.
- [x] Implement every missing blocking-local case using the existing unit/runtime owners, including
      SDK-boundary timeout mapping, B31 registration/budgets, runtime optimizer rejection/targets,
      Begin/Send parity and lifetime cancellation/cleanup. Preserve one transport and executor.
      This records executable coverage, not passing timing, host cancellation or qualification.
- [x] Bind all 217 blocking-local legs to actual commands, sentinels and retained artifact owners;
      remove placeholder bindings and preserve reviewed identities/scope. Record only the explicitly
      approved TLS baseline wording/digest clarification; execution and qualification remain separate.
- [x] Run and review the shared gates and expanded four-adapter lifetime matrix, retaining every
      failed control in the final working-tree observations above. Reconcile binding scope,
      explicit TLS approval and historical versus current observations without claiming a pass.
- [ ] Obtain complete passing local qualification and frozen same-candidate evidence, including
      the remaining measured timing, pending-source cancellation and Linux-only proof requirements.
      Source instrumentation, scheduled targets and SDK handle-release witnesses must not be
      substituted for one another or presented as hosted cancellation/client-delivery evidence.

Initial diagnostics are retained under `/private/tmp/pr275-closeout-*`: Axum passes 9/10 with
a 40.142 ms batch-entry miss; Spin passes 8/10 with a 13.095 ms early Begin miss and source
scheduling failure; configured native workerd passes 8/10 with cold Begin/handoff timing misses.
Earlier Cloudflare attempts stop for explicit missing/mismatched local tool selections, not
runtime acceptance. The matching installed tools are selected explicitly; no dependency pin or
timing tolerance is changed. New readback helper/unit regressions pass all 136 Viceroy library
tests. Actual SDK readback rejects the previous runtime's unsupported getter and passes on the
explicit repaired runtime, including distinct native field controls and the unchanged logical
budget. The [append-only record](../../audits/evidence/2026-11-02-viceroy-timeout-readback.json)
discloses that the earlier failed control's binary identity was not independently captured;
neither observation is frozen qualification. The reused
asynchronous Spin component passes its connect-refusal and preexisting eager/normalization guards
under pinned Wasmtime 48.0.2; the older PATH runtime's linker failure is retained separately.

### Fastly Post-Delivery Batch Coverage (2026-11-02)

Continue approved D6 criterion 19 in the existing Fastly lifetime binary and suite. No second
transport, executor, origin, public API or CI invocation is added. Two independent investigations
confirm that the earlier single Send after delivery and pre-delivery cleanup batches do not
cover this criterion. Both proposals favor a private case over expanding cleanup fault modes.

- [x] Complete a bounded buffered response through canonical FastlyService/run_custom before
      starting the same retained client's callback-owned three-slot batch. Use the actual
      egress report's request-start epoch, batch entry at 34 ms, absolute cutoff 534 ms and
      one-second per-call timeouts. Reuse the existing 20/40/60 ms receipt-relative source.
- [x] Wait until 450 ms through the existing paired-time/provider-backed scope, then collect
      the original batch. Snapshot three actual SDK body witnesses before collection, while
      their buffered outcomes are still retained. Require single releases, Completed, exact
      payloads and original elapsed values within 5 ms of release-relative arrival times.
      After callback return, verify scope closure and unchanged release counts.
- [x] Retain one bounded canonical record through the existing 2 KiB/1 MiB cleanup protocol,
      cursor and final-tail check. Consolidate dynamic/fixed validation in the same parser and
      wait loop; preserve old exact canonical assertions. Reproduce collection-stamped and
      coordinated early elapsed/release false acceptance as red tests, then reject both.
      Cross-bind each slot to its actual source-write delay without assuming synchronized epochs.
- [x] Pass all five root gates: 2,650 tests across 33 binaries with one existing ignored test,
      formatting, strict workspace Clippy, all-adapter feature check and Spin WASM compilation.
      The full serialized harness passes 454 tests with two existing ignored helpers. Fixture-core
      passes 61; strict harness/actual Fastly WASM fixture lint, docs lint/format/build,
      legacy/docs/inventory guards and frozen identity reproduction pass.
- [x] Run the expanded local inventory once after compilation: all 24 cases pass, including
      eight callback-wrapper cases and all log tails. New-case handoff finishes at 0.115 ms,
      the original batch enters at 35.198 ms and SDK body releases occur at
      56.330/76.150/96.181 ms. Collection at 451.266 ms retains arrival elapsed
      21.146/40.970/60.992 ms and completes at 451.302 ms with three exact payloads. Original
      cutoff stays 534 ms, callback closure is observed, and release counts remain one.
- [ ] Close remaining exact-candidate, runtime timing and hosted qualification. Executable
      coverage alone does not complete D6 or certify provider teardown/client delivery.

Specification and guest ownership reviews are clear after the causal timestamp-binding fix.
Historical strict timing failures remain recorded; this successful local run does not certify
other missing acceptance or hosted behavior. Inventory remains 255 criteria/511 legs, 153 ready/358 planned and
unqualified. No normative identity, SDK/runtime pin, capability or acceptance binding changes.
Demo/templates need no migration for this test-only case. Logs use
`/private/tmp/pr275-post-delivery-*`. GitHub created no Actions runs for the previous `f0ce2b38`
push despite eligible workflows and a mergeable PR; the triggering cause is not established.
Do not describe that head as green or merely running.
Initial development checks also retain the missing batch accessor and Deadline equality compile
errors, both corrected by inspecting the existing request-bound preparation witness; no API was
added. A broad sandboxed controller filter additionally hits an unrelated TCP-permission failure;
the correctly scoped negative tests and final authorized TCP/full harness runs are recorded
separately. The coordinated timestamp false-acceptance test fails before its causal guard and
passes afterward. No tolerance, resource cap or cancellation bound is weakened.

### Fastly Trickling-Slot And Live-Send Coverage (2026-11-02)

Continue approved D6 criteria 7/8 in the same fixture, bounded origin and runtime inventory.
The prior `eb8ac5df` head has all 30 reported checks passing; its strict timing failures remain
recorded and are not waived by CI success or these new executables.

- [x] Add Fastly-only `trickling-slot`: retain origin Begin at 2 ms, batch at 34 ms and cutoff
      534 ms. B sends its head at 100 ms and body at 600 ms; A/C retain 180/900 ms. The original
      origin streams one byte every 50 ms from 100 through 600 ms.
- [x] Join consumption of that original origin body with the original batch's bounded `next`
      loop. Record every read/Next return immediately in one ordered, request-local log, never
      sorted or stamped after join. Retain actual byte-receipt times, explicit EOF and the
      terminal Next's original 534..539 ms bound with B/C unresolved. Require intermediate
      receipts inside that terminal wait and continued origin progress afterward.
- [x] Bound observations independently from SDK chunk boundaries: at most 64 completed reads,
      64 copied payload bytes and four Next returns, for 68 records. Empty reads spend budget
      without progressing the prefix. Split/coalesced reads use their actual receipt time for
      each new byte, not interpolated or backdated times. Preserve source EOF/framing and the
      existing 32-event/eight-active/128-cumulative origin limits.
- [x] Add Fastly-only `send-during-batch`: start the original batch first, then Send the original
      streamed origin without Begin. Preserve source head at 700 ms, chunks at 700/900/1300 ms,
      checks at 701 ms, A/B elapsed 146/286 ms and C unresolved at Cutoff. Correlate request
      preparation and the `origin-send` operation with that already-live batch.
- [x] Demonstrate fail-first fixture/origin/controller tests. Two independent reviews identify
      missing-result-as-EOF and a late terminal-wait false acceptance; reproduce both as red
      tests, then require explicit result presence and actual intermediate-read/wait overlap.
      Scoped controls pass, including bounded empty/oversize reads and B's late peer-close path.
- [x] Pass all five root gates: 2,650 tests across 33 binaries with one existing ignored test,
      formatting, strict workspace Clippy, all-adapter feature compilation and Spin WASM check.
      The final serialized harness passes 449 tests with two existing ignored tests; fixture-core
      passes 61 with/without native witnesses. Strict harness, fixture and actual Fastly WASM
      fixture lint, docs lint/format/build, legacy/inventory guards and frozen identities pass.
      Independent specification and code-quality reviews are clear after the two guard fixes.
- [x] Run the complete expanded 23-case local Fastly inventory once after compilation. Both
      new cases pass. Trickle's terminal Next spans 185.055..534.902 ms; application receipts
      continue at 201.847..501.724 ms inside it and at 551.722/601.538 ms afterward, with EOF
      at 601.573 ms. A's elapsed is 150.059 ms, B/C remain unresolved, and cutoff is unchanged.
      Send-after-batch recovers A/B at 146.516/286.371 ms after observing the origin head at
      704.132 ms, while C remains unresolved.
- [ ] Close full timing and qualification acceptance. The inventory passes 21 of 23 and exits 1:
      slow-origin EOF is 1306.260 ms against 1300 ms +/- 5 ms; the fourth-slot case's B elapsed
      is 294.511 ms against 286 ms +/- 5 ms. All eight callback cases, three cleanup cases and
      log tails pass. New-case successes do not waive these failures, historical failures or
      hosted, exact-candidate and other remaining local acceptance.

No production API, transport, scheduler, SDK/runtime pin, timing tolerance, capability, CI
invocation, normative identity or acceptance binding changes. Inventory remains 153 ready/358
planned and unqualified. Demo/templates need no migration for test-only cases. Logs use
`/private/tmp/pr275-live-batch-*`; the two initial controller compile errors and duplicate-check
lookup failure are retained separately from successful tests, without weakened assertions.
The initial full harness run during concurrent builds passes 446, fails two unchanged process
tests and ignores two: direct-TERM cancellation is not observed within its existing bound, and
the nested-cancellation signal helper times out. The unchanged final serialized run passes all
449 with two ignored. Preserve both logs; no process timeout or cancellation assertion changes.
The legacy guard initially rejects the local name `cutoff`; use `observation_returned_at` for
the measured return timestamp without changing any deadline or allowing a compatibility name.

### Fastly Body-Read Arrival Acceptance (2026-11-02)

Continue the approved D6 baseline with EZ-6 criterion 6 in the existing local lifetime suite,
not another transport, scheduler, fixture binary or CI invocation. Prior checkpoint `5950548d`
has 30 passing reported checks; those checks do not waive its retained strict timing failures.

- [x] Add one Fastly-only `body-read-arrivals` case. Keep origin Begin at 2 ms, batch start at
      34 ms, cutoff at 534 ms and A/B/C at 180/320/900 ms. The original streamed origin supplies
      its head and first byte at 100 ms, then its second byte and terminal framing at 800 ms.
- [x] Read only that original body between head and EOF. After EOF, nonblocking checks recover
      A/B at 146/286 ms and the original batch collects as Cutoff with C unresolved. Require
      a streamed body, exact bytes, original cutoff, ordered nonoverlapping operations, source
      schedules and clean terminal framing. Do not infer arrival from late collection.
- [x] Demonstrate missing-case red tests in the origin, controller and guest fixture before
      implementation. Add negative controls for missing/late results, polling during body reads,
      changed cutoffs or bytes, nonstreamed bodies and absent EOF evidence.
- [x] Finish all five root gates: 2,650 tests across 33 binaries with one existing ignored test,
      formatting, strict Clippy, all-adapter feature compilation and Spin WASM compilation.
      The full harness passes 445 tests with two existing ignored tests; fixture-core passes
      59 with and without native witnesses. Strict harness, fixture and actual Fastly WASM
      fixture lint, docs lint/format/build, legacy/inventory guards and frozen identities pass.
      Independent specification and quality reviews are clear.
- [x] Execute the complete local Fastly inventory once after compilation. It passes 19 of 21
      and exits 1. All eight callback cases, three cleanup cases and their log tails pass.
      The new case recovers A/B at 148.972/288.603 ms after origin EOF at 802.816 ms, but its
      head is observed at 110.546 ms and body reading starts at 110.549 ms against 100 ms
      +/- 5 ms. The slow-origin case also fails A's elapsed time at 151.378 ms against
      146 ms +/- 5 ms. Neither the recovered results nor earlier passes override these failures.
- [ ] Close criterion 6's complete timing acceptance and the remaining exact-candidate/hosted
      qualification gates. Scenarios 7/8 and other missing local acceptance remain open.

The previous three timing misses remain failures. Read-only comparison finds no proven production
engine defect: the origin writes complete near schedule, guest observation is later, and CPU batch
entry is already 6.513 ms late before the measured 0.400 ms dispatch call. Unproven clock alignment
and absent per-turn engine timings prevent attributing those gaps to provider or adapter latency.
Do not backdate observations, add retries or widen the original tolerances. Logs use
`/private/tmp/pr275-body-read-arrivals-*`. No production API, policy, SDK/runtime pin, capability,
normative identity or acceptance binding changes; inventory stays 153 ready/358 planned,
unqualified. Demo/templates require no migration for this test-only case.
An accidentally unfiltered, sandboxed nonzero-guard command runs the full harness and fails
120 socket/process checks. Retain that log; the correctly scoped sentinel and socket-enabled,
serialized full suite pass without changed assertions. An initial byte-array inference error
is corrected with an explicit slice, not a lint suppression or behavioral workaround.

### Fastly Native Body-Readiness Witness (2026-11-02)

Complete the distinct Fastly observation permitted by EZ-6 criterion 16 and the existing D6 note.
This supersedes the earlier missing-wait-witness statements below, not failed evidence or hosted
qualification gates. Do not relabel native wait entry/timeout as an SDK read returning Pending.

- [x] Keep one request-owned witness and add finite, typed native-wait observations: first wait,
      first/last successful timeout, and saturating selected/error counts. Use only the prepared
      clock; retain first observations and sticky backwards-clock invalidation. Hold no resource,
      source, engine or waker in snapshots or a lock across native work.
- [x] Share a test-only generic resource owner with the existing stream observer. Its actual SDK
      resource drops before the private release guard. Fastly wraps the `BodyHandle` itself, so
      expiry, EOF/error or lease removal records after the handle drops, not at a later outer
      stream drop. Unattached stream observers still return the original stream unchanged.
- [x] Derive recorders only from active body owners supplying the actual submitted handle slice.
      Observe the one existing select invocation and preserve its exact result. Use submitted
      indices to distinguish this body selected, another handle selected, timeout and failure.
      Add no SDK `is_ready` call, poll, timer, wakeup, transport or production-policy change.
- [x] Require the unchanged original policies and full URI plus a positively observed timeout:
      `head <= wait-entry < 200 ms <= timeout-return <= next-return <= 205 ms`. Require SDK-owner
      destruction after that timeout and before return. Keep SDK-read fields separate, the
      zero-head/read/wait control and independent origin hold/release assertions. Missing, stale,
      unrelated, selected/failed, backwards, Ready-first, early/duplicate or unreleased evidence
      cannot substitute. The label is `sdk-readiness-wait`, never `sdk-read-pending`.
- [x] Demonstrate core and harness red/green proofs. The real pre-hook Viceroy run fails only for
      the pending-body case's missing measured head. The first instrumented run passes its whole
      20-case lifetime/callback inventory, including both wrapper families. Actual body waits
      start at 35.513 ms, return timeout at 201.069-201.071 ms, release each owner once at
      201.106-201.109 ms and return from `next` at 201.238 ms. Mixed head/body selection records
      `OtherReady` for bodies not selected. Retain the failed pre-hook run.
- [x] Pass 1,116 core tests including 13 owner/recorder tests, 176 native Fastly tests, 293 actual
      Fastly WASI library tests and 36 SDK contracts. Strict core, Fastly WASM with/without
      `test-utils`, harness and fixture lint pass. The 59 qualification/witness fixture tests
      and Axum's 28 transport contracts pass. Independent spec/ownership and quality reviews
      are clear.
- [x] Verify the shared owner on actual Axum, Spin and workerd. Their unchanged ten-case lifetime
      suites all pass on the new working tree. Preserve all earlier cold/timing failures; a
      later local pass is not a waiver or a frozen-candidate/hosted result.
- [x] Reject inconsistent first/last wait records, overlapping native calls and fewer recorded
      calls than the distinct observations require. Self-review red/green controls cover each;
      the final harness passes 443 tests with two existing ignored tests. All five root gates
      pass, including 2,650 tests across 33 binaries with one existing ignored test. Docs
      lint/format/build, legacy/docs/inventory guards and historical/scoped API identities pass.
- [x] Execute the final reviewed runtime inventory and retain its failures. The pending-body
      witness and all eight callback cases pass; the whole inventory passes 17 of 20, exits 1
      and remains open. Slow/fast origin heads miss their unchanged targets at 705.897 ms
      versus 700 ms +/- 5 ms and 125.273 ms versus 120 ms +/- 5 ms; CPU batch start is
      40.513 ms versus 34 ms +/- 5 ms. Earlier instrumented and intermediate runs pass all
      20 cases, but neither overrides this latest result or supplies candidate qualification.
- [ ] Complete the exact-candidate timing and qualification gates; do not widen tolerances,
      hide failed runs or promote a partial case to whole-suite acceptance.

The scope remains `test-utils`; no production API, deadline, B31 policy, decoding/cap rule,
SDK/runtime pin, CI invocation, normative identity or acceptance binding changes. SDK-handle
destruction is not proof of provider teardown. Local Fastly execution explicitly uses the tracked
Viceroy readiness repair, not a stock-runtime or hosted claim. Inventory stays 153 ready/358
planned and unqualified. Candidate freeze, owner-bound acceptance artifacts, consumer and hosted
qualification, remaining applicable lifecycle scenarios and provider limitations remain open.
Logs use `/private/tmp/pr275-fastly-wait-*`. Initial lint rejects unnamed generic parameters
and one ignored-unit pattern; use named parameters and an explicit unit pattern, without
suppressions or behavioral changes. No demo/template migration is needed for test-only metadata.

### Spin Native Pending-Body Witness (2026-11-01)

Extend the existing request-owned witness to Spin without new core APIs, report schemas,
observer types, schedulers, SDK calls, timers, wakeups or production policy changes. Read-only Spin
and Fastly investigations independently recommend this scope: Spin can use the existing proof;
Fastly needs a distinctly labelled native-wait witness, not an inferred SDK-read Pending.

- [x] Forward Spin's `test-utils` feature to core and enable the existing fixture witness feature.
      Capture the recorder from the prepared context, mark the head only after successful shared
      construction, observe the raw borrowed WASI read future inside the unchanged deadline race,
      and track destruction of the complete SDK-owning stream before cooperative wrapping.
- [x] Require Spin's three witnesses in the existing pending-head/body cases and extend all
      existing negative controls: missing/reused/Ready-first/late/backwards evidence, foreign
      origin, early release and missing/duplicate ownership. Preserve the 34/200/205 ms bounds,
      prepared metadata, original clock and zero-head/read control. Add no CI invocation.
- [x] Demonstrate the harness red/green failure for accepting only host heads. The real local
      Spin run before instrumentation fails the pending-body case with missing `head_at_us`;
      its prepared witnesses correctly report zero heads, reads and owners. Retain that failure.
- [x] Run actual Spin 4.1.0 after instrumentation. The complete pending-body case passes in both
      runs, with no fake transport or extra request. The final code records first Pending at
      38.238-38.321 ms and exactly one source release per slot at 203.806-203.845 ms, before
      `next` returns. The pending-head control remains head/read/owner-free.
- [ ] Complete every lifetime timing assertion. The intermediate suite passes eight of ten;
      the final suite passes nine of ten, retaining the cold first-case arrival mismatch:
      163.147 ms versus 146 ms +/- 5 ms. Neither partial result closes the full suite or scenario.
- [x] Finish final native/WASI lint, owning tests, root gates, docs and independent review before
      committing this checkpoint.

Guest source destruction does not certify completed host teardown: the SDK completion-writer
drop can initiate its existing background notification. Do not claim otherwise. No Fastly
instrumentation is added: its timed native select returning at cutoff cannot be backdated to
a pre-cutoff Pending return, and outer progress Pending is not an SDK-read observation. Keep
the separately labelled native-wait proof open without changing the requirement or adding a probe.

Local records use `/private/tmp/pr275-spin-witness-*`, including `runtime-red`, `runtime-green`
and `runtime-final`. Initial WASM lint rejects two shadowed bindings; rename them without
suppressions and check both `test-utils` and production-only WASM builds. A PATH-selected
Wasmtime 45.0.0 SDK run fails the required 0.3.2 ABI; use the verified existing 48.0.2 pin for
acceptance and retain the mismatch as failed setup evidence, not a provider failure. No runtime
pin, capability, acceptance binding or normative digest changes. Inventory stays 153 ready/358
planned and unqualified; Cloudflare's complete timing gate, Fastly native-wait evidence,
candidate freeze and hosted qualification remain open.

All five root gates pass: 2,645 tests across 33 binaries, one existing ignored, root formatting,
strict workspace Clippy, all-adapter feature compilation and Spin WASM compilation. Spin's native
suite passes 114 tests; its actual SDK contract/resource binaries pass 55 with verified Wasmtime
48.0.2. Strict WASM Clippy passes with and without `test-utils`; harness Clippy, format, docs
lint/format/build, legacy/docs/inventory guards and historical/scoped API identities pass.
Independent specification/ownership and code-quality reviews are clear.

The complete harness initially reports 440 passed, two failed and two ignored: the unchanged
process-group signal-helper timeout and three-second nested-cancellation bound fail during
concurrent builds. After the broad builds, its unchanged serialized rerun passes 442 with two
existing ignored tests in 157.55 seconds. Retain both logs, without extending timeouts, filtering
tests or relabelling the failed run. No demo/template or other-adapter edit is required for this
test-only integration; no compatibility path or extra CI job is added.

### Cloudflare Native Module Manifest (2026-11-01)

Repair the existing local launcher for the selected Wrangler 4.131.0 / Miniflare
5.20260910.0-alpha / workerd 2026-09-10 combination. This supersedes the unresolved constructor
boundary below, not its failed observations or the separate lifetime/hosted qualification gates.
No production adapter, interface, SDK/runtime pin, CI invocation or acceptance binding changes.

- [x] Enumerate staged files in Rust with a mandatory sorted inventory: at most 128 total
      directory/file entries, 64 MiB aggregate regular-file bytes and eight directory levels
      below the root. Reject empty/missing trees, symlinks, non-files and non-UTF-8 paths before
      passing paths, never module contents, in the existing launcher arguments.
- [x] Apply the selected SDK's rule compiler and matcher to that inventory, preserve rule
      precedence and put the verified ESModule main first. Convert explicit modules through the
      SDK's official converter and construct one native runtime. Delete the lossy legacy
      constructor shape; add no version fallback or parallel filesystem walker.
- [x] Preserve exact KV namespace IDs and use `resourcePersistencePath = persist/v3`, matching
      Wrangler's local seed layout. Preserve confined startup, private diagnostics, attestation,
      signal races and exactly-once memoized disposal. The inventory assumes owned staged files,
      not hostile concurrent filesystem mutation or a provider allocation guarantee.
- [x] Demonstrate red/green inventory and launcher tests, then pass all 442 harness tests with
      two existing ignored tests, strict harness Clippy, formatting and independent spec/security
      and quality reviews. Local smoke passes both assembly modes, seeded config visibility,
      declared-KV assertions and the existing outbound-progress sequence.
- [x] Run a separate bounded, local restart diagnostic through the unchanged launcher and
      existing linked-KV routes. Seed config once; write/read namespace `...02`, terminate,
      start a fresh process and read before another write. Both requested process terminations
      complete within five seconds. This is working-tree characterization, not a new CI suite,
      config-persistence-without-reseed qualification or hosted lifecycle proof.
- [ ] Pass the complete unchanged Cloudflare lifetime suite. Both local attempts run all ten
      cases; nine pass, including native SDK Pending/release witnesses, and the cold first case
      fails. First: begin at 8 ms versus 2 ms +/- 5 ms. Second: slot elapsed 153 ms versus
      146 ms +/- 5 ms. Retain both failures without retries that manufacture qualification,
      wider tolerances, partial-case acceptance or an inventory promotion.

The first pending-body case observes all three SDK reads Pending at 36 ms and exactly one source
release per slot at 202-203 ms, before `next` returns, retaining the original 200 ms cutoff.
The second attempt also passes that complete case. These results do not override either
whole-suite failure. Logs and raw records are under `/private/tmp/pr275-cf-manifest-*`, including
`smoke`, `lifetimes`, `lifetimes-recheck` and `persistence-recheck`; the initial ad hoc persistence
probe's incorrect fixture route/exit assertion is retained separately and receives no credit.
Compatibility remains `2026-04-22` with `enable_request_signal`. Inventory stays 153 ready/358
planned and unqualified; Spin/Fastly native-read witnesses, candidate freeze and hosted evidence
remain open.

The existing consolidated dependency/forwarding/focus compiler checks, 200 Node tooling tests,
legacy/docs/inventory guards, historical API identities and documentation format/lint/build pass.
Root Rust sources and CI configuration are unchanged; the prior root-gate counts below are not
relabelled as fresh execution for this fixture-only checkpoint.

### Native Pending-Body Witnesses: Axum And Cloudflare (2026-11-01)

Strengthen the existing pending-cutoff fixture without changing production outbound contracts,
transport, scheduling, clocks, deadlines or byte limits. This checkpoint instruments only
`test-utils` builds and attaches one explicit, finite witness to each original request. Prepared
target, mode, budget and entry instant come from the actual shared dispatch context; each recorder
uses that context's clock, not an ambient probe or a new time source.

- [x] Preserve witness identity through request parts and replay. Reject reuse and backwards
      samples; do not retain a request context, body bytes, source or waker in the report handle.
- [x] Record a head only after successful native-head construction. Observe Axum's actual
      `response.chunk()` future and Cloudflare's SDK ByteStream after the inner poll, before
      cooperative/deadline wrappers can manufacture Pending. Add no polls, timers or wakeups.
- [x] Record ownership release time only after dropping the actual inner SDK source. Require
      cutoff <= release <= return. Snapshot all
      three witnesses immediately after `next()` returns, before `collect()` can release work.
- [x] Require exact role/token/full-URI (including the independent controller-selected origin)
      and prepared-policy bindings for Axum and Cloudflare;
      reject absent, reused, Ready-first, late, backwards, incomplete, early-released or unreleased witnesses.
      Retain the zero-head/zero-read pending-head control and every existing host hold/release
      assertion. Preserve the 34 ms start, 200 ms cutoff and 205 ms return ceiling.
- [x] Finish feature/target checks, actual local Axum execution, strict lint, complete owning
      suites, documentation and independent spec/quality reviews.
- [ ] Complete actual local workerd lifetime execution after the selected SDK's Miniflare 5
      manifest/module boundary is implemented without losing module rules or KV persistence.

The real TCP Axum regression fails without the observation hook, then passes with native Pending
and source release before cutoff return. The harness regressions fail when host-written heads
alone, early release or foreign origins are accepted. Scoped spec review found the latter two
proof gaps; retaining a release timestamp and the full prepared URI closes them. Subsequent
spec and independent quality reviews are clear.

All five root Rust gates pass: 2,645 tests across 33 binaries, with one existing ignored test;
root formatting, strict Clippy, all-adapter feature checks and Spin WASM compilation pass. Core
passes 1,111 tests, Cloudflare's real SDK WASM library passes 105, Axum's transport contracts pass
28, and the complete harness passes 438 with two existing ignored tests. Fixture core passes
59 with and without native witnesses. Tooling passes 200 tests; documentation format/lint/build
and historical/scoped API identity checks pass. Earlier sandbox-denied TCP/process tests and
two process-timing failures are retained; the complete serialized, loopback-enabled harness
passes without changing assertions. Logs use `/private/tmp/pr275-native-body-*`.

Axum's complete local lifetime suite passes all ten cases. Its three actual SDK reads return
Pending around 36.6 ms, retain their original 200 ms cutoff and release between 201.116 and
201.124 ms, before `next` returns. The pending-head control has no head, read or source owner.
Raw records remain under `/private/tmp/pr275-native-body-axum-run` and are working-tree
characterization, not frozen-candidate evidence.

Cloudflare's two local runtime attempts fail before responder attestation and receive no credit.
The installed Wrangler 4.131.0 maps an explicit `rootPath`; the existing launcher now accepts it
only when it equals the owned absolute config directory. Its existing mock regression proves
exact forwarding, omission/default behavior, and foreign/relative rejection before construction,
with fixed diagnostics. This repair has its own red/green proof and does not change SDK pins.
The selected Miniflare 5.20260910.0-alpha then rejects the old constructor shape. A separate local
diagnostic confirms its official converter refuses `modulesRules` rather than preserving them.
That SDK manifest/module migration remains open; module rules, persistence, and timed assertions
are not removed or weakened to manufacture a runtime pass. The failed runs and diagnostics remain
under `/private/tmp/pr275-native-body-cloudflare-*`.

Hosted CI initially stops at the legacy-API guard because the new Axum regression used the retired
short variable spelling. Rename it to the canonical `observation_cutoff` without changing behavior
or weakening the guard. Retain the local guard red/green logs and rerun the Axum contracts, strict
lint and formatting for this follow-up.

Spin's WASI read future and Fastly's native body-readiness wait still need their own exact-boundary
witnesses. Their current records remain `host-head-written-only`, never SDK-read evidence. No new
CI invocation, acceptance binding, normative digest, runtime pin or capability promotion is added.
Inventory stays 153 ready/358 planned and unqualified. Native witnesses do not waive strict timing,
complete scenario ownership, candidate freeze, consumer acceptance or hosted qualification.

### Fastly Post-Preparation Dispatch Acceptance (2026-11-01)

Bind only EZ-3/acceptance/01 in `unit/fastly` to the existing inventory-guarded WASM library
and `outbound/unit/fastly/raw.log`. This is a binding-only checkpoint: no production code,
test implementation, CI invocation, YAML step, dependency, runtime pin or requirement changes.

The primary proof uses GET and HEAD with the original 500 ms budget. Body settlement advances
the injected clock by 26 ms; the prepared request retains its original entry time, absolute
deadline, duration and timeout source. Production backend configuration succeeds, and the
production Send path calls SDK `send_async` before acquiring the observed pending lease.
Dropping the future releases that lease. This establishes SDK dispatch, not origin receipt,
successful response completion or a hosted timing guarantee.

Two mandatory controls in the same library log retain live-budget acceptance, actual expiry
at equality and backwards-clock rejection. The checker pins the real SDK proof, both controls,
exact command and raw path. Negative mutations reject helper-only substitution, missing controls,
an unowned command and a different artifact. No new test invocation is added.

- [x] Match the complete criterion to its existing SDK/clock proofs and bind only that leg.
- [x] Add a failing ownership regression, implement the exact proof-set guard and update counts.
- [x] Complete scoped reviews after the guarded Fastly library, tooling and formatting/docs
      checks, before committing and pushing.

Verification: the existing inventory-guarded Fastly WASM library passes all 293 tests
in the tracked repaired Viceroy, with no ignored or filtered cases. Tooling passes 195 tests,
including 64 evidence tests; documentation format/lint/build, manifest, root formatting and audit
identity checks and independent spec/quality reviews pass. Structural comparison confirms only
this leg's binding fields changed.
The missing-guard mutation fails before the checker change and passes afterward. Logs are
`/private/tmp/pr275-dispatch-binding-*`. No fresh full production-workspace or other-provider
rerun is claimed for this binding-only checkpoint, and no hosted qualification is supplied.

Inventory is 153 ready/358 planned, unqualified. All 255 criteria and 511 identities, normative
digests, applicability and approval records remain intact. The batch-registration cases,
configured-connect/provider readback, lifecycle timing, successful native optimizer redirects,
candidate freeze, consumer acceptance and hosted qualification remain open. Existing lifecycle
diagnostic passes do not override failed whole-case assertions or create a missing CI owner.

### Focus Compiler Acceptance (2026-11-01)

Bind only EZ-8/acceptance/01 in core to the existing grouped Rust workspace check,
`CARGO_NET_OFFLINE=true scripts/test_integration.sh check`. No CI job, step, YAML list,
production API, runtime policy or dependency is added. The other 33 core library bindings keep
their existing command, 90 sentinels and raw artifact; compiler proofs are not library coverage.

- [x] Require all three existing numeric sentinels through the unchanged nonzero guard.
- [x] Extract the three actual focus Rustdoc examples, rejecting missing, duplicate, changed,
      unclosed or weakened examples. Compile one legal-variant control and all three negative
      shapes with Rustdoc; discovery and passed identities must match without ignored tests.
- [x] Share the existing bounded compiler/artifact plumbing with the forwarding check, retaining
      its original regression suite. Treat compiler/process failure as failure, never as proof
      that a negative example was correctly rejected.
- [x] Persist both numeric and compiler output plus the seven contributing markers in
      `target/outbound/unit/core/image-optimizer.log` (under the configured target directory when
      overridden). Clear an earlier artifact before work; validate and read back the new log
      before emitting its aggregate sentinel. Candidate/build attribution remains the existing
      evidence-assembly responsibility, not a claim supplied by a local file.
- [x] Pin the ready inventory row to that command, artifact, aggregate and complete proof set;
      reject numeric-only, unrelated or incomplete ownership. Preserve all 255 criteria and 511
      identities, requirements/source digests, applicability and approval records.
- [x] Finish the owning suites, grouped check, strict lint, docs checks and independent review
      before committing and pushing this checkpoint.

Verification: the complete harness passes 434 tests with two existing ignored tests, and strict
all-target harness Clippy passes. The unchanged core inventory command passes 1,103 tests;
tooling passes 194 tests, including 63 evidence tests. The actual grouped workspace check passes,
retaining the three numeric and four compiler proofs. Documentation format/lint/build, root and
harness formatting, manifest, audit identities and scoped reviews pass. Red controls demonstrate
missing ownership, malformed token boundaries and stale evidence after an earlier aggregate
failure; real Rustdoc rejects an unexpectedly valid negative and a broken positive control.
The first full run exposed the old help assertion and denied existing TCP/process cleanup
operations; the corrected, permission-enabled rerun passes. Final logs and raw proof output are
`/private/tmp/pr275-focus-owner-*` and
the target artifact above. Production Rust/WASM suites are unchanged, not claimed as fresh reruns
by this harness-only checkpoint. No hosted or exact-candidate qualification is supplied.

Inventory is 152 ready/359 planned, still unqualified. This closes the local compiler-owner gap
recorded by the earlier typed-focus amendment; it does not retroactively change that approval or
close Fastly timeout readback, successful native optimizer redirects, candidate freeze, consumer
acceptance or hosted qualification.

### Typed Focus And CLI Lint Follow-Up (2026-10-31)

The requester approved the [typed-focus wording amendment](../../audits/2026-10-18-outbound-api-approval.md#scoped-typed-focus-invariant-amendment).
EZ-8.3 and acceptance 01 now distinguish numeric builder errors from invalid combinations that
the public enum makes unrepresentable. Added the symmetric y-only compile-fail example beside
the existing x-only and smart-plus-offset examples. No option type, builder, runtime behavior,
parser or compatibility API changes. Only the whole-document digest and acceptance 01's text/digest
change in the inventory; all identities, other criterion digests, applicability and approvals stay.

This closes the wording question, not automatic compiler-test ownership. EZ-8/01 remains planned:
the existing core `--lib` owner cannot certify Rustdoc execution. Local compiler and numeric
checks remain separate from a future complete CI binding. Inventory stays 151 ready/360 planned,
unqualified; no hosted or provider gate is closed by this amendment.

CI at `fc973719` passes formatting and workspace all-feature Clippy, then fails the existing
Fastly CLI-only Clippy step on capture-fixture field order. Reproduced that exact feature set,
ordered the private fields alphabetically, and retained every field and behavior without a suppression.
Verified the no-default-feature library path too; all-features validation must not mask a narrower build.

Verification: all five root gates pass, including 2,637 tests across 33 binaries with one existing
ignored test. The exact CLI-only Clippy command, CLI tests and no-default-feature library Clippy
pass. The core inventory owner passes 1,103 tests; Fastly's guarded WASM library passes 293;
all three focus Rustdoc compile-fail examples pass. Tooling passes 193 tests, including 62 evidence
tests. Documentation, manifest, formatting, historical/current audit identities and scoped reviews
pass. Logs are `/private/tmp/pr275-focus-*` and `/private/tmp/pr275-provider-settings-fastly-*`.
These local results do not close compiler CI ownership, hosted qualification or candidate freeze.

### Provider Boundary Acceptance (2026-10-31)

The requester approved the next local provider-boundary pass. Four more blocking legs are
executable: EZ-4/01 on Axum, Cloudflare and Spin, and EZ-8/03 on Fastly. Inventory is now
151 ready/360 planned, unqualified. Reuse the existing same-candidate `companion_case`
mechanism: core owns parts/Follow readback and prepared-budget carriage; adapters own native
mapping or their documented BestEffort acceptance. Missing or substituted core companions
fail manifest validation and missing/failed/foreign-candidate evidence receives no release credit.
No new CI job, phase, provider wrapper or production API is introduced.

- [x] Extend the actual Axum pooled-connection contract with both 1 ms and 1 s settings;
      retain its original-budget stalled-TLS controls.
- [x] Exercise the actual Cloudflare client at the existing fetch boundary. A successful
      positive setting reaches fetch; a stalled fetch stays pending beyond the connect setting,
      then a paired timer wakes at the unchanged governing deadline and aborts once. Cover both
      per-call and caller-deadline attribution without asserting independent connect enforcement.
- [x] Call Spin's production `request_options` and read actual host-resource getters. Observe
      nanoseconds, remaining aggregate head/body budgets, a longer-than-budget setting and a
      one-nanosecond setting, without replacing the mapping with an injected setter callback.
- [x] Extend the existing private Fastly capture fixture, retaining its default pending-capture
      behavior. The real facade prepares Send, Follow and the next Follow hop; each invokes
      production `native_options`, compared with independently built SDK values for every option.
      SDK serialization exists only in this test comparison. Retain the SDK mapping matrix and
      actual-client Begin/Batch preflight proof in the same binding, plus core's readback companion.
- [x] Complete full owning suites, workspace gates and scoped review before publishing.

**Fastly timeout readback remains open.** The exploratory production `configure()`/SDK-getter
probe reaches registration, then the SDK aborts because pinned Viceroy returns Unsupported for
the getters. Its legacy registration path also discards the connect-timeout value; successful
registration therefore cannot prove numeric retention. Preserve
`/private/tmp/pr275-provider-settings-fastly.log` and the diagnostic attempts. Remove the unusable
probe from the executable suite, not its failure evidence. Do not substitute helper values,
Debug-string parsing, SDK copies or another runtime patch; EZ-4/01 on Fastly stays planned.

The recording optimizer test proves prepared-hop mapping, not submitted host claims, transformed
output, successful native optimized redirects or performance. Existing native-unavailability
controls remain separate; hosted optimizer/timing qualification and candidate freeze stay open.
The later typed-focus amendment above governs the approved wording correction; it does not
activate EZ-8/01 or retroactively change this checkpoint's verification. Original deadlines, B31,
caps, direct native IO, SDK/runtime pins, capabilities, demo and generator contracts are unchanged.

Verification: all five root gates pass, including 2,637 tests across 33 binaries with one
existing ignored test. The unchanged core companion guard passes 1,103 tests; the harness
passes 428 with two existing ignored tests and strict Clippy. Fastly's complete guarded WASM
library passes 293 tests, Cloudflare's passes 104, and the complete Axum and pinned Spin CI
suites pass, including 35 Spin contract tests and 20 SDK-resource tests. Strict Clippy passes
on all three changed WASM paths. Tooling passes 192 tests, with 61 in the evidence validator.
Dependency/forwarding checks, docs contract, formatting, manifest and legacy guards pass.
Scoped reviews are clear after retaining Axum's original two 1 ms controls and 1-second total
deadline. Logs and complete runtime artifacts are `/private/tmp/pr275-provider-settings-*`.
These results qualify no frozen candidate or hosted provider; the two explicit local gaps above
remain open. The requirements SHA-256 at this checkpoint (`fc973719`) is
`a3d58bcabe7e9e556d21227de973c660794d25606b7ee3365bd7808d3d101c70`.

### Unit Settings Acceptance (2026-10-31)

The requester selected the low-cost unit-acceptance pass. Bind twelve existing blocking legs:
EZ-4 criterion 02 (zero connect timeout) and 04 (capability metadata) on all four adapters;
EZ-8 criterion 03 in core and on Axum, Cloudflare and Spin. Inventory is 147 ready/364 planned,
still unqualified. Keep all 511 identities, frozen source text/digests, approvals and applicability.

Use actual native clients for zero-connect checks, not another provider wrapper. Send, Begin
and Follow use counted pending GET sources; Batch uses its required buffered-empty shape.
Live and already-expired budgets must both produce the specific typed 400 before source polling
or timer creation, and pending sources release exactly once. A positive connect setting with an
expired request deadline remains attributed 504. Polling is bounded. Fastly additionally checks
zero backend configurations/registrations inside its existing callback context. The new helper's
stub fails before implementation; the existing Batch body-shape precedence is not changed.

Strengthen existing optimizer tests with request attachment and parts round-trip, then compare
every populated setting against the original on both recorded prepared Follow hops. Reuse the
non-Fastly adapters' existing four-mode unsupported-option proofs. All 89 prior core sentinels
remain required, with one new unique readback sentinel, making 90 across 33 core legs.

Reuse the existing native Axum and real Fastly/Cloudflare WASM library commands. Spin's existing
SDK phase owns zero-connect and optimizer markers; the persisted-log validator rejects missing,
ignored and wrong-phase proofs. Capability rows bind the existing separately guarded CLI tests.
Spin accepts that separate ownership only for the exact command, sentinel and artifact with no
additional unowned sentinels. No new CI job, phase, per-test YAML list or runtime capability.

Do not overstate nearby coverage: all four adapter EZ-4/01 readback/setter legs remain planned;
helper timer values do not prove actual Fastly/Spin setter invocation. Fastly EZ-8/03 still needs
its native supported Follow-boundary proof. EZ-8/01 remains planned: numeric validation and
compile-fail focus examples pass separately, but the frozen criterion asks for runtime client
errors for two shapes the typed enum cannot represent. Do not add a redundant raw parser or
silently reinterpret that requirement. Hosted Image Optimizer performance is unaffected/open.

Verification: all five root gates pass, including 2,637 tests across 33 binaries with one
existing ignored test. The core inventory guard passes all 1,103 tests and requires every
bound sentinel. The isolated harness passes 428 tests with two existing ignored tests and
strict Clippy. Evidence/CI/shell/legacy tooling passes 190 tests; manifest validation remains
255 criteria/511 legs, unqualified. The combined dependency/forwarding check and documentation,
formatting and legacy checks pass. Two scoped reviews found no code/binding defect; their stale
dashboard-count finding is corrected.

All twelve binding owners pass: Axum's complete existing CI suite, Fastly's 292-test WASM
library, Cloudflare's 103-test WASM library, Spin's complete pinned CI suite (including 35
contract tests and 19 SDK-resource tests), and the four separate one-test CLI capability guards.
Strict Clippy passes on all three changed WASM test-utils paths. Both existing focus compile-fail
examples also pass separately without activating the literal runtime-error criterion.

Logs are `/private/tmp/pr275-unit-settings-*`; the Axum and pinned Spin CI directories retain
their full artifacts. Keep the initial Spin setup failure at `spin-ci` (ambient Wasmtime
differs from the pin) and Cloudflare's initial `cloudflare-units.log` runner-schema mismatch.
Separate successful runs explicitly select the existing pinned Wasmtime 48.0.2 and matching
wasm-bindgen 0.2.128 runner; no SDK/runtime pin changes or erased failures. These are local
execution results, not frozen-candidate or hosted qualification.

### Rust Check Consolidation (2026-10-31)

The approved [tooling consolidation plan](./2026-10-31-test-tooling-consolidation.md) starts
with static dependency audits and the mandatory forwarding doctest. One existing harness
command, `CARGO_NET_OFFLINE=true scripts/test_integration.sh check`, replaces four workspace
CI steps; five JavaScript files are removed without compatibility wrappers. Audited decoder
JSON data moves byte-for-byte under the harness's `acceptance/dependencies/`. Both decoder
audits share exactly one locked/offline metadata snapshot per root/demo/reusable workspace;
the no-CLI JSON-map feature query remains separate. Actual Cargo-invocation tests prove the
three metadata calls and one feature query, rather than asserting source text alone.

Forwarding tests still extract the actual guide, require exactly one compiled non-ignored
doctest, and exercise real compiler failures, fence injection and non-executed handler bodies.
Static commands run before provider parsing or fixture initialization. CLI controls share the
existing owned process helper, 30-second deadlines and actual fixture-root observations.
Review catches and repairs a final-stderr-cap race with a deterministic failing control;
both output streams are rechecked after command cleanup. Both scoped reviewers clear the fixes.

Verification passes the combined check command, 427 harness tests/two existing ignored and
strict harness Clippy. All five root gates pass, including 2,636 tests/33 binaries/one existing
ignored; CI tooling passes 34 and unchanged evidence tooling passes 58. Retained logs are
`/private/tmp/pr275-rust-tooling-harness-final.log`,
`/private/tmp/pr275-rust-tooling-harness-clippy.log`,
`/private/tmp/pr275-rust-tooling-check-final.log` and the matching `workspace-tests`,
`workspace-clippy`, `feature-check` and `spin-check` logs under that prefix.

Evidence-validator and remaining guard ports are separate pending checkpoints, not completed
by this move. All 255 criteria/511 identities and the 135 ready/376 planned inventory remain
unchanged and unqualified. No runtime, production API, decoder pin, capability or demo/template
behavior changes. Cloudflare timing, resource decisions, consumer/hosted qualification and
candidate freeze remain open; this is tooling ownership, not acceptance or capability promotion.

### Reader And Fallback Acceptance (2026-10-31)

Bind six more implemented local EZ-10 legs without changing production behavior: core reader
and declared-length criteria 01/02, Axum pipeline and extractor/custom-renderer criteria 03/07,
and actual generated/demo criterion 08. All 255 criteria/511 leg identities, expected source
text/digests, follow-up applicability, approval records, pins and capabilities remain unchanged.
Inventory is 135 ready/376 planned; executable readiness is not qualification or parent closure.

Extend existing owner tests rather than introducing wrapper implementations. Preserve the
original eight-byte cohort and six extractor variants; add exact nine-byte/cap-eight readers
and a real `ValidatedJsonWithin<_, 8>` with valid JSON controls. Count physical source polls,
including EOF. Consumed overflow replays the same typed 413 across all three context readers
at smaller/equal/larger/max caps without another poll. Successful cached reads remain reusable
after a stricter cap. A declared ten-byte body rejected at cap five records zero polls and then
returns the exact ten bytes at cap ten. A separate counted ten-byte fixture verifies that an
already-expired inbound deadline wins with sticky 408 and zero polling. Existing malformed
JSON and rendered error-wire controls remain mandatory. Ordinary JSON fixtures use `json!`.

Parameterize the existing Axum pipeline guard with the expected status. Preserve 400 controls
and add 413: accept one rejection or exactly rejection then healthy 200, rejecting extra or
corrupt responses. The former 400-only implementation fails the new positive 413 control before
the helper is repaired. The actual 2 KiB/1 KiB chunked pipeline test now uses that strict guard,
not a permissive search for any 200. The existing extractor TCP proof invokes a custom renderer
with the typed inbound overflow and confirms 413/custom bytes without its injected retry header.

Final evidence review also catches a suffix loophole: `wronghealthy` could pass the old healthy
body check. Its framed CL12 counterexample fails before repair. Reuse workspace-pinned
`httparse = "=1.10.1"` as an Axum dev dependency, adding only that dependency edge to the root
lockfile, with no new resolved version or runtime dependency. The helper now consumes complete
HTTP/1.1 buffered frames and requires exact healthy bytes/no trailing response. Negative controls
cover duplicate/signed lengths, TE ambiguity, truncation, extra data and bounded headers/fields/body
(4 KiB/32/1 KiB). These are test-evidence bounds, not production ingress limits. The corrected
grouped Axum run is `/private/tmp/pr275-local-readers-axum-framed-ci`; the earlier pass alone does
not supply this stronger framing proof. This repairs evidence, not an identified server defect.

Broaden the one existing core CI guard from `--lib outbound::` to `--lib`, migrating all 30
previous exact command bindings together. The 32 ready core legs require all 89 contributing
sentinels; the inventory regression checks their complete union and the owning CI command.
Keep Axum's grouped phase and both CLI guards unchanged. No new CI job, phase, per-test YAML
list, selector, transport, compatibility API or demo/template implementation is introduced.

The broadened core guard passes 1,103 tests, the grouped Axum command passes 289 native
tests/27 contracts plus its fixture/wire phases, and the bundled-demo guard passes five tests.
The fresh generated-workspace guard passes its owning test after host/all-three-WASM builds,
strict Clippy, 33 generated core tests and the actual fallback TCP matrix. Each application runs
the shared 16-observation matrix; owning-test success is not a separate authenticated raw-wire
capture. These are working-tree local assertions, not frozen-candidate or hosted qualification.
Raw owner logs are `/private/tmp/pr275-local-readers-core-guard-final.log`,
`/private/tmp/pr275-local-readers-axum-framed-ci`,
`/private/tmp/pr275-local-readers-demo-guard.log` and
`/private/tmp/pr275-local-readers-generated-guard.log`.

Final verification after the framing repair passes all five root gates: 2,636 tests across
33 binaries with one existing ignored, formatting, strict Clippy, feature compilation and Spin
WASM compilation. The standalone harness passes 409 tests/two existing ignored and strict
Clippy; evidence tooling passes 58 and CI tooling 33. Docs/legacy/inventory and decoder/JSON-map
audits pass. Independent specification, code-quality and final evidence reviews are clear after
the framing finding is fixed. Initial test-only Clippy naming/type issues are repaired without
suppressions. These local results neither freeze nor qualify a candidate.

The four provider-response unit bindings for criterion 10, hosted HTTP/2, consumer end-to-end,
other timing/lifecycle acceptance, resource decisions, candidate freeze and authorized hosted
qualification remain open. The retained Cloudflare elapsed failure is not retried or waived.

### Local Overflow Acceptance (2026-10-31)

The canonical direct Miniflare/workerd launcher, already shared by the fixture suites, resolves
the old local overflow-to-upload recovery failure without another transport or production fix.
An unchanged eleven-case baseline passes at
`/private/tmp/pr275-native-overflow-baseline-cloudflare`. Earlier Wrangler front-proxy failures
remain failed historical topology evidence; neither the old direct-listener diagnostic nor a
passing sibling case retroactively qualifies them.

The existing overflow runner now explicitly checks runtime shutdown, settles owned startup
identity/client captures/logs, and rereads bounded persisted evidence before printing acceptance
sentinels. Reuse the existing response/origin validators: require all eleven cases in their
original order, unique bounded tokens, exact adapter and matching client facts, coherent launch
identity, completed origin collection and successful cleanup without panic. Keep generated
413/no-retry output, ordinary upstream 413/retry retention and each adapter's zero-contact or
incomplete-upload rules. Generated errors may lack a launch header only after successful initial
attestation; a present mismatched header fails. No origin/body cap, observation window, SDK pin,
production API, demo/template contract, CI job or suite selector changes.

Fresh guarded execution `/private/tmp/pr275-overflow-acceptance-all` passes 44/44 cases:
eleven each on Axum, repaired Viceroy, workerd and Spin, with four checked cleanups and owned
startup attestations. Bind eleven existing EZ-10 legs: criteria 05/06 on all four runtimes and
criterion 03 on the three serverless runtimes. Axum 03 remains planned because its pipelining
clause needs an explicitly composed same-candidate TCP proof. Hosted HTTP/2, consumer end-to-end,
unit/extractor/generated-app bindings and exact-candidate qualification are not supplied by this
suite. Follow-up applicability, approvals, all 255 criteria/511 leg identities and frozen source
digests are unchanged. Inventory is now 129 ready/382 planned, still unqualified.

Three colocated mutation tests reject reordered cases and missing/duplicate/foreign case or
lifecycle records, capture disagreement, corrupt overflow/retry/origin facts, complete streamed overflow
and failed cleanup. The first two are observed failing before the readback checks are
implemented. The inventory test separately fails while these legs remain planned.
The final harness passes 409 tests with two existing ignored; evidence-tooling tests pass 57
and strict harness Clippy passes. The initial sandboxed TCP test denial supplies no coverage;
the successful full run has explicit loopback permission.

All five standard root gates pass: 2,636 tests across 33 binaries with one existing ignored,
formatting, strict Clippy, feature compilation and Spin WASM compilation. Docs, legacy,
inventory and decoder/JSON-map dependency audits pass. Independent spec and code-quality
reviews find no scoped defect; these checks do not freeze or qualify a candidate.

The separate Cloudflare elapsed investigation finds no collection-time restamping or renewed
budget. Its terminal host-event yield remains mandatory for frozen-clock freshness and is not
removed to shorten the measurement. The retained 153 ms versus 146 +/-5 ms failure remains open;
the existence of that yield does not prove historical causality. Other timing/lifecycle
acceptance, resource decisions, candidate freeze and authorized hosted qualification remain open.

### Lifecycle Phase Evidence (2026-10-31)

Separate source wait/dispatch/write timing from guest head/body timing without changing any
production transport, scheduling loop, deadline, cutoff, capability or frozen acceptance target.
Capture the original wait's return before each scheduled head/chunk write. Sealed-case checks,
including late C, require schedule <= wait return <= write start <= completion. A bounded
`timing_diagnostics` record computes source-timeline and guest head/body verdicts independently
and reports wait lateness, post-wait delay and write duration. Both phases still participate in
the whole-case validator. Neither diagnostic can override another failed assertion; guest
head/body does not include bidder elapsed or every batch contract. No new suite or CI job.

Two regression tests cover missing/early/late wait witnesses on all four source roles, separate
guest-only failures, and source failures in each of the three measured phases. The missing-field
mutation fails against the old validator before restoration; the final harness passes 406 tests
with two existing ignored. Existing source event and connection bounds remain unchanged.

An isolated diagnostic temporarily sampled the real Axum Send and Cloudflare Fetch futures'
first observed Ready result through existing request-scoped provider state, with only fixed
numeric data serialized after timed operations. It added no production API, request, executor
or alternate scheduler. These instrumented runs receive no acceptance credit:

- `/private/tmp/pr275-sdk-readiness-diagnostic-axum` passes 10/10 but does not reproduce the
  earlier 131.432 ms head miss. Its fast-origin SDK-Ready-to-collection gap is approximately
  25-56 microseconds, allowing independent timestamp truncation and including instrumentation,
  normalization and publication. It does not explain the earlier run.
- `/private/tmp/pr275-sdk-readiness-diagnostic-cloudflare` passes 9/10, missing the slow-origin
  guest head at 706 ms while its source timeline passes. The reconstructed SDK-Ready sample
  spans 704-706 ms and guest collection observes 706 ms; this does not isolate a core queueing
  delay. Ready observation is not the original JS promise settlement or physical receipt.
  Millisecond-aligned samples alone do not prove frozen or stale provider-clock behavior.

All temporary adapter/guest tracing is removed before the final canonical runs. Those retained
artifacts are `/private/tmp/pr275-phase-witness-final-{axum,cloudflare,fastly,spin}`:

| Runtime          | Passed | Result Boundary                                                                                                                                            |
| ---------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Axum             | 10/10  | No miss in this run; root verification builds/tests overlapped part of this run, so it is not an isolated scheduling benchmark.                            |
| workerd          | 9/10   | Slow-origin A elapsed is 153 ms versus 146 +/-5 ms. Source-timeline and guest head/body diagnostics both pass; the separate elapsed assertion still fails. |
| Repaired Viceroy | 20/20  | Twelve timeline/cleanup and eight callback cases; local repaired runtime only.                                                                             |
| Spin             | 10/10  | Local runtime evidence only.                                                                                                                               |

The last three runtimes run serially after the root gates finish. No unchanged failed run is
retried for passing credit. Earlier Axum/workerd misses and failed experiments remain retained;
the current elapsed miss is not evidence of an identified arrival-stamping defect. No provider
latency is inferred by subtracting source and guest timestamps across unsynchronized clocks.

All five root gates pass again: 2,636 tests/33 binaries/one existing ignored, formatting, strict
Clippy, feature compilation and Spin WASM compilation. Harness Clippy, 56 evidence-tooling tests,
docs/legacy and Brotli/flate/JSON-map dependency audits pass. Independent code/evidence reviews
find no scoped correctness defect; one explicitly confirms the Cloudflare elapsed assertion
must remain failed. Inventory stays 118 ready/393 planned, unqualified. Production, demo,
generator, pins and requirements are unchanged. Remaining timing/lifecycle acceptance, candidate
freeze, consumer composition and authorized hosted qualification remain open.

### Lifecycle Measurement Repair (2026-10-30)

Investigate retained strict failures without changing production transports, guest deadlines,
cutoffs or the frozen +/-5 ms targets. A fresh baseline Axum run passes 9/10 cases and misses
guest EOF at 1305.506 ms. Independent source/measurement and CPU-proof investigations find two
fixture defects: a cold request's receipt includes unknown setup/transit in the scheduling
estimate, and source writes themselves sometimes miss their prescribed cadence. Neither proves
an adapter defect, and origin socket-write completion is not peer receipt or guest observation.

Slow/Fast/Timeout sources now append one immutable pre-write alignment seal, using existing
initial `origin/a/b/c` receipts plus `d` for Timeout. Recompute the minimum receipt-minus-guest
sample and add an explicit 1 ms rounding guard; keep the raw provisional origin estimate and
all raw timestamps unchanged. Initial receipts must precede the first source target, and later
mediator receipts cannot reanchor it. CPU retains its existing separate causal calibration.
The estimate still assumes equal clock rates and discloses unknown transit uncertainty and
unproved clock synchronization. Strict guest/source checks remain independent of the estimate.

Scheduled writes use one condition-variable loop with waits capped at 1 ms against the original
absolute deadline, rechecking sticky stop and releasing the mutex while waiting. No new worker,
busy-spin path, timer restart, provider pin or resource-limit increase is introduced. The first
early-wake/precision-tail experiment still produced late source writes and is removed, not kept
as an alternate implementation. Preserve its failed `/private/tmp/pr275-lifetimes-repaired-axum`
run (6/10); passing unit controls never relabel it. Short waits are not a realtime guarantee.

Final payload and terminating-chunk times are captured before diagnostic formatting/locking;
successful framing and payload/terminator/close order are required in the sealed-case validator.
Header, payload and guest EOF targets are unchanged. Regression proofs cover cold alignment,
immutable/order-independent seals, missing/late initial roles and Timeout D, forged evidence,
spurious wakes, stop handling and missing/failed/inverted terminal framing. The earlier
typo-driven terminal test failure is not credited; the corrected removal mutation fails before
the guard is restored and passes afterward.

The serial scheduling comparison retains these results:

| Runtime          | Passed                          | Remaining Strict Failures                                                                                                                          |
| ---------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Axum             | 9/10                            | Guest fast-origin head at 131.432 ms despite the source head completing within 0.268 ms of its target. Source write is not proof of guest receipt. |
| Repaired Viceroy | 12/12 timeline/response-cleanup | Callback wrappers initially fail launch attestation, separately from timing.                                                                       |
| Spin             | 10/10                           | None in this local comparison; not hosted qualification.                                                                                           |
| workerd          | 8/10                            | Slow-case source write at 916.416 ms versus 910.623 +/-5 ms; Timeout guest origin head at 706 ms.                                                  |

Artifacts are `/private/tmp/pr275-lifetimes-quantum-{axum,fastly,spin,cloudflare}`. No failed run
is retried unchanged for passing credit. The earlier baseline and removed precision experiment
remain separate evidence. These results do not establish a universal scheduling bound or
attribute the remaining head delay to provider I/O, guest wakeup or another unobserved boundary.

The Fastly callback failure is a separate fixture defect: its replacement response bypasses
identity middleware. Add the existing `fixture_core::launch::stamp` before delivery, without
changing body bytes, response commitment, early-error behavior or startup verification. The
changed-fixture run `/private/tmp/pr275-lifetimes-attested-fastly` passes all 20 cases: twelve
timeline/response-cleanup and eight `run_custom`/`serve_custom` callback-exit cases, including
the negative early/setup outcomes. The earlier unverified run remains failed evidence.

All five standard root gates pass: 2,636 tests across 33 binaries, one existing ignored,
strict Clippy, formatting, feature compilation and Spin WASM compilation. Harness tests pass
404 with two existing ignored; focused Fastly progress fixture tests pass two, and actual WASI
progress-target Clippy passes. Final code and evidence reviews have no remaining scoped defect.
The acceptance inventory remains 118 ready/393 planned, unqualified. No new CI job, selector,
production API, capability claim, demo/template contract or frozen requirement changes.
Axum/workerd strict timing, other missing lifetime/cancellation acceptance, candidate freeze,
consumer composition and authorized hosted evidence remain open.

### Encoding Runtime Acceptance (2026-10-30)

EZ-5 criteria 04/05 now bind all four local runtimes, criterion 06 binds Viceroy and criterion
07 binds workerd. Reuse the existing coding/relay handler and deterministic fixtures: one
request builder and bounded reporter serve singles and real mixed batches. Spin enters that
same action through canonical application dispatch with its installed native client, without
inventing a second runtime or transport. Production APIs, demo/template behavior and decoder
pins do not change. Historical 24 coding/24 relay controls and redirect 68/84 inventories remain.

The new cohort has eight single requests and one two-slot batch on every adapter, plus two
actual Cloudflare relay modes: 38 cases/42 origin requests. Choices, buffered/streamed modes,
absent negotiation and exact `gzip, br` are checked against origin-observed fields. Token/slot
echoes bind each representation to its request; Original preserves exact gzip bytes, length,
strong ETag and Vary under a one-byte decoded cap and zero decoder allowance. Decode controls
return plain bytes. Conditional representation-metadata changes remain unadopted.

Each cohort owns the existing bounded origin recorder, retains failed/partial clients and
receipts, joins cleanup and validates persisted records before printing sentinels. Parent
validators require the complete cohort. Review added header-cardinality rejection, explicit
cleanup-failure rejection, malformed/partial-data controls and a bounded fragmented trailing-byte
check shared with forwarding. This is finite local framing evidence, not raw provider fidelity.

The workerd relay records exact 37-byte gzip payloads and `Content-Encoding: gzip`; both modes
also expose `Content-Length: 37` in the observed run. This does not promise universal Workers
framing or promote hosted support. Spin's exact encoded bytes are run-specific host-raw evidence,
not an independent observation inside the SDK boundary.

The first full Fastly run stopped before wire tests on startup identity. Its staged guest
embedded `a714723d16cb5b9c8f45a4c30414d6445427354b056167951a0f6f3abc98c2f6`, while
launch expected `a1283c303aa4daf2fe1dd5ef6125a7fc62098ac0271a590e8bdea6844e68c081` after
intervening crate-source edits. Preserve `/private/tmp/pr275-encoding-fastly-ci`; no failed
group is relabelled as passing. A separate source-stable native wire run passes. Final full
verification uses unchanged inputs, never an identity waiver or retry-based credit.

Final verification passes all five root gates: 2,636 tests across 33 binaries, zero failures
and one existing ignored test; strict Clippy, formatting, feature compilation and Spin WASM
compilation pass. The isolated harness passes 397 tests with two existing ignored; the shared
qualification fixture passes 59. All three guest WASM Clippy checks and 206 tooling tests pass,
as do dependency-pin, documentation, inventory and legacy-API audits. CI ownership tests parse
the existing workflow YAML and reject missing matrix members, skipped owners, commented commands
and mismatched output paths without changing CI itself.

Source-stable full Axum, Fastly and Spin CI runs pass at
`/private/tmp/pr275-encoding-final-{axum,fastly,spin}-ci`; native workerd wire checks pass at
`/private/tmp/pr275-encoding-final-cloudflare-wire`. Their persisted records contain all
38 cases/42 origin requests with successful joined cleanup, plus unchanged 68/84 redirect
inventories. The workerd relay length observation above is reverified in both final cases.
Fastly's client-progress phase retains its expected failing-probe exit code as an asserted
negative control, not a newly qualified scheduler or hosted lifetime guarantee.

Exactly ten command/artifact/readiness bindings change: 118 ready/393 planned, still unqualified.
All 255 criteria/511 identities, expected text/digests, applicability and approvals are unchanged.
No new CI job, runtime selector, provider pin or qualification promotion is introduced. Consumer
criterion 08, hosted evidence, other normalization/limit/lifetime acceptance and candidate freeze
remain open. Two independent investigations selected fixture reuse over a parallel encoding path;
separate code and evidence reviews drove the negative controls above.

### Axum Pending-Body Timing Acceptance (2026-10-30)

The missing EZ-1 criterion 06 native TCP proof is implemented. Both fixed cases use the real
`AxumServiceState`/`serve_http1` path while the client keeps its write side open and sends no
chunk or terminator: 200 ms outbound timeout yields attributed 504 by 205 ms; the original
100 ms ingress deadline yields 408 by 105 ms. The latter ages the request by 20 ms before
forwarding without restarting its admission clock. The outbound measurement conservatively
starts immediately before the facade call, not at a fabricated internal dispatch timestamp.

The handler stores terminal timestamps; the client timestamps complete Content-Length-framed
responses before EOF and joined cleanup. Numeric diagnostics run afterward. Actual source
Pending, zero payload/EOF, exactly one release and no origin connection are required. A ready
source-error negative control cannot satisfy the Pending witness. Baseline and reviewed TCP
runs passed with unchanged production code; this is an evidence repair, not a timer fix. The
unrelated retained EZ-6 lifetime failures are neither replaced nor waived.

One environment joins the existing grouped Axum guard; no job, selector or second execution
is added. Criterion 06 and its negative control are mandatory discovered sentinels. Its artifact
is the actual `outbound/redirect/axum/native-tests.log`, not the older native raw-log placeholder.
Older rows still need their canonical export mapping reconciled during Q3 candidate assembly.
Passing libtest output asserts both windows; focused `--nocapture` logs additionally retain the
numeric observations. The inventory is 108 ready/403 planned, still unqualified, with all frozen
255 criteria/511 identities, expected text/digests, applicability and approvals unchanged.

Two independent path/measurement investigations selected the existing TCP boundary over a
production timer change. Independent code and evidence review added the aged-deadline control,
explicit Pending witness and real phase-log binding. Hosted/consumer qualification, other
encoding/lifetime/cancellation acceptance and candidate freeze remain open.

Final verification passes all five root gates: 2,636 workspace tests in 33 binaries, zero
failures and one existing ignored test; strict Clippy, formatting and feature/Spin checks.
The isolated harness passes 390 tests with two existing ignored, and tooling passes 152.
The complete Axum CI suite passes 289 native tests, 27 contracts, fixture compilation and its
unchanged wire inventories. The final focused run observes complete 504/408 responses at
202.081/102.596 ms from their original anchors. Logs remain under
`/private/tmp/pr275-axum-settlement-*`, with the reviewed CI artifact under
`/private/tmp/pr275-axum-settlement-reviewed-ci`. No retry-based credit, deadline relaxation,
production transport change, hosted deployment or qualification promotion is performed.

### Real-Ingress Forwarding Acceptance (2026-10-30)

The existing wire suites now cover real inbound conversion and `from_request` on Fastly,
Cloudflare and Spin, sharing one fixture handler and bounded origin recorder. The 53 ingress
cases cover empty GET/HEAD framing, first-nonempty-item rejection, streamed POST/PUT/PATCH,
bodiless OPTIONS/DELETE, Begin before collection, streamed-Begin refusal, cached batch input,
one-settlement 307 following and zero-read unsupported-option rejection. Original source errors,
deadlines, method/body bytes and framing remain intact. Fastly's rejection controls also require
zero backend configuration and registration. Successful cleanup and persisted readback precede
sentinels; parent validation requires the complete group. Fragmentation is not a fixed boundary.

Spin's explicit `Content-Length: 0` now stays buffered without source/timer construction, including
after the inbound read deadline. Missing length remains unknown, framing stays `HostManaged`,
and explicit `body_bytes` deadline precedence is unchanged. Actual Spin red evidence recorded
the former 408 before the repair; the native ingress matrix now passes. The co-located private
regression is WASM-compiled; plain Wasmtime cannot execute that lib-test component's Spin KV
imports, so its build is not represented as an executed unit test.

The pending workerd request exposed a test-boundary problem: EdgeZero returned the correct 400
with zero reads, but Wrangler's outer dev proxy withheld it until client disconnect. Disabling
only dev body draining did not fix it. The approved replacement uses the pinned Wrangler public
configuration mapper and one native Miniflare/workerd listener; the same pending request passes
without draining, finishing the body or changing the adapter. Compatibility, KV seeds, staging,
launch identity and cleanup remain tested. No alternative launcher remains; application build,
deployment and `edgezero serve` are unchanged. Hosted guarantees are not promoted.

The inventory activates 22 provider-local EZ-1 legs and retains all 255 criteria/511 identities,
source text/digests, applicability and approvals: 107 ready, 404 planned, `qualified: false`.
The remaining local EZ-1 gate is Axum criterion 06's strict 200/205 ms and 100/105 ms timing.
Hosted criteria 02/05/06 and consumer criterion 13 remain open. These functional proofs do not
waive the recorded lifetime, overflow-recovery, SDK-parity or external qualification failures.

Independent read-only fixture, provider-boundary and evidence reviews selected one shared
handler and the pinned public configuration mapper rather than repeated transport controls.
Review caught the actual KV mapping shape, nested empty SDK defaults, shutdown cancellation
classification and impossible source-counter combinations; each repair has regression coverage.
Cloudflare smoke assertions follow the captured host body signal, never infer it from framing.

Verification passes all five root gates: 2,633 tests across 33 binaries, zero failures and one
existing ignored test; strict workspace Clippy, formatting and both feature/target checks.
The isolated harness passes 390 tests with two existing ignored; shared fixtures pass 56 tests
and tooling passes 150. Native and relevant WASM Clippy, docs/manifest, legacy and decoder-pin
audits pass. Full Fastly and Spin CI suites, native Cloudflare wire and Cloudflare smoke suites
pass. Runtime records remain under `/private/tmp/pr275-ez1-{fastly-ci,spin-ci,cloudflare-native-final,cloudflare-smoke-verified}`;
failed earlier probes remain retained. These local observations do not freeze a candidate,
qualify hosted behavior or close the other planned encoding, lifetime and cancellation criteria.

### Connect Capability Publication (2026-10-30)

The missing EZ-4.9 declaration is implemented: `outbound-connect-timeout` is Native on Fastly
and BestEffort on Axum, Cloudflare and Spin. Fastly's split-free flexible phase budgeting is
Native; no deadline, cancellation, scheduling or resource-accounting capability changes.
Core manifest round trips cover required/optional placement, and all four existing native CLI
matrix tests verify the declarations. No transport, provider timer, runtime pin or CI job changes.

The public guide, forwarding guide and normative matrix now agree. Native describes the
provider-backed setting, not qualified hosted precision: Viceroy does not enforce connect expiry,
DNS/TLS phase coverage remains unqualified and blocking calls stay non-preemptible. The existing
docs checker rejects lost/inverted caveats and the stale fixed-share claim. Review corrected
the default-timer wording and narrowed the B31 cardinality statement to a fixed connect setting.

This closes the previously recorded metadata implementation gap, not all D1c runtime acceptance.
Inventory bindings remain unchanged at 85 ready/426 planned: native CLI metadata proof must not
be attributed to the separate adapter SDK/runtime phases. Consumer/hosted evidence, strict local
timing and the remaining forwarding/encoding/lifetime scenarios stay open. No candidate freeze,
hosted deployment or provider qualification is authorized by this publication.

Two independent read-only reviews found no remaining issue after the timer-wording and
checker fixes. Final verification passes all five root gates: 2,633 tests across 33 binaries,
zero failures and one existing ignored; strict workspace Clippy, formatting, workspace feature
checks and the Spin target check. The 85 manifest tests, five tests in the four adapter CLI
matrix cards, documentation checker/self-tests, 57 relevant tooling tests, legacy scan and
unchanged manifest/digest validation pass. Logs remain under `/private/tmp/pr275-connect-capability-*`.

### Fastly Registration Observations (2026-10-30)

The approved `context::FastlyOutboundObservation` and immutable `FastlyOutboundStats` are now
implemented. Existing Fastly callback/helper owners retain one recorder; their shared core
provider family stores only a weak link. Public handles retain coherent numeric snapshots only.
Callback exit freezes recording and releases configuration keys even when an accepted response
body survives. Nested helpers preserve the outer record, and nested serving callbacks isolate it.
Canonical handles are available before prepare and cannot be replaced through scratch extensions.

Eligible canonical keys count before fallible backend-cache access. Expired preflight counts
nothing, cache hits do not count registrations, and live budgets are checked again after locking.
Actual registration calls are bracketed in the paired clock, including failures and late returns;
late success stays cached while the original timeout wins. Fixed warnings run outside the cache
mutex at 50 ms or longer. Backwards samples preserve outcomes and warn rather than fabricate time.

EZ-2 acceptance 22 binds the existing guarded Fastly unit phase to the threshold test and its
scope, accepted-body, cache, retained-client and request-extension companions. No new CI job or
runtime selector is added. The inventory is 85 ready/426 planned, still unqualified; the frozen
255 criteria/511 identities and all applicability/approval metadata are unchanged. Other D1b
runtime accounting/performance proofs remain open. No deployment or pin promotion is authorized.

The read-only closure audit also identified uncompleted local acceptance beyond this feature:
native forwarding matrices outside Axum, connect-timeout capability metadata, complete encoding
wire controls and the remaining lifetime/cancellation scenarios. Historical strict timing,
Cloudflare overflow recovery and Fastly HEAD/205 reference-parity failures remain recorded,
not waived. Consumer/hosted criteria, optimizer entitlement, resource-policy decisions and
candidate freeze still require their separately authorized evidence. Green CI is not completion.

Two independent read-only reviews selected callback-local recording over a second ownership
registry. Review fixes exclude accounting overhead from timing, count eligible cache failures,
retain post-lock expiry checks and distinguish a foreign extension from the live handle.
All five root gates pass: 2,631 tests across 33 binaries, no failures and one existing ignored
test; strict workspace Clippy, feature checks and formatting are clean. The guarded local
Fastly WASI phase passes 291 tests, including every new acceptance sentinel, and strict Fastly
WASI Clippy passes. The public API/native phases, 57 relevant tooling tests, docs/legacy audits
and manifest validation pass. Logs are retained under `/private/tmp/pr275-fastly-observation-*`;
these working-tree observations do not freeze or qualify a candidate.

### Unsupported Policy and Marker Acceptance (2026-10-30)

EZ-3 acceptance 08 binds all four adapter test phases to one core `test-utils` conformance
helper. Native validation/time/progress are delegated unchanged; the helper inspects and
reconstructs empty prepared batches privately before forwarding them to the native client.
Expired otherwise-valid requests differ only by optimizer presence: named 400 with unsupported
options, original `RequestDeadline` 504 without them. Separate pending-body cases require zero
polls, zero timers, zero eligible dispatch and exactly-once disposal. Follow must fail at hop
zero without running policy. A finite poll budget makes broken ordering fail rather than hang.
Fastly covers unsupported Begin/Batch only; native Send/Follow mapping and hosted optimizer
qualification remain open. No production implementer API, capability or compatibility path changes.

EZ-7 acceptance 04 binds ten actual origin exchanges per adapter in the existing wire runtime:
GET 200/204/304/302 and HEAD 200, each inheriting client-wide Off or explicitly selecting On.
The existing progress probe and Spin wire endpoint share the same send/report helper. A fresh
bounded origin reuses the current marker renderer, validates framing/method/token and records
actual method, path, port and token. Persisted readback requires ten unique reports, matching
captured HTTP bodies/receipts and successful cleanup. Mutation tests reject omission, duplication,
foreign metadata, wrong methods/statuses/policies, altered bytes and failed cleanup. Keep the
68/84 redirect, six-case batch and Spin policy/normalization inventories unchanged. Cloudflare
remains an explicitly executed local wire leg, not newly activated CI or hosted evidence.

Read-only policy and runtime reviewers selected a shared conformance helper and fixture report
tail over four repeated wrappers, synthetic successful transport or another runtime suite.
Review added bounded negative polling and inventory-metadata mutation coverage. The first Axum
marker run's text/binary capture mismatch is retained at `/private/tmp/pr275-policy-marker-axum-wire`;
the corrected run uses the existing binary capture API, without changing transport or assertions.
The inventory is 84 ready/427 planned. All 255 criteria/511 identities, expected text/digests,
applicability, pending approvals, production policies, pins and hosted gates remain unchanged.
These dirty-tree checks above `52d89c15` do not freeze or qualify a release candidate.

Verification: all five root gates pass, with 2,625 workspace tests (32 binaries, one existing
ignored), 384 controller tests (two existing ignored), 196 tooling tests and 56 shared-fixture
tests passing. Strict native, three-target adapter WASM and shared/Spin-guest Clippy pass.
Guarded Axum/Fastly/Cloudflare phases and the complete Spin CI run pass. Each real wire run
retains ten marker observations plus its unchanged earlier inventories. Raw working-tree records
are under `/private/tmp/pr275-policy-marker-{axum-wire-verified,fastly-wire,cloudflare-wire,spin-wire,spin-ci}`.
The focused post-review poll-bound test also passes. No hosted run, runtime promotion or
source-criterion change was performed.

### Client Selection Acceptance (2026-10-30)

EZ-2 acceptance 01 now binds the four existing adapter-owned CI phases. Axum retains its
handler-level recording-client and skipped-default-factory proofs. Fastly exercises the real
prepare/conversion/dispatch path with state plus prepare, prepare only and default-only cases.
Cloudflare and Spin exercise state/default selection through their adapter dispatch paths.
Supplied clients have distinct frozen-clock observation counters; the handler retrieves its
client and triggers a fixed invalid-GET preflight error, without outbound I/O or timer work.
Default-only controls retain the adapter's default marker setting. Spin's CI validator requires
its contract proof in the SDK phase and rejects missing, failed and wrong-phase observations.

Read-only selection and acceptance reviewers chose this finite proof/binding checkpoint over
new scripted transports or a second runtime marker suite. Production APIs, precedence, SDK
and runtime pins, acceptance identities/text/digests and pending approvals are unchanged.
The inventory has 76 ready and 435 planned legs; this is executable acceptance readiness,
not exact-candidate or hosted qualification. Remaining resource/timing evidence and external
Image Optimizer/consumer qualification gates stay open.

Verification: all five root gates, 2,623 workspace tests, 380 controller tests (two existing
ignored), strict native/three-target WASM Clippy and guarded Axum/Fastly/Cloudflare phases pass.
The fresh complete Spin CI run at `/private/tmp/pr275-client-precedence-spin-ci` passes with the
new 33-test SDK contract target and its separate 19-test resource target. Review repaired the
new guard's initially conflated SDK/async inputs and added malformed/wrong-phase negatives;
existing async-component proofs retain their original phase. These dirty-tree observations
above `0b949f0e` are not frozen-candidate qualification records.

### Cloudflare Native Wire Recovery (2026-10-30)

The full 68-exchange/84-origin-receipt redirect matrix and six-case/32-receipt batch matrix now
pass on the pinned local workerd. The initial failure was the provider's inspector network
preview, not a missing application cap: the same compiled guest and Rust origin pass under
direct Miniflare, while enabling only CDP `Network.enable` reproduces the fatal gzip decode.
The native Send/Follow regression retains malformed encoded bytes, typed header-limit precedence,
zero source pulls and exactly-once abort. No request/response limit or assertion is weakened.

One Cloudflare fixture launcher now uses the selected Wrangler package's public API with
inspection disabled. A small Node SDK bootstrap preserves config, bindings, nonce and KV
persistence, awaits readiness and disposes once; Rust retains all supervision and assertions.
The full smoke suite preserves both application modes, console telemetry, KV and ownership.
Remove the temporary batch-only selector: batch and redirect evidence share one full wire run.
Package-selection, startup/shutdown and error tests guard the launcher contract. Independent
read-only fixture and runtime reviews selected this path over a runtime patch or a second
transport. The inspector-enabled negative evidence and provider defect remain explicitly open.

Acceptance readiness remains 72/439; all reviewed identities, requirements, applicability,
decisions, SDK/runtime pins and hosted gates are unchanged. This resolves local full-wire
execution, not frozen-candidate, consumer, Image Optimizer or hosted qualification.

Verification: all five root gates pass, with 2,623 workspace tests, 379 controller tests,
100 Cloudflare WASM unit tests, strict native/WASM Clippy and 194 tooling tests passing.
The controlled inspector-on failure is retained at
`/private/tmp/pr275-cloudflare-app-fetch-inspector2.log`; the passing local full wire and
two-mode smoke observations are in `/private/tmp/pr275-cloudflare-recovery-final-wire` and
`/private/tmp/pr275-cloudflare-recovery-final-smoke`. These are working-tree characterizations
above `80f73757`, not frozen-candidate qualification records. Review also closed a late-SDK-error
propagation gap and moved subprocess lifecycle tests under bounded Rust-owned cleanup.

### Mixed Batch Recovery Acceptance (2026-10-30)

This historical checkpoint's temporary focused selector and full-wire failure are superseded
by the native wire recovery above; its failed observations remain retained.

EZ-6 acceptance 22 now binds all four native runtime legs to six shared controlled-origin cases:
mixed nonblocking check/next/collect and fresh collection, each with completion, cutoff and an
exact typed progress failure. Equal-sized foreign batches and missing receipts reject without
losing either input's original results or elapsed values. Keep private duplicate/conflict forgery
in the five existing core tests; each runtime leg requires their same-candidate companion proof.
The release checker withholds runtime credit for missing, failed or corrupt companion evidence.
Persisted runtime readback requires all six cases, 32 origin requests and unchanged owned outcomes.

Axum, Fastly and Spin retain one CI wire execution; the focused `outbound-batch` selection shares
the same runtime/fixture/matrix and provides Cloudflare's explicit local binding. It creates no
CI job, executor or transport. The modern Cloudflare fixture removes a redundant weak-reference
flag rejected by workerd; older demo/template dates keep explicit enablement. Full Cloudflare
redirect execution still aborts workerd on malformed gzip and remains an independent open failure.
Spin's initial startup-identity failure and subsequent unchanged-code isolated pass are retained.

The inventory has 72 ready and 439 planned legs. All 255 criteria/511 identities, expected text,
digests, applicability, optional decisions, production APIs, SDK/runtime pins and hosted gates
remain unchanged. These working-tree runtime checks above `a0fd6f77` are not frozen-candidate
qualification; remaining exact-candidate bindings, retained timing failures, hosted/consumer
evidence and fresh approval still block closure.

Checkpoint verification: all five root gates, 2,623 workspace tests (32 binaries, one existing
ignored), 377 controller tests (two existing ignored), 56 qualification-enabled shared-fixture
tests and 194 tooling tests pass. Strict controller/shared-fixture and all four guest target
Clippy checks pass. Consolidated Axum, Fastly and Spin CI suites and focused native workerd
batch/readback pass. The local Fastly run uses the tracked readiness repair without runtime
promotion. Read-only identity/evidence and controller/runtime reviewers found and verified fixes
for equal-sized foreign probes, exact restoration cardinality, mandatory companion corruption
checks and failure-record rejection. Select one shared runtime matrix plus the existing core
private-boundary proof; do not expose mutable receipt identity or repeat core per adapter.

### Spin Normalization Acceptance (2026-10-30)

EZ-5 acceptance 11's `unit/spin` leg now binds the existing shared CI invocation. Its two
colocated matrices cover normalization and body disposition through the same private
head-construction boundary as production; forbidden/raw fields are injected only there.
Native SDK controls retain the original reader, trailer and completion ownership, including
zero-read bodyless suppression and bounded one-item 205 disposal. Count delivered items
separately from pending polls and bound producer teardown with the original deadline.
Eight actual HTTP 200/206 representation controls cross both decoding choices and response
modes without altering the 68/84 redirect or 26/26 independent-cap inventories.

The controller requires every native/SDK/wire proof in its owning phase and reopens persisted
wire evidence, binding origin receipts, effective request settings and HTTP report bytes.
Mutation tests reject missing/duplicated cases, forged phases and altered fields or ownership.
The inventory now has 68 ready and 443 planned legs. Frozen requirements, precedence-table
assertions, optional metadata decisions, provider support and hosted qualification are unchanged.

### Integration Test Ownership (2026-10-30)

The approved [integration consolidation plan](2026-10-30-integration-test-consolidation.md)
moves runtime applications and acceptance assets under `crates/edgezero-test-harness`.
Use its [integration guide](../../../crates/edgezero-test-harness/README.md) for current commands:
`scripts/test_integration.sh --adapter NAME --suite ci` is the shared local/CI entrypoint.
Adapter-owned contracts and private-boundary tests remain colocated. The controller and guest
workspaces retain independent dependency graphs and lockfiles. Historical commands and raw
measurements below retain their original paths; this layout change does not revise requirements,
acceptance identities, failed observations, capability support or hosted qualification.

### Current Delivery Dashboard (2026-10-29)

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

The approved [test/CI consolidation plan](2026-10-29-test-ci-consolidation.md) now tracks
the fixture/runner cleanup separately from delivery requirements. It preserves native,
synchronous SDK, async CLI, actual HTTP and raw-ingress execution boundaries, all acceptance
identities and unqualified evidence. One Spin runtime suite replaces scattered YAML checks;
the shared Rust process owner replaces the separate ingress runner. Historical checkpoint
commands below remain revision-specific, not current invocation instructions. Consolidated
implementation workflows pass. A later exact-key warm run records a 373-second job span
versus the baseline's 625 seconds, missing the provisional six-minute target by 13 seconds.
The consolidation plan records cache transfers, workload caveats and the approved but rejected
20 GB repository-limit update; the effective limit remains 10 GB. This CI measurement closes
neither outbound acceptance nor hosted-provider qualification.

The six approved consolidation commitments are **not collectively complete**:

| Commitment                                                            | Current Implementation                                                                                                                                                                                                                                                                                | Remaining Work                                                                                                                                                                                               |
| --------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1. Shared preparation, validation, normalization, decoding and limits | **Hard-cut implementation locally verified.** The facade captures invocation ownership/time and prepared contexts; all four adapters return raw heads to shared normalization, disposition, decoding and bounded collection.                                                                          | Bind applicable exact-candidate acceptance legs (P1-P3, D1-D4).                                                                                                                                              |
| 2. One pipeline for send, begin, batches and redirects                | **Send/Begin/Batch plus explicit Follow implemented.** The shared pipeline includes eager drivers, ownership, raw observations, observation-only checks, mixed collection, lossless identity-bound recovery and head-first redirect policy.                                                           | Complete all-target redirect runtime parity and acceptance/qualification (P5, D1-D6, Q3-Q5).                                                                                                                 |
| 3. Remove superseded implementations and APIs without compatibility   | **Hard cut locally verified.** Old implementer send/raw-vector batches, `from_futures`, `finish_batch_item`, public receipt construction, Fastly's separate lifetime registry, copied task/deadline policies and old Body enum API are removed; no aliases remain. Follow uses those same primitives. | Complete applicable acceptance bindings before closing all P1-P5 work.                                                                                                                                       |
| 4. Preserve direct native Image Optimizer                             | **Send/Follow share native dispatch.** The native SDK mapping hands its head to the common finalizer without another fetch or transformation pipeline; prepared redirects retain optimizer options. Local actual-client Send/Follow controls verify native unsupported boundaries.                    | Qualify successful optimizer-first-hop redirects and hosted transformation/performance. The synchronous blocking horizon remains UNKNOWN (D1d/Q4b).                                                          |
| 5. Migrate demo, templates, clients and docs together                 | **Hard-cut callers migrated.** Supplied clients, demo, generator, fixtures, guides and guards use prepared contexts, shared ownership/progress, raw events and explicit collection states; demo/scaffold add explicit allowlisted Follow.                                                             | Complete remaining acceptance and qualification (P5/Q1-Q5).                                                                                                                                                  |
| 6. Document the TS hard-cut ownership boundary                        | **Documented** in the [adoption guide](../../guide/proxying.md#hard-cut-adoption). Transport stays upstream; application policies stay in the consumer.                                                                                                                                               | Implement the remaining EdgeZero prerequisites and obtain consumer composition/qualification evidence before cutover. Do not delegate body dropping, buffering or transport workarounds to TS (P1-P5/Q4/Q5). |

Keep three different status questions separate:

- **Implementation:** this dashboard and the requirement table describe code present at the
  snapshot. A foundation or a local checkpoint does not complete its parent task. No full P1-P6
  or D1-D6 migration task is closed by the checkpoints recorded so far.
- **Executable acceptance coverage:** G1's
  [inventory](../../../crates/edgezero-test-harness/acceptance/outbound-cutover-cases.json) covers 255 source
  criteria and 511 split/environment legs. Its `implementation: planned | ready` field describes
  readiness of that leg's declared command/sentinel, not overall feature implementation.
  EZ-23 local criteria 01-12 and EZ-9 core criteria 01-15 plus local runtime criteria 16/17 are
  `ready`; core policy, receipt/cutoff/cancellation and marker checkpoints activate EZ-4 core
  criterion 01, EZ-5 core criteria 01/02/03/10/11, EZ-6 core criteria 13/15/17/22 and EZ-7 core
  criteria 01/02/03. The [forwarding and shared-head checkpoint](#forwarding-and-shared-head-acceptance-checkpoint-2026-10-29)
  also binds EZ-1 core criterion 07 and EZ-3 core criterion 04's ordering clause. The
  [native policy checkpoint](#native-response-policy-and-evidence-validation-checkpoint-2026-10-29)
  adds each adapter's unit criterion 09, Axum/Cloudflare/Fastly unit criteria 10/11 and Axum
  forwarding criteria 01/04/08. The Spin host-wire checkpoint below adds its unit criterion 10:
  the subsequent Axum forwarding-mode checkpoint binds criteria 09/10/11/12/14. The current
  provider-local forwarding, client-selection, policy, marker, observation, encoding and overflow
  checkpoints plus the unit-settings, provider-boundary, focus-compiler and Fastly post-preparation
  dispatch passes give 153 ready legs, with 358 still `planned`.
  This includes the strict Axum pending-body proof, ten local encoding proofs, eleven local
  overflow legs and six reader/fallback proofs. G1/Q3 binding work remains unfinished, not evidence
  that these features are absent. Only the explicitly bound commands count as executable legs;
  unrelated checkpoint tests do not activate another leg.
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

### Axum Forwarding Mode Acceptance Checkpoint (2026-10-30)

Continue G1/Q3 and P2 with five complete Axum TCP legs: EZ-1 criteria 09/10/11/12/14.
Reuse the existing production HTTP/1 connection helper and origin recorder, extending the
recorder with request URI, 307 Location and a receipt/release gate. Separate small action
handlers preserve each operation's ordering assertions; no new forwarding framework or
production transport is introduced.

POST delivers exactly `abc` with Content-Length 3 and no Transfer-Encoding; bodyless
OPTIONS/DELETE also have no upstream Transfer-Encoding. Begin returns before the origin releases
response headers, and the handler observes the complete origin request before collection.
An actual streamed POST is refused synchronously without source polling or an origin connection.
Follow sends one GET to each of two origins through a 307, with zero bytes and no Transfer-Encoding.
A passive wrapper around the actual inbound chunked stream retains request parts/extensions
and observes one EOF, zero payload bytes and one release before replay and after completion.
Pending polls are not counted as extra reads. Cached-empty GET uses the same context in a
one-slot completed batch. Unsupported native optimizer options return the exact Axum 400 on
the first poll while the client withholds all chunks and EOF; the original source stays unread.

Environment `axum` also contains wire-report markers. Guard discovery now selects the exact
declared command within every requested environment, rejecting missing groups rather than
mistaking a wire marker for a Rust test. The existing CI command adds that environment once;
no job or duplicate sentinel list is added. Guard regression fixtures run the declared Cargo
argument prefix through a temporary fake executable, retaining listing/execution and cleanup
checks. Negative binding/selection tests fail first, then pass after activation. The inventory
has 67 ready / 444 planned legs; all 255 criteria, 511 identities, expected text, digests,
applicability, conditions and approvals remain unchanged. No sibling provider leg is promoted.

A fresh unchanged strict-lifetime probe at `91266319` remains failed: six of ten cases pass.
`slow-origin`, `timeout-before-cutoff`, `fast-origin` and `pending-body-cutoff` exceed their
original timing targets. Raw evidence remains at
`/private/tmp/pr275-91266319-close-axum-lifetimes/events.jsonl`, 84,430 bytes, SHA-256
`5dfba280f3631f82dbd9e34047ecb816482ca04912de2b40c6f525e5e2518000`.
This is local working-tree characterization, not a durable qualified release artifact. Do not
replace it with the new TCP tests or relax the +5 ms windows. Cloudflare overflow/recovery,
remaining forwarding/normalization legs, candidate freeze and authorized hosted qualification
remain open. Newton reviewed the actual-source/receipt proofs; Dewey reviewed guarded discovery
and all inventory identities. The POST framing observation found in review is asserted explicitly.

Verification passes all five root gates: formatting, strict workspace/all-targets/all-features
Clippy, 2,621 workspace tests across 32 binaries (one existing ignored), workspace feature
compilation and the Spin target check. The exact grouped Axum command discovers all 25 required
names before one execution of 285 library tests and 27 contracts, with no failures. All 136
evidence/guard and 31 CI/legacy regressions pass. Documentation lint/build, targeted Prettier,
Actionlint, ShellCheck, docs/legacy guards, inventory validation and diff checks pass.
The final naming correction uses `observation_cutoff` without a legacy alias and its focused
batch test passes. Production APIs, demo/generator behavior, SDK/runtime pins and provider
capabilities are unchanged. This checkpoint does not rerun the serverless runtime suites or ignored
generated-workspace gate, freeze a candidate or replace retained failed runtime evidence.

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

### Spin SDK Review Refresh Checkpoint (2026-10-28)

The requester authorized consuming the latest reviewed SDK contribution after the upstream
review fixes were pushed. Root, demo, reusable/request-fidelity fixtures and generator now pin
`6c59ed5b4fcb377072763aee0b27a3abdd8edb72`, with all four application locks aligned. This selects
WASIp3 0.10.0/WASI 0.3.2 and wit-bindgen 0.63.0; the preceding checkpoint's 0.9/0.62 observations
remain historical, not evidence for the refreshed graph. The
[dependency audit](../../audits/2026-10-27-spin-sdk-eager-progress.md#current-reviewed-revision-2026-10-28)
records the SDK review scope and new local evidence.

One private provider launcher explicitly detaches the new SDK join handle at every eager/batch
spawn site, keeping registration and cancellation with the existing shared task owner. No private
scheduler contract, deprecated API, second executor or compatibility mode is introduced. The native
WASM request-conversion test now constructs and dispatches inside the SDK executor, preserving
its admission/refusal assertions; the old raw-test setup reproduced an SDK task-context panic.

The five root gates pass (2,568 workspace tests, one existing ignored), as do strict native and
both Spin WASM Clippy configurations, all eight Spin WASM feature combinations, 35 WASM contracts,
21 SDK-resource cases and the real eager component on exact Wasmtime 48.0.2. Demo core (60), demo
and request-fidelity Spin builds, generator source/lock guards and the full Spin 4.1.0 wire matrix
(68 exchanges/84 receipts) pass. The full ignored fresh generated-workspace gate also passes:
33 core tests, CLI/configuration, all three WASM adapters and fallback TCP assertions. Decoder/map,
documentation and legacy/evidence-inventory guards pass. These are local working-tree results,
not a frozen or hosted qualification candidate.

The approved D6 characterization resumes with this SDK: six of eight local cases pass, including
per-call timeout and unobserved work. Slow-origin Begin timing and the CPU write window still
fail their original targets. Preserve both failures and all artifacts; this refresh does not
close D6, native body-entry evidence, consumer acceptance or hosted qualification. No source
criterion, requirement/interface digest, runtime pin, capability or acceptance binding changes.
The inventory remains 32 ready / 479 planned and unqualified; no deployment or candidate freeze.

### Main Integration Checkpoint (2026-10-28)

**Review hardening follow-up (2026-10-29):** retain all outbound requirements and
the original measured timing thresholds. Hard-cut pre-dispatch hooks to the protected
`PreDispatchRequest` view, sharing the admitted body cell and immutable head/input
policy through bounded transformations and routing. Revalidate continuing heads;
preserve configured fallback overflow/timeout responses even when a hook ignores a
read failure. Migrate every in-tree hook rather than adding a raw-request compatibility path.
Explicitly select reqwest's no-retry policy, recover transient accept errors with
selectable bounded backoff, yield on empty Axum response chunks, and reject unsupported
Workers statuses before SDK conversion. Exercise guards against scan failures and actual
zero-test execution, and make source-error variant assertions unconditional.

Fixture startup must be bounded and fail closed when responder ownership cannot be
established. This is an identity mitigation for the existing port handoff, not atomic
socket reservation. Do not add production lifecycle APIs only for fixture attestation.
Retained readiness overruns remain failures, and existing nonzero exit handling remains
intact. Local proof and hosted qualification remain separate; none of these fixes widens
the +5 ms targets or promotes provider guarantees.

Independent core/runtime and guard/harness reviews identified additional edge cases:
cached-transform input errors must poison the shared cell; uncapped streams must not gain a
`usize`-sized cumulative cap; the first streaming error must retain its concrete cause through
nested maps; and a headerless fault response cannot borrow a later non-recovery response's
identity. The implementation addresses these without compatibility paths or new transports.
Fault/recovery probes now await correlated failure and delivery-work witnesses before sending
recovery instead of inferring callback completion from client EOF. Reuse remains a strict
observed assertion, not a guaranteed provider lifetime. Retain failed characterization runs
alongside the subsequent passing smoke run; do not replace their timestamps or verdicts.

Local verification passes all five required Rust gates, all six provider WASM Clippy
combinations, 1,101 core tests, native TCP/adapter contracts, 308 harness tests (one
controlled subprocess helper remains ignored), 64 shell-guard regressions and the
60-test demo. Actual SDK suites pass Cloudflare 99, repaired-Viceroy Fastly 280 and
Wasmtime Spin 21. Fastly smoke and the Axum/Spin wire runners pass unchanged assertions.
Documentation lint, formatting, build and contract checks pass. These are local dirty-tree
observations, not frozen or hosted qualification: the inventory remains 61 ready /
450 planned, qualified false. Headerless canonical assembly-response attestation stays open.

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

| Local Runtime                         | Pending Head | Pending Body |
| ------------------------------------- | ------------ | ------------ |
| Axum                                  | 202.668 ms   | 201.179 ms   |
| Repaired Viceroy 0.21.0               | 201.182 ms   | 201.194 ms   |
| Spin 4.1.0 / signed SDK revision      | 202.133 ms   | 202.983 ms   |
| workerd 2026-09-10 / Wrangler 4.131.0 | 202 ms       | 204 ms       |

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

| Local Runtime           | Mapped B Write Interval | CPU Interval    | B Outcome  |
| ----------------------- | ----------------------- | --------------- | ---------- |
| Axum                    | 533.322..542.720 ms     | 523.207..560 ms | Unresolved |
| Repaired Viceroy 0.21.0 | 532.918..542.897 ms     | 520.164..560 ms | Unresolved |
| Spin 4.1.0              | 529.763..537.807 ms     | 521.943..560 ms | Unresolved |
| workerd 2026-09-10      | 532.657..539.969 ms     | 521..560 ms     | Unresolved |

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

| Local Runtime           | B Write Interval In Guest Time | CPU Case                       |
| ----------------------- | ------------------------------ | ------------------------------ |
| Axum                    | 531.190..537.288 ms            | Fail: source window not proved |
| Repaired Viceroy 0.21.0 | 530.010..537.326 ms            | Fail: source window not proved |
| Spin 4.1.0              | 530.770..537.093 ms            | Fail: source window not proved |
| workerd 2026-09-10      | 528.539..532.639 ms            | Pass, unqualified              |

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

### D6 Readiness-Driven Test Origin Checkpoint (2026-10-28)

The requester approved replacing the test origin's accept polling after `0d09236a`.
Use one private readiness owner per listener: a registered nonblocking Mio listener, `Poll`,
two-event buffer and one shared waker. Both are prepared before spawning either collector.
Mio `=1.2.1` is pinned only in the isolated, non-published harness workspace, with default
features disabled. Its lock adds only Mio/WASI; production and guest dependency graphs and
SDK/runtime pins are unchanged. There is no alternative collector or synthetic TCP wake path.

Drain accepts through `WouldBlock`, handle interrupted turns, and recheck sticky stop under the
accounting mutex before admitting/spawning work. Stop releases existing gates, drains both
wake handles once and attempts every wake outside the mutex, retaining fixed failure categories.
Each collector holds its own wake owner through exit. A 50 ms poll fallback prevents indefinite
waiting after a missed/failed wake; it is not a realtime or whole-shutdown guarantee. Preserve
the two collectors, 128 cumulative/eight-active limits, 5-second I/O and 6-second gate budgets,
collector-before-worker joins and late-error validation. Partial startup cleanup remains owned
by `Drop`; forced failure of the second OS thread spawn was reviewed, not directly injected.

Colocated tests cover queued readiness/draining, wake-before-poll and wait races, idle
finish/drop with zero synthetic connections, stop winning after accept, both global limits,
partial headers and failed wake fallback. The bounded waiting-collector count is published
after the last stop check and before an unconditional poll. This lets the failed-wake test
place both collectors beyond that check rather than passing through an early-stop shortcut.
Both fallback tests have watchdog channels, real-waker rescue and joins. Removing the poll
timeout made both fail cleanly at approximately two seconds; restoring 50 ms returns green.
The 238 harness tests (one ignored), strict Clippy and formatting pass. Existing CI jobs already
fetch/build/test this workspace; no per-case workflow list or production test mode was added.
All five root Rust gates pass, including 2,568 workspace tests (one ignored), along with 48
qualification-enabled guest tests, docs lint/format, legacy/docs/inventory guards and the 36
evidence-checker regressions. Independent source and documentation reviews are clear.

All four ordinary outbound-wire suites pass: 68 exchanges and 84 correlated origin requests
per target, with raw framing, lifecycle and resource assertions unchanged. Artifacts are under
`/private/tmp/edgezero-ready-wire-20261028/{axum,viceroy,spin,workerd}`. This is local regression
evidence, not hosted qualification.

Final working-tree lifetime characterizations remain mixed and retain the strict targets:

| Local Runtime           | Fast Origin | CPU B Write Interval | CPU Case          |
| ----------------------- | ----------- | -------------------- | ----------------- |
| Axum                    | Pass        | 532.829..535.181 ms  | Fail              |
| Repaired Viceroy 0.21.0 | Pass        | 526.077..535.802 ms  | Fail              |
| Spin 4.1.0              | Fail        | 531.678..534.442 ms  | Pass, unqualified |
| workerd 2026-09-10      | Fail        | 531.097..535.313 ms  | Fail              |

All four controls pass on every target; slow-origin misses remain on every target. All four
complete seven-case suites exit 1, and every B slot stays unresolved. Preserve the actual
all-offset intervals instead of selecting passing earlier samples or relaxing tolerances.
Artifacts are under `/private/tmp/edgezero-ready-accept-final-20261028/{axum,viceroy,spin,workerd}`;
earlier pre-review runs remain separately under `/private/tmp/edgezero-ready-accept-20261028`.
Removing artificial accept delay does not establish sufficient clock precision or a scheduler
guarantee, nor does a failed source-timing precondition establish a production outbound defect.

The fixture-polling work is implemented, but D6 timing/native SDK body-entry evidence and hosted
qualification remain open. Inventory remains 32 ready / 479 planned, unqualified. No frozen
requirement/interface, capability, production code or SDK/runtime pin changes; no deployment,
candidate freeze or qualification promotion.

### CI Diagnostic Follow-Up To Readiness Checkpoint (2026-10-28)

At `d3f48564`, workspace CI job `123098227714` in run `40768269499` failed in the pre-existing
request-fidelity runner's `version_diagnostic_includes_expected_and_actual` test, before the new
shared-harness tests ran. The assertion hid the returned error. Missing `expected-pin` shows
that command execution failed before version comparison, but the log does not establish the
underlying OS/process error. The exact nonzero test passes locally; all 20 runner tests pass
locally with one ignored, as do its formatting and strict Clippy checks.

Add the error's debug representation to both assertions, without changing their conditions,
subprocess execution, version policy, retries or cancellation. Independent reviews found no
concrete cross-test cancellation or overwrite. A possible transient spawn failure remains a
hypothesis until CI exposes the actual error; do not report the CI failure fixed from a local
pass or silently replace the integration test. Fresh hosted checks remain required.

### D6 Fourth-Slot Timeout Acceptance Checkpoint (2026-10-28)

Continue the approved D6 acceptance implementation with EZ-6 criterion 14, without changing the
original three-slot slow-origin case. The distinct `timeout-before-cutoff` case preserves origin
Begin at 2 ms, batch start 34 ms, cutoff 534 ms, A/B/C schedules and the chunked origin. Only a
fourth slot D receives a 300 ms per-call timeout; no absolute request deadline replaces it.
D's source sends no bytes and closes boundedly at scenario 1500 ms. Late checks must return
indices 0, 1 and 3 with elapsed 146/286/300 ms, exact `PerCallTimeout`/504 for D, then `Cutoff`.
Collection preserves three `AlreadyDelivered` positions and unresolved C. Existing worker,
connection, event, body and I/O bounds remain unchanged; no additional transport or scheduler.

Tests first fail for the absent case, then cover request construction, source silence, typed
timeout provenance, non-consecutive identity and lossless collection. A separate diagnostic
`timeout_contract` subresult is evaluated even if strict source timing fails; overall acceptance
still requires both and never substitutes scheduled time for actual observations. Review found
missing late-C validation in both slow cases; a red missing-terminal mutation then drove exact
900 ms head/payload/terminal checks, including supported late peer-close shapes without early
payload. Independent specification, source/cleanup and code-quality reviews are clear.

All five root Rust gates pass, including 2,568 tests in 32 binaries (one ignored). The complete
TCP-enabled harness has 243 passing tests (one ignored), the qualification guest has 49, and
strict harness/guest Clippy, formatting, docs lint/format, legacy/docs/inventory guards and 36
evidence-checker regressions pass. An initial sandboxed full harness run denied existing TCP
binds; the unchanged loopback-enabled rerun passes.

Final reviewed local runs preserve the entire eight-case suite and all strict timing checks:

| Local Runtime           | Timeout Contract Diagnostic | Complete Fourth-Slot Case          | Complete Suite |
| ----------------------- | --------------------------- | ---------------------------------- | -------------- |
| Axum                    | Pass                        | Fail: origin EOF at 1305.325 ms    | Exit 1         |
| Repaired Viceroy 0.21.0 | Pass                        | Fail: B source write 5.033 ms late | Exit 1         |
| Spin 4.1.0              | Pass                        | Pass, unqualified                  | Exit 1         |
| workerd 2026-09-10      | Pass                        | Fail: origin EOF at 1306 ms        | Exit 1         |

D's elapsed is exactly 300 ms on all four, with `PerCallTimeout`, indices 0/1/3 and the required
lossless collection states. All four quiet/pending-cutoff controls pass on every target. Slow
origin still fails on every target; CPU passes locally on Axum/repaired Viceroy/Spin and fails
the B write-interval precondition on workerd. Artifacts remain at
`/private/tmp/edgezero-timeout-reviewed-20261028/{axum,viceroy,spin,workerd}`. Earlier Axum and
stock-Viceroy characterizations are kept separately; no passing earlier sample replaces a final
failure. These observations do not qualify criterion 14 or promote any inventory leg.

An isolated native diagnostic reproduced condition-variable wake overshoot of 0.630..5.040 ms
over twelve waits, enough to invalidate a strict source-write target before any transport
comparison. This is not proof of a production defect or a reason to relax the frozen targets.
No further timer workaround is added. Actual SDK body-entry evidence still requires a proved
observation of the same buffered batch; native call-site inspection is not that witness.
G1 remains 32 ready / 479 planned and unqualified; no candidate freeze, deployment, requirement
change, capability promotion or provider/runtime pin change is authorized by this checkpoint.

### D6 Buffered Cap Acceptance Checkpoint (2026-10-28)

Continue the approved D6 acceptance implementation with EZ-6 criterion 9. The distinct ninth
`caps-while-waiting` case preserves three slots, batch start 34 ms, cutoff 534 ms and the original
per-request budgets. A receives 128 KiB decoded gzip over 300 ms under independent 1 MiB decoded,
64 KiB buffered and 256 KiB encoded limits. B/C return their healthy one-byte responses. Immediate
collection must preserve both siblings and A's original `ResponseTooLarge`/`BufferedBody`/502.
Capture `slot_error` before report-body consumption so the reporter's own 64-byte read limit
cannot manufacture a passing production error. No production outbound implementation changes.

The native-only pinned harness encoder emits one gzip member in eight flushed 16 KiB plaintext
pieces, with payload in the final scheduled write. Logical encoded size is bounded at 129 KiB;
this is not a decoder/RSS or provider allocation certificate. Count partial writes exactly and
bound interrupted retries. Accept recognized peer-close errors only after a successfully written
prefix decodes beyond 64 KiB. Retain existing worker, connection and 32-event bounds; this case
records at most twenty events. No guest dependency, transport, scheduler or runtime pin is added.

Source scheduling uses the observed body start. The exact stored host anchor timestamps guest
response return before worker drain, proving the over-cap prefix and both healthy payloads were
written before return, and return preceded the source's 300 ms ending. Later socket shutdown is
allowed. Require guest arrival elapsed values to admit one common epoch inside the measured
batch-invocation interval, no later than collection. No host/guest clock alignment is inferred.

Tests first fail for the absent case and provenance, then cover source boundaries, round-trip
payload, exact-cap/first-excess prefixes, partial writes and bounded retries. Independent reviews
found an incorrect schedule label, bunched source writes, impossible elapsed values and siblings
written after response return; separate failing mutations drive each correction. Final source,
guest and validator reviews report no remaining findings. Full harness tests pass 252 with one
ignored, qualification guest tests pass 51, and all five root Rust gates pass, including 2,568
tests in 32 binaries with one ignored. Strict harness and all four qualification-entrypoint
Clippy checks, root/harness/fixture formatting, docs lint/format, legacy/docs/inventory guards,
shared dependency and decoder/map audits, and 36 evidence-checker regressions pass.

Final reviewed local runs retain the complete nine-case suite and unchanged strict timing:

| Local Runtime           | Cap Case                         | A Elapsed  | Return After Body Start | Complete Suite |
| ----------------------- | -------------------------------- | ---------- | ----------------------- | -------------- |
| Axum                    | Pass, unqualified                | 177.576 ms | 173.272 ms              | 8/9; exit 1    |
| Repaired Viceroy 0.21.0 | Pass, unqualified                | 177.294 ms | 172.835 ms              | 7/9; exit 1    |
| Spin 4.1.0              | Pass, unqualified                | 177.719 ms | 174.463 ms              | 7/9; exit 1    |
| workerd 2026-09-10      | Fail: source write 5.055 ms late | 178 ms     | 174.285 ms              | 6/9; exit 1    |

All four return the original typed cap failure with both healthy siblings and peer-close only
after a confirmed over-cap prefix. That does not waive workerd's failed source cadence. Axum
also fails the CPU B-write interval; Viceroy fails slow/fast-origin head schedules; Spin fails
slow-origin head and CPU B-write interval; workerd fails slow-origin head and fourth-slot origin
EOF timing. Preserve these actual failures instead of replacing them with earlier passing runs.
Artifacts are `/private/tmp/edgezero-d6-caps-final-20261028/{axum,viceroy,spin,workerd}`; initial
and intermediate reviewed runs remain separately under `edgezero-d6-caps-runtime-20261028` and
`edgezero-d6-caps-reviewed-20261028`. The initial workerd preflight without its explicitly selected
binary remains an unsupported exit 2, not runtime evidence.

This checkpoint does not close D6 or any qualification leg. Native SDK pending-body-entry
witnesses, remaining response-owned/cleanup/reuse scenarios and strict timing evidence remain
open. G1 remains 32 ready / 479 planned and unqualified. No frozen requirement/interface change,
capability promotion, candidate freeze, deployment or provider/runtime pin change is authorized.

### D6 Core Receipt Acceptance Binding (2026-10-28)

Activate only `EZ-6/acceptance/22@unit/core`, replacing its placeholder with five existing
receipt/accumulator tests. Strengthen the identity test to six literal-three-slot runs:
Completed, Cutoff and an original typed driver failure, each with fresh collection and with
check(0)/next(1)/collect. Gate terminal production until both mixed receipts transfer; an
immediately ready failed driver would not prove failure after delivery. Retain the separate
out-of-order delivery test unchanged, and preserve the earlier four-slot cutoff/failure runs,
including merging a newly collected third partial with the two previously delivered receipts.

Assert the exact pre-merge `AlreadyDelivered` positions, third-slot state and original error
identity before and after merge. No body is consumed during receipt inspection/collection;
each original payload is read once after merge and its owner releases once. Companion tests
prove missing/foreign recovery and retain both inputs through duplicate, conflicting-slot and
invalid-index/count rejection, including the original payload and conflicting error. Shared
test-only setup/assertion helpers keep individual proofs within the existing lint limits.
No production API, behavior, transport, scheduler, dependency or runtime pin changes.

The inventory guard discovers 56 exact core sentinels: the original 51 redirect proofs plus
five receipt proofs. Broaden the one existing CI execution to `--lib outbound::`, and align
all fifteen redirect bindings with the shared `outbound/unit/core/raw.log` output. Do not add
per-test CI commands, a second guard or an exporter. The old redirect-only filter is verified
to fail before execution because it omits all five batch sentinels. The new binding regression
first fails on the planned placeholder, then passes after activation. Parametric guard tests
retain Axum coverage and reject every missing core sentinel, execute once and preserve the
original suite exit status.

Final reviewed verification passes all five root Rust gates, including 2,568 tests across 32
binaries with one existing ignored. The grouped outbound guard executes 296 tests; 37 evidence
checker and 14 nonzero-guard tests pass, including omission of every required core sentinel.
Root formatting and strict Clippy, workflow/modified-script/inventory formatting, docs lint/format,
legacy/docs/inventory guards and dependency/decoder/map audits pass. The first sandboxed workspace
run denied existing Axum TCP binds; its unchanged loopback-enabled rerun and final reviewed run
pass. Specification, tooling and code-quality reviews are clear after adding explicit merged
cardinality and post-merge termination assertions and retaining the earlier four-slot proofs.
Local logs are `/private/tmp/edgezero-d6-mixed-*`; none is a frozen-candidate or hosted record.

Readiness is now 33 ready / 478 planned across the unchanged 255 criteria / 511 legs. It is
not qualification or applicability approval. All four runtime legs of criterion 22 remain
planned; prior logs are not relabelled as evidence for the broadened command. This checkpoint
does not close D6, criteria 20/21/23, native pending-body witnesses, strict timing failures or
hosted qualification. No candidate freeze or deployment is authorized.

Read-only ownership review confirms why those existing legs remain open: retaining work through
handoff is not successful collection of both a pending begun origin and batch inside the body;
shared callback-scope unit coverage is not the complete two-wrapper/Viceroy sequence. A native
pending-body witness must observe the actual SDK read future, or Fastly's distinctly labelled
body-readiness wait, not the outer batch wait or a synthetic yield. No blocking read fallback
or additional production observer is introduced by this core-only binding.

### D6 Response Handoff Checkpoint (2026-10-29)

Implement EZ-6 criterion 20 in the existing lifetime fixture and native runner, without changing
production outbound APIs, ownership, SDK/runtime pins or frozen requirements. One provisional
response-body owner starts the original streamed origin at 2 ms and the three-slot batch at
34 ms with the original 534 ms observation cutoff and per-request caps/timeouts. Before handler
return both are pending. Attach that exact owner to the NDJSON response and collect both
original handles inside its body; no replay, extra witness fetch, scheduler or production hook.

All four origin heads, data and EOF wait behind one source latch. The native client's existing
HTTP reader validates the complete canonical prelude, status/media type, token, pending flags,
closed callback and live body scope before opening it. Cloudflare/Spin's existing fixture return
flag is now a wakeable signal marked after `dispatch_app` returns; the prefix awaits that exact
boundary. Axum's native handoff and closed callback provide its witness. This is EdgeZero
dispatch return, not literal exported callback return or confirmed hosted delivery.

Bound the prelude to 512 bytes and final report to 16 KiB, each including newline; require exactly
two records. Reuse fixed/chunked/close-delimited capture, stop at the complete prefix even within
a larger HTTP chunk, reject ambiguous framing before notification and retain original partial
captures on rejection. Four source roles produce 22 events plus one runner return boundary,
within unchanged event/connection/worker budgets. Verify actual write-start order after release,
not just completed writes. Allow late request receipts and socket cleanup. One common batch
epoch must put every successful arrival after the pending check and strictly before the original
cutoff; collection after that cutoff remains valid for already recorded on-time arrivals.

The actual-route unit covers native and signalled entrypoints with four preexisting resource
owners, original handles/call counts, no cancellation/replay, EOF before family release and one
terminal family release. Negative controls reject forged/duplicate/missing preludes, wrong
dispatch/scope evidence, premature/failed/reordered source phases, impossible arrival histories
and bounded-record overflow. Source-drain errors cannot replace the primary client failure or
omit its case/partial evidence. Reuse shared settlement to retain client completions and logs
after runtime/origin cleanup. No alternate parser, executor or transport is introduced.

Reviewed local artifacts are `/private/tmp/edgezero-handoff-reviewed/{axum,viceroy,spin,workerd}`.
The handoff case passes on Axum, Spin 4.1.0 and local workerd; repaired Viceroy 0.21.0 explicitly
reports it inapplicable under criterion 20 and executes the original nine cases. Full suites
remain nonzero: Axum 5/10, Viceroy 5/9, Spin 6/10 and workerd 5/10. All four miss strict slow/fast
or fourth-slot source schedules. Axum/workerd also miss arrival targets in CPU-stretch. All four
cap cases retain the original typed `BufferedBody` failure and healthy siblings but fail source
cadence: first late pieces are 6.181, 6.781, 9.605 and 5.916 ms late respectively against the
unchanged 5 ms bound. A passing timeout diagnostic or handoff cannot pass those complete suites.
Earlier artifacts under `/private/tmp/edgezero-handoff-initial` and
`/private/tmp/edgezero-handoff-final` remain retained, including the earlier Spin 205 ms cutoff
miss; no passing retry substitutes for a failed observation.

Verification: all five root Rust gates pass (2,568 tests across 32 binaries, one existing ignored),
263 native harness tests pass (one ignored), 54 fixture-core tests pass, and strict harness/core/
four-entrypoint Clippy passes. All 51 evidence-checker/nonzero-guard tests, docs lint/format,
legacy/docs/inventory guards, shared pins and decoder/map dependency audits pass. The independently
reviewed helper and evidence fixes add no production behavior.
All commands remain local-only. G1 stays 33 ready / 478 planned across 255 criteria / 511 legs,
unqualified; criterion 20's inventory legs are not promoted by a partial parent suite. D6,
criteria 21/23, actual native pending-body witnesses, strict timing closure, candidate freeze and
authorized hosted qualification remain open. No deployment is authorized.

### D6 Response Cleanup Checkpoint (2026-10-29)

Add the Fastly normal-completion/same-client isolation portion of EZ-6 criterion 21 to the
existing lifetime suite. Use the real client and existing `run_custom` serving callback, with
non-nested executor phases around the canonical request/response service. Begin a callback-owned
exchange first; start one response-owned Begin and the original three-slot batch under an exact
provisional body owner. Keep their public handles outside the streamed response through egress.
One bounded same-client readiness Send waits until all five original source requests are waiting,
establishing the precondition before egress. This is fixture orchestration, not consumer policy
or another transport. Request timeouts and the batch cutoff interval remain three seconds;
buffered results and inspected bodies are capped at 64 bytes, and the retained stream is never
drained. The existing prepare hook injects the same client into the canonical request.

After canonical response completion, verify one terminal report, closed body ownership, typed
scope errors on the retained body-owned Begin/Batch, and a still-pending live callback sibling.
Only after the native client validates the complete response and canonical `cleaned` evidence
does the source gate open. The original callback exchange then returns K, the same client sends
fresh post-delivery work returning P, and callback return closes its scope. Require all three
ordered protocol records: `cleaned`, `continued`, `returned`. Keep records at most 2048 bytes and
the native log reader at most 1 MiB. Exact canonical JSON rejects malformed, duplicate, foreign,
reordered, extra and truncated protocol records; partial third records may remain pending, but
an incomplete fourth/prefix cannot pass final validation. Critical gate decisions do not use the
existing permissive runtime-log parser. Source and client failures remain separate and recorded.

Seven source roles produce 25 events plus one native response/cleanup-observation boundary,
within unchanged 32-event/eight-active/128-cumulative budgets. Use actual write-start causality:
all five pending receipts precede readiness, the complete response and validated cleanup precede
release, and callback/post-delivery payloads follow it. The four body-owned sockets close only
because the fixture releases them. Their `fixture_released` events do not prove guest or host
cancellation, and the typed post-terminal handle errors do not independently count SDK drops.

Extend the existing actual-route fixture unit, retaining its successful native/signalled handoff
paths. Inject Completed, ClientDisconnected, DeadlineExceeded, SourceError and RequestCancelled
while the writing attempt, body wrapper and original jobs remain retained. Assert four preexisting
resource-owner releases, one Begin cancellation, one batch-group release, one family release,
cancelled receivers, no further inner producer polls and one terminal report before dropping the
attempt itself. Later attempt/body/sender drops cannot defer or duplicate cleanup. The separate
implicit attempt-drop row records TransportError. These are core terminal-injection/mock-resource
proofs, not evidence that an adapter detects those failure paths or bounds SDK/host teardown.

The initial full harness run exposed a test-only readiness assumption: one listener wake does
not guarantee three immediate nonblocking accepts. Retain that failure log, and fix only the
test to retry WouldBlock/Interrupted under one absolute two-second deadline, preserving explicit
listener readiness in both phases, drain-to-WouldBlock and peer-address assertions. Production
Acceptor and all source timing bounds are unchanged. Three focused repetitions and the full
normal-parallel harness then pass. No sleeps or larger timing allowance are introduced.

Final local artifacts are `/private/tmp/edgezero-cleanup-reviewed/{axum,viceroy,spin,workerd}`.
The Fastly cleanup case passes on repaired Viceroy 0.21.0; the prior asynchronous handoff case
passes on Axum, Spin 4.1.0 and workerd. All full suites remain exit 1: Axum 7/10, Viceroy 7/10,
Spin 7/10, workerd 6/10. Axum misses slow/fourth-slot/fast source schedules. Viceroy misses slow/
fast source heads and the CPU B-write interval (530.578..535.658 ms). Spin misses slow/fourth-slot
source schedules; workerd misses slow/fourth-slot schedules and fast batch observation. Both also
report `inconsistent caps write progress`: measured late pieces include 5.039 ms on Spin and
11.482/61.875 ms on workerd against the unchanged 5 ms bound. That shared error need not imply
only a cadence failure. The earlier `/private/tmp/edgezero-cleanup-local-viceroy` cleanup pass/full-suite
failure and `/private/tmp/edgezero-cleanup-harness-tests.log` readiness failure remain retained;
no later passing observation substitutes for them.

All five root Rust gates pass: 2,568 tests across 32 binaries, one existing ignored. The reviewed
harness passes 267 tests (one ignored), fixture-core passes 55, and strict harness/fixture-core/
four-entrypoint Clippy passes. Guest/source, protocol/specification and unit/code-quality reviews
preserve the explicit limits above. Docs lint/format, legacy/docs/inventory guards, all 51 Node
guard regressions, shared dependency pins and decoder/map audits pass.
No production API, scheduler, transport, SDK/runtime pin, capability, frozen requirement or
deployment change. The demo and generator need no migration for this fixture-only addition.
G1 stays 33 ready / 478 planned across 255 criteria / 511 legs, unqualified. Full criterion 21's
adapter units and observable disconnect/deadline/source/cancellation runtime paths, both Fastly
wrapper sequences in criterion 23, native body-entry witnesses, strict timing closure, candidate
freeze and authorized hosted/consumer qualification remain open. This does not close D6.

### D6 Response Cleanup Failure Checkpoint (2026-10-29)

Extend the existing Fastly cleanup helper, not its production delivery or client APIs. One
validated private header selects normal completion, intentional source failure or a real
one-second egress-start-relative write deadline. All retain the original response-owned Begin
and three-slot batch outside the body; their three-second logical budgets are never renewed.
Keep the original callback-owned exchange pending through terminal egress, then require its K
payload and a fresh same-client P exchange before callback return closes that scope.

Failure bodies first emit an exact canonical JSON prefix, bounded to 512 bytes including newline.
Only a native observer that validates the response head and complete prefix may acknowledge the
new prefix gate. A bounded same-client ACK Send waits on that gate; acknowledgment never releases
the original sources. The source case then emits a real error and records its intentional branch.
The deadline case actually polls the retained original batch to Pending until the response timer
wins. Canonical delivery must be routed and committed, with the matching typed outcome/error,
exact accepted prefix byte count, application body, no fallback/framing failures and one report.
Require application/internal source categorization only for SourceError; require no source failure,
elapsed at least one second and a still-live original batch cutoff for DeadlineExceeded.

The shared native reader now carries a typed interruption origin and moves its existing head/body
into partial-response evidence. Premature EOF or recognized peer closure after the exact prefix
is required for both failed-delivery cases. Reader timeouts, invalid framing, capture overflow,
observer rejection, missing prefixes and complete HTTP success cannot substitute. Chunk-header
and trailer CRLF and each data-delimiter byte are validated before accepting an interrupted read.
The original source gate opens only after this wire evidence and the canonical `cleaned` record
validate. Normal delivery remains a complete-response case. Both failures require callback and
post-delivery success just as normal completion does.

One cursor spans all three cases under the unchanged total 1 MiB log ceiling. Advance only through
validated complete lines, retain partial lines, track observed-length high water and reject
shrinkage even above the consumed offset. After the shared runner stops and joins the runtime,
check the final tail for late protocol records/fragments and overflow, including when a primary
case failed. Record tail errors separately without hiding the primary suite failure. Three review
regressions first failed: malformed partial chunk delimiters appeared as EOF, unconsumed-tail
truncation was undetected, and final-case late records escaped validation. Focused negatives and
the full harness now pass; separate tail baselines and exact error assertions prevent one failure
mode from masking another. The normal source still has 25 events; failures add three ACK events
and one acknowledgment, for 29. One native boundary makes 26/30, within the unchanged 32-event,
eight-active/128-cumulative connection bounds. Fixture release still closes the four body-owned
source sockets; none of this counts SDK drops or certifies host cancellation.

Final serial local artifacts are `/private/tmp/edgezero-cleanup-faults-reviewed-{axum,fastly,spin,cloudflare}`.
All three Fastly cleanup cases pass on repaired Viceroy 0.21.0, with both failed responses ending
in typed premature EOF after the exact prefix and the post-shutdown log-tail check passing.
The asynchronous handoff case still passes on Axum, Spin 4.1.0 and workerd. All full suites remain
exit 1, with every failure retained rather than waived:

| Runtime          | Cases Passed | Remaining Strict Failures                                                                                                            |
| ---------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| Axum             | 7/10         | Fourth-slot origin head 726.755 ms; fast origin head 126.828 ms; CPU B-write interval 533.363..535.854 ms.                           |
| Repaired Viceroy | 11/12        | Slow-origin head 708.790 ms.                                                                                                         |
| Spin             | 8/10         | Slow-origin head 712.909 ms; CPU B-write interval 533.715..536.344 ms.                                                               |
| workerd          | 7/10         | Slow-origin head 714 ms; bidder A head in the fourth-slot case at 187.157 ms against 182.119 +/- 5 ms; CPU batch start misses 34 ms. |

The initial `/private/tmp/edgezero-cleanup-faults-fastly-1` run also passed all three cleanup cases
and failed slow-origin head at 706.043 ms. Preserve that earlier failure. Final runtime checks
used the reviewed reader/cursor and ran serially without competing compilation. No hosted run
or deployment occurred. Source cadence, guest timing and qualification thresholds are unchanged.

All five standard root gates pass, with `--offline --locked` on Rust compilation/test commands:
root formatting, strict all-feature/all-target Clippy, workspace tests, all-adapter feature check,
and Spin WASIp2 check. Workspace tests pass 2,568 tests across 32 binaries with one existing
ignored test. The first sandboxed workspace run failed solely on denied local TCP listeners;
the socket-enabled rerun passes. The final harness passes 275 tests with one ignored;
fixture-core passes 55 and the native guest mode-parser test passes one. Strict harness,
fixture-core and all four actual-target entrypoint Clippy checks pass. Standalone formatting,
docs lint/format, legacy/docs/inventory guards, 51 Node guard regressions, shared dependency pins
and decoder/map audits pass. Native protocol/source
and guest/ownership reviews preserve the scope above. D6 and full criterion 21 remain open for
the other adapter failure paths, observable disconnect/cancellation, actual SDK resource/poll
witnesses, both Fastly wrappers in criterion 23, native pending-body witnesses and strict timing.
G1 stays 33 ready / 478 planned across 255 criteria / 511 legs, unqualified. No frozen requirement,
capability, SDK/runtime pin, production implementation, demo or generator change is needed.

### Local Lifecycle Matrix And Binding Checkpoint (2026-10-29)

Continue local D6 acceptance without changing production outbound APIs or semantics. Reuse the
existing Fastly custom-single-request and custom-rebuild-per-request binaries, selected internally
by the current progress builder; do not pre-read the serving-loop request or add another binary,
executor or transport. One shared guest probe covers normal post-delivery return, post-delivery
error, early error and setup failure under both `run_custom` and `serve_custom`. Each serving loop
is limited to one request; this is not sandbox-reuse adoption or next-request evidence.

Keep the original unpolled Send, begun origin and three-slot batch in state outside the callback.
Four original source receipts precede readiness. After-delivery cases additionally start a pending
post-delivery exchange on the same client and confirm its receipt before callback exit. Before
consuming any original handle, verify closed callback scope, one request-data release, zero
unpolled source polls and disposal of every fixture-wrapped native timer. Original handles yield
typed Internal; the batch preserves three unresolved slots. Later handle consumption cannot change
those counters. These are request-data/timer observations, not counts of SDK handle drops.

Share the prior response-cleanup parser/cursor rather than adding another log parser. Three exact
canonical records per case (`pending`, `returning`, `closed`) precede original source release,
under the unchanged 2048-byte record / total 1 MiB log limits, observed-length checks and
post-shutdown tail validation. Early/after-delivery source evidence uses 16/22 events plus a
native boundary, within unchanged resource limits. Parent diagnostics aggregate original timing,
response-tail and both-wrapper failures, preserving every error and raw observation.

The first local run exposed a fixture error: after checking the expected callback error, it
returned success from `main`, producing an empty provider response. Propagate the original error
after cleanup verification instead; do not weaken the wire assertion. Retain that failed run at
`/private/tmp/edgezero-finish-callback-viceroy-1`. The repaired run at
`/private/tmp/edgezero-finish-callback-viceroy-2` passes all eight callback guest-cleanup cases,
both callback-log tails and all three response-cleanup cases. Its full suite remains exit 1 for
the original slow-origin observation at 706.217 ms against the unchanged 705 ms upper bound.
After-delivery responses retain their canonical body. Before delivery, a typed no-head peer close
is distinct from `UnverifiedHttp500`; every observed 500 has
`no_replacement_response_proven: false`. Do not count it as evidence that no replacement was sent.

Strengthen existing adapter-local helpers, not parallel mock transports. Axum retains real native
Begin/Batch sockets through EOF, source failure, connection-deadline signalling and transport
failure, checks original handles non-consumingly before termination, and observes TCP close before
consuming their typed scope errors. Its explicit timer driver waits on the registered connection
deadline; an outer safety-timeout wake is not expiry proof. Assert the four-byte terminal report
and source release while the body/handles remain retained, then stable counts after later drops.
Cloudflare's actual SDK/JS matrix reads a prefix before EOF, source failure, one-second deadline,
request AbortSignal or readable cancellation with another write eligible. It checks three abort
notifications, typed retained Begin/Batch errors, disposed source receiver and stable poll/report
counts. These are JS-runtime observations, not hosted transport teardown or immediate detection
of a disconnected readable while the producer remains blocked.

Spin extends its existing private mock client/committer, retaining an original begun request and
three-slot batch alongside the unpolled Send. Four mock guards and both cancellation notifications
remain live until the appropriate terminal boundary, with explicit forbidden-Send dispatch checks.
Completion observes their disposal before retained handles are consumed. HEAD/EOF do not release
early; fallback revokes application work before SDK-terminal simulation. Source/deadline/write/
finish errors and coordinator cancellation at every await share the same assertions. This is
mock-resource/core revocation coverage, not real Spin SDK-drop or scheduling qualification.

Bind only four already-complete core acceptance legs: EZ-6 criteria 15/17 and EZ-7 criteria 01/03.
Add cutoff-equality, recorded-undelivered cancellation and exact marker-off/upstream-marker tests
to the existing inventory guard, discovering 60 primary/additional sentinels before one unchanged
grouped command. It passes 296 tests. Node regressions first reject the missing bindings, then
validate 37 ready / 474 planned across the same 255 criteria / 511 legs. Preserve every source
digest, applicability and approval; neither callback partial coverage nor counts promote a leg.
Refresh the stale G4 prerequisite description to name existing scoped authorizations while keeping
its remaining policy/qualification decisions pending.

Independent timing investigation found no demonstrated production defect in the saved misses:
cold receipt-minus-guest alignment includes connection delay; some source writes miss their own
schedule; CPU correlation intervals cross the unchanged acceptance boundary. Preserve those
failures and the original capture points. Do not backdate results, sort outcomes to fit a desired
order, replace interval proof with a midpoint, widen targets or claim a scheduler guarantee.

Demo and full generated-project integration checks pass with current hard-cut APIs. The new code
is test/fixture/binding work; no production signature, SDK pin, capability, runtime promotion,
frozen requirement or deployment changes.

Final verification passes all five root gates: formatting, strict workspace Clippy, 2,569 tests
in 32 binaries (one existing ignored), workspace feature compilation and the Spin target check.
All seven adapter feature matrices pass all eight configurations each. The consolidated Axum TCP
command passes 294 tests with none ignored; native Spin passes 102. Cloudflare's actual SDK/Node
library runs pass 83/82 with/without test-utils. Fastly's repaired-Viceroy library/contract runs
pass 271/36; pinned Wasmtime 48.0.2 passes Spin's supported SDK-resource/contract targets 21/35.
The first SDK-resource attempt used installed Wasmtime 45.0.0 and fails before tests; preserve
that negative. The standalone Spin library-test component also cannot link Spin-specific KV
imports in Wasmtime, including the pinned runner; its mock handoff units execute natively, not
as claimed SDK execution. Preserve `/private/tmp/edgezero-finish-spin-full-library-host-gap.log`.
Harness tests pass 289 (one ignored), fixture-core 55, native Fastly fixture two, and the guarded
core outbound suite 296. Strict harness, provider-target and changed progress-fixture Clippy,
standalone formatting, 51 Node regressions, docs lint/format/build, legacy/docs/inventory guards,
shared pins and decoder/map audits pass. The checked-in demo passes 66 tests; the generated
workspace integration runs its 33 generated tests and all three provider builds.

After compilation stops, single serial runs at
`/private/tmp/edgezero-finish-final-{axum,fastly,spin,cloudflare}-lifetimes` retain Axum 8/10,
repaired Viceroy 18/20, Spin 6/10 and workerd 8/10; every full suite is exit 1. All eight Fastly
callback cases, both callback tails, all three response-cleanup cases and the main tail pass.
Retain the exact strict scheduling/arrival and CPU-correlation failures. Spin additionally
reports its typed BufferedBody result at guest collection 214.072 ms, but the native diagnostic
response returns at 370.800 ms, after the source's 341.181 ms full-trickle target and 341.827 ms
EOF. Its then-current cap validator therefore fails. Preserve this original artifact and failure;
do not infer a missing cap or attribute the response gap to a provider without proof.

Both independent reviews identify that gate as stronger than acceptance 9: the required boundary
is original slot failure, not diagnostic HTTP delivery. Correct the existing validator using
one-sided causal request-receipt bounds and the original elapsed values. Intersect one common
batch epoch with collection, payload-write and native-response constraints for all three slots.
Collection completion must itself precede native HTTP receipt under that same offset; a paired
negative/positive regression reproduces and closes the initially omitted joint constraint.
Require the worst-case cap terminal upper bound strictly before the unchanged body-start-plus-
300 ms boundary; retain all source cadence, prefix, typed outcome and capture checks. Round
invocation/collection outward by one microsecond and use the existing additional 1 ms clock
guard, never a midpoint or a later socket close. Correlation explicitly reports
`same_rate_assumed: true` and `clock_sync_proven: false`: monotonicity alone cannot prove equal
rates. This is conditional local evidence, not a provider timing guarantee. Eight new regressions
reproduce five logic failures, then pass with the existing six cap validators. They cover
late diagnostic delivery, equality/overlap, impossible early errors, shared-epoch contradictions,
missing/future stamps, shifted native origins, controlling A/B/C receipts and extreme arithmetic. Repair the positive unit
fixture's formerly impossible pre-prefix arrival rather than accepting contradictory evidence.
The response-delivery gap and uncorrelated Spin shutdown worker-panic line remain unqualified.

After the reviewed correction, one further serial pass at
`/private/tmp/edgezero-finish-postcaps-{axum,fastly,spin,cloudflare}-lifetimes` records Axum 5/10,
repaired Viceroy 19/20, Spin 7/10 and workerd 6/10. All four full suites remain exit 1; retain
these observations alongside, not in place of, the earlier captures. All eight callback cases,
both callback tails, all three response-cleanup cases and the main tail pass again. The cap
terminal upper bounds precede the original window end on every target, but Axum's healthy-sibling
cadence and workerd's counted-write cadence still fail their unchanged checks. Their complete
cap cases remain failed. Spin's new cap case passes with terminal upper 216.155 ms before the
340.908 ms window end; this neither changes nor explains its older delayed HTTP response.
Other strict arrival, scheduling, pending-cutoff and CPU-interval misses remain open. No source
or operation timestamp, window, capture point, error category or approval was changed to pass.

Read-only guest/source, timing, adapter ownership and verification reviews found no further
defect in this checkpoint's changes. Full criteria 21/23, native SDK/poll witnesses, all required
exact-candidate bindings, strict timing and hosted/consumer evidence remain open. No candidate
freeze or qualification is supplied by this checkpoint.

### Core Policy Conformance Binding Checkpoint (2026-10-29)

Continue G1/Q3 with seven complete core legs, not another transport or an optional scripted-client
implementation. Independent source/spec reviews find three already-complete coding legs and four
small proof gaps. Add the missing assertions before binding; do not treat a validation-helper
test as proof of actual facade dispatch.

Reuse the existing redirect recording client for EZ-4 criterion 01. A literal one-second connect
setting survives parts round-trip, public readback, plain Send and a two-dispatch Follow. The
policy checks the rebuilt request and advances paired fake time by 500 ms. Each recorded request
still has its full 15-second per-call budget and original 20-second absolute deadline. This proves
carriage and that connect settings do not shorten aggregate budgets, not enforcement of a deadline
that never governs this test. Provider setter mappings remain separate planned legs.

Reuse the existing raw-coding matrix for EZ-5 criterion 03, retaining every four-byte control and
adding gzip, ten invalid-gzip bytes, `Content-Range: bytes 10-19/100` and length ten under both
decoding choices and response modes. Compare all three headers with the pre-normalization input;
decoder allocation zero and decoded cap one cannot affect the partial payload. Bind already-complete
criteria 01/02/10 to their full valid-codec, declared/observed encoded/final cap and identity-cap
sentinel bundles. No metadata-change adoption or bodyless-matrix completion is inferred.

For EZ-6 criterion 13, extend only the existing private settlement client's observability. Its
bounded 64-byte drain records the original payload, method, mode and stream shape without holding
a mutex across an await. Actual Begin refuses a streamed POST before source polling or transport
entry. An unread inbound GET stays unread through conversion, then Begin certifies its empty
payload; recording precedes pending-result collection. Actual Send delivers `abc` from the original
three-chunk POST source, including an empty chunk. Exact poll, dispatch and upload counts prevent
replacement or skipped-work false positives. The 64-byte fixture bound is not production-cap
evidence. Keep every existing settlement/timer/source-error control.

For EZ-7 criterion 02, use one existing MarkerClient-backed off-configured handle. Inherited Send,
Begin and exactly two completed batch slots have no marker; only the explicit-on Send has `test`.
The original mixed-marker matrix stays intact. Successful Begin probes use a direct readiness poll,
not nested executor entry through a synchronous private fixture.

The binding regression first fails on the planned core rows, then passes with exactly seven
implementation/command/sentinel/artifact updates. Preserve all 255 criteria, 511 legs, source
identities, expected text, applicability, conditions and approvals. There are now 44 ready / 467
planned legs, still unqualified. All eight new unique sentinels join the same guarded core command:
68 names discovered before one 299-test execution, with no CI job or command-list growth. The strict
redirect namespace check covers its original 51 tests plus the new connect proof; it is not relaxed
to permit arbitrary extras. All 52 Node regressions pass.

Final verification passes all five root gates: formatting, strict workspace/all-targets/all-features
Clippy, 2,572 tests in 32 binaries (one existing ignored), workspace feature compilation and the
Spin target check. The checked-in demo passes 66 tests in nine binaries. Documentation lint,
format and build, legacy/docs contract checks, manifest validation and diff checks pass. Independent
spec, code-quality and binding/evidence reviews are clear. The previous `52719940` hosted CI run
also completes without reported failures; it is not evidence for a new frozen candidate.

This test-only checkpoint does not rerun or relabel the historical provider timing probes, SDK
qualification, full provider feature matrices or ignored generated-workspace integration gate.
Production interfaces, generator and demo code are unchanged; earlier generator verification is
not presented as a fresh run. The existing strict runtime failures remain failures.

No production API, implementer protocol, SDK/dependency pin, provider capability or runtime policy
changes. The normative requirements and frozen interfaces remain unchanged. EZ-1's complete
forwarding matrix, EZ-5 bodyless/conditional metadata cases, every affected provider leg, SDK/poll
witnesses, strict runtime failures, candidate freeze and authorized hosted/consumer qualification
remain open. No parent task or provider guarantee closes through these core bindings.

### Forwarding And Shared-Head Acceptance Checkpoint (2026-10-29)

Continue P2/P3 and G1/Q3 with three existing core acceptance legs. Independent forwarding and
normalization investigations found missing transport/head assertions, not a need for another
implementation path. Reuse the private dispatch recorder, injected batch clock and shared-head
fixtures. Production APIs, provider behavior, frozen requirements/interfaces and SDK pins stay
unchanged. The existing three-slot ordering and F normalization-precedence tables stay intact.

EZ-1 criterion 07 forwards through `RequestContext::into_request` and `OutboundRequest::from_request`.
Two empty chunks and EOF become an actually empty buffered transport upload through Send and
Begin. Begin explicitly yields between empty chunks before dispatch. A forwarded POST polls its
source only after transport entry and retains its exact three chunks' payload. An already-expired
absolute outbound deadline produces an attributed 504 with no source, timer or transport work.
Empty buffered and taken bodies forward empty without extra reads. A poisoned context fails
before outbound conversion/dispatch with the stored category, status, typed fields, message,
display and provider detail intact; boxed-error identity is not asserted across a sticky snapshot.
The recorder's 64-byte bound remains fixture-only evidence, not a production allocation claim.

EZ-3 criterion 04's core clause uses exactly two slots and an injected clock: `next` delivers
indices 1 then 0 with elapsed values 1 ms then 2 ms, followed by explicit Completed. An equivalent
batch collects exactly two input-aligned slots with elapsed values 2 ms then 1 ms and distinguishable
outcomes. This does not prove Fastly's separate known-origin-before-registration SHOULD.

EZ-5 criterion 11 enters raw fields through `OutboundHead::new`, not a manually normalized copy.
Both decoding choices, both response modes and 200/206 paths cover nominated coding/length/ETag,
malformed nominated length, absent coding, identity decoded-cap overflow and exact-cap success,
opaque ETag/coding, surviving malformed length and unchanged surviving representation fields.
The bodyless matrix retains 1 GiB representation lengths where appropriate despite zero payload
caps and decoder budget. 204 removes length, positive-length 205 preserves declared-body evidence
while normalizing length to zero, and HEAD/304 remain unread. Absent/zero-length 205 settles one
ready source item without decoding. Source and owner release are checked before body collection.
These injected headers establish no new provider raw-header-fidelity guarantee. EZ-5.9 and EZ-5.10
remain separately unadopted; conditional criteria 11b/12 and all adapter legs stay planned.

The binding regression fails first while the three rows are planned, then passes with only
their implementation, command, sentinel bundle and artifact bindings changed. All 255 criteria,
511 legs and source/decision metadata remain unchanged. The inventory now has 47 ready / 464
planned legs, still `qualified: false`. All 80 primary/additional core sentinels share the existing
single grouped command; no CI job or command list is added.

Fresh verification passes all five root Rust gates, including 2,582 workspace tests in 32
binaries (one existing ignored), strict workspace Clippy and the Spin target check. The guarded
core execution discovers all 80 names and passes 309 tests; all 53 evidence/guard regressions
pass. The demo passes 66 tests in nine binaries. Documentation format/lint/build, docs/legacy
contract checks, manifest validation and diff checks pass. Independent spec and quality/binding
reviews are clear. Requirements, frozen interfaces, source/decision metadata and F's precedence
table compare unchanged; only the informational inventory status and three core binding bundles
are updated.

This test-only checkpoint does not close a parent task, provider leg, candidate freeze or
hosted/consumer qualification. Historical strict runtime failures, SDK/poll witnesses and raw
resource-accounting limitations remain open rather than being relabelled by core test results.

### Native Response Policy and Evidence Validation Checkpoint (2026-10-29)

The requester selected the required EZ-1–9 baseline before the four unimplemented optional
extensions. Cache tags, chain-only TLS, idle-body mode and fire-and-forget remain follow-ups;
this checkpoint does not adopt those features, optional representation metadata or P6.

Each adapter's EZ-5 unit criterion 09 and Axum/Cloudflare/Fastly unit criteria 10/11 now have
complete production-head/source matrices.
Both decoding choices and response modes cover bodyless 1 GiB representation lengths with
64 KiB encoded caps, declared/observed identity overflow and exact-cap controls, original gzip
without decoder budget, encoded/final overflow, normalization precedence, opaque fields,
unchanged original/206 representation fields and both 205 dispositions. Inexpressible fields
remain explicitly shared-head injection, not a raw-header-fidelity claim. Buffered overflow
must fail before reporting; a generous reporter cannot supply the expected error. Local source
release is witnessed without claiming host allocation recovery or client delivery.

Axum's real TCP tests bind EZ-1 criteria 01/04/08: GET/HEAD and empty chunked forwarding,
literal five-byte GET rejection with no origin dispatch and safe pipeline boundaries, and
certified-empty forwarding after the inbound deadline. The existing plain Tokio helper adds
Begin against its supplied client for EZ-23 criterion 12; it is not native scheduler proof.
Inventory source text, digests, applicability and decisions stay unchanged: 255 criteria,
511 legs, 61 ready / 450 planned, unqualified. Axum uses one grouped suite across its three
inventory selections. Provider libraries retain one guarded execution each; Spin's existing
eager component emits criterion 09's marker only after its bodyless matrix passes.

The synthetic Spin full-body fixture fails at Wasmtime 48.0.2's unsupported intra-component
non-numeric trailer-future boundary, before a production transport conclusion can be drawn.
Its failed binary SHA-256 is `ff97bc949852c0e7e2a6fd07296b8b36d9619b0a43c97e2e27bf72b215a12f07`;
the unqualified log is `/private/tmp/edgezero-spin-policy-intra-component-failure.log`.
Remove the unusable probe and leave unit criteria 10/11 planned. Extend the existing Spin
host-wire matrix instead, preserving its trailer acknowledgement, original redirect counts and
production converter. No second executor, discarded completion check or runtime-pin change.

Three evidence defects are repaired after failing regression proofs: baseline batches are eager
on every adapter; head/chunk starts must respect original schedules and phase order; CPU-stretch
receipt causality uses actual payload production rather than earlier head production. The
original clock uncertainty, cutoff and +5 ms boundaries are unchanged. Independent spec and
quality review also caught a substituted Spin head constructor and reporter-generated overflow;
the tests now use the production converter and distinguish those failures.

Strict local timing remains open. The fresh Axum baseline in
`/private/tmp/edgezero-finish-7e9619ce-baseline-axum` fails two of ten cases: an origin write
misses +5 ms by 38 microseconds and a terminal sample misses it by 460 microseconds. A temporary
precision-wait experiment also fails two cases and is removed, not shipped as a timing workaround.
Its failed evidence remains in `/private/tmp/edgezero-finish-precision-axum-lifetimes`.
Neither run is qualified or relabelled by passing unit tests. Remaining required work includes
all-target forwarding/mode/lifetime acceptance, exact-candidate binding and approved
consumer/hosted evidence, including successful native Image Optimizer transformation/performance.
No deployment, capability promotion or completed parent migration is claimed.

Final checkpoint verification passes all five required Rust gates: 2,586 workspace tests in
32 binaries, zero failures and one existing ignored test. The grouped Axum run passes 271
library tests and 27 contracts; repaired-Viceroy Fastly passes 280 library tests; Cloudflare's
Node/WASM runner passes 90 library tests; the rebuilt Spin eager component passes its existing
lifecycle checks plus the new criterion 09 marker. Strict actual-target Clippy passes for all
three WASM adapters with and without `test-utils` (six configurations). The Rust harness passes
293 tests with one existing ignored case and strict Clippy; evidence/guard regressions pass 61;
the demo passes 66 tests. Docs format/lint/build, inventory validation, legacy/docs guards and
diff checks pass. Independent spec and quality reviews are clear for this checkpoint. Frozen
requirements/interfaces, dependency/tool pins, all leg identities/expected text and applicability
decisions remain unchanged. These local results do not qualify the retained failed timing runs
or complete the remaining required host-wire, consumer and hosted evidence.

### Spin Independent-Cap Wire Checkpoint (2026-10-29)

Extend the existing real Spin host-wire matrix for EZ-5 criterion 10, without another client,
executor, transport or synthetic trailer/completion bypass. Sixteen identity cases cover both
decoding choices, both response modes, declared/observed lengths and exact 10/overflow 11 bytes
under decoded cap 10 and encoded/final caps 20. Ten original-gzip cases retain zero decoder budget
and independently exercise exact 23-byte and exceeded 22-byte encoded/buffered caps. Streamed
responses have no buffered-final cap; the 1024-byte reporting collector cannot supply these errors.

Reuse the native-stream observer under `test-utils`, requiring an actual head attachment rather
than accepting an unused zero-poll counter. Native regression tests cover attachment, unread and
consumed release, overlapping installation and cancellation cleanup. The observer retains only
counters, not the SDK source. Policy exchanges remain serialized so the next-head probe is
unambiguous. Declared overflows reject during send without polling; observed streamed overflows
reject during body collection. Every case requires local source release and exact correlated
origin receipts. This does not certify host allocation recovery or interruption.

The actual local Spin 4.1 run in `.runs/18e3362f57076e68-eb4c-0` passes the 26-case matrix and
the unchanged 68-exchange/84-receipt redirect inventory, framing and scope/handoff controls.
Emit the new marker only after runtime/origin cleanup, and retain separate CI logs for eager
component and wire execution before checking all ready `unit/spin` markers. Criterion 10 is now
`ready`; criterion 11 and optional representation-metadata legs stay planned. The inventory is
62 ready / 449 planned, 255 criteria / 511 legs, `qualified: false`. No parent task, candidate
freeze, strict timing, hosted/consumer or Image Optimizer performance gate is closed.

CI following `4470e73e` exposed two infrastructure failures after the isolated fixture formatter
was repaired. Fastly passed all 280 guest tests, but a timestamped Viceroy connection diagnostic
split a named result from its standalone `ok`. The execution guard now recognizes only that
specific host interruption, with exact names, successful nonzero summaries and command/tee exit
checks preserved. Unknown text, another test or a summary clears pending credit. The actual
guarded 280-test run and 139 guard/evidence regressions pass, including the captured CI shape
and negative substitutions.

The controlled startup responder also ignored the recovery POST body. It now consumes declared
bytes through the same BufReader before replying, bounded to 16 KiB and the existing socket
timeout. A regression first fails for absent body capture, then verifies exact recovery bytes,
identity, pending-credit and teardown behavior; unexpected recovery errors retain their diagnostic.
All 313 harness tests pass, with one intentional subprocess-only ignored test. This helper repair
does not certify the original Linux I/O cause or change production HTTP behavior, connection
budgets, retry policy, timing thresholds or qualification. Independent reviews preserve the same
SDK reader/trailer/completion path and identify no remaining implementation issue in this slice.

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
- [x] Implement criterion 12's begun-request and spawned-local-work proof after the eager API
      migration; its `axum/native-tcp` leg is now `ready` and bound to the routed test.
- [ ] Obtain criterion 13's consumer-owned route/parity evidence; that leg remains `planned`.
- [ ] Reconcile the remaining executable bindings and all required applicability decisions;
      freeze and qualify a candidate separately through Q3-Q5.

At this historical binding checkpoint, the inventory reported 255 criteria, 511 legs, 500 planned
and `qualified: false`; subsequent implementation and bindings are reflected in the dashboard.
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
parity and exact-candidate qualification remain open. The eager API migration also implements
criterion 12's begun-request and spawned-local-work portion through the same TCP facility.

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
map: 299 ready / 212 planned legs, including all 217 required-local bindings. The separately
selected EZ-16--22 local bindings are 53/57 ready: EZ-16/acceptance/04 and EZ-17/acceptance/06 Viceroy plus EZ-22/acceptance/07
workerd/Spin retained-instance legs remain planned. Passing execution and qualification remain
explicit open tasks, not hidden by this table. The reviewed guide corrections for EZ-1/2/6/8
are implemented; they are not a claim of completed exact-candidate or hosted acceptance.

| Requirement | Implementation Tasks | Production Applicability                                                  | Current Implementation / Checkpoint                                                                                                                                                                                                                                                             | Next Unclosed Work                                                                                                                                                                                        |
| ----------- | -------------------- | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| EZ-1        | P2; D1-D4            | Blocking, including actual ingress forwarding                             | **Shared settlement and local functional coverage implemented and bound.** All four adapters retain byte/deadline precedence and zero-read unsupported-option checks; Spin preserves explicit CL=0 without constructing a read stream.                                                          | Exact-candidate timing, hosted and consumer acceptance. Retain failed measurements and Fastly's BestEffort synchronous reads.                                                                             |
| EZ-2        | G2/G4; D1/D5; F1     | Blocking ranges exactly as spec; reuse-only ranges deferred               | **B31 identity/timers, cooperative progress and known-first dispatch implemented.** Warm classification compares the complete canonical identity without SDK I/O or body polling. Original deadlines, indices and callback ownership are preserved.                                             | Execute the approved fixed-strict-TLS baseline and native readback companion. Configurable TLS stays deferred under EZ-12; strict local execution, hosted pooling and resource qualification remain open. |
| EZ-3        | D1/D5                | Blocking; bounded waits and original budgets retained                     | **Approved dispatch-slack retirement implemented and locally bound.** Expired later slots skip SDK registration/send; actual known-first SDK dispatch preserves positional results. Private planning retains original bodies and contexts.                                                      | Execute the registered strict budget probes and obtain M7 consumer comparison; preserve all failed observations, original deadlines and B31.                                                              |
| EZ-4        | P1; D1-D4            | Blocking; Native/BestEffort distinctions retained                         | **All required local bindings implemented.** Includes zero/round-trip/SDK mapping, distinct Fastly getter readback and Linux Spin silent-connect ownership with healthy recovery. Axum/Cloudflare retain their documented aggregate fallback.                                                   | Exact-candidate Linux silent-connect and other runtime execution, plus controlled hosted connect-expiry proof. The isolated Spin characterization is not frozen qualification or aggregate-branch proof.  |
| EZ-5        | P3; D1-D4            | Blocking; Spin conditional host result recorded                           | **Shared head finalization and required local bindings implemented.** Send/Begin/Batch/final Follow retain original coding, disposition, provenance and independent limits; followed heads are discarded unread. Native Send/Begin parity covers all four adapters.                             | Execute and qualify the exact candidate; separately adopted metadata legs and provider limitations remain explicit.                                                                                       |
| EZ-6        | P4; D1-D6            | Blocking except EZ-6.31 reuse-only                                        | **Shared progress, ownership and required local bindings implemented.** Native operation/body/readiness witnesses remain distinct. Cleanup includes callback isolation, both Fastly wrappers and adapter-owned response models; counter tests require their same-candidate ownership companion. | Complete strict local lifetime execution and provider/consumer qualification. Unit models, guest ownership and runtime observations do not establish hosted cancellation or client delivery.              |
| EZ-7        | P3; D1-D4            | Blocking                                                                  | **Shared marker policy and required local bindings implemented.** Charge pre-normalization fields and the optional marker once across Send/Begin/Batch/Follow.                                                                                                                                  | Exact-candidate wire execution and qualification; no additional compatibility or normalization path.                                                                                                      |
| EZ-8        | P1; D1d/D5           | Blocking; Fastly single-request Send/Follow only                          | **Shared-pipeline optimizer Send/Follow and required local bindings implemented.** Preserve direct native mapping without extra fetch/buffering/serialization. Unsupported adapters/modes reject before settlement; native unavailable controls retain typed results.                           | Successful optimized-first-hop redirects and hosted transformation/performance evidence. Blocking horizon remains UNKNOWN.                                                                                |
| EZ-9        | P1/P5; D1-D5         | Blocking; supplied clients share dispatch                                 | **Explicit Follow and required local bindings implemented.** Core owns the decision table, chain deadline, replay and unread disposal. Supplied clients and actual local ingress/wire suites use the same dispatch boundary.                                                                    | Exact-candidate execution, successful native optimizer redirect, consumer and hosted evidence. No compatibility transport.                                                                                |
| EZ-10       | F2                   | Follow-up unless approved as promoted/present                             | **Local implementation and executable overflow/reader/fallback acceptance:** [all-target overflow evidence](#local-overflow-acceptance-2026-10-31) and [reader/fallback proofs](#reader-and-fallback-acceptance-2026-10-31). Historical Wrangler proxy failures remain retained.                | Provider response-handling unit bindings, hosted HTTP/2, consumer end-to-end and exact-candidate qualification.                                                                                           |
| EZ-11       | F3a                  | Follow-up                                                                 | **Not implemented:** cache tags.                                                                                                                                                                                                                                                                | Tag validation/deduplication, provider mapping, mode/support matrix and actual cache probes.                                                                                                              |
| EZ-12       | F3b                  | Follow-up                                                                 | **Not implemented:** TLS name-check policy.                                                                                                                                                                                                                                                     | Typed setting/readback, backend-key isolation and trusted-chain/wrong-name fixture; no global verification bypass.                                                                                        |
| EZ-13       | F3c                  | Follow-up; closes with Unsupported evidence                               | **Unsupported retained with the required synchronous support hook integrated.** Spin authority and unsupported optimizer checks precede ordinary validation, timers and body polling.                                                                                                           | Bind the complete activity/SDK limitation and consumer configuration-rejection evidence; do not promote support.                                                                                          |
| EZ-14       | F4                   | Follow-up; includes acceptance 10/11                                      | **Not implemented:** opted-in idle-body policy.                                                                                                                                                                                                                                                 | New mode, encoded-progress resets, absolute-deadline precedence and actual adapter read races; existing total deadlines are not idle-mode support.                                                        |
| EZ-15       | F5                   | Follow-up; reuse legs separately conditional                              | **Not implemented:** explicit bounded fire-and-forget operation.                                                                                                                                                                                                                                | Separate capacity/ownership, 64 KiB response ceiling, cancellation/recovery and provider-lifetime proof.                                                                                                  |
| EZ-16       | F6a                  | Follow-up                                                                 | **Diagnostic checkpoints implemented:** [SDK/source details and capability publication](#f6a-capability-publication-checkpoint).                                                                                                                                                                | EZ-16/acceptance/04 Viceroy binding remains planned; full token-safe acceptance, execution and consumer/provider qualification remain open.                                                               |
| EZ-17       | F6b                  | Follow-up                                                                 | **Local implementation:** [owned delivery reports/safe replacement](#f6b-delivery-report-checkpoint); subsequent F9a assembly integration has landed.                                                                                                                                           | EZ-17/acceptance/06 Viceroy binding, consumer canonical-lifecycle acceptance and qualification remain open; assembly integration is implemented.                                                          |
| EZ-18       | F7a                  | Follow-up                                                                 | **Local implementation:** [buffered framing](#f7a-buffered-framing-checkpoint); historical 80 production passes and nine reference failures retained. The approved 2026-11-08 amendment separates content-bearing parity from twelve bodiless reference observations and strict suppression. | Qualify content-bearing parity, hosted compression and actual bodiless zero-payload/completion. No requirement to reproduce bodiless SDK framing remains; historical failures are not relabeled. |
| EZ-19       | F7b                  | Follow-up                                                                 | **Local implementation:** [bounded synthesized headers](#f7b-synthesized-header-checkpoint), wire and assembly-exclusion checkpoints.                                                                                                                                                           | Consumer-owned header integration and hosted/frozen-candidate qualification; local assembly exclusion is not still missing.                                                                               |
| EZ-20       | F8                   | Follow-up                                                                 | **Local implementation:** [declared KV bindings and operation delivery](#f8-declared-kv-checkpoint) on all three adopted serverless adapters.                                                                                                                                                   | Actual provider outage/restoration, consumer acceptance and qualification; the separate Spin retained-overlap uncertainty remains disclosed.                                                              |
| EZ-21       | F6c                  | Follow-up                                                                 | **Local implementation:** [six typed framing reasons](#f6c-framing-checkpoint), observer/log controls.                                                                                                                                                                                          | Consumer Viceroy acceptance and separately required hosted evidence across applicable failure paths.                                                                                                      |
| EZ-22       | F9a                  | Follow-up; individual SHOULD decisions explicit                           | **Local implementation:** [unified fallible assembly/failure delivery](#f9a-assembly-foundation-checkpoint), including adopted macro/serverless parts.                                                                                                                                          | EZ-22/acceptance/07 workerd/Spin retained-instance bindings, consumer acceptance and exact-candidate qualification remain open; no compatibility assembly path.                                           |
| EZ-23       | F9b                  | Follow-up                                                                 | **Local implementation:** [assembled-App Axum/TCP helpers](#f9b-assembled-app-tcp-checkpoint), including criterion 12's begun-request and spawned-local-work proof.                                                                                                                             | Criterion 13's consumer route/parity evidence and exact-candidate qualification.                                                                                                                          |
| EZ-24       | P6; F10a/F10b/F10c   | Host/WASI publication and each optional extension need separate decisions | **Not implemented as the proposed published utility; adoption pending.** Existing private doubles are not P6.                                                                                                                                                                                   | Attributed applicability decisions, P1-P5 semantics, then selected published-client/cancellation/demo/JS work. Pending is not approved deferral.                                                          |
| EZ-25       | Q1/Q2                | Documentation updates accompany each affected behavior                    | **Hard-cut migration and reviewed guide corrections implemented.** Includes capability/precedence/streaming/optimizer guidance, scaffold trees, declared stores, custom entrypoints, persistent local KV and repaired links, with scoped regression guards.                                     | Execute exact-candidate documentation/generated-project/consumer acceptance and qualify the frozen candidate. Source correction and current-head checks do not establish all EZ-25 acceptance.            |
| EZ-Q1       | G1; Q3-Q5            | Required blocking, approved promoted, and confirmed-present parts only    | **Tooling implemented, qualification open:** Rust inventory/assembly/frozen export/report validation, single-request Fastly outbound package and [runbook source](#q2-runbook-source-preparation-checkpoint).                                                                                   | Planned bindings/scope decisions, exact-candidate evidence and freeze, accepted Q4a package, authorized Q4b runs and durable passing Q5 record remain open.                                               |

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
CARGO_NET_OFFLINE=true scripts/test_integration.sh check dependencies
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
WASM commands in plan 03/Q2. For fixture changes use `--manifest-path crates/edgezero-test-harness/fixtures/reusable-app/Cargo.toml`;
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

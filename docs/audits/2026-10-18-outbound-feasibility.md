# Outbound Feasibility Gate

## Status

Executed locally on 2026-10-19. **G2: Failed.** The pinned Viceroy body-readiness
primitive discards a source error, confirmed by three independently dispatched paired controls.
Hosted Fastly remains **PendingExternal**. No capability is promoted. G2 itself did not change a
production transport; the later G3 investigation fixed an internal Spin response-egress defect
described below. This is characterization, not an EZ-Q1 passing qualification record.

The base checkout was `7cccaf10734914161803317aec6c55c22c2bccc9`; the probes were uncommitted
working-tree changes. They do not qualify that base commit. The
[retained evidence](evidence/2026-10-19-fastly-readiness.json) contains source-file SHA-256 hashes,
the compiled binary hash, the original JSONL event hash and all event objects. Reconstruct the
JSONL as `events.map(JSON.stringify).join("\n") + "\n"` to check the event hash. The binary is
identified, not retained as a release asset. Future positive qualification must run a clean,
committed candidate and keep its own durable build and runtime evidence.

## Pinned Boundary

| Component          | Identity                                                                 |
| ------------------ | ------------------------------------------------------------------------ |
| Compiler           | Rust 1.98.1, `48a229cea`, 2026-09-01                                     |
| Target             | `wasm32-wasip1`                                                          |
| SDK / sys / shared | Fastly 0.13.1                                                            |
| Runtime            | Standalone Viceroy 0.21.0, not the Fastly CLI's bundled 0.21.1           |
| Origin             | Local HTTP/1.1 TCP fixture, no credentials or external service           |
| Source identities  | Lockfile, fixture sources and Viceroy source hashes in retained evidence |

The experiment calls the pinned `fastly_sys::fastly_async_io::select(handles, len,
timeout_ms, selected_out)` ABI through a fixture-local scoped unsafe wrapper. Zero would wait
indefinitely, so every call passes at least 1 ms. The wrapper borrows live handles; it neither
closes nor transfers them. This is not a new public EdgeZero interface or production client.

Response-head readiness is followed by `PendingRequestHandle::wait`. The normal-body path calls
timed readiness before **each** `BodyHandle::read` with a one-byte output buffer. A generic ready
signal is not by itself proof that this read cannot block: Viceroy skips empty data chunks, and
the guest cannot inspect the next retained chunk's length. Head readiness retains its completed
`Result` in `PeekableTask`; body readiness has different error semantics.

In Viceroy 0.21.0, `src/body.rs`'s `Body::await_ready` matches only
`Some(Ok(chunk))` from `self.data().await`. It puts that successful chunk back, but consumes
`Some(Err(_))` without retaining it. `src/sandbox/async_item.rs` delegates normal bodies to this
method. `src/async_io.rs` then reports readiness. A later body read cannot recover the discarded
error through this primitive. The exact inspected source files are hashed with the evidence.

## Executed Control

```sh
scripts/smoke_test_reusable_app.sh --adapter fastly --suite outbound-readiness
```

Expected diagnostic exit: **1**, because the source-error preservation prerequisite fails.
The suite is opt-in and does not turn the normal unit or smoke gates red.

Each origin response declares three bytes, sends one byte, then closes after 50 ms. The direct
control performs one-byte reads without body readiness. The selected control reacquires body
readiness before every one-byte read. Both use a 500 ms observation budget and a timed response-head
wait. The direct read is deliberately blocking against this finite controlled source; it is not
an implementation proposal or a production fallback.

| Repetition | Direct Control                 | Readiness Control                            | Preservation |
| ---------- | ------------------------------ | -------------------------------------------- | ------------ |
| 0          | One byte, then `UnexpectedEof` | One byte, successful readiness, then `Ok(0)` | Failed       |
| 1          | One byte, then `UnexpectedEof` | One byte, successful readiness, then `Ok(0)` | Failed       |
| 2          | One byte, then `UnexpectedEof` | One byte, successful readiness, then `Ok(0)` | Failed       |

Six real origin receipts were required. Each read and readiness operation has guest-relative
start/end microseconds in the raw record. All completed before the 500 ms budget, well before
the experiment's two-second backend timers. An incomplete selected trace or a select timeout is
rejected separately; neither is counted as clean EOF or evidence of error consumption.

The harness guard `outbound_readiness_rejects_empty_or_unbounded_measurements` rejects absent
identity, zero/missing samples, zero/unbounded readiness waits, late operations, missing terminal
reads and selected reads without immediately preceding successful readiness. It is a validator
unit test, not proof of hosted behavior. The parent uses a finite socket timeout and process-group
teardown to contain a stuck diagnostic. That does not prove prompt guest cancellation, sibling
progress or a 500 ms hostcall interruption guarantee.

## Unproved Work

| Leg                                                   | Disposition                                                                                                        |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Truncated body typed-error preservation, local        | **Failed**, six measured controls                                                                                  |
| Silent head, permanently pending body, 300 ms trickle | Pending; full deadline +5 ms proof not run                                                                         |
| Sibling readiness and ready-then-exhausted body       | Pending; no sibling-progress claim                                                                                 |
| Malformed framing and other transport failures        | Pending; truncated body alone does not cover them                                                                  |
| Buffered and streamed production paths                | Pending; this probe tests the host primitive only                                                                  |
| Hosted Fastly and Image Optimizer entitlement         | **PendingExternal**; no deployment attempted                                                                       |
| Literal backend cardinality bound B                   | Not proposed or approved; primitive gate failed                                                                    |
| Numerical synchronous-call horizon H(K)               | Unknown; no finite rule or maximum proved or approved                                                              |
| G4 public API approval                                | [API-only approval by AG](2026-10-18-outbound-api-approval.md) recorded later; Fastly policy decisions remain open |

Milliseconds are the ABI wait unit; the experiment floors positive remaining milliseconds and
uses at least one. This is a diagnostic observation policy, not an approved production rounding
rule, minimum budget or backend discriminator. A successful head/read timing on this finite
origin cannot establish the remaining deadline domain, B, H(K) or a provider processing horizon.

## Task Disposition

- **Cancelled pending a new reviewed primitive/report:** D1a's proposed Fastly multiplexer and
  D1b's dependent stable-backend/slack removal; D1c and D1d cannot proceed through those prerequisites.
- **Deferred:** dependent Fastly D5/D6 qualification and any hosted capability promotion. Required
  acceptance legs remain required and visibly unpassed, not deleted from the inventory.
- **May proceed independently after their approvals:** portable P1-P5 and Axum/Cloudflare/Spin
  D2-D4. This failed local primitive does not prohibit that work or approve it automatically.
- **G3:** local characterization and the subsequent Spin repair are recorded below. The initial
  Spin failure is fixed; the later Cloudflare checkpoint now passes the exercised local cases.
  The incomplete gate does not approve a new scheduling primitive or complete G4.

Do not repin an SDK/runtime, add a blocking-read fallback, restore an old client, widen the
cutoff/tolerance, or waive the failed gate as BestEffort. The next Fastly step is maintainer review
of a provider/runtime primitive that preserves body errors, followed by a new exact-version
report and the remaining local and hosted probes. Production adoption remains blocked meanwhile.

## Local Runtime Repair (2026-10-23)

PR #275 now carries an [exact-version Viceroy source patch and build recipe](https://github.com/IABTechLab/edgezero/blob/docs/outbound-http-spec/patches/viceroy/README.md).
It retains original body errors in the chunk queue, skips empty frames before reporting readiness,
and yields when decompression produces no bytes. It does not add an SDK-side blocking fallback,
change dependency/runtime version pins or replace the installed runtime. A repaired executable
must be selected explicitly with `VICEROY_BIN` and identified by its source/patch/binary hashes.

The [separate repair record](evidence/2026-10-23-viceroy-readiness-repair.json) preserves a fresh
stock failure and repaired success using the same guest binary at `ff60e5ac9d6d673648ca5e06b1bdddfaab68083a`.
All three stock pairs lose `UnexpectedEof`; all three repaired pairs preserve it. The repaired
suite intentionally exits 2, not a G2 pass. Ten new explicit-poll regressions went red (nine fail)
before the repair and green afterward; all 135 runtime library tests pass. Strict upstream clippy
has the same diagnostics on stock and repaired source under Rust 1.98.1; those are not suppressed.

This fixes the measured error-loss/empty-frame defects in the locally repaired runtime only.
The unmodified pinned runtime remains a failed control. The remaining G2 stalled/trickle/sibling/
framing/buffered/streamed ABI cases, exact-version review, B/H(K) approvals and hosted evidence are
still open. The earlier failed evidence is unchanged; this report does not approve the atomic
production migration or promote any capability.

## G2 Expanded Primitive Matrix (2026-10-23)

The [separate matrix record](evidence/2026-10-23-fastly-readiness-matrix.json) extends the
diagnostic, not the production client. Eleven cases cover silent heads, pending bodies, trickle,
two siblings with only the second body ready, ready-then-exhausted, ordinary/empty EOF, truncated
bodies, malformed heads/chunks and pre-head connection closure. Each selected case runs buffered
and pull-streamed, with capacities 1/16384 and three repetitions. Three finite direct truncation
controls bring each full run to 135 probes and 147 correlated origin requests.

The guest borrows live handle identities for timed ABI select, then consumes a ready pending head
through nonblocking `PendingRequestHandle::poll`, not `wait`. Body reads consume one fresh
same-slot readiness result each. Positive millisecond waits are recomputed from one 500 ms absolute
deadline, floored with a one-millisecond minimum; early timer wakes rearm against that same instant.
Expiry wins over late readiness/read outcomes. Stream ownership transfers before any body read,
without resetting the clock; its consumer discards chunks instead of retaining the buffered body.
The two capacities are diagnostic variations, not an approved production maximum. Backend
registration is outside this window and phase timers are fixed at two seconds, so this is not a
backend-policy or whole-entry synchronous-call bound.

The validator requires complete unique identities, origin receipts/cleanup, monotonic raw samples,
fresh selection (including a final matching wait for each timeout), actual read/EOF/error outcomes,
matching terminal summaries and all completion
times within 505 ms. FNV is only a tiny known-payload checksum, not a provenance hash. Source,
guest, runtime and retained trace identities use SHA-256. Completed samples are also logged
incrementally, including failed ABI outcomes, so a failed guest does not erase its preceding trace.

The first complete repaired run failed: pending-body/streamed/1/repetition-0 completed at
507.794 ms (the timed select itself returned at 507.153 ms). Its source errors were preserved.
The same guest bytes on stock Viceroy lost truncated/malformed-body errors and had late samples.
After the trace-retention/deadline-check repairs, a fresh identical-guest stock/repaired pair
still failed: stock lost 24 selected source errors; repaired preserved them but completed
siblings/selected/streamed/1/repetition-1 at 507.233 ms. All four runs (540 probes) are retained
and replayed by the validator's regression test; the earlier failures are not replaced by later runs.
The lateness is observed at the host-call boundary; these traces do not isolate an OS/runtime
scheduling cause or establish a deterministic interruption bound. The +5 ms limit is unchanged.

Each origin owns at most two workers, a 4096-byte request-head cap and a three-second watchdog;
finish/drop closes sockets, wakes timers and joins workers. Harness-forced shutdown is labelled,
not SDK cancellation. Guest handle release is not proof of hosted transport teardown. All results
remain working-tree characterization. Full G2 is **Failed**, hosted remains **PendingExternal**,
and B/H(K), exact-version review, production migration and candidate freeze remain separate gates.

## G3 Local Characterization

The initial default-off `progress` fixture feature uses direct SDK operations, not a second production
client. The standalone `outbound-progress` suite runs the probes; ordinary `smoke` also builds and
runs them in separate artifact directories after its existing lifecycle cases. The all-adapter
smoke command was executed with the explicit local tool paths in the
[retained G3 record](evidence/2026-10-19-outbound-progress.json). It exited **1**, preserving the Spin
failure and the unavailable Cloudflare leg. This is not a passing release gate.

The record retains 41 G3 event objects, runtime logs, source-file and binary SHA-256 identities.
Reconstruct its retained event JSONL using `events.map(JSON.stringify).join("\n") + "\n"` to
verify `retained_events_sha256`. The full unrelated smoke history has a separate identity-only
hash. Binaries are identified, not retained release assets. Source hashes describe uncommitted
fixtures on the base revision above, not qualification of that base commit. The earlier G2 record
is historical and has not been rewritten to match these later source changes.

| Adapter / Local Runtime        | Progress Cases          | HTTP/1 Framing Cases | Disposition                                             |
| ------------------------------ | ----------------------- | -------------------- | ------------------------------------------------------- |
| Axum / Tokio native            | 5/5, 18 origin receipts | 5/5                  | Passed for these local observations only                |
| Fastly 0.13.1 / Viceroy 0.21.0 | 5/5, 18 origin receipts | 5/5                  | Passed for these local observations; G2 still Failed    |
| Spin SDK 7.0.0 / Spin 4.1.0    | 4/5, 15 origin receipts | 5/5                  | **Failed** response-owned execution                     |
| worker 0.8.5 / workerd         | Not executed            | Not executed         | Unverified: locked wasm-bindgen 0.2.129 CLI unavailable |

### Scheduling Method

The five cases are direct-SDK eager single and batch starts, the existing lazy public single
send, the existing never-polled public batch, and SDK jobs retained by a response stream. The
consumer job wrapper counts polls before the window closes. A separate same-guest monotonic
sample must show an unrelated wait of at least 100 ms; origin-local timestamps independently
prove when each request arrived. Guest and origin clock epochs are never compared. Unique case,
token, adapter and build identities, exact receipt counts and duplicate rejection are required.

Direct SDK starts reached the origin during the window on the three executed targets. The
existing single send was lazy on all three. Existing batches were lazy on native and Spin but
dispatched immediately on Fastly. Fastly's fixture captures pending handles synchronously and
sleeps the guest for the window: this establishes host-side request progress, not background
guest scheduling or an approved cooperative timed-body multiplexer. Its completion probe only
polls response heads from the controlled `Content-Length: 0` origin, avoiding the failed G2 path.

The response-owned stream yields one whitespace byte before awaiting its jobs. The origin cannot
complete those jobs until the client observes that byte and releases a finite response gate.
This rejects fully buffered substitutes. Native and Fastly passed this initial observation. It proves
handler return followed by response consumption, not return of the complete host export/callback.
The existing native entrypoint installs its `LocalSet`; the negative unit control proves only
that an absent Tokio runtime is rejected before SDK dispatch. It does not establish LocalSet
presence/liveness detection. The subsequent repair added a focused native unpolled-job cleanup
regression without claiming provider cancellation guarantees.

Spin's four handler-held cases passed before its response-owned case failed in repeated runs.
The retained run saw response-head EOF; other runs saw a 200 response interrupted after zero or
one byte. Runtime logs report `body writer dropped`, then outbound resets and `worker panicked`.
These initial observations did not prove provider impossibility. The isolated controls described
below subsequently identified an adapter bug independently of SDK `get` ownership.
No exception is caught or reclassified as a pass, and independent framing probes still run.

### Empty-Body Observations

Actual TCP ingress distinguishes absent framing, explicit zero length, a terminating chunk,
and nonempty fixed/chunked controls. All executed targets returned zero bytes for the first
three and four bytes for both controls. These are byte-count observations, not a payload-fidelity
test. No request body is cloned, consumed or
replaced when its host signal is sampled before normal adapter conversion.

Fastly reported a body handle even for absent framing; its SDK known length was zero for absent
and explicit-zero requests and unknown for chunked requests. Native's existing entrypoint does
not expose the original host object to this fixture, so its raw signal is explicitly unavailable.
Spin's SDK `is_end_stream` was false even for explicit zero length; only its header-derived hint
reported zero. That is not independent framing evidence and does not justify a known-empty fast
path. Cloudflare's non-consuming raw-object snapshot compiles, but has not run. The production
absent-body fallback protecting browser-created requests is unchanged.

### Spin Repair and Rerun

Four additional controls compared whitespace-first ready/timer-only streams through the adapter
and direct SDK `BodyWriter`, with no outbound calls. Before the fix, both SDK controls passed while
both adapter controls failed. Phase tracing then observed `Ready(Ok(()))` on the host response-result
future during pending body-source or write work. The adapter classified every ready response result
as a transport failure and dropped the writers. A guest panic hook did not fire; the originating
host worker-panic message was not itself the proof of this logic defect.

The adapter now retires successful observation exactly once and resumes the same pinned operation.
Host errors still preempt, deadlines remain active, and source/write/body-result failures remain
errors. Finish closes the byte stream and successfully publishes the body result before accepting
latched success, or awaits the remaining observer under the deadline. Completed observers are never
cancelled or repolled. The report remains `HostHandoff`, not confirmed client delivery.

The regression `successful_early_host_handoff_does_not_abort_pending_body_work` failed against the
old classification, then passed. Separate tests cover the same pending operation resuming, later
deadline/operation failure, host failure precedence and timer priority. Additional fixture tests
prove unpolled native worker cleanup while its runtime stays alive and exactly-once release of
the two jobs held by an unconsumed response. SDK-specific final-await cancellation/error interleavings
still need broader qualification; generic helper tests are not a substitute for them.

The [repair evidence](evidence/2026-10-19-spin-response-egress.json) retains source and artifact hashes,
49 selected current events, component stderr, runtime logs and intermediate before-fix control
observations. It is a working-tree characterization, not a clean-candidate qualification. The
earlier G2 and G3 evidence records remain unchanged. Its intermediate before-fix snapshot identifies
the compiled binary but does not claim a complete retained source snapshot.

Spin now passes 5/5 progress cases, 5/5 HTTP/1 framing cases, four comparison controls and 18 origin
receipts. Exactly two successful token-correlated SDK terminals are required from canonical component
stderr, with failure checks repeated after the finite observation hold. No worker panic was observed
while serving; the message appeared only after controlled SIGTERM. This separates the observation
phases; it does not establish the cause or safety of host shutdown. The latest all-adapter smoke run
exits **2**, preserving Cloudflare's tool gap and unverified same-guest overlap observations.

### Remaining G3 Work

Cloudflare requires a matching, explicitly provided `WASM_BINDGEN_BIN` plus absolute executable
`ESBUILD_BIN` and `WASM_OPT_BIN` overrides to avoid worker-build's automatic installations. The
available wasm-bindgen executables are older than 0.2.129; no download or repin was attempted.
All three WASM probe targets compile and pass clippy, which does not supply missing runtime proof.

HTTP/2 and HTTP/3 were not exercised. Hosted executions remain PendingExternal. Missing active
request-context detection on provider schedulers, host-export/callback completion, cancellation,
and integration through the future public begin API remain unproved. G3 stays open and G4 stays
incomplete; later scoped API approval by AG is recorded in
the G4 packet. These observations themselves neither approve an API, promote capabilities nor
waive a required acceptance leg.

### Verification

The five workspace Rust CI commands passed, including the full workspace tests. After the repair,
the fixture's 34 harness tests, 12 core tests and two native controls passed, as did the Spin
adapter's 63 feature-enabled tests and ten bounded-secret integration tests.
Fixture formatting and strict clippy passed for native and all three WASM probe targets. The
18 evidence-checker tests, inventory validation, outbound docs/legacy checks, decoder/map
dependency audits, and docs format/lint/build passed. Retained event and source hashes were
recomputed successfully. The initial all-adapter smoke failed; the repaired run is unverified as
recorded above. Green compilation/unit checks do not turn unavailable runtime legs into passes.

## Original-Coding Local Checkpoint

The [coding record](evidence/2026-10-19-outbound-coding.json) covers the separately implemented
Decode/Original request option and mandatory 206 preservation. Pinned Viceroy 0.21.0 and Spin 4.1
each passed 24 controlled-origin checks: gzip/deflate/Brotli, buffered/streamed, Original/200,
Original/206, Decode/206 and Decode/200. The first three combinations use decoder budget zero
and decoded cap one; controls verify that the fixtures actually decode to the expected plain bytes.
The record retains case results, delivered-byte hashes, original coding/length observations,
source hashes, binary identities and raw event-file identities. Both runner commands exited zero.

These are uncommitted working-tree characterizations, not qualification of a frozen candidate or
hosted runtime. Reports contain delivered bytes as JSON; they do not prove encoded downstream
relay framing. Fastly mixed-choice batch host execution, the full normalization/cap adapter matrix,
Cloudflare workerd/relay execution and hosted cases remain open. No new tool was downloaded,
dependency repinned, capability promoted or G2 failure waived. Optional HEAD/304 and representation
metadata changes remain unadopted.

## Proxy-Marker Local Checkpoint

The [marker record](evidence/2026-10-20-outbound-marker.json) retains 46 checks each through native
Axum, Fastly SDK 0.13.1 / Viceroy 0.21.0 and Spin SDK 7.0.0 / Spin 4.1.0. Each runner exited zero.
The matrix crosses inherited-off, explicit-off and explicit-on settings with absent, upstream
and Connection-nominated markers on GET/200, GET/204, HEAD/200, GET/304 and GET/302 responses,
plus one mixed-policy batch. The client inspector and returned fields are checked together.
The same runs passed 24 original/206/decode coding controls each. Source hashes, compiler/runtime
identities, binary hashes, case observations and raw event-file identities are retained.

### Spin SDK Header Visibility

The initial Spin run failed the nominated-marker check: with insertion disabled, the upstream
marker survived. A direct SDK control now snapshots `get_headers().copy_all()` before EdgeZero
conversion. Both ordinary and nominated upstream shapes expose the marker but no Connection field.
Two correlated controlled-origin requests establish this at the SDK boundary; EdgeZero cannot
recover the lost nomination. The failed observation is retained separately, without inventing an
initial source snapshot or asserting source equivalence with the rerun.

Fifteen Spin nominated-shape rows therefore record `provider_hidden_nomination: true`. Their passes
establish policy on adapter-visible fields only, not removal of raw nominated fields. Off preserves
an ordinary upstream marker; it must not guess that the provider hid a nomination. Spin's
OutboundHeaderFidelity assignment is corrected from Native to BestEffort in code, public guides
and the baseline capability notes. The strong fidelity definition itself is unchanged. Axum and
Viceroy remove the nominated field before marker policy in all exercised cases.

These are local working-tree characterizations, not hosted or frozen-candidate qualification.
Cloudflare runtime remains blocked on the matching wasm-bindgen 0.2.129 CLI; target compilation and
clippy do not replace that evidence. Head-first/eager/follow integration remains open. No tool was
downloaded, dependency repinned, capability promoted or Fastly G2 failure waived. Exact marker-off
header byte/count budgets, duplicate upstream fields, non-polling inspection and clone isolation
have separate deterministic core tests and Axum transport coverage; the 46 runtime cases do not
claim those resource-bound proofs for every host.

## Cloudflare Local Runtime Checkpoint

The [Cloudflare record](evidence/2026-10-20-cloudflare-outbound.json) supersedes the tool-unavailable
status above for this new local run only. Historical records remain unchanged. An explicitly
authorized temporary install supplied wasm-bindgen CLI 0.2.129, matching the locked crate; no
project dependency, SDK, compatibility setting or global tool was replaced. The first attempt
built successfully but the sandbox denied loopback sockets before any request ran. Authorized
loopback-only reruns used Wrangler 4.131.0 and its bundled workerd 2026-09-10 locally, without
deployment, remote bindings or metrics.

The strengthened harness passed all **80 checks**: 24 coding, 46 marker, five scheduling and five
HTTP/1 framing cases. It asserted **89 origin receipts**: 24 coding, 47 marker and 18 scheduling.
The scheduling receipts are individually retained; coding/marker counts are enforced by the
runner but their individual receipts were not emitted. Coding includes 18 original/206 preservation
cases and six decode controls across gzip, deflate and Brotli, in both response modes. Marker
checks include bodyless dispositions, manual redirects and a mixed-policy batch. Unlike Spin's
retained SDK control, these exercised Cloudflare nominated fields were removed before marker policy.
This does not establish raw-header fidelity for other field shapes or protocols.

Pre-conversion Workers `body_absent` was true for absent framing and explicit zero length, false
for chunked-empty and both nonempty controls. The latter preserved all four bytes. Missing,
null, mistyped or contradictory signals now fail the harness; regression tests preserve other
adapters' existing byte checks. All eight group-failure combinations prove coding/marker failures
cannot skip scheduling/framing and cannot turn the overall result into success. Phase-specific
errors are retained. Spin's terminal/panic checks and fatal evidence-write behavior are unchanged.

The record separates the original unchanged-harness run from the strengthened run and retains
source/lockfile SHA-256s, exact tool/runtime identities, effective Wrangler configuration, served
JS/WASM fingerprints, raw events, summary and runtime log. Compatibility date remains
`2026-04-22`, with `enable_request_signal` and `enable_weak_ref`. The built WASM and JS artifacts
did not change when only harness validation was strengthened.

These are **working-tree characterizations, not frozen-candidate or hosted qualification**.
SDK-native eager probes still contrast with existing lazy public APIs; they do not implement
`HttpClient::begin`, the head-first boundary, or the full adapter hard cut. Response-owned checks
prove handler/adapter dispatch return and gated client progress, not exported callback return or
confirmed delivery. Encoded downstream relay framing, cancellation/deadline qualification,
HTTP/2/3, scheduler-context rejection and hosted execution remain open. Cloudflare capability
assignments are unchanged. Fastly G2 remains failed; no B, H(K), slack removal or readiness waiver
is approved. The frozen API-only approval digest is unchanged.

## Actual Downstream Relay Checkpoint

The [relay record](evidence/2026-10-20-outbound-relay.json) supersedes the earlier local downstream
relay-proof gap, not the hosted qualification gap. The final all-runtime command exited zero on
Axum, workerd 2026-09-10, Viceroy 0.21.0 and Spin 4.1.0. Each adapter passed 24 actual downstream
relay cases and the existing 80 base checks, with 113 asserted base origin receipts. Spin also
retained its six SDK/header/stream controls. All 96 relay observations and 96 ordered relay origin
receipts are embedded, together with the 72 scheduling receipts; full raw-event files and binaries
are identified by hash but remain local, not release assets.

The relay shares coding target validation, caps and the existing client. It returns
`OutboundResponse::into_response()` directly, without collecting or reporting JSON. A shared TCP
reader captures at most 256 binary body bytes and performs no content decoding. Boundary tests
cover fixed, chunked and close-delimited overflow and exact-cap acceptance, plus fixed/chunked truncation. Reader
failures cannot pass the suite; their remaining diagnostic limitation is that they can occur before
a per-case record, although the group still records failure.

For each target, 18 Original/206 cases preserved the deterministic gzip/deflate/Brotli bytes and
surviving Content-Type, ETag, Last-Modified, Vary, Accept-Ranges, Cache-Control, Content-Digest and
206 Content-Range fields. The fixtures' digest values match their encoded bytes. Six Decode/200
controls delivered the expected plain bytes without Content-Encoding. Wire Content-Length, when
present, matched delivered bytes; the validator allows host-selected chunked or close-delimited
framing and rejects conflicting framing. This does not adopt optional decoded-metadata SHOULDs or
promise universal wire Content-Length retention. Cloudflare's outbound manual fetch and downstream
manual encoding have now been exercised together on local workerd.

Source snapshots identify base `0250edc9` plus the three changed fixture files. No production adapter,
public API, pin, capability, approved Interfaces digest or Fastly engine changed. Core fixture tests
prove streamed handoff does not poll the payload and relay marker-off does not mutate the original
coding client. These are constructed outbound GETs, not actual ingress GET/HEAD forwarding. Full
head-first/eager/follow migration, scheduler-context and cancellation/deadline cases, HTTP/2/3 and
hosted frozen-candidate qualification remain open. Fastly G2 remains failed without waiver.

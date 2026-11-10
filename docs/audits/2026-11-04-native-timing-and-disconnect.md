# Native Timing And Disconnect Investigation

The diagnostic source base is `33995888ef0003d64e07e7fa20060890732bc7dc`. These local
experiments are characterization, not candidate freeze, hosted qualification or acceptance
credit. Original failed captures, absolute deadlines and timing tolerances remain unchanged.

## Native Provider Control

A temporary test-only copy of the existing Rust harness drives the unchanged Miniflare
launcher and its direct guest socket. The served module uses a native `TransformStream`:
write one observed prefix, then leave production pending without further writes. No
EdgeZero or Wasm code runs in this request path. Observe the original `Request.signal`
and `writer.closed()` separately, excluding controller-induced cleanup notifications.

| Client action                                               | Request abort observed | Writer rejection observed | Read owner retained |
| ----------------------------------------------------------- | ---------------------- | ------------------------- | ------------------- |
| Ordinary close through the existing prefix-rejection helper | No                     | No                        | No                  |
| Explicit reset through the same helper                      | Yes                    | No                        | No                  |
| Write-half-close after the complete prefix chunk            | No                     | No                        | Yes                 |

The ordinary-close last sample is 1.999815041 seconds from client request start and
1.972245458 seconds after the helper returned. The actual socket-close instant was not
timestamped. Half-close retains the socket/read owner through observation. Every control
then explicitly releases its diagnostic owner, verifies a healthy response and stops/reaps
the runtime before asserting its result. The ordinary-close notification assertion fails;
the reset and half-close controls pass. Cleanup-induced notifications cannot satisfy it.

The explicit invocation selects Node 24.20.0, Wrangler 4.151.1, Miniflare
5.20261012.1-beta and workerd 1.20261012.1 on native Apple ARM64. Compatibility date is
`2026-04-22`, with `enable_request_signal`. The selected workerd SHA-256 is
`2449d1a1404e54b832fd3265fd61803072438bf7559b7b28d491fd5f24a627bf`;
the unchanged launcher SHA-256 is
`17da8f186a5fc58a630c00f4b2a57efd8c259feadb5d2e36c0e406136ffb42a8`.

This reproduces notification absence below EdgeZero for the tested shape. It does not
establish particular FIN packets, universal provider impossibility or hosted behavior.
Do not substitute reset success for the ordinary-close acceptance gate or promote egress
capabilities. A deployed runtime can behave differently and still requires its own evidence.

## Timing Phase Control

A separate temporary fixture records the original 34 ms target, duration selection,
first underlying SDK timer poll, wait return and existing preparation-complete timestamp.
It delegates the same timer future once per poll, adds no timer/I/O/epoch and keeps the
strict validators. Its source is modified instrumentation, not a clean release candidate.

Fail-first and manually advanced-clock tests distinguish construction delay, late readiness
and preparation delay while preserving the original target and cutoff. All 93 fixture tests
pass. The native diagnostic run has 16 passing scenarios and three failures, plus the
cleanup-tail failure; these results earn no qualification credit.

In this capture, first-poll entry/return equal duration selection at the guest clock's
millisecond resolution. Waits return at 35-36 ms, with preparation adding 0-1 ms. This does
not demonstrate the hypothesized registration delay in that capture. Cold admission is
3 ms, handler entry and origin-wait entry are 8 ms, and origin Begin is 9 ms: the 2 ms
target is already missed before the origin wait. A pending-body cutoff returns at 206 ms
against its original 200 ms cutoff. Instrumentation cost is neither subtracted nor credited.

## Production Timer Repair

Independent deterministic WASM tests expose an actual adapter-owned defect: the native
outbound timer converted its deadline to a duration before callback construction and both
JavaScript timer-function lookups. With a 10 ms deadline and a clock advanced by 7 ms
during either lookup, it registered 10 ms rather than 3 ms. Expiry or regression during
setup also allowed an unnecessary registration. All three regressions fail before repair.

One private host-event primitive now retains the original deadline and paired clock until
the final registration step. It rechecks time after successful setup, registers only the
remaining budget, skips expired registration and rejects regression. Encountered scheduling
failures remain errors. Terminal paths release callback, clock and waker state; pending
cancellation remains single-shot. Genuine fairness barriers still register a zero-delay
host event. The relative constructor and extra native-timer async wrapper are removed.

This repair closes that setup defect, not the historical cold-start cause or a hard
callback/resumption bound. No SDK/runtime pin, public API, capability, application policy,
observation cutoff or qualification tolerance changes. Full exact-candidate mapping,
durable evidence retention, native timing qualification and manual hosted checks remain open.

## Exact-Head Evidence Mapping (2026-11-05)

Reviewed source: `562acc1dedbf169315853900f9395e7ff5160076`. The existing inventory remains
the authority for all 255 criteria and 511 legs; no identity, applicability, requirement,
command or tolerance is changed. The existing `--assemble`, `--report` and `--release`
commands retain this external, pre-freeze review record without a new exporter or schema.

| Inventory category     | Legs | Evidence disposition                                                                                |
| ---------------------- | ---: | --------------------------------------------------------------------------------------------------- |
| Required local         |  217 | 14 exact-head lifetime observations: 11 passed, three failed; 203 lack an eligible assembled result |
| Required external      |   44 | No returned result; all remain missing                                                              |
| Conditional pending    |   50 | Conditions/approvals remain pending, not approved deferrals                                         |
| Follow-up not promoted |  200 | Not promoted; 29 ready bindings do not establish passing execution                                  |

The 14 lifetime observations contain 19 distinct constituent scenarios, with 16 passes and
three failures. Criterion 01 retains cold Begin at 9 ms versus the original 7 ms upper bound;
criterion 16 retains both pending phases and fails at 207 ms versus 205 ms; criterion 21
retains all five cleanup paths and fails ordinary-close cancellation. The suite exits 1,
including its separate cleanup-tail failure. Its actual Wasm SHA-256 is
`e4730e8f068c15ac35b7c7e28e70327fb06097d787eee7eba60fa6ac26c6c6bd`.
Passed observations are not qualified acceptance; the release checker accepts **zero** legs
while record-level gates remain open.

The exact-head wire capture retains 68 exchanges/84 origin requests and 17 matching criterion
sentinels, but its invocation overrides the inventory's output directory. Preserve the actual
command and bytes as supporting evidence rather than invent an inventory-exact command in a
passing result. Similarly, the 203 missing local results are evidence/provenance gaps in this
assembled record, not 203 demonstrated implementation failures.

All 30 reported PR checks are successful for `562acc1d`. However, the retained
[CI run](https://github.com/IABTechLab/edgezero/actions/runs/41976717013) must not be relabelled
as exact-head runtime evidence: Axum and Fastly runtime/build records report
`12287f9f63ab859bf99a6f264e54927ff4ada95d`; Cloudflare and Spin do not embed a source SHA.
Default PR checkout is consistent with an integration merge, but checkout parents are not
retained here. Workflow association alone does not prove exact-head source-to-build identity.
Keep these successful logs separately as supporting evidence.
The retained crosswalk identifies 106 legs with matching primary and additional sentinels
(92 blocking/14 unpromoted follow-up); none gains exact-head credit.

External bundle: `/private/tmp/pr275-562acc1d-closeout/evidence/`. `observations.json` records
all 511 derived dispositions, actual build identity and supporting provenance; `record.json`
is the existing checker's pre-freeze draft with verified raw-artifact hashes. Missing results are omitted, not
fabricated pending observations. `report-report.json` retains 247 missing required results
(203 local/44 external) and all independently evaluable blockers. Assembly, report and release
each exit 1 as expected. Temporary local retention is not durable Q5 publication.
The exact runner image and unobserved package-version precision remain unknown. Preserve
the original metadata draft under `initial-metadata/`; the active record uses a null runner
image and the actual `workerd 2026-10-12` version observation, not inferred package versions.

Full exact-head execution/build binding, companion and comparison proofs, unchanged failed
timing/cancellation gates, manual hosted results, candidate freeze and durable publication
remain open. This mapping neither authorizes deployment nor changes PR review state.

## Independent Batch Cutoff (2026-11-05)

Fail-first WASM tests confirm a coordinator defect: a Workers batch waits for every individual
exchange notification before emitting cutoff. One delivered timer wake can therefore leave
siblings parked. Three regressions fail on the original coordinator: missing cutoff, an
unnotified earlier-own timeout, and batch-timer failure with a previously delivered receipt.
A fourth regression exposes expiry crossed during child cleanup; the producer must recheck
before parking, not reuse its pre-cleanup expiry sample.

The existing batch task now owns one paired absolute-cutoff wait. At expiry it performs one
borrowed terminal poll of every parked pipeline, preserving existing deadline/error guards,
then drops the work and timer before terminal publication. No unordered-queue polling follows
the sweep. Normal completion, exact timer failures, published partials and earlier-own timeout
classification remain intact. A synthetic cleanup-cost control observes 208 ms after imposing
eight milliseconds of release work at a 200 ms cutoff; completion is not backdated to hide it.
The production fix adds no public API, transport, executor or compatibility path.

One unchanged native lifetime run uses working-tree source based on `46019b2d`, with source
patch SHA-256 `42c389d167fdd9a5ead9bdb3634b6b44d916639f3d3ee4ef6406cd742b069035` and Wasm
SHA-256 `43c6f0832019562c1126b8683cd959cab9e3b175b2a6a99b5225dbf4dc7877ee`. The existing
pending-head/body cases pass at **202/203 ms**, retaining the original **205 ms** upper bound.
All three pending-body sources are released by the observed return. This does not measure
native timer-callback entry or isolate abort cost, nor qualify a clean committed candidate.
The capture predates the final timer-state simplification and formatting; it belongs to the
archived patch, not byte-identical final source. Final-source SDK tests and lint are separate
evidence. No native rerun or source relabelling is used to obtain acceptance credit.

The full run remains **16 pass/three fail, exit 1**, plus failed cleanup-tail validation:
cold Begin is **10 ms** versus a **7 ms** upper bound; fast-origin observation is **326 ms**
versus **325 ms**; ordinary-close cleanup still lacks terminal proof. Workspace verification
ran concurrently, so this is not an isolated performance comparison. The new fast-origin miss
is retained, not dismissed as noise or retried for credit. The former 207 ms body-cutoff failure
and every earlier capture remain unchanged in their own evidence.

Raw capture remains in the existing `outbound/lifetimes/workerd` output. The former exact-head
capture was moved to `workerd-before-46019b2d-cutoff`; immutable prior packets are unchanged.
RED/GREEN logs, source patch and capture provenance are under
`/private/tmp/pr275-cutoff-coordinator/`. Unknown runner image, hosted/manual verification,
exact-candidate acceptance mapping, freeze and durable publication remain separate open gates.

## Merged-Head Native Characterization (2026-11-05)

One unchanged lifecycle invocation ran on clean merged source
`b56fccd2448dd7260c9353553cf3fe4c0b385273`, without concurrent builds, verification or a warmup.
The actual Wasm SHA-256 is
`5db99ff9dc2ce9a51d6eb4400358a1de4ad14f0a0beb1b4182debd92c54e8b10`.
The existing suite reports **15 passing/four failing scenarios, exit 1**, plus failed tail
validation. Pending-head/body cutoff returns at **203/202 ms**, within the original 205 ms
bound. The cold path records admission **3 ms**, handler **8 ms** and Begin **9 ms** against
the 7 ms upper bound. Timeout-before-cutoff origin EOF is **1307 ms** against 1305 ms; fast
origin EOF is **286 ms** against 265 ms, and its Next returns at **336 ms** against 325 ms.
The slow-origin diagnostic also retains EOF at 1306 ms. Ordinary-close cleanup remains unproved.
No failure is waived, reanchored, attributed to scheduler noise or retried for credit.

Raw results remain in the existing `outbound/lifetimes/workerd` output. The preceding capture
is preserved separately as `workerd-before-b56fccd2-isolated`; earlier immutable packets are
unchanged. The event stream SHA-256 is
`3c36a72fca1fd54a26e29596e103da107c8bb577612bb3f6992ce1b145ad3b2f`.
This is clean-source characterization before the following egress repair, not frozen-candidate
qualification or evidence for later bytecode. Hosted and durable-publication gates remain open.

## Absolute Response-Write Wait Repair (2026-11-05)

Four deterministic regressions fail on the existing Workers egress race: setup consumes the
whole budget, a pending source poll crosses expiry, a repoll sees expiry without a timer callback,
and seven milliseconds of setup still registers the original ten-millisecond relative delay.
The corrected race reuses the existing outbound absolute timer, checks cancellation and the
application clock before and after operation polls, and never waits for notification to recognize
observed expiry. Timer failures and regression remain transport failures.

Full SDK validation caught an intermediate accounting regression: a completed four-byte write
was hidden by earlier expiry arbitration. The unchanged ownership test now requires the accepted
prefix as before. A private operation-kind distinction within the same race permits one noop-waker
observation of an already-created native promise, never an expired body producer. Ready results
retain the selected failure and accepted bytes; pending promises do not delay termination. No
second executor, transport, timer implementation, public API or compatibility path is added.

This fixes the deterministic write-wait gaps, not the native timing or ordinary-close failures
above. The survivor's one-second ACK can expire while the controller still awaits victim cleanup,
explaining the observed protocol reordering without demonstrating victim release. Failed-case
records remain beyond the validated cursor, so the final tail error is consequential, not separate
proof of a leak. Neither effect justifies weakening the strict cleanup gate or substituting reset.
RED/GREEN and SDK logs are retained under `/private/tmp/pr275-egress-absolute-*.log`; temporary
storage is not durable Q5 publication. Capability levels, provider pins and acceptance bounds
remain unchanged.

Verification passes all five root Rust gates (2,666 tests across 33 binaries, one existing
ignored), all 134 Workers SDK tests and 30 contracts, and strict WASM lint with and without
test utilities. Ten added WASM regressions run through the existing phase, without another
runner or CI job. Both independent reviews find no remaining scoped defect. These results
do not close the clean-head native failures, source freeze or manual hosted qualification.

## Clean-Head Characterization (2026-11-06)

One inventory-exact Workers lifecycle invocation ran on clean source
`6e5ce7650e6ec8d52d0189dedc72fbecc54d5eb8`, without concurrent builds, source edits or warmup.
It uses the same direct Miniflare listener, compatibility settings, pinned runtime and original
budgets. Actual Wasm SHA-256:
`d8f4798f04f3553ea3bbbd5e40b53fa2a5b25994f5cdc8890516e9ef4a1b4c5b`.

The run retains **16 passing/three failing constituent scenarios, exit 1**, plus failed tail
validation. These map to **11 passing/three failing inventory legs**, not sixteen passing legs.
Cold Begin is **10 ms** against the unchanged **7 ms** upper bound; fast-origin slot elapsed is
**152 ms** against **146 +/- 5 ms**. Ordinary-close cleanup still lacks the required terminal
proof. The consequential tail failure is not independently established leakage. No failure is
waived, reanchored or retried for credit.

The raw archive is `/private/tmp/pr275-6e5ce765-native-lifetimes.tar.gz`, SHA-256
`73bae29ef823784cce6d811bcaa6eeef08145764aa071f5aecbffeb9bd1e5996`.
The prior capture is preserved as `workerd-b56fccd2-before-6e5ce765`. Invocation output and the
local command crosswalk are under `/private/tmp/pr275-6e5ce765-closeout/`. This is pre-freeze
characterization, not durable Q5 publication or proof for later source. All original failed
captures remain retained.

## Event-Driven Response Cancellation (2026-11-06)

Review identifies a separate concrete cost: a pending egress operation schedules recurring
one-millisecond abort checks. Three new WASM regressions fail before repair: no event listener
or timer-independent wake, no callback cleanup, and no registration-failure handling.

The existing operation race now owns one abort listener. Its callback only notifies the current
waker; signal state remains authoritative, including after synthetic abort events. Registration
rechecks cancellation, the original deadline and clock regression before operation construction.
Terminal completion and drop remove the same callback and release its waker, including partially
successful registration. The independent absolute deadline wait, `writer.closed()` rejection
watch and genuine zero-delay source fairness yield remain unchanged. No public API, transport,
executor, dependency pin or capability promotion is added.

Full SDK validation exposes a fixture ordering assumption: reading the prefix is not proof that
the native write promise is acknowledged. Prompt abort can precede that acknowledgement, so a
still-pending promise cannot report accepted bytes. The ownership fixture now uses a test-only
oneshot at the second source poll, proving the first write was accounted before terminal action.
The four-byte assertion and every cleanup assertion remain unchanged; no timer, spin or extra
microtask wait is added. Separate tests retain pending and already-ready native-promise cases.

This removes periodic cancellation wakeups, not the provider notification gap or the timing
failures above. Hosted CPU impact is unmeasured. Exact-candidate execution, manual hosted
checks, human re-review, freeze and durable qualification remain open.

Verification passes all five root Rust gates (2,666 tests across 33 binaries, one existing
ignored), all 137 Workers SDK tests, 30 WASM contracts, five native adapter contracts, and both
strict WASM feature configurations. Docs formatting/lint/build and legacy/documentation/coverage
guards pass. Two independent scoped reviews find no implementation defect; neither is maintainer
approval. The initial sandboxed workspace run cannot bind loopback sockets; the permitted TCP
run passes. No native timing or hosted gate is closed by these results.

## Local Reuse And Shutdown Closeout (2026-11-08)

The investigation starts at `26612053050812a6c418f006e9f199b2e36ac328`. A retained Fastly
failure has the first custom-initialization callback in one guest and the remaining four in
another. Receiving a response does not prove the first guest has registered for SDK reuse.
The fixture now observes that guest's native registration and matching pending-handle wait
before each of the next four original requests. The original five-request sequence, same-guest
assertion and 500 ms window remain unchanged. The final callback earns no reuse permission.
Nine deterministic tests cover ordering, malformed and invalid handles, stale/foreign records,
partial tails, bounded cursor consumption and expiry before/after reading. The native smoke
run passes with one guest and four correlated registrations. This is local functional evidence,
not hosted reuse qualification.

The separate direct-workerd routing probe now retains its byte-identical configuration,
JavaScript, Wasm, native log and PID/process-group identity in the caller's existing output.
A fail-first startup regression proves these artifacts survive failure. Previously, the
callee's temporary-directory destructor deleted the native log named by the error.

The optional `cloudflare_native_idle_versus_requests_term_diagnostic` uses the same real
fixture, native listener and process controller, with separate native control output. The local
runtime is workerd package `1.20260415.1` on Apple ARM64, executable SHA-256
`1a83a28ded72d8b4192fca1869e5899ea3d58670a84dcfefe8250ebbb231c7bf`. One
paired capture needs SIGKILL for the idle arm after the unchanged five-second TERM grace;
the request arm exits normally. A subsequent pair exits normally in both arms. Response work
is therefore not necessary for the intermittent failure, and passing samples do not resolve
it. At that stage, no valid native stack capture or verified controller defect was established. Keep this
shutdown investigation open; do not substitute SIGKILL, retries or an extended grace for the
original gate. Runtime pins, provider capabilities and all manual qualification gates remain
unchanged. The diagnostic is opt-in and receives no acceptance credit.

The full prescribed suite passes with `RUST_TEST_THREADS=2`, including all four adapter
CI suites. A separate default-parallel workspace run retains an Axum pending-upload timing
failure: the 100 ms request reaches its terminal observation at 107.941125 ms, beyond the
unchanged 105 ms bound. Its cause is unconfirmed, not dismissed or labelled pre-existing.
A standalone two-thread repeat also fails at 112.950416 ms. Reduced parallelism is not an
established repair. Both failures remain open despite the passing full-script run; no further
retry is used to turn them into acceptance credit.

## Native Timer And Shutdown Investigation

The next diagnostic pass removes a separately demonstrated native re-anchoring window, not
the unconfirmed cause of the historical 107.941125 ms and 112.950416 ms observations. Axum
previously computed a paired-clock remainder and then called `sleep(remaining)`, which anchors
again when Tokio constructs the timer. A deterministic paused-time regression models a 20 ms
preemption after the clock sample: the old conversion produces a 120 ms native target for a
100 ms deadline. Capture Tokio's anchor before sampling the paired clock and use `sleep_until`
with that absolute target instead. The regression then passes at 100 ms. Checked target
overflow is typed Internal, not expiry; absent-runtime and absent-timer-driver tests remain.
A companion test proves that a native timer wake cannot complete a frozen paired clock early.

Temporary, opt-in tracing used the existing native timer and application client injection;
it added no executor, native timer, fetch, warmup or changed budget. Twelve declared isolated
trials and two instrumented workspace runs passed. Their passing traces do not explain the
older failures. The approximately 250-line observer and its connection-test wiring were
removed after retaining the measurements; only the focused production conversion and
co-located regressions remain. The original TCP test and its 105/205 ms limits are unchanged.

Temporary instrumentation of the existing ignored workerd diagnostic invoked macOS
`sample <owned-pid> 1 10 -mayDie -file <retained-report>` directly, without discovering PIDs by
process name. It retained launch identity, actual process group and sample output, and observed
the sampler separately from native cleanup. Native cleanup started immediately with its original
five-second TERM grace; observer results were not used to override the native cleanup verdict. Sampling may perturb
scheduling, so every sampled observation is explicitly unqualified. The instrumentation was
removed after retaining its patch and captures; no profiler dependency or additional CI probe
is added to the permanent runner.

Two of four declared sampled pairs captured the idle failure. Both actual native reports
match the retained parent/PID, shutdown interval and executable UUID
`4C4C4409-5555-3144-A14E-67E446AFA2B0`. Their main-thread samples wait in KJ's Unix event
port (`doKqueueWait` / `kevent`); they show neither application execution nor a CPU-busy loop.
This does not distinguish an unresolved TERM notification from stalled drain/listener/task
propagation. The native signal promise is installed before the listener in the pinned source;
no verified controller or runtime repair follows from the stack alone.

A third, separately labelled temporary `native-control` arm loaded only minimal JavaScript, with no
generated bootstrap, Wasm or HTTP requests. Four declared three-arm runs retain one original
idle failure and three all-pass observations; the JavaScript-only controls all pass. Given
the intermittent original failure, those controls do not establish bootstrap causation.
The original real-fixture gates remain authoritative. No retry credit, runtime-pin change,
provider capability promotion, hosted claim or waiver is introduced by this investigation.

The repaired working tree passes all five primary Rust gates, 2,700 workspace tests across
33 binaries (zero failed, one existing ignored), 650 harness tests (zero failed, three opt-in
tests ignored), and all 30 additional workflow format/lint/WASM/docs commands. The complete
`RUST_TEST_THREADS=2 bash scripts/run_tests.sh` exits 1, however: Axum and Fastly integration
suites pass, then the real Cloudflare routing probe requires SIGKILL after the original
five-second TERM grace. Preserve run `18e64816e16e8f68-663-0`, including exact routing inputs,
native log and process identity. This is a retained failure, not a passing full-suite result
or an established regression classification. The script stops there; later checks executed
independently must be reported separately, not credited as continuation of a successful run.
The subsequent default-parallel workspace command exits 0, as do the separately executed Spin
CI suite, generated-workspace gate, demo tests and all three demo WASM checks, plus the legacy
and documentation guards. They do not replace the failed Cloudflare shutdown result or waive
any historical timing observation. Hosted qualification and independent review remain open.

## Startup-Fixture And Tool Restoration (2026-11-09)

The signed-origin cache checkpoint at `c8fd1939` retained a standalone harness connection
refusal and a later full-script failure caused by a missing temporary repaired Viceroy binary.
The latter is repaired by rebuilding the existing tracked patch, not substituting an installed
binary or changing a pin. The restored source is upstream Viceroy 0.21.0 commit
`a69d982d1d95e13c0fc9f92683b9118639bb4b04`; all 136 library tests pass. SHA-256 identities:

| Input Or Output         | SHA-256                                                            |
| ----------------------- | ------------------------------------------------------------------ |
| Upstream lockfile       | `d642d177081f42aec75d25c0d1124c013d3abcb081886251420a90e9c53c1a2a` |
| Tracked readiness patch | `42ff805147e003e0bec6ec190a5e72cab2ef445582940818c056f2718162663d` |
| Repaired body source    | `1c56533d85a5c26390f9224e102425cbecc0fe4a280513e00095e7117d6722f7` |
| Rebuilt executable      | `d8151efa02d82572951d980429e459f773bba5bab718233b7dbe3b776bb51c37` |

Build settings are Rust 1.98.1, dev profile with the upstream optimization override,
debug information disabled, incremental compilation disabled and two build jobs. Validation
explicitly selects that executable. Wasmtime 48.0.2 and Chrome/ChromeDriver 154.0.8037.92
are restored separately to match existing validation pins; project dependencies, runtime pins
and installed tools remain unchanged.

The historical refusal has no retained endpoint/ordinal evidence sufficient to establish its
cause. A separate deterministic regression demonstrates a fixture gap: the parent releases its
selected port before the responder child binds it. Holding that port in another listener makes
the old child exit 101 before readiness; the regression fails with one failed test (exit 101).
This is evidence for that race, not proof of the historical interleaving.

The fixture now lets the child bind an ephemeral port and retain the listener throughout its
lifetime. After validating its configuration, it atomically publishes a bounded endpoint with
PID and launch nonce. The parent checks at most 1,024 bytes, schema, identity, port, child
liveness and an absolute observation deadline. Its original 45-second startup deadline is
sampled before spawn and is not reset. Every responder caller uses the same private helper;
there is no legacy selection fallback or HTTP warmup. Endpoint publication is readiness only:
the first prescribed response must still attest, and identity failure remains sticky.

Fifteen startup tests pass, including the retained foreign listener, malformed/oversized or
mismatched publication, invalid ports, missing publication, expiry and exited-child controls.
The full two-thread harness run passes 653 tests with zero failures and three opt-in ignored.
Portable `Process::start`, its foreign-listener controls, production transport, acceptance
bounds and capability declarations are unchanged. Neither this fixture repair nor passing
samples waive historical timing/shutdown failures or establish hosted qualification.

The complete `RUST_TEST_THREADS=2 bash scripts/run_tests.sh` exits 0 with the restored repair
explicitly selected: all four adapter CI suites, fresh generated-project compilation, demo
tests and all three demo WASM checks pass. Separate default-parallel runs pass 2,700 workspace
tests across 33 binaries and 653 harness tests, with zero failures and one/three existing
ignored respectively. All five primary Rust gates and strict harness Clippy/formatting pass.
Documentation formatting, lint and build pass using the actual `format`, `lint` and `build`
package scripts; an earlier invocation of nonexistent `format:check` exits 1 and is retained
as a command-selection error, not a documentation defect. Manifest and coverage guards retain
255 criteria/514 legs, complete local bindings and qualification false.

This closes the missing-runtime prerequisite and validates the fixture repair. It does not
retrospectively explain the older connection refusal, native timing misses or intermittent
workerd shutdown failures. The real Cloudflare routing/shutdown gate passes in this run, but
that passing sample is not a shutdown repair. Hosted/manual checks, independent review,
candidate freeze and durable Q5 evidence remain open.

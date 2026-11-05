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

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

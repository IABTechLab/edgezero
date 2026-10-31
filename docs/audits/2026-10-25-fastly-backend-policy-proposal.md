# Fastly Backend Policy and Readiness Review Packet

## Decision Status

**Proposal only, 2026-10-25.** Prepared for G2/G4 against PR #275 source
`b33342d600765e5e17928f3f2164e11941a999b4`. No policy, runtime change, read cap or
production integration is approved by this packet. On 2026-10-25 the requester selected
`B = 31 bounded ladder (Recommended)` in response to the question asking which proposal to
finalize for review, explicitly excluding production integration, runtime changes and hosted
qualification. This records candidate selection, not full G4 resource/primitive approval.
Existing API-only approval remains separate.

The selected proposal is a ceiling-rounded ladder with literal **B = 31**, compared with
a fixed-ceiling **B = 1** alternative. Neither is an implemented backend policy. Arithmetic checks
below establish only properties of the proposed model, not provider acceptance or performance.
The [original feasibility record](2026-10-18-outbound-feasibility.md) and its negative evidence
remain authoritative: stock readiness loses body errors; local repaired timing has failed samples;
hosted qualification and the full engine migration remain open.

## Policy Alternatives

Fix one origin scheme/host/port, wire authority, TLS configuration and effective explicit connect
setting. Count complete provider timer vectors for that tuple, not individual timer values.
Other explicit transport settings, when implemented, also belong to this fixed scope.

| Candidate                      |        Proposed Deadline-Only B | Tradeoff                                                                                                                                               |
| ------------------------------ | ------------------------------: | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| One fixed seven-day ceiling    |                               1 | Fewest configurations and no budget-jitter boundaries; potentially much longer abandoned host work.                                                    |
| Upward-rounded ladder          |                              31 | More configurations and documented jitter boundaries; smaller timer settings for many shorter anchors, not a measured resource or latency improvement. |
| Existing exact-budget identity | Not an acceptable stable policy | Up to 604,800,000 millisecond budget values within one client namespace, plus client-specific names; retains the fixed phase split.                    |

B is not a bound on arbitrary origins, TLS configurations, explicit-setting diversity, sandbox
registrations, concurrent requests, allocations or provider work. Pool/resource limits and retirement
remain their own requirements. The SDK's default maximum of 200 pooled connections per backend is
not a service-wide or memory bound. Both candidates still need resource-policy approval.

## Recommended Arithmetic Candidate

Let `U = 604800000 ms`, EdgeZero's existing seven-day aggregate-budget clamp. The full positive
selected-budget domain is `0 < R <= U`, including sub-millisecond values. Expired/zero budgets
still fail without registration. Do not restrict the API to Fastly's published execution duration
or introduce a minimum logical budget to make the policy work.

At original invocation capture, choose anchor `A` as follows:

- With a configured per-call timeout, `A = min(configured timeout, U)`, even when a caller
  deadline or observation cutoff selects a much shorter `R`. Retain the original request setting;
  this is the same effective clamp already applied by core, not a new request-field mutation.
- Otherwise, `A = R`, the selected duration captured at original invocation, including the default
  budget where applicable. Never substitute the remainder after preparation or registration.

Define `n = ceil(A / 1 ms)` and:

```text
S = {2^k ms | k = 0..29} union {604800000 ms}
Q(A) = the smallest member of S greater than or equal to n milliseconds
```

The final member is exactly U, not `2^30 ms`. There are 31 members. For the existing aggregate
body policy, first-byte and between-bytes timers use the **same Q(A)**. An unset connect timer
uses that same value; an explicit connect setting is independently rounded upward to milliseconds,
not through this ladder. In particular, an explicit 200 ms connect setting must not become 256 ms.
Its validated effective setting stays fixed within the tuple being counted; do not make it or
its identity depend on the later remainder. Zero/oversized connect input handling and exact
provider mapping still require EZ-4's implementation and tests; this arithmetic packet does not
silently define another public range or error policy.

For each fixed tuple, every candidate timer vector is determined by one member of S. Thus the
deadline-only cardinality is at most 31, not `31 * 31`. Equal configured timeouts produce the same
vector under different deadlines/cutoffs or preparation delays. Deadline-only jitter crosses a
bucket only immediately above a listed boundary. Positive sub-millisecond anchors get a 1 ms
provider timer, **not** a rounded-up logical deadline or a minimum accepted request budget.

Since `Q(A) >= A >= R`, an ordinary provider timer armed no earlier than original entry cannot
expire before the logical deadline solely because of this rounding. This is a timer-arithmetic
argument, conditional on provider semantics, not an interruption or scheduler bound. For integer
`n > 0`, `Q(A) < 2n ms`; B31's largest setting is nevertheless still seven days. Long configured
timeouts with earlier deadlines keep that large setting to preserve eligibility. Native optimizer
blocking horizons remain UNKNOWN; timer arithmetic does not establish H(K).

Logical entry time, absolute deadline, source, cutoff semantics, caps, typed errors and native
optimizer dispatch remain unchanged requirements. Candidate names must encode a reviewed policy
version and all transport settings, not client/sandbox identity, exact remainder, path, query,
method, nontransport headers/body, response mode, marker, decoding choice or optimizer options.
This packet does not implement naming, collision checks, registration caching or slack removal.

### Unclosed Policy Evidence

- The pinned SDK serializes durations with `Duration::as_millis()` followed by checked `u32`
  conversion. Supply already-rounded integer milliseconds; otherwise sub-millisecond precision
  is truncated. All members of S fit. SDK representation is not verified hosted acceptance.
  [Fastly 0.13.1 builder source](https://docs.rs/fastly/0.13.1/src/fastly/backend/builder.rs.html)
- Viceroy 0.21.0 stores first-byte/between-bytes values but does not retain/enforce Connect. Its
  three timing getters return `NotAvailable`. Do not call SDK getters and label their result
  connect enforcement, or infer hosted maxima from local source alone.
- SDK/provider acceptance across the emitted range, startup/rolling-deploy identity, registration
  accounting, host-resource lifetime and actual cancellation remain unproved for this policy.
- Future idle-mode mapping requires separate slow-reader/backpressure evidence. Do not blindly
  apply this aggregate between-bytes bucket to idle streams: application pauses cannot consume
  their pending-read idle allowance. No complete idle timer vector or its resource bound is
  approved here; F4 and its capability/evidence gates remain open.

## Readiness Review

The candidate local sequence is supported only on explicitly repaired Viceroy 0.21.0:

1. Borrow live handle identities; recompute a positive ABI wait from the original deadline,
   flooring remaining milliseconds with a 1 ms minimum. Recheck expiry before and after selection;
   rearm an early timeout against the same absolute instant. Equality is expired.
2. Consume a selected pending head with nonblocking `PendingRequestHandle::poll`, not `wait`.
   Preserve slot/handle identity and the original typed outcome.
3. Consume one fresh, same-slot readiness result per raw body read, including EOF/error reads.
   Recheck expiry immediately before and after that read. Never use one readiness observation
   to authorize `read_exact`, repeated reads or a whole-body drain.
4. Release guest-owned handles before recording terminal completion. Cleanup is inside timing
   accounting, and guest release does not prove prompt host teardown.

The repair retains a nonempty chunk or original owned error, skips empty frames cooperatively
and yields on decoder no-output. The retained 135-probe/147-origin matrix exercised destination
capacities 1 and 16,384 bytes, tiny payloads, one 500 ms deadline and fixed two-second provider
timers. **16 KiB is a tested capacity, not an approved production read cap or full-sized-copy
cost measurement.** Registration was outside that window. The sibling probe waits for both heads
before body selection; it does not prove arbitrary mixed-head/body fairness, heterogeneous
deadlines or the production multiplexer.

Stock loses 24 selected body errors in the latest retained comparison. The repaired run preserves
those errors but includes a 505.158 ms terminal sample against the 505 ms target; cleanup ends at
505.117 ms. These failures are not rewritten as passes. The timing amendment allows the existing
Send-first subset, not automatic adoption of this primitive, a new runtime pin or hosted approval.

### Reviewed Input Identities

| Input                        | Identity                                                           |
| ---------------------------- | ------------------------------------------------------------------ |
| EdgeZero source              | `b33342d600765e5e17928f3f2164e11941a999b4`                         |
| Core time source SHA-256     | `bcd7047587f656b6547c860986f0c8f6459a5554850cbac4c1bfd9c480ec7435` |
| Fastly SDK/sys               | `0.13.1`                                                           |
| SDK builder source SHA-256   | `35cf9287819c6a07ed517500a3a01465fa2fe61e23893450080e79986a75004b` |
| Viceroy upstream             | `v0.21.0`, `a69d982d1d95e13c0fc9f92683b9118639bb4b04`              |
| Tracked repair patch SHA-256 | `8693ce34f82cc98dc272bf39379007b17a40dfc19138c55b445378fd9a163759` |

The [repair recipe](https://github.com/IABTechLab/edgezero/blob/b33342d600765e5e17928f3f2164e11941a999b4/patches/viceroy/README.md) and
[latest retained comparison](evidence/2026-10-24-fastly-local-recheck.json) identify separate
stock/repaired executables, source/guest hashes and traces. This source review adds no new host
execution. It cannot turn the unmodified runtime into a supported primitive.

## Executable Arithmetic Check

Run from the worktree root. This is a proposal model, not a test of live adapter dispatch,
provider registration, decoding, cancellation or native optimizer behavior. The checks cover every
ladder boundary at -1/exact/+1 ns within the domain, 1 ns, U, configured timeouts above U,
equal-timeout requests straddling a deadline-only bucket, explicit Connect quantization and zero
selected-budget rejection. Algebra supplies the interval coverage; samples do not exhaust time.

```sh
node --input-type=module <<'JS'
import assert from 'node:assert/strict';
const maxMs = 604800000n;
const unit = 1000000n;
const maxNs = maxMs * unit;
const ceilings = Array.from({length: 30}, (_, i) => 1n << BigInt(i)).concat(maxMs);
const clamp = ns => ns > maxNs ? maxNs : ns;
const ceilMs = ns => (ns + unit - 1n) / unit;
function phase(timeoutNs, selectedNs) {
  assert(selectedNs > 0n && selectedNs <= maxNs);
  const anchor = timeoutNs === undefined ? selectedNs : clamp(timeoutNs);
  assert(anchor >= selectedNs);
  const timer = ceilings.find(ms => ms >= ceilMs(anchor));
  assert(timer !== undefined);
  return timer;
}
assert.equal(ceilings.length, 31);
const samples = new Set([1n, maxNs]);
for (const ms of ceilings) {
  for (const delta of [-1n, 0n, 1n]) {
    const ns = ms * unit + delta;
    if (ns > 0n && ns <= maxNs) samples.add(ns);
  }
}
const configurations = new Set();
for (const ns of samples) {
  const timer = phase(undefined, ns);
  configurations.add(timer);
  assert(timer * unit >= ns);
  assert(timer < 2n * ceilMs(ns));
  assert(timer <= 4294967295n);
  assert(!ceilings.some(ms => ms >= ceilMs(ns) && ms < timer));
}
assert.equal(configurations.size, 31);
let timeoutControls = 0;
for (const rawTimeout of [1n, 800000000n, 1000000000n, 15000000000n, maxNs, maxNs * 100n]) {
  const timeout = clamp(rawTimeout);
  const selected = new Set([1n, timeout, timeout / 2n || 1n]);
  const expected = phase(rawTimeout, timeout);
  for (const ns of selected) {
    assert.equal(phase(rawTimeout, ns), expected);
    timeoutControls++;
  }
}
for (const ms of [511n, 513n, 640n, 655n]) {
  assert.equal(phase(800n * unit, ms * unit), 1024n);
}
assert.equal(phase(undefined, 511n * unit), 512n);
assert.equal(phase(undefined, 513n * unit), 1024n);
assert.equal(ceilMs(200n * unit), 200n);
assert.equal(phase(undefined, 1n), 1n);
assert.equal(phase(maxNs * 100n, maxNs), maxMs);
assert.throws(() => phase(undefined, 0n));
console.log(JSON.stringify({proposal_B: 31, boundary_samples: samples.size,
  timeout_controls: timeoutControls, qualification: false, production_integrated: false}));
JS
```

Observed: **93 boundary samples, 16 timeout-anchor controls**, plus the explicit crossed-bucket
controls. No adapter test, capability or acceptance leg is promoted by that result.

## Required Decisions

| Decision                                                              | State                                                                                      |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Proposal to finalize                                                  | B31 selected by the requester on 2026-10-25; B1 retained as the comparison.                |
| Anchor domain, rounding and fixed-tuple resource-policy approval      | Pending attributed maintainer review; candidate selection does not approve implementation. |
| Repaired-runtime primitive / unchanged stock-runtime disposition      | Pending; unchanged stock is not error-preserving.                                          |
| Production maximum read size and bounded-work/fairness implementation | Pending; exercised buffer capacities alone do not approve these.                           |
| Hosted acceptance, cancellation, slow-reader behavior and H(K)        | Pending external evidence; no deployment authorized.                                       |

Approve those subjects separately in the existing G4 record. A choice of arithmetic candidate,
green local checks, API approval or a generic instruction to continue does not approve the others.
No production timer, backend identity, dispatch slack, runtime pin, public protocol or resource
limit is changed by preparing this packet. The implemented outbound baseline remains protected.

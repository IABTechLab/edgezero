# Spin SDK Eager Progress Dependency Audit

## Current Reviewed Revision (2026-10-28)

The requester authorized updating the coordinated SDK pin to reviewed fork revision
`6c59ed5b4fcb377072763aee0b27a3abdd8edb72`. The upstream contribution remains
[spinframework/spin-rust-sdk#169](https://github.com/spinframework/spin-rust-sdk/pull/169).
SDK 7 now resolves `wasip3 0.10.0+wasi-0.3.2` with `async-spawn` and `wit-bindgen 0.63.0`.
Root, demo, reusable/request-fidelity fixtures and generator use the same immutable source;
the four application lockfiles are refreshed without unrelated dependency updates.

The SDK exposes `spawn_local` and retains its own deprecated `spawn` wrapper. EdgeZero continues
to use `spawn_local` exclusively; it adds no compatibility entrypoint or second executor.
The new join handle detaches on drop. EdgeZero must preserve its independently owned task
cancellation instead of treating the SDK handle as a second lifetime controller.

The SDK review candidate passes local Rust 1.94 workspace tests (33 SDK tests and 33 doctests),
strict Clippy, all 21 WASM workspace builds and twelve native/WASM feature configurations.
Its root-lock audit has the same findings as the upstream base under a frozen RustSec snapshot;
the audit still fails, with no new suppressions or policy changes. Those SDK observations are
not EdgeZero runtime evidence for this new revision. EdgeZero verification is recorded separately
below; the prior-revision results remain historical and do not qualify the new dependency.

### EdgeZero Verification

The SDK now returns a join handle from `spawn_local`. One private provider launcher explicitly
drops that handle and is shared by the four eager/batch scheduling call sites. It does not
change the private scheduler contracts: `OutboundTask` still registers before spawning and its
existing owner retains cancellation. No deprecated SDK call or compatibility entrypoint is used.

The native-request refusal contract initially aborted because SDK HTTP conversion now spawns
body work, while the raw Rust test constructed its request outside an SDK task. The isolated
failure was reproduced. Running construction and dispatch inside the SDK's own `block_on` repairs
the fixture; the refusal status, headers, body and handler/middleware exclusions are unchanged.
The complete 35-case WASM contract suite then passes, without filtering or suppressing the case.

All five root Rust gates pass on Rust 1.98.1, including 2,568 workspace tests with one existing
ignored test. Strict Spin-only and Spin/test-utils WASM Clippy, all eight Spin WASM feature
combinations and the generator's 23 tests pass. The demo passes 60 core tests and its Spin build;
the request-fidelity Spin build also passes. Actual SDK-resource tests pass all 21 cases on
Wasmtime 48.0.2 with the required Preview 3 and HTTP flags. The real eager component passes
callback cleanup, bodyless requests, Begin/Batch progress, late collection, handoff and cutoff.

Spin 4.1.0 passes the complete wire matrix: 68 exchanges and 84 correlated origin receipts.
Artifacts are at `/private/tmp/edgezero-sdk-review-wire-20261028`. The eight-case lifetime suite
passes six cases, including the per-call timeout, fast-origin flow, unobserved Begin/Batch and
both pending cutoff controls. It still exits 1: slow-origin Begin occurs at 27.661 ms rather
than 2 +/-5 ms, and the CPU B-write interval is 531.546..535.226 ms rather than wholly inside
530 +/-5 ms. These failed timing preconditions remain failures, not a demonstrated production
regression or a reason to widen tolerances. Preserve the full run at
`/private/tmp/edgezero-sdk-review-lifetimes-20261028`.

The full ignored fresh generated-workspace gate also passes, including 33 generated core tests,
CLI/configuration checks, all three WASM adapter builds and fallback TCP assertions. This is an
executed integration gate, not only a generator source comparison. Decoder/map dependency guards,
documentation lint/format and legacy/evidence-inventory guards pass. Runtime pins, logical
deadlines, caps, capabilities, acceptance bindings and qualification status are unchanged.
No deployment, candidate freeze or hosted guarantee is authorized by this refresh.

## Historical Checkpoint (2026-10-27)

### Source And Scope

The SDK is pinned to `https://github.com/stackpop/spin-rust-sdk.git` at
`e65ff90dadf4173b08b14194e878ad3acd555337`, based on upstream SDK 7 commit
`2dfd93d2e76e89d7cf74ca43140be7730e0746eb`. The proposed upstream change is
[spinframework/spin-rust-sdk#169](https://github.com/spinframework/spin-rust-sdk/pull/169).
It upgrades WASIp3 from 0.7.1 to 0.9.0 while retaining WASI 0.3.0 interfaces and uses
wit-bindgen 0.62.0, whose executor includes the eager task-queue correction.

The root workspace, demo, reusable fixture, request-fidelity fixture and generator all declare the same SDK version,
Git URL and full revision. Their lockfiles select the SDK and macro from that source.
Applications using the SDK directly must use the same source rather than mixing registry
and Git SDK instances. Dependency guards compare source/revision as well as version.

Main integration adds the request-fidelity fixture to that guard. Its lockfile must select the
same immutable SDK and macro source as the root workspace, not a second registry SDK instance.

The deliberate API migration is `wasip3::spawn` to `wasip3::spawn_local`. There is no alias,
vendored SDK, observer-driven producer, second executor or transport workaround. The private
task-context probe queries and immediately restores the opaque pointer through `wasip3_task_set`;
it never dereferences the executor's private task-set layout. Non-null context alone does not prove
executor identity; one coordinated SDK dependency and runtime tests provide that proof.

### Reproduction And SDK Verification

With wit-bindgen 0.57.1, newly spawned tasks are added after the current task poll returns
Pending, without polling the new tasks before waiting on an older host event. The SDK
regression waits for a short host timer, spawns a nested child and awaits its local result
while a thirty-second host timer remains pending. A five-second host timeout prevents the
long timer from rescuing the old executor. The exact regression times out on the old chain
and passes with this revision.

Local SDK validation used Rust 1.98.1 on macOS: 32 SDK tests, 33 SDK doctests, all 21 WASM
workspace packages, SDK-only strict Clippy, formatting and minimal/json/postgres4-types
feature checks passed. Existing ignored doctests remain ignored. Full-workspace strict Clippy
reports generated gRPC `result_large_err` warnings on this compiler; the same failure was
reproduced on unchanged upstream. Upstream's Rust 1.94 CI remains a separate check.

### Qualification Boundary

Final EdgeZero adoption checks pass all five root Rust gates, strict Spin-only and
Spin/test-utils WASM Clippy, 35 WASM contracts, 21 SDK-resource tests and the eager ownership
component on exact Wasmtime 48.0.2. The demo's 60 core tests and Spin build, fixture's 42 core
tests, generator contracts, fresh scaffold's 33 core tests and Spin build/strict Clippy pass.
The expensive ignored full generated-workspace gate was not rerun locally; these scoped
scaffold checks are not a claim that it ran. Decoder/map, documentation and evidence guards
retain their existing contracts and the inventory remains unqualified.

The signed SDK candidate removes the observed slow-origin batch starvation in local Spin
4.1.0, passes 68 HTTP wire exchanges with 84 origin receipts, and passes the existing eager
ownership component with the explicitly selected Wasmtime 48.0.2. These are local observations,
not hosted qualification. Fixed timeline probes still miss their strict timing targets; they
remain failures. No deadline is extended, tolerance widened, capability promoted, acceptance
leg activated or candidate frozen by this dependency decision. The original logical deadlines,
observation cutoffs, typed errors, caps and exactly-once ownership requirements are unchanged.

The local Spin runs retain a trailing `worker failed: worker panicked` host diagnostic, also
present before this SDK upgrade. Repeated timestamped shutdown traces with identical guest
artifacts place that message after requested shutdown and the last response. This distinguishes
the observed host cancellation diagnostic from an active-exchange failure; it does not prove
clean hosted shutdown, transport teardown or confirmed client delivery. Preserve the diagnostic
and the hosted lifecycle gate rather than suppressing the log or promoting capabilities.

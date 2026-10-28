# Spin SDK Eager Progress Dependency Audit

## Source And Scope

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

## Reproduction And SDK Verification

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

## Qualification Boundary

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

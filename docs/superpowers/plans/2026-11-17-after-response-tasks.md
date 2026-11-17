# After-Response Tasks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Issue #476. A task registered during a request runs after the response leaves the adapter, bounded by its budget, on Fastly and Axum. Axum SIGTERM waits for registered tasks within the grace. Other adapters return a capability error.

**Architecture:** A portable `AfterResponseHandle` sits in request extensions, and `RequestContext::after_response` reads it. Axum spawns each task on its `LocalSet`, gated on response-body drop, counted by a tracker and drained by `drain_tasks`. Fastly queues tasks in a request-local, thread-local queue and drives them after `finish_request`, inside the callback scope.

**Tech stack:** Rust 1.98.1 (`cargo +1.98.1`), tokio (existing features), futures. No new dependencies.

**Spec:** [After-response tasks design](../specs/2026-11-17-after-response-tasks-design.md).

**Base:** stacked on PR #406 (`issue-392-396-production-hosting-spec`, `e37e0df2`).

## Task 1: Core contract

**Files:** `crates/edgezero-core/src/after_response.rs` (new), `lib.rs`, `context.rs`, `manifest.rs`.

- [x] RED: tests in `after_response.rs` for three cases.
  - No scheduler: registration returns `NotImplemented` with status 501, and the task is never polled. A drop probe shows the future was dropped.
  - Zero budget: registration returns an internal (500) error, and the scheduler is never called.
  - Recording scheduler: it receives the exact budget, and the stored future runs to its side effect when polled.
- [x] GREEN: add the `AfterResponseScheduler` trait, the `AfterResponseHandle` handle (`new`, `schedule`) and `RequestContext::after_response`. Export the module from `lib.rs`.
- [x] Add `Capability::AfterResponseTasks` → `"after-response-tasks"` and extend the manifest capability list test.
- [x] Verify: `cargo +1.98.1 test -p edgezero-core` and `cargo +1.98.1 check -p edgezero-core --target wasm32-unknown-unknown`.

## Task 2: Axum scheduler, delivery gate and budget

**Files:** `crates/edgezero-adapter-axum/src/after_response.rs` (new), `lib.rs`, `service.rs`, `response.rs`.

- [x] Add `AfterResponseTracker`, an `Arc<watch::Sender<usize>>`.
  - `register()` returns a guard that decrements the count on drop. Both updates use `send_modify`.
  - `wait_idle()` waits until the count is zero.
  - `pending()` returns the current count.
- [x] Add `AxumAfterResponse { executor, tracker, delivered: watch::Receiver<()> }`, which implements `AfterResponseScheduler`. Its spawned task holds the guard, awaits `delivered.changed()` (an error means the sender was dropped, i.e. the request was released), then binds `let scope = OutboundScopeOwner::callback();` and runs `timeout(budget, scope.run(task))`, warning on timeout. `schedule` returns an internal error once the release has been dropped (`delivered.has_changed()` is `Err`); a spawn error is returned. The guard is created before `spawn` and moved into the future.
- [x] `AxumServiceState` gains a `tracker` field, set through `with_after_response_tracker`. Constructors default to a fresh tracker.
- [x] `dispatch_scoped_request`:
  - Creates `watch::channel(())` before `dispatch_admitted`.
  - Inserts `AfterResponseHandle::new(AxumAfterResponse{..})` into the core request extensions.
  - Attaches the sender to the returned response through `AxumEgressBody::release_after_response_on_drop`.
- [x] `AxumEgressBody` gains `after_response: Option<AfterResponseRelease>`. Plain drop order releases it after the attempt terminates in `Drop`. `finish()` runs on EOF, and Hyper drops the body after the final frame.
- [x] RED/GREEN server tests in `tests/after_response.rs`, through the public `AxumDevServer::serve` and a real listener:
  - **Delivery gate:** the response body is a stream gated by a oneshot. The handler registers a task that sets a flag.
    1. Read the headers and assert the flag is false.
    2. Release the final chunk, read to the end over a keep-alive connection that stays open, then assert the flag becomes true within 1 s.
  - **Budget (executor-level unit test):** the task sets `started`, then awaits `pending()` with a drop probe and a 50 ms budget. Assert it started, the probe is alive at 49 ms, then dropped, and the tracker is zero.
  - **Registration after release** returns an error, and the task is never polled.
  - **Outbound inside a task:** the task sends a request to a local upstream listener after delivery. Assert the upstream received it.
  - **Error response:** a handler that registers a task and then returns `Err` still runs the task.
- [x] Verify: `cargo +1.98.1 test -p edgezero-adapter-axum`.

## Task 3: Axum SIGTERM drain

**Files:** `crates/edgezero-adapter-axum/src/dev_server.rs`, `tests/fixtures/production_host.rs`, `tests/production_hosting.rs`.

- [x] `run_owned` creates one tracker, passes it to the service, and hands it to `drain_tasks`.
- [x] `drain_tasks`: after the connection loop, while the outcome is `Ok`, select on a second signal, `sleep_until(deadline)` and `tracker.wait_idle()`.
  - On timeout, log `"{n} after-response task(s) abandoned at shutdown grace"` and return `grace exhausted`.
  - The `cancel_connections` (prebound) path does not wait.
- [x] Fixture route `/after-task`: it registers a task that sleeps for `FIXTURE_TASK_MILLIS` (default 500), then prints `AFTER_TASK_DONE`, and responds `200 after-task-queued`.
- [x] Subprocess tests:
  - **`sigterm_waits_for_after_response_task`:** a 1.5 s task. Request `/after-task`, read the response, send SIGTERM immediately, and assert `AFTER_TASK_DONE` has not been printed yet. Then expect `AFTER_TASK_DONE`, followed by a clean exit (status 0).
  - **`grace_exhaustion_cancels_after_response_task`:** grace 1 s, task 10 s, SIGTERM. Expect a non-zero exit, `grace exhausted` in the log, no `AFTER_TASK_DONE`, and elapsed time under 4 s.
- [x] Subprocess tests after review: `connections_and_after_response_tasks_share_one_grace` (800 ms drain, 1 s grace, must exit well before 1.8 s) and `second_signal_during_after_response_wait_forces_stop`. The task wait selects idle ahead of the deadline and logs abandoned tasks on every exit path.
- [x] Verify: `cargo +1.98.1 test -p edgezero-adapter-axum --test production_hosting`, plus the existing tests unchanged.

## Task 4: Fastly queue and drain

**Files:** `crates/edgezero-adapter-fastly/src/after_response.rs` (new), `lib.rs`, `request.rs`, `tests/contract.rs`.

- [x] Add a thread-local `QUEUE: RefCell<Option<Vec<(Duration, LocalBoxFuture<'static, ()>)>>>`.
  - `arm()` returns a guard that restores the previous queue.
  - `FastlyAfterResponse` schedules into the queue, or errors when it is not armed.
  - One helper, `with_after_response(clock, dispatch_and_finish)`, arms, runs the closure, then `drain(clock)`. `drain` disarms and takes the tasks (no borrow is held while polling), then joins them, each raced against `native_time(clock).wait_until(start + budget)`, under `block_on(drive_callback(..))`. It warns on timeout or on a driver error. Each deadline is `Deadline::at_instant(start + budget)`, saturating.
- [x] `dispatch_with_handles` inserts `AfterResponseHandle::new(FastlyAfterResponse)` only while a queue is armed, after the `prepare` scratch extensions are copied. `into_core_request` and `capture_egress_for_test` get no handle.
- [x] `run_request_with_delivery` and `send_request_with_registries_and_hooks` both call `with_after_response` inside `with_callback`, with the clock read from the app before dispatch. The registries funnel gains `send_request_with_registries_and_hooks_for_test`. Each lifecycle has a generation, so a retained handle is rejected.
- [x] Host unit tests (feature off):
  - The handle is inserted only while armed.
  - An armed queue collects tasks, and the guard restores the previous queue.
- [x] Viceroy contract tests through `run_request_with_hooks_for_test`:
  - The `deliver` closure pushes `deliver`. The task first awaits a 10 ms timer, then pushes `task`. Expected order: `[deliver, task]`, through both funnels, with a `prepare` hook inserting a rejecting handle.
  - A task that sets `started` and then needs a 10 ms timer, with a 1 ms budget and a drop probe: `started` is set, the probe has dropped, and the task never finished.
  - A task's outbound request to a refused backend reports a Fastly provider failure (the exchange ran after delivery).
  - With no commitment, `may_send_native_response()` is true and the task already ran.
  - A task that tries to register a second task gets an error.
  - A `deliver` closure reporting no commitment: tasks still run before the funnel returns.
- [x] Verify: `cargo +1.98.1 test -p edgezero-adapter-fastly`, and the wasm contract suite under Viceroy (see the tool note below).

## Task 5: Capability matrices and docs

- [x] Adapter `capability()` entries:
  - Axum: `BoundedCooperative`.
  - Fastly: `BoundedCooperative`.
  - Cloudflare and Spin: `Unsupported`, falling through to the wildcard, with an explicit arm for readability.
- [x] Update each `adapter_capability_matrix_matches_contracts` test.
- [x] `docs/guide/capabilities.md`: a new `## After-Response Tasks` section with the matrix and its semantics.
- [x] `docs/guide/handlers.md`: a usage example.
- [x] `docs/guide/adapters/axum.md`: the SIGTERM drain includes tasks.
- [x] `docs/guide/adapters/fastly.md`: tasks run after send and before the callback returns, including the native-replacement note.
- [x] `CHANGELOG.md` under `## Unreleased`: the new API, and the fact that Axum SIGTERM can now exit non-zero when tasks outlive the grace. Keep it out of the migration table that the checker compares.
- [x] Verify the docs: `node scripts/check_outbound_docs_contract.mjs`, then in `docs/`: `npm ci && npx prettier --check . && npm run lint`.

## Task 6: Gates

- [x] `cargo +1.98.1 fmt --all -- --check`
- [x] `cargo +1.98.1 clippy --workspace --all-targets --all-features -- -D warnings`
- [x] `cargo +1.98.1 test --workspace --all-targets`
- [x] `cargo +1.98.1 check --workspace --all-targets --features "fastly cloudflare spin"`
- [x] `cargo +1.98.1 check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin`
- [x] `cargo +1.98.1 check -p edgezero-adapter-fastly --target wasm32-wasip1 --features fastly`
- [x] `cargo +1.98.1 check -p edgezero-core --target wasm32-unknown-unknown`

## Tool note

`.tool-versions` pins Viceroy 0.21.0, but the machine's default binary is 0.17.0. The Fastly contract and wasm library suites ran under a stock Viceroy 0.21.0, built with `cargo install viceroy --version =0.21.0 --locked --root <isolated dir>`, the same way `scripts/ci_viceroy.sh stock` builds it. CI's patched readiness build was not used locally. Docs checks ran under Node 24.20.0.

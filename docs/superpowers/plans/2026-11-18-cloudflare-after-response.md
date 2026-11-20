# Cloudflare after-response tasks implementation plan

**Goal:** Implement #478 over #509 with request-local terminal-egress gating.

**Architecture:** A scheduler registers gated local futures with Workers waitUntil.
A sole release guard follows response conversion and the response pump. Reuse the
portable after-response handle and Cloudflare deadline/outbound scope machinery.

**Tech stack:** Rust 1.98.1, worker 0.8.5, futures oneshot, Workers WASM, Node 24.20.0.

## Task 1: Scheduler and gate

Files: create `crates/edgezero-adapter-cloudflare/src/after_response.rs`; modify `src/lib.rs`.

- [x] Add colocated tests for unpolled registration, release, late registration,
      independent gates, multiple receivers and release on drop. Test non-Send futures.
      Compile the pure gate under cfg(test) on native; gate SDK code on cloudflare + wasm32.
      Add deterministic huge-budget cap, timer-failure drop and sibling survival after panic tests.
- [x] Run `cargo +1.98.1 test -p edgezero-adapter-cloudflare --lib` and observe meaningful
      failures before implementing the gate.
- [x] Implement a shared optional sender list with poison-safe drop, plus a sole release
      guard. Keep mutexes unlocked while signalling or entering the SDK.
- [x] Implement the platform scheduler using Arc<Context>, a signal receiver and
      `wait_until`. After release, create a fresh callback owner and clock/deadline;
      use `native_time` with effective budget capped at 30 seconds, race task/timer,
      drop the loser and log cancellation/timer failure/panic. No unsafe impls or new deps.
- [x] Run crate tests after this code change and target compilation on wasm32-unknown-unknown.

## Task 2: Production wiring and delivery ownership

Files: `src/request.rs`, `src/response.rs` in Cloudflare adapter.

- [x] Add tests that fail for early gate release during a stream and failed preparation
      followed by fallback, and for missing production scheduler installation.
- [x] Create scheduler and guard in `dispatch_with_handles`, install handle before app
      dispatch, attach guard to the runtime. Preserve all other request extensions.
- [x] Add optional release ownership to CloudflareEgressRuntime and WorkersCommitter;
      take the guard into the spawned writer future and keep it until the writer terminates.
      Synchronous no-body/error paths release when committer drops; dispatch cancellation
      drops the retained runtime. Existing assembly errors need no scheduler.
- [x] Run Cloudflare crate tests after changes; compile WASM and run existing egress tests.

## Task 3: Contract and runtime proof

Files: Cloudflare adapter colocated tests and `tests/contract.rs` as appropriate.

- [x] Capture test Context waitUntil promises and await them deterministically. Exercise
      real CloudflareService and registry-based dispatch, gated stream, pending task drop,
      deadline starts after release, stale/recursive handles and independent requests.
- [x] Include no-body response, fallback, failed writer/cancellation, and an outbound
      request constructed inside an after-response task. Contrast with a retained original-scope
      request failing after delivery, and observe outbound cancellation on task timeout. Keep existing contract checks.
- [x] Run cargo tests after test changes, plus Cloudflare guest suites through the
      project's available WASM harness. Record fixture limitations separately.
- [x] Build an owned disposable consumer using the public API and normal Worker entrypoint,
      start local Wrangler/workerd with isolated configuration, and test delayed task completion,
      response-first timing, streaming, timeout cancellation, repeated requests and outbound
      transport. Prove timing discriminates using an inline-delay control. Run a missing-gate
      negative control in an isolated source copy. Stop only owned processes after evidence capture.

## Task 4: Capability and documentation

Files: Cloudflare `src/cli.rs`, `docs/guide/capabilities.md`, `docs/guide/handlers.md`,
`docs/guide/adapters/cloudflare.md`, `CHANGELOG.md`, `scripts/check_outbound_docs_contract.mjs`.

- [x] Change Cloudflare AfterResponseTasks to BoundedCooperative and its matrix test.
- [x] Document terminal boundary, cooperative effective min(budget,30s), shared Workers
      lifetime, outbound scope rule, no persistence/retries and unchanged Spin support.
- [x] Update docs contract expected matrix and current changelog statements.
- [x] Run cargo tests after code changes and the docs contract script, formatting/lint/build.

## Task 5: Verify and publish

- [x] Run independent code review, resolve all supported blocking and optional findings,
      then rerun affected evidence.
- [x] Run cargo +1.98.1 fmt --all -- --check; strict workspace all-feature Clippy;
      cargo +1.98.1 test --workspace --all-targets; workspace check with fastly cloudflare spin;
      Spin wasm32-wasip2 check. Run Cloudflare target Clippy for cloudflare and
      cloudflare,test-utils features so native all-feature unification cannot hide errors.
- [x] Reconcile changed documentation and checks with current source. Retain unavailable
      qualification honestly; do not substitute native tests for Workers execution.
- [ ] Explain the verified changes, sign commits and verify signatures locally. Push and
      create an assigned draft PR against feature/476-after-response-tasks, using the project
      template and Closes #478. Confirm GitHub Verified on new commits.
- [ ] Independently self-review the complete PR against its merge base; resolve findings
      under the implementation authorization, rerun affected checks, and refresh the draft.

## Local verification

Local checks use Rust 1.98.1, Node 24.20.0 and lock-matched wasm-bindgen 0.2.128.
Workspace tests passed (2,826 passed, two ignored); final focused Cloudflare host
checks cover subsequent test refinements. App-demo passed 68 tests. Workspace strict
Clippy, formatting, all-adapter feature compilation and Spin WASI compilation passed.
Cloudflare WASM library and test-utils contracts passed 142 and 35 tests; Chrome
browser ingress passed five tests. Standalone cloudflare contracts and Clippy also run
without test-utils so feature unification cannot hide errors.

A public `run_app` consumer built with worker-build 0.8.3 ran in Wrangler 4.83.0's local
workerd with compatibility date 2025-05-05. Response-first timing was about 4 ms versus
303 ms for an inline 300 ms control. A roughly 609 ms stream finished before the
follow-up task began; timeout dropped a pending task. Real outbound HTTP worked in a
fresh task scope while an earlier-scope future remained revoked. Bodyless responses,
bounded fallback and overlapping request gates passed. The final rebuilt artifact was
probed again. These are local runtime observations, not hosted Cloudflare qualification.

In an isolated negative-control copy, releasing the guard at dispatch return instead
of retaining it in the writer made the streaming contract fail: a task was cancelled
before the stream finished. The unmodified consumer passes. The parent revision also
failed the public registration test with 501, and the old capability matrix failed its
new Cloudflare support assertion. Host panic isolation is an unwind-only check.

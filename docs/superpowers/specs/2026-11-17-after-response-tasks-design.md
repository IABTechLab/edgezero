# After-response tasks on Fastly and Axum

Date: 2026-11-17
Status: implemented. Two independent review rounds resolved every blocking and optional finding before implementation.

Issue: [#476](https://github.com/IABTechLab/edgezero/issues/476). Follow-ups: [#478](https://github.com/IABTechLab/edgezero/issues/478) (Cloudflare `waitUntil`) and [#483](https://github.com/IABTechLab/edgezero/issues/483) (Spin capability error).

Base: [PR #406](https://github.com/IABTechLab/edgezero/pull/406) at `e37e0df2`, which carries #275, #499 and #460. This work needs #406's SIGTERM drain and #275's Fastly callback scope.

## Goal

A handler or middleware registers a task while it handles a request. The adapter runs the task after the response leaves the adapter, and stops it when its time budget runs out. On Axum, SIGTERM waits for registered tasks within the existing shutdown grace. Adapters without support return a capability error when the task is registered. They never drop it silently.

Trusted Server needs this for pull sync and guarded follow-up record writes (its ID-5 and AX-1). The work must not delay the document response.

## Contract

```rust
impl RequestContext {
    pub fn after_response<F>(&self, budget: Duration, task: F) -> Result<(), EdgeError>
    where
        F: Future<Output = ()> + 'static;
}
```

- **Registration** succeeds only when the adapter installed a scheduler for this request. Otherwise it returns `EdgeError::NotImplemented` (HTTP 501), with the message `after-response tasks are not supported by this adapter`. Cloudflare and Spin behave this way until #478 and #483 land.
- **A zero budget** is a programming error. Registration returns `EdgeError::internal` (500), not a client-blaming 422, and the task is never polled.
- **Start.** The task is not polled before the request's response attempt ends at the adapter's terminal egress boundary. It starts whatever the outcome: completed, client disconnected, deadline exceeded, error response or fallback. This boundary is the same one `ResponseEgressOutcome::HostHandoff` describes. It is not proof that the client received the bytes. The work exists for the server's own state, so client receipt does not gate it. Cloudflare's `waitUntil` starts its promise immediately, so #478 must add its own end-of-response gate to meet this rule.
- **Budget.** Each task gets its own budget, measured from the moment the task may start. A task that is still pending when the budget runs out is dropped, which cancels it, and the adapter logs a warning. The task's output is `()`. The application handles and logs its own failures. Retries belong to the application, as Trusted Server's "retried on the next navigation" does.
- **Ordering.** Tasks from one request run concurrently, with no ordering guarantee among them or across requests.
- **Futures need not be `Send`.** They follow the portable rule that core futures are `?Send`. They may capture request handles such as `HttpClient` or `KvHandle` clones, and they may use outbound HTTP and timers.
- **Start outbound work inside the task.** Outbound futures capture the ambient callback scope when they are created. A send future created in the handler belongs to the request's scope, which closes when the request ends on Axum, so it is revoked. Write `ctx.after_response(budget, async move { client.send(request).await; })`, not a future built beforehand.
- **Registration window.** Registration is open while the request is being handled, from dispatch until the response attempt ends. Once the request's tasks are released, a retained `AfterResponseHandle` returns an internal error. A task therefore cannot register more tasks, and the total work per request is bounded by the budgets registered during the request. On Fastly, each armed lifecycle has a generation number, so a handle retained from an earlier request on `serve_app`'s reused instance is rejected rather than queued into a later request.

Core types (in `edgezero_core::after_response`):

- `AfterResponseScheduler: Send + Sync` is an adapter trait with `fn schedule(&self, budget: Duration, task: LocalBoxFuture<'static, ()>) -> Result<(), EdgeError>`.
- `AfterResponseHandle` is a cloneable `Arc<dyn AfterResponseScheduler>` handle. Adapters insert it into request extensions. `RequestContext::after_response` reads it.

The handle is `Send + Sync`, as request extensions require. The futures are local, so each adapter keeps them on its own thread (see below). This follows the pattern already used by `AxumOutboundExecutorHandle`.

## Manifest capability

New `Capability::AfterResponseTasks` (`after-response-tasks`). Applications can list it under `[capabilities] required`, so `edgezero build`, `serve` and `deploy` refuse an adapter that cannot run the tasks.

| Adapter    | Support              | Reason                                                                                                                              |
| ---------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Axum       | `BoundedCooperative` | Tokio timer cancels at the budget, at await points only; drained on SIGTERM within the grace. The prebound `serve()` embedding cancels, by its contract. |
| Fastly     | `BoundedCooperative` | The budget is checked whenever the task yields to the adapter's progress driver; Fastly cannot preempt a blocked source or hostcall. |
| Cloudflare | `Unsupported`        | #478 adds `waitUntil`.                                                                                                              |
| Spin       | `Unsupported`        | #483 keeps the capability error.                                                                                                    |

## Axum

- **Scheduler.** The scheduler holds the request's `AxumOutboundExecutorHandle`, the server's task tracker and a delivery receiver. `schedule` fails with an internal error once the request's release has been dropped. Otherwise it registers the task with the tracker and spawns it at once with `spawn_local` on the existing `LocalSet`; a spawn error is returned to the caller. Before polling the application future, the spawned task waits for the delivery signal. It then runs the future under `tokio::time::timeout(budget, …)`, inside a fresh `OutboundScopeOwner::callback().run(..)`, whose owner is bound to a local for the whole await. That scope gives outbound work started in the task a managed owner, which is revoked when the task finishes, times out or is cancelled.
- **Delivery signal.** `dispatch_scoped_request` creates a `watch` channel before dispatch and moves its sender into the `AxumEgressBody` that it returns. Hyper drops the body after its final frame, or when the connection ends early. Dropping the body drops the sender, which releases every task the request registered. If the service future is cancelled before a body exists, the sender is dropped with that future, and the tasks still run. Requests rejected before dispatch never reach a handler, so they register nothing.
- **Tracker.** An `Arc` holds a `watch::Sender<usize>` (updated with `send_modify`) that counts tasks registered but not yet finished. A guard, created before `spawn` and moved into the spawned future, decrements the count; if `spawn` fails, dropping the future drops the guard. It also decrements on completion, timeout, panic or cancellation. The tracker is created by the lifecycle owner (`run_owned`) and shared by every connection. `AxumServiceState` constructors used outside `run_owned` create their own tracker, which nothing drains.
- **SIGTERM drain.** `drain_tasks` keeps its single deadline (`now + grace`). It first reaps connections as it does today, then waits for the tracker to reach zero, still under that deadline; an already-idle tracker wins over a deadline that passed meanwhile. A second signal still forces an immediate stop. If the grace runs out with tasks pending, it returns the existing `grace exhausted` failure, so the process exits non-zero. On every exit path it logs how many tasks were abandoned. This applies to the production entrypoints, `run_app` and `AxumDevServer::run`. Leaving `run_local` drops the `LocalSet`, which cancels the remaining tasks. As with handlers today, runtime drop still waits for in-flight blocking-pool work such as DNS resolution.
- **Unchanged paths.** The prebound `serve()` embedding path keeps its documented cancel-not-drain semantics: tasks are cancelled with the runtime. Neither the development `run_app` path nor the production path changes its signals or grace configuration (`EDGEZERO__ADAPTER__SHUTDOWN_GRACE_SECONDS`, default 10 s).

## Fastly

- **Scheduler.** A `Send + Sync` scheduler, stamped with its lifecycle's generation, pushes the task and its budget into a thread-local queue when that lifecycle is still the armed one. Each request lifecycle arms the queue and takes it back afterwards. `dispatch_with_handles` inserts the handle only while a queue is armed. It inserts the handle after copying the `prepare` hook's scratch extensions, so a hook cannot replace it. Requests converted by the public `into_core_request` (for example under `serve_custom` or `run_custom` with manual dispatch) and `capture_egress_for_test` therefore get no handle, and registration returns the portable 501.
- **Arming and draining.** Both request funnels, `FastlyService::run_request_with_delivery` and `send_request_with_registries_and_hooks`, wrap dispatch and `finish_request` in `with_after_response`, which arms the queue first and drains afterwards. Each funnel has a test-utils entry with an injected delivery boundary (`run_request_with_hooks_for_test`, `send_request_with_registries_and_hooks_for_test`), so CI exercises both. The drain clock is read from the app before dispatch. After `finish_request` returns, still inside the same `with_callback` scope, they take the queued tasks out of the queue (so no `RefCell` borrow is held while polling) and drive them with `executor::block_on(drive_callback(...))`. The queue is disarmed before the tasks are polled, so registration from inside a task fails, as on Axum. Running inside that scope keeps outbound requests and timers working. Each task races `native_time(clock).wait_until(Deadline::at_instant(start + budget))`, saturating to `DEADLINE_FAR_FUTURE`. The arm guard restores the previous queue even if a nested lifecycle runs.
- **Covered entry points.** Every application entry point already funnels through these two functions: `run_app`, `run_app_with_hooks`, `serve_app*`, `run_app_with_config`, `FastlyService::send*` and the test-utils `run_request_with_hooks_for_test`. The generated `main` and the templates do not change. `send_assembly_error` has no application and registers nothing.
- **Instance lifetime.** The response has already been sent, so the tasks delay only the return from the request callback. On `serve_app`'s reused instance, that delays the next request on that instance. This is documented.
- **Native replacement.** When delivery did not commit and `FastlyRequestDelivery::may_send_native_response()` allows the caller to send its own replacement response, the tasks have already run by the time the funnel returns. They delay that replacement by at most the longest registered budget. This rare failure path is documented and tested. Skipping the tasks would drop work silently, and moving the drain outside the funnel would lose the callback scope.

## Cloudflare and Spin

No scheduler is installed, so registration returns the 501 capability error. Their private egress `spawn` hooks stay unchanged. #478 and #483 own the change.

## Preservation and acceptance

| Scenario                                          | Current                              | Intended                                                       | Evidence                                                                                                  |
| ------------------------------------------------- | ------------------------------------ | -------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Axum handler registers a task, streaming response | n/a                                  | Task not polled until the final body frame is handed to Hyper  | In-crate server test: gated streaming body, keep-alive connection; task flag false before release, true after the client reads the full body |
| Axum task exceeds its budget                      | n/a                                  | Task started, then dropped at the budget; tracker returns to zero | Executor-level unit test: started flag, probe alive before the budget, dropped after, tracker zero     |
| Registration after release                        | n/a                                  | Internal error, task never polled                              | Axum unit test and Fastly host test                                                                      |
| Axum SIGTERM with a pending task                  | Connections drained only             | Process waits for the task within the grace, exits 0           | Subprocess test: a 1.5 s task, SIGTERM sent right after the response; the marker is absent at SIGTERM, then printed, then a clean exit |
| Axum SIGTERM, task longer than the grace          | n/a                                  | `grace exhausted`, non-zero exit, task cancelled               | Subprocess test with a 1 s grace and a long task; no completion marker                                    |
| Fastly handler registers a task                   | n/a                                  | Runs after `deliver` returns, inside the callback scope        | Viceroy contract test: recorded order is `delivered` then `task`; a timer inside the task works; registration from inside a task fails |
| Fastly delivery not committed                     | n/a                                  | Tasks still run before the funnel returns                      | Viceroy contract test: no commitment, `may_send_native_response()` is true, and the task already ran   |
| Fastly task uses outbound HTTP                    | n/a                                  | The request reaches Fastly's transport after delivery          | Viceroy contract test: a refused backend yields a Fastly provider failure, not a closed-scope error    |
| Axum connections and tasks share the grace        | n/a                                  | No fresh deadline for tasks                                    | Subprocess test: an 800 ms drain plus a pending task exit near the 1 s grace, not ~1.8 s              |
| Axum second signal during the task wait           | n/a                                  | Immediate forced stop                                          | Subprocess test                                                                                        |
| Fastly manual `into_core_request`                 | n/a                                  | No handle; 501 at registration                                 | Host unit test: no handle while unarmed; a retained handle is rejected by a later lifecycle             |
| Fastly `prepare` hook inserts its own handle      | n/a                                  | The adapter handle wins                                        | Viceroy contract tests insert a rejecting handle in `prepare`; tasks still run                          |
| Fastly `run_app` / `serve_app` funnel             | n/a                                  | Same behavior as the service funnel                            | Viceroy contract test through `send_request_with_registries_and_hooks_for_test`                         |
| Axum task uses outbound HTTP                      | n/a                                  | Request started inside the task succeeds after delivery        | In-crate test against a local upstream listener                                                          |
| Fastly task exceeds its budget                    | n/a                                  | Dropped at the budget                                          | Viceroy contract test with a pending task and a short budget                                              |
| No scheduler (Cloudflare, Spin, bare core)        | n/a                                  | 501 `not_implemented` at registration                          | Core unit test                                                                                            |
| Existing Axum shutdown and egress behavior        | Drain, grace, second signal, cancel  | Unchanged when no tasks are registered                         | Existing `production_hosting` and egress tests                                                            |
| Capability gate                                   | n/a                                  | `required = ["after-response-tasks"]` passes on Axum and Fastly, fails on Cloudflare and Spin | Adapter matrix tests and the manifest list test                                      |

## Out of scope

- Cloudflare `waitUntil` (#478) and Spin's adapter-named error (#483).
- Retry policy, persistence of failed tasks, or a global concurrency limit.
- Proof of client receipt. This uses the same egress boundary the response-egress matrix already documents.

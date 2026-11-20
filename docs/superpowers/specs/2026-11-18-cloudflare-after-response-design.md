# Cloudflare after-response tasks

Issue: #478. Base: #509 (`feature/476-after-response-tasks`, `c3ba7e56`).

## Goal and scope

Enable the existing `RequestContext::after_response(budget, task)` API on Cloudflare
Workers without delaying response delivery. Preserve #476's Fastly and Axum behavior
and Spin's unsupported capability. This delivers reusable adapter infrastructure;
Trusted Server integration and deployed-provider qualification are separate work.

## Contract

A registered local, non-Send future remains unpolled until the request's terminal
egress boundary. For streamed responses this is completion or termination of the
adapter-owned writer, including cancellation, source errors and transport failure.
For bodyless responses it is native response handoff. This boundary does not certify
client receipt. Conversion failure retains the gate through the bounded fallback;
failed fallback and abandoned dispatch also release it. Registration closes at release,
and retained handles or recursive registration fail without polling the supplied task.

Each accepted task is retained with the request's Workers `Context::wait_until` before
dispatch returns, but awaits a request-local signal first. A fresh outbound callback
scope and the task deadline are created only after release. The task must construct
outbound requests inside its future; futures made earlier keep their original scope.
The effective cooperative budget is `min(requested budget, 30 seconds)`. A pending
future is dropped when this expires; a single blocking poll cannot be preempted.
Zero budgets retain the core API's existing rejection. Very large durations cannot
wrap JavaScript timer integers. Tasks on separate requests never release each other.

Workers independently limits all waitUntil work to a shared allowance of up to 30
seconds after response completion or disconnect, and may terminate work earlier.
This is bounded best-effort execution, without persistence, retries or a guarantee
that a task finishes. Platform CPU and memory limits also continue to apply.

## Architecture and alternatives

Use a request-local scheduler containing the existing SDK `Arc<Context>` and a
mutex-protected optional list of oneshot senders. The SDK implements Send/Sync for
Context; local futures live only in the waitUntil closures. No unsafe trait impls,
ambient task queue, new dependencies or core API changes are needed.

Scheduling adds a release receiver while registration is open, then calls waitUntil
outside the mutex. The queued future awaits release and races its task against the
adapter's existing deadline timer in a fresh callback scope. Log timeout, timer failure
and recoverable task panic without changing the already-delivered response. Panic
recovery requires an unwinding build; ordinary aborting WASM panics may terminate
the invocation. A host unwind test does not qualify Workers panic recovery.

One release guard owns closure of registration and signalling all accepted tasks.
It lives in `CloudflareEgressRuntime`, transfers to `WorkersCommitter`, and moves into
the actual response pump on spawn. Drop covers successful completion, caught panic,
cancellation, bodyless handoff, synchronous errors and dispatch abandonment. Fallback
uses the same committer so failed preparation cannot release tasks prematurely.

Installing the handle in the converged real dispatch path covers CloudflareService,
run_app and dispatch_app. Standalone request conversion has no delivery ownership and
continues without a scheduler. Capability reporting becomes BoundedCooperative,
matching Fastly/Axum, with Cloudflare's effective cap and platform limits documented.

Calling waitUntil directly at registration would permit work before delivery. Keeping
an ambient queue would complicate overlapping requests and non-Send ownership. The
release-signal design reuses existing egress ownership and supports concurrent requests.

## Acceptance and preservation

| Scenario                                | Required observation                                                      |
| --------------------------------------- | ------------------------------------------------------------------------- |
| Normal registration                     | Response succeeds and task completes after terminal egress                |
| Slow streaming response                 | Task stays unpolled while writer is active; full budget starts at release |
| Pending task                            | Future is dropped at its effective budget                                 |
| Bodyless/error/fallback/cancel/drop     | Gate releases exactly once after terminal attempt                         |
| Late/recursive registration             | Typed error; supplied future remains unpolled                             |
| Multiple tasks and overlapping requests | Every accepted task runs; gates remain independent                        |
| Outbound inside task                    | Real adapter HTTP client works after original callback scope closes       |
| Required capability                     | CLI accepts Cloudflare after-response-tasks; Spin remains unsupported     |
| Existing egress                         | Framing, observations, cancellation and bounded fallback remain correct   |

Use colocated gate/scheduler tests, Cloudflare WASM fixture tests and the existing
contract/egress suites. Fixture tests must await accepted promises rather than only
counting them. Run the actual Worker through local Wrangler/workerd using a disposable
consumer of the public API: response timing, delayed marker, timeout cancellation,
streaming, repeated requests and outbound side effects. Compare an inline-delay control
and a base or deliberately broken gate in an isolated copy. Keep fixture results separate
from workerd evidence and from unexecuted hosted Cloudflare deployment qualification.

# Spin after-response capability error

Date: 2026-11-18
Status: approved for implementation.

Issue: [#483](https://github.com/IABTechLab/edgezero/issues/483). Base: [PR #509](https://github.com/IABTechLab/edgezero/pull/509)
(`feature/476-after-response-tasks`), which adds `RequestContext::after_response`, the
`after-response-tasks` capability, and the portable `501` returned when no scheduler is installed.

## Goal

A Spin handler that calls `ctx.after_response(budget, task)` gets an explicit capability error
that names the Spin adapter. The task is never polled and is dropped at registration. Apps that
declare `after-response-tasks` as required are refused for Spin before deployment.

## Why an explicit error, not support

The issue fixes the scope: Spin returns a capability error. Spin's request lifecycle gives no
documented guarantee that work continues once the response is complete, and hosted Spin
(Akamai Functions) publishes no post-response execution budget. Running tasks there could drop
them silently, which is the outcome #476 forbids. Real support would need its own design and
platform evidence; it is out of scope.

## Current behavior on the base

Spin installs no `AfterResponseHandle`. Registration falls through to core's generic
`EdgeError::not_implemented("after-response tasks are not supported by this adapter")`. The
capability matrix already reports Spin `Unsupported`, so `required = ["after-response-tasks"]`
already fails for Spin.

The gap is that the error is implicit. It is indistinguishable from a bare core context or an
adapter that forgot to install a scheduler, and it says nothing about which adapter refused.

## Contract

- **Status and kind.** Registration on any Spin request returns `EdgeError::NotImplemented`
  (HTTP `501`, kind `not_implemented`), unchanged from the base.
- **Message.** `after-response tasks are not supported by the Spin adapter`. Defined, with no
  cfg gate, as `edgezero_adapter_spin::AFTER_RESPONSE_UNSUPPORTED` in `lib.rs` (beside
  `SPIN_PLATFORM`), so host-side app tests can name it too. Applications should branch on the
  status or kind, not the message.
- **Task.** Never polled. Dropped before `after_response` returns.
- **Zero budget.** Unchanged core rule: core rejects it with `500` before the scheduler is called.
- **Reach.** The handle is installed where every native Spin request becomes a core request
  (`into_core_request_head`). That covers `run_app`, `run_app_with_platform`, `dispatch_app`,
  `AppExt::dispatch`, `dispatch`, `dispatch_with_kv_label` and the public `into_core_request`.
  Not covered, by construction: `send_assembly_error` (no app, no handlers), and the test-utils
  `dispatch_ingress_*` bridges, which build the core request with `request_builder()` and already
  omit `SpinRequestContext` and ingress metadata. Those keep core's generic `501`.
- **Handler-inserted handles.** Spin has no pre-dispatch `prepare` hook. A handler or middleware
  that inserts its own `AfterResponseHandle` into extensions replaces the adapter's, as with any
  extension. That is the application's explicit choice and is not guarded.

## Design

New host-compilable module `crates/edgezero-adapter-spin/src/after_response.rs`:

```rust
// lib.rs, ungated
pub const AFTER_RESPONSE_UNSUPPORTED: &str =
    "after-response tasks are not supported by the Spin adapter";

// after_response.rs, gated any(test, all(feature = "spin", target_arch = "wasm32"))
use edgezero_core::after_response::{AfterResponseHandle, AfterResponseScheduler};

struct SpinAfterResponseUnsupported;

impl AfterResponseScheduler for SpinAfterResponseUnsupported {
    fn schedule(&self, _budget: Duration, task: LocalBoxFuture<'static, ()>)
        -> Result<(), EdgeError>
    {
        drop(task);
        Err(EdgeError::not_implemented(crate::AFTER_RESPONSE_UNSUPPORTED))
    }
}

pub(crate) fn unsupported_handle() -> AfterResponseHandle;
```

`request::into_core_request_head` inserts `after_response::unsupported_handle()` next to the
existing `SpinRequestContext` and outbound client.

The module compiles on the host (no `spin_sdk` imports), so its unit tests run under the plain
`cargo test --workspace`. The insertion is covered end to end in `tests/contract.rs`
(`ingress_contract`, `spin,test-utils`, wasm32 under Wasmtime): a real `SpinRequest` goes through
`test_support::dispatch_request`, which shares `into_core_request_head` with the production
funnels. CI runs that suite; it does not run the adapter's wasm32 `--lib` tests, whose binary
imports Spin KV resources that plain Wasmtime does not provide.

Core changes only in documentation: its module doc and its `AFTER_RESPONSE_UNSUPPORTED` doc no
longer imply that an unsupported adapter installs no handle. No change to other adapters, the CLI
capability matrix, or the manifest.

## Docs

- `docs/guide/capabilities.md`: the after-response bullet says Spin installs a rejecting
  scheduler that returns `501` naming the Spin adapter; Cloudflare keeps its own sentence.
- `docs/guide/adapters/spin.md`: short "After-response tasks" section.
- `CHANGELOG.md`: under the existing after-response heading, Spin no longer listed as pending
  (#483); state the adapter-named `501`.

## Acceptance and preservation

| Scenario | Base | Intended | Evidence |
| --- | --- | --- | --- |
| Spin handler registers a task | 501, generic message | registration error is 501 with the Spin message | wasm32 contract test dispatching a real `SpinRequest` (handler echoes status and message in a 200 body); `spin up` probe propagates the error with `?` and observes a 501 response |
| Task registered on Spin | dropped unpolled | dropped unpolled | host unit test with poll and drop probes |
| Zero budget on Spin | 500 | 500 | existing core test (core rejects before any scheduler) |
| Bare core context (no adapter) | 501, generic message | unchanged | existing core unit test |
| `required = ["after-response-tasks"]` on Spin | refused | refused | existing `adapter_capability_matrix_matches_contracts` |
| Existing Spin routes | unchanged | unchanged | existing Spin suites; `spin up` control route |

Runtime probe (`spin up` 4.1.0, scratch copy of `examples/app-demo`, not committed): a route
registers a task that would log a unique marker and returns the registration error's status and
message. Expect `501` with the Spin message, no marker in the Spin logs after a pause, and an
existing route still `200`.

A negative control removes the insertion and its import and confirms the wasm32 contract test fails with the
generic message.

## Out of scope

- Running after-response tasks on Spin.
- Cloudflare (#478, PR #512). PR #512 edits the same capabilities bullet; whichever lands second
  rebases that sentence.

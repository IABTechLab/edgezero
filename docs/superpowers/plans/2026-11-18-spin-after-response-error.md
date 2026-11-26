# Spin after-response capability error — plan

Design: [`2026-11-18-spin-after-response-error-design.md`](../specs/2026-11-18-spin-after-response-error-design.md).
Issue: #483. Base branch: `feature/476-after-response-tasks` (PR #509).

## Tasks

1. **Failing tests first.**
   - `crates/edgezero-adapter-spin/src/after_response.rs` host unit tests: the handle returns
     `501` with `AFTER_RESPONSE_UNSUPPORTED`; the task is never polled and is dropped at
     registration (drop probe uses core's `missing_trait_methods` expect pattern); a zero budget
     through the Spin handle returns `500`.
   - `crates/edgezero-adapter-spin/tests/contract.rs` (`ingress_contract`) wasm32 test: a real
     `SpinRequest` dispatched through `test_support::dispatch_request` to a route that registers a
     task gets a `501` registration error with the Spin message (the handler echoes status and
     message in a `200` body); the task is never polled and is dropped once; a control route on
     the same app returns `200`.
2. **Implement.** Define `AFTER_RESPONSE_UNSUPPORTED` ungated in `lib.rs` (no `pub use`, which
   the `restriction` lint group denies). Add the module (gated like `ingress`: `any(test,
   all(feature = "spin", target_arch = "wasm32"))`), insert the handle in
   `into_core_request_head`.
3. **Docs.** `capabilities.md` bullet, `adapters/spin.md` section, `CHANGELOG.md` line, and core's
   `after_response` module and constant docs (an unsupported adapter may install a rejecting handle).
4. **Negative control.** Run the contract test with the constant present but without the module, import and
   insertion; the wasm32 contract test must fail with the
   generic core message. Restore.
5. **Verification.**
   - `cargo fmt --all -- --check`
   - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
   - `cargo test --workspace --all-targets`
   - `cargo check --workspace --all-targets --features "fastly cloudflare spin"`
   - `cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin`
   - Spin wasm clippy (`spin`, `spin,test-utils`) as in `format.yml`
   - Spin wasm32 `--test contract --test sdk_resources` under Wasmtime 48.0.2 (the CI sdk-tests phase)
   - `node scripts/check_outbound_docs_contract.mjs`; docs `npm run lint && npm run format`
   - Runtime: `spin up` (4.1.0) on a scratch copy of `examples/app-demo` outside the repo, with a
     route whose task would log `AFTER_TASK_RAN`; expect `501` with the Spin message, no marker
     in the logs after 2 s, and an existing route `200`.

# Safe app-config validation diagnostics implementation plan

**Goal:** Preserve explicitly approved operator diagnostics while keeping untrusted validation details out of logs and HTTP responses.

**Architecture:** Shared bounded core sanitizer, default-empty static metadata registry, optional derive registration, separate operator and public error views. No new dependencies or store reads.

- [x] Add failing sanitizer tests in a colocated `config_diagnostics` module: exact path/code approval, nested arrays, unknown codes/keys, value/message canaries and limits. Run `cargo +1.98.1 test -p edgezero-core`; implement the bounded registry walker and rerun.
- [x] Add default registry metadata and optional derive attribute in `app_config.rs` and `edgezero-macros/src/app_config.rs`. Test normal derives and invalid registry declarations/attributes; preserve existing derives and manual metadata implementations.
- [x] Add failing extraction and error-rendering tests. Add opt-in diagnostic-bearing error variant and accessors; replace `first_violating_field` with sanitized reporting. Update logger operator formatting. Keep 503/kind/retry/public text and one read. Run scoped core tests after each change.
- [x] Add CLI typed validate/push/diff regression coverage and replace their raw validation formatting with shared reporting. Preserve secret pruning and perform no remote operations on validation failure. Run CLI tests.
- [x] Add affected adapter extraction/response contract coverage (Fastly, Cloudflare, Spin, Axum) and documentation in configuration and migration guides. Run applicable scoped tests after changes.
- [x] Review the combined diff against #457, including limits and secret/value paths. Run format, full-feature clippy, workspace tests, adapter feature checks, Spin WASM compile, and available actual adapter contract runners. Use Rust 1.98.1. Record blocked runners separately.
- [x] Report exact tested revision/worktree, API compatibility adjustments, verification, and remaining limitations. Publication remains separate.

## Verification result

Verified on Rust 1.98.1 and Node.js 24.20.0. The final workspace run passed
1,565 tests across 21 targets. Scoped core tests ran separately without
`test-utils` to exercise compiled-feature refusal, and with it to exercise the
same policy's success. The logger regression failed when temporarily routed to
`message()` and passed after restoring `operator_message()`.

Passed: workspace formatting, full-feature/all-target Clippy, workspace tests,
`fastly cloudflare spin` feature compilation, required Spin WASM compilation,
all three WASM adapter test-target compilation checks, Axum's extraction/response
contract, docs ESLint, and full docs Prettier checks. Independent source review
found no blocking defects; boundary controls and nested extraction coverage were
added in response to its coverage observations.

WASM runtime qualification remains outstanding: local Viceroy 0.17.0 cannot load
the current Fastly SDK (project pin 0.21.0); the Cloudflare runner is 0.2.122
instead of the locked 0.2.128 and has no browser driver; Wasmtime is unavailable.
These are compilation results, not claims of passing provider runtime contracts.
No hosted-provider or downstream Auctioneer execution is claimed.

Run the primary Rust gates locally before pushing, then the standalone integration harness. Report each actual result:

1. `cargo fmt --all -- --check`
2. `cargo clippy --workspace --all-targets --all-features -- -D warnings`
3. `cargo test --workspace --all-targets`
4. `cargo check --workspace --all-targets --features "fastly cloudflare spin"`
5. `cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin`
6. `cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml`

Use `scripts/run_tests.sh` for the complete prescribed suite, including excluded
demo/generated workspaces and target feature matrices. Runtime and hosted
qualification have separate gates in `crates/edgezero-test-harness/SUITES.md`.
Retain every failed attempt; a partial or retried pass is not a complete-suite pass.

Run the full CI gate checks locally before pushing. Run all six commands sequentially and report any failures:

1. `cargo fmt --all -- --check`
2. `cargo clippy --workspace --all-targets --all-features -- -D warnings`
3. `cargo test --workspace --all-targets`
4. `cargo test -p edgezero-core --doc`
5. `cargo check --workspace --all-targets --features "fastly cloudflare spin"`
6. `cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin`

If any step fails, show the errors and suggest fixes. Do not proceed to the next step until the current one passes.

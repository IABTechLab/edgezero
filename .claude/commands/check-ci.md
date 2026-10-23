Run the full CI gate checks locally before pushing. Run all five commands sequentially and report any failures:

1. `cargo fmt --all -- --check`
2. `cargo clippy --workspace --all-targets --all-features -- -D warnings`
3. `cargo test --workspace --all-targets`
4. `cargo test -p edgezero-core --doc`
5. `cargo check --workspace --all-targets --features "fastly cloudflare"`

If any step fails, show the errors and suggest fixes. Do not proceed to the next step until the current one passes.

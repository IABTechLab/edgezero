# EdgeZero Test Harness

Standalone, non-published native Rust runner for cross-adapter HTTP integration tests.
Its workspace and lockfile are isolated from production crates and deployable fixture
applications. Unit and adapter contract tests remain in their owning crates.

```sh
cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml
scripts/smoke_test_reusable_app.sh --adapter axum --suite outbound-wire
```

The shell entrypoint selects this runner; fixture applications, their target directory,
runtime configuration and per-run evidence remain under `tests/fixtures/reusable-app`.
Runs record both the fixture lock and the separate harness lock. See the
[fixture guide](../../tests/fixtures/reusable-app/README.md) for runtime tooling,
suite-specific assertions and qualification boundaries.

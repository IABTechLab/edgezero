# EdgeZero Test Harness

Standalone, non-published native Rust runner for cross-adapter HTTP integration tests.
Its workspace and lockfile are isolated from production crates and deployable fixture
applications. Unit and adapter contract tests remain in their owning crates.

```sh
cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml
scripts/smoke_test_reusable_app.sh --adapter axum --suite outbound-wire
scripts/smoke_test_reusable_app.sh --adapter axum --suite outbound-lifetimes
scripts/smoke_test_reusable_app.sh --adapter spin --suite ci
scripts/smoke_test_reusable_app.sh --adapter all --suite ingress-fidelity
```

The shell entrypoint selects this runner; fixture applications, their target directory,
runtime configuration and per-run evidence remain under `tests/fixtures/reusable-app`.
Runs record both the fixture lock and the separate harness lock. See the
[fixture guide](../../tests/fixtures/reusable-app/README.md) for runtime tooling,
suite-specific assertions and qualification boundaries.

`spin --suite ci` is the local and hosted-CI entrypoint. It keeps native tests,
synchronous Wasmtime SDK tests, async CLI execution, actual Spin HTTP and ingress
fidelity in separate logged phases. The suite requires installed pinned runtimes,
builds explicit fixture packages, validates persisted inventories and owning-phase
markers, and fails on command or cleanup errors. It never downloads a runtime or
turns local execution into hosted qualification. Ingress fidelity uses this same
process owner and retains its separate raw-request fixture and observation baseline.

## Bounded Startup

TCP readiness is provisional. The first prescribed response must match a launch-only
nonce, an independently embedded build identity and the expected fixture mode while
the owned child is still alive. The nonce is not sent in the HTTP request, and no
extra HTTP health request changes cold-request ordinals. Missing or mismatched identity
fails closed; an unverified response is not acceptance evidence.
Only explicitly prescribed headerless fault observations may remain pending/no-credit
until the prescribed identity-bearing recovery response. This exception does not allow
an unstamped recovery response to borrow a later response's identity.

Startup has one 45-second deadline beginning before process creation. HTTP exchanges
use at most 12 seconds, shortened by the remaining startup budget, across connection,
write, read and trailer handling. Capture is bounded to a 16 KiB status line, a separate
16 KiB aggregate field/chunk-metadata budget, and 1 MiB of text; existing smaller binary
capture limits remain in force. These are fixture limits, not production ingress claims.

Build identity is content-bound to source files (including dirty and untracked inputs),
locks/manifests, compiler/Cargo identity and build configuration. Generated outputs and
launch nonces are excluded. Cargo still validates every build; there is no skip-build
shortcut. Actual launches receive a freshly checked build identity and a separate fresh
nonce. Process-supervisor unit fixtures pass their fixed synthetic identity explicitly,
without probing the real compiler inside cancellation or readiness timing windows.

The existing ephemeral-port handoff is not atomic reservation. Identity checks mitigate
wrong-listener success without port reselection, respawn or retry-until-green. Modes
without an identity-bearing response, including canonical Fastly assembly failures,
remain explicitly unverified rather than gaining a test-only production lifecycle API.

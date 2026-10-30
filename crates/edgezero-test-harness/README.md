# EdgeZero Test Harness

The home for cross-runtime integration testing. The native Rust controller builds and
launches fixture applications, operates controlled origins, verifies HTTP observations
and releases owned processes. Adapter API/SDK contracts remain in their owning crates.

## Run Integration Tests

Run from the repository root with the tool versions in `.tool-versions` already installed:

```sh
scripts/test_integration.sh --adapter axum --suite ci
scripts/test_integration.sh --adapter fastly --suite ci
scripts/test_integration.sh --adapter cloudflare --suite ci
scripts/test_integration.sh --adapter spin --suite ci
```

These are the same commands CI uses. The shell launcher only invokes this Rust controller;
it contains no suite selection, assertions or process supervision. CI installs tools and
uploads artifacts, while Rust owns the test phases and persisted-evidence checks.

| Adapter    | Phases                                                                                                                                                           |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Axum       | Guarded native contracts/TCP tests, fixture compilation, redirect wire acceptance                                                                                |
| Fastly     | Native preflight, SDK contracts, runtime unit tests, feature matrix, ingress, fixture compilation, lifecycle smoke, client progress, redirect wire acceptance    |
| Cloudflare | Native preflight, guest-shim tests, SDK contracts, browser ingress, runtime unit tests, feature matrix, real-runtime ingress, fixture compilation                |
| Spin       | Pinned tools, feature matrix, native contracts, SDK/private-boundary tests, shared guest scenarios, async CLI component, actual HTTP wire/policy matrix, ingress |

Cloudflare's full outbound wire suite remains a focused, separately executable acceptance
leg; this consolidation does not silently activate or qualify it in CI. Fastly client
progress must retain its existing unqualified exit and exact 12/23-probe inventories.
Neither passing a local runtime suite nor green CI establishes hosted qualification.

Spin's full adapter library test binary imports provider KV resources that plain Wasmtime
does not implement. Preserve its WASM compile/Clippy coverage without claiming execution;
the SDK phase runs the existing executable contract/resource targets, and the separate
shared-scenario phase requires all eight migrated wrapper tests.

Spin normalization acceptance combines colocated shared-head tests for provider-inexpressible
fields, native SDK bodyless/205 disposal, and eight actual HTTP representation controls. The
existing 68 redirect exchanges and 26 independent-cap cases remain separate inventories. One
CI invocation checks every required proof in its owning phase; this adds no header-fidelity or
hosted qualification claim.

Use `--suite outbound-wire`, `--suite ingress-fidelity` or the other focused suites for
iteration. See [suite assertions and runtime setup](SUITES.md) for their exact contracts,
provider-specific tools, retained negative observations and hosted qualification limits.

## Ownership

```text
src/                         native controller, suites and evidence validators
fixtures/reusable-app/       shared application and provider-specific guest entrypoints
fixtures/request-fidelity/   raw-ingress applications and observation baseline
acceptance/                  outbound case inventory and external runbook source
```

The controller and the two guest workspaces retain separate manifests and lockfiles.
Native process/network dependencies must not leak into WASM guests, and test dependencies
must not change production dependency features. The guest workspace's `fixture-spin-support`
package owns scenarios shared by the Spin SDK tests and its CLI/HTTP components; consumers
use ordinary Cargo dependencies rather than including source from an adapter's test tree.

`cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml`
tests the controller itself; it does not launch the cross-runtime suites above. Unit tests
and contracts that need private adapter access stay colocated with that adapter, including
real-TCP tests where private connection state is part of the assertion.

Default run outputs live in `fixtures/reusable-app/.runs/`; `--output EMPTY_DIR` selects
an explicit directory. Runs retain command logs, build identities and separate harness/guest
locks. Missing tools, missing tests, phase failures and failed cleanup never become passes.

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

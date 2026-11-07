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
| Fastly     | Native preflight, SDK contracts, runtime unit tests, feature matrix, ingress, guarded fixture tests, lifecycle smoke, client progress, redirect wire acceptance  |
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

Mixed batch recovery runs six shared scenarios on every adapter: check/next followed by
collection, or fresh collection, each ending in completion, cutoff or an injected typed
progress failure. A separate bounded origin supplies 32 correlated requests; the original
redirect, cap and normalization inventories stay unchanged. The controller reopens persisted
evidence before reporting a runtime pass. Criterion 22 also requires the same-candidate core
identity tests for private duplicate/conflicting receipt cases, once rather than per adapter.
Missing or corrupt companion evidence grants no runtime acceptance credit.

Axum, Fastly and Spin execute this matrix in their existing CI wire phase. Cloudflare's
ready batch and redirect legs share one `--suite outbound-wire` command. The temporary
batch-only selector is removed. Cloudflare uses the selected Wrangler package's public API
with the inspector disabled: its network preview otherwise decodes malformed compressed bodies
before application policy. The small Node bootstrap only starts/disposes that SDK; Rust still
owns supervision and every assertion. This does not activate Cloudflare wire execution in CI
or qualify hosted behavior. Obtain current coverage from the checked inventory rather than
copying readiness totals into this guide:

```sh
scripts/test_integration.sh acceptance --coverage
```

The same `acceptance` command owns manifest/sentinel validation, draft assembly, frozen export
and release reporting. It does not start provider runtimes. Tests live with that Rust code;
there is no parallel JavaScript evidence checker. CI supplies the already-built harness through
`EDGEZERO_TEST_HARNESS_BIN` when selecting inventory-bound tests.

The report accounts for every reviewed leg exactly once. It separates required-local bindings
from external evidence, unconfirmed conditional scope, unpromoted follow-ups and explicitly
approved deferrals. It exits nonzero while required-local bindings remain planned. A zero exit
means binding completeness, not passing execution, consumer approval or qualification.

Callback cleanup combines two distinct local proofs: Fastly's fixture counter tests and the
same-candidate adapter ownership/isolation tests. The fixture tests validate bounded evidence
snapshots; they do not execute either lifecycle wrapper or establish SDK cancellation. The
independent eight-case Viceroy run covers both wrappers and actual pending SDK ownership.
Spin's native response-coordinator models execute in its existing native phase, not as
provider-runtime or client-delivery evidence.

The Spin silent-connect qualification additionally requires Linux `/proc` ownership evidence.
It holds a finite, unaccepted loopback backlog with silent-overflow policy, correlates the
runtime's exact socket with causal start/recovery requests, and rejects immediate refusal,
foreign sockets and stale observations. Other operating systems report that case unverified;
they cannot substitute a connect-refusal result. This is an explicit local qualification
prerequisite, not a new production platform requirement.
The observer resolves one exact listener owner inside the verified launcher's process group,
not necessarily the launcher itself. It checks leader and worker start times/group membership
before and after observation and never reselects a PID. Inspection is bounded to 4,096 numeric
process entries, 512 descriptors per inspected group member, 16 KiB per stat record and 64 KiB
per TCP table. Missing, ambiguous, recycled or out-of-bound ownership fails explicitly; process
supervision and teardown retain the original launcher/group identity.

Client-selection acceptance uses the existing adapter-owned phases: Axum's native handler
and transport-factory tests, Fastly/Cloudflare's runtime library tests, and Spin's executable
SDK contracts. The handler observes router-state over adapter defaults and, on Fastly,
router-state over prepare over default. Distinct supplied-client clocks identify the selected
handle through a fixed preflight error without network I/O. The controller rejects a missing,
failed or wrong-phase Spin proof. No extra runtime suite or CI job is introduced.

Unsupported Image Optimizer precedence shares one `test-utils` conformance helper across the
adapter-owned test phases. Real adapter validation and empty native batches are retained; expired
requests produce attributed 504s without options and named 400s with unsupported options. Counted
pending bodies, transport boundaries and timers prove zero activity, with a finite test poll budget.
Fastly tests Begin/Batch only; its successful native optimizer Send/Follow qualification stays open.

The existing wire phase separately executes ten marker cases: GET 200/204/304/302 and HEAD 200,
each inheriting client-wide Off or explicitly overriding On. One shared fixture helper performs
the sends. An independently owned bounded origin records actual method/path/token/port; persisted
validation requires exact reports, HTTP bytes and origin receipts after cleanup. Existing redirect,
batch, policy and normalization counts remain unchanged. Cloudflare retains its explicit local
wire command rather than silently gaining CI or hosted qualification credit.

Use `--suite outbound-wire`, `--suite ingress-fidelity` or the other focused suites for
iteration. See [suite assertions and runtime setup](SUITES.md) for their exact contracts,
provider-specific tools, retained negative observations and hosted qualification limits.

## Ownership

Static repository checks use this Rust harness without starting provider runtimes or creating
fixture output:

```sh
scripts/test_integration.sh check
scripts/test_integration.sh check dependencies
scripts/test_integration.sh check forwarding
```

The combined command audits both decoder contracts with one metadata snapshot per root,
demo and reusable-fixture workspace, separately checks the no-CLI JSON feature graph,
and compiles the actual forwarding guide example. Fetch locked dependencies first.
The harness tests include real compiler-negative controls and static-command regressions.

Remaining workflow/tooling guards are being consolidated from `scripts`. They currently
use Node with one locked YAML-parser dependency:

```sh
npm ci --prefix scripts --ignore-scripts --no-audit --no-fund
node --test scripts/ci_tooling.test.mjs
```

The workspace CI tooling step and local `scripts/run_tests.sh` install that dependency before
running the guards. Installation failure stops verification; there is no fallback parser.

```text
src/                         native controller, suites and evidence validators
src/checks/                  static dependency audits and forwarding doctest
fixtures/reusable-app/       shared application and provider-specific guest entrypoints
fixtures/request-fidelity/   raw-ingress applications and observation baseline
acceptance/                  outbound case inventory and external runbook source
acceptance/dependencies/     independent audited decoder contract data
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

Owned Unix processes are signalled directly through safe Nix PID/process-group APIs, not helper
subprocesses. Targets must be positive IDs greater than one; only `ESRCH` establishes absence.
Permission and other errors remain failures. Existing TERM/KILL/reaping deadlines are unchanged,
and probe errors are propagated only after owned-child cleanup has run. Earlier helper-timeout
observations remain retained; their exact startup-delay cause was not established.

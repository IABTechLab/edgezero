# Request ingress runtime fixtures

Run from the repository root with the Rust and Node versions in `.tool-versions`:

```sh
./scripts/test-request-fidelity.sh all
./scripts/test-request-fidelity.sh fastly --skip-build
```

`--skip-build` intentionally reuses existing fixture artifacts; it does not
prove source freshness. Use the default locked build for review evidence.

The isolated Cargo workspace and npm lock pin fixture dependencies. Install
Viceroy at the version in `.tool-versions`, Spin CLI and worker-build at the
versions in the fixture manifest's `[package.metadata.request-fidelity]`, and
wasm-bindgen CLI at the `wasm-bindgen` version in the fixture `Cargo.lock`.
The runner and CI read these authoritative pins.
The runner uses `WASM_BINDGEN_BIN` when set, otherwise resolves `wasm-bindgen`
from `PATH`, checks it against the fixture Cargo lock, and passes its absolute
path to worker-build. An invalid override fails instead of falling back to a
download. After `npm ci`, the runner requires the local esbuild binary, checks
its npm-locked version, and passes it as `ESBUILD_BIN`.
Wrangler (pinned in `package.json` / `package-lock.json`) and its local workerd
are installed by `npm ci`. Each runtime
uses its own target directory, an ephemeral loopback port, bounded readiness
and an owned process group that is cleaned up on success, failure or interruption.
No platform credentials or deployment are required.

The runner sends 56 fixed raw HTTP cases across Fastly, Cloudflare and Spin.
The client and process orchestration use an isolated native Rust harness with exact isolated Tokio/serde_json/tempfile/TOML pins. Tokio is confined to this host test helper; it adds no core or WASM runtime dependency. Responses contain fixed booleans/counts rather than unrestricted request values.
`observations.json` records the pinned local runtimes' observed outcomes, and
subsequent runs compare them exactly and report each changed case. Inspect
changes before updating it. Record all platforms to a temporary file first:

```sh
./scripts/test-request-fidelity.sh all --record-observations > /tmp/request-fidelity-observations.json
diff -u tests/fixtures/request-fidelity/observations.json /tmp/request-fidelity-observations.json
# After a successful run and inspection:
cp /tmp/request-fidelity-observations.json tests/fixtures/request-fidelity/observations.json
```

Recording a single platform emits only that platform; replacing the baseline
with that output would discard the other platforms. A failed run may leave an
empty output file.
A passing regression run includes explicitly reported unsupported cases; it
does not establish consumer compliance or production wire preservation.
The two framing probes explicitly finish their known-empty request bodies under a
zero-byte cap. This avoids reusing Wrangler proxy connections after cancelling an
unread body; it does not change adapter admission or claim unread-body cancellation
evidence. Every nonempty framing body fails the fixture's cap. The observed proxy
503 sequence is not accepted as a new baseline or attributed to EdgeZero's renderer.
Axum's real TCP ingress cases live alongside its request converter tests.

Known gates:

- Two Cloudflare routing regressions send raw UTF-8 `/café` and `/a\b`
  directly to the pinned workerd, bypassing Wrangler's development proxy,
  which normalizes these targets before Worker invocation. A no-hook
  application must match `/caf%C3%A9` and `/a/b` while ingress metadata
  independently retains the raw runtime URL with unknown wire preservation.
- Fastly 0.13.1 lacks a safe borrowed original-target getter. Ingress reports
  `NotExposed`; a preceding native path shortcut can see a normalized target.
  Its canonical origin is recovered separately from the client runtime URI
  and one validated consistent Host, before native mutation.
  A snapshot probe changes the native URL/Host after capture and proves the
  original origin remains available through explicit-snapshot dispatch.
- Local workerd rejects extension methods before Worker invocation, normalizes
  dot segments and coalesces repeated fields. Raw FF becomes U+FFFD before Rust
  and is copied as runtime UTF-8 EF BF BD. Common header fidelity remains
  `Unknown`, since a transforming runtime does not prove any particular field
  changed. Valid wire UTF-8 C3 A9 and C4 80 are asserted byte-for-byte.
  Repeated Cookie values are checked for exact comma joining versus semicolon
  joining. Compound probes compare a valid-looking diagnostics value plus
  unrelated invalid FF against original valid EF BF BD: their Worker verdicts
  are identical. They do not implement or prove Trusted Server cookie parsing.
  Absolute-form and duplicate Content-Length requests return local 500 errors.
- Spin 4.1.0 rejects non-UTF-8 header values before component invocation.
  It consumes chunked framing and removes Transfer-Encoding before the hook in
  `chunked` and `te-cl` cases.
- Global original field order is unavailable on Axum, Cloudflare and Spin.
  Fastly reports Unknown: original-name/count APIs provide partial evidence,
  without associating original fields with converted values. Local evidence
  cannot certify production runtime or TLS/client provenance guarantees.

The single [spec](../../../docs/superpowers/specs/2026-10-05-pre-dispatch-request-fidelity-design.md)
and [plan](../../../docs/superpowers/plans/2026-10-05-pre-dispatch-request-fidelity.md)
record the complete contract, verification and remaining upstream/consumer gates.

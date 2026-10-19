# AWS prospect demo integration RC

DO NOT MERGE. Integration-only RC; not for review.

This branch combines seven approved PRs for a later Trusted Server compatibility test. It does not replace their individual review/merge flows. No TS changes, AWS deployment, provisioning, release, tag, registry publication, or real AWS credentials are part of this work.

## Frozen inputs

Origin is `git@github.com:IABTechLab/edgezero.git`. Baseline main is `41f478726727dc1c8abdafcbd6ac9a2b5c3094da`. Live GitHub metadata and fetched `refs/pull/<number>/head` agreed with every planning head. Inputs are preserved locally under `refs/rc-inputs/aws-prospect-demo/pr-<number>`. All seven were merged with ordinary merge commits, in table order. Each full source head must remain an ancestor of the final pin.

| PR | Frozen head | GitHub base tip at freeze | Merge base with baseline main |
| --- | --- | --- | --- |
| [275](https://github.com/IABTechLab/edgezero/pull/275) | `7cccaf10734914161803317aec6c55c22c2bccc9` | `41f478726727dc1c8abdafcbd6ac9a2b5c3094da` | `41f478726727dc1c8abdafcbd6ac9a2b5c3094da` |
| [406](https://github.com/IABTechLab/edgezero/pull/406) | `3ab22096fd117f7506cf437b5ba128e0789e059c` | `dfd439c613af4f6ed295e71c032881b15bf7247c` | `98930917d96cb665c7255f36ca8bbd61fc7539e1` |
| [407](https://github.com/IABTechLab/edgezero/pull/407) | `3308041b910b648af746c15e809ee86b96adc2e9` | `3ab22096fd117f7506cf437b5ba128e0789e059c` | `98930917d96cb665c7255f36ca8bbd61fc7539e1` |
| [409](https://github.com/IABTechLab/edgezero/pull/409) | `66a05e25bee674ced69387467acec3c87b06c8b6` | `3308041b910b648af746c15e809ee86b96adc2e9` | `98930917d96cb665c7255f36ca8bbd61fc7539e1` |
| [414](https://github.com/IABTechLab/edgezero/pull/414) | `350527e4b7387a86da3f4cdaea0e8459665aa0f6` | `ea66eff045512cccc60646d6ec339e83ff44fed7` | `683202c66948146ec360f490126f1d60f920288b` |
| [401](https://github.com/IABTechLab/edgezero/pull/401) | `b8b82965a0464faecc4eb4c606fb5b98afca621a` | `98930917d96cb665c7255f36ca8bbd61fc7539e1` | `98930917d96cb665c7255f36ca8bbd61fc7539e1` |
| [389](https://github.com/IABTechLab/edgezero/pull/389) | `018529795767effa1bf0e5761367f4559aa47b30` | `f7176742d46aa4d1a04ce4588b606afda388fd30` | `f7176742d46aa4d1a04ce4588b606afda388fd30` |

406, 407 and 409 form a strict source stack. Their HTTP foundation predates frozen 275. 414 contains a separate production lifecycle bridge at `a08f54c4`, not the final 406 implementation. 401 and 389 have independent main-based ancestry. No excluded PR was selected separately. Existing baseline main changes remain included.

Source claims are not combined-state proof. At freeze, GitHub reported 29 successful checks for 275, 28 for 389, and 17 successful plus 5 failed checks for 409; other selected heads exposed no check rollup. Source bodies describe extensive local checks, but their older toolchains and source states do not qualify this RC.

## Integration decisions

- Keep 275's bounded HTTP/1 parser, response-head cap, synchronous acceptance-time header deadline, application clock, current outbound batch/body/provenance APIs, and explicit BestEffort/Unsupported classifications.
- Add 406's single production preparation/readiness/drain owner. Enforce the connection cap before parser/task allocation. Reap completed task records before applying the live cap. Catch connection-local unwinds without killing the server. The test embedding uses a one-second drain budget; production options retain their own budget.
- Keep bounded immutable local config snapshots. Add required-file semantics inside the existing loader, including aggregate residency and startup staging. Production source planning requires local roots only for bindings still using local providers.
- Keep logger-before-preflight/configure ordering, boot-target floor and logger-init failure propagation. Preserve application-owned logging. Migrate stale fixture secret variables to main's `EDGEZERO__SECRETS__<KEY>` convention.
- Keep transport-proven empty native bodies empty. The container's real TLS proxy test exposed that wrapping every inbound body as a stream made empty GET/HEAD forwarding fail portable outbound preflight. Unknown and nonempty bodies stay lazy and rejected for GET/HEAD; regression tests prove no speculative poll or discard.
- Reject non-Unicode ingress and snapshot-limit values at both environment-loading boundaries. A new real-process regression first reproduced startup reaching Ready with a discarded binary limit, then proves all ten settings fail before hooks/listening in development and production.
- Apply 407's managed body carrier to current outbound implementations without restoring retired transports. Keep lazy JSON provenance, limits, timeout precedence and source cleanup.
- Compose 409 diagnostics with the existing response-scoped completion. Preserve one captured terminal time and one egress owner. Checked socket/proxy metadata replaces public forwarding assertions; original normalized head accounting precedes stripping.
- Graft 414's source validation and sequential AWS preparation into the reconciled runner. Preserve JSON limit, diagnostics session, trusted-proxy policy and ingress limits. Config and secrets remain restart-adopted startup snapshots. DynamoDB remains live ordinary KV, not CAS, celld or automatic key rotation.
- Preserve main's deployment preflight/finalizer ownership while adding native build-feature normalization. Remove the unused older capture-dispatch helper. AWS dependencies stay target/feature gated.
- Keep 401's hardened recipe and checks, updating the generated compiler pin to Rust 1.98.1. Registry inspection of `rust:1.98.1-bookworm` returned index digest `sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e`, with amd64/arm64 entries. This replaces the older 1.95.0 builder digest. The direct Docker Cargo invocation is provider-free by default; generated guidance explicitly requires application-owned AWS feature flags and an external bindings file. Strict release scan policy is unchanged.
- 389's attachment middleware now uses the application's `MonotonicClock`. Standalone collectors default to a portable clock and can accept one explicitly. T0 remains collector creation. Marks/spans are diagnostic only; neither middleware return, span drop nor egress automatically marks timing completion. A deterministic injected-clock regression tests marks, spans and the separate exactly-once response completion.
- Preserve all provider runtime timing tests inside their actual WASM modules. Migrate timing consumers to current context accessors, without restoring retired request/proxy APIs. Narrow the legacy guard only for the legitimate Axum incoming-proxy module; retired outbound proxy types and other proxy modules remain rejected.
- Fix published guide links to the frozen source plan, because VitePress excludes internal superpowers notes. Preserve the current IABTechLab repository URL in active generated/runtime guidance.

Two read-only workers, both `openai-codex/gpt-6.1-sol`, investigated disjoint runtime and provider/generator contracts, proposed reconciliation choices and checked the combined code. Implementation and Cargo commands remained centrally owned. Documentation and non-Cargo static checks used separate worker-owned outputs.

## Validation

Toolchain follows checked-out `.tool-versions`: Rust 1.98.1. Host is native Linux amd64 with Docker 29.7.2 and all three Rust WASM targets installed. The inherited shell override was 1.95.0 and was not used. Documentation and worker Node/security checks use installed Node 24.20.0. The initial Bash-driven Node checks used inherited Node 24.12.0; final rechecks select 24.20.0 explicitly.

Local detailed logs, source snapshots and the initial ledger are under `/tmp/edgezero-rc-run/`. Final logs and the command/revision/exit/duration ledger are under `target/rc-late-evidence/`. After `/tmp` quota failures, test temporaries moved to `/home/pav/.cache/edgezero-rc-20261019-tmp`, outside any Cargo workspace. An intermediate TMPDIR inside this checkout made a no-parent-manifest test discover this workspace; rerunning outside the checkout passed. Neither environment failure is counted as a pass. Focused integration checkpoints passed core/Axum tests after 275, 406, 407 and 409. After 414, all 74 AWS all-feature unit tests passed and the serialized Axum all-feature suite passed, including five AWS startup and 32 production-hosting tests. The enhanced mixed-provider fixture proves typed initialization before listening, immutable provider reads after fixture listeners close, 64-byte acceptance/65-byte JSON rejection, checked Forwarded host/stripping and native observer delivery with request records disabled.

One parallel-load wire-disconnect test timed out before observing dispatch; it passed alone and in the full serialized Axum suite. This remains a load-sensitive test observation, not a waived failure.

### Executed combined-state checks

The final stable-code ledger has 24 gates, all exit 0, at `e3f13b699b03e99d21af92f12c6496c4d0c59d2b`. The subsequent commit completes only this excluded internal record. Earlier passing evidence is reused where relevant source inputs did not change; the distinction is explicit below.

| Check | Outcome and scope |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo test --workspace --all-targets` | PASS, including the transport-proven-empty and unknown/nonempty GET/HEAD regressions |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | PASS |
| `cargo check --workspace --all-targets --features 'fastly cloudflare spin'` | PASS |
| Axum `--all-features --all-targets -- --test-threads=1` | PASS; five AWS startup tests and 33 production-hosting tests, including all twenty binary-limit startup cases |
| Target Clippy, each adapter with its platform, `cli,test-utils`, `--all-targets -- -D warnings` | PASS on Fastly/wasip1, Spin/wasip2 and Cloudflare/unknown-unknown. Layout expectations apply only to 64-bit targets; actual test modules carry `cfg(test)` so test-only panic/unwrap conventions are recognized. |
| Generated workspace opt-in proof | PASS; strict config validation, formatting, native workspace, nonzero generated core tests, CLI delegation and all three installed WASM targets |
| Generated AWS startup opt-in proof | PASS; application build-feature forwarding, typed production initialization, local/AWS source planning, snapshots, JSON policy and trusted diagnostics composed |
| Generated DynamoDB opt-in proof | PASS; default-off, enabled CRUD/list/logical TTL/caps and restart persistence |
| `node scripts/run_native_aws_integration.mjs` | PASS using independent pinned DynamoDB Local, Moto and the real AppConfig Agent; synthetic local credentials only |
| Flate/Brotli/serde map/shared-pin/native-isolation/legacy-API guards | PASS. Demo and reusable fixture lockfiles were reconciled to the new exact pins; the demo's rustls 0.23.45 was preserved. |
| Prepared-context container Node suites | PASS, 54 tests, zero skips |
| Docs format, lint, VitePress build and outbound docs contract | PASS on Node 24.20.0 |

Earlier executed evidence whose runtime logic remains unchanged:

- Native Axum/Fastly/Cloudflare/Spin feature matrices, core doctests, AWS no-feature/each-provider/all-feature matrix (74 all-feature tests), demo tests (48), and reusable core/harness tests (5 + 26) passed.
- Real WASM harnesses passed: Viceroy Fastly contract 26 + library 161; Wasmtime Spin contract 37 + SDK resources 7; headless Chromium Cloudflare contract 31 + library 29. These include timing attachment and bounded local clock-advancement checks. They are local engine execution, not hosted provider qualification. Later WASM changes affect lint/test cfg attributes only, and strict target Clippy was rerun.
- Action pins, release-example pins, actionlint, offline zizmor, ShellCheck and four action shell suites passed. The earlier 71 Node tests included three missing-prepared-context skips; the final prepared-context suites above executed all 54 container cases without skips.
- Native amd64 generated-container packaging passed after the empty-body fix: ELF/shared-library checks, UID 10001, read-only root, capability drop, no-new-privileges, mounted read-only config/writable data, HTTP and controlled HTTPS trust/hostname rejection. Evidence records source `a3cd9e74a6b49748d451479f5c919c3cce226791`; later native source changes only align Windows prefix matching and test/lint cfg. This packaging check explicitly does not certify readiness, durable restart or SIGTERM; separate native tests cover those behaviors.

The source/combined-state review and these executions do not substitute for one final Git-identity consumer proof. After pushing, generate a fresh consumer, replace **every** EdgeZero workspace dependency with `git = "https://github.com/IABTechLab/edgezero.git"` and the same full final `rev`, verify Cargo metadata contains one EdgeZero Git source and no path copies, and build/run default-off and AWS-enabled native variants. Record the exact pushed SHA, that consumer's startup/snapshot/drain result and the final-pin packaging result in the draft PR and session. This avoids a self-referential commit SHA in this file.

Reproduction starts with a clean checkout of the PR's full pin, `RUSTUP_TOOLCHAIN=1.98.1`, Node 24.20.0, all three WASM targets and the harness tools. Run the four workspace commands above, serialized Axum all-feature tests, `scripts/check_adapter_feature_matrix.sh` for each adapter, and `scripts/run_native_aws_integration.mjs`. Explicit opt-in test names are `generated_workspace_compiles`, `generated_cli_build_features_and_native_aws_startup_are_proven_together`, and `generated_cli_default_off_and_enabled_dynamodb_crud_list_ttl_restart`; use `scripts/run_test_nonzero.sh --ignored <name> cargo test -p edgezero-cli --test <corresponding test file>`. Container preparation/packaging must use a disposable context outside the checkout. Never use real AWS credentials for these fixtures.

## Boundaries

Local execution does not qualify hosted adapter lifecycle or confirm end-client receipt. Native amd64 execution does not prove arm64 execution. Independent Docker services do not establish AWS IAM, KMS, production AppConfig polling or capacity behavior. Dependency DEBUG/TRACE may expose SDK metadata; EdgeZero's own redaction does not suppress third-party logs. DynamoDB has no CAS/exactly-once guarantee. A packaging scan in report-only mode cannot satisfy strict release publication policy; no registry-qualified release scan or publication was performed. Docs installation reported five dependency vulnerabilities (two moderate, three high); no dependency/security qualification is claimed.

After this RC, adapt TS in a separate task so every EdgeZero dependency uses the one final Git identity, migrate intentional API breaks, and run TS-specific compatibility checks before choosing AWS infrastructure or deploying anything.

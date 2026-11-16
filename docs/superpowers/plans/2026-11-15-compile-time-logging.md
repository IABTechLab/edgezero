# Compile-Time Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox
> syntax for tracking.

**Goal:** Address issue #504 inside PR #275 with compile-time manifest logging for
hand-written hooks, without changing outbound or lifecycle behavior.

**Architecture:** One `logging_config!(path, adapter)` expression macro reuses the
compiler-side manifest and logging emission pipeline used by `app!`. It emits typed
literals and a file-dependency marker, never runtime parsing or generated routes.

**Tech Stack:** Rust 1.98.1, existing syn/quote/proc-macro2, TOML manifest model,
trybuild and Rust compilation tests; Node 24.20.0 for existing documentation checks.

**Spec:** [Approved inline design and written acceptance contract](../specs/2026-11-15-compile-time-logging-design.md).

**Status:** Implementation authorized by the user's November 16 continuation.
Unchecked commands/results below are prospective, not executed evidence.
The user's correction explicitly selects the existing PR #275 worktree and branch.

November 16 checkpoint: the public-macro RED/GREEN proof, complete macro crate
tests and strict macro Clippy passed. Primitive-shadowing and competing-`From`
cases reproduced both review findings before their fixes passed. The rebuild
negative control returned stale settings with only its dependency marker disabled;
restoring the marker passed the same regression. Compiler inputs are now checked
across each consumer build, and reader errors have host-independent unit coverage
alongside Unix-specific diagnostic snapshots. Both independent code reviews cleared
the resulting changes.

An approval-service usage limit interrupted the first verification session;
command access is restored. Exact local runtimes and the missing WASIp2 target
are restored without replacing installed executables. The isolated Viceroy repair
passed all 136 library tests and the required named guards; its source, lockfile
and patch match the tracked audit. The complete prescribed suite now exits 0,
including all four adapter integrations and generated/demo consumers. Its first
tooling pass stopped because the local PATH omitted installed GNU Coreutils;
restoring that PATH cleared all 38 tooling tests without source changes.
One initial native-harness run failed the checked-cancellation registration
assertion in 0.03 seconds. The isolated test and two subsequent complete harness
runs passed (672 tests, four opt-in ignored); neither the assertion nor cleanup
behavior changed. Both independent investigators found the retained log
insufficient to establish a cause. The failure remains disclosed, not repaired
or credited as a passing run.

All primary Rust gates, strict rustdoc, twelve adapter Clippy configurations,
excluded-workspace formatting/Clippy and lifecycle/ingress fixture Clippy passed.
The separate Fastly assembly command exits 0 under capture
`18e898da0b851c20-9738-0`, with no failed events and no qualification claim.
Documentation format, ESLint and VitePress build also passed. Publication remains pending; its result is recorded in issue #504 and PR #275's handoff.
This verification checkpoint precedes #504 publication; the actual fixing head,
push readback and independent-review state are recorded in issue #504 and PR #275.
The attestation repair is the separate `c7e3ac97` checkpoint, not part
of the #504 implementation commit. No deployment or capability promotion occurred.

## Scope And Working Tree

- [x] Read issue #504 and current main/PR #275 code; compare two independent
      read-only investigations and solution proposals.
- [x] Obtain inline design approval and the correction to deliver inside PR #275.
- [x] Complete independent spec and implementation-plan reviews; fix all findings.
- [x] Obtain the user's written-spec approval before implementation.

Use `.claude/worktrees/pr-275-a7-rebuild`, branch `prfection/275-a7-rebuild`.
Preserve all existing tracked/untracked attestation changes. Do not create a new
branch/PR, revert pending work, or commit it incidentally as part of #504.

## File Map

| File                                                               | Responsibility                                                                         |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| `crates/edgezero-macros/src/app.rs`                                | Share existing manifest loading and logging lookup/literal generation.                 |
| `crates/edgezero-macros/src/logging_config.rs` (new)               | Parse the two macro arguments and expand only logging lookup plus dependency tracking. |
| `crates/edgezero-macros/src/lib.rs`                                | Register the new proc macro.                                                           |
| `crates/edgezero-core/src/lib.rs`                                  | Re-export it through the normal application dependency.                                |
| `crates/edgezero-core/src/app.rs`                                  | Document the opt-in helper on `Hooks::logging_for`; no signature/behavior change.      |
| `crates/edgezero-macros/tests/logging_config.rs` (new)             | Public-API parity, compiler failures and one manifest-only rebuild regression.         |
| `crates/edgezero-macros/tests/fixtures/logging_config*.toml` (new) | Small shared manifests for parity and route-independence controls.                     |
| `crates/edgezero-macros/tests/ui/logging_config*.rs` (new)         | Existing trybuild pass/fail mechanism, not another runner.                             |
| `crates/edgezero-macros/tests/hooked_app.rs`                       | Replace only logging delegation in the existing manual-hook example.                   |
| `docs/guide/adapters/{axum,cloudflare,fastly,spin}.md`             | Document static metadata separately from adapter logging ownership/overrides.          |
| `docs/guide/routing.md`, `CHANGELOG.md`                            | Align the manual-hook example and feature summary.                                     |

No runtime adapter, SDK, manifest schema, demo/scaffold API or CI job change is planned.

## Task 1: Establish The Missing Public API

- [x] Add the Rust compilation-test driver and a trybuild pass consumer whose
      source uses the proposed macro through `edgezero_core` only. The test runner
      itself must compile before the new macro exists.
- [x] Run `cargo test --offline --locked -p edgezero-macros --test logging_config`.
      Expected red: the named test fails because its positive consumer cannot find
      `logging_config`, not because of a fixture/type/import mistake.
- [x] Record the actual exit/result before adding production code. Do not label a
      zero-test selection, dependency failure or unrelated compiler error as red proof.

### Compiler Fixture Setup

- [x] Keep the driver in `tests/logging_config.rs`. Provision immutable copies of
      `tests/fixtures/logging_config*.toml` and any reused `owns_logging.toml` under
      the same relative paths in trybuild's synthetic consuming package. Obtain
      the effective `target_directory` through structured Cargo metadata, honoring
      the caller's target configuration; the pinned trybuild 1.0.116 package is
      `<target_directory>/tests/trybuild/edgezero-macros`. Its package setup creates
      that directory without deleting these inputs. Do not depend on `../../..`
      traversal from a target directory or hard-code an absolute checkout path.
- [x] Use `tests/ui/logging_config_basic.rs` as the initial pass case. Add pass
      consumers `logging_config_hygiene.rs` and `logging_config_manual_routes.rs`.
      Negative consumers are `logging_config_bad_args.rs`,
      `logging_config_missing.rs`, `logging_config_unreadable.rs`,
      `logging_config_non_utf8.rs`, `logging_config_bad_level.rs`,
      `logging_config_case_duplicate.rs` and `logging_config_bad_endpoint.rs`.
      Check in each negative consumer's sibling `.stderr` after reviewing the
      actual diagnostic; never bless unrelated compiler failures or machine paths.
- [x] Generate invalid UTF-8 bytes and an unreadable-as-text directory input only
      inside that synthetic package. The latter must report a read failure without
      depending on chmod behavior or whether CI runs with elevated privileges.
- [x] Export `CARGO_NET_OFFLINE=true` when invoking the driver so metadata and all
      descendants stay offline. Pinned trybuild also passes `--offline` to its
      nested builds; retain that behavior. Do not mutate process-global environment
      from concurrently running Rust tests.

## Task 2: Implement One Shared Compiler Pipeline

- [x] Extract only the existing path/read/parse/validate/finalize sequence required
      by both entrypoints. Keep existing diagnostics and PR #275 validation intact.
- [x] Refactor logging emission into one private lookup expression builder consumed
      by both `app!`'s generated method and the new expression macro. A tail-expression
      conditional chain avoids any generated `return` escaping the caller.
- [x] Parse `LitStr`, comma, `syn::Expr`, optional trailing comma, then end-of-input.
      Evaluate the adapter once as the `&str` argument of a typed noncapturing closure.
      Its private parameter uses a mixed-site identifier. Do not split a temporary
      borrow into a separate let statement before the lookup.
      The expression wrapper uses this shape after loading and validating the
      manifest (the names below are implementation-local):

```rust
let adapter_binding = Ident::new("__edgezero_logging_adapter", Span::mixed_site());
let lookup = build_logging_lookup(&manifest, &adapter_binding);
quote! {
    {
        const _: &[::core::primitive::u8] = ::core::include_bytes!(#manifest_path_lit);
        (|#adapter_binding: &::core::primitive::str| { #lookup })(#adapter_expr)
    }
}
```

- [x] Keep the shared lookup independent of the enclosing method or expression;
      reuse `logging_config_tokens` for the configuration literal:

```rust
pub(crate) fn build_logging_lookup(manifest: &Manifest, adapter: &Ident) -> TokenStream2 {
    let names = manifest
        .adapters
        .keys()
        .chain(manifest.logging.adapters.keys())
        .map(|name| name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut lookup = quote! {
        <::edgezero_core::manifest::ResolvedLoggingConfig as ::core::default::Default>::default()
    };
    for name in names.into_iter().rev() {
        let name_lit = LitStr::new(&name, Span::call_site());
        let config = logging_config_tokens(&manifest.logging_or_default(&name));
        lookup = quote! {
            if #adapter.eq_ignore_ascii_case(#name_lit) { #config } else { #lookup }
        };
    }
    lookup
}
```

- [x] Use qualified generated paths and constructors. Preserve current precedence,
      defaults and endpoint String ownership. Do not serialize a baked manifest or
      invoke route/middleware generation from this entrypoint.
- [x] Qualify the existing `app!` dependency marker too; a caller's same-named macro
      must not replace either entrypoint's compiler dependency tracking.
- [x] Register/re-export the macro, then rerun the exact red test and existing
      macro tests until green. No compatibility macro or second resolver.

Run:

```sh
CARGO_NET_OFFLINE=true cargo test --offline --locked -p edgezero-macros --test logging_config
CARGO_NET_OFFLINE=true cargo test --offline --locked -p edgezero-macros
```

Expected: named tests execute and pass; existing `app!` behavior is unchanged.

## Task 3: Differential And Compiler Regressions

- [x] Add differential assertions for existing behavior: all fields across four built-in
      adapters, custom/unknown names, case variants, levels and empty/partial overrides.
      Compare against `app!` and the existing test-only manifest resolver.
- [x] Prove one adapter-expression evaluation (including a temporary String borrow),
      continuation after the expression, caller binding/Option/Default/include_bytes
      hygiene, trailing commas, nested use and isolation across repeated invocations
      and multiple manifests.
- [x] Add a logging-only consumer with its own routes and no generated app type.
      Schema-valid unresolved/non-Rust handler or middleware references must not
      require route-symbol processing merely to obtain logging metadata.
- [x] Extend the existing trybuild pattern for argument errors and input validation;
      test missing/non-UTF-8/malformed manifests, bad levels/types, case-folded
      duplicates and effective Fastly endpoint failures. Preserve field attribution.
- [x] Add colocated expansion assertions excluding runtime parser/filesystem calls,
      manifest JSON construction and router generation.
- [x] Run the macro tests after each code change. Retain failed attempts honestly.

## Task 4: Manifest-Only Rebuild And Runtime Independence

- [x] Add one serial Rust integration test using the existing temporary-consumer
      compilation pattern. Use an isolated Cargo target directory to avoid nested
      Cargo build-lock deadlocks; reuse it for every build of this consumer.
- [x] Its generated Cargo manifest is a standalone `[workspace]` with one binary
      and a path dependency on local `edgezero-core` only, not `edgezero-macros` or
      an `app!` wrapper. Use `env!("CARGO")` for child Cargo commands. Resolve the
      native host using the existing rustc-version pattern, and pass that target
      explicitly. Per-child environment/arguments override inherited target settings;
      never change the test process's global environment.
- [x] Construct Cargo/TOML inputs with existing structured APIs. Generate its lock
      once with `cargo generate-lockfile --offline --manifest-path <consumer/Cargo.toml>`.
      Build with `cargo build --offline --locked --manifest-path <consumer/Cargo.toml> --target-dir <unique-temp-target> --target <native-host> --message-format=json`.
      Parse Cargo JSON artifact messages with serde_json to find the executable;
      do not guess target paths. The child source prints typed configuration fields
      as text; it needs no extra serialization dependency.
- [x] Build, run and assert initial logging values. Remove the manifest temporarily
      and run that executable directly, not `cargo run`; it must still succeed.
- [x] Restore and modify only the manifest; assert Rust source bytes are unchanged.
      Rebuild without cleaning and observe the changed values.
- [x] Include a caller-defined `include_bytes!` returning empty bytes in the consumer;
      the qualified generated marker must still track the real manifest input.
- [x] Change only the manifest to invalid configuration; require compile failure.
      Require the expected macro validation diagnostic and field/category, not just
      any nonzero exit. Assert the consumer Rust files, Cargo manifest/lockfile,
      compiler implementation and build options stay unchanged throughout the
      valid-to-valid-to-invalid sequence.
- [x] Establish development-only RED evidence for that same named regression by
      temporarily disabling only the new dependency marker with a scoped patch.
      The logging-only consumer must have no `app!` or other inclusion mechanism
      independently tracking that input. Expect its manifest-only rebuild assertion
      to expose the stale configuration. Restore the qualified marker immediately
      and rerun the identical regression to GREEN. Keep each complete sequence on
      a fixed compiler implementation; never make a CI test edit repository source.
- [x] Add no permanent fixture crate, shell/JS script, separate test framework or CI
      action for this regression. Keep its source and assertions in the owning crate.

## Task 5: Documentation And Consumer Alignment

- [x] Replace only `HookedApp::logging_for`'s private generated-app delegation with
      the public macro. Preserve its other metadata, state, continuation and terminal
      behavior. Keep unrelated introspection setup separate from no-parse proof.
- [x] Add the same concise supported pattern to all four adapter guides. Explain
      that lookup does not initialize a backend or consume runtime overrides.
- [x] Update `Hooks` docs, the routing example and changelog. Demo/templates already
      use `app!`; retain them and verify they still compile with the shared emitter.
- [ ] Record #504 as newly approved scope in PR #275 without rewriting the historical
      six-thread repair's explicit exclusion or claiming the attestation task complete.

## Task 6: Verification And Delivery

- [x] Bootstrap locked dependencies for the root, standalone harness, excluded
      demo and both fixture workspaces. Verify runtime/tool prerequisites from
      `.tool-versions`, `crates/edgezero-test-harness/README.md` and CI setup. Build
      or select the repaired Viceroy using `patches/viceroy/README.md`, recording
      source/patch/executable identity; a stock executable with the same version
      string is not a substitute. Do not replace installed tools or runtime pins.

```sh
export RUSTUP_TOOLCHAIN=1.98.1 ASDF_RUST_VERSION=1.98.1 ASDF_NODEJS_VERSION=24.20.0
export CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
cargo fetch --locked
cargo fetch --locked --manifest-path crates/edgezero-test-harness/Cargo.toml
cargo fetch --locked --manifest-path examples/app-demo/Cargo.toml
cargo fetch --locked --manifest-path crates/edgezero-test-harness/fixtures/reusable-app/Cargo.toml
cargo fetch --locked --manifest-path crates/edgezero-test-harness/fixtures/request-fidelity/Cargo.toml
npm ci --prefix scripts --ignore-scripts --no-audit --no-fund
npm ci --prefix docs --ignore-scripts --no-audit --no-fund
: "${EDGEZERO_REPAIRED_VICEROY:?select the executable built from patches/viceroy/README.md}"
test -x "$EDGEZERO_REPAIRED_VICEROY"
export VICEROY_BIN="$EDGEZERO_REPAIRED_VICEROY"
export CARGO_TARGET_WASM32_WASIP1_RUNNER="$VICEROY_BIN run"
export CARGO_NET_OFFLINE=true
```

- [x] Run the prescribed suite and primary gates using the repository's installed,
      pinned tools and explicit repaired Viceroy selection. Cleanup removed build
      caches; missing caches are not missing source or permission to skip a gate.

```sh
bash scripts/run_tests.sh
cargo fmt --all -- --check
cargo clippy --offline --locked --workspace --all-targets --all-features -- -D warnings
cargo check --offline --locked --workspace --all-targets --features "fastly cloudflare spin"
cargo check --offline --locked -p edgezero-adapter-spin --target wasm32-wasip2 --features spin
RUSTDOCFLAGS="-D warnings" cargo doc --offline --locked --workspace --all-features --no-deps
npm --prefix docs run format
npm --prefix docs run lint
npm --prefix docs run build
```

- [x] Run the existing `.github/workflows/format.yml` adapter Clippy matrix with
      `--no-default-features --all-targets -- -D warnings`: Cloudflare wasm32-unknown-unknown
      with `cloudflare` and `cloudflare,test-utils`; Fastly wasm32-wasip1 with
      `fastly`, `fastly,cli`, `fastly,test-utils` and `fastly,cli,test-utils`; Spin
      wasm32-wasip2 with `spin` and `spin,test-utils`; Axum native with `axum` and
      `axum,test-utils`. Retain that workflow's lifecycle/ingress fixture lint steps,
      including the inherited `assembly-attestation` feature for the Fastly fixture.
      Also run standalone harness strict Clippy/formatting and the existing docs
      contracts from `scripts/run_tests.sh`. Add no matrix row, job or runner.
- [x] Run every existing command gate in that workflow's `format` job, not just
      the adapter matrix: fixture and harness formatting/native Clippy, the
      CLI-only library check, native Fastly CLI-only/no-feature Clippy, and
      excluded app-demo workspace formatting/strict Clippy. Neither the root
      all-features build nor `scripts/run_tests.sh` replaces those feature and
      excluded-workspace checks.
- [x] Verify the inherited attestation repair separately with
      `scripts/test_integration.sh --adapter fastly --suite assembly`, preserving
      all first-response identity, canonical body, recovery, HEAD and cleanup gates.
- [x] Obtain independent spec/implementation review; address findings centrally.
- [ ] Stage only #504 files/hunks and inspect `git diff --cached` before committing.
      `CHANGELOG.md` is already dirty with an attestation entry: stage its #504 hunk
      only, preserving the unstaged entry and repair ledger. Do not use blanket
      staging, reset/revert other work, or accidentally include attestation files.
      Commit after verification, and push only to the existing PR #275 branch when
      the approved validation passes. If shared hunks cannot be separated safely,
      leave them unstaged and report that condition rather than altering either change.
- [ ] Update #504/PR #275 with real commands, exit codes and results; leave issue
      closure, thread acceptance and hosted/manual checkboxes to the existing gates.

The user approved the expression macro over an impl-item macro: it keeps manual
hooks explicit and is usable outside the trait. Both investigations selected reuse
of the existing compiler pipeline, not a runtime cache or metadata-only app wrapper.

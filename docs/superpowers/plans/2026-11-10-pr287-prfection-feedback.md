# PR #287 REVIEWER Feedback Repair Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Repository rules require central implementation with independent investigation and verification; preserve the user's adapter-owned CLI requirement.

**Goal:** Address the 34 current REVIEWER findings on PR #287 with substantive corrections, exact current-head evidence, and CREATOR dispositions in their original threads.

**Architecture:** Retain adapter-owned provider policy and provider-neutral CLI orchestration. Repair shared record parsing, atomic file output and deterministic environment lookup centrally; keep provider manifest, variable, runtime, and scaffold defaults in their adapter. Existing architecture authorization is recorded accurately rather than claimed as an independent planning review.

**Tech Stack:** Rust 1.98.1, TOML/toml_edit, std filesystem replacement, tempfile for CLI staging, advisory filesystem locks, existing adapter runtimes and CI-pinned verification tools.

## Resumed state and boundaries

- Existing implementation PR: https://github.com/IABTechLab/edgezero/pull/287; issue #288 and linked section issues #289–#297.
- Starting head: d62a151259957b9bc4f62935b785a4267a68bb1a; merge base/default: cf2c57b1cf0e139d3b86f128218c798027d7978b.
- User explicitly adopted .claude/worktrees/pr-287-nov02, branch fix/pr-287-nov02; CREATOR ownership/helpers are saved in its private Git directory.
- Remembered one-account mode: CREATOR and REVIEWER use aram356. CLI and connector identity match. This grants no merge exception.
- Original June planning artifacts are on main; October amendments are on this implementation branch. Prior direct user authorization covers adapter ownership; independent acceptance of the disclosed amendment remains for REVIEWER.
- Do not resolve/minimize/hide threads, reinterpret a COMMENT as formal approval, mutate live cloud resources, or touch the user's other worktrees/secrets.
- Implement centrally; investigation/proposal agents remain advisory. Source stays unchanged during the baseline.

## Task 1: Confidential, fallible provision previews and bounded staging

**Files:** edgezero-adapter/src/env_records.rs; edgezero-cli/src/provision.rs, path_safety.rs, lib.rs, copy_tree.rs, provision_lock.rs.
**Findings:** 4533587688, 4533589422, 4533589572, 4533589736, 4533589877, 4533591110, 4533592494.

- [x] Add regressions for commented credential URLs, base64 text that can also parse as a valid assignment key, and malformed bare URLs. No supplied sentinel may appear; both sides of actual preview must be protected.
- [x] Make redaction-only malformed prefixes non-disclosing; redact untrusted comment content rather than trusting apparent keys. Preserve the framework header and blank/physical-line structure; do not alter operator file bytes.
- [x] Propagate untyped, typed and deployed-writeback preview stdout errors through the existing Result APIs. Verify via isolated closed-pipe child commands and outcome acknowledgement, without global descriptor/environment mutation.
- [x] Create staging directories owner-only (0700 on Unix) before copying any bytes; verify under permissive umask in a subprocess.
- [x] Stage declared local-layout artifacts plus application/Cargo metadata, rather than recursive source/cache/state trees. Existing stage_local_artifacts already supplies provider-owned artifact paths. Preserve Cargo ancestry and remove/migrate obsolete copier-only tests if the copier is eliminated.
- [x] Verify root/nested dry-runs ignore unrelated caches, unreadable source and changing runtime state; tolerate NotFound for vanishing staging inputs appropriately.
- [x] Add an internal staging variant accepting a temp parent; remove TMPDIR mutation and test refusal directly.
- [x] Validate only declared serve_env inputs for serve, retaining declared-manifest guards and full artifact guards for provisioning/push/diff.
- [x] Stat the input manifest before creating a lock directory, then load transactional state while holding the lock. Report lock contention before blocking; retain one typed-flow lock.

## Task 2: Preserve operator state and make replacement atomic

**Files:** edgezero-adapter/src/env_file.rs and shared CLI I/O support; adapter Cargo/lib module wiring; edgezero-cli/src/provision.rs; Axum/Cloudflare/Spin local provision writers.
**Findings:** 4533589067, 4533589256.

- [x] Add byte-preservation regressions for active custom __NAME values, commented lines, duplicate occurrences, CRLF, quoted continuations and absent keys.
- [x] Use insert-only reconciliation for provision hints; retain the lossless logical-record scanner and explicit managed-upsert helper behavior for callers that deliberately own values.
- [x] Add a provider-neutral sibling-temp writer: private creation, write_all, sync_all, atomic persist/rename, cleanup on failure. Env output uses 0600; tracked manifests retain permissions and existing symlink-target behavior.
- [x] Integrate atomic replacement for shared env files and tracked deployed-state writeback; skip unchanged manifest writes.
- [x] Test deterministic pre-rename failure, unchanged original bytes, mode, no leaked temporary files, no-op inode behavior where portable, and dry-run non-mutation. Leave Fastly's specialized locking/hard-link safeguards intact.

## Task 3: Keep missing secrets missing and repair Spin behavior

**Files:** Axum/Cloudflare cli/mod.rs; Spin cli/mod.rs, provision_local.rs, push_local.rs, run.rs; shared scaffold.rs; generator.rs; specification and Spin guide.
**Findings:** 4533588111, 4533588282, 4533588430, 4533588572, 4533588721, 4533588871, 4533593140.

- [x] Emit inactive secret hints so fresh typed provisioning does not manufacture an empty credential. Audit sibling placeholder paths and update their documented fresh-secret policy without redefining arbitrary explicit empty secret-store values.
- [x] Validate Spin manifest LowerSnakeId grammar by underscore-separated words; update matching diagnostics/docs and validate direct local provision before any writes. Test foo__bar, token_2, trailing underscore, valid digit suffixes and no-mutation rejection.
- [x] Restore main's SDK7/Spin4.1 provenance and two compatibility guard tests without claiming the SDK pin constrains an independently installed runtime.
- [x] Exclude the Axum EDGEZERO__SECRETS__ namespace from guest argv for both overlay and inherited inputs; preserve other documented overrides, parent precedence, SPIN_VARIABLE child env and non-Unicode handling.
- [x] Thread local mode through private SQLite writes and gate both containment checks on it. Test non-local absolute/escaping-relative writes plus local rejection without external mutation.
- [x] Add generic static adapter_defaults to ManifestSpec; generic renderer emits it inside the selected adapter table. Only Spin supplies HTTPS proxy permission; no provider-name branch in CLI. Other blueprints use empty defaults.
- [x] Verify new tracked scaffold permission, generated Spin permission and clean-clone regeneration parity; keep undeclared existing-app synthesis deny-all and existing manifests preserved.
- [x] Update Spin child-overlay/manual workflow and tracked outbound-host guidance. Document the scaffold metadata source break.

## Task 4: Repair Fastly representations and restore coverage

**Files:** Fastly cli/provision_local.rs, provision_cloud.rs, staging.rs, run.rs; .cargo/config.toml.
**Findings:** 4533590171, 4533590377, 4533590751, 4533590959, 4533591489.

- [x] Make self-spawned exact names derive from module_path and require an actual one-test success.
- [x] Isolate inherited FASTLY_SERVICE_ID in cloud tests; use the established guard and fake provider for non-dry paths. Preserve portable dry-run coverage.
- [x] Restore manifest-selection/flag-stripping and incompatible local config-format tests from the base, adapting to current private-manifest behavior.
- [x] Append/deduplicate typed placeholders in inline arrays, preserve existing AOT data/env mappings, and skip file-backed tables with truthful status. Reject malformed unsupported representations without mutation.
- [x] Align the crate default test runner with the manifest-free CI runner. Verify from the adapter crate with runner overrides absent; record baseline failure and final successful runtime result.

## Task 5: Hermetic tests and runtime compatibility

**Files:** shared cli_support.rs; all three WASM adapter artifact test modules; Axum config_store.rs, test_utils.rs, cli/run.rs, dev_server.rs.
**Findings:** 4533590013, 4533590543, 4533591276.

- [x] Introduce a private injected ambient-environment source for target resolution; production still uses process values, tests use deterministic inputs. Keep context/argument precedence and all previous assertions.
- [x] Isolate adapter artifact test execution from inherited Cargo target/home settings without parent process env mutation; verify selected child test count and fresh/missing/stale branches.
- [x] Route Axum mutations through one existing async guard, including the PATH test.
- [x] Remove runtime-only symlink prohibitions; prove direct config and KV persistent-volume links work while CLI containment remains enforced.

## Task 6: Narrow lint and repository policy changes

**Files:** Cargo.toml; four adapter CLI module roots and actual diagnostic sites; .claude/settings.json; core manifest.rs; Cloudflare provision_cloud.rs and blueprint; root/template gitignore; test.yml.
**Findings:** 4533591965, 4533592137, 4533592646, 4533592796, 4533592942.

- [x] Restore deny-level workspace lint policy. Use compiler-confirmed narrow expectations with reasons for CLI module organization and coherent ordering; verify all relevant feature combinations.
- [x] Remove the two unrelated shared WebFetch permissions.
- [x] Mark the new deployed manifest struct non_exhaustive.
- [x] Remove unnecessary Unix-only guards from deterministic Cloudflare reconciliation tests.
- [x] Ignore and gate environment-specific secret variants through generic dotenv and Cloudflare-owned blueprint metadata. Preserve deliberate .env.example/template exceptions and exact ingress-fixture exemptions; test positive and negative cases.

## Task 7: Public scope, migrations, comments and rustdoc

**Files:** PR body/comment; CHANGELOG.md; migration, Axum/Cloudflare/Spin guides; June/October planning links; cited registry/core/CLI/adapter comments.
**Findings:** 4533587927, 4533591736, 4533592308, 4533593361, 4533593548.

- [ ] Record the exact prior direct user instruction and scope on the PR with CREATOR label; do not manufacture independent planning approval.
- [ ] Add implementation phase marker; link immutable current-head versions of June/October/repair artifacts; disclose adapter module split, execution/lifecycle/local-layout/cloud-target/output APIs and migration behavior.
- [ ] Request explicit planning-scope acceptance during re-review; no newly proposed architecture in this repair.
- [ ] Document teammate pull deletion before untracking manifests; back up custom content and use supported tracked declarations/shared templates.
- [ ] Complete Added/Breaking entries, including validation signature, scaffold metadata, generated manifests/local declarations, Spin permissions, project-root Axum data and verified manual Cloudflare ID adoption.
- [ ] Retain the deliberate refusal to promote unverified local namespace IDs; explain the historical old-thread behavior change honestly.
- [ ] Rewrite all cited internal plan/reviewer labels as actual code constraints. Move every misplaced doc block and remove the impossible symlink remediation without changing policy.

## Task 8: Current-head verification and per-thread handoff

- [ ] Scoped failing regression first, minimal correction second, scoped cargo test after each logical code change.
- [ ] Commit tested logical batches; after the final source/doc commit, execute the full prescribed suite at that exact clean head.
- [ ] Derive commands from CLAUDE/CI/plans: primary Rust gates; policy/audit/minimal profiles; root/demo/fixture suites; adapter/fixture WASM check/Clippy/runtime contracts; browser pins; real local ingress and HTTP smokes; docs; action/cache/deploy contracts.
- [ ] Keep actual setup failures and baseline author-owned failure separate. Record the five macOS-only harness skips and cover their applicable Linux cases through current-head CI; never call a skipped run passing.
- [ ] No authenticated cloud mutation. Exercise cloud control flow with explicitly isolated fake providers, and disclose that limitation.
- [ ] Independent final code/build review; stop all task servers/containers.
- [ ] Push normally to the existing PR branch, read back OPEN/non-draft/current head and actual CI states.
- [ ] Post CREATOR final dispositions in each of the 34 original finding threads, with correcting commit/location and exact successful commands/exit/result lines. Add current labeled evidence to the prior R1/R2 threads and clarify the declined unverified-ID behavior in its original thread as needed.
- [ ] Do not edit old comments merely to add labels; do not resolve threads. Request re-review with current planning links, evidence, account mode, relative worktree/branch, cleanup lists and next actor.
- [ ] Keep resources needed by the open PR. Stop when waiting on REVIEWER, approval or external required checks; no merge without its separate user/head-specific gate.


## Implementation and review notes

Tasks 1–6 are implemented and their focused regressions pass. The shared atomic
writer uses std-only `create_new` sibling files, preserving default WASM builds
without adding a filesystem dependency to the adapter registry crate.
The writer removes incomplete temporary files, syncs completed contents before
rename, and preserves the existing tracked-manifest target and permissions.

Internal plan review approved the repair boundaries. The independent code audit
found one additional atomic-writeback prerequisite: writable file permissions do
not imply the target directory permits sibling creation. A regression reproduced
provider dispatch before checkpointing was possible. Preflight now probes the
canonical target parent before any provider call; direct and symlinked cases pass.
The audit verified the correction and found no further substantive defect.

The initial default Fastly WASM runner failed because its demo manifest was
removed. Its repaired manifest-free runner passed all 97 library tests without
an override. Workspace tests, strict all-feature Clippy, format and feature
checks pass before commit. Exact current-head full-suite evidence, public
scope/authorization records and per-thread CREATOR dispositions remain the
Task 8 handoff; these internal checks are not independent PRfection approval.

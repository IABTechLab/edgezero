# PR 275 Review Repairs

> **For agentic workers:** Use `superpowers:executing-plans` to implement this
> approved repair plan centrally, with read-only review and verification handoffs.

**Goal:** Address both remaining reviewers' 32 threads without weakening the
approved outbound contracts or treating local evidence as hosted qualification.

**Architecture:** Repair the existing shared HTTP pipeline and provider boundaries.
Use the Rust harness as the single response-egress evidence validator. Keep the
hosted collector until its interruption semantics have a separately verified port.
No compatibility transport, aliases, executor or duplicate validator is introduced.

**Tech Stack:** Rust 1.98.1, existing provider SDK/runtime pins, existing Rust
integration harness and documentation/tooling checks.

## Approval And Preservation

The requester approved both reviewers' scope and this repair plan in the
conversation on 2026-11-07. This includes one explicit standards correction to
previously frozen expectations: accept up to 32 empty `Connection` list members
across all such fields in one normalization pass. Reject the 33rd empty member and
invalid nonempty tokens. No other frozen acceptance expectation is relaxed.

Preserve original absolute deadlines, observation cutoffs, per-slot outcomes,
partial results, strict body suppression, independent limits, paired clocks,
completion ownership, direct native Image Optimizer behavior, audited decoder
normal-dependency pins and all unverified hosted/parity/resource gates.

The four investigation agents examined CLI/macros, shared HTTP policy,
provider integration, and evidence/tooling. Their solution proposals favor
minimal repairs over suggested changes that conflict with approved contracts.

## Execution

- [x] Add regression tests for confirmed defects and observe their failures.
- [x] Repair HTTP normalization, including TE+CL protocol rejection before caps
      and bounded empty-member handling. Preserve nomination/bodyless precedence.
- [x] Repair exact selected-manifest execution, CLI feature gates and generated
      macro name hygiene. Test provider argv/cwd without platform credentials.
- [x] Preserve actual Spin connect-setting acceptance independently of requested
      configuration. Probe buffered framing and forbidden fields on pinned SDKs
      before changing their production paths.
- [x] Hard-port response-egress validation to the existing Rust harness. Require
      the canonical 26 probes and 104 lifecycle events, including recovery probes.
      Delete the old JavaScript validation path, not the hosted HTTPS collector.
- [x] Repair Cargo-config symlink identity, full-width socket identifiers and
      signing isolation in synthetic repositories.
- [x] Update active docs, commands, current interface digest assertions, default
      extractor caps and explicit hard-cut migration guidance. Preserve history.
- [x] Characterize Axum blocking storage and document existing proxy behavior.
      A new overload policy, blocking-work limit or shutdown contract needs a
      separate decision; investigation does not authorize one.
- [x] Run focused tests and the full prescribed Rust, WASM, fixture, demo, tooling
      and documentation suite. Record commands, exit codes and actual output.
- [ ] Commit and push verified repairs to the existing PR. Reply separately in
      every original thread; leave resolution to the reviewers.

## Thread Ledger

The ledger describes the approved scope and intended dispositions. Checked tasks
above have focused local evidence; full-suite and pinned-runtime verification are
still required before a pushed-head handoff. Fixing commits and final dispositions
will be recorded in each original review thread.

| Thread | Validated scope and planned response |
| --- | --- |
| 4483656372 | Keep audited decoder pins; do not substitute the root lockfile for a downstream dependency contract. Assess other shared-library pins separately. |
| 4483656377 | Retain allocator/checksum anchors required by source audits; document their purpose and linked update policy. |
| 4483656381 | Approved bounded empty-Connection standards correction at shared normalization boundaries. |
| 4483656386 | Already fixed and explicitly accepted by the reviewer; verify current coverage, no second cancellation implementation. |
| 4483656396 | Clarify explicit driver Cutoff obligation and test late observation plus EOF; preserve fail-closed EOF and partial results. |
| 4483656406 | Clarify cooperative helper overrun explicitly; test observed expiry precedence without a second timer. |
| 4483656417 | Document blocking service-wide backend quota as a hard operator constraint; a sandbox-local cap cannot prove global availability. |
| 4483656422 | Link immutable SDK source to its eager-progress audit/upstream exit condition; preserve coordinated pins. |
| 4492016684 | Preserve exact Spin selected file; map supported Fastly native filename selectors or refuse before launch. |
| 4492016690 | Align demo helper/import cfg with the CLI-gated module; verify both no-default-feature combinations. |
| 4492016695 | Follow explicitly selected Cargo-config symlinks and hash target contents; retain source-tree symlink rejection. |
| 4492016701 | Parse socket inode as nonzero u64, not a bounded duration; preserve stable-identity checks. |
| 4492016710 | Replace remaining removed RequestContext methods and compile the corrected examples. |
| 4492016717 | Disable inherited commit/tag signing in synthetic test repositories only. |
| 4492016728 | Correct the current-file digest assertion to the actual approved current interface file, not another historical digest. |
| 4492016740 | Replace deleted check commands and old fixture location in active verification instructions. |
| 4492016748 | Distinguish absent root-key field path from safe secret-schema field path; preserve redaction. |
| 4492016759 | Classify explicit connect timeouts only when the setter actually accepted them; retain aggregate expiry priority. |
| 4494112256 | Do not silently replace approved workspace discovery with ancestor-only lookup; document/test current selection and surface any policy change separately. |
| 4494112523 | Reject ordinary upstream payloads with surviving ambiguous TE+CL before content-length/cap decisions; preserve bodyless precedence. |
| 4494112817 | Require the complete canonical response-egress schedule in the sole Rust validator, with coherent-subset negative tests. |
| 4494113120 | First prove pinned Spin wire framing; derive any buffered length from actual bytes, never stale caller framing or prebuffered streams. |
| 4494113388 | Preserve typed host-read failures under EZ-1.4; characterize malformed uploads and do not blanket-map arbitrary failures to 400. |
| 4494113685 | Fully qualify generated Result/Ok paths and test caller aliases/shadowing. |
| 4494113946 | Investigate adapter-owned blocking KV operations; no unapproved offload-overload/shutdown policy. |
| 4494114314 | Existing proxy-environment behavior is not a new regression; document it rather than silently disabling deployed proxy setups. |
| 4494114613 | Preserve the single shared opaque-SDK readiness pulse and cache semantics; do not add unqualified backoff or bypass the SDK transport. |
| 4494114967 | Document JSON 8 MiB and form 1 MiB defaults, 413 behavior and explicit-limit alternatives. |
| 4494115310 | Document the intentional breaking target-aware adapter migration; do not restore unsafe execute fallback. |
| 4494115611 | Verify pinned forbidden-header behavior; normalize connection-specific HTTP2-Settings centrally. Response Host policy needs an explicit decision. |
| 4494115844 | Preserve RFC-permitted strict outbound comma-CL rejection and intentional egress normalization; explain the distinction. |
| 4494116079 | Verify existing B31 versioned identity/golden coverage and strengthen only missing policy-version documentation. |

## Verification And Handoff

For every runtime fix: add the failing regression, run it, make the smallest
correction, then rerun its owning crate. Exercise provider-only code on its WASM
target, not merely native stubs. Preserve existing test identities and negative
controls. Use `serde_json::json!` for ordinary JSON fixtures.

Final verification includes all five CLAUDE.md gates, `bash scripts/run_tests.sh`,
the workflow's WASM Clippy/feature matrix, harness tests and strict Clippy,
generated-workspace/demo checks, and docs formatting/lint/build. Targeted tests
supplement rather than replace that suite. Never promote missing hosted evidence.

Replies must identify fixed/won't-fix/disagree explicitly, link the pushed fixing
commit or governing contract, and include actual successful verification output
where a fix is claimed. Do not resolve, minimize or hide any review thread.

### Executed Checkpoint

The repair worktree is based on `6f0cc74ce9d9cb4acb284f97626ac28c6828aad9`.
On Rust 1.98.1, the complete `bash scripts/run_tests.sh` run exited 0 with
`All tests completed successfully.` The workspace phase passed 2698 tests across
33 binaries, with zero failures and one existing ignored test. Harness tests
passed 636, with zero failures and two existing ignored tests.

All four local adapter CI suites passed. Spin SDK resource tests passed 23;
the actual Spin wire capture records `content_length: ["3"]` and no transfer
encoding for both Send and Begin buffered POST controls. Its redirect inventory
remains 68 exchanges / 84 origin requests and `qualification: false`.

Independent read-only verification found a stale runbook command and missing
native selector aliases; both were repaired before the final runtime run. The
reviewers then reported no remaining issues in their assigned source scopes.
The only controller-ownership hash refresh is `src/main.rs:run`, for the new
static Rust evidence-validation dispatch. No acceptance identity was refreshed.

The separately prescribed 30-command formatting, strict-lint and docs-site matrix
also passes. The Spin WASM test-utils lint found test-only imports/numeric fallback
and the docs formatter found the changed CLI table; both were corrected and their
commands rerun successfully. Spin's 23 SDK resource tests passed again afterward.
All five primary Rust gates pass. The documented approval-identity verification
fence prints `Historical and scoped-amendment identities match; authorization
remains separately scoped.`

No hosted qualification or review acceptance is claimed. Final per-thread replies
must distinguish substantive fixes, intentionally retained behavior, and findings
already satisfied by the existing implementation. The reviewer alone resolves
threads. Pushing the checkpoint and recording those replies remain outstanding.

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

### Clean-Commit Logging Isolation Repair

The clean-commit full-suite rerun at `e65394a1` caught a Fastly test race: the
SDK's native capture helper set the process-wide facade maximum to `Off`, so
the concurrent registration-warning test could miss its unchanged 50 ms warning.
A deterministic facade-level assertion reproduced `Off` instead of `Trace`.

Reuse the existing registration test logger as the sole facade owner before
installing the SDK capture, and give that capture a real endpoint with a Trace
ceiling. Each production backend's Off-through-Trace filter matrix and all
threshold, redaction and release assertions remain unchanged. This is test-only
state isolation, not a production logging or timing-policy change. Retain the
failed run separately and rerun the complete suite on the final clean commit.

## Focused Review Closeout (2026-11-10)

AG requested items 1 (CodeQL cleanup) and 2 (review closeout), including resolution
of issues already addressed. This checkpoint starts at `a7d7fd0f` and preserves
the architecture and outbound requirements above. Issue #458 stays separate.

- [x] Replace the four hard-coded expected nonce arguments in startup-publication
      fixtures with the nonce generated for each owned child launch. Retain wrong
      nonce, PID, schema, byte-boundary, deadline and liveness controls; do not
      suppress CodeQL or change production startup validation.
- [x] Correct current Fastly guidance and the changelog: buffered SDK Send and
      batch heads use the shared positive 1 ms pulse under both PlatformDefault
      and Bypass. Preserve scheduler timing, cache selection and native optimizer.
- [x] Verify Spin's empty buffered request framing at the SDK and existing wire
      boundary before correcting unnecessary synthesized zero lengths. Preserve
      empty POST/PUT/PATCH framing, actual nonempty lengths, GET/HEAD behavior,
      limits, original deadlines and streamed bodies without prebuffering.
- [ ] Run focused regressions, all five root gates, the full prescribed adapter,
      demo/generated-project suite, strict harness/provider lint and docs checks.
      Verify the pushed-head CodeQL result separately from local test results.
- [ ] Push this checkpoint to PR #275 and reply in each affected original thread
      with the exact fix and executed evidence. Under AG's explicit request,
      resolve external threads only when the requested correction is verified
      or the reviewer has explicitly accepted the retained policy. Record that
      this maintainer-directed resolution is not independent approval. Own
      amendment threads and unresolved policy/hosted questions stay open.

No pin relaxation, alternate transport, backoff policy, generic body-error
reclassification, manifest-discovery policy change, hosted deployment, evidence
promotion, review dismissal or merge is authorized by this checkpoint. Further
source changes require fresh current-head validation and independent re-review.

## Scoped Repairs (2026-11-11)

AG approved cleanup followed by items 1-3 of the current closeout list. Reuse the
existing Rust harness, process supervisor, fixture logger and body preparation;
do not introduce another transport or compatibility API. Hosted qualification,
candidate freeze and review acceptance remain separate.

- [x] Remove inactive generated output while preserving source changes, runtime
      evidence and active worktrees/builds.
- [x] Fix CLI-only dependency-feature activation and retain an isolated check in
      the existing CI job and prescribed suite.
- [x] Emit qualification lifecycle JSON through both logger filters without
      enabling ordinary Info logging or wrapping the structured event.
- [x] Supervise progress fixture builds and Cargo metadata capture through the
      existing process owner; prove direct-controller cancellation reaps children.
- [x] Exclude only the canonical selected evidence subtree from every fresh
      build-identity capture; retain dirty/untracked source detection and scoped
      restoration across nesting, unwinding and threads.
- [x] Reconcile decoder charge arithmetic, standard Fastly assembly-error retry,
      the Axum full-suite selector and the current response-type path.
- [x] Complete the earlier nonce and Fastly disclosure changes, and verify Spin's
      empty-method SDK projection and actual host framing without changing its
      deadline, upload, ownership or body-suppression contracts.
- [x] Run focused regressions and the applicable Rust, harness, fixture and docs
      gates. Record failures without treating supporting captures as qualification.

### Focused Evidence

The CLI-only check, real qualification logger, selected-output identity tests,
build/metadata cancellation tests and flate subtotal assertion each reproduced
their original failure before the fix. Direct-controller cancellation checks
Cargo absence and descendant socket closure before either safety cleanup.

The 78 fixture-core tests pass. Spin's 23 SDK resource tests pass with Wasmtime
48.0.2, and the existing Spin outbound-wire suite passes with 166 passing records
and no failed records. The capture is
`crates/edgezero-test-harness/fixtures/reusable-app/.runs/18e70ebb3cd1b6d0-e2a5-0/`;
its records remain `qualification: false`. Send and Begin preserve zero-length
POST while DELETE/OPTIONS have no Content-Length and exactly the host's empty
chunk terminator. GET/HEAD requirements remain strict. The resource receipt is
added before the WASI response freezes its fields and rejects duplicate markers.

Retain the failed baseline harness run and initial failed wire capture. The
baseline source-verifier assertion passed on its exact rerun; no cause is claimed.
The first current full harness run passed 659 tests but detected the entrypoint's
changed control-flow fingerprint. Review confirmed that its sole change installs
the selected-output scope. Only that recorded fingerprint changed; the full
rerun passes all 660 tests. No selector, controller, result or qualification
predicate is relaxed.

Two read-only verification agents rechecked the process/logging/identity and Spin
resource/framing changes. Their follow-up reviews found no remaining issues after
the version-probe ownership, pre-cleanup observation and immutable-header fixes.
### Final Local Validation

- All five primary Rust gates pass on Rust 1.98.1. The workspace run reports 2,700
  passing tests across 33 binaries, no failures and one ignored generated-project
  build. Root, harness and fixture formatting checks pass.
- Harness: 660 passed, three ignored helper entrypoints. Fixture core with
  qualification enabled: 78 passed. Spin SDK resources: 23 passed. The final
  Spin outbound-wire capture has 166 passing records and no failures.
- Strict Clippy passes for the root workspace, harness, qualification fixture and
  Spin WASM all-targets build. Rename the new test's shadowed bindings rather than
  suppressing restriction lints; retain the initial failing lint/format logs.
- CI-tooling tests: 38 passed. Outbound legacy-API and docs contracts pass, as do
  docs formatting, lint and VitePress build. The documented Axum command resolves
  all 43 expected sentinels; this lookup is not a TCP execution claim.

The complete four-adapter/demo/generated-project prescribed suite was not rerun
in this scoped checkpoint. Publication, remote CodeQL/check verification,
per-thread replies/resolution, independent acceptance and hosted qualification
remain outstanding. No commit or push is included in the items 1-3 completion.

## Publication Followup (2026-11-11)

AG subsequently requested publishing this checkpoint and cleaning disposable
build output afterward. The remote branch had independently advanced to
`410a16238b7cb740275ad69b9af628cd52c57bdc` (received-head preparation). Retain
that commit by fast-forwarding before adding the scoped repairs; do not rewrite
or discard it. The earlier green counts above describe the repair tree before
this independent addition, not a full-suite pass for the combined published head.

The combined-tree publication attempt passed root formatting and the Spin WASM
compilation check. Workspace tests, strict workspace Clippy, feature compilation
and the harness rerun exited 101 with actual compiler/linker failures reporting
`No space left on device` (errno 28). The first Node run exited 1 without usable
output; docs/CLI checks could not create their redirected logs. Do not present
those attempts as passing, infer a source regression, suppress checks or waive
the complete-suite/hosted gates. Retain the failed logs and use CI to verify the
published head before any readiness claim.

After disk space recovered, repeat the interrupted checks without modifying
source, assertions or CI policy. The combined tree passes all five primary Rust
gates: 2,702 workspace tests across 34 binaries, strict workspace Clippy, native
feature compilation, Spin WASM compilation and root formatting. The harness
rerun reports `660 passed; 0 failed; 3 ignored`. The isolated CLI-only check,
all 38 tooling tests and outbound docs contract also pass on retry. These are
successful reruns, not a dismissal or relabeling of the failed attempts. The
complete four-adapter/demo/generated-project and hosted gates remain separate.

Inactive fixture build output was removed to recover working space. Only this
task's disposable validation/build outputs may be removed after remote-head
verification; preserve source, the active worktrees and runtime/test evidence.

## Approved Review Followup (2026-11-12)

AG approved these repairs in the existing PR, without compatibility wrappers or
changes to logical deadlines, body suppression, limits, admission or batch semantics:

- [x] Replace the watchdog test's signal-sensitive single read with `read_exact`;
      assert EOF after joining the origin and retain the original timing/cleanup checks.
- [x] Migrate root, demo, both fixtures and generator to exact published SDK 8.0.0;
      retain registry checksum/source guards and the prior decoder pins.
- [x] Re-audit SDK interfaces and execute resource, wire and progress checks.
- [x] Replace Axum's ambient local-spawn panic probe with explicit executor ownership;
      migrate all callers and prove missing/dead/foreign ownership fails before polling work.
- [x] Document the standalone harness gate and exact-pin tradeoffs.
- [ ] Run the prescribed validation, push and reply separately to each finding.

The unchanged baseline `scripts/run_tests.sh` failed the Axum pending-upload TCP
gate: the complete response took 105.035167 ms against a 105 ms maximum. Retain
that failed attempt, all original assertions and the separate hosted qualification
gates. CI's compiler-cache publication also failed after three attempts; its log
does not establish a source cause. No tolerance, exclusion or cache check is waived.

## Authorized Rebuild (2026-11-13)

AG directed rebuilding from `a7d7fd0f2face027e3810ed67e80f6341fcf68b6`
and approved including the scoped `b97efec1` repairs and the saved SDK 8,
Axum ownership and watchdog changes. This supersedes the earlier publication
instruction to retain `410a1623`; that paragraph remains historical evidence.
The rebuild is isolated on `prfection/275-a7-rebuild`. The original PR worktree,
its uncommitted diff, untracked source and a backup ref at `14a6ca8b` are retained.
No remote rewrite or merge is part of the local rebuild authorization.

- [x] Replay only the approved repairs from `b97efec1`, with `a7d7fd0f` as
      their direct parent (`ac5a6aaa`). Do not carry the unused received-head
      preparation API from `410a1623`: no approved consumer or adapter call site
      requires it. Existing normalization and outbound provenance remain intact.
- [x] Restore the saved published SDK 8, watchdog and explicit Axum ownership
      repairs. Declare the connection runtime before its executor so executor
      tasks are released while the runtime remains alive, including on unwind.
- [x] Independently repair evidence capacity: retain one reader, the 4 MiB
      artifact limit and 512 records for general CI evidence; use an explicit
      1,024-record limit for wire captures. Reject overflow before parsing an
      excess record, without truncation. The expanded-inventory regression first
      failed at 518 records and passes with the separate wire bound.
- [x] Independently repair framing receipt formatting. A counting writer proved
      that formatting the JSON value produced 24 writes. Serialize the complete
      record and newline before writing, retaining short-write/error handling.
      This is not a provider-level atomic-write guarantee.
- [x] Independently remove host-I/O timing from the callback-scope test. A
      never-ready upload supplies pending work; a separate deterministic
      item-then-pending driver verifies partial-consumption revocation across
      callbacks. Retain typed Internal failures and fresh-scope assertions.
- [x] Complete current-tree validation, including the reviewed local Viceroy
      repair, all adapter/runtime gates, demo/generated projects and docs.
- [x] Complete independent verification and report any residual failures before
      requesting authorization to replace the PR branch.

### Retained Baseline Evidence

The unchanged `a7d7fd0f` full-suite run passes its workspace, 653 harness tests
and Axum integration stages, but exits 134 in the Fastly SDK timeout-readback
test under stock Viceroy 0.21.0. That runtime lacks the already tracked repair;
the assertion and runtime pins are not relaxed. Rebuild the exact reviewed
patch for local verification, recording its source and executable identity.

The replayed repairs pass root tests, formatting and strict Clippy. The first
harness run reports 659 passes and one socket-close assertion failure in
unchanged `net/wire.rs`; its focused rerun and full 660-test rerun pass. The
cause is unproven. Retain all attempts rather than relabeling the initial
failure as passing or weakening its assertions.

### Rebuild Verification Checkpoint

Two fresh read-only verification agents found no further issues in their
non-overlapping scopes: explicit Axum ownership/shutdown, and SDK/fixture/tooling
contracts. The abort-profile fixture proves rejection and queue ownership; it
does not independently prove successful I/O or shutdown with active producers.
Keep those runtime checks separate. The chosen repairs use one existing runtime,
transport and evidence reader rather than carrying the three excluded commits.

The rebuilt tree passes all five primary Rust gates, all 661 harness tests,
80 qualification-fixture tests, strict root/harness/fixture Clippy and the docs
contract. The Axum scoped run passes 329 tests. An initial harness failure caught
the CI dispatch fingerprint changed solely by supplying the wire record bound;
explicit review updated that one fingerprint, with all controllers/predicates
unchanged. The initial Clippy import-style error and docs constructor-signature
guard failure were repaired without suppressions or compatibility overloads.

Docs lint/build pass. Two modified Markdown files required formatting; the
formatting rerun passes. The standalone tooling invocation initially lacked
`EDGEZERO_TEST_HARNESS_BIN`; build and supply the actual harness executable before
rerunning, as the prescribed full suite already does. The attempted native
Fastly-SDK test cannot link provider imports; only actual WASM/runtime execution
can establish that SDK callback case. These attempts are not runtime passes.

### Completed Local Rebuild Validation

`scripts/run_tests.sh` passes on the unchanged rebuilt source with Rust 1.98.1,
reviewed patched Viceroy 0.21.0, Wasmtime 48.0.2, Spin 4.1.0 and matching
wasm-bindgen 0.2.128 tools. All four adapter CI suites, native feature matrices,
dependency/forwarding guards, the full ignored generated-project build,
demo tests and all three demo WASM builds pass. The workspace reports 2,708
passing tests across 33 binaries, the harness 661, fixture core 80 and demo 66.
Strict WASM all-targets Clippy passes for Fastly, Cloudflare and Spin, in addition
to the five primary Rust gates and strict harness/fixture Clippy. The standalone
170-test tooling rerun also passes with the actual harness executable supplied.

The first full rebuild run stopped at the browser probe after selecting Safari
and receiving a WebDriver HTTP 500. Retain it. The same five ingress tests pass
with matching Chrome/ChromeDriver 157.0.8103.52; the subsequent complete run
passes with that explicit selection. No source, assertion, deadline or capability
changed between these attempts. Logs are retained at
`/private/tmp/pr275-a7-rebuild-NdW5Yx/`.

Latest supporting captures beneath the reusable fixture's `.runs/` are:

- Axum: `18e7ab63cdd89c08-106dc-0`.
- Fastly: `18e7ab6d05568830-10e61-0`, including both callback regressions
  in the 324-test SDK runtime run and the complete 518-record wire artifact.
- Cloudflare: `18e7ab85f9d628d0-12164-0`.
- Spin: `18e7ab9fd9ee4970-13909-0`.

These are working-tree supporting captures, not a frozen-candidate or hosted
qualification claim. Keep all `qualification:false` dispositions and manual
hosted gates open. The original PR branch and dirty worktree remain unchanged;
remote replacement, new-head hosted checks and independent review acceptance
are still outstanding.

## Approved Managed Logging And Upload Framing Repairs (2026-11-13)

AG approved both findings in PR comment `6863865880`, including the scoped
stream-framing amendment, then explicitly requested a hard migration without
compatibility wrappers. The starting head is `1467e28a`. Work stays in PR #275.

Two read-only investigation/proposal agents independently mapped the logging
and upload paths. Choose immutable per-instance managed logging, not concurrent
filter reload. Associate a validated forwarding declaration with its Body, not
request-only metadata that can become stale when public `Parts.body` is replaced.
This adds scalar Body storage, whose native/WASM layout must be measured; it adds
no buffer, allocation, identity token, transport, executor or public length setter.

### Task 1: Managed Platform Logging

Files: Cloudflare/Spin `src/logger.rs`, `src/lib.rs`, adapter manifests/locks;
existing reusable assembly fixtures and Rust `src/runners/assembly.rs`.

- [x] Add a failing isolated backend-installation/sink regression before implementation.
- [x] Hard-cut to `init_logger(level)` with explicit errors. Reuse the existing
      resolver, fern filtering and separate boot Warn target. Workers use the
      console; Spin uses stderr. Identical repeated setup succeeds, changed
      configuration or a foreign logger fails explicitly without reconfiguration.
- [x] Resolve environment policy before assembly, reuse the snapshot for dispatch,
      and propagate initialization failure. `owns_logging=true` changes neither
      installation nor filters. No no-op compatibility initializer remains.
- [x] Prove every level, boot warnings/errors under ordinary Off, repeated calls,
      conflicts and owned logging. Extend existing Rust fixture supervision for
      actual local Wrangler and Spin sink evidence, not an owned-fixture substitute.
- [x] Update adapter/configuration guides, the guide-only four-backend table,
      demo/templates and changelog. Add no manifest capability.

### Task 2: Validated Forwarded Stream Length

Files: core `src/body.rs`, `src/outbound.rs`, a co-located upload counter;
Spin `src/outbound.rs`, `tests/sdk_resources.rs`; existing forwarding fixtures
and Rust wire runner/origin.

- [x] Reproduce loss of a 4096-byte declaration through actual SDK field preparation.
- [x] Capture one valid streamed Content-Length only without Transfer-Encoding or
      Connection nomination. Keep stripping raw CL/TE. Ambiguous/malformed input
      supplies no declaration; retain existing ingress rejection and buffered
      stale-framing behavior. Later normalization can invalidate, never recapture.
- [x] Keep the declaration with its payload through ownership-only wrappers and
      unchanged Parts round-trips. Replacement, JSON, transformations, raw payload
      extraction and redirect rewrites must not retain stale declarations.
- [x] Set Spin's wire CL from this metadata and replace its byte counter with one
      shared scalar validator. Preserve upload-cap 413 precedence and typed source
      errors; observed overrun fails before writing its chunk, short clean EOF fails
      as local BadRequest without invented provider detail. Abort without a new drain.
- [x] Preserve early response/ReaderGone semantics: unconsumed abandoned bytes have
      no equality guarantee. No new prebuffering, stream eligibility for Begin/Batch,
      or change to GET/HEAD rules, deadlines, cancellation or resource ownership.
- [x] Add separately bounded 4096-byte known/unknown-length wire controls, short/long
      synthetic sources and incremental/abort evidence. Do not enlarge the old
      8-byte tests' limits or weaken their validators.
- [x] Update the normative framing paragraphs/API approval record and current guides,
      examples/templates and changelog; no provider qualification promotion.

### Task 3: Verification And Publication

- [x] Run focused red/green tests after each coherent code change, all five primary
      Rust gates, strict harness/fixture/provider lint and `scripts/run_tests.sh`.
      Run the local logging and forwarding controls through the existing Rust harness.
- [x] Complete independent read-only verification and retain failed attempts and exact
      runtime identities.
- [ ] Commit and push normally to the existing PR branch; record the published SHA
      in the separate replies below. Publication follows this validation checkpoint.
- [ ] Reply separately to both findings with executed evidence. Leave review
      acceptance, manual hosted qualification and deployment authorization separate.

Baseline: the prior rebuilt source passed the complete prescribed suite and all
three WASM Clippy targets. Its retained captures are supporting evidence, not a
new execution or hosted qualification for this follow-up.

### Focused Verification And Review

Both original defects were reproduced before implementation: an isolated Spin logger emitted
neither requested message, and actual Spin SDK field preparation omitted CL=4096. The repaired
tests pass. Native/WASM Body measurements are 56/40 bytes respectively, with 16 inline bytes
for the declaration and no metadata allocation. This is not a stable ABI promise.

Two read-only reviewers checked separate logging/framing scopes. They found fixture ownership,
saved-artifact coverage and live-peer test gaps; those were corrected and re-reviewed without
remaining concrete defects. Overlapping-initialization and declared-redirect regressions pass.
The full prescribed validation below was executed independently of these read-only reviews.

Local supporting runs passed Spin assembly (including ten stderr logging cases), Cloudflare
assembly (ten actual Wrangler logging cases) and the full Spin wire suite, including known /
unknown 4096-byte uploads and five gated live-pump probes. Retain captures under
`/private/tmp/pr275-logging-length-z2L3MA/`; all remain `qualification:false`.

Retain earlier failures: missing first-response attestation on headerless Spin assembly errors;
an unmatched wasm-bindgen CLI; a stale local Wrangler shim; an invalid test module-root path;
and the runtime prohibition on intra-component resource-bearing future transfers. The assembly
test now attests a healthy response first, without bypassing identity checks. Logging launches
Wrangler's current generated index, not the compatibility shim. Local pump tests never read the
unsupported completion future: length errors return while its peer remains live/unpolled, and
real origin exchanges separately prove HTTP completion. No runtime/dependency pin, production
framing guarantee, deadline, suppression rule or hosted gate was weakened.

Full-suite validation also exposed incompatible test-workspace binding locks (ingress and
production 0.2.128 versus reusable fixture 0.2.129). Align only the reusable fixture's nine-package
binding family with the production lock, using Cargo's resolver and removing temporary resolver
constraints afterward. Keep provider SDK/runtime and decoder pins unchanged; add no tool router,
duplicate transport, permanent dependency or skipped gate. Re-run the complete prescribed suite
and the actual logging controls with matching 0.2.128 tools. The earlier mismatched attempt is
retained, not counted as passing.

### Completed Validation Checkpoint

The complete `scripts/run_tests.sh` run with production-aligned bindings exits zero:
2,722 root workspace tests, 662 standalone harness tests, all four adapter CI integration
suites, the generated project, the demo's 66 tests and three WASM checks, and outbound
documentation contracts. The existing ignored tests retain their prescribed separate gates;
no test was skipped to obtain this result. The fresh Cloudflare assembly run also exits zero
with all ten actual Wrangler logging controls on the aligned binding graph.

All five primary Rust gates pass. Strict standalone harness/fixture Clippy and all three
provider WASM Clippy targets pass; final root Clippy, root formatting, new Spin wire-fixture
WASM Clippy and `git diff --check` pass after alignment. Documentation format, lint and build
pass. Retain the existing dependency audit warnings and `proc-macro-error2` future-compatibility
notice; neither was suppressed or changed by this repair.

Evidence remains under `/private/tmp/pr275-logging-length-z2L3MA/`, especially
`full-suite-production-bindings.log`, `cloudflare-assembly-production-controller.log` and
the `*-publication.log` final checks. The full-suite supporting runtime captures are
`18e7be32ca9bea90-7390-0` (Axum), `18e7be4190994b08-81f0-0` (Fastly),
`18e7be6772b8bad0-a470-0` (Cloudflare) and `18e7be70ac817d70-aeed-0` (Spin).
These remain local supporting evidence with `qualification:false`, not deployment,
hosted delivery certification or independent review acceptance. Publication and replies
are the remaining steps at this checkpoint; their actual SHA belongs in the PR handoff.

## Approved Fastly Logging And Timing Review Repairs (2026-11-13)

AG approved this scoped plan in conversation after reviewing comments `6869073346`,
`6869468283` and inline thread `4570088506`, starting from `737f8452`. Preserve
all outbound deadlines, error provenance, suppression and resource ownership. Do not
restore general runtime selector stores or introduce compatibility wrappers.

- [x] Add failing co-located regressions for target-bearing Fastly records, immutable
      complete configuration identity and bounded runtime level resolution. Run
      `cargo test --offline --locked -p edgezero-adapter-fastly --lib logger` and
      `cargo test --offline --locked -p edgezero-core logging::tests` (expected red
      before implementation, green afterward).
- [x] Generalize core's existing managed installation guard to compare an immutable
      configuration value. Migrate Cloudflare/Spin callers explicitly. Use it in
      Fastly with endpoint/level/stdout identity and the shared typed errors. Add a
      bounded logging-only optional Config Store lookup as specified in the logging
      design. Never buffer arbitrary values or call the growing SDK lookup helper.
- [x] Hard-cut Fastly custom lifecycle signatures to name the app Hooks type; share
      managed setup with standard entrypoints, before user callbacks/assembly, while
      leaving `owns_logging` completely untouched. Migrate all native/WASM contracts,
      fixtures, demo, templates and documentation without alternative entrypoints.
- [ ] Extend the existing Rust assembly runner with actual Viceroy endpoint receipts:
      one immutable production-Info wasm, multiple runtime-only overrides, target
      formatting, boot filtering, application ownership, retained-instance identity,
      repeated setup and rejected configuration changes. Keep capture sizes bounded.
      Run `scripts/test_integration.sh --adapter fastly --suite assembly`.
- [x] Share the existing Axum pending-forward TCP exercise. Keep exact status,
      original deadline/lower bounds, no-origin and exactly-once cleanup assertions.
      Separate a functional CI test with finite scheduling headroom from the unchanged
      strict +5 ms qualification sentinel. Correct the criterion's binding so a
      functional pass cannot certify 205/105 ms, retain measured failures, and do not
      mark the entire correctness proof ignored. Update requirements/approval records
      only within this approved separation.
- [x] Run focused tests after coherent changes, the five primary Rust gates, strict
      harness/fixture and provider WASM Clippy, `RUST_TEST_THREADS=2 bash scripts/run_tests.sh`,
      and docs format/lint/build. Independent read-only review must check ownership,
      reused-instance policy, bounded lookup and qualification separation.
- [ ] Commit and push normally to PR #275, reply separately to each original finding
      with exact executed evidence, and request current-head re-review. Reviewer
      acceptance, formal approval/one-account exception and manual hosted qualification
      remain separate. Never resolve reviewer threads or merge prematurely.

### Review And Partial Verification Checkpoint

Two read-only investigation/proposal lanes compared an explicit policy argument with
an app Hooks generic. Choose the generic: it preserves direct ownership/manifest provenance
without another policy object, while retained `T` and convertible callback errors remain
independent. One worker implemented only the disjoint Axum test/inventory slice; the
coordinator implemented logging and integrated the combined changes.

The new target regression failed with exit 101 and showed a Fastly line missing the target.
The full-settings guard regression also failed before its API change; both repaired groups
pass. Fastly native logger, bounded fixture logging validation, configuration identity and
foreign-backend tests pass. The qualification-fixture logger test exposed global sink
interference with a different telemetry protocol; isolate that test in a subprocess rather
than filtering its assertions or changing production telemetry. Its complete fixture suite
then passes. Retain all failed attempts.

Independent read-only logging and evidence reviewers checked the changed source. The evidence
review found missing settled-output revalidation, incomplete stdout/facade conflict checks and
an unbounded settlement reread. Corrections use one shared settlement algorithm with bounded
parsed logging events on success/failure, validate all tokens/owners after shutdown, reject
unexpected stdout echoes and sample facade state after every conflict. Mutation/oversize tests
pass; re-review found no remaining concrete issue. These reviews did not run validation.

The explicit strict Axum qualification command was executed with `RUST_TEST_NOCAPTURE=1`:
exit 0, `28 passed; 0 failed; 0 ignored`. Both original 5 ms and functional 50 ms tests pass
in that run; raw 504/408 observations remain in `axum-strict-qualification.log`. This does
not establish a scheduler guarantee or a frozen/hosted qualification record.

The final full-suite run found the shell guard's existing JavaScript regression matrix still
selected the old combined Axum command. Split that matrix into the actual ordinary and strict
commands and assert each has nonempty required sentinels, preventing vacuous missing-test loops.
The corrected guard suite exits 0: `135 tests`, `135 pass`, `0 fail`. An interactive-stdin
full-suite attempt then correctly failed the CLI's non-interactive consent tests and poisoned
their shared test mutex; retain that attempt and rerun with stdin closed, matching CI. Neither
failure is hidden by skipping an assertion or broadening the test selection.

The non-interactive full run passed Axum CI and Fastly SDK/runtime/feature/ingress checks,
then exposed the older negative-logger validator still matching the removed facade-error
message. The fixture correctly rejected changed complete settings. Update its co-located
test first: the old message must fail and the exact fixed configuration-change message must
pass. That test failed with exit 101 against the old validator; migrate the validator without
accepting both protocols. Retain that smoke failure. Narrow CLI/no-feature Clippy also caught
an off-target doc-markdown error; add its missing backticks without a lint suppression.

The new actual Viceroy logging controls pass all 27 cases, using one wasm and runtime-only
changes. However, the complete Fastly assembly suite exits 1 on seven older cases: canonical
assembly-failure responses intentionally lack application headers, while the first prescribed
response must attest its launch identity. Running the identical assembly command against exact
unchanged `737f8452` also exits 1 on those same seven cases. Its temporary base worktree was
removed only after ownership verification; the failed logs are retained. Do not call the
combined assembly suite passing or weaken startup identity to hide the incompatibility.
A supported bootstrap-attestation design retaining exact standard-entrypoint coverage is a
separate unresolved fixture/harness boundary, not a demonstrated logging regression.

Retain captures and logs in the local run directory `pr275-fastly-logging-67jhji/`, including
`fastly-assembly`, `fastly-assembly-final`, `fastly-assembly-baseline` and the explicit timing
log. An intermediate full-suite attempt was interrupted to incorporate evidence-review fixes;
it is not a pass. Two later chat interruptions also stopped validation; neither is counted
as completed evidence. No deployment, reviewer acceptance or merge has occurred.

### Final Local Validation Checkpoint

The uninterrupted `RUST_TEST_THREADS=2 bash scripts/run_tests.sh < /dev/null` run exits 0:
`All tests completed successfully.` Workspace results total 2,725 passed, zero failed and
two ignored across 33 result records; the standalone harness has `667 passed; 0 failed;
3 ignored`. The ignored strict Axum test retains its separately executed 28-test qualification
run above. The shell contracts have 140 passing tests, zero failures. All four adapter CI
integration suites, the fresh generated all-adapter application, the demo tests and three
demo WASM checks, dependency audits and outbound documentation contracts pass.

Fourteen final checks have exit 0 in `final-checks.tsv`: the complete suite; root, harness
and fixture formatting; workspace, harness and native-fixture strict Clippy; workspace feature
and Spin WASM compilation; Fastly CLI-only and no-feature strict Clippy; docs format, lint and
build. The provider WASM feature Clippy runs and Fastly progress-fixture strict Clippy also pass.
No dependency lock or SDK/runtime pin changes. Existing dependency audit/future-compatibility
notices remain unsuppressed.

The exact negative-logger validator test passes after its red run, and Fastly smoke now passes
that real runtime case. Final read-only review of the selector split, exact negative protocol
and doc correction found no concrete regression or weakened check. The authoritative inventory
still has 514 legs (299 ready, 215 planned); the suite guide no longer copies a stale total.

Supporting CI captures are `18e7d11e28174e00-8c44-0` (Axum),
`18e7d12b2d29c8c0-964a-0` (Fastly), `18e7d1502e4a4098-b597-0` (Cloudflare)
and `18e7d1fec7eb0e68-ce54-0` (Spin). These are local functional evidence, not hosted or
frozen-candidate qualification. The separate seven-case Fastly assembly-attestation blocker
above remains open despite all 27 new logging cases passing. Publication records the actual
commit and per-finding replies in the PR handoff; it does not grant merge approval or resolve
reviewer-owned threads.

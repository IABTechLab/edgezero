# Adapter delivery compression implementation plan

## Outcome and boundaries

Implement the [design](../specs/2026-11-02-362-delivery-compression-design.md)
for [EdgeZero #362](https://github.com/IABTechLab/edgezero/issues/362), then
integrate its exact PR commit into
[Trusted Server #884](https://github.com/IABTechLab/trusted-server/pull/884).
Both pull requests remain draft for joint review. Do not deploy either application.

Preserve all unmarked response behavior, standard converter buffering, and custom
sender laziness. Only the application decides when its output is identity and
eligible for platform delivery compression.

## 1. Establish the contract

- [x] Create isolated `feature/362-delivery-compression` worktree from current main.
- [x] Read existing superpowers specs, adapter response conversion, and the actual
      Trusted Server consumer.
- [x] Write this design and plan before source changes.
- [x] Review the design independently against source. Both agent reviews found
      no blockers; the design records the selected minimal API and consumer invariant.

Compare keeping application-local glue, standard-converter-only handling, and the
shared capability/intent/finalizer approach. Select the smallest approach that
also covers custom streaming senders. Do not broaden into a transport rewrite.

## 2. Implement portable capability and intent

- [x] Add a focused core module and export it from `edgezero-core/src/lib.rs`.
- [x] Default capability to unavailable and expose supported-codec information
      before callers choose their output encoder.
- [x] Add explicit typed identity-body delivery selection and query operations.
      Unsupported selection must leave the response unchanged.
- [x] Add focused tests and rustdoc examples. Keep the implementation dependency-
      free beyond the existing core HTTP types.

Run core tests for the new module, then neighboring response/body tests. Record
commands and tested source fingerprints outside the repository.

## 3. Implement Fastly finalization

- [x] Expose the supported capability and a reusable finalizer in the Fastly adapter.
- [x] Call the same finalizer from standard core-response conversion.
- [x] Preserve body ownership and no-poll semantics for custom senders.
- [x] Test header/body consistency, Vary preservation, idempotence, late overrides,
      unmarked encoded responses, and unchanged privacy/cache metadata.
- [x] Exercise standard conversion in the actual Fastly/Viceroy target. Do not
      substitute a host-only helper test for the SDK conversion path.
- [x] Document both normal and custom streaming integration and their ordering.

Add focused regressions before the behavior where practical, observe a meaningful
failure, then implement and rerun the affected neighboring tests.

## 4. Validate and publish EdgeZero first

- [x] Run workspace tests, formatting, strict all-feature Clippy, the
      `fastly cloudflare spin` feature check, and Spin `wasm32-wasip2` check.
- [x] Run targeted Fastly WASM tests with the repository-pinned Viceroy.
- [x] Run docs formatting, lint, and build. Classify any unrelated baseline failure
      once rather than repeatedly rerunning an unchanged failure.
- [x] Independently inspect the complete source diff and actual test output.
- [ ] Commit and push only this feature and its docs. Open a draft EdgeZero PR
      linked to #362 and Trusted Server #858/#884. Record its exact head SHA.

One owner runs Cargo per target directory. Other repositories may use independent
build directories. Do not use mutable local path dependencies as the PR contract.

## 5. Rebuild and integrate Trusted Server

Start only after the EdgeZero draft PR exists.

- [ ] Preserve the old Trusted Server branch as a reference.
- [ ] Rebuild the feature's old-main-to-head delta onto latest Trusted Server main,
      without replaying inherited obsolete parser/merge history.
- [ ] Pin all six workspace EdgeZero declarations to the same exact published PR
      SHA and verify all eight lockfile packages resolve to that source.
- [ ] Replace application-local generic marker, capability type, and Fastly header
      helper with the EdgeZero API. Keep application options only where they still
      bundle meaningful publisher policy.
- [ ] Keep input/output codec separation and SSAT eligibility, consent, auction,
      EOF, cache, trace/deadline/ingress, and terminal privacy behavior. Carry an explicit
      local pending output plan through inline, buffered, template, lazy, and ordered-
      EOF paths. Template encoders must use that plan instead of mutable headers.
      Clear representation metadata and mark only after installing the final identity
      bytes or identity-producing stream; the old pre-transform marker sites cannot
      call the checked EdgeZero API directly.
- [ ] Call the EdgeZero finalizer after late Fastly effects in the custom sender;
      never route it through the collecting standard converter.
- [ ] Adapt and run the compression scenarios plus relevant new-main regressions.
- [ ] Update the Trusted Server design and guide to describe the new ownership
      and temporary exact-commit dependency, with a removal condition of a merged
      EdgeZero release.

Rebase conflict resolution must preserve main's current trace and absolute-
deadline behavior. Resolve compression changes semantically, not by replacing
whole files from the old branch. Use a lease tied to the verified old remote head
when publishing the approved history rewrite.

## 6. Validate and publish Trusted Server

- [ ] Run focused publisher, adapter, and terminal-effect checks first.
- [ ] Run current repository CI gates and actual required EOF runtime controls,
      preserving proof tied to final relevant inputs.
- [ ] Independently inspect the combined consumer diff and dependency consistency.
- [ ] Push the rebuilt branch, update #884, and keep both PRs draft.
- [ ] Update both issue descriptions and PR descriptions with bidirectional links,
      exact dependency SHA, results, and remaining staging work.

## Completion record

Core, pure Fastly, and actual Viceroy contract tests observed meaningful failures
before implementation, then passed on the final source. The final focused runs
passed core 510, native Fastly 68, Viceroy contracts 12, SDK converter neighbors 4,
and the new rustdoc example. Scoped native/WASM strict Clippy passed. An independent
source review found no defects or smaller contract-preserving shape. Full workspace
tests, all-feature strict Clippy, feature compilation, Spin WASM checks, source/pin
guards, core documentation, and docs checks subsequently passed. Publication and
cross-repository integration results are recorded in the paired PR descriptions.

Record finished steps here as work proceeds. PR descriptions hold final check
commands and outcomes. Runtime delivery negotiation and performance remain
unverified until real Fastly staging, even when all local checks pass.

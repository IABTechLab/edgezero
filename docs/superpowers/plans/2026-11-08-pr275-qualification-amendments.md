# PR 275 Qualification Amendments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement AG's two directly approved qualification amendments without changing production outbound behavior or promoting hosted evidence.

**Architecture:** Reuse the Rust test harness, existing acceptance schema and attributed requirement-change decisions. Separate content-bearing SDK parity from bodiless reference observations. HTTP/3 is assessed per adapter/deployment, not inferred from another application's staging service.

**Tech Stack:** Rust fixture harness, existing JSON acceptance inventory and compressed regression snapshot, Markdown requirements/approval/runbook.

## Authority And Invariants

AG approved both scoped amendments in the conversation. Preserve all 255 criteria and the existing 511 leg identities; append exactly three HTTP/3 legs, for 514 total. Preserve historical requirement hashes, failed captures and nine SDK-reference deviations. Unknown support, a disabled deployment and unavailable clients are not proof of unsupported HTTP/3. No production behavior, dependency pins, capability enum, deployment, source freeze or qualification promotion changes.

## Task 1: Regression Coverage

**Files:** `crates/edgezero-test-harness/src/runners/framing.rs`, `src/evidence/cutover/catalog.rs` and existing wire tests in `src/runners/response_headers.rs` (paths relative to the harness).

- [x] Add failing tests for the six content-bearing SDK comparisons versus twelve bodiless reference observations. Keep changed buffered framing and unexpected reference changes fatal.
- [x] Add a HEAD wire validator test requiring clean EOF and zero actual post-header octets, rejecting header-only evidence, extra bytes and incomplete capture.
- [x] Add inventory tests proving all four pending HTTP/3 dispositions are required; one attributed unsupported-deployment requirement-change affects only its scoped leg; an ordinary defer or missing evidence cannot waive it.
- [x] Run `cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml runners::framing::tests` and the new catalog tests; record expected red results before implementing. Baseline: eight passes. New framing tests were red for the missing scope helper/shared validator visibility. HTTP/3 tests were red for missing legs and conditional applicability; both sets now pass.

## Task 2: Minimal Implementation

**Files:** the framing runner and existing `response_headers::assembly_request` helper.

- [x] Keep `validate_sdk_parity` strict for buffered/empty/explicit content-bearing responses. Classify HEAD/explicit HEAD, 204, 205, 304/explicit 304 as reference-only on both protocols, retaining exact controls for the nine differing observations.
- [x] Publish separate parity and reference counters. Reuse the bounded clean-EOF HTTP/1 capture for both HEAD routes; retain HTTP/2 curl observations and do not mislabel them as payload evidence.
- [x] Run the targeted Rust tests after the code change. No production adapter or response-framing changes.

## Task 3: Requirements And Acceptance Metadata

**Files:** `docs/superpowers/specs/2026-10-17-edgezero-pr275-requirements.md`, `docs/audits/2026-10-18-outbound-api-approval.md`, `crates/edgezero-test-harness/acceptance/outbound-cutover-cases.json`, `src/evidence/cutover.rs`, `src/evidence/cutover/reviewed_cases.json.gz` and affected executable-owner fingerprints only if required.

- [x] Append the dated authority and supersession record, preserving historical decisions. Amend EZ-1 criterion 2 for four adapter/deployment HTTP/3 dispositions while retaining its Fastly HTTP/1.1 and HTTP/2 requirements.
- [x] Amend EZ-18.4/18.6 and criteria 3/6 to content-bearing SDK parity and strict bodiless suppression. Preserve valid application HEAD/304 lengths on host handoff and existing 204/205 validation.
- [x] Retain the existing Fastly HTTP/3 identity; make its applicability blocking and add three equally scoped adapter legs with pending decisions. Remove only `catalog::baseline`'s old HTTP/3 conditional exception to prevent later downgrades. Unsupported is deferred only by an attributed single-leg `requirement-change` with evidence. Do not weaken generic decision validation.
- [x] Mechanically refresh current requirement/criterion hashes and affected snapshot values, preserving all original 140 test titles and 223 scenarios. Explicitly review the new 514-leg identity digest and all changed expectations, including compression-only sibling rows.
- [x] Run full harness tests and acceptance coverage; no dropped identities and no hosted qualification claim. Harness: 640 passed, two existing ignored; reviewed snapshot scenarios pass. Coverage: 255 criteria / 514 legs, 299 ready / 215 planned, 217 required-local bindings ready and 48 required-external legs planned; `qualified: false`.

## Task 4: Guidance And Verification

**Files:** `docs/guide/{capabilities,streaming}.md`, response-egress design, existing follow-up/implementation plans, harness `SUITES.md` and qualification runbook.

- [x] Align current guidance; add receive-protocol facts outside declarable capability matrices. Require negotiated HTTP/3 proof, exact runtime/deployment/config identity and original deadline/origin witnesses. A proxy or guest-normalized HTTP version is not transport proof.
- [x] Run the response-framing integration suite on all four local adapters: 80 production cases and eight clean-EOF HTTP/1 HEAD checks pass. Fastly reports six passing parity comparisons, twelve passing reference observations and the same nine reference differences. The complete run has zero failed/unsupported cases and `qualification: false`; hosted gates stay open. Local run identifier: `18e5fdca19019570-bae9-0`.
- [x] Run the five root CI gates, full `scripts/run_tests.sh`, harness/fixture/demo formatting and strict adapter/WASM/docs checks prescribed by current workflows. Record failures honestly; no acceptance relaxation to obtain green.
- [x] Obtain read-only independent verification of framing behavior and inventory/history scope and review the verified diff for publication.

Publication uses the existing PR #275 handoff as its completion record: commit and push, reply
separately in the two original amendment threads without resolving own threads, and update the
existing handoff with the actual pushed SHA. Existing disputed external threads remain open.

**Plan review:** Independent read-only review approved the plan and the narrow baseline-rule correction. Reuse `response_headers::assembly_request` and its existing `validate_wire` (sibling visibility only), avoiding a second parser or clean-EOF validator.

**Independent verification:** The framing reviewer found no source findings and confirmed unchanged
production behavior/comparators and preserved historical captures. The inventory reviewer verified
all original IDs/scenarios and found that a combined HTTP/3 N/A decision was still possible. A
new combined-scope mutation failed in the full harness (639 passed, one intended failure, two
existing ignored) and the focused test. The correction rejects only approved HTTP/3
requirement-change decisions whose scope is not exactly the current leg. Both focused tests and
the full 640-test harness rerun pass; the reviewer rechecked the fix with no remaining source
findings. Generic approval schemas and disposition rules remain unchanged. Central broad
verification completed as recorded below.

## Retained Verification Attempts

- The first Fastly framing launch rejected mismatched source/build identities because a harness
  source edit overlapped its fixture build. The guard remained intact; stable-source reruns pass
  the six content-bearing comparisons, twelve references (nine differences), and both HEAD wire
  checks. This failed launch is not qualification credit.
- One all-adapter framing attempt used the root workspace's wasm-bindgen 0.2.128 instead of the
  reusable fixture's locked 0.2.129 and correctly returned unsupported/exit 2 for Cloudflare.
  Subsequent attempts exposed an unconfigured Wrangler shim and an older fidelity-fixture SDK.
  The reusable launcher requires its documented Wrangler 4.131.0 / Miniflare 5 toolchain and
  matching bundled workerd; selecting installed tools is not a dependency or runtime-pin change.
- The first broad run passed all four adapter CI suites, then the generated-app TCP proof failed
  with `EINVAL` during bounded response reading. The exact generated-workspace command passed
  unchanged on retry. A later parallel broad run observed the Axum 200 ms terminal at
  216.769333 ms, exceeding its unchanged 205 ms target. Neither failure is relabeled or dismissed,
  and passing retries alone do not establish either failure's cause.
- The `RUST_TEST_THREADS=1` broad rerun passed workspace, harness, four adapter suites and the
  generated-app test, but its final strict result guard rejected the generated test's separated
  name/`ok` output. The guard is unchanged. The complete `RUST_TEST_THREADS=2` rerun passed,
  retaining the same tests, budgets and assertions. This is development verification, not
  frozen-candidate, hosted, or strict timing qualification.

## Final Local Verification

- Five root Rust gates: exit 0; workspace 2,698 passed, one existing ignored, 33 binaries.
- `RUST_TEST_THREADS=2 bash scripts/run_tests.sh`: exit 0, ending with
  `All tests completed successfully.` Includes 640 harness passes/two existing ignored,
  acceptance/dependency audits, all four native and adapter runtime suites, the explicitly
  executed generated-project gate, demo workspace tests, three demo WASM targets and doc guards.
- The 30 additional workflow format/lint/WASM/docs commands: all exit 0, including strict
  harness Clippy and docs format/lint/build.
- All-adapter response-framing: exit 0; 80 production cases, eight HEAD wire checks, six Fastly
  buffered parity comparisons and twelve bodiless references (nine differences). No failed or
  unsupported cases. Cloudflare uses the reusable fixture's matching wasm-bindgen 0.2.129,
  installed Wrangler 4.131.0 / Miniflare 5.20260910.0-alpha and its bundled workerd.
- Inventory: 255 criteria, 514 legs, 299 ready/215 planned, 217 required-local ready and 48
  required-external planned. All original 511 identities, 140 regression titles and 223 scenario
  records are preserved. `qualified: false`; no deployment, freeze or capability promotion.

These checks cover the implementation tree prepared for publication. Earlier failed attempts
remain disclosed above; passing development verification does not erase them or establish a
root cause, hosted guarantee, or exact-candidate qualification.

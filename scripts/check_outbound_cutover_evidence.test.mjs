import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  truncateSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import * as checker from "./check_outbound_cutover_evidence.mjs";
import {
  acceptanceCriteria,
  validateManifest as validateManifestWithPolicy,
  validateRelease,
} from "./check_outbound_cutover_evidence.mjs";

const source = `### EZ-1. Forwarding

#### Requirements

- **EZ-1.1** Forward without a body.

#### Acceptance criteria

1. Axum and workerd. The origin sees no bytes.

#### Out of scope
`;
const sha = "a".repeat(40);
const digest = (value) => createHash("sha256").update(value).digest("hex");
const clone = (value) => structuredClone(value);
function fixtureLegPolicy(input) {
  if (!input.startsWith("### ")) return undefined;
  return digest(
    JSON.stringify(
      acceptanceCriteria(input)
        .flatMap((c) => ["axum", "workerd"].map((env) => `${c.case_id}@${env}`))
        .sort(),
    ),
  );
}
function validateManifest(manifest, input) {
  return validateManifestWithPolicy(manifest, input, fixtureLegPolicy(input));
}
function inventory(input = source) {
  const criterion = acceptanceCriteria(input)[0];
  return {
    schema: 1,
    source_digest: digest(input),
    decisions: [],
    cases: ["axum", "workerd"].map((environment) => ({
      case_id: criterion.case_id,
      source_id: criterion.case_id,
      source_digest: criterion.source_digest,
      requirement_ids: [criterion.section + ".1"],
      environment,
      applicability: "blocking",
      approval: null,
      condition: null,
      owner: "EdgeZero",
      task: "P2",
      substep: "actual ingress forwarding",
      sentinel: "forwards_bodiless_get",
      command: `fixture ${environment} forwards_bodiless_get`,
      expected: criterion.text,
      artifact_path: `outbound/EZ-1/01/${environment}.json`,
      implementation: "ready",
    })),
  };
}
function evidence(manifest = inventory()) {
  return {
    schema: 1,
    stage: "frozen-candidate",
    source_state: "clean",
    candidate_sha: sha,
    builds: [
      {
        build_id: "d".repeat(64),
        candidate_sha: sha,
        artifact_path: "build/app.bin",
      },
    ],
    runner_image: "qualification@sha256:" + "b".repeat(64),
    toolchain: {
      rust: "1.98.1",
      fastly_sdk: "0.13.1",
      viceroy: "0.21.0",
      spin: "4.1.0",
      workerd: "1.20261018.0",
    },
    retention: {
      kind: "release-asset",
      uri: "https://example.com/release/evidence",
      expires_at: null,
    },
    comparisons: null,
    applicability: manifest.cases.map((c) => ({
      case_id: c.case_id,
      environment: c.environment,
      disposition: "required",
    })),
    results: manifest.cases.map((c) => ({
      case_id: c.case_id,
      environment: c.environment,
      candidate_sha: sha,
      build_id: "d".repeat(64),
      source_digest: c.source_digest,
      status: "passed",
      command: c.command,
      sentinel: c.sentinel,
      artifact_path: c.artifact_path,
      artifact_sha256: "c".repeat(64),
      samples: 1,
      assertions: [{ name: "origin body bytes", passed: true }],
      measurements: [],
    })),
  };
}
function release(manifest, record, input = source) {
  return validateRelease(manifest, record, {
    candidateSha: sha,
    source: input,
    reviewedLegSetSha256: fixtureLegPolicy(input),
  });
}

test("manifest validation accepts pending work, not qualification", () => {
  const manifest = inventory();
  manifest.cases.forEach((c) => {
    c.implementation = "planned";
  });
  assert.equal(validateManifest(manifest, source).legs, 2);
  assert.throws(() => release(manifest, evidence(manifest)), /planned/);
});
test("ready sentinel selection includes every required test, not planned legs", () => {
  assert.equal(typeof checker.readySentinels, "function");
  const manifest = inventory();
  manifest.cases[0].additional_sentinels = ["tests::factory"];
  assert.deepEqual(checker.readySentinels(manifest, "axum"), [
    "forwards_bodiless_get",
    "tests::factory",
  ]);
  manifest.cases[0].implementation = "planned";
  assert.throws(
    () => checker.readySentinels(manifest, "axum"),
    /no ready test sentinels/,
  );
});
test("additional test sentinels must be nonempty unique Rust test names", () => {
  for (const value of [
    null,
    [],
    [""],
    ["a\nb"],
    ["factory", "factory"],
    ["forwards_bodiless_get"],
  ]) {
    const manifest = inventory();
    manifest.cases[0].additional_sentinels = value;
    assert.throws(
      () => validateManifest(manifest, source),
      /additional_sentinels/,
    );
  }
});
test("grouped sentinel selection requires every environment and deduplicates shared tests", () => {
  const manifest = inventory();
  manifest.cases[1].sentinel = "tests::other";
  assert.deepEqual(checker.readySentinels(manifest, "axum,workerd"), [
    "forwards_bodiless_get",
    "tests::other",
  ]);
  manifest.cases[1].sentinel = "forwards_bodiless_get";
  assert.deepEqual(checker.readySentinels(manifest, "axum,workerd"), [
    "forwards_bodiless_get",
  ]);
  for (const selection of ["axum,", ",axum", "axum,missing", "axum,axum"]) {
    assert.throws(() => checker.readySentinels(manifest, selection));
  }
  manifest.cases[1].implementation = "planned";
  assert.throws(() => checker.readySentinels(manifest, "axum,workerd"));
});
test("complete exact-revision evidence passes", () => {
  assert.equal(release(inventory(), evidence()).passed, 2);
});
test("comparison legs require measured observations as well as build identities", () => {
  const manifest = inventory();
  manifest.cases[0].comparison = true;
  const record = evidence(manifest);
  record.comparisons = { baseline: "native-control", candidate: sha };
  assert.throws(() => release(manifest, record), /comparison measurements/);

  record.results[0].measurements = [
    {
      name: "fixture comparison",
      samples: 1,
      value: 1,
      rule: "le",
      bound: 1,
      passed: true,
    },
  ];
  assert.equal(release(manifest, record).passed, 2);
  record.comparisons = null;
  assert.throws(() => release(manifest, record), /baseline comparison/);
});
test("hosted optimizer acceptance requires a native-path comparison", () => {
  const manifest = JSON.parse(
    readFileSync(
      new URL(
        "../tests/fixtures/reusable-app/outbound-cutover-cases.json",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  const legs = manifest.cases.filter(
    (c) => c.source_id === "EZ-8/acceptance/08",
  );
  assert.equal(legs.length, 1);
  assert.equal(legs[0].comparison, true);
});
test("rejects a missing blocking environment leg", () => {
  const record = evidence();
  record.results.pop();
  assert.throws(() => release(inventory(), record), /missing result/);
});
test("rejects wrong candidate and per-result revisions", () => {
  for (const change of [
    (r) => {
      r.candidate_sha = "d".repeat(40);
    },
    (r) => {
      r.results[0].candidate_sha = "d".repeat(40);
    },
  ]) {
    const record = evidence();
    change(record);
    assert.throws(() => release(inventory(), record), /revision/);
  }
});
test("rejects zero samples and failed measurements or assertions", () => {
  for (const change of [
    (r) => {
      r.results[0].samples = 0;
    },
    (r) => {
      r.results[0].assertions[0].passed = false;
    },
    (r) => {
      r.results[0].measurements = [
        { name: "elapsed", samples: 0, passed: true },
      ];
    },
    (r) => {
      r.results[0].measurements = [
        { name: "elapsed", samples: 1, passed: false },
      ];
    },
    (r) => {
      r.results[0].status = "pending";
    },
  ]) {
    const record = evidence();
    change(record);
    assert.throws(() => release(inventory(), record));
  }
});
test("rejects expired-only artifacts, including long-lived CI artifacts", () => {
  for (const retention of [
    {
      kind: "ci-artifact",
      uri: "https://example.com/artifact",
      expires_at: "2099-01-01",
    },
    {
      kind: "release-asset",
      uri: "https://example.com/artifact",
      expires_at: "2026-11-01",
    },
  ]) {
    const record = evidence();
    record.retention = retention;
    assert.throws(() => release(inventory(), record), /retention/);
  }
});
test("rejects inherited or unknown numeric measurement rules", () => {
  for (const rule of ["toString", "constructor", "unknown"]) {
    const record = evidence();
    record.results[0].measurements = [
      {
        name: "deadline",
        samples: 1,
        value: 999,
        bound: 5,
        passed: true,
        rule,
      },
    ];
    assert.throws(() => release(inventory(), record), /measurement/);
  }
});
test("rejects wrong environments, duplicates and extra results", () => {
  for (const change of [
    (r) => {
      r.results[0].environment = "viceroy";
    },
    (r) => {
      r.results.push(clone(r.results[0]));
    },
    (r) => {
      r.results[0].case_id = "EZ-9/acceptance/99";
    },
  ]) {
    const record = evidence();
    change(record);
    assert.throws(() => release(inventory(), record));
  }
});
test("rejects unjustified deferral and missing applicability maps", () => {
  const record = evidence();
  record.applicability[0] = {
    ...record.applicability[0],
    disposition: "deferred",
    reason: "not implemented",
  };
  assert.throws(() => release(inventory(), record), /applicability/);
  delete record.applicability;
  assert.throws(() => release(inventory(), record), /applicability/);
});
test("cannot relabel a blocking criterion as an unadopted follow-up", () => {
  const manifest = inventory();
  manifest.cases[0].applicability = "follow-up";
  assert.throws(() => validateManifest(manifest, source), /blocking/);
});
test("requires separately scoped approval, not a sibling promotion", () => {
  const input = source.replaceAll("EZ-1", "EZ-24");
  const manifest = inventory(input);
  manifest.cases[0].applicability = "conditional";
  manifest.cases[0].condition = "EZ-5.9 adopted";
  manifest.cases[0].approval = "EZ-5.9:axum";
  manifest.decisions = [
    {
      id: "EZ-5.9:axum",
      status: "pending",
      scope: [manifest.cases[0].case_id + "@axum"],
    },
  ];
  const record = evidence(manifest);
  record.applicability[0] = {
    ...record.applicability[0],
    disposition: "deferred",
    condition: "EZ-5.9 adopted",
  };
  record.results.shift();
  assert.equal(release(manifest, record, input).deferred, 1);
  manifest.decisions[0] = {
    ...manifest.decisions[0],
    status: "approved",
    action: "promote",
  };
  assert.throws(() => release(manifest, record, input), /approval/);
});
test("a blocking Image Optimizer leg cannot be skipped for missing entitlement", () => {
  const record = evidence();
  record.applicability[0].disposition = "deferred";
  record.applicability[0].condition = "no entitlement";
  assert.throws(() => release(inventory(), record), /applicability/);
});
test("rejects changed criterion text, missing criteria and wrong requirement IDs", () => {
  for (const change of [
    (m) => {
      m.cases[0].source_digest = "0".repeat(64);
    },
    (m) => {
      m.cases = [];
    },
    (m) => {
      m.cases[0].requirement_ids = ["EZ-99.1"];
    },
    (m) => {
      m.cases[0].expected = "weaker assertion";
    },
  ]) {
    const manifest = inventory();
    change(manifest);
    assert.throws(() => validateManifest(manifest, source));
  }
});
test("parser inventories unnumbered criteria and hashes complete multiline text", () => {
  const text =
    "### EZ-9. Redirects\n#### Acceptance criteria\n\n- Unit. First.\n  A second line.\n- Unit. Second.\n\n#### Out of scope\n";
  const criteria = acceptanceCriteria(text);
  assert.deepEqual(
    criteria.map((c) => c.case_id),
    ["EZ-9/acceptance/01", "EZ-9/acceptance/02"],
  );
  assert.equal(
    criteria[0].source_digest,
    digest("Unit. First.\n  A second line."),
  );
});
test("rejects missing toolchain, runner image and comparison build identities", () => {
  for (const change of [
    (r) => {
      r.toolchain.rust = "1.95.0";
    },
    (r) => {
      delete r.runner_image;
    },
    (r) => {
      delete r.toolchain.workerd;
    },
    (r) => {
      r.comparisons = { baseline: "", candidate: sha };
    },
  ]) {
    const record = evidence();
    change(record);
    assert.throws(() => release(inventory(), record));
  }
});
test("real inventory covers all 26 acceptance sections with distinct caching/pooling cases", () => {
  const path = new URL(
    "../tests/fixtures/reusable-app/outbound-cutover-cases.json",
    import.meta.url,
  );
  const manifest = JSON.parse(readFileSync(path, "utf8"));
  const spec = readFileSync(
    new URL(
      "../docs/superpowers/specs/2026-10-17-edgezero-pr275-requirements.md",
      import.meta.url,
    ),
    "utf8",
  );
  const report = validateManifest(manifest, spec);
  assert.equal(
    new Set(acceptanceCriteria(spec).map((c) => c.section)).size,
    26,
  );
  assert.ok(report.legs > report.criteria);
  for (const suffix of ["14a", "14b"])
    assert.ok(
      manifest.cases.some((c) => c.case_id === `EZ-2/acceptance/${suffix}`),
    );
});
test("local assembled-app criteria bind guarded tests without closing pending legs", () => {
  const manifest = JSON.parse(
    readFileSync(
      new URL(
        "../tests/fixtures/reusable-app/outbound-cutover-cases.json",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  const workflow = readFileSync(
    new URL("../.github/workflows/test.yml", import.meta.url),
    "utf8",
  );
  const sentinels = [
    "configured_target_limit_applies_on_tcp",
    "configured_renderer_applies_to_not_found",
    "admission_abort_prevents_response_and_dispatch",
    "test_server_preserves_upload_and_read_deadline",
    "test_server_streams_before_source_completion",
    "test_server_buffers_with_exact_content_length",
    "test_server_suppresses_head_payload",
    "raw_head_limit_precedes_admission",
    "test_server_shutdown_releases_live_source",
    "state_client_skips_real_transport_initialization",
    "foreign_platform_app_keeps_configured_policies",
    "plain_tokio_test_runs_local_handler_work",
  ];
  const cases = manifest.cases.filter((c) => c.task === "F9b");
  assert.equal(cases.length, 13);
  for (const [index, name] of sentinels.entries()) {
    const criterion = String(index + 1).padStart(2, "0");
    const c = cases.find(
      (entry) => entry.case_id === `EZ-23/acceptance/${criterion}`,
    );
    assert.ok(c, criterion);
    const sentinel = `test_utils::tests::${name}`;
    const command =
      "scripts/run_test_nonzero.sh --include-ignored --inventory axum/native-tcp,unit/axum,axum/http1 cargo test --offline --locked -p edgezero-adapter-axum --no-default-features --features axum,test-utils --all-targets";
    assert.equal(c.implementation, "ready", criterion);
    assert.equal(c.environment, "axum/native-tcp", criterion);
    assert.equal(c.applicability, "follow-up", criterion);
    assert.equal(c.approval, null, criterion);
    assert.equal(c.sentinel, sentinel, criterion);
    assert.equal(c.command, command, criterion);
    assert.equal(
      c.artifact_path,
      `outbound/EZ-23/acceptance/${criterion}/axum/native-tcp.log`,
    );
    assert.ok(workflow.includes(command), `${criterion} CI command`);
  }
  for (const criterion of ["13"]) {
    const c = cases.find(
      (entry) => entry.case_id === `EZ-23/acceptance/${criterion}`,
    );
    assert.equal(c.implementation, "planned", criterion);
    assert.equal(c.approval, null, criterion);
  }
  const factory =
    "service::tests::state_client_skips_real_transport_initialization_factory";
  assert.deepEqual(
    cases.find((c) => c.case_id === "EZ-23/acceptance/10").additional_sentinels,
    [factory],
  );
  const axumStep = workflow
    .split("- name: Run Axum host tests")[1]
    .split("- name:")[0];
  assert.equal(
    axumStep.match(/cargo test/g)?.length,
    1,
    "one Axum suite execution",
  );
  assert.ok(
    !axumStep.includes("test_utils::tests::"),
    "no duplicate CI sentinel list",
  );
});
test("redirect core criteria bind every required test before one module execution", () => {
  const manifest = JSON.parse(
    readFileSync(
      new URL(
        "../tests/fixtures/reusable-app/outbound-cutover-cases.json",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  const spec = readFileSync(
    new URL(
      "../docs/superpowers/specs/2026-10-17-edgezero-pr275-requirements.md",
      import.meta.url,
    ),
    "utf8",
  );
  assert.deepEqual(validateManifest(manifest, spec), {
    criteria: 255,
    legs: 511,
    planned: 450,
  });
  const command =
    "scripts/run_test_nonzero.sh --inventory unit/core cargo test --offline --locked -p edgezero-core --features test-utils --lib outbound::";
  const bindings = [
    [
      "01",
      [
        "every_status_and_portable_method_obeys_the_redirect_table",
        "nonredirect_status_precedes_all_location_rules",
        "plain_send_does_not_follow_a_scripted_redirect",
      ],
    ],
    [
      "02",
      [
        "rewritten_post_drops_content_metadata_and_expectation_for_later_replay",
        "replaying_empty_post_preserves_content_type",
        "buffered_redirect_replay_shares_the_original_nonempty_allocation",
      ],
    ],
    [
      "03",
      [
        "streamed_upload_cannot_replay_and_never_consults_policy",
        "replay_rule_precedes_limit_and_invalid_location",
      ],
    ],
    [
      "04",
      [
        "inbound_empty_get_stream_is_settled_once_before_all_replayed_hops",
        "send_empty_get_scope_closed_from_eof_never_dispatches",
        "send_empty_get_scope_closed_from_source_drop_never_dispatches",
        "follow_empty_get_scope_closed_from_eof_never_dispatches",
        "follow_empty_get_scope_closed_from_source_drop_never_dispatches",
      ],
    ],
    [
      "05",
      [
        "limit_four_sends_five_requests_and_skips_a_fifth_policy_call",
        "redirect_limit_accepts_only_one_through_twenty",
        "location_presence_and_method_rules_precede_limit_and_location_resolution",
        "limit_precedes_invalid_location_and_policy",
      ],
    ],
    ["06", ["origin_changes_strip_only_automatic_credentials_before_policy"]],
    [
      "07",
      [
        "relative_and_absolute_locations_resolve_without_sending_fragments",
        "location_presence_and_method_rules_precede_limit_and_location_resolution",
        "unusable_locations_fail_at_the_answering_hop_before_policy",
        "original_nontext_and_duplicate_location_evidence_survives_normalization",
      ],
    ],
    [
      "08",
      [
        "original_nontext_and_duplicate_location_evidence_survives_normalization",
        "earlier_stop_rules_do_not_bypass_normal_header_filtering",
      ],
    ],
    [
      "09",
      [
        "policy_owns_local_state_and_returns_nonclone_nonsend_nondebug_error_unchanged",
        "policy_stop_returns_the_answering_head_as_final",
        "non_http_target_can_be_stopped_rejected_or_replaced_but_not_followed_prepared",
        "taking_prepared_request_then_following_prepared_is_a_protocol_failure",
        "policy_observes_normalized_head_and_next_request_before_any_next_dispatch",
        "policy_returned_invalid_request_remains_a_client_error_at_the_selected_hop",
      ],
    ],
    [
      "10",
      [
        "followed_bodies_are_dropped_without_polling_or_finalization",
        "stopped_and_limit_reached_heads_keep_ordinary_final_body_errors",
        "stopped_pending_final_body_waits_only_until_the_original_deadline",
        "transport_errors_keep_their_original_category_and_failed_hop_index",
      ],
    ],
    [
      "11",
      [
        "redirect_chain_uses_supplied_head_client_and_original_deadline",
        "original_chain_deadline_does_not_replace_a_fresh_requests_default_timeout",
        "later_prepared_hops_inherit_policy_overrides_not_the_original_options",
        "policy_time_is_charged_to_original_deadline_before_follow_or_finalization",
        "expired_policy_rejection_is_not_replaced_with_a_deadline_error",
        "unsupported_next_options_win_before_the_expired_chain_budget",
        "replacement_deadlines_cannot_extend_the_original_final_stream_bound",
        "policy_stop_clock_rewind_after_observed_head_is_internal",
        "policy_error_survives_a_clock_rewind_after_observed_head",
      ],
    ],
    [
      "12",
      [
        "prepared_requests_retain_all_unchanged_caps_and_options",
        "policy_authorized_cross_origin_credentials_are_not_stripped_again",
        "image_optimizer_options_survive_every_prepared_follow_hop",
        "freshly_built_policy_request_uses_its_own_default_caps_and_options",
        "later_prepared_hops_inherit_policy_overrides_not_the_original_options",
      ],
    ],
    [
      "13",
      [
        "dropping_inflight_second_hop_releases_it_and_prevents_a_third_dispatch",
        "dropping_unpolled_follow_releases_the_owned_request_without_transport_work",
        "first_pending_hop_drop_prevents_a_second_dispatch",
        "closing_callback_during_policy_prevents_the_next_dispatch",
        "closing_callback_from_followed_source_drop_prevents_the_next_dispatch",
      ],
    ],
    ["14", ["followed_hop_logs_omit_query_names_values_and_fragments"]],
    [
      "15",
      [
        "redirect_chain_uses_supplied_head_client_and_original_deadline",
        "every_status_and_portable_method_obeys_the_redirect_table",
        "rewritten_post_drops_content_metadata_and_expectation_for_later_replay",
        "relative_and_absolute_locations_resolve_without_sending_fragments",
        "origin_changes_strip_only_automatic_credentials_before_policy",
      ],
    ],
  ];
  const required = new Set();
  for (const [criterion, names] of bindings) {
    const c = manifest.cases.find(
      (entry) =>
        entry.case_id === `EZ-9/acceptance/${criterion}` &&
        entry.environment === "unit/core",
    );
    assert.ok(c, criterion);
    assert.equal(c.implementation, "ready", criterion);
    assert.equal(c.applicability, "blocking", criterion);
    assert.equal(c.approval, null, criterion);
    assert.equal(c.condition, null, criterion);
    assert.equal(c.command, command, criterion);
    assert.equal(c.artifact_path, "outbound/unit/core/raw.log", criterion);
    const expected = names.map((name) => `outbound::redirect_tests::${name}`);
    assert.deepEqual(
      [c.sentinel, ...(c.additional_sentinels ?? [])],
      expected,
      criterion,
    );
    for (const sentinel of expected) required.add(sentinel);
  }
  assert.equal(required.size, 51);
  assert.deepEqual(
    checker
      .readySentinels(manifest, "unit/core")
      .filter((name) => name.startsWith("outbound::redirect_tests::")),
    [
      "outbound::redirect_tests::connect_timeout_round_trip_and_follow_preserve_the_aggregate_budget",
      ...required,
    ],
  );
});

test("core batch and marker bindings guard exact tests without promoting adapter legs", () => {
  const manifest = JSON.parse(
    readFileSync(
      new URL(
        "../tests/fixtures/reusable-app/outbound-cutover-cases.json",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  const rows = manifest.cases.filter(
    (entry) => entry.case_id === "EZ-6/acceptance/22",
  );
  assert.equal(rows.length, 5);
  const core = rows.find((entry) => entry.environment === "unit/core");
  const names = [
    "batch_collections_preserve_identity_until_consumed",
    "mixed_batch_collection_preserves_delivery_state",
    "batch_accumulator_rejects_foreign_or_missing_items",
    "batch_recovery_returns_exact_nonclone_inputs",
    "accumulator_rejects_duplicate_index_count_and_conflict_before_mutation",
  ].map((name) => `outbound::batch::tests::${name}`);
  assert.equal(core.implementation, "ready");
  assert.equal(core.applicability, "blocking");
  assert.equal(core.approval, null);
  assert.equal(core.condition, null);
  assert.equal(
    core.command,
    "scripts/run_test_nonzero.sh --inventory unit/core cargo test --offline --locked -p edgezero-core --features test-utils --lib outbound::",
  );
  assert.equal(core.artifact_path, "outbound/unit/core/raw.log");
  assert.deepEqual([core.sentinel, ...core.additional_sentinels], names);
  const selected = checker.readySentinels(manifest, "unit/core");
  assert.equal(selected.length, 80);
  for (const name of names) assert.ok(selected.includes(name));
  const bindings = [
    [
      "EZ-6/acceptance/15",
      [
        "outbound::batch::tests::arrival_ledger_preserves_late_collection_and_cancel_safety",
        "outbound::tests::batch_ledger_rejects_an_outcome_observed_at_observation_cutoff",
      ],
    ],
    [
      "EZ-6/acceptance/17",
      [
        "outbound::batch::tests::cancellation_includes_recorded_but_undelivered_indices",
      ],
    ],
    [
      "EZ-7/acceptance/01",
      [
        "outbound::tests::proxy_marker_off_preserves_upstream_and_consumes_no_header_budget",
      ],
    ],
    [
      "EZ-7/acceptance/03",
      [
        "outbound::tests::proxy_marker_off_preserves_upstream_and_consumes_no_header_budget",
      ],
    ],
  ];
  for (const [caseId, expected] of bindings) {
    const row = manifest.cases.find(
      (c) => c.case_id === caseId && c.environment === "unit/core",
    );
    assert.ok(row, caseId);
    assert.equal(row.implementation, "ready", caseId);
    assert.equal(row.applicability, "blocking", caseId);
    assert.equal(row.approval, null, caseId);
    assert.equal(row.condition, null, caseId);
    assert.equal(row.command, core.command, caseId);
    assert.equal(row.artifact_path, core.artifact_path, caseId);
    assert.deepEqual(
      [row.sentinel, ...(row.additional_sentinels ?? [])],
      expected,
      caseId,
    );
    for (const name of expected) assert.ok(selected.includes(name), name);
  }
  const source = readFileSync(
    new URL("../crates/edgezero-core/src/outbound/batch.rs", import.meta.url),
    "utf8",
  );
  for (const name of names) {
    assert.ok(source.includes(`fn ${name.split("::").at(-1)}()`), name);
  }
  for (const row of rows.filter((entry) => entry !== core)) {
    assert.equal(row.implementation, "planned", row.environment);
  }
  const workflow = readFileSync(
    new URL("../.github/workflows/test.yml", import.meta.url),
    "utf8",
  );
  assert.ok(workflow.includes(`run: >-\n          ${core.command}\n`));
});

test("core policy conformance binds complete proofs without promoting provider legs", () => {
  const manifest = JSON.parse(
    readFileSync(
      new URL(
        "../tests/fixtures/reusable-app/outbound-cutover-cases.json",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  const bindings = [
    [
      "EZ-4/acceptance/01",
      [
        "outbound::redirect_tests::connect_timeout_round_trip_and_follow_preserve_the_aggregate_budget",
      ],
    ],
    [
      "EZ-5/acceptance/01",
      [
        "outbound::tests::original_coding_retains_valid_compressed_bytes_and_representation_fields",
      ],
    ],
    [
      "EZ-5/acceptance/02",
      [
        "outbound::tests::original_coding_declared_caps_fail_before_polling",
        "outbound::tests::original_coding_observed_caps_and_source_errors_are_preserved",
      ],
    ],
    [
      "EZ-5/acceptance/03",
      [
        "outbound::tests::original_coding_skips_decoder_but_keeps_transport_caps",
      ],
    ],
    [
      "EZ-5/acceptance/10",
      [
        "outbound::tests::original_identity_retains_declared_and_observed_decoded_caps",
        "outbound::tests::original_coding_retains_valid_compressed_bytes_and_representation_fields",
        "outbound::tests::original_coding_declared_caps_fail_before_polling",
        "outbound::tests::original_coding_observed_caps_and_source_errors_are_preserved",
      ],
    ],
    [
      "EZ-6/acceptance/13",
      [
        "outbound::dispatch::tests::facade_upload_rules_preserve_forwarded_begin_and_streamed_send",
      ],
    ],
    [
      "EZ-7/acceptance/02",
      [
        "outbound::tests::client_marker_default_is_shared_by_send_begin_and_two_slot_batch",
      ],
    ],
  ];
  const command =
    "scripts/run_test_nonzero.sh --inventory unit/core cargo test --offline --locked -p edgezero-core --features test-utils --lib outbound::";
  const selected = checker.readySentinels(manifest, "unit/core");
  for (const [caseId, names] of bindings) {
    const rows = manifest.cases.filter((row) => row.case_id === caseId);
    const core = rows.find((row) => row.environment === "unit/core");
    assert.ok(core, caseId);
    assert.equal(core.implementation, "ready", caseId);
    assert.equal(core.applicability, "blocking", caseId);
    assert.equal(core.approval, null, caseId);
    assert.equal(core.condition, null, caseId);
    assert.equal(core.command, command, caseId);
    assert.equal(core.artifact_path, "outbound/unit/core/raw.log", caseId);
    assert.deepEqual(
      [core.sentinel, ...(core.additional_sentinels ?? [])],
      names,
      caseId,
    );
    for (const name of names) assert.ok(selected.includes(name), name);
    for (const row of rows.filter(
      (row) =>
        row !== core &&
        !(
          caseId === "EZ-5/acceptance/10" &&
          /^unit\/(axum|fastly|cloudflare|spin)$/.test(row.environment)
        ),
    )) {
      assert.equal(
        row.implementation,
        "planned",
        `${caseId}@${row.environment}`,
      );
    }
  }
});

test("forwarding ordering and normalization core proofs bind only their reviewed legs", () => {
  const manifest = JSON.parse(
    readFileSync(
      new URL(
        "../tests/fixtures/reusable-app/outbound-cutover-cases.json",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  assert.match(manifest.status, /450 other legs remain planned/);
  assert.match(manifest.status, /EZ-1 core acceptance 07/);
  assert.match(manifest.status, /EZ-3 core acceptance 04/);
  const bindings = [
    [
      "EZ-1/acceptance/07",
      [
        "outbound::dispatch::tests::forwarded_zero_length_chunks_dispatch_bodiless_send_and_begin",
        "outbound::dispatch::tests::forwarded_post_polls_only_after_transport_dispatch",
        "outbound::dispatch::tests::forwarded_expired_absolute_deadline_leaves_source_unread",
        "outbound::dispatch::tests::forwarded_buffered_and_taken_bodies_need_no_extra_reads",
        "outbound::dispatch::tests::failed_inbound_read_stops_forwarding_with_preserved_error",
      ],
    ],
    [
      "EZ-3/acceptance/04",
      [
        "outbound::tests::two_slot_batches_separate_completion_and_input_index_order",
      ],
    ],
    [
      "EZ-5/acceptance/11",
      [
        "outbound::head::tests::original_coding_preserves_normalization_precedence",
        "outbound::head::tests::head_opaque_fields_keep_passthrough_without_restoring_etag",
        "outbound::head::tests::head_surviving_malformed_length_fails_before_payload",
        "outbound::head::tests::head_finalization_retains_coding_choices_and_partial_response_rules",
        "outbound::head::tests::bodyless_and_declared_reset_bodies_bypass_payload_processing",
        "outbound::head::tests::head_undeclared_reset_body_settles_once_without_payload_caps",
      ],
    ],
  ];
  const selected = checker.readySentinels(manifest, "unit/core");
  for (const [caseId, names] of bindings) {
    const rows = manifest.cases.filter((row) => row.case_id === caseId);
    const core = rows.find((row) => row.environment === "unit/core");
    assert.ok(core, caseId);
    assert.equal(core.implementation, "ready", caseId);
    assert.equal(core.applicability, "blocking", caseId);
    assert.equal(core.approval, null, caseId);
    assert.equal(core.condition, null, caseId);
    assert.equal(
      core.command,
      "scripts/run_test_nonzero.sh --inventory unit/core cargo test --offline --locked -p edgezero-core --features test-utils --lib outbound::",
      caseId,
    );
    assert.equal(core.artifact_path, "outbound/unit/core/raw.log", caseId);
    assert.deepEqual(
      [core.sentinel, ...(core.additional_sentinels ?? [])],
      names,
      caseId,
    );
    for (const name of names) assert.ok(selected.includes(name), name);
    for (const row of rows.filter(
      (row) =>
        row !== core &&
        !(
          caseId === "EZ-5/acceptance/11" &&
          /^unit\/(axum|fastly|cloudflare|spin)$/.test(row.environment)
        ),
    )) {
      assert.equal(
        row.implementation,
        "planned",
        `${caseId}@${row.environment}`,
      );
    }
  }
  for (const caseId of ["EZ-5/acceptance/11b", "EZ-5/acceptance/12"]) {
    const rows = manifest.cases.filter((row) => row.case_id === caseId);
    assert.ok(rows.length > 0, caseId);
    for (const row of rows) {
      assert.equal(
        row.implementation,
        "planned",
        `${caseId}@${row.environment}`,
      );
      assert.equal(
        row.applicability,
        "conditional",
        `${caseId}@${row.environment}`,
      );
      assert.equal(
        manifest.decisions.find((d) => d.id === row.approval)?.status,
        "pending",
        `${caseId}@${row.environment}`,
      );
    }
  }
});

test("redirect runtime bindings retain exact output paths and leave unproved legs planned", () => {
  const manifest = JSON.parse(
    readFileSync(
      new URL(
        "../tests/fixtures/reusable-app/outbound-cutover-cases.json",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  const providers = [
    ["axum", "axum"],
    ["viceroy", "fastly"],
    ["workerd", "cloudflare"],
    ["spin", "spin"],
  ];
  const bound = [];
  for (const [environment, adapter] of providers) {
    const criteria = ["axum", "viceroy"].includes(environment)
      ? ["16", "17"]
      : ["16"];
    for (const criterion of criteria) {
      const c = manifest.cases.find(
        (entry) =>
          entry.case_id === `EZ-9/acceptance/${criterion}` &&
          entry.environment === environment,
      );
      assert.ok(c, `${criterion}@${environment}`);
      assert.equal(c.implementation, "ready");
      assert.equal(c.applicability, "blocking");
      assert.equal(c.approval, null);
      assert.equal(c.condition, null);
      assert.equal(c.sentinel, `${adapter}-wire-inventory`);
      assert.equal(
        c.command,
        `scripts/smoke_test_reusable_app.sh --adapter ${adapter} --suite outbound-wire --output tests/fixtures/reusable-app/.runs/outbound/redirect/${environment}`,
      );
      assert.equal(
        c.artifact_path,
        `outbound/redirect/${environment}/events.jsonl`,
      );
      bound.push(c);
    }
  }
  assert.equal(bound.length, 6);
  const pending = manifest.cases.filter((c) =>
    ["16b", "18", "19", "20"].some(
      (criterion) => c.case_id === `EZ-9/acceptance/${criterion}`,
    ),
  );
  assert.equal(pending.length, 8);
  for (const c of pending) {
    assert.equal(c.implementation, "planned", `${c.case_id}@${c.environment}`);
    if (c.case_id.endsWith("/16b")) {
      assert.equal(c.applicability, "conditional");
      assert.equal(c.condition, "EZ-14 present");
      assert.equal(c.approval, `EZ-14 present@${c.environment}`);
      assert.equal(
        manifest.decisions.find((d) => d.id === c.approval)?.status,
        "pending",
      );
    }
  }
  const ready = manifest.cases.filter((c) => c.implementation === "ready");
  assert.equal(ready.length, 61);
  assert.ok(
    ready.every(
      (c) =>
        c.case_id.startsWith("EZ-9/acceptance/") ||
        (c.environment === "unit/core" &&
          [
            "EZ-1/acceptance/07",
            "EZ-3/acceptance/04",
            "EZ-4/acceptance/01",
            "EZ-5/acceptance/01",
            "EZ-5/acceptance/02",
            "EZ-5/acceptance/03",
            "EZ-5/acceptance/10",
            "EZ-5/acceptance/11",
            "EZ-6/acceptance/13",
            "EZ-6/acceptance/15",
            "EZ-6/acceptance/17",
            "EZ-6/acceptance/22",
            "EZ-7/acceptance/01",
            "EZ-7/acceptance/02",
            "EZ-7/acceptance/03",
          ].includes(c.case_id)) ||
        (/^unit\/(axum|fastly|cloudflare|spin)$/.test(c.environment) &&
          ["09", "10", "11"].some(
            (n) => c.case_id === `EZ-5/acceptance/${n}`,
          )) ||
        (["axum", "axum/http1"].includes(c.environment) &&
          ["01", "04", "08"].some(
            (n) => c.case_id === `EZ-1/acceptance/${n}`,
          )) ||
        (c.case_id.startsWith("EZ-23/acceptance/") &&
          Number(c.case_id.split("/").at(-1)) <= 12),
    ),
  );
});
test("adapter policy bindings run real SDK suites and keep conditional metadata unapproved", () => {
  const manifest = JSON.parse(
    readFileSync(
      new URL(
        "../tests/fixtures/reusable-app/outbound-cutover-cases.json",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  const workflow = readFileSync(
    new URL("../.github/workflows/test.yml", import.meta.url),
    "utf8",
  );
  for (const adapter of ["axum", "cloudflare", "fastly", "spin"]) {
    for (const criterion of ["09", "10", "11"]) {
      const row = manifest.cases.find(
        (row) =>
          row.case_id === `EZ-5/acceptance/${criterion}` &&
          row.environment === `unit/${adapter}`,
      );
      if (adapter === "spin" && criterion !== "09") {
        assert.equal(row?.implementation, "planned", `${criterion}@${adapter}`);
        continue;
      }
      assert.equal(row?.implementation, "ready", `${criterion}@${adapter}`);
      assert.equal(row.applicability, "blocking");
      assert.equal(row.approval, null);
      assert.equal(row.condition, null);
      assert.equal(row.artifact_path, `outbound/unit/${adapter}/raw.log`);
      assert.ok(
        workflow.includes(row.command),
        `${criterion}@${adapter} CI execution`,
      );
      if (adapter === "spin") {
        assert.equal(row.sentinel, `ez_5_acceptance_${criterion}_spin`);
        assert.equal(
          row.command,
          "wasmtime run -W component-model-async=y -S p3=y -S http=y target/wasm32-wasip2/debug/examples/outbound_eager.wasm",
        );
        assert.ok(
          workflow.includes(
            "cargo build --offline --locked -p edgezero-adapter-spin --no-default-features --features spin,test-utils --target wasm32-wasip2 --example outbound-eager",
          ),
        );
      } else {
        assert.ok(
          row.sentinel.startsWith(
            `outbound::${adapter === "axum" ? "clock_tests" : adapter === "fastly" ? "fastly_impl::tests" : "worker_impl::policy_tests"}::`,
          ),
        );
        if (criterion !== "09") assert.ok(row.additional_sentinels.length > 0);
        if (adapter !== "axum") assert.match(row.command, /--target wasm32-/);
      }
    }
    for (const criterion of ["09b", "11b", "12"]) {
      const row = manifest.cases.find(
        (row) =>
          row.case_id === `EZ-5/acceptance/${criterion}` &&
          row.environment === `unit/${adapter}`,
      );
      assert.equal(row.implementation, "planned");
      assert.equal(row.applicability, "conditional");
      assert.equal(
        manifest.decisions.find((decision) => decision.id === row.approval)
          ?.status,
        "pending",
      );
    }
  }
  assert.match(workflow, /--sentinels unit\/spin/);
  assert.match(workflow, /\$sentinel: passed/);
  const spinStep = workflow
    .split("- name: Run Spin eager exchange component")[1]
    .split("- name:")[0];
  assert.match(spinStep, /shell: bash/);
  const guarded = checker.readySentinels(
    manifest,
    "axum/native-tcp,unit/axum,axum/http1",
  );
  for (const [criterion, environment] of [
    ["01", "axum/http1"],
    ["04", "axum"],
    ["08", "axum"],
  ]) {
    const row = manifest.cases.find(
      (row) =>
        row.case_id === `EZ-1/acceptance/${criterion}` &&
        row.environment === environment,
    );
    assert.equal(row.implementation, "ready");
    assert.ok(workflow.includes(row.command));
    for (const name of [row.sentinel, ...(row.additional_sentinels ?? [])])
      assert.ok(guarded.includes(name), name);
  }
});
test("rejects deletion of reviewed split cases or environment legs", () => {
  const path = new URL(
    "../tests/fixtures/reusable-app/outbound-cutover-cases.json",
    import.meta.url,
  );
  const original = JSON.parse(readFileSync(path, "utf8"));
  const spec = readFileSync(
    new URL(
      "../docs/superpowers/specs/2026-10-17-edgezero-pr275-requirements.md",
      import.meta.url,
    ),
    "utf8",
  );
  for (const remove of [
    (c) => c.case_id === "EZ-2/acceptance/14b",
    (c) => c.case_id === "EZ-1/acceptance/01" && c.environment === "spin/http1",
    (c) => c.case_id === "EZ-5/acceptance/09",
  ]) {
    const manifest = clone(original);
    manifest.cases = manifest.cases.filter((c) => !remove(c));
    assert.throws(() => validateManifest(manifest, spec), /leg set/);
  }
});
test("CLI help is usable without evidence and unknown arguments fail", () => {
  const script = fileURLToPath(
    new URL("./check_outbound_cutover_evidence.mjs", import.meta.url),
  );
  const help = spawnSync(process.execPath, [script, "--help"], {
    encoding: "utf8",
  });
  assert.equal(help.status, 0);
  assert.match(help.stdout, /--manifest/);
  assert.match(help.stdout, /--release/);
  assert.notEqual(spawnSync(process.execPath, [script, "--made-up"]).status, 0);
});

function assemblyFixture(t) {
  const artifactsRoot = mkdtempSync(join(tmpdir(), "edgezero-record-"));
  t.after(() => rmSync(artifactsRoot, { recursive: true, force: true }));
  const manifest = inventory();
  const input = evidence(manifest);
  const build = Buffer.from("abc");
  input.builds[0].build_id = digest(build);
  mkdirSync(join(artifactsRoot, "build"));
  writeFileSync(join(artifactsRoot, "build/app.bin"), build);
  for (const [index, result] of input.results.entries()) {
    result.build_id = digest(build);
    delete result.artifact_sha256;
    const path = join(artifactsRoot, result.artifact_path);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, JSON.stringify({ candidate_sha: sha, sample: index }));
  }
  const options = {
    candidateSha: sha,
    source,
    reviewedLegSetSha256: fixtureLegPolicy(source),
    artifactsRoot,
  };
  return { artifactsRoot, manifest, input, options };
}

test("assembly preserves counts, identities and raw hashes without qualification", (t) => {
  assert.equal(typeof checker.assembleRecord, "function");
  const { manifest, input, options, artifactsRoot } = assemblyFixture(t);
  input.results[0].status = "failed";
  input.results[0].samples = 3;
  input.results[0].assertions[0].passed = false;
  input.results[1].status = "pending";
  input.results[1].samples = 5;
  const original = clone(input);
  const assembled = checker.assembleRecord(manifest, input, options);
  assert.deepEqual(input, original);
  assert.equal(assembled.candidate_sha, sha);
  assert.equal(assembled.stage, "pre-freeze");
  assert.equal(assembled.qualified, false);
  assert.deepEqual(assembled.assembly, {
    observed_legs: 2,
    passed: 0,
    failed: 1,
    pending: 1,
    samples: 8,
  });
  for (const [index, result] of assembled.results.entries()) {
    assert.equal(result.status, input.results[index].status);
    assert.equal(result.samples, input.results[index].samples);
    assert.deepEqual(result.assertions, input.results[index].assertions);
    assert.equal(result.build_id, digest(Buffer.from("abc")));
    assert.equal(
      result.artifact_sha256,
      digest(readFileSync(join(artifactsRoot, result.artifact_path))),
    );
  }
  const report = checker.inspectRelease(manifest, assembled, options);
  assert.equal(report.status, "blocked");
  assert.equal(report.qualified, false);
  assert.ok(
    report.findings.some((finding) => /pre-freeze/.test(finding.message)),
  );
  assert.throws(() => release(manifest, assembled), /pre-freeze/);
});

test("assembly never promotes working-tree observations or fills missing results", (t) => {
  const { manifest, input, options } = assemblyFixture(t);
  input.source_state = "working-tree";
  input.qualification = false;
  input.results.pop();
  const assembled = checker.assembleRecord(manifest, input, options);
  assert.equal(assembled.source_state, "working-tree");
  assert.equal(assembled.qualification, false);
  assert.equal(assembled.results.length, 1);
  assert.ok(
    checker
      .inspectRelease(manifest, assembled, options)
      .findings.some((finding) => /missing result/.test(finding.message)),
  );
});

test("assembly rejects incomplete or mismatched candidate, build and case provenance", (t) => {
  const { manifest, input, options } = assemblyFixture(t);
  for (const change of [
    (record) => delete record.source_state,
    (record) => delete record.candidate_sha,
    (record) => (record.results[0].candidate_sha = "e".repeat(40)),
    (record) => (record.builds[0].candidate_sha = "e".repeat(40)),
    (record) => delete record.builds,
    (record) => (record.builds[0].build_id = "e".repeat(64)),
    (record) => (record.results[0].build_id = "e".repeat(64)),
    (record) => (record.results[0].source_digest = "e".repeat(64)),
    (record) => (record.results[0].environment = "unknown"),
    (record) => record.results.push(clone(record.results[0])),
    (record) => (record.results[0].artifact_sha256 = "e".repeat(64)),
    (record) => (record.results[0].status = "unverified"),
    (record) => (record.results[0].samples = -1),
  ]) {
    const record = clone(input);
    change(record);
    assert.throws(() => checker.assembleRecord(manifest, record, options));
  }
});

test("assembly rejects artifact traversal, directories and symlink escapes", (t) => {
  const { manifest, input, options, artifactsRoot } = assemblyFixture(t);
  for (const path of ["../outside.bin", "/outside.bin", "build"]) {
    const record = clone(input);
    record.builds[0].artifact_path = path;
    assert.throws(
      () => checker.assembleRecord(manifest, record, options),
      /artifact/,
    );
  }
  const outside = mkdtempSync(join(tmpdir(), "edgezero-outside-"));
  t.after(() => rmSync(outside, { recursive: true, force: true }));
  writeFileSync(join(outside, "raw.json"), "not evidence");
  const path = join(artifactsRoot, input.results[0].artifact_path);
  rmSync(path);
  symlinkSync(join(outside, "raw.json"), path);
  assert.throws(
    () => checker.assembleRecord(manifest, input, options),
    /escapes/,
  );
});

test("assembly rejects a FIFO before opening it", (t) => {
  const { manifest, input, options, artifactsRoot } = assemblyFixture(t);
  const fifo = join(artifactsRoot, "build/pending");
  assert.equal(spawnSync("mkfifo", [fifo]).status, 0);
  input.builds[0].artifact_path = "build/pending";
  const script = fileURLToPath(
    new URL("./check_outbound_cutover_evidence.mjs", import.meta.url),
  );
  const child = spawnSync(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `import {assembleRecord} from ${JSON.stringify(script)};
     const {manifest,input,options}=JSON.parse(process.argv[1]);
     assembleRecord(manifest,input,options);`,
      JSON.stringify({ manifest, input, options }),
    ],
    { encoding: "utf8", timeout: 2000, killSignal: "SIGKILL" },
  );
  assert.equal(
    child.error,
    undefined,
    "non-regular artifact blocked the exporter",
  );
  assert.equal(child.status, 1);
  assert.match(child.stderr, /regular file/);
});

test("assembly hashes multiple read buffers and rejects oversized artifacts before reading", (t) => {
  const { manifest, input, options, artifactsRoot } = assemblyFixture(t);
  const path = join(artifactsRoot, input.results[0].artifact_path);
  const bytes = Buffer.alloc(128 * 1024 + 7, 0x5a);
  writeFileSync(path, bytes);
  assert.equal(
    checker.assembleRecord(manifest, input, options).results[0].artifact_sha256,
    digest(bytes),
  );
  truncateSync(path, 256 * 1024 * 1024 + 1);
  assert.throws(
    () => checker.assembleRecord(manifest, input, options),
    /256 MiB/,
  );
});

test("report and release both verify build and raw artifact hashes", (t) => {
  const { manifest, input, options, artifactsRoot } = assemblyFixture(t);
  const record = checker.assembleRecord(manifest, input, options);
  record.stage = "frozen-candidate";
  assert.equal(
    checker.validateRelease(manifest, record, options).status,
    "passed",
  );
  writeFileSync(
    join(artifactsRoot, record.results[0].artifact_path),
    "changed capture",
  );
  assert.equal(checker.inspectRelease(manifest, record, options).passed, 1);
  writeFileSync(join(artifactsRoot, "build/app.bin"), "changed build");
  const report = checker.inspectRelease(manifest, record, options);
  assert.equal(report.status, "blocked");
  assert.equal(
    report.passed,
    0,
    "corrupt build/artifact evidence cannot count as accepted",
  );
  for (const pattern of [
    /build artifact hash mismatch/,
    /raw artifact hash mismatch/,
  ]) {
    assert.ok(report.findings.some((finding) => pattern.test(finding.message)));
  }
  assert.throws(
    () => checker.validateRelease(manifest, record, options),
    /hash mismatch/,
  );
});

test("report lists independent blockers using the same release rules", () => {
  assert.equal(typeof checker.inspectRelease, "function");
  const manifest = inventory();
  const record = evidence(manifest);
  record.toolchain.rust = "wrong";
  record.results[0].samples = 0;
  record.results.pop();
  const report = checker.inspectRelease(manifest, record, {
    candidateSha: sha,
    source,
    reviewedLegSetSha256: fixtureLegPolicy(source),
  });
  assert.equal(report.status, "blocked");
  for (const pattern of [
    /wrong toolchain rust/,
    /zero samples/,
    /missing result/,
  ]) {
    assert.ok(
      report.findings.some((finding) => pattern.test(finding.message)),
      String(pattern),
    );
  }
  assert.throws(() => release(manifest, record), /wrong toolchain rust/);
  record.results = null;
  assert.doesNotThrow(() =>
    checker.inspectRelease(manifest, record, {
      candidateSha: sha,
      source,
      reviewedLegSetSha256: fixtureLegPolicy(source),
    }),
  );
});

test("report rejects invalid deferral totals and exposes planned commands without results", () => {
  const followup = source.replaceAll("EZ-1", "EZ-10");
  const manifest = inventory(followup);
  manifest.cases[1].applicability = "follow-up";
  const record = evidence(manifest);
  const options = {
    candidateSha: sha,
    source: followup,
    reviewedLegSetSha256: fixtureLegPolicy(followup),
  };
  assert.equal(checker.inspectRelease(manifest, record, options).deferred, 0);
  manifest.cases[0].implementation = "planned";
  record.results = [];
  const report = checker.inspectRelease(manifest, record, options);
  for (const rule of ["implementation", "result"]) {
    assert.ok(
      report.findings.some(
        (finding) =>
          finding.scope === "EZ-10/acceptance/01@axum" && finding.rule === rule,
      ),
    );
  }
});

test("a duplicate catalog entry does not erase unrelated evidence", () => {
  const manifest = inventory();
  const options = {
    candidateSha: sha,
    source,
    reviewedLegSetSha256: fixtureLegPolicy(source),
  };
  for (const field of ["results", "applicability"]) {
    const record = evidence(manifest);
    record[field].push(clone(record[field][0]));
    record.results[1].candidate_sha = "e".repeat(40);
    const report = checker.inspectRelease(manifest, record, options);
    assert.ok(
      report.findings.some((finding) => /duplicate/.test(finding.message)),
    );
    assert.ok(
      report.findings.some(
        (finding) =>
          finding.scope === "EZ-1/acceptance/01@workerd" &&
          /revision mismatch/.test(finding.message),
      ),
    );
    assert.equal(report.passed, 0);
  }
  const record = evidence(manifest);
  record.builds.push({
    build_id: "e".repeat(64),
    candidate_sha: "e".repeat(40),
    artifact_path: "bad.bin",
  });
  const report = checker.inspectRelease(manifest, record, options);
  assert.ok(
    report.findings.some((finding) =>
      /build identity or candidate mismatch/.test(finding.message),
    ),
  );
  assert.ok(
    !report.findings.some(
      (finding) =>
        finding.scope === "EZ-1/acceptance/01@axum" && finding.rule === "build",
    ),
  );
});

test("release rejects draft, dirty or unbound build identities", () => {
  for (const change of [
    (record) => (record.stage = "pre-freeze"),
    (record) => (record.source_state = "working-tree"),
    (record) => (record.qualification = false),
    (record) => delete record.builds,
    (record) => (record.results[0].build_id = "e".repeat(64)),
  ]) {
    const record = evidence();
    change(record);
    assert.throws(() => release(inventory(), record));
  }
});

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
    // Criterion 10 also exercises the separate default-factory failure seam.
    const filter = criterion === "10" ? name : sentinel;
    const command = `scripts/run_test_nonzero.sh --include-ignored ${sentinel} cargo test --offline --locked -p edgezero-adapter-axum --no-default-features --features axum,test-utils --lib ${filter}`;
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
    assert.ok(
      workflow.includes(`\n          ${command}\n`),
      `${criterion} CI command`,
    );
  }
  for (const criterion of ["12", "13"]) {
    const c = cases.find(
      (entry) => entry.case_id === `EZ-23/acceptance/${criterion}`,
    );
    assert.equal(c.implementation, "planned", criterion);
    assert.equal(c.approval, null, criterion);
  }
  const factory =
    "service::tests::state_client_skips_real_transport_initialization_factory";
  const factoryCommand = `scripts/run_test_nonzero.sh --include-ignored ${factory} cargo test --offline --locked -p edgezero-adapter-axum --no-default-features --features axum,test-utils --lib ${factory}`;
  assert.ok(
    workflow.includes(`\n          ${factoryCommand}\n`),
    "factory CI guard",
  );
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

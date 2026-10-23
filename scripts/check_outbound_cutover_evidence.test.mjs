import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";
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
    candidate_sha: sha,
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

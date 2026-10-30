#!/usr/bin/env node

import { createHash } from "node:crypto";
import {
  closeSync,
  fstatSync,
  openSync,
  readFileSync,
  readSync,
  realpathSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { dirname, isAbsolute, relative, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const SPEC = "docs/superpowers/specs/2026-10-17-edgezero-pr275-requirements.md";
const INVENTORY =
  "crates/edgezero-test-harness/acceptance/outbound-cutover-cases.json";
// Updating the reviewed environment/split set requires a separate policy review.
const REVIEWED_LEG_SET_SHA256 =
  "5ad5ad13cb3a0ea9d96307f1ec150649a21357490b4b9b26ae59027e0e539ec9";
const sha256 = (value) => createHash("sha256").update(value).digest("hex");
const hex = (value, length) =>
  typeof value === "string" && new RegExp(`^[a-f0-9]{${length}}$`).test(value);
const nonempty = (value) =>
  typeof value === "string" && value.trim().length > 0;
const key = (value) => `${value.case_id}@${value.environment}`;
function requireFact(value, message) {
  if (!value) throw new Error(message);
}
function unique(values, describe) {
  const map = new Map();
  for (const value of values) {
    const id = describe(value);
    requireFact(!map.has(id), `duplicate ${id}`);
    map.set(id, value);
  }
  return map;
}

// IDs follow acceptance order, including the unnumbered EZ-8/EZ-9 lists.
// Hash the whole criterion, not a shortened label or its first line.
export function acceptanceCriteria(source) {
  const criteria = [];
  for (const section of source.split(/(?=^### EZ-)/m)) {
    const name = section.match(/^### (EZ-(?:\d+|Q1))\./)?.[1];
    const acceptance = section
      .split(/^#### Acceptance criteria\s*$/m)[1]
      ?.split(/^#{2,4} /m)[0];
    if (!name || !acceptance) continue;
    const [listed, endToEnd] = acceptance.split(
      "\n\nThe Trusted Server end-to-end check ",
    );
    const entries = [
      ...listed.matchAll(/^(?:\d+\. |- )(.*(?:\n(?!\d+\. |- ).*)*)/gm),
    ];
    for (const [index, match] of entries.entries()) {
      const text = match[1].trim();
      criteria.push({
        section: name,
        case_id: `${name}/acceptance/${String(index + 1).padStart(2, "0")}`,
        text,
        source_digest: sha256(text),
      });
    }
    if (endToEnd) {
      const text = `The Trusted Server end-to-end check ${endToEnd.trim()}`;
      criteria.push({
        section: name,
        case_id: `${name}/acceptance/ts-e2e`,
        text,
        source_digest: sha256(text),
      });
    }
  }
  requireFact(criteria.length > 0, "source has no acceptance criteria");
  return criteria;
}

function validateApproval(decision) {
  requireFact(
    ["pending", "approved", "rejected"].includes(decision.status),
    "invalid approval status",
  );
  requireFact(
    Array.isArray(decision.scope) &&
      decision.scope.length > 0 &&
      new Set(decision.scope).size === decision.scope.length,
    "approval needs explicit leg scope",
  );
  if (decision.status !== "approved") return;
  requireFact(
    ["promote", "present", "defer", "requirement-change"].includes(
      decision.action,
    ) &&
      nonempty(decision.edgezero_approver) &&
      nonempty(decision.consumer_approver) &&
      /^\d{4}-\d{2}-\d{2}$/.test(decision.date) &&
      nonempty(decision.evidence) &&
      nonempty(decision.owner) &&
      nonempty(decision.ci_owner),
    "approval needs attributed decision, date, owners and evidence",
  );
  if (decision.action === "present")
    requireFact(
      nonempty(decision.changelog),
      "present approval needs CHANGELOG confirmation",
    );
}

function baselineApplicability(c) {
  const [section, , number] = c.source_id.split("/");
  if (section === "EZ-Q1") return "gate";
  if (section === "EZ-2" && Number(number) >= 16 && Number(number) <= 21)
    return "follow-up";
  if (section === "EZ-6" && number === "10") return "follow-up";
  if (
    [
      "EZ-5/acceptance/12",
      "EZ-2/acceptance/10b",
      "EZ-2/acceptance/13b",
      "EZ-5/acceptance/09b",
      "EZ-5/acceptance/11b",
      "EZ-6/acceptance/23b",
      "EZ-8/acceptance/06b",
      "EZ-9/acceptance/16b",
    ].includes(c.case_id) ||
    (c.source_id === "EZ-1/acceptance/02" && c.environment.endsWith("/http3"))
  )
    return "conditional";
  return Number(section.slice(3)) <= 9 ? "blocking" : "follow-up";
}

export function validateManifest(
  manifest,
  source,
  reviewedLegSetSha256 = REVIEWED_LEG_SET_SHA256,
) {
  requireFact(
    manifest?.schema === 1 &&
      Array.isArray(manifest.cases) &&
      manifest.cases.length > 0 &&
      Array.isArray(manifest.decisions),
    "invalid or empty inventory",
  );
  requireFact(
    manifest.source_digest === sha256(source),
    "source document changed; review acceptance preambles and inventory digests",
  );
  const criteria = unique(acceptanceCriteria(source), (c) => c.case_id);
  const requirements = new Set(
    [...source.matchAll(/\*\*(EZ-(?:\d+|Q1)\.\d+)\*\*/g)].map((m) => m[1]),
  );
  const decisions = unique(manifest.decisions, (d) => d.id);
  for (const decision of decisions.values()) validateApproval(decision);
  const cases = unique(manifest.cases, key);
  requireFact(
    sha256(JSON.stringify([...cases.keys()].sort())) === reviewedLegSetSha256,
    "reviewed leg set changed; split/environment policy review required",
  );
  const covered = new Set();
  for (const [id, c] of cases) {
    const supplemental =
      c.source_kind === "normative" &&
      c.source_id === "EZ-24.12" &&
      c.case_id === "EZ-24/requirements/12";
    const normativeText = supplemental
      ? source
          .split("\n")
          .find((line) => line.startsWith("- **EZ-24.12** "))
          ?.slice("- **EZ-24.12** ".length)
      : null;
    const origin =
      supplemental && normativeText
        ? { text: normativeText, source_digest: sha256(normativeText) }
        : criteria.get(c.source_id);
    requireFact(
      origin &&
        (supplemental ||
          c.case_id === c.source_id ||
          c.case_id.replace(/[a-z]$/, "") === c.source_id),
      `${id}: unknown source criterion`,
    );
    requireFact(
      c.source_digest === origin.source_digest,
      `${id}: source digest changed; review required`,
    );
    requireFact(
      c.expected === origin.text,
      `${id}: expected assertion differs from source`,
    );
    requireFact(
      Array.isArray(c.requirement_ids) &&
        c.requirement_ids.length > 0 &&
        c.requirement_ids.every((r) => requirements.has(r)),
      `${id}: unknown requirement IDs`,
    );
    requireFact(
      ["blocking", "follow-up", "conditional", "gate"].includes(
        c.applicability,
      ),
      `${id}: invalid applicability`,
    );
    const baseline = baselineApplicability(c);
    requireFact(
      !["blocking", "gate"].includes(baseline) || c.applicability === baseline,
      `${id}: cannot downgrade blocking applicability`,
    );
    requireFact(
      ["planned", "ready"].includes(c.implementation),
      `${id}: invalid implementation state`,
    );
    for (const field of [
      "environment",
      "owner",
      "task",
      "substep",
      "sentinel",
      "command",
      "artifact_path",
    ])
      requireFact(nonempty(c[field]), `${id}: missing ${field}`);
    if (c.additional_sentinels !== undefined)
      requireFact(
        Array.isArray(c.additional_sentinels) &&
          c.additional_sentinels.length > 0 &&
          c.additional_sentinels.every(
            (s) =>
              typeof s === "string" &&
              /^[A-Za-z_]\w*(?:::[A-Za-z_]\w*)*$/.test(s),
          ) &&
          new Set([c.sentinel, ...c.additional_sentinels]).size ===
            c.additional_sentinels.length + 1,
        `${id}: invalid additional_sentinels`,
      );
    if (c.companion_case !== undefined) {
      const companion = cases.get(c.companion_case);
      requireFact(
        nonempty(c.companion_case) &&
          c.companion_case !== id &&
          companion?.implementation === "ready" &&
          ["blocking", "gate"].includes(companion?.applicability) &&
          companion.companion_case === undefined,
        `${id}: companion must be a distinct ready mandatory proof without another companion`,
      );
    }
    if (
      c.case_id === "EZ-6/acceptance/22" &&
      c.implementation === "ready" &&
      ["axum", "viceroy", "workerd", "spin"].includes(c.environment)
    )
      requireFact(
        c.companion_case === "EZ-6/acceptance/22@unit/core",
        `${id}: private identity companion required`,
      );
    requireFact(
      !isAbsolute(c.artifact_path) &&
        !c.artifact_path.split(/[\\/]/).includes(".."),
      `${id}: unsafe artifact path`,
    );
    if (c.applicability === "conditional")
      requireFact(nonempty(c.condition), `${id}: explicit condition required`);
    if (c.approval !== null)
      requireFact(
        decisions.get(c.approval)?.scope.includes(id),
        `${id}: missing or incorrectly scoped approval`,
      );
    covered.add(c.source_id);
  }
  for (const id of criteria.keys())
    requireFact(covered.has(id), `missing criterion ${id}`);
  for (const decision of decisions.values())
    for (const scope of decision.scope)
      requireFact(cases.has(scope), `approval references unknown leg ${scope}`);
  return {
    criteria: criteria.size,
    legs: cases.size,
    planned: manifest.cases.filter((c) => c.implementation === "planned")
      .length,
  };
}

function disposition(c, decisions) {
  const decision = decisions.get(c.approval);
  if (decision?.status === "approved") {
    if (["promote", "present"].includes(decision.action)) return "required";
    // Missing implementation, entitlement or runtime never waives a blocking leg.
    requireFact(
      !["blocking", "gate"].includes(c.applicability) ||
        decision.action === "requirement-change",
      `${key(c)}: blocking applicability requires an approved requirement change`,
    );
    return "deferred";
  }
  return ["blocking", "gate"].includes(c.applicability)
    ? "required"
    : "deferred";
}

function validateMeasurement(measurement) {
  requireFact(
    nonempty(measurement.name) &&
      Number.isSafeInteger(measurement.samples) &&
      measurement.samples > 0 &&
      measurement.passed === true,
    "failed measurement or zero measurement samples",
  );
  requireFact(
    Number.isFinite(measurement.value),
    "measurement needs a finite value",
  );
  if (measurement.rule === "record") return;
  requireFact(
    ["le", "lt", "ge", "eq"].includes(measurement.rule),
    "unsupported measurement rule",
  );
  requireFact(
    Number.isFinite(measurement.bound),
    "measurement needs a finite bound",
  );
  const pass = {
    le: () => measurement.value <= measurement.bound,
    lt: () => measurement.value < measurement.bound,
    ge: () => measurement.value >= measurement.bound,
    eq: () => measurement.value === measurement.bound,
  }[measurement.rule];
  requireFact(pass && pass(), "failed measurement bound");
}

function validateBuildIdentity(build, candidateSha) {
  requireFact(
    hex(build?.build_id, 64) && build.candidate_sha === candidateSha,
    "build identity or candidate mismatch",
  );
  requireFact(nonempty(build.artifact_path), "build artifact path required");
}

function buildIdentities(record, candidateSha) {
  requireFact(
    Array.isArray(record?.builds) && record.builds.length > 0,
    "build identities required",
  );
  const builds = unique(record.builds, (build) => build?.build_id);
  for (const build of builds.values()) {
    validateBuildIdentity(build, candidateSha);
  }
  return builds;
}

function artifactHash(root, name) {
  requireFact(
    nonempty(name) && !isAbsolute(name) && !name.split(/[\\/]/u).includes(".."),
    "unsafe artifact path",
  );
  const path = realpathSync(resolve(root, name));
  const within = relative(root, path);
  requireFact(
    within !== "" && !isAbsolute(within) && within.split(/[\\/]/u)[0] !== "..",
    "artifact escapes evidence directory",
  );
  const limit = 256 * 1024 * 1024;
  const before = statSync(path);
  requireFact(
    before.isFile() && before.size <= limit,
    "artifact must be a regular file at most 256 MiB",
  );
  const file = openSync(path, "r");
  try {
    const metadata = fstatSync(file);
    requireFact(
      metadata.isFile() && metadata.size <= limit,
      "artifact must be a regular file at most 256 MiB",
    );
    const hash = createHash("sha256");
    const buffer = Buffer.alloc(64 * 1024);
    let total = 0;
    for (;;) {
      const bytes = readSync(file, buffer);
      if (bytes === 0) break;
      total += bytes;
      requireFact(total <= limit, "artifact exceeds 256 MiB while reading");
      hash.update(buffer.subarray(0, bytes));
    }
    requireFact(total === metadata.size, "artifact changed while reading");
    return hash.digest("hex");
  } finally {
    closeSync(file);
  }
}

export function assembleRecord(
  manifest,
  input,
  { candidateSha, source, reviewedLegSetSha256, artifactsRoot },
) {
  validateManifest(manifest, source, reviewedLegSetSha256);
  requireFact(
    hex(candidateSha, 40) &&
      input?.schema === 1 &&
      input.candidate_sha === candidateSha,
    "candidate revision mismatch",
  );
  requireFact(
    ["clean", "working-tree"].includes(input.source_state),
    "explicit source provenance required",
  );
  const root = realpathSync(resolve(artifactsRoot));
  const builds = buildIdentities(input, candidateSha);
  for (const build of builds.values()) {
    requireFact(
      artifactHash(root, build.artifact_path) === build.build_id,
      "build artifact hash mismatch",
    );
  }
  requireFact(
    Array.isArray(input.results) &&
      input.results.length > 0 &&
      input.results.length <= manifest.cases.length,
    "nonzero bounded observation list required",
  );
  const cases = unique(manifest.cases, key);
  const results = unique(input.results, key);
  const assembly = {
    observed_legs: results.size,
    passed: 0,
    failed: 0,
    pending: 0,
    samples: 0,
  };
  const output = structuredClone(input);
  output.results = [...results.values()].map((result) => {
    const id = key(result);
    const c = cases.get(id);
    requireFact(c, `unknown case/environment ${id}`);
    requireFact(
      result.candidate_sha === candidateSha && builds.has(result.build_id),
      `${id}: observation candidate/build mismatch`,
    );
    requireFact(
      result.source_digest === c.source_digest &&
        result.command === c.command &&
        result.sentinel === c.sentinel &&
        result.artifact_path === c.artifact_path,
      `${id}: result contract mismatch`,
    );
    requireFact(
      ["passed", "failed", "pending"].includes(result.status) &&
        Number.isSafeInteger(result.samples) &&
        result.samples >= 0,
      `${id}: invalid observation status or sample count`,
    );
    requireFact(
      Array.isArray(result.assertions) && Array.isArray(result.measurements),
      `${id}: assertion and measurement lists required`,
    );
    const hash = artifactHash(root, result.artifact_path);
    requireFact(
      result.artifact_sha256 === undefined || result.artifact_sha256 === hash,
      `${id}: raw artifact hash mismatch`,
    );
    assembly[result.status] += 1;
    assembly.samples += result.samples;
    requireFact(
      Number.isSafeInteger(assembly.samples),
      "aggregate sample count overflow",
    );
    return { ...structuredClone(result), artifact_sha256: hash };
  });
  output.stage = "pre-freeze";
  output.qualified = false;
  output.assembly = assembly;
  return output;
}

export function inspectRelease(
  manifest,
  record,
  { candidateSha, source, reviewedLegSetSha256, artifactsRoot },
) {
  validateManifest(manifest, source, reviewedLegSetSha256);
  const findings = [];
  const check = (scope, rule, validate) => {
    try {
      validate();
      return true;
    } catch (error) {
      findings.push({ scope, rule, message: error.message });
      return false;
    }
  };
  check("record", "candidate", () =>
    requireFact(
      hex(candidateSha, 40) &&
        record?.schema === 1 &&
        record.candidate_sha === candidateSha,
      "candidate revision mismatch",
    ),
  );
  check("record", "stage", () =>
    requireFact(
      record?.stage === "frozen-candidate",
      "pre-freeze or missing-stage evidence cannot qualify a release",
    ),
  );
  check("record", "source", () =>
    requireFact(
      record?.source_state === "clean" && record?.qualification !== false,
      "working-tree characterization cannot qualify a release",
    ),
  );
  check("record", "runner", () =>
    requireFact(nonempty(record?.runner_image), "runner image required"),
  );
  const versions = {
    rust: "1.98.1",
    fastly_sdk: "0.13.1",
    viceroy: "0.21.0",
    spin: "4.1.0",
  };
  for (const [tool, version] of Object.entries(versions))
    check("record", `toolchain:${tool}`, () =>
      requireFact(
        record?.toolchain?.[tool] === version,
        `wrong toolchain ${tool}`,
      ),
    );
  check("record", "toolchain:workerd", () =>
    requireFact(
      nonempty(record?.toolchain?.workerd),
      "workerd revision required",
    ),
  );
  const retention = record?.retention;
  check("record", "retention", () =>
    requireFact(
      ["release-asset", "repository"].includes(retention?.kind) &&
        nonempty(retention.uri) &&
        retention.expires_at === null,
      "durable non-expiring retention required while the candidate is pinned",
    ),
  );
  if (record?.comparisons !== null)
    check("record", "comparisons", () =>
      requireFact(
        nonempty(record?.comparisons?.baseline) &&
          record?.comparisons?.candidate === candidateSha,
        "comparison build identities required",
      ),
    );
  const map = (field, describe) => {
    const values = new Map();
    const ambiguous = new Set();
    if (
      !check("record", field, () =>
        requireFact(Array.isArray(record?.[field]), `${field} map required`),
      )
    )
      return values;
    for (const [index, value] of record[field].entries()) {
      let id;
      if (
        !check("record", `${field}:${index}`, () => {
          requireFact(
            value && typeof value === "object" && !Array.isArray(value),
            `invalid ${field} entry`,
          );
          id = describe(value);
          requireFact(nonempty(id), `invalid ${field} identity`);
        })
      )
        continue;
      if (values.has(id) || ambiguous.has(id)) {
        check(field === "builds" ? "record" : id, `${field}:duplicate`, () =>
          requireFact(false, `duplicate ${id}`),
        );
        values.delete(id);
        ambiguous.add(id);
      } else values.set(id, value);
    }
    return values;
  };
  const legIdentity = (value) =>
    nonempty(value.case_id) && nonempty(value.environment)
      ? key(value)
      : undefined;
  const results = map("results", legIdentity);
  const applicability = map("applicability", legIdentity);
  const cases = unique(manifest.cases, key);
  const decisions = unique(manifest.decisions, (d) => d.id);
  const builds = map("builds", (build) => build.build_id);
  check("record", "builds:nonempty", () =>
    requireFact(builds.size > 0, "build identities required"),
  );
  for (const [id, build] of builds) {
    if (
      !check("record", `build:${id}`, () =>
        validateBuildIdentity(build, candidateSha),
      )
    )
      builds.delete(id);
  }
  let root;
  if (artifactsRoot !== undefined) {
    check("record", "artifacts", () => {
      root = realpathSync(resolve(artifactsRoot));
    });
    for (const build of builds.values()) {
      check("record", `build:${build.build_id}`, () =>
        requireFact(
          root && artifactHash(root, build.artifact_path) === build.build_id,
          "build artifact hash mismatch",
        ),
      );
    }
  }
  for (const id of [...results.keys(), ...applicability.keys()])
    check(id, "known-leg", () =>
      requireFact(cases.has(id), `unknown case/environment ${id}`),
    );
  const passed = [];
  const deferred = [];
  for (const [id, c] of cases) {
    const start = findings.length;
    let required;
    if (
      !check(id, "disposition", () => {
        required = disposition(c, decisions);
      })
    )
      continue;
    const mapping = applicability.get(id);
    check(id, "applicability", () =>
      requireFact(
        mapping?.disposition === required,
        `${id}: wrong or missing applicability`,
      ),
    );
    if (required === "deferred") {
      const approved = decisions.get(c.approval)?.status === "approved";
      check(id, "deferral", () =>
        requireFact(
          approved
            ? mapping?.approval === c.approval
            : mapping?.condition ===
                (c.condition ?? "follow-up not promoted or confirmed present"),
          `${id}: unjustified applicability deferral`,
        ),
      );
      if (start === findings.length) deferred.push(id);
      continue;
    }
    check(id, "implementation", () =>
      requireFact(
        c.implementation === "ready",
        `${id}: planned command is not implemented evidence`,
      ),
    );
    if (c.comparison === true)
      check(id, "comparison", () =>
        requireFact(
          record?.comparisons !== null,
          `${id}: baseline comparison required`,
        ),
      );
    const result = results.get(id);
    if (
      !check(id, "result", () => requireFact(result, `${id}: missing result`))
    )
      continue;
    if (c.comparison === true)
      check(id, "comparison-measurements", () =>
        requireFact(
          Array.isArray(result.measurements) && result.measurements.length > 0,
          `${id}: comparison measurements required`,
        ),
      );
    check(id, "candidate", () =>
      requireFact(
        result.candidate_sha === candidateSha,
        `${id}: result revision mismatch`,
      ),
    );
    check(id, "build", () =>
      requireFact(
        builds.has(result.build_id),
        `${id}: missing or unknown result build identity`,
      ),
    );
    check(id, "contract", () =>
      requireFact(
        result.source_digest === c.source_digest &&
          result.command === c.command &&
          result.sentinel === c.sentinel &&
          result.artifact_path === c.artifact_path,
        `${id}: result contract mismatch`,
      ),
    );
    check(id, "outcome", () =>
      requireFact(
        result.status === "passed" &&
          Number.isSafeInteger(result.samples) &&
          result.samples > 0,
        `${id}: pending/failed result or zero samples`,
      ),
    );
    check(id, "artifact-digest", () =>
      requireFact(
        hex(result.artifact_sha256, 64),
        `${id}: raw artifact hash required`,
      ),
    );
    check(id, "assertions", () =>
      requireFact(
        Array.isArray(result.assertions) &&
          result.assertions.length > 0 &&
          result.assertions.every((a) => nonempty(a.name) && a.passed === true),
        `${id}: missing or failed assertions`,
      ),
    );
    if (
      check(id, "measurements", () =>
        requireFact(
          Array.isArray(result.measurements),
          `${id}: measurement list required`,
        ),
      )
    ) {
      for (const [index, measurement] of result.measurements.entries())
        check(id, `measurement:${index}`, () =>
          validateMeasurement(measurement),
        );
    }
    if (start === findings.length) passed.push(id);
  }
  if (artifactsRoot !== undefined) {
    for (const [id, result] of results) {
      check(id, "raw-artifact", () =>
        requireFact(
          root &&
            artifactHash(root, result.artifact_path) === result.artifact_sha256,
          "raw artifact hash mismatch",
        ),
      );
    }
  }
  const failedProofs = new Set(findings.map((finding) => finding.scope));
  for (const [id, c] of cases) {
    if (c.companion_case !== undefined && passed.includes(id))
      check(id, "companion", () =>
        requireFact(
          passed.includes(c.companion_case) &&
            !failedProofs.has(c.companion_case),
          `${id}: same-candidate companion did not pass: ${c.companion_case}`,
        ),
      );
  }
  const invalid = new Set(findings.map((finding) => finding.scope));
  const accepted = (ids) =>
    invalid.has("record") ? 0 : ids.filter((id) => !invalid.has(id)).length;
  return {
    status: findings.length === 0 ? "passed" : "blocked",
    candidate_sha: candidateSha,
    passed: accepted(passed),
    deferred: accepted(deferred),
    capabilities_changed: false,
    qualified: false,
    findings,
  };
}

export function validateRelease(manifest, record, options) {
  const report = inspectRelease(manifest, record, options);
  requireFact(report.findings.length === 0, report.findings[0]?.message);
  return report;
}

function readJson(path) {
  const metadata = statSync(path);
  requireFact(
    metadata.isFile() && metadata.size <= 16 * 1024 * 1024,
    "JSON input must be a regular file at most 16 MiB",
  );
  return JSON.parse(readFileSync(path, "utf8"));
}

export function readySentinels(manifest, environment, command) {
  requireFact(
    command === undefined || nonempty(command),
    "sentinel command must be nonempty",
  );
  const environments = environment.split(",");
  requireFact(
    environments.every(Boolean) &&
      new Set(environments).size === environments.length,
    "invalid or duplicate sentinel environment",
  );
  const names = environments.flatMap((selected) => {
    const names = manifest.cases
      .filter(
        (c) =>
          c.environment === selected &&
          c.implementation === "ready" &&
          (command === undefined || c.command === command),
      )
      .flatMap((c) => [c.sentinel, ...(c.additional_sentinels ?? [])]);
    requireFact(
      names.length > 0,
      "no ready test sentinels for selected environment",
    );
    return names;
  });
  requireFact(
    names.every((s) => /^[A-Za-z_]\w*(?:::[A-Za-z_]\w*)*$/.test(s)),
    "ready test sentinels must be Rust test names",
  );
  return [...new Set(names)];
}

function run(args) {
  if (args.length === 1 && args[0] === "--help") {
    process.stdout.write(
      "Usage: node scripts/check_outbound_cutover_evidence.mjs --manifest [INVENTORY.json]\n       node scripts/check_outbound_cutover_evidence.mjs --sentinels ENVIRONMENT [--command EXACT_COMMAND] [--inventory INVENTORY.json]\n       node scripts/check_outbound_cutover_evidence.mjs --assemble --candidate SHA --input OBSERVATIONS.json --artifacts DIRECTORY --output NEW_DRAFT.json [--inventory INVENTORY.json]\n       node scripts/check_outbound_cutover_evidence.mjs --report|--release --candidate SHA --record RECORD.json --artifacts DIRECTORY [--inventory INVENTORY.json]\nAssembly preserves observations in an unqualified pre-freeze draft and exits nonzero for open release gates. It refuses overwrites.\nManifest validation and sentinel listing permit planned/pending legs; neither qualifies a release.\nApprovals and raw measurements require independent review; this tool does not authenticate telemetry.\n",
    );
    return;
  }
  const mode = args.shift();
  requireFact(
    [
      "--manifest",
      "--sentinels",
      "--assemble",
      "--report",
      "--release",
    ].includes(mode),
    "choose --manifest, --sentinels, --assemble, --report or --release; see --help",
  );
  const environment = mode === "--sentinels" ? args.shift() : undefined;
  let inventory = resolve(ROOT, INVENTORY);
  const options = {};
  if (mode === "--manifest" && args.length === 1 && !args[0].startsWith("--"))
    inventory = resolve(args.shift());
  while (args.length) {
    const name = args.shift();
    requireFact(
      [
        "--candidate",
        "--command",
        "--record",
        "--artifacts",
        "--inventory",
        "--input",
        "--output",
      ].includes(name) &&
        nonempty(args[0]) &&
        !args[0].startsWith("--") &&
        options[name] === undefined,
      "unknown, duplicate or incomplete argument",
    );
    options[name] = args.shift();
  }
  if (options["--inventory"]) inventory = resolve(options["--inventory"]);
  const manifest = readJson(inventory);
  const source = readFileSync(resolve(ROOT, SPEC), "utf8");
  const report = validateManifest(manifest, source);
  requireFact(
    new Set(acceptanceCriteria(source).map((c) => c.section)).size === 26,
    "source must contain EZ-1 through EZ-25 and EZ-Q1",
  );
  if (mode === "--sentinels") {
    requireFact(
      nonempty(environment) &&
        Object.keys(options).every((k) =>
          ["--inventory", "--command"].includes(k),
        ),
      "sentinel listing needs an environment, optional command and inventory only",
    );
    process.stdout.write(
      readySentinels(manifest, environment, options["--command"]).join("\n") +
        "\n",
    );
    return;
  }
  if (mode === "--manifest") {
    requireFact(
      Object.keys(options).every((k) => k === "--inventory"),
      "release arguments cannot be used in manifest mode",
    );
    process.stdout.write(
      JSON.stringify({
        status: "valid-inventory",
        ...report,
        qualified: false,
      }) + "\n",
    );
    return;
  }
  const allowed =
    mode === "--assemble"
      ? ["--candidate", "--input", "--artifacts", "--output", "--inventory"]
      : ["--candidate", "--record", "--artifacts", "--inventory"];
  requireFact(
    Object.keys(options).every((name) => allowed.includes(name)),
    "arguments do not match selected mode",
  );
  requireFact(
    options["--candidate"] && options["--artifacts"],
    "candidate and artifacts directory required",
  );
  const context = {
    candidateSha: options["--candidate"],
    source,
    artifactsRoot: resolve(options["--artifacts"]),
  };
  if (mode === "--assemble") {
    requireFact(
      options["--input"] && options["--output"],
      "assembly needs --input and --output",
    );
    const draft = assembleRecord(
      manifest,
      readJson(resolve(options["--input"])),
      context,
    );
    const result = inspectRelease(manifest, draft, context);
    const output = JSON.stringify(draft, null, 2) + "\n";
    requireFact(
      Buffer.byteLength(output) <= 16 * 1024 * 1024,
      "assembled JSON exceeds 16 MiB",
    );
    writeFileSync(resolve(options["--output"]), output, {
      encoding: "utf8",
      flag: "wx",
    });
    process.stdout.write(
      JSON.stringify(
        { ...result, record_output: resolve(options["--output"]) },
        null,
        2,
      ) + "\n",
    );
    process.exitCode = result.status === "passed" ? 0 : 1;
    return;
  }
  requireFact(
    options["--record"] && options["--artifacts"],
    "release needs --record and --artifacts",
  );
  const record = readJson(resolve(options["--record"]));
  const result =
    mode === "--report"
      ? inspectRelease(manifest, record, context)
      : validateRelease(manifest, record, context);
  process.stdout.write(JSON.stringify(result, null, 2) + "\n");
  process.exitCode = result.status === "passed" ? 0 : 1;
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  try {
    run(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(`outbound cutover evidence: ${error.message}\n`);
    process.exitCode = 1;
  }
}

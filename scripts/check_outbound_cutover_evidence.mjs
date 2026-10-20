#!/usr/bin/env node

import { createHash } from "node:crypto";
import { readFileSync, realpathSync, statSync } from "node:fs";
import { dirname, isAbsolute, relative, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const SPEC = "docs/superpowers/specs/2026-10-17-edgezero-pr275-requirements.md";
const INVENTORY = "tests/fixtures/reusable-app/outbound-cutover-cases.json";
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

export function validateRelease(
  manifest,
  record,
  { candidateSha, source, reviewedLegSetSha256 },
) {
  validateManifest(manifest, source, reviewedLegSetSha256);
  requireFact(
    hex(candidateSha, 40) &&
      record?.schema === 1 &&
      record.candidate_sha === candidateSha,
    "candidate revision mismatch",
  );
  requireFact(nonempty(record.runner_image), "runner image required");
  const versions = {
    rust: "1.98.1",
    fastly_sdk: "0.13.1",
    viceroy: "0.21.0",
    spin: "4.1.0",
  };
  for (const [tool, version] of Object.entries(versions))
    requireFact(
      record.toolchain?.[tool] === version,
      `wrong toolchain ${tool}`,
    );
  requireFact(nonempty(record.toolchain.workerd), "workerd revision required");
  const retention = record.retention;
  requireFact(
    ["release-asset", "repository"].includes(retention?.kind) &&
      nonempty(retention.uri) &&
      retention.expires_at === null,
    "durable non-expiring retention required while the candidate is pinned",
  );
  if (record.comparisons !== null)
    requireFact(
      nonempty(record.comparisons?.baseline) &&
        record.comparisons?.candidate === candidateSha,
      "comparison build identities required",
    );
  requireFact(
    Array.isArray(record.results) && Array.isArray(record.applicability),
    "results and applicability map required",
  );
  const results = unique(record.results, key);
  const applicability = unique(record.applicability, key);
  const cases = unique(manifest.cases, key);
  const decisions = unique(manifest.decisions, (d) => d.id);
  for (const id of [...results.keys(), ...applicability.keys()])
    requireFact(cases.has(id), `unknown case/environment ${id}`);
  let passed = 0;
  let deferred = 0;
  for (const [id, c] of cases) {
    const required = disposition(c, decisions);
    const mapping = applicability.get(id);
    requireFact(
      mapping?.disposition === required,
      `${id}: wrong or missing applicability`,
    );
    if (required === "deferred") {
      const approved = decisions.get(c.approval)?.status === "approved";
      requireFact(
        approved
          ? mapping.approval === c.approval
          : mapping.condition ===
              (c.condition ?? "follow-up not promoted or confirmed present"),
        `${id}: unjustified applicability deferral`,
      );
      deferred++;
      continue;
    }
    const result = results.get(id);
    requireFact(result, `${id}: missing result`);
    requireFact(
      c.implementation === "ready",
      `${id}: planned command is not implemented evidence`,
    );
    requireFact(
      result.candidate_sha === candidateSha,
      `${id}: result revision mismatch`,
    );
    requireFact(
      result.source_digest === c.source_digest &&
        result.command === c.command &&
        result.sentinel === c.sentinel &&
        result.artifact_path === c.artifact_path,
      `${id}: result contract mismatch`,
    );
    requireFact(
      result.status === "passed" &&
        Number.isSafeInteger(result.samples) &&
        result.samples > 0,
      `${id}: pending/failed result or zero samples`,
    );
    requireFact(
      hex(result.artifact_sha256, 64),
      `${id}: raw artifact hash required`,
    );
    requireFact(
      Array.isArray(result.assertions) &&
        result.assertions.length > 0 &&
        result.assertions.every((a) => nonempty(a.name) && a.passed === true),
      `${id}: missing or failed assertions`,
    );
    requireFact(
      Array.isArray(result.measurements),
      `${id}: measurement list required`,
    );
    for (const measurement of result.measurements)
      validateMeasurement(measurement);
    if (c.comparison === true)
      requireFact(
        record.comparisons !== null,
        `${id}: baseline comparison required`,
      );
    passed++;
  }
  return {
    status: "passed",
    candidate_sha: candidateSha,
    passed,
    deferred,
    capabilities_changed: false,
  };
}

function readJson(path) {
  requireFact(
    statSync(path).size <= 16 * 1024 * 1024,
    "JSON input exceeds 16 MiB",
  );
  return JSON.parse(readFileSync(path, "utf8"));
}

function run(args) {
  if (args.length === 1 && args[0] === "--help") {
    process.stdout.write(
      "Usage: node scripts/check_outbound_cutover_evidence.mjs --manifest [INVENTORY.json]\n       node scripts/check_outbound_cutover_evidence.mjs --release --candidate SHA --record RECORD.json --artifacts DIRECTORY [--inventory INVENTORY.json]\nManifest validation permits planned/pending legs; release validation never does.\nApprovals and raw measurements require independent review; this tool does not authenticate telemetry.\n",
    );
    return;
  }
  const mode = args.shift();
  requireFact(
    ["--manifest", "--release"].includes(mode),
    "choose --manifest or --release; see --help",
  );
  let inventory = resolve(ROOT, INVENTORY);
  const options = {};
  if (mode === "--manifest" && args.length === 1 && !args[0].startsWith("--"))
    inventory = resolve(args.shift());
  while (args.length) {
    const name = args.shift();
    requireFact(
      ["--candidate", "--record", "--artifacts", "--inventory"].includes(
        name,
      ) &&
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
  requireFact(
    options["--record"] && options["--artifacts"],
    "release needs --record and --artifacts",
  );
  const record = readJson(resolve(options["--record"]));
  const result = validateRelease(manifest, record, {
    candidateSha: options["--candidate"],
    source,
  });
  const root = realpathSync(resolve(options["--artifacts"]));
  for (const artifact of record.results) {
    const path = realpathSync(resolve(root, artifact.artifact_path));
    const within = relative(root, path);
    requireFact(
      within !== "" && !isAbsolute(within) && within.split(/[\\/]/)[0] !== "..",
      "artifact escapes evidence directory",
    );
    requireFact(
      sha256(readFileSync(path)) === artifact.artifact_sha256,
      "raw artifact hash mismatch",
    );
  }
  process.stdout.write(JSON.stringify(result) + "\n");
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

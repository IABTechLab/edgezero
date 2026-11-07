import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { validateTarget } from "./qualify_response_egress.mjs";

test("hosted collection requires allowlisting, finite budgets and no private fields", () => {
  const target = {
    origin: "https://isolated.example.com", allowed_origin: "https://isolated.example.com",
    isolated_nonproduction: true, max_probe_ms: 2000,
    run: "run-1", revision: "a".repeat(40), artifact: "b".repeat(64), binary_sha256: "c".repeat(64),
    adapter: "cloudflare", sdk: "worker 0.8.5", runtime: { unknown: "provider-unpublished" },
    compatibility: { date: "2026-10-07", flags: [] }, deployment: "isolated-test-deployment",
  };
  assert.equal(validateTarget(target).origin, target.origin);
  for (const changed of [
    { ...target, origin: "http://isolated.example.com" },
    { ...target, origin: "https://token@isolated.example.com" },
    { ...target, allowed_origin: "https://another.example.com" },
    { ...target, isolated_nonproduction: false },
    { ...target, max_probe_ms: 0 },
    { ...target, private_token: "do-not-persist" },
    { ...target, adapter: "fastly" },
  ]) assert.throws(() => validateTarget(changed));
});

test("the hosted collector shares the canonical Rust schedule and cannot validate evidence", () => {
  const schedule = JSON.parse(readFileSync(new URL("../crates/edgezero-test-harness/acceptance/response-egress-schedule.json", import.meta.url)));
  assert.equal(schedule.length, 26);
  assert.equal(new Set(schedule.map(probe => probe.probe)).size, 26);
  const rejected = spawnSync(process.execPath, [new URL("./qualify_response_egress.mjs", import.meta.url).pathname, "--validate", "unused-clients.json", "unused-events.json"], { encoding: "utf8" });
  assert.equal(rejected.status, 1);
  assert.match(rejected.stderr, /Hosted lifecycle qualification failed/);
});

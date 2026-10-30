#!/usr/bin/env node
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { checksum, check_advisories, load } from './verify_artifacts.mjs';

export function auditLock(lockfile, inputsFile, output, binary, version) {
  const inputs = load(inputsFile);
  assert.equal(checksum(lockfile), inputs.lock_sha256, 'Audit lockfile/input mismatch');
  const tool = spawnSync(binary, ['--version'], { encoding: 'utf8' });
  assert.equal(tool.status, 0, 'Cannot execute cargo-audit');
  assert.equal(tool.stdout.trim(), `cargo-audit ${version}`, 'Unexpected cargo-audit version');
  const result = spawnSync(binary, ['audit', '--json', '--file', path.resolve(lockfile)], {
    encoding: 'utf8', timeout: 300000, maxBuffer: 16 * 1024 * 1024,
  });
  assert.ifError(result.error);
  assert.ok(result.status === 0 || result.status === 1, `cargo-audit execution failed (${result.status})`);
  const report = JSON.parse(result.stdout);
  const record = { schema: 1, lock_sha256: inputs.lock_sha256, tool: { name: 'cargo-audit', version }, report };
  check_advisories(record, inputs);
  assert.equal(result.status, Number(report.vulnerabilities.found), 'Unexpected cargo-audit result status');
  assert.equal(checksum(lockfile), inputs.lock_sha256, 'Audit lockfile changed during inspection');
  fs.writeFileSync(output, JSON.stringify(record, null, 2) + '\n');
  console.error(`Rust advisory inventory: ${report.vulnerabilities.count} vulnerabilities; report-only, not release approval.`);
  return record;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    assert.equal(process.argv.length, 7, 'Usage: audit_lock.mjs LOCK INPUTS OUTPUT CARGO_AUDIT VERSION');
    auditLock(...process.argv.slice(2));
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}

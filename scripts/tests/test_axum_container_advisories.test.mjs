import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { test } from 'node:test';
import { auditLock } from '../../examples/container-publishing/audit_lock.mjs';
import { check_advisories } from '../../examples/container-publishing/verify_artifacts.mjs';

function fixture(t, { count = 0, status = Number(count > 0), version = '0.22.2' } = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'axum-advisories-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const lock = path.join(root, 'Cargo.lock'), inputsFile = path.join(root, 'inputs.json');
  const output = path.join(root, 'rust-advisories.json'), binary = path.join(root, 'cargo-audit');
  fs.writeFileSync(lock, 'version = 4\n');
  const inputs = { lock_sha256: createHash('sha256').update(fs.readFileSync(lock)).digest('hex') };
  fs.writeFileSync(inputsFile, JSON.stringify(inputs));
  const report = {
    database: { 'last-commit': 'a'.repeat(40), 'last-updated': '2026-10-28T00:00:00Z' },
    lockfile: { 'dependency-count': 1 },
    settings: { ignore: [], target_arch: [], target_os: [], severity: null },
    vulnerabilities: { found: count > 0, count, list: count ? [{ advisory: { id: 'RUSTSEC-2026-0001' } }] : [] },
    warnings: {},
  };
  fs.writeFileSync(binary, `#!${process.execPath}\nif (process.argv[2] === '--version') console.log('cargo-audit ${version}');\nelse { console.log(${JSON.stringify(JSON.stringify(report))}); process.exitCode = ${status}; }\n`, { mode: 0o755 });
  return { lock, inputsFile, output, binary, inputs, report };
}

for (const count of [0, 1]) {
  test(`audit retains a lock-bound complete inventory (${count} findings, report-only)`, t => {
    const f = fixture(t, { count });
    auditLock(f.lock, f.inputsFile, f.output, f.binary, '0.22.2');
    const record = JSON.parse(fs.readFileSync(f.output));
    assert.equal(record.lock_sha256, f.inputs.lock_sha256);
    assert.deepEqual(record.report, f.report);
    assert.deepEqual(record.tool, { name: 'cargo-audit', version: '0.22.2' });
  });
}
for (const options of [{ status: 2 }, { status: 1 }, { count: 1, status: 0 }, { version: '0.0.0' }]) {
  test(`audit rejects failed or unexpected execution ${JSON.stringify(options)}`, t => {
    const f = fixture(t, options);
    assert.throws(() => auditLock(f.lock, f.inputsFile, f.output, f.binary, '0.22.2'));
    assert.ok(!fs.existsSync(f.output));
  });
}
test('audit rejects mismatched lock bytes before invoking the tool', t => {
  const f = fixture(t);
  fs.appendFileSync(f.lock, '# modified\n');
  assert.throws(() => auditLock(f.lock, f.inputsFile, f.output, '/missing/tool', '0.22.2'), /lockfile\/input mismatch/);
  assert.ok(!fs.existsSync(f.output));
});
for (const mutate of [
  r => { delete r.report.database; },
  r => { r.report.settings.ignore = ['RUSTSEC-2026-0001']; },
  r => { r.report.settings.target_arch = ['aarch64']; },
  r => { r.report.settings.target_os = ['linux']; },
  r => { r.report.settings.severity = 'high'; },
  r => { r.report.lockfile['dependency-count'] = 0; },
  r => { r.report.vulnerabilities.count = 1; },
  r => { r.report.vulnerabilities.found = true; },
]) {
  test(`advisory validation rejects missing or filtered evidence: ${mutate}`, t => {
    const f = fixture(t);
    const record = { schema: 1, lock_sha256: f.inputs.lock_sha256, tool: { name: 'cargo-audit', version: '0.22.2' }, report: f.report };
    mutate(record);
    assert.throws(() => check_advisories(record, f.inputs));
  });
}

import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { gzipSync } from 'node:zlib';
import { tarEntry } from './axum_container_tar.mjs';
import { test } from 'node:test';
import { auditLock } from '../../examples/container-publishing/audit_lock.mjs';
import { check_advisories } from '../../examples/container-publishing/verify_artifacts.mjs';

const handoff = fs.readFileSync(new URL('../test_axum_container_handoff.sh', import.meta.url), 'utf8');
const tools = fs.readFileSync(new URL('../../examples/container-publishing/tools.env', import.meta.url), 'utf8');
const auditVersion = tools.match(/^CARGO_AUDIT_VERSION=(.+)$/m)[1];

test('cargo-audit source archive has a committed checksum pin', () => {
  assert.equal(tools.match(/^CARGO_AUDIT_SHA256=(.+)$/m)[1],
    '700c2b240f7fd330c24b675fe429f73a5b676531fcc6300400b2b67f155ba12a');
});

for (const scenario of ['success', 'bad checksum', 'missing lock']) {
  test(`cargo-audit source installation: ${scenario}`, t => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'axum-audit-install-'));
    t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const bin = path.join(root, 'bin'), work = path.join(root, 'work');
    fs.mkdirSync(bin); fs.mkdirSync(work);
    const entries = [tarEntry(`cargo-audit-${auditVersion}/Cargo.toml`, '[package]\nname="cargo-audit"\n')];
    if (scenario !== 'missing lock') entries.push(tarEntry(`cargo-audit-${auditVersion}/Cargo.lock`, 'version = 4\n'));
    const archive = gzipSync(Buffer.concat([...entries, Buffer.alloc(1024)]));
    const source = path.join(root, 'source.crate'), installed = path.join(root, 'installed.json');
    const downloaded = path.join(root, 'downloaded.json');
    fs.writeFileSync(source, archive);
    fs.writeFileSync(path.join(bin, 'curl'), `#!${process.execPath}\nconst fs = require('node:fs'); const args = process.argv.slice(2); fs.writeFileSync(process.env.DOWNLOADED, JSON.stringify(args)); fs.copyFileSync(process.env.ARCHIVE, args[args.indexOf('--output') + 1]);\n`, { mode: 0o755 });
    fs.writeFileSync(path.join(bin, 'cargo'), `#!${process.execPath}\nrequire('node:fs').writeFileSync(process.env.INSTALLED, JSON.stringify({ args: process.argv.slice(2), rust: process.env.RUSTUP_TOOLCHAIN }));\n`, { mode: 0o755 });
    // Execute the shipping block, but use a tiny local archive and no network or Cargo.
    const block = handoff.slice(handoff.indexOf('  curl --fail --silent --show-error --location'), handoff.indexOf('  mkdir "$WORK/audit-home"'));
    assert.ok(!block.includes('sha256sum --check'));
    assert.ok(block.includes('sha256sum "$WORK/cargo-audit.crate"'));
    const result = spawnSync('bash', ['-euo', 'pipefail', '-c', block], { encoding: 'utf8', env: {
      ...process.env, PATH: `${bin}:${process.env.PATH}`, WORK: work, rust_pin: '1.95.0',
      CARGO_AUDIT_VERSION: auditVersion,
      CARGO_AUDIT_SHA256: scenario === 'bad checksum' ? '0'.repeat(64) : createHash('sha256').update(archive).digest('hex'),
      ARCHIVE: source, INSTALLED: installed, DOWNLOADED: downloaded,
    } });
    assert.ok(JSON.parse(fs.readFileSync(downloaded)).includes(`https://static.crates.io/crates/cargo-audit/cargo-audit-${auditVersion}.crate`));
    if (scenario === 'success') {
      assert.equal(result.status, 0, result.stderr);
      assert.deepEqual(JSON.parse(fs.readFileSync(installed)), {
        args: ['install', '--path', `${work}/audit-source/cargo-audit-${auditVersion}`, '--locked', '--root', `${work}/audit-tools`],
        rust: '1.95.0',
      });
    } else {
      assert.notEqual(result.status, 0);
      assert.ok(!fs.existsSync(installed));
      if (scenario === 'bad checksum') {
        assert.match(result.stderr, /cargo-audit checksum mismatch/);
        assert.ok(!fs.existsSync(path.join(work, 'audit-source')));
      } else assert.match(result.stderr, /Expected packaged cargo-audit lockfile/);
    }
  });
}

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
    settings: { ignore: [], target_arch: [], target_os: [], severity: null,
      informational_warnings: ['unmaintained', 'unsound', 'notice'] },
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
test('strict advisory policy permits other informational warnings but not vulnerabilities or unsoundness', t => {
  const f = fixture(t);
  const record = { schema: 1, lock_sha256: f.inputs.lock_sha256, tool: { name: 'cargo-audit', version: '0.22.2' }, report: f.report };
  record.report.warnings = { unmaintained: [{ advisory: { id: 'RUSTSEC-test' } }], unsound: [] };
  assert.doesNotThrow(() => check_advisories(record, f.inputs));
  record.report.warnings.unsound.push({ advisory: { id: 'RUSTSEC-test' } });
  assert.throws(() => check_advisories(record, f.inputs), /Blocked Rust unsound advisories/);
  assert.doesNotThrow(() => check_advisories(record, f.inputs, { policy: 'report-only' }));
  record.report.warnings.unsound = [];
  record.report.vulnerabilities = { found: true, count: 1, list: [{ advisory: { id: 'RUSTSEC-test' } }] };
  assert.throws(() => check_advisories(record, f.inputs), /Blocked Rust vulnerabilities/);
  assert.doesNotThrow(() => check_advisories(record, f.inputs, { policy: 'report-only' }));
});
for (const mutate of [
  r => { delete r.report.database; },
  r => { r.report.settings.ignore = ['RUSTSEC-2026-0001']; },
  r => { r.report.settings.target_arch = ['aarch64']; },
  r => { r.report.settings.target_os = ['linux']; },
  r => { r.report.settings.severity = 'high'; },
  r => { delete r.report.settings.informational_warnings; },
  r => { r.report.settings.informational_warnings = []; },
  r => { r.report.settings.informational_warnings = ['unsound']; },
  r => { r.report.settings.informational_warnings = ['unmaintained', 'notice']; },
  r => { r.report.lockfile['dependency-count'] = 0; },
  r => { r.report.vulnerabilities.count = 1; },
  r => { r.report.vulnerabilities.found = true; },
  r => { delete r.report.warnings; },
  r => { r.report.warnings = []; },
  r => { r.report.warnings.unsound = {}; },
]) {
  test(`advisory validation rejects missing or filtered evidence: ${mutate}`, t => {
    const f = fixture(t);
    const record = { schema: 1, lock_sha256: f.inputs.lock_sha256, tool: { name: 'cargo-audit', version: '0.22.2' }, report: f.report };
    mutate(record);
    for (const policy of ['strict', 'report-only']) {
      assert.throws(() => check_advisories(record, f.inputs, { policy }));
    }
  });
}

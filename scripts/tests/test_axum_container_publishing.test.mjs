import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { contextHash } from '../axum_container_helpers.mjs';
import { hashContext } from '../../examples/container-publishing/prepare_inputs.mjs';

const root = new URL('../../', import.meta.url);
const workflow = readFileSync(new URL('examples/container-publishing/release.yml', root), 'utf8');
const preparation = new URL('examples/container-publishing/prepare_inputs.mjs', root);
const sha256 = value => createHash('sha256').update(value).digest('hex');

function scratch(t) {
  const directory = mkdtempSync(join(tmpdir(), 'axum-publishing-test-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

function prepare(app, source) {
  return spawnSync(process.execPath, [preparation.pathname, app, source], {
    encoding: 'utf8', env: { ...process.env, GITHUB_SHA: 'a'.repeat(40) },
  });
}

test('application preparation hashes exactly the sorted committed context bytes', t => {
  const directory = scratch(t);
  const app = join(directory, 'context');
  mkdirSync(join(app, 'assets'), { recursive: true });
  const files = {
    'assets/message.txt': 'app asset\n',
    'assets/\uE000.txt': 'BMP filename\n',
    'assets/\u{10000}.txt': 'supplementary filename\n',
    'Cargo.lock': 'application-owned lock\n',
    'Dockerfile': `FROM debian@sha256:${'b'.repeat(64)}\n`,
    '.tool-versions': 'rust 1.95.0\n',
  };
  for (const [name, data] of Object.entries(files)) writeFileSync(join(app, name), data);
  const source = join(directory, 'source.tar');
  writeFileSync(source, 'immutable git archive bytes');
  const fixtureHash = contextHash(app);
  const result = prepare(app, source);
  assert.equal(result.status, 0, result.stderr);
  const inputs = JSON.parse(readFileSync(join(app, 'fixture-inputs.json')));
  const context = createHash('sha256');
  for (const name of Object.keys(files).sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)))) {
    context.update(name + '\0' + files[name] + '\0');
  }
  assert.equal(inputs.context_sha256, context.digest('hex'));
  assert.equal(inputs.context_sha256, fixtureHash);
  assert.equal(inputs.context_sha256, hashContext(Object.entries(files)));
  assert.notEqual(prepare(app, source).status, 0, 'reserved receipt must not enter its own context hash');
  assert.equal(inputs.source_sha256, sha256(readFileSync(source)));
  assert.equal(inputs.lock_sha256, sha256(files['Cargo.lock']));
  assert.equal(inputs.dockerfile_sha256, sha256(files.Dockerfile));
  assert.deepEqual(inputs.base_images, [`debian@sha256:${'b'.repeat(64)}`]);
  assert.equal(inputs.toolchain, files['.tool-versions']);
  assert.equal(inputs.revision, 'a'.repeat(40));
  assert.equal(inputs.dirty, false);
});

test('application preparation rejects symlinked source instead of hashing external bytes', t => {
  const directory = scratch(t);
  const app = join(directory, 'context');
  mkdirSync(app);
  const outside = join(directory, 'outside');
  writeFileSync(outside, 'must not enter the build context');
  symlinkSync(outside, join(app, 'link'));
  const result = prepare(app, outside);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /must not contain links/);
});

test('release protection fails closed unless every required setting is present', t => {
  const directory = scratch(t);
  const environment = { can_admins_bypass: false, protection_rules: [
    { type: 'required_reviewers', reviewers: [{ id: 1 }], prevent_self_review: true },
  ] };
  const guard = workflow.match(/          jq -e -n --slurpfile environment[\s\S]*?\n            }/)[0]
    .replace(/^          /gm, '');
  function check(value, branch = { protected: true }) {
    writeFileSync(join(directory, 'environment.json'), JSON.stringify(value));
    writeFileSync(join(directory, 'branch.json'), JSON.stringify(branch));
    return spawnSync('bash', ['-euc', guard], {
      encoding: 'utf8', env: { ...process.env, RUNNER_TEMP: directory },
    });
  }
  assert.equal(check(environment).status, 0);
  for (const invalid of [
    {}, { ...environment, can_admins_bypass: true },
    { ...environment, protection_rules: [] },
    { ...environment, protection_rules: [{ ...environment.protection_rules[0], reviewers: [] }] },
    { ...environment, protection_rules: [{ ...environment.protection_rules[0], prevent_self_review: false }] },
  ]) assert.notEqual(check(invalid).status, 0);
  assert.notEqual(check(environment, { protected: false }).status, 0);
});

test('publisher merges child blobs without changing bytes and records exact index digest', t => {
  const directory = scratch(t);
  const work = join(directory, 'work');
  const output = join(directory, 'output');
  mkdirSync(output);
  for (const arch of ['amd64', 'arm64']) mkdirSync(join(work, arch, 'blobs/sha256'), { recursive: true });
  const shared = Buffer.from('shared layer bytes');
  const child = Buffer.from('arm64 config bytes');
  writeFileSync(join(work, 'amd64/blobs/sha256', sha256(shared)), shared);
  writeFileSync(join(work, 'arm64/blobs/sha256', sha256(shared)), shared);
  writeFileSync(join(work, 'arm64/blobs/sha256', sha256(child)), child);
  const index = Buffer.from('{"schemaVersion":2,"manifests":[]}\n');
  writeFileSync(join(output, 'index.json'), index);
  const publisher = readFileSync(new URL('examples/container-publishing/publish.sh', root), 'utf8');
  const merge = publisher.slice(publisher.indexOf('mkdir -p "$WORK/merged'), publisher.indexOf('AUTH=();'));
  function runMerge() {
    return spawnSync('bash', ['-euc', merge], {
      encoding: 'utf8', env: { ...process.env, WORK: work, OUTPUT: output },
    });
  }
  const result = runMerge();
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(readFileSync(join(work, 'merged/blobs/sha256', sha256(shared))), shared);
  assert.deepEqual(readFileSync(join(work, 'merged/blobs/sha256', sha256(child))), child);
  assert.deepEqual(readFileSync(join(work, 'merged/blobs/sha256', sha256(index))), index);
  assert.equal(readFileSync(join(output, 'digest'), 'utf8'), `sha256:${sha256(index)}\n`);
  const descriptor = JSON.parse(readFileSync(join(work, 'merged/index.json'))).manifests[0];
  assert.equal(descriptor.digest, `sha256:${sha256(index)}`);
  assert.equal(descriptor.size, index.length);
  writeFileSync(join(work, 'arm64/blobs/sha256', sha256(shared)), 'conflicting blob');
  assert.notEqual(runMerge().status, 0);
});

test('Buildx setup reuses the daemon and the handoff alone creates the pinned builder', () => {
  const native = readFileSync(new URL('.github/workflows/axum-container.yml', root), 'utf8');
  for (const source of [native, workflow]) {
    const setup = source.match(/      - uses: docker\/setup-buildx-action@[^\n]+\n        with:\n((?:          [^\n]*\n)+)/)?.[1];
    assert.ok(setup, 'expected Buildx setup');
    assert.match(setup, /driver: docker\n/);
    assert.match(setup, /use: false\n/);
    assert.doesNotMatch(setup, /driver-opts:/);
    assert.doesNotMatch(source, /moby\/buildkit:/);
  }
  const handoff = readFileSync(new URL('scripts/test_axum_container_handoff.sh', root), 'utf8');
  assert.match(handoff, /docker buildx create .*--driver docker-container --driver-opt "image=\$BUILDKIT_IMAGE"/);
  assert.match(handoff, /docker buildx build --builder "\$BUILDER"/);
});

test('container helper CI has one suite owner and derives framework Node pins', () => {
  const native = readFileSync(new URL('.github/workflows/axum-container.yml', root), 'utf8');
  const actions = readFileSync(new URL('.github/workflows/deploy-action.yml', root), 'utf8');
  assert.doesNotMatch(native, /- docs\//);
  assert.equal(native.split('node --test scripts/tests/test_axum_container*.test.mjs').length - 1, 1);
  assert.ok(native.includes('AXUM_PREPARED_CONTEXT='));
  assert.ok(!actions.includes('node --test scripts/tests/test_axum_container*.test.mjs'));
  for (const source of [native, actions]) {
    assert.ok(!source.includes('node-version: "24.12.0"'));
    assert.ok(source.includes('steps.node-version.outputs.node-version'));
  }
  assert.equal(native.split('- scripts/package*.json').length - 1, 2);
});

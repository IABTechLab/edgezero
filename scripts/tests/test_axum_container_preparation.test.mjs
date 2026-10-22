import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';
import test from 'node:test';
import { paxRecord, tarEntry } from './axum_container_tar.mjs';
import {
  appName, contextFiles, contextHash, isWithin, parseToml, recordInputs,
  rewriteManifest, stageSources, validateRuntimeTar,
} from '../axum_container_helpers.mjs';

const helper = fileURLToPath(new URL('../axum_container_helpers.mjs', import.meta.url));
const APP = process.env.AXUM_PREPARED_CONTEXT;
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex');
const readToml = (file) => parseToml(fs.readFileSync(file, 'utf8'));
const readJson = (file) => JSON.parse(fs.readFileSync(file, 'utf8'));
function temporary(t) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'axum-preparation-test-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  return directory;
}
function put(root, name, content) {
  const file = path.join(root, name);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, content);
  return file;
}

for (const [name, check] of [
  ['nested workspace and features preserved', (app, manifest) => {
    assert.deepEqual(manifest.workspace.exclude, ['vendor/edgezero']);
    for (const [name, dep] of Object.entries(manifest.workspace.dependencies)) {
      if (name.startsWith('edgezero-')) assert.equal(dep.path, `vendor/edgezero/crates/${name}`);
    }
    const axum = readToml(path.join(app, 'crates/container-probe-adapter-axum/Cargo.toml'));
    assert.deepEqual(axum.dependencies['edgezero-adapter-axum'].features, ['axum']);
    const cli = readToml(path.join(app, 'crates/container-probe-cli/Cargo.toml'));
    assert.ok(!Object.hasOwn(cli.dependencies['edgezero-cli'], 'default-features'));
    const framework = readToml(path.join(app, 'vendor/edgezero/Cargo.toml'));
    assert.ok(Object.hasOwn(framework.workspace.dependencies, 'async-compression'));
  }],
  ['own lockfile and confined local packages', (app, _manifest, inputs) => {
    assert.ok(!fs.existsSync(path.join(app, '.git')));
    const lock = fs.readFileSync(path.join(app, 'Cargo.lock'));
    assert.equal(digest(lock), inputs.lock_sha256);
    assert.ok(!lock.equals(fs.readFileSync(path.join(app, 'vendor/edgezero/Cargo.lock'))));
    for (const pkg of readJson(path.join(app, '..', 'metadata.json')).packages) {
      if (pkg.source === null) assert.ok(isWithin(app, fs.realpathSync(pkg.manifest_path)), pkg.name);
    }
  }],
  ['no git or host build output staged', (app, _manifest, inputs) => {
    const vendor = path.join(app, 'vendor');
    function inspect(directory) {
      for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
        const file = path.join(directory, entry.name);
        const parts = path.relative(app, file).split(path.sep);
        assert.ok(!parts.includes('.git'));
        assert.ok(!parts.includes('target'));
        if (entry.isDirectory()) inspect(file);
      }
    }
    inspect(vendor);
    assert.equal(inputs.dependency_mode, 'staged-test-sources');
  }],
]) {
  test(`prepared context: ${name}`, { skip: APP ? false : 'set AXUM_PREPARED_CONTEXT to the freshly prepared fixture' }, () => {
    const app = fs.realpathSync(APP);
    check(app, readToml(path.join(app, 'Cargo.toml')), readJson(path.join(app, 'fixture-inputs.json')));
  });
}

test('TOML reads generated tables, dotted keys, arrays, comments and escaped strings', () => {
  const result = parseToml(String.raw`
"app".name = "probe\u002dapp" # decoded before validation
[workspace.dependencies]
"edgezero-core" = { path = 'old/path', default-features = false, features = [
  "axum", # array comment
  'other',
] }
[workspace.package]
authors = []
description = """
line one
line two"""
[[triggers.http]]
path = "/"
[[triggers.http]]
path = "/other"
`);
  assert.equal(result.app.name, 'probe-app');
  assert.deepEqual(result.workspace.dependencies['edgezero-core'].features, ['axum', 'other']);
  assert.equal(result.workspace.dependencies['edgezero-core']['default-features'], false);
  assert.equal(result.workspace.package.description, 'line one\nline two');
  assert.equal(result.triggers.http[1].path, '/other');
  assert.equal(parseToml("[app]\nname = '''\nprobe-app'''\n").app.name, 'probe-app');
  assert.equal(parseToml('app = { name = "probe" }\n').app.name, 'probe');
  assert.equal(parseToml('[app]\nname = """probe\\\n  -app"""\n').app.name, 'probe-app');
});

test('TOML rejects duplicate keys, broken values, invalid escapes and unterminated input', () => {
  for (const source of [
    '[app]\nname="x"\nname="y"', '[app]\n[app]\nname="x"',
    '[app]\nname="unterminated', '[app]\nname="bad\\q"',
    '[app]\nname="bad\\uD800"', '[app]\nname=bare',
    '[app]\nname=["x"', '[app]\nname={x="y"', '[app]\nname="x" trailing',
  ]) assert.throws(() => parseToml(source), /TOML line/);
});

test('app-name CLI reads TOML and rejects shell-unsafe or missing names', (t) => {
  const directory = temporary(t);
  const manifest = put(directory, 'edgezero.toml', '[app] # table comment\nname = "probe\\u002dapp" # name comment\n');
  assert.equal(appName(manifest), 'probe-app');
  assert.equal(execFileSync(process.execPath, [helper, 'app-name', manifest], { encoding: 'utf8' }), 'probe-app\n');
  for (const source of ['[app]\nname="../escape"', '[app]\nname="probe\\n"', '[app]\nname=12', '[other]\nname="probe"']) {
    fs.writeFileSync(manifest, source);
    const result = spawnSync(process.execPath, [helper, 'app-name', manifest], { encoding: 'utf8' });
    assert.notEqual(result.status, 0);
    assert.equal(result.stdout, '');
  }
});

test('dependency rewrite preserves flags and nested workspace policy, fails closed on unexpected shape', (t) => {
  const vendor = temporary(t);
  put(vendor, 'crates/edgezero-core/Cargo.toml', '[package]\nname="edgezero-core"\n');
  const original = '[workspace]\nresolver = "2"\n[workspace.dependencies]\nedgezero-core = { path = "old", default-features = false, features = ["axum"] } # keep\n';
  const rewritten = rewriteManifest(original, vendor);
  assert.ok(rewritten.includes('default-features = false, features = ["axum"] } # keep'));
  assert.equal(readToml(put(vendor, 'rewritten.toml', rewritten)).workspace.dependencies['edgezero-core'].path,
    'vendor/edgezero/crates/edgezero-core');
  assert.deepEqual(parseToml(rewritten).workspace.exclude, ['vendor/edgezero']);
  assert.throws(() => rewriteManifest(original.replace('edgezero-core = { path = "old", default-features = false, features = ["axum"] }', 'edgezero-core = "1"'), vendor), /checkout dependency/);
  assert.throws(() => rewriteManifest(original.replace('resolver = "2"', 'resolver = "3"'), vendor), /resolver/);
  assert.throws(() => rewriteManifest(original.replace('edgezero-core =', '"edgezero-core" ='), vendor), /Cannot stage/);
});

function sourceFixture(t) {
  const directory = temporary(t);
  const root = path.join(directory, 'root');
  const app = path.join(directory, 'app');
  put(root, '.gitignore', 'target/\n');
  put(root, 'Cargo.toml', '[workspace]\nmembers=[]\n');
  put(root, 'Cargo.lock', 'framework lock\n');
  put(root, '.tool-versions', 'rust 1.95.0\n');
  put(root, 'crates/edgezero-core/Cargo.toml', '[package]\nname="edgezero-core"\n');
  put(root, 'crates/edgezero-core/src/lib.rs', 'pub fn probe() {}\n');
  put(root, 'examples/app-demo/src/lib.rs', '// optional example\n');
  put(root, 'docs/irrelevant.md', 'not staged\n');
  put(root, 'target/host-artifact', 'not staged\n');
  execFileSync('git', ['init', '-q', root]);
  execFileSync('git', ['add', '.'], { cwd: root });
  execFileSync('git', ['-c', 'user.name=Test', '-c', 'user.email=test@example.test', '-c', 'commit.gpgsign=false', 'commit', '-qm', 'fixture'], { cwd: root });
  // Untracked source is part of the exact source hash too. These names exercise
  // Unicode code-point sorting, which differs from JavaScript UTF-16 sorting.
  put(root, 'crates/\u{10000}.rs', '// supplementary\n');
  put(root, 'crates/\ue000.rs', '// bmp\n');
  put(app, 'Cargo.toml', '[workspace]\nresolver = "2"\n[workspace.dependencies]\nedgezero-core = { path = "old", features = ["axum"] }\n');
  fs.mkdirSync(path.join(app, '.git'));
  return { root, app };
}

test('source staging preserves selected source bytes and path-NUL-bytes-NUL hash contract', (t) => {
  const { root, app } = sourceFixture(t);
  stageSources(root, app);
  const names = ['.tool-versions', 'Cargo.lock', 'Cargo.toml', 'crates/edgezero-core/Cargo.toml',
    'crates/edgezero-core/src/lib.rs', 'crates/\ue000.rs', 'crates/\u{10000}.rs', 'examples/app-demo/src/lib.rs'];
  const expected = createHash('sha256');
  for (const name of names) {
    const bytes = fs.readFileSync(path.join(root, name));
    expected.update(Buffer.concat([Buffer.from(`${name}\0`), bytes, Buffer.from([0])]));
    assert.ok(bytes.equals(fs.readFileSync(path.join(app, 'vendor/edgezero', name))));
  }
  const source = readJson(path.join(app, 'fixture-source.json'));
  assert.equal(source.source_sha256, expected.digest('hex'));
  assert.equal(source.revision, execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim());
  assert.equal(source.dirty, true);
  assert.equal(source.dependency_mode, 'staged-test-sources');
  assert.ok(!fs.existsSync(path.join(app, '.git')));
  assert.ok(!fs.existsSync(path.join(app, 'vendor/edgezero/docs')));
  assert.ok(!fs.existsSync(path.join(app, 'vendor/edgezero/target')));
});

test('source staging rejects symlinks and missing tracked sources', (t) => {
  for (const symlink of [true, false]) {
    const { root, app } = sourceFixture(t);
    const file = path.join(root, 'crates/edgezero-core/src/lib.rs');
    fs.unlinkSync(file);
    if (symlink) fs.symlinkSync(path.join(root, 'Cargo.lock'), file);
    assert.throws(() => stageSources(root, app), /Expected regular source file/);
  }
});

test('record-inputs verifies confinement and retains context, lock, Dockerfile and source hash contracts', (t) => {
  const { root, app } = sourceFixture(t);
  stageSources(root, app);
  put(app, 'Cargo.lock', 'own app lock\n');
  put(app, '.tool-versions', 'rust 1.95.0\nnodejs 22.0.0\n');
  put(app, 'Dockerfile', 'FROM rust:pin AS builder\nFROM debian:pin\n');
  const metadata = put(path.dirname(app), 'metadata.json', JSON.stringify({ packages: [
    { name: 'app', source: null, manifest_path: path.join(app, 'Cargo.toml') },
    { name: 'registry', source: 'registry+example', manifest_path: '/outside/context' },
  ] }));
  const expectedHash = createHash('sha256');
  for (const file of contextFiles(app)) {
    expectedHash.update(`${path.relative(app, file)}\0`).update(fs.readFileSync(file)).update('\0');
  }
  const before = expectedHash.digest('hex');
  assert.equal(contextHash(app), before);
  recordInputs(app, metadata);
  const inputs = readJson(path.join(app, 'fixture-inputs.json'));
  assert.equal(inputs.context_sha256, before);
  assert.equal(inputs.lock_sha256, digest(fs.readFileSync(path.join(app, 'Cargo.lock'))));
  assert.equal(inputs.dockerfile_sha256, digest(fs.readFileSync(path.join(app, 'Dockerfile'))));
  assert.equal(inputs.toolchain, 'rust 1.95.0\nnodejs 22.0.0\n');
  assert.deepEqual(inputs.base_images, ['rust:pin', 'debian:pin']);
  assert.equal(inputs.package, 'container-probe-adapter-axum');
  assert.equal(inputs.binary, inputs.package);
  assert.equal(inputs.source_sha256, readJson(path.join(app, 'fixture-source.json')).source_sha256);
  fs.writeFileSync(metadata, JSON.stringify({ packages: [
    { name: 'escaped', source: null, manifest_path: path.join(root, 'Cargo.toml') },
  ] }));
  assert.throws(() => recordInputs(app, metadata), /Out-of-context dependency: escaped/);
});

function elf(machine = 62) {
  const data = Buffer.alloc(20);
  data.set([0x7f, 69, 76, 70, 2, 1]);
  data.writeUInt16LE(machine, 18);
  return data;
}
function runtimeTar(t, entries) {
  return put(temporary(t), 'runtime.tar', Buffer.concat([...entries, Buffer.alloc(1024)]));
}

test('runtime tar validates native ELF for amd64 and arm64, including linked app files', async (t) => {
  for (const [platform, machine] of [['linux/amd64', 62], ['linux/arm64', 183]]) {
    const file = runtimeTar(t, [tarEntry('usr/local/bin/app', elf(machine))]);
    assert.equal(await validateRuntimeTar(file, platform), `ELF class 2 machine ${machine}`);
  }
  const linked = runtimeTar(t, [tarEntry('usr/local/bin/program', elf()), tarEntry('usr/local/bin/app', '', '2', 'program')]);
  assert.match(await validateRuntimeTar(linked, 'linux/amd64'), /machine 62/);
  const hardlinked = runtimeTar(t, [tarEntry('usr/local/bin/program', elf()), tarEntry('usr/local/bin/app', '', '1', 'usr/local/bin/program')]);
  assert.match(await validateRuntimeTar(hardlinked, 'linux/amd64'), /machine 62/);
});

test('runtime tar streams past large payloads and accepts compressed exports', async (t) => {
  const file = runtimeTar(t, [
    tarEntry('usr/local/bin/app', Buffer.concat([elf(), Buffer.alloc(2 * 1024 * 1024)])),
    tarEntry('usr/share/large', Buffer.alloc(2 * 1024 * 1024)),
    tarEntry('etc/os-release', 'test runtime'),
  ]);
  assert.match(await validateRuntimeTar(file, 'linux/amd64'), /machine 62/);
  const compressed = put(temporary(t), 'runtime.tar.gz', gzipSync(fs.readFileSync(file)));
  assert.match(await validateRuntimeTar(compressed, 'linux/amd64'), /machine 62/);
  fs.writeFileSync(file, Buffer.concat([
    tarEntry('usr/local/bin/app', Buffer.concat([elf(), Buffer.alloc(2 * 1024 * 1024)])),
    tarEntry('usr/bin/cargo', 'forbidden after large app'), Buffer.alloc(1024),
  ]));
  await assert.rejects(validateRuntimeTar(file, 'linux/amd64'), /Forbidden runtime path/);
});

test('runtime tar rejects build inputs, tools and secret sentinels, including PAX/GNU long names', async (t) => {
  for (const name of ['build/file', './context/file', 'usr/local/cargo/config', 'usr/local/rustup/file',
    'app/.git/config', 'app/container-sentinel', 'usr/bin/cargo', 'usr/bin/rustc', 'usr/bin/node',
    'usr/bin/npm', 'app/Cargo.toml']) {
    const file = runtimeTar(t, [tarEntry('usr/local/bin/app', elf()), tarEntry(name, 'forbidden')]);
    await assert.rejects(validateRuntimeTar(file, 'linux/amd64'), /Forbidden runtime path/);
  }
  for (const extension of [
    tarEntry('PaxHeader', paxRecord('path', `build/${'x'.repeat(120)}`), 'x'),
    tarEntry('././@LongLink', `build/${'x'.repeat(120)}\0`, 'L'),
  ]) {
    const file = runtimeTar(t, [tarEntry('usr/local/bin/app', elf()), extension, tarEntry('safe', 'forbidden')]);
    await assert.rejects(validateRuntimeTar(file, 'linux/amd64'), /Forbidden runtime path/);
  }
});

test('runtime tar rejects missing, truncated, malformed or wrong-platform executables', async (t) => {
  const bigEndian = elf();
  bigEndian[5] = 2;
  for (const [entries, error] of [
    [[tarEntry('app/other', 'file')], /Missing runtime executable/],
    [[tarEntry('usr/local/bin/app', 'not elf')], /native ELF/],
    [[tarEntry('usr/local/bin/app', bigEndian)], /ELF64 little-endian/],
    [[tarEntry('usr/local/bin/app', elf(183))], /platform mismatch/],
    [[tarEntry('usr/local/bin/app', '', '2', 'app')], /Cyclic/],
  ]) await assert.rejects(validateRuntimeTar(runtimeTar(t, entries), 'linux/amd64'), error);
  const corrupt = tarEntry('usr/local/bin/app', elf());
  corrupt[0] = 0;
  await assert.rejects(validateRuntimeTar(runtimeTar(t, [corrupt]), 'linux/amd64'), /checksum/);
  const truncated = put(temporary(t), 'runtime.tar', tarEntry('usr/local/bin/app', elf()).subarray(0, 515));
  await assert.rejects(validateRuntimeTar(truncated, 'linux/amd64'), /Truncated/);
});

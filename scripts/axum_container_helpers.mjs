#!/usr/bin/env node
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createGunzip } from 'node:zlib';
import { readTar } from '../examples/container-publishing/verify_artifacts.mjs';
import { parse as parseToml } from 'smol-toml';
import { compareNames, hashContext } from '../examples/container-publishing/prepare_inputs.mjs';

export { parseToml };
const read = (file) => fs.readFileSync(file, 'utf8');
const json = (file) => JSON.parse(read(file));
const writeJson = (file, value) => fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
export function isWithin(parent, child) {
  const relative = path.relative(parent, child);
  return relative === '' || (!relative.startsWith(`..${path.sep}`) && relative !== '..' && !path.isAbsolute(relative));
}
export function appName(file) {
  const name = parseToml(read(file)).app?.name;
  assert.equal(typeof name, 'string', 'Expected app.name string');
  assert.match(name, /^[a-zA-Z0-9_-]+(?![\s\S])/, 'Invalid generated app name');
  return name;
}

export function rewriteManifest(text, vendor) {
  const deps = parseToml(text).workspace?.dependencies;
  assert.ok(deps && typeof deps === 'object', 'Expected workspace.dependencies');
  for (const [name, dep] of Object.entries(deps)) {
    if (!name.startsWith('edgezero-')) continue;
    assert.ok(dep && typeof dep === 'object' && Object.hasOwn(dep, 'path') &&
      fs.statSync(path.join(vendor, 'crates', name, 'Cargo.toml'), { throwIfNoEntry: false })?.isFile(),
    `Expected generated checkout dependency: ${name}`);
    const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const pattern = new RegExp(`^(${escaped}\\s*=\\s*\\{[^\\n]*?\\bpath\\s*=\\s*)"[^"]*"`, 'gm');
    let count = 0;
    text = text.replace(pattern, (_, prefix) => {
      count++;
      return `${prefix}"vendor/edgezero/crates/${name}"`;
    });
    assert.equal(count, 1, `Cannot stage generated dependency: ${name}`);
  }
  const resolver = 'resolver = "2"';
  assert.equal(text.split(resolver).length - 1, 1, 'Unexpected generated workspace resolver');
  // Preserve the framework's own workspace dependency/lint inheritance.
  return text.replace(resolver, `exclude = ["vendor/edgezero"]\n${resolver}`);
}

export function stageSources(root, app) {
  root = fs.realpathSync(root);
  const vendor = path.join(app, 'vendor', 'edgezero');
  const names = execFileSync('git', ['ls-files', '-z', '--cached', '--others', '--exclude-standard'], {
    cwd: root, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024,
  }).split('\0');
  const sourceHash = createHash('sha256');
  for (const name of [...new Set(names.filter(Boolean))].sort(compareNames)) {
    if (!['Cargo.toml', 'Cargo.lock', '.tool-versions'].includes(name) &&
        !name.startsWith('crates/') && !name.startsWith('examples/app-demo/')) continue;
    const source = path.join(root, name);
    assert.ok(fs.lstatSync(source, { throwIfNoEntry: false })?.isFile() &&
      isWithin(root, fs.realpathSync(source)), `Expected regular source file: ${name}`);
    const destination = path.join(vendor, name);
    fs.mkdirSync(path.dirname(destination), { recursive: true });
    const bytes = fs.readFileSync(source);
    fs.writeFileSync(destination, bytes);
    sourceHash.update(name).update('\0').update(bytes).update('\0');
  }
  const manifest = path.join(app, 'Cargo.toml');
  fs.writeFileSync(manifest, rewriteManifest(read(manifest), vendor));
  fs.rmSync(path.join(app, '.git'), { recursive: true, force: true });
  writeJson(path.join(app, 'fixture-source.json'), {
    revision: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim(),
    source_sha256: sourceHash.digest('hex'),
    dirty: Boolean(execFileSync('git', ['status', '--porcelain'], { cwd: root }).length),
    dependency_mode: 'staged-test-sources',
  });
}

export function contextFiles(app) {
  const files = [];
  function walk(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      // Directory symlinks are not followed; file symlinks hash their target bytes.
      if (entry.isDirectory()) walk(file);
      else if (fs.statSync(file, { throwIfNoEntry: false })?.isFile()) files.push(file);
    }
  }
  walk(app);
  return files.sort((a, b) => compareNames(path.relative(app, a), path.relative(app, b)));
}
export function contextHash(app) {
  return hashContext(contextFiles(app).map(file => [
    path.relative(app, file).split(path.sep).join('/'), fs.readFileSync(file),
  ]));
}
export function recordInputs(app, metadataFile) {
  app = fs.realpathSync(app);
  for (const pkg of json(metadataFile).packages) {
    if (pkg.source === null) {
      assert.ok(isWithin(app, fs.realpathSync(pkg.manifest_path)), `Out-of-context dependency: ${pkg.name}`);
    }
  }
  const inputs = json(path.join(app, 'fixture-source.json'));
  inputs.package = 'container-probe-adapter-axum';
  inputs.binary = 'container-probe-adapter-axum';
  inputs.lock_sha256 = sha256(fs.readFileSync(path.join(app, 'Cargo.lock')));
  inputs.toolchain = read(path.join(app, '.tool-versions'));
  inputs.dockerfile_sha256 = sha256(fs.readFileSync(path.join(app, 'Dockerfile')));
  inputs.base_images = [...read(path.join(app, 'Dockerfile')).matchAll(/^FROM (\S+)/gm)].map((match) => match[1]);
  inputs.context_sha256 = contextHash(app);
  writeJson(path.join(app, 'fixture-inputs.json'), inputs);
}

// Check resolved PAX/GNU member names without extracting paths. Retain only
// each file's ELF-sized prefix, so app links can be resolved after streaming.
export async function validateRuntimeTar(file, platform) {
  assert.ok(['linux/amd64', 'linux/arm64'].includes(platform), 'Unsupported platform');
  const members = new Map();
  const paths = new Set();
  const forbidden = ['build/', 'context/', 'usr/local/cargo/', 'usr/local/rustup/'];
  const tools = new Set(['usr/bin/cargo', 'usr/bin/rustc', 'usr/bin/node', 'usr/bin/npm', 'app/Cargo.toml']);
  const fd = fs.openSync(file, 'r');
  const magic = Buffer.alloc(2);
  try { fs.readSync(fd, magic); } finally { fs.closeSync(fd); }
  const input = fs.createReadStream(file);
  let stream = input;
  if (magic[0] === 0x1f && magic[1] === 0x8b) {
    stream = createGunzip();
    input.on('error', (error) => stream.destroy(error));
    input.pipe(stream);
  }
  try {
    await readTar(stream, async (member, chunks) => {
      const name = member.name.replace(/^[./]+/, '');
      assert.ok(!forbidden.some((prefix) => name.startsWith(prefix)) && !name.includes('/.git/') &&
        !name.endsWith('container-sentinel') && !tools.has(name), `Forbidden runtime path: ${name}`);
      assert.notEqual(member.type, 'S', 'Sparse runtime tar members are unsupported');
      paths.add(name);
      const prefixes = [];
      let length = 0;
      if (['0', '7'].includes(member.type)) {
        for await (const chunk of chunks) {
          const prefix = chunk.subarray(0, 20 - length);
          prefixes.push(Buffer.from(prefix));
          length += prefix.length;
          if (length === 20) break;
        }
      }
      members.set(name, { ...member, header: Buffer.concat(prefixes, length) });
    });
  } finally {
    input.destroy();
    stream.destroy();
  }
  assert.ok(paths.has('usr/local/bin/app'), 'Missing runtime executable');
  const visited = new Set();
  function extract(name) {
    assert.ok(!visited.has(name), 'Cyclic runtime executable link');
    visited.add(name);
    const member = members.get(name);
    assert.ok(member, `Missing runtime member: ${name}`);
    if (member.type === '1') return extract(member.linkname.replace(/^(?:\.\/|\/)+/, ''));
    if (member.type === '2') {
      const target = member.linkname.startsWith('/') ? member.linkname.slice(1) :
        path.posix.join(path.posix.dirname(name), member.linkname);
      return extract(path.posix.normalize(target));
    }
    assert.ok(['0', '7'].includes(member.type), 'Runtime executable is not a regular file');
    return member.header;
  }
  const header = extract('usr/local/bin/app');
  assert.ok(header.length === 20 && header.subarray(0, 4).equals(Buffer.from([0x7f, 69, 76, 70])),
    'not a native ELF executable');
  assert.ok(header[4] === 2 && header[5] === 1, 'expected ELF64 little-endian');
  const machine = header.readUInt16LE(18);
  assert.equal(machine, platform === 'linux/amd64' ? 62 : 183, 'ELF/platform mismatch');
  return `ELF class ${header[4]} machine ${machine}`;
}

async function main(args) {
  const [command, ...values] = args;
  const counts = { 'app-name': 1, dependencies: 1, 'stage-sources': 2, 'record-inputs': 2, 'runtime-tar': 2 };
  assert.ok(Object.hasOwn(counts, command) && values.length === counts[command],
    'Usage: axum_container_helpers.mjs app-name TOML | dependencies CARGO_TOML | stage-sources ROOT APP | record-inputs APP METADATA | runtime-tar TAR PLATFORM');
  if (command === 'app-name') console.log(appName(values[0]));
  else if (command === 'dependencies') {
    for (const [name, dep] of Object.entries(parseToml(read(values[0])).workspace.dependencies)) {
      if (name.startsWith('edgezero-')) console.log(name, JSON.stringify(dep));
    }
  } else if (command === 'stage-sources') stageSources(...values);
  else if (command === 'record-inputs') recordInputs(...values);
  else console.log(await validateRuntimeTar(...values));
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(process.argv.slice(2)); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}

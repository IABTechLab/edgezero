#!/usr/bin/env node
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// UTF-8 byte ordering follows Unicode code-point order, not UTF-16 order.
export const compareNames = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b));
export function hashContext(entries) {
  const hash = createHash('sha256');
  for (const [name, bytes] of [...entries].sort(([a], [b]) => compareNames(a, b))) {
    // Each UTF-8 name and file payload has an unsigned 64-bit byte length.
    for (const value of [Buffer.from(name), typeof bytes === 'string' ? Buffer.from(bytes) : bytes]) {
      const length = Buffer.alloc(8);
      length.writeBigUInt64BE(BigInt(value.length));
      hash.update(length).update(value);
    }
  }
  return hash.digest('hex');
}

export function baseImages(dockerfile) {
  return [...dockerfile.matchAll(/^\s*FROM\s+(?:--\S+\s+)*(\S+)/gim)].map(match => match[1]);
}

function contextEntries(app) {
  const files = new Map();
  function walk(directory, prefix = '') {
    for (const name of fs.readdirSync(directory)) {
      const relative = prefix + name;
      if (relative === 'fixture-inputs.json') continue;
      const file = path.join(directory, name);
      const stat = fs.lstatSync(file);
      if (stat.isDirectory()) walk(file, relative + '/');
      else {
        assert.ok(stat.isFile(), 'Release source must not contain links or special files');
        files.set(relative, fs.readFileSync(file));
      }
    }
  }
  walk(app);
  return files;
}

export function verifyContext(app) {
  const receipt = path.join(app, 'fixture-inputs.json');
  assert.ok(fs.lstatSync(receipt).isFile(), 'Expected regular input receipt');
  const inputs = JSON.parse(fs.readFileSync(receipt, 'utf8'));
  assert.equal(hashContext(contextEntries(app)), inputs.context_sha256, 'Prepared context hash mismatch');
}

export function prepareInputs(app, source, revision) {
  assert.ok(!fs.existsSync(path.join(app, 'fixture-inputs.json')), 'Reserved fixture-inputs.json already exists');
  const files = contextEntries(app);
  assert.ok(fs.lstatSync(source).isFile(), 'Expected regular source archive');
  const hash = bytes => createHash('sha256').update(bytes).digest('hex');
  const inputs = {
    revision, dirty: false,
    dependency_mode: 'application-owned-committed-lock',
    source_sha256: hash(fs.readFileSync(source)), context_sha256: hashContext(files),
    lock_sha256: hash(files.get('Cargo.lock')),
    dockerfile_sha256: hash(files.get('Dockerfile')),
    base_images: baseImages(files.get('Dockerfile').toString('utf8')),
    toolchain: files.get('.tool-versions').toString('utf8'),
  };
  fs.writeFileSync(path.join(app, 'fixture-inputs.json'), JSON.stringify(inputs, null, 2) + '\n');
  return inputs;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    assert.equal(process.argv.length, 4, 'Usage: prepare_inputs.mjs CONTEXT SOURCE_TAR | --verify CONTEXT');
    if (process.argv[2] === '--verify') verifyContext(process.argv[3]);
    else {
      assert.match(process.env.GITHUB_SHA ?? '', /^[0-9a-f]{40}(?![\s\S])/, 'Expected immutable source commit');
      prepareInputs(process.argv[2], process.argv[3], process.env.GITHUB_SHA);
    }
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}

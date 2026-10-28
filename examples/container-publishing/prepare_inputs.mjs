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
    hash.update(name).update('\0').update(bytes).update('\0');
  }
  return hash.digest('hex');
}

export function prepareInputs(app, source, revision) {
  assert.ok(!fs.existsSync(path.join(app, 'fixture-inputs.json')), 'Reserved fixture-inputs.json already exists');
  const files = new Map();
  function walk(directory, prefix = '') {
    for (const name of fs.readdirSync(directory)) {
      const relative = prefix + name;
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
  assert.ok(fs.lstatSync(source).isFile(), 'Expected regular source archive');
  const hash = bytes => createHash('sha256').update(bytes).digest('hex');
  const inputs = {
    revision, dirty: false,
    dependency_mode: 'application-owned-committed-lock',
    source_sha256: hash(fs.readFileSync(source)), context_sha256: hashContext(files),
    lock_sha256: hash(files.get('Cargo.lock')),
    dockerfile_sha256: hash(files.get('Dockerfile')),
    base_images: [...files.get('Dockerfile').toString('utf8').matchAll(/^FROM (\S+)/gm)].map(m => m[1]),
    toolchain: files.get('.tool-versions').toString('utf8'),
  };
  fs.writeFileSync(path.join(app, 'fixture-inputs.json'), JSON.stringify(inputs, null, 2) + '\n');
  return inputs;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    assert.equal(process.argv.length, 4, 'Usage: prepare_inputs.mjs CONTEXT SOURCE_TAR');
    assert.match(process.env.GITHUB_SHA ?? '', /^[0-9a-f]{40}(?![\s\S])/, 'Expected immutable source commit');
    prepareInputs(process.argv[2], process.argv[3], process.env.GITHUB_SHA);
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}

#!/usr/bin/env node
/** Verify single-platform OCI bundles without npm dependencies.
 * Receipts/checksums prove integrity, not authentication. The publisher must
 * select immutable artifacts from its approved same-run producer first.
 */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { createHash } from 'node:crypto';
import { createGunzip } from 'node:zlib';
import { spawn } from 'node:child_process';
import http from 'node:http';
import https from 'node:https';
import { isDeepStrictEqual } from 'node:util';
import { fileURLToPath } from 'node:url';

export const MANIFEST = 'application/vnd.oci.image.manifest.v1+json';
export const CONFIG = 'application/vnd.oci.image.config.v1+json';
export const INDEX = 'application/vnd.oci.image.index.v1+json';
export const LAYERS = new Set(['application/vnd.oci.image.layer.v1.tar', 'application/vnd.oci.image.layer.v1.tar+gzip', 'application/vnd.oci.image.layer.v1.tar+zstd']);
export const DIGEST = /^sha256:[0-9a-f]{64}(?![\s\S])/;
export const PLATFORMS = new Set(['linux/amd64', 'linux/arm64']);
export const SIDECARS = ['inputs.json', 'scan.json', 'scanner.json', 'sbom.json', 'provenance.json'];
const equal = isDeepStrictEqual;
const utf8 = data => new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(data);
const present = value => Boolean(value) && (typeof value !== 'object' || Object.keys(value).length > 0);
export function require(condition, message) {
  if (!condition) throw new Error(message);
}
const hash = data => createHash('sha256').update(data).digest('hex');
function regular(file, message) {
  require(fs.lstatSync(file).isFile(), message);
}
export function checksum(file) {
  regular(file, 'Expected regular artifact file');
  const digest = createHash('sha256');
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const chunk = Buffer.alloc(1024 * 1024);
    let size;
    while ((size = fs.readSync(fd, chunk)) > 0) digest.update(chunk.subarray(0, size));
  } finally { fs.closeSync(fd); }
  return digest.digest('hex');
}
export function load(file) {
  regular(file, 'Expected regular JSON file');
  require(fs.statSync(file).size <= 16 * 1024 * 1024, `JSON too large: ${path.basename(file)}`);
  return JSON.parse(utf8(fs.readFileSync(file)));
}
// Python json.dumps(..., indent=2) uses ASCII escapes and preserves insertion order.
// The exact index bytes, including the final newline, identify the release.
export function serialize(value) {
  return JSON.stringify(value, null, 2).replace(/[\u007f-\uffff]/g, c => `\\u${c.charCodeAt(0).toString(16).padStart(4, '0')}`) + '\n';
}

function child(command, args, options = {}) {
  const process = spawn(command, args, { ...options, stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = '';
  process.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-8192); });
  const done = new Promise((resolve, reject) => {
    process.on('error', reject);
    process.on('close', code => code === 0 ? resolve() : reject(new Error(`${command} failed (${code}): ${stderr.trim()}`)));
  });
  // An error may arrive while the tar reader is still awaiting stdout.
  done.catch(() => {});
  return { process, done };
}
function tarNumber(field) {
  let value;
  if (field[0] & 0x80) {
    require(field[0] === 0x80, 'Invalid archive number');
    value = 0;
    for (const byte of field.subarray(1)) value = value * 256 + byte;
  } else {
    const text = field.toString('ascii').replace(/\0.*$/s, '').trim();
    require(/^[0-7]*(?![\s\S])/.test(text), 'Invalid archive number');
    value = text ? Number.parseInt(text, 8) : 0;
  }
  require(Number.isSafeInteger(value) && value >= 0, 'Invalid archive size');
  return value;
}
function tarString(field) { return utf8(field.subarray(0, field.indexOf(0) < 0 ? field.length : field.indexOf(0))); }
function pax(data) {
  const fields = {};
  let offset = 0;
  while (offset < data.length) {
    const space = data.indexOf(32, offset);
    require(space > offset && /^[0-9]+(?![\s\S])/.test(data.toString('ascii', offset, space)), 'Invalid PAX record');
    const length = Number(data.toString('ascii', offset, space));
    require(length > space - offset + 1 && offset + length <= data.length && data[offset + length - 1] === 10, 'Invalid PAX record');
    const record = utf8(data.subarray(space + 1, offset + length - 1));
    const separator = record.indexOf('=');
    require(separator > 0, 'Invalid PAX record');
    const key = record.slice(0, separator);
    require(!/sparse/i.test(key), 'Unexpected archive member');
    fields[key] = record.slice(separator + 1);
    offset += length;
  }
  return fields;
}

/** Read tar records without trusting tar's filesystem extraction or listing format.
 * Payloads stream in bounded chunks. Metadata is bounded separately. */
export async function readTar(stream, visit) {
  const iterator = stream[Symbol.asyncIterator]();
  let pending = Buffer.alloc(0);
  async function* take(length) {
    while (length > 0) {
      if (!pending.length) {
        const next = await iterator.next();
        require(!next.done, 'Truncated archive member');
        pending = next.value;
      }
      const size = Math.min(length, pending.length);
      yield pending.subarray(0, size);
      pending = pending.subarray(size);
      length -= size;
    }
  }
  async function bytes(length) {
    const chunks = [];
    for await (const chunk of take(length)) chunks.push(chunk);
    return Buffer.concat(chunks, length);
  }
  let global = {}, local = {}, longName, longLink;
  for (;;) {
    const header = await bytes(512);
    if (header.every(byte => byte === 0)) break;
    const expected = tarNumber(header.subarray(148, 156));
    let unsigned = 0, signed = 0;
    for (let i = 0; i < 512; i++) {
      const byte = i >= 148 && i < 156 ? 32 : header[i];
      unsigned += byte;
      signed += byte > 127 ? byte - 256 : byte;
    }
    require(expected === unsigned || expected === signed, 'Invalid archive checksum');
    const type = String.fromCharCode(header[156] || 48);
    let name = tarString(header.subarray(0, 100));
    if (header.toString('ascii', 257, 263) === 'ustar\0') {
      const prefix = tarString(header.subarray(345, 500));
      if (prefix) name = prefix + '/' + name;
    }
    let size = tarNumber(header.subarray(124, 136));
    if (['x', 'g', 'L', 'K'].includes(type)) {
      require(size <= 1024 * 1024, 'Archive metadata too large');
      const data = await bytes(size);
      if (type === 'L') longName = tarString(data);
      else if (type === 'K') longLink = tarString(data);
      else if (type === 'g') global = { ...global, ...pax(data) };
      else local = { ...local, ...pax(data) };
      await bytes((512 - size % 512) % 512);
      continue;
    }
    const fields = { ...global, ...local };
    name = fields.path ?? longName ?? name;
    if (fields.size !== undefined) {
      require(/^[0-9]+(?![\s\S])/.test(fields.size), 'Invalid archive size');
      size = Number(fields.size);
      require(Number.isSafeInteger(size), 'Invalid archive size');
    }
    const linkname = fields.linkpath ?? longLink ?? tarString(header.subarray(157, 257));
    local = {};
    longName = undefined;
    longLink = undefined;
    const payload = take(size);
    // A visitor may stop after a prefix without closing the underlying payload.
    const data = { [Symbol.asyncIterator]: () => ({ next: () => payload.next(), return: async () => ({ done: true }) }) };
    await visit({ name, type, size, linkname }, data);
    // Always consume the remainder, including after a visitor reads only a prefix.
    for await (const chunk of payload) { void chunk; }
    await bytes((512 - size % 512) % 512);
  }
  // git archive's exact bytes include padding beyond the end marker.
  for (;;) { const next = await iterator.next(); if (next.done) break; }
}

export async function unpack(archive, destination) {
  regular(archive, 'Expected regular artifact file');
  fs.mkdirSync(destination);
  const fd = fs.openSync(archive, 'r');
  const magic = Buffer.alloc(6);
  try { fs.readSync(fd, magic); } finally { fs.closeSync(fd); }
  const input = fs.createReadStream(archive);
  let stream = input, decompressor;
  if (magic[0] === 0x1f && magic[1] === 0x8b) {
    stream = createGunzip();
    input.on('error', error => stream.destroy(error));
    input.pipe(stream);
  } else if (magic.toString('ascii', 0, 3) === 'BZh' || magic.toString('hex') === 'fd377a585a00') {
    decompressor = child(magic[0] === 66 ? 'bzip2' : 'xz', ['-dc', '--', path.resolve(archive)]);
    stream.destroy();
    stream = decompressor.process.stdout;
  }
  const seen = new Set();
  let total = 0;
  try {
    await readTar(stream, async (member, data) => {
      const name = member.name.replace(/^\.\//, '');
      if (['', '.'].includes(name) && member.type === '5') return;
      const normalized = name.replace(/\/+$/, '');
      require(!name.startsWith('/') && !name.includes('\0') && !name.split('/').includes('..') && path.posix.normalize(name).replace(/\/+$/, '') === normalized && !name.split('/').includes('.'), 'Unsafe archive path');
      require(!seen.has(normalized), 'Duplicate archive member');
      seen.add(normalized);
      require(seen.size <= 10000, 'Too many archive members');
      const directory = member.type === '5' && ['blobs', 'blobs/sha256'].includes(normalized);
      const file = ['0', '7'].includes(member.type) && (['oci-layout', 'index.json'].includes(name) || /^blobs\/sha256\/[0-9a-f]{64}(?![\s\S])/.test(name));
      require((directory || file) && !member.linkname, 'Unexpected archive member');
      total += member.size;
      require(total <= 4 * 1024 ** 3, 'Archive exceeds 4 GiB limit');
      const target = path.join(destination, normalized);
      if (directory) fs.mkdirSync(target, { recursive: true });
      else {
        fs.mkdirSync(path.dirname(target), { recursive: true });
        const output = fs.openSync(target, 'wx');
        try {
          for await (const chunk of data) {
            let offset = 0;
            while (offset < chunk.length) offset += fs.writeSync(output, chunk, offset);
          }
        } finally { fs.closeSync(output); }
        require(fs.statSync(target).size === member.size, 'Truncated archive member');
      }
    });
    if (decompressor) await decompressor.done;
  } finally {
    stream.destroy();
    input.destroy();
    if (decompressor && decompressor.process.exitCode === null) decompressor.process.kill();
  }
}

export function blob(layout, descriptor, mediaTypes) {
  const { digest, mediaType, size } = descriptor;
  require(typeof digest === 'string' && DIGEST.test(digest), 'Unsupported blob digest');
  require(mediaTypes.has(mediaType), 'Unexpected blob media type');
  require(Number.isSafeInteger(size) && size >= 0, 'Invalid descriptor size');
  const file = path.join(layout, 'blobs/sha256', digest.slice(7));
  regular(file, 'Missing regular blob');
  require(fs.statSync(file).size === size && checksum(file) === digest.slice(7), 'Blob digest/size mismatch');
  return file;
}
export function describe(layout, platform) {
  require(PLATFORMS.has(platform), 'Unsupported platform');
  require(equal(load(path.join(layout, 'oci-layout')), { imageLayoutVersion: '1.0.0' }), 'Unsupported OCI layout');
  const index = load(path.join(layout, 'index.json'));
  require(index.schemaVersion === 2 && (index.mediaType ?? INDEX) === INDEX, 'Invalid OCI index');
  require(Array.isArray(index.manifests) && index.manifests.length === 1, 'Expected exactly one runtime manifest, no attestations');
  const descriptor = index.manifests[0];
  const manifest = load(blob(layout, descriptor, new Set([MANIFEST])));
  require(manifest.schemaVersion === 2 && manifest.mediaType === MANIFEST, 'Invalid runtime manifest');
  const config = load(blob(layout, manifest.config, new Set([CONFIG])));
  require(`${config.os}/${config.architecture}` === platform, 'Config/platform mismatch');
  if ('platform' in descriptor) require(`${descriptor.platform.os}/${descriptor.platform.architecture}` === platform, 'Descriptor/platform mismatch');
  require(Array.isArray(manifest.layers) && manifest.layers.length, 'Runtime image has no layers');
  for (const layer of manifest.layers) blob(layout, layer, LAYERS);
  return { platform, manifest_digest: descriptor.digest, manifest_size: descriptor.size, config_digest: manifest.config.digest,
    revision_label: config.config?.Labels?.['org.opencontainers.image.revision'] ?? null };
}
export function verify_pulled(layout, inspection, platform, reference) {
  const info = describe(layout, platform), image = inspection[0];
  require(reference.endsWith('@' + info.manifest_digest), 'Expected pulled child digest mismatch');
  require(image.RepoDigests.includes(reference), 'Pulled digest reference mismatch');
  require(`${image.Os}/${image.Architecture}` === platform, 'Pulled platform mismatch');
  const config = load(path.join(layout, 'blobs/sha256', info.config_digest.slice(7)));
  require(Object.hasOwn(image.RootFS, 'Layers') && Object.hasOwn(config.rootfs, 'diff_ids') && equal(image.RootFS.Layers, config.rootfs.diff_ids), 'Pulled filesystem identity mismatch');
  for (const key of ['User', 'Env', 'Entrypoint', 'Cmd', 'Labels', 'WorkingDir', 'ExposedPorts', 'StopSignal']) {
    require(equal(image.Config[key] ?? null, config.config[key] ?? null), 'Pulled runtime config mismatch');
  }
  if ('Descriptor' in image) require(image.Descriptor.digest === info.manifest_digest, 'Pulled manifest mismatch');
  return info;
}
export function scan_policy(report, exceptions, manifestDigest, today = new Date().toISOString().slice(0, 10), { policy = 'strict' } = {}) {
  require(['strict', 'report-only'].includes(policy), 'Invalid scan policy');
  require(report.SchemaVersion === 2 && Array.isArray(report.Results) && report.Results.length, 'Missing Trivy results');
  require(Array.isArray(exceptions), 'Exceptions must be a list');
  const allowed = new Set();
  const key = (digest, vulnerability, pkg) => JSON.stringify([digest, vulnerability, pkg]);
  for (const entry of exceptions) {
    require(equal(Object.keys(entry).sort(), ['manifest_digest', 'vulnerability', 'package', 'owner', 'reason', 'expires'].sort()), 'Invalid exception fields');
    require(Object.values(entry).every(value => typeof value === 'string' && value.trim()), 'Empty exception field');
    require(DIGEST.test(entry.manifest_digest), 'Invalid exception artifact scope');
    require(!/[*?\[\]]/.test(entry.package + entry.vulnerability), 'Wildcard scan exception');
    require(/^\d{4}-\d{2}-\d{2}(?![\s\S])/.test(entry.expires) && !Number.isNaN(Date.parse(entry.expires)) && new Date(entry.expires).toISOString().slice(0, 10) === entry.expires, 'Invalid exception expiry');
    require(entry.expires >= today, 'Expired scan exception');
    const identity = key(entry.manifest_digest, entry.vulnerability, entry.package);
    require(!allowed.has(identity), 'Duplicate scan exception');
    allowed.add(identity);
  }
  const counts = { HIGH: 0, CRITICAL: 0 };
  let fixable = 0;
  for (const result of report.Results) {
    for (const finding of result.Vulnerabilities || []) {
      require(['UNKNOWN', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'].includes(finding.Severity), 'Invalid vulnerability severity');
      if (Object.hasOwn(counts, finding.Severity)) {
        require(['VulnerabilityID', 'PkgName'].every(field => typeof finding[field] === 'string' && finding[field].trim()), 'Invalid vulnerability/package identity');
        counts[finding.Severity]++;
        fixable += present(finding.FixedVersion);
        if (policy === 'strict') require(allowed.has(key(manifestDigest, finding.VulnerabilityID, finding.PkgName)), `Blocked ${finding.Severity} vulnerability: ${finding.VulnerabilityID} in ${finding.PkgName}`);
      }
    }
  }
  if (policy === 'report-only') console.error(`WARNING: report-only scan: ${counts.HIGH} HIGH, ${counts.CRITICAL} CRITICAL package findings, ${fixable} with fixes. Packaging evidence only; not approved for release.`);
}
export function check_inputs(inputs, revision, runId) {
  require(inputs.revision === revision, 'Input/source revision mismatch');
  require(typeof inputs.dirty === 'boolean', 'Input state missing');
  require(runId === 'local' || !inputs.dirty, 'Dirty source cannot be a trusted release input');
  for (const name of ['source_sha256', 'context_sha256', 'lock_sha256', 'dockerfile_sha256']) require(typeof inputs[name] === 'string' && /^[0-9a-f]{64}(?![\s\S])/.test(inputs[name]), 'Input hash missing');
  require(typeof inputs.toolchain === 'string' && /^rust [0-9]+\.[0-9]+\.[0-9]+$/m.test(inputs.toolchain), 'Toolchain trace missing');
  require(Array.isArray(inputs.base_images) && inputs.base_images.length && inputs.base_images.every(base => typeof base === 'string' && /^[^\s@]+@sha256:[0-9a-f]{64}(?![\s\S])/.test(base)), 'Pinned base trace missing');
}
export function seal(bundle, revision, runId, platform) {
  require(typeof revision === 'string' && /^[0-9a-f]{40}(?![\s\S])/.test(revision), 'Expected immutable source commit');
  require(typeof runId === 'string' && /^(?:[0-9]+(?:-[0-9]+)?|local)(?![\s\S])/.test(runId), 'Invalid run identity');
  const info = describe(path.join(bundle, 'oci'), platform);
  require(info.revision_label === revision, 'Image/source revision mismatch');
  check_inputs(load(path.join(bundle, 'inputs.json')), revision, runId);
  const provenance = load(path.join(bundle, 'provenance.json'));
  require(provenance.subject === info.manifest_digest && provenance.revision === revision, 'Provenance subject/source mismatch');
  const hashes = Object.fromEntries(SIDECARS.map(name => [name, checksum(path.join(bundle, name))]));
  const receipt = { schema: 1, revision, run_id: runId, checks: 'packaging-passed', ...info, archive_sha256: checksum(path.join(bundle, 'image.tar')), sidecars: hashes };
  fs.writeFileSync(path.join(bundle, 'receipt.json'), serialize(receipt));
  return receipt;
}
export async function verify(bundle, revision, runId, platform, exceptions, { policy = 'strict' } = {}) {
  const receipt = load(path.join(bundle, 'receipt.json'));
  require(receipt.schema === 1 && receipt.checks === 'packaging-passed', 'Acceptance missing');
  require(receipt.revision === revision && receipt.run_id === runId, 'Unauthorized source/run');
  require(receipt.platform === platform, 'Receipt/platform mismatch');
  require(checksum(path.join(bundle, 'image.tar')) === receipt.archive_sha256, 'Archive checksum mismatch');
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'verify-artifacts-'));
  let info, config;
  try {
    const layout = path.join(scratch, 'oci');
    await unpack(path.join(bundle, 'image.tar'), layout);
    info = describe(layout, platform);
    config = load(path.join(layout, 'blobs/sha256', info.config_digest.slice(7)));
  } finally { fs.rmSync(scratch, { recursive: true, force: true }); }
  require(Object.entries(info).every(([key, value]) => equal(receipt[key], value)), 'Receipt/image mismatch');
  require(info.revision_label === revision, 'Image revision label mismatch');
  require(equal(Object.keys(receipt.sidecars).sort(), [...SIDECARS].sort()), 'Missing sidecar identities');
  for (const [name, expected] of Object.entries(receipt.sidecars)) require(checksum(path.join(bundle, name)) === expected, 'Metadata checksum mismatch');
  const inputs = load(path.join(bundle, 'inputs.json'));
  check_inputs(inputs, revision, runId);
  const provenance = load(path.join(bundle, 'provenance.json'));
  require(provenance.subject === info.manifest_digest && provenance.revision === revision, 'Provenance subject/source mismatch');
  require(equal(provenance.inputs, inputs) && provenance.run_id === runId, 'Provenance input/run mismatch');
  const scanner = load(path.join(bundle, 'scanner.json'));
  require(present(scanner.Version) && present(scanner.VulnerabilityDB?.UpdatedAt), 'Scanner/database identity missing');
  require(scanner.SubjectManifestDigest === info.manifest_digest, 'Scanner subject mismatch');
  const report = load(path.join(bundle, 'scan.json'));
  require(report.Metadata?.ImageID === info.config_digest, 'Scan/image config mismatch');
  require(Object.hasOwn(config.rootfs, 'diff_ids') && equal(report.Metadata?.DiffIDs, config.rootfs.diff_ids), 'Scan/filesystem mismatch');
  const family = report.Metadata?.OS?.Family;
  require(family && report.Results.some(result => result.Class === 'os-pkgs' && result.Type === family), 'Supported OS scan missing');
  require(report.Results.every(result => ['os-pkgs', 'lang-pkgs'].includes(result.Class) && present(result.Target)), 'Incomplete supported scan results');
  scan_policy(report, exceptions, info.manifest_digest, undefined, { policy });
  const sbom = load(path.join(bundle, 'sbom.json'));
  require(sbom.bomFormat === 'CycloneDX' && Array.isArray(sbom.components) && sbom.components.length, 'SBOM missing');
  require((sbom.metadata?.component?.properties ?? []).some(property => property.name === 'aquasecurity:trivy:ImageID' && property.value === info.config_digest), 'SBOM/image config mismatch');
  return receipt;
}
export function runtime_index(receipts) {
  require(receipts.length === 2 && equal(new Set(receipts.map(receipt => receipt.platform)), PLATFORMS), 'Both unique native variants required');
  require(new Set(receipts.map(receipt => JSON.stringify([receipt.revision, receipt.run_id]))).size === 1, 'Variant source/run mismatch');
  require(new Set(receipts.map(receipt => receipt.sidecars['inputs.json'])).size === 1, 'Variant prepared input mismatch');
  return { schemaVersion: 2, mediaType: INDEX, manifests: [...receipts].sort((a, b) => a.platform < b.platform ? -1 : 1).map(receipt => ({ mediaType: MANIFEST, digest: receipt.manifest_digest, size: receipt.manifest_size, platform: { os: 'linux', architecture: receipt.platform.split('/')[1] } })) };
}
export function artifact_ids(metadata, runId, attempt, revision = undefined) {
  const names = new Set(['amd64', 'arm64'].map(arch => `accepted-${arch}-${runId}-${attempt}`));
  const selected = metadata.artifacts.filter(artifact => names.has(artifact.name));
  require(selected.length === 2 && equal(new Set(selected.map(artifact => artifact.name)), names), 'Accepted artifacts missing or duplicated');
  for (const artifact of selected) {
    require(Number.isSafeInteger(artifact.id) && artifact.id > 0 && Object.hasOwn(artifact, 'expired') && !present(artifact.expired), 'Invalid artifact identity');
    require(typeof artifact.digest === 'string' && DIGEST.test(artifact.digest), 'Actions artifact digest missing');
    require(String(artifact.workflow_run.id) === runId, 'Artifact run mismatch');
    if (revision !== undefined) require(artifact.workflow_run.head_sha === revision, 'Artifact producer source mismatch');
  }
  return selected.sort((a, b) => a.name < b.name ? -1 : 1).map(artifact => artifact.id).join(',');
}
export async function release_record(source, bundles, metadata, revision, runId, exceptions) {
  require(typeof revision === 'string' && /^[0-9a-f]{40}(?![\s\S])/.test(revision), 'Expected immutable source commit');
  const git = child('git', ['archive', '--format=tar', revision], { cwd: source });
  const sourceHash = createHash('sha256');
  const files = new Map();
  async function* hashed() {
    for await (const chunk of git.process.stdout) { sourceHash.update(chunk); yield chunk; }
  }
  try {
    await readTar(hashed(), async (entry, data) => {
      require(['0', '7', '5'].includes(entry.type), 'Release source must not contain links');
      if (['0', '7'].includes(entry.type)) {
        const chunks = [];
        for await (const chunk of data) chunks.push(chunk);
        files.set(entry.name, Buffer.concat(chunks));
      }
    });
    await git.done;
  } finally { if (git.process.exitCode === null) git.process.kill(); }
  const context = createHash('sha256');
  // Compare Unicode code points, as Python sorted(str) does, rather than UTF-16.
  const ordered = [...files].sort(([a], [b]) => Buffer.compare(Buffer.from(a), Buffer.from(b)));
  for (const [name, data] of ordered) context.update(name).update('\0').update(data).update('\0');
  const expected = { source_sha256: sourceHash.digest('hex'), context_sha256: context.digest('hex'), lock_sha256: hash(files.get('Cargo.lock')), dockerfile_sha256: hash(files.get('Dockerfile')), toolchain: utf8(files.get('.tool-versions')), base_images: [...utf8(files.get('Dockerfile')).matchAll(/^FROM (\S+)/gm)].map(match => match[1]), dirty: false };
  require(/^[0-9]+-[0-9]+(?![\s\S])/.test(runId), 'Invalid release run identity');
  const [run, attempt] = runId.split('-');
  artifact_ids(metadata, run, attempt, revision);
  require(bundles.length === 2, 'Both unique native variants required');
  const receipts = [], children = [];
  for (const [index, platform] of ['linux/amd64', 'linux/arm64'].entries()) {
    const bundle = bundles[index];
    const receipt = await verify(bundle, revision, runId, platform, exceptions);
    const inputs = load(path.join(bundle, 'inputs.json'));
    require(Object.entries(expected).every(([key, value]) => equal(inputs[key], value)), 'Prepared context differs from trusted source');
    receipts.push(receipt);
    const scanner = load(path.join(bundle, 'scanner.json'));
    const artifact = metadata.artifacts.find(item => item.name === `accepted-${platform.split('/')[1]}-${run}-${attempt}`);
    children.push({ platform, manifest_digest: receipt.manifest_digest, config_digest: receipt.config_digest, archive_sha256: receipt.archive_sha256, sidecars: receipt.sidecars, actions_artifact: { id: artifact.id, digest: artifact.digest, name: artifact.name }, scanner: { version: scanner.Version, database: scanner.VulnerabilityDB }, tests: 'application-owned checks succeeded in native job' });
  }
  return { version: 1, scope: 'application container acceptance, not framework production certification', source: { revision, ...expected }, run_id: runId, children, index_digest: 'sha256:' + hash(serialize(runtime_index(receipts))), scan_policy: 'High/Critical including unfixed; only independently trusted scoped exceptions', exceptions };
}

// Native HTTP has no implicit proxy or redirect behavior. Every redirect is rejected.
async function request(url, headers, limit) {
  return new Promise((resolve, reject) => {
    const transport = url.startsWith('https:') ? https : http;
    const req = transport.get(url, { headers, agent: false }, response => {
      if (response.statusCode >= 300 && response.statusCode < 400) {
        response.destroy(); reject(new Error('Registry redirect is not permitted')); return;
      }
      let length = 0;
      const chunks = [];
      response.on('data', chunk => {
        length += chunk.length;
        if (length > limit) response.destroy(new Error('Registry response too large'));
        else chunks.push(chunk);
      });
      response.on('error', reject);
      response.on('end', () => resolve({ status: response.statusCode, raw: Buffer.concat(chunks) }));
    });
    req.setTimeout(30000, () => req.destroy(new Error('Registry request timed out')));
    req.on('error', reject);
  });
}
export async function check_tag(repository, tag, digest, authFile = undefined) {
  require(typeof digest === 'string' && DIGEST.test(digest), 'Invalid expected index digest');
  require(/^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}(?![\s\S])/.test(tag), 'Invalid version tag');
  const separator = repository.indexOf('/');
  const host = repository.slice(0, separator), name = repository.slice(separator + 1);
  require(separator > 0 && name.split('/').every(component => /^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*(?![\s\S])/.test(component)), 'Invalid repository path');
  require(host === 'ghcr.io' || /^127\.0\.0\.1:[0-9]+(?![\s\S])/.test(host), 'Unsupported registry');
  const headers = { Accept: [INDEX, MANIFEST, 'application/vnd.docker.distribution.manifest.list.v2+json', 'application/vnd.docker.distribution.manifest.v2+json'].join(', ') };
  if (host === 'ghcr.io') {
    require(authFile !== undefined, 'GHCR auth file required');
    const auth = load(authFile).auths['ghcr.io'].auth;
    require(typeof auth === 'string' && auth, 'Registry credentials missing');
    const query = new URLSearchParams({ service: host, scope: `repository:${name}:pull` });
    const response = await request(`https://${host}/token?${query}`, { Authorization: 'Basic ' + auth }, 1000000);
    require(response.status === 200, `Registry token check failed: HTTP ${response.status}`);
    const { token } = JSON.parse(response.raw);
    require(typeof token === 'string' && token, 'Registry token missing');
    headers.Authorization = 'Bearer ' + token;
  }
  const response = await request(`${host === 'ghcr.io' ? 'https' : 'http'}://${host}/v2/${name}/manifests/${tag}`, headers, 16000000);
  if (response.status === 404) {
    require(response.raw.length <= 1000000, 'Registry error response too large');
    const errors = JSON.parse(response.raw).errors;
    require(Array.isArray(errors) && errors.length && errors.every(error => ['MANIFEST_UNKNOWN', 'NAME_UNKNOWN'].includes(error.code)), 'Registry did not confirm tag absence');
    return { state: 'absent' };
  }
  require(response.status >= 200 && response.status < 300, `Registry tag check failed: HTTP ${response.status}`);
  require('sha256:' + hash(response.raw) === digest, 'Version tag identifies a different release');
  return { state: 'same-index' };
}

export async function main(argv = process.argv.slice(2)) {
  const [command, ...args] = argv;
  const specs = {
    extract: [2, ['platform'], ['platform']], seal: [1, ['revision', 'run-id', 'platform'], ['revision', 'run-id', 'platform']],
    verify: [1, ['revision', 'run-id', 'platform', 'exceptions', 'scan-policy'], ['revision', 'run-id', 'platform']],
    index: [2, ['revision', 'run-id', 'exceptions', 'scan-policy'], ['revision', 'run-id']],
    pulled: [2, ['platform', 'reference'], ['platform', 'reference']],
    'artifact-ids': [1, ['run-id', 'attempt', 'revision'], ['run-id', 'attempt']],
    'release-record': [4, ['revision', 'run-id', 'exceptions'], ['revision', 'run-id']],
    'tag-check': [3, ['auth-file'], []],
  };
  if (command === '--help' || command === '-h') {
    console.log(`Usage: node verify_artifacts.mjs <${Object.keys(specs).join('|')}> [arguments]`); return;
  }
  require(Object.hasOwn(specs, command ?? ''), 'Unknown or missing command');
  const [count, allowed, required] = specs[command];
  const positions = [], options = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--help' || args[i] === '-h') {
      console.log(`Usage: node verify_artifacts.mjs ${command} <${count} positional arguments> ${required.map(name => `--${name} VALUE`).join(' ')}`); return;
    }
    if (args[i].startsWith('--')) {
      const [name, ...value] = args[i].slice(2).split('=');
      require(allowed.includes(name), `unrecognized arguments: --${name}`);
      if (!value.length) { require(i + 1 < args.length && !args[i + 1].startsWith('--'), `Expected value for --${name}`); options[name] = args[++i]; }
      else options[name] = value.join('=');
    } else positions.push(args[i]);
  }
  require(positions.length === count, `Expected ${count} positional arguments`);
  for (const name of required) require(Object.hasOwn(options, name), `Missing --${name}`);
  if (options.platform !== undefined) require(PLATFORMS.has(options.platform), 'Unsupported platform');
  if (options['scan-policy'] !== undefined) require(['strict', 'report-only'].includes(options['scan-policy']), 'Invalid scan policy');
  const revision = options.revision, run = options['run-id'], platform = options.platform;
  const exceptions = options.exceptions ? load(options.exceptions) : [];
  const policy = options['scan-policy'] ?? 'strict';
  let result;
  if (command === 'artifact-ids') { console.log(artifact_ids(load(positions[0]), run, options.attempt, revision)); return; }
  if (command === 'extract') { await unpack(...positions); result = describe(positions[1], platform); }
  else if (command === 'seal') result = seal(positions[0], revision, run, platform);
  else if (command === 'verify') result = await verify(positions[0], revision, run, platform, exceptions, { policy });
  else if (command === 'pulled') result = verify_pulled(positions[0], load(positions[1]), platform, options.reference);
  else if (command === 'index') result = runtime_index(await Promise.all(positions.map((bundle, i) => verify(bundle, revision, run, ['linux/amd64', 'linux/arm64'][i], exceptions, { policy }))));
  else if (command === 'release-record') result = await release_record(positions[0], positions.slice(1, 3), load(positions[3]), revision, run, exceptions);
  else result = await check_tag(...positions, options['auth-file']);
  process.stdout.write(serialize(result));
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => { console.error(`Artifact verification failed: ${error.message}`); process.exitCode = 1; });
}

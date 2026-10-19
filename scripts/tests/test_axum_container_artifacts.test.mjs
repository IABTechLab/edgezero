import assert from 'node:assert/strict';
import { test } from 'node:test';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';
import { spawnSync } from 'node:child_process';
import http from 'node:http';
import { once } from 'node:events';
import { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';
import * as v from '../../examples/container-publishing/verify_artifacts.mjs';

const ROOT = fileURLToPath(new URL('../../', import.meta.url));
const SCRIPT = path.join(ROOT, 'examples/container-publishing/verify_artifacts.mjs');
const REV = 'a'.repeat(40);
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const put = (file, value) => fs.writeFileSync(file, JSON.stringify(value));
function run(args, options = {}) {
  return spawnSync(process.execPath, [SCRIPT, ...args], { encoding: 'utf8', ...options });
}
function tar(entries, padding = true) {
  const chunks = [];
  for (const { name, type = '0', data = Buffer.alloc(0), size = data.length, link = '', prefix = '' } of entries) {
    const header = Buffer.alloc(512);
    header.write(name, 0, 100);
    header.write('0000644\0', 100, 8);
    header.write('0000000\0', 108, 8);
    header.write('0000000\0', 116, 8);
    header.write(size.toString(8).padStart(11, '0') + '\0', 124, 12);
    header.write('00000000000\0', 136, 12);
    header.fill(32, 148, 156);
    header.write(type, 156, 1);
    header.write(link, 157, 100);
    header.write('ustar\0', 257, 6);
    header.write('00', 263, 2);
    header.write(prefix, 345, 155);
    const checksum = header.reduce((sum, byte) => sum + byte, 0);
    header.write(checksum.toString(8).padStart(6, '0') + '\0 ', 148, 8);
    chunks.push(header, data, Buffer.alloc((512 - data.length % 512) % 512));
  }
  if (padding) chunks.push(Buffer.alloc(1024));
  return Buffer.concat(chunks);
}
function paxRecord(key, value) {
  const text = `${key}=${value}\n`;
  let length = Buffer.byteLength(text) + 2;
  while (Buffer.byteLength(`${length} ${text}`) !== length) length = Buffer.byteLength(`${length} ${text}`);
  return Buffer.from(`${length} ${text}`);
}
function pack(layout, archive) {
  const entries = [];
  function walk(directory, prefix = '') {
    for (const name of fs.readdirSync(directory).sort()) {
      const file = path.join(directory, name), relative = prefix + name;
      if (fs.statSync(file).isDirectory()) { entries.push({ name: relative, type: '5' }); walk(file, relative + '/'); }
      else entries.push({ name: relative, data: fs.readFileSync(file) });
    }
  }
  walk(layout);
  fs.writeFileSync(archive, tar(entries));
}
function fixture(root, arch = 'amd64', revision = REV) {
  const layout = path.join(root, 'oci');
  fs.mkdirSync(path.join(layout, 'blobs/sha256'), { recursive: true });
  function store(value, mediaType) {
    const data = Buffer.isBuffer(value) ? value : Buffer.from(JSON.stringify(value));
    const digest = sha(data);
    fs.writeFileSync(path.join(layout, 'blobs/sha256', digest), data);
    return { mediaType, size: data.length, digest: 'sha256:' + digest };
  }
  const rawLayer = tar([{ name: 'transport-fixture', data: Buffer.from('layer') }]);
  const diffId = 'sha256:' + sha(rawLayer);
  const config = store({ os: 'linux', architecture: arch, rootfs: { type: 'layers', diff_ids: [diffId] }, config: { Labels: { 'org.opencontainers.image.revision': revision } } }, v.CONFIG);
  const layer = store(gzipSync(rawLayer, { mtime: 0 }), 'application/vnd.oci.image.layer.v1.tar+gzip');
  const image = store({ schemaVersion: 2, mediaType: v.MANIFEST, config, layers: [layer] }, v.MANIFEST);
  put(path.join(layout, 'oci-layout'), { imageLayoutVersion: '1.0.0' });
  put(path.join(layout, 'index.json'), { schemaVersion: 2, mediaType: v.INDEX, manifests: [image] });
  pack(layout, path.join(root, 'image.tar'));
  put(path.join(root, 'inputs.json'), { revision, dirty: false, toolchain: 'rust 1.95.0\n', source_sha256: 'a'.repeat(64), context_sha256: 'b'.repeat(64), lock_sha256: 'c'.repeat(64), dockerfile_sha256: 'd'.repeat(64), base_images: ['debian@sha256:' + 'e'.repeat(64)] });
  put(path.join(root, 'scan.json'), { SchemaVersion: 2, Metadata: { ImageID: config.digest, DiffIDs: [diffId], OS: { Family: 'debian' } }, Results: [{ Class: 'os-pkgs', Type: 'debian', Target: 'fixture', Vulnerabilities: [] }] });
  put(path.join(root, 'scanner.json'), { Version: 'test', VulnerabilityDB: { UpdatedAt: '2026-10-07' }, SubjectManifestDigest: image.digest });
  put(path.join(root, 'sbom.json'), { bomFormat: 'CycloneDX', specVersion: '1.6', version: 1, components: [{ type: 'library', name: 'transport-fixture' }], metadata: { component: { type: 'container', name: 'fixture', properties: [{ name: 'aquasecurity:trivy:ImageID', value: config.digest }] } } });
  put(path.join(root, 'provenance.json'), { revision, subject: image.digest, run_id: '123-1', inputs: v.load(path.join(root, 'inputs.json')) });
  return image;
}
function caseTest(name, fn) {
  test(name, async t => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'artifacts-test-'));
    t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const image = fixture(root);
    const seal = () => v.seal(root, REV, '123-1', 'linux/amd64');
    const verify = (revision = REV, run = '123-1', platform = 'linux/amd64') => v.verify(root, revision, run, platform, []);
    const cli = ['verify', root, '--revision', REV, '--run-id', '123-1', '--platform', 'linux/amd64'];
    const arm = () => {
      const directory = path.join(root, 'arm'); fs.mkdirSync(directory); fixture(directory, 'arm64');
      v.seal(directory, REV, '123-1', 'linux/arm64'); return directory;
    };
    await fn({ t, root, image, seal, verify, cli, arm });
  });
}
const metadata = revision => ({ artifacts: ['amd64', 'arm64'].map((arch, index) => ({ id: index + 1, name: `accepted-${arch}-123-1`, expired: false, digest: 'sha256:' + String(index + 1).repeat(64), workflow_run: { id: 123, head_sha: revision } })) });

caseTest('receipt and archive roundtrip', async ({ root, image, seal, verify, cli }) => {
  const receipt = seal();
  assert.deepEqual(await verify(), receipt);
  await v.unpack(path.join(root, 'image.tar'), path.join(root, 'copy'));
  assert.equal(v.describe(path.join(root, 'copy'), 'linux/amd64').manifest_digest, image.digest);
  const result = run(cli); assert.equal(result.status, 0, result.stderr); assert.deepEqual(JSON.parse(result.stdout), receipt);
});
caseTest('arm64', async ({ arm }) => { await v.verify(arm(), REV, '123-1', 'linux/arm64', []); });
caseTest('wrong run source platform and acceptance', async ({ root, seal, verify }) => {
  const receipt = seal();
  for (const args of [['b'.repeat(40)], [REV, '124-1'], [REV, '123-1', 'linux/arm64']]) await assert.rejects(verify(...args));
  receipt.checks = 'failed'; put(path.join(root, 'receipt.json'), receipt); await assert.rejects(verify());
});
caseTest('mutated archive or sidecar', async ({ root, seal, verify }) => {
  seal(); fs.appendFileSync(path.join(root, 'image.tar'), 'changed'); await assert.rejects(verify());
  seal(); put(path.join(root, 'sbom.json'), { bomFormat: 'changed' }); await assert.rejects(verify());
});
caseTest('empty scan wrong subject and dirty source', async ({ root, seal, verify }) => {
  for (const changes of [{ Results: [] }, { Metadata: { ImageID: 'sha256:' + 'f'.repeat(64) } }]) {
    put(path.join(root, 'scan.json'), { ...v.load(path.join(root, 'scan.json')), ...changes }); seal(); await assert.rejects(verify());
  }
  const inputs = v.load(path.join(root, 'inputs.json')); inputs.dirty = true; put(path.join(root, 'inputs.json'), inputs); assert.throws(seal);
});
caseTest('pulled identity on both Docker stores', ({ root }) => {
  const layout = path.join(root, 'oci'), info = v.describe(layout, 'linux/amd64');
  const config = v.load(path.join(layout, 'blobs/sha256', info.config_digest.slice(7)));
  const reference = '127.0.0.1/app@' + info.manifest_digest;
  const inspection = { Id: info.manifest_digest, RepoDigests: [reference], Os: 'linux', Architecture: 'amd64', Config: config.config, RootFS: { Layers: config.rootfs.diff_ids }, Descriptor: { digest: info.manifest_digest } };
  v.verify_pulled(layout, [inspection], 'linux/amd64', reference);
  inspection.Id = info.config_digest; delete inspection.Descriptor;
  v.verify_pulled(layout, [inspection], 'linux/amd64', reference);
  inspection.RootFS.Layers = []; assert.throws(() => v.verify_pulled(layout, [inspection], 'linux/amd64', reference));
});
caseTest('invalid base pins and symlink sidecars', async ({ root, seal, verify }) => {
  const original = v.load(path.join(root, 'inputs.json'));
  for (const bases of ['not-a-list', [], ['debian@sha256:garbage'], [5]]) assert.throws(() => v.check_inputs({ ...original, base_images: bases }, REV, '123-1'));
  seal(); fs.renameSync(path.join(root, 'sbom.json'), path.join(root, 'other.json')); fs.symlinkSync(path.join(root, 'other.json'), path.join(root, 'sbom.json')); await assert.rejects(verify());
});
caseTest('SBOM wrong subject and scan wrong filesystem', async ({ root, seal, verify }) => {
  seal(); const bill = v.load(path.join(root, 'sbom.json')); bill.metadata.component.properties[0].value = 'sha256:' + 'f'.repeat(64);
  put(path.join(root, 'sbom.json'), bill); seal(); await assert.rejects(verify());
  const report = v.load(path.join(root, 'scan.json')); report.Metadata.DiffIDs = []; put(path.join(root, 'scan.json'), report); seal(); await assert.rejects(verify());
});
caseTest('missing config filesystem identity fails closed', async ({ root, image, seal, verify }) => {
  const layout = path.join(root, 'oci'), directory = path.join(layout, 'blobs/sha256');
  const manifest = v.load(path.join(directory, image.digest.slice(7)));
  const config = v.load(path.join(directory, manifest.config.digest.slice(7)));
  delete config.rootfs.diff_ids;
  function replace(value, mediaType) {
    const data = Buffer.from(JSON.stringify(value)), digest = sha(data);
    fs.writeFileSync(path.join(directory, digest), data);
    return { mediaType, digest: 'sha256:' + digest, size: data.length };
  }
  manifest.config = replace(config, v.CONFIG);
  const nextImage = replace(manifest, v.MANIFEST);
  put(path.join(layout, 'index.json'), { schemaVersion: 2, mediaType: v.INDEX, manifests: [nextImage] });
  const report = v.load(path.join(root, 'scan.json')); report.Metadata.ImageID = manifest.config.digest; delete report.Metadata.DiffIDs; put(path.join(root, 'scan.json'), report);
  const scanner = v.load(path.join(root, 'scanner.json')); scanner.SubjectManifestDigest = nextImage.digest; put(path.join(root, 'scanner.json'), scanner);
  const bill = v.load(path.join(root, 'sbom.json')); bill.metadata.component.properties[0].value = manifest.config.digest; put(path.join(root, 'sbom.json'), bill);
  const provenance = v.load(path.join(root, 'provenance.json')); provenance.subject = nextImage.digest; put(path.join(root, 'provenance.json'), provenance);
  pack(layout, path.join(root, 'image.tar')); seal(); await assert.rejects(verify(), /Scan\/filesystem mismatch/);
  const reference = '127.0.0.1/app@' + nextImage.digest;
  assert.throws(() => v.verify_pulled(layout, [{ RepoDigests: [reference], Os: 'linux', Architecture: 'amd64', Config: config.config, RootFS: {} }], 'linux/amd64', reference), /Pulled filesystem identity mismatch/);
});
caseTest('index CLI rejects unverified receipts and mutated bundles', ({ root, seal, arm }) => {
  seal(); const other = arm();
  const command = ['index', root, other, '--revision', REV, '--run-id', '123-1'];
  const result = run(command); assert.equal(result.status, 0, result.stderr); assert.equal(JSON.parse(result.stdout).manifests.length, 2);
  put(path.join(other, 'scan.json'), {}); assert.notEqual(run(command).status, 0);
});
caseTest('artifact selection binds attempt source and digest', () => {
  const data = metadata(REV); assert.equal(v.artifact_ids(data, '123', '1', REV), '1,2');
  for (const args of [['124', '1', REV], ['123', '2', REV], ['123', '1', 'b'.repeat(40)]]) assert.throws(() => v.artifact_ids(data, ...args));
  data.artifacts[0].digest = null; assert.throws(() => v.artifact_ids(data, '123', '1', REV));
  const incomplete = metadata(REV); delete incomplete.artifacts[0].expired; assert.throws(() => v.artifact_ids(incomplete, '123', '1', REV), /Invalid artifact identity/);
});
test('registry tag preflight fails closed using real HTTP', async t => {
  let status = 200, body = Buffer.from('index'), redirect;
  const server = http.createServer((request, response) => { response.writeHead(status, redirect ? { Location: redirect } : {}); response.end(body); });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => { server.closeAllConnections(); server.close(); });
  const repository = `127.0.0.1:${server.address().port}/app`, expected = 'sha256:' + sha(body);
  assert.deepEqual(await v.check_tag(repository, 'fixture', expected), { state: 'same-index' });
  await assert.rejects(v.check_tag(repository, 'fixture', 'sha256:' + 'f'.repeat(64)));
  for (const [code, value, succeeds] of [[404, { errors: [{ code: 'MANIFEST_UNKNOWN' }] }, true], [404, { errors: [{ code: 'NAME_UNKNOWN' }] }, true], [404, { errors: [{ code: 'UNAUTHORIZED' }] }, false], [404, { errors: [] }, false], [401, {}, false], [429, {}, false], [500, {}, false]]) {
    status = code; body = Buffer.from(JSON.stringify(value));
    if (succeeds) assert.deepEqual(await v.check_tag(repository, 'fixture', expected), { state: 'absent' });
    else await assert.rejects(v.check_tag(repository, 'fixture', expected));
  }
  status = 302; redirect = 'http://127.0.0.1:1/steal'; await assert.rejects(v.check_tag(repository, 'fixture', expected), /redirect/);
  redirect = undefined; status = 404; body = Buffer.from('not json'); await assert.rejects(v.check_tag(repository, 'fixture', expected));
  status = 200; body = Buffer.alloc(16000001); await assert.rejects(v.check_tag(repository, 'fixture', expected), /too large/);
  for (const repo of ['example.com/app', '127.0.0.1:5000/../app', '127.0.0.1:5000/app?query', 'ghcr.io/owner/APP']) await assert.rejects(v.check_tag(repo, 'fixture', expected));
  await assert.rejects(v.check_tag('ghcr.io/owner/app', 'fixture', expected), /auth file/);
  await assert.rejects(v.check_tag(repository, 'fixture\n', expected), /Invalid version tag/);
  await assert.rejects(v.check_tag(repository + '\n', 'fixture', expected), /Invalid repository path/);
  await assert.rejects(v.check_tag(repository, 'fixture', expected + '\n'), /Invalid expected index digest/);
  server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  await assert.rejects(v.check_tag(repository, 'fixture', expected));
});
test('registry preflight accepts valid image-name separators and rejects malformed paths', async t => {
  const body = Buffer.from('index'), expected = 'sha256:' + sha(body), requests = [];
  const server = http.createServer((request, response) => { requests.push(request.url); response.end(body); });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => { server.closeAllConnections(); server.close(); });
  const host = `127.0.0.1:${server.address().port}`;
  for (const name of ['owner/my-app', 'owner/my--app', 'owner/my---app', 'owner/my__app', 'owner/my.app_name', 'owner/my--app/nested__image']) {
    assert.deepEqual(await v.check_tag(`${host}/${name}`, 'v1.0.0', expected), { state: 'same-index' }, name);
    assert.equal(requests.at(-1), `/v2/${name}/manifests/v1.0.0`);
    await assert.rejects(v.check_tag(`ghcr.io/${name}`, 'v1.0.0', expected), /GHCR auth file required/, name);
  }
  const acceptedRequests = requests.length;
  for (const name of ['/app', 'app/', 'owner//app', '.app', 'app.', 'app..name', 'app___name', 'app._name', '-app', 'app-', 'owner/APP', 'owner/app\n', 'owner/app?query', 'owner/../app']) {
    await assert.rejects(v.check_tag(`${host}/${name}`, 'v1.0.0', expected), /Invalid repository path/, name);
    await assert.rejects(v.check_tag(`ghcr.io/${name}`, 'v1.0.0', expected), /Invalid repository path/, name);
  }
  assert.equal(requests.length, acceptedRequests, 'malformed paths must not reach the registry');
});
caseTest('release record recomputes committed source', async ({ root }) => {
  const source = path.join(root, 'source'); fs.mkdirSync(source);
  const contents = { 'Cargo.lock': '# locked\n', '.tool-versions': 'rust 1.95.0\n', Dockerfile: 'FROM debian@sha256:' + 'e'.repeat(64) + '\n', 'a/b': 'nested', 'a-extra/c': 'ordering', 'unicode/é': 'UTF-8', 'unicode/\ue000': 'BMP scalar', 'unicode/\u{10000}': 'astral scalar' };
  for (const [name, text] of Object.entries(contents)) { const file = path.join(source, name); fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, text); }
  function git(...args) { const result = spawnSync('git', ['-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgsign=false', ...args], { cwd: source }); assert.equal(result.status, 0, result.stderr.toString()); return result.stdout; }
  git('init', '-q'); git('add', '.'); git('-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'fixture');
  const revision = git('rev-parse', 'HEAD').toString().trim(), archive = git('archive', '--format=tar', revision);
  const context = createHash('sha256');
  for (const name of ['.tool-versions', 'Cargo.lock', 'Dockerfile', 'a-extra/c', 'a/b', 'unicode/é', 'unicode/\ue000', 'unicode/\u{10000}']) context.update(name).update('\0').update(contents[name]).update('\0');
  const contextDigest = context.digest('hex'), bundles = [];
  for (const arch of ['amd64', 'arm64']) {
    const bundle = path.join(root, 'release-' + arch); fs.mkdirSync(bundle); fixture(bundle, arch, revision);
    const inputs = { ...v.load(path.join(bundle, 'inputs.json')), source_sha256: sha(archive), context_sha256: contextDigest, lock_sha256: v.checksum(path.join(source, 'Cargo.lock')), dockerfile_sha256: v.checksum(path.join(source, 'Dockerfile')) };
    put(path.join(bundle, 'inputs.json'), inputs);
    put(path.join(bundle, 'provenance.json'), { ...v.load(path.join(bundle, 'provenance.json')), inputs });
    v.seal(bundle, revision, '123-1', 'linux/' + arch); bundles.push(bundle);
  }
  const data = metadata(revision);
  const record = await v.release_record(source, bundles, data, revision, '123-1', []);
  assert.equal(record.source.source_sha256, sha(archive)); assert.equal(record.source.context_sha256, contextDigest);
  assert.equal(record.source.lock_sha256, v.checksum(path.join(source, 'Cargo.lock'))); assert.equal(record.children.length, 2);
  const indexResult = run(['index', ...bundles, '--revision', revision, '--run-id', '123-1']); assert.equal(indexResult.status, 0, indexResult.stderr);
  assert.equal(record.index_digest, 'sha256:' + sha(indexResult.stdout));
  // A dirty working tree must not replace the committed source used above.
  fs.writeFileSync(path.join(source, 'Cargo.lock'), 'dirty working tree');
  assert.equal((await v.release_record(source, bundles, data, revision, '123-1', [])).source.lock_sha256, sha(contents['Cargo.lock']));
  const report = v.load(path.join(bundles[0], 'scan.json'));
  report.Results[0].Vulnerabilities = [{ Severity: 'HIGH', VulnerabilityID: 'CVE-test', PkgName: 'lib-test', FixedVersion: '' }];
  put(path.join(bundles[0], 'scan.json'), report); v.seal(bundles[0], revision, '123-1', 'linux/amd64');
  await assert.rejects(v.release_record(source, bundles, data, revision, '123-1', []), /Blocked HIGH/);
  report.Results[0].Vulnerabilities = []; put(path.join(bundles[0], 'scan.json'), report);
  const inputs = v.load(path.join(bundles[0], 'inputs.json')); inputs.lock_sha256 = 'f'.repeat(64);
  put(path.join(bundles[0], 'inputs.json'), inputs); put(path.join(bundles[0], 'provenance.json'), { ...v.load(path.join(bundles[0], 'provenance.json')), inputs });
  v.seal(bundles[0], revision, '123-1', 'linux/amd64'); await assert.rejects(v.release_record(source, bundles, data, revision, '123-1', []), /Prepared context/);
  fs.symlinkSync('Cargo.lock', path.join(source, 'link')); git('add', 'link'); git('-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'link');
  await assert.rejects(v.release_record(source, bundles, data, git('rev-parse', 'HEAD').toString().trim(), '123-1', []), /must not contain links/);
});
caseTest('blob digest and size', ({ root, image }) => {
  const file = v.blob(path.join(root, 'oci'), image, new Set([v.MANIFEST])); fs.writeFileSync(file, 'changed'); assert.throws(() => v.describe(path.join(root, 'oci'), 'linux/amd64'));
});
caseTest('no attestations in runtime index', ({ root, image }) => {
  put(path.join(root, 'oci/index.json'), { schemaVersion: 2, mediaType: v.INDEX, manifests: [image, image] }); assert.throws(() => v.describe(path.join(root, 'oci'), 'linux/amd64'));
});
caseTest('config platform is authority', ({ root }) => { assert.throws(() => v.describe(path.join(root, 'oci'), 'linux/arm64')); });
caseTest('provenance subject', ({ root, seal }) => { put(path.join(root, 'provenance.json'), { revision: REV, subject: 'sha256:' + 'b'.repeat(64) }); assert.throws(seal); });
caseTest('index requires two same-run variants', ({ seal }) => {
  const one = seal(), two = { ...one, platform: 'linux/arm64', manifest_digest: 'sha256:' + 'c'.repeat(64) };
  assert.deepEqual(new Set(v.runtime_index([one, two]).manifests.map(manifest => manifest.platform.architecture)), new Set(['amd64', 'arm64']));
  for (const receipts of [[one], [one, one], [one, { ...two, run_id: '124-1' }], [one, { ...two, sidecars: { ...two.sidecars, 'inputs.json': 'f'.repeat(64) } }]]) assert.throws(() => v.runtime_index(receipts));
});
caseTest('unsafe archive members', async ({ root }) => {
  let index = 0;
  for (const [name, type] of [['../escape', '0'], ['/absolute', '0'], ['blobs/sha256/' + 'a'.repeat(64), '2'], ['index.json', '1'], ['script.sh', '0'], ['index.json', '6'], ['index.json', '3'], ['index.json', '4'], ['index.json', 'S'], ['blobs//sha256/' + 'a'.repeat(64), '0'], ['blobs/./sha256/' + 'a'.repeat(64), '0']]) {
    const archive = path.join(root, 'unsafe.tar'); fs.writeFileSync(archive, tar([{ name, type, link: '/escape' }]));
    await assert.rejects(v.unpack(archive, path.join(root, 'unsafe-' + index++)), undefined, `${name} ${type}`);
  }
});
caseTest('duplicate archive member', async ({ root }) => {
  const archive = path.join(root, 'unsafe.tar'); fs.writeFileSync(archive, tar([{ name: 'index.json', data: Buffer.from('{}') }, { name: 'index.json', data: Buffer.from('{}') }]));
  await assert.rejects(v.unpack(archive, path.join(root, 'unsafe')), /Duplicate/);
});
caseTest('scan policy includes unfixed', ({ image }) => {
  const report = { SchemaVersion: 2, Results: [{ Vulnerabilities: [{ Severity: 'HIGH', VulnerabilityID: 'CVE-test', PkgName: 'lib-test', FixedVersion: '' }] }] };
  assert.throws(() => v.scan_policy(report, [], image.digest));
  const exception = { manifest_digest: image.digest, vulnerability: 'CVE-test', package: 'lib-test', owner: 'operator', reason: 'reviewed risk', expires: '2099-01-01' };
  v.scan_policy(report, [exception], image.digest);
  for (const change of [{ expires: '2000-01-01' }, { reason: '' }, { package: '*' }, { typo: 'value' }, { expires: '2099-02-30' }, { manifest_digest: 'sha256:' + 'd'.repeat(64) }]) assert.throws(() => v.scan_policy(report, [{ ...exception, ...change }], image.digest));
  assert.throws(() => v.scan_policy(report, [exception, exception], image.digest), /Duplicate/);
  assert.throws(() => v.scan_policy({}, [], image.digest));
});
caseTest('report-only scan does not approve release', async ({ t, root, seal, verify, cli, arm }) => {
  const report = v.load(path.join(root, 'scan.json'));
  report.Results[0].Vulnerabilities = [['HIGH', ''], ['CRITICAL', '2.0']].map(([Severity, FixedVersion]) => ({ Severity, FixedVersion, VulnerabilityID: 'CVE-' + Severity, PkgName: 'lib-test' }));
  put(path.join(root, 'scan.json'), report); const receipt = seal();
  const warning = t.mock.method(console, 'error', () => {});
  assert.deepEqual(await v.verify(root, REV, '123-1', 'linux/amd64', [], { policy: 'report-only' }), receipt);
  assert.match(warning.mock.calls[0].arguments[0], /not approved for release/);
  await assert.rejects(verify(), /Blocked HIGH/);
  assert.notEqual(run(cli, { env: { ...process.env, SCAN_POLICY: 'report-only' } }).status, 0);
  const result = run([...cli, '--scan-policy', 'report-only']); assert.equal(result.status, 0, result.stderr); assert.deepEqual(JSON.parse(result.stdout), receipt);
  const other = arm(), index = ['index', root, other, '--revision', REV, '--run-id', '123-1'];
  assert.notEqual(run(index).status, 0); assert.equal(run([...index, '--scan-policy', 'report-only']).status, 0);
  const release = run(['release-record', root, root, other, path.join(root, 'metadata.json'), '--revision', REV, '--run-id', '123-1', '--scan-policy', 'report-only']);
  assert.notEqual(release.status, 0); assert.match(release.stderr, /unrecognized arguments/);
});
caseTest('report-only preserves scan validation', async ({ t, root, image, seal, cli }) => {
  t.mock.method(console, 'error', () => {});
  const original = v.load(path.join(root, 'scan.json'));
  for (const change of [{ Results: [] }, { SchemaVersion: 1 }, { Metadata: { ...original.Metadata, ImageID: 'sha256:' + 'f'.repeat(64) } }, { Metadata: { ...original.Metadata, DiffIDs: [] } }, { Results: [{ Class: 'unsupported', Target: 'fixture' }] }, { Results: [{ ...original.Results[0], Target: [] }] }, { Results: [{ ...original.Results[0], Vulnerabilities: [{ Severity: 'INVALID', VulnerabilityID: 'CVE-test', PkgName: 'lib-test' }] }] }]) {
    put(path.join(root, 'scan.json'), { ...original, ...change }); seal(); await assert.rejects(v.verify(root, REV, '123-1', 'linux/amd64', [], { policy: 'report-only' }));
  }
  for (const Severity of ['HIGH', 'CRITICAL']) {
    for (const field of ['VulnerabilityID', 'PkgName']) {
      const finding = { Severity, VulnerabilityID: 'CVE-test', PkgName: 'lib-test' }; delete finding[field];
      const report = { ...original, Results: [{ ...original.Results[0], Vulnerabilities: [finding] }] };
      put(path.join(root, 'scan.json'), report); seal(); await assert.rejects(v.verify(root, REV, '123-1', 'linux/amd64', [], { policy: 'report-only' }));
      const result = run([...cli, '--scan-policy', 'report-only']); assert.notEqual(result.status, 0); assert.equal(result.stdout, '');
      for (const invalid of [null, '', ' ', []]) {
        const bad = { ...finding, VulnerabilityID: 'CVE-test', PkgName: 'lib-test', [field]: invalid };
        const badReport = { ...original, Results: [{ ...original.Results[0], Vulnerabilities: [bad] }] };
        for (const policy of ['strict', 'report-only']) assert.throws(() => v.scan_policy(badReport, [], image.digest, undefined, { policy }), /Invalid vulnerability\/package identity/);
      }
    }
  }
  assert.throws(() => v.scan_policy(original, [], image.digest, undefined, { policy: 'typo' }), /Invalid scan policy/);
  assert.throws(() => v.scan_policy(original, [{ manifest_digest: image.digest, vulnerability: 'CVE-test', package: 'lib-test', owner: 'operator', reason: 'reviewed risk', expires: '2000-01-01' }], image.digest, undefined, { policy: 'report-only' }), /Expired scan exception/);
});
caseTest('GHCR publication rejects report-only before side effects', ({ root }) => {
  const output = path.join(root, 'published');
  const command = [path.join(ROOT, 'examples/container-publishing/publish.sh'), 'ghcr.io/owner/app', 'v1.0.0', root, root, output, REV, '123-1'];
  const result = spawnSync('bash', command, { encoding: 'utf8', env: { ...process.env, SCAN_POLICY: 'report-only' } });
  assert.notEqual(result.status, 0); assert.match(result.stderr, /GHCR publication requires strict scan policy/); assert.equal(fs.existsSync(output), false);
  for (const args of [command, [path.join(ROOT, 'scripts/test_axum_container_handoff.sh'), 'build', root, output, 'linux/amd64', REV, '123-1']]) {
    const invalid = spawnSync('bash', args, { encoding: 'utf8', env: { ...process.env, SCAN_POLICY: 'typo' } });
    assert.notEqual(invalid.status, 0); assert.match(invalid.stderr, /Invalid scan policy/); assert.equal(fs.existsSync(output), false);
  }
});
caseTest('smoke borrowed image not built or deleted', ({ root }) => {
  const app = path.join(root, 'app'); fs.mkdirSync(app);
  for (const name of ['Dockerfile', '.dockerignore', 'Cargo.lock', '.tool-versions']) fs.writeFileSync(path.join(app, name), '');
  fs.writeFileSync(path.join(app, 'edgezero.toml'), '[app]\nname="test-app"\n'); fs.writeFileSync(path.join(app, 'Cargo.toml'), '[workspace.dependencies]\n');
  const binaries = path.join(root, 'bin'); fs.mkdirSync(binaries); const log = path.join(root, 'docker-calls');
  fs.writeFileSync(path.join(binaries, 'docker'), `#!/usr/bin/env bash\nprintf '%s\\n' "$*" >> "$CALLS"\ncase "$*" in\n  'info --format '*) echo x86_64 ;;\n  'image inspect supplied-image --format '*) echo linux/arm64 ;;\nesac\n`, { mode: 0o755 });
  const result = spawnSync('bash', [path.join(ROOT, 'scripts/test_generated_axum_container.sh'), app, '--image', 'supplied-image', '--platform', 'linux/amd64'], { encoding: 'utf8', env: { ...process.env, PATH: binaries + ':' + process.env.PATH, CALLS: log } });
  assert.notEqual(result.status, 0); assert.match(result.stderr, /Image\/platform mismatch/);
  const calls = fs.readFileSync(log, 'utf8'); assert.doesNotMatch(calls, /image rm supplied-image/); assert.doesNotMatch(calls, /-t supplied-image/);
});

caseTest('archive compressed formats PAX paths and GNU long names', async ({ root, image }) => {
  const raw = fs.readFileSync(path.join(root, 'image.tar'));
  fs.writeFileSync(path.join(root, 'compressed.tar.gz'), gzipSync(raw));
  await v.unpack(path.join(root, 'compressed.tar.gz'), path.join(root, 'gzip'));
  assert.equal(v.describe(path.join(root, 'gzip'), 'linux/amd64').manifest_digest, image.digest);
  for (const binary of ['bzip2', 'xz']) {
    const compressed = spawnSync(binary, ['-c'], { input: raw }); assert.equal(compressed.status, 0, compressed.stderr.toString());
    const archive = path.join(root, binary + '.tar'); fs.writeFileSync(archive, compressed.stdout);
    await v.unpack(archive, path.join(root, binary)); assert.equal(v.describe(path.join(root, binary), 'linux/amd64').manifest_digest, image.digest);
  }
  let i = 0;
  for (const entries of [[{ name: 'pax', type: 'x', data: paxRecord('path', 'index.json') }, { name: 'ignored', data: Buffer.from('{}') }], [{ name: 'long', type: 'L', data: Buffer.from('index.json\0') }, { name: 'ignored', data: Buffer.from('{}') }]]) {
    const archive = path.join(root, 'extended.tar'); fs.writeFileSync(archive, tar(entries)); const destination = path.join(root, 'extended-' + i++);
    await v.unpack(archive, destination); assert.deepEqual(v.load(path.join(destination, 'index.json')), {});
  }
});
caseTest('archive rejects PAX traversal sparse files limits and truncation', async ({ root }) => {
  const entries = [
    [{ name: 'pax', type: 'x', data: paxRecord('path', '../escape') }, { name: 'index.json' }],
    [{ name: 'pax', type: 'x', data: paxRecord('GNU.sparse.map', '0,1') }, { name: 'index.json' }],
    [{ name: 'pax', type: 'x', data: paxRecord('linkpath', '/escape') }, { name: 'index.json' }],
    [{ name: 'pax', type: 'x', data: paxRecord('size', '2\n') }, { name: 'index.json', data: Buffer.from('{}') }],
    [{ name: 'index.json', size: 4 * 1024 ** 3 + 1 }],
    [{ name: 'pax', type: 'x', size: 1024 * 1024 + 1 }],
    [{ name: 'blobs', type: '5' }, { name: 'blobs/', type: '5' }],
  ];
  let i = 0;
  for (const records of entries) {
    const archive = path.join(root, 'invalid.tar'); fs.writeFileSync(archive, tar(records)); await assert.rejects(v.unpack(archive, path.join(root, 'invalid-' + i++)));
  }
  const header = tar([{ name: 'index.json', data: Buffer.from('{}') }]);
  fs.writeFileSync(path.join(root, 'truncated.tar'), header.subarray(0, 513)); await assert.rejects(v.unpack(path.join(root, 'truncated.tar'), path.join(root, 'truncated')), /Truncated/);
  header[0] ^= 1; fs.writeFileSync(path.join(root, 'checksum.tar'), header); await assert.rejects(v.unpack(path.join(root, 'checksum.tar'), path.join(root, 'checksum')), /checksum/);
  const compressed = gzipSync(fs.readFileSync(path.join(root, 'image.tar'))); fs.writeFileSync(path.join(root, 'truncated.gz'), compressed.subarray(0, compressed.length - 8));
  await assert.rejects(v.unpack(path.join(root, 'truncated.gz'), path.join(root, 'truncated-gzip')));
  const many = Array.from({ length: 10001 }, (_, index) => ({ name: 'blobs/sha256/' + index.toString(16).padStart(64, '0') }));
  fs.writeFileSync(path.join(root, 'many.tar'), tar(many)); await assert.rejects(v.unpack(path.join(root, 'many.tar'), path.join(root, 'many')), /Too many/);
});
caseTest('descriptor and file boundary validation', ({ root, image, seal }) => {
  const layout = path.join(root, 'oci');
  for (const change of [{ digest: 'sha512:' + 'a'.repeat(64) }, { size: true }, { size: -1 }, { size: 1.5 }, { mediaType: v.INDEX }, { digest: image.digest + '\n' }]) assert.throws(() => v.blob(layout, { ...image, ...change }, new Set([v.MANIFEST])));
  const inputs = v.load(path.join(root, 'inputs.json'));
  assert.throws(() => v.check_inputs({ ...inputs, dirty: 'false' }, REV, '123-1'));
  const file = path.join(root, 'large.json'), fd = fs.openSync(file, 'w'); fs.ftruncateSync(fd, 16 * 1024 * 1024 + 1); fs.closeSync(fd); assert.throws(() => v.load(file), /too large/);
  const invalid = path.join(root, 'invalid-utf8.json'); fs.writeFileSync(invalid, Buffer.from([123, 34, 120, 34, 58, 34, 255, 34, 125])); assert.throws(() => v.load(invalid));
  fs.writeFileSync(invalid, '\ufeff{}'); assert.throws(() => v.load(invalid));
  seal(); assert.throws(() => v.seal(root, REV + '\n', '123-1', 'linux/amd64'), /Expected immutable source commit/);
  assert.throws(() => v.seal(root, REV, '123-1\n', 'linux/amd64'), /Invalid run identity/);
  assert.equal(v.DIGEST.test(image.digest + '\n'), false);
  assert.throws(() => v.blob(layout, { ...image, digest: image.digest + '\n' }, new Set([v.MANIFEST])), /Unsupported blob digest/);
  assert.throws(() => v.check_inputs({ ...inputs, lock_sha256: inputs.lock_sha256 + '\n' }, REV, '123-1'), /Input hash/);
  assert.throws(() => v.check_inputs({ ...inputs, base_images: [inputs.base_images[0] + '\n'] }, REV, '123-1'), /Pinned base/);
  const data = metadata(REV); data.artifacts[0].digest += '\n'; assert.throws(() => v.artifact_ids(data, '123', '1', REV), /digest missing/);
});

test('shared tar reader drains partial payloads and reports links without extraction', async () => {
  const raw = tar([
    { name: 'app', data: Buffer.alloc(1024 * 1024, 65) },
    { name: 'hardlink', type: '1', link: 'app' },
    { name: 'pax', type: 'x', data: paxRecord('linkpath', 'app') },
    { name: 'symlink', type: '2', link: 'ignored' },
    { name: 'long', type: 'K', data: Buffer.from('app\0') },
    { name: 'gnu-link', type: '2' },
    { name: 'last', data: Buffer.from('end') },
  ]);
  const parts = [];
  for (let offset = 0; offset < raw.length; offset += 123) parts.push(raw.subarray(offset, offset + 123));
  const records = [];
  await v.readTar(Readable.from(parts), async (member, chunks) => {
    const prefix = [];
    for await (const chunk of chunks) { prefix.push(chunk.subarray(0, 20)); break; }
    records.push({ ...member, prefix: Buffer.concat(prefix).toString() });
  });
  assert.deepEqual(records.map(record => record.name), ['app', 'hardlink', 'symlink', 'gnu-link', 'last']);
  assert.equal(records[0].prefix, 'A'.repeat(20));
  for (const record of records.slice(1, 4)) assert.equal(record.linkname, 'app');
  assert.equal(records.at(-1).prefix, 'end');
});

test('index serialization matches Python indent=2 bytes', () => {
  const receipts = ['amd64', 'arm64'].map((arch, index) => ({ platform: 'linux/' + arch, revision: REV, run_id: '123-1', sidecars: { 'inputs.json': 'same' }, manifest_digest: 'sha256:' + String(index + 1).repeat(64), manifest_size: 123 + index }));
  const expected = `{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.index.v1+json",
  "manifests": [
    {
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "digest": "sha256:${'1'.repeat(64)}",
      "size": 123,
      "platform": {
        "os": "linux",
        "architecture": "amd64"
      }
    },
    {
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "digest": "sha256:${'2'.repeat(64)}",
      "size": 124,
      "platform": {
        "os": "linux",
        "architecture": "arm64"
      }
    }
  ]
}
`;
  assert.equal(v.serialize(v.runtime_index(receipts.reverse())), expected);
  assert.equal(v.serialize({ text: 'é😀\x7f' }), '{\n  "text": "\\u00e9\\ud83d\\ude00\\u007f"\n}\n');
});

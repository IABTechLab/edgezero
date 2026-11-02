import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
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

for (const [label, source] of [
  ['framework', readFileSync(new URL('.github/workflows/axum-container.yml', root), 'utf8')],
  ['application', workflow],
]) {
  test(`${label} artifact selection includes later pages and rejects cross-page duplicates`, t => {
    const directory = scratch(t);
    const first = { artifacts: Array.from({ length: 100 }, (_, index) => ({ name: `unrelated-${index}` })) };
    const accepted = ['amd64', 'arm64'].map((arch, index) => ({
      id: index + 1, name: `accepted-${arch}-123-1`, expired: false,
      digest: 'sha256:' + String(index + 1).repeat(64),
      workflow_run: { id: 123, head_sha: 'a'.repeat(40) },
    }));
    const pages = join(directory, 'pages.json'), firstPage = join(directory, 'first.json');
    writeFileSync(firstPage, JSON.stringify(first));
    const step = source.slice(source.indexOf('        id: artifacts\n'));
    const selection = step.slice(step.indexOf('        run: |\n') + '        run: |\n'.length,
      step.indexOf('      - uses: actions/download-artifact@'));
    const script = selection.replace(/^          /gm, '');
    assert.match(script, /artifact-ids/);
    function select(duplicate = false, apiFailure = false) {
      writeFileSync(pages, JSON.stringify([first, { artifacts: [accepted[0]] },
        { artifacts: [accepted[1], ...(duplicate ? [{ ...accepted[0], id: 3 }] : [])] }]));
      writeFileSync(join(directory, 'output'), '');
      return spawnSync('bash', ['-euo', 'pipefail', '-c', `
        gh() {
          if [[ " $* " == *' --paginate '* && " $* " == *' --slurp '* ]]; then
            cat "$PAGES"
          else
            cat "$FIRST_PAGE"
          fi
          if [[ "$API_FAILURE" == true ]]; then return 1; fi
        }
        ${script}
      `], {
        cwd: root, encoding: 'utf8', timeout: 5000,
        env: { ...process.env, PAGES: pages, FIRST_PAGE: firstPage, API_FAILURE: String(apiFailure), RUNNER_TEMP: directory,
          GITHUB_REPOSITORY: 'owner/app', GITHUB_RUN_ID: '123', GITHUB_RUN_ATTEMPT: '1',
          GITHUB_SHA: 'a'.repeat(40), GITHUB_OUTPUT: join(directory, 'output') },
      });
    }
    const result = select();
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stderr);
    assert.equal(readFileSync(join(directory, 'output'), 'utf8'), 'ids=1,2\n');
    assert.equal(JSON.parse(readFileSync(join(directory, 'artifacts.json'))).artifacts.length, 102);
    const duplicate = select(true);
    assert.ifError(duplicate.error);
    assert.notEqual(duplicate.status, 0);
    assert.match(duplicate.stderr, /Accepted artifacts missing or duplicated/);
    assert.equal(readFileSync(join(directory, 'output'), 'utf8'), '');
    const failedApi = select(false, true);
    assert.ifError(failedApi.error);
    assert.notEqual(failedApi.status, 0);
    assert.equal(readFileSync(join(directory, 'output'), 'utf8'), '');
  });
}

test('localhost index publication requires registry readiness', () => {
  const source = readFileSync(new URL('.github/workflows/axum-container.yml', root), 'utf8');
  const readiness = source.slice(source.indexOf('          for _ in $(seq 1 30);'),
    source.indexOf('          ./examples/container-publishing/publish.sh'));
  assert.match(readiness, /curl/);
  for (const [failures, ready, calls] of [[0, true, 2], [3, true, 5], [99, false, 31]]) {
    const result = spawnSync('bash', ['-euo', 'pipefail', '-c', `
      port=12345
      calls=0
      curl() { calls=$((calls + 1)); [[ "$calls" -gt ${failures} ]]; }
      sleep() { :; }
      ${readiness}
      printf 'publish-ready %s\\n' "$calls"
    `], { encoding: 'utf8', timeout: 5000 });
    assert.ifError(result.error);
    assert.equal(result.status === 0, ready, result.stderr);
    assert.equal(result.stdout, ready ? `publish-ready ${calls}\n` : '');
  }
});

test('application release uses the validated standalone Node version', () => {
  const version = readFileSync(new URL('.tool-versions', root), 'utf8').match(/^nodejs\s+(\S+)/m)[1];
  const pins = [...workflow.matchAll(/^\s*node-version:\s*['"]?([0-9.]+)['"]?\s*$/gm)];
  assert.ok(pins.length > 0, 'the standalone recipe must declare its Node version');
  for (const [, pin] of pins) assert.equal(pin, version);
});

test('container helper CI has one suite owner and derives framework Node pins', () => {
  const native = readFileSync(new URL('.github/workflows/axum-container.yml', root), 'utf8');
  const actions = readFileSync(new URL('.github/workflows/deploy-action.yml', root), 'utf8');
  assert.doesNotMatch(native, /^\s*-\s+['"]?docs\//m);
  const suite = /\bnode\s+--test\s+scripts\/tests\/test_axum_container\*\.test\.mjs\b/g;
  assert.equal([...native.matchAll(suite)].length, 1);
  const owners = readdirSync(new URL('.github/workflows/', root))
    .filter(name => /\.ya?ml$/.test(name))
    .flatMap(name => [...readFileSync(new URL(`.github/workflows/${name}`, root), 'utf8').matchAll(suite)]
      .map(() => name));
  assert.deepEqual(owners, ['axum-container.yml']);
  assert.match(native, /AXUM_PREPARED_CONTEXT="\$RUNNER_TEMP\/fixture\/container-probe" \\\n\s+node --test scripts\/tests\/test_axum_container\*\.test\.mjs/);
  assert.doesNotMatch(actions, suite);
  for (const source of [native, actions]) {
    assert.doesNotMatch(source, /^\s*node-version:/m);
    assert.match(source, /node-version-file:\s*['"]?\.tool-versions['"]?(?:\s|$)/);
  }
  assert.match(native, /^\s*-\s+['"]?scripts\/package\*\.json['"]?\s*$/m);
});

test('Axum static validation moves with its triggers and retains repository-wide gates', () => {
  const native = readFileSync(new URL('.github/workflows/axum-container.yml', root), 'utf8');
  const actions = readFileSync(new URL('.github/workflows/deploy-action.yml', root), 'utf8');
  const axumPaths = [
    'scripts/*axum_container*.sh', 'scripts/axum_container*.mjs',
    'scripts/tests/*axum_container*.mjs', 'examples/container-publishing/**',
    'examples/axum-deployment/**',
  ];
  const sharedPaths = [
    '.github/actions/deploy-core/tests/check-action-pins.sh', '.github/zizmor.yml',
    'scripts/install-actionlint.sh', 'scripts/install-yq.sh',
  ];
  for (const event of ['pull_request', 'push']) {
    function paths(source) {
      const triggers = source.slice(0, source.indexOf('\npermissions:'));
      const block = triggers.split(`  ${event}:\n`)[1]?.split(/^  \w+:/m)[0];
      assert.ok(block, `missing ${event} trigger`);
      return [...block.matchAll(/^      - (.+)$/gm)].map(([, path]) => path);
    }
    const container = paths(native), deployment = paths(actions);
    for (const path of [...axumPaths, ...sharedPaths, 'scripts/package*.json']) {
      assert.ok(container.includes(path), `${event} container trigger must include ${path}`);
    }
    for (const path of [...axumPaths, 'docs/guide/*axum-containers.md']) {
      assert.ok(!deployment.includes(path), `${event} deployment trigger must omit ${path}`);
    }
    for (const path of ['.github/workflows/**', '.github/actions/**', ...sharedPaths.slice(1)]) {
      assert.ok(deployment.includes(path), `${event} global gate must include ${path}`);
    }
  }

  const prepare = native.slice(native.indexOf('  prepare:'), native.indexOf('\n  native:'));
  for (const command of [
    `"$RUNNER_TEMP/tools/bin/actionlint" -shellcheck='shellcheck -S warning' examples/container-publishing/release.yml`,
    '.github/actions/deploy-core/tests/check-action-pins.sh examples/container-publishing/release.yml',
    '"$RUNNER_TEMP/tools/bin/zizmor" --offline examples/container-publishing/release.yml',
    'shellcheck -e SC1091 scripts/*axum_container*.sh examples/container-publishing/publish.sh',
  ]) {
    assert.ok(prepare.includes(command), `prepare must own ${command}`);
    assert.equal(native.split(command).length - 1, 1, 'static checks must run once');
    assert.ok(!actions.includes(command), 'Axum-only checks must leave the deployment owner');
  }
  assert.ok(prepare.indexOf('Validate the inactive application release recipe') < prepare.indexOf('Prepare one application'));
  for (const source of [native, actions]) {
    for (const pin of ['ACTIONLINT_VERSION: 1.7.7', 'YQ_VERSION: 4.53.3', 'ZIZMOR_VERSION: 1.16.3']) {
      assert.ok(source.includes(pin), `validator pin must remain ${pin}`);
    }
    assert.match(source, /INSTALL_DIR="\$bin" scripts\/install-actionlint\.sh "\$ACTIONLINT_VERSION"/);
    assert.match(source, /INSTALL_DIR="\$bin" scripts\/install-yq\.sh "\$YQ_VERSION"/);
    assert.match(source, /cargo install zizmor --version "\$ZIZMOR_VERSION" --locked/);
  }
  const global = actions.slice(actions.indexOf('  static-checks:'), actions.indexOf('\n  # Exercise source reuse'));
  assert.match(global, /^          "\$RUNNER_TEMP\/tools\/bin\/actionlint" -shellcheck='shellcheck -S warning'$/m);
  assert.match(global, /^          \.github\/actions\/deploy-core\/tests\/check-action-pins\.sh$/m);
  assert.match(global, /"\$RUNNER_TEMP\/tools\/bin\/zizmor" --offline \.github['"]?$/m);
  assert.doesNotMatch(global, /examples\/container-publishing|scripts\/\*axum_container/);
  assert.match(global, /\.github\/actions\/\*\/scripts\/\*\.sh/);
  assert.match(global, /\.github\/actions\/\*\/tests\/\*\.sh/);
  assert.match(global, /\.github\/actions\/deploy-core\/tests\/\*\.sh/);
});

test('Axum guides build in the existing docs format job', () => {
  const source = readFileSync(new URL('.github/workflows/format.yml', root), 'utf8');
  const docs = source.slice(source.indexOf('  format-docs:'));
  assert.match(docs, /working-directory: docs/);
  assert.ok(docs.indexOf('run: npm run build') > docs.indexOf('run: npm run format'));
  assert.ok(docs.indexOf('run: npm run build') > docs.indexOf('run: npm run lint'));
});

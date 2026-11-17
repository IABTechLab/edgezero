import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';

const source = readFileSync(new URL('../test_generated_axum_container.sh', import.meta.url), 'utf8');
const cleanup = source.slice(source.indexOf('cleanup() {'), source.indexOf('trap cleanup EXIT') + 'trap cleanup EXIT'.length);
for (const containers of [[], ['first container', 'second']]) {
  for (const borrowed of [false, true]) {
    for (const status of [0, 37]) {
      test(`generated cleanup preserves exit ${status}, ${containers.length} containers, borrowed image ${borrowed}`, t => {
        const directory = mkdtempSync(join(tmpdir(), 'axum-cleanup-test-'));
        t.after(() => rmSync(directory, { recursive: true, force: true }));
        const work = join(directory, 'work'), evidence = join(directory, 'evidence'), events = join(directory, 'events');
        mkdirSync(join(work, 'tls'), { recursive: true });
        mkdirSync(evidence);
        writeFileSync(join(work, 'tls/ca.key'), 'private test key');
        writeFileSync(join(work, 'tls/server.key'), 'private test key');
        writeFileSync(join(work, 'context.log'), 'context diagnostics');
        const result = spawnSync('bash', ['-euo', 'pipefail', '-c', `
          CONTAINERS=(${containers.map(name => JSON.stringify(name)).join(' ')})
          NETWORK=network; PROBE=probe; IMAGE=app
          docker() {
            printf '<%s>' "$@" >> "$EVENTS"; printf '\\n' >> "$EVENTS"
            if [[ "$1" == logs ]]; then echo 'container diagnostics'; fi
          }
          ${cleanup}
          exit ${status}
        `], { encoding: 'utf8', env: { ...process.env, WORK: work, EVIDENCE: evidence, EVENTS: events,
          BORROWED_IMAGE: borrowed ? 'app' : '' } });
        assert.ifError(result.error);
        assert.equal(result.status, status, result.stderr);
        assert.doesNotMatch(result.stderr, /unbound variable/);
        assert.equal(readFileSync(join(evidence, 'exit-status'), 'utf8'), `${status}\n`);
        assert.equal(readFileSync(join(evidence, 'context.log'), 'utf8'), 'context diagnostics');
        const calls = readFileSync(events, 'utf8');
        for (const container of containers) {
          assert.ok(calls.includes(`<logs><${container}>\n`), calls);
          assert.ok(calls.includes(`<rm><-f><${container}>\n`), calls);
          assert.match(readFileSync(join(evidence, `${container}.log`), 'utf8'), /container diagnostics/);
        }
        assert.ok(calls.includes('<network><rm><network>\n'));
        assert.ok(calls.includes('<image><rm><probe>\n'));
        assert.equal(calls.includes('<image><rm><app>\n'), !borrowed);
        assert.equal(existsSync(work), status !== 0);
        assert.equal(existsSync(join(work, 'tls/ca.key')), false);
        assert.equal(existsSync(join(work, 'tls/server.key')), false);
        if (status !== 0) assert.match(result.stderr, /Failed; diagnostics retained/);
      });
    }
  }
}

const lines = source.split('\n');
const assertions = [];
for (let index = 0; index < lines.length; index++) {
  if (!lines[index].trimStart().startsWith('[[')) continue;
  const statement = [lines[index]];
  while (lines[index].endsWith('\\')) statement.push(lines[++index]);
  const code = statement.join('\n');
  if (/Config\.User|HostConfig\.Tmpfs|"\$status" =~/.test(code)) assertions.push(code);
}

test('generated smoke exercises every UID, restriction, tmpfs and TLS status assertion', () => {
  assert.equal(assertions.length, 5);
});

for (const [index, code] of assertions.entries()) {
  test(`generated smoke assertion ${index + 1} rejects invalid inspection or TLS status`, () => {
    const expected = code.includes('HostConfig.Tmpfs')
      ? 'rw,noexec,nosuid,size=1m,uid=10001,gid=10001,mode=0700'
      : code.includes('ReadonlyRootfs')
        ? '10001:10001 true [ALL] [no-new-privileges:true]'
        : '10001:10001';
    const tls = code.includes('"$status" =~');
    const invalid = tls ? ['200', '404', '600', ''] : ['0:0', ''];
    for (const [valid, values] of [[true, [tls ? '502' : expected]], [false, invalid]]) {
      for (const value of values) {
        // Execute the shipping assertion, without Docker, on the bash selected by PATH.
        // Run this suite with Bash 3.2 as well as the hosted Linux Bash version.
        const result = spawnSync('bash', ['-euo', 'pipefail', '-c', `
          docker() { printf '%s\\n' "$INSPECTION"; }
          IMAGE=fixture; container=fixture; ID=fixture; status="$HTTP_STATUS"
          ${code}
          printf 'accepted\\n'
        `], {
          encoding: 'utf8', env: { ...process.env, INSPECTION: value, HTTP_STATUS: value },
        });
        assert.ifError(result.error);
        assert.equal(result.status === 0, valid, result.stderr);
        assert.equal(result.stdout, valid ? 'accepted\n' : '');
      }
    }
  });
}

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

const source = readFileSync(new URL('../test_generated_axum_container.sh', import.meta.url), 'utf8');
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

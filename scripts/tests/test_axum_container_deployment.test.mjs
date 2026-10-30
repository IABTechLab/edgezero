import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

const script = fileURLToPath(new URL('../test_axum_container_deployment.sh', import.meta.url));
const mocks = String.raw`
docker() {
  if [[ "$1" == compose ]]; then
    shift 5
    printf 'compose %s\n' "$1" >> "$MOCK_DIR/events"
    case "$1" in
      up|start)
        if [[ "$1" == up ]]; then echo initial; else echo restart; fi > "$MOCK_DIR/phase"
        echo 0 > "$MOCK_DIR/count" ;;
      ps) echo fixture ;;
      port)
        if [[ "$(< "$MOCK_DIR/phase")" == initial ]]; then echo 127.0.0.1:40001; else echo 127.0.0.1:40002; fi ;;
      logs) echo 'fixture container logs' ;;
      down)
        local attempts
        attempts=$(( $(< "$MOCK_DIR/down-count") + 1 ))
        echo "$attempts" > "$MOCK_DIR/down-count"
        if [[ "$attempts" -le "$DOWN_FAILURES" ]]; then
          echo 'mock teardown failed' >&2
          return 42
        fi ;;
      config|stop) ;;
      *) return 97 ;;
    esac
  elif [[ "$1" == inspect ]]; then
    if [[ "$*" == *State.ExitCode* ]]; then
      echo "$STOP_EXIT"
    else
      echo "$RESTRICTIONS"
    fi
  elif [[ "$1" != exec ]]; then
    return 97
  fi
}
curl() {
  local phase count
  phase="$(< "$MOCK_DIR/phase")"
  count=$(( $(< "$MOCK_DIR/count") + 1 ))
  echo "$count" > "$MOCK_DIR/count"
  printf 'curl %s %s %s\n' "$phase" "$count" "$*" >> "$MOCK_DIR/events"
  if [[ "$FAIL_PHASE" == "$phase" ]]; then return 22; fi
  if [[ "$count" -le 2 ]]; then return 22; fi
  if [[ "$WRONG_BODY_PHASE" == "$phase" ]]; then echo wrong; else echo "$EXPECTED_NAME app"; fi
}
sleep() { printf 'sleep %s\n' "$1" >> "$MOCK_DIR/events"; }
`;

function runSmoke(t, overrides = {}, name = 'container-probe') {
  const directory = mkdtempSync(join(tmpdir(), 'axum-deployment-test-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const bashEnv = join(directory, 'mocks.sh');
  writeFileSync(bashEnv, mocks);
  writeFileSync(join(directory, 'down-count'), '0');
  const result = spawnSync('bash', [script, 'localhost/app@sha256:' + 'a'.repeat(64), name], {
    encoding: 'utf8', timeout: 5000,
    env: { ...process.env, BASH_ENV: bashEnv, TMPDIR: directory, MOCK_DIR: directory,
      FAIL_PHASE: '', WRONG_BODY_PHASE: '', EXPECTED_NAME: name, STOP_EXIT: '0', DOWN_FAILURES: '0',
      RESTRICTIONS: '10001:10001 true [ALL] [no-new-privileges:true]', ...overrides },
  });
  assert.ifError(result.error);
  return { ...result, events: readFileSync(join(directory, 'events'), 'utf8').trim().split('\n') };
}

test('Compose smoke polls both starts and refreshes the published port', t => {
  const result = runSmoke(t, {}, 'custom-app');
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /PASS: no-store Compose/);
  for (const [phase, port] of [['initial', 40001], ['restart', 40002]]) {
    const calls = result.events.filter(event => event.startsWith(`curl ${phase} `));
    assert.equal(calls.length, 4, `${phase} must retry, then verify the final response`);
    for (const call of calls) assert.ok(call.endsWith(`--fail --silent --max-time 2 http://127.0.0.1:${port}/`), call);
  }
  assert.equal(result.events.filter(event => event === 'sleep 1').length, 4);
  assert.ok(result.events.includes('compose stop'));
  assert.ok(result.events.includes('compose start'));
  assert.equal(result.events.filter(event => event === 'compose down').length, 1);
  assert.equal(result.events.at(-1), 'compose down');
});

for (const DOWN_FAILURES of ['1', '2']) {
  test(`Compose smoke rejects failed teardown even if cleanup ${DOWN_FAILURES === '1' ? 'succeeds' : 'also fails'}`, t => {
    const result = runSmoke(t, { DOWN_FAILURES });
    assert.equal(result.status, 42);
    assert.doesNotMatch(result.stdout, /PASS:/);
    const diagnostics = result.stderr.match(/Diagnostics: ([^\n]+)/)?.[1];
    assert.ok(diagnostics, result.stderr);
    assert.match(readFileSync(join(diagnostics, 'teardown.log'), 'utf8'), /mock teardown failed/);
    assert.match(readFileSync(join(diagnostics, 'compose.log'), 'utf8'), /fixture container logs/);
    assert.equal(result.events.filter(event => event === 'compose down').length, 2);
    assert.equal(result.events.at(-1), 'compose down');
  });
}

test('Compose smoke preserves the primary failure when cleanup also fails', t => {
  const result = runSmoke(t, { WRONG_BODY_PHASE: 'initial', DOWN_FAILURES: '1' });
  assert.equal(result.status, 1);
  assert.doesNotMatch(result.stdout, /PASS:/);
  const diagnostics = result.stderr.match(/Diagnostics: ([^\n]+)/)?.[1];
  assert.ok(diagnostics, result.stderr);
  assert.match(readFileSync(join(diagnostics, 'cleanup.log'), 'utf8'), /mock teardown failed/);
  assert.match(readFileSync(join(diagnostics, 'cleanup-compose.log'), 'utf8'), /fixture container logs/);
});

for (const phase of ['initial', 'restart']) {
  test(`Compose smoke fails and cleans up when ${phase} never serves HTTP`, t => {
    const result = runSmoke(t, { FAIL_PHASE: phase });
    assert.notEqual(result.status, 0);
    assert.doesNotMatch(result.stdout, /PASS:/);
    assert.equal(result.events.filter(event => event.startsWith(`curl ${phase} `)).length, 31);
    if (phase === 'initial') assert.ok(!result.events.includes('compose start'));
    assert.equal(result.events.at(-1), 'compose down');
  });

  test(`Compose smoke rejects an incorrect ${phase} response body`, t => {
    const result = runSmoke(t, { WRONG_BODY_PHASE: phase });
    assert.notEqual(result.status, 0);
    assert.doesNotMatch(result.stdout, /PASS:/);
    if (phase === 'initial') assert.ok(!result.events.includes('compose start'));
    assert.equal(result.events.at(-1), 'compose down');
  });
}

for (const STOP_EXIT of ['137', '1', '']) {
  test(`Compose smoke rejects unclean stop (${STOP_EXIT}) before restart`, t => {
    const result = runSmoke(t, { STOP_EXIT });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /did not stop cleanly/);
    assert.ok(!result.events.includes('compose start'));
    assert.equal(result.events.at(-1), 'compose down');
  });
}
for (const RESTRICTIONS of ['0:0 true [ALL] [no-new-privileges:true]', '10001:10001 false [ALL] [no-new-privileges:true]', '10001:10001 true [] [no-new-privileges:true]', '10001:10001 true [ALL] []']) {
  test(`Compose smoke rejects insecure configuration: ${RESTRICTIONS}`, t => {
    const result = runSmoke(t, { RESTRICTIONS });
    assert.notEqual(result.status, 0);
    assert.ok(!result.events.includes('compose start'));
    assert.equal(result.events.at(-1), 'compose down');
  });
}

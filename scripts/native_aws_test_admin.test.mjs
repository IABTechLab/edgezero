import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import http from "node:http";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { fixtureAction } from "./native_aws_test_admin.mjs";

const exec = promisify(execFile);
const script = fileURLToPath(new URL("./native_aws_test_admin.mjs", import.meta.url));

async function emulator(t, reply = () => ({})) {
  const calls = [];
  const server = http.createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const call = {
      target: request.headers["x-amz-target"], headers: request.headers,
      payload: JSON.parse(Buffer.concat(chunks)), method: request.method,
    };
    calls.push(call);
    const result = reply(call);
    response.writeHead(result.status ?? 200);
    response.end(result.raw ?? JSON.stringify(result));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const endpoint = `http://127.0.0.1:${server.address().port}`;
  return { calls, endpoints: { dynamo: endpoint, moto: endpoint } };
}

test("fixture administration rejects non-loopback endpoints before sending requests", async (t) => {
  const setup = await emulator(t);
  for (const endpoint of [
    undefined, "https://127.0.0.1:8000", "http://localhost:8000", "http://127.0.0.1:0",
    "http://127.0.0.1:65536", "http://127.0.0.1:8000/", "http://127.0.0.1:8000@aws.example",
  ]) {
    for (const service of ["dynamo", "moto"]) {
      await assert.rejects(fixtureAction("ready", {}, { ...setup.endpoints, [service]: endpoint }), /literal/);
    }
  }
  assert.deepEqual(setup.calls, []);
});

test("readiness checks both emulator JSON APIs with dummy credentials", async (t) => {
  const setup = await emulator(t);
  assert.deepEqual(await fixtureAction("ready", {}, setup.endpoints), {});
  assert.deepEqual(setup.calls.map((call) => call.target), ["DynamoDB_20120810.ListTables", "secretsmanager.ListSecrets"]);
  for (const call of setup.calls) {
    assert.equal(call.method, "POST");
    assert.deepEqual(call.payload, {});
    assert.match(call.headers.authorization, /Credential=DUMMYLOCAL\/\d{8}\/us-east-1\//);
  }
});

test("seed preserves table schemas, expiry rows, secret bytes and version IDs", async (t) => {
  const setup = await emulator(t, ({ payload }) => ({ ARN: payload.Name ?? payload.SecretId, VersionId: payload.ClientRequestToken }));
  const result = await fixtureAction("seed", {}, setup.endpoints);
  const tables = setup.calls.filter((call) => call.target.endsWith(".CreateTable")).map((call) => call.payload);
  assert.deepEqual(tables.map((table) => table.KeySchema.length), [2, 1, 2]);
  assert.equal(tables[2].GlobalSecondaryIndexes[0].IndexName, "unsupported-index");
  assert.equal(result.table, tables[0].TableName);
  assert.equal(result.wrong_table, tables[1].TableName);
  assert.equal(result.indexed_table, tables[2].TableName);
  const rows = setup.calls.filter((call) => call.target.endsWith(".PutItem")).map((call) => call.payload.Item);
  assert.deepEqual(rows.map((row) => Buffer.from(row.v.B, "base64").toString()), ["expired", "live", "old"]);
  assert.deepEqual(rows.map((row) => row.expires_at_ms?.N), ["1", undefined, "1"]);
  assert.deepEqual(rows.map((row) => row.ttl_epoch_s?.N), ["1", undefined, "1"]);
  const secrets = setup.calls.filter((call) => call.target.endsWith(".CreateSecret")).map((call) => call.payload);
  assert.equal(secrets[1].SecretString, ' { "answer": 42 }\n');
  assert.deepEqual([...Buffer.from(secrets[2].SecretBinary, "base64")], [0, 255, 10, 34]);
  for (const secret of secrets) assert.ok(secret.ClientRequestToken.length >= 32);
  assert.equal(result.secrets.text.version_a, "a".repeat(32));
  assert.equal(result.secrets.text.version_b, "b".repeat(32));
});

test("rotation moves PINNED before default adoption and independently checks selectors", async (t) => {
  const setup = await emulator(t, ({ payload }) => ({
    VersionId: payload.ClientRequestToken,
    SecretString: payload.VersionId ? "secret-A" : "secret-B",
  }));
  assert.deepEqual(await fixtureAction("rotate", {}, setup.endpoints), { version_current: "c".repeat(32) });
  assert.deepEqual(setup.calls.map((call) => call.target), [
    "secretsmanager.UpdateSecretVersionStage", "secretsmanager.PutSecretValue",
    "secretsmanager.GetSecretValue", "secretsmanager.GetSecretValue", "secretsmanager.GetSecretValue",
  ]);
  assert.equal(setup.calls[0].payload.MoveToVersionId, "b".repeat(32));
  assert.equal(setup.calls[0].payload.RemoveFromVersionId, "a".repeat(32));
  assert.equal(setup.calls[3].payload.VersionStage, "PINNED");
  assert.equal(setup.calls[4].payload.VersionId, "a".repeat(32));
});

test("rotation fails if independent reads do not confirm adoption", async (t) => {
  const setup = await emulator(t, () => ({ SecretString: "secret-A" }));
  await assert.rejects(fixtureAction("rotate", {}, setup.endpoints), /secret-A.*secret-B/s);
});

test("item readback decodes binary values and preserves TTL and absence output", async (t) => {
  let item;
  const setup = await emulator(t, () => ({ Item: item }));
  const read = () => fixtureAction("item", { namespace: "primary", key: "key" }, setup.endpoints);
  assert.deepEqual(await read(), { present: false });
  item = { v: { B: Buffer.from([0, 255, 10, 34]).toString("base64") }, expires_at_ms: { N: "1234" }, ttl_epoch_s: { N: "2" } };
  assert.deepEqual(await read(), { present: true, value: [0, 255, 10, 34], expires_at_ms: 1234, ttl_epoch_s: 2 });
  delete item.expires_at_ms;
  delete item.ttl_epoch_s;
  assert.deepEqual(await read(), { present: true, value: [0, 255, 10, 34], expires_at_ms: null, ttl_epoch_s: null });
  assert.deepEqual(setup.calls[0].payload.Key, { pk: { S: "primary" }, sk: { S: "key" } });
  assert.equal(setup.calls[0].payload.ConsistentRead, true);
});

test("CLI returns JSON only and rejects unknown actions and malformed input", async (t) => {
  const setup = await emulator(t);
  const options = { env: {
    ...process.env,
    EDGEZERO_AWS_TEST_DYNAMODB_ENDPOINT: setup.endpoints.dynamo,
    EDGEZERO_AWS_TEST_SECRETS_ENDPOINT: setup.endpoints.moto,
  } };
  const { stdout, stderr } = await exec(process.execPath, [script, "delete_secret", "{}"], options);
  assert.equal(stdout, "{}\n");
  assert.equal(stderr, "");
  assert.deepEqual(setup.calls[0].payload, { SecretId: "edgezero-emulator-text", ForceDeleteWithoutRecovery: true });
  await assert.rejects(exec(process.execPath, [script, "unknown", "{}"], options), /unknown fixture action/);
  await assert.rejects(exec(process.execPath, [script, "seed", "not JSON"], options));
  assert.equal(setup.calls.length, 1);
});

test("HTTP failures, redirects and malformed responses fail without retries", async (t) => {
  for (const result of [{ status: 500 }, { status: 302 }, { raw: "not JSON" }]) {
    const setup = await emulator(t, () => result);
    await assert.rejects(fixtureAction("ready", {}, setup.endpoints));
    assert.equal(setup.calls.length, 1);
  }
});

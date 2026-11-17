import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";
import { auditProxy, childEnvironment, DYNAMO_IMAGE, localEndpoint, runIntegration, serviceConfig, syntheticCsv } from "./run_geo_kv_integration.mjs";
import { fixtureClient } from "./native_aws_test_admin.mjs";

async function server(t, handler) {
  const instance = http.createServer(handler);
  await new Promise((resolve) => instance.listen(0, "127.0.0.1", resolve));
  t.after(() => { instance.closeAllConnections(); instance.close(); });
  return `http://127.0.0.1:${instance.address().port}`;
}

function post(endpoint, operation, body = "{}") {
  return new Promise((resolve, reject) => {
    const req = http.request(endpoint, {
      method: "POST", headers: { "x-amz-target": `DynamoDB_20120810.${operation}` },
    }, (response) => {
      let text = "";
      response.on("data", (chunk) => { text += chunk; });
      response.on("end", () => resolve({ status: response.statusCode, text }));
      response.on("error", reject);
    });
    req.on("error", reject);
    req.end(body);
  });
}

const classify = (req) => ({ operation: req.headers["x-amz-target"].split(".").at(-1), write: req.headers["x-amz-target"].endsWith(".PutItem") });

test("local proof rejects external, ambiguous and credential-bearing endpoints", () => {
  assert.equal(localEndpoint("http://127.0.0.1:1234"), "http://127.0.0.1:1234");
  assert.equal(localEndpoint("https://127.0.0.1:1234", "https:"), "https://127.0.0.1:1234");
  for (const endpoint of [
    "http://aws.example:8000", "http://localhost:8000", "http://127.1:8000", "http://2130706433:8000",
    "http://127.0.0.1:0", "http://127.0.0.1:65536", "http://127.0.0.1:8000/",
    "http://user:secret@127.0.0.1:8000", "https://127.0.0.1:8000", "http://127.0.0.1:8000?redirect=remote",
  ]) assert.throws(() => localEndpoint(endpoint), undefined, endpoint);
});

test("child environment cannot inherit production credentials, endpoints, proxy or artifact overrides", () => {
  const env = childEnvironment({
    PATH: "/bin", HOME: "/production-home", AWS_PROFILE: "production", AWS_ACCESS_KEY_ID: "real-key",
    AWS_SECRET_ACCESS_KEY: "real-secret", AWS_ENDPOINT_URL_DYNAMODB: "https://aws.example",
    HTTP_PROXY: "https://proxy.example", SSL_CERT_FILE: "/production-ca",
    FIXTURE_GEO_ARTIFACT: "/other-artifact", EDGEZERO__STORE_BINDINGS_FILE: "/production-bindings",
    EDGEZERO__ADAPTER__GEO__PROVIDER: "cloudfront",
  }, "/tmp/local-proof");
  assert.equal(env.HOME, "/tmp/local-proof/home");
  assert.equal(env.AWS_ACCESS_KEY_ID, "DUMMYLOCAL");
  assert.equal(env.AWS_SECRET_ACCESS_KEY, "DUMMYLOCAL");
  assert.equal(env.AWS_EC2_METADATA_DISABLED, "true");
  for (const key of ["AWS_PROFILE", "AWS_ENDPOINT_URL_DYNAMODB", "HTTP_PROXY", "SSL_CERT_FILE", "FIXTURE_GEO_ARTIFACT", "EDGEZERO__STORE_BINDINGS_FILE", "EDGEZERO__ADAPTER__GEO__PROVIDER"]) assert.equal(env[key], undefined);
  assert.match(DYNAMO_IMAGE, /^amazon\/dynamodb-local@sha256:[0-9a-f]{64}$/);
});

test("missing binaries fail prerequisites, never produce a passing skip or build", async () => {
  await assert.rejects(runIntegration({ env: {} }), /explicitly selected absolute executable/);
  await assert.rejects(runIntegration({ env: { CELLD_BIN: "relative-celld" } }), /explicitly selected absolute executable/);
});

test("celld policy separates publisher grants and read-only serving token without storing plaintext", () => {
  const config = serviceConfig("publisher-token", "serving-token");
  const policy = JSON.parse(config.vars.EDGEZERO_STORE_POLICY);
  assert.equal(policy.access.mode, "authenticated");
  assert.deepEqual(policy.access.tokens[0].grants[0].operations, ["read", "write", "delete", "list"]);
  assert.deepEqual(policy.access.tokens[1].grants[0].operations, ["read"]);
  assert(!JSON.stringify(config).includes("publisher-token"));
  assert(!JSON.stringify(config).includes("serving-token"));
});

test("synthetic A and B preserve boundaries, unsupported country, country-only and missing-geoname cases", () => {
  const a = syntheticCsv();
  const b = syntheticCsv("DE", "BE");
  assert.equal(Object.keys(a).length, 3);
  assert.match(a["GeoLite2-City-Locations-en.csv"], /1,US,CA\n2,GB,\n3,XK,/);
  assert.match(b["GeoLite2-City-Locations-en.csv"], /1,DE,BE/);
  assert.equal(a["GeoLite2-City-Blocks-IPv4.csv"], b["GeoLite2-City-Blocks-IPv4.csv"]);
  assert.match(a["GeoLite2-City-Blocks-IPv4.csv"], /192\.0\.2\.0\/32,1,2\n192\.0\.2\.2\/32,3,2/);
  assert.match(a["GeoLite2-City-Blocks-IPv4.csv"], /192\.0\.2\.238\/32/);
  assert.match(a["GeoLite2-City-Blocks-IPv4.csv"], /198\.51\.100\.0\/24,,1/);
  assert.match(a["GeoLite2-City-Blocks-IPv6.csv"], /2001:db8::\/126,2/);
});

test("audit forwards each call exactly once and records operation without retaining reader IPs or credentials", async (t) => {
  const requests = [];
  const target = await server(t, async (req, res) => {
    let body = "";
    for await (const chunk of req) body += chunk;
    requests.push(body);
    res.end('{"Item":null}');
  });
  const proxy = await auditProxy(target, classify);
  t.after(() => { proxy.server.closeAllConnections(); proxy.server.close(); });
  const result = await post(proxy.endpoint, "GetItem", '{"key":"synthetic"}');
  assert.equal(result.status, 200);
  assert.deepEqual(requests, ['{"key":"synthetic"}']);
  assert.deepEqual(proxy.calls, [{ operation: "GetItem", write: false, responseBytes: 13, status: 200 }]);
});

test("uncertain-write fault observes backend completion but withholds acknowledgement and never retries", async (t) => {
  let applied = 0;
  const target = await server(t, async (req, res) => {
    for await (const chunk of req) void chunk;
    applied++;
    res.end("{}");
  });
  const proxy = await auditProxy(target, classify);
  t.after(() => { proxy.server.closeAllConnections(); proxy.server.close(); });
  const appliedPromise = proxy.arm();
  const req = http.request(proxy.endpoint, { method: "POST", headers: { "x-amz-target": "DynamoDB_20120810.PutItem" } });
  let acknowledged = false;
  req.on("response", () => { acknowledged = true; });
  req.on("error", () => {});
  req.end("{}");
  const result = await appliedPromise;
  assert.equal(result.status, 200);
  assert.equal(applied, 1);
  assert.equal(acknowledged, false);
  req.destroy();
  assert.equal((await post(proxy.endpoint, "GetItem")).status, 200);
  assert.equal(applied, 2, "one call per explicit request; fault does not replay");
});

test("exported admin client still rejects external endpoint before any I/O", () => {
  assert.throws(() => fixtureClient("https://dynamodb.us-east-1.amazonaws.com", "dynamodb"), /literal/);
});

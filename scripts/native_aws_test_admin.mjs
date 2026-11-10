#!/usr/bin/env node
// Local emulator fixture administration, never an application AWS client.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import http from "node:http";
import { pathToFileURL } from "node:url";

const TABLE = "edgezero-emulator-kv";
const TEXT_SECRET = "edgezero-emulator-text";
const VERSION_A = "a".repeat(32);
const VERSION_B = "b".repeat(32);

function client(endpoint, service) {
  const match = /^http:\/\/127\.0\.0\.1:([0-9]+)$/.exec(endpoint ?? "");
  if (!match || Number(match[1]) < 1 || Number(match[1]) > 65535) {
    throw new Error(`Fixture endpoint must be literal http://127.0.0.1:<port>, got: ${endpoint}`);
  }
  return (operation, payload = {}) => new Promise((resolve, reject) => {
    const body = JSON.stringify(payload);
    const date = new Date().toISOString().replace(/[:-]|\.\d{3}/g, "");
    // These emulators need a credential scope but do not validate signatures.
    // Fixed dummy authorization is safe only because endpoints are strictly local.
    const request = http.request(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": service === "dynamodb" ? "application/x-amz-json-1.0" : "application/x-amz-json-1.1",
        "Content-Length": Buffer.byteLength(body),
        "X-Amz-Target": `${service === "dynamodb" ? "DynamoDB_20120810" : "secretsmanager"}.${operation}`,
        "X-Amz-Date": date,
        Authorization: `AWS4-HMAC-SHA256 Credential=DUMMYLOCAL/${date.slice(0, 8)}/us-east-1/${service}/aws4_request, SignedHeaders=host;x-amz-date, Signature=${"0".repeat(64)}`,
      },
    }, (response) => {
      const chunks = [];
      let size = 0;
      response.on("data", (chunk) => {
        size += chunk.length;
        if (size > 1024 * 1024) request.destroy(new Error("Oversized fixture response"));
        else chunks.push(chunk);
      });
      response.on("error", reject);
      response.on("end", () => {
        const text = Buffer.concat(chunks).toString("utf8");
        if (response.statusCode !== 200) {
          reject(new Error(`${service}.${operation} failed: HTTP ${response.statusCode} ${text}`));
          return;
        }
        try { resolve(JSON.parse(text)); } catch (error) { reject(error); }
      });
    });
    // No environment proxy discovery, redirects, credential lookup or retries.
    const timer = setTimeout(() => request.destroy(new Error("Fixture request timeout")), 3000);
    request.on("error", reject);
    request.on("close", () => clearTimeout(timer));
    request.end(body);
  });
}

async function seed(dynamo, secrets) {
  const keys = [
    { AttributeName: "pk", KeyType: "HASH" },
    { AttributeName: "sk", KeyType: "RANGE" },
  ];
  const attributes = [
    { AttributeName: "pk", AttributeType: "S" },
    { AttributeName: "sk", AttributeType: "S" },
  ];
  await dynamo("CreateTable", {
    TableName: TABLE, KeySchema: keys, AttributeDefinitions: attributes, BillingMode: "PAY_PER_REQUEST",
  });
  const wrong = "edgezero-emulator-wrong";
  await dynamo("CreateTable", {
    TableName: wrong, KeySchema: keys.slice(0, 1), AttributeDefinitions: attributes.slice(0, 1),
    BillingMode: "PAY_PER_REQUEST",
  });
  const indexed = "edgezero-emulator-indexed";
  await dynamo("CreateTable", {
    TableName: indexed, KeySchema: keys, AttributeDefinitions: attributes, BillingMode: "PAY_PER_REQUEST",
    GlobalSecondaryIndexes: [{
      IndexName: "unsupported-index", KeySchema: [{ AttributeName: "sk", KeyType: "HASH" }],
      Projection: { ProjectionType: "KEYS_ONLY" },
    }],
  });
  for (const [key, value, expired] of [
    ["expired/0", "expired", true], ["expired/1", "live", false], ["renew/item", "old", true],
  ]) {
    const item = { pk: { S: "primary" }, sk: { S: key }, v: { B: Buffer.from(value).toString("base64") } };
    if (expired) Object.assign(item, { expires_at_ms: { N: "1" }, ttl_epoch_s: { N: "1" } });
    await dynamo("PutItem", { TableName: TABLE, Item: item });
  }
  const text = await secrets("CreateSecret", {
    Name: TEXT_SECRET, SecretString: "secret-A", ClientRequestToken: VERSION_A,
  });
  const versionB = await secrets("PutSecretValue", {
    SecretId: text.ARN, SecretString: "secret-B", ClientRequestToken: VERSION_B, VersionStages: ["CANDIDATE"],
  });
  await secrets("UpdateSecretVersionStage", {
    SecretId: text.ARN, VersionStage: "PINNED", MoveToVersionId: text.VersionId,
  });
  const wholeJson = await secrets("CreateSecret", {
    Name: "edgezero-emulator-json", SecretString: ' { "answer": 42 }\n', ClientRequestToken: randomUUID(),
  });
  const binary = await secrets("CreateSecret", {
    Name: "edgezero-emulator-binary", SecretBinary: Buffer.from([0, 255, 10, 34]).toString("base64"),
    ClientRequestToken: randomUUID(),
  });
  return {
    table: TABLE, wrong_table: wrong, indexed_table: indexed, missing_table: "edgezero-emulator-missing",
    secrets: {
      text: { arn: text.ARN, version_a: text.VersionId, version_b: versionB.VersionId },
      json: { arn: wholeJson.ARN }, binary: { arn: binary.ARN },
    },
  };
}

async function rotate(secrets) {
  // Moto 5.2.2 does not update no-selector reads after a manual AWSCURRENT move.
  // Move PINNED first: PutSecretValue also resets the old current version's stages.
  // The application snapshots stay A while independent reads confirm adoption of B.
  await secrets("UpdateSecretVersionStage", {
    SecretId: TEXT_SECRET, VersionStage: "PINNED", MoveToVersionId: VERSION_B, RemoveFromVersionId: VERSION_A,
  });
  const current = await secrets("PutSecretValue", {
    SecretId: TEXT_SECRET, SecretString: "secret-B", ClientRequestToken: "c".repeat(32),
  });
  for (const selector of [{}, { VersionStage: "PINNED" }, { VersionId: VERSION_A }]) {
    const actual = await secrets("GetSecretValue", { SecretId: TEXT_SECRET, ...selector });
    assert.equal(actual.SecretString, selector.VersionId ? "secret-A" : "secret-B");
  }
  return { version_current: current.VersionId };
}

export async function fixtureAction(action, payload, endpoints = {
  dynamo: process.env.EDGEZERO_AWS_TEST_DYNAMODB_ENDPOINT,
  moto: process.env.EDGEZERO_AWS_TEST_SECRETS_ENDPOINT,
}) {
  const dynamo = client(endpoints.dynamo, "dynamodb");
  const secrets = client(endpoints.moto, "secretsmanager");
  switch (action) {
    case "ready":
      await dynamo("ListTables");
      await secrets("ListSecrets");
      return {};
    case "seed": return seed(dynamo, secrets);
    case "rotate": return rotate(secrets);
    case "delete_secret":
      await secrets("DeleteSecret", { SecretId: TEXT_SECRET, ForceDeleteWithoutRecovery: true });
      return {};
    case "item": {
      const { Item: item } = await dynamo("GetItem", {
        TableName: TABLE, Key: { pk: { S: payload.namespace }, sk: { S: payload.key } }, ConsistentRead: true,
      });
      if (!item) return { present: false };
      return {
        present: true, value: [...Buffer.from(item.v.B, "base64")],
        expires_at_ms: item.expires_at_ms ? Number(item.expires_at_ms.N) : null,
        ttl_epoch_s: item.ttl_epoch_s ? Number(item.ttl_epoch_s.N) : null,
      };
    }
    default: throw new Error(`unknown fixture action: ${action}`);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  Promise.resolve().then(() => fixtureAction(process.argv[2], JSON.parse(process.argv[3])))
    .then((result) => console.log(JSON.stringify(result)))
    .catch((error) => { console.error(error); process.exitCode = 1; });
}

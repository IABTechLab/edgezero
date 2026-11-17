#!/usr/bin/env node
// Required CI proof against independent local services, without an AWS account.
import { execFile, spawn } from "node:child_process";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import http from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { fixtureAction } from "./native_aws_test_admin.mjs";

const exec = promisify(execFile);
const root = fileURLToPath(new URL("../", import.meta.url));
// Multi-architecture index digests checked against the upstream registries.
// DynamoDB Local 3.3.1; Moto 5.2.2; AppConfig Agent 2.0.224095.
const images = {
  dynamo: "amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab",
  moto: "ghcr.io/getmoto/motoserver@sha256:d8ae5edc2bf080e7e4c13f9bd4b29b53ac3b4427e92956318db3dbe23ec43eb7",
  agent: "public.ecr.aws/aws-appconfig/aws-appconfig-agent@sha256:cd604064652c56d9d6dc8435aa0d4bf3e90aa348830fa3b8a7b8650e3500ec3e",
};

export function publishedEndpoint(address) {
  const match = /^127\.0\.0\.1:([0-9]+)$/.exec(address.trim());
  if (!match || Number(match[1]) < 1 || Number(match[1]) > 65535) {
    throw new Error(`Expected one exclusively loopback-published port, got: ${address}`);
  }
  return `http://${address.trim()}`;
}

async function docker(args, timeout = 30_000) {
  const { stdout, stderr } = await exec("docker", args, {
    timeout,
    killSignal: "SIGKILL",
    maxBuffer: 10 * 1024 * 1024,
  });
  return (args[0] === "logs" ? `${stdout}${stderr}` : stdout).trim();
}

function agentReady(endpoint) {
  return new Promise((resolve, reject) => {
    // http.request does not discover environment proxies or follow redirects.
    const request = http.get(
      `${endpoint}/applications/probe/environments/test/configurations/ready`,
      { headers: { Authorization: "Bearer local-agent-token" } },
      (response) => {
        let body = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          body += chunk;
          if (body.length > 4096) request.destroy(new Error("Oversized readiness response"));
        });
        response.on("error", reject);
        response.on("end", () => resolve(response.statusCode === 200 && body === "agent-ready\n"));
      },
    );
    const timer = setTimeout(() => request.destroy(new Error("Agent readiness timeout")), 3000);
    request.on("error", reject);
    request.on("close", () => clearTimeout(timer));
  });
}

export async function main() {
  await docker(["info"], 20_000);
  const work = await mkdtemp(join(tmpdir(), "edgezero-aws-test-"));
  const prefix = `edgezero-aws-${work.split("/").at(-1)}`;
  const network = `${prefix}-network`;
  const names = { dynamo: `${prefix}-dynamo`, moto: `${prefix}-moto`, agent: `${prefix}-agent` };
  const configs = join(work, "configs");
  let testProcess;
  let testGroup;
  let interruptTimer;
  let interrupted;
  let failure;
  const signalTest = (signal) => {
    if (testGroup) {
      try { process.kill(-testGroup, signal); } catch (error) {
        if (error.code !== "ESRCH") throw error;
      }
    }
  };
  const onInterrupt = (signal) => {
    interrupted = signal;
    signalTest("SIGTERM");
    if (!interruptTimer) interruptTimer = setTimeout(() => signalTest("SIGKILL"), 5000);
  };
  const onSigint = () => onInterrupt("SIGINT");
  const onSigterm = () => onInterrupt("SIGTERM");
  process.on("SIGINT", onSigint);
  process.on("SIGTERM", onSigterm);
  const checkInterrupted = () => {
    if (interrupted) throw new Error(`Interrupted by ${interrupted}`);
  };

  try {
    for (const image of Object.values(images)) {
      checkInterrupted();
      console.log(`Pulling pinned service ${image}`);
      await docker(["pull", image], 180_000);
    }
    checkInterrupted();
    await mkdir(configs);
    await writeFile(join(configs, "probe:test:ready"), "agent-ready\n");
    // A normal bridge supports published ports. It is not an egress-deny sandbox.
    await docker(["network", "create", network]);
    await docker([
      "run", "-d", "--name", names.dynamo, "--network", network,
      "--network-alias", "dynamodb", "--publish", "127.0.0.1::8000",
      images.dynamo, "-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb", "-disableTelemetry",
    ]);
    checkInterrupted();
    await docker([
      "run", "-d", "--name", names.moto, "--network", network,
      "--publish", "127.0.0.1::5000", images.moto,
    ]);
    await docker([
      "run", "-d", "--name", names.agent, "--network", network,
      "--publish", "127.0.0.1::2772", "--mount", `type=bind,source=${configs},target=/configs,readonly`,
      "--env", "LOCAL_DEVELOPMENT_DIRECTORY=/configs", "--env", "SERVICE_REGION=us-east-1",
      "--env", "AWS_ACCESS_KEY_ID=DUMMYLOCAL", "--env", "AWS_SECRET_ACCESS_KEY=DUMMYLOCAL",
      "--env", "AWS_EC2_METADATA_DISABLED=true", "--env", "ACCESS_TOKEN=local-agent-token",
      "--env", "HTTP_HOST=0.0.0.0", "--env", "POLL_INTERVAL=1s", images.agent,
    ]);
    const endpoints = {};
    for (const [service, port] of [["dynamo", 8000], ["moto", 5000], ["agent", 2772]]) {
      endpoints[service] = publishedEndpoint(await docker(["port", names[service], `${port}/tcp`]));
    }
    const deadline = Date.now() + 60_000;
    let ready = false;
    let readinessError;
    while (Date.now() < deadline) {
      checkInterrupted();
      try {
        await fixtureAction("ready", {}, endpoints);
        ready = await agentReady(endpoints.agent);
        if (ready) break;
      } catch (error) { readinessError = error; }
      await delay(200);
    }
    if (!ready) throw new Error("Local AWS services did not become ready within 60 seconds", { cause: readinessError });
    checkInterrupted();
    console.log("Pinned local AWS services ready; running generated consumer contracts");
    const env = {
      ...process.env,
      TMPDIR: work,
      EDGEZERO_AWS_TEST_DYNAMODB_ENDPOINT: endpoints.dynamo,
      EDGEZERO_AWS_TEST_SECRETS_ENDPOINT: endpoints.moto,
      EDGEZERO_AWS_TEST_AGENT_ENDPOINT: endpoints.agent,
      EDGEZERO_AWS_TEST_AGENT_DIRECTORY: configs,
      EDGEZERO_AWS_TEST_NODE: process.execPath,
      EDGEZERO_AWS_TEST_AGENT_TOKEN: "local-agent-token",
    };
    // The nonzero runner rejects misspelled filters and accidental cfg exclusion.
    await new Promise((resolve, reject) => {
      testProcess = spawn(join(root, "scripts/run_test_nonzero.sh"), [
        "--ignored", "generated_native_aws_emulator_contracts", "cargo", "test",
        "--offline", "--locked", "-p", "edgezero-cli", "--test", "native_aws_emulator_proof",
      ], { cwd: root, env, stdio: "inherit", detached: true });
      testGroup = testProcess.pid;
      let timedOut = false;
      const timer = setTimeout(() => { timedOut = true; signalTest("SIGKILL"); }, 20 * 60_000);
      testProcess.once("error", (error) => { clearTimeout(timer); reject(error); });
      testProcess.once("close", (code, signal) => {
        clearTimeout(timer);
        testProcess = undefined;
        if (code === 0 && !timedOut && !interrupted) resolve();
        else reject(new Error(`Native AWS integration failed: ${timedOut ? "20 minute timeout" : interrupted ?? signal ?? `exit ${code}`}`));
      });
    });
  } catch (error) {
    failure = error;
    for (const container of Object.values(names)) {
      try { console.error(`--- ${container} logs ---\n${await docker(["logs", container], 10_000)}`); }
      catch (logError) { console.error(logError.message); }
    }
  } finally {
    clearTimeout(interruptTimer);
    signalTest("SIGKILL");
    for (const container of Object.values(names)) {
      try { await docker(["rm", "-f", container], 10_000); }
      catch (error) { if (!failure) failure = error; }
    }
    try { await docker(["network", "rm", network], 20_000); }
    catch (error) { if (!failure) failure = error; }
    await rm(work, { recursive: true, force: true });
    clearTimeout(interruptTimer);
    process.off("SIGINT", onSigint);
    process.off("SIGTERM", onSigterm);
  }
  if (interrupted && !failure) failure = new Error(`Interrupted by ${interrupted} during cleanup`);
  if (failure) throw failure;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => { console.error(error); process.exitCode = 1; });
}

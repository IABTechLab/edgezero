# Publish a pinned geo dataset into Spin KV

This separate operator application reuses the KV geo tool's artifact validation,
immutable publication and full remote-tree verification. It is never part of the
serving application. It requires Python 3.11+, Rust with `wasm32-wasip2`, and Spin
4.1.0. The runner supports macOS/Linux and invokes the installed Spin executable;
it neither provisions resources nor deploys an app.

Build the loader from the repository root:

```sh
cargo build --locked --release --manifest-path examples/geo-spin-loader/Cargo.toml \
  --target wasm32-wasip2
cargo test --locked --manifest-path examples/geo-spin-loader/Cargo.toml
python3 -m unittest discover -s examples/geo-spin-loader -p test_load.py
```

First [build and verify a permitted geo artifact](../geo-kv-prototype/README.md).
Keep licensed CSV, derived artifacts, inventories, credentials and runtime databases
outside Git and release images. The loader stages an owned temporary artifact copy
inside its application directory because Spin only maps files from that directory.
The guest reads the mounted `/artifact`; the runner removes its temporary copy,
manifest, logs and state on exit. Account for this temporary disk usage and the
host's temporary-storage retention policy.

Create an explicit Spin runtime configuration outside Git. For local file-backed
storage, use an absolute path shared by loader and serving:

```toml
[key_value_store.geo_runtime]
type = "spin"
path = "/srv/edgezero/geography.db"
```

Create the parent directory and ensure the process has access. Supply the same
configuration file, exact named store and backend credentials to both applications.
A relative path or two implicit `.spin` directories may load an entirely different
database. Stop local SQLite serving during publication. External Redis, CosmosDB
and DynamoDB backing are configured through Spin's supported runtime configuration;
local tests do not qualify their credentials, propagation, quotas or permissions.
Do not populate Spin's SQLite tables directly: their format is not a stable API.

```sh
python3 examples/geo-spin-loader/load.py \
  --spin spin \
  --artifact "$GEO_ARTIFACT" \
  --runtime-config "$SPIN_RUNTIME_CONFIG" \
  --store geo_runtime
```

`--spin` accepts a PATH command or an executable path. `--wasm` overrides the built
component path. The runner creates a loopback-only component manifest with the
selected store permission, generates an ephemeral credential, and supplies it through
Spin's environment-backed variable provider. Its POST sends that credential from
process memory. No credential appears in arguments, source, reports or logs. The
listener is not an origin upload endpoint; do not add it to a public Spin manifest.
Readiness defaults to 30 seconds, total publication to 600 seconds; finite positive
`--readiness-timeout` and `--publication-timeout` overrides are supported. A watchdog
stops the owned runtime when the publication window expires, even if a response keeps
sending bytes. Success, failure, interruption and timeout stop and reap the child.

The loader authenticates before reading artifacts. It validates inventory closure
and hashes before opening the remote store, skips byte-identical objects, rejects
conflicts, writes absent objects, reads each write back, publishes the manifest last,
and verifies the complete remote tree. The report contains counts and the exact
`<generation>@<manifest-sha256>` pin. On an interrupted or uncertain write, rerun the
same artifact: publication is resumable. A single operator must own each generation;
read-before-write is not a distributed lock. There is no overwrite repair, active
pointer or automatic serving fallback.

Configuration, startup/transport, authentication, publication and deadline failures
have fixed diagnostic categories. Raw SDK errors and secrets are not printed. Use
the offline artifact verifier to diagnose input failures and supported backend
operator tooling to inspect permissions/conflicts. Publication may leave validated
partial objects on failure; do not select the pin until successful verification.

After loading, configure the [Spin serving geo selection](../../docs/guide/adapters/spin.md#geographic-context-from-a-pinned-kv-dataset)
and pass the same runtime configuration to `spin up`. Serving must declare physical
store `geo_runtime` and map logical `geo` through
`EDGEZERO__STORES__KV__GEO__NAME=geo_runtime`. Verify through the actual serving
identity/path before selecting the pin across the fleet. Rollback requires a
reverified, still-permitted previous generation. Retirement/deletion and data
licensing remain operator responsibilities.

The component source is intentionally separate from the serving adapter. Loader
authentication tests and runner tests cover duplicate/wrong credentials, executable
resolution and actual child cleanup on success, startup timeout, publication timeout
and continuously arriving responses. Existing publisher tests cover artifact
validation, replay, conflict, uncertain writes, readback and manifest ordering.

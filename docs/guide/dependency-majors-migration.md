# Dependency Majors Migration

EdgeZero moved several dependencies across major versions at once. An
application generated before that change keeps its own pins, and those
pins have to move together with EdgeZero's — the adapters hand
provider-owned types (`fastly::Request`, `spin_sdk::http::Request`)
straight through, and typed config derives `validator::Validate`.

A crate resolved at two majors produces two unrelated types with the
same name, so the mismatch surfaces as a trait or type error pointing
at your own code:

```
error[E0277]: the trait bound `MyAppConfig: validator::traits::Validate`
is not satisfied
```

Nothing in that message names the real cause, which is why this page
exists.

## What to change

Update these in your application's workspace `Cargo.toml` at the same
time as the EdgeZero dependency:

| Crate        | Old    | New                                  | Applies to                  |
| ------------ | ------ | ------------------------------------ | --------------------------- |
| `fastly`     | `0.12` | `=0.13.1`                            | Fastly adapter              |
| `log-fastly` | `0.12` | `=0.13.1`                            | Fastly adapter              |
| `spin-sdk`   | `6`    | `=7.0.0` plus immutable Git revision | Spin adapter                |
| `validator`  | `0.20` | `0.21`                               | Any typed app config        |
| `rusqlite`   | `0.32` | `0.40`                               | Only if you use it directly |

Keep the audited SDK requirements exact, including
`worker = { version = "=0.8.5", default-features = false, features = ["http"] }`
for Cloudflare applications. Spin applications that name the SDK directly must also copy
the Git URL and full revision from the matching EdgeZero pin, not mix registry and Git SDKs.
The [Spin dependency audit](../audits/2026-10-27-spin-sdk-eager-progress.md) records that source
and the upstream contribution. Migrate SDK spawn calls to `wasip3::spawn_local`; no compatibility
alias or second executor is provided. Then refresh the lockfile:

```bash
cargo update
cargo test --workspace --all-targets
```

## Runtime requirement: Spin 4.1

`spin-sdk` 7 imports `wasi:http/types@0.3.0`, which Spin 4.0.x does not
provide. This one does not fail at build time — the component compiles,
its tests pass under `wasmtime`, and only `spin up` reports:

```
Error: component imports instance `wasi:http/types@0.3.0`, but a
matching implementation was not found in the linker
```

Install Spin 4.1 or newer ([install](https://spinframework.dev/install))
before upgrading the SDK. `cargo check` cannot detect this.

## Toolchain

The workspace builds on Rust 1.98.1. If your application pins a
toolchain in `.tool-versions`, move it up with the dependencies; the
`edition = "2024"` crates in EdgeZero need 1.85 or newer regardless.

A scaffolded app's `.tool-versions` also pins the provider CLIs, and
those moved too:

| Tool      | Old     | New         | Applies to                  |
| --------- | ------- | ----------- | --------------------------- |
| `rust`    | 1.95.0  | **1.98.1**  | every app                   |
| `fastly`  | 15.1.0  | **16.0.0**  | Fastly adapter              |
| `viceroy` | 0.17.0  | **0.21.0**  | Fastly local testing        |
| `spin`    | Unspecified | **4.1.0** | Spin adapter                |
| `nodejs`  | 24.12.0 | **24.20.0** | Cloudflare (wrangler), docs |

The deploy action's `install-fastly.sh` checks its bundled `versions.json` against
the action repository's `.tool-versions`, not the application's file. An application
that leaves its own Fastly pin behind is not rejected by that check. Update application
CLI pins and the selected deploy-action release explicitly; do not rely on the action's
internal consistency check to validate an application's toolchain.

## Verifying

The mismatch this page describes is a compile error, so a clean build
is the check:

```bash
cargo build --workspace
cargo test --workspace --all-targets
```

For the Spin runtime requirement, build is not enough — run the app:

```bash
spin --version          # expect 4.1.0 or newer
spin up --from crates/<your-app>-adapter-spin
```

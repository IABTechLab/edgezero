# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking changes

- **Generated adapter manifests.** `axum.toml`, `wrangler.toml`, `fastly.toml`,
  `spin.toml`, and `runtime-config.toml` are local, gitignored outputs. Back up
  custom contents before pulling their removal: Git deletes teammates' clean
  tracked copies. After cloning, run the project CLI's `provision --local` for
  each adapter. Preserve shared customizations in supported tracked declarations
  or templates; provisioning regenerates defaults, not arbitrary old settings.
- **Declared local paths.** Local provision and config operations require both
  `[adapters.<name>.adapter].manifest` and `.crate`. Adapter manifest validation
  now receives `allow_component_refresh: bool`; update external implementations
  and callers of `Adapter::validate_adapter_manifest`.
- **Scaffold adapter defaults.** `ManifestSpec` literals require
  `adapter_defaults`; use `""` when no tracked defaults are needed. Spin scaffolds
  declare HTTPS outbound access for their proxy route. Older generated Spin
  manifests also allowed HTTPS by default; regeneration drops that permission
  unless the required `allowed_outbound_hosts` are first copied into tracked
  `edgezero.toml`. Preserve those hosts before removing manifests or pulling
  their removal commit.
- **Axum KV location.** Runtime KV databases now live under the application
  project root's `.edgezero/`, rather than the launch directory. Stop the old
  runtime, back up its databases, and copy their existing filenames into the
  project's `.edgezero/` before restarting.
- **Cloudflare namespace adoption.** Cloud provision skips unverified IDs
  held only in local Wrangler state. Verify the account, binding and primary/
  preview IDs, then record them in the corresponding tracked deployed maps.
  Commit verified IDs before removing manifests or asking teammates to pull.
  Local provisioning can then restore them without creating duplicates.
- **Unset typed secrets.** Fresh Axum and Cloudflare secret hints are commented;
  Spin secret variables set `required = true`, have no default and require a
  provider value. Uncomment
  and fill hints or export the documented variable before serving. Existing
  operator values remain untouched; remove old generated empty assignments or
  replace empty Spin defaults with `required = true` when the credential should
  remain unset. Add that flag to earlier generated Spin variables lacking both
  `required` and `default`.
- **Deployed manifest construction.** `ManifestAdapterDeployed` is now
  `#[non_exhaustive]`; external callers construct it through `Default` and set
  its public fields.

- **Fallible CLI payload writes.** `edgezero_cli::stream::stdout_line` now
  returns `io::Result<()>`; propagate or handle its write errors. Config diff
  and push previews fail when output cannot be written.
- **Adapter CLI execution and lifecycle contexts.** `Adapter::execute` takes
  `&AdapterExecContext`, and `deploy` takes both `&AdapterDeployContext` and
  `&AdapterExecContext`. `preflight_deploy` and `finalize_deploy` now also receive
  execution context. Apply `exec.env()` to every child process and credential
  lookup while retaining the adapter's selected working directory. Build
  contexts with `AdapterExecContext::new().with_env(...).with_cwd(...)`; honor
  declared manifest/crate/component paths instead of rediscovering them.
- **Adapter-owned local CLI metadata.** Implement `Adapter::local_layout` to
  describe provision artifacts and the optional serve env file. Paths are
  project-relative and receive the manifest, crate, and component declarations
  through `AdapterLocalContext`. Mark secret-bearing env files with
  `LocalArtifactKind::Env`, including custom filenames, and credential-bearing
  TOML with `LocalArtifactKind::SensitiveToml`. The default describes
  only the declared manifest. `AdapterBlueprint` literals now require
  `tool_pins`, `install_notes`, and `local_ignore_patterns`; use empty slices
  when unnecessary. Logging endpoints support the command template vocabulary
  plus `{project_mod}`. `CommandTemplates` also requires `emit_commands`: choose
  `false` when registry execution must honor the adapter manifest.
- **Explicit config cloud target.** `AdapterPushContext::cloud_target` and
  `.with_cloud_target(bool)` replace deploy-command inference. Set
  `[adapters.spin.adapter].cloud = true` for a Spin cloud target and use the context
  builder instead of the removed deploy-command context field.
- **Adapter option ownership.** CLI adapters declare supported option names
  through `Adapter::manifest_option_fields`. Explicit unsupported `cloud` and
  `allowed_outbound_hosts` settings fail validation, including `false` and `[]`.
  Spin owns both; custom adapters can declare support without CLI type checks.
  `ManifestAdapterDefinition::allowed_outbound_hosts` is now
  `Option<Vec<String>>`; use `None` for absence and `Some(hosts)` when configured.
- **Shared CLI utilities.** `edgezero-adapter` filesystem helpers and local
  layout metadata require its `cli` feature, which also enables `regex`.
  Runtime-only builds omit them. Unix atomic replacement syncs the parent
  directory after rename and reports if installation succeeded but that sync
  failed.

- **`edgezero-adapter::Adapter::provision` trait method changed shape.** Was
  `fn provision(&self, root, adapter_manifest, component, stores, dry_run) -> Result<Vec<String>, String>`
  with an `Ok(Vec::new())` default. Is now
  `fn provision(&self, root, adapter_manifest, component, stores, deployed: Option<&AdapterDeployedState>, mode: ProvisionMode, dry_run) -> Result<ProvisionOutcome, String>`
  with **no default** — every `impl Adapter` must supply it. Any
  out-of-tree adapter written against the previous shape will fail to
  compile with two errors: the method's arity and its return type. To
  migrate:
  1. Add a `mode: ProvisionMode` match arm and a `deployed: Option<&AdapterDeployedState>` parameter.
  2. Return `ProvisionOutcome::from_status_lines(lines)` (or
     `::with_deployed(lines, deployed_state)` when the cloud arm has
     an id to write back) instead of `Ok(vec![...])`.
  3. Add a fall-through arm `other => Err(...)` on the `match mode` —
     `ProvisionMode` is `#[non_exhaustive]` and may gain variants.

- **`edgezero-adapter::AdapterDeployedState`, `ProvisionOutcome`,
  and `ProvisionMode` are now `#[non_exhaustive]`.** Struct-literal
  construction from a downstream crate (e.g.
  `ProvisionOutcome { status_lines, deployed }`) no longer compiles.
  Use the new constructors:
  - `ProvisionOutcome::from_status_lines(status_lines)` for local mode
    (which returns `deployed: None`).
  - `ProvisionOutcome::with_deployed(status_lines, deployed_state)`
    for cloud mode that populates the writeback.
  - `AdapterDeployedState::default()` + `.fields.insert(...)` /
    `.sub_tables.insert(...)` for the deployed state.

### Added

- **`provision --local`.** Generate adapter manifests, local store bindings and
  typed secret hints from tracked application configuration. Dry-run uses
  private, bounded staging and redacted previews without modifying the project.

- **`edgezero provision` / `edgezero config push` cross-process advisory
  lock** (`.edgezero/provision.lock` under the manifest's project root).
  Serialises concurrent invocations against the same tree so
  read-modify-write on `.env` / `.dev.vars` / `edgezero.toml` no
  longer silently drops a competing writer's edits. Dry-run skips
  the lock. Auto-released on process exit; the sentinel file itself
  is git-ignored per-machine and safe to delete when no invocation
  is running.

### Fixed

- Preserve operator-authored env records during re-provision, including commented
  hints, custom values, duplicates and multiline quoted values. Redact untrusted
  preview comments and malformed prefixes, and propagate preview output errors.
- Atomically replace env files and deployed manifest state. Check the canonical
  manifest target's directory before creating cloud resources; skip unchanged
  writeback and retain supported top-level manifest symlinks and permissions.

- Resolve relative Cargo homes from the adapter build directory and treat an
  empty `CARGO_HOME` as unset, avoiding stale WASM selection across adapters.
- Generate correct Cloudflare entrypoint and source-watch paths for root crates,
  optional `./` prefixes, and nested Wrangler manifests.
- Keep Fastly's temporary startup manifests private and use Spin's guarded
  normalized destination for component copies. Infer Spin-compatible component
  IDs independently of Cargo artifact names and validate explicit selectors.
- Match Cargo target-directory precedence and configuration-file selection,
  preserve standalone CLI audit builds, and let authentication run before
  application secrets and runtime paths are ready.
- Select Wrangler preview namespaces consistently for local KV writes/reads
  and primary namespaces for remote reads. Accept Axum secret keys whose
  derived environment names do not collide with runtime configuration.
- Stop all captured smoke-test descendants before restoring files, isolate
  explicitly selected Spin cloud smoke state, and correct Axum/Spin secret
  setup instructions. Preserve valid TOML structure in redacted previews.
- Redact native TOML values and comments in local provisioning previews and
  omit source excerpts from native provision parse errors. Serialize Fastly
  local provision with deploy writeback under the same manifest lock.
- Preserve relative application-manifest selection across child working
  directories, accept root adapter crates, and guard Spin artifact copies and
  SQLite paths against intermediate symlinks, including paths containing `..`.

- Preserve quoted multiline env records and original line endings during
  provisioning. Readers, writers, and preview redaction share dotenv grammar;
  generated values are encoded losslessly or rejected explicitly.
- Targeted provision/config commands validate the selected adapter's policy;
  whole-project config validation still checks all registered adapters. Derived
  artifacts are checked in the original tree before previews are staged.
- Remove inherited project-lock borrowing and deploy-wide shell locks. Rust
  provision/config transactions hold real exclusive locks. Native Fastly deploy
  writes a private manifest and merges only `service_id`, preserving unrelated
  concurrent edits and refusing conflicting service changes. Immutable managed
  deploys use a private native startup directory and forward execution context
  to both Fastly and curl. Native Fastly serve requires a `fastly.toml` basename.
- Reject symlinked Cloudflare local persistence before reads as well as writes;
  Wrangler can initialize local state while reading.

- Fastly `service_id` no longer lands under `[local_server]` on
  re-provision; the merged `fastly.toml` correctly carries it at the
  TOML root so `fastly compute deploy` picks it up.
- Fastly cloud `provision` no longer auto-captures `service_id` from
  the gitignored, per-machine `fastly.toml` into tracked
  `[adapters.fastly.deployed]`. Per the v1 contract that writeback
  stays a documented one-time manual copy after `fastly compute deploy`;
  auto-capturing it let a stale local file silently overwrite the
  team's committed service id.
- Cloudflare `.dev.vars` commented `__KEY` placeholder now uses
  `<logical>_staging` (was `<placeholder-<logical>-key>`).
- Cross-adapter `path_mutation_guard` unification (`edgezero-cli`
  test binary): scaffold + push-shim tests share the same mutex, no
  more intermittent CI flakes from PATH-restore races.
- Cloud `config push` now honours the spec's path-containment MUST
  (absolute path + `..` traversal rejection). The strict-local
  "manifest inside adapter crate" check stays `--local`-gated so
  existing cloud fixtures with root-level manifest paths keep
  working.
- Provision `.dev.vars` / `.env` written 0600 on Unix so operator-
  filled secret values are not world-readable.
- Provision line-oriented files reject values containing `\n` or
  `\r` — a malicious env override can no longer split into a second
  `KEY=VALUE` line and inject an unintended env-var.
- Dry-run report emits a **unified diff with 2-line context radius**
  instead of the full pre-image; `.dev.vars` / `.env` operator
  values no longer stream into CI logs.
- Adapter error paths inside `run_local_dry_run` sanitise raw
  `/var/folders/.../edgezero-staging-*` tempdir paths back to the
  project-relative form before surfacing.

### Test coverage

- End-to-end env-overlay: `EDGEZERO__STORES__<KIND>__<LOGICAL>__NAME`
  now has an integration test that drives it from process env
  through `EnvConfig::store_name()` into the emitted `.edgezero/.env`.
- Case-insensitive adapter arg: `--adapter AXUM` against
  `[adapters.axum]` lowercase now covered (previously only the
  reverse direction was locked).
- Cloudflare `wrangler.toml` schema header preserved at line 1 after
  provision merge into an operator-authored doc.
- `*.toml.hbs` scaffold templates walker asserts no `KEY = ""`
  placeholder leaks past `write_baseline_to_disk`.
- Fastly root-scalar assertions (`service_id`, `[[local_server.kv_stores.sessions]]`
  stub row) now reparse-then-index instead of substring-match, so
  the shipped `service_id`-under-`[local_server]` bug's regression
  class is locked.

### Deprecated / renamed

- Three tests named `provision_local_push_after_provision_preserves_*`
  were renamed to `provision_typed_local_re_run_preserves_*` —
  their bodies never invoked `push_config_entries` and the prior
  name misled readers looking for real push→provision coverage.
  Real push→provision coverage now exists as
  `push_after_provision_preserves_*` in each adapter (Axum/Cloudflare/
  Fastly/Spin): each seeds an operator secret into the provision-written
  file, runs `config push --local`, and asserts that file is untouched.

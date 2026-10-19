//! `EdgeZero` CLI library.
//!
//! Exposes the built-in command handlers (`run_build`, `run_deploy`,
//! `run_new`, `run_serve`, `run_config_validate*`) and their argument
//! structs so downstream projects can build their own CLI binary that
//! reuses any subset of edgezero's built-in commands. The default
//! `edgezero` binary (`main.rs`) is a thin wrapper over this library.
//!
//! `run_demo` is an additional contributor-only handler, available only
//! under the `demo-example` feature — it runs the in-repo `app-demo`
//! example and is not meant for downstream CLIs.

// `pub use config::*` re-exports `run_config_validate*` at the crate
// root. The lint is module-scoped (cannot be `#[expect]`-ed per-item);
// downstream CLIs already call `edgezero_cli::run_build` / `run_serve`
// at the crate root, so the new validators follow the same convention.
#![expect(
    clippy::pub_use,
    reason = "config-validate entry points re-export at the crate root to match the existing run_* surface downstream CLIs already use"
)]

#[cfg(feature = "cli")]
mod adapter;
#[cfg(feature = "cli")]
mod auth;
#[cfg(feature = "cli")]
mod config;
#[cfg(feature = "cli")]
mod copy_tree;
#[cfg(all(feature = "cli", feature = "demo-example"))]
mod demo_server;
#[cfg(feature = "cli")]
mod diff;
#[cfg(feature = "cli")]
mod env_file;
#[cfg(feature = "cli")]
mod generator;
#[cfg(feature = "cli")]
mod path_safety;
#[cfg(feature = "cli")]
mod provision;
#[cfg(feature = "cli")]
mod provision_lock;
#[cfg(feature = "cli")]
mod scaffold;
#[cfg(all(test, feature = "cli"))]
mod shared_test_guards;
/// CLI stream-discipline helpers -- `stdout_line`, `info_line`,
/// `prompt`. Every stdout/stderr write in the `edgezero` binary
/// (and in the scaffolded downstream binaries that reuse this
/// crate) MUST go through these so the workspace
/// `clippy::print_stderr` / `clippy::print_stdout` restrictions
/// still catch accidental prints elsewhere as real bugs.
#[cfg(feature = "cli")]
pub mod stream;
#[cfg(all(test, feature = "cli"))]
mod test_support;

/// CLI argument structs (`Args`, `Command`, and the per-command `*Args`
/// types). A `pub mod` so downstream binaries can reuse the built-in
/// command argument types — e.g. `edgezero_cli::args::BuildArgs`.
#[cfg(feature = "cli")]
pub mod args;

#[cfg(feature = "cli")]
pub use auth::run_auth;
#[cfg(feature = "cli")]
pub use config::{
    DiffExit, run_config_diff_typed, run_config_gc, run_config_push, run_config_push_typed,
    run_config_validate, run_config_validate_typed,
};
#[cfg(feature = "cli")]
pub use provision::{run_provision, run_provision_typed};

#[cfg(feature = "cli")]
use args::{
    ActiveVersionArgs, BuildArgs, DeployArgs, HealthcheckArgs, NewArgs, RollbackArgs, ServeArgs,
};
use edgezero_adapter::registry::{AdapterDeployContext, DeployStoreIds};
#[cfg(feature = "cli")]
use edgezero_core::manifest::{Manifest, ManifestLoader, StoreDeclaration};
#[cfg(feature = "cli")]
use path_safety::assert_provision_paths_safe;
#[cfg(feature = "cli")]
use std::collections::BTreeMap;
#[cfg(feature = "cli")]
use std::env;
#[cfg(feature = "cli")]
use std::io::ErrorKind;
#[cfg(feature = "cli")]
use std::path::{Path, PathBuf};

/// CLI output logger: prints `record.args()` verbatim with no
/// timestamps, levels, or module prefixes — the CLI's output IS
/// the user-facing UX, not a debug log. `info` goes to stdout;
/// `warn`/`error` go to stderr. `debug` and `trace` are filtered
/// out by `enabled()` and `LevelFilter::Info`; there is no
/// verbosity flag yet — adding one is a follow-up that would
/// route debug/trace alongside info.
///
/// Replaces the previous `SimpleLogger`-based init: `SimpleLogger`
/// always emitted `INFO [edgezero_cli::xxx] ...` prefixes even
/// with `without_timestamps()`, regressing the user-facing CLI UX
/// the surrounding doc comment promised.
#[cfg(feature = "cli")]
struct CliLogger;

#[cfg(feature = "cli")]
impl log::Log for CliLogger {
    #[inline]
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Info
    }

    #[inline]
    fn flush(&self) {}

    #[inline]
    fn log(&self, record: &log::Record<'_>) {
        use crate::stream::{info_line, stdout_line};
        if !self.enabled(record.metadata()) {
            return;
        }
        match record.level() {
            log::Level::Error | log::Level::Warn => {
                info_line(&format!("{}", record.args()));
            }
            log::Level::Info => {
                stdout_line(&format!("{}", record.args()));
            }
            log::Level::Debug | log::Level::Trace => {}
        }
    }
}

/// Initialize a CLI logger that prints messages without timestamps
/// or level prefixes — the CLI's output IS the user-facing UX, not
/// a debug log. See [`CliLogger`] for the routing rules.
#[cfg(feature = "cli")]
#[inline]
pub fn init_cli_logger() {
    static CLI_LOGGER: CliLogger = CliLogger;
    let _logger_init =
        log::set_logger(&CLI_LOGGER).map(|()| log::set_max_level(log::LevelFilter::Info));
}

/// Build the project for a target edge adapter.
///
/// # Errors
///
/// Returns an error if the manifest cannot be loaded, the adapter is not
/// configured, or the adapter build command fails.
#[cfg(feature = "cli")]
#[inline]
pub fn run_build(args: &BuildArgs) -> Result<(), String> {
    let manifest = load_manifest_optional()?;
    ensure_adapter_defined(&args.adapter, manifest.as_ref())?;
    // Same absolute-path / `..` / symlink guard `serve` and provision
    // apply -- `build` and `deploy` dispatch the declared adapter
    // manifest too, so a poisoned `[adapters.<name>.adapter].manifest`
    // must not steer them at an out-of-tree project either.
    assert_adapter_declared_paths_safe(manifest.as_ref(), &args.adapter)?;
    if let Some(loader) = &manifest {
        log_store_bindings(&args.adapter, loader);
    }
    adapter::execute(
        &args.adapter,
        adapter::Action::Build,
        manifest.as_ref(),
        &args.adapter_args,
    )
}

/// Run the shared absolute-path / `..` / symlink guard on the
/// `[adapters.<name>.adapter]` `manifest` + `crate` strings before any
/// dispatch that resolves them. No-op when no manifest is loaded or the
/// adapter isn't declared.
#[cfg(feature = "cli")]
fn assert_adapter_declared_paths_safe(
    manifest: Option<&ManifestLoader>,
    adapter: &str,
) -> Result<(), String> {
    if let Some(loader) = manifest
        && let Some(root) = loader.manifest().root()
        && let Some((_key, adapter_cfg)) = loader.manifest().adapter_entry(adapter)
    {
        assert_provision_paths_safe(
            root,
            adapter_cfg.adapter.manifest.as_deref(),
            adapter_cfg.adapter.crate_path.as_deref(),
        )?;
    }
    Ok(())
}

/// Acquire the cross-process provision lock for a deploy and return it
/// alongside the env overlay a nested `provision` / `config push` / deploy
/// inherits to BORROW the same lock.
///
/// The lock is held for the whole deploy so a vendor `service_id` writeback
/// (e.g. `fastly compute deploy` rewriting the gitignored `fastly.toml`)
/// can't interleave with a concurrent `provision` / `config push` and lose
/// one side's edits -- the reconcile-on-next-run fallback can't recover a
/// value already discarded.
///
/// The overlay advertises the lock via the CHILD's environment (never the
/// parent's global env, which edition-2024 `set_var` makes unsafe and the
/// workspace forbids). It carries the lock path, the per-holder token (a
/// stale/leaked advertisement whose token no longer matches the lock file is
/// refused), and the borrow DEPTH (so a nested provision serialises on the
/// correct depth-keyed sibling lock -- one level deeper than this deploy
/// holds, avoiding a composed-deploy deadlock while still serialising
/// co-siblings). All three keys OVERRIDE any inherited ancestor
/// advertisement (see `adapter::build_child_env`) so this deploy always
/// advertises ITS OWN lock. The caller MUST hold the returned lock (via its
/// `Drop`) until the deploy finishes.
#[cfg(feature = "cli")]
fn acquire_deploy_lock() -> Result<(provision_lock::ProvisionLock, Vec<(String, String)>), String> {
    let lock_root = manifest_root_for_lock();
    let lock = provision_lock::ProvisionLock::acquire_for_deploy(&lock_root)?;
    let advert = provision_lock::ProvisionLock::lock_path_for(&lock_root);
    let overlay = vec![
        (
            provision_lock::LOCK_ENV.to_owned(),
            advert.to_string_lossy().into_owned(),
        ),
        (
            provision_lock::LOCK_TOKEN_ENV.to_owned(),
            lock.token().to_owned(),
        ),
        (
            provision_lock::SIBLING_DEPTH_ENV.to_owned(),
            lock.child_sibling_depth().to_string(),
        ),
    ];
    Ok((lock, overlay))
}

/// Deploy the project to a target edge adapter.
///
/// # Errors
///
/// Returns an error if the manifest cannot be loaded, the adapter is not
/// configured, or the adapter deploy command fails.
#[cfg(feature = "cli")]
#[inline]
pub fn run_deploy(args: &DeployArgs) -> Result<(), String> {
    // Reject reserved staging-lifecycle spellings in the passthrough. `--staging` is
    // a typed flag (before `--`); if it — or the renamed-away `--stage` — appears in
    // the passthrough (after `--`), the operator meant to stage but `args.staging` is
    // false, so this would silently run a PRODUCTION deploy and forward the token to
    // a manifest deploy command that ignores the flag. Fail closed instead of aliasing.
    if let Some(flag) = args.adapter_args.iter().find(|arg| {
        let text = arg.as_str();
        // Bare (`--staging`) AND equals-forms (`--staging=true`, `--stage=1`): a
        // prefix match closes the `--stage=…` bypass that a bare `==` would miss.
        matches!(text, "--staging" | "--stage")
            || text.starts_with("--staging=")
            || text.starts_with("--stage=")
    }) {
        return Err(format!(
            "`{flag}` is not a passthrough deploy arg. To stage, use the typed flag: \
             `deploy --adapter {} --staging` (before any `--`). Refusing to run a \
             production deploy with `{flag}` after `--`.",
            args.adapter
        ));
    }

    let manifest = load_manifest_optional()?;
    ensure_adapter_defined(&args.adapter, manifest.as_ref())?;
    assert_adapter_declared_paths_safe(manifest.as_ref(), &args.adapter)?;
    let (_deploy_lock, overlay) = acquire_deploy_lock()?;
    let manifest_stores = manifest.as_ref().map(|loader| &loader.manifest().stores);
    let declared_ids = |declaration: Option<&StoreDeclaration>| {
        declaration
            .map(|store| store.ids.clone())
            .unwrap_or_default()
    };
    let deploy_stores = DeployStoreIds {
        config: declared_ids(manifest_stores.and_then(|declared| declared.config.as_ref())),
        kv: declared_ids(manifest_stores.and_then(|declared| declared.kv.as_ref())),
        secrets: declared_ids(manifest_stores.and_then(|declared| declared.secrets.as_ref())),
    };
    let variable_defaults = manifest
        .as_ref()
        .map(|loader| manifest_variable_defaults(loader.manifest(), &args.adapter))
        .unwrap_or_default();
    let (adapter_manifest_path, adapter_manifest_path_error) =
        match resolve_adapter_manifest_path(manifest.as_ref(), &args.adapter) {
            Ok(path) => (path.map(PathBuf::from), None),
            Err(err) => (None, Some(err)),
        };
    let application_manifest_path = loaded_application_manifest_path(manifest.as_ref())?;
    let application_release_root = resolve_application_release_root(
        args.application_release.as_deref(),
        application_manifest_path.as_deref(),
    )?;
    let context = AdapterDeployContext {
        adapter_manifest_path,
        application_manifest_path,
        application_release_root,
        service_id: args.service_id.clone(),
        stores: deploy_stores,
        staging: args.staging,
        variable_defaults,
    };

    adapter::deploy(
        &args.adapter,
        &context,
        adapter_manifest_path_error.as_deref(),
        manifest.as_ref(),
        &args.adapter_args,
        &overlay,
    )
}

#[cfg(feature = "cli")]
fn manifest_variable_defaults(manifest: &Manifest, adapter: &str) -> BTreeMap<String, String> {
    manifest
        .environment_for(adapter)
        .variables
        .into_iter()
        .filter_map(|binding| binding.value.map(|value| (binding.env, value)))
        .collect()
}

#[cfg(feature = "cli")]
fn loaded_application_manifest_path(
    loader: Option<&ManifestLoader>,
) -> Result<Option<PathBuf>, String> {
    if loader.is_none() {
        return Ok(None);
    }
    let path = env::var("EDGEZERO_MANIFEST")
        .map_or_else(|_| PathBuf::from("edgezero.toml"), PathBuf::from);
    let canonical = path.canonicalize().map_err(|error| {
        format!(
            "could not resolve loaded application manifest {}: {error}",
            path.display()
        )
    })?;
    if !canonical.is_file() {
        return Err(format!(
            "loaded application manifest {} is not a regular file",
            canonical.display()
        ));
    }
    Ok(Some(canonical))
}

#[cfg(feature = "cli")]
fn resolve_application_release_root(
    requested_root: Option<&Path>,
    application_manifest: Option<&Path>,
) -> Result<Option<PathBuf>, String> {
    let Some(requested_root_path) = requested_root else {
        return Ok(None);
    };
    let root = requested_root_path.canonicalize().map_err(|error| {
        format!(
            "could not resolve application release root {}: {error}",
            requested_root_path.display()
        )
    })?;
    if !root.is_dir() {
        return Err(format!(
            "application release root {} is not a directory",
            root.display()
        ));
    }
    let manifest = application_manifest
        .ok_or_else(|| "--application-release requires a loaded application manifest".to_owned())?;
    if !manifest.starts_with(&root) {
        return Err(format!(
            "loaded application manifest {} is outside application release root {}",
            manifest.display(),
            root.display()
        ));
    }
    Ok(Some(root))
}

/// Resolve the absolute path of the adapter's platform manifest
/// (`[adapters.<adapter>.adapter].manifest`), CONFINED to the loaded
/// manifest's own directory. Used by the Fastly staged deploy to target
/// the operator-selected app in a monorepo.
///
/// `Ok(None)` when there is no loaded manifest, no root, no entry for the
/// adapter, or no `manifest` key (the adapter then does its own cwd
/// search). The `manifest` value is committed app source, but an absolute
/// path, a `../` traversal, or a symlink escape would otherwise let a
/// credential-bearing `fastly compute build/deploy` run against source
/// OUTSIDE the repository `source-revision` describes and outside the
/// dirty-source guard — so the resolved target is canonicalized and
/// required to be a regular file beneath the (canonicalized) manifest
/// root, and any escape is a hard error.
#[cfg(feature = "cli")]
fn resolve_adapter_manifest_path(
    loader: Option<&ManifestLoader>,
    adapter: &str,
) -> Result<Option<String>, String> {
    use std::fs;
    use std::path::Path;

    let Some(manifest) = loader.map(ManifestLoader::manifest) else {
        return Ok(None);
    };
    let (Some(root), Some((_canonical, cfg))) = (manifest.root(), manifest.adapter_entry(adapter))
    else {
        return Ok(None);
    };
    let Some(rel) = cfg.adapter.manifest.as_deref() else {
        return Ok(None);
    };
    if Path::new(rel).is_absolute() {
        return Err(format!(
            "adapter '{adapter}' manifest path {rel:?} must be relative to the manifest, not absolute"
        ));
    }
    let root_real = fs::canonicalize(root).map_err(|err| {
        format!(
            "could not resolve the manifest root {}: {err}",
            root.display()
        )
    })?;
    let candidate = root.join(rel);
    let candidate_real = fs::canonicalize(&candidate).map_err(|err| {
        format!(
            "could not resolve adapter '{adapter}' manifest {}: {err}",
            candidate.display()
        )
    })?;
    if !candidate_real.starts_with(&root_real) {
        return Err(format!(
            "adapter '{adapter}' manifest {} resolves outside the application manifest root {}",
            candidate_real.display(),
            root_real.display()
        ));
    }
    if !candidate_real.is_file() {
        return Err(format!(
            "adapter '{adapter}' manifest {} is not a regular file",
            candidate_real.display()
        ));
    }
    Ok(Some(candidate_real.to_string_lossy().into_owned()))
}

/// Probe a deployed version's health (Fastly staging lifecycle)
/// and return `Err` when the probe is unhealthy after retries so
/// the process exits non-zero (letting a CI caller gate rollback on
/// failure).
///
/// # Errors
///
/// Returns an error if the manifest cannot be loaded, the adapter is
/// not configured / registered, the adapter does not support
/// healthchecks, or the probe is unhealthy after all retries.
#[cfg(feature = "cli")]
#[inline]
pub fn run_healthcheck(args: &HealthcheckArgs) -> Result<(), String> {
    // Manifest-independent, like `active-version`: a pure API/curl probe keyed on
    // explicit flags, never a manifest-command override. Not loading the manifest
    // keeps it correct regardless of the current directory (monorepo safety).
    let mut passthrough: Vec<String> = vec![
        "--service-id".to_owned(),
        args.service_id.clone(),
        "--version".to_owned(),
        args.version.clone(),
        "--domain".to_owned(),
        args.domain.clone(),
        "--path".to_owned(),
        args.path.clone(),
    ];
    if args.staging {
        passthrough.push("--staging".to_owned());
    }
    passthrough.extend([
        "--retry".to_owned(),
        args.retry.to_string(),
        "--retry-delay".to_owned(),
        args.retry_delay.to_string(),
        "--timeout".to_owned(),
        args.timeout.to_string(),
    ]);
    adapter::execute(
        &args.adapter,
        adapter::Action::Healthcheck,
        None,
        &passthrough,
    )
}

/// Roll a service back (Fastly staging lifecycle):
/// production activates the previous version; staging deactivates the
/// staged version.
///
/// # Errors
///
/// Returns an error if the manifest cannot be loaded, the adapter is
/// not configured / registered, the adapter does not support
/// rollback, or the rollback API call fails.
#[cfg(feature = "cli")]
#[inline]
pub fn run_rollback(args: &RollbackArgs) -> Result<(), String> {
    // Manifest-independent, like `active-version` / `healthcheck`: a pure API
    // operation keyed on explicit flags, never a manifest-command override.
    let mut passthrough: Vec<String> = vec![
        "--service-id".to_owned(),
        args.service_id.clone(),
        "--version".to_owned(),
        args.version.clone(),
    ];
    if args.staging {
        passthrough.push("--staging".to_owned());
    } else if let Some(target) = args.rollback_to.as_deref() {
        // Production activates an EXPLICIT target — Fastly has no field to infer
        // a previously-live version from a staged one, so the caller passes the
        // version captured before the superseding deploy.
        passthrough.push("--rollback-to".to_owned());
        passthrough.push(target.to_owned());
    } else {
        return Err(
            "a production rollback requires --rollback-to (the version to re-activate). Fastly \
             exposes no metadata to infer it, so it must be captured before the deploy that \
             superseded it -- run `active-version` (it prints `version=<N>`) before deploying, \
             or wire the deploy-fastly GitHub action's `previous-version` output. If it was \
             never captured, choose the target from the service's version history -- Fastly \
             cannot identify it for you. Pass --staging to deactivate a staged version instead."
                .to_owned(),
        );
    }
    adapter::execute(&args.adapter, adapter::Action::Rollback, None, &passthrough)
}

/// Resolve and print the currently-active service version as `version=<N>`.
///
/// Captured BEFORE a deploy so it can be threaded to a later production
/// rollback — Fastly's version list has no field to infer a previously-live
/// version afterward.
///
/// # Errors
///
/// Returns an error if the manifest cannot be loaded, the adapter is not
/// configured, or the active version cannot be resolved.
#[cfg(feature = "cli")]
#[inline]
pub fn run_active_version(args: &ActiveVersionArgs) -> Result<(), String> {
    // No manifest load: `active-version` is a pure Fastly-API operation keyed on
    // `--adapter` + `--service-id`, and `EmitVersion` can never be a
    // manifest-command override (see `adapter::manifest_command`). Loading the
    // manifest would only couple it to the current directory — breaking it in a
    // monorepo where a stray root `edgezero.toml` shadows the app's. The adapter
    // registry still validates `--adapter`.
    adapter::execute(
        &args.adapter,
        adapter::Action::EmitVersion,
        None,
        &["--service-id".to_owned(), args.service_id.clone()],
    )
}

/// Run a local simulation for a target edge adapter.
///
/// # Errors
///
/// Returns an error if the manifest cannot be loaded, the adapter is not
/// configured, or the adapter serve command fails.
#[cfg(feature = "cli")]
#[inline]
pub fn run_serve(args: &ServeArgs) -> Result<(), String> {
    let manifest = load_manifest_optional()?;
    ensure_adapter_defined(&args.adapter, manifest.as_ref())?;

    // Adapter-scoped env-file load: `axum` reads `.edgezero/.env`,
    // `spin` reads the `.env` next to the resolved `spin.toml`.
    // `cloudflare` and `fastly` read their own files (`.dev.vars`,
    // `[local_server.*]`) via their emulators and need no CLI-side
    // help.
    //
    // Spin's env path is derived from
    // `[adapters.spin.adapter].manifest` (see
    // `resolve_serve_env_file`), so an operator-authored poisoned
    // manifest string like `manifest = "/etc/spin.toml"` or
    // `manifest = "../../../secrets/env"` would resolve to an
    // out-of-tree `.env` that the parser would happily read and
    // hand to the spawned adapter. Run the same absolute-path +
    // `..` traversal + symlink-component guard provision and
    // config already use before touching the resolved path.
    //
    // The overlay is threaded into the spawned child via
    // `Command::env` (see `adapter::execute_with_env_overlay`)
    // rather than `std::env::set_var`. On Unix `setenv` /
    // `getenv` are not thread-safe: a downstream multithreaded
    // process calling `run_serve` from one thread while another
    // thread reads `std::env::var` observes a torn read. Passing
    // the overlay through `Command::env` keeps every mutation on
    // the `Command`'s private map — no shared state, no race.
    assert_adapter_declared_paths_safe(manifest.as_ref(), &args.adapter)?;
    let mut env_overlay: Vec<(String, String)> = Vec::new();
    if let Some(loader) = manifest.as_ref()
        && let Some(root) = loader.manifest().root()
    {
        let manifest_data = loader.manifest();
        if let Some(env_path) = resolve_serve_env_file(manifest_data, &args.adapter, root) {
            // The env-file chain (e.g. `<root>/.edgezero/.env`) is NOT
            // covered by `assert_adapter_declared_paths_safe`, which only
            // guards the declared `.manifest` / `.crate`. Guard it
            // UNCONDITIONALLY -- the walk stops at the first missing
            // component, so even with NO `.env` it still rejects a
            // symlinked `.edgezero` directory, which Axum reads
            // `local-config-*.json` from at request time. Gating this on
            // `.env` existing left that config read able to follow a
            // symlink off-tree and inject externally-controlled values.
            use edgezero_adapter::env_file::reject_symlink_components;
            reject_symlink_components(root, &env_path)?;
            if env_path.exists() {
                env_overlay = env_file::parse_env_overlay(&env_path)?;
            }
        }
    }

    adapter::execute_with_env_overlay(
        &args.adapter,
        adapter::Action::Serve,
        manifest.as_ref(),
        &[],
        &env_overlay,
    )
}

/// Return the `.env` file `run_serve` should pre-load into the
/// process environment for the selected adapter, or `None` if the
/// adapter reads its env file directly (cloudflare, fastly) or the
/// adapter is unknown.
///
/// `axum` maps to `<manifest_root>/.edgezero/.env` (the writer
/// target). `spin` maps to `<spin.toml parent>/.env`, derived from the
/// required `[adapters.spin.adapter].manifest` (the same directory
/// provision writes `.env` into); a spin adapter without `.manifest`
/// resolves to `None` — there is no `.crate`/root fallback.
///
/// The adapter name is matched case-insensitively so `--adapter Spin`
/// or `SPIN` resolves the same as `spin`.
#[cfg(feature = "cli")]
fn resolve_serve_env_file(
    manifest: &Manifest,
    adapter_name: &str,
    manifest_root: &Path,
) -> Option<PathBuf> {
    let adapter_lower = adapter_name.to_ascii_lowercase();
    match adapter_lower.as_str() {
        "axum" => Some(manifest_root.join(".edgezero").join(".env")),
        "spin" => {
            // Spin provision writes `.env` next to the resolved
            // `spin.toml` (see
            // `edgezero-adapter-spin/src/cli/provision_local.rs`:
            // `env_path = spin_dir.join(".env")`). Derive the same path
            // here from the REQUIRED `[adapters.spin.adapter].manifest`.
            // A nested manifest like
            // `[adapters.spin.adapter].manifest = "crates/spin/config/spin.toml"`
            // places `.env` at `crates/spin/config/.env`.
            //
            // There is deliberately NO `.crate`/root fallback: build,
            // deploy, provision, and config all reject a spin adapter
            // whose `.manifest` is unset, so an absent `.manifest` is a
            // malformed manifest, not a legacy shape to accommodate.
            // Deriving from `.crate` would look in the wrong directory
            // and miss the runtime env-label + typed `SPIN_VARIABLE_*`
            // lines provision wrote. Absent `.manifest` => no overlay.
            let (_key, adapter_cfg) = manifest.adapter_entry(adapter_name)?;
            let manifest_rel = adapter_cfg.adapter.manifest.as_deref()?;
            let manifest_abs = manifest_root.join(manifest_rel);
            let env_dir = manifest_abs
                .parent()
                .map_or_else(|| manifest_root.to_path_buf(), Path::to_path_buf);
            Some(env_dir.join(".env"))
        }
        _ => None,
    }
}

/// Create a new `EdgeZero` app skeleton.
///
/// # Errors
///
/// Returns an error if the project cannot be scaffolded.
#[cfg(feature = "cli")]
#[inline]
pub fn run_new(args: &NewArgs) -> Result<(), String> {
    generator::generate_new(args).map_err(|err| err.to_string())
}

/// Run the bundled `app-demo` example locally on the axum dev server.
///
/// Contributor-only: available only under the `demo-example` feature,
/// which pulls in the in-repo `examples/app-demo` crate.
///
/// # Errors
///
/// Returns an error if the demo server fails to start.
#[cfg(all(feature = "cli", feature = "demo-example"))]
#[inline]
pub fn run_demo() -> Result<(), String> {
    demo_server::run_demo()
}

#[cfg(feature = "cli")]
fn store_bindings_message(adapter_name: &str, manifest: &ManifestLoader) -> Option<String> {
    let manifest_data = manifest.manifest();
    if !manifest_data.secret_store_enabled(adapter_name) {
        return None;
    }

    // Note: the configured binding identifier is intentionally NOT included in
    // this log line. CodeQL's `rust/cleartext-logging` rule taints any value
    // returned by a function whose name contains "secret" (it can't tell
    // metadata from secret material), and adapters/operators can read the
    // binding name from their own `edgezero.toml` if they need to verify it.
    let message = match adapter_name {
        "axum" => {
            "[edgezero] secrets enabled for axum -- ensure the required environment variables are set for local runs"
        }
        "cloudflare" => {
            "[edgezero] secrets enabled for cloudflare -- ensure the required secret bindings exist in wrangler"
        }
        _ => {
            "[edgezero] secrets enabled -- ensure the configured secret store is provisioned on the target platform"
        }
    };

    Some(message.to_owned())
}

#[cfg(feature = "cli")]
fn log_store_bindings(adapter_name: &str, manifest: &ManifestLoader) {
    if let Some(message) = store_bindings_message(adapter_name, manifest) {
        log::info!("{message}");
    }
}

#[cfg(feature = "cli")]
fn ensure_adapter_defined(
    adapter_name: &str,
    manifest_loader: Option<&ManifestLoader>,
) -> Result<(), String> {
    if let Some(loader) = manifest_loader {
        if loader.manifest().adapter_entry(adapter_name).is_some() {
            return Ok(());
        }
        let available: Vec<String> = loader.manifest().adapters.keys().cloned().collect();
        if available.is_empty() {
            Err(format!(
                "adapter `{adapter_name}` is not configured in edgezero.toml (no adapters defined)"
            ))
        } else {
            Err(format!(
                "adapter `{}` is not configured in edgezero.toml (available: {})",
                adapter_name,
                available.join(", ")
            ))
        }
    } else {
        Ok(())
    }
}

#[cfg(feature = "cli")]
/// Directory whose `.edgezero/provision.lock` guards this project, derived
/// from the same manifest source `load_manifest_optional` uses so the
/// deploy lock and the provision/push locks target the same sentinel.
#[cfg(feature = "cli")]
fn manifest_root_for_lock() -> PathBuf {
    let path = env::var("EDGEZERO_MANIFEST")
        .map_or_else(|_| PathBuf::from("edgezero.toml"), PathBuf::from);
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

fn load_manifest_optional() -> Result<Option<ManifestLoader>, String> {
    let (path, explicit) = env::var("EDGEZERO_MANIFEST").map_or_else(
        |_| (PathBuf::from("edgezero.toml"), false),
        |raw| (PathBuf::from(raw), true),
    );

    match ManifestLoader::from_path(&path) {
        Ok(loader) => Ok(Some(loader)),
        // A missing default `edgezero.toml` is permissive — built-in adapters
        // can still serve the request. An explicitly set `EDGEZERO_MANIFEST`
        // that points at a missing file is a hard error so typos surface
        // instead of silently falling back.
        Err(err) if err.kind() == ErrorKind::NotFound && !explicit => Ok(None),
        Err(err) => Err(format!("failed to load {}: {err}", path.display())),
    }
}

#[cfg(test)]
#[cfg(feature = "cli")]
mod tests {
    use super::*;
    use crate::test_support::{BASIC_MANIFEST, EnvOverride, manifest_guard};
    use edgezero_adapter::registry::AdapterExecContext;
    use edgezero_adapter::registry::{
        self as adapter_registry, Adapter, AdapterAction, DeployOwnership,
    };
    use edgezero_core::manifest::ManifestLoader;
    #[cfg(unix)]
    use edgezero_core::test_env::PathPrepend;
    use std::collections::BTreeMap;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{LazyLock, Mutex};
    use tempfile::TempDir;

    const SPIN_MANIFEST_LOWER: &str = r#"
[app]
name = "demo-app"

[adapters.spin.adapter]
crate = "crates/spin"
manifest = "crates/spin/spin.toml"

[adapters.spin.commands]
build = "echo"
deploy = "echo"
serve = "echo"
"#;

    const SPIN_MANIFEST_MIXED_CASE: &str = r#"
[app]
name = "demo-app"

[adapters.Spin.adapter]
crate = "crates/spin"
manifest = "crates/spin/spin.toml"

[adapters.Spin.commands]
build = "echo"
deploy = "echo"
serve = "echo"
"#;

    /// Nested-manifest fixture: `[adapters.spin.adapter].manifest`
    /// points at a sub-directory inside `.crate`. Provision writes
    /// `.env` next to the resolved `spin.toml`
    /// (`crates/spin/config/.env`) — `run_serve` must load from
    /// the same path.
    const SPIN_MANIFEST_NESTED: &str = r#"
[app]
name = "demo-app"

[adapters.spin.adapter]
crate = "crates/spin"
manifest = "crates/spin/config/spin.toml"

[adapters.spin.commands]
build = "echo"
deploy = "echo"
serve = "echo"
"#;
    const PREFLIGHT_MANIFEST_COMMAND: usize = 0;
    const PREFLIGHT_ADAPTER_MANAGED: usize = 1;
    const PREFLIGHT_ERROR: usize = 2;

    static DEPLOY_PREFLIGHT_MODE: AtomicUsize = AtomicUsize::new(PREFLIGHT_MANIFEST_COMMAND);
    static DEPLOY_CALLS: AtomicUsize = AtomicUsize::new(0);
    static DEPLOY_CONTEXT: LazyLock<Mutex<Option<AdapterDeployContext>>> =
        LazyLock::new(|| Mutex::new(None));
    static DEPLOY_FINALIZE_CALLS: AtomicUsize = AtomicUsize::new(0);
    static DEPLOY_PREFLIGHT_CONTEXT: LazyLock<Mutex<Option<AdapterDeployContext>>> =
        LazyLock::new(|| Mutex::new(None));
    static RECORDING_DEPLOY_ADAPTER: RecordingDeployAdapter = RecordingDeployAdapter;

    struct RecordingDeployAdapter;

    #[expect(
        clippy::missing_trait_methods,
        reason = "the recording adapter exercises only deploy preflight, deploy dispatch, and finalization"
    )]
    impl Adapter for RecordingDeployAdapter {
        fn provision(
            &self,
            _root: &Path,
            _manifest: Option<&str>,
            _component: Option<&str>,
            _stores: &edgezero_adapter::ProvisionStores<'_>,
            _deployed: Option<&edgezero_adapter::AdapterDeployedState>,
            _mode: edgezero_adapter::ProvisionMode,
            _dry_run: bool,
        ) -> Result<edgezero_adapter::ProvisionOutcome, String> {
            Ok(edgezero_adapter::ProvisionOutcome::default())
        }

        fn deploy(
            &self,
            context: &AdapterDeployContext,
            _args: &[String],
            _ctx: &AdapterExecContext<'_>,
        ) -> Result<(), String> {
            *DEPLOY_CONTEXT
                .lock()
                .map_err(|err| format!("deploy context lock poisoned: {err}"))? =
                Some(context.clone());
            DEPLOY_CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn execute(
            &self,
            action: AdapterAction,
            _args: &[String],
            _ctx: &AdapterExecContext<'_>,
        ) -> Result<(), String> {
            if action == AdapterAction::Deploy {
                return Err("managed deployment must call Adapter::deploy".to_owned());
            }
            Err(format!("unexpected recording adapter action: {action:?}"))
        }

        fn finalize_deploy(
            &self,
            _context: &AdapterDeployContext,
            _command_output: Option<&str>,
        ) -> Result<(), String> {
            DEPLOY_FINALIZE_CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn name(&self) -> &'static str {
            "recording_deploy_test"
        }

        fn preflight_deploy(
            &self,
            context: &AdapterDeployContext,
            _args: &[String],
        ) -> Result<DeployOwnership, String> {
            *DEPLOY_PREFLIGHT_CONTEXT
                .lock()
                .map_err(|err| format!("deploy preflight context lock poisoned: {err}"))? =
                Some(context.clone());
            match DEPLOY_PREFLIGHT_MODE.load(Ordering::SeqCst) {
                PREFLIGHT_ADAPTER_MANAGED => Ok(DeployOwnership::AdapterManaged),
                PREFLIGHT_ERROR => Err("recording preflight failed".to_owned()),
                _ => Ok(DeployOwnership::ManifestCommand),
            }
        }
    }

    fn reset_recording_deploy_adapter(mode: usize) {
        adapter_registry::register_adapter(&RECORDING_DEPLOY_ADAPTER);
        DEPLOY_PREFLIGHT_MODE.store(mode, Ordering::SeqCst);
        DEPLOY_CALLS.store(0, Ordering::SeqCst);
        *DEPLOY_CONTEXT.lock().expect("deploy context lock") = None;
        DEPLOY_FINALIZE_CALLS.store(0, Ordering::SeqCst);
        *DEPLOY_PREFLIGHT_CONTEXT
            .lock()
            .expect("deploy preflight context lock") = None;
    }

    #[cfg(all(
        unix,
        feature = "edgezero-adapter-cloudflare",
        feature = "edgezero-adapter-spin"
    ))]
    #[test]
    fn registry_deploy_preserves_child_env_and_native_vendor_arguments() {
        use edgezero_core::test_env::PathPrepend;
        use std::os::unix::fs::PermissionsExt as _;
        let _guard = manifest_guard().lock().expect("manifest guard");
        for (adapter, vendor, filename) in [
            ("cloudflare", "wrangler", "wrangler.toml"),
            ("spin", "spin", "spin.toml"),
        ] {
            let dir = TempDir::new().expect("temporary project");
            let crate_dir = dir.path().join("crates/worker");
            let config_dir = crate_dir.join("config");
            fs::create_dir_all(&config_dir).expect("config directory");
            fs::write(
                crate_dir.join("Cargo.toml"),
                "[package]\nname=\"worker\"\nversion=\"0.1.0\"\n",
            )
            .expect("crate manifest");
            fs::write(config_dir.join(filename), "name = \"demo\"\n").expect("provider manifest");
            let manifest = dir.path().join("edgezero.toml");
            fs::write(&manifest, format!("[app]\nname=\"demo\"\n[adapters.{adapter}.adapter]\ncrate=\"crates/worker\"\nmanifest=\"crates/worker/config/{filename}\"\n[[environment.variables]]\nname=\"DEPLOY_MARKER\"\nenv=\"EDGEZERO_MERGE_DEPLOY_MARKER\"\nvalue=\"from-manifest\"\nadapters=[\"{adapter}\"]\n")).expect("application manifest");
            let argv = dir.path().join("argv");
            let child_env = dir.path().join("env");
            let shim = dir.path().join(vendor);
            fs::write(&shim, format!("#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf '%s' \"$EDGEZERO_MERGE_DEPLOY_MARKER\" > '{}'\n", argv.display(), child_env.display())).expect("vendor shim");
            fs::set_permissions(&shim, fs::Permissions::from_mode(0o755)).expect("executable shim");
            let _path = PathPrepend::new(dir.path());
            let _manifest = EnvOverride::set("EDGEZERO_MANIFEST", &manifest);
            run_deploy(&DeployArgs {
                adapter: adapter.to_owned(),
                application_release: None,
                service_id: None,
                staging: false,
                adapter_args: vec!["--help".to_owned()],
            })
            .expect("registry deploy");
            let recorded = fs::read_to_string(argv).expect("recorded vendor arguments");
            assert!(
                !recorded.contains("--manifest-path"),
                "private manifest directive reached {vendor}: {recorded}"
            );
            assert!(
                recorded.lines().any(|arg| arg == "--help"),
                "operator arguments reach {vendor}"
            );
            assert!(
                recorded.contains(config_dir.join(filename).to_string_lossy().as_ref()),
                "declared provider manifest reaches {vendor}"
            );
            assert_eq!(
                fs::read_to_string(child_env).expect("child environment"),
                "from-manifest"
            );
        }
    }

    #[test]
    fn load_manifest_optional_hard_errors_when_explicit_env_path_missing() {
        // An explicit `EDGEZERO_MANIFEST` pointing at a missing file must
        // fail loudly so typos surface instead of silently falling back to
        // the built-in adapters.
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let manifest_path = temp.path().join("missing.toml");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        match load_manifest_optional() {
            Err(err) => assert!(
                err.contains("missing.toml"),
                "error should name the bad path: {err}"
            ),
            Ok(_) => panic!("expected hard error for missing explicit EDGEZERO_MANIFEST"),
        }
    }

    #[test]
    fn load_manifest_optional_returns_none_when_default_missing() {
        // Default `edgezero.toml` missing is the no-manifest case — built-in
        // adapters can still serve the request, so this remains permissive.
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let _env = EnvOverride::remove("EDGEZERO_MANIFEST");
        let original_cwd = env::current_dir().expect("cwd");
        env::set_current_dir(temp.path()).expect("cd temp");
        let result = load_manifest_optional();
        env::set_current_dir(original_cwd).expect("restore cwd");
        match result {
            Ok(None) => {}
            Ok(Some(_)) => panic!("expected no manifest in a temp dir"),
            Err(err) => panic!("default missing edgezero.toml should be permissive: {err}"),
        }
    }

    #[test]
    fn load_manifest_optional_reads_manifest() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(&manifest_path, BASIC_MANIFEST).expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let manifest = load_manifest_optional()
            .expect("load result")
            .expect("manifest present");
        assert!(manifest.manifest().adapters.contains_key("fastly"));
    }

    #[cfg(not(windows))]
    #[test]
    fn run_deploy_manifest_command_forwards_adapter_args_verbatim() {
        // With `[adapters.fastly.commands] deploy = ...` the deploy runs
        // as a shell command, NOT the built-in Fastly path — so anything
        // the caller (e.g. the deploy action) passes as an adapter arg,
        // `--non-interactive` included, must reach that command verbatim.
        // The EdgeZero-internal `--manifest-path` must NOT: the shell
        // command's own CLI has no such flag.
        let _lock = manifest_guard().lock().expect("manifest guard");

        let temp = TempDir::new().expect("temp dir");
        let curl = temp.path().join("curl");
        fs::write(
            &curl,
            "#!/bin/sh\ncat >/dev/null\nprintf '[{\"number\":42,\"active\":true,\"locked\":true,\"staging\":false,\"deployed\":true,\"environments\":[]}]\\n200'\n",
        )
        .expect("write curl fake");
        let mut permissions = fs::metadata(&curl).expect("curl metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&curl, permissions).expect("make curl fake executable");
        let _path = PathPrepend::new(temp.path());
        let _token = EnvOverride::set("FASTLY_API_TOKEN", "test-token");
        let args_file = temp.path().join("argv.txt");
        let script = temp.path().join("record.sh");
        fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" > '{}'\necho version=42\n",
                args_file.display()
            ),
        )
        .expect("write record script");

        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(
            &manifest_path,
            format!(
                "[app]\nname = \"demo-app\"\n\n[adapters.fastly.adapter]\ncrate = \"crates/demo-fastly\"\nmanifest = \"crates/demo-fastly/fastly.toml\"\n\n[adapters.fastly.commands]\ndeploy = \"sh {}\"\n",
                script.display()
            ),
        )
        .expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);

        let args = DeployArgs {
            adapter: "fastly".to_owned(),
            application_release: None,
            adapter_args: vec!["--non-interactive".to_owned()],
            service_id: Some("SVC1".to_owned()),
            staging: false,
        };
        run_deploy(&args).expect("manifest deploy command runs");

        let forwarded = fs::read_to_string(&args_file).expect("command recorded its args");
        assert_eq!(
            forwarded.trim(),
            "--service-id SVC1 --non-interactive",
            "manifest deploy command must receive the adapter args verbatim"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn run_staging_deploy_resolves_explicit_manifest_even_with_custom_production_command() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(
            &manifest_path,
            "[app]\nname = \"demo-app\"\n\n[adapters.fastly.adapter]\ncrate = \"crates/demo-fastly\"\nmanifest = \"missing/fastly.toml\"\n\n[adapters.fastly.commands]\ndeploy = \"true\"\n",
        )
        .expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);

        let err = run_deploy(&DeployArgs {
            adapter: "fastly".to_owned(),
            application_release: None,
            adapter_args: Vec::new(),
            service_id: Some("SVC1".to_owned()),
            staging: true,
        })
        .expect_err("staging must resolve the explicitly selected fastly.toml");

        assert!(
            err.contains("missing/fastly.toml") && err.contains("could not resolve"),
            "staging reports the selected missing manifest before discovery: {err}"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn run_custom_deploy_with_stores_runs_without_registered_adapter() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let marker = temp.path().join("deploy-ran");
        let script = temp.path().join("deploy.sh");
        fs::write(
            &script,
            format!("#!/bin/sh\ntouch '{}'\n", marker.display()),
        )
        .expect("write deploy script");
        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(
            &manifest_path,
            format!(
                "[app]\nname = \"demo-app\"\n\n[stores.config]\nids = [\"app_config\"]\n\n[adapters.unregistered_test.adapter]\n\n[adapters.unregistered_test.commands]\ndeploy = \"sh {}\"\n",
                script.display()
            ),
        )
        .expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);

        run_deploy(&DeployArgs {
            adapter: "unregistered_test".to_owned(),
            application_release: None,
            adapter_args: Vec::new(),
            service_id: None,
            staging: false,
        })
        .expect("an unregistered custom adapter can run its manifest deploy command");

        assert!(
            marker.exists(),
            "the custom deploy command should run without a registered adapter"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn deploy_preflight_adapter_managed_bypasses_manifest_command_and_receives_context() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        reset_recording_deploy_adapter(PREFLIGHT_ADAPTER_MANAGED);
        let _secret = EnvOverride::set("EDGEZERO_TEST_SECRET", "fixture-secret");
        let temp = TempDir::new().expect("temp dir");
        let marker = temp.path().join("manifest-deploy-ran");
        let adapter_dir = temp.path().join("nested/adapter");
        fs::create_dir_all(&adapter_dir).expect("adapter dir");
        let adapter_manifest = adapter_dir.join("adapter.toml");
        fs::write(&adapter_manifest, "name = \"recording\"\n").expect("adapter manifest");
        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(
            &manifest_path,
            format!(
                r#"[app]
name = "demo-app"

[[environment.variables]]
name = "APPLICABLE_DEFAULT"
env = "EDGEZERO_TEST_APPLICABLE"
value = "from-manifest"
adapters = ["recording_deploy_test"]

[[environment.variables]]
name = "OTHER_DEFAULT"
env = "EDGEZERO_TEST_OTHER"
value = "other-adapter"
adapters = ["other"]

[[environment.variables]]
name = "UNSET_DEFAULT"
env = "EDGEZERO_TEST_UNSET"

[[environment.secrets]]
name = "PRIVATE_TOKEN"
env = "EDGEZERO_TEST_SECRET"
value = "must-not-leak"
adapters = ["recording_deploy_test"]

[adapters.recording_deploy_test.adapter]
crate = "crates/demo"
manifest = "nested/adapter/adapter.toml"

[adapters.recording_deploy_test.commands]
deploy = "touch '{}'"
"#,
                marker.display()
            ),
        )
        .expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);

        run_deploy(&DeployArgs {
            adapter: "recording_deploy_test".to_owned(),
            application_release: None,
            adapter_args: Vec::new(),
            service_id: None,
            staging: false,
        })
        .expect("adapter-managed deploy succeeds");

        assert!(!marker.exists(), "adapter ownership bypasses the command");
        assert_eq!(DEPLOY_CALLS.load(Ordering::SeqCst), 1);
        assert_eq!(DEPLOY_FINALIZE_CALLS.load(Ordering::SeqCst), 0);
        let preflight_context = DEPLOY_PREFLIGHT_CONTEXT
            .lock()
            .expect("deploy preflight context lock")
            .clone()
            .expect("Adapter::preflight_deploy captured its context");
        let deployed_context = DEPLOY_CONTEXT
            .lock()
            .expect("deploy context lock")
            .clone()
            .expect("Adapter::deploy captured its context");
        let expected_adapter_manifest = adapter_manifest.canonicalize().expect("canonical path");
        assert_eq!(
            preflight_context.adapter_manifest_path,
            Some(expected_adapter_manifest.clone())
        );
        assert_eq!(
            deployed_context.adapter_manifest_path,
            Some(expected_adapter_manifest)
        );
        assert_eq!(
            deployed_context.variable_defaults,
            BTreeMap::from([(
                "EDGEZERO_TEST_APPLICABLE".to_owned(),
                "from-manifest".to_owned()
            )])
        );
        assert_eq!(
            deployed_context.application_manifest_path,
            Some(
                manifest_path
                    .canonicalize()
                    .expect("canonical app manifest")
            )
        );
        assert!(deployed_context.application_release_root.is_none());
    }

    #[cfg(not(windows))]
    #[test]
    fn deploy_preflight_receives_confined_application_release_paths() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        reset_recording_deploy_adapter(PREFLIGHT_ADAPTER_MANAGED);
        let release = TempDir::new().expect("release root");
        let adapter_dir = release.path().join("adapter");
        fs::create_dir_all(&adapter_dir).expect("adapter directory");
        let adapter_manifest = adapter_dir.join("fastly.toml");
        fs::write(&adapter_manifest, "name = \"recording\"\n").expect("adapter manifest");
        let manifest_path = release.path().join("edgezero.toml");
        fs::write(
            &manifest_path,
            "[app]\nname = \"demo\"\n[adapters.recording_deploy_test.adapter]\ncrate = \"crates/demo\"\nmanifest = \"adapter/fastly.toml\"\n",
        )
        .expect("application manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);

        run_deploy(&DeployArgs {
            adapter: "recording_deploy_test".to_owned(),
            application_release: Some(release.path().to_path_buf()),
            ..DeployArgs::default()
        })
        .expect("managed release deploy");

        let context = DEPLOY_CONTEXT
            .lock()
            .expect("deploy context lock")
            .clone()
            .expect("deploy context");
        assert_eq!(
            context.application_release_root,
            Some(release.path().canonicalize().unwrap())
        );
        assert_eq!(
            context.application_manifest_path,
            Some(manifest_path.canonicalize().unwrap())
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn deploy_rejects_application_manifest_outside_release_root() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        reset_recording_deploy_adapter(PREFLIGHT_ADAPTER_MANAGED);
        let release = TempDir::new().expect("release root");
        let application = TempDir::new().expect("application root");
        let manifest_path = application.path().join("edgezero.toml");
        fs::write(
            &manifest_path,
            "[app]\nname = \"demo\"\n[adapters.recording_deploy_test.adapter]\ncrate = \"crates/demo\"\n",
        )
        .expect("application manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);

        let error = run_deploy(&DeployArgs {
            adapter: "recording_deploy_test".to_owned(),
            application_release: Some(release.path().to_path_buf()),
            ..DeployArgs::default()
        })
        .expect_err("application manifest must be confined to release");

        assert!(error.contains("outside application release"), "{error}");
        assert_eq!(DEPLOY_CALLS.load(Ordering::SeqCst), 0);
    }

    #[cfg(not(windows))]
    #[test]
    fn deploy_preflight_adapter_managed_rejects_invalid_manifest_before_deploy() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        reset_recording_deploy_adapter(PREFLIGHT_ADAPTER_MANAGED);
        let temp = TempDir::new().expect("temp dir");
        let marker = temp.path().join("manifest-deploy-ran");
        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(
            &manifest_path,
            format!(
                "[app]\nname = \"demo-app\"\n\n[adapters.recording_deploy_test.adapter]\ncrate = \"crates/demo\"\nmanifest = \"nested/missing.toml\"\n\n[adapters.recording_deploy_test.commands]\ndeploy = \"touch '{}'\"\n",
                marker.display()
            ),
        )
        .expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);

        let err = run_deploy(&DeployArgs {
            adapter: "recording_deploy_test".to_owned(),
            application_release: None,
            adapter_args: Vec::new(),
            service_id: None,
            staging: false,
        })
        .expect_err("managed deploy requires its configured adapter manifest");

        assert!(
            err.contains("nested/missing.toml") && err.contains("could not resolve"),
            "resolution error is preserved: {err}"
        );
        assert!(!marker.exists(), "managed ownership bypasses the command");
        assert_eq!(DEPLOY_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(DEPLOY_FINALIZE_CALLS.load(Ordering::SeqCst), 0);
        assert!(
            DEPLOY_CONTEXT
                .lock()
                .expect("deploy context lock")
                .is_none(),
            "invalid manifest fails before Adapter::deploy"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn deploy_preflight_manifest_command_ignores_invalid_adapter_manifest() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        reset_recording_deploy_adapter(PREFLIGHT_MANIFEST_COMMAND);
        let temp = TempDir::new().expect("temp dir");
        let marker = temp.path().join("manifest-deploy-ran");
        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(
            &manifest_path,
            format!(
                "[app]\nname = \"demo-app\"\n\n[adapters.recording_deploy_test.adapter]\ncrate = \"crates/demo\"\nmanifest = \"nested/missing.toml\"\n\n[adapters.recording_deploy_test.commands]\ndeploy = \"touch '{}'\"\n",
                marker.display()
            ),
        )
        .expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);

        run_deploy(&DeployArgs {
            adapter: "recording_deploy_test".to_owned(),
            application_release: None,
            adapter_args: Vec::new(),
            service_id: None,
            staging: false,
        })
        .expect("manifest ownership does not require an adapter manifest");

        assert!(marker.exists(), "the manifest deploy command runs");
        assert_eq!(DEPLOY_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(DEPLOY_FINALIZE_CALLS.load(Ordering::SeqCst), 1);
        assert!(
            DEPLOY_CONTEXT
                .lock()
                .expect("deploy context lock")
                .is_none()
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn deploy_preflight_manifest_command_with_stores_rejects_invalid_adapter_manifest() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        reset_recording_deploy_adapter(PREFLIGHT_MANIFEST_COMMAND);
        let temp = TempDir::new().expect("temp dir");
        let marker = temp.path().join("manifest-deploy-ran");
        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(
            &manifest_path,
            format!(
                "[app]\nname = \"demo-app\"\n\n[stores.config]\nids = [\"app_config\"]\n\n[adapters.recording_deploy_test.adapter]\ncrate = \"crates/demo\"\nmanifest = \"nested/missing.toml\"\n\n[adapters.recording_deploy_test.commands]\ndeploy = \"touch '{}'\"\n",
                marker.display()
            ),
        )
        .expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);

        let err = run_deploy(&DeployArgs {
            adapter: "recording_deploy_test".to_owned(),
            application_release: None,
            adapter_args: Vec::new(),
            service_id: None,
            staging: false,
        })
        .expect_err("registered finalization with stores requires its adapter manifest");

        assert!(
            err.contains("nested/missing.toml") && err.contains("could not resolve"),
            "resolution error is preserved: {err}"
        );
        assert!(
            !marker.exists(),
            "manifest resolution fails before the command"
        );
        assert_eq!(DEPLOY_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(DEPLOY_FINALIZE_CALLS.load(Ordering::SeqCst), 0);
    }

    #[cfg(not(windows))]
    #[test]
    fn deploy_preflight_error_prevents_manifest_command() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        reset_recording_deploy_adapter(PREFLIGHT_ERROR);
        let temp = TempDir::new().expect("temp dir");
        let marker = temp.path().join("manifest-deploy-ran");
        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(
            &manifest_path,
            format!(
                "[app]\nname = \"demo-app\"\n\n[adapters.recording_deploy_test.adapter]\ncrate = \"crates/demo\"\nmanifest = \"crates/demo/config.toml\"\n\n[adapters.recording_deploy_test.commands]\ndeploy = \"touch '{}'\"\n",
                marker.display()
            ),
        )
        .expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);

        let err = run_deploy(&DeployArgs {
            adapter: "recording_deploy_test".to_owned(),
            application_release: None,
            adapter_args: Vec::new(),
            service_id: None,
            staging: false,
        })
        .expect_err("preflight failure stops deployment");

        assert!(err.contains("recording preflight failed"), "{err}");
        assert!(!marker.exists(), "preflight fails before the command runs");
        assert_eq!(DEPLOY_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(DEPLOY_FINALIZE_CALLS.load(Ordering::SeqCst), 0);
    }

    #[cfg(not(windows))]
    #[test]
    fn run_deploy_store_backed_fastly_bypasses_manifest_command_and_requires_release() {
        use std::os::unix::fs::PermissionsExt as _;

        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let adapter_dir = temp.path().join("crates/demo-fastly");
        fs::create_dir_all(&adapter_dir).expect("adapter dir");
        fs::write(adapter_dir.join("fastly.toml"), "name = \"demo\"\n").expect("fastly manifest");

        let marker = temp.path().join("manifest-command-ran");
        let deploy_script = temp.path().join("deploy.sh");
        fs::write(
            &deploy_script,
            format!("#!/bin/sh\ntouch '{}'\n", marker.display()),
        )
        .expect("deploy script");
        let mut deploy_perms = fs::metadata(&deploy_script).expect("meta").permissions();
        deploy_perms.set_mode(0o755);
        fs::set_permissions(&deploy_script, deploy_perms).expect("chmod deploy");

        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(
            &manifest_path,
            format!(
                "[app]\nname = \"demo-app\"\n\n[stores.secrets]\nids = [\"credentials\"]\n\n[adapters.fastly.adapter]\ncrate = \"crates/demo-fastly\"\nmanifest = \"crates/demo-fastly/fastly.toml\"\n\n[adapters.fastly.commands]\ndeploy = \"{}\"\n",
                deploy_script.display()
            ),
        )
        .expect("edgezero manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _manifest = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let _selector = EnvOverride::set(
            "EDGEZERO__STORES__SECRETS__CREDENTIALS__NAME",
            "credentials-staging",
        );

        let error = run_deploy(&DeployArgs {
            adapter: "fastly".to_owned(),
            application_release: None,
            adapter_args: vec!["--non-interactive".to_owned()],
            service_id: Some("SVC1".to_owned()),
            staging: false,
        })
        .expect_err("store-backed Fastly deploy requires an immutable release");

        assert!(
            error.contains("--application-release"),
            "managed ownership reaches the release verifier: {error}"
        );
        assert!(!marker.exists(), "the manifest deploy command is bypassed");
    }

    #[test]
    fn run_deploy_rejects_staging_spellings_in_passthrough() {
        // A reserved lifecycle spelling after `--` must FAIL CLOSED before any deploy
        // action runs — never silently route to production. The guard is the first
        // thing run_deploy does, so no manifest/adapter setup is needed to reach it.
        // Bare AND equals-forms: `--stage=true`/`--staging=1` must not slip past a
        // bare-equality check and route to production.
        for flag in ["--stage", "--staging", "--stage=true", "--staging=1"] {
            let args = DeployArgs {
                adapter: "fastly".to_owned(),
                application_release: None,
                adapter_args: vec![flag.to_owned()],
                service_id: Some("SVC1".to_owned()),
                staging: false,
            };
            let err = run_deploy(&args)
                .expect_err("a reserved staging spelling in passthrough must be rejected");
            assert!(
                err.contains("--staging") && err.contains(flag),
                "the error must name the flag and point at the typed --staging: {err}"
            );
        }
    }

    #[test]
    fn ensure_adapter_defined_accepts_known_adapter() {
        let loader = ManifestLoader::load_from_str(BASIC_MANIFEST);
        ensure_adapter_defined("fastly", Some(&loader)).expect("known adapter");
    }

    #[test]
    fn ensure_adapter_defined_reports_unknown_adapter() {
        let loader = ManifestLoader::load_from_str(BASIC_MANIFEST);
        let err = ensure_adapter_defined("cloudflare", Some(&loader)).expect_err("should err");
        assert!(err.contains("available"));
        assert!(err.contains("fastly"));
    }

    #[test]
    fn ensure_adapter_defined_allows_when_manifest_missing() {
        ensure_adapter_defined("fastly", None).expect("manifest missing -> permissive");
    }

    #[cfg(not(windows))]
    #[test]
    fn run_build_executes_manifest_command() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(&manifest_path, BASIC_MANIFEST).expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let args = BuildArgs {
            adapter: "fastly".to_owned(),
            adapter_args: Vec::new(),
        };
        run_build(&args).expect("build command runs");
    }

    #[cfg(not(windows))]
    #[test]
    fn run_deploy_executes_manifest_command() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(&manifest_path, BASIC_MANIFEST).expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let args = DeployArgs {
            adapter: "fastly".to_owned(),
            application_release: None,
            adapter_args: Vec::new(),
            // No service id → the production version-emit step is
            // skipped, so this test exercises only the
            // manifest `deploy` command path.
            service_id: None,
            staging: false,
        };
        run_deploy(&args).expect("deploy command runs");
    }

    #[cfg(not(windows))]
    #[test]
    fn run_serve_executes_manifest_command() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let manifest_path = temp.path().join("edgezero.toml");
        fs::write(&manifest_path, BASIC_MANIFEST).expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let args = ServeArgs {
            adapter: "fastly".to_owned(),
        };
        run_serve(&args).expect("serve command runs");
    }

    // Write an edgezero.toml declaring an adapter `manifest` and load it, so
    // `manifest.root()` is set — the confinement is only meaningful for a
    // file-backed manifest with a real root. Returns the loader so each test can
    // call `resolve_adapter_manifest_path` and assert on its Result.
    #[cfg(not(windows))]
    fn manifest_loader_declaring(dir: &Path, manifest_rel: &str) -> ManifestLoader {
        let manifest_path = dir.join("edgezero.toml");
        fs::write(
            &manifest_path,
            format!(
                "[app]\nname = \"demo-app\"\nentry = \"crates/demo-core\"\n\n[adapters.fastly.adapter]\ncrate = \"crates/demo-fastly\"\nmanifest = \"{manifest_rel}\"\n"
            ),
        )
        .expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        load_manifest_optional()
            .expect("load manifest")
            .expect("manifest present")
    }

    #[cfg(not(windows))]
    #[test]
    fn resolve_adapter_manifest_path_accepts_an_in_root_file() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        fs::create_dir_all(temp.path().join("crates/demo-fastly")).expect("mkdir");
        fs::write(temp.path().join("crates/demo-fastly/fastly.toml"), "")
            .expect("write fastly.toml");
        let loader = manifest_loader_declaring(temp.path(), "crates/demo-fastly/fastly.toml");
        let resolved = resolve_adapter_manifest_path(Some(&loader), "fastly")
            .expect("in-root manifest resolves")
            .expect("a path is returned");
        assert!(resolved.ends_with("crates/demo-fastly/fastly.toml"));
    }

    #[cfg(not(windows))]
    #[test]
    fn resolve_adapter_manifest_path_rejects_absolute() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let loader = manifest_loader_declaring(temp.path(), "/etc/passwd");
        let err = resolve_adapter_manifest_path(Some(&loader), "fastly")
            .expect_err("an absolute manifest path must be rejected");
        assert!(err.contains("absolute"), "{err}");
    }

    #[cfg(not(windows))]
    #[test]
    fn resolve_adapter_manifest_path_rejects_traversal() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        let outside = TempDir::new().expect("outside dir");
        fs::write(outside.path().join("fastly.toml"), "").expect("write outside fastly.toml");
        let temp = TempDir::new().expect("temp dir");
        // A `../` escape to a real file outside the manifest root must fail closed.
        let rel = format!(
            "../{}/fastly.toml",
            outside.path().file_name().unwrap().to_string_lossy()
        );
        let loader = manifest_loader_declaring(temp.path(), &rel);
        let err = resolve_adapter_manifest_path(Some(&loader), "fastly")
            .expect_err("a traversal manifest path must be rejected");
        assert!(err.contains("outside"), "{err}");
    }

    #[cfg(not(windows))]
    #[test]
    fn resolve_adapter_manifest_path_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;
        let _lock = manifest_guard().lock().expect("manifest guard");
        let outside = TempDir::new().expect("outside dir");
        fs::write(outside.path().join("fastly.toml"), "").expect("write outside fastly.toml");
        let temp = TempDir::new().expect("temp dir");
        // A symlink that points at a file outside the root must not smuggle it in.
        symlink(
            outside.path().join("fastly.toml"),
            temp.path().join("fastly.toml"),
        )
        .expect("create symlink");
        let loader = manifest_loader_declaring(temp.path(), "fastly.toml");
        let err = resolve_adapter_manifest_path(Some(&loader), "fastly")
            .expect_err("a symlink escaping the manifest root must be rejected");
        assert!(err.contains("outside"), "{err}");
    }

    #[test]
    fn secret_store_binding_is_readable_from_manifest() {
        let manifest_with_secrets = r#"
[app]
name = "demo-app"
entry = "crates/demo-core"

[stores.secrets]
ids = ["MY_SECRETS"]

[adapters.fastly.commands]
build = "echo build"
deploy = "echo deploy"
serve = "echo serve"
"#;
        let loader = ManifestLoader::load_from_str(manifest_with_secrets);
        let declared = loader
            .manifest()
            .stores
            .secrets
            .as_ref()
            .expect("[stores.secrets] declared");
        assert_eq!(declared.ids, vec!["MY_SECRETS".to_owned()]);
        assert_eq!(declared.default_id(), "MY_SECRETS");
    }

    #[test]
    fn store_bindings_message_is_adapter_specific() {
        let loader = ManifestLoader::load_from_str(
            r#"
[stores.secrets]
ids = ["MY_SECRETS"]
"#,
        );

        let axum = store_bindings_message("axum", &loader).expect("axum message");
        assert!(axum.contains("environment variables"));

        let cloudflare = store_bindings_message("cloudflare", &loader).expect("cloudflare message");
        assert!(cloudflare.contains("wrangler"));

        let fastly = store_bindings_message("fastly", &loader).expect("fastly message");
        assert!(fastly.contains("secrets enabled"));
    }

    #[test]
    fn store_bindings_message_is_absent_without_secret_store() {
        let loader = ManifestLoader::load_from_str("[app]\nname = \"x\"\n");
        assert!(store_bindings_message("fastly", &loader).is_none());
    }

    #[test]
    fn resolve_serve_env_file_axum_returns_dot_edgezero_dot_env() {
        // axum's `.env` lives under `<manifest_root>/.edgezero/.env`
        // — the target the line writer produces.
        let loader = ManifestLoader::load_from_str(BASIC_MANIFEST);
        let root = PathBuf::from("/tmp/proj");
        let resolved = resolve_serve_env_file(loader.manifest(), "axum", &root)
            .expect("axum arm returns Some");
        assert_eq!(resolved, root.join(".edgezero").join(".env"));
    }

    #[test]
    fn resolve_serve_env_file_spin_derives_env_from_manifest_parent() {
        // spin's `.env` lives next to the resolved `spin.toml` — the
        // target the line writer produces — derived from the required
        // `[adapters.spin.adapter].manifest`.
        let loader = ManifestLoader::load_from_str(SPIN_MANIFEST_LOWER);
        let root = PathBuf::from("/tmp/proj");
        let resolved = resolve_serve_env_file(loader.manifest(), "spin", &root)
            .expect("spin arm returns Some");
        assert_eq!(resolved, root.join("crates/spin").join(".env"));
    }

    #[test]
    fn resolve_serve_env_file_spin_without_manifest_returns_none() {
        // `.manifest` is required for spin (build/deploy/provision/config
        // all reject its absence). run_serve must NOT fall back to
        // `.crate`/root -- that would look for `.env` in the wrong
        // directory. A crate-only spin adapter resolves to None.
        let loader = ManifestLoader::load_from_str(
            "[app]\nname = \"demo-app\"\n\n[adapters.spin.adapter]\ncrate = \"crates/spin\"\n\n[adapters.spin.commands]\nbuild = \"echo\"\ndeploy = \"echo\"\nserve = \"echo\"\n",
        );
        let root = PathBuf::from("/tmp/proj");
        assert!(
            resolve_serve_env_file(loader.manifest(), "spin", &root).is_none(),
            "no `.manifest` => no serve env overlay (no legacy `.crate` fallback)"
        );
    }

    #[test]
    fn resolve_serve_env_file_spin_honors_nested_manifest_parent() {
        // Regression: provision writes `.env` next to the resolved
        // `spin.toml` (`spin_dir.join(".env")` in
        // `edgezero-adapter-spin/src/cli/provision_local.rs`).
        // With `[adapters.spin.adapter].manifest = "crates/spin/config/spin.toml"`,
        // provision writes `crates/spin/config/.env`. run_serve
        // MUST load from the same path — deriving from
        // `.crate = "crates/spin"` alone would look at
        // `crates/spin/.env` and miss every EDGEZERO__STORES__…__NAME
        // + typed SPIN_VARIABLE_* line provision just wrote.
        let loader = ManifestLoader::load_from_str(SPIN_MANIFEST_NESTED);
        let root = PathBuf::from("/tmp/proj");
        let resolved = resolve_serve_env_file(loader.manifest(), "spin", &root)
            .expect("spin arm returns Some");
        assert_eq!(
            resolved,
            root.join("crates/spin/config").join(".env"),
            "spin serve MUST load .env from the manifest's parent dir (matches provision's writeback path), NOT from the adapter crate root"
        );
    }

    #[test]
    fn resolve_serve_env_file_cloudflare_returns_none() {
        // Wrangler reads `.dev.vars` itself; run_serve does not touch it.
        let loader = ManifestLoader::load_from_str(BASIC_MANIFEST);
        let root = PathBuf::from("/tmp/proj");
        assert!(resolve_serve_env_file(loader.manifest(), "cloudflare", &root).is_none());
    }

    #[test]
    fn resolve_serve_env_file_fastly_returns_none() {
        // Fastly's emulator reads `[local_server.*]` blocks in
        // `fastly.toml`; run_serve does not touch it.
        let loader = ManifestLoader::load_from_str(BASIC_MANIFEST);
        let root = PathBuf::from("/tmp/proj");
        assert!(resolve_serve_env_file(loader.manifest(), "fastly", &root).is_none());
    }

    #[cfg(not(windows))]
    #[test]
    fn run_serve_rejects_absolute_spin_adapter_manifest_path() {
        // Regression: `resolve_serve_env_file` derives Spin's `.env`
        // path from `[adapters.spin.adapter].manifest.parent()` and
        // `run_serve` then reads that file. A poisoned absolute
        // manifest string like `/etc/spin.toml` would resolve to
        // `/etc/.env` — `env_file::load_into_process_env` would
        // read it and inject its lines into the process env,
        // subsequently inherited by the spawned adapter. The path
        // safety guard must fire BEFORE the read.
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let manifest_path = temp.path().join("edgezero.toml");
        let poisoned = r#"
[app]
name = "demo-app"

[adapters.spin.adapter]
crate = "crates/spin"
manifest = "/etc/spin.toml"

[adapters.spin.commands]
build = "echo"
deploy = "echo"
serve = "echo"
"#;
        fs::write(&manifest_path, poisoned).expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let args = ServeArgs {
            adapter: "spin".to_owned(),
        };
        let err = run_serve(&args).expect_err(
            "run_serve MUST refuse to resolve an absolute [adapters.spin.adapter].manifest",
        );
        assert!(
            err.contains("must be a project-relative path"),
            "path-safety guard must fire before the .env read: {err}"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn run_build_rejects_absolute_adapter_manifest_path() {
        // `build`/`deploy` dispatch the declared adapter manifest just
        // like `serve`, so the same path-safety guard must fire.
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let manifest_path = temp.path().join("edgezero.toml");
        let poisoned = "[app]\nname = \"demo-app\"\n\n[adapters.spin.adapter]\ncrate = \"crates/spin\"\nmanifest = \"/etc/spin.toml\"\n";
        fs::write(&manifest_path, poisoned).expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let args = BuildArgs {
            adapter: "spin".to_owned(),
            adapter_args: Vec::new(),
        };
        let err = run_build(&args)
            .expect_err("run_build MUST refuse an absolute [adapters.spin.adapter].manifest");
        assert!(
            err.contains("must be a project-relative path"),
            "build path-safety guard must fire: {err}"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn run_deploy_rejects_parent_traversal_in_adapter_manifest() {
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let manifest_path = temp.path().join("edgezero.toml");
        let poisoned = "[app]\nname = \"demo-app\"\n\n[adapters.spin.adapter]\ncrate = \"crates/spin\"\nmanifest = \"../../../outside/spin.toml\"\n";
        fs::write(&manifest_path, poisoned).expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let args = DeployArgs {
            application_release: None,
            adapter: "spin".to_owned(),
            adapter_args: Vec::new(),
            service_id: None,
            staging: false,
        };
        let err =
            run_deploy(&args).expect_err("run_deploy MUST refuse a `..`-traversing manifest path");
        assert!(
            err.contains("`..`"),
            "deploy path-safety guard must fire: {err}"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn run_serve_rejects_parent_traversal_in_spin_adapter_manifest() {
        // Symmetric to the absolute-path guard: `..` in the
        // manifest string would resolve `.env` above the project
        // root. Must fire BEFORE the read.
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let manifest_path = temp.path().join("edgezero.toml");
        let poisoned = r#"
[app]
name = "demo-app"

[adapters.spin.adapter]
crate = "crates/spin"
manifest = "../../../outside/spin.toml"

[adapters.spin.commands]
build = "echo"
deploy = "echo"
serve = "echo"
"#;
        fs::write(&manifest_path, poisoned).expect("write manifest");
        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let args = ServeArgs {
            adapter: "spin".to_owned(),
        };
        let err = run_serve(&args)
            .expect_err("run_serve MUST refuse a `..` traversal in the manifest string");
        assert!(
            err.contains("must not contain `..` traversal"),
            "traversal guard must fire before the .env read: {err}"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn run_serve_loads_env_file_into_process_env_before_spawning_child() {
        // Contract test (spec §"Adapter-scoped env-file load"): the
        // `.env` next to the resolved `spin.toml` must have its
        // `KEY=VALUE` lines set into the process env BEFORE
        // `adapter::execute` runs the manifest's serve command, so
        // the SPAWNED CHILD sees the values.
        //
        // Test fidelity note: proving the parent process loaded the
        // .env (via `env::var(marker_key)` after `run_serve`) is
        // necessary but not sufficient — the spec asks for
        // intercepting the spawned child's env. We achieve that
        // here by making the serve command a small shell script
        // that writes its OWN observed value of the marker to a
        // file. The test then reads that file and asserts the
        // child saw the same value the parent set.
        //
        // Prior to c38cb54 the resolver looked at `<crate>/.env`;
        // the manifest below deliberately places `.env` under a
        // nested manifest parent to prove the fix loads THAT file
        // (not a same-named file under `.crate`).
        let marker_key = "EDGEZERO_TEST_SERVE_ENV_LOADED_MARKER";
        let marker_value = "spin-nested-manifest-parent";
        let _lock = manifest_guard().lock().expect("manifest guard");
        let _pre = EnvOverride::remove(marker_key);

        let temp = TempDir::new().expect("temp dir");

        // The child writes its observed marker value here. We pick
        // a path INSIDE the tempdir so parallel test runs never
        // collide, and thread it into the manifest's serve command
        // string via `format!`.
        let observed_path = temp.path().join("child_observed.txt");
        let observed_path_display = observed_path.to_string_lossy();

        let manifest_path = temp.path().join("edgezero.toml");
        // Serve command: a `sh -c` script that writes the child's
        // OWN view of $MARKER_KEY to `observed_path`. If the
        // spawned child inherits nothing (i.e. `run_serve` did NOT
        // load the .env before dispatch), the file will contain an
        // empty value and the assertion below will fail with a
        // useful diff.
        let manifest_body = format!(
            r#"
[app]
name = "demo-app"

[adapters.spin.adapter]
crate = "crates/spin"
manifest = "crates/spin/config/spin.toml"

[adapters.spin.commands]
build = "echo"
deploy = "echo"
serve = 'sh -c "printf %s \"${{{marker_key}:-<unset>}}\" > {observed_path_display}"'
"#,
        );
        fs::write(&manifest_path, &manifest_body).expect("write manifest");

        // Provision writes `.env` next to the RESOLVED spin.toml
        // (`crates/spin/config/.env` here). Seed one directly.
        let env_dir = temp.path().join("crates/spin/config");
        fs::create_dir_all(&env_dir).expect("mkdir nested spin dir");
        let env_path = env_dir.join(".env");
        fs::write(&env_path, format!("{marker_key}={marker_value}\n")).expect("seed nested .env");

        // Also seed a decoy `.env` at the pre-fix location
        // (`crates/spin/.env`) with a DIFFERENT value so a
        // regression to the crate-based lookup surfaces as a
        // wrong-value assertion in the CHILD-observed file, not a
        // silent pass.
        let decoy_dir = temp.path().join("crates/spin");
        fs::create_dir_all(&decoy_dir).expect("mkdir spin crate");
        let decoy_env = decoy_dir.join(".env");
        fs::write(
            &decoy_env,
            format!("{marker_key}=THIS_MUST_NOT_BE_LOADED_from_crate_root\n"),
        )
        .expect("seed decoy .env");

        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let args = ServeArgs {
            adapter: "spin".to_owned(),
        };
        run_serve(&args).expect("run_serve must succeed with the sh-echo serve command");

        // Child-side check (the spec's real ask): the spawned
        // shell wrote its OWN view of $MARKER_KEY to disk. That
        // value MUST match the .env's value — proving the child
        // inherited the Command::env overlay before executing.
        //
        // Note: the load path
        // now threads the overlay through Command::env instead of
        // std::env::set_var, so the PARENT's env stays unchanged.
        // A dedicated no-parent-mutation assertion lives in
        // `run_serve_does_not_mutate_parent_process_env_yet_child_sees_env_file_value`
        // below; this test intentionally stays focused on the
        // manifest-parent .env resolution regression.
        let child_observed =
            fs::read_to_string(&observed_path).expect("child wrote observed marker");
        assert_eq!(
            child_observed,
            marker_value,
            "spawned serve child MUST see the marker from the nested-manifest-parent .env — \
             got {child_observed:?} at {}",
            observed_path.display()
        );

        // Sanity: this test intentionally poisons the decoy path;
        // the child-observed assertion above proves the resolver
        // preferred the manifest-derived nested path over it.
        assert!(env_path.exists() && decoy_env.exists());
    }

    #[test]
    fn resolve_serve_env_file_adapter_name_is_case_insensitive() {
        // Manifest declares `[adapters.Spin]` (mixed case). Passing
        // `--adapter spin` (or SPIN) must still resolve to the Spin
        // arm's `<spin.toml parent>/.env` — the arm lowercases once and
        // matches on the lowercase form.
        let loader = ManifestLoader::load_from_str(SPIN_MANIFEST_MIXED_CASE);
        let root = PathBuf::from("/tmp/proj");
        let expected = root.join("crates/spin").join(".env");
        assert_eq!(
            resolve_serve_env_file(loader.manifest(), "spin", &root),
            Some(expected.clone())
        );
        assert_eq!(
            resolve_serve_env_file(loader.manifest(), "SPIN", &root),
            Some(expected)
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn run_serve_does_not_mutate_parent_process_env_yet_child_sees_env_file_value() {
        // Regression: the pre-fix
        // `env_file::load_into_process_env` wrote every KEY=VALUE
        // pair via `std::env::set_var`. That's not thread-safe on
        // Unix and can race with any concurrent reader in a
        // multithreaded downstream process embedding
        // `edgezero_cli::run_serve`.
        //
        // The current design threads the `.env` overlay through
        // `Command::env` (see `env_file::parse_env_overlay` +
        // `adapter::execute_with_env_overlay`), so the CHILD sees
        // the file's values at exec time while the PARENT's shared
        // `environ` stays untouched.
        //
        // This test seeds a nested Spin `.env` with a marker, runs
        // `run_serve`, and asserts BOTH:
        //   (a) the child's observed marker equals the .env value
        //       (proving the overlay reached the spawned process),
        //   (b) the parent's `env::var(marker_key)` is still None
        //       (proving `run_serve` did NOT call `set_var`).
        let marker_key = "EDGEZERO_TEST_SERVE_ENV_NO_PARENT_MUTATION_MARKER";
        let marker_value = "child-only-do-not-leak-to-parent";
        let _lock = manifest_guard().lock().expect("manifest guard");
        // Pre-condition: the marker MUST be unset in the parent, or
        // `existing env wins` would drop the overlay and the test
        // could pass vacuously.
        let _pre = EnvOverride::remove(marker_key);

        let temp = TempDir::new().expect("temp dir");
        let observed_path = temp.path().join("child_observed.txt");
        let observed_path_display = observed_path.to_string_lossy();
        let manifest_path = temp.path().join("edgezero.toml");
        let manifest_body = format!(
            r#"
[app]
name = "demo-app"

[adapters.spin.adapter]
crate = "crates/spin"
manifest = "crates/spin/spin.toml"

[adapters.spin.commands]
build = "echo"
deploy = "echo"
serve = 'sh -c "printf %s \"${{{marker_key}:-<unset>}}\" > {observed_path_display}"'
"#,
        );
        fs::write(&manifest_path, &manifest_body).expect("write manifest");
        let env_dir = temp.path().join("crates/spin");
        fs::create_dir_all(&env_dir).expect("mkdir spin");
        fs::write(
            env_dir.join(".env"),
            format!("{marker_key}={marker_value}\n"),
        )
        .expect("seed .env");

        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let args = ServeArgs {
            adapter: "spin".to_owned(),
        };
        run_serve(&args).expect("run_serve must succeed");

        // (a) CHILD side — must see the marker.
        let child_observed =
            fs::read_to_string(&observed_path).expect("child wrote observed marker");
        assert_eq!(
            child_observed, marker_value,
            "spawned child MUST inherit the .env overlay: got {child_observed:?}"
        );

        // (b) PARENT side — must NOT see the marker. This is the
        // load-bearing thread-safety assertion.
        let parent_observed = env::var(marker_key).ok();
        assert!(
            parent_observed.is_none(),
            "run_serve MUST NOT mutate the parent process env; found `{marker_key}={parent_observed:?}`. \
             Regression: `env_file::parse_env_overlay` bypassed and someone re-introduced \
             `std::env::set_var` on the load path."
        );
    }

    #[cfg(unix)]
    #[test]
    fn run_serve_refuses_symlinked_env_file() {
        // A symlinked `.env` in the serve env-file chain could inject
        // externally-controlled values into the spawned child. run_serve
        // must refuse it before parsing, and before the child spawns.
        use std::os::unix::fs::symlink;
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let observed_path = temp.path().join("child_observed.txt");
        let observed_path_display = observed_path.to_string_lossy();
        let manifest_path = temp.path().join("edgezero.toml");
        let manifest_body = format!(
            r#"
[app]
name = "demo-app"

[adapters.spin.adapter]
crate = "crates/spin"
manifest = "crates/spin/spin.toml"

[adapters.spin.commands]
build = "echo"
deploy = "echo"
serve = 'sh -c "printf ran > {observed_path_display}"'
"#,
        );
        fs::write(&manifest_path, &manifest_body).expect("write manifest");
        let env_dir = temp.path().join("crates/spin");
        fs::create_dir_all(&env_dir).expect("mkdir spin");
        // `.env` is a symlink to an out-of-tree file the operator does
        // not control.
        let outside = temp.path().join("attacker.env");
        fs::write(&outside, "INJECTED=1\n").expect("write outside env");
        symlink(&outside, env_dir.join(".env")).expect("symlink .env");

        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let args = ServeArgs {
            adapter: "spin".to_owned(),
        };
        let err = run_serve(&args).expect_err("a symlinked .env must be refused");
        assert!(err.contains("symlink"), "error names the symlink: {err}");
        assert!(
            !observed_path.exists(),
            "the child must NOT have spawned once the env chain was rejected"
        );
    }

    #[cfg(unix)]
    #[test]
    fn run_serve_refuses_symlinked_edgezero_dir_even_without_env_file() {
        // Regression: the symlink guard used to run only when `.env`
        // existed. Axum reads `<root>/.edgezero/local-config-*.json` at
        // request time, so a symlinked `.edgezero` must be refused even
        // with NO `.env` inside it.
        use std::os::unix::fs::symlink;
        let _lock = manifest_guard().lock().expect("manifest guard");
        let temp = TempDir::new().expect("temp dir");
        let observed_path = temp.path().join("child_observed.txt");
        let observed_path_display = observed_path.to_string_lossy();
        let manifest_path = temp.path().join("edgezero.toml");
        let manifest_body = format!(
            r#"
[app]
name = "demo-app"

[adapters.axum.adapter]
crate = "crates/server"
manifest = "crates/server/axum.toml"

[adapters.axum.commands]
build = "echo"
deploy = "echo"
serve = 'sh -c "printf ran > {observed_path_display}"'
"#,
        );
        fs::write(&manifest_path, &manifest_body).expect("write manifest");
        // `.edgezero` is a symlink to an out-of-tree dir holding config
        // the operator does not control. Note: NO `.env` inside.
        let outside = temp.path().join("attacker-edgezero");
        fs::create_dir_all(&outside).expect("mkdir outside");
        fs::write(
            outside.join("local-config-app_config.json"),
            "{\"greeting\":\"pwned\"}\n",
        )
        .expect("write outside config");
        symlink(&outside, temp.path().join(".edgezero")).expect("symlink .edgezero");

        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        let args = ServeArgs {
            adapter: "axum".to_owned(),
        };
        let err = run_serve(&args).expect_err("a symlinked .edgezero must be refused");
        assert!(err.contains("symlink"), "error names the symlink: {err}");
        assert!(
            !observed_path.exists(),
            "the child must NOT have spawned once the .edgezero chain was rejected"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn run_serve_existing_parent_env_wins_over_env_file() {
        // Same contract, opposite direction: when the parent env
        // already has the key, the .env's value must NOT overlay.
        // Serialised access to `EDGEZERO_TEST_SERVE_ENV_KEEP` via
        // the manifest guard so a concurrent test can't observe
        // torn state.
        let marker_key = "EDGEZERO_TEST_SERVE_ENV_KEEP";
        let parent_value = "from-parent";
        let file_value = "from-dot-env-do-not-leak";
        let _lock = manifest_guard().lock().expect("manifest guard");
        let _pre = EnvOverride::set(marker_key, parent_value);

        let temp = TempDir::new().expect("temp dir");
        let observed_path = temp.path().join("child_observed.txt");
        let observed_path_display = observed_path.to_string_lossy();
        let manifest_path = temp.path().join("edgezero.toml");
        let manifest_body = format!(
            r#"
[app]
name = "demo-app"

[adapters.spin.adapter]
crate = "crates/spin"
manifest = "crates/spin/spin.toml"

[adapters.spin.commands]
build = "echo"
deploy = "echo"
serve = 'sh -c "printf %s \"${{{marker_key}:-<unset>}}\" > {observed_path_display}"'
"#,
        );
        fs::write(&manifest_path, &manifest_body).expect("write manifest");
        let env_dir = temp.path().join("crates/spin");
        fs::create_dir_all(&env_dir).expect("mkdir spin");
        fs::write(env_dir.join(".env"), format!("{marker_key}={file_value}\n")).expect("seed .env");

        let manifest_str = manifest_path.to_string_lossy().into_owned();
        let _env = EnvOverride::set("EDGEZERO_MANIFEST", &manifest_str);
        run_serve(&ServeArgs {
            adapter: "spin".to_owned(),
        })
        .expect("run_serve must succeed");

        let child_observed = fs::read_to_string(&observed_path).expect("child wrote observed");
        assert_eq!(
            child_observed, parent_value,
            "parent env MUST win over .env overlay: got {child_observed:?}"
        );
    }
}

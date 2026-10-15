//! Adapter helpers for Cloudflare Workers.

#[cfg(all(feature = "cli", not(target_arch = "wasm32")))]
pub mod cli;

// `config_store` compiles on host for its `InMemory` test backend; the
// production `Kv` backend is feature-gated internally.
#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
pub mod config_store;
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
pub mod context;
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
pub mod key_value_store;
#[cfg(any(
    test,
    feature = "test-utils",
    all(feature = "cloudflare", target_arch = "wasm32")
))]
pub mod outbound;
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
pub mod request;
#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
pub mod response;
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
pub mod secret_store;

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
use edgezero_core::app::StoresMetadata;
#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
use edgezero_core::app::{App, Hooks, StoreMetadata};
#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
use edgezero_core::context::RuntimeVariables;
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
use edgezero_core::env_config::EnvConfig;
#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
use edgezero_core::error::EdgeError;
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
use edgezero_core::manifest::BakedManifest;
#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
use edgezero_core::manifest::Manifest;
#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
use edgezero_core::manifest::ResolvedEnvironmentBinding;
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
use worker::{Context, Env, Error as WorkerError, Request, Response};

/// Cloudflare Workers' published per-isolate memory limit.
pub const CLOUDFLARE_PLATFORM: edgezero_core::PlatformMetadata =
    edgezero_core::PlatformMetadata::new(
        edgezero_core::PlatformFact::known(
            edgezero_core::MemoryCeiling::new(
                128_000_000,
                edgezero_core::MemoryCeilingScope::PerInstance,
                None,
            ),
            edgezero_core::PlatformResourceSource::PlatformLimit {
                provider: "Cloudflare Workers",
            },
        ),
        edgezero_core::PlatformFact::unknown(
            edgezero_core::PlatformUnknownReason::ProviderUnpublished,
        ),
        edgezero_core::PlatformFact::unknown(
            edgezero_core::PlatformUnknownReason::ProviderUnpublished,
        ),
    );

/// Policy for attaching a Worker `Env`-backed secret store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretPolicy {
    /// Attach `CloudflareSecretStore`, which reads Worker secret bindings.
    AllowWorkerSecrets,
    /// Reject applications that declare secret stores.
    RejectDeclared,
}

/// Config KV stream consumption strategy selected for the Worker runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigReadMode {
    /// Use a BYOB reader to fill caller-sized Rust buffers.
    Byob,
    /// Use the stream's default reader, checking each host-allocated chunk
    /// against the caller's byte limits before copying it into Rust memory.
    /// This bounds Rust-owned value storage but not the host chunk allocation.
    DefaultStream,
}

/// Identity and policy for the shared Worker runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerRuntimeOptions {
    /// Canonical adapter id used for manifest environment and logging selection.
    pub adapter_key: &'static str,
    /// Selects how config KV response streams are read.
    pub config_read_mode: ConfigReadMode,
    /// Platform metadata supplied before application configuration hooks run.
    pub platform: edgezero_core::PlatformMetadata,
    /// Controls whether the Worker secret store can be attached.
    pub secret_policy: SecretPolicy,
}

impl WorkerRuntimeOptions {
    /// Cloudflare's existing runtime behavior.
    pub const CLOUDFLARE: Self = Self {
        adapter_key: "cloudflare",
        config_read_mode: ConfigReadMode::Byob,
        platform: CLOUDFLARE_PLATFORM,
        secret_policy: SecretPolicy::AllowWorkerSecrets,
    };
}

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
fn build_app_for_dispatch<A: Hooks>(options: WorkerRuntimeOptions) -> Result<App, WorkerError> {
    build_target_app::<A>(options).map_err(|error| {
        log::error!(target: edgezero_core::BOOT_LOG_TARGET, "application configuration failed: {}", error.kind());
        WorkerError::RustError("application configuration failed".to_owned())
    })
}

#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
fn build_target_app<A: Hooks>(options: WorkerRuntimeOptions) -> Result<App, EdgeError> {
    App::build::<A>(options.platform)
}

#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
fn validate_secret_policy(
    secrets: Option<StoreMetadata>,
    policy: SecretPolicy,
) -> Result<(), &'static str> {
    if secrets.is_some() && policy == SecretPolicy::RejectDeclared {
        return Err("celld does not support declared secret stores");
    }
    Ok(())
}

/// Test seam for the production application-assembly error mapping.
#[cfg(all(feature = "test-utils", feature = "cloudflare", target_arch = "wasm32"))]
#[doc(hidden)]
#[inline]
pub fn build_app_for_test<A: Hooks>() -> Result<App, WorkerError> {
    build_app_for_dispatch::<A>(WorkerRuntimeOptions::CLOUDFLARE)
}

/// # Errors
/// Never; this is a no-op. Workers console logging does not install a Rust
/// `log` backend. Applications needing facade output must install a backend
/// and its filters before [`run_app`] and set `Hooks::owns_logging()` to `true`.
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
#[inline]
pub fn init_logger() -> Result<(), log::SetLoggerError> {
    Ok(())
}

/// # Errors
/// Never; this is a no-op stub on non-wasm targets.
#[cfg(not(all(feature = "cloudflare", target_arch = "wasm32")))]
#[inline]
pub fn init_logger() -> Result<(), log::SetLoggerError> {
    Ok(())
}

/// Build an [`EnvConfig`] from a Cloudflare `Env`. Workers have no
/// `std::env`, and the `Env` binding object cannot be enumerated, so the exact
/// `EDGEZERO__STORES__<KIND>__<ID>__NAME` / `__KEY` keys are derived from the
/// baked store metadata and queried individually, alongside the fixed
/// `EDGEZERO__ADAPTER__*` / `EDGEZERO__LOGGING__*` keys.
///
/// `__KEY` is included for `CONFIG` ids only -- it's how spec 5.4 routes the
/// runtime extractor at a per-environment override blob (e.g. `app_config`
/// vs `app_config_staging`). KV/SECRETS bindings don't have a per-id key
/// override.
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
fn env_config_from_worker(env: &Env, stores: StoresMetadata) -> EnvConfig {
    let mut keys: Vec<String> = vec![
        "EDGEZERO__ADAPTER__HOST".to_owned(),
        "EDGEZERO__ADAPTER__PORT".to_owned(),
        "EDGEZERO__LOGGING__LEVEL".to_owned(),
    ];
    for (kind, store_meta) in [
        ("CONFIG", stores.config),
        ("KV", stores.kv),
        ("SECRETS", stores.secrets),
    ] {
        if let Some(meta) = store_meta {
            for id in meta.ids {
                let id_upper = id.to_ascii_uppercase();
                keys.push(format!("EDGEZERO__STORES__{kind}__{id_upper}__NAME"));
                if kind == "CONFIG" {
                    keys.push(format!("EDGEZERO__STORES__{kind}__{id_upper}__KEY"));
                }
            }
        }
    }
    let vars = keys
        .into_iter()
        .filter_map(|key| env.var(&key).ok().map(|value| (key, value.to_string())));
    EnvConfig::from_vars(vars)
}

#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
fn collect_worker_variables(
    bindings: &[ResolvedEnvironmentBinding],
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> RuntimeVariables {
    RuntimeVariables::from_vars(bindings.iter().filter_map(|binding| {
        lookup(&binding.env)
            .or_else(|| binding.value.clone())
            .map(|value| (binding.name.clone(), value))
    }))
}

#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
fn manifest_runtime_variables(
    manifest: &Manifest,
    adapter_key: &str,
    lookup: impl FnMut(&str) -> Option<String>,
) -> RuntimeVariables {
    let resolved = manifest.environment_for(adapter_key);
    collect_worker_variables(&resolved.variables, lookup)
}

/// Entry point for a Cloudflare Workers application.
///
/// Portable store config is baked into `A` by the `app!` macro; adapter-specific
/// values (platform store names) are read at runtime from `EDGEZERO__*`
/// variables on the worker `Env`. No `edgezero.toml` is required.
///
/// # Errors
/// Returns [`worker::Error`] if the inner dispatch fails or any required
/// store binding cannot be opened.
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
#[inline]
pub async fn run_app<A: Hooks>(
    req: Request,
    env: Env,
    ctx: Context,
) -> Result<Response, WorkerError> {
    run_app_with_options::<A>(req, env, ctx, WorkerRuntimeOptions::CLOUDFLARE).await
}

/// Entry point for an application using the shared Worker transport under another adapter id.
///
/// # Errors
/// Returns [`worker::Error`] if application configuration, secret policy, or dispatch fails.
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
#[inline]
pub async fn run_app_with_options<A: Hooks>(
    req: Request,
    env: Env,
    ctx: Context,
    options: WorkerRuntimeOptions,
) -> Result<Response, WorkerError> {
    validate_secret_policy(A::stores().secrets, options.secret_policy)
        .map_err(|message| WorkerError::RustError(message.to_owned()))?;
    // Worker logging remains the existing no-op initializer. The manifest's
    // adapter-specific Hooks::logging_for contract has no Worker logger backend.
    if !A::owns_logging() {
        drop(init_logger());
    }
    let app = build_app_for_dispatch::<A>(options)?;
    dispatch_app_with_options::<A>(&app, req, env, ctx, options).await
}

/// Dispatch a caller-owned app through canonical admission and response egress.
///
/// The app must be built from `A` with [`CLOUDFLARE_PLATFORM`]. This function does not
/// initialize logging or retain an app. Bindings and runtime variables are resolved
/// for every request; only application-owned, concurrency-safe state may be retained.
///
/// # Errors
/// Returns category-safe configuration, admission-abort, or adapter delivery errors.
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
#[inline]
pub async fn dispatch_app<A: Hooks>(
    app: &App,
    req: Request,
    env: Env,
    ctx: Context,
) -> Result<Response, WorkerError> {
    dispatch_app_with_options::<A>(app, req, env, ctx, WorkerRuntimeOptions::CLOUDFLARE).await
}

/// Dispatch with an explicit adapter identity, platform metadata, secret policy, and config
/// stream reader.
///
/// # Errors
/// Returns a Worker error if bindings cannot be opened, the manifest is invalid, or dispatch
/// fails.
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
#[inline]
pub async fn dispatch_app_with_options<A: Hooks>(
    app: &App,
    req: Request,
    env: Env,
    ctx: Context,
    options: WorkerRuntimeOptions,
) -> Result<Response, WorkerError> {
    let stores = A::stores();
    validate_secret_policy(stores.secrets, options.secret_policy)
        .map_err(|message| WorkerError::RustError(message.to_owned()))?;
    let env_config = env_config_from_worker(&env, stores);
    let runtime_variables = match A::manifest() {
        BakedManifest::Absent => RuntimeVariables::default(),
        BakedManifest::Present(manifest) => {
            manifest_runtime_variables(manifest, options.adapter_key, |key| {
                env.var(key).ok().map(|value| value.to_string())
            })
        }
        BakedManifest::Malformed(_) | _ => {
            return Err(WorkerError::RustError(
                "application manifest is invalid".to_owned(),
            ));
        }
    };
    request::dispatch_with_registries(
        app,
        req,
        env,
        ctx,
        request::RegistryInputs {
            config_meta: stores.config,
            config_read_mode: options.config_read_mode,
            env_config: &env_config,
            kv_meta: stores.kv,
            runtime_variables,
            secret_meta: stores.secrets,
        },
    )
    .await
}

#[cfg(test)]
mod platform_tests {
    use edgezero_core::app::{App, Hooks, StoreMetadata};
    use edgezero_core::error::EdgeError;
    use edgezero_core::manifest::ManifestLoader;
    use edgezero_core::manifest::ResolvedEnvironmentBinding;
    use edgezero_core::router::RouterService;

    struct PlatformAwareConfiguration;

    #[expect(
        clippy::missing_trait_methods,
        reason = "test hook exercises only target metadata propagation"
    )]
    impl Hooks for PlatformAwareConfiguration {
        fn configure(app: &mut App) -> Result<(), EdgeError> {
            if app.platform() == crate::CLOUDFLARE_PLATFORM {
                Ok(())
            } else {
                Err(EdgeError::service_unavailable("wrong application platform"))
            }
        }

        fn routes() -> RouterService {
            RouterService::builder().build()
        }
    }

    #[test]
    fn cloudflare_runtime_keeps_byob_config_read_mode() {
        assert_eq!(
            super::WorkerRuntimeOptions::CLOUDFLARE.config_read_mode,
            super::ConfigReadMode::Byob
        );
    }

    #[test]
    fn application_configuration_receives_cloudflare_platform_metadata() {
        let app = super::build_target_app::<PlatformAwareConfiguration>(
            super::WorkerRuntimeOptions::CLOUDFLARE,
        )
        .expect("configured application");

        assert_eq!(app.platform(), crate::CLOUDFLARE_PLATFORM);
    }

    #[test]
    fn cloudflare_secret_policy_preserves_worker_secret_support() {
        let metadata = StoreMetadata {
            default: "secrets",
            ids: &["secrets"],
        };
        assert_eq!(
            super::validate_secret_policy(Some(metadata), super::SecretPolicy::AllowWorkerSecrets,),
            Ok(())
        );
    }

    #[test]
    fn celld_secret_policy_rejects_declared_secret_stores() {
        let metadata = StoreMetadata {
            default: "secrets",
            ids: &["secrets"],
        };
        assert_eq!(
            super::validate_secret_policy(Some(metadata), super::SecretPolicy::RejectDeclared,),
            Err("celld does not support declared secret stores")
        );
        assert_eq!(
            super::validate_secret_policy(None, super::SecretPolicy::RejectDeclared),
            Ok(())
        );
    }

    #[test]
    fn declared_worker_variables_prefer_binding_then_manifest_default() {
        let bindings = [
            ResolvedEnvironmentBinding {
                description: None,
                env: "UPSTREAM_ORIGIN".to_owned(),
                name: "API_BASE_URL".to_owned(),
                value: Some("https://default.example".to_owned()),
            },
            ResolvedEnvironmentBinding {
                description: None,
                env: "OPTIONAL_BINDING".to_owned(),
                name: "OPTIONAL".to_owned(),
                value: None,
            },
        ];
        let runtime = super::collect_worker_variables(&bindings, |key| {
            (key == "UPSTREAM_ORIGIN").then(|| "https://worker.example".to_owned())
        });
        assert_eq!(runtime.get("API_BASE_URL"), Some("https://worker.example"));
        assert_eq!(runtime.get("OPTIONAL"), None);

        let defaulted = super::collect_worker_variables(&bindings, |_| None);
        assert_eq!(
            defaulted.get("API_BASE_URL"),
            Some("https://default.example")
        );
    }

    #[test]
    fn manifest_environment_selection_uses_the_selected_adapter_key() {
        let loader = ManifestLoader::load_from_str(
            r#"
[[environment.variables]]
name = "API_BASE_URL"
value = "https://cf.example"
adapters = ["cloudflare"]

[[environment.variables]]
name = "API_BASE_URL"
value = "https://celld.example"
adapters = ["celld"]
"#,
        );
        let manifest = loader.manifest();

        let cloudflare = super::manifest_runtime_variables(manifest, "cloudflare", |_| None);
        let celld = super::manifest_runtime_variables(manifest, "celld", |_| None);
        assert_eq!(cloudflare.get("API_BASE_URL"), Some("https://cf.example"));
        assert_eq!(celld.get("API_BASE_URL"), Some("https://celld.example"));
    }
}

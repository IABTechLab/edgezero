//! Experimental local runtime adapter for celld.
//!
//! The HTTP implementation reuses the Worker transport. This crate supplies
//! the celld identity and rejects unsupported secret-store declarations.

#[cfg(all(feature = "cli", not(target_arch = "wasm32")))]
pub mod cli;

use edgezero_core::{PlatformFact, PlatformMetadata, PlatformUnknownReason};

/// celld's published runtime limits do not establish `EdgeZero` platform facts.
pub const CELLD_PLATFORM: PlatformMetadata = PlatformMetadata::new(
    PlatformFact::unknown(PlatformUnknownReason::ProviderUnpublished),
    PlatformFact::unknown(PlatformUnknownReason::ProviderUnpublished),
    PlatformFact::unknown(PlatformUnknownReason::ProviderUnpublished),
);

/// Adapter identity used to resolve celld-specific manifest settings.
pub const CELLD_ADAPTER: &str = "celld";

/// Runtime identity and policy used by [`run_app`].
#[cfg(feature = "celld")]
pub const CELLD_RUNTIME_OPTIONS: edgezero_adapter_cloudflare::WorkerRuntimeOptions =
    edgezero_adapter_cloudflare::WorkerRuntimeOptions {
        adapter_key: CELLD_ADAPTER,
        config_read_mode: edgezero_adapter_cloudflare::ConfigReadMode::DefaultStream,
        platform: CELLD_PLATFORM,
        secret_policy: edgezero_adapter_cloudflare::SecretPolicy::RejectDeclared,
    };

#[cfg(all(feature = "celld", target_arch = "wasm32"))]
/// Worker request context alias for consumers that need the Worker-compatible context.
pub type CelldRequestContext = edgezero_adapter_cloudflare::context::CloudflareRequestContext;

/// Handle one Worker-compatible request under celld's EdgeZero identity.
///
/// The generated application must declare its celld-specific environment and
/// stores in `edgezero.toml`. Typed config and ordinary KV use celld's local
/// Worker KV binding. Declared secret stores are rejected.
///
/// # Errors
/// Returns a Worker error when application configuration, secret policy, or
/// request dispatch fails.
#[cfg(all(feature = "celld", target_arch = "wasm32"))]
#[inline]
pub async fn run_app<A: edgezero_core::app::Hooks>(
    req: worker::Request,
    env: worker::Env,
    ctx: worker::Context,
) -> worker::Result<worker::Response> {
    edgezero_adapter_cloudflare::run_app_with_options::<A>(req, env, ctx, CELLD_RUNTIME_OPTIONS)
        .await
}

#[cfg(test)]
mod tests {
    use std::io::Error;

    use edgezero_core::app::{App, Hooks};
    use edgezero_core::error::EdgeError;
    use edgezero_core::router::RouterService;

    struct PlatformHooks;

    #[expect(
        clippy::missing_trait_methods,
        reason = "the test checks only platform metadata before configure"
    )]
    impl Hooks for PlatformHooks {
        fn configure(app: &mut App) -> Result<(), EdgeError> {
            if app.platform() == super::CELLD_PLATFORM {
                Ok(())
            } else {
                Err(EdgeError::internal(Error::other("celld metadata missing")))
            }
        }

        fn routes() -> RouterService {
            RouterService::builder().build()
        }
    }

    #[test]
    fn celld_metadata_is_supplied_before_application_configuration() {
        let app = App::build::<PlatformHooks>(super::CELLD_PLATFORM).expect("celld app");
        assert_eq!(app.platform(), super::CELLD_PLATFORM);
    }

    #[test]
    fn celld_metadata_does_not_claim_provider_facts() {
        assert!(super::CELLD_PLATFORM.memory_ceiling().value().is_none());
        assert!(
            super::CELLD_PLATFORM
                .inbound_request_population_bound()
                .value()
                .is_none()
        );
        assert!(
            super::CELLD_PLATFORM
                .host_ingress_memory_accounting()
                .value()
                .is_none()
        );
    }

    #[cfg(feature = "celld")]
    #[test]
    fn celld_runtime_options_never_enable_worker_plaintext_secrets() {
        assert_eq!(
            super::CELLD_RUNTIME_OPTIONS.secret_policy,
            edgezero_adapter_cloudflare::SecretPolicy::RejectDeclared
        );
        assert_eq!(super::CELLD_RUNTIME_OPTIONS.adapter_key, "celld");
        assert_eq!(super::CELLD_RUNTIME_OPTIONS.platform, super::CELLD_PLATFORM);
        assert_eq!(
            super::CELLD_RUNTIME_OPTIONS.config_read_mode,
            edgezero_adapter_cloudflare::ConfigReadMode::DefaultStream
        );
    }
}

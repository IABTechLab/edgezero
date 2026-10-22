//! Utilities for bridging Fastly Compute@Edge requests into the
//! `edgezero-core` service abstractions.

#![cfg_attr(
    feature = "fastly",
    expect(
        clippy::pub_use,
        reason = "reuse the platform serving limits and summary"
    )
)]

#[cfg(feature = "fastly")]
use anyhow::Context as _;

// Compile storage helpers only for their runtime, CLI or colocated tests.
#[cfg(any(
    feature = "fastly",
    test,
    all(feature = "cli", not(target_arch = "wasm32"))
))]
pub(crate) mod chunked_config;
#[cfg(all(feature = "cli", not(target_arch = "wasm32")))]
pub mod cli;
#[cfg(feature = "fastly")]
pub mod config_store;
pub mod context;
pub mod delivery;
#[cfg(feature = "fastly")]
pub mod key_value_store;
pub mod lifecycle;
#[cfg(any(feature = "fastly", all(test, not(target_arch = "wasm32"))))]
pub mod logger;
#[cfg(any(test, feature = "test-utils", feature = "fastly"))]
pub mod outbound;
#[cfg(feature = "fastly")]
pub mod request;
#[cfg(any(test, feature = "fastly"))]
pub mod response;
#[cfg(feature = "fastly")]
pub mod secret_store;

#[cfg(any(feature = "fastly", test))]
use edgezero_core::app::{App, Hooks};
#[cfg(any(feature = "fastly", test))]
use edgezero_core::error::EdgeError;
#[cfg(feature = "fastly")]
use edgezero_core::http::{Extensions, Response};
#[cfg(any(feature = "fastly", test))]
use edgezero_core::manifest::ResolvedLoggingConfig;
#[cfg(feature = "fastly")]
pub use fastly::http::serve::{Serve, ServeSummary};
use std::num::NonZeroU32;
#[cfg(feature = "fastly")]
use std::sync::Once;

/// Fastly Compute's published per-execution heap and stack limits.
pub const FASTLY_PLATFORM: edgezero_core::PlatformMetadata = edgezero_core::PlatformMetadata::new(
    edgezero_core::PlatformFact::known(
        edgezero_core::MemoryCeiling::new(
            128_000_000,
            edgezero_core::MemoryCeilingScope::PerExecution,
            Some(1_000_000),
        ),
        edgezero_core::PlatformResourceSource::PlatformLimit {
            provider: "Fastly Compute",
        },
    ),
    edgezero_core::PlatformFact::known(
        edgezero_core::InboundRequestPopulationBound::new(NonZeroU32::MIN),
        edgezero_core::PlatformResourceSource::PlatformLimit {
            provider: "Fastly Compute",
        },
    ),
    edgezero_core::PlatformFact::unknown(edgezero_core::PlatformUnknownReason::ProviderUnpublished),
);

#[cfg(feature = "fastly")]
static FASTLY_ABI_INIT: Once = Once::new();

#[cfg(any(feature = "fastly", test))]
#[derive(Debug, Clone)]
pub struct FastlyLogging {
    pub echo_stdout: bool,
    pub endpoint: Option<String>,
    pub level: log::LevelFilter,
    pub use_fastly_logger: bool,
}

#[cfg(any(feature = "fastly", test))]
impl From<ResolvedLoggingConfig> for FastlyLogging {
    #[inline]
    fn from(config: ResolvedLoggingConfig) -> Self {
        let use_fastly_logger = config.endpoint.is_some();
        Self {
            echo_stdout: config.echo_stdout.unwrap_or(true),
            endpoint: config.endpoint,
            level: config.level.into(),
            use_fastly_logger,
        }
    }
}

#[cfg(feature = "fastly")]
fn build_app_for_dispatch<A>() -> Result<App, fastly::Error>
where
    A: Hooks,
{
    build_target_app::<A>().context("application configuration failed")
}

#[cfg(any(feature = "fastly", test))]
fn build_target_app<A>() -> Result<App, EdgeError>
where
    A: Hooks,
{
    App::build::<A>(FASTLY_PLATFORM)
}

/// Test seam for the production application-assembly error mapping.
#[cfg(all(feature = "test-utils", feature = "fastly"))]
#[doc(hidden)]
#[inline]
pub fn build_app_for_test<A>() -> Result<App, fastly::Error>
where
    A: Hooks,
{
    build_app_for_dispatch::<A>()
}

#[cfg(feature = "fastly")]
fn init_fastly_abi() {
    FASTLY_ABI_INIT.call_once(fastly::init);
}

/// # Errors
/// Returns [`logger::InitLoggerError::Build`] if the underlying logger
/// builder rejects its inputs (e.g. an empty endpoint), or
/// [`logger::InitLoggerError::SetLogger`] if a global logger is already
/// installed.
#[cfg(feature = "fastly")]
#[inline]
pub fn init_logger(
    endpoint: &str,
    level: log::LevelFilter,
    echo_stdout: bool,
) -> Result<(), logger::InitLoggerError> {
    logger::init_logger(endpoint, level, echo_stdout)
}

/// # Errors
/// Never; this is a no-op stub on builds without the `fastly` feature.
#[cfg(not(feature = "fastly"))]
#[inline]
pub fn init_logger(
    _endpoint: &str,
    _level: log::LevelFilter,
    _echo_stdout: bool,
) -> Result<(), log::SetLoggerError> {
    Ok(())
}

/// Entry point for a Fastly Compute application.
///
/// Portable store declarations and Fastly logging settings are baked into `A`
/// by the `app!` macro. Deployment binds physical stores to the baked logical
/// IDs through Fastly resource links.
///
/// # Errors
/// Logger setup failures and unavailable required stores return errors.
#[cfg(feature = "fastly")]
#[inline]
pub fn run_app<A>() -> Result<(), fastly::Error>
where
    A: Hooks,
{
    run_app_with_hooks::<A, _, _, ()>(|_req, _extensions| {}, |_response| ()).map(|_state| ())
}

/// Runs the manifest-wired app through Fastly's closed request/response lifecycle.
///
/// `prepare` borrows the raw request and extension bag before conversion. `finalize` borrows only
/// routed responses before response-egress policy and framing. `EdgeZero` retains transmission
/// ownership; routed hook state is returned only after terminal delivery, while detached ingress
/// responses skip `finalize` and return `None`.
///
/// # Errors
/// Logger setup failures and unavailable required stores return errors.
#[cfg(feature = "fastly")]
#[inline]
pub fn run_app_with_hooks<A, Prepare, Finalize, State>(
    prepare: Prepare,
    finalize: Finalize,
) -> Result<Option<State>, fastly::Error>
where
    A: Hooks,
    Prepare: FnOnce(&mut fastly::Request, &mut Extensions),
    Finalize: FnOnce(&mut Response) -> State,
{
    init_fastly_abi();
    let stores = A::stores();
    let logging = FastlyLogging::from(A::logging_for("fastly"));
    if logging.use_fastly_logger && !A::owns_logging() {
        let endpoint = logging.endpoint.as_deref().unwrap_or("stdout");
        init_logger(endpoint, logging.level, logging.echo_stdout)?;
    }
    let app = build_app_for_dispatch::<A>()?;
    request::send_with_registries_and_hooks(&app, stores, prepare, finalize).into_standard_result()
}

/// Retain a successfully built app while the SDK serves sequential requests.
///
/// Each callback resolves fresh logical store registries. Logging uses baked
/// settings, and the adapter owns response delivery without SDK resends.
#[cfg(feature = "fastly")]
#[must_use = "inspect the summary or handle its terminal error with into_result()"]
#[inline]
pub fn serve_app<A>(serve: Serve) -> ServeSummary<fastly::Error>
where
    A: Hooks,
{
    serve_app_with_hooks::<A, _, _, _>(serve, |_request, _extensions| {}, |_response| ())
}

/// Retained serving with request-local preparation and routed response finalization.
///
/// The hooks follow [`run_app_with_hooks`]. Preparation runs before admission;
/// finalization runs only for routed responses, before adapter-owned delivery.
/// Captures persist across callbacks and must not accidentally retain request resources.
#[cfg(feature = "fastly")]
#[must_use = "inspect the summary or handle its terminal error with into_result()"]
#[inline]
pub fn serve_app_with_hooks<A, Prepare, Finalize, State>(
    serve: Serve,
    mut prepare: Prepare,
    mut finalize: Finalize,
) -> ServeSummary<fastly::Error>
where
    A: Hooks,
    Prepare: FnMut(&mut fastly::Request, &mut Extensions),
    Finalize: FnMut(&mut Response) -> State,
{
    init_fastly_abi();
    let stores = A::stores();
    let logging = FastlyLogging::from(A::logging_for("fastly"));
    lifecycle::serve_custom(serve, move |req, retained: &mut lifecycle::Sandbox<App>| {
        retained.setup_once(|| {
            if logging.use_fastly_logger && !A::owns_logging() {
                init_logger(
                    logging.endpoint.as_deref().unwrap_or("stdout"),
                    logging.level,
                    logging.echo_stdout,
                )?;
            }
            Ok::<(), fastly::Error>(())
        })?;
        retained.initialize(build_app_for_dispatch::<A>)?;
        let app = retained.state().ok_or_else(|| {
            fastly::Error::msg("application initialization did not retain an app")
        })?;
        request::send_request_with_registries_and_hooks(
            app,
            stores,
            req,
            &mut prepare,
            &mut finalize,
        )
        .into_standard_result()
        .map(|_state| ())
    })
}

/// Dispatch with an explicitly wired config store. Its name comes from the caller,
/// and its default key is `"default"`, not a manifest logical ID. KV is not
/// automatically injected; use [`request::FastlyService`] for explicit bindings.
///
/// # Errors
/// Returns an error if logger setup fails or the underlying handler returns an error.
#[cfg(feature = "fastly")]
#[inline]
pub fn run_app_with_config<A>(
    logging: &FastlyLogging,
    config_store_name: Option<&str>,
) -> Result<(), fastly::Error>
where
    A: Hooks,
{
    init_fastly_abi();
    if logging.use_fastly_logger && !A::owns_logging() {
        let endpoint = logging.endpoint.as_deref().unwrap_or("stdout");
        init_logger(endpoint, logging.level, logging.echo_stdout)?;
    }
    let app = build_app_for_dispatch::<A>()?;
    let mut service = request::FastlyService::new(&app);
    if let Some(name) = config_store_name {
        service = service.with_config(name);
    }
    service.send()
}

#[cfg(test)]
mod fastly_logging_tests {
    use super::*;
    use edgezero_core::manifest::LogLevel;

    #[test]
    fn fastly_logging_from_manifest_converts_defaults() {
        let config = ResolvedLoggingConfig {
            echo_stdout: Some(false),
            endpoint: Some("endpoint".to_owned()),
            level: LogLevel::Debug,
        };

        let logging: FastlyLogging = config.into();
        assert_eq!(logging.endpoint.as_deref(), Some("endpoint"));
        assert_eq!(logging.level, log::LevelFilter::Debug);
        assert!(!logging.echo_stdout);
        assert!(logging.use_fastly_logger);
    }

    #[test]
    fn fastly_logging_without_manifest_endpoint_does_not_install_named_logger() {
        let logging = FastlyLogging::from(ResolvedLoggingConfig::default());

        assert_eq!(logging.level, log::LevelFilter::Info);
        assert_eq!(logging.endpoint, None);
        assert!(!logging.use_fastly_logger);
        assert!(logging.echo_stdout);
    }
}

#[cfg(test)]
mod platform_tests {
    use edgezero_core::app::{App, Hooks};
    use edgezero_core::error::EdgeError;
    use edgezero_core::router::RouterService;

    struct PlatformAwareConfiguration;

    #[expect(
        clippy::missing_trait_methods,
        reason = "test hook exercises only target metadata propagation"
    )]
    impl Hooks for PlatformAwareConfiguration {
        fn configure(app: &mut App) -> Result<(), EdgeError> {
            if app.platform() == crate::FASTLY_PLATFORM {
                Ok(())
            } else {
                Err(EdgeError::service_unavailable("wrong application platform"))
            }
        }

        fn routes() -> Result<RouterService, EdgeError> {
            Ok(RouterService::builder().build())
        }
    }

    #[test]
    fn application_configuration_receives_fastly_platform_metadata() {
        let app = super::build_target_app::<PlatformAwareConfiguration>()
            .expect("configured application");

        assert_eq!(app.platform(), crate::FASTLY_PLATFORM);
    }

    #[test]
    fn application_route_failure_preserves_target_error() {
        struct RouteFailure;
        #[expect(
            clippy::missing_trait_methods,
            reason = "tests typed route failure at target assembly"
        )]
        impl Hooks for RouteFailure {
            fn configure(_app: &mut App) -> Result<(), EdgeError> {
                Err(EdgeError::bad_request("configuration must not run"))
            }
            fn routes() -> Result<RouterService, EdgeError> {
                Err(EdgeError::service_unavailable("route state unavailable"))
            }
        }
        let Err(error) = super::build_target_app::<RouteFailure>() else {
            panic!("route failure must prevent target application assembly");
        };
        assert_eq!(error.status().as_u16(), 503);
        assert_eq!(error.message(), "route state unavailable");
    }
}

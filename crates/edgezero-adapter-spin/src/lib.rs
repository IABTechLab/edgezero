//! Adapter helpers for Spin (Fermyon).

#[cfg(all(feature = "cli", not(target_arch = "wasm32")))]
pub mod cli;

#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
pub mod config_store;
pub mod context;
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
mod ingress;
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
pub mod key_value_store;
// `kv_pagination` is the pure paging logic for `SpinKvStore::list_keys_page`.
// It is host-compilable so its tests run under `cargo test`, while the wasm32
// `SpinKvStore` is the production consumer.
mod kv_pagination;
#[cfg(any(
    test,
    feature = "test-utils",
    all(feature = "spin", target_arch = "wasm32")
))]
pub mod outbound;
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
pub mod request;
#[cfg(all(feature = "spin", any(test, target_arch = "wasm32")))]
pub mod response;
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
pub mod secret_store;
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
use core::future::Future;
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
use core::pin::Pin;

#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
use edgezero_core::app::{App, Hooks};
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
use edgezero_core::env_config::EnvConfig;
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
use edgezero_core::error::EdgeError;
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
use edgezero_core::outbound::OutboundScopeOwner;
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
use edgezero_core::time::MonotonicClock;
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
use futures_util::future::LocalBoxFuture;
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
use spin_sdk::http::Request as SpinRequest;
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
use spin_sdk::wasip3::http::types::Response as WasiResponse;
use std::num::NonZeroU32;

/// Akamai Functions' published default quota for a Spin function execution.
pub const AKAMAI_FUNCTIONS_PLATFORM: edgezero_core::PlatformMetadata =
    edgezero_core::PlatformMetadata::new(
        edgezero_core::PlatformFact::known(
            edgezero_core::MemoryCeiling::new(
                128 * 1024 * 1024,
                edgezero_core::MemoryCeilingScope::PerExecution,
                None,
            ),
            edgezero_core::PlatformResourceSource::HostedDefault {
                provider: "Akamai Functions",
            },
        ),
        edgezero_core::PlatformFact::known(
            edgezero_core::InboundRequestPopulationBound::new(NonZeroU32::MIN),
            edgezero_core::PlatformResourceSource::HostedDefault {
                provider: "Akamai Functions",
            },
        ),
        edgezero_core::PlatformFact::unknown(
            edgezero_core::PlatformUnknownReason::ProviderUnpublished,
        ),
    );

/// Generic Spin resource limits and accounting are runtime-configured.
pub const SPIN_PLATFORM: edgezero_core::PlatformMetadata = edgezero_core::PlatformMetadata::new(
    edgezero_core::PlatformFact::unknown(edgezero_core::PlatformUnknownReason::RuntimeConfigured),
    edgezero_core::PlatformFact::unknown(edgezero_core::PlatformUnknownReason::RuntimeConfigured),
    edgezero_core::PlatformFact::unknown(edgezero_core::PlatformUnknownReason::RuntimeConfigured),
);

/// Raw `WASIp3` response whose body and transmission lifetime remain owned by `EdgeZero`.
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
pub type SpinResponse = WasiResponse;

#[cfg(all(feature = "spin", target_arch = "wasm32"))]
pub trait AppExt {
    /// Dispatch a Spin request and return the raw response backed by `EdgeZero`'s owned writer.
    fn dispatch<'app>(
        &'app self,
        req: SpinRequest,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<SpinResponse>> + 'app>>;
}

#[cfg(all(feature = "spin", target_arch = "wasm32"))]
impl AppExt for App {
    #[inline]
    fn dispatch<'app>(
        &'app self,
        req: SpinRequest,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<SpinResponse>> + 'app>> {
        Box::pin(request::dispatch(self, req))
    }
}

#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
fn build_target_app<A>(platform: edgezero_core::PlatformMetadata) -> Result<App, EdgeError>
where
    A: Hooks,
{
    App::build::<A>(platform)
}

#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
fn in_outbound_callback<'future, F>(future: F) -> LocalBoxFuture<'future, F::Output>
where
    F: Future + 'future,
    F::Output: 'future,
{
    // Bound composed dispatch frames without retaining ambient activation between polls.
    Box::pin(async move {
        match OutboundScopeOwner::callback_if_absent() {
            Some(owner) => owner.run(future).await,
            None => future.await,
        }
    })
}

/// Initialize the logger for Spin.
///
/// This is a no-op. Host stdout/stderr capture does not install a Rust `log`
/// backend. Applications needing facade output must install their backend and
/// filters before the adapter entrypoint and set `Hooks::owns_logging()` to `true`.
///
/// # Errors
/// Never; no logger is installed or reconfigured.
#[inline]
pub fn init_logger() -> Result<(), log::SetLoggerError> {
    Ok(())
}

/// Convenience entry point: build the app from `Hooks`, dispatch the
/// incoming Spin request through the `EdgeZero` router, and return the
/// response.
///
/// Portable store config is baked into `A` by the `app!` macro; the KV store
/// label is resolved at runtime from `EDGEZERO__STORES__KV__<ID>__NAME`. No
/// `edgezero.toml` is required.
///
/// Usage in a Spin component:
///
/// ```ignore
/// use spin_sdk::http_service;
/// use my_core::App;
///
/// #[http_service]
/// async fn handle(
///     req: spin_sdk::http::Request,
/// ) -> anyhow::Result<edgezero_adapter_spin::SpinResponse> {
///     edgezero_adapter_spin::run_app::<App>(req).await
/// }
/// ```
///
/// Returns the concrete raw [`SpinResponse`]. Its body writer and transmission-result observer
/// remain owned by the adapter's spawned response-egress coordinator.
///
/// # Errors
/// Returns [`anyhow::Error`] for request conversion, admission abort or failed delivery.
/// Assembly failures use their typed HTTP status and the default error response.
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
#[inline]
pub async fn run_app<A>(req: SpinRequest) -> anyhow::Result<SpinResponse>
where
    A: Hooks,
{
    run_app_with_platform::<A>(req, SPIN_PLATFORM).await
}

/// Build and dispatch a Spin application with explicit runtime platform metadata.
///
/// Use [`AKAMAI_FUNCTIONS_PLATFORM`] for Akamai Functions. Self-hosted Spin
/// deployments can construct metadata from their configured `max_instance_memory`.
///
/// # Errors
/// Returns [`anyhow::Error`] for request conversion, admission abort or failed delivery.
/// Assembly failures use their typed HTTP status and the default error response.
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
#[inline]
pub async fn run_app_with_platform<A>(
    req: SpinRequest,
    platform: edgezero_core::PlatformMetadata,
) -> anyhow::Result<SpinResponse>
where
    A: Hooks,
{
    in_outbound_callback(async move {
        if !A::owns_logging() {
            drop(init_logger());
        }
        let clock = MonotonicClock::default();
        let request_start = clock.now();
        let app = match build_target_app::<A>(platform) {
            Ok(app) => app,
            Err(error) => {
                return Ok(request::send_assembly_error(
                    req,
                    error,
                    request_start,
                    clock,
                )?);
            }
        };
        dispatch_app::<A>(&app, req).await
    })
    .await
}

/// Dispatch a caller-owned app through canonical admission and response egress.
///
/// This does not build or cache an app or install logging. The caller must pass
/// an app built from `A` with its selected platform; `App` erases its construction type, so this pairing
/// remains a caller precondition. Retain only application-owned values and keep
/// native handles and pending work request-local. Shared state must support
/// overlapping invocations.
///
/// # Errors
/// Returns configuration, admission-abort, or adapter delivery errors.
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
#[inline]
pub async fn dispatch_app<A>(app: &App, req: SpinRequest) -> anyhow::Result<SpinResponse>
where
    A: Hooks,
{
    in_outbound_callback(async move {
        let stores = A::stores();
        let env = EnvConfig::from_env();
        request::dispatch_with_registries(app, req, stores.config, stores.kv, stores.secrets, &env)
            .await
    })
    .await
}

#[cfg(test)]
mod callback_scope_tests {
    use std::rc::Rc;
    use std::task::Poll;

    use edgezero_core::outbound::{
        IndependentOutboundProgress, OutboundScope, OutboundScopeKind, OutboundScopeOwner,
    };
    use futures::executor::block_on;
    use futures_util::future::{FutureExt as _, poll_fn};

    fn capture() -> OutboundScope {
        OutboundScope::capture(Rc::new(IndependentOutboundProgress))
    }

    #[test]
    fn callback_return_closes_captured_scope_without_leaking_ambient_state() {
        assert!(!capture().is_managed());
        let captured = block_on(super::in_outbound_callback(async {
            let captured = capture();
            assert!(captured.is_managed());
            assert_eq!(captured.kind(), Some(OutboundScopeKind::Callback));
            captured.check().expect("active callback");
            captured
        }));
        assert!(captured.check().is_err());
        assert!(!capture().is_managed());
    }

    #[test]
    fn nested_dispatch_reuses_callback_without_closing_outer_work() {
        let outer = OutboundScopeOwner::callback();
        let outer_state = outer.scope().provider_state(|| 7_u8).expect("outer state");
        let captured = block_on(outer.run(super::in_outbound_callback(async {
            super::in_outbound_callback(async { capture() }).await
        })));
        let state = captured
            .provider_state(|| 9_u8)
            .expect("same callback state");
        assert!(Rc::ptr_eq(&state, &outer_state));
        captured.check().expect("outer callback remains live");
        drop(outer);
        assert!(captured.check().is_err());
    }

    #[test]
    fn nested_dispatch_reuses_response_body_owner_without_reparenting() {
        let body = OutboundScopeOwner::response_body();
        let captured = block_on(body.run(super::in_outbound_callback(async { capture() })));
        assert_eq!(captured.kind(), Some(OutboundScopeKind::ResponseBody));
        captured.check().expect("body remains live");
        drop(body);
        assert!(captured.check().is_err());
    }

    #[test]
    fn pending_callback_restores_ambient_and_drop_closes_retained_work() {
        let mut captured = None;
        let callback = super::in_outbound_callback(poll_fn(|_cx| {
            captured = Some(capture());
            Poll::<()>::Pending
        }));
        let mut pending = Box::pin(callback);
        assert!(pending.as_mut().now_or_never().is_none());
        assert!(!capture().is_managed());
        drop(pending);
        assert!(captured.expect("polled callback").check().is_err());
    }

    #[test]
    fn callback_error_closes_scope_and_accepts_borrowed_future() {
        let mut captured = None;
        let failure = "caller-owned error";
        let result = block_on(super::in_outbound_callback(async {
            captured = Some(capture());
            Err::<(), _>(&failure)
        }));
        assert_eq!(result, Err(&failure));
        assert!(captured.expect("callback error").check().is_err());
        assert!(!capture().is_managed());
    }
}

#[cfg(test)]
mod platform_tests {
    use edgezero_core::app::{App, Hooks};
    use edgezero_core::error::EdgeError;
    use edgezero_core::router::RouterService;

    macro_rules! platform_hooks {
        ($hooks:ty, $platform:expr) => {
            #[expect(
                clippy::missing_trait_methods,
                reason = "test hook exercises only target metadata propagation"
            )]
            impl Hooks for $hooks {
                fn configure(app: &mut App) -> Result<(), EdgeError> {
                    if app.platform() == $platform {
                        Ok(())
                    } else {
                        Err(EdgeError::service_unavailable("wrong application platform"))
                    }
                }

                fn routes() -> Result<RouterService, EdgeError> {
                    Ok(RouterService::builder().build())
                }
            }
        };
    }

    struct GenericPlatformConfiguration;
    struct HostedPlatformConfiguration;

    platform_hooks!(GenericPlatformConfiguration, crate::SPIN_PLATFORM);
    platform_hooks!(
        HostedPlatformConfiguration,
        crate::AKAMAI_FUNCTIONS_PLATFORM
    );

    #[test]
    fn application_configuration_receives_generic_spin_platform_metadata() {
        let app = super::build_target_app::<GenericPlatformConfiguration>(crate::SPIN_PLATFORM)
            .expect("configured application");

        assert_eq!(app.platform(), crate::SPIN_PLATFORM);
    }

    #[test]
    fn application_configuration_receives_explicit_hosted_platform_metadata() {
        let app = super::build_target_app::<HostedPlatformConfiguration>(
            crate::AKAMAI_FUNCTIONS_PLATFORM,
        )
        .expect("configured application");

        assert_eq!(app.platform(), crate::AKAMAI_FUNCTIONS_PLATFORM);
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
        for platform in [crate::SPIN_PLATFORM, crate::AKAMAI_FUNCTIONS_PLATFORM] {
            let Err(error) = super::build_target_app::<RouteFailure>(platform) else {
                panic!("route failure must prevent target application assembly");
            };
            assert_eq!(error.status().as_u16(), 503);
            assert_eq!(error.message(), "route state unavailable");
        }
    }
}

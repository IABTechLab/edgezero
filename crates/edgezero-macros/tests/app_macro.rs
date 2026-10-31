//! Integration coverage: `app!(..., owns_logging = true)` emits a `Hooks` impl
//! whose `owns_logging()` returns `true`. The manifest path resolves against
//! this crate's `CARGO_MANIFEST_DIR`, so the fixture is `tests/fixtures/...`.

// The macro emits `pub struct OwnedLoggingApp;`, a `Hooks` impl, and a free
// `build_router()` at this module scope.
edgezero_core::app!(
    "tests/fixtures/owns_logging.toml",
    OwnedLoggingApp,
    owns_logging = true
);

#[cfg(test)]
mod tests {
    use edgezero_core::EdgeError;
    use edgezero_core::app::Hooks as _;
    use edgezero_core::manifest::LogLevel;
    use edgezero_core::router::RouterService;

    #[test]
    fn app_macro_emits_owns_logging_true() {
        assert!(super::OwnedLoggingApp::owns_logging());
    }

    #[test]
    fn app_macro_routes_and_generated_builder_are_fallible() {
        let routes: Result<RouterService, EdgeError> = super::OwnedLoggingApp::routes();
        let built: Result<RouterService, EdgeError> = super::build_router();
        assert_eq!(routes.unwrap().routes(), built.unwrap().routes());
    }

    #[test]
    fn app_macro_bakes_adapter_logging_from_the_manifest() {
        let logging = super::OwnedLoggingApp::logging_for("FASTLY");
        assert_eq!(logging.endpoint.as_deref(), Some("fixture_logs"));
        assert_eq!(logging.level, LogLevel::Debug);
        assert_eq!(logging.echo_stdout, Some(false));

        let missing = super::OwnedLoggingApp::logging_for("custom");
        assert!(missing.endpoint.is_none());
        assert_eq!(missing.level, LogLevel::Info);
        assert!(missing.echo_stdout.is_none());
    }
}

#[cfg(test)]
mod configured_app {
    use edgezero_core::app::App;
    use edgezero_core::body::Body;
    use edgezero_core::config_store::ConfigExtractionLimits;
    use edgezero_core::http::{HeaderMap, Method, Response, StatusCode, Uri, Version};
    use edgezero_core::ingress::{AdmissionDecision, IngressBeginOutcome, IngressHeadParts};
    use edgezero_core::response_egress::ResponseEgressCompletion;
    use edgezero_core::response_egress_framing::ResponseEgressFramingMode;
    use edgezero_core::time::MonotonicInstant;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static STATE_BUILDS: AtomicUsize = AtomicUsize::new(0);

    fn app_state() -> Arc<AtomicUsize> {
        STATE_BUILDS.fetch_add(1, Ordering::SeqCst);
        Arc::new(AtomicUsize::new(0))
    }

    fn configure_app(app: &mut App) -> Result<(), edgezero_core::EdgeError> {
        app.state::<Arc<AtomicUsize>>()
            .ok_or_else(|| edgezero_core::EdgeError::service_unavailable("missing app state"))?
            .fetch_add(1, Ordering::SeqCst);
        app.set_config_extraction_limits(ConfigExtractionLimits::default())?;
        app.set_ingress_admission_policy(|_| {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
            AdmissionDecision::Refuse {
                completion: ResponseEgressCompletion::empty(),
                response,
            }
        });
        Ok(())
    }

    edgezero_core::app!(
        "tests/fixtures/owns_logging.toml",
        ConfiguredApp,
        configure = configure_app,
        state = app_state()
    );

    #[test]
    fn app_macro_configure_callback_installs_ingress_policy() {
        let app = App::build::<ConfiguredApp>(edgezero_core::PlatformMetadata::default())
            .expect("configured app");
        assert_eq!(STATE_BUILDS.load(Ordering::SeqCst), 1);
        assert_eq!(
            app.state::<Arc<AtomicUsize>>()
                .unwrap()
                .load(Ordering::SeqCst),
            1
        );
        let head = IngressHeadParts::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            HeaderMap::new(),
        );
        let outcome = app
            .begin_ingress(head, MonotonicInstant::now())
            .expect("begin ingress");
        let IngressBeginOutcome::Refused(response) = outcome else {
            panic!("configured policy must refuse ingress");
        };
        let Ok((prepared, _, mut attempt, clock)) =
            response.begin(ResponseEgressFramingMode::ExplicitBufferedLength)
        else {
            panic!("begin response egress");
        };
        assert!(attempt.begin_writing());
        assert!(attempt.complete(clock.now()));
        assert_eq!(
            prepared.into_response().status(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }
}

#[cfg(test)]
mod fallible_state_app {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use edgezero_core::app::App;
    use edgezero_core::http::StatusCode;
    use edgezero_core::{EdgeError, PlatformMetadata};

    static CONFIGURATIONS: AtomicUsize = AtomicUsize::new(0);
    static STATE_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

    fn app_state() -> Result<Arc<AtomicUsize>, EdgeError> {
        if STATE_ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(EdgeError::service_unavailable("state unavailable"));
        }
        Ok(Arc::new(AtomicUsize::new(0)))
    }

    fn configure_app(app: &mut App) -> Result<(), EdgeError> {
        let state = app
            .state::<Arc<AtomicUsize>>()
            .ok_or_else(|| EdgeError::service_unavailable("missing state"))?;
        state.fetch_add(1, Ordering::SeqCst);
        CONFIGURATIONS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    edgezero_core::app!(
        "tests/fixtures/owns_logging.toml",
        FallibleStateApp,
        configure = configure_app,
        state = app_state()?
    );

    #[test]
    fn fallible_macro_state_skips_configuration_and_retries_without_caching_failure() {
        let Err(error) = App::build::<FallibleStateApp>(PlatformMetadata::default()) else {
            panic!("failed state must prevent app assembly");
        };
        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.message(), "state unavailable");
        assert_eq!(CONFIGURATIONS.load(Ordering::SeqCst), 0);
        let app = App::build::<FallibleStateApp>(PlatformMetadata::default()).unwrap();
        assert_eq!(
            app.state::<Arc<AtomicUsize>>()
                .unwrap()
                .load(Ordering::SeqCst),
            1
        );
        assert_eq!(STATE_ATTEMPTS.load(Ordering::SeqCst), 2);
        assert_eq!(CONFIGURATIONS.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod failing_configured_app {
    use edgezero_core::app::App;
    use edgezero_core::error::EdgeError;
    use edgezero_core::http::StatusCode;

    fn configure_app(_app: &mut App) -> Result<(), EdgeError> {
        Err(EdgeError::service_unavailable("configuration unavailable"))
    }

    edgezero_core::app!(
        "tests/fixtures/owns_logging.toml",
        FailingConfiguredApp,
        configure = configure_app
    );

    #[test]
    fn app_macro_propagates_configuration_failure() {
        let Err(error) =
            App::build::<FailingConfiguredApp>(edgezero_core::PlatformMetadata::default())
        else {
            panic!("configuration failure must stop application assembly");
        };
        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}

#[cfg(test)]
mod platform_configured_app {
    use std::num::NonZeroU32;

    use edgezero_core::app::App;
    use edgezero_core::{
        EdgeError, HostIngressMemoryAccounting, InboundRequestPopulationBound, MemoryCeiling,
        MemoryCeilingScope, PlatformFact, PlatformMetadata, PlatformResourceSource,
    };

    const SOURCE: PlatformResourceSource = PlatformResourceSource::PlatformLimit {
        provider: "macro-test",
    };
    const TEST_PLATFORM: PlatformMetadata = PlatformMetadata::new(
        PlatformFact::known(
            MemoryCeiling::new(64_000_000, MemoryCeilingScope::PerExecution, None),
            SOURCE,
        ),
        PlatformFact::known(InboundRequestPopulationBound::new(NonZeroU32::MIN), SOURCE),
        PlatformFact::known(HostIngressMemoryAccounting::OutsideCeiling, SOURCE),
    );

    fn configure_app(app: &mut App) -> Result<(), EdgeError> {
        if app.platform() == TEST_PLATFORM {
            Ok(())
        } else {
            Err(EdgeError::service_unavailable(
                "platform metadata unavailable during configuration",
            ))
        }
    }

    edgezero_core::app!(
        "tests/fixtures/owns_logging.toml",
        PlatformConfiguredApp,
        configure = configure_app
    );

    #[test]
    fn app_macro_installs_platform_metadata_before_configuration() {
        let app = App::build::<PlatformConfiguredApp>(TEST_PLATFORM)
            .expect("platform-aware configured app");

        assert_eq!(app.platform(), TEST_PLATFORM);
    }
}

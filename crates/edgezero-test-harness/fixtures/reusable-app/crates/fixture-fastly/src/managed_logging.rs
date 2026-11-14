use edgezero_adapter_fastly::{
    FASTLY_PLATFORM, Serve, init_logger,
    lifecycle::{Sandbox, run_custom, serve_custom},
};
use edgezero_core::{
    app::{App, Hooks},
    logging::ManagedLoggerError,
    manifest::ResolvedLoggingConfig,
};
use fixture_core::managed_logging::LoggingApp;
use log::LevelFilter;

struct LoggingHooks<const OWNED: bool>;

impl<const OWNED: bool> Hooks for LoggingHooks<OWNED> {
    fn owns_logging() -> bool {
        OWNED
    }
    fn logging_for(_adapter: &str) -> ResolvedLoggingConfig {
        ResolvedLoggingConfig {
            endpoint: Some("fixture-logs".to_owned()),
            echo_stdout: Some(false),
            ..Default::default()
        }
    }
    fn routes() -> Result<edgezero_core::router::RouterService, edgezero_core::EdgeError> {
        LoggingApp::<OWNED, false>::routes()
    }
}

fn initialize(level: LevelFilter) -> Result<(), ManagedLoggerError> {
    init_logger("fixture-logs", level, false)
}

fn observe<const OWNED: bool>(request: &fastly::Request, before: LevelFilter, level: LevelFilter) {
    let token = request
        .get_header_str("x-request-token")
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .expect("bounded probe token");
    assert_eq!(
        log::max_level(),
        if OWNED {
            LevelFilter::Info
        } else {
            level.max(LevelFilter::Warn)
        },
        "runner must install before callback"
    );
    let mut witness =
        fixture_core::managed_logging::reinitialization_probe(OWNED, before, level, initialize);
    if !OWNED {
        let maximum = log::max_level();
        assert_eq!(
            init_logger("other-endpoint", level, false),
            Err(ManagedLoggerError::ConfigurationChanged)
        );
        assert_eq!(log::max_level(), maximum);
        assert_eq!(
            init_logger("fixture-logs", level, true),
            Err(ManagedLoggerError::ConfigurationChanged)
        );
        assert_eq!(log::max_level(), maximum);
    }
    witness["unchanged"] =
        (witness["unchanged"] == true && witness["after"] == log::max_level().to_string()).into();
    witness["endpoint_conflict"] = true.into();
    witness["stdout_conflict"] = true.into();
    fixture_core::managed_logging::emit_records(&format!("after-{token}"));
    println!("{witness}");
}

fn execute<const OWNED: bool>(case: &str, level: LevelFilter) -> Result<(), fastly::Error> {
    if OWNED {
        fixture_core::init_fixture_logging(|line| println!("{line}"))?;
    }
    let before = log::max_level();
    let serving = Serve::new().with_max_requests(2);
    if case.contains("-standard-") {
        return crate::finish(edgezero_adapter_fastly::serve_app_with_hooks::<
            LoggingHooks<OWNED>,
            _,
            _,
            _,
        >(
            serving,
            |request, _| observe::<OWNED>(request, before, level),
            |_| (),
        ));
    }
    let handler = |request: fastly::Request, state: &mut Sandbox<App>| {
        state.initialize(|| App::build::<LoggingHooks<OWNED>>(FASTLY_PLATFORM))?;
        crate::strict_delivery(
            edgezero_adapter_fastly::request::send_request_with_registries_and_hooks(
                state.state().expect("retained logging app"),
                LoggingHooks::<OWNED>::stores(),
                request,
                |request, _| observe::<OWNED>(request, before, level),
                |_| (),
            ),
        )
    };
    if case.contains("-run-") {
        run_custom::<LoggingHooks<OWNED>, _, _, _>(fastly::Request::from_client(), handler)
    } else {
        crate::finish(serve_custom::<LoggingHooks<OWNED>, _, _, _>(
            serving, handler,
        ))
    }
}

pub fn run(case: &str, level: LevelFilter) -> Result<(), fastly::Error> {
    if case.ends_with("-owned") {
        execute::<true>(case, level)
    } else {
        execute::<false>(case, level)
    }
}

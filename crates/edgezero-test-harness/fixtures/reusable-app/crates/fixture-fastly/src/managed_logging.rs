use edgezero_adapter_fastly::{
    FASTLY_PLATFORM, Serve, init_logger,
    lifecycle::{Sandbox, run_custom, serve_custom},
    request::send_request_with_registries_and_hooks,
};
use edgezero_core::{
    app::{App, Hooks},
    logging::ManagedLoggerError,
    manifest::ResolvedLoggingConfig,
};
use fixture_core::managed_logging::LoggingApp;
use log::LevelFilter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

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

static CHANGE_LOGGING: AtomicBool = AtomicBool::new(false);

struct ChangingLoggingHooks;

impl Hooks for ChangingLoggingHooks {
    fn logging_for(adapter: &str) -> ResolvedLoggingConfig {
        let mut logging = LoggingHooks::<false>::logging_for(adapter);
        if CHANGE_LOGGING.load(Ordering::SeqCst) {
            logging.endpoint = Some("other-endpoint".to_owned());
        }
        logging
    }

    fn routes() -> Result<edgezero_core::router::RouterService, edgezero_core::EdgeError> {
        LoggingHooks::<false>::routes()
    }
}

struct Terminal(Result<(), fastly::Error>);

impl fastly::http::serve::HandlerResult for Terminal {
    type Error = fastly::Error;

    fn send(self) -> Result<(), Self::Error> {
        self.0
    }
}

#[derive(Default)]
struct ControlWitness {
    callbacks: u64,
    healthy_token: Option<String>,
    maximum: Option<String>,
}

fn control_token(request: &fastly::Request) -> Result<String, fastly::Error> {
    request
        .get_header_str("x-request-token")
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .map(str::to_owned)
        .ok_or_else(|| fastly::Error::msg("missing bounded logging control token"))
}

fn healthy_control(case: &str, token: String, witness: &mut ControlWitness) {
    witness.healthy_token = Some(token);
    witness.maximum = Some(log::max_level().to_string());
    println!(
        "{}",
        serde_json::json!({"event":"managed-logging-control-healthy","case":case,
            "instance":fixture_core::managed_logging::instance(),
            "healthy_token":witness.healthy_token})
    );
}

fn outer_healthy(
    request: fastly::Request,
    case: &str,
    witness: &mut ControlWitness,
) -> Result<(), fastly::Error> {
    if request.get_path() != "/managed-logging" {
        return Err(fastly::Error::msg(
            "expected prescribed healthy logging request",
        ));
    }
    let token = control_token(&request)?;
    let app = App::build::<LoggingHooks<true>>(FASTLY_PLATFORM)?;
    crate::strict_delivery(send_request_with_registries_and_hooks(
        &app,
        LoggingHooks::<true>::stores(),
        request,
        |_, _| {},
        |_| (),
    ))?;
    healthy_control(case, token, witness);
    Ok(())
}

fn control_callback(
    request: fastly::Request,
    state: &mut Sandbox<App>,
    case: &str,
    witness: &mut ControlWitness,
    single: bool,
    callback_error: bool,
) -> Result<(), fastly::Error> {
    witness.callbacks += 1;
    let token = control_token(&request)?;
    println!(
        "{}",
        serde_json::json!({"event":"managed-logging-control-callback","case":case,
            "instance":fixture_core::managed_logging::instance(),
            "ordinal":witness.callbacks,"token":token})
    );
    let healthy = witness.healthy_token.is_none();
    let path = if healthy {
        "/managed-logging"
    } else {
        "/managed-logging/failure"
    };
    if request.get_path() != path || (!healthy && !callback_error) || witness.callbacks > 2 {
        return Err(fastly::Error::msg(
            "unexpected logging control callback entry",
        ));
    }
    state.initialize(|| App::build::<LoggingHooks<false>>(FASTLY_PLATFORM))?;
    crate::strict_delivery(send_request_with_registries_and_hooks(
        state.state().expect("logging control app"),
        LoggingHooks::<false>::stores(),
        request,
        |_, _| {},
        |_| (),
    ))?;
    if healthy {
        healthy_control(case, token, witness);
        if !single && !callback_error {
            CHANGE_LOGGING.store(true, Ordering::SeqCst);
        }
        Ok(())
    } else {
        Err(if single {
            ManagedLoggerError::AlreadyInstalled
        } else {
            ManagedLoggerError::ConfigurationChanged
        }
        .into())
    }
}

fn failure_control(case: &str) -> Result<(), fastly::Error> {
    let (single, callback_error) = match case {
        "managed-logging-pre-callback-serve-get" | "managed-logging-pre-callback-serve-head" => {
            (false, false)
        }
        "managed-logging-pre-callback-run-get" | "managed-logging-pre-callback-run-head" => {
            (true, false)
        }
        "managed-logging-callback-error-serve" => (false, true),
        "managed-logging-callback-error-run" => (true, true),
        _ => return Err(fastly::Error::msg("unknown logging failure control")),
    };
    let serving = Serve::new()
        .with_max_requests(3)
        .with_timeout(Duration::from_secs(5));
    let mut witness = ControlWitness::default();
    let summary = if single {
        if !callback_error {
            fixture_core::init_fixture_logging(|line| println!("{line}"))?;
        }
        let mut attempted = 0;
        // The bootstrap loop is fixture-only; the failure request enters real run_custom.
        serving.run_with_context(
            |request, _: &mut ()| {
                attempted += 1;
                Terminal(if attempted == 1 && !callback_error {
                    outer_healthy(request, case, &mut witness)
                } else {
                    run_custom::<LoggingHooks<false>, _, _, _>(request, |request, state| {
                        control_callback(request, state, case, &mut witness, true, callback_error)
                    })
                })
            },
            &mut (),
        )
    } else {
        serve_custom::<ChangingLoggingHooks, _, _, _>(serving, |request, state| {
            control_callback(request, state, case, &mut witness, false, callback_error)
        })
    };
    let attempted = summary.requests();
    let result = summary.into_result();
    let error = result
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<ManagedLoggerError>());
    println!(
        "{}",
        serde_json::json!({"event":"managed-logging-control-terminal","case":case,
            "instance":fixture_core::managed_logging::instance(),
            "healthy_token":witness.healthy_token,"attempted":attempted,"limit":3,
            "callbacks":witness.callbacks,"error":error.map(|error| format!("{error:?}")),
            "maximum_before":witness.maximum,"maximum_after":log::max_level().to_string(),
            "qualification":false})
    );
    result
}

pub fn run(case: &str, level: LevelFilter) -> Result<(), fastly::Error> {
    if case.starts_with("managed-logging-pre-callback-")
        || case.starts_with("managed-logging-callback-error-")
    {
        return failure_control(case);
    }
    if case.ends_with("-owned") {
        execute::<true>(case, level)
    } else {
        execute::<false>(case, level)
    }
}

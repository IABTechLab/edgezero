#[cfg(feature = "fastly")]
use crate::{FastlyLogging, config_store::logging_level_override};
#[cfg(feature = "fastly")]
use edgezero_core::app::Hooks;
use edgezero_core::env_config::EnvConfig;
use edgezero_core::logging::{
    BOOT_LOG_LEVEL, BOOT_LOG_TARGET, ManagedLoggerError, ManagedLoggerInstallation,
};
use log::LevelFilter;
#[cfg(feature = "fastly")]
use std::sync::OnceLock;

static INSTALLATION: ManagedLoggerInstallation<Configuration> = ManagedLoggerInstallation::new();
#[cfg(feature = "fastly")]
static OVERRIDE: OnceLock<Result<Option<LevelFilter>, ManagedLoggerError>> = OnceLock::new();

#[derive(Clone, Debug, Eq, PartialEq)]
struct Configuration {
    echo_stdout: bool,
    endpoint: String,
    level: LevelFilter,
}

fn build_backend(
    endpoint: &str,
    level: LevelFilter,
    echo_stdout: bool,
) -> Result<log_fastly::Logger, ManagedLoggerError> {
    log_fastly::Logger::builder()
        .default_endpoint(endpoint)
        .echo_stdout(echo_stdout)
        .max_level(level.max(BOOT_LOG_LEVEL))
        .build()
        .map_err(|_error| ManagedLoggerError::InvalidConfiguration)
}

fn build_dispatch(logger: Box<dyn log::Log>, level: LevelFilter) -> fern::Dispatch {
    fern::Dispatch::new()
        .level(level)
        .level_for(BOOT_LOG_TARGET, BOOT_LOG_LEVEL)
        .format(|out, message, record| {
            out.finish(format_args!(
                "{} {} {} {}",
                chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                record.level(),
                record.target(),
                message
            ));
        })
        .chain(logger)
}

/// Initialize logging (opinionated): formatted timestamps using `fern`,
/// chained to the Fastly logger. Boot warnings are retained independently of
/// the ordinary runtime level, including `Off`.
///
/// # Errors
/// Returns a fixed typed error for invalid settings, a foreign logger, or
/// settings that differ from this instance's managed installation.
#[inline]
pub fn init_logger(
    endpoint: &str,
    level: LevelFilter,
    echo_stdout: bool,
) -> Result<(), ManagedLoggerError> {
    INSTALLATION.initialize(
        &Configuration {
            echo_stdout,
            endpoint: endpoint.to_owned(),
            level,
        },
        || {
            let logger = build_backend(endpoint, level, echo_stdout)?;
            build_dispatch(Box::new(logger), level)
                .apply()
                .map_err(|_error| ManagedLoggerError::AlreadyInstalled)
        },
    )
}

fn resolve_level(baked: LevelFilter, value: Option<&str>) -> LevelFilter {
    value.map_or(baked, |raw| {
        edgezero_core::resolve_logging_level(&EnvConfig::from_vars([(
            "EDGEZERO__LOGGING__LEVEL",
            raw,
        )]))
    })
}

#[cfg(feature = "fastly")]
pub(crate) fn initialize_for_app<A>() -> Result<(), ManagedLoggerError>
where
    A: Hooks,
{
    if A::owns_logging() {
        return Ok(());
    }
    configure::<A>(&FastlyLogging::from(A::logging_for("fastly")))
}

#[cfg(feature = "fastly")]
pub(crate) fn configure<A>(logging: &FastlyLogging) -> Result<(), ManagedLoggerError>
where
    A: Hooks,
{
    if A::owns_logging() || !logging.use_fastly_logger {
        return Ok(());
    }
    // A bounded instance snapshot avoids repeated host reads during sandbox reuse.
    let override_level = *OVERRIDE.get_or_init(|| {
        logging_level_override().map(|value| {
            value
                .as_deref()
                .map(|raw| resolve_level(LevelFilter::Info, Some(raw)))
        })
    });
    init_logger(
        logging.endpoint.as_deref().unwrap_or("stdout"),
        override_level?.unwrap_or(logging.level),
        logging.echo_stdout,
    )
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use crate::context::tests::init_registration_logger;
    use edgezero_core::logging::{BOOT_LOG_LEVEL, BOOT_LOG_TARGET, ManagedLoggerError};
    use log::{Level, LevelFilter, Log as _, Metadata, Record};
    use std::env;
    use std::process::{Command, Stdio};

    struct Foreign;
    impl log::Log for Foreign {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }
        fn flush(&self) {}
        fn log(&self, _record: &Record<'_>) {}
    }

    #[test]
    fn managed_logger_preserves_foreign_backend_and_complete_settings() {
        const CHILD: &str = "EDGEZERO_FASTLY_LOGGER_CHILD";
        if let Some(mode) = env::var_os(CHILD) {
            if mode == "foreign" {
                log::set_logger(&Foreign).unwrap();
                log::set_max_level(LevelFilter::Error);
                assert_eq!(
                    super::init_logger("test-endpoint", LevelFilter::Info, false),
                    Err(ManagedLoggerError::AlreadyInstalled)
                );
                assert_eq!(log::max_level(), LevelFilter::Error);
            } else {
                super::init_logger("test-endpoint", LevelFilter::Info, false).unwrap();
                super::init_logger("test-endpoint", LevelFilter::Info, false).unwrap();
                for (endpoint, level, echo) in [
                    ("changed", LevelFilter::Info, false),
                    ("test-endpoint", LevelFilter::Debug, false),
                    ("test-endpoint", LevelFilter::Info, true),
                ] {
                    assert_eq!(
                        super::init_logger(endpoint, level, echo),
                        Err(ManagedLoggerError::ConfigurationChanged)
                    );
                    assert_eq!(log::max_level(), LevelFilter::Info);
                }
            }
            return;
        }
        for mode in ["managed", "foreign"] {
            let result = Command::new(env::current_exe().unwrap())
                .args([
                    "--exact",
                    "logger::tests::managed_logger_preserves_foreign_backend_and_complete_settings",
                ])
                .env(CHILD, mode)
                .stdin(Stdio::null())
                .output()
                .unwrap();
            assert!(result.status.success(), "{result:?}");
        }
    }

    #[test]
    fn runtime_override_preserves_baked_default_and_uses_shared_invalid_fallback() {
        for baked in [
            LevelFilter::Off,
            LevelFilter::Warn,
            LevelFilter::Info,
            LevelFilter::Trace,
        ] {
            assert_eq!(super::resolve_level(baked, None), baked);
            for (value, expected) in [
                ("DeBuG", LevelFilter::Debug),
                ("off", LevelFilter::Off),
                ("", LevelFilter::Info),
                ("invalid", LevelFilter::Info),
            ] {
                assert_eq!(super::resolve_level(baked, Some(value)), expected);
            }
        }
    }

    #[test]
    fn backend_rejects_records_even_when_facade_allows_them() {
        let logger = log_fastly::Logger::builder()
            .default_endpoint("logging-test")
            .max_level(LevelFilter::Error)
            .build()
            .expect("SDK logger");
        assert!(
            !logger.enabled(
                &Metadata::builder()
                    .target(BOOT_LOG_TARGET)
                    .level(Level::Warn)
                    .build()
            )
        );
    }

    #[test]
    fn production_filters_preserve_boot_warnings_and_runtime_level() {
        // Share the facade owner with registration tests; backend filtering stays local.
        init_registration_logger();
        let guard = log_fastly::reset_logger(
            log_fastly::Logger::builder()
                .default_endpoint("logging-test")
                .max_level(LevelFilter::Trace)
                .build()
                .expect("isolated SDK capture"),
        );
        assert_eq!(log::max_level(), LevelFilter::Trace);

        for runtime_level in [
            LevelFilter::Off,
            LevelFilter::Error,
            LevelFilter::Warn,
            LevelFilter::Info,
            LevelFilter::Debug,
            LevelFilter::Trace,
        ] {
            let backend =
                super::build_backend("logging-test", runtime_level, false).expect("SDK backend");
            let (ceiling, logger) =
                super::build_dispatch(Box::new(backend), runtime_level).into_log();
            assert_eq!(ceiling, runtime_level.max(BOOT_LOG_LEVEL));
            let preserved_warning = format!("preserved boot warning {runtime_level}");

            for level in [
                Level::Error,
                Level::Warn,
                Level::Info,
                Level::Debug,
                Level::Trace,
            ] {
                let boot = Metadata::builder()
                    .target(BOOT_LOG_TARGET)
                    .level(level)
                    .build();
                let ordinary = Metadata::builder()
                    .target("application")
                    .level(level)
                    .build();
                assert_eq!(logger.enabled(&boot), level <= BOOT_LOG_LEVEL);
                assert_eq!(logger.enabled(&ordinary), level <= runtime_level);
            }

            logger.log(
                &Record::builder()
                    .target(BOOT_LOG_TARGET)
                    .level(Level::Warn)
                    .args(format_args!("{preserved_warning}"))
                    .build(),
            );
            guard.assert_contains("logging-test", &preserved_warning);
            guard.assert_contains(
                "logging-test",
                &format!("WARN {BOOT_LOG_TARGET} {preserved_warning}"),
            );

            logger.log(
                &Record::builder()
                    .target(BOOT_LOG_TARGET)
                    .level(Level::Info)
                    .args(format_args!("suppressed boot info"))
                    .build(),
            );
            guard.assert_absent("logging-test", "suppressed boot info");

            if runtime_level <= LevelFilter::Error {
                logger.log(
                    &Record::builder()
                        .target("application")
                        .level(Level::Warn)
                        .args(format_args!("suppressed runtime warning"))
                        .build(),
                );
                guard.assert_absent("logging-test", "suppressed runtime warning");
            }
        }
    }
}

use edgezero_core::logging::ManagedLoggerError;
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
use edgezero_core::logging::ManagedLoggerInstallation;
#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
use edgezero_core::logging::{BOOT_LOG_LEVEL, BOOT_LOG_TARGET};
use log::LevelFilter;

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
static INSTALLATION: ManagedLoggerInstallation = ManagedLoggerInstallation::new();

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
struct Console;

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
impl log::Log for Console {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn flush(&self) {}

    fn log(&self, record: &log::Record<'_>) {
        match record.level() {
            log::Level::Error => worker::console_error!("{}", record.args()),
            log::Level::Warn => worker::console_warn!("{}", record.args()),
            log::Level::Info => worker::console_log!("{}", record.args()),
            log::Level::Debug | log::Level::Trace => worker::console_debug!("{}", record.args()),
        }
    }
}

#[cfg(any(test, all(feature = "cloudflare", target_arch = "wasm32")))]
fn build_dispatch(logger: Box<dyn log::Log>, level: LevelFilter) -> fern::Dispatch {
    fern::Dispatch::new()
        .level(level)
        .level_for(BOOT_LOG_TARGET, BOOT_LOG_LEVEL)
        .format(|out, message, record| {
            out.finish(format_args!(
                "{} {} {}",
                record.level(),
                record.target(),
                message
            ));
        })
        .chain(logger)
}

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
pub(crate) fn initialize(level: LevelFilter) -> Result<(), ManagedLoggerError> {
    INSTALLATION.initialize(level, || {
        build_dispatch(Box::new(Console), level)
            .apply()
            .map_err(|_error| ManagedLoggerError::AlreadyInstalled)
    })
}

#[cfg(not(all(feature = "cloudflare", target_arch = "wasm32")))]
pub(crate) fn initialize(_level: LevelFilter) -> Result<(), ManagedLoggerError> {
    Err(ManagedLoggerError::UnsupportedTarget)
}

#[cfg(test)]
mod tests {
    use edgezero_core::logging::{BOOT_LOG_LEVEL, BOOT_LOG_TARGET};
    use log::{Level, LevelFilter, Metadata, Record};

    struct Sink;
    impl log::Log for Sink {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }
        fn flush(&self) {}
        fn log(&self, _record: &Record<'_>) {}
    }

    #[test]
    fn console_dispatch_matches_fastly_runtime_and_boot_filters() {
        for level in [
            LevelFilter::Off,
            LevelFilter::Error,
            LevelFilter::Warn,
            LevelFilter::Info,
            LevelFilter::Debug,
            LevelFilter::Trace,
        ] {
            let (ceiling, backend) = super::build_dispatch(Box::new(Sink), level).into_log();
            assert_eq!(ceiling, level.max(BOOT_LOG_LEVEL));
            for record_level in [
                Level::Error,
                Level::Warn,
                Level::Info,
                Level::Debug,
                Level::Trace,
            ] {
                for target in [BOOT_LOG_TARGET, "edgezero::boot::nested", "application"] {
                    let expected = if target.starts_with(BOOT_LOG_TARGET) {
                        record_level <= BOOT_LOG_LEVEL
                    } else {
                        record_level <= level
                    };
                    assert_eq!(
                        backend.enabled(
                            &Metadata::builder()
                                .target(target)
                                .level(record_level)
                                .build()
                        ),
                        expected
                    );
                }
            }
        }
    }

    #[cfg(not(all(feature = "cloudflare", target_arch = "wasm32")))]
    #[test]
    fn off_target_initialization_is_explicit_and_does_not_change_logging() {
        let before = log::max_level();
        assert_eq!(
            super::initialize(LevelFilter::Trace),
            Err(super::ManagedLoggerError::UnsupportedTarget)
        );
        assert_eq!(log::max_level(), before);
    }
}

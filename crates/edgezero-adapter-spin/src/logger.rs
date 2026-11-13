use edgezero_core::logging::{
    BOOT_LOG_LEVEL, BOOT_LOG_TARGET, ManagedLoggerError, ManagedLoggerInstallation,
};
use log::LevelFilter;
use std::io;

static INSTALLATION: ManagedLoggerInstallation = ManagedLoggerInstallation::new();

pub(crate) fn initialize(level: LevelFilter) -> Result<(), ManagedLoggerError> {
    INSTALLATION.initialize(level, || {
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
            .chain(io::stderr())
            .apply()
            .map_err(|_error| ManagedLoggerError::AlreadyInstalled)
    })
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use edgezero_core::logging::{BOOT_LOG_TARGET, ManagedLoggerError};
    use log::{LevelFilter, Log, Metadata, Record};
    use std::env;
    use std::process::{Command, Stdio};

    struct Foreign;
    impl Log for Foreign {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }
        fn flush(&self) {}
        fn log(&self, _record: &Record<'_>) {}
    }

    #[test]
    fn managed_logger_emits_handler_warning_and_boot_error() {
        if let Some(mode) = env::var_os("EDGEZERO_SPIN_LOGGER_CHILD") {
            if mode == "foreign" {
                log::set_logger(&Foreign).unwrap();
                log::set_max_level(LevelFilter::Error);
                assert_eq!(
                    super::initialize(LevelFilter::Trace),
                    Err(ManagedLoggerError::AlreadyInstalled)
                );
                assert_eq!(log::max_level(), LevelFilter::Error);
            } else {
                super::initialize(LevelFilter::Info).expect("managed logger");
                log::warn!("managed-spin-handler-warning");
                log::error!(target: BOOT_LOG_TARGET, "managed-spin-boot-error");
                super::initialize(LevelFilter::Info).unwrap();
                assert_eq!(
                    super::initialize(LevelFilter::Off),
                    Err(ManagedLoggerError::ConfigurationChanged)
                );
                assert_eq!(log::max_level(), LevelFilter::Info);
            }
            return;
        }
        for mode in ["managed", "foreign"] {
            let child = Command::new(env::current_exe().expect("test executable"))
                .args([
                    "--exact",
                    "logger::tests::managed_logger_emits_handler_warning_and_boot_error",
                    "--nocapture",
                ])
                .env("EDGEZERO_SPIN_LOGGER_CHILD", mode)
                .stdin(Stdio::null())
                .output()
                .expect("logger subprocess");
            assert!(child.status.success(), "{child:?}");
            if mode == "managed" {
                let stderr = String::from_utf8(child.stderr).unwrap();
                assert!(stderr.contains("managed-spin-handler-warning"), "{stderr}");
                assert!(stderr.contains("managed-spin-boot-error"), "{stderr}");
            }
        }
    }
}

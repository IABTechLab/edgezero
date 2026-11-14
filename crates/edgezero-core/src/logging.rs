//! Shared logging policy for adapter and application-owned backends.

use std::str::FromStr as _;
use std::sync::OnceLock;

use log::LevelFilter;

use crate::env_config::EnvConfig;

/// Default boot filter for managed backends, independent of ordinary logging.
pub const BOOT_LOG_LEVEL: LevelFilter = LevelFilter::Warn;
/// Logical target for startup diagnostics; this is not a platform log endpoint.
pub const BOOT_LOG_TARGET: &str = "edgezero::boot";

/// Fixed, category-only failures from an adapter-managed logger.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ManagedLoggerError {
    #[error("a logger is already installed")]
    AlreadyInstalled,
    #[error("managed logging configuration changed within an instance")]
    ConfigurationChanged,
    #[error("managed logging configuration is unavailable")]
    ConfigurationUnavailable,
    #[error("managed logging configuration is invalid")]
    InvalidConfiguration,
    #[error("the platform logging backend is unavailable on this target")]
    UnsupportedTarget,
}

/// One immutable managed-backend installation per runtime instance.
///
/// Repeated setup must use the same complete settings. Failed global installation
/// is retained because the log facade cannot replace an installed backend.
#[derive(Default)]
pub struct ManagedLoggerInstallation<Configuration> {
    installed: OnceLock<Result<Configuration, ManagedLoggerError>>,
}

impl<Configuration: Clone + Eq> ManagedLoggerInstallation<Configuration> {
    /// Run the provider's registration exactly once, without reconfiguring live work.
    ///
    /// # Errors
    /// Returns the registration failure or a changed instance-wide configuration.
    #[inline]
    pub fn initialize<Install>(
        &self,
        configuration: &Configuration,
        install: Install,
    ) -> Result<(), ManagedLoggerError>
    where
        Install: FnOnce() -> Result<(), ManagedLoggerError>,
    {
        match self
            .installed
            .get_or_init(|| install().map(|()| configuration.clone()))
        {
            Ok(installed) if installed == configuration => Ok(()),
            Ok(_) => Err(ManagedLoggerError::ConfigurationChanged),
            Err(error) => Err(*error),
        }
    }

    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self {
            installed: OnceLock::new(),
        }
    }
}

/// Resolve `EDGEZERO__LOGGING__LEVEL` without installing or reconfiguring a logger.
///
/// Valid values are case-insensitive `off`, `error`, `warn`, `info`, `debug`, and
/// `trace`. Missing or invalid values fall back to `Info`. The caller supplies
/// platform-resolved environment configuration; this function does not read the
/// process environment or implicitly apply manifest logging settings.
///
/// Application-owned backends must apply this level to ordinary records, apply
/// their chosen boot filter to [`BOOT_LOG_TARGET`], and set the facade maximum
/// high enough for both. Raising the facade maximum does not override a backend
/// filter.
#[must_use]
#[inline]
pub fn resolve_logging_level(env: &EnvConfig) -> LevelFilter {
    env.logging_level()
        .and_then(|raw| LevelFilter::from_str(raw).ok())
        .unwrap_or(LevelFilter::Info)
}

#[cfg(test)]
mod tests {
    use log::LevelFilter;
    use std::cell::Cell;

    use crate::env_config::EnvConfig;

    #[test]
    fn managed_installation_is_idempotent_and_never_reloads_a_live_filter() {
        let installation = super::ManagedLoggerInstallation::new();
        let attempts = Cell::new(0_usize);
        let install = || {
            attempts.set(attempts.get() + 1);
            Ok(())
        };
        installation
            .initialize(&LevelFilter::Info, install)
            .unwrap();
        installation
            .initialize(&LevelFilter::Info, install)
            .unwrap();
        assert_eq!(attempts.get(), 1);
        assert_eq!(
            installation.initialize(&LevelFilter::Off, install),
            Err(super::ManagedLoggerError::ConfigurationChanged)
        );
        assert_eq!(attempts.get(), 1);
    }

    #[test]
    fn managed_installation_retains_foreign_logger_conflict_without_retry() {
        let installation = super::ManagedLoggerInstallation::new();
        let failure = super::ManagedLoggerError::AlreadyInstalled;
        assert_eq!(
            installation.initialize(&LevelFilter::Info, || Err(failure)),
            Err(failure)
        );
        assert_eq!(
            installation.initialize(&LevelFilter::Trace, || panic!("must not reinstall")),
            Err(failure)
        );
    }

    #[test]
    fn managed_installation_compares_complete_settings_without_reinstallation() {
        let installation = super::ManagedLoggerInstallation::new();
        let settings = ("endpoint".to_owned(), LevelFilter::Info, false);
        installation.initialize(&settings, || Ok(())).unwrap();
        installation
            .initialize(&settings, || panic!("must not reinstall"))
            .unwrap();
        for changed in [
            ("different".to_owned(), LevelFilter::Info, false),
            ("endpoint".to_owned(), LevelFilter::Debug, false),
            ("endpoint".to_owned(), LevelFilter::Info, true),
        ] {
            assert_eq!(
                installation.initialize(&changed, || panic!("must not reinstall")),
                Err(super::ManagedLoggerError::ConfigurationChanged),
            );
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn overlapping_managed_initialization_registers_only_once() {
        use std::iter::repeat_with;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier};
        use std::thread;

        let installation = Arc::new(super::ManagedLoggerInstallation::new());
        let attempts = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));
        let workers: Vec<_> = repeat_with(|| {
            let shared = Arc::clone(&installation);
            let count = Arc::clone(&attempts);
            let start = Arc::clone(&barrier);
            thread::spawn(move || {
                start.wait();
                shared
                    .initialize(&LevelFilter::Info, || {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .unwrap();
            })
        })
        .take(8)
        .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn boot_target_preserves_warnings_without_runtime_verbosity() {
        assert_eq!(super::BOOT_LOG_TARGET, "edgezero::boot");
        assert_eq!(super::BOOT_LOG_LEVEL, LevelFilter::Warn);
    }

    #[test]
    fn resolver_defaults_absent_and_invalid_levels_to_info() {
        for raw in [None, Some(""), Some("invalid"), Some(" debug ")] {
            let env = EnvConfig::from_vars(raw.map(|level| ("EDGEZERO__LOGGING__LEVEL", level)));
            assert_eq!(super::resolve_logging_level(&env), LevelFilter::Info);
        }
    }

    #[test]
    fn resolver_preserves_all_levels_and_case_insensitivity() {
        for (raw, expected) in [
            ("off", LevelFilter::Off),
            ("error", LevelFilter::Error),
            ("warn", LevelFilter::Warn),
            ("info", LevelFilter::Info),
            ("debug", LevelFilter::Debug),
            ("trace", LevelFilter::Trace),
            ("DeBuG", LevelFilter::Debug),
        ] {
            let env = EnvConfig::from_vars([("EDGEZERO__LOGGING__LEVEL", raw)]);
            assert_eq!(super::resolve_logging_level(&env), expected);
        }
    }

    #[test]
    fn resolver_does_not_change_global_logging_state() {
        let before = log::max_level();
        let env = EnvConfig::from_vars([("EDGEZERO__LOGGING__LEVEL", "trace")]);
        assert_eq!(super::resolve_logging_level(&env), LevelFilter::Trace);
        assert_eq!(log::max_level(), before);
    }
}

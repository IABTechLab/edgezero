use std::env::vars_os;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::time::{Duration, Instant};

use edgezero_core::addr;
use edgezero_core::app::StoresMetadata;
use edgezero_core::env_config::EnvConfig;
use log::LevelFilter;

/// Validated explicit production hosting options. Construction never selects a
/// policy from build profile, cwd or container detection. Explicit constructors
/// do not merge environment values; only `from_env` reads hosting environment.
#[derive(Clone)]
pub struct AxumRunOptions {
    addr: SocketAddr,
    config_dir: Option<PathBuf>,
    data_dir: Option<PathBuf>,
    env: EnvConfig,
    grace: Duration,
    level: LevelFilter,
    non_unicode_overrides: Vec<Vec<String>>,
}

impl AxumRunOptions {
    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) fn config_dir(&self) -> Option<&Path> {
        self.config_dir.as_deref()
    }

    pub(crate) fn data_dir(&self) -> Option<&Path> {
        self.data_dir.as_deref()
    }

    pub(crate) fn env_config(&self) -> &EnvConfig {
        &self.env
    }

    /// Captures hosting settings and store overrides once. Secret values still
    /// use the existing environment-backed secret backend.
    ///
    /// # Errors
    /// Returns safe setting categories for supplied invalid hosting settings.
    #[inline]
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_os_vars(vars_os(), cfg!(windows))
    }

    fn from_os_vars<I>(vars: I, windows: bool) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = (OsString, OsString)>,
    {
        let mut pairs = Vec::new();
        let mut non_unicode = Vec::new();
        for (os_key, os_value) in vars {
            let Some(key) = os_key.to_str() else { continue };
            let Some(path) = config_path(key, windows) else {
                continue;
            };
            if let Some(value) = os_value.to_str() {
                pairs.push((format!("EDGEZERO__{}", path.join("__")), value.to_owned()));
            } else if let Some(setting) = fixed_setting(&path) {
                return Err(failure("settings", setting, "non-Unicode value"));
            } else {
                let segments: Vec<&str> = path.iter().map(String::as_str).collect();
                if matches!(
                    segments.as_slice(),
                    ["stores", "config" | "kv" | "secrets", _, "name" | "key"]
                ) {
                    non_unicode.push(path);
                }
            }
        }
        let mut options = Self::from_vars(pairs)?;
        options.non_unicode_overrides = non_unicode;
        Ok(options)
    }

    /// Constructs options using only supplied string pairs and omission defaults.
    /// Declared-store overrides are validated against baked metadata at startup.
    ///
    /// # Errors
    /// Returns safe setting categories for invalid bind, logging, roots or grace.
    #[inline]
    pub fn from_vars<I, K, V>(vars: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let env = EnvConfig::from_vars(vars);
        let host = env.adapter_host().map_or(Ok(addr::DEFAULT_HOST), |raw| {
            raw.parse()
                .map_err(|_error| failure("settings", "HOST", "invalid IP literal"))
        })?;
        let port = env.adapter_port().map_or(Ok(addr::DEFAULT_PORT), |raw| {
            raw.parse::<u16>()
                .ok()
                .filter(|port| *port != 0)
                .ok_or_else(|| failure("settings", "PORT", "invalid nonzero port"))
        })?;
        let mut options = Self::new(SocketAddr::new(host, port))?;
        if let Some(level) = env.logging_level() {
            options.level = LevelFilter::from_str(level)
                .map_err(|_error| failure("settings", "LOGGING__LEVEL", "invalid level"))?;
        }
        options.grace = shutdown_grace(&env)?;
        if let Some(root) = env.get(&["adapter", "config_dir"]) {
            options = options.with_config_dir(root)?;
        }
        if let Some(root) = env.get(&["adapter", "data_dir"]) {
            options = options.with_data_dir(root)?;
        }
        options.env = env;
        Ok(options)
    }

    pub(crate) fn logging_level(&self) -> LevelFilter {
        self.level
    }

    /// Explicit bind with default grace/logging, no roots, and no store overrides.
    ///
    /// # Errors
    /// Rejects port zero. Production omission defaults apply only to omitted values.
    #[inline]
    pub fn new(addr: SocketAddr) -> anyhow::Result<Self> {
        if addr.port() == 0 {
            return Err(failure("settings", "PORT", "invalid nonzero port"));
        }
        Ok(Self {
            addr,
            config_dir: None,
            data_dir: None,
            env: EnvConfig::default(),
            grace: Duration::from_secs(10),
            level: LevelFilter::Info,
            non_unicode_overrides: Vec::new(),
        })
    }

    pub(crate) fn shutdown_grace(&self) -> Duration {
        self.grace
    }

    pub(crate) fn validate_stores(&self, metadata: StoresMetadata) -> anyhow::Result<()> {
        for (kind, declaration) in [
            ("config", metadata.config),
            ("kv", metadata.kv),
            ("secrets", metadata.secrets),
        ] {
            if let Some(meta) = declaration {
                for id in meta.ids {
                    for field in ["name", "key"] {
                        if self
                            .non_unicode_overrides
                            .iter()
                            .any(|path| match path.as_slice() {
                                [_, selected_kind, selected_id, selected_field] => {
                                    selected_kind == kind
                                        && selected_id.eq_ignore_ascii_case(id)
                                        && selected_field == field
                                }
                                _ => false,
                            })
                        {
                            return Err(failure("settings", id, "non-Unicode store override"));
                        }
                        if self.env.store_setting(kind, id, field).is_some_and(|raw| {
                            raw.is_empty()
                                || raw.chars().all(char::is_whitespace)
                                || raw.chars().any(char::is_control)
                        }) {
                            return Err(failure("settings", id, "invalid store NAME/KEY override"));
                        }
                    }
                }
            }
        }
        for (meta, root, label) in [
            (metadata.config, self.config_dir.as_deref(), "CONFIG_DIR"),
            (metadata.kv, self.data_dir.as_deref(), "DATA_DIR"),
        ] {
            if meta.is_some() && root.is_none() {
                return Err(failure("settings", label, "required for declared stores"));
            }
            if let Some(directory) = root {
                validate_root(directory, label)?;
            }
        }
        Ok(())
    }

    /// Selects an existing absolute config root; never creates it.
    ///
    /// # Errors
    /// Rejects relative, missing or non-directory roots.
    #[inline]
    pub fn with_config_dir<Root>(mut self, root: Root) -> anyhow::Result<Self>
    where
        Root: Into<PathBuf>,
    {
        let directory = root.into();
        validate_root(&directory, "CONFIG_DIR")?;
        self.config_dir = Some(directory);
        Ok(self)
    }

    /// Selects an existing absolute data root. Startup also checks actual write access.
    ///
    /// # Errors
    /// Rejects relative, missing or non-directory roots.
    #[inline]
    pub fn with_data_dir<Root>(mut self, root: Root) -> anyhow::Result<Self>
    where
        Root: Into<PathBuf>,
    {
        let directory = root.into();
        validate_root(&directory, "DATA_DIR")?;
        self.data_dir = Some(directory);
        Ok(self)
    }

    #[inline]
    #[must_use]
    pub fn with_logging_level(mut self, level: LevelFilter) -> Self {
        self.level = level;
        self
    }

    /// Selects a positive, representable integer-second drain budget.
    ///
    /// # Errors
    /// Rejects zero, fractional seconds and unusable native timer values.
    #[inline]
    pub fn with_shutdown_grace(mut self, grace: Duration) -> anyhow::Result<Self> {
        validate_grace(grace)?;
        self.grace = grace;
        Ok(self)
    }
}

pub(crate) fn failure(stage: &str, subject: &str, category: &str) -> anyhow::Error {
    // No source chain survives this boundary. Subject is a fixed setting name or
    // baked logical id, never settings/config/secret contents or a provider string.
    anyhow::anyhow!("startup {stage}: {subject}: {category}")
}

// Match EnvConfig's process-key identity without a second public configuration API.
pub(crate) fn config_path(key: &str, windows: bool) -> Option<Vec<String>> {
    let prefix = "EDGEZERO__";
    let supplied_prefix = key.get(..prefix.len())?;
    if supplied_prefix != prefix && !(windows && supplied_prefix.eq_ignore_ascii_case(prefix)) {
        return None;
    }
    let path: Vec<String> = key
        .get(prefix.len()..)?
        .split("__")
        .map(str::to_ascii_lowercase)
        .collect();
    (!path.iter().any(String::is_empty)).then_some(path)
}

fn fixed_setting(path: &[String]) -> Option<&'static str> {
    let segments: Vec<&str> = path.iter().map(String::as_str).collect();
    match segments.as_slice() {
        ["adapter", "host"] => Some("HOST"),
        ["adapter", "port"] => Some("PORT"),
        ["adapter", "config_dir"] => Some("CONFIG_DIR"),
        ["adapter", "data_dir"] => Some("DATA_DIR"),
        ["adapter", "shutdown_grace_seconds"] => Some("SHUTDOWN_GRACE_SECONDS"),
        ["logging", "level"] => Some("LOGGING__LEVEL"),
        _ => native_limit_setting(path),
    }
}

pub(crate) fn native_limit_setting(path: &[String]) -> Option<&'static str> {
    let segments: Vec<&str> = path.iter().map(String::as_str).collect();
    match segments.as_slice() {
        [
            "adapter",
            "ingress",
            "max_connections" | "max_raw_head_bytes" | "max_header_count" | "head_read_timeout_ms",
        ] => Some("INGRESS"),
        [
            "adapter",
            "config_store",
            "max_file_bytes" | "max_entries" | "max_key_bytes" | "max_value_bytes"
            | "max_resident_bytes" | "max_startup_bytes",
        ] => Some("CONFIG_STORE"),
        _ => None,
    }
}

pub(crate) fn shutdown_grace(env: &EnvConfig) -> anyhow::Result<Duration> {
    let Some(raw) = env.get(&["adapter", "shutdown_grace_seconds"]) else {
        return Ok(Duration::from_secs(10));
    };
    let seconds = raw
        .parse::<u64>()
        .ok()
        .filter(|_seconds| raw.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| {
            failure(
                "settings",
                "SHUTDOWN_GRACE_SECONDS",
                "invalid positive integer seconds",
            )
        })?;
    let grace = Duration::from_secs(seconds);
    validate_grace(grace)?;
    Ok(grace)
}

fn validate_grace(grace: Duration) -> anyhow::Result<()> {
    if grace.is_zero() || grace.subsec_nanos() != 0 || Instant::now().checked_add(grace).is_none() {
        return Err(failure(
            "settings",
            "SHUTDOWN_GRACE_SECONDS",
            "invalid or unusable positive integer seconds",
        ));
    }
    Ok(())
}

fn validate_root(root: &Path, setting: &str) -> anyhow::Result<()> {
    if !root.is_absolute() || !root.is_dir() {
        return Err(failure(
            "settings",
            setting,
            "requires an absolute existing directory",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::app::StoreMetadata;

    #[test]
    fn defaults_and_invalid_supplied_scalars_are_distinct() {
        let options = AxumRunOptions::from_vars(Vec::<(String, String)>::new()).expect("defaults");
        assert_eq!(
            options.addr,
            SocketAddr::from((addr::DEFAULT_HOST, addr::DEFAULT_PORT))
        );
        assert_eq!(options.grace, Duration::from_secs(10));
        for (key, values) in [
            (
                "EDGEZERO__ADAPTER__HOST",
                vec!["sentinel-host", "", "localhost"],
            ),
            (
                "EDGEZERO__ADAPTER__PORT",
                vec!["0", "65536", "sentinel-port", ""],
            ),
            ("EDGEZERO__LOGGING__LEVEL", vec!["sentinel-level", ""]),
            (
                "EDGEZERO__ADAPTER__SHUTDOWN_GRACE_SECONDS",
                vec![
                    "0",
                    "",
                    "-1",
                    "+1",
                    "sentinel-duration",
                    "18446744073709551615",
                ],
            ),
        ] {
            for value in values {
                let error = AxumRunOptions::from_vars([(key, value)])
                    .err()
                    .expect("invalid");
                assert_eq!(error.chain().count(), 1);
                assert!(!format!("{error:?}").contains("sentinel"));
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn binary_owned_limits_fail_capture_without_disclosing_values() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt as _;

        for path in [
            "ADAPTER__INGRESS__MAX_CONNECTIONS",
            "ADAPTER__INGRESS__MAX_RAW_HEAD_BYTES",
            "ADAPTER__INGRESS__MAX_HEADER_COUNT",
            "ADAPTER__INGRESS__HEAD_READ_TIMEOUT_MS",
            "ADAPTER__CONFIG_STORE__MAX_FILE_BYTES",
            "ADAPTER__CONFIG_STORE__MAX_ENTRIES",
            "ADAPTER__CONFIG_STORE__MAX_KEY_BYTES",
            "ADAPTER__CONFIG_STORE__MAX_VALUE_BYTES",
            "ADAPTER__CONFIG_STORE__MAX_RESIDENT_BYTES",
            "ADAPTER__CONFIG_STORE__MAX_STARTUP_BYTES",
        ] {
            let result = AxumRunOptions::from_os_vars(
                [(
                    OsString::from(format!("EDGEZERO__{path}")),
                    OsString::from_vec(b"PRIVATE_SENTINEL\xff".to_vec()),
                )],
                false,
            );
            let error = result.err().expect("supplied binary owned limit must fail");
            assert_eq!(error.chain().count(), 1);
            assert!(!format!("{error:?}").contains("PRIVATE_SENTINEL"));
            assert!(error.to_string().contains(if path.contains("INGRESS") {
                "INGRESS"
            } else {
                "CONFIG_STORE"
            }));
        }
        AxumRunOptions::from_os_vars(
            [(
                OsString::from("EDGEZERO__ADAPTER__UNKNOWN"),
                OsString::from_vec(vec![0xff]),
            )],
            false,
        )
        .expect("unknown binary setting remains ignored");
    }

    #[test]
    fn os_capture_preserves_platform_prefix_identity() {
        use std::ffi::OsString;

        let pair = || {
            (
                OsString::from("edgezero__adapter__port"),
                OsString::from("4321"),
            )
        };
        let unix = AxumRunOptions::from_os_vars([pair()], false).expect("Unix capture");
        assert_eq!(unix.addr.port(), addr::DEFAULT_PORT);
        let windows = AxumRunOptions::from_os_vars([pair()], true).expect("Windows capture");
        assert_eq!(windows.addr.port(), 4321);
    }

    #[test]
    fn selected_override_validation_precedes_normalization() {
        let metadata = StoresMetadata {
            config: Some(StoreMetadata {
                ids: &["settings"],
                default: "settings",
            }),
            ..StoresMetadata::default()
        };
        let root = tempfile::tempdir().expect("root");
        for value in ["", "   ", "sentinel\n"] {
            let options = AxumRunOptions::from_vars([
                (
                    "EDGEZERO__ADAPTER__CONFIG_DIR",
                    root.path().to_str().expect("path"),
                ),
                ("EDGEZERO__STORES__CONFIG__SETTINGS__KEY", value),
                ("EDGEZERO__STORES__KV__UNDECLARED__NAME", "\n"),
            ])
            .expect("native settings");
            let error = options
                .validate_stores(metadata)
                .expect_err("declared invalid override");
            assert!(!format!("{error:?}").contains("sentinel"));
        }
        let options =
            AxumRunOptions::new(SocketAddr::from(([127, 0, 0, 1], 1234))).expect("explicit");
        assert!(options.env.adapter_host().is_none());
        assert_eq!(options.env.store_key("config", "settings"), "settings");
        assert!(options.validate_stores(metadata).is_err());
    }
}

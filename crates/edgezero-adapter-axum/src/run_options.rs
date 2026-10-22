use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::env::{var_os, vars_os};
use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::time::{Duration, Instant};

use crate::diagnostics::NativeDiagnosticsHandle;
use crate::geo::{NativeGeo, NativeGeoCapability, NativeGeoConfigError, NativeGeoSelection};
use crate::proxy::TrustedProxyPolicy;
use edgezero_core::addr;
use edgezero_core::app::StoresMetadata;
use edgezero_core::env_config::EnvConfig;
use edgezero_store_aws::settings::{AgentAccessToken, AwsPreparationLimits};
use edgezero_store_celld::settings::{CelldAccessToken, CelldPreparationLimits};
use log::LevelFilter;

use crate::native_bindings::{
    NativeBindingsLimits, NativeConfigBinding, NativeKvBinding, NativePreparationPolicySources,
    NativeStoreBindings, NativeStoreBindingsInput,
};

/// Validated explicit production hosting options. Construction never selects a
/// policy from build profile, cwd or container detection. Explicit constructors
/// do not merge environment values; only `from_env` reads hosting environment.
#[derive(Clone)]
pub struct AxumRunOptions {
    addr: SocketAddr,
    aws_limits: Option<AwsPreparationLimits>,
    bindings: NativeStoreBindingsInput,
    bindings_limits: NativeBindingsLimits,
    bootstrap_values: BTreeMap<String, BootstrapValue>,
    celld_limits: Option<CelldPreparationLimits>,
    config_dir: Option<PathBuf>,
    data_dir: Option<PathBuf>,
    diagnostics: NativeDiagnosticsHandle,
    env: EnvConfig,
    geo: NativeGeoSelection,
    grace: Duration,
    level: LevelFilter,
    non_unicode_overrides: Vec<Vec<String>>,
    request_records: bool,
    trusted_proxy_policy: TrustedProxyPolicy,
}

#[derive(Clone)]
enum BootstrapValue {
    Ambiguous,
    Value(String),
}

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "scalar validation precedes local-root validation; compatibility constructors remain grouped"
)]
impl AxumRunOptions {
    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) fn bindings(&self) -> &NativeStoreBindingsInput {
        &self.bindings
    }

    pub(crate) fn bindings_limits(&self) -> NativeBindingsLimits {
        self.bindings_limits
    }

    pub(crate) fn aws_limits(&self) -> Option<AwsPreparationLimits> {
        self.aws_limits
    }

    pub(crate) fn celld_limits(&self) -> Option<CelldPreparationLimits> {
        self.celld_limits
    }

    pub(crate) fn preparation_policies(&self) -> NativePreparationPolicySources {
        NativePreparationPolicySources {
            aws: self.aws_limits(),
            celld: self.celld_limits(),
        }
    }

    pub(crate) fn celld_token(&self, name: &str) -> anyhow::Result<CelldAccessToken> {
        CelldAccessToken::new(self.token_value(name, "celld token")?)
            .map_err(|_category| failure("settings", "celld token", "invalid configured value"))
    }

    pub(crate) fn agent_token(&self, name: &str) -> anyhow::Result<AgentAccessToken> {
        AgentAccessToken::new(self.token_value(name, "Agent token")?)
            .map_err(|_category| failure("settings", "Agent token", "invalid configured value"))
    }

    fn token_value(&self, name: &str, subject: &str) -> anyhow::Result<String> {
        let value = match self.bootstrap_values.get(name) {
            Some(BootstrapValue::Value(value)) => value.clone(),
            Some(BootstrapValue::Ambiguous) => {
                return Err(failure("settings", subject, "duplicate configured value"));
            }
            None => {
                return Err(failure("settings", subject, "configured value absent"));
            }
        };
        Ok(value)
    }

    pub(crate) fn config_dir(&self) -> Option<&Path> {
        self.config_dir.as_deref()
    }

    pub(crate) fn data_dir(&self) -> Option<&Path> {
        self.data_dir.as_deref()
    }

    pub(crate) fn diagnostics(&self) -> NativeDiagnosticsHandle {
        self.diagnostics.clone()
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
        let mut pairs = Vec::new();
        let mut non_unicode = Vec::new();
        for (os_key, os_value) in vars_os() {
            let Some(key) = os_key.to_str() else { continue };
            let Some(rest) = strip_native_config_prefix(key) else {
                continue;
            };
            let path: Vec<String> = rest.split("__").map(str::to_ascii_lowercase).collect();
            if let Some(value) = os_value.to_str() {
                pairs.push((key.to_owned(), value.to_owned()));
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
        // Only this env-loading boundary may resolve named token variables.
        // Explicit constructors and selection methods never call var_os.
        options.capture_agent_tokens(|name| var_os(name))?;
        Ok(options)
    }

    fn capture_agent_tokens<Lookup>(&mut self, mut lookup: Lookup) -> anyhow::Result<()>
    where
        Lookup: FnMut(&str) -> Option<OsString>,
    {
        // Capture shared names once; validate them against every selected token type.
        let mut names = BTreeMap::<String, (bool, bool)>::new();
        if let Some(document) = self.bindings.document() {
            for binding in document.config().values() {
                match binding {
                    NativeConfigBinding::AppConfigAgent(settings) => {
                        if let Some(name) = &settings.access_token_env {
                            names.entry(name.clone()).or_default().0 = true;
                        }
                    }
                    NativeConfigBinding::Celld(settings) => {
                        if let Some(name) = settings.access.token_env() {
                            names.entry(name.to_owned()).or_default().1 = true;
                        }
                    }
                }
            }
            for binding in document.kv().values() {
                if let NativeKvBinding::Celld(settings) = binding
                    && let Some(name) = settings.access.token_env()
                {
                    names.entry(name.to_owned()).or_default().1 = true;
                }
            }
        }
        for (name, (aws, celld)) in names {
            let subject = if celld { "celld token" } else { "Agent token" };
            let value = lookup(&name)
                .and_then(|value| value.into_string().ok())
                .ok_or_else(|| {
                    failure(
                        "settings",
                        subject,
                        "configured value absent or non-Unicode",
                    )
                })?;
            if aws {
                AgentAccessToken::new(value.clone()).map_err(|_category| {
                    failure("settings", "Agent token", "invalid configured value")
                })?;
            }
            if celld {
                CelldAccessToken::new(value.clone()).map_err(|_category| {
                    failure("settings", "celld token", "invalid configured value")
                })?;
            }
            self.bootstrap_values
                .insert(name, BootstrapValue::Value(value));
        }
        Ok(())
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
        let pairs: Vec<(String, String)> = vars
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_owned(), value.into()))
            .collect();
        let mut seen = BTreeSet::new();
        for (key, _value) in &pairs {
            if let Some(rest) = strip_native_config_prefix(key) {
                let path: Vec<String> = rest.split("__").map(str::to_ascii_lowercase).collect();
                let segments: Vec<&str> = path.iter().map(String::as_str).collect();
                if (fixed_setting(&path).is_some()
                    || matches!(
                        segments.as_slice(),
                        ["stores", "config" | "kv" | "secrets", _, "name" | "key"]
                    ))
                    && !seen.insert(path)
                {
                    return Err(failure(
                        "settings",
                        "bootstrap",
                        "duplicate normalized setting",
                    ));
                }
            }
        }
        let env = EnvConfig::from_vars(pairs.iter().map(|(key, value)| (key, value.clone())));
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
        (options.trusted_proxy_policy, options.request_records) = native_settings(&env)?;
        if let Some(root) = env.get(&["adapter", "config_dir"]) {
            options = options.with_config_dir(root)?;
        }
        if let Some(root) = env.get(&["adapter", "data_dir"]) {
            options = options.with_data_dir(root)?;
        }
        options.env = env;
        for (key, value) in pairs {
            match options.bootstrap_values.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(BootstrapValue::Value(value));
                }
                Entry::Occupied(mut entry) => {
                    entry.insert(BootstrapValue::Ambiguous);
                }
            }
        }
        if let Some(path) = options.env.get(&["store_bindings_file"]).map(str::to_owned) {
            options = options.with_store_bindings_file(path)?;
        }
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
            diagnostics: NativeDiagnosticsHandle::new(),
            trusted_proxy_policy: TrustedProxyPolicy::no_trust(),
            request_records: true,
            geo: NativeGeoSelection::Off,
            bindings: NativeStoreBindingsInput::LocalDefaults,
            bindings_limits: NativeBindingsLimits::default(),
            aws_limits: None,
            bootstrap_values: BTreeMap::new(),
            celld_limits: None,
            addr,
            config_dir: None,
            data_dir: None,
            env: EnvConfig::default(),
            grace: Duration::from_secs(10),
            level: LevelFilter::Info,
            non_unicode_overrides: Vec::new(),
        })
    }

    /// Select geographic resolution explicitly. Validation happens before runner resources.
    #[must_use]
    #[inline]
    pub fn with_geo(mut self, selection: NativeGeoSelection) -> Self {
        self.geo = selection;
        self
    }

    /// Validate the finalized selection and capture its startup capability.
    ///
    /// # Errors
    /// Rejects unavailable implementations and invalid protected-source policies.
    #[inline]
    pub fn geo_capability(&self) -> Result<NativeGeoCapability, NativeGeoConfigError> {
        self.native_geo().map(|geo| geo.capability())
    }

    pub(crate) fn native_geo(&self) -> Result<NativeGeo, NativeGeoConfigError> {
        NativeGeo::validate(&self.geo)
    }

    /// Shares native counters and the optional request observer.
    #[must_use]
    #[inline]
    pub fn with_diagnostics(mut self, diagnostics: NativeDiagnosticsHandle) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    pub(crate) fn request_records(&self) -> bool {
        self.request_records
    }
    pub(crate) fn trusted_proxy_policy(&self) -> TrustedProxyPolicy {
        self.trusted_proxy_policy.clone()
    }

    pub(crate) fn shutdown_grace(&self) -> Duration {
        self.grace
    }

    #[cfg(test)]
    fn validate_stores(&self, metadata: StoresMetadata) -> anyhow::Result<()> {
        self.validate_store_overlays(metadata)?;
        self.validate_local_roots(metadata.config.is_some(), metadata.kv.is_some())
    }

    pub(crate) fn validate_store_overlays(&self, metadata: StoresMetadata) -> anyhow::Result<()> {
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
        Ok(())
    }

    pub(crate) fn validate_local_roots(&self, config: bool, kv: bool) -> anyhow::Result<()> {
        for (required, root, label) in [
            (config, self.config_dir.as_deref(), "CONFIG_DIR"),
            (kv, self.data_dir.as_deref(), "DATA_DIR"),
        ] {
            if required && root.is_none() {
                return Err(failure("settings", label, "required for declared stores"));
            }
            if let Some(directory) = root {
                validate_root(directory, label)?;
            }
        }
        Ok(())
    }

    /// Selects typed bindings. A second explicit selection is an error.
    ///
    /// # Errors
    /// Rejects duplicate selection and invalid settings/policy sources.
    #[inline]
    pub fn with_store_bindings(mut self, bindings: NativeStoreBindings) -> anyhow::Result<Self> {
        if !matches!(self.bindings, NativeStoreBindingsInput::LocalDefaults) {
            return Err(failure("settings", "bindings", "duplicate selection"));
        }
        bindings
            .validate_with_policies(self.bindings_limits, self.preparation_policies())
            .map_err(|category| failure("settings", "bindings", &category.to_string()))?;
        self.bindings = NativeStoreBindingsInput::Inline(bindings);
        Ok(self)
    }

    /// Captures one absolute bindings file once. Explicit selection does not read environment values.
    ///
    /// # Errors
    /// Rejects duplicate/relative/missing/malformed/oversized selections.
    #[inline]
    pub fn with_store_bindings_file<Root: Into<PathBuf>>(
        mut self,
        locator: Root,
    ) -> anyhow::Result<Self> {
        if !matches!(self.bindings, NativeStoreBindingsInput::LocalDefaults) {
            return Err(failure("settings", "bindings", "duplicate selection"));
        }
        let path = locator.into();
        let bindings = NativeStoreBindings::read_with_policies(
            &path,
            self.bindings_limits,
            self.preparation_policies(),
        )
        .map_err(|category| failure("settings", "bindings", &category.to_string()))?;
        self.bindings = NativeStoreBindingsInput::File { path, bindings };
        Ok(self)
    }

    /// Supplies named bootstrap values explicitly, never merging ambient variables.
    ///
    /// # Errors
    /// Rejects duplicate variable inputs rather than selecting precedence.
    #[inline]
    pub fn with_bootstrap_values<I, K, V>(mut self, values: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (name, value) in values {
            if self
                .bootstrap_values
                .insert(name.into(), BootstrapValue::Value(value.into()))
                .is_some()
            {
                return Err(failure("settings", "bootstrap", "duplicate variable"));
            }
        }
        Ok(self)
    }

    /// Selects trusted file/ID bounds before selecting a document.
    ///
    /// # Errors
    /// Rejects invalid bounds or policy changes after a file/inline selection.
    #[inline]
    pub fn with_native_bindings_limits(
        mut self,
        limits: NativeBindingsLimits,
    ) -> anyhow::Result<Self> {
        if !matches!(self.bindings, NativeStoreBindingsInput::LocalDefaults) {
            return Err(failure(
                "settings",
                "bindings",
                "limits must precede selection",
            ));
        }
        limits
            .validate()
            .map_err(|category| failure("settings", "bindings", &category.to_string()))?;
        self.bindings_limits = limits;
        Ok(self)
    }

    /// Selects one explicit remote preparation policy, not an override/merge of file policies.
    ///
    /// # Errors
    /// Rejects duplicate policies, competing document policies and invalid limits.
    #[inline]
    pub fn with_aws_preparation_limits(
        mut self,
        limits: AwsPreparationLimits,
    ) -> anyhow::Result<Self> {
        if self.aws_limits.is_some() {
            return Err(failure("settings", "AWS limits", "duplicate policy"));
        }
        limits
            .validate()
            .map_err(|category| failure("settings", "AWS limits", &category.to_string()))?;
        if let Some(document) = self.bindings.document() {
            document
                .validate_with_policies(
                    self.bindings_limits,
                    NativePreparationPolicySources {
                        aws: Some(limits),
                        celld: self.celld_limits,
                    },
                )
                .map_err(|category| failure("settings", "AWS limits", &category.to_string()))?;
        }
        self.aws_limits = Some(limits);
        Ok(self)
    }

    /// Selects a celld policy independently of any AWS policy.
    ///
    /// # Errors
    /// Rejects duplicate policies, same-provider document conflicts and invalid limits.
    #[inline]
    pub fn with_celld_preparation_limits(
        mut self,
        limits: CelldPreparationLimits,
    ) -> anyhow::Result<Self> {
        if self.celld_limits.is_some() {
            return Err(failure("settings", "celld limits", "duplicate policy"));
        }
        limits
            .validate()
            .map_err(|category| failure("settings", "celld limits", &category.to_string()))?;
        if let Some(document) = self.bindings.document() {
            document
                .validate_with_policies(
                    self.bindings_limits,
                    NativePreparationPolicySources {
                        aws: self.aws_limits,
                        celld: Some(limits),
                    },
                )
                .map_err(|category| failure("settings", "celld limits", &category.to_string()))?;
        }
        self.celld_limits = Some(limits);
        Ok(self)
    }

    /// Selects an existing absolute config root; never creates it.
    ///
    /// # Errors
    /// Rejects relative, missing or non-directory roots.
    #[inline]
    pub fn with_config_dir<Root: Into<PathBuf>>(mut self, root: Root) -> anyhow::Result<Self> {
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
    pub fn with_data_dir<Root: Into<PathBuf>>(mut self, root: Root) -> anyhow::Result<Self> {
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

    /// Enables only default native request logs; observers and accounting remain active.
    #[inline]
    #[must_use]
    pub fn with_request_records(mut self, enabled: bool) -> Self {
        self.request_records = enabled;
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

    /// Installs an explicit immutable incoming-proxy policy without reading the environment.
    #[inline]
    #[must_use]
    pub fn with_trusted_proxy_policy(mut self, policy: TrustedProxyPolicy) -> Self {
        self.trusted_proxy_policy = policy;
        self
    }
}

pub(crate) fn native_settings(env: &EnvConfig) -> anyhow::Result<(TrustedProxyPolicy, bool)> {
    let policy = TrustedProxyPolicy::from_values(
        env.get(&["adapter", "trusted_proxy_cidrs"]),
        env.get(&["adapter", "forwarding_header_family"]),
    )
    .map_err(|_error| {
        failure(
            "settings",
            "TRUSTED_PROXY_POLICY",
            "invalid literal networks or forwarding family",
        )
    })?;
    let records = match env.get(&["logging", "request_records"]) {
        None | Some("true") => true,
        Some("false") => false,
        Some(_) => {
            return Err(failure(
                "settings",
                "LOGGING__REQUEST_RECORDS",
                "invalid boolean",
            ));
        }
    };
    Ok((policy, records))
}

pub(crate) fn failure(stage: &str, subject: &str, category: &str) -> anyhow::Error {
    // No source chain survives this boundary. Subject is a fixed setting name or
    // baked logical id, never settings/config/secret contents or a provider string.
    anyhow::anyhow!("startup {stage}: {subject}: {category}")
}

fn fixed_setting(path: &[String]) -> Option<&'static str> {
    let segments: Vec<&str> = path.iter().map(String::as_str).collect();
    match segments.as_slice() {
        ["adapter", "host"] => Some("HOST"),
        ["adapter", "port"] => Some("PORT"),
        ["adapter", "config_dir"] => Some("CONFIG_DIR"),
        ["adapter", "data_dir"] => Some("DATA_DIR"),
        ["adapter", "shutdown_grace_seconds"] => Some("SHUTDOWN_GRACE_SECONDS"),
        ["adapter", "trusted_proxy_cidrs"] => Some("TRUSTED_PROXY_CIDRS"),
        ["adapter", "forwarding_header_family"] => Some("FORWARDING_HEADER_FAMILY"),
        ["logging", "request_records"] => Some("LOGGING__REQUEST_RECORDS"),
        ["logging", "level"] => Some("LOGGING__LEVEL"),
        ["store_bindings_file"] => Some("STORE_BINDINGS_FILE"),
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

/// Match the process environment's key identity, as `EnvConfig` does.
pub(crate) fn strip_native_config_prefix(key: &str) -> Option<&str> {
    const PREFIX: &str = "EDGEZERO__";
    if cfg!(windows) {
        let prefix = key.get(..PREFIX.len())?;
        if !prefix.eq_ignore_ascii_case(PREFIX) {
            return None;
        }
        key.get(PREFIX.len()..)
    } else {
        key.strip_prefix(PREFIX)
    }
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
    use std::fs;

    use super::*;
    use edgezero_core::app::StoreMetadata;

    const BINDINGS: &str = "version = 1\n[config.app]\nprovider = 'aws-appconfig-agent'\ndefault_key = 'settings'\nendpoint = 'http://127.0.0.1:2772'\naccess_token_env = 'AGENT_TOKEN'\n[config.app.documents.settings]\napplication = 'example'\nenvironment = 'production'\nprofile = 'envelope'\n";

    const CELLD: &str = "version=1\n[config.remote]\nprovider='celld-config'\nendpoint='https://unreachable.invalid'\ndefault_key='settings'\n[config.remote.access]\nmode='authenticated'\ntoken_env='CELL_TOKEN'\n[config.remote.documents.settings]\nstore='config-fixture'\nkey='settings'\n[kv.state]\nprovider='celld-kv'\nendpoint='https://unreachable.invalid'\nstore='state-fixture'\n[kv.state.access]\nmode='authenticated'\ntoken_env='CELL_TOKEN'\n";
    const CELL_TOKEN: &str = "celld-bootstrap-value-01234567890123456789";

    #[test]
    fn geo_selection_defaults_and_whole_replacement_preserve_native_option_bounds() {
        use crate::geo::ProtectedCloudFrontAlb;
        let options = AxumRunOptions::new("127.0.0.1:8787".parse().unwrap()).unwrap();
        let _thread_safe_clone: Box<dyn Send + Sync> = Box::new(options.clone());
        assert!(matches!(
            options.geo_capability(),
            Ok(NativeGeoCapability::Off)
        ));
        let selected = options.with_geo(NativeGeoSelection::CloudFront {
            policy: ProtectedCloudFrontAlb::new(["127.0.0.1/32"], "x-origin-verify-fixture")
                .unwrap(),
        });
        let replaced = selected.with_geo(NativeGeoSelection::Off);
        assert!(matches!(
            replaced.geo_capability(),
            Ok(NativeGeoCapability::Off)
        ));
    }

    #[test]
    fn native_settings_defaults_explicit_builders_and_strict_values() {
        let defaults = AxumRunOptions::from_vars(Vec::<(String, String)>::new()).expect("defaults");
        assert!(defaults.request_records());
        assert_eq!(
            defaults.trusted_proxy_policy(),
            TrustedProxyPolicy::no_trust()
        );
        let configured = AxumRunOptions::from_vars([
            ("EDGEZERO__ADAPTER__TRUSTED_PROXY_CIDRS", "127.0.0.2/32"),
            ("EDGEZERO__ADAPTER__FORWARDING_HEADER_FAMILY", "forwarded"),
            ("EDGEZERO__LOGGING__REQUEST_RECORDS", "false"),
        ])
        .expect("configured");
        assert!(!configured.request_records());
        assert_ne!(
            configured.trusted_proxy_policy(),
            TrustedProxyPolicy::no_trust()
        );
        let explicit = AxumRunOptions::new(defaults.addr())
            .expect("explicit")
            .with_request_records(false)
            .with_trusted_proxy_policy(configured.trusted_proxy_policy());
        assert!(!explicit.request_records());
        assert_eq!(
            explicit.trusted_proxy_policy(),
            configured.trusted_proxy_policy()
        );
        assert!(
            explicit
                .env_config()
                .get(&["adapter", "trusted_proxy_cidrs"])
                .is_none()
        );
        for (key, value) in [
            ("EDGEZERO__ADAPTER__TRUSTED_PROXY_CIDRS", "127.0.0.2"),
            ("EDGEZERO__ADAPTER__TRUSTED_PROXY_CIDRS", "sentinel-policy"),
            (
                "EDGEZERO__ADAPTER__FORWARDING_HEADER_FAMILY",
                "sentinel-family",
            ),
            ("EDGEZERO__LOGGING__REQUEST_RECORDS", "TRUE"),
            ("EDGEZERO__LOGGING__REQUEST_RECORDS", "false "),
            ("EDGEZERO__LOGGING__REQUEST_RECORDS", ""),
        ] {
            let error = AxumRunOptions::from_vars([(key, value)])
                .err()
                .expect("invalid supplied setting");
            assert_eq!(error.chain().count(), 1);
            assert!(!error.to_string().contains("sentinel"));
        }
    }

    fn explicit() -> AxumRunOptions {
        AxumRunOptions::new(SocketAddr::from(([127, 0, 0, 1], 1234))).expect("explicit options")
    }

    #[test]
    fn celld_capture_unions_config_and_kv_names_and_validates_shared_aws_use() {
        for (input, expected) in [
            (CELLD.to_owned(), 1),
            (
                CELLD.replace(
                    "[kv.state.access]\nmode='authenticated'\ntoken_env='CELL_TOKEN'",
                    "[kv.state.access]\nmode='authenticated'\ntoken_env='OTHER_TOKEN'",
                ),
                2,
            ),
            (
                format!(
                    "{BINDINGS}{}",
                    CELLD
                        .replace("version=1\n", "")
                        .replace("CELL_TOKEN", "AGENT_TOKEN")
                ),
                1,
            ),
        ] {
            let document = NativeStoreBindings::parse(&input, NativeBindingsLimits::default())
                .expect("bindings");
            let mut options = explicit().with_store_bindings(document).expect("inline");
            let mut calls = 0_usize;
            options
                .capture_agent_tokens(|_name| {
                    calls += 1;
                    Some(OsString::from(CELL_TOKEN))
                })
                .expect("captured union");
            assert_eq!(calls, expected);
            let name = if input.contains("AGENT_TOKEN") {
                "AGENT_TOKEN"
            } else {
                "CELL_TOKEN"
            };
            assert_eq!(
                options.celld_token(name).expect("celld token").expose(),
                CELL_TOKEN
            );
            if name == "AGENT_TOKEN" {
                assert_eq!(
                    options.agent_token(name).expect("AWS token").expose(),
                    CELL_TOKEN
                );
            }
        }
        let shared = format!(
            "{BINDINGS}{}",
            CELLD
                .replace("version=1\n", "")
                .replace("CELL_TOKEN", "AGENT_TOKEN")
        );
        let document =
            NativeStoreBindings::parse(&shared, NativeBindingsLimits::default()).expect("bindings");
        let mut options = explicit().with_store_bindings(document).expect("inline");
        // Valid AWS token, invalid celld token: shared names must not skip the stricter type.
        let error = options
            .capture_agent_tokens(|_name| Some(OsString::from("short")))
            .expect_err("celld minimum");
        assert_eq!(error.chain().count(), 1);
        assert!(error.to_string().contains("celld token"));
    }

    #[test]
    fn celld_bootstrap_is_explicit_closed_and_trusted_network_looks_up_nothing() {
        let document =
            NativeStoreBindings::parse(CELLD, NativeBindingsLimits::default()).expect("bindings");
        let options = explicit().with_store_bindings(document).expect("inline");
        options
            .celld_token("CELL_TOKEN")
            .expect_err("explicit token required");
        for raw in [
            None,
            Some(OsString::new()),
            Some(OsString::from("short")),
            Some(OsString::from("invalid token sentinel")),
        ] {
            let error = options
                .clone()
                .capture_agent_tokens(|_name| raw.clone())
                .expect_err("invalid token");
            assert_eq!(error.chain().count(), 1);
            assert!(!format!("{error:?}").contains("sentinel"));
        }
        let supplied = options
            .with_bootstrap_values([("CELL_TOKEN", CELL_TOKEN)])
            .expect("explicit token");
        assert_eq!(
            supplied.celld_token("CELL_TOKEN").expect("token").expose(),
            CELL_TOKEN
        );
        assert!(!format!("{:?}", supplied.celld_token("CELL_TOKEN")).contains(CELL_TOKEN));
        let duplicate =
            AxumRunOptions::from_vars([("CELL_TOKEN", CELL_TOKEN), ("CELL_TOKEN", CELL_TOKEN)])
                .expect("pairs");
        duplicate
            .celld_token("CELL_TOKEN")
            .expect_err("duplicate token values rejected");
        let trusted = CELLD
            .replace("https://unreachable.invalid", "http://127.0.0.1:8788")
            .replace(
                "mode='authenticated'\ntoken_env='CELL_TOKEN'",
                "mode='trusted-network'",
            );
        let mut trusted_options = explicit()
            .with_store_bindings(
                NativeStoreBindings::parse(&trusted, NativeBindingsLimits::default())
                    .expect("trusted bindings"),
            )
            .expect("inline");
        trusted_options
            .capture_agent_tokens(|_name| panic!("trusted network must not resolve tokens"))
            .expect("no lookup");
    }

    #[cfg(unix)]
    #[test]
    fn celld_non_unicode_capture_fails_closed() {
        use std::os::unix::ffi::OsStringExt as _;
        let document =
            NativeStoreBindings::parse(CELLD, NativeBindingsLimits::default()).expect("bindings");
        let mut options = explicit().with_store_bindings(document).expect("inline");
        assert!(
            options
                .capture_agent_tokens(|_name| Some(OsString::from_vec(vec![0xff])))
                .is_err()
        );
    }

    #[test]
    fn options_preserve_independent_policy_sources_in_both_orders() {
        let aws = AwsPreparationLimits {
            max_remote_values: 7,
            ..Default::default()
        };
        let celld = CelldPreparationLimits {
            max_remote_values: 9,
            ..Default::default()
        };
        for options in [
            explicit()
                .with_aws_preparation_limits(aws)
                .expect("AWS")
                .with_celld_preparation_limits(celld)
                .expect("celld"),
            explicit()
                .with_celld_preparation_limits(celld)
                .expect("celld")
                .with_aws_preparation_limits(aws)
                .expect("AWS"),
        ] {
            assert_eq!(
                options.aws_limits().expect("AWS policy").max_remote_values,
                7
            );
            assert_eq!(
                options
                    .celld_limits()
                    .expect("celld policy")
                    .max_remote_values,
                9
            );
            assert!(
                options
                    .clone()
                    .with_celld_preparation_limits(celld)
                    .is_err()
            );
            assert!(options.with_aws_preparation_limits(aws).is_err());
        }
        for (table, explicit_aws) in [
            ("[limits.celld]\nmax_remote_values=9", true),
            ("[limits.aws]\nmax_remote_values=7", false),
        ] {
            let document = NativeStoreBindings::parse(
                &format!("{CELLD}\n{table}"),
                NativeBindingsLimits::default(),
            )
            .expect("file policy");
            let selected = explicit()
                .with_store_bindings(document.clone())
                .expect("inline");
            if explicit_aws {
                selected
                    .with_aws_preparation_limits(aws)
                    .expect("cross-provider policy");
                explicit()
                    .with_aws_preparation_limits(aws)
                    .expect("AWS")
                    .with_store_bindings(document)
                    .expect("cross-provider file");
            } else {
                selected
                    .with_celld_preparation_limits(celld)
                    .expect("cross-provider policy");
                explicit()
                    .with_celld_preparation_limits(celld)
                    .expect("celld")
                    .with_store_bindings(document)
                    .expect("cross-provider file");
            }
        }
    }

    #[test]
    fn native_prefix_matching_agrees_with_portable_env_config() {
        for key in [
            "EDGEZERO__ADAPTER__PORT",
            "edgezero__adapter__port",
            "EdGeZeRo__adapter__port",
            "EDGEZERO_ADAPTER__PORT",
            "short",
            "EDGEZERO\u{e9}__adapter__port",
        ] {
            let portable = EnvConfig::from_vars([(key, "9090")]);
            assert_eq!(
                strip_native_config_prefix(key).is_some(),
                portable.adapter_port().is_some(),
                "prefix identity for {key}"
            );
        }
    }

    #[test]
    fn prefix_case_variants_capture_bindings_and_reject_duplicate_settings() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("bindings.toml");
        fs::write(&path, BINDINGS).expect("bindings");
        for prefix in ["EDGEZERO__", "edgezero__", "EdGeZeRo__"] {
            let options = AxumRunOptions::from_vars([(
                format!("{prefix}store_bindings_file"),
                path.to_str().expect("path"),
            )])
            .expect("valid locator or unrecognized Unix prefix");
            assert_eq!(
                options.bindings().document().is_some(),
                prefix == "EDGEZERO__" || cfg!(windows)
            );
            let duplicate = AxumRunOptions::from_vars([
                ("EDGEZERO__ADAPTER__PORT".to_owned(), "9090"),
                (format!("{prefix}adapter__port"), "9091"),
            ]);
            if prefix == "EDGEZERO__" || cfg!(windows) {
                let error = duplicate.err().expect("duplicate normalized setting");
                assert!(error.to_string().contains("duplicate normalized setting"));
            } else {
                assert_eq!(duplicate.expect("Unix prefix ignored").addr.port(), 9090);
            }
        }
    }

    #[test]
    fn file_selection_is_captured_once_and_conflicts_do_not_read_a_second_file() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("bindings.toml");
        fs::write(&path, BINDINGS).expect("write bindings");
        let options = explicit()
            .with_store_bindings_file(&path)
            .expect("selected file");
        fs::remove_file(&path).expect("remove file after capture");
        assert_eq!(
            options
                .bindings()
                .document()
                .expect("captured")
                .config()
                .len(),
            1
        );
        let document = NativeStoreBindings::parse(BINDINGS, NativeBindingsLimits::default())
            .expect("typed bindings");
        assert!(
            options
                .clone()
                .with_store_bindings(document)
                .err()
                .expect("duplicate")
                .to_string()
                .contains("duplicate")
        );
        assert!(
            options
                .with_store_bindings_file(path)
                .err()
                .expect("duplicate file")
                .to_string()
                .contains("duplicate")
        );
        assert!(
            explicit()
                .with_store_bindings_file("relative.toml")
                .is_err()
        );
    }

    #[test]
    fn explicit_bootstrap_values_do_not_consult_ambient_variables() {
        let document = NativeStoreBindings::parse(BINDINGS, NativeBindingsLimits::default())
            .expect("bindings");
        let unconfigured = explicit().with_store_bindings(document).expect("inline");
        let missing = unconfigured
            .agent_token("AGENT_TOKEN")
            .expect_err("missing token");
        assert_eq!(missing.chain().count(), 1);
        let options = unconfigured
            .with_bootstrap_values([("AGENT_TOKEN", "TOKEN_SENTINEL")])
            .expect("supplied token");
        assert_eq!(
            options
                .agent_token("AGENT_TOKEN")
                .expect("captured")
                .expose(),
            "TOKEN_SENTINEL"
        );
        assert!(!format!("{:?}", options.agent_token("AGENT_TOKEN")).contains("SENTINEL"));
        assert!(
            options
                .with_bootstrap_values([("AGENT_TOKEN", "OTHER_SENTINEL")])
                .is_err()
        );
    }

    #[test]
    fn explicit_vars_capture_locator_and_named_values_without_reopening() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("bindings.toml");
        fs::write(&path, BINDINGS).expect("write bindings");
        let options = AxumRunOptions::from_vars([
            (
                "EDGEZERO__STORE_BINDINGS_FILE",
                path.to_str().expect("path"),
            ),
            ("AGENT_TOKEN", "TOKEN_SENTINEL"),
        ])
        .expect("supplied variables");
        fs::remove_file(path).expect("remove");
        assert!(options.bindings().document().is_some());
        assert_eq!(
            options.agent_token("AGENT_TOKEN").expect("token").expose(),
            "TOKEN_SENTINEL"
        );
        for (first, second) in [
            (
                "EDGEZERO__STORE_BINDINGS_FILE",
                "EDGEZERO__store_bindings_file",
            ),
            (
                "EDGEZERO__STORES__CONFIG__APP__NAME",
                "EDGEZERO__stores__config__app__name",
            ),
            ("EDGEZERO__ADAPTER__PORT", "EDGEZERO__adapter__port"),
        ] {
            let error = AxumRunOptions::from_vars([(first, "SENTINEL"), (second, "SENTINEL")])
                .err()
                .expect("duplicate normalized path");
            assert!(error.to_string().contains("duplicate"));
            assert!(!format!("{error:?}").contains("SENTINEL"));
        }
    }

    #[test]
    fn duplicate_unrelated_vars_keep_existing_env_semantics_but_tokens_fail_closed() {
        let options = AxumRunOptions::from_vars([
            ("EDGEZERO__UNRELATED", "first"),
            ("EDGEZERO__UNRELATED", "second"),
            ("AGENT_TOKEN", "FIRST_SENTINEL"),
            ("AGENT_TOKEN", "SECOND_SENTINEL"),
        ])
        .expect("unrelated env keys retain existing semantics");
        assert_eq!(options.env.get(&["unrelated"]), Some("second"));
        let error = options
            .agent_token("AGENT_TOKEN")
            .expect_err("ambiguous token");
        assert!(error.to_string().contains("duplicate"));
        assert!(!format!("{error:?}").contains("SENTINEL"));
    }

    #[test]
    fn named_token_capture_is_once_per_variable_and_rejects_missing_empty() {
        let duplicate = format!(
            "{BINDINGS}\n{}",
            BINDINGS
                .replace("version = 1\n", "")
                .replace("config.app", "config.other")
        );
        let document = NativeStoreBindings::parse(&duplicate, NativeBindingsLimits::default())
            .expect("shared token bindings");
        let options = explicit().with_store_bindings(document).expect("inline");
        let mut captured = options.clone();
        let mut calls = 0_usize;
        captured
            .capture_agent_tokens(|name| {
                assert_eq!(name, "AGENT_TOKEN");
                calls += 1;
                Some(OsString::from("TOKEN_SENTINEL"))
            })
            .expect("capture");
        assert_eq!(calls, 1);
        for value in [None, Some(OsString::new())] {
            let error = options
                .clone()
                .capture_agent_tokens(|_name| value.clone())
                .expect_err("configured token invalid");
            assert!(!format!("{error:?}").contains("SENTINEL"));
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_configured_token_fails_closed() {
        use std::os::unix::ffi::OsStringExt as _;
        let document = NativeStoreBindings::parse(BINDINGS, NativeBindingsLimits::default())
            .expect("bindings");
        let mut options = explicit().with_store_bindings(document).expect("inline");
        let error = options
            .capture_agent_tokens(|_name| Some(OsString::from_vec(vec![0xff])))
            .expect_err("non-Unicode token");
        assert_eq!(error.chain().count(), 1);
        assert!(error.to_string().contains("non-Unicode"));
    }

    #[test]
    fn trusted_caps_and_remote_policy_sources_are_explicit() {
        let document = NativeStoreBindings::parse(BINDINGS, NativeBindingsLimits::default())
            .expect("bindings");
        let options = explicit().with_store_bindings(document).expect("inline");
        assert!(
            options
                .with_native_bindings_limits(NativeBindingsLimits::default())
                .is_err()
        );
        let with_policy = NativeStoreBindings::parse(
            &format!("{BINDINGS}\n[limits.aws]\nmax_snapshot_bytes = 16777216"),
            NativeBindingsLimits::default(),
        )
        .expect("policy");
        assert!(
            explicit()
                .with_aws_preparation_limits(AwsPreparationLimits::default())
                .expect("explicit policy")
                .with_store_bindings(with_policy.clone())
                .is_err()
        );
        assert!(
            explicit()
                .with_store_bindings(with_policy)
                .expect("file policy")
                .with_aws_preparation_limits(AwsPreparationLimits::default())
                .is_err()
        );
    }

    #[test]
    fn env_bootstrap_child() {
        use std::env;

        let Some(expected) = env::var_os("EDGEZERO_NATIVE_BOOTSTRAP_TEST") else {
            return;
        };
        if expected == "celld" {
            let options = AxumRunOptions::from_env().expect("celld env token capture");
            assert_eq!(
                options
                    .celld_token("CELL_TOKEN")
                    .expect("captured celld token")
                    .expose(),
                CELL_TOKEN
            );
            let path = env::var_os("EDGEZERO__STORE_BINDINGS_FILE").expect("locator");
            fs::write(
                PathBuf::from(path),
                CELLD.replace("CELL_TOKEN", "OTHER_TOKEN"),
            )
            .expect("change file after capture");
            assert_eq!(
                options
                    .celld_token("CELL_TOKEN")
                    .expect("retained token")
                    .expose(),
                CELL_TOKEN
            );
        } else if expected == "valid" {
            let options = AxumRunOptions::from_env().expect("env token capture");
            assert_eq!(
                options
                    .agent_token("AGENT_TOKEN")
                    .expect("captured token")
                    .expose(),
                "TOKEN_SENTINEL"
            );
            let path = env::var_os("EDGEZERO__STORE_BINDINGS_FILE").expect("locator");
            fs::write(
                PathBuf::from(path),
                BINDINGS.replace("AGENT_TOKEN", "OTHER_SENTINEL"),
            )
            .expect("change file after capture");
            let NativeConfigBinding::AppConfigAgent(settings) = options
                .bindings()
                .document()
                .expect("captured document")
                .config()
                .get("app")
                .expect("captured binding")
            else {
                panic!("expected AppConfig binding")
            };
            assert_eq!(settings.access_token_env.as_deref(), Some("AGENT_TOKEN"));
            assert_eq!(
                options
                    .agent_token("AGENT_TOKEN")
                    .expect("retained token")
                    .expose(),
                "TOKEN_SENTINEL"
            );
        } else {
            let error = AxumRunOptions::from_env()
                .err()
                .expect("invalid env bootstrap");
            assert_eq!(error.chain().count(), 1);
            assert!(!format!("{error:?}").contains("SENTINEL"));
        }
    }

    #[cfg(unix)]
    #[test]
    fn real_env_entrypoint_captures_named_tokens_and_rejects_non_unicode_inputs() {
        use std::env::current_exe;
        use std::os::unix::ffi::OsStringExt as _;
        use std::process::Command;

        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("bindings.toml");
        for (token, valid) in [
            (Some(OsString::from("TOKEN_SENTINEL")), true),
            (None, false),
            (Some(OsString::new()), false),
            (Some(OsString::from_vec(vec![0xff])), false),
        ] {
            fs::write(&path, BINDINGS).expect("fresh bindings");
            let mut command = Command::new(current_exe().expect("test binary"));
            command
                .env_clear()
                .arg("--exact")
                .arg("run_options::tests::env_bootstrap_child")
                .env(
                    "EDGEZERO_NATIVE_BOOTSTRAP_TEST",
                    if valid { "valid" } else { "invalid" },
                )
                .env("EDGEZERO__STORE_BINDINGS_FILE", &path);
            if let Some(value) = token {
                command.env("AGENT_TOKEN", value);
            }
            let output = command.output().expect("isolated env fixture");
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(!String::from_utf8_lossy(&output.stdout).contains("SENTINEL"));
            assert!(!String::from_utf8_lossy(&output.stderr).contains("SENTINEL"));
        }
        let output = Command::new(current_exe().expect("binary"))
            .env_clear()
            .arg("--exact")
            .arg("run_options::tests::env_bootstrap_child")
            .env("EDGEZERO_NATIVE_BOOTSTRAP_TEST", "invalid")
            .env(
                "EDGEZERO__STORE_BINDINGS_FILE",
                OsString::from_vec(vec![0xff]),
            )
            .output()
            .expect("non-Unicode locator fixture");
        assert!(output.status.success());
    }

    #[cfg(unix)]
    #[test]
    fn real_env_celld_capture_rejects_invalid_selected_values_and_retains_snapshot() {
        use std::env::current_exe;
        use std::os::unix::ffi::OsStringExt as _;
        use std::process::Command;
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("bindings.toml");
        for (token, valid) in [
            (Some(OsString::from(CELL_TOKEN)), true),
            (None, false),
            (Some(OsString::new()), false),
            (Some(OsString::from("short")), false),
            (Some(OsString::from_vec(vec![0xff])), false),
        ] {
            fs::write(&path, CELLD).expect("bindings");
            let mut command = Command::new(current_exe().expect("test binary"));
            command
                .env_clear()
                .arg("--exact")
                .arg("run_options::tests::env_bootstrap_child")
                .env(
                    "EDGEZERO_NATIVE_BOOTSTRAP_TEST",
                    if valid { "celld" } else { "invalid" },
                )
                .env("EDGEZERO__STORE_BINDINGS_FILE", &path);
            if let Some(value) = token {
                command.env("CELL_TOKEN", value);
            }
            let output = command.output().expect("isolated env child");
            assert!(output.status.success(), "child completed assertions");
            assert!(!String::from_utf8_lossy(&output.stdout).contains(CELL_TOKEN));
            assert!(!String::from_utf8_lossy(&output.stderr).contains(CELL_TOKEN));
        }
    }

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
                assert!(error.chain().count() == 1);
                assert!(!format!("{error:?}").contains("sentinel"));
            }
        }
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

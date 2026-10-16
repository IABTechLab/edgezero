//! Native bootstrap schema and pure per-ID source decisions, without service clients.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "schema, bootstrap I/O and source validation are grouped"
)]

#[cfg(unix)]
use libc::O_NONBLOCK;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::OpenOptions;
use std::io::Read as _;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};

use edgezero_core::app::{StoreMetadata, StoresMetadata};
use edgezero_core::env_config::EnvConfig;
use edgezero_core::manifest::{ManifestStores, StoreDeclaration};
use edgezero_core::secret_store::MAX_NAME_LEN;
use edgezero_store_aws::settings::{
    AppConfigAgentSettings, AwsPreparationLimits, DynamoDbKvSettings, SecretsManagerSettings,
};
use edgezero_store_celld::settings::{
    CelldConfigSettings, CelldKvSettings, CelldPreparationLimits,
};
use serde::Deserialize;

/// Closed bootstrap failures. File, TOML and provider inputs never become source chains.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NativeBindingsError {
    #[error("duplicate binding source")]
    Duplicate,
    #[error("invalid native bindings")]
    Invalid,
    #[error("invalid declared metadata")]
    InvalidMetadata,
    #[error("explicit binding conflicts with NAME/KEY overlay")]
    OverlayConflict,
    #[error("undeclared binding")]
    Undeclared,
    #[error("malformed native bindings")]
    Malformed,
    #[error("native bindings size limit")]
    SizeLimit,
    #[error("native bindings file unavailable")]
    Unavailable,
    #[error("provider not compiled")]
    UnsupportedProvider,
}

/// Trusted bootstrap bounds. The file cannot raise its own read/ID limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeBindingsLimits {
    pub max_file_bytes: usize,
    pub max_explicit_ids: usize,
}

impl Default for NativeBindingsLimits {
    #[inline]
    fn default() -> Self {
        Self {
            max_file_bytes: 256 * 1_024,
            max_explicit_ids: 128,
        }
    }
}

impl NativeBindingsLimits {
    /// Checks positive counters and the cap+1 overflow probe.
    ///
    /// # Errors
    /// Rejects zero or unusable counter values.
    #[inline]
    pub fn validate(&self) -> Result<(), NativeBindingsError> {
        if self.max_file_bytes == 0
            || self.max_explicit_ids == 0
            || self.max_file_bytes.checked_add(1).is_none()
            || u64::try_from(self.max_file_bytes).is_err()
        {
            return Err(NativeBindingsError::Invalid);
        }
        Ok(())
    }
}

/// Providers present in a resolved target, independent of the running CLI's features.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each provider feature is independent, not a state-machine flag"
)]
pub struct NativeProviderCapabilities {
    pub appconfig_agent: bool,
    pub secrets_manager: bool,
    pub dynamodb: bool,
    pub celld_config: bool,
    pub celld_kv: bool,
}

/// Strict configuration dispatch. Each provider validates its own settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "provider", deny_unknown_fields)]
pub enum NativeConfigBinding {
    #[serde(rename = "aws-appconfig-agent")]
    AppConfigAgent(AppConfigAgentSettings),
    #[serde(rename = "celld-config")]
    Celld(CelldConfigSettings),
}

/// Strict KV dispatch. The physical namespace is independent of the declared ID.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "provider", deny_unknown_fields)]
pub enum NativeKvBinding {
    #[serde(rename = "aws-dynamodb")]
    DynamoDb(DynamoDbKvSettings),
    #[serde(rename = "celld-kv")]
    Celld(CelldKvSettings),
}

/// Strict secret dispatch. No arbitrary remote names or per-request discovery.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "provider", deny_unknown_fields)]
pub enum NativeSecretBinding {
    #[serde(rename = "aws-secrets-manager")]
    SecretsManager(SecretsManagerSettings),
}

/// Independent explicit preparation policy sources. Same-provider sources cannot compete.
#[derive(Clone, Copy, Debug, Default)]
pub struct NativePreparationPolicySources {
    pub aws: Option<AwsPreparationLimits>,
    pub celld: Option<CelldPreparationLimits>,
}

/// Resolved provider policies, including defaults for providers not selected.
#[derive(Clone, Copy, Debug)]
pub struct NativePreparationPolicies {
    pub aws: AwsPreparationLimits,
    pub celld: CelldPreparationLimits,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitsTable {
    aws: Option<AwsPreparationLimits>,
    celld: Option<CelldPreparationLimits>,
}

/// Version 1 native document. Logical IDs and lookup keys remain case-sensitive.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeStoreBindings {
    version: u32,
    #[serde(default)]
    config: BTreeMap<String, NativeConfigBinding>,
    #[serde(default)]
    secrets: BTreeMap<String, NativeSecretBinding>,
    #[serde(default)]
    kv: BTreeMap<String, NativeKvBinding>,
    limits: Option<LimitsTable>,
}

impl fmt::Debug for NativeStoreBindings {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeStoreBindings")
            .finish_non_exhaustive()
    }
}

impl NativeStoreBindings {
    /// Builds a typed document with no file or ambient environment access.
    #[inline]
    #[must_use]
    pub fn new(
        config: BTreeMap<String, NativeConfigBinding>,
        secrets: BTreeMap<String, NativeSecretBinding>,
    ) -> Self {
        Self {
            version: 1,
            config,
            secrets,
            kv: BTreeMap::new(),
            limits: None,
        }
    }

    /// Parses strict TOML and validates resource shapes and trusted metadata bounds.
    ///
    /// # Errors
    /// Returns fixed categories, never TOML excerpts or raw settings.
    #[inline]
    pub fn parse(input: &str, limits: NativeBindingsLimits) -> Result<Self, NativeBindingsError> {
        Self::parse_with_policy(input, limits, None)
    }

    /// Parses using exactly one document or explicit remote policy source.
    ///
    /// # Errors
    /// Rejects competing policies and malformed/invalid/over-limit documents.
    #[inline]
    pub fn parse_with_policy(
        input: &str,
        limits: NativeBindingsLimits,
        aws: Option<AwsPreparationLimits>,
    ) -> Result<Self, NativeBindingsError> {
        Self::parse_with_policies(
            input,
            limits,
            NativePreparationPolicySources { aws, celld: None },
        )
    }

    /// Parses under independent explicit provider policies, before applying mapping caps.
    ///
    /// # Errors
    /// Rejects malformed, conflicting or over-limit inputs without source excerpts.
    #[inline]
    pub fn parse_with_policies(
        input: &str,
        limits: NativeBindingsLimits,
        policies: NativePreparationPolicySources,
    ) -> Result<Self, NativeBindingsError> {
        limits.validate()?;
        if input.len() > limits.max_file_bytes {
            return Err(NativeBindingsError::SizeLimit);
        }
        let bindings: Self =
            toml::from_str(input).map_err(|_error| NativeBindingsError::Malformed)?;
        bindings.validate_with_policies(limits, policies)?;
        Ok(bindings)
    }

    /// Reads one explicitly selected absolute regular file, capped before parsing.
    ///
    /// # Errors
    /// Missing, non-file, malformed and cap+1 inputs fail without echoing paths/contents.
    #[inline]
    pub fn read(path: &Path, limits: NativeBindingsLimits) -> Result<Self, NativeBindingsError> {
        Self::read_with_policy(path, limits, None)
    }

    /// Captures one capped file under an explicit remote limits policy if supplied.
    ///
    /// # Errors
    /// Rejects competing policies, unusable files and malformed/over-limit documents.
    #[inline]
    pub fn read_with_policy(
        path: &Path,
        limits: NativeBindingsLimits,
        aws: Option<AwsPreparationLimits>,
    ) -> Result<Self, NativeBindingsError> {
        Self::read_with_policies(
            path,
            limits,
            NativePreparationPolicySources { aws, celld: None },
        )
    }

    /// Captures one capped file using independent explicit provider policies.
    ///
    /// # Errors
    /// Rejects unusable files, conflicting policies and invalid documents.
    #[inline]
    pub fn read_with_policies(
        path: &Path,
        limits: NativeBindingsLimits,
        policies: NativePreparationPolicySources,
    ) -> Result<Self, NativeBindingsError> {
        limits.validate()?;
        if !path.is_absolute() {
            return Err(NativeBindingsError::Invalid);
        }
        let mut open = OpenOptions::new();
        open.read(true);
        // A named pipe must not block before metadata can reject it. The check
        // uses the opened handle, so replacing a path cannot bypass its type.
        #[cfg(unix)]
        open.custom_flags(O_NONBLOCK);
        let file = open
            .open(path)
            .map_err(|_error| NativeBindingsError::Unavailable)?;
        if !file
            .metadata()
            .map_err(|_error| NativeBindingsError::Unavailable)?
            .is_file()
        {
            return Err(NativeBindingsError::Unavailable);
        }
        let probe = limits
            .max_file_bytes
            .checked_add(1)
            .ok_or(NativeBindingsError::Invalid)?;
        let mut bytes = Vec::new();
        file.take(u64::try_from(probe).map_err(|_error| NativeBindingsError::Invalid)?)
            .read_to_end(&mut bytes)
            .map_err(|_error| NativeBindingsError::Unavailable)?;
        if bytes.len() > limits.max_file_bytes {
            return Err(NativeBindingsError::SizeLimit);
        }
        let input = String::from_utf8(bytes).map_err(|_error| NativeBindingsError::Malformed)?;
        Self::parse_with_policies(&input, limits, policies)
    }

    /// Checks the whole settings document before constructing providers.
    ///
    /// # Errors
    /// Rejects unsupported schema, mappings, limits and competing policy sources.
    #[inline]
    pub fn validate(
        &self,
        limits: NativeBindingsLimits,
        explicit_aws: Option<AwsPreparationLimits>,
    ) -> Result<AwsPreparationLimits, NativeBindingsError> {
        self.validate_with_policies(
            limits,
            NativePreparationPolicySources {
                aws: explicit_aws,
                celld: None,
            },
        )
        .map(|policies| policies.aws)
    }

    /// Validates all settings and resolves each provider policy independently.
    ///
    /// # Errors
    /// Rejects unsupported schema, invalid settings, limits and same-provider conflicts.
    #[inline]
    pub fn validate_with_policies(
        &self,
        limits: NativeBindingsLimits,
        explicit: NativePreparationPolicySources,
    ) -> Result<NativePreparationPolicies, NativeBindingsError> {
        limits.validate()?;
        if self.version != 1 {
            return Err(NativeBindingsError::Invalid);
        }
        if self
            .config
            .len()
            .checked_add(self.secrets.len())
            .and_then(|count| count.checked_add(self.kv.len()))
            .ok_or(NativeBindingsError::SizeLimit)?
            > limits.max_explicit_ids
        {
            return Err(NativeBindingsError::SizeLimit);
        }
        let file_aws = self.limits.as_ref().and_then(|table| table.aws);
        let file_celld = self.limits.as_ref().and_then(|table| table.celld);
        if (file_aws.is_some() && explicit.aws.is_some())
            || (file_celld.is_some() && explicit.celld.is_some())
        {
            return Err(NativeBindingsError::Duplicate);
        }
        let policies = NativePreparationPolicies {
            aws: explicit.aws.or(file_aws).unwrap_or_default(),
            celld: explicit.celld.or(file_celld).unwrap_or_default(),
        };
        policies
            .aws
            .validate()
            .map_err(|_category| NativeBindingsError::Invalid)?;
        policies
            .celld
            .validate()
            .map_err(|_category| NativeBindingsError::Invalid)?;
        let mut aws_mappings = 0_u64;
        let mut celld_mappings = 0_u64;
        for binding in self.kv.values() {
            let counter = match binding {
                NativeKvBinding::DynamoDb(settings) => {
                    settings
                        .validate()
                        .map_err(|_category| NativeBindingsError::Invalid)?;
                    &mut aws_mappings
                }
                NativeKvBinding::Celld(settings) => {
                    settings
                        .validate()
                        .map_err(|_category| NativeBindingsError::Invalid)?;
                    &mut celld_mappings
                }
            };
            charge_mappings(counter, 1)?;
        }
        for binding in self.config.values() {
            let (counter, count) = match binding {
                NativeConfigBinding::AppConfigAgent(settings) => {
                    settings
                        .validate()
                        .map_err(|_category| NativeBindingsError::Invalid)?;
                    (&mut aws_mappings, settings.documents.len())
                }
                NativeConfigBinding::Celld(settings) => {
                    settings
                        .validate()
                        .map_err(|_category| NativeBindingsError::Invalid)?;
                    (&mut celld_mappings, settings.documents.len())
                }
            };
            charge_mappings(counter, count)?;
        }
        for binding in self.secrets.values() {
            let NativeSecretBinding::SecretsManager(settings) = binding;
            settings
                .validate()
                .map_err(|_category| NativeBindingsError::Invalid)?;
            charge_mappings(&mut aws_mappings, settings.entries.len())?;
        }
        if aws_mappings > policies.aws.max_remote_values
            || celld_mappings > policies.celld.max_remote_values
        {
            return Err(NativeBindingsError::SizeLimit);
        }
        Ok(policies)
    }

    /// Explicit configuration mappings.
    #[inline]
    #[must_use]
    pub fn config(&self) -> &BTreeMap<String, NativeConfigBinding> {
        &self.config
    }

    /// Explicit live KV bindings.
    #[inline]
    #[must_use]
    pub fn kv(&self) -> &BTreeMap<String, NativeKvBinding> {
        &self.kv
    }

    /// Adds KV bindings without changing the original config/secrets constructor.
    #[inline]
    #[must_use]
    pub fn with_kv(mut self, kv: BTreeMap<String, NativeKvBinding>) -> Self {
        self.kv = kv;
        self
    }

    /// Explicit secret mappings.
    #[inline]
    #[must_use]
    pub fn secrets(&self) -> &BTreeMap<String, NativeSecretBinding> {
        &self.secrets
    }

    /// Selects one typed remote limits policy; it does not merge policy sources.
    #[inline]
    #[must_use]
    pub fn with_aws_limits(mut self, limits: AwsPreparationLimits) -> Self {
        self.limits.get_or_insert_with(LimitsTable::default).aws = Some(limits);
        self
    }

    /// Selects a celld document policy without discarding an AWS policy.
    #[inline]
    #[must_use]
    pub fn with_celld_limits(mut self, limits: CelldPreparationLimits) -> Self {
        self.limits.get_or_insert_with(LimitsTable::default).celld = Some(limits);
        self
    }
}

fn charge_mappings(counter: &mut u64, count: usize) -> Result<(), NativeBindingsError> {
    *counter = counter
        .checked_add(u64::try_from(count).map_err(|_error| NativeBindingsError::SizeLimit)?)
        .ok_or(NativeBindingsError::SizeLimit)?;
    Ok(())
}

/// Cloneable bootstrap selection. File contents are captured once at selection.
#[derive(Clone, Default)]
pub enum NativeStoreBindingsInput {
    #[default]
    LocalDefaults,
    File {
        path: PathBuf,
        bindings: NativeStoreBindings,
    },
    Inline(NativeStoreBindings),
}

impl NativeStoreBindingsInput {
    /// Captured settings, without reopening the selected file.
    #[inline]
    #[must_use]
    pub fn document(&self) -> Option<&NativeStoreBindings> {
        match self {
            Self::LocalDefaults => None,
            Self::File { bindings, .. } | Self::Inline(bindings) => Some(bindings),
        }
    }
}

impl fmt::Debug for NativeStoreBindingsInput {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::LocalDefaults => "LocalDefaults",
            Self::File { .. } => "File([redacted])",
            Self::Inline(_) => "Inline([redacted])",
        })
    }
}

/// Pure source classification. Custom handles are not inspected or quota-accounted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeBindingSource {
    Local,
    Supplied,
    BuiltIn,
}

/// Supplied IDs only. This carries no handles/futures and is usable by offline CLI checks.
#[derive(Default)]
pub struct NativeSuppliedIds {
    pub config: BTreeSet<String>,
    pub kv: BTreeSet<String>,
    pub secrets: BTreeSet<String>,
}

/// Borrowed declarations for compiled metadata or a CLI-owned portable manifest.
pub struct NativeStoreDeclarations<'declarations> {
    pub config: Option<NativeStoreDeclaration<'declarations>>,
    pub kv: Option<NativeStoreDeclaration<'declarations>>,
    pub secrets: Option<NativeStoreDeclaration<'declarations>>,
}

/// Names remain borrowed from the caller; validation does not leak or intern IDs.
pub struct NativeStoreDeclaration<'declarations> {
    pub ids: Vec<&'declarations str>,
    pub default: &'declarations str,
}

impl From<StoreMetadata> for NativeStoreDeclaration<'static> {
    #[inline]
    fn from(metadata: StoreMetadata) -> Self {
        Self {
            ids: metadata.ids.to_vec(),
            default: metadata.default,
        }
    }
}

impl From<StoresMetadata> for NativeStoreDeclarations<'static> {
    #[inline]
    fn from(metadata: StoresMetadata) -> Self {
        Self {
            config: metadata.config.map(Into::into),
            kv: metadata.kv.map(Into::into),
            secrets: metadata.secrets.map(Into::into),
        }
    }
}

impl<'declarations> TryFrom<&'declarations StoreDeclaration>
    for NativeStoreDeclaration<'declarations>
{
    type Error = NativeBindingsError;

    #[inline]
    fn try_from(declaration: &'declarations StoreDeclaration) -> Result<Self, Self::Error> {
        if !declaration.legacy.is_empty()
            || (declaration.ids.len() > 1 && declaration.default.is_none())
        {
            return Err(NativeBindingsError::InvalidMetadata);
        }
        Ok(Self {
            ids: declaration.ids.iter().map(String::as_str).collect(),
            default: declaration.default_id(),
        })
    }
}

impl<'declarations> TryFrom<&'declarations ManifestStores>
    for NativeStoreDeclarations<'declarations>
{
    type Error = NativeBindingsError;

    #[inline]
    fn try_from(stores: &'declarations ManifestStores) -> Result<Self, Self::Error> {
        Ok(Self {
            config: stores
                .config
                .as_ref()
                .map(NativeStoreDeclaration::try_from)
                .transpose()?,
            kv: stores
                .kv
                .as_ref()
                .map(NativeStoreDeclaration::try_from)
                .transpose()?,
            secrets: stores
                .secrets
                .as_ref()
                .map(NativeStoreDeclaration::try_from)
                .transpose()?,
        })
    }
}

/// Complete per-ID decisions. Registries keep their original metadata/defaults.
pub struct NativeSourceSelection<'declarations> {
    pub config: BTreeMap<&'declarations str, NativeBindingSource>,
    pub kv: BTreeMap<&'declarations str, NativeBindingSource>,
    pub secrets: BTreeMap<&'declarations str, NativeBindingSource>,
}

impl<'declarations> NativeSourceSelection<'declarations> {
    /// Validates metadata, every source and overlay, then checks resolved capabilities.
    ///
    /// # Errors
    /// Duplicate/undeclared sources, scalar collisions and absent target providers fail closed.
    #[inline]
    pub fn validate<Declarations: Into<NativeStoreDeclarations<'declarations>>>(
        metadata: Declarations,
        bindings: &NativeStoreBindings,
        supplied: &NativeSuppliedIds,
        env: &EnvConfig,
        capabilities: NativeProviderCapabilities,
        limits: NativeBindingsLimits,
        aws: Option<AwsPreparationLimits>,
    ) -> Result<Self, NativeBindingsError> {
        Self::validate_with_policies(
            metadata,
            bindings,
            supplied,
            env,
            capabilities,
            limits,
            NativePreparationPolicySources { aws, celld: None },
        )
    }

    /// Validates all sources with independent provider policies and actual-variant capabilities.
    ///
    /// # Errors
    /// Rejects invalid declarations, settings, conflicts and unavailable selected providers.
    #[inline]
    pub fn validate_with_policies<Declarations: Into<NativeStoreDeclarations<'declarations>>>(
        metadata: Declarations,
        bindings: &NativeStoreBindings,
        supplied: &NativeSuppliedIds,
        env: &EnvConfig,
        capabilities: NativeProviderCapabilities,
        limits: NativeBindingsLimits,
        policies: NativePreparationPolicySources,
    ) -> Result<Self, NativeBindingsError> {
        bindings.validate_with_policies(limits, policies)?;
        let declarations = metadata.into();
        let selection = Self {
            config: resolve(
                declarations.config,
                &supplied.config,
                bindings.config.keys(),
                env,
                "config",
            )?,
            kv: resolve(declarations.kv, &supplied.kv, bindings.kv.keys(), env, "kv")?,
            secrets: resolve(
                declarations.secrets,
                &supplied.secrets,
                bindings.secrets.keys(),
                env,
                "secrets",
            )?,
        };
        if bindings.config.values().any(|binding| match binding {
            NativeConfigBinding::AppConfigAgent(_) => !capabilities.appconfig_agent,
            NativeConfigBinding::Celld(_) => !capabilities.celld_config,
        }) || (!bindings.secrets.is_empty() && !capabilities.secrets_manager)
            || bindings.kv.values().any(|binding| match binding {
                NativeKvBinding::DynamoDb(_) => !capabilities.dynamodb,
                NativeKvBinding::Celld(_) => !capabilities.celld_kv,
            })
        {
            return Err(NativeBindingsError::UnsupportedProvider);
        }
        Ok(selection)
    }
}

fn resolve<'declarations, 'settings, Ids: Iterator<Item = &'settings String>>(
    metadata: Option<NativeStoreDeclaration<'declarations>>,
    supplied: &BTreeSet<String>,
    bound: Ids,
    env: &EnvConfig,
    kind: &str,
) -> Result<BTreeMap<&'declarations str, NativeBindingSource>, NativeBindingsError> {
    let settings: BTreeSet<&str> = bound.map(String::as_str).collect();
    let Some(meta) = metadata else {
        return if supplied.is_empty() && settings.is_empty() {
            Ok(BTreeMap::new())
        } else {
            Err(NativeBindingsError::Undeclared)
        };
    };
    if meta.ids.is_empty()
        || !meta.ids.contains(&meta.default)
        || meta.ids.iter().enumerate().any(|(index, id)| {
            id.is_empty()
                || !id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                || meta.ids.iter().take(index).any(|previous| previous == id)
        })
    {
        return Err(NativeBindingsError::InvalidMetadata);
    }
    if supplied.iter().any(|id| !meta.ids.contains(&id.as_str()))
        || settings.iter().any(|id| !meta.ids.contains(id))
    {
        return Err(NativeBindingsError::Undeclared);
    }
    let mut result = BTreeMap::new();
    for id in meta.ids {
        let source = match (supplied.contains(id), settings.contains(id)) {
            (true, true) => return Err(NativeBindingsError::Duplicate),
            (true, false) => NativeBindingSource::Supplied,
            (false, true) => NativeBindingSource::BuiltIn,
            (false, false) => NativeBindingSource::Local,
        };
        if source != NativeBindingSource::Local
            && ["name", "key"]
                .iter()
                .any(|field| env.store_setting(kind, id, field).is_some())
        {
            return Err(NativeBindingsError::OverlayConflict);
        }
        if source == NativeBindingSource::Local
            && ["name", "key"].iter().any(|field| {
                env.store_setting(kind, id, field).is_some_and(|value| {
                    value.trim().is_empty() || value.chars().any(char::is_control)
                })
            })
        {
            return Err(NativeBindingsError::Invalid);
        }
        if kind == "secrets" && source != NativeBindingSource::Supplied {
            let namespace = if source == NativeBindingSource::Local {
                env.store_setting(kind, id, "name").map_or(id, str::trim)
            } else {
                id
            };
            if namespace.len() > MAX_NAME_LEN {
                return Err(
                    if source == NativeBindingSource::Local
                        && env.store_setting(kind, id, "name").is_some()
                    {
                        NativeBindingsError::Invalid
                    } else {
                        NativeBindingsError::InvalidMetadata
                    },
                );
            }
        }
        result.insert(id, source);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::assertions_on_result_states,
        reason = "strict parser tests compare accepted/rejected input matrices"
    )]
    #![expect(
        clippy::panic_in_result_fn,
        reason = "tests use fallible setup and assertions"
    )]

    use std::collections::{BTreeMap, BTreeSet};
    use std::error::Error;
    use std::fmt::Write as _;
    use std::fs;
    use std::path::Path;

    use edgezero_core::app::{StoreMetadata, StoresMetadata};
    use edgezero_core::env_config::EnvConfig;
    use edgezero_core::manifest::ManifestStores;
    use edgezero_store_aws::settings::AwsPreparationLimits;
    use edgezero_store_celld::settings::CelldPreparationLimits;

    use super::{
        NativeBindingSource, NativeBindingsError, NativeBindingsLimits,
        NativePreparationPolicySources, NativeProviderCapabilities, NativeSourceSelection,
        NativeStoreBindings, NativeStoreDeclarations, NativeSuppliedIds,
    };

    const CONFIG: &str = "version = 1\n[config.app]\nprovider = 'aws-appconfig-agent'\ndefault_key = 'settings'\nendpoint = 'http://127.0.0.1:2772'\n[config.app.documents.settings]\napplication = 'example-app'\nenvironment = 'production'\nprofile = 'envelope'\n";

    const CELLD: &str = "version=1\n[config.app]\nprovider='celld-config'\nendpoint='http://127.0.0.1:8788'\ndefault_key='settings'\n[config.app.access]\nmode='trusted-network'\n[config.app.documents.settings]\nstore='config-fixture'\nkey='settings'\n[kv.state]\nprovider='celld-kv'\nendpoint='http://127.0.0.1:8788'\nstore='state-fixture'\n[kv.state.access]\nmode='trusted-network'\n";

    #[test]
    fn celld_schema_is_structural_strict_and_settings_only() -> Result<(), NativeBindingsError> {
        let limits = NativeBindingsLimits::default();
        for input in [
            CELLD.to_owned(),
            CELLD
                .replace("http://127.0.0.1:8788", "https://unreachable.invalid")
                .replace(
                    "mode='trusted-network'",
                    "mode='authenticated'\ntoken_env='ABSENT_TOKEN'",
                ),
        ] {
            let document = NativeStoreBindings::parse(&input, limits)?;
            assert_eq!(document.config().len(), 1);
            assert_eq!(document.kv().len(), 1);
            assert!(!format!("{document:?}").contains("fixture"));
        }
        for bad in [
            CELLD.replace("celld-config", "celld-unknown"),
            CELLD.replace("[config.app]", "[secrets.app]"),
            CELLD.replace("default_key='settings'", "default_key='missing'"),
            CELLD.replace("store='state-fixture'", "store='bad/store'"),
            CELLD.replace("mode='trusted-network'", "mode='authenticated'"),
            CELLD.replace(
                "mode='trusted-network'",
                "mode='trusted-network'\ntoken_env='SENTINEL'",
            ),
            CELLD.replace("http://127.0.0.1:8788", "http://private.invalid"),
            format!("{CELLD}\n[limits.celld]\noperation_timeout_ms=0"),
            format!("{CELLD}\n[limits.celld]\nunknown=1"),
        ] {
            let error = NativeStoreBindings::parse(&bad, limits)
                .err()
                .ok_or(NativeBindingsError::Invalid)?;
            assert!(error.source().is_none());
            assert!(!format!("{error:?}").contains("SENTINEL"));
        }
        Ok(())
    }

    #[test]
    fn capabilities_follow_actual_variants() -> Result<(), NativeBindingsError> {
        let limits = NativeBindingsLimits::default();
        let aws_kv = "\n[kv.state]\nprovider='aws-dynamodb'\nregion='us-east-1'\ntable='fixture-table'\nnamespace='fixture-space'\n";
        let celld_kv = CELLD
            .split("[kv.state]")
            .nth(1)
            .ok_or(NativeBindingsError::Invalid)?;
        let celld_config = CELLD
            .split("[kv.state]")
            .next()
            .ok_or(NativeBindingsError::Invalid)?;
        for (input, aws_config, aws_state, celld_app, celld_state) in [
            (CELLD.to_owned(), false, false, true, true),
            (format!("{CONFIG}{aws_kv}"), true, true, false, false),
            (
                format!("{CONFIG}[kv.state]{celld_kv}"),
                true,
                false,
                false,
                true,
            ),
            (format!("{celld_config}{aws_kv}"), false, true, true, false),
        ] {
            let document = NativeStoreBindings::parse(&input, limits)?;
            for mask in 0_u8..16 {
                let caps = NativeProviderCapabilities {
                    appconfig_agent: mask & 1 != 0,
                    dynamodb: mask & 2 != 0,
                    celld_config: mask & 4 != 0,
                    celld_kv: mask & 8 != 0,
                    secrets_manager: false,
                };
                let accepted = (!aws_config || caps.appconfig_agent)
                    && (!aws_state || caps.dynamodb)
                    && (!celld_app || caps.celld_config)
                    && (!celld_state || caps.celld_kv);
                assert_eq!(
                    NativeSourceSelection::validate(
                        metadata(),
                        &document,
                        &NativeSuppliedIds::default(),
                        &EnvConfig::default(),
                        caps,
                        limits,
                        None
                    )
                    .is_ok(),
                    accepted
                );
            }
        }
        Ok(())
    }

    #[test]
    fn provider_policy_sources_and_mapping_counters_are_independent()
    -> Result<(), NativeBindingsError> {
        let limits = NativeBindingsLimits::default();
        let aws = AwsPreparationLimits {
            max_remote_values: 1,
            ..Default::default()
        };
        let celld = CelldPreparationLimits {
            max_remote_values: 2,
            ..Default::default()
        };
        let policies = NativePreparationPolicySources {
            aws: Some(aws),
            celld: Some(celld),
        };
        let mixed = format!(
            "{CONFIG}{}",
            CELLD
                .replace("version=1\n", "")
                .replace("config.app", "config.remote")
        );
        NativeStoreBindings::parse_with_policies(&mixed, limits, policies)?;
        let too_many = format!(
            "{mixed}\n[config.remote.documents.alias]\nstore='config-fixture'\nkey='settings'\n"
        );
        assert_eq!(
            NativeStoreBindings::parse_with_policies(&too_many, limits, policies).err(),
            Some(NativeBindingsError::SizeLimit)
        );
        let reverse = NativePreparationPolicySources {
            aws: Some(AwsPreparationLimits {
                max_remote_values: 2,
                ..Default::default()
            }),
            celld: Some(CelldPreparationLimits {
                max_remote_values: 1,
                ..Default::default()
            }),
        };
        assert_eq!(
            NativeStoreBindings::parse_with_policies(&mixed, limits, reverse).err(),
            Some(NativeBindingsError::SizeLimit)
        );
        for (table, explicit) in [
            (
                "[limits.aws]\nmax_remote_values=1",
                NativePreparationPolicySources {
                    aws: None,
                    celld: Some(celld),
                },
            ),
            (
                "[limits.celld]\nmax_remote_values=2",
                NativePreparationPolicySources {
                    aws: Some(aws),
                    celld: None,
                },
            ),
            (
                "[limits.aws]\nmax_remote_values=1\n[limits.celld]\nmax_remote_values=2",
                NativePreparationPolicySources::default(),
            ),
        ] {
            let document = NativeStoreBindings::parse_with_policies(
                &format!("{mixed}\n{table}"),
                limits,
                explicit,
            )?;
            let resolved = document.validate_with_policies(limits, explicit)?;
            assert_eq!(resolved.aws.max_remote_values, 1);
            assert_eq!(resolved.celld.max_remote_values, 2);
            assert!(matches!(
                document.validate_with_policies(limits, policies),
                Err(NativeBindingsError::Duplicate)
            ));
        }
        let document = NativeStoreBindings::parse(&mixed, limits)?;
        for typed in [
            document
                .clone()
                .with_aws_limits(aws)
                .with_celld_limits(celld),
            document.with_celld_limits(celld).with_aws_limits(aws),
        ] {
            let resolved =
                typed.validate_with_policies(limits, NativePreparationPolicySources::default())?;
            assert_eq!(resolved.aws.max_remote_values, 1);
            assert_eq!(resolved.celld.max_remote_values, 2);
        }
        Ok(())
    }

    #[test]
    fn explicit_celld_policy_raises_caps_during_file_parse() -> Result<(), Box<dyn Error>> {
        let mut input = CELLD.to_owned();
        for index in 0..1_024_u32 {
            writeln!(
                input,
                "\n[config.app.documents.alias{index}]\nstore='config-fixture'\nkey='settings'"
            )?;
        }
        let limits = NativeBindingsLimits::default();
        assert_eq!(
            NativeStoreBindings::parse(&input, limits).err(),
            Some(NativeBindingsError::SizeLimit)
        );
        let explicit = NativePreparationPolicySources {
            celld: Some(CelldPreparationLimits {
                max_remote_values: 2_048,
                ..Default::default()
            }),
            aws: None,
        };
        let root = tempfile::tempdir()?;
        let path = root.path().join("bindings.toml");
        fs::write(&path, input)?;
        NativeStoreBindings::read_with_policies(&path, limits, explicit)?;
        Ok(())
    }

    fn metadata() -> StoresMetadata {
        StoresMetadata {
            config: Some(StoreMetadata {
                ids: &["app", "local"],
                default: "app",
            }),
            kv: Some(StoreMetadata {
                ids: &["state"],
                default: "state",
            }),
            secrets: None,
        }
    }

    #[test]
    fn dynamodb_binding_source_is_per_id_and_fails_closed() -> Result<(), NativeBindingsError> {
        let limits = NativeBindingsLimits::default();
        let bindings = NativeStoreBindings::parse(
            "version=1\n[kv.state]\nprovider='aws-dynamodb'\nregion='us-east-1'\ntable='edgezero-kv'\nnamespace='private-space'",
            limits,
        )?;
        let capabilities = NativeProviderCapabilities {
            dynamodb: true,
            ..Default::default()
        };
        let selection = NativeSourceSelection::validate(
            metadata(),
            &bindings,
            &NativeSuppliedIds::default(),
            &EnvConfig::default(),
            capabilities,
            limits,
            None,
        )?;
        assert_eq!(
            selection.kv.get("state"),
            Some(&NativeBindingSource::BuiltIn)
        );
        assert!(matches!(
            NativeSourceSelection::validate(
                metadata(),
                &bindings,
                &NativeSuppliedIds::default(),
                &EnvConfig::default(),
                NativeProviderCapabilities::default(),
                limits,
                None
            ),
            Err(NativeBindingsError::UnsupportedProvider)
        ));
        let supplied = NativeSuppliedIds {
            kv: ["state".to_owned()].into_iter().collect(),
            ..Default::default()
        };
        assert!(matches!(
            NativeSourceSelection::validate(
                metadata(),
                &bindings,
                &supplied,
                &EnvConfig::default(),
                capabilities,
                limits,
                None
            ),
            Err(NativeBindingsError::Duplicate)
        ));
        assert!(!format!("{bindings:?}").contains("private-space"));
        Ok(())
    }

    #[test]
    fn manifest_ids_remain_borrowed_and_invalid_declarations_fail() -> Result<(), Box<dyn Error>> {
        let owned: ManifestStores =
            toml::from_str("[config]\nids = ['app', 'local']\ndefault = 'app'\n")?;
        let bindings = NativeStoreBindings::parse(CONFIG, NativeBindingsLimits::default())?;
        let selection = NativeSourceSelection::validate(
            NativeStoreDeclarations::try_from(&owned)?,
            &bindings,
            &NativeSuppliedIds::default(),
            &EnvConfig::default(),
            NativeProviderCapabilities {
                appconfig_agent: true,
                secrets_manager: false,
                dynamodb: false,
                ..Default::default()
            },
            NativeBindingsLimits::default(),
            None,
        )?;
        assert_eq!(
            selection.config.get("app"),
            Some(&NativeBindingSource::BuiltIn)
        );
        let stored_id = selection
            .config
            .get_key_value("app")
            .ok_or(NativeBindingsError::InvalidMetadata)?
            .0;
        let original_id = &owned
            .config
            .as_ref()
            .ok_or(NativeBindingsError::InvalidMetadata)?
            .ids[0];
        assert_eq!(stored_id.as_ptr(), original_id.as_ptr());
        for input in [
            "[config]\nids = ['app', 'local']\n",
            "[config]\nids = ['app']\nname = 'legacy'\n",
        ] {
            let invalid: ManifestStores = toml::from_str(input)?;
            assert!(matches!(
                NativeStoreDeclarations::try_from(&invalid),
                Err(NativeBindingsError::InvalidMetadata)
            ));
        }
        Ok(())
    }

    #[test]
    fn secret_namespace_limits_are_checked_before_preparation() {
        let metadata = StoresMetadata {
            config: None,
            kv: None,
            secrets: Some(StoreMetadata {
                ids: &["signing"],
                default: "signing",
            }),
        };
        let bindings = NativeStoreBindings::new(BTreeMap::new(), BTreeMap::new());
        for (bytes, accepted) in [(512, true), (513, false)] {
            let env = EnvConfig::from_vars([(
                "EDGEZERO__STORES__SECRETS__SIGNING__NAME",
                "x".repeat(bytes),
            )]);
            let result = NativeSourceSelection::validate(
                metadata,
                &bindings,
                &NativeSuppliedIds::default(),
                &env,
                NativeProviderCapabilities::default(),
                NativeBindingsLimits::default(),
                None,
            );
            assert_eq!(result.is_ok(), accepted);
        }
    }

    #[test]
    fn strict_tagged_schema_and_case_sensitive_mappings() {
        let limits = NativeBindingsLimits::default();
        assert!(NativeStoreBindings::parse(CONFIG, limits).is_ok());
        for input in [
            CONFIG.replace("version = 1", "version = 2"),
            CONFIG.replace("version = 1", "version = 1\nunknown = 'sentinel'"),
            CONFIG.replace("aws-appconfig-agent", "dynamodb"),
            CONFIG.replace("default_key = 'settings'", "default_key = 'Settings'"),
            CONFIG.replace("endpoint =", "unknown = 'sentinel'\nendpoint ="),
            CONFIG.replace("application =", "unknown = 'sentinel'\napplication ="),
            CONFIG.replace("[config.app.documents.settings]", "[config.app.documents]"),
            format!("{CONFIG}\n[kv.state]\nprovider = 'dynamodb'"),
            format!("{CONFIG}\n[unknown]\nvalue = 'sentinel'"),
            CONFIG.replace("version = 1", "version = 1\nversion = 1"),
            format!("{CONFIG}\n[config.app]\nprovider = 'aws-appconfig-agent'"),
            format!("{CONFIG}\n[config.app.documents.settings]\nprofile = 'sentinel'"),
        ] {
            let error = NativeStoreBindings::parse(&input, limits).err();
            assert!(error.is_some(), "accepted malformed bindings");
            assert!(!format!("{error:?}").contains("sentinel"));
        }
    }

    #[test]
    fn capped_file_read_and_trusted_id_limit() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("sentinel.toml");
        let limits = NativeBindingsLimits {
            max_file_bytes: CONFIG.len(),
            max_explicit_ids: 1,
        };
        fs::write(&path, CONFIG)?;
        assert!(NativeStoreBindings::read(&path, limits).is_ok());
        fs::write(&path, format!("{CONFIG} "))?;
        assert_eq!(
            NativeStoreBindings::read(&path, limits).err(),
            Some(NativeBindingsError::SizeLimit)
        );
        fs::write(&path, [0xff])?;
        let malformed = NativeStoreBindings::read(&path, limits)
            .err()
            .ok_or(NativeBindingsError::Invalid)?;
        assert_eq!(malformed, NativeBindingsError::Malformed);
        assert!(!format!("{malformed:?}").contains("sentinel"));
        assert!(malformed.source().is_none());
        assert_eq!(
            NativeStoreBindings::read(directory.path(), limits).err(),
            Some(NativeBindingsError::Unavailable)
        );
        assert_eq!(
            NativeStoreBindings::read(Path::new("relative.toml"), limits).err(),
            Some(NativeBindingsError::Invalid)
        );
        let mut many = String::from("version = 1\n");
        for index in 0..=128_u32 {
            many.push_str(
                &CONFIG
                    .replace("version = 1\n", "")
                    .replace("config.app", &format!("config.store{index}")),
            );
            if index == 127 {
                assert!(NativeStoreBindings::parse(&many, NativeBindingsLimits::default()).is_ok());
            }
        }
        assert_eq!(
            NativeStoreBindings::parse(&many, NativeBindingsLimits::default()).err(),
            Some(NativeBindingsError::SizeLimit)
        );
        Ok(())
    }

    #[test]
    fn whole_source_validation_and_target_capabilities() -> Result<(), NativeBindingsError> {
        let limits = NativeBindingsLimits::default();
        let bindings = NativeStoreBindings::parse(CONFIG, limits)?;
        let available = NativeProviderCapabilities {
            appconfig_agent: true,
            secrets_manager: false,
            dynamodb: false,
            ..Default::default()
        };
        let mut supplied = NativeSuppliedIds::default();
        let env = EnvConfig::default();
        let selection = NativeSourceSelection::validate(
            metadata(),
            &bindings,
            &supplied,
            &env,
            available,
            limits,
            None,
        )?;
        assert_eq!(
            selection.config.get("app"),
            Some(&NativeBindingSource::BuiltIn)
        );
        assert_eq!(
            selection.config.get("local"),
            Some(&NativeBindingSource::Local)
        );
        assert_eq!(selection.kv.get("state"), Some(&NativeBindingSource::Local));
        assert!(matches!(
            NativeSourceSelection::validate(
                metadata(),
                &bindings,
                &supplied,
                &env,
                NativeProviderCapabilities::default(),
                limits,
                None
            ),
            Err(NativeBindingsError::UnsupportedProvider)
        ));
        supplied.config = BTreeSet::from(["app".to_owned()]);
        assert!(matches!(
            NativeSourceSelection::validate(
                metadata(),
                &bindings,
                &supplied,
                &env,
                available,
                limits,
                None
            ),
            Err(NativeBindingsError::Duplicate)
        ));
        supplied.config = BTreeSet::from(["APP".to_owned()]);
        assert!(matches!(
            NativeSourceSelection::validate(
                metadata(),
                &bindings,
                &supplied,
                &env,
                available,
                limits,
                None
            ),
            Err(NativeBindingsError::Undeclared)
        ));
        supplied.config.clear();
        for field in ["NAME", "KEY"] {
            let overlay =
                EnvConfig::from_vars([(format!("EDGEZERO__STORES__CONFIG__APP__{field}"), "")]);
            assert!(matches!(
                NativeSourceSelection::validate(
                    metadata(),
                    &bindings,
                    &supplied,
                    &overlay,
                    available,
                    limits,
                    None
                ),
                Err(NativeBindingsError::OverlayConflict)
            ));
        }
        Ok(())
    }

    #[test]
    fn explicit_policy_can_raise_mapping_caps_before_file_parsing() -> Result<(), Box<dyn Error>> {
        let mut input = CONFIG.to_owned();
        for index in 0..1_024_u32 {
            writeln!(
                input,
                "\n[config.app.documents.alias{index}]\napplication = 'example-app'\nenvironment = 'production'\nprofile = 'envelope'"
            )?;
        }
        let limits = NativeBindingsLimits::default();
        assert_eq!(
            NativeStoreBindings::parse(&input, limits).err(),
            Some(NativeBindingsError::SizeLimit)
        );
        let aws = AwsPreparationLimits {
            max_remote_values: 2_048,
            ..AwsPreparationLimits::default()
        };
        let root = tempfile::tempdir()?;
        let path = root.path().join("bindings.toml");
        fs::write(&path, input)?;
        let bindings = NativeStoreBindings::read_with_policy(&path, limits, Some(aws))?;
        assert_eq!(
            bindings.validate(limits, Some(aws))?.max_remote_values,
            2_048
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn named_pipe_is_rejected_without_waiting_for_a_writer() -> Result<(), Box<dyn Error>> {
        use std::process::Command;
        let root = tempfile::tempdir()?;
        let path = root.path().join("bindings.fifo");
        assert!(Command::new("mkfifo").arg(&path).status()?.success());
        assert_eq!(
            NativeStoreBindings::read(&path, NativeBindingsLimits::default()).err(),
            Some(NativeBindingsError::Unavailable)
        );
        Ok(())
    }

    #[test]
    fn secret_schema_rejects_irrelevant_fields_and_preserves_selectors()
    -> Result<(), NativeBindingsError> {
        let input = "version = 1\n[secrets.signing]\nprovider = 'aws-secrets-manager'\nregion = 'us-east-1'\n[secrets.signing.entries.active]\nsecret_id = 'arn:aws:secretsmanager:us-east-1:111122223333:secret:SENTINEL-AbCdEf'\nversion_stage = 'AWSCURRENT'\n";
        let limits = NativeBindingsLimits::default();
        let document = NativeStoreBindings::parse(input, limits)?;
        assert_eq!(document.secrets().len(), 1);
        assert!(!format!("{document:?}").contains("SENTINEL"));
        for bad in [
            input.replace("region =", "endpoint = 'http://127.0.0.1'\nregion ="),
            input.replace("version_stage =", "unknown = 'SENTINEL'\nversion_stage ="),
            input.replace("us-east-1'\n[secrets", "us-west-2'\n[secrets"),
            input.replace(
                "version_stage =",
                "version_id = '12345678901234567890123456789012'\nversion_stage =",
            ),
            input.replace("secret_id =", "secret_id = 'partial-name'\nother ="),
        ] {
            let error = NativeStoreBindings::parse(&bad, limits)
                .err()
                .ok_or(NativeBindingsError::Invalid)?;
            assert!(!format!("{error:?}").contains("SENTINEL"));
            assert!(error.source().is_none());
        }
        Ok(())
    }

    #[test]
    fn exact_mapping_boundary_uses_one_file_or_explicit_policy() -> Result<(), NativeBindingsError>
    {
        let limits = NativeBindingsLimits::default();
        let aws = AwsPreparationLimits {
            max_remote_values: 1,
            ..AwsPreparationLimits::default()
        };
        let explicit = NativeStoreBindings::parse_with_policy(CONFIG, limits, Some(aws))?;
        assert_eq!(explicit.validate(limits, Some(aws))?, aws);
        let file = NativeStoreBindings::parse(
            &format!("{CONFIG}\n[limits.aws]\nmax_remote_values = 1"),
            limits,
        )?;
        assert_eq!(file.validate(limits, None)?, aws);
        let alias = "\n[config.app.documents.alias]\napplication = 'example-app'\nenvironment = 'production'\nprofile = 'envelope'\n";
        assert_eq!(
            NativeStoreBindings::parse_with_policy(&format!("{CONFIG}{alias}"), limits, Some(aws))
                .err(),
            Some(NativeBindingsError::SizeLimit)
        );
        assert_eq!(
            NativeStoreBindings::parse(
                &format!("{CONFIG}{alias}\n[limits.aws]\nmax_remote_values = 1"),
                limits
            )
            .err(),
            Some(NativeBindingsError::SizeLimit)
        );
        Ok(())
    }

    #[test]
    fn limit_policy_sources_do_not_merge_and_aliases_count_before_dedup()
    -> Result<(), NativeBindingsError> {
        let limits = NativeBindingsLimits::default();
        let with_policy = format!("{CONFIG}\n[limits.aws]\nmax_snapshot_bytes = 16777216");
        let bindings = NativeStoreBindings::parse(&with_policy, limits)?;
        assert_eq!(
            bindings.validate(limits, Some(AwsPreparationLimits::default())),
            Err(NativeBindingsError::Duplicate)
        );
        let aliases = format!(
            "{CONFIG}\n[config.app.documents.alias]\napplication = 'example-app'\nenvironment = 'production'\nprofile = 'envelope'\n[limits.aws]\nmax_remote_values = 1"
        );
        assert_eq!(
            NativeStoreBindings::parse(&aliases, limits).err(),
            Some(NativeBindingsError::SizeLimit)
        );
        Ok(())
    }
}

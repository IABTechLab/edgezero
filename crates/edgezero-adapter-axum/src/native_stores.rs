//! Native per-ID provider inputs. Deferred opening belongs inside the future body.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "typed inputs and per-kind insertion methods precede source validation and preparation"
)]

use std::collections::{BTreeMap, btree_map::Entry};
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use edgezero_core::app::StoresMetadata;
use edgezero_core::key_value_store::KvHandle;
use edgezero_core::store_registry::{BoundSecretStore, ConfigStoreBinding};
#[cfg(any(
    feature = "aws-appconfig-agent",
    feature = "aws-secrets-manager",
    feature = "aws-dynamodb",
    feature = "celld-config",
    feature = "celld-kv"
))]
use edgezero_core::time::MonotonicClock;
#[cfg(any(
    feature = "aws-appconfig-agent",
    feature = "aws-secrets-manager",
    feature = "aws-dynamodb"
))]
use edgezero_store_aws::AwsPreparation;
#[cfg(not(any(
    feature = "aws-appconfig-agent",
    feature = "aws-secrets-manager",
    feature = "aws-dynamodb"
)))]
use edgezero_store_aws::settings::AwsPreparationLimits;
use edgezero_store_aws::settings::{AgentAccessToken, AppConfigAgentSettings, DynamoDbKvSettings};
#[cfg(any(feature = "celld-config", feature = "celld-kv"))]
use edgezero_store_celld::CelldPreparation;
#[cfg(not(any(feature = "celld-config", feature = "celld-kv")))]
use edgezero_store_celld::settings::CelldPreparationLimits;
use edgezero_store_celld::settings::{CelldAccessToken, CelldConfigSettings, CelldKvSettings};

use crate::config_store_limits::ConfigStoreLimits;
use crate::native_bindings::{
    NativeBindingSource, NativeConfigBinding, NativeKvBinding, NativePreparationPolicies,
    NativeProviderCapabilities, NativeSecretBinding, NativeSourceSelection, NativeStoreBindings,
    NativeSuppliedIds,
};
use crate::run_options::{AxumRunOptions, failure};

/// Closed preparation categories. Never wrap provider errors, settings or payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NativeStorePreparationError {
    #[error("invalid binding")]
    InvalidBinding,
    #[error("duplicate binding")]
    DuplicateBinding,
    #[error("provider unavailable")]
    Unavailable,
    #[error("preparation deadline")]
    Deadline,
    #[error("size limit")]
    SizeLimit,
    #[error("malformed value")]
    Malformed,
}

/// Owned native setup. No `Send` bound; provider handles retain their core bounds.
pub type NativeStoreFuture<H> =
    Pin<Box<dyn Future<Output = Result<H, NativeStorePreparationError>> + 'static>>;

/// Supplied inputs have no built-in payload; validated plans select one per kind.
pub(crate) enum NativeStoreSource<H, B = Infallible> {
    Local,
    Ready(H),
    Prepare(NativeStoreFuture<H>),
    BuiltIn(B),
}

pub(crate) enum NativeConfigSource {
    AppConfigAgent {
        settings: AppConfigAgentSettings,
        token: Option<AgentAccessToken>,
    },
    Celld {
        settings: CelldConfigSettings,
        token: Option<CelldAccessToken>,
    },
}

pub(crate) enum NativeKvSource {
    DynamoDb(DynamoDbKvSettings),
    Celld {
        settings: CelldKvSettings,
        token: Option<CelldAccessToken>,
    },
}

#[cfg(any(feature = "celld-config", feature = "celld-kv"))]
pub(crate) type NativeCelldSession = Option<CelldPreparation>;
#[cfg(not(any(feature = "celld-config", feature = "celld-kv")))]
pub(crate) struct NativeCelldSession;

pub(crate) struct NativePreparationSessions {
    aws: NativeAwsSession,
    celld: NativeCelldSession,
}

#[cfg(any(
    feature = "aws-appconfig-agent",
    feature = "aws-secrets-manager",
    feature = "aws-dynamodb"
))]
pub(crate) type NativeAwsSession = Option<AwsPreparation>;
#[cfg(not(any(
    feature = "aws-appconfig-agent",
    feature = "aws-secrets-manager",
    feature = "aws-dynamodb"
)))]
pub(crate) struct NativeAwsSession;

/// Move-only inputs, separate from cloneable hosting options.
///
/// Deferred construction must be side-effect-free. The runner promises not to poll
/// setup until source validation succeeds; it cannot undo earlier caller-owned work.
#[derive(Default)]
pub struct NativeStoreOverrides {
    config: BTreeMap<String, NativeStoreSource<ConfigStoreBinding>>,
    kv: BTreeMap<String, NativeStoreSource<KvHandle>>,
    secrets: BTreeMap<String, NativeStoreSource<BoundSecretStore>>,
}

impl fmt::Debug for NativeStoreOverrides {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeStoreOverrides")
            .finish_non_exhaustive()
    }
}

impl NativeStoreOverrides {
    /// Supplies an existing config binding, including its own default key.
    ///
    /// # Errors
    /// Rejects a second supplied input for the same config ID.
    #[inline]
    pub fn insert_config<Id: Into<String>>(
        &mut self,
        id: Id,
        binding: ConfigStoreBinding,
    ) -> Result<(), NativeStorePreparationError> {
        insert(
            &mut self.config,
            id.into(),
            NativeStoreSource::Ready(binding),
        )
    }

    /// Defers config opening until the runner has validated all sources.
    ///
    /// # Errors
    /// Rejects a second supplied input for the same config ID.
    #[inline]
    pub fn prepare_config<Id, Setup>(
        &mut self,
        id: Id,
        future: Setup,
    ) -> Result<(), NativeStorePreparationError>
    where
        Id: Into<String>,
        Setup: Future<Output = Result<ConfigStoreBinding, NativeStorePreparationError>> + 'static,
    {
        insert(
            &mut self.config,
            id.into(),
            NativeStoreSource::Prepare(Box::pin(future)),
        )
    }

    /// Supplies an already-open KV handle.
    ///
    /// # Errors
    /// Rejects a second supplied input for the same KV ID.
    #[inline]
    pub fn insert_kv<Id: Into<String>>(
        &mut self,
        id: Id,
        handle: KvHandle,
    ) -> Result<(), NativeStorePreparationError> {
        insert(&mut self.kv, id.into(), NativeStoreSource::Ready(handle))
    }

    /// Defers KV opening until the runner has validated all sources.
    ///
    /// # Errors
    /// Rejects a second supplied input for the same KV ID.
    #[inline]
    pub fn prepare_kv<Id, Setup>(
        &mut self,
        id: Id,
        future: Setup,
    ) -> Result<(), NativeStorePreparationError>
    where
        Id: Into<String>,
        Setup: Future<Output = Result<KvHandle, NativeStorePreparationError>> + 'static,
    {
        insert(
            &mut self.kv,
            id.into(),
            NativeStoreSource::Prepare(Box::pin(future)),
        )
    }

    /// Supplies a secret binding, preserving its explicit namespace.
    ///
    /// # Errors
    /// Rejects a second supplied input for the same secret ID.
    #[inline]
    pub fn insert_secrets<Id: Into<String>>(
        &mut self,
        id: Id,
        binding: BoundSecretStore,
    ) -> Result<(), NativeStorePreparationError> {
        insert(
            &mut self.secrets,
            id.into(),
            NativeStoreSource::Ready(binding),
        )
    }

    /// Defers secret opening until the runner has validated all sources.
    ///
    /// # Errors
    /// Rejects a second supplied input for the same secret ID.
    #[inline]
    pub fn prepare_secrets<Id, Setup>(
        &mut self,
        id: Id,
        future: Setup,
    ) -> Result<(), NativeStorePreparationError>
    where
        Id: Into<String>,
        Setup: Future<Output = Result<BoundSecretStore, NativeStorePreparationError>> + 'static,
    {
        insert(
            &mut self.secrets,
            id.into(),
            NativeStoreSource::Prepare(Box::pin(future)),
        )
    }
}

fn insert<H>(
    map: &mut BTreeMap<String, NativeStoreSource<H>>,
    id: String,
    source: NativeStoreSource<H>,
) -> Result<(), NativeStorePreparationError> {
    match map.entry(id) {
        Entry::Vacant(entry) => {
            entry.insert(source);
            Ok(())
        }
        Entry::Occupied(_) => Err(NativeStorePreparationError::DuplicateBinding),
    }
}

pub(crate) struct NativeSourcePlan {
    pub config_limits: ConfigStoreLimits,
    pub policies: NativePreparationPolicies,
    pub config: BTreeMap<&'static str, NativeStoreSource<ConfigStoreBinding, NativeConfigSource>>,
    pub kv: BTreeMap<&'static str, NativeStoreSource<KvHandle, NativeKvSource>>,
    pub secrets: BTreeMap<&'static str, NativeStoreSource<BoundSecretStore, NativeSecretBinding>>,
}

impl NativeSourcePlan {
    pub(crate) fn validate(
        metadata: StoresMetadata,
        options: &AxumRunOptions,
        overrides: NativeStoreOverrides,
    ) -> anyhow::Result<Self> {
        options.validate_store_overlays(metadata)?;
        let local_defaults = NativeStoreBindings::new(BTreeMap::new(), BTreeMap::new());
        let bindings = options.bindings().document().unwrap_or(&local_defaults);
        let supplied = NativeSuppliedIds {
            config: overrides.config.keys().cloned().collect(),
            kv: overrides.kv.keys().cloned().collect(),
            secrets: overrides.secrets.keys().cloned().collect(),
        };
        let selection = NativeSourceSelection::validate_with_policies(
            metadata,
            bindings,
            &supplied,
            options.env_config(),
            NativeProviderCapabilities {
                appconfig_agent: cfg!(feature = "aws-appconfig-agent"),
                secrets_manager: cfg!(feature = "aws-secrets-manager"),
                dynamodb: cfg!(feature = "aws-dynamodb"),
                celld_config: cfg!(feature = "celld-config"),
                celld_kv: cfg!(feature = "celld-kv"),
            },
            options.bindings_limits(),
            options.preparation_policies(),
        )
        .map_err(|category| failure("sources", "bindings", &category.to_string()))?;
        let plan = Self {
            config_limits: ConfigStoreLimits::from_env(options.env_config())?,
            policies: bindings
                .validate_with_policies(options.bindings_limits(), options.preparation_policies())
                .map_err(|category| failure("sources", "bindings", &category.to_string()))?,
            config: resolve(
                selection.config,
                overrides.config,
                "config",
                |id| match bindings
                    .config()
                    .get(id)
                    .ok_or_else(|| failure("config", id, "invalid binding"))?
                {
                    NativeConfigBinding::AppConfigAgent(settings) => {
                        Ok(NativeConfigSource::AppConfigAgent {
                            token: settings
                                .access_token_env
                                .as_deref()
                                .map(|name| options.agent_token(name))
                                .transpose()?,
                            settings: settings.clone(),
                        })
                    }
                    NativeConfigBinding::Celld(settings) => Ok(NativeConfigSource::Celld {
                        token: settings
                            .access
                            .token_env()
                            .map(|name| options.celld_token(name))
                            .transpose()?,
                        settings: settings.clone(),
                    }),
                },
            )?,
            kv: resolve(selection.kv, overrides.kv, "kv", |id| {
                match bindings
                    .kv()
                    .get(id)
                    .ok_or_else(|| failure("kv", id, "invalid binding"))?
                {
                    NativeKvBinding::DynamoDb(settings) => {
                        Ok(NativeKvSource::DynamoDb(settings.clone()))
                    }
                    NativeKvBinding::Celld(settings) => Ok(NativeKvSource::Celld {
                        token: settings
                            .access
                            .token_env()
                            .map(|name| options.celld_token(name))
                            .transpose()?,
                        settings: settings.clone(),
                    }),
                }
            })?,
            secrets: resolve(selection.secrets, overrides.secrets, "secrets", |id| {
                bindings
                    .secrets()
                    .get(id)
                    .cloned()
                    .ok_or_else(|| failure("secrets", id, "invalid binding"))
            })?,
        };
        options.validate_local_roots(
            plan.config
                .values()
                .any(|source| matches!(source, NativeStoreSource::Local)),
            plan.kv
                .values()
                .any(|source| matches!(source, NativeStoreSource::Local)),
        )?;
        Ok(plan)
    }

    #[cfg_attr(
        not(any(
            feature = "aws-appconfig-agent",
            feature = "aws-secrets-manager",
            feature = "aws-dynamodb",
            feature = "celld-config",
            feature = "celld-kv"
        )),
        expect(
            clippy::unnecessary_wraps,
            reason = "compiled-out sessions retain the fallible preparation boundary"
        )
    )]
    pub(crate) fn sessions(&self) -> anyhow::Result<NativePreparationSessions> {
        // Budgets start together and include time spent preparing the other provider.
        #[cfg(any(
            feature = "aws-appconfig-agent",
            feature = "aws-secrets-manager",
            feature = "aws-dynamodb"
        ))]
        let aws = if self.config.values().any(|source| {
            matches!(
                source,
                NativeStoreSource::BuiltIn(NativeConfigSource::AppConfigAgent { .. })
            )
        }) || self.kv.values().any(|source| {
            matches!(
                source,
                NativeStoreSource::BuiltIn(NativeKvSource::DynamoDb(_))
            )
        }) || self
            .secrets
            .values()
            .any(|source| matches!(source, NativeStoreSource::BuiltIn(_)))
        {
            Some(
                AwsPreparation::new(MonotonicClock::default(), self.policies.aws)
                    .map_err(|category| failure("sources", "bindings", &category.to_string()))?,
            )
        } else {
            None
        };
        #[cfg(not(any(
            feature = "aws-appconfig-agent",
            feature = "aws-secrets-manager",
            feature = "aws-dynamodb"
        )))]
        let aws = {
            let _: AwsPreparationLimits = self.policies.aws;
            NativeAwsSession
        };
        #[cfg(any(feature = "celld-config", feature = "celld-kv"))]
        let celld = if self.config.values().any(|source| {
            matches!(
                source,
                NativeStoreSource::BuiltIn(NativeConfigSource::Celld { .. })
            )
        }) || self.kv.values().any(|source| {
            matches!(
                source,
                NativeStoreSource::BuiltIn(NativeKvSource::Celld { .. })
            )
        }) {
            Some(
                CelldPreparation::new(MonotonicClock::default(), self.policies.celld)
                    .map_err(|category| failure("sources", "bindings", &category.to_string()))?,
            )
        } else {
            None
        };
        #[cfg(not(any(feature = "celld-config", feature = "celld-kv")))]
        let celld = {
            let _: CelldPreparationLimits = self.policies.celld;
            NativeCelldSession
        };
        Ok(NativePreparationSessions { aws, celld })
    }
}

fn resolve<H, B, BuiltIn: Fn(&str) -> anyhow::Result<B>>(
    selection: BTreeMap<&'static str, NativeBindingSource>,
    mut supplied: BTreeMap<String, NativeStoreSource<H>>,
    kind: &str,
    builtin: BuiltIn,
) -> anyhow::Result<BTreeMap<&'static str, NativeStoreSource<H, B>>> {
    selection
        .into_iter()
        .map(|(id, selected)| {
            let source = match selected {
                NativeBindingSource::Local => NativeStoreSource::Local,
                NativeBindingSource::Supplied => match supplied
                    .remove(id)
                    .ok_or_else(|| failure(kind, id, "invalid binding"))?
                {
                    NativeStoreSource::Local => NativeStoreSource::Local,
                    NativeStoreSource::Ready(handle) => NativeStoreSource::Ready(handle),
                    NativeStoreSource::Prepare(future) => NativeStoreSource::Prepare(future),
                    NativeStoreSource::BuiltIn(never) => match never {},
                },
                NativeBindingSource::BuiltIn => NativeStoreSource::BuiltIn(builtin(id)?),
            };
            Ok((id, source))
        })
        .collect()
}

pub(crate) async fn prepare<H, B, Open>(
    source: NativeStoreSource<H, B>,
    kind: &str,
    id: &str,
    local: Open,
) -> anyhow::Result<H>
where
    Open: FnOnce() -> anyhow::Result<H>,
{
    match source {
        NativeStoreSource::Local => local(),
        NativeStoreSource::Ready(handle) => Ok(handle),
        NativeStoreSource::Prepare(future) => future
            .await
            .map_err(|category| failure(kind, id, &category.to_string())),
        NativeStoreSource::BuiltIn(_) => Err(failure(kind, id, "provider not compiled")),
    }
}

impl NativePreparationSessions {
    #[cfg_attr(
        not(any(feature = "aws-appconfig-agent", feature = "celld-config")),
        expect(
            clippy::unused_async,
            reason = "compiled-out config dispatch keeps the asynchronous boundary"
        )
    )]
    pub(crate) async fn prepare_config(
        &mut self,
        builtin: NativeConfigSource,
        id: &str,
    ) -> anyhow::Result<ConfigStoreBinding> {
        match builtin {
            NativeConfigSource::AppConfigAgent { settings, token } => {
                #[cfg(feature = "aws-appconfig-agent")]
                {
                    let handle = self
                        .aws
                        .as_mut()
                        .ok_or_else(|| failure("config", id, "invalid preparation session"))?
                        .prepare_appconfig(&settings, token.as_ref())
                        .await
                        .map_err(|category| failure("config", id, &category.to_string()))?;
                    Ok(ConfigStoreBinding {
                        default_key: settings.default_key,
                        handle,
                    })
                }
                #[cfg(not(feature = "aws-appconfig-agent"))]
                {
                    let _: (
                        AppConfigAgentSettings,
                        Option<AgentAccessToken>,
                        &mut NativeAwsSession,
                    ) = (settings, token, &mut self.aws);
                    Err(failure("config", id, "provider not compiled"))
                }
            }
            NativeConfigSource::Celld { settings, token } => {
                #[cfg(feature = "celld-config")]
                {
                    let handle = self
                        .celld
                        .as_mut()
                        .ok_or_else(|| failure("config", id, "invalid preparation session"))?
                        .prepare_config(&settings, token.as_ref())
                        .await
                        .map_err(|category| failure("config", id, &category.to_string()))?;
                    Ok(ConfigStoreBinding {
                        default_key: settings.default_key,
                        handle,
                    })
                }
                #[cfg(not(feature = "celld-config"))]
                {
                    let _: (
                        CelldConfigSettings,
                        Option<CelldAccessToken>,
                        &mut NativeCelldSession,
                    ) = (settings, token, &mut self.celld);
                    Err(failure("config", id, "provider not compiled"))
                }
            }
        }
    }

    #[cfg_attr(
        not(any(feature = "aws-dynamodb", feature = "celld-kv")),
        expect(
            clippy::unused_async,
            reason = "compiled-out KV dispatch keeps the asynchronous boundary"
        )
    )]
    pub(crate) async fn prepare_kv(
        &mut self,
        builtin: NativeKvSource,
        id: &str,
    ) -> anyhow::Result<KvHandle> {
        match builtin {
            NativeKvSource::DynamoDb(settings) => {
                #[cfg(feature = "aws-dynamodb")]
                {
                    self.aws
                        .as_mut()
                        .ok_or_else(|| failure("kv", id, "invalid preparation session"))?
                        .prepare_dynamodb(&settings)
                        .await
                        .map_err(|category| failure("kv", id, &category.to_string()))
                }
                #[cfg(not(feature = "aws-dynamodb"))]
                {
                    let _: (DynamoDbKvSettings, &mut NativeAwsSession) = (settings, &mut self.aws);
                    Err(failure("kv", id, "provider not compiled"))
                }
            }
            NativeKvSource::Celld { settings, token } => {
                #[cfg(feature = "celld-kv")]
                {
                    self.celld
                        .as_mut()
                        .ok_or_else(|| failure("kv", id, "invalid preparation session"))?
                        .prepare_kv(&settings, token.as_ref())
                        .await
                        .map_err(|category| failure("kv", id, &category.to_string()))
                }
                #[cfg(not(feature = "celld-kv"))]
                {
                    let _: (
                        CelldKvSettings,
                        Option<CelldAccessToken>,
                        &mut NativeCelldSession,
                    ) = (settings, token, &mut self.celld);
                    Err(failure("kv", id, "provider not compiled"))
                }
            }
        }
    }

    #[cfg_attr(
        not(feature = "aws-secrets-manager"),
        expect(
            clippy::unused_async,
            reason = "compiled-out secret dispatch keeps the asynchronous boundary"
        )
    )]
    pub(crate) async fn prepare_secrets(
        &mut self,
        builtin: NativeSecretBinding,
        id: &str,
    ) -> anyhow::Result<BoundSecretStore> {
        #[cfg(feature = "aws-secrets-manager")]
        {
            let NativeSecretBinding::SecretsManager(settings) = builtin;
            let handle = self
                .aws
                .as_mut()
                .ok_or_else(|| failure("secrets", id, "invalid preparation session"))?
                .prepare_secrets(&settings, id)
                .await
                .map_err(|category| failure("secrets", id, &category.to_string()))?;
            Ok(BoundSecretStore::new(handle, id.to_owned()))
        }
        #[cfg(not(feature = "aws-secrets-manager"))]
        {
            let _: (NativeSecretBinding, &mut NativeAwsSession) = (builtin, &mut self.aws);
            Err(failure("secrets", id, "provider not compiled"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{NativeKvSource, NativeSourcePlan, NativeStoreOverrides, NativeStoreSource};
    use crate::{
        native_bindings::{NativeBindingsLimits, NativeStoreBindings},
        run_options::AxumRunOptions,
    };
    use edgezero_core::app::{StoreMetadata, StoresMetadata};
    use std::collections::BTreeMap;

    const CELLD_KV: &str = "version=1\n[kv.state]\nprovider='celld-kv'\nendpoint='http://127.0.0.1:8788'\nstore='state-fixture'\n[kv.state.access]\nmode='trusted-network'\n";
    const CELLD_CONFIG: &str = "version=1\n[config.app]\nprovider='celld-config'\nendpoint='http://127.0.0.1:8788'\ndefault_key='settings'\n[config.app.access]\nmode='trusted-network'\n[config.app.documents.settings]\nstore='config-fixture'\nkey='settings'\n";

    #[test]
    fn celld_variants_check_independent_capabilities_without_local_roots() {
        for (input, metadata, compiled) in [
            (
                CELLD_KV,
                StoresMetadata {
                    config: None,
                    kv: Some(StoreMetadata {
                        ids: &["state"],
                        default: "state",
                    }),
                    secrets: None,
                },
                cfg!(feature = "celld-kv"),
            ),
            (
                CELLD_CONFIG,
                StoresMetadata {
                    config: Some(StoreMetadata {
                        ids: &["app"],
                        default: "app",
                    }),
                    kv: None,
                    secrets: None,
                },
                cfg!(feature = "celld-config"),
            ),
        ] {
            let document = NativeStoreBindings::parse(input, NativeBindingsLimits::default())
                .expect("settings only");
            let options = AxumRunOptions::new("127.0.0.1:8787".parse().expect("address"))
                .expect("options")
                .with_store_bindings(document)
                .expect("selected");
            let plan =
                NativeSourcePlan::validate(metadata, &options, NativeStoreOverrides::default());
            if compiled {
                let selected = plan.expect("no local roots required");
                let sessions = selected.sessions().expect("provider sessions");
                #[cfg(any(feature = "celld-config", feature = "celld-kv"))]
                assert!(sessions.celld.is_some());
                #[cfg(any(
                    feature = "aws-appconfig-agent",
                    feature = "aws-secrets-manager",
                    feature = "aws-dynamodb"
                ))]
                assert!(sessions.aws.is_none());
                let _: super::NativePreparationSessions = sessions;
            } else {
                let error = plan.err().expect("selected feature absent");
                assert!(error.to_string().contains("provider not compiled"));
            }
        }
    }

    #[test]
    fn local_ready_and_deferred_select_no_provider_sessions() {
        use crate::key_value_store::PersistentKvStore;
        use edgezero_core::key_value_store::KvHandle;
        use std::cell::Cell;
        use std::rc::Rc;
        use std::sync::Arc;
        let root = tempfile::tempdir().expect("root");
        let handle = KvHandle::new(Arc::new(
            PersistentKvStore::new(root.path().join("supplied.redb")).expect("caller-owned store"),
        ));
        let mut overrides = NativeStoreOverrides::default();
        overrides.insert_kv("ready", handle.clone()).expect("ready");
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        overrides
            .prepare_kv("deferred", async move {
                observed.set(1);
                Ok(handle)
            })
            .expect("deferred");
        let options = AxumRunOptions::new("127.0.0.1:8787".parse().expect("address"))
            .expect("options")
            .with_data_dir(root.path())
            .expect("root");
        let metadata = StoresMetadata {
            config: None,
            kv: Some(StoreMetadata {
                ids: &["local", "ready", "deferred"],
                default: "local",
            }),
            secrets: None,
        };
        let plan =
            NativeSourcePlan::validate(metadata, &options, overrides).expect("valid sources");
        assert_eq!(polls.get(), 0);
        let sessions = plan.sessions().expect("no sessions");
        #[cfg(any(feature = "celld-config", feature = "celld-kv"))]
        assert!(sessions.celld.is_none());
        #[cfg(any(
            feature = "aws-appconfig-agent",
            feature = "aws-secrets-manager",
            feature = "aws-dynamodb"
        ))]
        assert!(sessions.aws.is_none());
        let _: super::NativePreparationSessions = sessions;
    }

    #[cfg(all(feature = "aws-appconfig-agent", feature = "celld-kv"))]
    #[test]
    fn sessions_select_actual_aws_celld_and_mixed_variants() {
        let aws = "version=1\n[config.app]\nprovider='aws-appconfig-agent'\nendpoint='http://127.0.0.1:2772'\ndefault_key='settings'\n[config.app.documents.settings]\napplication='example'\nenvironment='production'\nprofile='settings'\n";
        for mixed in [false, true] {
            let input = if mixed {
                format!("{aws}{}", CELLD_KV.replace("version=1\n", ""))
            } else {
                aws.to_owned()
            };
            let options = AxumRunOptions::new("127.0.0.1:8787".parse().expect("address"))
                .expect("options")
                .with_store_bindings(
                    NativeStoreBindings::parse(&input, NativeBindingsLimits::default())
                        .expect("bindings"),
                )
                .expect("selection");
            let metadata = StoresMetadata {
                config: Some(StoreMetadata {
                    ids: &["app"],
                    default: "app",
                }),
                kv: mixed.then_some(StoreMetadata {
                    ids: &["state"],
                    default: "state",
                }),
                secrets: None,
            };
            let sessions =
                NativeSourcePlan::validate(metadata, &options, NativeStoreOverrides::default())
                    .expect("plan")
                    .sessions()
                    .expect("sessions");
            assert!(sessions.aws.is_some());
            assert_eq!(sessions.celld.is_some(), mixed);
        }
    }

    #[cfg(all(feature = "celld-config", feature = "celld-kv"))]
    #[test]
    fn typed_celld_sources_retain_distinct_explicit_tokens() {
        use super::NativeConfigSource;
        let input = format!("{CELLD_CONFIG}{}", CELLD_KV.replace("version=1\n", ""))
            .replace("http://127.0.0.1:8788", "https://unreachable.invalid")
            .replace(
                "[config.app.access]\nmode='trusted-network'",
                "[config.app.access]\nmode='authenticated'\ntoken_env='CONFIG_TOKEN'",
            )
            .replace(
                "[kv.state.access]\nmode='trusted-network'",
                "[kv.state.access]\nmode='authenticated'\ntoken_env='KV_TOKEN'",
            );
        let config_token = "config-token-012345678901234567890123456789";
        let kv_token = "kv-token-0123456789012345678901234567890123";
        let options =
            AxumRunOptions::from_vars([("CONFIG_TOKEN", config_token), ("KV_TOKEN", kv_token)])
                .expect("explicit values")
                .with_store_bindings(
                    NativeStoreBindings::parse(&input, NativeBindingsLimits::default())
                        .expect("bindings"),
                )
                .expect("selection");
        let metadata = StoresMetadata {
            config: Some(StoreMetadata {
                ids: &["app"],
                default: "app",
            }),
            kv: Some(StoreMetadata {
                ids: &["state"],
                default: "state",
            }),
            secrets: None,
        };
        let plan = NativeSourcePlan::validate(metadata, &options, NativeStoreOverrides::default())
            .expect("captured plan");
        let Some(NativeStoreSource::BuiltIn(NativeConfigSource::Celld {
            token: Some(config),
            ..
        })) = plan.config.get("app")
        else {
            panic!("typed config token")
        };
        let Some(NativeStoreSource::BuiltIn(NativeKvSource::Celld {
            token: Some(kv), ..
        })) = plan.kv.get("state")
        else {
            panic!("typed KV token")
        };
        assert_eq!(config.expose(), config_token);
        assert_eq!(kv.expose(), kv_token);
    }

    #[test]
    fn later_celld_token_or_capability_failure_never_polls_or_opens_local_stores() {
        use std::cell::Cell;
        use std::fs::read_dir;
        use std::rc::Rc;
        let root = tempfile::tempdir().expect("root");
        let input = CELLD_KV
            .replace("http://127.0.0.1:8788", "https://unreachable.invalid")
            .replace(
                "mode='trusted-network'",
                "mode='authenticated'\ntoken_env='ABSENT_TOKEN'",
            );
        let options = AxumRunOptions::new("127.0.0.1:8787".parse().expect("address"))
            .expect("options")
            .with_config_dir(root.path())
            .expect("config root")
            .with_data_dir(root.path())
            .expect("data root")
            .with_store_bindings(
                NativeStoreBindings::parse(&input, NativeBindingsLimits::default())
                    .expect("bindings"),
            )
            .expect("selection");
        let mut overrides = NativeStoreOverrides::default();
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        overrides
            .prepare_config("deferred", async move {
                observed.set(1);
                Err(super::NativeStorePreparationError::Unavailable)
            })
            .expect("non-Send future");
        let metadata = StoresMetadata {
            config: Some(StoreMetadata {
                ids: &["local", "deferred"],
                default: "local",
            }),
            kv: Some(StoreMetadata {
                ids: &["state", "local"],
                default: "state",
            }),
            secrets: None,
        };
        let error = NativeSourcePlan::validate(metadata, &options, overrides)
            .err()
            .expect("invalid selected source");
        assert_eq!(polls.get(), 0);
        assert_eq!(read_dir(root.path()).expect("root").count(), 0);
        assert_eq!(error.chain().count(), 1);
        assert!(!format!("{error:?}").contains("unreachable.invalid"));
        if cfg!(feature = "celld-kv") {
            assert!(error.to_string().contains("celld token"));
        } else {
            assert!(error.to_string().contains("provider not compiled"));
        }
    }

    #[cfg(feature = "aws-dynamodb")]
    #[test]
    fn dynamodb_local_ready_and_non_send_deferred_sources_validate_without_polling() {
        use crate::key_value_store::PersistentKvStore;
        use edgezero_core::key_value_store::KvHandle;
        use std::cell::Cell;
        use std::fs::read_dir;
        use std::rc::Rc;
        use std::sync::Arc;
        let directory = tempfile::tempdir().unwrap();
        // The caller owns this already-open handle; validation opens no other store.
        let supplied = KvHandle::new(Arc::new(
            PersistentKvStore::new(directory.path().join("supplied.redb")).unwrap(),
        ));
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let deferred_handle = supplied.clone();
        let mut overrides = NativeStoreOverrides::default();
        overrides.insert_kv("supplied", supplied).unwrap();
        overrides
            .prepare_kv("deferred", async move {
                observed.set(1);
                Ok(deferred_handle)
            })
            .unwrap();
        let bindings = NativeStoreBindings::parse("version=1\n[kv.remote]\nprovider='aws-dynamodb'\nregion='us-east-1'\ntable='edgezero-kv'\nnamespace='scope'", NativeBindingsLimits::default()).unwrap();
        let options = AxumRunOptions::new("127.0.0.1:8787".parse().unwrap())
            .unwrap()
            .with_data_dir(directory.path())
            .unwrap()
            .with_store_bindings(bindings)
            .unwrap();
        let metadata = StoresMetadata {
            kv: Some(StoreMetadata {
                ids: &["remote", "local", "supplied", "deferred"],
                default: "remote",
            }),
            config: None,
            secrets: None,
        };
        let validated = NativeSourcePlan::validate(metadata, &options, overrides).unwrap();
        assert!(matches!(
            validated.kv.get("remote"),
            Some(NativeStoreSource::BuiltIn(NativeKvSource::DynamoDb(_)))
        ));
        assert!(matches!(
            validated.kv.get("local"),
            Some(NativeStoreSource::Local)
        ));
        assert!(matches!(
            validated.kv.get("supplied"),
            Some(NativeStoreSource::Ready(_))
        ));
        assert!(matches!(
            validated.kv.get("deferred"),
            Some(NativeStoreSource::Prepare(_))
        ));
        assert_eq!(polls.get(), 0);
        assert_eq!(read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn dynamodb_only_needs_no_local_root_and_checks_actual_feature() {
        let bindings = NativeStoreBindings::parse("version=1\n[kv.state]\nprovider='aws-dynamodb'\nregion='us-east-1'\ntable='edgezero-kv'\nnamespace='scope'", NativeBindingsLimits::default()).unwrap();
        let options = AxumRunOptions::new("127.0.0.1:8787".parse().unwrap())
            .unwrap()
            .with_store_bindings(bindings)
            .unwrap();
        let metadata = StoresMetadata {
            kv: Some(StoreMetadata {
                ids: &["state"],
                default: "state",
            }),
            config: None,
            secrets: None,
        };
        let plan = NativeSourcePlan::validate(metadata, &options, NativeStoreOverrides::default());
        if cfg!(feature = "aws-dynamodb") {
            let validated_plan = plan.unwrap();
            assert!(matches!(
                validated_plan.kv.get("state"),
                Some(NativeStoreSource::BuiltIn(NativeKvSource::DynamoDb(_)))
            ));
        } else {
            assert!(plan.is_err());
        }
        let empty = NativeStoreBindings::new(BTreeMap::new(), BTreeMap::new());
        assert!(empty.kv().is_empty());
    }
}

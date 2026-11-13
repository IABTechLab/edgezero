//! One startup's aggregate clock, accounting and authorization-scoped caches.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "group public session operations before private accounting and feature-gated preparation"
)]

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "config")]
use edgezero_core::config_store::{ConfigStoreHandle, ConfigValue};
#[cfg(feature = "kv")]
use edgezero_core::key_value_store::KvHandle;
use edgezero_core::time::{Deadline, MonotonicClock};
#[cfg(feature = "config")]
use reqwest::Method;
#[cfg(feature = "config")]
use std::collections::{BTreeMap, HashMap};

use crate::PreparationError;
#[cfg(feature = "config")]
use crate::client::ResponseKind;
use crate::client::ScopedClient;
#[cfg(feature = "config")]
use crate::config::ConfigSnapshot;
#[cfg(feature = "kv")]
use crate::kv::CelldKvStore;
#[cfg(feature = "config")]
use crate::settings::CelldConfigSettings;
#[cfg(feature = "kv")]
use crate::settings::CelldKvSettings;
use crate::settings::{CelldAccess, CelldAccessToken, CelldPreparationLimits, endpoint};

/// Aggregate counters, not process memory measurements. Metadata counts bounded
/// identifier storage plus conservative per-record overhead. Staging includes at
/// least two document-sized buffers, but excludes allocator and HTTP chunk overhead.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PreparationStats {
    pub mappings: u64,
    pub unique_retained_payload_bytes: u64,
    pub metadata_bytes: u64,
    pub peak_transient_staging_bytes: u64,
}

/// A session is permanently poisoned after any failed or cancelled preparation.
/// Handles already returned are immutable/config or independently live/KV.
pub struct CelldPreparation {
    clients: Vec<(ClientIdentity, Arc<ScopedClient>)>,
    clock: MonotonicClock,
    deadline: Deadline,
    limits: CelldPreparationLimits,
    poisoned: bool,
    stats: PreparationStats,
    #[cfg(feature = "config")]
    values: HashMap<TargetIdentity, ConfigValue>,
}

impl fmt::Debug for CelldPreparation {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CelldPreparation")
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

impl CelldPreparation {
    /// Starts one shared startup deadline, before any client construction.
    ///
    /// # Errors
    /// Rejects invalid limits and unrepresentable deadlines.
    #[inline]
    pub fn new(
        clock: MonotonicClock,
        limits: CelldPreparationLimits,
    ) -> Result<Self, PreparationError> {
        limits.validate()?;
        let deadline = clock
            .now()
            .checked_add(Duration::from_millis(limits.preparation_timeout_ms))
            .map(Deadline::at_instant)
            .ok_or(PreparationError::InvalidLimits)?;
        Ok(Self {
            clients: Vec::new(),
            clock,
            deadline,
            limits,
            poisoned: false,
            stats: PreparationStats::default(),
            #[cfg(feature = "config")]
            values: HashMap::new(),
        })
    }

    /// Fetches all mapped complete UTF-8 documents, with no stale/local fallback.
    ///
    /// # Errors
    /// Returns a closed category on validation, quota, deadline or protocol failure.
    /// A failure poisons the session, including failed validation.
    #[cfg(feature = "config")]
    #[inline]
    pub async fn prepare_config(
        &mut self,
        settings: &CelldConfigSettings,
        token: Option<&CelldAccessToken>,
    ) -> Result<ConfigStoreHandle, PreparationError> {
        self.begin()?;
        let result = self.config_inner(settings, token).await;
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }

    /// Builds and accounts a live handle. This intentionally performs no remote
    /// probe and makes no claim about connectivity or any operation permission.
    /// Live calls start fresh deadlines, independent of this session's deadline.
    ///
    /// # Errors
    /// Returns a closed category on invalid input, quota or client construction.
    #[cfg(feature = "kv")]
    #[inline]
    #[expect(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        reason = "the agreed preparation API is uniformly async; live KV setup deliberately performs no remote probe"
    )]
    pub async fn prepare_kv(
        &mut self,
        settings: &CelldKvSettings,
        token: Option<&CelldAccessToken>,
    ) -> Result<KvHandle, PreparationError> {
        self.begin()?;
        let result = self.kv_inner(settings, token);
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }

    /// Safe aggregate counters; no resource or authorization identifiers.
    #[inline]
    #[must_use]
    pub const fn stats(&self) -> PreparationStats {
        self.stats
    }

    fn begin(&mut self) -> Result<(), PreparationError> {
        if self.poisoned {
            return Err(PreparationError::InvalidBinding);
        }
        self.poisoned = true;
        self.check()
    }

    fn check(&self) -> Result<(), PreparationError> {
        if self.deadline.is_expired_at(self.clock.now()) {
            return Err(PreparationError::Deadline);
        }
        Ok(())
    }

    fn charge_metadata(&mut self, bytes: usize) -> Result<(), PreparationError> {
        self.stats.metadata_bytes = u64::try_from(bytes)
            .ok()
            .and_then(|count| self.stats.metadata_bytes.checked_add(count))
            .filter(|total| *total <= self.limits.max_metadata_bytes)
            .ok_or(PreparationError::SizeLimit)?;
        Ok(())
    }

    fn charge_mapping(&mut self, sizes: &[usize]) -> Result<(), PreparationError> {
        self.stats.mappings = self
            .stats
            .mappings
            .checked_add(1)
            .filter(|count| *count <= self.limits.max_remote_values)
            .ok_or(PreparationError::SizeLimit)?;
        // Covers map nodes, strings in snapshot/cache targets and temporary target
        // lists conservatively. This is a logical metadata quota, not RSS.
        let bytes = sizes.iter().try_fold(512_usize, |total, size| {
            size.checked_mul(3)
                .and_then(|scaled_size| total.checked_add(scaled_size))
                .ok_or(PreparationError::SizeLimit)
        })?;
        self.charge_metadata(bytes)
    }

    fn client(
        &mut self,
        endpoint_text: &str,
        access: &CelldAccess,
        token: Option<&CelldAccessToken>,
    ) -> Result<(usize, Arc<ScopedClient>), PreparationError> {
        if access.token_env().is_some() != token.is_some() {
            return Err(PreparationError::Credentials);
        }
        let endpoint = endpoint(endpoint_text, access)?;
        let identity = ClientIdentity {
            endpoint: endpoint.as_str().to_owned(),
            access: access.clone(),
            token: token.map(|credential| credential.expose().to_owned()),
        };
        if let Some((index, (_, client))) = self
            .clients
            .iter()
            .enumerate()
            .find(|(_, (key, _))| *key == identity)
        {
            return Ok((index, Arc::clone(client)));
        }
        if self.clients.len() >= 128 {
            return Err(PreparationError::SizeLimit);
        }
        let metadata = [
            identity.endpoint.len(),
            access.token_env().map_or(0, str::len),
            token.map_or(0, |credential| credential.expose().len()),
        ]
        .into_iter()
        .try_fold(1_024_usize, |total, size| {
            size.checked_mul(3)
                .and_then(|scaled_size| total.checked_add(scaled_size))
                .ok_or(PreparationError::SizeLimit)
        })?;
        self.charge_metadata(metadata)?;
        let client = Arc::new(ScopedClient::new(
            endpoint,
            token,
            self.clock.clone(),
            self.limits,
        )?);
        self.check()?;
        let index = self.clients.len();
        self.clients.push((identity, Arc::clone(&client)));
        Ok((index, client))
    }

    #[cfg(feature = "config")]
    async fn config_inner(
        &mut self,
        settings: &CelldConfigSettings,
        token: Option<&CelldAccessToken>,
    ) -> Result<ConfigStoreHandle, PreparationError> {
        settings.validate()?;
        // Charge every logical mapping before deduplication and before the first
        // client/fetch, including mappings aliasing an already retained document.
        for (key, document) in &settings.documents {
            self.charge_mapping(&[
                key.len(),
                settings.endpoint.len(),
                document.store.len(),
                document.key.len(),
            ])?;
        }
        let (client_id, client) = self.client(&settings.endpoint, &settings.access, token)?;
        let mut entries = BTreeMap::new();
        for (key, document) in &settings.documents {
            self.check()?;
            let target = TargetIdentity {
                client_id,
                store: document.store.clone(),
                key: document.key.clone(),
            };
            let value = if let Some(value) = self.values.get(&target) {
                value.clone()
            } else {
                let remaining = self
                    .limits
                    .max_snapshot_bytes
                    .checked_sub(self.stats.unique_retained_payload_bytes)
                    .ok_or(PreparationError::SizeLimit)?;
                let cap = usize::try_from(remaining.min(self.limits.max_config_document_bytes))
                    .map_err(|_error| PreparationError::InvalidLimits)?;
                let mut url = client.url(&document.store, "value")?;
                url.query_pairs_mut().append_pair("key", &document.key);
                let deadline =
                    Deadline::at_instant(self.deadline.instant().min(client.deadline()?.instant()));
                let body = client
                    .execute(
                        Method::GET,
                        url,
                        None,
                        cap,
                        deadline,
                        ResponseKind::Document,
                    )
                    .await?
                    .ok_or(PreparationError::Missing)?;
                self.check()?;
                let bytes =
                    u64::try_from(body.len()).map_err(|_error| PreparationError::SizeLimit)?;
                self.stats.peak_transient_staging_bytes = self
                    .stats
                    .peak_transient_staging_bytes
                    .max(bytes.checked_mul(2).ok_or(PreparationError::SizeLimit)?);
                let text = String::from_utf8(body).map_err(|_error| PreparationError::Malformed);
                self.check()?;
                let value = ConfigValue::from(text?);
                self.stats.unique_retained_payload_bytes = self
                    .stats
                    .unique_retained_payload_bytes
                    .checked_add(bytes)
                    .filter(|total| *total <= self.limits.max_snapshot_bytes)
                    .ok_or(PreparationError::SizeLimit)?;
                self.values.insert(target, value.clone());
                value
            };
            entries.insert(key.clone(), value);
        }
        self.check()?;
        Ok(ConfigStoreHandle::new(Arc::new(ConfigSnapshot::new(
            entries,
        ))))
    }

    #[cfg(feature = "kv")]
    fn kv_inner(
        &mut self,
        settings: &CelldKvSettings,
        token: Option<&CelldAccessToken>,
    ) -> Result<KvHandle, PreparationError> {
        settings.validate()?;
        self.charge_mapping(&[settings.endpoint.len(), settings.store.len()])?;
        let (_, client) = self.client(&settings.endpoint, &settings.access, token)?;
        let endpoint = endpoint(&settings.endpoint, &settings.access)?;
        self.check()?;
        Ok(KvHandle::new(Arc::new(CelldKvStore::new(
            client,
            endpoint,
            settings.store.clone(),
            self.limits,
        ))))
    }
}

// Session policy is immutable, so it is implicitly part of every cache identity.
// Access includes the variable reference and remote-HTTP policy, not just bearer
// bytes. Equal tokens selected through different references remain separate.
#[derive(Eq, PartialEq)]
struct ClientIdentity {
    endpoint: String,
    access: CelldAccess,
    token: Option<String>,
}

impl fmt::Debug for ClientIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientIdentity").finish_non_exhaustive()
    }
}

#[cfg(feature = "config")]
#[derive(Eq, Hash, PartialEq)]
struct TargetIdentity {
    client_id: usize,
    store: String,
    key: String,
}

#[cfg(feature = "config")]
impl fmt::Debug for TargetIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TargetIdentity").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use edgezero_core::time::MonotonicClock;

    use super::CelldPreparation;
    use crate::PreparationError;
    use crate::settings::{CelldAccess, CelldAccessToken, CelldPreparationLimits};

    #[test]
    fn cache_identity_includes_credentials_reference_and_complete_access_policy() {
        let mut preparation =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .unwrap();
        let access = CelldAccess::Authenticated {
            token_env: "FIRST".to_owned(),
        };
        let token = CelldAccessToken::new("a".repeat(32)).unwrap();
        let (first, _) = preparation
            .client("https://resource-sentinel.invalid", &access, Some(&token))
            .unwrap();
        let (same, _) = preparation
            .client("https://resource-sentinel.invalid/", &access, Some(&token))
            .unwrap();
        assert_eq!(first, same);
        let other = CelldAccessToken::new("b".repeat(32)).unwrap();
        assert_ne!(
            preparation
                .client("https://resource-sentinel.invalid", &access, Some(&other))
                .unwrap()
                .0,
            first
        );
        let other_reference = CelldAccess::Authenticated {
            token_env: "SECOND".to_owned(),
        };
        assert_ne!(
            preparation
                .client(
                    "https://resource-sentinel.invalid",
                    &other_reference,
                    Some(&token)
                )
                .unwrap()
                .0,
            first
        );
        let trusted = CelldAccess::TrustedNetwork {
            allow_remote_http: false,
        };
        let trusted_opt_in = CelldAccess::TrustedNetwork {
            allow_remote_http: true,
        };
        let (trusted_id, _) = preparation
            .client("https://resource-sentinel.invalid", &trusted, None)
            .unwrap();
        assert_ne!(
            preparation
                .client("https://resource-sentinel.invalid", &trusted_opt_in, None)
                .unwrap()
                .0,
            trusted_id
        );
        assert!(matches!(
            preparation.client("https://resource-sentinel.invalid", &trusted, Some(&token)),
            Err(PreparationError::Credentials)
        ));
        assert!(matches!(
            preparation.client("https://resource-sentinel.invalid", &access, None),
            Err(PreparationError::Credentials)
        ));
        assert!(!format!("{preparation:?} {:?}", preparation.clients[0].0).contains("sentinel"));
        assert_eq!(preparation.stats().unique_retained_payload_bytes, 0);
    }

    #[test]
    fn client_cache_and_metadata_are_finite_for_direct_callers() {
        let access = CelldAccess::TrustedNetwork {
            allow_remote_http: false,
        };
        let mut preparation =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .unwrap();
        for index in 0..128_u16 {
            preparation
                .client(
                    &format!("https://127.0.0.1:{}", 1_000 + index),
                    &access,
                    None,
                )
                .unwrap();
        }
        assert!(matches!(
            preparation.client("https://127.0.0.1:2000", &access, None),
            Err(PreparationError::SizeLimit)
        ));
        assert_eq!(preparation.clients.len(), 128);
        let limits = CelldPreparationLimits {
            max_metadata_bytes: 1,
            ..CelldPreparationLimits::default()
        };
        let mut metadata_limited =
            CelldPreparation::new(MonotonicClock::default(), limits).unwrap();
        assert!(matches!(
            metadata_limited.client("https://127.0.0.1", &access, None),
            Err(PreparationError::SizeLimit)
        ));
        assert!(metadata_limited.clients.is_empty());
    }
}

#[cfg(test)]
#[cfg(feature = "config")]
mod config_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use edgezero_core::time::{MonotonicClock, MonotonicInstant};
    use tokio::time::sleep;

    use super::CelldPreparation;
    use crate::PreparationError;
    use crate::settings::{
        CelldAccess, CelldConfigSettings, CelldDocument, CelldPreparationLimits,
    };
    use crate::test_support::{Fixture, Reply};

    fn settings(fixture: &Fixture, mappings: &[(&str, &str)]) -> CelldConfigSettings {
        CelldConfigSettings {
            endpoint: fixture.endpoint().to_owned(),
            access: CelldAccess::TrustedNetwork {
                allow_remote_http: false,
            },
            default_key: mappings
                .first()
                .expect("config fixture requires a default mapping")
                .0
                .to_owned(),
            documents: mappings
                .iter()
                .map(|(local, remote)| {
                    (
                        (*local).to_owned(),
                        CelldDocument {
                            store: "config".to_owned(),
                            key: (*remote).to_owned(),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn exact_bytes_dedup_and_restart_adoption_without_request_time_io() {
        let fixture = Fixture::new(vec![]).await;
        let text = "  {\"schema\": 1, \"payload\": {\"a\": 1}}\n";
        fixture.seed("config", "remote", text.as_bytes()).await;
        fixture.seed("config", "empty", b"").await;
        let settings = settings(
            &fixture,
            &[("first", "remote"), ("alias", "remote"), ("empty", "empty")],
        );
        let mut preparation =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .unwrap();
        let snapshot = preparation.prepare_config(&settings, None).await.unwrap();
        assert_eq!(fixture.count().await, 2);
        assert_eq!(preparation.stats().mappings, 3);
        assert_eq!(
            preparation.stats().unique_retained_payload_bytes,
            u64::try_from(text.len()).unwrap()
        );
        assert!(
            preparation.stats().peak_transient_staging_bytes
                >= u64::try_from(text.len() * 2).unwrap()
        );
        assert_eq!(snapshot.get("first").await.unwrap().unwrap().as_str(), text);
        assert_eq!(snapshot.get("alias").await.unwrap().unwrap().as_str(), text);
        assert_eq!(snapshot.get("empty").await.unwrap().unwrap().as_str(), "");
        assert!(snapshot.get("unmapped").await.unwrap().is_none());
        fixture.seed("config", "remote", b"new revision").await;
        let same_session = preparation.prepare_config(&settings, None).await.unwrap();
        assert_eq!(
            same_session.get("first").await.unwrap().unwrap().as_str(),
            text
        );
        assert_eq!(preparation.stats().mappings, 6);
        assert_eq!(fixture.count().await, 2);
        drop(preparation);
        assert_eq!(snapshot.get("first").await.unwrap().unwrap().as_str(), text);
        let mut fresh =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .unwrap();
        let adopted = fresh.prepare_config(&settings, None).await.unwrap();
        assert_eq!(
            adopted.get("first").await.unwrap().unwrap().as_str(),
            "new revision"
        );
        assert_eq!(fixture.count().await, 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mapping_and_metadata_caps_are_precharged_before_dedup_or_first_fetch() {
        for limits in [
            CelldPreparationLimits {
                max_remote_values: 1,
                ..CelldPreparationLimits::default()
            },
            CelldPreparationLimits {
                max_metadata_bytes: 1,
                ..CelldPreparationLimits::default()
            },
        ] {
            let fixture = Fixture::new(vec![]).await;
            fixture.seed("config", "shared", b"v").await;
            let settings = settings(&fixture, &[("a", "shared"), ("b", "shared")]);
            let mut preparation = CelldPreparation::new(MonotonicClock::default(), limits).unwrap();
            assert!(matches!(
                preparation.prepare_config(&settings, None).await,
                Err(PreparationError::SizeLimit)
            ));
            assert_eq!(fixture.count().await, 0);
            assert!(matches!(
                preparation.prepare_config(&settings, None).await,
                Err(PreparationError::InvalidBinding)
            ));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retained_quota_clips_transfer_and_failure_permanently_poisons_session() {
        let fixture = Fixture::new(vec![]).await;
        fixture.seed("config", "one", b"123").await;
        fixture.seed("config", "two", b"456").await;
        let settings = settings(&fixture, &[("a", "one"), ("b", "two")]);
        let limits = CelldPreparationLimits {
            max_config_document_bytes: 4,
            max_value_bytes: 4,
            max_snapshot_bytes: 5,
            ..CelldPreparationLimits::default()
        };
        let mut preparation = CelldPreparation::new(MonotonicClock::default(), limits).unwrap();
        assert!(matches!(
            preparation.prepare_config(&settings, None).await,
            Err(PreparationError::SizeLimit)
        ));
        assert_eq!(preparation.stats().unique_retained_payload_bytes, 3);
        assert_eq!(fixture.count().await, 2);
        fixture.seed("config", "two", b"4").await;
        assert!(matches!(
            preparation.prepare_config(&settings, None).await,
            Err(PreparationError::InvalidBinding)
        ));
        assert_eq!(fixture.count().await, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn required_misses_invalid_utf8_and_late_invalid_mapping_publish_nothing() {
        for (bytes, expected) in [
            (Some(vec![255]), PreparationError::Malformed),
            (None, PreparationError::Missing),
        ] {
            let fixture = Fixture::new(vec![]).await;
            if let Some(payload) = bytes {
                fixture.seed("config", "remote", &payload).await;
            }
            let settings = settings(&fixture, &[("a", "remote")]);
            let mut preparation =
                CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                    .unwrap();
            assert!(
                matches!(preparation.prepare_config(&settings, None).await, Err(error) if error == expected)
            );
            assert!(matches!(
                preparation.prepare_config(&settings, None).await,
                Err(PreparationError::InvalidBinding)
            ));
            assert_eq!(fixture.count().await, 1);
        }
        let fixture = Fixture::new(vec![]).await;
        let settings = settings(&fixture, &[("a", "valid"), ("b", "invalid\n")]);
        let mut preparation =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .unwrap();
        assert!(matches!(
            preparation.prepare_config(&settings, None).await,
            Err(PreparationError::InvalidResource)
        ));
        assert_eq!(fixture.count().await, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn aggregate_deadline_never_restarts_between_bindings() {
        let fixture = Fixture::new(vec![]).await;
        fixture.seed("config", "remote", b"v").await;
        let start = MonotonicInstant::now();
        let elapsed = Arc::new(AtomicU64::new(0));
        let ticks = Arc::clone(&elapsed);
        let clock = MonotonicClock::new(move || {
            start
                .checked_add(Duration::from_millis(ticks.load(Ordering::SeqCst)))
                .unwrap()
        });
        let mut preparation =
            CelldPreparation::new(clock, CelldPreparationLimits::default()).unwrap();
        let settings = settings(&fixture, &[("a", "remote")]);
        preparation.prepare_config(&settings, None).await.unwrap();
        elapsed.store(30_000, Ordering::SeqCst);
        assert!(matches!(
            preparation.prepare_config(&settings, None).await,
            Err(PreparationError::Deadline)
        ));
        assert_eq!(fixture.count().await, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    #[expect(
        clippy::integer_division_remainder_used,
        reason = "Tokio select generates bounded branch-index arithmetic"
    )]
    async fn cancellation_also_poisons_unpublished_partial_preparation() {
        let fixture = Fixture::new(vec![Reply::Stall]).await;
        let settings = settings(&fixture, &[("a", "remote")]);
        let mut preparation =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .unwrap();
        let mut pending = Box::pin(preparation.prepare_config(&settings, None));
        tokio::select! {
            biased;
            _result = pending.as_mut() => panic!("stalled preparation completed before cancellation"),
            () = async {
                while fixture.count().await == 0 {
                    sleep(Duration::from_millis(1)).await;
                }
            } => {}
            () = sleep(Duration::from_secs(3)) => panic!("request admission deadline"),
        }
        drop(pending);
        assert!(matches!(
            preparation.prepare_config(&settings, None).await,
            Err(PreparationError::InvalidBinding)
        ));
        assert_eq!(fixture.count().await, 1);
    }
}

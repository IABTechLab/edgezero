//! One startup's aggregate clock, accounting and authorization-scoped client cache.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "group public session operations before private accounting and feature-gated preparation"
)]

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use edgezero_core::key_value_store::KvHandle;
#[cfg(feature = "records")]
use edgezero_core::record_store::RecordLayout;
use edgezero_core::time::{Deadline, MonotonicClock};

use crate::PreparationError;
use crate::client::ScopedClient;
use crate::kv::CelldKvStore;
#[cfg(feature = "records")]
use crate::records::CelldRecordKvStore;
use crate::settings::{
    CelldAccess, CelldAccessToken, CelldKvSettings, CelldPreparationLimits, endpoint,
};

/// Aggregate counters, not process memory measurements. Metadata counts bounded
/// identifier storage plus conservative per-binding overhead.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PreparationStats {
    pub mappings: u64,
    pub metadata_bytes: u64,
}

/// A session is permanently poisoned after any failed or cancelled preparation.
/// Handles already returned stay independently live.
pub struct CelldPreparation {
    clients: Vec<(ClientIdentity, Arc<ScopedClient>)>,
    clock: MonotonicClock,
    deadline: Deadline,
    limits: CelldPreparationLimits,
    poisoned: bool,
    stats: PreparationStats,
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
        })
    }

    /// Builds and accounts a live handle. This intentionally performs no remote
    /// probe and makes no claim about connectivity or any operation permission.
    /// Live calls start fresh deadlines, independent of this session's deadline.
    ///
    /// # Errors
    /// Returns a closed category on invalid input, quota or client construction.
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

    /// Prepares a record handle using a read-only service capability exchange.
    ///
    /// # Errors
    /// Rejects invalid input, unsupported service capabilities and unavailable
    /// selected stores. A failed session stays poisoned; no fallback is prepared.
    #[cfg(feature = "records")]
    #[inline]
    pub async fn prepare_record_store(
        &mut self,
        settings: &CelldKvSettings,
        layout: &RecordLayout,
        token: Option<&CelldAccessToken>,
    ) -> Result<KvHandle, PreparationError> {
        self.begin()?;
        let result = self.records_inner(settings, layout, token).await;
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }

    #[cfg(feature = "records")]
    async fn records_inner(
        &mut self,
        settings: &CelldKvSettings,
        layout: &RecordLayout,
        token: Option<&CelldAccessToken>,
    ) -> Result<KvHandle, PreparationError> {
        settings.validate()?;
        self.charge_mapping(&[settings.endpoint.len(), settings.store.len()])?;
        let (_, client) = self.client(&settings.endpoint, &settings.access, token)?;
        let endpoint = endpoint(&settings.endpoint, &settings.access)?;
        let store = CelldRecordKvStore::prepare(
            client,
            &endpoint,
            settings.store.clone(),
            self.limits,
            layout,
            self.deadline,
        )
        .await?;
        self.check()?;
        Ok(KvHandle::new(Arc::new(store)))
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

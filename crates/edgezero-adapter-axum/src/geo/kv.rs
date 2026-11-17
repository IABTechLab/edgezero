//! Axum-owned preparation and execution budgets for a portable KV resolver.

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use edgezero_core::geo::kv::{IndexLimits, Receipt, Resolver};
use edgezero_core::geo::{GeoError, GeoSource, GeoStatus, GeoTrustBasis, GeoUnavailableReason};
use edgezero_core::ingress::CheckedHostSource;
use edgezero_core::store_registry::KvRegistry;
use tokio::sync::Semaphore;
use tokio::time::timeout;

use super::NativeGeoConfigError;
use crate::diagnostics::NativeIngressMetadata;

// The 16 KiB production-size prototype needed up to three nodes; the 64 KiB
// index needed two. The cache payload budget is the measured prototype budget.
const MAX_PATH_OBJECTS: usize = 3;
const CACHE_BYTES: usize = 4 * 1024 * 1024;
// 4 MiB holds 256 objects at the smaller measured object budget (16 KiB).
const CACHE_ENTRIES: usize = 256;
// Do not exceed the smaller qualified backend ceiling (ordinary DynamoDB).
pub(crate) const MAX_OBJECT_BYTES: usize = 384 * 1024;
// Use the native celld preparation window for the bounded manifest/root probe.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// How this listener acquires a reader IP for KV geography.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KvGeoReaderIp {
    /// This listener receives reader connections directly.
    DirectPeer,
    /// Only the client approved by the trusted proxy policy is usable.
    ForwardedRequired,
}

impl KvGeoReaderIp {
    pub(crate) const fn trust_basis(self) -> GeoTrustBasis {
        match self {
            Self::DirectPeer => GeoTrustBasis::CheckedDirectPeer,
            Self::ForwardedRequired => GeoTrustBasis::CheckedForwardedClient,
        }
    }

    fn address(
        self,
        peer: Option<SocketAddr>,
        checked: Option<&NativeIngressMetadata>,
    ) -> Option<IpAddr> {
        match self {
            Self::DirectPeer => peer.map(|address| address.ip().to_canonical()),
            Self::ForwardedRequired => checked
                .filter(|metadata| {
                    matches!(
                        metadata.client_source(),
                        CheckedHostSource::TrustedForwarded | CheckedHostSource::TrustedXForwarded
                    )
                })
                .and_then(NativeIngressMetadata::effective_client),
        }
    }
}

/// Validated KV Geo selection. The serving credential comes from the named KV binding.
#[derive(Clone)]
pub struct KvGeoSelection {
    store_id: String,
    receipt: Receipt,
    reader_ip: KvGeoReaderIp,
    lookup_timeout: Duration,
    max_in_flight: usize,
}

impl fmt::Debug for KvGeoSelection {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KvGeoSelection")
            .field("reader_ip", &self.reader_ip)
            .finish_non_exhaustive()
    }
}

impl KvGeoSelection {
    /// Capture an explicit `<generation>@<manifest-sha256>` pin and request budgets.
    ///
    /// # Errors
    /// Rejects missing/invalid configuration and a malformed pin.
    #[inline]
    pub fn new(
        store_id: &str,
        pin: &str,
        reader_ip: KvGeoReaderIp,
        lookup_timeout_ms: u32,
        max_in_flight: usize,
    ) -> Result<Self, NativeGeoConfigError> {
        if store_id.is_empty()
            || lookup_timeout_ms == 0
            || !(1..=Semaphore::MAX_PERMITS).contains(&max_in_flight)
        {
            return Err(NativeGeoConfigError::InvalidSelection);
        }
        let receipt =
            Receipt::from_pin(pin).map_err(|_error| NativeGeoConfigError::InvalidSelection)?;
        Ok(Self {
            store_id: store_id.into(),
            receipt,
            reader_ip,
            lookup_timeout: Duration::from_millis(lookup_timeout_ms.into()),
            max_in_flight,
        })
    }

    pub(super) const fn trust_basis(&self) -> GeoTrustBasis {
        self.reader_ip.trust_basis()
    }

    pub(super) fn store_id(&self) -> &str {
        &self.store_id
    }

    pub(super) async fn prepare(
        self,
        stores: Option<&KvRegistry>,
        max_object_bytes: usize,
    ) -> Result<PreparedKvGeo, NativeGeoConfigError> {
        let handle = stores
            .and_then(|registry| registry.named(&self.store_id))
            .ok_or(NativeGeoConfigError::MissingKvBinding)?;
        let index = timeout(STARTUP_TIMEOUT, async {
            let index = Resolver::open(
                handle,
                &self.receipt,
                IndexLimits {
                    max_object_bytes,
                    max_path_objects: MAX_PATH_OBJECTS,
                    cache_bytes: CACHE_BYTES,
                    cache_entries: CACHE_ENTRIES,
                },
            )
            .await?;
            index.verify_roots().await?;
            Ok::<_, anyhow::Error>(index)
        })
        .await
        .map_err(|_error| NativeGeoConfigError::KvPreparationDeadline)?
        .map_err(|_error| NativeGeoConfigError::KvPreparationFailed)?;
        log::info!(target: "edgezero::native", "event=geo_ready schema={} generation={}", index.manifest().schema, index.manifest().generation);
        Ok(PreparedKvGeo {
            index,
            reader_ip: self.reader_ip,
            lookup_timeout: self.lookup_timeout,
            slots: Semaphore::new(self.max_in_flight),
        })
    }
}

pub(crate) struct PreparedKvGeo {
    index: Resolver,
    reader_ip: KvGeoReaderIp,
    lookup_timeout: Duration,
    slots: Semaphore,
}

impl PreparedKvGeo {
    pub(super) const fn trust_basis(&self) -> GeoTrustBasis {
        self.reader_ip.trust_basis()
    }
    pub(super) async fn resolve(
        &self,
        peer: Option<SocketAddr>,
        checked: Option<&NativeIngressMetadata>,
    ) -> GeoStatus {
        let Some(ip) = self.reader_ip.address(peer, checked) else {
            return GeoStatus::Unavailable(GeoUnavailableReason::MissingReaderIp);
        };
        let Ok(_slot) = self.slots.try_acquire() else {
            return GeoStatus::Error(GeoError::ProviderFailure);
        };
        match timeout(self.lookup_timeout, self.index.lookup(ip)).await {
            Ok(Ok(Some(source))) => match source.geo_location() {
                Some(location) => GeoStatus::Located {
                    location,
                    source: GeoSource::Kv,
                    trust_basis: self.reader_ip.trust_basis(),
                },
                None => GeoStatus::Unavailable(GeoUnavailableReason::UnsupportedGeography),
            },
            Ok(Ok(None)) => GeoStatus::Unavailable(GeoUnavailableReason::NoMatch),
            _ => GeoStatus::Error(GeoError::ProviderFailure),
        }
    }
}

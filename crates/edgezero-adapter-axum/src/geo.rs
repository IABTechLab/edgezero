//! Explicit native Geo composition for local `GeoLite` databases and protected assertions.
//!
//! Local database selection works without `CloudFront`, AWS stores or credentials.
//! Select `geo-maxmind` to load an operator-supplied read-only snapshot before the
//! runner initializes. Request lookup uses only checked ingress and memory.
//! Database preparation is synchronous, before signal registration/stop polling.
//!
//! Direct-reader configuration, with no file I/O during capability inspection:
//! ```no_run
//! use edgezero_adapter_axum::dev_server::AxumRunOptions;
//! use edgezero_adapter_axum::geo::{MaxMindGeoConfig, NativeGeoSelection, ReaderIpExpectation};
//!
//! let options = AxumRunOptions::new("0.0.0.0:8787".parse()?)?
//!     .with_geo(NativeGeoSelection::MaxMind {
//!         config: MaxMindGeoConfig::new(
//!             "/var/lib/edgezero/geo/GeoLite2-City.mmdb",
//!             ReaderIpExpectation::PeerAllowed,
//!         ),
//!     });
//! let _configured = options.geo_capability()?;
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! A reverse proxy must resolve the reader through the existing trusted policy:
//! ```no_run
//! use edgezero_adapter_axum::dev_server::AxumRunOptions;
//! use edgezero_adapter_axum::geo::{MaxMindGeoConfig, NativeGeoSelection, ReaderIpExpectation};
//! use edgezero_adapter_axum::proxy::{ForwardingHeaderFamily, TrustedProxyPolicy};
//!
//! let proxy = TrustedProxyPolicy::new(
//!     ForwardingHeaderFamily::Forwarded, ["10.20.0.8/32"],
//! )?;
//! let options = AxumRunOptions::new("0.0.0.0:8787".parse()?)?
//!     .with_trusted_proxy_policy(proxy)
//!     .with_geo(NativeGeoSelection::MaxMind {
//!         config: MaxMindGeoConfig::new(
//!             "/var/lib/edgezero/geo/GeoLite2-City.mmdb",
//!             ReaderIpExpectation::ForwardedRequired,
//!         ),
//!     });
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! The `CloudFront` policy declares external infrastructure protection. A matching socket peer
//! is not proof of an ALB credential rule or a particular `CloudFront` distribution.
//! See the native Geo guide for the required network, header and cache controls.
//!
//! Off is valid and is the default, with no ambient Geo selector:
//! ```
//! use edgezero_adapter_axum::dev_server::AxumRunOptions;
//! use edgezero_adapter_axum::geo::{NativeGeoCapability, NativeGeoSelection};
//! let options = AxumRunOptions::new("127.0.0.1:8787".parse()?)?
//!     .with_geo(NativeGeoSelection::Off);
//! assert_eq!(options.geo_capability()?, NativeGeoCapability::Off);
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! Capture capability from finalized options in the existing local initializer.
//! The selector compiles without `geo-cloudfront`, but startup then rejects it:
//! ```no_run
//! use edgezero_adapter_axum::dev_server::{AxumRunOptions, run_app_with_options_and_initializer};
//! use edgezero_adapter_axum::geo::{NativeGeoSelection, ProtectedCloudFrontAlb};
//! use edgezero_core::app::Hooks;
//!
//! fn run<A: Hooks>() -> anyhow::Result<()> {
//!     let policy = ProtectedCloudFrontAlb::new(
//!         ["10.42.1.0/24", "10.42.2.0/24"], "x-origin-verify-randomsuffix",
//!     )?;
//!     let options = AxumRunOptions::new("0.0.0.0:8787".parse()?)?
//!         .with_geo(NativeGeoSelection::CloudFront { policy });
//!     let capability = options.geo_capability()?;
//!     run_app_with_options_and_initializer::<A, _, _>(
//!         options, std::future::pending::<()>(), move |_app, _stores| {
//!             Box::pin(async move {
//!                 let _configured_capability = capability;
//!                 // Validate application requirements here, without inferring
//!                 // that every request will have geographic metadata.
//!                 Ok(())
//!             })
//!         },
//!     )
//! }
//! ```
//!
//! Supplied providers work without the built-in feature and use the same bounded,
//! synchronous, local-only contract. Construction belongs to the application:
//! ```
//! use std::sync::Arc;
//! use edgezero_adapter_axum::dev_server::AxumRunOptions;
//! use edgezero_adapter_axum::geo::{NativeGeoSelection, ProtectedCloudFrontAlb};
//! use edgezero_core::geo::{GeoError, GeoInput, GeoProvider, GeoProviderId, GeoResolution};
//!
//! struct ExampleProvider;
//! impl GeoProvider for ExampleProvider {
//!     fn resolve(&self, input: &GeoInput) -> GeoResolution {
//!         match input {
//!             GeoInput::Assertions { .. } => GeoResolution::MissingMetadata,
//!             GeoInput::ReaderIp { .. } => GeoResolution::Error(GeoError::ProviderFailure),
//!         }
//!     }
//! }
//! let policy = ProtectedCloudFrontAlb::new(
//!     ["10.42.1.0/24"], "x-origin-verify-randomsuffix",
//! )?;
//! let options = AxumRunOptions::new("127.0.0.1:8787".parse()?)?
//!     .with_geo(NativeGeoSelection::Supplied {
//!         policy,
//!         id: GeoProviderId::new("example.local")?,
//!         provider: Arc::new(ExampleProvider),
//!     });
//! let _capability = options.geo_capability()?;
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! Consumers distinguish every outcome. Even a located country/subdivision pair
//! is an estimate, and subdivision membership is not verified:
//! ```
//! use edgezero_core::geo::{GeoError, GeoOutcome, GeoStatus, GeoUnavailableReason};
//! fn describe(outcome: &GeoOutcome) -> &'static str {
//!     match outcome {
//!         GeoOutcome::NoSelection => "not configured",
//!         GeoOutcome::Selected { status, .. } => match status {
//!             GeoStatus::Unavailable(GeoUnavailableReason::UntrustedIngress) => "source not admitted",
//!             GeoStatus::Unavailable(GeoUnavailableReason::MissingMetadata) => "metadata missing",
//!             GeoStatus::Unavailable(GeoUnavailableReason::MissingReaderIp) => "reader unavailable",
//!             GeoStatus::Unavailable(GeoUnavailableReason::NoMatch) => "no geographic match",
//!             GeoStatus::Error(GeoError::InvalidAssertion) => "assertion rejected",
//!             GeoStatus::Error(GeoError::ProviderFailure) => "provider failed",
//!             GeoStatus::Located { location, .. } if location.subdivision().is_none() => "country only",
//!             GeoStatus::Located { .. } => "country and unverified subdivision assertion",
//!         },
//!     }
//! }
//! ```

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "public composition types precede private request-bound resolution and tests"
)]
#![cfg_attr(
    any(feature = "geo-cloudfront", feature = "geo-maxmind"),
    expect(
        clippy::self_named_module_files,
        reason = "repo convention uses geo.rs with submodules under geo/, not mod.rs"
    )
)]

#[cfg(feature = "geo-cloudfront")]
mod cloudfront;
#[cfg(feature = "geo-maxmind")]
mod maxmind;
#[cfg(test)]
#[path = "geo/reader_tests.rs"]
mod reader_tests;

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use edgezero_core::geo::{
    GeoAssertion, GeoInput, GeoIpSource, GeoOutcome, GeoProvider, GeoProviderId, GeoResolution,
    GeoSource, GeoStatus, GeoTrustBasis, GeoUnavailableReason,
};
use edgezero_core::http::{HeaderMap, HeaderName};
use edgezero_core::ingress::CheckedHostSource;

use crate::peer_networks::PeerNetworks;
use crate::proxy::CheckedProxyMetadata;

/// Bounded startup configuration categories. No policy inputs are retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NativeGeoConfigError {
    #[error("native_geo: invalid protected source policy")]
    InvalidProtectedPolicy,
    #[error("native_geo: invalid credential removal policy")]
    InvalidCredentialHeader,
    #[error("native_geo: cloudfront implementation unavailable")]
    UnavailableCloudFront,
    #[error("native_geo: reserved supplied provider identity")]
    ReservedProviderId,
    #[error("native_geo: maxmind implementation unavailable")]
    UnavailableMaxMind,
    #[error("native_geo: invalid database preparation limits")]
    InvalidPreparationLimits,
    #[error("native_geo: database unavailable")]
    UnavailableDatabase,
    #[error("native_geo: invalid database")]
    InvalidDatabase,
    #[error("native_geo: database preparation limit exceeded")]
    DatabaseLimit,
    #[error("native_geo: unsupported database")]
    UnsupportedDatabase,
    #[error("native_geo: incompatible database schema")]
    InvalidDatabaseSchema,
}

/// Whether checked ingress must explicitly resolve a reader through a proxy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReaderIpExpectation {
    PeerAllowed,
    ForwardedRequired,
}

/// Configurable startup bounds. They do not impose an I/O deadline or RSS quota.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaxMindPreparationLimits {
    pub max_database_bytes: usize,
    pub max_validation_networks: usize,
}

impl Default for MaxMindPreparationLimits {
    #[inline]
    fn default() -> Self {
        Self {
            max_database_bytes: 128 * 1024 * 1024,
            max_validation_networks: 32_000_000,
        }
    }
}

impl MaxMindPreparationLimits {
    fn validate(self) -> Result<(), NativeGeoConfigError> {
        if self.max_database_bytes == 0
            || self.max_database_bytes.checked_add(1).is_none()
            || self.max_validation_networks == 0
        {
            return Err(NativeGeoConfigError::InvalidPreparationLimits);
        }
        Ok(())
    }
}

/// Operator-owned database location and explicit reader expectation. No I/O on construction.
#[derive(Clone)]
pub struct MaxMindGeoConfig {
    pub path: PathBuf,
    pub limits: MaxMindPreparationLimits,
    pub expectation: ReaderIpExpectation,
}

impl MaxMindGeoConfig {
    #[inline]
    #[must_use]
    pub fn new<P>(path: P, expectation: ReaderIpExpectation) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            path: path.into(),
            limits: MaxMindPreparationLimits::default(),
            expectation,
        }
    }
}

impl fmt::Debug for MaxMindGeoConfig {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaxMindGeoConfig")
            .field("path", &"[redacted]")
            .field("limits", &self.limits)
            .field("expectation", &self.expectation)
            .finish()
    }
}

/// An operator declaration of a credential-gated, isolated CloudFront/ALB path.
///
/// This is distinct from ordinary proxy trust. The policy contains no credential
/// value. The ALB must validate the credential before forwarding to the target.
#[derive(Clone)]
pub struct ProtectedCloudFrontAlb {
    networks: PeerNetworks,
    credential_header: HeaderName,
}

impl ProtectedCloudFrontAlb {
    /// Validate a nonempty bounded literal peer allowlist and dedicated header name.
    ///
    /// Names must be literal HTTP tokens of at most 128 ASCII bytes beginning with
    /// `x-`. Forwarding, AWS, `EdgeZero`, real-IP and correlation names are reserved.
    /// Neither malformed names nor network inputs are repaired or logged.
    ///
    /// # Errors
    /// Returns a fixed category for invalid networks or an unsafe header name.
    #[inline]
    pub fn new<I, S>(raw_networks: I, credential_name: &str) -> Result<Self, NativeGeoConfigError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let networks = PeerNetworks::new(raw_networks)
            .map_err(|_error| NativeGeoConfigError::InvalidProtectedPolicy)?;
        if networks.is_empty() {
            return Err(NativeGeoConfigError::InvalidProtectedPolicy);
        }
        if credential_name.len() > 128 || !credential_name.is_ascii() {
            return Err(NativeGeoConfigError::InvalidCredentialHeader);
        }
        let normalized = credential_name.to_ascii_lowercase();
        if !normalized.starts_with("x-")
            || ["x-forwarded-", "x-amz-", "x-amzn-", "x-edge-"]
                .iter()
                .any(|prefix| normalized.starts_with(prefix))
            || ["x-real-ip", "x-request-id", "x-correlation-id"].contains(&normalized.as_str())
        {
            return Err(NativeGeoConfigError::InvalidCredentialHeader);
        }
        let credential_header = HeaderName::from_bytes(normalized.as_bytes())
            .map_err(|_error| NativeGeoConfigError::InvalidCredentialHeader)?;
        Ok(Self {
            networks,
            credential_header,
        })
    }

    fn admit<'request>(
        &self,
        peer: Option<SocketAddr>,
        headers: &'request HeaderMap,
    ) -> Option<ProtectedAssertions<'request>> {
        peer.filter(|actual_peer| self.networks.trusts(actual_peer.ip()))
            .map(|_peer| ProtectedAssertions { headers })
    }
}

impl fmt::Debug for ProtectedCloudFrontAlb {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtectedCloudFrontAlb")
            .field("network_count", &self.networks.len())
            .finish_non_exhaustive()
    }
}

/// One complete deployment selection. Repeated `with_geo` calls replace it.
#[derive(Clone, Default)]
pub enum NativeGeoSelection {
    #[default]
    Off,
    CloudFront {
        policy: ProtectedCloudFrontAlb,
    },
    MaxMind {
        config: MaxMindGeoConfig,
    },
    SuppliedReaderIp {
        expectation: ReaderIpExpectation,
        id: GeoProviderId,
        provider: Arc<dyn GeoProvider + Send + Sync>,
    },
    Supplied {
        policy: ProtectedCloudFrontAlb,
        id: GeoProviderId,
        provider: Arc<dyn GeoProvider + Send + Sync>,
    },
}

impl fmt::Debug for NativeGeoSelection {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Off => f.write_str("Off"),
            Self::CloudFront { policy } => f
                .debug_struct("CloudFront")
                .field("policy", policy)
                .finish(),
            Self::MaxMind { config } => f.debug_struct("MaxMind").field("config", config).finish(),
            Self::SuppliedReaderIp {
                expectation, id, ..
            } => f
                .debug_struct("SuppliedReaderIp")
                .field("expectation", expectation)
                .field("id", id)
                .finish_non_exhaustive(),
            Self::Supplied { policy, id, .. } => f
                .debug_struct("Supplied")
                .field("policy", policy)
                .field("id", id)
                .finish_non_exhaustive(),
        }
    }
}

/// Configured input contract, without eventual request provenance or database readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeGeoInputContract {
    ProtectedCloudFrontAlb,
    ReaderIp { expectation: ReaderIpExpectation },
}

/// Owned startup configuration capability. Inspection performs no database I/O.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeGeoCapability {
    Off,
    Selected {
        provider: GeoProviderId,
        input: NativeGeoInputContract,
    },
}

impl NativeGeoSelection {
    pub(crate) fn capability(&self) -> Result<NativeGeoCapability, NativeGeoConfigError> {
        let (provider, input) = match self {
            Self::Off => return Ok(NativeGeoCapability::Off),
            Self::CloudFront { .. } => {
                if !cfg!(feature = "geo-cloudfront") {
                    return Err(NativeGeoConfigError::UnavailableCloudFront);
                }
                (
                    GeoProviderId::cloudfront(),
                    NativeGeoInputContract::ProtectedCloudFrontAlb,
                )
            }
            Self::MaxMind { config } => {
                config.limits.validate()?;
                if !cfg!(all(feature = "geo-maxmind", unix)) {
                    return Err(NativeGeoConfigError::UnavailableMaxMind);
                }
                (
                    GeoProviderId::maxmind(),
                    NativeGeoInputContract::ReaderIp {
                        expectation: config.expectation,
                    },
                )
            }
            Self::Supplied { id, .. } | Self::SuppliedReaderIp { id, .. } => {
                if id == &GeoProviderId::cloudfront() || id == &GeoProviderId::maxmind() {
                    return Err(NativeGeoConfigError::ReservedProviderId);
                }
                let input = if let Self::SuppliedReaderIp { expectation, .. } = self {
                    NativeGeoInputContract::ReaderIp {
                        expectation: *expectation,
                    }
                } else {
                    NativeGeoInputContract::ProtectedCloudFrontAlb
                };
                (id.clone(), input)
            }
        };
        Ok(NativeGeoCapability::Selected { provider, input })
    }
}

/// Source admission remains framework-owned and independent of provider identity.
#[derive(Clone)]
pub(crate) enum NativeGeoAdmission {
    Assertions(ProtectedCloudFrontAlb),
    ReaderIp {
        expectation: ReaderIpExpectation,
        source: GeoSource,
    },
}

/// Prepared runner-owned composition, never installed in public extensions.
#[derive(Clone, Default)]
pub(crate) enum NativeGeo {
    #[default]
    Off,
    Selected {
        admission: NativeGeoAdmission,
        id: GeoProviderId,
        provider: Arc<dyn GeoProvider + Send + Sync>,
        #[cfg(feature = "geo-maxmind")]
        snapshot: Option<maxmind::MaxMindSnapshot>,
    },
}

impl NativeGeo {
    pub(crate) fn prepare(selection: &NativeGeoSelection) -> Result<Self, NativeGeoConfigError> {
        let _capability = selection.capability()?;
        let (admission, id, provider): (_, _, Arc<dyn GeoProvider + Send + Sync>) = match selection
        {
            NativeGeoSelection::Off => return Ok(Self::Off),
            #[cfg(feature = "geo-cloudfront")]
            NativeGeoSelection::CloudFront { policy } => (
                NativeGeoAdmission::Assertions(policy.clone()),
                GeoProviderId::cloudfront(),
                Arc::new(cloudfront::CloudFrontProvider),
            ),
            #[cfg(not(feature = "geo-cloudfront"))]
            NativeGeoSelection::CloudFront { .. } => {
                return Err(NativeGeoConfigError::UnavailableCloudFront);
            }
            #[cfg(feature = "geo-maxmind")]
            NativeGeoSelection::MaxMind { config } => {
                let provider = maxmind::MaxMindProvider::prepare(config)?;
                let snapshot = Some(provider.metadata.clone());
                return Ok(Self::Selected {
                    admission: NativeGeoAdmission::ReaderIp {
                        expectation: config.expectation,
                        source: GeoSource::LocalDatabase,
                    },
                    id: GeoProviderId::maxmind(),
                    provider: Arc::new(provider),
                    snapshot,
                });
            }
            #[cfg(not(feature = "geo-maxmind"))]
            NativeGeoSelection::MaxMind { .. } => {
                return Err(NativeGeoConfigError::UnavailableMaxMind);
            }
            NativeGeoSelection::Supplied {
                policy,
                id,
                provider,
            } => (
                NativeGeoAdmission::Assertions(policy.clone()),
                id.clone(),
                Arc::clone(provider),
            ),
            NativeGeoSelection::SuppliedReaderIp {
                expectation,
                id,
                provider,
            } => (
                NativeGeoAdmission::ReaderIp {
                    expectation: *expectation,
                    source: GeoSource::CheckedReaderIp,
                },
                id.clone(),
                Arc::clone(provider),
            ),
        };
        Ok(Self::Selected {
            admission,
            id,
            provider,
            #[cfg(feature = "geo-maxmind")]
            snapshot: None,
        })
    }

    #[cfg_attr(
        not(feature = "geo-maxmind"),
        expect(
            clippy::unused_self,
            reason = "the stable runner hook has no snapshot when the built-in reader is absent"
        )
    )]
    pub(crate) fn log_snapshot(&self) {
        #[cfg(feature = "geo-maxmind")]
        if let Self::Selected {
            snapshot: Some(snapshot),
            ..
        } = self
        {
            log::info!(target: "edgezero::native", "event=geo_database provider=maxmind database_type={} build_epoch={} sha256={}", snapshot.database_type, snapshot.build_epoch, snapshot.sha256);
        }
    }

    pub(crate) fn strip_credential(&self, headers: &mut HeaderMap) {
        if let Self::Selected {
            admission: NativeGeoAdmission::Assertions(policy),
            ..
        } = self
        {
            let _removed = headers.remove(&policy.credential_header);
        }
    }

    pub(crate) fn reader_input(&self, checked: &CheckedProxyMetadata) -> Option<GeoInput> {
        let Self::Selected {
            admission: NativeGeoAdmission::ReaderIp { expectation, .. },
            ..
        } = self
        else {
            return None;
        };
        let _peer = checked.direct_peer()?;
        let source = match checked.client_source() {
            CheckedHostSource::Direct if *expectation == ReaderIpExpectation::PeerAllowed => {
                GeoIpSource::DirectPeer
            }
            CheckedHostSource::TrustedForwarded => GeoIpSource::TrustedForwarded,
            CheckedHostSource::TrustedXForwarded => GeoIpSource::TrustedXForwarded,
            CheckedHostSource::Direct | CheckedHostSource::Unavailable | _ => return None,
        };
        Some(GeoInput::ReaderIp {
            address: checked.effective_client()?,
            source,
        })
    }

    pub(crate) fn resolve(
        &self,
        peer: Option<SocketAddr>,
        headers: &HeaderMap,
        reader: Option<&GeoInput>,
    ) -> GeoOutcome {
        let Self::Selected {
            admission,
            id,
            provider,
            ..
        } = self
        else {
            return GeoOutcome::NoSelection;
        };
        let admitted = match admission {
            NativeGeoAdmission::Assertions(policy) => policy
                .admit(peer, headers)
                .map(|admitted| {
                    (
                        admitted.input(),
                        GeoSource::CloudFront,
                        GeoTrustBasis::DeclaredProtectedAlb,
                    )
                })
                .ok_or(GeoUnavailableReason::UntrustedIngress),
            NativeGeoAdmission::ReaderIp { source, .. } => match reader {
                Some(
                    input @ GeoInput::ReaderIp {
                        source: reader_source,
                        ..
                    },
                ) => Ok((
                    input.clone(),
                    *source,
                    GeoTrustBasis::CheckedReaderIp(*reader_source),
                )),
                _ => Err(GeoUnavailableReason::MissingReaderIp),
            },
        };
        let status = match admitted {
            Err(reason) => GeoStatus::Unavailable(reason),
            Ok((input, source, trust_basis)) => match provider.resolve(&input) {
                GeoResolution::MissingMetadata => {
                    GeoStatus::Unavailable(GeoUnavailableReason::MissingMetadata)
                }
                GeoResolution::NoMatch => GeoStatus::Unavailable(GeoUnavailableReason::NoMatch),
                GeoResolution::Error(error) => GeoStatus::Error(error),
                GeoResolution::Located(location) => GeoStatus::Located {
                    location,
                    source,
                    trust_basis,
                },
            },
        };
        GeoOutcome::Selected {
            provider: id.clone(),
            status,
        }
    }
}

/// Private witness tied to the admitted request's still-owned header snapshot.
struct ProtectedAssertions<'request> {
    headers: &'request HeaderMap,
}

impl ProtectedAssertions<'_> {
    fn input(&self) -> GeoInput {
        GeoInput::Assertions {
            country: assertion(self.headers, "cloudfront-viewer-country"),
            subdivision: assertion(self.headers, "cloudfront-viewer-country-region"),
        }
    }
}

fn assertion<const N: usize>(headers: &HeaderMap, name: &'static str) -> GeoAssertion<N> {
    let mut values = headers.get_all(name).iter();
    let Some(first) = values.next() else {
        return GeoAssertion::absent();
    };
    if values.next().is_some() {
        return GeoAssertion::invalid();
    }
    GeoAssertion::from_bytes(first.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_options::AxumRunOptions;
    use edgezero_core::geo::{CountryCode, GeoError, GeoLocation};
    use edgezero_core::http::HeaderValue;
    use std::iter::empty;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn policy() -> ProtectedCloudFrontAlb {
        ProtectedCloudFrontAlb::new(["127.0.0.1/32"], "x-origin-verify-fixture").unwrap()
    }

    #[test]
    fn geo_protected_policy_reuses_network_bounds() {
        assert_eq!(
            ProtectedCloudFrontAlb::new(empty::<&str>(), "x-origin-verify-fixture").unwrap_err(),
            NativeGeoConfigError::InvalidProtectedPolicy
        );
        ProtectedCloudFrontAlb::new(["127.0.0.1"; 256], "x-origin-verify-fixture").unwrap();
        assert_eq!(
            ProtectedCloudFrontAlb::new(["127.0.0.1"; 257], "x-origin-verify-fixture").unwrap_err(),
            NativeGeoConfigError::InvalidProtectedPolicy
        );
        let exact = format!("127.0.0.1{}", " ".repeat(0x4000 - 9));
        ProtectedCloudFrontAlb::new([exact.as_str()], "x-origin-verify-fixture").unwrap();
        assert_eq!(
            ProtectedCloudFrontAlb::new([format!("{exact} ")], "x-origin-verify-fixture")
                .unwrap_err(),
            NativeGeoConfigError::InvalidProtectedPolicy
        );
        for ranges in [
            vec!["0.0.0.0/0"],
            vec!["::/0"],
            vec!["0.0.0.0/1", "128.0.0.0/1"],
            vec!["::/1", "8000::/1"],
            vec!["::ffff:0:0/96"],
            vec!["not-a-network"],
        ] {
            assert_eq!(
                ProtectedCloudFrontAlb::new(ranges, "x-origin-verify-fixture").unwrap_err(),
                NativeGeoConfigError::InvalidProtectedPolicy
            );
        }
        let selected = ProtectedCloudFrontAlb::new(
            ["192.0.2.0/24", "2001:db8::/32"],
            "x-origin-verify-fixture",
        )
        .unwrap();
        let headers = HeaderMap::new();
        for peer in ["192.0.2.1:80", "[2001:db8::1]:80", "[::ffff:192.0.2.1]:80"] {
            assert!(
                selected
                    .admit(Some(peer.parse().unwrap()), &headers)
                    .is_some()
            );
        }
        assert!(selected.admit(None, &headers).is_none());
        assert!(
            selected
                .admit(Some("192.0.3.1:80".parse().unwrap()), &headers)
                .is_none()
        );
        ProtectedCloudFrontAlb::new(["192.0.2.0/24"; 256], "x-origin-verify-fixture").unwrap();
    }

    #[test]
    fn credential_names_are_dedicated_bounded_and_redacted() {
        for name in [
            "authorization",
            "cookie",
            "host",
            "forwarded",
            "CloudFront-Viewer-Country",
            "CloudFront-Viewer-Country-Region",
            "X-FORWARDED-FOR",
            "x-forwarded-host",
            "X-Real-IP",
            "X-Amz-Token",
            "x-amzn-trace-id",
            "x-edge-request",
            "x-request-id",
            "X-Correlation-ID",
            "x-bad name",
            "x-bad\r\nname",
            " x-secret",
            "x-secret ",
            "x-\u{e9}",
            "",
        ] {
            assert!(
                ProtectedCloudFrontAlb::new(["127.0.0.1"], name).is_err(),
                "{name:?}"
            );
        }
        let exact = format!("x-{}", "a".repeat(126));
        ProtectedCloudFrontAlb::new(["127.0.0.1"], &exact).unwrap();
        assert_eq!(
            ProtectedCloudFrontAlb::new(["127.0.0.1"], &format!("{exact}a")).unwrap_err(),
            NativeGeoConfigError::InvalidCredentialHeader
        );
        let policy =
            ProtectedCloudFrontAlb::new(["192.0.2.0/24"], "X-Origin-Private-Sentinel").unwrap();
        let debug = format!("{policy:?}");
        assert!(!debug.contains("192.0.2"));
        assert!(!debug.to_ascii_lowercase().contains("sentinel"));
        let error =
            ProtectedCloudFrontAlb::new(["not-a-network-secret"], "x-private-secret").unwrap_err();
        assert!(!format!("{error:?} {error}").contains("secret"));
    }

    struct CountingProvider {
        calls: Arc<AtomicUsize>,
        result: GeoResolution,
    }
    impl GeoProvider for CountingProvider {
        fn resolve(&self, _input: &GeoInput) -> GeoResolution {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.result.clone()
        }
    }

    #[test]
    fn source_admission_precedes_header_inspection_and_provider_calls() {
        let calls = Arc::new(AtomicUsize::new(0));
        let selected = NativeGeo::prepare(&NativeGeoSelection::Supplied {
            policy: policy(),
            id: GeoProviderId::new("counting").unwrap(),
            provider: Arc::new(CountingProvider {
                calls: Arc::clone(&calls),
                result: GeoResolution::Error(GeoError::ProviderFailure),
            }),
        })
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.append("cloudfront-viewer-country", HeaderValue::from_static("US"));
        headers.append("cloudfront-viewer-country", HeaderValue::from_static("CA"));
        for peer in [None, Some("192.0.2.1:80".parse().unwrap())] {
            assert_eq!(
                selected.resolve(peer, &headers, None),
                GeoOutcome::Selected {
                    provider: GeoProviderId::new("counting").unwrap(),
                    status: GeoStatus::Unavailable(GeoUnavailableReason::UntrustedIngress)
                }
            );
        }
        assert_eq!(
            NativeGeo::Off.resolve(Some("127.0.0.1:80".parse().unwrap()), &headers, None),
            GeoOutcome::NoSelection
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        for values in [&headers, &HeaderMap::new()] {
            assert_eq!(
                selected.resolve(Some("127.0.0.1:80".parse().unwrap()), values, None),
                GeoOutcome::Selected {
                    provider: GeoProviderId::new("counting").unwrap(),
                    status: GeoStatus::Error(GeoError::ProviderFailure)
                }
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn duplicate_and_oversize_assertions_are_invalid_before_copying() {
        let mut headers = HeaderMap::new();
        headers.append("cloudfront-viewer-country", HeaderValue::from_static("US"));
        headers.append("cloudfront-viewer-country", HeaderValue::from_static("US"));
        assert_eq!(
            assertion::<2>(&headers, "cloudfront-viewer-country").as_bytes(),
            Err(GeoError::InvalidAssertion)
        );
        headers.remove("cloudfront-viewer-country");
        headers.insert("cloudfront-viewer-country", HeaderValue::from_static("USA"));
        assert_eq!(
            assertion::<2>(&headers, "cloudfront-viewer-country").as_bytes(),
            Err(GeoError::InvalidAssertion)
        );
        headers.insert(
            "cloudfront-viewer-country-region",
            HeaderValue::from_static("ABCD"),
        );
        assert_eq!(
            assertion::<3>(&headers, "cloudfront-viewer-country-region").as_bytes(),
            Err(GeoError::InvalidAssertion)
        );
    }

    #[test]
    fn credential_removal_strips_all_instances_and_preserves_geography() {
        let selected = NativeGeo::prepare(&NativeGeoSelection::Supplied {
            policy: policy(),
            id: GeoProviderId::new("custom").unwrap(),
            provider: Arc::new(CountingProvider {
                calls: Arc::default(),
                result: GeoResolution::MissingMetadata,
            }),
        })
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.append("x-origin-verify-fixture", HeaderValue::from_static("first"));
        headers.append(
            "x-origin-verify-fixture",
            HeaderValue::from_static("second"),
        );
        headers.insert("cloudfront-viewer-country", HeaderValue::from_static("US"));
        selected.strip_credential(&mut headers);
        assert_eq!(headers.get_all("x-origin-verify-fixture").iter().count(), 0);
        assert_eq!(headers["cloudfront-viewer-country"], "US");
    }

    #[test]
    fn supplied_identity_cannot_claim_builtin_and_options_keep_thread_bounds() {
        fn assert_bounds<T>()
        where
            T: Clone + Send + Sync,
        {
        }
        assert_bounds::<AxumRunOptions>();
        let error = NativeGeo::prepare(&NativeGeoSelection::Supplied {
            policy: policy(),
            id: GeoProviderId::cloudfront(),
            provider: Arc::new(CountingProvider {
                calls: Arc::default(),
                result: GeoResolution::Located(GeoLocation::new(
                    CountryCode::new("US").unwrap(),
                    None,
                )),
            }),
        })
        .err()
        .unwrap();
        assert_eq!(error, NativeGeoConfigError::ReservedProviderId);
        let options = AxumRunOptions::new("127.0.0.1:8080".parse().unwrap()).unwrap();
        assert_eq!(options.geo_capability().unwrap(), NativeGeoCapability::Off);
        let supplied = NativeGeoSelection::Supplied {
            policy: policy(),
            id: GeoProviderId::new("custom").unwrap(),
            provider: Arc::new(CountingProvider {
                calls: Arc::default(),
                result: GeoResolution::MissingMetadata,
            }),
        };
        let selected_options = options.with_geo(supplied);
        assert!(
            matches!(selected_options.geo_capability().unwrap(), NativeGeoCapability::Selected { provider, .. } if provider.as_str() == "custom")
        );
        assert_eq!(
            selected_options
                .with_geo(NativeGeoSelection::Off)
                .geo_capability()
                .unwrap(),
            NativeGeoCapability::Off
        );
    }

    #[cfg(not(feature = "geo-cloudfront"))]
    #[test]
    fn compiled_out_builtin_is_an_error_not_off() {
        let selected = NativeGeoSelection::CloudFront { policy: policy() };
        assert_eq!(
            NativeGeo::prepare(&selected).err(),
            Some(NativeGeoConfigError::UnavailableCloudFront)
        );
    }
}

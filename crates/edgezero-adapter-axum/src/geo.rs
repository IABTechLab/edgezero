//! Explicit native Geo composition for an operator-protected CloudFront/ALB path.
//!
//! This policy declares external infrastructure protection. A matching socket peer
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
//! use edgezero_core::geo::{GeoInput, GeoProvider, GeoProviderId, GeoResolution};
//!
//! struct ExampleProvider;
//! impl GeoProvider for ExampleProvider {
//!     fn resolve(&self, _input: &GeoInput) -> GeoResolution {
//!         GeoResolution::MissingMetadata
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
    feature = "geo-cloudfront",
    expect(
        clippy::self_named_module_files,
        reason = "repo convention uses geo.rs with submodules under geo/, not mod.rs"
    )
)]

#[cfg(feature = "geo-cloudfront")]
mod cloudfront;

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use edgezero_core::geo::{
    GeoAssertion, GeoInput, GeoOutcome, GeoProvider, GeoProviderId, GeoResolution, GeoSource,
    GeoStatus, GeoTrustBasis, GeoUnavailableReason,
};
use edgezero_core::http::{HeaderMap, HeaderName};

use crate::peer_networks::PeerNetworks;

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
            Self::Supplied { policy, id, .. } => f
                .debug_struct("Supplied")
                .field("policy", policy)
                .field("id", id)
                .finish_non_exhaustive(),
        }
    }
}

/// Owned startup capability, not proof of per-request coverage or infrastructure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeGeoCapability {
    Off,
    Selected {
        provider: GeoProviderId,
        source: GeoSource,
        trust_basis: GeoTrustBasis,
    },
}

/// Validated runner-owned composition, never installed in public extensions.
#[derive(Clone, Default)]
pub(crate) enum NativeGeo {
    #[default]
    Off,
    Selected {
        policy: ProtectedCloudFrontAlb,
        id: GeoProviderId,
        provider: Arc<dyn GeoProvider + Send + Sync>,
    },
}

impl NativeGeo {
    pub(crate) fn validate(selection: &NativeGeoSelection) -> Result<Self, NativeGeoConfigError> {
        match selection {
            NativeGeoSelection::Off => Ok(Self::Off),
            #[cfg(feature = "geo-cloudfront")]
            NativeGeoSelection::CloudFront { policy } => Ok(Self::Selected {
                policy: policy.clone(),
                id: GeoProviderId::cloudfront(),
                provider: Arc::new(cloudfront::CloudFrontProvider),
            }),
            #[cfg(not(feature = "geo-cloudfront"))]
            NativeGeoSelection::CloudFront { .. } => {
                Err(NativeGeoConfigError::UnavailableCloudFront)
            }
            NativeGeoSelection::Supplied {
                policy,
                id,
                provider,
            } => {
                if id == &GeoProviderId::cloudfront() {
                    return Err(NativeGeoConfigError::ReservedProviderId);
                }
                Ok(Self::Selected {
                    policy: policy.clone(),
                    id: id.clone(),
                    provider: Arc::clone(provider),
                })
            }
        }
    }

    pub(crate) fn capability(&self) -> NativeGeoCapability {
        match self {
            Self::Off => NativeGeoCapability::Off,
            Self::Selected { id, .. } => NativeGeoCapability::Selected {
                provider: id.clone(),
                source: GeoSource::CloudFront,
                trust_basis: GeoTrustBasis::DeclaredProtectedAlb,
            },
        }
    }

    pub(crate) fn strip_credential(&self, headers: &mut HeaderMap) {
        if let Self::Selected { policy, .. } = self {
            let _removed = headers.remove(&policy.credential_header);
        }
    }

    pub(crate) fn resolve(&self, peer: Option<SocketAddr>, headers: &HeaderMap) -> GeoOutcome {
        let Self::Selected {
            policy,
            id,
            provider,
        } = self
        else {
            return GeoOutcome::NoSelection;
        };
        let status = match policy.admit(peer, headers) {
            None => GeoStatus::Unavailable(GeoUnavailableReason::UntrustedIngress),
            Some(admitted) => match provider.resolve(&admitted.input()) {
                GeoResolution::MissingMetadata => {
                    GeoStatus::Unavailable(GeoUnavailableReason::MissingMetadata)
                }
                GeoResolution::Error(error) => GeoStatus::Error(error),
                GeoResolution::Located(location) => GeoStatus::Located {
                    location,
                    source: GeoSource::CloudFront,
                    trust_basis: GeoTrustBasis::DeclaredProtectedAlb,
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
        GeoInput {
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
        let selected = NativeGeo::validate(&NativeGeoSelection::Supplied {
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
                selected.resolve(peer, &headers),
                GeoOutcome::Selected {
                    provider: GeoProviderId::new("counting").unwrap(),
                    status: GeoStatus::Unavailable(GeoUnavailableReason::UntrustedIngress)
                }
            );
        }
        assert_eq!(
            NativeGeo::Off.resolve(Some("127.0.0.1:80".parse().unwrap()), &headers),
            GeoOutcome::NoSelection
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        for values in [&headers, &HeaderMap::new()] {
            assert_eq!(
                selected.resolve(Some("127.0.0.1:80".parse().unwrap()), values),
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
        let selected = NativeGeo::validate(&NativeGeoSelection::Supplied {
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
        fn assert_bounds<T: Clone + Send + Sync>() {}
        assert_bounds::<AxumRunOptions>();
        let error = NativeGeo::validate(&NativeGeoSelection::Supplied {
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
            NativeGeo::validate(&selected).err(),
            Some(NativeGeoConfigError::UnavailableCloudFront)
        );
    }
}

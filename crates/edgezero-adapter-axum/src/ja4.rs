//! JA4 from a trusted TLS-terminating proxy.
//!
//! The Axum adapter serves plain HTTP and never sees a TLS `ClientHello`, so on
//! its own it reports JA4 as `NotExposed`. A TLS-terminating proxy in front of
//! it (`FoxIO`'s nginx module, `HAProxy`'s ja4 module, the Apache Traffic Server
//! plugin, …) can compute JA4 and forward it in a header. The adapter records
//! that header only when the direct TCP peer is inside an operator-configured
//! CIDR allowlist; from any other peer JA4 stays `NotExposed`, so clients
//! cannot forge it. List only proxies the operator runs: shared provider ranges
//! such as `CloudFront`'s let any of that provider's customers pass the check.

use std::net::{IpAddr, SocketAddr};

use anyhow::{Context as _, bail};
use edgezero_core::env_config::EnvConfig;
use edgezero_core::http::{HeaderMap, HeaderName};
use edgezero_core::ja4::{CapturedJa4, Ja4Unavailable};
use edgezero_core::manifest::{BakedManifest, ManifestJa4Source};
use ipnet::IpNet;

use crate::geo::ProtectedCloudFrontAlb;

/// The fixed header `CloudFront` writes the viewer's JA4 to when an origin request policy includes it.
const CLOUDFRONT_JA4_HEADER: &str = "cloudfront-viewer-ja4-fingerprint";

/// `EDGEZERO__ADAPTER__JA4_HEADER`: the header the trusted proxy writes JA4 to.
const HEADER_KEY: [&str; 2] = ["adapter", "ja4_header"];
/// `EDGEZERO__ADAPTER__JA4_TRUSTED_PROXY_CIDRS`: comma-separated proxy CIDRs or IPs.
const TRUSTED_PROXIES_KEY: [&str; 2] = ["adapter", "ja4_trusted_proxy_cidrs"];

/// Operator configuration for reading JA4 from a trusted proxy header.
#[derive(Clone, Debug)]
pub struct TrustedJa4Header {
    header: HeaderName,
    trusted_proxies: Vec<IpNet>,
}

impl TrustedJa4Header {
    /// Record the client's JA4 for this request.
    ///
    /// A peer inside the allowlist yields the header's capture (`Captured`,
    /// `Missing` or `Malformed`). An unknown or untrusted peer yields
    /// `NotExposed`, and the header is ignored.
    pub(crate) fn capture(&self, peer: Option<SocketAddr>, headers: &HeaderMap) -> CapturedJa4 {
        if peer.is_some_and(|address| self.trusts(address.ip())) {
            CapturedJa4::from_trusted_proxy_header(headers.get_all(&self.header))
        } else {
            CapturedJa4::Unavailable(Ja4Unavailable::NotExposed)
        }
    }

    /// Read the configuration from `EDGEZERO__ADAPTER__JA4_HEADER` and
    /// `EDGEZERO__ADAPTER__JA4_TRUSTED_PROXY_CIDRS`.
    ///
    /// Returns `Ok(None)` when neither variable is set.
    ///
    /// # Errors
    /// Fails closed when only one variable is set, the header name is invalid,
    /// or the proxy list is empty, invalid or contains a `/0` network.
    #[inline]
    pub fn from_env(env: &EnvConfig) -> anyhow::Result<Option<Self>> {
        match (env.get(&HEADER_KEY), env.get(&TRUSTED_PROXIES_KEY)) {
            (None, None) => Ok(None),
            (Some(header), Some(proxies)) => Self::parse(header, proxies).map(Some),
            _ => bail!(
                "EDGEZERO__ADAPTER__JA4_HEADER and EDGEZERO__ADAPTER__JA4_TRUSTED_PROXY_CIDRS \
                 must be set together"
            ),
        }
    }

    /// Trust `header` only from peers inside `trusted_proxies`.
    #[inline]
    #[must_use]
    pub const fn new(header: HeaderName, trusted_proxies: Vec<IpNet>) -> Self {
        Self {
            header,
            trusted_proxies,
        }
    }

    /// Parse a header name and a comma-separated list of CIDRs or bare IPs.
    ///
    /// # Errors
    /// Fails when the header name is invalid, the list is empty, an entry is
    /// not a CIDR or IP, or an entry is a `/0` network that would trust every
    /// peer.
    #[inline]
    pub fn parse(header_name: &str, proxy_list: &str) -> anyhow::Result<Self> {
        let header = HeaderName::from_bytes(header_name.trim().as_bytes())
            .context("invalid JA4 proxy header name")?;
        let trusted_proxies = proxy_list
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(parse_proxy)
            .collect::<anyhow::Result<Vec<_>>>()?;
        if trusted_proxies.is_empty() {
            bail!("JA4 trusted proxy list is empty");
        }
        Ok(Self::new(header, trusted_proxies))
    }

    /// Whether `peer` is a configured proxy. IPv4-mapped IPv6 peers from
    /// dual-stack listeners match their IPv4 networks.
    fn trusts(&self, peer: IpAddr) -> bool {
        let canonical = peer.to_canonical();
        self.trusted_proxies
            .iter()
            .any(|network| network.contains(&canonical))
    }
}

/// The JA4 source of one managed listener.
#[derive(Clone, Debug)]
pub(crate) enum Ja4Selection {
    /// An operator-run proxy named by `EDGEZERO__ADAPTER__JA4_*`.
    Generic(TrustedJa4Header),
    /// `CloudFront-Viewer-JA4-Fingerprint` from a peer the protected `CloudFront` Geo path admits.
    ProtectedCloudFront(ProtectedCloudFrontAlb),
}

impl Ja4Selection {
    /// Capture JA4 for one request from the listener's owned socket peer.
    pub(crate) fn capture(&self, peer: Option<SocketAddr>, headers: &HeaderMap) -> CapturedJa4 {
        match self {
            Self::Generic(config) => config.capture(peer, headers),
            Self::ProtectedCloudFront(policy) => {
                if policy.admits(peer) {
                    CapturedJa4::from_trusted_proxy_header(headers.get_all(CLOUDFRONT_JA4_HEADER))
                } else {
                    CapturedJa4::Unavailable(Ja4Unavailable::NotExposed)
                }
            }
        }
    }

    /// Resolve the managed listener's JA4 source from `[adapters.axum.ja4] source`, the
    /// resolved Geo selection's protected `CloudFront` path, and the generic proxy settings.
    ///
    /// # Errors
    /// Protected JA4 needs a Geo selection that carries the protected path (`CloudFront` or a
    /// supplied provider) and cannot be combined with generic proxy JA4.
    pub(crate) fn resolve(
        manifest: &BakedManifest,
        protected_path: Option<ProtectedCloudFrontAlb>,
        generic: Option<TrustedJa4Header>,
    ) -> anyhow::Result<Option<Self>> {
        let source = match manifest {
            BakedManifest::Present(baked) => baked
                .adapter_entry("axum")
                .and_then(|(_, adapter)| adapter.ja4)
                .and_then(|ja4| ja4.source),
            BakedManifest::Absent | BakedManifest::Malformed(_) | _ => None,
        };
        match source {
            None => Ok(generic.map(Self::Generic)),
            Some(ManifestJa4Source::ProtectedCloudFront) => {
                if generic.is_some() {
                    bail!(
                        "protected CloudFront JA4 cannot be combined with EDGEZERO__ADAPTER__JA4_* settings"
                    );
                }
                let policy = protected_path.context(
                    "protected CloudFront JA4 requires a CloudFront or supplied Geo selection",
                )?;
                Ok(Some(Self::ProtectedCloudFront(policy)))
            }
            Some(_) => bail!("unsupported Axum JA4 source"),
        }
    }
}

fn parse_proxy(entry: &str) -> anyhow::Result<IpNet> {
    let network = entry
        .parse::<IpNet>()
        .or_else(|_error| entry.parse::<IpAddr>().map(IpNet::from))
        .with_context(|| format!("invalid JA4 trusted proxy `{entry}`"))?;
    if network.prefix_len() == 0 {
        bail!("JA4 trusted proxy `{entry}` would trust every peer");
    }
    Ok(network)
}

#[cfg(test)]
mod tests {
    use super::TrustedJa4Header;
    use edgezero_core::env_config::EnvConfig;
    use edgezero_core::http::{HeaderMap, HeaderValue};
    use edgezero_core::ja4::{CapturedJa4, Ja4Source, Ja4Unavailable};

    const FINGERPRINT: &str = "t13d1516h2_8daaf6152771_e5627efa2ab1";

    fn headers(values: &[&'static str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append("x-ja4", HeaderValue::from_static(value));
        }
        headers
    }

    fn config() -> TrustedJa4Header {
        TrustedJa4Header::parse("X-JA4", "10.0.0.0/8, 192.0.2.7, 2001:db8::/32")
            .expect("should parse config")
    }

    #[test]
    fn ja4_proxy_trusted_peer_header_is_captured() {
        for peer in [
            "10.1.2.3:443",
            "192.0.2.7:1",
            "[::ffff:10.0.0.1]:1",
            "[2001:db8::1]:1",
        ] {
            let captured =
                config().capture(Some(peer.parse().expect("peer")), &headers(&[FINGERPRINT]));
            assert!(
                matches!(
                    captured,
                    CapturedJa4::Captured {
                        source: Ja4Source::TrustedProxyHeader,
                        ..
                    }
                ),
                "should capture from trusted peer {peer}"
            );
        }
    }

    #[test]
    fn ja4_proxy_untrusted_or_unknown_peer_is_not_exposed() {
        let not_exposed = CapturedJa4::Unavailable(Ja4Unavailable::NotExposed);
        for address in [
            Some("192.0.2.8:1"),
            Some("11.0.0.1:1"),
            Some("[2001:db9::1]:1"),
            None,
        ] {
            let peer = address.map(|text| text.parse().expect("peer"));
            assert_eq!(
                config().capture(peer, &headers(&[FINGERPRINT])),
                not_exposed
            );
        }
    }

    #[test]
    fn ja4_proxy_trusted_peer_reports_missing_and_malformed() {
        let peer = Some("10.0.0.1:1".parse().expect("peer"));
        assert_eq!(
            config().capture(peer, &headers(&[])),
            CapturedJa4::Unavailable(Ja4Unavailable::Missing)
        );
        for values in [&["not-a-ja4"][..], &[FINGERPRINT, FINGERPRINT]] {
            assert_eq!(
                config().capture(peer, &headers(values)),
                CapturedJa4::Unavailable(Ja4Unavailable::Malformed)
            );
        }
    }

    #[test]
    fn ja4_proxy_config_rejects_unsafe_or_invalid_input() {
        for (header, proxies) in [
            ("x-ja4", ""),
            ("x-ja4", " , "),
            ("x-ja4", "not-an-ip"),
            ("x-ja4", "10.0.0.0/33"),
            ("x-ja4", "0.0.0.0/0"),
            ("x-ja4", "::/0"),
            ("bad header", "10.0.0.1"),
        ] {
            assert!(
                TrustedJa4Header::parse(header, proxies).is_err(),
                "should reject {header:?} / {proxies:?}"
            );
        }
    }

    #[test]
    fn ja4_proxy_env_requires_both_settings() {
        let header = ("EDGEZERO__ADAPTER__JA4_HEADER", "x-ja4");
        let proxies = ("EDGEZERO__ADAPTER__JA4_TRUSTED_PROXY_CIDRS", "10.0.0.0/8");
        assert!(
            TrustedJa4Header::from_env(&EnvConfig::from_vars(Vec::<(&str, &str)>::new()))
                .expect("should accept no config")
                .is_none()
        );
        assert!(
            TrustedJa4Header::from_env(&EnvConfig::from_vars([header, proxies]))
                .expect("should accept full config")
                .is_some()
        );
        for partial in [[header], [proxies]] {
            let _error = TrustedJa4Header::from_env(&EnvConfig::from_vars(partial))
                .expect_err("should require both settings");
        }
    }

    #[test]
    fn ja4_selection_requires_a_protected_path_and_no_generic_proxy() {
        use super::Ja4Selection;
        use crate::geo::{NativeGeo, NativeGeoPlan, ProtectedCloudFrontAlb};
        use edgezero_core::manifest::{BakedManifest, Manifest};
        let protected = Manifest::from_baked_json(
            r#"{"adapters":{"axum":{"ja4":{"source":"protected-cloudfront"}}}}"#,
        );
        let policy = ProtectedCloudFrontAlb::new(["10.0.0.0/24"], "x-origin-verify").unwrap();
        assert!(
            Ja4Selection::resolve(&BakedManifest::Absent, Some(policy.clone()), None)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            Ja4Selection::resolve(&BakedManifest::Absent, None, Some(config())),
            Ok(Some(Ja4Selection::Generic(_)))
        ));
        assert!(matches!(
            Ja4Selection::resolve(&protected, Some(policy.clone()), None),
            Ok(Some(Ja4Selection::ProtectedCloudFront(_)))
        ));
        let no_path = Ja4Selection::resolve(&protected, None, None).unwrap_err();
        assert!(no_path.to_string().contains("requires"), "{no_path}");
        let combined = Ja4Selection::resolve(&protected, Some(policy), Some(config())).unwrap_err();
        assert!(
            combined.to_string().contains("cannot be combined"),
            "{combined}"
        );
        assert!(
            NativeGeoPlan::Ready(NativeGeo::Off)
                .protected_path()
                .is_none(),
            "Geo off carries no protected path"
        );
    }
}

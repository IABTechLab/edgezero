//! Checked-IP composition tests run without either built-in implementation.

use super::*;
use crate::proxy::{ForwardingHeaderFamily, TrustedProxyPolicy, normalize};
use edgezero_core::geo::GeoError;
use edgezero_core::http::{HeaderValue, request_builder};
use std::sync::Mutex;

struct Counted(Arc<Mutex<Vec<GeoInput>>>);

impl GeoProvider for Counted {
    fn resolve(&self, input: &GeoInput) -> GeoResolution {
        self.0.lock().unwrap().push(input.clone());
        GeoResolution::NoMatch
    }
}

fn selected(expectation: ReaderIpExpectation, calls: &Arc<Mutex<Vec<GeoInput>>>) -> NativeGeo {
    NativeGeo::prepare(&NativeGeoSelection::SuppliedReaderIp {
        expectation,
        id: GeoProviderId::new("counted-reader").unwrap(),
        provider: Arc::new(Counted(Arc::clone(calls))),
    })
    .unwrap()
}

#[expect(
    clippy::too_many_lines,
    reason = "the table keeps both forwarding families and reader expectations under the same counted admission assertions"
)]
#[test]
fn geo_reader_admission_uses_private_checked_provenance() {
    for (family, name, source) in [
        (
            ForwardingHeaderFamily::Forwarded,
            "forwarded",
            GeoIpSource::TrustedForwarded,
        ),
        (
            ForwardingHeaderFamily::XForwarded,
            "x-forwarded-for",
            GeoIpSource::TrustedXForwarded,
        ),
    ] {
        let policy = TrustedProxyPolicy::new(family, ["127.0.0.0/8"]).unwrap();
        for expectation in [
            ReaderIpExpectation::PeerAllowed,
            ReaderIpExpectation::ForwardedRequired,
        ] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let geo = selected(expectation, &calls);
            for (raw_peer, value, address, provenance) in [
                (
                    Some("127.0.0.1:80"),
                    Some(if name == "forwarded" {
                        "for=203.0.113.9"
                    } else {
                        "203.0.113.9"
                    }),
                    Some("203.0.113.9"),
                    Some(source),
                ),
                (
                    Some("127.0.0.1:80"),
                    Some(if name == "forwarded" {
                        "for=\"[2001:db8::9]\""
                    } else {
                        "2001:db8::9"
                    }),
                    Some("2001:db8::9"),
                    Some(source),
                ),
                (
                    Some("127.0.0.1:80"),
                    Some(if name == "forwarded" {
                        "for=\"[::ffff:203.0.113.9]\""
                    } else {
                        "::ffff:203.0.113.9"
                    }),
                    Some("203.0.113.9"),
                    Some(source),
                ),
                (Some("127.0.0.1:80"), Some("malformed"), None, None),
                (
                    Some("127.0.0.1:80"),
                    Some(if name == "forwarded" {
                        "for=127.0.0.2"
                    } else {
                        "127.0.0.2"
                    }),
                    None,
                    None,
                ),
                (Some("127.0.0.1:80"), None, None, None),
                (
                    Some("192.0.2.1:80"),
                    Some(if name == "forwarded" {
                        "for=203.0.113.9"
                    } else {
                        "203.0.113.9"
                    }),
                    None,
                    None,
                ),
                (Some("[::ffff:192.0.2.1]:80"), None, None, None),
                (Some("[2001:db8::1]:80"), None, None, None),
                (
                    None,
                    Some(if name == "forwarded" {
                        "for=203.0.113.9"
                    } else {
                        "203.0.113.9"
                    }),
                    None,
                    None,
                ),
            ] {
                let peer: Option<SocketAddr> = raw_peer.map(|socket| socket.parse().unwrap());
                let (mut parts, ()) = request_builder().uri("/").body(()).unwrap().into_parts();
                if let Some(assertion) = value {
                    parts
                        .headers
                        .insert(name, HeaderValue::from_str(assertion).unwrap());
                }
                parts
                    .headers
                    .insert("cloudfront-viewer-country", HeaderValue::from_static("DE"));
                parts.extensions.insert(GeoInput::ReaderIp {
                    address: "198.51.100.1".parse().unwrap(),
                    source,
                });
                let checked = normalize(&policy, peer, &parts);
                let input = geo.reader_input(&checked);
                let expected = match (address, provenance) {
                    (Some(expected_address), Some(expected_source)) => Some(GeoInput::ReaderIp {
                        address: expected_address.parse().unwrap(),
                        source: expected_source,
                    }),
                    _ if expectation == ReaderIpExpectation::PeerAllowed && peer.is_some() => {
                        Some(GeoInput::ReaderIp {
                            address: checked.effective_client().unwrap(),
                            source: GeoIpSource::DirectPeer,
                        })
                    }
                    _ => None,
                };
                assert_eq!(input, expected, "{family:?} {expectation:?} {value:?}");
                let before = calls.lock().unwrap().len();
                let outcome = geo.resolve(peer, &parts.headers, input.as_ref());
                let reason = if input.is_some() {
                    GeoUnavailableReason::NoMatch
                } else {
                    GeoUnavailableReason::MissingReaderIp
                };
                assert_eq!(
                    outcome,
                    GeoOutcome::Selected {
                        provider: GeoProviderId::new("counted-reader").unwrap(),
                        status: GeoStatus::Unavailable(reason)
                    }
                );
                assert_eq!(
                    calls.lock().unwrap().len(),
                    before + usize::from(input.is_some())
                );
                if let Some(admitted_input) = input {
                    assert_eq!(calls.lock().unwrap().last(), Some(&admitted_input));
                }
            }
        }
    }
}

#[test]
fn geo_reader_capability_is_pure_and_configuration_debug_is_redacted() {
    let config = MaxMindGeoConfig::new(
        "MISSING_PATH_SENTINEL/not-present.mmdb",
        ReaderIpExpectation::ForwardedRequired,
    );
    let selection = NativeGeoSelection::MaxMind { config };
    assert!(!format!("{selection:?}").contains("MISSING_PATH_SENTINEL"));
    for _ in 0_u8..3 {
        let actual = selection.capability();
        if cfg!(all(feature = "geo-maxmind", unix)) {
            assert_eq!(
                actual.unwrap(),
                NativeGeoCapability::Selected {
                    provider: GeoProviderId::maxmind(),
                    input: NativeGeoInputContract::ReaderIp {
                        expectation: ReaderIpExpectation::ForwardedRequired
                    }
                }
            );
        } else {
            assert_eq!(
                actual.unwrap_err(),
                NativeGeoConfigError::UnavailableMaxMind
            );
        }
    }
    let mut invalid_config =
        MaxMindGeoConfig::new("PRIVATE_PATH", ReaderIpExpectation::PeerAllowed);
    invalid_config.limits.max_database_bytes = 0;
    assert_eq!(
        NativeGeoSelection::MaxMind {
            config: invalid_config
        }
        .capability()
        .unwrap_err(),
        NativeGeoConfigError::InvalidPreparationLimits
    );
}

#[test]
fn geo_reader_supplied_rejects_both_reserved_ids() {
    for id in [GeoProviderId::cloudfront(), GeoProviderId::maxmind()] {
        let selection = NativeGeoSelection::SuppliedReaderIp {
            expectation: ReaderIpExpectation::PeerAllowed,
            id,
            provider: Arc::new(Counted(Arc::default())),
        };
        assert_eq!(
            selection.capability().unwrap_err(),
            NativeGeoConfigError::ReservedProviderId
        );
        assert_eq!(
            NativeGeo::prepare(&selection).err(),
            Some(NativeGeoConfigError::ReservedProviderId)
        );
    }
}

#[test]
fn geo_reader_provider_failures_remain_selected_outcomes() {
    struct Failed;
    impl GeoProvider for Failed {
        fn resolve(&self, _input: &GeoInput) -> GeoResolution {
            GeoResolution::Error(GeoError::ProviderFailure)
        }
    }
    let geo = NativeGeo::prepare(&NativeGeoSelection::SuppliedReaderIp {
        expectation: ReaderIpExpectation::PeerAllowed,
        id: GeoProviderId::new("failed-reader").unwrap(),
        provider: Arc::new(Failed),
    })
    .unwrap();
    let input = GeoInput::ReaderIp {
        address: "192.0.2.7".parse().unwrap(),
        source: GeoIpSource::DirectPeer,
    };
    assert_eq!(
        geo.resolve(None, &HeaderMap::new(), Some(&input)),
        GeoOutcome::Selected {
            provider: GeoProviderId::new("failed-reader").unwrap(),
            status: GeoStatus::Error(GeoError::ProviderFailure)
        }
    );
}

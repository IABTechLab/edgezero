#![cfg_attr(
    unix,
    expect(
        clippy::absolute_paths,
        reason = "filesystem tests spell out standard-library operations"
    )
)]

use super::*;
use crate::geo::{MaxMindPreparationLimits, ReaderIpExpectation};
#[cfg(unix)]
use edgezero_core::geo::{GeoAssertion, GeoIpSource};
use std::path::PathBuf;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/geoip")
        .join(format!("{name}.mmdb"))
}

fn config(name: &str) -> MaxMindGeoConfig {
    MaxMindGeoConfig {
        path: fixture(name),
        limits: MaxMindPreparationLimits {
            max_database_bytes: 128 * 1024 * 1024,
            max_validation_networks: 32_000_000,
        },
        expectation: ReaderIpExpectation::PeerAllowed,
    }
}

#[cfg(unix)]
fn input(address: &str) -> GeoInput {
    GeoInput::ReaderIp {
        address: address.parse().unwrap(),
        source: GeoIpSource::DirectPeer,
    }
}

#[cfg(unix)]
fn located(country: &str, subdivision: Option<&str>) -> GeoResolution {
    GeoResolution::Located(GeoLocation::new(
        CountryCode::new(country).unwrap(),
        subdivision.map(|code| SubdivisionCode::new(code).unwrap()),
    ))
}

#[cfg(unix)]
#[test]
fn geo_maxmind_city_projection_and_request_code_validation() {
    let provider = MaxMindProvider::prepare(&config("city")).unwrap();
    let invalid = GeoResolution::Error(GeoError::InvalidAssertion);
    for (address, expected) in [
        ("127.0.0.1", located("US", Some("CA"))),
        ("192.0.2.1", located("US", Some("CA"))),
        ("192.0.2.2", located("GB", None)),
        ("192.0.2.3", located("JP", Some("01"))),
        ("192.0.2.4", located("US", Some("ZZ"))),
        ("192.0.2.5", located("US", None)),
        ("192.0.2.6", located("US", None)),
        ("192.0.2.7", GeoResolution::NoMatch),
        ("192.0.2.8", invalid.clone()),
        ("192.0.2.9", invalid.clone()),
        ("192.0.2.10", invalid.clone()),
        ("192.0.2.11", invalid.clone()),
        ("192.0.2.12", invalid.clone()),
        ("192.0.2.13", GeoResolution::NoMatch),
        ("192.0.2.14", GeoResolution::NoMatch),
        ("192.0.2.15", located("US", None)),
        ("192.0.2.16", located("US", Some("CA"))),
        ("192.0.2.17", located("US", Some("A1B"))),
        ("192.0.2.18", invalid.clone()),
        ("192.0.2.19", invalid.clone()),
        ("192.0.2.20", invalid),
        ("192.0.2.254", GeoResolution::NoMatch),
        ("10.0.0.1", GeoResolution::NoMatch),
        ("2001:db8::1", GeoResolution::NoMatch),
        ("::ffff:192.0.2.1", GeoResolution::NoMatch),
    ] {
        assert_eq!(provider.resolve(&input(address)), expected, "{address}");
    }
    for source in [
        GeoIpSource::DirectPeer,
        GeoIpSource::TrustedForwarded,
        GeoIpSource::TrustedXForwarded,
    ] {
        assert_eq!(
            provider.resolve(&GeoInput::ReaderIp {
                address: "192.0.2.1".parse().unwrap(),
                source
            }),
            located("US", Some("CA"))
        );
    }
    assert_eq!(
        provider.resolve(&GeoInput::Assertions {
            country: GeoAssertion::from_bytes(b"US"),
            subdivision: GeoAssertion::absent()
        }),
        GeoResolution::Error(GeoError::ProviderFailure)
    );
}

#[cfg(unix)]
#[test]
fn geo_maxmind_country_ignores_subdivisions_and_record_sizes_work() {
    let provider = MaxMindProvider::prepare(&config("country")).unwrap();
    assert_eq!(provider.resolve(&input("192.0.2.1")), located("US", None));
    for name in ["record-28", "record-32"] {
        let sized_provider = MaxMindProvider::prepare(&config(name)).unwrap();
        assert_eq!(
            sized_provider.resolve(&input("192.0.2.1")),
            located("US", Some("CA"))
        );
        assert_eq!(
            sized_provider.resolve(&input("198.51.100.1")),
            GeoResolution::NoMatch
        );
    }
}

#[cfg(unix)]
#[test]
fn geo_maxmind_ipv6_and_ipv4_subtree() {
    let provider = MaxMindProvider::prepare(&config("ipv6")).unwrap();
    assert_eq!(
        provider.resolve(&input("2001:db8::7")),
        located("GB", Some("ENG"))
    );
    assert_eq!(
        provider.resolve(&input("192.0.2.1")),
        located("US", Some("CA"))
    );
    assert_eq!(
        provider.resolve(&input("2001:db9::7")),
        GeoResolution::NoMatch
    );
    assert_eq!(
        provider.resolve(&input("192.0.2.2")),
        GeoResolution::NoMatch
    );
}

#[cfg(unix)]
#[test]
fn geo_maxmind_consumed_type_failures_are_startup_errors_even_off_path() {
    for name in [
        "bad-country-parent",
        "bad-country-code",
        "bad-subdivisions-parent",
        "bad-first-subdivision",
        "bad-subdivision-code",
    ] {
        assert!(
            matches!(
                MaxMindProvider::prepare(&config(name)),
                Err(NativeGeoConfigError::InvalidDatabaseSchema)
            ),
            "{name}"
        );
    }
}

#[cfg(unix)]
#[test]
#[ignore = "#438 acceptance blocker: maxminddb 0.32.0 exposes no strict raw metadata decoding"]
fn geo_maxmind_rejects_unused_metadata_nonstring_keys() {
    assert!(
        MaxMindProvider::prepare(&config("bad-metadata-key")).is_err(),
        "selected malformed metadata must fail startup; upstream verification currently accepts it"
    );
}

#[cfg(unix)]
#[test]
fn geo_maxmind_structural_pass_closes_upstream_data_gaps() {
    for name in ["bad-unrelated-key", "bad-unrelated-chain", "bad-root-chain"] {
        let reader = Reader::from_source(std::fs::read(fixture(name)).unwrap()).unwrap();
        reader.verify().unwrap(); // Regression: verification alone is insufficient.
        assert!(
            matches!(
                MaxMindProvider::prepare(&config(name)),
                Err(NativeGeoConfigError::InvalidDatabase)
            ),
            "{name}"
        );
    }
}

#[cfg(unix)]
#[test]
fn geo_maxmind_off_path_corruption_and_metadata_fail_preparation() {
    for name in [
        "bad-offpath-pointer",
        "bad-offpath-utf8",
        "bad-metadata",
        "bad-tree-cycle",
        "bad-separator",
        "truncated",
    ] {
        assert!(
            matches!(
                MaxMindProvider::prepare(&config(name)),
                Err(NativeGeoConfigError::InvalidDatabase)
            ),
            "{name}"
        );
    }
    assert!(matches!(
        MaxMindProvider::prepare(&config("unsupported")),
        Err(NativeGeoConfigError::UnsupportedDatabase)
    ));
}

#[cfg(unix)]
#[test]
fn geo_maxmind_limits_fail_without_partial_publication() {
    let mut settings = config("city");
    for bytes in [0, 1, usize::MAX] {
        settings.limits.max_database_bytes = bytes;
        assert!(matches!(
            MaxMindProvider::prepare(&settings),
            Err(NativeGeoConfigError::DatabaseLimit)
        ));
    }
    settings = config("city");
    let bytes = std::fs::read(&settings.path).unwrap();
    settings.limits.max_database_bytes = bytes.len();
    MaxMindProvider::prepare(&settings).unwrap();
    settings.limits.max_database_bytes = bytes.len().saturating_sub(1);
    assert!(matches!(
        MaxMindProvider::prepare(&settings),
        Err(NativeGeoConfigError::DatabaseLimit)
    ));
    settings = config("city");
    let reader = Reader::from_source(bytes).unwrap();
    let allowance = usize::try_from(reader.metadata().node_count)
        .unwrap()
        .saturating_mul(3)
        .saturating_add(1);
    settings.limits.max_validation_networks = allowance.saturating_sub(1);
    assert!(matches!(
        MaxMindProvider::prepare(&settings),
        Err(NativeGeoConfigError::DatabaseLimit)
    ));
    settings.limits.max_validation_networks = allowance;
    MaxMindProvider::prepare(&settings).unwrap();
    settings.limits.max_validation_networks = 0;
    assert!(matches!(
        MaxMindProvider::prepare(&settings),
        Err(NativeGeoConfigError::DatabaseLimit)
    ));
    let expansion =
        Reader::from_source(std::fs::read(fixture("expansion-limit")).unwrap()).unwrap();
    expansion.verify().unwrap();
    for name in [
        "expansion-limit",
        "verification-limit",
        "metadata-verification-limit",
    ] {
        assert!(
            matches!(
                MaxMindProvider::prepare(&config(name)),
                Err(NativeGeoConfigError::DatabaseLimit)
            ),
            "{name}"
        );
    }
    let scalar_provider = MaxMindProvider::prepare(&config("all-values")).unwrap();
    assert_eq!(
        scalar_provider.resolve(&input("192.0.2.1")),
        located("US", None)
    );
}

#[test]
fn geo_maxmind_actual_read_limit_checks_the_extra_byte() {
    use std::io::Cursor;
    assert_eq!(read_bounded(Cursor::new(b"1234"), 4).unwrap(), b"1234");
    assert!(matches!(
        read_bounded(Cursor::new(b"12345"), 4),
        Err(NativeGeoConfigError::DatabaseLimit)
    ));
    let mut source = Cursor::new(b"123456789");
    assert!(matches!(
        read_bounded(&mut source, 4),
        Err(NativeGeoConfigError::DatabaseLimit)
    ));
    assert_eq!(source.position(), 5);
    assert!(read_bounded(Cursor::new(b""), 4).unwrap().is_empty());
}

#[cfg(unix)]
#[test]
fn geo_maxmind_rejects_missing_directory_device_and_fifo_without_reading() {
    let mut settings = config("does-not-exist");
    assert!(matches!(
        MaxMindProvider::prepare(&settings),
        Err(NativeGeoConfigError::UnavailableDatabase)
    ));
    settings.path = fixture("city").parent().unwrap().to_owned();
    assert!(matches!(
        MaxMindProvider::prepare(&settings),
        Err(NativeGeoConfigError::UnavailableDatabase)
    ));
    settings.path = PathBuf::from("/dev/zero");
    assert!(matches!(
        MaxMindProvider::prepare(&settings),
        Err(NativeGeoConfigError::UnavailableDatabase)
    ));
    let path = temporary("fifo");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    settings.path = path.clone();
    let result = MaxMindProvider::prepare(&settings);
    std::fs::remove_file(path).unwrap();
    assert!(matches!(
        result,
        Err(NativeGeoConfigError::UnavailableDatabase)
    ));
}

#[cfg(unix)]
fn temporary(label: &str) -> PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "edgezero-geo-{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

#[cfg(unix)]
#[test]
fn geo_maxmind_owned_snapshot_survives_replacement_and_deletion() {
    use std::os::unix::fs::PermissionsExt as _;
    let path = temporary("snapshot.mmdb");
    std::fs::copy(fixture("city"), &path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
    let mut settings = config("city");
    settings.path = path.clone();
    let old = MaxMindProvider::prepare(&settings).unwrap();
    let original = old.metadata.clone();
    assert_eq!(original.database_type, "GeoLite2-City");
    assert_eq!(original.build_epoch, 1_793_750_400);
    assert_eq!(
        original.sha256,
        encode_string(&Sha256::digest(std::fs::read(&path).unwrap()))
    );
    let replacement = temporary("replacement.mmdb");
    std::fs::copy(fixture("city-replacement"), &replacement).unwrap();
    std::fs::rename(replacement, &path).unwrap();
    let new = MaxMindProvider::prepare(&settings).unwrap();
    assert_ne!(new.metadata.sha256, original.sha256);
    assert_ne!(new.metadata.build_epoch, original.build_epoch);
    std::fs::remove_file(path).unwrap();
    assert_eq!(old.metadata, original);
    assert_eq!(old.resolve(&input("127.0.0.1")), located("US", Some("CA")));
    assert_eq!(new.resolve(&input("127.0.0.1")), located("US", Some("NY")));
}

#[cfg(not(unix))]
#[test]
fn geo_maxmind_unsupported_platform_does_not_open_source() {
    assert!(matches!(
        MaxMindProvider::prepare(&config("city")),
        Err(NativeGeoConfigError::UnavailableDatabase)
    ));
}

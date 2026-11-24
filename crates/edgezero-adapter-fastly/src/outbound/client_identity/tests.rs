use std::cell::RefCell;
use std::collections::HashMap;

use edgezero_core::error::EdgeError;
use edgezero_core::http::StatusCode;
use edgezero_core::manifest::ManifestLoader;
use edgezero_core::outbound::{
    ClientIdentityError, ClientIdentityMaterial, ClientIdentityMetadata, ClientIdentityName,
    ClientIdentityPolicy,
};

use super::{
    BindingCache, BindingFailure, ConfigEntryReader, ConfigReadError, MAX_CERTIFICATE_BYTES,
    MAX_HOSTS_BYTES, binding_error, certificate_key, hosts_key,
};

const MANIFEST: &str = r#"
[capabilities.outbound.client-identities.mtls]
config-store = "certs"
secret-store = "keys"

[stores.config]
ids = ["certs"]

[stores.secrets]
ids = ["keys"]
"#;

type Entries = HashMap<(String, String), Result<Option<String>, ConfigReadError>>;

/// A config reader that serves fixed entries and records every read.
struct FakeReader {
    entries: Entries,
    reads: RefCell<Vec<(String, String, u64)>>,
}

impl FakeReader {
    fn new(
        entries: impl IntoIterator<Item = (&'static str, Result<Option<String>, ConfigReadError>)>,
    ) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(key, value)| (("certs".to_owned(), key.to_owned()), value))
                .collect(),
            reads: RefCell::new(Vec::new()),
        }
    }

    fn read_count(&self) -> usize {
        self.reads.borrow().len()
    }
}

impl ConfigEntryReader for FakeReader {
    fn read(&self, store: &str, key: &str, limit: u64) -> Result<Option<String>, ConfigReadError> {
        self.reads
            .borrow_mut()
            .push((store.to_owned(), key.to_owned(), limit));
        self.entries
            .get(&(store.to_owned(), key.to_owned()))
            .cloned()
            .unwrap_or(Ok(None))
    }
}

fn policy() -> ClientIdentityPolicy {
    ClientIdentityPolicy::from_manifest(ManifestLoader::load_from_str(MANIFEST).manifest())
}

fn metadata(policy: &ClientIdentityPolicy, name: &str) -> ClientIdentityMetadata {
    policy
        .identity(&ClientIdentityName::new(name).expect("name"))
        .expect("declared")
        .clone()
}

fn certificate() -> String {
    "-----BEGIN CERTIFICATE-----\nbGVhZiBkZXI=\n-----END CERTIFICATE-----\n".to_owned()
}

fn complete() -> FakeReader {
    FakeReader::new([
        ("mtls.hosts", Ok(Some("https://pbs.example\n".to_owned()))),
        ("mtls.certificate", Ok(Some(certificate()))),
    ])
}

fn material(failure: BindingFailure) -> ClientIdentityError {
    match failure {
        BindingFailure::Material(reason) => reason,
        BindingFailure::DeadlineExceeded | BindingFailure::Unavailable => {
            panic!("expected a material failure, got {failure:?}")
        }
    }
}

#[test]
fn entry_names_follow_the_identity_name() {
    let policy = policy();
    let identity = metadata(&policy, "mtls");
    assert_eq!(certificate_key(&identity), "mtls.certificate");
    assert_eq!(hosts_key(&identity), "mtls.hosts");
}

#[test]
fn binding_is_read_once_per_inbound_request() {
    let policy = policy();
    let identity = metadata(&policy, "mtls");
    let reader = complete();
    let cache = BindingCache::default();

    let first = cache.resolve(&identity, &policy, &reader).expect("binding");
    assert_eq!(
        first.hosts.origins().collect::<Vec<_>>(),
        [("pbs.example", 443)]
    );
    assert_eq!(
        reader.reads.borrow().clone(),
        [
            ("certs".to_owned(), "mtls.hosts".to_owned(), MAX_HOSTS_BYTES),
            (
                "certs".to_owned(),
                "mtls.certificate".to_owned(),
                MAX_CERTIFICATE_BYTES
            ),
        ]
    );

    let second = cache.resolve(&identity, &policy, &reader).expect("cached");
    assert_eq!(
        second.chain.chain_fingerprint(),
        first.chain.chain_fingerprint()
    );
    assert_eq!(reader.read_count(), 2, "the second dispatch reads nothing");

    let next_request = BindingCache::default();
    let _reread = next_request
        .resolve(&identity, &policy, &reader)
        .expect("reread");
    assert_eq!(
        reader.read_count(),
        4,
        "the next inbound request reads again"
    );
}

#[test]
fn material_failures_map_and_are_cached() {
    let policy = policy();
    let identity = metadata(&policy, "mtls");
    let cases = [
        (
            FakeReader::new([("mtls.certificate", Ok(Some(certificate())))]),
            ClientIdentityError::MaterialMissing {
                which: ClientIdentityMaterial::Hosts,
            },
        ),
        (
            FakeReader::new([("mtls.hosts", Ok(Some("https://pbs.example".to_owned())))]),
            ClientIdentityError::MaterialMissing {
                which: ClientIdentityMaterial::Certificate,
            },
        ),
        (
            FakeReader::new([
                ("mtls.hosts", Ok(Some("https://pbs.example".to_owned()))),
                ("mtls.certificate", Err(ConfigReadError::TooLarge)),
            ]),
            ClientIdentityError::MaterialInvalid {
                which: ClientIdentityMaterial::Certificate,
            },
        ),
        (
            FakeReader::new([("mtls.hosts", Err(ConfigReadError::TooLarge))]),
            ClientIdentityError::MaterialInvalid {
                which: ClientIdentityMaterial::Hosts,
            },
        ),
        (
            FakeReader::new([
                ("mtls.hosts", Ok(Some("https://*.example".to_owned()))),
                ("mtls.certificate", Ok(Some(certificate()))),
            ]),
            ClientIdentityError::MaterialInvalid {
                which: ClientIdentityMaterial::Hosts,
            },
        ),
        (
            FakeReader::new([
                ("mtls.hosts", Ok(Some("https://pbs.example".to_owned()))),
                ("mtls.certificate", Ok(Some("not a certificate".to_owned()))),
            ]),
            ClientIdentityError::MaterialInvalid {
                which: ClientIdentityMaterial::Certificate,
            },
        ),
        (
            FakeReader::new([("mtls.hosts", Err(ConfigReadError::StoreMissing))]),
            ClientIdentityError::MaterialMissing {
                which: ClientIdentityMaterial::Store,
            },
        ),
    ];
    for (reader, expected) in cases {
        let cache = BindingCache::default();
        let failure = cache
            .resolve(&identity, &policy, &reader)
            .expect_err("material failure");
        assert_eq!(material(failure), expected);
        let reads = reader.read_count();
        let again = cache
            .resolve(&identity, &policy, &reader)
            .expect_err("cached failure");
        assert_eq!(material(again), expected);
        assert_eq!(reader.read_count(), reads, "{expected:?} is cached");
    }
}

#[test]
fn deadline_and_unavailable_reads_are_not_cached() {
    let policy = policy();
    let identity = metadata(&policy, "mtls");
    for transient in [
        ConfigReadError::DeadlineExceeded,
        ConfigReadError::Unavailable,
    ] {
        let reader = FakeReader::new([("mtls.hosts", Err(transient))]);
        let cache = BindingCache::default();
        let failure = cache
            .resolve(&identity, &policy, &reader)
            .expect_err("transient");
        assert!(
            !matches!(failure, BindingFailure::Material(_)),
            "{failure:?}"
        );
        let _retry = cache
            .resolve(&identity, &policy, &reader)
            .expect_err("still transient");
        assert_eq!(reader.read_count(), 2, "{transient:?} is read again");
    }
}

#[test]
fn identity_without_a_config_store_is_missing_its_store() {
    let policy = ClientIdentityPolicy::from_manifest(
        ManifestLoader::load_from_str("[capabilities.outbound.client-identities.storeless]\n")
            .manifest(),
    );
    let identity = metadata(&policy, "storeless");
    let reader = complete();
    let failure = BindingCache::default()
        .resolve(&identity, &policy, &reader)
        .expect_err("no store");
    assert_eq!(
        material(failure),
        ClientIdentityError::MaterialMissing {
            which: ClientIdentityMaterial::Store
        }
    );
    assert_eq!(reader.read_count(), 0);
}

#[test]
fn binding_failures_become_request_errors() {
    let policy = policy();
    let identity = metadata(&policy, "mtls");
    let timeout = || EdgeError::gateway_timeout("deadline");

    let error = binding_error(
        &identity,
        BindingFailure::Material(ClientIdentityError::MaterialMissing {
            which: ClientIdentityMaterial::Key,
        }),
        timeout,
    );
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        error.client_identity_error(),
        Some(&ClientIdentityError::MaterialMissing {
            which: ClientIdentityMaterial::Key
        })
    );

    let deadline = binding_error(&identity, BindingFailure::DeadlineExceeded, timeout);
    assert_eq!(deadline.status(), StatusCode::GATEWAY_TIMEOUT);
    assert!(deadline.client_identity_error().is_none());

    let unavailable = binding_error(&identity, BindingFailure::Unavailable, timeout);
    assert_eq!(
        unavailable.client_identity_error(),
        Some(&ClientIdentityError::MaterialMissing {
            which: ClientIdentityMaterial::Store
        })
    );
}

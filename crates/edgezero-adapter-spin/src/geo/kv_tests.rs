#![expect(
    clippy::missing_trait_methods,
    clippy::too_many_lines,
    clippy::unnecessary_wraps,
    clippy::shadow_unrelated,
    clippy::assertions_on_result_states,
    clippy::clone_on_ref_ptr,
    clippy::default_numeric_fallback,
    clippy::absolute_paths,
    reason = "colocated fault matrices use explicit test providers, default record methods and compact repeated fixtures"
)]
use super::*;
use edgezero_core::app::StoreMetadata;
use edgezero_core::env_config::EnvConfig;
use edgezero_core::manifest::{BakedManifest, Manifest};
use std::time::Duration;

const PIN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn metadata() -> Option<StoreMetadata> {
    Some(StoreMetadata {
        ids: &["geo"],
        default: "geo",
    })
}

#[test]
fn kv_selection_uses_manifest_and_validated_overrides() {
    let json: &'static str = Box::leak(format!(r#"{{"stores":{{"kv":{{"ids":["geo"]}}}},"adapters":{{"spin":{{"geo":{{"provider":"kv","store_id":"geo","pin":"{PIN}","reader_ip":"authenticated-header","credential_variable":"geo_token"}}}}}}}}"#).into_boxed_str());
    let baked = Manifest::from_baked_json(json);
    assert!(matches!(baked, BakedManifest::Present(_)));
    let selected = SpinKvGeo::from_config(baked, &EnvConfig::default(), metadata())
        .unwrap()
        .unwrap();
    assert_eq!(selected.store_id, "geo");
    assert_eq!(selected.lookup_timeout, Duration::from_millis(250));
    let env = EnvConfig::from_vars([("EDGEZERO__ADAPTER__GEO__LOOKUP_TIMEOUT_MS", "17")]);
    assert_eq!(
        SpinKvGeo::from_config(baked, &env, metadata())
            .unwrap()
            .unwrap()
            .lookup_timeout,
        Duration::from_millis(17)
    );
    for (key, value) in [
        ("PROVIDER", "cloudfront"),
        ("STORE_ID", "missing"),
        ("PIN", "bad"),
        ("CREDENTIAL_VARIABLE", "bad__key"),
        ("READER_IP", "direct-peer"),
        ("LOOKUP_TIMEOUT_MS", "0"),
        ("LOOKUP_TIMEOUT_MS", "30001"),
        ("TYPO", "private"),
    ] {
        let env = EnvConfig::from_vars([(format!("EDGEZERO__ADAPTER__GEO__{key}"), value)]);
        assert!(
            SpinKvGeo::from_config(baked, &env, metadata()).is_err(),
            "{key}"
        );
    }
    assert!(SpinKvGeo::from_config(baked, &EnvConfig::default(), None).is_err());
    assert!(
        SpinKvGeo::from_config(
            BakedManifest::Malformed("broken"),
            &EnvConfig::default(),
            metadata()
        )
        .is_err()
    );
    assert!(
        SpinKvGeo::from_config(BakedManifest::Absent, &EnvConfig::default(), metadata())
            .unwrap()
            .is_none()
    );
    let env = EnvConfig::from_vars([("EDGEZERO__ADAPTER__GEO__STORE_ID", "geo")]);
    assert!(SpinKvGeo::from_config(BakedManifest::Absent, &env, metadata()).is_err());
}

use bytes::Bytes;
use edgezero_core::geo::kv::{Location, Range, Source, build, hex};
use edgezero_core::geo::{GeoOutcome, GeoSource, GeoStatus, GeoTrustBasis, GeoUnavailableReason};
use edgezero_core::http::{HeaderMap, HeaderValue};
use edgezero_core::key_value_store::{KvError, KvHandle, KvPage, KvStore};
use edgezero_core::secret_store::{SecretError, SecretStore};
use edgezero_core::time::MonotonicClock;
use futures::executor::block_on;
use std::collections::BTreeMap;
use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Memory {
    data: Mutex<BTreeMap<String, Bytes>>,
    reads: AtomicUsize,
    writes: AtomicUsize,
}
#[async_trait::async_trait(?Send)]
impl KvStore for Memory {
    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.data.lock().unwrap().get(key).cloned())
    }
    async fn put_bytes(&self, key: &str, value: Bytes) -> Result<(), KvError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.data.lock().unwrap().insert(key.to_owned(), value);
        Ok(())
    }
    async fn delete(&self, key: &str) -> Result<(), KvError> {
        self.data.lock().unwrap().remove(key);
        Ok(())
    }
    async fn list_keys_page(&self, _: &str, _: Option<&str>, _: usize) -> Result<KvPage, KvError> {
        panic!("serving must not list")
    }
    async fn put_bytes_with_ttl(&self, _: &str, _: Bytes, _: Duration) -> Result<(), KvError> {
        panic!("serving must not write TTL")
    }
}
struct Secret;
#[async_trait::async_trait(?Send)]
impl SecretStore for Secret {
    async fn get_bytes(&self, _: &str, _: &str) -> Result<Option<Bytes>, SecretError> {
        Ok(Some(Bytes::from_static(
            b"0123456789abcdef0123456789abcdef",
        )))
    }
    async fn get_bytes_bounded(
        &self,
        store_name: &str,
        key: &str,
        clock: &MonotonicClock,
        deadline: edgezero_core::time::Deadline,
        max_backend_bytes: u64,
        max_value_bytes: u64,
    ) -> Result<edgezero_core::config_store::BoundedStoreRead<Bytes>, SecretError> {
        edgezero_core::secret_store::finish_bounded_secret_read(
            self.get_bytes(store_name, key).await,
            clock,
            deadline,
            max_backend_bytes,
            max_value_bytes,
        )
    }
}

fn inputs(ip: Option<&str>, token: &str) -> CapturedGeo {
    let mut headers = HeaderMap::new();
    headers.insert(ingress::TOKEN_HEADER, HeaderValue::from_str(token).unwrap());
    if let Some(address) = ip {
        headers.insert(ingress::IP_HEADER, HeaderValue::from_str(address).unwrap());
    }
    let input = ingress::capture_inputs(&mut headers);
    assert!(headers.is_empty());
    input
}

#[test]
fn pinned_lookup_reaches_real_objects_and_never_writes() {
    block_on(async {
        let memory = Arc::new(Memory::default());
        let kv = KvHandle::new(memory.clone());
        let (receipt, _, _) = build(
            &kv,
            Source {
                edition: "synthetic".into(),
                files: vec![],
            },
            &[
                Range {
                    start: 1,
                    end: 10,
                    location: Location::new("US", "CA").unwrap(),
                },
                Range {
                    start: 20,
                    end: 30,
                    location: Location::new("GB", "").unwrap(),
                },
                Range {
                    start: 40,
                    end: 50,
                    location: Location::new("XK", "").unwrap(),
                },
            ],
            &[Range {
                start: 1,
                end: 10,
                location: Location::new("US", "CA").unwrap(),
            }],
            1024,
        )
        .await
        .unwrap();
        let pin = format!(
            "{}@{}",
            receipt.manifest_key.split('/').nth(2).unwrap(),
            hex(&receipt.manifest_hash)
        );
        let env = EnvConfig::from_vars([
            ("EDGEZERO__ADAPTER__GEO__PROVIDER", "kv"),
            ("EDGEZERO__ADAPTER__GEO__STORE_ID", "geo"),
            ("EDGEZERO__ADAPTER__GEO__PIN", pin.as_str()),
            ("EDGEZERO__ADAPTER__GEO__READER_IP", "authenticated-header"),
            ("EDGEZERO__ADAPTER__GEO__CREDENTIAL_VARIABLE", "geo_token"),
        ]);
        let selected = SpinKvGeo::from_config(BakedManifest::Absent, &env, metadata())
            .unwrap()
            .unwrap();
        let clock = MonotonicClock::default();
        let token = "0123456789abcdef0123456789abcdef";
        let writes = memory.writes.load(Ordering::SeqCst);
        for ip in ["0.0.0.1", "::1", "::ffff:0.0.0.1"] {
            let outcome = selected
                .resolve(
                    inputs(Some(ip), token),
                    &clock,
                    clock.deadline_after(Duration::from_secs(1)),
                    &Secret,
                    Some(kv.clone()),
                    |_| pending(),
                )
                .await;
            assert!(
                matches!(outcome, GeoOutcome::Selected { status: GeoStatus::Located { location, source: GeoSource::Kv, trust_basis: GeoTrustBasis::AuthenticatedIngressHeader }, .. } if location.country().as_str() == "US" && location.subdivision().unwrap().as_str() == "CA")
            );
        }
        let country_only = selected
            .resolve(
                inputs(Some("0.0.0.20"), token),
                &clock,
                clock.deadline_after(Duration::from_secs(1)),
                &Secret,
                Some(kv.clone()),
                |_| pending(),
            )
            .await;
        assert!(
            matches!(country_only, GeoOutcome::Selected { status: GeoStatus::Located { location, .. }, .. } if location.country().as_str() == "GB" && location.subdivision().is_none())
        );
        for (ip, reason) in [
            (Some("0.0.0.40"), GeoUnavailableReason::UnsupportedGeography),
            (Some("0.0.0.11"), GeoUnavailableReason::NoMatch),
            (None, GeoUnavailableReason::MissingReaderIp),
        ] {
            let result = selected
                .resolve(
                    inputs(ip, token),
                    &clock,
                    clock.deadline_after(Duration::from_secs(1)),
                    &Secret,
                    Some(kv.clone()),
                    |_| pending(),
                )
                .await;
            assert!(
                matches!(result, GeoOutcome::Selected { status: GeoStatus::Unavailable(actual), .. } if actual == reason)
            );
        }
        let reads = memory.reads.load(Ordering::SeqCst);
        for (ip, token) in [
            (Some("0.0.0.1"), "ffffffffffffffffffffffffffffffff"),
            (Some("0.0.0.1, 0.0.0.2"), token),
            (Some("0.0.0.1:80"), token),
        ] {
            let result = selected
                .resolve(
                    inputs(ip, token),
                    &clock,
                    clock.deadline_after(Duration::from_secs(1)),
                    &Secret,
                    Some(kv.clone()),
                    |_| pending(),
                )
                .await;
            assert!(matches!(
                result,
                GeoOutcome::Selected {
                    status: GeoStatus::Unavailable(GeoUnavailableReason::UntrustedIngress),
                    ..
                }
            ));
        }
        assert_eq!(reads, memory.reads.load(Ordering::SeqCst));
        assert_eq!(writes, memory.writes.load(Ordering::SeqCst));
        let elapsed = Arc::new(AtomicUsize::new(0));
        let observed = elapsed.clone();
        let started = clock.now();
        let advancing_clock = MonotonicClock::new(move || {
            started + Duration::from_millis(u64::try_from(observed.load(Ordering::SeqCst)).unwrap())
        });
        let slow = KvHandle::new(Arc::new(AdvancingKv {
            inner: kv.clone(),
            elapsed: elapsed.clone(),
        }));
        let result = selected
            .resolve(
                inputs(Some("0.0.0.1"), token),
                &advancing_clock,
                advancing_clock.deadline_after(Duration::from_secs(1)),
                &Secret,
                Some(slow),
                |_| pending(),
            )
            .await;
        assert!(
            matches!(
                result,
                GeoOutcome::Selected {
                    status: GeoStatus::Error(_),
                    ..
                }
            ),
            "four individually short reads must exceed one aggregate 250ms budget"
        );
        assert_eq!(elapsed.load(Ordering::SeqCst), 320);
        memory.data.lock().unwrap().remove(&receipt.manifest_key);
        let result = selected
            .resolve(
                inputs(Some("0.0.0.1"), token),
                &clock,
                clock.deadline_after(Duration::from_secs(1)),
                &Secret,
                Some(kv),
                |_| pending(),
            )
            .await;
        assert!(matches!(
            result,
            GeoOutcome::Selected {
                status: GeoStatus::Error(_),
                ..
            }
        ));
    });
}

#[test]
fn duplicate_credentials_and_ip_assertions_are_rejected_and_removed() {
    for header in [ingress::TOKEN_HEADER, ingress::IP_HEADER] {
        let mut headers = HeaderMap::new();
        headers.insert(
            ingress::TOKEN_HEADER,
            HeaderValue::from_static("0123456789abcdef0123456789abcdef"),
        );
        headers.insert(ingress::IP_HEADER, HeaderValue::from_static("192.0.2.1"));
        let duplicate = headers[header].clone();
        headers.append(header, duplicate);
        let captured = ingress::capture_inputs(&mut headers);
        assert!(headers.is_empty());
        if header == ingress::TOKEN_HEADER {
            assert!(captured.token.is_none());
        } else {
            assert!(captured.ip.is_err());
        }
    }
    for ip in [
        " 192.0.2.1",
        "192.0.2.1 ",
        "192.0.2.1,192.0.2.2",
        "[::1]",
        "fe80::1%eth0",
        "192.0.2.1:80",
    ] {
        assert!(
            inputs(Some(ip), "0123456789abcdef0123456789abcdef")
                .ip
                .is_err()
        );
    }
}

struct PendingKv(Arc<AtomicUsize>);
struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait(?Send)]
impl KvStore for PendingKv {
    async fn get_bytes(&self, _: &str) -> Result<Option<Bytes>, KvError> {
        let _guard = Dropped(self.0.clone());
        pending().await
    }
    async fn put_bytes(&self, _: &str, _: Bytes) -> Result<(), KvError> {
        panic!("read only")
    }
    async fn delete(&self, _: &str) -> Result<(), KvError> {
        panic!("read only")
    }
    async fn list_keys_page(&self, _: &str, _: Option<&str>, _: usize) -> Result<KvPage, KvError> {
        panic!("read only")
    }
    async fn put_bytes_with_ttl(&self, _: &str, _: Bytes, _: Duration) -> Result<(), KvError> {
        panic!("read only")
    }
}

#[test]
fn lookup_timeout_drops_pending_backend_work() {
    block_on(async {
        let env = EnvConfig::from_vars([
            ("EDGEZERO__ADAPTER__GEO__PROVIDER", "kv"),
            ("EDGEZERO__ADAPTER__GEO__STORE_ID", "geo"),
            ("EDGEZERO__ADAPTER__GEO__PIN", PIN),
            ("EDGEZERO__ADAPTER__GEO__READER_IP", "authenticated-header"),
            ("EDGEZERO__ADAPTER__GEO__CREDENTIAL_VARIABLE", "geo_token"),
        ]);
        let selection = SpinKvGeo::from_config(BakedManifest::Absent, &env, metadata())
            .unwrap()
            .unwrap();
        let clock = MonotonicClock::default();
        let drops = Arc::new(AtomicUsize::new(0));
        let timers = std::cell::Cell::new(0);
        let outcome = selection
            .resolve(
                inputs(Some("192.0.2.1"), "0123456789abcdef0123456789abcdef"),
                &clock,
                clock.deadline_after(Duration::from_secs(1)),
                &Secret,
                Some(KvHandle::new(Arc::new(PendingKv(drops.clone())))),
                |_| {
                    let call = timers.get();
                    timers.set(call + 1);
                    if call == 0 {
                        futures::future::Either::Left(pending())
                    } else {
                        futures::future::Either::Right(futures::future::ready(()))
                    }
                },
            )
            .await;
        assert!(matches!(
            outcome,
            GeoOutcome::Selected {
                status: GeoStatus::Error(_),
                ..
            }
        ));
        assert_eq!(timers.get(), 2);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "guest cancellation drops its pending read"
        );
    });
}

struct AdvancingKv {
    inner: KvHandle,
    elapsed: Arc<AtomicUsize>,
}
#[async_trait::async_trait(?Send)]
impl KvStore for AdvancingKv {
    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        self.elapsed.fetch_add(80, Ordering::SeqCst);
        self.inner.get_bytes(key).await
    }
    async fn put_bytes(&self, _: &str, _: Bytes) -> Result<(), KvError> {
        panic!("read only")
    }
    async fn delete(&self, _: &str) -> Result<(), KvError> {
        panic!("read only")
    }
    async fn list_keys_page(&self, _: &str, _: Option<&str>, _: usize) -> Result<KvPage, KvError> {
        panic!("read only")
    }
    async fn put_bytes_with_ttl(&self, _: &str, _: Bytes, _: Duration) -> Result<(), KvError> {
        panic!("read only")
    }
}

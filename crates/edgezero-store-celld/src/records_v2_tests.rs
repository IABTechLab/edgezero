use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvStore as _};
use edgezero_core::record_store::{
    RecordKeyLayout, RecordLayout, RecordWrite, RecordWriteMode, RecordWriteOutcome,
};
use edgezero_core::time::MonotonicClock;
use serde_json::json;
use url::Url;

use super::CelldRecordKvStore;
use crate::client::ScopedClient;
use crate::settings::{CelldAccess, CelldKvSettings, CelldPreparationLimits};
use crate::test_support::{Fixture, Reply};
use crate::{CelldPreparation, PreparationError};

struct Layout;
impl RecordKeyLayout for Layout {
    fn id(&self) -> &'static str {
        "fixture-groups"
    }
    fn version(&self) -> u32 {
        1
    }
    fn validate(&self) -> Result<(), KvError> {
        Ok(())
    }
    fn partition_prefix<'key>(&self, key: &'key str) -> Result<&'key str, KvError> {
        key.find('/')
            .and_then(|index| key.get(..=index))
            .ok_or_else(|| KvError::Validation("missing fixture group".to_owned()))
    }
    fn validate_partition(&self, prefix: &str) -> Result<(), KvError> {
        if prefix.ends_with('/') && prefix.matches('/').count() == 1 && prefix.len() > 1 {
            Ok(())
        } else {
            Err(KvError::Validation("invalid fixture group".to_owned()))
        }
    }
}
fn layout() -> RecordLayout {
    RecordLayout::new(Layout).unwrap()
}
fn reply(status: u16, value: &serde_json::Value) -> Reply {
    Reply::custom(
        status,
        "application/json",
        "x-celld-store-version: 1\r\nx-celld-record-version: 2\r\n",
        &serde_json::to_vec(value).unwrap(),
    )
}
fn ack() -> Reply {
    Reply::custom(
        204,
        "application/octet-stream",
        "x-celld-store-version: 1\r\nx-celld-record-version: 2\r\n",
        b"",
    )
}
fn capabilities() -> Reply {
    reply(
        200,
        &json!({"version":2_u8,"format":"records-v2","pagination":"keyset","no_expiry":true,"max_metadata_bytes":1_024_usize}),
    )
}
fn store(fixture: &Fixture) -> CelldRecordKvStore {
    store_with_limits(fixture, CelldPreparationLimits::default())
}
fn store_with_limits(fixture: &Fixture, limits: CelldPreparationLimits) -> CelldRecordKvStore {
    let endpoint = Url::parse(fixture.endpoint()).unwrap();
    let client = Arc::new(
        ScopedClient::new(endpoint.clone(), None, MonotonicClock::default(), limits).unwrap(),
    );
    CelldRecordKvStore::new(client, &endpoint, "state".to_owned(), limits, &layout()).unwrap()
}
fn write(mode: RecordWriteMode) -> RecordWrite<'static> {
    RecordWrite {
        body: b"\0\xff",
        metadata: Some("\u{e9}"),
        ttl: None,
        mode,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn preparation_requires_read_only_v2_capabilities_and_preserves_raw_setup() {
    let fixture = Fixture::new(vec![capabilities()]).await;
    let settings = CelldKvSettings {
        endpoint: fixture.endpoint().to_owned(),
        access: CelldAccess::TrustedNetwork {
            allow_remote_http: false,
        },
        store: "state".to_owned(),
    };
    let mut preparation =
        CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
            .unwrap();
    let kv = preparation
        .prepare_record_store(&settings, &layout(), None)
        .await
        .unwrap();
    assert_eq!(
        kv.require_records().unwrap().binding().format,
        "celld-records-v2"
    );
    let requests = fixture.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].target, "/v2/stores/state/record-capabilities");
    assert!(requests[0].body.is_empty());
    let raw = preparation.prepare_kv(&settings, None).await.unwrap();
    raw.require_records().unwrap_err();
    assert_eq!(fixture.count().await, 0, "raw preparation stays probe-free");
    for response in [
        Reply::response(200, "application/json", br#"{"keys":[]}"#),
        reply(
            200,
            &json!({"version":2_u8,"format":"records-v2","pagination":"keyset","no_expiry":false,"max_metadata_bytes":1_024_usize}),
        ),
        reply(
            200,
            &json!({"version":2_u8,"format":"records-v2","pagination":"keyset","no_expiry":true}),
        ),
    ] {
        let incompatible_fixture = Fixture::new(vec![response]).await;
        let incompatible_settings = CelldKvSettings {
            endpoint: incompatible_fixture.endpoint().to_owned(),
            ..settings.clone()
        };
        let mut incompatible_preparation =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .unwrap();
        incompatible_preparation
            .prepare_record_store(&incompatible_settings, &layout(), None)
            .await
            .unwrap_err();
        assert!(matches!(
            incompatible_preparation
                .prepare_record_store(&incompatible_settings, &layout(), None)
                .await,
            Err(PreparationError::InvalidBinding)
        ));
        assert_eq!(incompatible_fixture.count().await, 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_and_record_operations_use_only_v2_rows_and_native_no_expiry() {
    let fixture = Fixture::new(vec![
        ack(),
        ack(),
        reply(
            200,
            &json!({"body":"AP8=","metadata":"w6k=","generation":"18446744073709551615"}),
        ),
        ack(),
    ])
    .await;
    let kv = store(&fixture);
    kv.put_bytes("p/key", Bytes::from_static(b"\0\xff"))
        .await
        .unwrap();
    kv.put_bytes_with_ttl(
        "p/key",
        Bytes::new(),
        Duration::from_secs(60) + Duration::from_nanos(1),
    )
    .await
    .unwrap();
    let partition = layout().partition("p/").unwrap();
    let record = kv
        .record_lookup("p/key", &partition)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.body.as_ref(), b"\0\xff");
    assert_eq!(record.metadata.as_deref(), Some("\u{e9}"));
    assert_eq!(record.generation.as_u64(), u64::MAX);
    kv.record_delete("p/key", &partition).await.unwrap();
    let requests = fixture.requests().await;
    assert!(
        requests
            .iter()
            .all(|request| request.target.starts_with("/v2/stores/state/record?key="))
    );
    let plain: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert!(plain["ttl_ms"].is_null());
    assert!(plain["metadata"].is_null());
    assert_eq!(plain["mode"], "overwrite");
    assert_eq!(plain["body"], "AP8=");
    let ttl: serde_json::Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(ttl["ttl_ms"], "60001");
}

#[tokio::test(flavor = "current_thread")]
async fn uncertain_mutations_are_payload_free_and_never_replayed() {
    for delete in [false, true] {
        for stall in [false, true] {
            let fixture = Fixture::new(vec![if stall { Reply::Stall } else { Reply::Drop }]).await;
            let kv = store_with_limits(
                &fixture,
                CelldPreparationLimits {
                    operation_timeout_ms: 40,
                    connect_timeout_ms: 10,
                    ..CelldPreparationLimits::default()
                },
            );
            let error = if delete {
                kv.delete("p/private-key").await.unwrap_err()
            } else {
                kv.put_bytes("p/private-key", Bytes::from_static(b"private-body"))
                    .await
                    .unwrap_err()
            };
            assert!(matches!(error, KvError::MutationUncertain));
            assert!(!format!("{error:?} {error}").contains("private"));
            assert_eq!(fixture.count().await, 1);
        }
    }
    let fixture = Fixture::new(vec![Reply::response(204, "application/octet-stream", b"")]).await;
    assert!(
        matches!(
            store(&fixture).delete("p/key").await,
            Err(KvError::MutationUncertain)
        ),
        "missing v2 response header after dispatch is uncertain"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn only_definitive_add_or_cas_failure_is_contention() {
    let fixture = Fixture::new(vec![
        reply(412, &json!({"error":"precondition_failed"})),
        reply(412, &json!({"error":"precondition_failed"})),
        reply(412, &json!({"error":"precondition_failed"})),
    ])
    .await;
    let kv = store(&fixture);
    let partition = layout().partition("p/").unwrap();
    assert_eq!(
        kv.record_insert("p/key", &partition, write(RecordWriteMode::Add))
            .await
            .unwrap(),
        RecordWriteOutcome::PreconditionFailed
    );
    assert_eq!(
        kv.record_insert(
            "p/key",
            &partition,
            write(RecordWriteMode::IfGenerationMatch(
                edgezero_core::RecordGeneration::from_u64(1)
            ))
        )
        .await
        .unwrap(),
        RecordWriteOutcome::PreconditionFailed
    );
    assert!(matches!(
        kv.record_insert("p/key", &partition, write(RecordWriteMode::Overwrite))
            .await,
        Err(KvError::Unavailable)
    ));
    assert_eq!(fixture.count().await, 3);
}

#[tokio::test(flavor = "current_thread")]
async fn validated_rejections_are_not_ambiguous_but_untrusted_failures_are() {
    let fixture = Fixture::new(vec![
        reply(413, &json!({"error":"limit_exceeded"})),
        reply(403, &json!({"error":"forbidden"})),
        reply(503, &json!({"error":"unavailable"})),
        reply(413, &json!({"error":"wrong_error_body"})),
    ])
    .await;
    let kv = store(&fixture);
    assert!(matches!(
        kv.delete("p/key").await,
        Err(KvError::LimitExceeded { .. })
    ));
    assert!(matches!(
        kv.delete("p/key").await,
        Err(KvError::Unavailable)
    ));
    assert!(matches!(
        kv.delete("p/key").await,
        Err(KvError::MutationUncertain)
    ));
    assert!(matches!(
        kv.delete("p/key").await,
        Err(KvError::MutationUncertain)
    ));
    assert_eq!(fixture.count().await, 4);
}

#[tokio::test(flavor = "current_thread")]
async fn metadata_is_strict_utf8_and_empty_or_missing_is_none() {
    let fixture = Fixture::new(vec![
        reply(200, &json!({"body":"","metadata":"/w==","generation":"1"})),
        reply(200, &json!({"body":"","metadata":"","generation":"2"})),
        reply(200, &json!({"body":"","metadata":null,"generation":"3"})),
    ])
    .await;
    let kv = store(&fixture);
    let partition = layout().partition("p/").unwrap();
    assert!(matches!(
        kv.record_lookup("p/key", &partition).await,
        Err(KvError::Unavailable)
    ));
    assert_eq!(
        kv.record_lookup("p/key", &partition)
            .await
            .unwrap()
            .unwrap()
            .metadata,
        None
    );
    assert_eq!(
        kv.record_lookup("p/key", &partition)
            .await
            .unwrap()
            .unwrap()
            .metadata,
        None
    );
}

#[tokio::test(flavor = "current_thread")]
async fn pages_bind_scope_prefix_operation_and_progress_with_native_continuation() {
    let fixture = Fixture::new(vec![
        reply(200, &json!({"keys":["p/a"],"cursor":"native-one"})),
        reply(200, &json!({"keys":[],"cursor":"native-two"})),
        reply(200, &json!({"keys":["p/a"],"cursor":null})),
    ])
    .await;
    let kv = store(&fixture);
    let first = kv.list_keys_page("p/", None, 2).await.unwrap();
    let token = first.cursor.unwrap();
    let partition = layout().partition("p/").unwrap();
    kv.record_list_partition(&partition, Some(&token), 2)
        .await
        .unwrap_err();
    kv.list_keys_page("q/", Some(&token), 2).await.unwrap_err();
    let other = Fixture::new(vec![]).await;
    store(&other)
        .list_keys_page("p/", Some(&token), 2)
        .await
        .unwrap_err();
    assert_eq!(other.count().await, 0);
    let second = kv.list_keys_page("p/", Some(&token), 2).await.unwrap();
    assert!(second.keys.is_empty());
    assert!(second.cursor.is_some());
    kv.list_keys_page("p/", second.cursor.as_deref(), 2)
        .await
        .expect_err("repeated prior key after an empty page must fail");
    let requests = fixture.requests().await;
    assert_eq!(requests.len(), 3);
    assert!(requests[1].target.contains("cursor=native-one"));
    assert!(requests[2].target.contains("cursor=native-two"));
    assert!(
        requests
            .iter()
            .all(|request| request.target.starts_with("/v2/stores/state/record-keys?"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_direct_inputs_fail_before_transport() {
    let fixture = Fixture::new(vec![]).await;
    let kv = store(&fixture);
    let partition = layout().partition("p/").unwrap();
    kv.put_bytes("not-declared", Bytes::new())
        .await
        .unwrap_err();
    kv.put_bytes_with_ttl("p/key", Bytes::new(), Duration::from_secs(59))
        .await
        .unwrap_err();
    kv.list_keys_page("", None, 0).await.unwrap_err();
    kv.list_keys_page("", None, 1001).await.unwrap_err();
    let mut oversized = write(RecordWriteMode::Overwrite);
    let metadata = "\u{e9}".repeat(513);
    oversized.metadata = Some(&metadata);
    kv.record_insert("p/key", &partition, oversized)
        .await
        .unwrap_err();
    assert_eq!(fixture.count().await, 0);
}

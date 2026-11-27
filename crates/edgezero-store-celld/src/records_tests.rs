use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvStore as _};
use edgezero_core::record_store::{
    RecordGeneration, RecordKeyLayout, RecordLayout, RecordWrite, RecordWriteMode,
    RecordWriteOutcome,
};
use edgezero_core::time::MonotonicClock;
use serde_json::{Value, json};
use url::Url;

use super::CelldRecordKvStore;
use crate::client::ScopedClient;
use crate::settings::{CelldAccess, CelldKvSettings, CelldPreparationLimits};
use crate::test_support::{Fixture, Reply};
use crate::{CelldPreparation, PreparationError};

const INSTANCE: &str = "AAAAAAAAAAAAAAAAAAAAAA";
const OTHER_INSTANCE: &str = "BBBBBBBBBBBBBBBBBBBBBB";

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
fn headers(instance: &str) -> String {
    format!(
        "x-edgezero-store-version: 1\r\nx-edgezero-record-version: 1\r\nx-edgezero-store-instance: {instance}\r\n"
    )
}
fn reply(status: u16, value: &Value) -> Reply {
    Reply::custom(
        status,
        "application/json",
        &headers(INSTANCE),
        &serde_json::to_vec(value).unwrap(),
    )
}
fn ack() -> Reply {
    Reply::custom(204, "application/json", &headers(INSTANCE), b"")
}
fn capability_value(instance: &str) -> Value {
    json!({"service":"edgezero-celld-store","protocol":1,"format":"rows-v1","pagination":"keyset",
        "no_expiry":true,"mutation_ledger":true,"records":true,"record_protocol":1,
        "max_value_bytes":0x0010_0000_u64,"max_metadata_bytes":1_024_usize,"instance":instance})
}
fn capabilities() -> Reply {
    reply(200, &capability_value(INSTANCE))
}
async fn store_with_limits(
    fixture: &Fixture,
    limits: CelldPreparationLimits,
) -> CelldRecordKvStore {
    let endpoint = Url::parse(fixture.endpoint()).unwrap();
    let clock = MonotonicClock::default();
    let client = Arc::new(ScopedClient::new(endpoint.clone(), None, clock, limits).unwrap());
    let deadline = client.deadline().unwrap();
    CelldRecordKvStore::prepare(
        client,
        &endpoint,
        "state".to_owned(),
        limits,
        &layout(),
        deadline,
    )
    .await
    .unwrap()
}
async fn store(fixture: &Fixture) -> CelldRecordKvStore {
    store_with_limits(fixture, CelldPreparationLimits::default()).await
}
fn write(mode: RecordWriteMode) -> RecordWrite<'static> {
    RecordWrite {
        body: b"\0\xff",
        metadata: Some("\u{e9}"),
        ttl: None,
        mode,
    }
}
fn settings(fixture: &Fixture) -> CelldKvSettings {
    CelldKvSettings {
        endpoint: fixture.endpoint().to_owned(),
        store: "state".to_owned(),
        access: CelldAccess::TrustedNetwork {
            allow_remote_http: false,
        },
    }
}

#[tokio::test(flavor = "current_thread")]
async fn preparation_checks_read_only_stock_capabilities_and_keeps_raw_setup_probe_free() {
    let fixture = Fixture::new(vec![capabilities()]).await;
    let mut prep =
        CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
            .unwrap();
    let kv = prep
        .prepare_record_store(&settings(&fixture), &layout(), None)
        .await
        .unwrap();
    assert_eq!(
        kv.require_records().unwrap().binding().format,
        "celld-store-records-v1"
    );
    let requests = fixture.requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].target, "/v1/stores/state/capabilities");
    assert_eq!(requests[0].record_version.as_deref(), Some("1"));
    assert!(requests[0].mutation_id.is_none());
    assert!(requests[0].expected_instance.is_none());
    assert!(requests[0].body.is_empty());
    prep.prepare_kv(&settings(&fixture), None)
        .await
        .unwrap()
        .require_records()
        .unwrap_err();
    assert_eq!(fixture.count().await, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn incompatible_capabilities_poison_preparation_without_fallback() {
    let mut replies = vec![Reply::response(
        200,
        "application/json",
        &serde_json::to_vec(&capability_value(INSTANCE)).unwrap(),
    )];
    for (field, value) in [
        ("records", json!(false)),
        ("no_expiry", json!(false)),
        ("mutation_ledger", json!(false)),
        ("record_protocol", json!(2_u8)),
        ("format", json!("records-v2")),
        ("pagination", json!("single-page")),
        ("instance", json!(OTHER_INSTANCE)),
    ] {
        let mut caps = capability_value(INSTANCE);
        caps[field] = value;
        replies.push(reply(200, &caps));
    }
    for response in replies {
        let fixture = Fixture::new(vec![response]).await;
        let mut prep =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .unwrap();
        prep.prepare_record_store(&settings(&fixture), &layout(), None)
            .await
            .unwrap_err();
        assert_eq!(
            prep.prepare_kv(&settings(&fixture), None)
                .await
                .unwrap_err(),
            PreparationError::InvalidBinding
        );
        assert_eq!(fixture.count().await, 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_and_record_methods_use_stock_rows_and_expected_instance_on_every_operation() {
    let fixture = Fixture::new(vec![
        capabilities(),
        ack(),
        ack(),
        reply(
            200,
            &json!({"body":"AP8=","metadata":"w6k=","generation":"18446744073709551615"}),
        ),
        Reply::custom(200, "application/json", &headers(INSTANCE), b""),
        ack(),
        ack(),
    ])
    .await;
    let kv = store(&fixture).await;
    kv.put_bytes("p/a b/\u{e9}", Bytes::from_static(b"ordinary"))
        .await
        .unwrap();
    kv.put_bytes_with_ttl("p/k", Bytes::from_static(b"ttl"), Duration::from_secs(60))
        .await
        .unwrap();
    let value = kv.lookup("p/k").await.unwrap().unwrap();
    assert_eq!(value.body.as_ref(), b"\0\xff");
    assert_eq!(value.metadata.as_deref(), Some("\u{e9}"));
    assert_eq!(value.generation.as_u64(), u64::MAX);
    assert!(kv.exists_value("p/k").await.unwrap());
    assert_eq!(
        kv.insert(
            "p/k",
            write(RecordWriteMode::IfGenerationMatch(value.generation))
        )
        .await
        .unwrap(),
        RecordWriteOutcome::Written
    );
    kv.delete("p/k").await.unwrap();
    let requests = fixture.requests().await;
    assert_eq!(requests.len(), 7);
    let mut ids = BTreeSet::new();
    for request in &requests[1..] {
        assert!(request.target.starts_with("/v1/stores/state/record"));
        assert_eq!(request.record_version.as_deref(), Some("1"));
        assert_eq!(request.expected_instance.as_deref(), Some(INSTANCE));
        if request.method == "POST" {
            assert!(ids.insert(request.mutation_id.as_deref().unwrap()));
        }
    }
    assert_eq!(ids.len(), 4);
    let body: Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(body["mode"], "overwrite");
    assert!(body["metadata"].is_null());
    assert!(body["ttl_ms"].is_null());
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[2].body).unwrap()["ttl_ms"],
        60_000_u64
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[5].body).unwrap()["generation"],
        u64::MAX.to_string()
    );
    assert_eq!(requests[6].method, "POST");
    assert!(requests[6].target.contains("/record-delete?"));
    assert!(requests[6].body.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn only_validated_conditional_rejection_is_precondition_failed() {
    let rejected = || reply(412, &json!({"error":"precondition_failed"}));
    let fixture = Fixture::new(vec![capabilities(), rejected(), rejected(), rejected()]).await;
    let kv = store(&fixture).await;
    for mode in [
        RecordWriteMode::Add,
        RecordWriteMode::IfGenerationMatch(RecordGeneration::from_u64(1)),
    ] {
        assert_eq!(
            kv.insert("p/k", write(mode)).await.unwrap(),
            RecordWriteOutcome::PreconditionFailed
        );
    }
    assert!(matches!(
        kv.insert("p/k", write(RecordWriteMode::Overwrite)).await,
        Err(KvError::Unavailable)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn missing_keys_require_a_store_instance_not_a_proxy_404() {
    let fixture = Fixture::new(vec![
        capabilities(),
        reply(404, &json!({"error":"not_found"})),
        Reply::custom(404, "application/json", &headers(INSTANCE), b""),
        Reply::custom(
            404,
            "application/json",
            "x-edgezero-store-version: 1\r\nx-edgezero-record-version: 1\r\n",
            br#"{"error":"not_found"}"#,
        ),
    ])
    .await;
    let kv = store(&fixture).await;
    assert!(kv.lookup("p/k").await.unwrap().is_none());
    assert!(!kv.exists_value("p/k").await.unwrap());
    assert!(matches!(kv.lookup("p/k").await, Err(KvError::Unavailable)));
}

#[tokio::test(flavor = "current_thread")]
async fn instance_change_invalidates_handles_and_scope_and_never_retries_mutations() {
    let different = |status, body: &Value| {
        Reply::custom(
            status,
            "application/json",
            &headers(OTHER_INSTANCE),
            &serde_json::to_vec(body).unwrap(),
        )
    };
    let fixture = Fixture::new(vec![
        capabilities(),
        different(200, &capability_value(OTHER_INSTANCE)),
        different(
            200,
            &json!({"body":"eA==","metadata":null,"generation":"1"}),
        ),
        different(204, &json!({})),
    ])
    .await;
    let first = store(&fixture).await;
    let second = store(&fixture).await;
    assert_ne!(first.binding.scope, second.binding.scope);
    assert!(matches!(
        first.lookup("p/k").await,
        Err(KvError::Unavailable)
    ));
    assert!(matches!(
        first.insert("p/k", write(RecordWriteMode::Add)).await,
        Err(KvError::MutationUncertain)
    ));
    assert_eq!(fixture.count().await, 4);
}

#[tokio::test(flavor = "current_thread")]
async fn lost_ack_and_deadline_are_uncertain_without_transport_replay() {
    for response in [Reply::Drop, Reply::Stall] {
        let fixture = Fixture::new(vec![capabilities(), response]).await;
        let limits = CelldPreparationLimits {
            operation_timeout_ms: 100,
            connect_timeout_ms: 50,
            ..CelldPreparationLimits::default()
        };
        let kv = store_with_limits(&fixture, limits).await;
        assert!(matches!(
            kv.insert("p/k", write(RecordWriteMode::Add)).await,
            Err(KvError::MutationUncertain)
        ));
        assert_eq!(
            fixture.count().await,
            2,
            "one capability request and one mutation, no replay"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn complete_partition_and_global_pages_keep_separate_scoped_continuations() {
    let fixture = Fixture::new(vec![
        capabilities(),
        reply(200, &json!({"keys":["p/a"],"cursor":"first"})),
        reply(200, &json!({"keys":[],"cursor":"second"})),
        reply(200, &json!({"keys":["p/b"],"cursor":null})),
        reply(200, &json!({"keys":["q/a"],"cursor":null})),
    ])
    .await;
    let kv = store(&fixture).await;
    let first = kv.page("p/", None, 1, "partition").await.unwrap();
    let cursor = first.cursor.unwrap();
    kv.page("q/", Some(&cursor), 1, "partition")
        .await
        .unwrap_err();
    kv.page("p/", Some(&cursor), 1, "ordinary")
        .await
        .unwrap_err();
    let empty = kv.page("p/", Some(&cursor), 1, "partition").await.unwrap();
    assert!(empty.keys.is_empty());
    let last = kv
        .page("p/", empty.cursor.as_deref(), 1, "partition")
        .await
        .unwrap();
    assert_eq!(last.keys, ["p/b"]);
    assert!(last.cursor.is_none());
    assert_eq!(kv.list_keys_page("", None, 1).await.unwrap().keys, ["q/a"]);
    let requests = fixture.requests().await;
    assert_eq!(requests.len(), 5);
    assert!(requests[1].target.contains("mode=partition"));
    assert!(requests[4].target.contains("mode=ordinary"));
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_metadata_generations_and_nonadvancing_pages_are_errors() {
    let fixture = Fixture::new(vec![
        capabilities(),
        reply(
            200,
            &json!({"body":"YQ==","metadata":"/w==","generation":"1"}),
        ),
        reply(
            200,
            &json!({"body":"YQ==","metadata":null,"generation":"01"}),
        ),
        reply(
            200,
            &json!({"body":"YQ==","metadata":null,"generation":"18446744073709551616"}),
        ),
        reply(200, &json!({"keys":["p/a"],"cursor":"same"})),
        reply(200, &json!({"keys":[],"cursor":"same"})),
    ])
    .await;
    let kv = store(&fixture).await;
    for _ in 0_u8..3_u8 {
        assert!(matches!(kv.lookup("p/k").await, Err(KvError::Unavailable)));
    }
    let page = kv.page("p/", None, 1, "partition").await.unwrap();
    kv.page("p/", page.cursor.as_deref(), 1, "partition")
        .await
        .unwrap_err();
}

#[tokio::test(flavor = "current_thread")]
async fn value_limits_are_effective_and_record_ttl_is_encoded_as_integer_milliseconds() {
    let fixture = Fixture::new(vec![capabilities(), ack(), ack()]).await;
    let limits = CelldPreparationLimits {
        max_value_bytes: 8,
        ..CelldPreparationLimits::default()
    };
    let kv = store_with_limits(&fixture, limits).await;
    assert!(
        kv.put_bytes("p/k", Bytes::from_static(b"too-large"))
            .await
            .is_err()
    );
    assert_eq!(fixture.count().await, 1);
    let mut value = write(RecordWriteMode::Overwrite);
    value.ttl = Some(Duration::from_nanos(1));
    kv.insert("p/k", value).await.unwrap();
    value.ttl = Some(Duration::ZERO);
    kv.insert("p/k", value).await.unwrap_err();
    value.ttl = None;
    value.metadata = Some("");
    kv.insert("p/k", value).await.unwrap();
    let requests = fixture.requests().await;
    assert_eq!(requests.len(), 3);
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[1].body).unwrap()["ttl_ms"],
        1_u64
    );
    assert!(serde_json::from_slice::<Value>(&requests[2].body).unwrap()["metadata"].is_null());
}

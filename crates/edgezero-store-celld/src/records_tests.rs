use std::time::Duration;

use edgezero_core::time::MonotonicClock;
use serde_json::{Value, json};
use url::Url;

use super::{CelldRecordStore, RecordWrite, RecordWriteMode, RecordWriteOutcome};
use crate::settings::{CelldAccess, CelldKvSettings, CelldPreparationLimits};
use crate::test_support::{Fixture, Reply};
use crate::{CelldPreparation, PreparationError};

fn reply(status: u16, body: &Value) -> Reply {
    Reply::custom(
        status,
        if status == 204 {
            "application/octet-stream"
        } else {
            "application/json"
        },
        "x-celld-store-version: 1\r\nx-celld-record-version: 1\r\n",
        if status == 204 {
            Vec::new()
        } else {
            serde_json::to_vec(body).expect("should serialize record fixture")
        }
        .as_slice(),
    )
}

fn probe() -> Reply {
    reply(200, &json!({"keys":[]}))
}

fn settings(fixture: &Fixture) -> CelldKvSettings {
    CelldKvSettings {
        endpoint: fixture.endpoint().to_owned(),
        access: CelldAccess::TrustedNetwork {
            allow_remote_http: false,
        },
        store: "identity".to_owned(),
    }
}

async fn prepare(fixture: &Fixture) -> CelldRecordStore {
    let mut session =
        CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
            .expect("should create session");
    session
        .prepare_records(&settings(fixture), None)
        .await
        .expect("should prepare records")
}

#[tokio::test(flavor = "current_thread")]
async fn opaque_bytes_live_reads_and_full_u64_generation_round_trip() {
    let fixture = Fixture::new(vec![
        probe(),
        reply(
            200,
            &json!({"body":"/wA=","metadata":"AA==","generation":u64::MAX.to_string()}),
        ),
        reply(
            200,
            &json!({"body":"","metadata":"","generation":"9007199254740993"}),
        ),
        reply(404, &json!({"error":"not_found"})),
    ])
    .await;
    let store = prepare(&fixture).await;
    let key = "scope:\u{e9} /?+";
    let first = store
        .lookup(key)
        .await
        .expect("should read record")
        .expect("should find record");
    assert_eq!(first.body, [255, 0]);
    assert_eq!(first.metadata, Some(vec![0]));
    assert_eq!(first.generation, u64::MAX);
    let second = store
        .lookup(key)
        .await
        .expect("should re-read live state")
        .expect("should find changed row");
    assert!(second.body.is_empty());
    assert_eq!(second.metadata, Some(vec![]));
    assert_eq!(second.generation, 0x0020_0000_0000_0001);
    assert!(
        store
            .lookup(key)
            .await
            .expect("should distinguish miss")
            .is_none()
    );
    let requests = fixture.requests().await;
    assert_eq!(
        requests[0].target,
        "/v1/stores/identity/record-keys?prefix=&limit=1"
    );
    let url =
        Url::parse(&format!("http://fixture{}", requests[1].target)).expect("should parse target");
    assert_eq!(
        url.query_pairs()
            .find(|(name, _)| name == "key")
            .expect("should include key")
            .1,
        key
    );
    assert_eq!(requests.len(), 4);
    assert!(!format!("{store:?} {first:?}").contains("identity"));
}

#[tokio::test(flavor = "current_thread")]
async fn conditional_modes_and_conflicts_have_exact_wire_semantics() {
    let fixture = Fixture::new(vec![
        probe(),
        reply(204, &Value::Null),
        reply(204, &Value::Null),
        reply(412, &json!({"error":"precondition_failed"})),
    ])
    .await;
    let store = prepare(&fixture).await;
    for (mode, expected) in [
        (RecordWriteMode::Add, RecordWriteOutcome::Written),
        (RecordWriteMode::Overwrite, RecordWriteOutcome::Written),
        (
            RecordWriteMode::IfGenerationMatch(u64::MAX),
            RecordWriteOutcome::PreconditionFailed,
        ),
    ] {
        assert_eq!(
            store
                .insert(
                    "root",
                    RecordWrite {
                        body: b"entry",
                        metadata: Some(&[255]),
                        ttl: Duration::from_nanos(1),
                        mode
                    }
                )
                .await
                .expect("should classify write"),
            expected
        );
    }
    let requests = fixture.requests().await;
    assert_eq!(requests.len(), 4);
    for (index, mode) in ["add", "overwrite", "if-generation-match"]
        .iter()
        .enumerate()
    {
        let request = &requests[index + 1];
        assert_eq!(request.method, "PUT");
        let body: Value = serde_json::from_slice(&request.body).expect("should decode request");
        assert_eq!(body["body"], "ZW50cnk=");
        assert_eq!(body["metadata"], "/w==");
        assert_eq!(body["ttl_ms"], "1");
        assert_eq!(body["mode"], *mode);
        if index == 2 {
            assert_eq!(body["generation"], u64::MAX.to_string());
        } else {
            assert!(body.get("generation").is_none());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn physical_head_and_delete_use_the_record_route() {
    let header = "x-celld-store-version: 1\r\nx-celld-record-version: 1\r\n";
    let fixture = Fixture::new(vec![
        probe(),
        Reply::custom(200, "application/octet-stream", header, b""),
        Reply::custom(404, "application/octet-stream", header, b""),
        reply(204, &Value::Null),
    ])
    .await;
    let store = prepare(&fixture).await;
    assert!(
        store
            .key_exists("expired")
            .await
            .expect("should query physical existence")
    );
    assert!(
        !store
            .key_exists("missing")
            .await
            .expect("should prove exact absence")
    );
    store.delete("expired").await.expect("should delete record");
    let requests = fixture.requests().await;
    for request in &requests[1..] {
        assert!(
            request
                .target
                .starts_with("/v1/stores/identity/record?key=")
        );
    }
    assert_eq!(requests[3].method, "DELETE");
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_records_never_become_misses_or_valid_rows() {
    let bodies = [
        json!({"body":"AA==","metadata":null,"generation":0_u8}),
        json!({"body":"AA==","metadata":null,"generation":"0"}),
        json!({"body":"AA==","metadata":null,"generation":"01"}),
        json!({"body":"AA==","metadata":null,"generation":"+1"}),
        json!({"body":"AA==","metadata":null,"generation":"18446744073709551616"}),
        json!({"body":"AA==","generation":"1"}),
        json!({"body":"AA","metadata":null,"generation":"1"}),
        json!({"body":"AB==","metadata":null,"generation":"1"}),
        json!({"body":"AA==","metadata":"!","generation":"1"}),
        json!({"body":"AA==","metadata":null,"generation":"1","extra":true}),
    ];
    for body in bodies {
        let fixture = Fixture::new(vec![probe(), reply(200, &body)]).await;
        let store = prepare(&fixture).await;
        assert!(
            matches!(store.lookup("root").await, Err(PreparationError::Malformed)),
            "should reject {body}"
        );
        assert_eq!(fixture.count().await, 2);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn null_metadata_and_binary_limits_are_distinct() {
    let fixture = Fixture::new(vec![
        probe(),
        reply(
            200,
            &json!({"body":"AA==","metadata":null,"generation":"1"}),
        ),
    ])
    .await;
    let store = prepare(&fixture).await;
    assert!(
        store
            .lookup("root")
            .await
            .expect("should read row")
            .expect("should find row")
            .metadata
            .is_none()
    );
    let before = fixture.count().await;
    for metadata in [Some(vec![0; 1_025]), None] {
        let body = if metadata.is_some() {
            vec![]
        } else {
            vec![0; 0x0010_0001]
        };
        assert_eq!(
            store
                .insert(
                    "root",
                    RecordWrite {
                        body: &body,
                        metadata: metadata.as_deref(),
                        ttl: Duration::from_secs(1),
                        mode: RecordWriteMode::Add
                    }
                )
                .await,
            Err(PreparationError::SizeLimit)
        );
    }
    assert_eq!(fixture.count().await, before);
}

#[tokio::test(flavor = "current_thread")]
async fn input_failures_do_not_send_and_positive_ttl_rounds_up() {
    let fixture = Fixture::new(vec![probe(), reply(204, &Value::Null)]).await;
    let store = prepare(&fixture).await;
    for (key, ttl, mode) in [
        ("", Duration::from_secs(1), RecordWriteMode::Add),
        ("root", Duration::ZERO, RecordWriteMode::Add),
        ("root", Duration::MAX, RecordWriteMode::Add),
        (
            "root",
            Duration::from_secs(1),
            RecordWriteMode::IfGenerationMatch(0),
        ),
    ] {
        assert_eq!(
            store
                .insert(
                    key,
                    RecordWrite {
                        body: b"",
                        metadata: None,
                        ttl,
                        mode
                    }
                )
                .await,
            Err(PreparationError::InvalidResource)
        );
    }
    assert_eq!(fixture.count().await, 1);
    store
        .insert(
            "root",
            RecordWrite {
                body: b"",
                metadata: None,
                ttl: Duration::from_nanos(1_000_001),
                mode: RecordWriteMode::Add,
            },
        )
        .await
        .expect("should round TTL upward");
    let requests = fixture.requests().await;
    let body: Value = serde_json::from_slice(&requests[1].body).expect("should parse request");
    assert_eq!(body["ttl_ms"], "2");
    assert!(body["metadata"].is_null());
}

#[tokio::test(flavor = "current_thread")]
async fn one_page_listing_is_bounded_sorted_and_prefix_checked() {
    let fixture = Fixture::new(vec![
        probe(),
        reply(200, &json!({"keys":["root:a","root:b"]})),
    ])
    .await;
    let store = prepare(&fixture).await;
    assert_eq!(
        store
            .list_keys_with_prefix("root:", 2)
            .await
            .expect("should read page"),
        ["root:a", "root:b"]
    );
    assert!(
        store
            .list_keys_with_prefix("root:", 0)
            .await
            .expect("should handle zero locally")
            .is_empty()
    );
    assert_eq!(
        store.list_keys_with_prefix("", 1001).await,
        Err(PreparationError::InvalidResource)
    );
    assert_eq!(fixture.count().await, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_pages_do_not_establish_authoritative_results() {
    for body in [
        json!({"keys":["root:b","root:a"]}),
        json!({"keys":["root:a","root:a"]}),
        json!({"keys":["wrong:a"]}),
        json!({"keys":["root:\n"]}),
        json!({"keys":["root:a","root:b","root:c"]}),
        json!({"keys":[],"cursor":null}),
    ] {
        let fixture = Fixture::new(vec![probe(), reply(200, &body)]).await;
        let store = prepare(&fixture).await;
        assert_eq!(
            store.list_keys_with_prefix("root:", 2).await,
            Err(PreparationError::Malformed)
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn missing_duplicate_or_unknown_record_version_rejects_preparation() {
    for headers in [
        "x-celld-store-version: 1\r\n",
        "x-celld-store-version: 1\r\nx-celld-record-version: 1\r\nx-celld-record-version: 1\r\n",
        "x-celld-store-version: 1\r\nx-celld-record-version: 2\r\n",
    ] {
        let fixture = Fixture::new(vec![Reply::custom(
            200,
            "application/json",
            headers,
            br#"{"keys":[]}"#,
        )])
        .await;
        let mut session =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .expect("should create session");
        assert!(matches!(
            session.prepare_records(&settings(&fixture), None).await,
            Err(PreparationError::UnsupportedVersion)
        ));
        assert!(matches!(
            session.prepare_records(&settings(&fixture), None).await,
            Err(PreparationError::InvalidBinding)
        ));
        assert_eq!(fixture.count().await, 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lost_mutation_response_is_not_replayed_or_reported_written() {
    let fixture = Fixture::new(vec![probe(), Reply::Drop]).await;
    let store = prepare(&fixture).await;
    assert_eq!(
        store
            .insert(
                "root",
                RecordWrite {
                    body: b"entry",
                    metadata: None,
                    ttl: Duration::from_secs(1),
                    mode: RecordWriteMode::Overwrite
                }
            )
            .await,
        Err(PreparationError::Unavailable)
    );
    assert_eq!(fixture.count().await, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn record_mutations_reject_nonzero_204_content_length() {
    for deletion in [false, true] {
        let fixture = Fixture::new(vec![
            probe(),
            Reply::custom(
                204,
                "application/octet-stream",
                "x-celld-store-version: 1\r\nx-celld-record-version: 1\r\n",
                b"x",
            ),
        ])
        .await;
        let store = prepare(&fixture).await;
        let result = if deletion {
            store.delete("root").await
        } else {
            store
                .insert(
                    "root",
                    RecordWrite {
                        body: b"",
                        metadata: None,
                        ttl: Duration::from_secs(1),
                        mode: RecordWriteMode::Add,
                    },
                )
                .await
                .map(|_| ())
        };
        assert_eq!(result, Err(PreparationError::Malformed));
        assert_eq!(fixture.count().await, 2);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unconditional_overwrite_cannot_report_a_failed_precondition() {
    let fixture = Fixture::new(vec![
        probe(),
        reply(412, &json!({"error":"precondition_failed"})),
    ])
    .await;
    let store = prepare(&fixture).await;
    assert_eq!(
        store
            .insert(
                "root",
                RecordWrite {
                    body: b"",
                    metadata: None,
                    ttl: Duration::from_secs(1),
                    mode: RecordWriteMode::Overwrite,
                }
            )
            .await,
        Err(PreparationError::Malformed)
    );
    assert_eq!(fixture.count().await, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn only_a_validated_precondition_failure_is_normal_contention() {
    for body in [
        json!({"error":"wrong"}),
        json!({"error":"precondition_failed","detail":"remote-data"}),
    ] {
        let fixture = Fixture::new(vec![probe(), reply(412, &body)]).await;
        let store = prepare(&fixture).await;
        assert_eq!(
            store
                .insert(
                    "root",
                    RecordWrite {
                        body: b"",
                        metadata: None,
                        ttl: Duration::from_secs(1),
                        mode: RecordWriteMode::Add
                    }
                )
                .await,
            Err(PreparationError::Malformed)
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn redirects_and_wrong_grants_do_not_turn_into_readiness() {
    for (status, body, expected) in [
        (
            403,
            json!({"error":"forbidden"}),
            PreparationError::Forbidden,
        ),
        (
            503,
            json!({"error":"unavailable"}),
            PreparationError::Unavailable,
        ),
        (302, json!({}), PreparationError::Malformed),
    ] {
        let fixture = Fixture::new(vec![reply(status, &body)]).await;
        let mut session =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .expect("should create session");
        assert!(
            matches!(session.prepare_records(&settings(&fixture),None).await,Err(error) if error==expected)
        );
        assert_eq!(fixture.count().await, 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_preparation_fails_closed() {
    let fixture = Fixture::new(vec![Reply::Stall]).await;
    let limits = CelldPreparationLimits {
        operation_timeout_ms: 25,
        connect_timeout_ms: 10,
        preparation_timeout_ms: 100,
        ..CelldPreparationLimits::default()
    };
    let mut session =
        CelldPreparation::new(MonotonicClock::default(), limits).expect("should create session");
    assert!(matches!(
        session.prepare_records(&settings(&fixture), None).await,
        Err(PreparationError::Deadline)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn live_response_decoding_enforces_the_configured_value_limit() {
    let fixture = Fixture::new(vec![
        probe(),
        reply(
            200,
            &json!({"body":"AAAAAAA=","metadata":null,"generation":"1"}),
        ),
    ])
    .await;
    let limits = CelldPreparationLimits {
        max_value_bytes: 4,
        ..CelldPreparationLimits::default()
    };
    let mut session =
        CelldPreparation::new(MonotonicClock::default(), limits).expect("should create session");
    let store = session
        .prepare_records(&settings(&fixture), None)
        .await
        .expect("should prepare records");
    assert!(matches!(
        store.lookup("root").await,
        Err(PreparationError::SizeLimit)
    ));
}

//! Real SDK wire tests. The local HTTP peer models conditional row mutations;
//! `DynamoDB` Local acceptance remains a separate integration check.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "group fixture state, protocol helpers and behavior cases by responsibility"
)]

use super::*;
use crate::dynamodb_kv::sdk_tests::{active_table, loopback_client, read_request};
use crate::settings::{AwsPreparationLimits, DynamoDbKvLimits};
use edgezero_core::record_store::{RecordKeyLayout, contract};
use edgezero_core::time::MonotonicClock;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{ErrorKind, Write as _};
use std::net::{SocketAddr, TcpListener};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

#[derive(Clone, Copy)]
struct Groups;

impl RecordKeyLayout for Groups {
    fn id(&self) -> &'static str {
        "wire-groups"
    }
    fn version(&self) -> u32 {
        1
    }
    fn validate(&self) -> Result<(), KvError> {
        Ok(())
    }
    fn partition_prefix<'key>(&self, key: &'key str) -> Result<&'key str, KvError> {
        let end = key.find('/').ok_or_else(invalid_input)?;
        key.get(..=end).ok_or_else(invalid_input)
    }
    fn validate_partition(&self, prefix: &str) -> Result<(), KvError> {
        if prefix.ends_with('/') && prefix.bytes().filter(|byte| *byte == b'/').count() == 1 {
            Ok(())
        } else {
            Err(invalid_input())
        }
    }
}

#[derive(Default)]
struct State {
    rows: BTreeMap<(String, String), Value>,
    requests: Vec<(String, Value)>,
    query_pages: VecDeque<Value>,
    scan_pages: VecDeque<Value>,
    failure: Option<(String, bool, u16, Value)>,
    allocation_delay: Option<Duration>,
    describe_response: Option<Value>,
}

struct Fixture {
    address: SocketAddr,
    state: Arc<Mutex<State>>,
    stopped: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl Fixture {
    fn start() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shared_state = Arc::new(Mutex::new(State::default()));
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_state = Arc::clone(&shared_state);
        let worker_stopped = Arc::clone(&stopped);
        let worker = thread::spawn(move || {
            while !worker_stopped.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .unwrap();
                        stream
                            .set_write_timeout(Some(Duration::from_secs(3)))
                            .unwrap();
                        let Some((target, wire_body)) = read_request(&mut stream) else {
                            continue;
                        };
                        let body: Value = serde_json::from_slice(&wire_body).unwrap();
                        let operation = target.rsplit('.').next().unwrap().to_owned();
                        let mut state = worker_state.lock().unwrap();
                        state.requests.push((operation.clone(), body.clone()));
                        let failure = state
                            .failure
                            .clone()
                            .filter(|(failed_target, _, _, _)| failed_target == &operation);
                        let result = if let Some((_, commit, status, response)) = failure {
                            if commit {
                                drop(respond(&mut state, &operation, &body));
                            }
                            (status, response)
                        } else {
                            respond(&mut state, &operation, &body)
                        };
                        let delay = (operation == "UpdateItem")
                            .then_some(state.allocation_delay)
                            .flatten();
                        drop(state);
                        if let Some(wait) = delay {
                            thread::sleep(wait);
                        }
                        let encoded = serde_json::to_vec(&result.1).unwrap();
                        let header = format!(
                            "HTTP/1.1 {} Response\r\ncontent-type: application/x-amz-json-1.0\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            result.0,
                            encoded.len()
                        );
                        stream
                            .write_all(header.as_bytes())
                            .and_then(|()| stream.write_all(&encoded))
                            .unwrap_or_default();
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("loopback accept: {error}"),
                }
            }
        });
        Self {
            address,
            state: shared_state,
            stopped,
            worker: Some(worker),
        }
    }

    fn requests(&self) -> Vec<(String, Value)> {
        self.state.lock().unwrap().requests.clone()
    }

    fn fail(&self, operation: &str, commit: bool, status: u16, code: &str) {
        self.state.lock().unwrap().failure = Some((
            operation.to_owned(),
            commit,
            status,
            json!({"__type": code, "message": "PRIVATE_RECORD_SENTINEL"}),
        ));
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.worker.take().unwrap().join().unwrap();
    }
}

fn wire_key(item: &Value) -> (String, String) {
    (
        item["pk"]["S"].as_str().unwrap().to_owned(),
        item["sk"]["S"].as_str().unwrap().to_owned(),
    )
}

fn conditional_failure() -> (u16, Value) {
    (400, json!({"__type": "ConditionalCheckFailedException"}))
}

fn respond(state: &mut State, operation: &str, body: &Value) -> (u16, Value) {
    match operation {
        "DescribeTable" => (
            200,
            state.describe_response.clone().unwrap_or_else(record_table),
        ),
        "UpdateItem" => {
            let key = wire_key(&body["Key"]);
            let current = state
                .rows
                .get(&key)
                .and_then(|item| item["generation"]["N"].as_str())
                .map_or(0, |number| number.parse::<u64>().unwrap());
            let Some(next) = current.checked_add(1) else {
                return conditional_failure();
            };
            let mut item = body["Key"].clone();
            item["generation"] = json!({"N": next.to_string()});
            state.rows.insert(key, item);
            (
                200,
                json!({"Attributes": {"generation": {"N": next.to_string()}}}),
            )
        }
        "PutItem" => {
            let item = &body["Item"];
            let key = wire_key(item);
            let existing = state.rows.get(&key);
            if body["ConditionExpression"] == "attribute_not_exists(#pk)" && existing.is_some() {
                return conditional_failure();
            }
            if body["ConditionExpression"] == "#generation = :expected"
                && existing.map(|stored| &stored["generation"])
                    != Some(&body["ExpressionAttributeValues"][":expected"])
            {
                return conditional_failure();
            }
            state.rows.insert(key, item.clone());
            (200, json!({}))
        }
        "GetItem" => (
            200,
            state
                .rows
                .get(&wire_key(&body["Key"]))
                .map_or_else(|| json!({}), |item| json!({"Item": item})),
        ),
        "DeleteItem" => {
            state.rows.remove(&wire_key(&body["Key"]));
            (200, json!({}))
        }
        "Query" => (
            200,
            state
                .query_pages
                .pop_front()
                .unwrap_or_else(|| list_response(state, body, true)),
        ),
        "Scan" => (
            200,
            state
                .scan_pages
                .pop_front()
                .unwrap_or_else(|| list_response(state, body, false)),
        ),
        _ => panic!("unexpected operation {operation}"),
    }
}

fn record_table() -> Value {
    let mut table = active_table();
    table["Table"]["TableArn"] = json!("arn:aws:dynamodb:us-east-1:111111111111:table/fixture-kv");
    table["Table"]["TableId"] = json!("fixture-table-id");
    table
}

fn list_response(state: &State, body: &Value, query: bool) -> Value {
    let after = body.get("ExclusiveStartKey").map(wire_key);
    let values = &body["ExpressionAttributeValues"];
    let evaluated: Vec<_> = state
        .rows
        .iter()
        .filter(|(key, _)| {
            after.as_ref().is_none_or(|previous| *key > previous)
                && (!query || key.0 == values[":pk"]["S"].as_str().unwrap())
        })
        .take(usize::try_from(body["Limit"].as_u64().unwrap()).unwrap())
        .collect();
    let items: Vec<_> = evaluated
        .iter()
        .filter(|(key, item)| {
            item["format"]["S"] == FORMAT
                && item["kind"]["S"] == "r"
                && (query
                    || (key.0.starts_with(values[":binding"]["S"].as_str().unwrap())
                        && key.1.starts_with(values[":prefix"]["S"].as_str().unwrap())))
        })
        .map(|(_, item)| (*item).clone())
        .collect();
    let mut response = json!({"Items": items});
    if evaluated.len() == usize::try_from(body["Limit"].as_u64().unwrap()).unwrap()
        && let Some((key, _)) = evaluated.last()
    {
        response["LastEvaluatedKey"] = json!({"pk": {"S": key.0}, "sk": {"S": key.1}});
    }
    response
}

fn settings() -> DynamoDbKvSettings {
    DynamoDbKvSettings {
        region: "us-east-1".to_owned(),
        table: "fixture-kv".to_owned(),
        namespace: "records-wire".to_owned(),
        limits: DynamoDbKvLimits::default(),
    }
}

fn runtime() -> Runtime {
    RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn store(
    runtime: &Runtime,
    fixture: &Fixture,
    settings: &DynamoDbKvSettings,
) -> DynamoDbRecordStore {
    let client = loopback_client(
        runtime,
        &format!("http://{}", fixture.address),
        settings.limits,
    );
    let layout = RecordLayout::new(Groups).unwrap();
    let base = record_partition_base(settings, &layout).unwrap();
    let inner = runtime.block_on(async {
        let mut preparation =
            AwsPreparation::new(MonotonicClock::default(), AwsPreparationLimits::default())
                .unwrap();
        preparation.validate_dynamodb_mapping(settings).unwrap();
        preparation
            .finish_dynamodb_preparation(settings, client)
            .await
            .unwrap()
    });
    let store = DynamoDbRecordStore::new(inner, layout, base).unwrap();
    fixture.state.lock().unwrap().requests.clear();
    store
}

fn write(body: &[u8], mode: RecordWriteMode) -> RecordWrite<'_> {
    RecordWrite {
        body,
        metadata: Some("m\u{e9}tadata"),
        ttl: None,
        mode,
    }
}

#[test]
fn resolved_endpoint_and_table_account_bind_native_cursors() {
    let runtime = runtime();
    let first_fixture = Fixture::start();
    let second_fixture = Fixture::start();
    let first = store(&runtime, &first_fixture, &settings());
    let second = store(&runtime, &second_fixture, &settings());
    assert_ne!(first.binding.scope, second.binding.scope);
    let cursor = first
        .next_cursor(
            "scan",
            "",
            None,
            Some(physical_key("foreign".to_owned(), "internal".to_owned())),
            None,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        runtime.block_on(second.page("", None, Some(&cursor), 1)),
        Err(KvError::Validation(_))
    ));
    assert!(second_fixture.requests().is_empty());
    let mut account_description = record_table();
    account_description["Table"]["TableArn"] =
        json!("arn:aws:dynamodb:us-east-1:222222222222:table/fixture-kv");
    first_fixture.state.lock().unwrap().describe_response = Some(account_description);
    let other_account = store(&runtime, &first_fixture, &settings());
    assert_ne!(first.binding.scope, other_account.binding.scope);
    assert!(matches!(
        runtime.block_on(other_account.page("", None, Some(&cursor), 1)),
        Err(KvError::Validation(_))
    ));
    assert!(first_fixture.requests().is_empty());
}

#[test]
fn endpoint_scope_excludes_userinfo_query_and_fragment() {
    assert_eq!(
        endpoint_identity(
            "http://user:PRIVATE_PASSWORD@127.0.0.1:8000/base?token=PRIVATE_TOKEN#PRIVATE_FRAGMENT"
        )
        .unwrap(),
        "http://127.0.0.1:8000/base"
    );
}

#[test]
fn missing_table_identity_is_rejected_only_by_record_construction() {
    let runtime = runtime();
    let fixture = Fixture::start();
    fixture.state.lock().unwrap().describe_response = Some(active_table());
    let settings = settings();
    let client = loopback_client(
        &runtime,
        &format!("http://{}", fixture.address),
        settings.limits,
    );
    let inner = runtime.block_on(async {
        let mut preparation =
            AwsPreparation::new(MonotonicClock::default(), AwsPreparationLimits::default())
                .unwrap();
        preparation.validate_dynamodb_mapping(&settings).unwrap();
        preparation
            .finish_dynamodb_preparation(&settings, client)
            .await
            .unwrap()
    });
    let layout = RecordLayout::new(Groups).unwrap();
    assert!(matches!(
        DynamoDbRecordStore::new(
            inner,
            layout.clone(),
            record_partition_base(&settings, &layout).unwrap()
        ),
        Err(PreparationError::Malformed)
    ));
    assert_eq!(fixture.requests().len(), 1);
    assert_eq!(fixture.requests()[0].0, "DescribeTable");
}

#[test]
fn shared_public_record_contract_uses_real_sdk_wire_requests() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let kv = KvHandle::new(Arc::new(store(&runtime, &fixture, &settings())));
    runtime.block_on(contract::assert_record_contract(&kv, "group/"));
}

#[test]
fn record_preparation_shares_table_preflight_without_mutation_probe() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let settings = settings();
    let client = loopback_client(
        &runtime,
        &format!("http://{}", fixture.address),
        settings.limits,
    );
    runtime.block_on(async {
        let mut preparation =
            AwsPreparation::new(MonotonicClock::default(), AwsPreparationLimits::default())
                .unwrap();
        preparation.validate_dynamodb_mapping(&settings).unwrap();
        let inner = preparation
            .finish_dynamodb_preparation(&settings, client)
            .await
            .unwrap();
        let layout = RecordLayout::new(Groups).unwrap();
        let kv = KvHandle::new(Arc::new(
            DynamoDbRecordStore::new(
                inner,
                layout.clone(),
                record_partition_base(&settings, &layout).unwrap(),
            )
            .unwrap(),
        ));
        assert_eq!(
            kv.require_records().unwrap().binding().max_metadata_bytes,
            settings.limits.max_value_bytes
        );
    });
    assert_eq!(
        fixture
            .requests()
            .iter()
            .map(|(target, _)| target.as_str())
            .collect::<Vec<_>>(),
        ["DescribeTable"]
    );
}

#[test]
fn atomic_allocation_and_conditions_have_distinct_partition_and_row_keys() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let provider = store(&runtime, &fixture, &settings());
    runtime.block_on(async {
        for key in ["a/full-key", "b/full-key"] {
            let partition = provider.binding.layout.resolve(key).unwrap();
            assert_eq!(
                provider
                    .insert(key, &partition, write(&[0, 255], RecordWriteMode::Add))
                    .await
                    .unwrap(),
                RecordWriteOutcome::Written
            );
        }
    });
    let requests = fixture.requests();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[0].0, "UpdateItem");
    assert_eq!(requests[0].1["Key"]["sk"]["S"], "g#a/full-key");
    assert_eq!(requests[1].1["Item"]["sk"]["S"], "r#a/full-key");
    assert_ne!(requests[0].1["Key"]["pk"], requests[2].1["Key"]["pk"]);
    assert_eq!(
        requests[0].1["ConditionExpression"],
        "attribute_not_exists(#generation) OR #generation < :ceiling"
    );
    assert_eq!(
        requests[0].1["ExpressionAttributeValues"][":ceiling"]["N"],
        u64::MAX.to_string()
    );
    assert_eq!(requests[0].1["ReturnValues"], "UPDATED_NEW");
    assert!(requests[0].1.get("ttl_epoch_s").is_none());
    assert_eq!(
        requests[1].1["ConditionExpression"],
        "attribute_not_exists(#pk)"
    );
    assert_eq!(requests[1].1["Item"]["v"]["B"], "AP8=");
}

#[test]
fn same_row_replacement_clears_metadata_ttl_and_invalidates_old_generations() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let kv = KvHandle::new(Arc::new(store(&runtime, &fixture, &settings())));
    runtime.block_on(async {
        let records = kv.require_records().unwrap();
        let key = "a/coherent";
        records
            .insert(
                key,
                RecordWrite {
                    ttl: Some(Duration::from_secs(60)),
                    ..write(&[0, 255], RecordWriteMode::Add)
                },
            )
            .await
            .unwrap();
        let old = records.lookup(key).await.unwrap().unwrap();
        assert_eq!(old.body.as_ref(), &[0, 255]);
        assert_eq!(old.metadata.as_deref(), Some("m\u{e9}tadata"));
        kv.put_bytes(key, Bytes::from_static(b"ordinary"))
            .await
            .unwrap();
        let fresh = records.lookup(key).await.unwrap().unwrap();
        assert_eq!(fresh.body.as_ref(), b"ordinary");
        assert_eq!(fresh.metadata, None);
        assert_ne!(fresh.generation, old.generation);
        assert_eq!(
            records
                .insert(
                    key,
                    write(b"stale", RecordWriteMode::IfGenerationMatch(old.generation))
                )
                .await
                .unwrap(),
            RecordWriteOutcome::PreconditionFailed
        );
        records.delete(key).await.unwrap();
        assert!(!records.exists_strong(key).await.unwrap());
        records
            .insert(key, write(b"recreated", RecordWriteMode::Add))
            .await
            .unwrap();
        assert_eq!(
            records
                .insert(
                    key,
                    write(
                        b"stale",
                        RecordWriteMode::IfGenerationMatch(fresh.generation)
                    )
                )
                .await
                .unwrap(),
            RecordWriteOutcome::PreconditionFailed
        );
    });
    let requests = fixture.requests();
    let puts: Vec<_> = requests
        .iter()
        .filter(|(target, _)| target == "PutItem")
        .collect();
    assert!(puts[0].1["Item"].get("ttl_epoch_s").is_some());
    assert!(puts[0].1["Item"].get("expires_at_ms").is_none());
    assert!(puts[1].1["Item"].get("ttl_epoch_s").is_none());
    assert!(puts[1].1["Item"].get("metadata").is_none());
    assert!(
        requests
            .iter()
            .filter(|(target, _)| target == "GetItem")
            .all(|(_, body)| body["ConsistentRead"] == true)
    );
    assert!(
        requests
            .iter()
            .filter(|(target, _)| target == "DeleteItem")
            .all(|(_, body)| body["Key"]["sk"]["S"].as_str().unwrap().starts_with("r#"))
    );
}

#[test]
fn native_expired_but_retained_values_remain_visible_and_block_add() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let provider = store(&runtime, &fixture, &settings());
    runtime.block_on(async {
        let partition = provider.binding.layout.resolve("a/ttl").unwrap();
        provider
            .insert("a/ttl", &partition, write(b"body", RecordWriteMode::Add))
            .await
            .unwrap();
        let key = (
            provider.partition_key(&partition).unwrap(),
            "r#a/ttl".to_owned(),
        );
        fixture.state.lock().unwrap().rows.get_mut(&key).unwrap()["ttl_epoch_s"] =
            json!({"N": "1"});
        assert!(
            provider
                .lookup("a/ttl", &partition)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            provider
                .insert("a/ttl", &partition, write(b"other", RecordWriteMode::Add))
                .await
                .unwrap(),
            RecordWriteOutcome::PreconditionFailed
        );
    });
}

#[test]
fn independent_add_and_cas_writers_have_one_acknowledged_winner() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let first = store(&runtime, &fixture, &settings());
    let second = store(&runtime, &fixture, &settings());
    runtime.block_on(async {
        let partition = first.binding.layout.resolve("a/race").unwrap();
        let (add_first, add_second) = futures::join!(
            first.insert("a/race", &partition, write(b"first", RecordWriteMode::Add)),
            second.insert("a/race", &partition, write(b"second", RecordWriteMode::Add))
        );
        assert_eq!(
            [add_first.unwrap(), add_second.unwrap()]
                .iter()
                .filter(|outcome| **outcome == RecordWriteOutcome::Written)
                .count(),
            1
        );
        let generation = first
            .lookup("a/race", &partition)
            .await
            .unwrap()
            .unwrap()
            .generation;
        let (cas_first, cas_second) = futures::join!(
            first.insert(
                "a/race",
                &partition,
                write(b"first", RecordWriteMode::IfGenerationMatch(generation))
            ),
            second.insert(
                "a/race",
                &partition,
                write(b"second", RecordWriteMode::IfGenerationMatch(generation))
            )
        );
        assert_eq!(
            [cas_first.unwrap(), cas_second.unwrap()]
                .iter()
                .filter(|outcome| **outcome == RecordWriteOutcome::Written)
                .count(),
            1
        );
    });
}

#[test]
fn lost_value_acknowledgement_is_uncertain_and_never_replayed() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let provider = store(&runtime, &fixture, &settings());
    fixture.fail("PutItem", true, 500, "InternalServerError");
    runtime.block_on(async {
        let partition = provider.binding.layout.resolve("a/uncertain").unwrap();
        let error = provider
            .insert(
                "a/uncertain",
                &partition,
                write(b"PRIVATE_BODY", RecordWriteMode::Add),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, KvError::MutationUncertain));
        assert!(!format!("{error:?} {error}").contains("PRIVATE"));
        assert!(
            provider
                .lookup("a/uncertain", &partition)
                .await
                .unwrap()
                .is_some()
        );
    });
    assert_eq!(
        fixture
            .requests()
            .iter()
            .filter(|(target, _)| target == "PutItem")
            .count(),
        1
    );
}

#[test]
fn lost_delete_acknowledgement_is_uncertain_without_deleting_allocator() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let provider = store(&runtime, &fixture, &settings());
    runtime.block_on(async {
        let partition = provider.binding.layout.resolve("a/delete").unwrap();
        provider
            .insert("a/delete", &partition, write(b"body", RecordWriteMode::Add))
            .await
            .unwrap();
        fixture.fail("DeleteItem", true, 500, "InternalServerError");
        assert!(matches!(
            provider
                .delete_value("a/delete", &partition)
                .await
                .unwrap_err(),
            KvError::MutationUncertain
        ));
        assert!(
            provider
                .lookup("a/delete", &partition)
                .await
                .unwrap()
                .is_none()
        );
    });
    assert_eq!(
        fixture
            .requests()
            .iter()
            .filter(|(target, _)| target == "DeleteItem")
            .count(),
        1
    );
    assert_eq!(fixture.state.lock().unwrap().rows.len(), 1);
}

#[test]
fn allocator_failure_or_exhaustion_never_dispatches_a_value_mutation() {
    for exhausted in [false, true] {
        let runtime = runtime();
        let fixture = Fixture::start();
        let provider = store(&runtime, &fixture, &settings());
        let partition = provider.binding.layout.resolve("a/exhausted").unwrap();
        if exhausted {
            let key = provider.key("a/exhausted", &partition, "g").unwrap();
            fixture.state.lock().unwrap().rows.insert((provider.partition_key(&partition).unwrap(), "g#a/exhausted".to_owned()), json!({"pk": {"S": key["pk"].as_s().unwrap()}, "sk": {"S": "g#a/exhausted"}, "generation": {"N": u64::MAX.to_string()}}));
        } else {
            fixture.fail("UpdateItem", true, 500, "InternalServerError");
        }
        assert!(matches!(
            runtime
                .block_on(provider.insert(
                    "a/exhausted",
                    &partition,
                    write(b"body", RecordWriteMode::Add)
                ))
                .unwrap_err(),
            KvError::Unavailable
        ));
        assert_eq!(fixture.requests().len(), 1);
        assert_eq!(fixture.requests()[0].0, "UpdateItem");
    }
}

#[test]
fn one_deadline_bounds_allocation_before_value_dispatch() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let mut settings = settings();
    settings.limits.operation_timeout_ms = 40;
    settings.limits.attempt_timeout_ms = 40;
    settings.limits.connect_timeout_ms = 40;
    let provider = store(&runtime, &fixture, &settings);
    fixture.state.lock().unwrap().allocation_delay = Some(Duration::from_millis(100));
    let partition = provider.binding.layout.resolve("a/budget").unwrap();
    assert!(matches!(
        runtime
            .block_on(provider.insert("a/budget", &partition, write(b"body", RecordWriteMode::Add)))
            .unwrap_err(),
        KvError::Unavailable
    ));
    assert_eq!(fixture.requests().len(), 1);
}

#[test]
fn query_and_scan_keep_empty_filtered_pages_and_native_foreign_positions() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let provider = store(&runtime, &fixture, &settings());
    let partition = provider.binding.layout.partition("a/").unwrap();
    let pk = provider.partition_key(&partition).unwrap();
    fixture.state.lock().unwrap().query_pages.push_back(
        json!({"Items": [], "LastEvaluatedKey": {"pk": {"S": pk}, "sk": {"S": "g#a/internal"}}}),
    );
    fixture.state.lock().unwrap().scan_pages.push_back(json!({"Items": [], "LastEvaluatedKey": {"pk": {"S": "foreign-namespace"}, "sk": {"S": "foreign-key"}}}));
    runtime.block_on(async {
        let query = provider
            .record_list_partition(&partition, None, 1)
            .await
            .unwrap();
        assert!(query.keys.is_empty());
        let query_cursor = query.cursor.unwrap();
        provider
            .page("a/", None, Some(&query_cursor), 1)
            .await
            .unwrap_err();
        provider
            .record_list_partition(&partition, Some(&query_cursor), 1)
            .await
            .unwrap();
        let scan = provider.page("", None, None, 1).await.unwrap();
        assert!(scan.keys.is_empty());
        let scan_cursor = scan.cursor.unwrap();
        provider
            .record_list_partition(&partition, Some(&scan_cursor), 1)
            .await
            .unwrap_err();
        provider
            .page("", None, Some(&scan_cursor), 1)
            .await
            .unwrap();
    });
    let requests = fixture.requests();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[0].1["ConsistentRead"], true);
    assert_eq!(requests[0].1["ExpressionAttributeValues"][":pk"]["S"], pk);
    assert_eq!(
        requests[1].1["ExclusiveStartKey"]["sk"]["S"],
        "g#a/internal"
    );
    assert_eq!(
        requests[3].1["ExclusiveStartKey"]["pk"]["S"],
        "foreign-namespace"
    );
    assert_eq!(
        requests[2].1["FilterExpression"],
        "begins_with(#pk, :binding) AND #format = :format AND #kind = :kind AND begins_with(#sk, :prefix)"
    );
    assert_eq!(requests[2].1["Limit"], 1_i32);
    assert!(requests[2].1.get("IndexName").is_none());
}

#[test]
fn cursors_bind_scope_layout_operation_prefix_and_reject_nonadvancement() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let provider = store(&runtime, &fixture, &settings());
    let position = physical_key("foreign".to_owned(), "internal".to_owned());
    let cursor = provider
        .next_cursor("scan", "a", None, Some(position.clone()), None)
        .unwrap()
        .unwrap();
    provider
        .decode_cursor("scan", "b", &cursor, None)
        .unwrap_err();
    provider
        .decode_cursor("query", "a", &cursor, None)
        .unwrap_err();
    provider
        .next_cursor("scan", "a", Some(&position), Some(position.clone()), None)
        .unwrap_err();
    for field in ["format", "scope", "layout_id", "mode", "prefix"] {
        let mut altered: Value = serde_json::from_str(&cursor).unwrap();
        altered[field] = json!("other");
        provider
            .decode_cursor("scan", "a", &altered.to_string(), None)
            .unwrap_err();
    }
    let mut altered: Value = serde_json::from_str(&cursor).unwrap();
    altered["layout_version"] = json!(2_u32);
    provider
        .decode_cursor("scan", "a", &altered.to_string(), None)
        .unwrap_err();
    assert!(fixture.requests().is_empty());
}

#[test]
fn validation_precedes_allocation_and_metadata_counts_utf8_bytes() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let provider = store(&runtime, &fixture, &settings());
    runtime.block_on(async {
        let partition = provider.binding.layout.resolve("a/limits").unwrap();
        provider
            .insert(
                "a/limits",
                &partition,
                RecordWrite {
                    metadata: Some(
                        &"\u{e9}".repeat(
                            settings()
                                .limits
                                .max_value_bytes
                                .div_ceil(2)
                                .checked_add(1)
                                .unwrap(),
                        ),
                    ),
                    ..write(b"body", RecordWriteMode::Add)
                },
            )
            .await
            .unwrap_err();
        provider
            .insert(
                "a/limits",
                &partition,
                write(
                    &vec![0; settings().limits.max_value_bytes + 1],
                    RecordWriteMode::Add,
                ),
            )
            .await
            .unwrap_err();
        provider
            .insert(
                "a/limits",
                &partition,
                RecordWrite {
                    ttl: Some(Duration::ZERO),
                    ..write(b"body", RecordWriteMode::Add)
                },
            )
            .await
            .unwrap_err();
        provider.page("", None, None, 0).await.unwrap_err();
    });
    assert!(fixture.requests().is_empty());
}

#[test]
fn metadata_and_body_share_the_configured_native_payload_budget() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let mut settings = settings();
    settings.limits.max_value_bytes = 8;
    let provider = store(&runtime, &fixture, &settings);
    assert_eq!(provider.binding.max_metadata_bytes, 8);
    let partition = provider.binding.layout.resolve("a/budget").unwrap();
    runtime.block_on(async {
        assert_eq!(
            provider
                .insert(
                    "a/budget",
                    &partition,
                    RecordWrite {
                        body: b"1234",
                        metadata: Some("\u{e9}\u{e9}"),
                        ttl: None,
                        mode: RecordWriteMode::Add,
                    }
                )
                .await
                .unwrap(),
            RecordWriteOutcome::Written
        );
        let before = fixture.requests().len();
        provider
            .insert(
                "a/budget",
                &partition,
                RecordWrite {
                    body: b"12345",
                    metadata: Some("\u{e9}\u{e9}"),
                    ttl: None,
                    mode: RecordWriteMode::Overwrite,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(fixture.requests().len(), before);
        assert_eq!(
            provider
                .insert(
                    "a/budget",
                    &partition,
                    RecordWrite {
                        body: b"",
                        metadata: Some("\u{e9}\u{e9}\u{e9}\u{e9}"),
                        ttl: None,
                        mode: RecordWriteMode::Overwrite,
                    }
                )
                .await
                .unwrap(),
            RecordWriteOutcome::Written
        );
        assert_eq!(
            provider
                .lookup("a/budget", &partition)
                .await
                .unwrap()
                .unwrap()
                .metadata
                .as_deref(),
            Some("\u{e9}\u{e9}\u{e9}\u{e9}")
        );
        let key = (
            provider.partition_key(&partition).unwrap(),
            "r#a/budget".to_owned(),
        );
        fixture.state.lock().unwrap().rows.get_mut(&key).unwrap()["v"] = json!({"B": "eA=="});
        assert!(matches!(
            provider.lookup("a/budget", &partition).await.unwrap_err(),
            KvError::Unavailable
        ));
    });
}

#[test]
fn multi_partition_scan_and_query_traverse_values_once_through_allocator_pages() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let provider = store(&runtime, &fixture, &settings());
    let mut foreign_settings = settings();
    foreign_settings.namespace = "foreign".to_owned();
    let foreign = store(&runtime, &fixture, &foreign_settings);
    runtime.block_on(async {
        for key in ["a/one", "a/two", "b/one", "b/two"] {
            let partition = provider.binding.layout.resolve(key).unwrap();
            provider
                .insert(key, &partition, write(b"body", RecordWriteMode::Add))
                .await
                .unwrap();
        }
        foreign
            .insert(
                "a/foreign",
                &foreign.binding.layout.resolve("a/foreign").unwrap(),
                write(b"body", RecordWriteMode::Add),
            )
            .await
            .unwrap();
        let partition = provider.binding.layout.partition("a/").unwrap();
        let mut cursor = None;
        let mut keys = BTreeSet::new();
        let mut empty_pages = 0_usize;
        loop {
            let page = provider
                .record_list_partition(&partition, cursor.as_deref(), 1)
                .await
                .unwrap();
            if page.keys.is_empty() && page.cursor.is_some() {
                empty_pages += 1;
            }
            for key in page.keys {
                assert!(keys.insert(key));
            }
            cursor = page.cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(keys.into_iter().collect::<Vec<_>>(), ["a/one", "a/two"]);
        assert_eq!(empty_pages, 2);
        for prefix in ["", "a", "a/o", "missing"] {
            let mut scan_cursor = None;
            let mut scan_keys = BTreeSet::new();
            loop {
                let page = provider
                    .page(prefix, None, scan_cursor.as_deref(), 1)
                    .await
                    .unwrap();
                for key in page.keys {
                    assert!(scan_keys.insert(key));
                }
                scan_cursor = page.cursor;
                if scan_cursor.is_none() {
                    break;
                }
            }
            let expected: BTreeSet<_> = ["a/one", "a/two", "b/one", "b/two"]
                .into_iter()
                .filter(|key| key.starts_with(prefix))
                .map(str::to_owned)
                .collect();
            assert_eq!(scan_keys, expected);
        }
    });
}

#[test]
fn recreated_provider_and_expired_row_removal_do_not_reset_allocator() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let provider = store(&runtime, &fixture, &settings());
    let partition = provider.binding.layout.resolve("a/reopen").unwrap();
    let old = runtime.block_on(async {
        provider
            .insert("a/reopen", &partition, write(b"old", RecordWriteMode::Add))
            .await
            .unwrap();
        provider
            .lookup("a/reopen", &partition)
            .await
            .unwrap()
            .unwrap()
            .generation
    });
    fixture.state.lock().unwrap().rows.remove(&(
        provider.partition_key(&partition).unwrap(),
        "r#a/reopen".to_owned(),
    ));
    drop(provider);
    let reopened = store(&runtime, &fixture, &settings());
    runtime.block_on(async {
        reopened
            .insert("a/reopen", &partition, write(b"new", RecordWriteMode::Add))
            .await
            .unwrap();
        assert_ne!(
            reopened
                .lookup("a/reopen", &partition)
                .await
                .unwrap()
                .unwrap()
                .generation,
            old
        );
        assert_eq!(
            reopened
                .insert(
                    "a/reopen",
                    &partition,
                    write(b"stale", RecordWriteMode::IfGenerationMatch(old))
                )
                .await
                .unwrap(),
            RecordWriteOutcome::PreconditionFailed
        );
    });
}

#[test]
fn partition_encoding_is_collision_free_and_preserves_full_unicode_keys() {
    let runtime = runtime();
    let fixture = Fixture::start();
    let mut namespace_a = settings();
    namespace_a.namespace = "a:1:b".to_owned();
    let mut namespace_b = settings();
    namespace_b.namespace = "a".to_owned();
    let provider_a = store(&runtime, &fixture, &namespace_a);
    let provider_b = store(&runtime, &fixture, &namespace_b);
    let partition = provider_a.binding.layout.partition("b/").unwrap();
    assert_ne!(
        provider_a.partition_key(&partition).unwrap(),
        provider_b.partition_key(&partition).unwrap()
    );
    let key = "\u{e9}/e\u{301}";
    let key_partition = provider_a.binding.layout.resolve(key).unwrap();
    assert_eq!(
        provider_a.key(key, &key_partition, "r").unwrap()["sk"],
        AttributeValue::S(format!("r#{key}"))
    );
}

//! Loopback SDK and portable-contract tests for the real `DynamoDbStore`.
//!
//! These fixtures exercise the actual AWS SDK serializer/deserializer and the
//! actual `DynamoDbStore`; the in-process service is only the HTTP peer. No
//! ambient AWS config is consulted by `loopback_client`.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "fixture, protocol, runtime wrapper and contract cases are grouped"
)]

use super::*;
use crate::preparation::AwsPreparation;
use crate::settings::{AwsPreparationLimits, DynamoDbKvLimits, DynamoDbKvSettings};
use edgezero_core::time::MonotonicClock;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{ErrorKind, Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::str;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::Credentials;
use aws_sdk_dynamodb::config::timeout::TimeoutConfig;
use aws_sdk_dynamodb::config::{Region, retry::RetryConfig};
use aws_sdk_dynamodb::operation::get_item::GetItemOutput;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvHandle, KvPage, KvStore};
use serde_json::{Value, json};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

const TABLE: &str = "fixture-kv";
const NAMESPACE: &str = "sdk-tests";
const REGION: &str = "us-east-1";
const MAX_HEADER_BYTES: usize = 0x1_0000;
const MAX_REQUEST_BODY_BYTES: usize = 0x20_0000;
const TEST_KEY: &str = "nfc/\u{e9}";
const TEST_VALUE: &[u8] = &[0, 0xff, b'\n', b'\''];

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RowKey {
    pk: String,
    sk: String,
}

#[derive(Clone, Debug)]
struct Row {
    expires_at_ms: Option<i64>,
    ttl_epoch_s: Option<i64>,
    value: Vec<u8>,
}

#[derive(Default)]
struct FixtureState {
    commit_before_error: bool,
    describe_response: Option<Value>,
    fail_target: Option<(String, u16, Value)>,
    query_pages: VecDeque<Value>,
    requests: Vec<(String, Value)>,
    rows: BTreeMap<RowKey, Row>,
    stall_target: Option<String>,
}

/// Minimal `DynamoDB` JSON 1.0 endpoint. `Drop` stops accept and joins the thread;
/// every socket read has a deadline, and each response closes the connection.
struct DynamoFixture {
    accepted: Arc<AtomicUsize>,
    addr: SocketAddr,
    state: Arc<Mutex<FixtureState>>,
    stopped: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl DynamoFixture {
    fn commit_before_error(&self) {
        self.state.lock().unwrap().commit_before_error = true;
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn fail_target(&self, target: &str, status: u16, response: Value) {
        self.state.lock().unwrap().fail_target = Some((target.to_owned(), status, response));
    }

    fn has_row(&self, key: &str) -> bool {
        self.state.lock().unwrap().rows.contains_key(&RowKey {
            pk: NAMESPACE.into(),
            sk: key.into(),
        })
    }

    fn push_query_page(&self, response: Value) {
        self.state.lock().unwrap().query_pages.push_back(response);
    }

    fn request_count(&self) -> usize {
        self.state.lock().unwrap().requests.len()
    }

    fn requests(&self) -> Vec<(String, Value)> {
        self.state.lock().unwrap().requests.clone()
    }

    fn seed(&self, key: &str, row: Row) {
        self.state.lock().unwrap().rows.insert(
            RowKey {
                pk: NAMESPACE.into(),
                sk: key.into(),
            },
            row,
        );
    }

    fn set_describe_response(&self, response: Value) {
        self.state.lock().unwrap().describe_response = Some(response);
    }

    fn stall_target(&self, target: &str) {
        self.state.lock().unwrap().stall_target = Some(target.to_owned());
    }

    fn start() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind literal loopback");
        listener.set_nonblocking(true).expect("nonblocking accept");
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(FixtureState::default()));
        let stopped = Arc::new(AtomicBool::new(false));
        let accepted = Arc::new(AtomicUsize::new(0));
        let worker_state = Arc::clone(&state);
        let worker_stopped = Arc::clone(&stopped);
        let worker_accepted = Arc::clone(&accepted);
        let worker = thread::spawn(move || {
            while !worker_stopped.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        worker_accepted.fetch_add(1, Ordering::Relaxed);
                        stream
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .expect("set read timeout");
                        stream
                            .set_write_timeout(Some(Duration::from_secs(3)))
                            .expect("set write timeout");
                        serve_one(&mut stream, &worker_state);
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) if worker_stopped.load(Ordering::Acquire) => break,
                    Err(error) => panic!("fixture accept failed: {error}"),
                }
            }
        });
        Self {
            accepted,
            addr,
            state,
            stopped,
            worker: Some(worker),
        }
    }
}

impl Drop for DynamoFixture {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().expect("fixture thread joined");
        }
        debug_assert!(
            self.request_count()
                .checked_add(1)
                .is_some_and(|requests| self.accepted.load(Ordering::Relaxed) <= requests)
        );
    }
}

fn serve_one(stream: &mut TcpStream, state: &Arc<Mutex<FixtureState>>) {
    let Some((target, body)) = read_request(stream) else {
        return;
    };
    let parsed: Value = serde_json::from_slice(&body).expect("SDK emits valid JSON");
    let mut fixture = state.lock().unwrap();
    fixture.requests.push((target.clone(), parsed.clone()));
    if fixture.stall_target.as_deref() == Some(&target) {
        // Client operation timeout must bound the wait; fixture's finite upper
        // bound also prevents a broken client from hanging test teardown.
        drop(fixture);
        thread::sleep(Duration::from_secs(3));
        return;
    }
    let (status, response) = if let Some((failed, status, response)) = fixture.fail_target.clone() {
        if failed == target {
            if fixture.commit_before_error
                && matches!(target.rsplit('.').next(), Some("PutItem" | "DeleteItem"))
            {
                drop(respond(&mut fixture, &target, &parsed));
            }
            (status, response)
        } else {
            respond(&mut fixture, &target, &parsed)
        }
    } else {
        respond(&mut fixture, &target, &parsed)
    };
    drop(fixture);
    let encoded = serde_json::to_vec(&response).unwrap();
    let status_text = if status == 200 { "OK" } else { "Bad Request" };
    let head = format!(
        "HTTP/1.1 {status} {status_text}\r\ncontent-type: application/x-amz-json-1.0\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        encoded.len()
    );
    // Timed-out clients may close the socket before the finite fixture response.
    stream
        .write_all(head.as_bytes())
        .and_then(|()| stream.write_all(&encoded))
        .unwrap_or_default();
}

fn read_request(stream: &mut TcpStream) -> Option<(String, Vec<u8>)> {
    let mut data = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 2048];
    let header_end = loop {
        let count = stream.read(&mut chunk).ok()?;
        if count == 0 {
            return None;
        }
        data.extend_from_slice(&chunk[..count]);
        if let Some(end) = data.windows(4).position(|window| window == b"\r\n\r\n") {
            break end.checked_add(4)?;
        }
        assert!(data.len() < MAX_HEADER_BYTES, "oversized request headers");
    };
    let headers = str::from_utf8(data.get(..header_end)?).ok()?;
    let target = headers.lines().find_map(|line| {
        line.split_once(':')
            .filter(|(name, _)| name.eq_ignore_ascii_case("x-amz-target"))
            .map(|(_, value)| value.trim().to_owned())
    })?;
    let length = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("content-length") {
            value.trim().parse::<usize>().ok()
        } else {
            None
        }
    })?;
    assert!(
        length < MAX_REQUEST_BODY_BYTES,
        "fixture request body too large"
    );
    let body_end = header_end.checked_add(length)?;
    while data.len() < body_end {
        let count = stream.read(&mut chunk).ok()?;
        if count == 0 {
            return None;
        }
        data.extend_from_slice(&chunk[..count]);
    }
    Some((target, data.get(header_end..body_end)?.to_vec()))
}

fn respond(state: &mut FixtureState, target: &str, body: &Value) -> (u16, Value) {
    match target.rsplit('.').next().unwrap_or_default() {
        "DescribeTable" if state.describe_response.is_some() => {
            (200, state.describe_response.take().unwrap())
        }
        "DescribeTable" => (200, active_table()),
        "PutItem" => {
            let item = &body["Item"];
            let key = RowKey {
                pk: item["pk"]["S"].as_str().unwrap().into(),
                sk: item["sk"]["S"].as_str().unwrap().into(),
            };
            assert_eq!(
                key.pk, NAMESPACE,
                "wire partition must be explicit namespace"
            );
            let value = item["v"]["B"].as_str().expect("Binary value on wire");
            let bytes = decode_base64_known(value);
            let expires_at_ms = item
                .get("expires_at_ms")
                .and_then(|attribute| attribute["N"].as_str())
                .and_then(|number| number.parse().ok());
            let ttl_epoch_s = item
                .get("ttl_epoch_s")
                .and_then(|attribute| attribute["N"].as_str())
                .and_then(|number| number.parse().ok());
            state.rows.insert(
                key,
                Row {
                    value: bytes,
                    expires_at_ms,
                    ttl_epoch_s,
                },
            );
            (200, json!({}))
        }
        "GetItem" => {
            let key = wire_key(body);
            let mut response = json!({});
            if let Some(row) = state.rows.get(&key) {
                response["Item"] = row_to_item(&key, row);
            }
            (200, response)
        }
        "DeleteItem" => {
            state.rows.remove(&wire_key(body));
            (200, json!({}))
        }
        "Query" => {
            if let Some(page) = state.query_pages.pop_front() {
                return (200, page);
            }
            let partition = body["ExpressionAttributeValues"][":pk"]["S"]
                .as_str()
                .unwrap();
            assert_eq!(partition, NAMESPACE, "query is namespace scoped");
            let prefix = body["ExpressionAttributeValues"]
                .get(":prefix")
                .and_then(|attribute| attribute["S"].as_str())
                .unwrap_or("");
            let after = body
                .get("ExclusiveStartKey")
                .and_then(|key| key["sk"]["S"].as_str())
                .unwrap_or("");
            let limit = usize::try_from(body["Limit"].as_u64().unwrap_or(100)).unwrap_or(100);
            let matching: Vec<_> = state
                .rows
                .iter()
                .filter(|(key, _)| {
                    key.pk == partition && key.sk.starts_with(prefix) && key.sk.as_str() > after
                })
                .collect();
            let selected: Vec<_> = matching.iter().take(limit).copied().collect();
            let rows: Vec<Value> = selected
                .iter()
                .map(|(key, row)| row_to_item(key, row))
                .collect();
            let mut response = json!({"Items": rows});
            if matching.len() > limit {
                let (last_key, _) = selected
                    .last()
                    .expect("positive query limit has selected row");
                response["LastEvaluatedKey"] =
                    json!({"pk":{"S":last_key.pk}, "sk":{"S":last_key.sk}});
            }
            (200, response)
        }
        other => panic!("unexpected DynamoDB SDK operation target: {other}"),
    }
}

fn wire_key(body: &Value) -> RowKey {
    RowKey {
        pk: body["Key"]["pk"]["S"].as_str().unwrap().into(),
        sk: body["Key"]["sk"]["S"].as_str().unwrap().into(),
    }
}

fn row_to_item(key: &RowKey, row: &Row) -> Value {
    let mut item =
        json!({"pk":{"S":key.pk}, "sk":{"S":key.sk}, "v":{"B":encode_base64(&row.value)}});
    if let Some(value) = row.expires_at_ms {
        item["expires_at_ms"] = json!({"N":value.to_string()});
    }
    if let Some(value) = row.ttl_epoch_s {
        item["ttl_epoch_s"] = json!({"N":value.to_string()});
    }
    item
}

fn encode_base64(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

fn decode_base64_known(input: &str) -> Vec<u8> {
    STANDARD
        .decode(input)
        .expect("SDK Binary uses standard base64")
}

fn loopback_client(runtime: &Runtime, endpoint: &str, limits: DynamoDbKvLimits) -> Client {
    // Region and static credentials are explicitly injected before config load;
    // the service endpoint is explicit in the DynamoDB config. There is no
    // profile, web-identity, IMDS, environment-credential, or endpoint lookup.
    let shared = runtime.block_on(
        aws_config::defaults(BehaviorVersion::v2026_01_12())
            .region(Region::new(REGION))
            .credentials_provider(Credentials::new(
                "fixture-access-key",
                "fixture-secret-key",
                None,
                None,
                "sdk-tests",
            ))
            .load(),
    );
    let budget = Duration::from_millis(limits.operation_timeout_ms);
    let config = aws_sdk_dynamodb::Config::from(&shared)
        .to_builder()
        .region(Region::new(REGION))
        .endpoint_url(endpoint)
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(budget.min(Duration::from_millis(limits.connect_timeout_ms)))
                .operation_attempt_timeout(
                    budget.min(Duration::from_millis(limits.attempt_timeout_ms)),
                )
                .operation_timeout(budget)
                .build(),
        )
        .build();
    Client::from_conf(config)
}

fn active_table() -> Value {
    json!({"Table": {
        "TableName": TABLE, "TableStatus": "ACTIVE",
        "KeySchema": [
            {"AttributeName":"pk", "KeyType":"HASH"},
            {"AttributeName":"sk", "KeyType":"RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName":"pk", "AttributeType":"S"},
            {"AttributeName":"sk", "AttributeType":"S"}
        ], "GlobalSecondaryIndexes": [], "LocalSecondaryIndexes": [], "Replicas": []
    }})
}

/// Runtime is created before constructing the actual provider and before
/// invoking the futures-executor contract macro. The wrapper is only an
/// executor bridge; every operation still reaches `DynamoDbStore` + the real SDK.
struct RuntimeStore {
    fixture: DynamoFixture,
    runtime: Runtime,
    store: DynamoDbStore,
}

impl RuntimeStore {
    fn new() -> Self {
        let fixture = DynamoFixture::start();
        // Establish the runtime before provider construction/factory use.
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let limits = DynamoDbKvLimits::default();
        let client = loopback_client(&runtime, &fixture.endpoint(), limits);
        let store = DynamoDbStore::new(&settings(), client, MonotonicClock::default());
        Self {
            fixture,
            runtime,
            store,
        }
    }
}

fn settings() -> DynamoDbKvSettings {
    DynamoDbKvSettings {
        region: REGION.into(),
        table: TABLE.into(),
        namespace: NAMESPACE.into(),
        limits: DynamoDbKvLimits::default(),
    }
}

#[async_trait(?Send)]
impl KvStore for RuntimeStore {
    async fn delete(&self, key: &str) -> Result<(), KvError> {
        self.runtime.block_on(self.store.delete(key))
    }
    async fn exists(&self, key: &str) -> Result<bool, KvError> {
        self.runtime.block_on(self.store.exists(key))
    }
    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        self.runtime.block_on(self.store.get_bytes(key))
    }
    async fn list_keys_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        self.runtime
            .block_on(self.store.list_keys_page(prefix, cursor, limit))
    }
    async fn put_bytes(&self, key: &str, value: Bytes) -> Result<(), KvError> {
        self.runtime.block_on(self.store.put_bytes(key, value))
    }
    async fn put_bytes_with_ttl(
        &self,
        key: &str,
        value: Bytes,
        ttl: Duration,
    ) -> Result<(), KvError> {
        self.runtime
            .block_on(self.store.put_bytes_with_ttl(key, value, ttl))
    }
}

edgezero_core::key_value_store_contract_tests!(dynamodb_real_store_contract, RuntimeStore::new());

#[test]
fn sdk_wire_put_preserves_raw_binary_and_explicit_namespace() {
    let store = RuntimeStore::new();
    store
        .runtime
        .block_on(
            store
                .store
                .put_bytes(TEST_KEY, Bytes::from_static(TEST_VALUE)),
        )
        .unwrap();
    let requests = store.fixture.requests();
    let (target, body) = requests
        .iter()
        .find(|(target, _)| target.ends_with("PutItem"))
        .unwrap();
    assert!(target.starts_with("DynamoDB_20120810."));
    assert_eq!(body["Item"]["pk"]["S"], NAMESPACE);
    assert_eq!(body["Item"]["sk"]["S"], TEST_KEY);
    assert_eq!(
        decode_base64_known(body["Item"]["v"]["B"].as_str().unwrap()),
        TEST_VALUE
    );
}

#[test]
fn nfc_and_nfd_are_distinct_wire_sort_keys() {
    let store = RuntimeStore::new();
    store.runtime.block_on(async {
        store
            .store
            .put_bytes("\u{e9}", Bytes::from_static(b"nfc"))
            .await
            .unwrap();
        store
            .store
            .put_bytes("e\u{301}", Bytes::from_static(b"nfd"))
            .await
            .unwrap();
        assert_eq!(
            store.store.get_bytes("\u{e9}").await.unwrap(),
            Some(Bytes::from_static(b"nfc"))
        );
        assert_eq!(
            store.store.get_bytes("e\u{301}").await.unwrap(),
            Some(Bytes::from_static(b"nfd"))
        );
    });
    let sort_keys: Vec<_> = store
        .fixture
        .requests()
        .iter()
        .filter(|(target, _)| target.ends_with("PutItem"))
        .map(|(_, body)| body["Item"]["sk"]["S"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(sort_keys, ["\u{e9}", "e\u{301}"]);
}

#[test]
fn malformed_and_cross_scope_cursors_fail_before_http() {
    let store = RuntimeStore::new();
    let before = store.fixture.request_count();
    let malformed = store
        .runtime
        .block_on(store.store.list_keys_page("", Some("not-a-cursor"), 2));
    assert!(matches!(malformed, Err(KvError::Validation(_))));
    assert_eq!(store.fixture.request_count(), before);
    let cursor = store
        .store
        .encode_cursor("p/", "p/after".to_owned())
        .unwrap();
    let mut other_settings = settings();
    other_settings.namespace = "another-namespace".to_owned();
    let other_client = loopback_client(
        &store.runtime,
        &store.fixture.endpoint(),
        other_settings.limits,
    );
    let other = DynamoDbStore::new(&other_settings, other_client, MonotonicClock::default());
    let cross_scope = store
        .runtime
        .block_on(other.list_keys_page("p/", Some(&cursor), 2));
    assert!(matches!(cross_scope, Err(KvError::Validation(_))));
    assert_eq!(store.fixture.request_count(), before);
}

#[test]
fn empty_expired_query_page_with_last_evaluated_key_is_not_filled() {
    let store = RuntimeStore::new();
    let continuation = json!({"pk":{"S":NAMESPACE}, "sk":{"S":"p/expired"}});
    store.fixture.push_query_page(json!({
        "Items": [{
            "pk":{"S":NAMESPACE}, "sk":{"S":"p/expired"},
            "expires_at_ms":{"N":"1"}, "ttl_epoch_s":{"N":"1"}
        }],
        "LastEvaluatedKey": continuation
    }));
    store.fixture.push_query_page(json!({
        "Items": [{"pk":{"S":NAMESPACE}, "sk":{"S":"p/live"}, "v":{"B":"bGl2ZQ=="}}]
    }));
    let first = store
        .runtime
        .block_on(store.store.list_keys_page("p/", None, 1))
        .unwrap();
    assert!(first.keys.is_empty());
    assert!(first.cursor.is_some());
    assert_eq!(
        store
            .fixture
            .requests()
            .iter()
            .filter(|(target, _)| target.ends_with("Query"))
            .count(),
        1,
        "provider must not fill an empty expired page with another Query"
    );
    let second = store
        .runtime
        .block_on(store.store.list_keys_page("p/", first.cursor.as_deref(), 1))
        .unwrap();
    assert_eq!(second.keys, ["p/live"]);
    let queries: Vec<_> = store
        .fixture
        .requests()
        .into_iter()
        .filter(|(target, _)| target.ends_with("Query"))
        .collect();
    assert!(queries[1].1.get("ExclusiveStartKey").is_some());
    assert_eq!(queries.len(), 2);
}

fn run_prepare(
    fixture: &DynamoFixture,
    settings: &DynamoDbKvSettings,
) -> Result<KvHandle, crate::PreparationError> {
    let runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_error| crate::PreparationError::Unavailable)?;
    let client = loopback_client(&runtime, &fixture.endpoint(), settings.limits);
    let mut preparation =
        AwsPreparation::new(MonotonicClock::default(), AwsPreparationLimits::default())?;
    runtime.block_on(preparation.prepare_dynamodb_with_client(settings, client))
}

#[test]
fn preparation_describes_and_accepts_only_the_supported_active_schema() {
    let fixture = DynamoFixture::start();
    let result = run_prepare(&fixture, &settings());
    result.unwrap();
    let requests = fixture.requests();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].0.ends_with("DescribeTable"));
    assert_eq!(requests[0].1["TableName"], TABLE);
}

#[test]
fn preparation_rejects_missing_denied_and_unsupported_tables_closed() {
    let cases = [
        (
            400,
            json!({"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException", "message":"fixture-secret-marker"}),
        ),
        (
            400,
            json!({"__type":"com.amazonaws.dynamodb.v20120810#AccessDeniedException", "message":"fixture-secret-marker"}),
        ),
    ];
    for (status, response) in cases {
        let fixture = DynamoFixture::start();
        fixture.fail_target("DynamoDB_20120810.DescribeTable", status, response);
        let result = run_prepare(&fixture, &settings());
        let failure = result.unwrap_err();
        assert_eq!(fixture.request_count(), 1);
        assert!(!format!("{failure:?}").contains("fixture-secret-marker"));
    }

    let fixture = DynamoFixture::start();
    let mut wrong_schema = active_table();
    wrong_schema["Table"]["KeySchema"][0]["AttributeName"] = json!("wrong-pk");
    fixture.set_describe_response(wrong_schema);
    let result = run_prepare(&fixture, &settings());
    assert_eq!(result.err(), Some(crate::PreparationError::InvalidResource));
    assert_eq!(fixture.request_count(), 1);
}

#[test]
fn preparation_rejects_indexes_and_global_table_metadata() {
    let indexed = [
        ("GlobalSecondaryIndexes", json!([{"IndexName":"gsi"}])),
        ("LocalSecondaryIndexes", json!([{"IndexName":"lsi"}])),
        ("Replicas", json!([{"RegionName":"us-west-2"}])),
    ];
    for (field, value) in indexed {
        let fixture = DynamoFixture::start();
        let mut table = active_table();
        table["Table"][field] = value;
        fixture.set_describe_response(table);
        let result = run_prepare(&fixture, &settings());
        assert_eq!(
            result.err(),
            Some(crate::PreparationError::InvalidResource),
            "unexpectedly accepted {field}"
        );
        assert_eq!(fixture.request_count(), 1);
    }
}

#[test]
fn provider_enforces_value_and_key_boundaries_before_io() {
    let store = RuntimeStore::new();
    let accepted = Bytes::from(vec![0x5a; store.store.limits.max_value_bytes]);
    store
        .runtime
        .block_on(store.store.put_bytes("boundary", accepted))
        .unwrap();
    assert_eq!(
        store
            .runtime
            .block_on(store.store.get_bytes("boundary"))
            .unwrap(),
        Some(Bytes::from(vec![0x5a; store.store.limits.max_value_bytes]))
    );
    let after_valid = store.fixture.request_count();
    assert_eq!(after_valid, 2);
    let too_large_size = store
        .store
        .limits
        .max_value_bytes
        .checked_add(1)
        .expect("test cap has headroom");
    let too_large = Bytes::from(vec![0x5a; too_large_size]);
    let oversized_result = store
        .runtime
        .block_on(store.store.put_bytes("too-large", too_large));
    assert!(matches!(oversized_result, Err(KvError::Validation(_))));
    assert_eq!(store.fixture.request_count(), after_valid);

    store
        .runtime
        .block_on(store.store.put_bytes("empty-binary", Bytes::new()))
        .unwrap();
    assert_eq!(
        store
            .runtime
            .block_on(store.store.get_bytes("empty-binary"))
            .unwrap(),
        Some(Bytes::new())
    );

    let key_512 = "k".repeat(512);
    store
        .runtime
        .block_on(store.store.put_bytes(&key_512, Bytes::new()))
        .unwrap();
    let before_invalid_key = store.fixture.request_count();
    let invalid_key_result = store
        .runtime
        .block_on(store.store.get_bytes(&"k".repeat(513)));
    assert!(matches!(invalid_key_result, Err(KvError::Validation(_))));
    assert_eq!(store.fixture.request_count(), before_invalid_key);
}

#[test]
fn malformed_stored_expiry_fails_closed() {
    let store = RuntimeStore::new();
    store.fixture.seed(
        "malformed",
        Row {
            expires_at_ms: Some(1_000),
            ttl_epoch_s: Some(9),
            value: b"secret-row-value".to_vec(),
        },
    );
    let result = store.runtime.block_on(store.store.get_bytes("malformed"));
    assert!(matches!(&result, Err(KvError::Unavailable)));
    assert_eq!(store.fixture.request_count(), 1);
    assert!(!format!("{result:?}").contains("secret-row-value"));
}

#[test]
fn ttl_renewal_survives_and_plain_put_clears_expiry_fields() {
    let store = RuntimeStore::new();
    store
        .runtime
        .block_on(store.store.put_bytes_with_ttl(
            "renew",
            Bytes::from_static(b"old"),
            Duration::from_millis(1),
        ))
        .unwrap();
    store
        .runtime
        .block_on(store.store.put_bytes_with_ttl(
            "renew",
            Bytes::from_static(b"renewed"),
            Duration::from_secs(2),
        ))
        .unwrap();
    thread::sleep(Duration::from_millis(25));
    assert_eq!(
        store
            .runtime
            .block_on(store.store.get_bytes("renew"))
            .unwrap(),
        Some(Bytes::from_static(b"renewed"))
    );
    store
        .runtime
        .block_on(
            store
                .store
                .put_bytes("renew", Bytes::from_static(b"permanent")),
        )
        .unwrap();
    let puts: Vec<_> = store
        .fixture
        .requests()
        .into_iter()
        .filter(|(target, _)| target.ends_with("PutItem"))
        .collect();
    assert_eq!(puts.len(), 3);
    assert!(puts[0].1["Item"].get("expires_at_ms").is_some());
    assert!(puts[0].1["Item"].get("ttl_epoch_s").is_some());
    assert!(puts[2].1["Item"].get("expires_at_ms").is_none());
    assert!(puts[2].1["Item"].get("ttl_epoch_s").is_none());
}

#[test]
fn write_failure_is_one_attempt_and_reports_no_secret_material() {
    let store = RuntimeStore::new();
    let marker = "private-fixture-value-7fbc";
    store.fixture.commit_before_error();
    store.fixture.fail_target(
        "DynamoDB_20120810.PutItem",
        500,
        json!({"__type":"InternalServerError", "message":"fixture-secret-marker"}),
    );
    let result = store.runtime.block_on(
        store
            .store
            .put_bytes("private-fixture-key", Bytes::from(marker)),
    );
    assert!(matches!(&result, Err(KvError::Unavailable)));
    let requests = store.fixture.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|(target, _)| target.ends_with("PutItem"))
            .count(),
        1
    );
    assert!(
        store.fixture.has_row("private-fixture-key"),
        "fixture committed before returning 500"
    );
    let diagnostic = format!("{result:?}");
    for secret in [
        marker,
        "private-fixture-key",
        "fixture-secret-key",
        "fixture-secret-marker",
    ] {
        assert!(!diagnostic.contains(secret));
    }
}

#[test]
fn ambiguous_delete_can_remove_row_before_unavailable_response() {
    let store = RuntimeStore::new();
    store.fixture.seed(
        "ambiguous-delete",
        Row {
            expires_at_ms: None,
            ttl_epoch_s: None,
            value: b"before".to_vec(),
        },
    );
    store.fixture.commit_before_error();
    store.fixture.fail_target(
        "DynamoDB_20120810.DeleteItem",
        500,
        json!({"__type":"InternalServerError", "message":"fixture"}),
    );
    let result = store
        .runtime
        .block_on(store.store.delete("ambiguous-delete"));
    assert!(matches!(result, Err(KvError::Unavailable)));
    assert_eq!(store.fixture.request_count(), 1);
    assert!(!store.fixture.has_row("ambiguous-delete"));
}

#[test]
fn operation_stall_is_bounded() {
    let store = RuntimeStore::new();
    store.fixture.stall_target("DynamoDB_20120810.GetItem");
    let start = Instant::now();
    let result = store.runtime.block_on(store.store.get_bytes("stall"));
    assert!(matches!(result, Err(KvError::Unavailable)));
    assert!(
        start.elapsed() < Duration::from_secs(4),
        "operation timeout did not bound stall"
    );
    assert_eq!(store.fixture.request_count(), 1);
}

#[test]
fn sdk_get_output_debug_contains_key_and_binary_hex() {
    let key = "debug-key-sentinel-91a2";
    let binary = AttributeValue::B(Blob::new(vec![0xde, 0xad, 0xb3, 0x3f]));
    let item = HashMap::from([
        ("sk".to_owned(), AttributeValue::S(key.to_owned())),
        ("v".to_owned(), binary),
    ]);
    let output = GetItemOutput::builder().set_item(Some(item)).build();
    let diagnostic = format!("{output:?}");
    assert!(diagnostic.contains(key));
    assert!(diagnostic.contains("deadb33f"));
}

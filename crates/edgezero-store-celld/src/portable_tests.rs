//! The portable exchanger through a real `HttpClient` against the scripted service.
//! Production bindings require HTTPS; these tests construct the exchanger directly
//! over loopback HTTP to observe its requests. Real TLS is proven under Viceroy.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use edgezero_adapter_axum::outbound::AxumOutboundClient;
use edgezero_adapter_axum::outbound_executor::AxumOutboundExecutor;
use edgezero_core::config_store::BoundedStoreRead;
use edgezero_core::key_value_store::{KvError, KvStore as _};
use edgezero_core::outbound::HttpClient;
use edgezero_core::record_store::{
    RecordKeyLayout, RecordLayout, RecordWrite, RecordWriteMode, RecordWriteOutcome,
};
use edgezero_core::secret_store::{InMemorySecretStore, SecretError, SecretHandle, SecretStore};
use edgezero_core::store_registry::BoundSecretStore;
use edgezero_core::time::{Deadline, MonotonicClock};
use futures_util::lock::Mutex;
use serde_json::json;
use url::Url;

use super::{PortableExchanger, PreparedCelldRecords, Shared, celld_token_key};
use crate::PreparationError;
use crate::records::{CelldRecordKvStore, record_binding};
use crate::settings::{CelldPreparationLimits, CelldRecordsSettings};
use crate::test_support::{Fixture, Reply};

const INSTANCE: &str = "AAAAAAAAAAAAAAAAAAAAAA";
const TOKEN: &str = "portable-test-bearer-credential-32-bytes";

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

struct Counting {
    inner: InMemorySecretStore,
    reads: Arc<AtomicUsize>,
}

#[async_trait::async_trait(?Send)]
impl SecretStore for Counting {
    async fn get_bytes(&self, store_name: &str, key: &str) -> Result<Option<Bytes>, SecretError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get_bytes(store_name, key).await
    }

    async fn get_bytes_bounded(
        &self,
        store_name: &str,
        key: &str,
        clock: &MonotonicClock,
        deadline: Deadline,
        max_backend_bytes: u64,
        max_value_bytes: u64,
    ) -> Result<BoundedStoreRead<Bytes>, SecretError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner
            .get_bytes_bounded(
                store_name,
                key,
                clock,
                deadline,
                max_backend_bytes,
                max_value_bytes,
            )
            .await
    }
}

fn layout() -> RecordLayout {
    RecordLayout::new(Layout).unwrap()
}

fn headers() -> String {
    format!(
        "x-edgezero-store-version: 1\r\nx-edgezero-record-version: 1\r\nx-edgezero-store-instance: {INSTANCE}\r\n"
    )
}

fn ack() -> Reply {
    Reply::custom(204, "application/json", &headers(), b"")
}

fn error(status: u16, category: &str) -> Reply {
    Reply::custom(
        status,
        "application/json",
        &headers(),
        &serde_json::to_vec(&json!({ "error": category })).unwrap(),
    )
}

fn secrets(token: bool) -> (BoundSecretStore, Arc<AtomicUsize>) {
    let key = format!("stores/{}", celld_token_key("sessions"));
    let entries = if token {
        vec![(key, TOKEN.to_owned())]
    } else {
        Vec::new()
    };
    let reads = Arc::new(AtomicUsize::new(0));
    let store = Counting {
        inner: InMemorySecretStore::new(entries),
        reads: Arc::clone(&reads),
    };
    (
        BoundSecretStore::new(SecretHandle::new(Arc::new(store)), "stores".to_owned()),
        reads,
    )
}

fn store(
    fixture: &Fixture,
    executor: &AxumOutboundExecutor,
    secrets: BoundSecretStore,
) -> CelldRecordKvStore<PortableExchanger> {
    let endpoint = Url::parse(fixture.endpoint()).unwrap();
    let client = HttpClient::with_client(AxumOutboundClient::try_new(executor.handle()).unwrap());
    let instance = *b"AAAAAAAAAAAAAAAAAAAAAA";
    let limits = CelldPreparationLimits::PORTABLE;
    CelldRecordKvStore::pinned(
        Arc::new(PortableExchanger {
            authorization: Mutex::new(None),
            clock: client.time().clock().clone(),
            client,
            shared: Arc::new(Shared {
                endpoint: endpoint.clone(),
                instance,
                limits,
                secrets,
                store: "state".to_owned(),
                token_key: celld_token_key("sessions"),
            }),
        }),
        record_binding(&endpoint, "state", INSTANCE, &layout()).unwrap(),
        instance,
        limits,
        "state".to_owned(),
    )
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
async fn requests_carry_bearer_instance_and_fresh_mutation_ids_with_one_token_read() {
    let fixture = Fixture::new(vec![ack(), ack()]).await;
    let mut executor = AxumOutboundExecutor::new();
    let (secrets, reads) = secrets(true);
    let records = store(&fixture, &executor, secrets);
    executor
        .run_until(async {
            let partition = records
                .record_binding()
                .unwrap()
                .layout
                .resolve("group/a")
                .unwrap();
            for key in ["group/a", "group/b"] {
                assert_eq!(
                    records
                        .record_insert(key, &partition, write(RecordWriteMode::Add))
                        .await
                        .unwrap(),
                    RecordWriteOutcome::Written
                );
            }
        })
        .await;
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    let requests = fixture.requests().await;
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_eq!(request.method, "POST");
        assert_eq!(
            request.authorization.as_deref(),
            Some(format!("Bearer {TOKEN}").as_str())
        );
        assert_eq!(request.record_version.as_deref(), Some("1"));
        assert_eq!(request.expected_instance.as_deref(), Some(INSTANCE));
        assert_eq!(request.encoding.as_deref(), Some("identity"));
        assert!(request.mutation_id.is_some());
    }
    assert_ne!(requests[0].mutation_id, requests[1].mutation_id);
}

#[tokio::test(flavor = "current_thread")]
async fn outcomes_use_the_native_classifier_without_retry() {
    for (reply, mode, expected) in [
        (
            error(412, "precondition_failed"),
            RecordWriteMode::Add,
            Ok(RecordWriteOutcome::PreconditionFailed),
        ),
        (
            error(503, "unavailable"),
            RecordWriteMode::Add,
            Err(KvError::MutationUncertain),
        ),
        (
            Reply::Drop,
            RecordWriteMode::Overwrite,
            Err(KvError::MutationUncertain),
        ),
        (
            error(403, "forbidden"),
            RecordWriteMode::Overwrite,
            Err(KvError::Unavailable),
        ),
    ] {
        let fixture = Fixture::new(vec![reply]).await;
        let mut executor = AxumOutboundExecutor::new();
        let (secrets, _reads) = secrets(true);
        let records = store(&fixture, &executor, secrets);
        let outcome = executor
            .run_until(async {
                let partition = records
                    .record_binding()
                    .unwrap()
                    .layout
                    .resolve("group/a")
                    .unwrap();
                records
                    .record_insert("group/a", &partition, write(mode))
                    .await
            })
            .await;
        assert_eq!(
            outcome.map_err(|failure| failure.to_string()),
            expected.map_err(|failure: KvError| failure.to_string())
        );
        assert_eq!(fixture.count().await, 1, "no retry");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_missing_token_fails_before_dispatch() {
    let fixture = Fixture::new(vec![ack()]).await;
    let mut executor = AxumOutboundExecutor::new();
    let (secrets, _reads) = secrets(false);
    let records = store(&fixture, &executor, secrets);
    let outcome = executor
        .run_until(async {
            let partition = records
                .record_binding()
                .unwrap()
                .layout
                .resolve("group/a")
                .unwrap();
            records
                .record_insert("group/a", &partition, write(RecordWriteMode::Add))
                .await
        })
        .await;
    assert!(matches!(outcome, Err(KvError::Unavailable)));
    assert_eq!(fixture.count().await, 0);
}

#[test]
fn portable_bindings_require_authenticated_https_and_a_pinned_instance() {
    let settings =
        |value: serde_json::Value| serde_json::from_value::<CelldRecordsSettings>(value).unwrap();
    let valid = json!({
        "endpoint": "https://celld.example.net",
        "store": "sessions",
        "pinned": { "instance": INSTANCE },
        "access": { "mode": "authenticated", "token_secret": { "secrets": "stores" } },
    });
    let (bound, reads) = secrets(true);
    let prepared =
        PreparedCelldRecords::new("sessions", &settings(valid.clone()), &layout(), bound).unwrap();
    assert_eq!(
        prepared.record_binding().scope,
        record_binding(
            &Url::parse("https://celld.example.net").unwrap(),
            "sessions",
            INSTANCE,
            &layout()
        )
        .unwrap()
        .scope
    );
    assert_eq!(prepared.record_binding().max_metadata_bytes, 1_024);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    let with = |field: &str, value: serde_json::Value| {
        let mut changed = valid.clone();
        changed[field] = value;
        changed
    };
    let mut unpinned = valid.clone();
    unpinned.as_object_mut().unwrap().remove("pinned");
    for (input, error) in [
        (
            with("endpoint", json!("http://celld.example.net")),
            PreparationError::InvalidEndpoint,
        ),
        (
            with(
                "access",
                json!({ "mode": "authenticated", "token_env": "TOKEN" }),
            ),
            PreparationError::InvalidBinding,
        ),
        (
            with("access", json!({ "mode": "trusted-network" })),
            PreparationError::InvalidBinding,
        ),
        (unpinned, PreparationError::InvalidBinding),
    ] {
        let (rejected_store, _reads) = secrets(true);
        assert_eq!(
            PreparedCelldRecords::new(
                "sessions",
                &settings(input.clone()),
                &layout(),
                rejected_store,
            )
            .map(|_prepared| ())
            .unwrap_err(),
            error,
            "{input}"
        );
    }
    let (uppercase_store, _reads) = secrets(true);
    assert_eq!(
        PreparedCelldRecords::new("Sessions", &settings(valid), &layout(), uppercase_store)
            .map(|_prepared| ())
            .unwrap_err(),
        PreparationError::InvalidBinding
    );
}

//! Explicit `DynamoDB` Local acceptance. Invoked by the existing local AWS runner.
//! No hosted AWS credentials, IAM qualification, retry fault injection or native
//! TTL sweeper behavior is claimed by these tests.

#![cfg(all(not(target_arch = "wasm32"), feature = "dynamodb-records"))]
#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "group local-service setup, provider helpers and complete test lifecycles"
)]

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::env;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::process::{self, Command};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use aws_sdk_dynamodb::Client;
    use aws_sdk_dynamodb::config::{Credentials, Region, retry::RetryConfig};
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    };
    use edgezero_core::key_value_store::{KvError, KvHandle};
    use edgezero_core::record_store::{
        RecordGeneration, RecordKeyLayout, RecordLayout, RecordWrite, RecordWriteMode,
        RecordWriteOutcome, contract,
    };
    use edgezero_core::time::MonotonicClock;
    use edgezero_store_aws::AwsPreparation;
    use edgezero_store_aws::settings::{
        AwsPreparationLimits, DynamoDbKvLimits, DynamoDbKvSettings,
    };
    use tokio::runtime::Builder;

    type Item = HashMap<String, AttributeValue>;

    #[derive(Clone, Copy)]
    struct Groups;

    impl RecordKeyLayout for Groups {
        fn id(&self) -> &'static str {
            "dynamodb-local-groups"
        }
        fn version(&self) -> u32 {
            1
        }
        fn validate(&self) -> Result<(), KvError> {
            Ok(())
        }
        fn partition_prefix<'key>(&self, key: &'key str) -> Result<&'key str, KvError> {
            let end = key
                .find('/')
                .ok_or_else(|| KvError::Validation("missing group".to_owned()))?;
            key.get(..=end)
                .ok_or_else(|| KvError::Validation("invalid group".to_owned()))
        }
        fn validate_partition(&self, prefix: &str) -> Result<(), KvError> {
            if prefix.ends_with('/') && prefix.bytes().filter(|byte| *byte == b'/').count() == 1 {
                Ok(())
            } else {
                Err(KvError::Validation("incomplete group".to_owned()))
            }
        }
    }

    fn endpoint() -> String {
        let endpoint = env::var("EDGEZERO_AWS_TEST_DYNAMODB_ENDPOINT")
            .expect("explicit DynamoDB Local endpoint required; use the existing local AWS runner");
        let address = endpoint
            .strip_prefix("http://")
            .and_then(|authority| authority.parse::<SocketAddr>().ok())
            .expect("DynamoDB Local endpoint must be literal http://127.0.0.1:<port>");
        assert_eq!(address.ip(), Ipv4Addr::LOCALHOST);
        assert_ne!(address.port(), 0);
        assert_eq!(endpoint, format!("http://{address}"));
        endpoint
    }

    fn isolated_child(test: &str) -> bool {
        let endpoint = endpoint();
        if env::var("EDGEZERO_DYNAMODB_RECORDS_CHILD").as_deref() == Ok(test) {
            assert!(
                env::var("AWS_ENDPOINT_URL_DYNAMODB").is_ok_and(|value| value == endpoint),
                "public preparation must use the validated local endpoint"
            );
            assert!(
                env::var("AWS_ACCESS_KEY_ID").as_deref() == Ok("DUMMYLOCAL")
                    && env::var("AWS_SECRET_ACCESS_KEY").as_deref() == Ok("DUMMYLOCAL"),
                "only dummy local credentials are allowed"
            );
            return false;
        }
        let qualified_test = format!("tests::{test}");
        let status = Command::new(env::current_exe().expect("test executable"))
            .args(["--ignored", "--exact", &qualified_test, "--nocapture"])
            .env("EDGEZERO_DYNAMODB_RECORDS_CHILD", test)
            .env("EDGEZERO_AWS_TEST_DYNAMODB_ENDPOINT", &endpoint)
            .env("AWS_ENDPOINT_URL_DYNAMODB", &endpoint)
            .env("AWS_ENDPOINT_URL", &endpoint)
            .env("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
            .env("AWS_ACCESS_KEY_ID", "DUMMYLOCAL")
            .env("AWS_SECRET_ACCESS_KEY", "DUMMYLOCAL")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_ACCOUNT_ID_ENDPOINT_MODE", "disabled")
            .env_remove("AWS_SESSION_TOKEN")
            .env_remove("AWS_PROFILE")
            .env_remove("AWS_WEB_IDENTITY_TOKEN_FILE")
            .env_remove("AWS_ROLE_ARN")
            .status()
            .expect("spawn isolated dummy-credential test process");
        assert!(status.success(), "DynamoDB Local child failed");
        true
    }

    async fn admin() -> Client {
        let config = aws_config::defaults(aws_config::BehaviorVersion::v2026_01_12())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "DUMMYLOCAL",
                "DUMMYLOCAL",
                None,
                None,
                "local-record-proof",
            ))
            .endpoint_url(endpoint())
            .load()
            .await;
        Client::from_conf(
            aws_sdk_dynamodb::Config::from(&config)
                .to_builder()
                .retry_config(RetryConfig::standard().with_max_attempts(1))
                .build(),
        )
    }

    async fn table(client: &Client) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let table = format!("edgezero-records-local-{}-{nanos}", process::id());
        client
            .create_table()
            .table_name(&table)
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("pk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("sk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("sk")
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .expect("create unique local record table");
        table
    }

    async fn prepared(table: &str, namespace: &str) -> KvHandle {
        let mut preparation =
            AwsPreparation::new(MonotonicClock::default(), AwsPreparationLimits::default())
                .unwrap();
        preparation
            .prepare_dynamodb_records(
                &DynamoDbKvSettings {
                    region: "us-east-1".to_owned(),
                    table: table.to_owned(),
                    namespace: namespace.to_owned(),
                    limits: DynamoDbKvLimits::default(),
                },
                &RecordLayout::new(Groups).unwrap(),
            )
            .await
            .expect("prepare actual public record provider")
    }

    fn write(body: &[u8], mode: RecordWriteMode) -> RecordWrite<'_> {
        RecordWrite {
            body,
            metadata: Some("exact m\u{e9}tadata"),
            ttl: None,
            mode,
        }
    }

    async fn all_items(client: &Client, table: &str) -> Vec<Item> {
        let mut after = None;
        let mut items = Vec::new();
        loop {
            let output = client
                .scan()
                .table_name(table)
                .consistent_read(true)
                .set_exclusive_start_key(after)
                .send()
                .await
                .unwrap();
            items.extend(output.items.unwrap_or_default());
            after = output.last_evaluated_key.filter(|key| !key.is_empty());
            if after.is_none() {
                return items;
            }
        }
    }

    fn text<'item>(item: &'item Item, name: &str) -> &'item str {
        item.get(name)
            .and_then(|value| value.as_s().ok())
            .expect("physical string attribute")
    }

    fn key(item: &Item) -> Item {
        Item::from([
            ("pk".to_owned(), item["pk"].clone()),
            ("sk".to_owned(), item["sk"].clone()),
        ])
    }

    async fn global_keys(kv: &KvHandle, prefix: &str) -> (BTreeSet<String>, usize) {
        let mut cursor = None;
        let mut keys = BTreeSet::new();
        let mut empty_continuations = 0_usize;
        loop {
            let page = kv
                .list_keys_page(prefix, cursor.as_deref(), 1)
                .await
                .unwrap();
            assert!(page.keys.len() <= 1);
            if page.keys.is_empty() && page.cursor.is_some() {
                empty_continuations = empty_continuations.checked_add(1).expect("page counter");
            }
            for key in page.keys {
                assert!(keys.insert(key), "duplicate global Scan key");
            }
            cursor = page.cursor;
            if cursor.is_none() {
                return (keys, empty_continuations);
            }
        }
    }

    #[test]
    #[ignore = "requires EDGEZERO_AWS_TEST_DYNAMODB_ENDPOINT from existing local AWS runner"]
    #[expect(
        clippy::too_many_lines,
        reason = "keep the actual common contract, independent writers and physical-layout assertions together"
    )]
    fn dynamodb_records_local_contract() {
        if isolated_child("dynamodb_records_local_contract") {
            return;
        }
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let client = admin().await;
                let table = table(&client).await;
                let kv = prepared(&table, "contract").await;
                contract::assert_record_contract(&kv, "group/").await;
                let first = prepared(&table, "independent")
                    .await
                    .require_records()
                    .unwrap();
                let second = prepared(&table, "independent")
                    .await
                    .require_records()
                    .unwrap();
                let (add_first, add_second) = futures::join!(
                    first.insert("a/add-race", write(b"first", RecordWriteMode::Add)),
                    second.insert("a/add-race", write(b"second", RecordWriteMode::Add))
                );
                assert_eq!(
                    [add_first.unwrap(), add_second.unwrap()]
                        .iter()
                        .filter(|outcome| **outcome == RecordWriteOutcome::Written)
                        .count(),
                    1
                );
                let generation = first
                    .lookup("a/add-race")
                    .await
                    .unwrap()
                    .unwrap()
                    .generation;
                let (cas_first, cas_second) = futures::join!(
                    first.insert(
                        "a/add-race",
                        write(b"first", RecordWriteMode::IfGenerationMatch(generation))
                    ),
                    second.insert(
                        "a/add-race",
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
                let listing = prepared(&table, "listing").await;
                let records = listing.require_records().unwrap();
                let expected: BTreeSet<_> = ["a/one", "a/two", "b/one", "b/two"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect();
                for key in &expected {
                    records
                        .insert(key, write(key.as_bytes(), RecordWriteMode::Add))
                        .await
                        .unwrap();
                }
                let foreign = prepared(&table, "foreign").await.require_records().unwrap();
                foreign
                    .insert("a/foreign", write(b"foreign", RecordWriteMode::Add))
                    .await
                    .unwrap();
                let mut cursor = None;
                let mut keys = BTreeSet::new();
                let mut empty_continuations = 0_usize;
                let partition = records.partition("a/").unwrap();
                loop {
                    let page = records
                        .list_partition(&partition, cursor.as_deref(), 1)
                        .await
                        .unwrap();
                    if page.keys.is_empty() && page.cursor.is_some() {
                        empty_continuations =
                            empty_continuations.checked_add(1).expect("page counter");
                    }
                    for key in page.keys {
                        assert!(keys.insert(key), "duplicate strong Query key");
                    }
                    cursor = page.cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
                assert_eq!(
                    keys,
                    BTreeSet::from(["a/one".to_owned(), "a/two".to_owned()])
                );
                assert!(
                    empty_continuations >= 2,
                    "native Query traverses allocator-only pages"
                );
                let (global, empty) = global_keys(&listing, "").await;
                assert_eq!(global, expected);
                assert!(
                    empty > 0,
                    "native Scan preserves internal/foreign filtered pages"
                );
                assert_eq!(
                    global_keys(&listing, "a/o").await.0,
                    BTreeSet::from(["a/one".to_owned()])
                );
                assert!(global_keys(&listing, "nonmatching").await.0.is_empty());
                let items = all_items(&client, &table).await;
                let values: BTreeMap<_, _> = items
                    .iter()
                    .filter(|item| {
                        ["r#a/one", "r#a/two", "r#b/one", "r#b/two"].contains(&text(item, "sk"))
                    })
                    .map(|item| (text(item, "sk"), item))
                    .collect();
                assert_ne!(text(values["r#a/one"], "pk"), text(values["r#b/one"], "pk"));
                assert_eq!(text(values["r#a/one"], "pk"), text(values["r#a/two"], "pk"));
                for logical in &expected {
                    let value = values[format!("r#{logical}").as_str()];
                    let allocator = items
                        .iter()
                        .find(|item| {
                            text(item, "pk") == text(value, "pk")
                                && text(item, "sk") == format!("g#{logical}")
                        })
                        .expect("retained per-key allocator colocated with value");
                    assert!(allocator.get("ttl_epoch_s").is_none());
                    assert!(allocator.get("v").is_none());
                    assert!(
                        allocator
                            .get("generation")
                            .unwrap()
                            .as_n()
                            .unwrap()
                            .parse::<u64>()
                            .unwrap()
                            > 0
                    );
                }
                assert!(
                    items
                        .iter()
                        .filter(|item| item.get("generation").is_some() && item.get("v").is_none())
                        .all(|item| text(item, "sk").starts_with("g#")),
                    "no namespace-wide allocator row"
                );
                client
                    .delete_table()
                    .table_name(&table)
                    .send()
                    .await
                    .unwrap();
            });
    }

    #[test]
    #[ignore = "requires EDGEZERO_AWS_TEST_DYNAMODB_ENDPOINT from existing local AWS runner"]
    #[expect(
        clippy::too_many_lines,
        reason = "keep retained-row visibility, simulated cleanup and provider reconstruction in one lifecycle"
    )]
    fn dynamodb_records_local_simulated_expiry_reopen() {
        if isolated_child("dynamodb_records_local_simulated_expiry_reopen") {
            return;
        }
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let client = admin().await;
                let table = table(&client).await;
                let kv = prepared(&table, "expiry-history").await;
                let records = kv.require_records().unwrap();
                records
                    .insert(
                        "a/expiry",
                        RecordWrite {
                            ttl: Some(Duration::from_secs(60)),
                            ..write(b"retained", RecordWriteMode::Add)
                        },
                    )
                    .await
                    .unwrap();
                let old = records
                    .lookup("a/expiry")
                    .await
                    .unwrap()
                    .unwrap()
                    .generation;
                let items = all_items(&client, &table).await;
                let value = items
                    .iter()
                    .find(|item| text(item, "sk") == "r#a/expiry")
                    .unwrap();
                let physical = key(value);
                client
                    .update_item()
                    .table_name(&table)
                    .set_key(Some(physical.clone()))
                    .update_expression("SET #ttl = :past")
                    .expression_attribute_names("#ttl", "ttl_epoch_s")
                    .expression_attribute_values(":past", AttributeValue::N("1".to_owned()))
                    .send()
                    .await
                    .unwrap();
                assert!(
                    records.exists_strong("a/expiry").await.unwrap(),
                    "native past-TTL retained row remains visible"
                );
                assert_eq!(
                    records
                        .lookup("a/expiry")
                        .await
                        .unwrap()
                        .unwrap()
                        .generation,
                    old
                );
                assert_eq!(
                    records
                        .insert("a/expiry", write(b"blocked", RecordWriteMode::Add))
                        .await
                        .unwrap(),
                    RecordWriteOutcome::PreconditionFailed
                );
                assert!(global_keys(&kv, "a/").await.0.contains("a/expiry"));
                // DynamoDB Local does not sweep TTL. This deletion simulates physical
                // expiry cleanup of only the value row, not a native sweeper proof.
                client
                    .delete_item()
                    .table_name(&table)
                    .set_key(Some(physical))
                    .send()
                    .await
                    .unwrap();
                drop(records);
                drop(kv);
                let reopened = prepared(&table, "expiry-history")
                    .await
                    .require_records()
                    .unwrap();
                reopened
                    .insert("a/expiry", write(b"recreated", RecordWriteMode::Add))
                    .await
                    .unwrap();
                assert_ne!(
                    reopened
                        .lookup("a/expiry")
                        .await
                        .unwrap()
                        .unwrap()
                        .generation,
                    old
                );
                assert_eq!(
                    reopened
                        .insert(
                            "a/expiry",
                            write(b"stale", RecordWriteMode::IfGenerationMatch(old))
                        )
                        .await
                        .unwrap(),
                    RecordWriteOutcome::PreconditionFailed
                );
                let reopened_items = all_items(&client, &table).await;
                let allocator = reopened_items
                    .iter()
                    .find(|item| text(item, "sk") == "g#a/expiry")
                    .unwrap();
                assert!(allocator.get("ttl_epoch_s").is_none());
                client
                    .update_item()
                    .table_name(&table)
                    .set_key(Some(key(allocator)))
                    .update_expression("SET #generation = :ceiling")
                    .expression_attribute_names("#generation", "generation")
                    .expression_attribute_values(
                        ":ceiling",
                        AttributeValue::N(u64::MAX.to_string()),
                    )
                    .send()
                    .await
                    .unwrap();
                assert!(matches!(
                    reopened
                        .insert("a/expiry", write(b"exhausted", RecordWriteMode::Overwrite))
                        .await
                        .unwrap_err(),
                    KvError::Unavailable
                ));
                assert_ne!(
                    reopened
                        .lookup("a/expiry")
                        .await
                        .unwrap()
                        .unwrap()
                        .generation,
                    RecordGeneration::from_u64(u64::MAX)
                );
                client
                    .delete_table()
                    .table_name(&table)
                    .send()
                    .await
                    .unwrap();
            });
    }
}

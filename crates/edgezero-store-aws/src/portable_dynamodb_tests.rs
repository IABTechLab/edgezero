//! Wire codec, signing and classification checks. Real-transport proof lives in the
//! loopback interop tests and the `DynamoDB` Local runner.

use super::*;
use edgezero_core::key_value_store::KvError;
use edgezero_core::record_store::RecordKeyLayout;
use edgezero_core::secret_store::{InMemorySecretStore, SecretHandle};
use serde_json::json;

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
        let end = key
            .find('/')
            .ok_or_else(|| KvError::Validation("missing group".to_owned()))?;
        key.get(..=end)
            .ok_or_else(|| KvError::Validation("missing group".to_owned()))
    }
    fn validate_partition(&self, prefix: &str) -> Result<(), KvError> {
        if prefix.ends_with('/') && prefix.matches('/').count() == 1 {
            Ok(())
        } else {
            Err(KvError::Validation("invalid group".to_owned()))
        }
    }
}

fn settings() -> DynamoDbRecordsSettings {
    toml::from_str(
        "region = 'us-east-1'\ntable = 'fixture-kv'\nnamespace = 'records-wire'\n\
         pinned = { table_arn = 'arn:aws:dynamodb:us-east-1:111111111111:table/fixture-kv', table_id = 'fixture-table-id' }\n\
         credentials = { secrets = 'stores' }",
    )
    .unwrap()
}

fn secrets() -> BoundSecretStore {
    BoundSecretStore::new(
        SecretHandle::new(Arc::new(InMemorySecretStore::new([(
            "KV__IDENTITY__AWS_ACCESS_KEY_ID",
            "AKIDFIXTURE",
        )]))),
        "stores".to_owned(),
    )
}

#[test]
fn credential_keys_are_namespaced_by_the_kv_id() {
    assert_eq!(
        DynamoDbCredentialKeys::for_kv_id("identity"),
        DynamoDbCredentialKeys {
            access_key_id: "KV__IDENTITY__AWS_ACCESS_KEY_ID".to_owned(),
            secret_access_key: "KV__IDENTITY__AWS_SECRET_ACCESS_KEY".to_owned(),
            session_token: "KV__IDENTITY__AWS_SESSION_TOKEN".to_owned(),
        }
    );
}

#[test]
fn preparation_needs_pins_credentials_and_a_lowercase_id_without_io() {
    let layout = RecordLayout::new(Groups).unwrap();
    let prepared =
        PreparedDynamoDbRecords::new("identity", &settings(), &layout, secrets()).unwrap();
    assert_eq!(
        prepared.record_binding().max_metadata_bytes,
        DynamoDbKvLimits::PORTABLE.max_value_bytes
    );
    // The scope is the tuple native preparation binds, with the regional endpoint.
    assert_eq!(
        prepared.record_binding().scope,
        json!([
            "https://dynamodb.us-east-1.amazonaws.com/",
            "arn:aws:dynamodb:us-east-1:111111111111:table/fixture-kv",
            "fixture-table-id",
            "us-east-1",
            "fixture-kv",
            "records-wire"
        ])
        .to_string()
    );
    for id in ["Identity", "", "id-entity"] {
        assert_eq!(
            PreparedDynamoDbRecords::new(id, &settings(), &layout, secrets()).unwrap_err(),
            PreparationError::InvalidBinding
        );
    }
    let mut unpinned = settings();
    unpinned.pinned = None;
    assert_eq!(
        PreparedDynamoDbRecords::new("identity", &unpinned, &layout, secrets()).unwrap_err(),
        PreparationError::InvalidBinding
    );
    let mut uncredentialed = settings();
    uncredentialed.credentials = None;
    assert_eq!(
        PreparedDynamoDbRecords::new("identity", &uncredentialed, &layout, secrets()).unwrap_err(),
        PreparationError::InvalidBinding
    );
}

#[test]
fn portable_endpoints_require_https_except_on_loopback() {
    let with = |endpoint: &str| {
        let mut binding = settings();
        binding.endpoint = Some(endpoint.to_owned());
        binding.portable_endpoint()
    };
    assert_eq!(
        with("http://127.0.0.1:8000").unwrap(),
        "http://127.0.0.1:8000/"
    );
    assert_eq!(with("http://[::1]:8000/").unwrap(), "http://[::1]:8000/");
    assert_eq!(
        with("https://dynamodb.example.net").unwrap(),
        "https://dynamodb.example.net/"
    );
    for rejected in [
        "http://dynamodb.us-east-1.amazonaws.com",
        "http://localhost:8000",
        "https://user:secret@dynamodb.example.net",
        "https://dynamodb.example.net/base",
        "https://dynamodb.example.net/?token=1",
        "ftp://dynamodb.example.net",
    ] {
        assert_eq!(
            with(rejected).unwrap_err(),
            PreparationError::InvalidEndpoint
        );
    }
    let mut china = settings();
    china.region = "cn-north-1".to_owned();
    assert_eq!(
        china.portable_endpoint().unwrap(),
        "https://dynamodb.cn-north-1.amazonaws.com.cn/"
    );
}

#[test]
fn requests_encode_the_json_protocol_and_omit_empty_maps() {
    let request = Request {
        key: Some(Item::from([
            ("pk".to_owned(), Attr::Text("p".to_owned())),
            ("sk".to_owned(), Attr::Text("r#k".to_owned())),
        ])),
        consistent_read: Some(true),
        ..Request::default()
    };
    assert_eq!(
        encode_request("fixture-kv", request),
        json!({"TableName": "fixture-kv", "Key": {"pk": {"S": "p"}, "sk": {"S": "r#k"}}, "ConsistentRead": true})
    );
    let update = Request {
        item: Some(Item::from([
            ("v".to_owned(), Attr::Binary(vec![0, 255])),
            ("generation".to_owned(), Attr::Number("7".to_owned())),
        ])),
        condition_expression: Some("attribute_not_exists(#pk)"),
        names: [("#pk".to_owned(), "pk".to_owned())].into(),
        return_updated_new: true,
        limit: Some(3_i32),
        ..Request::default()
    };
    assert_eq!(
        encode_request("t", update),
        json!({
            "TableName": "t",
            "Item": {"v": {"B": "AP8="}, "generation": {"N": "7"}},
            "ConditionExpression": "attribute_not_exists(#pk)",
            "ExpressionAttributeNames": {"#pk": "pk"},
            "ReturnValues": "UPDATED_NEW",
            "Limit": 3_i32
        })
    );
}

#[test]
fn responses_decode_scalars_and_keep_other_types_opaque() {
    let decoded = decode_response(
        br#"{"Items":[{"pk":{"S":"p"},"v":{"B":"AP8="},"n":{"N":"1"},"m":{"M":{}}}],
            "LastEvaluatedKey":{"pk":{"S":"p"},"sk":{"S":"s"}},"Count":1,"ScannedCount":4}"#,
    )
    .unwrap();
    assert_eq!(decoded.items.len(), 1);
    assert_eq!(decoded.items[0]["v"], Attr::Binary(vec![0, 255]));
    assert_eq!(decoded.items[0]["n"], Attr::Number("1".to_owned()));
    assert_eq!(decoded.items[0]["m"], Attr::Other);
    assert_eq!(
        decoded.last_evaluated_key.unwrap()["sk"],
        Attr::Text("s".to_owned())
    );
    assert!(decode_response(b"{}").unwrap().item.is_none());
    for malformed in [
        b"[]".as_slice(),
        br#"{"Item":{"v":{"B":"not base64!"}}}"#,
        br#"{"Item":{"v":{"S":1}}}"#,
        br#"{"Item":{"v":{"S":"a","N":"1"}}}"#,
        br#"{"Items":{}}"#,
        b"not json",
    ] {
        assert!(decode_response(malformed).is_none());
    }
}

#[test]
fn only_conditional_check_failures_are_conditional() {
    for body in [
        br#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"}"#
            .as_slice(),
        br#"{"__type":"ConditionalCheckFailedException","message":"x"}"#,
    ] {
        assert!(is_conditional_failure(body));
    }
    for body in [
        br#"{"__type":"com.amazonaws.dynamodb.v20120810#ValidationException"}"#.as_slice(),
        br#"{"message":"ConditionalCheckFailedException"}"#,
        b"",
    ] {
        assert!(!is_conditional_failure(body));
    }
}

#[test]
fn facade_rejections_never_reach_the_network() {
    assert_eq!(
        send_error(&EdgeError::validation("rejected before dispatch")),
        TransportError::NotDispatched
    );
    assert_eq!(
        send_error(&EdgeError::bad_gateway("reset after dispatch")),
        TransportError::Uncertain
    );
    assert_eq!(
        send_error(&EdgeError::gateway_timeout("late")),
        TransportError::Uncertain
    );
}

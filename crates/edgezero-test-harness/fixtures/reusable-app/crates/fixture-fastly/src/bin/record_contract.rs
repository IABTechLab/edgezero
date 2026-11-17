//! Run the shared public-handle contract against Viceroy's native KV operations.
//! Use the fixture's empty fixture_records store, not SDK native-test stubs.

use std::sync::Arc;

use edgezero_adapter_fastly::key_value_store::FastlyKvStore;
use edgezero_core::key_value_store::{KvError, KvHandle};
use edgezero_core::record_store::{
    RecordKeyLayout, RecordLayout, contract::assert_record_contract,
};
use fastly::{Error, Request, Response};
use futures::executor::block_on;

#[path = "../record_cases.rs"]
mod record_cases;

struct FixtureLayout;

impl RecordKeyLayout for FixtureLayout {
    fn id(&self) -> &str {
        "fastly-record-contract"
    }

    fn version(&self) -> u32 {
        1
    }

    fn validate(&self) -> Result<(), KvError> {
        Ok(())
    }

    fn partition_prefix<'a>(&self, key: &'a str) -> Result<&'a str, KvError> {
        key.find('/')
            .filter(|index| *index > 0 && *index + 1 < key.len())
            .map(|index| &key[..=index])
            .ok_or_else(|| KvError::Validation("invalid fixture record key".to_owned()))
    }

    fn validate_partition(&self, prefix: &str) -> Result<(), KvError> {
        if prefix.ends_with('/') && prefix.len() > 1 && prefix.matches('/').count() == 1 {
            Ok(())
        } else {
            Err(KvError::Validation("invalid fixture partition".to_owned()))
        }
    }
}

fn main() -> Result<(), Error> {
    fixture_fastly::initialize_launch(env!("CARGO_BIN_NAME"));
    fastly::init();
    let request = Request::from_client();
    if request.get_path() == "/record-ready" {
        send_receipt(serde_json::json!({
            "record_ready": true,
            "provider": "fastly-native",
        }));
        return Ok(());
    }
    let path = request.get_path();
    if path != "/record-contract"
        && !path.starts_with("/record-race/")
        && !path.starts_with("/record-existence/")
        && path != "/record-cases/clean"
    {
        Response::from_status(404).send_to_client();
        return Ok(());
    }

    let layout = RecordLayout::new(FixtureLayout)?;
    let store = FastlyKvStore::open_records("fixture_records", layout)?;
    let kv = KvHandle::new(Arc::new(store));
    if path != "/record-contract" {
        send_receipt(block_on(record_cases::run(&kv, path))?);
        return Ok(());
    }
    block_on(assert_record_contract(&kv, "group/"));

    // A failed assertion traps before this receipt. A successful receipt proves
    // this suite executed, not races, expiry timing, restart or lost acknowledgements.
    let receipt = serde_json::json!({
        "record_contract": "passed",
        "provider": "fastly-native",
        "executed_suites": 1,
    });
    println!("{receipt}");
    send_receipt(receipt);
    Ok(())
}

fn send_receipt(receipt: serde_json::Value) {
    Response::from_status(200)
        .with_header(
            fixture_core::launch::HEADER,
            fixture_core::launch::value().unwrap_or("unconfigured"),
        )
        .with_header("content-type", "application/json")
        .with_body(receipt.to_string())
        .send_to_client();
}

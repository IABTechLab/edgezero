//! Run the shared record contract against the portable DynamoDB and celld providers
//! through the canonical Fastly path: the `edgezero_store_bindings` Config Store
//! document, logical Secret Store credentials and the adapter's per-request KV registry.
//! No provider is constructed here; a receipt proves the adapter selected it.

use edgezero_core::app::{App, Hooks, StoreMetadata, StoresMetadata};
use edgezero_core::body::Body;
use edgezero_core::context::RequestContext;
use edgezero_core::error::EdgeError;
use edgezero_core::http::{Response, response_builder};
use edgezero_core::key_value_store::{KvError, KvHandle};
use edgezero_core::record_store::{
    RecordKeyLayout, RecordWrite, RecordWriteMode, RecordWriteOutcome,
    contract::assert_record_contract,
};
use edgezero_core::{action, router::RouterService};

#[path = "../record_cases.rs"]
mod record_cases;

/// KV ID, receipt provider label and the record format its provider must report.
const PROVIDERS: [(&str, &str, &str); 2] = [
    (
        "dynamodb_records",
        "dynamodb-portable",
        "edgezero-dynamodb-records-v1",
    ),
    ("celld_records", "celld-portable", "celld-store-records-v1"),
];

/// The native Fastly suite's layout, so both suites exercise the same keys.
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

struct PortableRecordsApp;

impl Hooks for PortableRecordsApp {
    fn stores() -> StoresMetadata {
        StoresMetadata {
            config: None,
            kv: Some(StoreMetadata {
                default: "dynamodb_records",
                ids: &["dynamodb_records", "celld_records"],
            }),
            secrets: Some(StoreMetadata {
                default: "stores",
                ids: &["stores"],
            }),
        }
    }

    fn configure(app: &mut App) -> Result<(), EdgeError> {
        for (id, _, _) in PROVIDERS {
            app.require_record_store(id, FixtureLayout)?;
        }
        Ok(())
    }

    fn routes() -> Result<RouterService, EdgeError> {
        Ok(RouterService::builder()
            .get("/record-ready", ready)
            .get("/record-contract/{id}", contract)
            .get("/record-native/{id}", native)
            .get("/record/{id}/{*rest}", cases)
            .build())
    }
}

fn receipt(value: serde_json::Value) -> Result<Response, EdgeError> {
    response_builder()
        .header(
            fixture_core::launch::HEADER,
            fixture_core::launch::value().unwrap_or("unconfigured"),
        )
        .header("content-type", "application/json")
        .body(Body::from(value.to_string()))
        .map_err(EdgeError::internal)
}

/// The adapter-selected handle for one portable KV ID, with its provider label.
fn bound(ctx: &RequestContext) -> Result<(KvHandle, &'static str), EdgeError> {
    let id = ctx.path_params().get("id").unwrap_or_default();
    let (id, label, format) = PROVIDERS
        .into_iter()
        .find(|(candidate, _, _)| *candidate == id)
        .ok_or_else(|| EdgeError::bad_request("unknown portable KV ID"))?;
    let kv = ctx
        .kv_store(id)
        .ok_or_else(|| EdgeError::service_unavailable("portable KV handle absent"))?;
    // A Fastly KV fallback would report the native format; refuse it outright.
    let records = kv.require_records().map_err(EdgeError::from)?;
    if records.binding().format != format {
        return Err(EdgeError::service_unavailable(
            "KV ID is not served by its portable provider",
        ));
    }
    Ok((kv, label))
}

/// Read-only launch attestation. It never opens a store.
#[action]
async fn ready() -> Result<Response, EdgeError> {
    receipt(serde_json::json!({
        "record_ready": true,
        "providers": PROVIDERS.map(|(_, label, _)| label),
    }))
}

#[action]
async fn contract(ctx: RequestContext) -> Result<Response, EdgeError> {
    let (kv, label) = bound(&ctx)?;
    // A failed assertion traps before this receipt.
    assert_record_contract(&kv, "group/").await;
    let value = serde_json::json!({
        "record_contract": "passed",
        "provider": label,
        "executed_suites": 1,
    });
    println!("{value}");
    receipt(value)
}

/// Reads the row the orchestrator wrote natively, then deletes it through this provider.
#[action]
async fn native(ctx: RequestContext) -> Result<Response, EdgeError> {
    let (kv, label) = bound(&ctx)?;
    let records = kv.require_records().map_err(EdgeError::from)?;
    let seeded = records
        .lookup("native/seed")
        .await
        .map_err(EdgeError::from)?
        .ok_or_else(|| EdgeError::service_unavailable("native seed row absent"))?;
    let matched =
        seeded.body.as_ref() == b"native-seed" && seeded.metadata.as_deref() == Some("native");
    // The native generation must gate a portable CAS on the same row.
    let cas = records
        .insert(
            "native/seed",
            RecordWrite {
                body: b"portable-cas",
                metadata: None,
                ttl: None,
                mode: RecordWriteMode::IfGenerationMatch(seeded.generation),
            },
        )
        .await
        .map_err(EdgeError::from)?;
    records
        .delete("native/seed")
        .await
        .map_err(EdgeError::from)?;
    let absent = !records
        .exists_strong("native/seed")
        .await
        .map_err(EdgeError::from)?;
    receipt(serde_json::json!({
        "record_native": if matched && cas == RecordWriteOutcome::Written && absent { "passed" } else { "failed" },
        "provider": label,
        "native_body_matched": matched,
        "native_generation_cas": cas == RecordWriteOutcome::Written,
        "deleted": absent,
    }))
}

/// The native suite's race, existence and cleanup cases, against one portable ID.
#[action]
async fn cases(ctx: RequestContext) -> Result<Response, EdgeError> {
    let (kv, label) = bound(&ctx)?;
    let rest = ctx.path_params().get("rest").unwrap_or_default();
    let mut value = record_cases::run(&kv, &format!("/{rest}"))
        .await
        .map_err(EdgeError::from)?;
    value["provider"] = serde_json::Value::from(label);
    receipt(value)
}

fn main() -> Result<(), fastly::Error> {
    fixture_fastly::initialize_launch(env!("CARGO_BIN_NAME"));
    edgezero_adapter_fastly::run_app::<PortableRecordsApp>()
}

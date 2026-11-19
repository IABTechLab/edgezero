use std::collections::HashSet;

use edgezero_core::key_value_store::{KvError, KvHandle};
use edgezero_core::record_store::{
    RecordGeneration, RecordWrite, RecordWriteMode, RecordWriteOutcome,
};
use serde_json::{Value, json};

const DESCENDANTS: usize = 1001;
const EXACT: &str = "pages/needle";

fn write(body: &[u8], mode: RecordWriteMode) -> RecordWrite<'_> {
    RecordWrite {
        body,
        metadata: None,
        ttl: None,
        mode,
    }
}

fn invalid_route() -> KvError {
    KvError::Validation("invalid native record case".to_owned())
}

fn descendant(index: usize) -> String {
    format!("{EXACT}/{index:04}")
}

pub async fn run(kv: &KvHandle, path: &str) -> Result<Value, KvError> {
    let records = kv.require_records()?;
    let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
    match parts.as_slice() {
        ["record-race", "prepare"] => {
            records.delete("race/add").await?;
            records.delete("race/cas").await?;
            assert_eq!(
                records
                    .insert("race/cas", write(b"seed", RecordWriteMode::Add))
                    .await?,
                RecordWriteOutcome::Written
            );
            let generation = records.lookup("race/cas").await?.unwrap().generation;
            Ok(json!({"record_race":"prepared","generation":generation.as_u64()}))
        }
        ["record-race", mode, writer, rest @ ..] if ["a", "b"].contains(writer) => {
            let condition = match (*mode, rest) {
                ("add", []) => RecordWriteMode::Add,
                ("cas", [generation]) => RecordWriteMode::IfGenerationMatch(
                    RecordGeneration::from_u64(generation.parse().map_err(|_| invalid_route())?),
                ),
                _ => return Err(invalid_route()),
            };
            let outcome = records
                .insert(&format!("race/{mode}"), write(writer.as_bytes(), condition))
                .await?;
            Ok(
                json!({"record_race":mode,"writer":writer,"outcome":match outcome {
                    RecordWriteOutcome::Written => "written",
                    RecordWriteOutcome::PreconditionFailed => "precondition-failed",
                }}),
            )
        }
        ["record-race", "result", mode] if ["add", "cas"].contains(mode) => {
            let record = records.lookup(&format!("race/{mode}")).await?.unwrap();
            Ok(json!({"record_race":mode,"body":std::str::from_utf8(&record.body).unwrap()}))
        }
        ["record-race", "cleanup"] => {
            records.delete("race/add").await?;
            records.delete("race/cas").await?;
            Ok(json!({"record_race":"cleaned"}))
        }
        ["record-existence", operation, start, end] if ["seed", "cleanup"].contains(operation) => {
            let start: usize = start.parse().map_err(|_| invalid_route())?;
            let end: usize = end.parse().map_err(|_| invalid_route())?;
            if start >= end || end > DESCENDANTS {
                return Err(invalid_route());
            }
            for index in start..end {
                let key = descendant(index);
                if *operation == "seed" {
                    assert_eq!(
                        records
                            .insert(&key, write(b"descendant", RecordWriteMode::Add))
                            .await?,
                        RecordWriteOutcome::Written
                    );
                } else {
                    records.delete(&key).await?;
                }
            }
            Ok(json!({"record_existence":operation,"count":end-start}))
        }
        ["record-existence", "check"] => {
            let partition = records.partition("pages/")?;
            let mut cursor = None;
            let mut keys = HashSet::new();
            let mut cursors = HashSet::new();
            let mut pages = 0;
            loop {
                let page = records
                    .list_partition(&partition, cursor.as_deref(), 1000)
                    .await?;
                pages += 1;
                assert!(page.keys.len() <= 1000);
                for key in page.keys {
                    assert!(keys.insert(key));
                }
                cursor = page.cursor;
                let Some(next) = &cursor else { break };
                assert!(cursors.insert(next.clone()));
            }
            assert!(pages >= 2);
            assert_eq!(keys, (0..DESCENDANTS).map(descendant).collect());
            assert!(!records.exists_strong(EXACT).await?);
            assert_eq!(
                records
                    .insert(EXACT, write(b"exact", RecordWriteMode::Add))
                    .await?,
                RecordWriteOutcome::Written
            );
            assert!(records.exists_strong(EXACT).await?);
            records.delete(EXACT).await?;
            assert!(!records.exists_strong(EXACT).await?);
            Ok(
                json!({"record_existence":"passed","descendants":DESCENDANTS,"native_pages":pages,
                "exact_absent_before":true,"exact_present":true,"exact_absent_after":true}),
            )
        }
        ["record-cases", "clean"] => {
            // Drain the whole global listing. Some providers keep internal rows, such
            // as DynamoDB generation allocators, that yield empty pages with a cursor.
            let mut cursor = None;
            let mut cursors = HashSet::new();
            let mut pages = 0_usize;
            loop {
                let page = kv.list_keys_page("", cursor.as_deref(), 1000).await?;
                pages += 1;
                assert!(page.keys.is_empty(), "logical keys remain after cleanup");
                cursor = page.cursor;
                let Some(next) = &cursor else { break };
                assert!(cursors.insert(next.clone()), "repeated cleanup cursor");
            }
            Ok(json!({"record_cases":"clean","remaining_keys":0,"pages":pages}))
        }
        _ => Err(invalid_route()),
    }
}

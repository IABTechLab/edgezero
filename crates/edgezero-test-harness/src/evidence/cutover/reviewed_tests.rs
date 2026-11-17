use super::*;
use std::io::Read;

fn pre_separation_inventory(mut inventory: Value) -> Value {
    use super::ownership_tests::{AXUM_CORRECTNESS, AXUM_NATIVE, AXUM_QUALIFICATION};

    let previous = "scripts/run_test_nonzero.sh --include-ignored --inventory axum/native-tcp,unit/axum,axum/http1,axum,axum/http1-chunked cargo test --offline --locked -p edgezero-adapter-axum --no-default-features --features axum,test-utils --all-targets";
    let mut counts = (0, 0);
    for row in inventory["cases"].as_array_mut().unwrap() {
        if row["command"] == AXUM_NATIVE {
            row["command"] = json!(previous);
            counts.0 += 1;
        } else if row["command"] == AXUM_QUALIFICATION {
            assert_eq!(key(row), "EZ-1/acceptance/06@axum/http1-chunked");
            row["command"] = json!(previous);
            row["artifact_path"] = json!("outbound/redirect/axum/native-tests.log");
            counts.1 += 1;
        }
        if key(row) == "EZ-1/acceptance/04@axum" {
            assert_eq!(
                row.as_object_mut().unwrap().remove("additional_sentinels"),
                Some(json!([AXUM_CORRECTNESS]))
            );
        }
    }
    assert_eq!(counts, (37, 1));
    inventory
}

fn decode(encoded: &Value, inventory: &Value, source: &str) -> Value {
    match text(&encoded[0]) {
        "literal" => encoded[1].clone(),
        "source" => json!(source),
        "inventory" => {
            let mut value = inventory.clone();
            for patch in encoded[1].as_array().expect("patches") {
                let path = patch[1].as_array().expect("path");
                if path.is_empty() {
                    value = patch[2].clone();
                    continue;
                }
                let mut parent = &mut value;
                for part in &path[..path.len() - 1] {
                    parent = if parent.is_array() {
                        &mut parent[text(part).parse::<usize>().expect("index")]
                    } else {
                        &mut parent[text(part)]
                    };
                }
                let last = text(path.last().expect("leaf"));
                if patch[0] == "remove" {
                    parent.as_object_mut().expect("object").remove(last);
                } else if parent.is_array() {
                    parent[last.parse::<usize>().expect("index")] = patch[2].clone();
                } else {
                    parent[last] = patch[2].clone();
                }
            }
            value
        }
        _ => panic!("unknown reviewed input encoding"),
    }
}

#[test]
fn reviewed_catalog_scenarios_preserve_all_parser_policy_and_selection_outcomes() {
    let mut bytes = Vec::new();
    flate2::read::GzDecoder::new(&include_bytes!("reviewed_cases.json.gz")[..])
        .take(8 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(bytes.len() <= 8 * 1024 * 1024);
    let reviewed: Value = serde_json::from_slice(&bytes).unwrap();
    let root = crate::repository_root();
    let source = std::fs::read_to_string(root.join(SPEC)).unwrap();
    let inventory_bytes = std::fs::read(root.join(INVENTORY)).unwrap();
    // Keep the frozen replay on its original selectors; ownership_tests checks the live separation.
    let inventory = pre_separation_inventory(serde_json::from_slice(&inventory_bytes).unwrap());
    assert_eq!(reviewed["schema"], 1);
    assert_eq!(reviewed["source_sha256"], hash(&source));
    assert_eq!(
        reviewed["inventory_sha256"],
        "88018adef3dfd1b7342bbf465faf0826920ec8208243dd51bca8c451c54cc2cd"
    );
    assert_eq!(
        hash(&inventory_bytes),
        "60d761748e28de9bd1a4d69d43a36c3cbd45ebb1d94cf68ee87b5c95a6810a58"
    );
    assert_eq!(
        reviewed["original_test_titles"].as_array().unwrap().len(),
        140
    );
    let cases = reviewed["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 223);
    for (index, case) in cases.iter().enumerate() {
        let args = case["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| {
                decode(
                    &reviewed["values"][id.as_u64().unwrap() as usize],
                    &inventory,
                    &source,
                )
            })
            .collect::<Vec<_>>();
        let leg_set = args.get(2).and_then(Value::as_str).unwrap_or(REVIEWED_LEGS);
        let actual = match text(&case["api"]) {
            "criteria" => catalog::acceptance_criteria(text(&args[0])),
            "manifest" => catalog::validate_manifest(&args[0], text(&args[1]), leg_set),
            "coverage" => catalog::coverage(&args[0], text(&args[1]), leg_set),
            "sentinels" => catalog::sentinels(
                &args[0],
                text(&args[1]),
                args.get(2).and_then(Value::as_str),
            ),
            _ => panic!("unknown reviewed operation"),
        };
        if let Some(expected) = case.get("error") {
            assert_eq!(
                actual.as_ref().err().map(ToString::to_string).as_deref(),
                expected.as_str(),
                "scenario {index}: {} {}",
                case["api"],
                case["titles"]
            );
        } else {
            let actual = actual.unwrap_or_else(|error| {
                panic!(
                    "scenario {index}: {} {}: {error}",
                    case["api"], case["titles"]
                )
            });
            let expected = &reviewed["values"][case["result"].as_u64().unwrap() as usize];
            assert!(
                actual == decode(expected, &inventory, &source),
                "scenario {index}: {} {}: result differs",
                case["api"],
                case["titles"]
            );
        }
    }
}

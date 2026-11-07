use super::{fixtures::Fixture, *};
use std::fs;

fn set(value: &mut Value, path: &str, replacement: Value) {
    *value
        .pointer_mut(path)
        .unwrap_or_else(|| panic!("missing {path}")) = replacement;
}

fn finding(report: &Value, rule: &str) -> bool {
    report["findings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| text(&row["rule"]) == rule)
}

#[test]
fn frozen_export_verifies_independent_provenance_without_qualifying_or_mutating() {
    let fixture = Fixture::new();
    let original = fixture.input.clone();
    let record = fixture.export().unwrap();
    assert_eq!(fixture.input, original);
    assert_eq!(record["results"], original["results"]);
    assert_eq!(record["stage"], "frozen-candidate");
    assert_eq!(record["qualified"], false);
    assert_eq!(fixture.report(&record)["status"], "passed");
    assert_eq!(
        record::validate_release(&fixture.manifest, &record, &fixture.context).unwrap()["qualified"],
        false
    );
    for path in [
        "build/app.bin",
        "build/recipe.json",
        "provenance.json",
        "observations.json",
    ] {
        assert!(fixture.artifacts.path().join(path).is_file());
    }
}

#[test]
fn reconstructed_record_accepts_equivalent_decimal_json_numbers() {
    let fixture = Fixture::new();
    let mut record = fixture.export().unwrap();
    record["schema"] = json!(1.0);
    record["results"][0]["samples"] = json!(1.0);
    assert_eq!(fixture.report(&record)["status"], "passed");
}

#[test]
fn frozen_export_rejects_missing_provenance_draft_relabelling_and_unattested_changes() {
    let fixture = Fixture::new();
    for field in ["root", "receipt", "hash"] {
        let mut context = fixture.context.clone();
        match field {
            "root" => context.candidate_root = None,
            "receipt" => context.provenance_path = None,
            _ => context.provenance_sha256 = None,
        }
        assert!(
            record::export_frozen_record(&fixture.manifest, &fixture.input, &context).is_err(),
            "{field}"
        );
    }
    for (path, replacement) in [
        ("/stage", json!("pre-freeze")),
        ("/source_state", json!("working-tree")),
        ("/results/0/samples", json!(2)),
        ("/results/0/artifact_sha256", Value::Null),
    ] {
        let mut input = fixture.input.clone();
        set(&mut input, path, replacement);
        assert!(
            record::export_frozen_record(&fixture.manifest, &input, &fixture.context).is_err(),
            "{path}"
        );
    }
    for flag in ["qualification", "characterization"] {
        let mut input = fixture.input.clone();
        input[flag] = json!(flag == "characterization");
        assert!(
            record::export_frozen_record(&fixture.manifest, &input, &fixture.context).is_err(),
            "{flag}"
        );
    }
    let mut relabelled =
        record::assemble_record(&fixture.manifest, &fixture.input, &fixture.context).unwrap();
    relabelled["stage"] = json!("frozen-candidate");
    assert!(finding(&fixture.report(&relabelled), "frozen-provenance"));
}

#[test]
fn dirty_staged_hidden_or_wrong_candidate_source_never_exports() {
    let mut fixture = Fixture::new();
    for flag in ["assume-unchanged", "skip-worktree"] {
        fixture.git(&["update-index", &format!("--{flag}"), "source.rs"]);
        fs::write(
            fixture.candidate.path().join("source.rs"),
            "fn changed() {}\n",
        )
        .unwrap();
        assert!(
            fixture
                .export()
                .unwrap_err()
                .to_string()
                .contains("index flags")
        );
        fs::write(
            fixture.candidate.path().join("source.rs"),
            "fn fixture() {}\n",
        )
        .unwrap();
        fixture.git(&["update-index", &format!("--no-{flag}"), "source.rs"]);
    }
    for path in ["untracked", "source.rs"] {
        fs::write(fixture.candidate.path().join(path), "changed").unwrap();
        assert!(fixture.export().unwrap_err().to_string().contains("clean"));
        if path == "untracked" {
            fs::remove_file(fixture.candidate.path().join(path)).unwrap();
        }
    }
    fixture.git(&["add", "source.rs"]);
    assert!(fixture.export().unwrap_err().to_string().contains("clean"));
    fixture.git(&["reset", "--", "source.rs"]);
    fs::write(
        fixture.candidate.path().join("source.rs"),
        "fn fixture() {}\n",
    )
    .unwrap();
    let original = fixture.context.candidate_sha.clone();
    fixture.context.candidate_sha = "e".repeat(40);
    assert!(fixture.export().is_err());
    fixture.context.candidate_sha = original;
    fixture.context.source.push_str("altered requirements");
    assert!(fixture.export().is_err());
}

#[test]
fn resealed_capture_still_requires_each_build_and_producer_hash() {
    for (target, path, replacement, message) in [
        ("receipt", "/builds", json!([]), "build attestations"),
        (
            "receipt",
            "/builds/0/source_tree",
            json!("0".repeat(40)),
            "source-tree",
        ),
        (
            "receipt",
            "/builds/0/recipe_sha256",
            json!("0".repeat(64)),
            "recipe artifact hash",
        ),
        (
            "input",
            "/results/0/artifact_sha256",
            Value::Null,
            "producer-supplied raw hashes",
        ),
        (
            "receipt",
            "/candidate_tree",
            json!("0".repeat(40)),
            "source/capture provenance",
        ),
    ] {
        let mut fixture = Fixture::new();
        set(
            if target == "receipt" {
                &mut fixture.receipt
            } else {
                &mut fixture.input
            },
            path,
            replacement,
        );
        fixture.reseal();
        assert!(
            fixture.export().unwrap_err().to_string().contains(message),
            "{path}"
        );
    }
    let mut fixture = Fixture::new();
    let duplicate = fixture.receipt["builds"][0].clone();
    fixture.receipt["builds"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    fixture.reseal();
    assert!(fixture.export().is_err());
    let fixture = Fixture::new();
    let recipe = fixture.artifacts.path().join("build/recipe.json");
    fs::write(recipe, b"tampered recipe").unwrap();
    assert!(fixture.export().is_err());
}

#[test]
fn frozen_export_preserves_failed_pending_missing_and_empty_observations_as_blockers() {
    for statuses in [vec!["failed", "pending"], vec!["passed"], vec![]] {
        let mut fixture = Fixture::new();
        fixture.input["results"]
            .as_array_mut()
            .unwrap()
            .truncate(statuses.len());
        for (index, status) in statuses.iter().enumerate() {
            fixture.input["results"][index]["status"] = json!(status);
            fixture.input["results"][index]["samples"] = json!(index + 2);
        }
        fixture.reseal();
        let record = fixture.export().unwrap();
        assert_eq!(record["results"], fixture.input["results"]);
        assert_eq!(record["assembly"]["observed_legs"], statuses.len());
        assert_eq!(record["qualified"], false);
        let report = fixture.report(&record);
        assert_eq!(report["status"], "blocked");
        assert_eq!(
            report["passed"],
            statuses.len().min(1) * usize::from(statuses == ["passed"])
        );
    }
}

#[test]
fn release_reports_each_contract_outcome_numeric_retention_and_toolchain_violation() {
    let fixture = Fixture::new();
    let valid = fixture.export().unwrap();
    let mutations = [
        ("/candidate_sha", json!("e".repeat(40)), "candidate"),
        (
            "/results/0/candidate_sha",
            json!("e".repeat(40)),
            "candidate",
        ),
        ("/results/0/build_id", json!("e".repeat(64)), "build"),
        ("/results/0/samples", json!(0), "outcome"),
        ("/results/0/samples", json!(-1), "outcome"),
        ("/results/0/samples", json!(0.5), "outcome"),
        (
            "/results/0/samples",
            json!(9_007_199_254_740_992_u64),
            "outcome",
        ),
        ("/results/0/status", json!("pending"), "outcome"),
        ("/results/0/status", json!("failed"), "outcome"),
        ("/results/0/assertions", json!([]), "assertions"),
        ("/results/0/assertions/0/passed", json!(false), "assertions"),
        (
            "/results/0/source_digest",
            json!("e".repeat(64)),
            "contract",
        ),
        ("/results/0/sentinel", json!("different"), "contract"),
        ("/results/0/command", json!("different"), "contract"),
        ("/results/0/artifact_path", json!("different"), "contract"),
        ("/results/0/artifact_sha256", Value::Null, "artifact-digest"),
        ("/results/0/measurements", Value::Null, "measurements"),
        (
            "/results/0/measurements",
            json!([{"name":"latency","samples":1,"passed":true,"value":2,"rule":"le","bound":1}]),
            "measurement:0",
        ),
        (
            "/results/0/measurements",
            json!([{"name":"latency","samples":1,"passed":true,"value":0,"rule":"constructor","bound":1}]),
            "measurement:0",
        ),
        ("/retention/kind", json!("ci-artifact"), "retention"),
        ("/retention/expires_at", json!("2099-01-01"), "retention"),
        ("/retention/uri", Value::Null, "retention"),
        ("/runner_image", Value::Null, "runner"),
        ("/toolchain/rust", Value::Null, "toolchain:rust"),
        (
            "/toolchain/fastly_sdk",
            json!("old"),
            "toolchain:fastly_sdk",
        ),
        ("/toolchain/viceroy", Value::Null, "toolchain:viceroy"),
        ("/toolchain/spin", Value::Null, "toolchain:spin"),
        ("/toolchain/workerd", Value::Null, "toolchain:workerd"),
        (
            "/applicability/0/disposition",
            json!("deferred"),
            "applicability",
        ),
        ("/applicability", json!([]), "applicability"),
        ("/results", json!([]), "result"),
        ("/stage", json!("pre-freeze"), "stage"),
        ("/source_state", json!("working-tree"), "source"),
    ];
    for (path, replacement, rule) in mutations {
        let mut record = valid.clone();
        set(&mut record, path, replacement);
        let report = fixture.report(&record);
        assert_eq!(report["status"], "blocked", "{path}");
        assert!(finding(&report, rule), "{path}: {report}");
        assert!(record::validate_release(&fixture.manifest, &record, &fixture.context).is_err());
    }
    for field in ["results", "applicability", "builds"] {
        let mut record = valid.clone();
        let duplicate = record[field][0].clone();
        record[field].as_array_mut().unwrap().push(duplicate);
        record["results"][1]["candidate_sha"] = json!("e".repeat(40));
        let report = fixture.report(&record);
        assert!(finding(&report, &format!("{field}:duplicate")));
        assert!(report["findings"].as_array().unwrap().iter().any(|row| row["scope"] == key(&record["results"][1]) && row["rule"] == "candidate"));
        assert_eq!(report["passed"], 0);
    }
}

#[test]
fn assembly_rejects_unknown_ambiguous_or_mismatched_provenance_and_never_fills_missing_results() {
    let fixture = Fixture::new();
    for (path, replacement) in [
        ("/source_state", Value::Null),
        ("/candidate_sha", Value::Null),
        ("/results/0/candidate_sha", json!("e".repeat(40))),
        ("/builds/0/candidate_sha", json!("e".repeat(40))),
        ("/builds", Value::Null),
        ("/builds/0/build_id", json!("e".repeat(64))),
        ("/results/0/build_id", json!("e".repeat(64))),
        ("/results/0/source_digest", json!("e".repeat(64))),
        ("/results/0/environment", json!("unknown")),
        ("/results/0/artifact_sha256", json!("e".repeat(64))),
        ("/results/0/status", json!("unverified")),
        ("/results/0/samples", json!(-1)),
    ] {
        let mut input = fixture.input.clone();
        set(&mut input, path, replacement);
        assert!(
            record::assemble_record(&fixture.manifest, &input, &fixture.context).is_err(),
            "{path}"
        );
    }
    let mut input = fixture.input.clone();
    input["results"][1] = input["results"][0].clone();
    assert!(record::assemble_record(&fixture.manifest, &input, &fixture.context).is_err());
    input = fixture.input.clone();
    input["source_state"] = json!("working-tree");
    input["qualification"] = json!(false);
    input["results"].as_array_mut().unwrap().pop();
    let draft = record::assemble_record(&fixture.manifest, &input, &fixture.context).unwrap();
    assert_eq!(draft["results"].as_array().unwrap().len(), 1);
    let report = fixture.report(&draft);
    for rule in ["source", "result", "stage", "frozen-provenance"] {
        assert!(finding(&report, rule), "{report}");
    }
    assert_eq!(report["passed"], 0);
    assert_eq!(report["qualified"], false);
}

#[test]
fn report_and_release_authenticate_raw_build_recipe_and_capture_files() {
    for path in [
        "build/app.bin",
        "raw/axum.json",
        "build/recipe.json",
        "observations.json",
        "provenance.json",
    ] {
        let fixture = Fixture::new();
        let record = fixture.export().unwrap();
        let file = fixture.artifacts.path().join(path);
        let original = fs::read(&file).unwrap();
        fs::write(&file, b"corrupted").unwrap();
        let report = fixture.report(&record);
        assert_eq!(report["status"], "blocked", "{path}");
        assert!(record::validate_release(&fixture.manifest, &record, &fixture.context).is_err());
        fs::write(file, original).unwrap();
        assert_eq!(fixture.report(&record)["status"], "passed");
    }
}

#[test]
fn corrupted_same_candidate_companion_prevents_runtime_credit() {
    let fixture = Fixture::actual();
    let record = fixture.export().unwrap();
    let rows = fixture.manifest["cases"].as_array().unwrap();
    let runtime = rows
        .iter()
        .find(|row| row["case_id"] == "EZ-4/acceptance/01" && row["environment"] == "unit/fastly")
        .unwrap();
    let companion = rows
        .iter()
        .find(|row| key(row) == text(&runtime["companion_case"]))
        .unwrap();
    let file = fixture
        .artifacts
        .path()
        .join(text(&companion["artifact_path"]));
    fs::write(file, b"corrupted companion").unwrap();
    let report = fixture.report(&record);
    assert!(
        report["findings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|finding| finding["scope"] == key(runtime) && finding["rule"] == "companion")
    );
    assert_eq!(report["status"], "blocked");
    assert_eq!(report["qualified"], false);
}

#[test]
fn measurement_rules_require_finite_bounded_values_and_positive_safe_sample_counts() {
    let mut fixture = Fixture::new();
    fixture.input["results"][0]["measurements"] =
        json!([{"name":"latency","samples":1,"passed":true,"value":1,"rule":"le","bound":1}]);
    fixture.reseal();
    assert_eq!(
        fixture.report(&fixture.export().unwrap())["status"],
        "passed"
    );
    let valid = fixture.export().unwrap();
    for rule in ["le", "lt", "ge", "eq", "record"] {
        let mut record = valid.clone();
        record["results"][0]["measurements"][0]["rule"] = json!(rule);
        let report = fixture.report(&record);
        assert_eq!(finding(&report, "measurement:0"), rule == "lt", "{rule}");
    }
}

#[test]
fn frozen_cli_verifies_before_exclusive_output_and_reports_blocked_without_relabelling() {
    let mut fixture = Fixture::actual();
    fixture.input["results"].as_array_mut().unwrap().truncate(1);
    fixture.reseal();
    let output = fixture.artifacts.path().join("frozen-record.json");
    let arguments = |mode: &str, output: &std::path::Path, hash: &str| {
        let mut args = vec![
            mode.into(),
            "--candidate".into(),
            fixture.context.candidate_sha.clone(),
            "--artifacts".into(),
            fixture.artifacts.path().display().to_string(),
            "--candidate-root".into(),
            fixture.candidate.path().display().to_string(),
            "--provenance".into(),
            "provenance.json".into(),
            "--provenance-sha256".into(),
            hash.into(),
            "--inventory".into(),
            fixture
                .candidate
                .path()
                .join(INVENTORY)
                .display()
                .to_string(),
        ];
        if mode == "--export-frozen" {
            args.extend([
                "--input".into(),
                fixture
                    .artifacts
                    .path()
                    .join("observations.json")
                    .display()
                    .to_string(),
                "--output".into(),
                output.display().to_string(),
            ]);
        } else {
            args.extend(["--record".into(), output.display().to_string()]);
        }
        args
    };
    let execute =
        |mode, path, hash| cli::execute(&arguments(mode, path, hash), &crate::repository_root());
    let (exit, report) = execute(
        "--export-frozen",
        &output,
        fixture.context.provenance_sha256.as_deref().unwrap(),
    )
    .unwrap();
    assert_eq!(exit, 1);
    assert_eq!(
        serde_json::from_str::<Value>(&report).unwrap()["qualified"],
        false
    );
    let bytes = fs::read(&output).unwrap();
    let record: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(record["results"], fixture.input["results"]);
    assert_eq!(record["stage"], "frozen-candidate");
    assert!(
        execute(
            "--export-frozen",
            &output,
            fixture.context.provenance_sha256.as_deref().unwrap()
        )
        .is_err()
    );
    assert_eq!(fs::read(&output).unwrap(), bytes);
    assert_eq!(
        execute(
            "--report",
            &output,
            fixture.context.provenance_sha256.as_deref().unwrap()
        )
        .unwrap()
        .0,
        1
    );
    assert!(
        execute(
            "--release",
            &output,
            fixture.context.provenance_sha256.as_deref().unwrap()
        )
        .is_err()
    );
    let invalid = output.with_file_name("invalid.json");
    assert!(execute("--export-frozen", &invalid, &"0".repeat(64)).is_err());
    assert!(!invalid.exists());
}

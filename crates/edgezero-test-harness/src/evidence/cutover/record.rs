use super::*;
use std::collections::{HashMap, HashSet};

fn validate_build(build: &Value, candidate: &str) -> Result<()> {
    require(
        hex(&build["build_id"], 64) && build["candidate_sha"] == candidate,
        "build identity or candidate mismatch",
    )?;
    require(
        nonempty(&build["artifact_path"]),
        "build artifact path required",
    )
}

fn builds<'a>(input: &'a Value, candidate: &str) -> Result<HashMap<String, &'a Value>> {
    let rows = input["builds"]
        .as_array()
        .filter(|rows| !rows.is_empty())
        .ok_or("build identities required")?;
    let values = unique(rows, |row| text(&row["build_id"]).to_owned())?;
    for row in rows {
        validate_build(row, candidate)?;
    }
    Ok(values)
}

fn same_contract(result: &Value, row: &Value) -> bool {
    ["source_digest", "command", "sentinel", "artifact_path"]
        .iter()
        .all(|field| result[field] == row[field])
}

fn assemble_observations(
    manifest: &Value,
    input: &Value,
    context: &Context,
    allow_empty: bool,
) -> Result<Value> {
    catalog::validate_manifest(manifest, &context.source, &context.reviewed_legs)?;
    require(
        hex(&json!(context.candidate_sha), 40)
            && input["schema"].as_f64() == Some(1.0)
            && input["candidate_sha"] == context.candidate_sha,
        "candidate revision mismatch",
    )?;
    require(
        ["clean", "working-tree"].contains(&text(&input["source_state"])),
        "explicit source provenance required",
    )?;
    let root = context
        .artifacts_root
        .as_ref()
        .ok_or("artifacts directory required")?
        .canonicalize()?;
    let build_map = builds(input, &context.candidate_sha)?;
    for build in input["builds"]
        .as_array()
        .ok_or("build identities required")?
    {
        require(
            io::artifact_hash(&root, text(&build["artifact_path"]))? == text(&build["build_id"]),
            "build artifact hash mismatch",
        )?;
    }
    let rows = manifest["cases"].as_array().ok_or("invalid inventory")?;
    let results = input["results"]
        .as_array()
        .filter(|results| (allow_empty || !results.is_empty()) && results.len() <= rows.len())
        .ok_or("nonzero bounded observation list required")?;
    let cases = unique(rows, key)?;
    unique(results, key)?;
    let mut assembly =
        json!({"observed_legs":results.len(),"passed":0,"failed":0,"pending":0,"samples":0});
    let mut output = input.clone();
    let mut observed = Vec::new();
    for result in results {
        let id = key(result);
        let row = cases
            .get(&id)
            .ok_or_else(|| format!("unknown case/environment {id}"))?;
        require(
            result["candidate_sha"] == context.candidate_sha
                && build_map.contains_key(text(&result["build_id"])),
            format!("{id}: observation candidate/build mismatch"),
        )?;
        require(
            same_contract(result, row),
            format!("{id}: result contract mismatch"),
        )?;
        let samples = safe_integer(&result["samples"]);
        require(
            ["passed", "failed", "pending"].contains(&text(&result["status"])) && samples.is_some(),
            format!("{id}: invalid observation status or sample count"),
        )?;
        require(
            result["assertions"].is_array() && result["measurements"].is_array(),
            format!("{id}: assertion and measurement lists required"),
        )?;
        let hash = io::artifact_hash(&root, text(&result["artifact_path"]))?;
        require(
            result
                .get("artifact_sha256")
                .is_none_or(|value| value == &hash),
            format!("{id}: raw artifact hash mismatch"),
        )?;
        let status = text(&result["status"]);
        assembly[status] = json!(assembly[status].as_u64().ok_or("invalid outcome count")? + 1);
        let total = assembly["samples"]
            .as_u64()
            .and_then(|total| total.checked_add(samples.unwrap_or(0)))
            .filter(|total| *total <= 9_007_199_254_740_991)
            .ok_or("aggregate sample count overflow")?;
        assembly["samples"] = json!(total);
        let mut copied = result.clone();
        copied["artifact_sha256"] = json!(hash);
        observed.push(copied);
    }
    output["results"] = json!(observed);
    output["assembly"] = assembly;
    Ok(output)
}

pub(super) fn assemble_record(manifest: &Value, input: &Value, context: &Context) -> Result<Value> {
    let mut output = assemble_observations(manifest, input, context, false)?;
    output["stage"] = json!("pre-freeze");
    output["qualified"] = json!(false);
    Ok(output)
}

pub(super) fn export_frozen_record(
    manifest: &Value,
    input: &Value,
    context: &Context,
) -> Result<Value> {
    let root = context
        .artifacts_root
        .as_ref()
        .ok_or("independently reviewed frozen provenance and artifact root required")?
        .canonicalize()?;
    let reference = json!({"artifact_path":context.provenance_path,"artifact_sha256":context.provenance_sha256});
    require(
        nonempty(&reference["artifact_path"]) && hex(&reference["artifact_sha256"], 64),
        "independently reviewed frozen provenance and artifact root required",
    )?;
    require(
        input["stage"] == "frozen-capture"
            && input["source_state"] == "clean"
            && input["qualification"] != false
            && input["characterization"] != true,
        "draft or characterization observations cannot be frozen",
    )?;
    let candidate = io::verified_candidate(manifest, context)?;
    let receipt = io::verified_json(&root, &reference, "provenance")?;
    require(
        receipt["schema"].as_f64() == Some(1.0)
            && nonempty(&receipt["freeze_ref"])
            && nonempty(&receipt["capture_ref"])
            && receipt["candidate_sha"] == context.candidate_sha
            && receipt["candidate_tree"] == candidate["tree"]
            && receipt["source_digest"] == hash(&context.source)
            && receipt["inventory_sha256"] == candidate["inventorySha256"],
        "frozen source/capture provenance mismatch",
    )?;
    let captured = io::verified_json(&root, &receipt["observations"], "observation capture")?;
    require(
        json_equal(&captured, input),
        "observations differ from attested capture",
    )?;
    let build_map = builds(input, &context.candidate_sha)?;
    let attestations = receipt["builds"]
        .as_array()
        .filter(|rows| rows.len() == build_map.len())
        .ok_or("frozen build attestations required")?;
    let attestations = unique(attestations, |row| text(&row["build_id"]).to_owned())?;
    for build in input["builds"]
        .as_array()
        .ok_or("build identities required")?
    {
        let attestation = attestations
            .get(text(&build["build_id"]))
            .copied()
            .unwrap_or(&Value::Null);
        require(
            attestation["source_tree"] == candidate["tree"],
            "build source-tree attestation mismatch",
        )?;
        let recipe = io::verified_json(
            &root,
            &json!({"artifact_path":attestation["recipe_path"],"artifact_sha256":attestation["recipe_sha256"]}),
            "build recipe",
        )?;
        require(
            recipe["schema"].as_f64() == Some(1.0)
                && recipe["candidate_sha"] == context.candidate_sha
                && recipe["source_tree"] == candidate["tree"]
                && recipe["argv"].as_array().is_some_and(|args| {
                    !args.is_empty() && args.len() <= 128 && args.iter().all(nonempty)
                })
                && recipe["env"]
                    .as_object()
                    .is_some_and(|env| env.len() <= 128 && env.values().all(Value::is_string)),
            "invalid attested build recipe",
        )?;
    }
    require(
        input["results"].as_array().is_some_and(|results| {
            results
                .iter()
                .all(|result| hex(&result["artifact_sha256"], 64))
        }),
        "frozen observations require producer-supplied raw hashes",
    )?;
    let mut output = assemble_observations(manifest, input, context, true)?;
    require(
        io::verified_candidate(manifest, context)? == candidate,
        "candidate changed during export",
    )?;
    output["stage"] = json!("frozen-candidate");
    output["qualified"] = json!(false);
    output["frozen_provenance"] = reference;
    Ok(output)
}

fn measurement(value: &Value) -> Result<()> {
    require(
        nonempty(&value["name"])
            && safe_integer(&value["samples"]).is_some_and(|samples| samples > 0)
            && value["passed"] == true,
        "failed measurement or zero measurement samples",
    )?;
    let measured = value["value"]
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or("measurement needs a finite value")?;
    let rule = text(&value["rule"]);
    if rule == "record" {
        return Ok(());
    }
    require(
        ["le", "lt", "ge", "eq"].contains(&rule),
        "unsupported measurement rule",
    )?;
    let bound = value["bound"]
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or("measurement needs a finite bound")?;
    require(
        match rule {
            "le" => measured <= bound,
            "lt" => measured < bound,
            "ge" => measured >= bound,
            "eq" => measured == bound,
            _ => false,
        },
        "failed measurement bound",
    )
}

fn check(findings: &mut Vec<Value>, scope: &str, rule: &str, result: Result<()>) -> bool {
    match result {
        Ok(()) => true,
        Err(error) => {
            findings.push(json!({"scope":scope,"rule":rule,"message":error.to_string()}));
            false
        }
    }
}

fn collect<'a>(
    record: &'a Value,
    field: &str,
    findings: &mut Vec<Value>,
) -> HashMap<String, &'a Value> {
    let Some(values) = record[field].as_array() else {
        check(
            findings,
            "record",
            field,
            Err(format!("{field} map required").into()),
        );
        return HashMap::new();
    };
    let mut found = HashMap::new();
    let mut ambiguous = HashSet::new();
    for (index, value) in values.iter().enumerate() {
        if !check(
            findings,
            "record",
            &format!("{field}:{index}"),
            require(value.is_object(), format!("invalid {field} entry")),
        ) {
            continue;
        }
        let identity = if field == "builds" {
            value["build_id"]
                .as_str()
                .filter(|id| !id.trim().is_empty())
                .map(str::to_owned)
        } else {
            (nonempty(&value["case_id"]) && nonempty(&value["environment"])).then(|| key(value))
        };
        let Some(id) = identity else {
            check(
                findings,
                "record",
                &format!("{field}:{index}"),
                Err(format!("invalid {field} identity").into()),
            );
            continue;
        };
        if found.contains_key(&id) || ambiguous.contains(&id) {
            check(
                findings,
                if field == "builds" { "record" } else { &id },
                &format!("{field}:duplicate"),
                Err(format!("duplicate {id}").into()),
            );
            found.remove(&id);
            ambiguous.insert(id);
        } else {
            found.insert(id, value);
        }
    }
    found
}

fn verify_export(manifest: &Value, record: &Value, context: &Context) -> Result<()> {
    let reference = &record["frozen_provenance"];
    let root = context
        .artifacts_root
        .as_ref()
        .ok_or("independently reviewed frozen provenance required")?;
    require(
        nonempty(&json!(context.provenance_path))
            && hex(&json!(context.provenance_sha256), 64)
            && reference["artifact_path"] == json!(context.provenance_path)
            && reference["artifact_sha256"] == json!(context.provenance_sha256),
        "independently reviewed frozen provenance required",
    )?;
    let receipt = io::verified_json(root, reference, "provenance")?;
    let capture = io::verified_json(root, &receipt["observations"], "observation capture")?;
    require(
        json_equal(record, &export_frozen_record(manifest, &capture, context)?),
        "record differs from verified frozen export",
    )
}

pub(super) fn inspect_release(
    manifest: &Value,
    record: &Value,
    context: &Context,
) -> Result<Value> {
    catalog::validate_manifest(manifest, &context.source, &context.reviewed_legs)?;
    let mut findings = Vec::new();
    check(
        &mut findings,
        "record",
        "candidate",
        require(
            hex(&json!(context.candidate_sha), 40)
                && record["schema"].as_f64() == Some(1.0)
                && record["candidate_sha"] == context.candidate_sha,
            "candidate revision mismatch",
        ),
    );
    check(
        &mut findings,
        "record",
        "source",
        require(
            record["source_state"] == "clean" && record["qualification"] != false,
            "working-tree characterization cannot qualify a release",
        ),
    );
    check(
        &mut findings,
        "record",
        "runner",
        require(nonempty(&record["runner_image"]), "runner image required"),
    );
    for (tool, version) in [
        ("rust", "1.98.1"),
        ("fastly_sdk", "0.13.1"),
        ("viceroy", "0.21.0"),
        ("spin", "4.1.0"),
    ] {
        check(
            &mut findings,
            "record",
            &format!("toolchain:{tool}"),
            require(
                record["toolchain"][tool] == version,
                format!("wrong toolchain {tool}"),
            ),
        );
    }
    check(
        &mut findings,
        "record",
        "toolchain:workerd",
        require(
            nonempty(&record["toolchain"]["workerd"]),
            "workerd revision required",
        ),
    );
    let retention = &record["retention"];
    check(
        &mut findings,
        "record",
        "retention",
        require(
            ["release-asset", "repository"].contains(&text(&retention["kind"]))
                && nonempty(&retention["uri"])
                && retention.get("expires_at") == Some(&Value::Null),
            "durable non-expiring retention required while the candidate is pinned",
        ),
    );
    if record.get("comparisons") != Some(&Value::Null) {
        check(
            &mut findings,
            "record",
            "comparisons",
            require(
                nonempty(&record["comparisons"]["baseline"])
                    && record["comparisons"]["candidate"] == context.candidate_sha,
                "comparison build identities required",
            ),
        );
    }
    let results = collect(record, "results", &mut findings);
    let applicability = collect(record, "applicability", &mut findings);
    let rows = manifest["cases"].as_array().ok_or("invalid cases")?;
    let cases = unique(rows, key)?;
    let decisions = unique(
        manifest["decisions"]
            .as_array()
            .ok_or("invalid decisions")?,
        |value| text(&value["id"]).to_owned(),
    )?;
    let mut builds = collect(record, "builds", &mut findings);
    check(
        &mut findings,
        "record",
        "builds:nonempty",
        require(!builds.is_empty(), "build identities required"),
    );
    for build in record["builds"].as_array().into_iter().flatten() {
        let id = text(&build["build_id"]);
        if builds.contains_key(id)
            && !check(
                &mut findings,
                "record",
                &format!("build:{id}"),
                validate_build(build, &context.candidate_sha),
            )
        {
            builds.remove(id);
        }
    }
    if let Some(root) = &context.artifacts_root {
        let valid = check(
            &mut findings,
            "record",
            "artifacts",
            root.canonicalize().map(|_| ()).map_err(Into::into),
        );
        for build in record["builds"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|build| builds.contains_key(text(&build["build_id"])))
        {
            let result = if valid {
                io::artifact_hash(root, text(&build["artifact_path"])).and_then(|hash| {
                    require(build["build_id"] == hash, "build artifact hash mismatch")
                })
            } else {
                Err("build artifact hash mismatch".into())
            };
            check(
                &mut findings,
                "record",
                &format!("build:{}", text(&build["build_id"])),
                result,
            );
        }
    }
    for field in ["results", "applicability"] {
        for row in record[field].as_array().into_iter().flatten() {
            let id = key(row);
            if (if field == "results" {
                &results
            } else {
                &applicability
            })
            .contains_key(&id)
            {
                check(
                    &mut findings,
                    &id,
                    "known-leg",
                    require(
                        cases.contains_key(&id),
                        format!("unknown case/environment {id}"),
                    ),
                );
            }
        }
    }
    let mut passed = Vec::new();
    let mut deferred = Vec::new();
    for row in rows {
        let id = key(row);
        let start = findings.len();
        let required = match catalog::disposition(row, &decisions) {
            Ok(required) => required,
            Err(error) => {
                check(&mut findings, &id, "disposition", Err(error));
                continue;
            }
        };
        let mapping = applicability.get(&id).copied().unwrap_or(&Value::Null);
        check(
            &mut findings,
            &id,
            "applicability",
            require(
                mapping["disposition"] == required,
                format!("{id}: wrong or missing applicability"),
            ),
        );
        if required == "deferred" {
            let approved = catalog::decision_for(row, &decisions)
                .is_some_and(|decision| decision["status"] == "approved");
            check(
                &mut findings,
                &id,
                "deferral",
                require(
                    if approved {
                        mapping["approval"] == row["approval"]
                    } else {
                        mapping["condition"]
                            == row
                                .get("condition")
                                .filter(|value| !value.is_null())
                                .cloned()
                                .unwrap_or_else(|| json!(catalog::UNPROMOTED))
                    },
                    format!("{id}: unjustified applicability deferral"),
                ),
            );
            if start == findings.len() {
                deferred.push(id);
            }
            continue;
        }
        check(
            &mut findings,
            &id,
            "implementation",
            require(
                row["implementation"] == "ready",
                format!("{id}: planned command is not implemented evidence"),
            ),
        );
        if row["comparison"] == true {
            check(
                &mut findings,
                &id,
                "comparison",
                require(
                    record.get("comparisons") != Some(&Value::Null),
                    format!("{id}: baseline comparison required"),
                ),
            );
        }
        let Some(result) = results.get(&id) else {
            check(
                &mut findings,
                &id,
                "result",
                Err(format!("{id}: missing result").into()),
            );
            continue;
        };
        if row["comparison"] == true {
            check(
                &mut findings,
                &id,
                "comparison-measurements",
                require(
                    result["measurements"]
                        .as_array()
                        .is_some_and(|values| !values.is_empty()),
                    format!("{id}: comparison measurements required"),
                ),
            );
        }
        check(
            &mut findings,
            &id,
            "candidate",
            require(
                result["candidate_sha"] == context.candidate_sha,
                format!("{id}: result revision mismatch"),
            ),
        );
        check(
            &mut findings,
            &id,
            "build",
            require(
                builds.contains_key(text(&result["build_id"])),
                format!("{id}: missing or unknown result build identity"),
            ),
        );
        check(
            &mut findings,
            &id,
            "contract",
            require(
                same_contract(result, row),
                format!("{id}: result contract mismatch"),
            ),
        );
        check(
            &mut findings,
            &id,
            "outcome",
            require(
                result["status"] == "passed"
                    && safe_integer(&result["samples"]).is_some_and(|samples| samples > 0),
                format!("{id}: pending/failed result or zero samples"),
            ),
        );
        check(
            &mut findings,
            &id,
            "artifact-digest",
            require(
                hex(&result["artifact_sha256"], 64),
                format!("{id}: raw artifact hash required"),
            ),
        );
        check(
            &mut findings,
            &id,
            "assertions",
            require(
                result["assertions"].as_array().is_some_and(|assertions| {
                    !assertions.is_empty()
                        && assertions.iter().all(|assertion| {
                            nonempty(&assertion["name"]) && assertion["passed"] == true
                        })
                }),
                format!("{id}: missing or failed assertions"),
            ),
        );
        if let Some(measurements) = result["measurements"].as_array() {
            for (index, value) in measurements.iter().enumerate() {
                check(
                    &mut findings,
                    &id,
                    &format!("measurement:{index}"),
                    measurement(value),
                );
            }
        } else {
            check(
                &mut findings,
                &id,
                "measurements",
                Err(format!("{id}: measurement list required").into()),
            );
        }
        if start == findings.len() {
            passed.push(id);
        }
    }
    if let Some(root) = &context.artifacts_root {
        for result in record["results"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|result| results.contains_key(&key(result)))
        {
            check(
                &mut findings,
                &key(result),
                "raw-artifact",
                io::artifact_hash(root, text(&result["artifact_path"])).and_then(|hash| {
                    require(
                        result["artifact_sha256"] == hash,
                        "raw artifact hash mismatch",
                    )
                }),
            );
        }
    }
    let failed = findings
        .iter()
        .map(|finding| text(&finding["scope"]).to_owned())
        .collect::<HashSet<_>>();
    for row in rows {
        let id = key(row);
        if let Some(companion) = row.get("companion_case")
            && passed.contains(&id)
        {
            check(
                &mut findings,
                &id,
                "companion",
                require(
                    passed.iter().any(|value| companion == value)
                        && !failed.contains(text(companion)),
                    format!(
                        "{id}: same-candidate companion did not pass: {}",
                        text(companion)
                    ),
                ),
            );
        }
    }
    check(
        &mut findings,
        "record",
        "stage",
        require(
            record["stage"] == "frozen-candidate",
            "pre-freeze or missing-stage evidence cannot qualify a release",
        ),
    );
    check(
        &mut findings,
        "record",
        "frozen-provenance",
        verify_export(manifest, record, context),
    );
    let invalid = findings
        .iter()
        .map(|finding| text(&finding["scope"]))
        .collect::<HashSet<_>>();
    let accepted = |ids: &[String]| {
        if invalid.contains("record") {
            0
        } else {
            ids.iter()
                .filter(|id| !invalid.contains(id.as_str()))
                .count()
        }
    };
    Ok(
        json!({"status":if findings.is_empty() {"passed"} else {"blocked"},"candidate_sha":context.candidate_sha,"passed":accepted(&passed),"deferred":accepted(&deferred),"capabilities_changed":false,"qualified":false,"findings":findings}),
    )
}

pub(super) fn validate_release(
    manifest: &Value,
    record: &Value,
    context: &Context,
) -> Result<Value> {
    let report = inspect_release(manifest, record, context)?;
    let findings = report["findings"]
        .as_array()
        .ok_or("missing report findings")?;
    require(
        findings.is_empty(),
        findings
            .first()
            .map(|finding| text(&finding["message"]))
            .unwrap_or("release blocked"),
    )?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assembly_preserves_failed_pending_counts_and_verified_raw_hashes() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("app.bin"), b"abc").unwrap();
        std::fs::write(root.path().join("raw.json"), b"{}").unwrap();
        let source = "### EZ-1. Forwarding\n- **EZ-1.1** bounded\n#### Acceptance criteria\n\n1. A proof.\n\n#### Out of scope\n";
        let criterion = catalog::acceptance_criteria(source).unwrap()[0].clone();
        let row = json!({"case_id":criterion["case_id"],"source_id":criterion["case_id"],"source_digest":criterion["source_digest"],"requirement_ids":["EZ-1.1"],"environment":"axum","applicability":"blocking","approval":null,"condition":null,"owner":"EdgeZero","task":"P2","substep":"fixture","sentinel":"proof","command":"fixture proof","expected":criterion["text"],"artifact_path":"raw.json","implementation":"ready"});
        let manifest =
            json!({"schema":1,"source_digest":hash(source),"decisions":[],"cases":[row.clone()]});
        let candidate = "a".repeat(40);
        let input = json!({"schema":1,"candidate_sha":candidate,"source_state":"working-tree","qualification":false,"builds":[{"build_id":hash(b"abc"),"candidate_sha":candidate,"artifact_path":"app.bin"}],"results":[{"case_id":row["case_id"],"environment":"axum","candidate_sha":candidate,"build_id":hash(b"abc"),"source_digest":row["source_digest"],"command":row["command"],"sentinel":"proof","artifact_path":"raw.json","status":"failed","samples":3,"assertions":[{"name":"negative","passed":false}],"measurements":[]}]});
        let context = Context {
            candidate_sha: candidate,
            source: source.into(),
            reviewed_legs: hash(serde_json::to_vec(&vec![key(&row)]).unwrap()),
            artifacts_root: Some(root.path().into()),
            candidate_root: None,
            provenance_path: None,
            provenance_sha256: None,
        };
        let original = input.clone();
        let record = assemble_record(&manifest, &input, &context).unwrap();
        assert_eq!(input, original);
        assert_eq!(record["stage"], "pre-freeze");
        assert_eq!(record["qualification"], false);
        assert_eq!(record["qualified"], false);
        assert_eq!(
            record["assembly"],
            json!({"observed_legs":1,"passed":0,"failed":1,"pending":0,"samples":3})
        );
        assert_eq!(record["results"][0]["artifact_sha256"], hash(b"{}"));
    }
}

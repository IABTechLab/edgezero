use super::*;

const CASES: [&str; 6] = [
    "mixed-completed",
    "mixed-cutoff",
    "mixed-failure",
    "fresh-completed",
    "fresh-cutoff",
    "fresh-failure",
];

fn expected_paths(base: &str, completed: bool) -> Vec<String> {
    let mut paths = (0..3)
        .map(|index| format!("{base}/slot/{index}"))
        .collect::<Vec<_>>();
    paths.extend((0..if completed { 3 } else { 2 }).map(|index| format!("{base}/release/{index}")));
    paths.sort_unstable();
    paths
}

fn validate_report(case: &str, report: &Value) -> Result<()> {
    require(CASES.contains(&case), "unknown batch recovery case")?;
    let mixed = case.starts_with("mixed-");
    let completed = case.ends_with("completed");
    let failure = case.ends_with("failure");
    let terminal = if failure {
        "Failure"
    } else if completed {
        "Completed"
    } else {
        "Cutoff"
    };
    require(
        report["case"] == case
            && report["initial_pending"] == true
            && report["qualification"] == false
            && report["terminal"] == terminal
            && report["same_error"] == failure
            && report["faults"] == u64::from(failure)
            && report["rejections"]
                == json!({"foreign_receipt":mixed,"missing_receipt":mixed,"foreign_collection":mixed,"foreign_outcomes_preserved":mixed}),
        "wrong batch disposition, identity recovery or original failure",
    )?;
    let before: Vec<String> = serde_json::from_value(report["before_merge"].clone())?;
    let after: Vec<String> = serde_json::from_value(report["after_merge"].clone())?;
    let outcomes = report["outcomes"]
        .as_array()
        .ok_or("missing batch outcomes")?;
    let observations = report["observations"]
        .as_array()
        .ok_or("missing batch observations")?;
    require(
        before.len() == 3
            && after.len() == 3
            && outcomes.len() == 3
            && observations.len() == if mixed { 2 } else { 0 },
        "wrong batch population",
    )?;
    for index in 0..3 {
        let resolved = index < 2 && (mixed || !failure) || index == 2 && completed;
        let expected_before = if mixed && index < 2 {
            "AlreadyDelivered"
        } else if resolved {
            "Resolved"
        } else {
            "Unresolved"
        };
        require(
            before[index] == expected_before
                && after[index] == if resolved { "Resolved" } else { "Unresolved" },
            "batch delivery state lost",
        )?;
        if resolved {
            let value = &outcomes[index];
            let elapsed = value["elapsed_ns"]
                .as_str()
                .ok_or("missing original elapsed")?;
            require(
                value.as_object().is_some_and(|value| value.len() == 3)
                    && value["index"] == index
                    && value["bytes"] == json!([b'A' + index as u8])
                    && elapsed
                        .parse::<u128>()
                        .is_ok_and(|number| number.to_string() == elapsed),
                "merged batch outcome changed",
            )?;
        } else {
            require(
                outcomes[index].is_null(),
                "unresolved batch invented a result",
            )?;
        }
    }
    if mixed {
        for (index, via) in ["check", "next"].into_iter().enumerate() {
            require(
                observations[index]["via"] == via && observations[index]["item"] == outcomes[index],
                "observation replayed, cloned or lost its original receipt",
            )?;
        }
    }
    Ok(())
}

pub(super) fn validate_artifact(events: &[Value], adapter: &str) -> Result<()> {
    require(
        events.iter().all(|event| event["status"] != "fail"),
        "failed batch evidence",
    )?;
    let inventories = events
        .iter()
        .filter(|value| value["event"] == "batch-recovery-inventory" && value["adapter"] == adapter)
        .collect::<Vec<_>>();
    require(
        inventories.len() == 1,
        "missing or duplicate batch recovery inventory",
    )?;
    let inventory = inventories[0];
    let origin = inventory["origin_port"]
        .as_u64()
        .filter(|port| (1..=u64::from(u16::MAX)).contains(port))
        .ok_or("missing batch origin")?;
    require(
        inventory["cases"] == 6
            && inventory["origin_requests"] == 32
            && inventory["qualification"] == false
            && inventory["status"] == "pass",
        "incomplete batch recovery inventory",
    )?;
    let mut observed = Vec::new();
    for event in events
        .iter()
        .filter(|value| value["event"] == "batch-recovery-case" && value["adapter"] == adapter)
    {
        let case = event["case"].as_str().ok_or("missing batch case")?;
        let token = event["token"]
            .as_str()
            .filter(|token| {
                !token.is_empty()
                    && token.len() <= 64
                    && token
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
            .ok_or("invalid batch token")?;
        let base = format!("/p5/batch/{token}/{case}");
        require(
            event["base"] == base && event["qualification"] == false,
            "wrong batch base",
        )?;
        let receipts: Vec<(u16, String)> =
            serde_json::from_value(event["origin_requests"].clone())?;
        require(
            receipts.iter().all(|(port, _)| u64::from(*port) == origin),
            "foreign batch origin",
        )?;
        let mut paths = receipts
            .into_iter()
            .map(|(_, path)| path)
            .collect::<Vec<_>>();
        paths.sort_unstable();
        require(
            paths == expected_paths(&base, case.ends_with("completed")),
            "missing, replayed or foreign batch HTTP request",
        )?;
        require(
            event["response"]["status"] == 200
                && serde_json::from_slice::<Value>(&body(&event["response"])?)? == event["report"],
            "batch HTTP result and report disagree",
        )?;
        validate_report(case, &event["report"])?;
        observed.push(case);
    }
    observed.sort_unstable();
    let mut expected = CASES;
    expected.sort_unstable();
    require(
        observed == expected,
        "missing or duplicate batch recovery case",
    )
}

pub(super) fn run(adapter: &str, port: u16, records: &mut Records) -> Result<()> {
    let origins = WireOrigins::start()?;
    let outcome: Result<()> = (|| {
        for case in CASES {
            require(
                origins.take_requests().is_empty(),
                "late request escaped preceding batch",
            )?;
            let token = token();
            let base = format!("/p5/batch/{token}/{case}");
            let uri = format!("http://127.0.0.1:{}{base}", origins.port(0));
            for index in 0..3 {
                origins.arm(&format!("batch-{token}-{case}-{index}"));
            }
            let endpoint = if adapter == "spin" {
                "/batch-recovery".to_owned()
            } else {
                format!("/progress/batch-recovery/{case}")
            };
            let response = net::header_request(
                port,
                &endpoint,
                &token,
                &[
                    (
                        if adapter == "spin" {
                            "x-origin"
                        } else {
                            "x-fixture-backend"
                        },
                        &uri,
                    ),
                    ("x-batch-case", case),
                ],
                None,
            );
            for index in 0..3 {
                origins.release(&format!("batch-{token}-{case}-{index}"));
            }
            let response = response?;
            let receipts = origins.take_requests();
            let report: Result<Value> =
                body(&response).and_then(|bytes| Ok(serde_json::from_slice(&bytes)?));
            records.push(json!({"event":"batch-recovery-case","adapter":adapter,"case":case,"token":token,"base":base,"origin_requests":receipts,"response":response,"report":report.as_ref().ok(),"qualification":false}))?;
            require(response["status"] == 200, "batch recovery guest failed")?;
            let mut paths = receipts
                .iter()
                .map(|(_, path)| path.clone())
                .collect::<Vec<_>>();
            paths.sort_unstable();
            require(
                receipts.iter().all(|(port, _)| *port == origins.port(0))
                    && paths == expected_paths(&base, case.ends_with("completed")),
                "batch dispatch replayed, omitted or changed a request",
            )?;
            validate_report(case, &report?)?;
        }
        Ok(())
    })();
    let origin = origins.port(0);
    let cleanup = origins.finish();
    if let Err(error) = &cleanup {
        records.push(json!({"status":"fail","adapter":adapter,"stage":"batch-origin-cleanup","error":error.to_string(),"qualification":false}))?;
    }
    outcome?;
    cleanup?;
    records.push(json!({"event":"batch-recovery-inventory","adapter":adapter,"cases":6,"origin_requests":32,"origin_port":origin,"status":"pass","qualification":false}))?;
    validate_artifact(&super::super::ci::read_events(&records.path)?, adapter)?;
    println!("ez_6_acceptance_22_{adapter}_runtime: passed");
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    pub(crate) fn artifact(adapter: &str) -> Vec<Value> {
        let mut events = Vec::new();
        for case in CASES {
            let mixed = case.starts_with("mixed-");
            let completed = case.ends_with("completed");
            let failure = case.ends_with("failure");
            let mut before = Vec::new();
            let mut after = Vec::new();
            let mut outcomes = Vec::new();
            for index in 0..3 {
                let resolved = index < 2 && (mixed || !failure) || index == 2 && completed;
                before.push(if mixed && index < 2 {
                    "AlreadyDelivered"
                } else if resolved {
                    "Resolved"
                } else {
                    "Unresolved"
                });
                after.push(if resolved { "Resolved" } else { "Unresolved" });
                outcomes.push(if resolved {json!({"index":index,"bytes":[b'A'+index as u8],"elapsed_ns":(index+1).to_string()})} else {Value::Null});
            }
            let report = json!({"case":case,"initial_pending":true,"before_merge":before,"after_merge":after,"outcomes":outcomes,"observations":if mixed {vec![json!({"via":"check","item":outcomes[0]}),json!({"via":"next","item":outcomes[1]})]} else {Vec::new()},"terminal":if failure {"Failure"}else if completed {"Completed"}else{"Cutoff"},"same_error":failure,"faults":u64::from(failure),"rejections":{"foreign_receipt":mixed,"missing_receipt":mixed,"foreign_collection":mixed,"foreign_outcomes_preserved":mixed},"qualification":false});
            let token = "owned";
            let base = format!("/p5/batch/{token}/{case}");
            let receipts = expected_paths(&base, completed)
                .into_iter()
                .map(|path| (1, path))
                .collect::<Vec<_>>();
            events.push(json!({"event":"batch-recovery-case","adapter":adapter,"case":case,"token":token,"base":base,"origin_requests":receipts,"report":report,"response":{"status":200,"body_bytes":serde_json::to_vec(&report).unwrap()},"qualification":false}));
        }
        events.push(json!({"event":"batch-recovery-inventory","adapter":adapter,"cases":6,"origin_requests":32,"origin_port":1,"status":"pass","qualification":false}));
        events
    }

    #[test]
    fn batch_recovery_evidence_requires_exact_cases_receipts_and_unmodified_results() {
        for adapter in ["axum", "fastly", "cloudflare", "spin"] {
            let valid = artifact(adapter);
            validate_artifact(&valid, adapter).unwrap();
            for missing in 0..valid.len() {
                let mut changed = valid.clone();
                changed.remove(missing);
                assert!(validate_artifact(&changed, adapter).is_err());
            }
            for (pointer, value) in [
                ("/report/before_merge/0", json!("Unresolved")),
                ("/report/after_merge/0", json!("AlreadyDelivered")),
                ("/report/outcomes/0/bytes", json!([66])),
                ("/report/observations/0/via", json!("next")),
                ("/report/rejections/foreign_receipt", json!(false)),
                (
                    "/report/rejections/foreign_outcomes_preserved",
                    json!(false),
                ),
                ("/report/initial_pending", json!(false)),
                ("/origin_requests/0/0", json!(2)),
                ("/response/status", json!(500)),
            ] {
                let mut changed = valid.clone();
                *changed[0].pointer_mut(pointer).unwrap() = value;
                if pointer.starts_with("/report/") {
                    changed[0]["response"]["body_bytes"] =
                        json!(serde_json::to_vec(&changed[0]["report"]).unwrap());
                }
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "accepted {pointer}"
                );
            }
            let mut changed = valid.clone();
            changed[2]["report"]["same_error"] = json!(false);
            changed[2]["response"]["body_bytes"] =
                json!(serde_json::to_vec(&changed[2]["report"]).unwrap());
            assert!(validate_artifact(&changed, adapter).is_err());
            let mut changed = valid.clone();
            changed[0] = changed[1].clone();
            assert!(validate_artifact(&changed, adapter).is_err());
            let mut changed = valid.clone();
            changed.push(json!({"status":"fail","adapter":adapter,"error":"fixture failure"}));
            assert!(validate_artifact(&changed, adapter).is_err());
            assert!(validate_artifact(&valid, "foreign").is_err());
        }
    }
}

use super::*;
use crate::net::overflow::{self as wire, UploadOrigin};

pub(super) fn run(name: &str, parent: &Path, records: &mut Records) -> Result<()> {
    let first_record = records.values.len();
    let run = parent.join(format!("overflow-{name}"));
    fs::create_dir_all(&run)?;
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(fixture_root())
        .output()?;
    require(
        revision.status.success(),
        "missing overflow source revision",
    )?;
    let revision = String::from_utf8(revision.stdout)?.trim().to_owned();
    let Some((mut command, port)) =
        progress::build(name, &run, records, &revision, "outbound-overflow")?
    else {
        return Ok(());
    };
    let log = run.join("runtime.log");
    let outcome = Process::start(&mut command, &log, port).and_then(|mut process| {
        let checked = checks(name, port, records);
        let panic = fs::read_to_string(&log)?.contains("worker failed: worker panicked");
        records.push(json!({"event":"overflow-before-shutdown","adapter":name,"worker_panicked":panic,"qualification":false}))?;
        let cleanup = process.finish();
        records.push(json!({"event":"overflow-cleanup","adapter":name,"status":if cleanup.is_ok(){"pass"}else{"fail"},
            "error":cleanup.as_ref().err().map(ToString::to_string),"qualification":false}))?;
        checked.and(require(!panic, "runtime worker panicked during overflow probes")).and(cleanup)
    });
    if name == "spin" {
        for file in ["fixture_stdout.txt", "fixture_stderr.txt"] {
            for event in logs(&run.join(".spin/logs").join(file)) {
                records.push(annotate(
                    event,
                    json!({"adapter":name,"diagnostic_source":file}),
                ))?;
            }
        }
    }
    settle(
        records,
        &log,
        json!({"adapter":name,"suite":"outbound-overflow"}),
        json!({"phase":"outbound-overflow"}),
        outcome,
    )?;
    let persisted = ci::read_events(&records.path)?;
    validate_artifact(
        persisted
            .get(first_record..)
            .ok_or("overflow artifact was truncated")?,
        name,
    )?;
    let environment = match name {
        "axum" => "axum",
        "fastly" => "viceroy",
        "cloudflare" => "workerd",
        "spin" => "spin",
        _ => return Err("unsupported overflow adapter".into()),
    };
    if name != "axum" {
        println!("ez_10_acceptance_03_{environment}_http1_chunked: passed");
    }
    for criterion in ["05", "06"] {
        println!("ez_10_acceptance_{criterion}_{environment}: passed");
    }
    Ok(())
}

fn validate_origin(snapshot: &Value, adapter: &str, token: &str, case: &str) -> Result<()> {
    require(
        snapshot["collection_complete"] == true && snapshot["overflow"] == false,
        "origin collection incomplete or exceeded connection bound",
    )?;
    let connections = snapshot["connections"]
        .as_array()
        .ok_or("missing origin receipts")?;
    require(
        snapshot["accepted"].as_u64() == Some(connections.len() as u64),
        "origin accepted/terminal count mismatch",
    )?;
    let over = case.ends_with("-9");
    if over && (case.starts_with("buffered") || matches!(adapter, "axum" | "cloudflare")) {
        return require(
            connections.is_empty(),
            "over-cap upload contacted the origin before rejection",
        );
    }
    if over && connections.is_empty() {
        return Ok(());
    }
    require(
        connections.len() == 1,
        "missing or duplicate upload origin request",
    )?;
    let receipt = &connections[0];
    if over && receipt["head"].is_null() {
        return require(
            receipt.get("head") == Some(&Value::Null)
                && receipt["head_state"] == "absent"
                && receipt["head_bytes"] == 0
                && receipt["body_bytes"] == 0
                && receipt["body_prefix"] == json!([])
                && matches!(receipt["terminal"].as_str(), Some("eof" | "reset")),
            "headless contact lacks an explicit zero-byte abort witness",
        );
    }
    require(
        receipt["head_state"] == "complete"
            && receipt["head_bytes"]
                .as_u64()
                .is_some_and(|bytes| bytes > 0 && bytes <= 16 * 1024)
            && receipt["head"]["method"] == "POST"
            && receipt["head"]["target"] == format!("/overflow/{token}/{case}"),
        "foreign or headless upload origin receipt",
    )?;
    let length = receipt["body_bytes"]
        .as_u64()
        .ok_or("missing upload byte count")?;
    let bytes = receipt["body_prefix"]
        .as_array()
        .ok_or("missing bounded upload bytes")?;
    require(
        length == bytes.len() as u64
            && bytes
                .iter()
                .all(|byte| byte.as_u64() == Some(u64::from(b'x'))),
        "upload content/count mismatch",
    )?;
    if over {
        return require(
            length <= 8 && matches!(receipt["terminal"].as_str(), Some("eof" | "reset")),
            "streamed overflow completed a truncated request or lacks abort evidence",
        );
    }
    let expected = if case.ends_with("-7") { 7 } else { 8 };
    require(
        receipt["terminal"] == "complete" && length == expected && receipt["reply_written"] == true,
        "valid upload did not complete exactly",
    )
}

fn validate_response(reply: &Value, case: &str, len: usize) -> Result<()> {
    let overflow = (case == "inbound" && len > 1024) || case.ends_with("-9");
    let expected = if overflow { 413 } else { 200 };
    require(
        reply["status"] == expected,
        "overflow probe status mismatch",
    )?;
    let headers = reply["headers"]
        .as_array()
        .ok_or("missing response headers")?;
    require(
        !headers.iter().any(|field| {
            field[0]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("retry-after"))
        }),
        "generated overflow/probe response gained Retry-After",
    )?;
    let report: Value =
        serde_json::from_str(reply["body"].as_str().ok_or("missing response body")?)?;
    if overflow {
        return require(
            report
                == json!({"error":{"status":413,"kind":"payload_too_large","message":"request body too large"}}),
            "generated overflow lost its fixed public category/body",
        );
    }
    if case == "inbound" {
        return require(
            report["case"] == case && report["received"] == len,
            "inbound cap boundary byte count mismatch",
        );
    }
    require(
        report["case"] == case
            && report["outcome"] == "response"
            && report["upstream_status"] == if case == "upstream-413" { 413 } else { 200 },
        "upstream status was not an ordinary response",
    )?;
    require(
        if case == "upstream-413" {
            report["retry_after"] == "120"
        } else {
            report["retry_after"].is_null()
        },
        "ordinary upstream Retry-After changed",
    )
}

fn validate_artifact(events: &[Value], adapter: &str) -> Result<()> {
    require(
        matches!(adapter, "axum" | "fastly" | "cloudflare" | "spin")
            && events.iter().all(|event| {
                !matches!(
                    event["status"].as_str(),
                    Some("fail" | "unverified" | "unsupported")
                )
            }),
        "failed or unsupported overflow evidence",
    )?;
    let only = |kind: &str| -> Result<&Value> {
        let mut selected = events.iter().filter(|event| event["event"] == kind);
        let event = selected.next().ok_or("missing overflow lifecycle record")?;
        require(
            selected.next().is_none(),
            "duplicate overflow lifecycle record",
        )?;
        require(
            event["adapter"] == adapter && event["qualification"] == false,
            "foreign overflow lifecycle record",
        )?;
        Ok(event)
    };
    let coverage = only("overflow-coverage")?;
    require(
        coverage["status"] == "pass" && coverage["attempted"] == 11,
        "incomplete overflow coverage",
    )?;
    require(
        only("overflow-before-shutdown")?["worker_panicked"] == false,
        "overflow runtime panicked",
    )?;
    let cleanup = only("overflow-cleanup")?;
    require(
        cleanup["status"] == "pass" && cleanup.get("error") == Some(&Value::Null),
        "overflow runtime cleanup failed",
    )?;
    let startup = only("startup-identity")?;
    require(
        startup["status"] == "pass"
            && startup.get("reason") == Some(&Value::Null)
            && startup["suite"] == "outbound-overflow"
            && startup["owned_child"].as_u64().is_some_and(|id| id > 0)
            && startup["port"]
                .as_u64()
                .is_some_and(|port| (1..=u64::from(u16::MAX)).contains(&port)),
        "overflow runtime was not attested",
    )?;
    let identity = startup["launch_identity"]
        .as_str()
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .ok_or("missing overflow launch identity")?;
    let cases: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "overflow-case")
        .collect();
    let expected = [
        "inbound-1023",
        "inbound-1024",
        "inbound-1025",
        "buffered-7",
        "buffered-8",
        "buffered-9",
        "streamed-7",
        "streamed-8",
        "streamed-9",
        "upstream-413",
        "inbound-2048",
    ];
    require(
        cases.len() == expected.len(),
        "missing or duplicate overflow case",
    )?;
    let clients: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "client_completed")
        .collect();
    require(
        clients.len() == expected.len(),
        "missing or duplicate settled overflow client",
    )?;
    let mut tokens = HashSet::new();
    for (event, expected_case) in cases.into_iter().zip(expected) {
        let token = event["token"]
            .as_str()
            .filter(|value| {
                !value.is_empty()
                    && value.len() <= 64
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
            .ok_or("invalid overflow correlation token")?;
        require(tokens.insert(token), "duplicate overflow correlation token")?;
        require(
            event["case"] == expected_case
                && event["adapter"] == adapter
                && event["status"] == "pass"
                && event["qualification"] == false
                && event.get("error") == Some(&Value::Null),
            "changed overflow case or order",
        )?;
        let client = &event["client"];
        require(
            client["token"] == token && client["event"] == "client_completed",
            "foreign overflow client",
        )?;
        let inbound = expected_case.strip_prefix("inbound-");
        let len = inbound.map(str::parse::<usize>).transpose()?.unwrap_or(0);
        validate_response(
            client,
            if inbound.is_some() {
                "inbound"
            } else {
                expected_case
            },
            len,
        )?;
        if inbound.is_none() {
            require(
                event.get("origin_collection_error") == Some(&Value::Null),
                "overflow origin collection failed",
            )?;
            validate_origin(&event["origin"], adapter, token, expected_case)?;
        }
        let headers = client["headers"]
            .as_array()
            .ok_or("missing overflow client headers")?;
        require(
            headers.len() <= 100
                && headers.iter().all(|field| {
                    field.as_array().is_some_and(|pair| {
                        pair.len() == 2 && pair[0].is_string() && pair[1].is_string()
                    })
                }),
            "invalid overflow client fields",
        )?;
        let identities: Vec<_> = headers
            .iter()
            .filter(|field| {
                field[0]
                    .as_str()
                    .is_some_and(|name| name.eq_ignore_ascii_case("x-edgezero-fixture-launch"))
            })
            .collect();
        require(
            if identities.is_empty() {
                client["status"] == 413 && client["response_identity"] == "unattested"
            } else {
                identities.len() == 1
                    && identities[0][1] == identity
                    && client["response_identity"] == "matched-launch"
            },
            "overflow response launch identity changed",
        )?;
        let matched: Vec<_> = clients
            .iter()
            .filter(|entry| entry["token"] == token)
            .collect();
        let [settled] = matched.as_slice() else {
            return Err("missing or duplicate overflow client correlation".into());
        };
        require(
            settled["adapter"] == adapter
                && [
                    "event",
                    "token",
                    "status",
                    "headers",
                    "body",
                    "header_ns",
                    "complete_ns",
                    "response_identity",
                ]
                .iter()
                .all(|field| settled[*field] == client[*field]),
            "settled overflow capture changed",
        )?;
    }
    Ok(())
}

fn check_inbound(adapter: &str, port: u16, records: &mut Records, len: usize) -> Result<bool> {
    let token = token();
    let response = wire::request(port, "inbound", &token, None, Some(len));
    let validation = response
        .as_ref()
        .map_err(|error| error.to_string().into())
        .and_then(|reply| validate_response(reply, "inbound", len));
    records.push(json!({"event":"overflow-case","adapter":adapter,"case":format!("inbound-{len}"),"token":token,
        "client":response.as_ref().ok(),"status":if validation.is_ok(){"pass"}else{"fail"},
        "error":validation.as_ref().err().map(ToString::to_string),"qualification":false}))?;
    Ok(validation.is_ok())
}

fn checks(adapter: &str, port: u16, records: &mut Records) -> Result<()> {
    let mut failed = false;
    for len in [1023_usize, 1024, 1025] {
        failed |= !check_inbound(adapter, port, records, len)?;
    }
    for case in [
        "buffered-7",
        "buffered-8",
        "buffered-9",
        "streamed-7",
        "streamed-8",
        "streamed-9",
        "upstream-413",
    ] {
        let token = token();
        let origin = UploadOrigin::start()?;
        let response = wire::request(port, case, &token, Some(&origin.url(&token, case)), None);
        // Always close and join the origin collector, including failed client operations.
        let snapshot = origin.finish();
        let validation = (|| -> Result<()> {
            let reply = response.as_ref().map_err(|error| error.to_string())?;
            let receipt = snapshot.as_ref().map_err(|error| error.to_string())?;
            validate_response(reply, case, 0)?;
            validate_origin(receipt, adapter, &token, case)
        })();
        records.push(json!({"event":"overflow-case","adapter":adapter,"case":case,"token":token,
            "client":response.as_ref().ok(),"origin":snapshot.as_ref().ok(),
            "origin_collection_error":snapshot.as_ref().err().map(ToString::to_string),
            "status":if validation.is_ok(){"pass"}else{"fail"},"error":validation.as_ref().err().map(ToString::to_string),"qualification":false}))?;
        failed |= validation.is_err();
    }
    // Preserve the recorded overflow-to-upload recovery sequence before this exact EZ-10 case.
    failed |= !check_inbound(adapter, port, records, 2048)?;
    records.push(
        json!({"event":"overflow-coverage","adapter":adapter,"attempted":11,"qualification":false,
        "status":if failed{"fail"}else{"pass"}}),
    )?;
    require(!failed, "one or more overflow probes failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact(adapter: &str) -> Vec<Value> {
        let identity = "v1;nonce;build;fixture-retained";
        let mut events = Vec::new();
        for (index, case) in [
            "inbound-1023",
            "inbound-1024",
            "inbound-1025",
            "buffered-7",
            "buffered-8",
            "buffered-9",
            "streamed-7",
            "streamed-8",
            "streamed-9",
            "upstream-413",
            "inbound-2048",
        ]
        .into_iter()
        .enumerate()
        {
            let token = format!("token-{index}");
            let inbound = case.starts_with("inbound-");
            let len = if inbound {
                case.strip_prefix("inbound-")
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
            } else if case.ends_with("-7") {
                7
            } else if case.ends_with("-9") {
                9
            } else {
                8
            };
            let over = if inbound { len > 1024 } else { len > 8 };
            let report = if over {
                json!({"error":{"status":413,"kind":"payload_too_large","message":"request body too large"}})
            } else if inbound {
                json!({"case":"inbound","received":len,"qualification":false})
            } else {
                json!({"case":case,"outcome":"response","upstream_status":if case=="upstream-413"{413}else{200},
                    "retry_after":if case=="upstream-413"{Some("120")}else{None},"qualification":false})
            };
            let client = json!({"event":"client_completed","token":token,"status":if over{413}else{200},
                "headers":[["x-edgezero-fixture-launch",identity]],"body":serde_json::to_string(&report).unwrap(),
                "header_ns":1,"complete_ns":2,"response_identity":"matched-launch"});
            let mut event = json!({"event":"overflow-case","adapter":adapter,"case":case,"token":token,
                "client":client,"status":"pass","error":null,"qualification":false});
            if !inbound {
                let connections = if over
                    && (case.starts_with("buffered") || matches!(adapter, "axum" | "cloudflare"))
                {
                    Vec::new()
                } else {
                    vec![
                        json!({"head":{"method":"POST","target":format!("/overflow/{token}/{case}")},
                        "head_state":"complete","head_bytes":64,"terminal":if over{"eof"}else{"complete"},
                        "body_bytes":if over{4}else{len},"body_prefix":vec![b'x';if over{4}else{len}],"reply_written":!over}),
                    ]
                };
                event["origin"] = snapshot(connections);
                event["origin_collection_error"] = Value::Null;
            }
            events.push(event);
            events.push(annotate(client, json!({"adapter":adapter})));
        }
        events.push(json!({"event":"overflow-coverage","adapter":adapter,"attempted":11,"status":"pass","qualification":false}));
        events.push(json!({"event":"overflow-before-shutdown","adapter":adapter,"worker_panicked":false,"qualification":false}));
        events.push(json!({"event":"overflow-cleanup","adapter":adapter,"status":"pass","error":null,"qualification":false}));
        events.push(json!({"event":"startup-identity","adapter":adapter,"suite":"outbound-overflow",
            "launch_identity":identity,"owned_child":1,"port":1234,"status":"pass","reason":null,"qualification":false}));
        events
    }

    #[test]
    fn persisted_overflow_requires_every_ordered_case_and_settled_owner() {
        for adapter in ["axum", "cloudflare", "fastly", "spin"] {
            let events = artifact(adapter);
            validate_artifact(&events, adapter).unwrap();
            for index in 0..events.len() {
                let mut missing = events.clone();
                missing.remove(index);
                assert!(
                    validate_artifact(&missing, adapter).is_err(),
                    "{adapter}: missing {index}"
                );
                let mut duplicate = events.clone();
                duplicate.push(events[index].clone());
                assert!(
                    validate_artifact(&duplicate, adapter).is_err(),
                    "{adapter}: duplicate {index}"
                );
            }
            let mut reordered = events.clone();
            reordered.swap(0, 2);
            assert!(validate_artifact(&reordered, adapter).is_err());
            for (event, field, value) in [
                ("overflow-case", "adapter", json!("foreign")),
                ("overflow-case", "token", json!("foreign")),
                ("overflow-case", "status", json!("fail")),
                ("overflow-case", "qualification", json!(true)),
                ("overflow-case", "error", json!("capture failed")),
                ("overflow-coverage", "attempted", json!(10)),
                ("overflow-before-shutdown", "worker_panicked", json!(true)),
                ("overflow-cleanup", "status", json!("fail")),
                ("overflow-cleanup", "error", json!("join failed")),
                ("startup-identity", "status", json!("unverified")),
                ("startup-identity", "owned_child", json!(0)),
                ("startup-identity", "port", json!(0)),
                ("startup-identity", "launch_identity", json!("foreign")),
            ] {
                let mut changed = events.clone();
                let row = changed
                    .iter_mut()
                    .find(|row| row["event"] == event)
                    .unwrap();
                row[field] = value;
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "{adapter}: {event} {field}"
                );
            }
        }
    }

    #[test]
    fn persisted_overflow_rechecks_wire_facts_and_origin_release() {
        for adapter in ["axum", "cloudflare", "fastly", "spin"] {
            let events = artifact(adapter);
            for mutation in [
                "status",
                "body",
                "retry",
                "identity",
                "client",
                "collection",
                "contact",
                "terminal",
                "origin-error",
            ] {
                let mut changed = events.clone();
                let row = changed
                    .iter_mut()
                    .find(|row| row["event"] == "overflow-case" && row["case"] == "buffered-9")
                    .unwrap();
                match mutation {
                    "status" => row["client"]["status"] = json!(200),
                    "body" => row["client"]["body"] = json!(json!({}).to_string()),
                    "retry" => row["client"]["headers"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!(["retry-after", "60"])),
                    "identity" => row["client"]["headers"][0][1] = json!("foreign"),
                    "client" => row["client"]["token"] = json!("foreign"),
                    "collection" => row["origin"]["collection_complete"] = json!(false),
                    "contact" => row["origin"] = snapshot(vec![receipt("eof", 4)]),
                    "terminal" => row["origin"]["overflow"] = json!(true),
                    _ => row["origin_collection_error"] = json!("join failed"),
                }
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "{adapter}: {mutation}"
                );
            }
        }
    }

    #[test]
    fn persisted_overflow_retains_generated_error_identity_and_stream_abort_rules() {
        for adapter in ["axum", "cloudflare", "fastly", "spin"] {
            let mut events = artifact(adapter);
            for event in &mut events {
                let client = if event["event"] == "overflow-case" {
                    &mut event["client"]
                } else {
                    event
                };
                if client["status"] == 413 {
                    client["headers"] = json!([]);
                    client["response_identity"] = json!("unattested");
                }
            }
            validate_artifact(&events, adapter).unwrap();
            let mut missing_first = events.clone();
            for event in &mut missing_first {
                let client = if event["event"] == "overflow-case" {
                    &mut event["client"]
                } else {
                    event
                };
                if client["token"] == "token-0" {
                    client["headers"] = json!([]);
                    client["response_identity"] = json!("unattested");
                }
            }
            assert!(validate_artifact(&missing_first, adapter).is_err());
            let mut completed = events.clone();
            let row = completed
                .iter_mut()
                .find(|row| row["event"] == "overflow-case" && row["case"] == "streamed-9")
                .unwrap();
            let mut receipt = receipt("complete", 4);
            receipt["head"]["target"] = json!(format!(
                "/overflow/{}/streamed-9",
                row["token"].as_str().unwrap()
            ));
            row["origin"] = snapshot(vec![receipt]);
            assert!(validate_artifact(&completed, adapter).is_err());
        }
    }

    #[test]
    fn two_kib_ingress_requires_fixed_overflow_without_retry_after() {
        let reply = json!({"status":413,"headers":[],"body":serde_json::to_string(&json!({
            "error":{"status":413,"kind":"payload_too_large","message":"request body too large"}
        })).unwrap()});
        validate_response(&reply, "inbound", 2048).unwrap();
        let mut invalid = reply.clone();
        invalid["status"] = json!(200);
        assert!(validate_response(&invalid, "inbound", 2048).is_err());
        invalid = reply.clone();
        invalid["headers"] = json!([["ReTrY-AfTeR", "120"]]);
        assert!(validate_response(&invalid, "inbound", 2048).is_err());
        invalid = reply;
        invalid["body"] = json!(
            serde_json::to_string(&json!({"error":{"status":413,
            "kind":"payload_too_large","message":"provider diagnostic"}}))
            .unwrap()
        );
        assert!(validate_response(&invalid, "inbound", 2048).is_err());
    }

    fn receipt(terminal: &str, length: usize) -> Value {
        json!({"head":{"method":"POST","target":"/overflow/test/streamed-9"},"head_state":"complete","head_bytes":64,
            "terminal":terminal,"body_bytes":length,"body_prefix":vec![b'x';length]})
    }

    fn snapshot(receipts: Vec<Value>) -> Value {
        json!({"accepted":receipts.len(),"connections":receipts,"collection_complete":true,"overflow":false})
    }

    #[test]
    fn parsed_malformed_metadata_never_passes_as_an_upload_abort() {
        for tail in ["4\n", "4\r\nxxxxX", "Z", "4\r\nxxxx\r\n0\r\n:bad\r\n"] {
            let raw = format!(
                "POST /overflow/test/streamed-9 HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n{tail}"
            );
            let receipt = wire::inspect(&mut std::io::Cursor::new(raw));
            assert!(
                validate_origin(&snapshot(vec![receipt]), "fastly", "test", "streamed-9").is_err()
            );
        }
        let raw = "POST /overflow/test/streamed-9 HTTP/1.1\r\nX-Bad: a\0b\r\nContent-Length: 9\r\n\r\nxxxx";
        let receipt = wire::inspect(&mut std::io::Cursor::new(raw));
        assert!(validate_origin(&snapshot(vec![receipt]), "fastly", "test", "streamed-9").is_err());
        for raw in [
            "POST /overflow/test/streamed-9 HTTP/1.1\r\nContent-Length: 0\r\n\r\n",
            "POST /overflow/test/streamed-9 HTTP/1.1\r\n\r\n",
            "POST /overflow/test/streamed-9 HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        ] {
            let receipt = wire::inspect(&mut std::io::Cursor::new(raw));
            assert!(
                validate_origin(&snapshot(vec![receipt]), "fastly", "test", "streamed-9").is_err()
            );
        }
    }

    #[test]
    fn full_head_abort_requires_complete_numeric_head_evidence() {
        let raw = "POST /overflow/test/streamed-9 HTTP/1.1\r\nContent-Length: 9\r\n\r\nxxxx";
        let receipt = wire::inspect(&mut std::io::Cursor::new(raw));
        validate_origin(
            &snapshot(vec![receipt.clone()]),
            "fastly",
            "test",
            "streamed-9",
        )
        .unwrap();
        for field in ["head_state", "head_bytes"] {
            let mut missing = receipt.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                validate_origin(&snapshot(vec![missing]), "fastly", "test", "streamed-9").is_err()
            );
        }
        for (field, value) in [
            ("head_state", json!("partial")),
            ("head_state", json!("absent")),
            ("head_bytes", json!(0)),
            ("head_bytes", json!("64")),
        ] {
            let mut invalid = receipt.clone();
            invalid[field] = value;
            assert!(
                validate_origin(&snapshot(vec![invalid]), "fastly", "test", "streamed-9").is_err()
            );
        }
    }

    #[test]
    fn only_correlated_incomplete_prefixes_prove_streamed_upload_abort() {
        for adapter in ["fastly", "spin"] {
            for terminal in ["eof", "reset"] {
                validate_origin(
                    &snapshot(vec![receipt(terminal, 4)]),
                    adapter,
                    "test",
                    "streamed-9",
                )
                .unwrap();
            }
            for terminal in [
                "complete",
                "timeout",
                "io_error",
                "invalid_framing",
                "capture_limit",
                "reading",
            ] {
                assert!(
                    validate_origin(
                        &snapshot(vec![receipt(terminal, 4)]),
                        adapter,
                        "test",
                        "streamed-9"
                    )
                    .is_err()
                );
            }
            assert!(
                validate_origin(
                    &snapshot(vec![receipt("eof", 9)]),
                    adapter,
                    "test",
                    "streamed-9"
                )
                .is_err()
            );
            let duplicate = receipt("eof", 4);
            assert!(
                validate_origin(
                    &snapshot(vec![duplicate.clone(), duplicate]),
                    adapter,
                    "test",
                    "streamed-9"
                )
                .is_err()
            );
            let mut foreign = receipt("eof", 4);
            foreign["head"]["target"] = json!("/overflow/other/streamed-9");
            assert!(
                validate_origin(&snapshot(vec![foreign]), adapter, "test", "streamed-9").is_err()
            );
            let mut headless = receipt("eof", 4);
            headless["head"] = Value::Null;
            assert!(
                validate_origin(&snapshot(vec![headless]), adapter, "test", "streamed-9").is_err()
            );
            let empty_contact = json!({"head":null,"head_state":"absent","head_bytes":0,
                "terminal":"eof","body_bytes":0,"body_prefix":[]});
            validate_origin(
                &snapshot(vec![empty_contact.clone()]),
                adapter,
                "test",
                "streamed-9",
            )
            .unwrap();
            let mut partial = empty_contact.clone();
            partial["head_state"] = json!("partial");
            partial["head_bytes"] = json!(5);
            assert!(
                validate_origin(&snapshot(vec![partial]), adapter, "test", "streamed-9").is_err()
            );
            let mut unobserved = empty_contact;
            unobserved.as_object_mut().unwrap().remove("head_state");
            assert!(
                validate_origin(&snapshot(vec![unobserved]), adapter, "test", "streamed-9")
                    .is_err()
            );
        }
        for adapter in ["axum", "cloudflare"] {
            assert!(
                validate_origin(
                    &snapshot(vec![receipt("eof", 4)]),
                    adapter,
                    "test",
                    "streamed-9"
                )
                .is_err()
            );
            validate_origin(&snapshot(vec![]), adapter, "test", "streamed-9").unwrap();
        }
        let mut missing = snapshot(vec![]);
        missing["accepted"] = json!(1);
        assert!(validate_origin(&missing, "fastly", "test", "streamed-9").is_err());
        missing["accepted"] = json!(0);
        missing["collection_complete"] = json!(false);
        assert!(validate_origin(&missing, "fastly", "test", "streamed-9").is_err());
    }
}

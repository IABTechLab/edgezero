use super::*;

fn header<'a>(reply: &'a Value, name: &str) -> Option<&'a str> {
    reply["headers"].as_array()?.iter().find_map(|field| {
        field[0]
            .as_str()?
            .eq_ignore_ascii_case(name)
            .then(|| field[1].as_str())
            .flatten()
    })
}

fn validate_error(reply: &Value, case: &str) -> Result<()> {
    let internal = case == "single-internal";
    let config = matches!(
        case,
        "single-config" | "single-hooks" | "single-config-explicit"
    );
    let (status, kind, message) = if internal {
        (500, "internal", "internal server error")
    } else if config {
        (503, "config_out_of_date", "configuration unavailable")
    } else {
        (
            503,
            "service_unavailable",
            if case == "custom-retained" {
                "configuration unavailable"
            } else {
                "state unavailable"
            },
        )
    };
    require(reply["status"] == status, "assembly failure status changed")?;
    require(
        header(reply, "content-type") == Some("application/json"),
        "assembly failure lost JSON content type",
    )?;
    require(
        header(reply, "x-assembly-configured").is_none(),
        "failed app headers escaped",
    )?;
    require(
        header(reply, "retry-after") == config.then_some("60"),
        "assembly retry policy changed",
    )?;
    let body: Value = serde_json::from_str(reply["body"].as_str().ok_or("missing assembly body")?)?;
    require(
        body == json!({"error":{"kind":kind,"message":message,"status":status}}),
        "assembly default error body changed",
    )
}

fn validate_retry(replies: &[Value], events: &[Value], custom: bool) -> Result<()> {
    require(replies.len() == 3, "assembly retry needs three requests")?;
    validate_error(
        &replies[0],
        if custom {
            "custom-retained"
        } else {
            "standard-retained"
        },
    )?;
    let mut instance = None;
    for (index, reply) in replies[1..].iter().enumerate() {
        require(
            reply["status"] == 200,
            "failed assembly was cached or retry did not succeed",
        )?;
        require(
            header(reply, "x-assembly-configured") == Some("true"),
            "successful app configuration missing",
        )?;
        let value: Value =
            serde_json::from_str(reply["body"].as_str().ok_or("missing retry body")?)?;
        require(
            value["generation"] == 2 && value["attempts"] == 2,
            "retained app was rebuilt",
        )?;
        require(
            value["visits"] == index + 1,
            "successful registered state was not retained",
        )?;
        require(
            value["configurations"] == if custom { 2 } else { 1 },
            "configuration count changed",
        )?;
        let observed = value["instance"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or("missing assembly instance")?;
        if let Some(expected) = &instance {
            require(observed == expected, "recovery crossed instances")?;
        }
        instance = Some(observed.to_owned());
    }
    let attempts = events
        .iter()
        .filter(|e| e["event"] == "assembly-attempt")
        .collect::<Vec<_>>();
    require(
        attempts.len() == 2,
        "expected exactly two assembly attempts",
    )?;
    for (index, event) in attempts.iter().enumerate() {
        require(
            event["attempt"] == index + 1 && event["instance"].as_str() == instance.as_deref(),
            "assembly attempts crossed instances or ordinals",
        )?;
    }
    if custom {
        let drops = events
            .iter()
            .filter(|e| e["event"] == "assembly-state-drop" && e["generation"] == 1)
            .count();
        require(
            drops == 1,
            "failed configuration did not release partial state exactly once",
        )?;
        let deliveries = events
            .iter()
            .filter(|e| e["event"] == "assembly-delivery")
            .collect::<Vec<_>>();
        require(
            deliveries.len() == 3,
            "missing owned assembly delivery report",
        )?;
        for (index, delivery) in deliveries.iter().enumerate() {
            require(
                delivery["disposition"] == if index == 0 { "detached" } else { "routed" },
                "assembly delivery disposition changed",
            )?;
            require(
                delivery["commitment"] == "Committed"
                    && delivery["outcome"] == "Completed"
                    && delivery["fallback"] == false
                    && delivery["may_send_native"] == false,
                "assembly delivery failed or allowed a native resend",
            )?;
            require(
                delivery["attempts"] == if index == 0 { 1 } else { 2 },
                "retained build attempt count changed",
            )?;
            require(
                delivery["retained"] == (index != 0),
                "partial application was retained",
            )?;
        }
    }
    Ok(())
}

fn validate_boot(events: &[Value], expected: usize, expected_internal: usize) -> Result<()> {
    let logs = events
        .iter()
        .filter(|event| {
            event["event"] == "fixture_log"
                && event["target"] == "edgezero::boot"
                && event["message"]
                    .as_str()
                    .is_some_and(|m| m.starts_with("application assembly failed:"))
        })
        .collect::<Vec<_>>();
    require(
        logs.len() == expected,
        "assembly errors were not logged exactly once per failure",
    )?;
    require(
        logs.iter().all(|event| event["level"] == "ERROR"),
        "assembly boot diagnostic level changed",
    )?;
    if expected_internal > 0 {
        require(
            logs.iter()
                .filter(|event| {
                    event["message"]
                        .as_str()
                        .is_some_and(|m| m.contains("assembly-private-token=secret"))
                })
                .count()
                == expected_internal,
            "full internal error was not logged exactly once",
        )?;
    }
    Ok(())
}

pub(super) fn fastly(exe: &Path, run: &Path, records: &mut Records) -> Result<()> {
    checked(cargo_build().args([
        "build",
        "--locked",
        "--release",
        "-p",
        "fixture-fastly",
        "--bin",
        "fixture-fastly-assembly",
        "--target",
        "wasm32-wasip1",
    ]))?;
    let artifact_path = stage(
        &root().join("target/wasm32-wasip1/release/fixture-fastly-assembly.wasm"),
        run,
    )?;
    records.push(json!({"event":"build","adapter":"fastly","variant":"assembly","artifact":artifact(&artifact_path)?}))?;
    for case in [
        "standard-retained",
        "custom-retained",
        "single-routes",
        "single-config",
        "single-internal",
        "single-hooks",
        "single-config-explicit",
    ] {
        let log = run.join(format!("assembly-{case}.log"));
        let config = run.join(format!("assembly-{case}.toml"));
        let mut contents = fs::read_to_string(root().join("crates/fixture-fastly/fastly.toml"))?;
        contents.push_str(&format!("\n[local_server.config_stores.assembly]\nformat = \"inline-toml\"\n[local_server.config_stores.assembly.contents]\ncase = {case:?}\n"));
        fs::write(&config, contents)?;
        let outcome = (|| -> Result<()> {
            let port = net::port()?;
            let process = Process::start(
                &mut fastly_command(exe, &config, run, "assembly", port),
                &log,
                port,
            )?;
            let retained = case.ends_with("retained");
            let mut replies = Vec::new();
            for _ in 0..if retained { 3 } else { 1 } {
                replies.push(req(port, "/assembly/retry")?);
            }
            if retained {
                await_evidence(|| {
                    validate_retry(&replies, &logs(&log), case == "custom-retained")
                })?;
            } else {
                validate_error(&replies[0], case)?;
                let head = net::head_request(port, "/assembly/retry")?;
                require(
                    head["status"] == replies[0]["status"] && head["body"] == "",
                    "HEAD assembly failure transmitted bytes or changed status",
                )?;
                records.push(annotate(
                    head,
                    json!({"adapter":"fastly","case":case,"method":"HEAD"}),
                ))?;
            }
            await_evidence(|| {
                validate_boot(
                    &logs(&log),
                    if retained { 1 } else { 2 },
                    if case == "single-internal" { 2 } else { 0 },
                )
            })?;
            drop(process);
            validate_boot(
                &logs(&log),
                if retained { 1 } else { 2 },
                if case == "single-internal" { 2 } else { 0 },
            )?;
            for reply in replies {
                records.push(annotate(reply, json!({"adapter":"fastly","case":case})))?;
            }
            records.push(json!({"assertion":"canonical-assembly-delivery","adapter":"fastly","case":case,"status":"pass","qualification":false}))
        })();
        settle(
            records,
            &log,
            json!({"adapter":"fastly","case":case}),
            json!({"phase":"assembly"}),
            outcome,
        )?;
    }
    Ok(())
}

pub(super) fn provider(name: &str, run: &Path, records: &mut Records) -> Result<()> {
    if name == "axum" {
        return Ok(());
    }
    let package = root().join("crates").join(format!("fixture-{name}"));
    let Some((exe, staged)) = provider_build(name, &package, run, records)? else {
        return Ok(());
    };
    for mode in ["per-request", "retained"] {
        let log = run.join(format!("assembly-{name}-{mode}.log"));
        let outcome = (|| -> Result<()> {
            let port = net::port()?;
            let process = Process::start(
                &mut provider_command(name, mode, &exe, &staged, &package, run, port)?,
                &log,
                port,
            )?;
            for (path, case) in [
                ("/assembly/configuration", "single-config"),
                ("/assembly/internal", "single-internal"),
            ] {
                let reply = req(port, path)?;
                validate_error(&reply, case)?;
                records.push(annotate(
                    reply,
                    json!({"adapter":name,"mode":mode,"case":case}),
                ))?;
            }
            let head = net::head_request(port, "/assembly/configuration")?;
            require(
                head["status"] == 503
                    && head["body"] == ""
                    && header(&head, "retry-after") == Some("60"),
                "HEAD config failure lost its status, suppression or retry policy",
            )?;
            records.push(annotate(
                head,
                json!({"adapter":name,"mode":mode,"method":"HEAD"}),
            ))?;
            let mut replies = vec![req(port, "/assembly/retry")?];
            validate_error(&replies[0], "standard-retained")?;
            if mode == "retained" {
                replies.push(req(port, "/assembly/retry")?);
                replies.push(req(port, "/assembly/retry")?);
                await_evidence(|| {
                    require(
                        logs(&log)
                            .iter()
                            .filter(|e| e["event"] == "assembly-attempt")
                            .count()
                            >= 2,
                        "assembly attempts not observed",
                    )
                })?;
                let events = logs(&log);
                let attempts = events
                    .iter()
                    .filter(|e| e["event"] == "assembly-attempt")
                    .collect::<Vec<_>>();
                if replies[1]["status"] == 503
                    && attempts.len() == 3
                    && attempts.iter().all(|e| e["attempt"] == 1)
                    && attempts
                        .windows(2)
                        .all(|pair| pair[0]["instance"] != pair[1]["instance"])
                {
                    records.push(json!({"assertion":"same-instance-assembly-retry","adapter":name,"mode":mode,"status":"unverified","reason":"runtime supplied fresh instances; same-instance retry was not observable","qualification":false}))?;
                } else {
                    validate_retry(&replies, &events, false)?;
                }
            }
            let errors = 3 + replies.iter().filter(|r| r["status"] == 503).count();
            await_evidence(|| validate_boot(&logs(&log), errors, 1))?;
            drop(process);
            validate_boot(&logs(&log), errors, 1)?;
            for reply in replies {
                records.push(annotate(
                    reply,
                    json!({"adapter":name,"mode":mode,"case":"retry"}),
                ))?;
            }
            records.push(json!({"assertion":"canonical-assembly-delivery","adapter":name,"mode":mode,"status":"pass","qualification":false}))
        })();
        settle(
            records,
            &log,
            json!({"adapter":name,"mode":mode}),
            json!({"phase":"assembly"}),
            outcome,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assembly_boot_evidence_counts_each_failed_request_including_head() {
        let event = json!({"event":"fixture_log","target":"edgezero::boot","level":"ERROR",
            "message":"application assembly failed: assembly-private-token=secret"});
        validate_boot(&[event.clone(), event], 2, 2).unwrap();
    }

    #[test]
    fn assembly_evidence_rejects_changed_status_body_and_required_headers() {
        let valid = json!({"status":503,"headers":[["content-type","application/json"],["retry-after","60"]],
            "body":json!({"error":{"kind":"config_out_of_date","message":"configuration unavailable","status":503}}).to_string()});
        validate_error(&valid, "single-config").unwrap();
        for (field, value) in [
            ("status", json!(500)),
            ("body", json!("private-configured-renderer")),
            ("headers", json!([["content-type", "application/json"]])),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            assert!(validate_error(&invalid, "single-config").is_err());
        }
    }
}

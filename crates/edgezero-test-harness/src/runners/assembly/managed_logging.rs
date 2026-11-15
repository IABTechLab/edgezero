use super::*;
use std::io::Read as _;

const LEVELS: [&str; 5] = ["ERROR", "WARN", "INFO", "DEBUG", "TRACE"];

fn bounded_log(path: &Path) -> Result<String> {
    let mut text = String::new();
    fs::File::open(path)?
        .take(1024 * 1024 + 1)
        .read_to_string(&mut text)?;
    require(
        text.len() <= 1024 * 1024,
        "logging evidence exceeded one MiB",
    )?;
    Ok(text)
}

fn bounded_events(path: &Path) -> Result<Vec<Value>> {
    bounded_log(path).map(|text| parse_logs(&text))
}

fn validate_fastly_records(
    text: &str,
    tokens: &[String],
    ceiling: usize,
    owned: bool,
) -> Result<()> {
    let endpoint = text
        .lines()
        .filter(|line| line.contains("fixture-logs :: "))
        .collect::<Vec<_>>()
        .join("\n");
    if owned {
        require(
            endpoint.is_empty(),
            "owned logging installed a managed endpoint",
        )?;
    } else {
        require(
            text.lines()
                .filter(|line| {
                    line.contains("managed-logging-record ")
                        || line.contains("managed-logging-boot ")
                })
                .all(|line| line.contains("fixture-logs :: ")),
            "managed logging unexpectedly echoed records",
        )?;
    }
    let emitted = if owned { text } else { &endpoint };
    for token in tokens {
        validate_records(emitted, token, ceiling, owned)?;
        validate_records(emitted, &format!("after-{token}"), ceiling, owned)?;
    }
    let maximum = if owned {
        "INFO"
    } else {
        LEVELS[ceiling.max(2) - 1]
    };
    validate_owners(text, owned, maximum, tokens.len())
}

fn validate_records(text: &str, token: &str, ceiling: usize, owned: bool) -> Result<()> {
    for (index, level) in LEVELS.iter().enumerate() {
        for (kind, count) in [("record", ceiling), ("boot", 2)] {
            let marker = format!("managed-logging-{kind} {token} {level}");
            require(
                text.lines().filter(|line| line.contains(&marker)).count()
                    == usize::from(index < count),
                "managed logger lost, duplicated or leaked a filtered record",
            )?;
            if !owned && index < count {
                let target = if kind == "boot" {
                    "edgezero::boot"
                } else {
                    "managed_logging_probe"
                };
                require(
                    text.lines()
                        .filter(|line| line.contains(&marker))
                        .all(|line| line.contains(&format!("{level} {target} {marker}"))),
                    "managed logger lost or misplaced the record target",
                )?;
            }
        }
    }
    require(
        text.contains("\"event\":\"fixture_log\"") == owned,
        "application logger ownership changed",
    )
}

fn validate_owner(path: &Path, owned: bool, expected_max: &str) -> Result<()> {
    let text = bounded_log(path)?;
    validate_owners(&text, owned, expected_max, 1)
}

fn validate_owners(text: &str, owned: bool, expected_max: &str, count: usize) -> Result<()> {
    let events: Vec<_> = text
        .lines()
        .filter_map(|line| {
            let start = line.find('{')?;
            serde_json::from_str::<Value>(&line[start..]).ok()
        })
        .filter(|event| event["event"] == "managed-logging-owner")
        .collect();
    require(
        events.len() == count,
        "missing or duplicate logger ownership witness",
    )?;
    for event in &events {
        require(
            event["owned"] == owned
                && event["after"] == expected_max
                && event["unchanged"] == true
                && event["repeated"] == true
                && event["conflict"] == true
                && (!owned || event["before"] == event["after"]),
            "logger was reinstalled, reconfigured or changed application-owned state",
        )?;
    }
    Ok(())
}

fn validate_failure_wire(
    healthy: &Value,
    reply: &Value,
    callback_error: bool,
    head: bool,
) -> Result<()> {
    for capture in [healthy, reply] {
        require(
            capture["protocol"] == "HTTP/1.1"
                && capture["eof"] == true
                && capture.get("capture_error") == Some(&Value::Null),
            "logger failure capture did not reach clean EOF",
        )?;
    }
    let body: Value =
        serde_json::from_str(healthy["body"].as_str().ok_or("missing healthy body")?)?;
    require(
        healthy["status"] == 200
            && header_values(healthy, startup::HEADER)?.len() == 1
            && body["instance"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
            && body["token"] == healthy["token"]
            && body["max_level"] == "INFO"
            && healthy["token"] != reply["token"],
        "missing prescribed healthy logging attestation",
    )?;
    if callback_error {
        let delivered: Value =
            serde_json::from_str(reply["body"].as_str().ok_or("missing callback body")?)?;
        require(
            reply["status"] == 200
                && delivered["instance"] == body["instance"]
                && delivered["token"] == reply["token"]
                && delivered["max_level"] == body["max_level"]
                && header_values(reply, startup::HEADER)?
                    == header_values(healthy, startup::HEADER)?,
            "committed callback error replaced or appended to the application response",
        )
    } else {
        require(
            header_values(reply, startup::HEADER)?.is_empty(),
            "pre-callback failure acquired an application launch header",
        )?;
        if head {
            validate_error_headers(reply, "single-internal")?;
            require(reply["body"] == "", "logger failure sent a HEAD payload")
        } else {
            validate_error(reply, "single-internal")
        }
    }
}

fn validate_failure_events(
    events: &[Value],
    case: &str,
    healthy: &Value,
    reply: &Value,
    expected_error: &str,
    callbacks: usize,
) -> Result<()> {
    let body: Value =
        serde_json::from_str(healthy["body"].as_str().ok_or("missing healthy body")?)?;
    let selected = |kind: &str| {
        events
            .iter()
            .enumerate()
            .filter(|(_, event)| event["event"] == kind)
            .collect::<Vec<_>>()
    };
    let health = selected("managed-logging-control-healthy");
    let terminal = selected("managed-logging-control-terminal");
    let entered = selected("managed-logging-control-callback");
    require(
        health.len() == 1 && terminal.len() == 1 && entered.len() == callbacks,
        "missing, duplicate or unexpected logging control callbacks/terminal evidence",
    )?;
    let (healthy_index, healthy_event) = health[0];
    let (terminal_index, terminal_event) = terminal[0];
    for event in [healthy_event, terminal_event] {
        require(
            event["case"] == case
                && event["instance"] == body["instance"]
                && event["healthy_token"] == healthy["token"],
            "logger failure crossed instances, cases or healthy request tokens",
        )?;
    }
    require(
        healthy_index < terminal_index
            && terminal_event["error"] == expected_error
            && terminal_event["attempted"] == 2
            && terminal_event["limit"]
                .as_u64()
                .is_some_and(|limit| limit > 2)
            && terminal_event["callbacks"].as_u64() == Some(callbacks as u64)
            && terminal_event["maximum_before"] == body["max_level"]
            && terminal_event["maximum_after"] == body["max_level"]
            && terminal_event["qualification"] == false,
        "logger error lost its type, changed filters or terminated at the request limit",
    )?;
    for (ordinal, (index, event)) in entered.iter().enumerate() {
        require(
            *index < terminal_index
                && (*index < healthy_index) == (ordinal == 0)
                && event["case"] == case
                && event["instance"] == body["instance"]
                && event["ordinal"].as_u64() == Some(ordinal as u64 + 1)
                && &event["token"]
                    == if ordinal == 0 {
                        &healthy["token"]
                    } else {
                        &reply["token"]
                    },
            "callback entry was foreign, duplicated or occurred after termination",
        )?;
    }
    Ok(())
}

fn fastly_failure_controls(
    exe: &Path,
    run: &Path,
    records: &mut Records,
    base: &str,
) -> Result<()> {
    let mut failures = 0;
    for (mode, callback_error, head) in [
        ("serve", false, false),
        ("serve", false, true),
        ("run", false, false),
        ("run", false, true),
        ("serve", true, false),
        ("run", true, false),
    ] {
        let case = if callback_error {
            format!("managed-logging-callback-error-{mode}")
        } else {
            format!(
                "managed-logging-pre-callback-{mode}-{}",
                if head { "head" } else { "get" }
            )
        };
        let log = run.join(format!("{case}.log"));
        let outcome = (|| -> Result<()> {
            let config = run.join(format!("{case}.toml"));
            fs::write(
                &config,
                format!(
                    "{base}\n[local_server.config_stores.assembly]\nformat = \"inline-toml\"\n[local_server.config_stores.assembly.contents]\ncase = {case:?}\nexpected_level = \"info\"\n"
                ),
            )?;
            let port = net::port()?;
            let mut process = Process::start(
                &mut fastly_command(exe, &config, run, "assembly", port)?,
                &log,
                port,
            )?;
            let healthy = wire_request(
                "fastly",
                &case,
                port,
                "/managed-logging",
                false,
                run,
                records,
            )?;
            require(
                healthy["status"] == 200,
                "logging control healthy request failed",
            )?;
            let reply = wire_request(
                "fastly",
                &case,
                port,
                "/managed-logging/failure",
                head,
                run,
                records,
            )?;
            validate_failure_wire(&healthy, &reply, callback_error, head)?;
            let expected_error = if mode == "run" {
                "AlreadyInstalled"
            } else {
                "ConfigurationChanged"
            };
            let callbacks = if callback_error {
                2
            } else {
                usize::from(mode == "serve")
            };
            await_evidence(|| {
                validate_failure_events(
                    &bounded_events(&log)?,
                    &case,
                    &healthy,
                    &reply,
                    expected_error,
                    callbacks,
                )
            })?;
            process.finish()?;
            validate_failure_events(
                &bounded_events(&log)?,
                &case,
                &healthy,
                &reply,
                expected_error,
                callbacks,
            )?;
            records.push(
                json!({"assertion":"managed-logging-failure-boundary","adapter":"fastly",
                "case":case,"mode":mode,"method":if head { "HEAD" } else { "GET" },
                "callback_error":callback_error,"same_instance":true,"same_wasm":true,
                "status":"pass","qualification":false}),
            )
        })();
        let result = settle_events(
            records,
            &log,
            json!({"adapter":"fastly","case":case,"qualification":false}),
            json!({"phase":"managed-logging-failure-boundary"}),
            outcome,
            bounded_events(&log),
        );
        failures += usize::from(result.is_err());
    }
    require(
        failures == 0,
        "one or more Fastly logging failure controls failed",
    )
}

pub(super) fn fastly(exe: &Path, run: &Path, records: &mut Records) -> Result<()> {
    let base = fs::read_to_string(fixture_root().join("crates/fixture-fastly/fastly.toml"))?;
    let mut failures = 0;
    for mode in ["custom", "standard", "run"] {
        for (case, value, expected, ceiling, owned) in [
            ("default", None, "info", 3, false),
            ("missing-key", None, "info", 3, false),
            ("info", Some("info"), "info", 3, false),
            ("debug", Some("debug"), "debug", 4, false),
            ("mixed", Some("DeBuG"), "debug", 4, false),
            ("off", Some("off"), "off", 0, false),
            ("invalid", Some("invalid"), "info", 3, false),
            (
                "oversize",
                Some("invalid-level-over-sixteen-bytes"),
                "info",
                3,
                false,
            ),
            ("owned", Some("off"), "off", 2, true),
        ] {
            let name = format!("managed-logging-{mode}-{case}");
            let log = run.join(format!("{name}.log"));
            let outcome = (|| -> Result<()> {
                let config = run.join(format!("{name}.toml"));
                let mut text = format!(
                    "{base}\n[local_server.config_stores.assembly]\nformat = \"inline-toml\"\n[local_server.config_stores.assembly.contents]\ncase = {name:?}\nexpected_level = {expected:?}\n"
                );
                if case != "default" {
                    text.push_str("\n[local_server.config_stores.edgezero_logging]\nformat = \"inline-toml\"\n[local_server.config_stores.edgezero_logging.contents]\n");
                    if let Some(value) = value {
                        text.push_str(&format!("EDGEZERO__LOGGING__LEVEL = {value:?}\n"));
                    }
                }
                fs::write(&config, text)?;
                let port = net::port()?;
                let mut process = Process::start(
                    &mut fastly_command(exe, &config, run, "assembly", port)?,
                    &log,
                    port,
                )?;
                let mut instance = None;
                let mut tokens = Vec::new();
                for _ in 0..2 {
                    let token = token();
                    let response = net::request(port, "/managed-logging", &token, None, None)?;
                    require(response["status"] == 200, "managed Fastly callback failed")?;
                    let body: Value = serde_json::from_str(
                        response["body"].as_str().ok_or("missing logging body")?,
                    )?;
                    let current = body["instance"]
                        .as_str()
                        .ok_or("missing logging instance")?
                        .to_owned();
                    if mode != "run" {
                        if let Some(prior) = &instance {
                            require(
                                prior == &current,
                                "retained logger proof requires one instance",
                            )?;
                        }
                        instance = Some(current);
                    }
                    tokens.push(token);
                    await_evidence(|| {
                        validate_fastly_records(&bounded_log(&log)?, &tokens, ceiling, owned)
                    })?;
                }
                process.finish()?;
                validate_fastly_records(&bounded_log(&log)?, &tokens, ceiling, owned)?;
                records.push(
                    json!({"assertion":"managed-logging","adapter":"fastly","mode":mode,"case":case,
                    "level":value,"sink":"named Fastly endpoint","same_wasm":true,
                    "same_instance":mode != "run","status":"pass","qualification":false}),
                )
            })();
            let result = settle_events(
                records,
                &log,
                json!({"adapter":"fastly","case":name}),
                json!({"phase":"managed-logging"}),
                outcome,
                bounded_events(&log),
            );
            failures += usize::from(result.is_err());
        }
    }
    failures += usize::from(fastly_failure_controls(exe, run, records, &base).is_err());
    require(
        failures == 0,
        "one or more Fastly managed logging cases failed",
    )
}

fn command(
    name: &str,
    level: Option<&str>,
    exe: &Path,
    staged: &Path,
    package: &Path,
    run: &Path,
    port: u16,
) -> Result<Command> {
    if name == "spin" {
        let mut command = provider_command(name, "per-request", exe, staged, package, run, port)?;
        if let Some(level) = level {
            command.args(["--env", &format!("EDGEZERO__LOGGING__LEVEL={level}")]);
        }
        return Ok(command);
    }
    // Logging qualification uses the supported Wrangler launcher, not the native
    // boundary launcher used to isolate upload/egress transport assertions.
    let config = run.join("managed-logging-wrangler.toml");
    fs::write(
        &config,
        wrangler_runtime_config(package, &staged.join("index.js"))?,
    )?;
    let mut command = Command::new(exe);
    let nonce = startup::command(
        &mut command,
        "fixture-cloudflare-per-request",
        &startup::build_id()?,
    );
    command
        .current_dir(package)
        .args(["dev", "--local", "--no-bundle", "--config"])
        .arg(config)
        .args([
            "--ip",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--log-level",
            "debug",
            "--var",
            "FIXTURE_MODE:per-request",
            "--var",
            &format!("{}:{nonce}", startup::NONCE),
        ])
        .env("WRANGLER_SEND_METRICS", "false");
    if let Some(level) = level {
        command.args(["--var", &format!("EDGEZERO__LOGGING__LEVEL:{level}")]);
    }
    Ok(command)
}

pub(super) fn run(
    name: &str,
    exe: &Path,
    staged: &Path,
    package: &Path,
    run: &Path,
    records: &mut Records,
) -> Result<()> {
    let mut failed = 0;
    for (case, level, ceiling, owned) in [
        ("default", None, 3, false),
        ("off", Some("off"), 0, false),
        ("error", Some("error"), 1, false),
        ("warn", Some("warn"), 2, false),
        ("info", Some("info"), 3, false),
        ("debug", Some("debug"), 4, false),
        ("trace", Some("trace"), 5, false),
        ("mixed", Some("DeBuG"), 4, false),
        ("invalid", Some("not-a-level"), 3, false),
        ("owned", Some("off"), 2, true),
    ] {
        let log = run.join(format!("managed-logging-{name}-{case}.log"));
        let stdout = run.join(format!("managed-logging-{name}-{case}.stdout.log"));
        let outcome = (|| -> Result<()> {
            let port = net::port()?;
            let mut cmd = command(name, level, exe, staged, package, run, port)?;
            let mut process = if name == "spin" {
                Process::start_with_stdout(&mut cmd, &log, &stdout, port)?
            } else {
                Process::start(&mut cmd, &log, port)?
            };
            let token = token();
            if name == "cloudflare" && case == "off" {
                let failure = net::request(port, "/managed-logging/failure", &token, None, None)?;
                require(
                    failure["status"] == 503,
                    "cold startup failure was not rendered",
                )?;
                await_evidence(|| {
                    require(
                        bounded_log(&log)?.contains("managed-logging-startup-failure"),
                        "cold boot failure was suppressed at ordinary Off level",
                    )
                })?;
            }
            let reply = net::request(
                port,
                if owned {
                    "/managed-logging/owned"
                } else {
                    "/managed-logging"
                },
                &token,
                None,
                None,
            )?;
            require(reply["status"] == 200, "logging handler failed")?;
            let expected_max = if owned {
                "INFO"
            } else {
                LEVELS[ceiling.max(2) - 1]
            };
            await_evidence(|| {
                let text = bounded_log(&log)?;
                validate_records(&text, &token, ceiling, owned)?;
                validate_records(&text, &format!("after-{token}"), ceiling, owned)?;
                validate_owner(&log, owned, expected_max)
            })?;
            if name == "spin" && case == "off" {
                let failure = net::request(port, "/managed-logging/failure", &token, None, None)?;
                require(
                    failure["status"] == 503,
                    "logging startup failure was not rendered",
                )?;
                await_evidence(|| {
                    require(
                        bounded_log(&log)?.contains("managed-logging-startup-failure"),
                        "boot failure was suppressed at ordinary Off level",
                    )
                })?;
            }
            process.finish()?;
            validate_records(&bounded_log(&log)?, &token, ceiling, owned)?;
            validate_records(
                &bounded_log(&log)?,
                &format!("after-{token}"),
                ceiling,
                owned,
            )?;
            validate_owner(&log, owned, expected_max)?;
            records.push(
                json!({"assertion":"managed-logging","adapter":name,"case":case,
                "level":level,"ordinary_records":ceiling,"boot_records":2,
                "sink":if name == "spin" {"stderr"} else {"Wrangler console"},
                "status":"pass","qualification":false}),
            )
        })();
        let result = settle_events(
            records,
            &log,
            json!({"adapter":name,"case":case}),
            json!({"phase":"managed-logging"}),
            outcome,
            bounded_events(&log),
        );
        failed += usize::from(result.is_err());
    }
    require(failed == 0, "one or more managed logging cases failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires Viceroy and the wasm32-wasip1 fixture toolchain"]
    fn fastly_pre_callback_wire_controls() -> Result<()> {
        let exe = executable("VICEROY_BIN", "viceroy")
            .ok_or("Viceroy unavailable; set VICEROY_BIN to the runtime under test")?;
        let run = fixture_root()
            .join(".runs")
            .join(format!("fastly-505-{}", token()));
        fs::create_dir_all(&run)?;
        let run = run.canonicalize()?;
        println!("Fastly #505 wire evidence: {}", run.display());
        let _output = startup::OutputDirectory::enter(&run)?;
        fs::copy(fixture_root().join("Cargo.lock"), run.join("Cargo.lock"))?;
        fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock"),
            run.join("harness-Cargo.lock"),
        )?;
        let mut records = Records {
            path: run.join("events.jsonl"),
            values: Vec::new(),
        };
        records.push(
            json!({"event":"run","suite":"assembly","selection":"fastly-505-only",
            "cases":6,"fixture_build_id":startup::build_id()?,"qualification":false}),
        )?;
        let version = Command::new(&exe).arg("--version").output()?;
        require(version.status.success(), "Viceroy version command failed")?;
        records.push(
            json!({"event":"runtime","adapter":"fastly","executable":exe,
            "version":String::from_utf8(version.stdout)?.trim(),"qualification":false}),
        )?;
        checked(cargo_build()?.args([
            "build",
            "--offline",
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
            &fixture_root().join("target/wasm32-wasip1/release/fixture-fastly-assembly.wasm"),
            &run,
        )?;
        records.push(
            json!({"event":"build","adapter":"fastly","variant":"assembly",
            "artifact":artifact(&artifact_path)?,"qualification":false}),
        )?;
        let base = fs::read_to_string(fixture_root().join("crates/fixture-fastly/fastly.toml"))?;
        fastly_failure_controls(&exe, &run, &mut records, &base)
    }

    fn failure_evidence(callbacks: usize, head: bool) -> (Value, Value, Vec<Value>) {
        let body =
            |token: &str| json!({"instance":"one","token":token,"max_level":"INFO"}).to_string();
        let healthy = json!({"status":200,"protocol":"HTTP/1.1","eof":true,"capture_error":null,
            "token":"healthy","headers":[[startup::HEADER,"nonce"],["content-type","application/json"]],
            "body":body("healthy")});
        let reply = if callbacks == 2 {
            let mut reply = healthy.clone();
            reply["token"] = json!("failure");
            reply["body"] = json!(body("failure"));
            reply
        } else {
            json!({"status":500,"protocol":"HTTP/1.1","eof":true,"capture_error":null,
                "token":"failure","headers":[["content-type","application/json"]],
                "body":if head { String::new() } else { json!({"error":{"kind":"internal","message":"internal server error","status":500}}).to_string() }})
        };
        let callback = |ordinal, token| {
            json!({"event":"managed-logging-control-callback",
            "case":"control","instance":"one","ordinal":ordinal,"token":token})
        };
        let mut events = Vec::new();
        if callbacks > 0 {
            events.push(callback(1, "healthy"));
        }
        events.push(
            json!({"event":"managed-logging-control-healthy","case":"control",
            "instance":"one","healthy_token":"healthy"}),
        );
        if callbacks == 2 {
            events.push(callback(2, "failure"));
        }
        events.push(
            json!({"event":"managed-logging-control-terminal","case":"control",
            "instance":"one","healthy_token":"healthy","attempted":2,"limit":3,
            "callbacks":callbacks,"error":"ConfigurationChanged","maximum_before":"INFO",
            "maximum_after":"INFO","qualification":false}),
        );
        (healthy, reply, events)
    }

    #[test]
    fn logging_failure_controls_validate_setup_and_same_type_callback_errors() {
        for (callbacks, head) in [(0, false), (0, true), (1, false), (1, true), (2, false)] {
            let (healthy, reply, mut events) = failure_evidence(callbacks, head);
            validate_failure_wire(&healthy, &reply, callbacks == 2, head).unwrap();
            for error in ["ConfigurationChanged", "AlreadyInstalled"] {
                events.last_mut().unwrap()["error"] = json!(error);
                validate_failure_events(&events, "control", &healthy, &reply, error, callbacks)
                    .unwrap();
            }
        }
    }

    #[test]
    fn logging_failure_evidence_rejects_turnover_wrong_errors_limits_and_callbacks() {
        let (healthy, reply, events) = failure_evidence(1, false);
        for (field, value) in [
            ("instance", json!("other")),
            ("healthy_token", json!("other")),
            ("case", json!("other")),
            ("attempted", json!(1)),
            ("limit", json!(2)),
            ("callbacks", json!(2)),
            (
                "error",
                json!("managed logging configuration changed within an instance"),
            ),
            ("error", Value::Null),
            ("maximum_after", json!("OFF")),
            ("qualification", json!(true)),
        ] {
            let mut invalid = events.clone();
            invalid.last_mut().unwrap()[field] = value;
            assert!(
                validate_failure_events(
                    &invalid,
                    "control",
                    &healthy,
                    &reply,
                    "ConfigurationChanged",
                    1
                )
                .is_err(),
                "accepted changed {field}"
            );
        }
        for mutation in [
            "duplicate-terminal",
            "missing-health",
            "extra-callback",
            "foreign-callback",
            "late-callback",
        ] {
            let mut invalid = events.clone();
            match mutation {
                "duplicate-terminal" => invalid.push(events.last().unwrap().clone()),
                "missing-health" => {
                    invalid.remove(1);
                }
                "extra-callback" => invalid.push(events[0].clone()),
                "foreign-callback" => invalid[0]["instance"] = json!("other"),
                "late-callback" => invalid.swap(0, 2),
                _ => unreachable!(),
            }
            assert!(
                validate_failure_events(
                    &invalid,
                    "control",
                    &healthy,
                    &reply,
                    "ConfigurationChanged",
                    1
                )
                .is_err(),
                "accepted {mutation}"
            );
        }
    }

    #[test]
    fn logging_failure_wire_rejects_false_head_eof_and_replaced_committed_responses() {
        let (healthy, reply, _) = failure_evidence(1, true);
        for (field, value) in [
            ("body", json!("payload")),
            ("eof", json!(false)),
            ("capture_error", json!("deadline")),
            ("status", json!(200)),
        ] {
            let mut invalid = reply.clone();
            invalid[field] = value;
            assert!(validate_failure_wire(&healthy, &invalid, false, true).is_err());
        }
        let (healthy, reply, _) = failure_evidence(2, false);
        for body in [
            json!({"error":{"kind":"internal","message":"internal server error","status":500}})
                .to_string(),
            format!(
                "{}HTTP/1.1 500 Error\r\n\r\n",
                reply["body"].as_str().unwrap()
            ),
        ] {
            let mut invalid = reply.clone();
            invalid["body"] = json!(body);
            assert!(validate_failure_wire(&healthy, &invalid, true, false).is_err());
        }
    }

    #[test]
    fn final_logging_validation_rejects_echoes_late_duplicates_and_extra_owners() {
        let mut text = String::new();
        for token in ["probe", "after-probe"] {
            for (kind, target) in [
                ("record", "managed_logging_probe"),
                ("boot", "edgezero::boot"),
            ] {
                for level in ["ERROR", "WARN"] {
                    text.push_str(&format!(
                        "fixture-logs :: {level} {target} managed-logging-{kind} {token} {level}\n"
                    ));
                }
            }
        }
        let owner = json!({"event":"managed-logging-owner","owned":false,"after":"WARN",
            "unchanged":true,"repeated":true,"conflict":true})
        .to_string();
        text.push_str(&format!("{owner}\n"));
        let tokens = ["probe".to_owned()];
        validate_fastly_records(&text, &tokens, 2, false).unwrap();
        for tail in [format!("{owner}\n"),
            "WARN managed_logging_probe managed-logging-record probe WARN\n".to_owned(),
            "fixture-logs :: WARN managed_logging_probe managed-logging-record probe WARN\n".to_owned(),
            "fixture-logs :: DEBUG managed_logging_probe managed-logging-record after-probe DEBUG\n".to_owned()] {
            assert!(validate_fastly_records(&format!("{text}{tail}"), &tokens, 2, false).is_err());
        }
    }

    #[test]
    fn logging_settlement_does_not_reread_oversized_raw_logs() {
        let directory = tempfile::tempdir().unwrap();
        let log = directory.path().join("oversized.log");
        fs::write(&log, vec![b'x'; 1024 * 1024 + 1]).unwrap();
        let mut records = Records {
            path: directory.path().join("events.jsonl"),
            values: Vec::new(),
        };
        assert!(
            settle_events(
                &mut records,
                &log,
                json!({}),
                json!({}),
                Ok(()),
                bounded_events(&log)
            )
            .is_err()
        );
        let events = fs::read_to_string(directory.path().join("events.jsonl")).unwrap();
        assert!(events.contains("logging evidence exceeded one MiB"));
        assert!(!events.contains("xxxxxxxx"));
    }

    #[test]
    fn logging_validator_rejects_missing_duplicate_and_filtered_output() {
        let token = "probe";
        let text = "ERROR managed_logging_probe managed-logging-record probe ERROR\nWARN managed_logging_probe managed-logging-record probe WARN\nERROR edgezero::boot managed-logging-boot probe ERROR\nWARN edgezero::boot managed-logging-boot probe WARN\n";
        validate_records(text, token, 2, false).unwrap();
        assert!(
            validate_records(
                &text.replace(
                    "WARN managed_logging_probe managed-logging-record probe WARN\n",
                    ""
                ),
                token,
                2,
                false
            )
            .is_err()
        );
        assert!(validate_records(&format!("{text}{text}"), token, 2, false).is_err());
        assert!(
            validate_records(
                &text.replace("WARN managed_logging_probe ", "WARN "),
                token,
                2,
                false
            )
            .is_err()
        );
        assert!(
            validate_records(
                &format!("{text}INFO app managed-logging-record probe INFO\n"),
                token,
                2,
                false
            )
            .is_err()
        );
    }
}

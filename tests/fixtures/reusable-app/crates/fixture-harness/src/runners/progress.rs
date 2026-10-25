use super::*;

pub(super) fn run(name: &str, parent: &Path, records: &mut Records) -> Result<()> {
    let run = parent.join(format!("progress-{name}"));
    fs::create_dir_all(&run)?;
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root())
        .output()?;
    require(
        revision.status.success(),
        "cannot determine progress source revision",
    )?;
    let revision = String::from_utf8(revision.stdout)?.trim().to_owned();
    let Some((mut command, port)) = build(name, &run, records, &revision, "outbound-progress")?
    else {
        return Ok(());
    };
    let log = run.join("runtime.log");
    let mut worker_panic_before_shutdown = None;
    let outcome = Process::start(&mut command, &log, port).and_then(|_process| {
        // Characterize independent groups even when an earlier policy probe fails.
        let outcomes = check_groups(|group| match group {
            "coding" => coding_checks(name, port, records),
            "marker" => marker_checks(name, port, records),
            "relay" => relay_checks(name, port, records),
            _ => checks(name, port, &revision, records),
        });
        for (group, result) in &outcomes {
            records.push(json!({"event":"progress-group","adapter":name,"group":group,
                "status":if result.is_ok(){"pass"}else{"fail"},
                "error":result.as_ref().err().map(ToString::to_string),"qualification":false}))?;
        }
        let checked = outcomes.into_iter().try_for_each(|(_, result)| result);
        let checked = if name == "fastly" {
            checked.and(await_evidence(|| {
                require(!fs::read_to_string(&log)?.contains("private-token-secret"), "private sentinel escaped into runtime diagnostics")?;
                validate_framing_evidence(&logs(&log))
            }))
        } else { checked };
        if name!="spin" {return checked;}
        let expected:Vec<_> = records.values.iter().filter(|e|e["event"]=="response-stream-control" && e["status"]=="pass"
            && e["case"].as_str().is_some_and(|case|case.starts_with("sdk-stream-")))
            .map(|e|json!({"case":e["case"],"token":e["observation"]["probe"]["token"]})).collect();
        let stderr = run.join(".spin/logs/fixture_stderr.txt");
        let diagnostics = await_evidence(||validate_sdk_terminals(&logs(&stderr),&expected));
        thread::sleep(Duration::from_millis(100));
        let diagnostics = diagnostics.and_then(|()|validate_sdk_terminals(&logs(&stderr),&expected));
        let premature_panic = fs::read_to_string(&log)?.contains("worker failed: worker panicked");
        worker_panic_before_shutdown = Some(premature_panic);
        let diagnostics = diagnostics.and_then(|()|require(!premature_panic,"Spin worker panicked before controlled shutdown"));
        records.push(json!({"event":"spin-before-shutdown","worker_panic":premature_panic,"sdk_terminal_evidence":if diagnostics.is_ok(){"pass"}else{"fail"},
            "error":diagnostics.as_ref().err().map(ToString::to_string),"qualification":false}))?;
        checked.and(diagnostics)
    });
    if name == "spin" {
        let after = fs::read_to_string(&log)
            .unwrap_or_default()
            .contains("worker failed: worker panicked");
        records.push(json!({"event":"spin-shutdown-observation","worker_panic_before_shutdown":worker_panic_before_shutdown,
            "worker_panic_after_controlled_shutdown":after,"qualification":false}))?;
        for file in ["fixture_stdout.txt", "fixture_stderr.txt"] {
            let path = run.join(".spin/logs").join(file);
            for event in logs(&path) {
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
        json!({"adapter":name,"variant":format!("{name}-outbound-progress")}),
        json!({"phase":"outbound-progress"}),
        outcome,
    )
}

fn check_groups(mut check: impl FnMut(&str) -> Result<()>) -> [(&'static str, Result<()>); 4] {
    ["coding", "marker", "relay", "progress-and-framing"].map(|group| (group, check(group)))
}

fn validate_relay_response(reply: &Value, coding: &str, status: u16, raw: bool) -> Result<()> {
    let fields = reply["headers"].as_array().ok_or("missing relay headers")?;
    let values = |name: &str| {
        fields
            .iter()
            .filter(|field| {
                field[0]
                    .as_str()
                    .is_some_and(|field| field.eq_ignore_ascii_case(name))
            })
            .map(|field| field[1].as_str().ok_or("non-text relay header"))
            .collect::<std::result::Result<Vec<_>, _>>()
    };
    let expected = if raw {
        net::coding_body(coding)?
    } else {
        b"raw coding probe\n"
    };
    require(
        reply["status"] == status && reply["body_bytes"] == json!(expected),
        "relay wire bytes/status mismatch",
    )?;
    require(
        values("x-edgezero-proxy")?.is_empty(),
        "relay added a synthetic proxy marker",
    )?;
    let encoding = values("content-encoding")?;
    require(
        if raw {
            encoding == [coding]
        } else {
            encoding.is_empty()
        },
        "relay wire coding mismatch",
    )?;
    if raw {
        for (name, value) in net::coding_representation_headers(coding, status)? {
            require(
                values(name)? == [value.as_str()],
                "relay representation metadata changed",
            )?;
        }
    }
    let length = values("content-length")?;
    let transfer = values("transfer-encoding")?;
    require(
        length.len() <= 1 && transfer.len() <= 1 && (length.is_empty() || transfer.is_empty()),
        "ambiguous relay wire framing",
    )?;
    if let Some(length) = length.first() {
        require(
            length.parse::<usize>()? == expected.len(),
            "relay wire length mismatch",
        )?;
    }
    if let Some(transfer) = transfer.first() {
        require(
            transfer.eq_ignore_ascii_case("chunked"),
            "unsupported relay wire framing",
        )?;
    }
    Ok(())
}

fn relay_checks(name: &str, port: u16, records: &mut Records) -> Result<()> {
    let backend = Backend::start()?;
    let mut expected_receipts = Vec::new();
    for coding in ["gzip", "deflate", "br"] {
        for mode in ["buffered", "streamed"] {
            for (choice, status) in [
                ("original", 200),
                ("original", 206),
                ("decode", 206),
                ("decode", 200),
            ] {
                let case = format!("relay-{coding}-{mode}-{status}-{choice}");
                let path = format!("/coding/{coding}/{status}");
                let response = net::binary_request(
                    port,
                    &format!("/progress/{case}"),
                    &token(),
                    Some(&backend.url(&path)),
                )?;
                let result = validate_relay_response(
                    &response,
                    coding,
                    status,
                    choice == "original" || status == 206,
                );
                records.push(json!({"event":"outbound-relay","adapter":name,"case":case,
                    "status":if result.is_ok(){"pass"}else{"fail"},"error":result.as_ref().err().map(ToString::to_string),
                    "client":response,"qualification":false}))?;
                result?;
                expected_receipts.push(path);
            }
        }
    }
    let receipts = backend.receipts.lock().unwrap().clone();
    let result = require(
        receipts == expected_receipts && receipts.len() == 24,
        "relay origin receipts mismatch",
    );
    records.push(
        json!({"event":"outbound-relay-coverage","adapter":name,"attempted":24,"receipts":receipts,
        "status":if result.is_ok(){"pass"}else{"fail"},"qualification":false}),
    )?;
    result
}

fn marker_checks(name: &str, port: u16, records: &mut Records) -> Result<()> {
    let mut provider_hides_nomination = false;
    if name == "spin" {
        let control = Backend::start()?;
        for shape in ["upstream", "nominated"] {
            let reply = net::request(
                port,
                "/progress/sdk-marker-headers",
                &token(),
                Some(&control.url(&format!("/marker/get/200/{shape}"))),
                None,
            )?;
            let probe = guest(&reply)?;
            require(
                probe["sdk_native"] == true && probe["markers"] == json!(["upstream"]),
                "missing SDK-only marker control",
            )?;
            if shape == "nominated" {
                provider_hides_nomination = probe["connection"] == json!([]);
            }
            records.push(json!({"event":"marker-sdk-header-visibility","adapter":name,"shape":shape,"probe":probe,"client":reply,"qualification":false}))?;
        }
        require(
            control.receipts.lock().unwrap().len() == 2,
            "SDK header control receipt mismatch",
        )?;
    }
    let backend = Backend::start()?;
    for (method, status) in [
        ("get", 200),
        ("get", 204),
        ("head", 200),
        ("get", 304),
        ("get", 302),
    ] {
        for shape in ["absent", "upstream", "nominated"] {
            for setting in ["inherit", "on", "off"] {
                let case = format!("marker-{method}-{status}-{shape}-{setting}");
                let reply = net::request(
                    port,
                    &format!("/progress/{case}"),
                    &token(),
                    Some(&backend.url(&format!("/marker/{method}/{status}/{shape}"))),
                    None,
                )?;
                let result = (|| -> Result<()> {
                    let probe = guest(&reply)?;
                    let expected = if setting == "on" {
                        json!([name])
                    } else if shape == "upstream"
                        || (shape == "nominated" && provider_hides_nomination)
                    {
                        json!(["upstream"])
                    } else {
                        json!([])
                    };
                    require(
                        probe["case"] == case
                            && probe["status"] == status
                            && probe["markers"] == expected
                            && probe["enabled"] == (setting == "on"),
                        "marker policy/status mismatch",
                    )
                })();
                records.push(json!({"event":"outbound-marker","adapter":name,"case":case,"status":if result.is_ok(){"pass"}else{"fail"},"provider_hidden_nomination":shape == "nominated" && provider_hides_nomination,"error":result.as_ref().err().map(ToString::to_string),"client":reply,"qualification":false}))?;
                result?;
            }
        }
    }
    let reply = net::request(
        port,
        "/progress/marker-batch",
        &token(),
        Some(&backend.url("/marker/get/200/absent")),
        None,
    )?;
    let result = (|| -> Result<()> {
        let probe = guest(&reply)?;
        require(
            probe["case"] == "marker-batch" && probe["markers"] == json!([null, name]),
            "batch marker policy mismatch",
        )
    })();
    records.push(json!({"event":"outbound-marker","adapter":name,"case":"marker-batch","status":if result.is_ok(){"pass"}else{"fail"},"error":result.as_ref().err().map(ToString::to_string),"client":reply,"qualification":false}))?;
    result?;
    require(
        backend.receipts.lock().unwrap().len() == 47,
        "marker origin receipt count mismatch",
    )
}

fn coding_checks(name: &str, port: u16, records: &mut Records) -> Result<()> {
    let backend = Backend::start()?;
    for coding in ["gzip", "deflate", "br"] {
        for mode in ["buffered", "streamed"] {
            for (choice, status) in [
                ("original", 200),
                ("original", 206),
                ("decode", 206),
                ("decode", 200),
            ] {
                let case = format!("coding-{coding}-{mode}-{status}-{choice}");
                let response = net::request(
                    port,
                    &format!("/progress/{case}"),
                    &token(),
                    Some(&backend.url(&format!("/coding/{coding}/{status}"))),
                    None,
                )?;
                let result = (|| -> Result<()> {
                    let probe = guest(&response)?;
                    let raw = choice == "original" || status == 206;
                    let expected = if raw {
                        net::coding_body(coding)?
                    } else {
                        b"raw coding probe\n"
                    };
                    require(
                        probe["case"] == case
                            && probe["status"] == status
                            && probe["bytes"] == json!(expected),
                        "coding bytes/status mismatch",
                    )?;
                    if raw {
                        require(
                            probe["content_encoding"] == coding
                                && probe["content_length"]
                                    .as_str()
                                    .and_then(|value| value.parse::<usize>().ok())
                                    == Some(expected.len()),
                            "original coding metadata lost",
                        )?;
                    } else {
                        require(
                            probe["content_encoding"].is_null()
                                && probe["content_length"].is_null(),
                            "decoded coding metadata retained",
                        )?;
                    }
                    Ok(())
                })();
                records.push(json!({"event":"outbound-coding","adapter":name,"case":case,"status":if result.is_ok(){"pass"}else{"fail"},"error":result.as_ref().err().map(ToString::to_string),"client":response,"qualification":false}))?;
                result?;
            }
        }
    }
    require(
        backend.receipts.lock().unwrap().len() == 24,
        "coding origin receipt count mismatch",
    )
}

pub(super) fn build(
    name: &str,
    run: &Path,
    records: &mut Records,
    revision: &str,
    suite: &str,
) -> Result<Option<(Command, u16)>> {
    let package = root().join("crates").join(format!("fixture-{name}"));
    let (variable, fallback) = match name {
        "fastly" => ("VICEROY_BIN", "viceroy"),
        "spin" => ("SPIN_BIN", "spin"),
        "cloudflare" => ("WRANGLER_BIN", "wrangler"),
        "axum" => ("CARGO", "cargo"),
        _ => return Err("unknown progress adapter".into()),
    };
    let Some(exe) = executable(variable, fallback) else {
        records.push(json!({"status":"unsupported","adapter":name,"phase":"outbound-progress","reason":format!("{fallback} unavailable"),"qualification":false}))?;
        return Ok(None);
    };
    records.push(json!({"event":"progress-runtime","adapter":name,"executable":exe,
        "version":String::from_utf8_lossy(&Command::new(&exe).arg("--version").output()?.stdout).trim(),"revision":revision,"source_state":"working-tree-characterization"}))?;
    let artifact_path = if name == "cloudflare" {
        let Some(builder) = executable("WORKER_BUILD_BIN", "worker-build") else {
            records.push(json!({"status":"unsupported","adapter":name,"phase":"outbound-progress","reason":"worker-build unavailable"}))?;
            return Ok(None);
        };
        if !check_bindgen(records)? {
            return Ok(None);
        }
        checked(
            Command::new(builder)
                .args([
                    "--release",
                    ".",
                    "--",
                    "--offline",
                    "--locked",
                    "--features",
                    "progress",
                ])
                .env("CARGO_TARGET_DIR", root().join("target"))
                .env("EDGEZERO_QUALIFICATION_REVISION", revision)
                .env("EDGEZERO_QUALIFICATION_SUITE", suite)
                .current_dir(&package),
        )?;
        let staged = stage(&package.join("build"), run)?;
        fs::write(
            run.join("wrangler.toml"),
            wrangler_runtime_config(&package, &staged.join("worker/shim.mjs"))?,
        )?;
        staged
    } else {
        let mut command = cargo_build();
        command
            .args([
                "build",
                "--offline",
                "--locked",
                "--release",
                "-p",
                &format!("fixture-{name}"),
                "--features",
                "progress",
            ])
            .env("EDGEZERO_QUALIFICATION_REVISION", revision)
            .env("EDGEZERO_QUALIFICATION_SUITE", suite);
        if name == "spin" {
            command.args(["--target", "wasm32-wasip2"]);
        } else if name == "fastly" {
            command.args([
                "--target",
                "wasm32-wasip1",
                "--bin",
                "fixture-fastly-outbound-progress",
            ]);
        } else {
            command.args(["--bin", "fixture-axum-outbound-progress"]);
        }
        command.arg("--message-format=json-render-diagnostics");
        let output = command.stderr(Stdio::inherit()).output()?;
        require(
            output.status.success(),
            format!("{name} progress fixture build failed"),
        )?;
        let source = match name {
            "spin" => root().join("target/wasm32-wasip2/release/fixture_spin.wasm"),
            "fastly" => {
                root().join("target/wasm32-wasip1/release/fixture-fastly-outbound-progress.wasm")
            }
            _ => built_executable(&output.stdout, "fixture-axum-outbound-progress")?,
        };
        stage(&source, run)?
    };
    let fingerprint = if name == "cloudflare" {
        artifact_path.join("index_bg.wasm")
    } else {
        artifact_path.clone()
    };
    records.push(json!({"event":"build","adapter":name,"variant":format!("{name}-outbound-progress"),"revision":revision,"artifact":artifact(&fingerprint)?}))?;
    let port = net::port()?;
    let command = if name == "fastly" {
        let config = run.join("fastly.toml");
        fs::copy(package.join("fastly.toml"), &config)?;
        fastly_command(&exe, &config, run, "outbound-progress", port)
    } else {
        let exe = if name == "axum" { &artifact_path } else { &exe };
        provider_command(name, "retained", exe, &artifact_path, &package, run, port)?
    };
    Ok(Some((command, port)))
}

// Prevent worker-build from implicitly downloading/replacing tools during characterization.
pub(super) fn check_bindgen(records: &mut Records) -> Result<bool> {
    let output = cargo_build()
        .args([
            "metadata",
            "--offline",
            "--locked",
            "--format-version",
            "1",
            "--filter-platform",
            "wasm32-unknown-unknown",
        ])
        .output()?;
    require(
        output.status.success(),
        "cannot inspect locked wasm-bindgen version",
    )?;
    let metadata: Value = serde_json::from_slice(&output.stdout)?;
    let version = metadata["packages"]
        .as_array()
        .ok_or("missing Cargo packages")?
        .iter()
        .find(|p| p["name"] == "wasm-bindgen")
        .and_then(|p| p["version"].as_str())
        .ok_or("missing locked wasm-bindgen")?;
    let installed = executable("WASM_BINDGEN_BIN", "wasm-bindgen");
    let observed = installed
        .as_ref()
        .map(|exe| Command::new(exe).arg("--version").output())
        .transpose()?;
    let observed = observed
        .as_ref()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned());
    let ready = observed.as_deref() == Some(format!("wasm-bindgen {version}").as_str());
    if !ready {
        records.push(json!({"status":"unsupported","adapter":"cloudflare","phase":"build-tooling","required_wasm_bindgen":version,"observed":observed,"reason":"matching wasm-bindgen CLI unavailable; no download or dependency repin attempted","qualification":false}))?;
    } else if std::env::var_os("WASM_BINDGEN_BIN").is_none() {
        // worker-build does not use PATH by default; require an explicit override.
        records.push(json!({"status":"unsupported","adapter":"cloudflare","phase":"build-tooling","reason":"set WASM_BINDGEN_BIN to the matching executable to prevent automatic installation","qualification":false}))?;
        return Ok(false);
    }
    if ready {
        for variable in ["WASM_BINDGEN_BIN", "ESBUILD_BIN", "WASM_OPT_BIN"] {
            let explicit = std::env::var_os(variable).map(PathBuf::from);
            let Some(path) = explicit.filter(|p| p.is_absolute() && p.is_file()) else {
                records.push(json!({"status":"unsupported","adapter":"cloudflare","phase":"build-tooling","reason":format!("set {variable} to an existing absolute executable; automatic installation is disabled"),"qualification":false}))?;
                return Ok(false);
            };
            let output = Command::new(&path).arg("--version").output()?;
            require(
                output.status.success(),
                format!("{variable} version command failed"),
            )?;
            records.push(json!({"event":"worker-build-tool","variable":variable,"executable":path,"version":String::from_utf8_lossy(&output.stdout).trim()}))?;
        }
    }
    Ok(ready)
}

fn validate_identity(
    probe: &Value,
    name: &str,
    revision: &str,
    case: &str,
    token: &str,
) -> Result<()> {
    require(
        probe["revision"] == revision
            && probe["adapter"] == name
            && probe["case"] == case
            && probe["token"] == token,
        "probe identity mismatch",
    )
}

fn validate_sdk_terminals(events: &[Value], expected: &[Value]) -> Result<()> {
    require(expected.len() == 2, "missing SDK comparison controls")?;
    require(
        !events.iter().any(|e| e["event"] == "progress-guest-panic"),
        "guest panic in response-stream controls",
    )?;
    let terminals: Vec<_> = events
        .iter()
        .filter(|e| e["event"] == "sdk-stream-coordinator-terminal")
        .collect();
    require(
        terminals.len() == expected.len(),
        "missing or duplicate SDK terminal observations",
    )?;
    for control in expected {
        let matching: Vec<_> = terminals
            .iter()
            .filter(|e| e["case"] == control["case"] && e["token"] == control["token"])
            .collect();
        require(
            matching.len() == 1 && matching[0]["coordinator_ok"] == true,
            "SDK terminal missing, failed or uncorrelated",
        )?;
    }
    Ok(())
}

fn validate_empty_signal(name: &str, framing: net::Framing, probe: &Value) -> Result<()> {
    let expected = if matches!(framing, net::Framing::FixedBody | net::Framing::ChunkedBody) {
        4
    } else {
        0
    };
    require(
        probe["bytes_read"] == expected,
        "empty-body premise discarded or invented body bytes",
    )?;
    if name == "cloudflare" {
        let absent = matches!(framing, net::Framing::Absent | net::Framing::LengthZero);
        require(
            probe["adapter"] == name && probe["case"] == "empty" && probe["method"] == "GET",
            "empty-body probe identity mismatch",
        )?;
        require(
            probe["host_signal"]["source"] == "workerd-before-conversion"
                && probe["host_signal"]["body_absent"] == absent,
            "missing or contradictory original Workers body signal",
        )?;
        require(
            probe["body_kind_before"] == "Initial" && probe["body_is_stream"] == !absent,
            "Workers body storage did not preserve the original source presence",
        )?;
    }
    Ok(())
}

fn checks(name: &str, port: u16, revision: &str, records: &mut Records) -> Result<()> {
    framing_checks(name, port, records)?;
    if name == "spin" {
        source_diagnostic_checks(port, records)?;
    }
    let control_passes = if name == "spin" {
        response_controls(name, port, revision, records)?
    } else {
        0
    };
    let mut count = 0;
    let mut origin_requests = 0;
    let mut failures = Vec::new();
    for case in [
        "eager-single",
        "lazy-single",
        "eager-batch",
        "baseline-batch",
        "response-owned",
    ] {
        let backend = if case == "response-owned" {
            Backend::start_gated_progress()?
        } else {
            Backend::start()?
        };
        let token = token();
        let result = (|| -> Result<()> {
            let reply = net::request(
                port,
                &format!("/progress/{case}"),
                &token,
                Some(&backend.url(&format!("/g3/{token}"))),
                (case == "response-owned").then_some(&backend.release),
            )?;
            let mut probe = guest(&reply)?;
            probe["protocol"] = json!("http1");
            validate_identity(&probe, name, revision, case, &token)?;
            if case == "response-owned" {
                require(
                    probe["handler_returned"] == true,
                    "body consumed inside handler",
                )?;
                if name == "cloudflare" || name == "spin" {
                    require(
                        probe["dispatch_returned"] == true,
                        "body consumed before SDK dispatch returned",
                    )?;
                }
            }
            let receipts = backend.progress.lock().unwrap().clone();
            let result = validate_outbound_progress(&probe, &receipts);
            records.push(json!({"event":"outbound-progress","adapter":name,"case":case,"status":if result.is_ok(){"pass"}else{"fail"},"probe":probe,"receipts":receipts,"client":reply,"qualification":false}))?;
            result
        })();
        let receipts = backend.progress.lock().unwrap().clone();
        origin_requests += receipts.len();
        match result {
            Ok(()) => count += 1,
            Err(error) => {
                records.push(json!({"event":"outbound-progress-failure","status":"fail","adapter":name,"case":case,
                    "error":error.to_string(),"receipts":receipts,"qualification":false}))?;
                failures.push(case.to_owned());
            }
        }
    }
    let mut empty_count = 0;
    for framing in [
        net::Framing::Absent,
        net::Framing::LengthZero,
        net::Framing::ChunkedEmpty,
        net::Framing::FixedBody,
        net::Framing::ChunkedBody,
    ] {
        let result = (|| -> Result<()> {
            let reply = net::framed_request(port, framing)?;
            let probe = guest(&reply)?;
            validate_empty_signal(name, framing, &probe)?;
            records.push(json!({"event":"inbound-empty-signal","adapter":name,"framing":format!("{framing:?}"),"protocol":"http1","probe":probe,"client":reply,"qualification":false}))
        })();
        match result {
            Ok(()) => empty_count += 1,
            Err(error) => {
                records.push(json!({"event":"inbound-empty-failure","status":"fail","adapter":name,"framing":format!("{framing:?}"),"error":error.to_string(),"qualification":false}))?;
                failures.push(format!("{framing:?}"));
            }
        }
    }
    records.push(json!({"event":"progress-coverage","adapter":name,"progress_passes":count,"empty_body_passes":empty_count,"attempted_progress_cases":5,"attempted_empty_body_cases":5,"origin_requests":origin_requests,"failures":failures,"qualification":false,
        "limitations":["HTTP/2 and HTTP/3 not exercised","hosted execution pending","response-owned tests handler/dispatch return, not host callback return","missing-context detection unproved except native absent-runtime control","no cancellation or G2 readiness qualification"]}))?;
    require(
        count == 5
            && empty_count == 5
            && origin_requests == 18
            && (name != "spin" || control_passes == 4),
        "SDK progress or empty-body characterization has failed cases; see recorded observations",
    )
}

fn framing_checks(name: &str, port: u16, records: &mut Records) -> Result<()> {
    let reply = net::request(port, "/progress/framing-trailers", &token(), None, None)?;
    let result = validate_framing_wire(&reply);
    records.push(
        json!({"event":"framing-rule", "adapter":name, "reason":"UnsupportedTrailers",
        "client":reply,"status":if result.is_ok(){"pass"}else{"fail"},"qualification":false}),
    )?;
    result
}

fn validate_framing_wire(reply: &Value) -> Result<()> {
    require(
        reply["status"] == 500
            && reply["body"] == "internal server error"
            && !reply.to_string().contains("private-token-secret"),
        "framing fallback wire contract changed",
    )
}

fn validate_framing_evidence(events: &[Value]) -> Result<()> {
    require(
        !events
            .iter()
            .any(|event| event.to_string().contains("private-token-secret")),
        "private sentinel escaped into captured diagnostics",
    )?;
    let reports: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "framing-terminal")
        .collect();
    require(
        reports.len() == 1
            && reports[0]["application"] == "UnsupportedTrailers"
            && reports[0]["fallback"].is_null()
            && reports[0]["outcome"] == "ConversionError"
            && reports[0]["body_kind"] == "Fallback"
            && reports[0]["disposition"] == "Completed"
            && reports[0]["bytes_written"] == 21,
        "missing or altered framing terminal report",
    )?;
    require(
        events.iter().any(|event| {
            event["event"] == "fixture_log"
                && event["level"] == "ERROR"
                && event["target"] == "edgezero_core::response_egress"
                && event["message"].as_str().is_some_and(|message| {
                    message.contains("response trailers are unsupported")
                        && !message.contains("secret")
                })
        }),
        "missing bounded framing error log",
    )
}

fn source_diagnostic_checks(port: u16, records: &mut Records) -> Result<()> {
    for (path, event, cases, expected) in [
        (
            "/progress/sdk-source-diagnostics",
            "source-diagnostic-race",
            8,
            json!({"source_diagnostics":"pass", "cases":8, "qualification":false}),
        ),
        (
            "/progress/sdk-connect-fallback",
            "connect-timeout-fallback",
            60,
            json!({"connect_fallback":"pass", "outcomes":5, "budget_sources":4, "checks":60, "qualification":false}),
        ),
    ] {
        let reply = net::request(port, path, &token(), None, None)?;
        let probe = guest(&reply)?;
        let result = require(
            reply["status"] == 200 && probe == expected,
            "Spin SDK policy probe changed",
        );
        records.push(json!({"event":event, "adapter":"spin", "cases":cases, "observation":probe,
            "status":if result.is_ok(){"pass"}else{"fail"}, "client":reply, "qualification":false}))?;
        result?;
    }
    Ok(())
}

fn response_controls(
    name: &str,
    port: u16,
    revision: &str,
    records: &mut Records,
) -> Result<usize> {
    let mut passes = 0;
    let mut failures = Vec::new();
    for case in [
        "response-owned-ready",
        "response-owned-timer-only",
        "sdk-stream-ready",
        "sdk-stream-timer-only",
    ] {
        let token = token();
        let result = (|| -> Result<Value> {
            let reply = net::request(port, &format!("/progress/{case}"), &token, None, None)?;
            let probe = guest(&reply)?;
            validate_identity(&probe, name, revision, case, &token)?;
            if case.starts_with("response-owned") {
                require(
                    probe["handler_returned_at_first_poll"] == true
                        && probe["dispatch_returned_at_first_poll"] == true
                        && probe["handler_returned"] == true
                        && probe["dispatch_returned"] == true,
                    "stream control was polled before return",
                )?;
            } else {
                require(
                    probe["sdk_native"] == true,
                    "SDK control did not use the SDK response writer",
                )?;
            }
            if case.ends_with("timer-only") {
                require(
                    probe["pause_ns"]
                        .as_u64()
                        .is_some_and(|ns| ns >= 100_000_000),
                    "timer control did not complete its measured wait",
                )?;
            }
            Ok(json!({"probe":probe,"client":reply}))
        })();
        match result {
            Ok(observation) => {
                passes += 1;
                records.push(json!({"event":"response-stream-control","adapter":name,"case":case,"status":"pass","observation":observation,"qualification":false}))?;
            }
            Err(error) => {
                failures.push(case);
                let partial = error
                    .downcast_ref::<net::InterruptedResponse>()
                    .map(|r| json!({"status":r.status,"body_bytes":r.body.len()}));
                records.push(json!({"event":"response-stream-control","adapter":name,"case":case,"status":"fail","error":error.to_string(),"partial":partial,"qualification":false}))?;
            }
        }
    }
    records.push(json!({"event":"response-stream-control-coverage","adapter":name,"attempted":4,"passes":passes,"failures":failures,"qualification":false}))?;
    Ok(passes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_wire_assertions_reject_recoding_and_missing_representation_fields() {
        let mut headers = json!([
            ["content-encoding", "gzip"],
            ["content-length", "37"],
            ["content-type", "application/octet-stream"],
            ["etag", "\"coding-probe\""],
            ["last-modified", "Tue, 20 Oct 2026 00:00:00 GMT"],
            ["vary", "Accept-Encoding"],
            ["accept-ranges", "bytes"],
            ["cache-control", "max-age=60"],
            [
                "content-digest",
                "sha-256=:BkzflHMq10MSCPDB7aAGrb3zFOrrCTOp44/5RdKVon4=:"
            ],
            ["content-range", "bytes 0-36/37"]
        ]);
        let reply =
            json!({"status":206,"headers":headers,"body_bytes":net::coding_body("gzip").unwrap()});
        validate_relay_response(&reply, "gzip", 206, true).unwrap();
        for change in [
            "body",
            "encoding",
            "length",
            "metadata",
            "range",
            "marker",
            "status",
            "ambiguous-framing",
        ] {
            let mut broken = reply.clone();
            match change {
                "body" => broken["body_bytes"] = json!([0, 1]),
                "encoding" => broken["headers"][0][1] = json!("br"),
                "length" => broken["headers"][1][1] = json!("1"),
                "metadata" => {
                    broken["headers"].as_array_mut().unwrap().remove(3);
                }
                "range" => {
                    broken["headers"].as_array_mut().unwrap().pop();
                }
                "marker" => broken["headers"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!(["x-edgezero-proxy", "cloudflare"])),
                "status" => broken["status"] = json!(200),
                "ambiguous-framing" => broken["headers"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!(["transfer-encoding", "chunked"])),
                _ => unreachable!(),
            }
            assert!(
                validate_relay_response(&broken, "gzip", 206, true).is_err(),
                "{change}"
            );
        }
        headers.as_array_mut().unwrap().retain(|field| {
            !matches!(
                field[0].as_str(),
                Some("content-encoding" | "content-range")
            )
        });
        for field in headers.as_array_mut().unwrap() {
            if field[0] == "content-length" {
                field[1] = json!("17");
            }
        }
        let decoded = json!({"status":200,"headers":headers,"body_bytes":b"raw coding probe\n"});
        validate_relay_response(&decoded, "gzip", 200, false).unwrap();
        let mut stale = decoded.clone();
        stale["headers"]
            .as_array_mut()
            .unwrap()
            .push(json!(["content-encoding", "gzip"]));
        assert!(validate_relay_response(&stale, "gzip", 200, false).is_err());
    }

    #[test]
    fn progress_groups_keep_later_observations_after_policy_failures() {
        for mask in 0..16 {
            let failed: Vec<_> = ["coding", "marker", "relay", "progress-and-framing"]
                .into_iter()
                .enumerate()
                .filter_map(|(index, group)| (mask & (1 << index) != 0).then_some(group))
                .collect();
            let mut visited = Vec::new();
            let outcomes = check_groups(|group| {
                visited.push(group.to_owned());
                if failed.contains(&group) {
                    Err(format!("{group} failure").into())
                } else {
                    Ok(())
                }
            });
            assert_eq!(
                visited,
                ["coding", "marker", "relay", "progress-and-framing"]
            );
            let errors: Vec<_> = outcomes
                .iter()
                .filter_map(|(group, result)| {
                    result
                        .as_ref()
                        .err()
                        .map(|error| (*group, error.to_string()))
                })
                .collect();
            assert_eq!(
                errors,
                failed
                    .iter()
                    .map(|group| (*group, format!("{group} failure")))
                    .collect::<Vec<_>>()
            );
            let result = outcomes.into_iter().try_for_each(|(_, result)| result);
            assert_eq!(result.is_ok(), failed.is_empty());
            assert_eq!(
                result.err().map(|error| error.to_string()),
                failed.first().map(|group| format!("{group} failure"))
            );
        }
    }

    #[test]
    fn cloudflare_empty_signal_requires_the_original_host_snapshot() {
        for (framing, bytes, absent) in [
            (net::Framing::Absent, 0, true),
            (net::Framing::LengthZero, 0, true),
            (net::Framing::ChunkedEmpty, 0, false),
            (net::Framing::FixedBody, 4, false),
            (net::Framing::ChunkedBody, 4, false),
        ] {
            let probe = json!({"adapter":"cloudflare","case":"empty","method":"GET",
                "bytes_read":bytes,"body_is_stream":!absent,"body_kind_before":"Initial",
                "host_signal":{"source":"workerd-before-conversion","body_absent":absent}});
            validate_empty_signal("cloudflare", framing, &probe).unwrap();
            for change in [
                "missing",
                "missing-boolean",
                "null-boolean",
                "contradictory",
                "string",
                "source",
                "bytes",
                "adapter",
                "method",
                "case",
                "storage-missing",
                "storage-null",
                "storage-string",
                "storage-inverted",
                "cached-before-transfer",
            ] {
                let mut invalid = probe.clone();
                match change {
                    "missing" => invalid["host_signal"] = Value::Null,
                    "missing-boolean" => {
                        invalid["host_signal"]
                            .as_object_mut()
                            .unwrap()
                            .remove("body_absent");
                    }
                    "null-boolean" => invalid["host_signal"]["body_absent"] = Value::Null,
                    "contradictory" => invalid["host_signal"]["body_absent"] = json!(!absent),
                    "string" => invalid["host_signal"]["body_absent"] = json!(absent.to_string()),
                    "source" => invalid["host_signal"]["source"] = json!("after-conversion"),
                    "bytes" => invalid["bytes_read"] = json!(bytes + 1),
                    "adapter" => invalid["adapter"] = json!("spin"),
                    "method" => invalid["method"] = json!("HEAD"),
                    "case" => invalid["case"] = json!("other"),
                    "storage-missing" => {
                        invalid.as_object_mut().unwrap().remove("body_is_stream");
                    }
                    "storage-null" => invalid["body_is_stream"] = Value::Null,
                    "storage-string" => invalid["body_is_stream"] = json!((!absent).to_string()),
                    "storage-inverted" => invalid["body_is_stream"] = json!(absent),
                    "cached-before-transfer" => {
                        invalid["body_kind_before"] = json!("Cached { len: 0 }")
                    }
                    _ => unreachable!(),
                }
                assert!(
                    validate_empty_signal("cloudflare", framing, &invalid).is_err(),
                    "{framing:?}: {change}"
                );
            }
        }
    }

    #[test]
    fn other_empty_probes_do_not_claim_cloudflare_raw_signals() {
        for name in ["axum", "fastly", "spin"] {
            for (framing, bytes) in [(net::Framing::Absent, 0), (net::Framing::FixedBody, 4)] {
                let probe =
                    json!({"adapter":name,"case":"empty","method":"GET","bytes_read":bytes});
                validate_empty_signal(name, framing, &probe).unwrap();
                assert!(
                    validate_empty_signal(name, framing, &json!({"bytes_read":bytes+1})).is_err()
                );
            }
        }
    }

    #[test]
    fn framing_evidence_requires_exact_report_and_bounded_error_log() {
        let events = vec![
            json!({"event":"framing-terminal", "application":"UnsupportedTrailers",
            "fallback":null,"outcome":"ConversionError","body_kind":"Fallback","disposition":"Completed","bytes_written":21}),
            json!({"event":"fixture_log","level":"ERROR","target":"edgezero_core::response_egress",
                "message":"response-egress Application framing validation failed before commit: response trailers are unsupported"}),
        ];
        validate_framing_evidence(&events).unwrap();
        for (index, key, value) in [
            (0, "application", json!("InvalidContentLength")),
            (0, "fallback", json!("UnsupportedTrailers")),
            (0, "outcome", json!("DeadlineExceeded")),
            (0, "disposition", json!("Aborted")),
            (0, "bytes_written", json!(0)),
            (1, "level", json!("DEBUG")),
            (
                1,
                "message",
                json!("response trailers are unsupported private-token-secret"),
            ),
        ] {
            let mut altered = events.clone();
            altered[index][key] = value;
            assert!(validate_framing_evidence(&altered).is_err());
        }
        assert!(validate_framing_evidence(&[]).is_err());
        let mut duplicate = events.clone();
        duplicate.push(events[0].clone());
        assert!(validate_framing_evidence(&duplicate).is_err());
        let mut additional_leak = events;
        additional_leak.push(json!({"event":"fixture_log", "message":"private-token-secret"}));
        assert!(validate_framing_evidence(&additional_leak).is_err());
        let reply = json!({"status":500,"body":"internal server error","headers":[["content-type","text/plain"]]});
        validate_framing_wire(&reply).unwrap();
        let mut leaked_header = reply;
        leaked_header["headers"]
            .as_array_mut()
            .unwrap()
            .push(json!(["x-private", "private-token-secret"]));
        assert!(validate_framing_wire(&leaked_header).is_err());
    }

    #[test]
    fn sdk_terminal_evidence_rejects_missing_failed_duplicate_and_foreign_completion() {
        let expected = vec![
            json!({"case":"sdk-stream-ready","token":"one"}),
            json!({"case":"sdk-stream-timer-only","token":"two"}),
        ];
        let events:Vec<_> = expected.iter().map(|e|json!({"event":"sdk-stream-coordinator-terminal","case":e["case"],"token":e["token"],"coordinator_ok":true})).collect();
        validate_sdk_terminals(&events, &expected).unwrap();
        for change in ["missing", "failed", "duplicate", "foreign", "panic"] {
            let mut mutated = events.clone();
            match change {
                "missing" => {
                    mutated.pop();
                }
                "failed" => mutated[0]["coordinator_ok"] = json!(false),
                "duplicate" => mutated.push(mutated[0].clone()),
                "foreign" => mutated[0]["token"] = json!("other"),
                "panic" => mutated.push(json!({"event":"progress-guest-panic"})),
                _ => unreachable!(),
            }
            assert!(
                validate_sdk_terminals(&mutated, &expected).is_err(),
                "{change}"
            );
        }
        assert!(validate_sdk_terminals(&[], &[]).is_err());
    }

    #[test]
    fn progress_identity_rejects_a_different_case_or_request() {
        let probe =
            json!({"adapter":"axum","revision":"candidate","case":"eager-single","token":"one"});
        validate_identity(&probe, "axum", "candidate", "eager-single", "one").unwrap();
        assert!(validate_identity(&probe, "axum", "candidate", "lazy-single", "one").is_err());
        assert!(validate_identity(&probe, "axum", "candidate", "eager-single", "two").is_err());
        assert!(validate_identity(&probe, "spin", "candidate", "eager-single", "one").is_err());
        assert!(validate_identity(&probe, "axum", "different", "eager-single", "one").is_err());
    }

    #[test]
    fn progress_builds_use_adapter_qualified_variants() {
        let events = ["axum", "fastly"].map(|name| {
            json!({"event":"build","adapter":name,
            "variant":format!("{name}-outbound-progress"),"artifact":{"fnv1a64":name}})
        });
        assert_eq!(summarize(&events)["build_identity"]["status"], "consistent");
    }
}

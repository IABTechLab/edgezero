use super::*;
use crate::net::WireOrigins;
use std::io::Read as _;

fn body(reply: &Value) -> Result<Vec<u8>> {
    Ok(serde_json::from_value(reply["body_bytes"].clone())?)
}

fn handoff(bytes: &[u8]) -> Result<()> {
    require(
        bytes.len() == 21 && bytes.starts_with(b"origin"),
        "handoff origin bytes differ",
    )?;
    let mut bids = bytes[6..].as_chunks::<5>().0.to_vec();
    bids.sort_unstable();
    require(
        bids == [*b"bid-A", *b"bid-B", *b"bid-C"],
        "handoff lost or changed bidder bytes",
    )
}

fn error_is(report: &Value, kind: &str, reason: &str) -> Result<()> {
    let error = &report["outcome"]["error"];
    require(
        error["kind"] == kind
            && error["reason"] == reason
            && error["status"] == if kind == "gateway_timeout" { 504 } else { 502 },
        "wrong typed wire outcome",
    )
}

struct Matrix<'a> {
    adapter: &'static str,
    endpoint: &'static str,
    port: u16,
    origins: &'a WireOrigins,
    records: &'a mut Records,
    exchanges: usize,
    dispatches: usize,
}

impl Matrix<'_> {
    fn call(
        &mut self,
        path: &str,
        mode: &str,
        streamed: bool,
        encoded: bool,
        expected: Option<Vec<(u16, String)>>,
        gate: Option<&str>,
    ) -> Result<Value> {
        require(
            self.origins.take_requests().is_empty(),
            "request escaped the preceding wire exchange",
        )?;
        if let Some(name) = gate {
            self.origins.arm(name);
        }
        let uri = format!("http://127.0.0.1:{}{path}", self.origins.port(0));
        let response = net::header_request(
            self.port,
            self.endpoint,
            &token(),
            &[
                ("x-origin", &uri),
                ("x-p5-mode", mode),
                ("x-p5-stream", if streamed { "true" } else { "false" }),
                ("x-p5-encoded-cap", if encoded { "true" } else { "false" }),
            ],
        );
        if let Some(name) = gate {
            self.origins.release(name);
        }
        let response = response?;
        require(
            response["status"] == 200,
            "wire guest rejected the exchange",
        )?;
        let observed = self.origins.take_requests();
        let expected = expected.unwrap_or_else(|| vec![(self.origins.port(0), path.to_owned())]);
        if observed != expected {
            self.records.push(json!({"event":format!("{}-wire-receipt-failure", self.adapter),"status":"fail",
                "path":path,"mode":mode,"observed":observed,"expected":expected,"qualification":false}))?;
        }
        require(
            observed == expected,
            "missing, extra or foreign wire origin requests",
        )?;
        let report: Value = serde_json::from_slice(&body(&response)?)?;
        self.exchanges += 1;
        self.dispatches += observed.len();
        self.records.push(
            json!({"event":format!("{}-wire-case", self.adapter),"path":path,"mode":mode,
            "streamed":streamed,"encoded_cap":encoded,"origin_requests":observed,
            "report":report,"qualification":false}),
        )?;
        Ok(report)
    }

    fn parity(&mut self, path: &str, streamed: bool, encoded: bool) -> Result<Value> {
        let sent = self.call(path, "send", streamed, encoded, None, None)?;
        let followed = self.call(path, "follow", streamed, encoded, None, None)?;
        require(
            sent["outcome"] == followed["outcome"],
            "Send/Follow outcomes differ",
        )?;
        Ok(followed)
    }

    fn run(&mut self) -> Result<()> {
        let first = self.origins.port(0);
        let second = self.origins.port(1);
        let base = format!("http://127.0.0.1:{first}");
        for streamed in [false, true] {
            for coding in ["identity", "gzip", "passthrough"] {
                for size in [10, 11] {
                    let path = format!("/p5/body/{coding}/{size}");
                    let encoded = streamed && coding == "passthrough";
                    let followed = self.parity(&path, streamed, encoded)?;
                    require(
                        followed["policies"] == 0 && followed["hop"] == 0,
                        "unexpected no-redirect policy/hop",
                    )?;
                    if size == 10 {
                        let outcome = &followed["outcome"];
                        require(
                            outcome["status"] == 200
                                && outcome["bytes"] == json!(vec![b'a'; 10])
                                && followed["streamed"] == streamed
                                && followed["final_uri"] == format!("{base}{path}")
                                && followed["redirects"] == 0
                                && followed.get("stop") == Some(&Value::Null),
                            "wrong no-redirect final response",
                        )?;
                        let fields = outcome["headers"]
                            .as_array()
                            .ok_or("missing wire headers")?;
                        let field = |name: &str| {
                            fields
                                .iter()
                                .find(|entry| entry[0] == name)
                                .map(|entry| &entry[1])
                        };
                        require(
                            field("x-p5-fixture") == Some(&json!(b"spin-wire".as_slice())),
                            "missing wire fixture header",
                        )?;
                        if coding == "gzip" {
                            require(
                                field("content-encoding").is_none()
                                    && field("content-length").is_none(),
                                "decoded representation fields retained",
                            )?;
                        } else {
                            let value: &[u8] = if coding == "identity" {
                                b"identity"
                            } else {
                                b"x-p5-opaque"
                            };
                            require(
                                field("content-encoding") == Some(&json!(value)),
                                "coding field changed",
                            )?;
                        }
                    } else {
                        error_is(
                            &followed,
                            "response_too_large",
                            if encoded {
                                "EncodedBody"
                            } else if streamed {
                                "DecodedBody"
                            } else {
                                "BufferedBody"
                            },
                        )?;
                    }
                }
                let followed = self.parity(&format!("/p5/declared/{coding}"), streamed, true)?;
                error_is(&followed, "response_too_large", "EncodedBody")?;
            }
        }
        for kind in ["pending", "error", "emptybr", "oversized"] {
            let mut errors = Vec::new();
            for mode in ["follow", "send", "stop", "limit"] {
                let name = format!("{kind}-{mode}");
                let redirect = format!("/p5/redirect/{kind}/{name}");
                let path = if mode == "limit" {
                    format!("/p5/limit/{kind}/{name}")
                } else {
                    redirect.clone()
                };
                let mut expected = vec![(first, path.clone())];
                if mode == "follow" {
                    expected.push((first, format!("/p5/next/{name}")));
                } else if mode == "limit" {
                    expected.push((first, redirect));
                }
                let report = self.call(&path, mode, false, false, Some(expected), Some(&name))?;
                if mode == "follow" {
                    require(
                        report["outcome"]["status"] == 200
                            && report["outcome"]["bytes"] == json!(b"final".as_slice())
                            && report["redirects"] == 1
                            && report["policies"] == 1
                            && report.get("stop") == Some(&Value::Null)
                            && report["final_uri"] == format!("{base}/p5/next/{name}"),
                        "wrong intermediate-body follow result",
                    )?;
                } else {
                    require(
                        report["hop"] == usize::from(mode == "limit")
                            && report["policies"] == usize::from(mode != "send"),
                        "wrong stopped/limit hop or policy count",
                    )?;
                    match kind {
                        "pending" => error_is(&report, "gateway_timeout", "PerCallTimeout")?,
                        "error" => {
                            source_error(&report, self.adapter)?;
                        }
                        "emptybr" => error_is(&report, "bad_gateway", "Decode(Brotli)")?,
                        _ => error_is(&report, "response_too_large", "BufferedBody")?,
                    }
                    errors.push(report["outcome"].clone());
                }
            }
            require(
                errors.len() == 3 && errors.iter().all(|error| error == &errors[0]),
                "Send/stop/limit final errors differ",
            )?;
        }
        for mode in ["stop", "limit"] {
            let name = format!("clean-{mode}");
            let redirect = format!("/p5/redirect/clean/{name}");
            let path = if mode == "limit" {
                format!("/p5/limit/clean/{name}")
            } else {
                redirect.clone()
            };
            let mut expected = vec![(first, path.clone())];
            if mode == "limit" {
                expected.push((first, redirect));
            }
            let report = self.call(&path, mode, false, false, Some(expected), Some(&name))?;
            require(
                report["outcome"]["status"] == 302
                    && report["outcome"]["bytes"] == json!(b"found".as_slice())
                    && report["stop"]
                        == if mode == "limit" {
                            "LimitReached"
                        } else {
                            "StoppedByPolicy"
                        }
                    && report["redirects"] == usize::from(mode == "limit"),
                "wrong clean stop/limit response",
            )?;
        }
        let sent = self.call("/p5/cross", "send", false, false, None, None)?;
        require(
            sent["outcome"]["status"] == 302,
            "plain Send followed the cross-origin redirect",
        )?;
        let report = self.call(
            "/p5/cross",
            "follow",
            false,
            false,
            Some(vec![
                (first, "/p5/cross".to_owned()),
                (second, "/p5/final".to_owned()),
            ]),
            None,
        )?;
        require(
            report["outcome"]["status"] == 200
                && report["outcome"]["bytes"] == json!(b"final".as_slice())
                && report["redirects"] == 1
                && report["final_uri"] == format!("http://127.0.0.1:{second}/p5/final"),
            "wrong cross-origin final response",
        )?;
        for coding in ["identity", "gzip", "passthrough"] {
            for size in [10, 11] {
                let final_path = format!("/p5/body/{coding}/{size}");
                let path = format!("/p5/to-body/{coding}/{size}");
                let encoded = coding == "passthrough";
                let sent = self.call(&final_path, "send", true, encoded, None, None)?;
                let followed = self.call(
                    &path,
                    "follow",
                    true,
                    encoded,
                    Some(vec![(first, path.clone()), (first, final_path.clone())]),
                    None,
                )?;
                require(
                    sent["outcome"] == followed["outcome"]
                        && followed["streamed"] == true
                        && followed["redirects"] == 1
                        && followed["policies"] == 1
                        && followed["final_uri"] == format!("{base}{final_path}"),
                    "streamed final Send/Follow parity differs",
                )?;
            }
        }
        require(
            self.exchanges == 68 && self.dispatches == 84,
            "incomplete provider wire matrix",
        )
    }
}

fn source_error_reason(adapter: &str) -> &'static str {
    if adapter == "spin" {
        "Protocol"
    } else {
        "Transport"
    }
}

fn source_error(report: &Value, adapter: &str) -> Result<()> {
    error_is(report, "bad_gateway", source_error_reason(adapter))?;
    let provider = report["outcome"]["error"]["provider"].as_str();
    require(
        provider.is_some_and(|detail| detail.contains("ResponseBodyRead")),
        "lost source-error provider phase",
    )?;
    require(
        adapter != "cloudflare"
            || provider == Some("Cloudflare { phase: ResponseBodyRead, cause: Unknown }"),
        "unexpected workerd source-error cause",
    )
}

fn component_text(directory: &Path) -> Result<String> {
    let mut text = String::new();
    let mut directories = vec![directory.to_owned()];
    let mut entries = 0;
    while let Some(directory) = directories.pop() {
        if !directory.exists() {
            continue;
        }
        for entry in fs::read_dir(directory)? {
            entries += 1;
            require(entries <= 128, "too many component log entries")?;
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                directories.push(entry.path());
            } else if kind.is_file() {
                fs::File::open(entry.path())?
                    .take(
                        (1024 * 1024_usize)
                            .saturating_sub(text.len())
                            .saturating_add(1) as u64,
                    )
                    .read_to_string(&mut text)?;
                require(text.len() <= 1024 * 1024, "component logs exceeded 1 MiB")?;
            }
        }
    }
    Ok(text)
}

fn controls(port: u16, origins: &WireOrigins, log_dir: &Path, process: &mut Process) -> Result<()> {
    for name in ["get", "head", "get-settled", "head-settled", "post"] {
        let uri = format!("http://127.0.0.1:{}/{name}", origins.port(0));
        let reply =
            net::header_request(port, &format!("/{name}"), &token(), &[("x-origin", &uri)])?;
        require(reply["status"] == 200, "wire control guest failed")?;
        let captured = origins.take_wire()?;
        require(
            captured["method"]
                == if name == "post" {
                    "POST"
                } else if name.starts_with("head") {
                    "HEAD"
                } else {
                    "GET"
                }
                && captured["target"] == format!("/{name}"),
            "wire control method/target changed",
        )?;
        if name == "post" {
            require(
                captured["payload"] == "upload-control",
                "upload control bytes differ",
            )?;
        } else {
            require(
                captured["content_length"] == json!([])
                    && captured["transfer_encoding"] == json!([])
                    && captured["encoded_body_hex"] == "",
                "bodyless request carried framing or bytes",
            )?;
        }
    }
    require(
        net::header_request(port, "/scope", &token(), &[])?["status"] == 200,
        "canonical scope control failed",
    )?;
    let uri = format!("http://127.0.0.1:{}", origins.port(0));
    let reply = net::header_request(port, "/handoff", &token(), &[("x-origin", &uri)])?;
    require(reply["status"] == 200, "handoff guest failed")?;
    handoff(&body(&reply)?)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        process.check_running()?;
        let text = component_text(log_dir)?;
        if text.contains("PASS: Spin attached Begin/Batch handoff closed at SDK terminal") {
            return Ok(());
        }
        require(
            !text.contains("FAIL: Spin attached") && Instant::now() < deadline,
            "SDK-terminal ownership proof missing",
        )?;
        thread::sleep(Duration::from_millis(10));
    }
}

pub(super) fn run(name: &str, run: &Path, records: &mut Records) -> Result<()> {
    match name {
        "cloudflare" => return progress_wire("cloudflare", run, records),
        "axum" => return progress_wire("axum", run, records),
        "fastly" => return progress_wire("fastly", run, records),
        _ => {}
    }
    require(name == "spin", "wire suite requires Spin")?;
    let repo = root().join("../../..").canonicalize()?;
    let executable = executable("SPIN_BIN", "spin").ok_or("Spin runtime unavailable")?;
    let version = Command::new(&executable).arg("--version").output()?;
    require(version.status.success(), "Spin version probe failed")?;
    let version = String::from_utf8(version.stdout)?;
    let pins = fs::read_to_string(repo.join(".tool-versions"))?;
    let pin = pins
        .lines()
        .find_map(|line| line.strip_prefix("spin "))
        .ok_or("missing Spin pin")?;
    require(
        version.split_whitespace().nth(1) == Some(pin),
        "Spin runtime does not match repository pin",
    )?;
    records.push(json!({"event":"runtime","adapter":"spin","version":version.trim(),"executable":executable,"qualification":false}))?;
    checked(
        Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .current_dir(&repo)
            .env("CARGO_TARGET_DIR", repo.join("target"))
            .args([
                "build",
                "--offline",
                "--locked",
                "-p",
                "edgezero-adapter-spin",
                "--no-default-features",
                "--features",
                "spin,test-utils",
                "--target",
                "wasm32-wasip2",
                "--example",
                "outbound-bodyless-wire",
            ]),
    )?;
    let relative_wasm = "target/wasm32-wasip2/debug/examples/outbound_bodyless_wire.wasm";
    let wasm = run.join(relative_wasm);
    copy_tree(&repo.join(relative_wasm), &wasm)?;
    let relative_manifest =
        "crates/edgezero-adapter-spin/examples/outbound-bodyless-wire.spin.toml";
    let manifest = run.join(relative_manifest);
    copy_tree(&repo.join(relative_manifest), &manifest)?;
    records.push(json!({"event":"build","adapter":"spin","artifact":artifact(&wasm)?,"root_lockfile":artifact(&repo.join("Cargo.lock"))?,"qualification":false}))?;
    let origins = WireOrigins::start()?;
    let port = net::port()?;
    let state = run.join("spin-wire-state");
    let component_logs = run.join("component-logs");
    let log = run.join("spin-wire.log");
    let mut command = Command::new(executable);
    command
        .args(["up", "--from"])
        .arg(manifest)
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .arg("--state-dir")
        .arg(&state)
        .arg("--log-dir")
        .arg(&component_logs)
        .arg("--disable-cache");
    let mut process = None;
    let outcome: Result<(usize, usize)> = (|| {
        let running = process.insert(Process::start(&mut command, &log, port)?);
        controls(port, &origins, &component_logs, running)?;
        let mut matrix = Matrix {
            adapter: "spin",
            endpoint: "/p5",
            port,
            origins: &origins,
            records,
            exchanges: 0,
            dispatches: 0,
        };
        matrix.run()?;
        running.check_running()?;
        Ok((matrix.exchanges, matrix.dispatches))
    })();
    let (exchanges, dispatches) = finish_wire(
        "spin",
        &mut process,
        Some(origins),
        &state,
        records,
        outcome,
    )?;
    records.push(json!({"event":"spin-wire-inventory","status":"pass","exchanges":exchanges,"origin_requests":dispatches,
        "bodyless_controls":4,"upload_controls":1,"scope_controls":1,"handoff_controls":1,"qualification":false}))
}

fn progress_wire(name: &'static str, run: &Path, records: &mut Records) -> Result<()> {
    let result = progress_suite(
        name,
        "outbound-wire",
        run,
        records,
        |port, origins, records| {
            let mut matrix = Matrix {
                adapter: name,
                endpoint: "/progress/redirect",
                port,
                origins,
                records,
                exchanges: 0,
                dispatches: 0,
            };
            matrix.run()?;
            Ok((matrix.exchanges, matrix.dispatches))
        },
    )?;
    if let Some((exchanges, dispatches)) = result {
        records.push(
            json!({"event":format!("{name}-wire-inventory"),"status":"pass",
            "exchanges":exchanges,"origin_requests":dispatches,"qualification":false}),
        )?;
    }
    Ok(())
}

pub(super) fn progress_suite<T>(
    name: &str,
    suite: &str,
    run: &Path,
    records: &mut Records,
    check: impl FnOnce(u16, &WireOrigins, &mut Records) -> Result<T>,
) -> Result<Option<T>> {
    let workerd = if name == "cloudflare" {
        let Some(workerd) = executable("MINIFLARE_WORKERD_PATH", "workerd") else {
            records.push(json!({"status":"unsupported","adapter":"cloudflare",
                "reason":"set MINIFLARE_WORKERD_PATH to the local workerd executable","qualification":false}))?;
            return Ok(None);
        };
        let version = Command::new(&workerd).arg("--version").output()?;
        require(version.status.success(), "workerd version probe failed")?;
        records.push(json!({"event":"workerd-runtime","adapter":"cloudflare",
            "version":String::from_utf8(version.stdout)?.trim(),"artifact":artifact(&workerd)?,"qualification":false}))?;
        Some(workerd)
    } else {
        None
    };
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root())
        .output()?;
    require(
        revision.status.success(),
        "cannot determine wire source revision",
    )?;
    let revision = String::from_utf8(revision.stdout)?;
    let state = if name == "axum" {
        run.join("axum-state")
    } else {
        run.join("retained/worker-state")
    };
    let log = run.join(format!("{name}-{suite}.log"));
    let mut process = None;
    let mut origins = None;
    let outcome: Result<Option<T>> = (|| {
        let Some((mut command, port)) =
            progress::build(name, run, records, revision.trim(), suite)?
        else {
            return Ok(None);
        };
        if let Some(workerd) = workerd {
            command
                .env("MINIFLARE_WORKERD_PATH", workerd)
                .env("WRANGLER_SEND_METRICS", "false")
                .env("WRANGLER_LOG_PATH", run.join("wrangler-logs"));
            records.push(
                json!({"event":"workerd-configuration","adapter":"cloudflare",
                "configuration":fs::read_to_string(run.join("wrangler.toml"))?,"qualification":false}),
            )?;
        }
        let origins = origins.insert(WireOrigins::start()?);
        let running = process.insert(Process::start(&mut command, &log, port)?);
        let result = check(port, origins, records)?;
        running.check_running()?;
        Ok(Some(result))
    })();
    finish_wire(name, &mut process, origins, &state, records, outcome)
}

fn finish_wire<T>(
    adapter: &str,
    process: &mut Option<Process>,
    origins: Option<WireOrigins>,
    state: &Path,
    records: &mut Records,
    outcome: Result<T>,
) -> Result<T> {
    let process_result = process.as_mut().map_or(Ok(()), Process::finish);
    let origin_result = origins.map_or(Ok(()), WireOrigins::finish);
    let state_result: Result<()> = if state.exists() {
        fs::remove_dir_all(state).map_err(Into::into)
    } else {
        Ok(())
    };
    for (stage, result) in [
        ("process", &process_result),
        ("origin", &origin_result),
        ("state", &state_result),
    ] {
        if let Err(error) = result {
            records.push(json!({"status":"fail","adapter":adapter,"stage":stage,
                "error":error.to_string(),"qualification":false}))?;
        }
    }
    let inventory = outcome?;
    process_result.and(origin_result).and(state_result)?;
    Ok(inventory)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_startup_failure_removes_partial_state_and_preserves_primary_error() {
        let run = std::env::temp_dir().join(format!("wire-cleanup-{}", token()));
        let state = run.join("retained/worker-state");
        fs::create_dir_all(&state).unwrap();
        fs::write(state.join("partial-value"), b"synthetic").unwrap();
        let mut records = Records {
            path: run.join("events.jsonl"),
            values: Vec::new(),
        };
        let outcome: Result<()> = Err("build failed before runtime startup".into());
        let error =
            finish_wire("cloudflare", &mut None, None, &state, &mut records, outcome).unwrap_err();
        assert_eq!(error.to_string(), "build failed before runtime startup");
        assert!(!state.exists());
        assert!(records.values.is_empty());
        fs::remove_dir_all(run).unwrap();
    }

    #[test]
    fn late_origin_failure_is_not_hidden_by_success_or_primary_check_failure() {
        for primary in [None, Some("primary lifetime check failed")] {
            let run = std::env::temp_dir().join(format!("wire-late-cleanup-{}", token()));
            let state = run.join("worker-state");
            fs::create_dir_all(&state).unwrap();
            let mut records = Records {
                path: run.join("events.jsonl"),
                values: Vec::new(),
            };
            let origins = WireOrigins::start().unwrap();
            origins.arm("invalid++");
            let outcome = primary.map_or(Ok(()), |error| Err(error.into()));
            let error = finish_wire(
                "axum",
                &mut None,
                Some(origins),
                &state,
                &mut records,
                outcome,
            )
            .unwrap_err();
            assert_eq!(error.to_string(), primary.unwrap_or("invalid P5 gate name"));
            assert!(!state.exists());
            assert_eq!(records.values.len(), 1);
            assert_eq!(records.values[0]["stage"], "origin");
            assert_eq!(records.values[0]["status"], "fail");
            fs::remove_dir_all(run).unwrap();
        }
    }

    #[test]
    fn source_error_expectations_are_provider_specific() {
        assert_eq!(source_error_reason("spin"), "Protocol");
        assert_eq!(source_error_reason("cloudflare"), "Transport");
        assert_eq!(source_error_reason("axum"), "Transport");
        assert_eq!(source_error_reason("fastly"), "Transport");
    }

    #[test]
    fn workerd_source_errors_require_the_observed_typed_reason_phase_and_cause() {
        let valid = json!({"outcome":{"error":{"kind":"bad_gateway","status":502,
            "reason":"Transport","provider":"Cloudflare { phase: ResponseBodyRead, cause: Unknown }"}}});
        source_error(&valid, "cloudflare").unwrap();
        for (field, value) in [
            ("reason", json!("Protocol")),
            ("provider", Value::Null),
            (
                "provider",
                json!("Cloudflare { phase: Fetch, cause: Unknown }"),
            ),
            (
                "provider",
                json!("Cloudflare { phase: ResponseBodyRead, cause: TypeError }"),
            ),
            ("status", json!(500)),
        ] {
            let mut changed = valid.clone();
            changed["outcome"]["error"][field] = value;
            assert!(source_error(&changed, "cloudflare").is_err(), "{changed}");
        }
    }

    #[test]
    fn handoff_retains_exact_bytes_and_all_results() {
        for bytes in [b"originbid-Abid-Bbid-C", b"originbid-Cbid-Abid-B"] {
            handoff(bytes).unwrap();
        }
        for index in [0, 6, 11, 16] {
            let mut altered = b"originbid-Abid-Bbid-C".to_vec();
            altered[index] |= 0x80;
            assert!(handoff(&altered).is_err());
        }
        assert!(handoff(b"originbid-Abid-Abid-C").is_err());
        assert!(handoff(b"origin").is_err());
    }

    #[test]
    fn body_capture_does_not_clamp_or_erase_nontext_bytes() {
        assert_eq!(
            body(&json!({"body_bytes":[0,255,128]})).unwrap(),
            [0, 255, 128]
        );
        assert!(body(&json!({"body_bytes":[256]})).is_err());
        assert!(body(&json!({})).is_err());
    }
}

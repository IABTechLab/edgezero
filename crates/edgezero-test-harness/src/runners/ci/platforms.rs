use super::*;

fn guarded(options: &[&str], arguments: &[&str]) -> Command {
    let mut command = Command::new("bash");
    command
        .arg("scripts/run_test_nonzero.sh")
        .args(options)
        .arg("cargo")
        .args(arguments);
    command
}

fn cloudflare_runner(configured: Option<std::ffi::OsString>) -> std::ffi::OsString {
    configured.unwrap_or_else(|| "wasm-bindgen-test-runner".into())
}

fn nested(adapter: &str, suite: &str, output: &Path) -> Result<Command> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["--adapter", adapter, "--suite", suite, "--output"])
        .arg(output);
    Ok(command)
}

fn validate_wire(events: &[Value], adapter: &str) -> Result<()> {
    require(
        !events.is_empty() && events.iter().all(|event| event["status"] != "fail"),
        "missing or failed redirect wire evidence",
    )?;
    let inventory_name = format!("{adapter}-wire-inventory");
    let case_name = format!("{adapter}-wire-case");
    let inventories = events
        .iter()
        .filter(|event| event["event"] == inventory_name)
        .collect::<Vec<_>>();
    require(
        inventories.len() == 1
            && inventories[0]["status"] == "pass"
            && inventories[0]["exchanges"] == 68
            && inventories[0]["origin_requests"] == 84
            && inventories[0]["qualification"] == false,
        "invalid redirect wire inventory",
    )?;
    let cases = events
        .iter()
        .filter(|event| event["event"] == case_name)
        .collect::<Vec<_>>();
    let receipts = cases
        .iter()
        .map(|case| case["origin_requests"].as_array().map(Vec::len))
        .collect::<Option<Vec<_>>>()
        .ok_or("missing redirect origin receipts")?;
    require(
        cases.len() == 68 && receipts.iter().sum::<usize>() == 84,
        "incomplete redirect wire cases or origin receipts",
    )?;
    super::super::wire::validate_batch_artifact(events, adapter)?;
    super::super::wire::validate_marker_artifact(events, adapter)?;
    if adapter != "axum" {
        super::super::wire::validate_forwarding_artifact(events, adapter)?;
    }
    Ok(())
}

fn validate_progress(events: &[Value]) -> Result<()> {
    require(
        !events.is_empty() && events.iter().all(|event| event["status"] != "fail"),
        "missing or failed Fastly progress evidence",
    )?;
    for (name, probes, receipts) in [
        ("fastly-client-progress-inventory", 12, 18),
        ("fastly-p5-inventory", 23, 43),
    ] {
        let inventories = events
            .iter()
            .filter(|event| event["event"] == name)
            .collect::<Vec<_>>();
        require(
            inventories.len() == 1
                && inventories[0]["status"] == "pass"
                && inventories[0]["probes"] == probes
                && inventories[0]["expected_probes"] == probes
                && inventories[0]["origin_requests"] == receipts
                && inventories[0]["qualification"] == false,
            "invalid Fastly client progress inventory",
        )?;
    }
    require(
        events
            .iter()
            .any(|event| event["status"] == "unverified" && event["qualification"] == false),
        "Fastly progress qualification disposition must remain unverified",
    )
}

pub(super) fn run(args: &Args, output: &Path, records: &mut Records) -> Result<()> {
    let adapter = args.adapter.as_str();
    let repo = repository_root();
    match adapter {
        "axum" => {
            phase(
                adapter,
                "native-tests",
                &mut guarded(
                    &[
                        "--include-ignored",
                        "--inventory",
                        "axum/native-tcp,unit/axum,axum/http1,axum",
                    ],
                    &[
                        "test",
                        "--offline",
                        "--locked",
                        "-p",
                        "edgezero-adapter-axum",
                        "--no-default-features",
                        "--features",
                        "axum,test-utils",
                        "--all-targets",
                    ],
                ),
                &repo,
                output,
                records,
            )?;
            phase(
                adapter,
                "fixture-check",
                Command::new("cargo")
                    .args(["check", "--offline", "--locked", "--manifest-path"])
                    .arg(fixture_root().join("Cargo.toml"))
                    .args(["-p", "fixture-axum"]),
                &repo,
                output,
                records,
            )?;
        }
        "cloudflare" => {
            let runner = cloudflare_runner(std::env::var_os(
                "CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER",
            ));
            phase(
                adapter,
                "native-contracts",
                &mut guarded(
                    &["batch_preflight_precedence_and_indices"],
                    &[
                        "test",
                        "--offline",
                        "--locked",
                        "-p",
                        "edgezero-adapter-cloudflare",
                        "--no-default-features",
                        "--features",
                        "test-utils",
                        "--test",
                        "contract",
                    ],
                ),
                &repo,
                output,
                records,
            )?;
            phase(
                adapter,
                "guest-shim-tests",
                Command::new("node").args([
                    "--test",
                    "crates/edgezero-adapter-cloudflare/tests/worker_runtime.test.mjs",
                ]),
                &repo,
                output,
                records,
            )?;
            phase(
                adapter,
                "sdk-tests",
                guarded(
                    &["dispatch_runs_router_and_returns_response"],
                    &[
                        "test",
                        "--offline",
                        "--locked",
                        "-p",
                        "edgezero-adapter-cloudflare",
                        "--no-default-features",
                        "--features",
                        "cloudflare,test-utils",
                        "--target",
                        "wasm32-unknown-unknown",
                        "--test",
                        "contract",
                    ],
                )
                .env("CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER", &runner)
                .env_remove("HEADLESS"),
                &repo,
                output,
                records,
            )?;
            phase(
                adapter,
                "browser-ingress",
                guarded(
                    &["ingress_preserves_runtime_extension_methods_and_header_strings"],
                    &[
                        "test",
                        "--offline",
                        "--locked",
                        "-p",
                        "edgezero-adapter-cloudflare",
                        "--no-default-features",
                        "--features",
                        "cloudflare,test-utils",
                        "--target",
                        "wasm32-unknown-unknown",
                        "--test",
                        "ingress_fidelity",
                    ],
                )
                .env("CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER", &runner)
                .env("HEADLESS", "1"),
                &repo,
                output,
                records,
            )?;
            phase(
                adapter,
                "runtime-unit-tests",
                guarded(
                    &["--inventory", "unit/cloudflare"],
                    &[
                        "test",
                        "--offline",
                        "--locked",
                        "-p",
                        "edgezero-adapter-cloudflare",
                        "--no-default-features",
                        "--features",
                        "cloudflare,test-utils",
                        "--target",
                        "wasm32-unknown-unknown",
                        "--lib",
                    ],
                )
                .env("CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER", &runner)
                .env_remove("HEADLESS"),
                &repo,
                output,
                records,
            )?;
        }
        "fastly" => {
            phase(
                adapter,
                "native-contracts",
                &mut guarded(
                    &["batch_preflight_rejects_streamed_slots_without_poisoning_siblings"],
                    &[
                        "test",
                        "--offline",
                        "--locked",
                        "-p",
                        "edgezero-adapter-fastly",
                        "--no-default-features",
                        "--features",
                        "test-utils",
                        "--test",
                        "contract",
                    ],
                ),
                &repo,
                output,
                records,
            )?;
            let viceroy = executable("VICEROY_BIN", "viceroy").ok_or("Viceroy is not installed")?;
            let runner = format!("{} run", viceroy.display());
            phase(adapter, "sdk-tests", guarded(
                &["dispatch_runs_router_and_returns_response\nclosed_lifecycle_orders_hooks_and_returns_state_after_delivery"],
                &["test", "--offline", "--locked", "-p", "edgezero-adapter-fastly", "--no-default-features", "--features", "fastly,test-utils", "--target", "wasm32-wasip1", "--test", "contract"],
            ).env("CARGO_TARGET_WASM32_WASIP1_RUNNER", &runner), &repo, output, records)?;
            phase(
                adapter,
                "runtime-unit-tests",
                guarded(
                    &["--inventory", "unit/fastly"],
                    &[
                        "test",
                        "--offline",
                        "--locked",
                        "-p",
                        "edgezero-adapter-fastly",
                        "--no-default-features",
                        "--features",
                        "fastly,test-utils",
                        "--target",
                        "wasm32-wasip1",
                        "--lib",
                    ],
                )
                .env("CARGO_TARGET_WASM32_WASIP1_RUNNER", runner),
                &repo,
                output,
                records,
            )?;
        }
        _ => return Err("unsupported integration CI adapter".into()),
    }
    if adapter != "axum" {
        let target = if adapter == "fastly" {
            "wasm32-wasip1"
        } else {
            "wasm32-unknown-unknown"
        };
        phase(
            adapter,
            "feature-matrix",
            Command::new("bash").args(["scripts/check_adapter_feature_matrix.sh", adapter, target]),
            &repo,
            output,
            records,
        )?;
        phase(
            adapter,
            "ingress",
            &mut nested(adapter, "ingress-fidelity", &output.join("ingress"))?,
            &repo,
            output,
            records,
        )?;
        phase(
            adapter,
            "fixture-check",
            Command::new("cargo")
                .args(["check", "--offline", "--locked", "--manifest-path"])
                .arg(fixture_root().join("Cargo.toml"))
                .args([
                    "-p",
                    &format!("fixture-{adapter}"),
                    "--target",
                    target,
                    "--all-targets",
                ]),
            &repo,
            output,
            records,
        )?;
    }
    if adapter == "fastly" {
        phase(
            adapter,
            "smoke",
            &mut nested(adapter, "smoke", &output.join("smoke"))?,
            &repo,
            output,
            records,
        )?;
        let progress = output.join("client-progress");
        phase_with_exit(
            adapter,
            "client-progress",
            &mut nested(adapter, "outbound-client-progress", &progress)?,
            &repo,
            output,
            records,
            2,
        )?;
        validate_progress(&read_events(&progress.join("events.jsonl"))?)?;
    }
    if matches!(adapter, "axum" | "fastly") {
        let wire = output.join("wire");
        phase(
            adapter,
            "wire",
            &mut nested(adapter, "outbound-wire", &wire)?,
            &repo,
            output,
            records,
        )?;
        validate_wire(&read_events(&wire.join("events.jsonl"))?, adapter)?;
    }
    records.push(
        json!({"event":"ci-inventory","adapter":adapter,"status":"pass","qualification":false}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloudflare_preserves_explicit_cargo_runner_selection() {
        assert_eq!(cloudflare_runner(None), "wasm-bindgen-test-runner");
        for configured in ["/locked/tools/wasm-bindgen-test-runner", ""] {
            assert_eq!(cloudflare_runner(Some(configured.into())), configured);
        }
    }

    fn wire(adapter: &str) -> Vec<Value> {
        let mut cases = (0..68).map(|index| json!({"event":format!("{adapter}-wire-case"),"origin_requests":if index < 16 {vec![json!({}),json!({})]} else {vec![json!({})]}})).collect::<Vec<_>>();
        cases.push(json!({"event":format!("{adapter}-wire-inventory"),"status":"pass","exchanges":68,"origin_requests":84,"qualification":false}));
        cases.extend(super::super::super::wire::batch_recovery_artifact(adapter));
        cases.extend(super::super::super::wire::marker_artifact(adapter));
        if adapter != "axum" {
            cases.extend(super::super::super::wire::forwarding_artifact(adapter));
        }
        cases
    }

    #[test]
    fn persisted_wire_requires_all_cases_and_receipts_from_the_selected_adapter() {
        for adapter in ["axum", "fastly"] {
            let events = wire(adapter);
            validate_wire(&events, adapter).unwrap();
            assert!(validate_wire(&events, "cloudflare").is_err());
            let mut changed = events.clone();
            changed.pop();
            assert!(validate_wire(&changed, adapter).is_err());
            if adapter != "axum" {
                let without_forwarding = events
                    .iter()
                    .filter(|event| {
                        !event["event"]
                            .as_str()
                            .is_some_and(|name| name.starts_with("inbound-forwarding-"))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                assert!(validate_wire(&without_forwarding, adapter).is_err());
            }
            let mut changed = events.clone();
            changed.remove(0);
            assert!(validate_wire(&changed, adapter).is_err());
            let mut changed = events.clone();
            changed[0]["origin_requests"] = json!([]);
            assert!(validate_wire(&changed, adapter).is_err());
            let mut changed = events.clone();
            changed.push(events.last().unwrap().clone());
            assert!(validate_wire(&changed, adapter).is_err());
            let mut changed = events.clone();
            changed[0]["status"] = json!("fail");
            assert!(validate_wire(&changed, adapter).is_err());
        }
    }

    #[test]
    fn progress_keeps_both_inventories_and_the_explicit_unqualified_disposition() {
        let events = vec![
            json!({"event":"fastly-client-progress-inventory","status":"pass","probes":12,"expected_probes":12,"origin_requests":18,"qualification":false}),
            json!({"event":"fastly-p5-inventory","status":"pass","probes":23,"expected_probes":23,"origin_requests":43,"qualification":false}),
            json!({"status":"unverified","qualification":false}),
        ];
        validate_progress(&events).unwrap();
        for index in 0..events.len() {
            let mut changed = events.clone();
            changed.remove(index);
            assert!(validate_progress(&changed).is_err());
        }
        for field in [
            "probes",
            "expected_probes",
            "origin_requests",
            "qualification",
            "status",
        ] {
            let mut changed = events.clone();
            changed[1][field] = json!(null);
            assert!(validate_progress(&changed).is_err(), "{field}");
        }
        let mut changed = events.clone();
        changed.push(json!({"status":"fail"}));
        assert!(validate_progress(&changed).is_err());
    }
}

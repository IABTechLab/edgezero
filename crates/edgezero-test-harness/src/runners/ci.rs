use super::*;

mod platforms;

const ARTIFACT_BYTES: u64 = 4 * 1024 * 1024;

fn read_artifact(path: &Path) -> Result<String> {
    let mut text = String::new();
    fs::File::open(path)?
        .take(ARTIFACT_BYTES + 1)
        .read_to_string(&mut text)?;
    require(
        !text.is_empty() && text.len() as u64 <= ARTIFACT_BYTES,
        "missing or oversized CI artifact",
    )?;
    Ok(text)
}

pub(super) fn read_events(path: &Path) -> Result<Vec<Value>> {
    let text = read_artifact(path)?;
    let events = text
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    require(events.len() <= 512, "too many CI artifact records")?;
    Ok(events)
}

fn validate_ingress(path: &Path, baseline: &Path) -> Result<()> {
    let observed: Value = serde_json::from_str(&read_artifact(path)?)?;
    let expected: Value = serde_json::from_str(&read_artifact(baseline)?)?;
    require(
        observed.as_object().is_some_and(|rows| rows.len() == 1)
            && expected["spin"]
                .as_object()
                .is_some_and(|rows| !rows.is_empty())
            && observed["spin"] == expected["spin"],
        "persisted Spin ingress observations differ from the baseline",
    )
}

fn validate_markers(
    inventory: &Value,
    native_log: &str,
    sdk_log: &str,
    async_log: &str,
    wire_log: &str,
) -> Result<()> {
    let cases = inventory["cases"]
        .as_array()
        .ok_or("missing acceptance inventory")?;
    let mut required = 0;
    for case in cases
        .iter()
        .filter(|case| case["environment"] == "unit/spin" && case["implementation"] == "ready")
    {
        let log = match case["case_id"].as_str() {
            Some("EZ-2/acceptance/01" | "EZ-3/acceptance/08") => sdk_log,
            Some("EZ-5/acceptance/09") => async_log,
            Some("EZ-5/acceptance/10") => wire_log,
            Some("EZ-5/acceptance/11") => wire_log,
            _ => return Err("ready Spin criterion has no implemented CI phase".into()),
        };
        let mut names = vec![case["sentinel"].as_str().ok_or("missing Spin sentinel")?];
        if let Some(additional) = case.get("additional_sentinels") {
            for name in additional
                .as_array()
                .ok_or("invalid additional Spin sentinels")?
            {
                names.push(name.as_str().ok_or("invalid additional Spin sentinel")?);
            }
        }
        if case["case_id"] == "EZ-5/acceptance/11" {
            require(names.len() == 4 && [
                "ez_5_acceptance_11_wire_spin",
                "outbound::head::tests::adapter_visible_normalization_preserves_policy_precedence",
                "outbound::head::tests::adapter_visible_bodyless_and_reset_dispositions_precede_caps",
                "ez_5_acceptance_11_native_spin",
            ].iter().all(|name| names.contains(name)), "normalization criterion must retain all owning-phase proofs")?;
        }
        for name in names {
            require(
                !name.is_empty() && !name.contains(char::is_whitespace),
                "invalid Spin sentinel name",
            )?;
            let (log, expected) = match (case["case_id"].as_str(), name) {
                (Some("EZ-2/acceptance/01" | "EZ-3/acceptance/08"), _) => {
                    (log, format!("test {name} ... ok"))
                }
                (
                    Some("EZ-5/acceptance/11"),
                    "outbound::head::tests::adapter_visible_normalization_preserves_policy_precedence"
                    | "outbound::head::tests::adapter_visible_bodyless_and_reset_dispositions_precede_caps",
                ) => (native_log, format!("test {name} ... ok")),
                (Some("EZ-5/acceptance/11"), "ez_5_acceptance_11_native_spin") => {
                    (async_log, format!("{name}: passed"))
                }
                _ => (log, format!("{name}: passed")),
            };
            require(
                log.lines()
                    .any(|line| line.trim_end_matches('\r') == expected),
                format!("Spin CI marker not passed in its owning phase: {name}"),
            )?;
            required += 1;
        }
    }
    require(required > 0, "no implemented Spin CI criteria")
}

fn phase(
    adapter: &str,
    name: &str,
    command: &mut Command,
    repo: &Path,
    run: &Path,
    records: &mut Records,
) -> Result<PathBuf> {
    phase_with_exit(adapter, name, command, repo, run, records, 0)
}

fn phase_with_exit(
    adapter: &str,
    name: &str,
    command: &mut Command,
    repo: &Path,
    run: &Path,
    records: &mut Records,
    expected_exit: i32,
) -> Result<PathBuf> {
    let log = run.join(format!("{name}.log"));
    let start = Instant::now();
    eprintln!("{adapter} CI: {name}");
    command.current_dir(repo).stdin(Stdio::null());
    let outcome = Process::spawn(command, &log)?.wait_exit_code(expected_exit);
    records.push(json!({"event":"ci-phase","adapter":adapter,"phase":name,
        "status":if outcome.is_ok() {"pass"} else {"fail"},"elapsed_ms":start.elapsed().as_millis(),
        "log":log,"expected_exit":expected_exit,"qualification":false}))?;
    if outcome.is_err()
        && let Ok(file) = fs::File::open(&log)
    {
        let mut excerpt = Vec::new();
        if file.take(64 * 1024).read_to_end(&mut excerpt).is_ok() {
            eprintln!("{}", String::from_utf8_lossy(&excerpt));
        }
    }
    outcome?;
    Ok(log)
}

fn pinned_tool(
    variable: &str,
    name: &str,
    repo: &Path,
    run: &Path,
    records: &mut Records,
) -> Result<PathBuf> {
    let executable = executable(variable, name).ok_or(format!("{name} is not installed"))?;
    let pin = fs::read_to_string(repo.join(".tool-versions"))?
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")).map(str::to_owned))
        .ok_or(format!("missing {name} pin"))?;
    let log = phase(
        "spin",
        &format!("{name}-version"),
        Command::new(&executable).arg("--version"),
        repo,
        run,
        records,
    )?;
    let version = fs::read_to_string(log)?;
    require(
        version.split_whitespace().nth(1) == Some(pin.trim()),
        format!("{name} does not match the repository pin"),
    )?;
    records.push(json!({"event":"runtime","adapter":"spin","tool":name,"version":pin.trim(),"executable":executable,"qualification":false}))?;
    Ok(executable)
}

pub(crate) fn run(args: &Args, run: &Path, records: &mut Records) -> Result<()> {
    if args.adapter != "spin" {
        return platforms::run(args, run, records);
    }
    require(
        args.adapter == "spin",
        "CI runtime suite requires the Spin adapter",
    )?;
    let repo = repository_root();
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let wasmtime = pinned_tool("WASMTIME_BIN", "wasmtime", &repo, run, records)?;
    let spin = pinned_tool("SPIN_BIN", "spin", &repo, run, records)?;

    phase(
        "spin",
        "feature-matrix",
        Command::new("bash").args([
            "scripts/check_adapter_feature_matrix.sh",
            "spin",
            "wasm32-wasip2",
        ]),
        &repo,
        run,
        records,
    )?;
    let native_log = phase(
        "spin",
        "native-tests",
        Command::new("bash").args([
            "scripts/run_test_nonzero.sh",
            "batch_preflight_precedence_and_indices\noutbound::head::tests::adapter_visible_normalization_preserves_policy_precedence\noutbound::head::tests::adapter_visible_bodyless_and_reset_dispositions_precede_caps",
            "cargo",
            "test",
            "--offline",
            "--locked",
            "-p",
            "edgezero-adapter-spin",
            "--no-default-features",
            "--features",
            "test-utils",
            "--all-targets",
        ]),
        &repo,
        run,
        records,
    )?;
    let sdk_log = phase(
        "spin",
        "sdk-tests",
        Command::new("bash")
            .args([
                "scripts/run_test_nonzero.sh",
                "router_dispatches_get_and_returns_response\nspin_error_code_table_is_exhaustive",
            ])
            .arg(&cargo)
            .args([
                "test",
                "--offline",
                "--locked",
                "-p",
                "edgezero-adapter-spin",
                "--no-default-features",
                "--features",
                "spin,test-utils",
                "--target",
                "wasm32-wasip2",
                "--test",
                "contract",
                "--test",
                "sdk_resources",
            ])
            .env(
                "CARGO_TARGET_WASM32_WASIP2_RUNNER",
                format!(
                    "{} run -W component-model-async=y -S p3=y -S http=y",
                    wasmtime.display()
                ),
            ),
        &repo,
        run,
        records,
    )?;

    let fixtures = fixture_root();
    phase(
        "spin",
        "shared-sdk-tests",
        Command::new("bash")
            .args([
                "scripts/run_test_nonzero.sh",
                "tests::ingress_contract::deferred_request_and_response_paths_retain_the_injected_clock\ntests::ingress_contract::converted_response_retains_injected_clock\ntests::ingress_contract::converted_response_preserves_original_content_type_provenance\ntests::sdk_resources::response_conversion_checks_deadline_before_handoff\ntests::sdk_resources::provider_diagnostics_survive_ready_deadlines\ntests::clock_tests::response_conversion_checks_deadline_before_handoff\ntests::clock_tests::converted_response_retains_injected_clock\ntests::clock_tests::converted_response_preserves_original_content_type_provenance",
                "cargo", "test", "--offline", "--locked", "--manifest-path",
            ])
            .arg(fixtures.join("Cargo.toml"))
            .args(["-p", "fixture-spin-support", "--target", "wasm32-wasip2", "--lib"])
            .env("CARGO_TARGET_WASM32_WASIP2_RUNNER", format!("{} run -W component-model-async=y -S p3=y -S http=y", wasmtime.display())),
        &repo,
        run,
        records,
    )?;
    phase(
        "spin",
        "async-build",
        Command::new(&cargo)
            .args(["build", "--offline", "--locked", "--manifest-path"])
            .arg(fixtures.join("Cargo.toml"))
            .env("CARGO_TARGET_DIR", fixtures.join("target"))
            .args(["-p", "fixture-spin-async", "--target", "wasm32-wasip2"]),
        &repo,
        run,
        records,
    )?;
    let component = fixtures.join("target/wasm32-wasip2/debug/fixture_spin_async.wasm");
    require(
        component.is_file(),
        "async fixture build did not produce its component",
    )?;
    records.push(json!({"event":"build","adapter":"spin","phase":"async","artifact":artifact(&component)?,"qualification":false}))?;
    let async_log = phase(
        "spin",
        "async",
        Command::new(&wasmtime)
            .args([
                "run",
                "-W",
                "component-model-async=y",
                "-S",
                "p3=y",
                "-S",
                "http=y",
            ])
            .arg(&component),
        &repo,
        run,
        records,
    )?;

    let wire_dir = run.join("wire");
    let wire_log = phase(
        "spin",
        "wire",
        Command::new(std::env::current_exe()?)
            .args(["--adapter", "spin", "--suite", "outbound-wire", "--output"])
            .arg(&wire_dir)
            .env("SPIN_BIN", &spin),
        &repo,
        run,
        records,
    )?;
    super::wire::validate_spin_artifact(&read_events(&wire_dir.join("events.jsonl"))?)?;
    let inventory: Value = serde_json::from_slice(&fs::read(
        acceptance_root().join("outbound-cutover-cases.json"),
    )?)?;
    validate_markers(
        &inventory,
        &fs::read_to_string(native_log)?,
        &fs::read_to_string(sdk_log)?,
        &fs::read_to_string(async_log)?,
        &fs::read_to_string(wire_log)?,
    )?;

    phase(
        "spin",
        "ingress",
        Command::new(std::env::current_exe()?)
            .args([
                "--adapter",
                "spin",
                "--suite",
                "ingress-fidelity",
                "--output",
            ])
            .arg(run.join("ingress"))
            .arg("--readiness-timeout")
            .arg(args.readiness_timeout.as_secs_f64().to_string())
            .env("SPIN_BIN", &spin),
        &repo,
        run,
        records,
    )?;
    validate_ingress(
        &run.join("ingress/observations.json"),
        &repo.join("crates/edgezero-test-harness/fixtures/request-fidelity/observations.json"),
    )?;
    records.push(json!({"event":"spin-ci-inventory","status":"pass","qualification":false}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_bytes_do_not_replace_the_failed_command_status() {
        let directory = tempfile::tempdir().unwrap();
        let run = directory.path();
        let mut records = Records {
            path: run.join("events.jsonl"),
            values: Vec::new(),
        };
        let error = phase(
            "spin",
            "failure",
            Command::new("sh").args(["-c", "printf '\\377'; exit 23"]),
            run,
            run,
            &mut records,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("23"), "original failure lost: {error}");
    }

    #[test]
    fn ingress_readback_requires_the_complete_unchanged_selected_baseline() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("observations.json");
        let baseline = directory.path().join("baseline.json");
        fs::write(
            &baseline,
            json!({"spin":{"case":true},"fastly":{"case":false}}).to_string(),
        )
        .unwrap();
        assert!(validate_ingress(&path, &baseline).is_err());
        for observed in [
            json!({}),
            json!({"spin":null}),
            json!({"spin":{"case":false}}),
            json!({"spin":{"case":true},"fastly":{}}),
        ] {
            fs::write(&path, observed.to_string()).unwrap();
            assert!(validate_ingress(&path, &baseline).is_err());
        }
        fs::write(&path, json!({"spin":{"case":true}}).to_string()).unwrap();
        validate_ingress(&path, &baseline).unwrap();
        fs::write(&baseline, json!({"spin":{}}).to_string()).unwrap();
        assert!(validate_ingress(&path, &baseline).is_err());
    }

    #[test]
    fn client_precedence_requires_the_sdk_phase() {
        let inventory = json!({"cases":[{
            "case_id":"EZ-2/acceptance/01", "environment":"unit/spin",
            "implementation":"ready", "sentinel":"tests::ingress_contract::router_state_client_overrides_adapter_default"
        }]});
        let passed =
            "test tests::ingress_contract::router_state_client_overrides_adapter_default ... ok\n";
        validate_markers(&inventory, "", passed, "", "").unwrap();
        for (native, sdk, asynchronous, wire) in [
            (passed, "", "", ""),
            ("", "", passed, ""),
            ("", "", "", passed),
            ("", "", "", ""),
            (
                "",
                "test tests::ingress_contract::router_state_client_overrides_adapter_default ... FAILED\n",
                "",
                "",
            ),
        ] {
            assert!(validate_markers(&inventory, native, sdk, asynchronous, wire).is_err());
        }
        for rejected in [
            passed.replace(" ... ok", " ... ignored"),
            passed.trim_end().trim_end_matches("ok").to_owned(),
            format!("prefix {passed}"),
            passed.replace(" ... ok", " ... ok suffix"),
            passed
                .trim_start_matches("test ")
                .replace(" ... ok", ": passed"),
        ] {
            assert!(validate_markers(&inventory, "", &rejected, "", "").is_err());
        }
        for sentinel in [Value::Null, json!(7), json!(""), json!("bad sentinel")] {
            let mut changed = inventory.clone();
            changed["cases"][0]["sentinel"] = sentinel;
            assert!(validate_markers(&changed, "", passed, "", "").is_err());
        }
        let mut missing = inventory.clone();
        missing["cases"][0]
            .as_object_mut()
            .unwrap()
            .remove("sentinel");
        assert!(validate_markers(&missing, "", passed, "", "").is_err());
        for additional in [Value::Null, json!("not an array"), json!([7])] {
            let mut changed = inventory.clone();
            changed["cases"][0]["additional_sentinels"] = additional;
            assert!(validate_markers(&changed, "", passed, "", "").is_err());
        }
    }

    #[test]
    fn markers_require_a_ready_criterion_and_its_own_phase() {
        let inventory = json!({"cases":[
            {"case_id":"EZ-5/acceptance/09","environment":"unit/spin","implementation":"ready","sentinel":"async_case"},
            {"case_id":"EZ-5/acceptance/10","environment":"unit/spin","implementation":"ready","sentinel":"wire_case"}
        ]});
        validate_markers(
            &inventory,
            "",
            "",
            "async_case: passed\n",
            "wire_case: passed\n",
        )
        .unwrap();
        for (async_log, wire_log) in [
            ("", "async_case: passed\nwire_case: passed\n"),
            ("async_case: passed\nwire_case: passed\n", ""),
            (" async_case: passed\n", "wire_case: passed\n"),
            ("async_case: failed\n", "wire_case: passed\n"),
        ] {
            assert!(validate_markers(&inventory, "", "", async_log, wire_log).is_err());
        }
        assert!(validate_markers(&json!({"cases":[]}), "", "", "", "").is_err());
        let mut unknown = inventory;
        unknown["cases"][0]["case_id"] = json!("EZ-5/acceptance/12");
        assert!(
            validate_markers(
                &unknown,
                "",
                "",
                "async_case: passed\n",
                "wire_case: passed\n"
            )
            .is_err()
        );
    }

    #[test]
    fn normalization_markers_require_every_proof_in_its_own_phase() {
        let names = [
            "outbound::head::tests::adapter_visible_normalization_preserves_policy_precedence",
            "outbound::head::tests::adapter_visible_bodyless_and_reset_dispositions_precede_caps",
        ];
        let native = names.map(|name| format!("test {name} ... ok\n")).concat();
        let sdk = "ez_5_acceptance_11_native_spin: passed\n";
        let wire = "ez_5_acceptance_11_wire_spin: passed\n";
        let inventory = json!({"cases":[{"case_id":"EZ-5/acceptance/11","environment":"unit/spin","implementation":"ready","sentinel":"ez_5_acceptance_11_wire_spin","additional_sentinels":[names[0],names[1],"ez_5_acceptance_11_native_spin"]}]});
        validate_markers(&inventory, &native, "", sdk, wire).unwrap();
        for missing in 0..3 {
            let mut changed = inventory.clone();
            changed["cases"][0]["additional_sentinels"]
                .as_array_mut()
                .unwrap()
                .remove(missing);
            assert!(
                validate_markers(&changed, &native, "", sdk, wire).is_err(),
                "omitted mandatory normalization proof {missing}"
            );
        }
        let mut omitted = inventory.clone();
        omitted["cases"][0]
            .as_object_mut()
            .unwrap()
            .remove("additional_sentinels");
        assert!(validate_markers(&omitted, &native, "", sdk, wire).is_err());
        for (native_log, sdk_log, wire_log) in [
            ("", sdk, wire),
            (&native, "", wire),
            (&native, sdk, ""),
            (sdk, &native, wire),
            (wire, sdk, &native),
        ] {
            assert!(validate_markers(&inventory, native_log, "", sdk_log, wire_log).is_err());
        }
        let missing = native.lines().skip(1).collect::<Vec<_>>().join("\n");
        assert!(validate_markers(&inventory, &missing, "", sdk, wire).is_err());
        assert!(
            validate_markers(
                &inventory,
                &native.replace(" ... ok", " ... FAILED"),
                "",
                sdk,
                wire
            )
            .is_err()
        );
    }

    #[test]
    fn optimizer_precedence_requires_the_sdk_phase() {
        let inventory = json!({"cases":[{
            "case_id":"EZ-3/acceptance/08", "environment":"unit/spin",
            "implementation":"ready", "sentinel":"tests::ingress_contract::unsupported_optimizer_precedes_expiry_without_transport"
        }]});
        let passed = "test tests::ingress_contract::unsupported_optimizer_precedes_expiry_without_transport ... ok\n";
        validate_markers(&inventory, "", passed, "", "").unwrap();
        for (native, sdk, asynchronous, wire) in [
            (passed, "", "", ""),
            ("", "", passed, ""),
            ("", "", "", passed),
            ("", "", "", ""),
            (
                "",
                "test tests::ingress_contract::unsupported_optimizer_precedes_expiry_without_transport ... ignored\n",
                "",
                "",
            ),
        ] {
            assert!(validate_markers(&inventory, native, sdk, asynchronous, wire).is_err());
        }
    }

    #[test]
    fn persisted_artifacts_reject_empty_malformed_truncated_and_oversized_jsonl() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("events.jsonl");
        assert!(read_events(&file).is_err());
        for text in ["", "{", "{}\n{", "{}\n\n"] {
            fs::write(&file, text).unwrap();
            assert!(read_events(&file).is_err(), "accepted {text:?}");
        }
        fs::write(&file, format!("{}\n", json!({"event":"pass"}))).unwrap();
        assert_eq!(read_events(&file).unwrap().len(), 1);
        fs::write(&file, "{}\n".repeat(513)).unwrap();
        assert!(read_events(&file).is_err());
        fs::write(&file, vec![b' '; ARTIFACT_BYTES as usize + 1]).unwrap();
        assert!(read_events(&file).is_err());
    }
}

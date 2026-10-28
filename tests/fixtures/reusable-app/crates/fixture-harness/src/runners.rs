use crate::{evidence::*, net::Backend, *};
use std::collections::{BTreeSet, HashMap, HashSet};
mod assembly;
mod framing;
mod overflow;
mod progress;
mod readiness;
mod response_headers;
mod wire;
fn cargo_build() -> Command {
    let mut command = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command
        .current_dir(root())
        .env("CARGO_TARGET_DIR", root().join("target"));
    command
}

fn built_executable(output: &[u8], name: &str) -> Result<PathBuf> {
    String::from_utf8_lossy(output)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|record| {
            (record["reason"] == "compiler-artifact" && record["target"]["name"] == name)
                .then(|| record["executable"].as_str().map(PathBuf::from))
                .flatten()
        })
        .ok_or_else(|| format!("Cargo did not report an executable for {name}").into())
}
/// Copies a built artifact (file or directory) into the run directory, so a
/// concurrent build into the shared target cannot swap it under a running
/// server; the copy is what gets served and fingerprinted.
fn stage(from: &Path, run: &Path) -> Result<PathBuf> {
    let to = run
        .join("artifacts")
        .join(from.file_name().ok_or("artifact has no file name")?);
    copy_tree(from, &to)?;
    Ok(to)
}
fn copy_tree(from: &Path, to: &Path) -> Result<()> {
    if from.is_dir() {
        fs::create_dir_all(to)?;
        for entry in fs::read_dir(from)? {
            let entry = entry?;
            copy_tree(&entry.path(), &to.join(entry.file_name()))?;
        }
    } else {
        fs::create_dir_all(to.parent().ok_or("artifact has no parent")?)?;
        fs::copy(from, to)?;
    }
    Ok(())
}
/// Teardown shared by every runtime run. The runtime lives inside `outcome`'s
/// computation, so it has stopped by now: record client completions and guest
/// logs, then a failure carrying `fields` and `failure` context.
fn settle(
    records: &mut Records,
    log: &Path,
    fields: Value,
    failure: Value,
    outcome: Result<()>,
) -> Result<()> {
    records.collect_clients(fields.clone())?;
    for event in logs(log) {
        records.push(annotate(event, fields.clone()))?;
    }
    if let Err(error) = &outcome {
        records.push(annotate(
            annotate(json!({"status":"fail","error":error.to_string()}), fields),
            failure,
        ))?;
    }
    outcome
}
fn wrangler_runtime_config(package: &Path, main: &Path) -> Result<String> {
    let config = fs::read_to_string(package.join("wrangler.toml"))?;
    let mut skip = false;
    let mut lines = Vec::new();
    for line in config.lines() {
        if line.trim().starts_with('[') {
            skip = line.trim() == "[build]";
        }
        if skip {
            continue;
        }
        lines.push(if line.starts_with("main =") {
            format!("main = {main:?}")
        } else {
            line.to_owned()
        });
    }
    Ok(lines.join("\n") + "\n")
}

fn wasm(run: &Path, variant: &str) -> PathBuf {
    run.join("artifacts").join(wasm_name(variant))
}
fn wasm_name(variant: &str) -> String {
    let name = match variant {
        "a" => "single-request",
        "b" => "rebuild-per-request",
        "c" => "retained-app",
        "custom-a" => "custom-single-request",
        "custom-b" => "custom-rebuild-per-request",
        "custom-c" => "custom-retained-app",
        other => other,
    };
    format!("fixture-fastly-{name}.wasm")
}
fn stage_fastly_artifacts(run: &Path) -> Result<()> {
    let release = root().join("target/wasm32-wasip1/release");
    for variant in [
        "a",
        "b",
        "c",
        "custom-a",
        "custom-b",
        "custom-c",
        "logger-negative",
        "limit-requests",
        "limit-lifetime",
        "limit-memory",
        "faults",
    ] {
        stage(&release.join(wasm_name(variant)), run)?;
    }
    Ok(())
}
fn fastly_command(exe: &Path, config: &Path, run: &Path, variant: &str, port: u16) -> Command {
    let mut cmd = Command::new(exe);
    cmd.args(["serve", "--addr", &format!("127.0.0.1:{port}"), "--config"])
        .arg(config)
        .arg(wasm(run, variant));
    cmd
}

fn custom_initialization_contract(
    exe: &Path,
    config: &Path,
    run: &Path,
    records: &mut Records,
) -> Result<()> {
    let log = run.join("custom-initialization.log");
    let outcome = (|| -> Result<()> {
        let port = net::port()?;
        let _process = Process::start(
            &mut fastly_command(exe, config, run, "custom-c", port),
            &log,
            port,
        )?;
        let mut instances = Vec::new();
        for (path, status, attempts, initialized) in [
            ("/health", 200, 0, false),
            ("/initialization-error", 503, 1, false),
            ("/health", 200, 1, false),
            ("/probe/recovered", 200, 2, true),
            ("/probe/retained", 200, 2, true),
        ] {
            let reply = req(port, path)?;
            require(
                reply["status"] == status,
                "custom initialization status mismatch",
            )?;
            let parsed: Value =
                serde_json::from_str(reply["body"].as_str().ok_or("missing body")?)?;
            let value = if status == 503 {
                require(
                    parsed
                        == json!({"error":{"kind":"service_unavailable","message":"state unavailable","status":503}}),
                    "initialization did not use the canonical error body",
                )?;
                await_evidence(|| {
                    require(
                        logs(&log)
                            .iter()
                            .filter(|event| {
                                event["event"] == "assembly-rejected"
                                    && event["token"] == reply["token"]
                            })
                            .count()
                            == 1,
                        "missing correlated assembly failure",
                    )
                })?;
                logs(&log)
                    .into_iter()
                    .find(|event| {
                        event["event"] == "assembly-rejected" && event["token"] == reply["token"]
                    })
                    .ok_or("missing assembly failure metadata")?
            } else {
                parsed
            };
            instances.push(value["instance"].clone());
            let same_instance = instances.iter().all(|id| id == &instances[0]);
            if same_instance {
                require(
                    value["ordinal"] == instances.len(),
                    "recovery ordinal mismatch",
                )?;
                if initialized {
                    require(value["builds"] == 1, "recovered application was rebuilt")?;
                } else {
                    require(
                        value["retained"] == false,
                        "health or failure retained an app",
                    )?;
                }
                if status == 503 {
                    require(
                        value["attempts"] == attempts,
                        "failed assembly attempt count mismatch",
                    )?;
                    require(
                        !reply["headers"].as_array().unwrap().iter().any(|h| {
                            h[0].as_str()
                                .is_some_and(|name| name.eq_ignore_ascii_case("x-fixture-attempts"))
                        }),
                        "failed assembly used application headers",
                    )?;
                } else {
                    require(
                        reply["headers"].as_array().unwrap().iter().any(|h| {
                            h[0].as_str()
                                .is_some_and(|name| name.eq_ignore_ascii_case("x-fixture-attempts"))
                                && h[1].as_str().and_then(|v| v.parse::<u64>().ok())
                                    == Some(attempts)
                        }),
                        "lazy build attempt count mismatch",
                    )?;
                }
            }
            records.push(annotate(
                reply,
                json!({"variant":"custom-initialization", "guest":value}),
            ))?;
        }
        records.push(json!({"assertion":"custom_lazy_initialization_recovery", "status":if instances.iter().all(|id| id == &instances[0]) {"pass"} else {"unverified"}, "reason":"recovery requires all five callbacks in the same guest"}))
    })();
    settle(
        records,
        &log,
        json!({"variant":"custom-initialization","repetition":0}),
        json!({"adapter":"fastly"}),
        outcome,
    )
}

pub fn fastly(args: &Args, run: &Path, records: &mut Records) -> Result<()> {
    let Some(exe) = executable("VICEROY_BIN", "viceroy") else {
        return records.push(json!({"status":"unsupported","reason":"Viceroy unavailable"}));
    };
    let version = Command::new(&exe).arg("--version").output()?;
    require(version.status.success(), "Viceroy version command failed")?;
    let version = String::from_utf8(version.stdout)?.trim().to_owned();
    let pins = fs::read_to_string(root().join("../../../.tool-versions"))?;
    let pin = pins
        .lines()
        .find_map(|line| line.strip_prefix("viceroy "))
        .map(str::trim)
        .ok_or("missing Viceroy pin")?;
    records.push(json!({"event":"runtime","adapter":"fastly","executable":exe,"version":version,"expected":pin}))?;
    if version.split_whitespace().nth(1) != Some(pin) {
        // Keep going: an unpinned Viceroy still gets signal, and a failure still exits 1.
        records.push(json!({"status":"unverified","adapter":"fastly","reason":format!("Viceroy version {version} does not match pinned {pin}")}))?;
    }
    if args.suite == "response-framing" {
        return framing::run("fastly", run, records);
    }
    if args.suite == "synthesized-headers" {
        return response_headers::run("fastly", run, records);
    }
    if args.suite == "outbound-overflow" {
        return overflow::run("fastly", run, records);
    }
    if args.suite == "assembly" {
        return assembly::fastly(&exe, run, records);
    }
    if args.suite == "outbound-progress" {
        return progress::run("fastly", run, records);
    }
    if let Some(suite) = readiness::ReadinessSuite::parse(&args.suite) {
        require(
            version.split_whitespace().nth(1) == Some(pin),
            "readiness proof requires the pinned runtime",
        )?;
        return readiness::run(&exe, run, records, suite);
    }
    checked(
        cargo_build()
            .args([
                "build",
                "--locked",
                "--release",
                "-p",
                "fixture-fastly",
                "--bins",
                "--target",
                "wasm32-wasip1",
            ])
            .env("FIXTURE_BUILD_ROUNDS", args.construction_rounds.to_string())
            .env("FIXTURE_MAX_REQUESTS", args.max_requests.to_string()),
    )?;
    stage_fastly_artifacts(run)?;
    let config_snapshot = run.join("fastly.toml");
    fs::copy(
        root().join("crates/fixture-fastly/fastly.toml"),
        &config_snapshot,
    )?;
    if args.suite == "smoke" {
        custom_initialization_contract(&exe, &config_snapshot, run, records)?;
    }
    let mut variants = vec!["a", "b", "c", "custom-a", "custom-b", "custom-c"];
    if args.suite == "smoke" {
        variants.extend(["limit-requests", "limit-lifetime", "limit-memory"]);
    }
    let mut rng = args.seed;
    for repetition in 0..if args.suite == "benchmark" {
        args.repetitions
    } else {
        1
    } {
        // A recorded deterministic Fisher-Yates order; independent of language RNG versions.
        for i in (1..variants.len()).rev() {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            variants.swap(i, (rng as usize) % (i + 1));
        }
        records
            .push(json!({"event":"variant_order","repetition":repetition,"variants":variants}))?;
        for variant in &variants {
            fastly_variant(
                args,
                &exe,
                &config_snapshot,
                run,
                variant,
                repetition,
                records,
            )?;
        }
    }
    if args.suite == "smoke" {
        logging(&exe, run, records)?;
        faults(&exe, run, records)?;
        progress::run("fastly", run, records)?;
    }
    if records
        .values
        .iter()
        .any(|e| e["event"] == "heap_preflight" && e["heap_mib"].is_null())
    {
        records.push(json!({"status":"unsupported","reason":"heap snapshot unavailable; memory limit unverified"}))?;
    }
    Ok(())
}

fn fastly_variant(
    args: &Args,
    exe: &Path,
    config: &Path,
    run: &Path,
    variant: &str,
    repetition: usize,
    records: &mut Records,
) -> Result<()> {
    records.push(json!({"event":"build","variant":variant,"repetition":repetition,"target":"wasm32-wasip1","artifact":artifact(&wasm(run, variant))?,"config":artifact(config)?}))?;
    let log = run.join(format!("{variant}-{repetition}.log"));
    let mut declared = DeclaredKvChecks::default();
    let outcome = (|| -> Result<()> {
        let backend = Backend::start()?;
        let port = net::port()?;
        let _process = Process::start(
            &mut fastly_command(exe, config, run, variant, port),
            &log,
            port,
        )?;
        let seen = fastly_probes(args, port, variant, repetition, records)?;
        if ["c", "custom-c"].contains(&variant) {
            thread::sleep(Duration::from_secs(2));
            let reply = req(port, "/probe/after-idle")?;
            require(
                !seen.contains_key(
                    guest(&reply)?["instance"]
                        .as_str()
                        .ok_or("missing instance")?,
                ),
                "idle guest was reused",
            )?;
            records.push(annotate(
                reply,
                json!({"variant":variant,"repetition":repetition,"assertion":"idle_fresh_initialization"}),
            ))?;
        }
        if args.suite == "smoke" && variant == "c" {
            declared_kv_checks("fastly", port, &log, records, &mut declared)?;
        }
        let bindings = req(port, "/bindings")?;
        require(
            guest(&bindings)? == json!({"config":true,"kv":true,"secrets":true,"unknown":false}),
            "binding mismatch",
        )?;
        records.push(annotate(
            bindings,
            json!({"variant":variant,"repetition":repetition,"assertion":"binding_reads"}),
        ))?;
        require(
            cookie_values(&req(port, "/cookies")?["headers"])
                == vec!["first=1; Path=/", "second=2; Path=/"],
            "duplicate cookies lost",
        )?;
        require(
            req(port, "/rendered-error")?["status"] == 400,
            "rendered error mismatch",
        )?;
        require(
            req(port, "/probe/after")?["status"] == 200,
            "request after error failed",
        )?;
        if variant.starts_with("custom-") {
            custom_stream_checks(port, &backend, &log, variant, repetition, records)?;
        }
        if args.suite == "smoke" {
            smoke_origin_checks(port, &backend, &log, variant, repetition, records)?;
        }
        if variant.starts_with("limit-") {
            await_evidence(|| validate_limit_summaries(&logs(&log)))?;
        }
        records.push(json!({"status":"pass","variant":variant,"repetition":repetition,"assertions":["initialization","isolation","cookies","rendered_error"]}))
    })();
    let teardown = if args.suite == "smoke" && variant == "c" {
        finish_declared_kv_checks("fastly", &log, records, &declared)
    } else {
        Ok(())
    };
    settle(
        records,
        &log,
        json!({"variant":variant,"repetition":repetition}),
        json!({"adapter":"fastly"}),
        outcome.and(teardown),
    )
}

const DECLARED_KV_FAILURE_CASES: [(&str, u16, usize); 7] = [
    ("/declared-kv/free", 200, 0),
    ("/declared-kv/missing/read", 500, 1),
    ("/declared-kv/missing/read", 500, 1),
    ("/declared-kv/free", 200, 1),
    ("/declared-kv/missing/write", 500, 1),
    ("/declared-kv/missing/delete", 500, 1),
    ("/declared-kv/missing/list", 500, 1),
];
const DECLARED_KV_LINKED_OPERATIONS: [&str; 6] =
    ["write", "read", "list", "delete", "read", "list"];

struct DeclaredKvRequest {
    token: String,
    path: String,
    instance: Option<String>,
}

#[derive(Default)]
struct DeclaredKvChecks {
    instance: Option<String>,
    requests: Vec<DeclaredKvRequest>,
}

impl DeclaredKvChecks {
    fn failed_instances(&self) -> BTreeSet<&str> {
        self.requests
            .iter()
            .filter(|request| request.path.starts_with("/declared-kv/missing/"))
            .filter_map(|request| request.instance.as_deref())
            .collect()
    }

    fn warning_count(&self, text: &str, adapter: &str) -> Result<usize> {
        let failed = self.failed_instances();
        let count = if adapter == "fastly" {
            declared_kv_warning_count(text)
        } else {
            let instances: BTreeSet<_> = self
                .requests
                .iter()
                .filter_map(|request| request.instance.as_deref())
                .collect();
            let mut count = 0;
            for instance in instances {
                let observed = provider_kv_warning_count(text, adapter, instance);
                require(
                    observed == usize::from(failed.contains(instance)),
                    "declared KV warning count mismatch within an instance",
                )?;
                count += observed;
            }
            count
        };
        require(count == failed.len(), "declared KV warning count mismatch")?;
        Ok(count)
    }

    fn validate(&self, events: &[Value], adapter: &str) -> Result<()> {
        require(
            self.requests.len()
                == DECLARED_KV_FAILURE_CASES.len() + DECLARED_KV_LINKED_OPERATIONS.len(),
            "declared KV matrix did not execute every request",
        )?;
        let prefix_instance = self
            .instance
            .as_deref()
            .ok_or("missing declared KV instance")?;
        let mut tokens = HashSet::new();
        for (index, request) in self.requests.iter().enumerate() {
            require(
                tokens.insert(request.token.as_str()),
                "declared KV request token reused",
            )?;
            let expected = request
                .instance
                .as_deref()
                .ok_or("missing declared KV request instance")?;
            if let Some((path, _, _)) = DECLARED_KV_FAILURE_CASES.get(index) {
                require(
                    request.path == *path && (index >= 4 || expected == prefix_instance),
                    "declared KV failure prefix changed or turned over",
                )?;
            } else {
                let operation =
                    DECLARED_KV_LINKED_OPERATIONS[index - DECLARED_KV_FAILURE_CASES.len()];
                require(
                    request.path == format!("/declared-kv/linked/{operation}"),
                    "declared KV linked sequence changed",
                )?;
            }
            validate_declared_kv_lifecycle(
                events,
                &request.token,
                Some(expected),
                if adapter == "fastly" {
                    "Completed"
                } else {
                    "HostHandoff"
                },
            )?;
            validate_declared_kv_operation(
                events,
                &request.token,
                expected,
                adapter,
                &request.path,
            )?;
        }
        Ok(())
    }
}

fn validate_declared_kv_operation(
    events: &[Value],
    token: &str,
    instance: &str,
    adapter: &str,
    path: &str,
) -> Result<()> {
    let matching: Vec<_> = events
        .iter()
        .filter(|event| {
            event["token"] == token
                && matches!(
                    event["event"].as_str(),
                    Some(
                        "request_start"
                            | "declared_kv_enter"
                            | "declared_kv_result"
                            | "egress_terminal"
                    )
                )
        })
        .collect();
    if path == "/declared-kv/free" {
        return require(
            matching.len() == 2
                && matching[0]["event"] == "request_start"
                && matching[1]["event"] == "egress_terminal",
            "KV-free route invoked an operation handler or reordered its lifecycle",
        );
    }
    let (store, operation) = path
        .strip_prefix("/declared-kv/")
        .and_then(|path| path.split_once('/'))
        .ok_or("invalid declared KV fixture path")?;
    require(
        matching.len() == 4
            && matching[0]["event"] == "request_start"
            && matching[1]["event"] == "declared_kv_enter"
            && matching[2]["event"] == "declared_kv_result"
            && matching[3]["event"] == "egress_terminal",
        "declared KV handler/result receipt missing, repeated or reordered",
    )?;
    require(
        matching.iter().all(|event| event["instance"] == instance)
            && matching[1..3]
                .iter()
                .all(|event| event["store"] == store && event["operation"] == operation),
        "declared KV operation receipt mismatched its request",
    )?;
    let (outcome, reason) = if store == "missing" {
        (
            "open-failed",
            json!(if adapter == "spin" {
                "InvalidConfiguration"
            } else {
                "MissingBinding"
            }),
        )
    } else {
        ("success", json!(null))
    };
    require(
        matching[2]["outcome"] == outcome && matching[2]["reason"] == reason,
        "declared KV operation lost its typed outcome",
    )
}

fn finish_declared_kv_checks(
    adapter: &str,
    log: &Path,
    records: &mut Records,
    checks: &DeclaredKvChecks,
) -> Result<()> {
    let text = fs::read_to_string(log)?;
    let warnings = checks.warning_count(&text, adapter);
    let count = warnings.as_ref().ok().copied();
    let result = checks
        .validate(&logs(log), adapter)
        .and(warnings.map(|_count| ()));
    records.push(json!({"event":"declared-kv-after-shutdown","adapter":adapter,"instance":checks.instance,"failed_instances":checks.failed_instances(),"requests":checks.requests.len(),"warnings":count,"status":if result.is_ok() {"pass"} else {"fail"},"error":result.as_ref().err().map(ToString::to_string),"qualification":false}))?;
    result
}

fn declared_kv_warning_count(text: &str) -> usize {
    text.lines()
        .filter(|line| {
            line.starts_with("fixture-logs :: ")
                && line.contains(
                    "declared Fastly KV store 'missing_fixture_kv' failed to open: missing binding",
                )
        })
        .count()
}

fn validate_declared_kv_lifecycle(
    events: &[Value],
    token: &str,
    expected_instance: Option<&str>,
    expected_outcome: &str,
) -> Result<String> {
    let starts: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "request_start" && event["token"] == token)
        .collect();
    let ends: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "egress_terminal" && event["token"] == token)
        .collect();
    require(
        starts.len() == 1 && ends.len() == 1,
        "declared KV request lost exactly-once lifecycle evidence",
    )?;
    let current = starts[0]["instance"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or("missing declared KV instance")?;
    require(
        expected_instance.is_none_or(|expected| expected == current),
        "declared KV sandbox turnover: deduplication unverified",
    )?;
    require(
        ends[0]["instance"] == starts[0]["instance"]
            && ends[0]["outcome"] == expected_outcome
            && ends[0]["body_kind"] == "Application"
            && ends[0].get("fallback") == Some(&Value::Null),
        "declared KV terminal disposition mismatch",
    )?;
    Ok(current.to_owned())
}

fn provider_kv_warning_count(text: &str, adapter: &str, instance: &str) -> usize {
    let (platform, reason) = if adapter == "spin" {
        ("Spin", "invalid binding configuration")
    } else {
        ("Cloudflare", "missing binding")
    };
    let message =
        format!("declared {platform} KV store 'missing_fixture_kv' failed to open: {reason}");
    text.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|event| {
            event["event"] == "fixture_log"
                && event["instance"] == instance
                && event["level"] == "WARN"
                && event["target"] == "edgezero::boot"
                && event["message"] == message
        })
        .count()
}

fn validate_declared_kv_linked_response(response: &Value, expected: &Value) -> Result<()> {
    require(
        guest(response)?.get("kv_outcome") == Some(expected),
        "linked KV operation failed after declared failure",
    )
}

fn validate_declared_kv_guest(response: &Value, token: &str, instance: &str) -> Result<()> {
    let body = guest(response)?;
    require(
        body.get("token").and_then(Value::as_str) == Some(token)
            && body.get("instance").and_then(Value::as_str) == Some(instance),
        "declared KV success body does not match its request lifecycle",
    )
}

fn declared_kv_checks(
    adapter: &str,
    port: u16,
    log: &Path,
    records: &mut Records,
    checks: &mut DeclaredKvChecks,
) -> Result<()> {
    for (index, (path, status, warnings)) in DECLARED_KV_FAILURE_CASES.into_iter().enumerate() {
        let token = token();
        checks.requests.push(DeclaredKvRequest {
            token: token.clone(),
            path: path.to_owned(),
            instance: None,
        });
        let response = net::request(port, path, &token, None, None)?;
        require(response["status"] == status, "declared KV status mismatch")?;
        if status == 200 {
            let value = guest(&response)?;
            require(
                value["declared_kv"] == true
                    && value["default_kv"] == true
                    && value["unknown_kv"] == false
                    && value["builds"] == 1,
                "declared KV registry or retained application mismatch",
            )?;
        } else {
            let body = response["body"]
                .as_str()
                .ok_or("missing declared KV error body")?;
            let public: Value = serde_json::from_str(body)?;
            require(
                public
                    == json!({"error":{"kind":"internal","message":"internal server error","status":500}}),
                "declared KV error gained provider diagnostics",
            )?;
        }
        let mut current = None;
        await_evidence(|| {
            current = Some(validate_declared_kv_lifecycle(
                &logs(log),
                &token,
                if index < 4 {
                    checks.instance.as_deref()
                } else {
                    None
                },
                if adapter == "fastly" {
                    "Completed"
                } else {
                    "HostHandoff"
                },
            )?);
            validate_declared_kv_operation(
                &logs(log),
                &token,
                current.as_deref().ok_or("missing declared KV instance")?,
                adapter,
                path,
            )?;
            Ok(())
        })?;
        checks
            .requests
            .last_mut()
            .ok_or("missing declared KV request")?
            .instance = current.clone();
        if index < 4 {
            checks.instance = current.clone();
        }
        if status == 200 {
            validate_declared_kv_guest(
                &response,
                &token,
                current.as_deref().ok_or("missing declared KV instance")?,
            )?;
        }
        let text = fs::read_to_string(log)?;
        let count = checks.warning_count(&text, adapter)?;
        require(
            index >= 4 || count == warnings,
            "declared KV prefix warning count mismatch",
        )?;
        records.push(json!({"event":"declared-kv-case","adapter":adapter,"path":path,"token":token,"instance":current,
            "client":response,"warnings":count,"status":"pass","qualification":false}))?;
    }
    for (operation, expected) in [
        ("write", json!("written")),
        ("read", json!("f8-value")),
        ("list", json!(["f8-key"])),
        ("delete", json!("deleted")),
        ("read", json!(null)),
        ("list", json!([])),
    ] {
        let path = format!("/declared-kv/linked/{operation}");
        let token = token();
        checks.requests.push(DeclaredKvRequest {
            token: token.clone(),
            path: path.clone(),
            instance: None,
        });
        let response = net::request(port, &path, &token, None, None)?;
        validate_declared_kv_linked_response(&response, &expected)?;
        let mut linked_instance = None;
        await_evidence(|| {
            linked_instance = Some(validate_declared_kv_lifecycle(
                &logs(log),
                &token,
                None,
                if adapter == "fastly" {
                    "Completed"
                } else {
                    "HostHandoff"
                },
            )?);
            validate_declared_kv_operation(
                &logs(log),
                &token,
                linked_instance
                    .as_deref()
                    .ok_or("missing linked KV instance")?,
                adapter,
                &path,
            )?;
            Ok(())
        })?;
        checks
            .requests
            .last_mut()
            .ok_or("missing linked KV request")?
            .instance = linked_instance.clone();
        validate_declared_kv_guest(
            &response,
            &token,
            linked_instance
                .as_deref()
                .ok_or("missing linked KV instance")?,
        )?;
        records.push(json!({"event":"declared-kv-linked","adapter":adapter,"operation":operation,"instance":linked_instance,"client":response,"status":"pass","qualification":false}))?;
    }
    checks.warning_count(&fs::read_to_string(log)?, adapter)?;
    checks.validate(&logs(log), adapter)
}
fn fastly_probes(
    args: &Args,
    port: u16,
    variant: &str,
    repetition: usize,
    records: &mut Records,
) -> Result<HashMap<String, Value>> {
    let mut seen: HashMap<String, Value> = HashMap::new();
    let mut reused = false;
    for _ in 0..if args.suite == "benchmark" {
        args.requests
    } else {
        5
    } {
        let token = token();
        let reply = net::request(port, &format!("/probe/{token}"), &token, None, None)?;
        let value = guest(&reply)?;
        require(
            value["token"] == token && value["path"] == token && value["body"] == token,
            format!("request isolation: {value}"),
        )?;
        if let Some(previous) = seen.get(value["instance"].as_str().ok_or("missing instance")?) {
            reused = true;
            require(
                value["ordinal"].as_u64() > previous["ordinal"].as_u64()
                    && value["correlation"] != previous["correlation"],
                "reuse ordinal/correlation mismatch",
            )?;
        }
        require(
            value["mutated"] == variant.starts_with("custom-"),
            "raw request mutation mismatch",
        )?;
        if variant.starts_with("custom-") {
            require(
                reply["headers"].as_array().unwrap().iter().any(|h| {
                    h[0].as_str()
                        .is_some_and(|name| name.eq_ignore_ascii_case("x-fixture-finalized"))
                        && h[1] == token
                }),
                "request-specific response extension was lost or leaked",
            )?;
        }
        match variant {
            "c" | "custom-c" => require(
                value["ordinal"]
                    .as_u64()
                    .is_some_and(|n| n <= args.max_requests as u64)
                    && value["builds"] == 1,
                "retained app initialization mismatch",
            )?,
            "b" | "custom-b" => require(
                value["builds"] == value["ordinal"],
                "per-request initialization mismatch",
            )?,
            _ => require(
                value["ordinal"] == 1 && value["builds"] == 1,
                "single-request initialization mismatch",
            )?,
        }
        validate_shared_state(&value, ["c", "custom-c"].contains(&variant))?;
        seen.insert(value["instance"].as_str().unwrap().into(), value.clone());
        records.push(annotate(
            reply,
            json!({"variant":variant,"repetition":repetition,"guest":value,"workload":"probe"}),
        ))?;
    }
    if ["b", "c", "custom-b", "custom-c"].contains(&variant) && !reused {
        records.push(json!({"status":"unverified","variant":variant,"repetition":repetition,"reason":"no actual guest reuse observed"}))?;
    }
    Ok(seen)
}
fn custom_stream_checks(
    port: u16,
    backend: &Backend,
    log: &Path,
    variant: &str,
    repetition: usize,
    records: &mut Records,
) -> Result<()> {
    let token = token();
    let stream = net::request(
        port,
        "/stream",
        &token,
        Some(&backend.url(&format!("/chunks?token={token}"))),
        Some(&backend.release),
    )?;
    require(
        stream["body"] == "firstsecond",
        "progressive stream mismatch",
    )?;
    require(
        stream["headers"].as_array().unwrap().iter().any(|h| {
            h[0].as_str()
                .is_some_and(|s| s.eq_ignore_ascii_case("x-fixture-finalized"))
                && h[1] == token
        }),
        "response not finalized",
    )?;
    await_evidence(|| validate_post_send(&token, &backend.receipts.lock().unwrap(), &logs(log)))?;
    records.push(annotate(
        stream,
        json!({"variant":variant,"repetition":repetition,"assertion":"progressive_stream_and_post_send"}),
    ))?;
    *backend.release.0.lock().unwrap() = false;
    let large_token = crate::token();
    let large = net::request(
        port,
        "/stream",
        &large_token,
        Some(&backend.url("/chunks?large=1")),
        Some(&backend.release),
    )?;
    let large_body = large["body"].as_str().ok_or("missing large body")?;
    require(
        large_body.starts_with("first") && large_body.len() == 5 + 256 * 1024,
        "large stream truncated",
    )?;
    records.push(annotate(
        large,
        json!({"variant":variant,"repetition":repetition,"assertion":"large_progressive_stream"}),
    ))?;
    // Trigger truncation only after the downstream client has observed the partial body.
    *backend.release.0.lock().unwrap() = false;
    let failure_token = crate::token();
    let failure = net::request(
        port,
        "/stream-error",
        &failure_token,
        Some(&backend.url(&format!("/abort?gated=1&token={failure_token}"))),
        Some(&backend.release),
    );
    let failure = match failure {
        Ok(reply) => {
            return Err(format!("interrupted stream unexpectedly completed: {reply}").into());
        }
        Err(error) => error,
    };
    let partial = failure
        .downcast_ref::<net::InterruptedResponse>()
        .ok_or("stream failure occurred before response body: not post-commit evidence")?;
    require(
        partial.status == 200 && partial.body == b"partial",
        "missing partial committed response",
    )?;
    await_evidence(|| {
        require(
            logs(log)
                .iter()
                .any(|e| e["event"] == "post_commit_error" && e["token"] == failure_token),
            "missing correlated post-commit error",
        )
    })?;
    await_evidence(|| {
        validate_post_send(
            &failure_token,
            &backend.receipts.lock().unwrap(),
            &logs(log),
        )
    })?;
    validate_fastly_delivery_report(
        &failure_token,
        &logs(log),
        &json!({"disposition":"routed",
        "commitment":"Committed","body_kind":"Application","outcome":"SourceError",
        "fallback":null,"may_send_native":false}),
    )?;
    records.push(json!({"event":"expected_stream_error","variant":variant,"repetition":repetition,"token":failure_token,"status_code":partial.status,"partial_body":String::from_utf8_lossy(&partial.body),"source":"controlled_backend_abort"}))?;
    let fallback_token = crate::token();
    let fallback = net::request(
        port,
        "/probe/framing-fallback",
        &fallback_token,
        Some(&backend.url(&format!("/chunks?token={fallback_token}"))),
        None,
    )?;
    require(
        fallback["status"] == 500 && fallback["body"] == "internal server error",
        "framing fallback wire mismatch",
    )?;
    await_evidence(|| {
        require(
            logs(log).iter().any(|event| {
                event["event"] == "guest_completed" && event["token"] == fallback_token
            }),
            "missing fallback callback completion",
        )
    })?;
    validate_fastly_delivery_report(
        &fallback_token,
        &logs(log),
        &json!({"disposition":"routed",
        "commitment":"Committed","body_kind":"Fallback","outcome":"ConversionError",
        "fallback":"Completed","may_send_native":false}),
    )?;
    require(
        !backend
            .receipts
            .lock()
            .unwrap()
            .contains(&format!("/post-send?token={fallback_token}")),
        "fallback incorrectly scheduled post-send work",
    )?;
    records.push(annotate(fallback, json!({"variant":variant,"repetition":repetition,"token":fallback_token,"assertion":"reported_fallback_skips_post_send"})))
}
fn smoke_origin_checks(
    port: u16,
    backend: &Backend,
    log: &Path,
    variant: &str,
    repetition: usize,
    records: &mut Records,
) -> Result<()> {
    for repeat in 0..3 {
        let reply = net::request(
            port,
            "/origin/repeated",
            &token(),
            Some(&backend.url("/ok")),
            None,
        )?;
        require(
            reply["status"] == 200 && reply["body"] == "ok",
            "repeated origin failed",
        )?;
        records.push(annotate(reply, json!({"variant":variant,"repetition":repetition,"origin_repeat":repeat,"assertion":"bounded_repeated_origin"})))?;
    }
    for origin in 0..3 {
        let distinct = Backend::start()?;
        let reply = net::request(
            port,
            &format!("/origin/{origin}"),
            &token(),
            Some(&distinct.url("/ok")),
            None,
        )?;
        require(
            reply["status"] == 200 && reply["body"] == "ok",
            "distinct origin failed",
        )?;
        records.push(annotate(
            reply,
            json!({"variant":variant,"repetition":repetition,"assertion":"bounded_distinct_origin"}),
        ))?;
    }
    if ["custom-b", "custom-c"].contains(&variant) {
        let token = token();
        if let Ok(failed) = net::request(port, "/panic", &token, None, None) {
            require(
                failed["status"].as_u64().is_some_and(|s| s >= 500),
                "panic did not fail",
            )?;
        }
        let fresh = req(port, "/probe/fresh")?;
        let value = guest(&fresh)?;
        let mut failed_guest = Value::Null;
        await_evidence(|| {
            failed_guest = logs(log)
                .iter()
                .find(|e| e["event"] == "request_start" && e["token"] == token)
                .map(|e| e["instance"].clone())
                .ok_or("missing panic attempt")?;
            Ok(())
        })?;
        require(value["instance"] != failed_guest, "terminated guest reused")?;
        records.push(annotate(fresh,json!({"variant":variant,"repetition":repetition,"assertion":"terminated_guest_not_reused","failed_guest":failed_guest})))?;
    }
    Ok(())
}
fn logging(exe: &Path, run: &Path, records: &mut Records) -> Result<()> {
    let base = fs::read_to_string(root().join("crates/fixture-fastly/fastly.toml"))?;
    let config = run.join("logging.toml");
    fs::write(&config, base)?;
    for variant in ["b", "c", "custom-c", "logger-negative"] {
        let log = run.join(format!("logging-{variant}.log"));
        let outcome = (|| -> Result<()> {
            let port = net::port()?;
            let process = Process::start(
                &mut fastly_command(exe, &config, run, variant, port),
                &log,
                port,
            )?;
            let mut replies = Vec::new();
            for _ in 0..if variant == "logger-negative" { 2 } else { 3 } {
                replies.push(req(port, "/probe/log")?);
            }
            drop(process);
            records.push(json!({"event":"logging_check","variant":variant,"statuses":replies.iter().map(|r|r["status"].clone()).collect::<Vec<_>>(),"log_file":log.file_name().map(|n| n.to_string_lossy())}))?;
            if variant == "logger-negative" {
                let starts = logs(&log)
                    .into_iter()
                    .filter(|e| e["event"] == "request_start")
                    .collect::<Vec<_>>();
                let status = validate_negative_logging(&replies, &starts)?;
                records.push(json!({"status":status,"assertion":"repeated_logger_installation","variant":variant}))?;
            } else {
                let guests = replies.iter().map(guest).collect::<Result<Vec<_>>>()?;
                let text = fs::read_to_string(&log)?;
                let delivered = text
                    .lines()
                    .filter(|l| l.starts_with("fixture-logs :: "))
                    .filter_map(|l| {
                        l.rsplit_once("fixture request ")
                            .map(|(_, v)| v.trim().to_owned())
                    })
                    .collect::<Vec<_>>();
                let status = validate_logging(&guests, &delivered)?;
                records.push(json!({"status":status,"assertion":"named_endpoint_receipt","variant":variant,"received":delivered.len()}))?;
            }
            Ok(())
        })();
        settle(
            records,
            &log,
            json!({"variant":format!("logging-{variant}"),"repetition":0}),
            json!({"adapter":"fastly"}),
            outcome,
        )?;
    }
    Ok(())
}
pub fn provider(name: &str, args: &Args, run: &Path, records: &mut Records) -> Result<()> {
    if args.suite == "outbound-wire" {
        return wire::run(name, run, records);
    }
    if args.suite == "synthesized-headers" {
        return response_headers::run(name, run, records);
    }
    if args.suite == "response-framing" {
        return framing::run(name, run, records);
    }
    if args.suite == "assembly" {
        return assembly::provider(name, run, records);
    }
    if args.suite == "outbound-overflow" {
        return overflow::run(name, run, records);
    }
    if args.suite == "outbound-progress" {
        return progress::run(name, run, records);
    }
    let package = root().join("crates").join(format!("fixture-{name}"));
    let Some((exe, staged)) = provider_build(name, &package, run, records)? else {
        return Ok(());
    };
    let modes = if name == "axum" {
        vec!["retained"]
    } else {
        vec!["per-request", "retained"]
    };
    for mode in modes {
        let port = net::port()?;
        let mut command = provider_command(name, mode, &exe, &staged, &package, run, port)?;
        let log = run.join(format!("{name}-{mode}.log"));
        let mut failure = json!({"phase":"startup"});
        let mut declared = DeclaredKvChecks::default();
        let outcome = Process::start(&mut command, &log, port).and_then(|_process| {
            // Establish single-instance dedup before overlap creates additional instances.
            if args.suite == "smoke" && mode == "retained" && name != "axum" {
                failure = json!({"phase":"declared_kv"});
                declared_kv_checks(name, port, &log, records, &mut declared)?;
            }
            provider_checks(name, mode, port, args, records, &mut failure)?;
            Ok(())
        });
        let teardown = if args.suite == "smoke" && mode == "retained" && name != "axum" {
            finish_declared_kv_checks(name, &log, records, &declared)
        } else {
            Ok(())
        };
        settle(
            records,
            &log,
            json!({"adapter":name,"mode":mode}),
            failure,
            outcome.and(teardown),
        )?;
    }
    if args.suite == "smoke" {
        progress::run(name, run, records)?;
    }
    Ok(())
}
/// Builds the provider fixture and stages its artifact in the run directory.
/// Returns the runtime executable and the staged artifact, or `None` when a
/// required tool is unavailable.
fn provider_build(
    name: &str,
    package: &Path,
    run: &Path,
    records: &mut Records,
) -> Result<Option<(PathBuf, PathBuf)>> {
    if name == "axum" {
        let output = cargo_build()
            .args([
                "build",
                "--locked",
                "-p",
                "fixture-axum",
                "--message-format=json-render-diagnostics",
            ])
            .stderr(Stdio::inherit())
            .output()?;
        require(output.status.success(), "Axum fixture build failed")?;
        let staged = stage(&built_executable(&output.stdout, "fixture-axum")?, run)?;
        records.push(json!({"event":"build","adapter":name,"artifact":artifact(&staged)?}))?;
        return Ok(Some((staged.clone(), staged)));
    }
    let (variable, tool) = if name == "cloudflare" {
        ("WRANGLER_BIN", "wrangler")
    } else {
        ("SPIN_BIN", "spin")
    };
    let Some(exe) = executable(variable, tool) else {
        records.push(
            json!({"status":"unsupported","adapter":name,"reason":format!("{tool} unavailable")}),
        )?;
        return Ok(None);
    };
    records.push(json!({"event":"runtime","adapter":name,"executable":exe,"version":String::from_utf8_lossy(&Command::new(&exe).arg("--version").output()?.stdout).trim()}))?;
    let (staged, wasm) = if name == "spin" {
        let help = Command::new(&exe).args(["up", "--help"]).output()?;
        fs::write(run.join("spin-up-help.txt"), &help.stdout)?;
        records.push(json!({"event":"runtime_controls","adapter":"spin","selected":"host defaults","source":"spin-up-help.txt"}))?;
        checked(cargo_build().args([
            "build",
            "--locked",
            "--release",
            "-p",
            "fixture-spin",
            "--target",
            "wasm32-wasip2",
        ]))?;
        let staged = stage(
            &root().join("target/wasm32-wasip2/release/fixture_spin.wasm"),
            run,
        )?;
        (staged.clone(), staged)
    } else {
        let Some(builder) = executable("WORKER_BUILD_BIN", "worker-build") else {
            records.push(json!({"status":"unsupported","reason":"worker-build unavailable"}))?;
            return Ok(None);
        };
        if !progress::check_bindgen(records)? {
            return Ok(None);
        }
        checked(
            Command::new(builder)
                .args(["--release", ".", "--", "--locked"])
                .env("CARGO_TARGET_DIR", root().join("target"))
                .current_dir(package),
        )?;
        let staged = stage(&package.join("build"), run)?;
        // Only the explicit builder runs before artifact fingerprinting.
        fs::write(
            run.join("wrangler.toml"),
            wrangler_runtime_config(package, &staged.join("worker/shim.mjs"))?,
        )?;
        let wasm = staged.join("index_bg.wasm");
        (staged, wasm)
    };
    records.push(json!({"event":"build","adapter":name,"artifact":artifact(&wasm)?}))?;
    Ok(Some((exe, staged)))
}
fn provider_command(
    name: &str,
    mode: &str,
    exe: &Path,
    staged: &Path,
    package: &Path,
    run: &Path,
    port: u16,
) -> Result<Command> {
    let mut command = Command::new(exe);
    command
        .env("CARGO_TARGET_DIR", root().join("target"))
        .current_dir(package)
        .env("EDGEZERO__ADAPTER__HOST", "127.0.0.1")
        .env("EDGEZERO__ADAPTER__PORT", port.to_string());
    if name == "axum" {
        let state = run.join("axum-state");
        fs::create_dir_all(state.join(".edgezero"))?;
        fs::write(
            state.join(".edgezero/local-config-fixture_config.json"),
            "{\"marker\":\"local-fixture\"}",
        )?;
        command
            .current_dir(state)
            .env("EDGEZERO__SECRETS__FIXTURE_MARKER", "fixture-only-value");
    } else if name == "cloudflare" {
        let state = run.join(mode).join("worker-state");
        checked(
            Command::new(exe)
                .args([
                    "kv",
                    "key",
                    "put",
                    "marker",
                    "local-fixture",
                    "--binding",
                    "fixture_config",
                    "--local",
                    "--persist-to",
                ])
                .arg(&state)
                .current_dir(package),
        )?;
        command
            .arg("--config")
            .arg(run.join("wrangler.toml"))
            .args([
                "dev",
                "--local",
                "--ip",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--var",
                &format!("FIXTURE_MODE:{mode}"),
                "--persist-to",
            ])
            .arg(state);
    } else {
        let manifest = fs::read_to_string(package.join("spin.toml"))?
            .replace(
                "../../target/wasm32-wasip2/release/fixture_spin.wasm",
                &staged.display().to_string(),
            )
            .replace(
                "FIXTURE_MODE = \"retained\"",
                &format!("FIXTURE_MODE = \"{mode}\""),
            );
        let config = run.join(format!("spin-{mode}.toml"));
        fs::write(&config, manifest)?;
        command
            .args(["up", "--listen", &format!("127.0.0.1:{port}"), "--from"])
            .arg(config)
            .arg("--runtime-config-file")
            .arg(package.join("runtime-config.toml"));
    }
    Ok(command)
}
/// Runs the provider assertions against a listening runtime, keeping
/// `failure` pointed at the current phase so a failure record names its step.
fn provider_checks(
    name: &str,
    mode: &str,
    port: u16,
    args: &Args,
    records: &mut Records,
    failure: &mut Value,
) -> Result<()> {
    let mut seen: HashMap<String, Value> = HashMap::new();
    let mut reused = false;
    let repetitions = if args.suite == "benchmark" {
        args.repetitions
    } else {
        1
    };
    let requests = if args.suite == "benchmark" {
        args.requests
    } else {
        5
    };
    for repetition in 0..repetitions {
        *failure = json!({"phase":"probe","repetition":repetition});
        for _ in 0..requests {
            let token = token();
            let reply = net::request(port, &format!("/probe/{token}"), &token, None, None)?;
            let value = guest(&reply)?;
            require(
                value["token"] == token && value["path"] == token && value["body"] == token,
                "probe request isolation mismatch",
            )?;
            if mode == "retained" {
                require(value["builds"] == 1, "retained initialization mismatch")?;
            }
            validate_shared_state(&value, mode == "retained")?;
            let instance = value["instance"].as_str().ok_or("missing instance")?;
            if let Some(previous) = seen.get(instance) {
                reused = true;
                require(
                    value["ordinal"].as_u64() > previous["ordinal"].as_u64(),
                    "ordinal did not increase",
                )?;
                if mode == "per-request" {
                    require(
                        value["builds"].as_u64() > previous["builds"].as_u64(),
                        "per-request app was not rebuilt",
                    )?;
                }
            }
            seen.insert(instance.into(), value.clone());
            records.push(annotate(
                reply,
                json!({"guest":value,"adapter":name,"mode":mode,"repetition":repetition,"workload":"probe"}),
            ))?;
        }
    }
    if !reused {
        records.push(json!({"status":"unverified","adapter":name,"mode":mode,"reason":"no same-guest sequential reuse"}))?;
    }
    if mode == "retained" && name != "axum" {
        *failure = json!({"phase":"binding_failure"});
        let failed = req(port, "/binding-failure")?;
        require(
            failed["status"] == 503,
            "injected required binding did not fail",
        )?;
        let failed_guest: Value = serde_json::from_str(failed["body"].as_str().unwrap())?;
        require(
            failed_guest["error"].as_str().is_some_and(|error| {
                error.starts_with("kv store open failed:") && !error.contains("missing_fixture_kv")
            }),
            "binding failure lost its sanitized open category",
        )?;
        let recovered = guest(&req(port, "/probe/after-binding-failure")?)?;
        require(
            recovered["builds"] == 1,
            "app rebuilt after binding failure",
        )?;
        records.push(json!({"adapter":name,"mode":mode,"status":if failed_guest["instance"] == recovered["instance"] {"pass"} else {"unverified"},"assertion":"same_guest_binding_recovery","source":"injected_required_binding"}))?;
    }
    *failure = json!({"phase":"bindings"});
    let bindings = req(port, "/bindings")?;
    require(
        guest(&bindings)? == json!({"config":true,"kv":true,"secrets":true,"unknown":false}),
        "binding mismatch",
    )?;
    records.push(annotate(
        bindings,
        json!({"adapter":name,"mode":mode,"assertion":"binding_reads"}),
    ))?;
    *failure = json!({"phase":"cookies"});
    let cookies = req(port, "/cookies")?;
    if cookie_values(&cookies["headers"]) != vec!["first=1; Path=/", "second=2; Path=/"] {
        records.push(annotate(cookies,json!({"status":"fail","adapter":name,"mode":mode,"phase":"cookies","reason":"duplicate Set-Cookie values were not preserved"})))?;
    }
    *failure = json!({"phase":"rendered_error"});
    require(
        req(port, "/rendered-error")?["status"] == 400,
        "rendered error mismatch",
    )?;
    if name != "axum" {
        *failure = json!({"phase":"alternate_app"});
        require(
            req(port, "/other")?["body"] == "other-app",
            "alternate app mismatch",
        )?;
    }
    *failure = json!({"phase":"overlap"});
    overlap_check(name, mode, port, records)
}
fn overlap_check(name: &str, mode: &str, port: u16, records: &mut Records) -> Result<()> {
    let mut observed_overlap = false;
    for attempt in 0..3 {
        let backend = Backend::start()?;
        let tokens = [token(), token()];
        let replies = thread::scope(|scope| {
            let jobs = tokens
                .iter()
                .map(|token| {
                    let url = backend.url("/barrier");
                    scope.spawn(move || {
                        net::request(port, &format!("/overlap/{token}"), token, Some(&url), None)
                    })
                })
                .collect::<Vec<_>>();
            jobs.into_iter()
                .map(|job| {
                    job.join()
                        .map_err(|_| "overlap request thread panicked".into())
                        .and_then(|v| v)
                })
                .collect::<Result<Vec<_>>>()
        })?;
        let values = replies.iter().map(guest).collect::<Result<Vec<_>>>()?;
        for (value, token) in values.iter().zip(&tokens) {
            require(
                value["token"] == *token
                    && value["before"]["token"] == *token
                    && value["path"] == *token
                    && value["before"]["path"] == *token,
                "overlap request-local values changed",
            )?;
        }
        if let Err(error) = validate_overlap(&values[0], &values[1]) {
            records.push(json!({"event":"overlap_observation","adapter":name,"mode":mode,"attempt":attempt,"reason":error.to_string(),"observations":values}))?;
            continue;
        }
        observed_overlap = true;
        break;
    }
    records.push(json!({"status":if observed_overlap {"pass"} else {"unverified"},"adapter":name,"mode":mode,"assertion":"same_guest_overlap","attempt_limit":3}))
}

fn faults(exe: &Path, run: &Path, records: &mut Records) -> Result<()> {
    let backend = Backend::start()?;
    let config = run.join("faults.toml");
    fs::write(
        &config,
        format!(
            "{}\n[local_server.backends.fault-origin]\nurl = {:?}\n",
            fs::read_to_string(root().join("crates/fixture-fastly/fastly.toml"))?,
            backend.url("/")
        ),
    )?;
    for (index, path) in [
        "/constructor-panic",
        "/collect-error",
        "/inbound-error",
        "/terminal-store-error",
        "/recoverable-store-error",
    ]
    .iter()
    .enumerate()
    {
        let log = run.join(format!("fault-{index}.log"));
        let outcome = (|| -> Result<()> {
            let port = net::port()?;
            let _process = Process::start(
                &mut fastly_command(exe, &config, run, "faults", port),
                &log,
                port,
            )?;
            let malformed_token = token();
            let rejected = net::malformed_request(port, &malformed_token)?;
            require(
                rejected.contains(" 400 "),
                format!("malformed request was not rejected: {rejected}"),
            )?;
            require(
                !logs(&log).iter().any(|e| e["token"] == malformed_token),
                "malformed request reached callback unexpectedly",
            )?;
            records.push(json!({"status":"pass","assertion":"malformed_rejected_before_dispatch","token":malformed_token,"status_line":rejected}))?;
            let failed_token = token();
            let reply = if *path == "/collect-error" {
                let failure = net::request(port, path, &failed_token, None, None)
                    .err()
                    .ok_or("owned egress must abandon the failed response stream")?;
                if let Some(partial) = failure.downcast_ref::<net::InterruptedResponse>() {
                    require(
                        partial.status == 200 && partial.body.is_empty(),
                        "egress source failure sent a replacement response",
                    )?;
                    json!({"status":partial.status,"body":"","interrupted":true})
                } else {
                    // An immediate source error can reset delivery before the head is observed.
                    require(
                        failure.downcast_ref::<net::ResponseHeadAborted>().is_some(),
                        format!("unexpected response-head failure: {failure}"),
                    )?;
                    json!({"no_response_head":true,"interrupted":true,"error":failure.to_string()})
                }
            } else {
                net::request(port, path, &failed_token, None, None)?
            };
            let recoverable = *path == "/recoverable-store-error";
            let rendered_ingress = *path == "/inbound-error";
            require(
                (*path == "/collect-error" && reply["no_response_head"] == true)
                    || reply["status"]
                        == if *path == "/collect-error" {
                            200
                        } else if recoverable {
                            503
                        } else {
                            500
                        },
                format!("fault not observed: {path}: {reply}"),
            )?;
            let following = req(port, "/probe/recovered")?;
            let value = guest(&following)?;
            let mut failed_guest = Value::Null;
            await_evidence(|| {
                let events = logs(&log);
                failed_guest = events
                    .iter()
                    .find(|e| e["event"] == "request_start" && e["token"] == failed_token)
                    .map(|e| e["instance"].clone())
                    .ok_or("missing fault attempt")?;
                let event = if *path == "/constructor-panic" {
                    "constructor_panic"
                } else if rendered_ingress {
                    "ingress_error_response"
                } else if *path == "/collect-error" {
                    "post_commit_error"
                } else {
                    "adapter_error"
                };
                require(
                    events
                        .iter()
                        .any(|e| e["event"] == event && e["instance"] == failed_guest),
                    "missing actual failure boundary",
                )
            })?;
            if *path == "/collect-error" {
                validate_fastly_delivery_report(
                    &failed_token,
                    &logs(&log),
                    &json!({"disposition":"routed",
                    "commitment":"Committed","body_kind":"Application","outcome":"SourceError",
                    "fallback":null,"may_send_native":false}),
                )?;
                let terminal = logs(&log)
                    .into_iter()
                    .filter(|event| {
                        event["event"] == "egress_terminal" && event["token"] == failed_token
                    })
                    .collect::<Vec<_>>();
                require(
                    terminal.len() == 1
                        && terminal[0]["outcome"] == "SourceError"
                        && terminal[0]["body_kind"] == "Application"
                        && terminal[0]["bytes_written"] == 0
                        && terminal[0]["fallback"].is_null(),
                    "immediate source failure must terminate once without fallback delivery",
                )?;
            }
            if matches!(*path, "/recoverable-store-error" | "/terminal-store-error") {
                validate_fastly_delivery_report(
                    &failed_token,
                    &logs(&log),
                    &json!({"disposition":"no-attempt",
                    "commitment":null,"body_kind":null,"outcome":null,"fallback":null,"may_send_native":true,
                    "no_attempt_reason":"required-store"}),
                )?;
            }
            if recoverable || rendered_ingress {
                let status = if value["instance"] == failed_guest {
                    "pass"
                } else {
                    "unverified"
                };
                require(
                    value["builds"] == 1,
                    "retained app rebuilt after handled failure",
                )?;
                require(
                    guest(&req(port, "/bindings")?)?
                        == json!({"config":true,"kv":true,"secrets":true,"unknown":false}),
                    "bindings failed to recover",
                )?;
                if rendered_ingress {
                    records.push(json!({"status":status,"assertion":"admitted_body_failure_rendered","instance":failed_guest,"source":"canonical_admitted_request_dispatch"}))?;
                } else {
                    records.push(json!({"status":status,"assertion":"injected_selector_failure_recovery","instance":failed_guest,"source":"custom_callback_handles_real_registry_error"}))?;
                }
            } else {
                require(
                    value["instance"] != failed_guest,
                    "terminal failure guest reused",
                )?;
                if *path != "/constructor-panic" {
                    await_evidence(|| {
                        require(
                            logs(&log).iter().any(|e| {
                                e["event"] == "sdk_summary"
                                    && e["instance"] == failed_guest
                                    && e["attempted"] == 1
                            }),
                            "missing terminal attempt summary",
                        )
                    })?;
                }
                records.push(json!({"status":"pass","assertion":"terminal_fault","path":path,"failed_guest":failed_guest,"source":"controlled_fixture_input"}))?;
            }
            records.push(annotate(
                reply,
                json!({"variant":"faults","repetition":index,"assertion":path}),
            ))?;
            Ok(())
        })();
        settle(
            records,
            &log,
            json!({"variant":"faults","repetition":index}),
            json!({"adapter":"fastly","path":path}),
            outcome,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_kv_success_body_requires_request_and_instance_correlation() {
        let response = |body: Value| json!({"status":200,"body":body.to_string()});
        let valid = json!({"token":"request","instance":"one"});
        validate_declared_kv_guest(&response(valid.clone()), "request", "one").unwrap();
        for body in [
            json!({}),
            json!({"token":"request"}),
            json!({"instance":"one"}),
            json!({"token":"other","instance":"one"}),
            json!({"token":"request","instance":"other"}),
        ] {
            assert!(validate_declared_kv_guest(&response(body), "request", "one").is_err());
        }
        assert!(
            validate_declared_kv_guest(
                &json!({"status":500,"body":valid.to_string()}),
                "request",
                "one"
            )
            .is_err()
        );
    }

    #[test]
    fn declared_kv_linked_result_requires_a_present_field_even_when_null() {
        let response = |body: Value| json!({"status":200,"body":body.to_string()});
        validate_declared_kv_linked_response(&response(json!({"kv_outcome":null})), &json!(null))
            .unwrap();
        assert!(validate_declared_kv_linked_response(&response(json!({})), &json!(null)).is_err());
        assert!(
            validate_declared_kv_linked_response(
                &response(json!({"kv_outcome":"stale"})),
                &json!(null)
            )
            .is_err()
        );
        validate_declared_kv_linked_response(&response(json!({"kv_outcome":[]})), &json!([]))
            .unwrap();
        assert!(
            validate_declared_kv_linked_response(
                &response(json!({"kv_outcome":["stale"]})),
                &json!([])
            )
            .is_err()
        );
        assert!(
            validate_declared_kv_linked_response(&json!({"status":500,"body":"{}"}), &json!(null))
                .is_err()
        );
    }

    #[test]
    fn declared_kv_operation_receipts_require_the_actual_typed_result() {
        let start = json!({"event":"request_start","token":"case","instance":"one"});
        let terminal = json!({"event":"egress_terminal","token":"case","instance":"one"});
        let entry = json!({"event":"declared_kv_enter","token":"case","instance":"one","store":"missing","operation":"write"});
        let result = json!({"event":"declared_kv_result","token":"case","instance":"one","store":"missing","operation":"write","outcome":"open-failed","reason":"MissingBinding"});
        let valid = vec![
            start.clone(),
            entry.clone(),
            result.clone(),
            terminal.clone(),
        ];
        validate_declared_kv_operation(
            &valid,
            "case",
            "one",
            "fastly",
            "/declared-kv/missing/write",
        )
        .unwrap();
        assert!(
            validate_declared_kv_operation(
                &valid,
                "case",
                "one",
                "spin",
                "/declared-kv/missing/write"
            )
            .is_err()
        );
        assert!(
            validate_declared_kv_operation(&valid, "case", "one", "fastly", "/declared-kv/free")
                .is_err()
        );
        validate_declared_kv_operation(
            &[start.clone(), terminal.clone()],
            "case",
            "one",
            "fastly",
            "/declared-kv/free",
        )
        .unwrap();
        for invalid in [
            vec![],
            vec![entry.clone()],
            vec![result.clone()],
            vec![entry.clone(), entry.clone(), result.clone()],
            vec![entry.clone(), result.clone(), result.clone()],
            vec![
                terminal.clone(),
                start.clone(),
                entry.clone(),
                result.clone(),
            ],
            vec![
                start.clone(),
                terminal.clone(),
                entry.clone(),
                result.clone(),
            ],
            vec![
                start.clone(),
                result.clone(),
                entry.clone(),
                terminal.clone(),
            ],
        ] {
            assert!(
                validate_declared_kv_operation(
                    &invalid,
                    "case",
                    "one",
                    "fastly",
                    "/declared-kv/missing/write"
                )
                .is_err()
            );
        }
        for (field, value) in [
            ("token", json!("other")),
            ("instance", json!("other")),
            ("store", json!("linked")),
            ("operation", json!("read")),
            ("outcome", json!("operation-failed")),
            ("reason", json!("Unavailable")),
            ("reason", json!(null)),
        ] {
            let mut changed = result.clone();
            changed[field] = value;
            assert!(
                validate_declared_kv_operation(
                    &[start.clone(), entry.clone(), changed, terminal.clone()],
                    "case",
                    "one",
                    "fastly",
                    "/declared-kv/missing/write"
                )
                .is_err()
            );
        }
        let mut linked_entry = entry.clone();
        linked_entry["store"] = json!("linked");
        let mut linked_result = result;
        linked_result["store"] = json!("linked");
        linked_result["outcome"] = json!("success");
        linked_result["reason"] = json!(null);
        validate_declared_kv_operation(
            &[start, linked_entry, linked_result, terminal],
            "case",
            "one",
            "spin",
            "/declared-kv/linked/write",
        )
        .unwrap();
    }

    #[test]
    fn declared_kv_final_validation_rejects_late_duplicates_and_missing_cases() {
        let mut checks = DeclaredKvChecks::default();
        let mut events = Vec::new();
        for (index, path) in [
            "/declared-kv/free",
            "/declared-kv/missing/read",
            "/declared-kv/missing/read",
            "/declared-kv/free",
            "/declared-kv/missing/write",
            "/declared-kv/missing/delete",
            "/declared-kv/missing/list",
            "/declared-kv/linked/write",
            "/declared-kv/linked/read",
            "/declared-kv/linked/list",
            "/declared-kv/linked/delete",
            "/declared-kv/linked/read",
            "/declared-kv/linked/list",
        ]
        .into_iter()
        .enumerate()
        {
            let token = index.to_string();
            let instance = if index < 7 { "one" } else { "healthy-turnover" };
            checks.requests.push(DeclaredKvRequest {
                token: token.clone(),
                path: path.to_owned(),
                instance: Some(instance.to_owned()),
            });
            events.push(json!({"event":"request_start","token":token,"instance":instance}));
            if path != "/declared-kv/free" {
                let (store, operation) = path
                    .strip_prefix("/declared-kv/")
                    .unwrap()
                    .split_once('/')
                    .unwrap();
                events.push(json!({"event":"declared_kv_enter","token":token,"instance":instance,"store":store,"operation":operation}));
                events.push(json!({"event":"declared_kv_result","token":token,"instance":instance,"store":store,"operation":operation,"outcome":if store == "missing" {"open-failed"} else {"success"},"reason":if store == "missing" {json!("MissingBinding")} else {json!(null)}}));
            }
            events.push(json!({"event":"egress_terminal","token":token,"instance":instance,"outcome":"Completed","body_kind":"Application","fallback":null}));
        }
        checks.instance = Some("one".to_owned());
        checks.validate(&events, "fastly").unwrap();
        let mut premature = events.clone();
        let result = premature
            .iter()
            .position(|event| event["token"] == "1" && event["event"] == "declared_kv_result")
            .unwrap();
        let terminal = premature
            .iter()
            .position(|event| event["token"] == "1" && event["event"] == "egress_terminal")
            .unwrap();
        premature.swap(result, terminal);
        assert!(checks.validate(&premature, "fastly").is_err());
        let mut late = events.clone();
        late.push(events[1].clone());
        assert!(checks.validate(&late, "fastly").is_err());
        let mut changed = events.clone();
        for event in &mut changed {
            if event["token"] == "2" {
                event["instance"] = json!("turned-over");
            }
        }
        assert!(checks.validate(&changed, "fastly").is_err());
        let mut natural_turnover = events.clone();
        for event in &mut natural_turnover {
            if event["token"] == "4" {
                event["instance"] = json!("next-missing-instance");
            }
        }
        checks.requests[4].instance = Some("next-missing-instance".to_owned());
        checks.validate(&natural_turnover, "fastly").unwrap();
        assert_eq!(checks.failed_instances().len(), 2);
        let warning = "fixture-logs :: WARN declared Fastly KV store 'missing_fixture_kv' failed to open: missing binding\n";
        assert!(checks.warning_count(warning, "fastly").is_err());
        assert_eq!(
            checks.warning_count(&warning.repeat(2), "fastly").unwrap(),
            2
        );
        assert!(checks.warning_count(&warning.repeat(3), "fastly").is_err());
        checks.requests.pop();
        assert!(checks.validate(&events, "fastly").is_err());
        checks.requests.clear();
        assert!(checks.validate(&events, "fastly").is_err());
    }

    #[test]
    fn declared_kv_provider_warnings_cannot_offset_between_instances() {
        let checks = DeclaredKvChecks {
            instance: Some("one".to_owned()),
            requests: [
                ("one", "/declared-kv/missing/read"),
                ("two", "/declared-kv/missing/write"),
                ("healthy", "/declared-kv/linked/read"),
            ]
            .into_iter()
            .enumerate()
            .map(|(index, (instance, path))| DeclaredKvRequest {
                token: index.to_string(),
                instance: Some(instance.to_owned()),
                path: path.to_owned(),
            })
            .collect(),
        };
        for (adapter, platform, reason) in [
            ("spin", "Spin", "invalid binding configuration"),
            ("cloudflare", "Cloudflare", "missing binding"),
        ] {
            let warning = |instance: &str| {
                json!({"event":"fixture_log","instance":instance,"level":"WARN","target":"edgezero::boot","message":format!("declared {platform} KV store 'missing_fixture_kv' failed to open: {reason}")}).to_string()
            };
            let valid = format!("{}\n{}\n", warning("one"), warning("two"));
            assert_eq!(checks.warning_count(&valid, adapter).unwrap(), 2);
            assert!(checks.warning_count(&warning("one"), adapter).is_err());
            assert!(
                checks
                    .warning_count(
                        &format!("{}\n{}\n", warning("one"), warning("one")),
                        adapter
                    )
                    .is_err()
            );
            assert!(
                checks
                    .warning_count(&format!("{valid}{}\n", warning("healthy")), adapter)
                    .is_err()
            );
            assert!(
                checks
                    .warning_count(&format!("{valid}{}\n", warning("two")), adapter)
                    .is_err()
            );
        }
    }

    #[test]
    fn declared_kv_lifecycle_rejects_missing_duplicate_failed_and_cross_instance() {
        let start = json!({"event":"request_start","token":"case","instance":"one"});
        let end = json!({"event":"egress_terminal","token":"case","instance":"one","outcome":"Completed","body_kind":"Application","fallback":null});
        let valid = vec![start.clone(), end.clone()];
        assert_eq!(
            validate_declared_kv_lifecycle(&valid, "case", Some("one"), "Completed").unwrap(),
            "one"
        );
        assert!(
            validate_declared_kv_lifecycle(&valid, "case", Some("other"), "Completed").is_err()
        );
        for invalid in [
            vec![],
            vec![start.clone()],
            vec![end.clone()],
            vec![start.clone(), start.clone(), end.clone()],
            vec![start.clone(), end.clone(), end.clone()],
        ] {
            assert!(validate_declared_kv_lifecycle(&invalid, "case", None, "Completed").is_err());
        }
        for (field, value) in [
            ("instance", json!("other")),
            ("token", json!("other")),
            ("outcome", json!("TransportError")),
            ("outcome", json!("HostHandoff")),
            ("body_kind", json!("Fallback")),
            ("fallback", json!("Completed")),
        ] {
            let mut changed = end.clone();
            changed[field] = value;
            assert!(
                validate_declared_kv_lifecycle(
                    &[start.clone(), changed],
                    "case",
                    None,
                    "Completed"
                )
                .is_err()
            );
        }
    }

    #[test]
    fn declared_kv_warning_counter_ignores_echoes_but_not_duplicates() {
        let warning =
            "declared Fastly KV store 'missing_fixture_kv' failed to open: missing binding";
        assert_eq!(
            declared_kv_warning_count(&format!("fixture-logs :: WARN {warning}\nWARN {warning}\n")),
            1
        );
        assert_eq!(
            declared_kv_warning_count(&format!(
                "fixture-logs :: WARN {warning}\nfixture-logs :: WARN {warning}\n"
            )),
            2
        );
        assert_eq!(
            declared_kv_warning_count("fixture-logs :: ordinary warning\n"),
            0
        );
    }
    #[test]
    fn provider_warning_receipts_require_instance_target_and_exact_category() {
        let valid = json!({"event":"fixture_log","instance":"one","level":"WARN","target":"edgezero::boot",
            "message":"declared Spin KV store 'missing_fixture_kv' failed to open: invalid binding configuration"});
        assert_eq!(
            provider_kv_warning_count(&valid.to_string(), "spin", "one"),
            1
        );
        assert_eq!(
            provider_kv_warning_count(&valid.to_string(), "spin", "other"),
            0
        );
        assert_eq!(
            provider_kv_warning_count(&format!("{valid}\n{valid}\n"), "spin", "one"),
            2
        );
        for (field, value) in [
            ("target", "other"),
            ("event", "other"),
            ("level", "INFO"),
            ("message", "provider-token"),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = json!(value);
            assert_eq!(
                provider_kv_warning_count(&invalid.to_string(), "spin", "one"),
                0
            );
        }
    }
    #[test]
    fn cargo_artifacts_select_the_fresh_executable_in_a_configured_target() {
        let output = concat!(
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"dependency\"},\"executable\":null}\n",
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"fixture-axum\"},\"executable\":\"/tmp/target/host-triple/debug/fixture-axum\"}\n",
            "{\"reason\":\"build-finished\",\"success\":true}\n",
        );
        assert_eq!(
            built_executable(output.as_bytes(), "fixture-axum").unwrap(),
            PathBuf::from("/tmp/target/host-triple/debug/fixture-axum")
        );
        assert!(built_executable(b"", "fixture-axum").is_err());
    }
    #[test]
    fn wrangler_runtime_configuration_does_not_rebuild_fingerprinted_artifacts() {
        let package = root().join("crates/fixture-cloudflare");
        let staged = Path::new("/run/artifacts/build/worker/shim.mjs");
        let config = wrangler_runtime_config(&package, staged).unwrap();
        assert!(!config.contains("[build]") && !config.contains("worker-build"));
        assert!(config.contains(&format!("main = {staged:?}")));
        assert!(config.contains("binding = \"fixture_kv\""));
    }
    #[test]
    fn builds_share_a_fixed_target_directory() {
        let command = cargo_build();
        let target = root().join("target");
        assert!(
            command
                .get_envs()
                .any(|(key, value)| key == "CARGO_TARGET_DIR" && value == Some(target.as_os_str()))
        );
        assert_eq!(command.get_current_dir(), Some(root().as_path()));
    }
    #[test]
    fn served_artifacts_are_copies_inside_the_run_directory() {
        let run = std::env::temp_dir().join(format!("fixture-harness-stage-{}", token()));
        let source = run.join("source/fixture-fastly-retained-app.wasm");
        fs::create_dir_all(source.parent().unwrap()).unwrap();
        fs::write(&source, b"first").unwrap();
        let staged = stage(&source, &run).unwrap();
        assert_eq!(staged, wasm(&run, "c"));
        fs::write(&source, b"second").unwrap();
        assert_eq!(fs::read(&staged).unwrap(), b"first");
        fs::remove_dir_all(&run).unwrap();
    }
}

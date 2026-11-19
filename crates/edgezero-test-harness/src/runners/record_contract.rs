//! Target-matched public-handle Fastly record conformance, without native SDK stubs.

use super::*;
use std::sync::{Arc, Barrier};

const BIN: &str = "fixture-fastly-record-contract";

fn validate_receipt(receipt: &Value) -> Result<()> {
    require(
        receipt["record_contract"] == "passed"
            && receipt["provider"] == "fastly-native"
            && receipt["executed_suites"] == 1,
        "missing or invalid native record contract receipt",
    )
}

fn validate_reply(reply: &Value) -> Result<Value> {
    let receipt = guest(reply)?;
    validate_receipt(&receipt)?;
    Ok(receipt)
}

fn validate_ready(reply: &Value) -> Result<()> {
    let ready = guest(reply)?;
    require(
        ready["record_ready"] == true
            && ready["provider"] == "fastly-native"
            && ready.get("record_contract").is_none()
            && ready.get("executed_suites").is_none(),
        "invalid read-only record readiness response",
    )
}

pub(super) fn validate_events(events: &[Value]) -> Result<()> {
    require(
        !events.iter().any(|event| event["status"] == "fail"),
        "failed record contract evidence",
    )?;
    let suites = events
        .iter()
        .filter(|event| event["event"] == "fastly-record-contract")
        .collect::<Vec<_>>();
    require(
        suites.len() == 1 && suites[0]["status"] == "pass" && suites[0]["executed_suites"] == 1,
        "missing or duplicate record contract suite",
    )?;
    validate_receipt(&suites[0]["receipt"])?;
    for mode in ["add", "cas"] {
        let races = events
            .iter()
            .filter(|event| event["event"] == "fastly-record-race" && event["mode"] == mode)
            .collect::<Vec<_>>();
        require(
            races.len() == 1
                && races[0]["status"] == "pass"
                && races[0]["independent_writers"] == 2
                && races[0]["written"] == 1
                && races[0]["precondition_failed"] == 1,
            "missing native record race evidence",
        )?;
    }
    let pages = events
        .iter()
        .filter(|event| event["event"] == "fastly-record-existence")
        .collect::<Vec<_>>();
    require(
        pages.len() == 1
            && pages[0]["status"] == "pass"
            && pages[0]["receipt"]["descendants"] == 1001
            && pages[0]["receipt"]["native_pages"]
                .as_u64()
                .is_some_and(|count| count >= 2)
            && pages[0]["receipt"]["exact_absent_before"] == true
            && pages[0]["receipt"]["exact_present"] == true
            && pages[0]["receipt"]["exact_absent_after"] == true,
        "missing native exact-existence continuation evidence",
    )?;
    let cleanup = events
        .iter()
        .filter(|event| event["event"] == "fastly-record-cleanup")
        .collect::<Vec<_>>();
    require(
        cleanup.len() == 1
            && cleanup[0]["status"] == "pass"
            && cleanup[0]["receipt"]["remaining_keys"] == 0,
        "missing native record cleanup evidence",
    )
}

fn race_clients(port: u16, paths: [String; 2]) -> Result<Vec<Value>> {
    let barrier = Arc::new(Barrier::new(3));
    thread::scope(|scope| {
        let clients = paths
            .into_iter()
            .map(|path| {
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    guest(&req(port, &path)?)
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        clients
            .into_iter()
            .map(|client| {
                client
                    .join()
                    .map_err(|_| "native record race client panicked")?
            })
            .collect()
    })
}

fn run_native_cases(port: u16, records: &mut Records) -> Result<()> {
    let prepared = guest(&req(port, "/record-race/prepare")?)?;
    require(
        prepared["record_race"] == "prepared",
        "native race preparation failed",
    )?;
    let generation = prepared["generation"]
        .as_u64()
        .ok_or("missing native race generation")?;
    for mode in ["add", "cas"] {
        let path = |writer| {
            if mode == "add" {
                format!("/record-race/add/{writer}")
            } else {
                format!("/record-race/cas/{writer}/{generation}")
            }
        };
        let replies = race_clients(port, [path("a"), path("b")])?;
        require(
            replies.iter().all(|reply| reply["record_race"] == mode)
                && replies
                    .iter()
                    .filter(|reply| reply["outcome"] == "written")
                    .count()
                    == 1
                && replies
                    .iter()
                    .filter(|reply| reply["outcome"] == "precondition-failed")
                    .count()
                    == 1,
            "native record race did not have exactly one winner",
        )?;
        let winner = replies
            .iter()
            .find(|reply| reply["outcome"] == "written")
            .unwrap();
        let stored = guest(&req(port, &format!("/record-race/result/{mode}"))?)?;
        require(
            stored["body"] == winner["writer"],
            "native race stored the losing value",
        )?;
        records.push(
            json!({"event":"fastly-record-race","status":"pass","mode":mode,
            "independent_writers":2,"written":1,"precondition_failed":1,
            "winner":winner["writer"],"clients":replies,"stored":stored,"qualification":false}),
        )?;
    }
    let cleaned = guest(&req(port, "/record-race/cleanup")?)?;
    require(
        cleaned["record_race"] == "cleaned",
        "native race cleanup failed",
    )?;

    // Batch test-fixture setup and cleanup across requests. This is not a
    // provider operation-budget claim or a production mutation retry policy.
    for operation in ["seed", "cleanup"] {
        for start in (0..1001).step_by(100) {
            let end = (start + 100).min(1001);
            let reply = guest(&req(
                port,
                &format!("/record-existence/{operation}/{start}/{end}"),
            )?)?;
            require(
                reply["record_existence"] == operation && reply["count"] == end - start,
                "native existence batch incomplete",
            )?;
        }
        if operation == "seed" {
            let receipt = guest(&req(port, "/record-existence/check")?)?;
            require(
                receipt["record_existence"] == "passed",
                "native exact existence failed",
            )?;
            records.push(json!({"event":"fastly-record-existence","status":"pass","receipt":receipt,"qualification":false}))?;
        }
    }
    let receipt = guest(&req(port, "/record-cases/clean")?)?;
    require(
        receipt["record_cases"] == "clean" && receipt["remaining_keys"] == 0,
        "native fixture cleanup failed",
    )?;
    records.push(json!({"event":"fastly-record-cleanup","status":"pass","receipt":receipt,"qualification":false}))
}

pub(super) fn run(exe: &Path, parent: &Path, records: &mut Records) -> Result<()> {
    let run = parent.join("record-contract");
    fs::create_dir_all(&run)?;

    let output = Command::new(exe).arg("--version").output()?;
    require(output.status.success(), "Viceroy version command failed")?;
    let version = String::from_utf8(output.stdout)?.trim().to_owned();
    let pins = fs::read_to_string(repository_root().join(".tool-versions"))?;
    let pin = pins
        .lines()
        .find_map(|line| line.strip_prefix("viceroy "))
        .map(str::trim)
        .ok_or("missing Viceroy pin")?;
    require(
        version.split_whitespace().nth(1) == Some(pin),
        "record contract requires the pinned Viceroy runtime",
    )?;

    let mut build = cargo_build()?;
    // Keep explicit caller target ownership instead of silently using the
    // fixture workspace's historical target directory.
    if let Some(target) = std::env::var_os("CARGO_TARGET_DIR") {
        build.env("CARGO_TARGET_DIR", target);
    }
    let output = build
        .args([
            "build",
            "--locked",
            "--offline",
            "--release",
            "-p",
            "fixture-fastly",
            "--features",
            "records",
            "--bin",
            BIN,
            "--target",
            "wasm32-wasip1",
            "--message-format=json-render-diagnostics",
        ])
        .stderr(Stdio::from(fs::File::create(run.join("build.log"))?))
        .output()?;
    fs::write(run.join("build.jsonl"), &output.stdout)?;
    require(
        output.status.success(),
        "Fastly record fixture build failed",
    )?;
    let wasm = stage(&built_executable(&output.stdout, BIN)?, &run)?;
    let config = run.join("fastly.toml");
    fs::copy(
        fixture_root().join("crates/fixture-fastly/fastly.toml"),
        &config,
    )?;
    records.push(json!({
        "event":"build", "adapter":"fastly", "suite":"record-contract",
        "variant":"fastly-record-contract",
        "target":"wasm32-wasip1", "artifact":artifact(&wasm)?,
        "config":artifact(&config)?, "runtime":artifact(exe)?, "runtime_version":version,
    }))?;

    let log = run.join("runtime.log");
    let outcome = (|| -> Result<()> {
        let port = net::port()?;
        let mut process = Process::start(
            &mut fastly_command(exe, &config, &run, "record-contract", port)?,
            &log,
            port,
        )?;
        // Attest the launch through a route that never opens KV. The mutating
        // contract is dispatched once only after startup identity is verified.
        let ready = req(port, "/record-ready")?;
        validate_ready(&ready)?;
        records.push(json!({
            "event":"fastly-record-ready", "status":"pass", "client":ready,
            "qualification":false,
        }))?;
        let reply = req(port, "/record-contract")?;
        let receipt = validate_reply(&reply)?;
        run_native_cases(port, records)?;
        process.finish()?;
        records.push(json!({
            "event":"fastly-record-contract", "adapter":"fastly", "status":"pass",
            "executed_suites":1, "receipt":receipt, "client":reply,
            "qualification":false,
        }))?;
        Ok(())
    })();
    settle(
        records,
        &log,
        json!({"adapter":"fastly","suite":"record-contract","qualification":false}),
        json!({"phase":"record-contract"}),
        outcome,
    )
}

const PORTABLE_BIN: &str = "fixture-fastly-record-contract-portable";
/// Each portable KV ID and the provider label its receipts must carry.
const PORTABLE: [(&str, &str); 2] = [
    ("dynamodb_records", "dynamodb-portable"),
    ("celld_records", "celld-portable"),
];

fn validate_portable_ready(reply: &Value) -> Result<()> {
    let ready = guest(reply)?;
    require(
        ready["record_ready"] == true
            && ready["providers"] == json!(PORTABLE.map(|(_, label)| label))
            && ready.get("record_contract").is_none()
            && ready.get("executed_suites").is_none(),
        "invalid read-only portable record readiness response",
    )
}

fn validate_portable_receipt(receipt: &Value, provider: &str) -> Result<()> {
    require(
        receipt["record_contract"] == "passed"
            && receipt["provider"] == provider
            && receipt["executed_suites"] == 1,
        "missing or invalid portable record contract receipt",
    )
}

/// One provider's evidence of `event`; exactly one must exist.
fn only<'a>(
    events: &'a [Value],
    event: &str,
    provider: &str,
    mode: Option<&str>,
) -> Result<&'a Value> {
    let matching = events
        .iter()
        .filter(|record| {
            record["event"] == event
                && record["provider"] == provider
                && mode.is_none_or(|expected| record["mode"] == expected)
        })
        .collect::<Vec<_>>();
    require(
        matching.len() == 1 && matching[0]["status"] == "pass",
        format!("missing or duplicate {event} evidence for {provider}"),
    )?;
    Ok(matching[0])
}

/// Every portable provider needs its own complete, passing evidence. One provider's
/// receipts never satisfy another's, and no other provider label may appear.
pub(super) fn validate_portable_events(events: &[Value]) -> Result<()> {
    require(
        !events.iter().any(|event| event["status"] == "fail"),
        "failed portable record contract evidence",
    )?;
    let labels = PORTABLE.map(|(_, label)| label);
    require(
        events
            .iter()
            .filter_map(|event| event["provider"].as_str())
            .all(|provider| labels.contains(&provider)),
        "unexpected provider in portable record evidence",
    )?;
    for label in labels {
        let suite = only(events, "portable-record-contract", label, None)?;
        require(
            suite["executed_suites"] == 1,
            "portable record contract suite did not execute once",
        )?;
        validate_portable_receipt(&suite["receipt"], label)?;
        for mode in ["add", "cas"] {
            let race = only(events, "portable-record-race", label, Some(mode))?;
            require(
                race["independent_writers"] == 2
                    && race["written"] == 1
                    && race["precondition_failed"] == 1,
                format!("missing portable {mode} race evidence for {label}"),
            )?;
        }
        let pages = &only(events, "portable-record-existence", label, None)?["receipt"];
        require(
            pages["descendants"] == 1001
                && pages["native_pages"]
                    .as_u64()
                    .is_some_and(|count| count >= 2)
                && pages["exact_absent_before"] == true
                && pages["exact_present"] == true
                && pages["exact_absent_after"] == true,
            format!("missing portable exact-existence continuation evidence for {label}"),
        )?;
        let native = &only(events, "portable-record-native-read", label, None)?["receipt"];
        require(
            native["record_native"] == "passed"
                && native["native_body_matched"] == true
                && native["native_generation_cas"] == true
                && native["deleted"] == true,
            format!("missing native-to-portable interop evidence for {label}"),
        )?;
        let cleanup = only(events, "portable-record-cleanup", label, None)?;
        require(
            cleanup["receipt"]["remaining_keys"] == 0,
            format!("missing portable cleanup evidence for {label}"),
        )?;
    }
    Ok(())
}

/// The native suite's cases against one portable KV ID, under `/record/{id}`.
fn run_portable_cases(port: u16, id: &str, label: &str, records: &mut Records) -> Result<()> {
    let base = format!("/record/{id}");
    let prepared = guest(&req(port, &format!("{base}/record-race/prepare"))?)?;
    require(
        prepared["record_race"] == "prepared",
        "portable race preparation failed",
    )?;
    let generation = prepared["generation"]
        .as_u64()
        .ok_or("missing portable race generation")?;
    for mode in ["add", "cas"] {
        let path = |writer| {
            if mode == "add" {
                format!("{base}/record-race/add/{writer}")
            } else {
                format!("{base}/record-race/cas/{writer}/{generation}")
            }
        };
        let replies = race_clients(port, [path("a"), path("b")])?;
        let written = replies
            .iter()
            .filter(|reply| reply["outcome"] == "written")
            .count();
        let rejected = replies
            .iter()
            .filter(|reply| reply["outcome"] == "precondition-failed")
            .count();
        require(
            replies.iter().all(|reply| reply["record_race"] == mode)
                && written == 1
                && rejected == 1,
            format!("portable {mode} race for {label} did not have exactly one winner"),
        )?;
        let winner = replies
            .iter()
            .find(|reply| reply["outcome"] == "written")
            .ok_or("missing portable race winner")?;
        let stored = guest(&req(port, &format!("{base}/record-race/result/{mode}"))?)?;
        require(
            stored["body"] == winner["writer"],
            "portable race stored the losing value",
        )?;
        records.push(
            json!({"event":"portable-record-race","status":"pass","provider":label,
            "mode":mode,"independent_writers":2,"written":1,"precondition_failed":1,
            "winner":winner["writer"],"clients":replies,"stored":stored,"qualification":false}),
        )?;
    }
    let cleaned = guest(&req(port, &format!("{base}/record-race/cleanup"))?)?;
    require(
        cleaned["record_race"] == "cleaned",
        "portable race cleanup failed",
    )?;

    // Batch fixture setup and cleanup across requests. This is not a provider
    // operation-budget claim or a production mutation retry policy.
    for operation in ["seed", "cleanup"] {
        for start in (0..1001).step_by(100) {
            let end = (start + 100).min(1001);
            let reply = guest(&req(
                port,
                &format!("{base}/record-existence/{operation}/{start}/{end}"),
            )?)?;
            require(
                reply["record_existence"] == operation && reply["count"] == end - start,
                "portable existence batch incomplete",
            )?;
        }
        if operation == "seed" {
            let receipt = guest(&req(port, &format!("{base}/record-existence/check"))?)?;
            require(
                receipt["record_existence"] == "passed",
                "portable exact existence failed",
            )?;
            records.push(json!({"event":"portable-record-existence","status":"pass",
                "provider":label,"receipt":receipt,"qualification":false}))?;
        }
    }
    let native = guest(&req(port, &format!("/record-native/{id}"))?)?;
    require(
        native["record_native"] == "passed" && native["provider"] == label,
        format!("native-to-portable interop failed for {label}"),
    )?;
    records.push(
        json!({"event":"portable-record-native-read","status":"pass",
        "provider":label,"receipt":native,"qualification":false}),
    )?;
    let receipt = guest(&req(port, &format!("{base}/record-cases/clean"))?)?;
    require(
        receipt["record_cases"] == "clean" && receipt["remaining_keys"] == 0,
        "portable fixture cleanup failed",
    )?;
    records.push(
        json!({"event":"portable-record-cleanup","status":"pass","provider":label,
        "receipt":receipt,"qualification":false}),
    )
}

/// An absolute regular file named by `name`; the suite never guesses inputs.
fn explicit_file(name: &str) -> Result<PathBuf> {
    let path = std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{name} is required; the portable suite never skips"))?;
    require(
        path.is_absolute() && path.is_file(),
        format!("{name} must name an absolute file"),
    )?;
    Ok(path)
}

/// Portable DynamoDB and celld providers through the canonical Fastly path. The caller
/// provisions the services and supplies the Viceroy configuration and trust root.
pub(super) fn run_portable(exe: &Path, parent: &Path, records: &mut Records) -> Result<()> {
    let run = parent.join("portable-record-contract");
    fs::create_dir_all(&run)?;
    let supplied = explicit_file("EDGEZERO_PORTABLE_RECORDS_CONFIG")?;
    let trust = explicit_file("EDGEZERO_PORTABLE_RECORDS_CA")?;

    let output = Command::new(exe).arg("--version").output()?;
    require(output.status.success(), "Viceroy version command failed")?;
    let version = String::from_utf8(output.stdout)?.trim().to_owned();
    let pins = fs::read_to_string(repository_root().join(".tool-versions"))?;
    let pin = pins
        .lines()
        .find_map(|line| line.strip_prefix("viceroy "))
        .map(str::trim)
        .ok_or("missing Viceroy pin")?;
    require(
        version.split_whitespace().nth(1) == Some(pin),
        "portable record contract requires the pinned Viceroy runtime",
    )?;

    let mut build = cargo_build()?;
    if let Some(target) = std::env::var_os("CARGO_TARGET_DIR") {
        build.env("CARGO_TARGET_DIR", target);
    }
    let output = build
        .args([
            "build",
            "--locked",
            "--offline",
            "--release",
            "-p",
            "fixture-fastly",
            "--features",
            "portable-records",
            "--bin",
            PORTABLE_BIN,
            "--target",
            "wasm32-wasip1",
            "--message-format=json-render-diagnostics",
        ])
        .stderr(Stdio::from(fs::File::create(run.join("build.log"))?))
        .output()?;
    fs::write(run.join("build.jsonl"), &output.stdout)?;
    require(
        output.status.success(),
        "portable Fastly record fixture build failed",
    )?;
    let wasm = stage(&built_executable(&output.stdout, PORTABLE_BIN)?, &run)?;
    // The configuration carries test-only credentials. Record its identity, not a copy.
    records.push(json!({
        "event":"build", "adapter":"fastly", "suite":"portable-record-contract",
        "variant":"fastly-portable-record-contract", "target":"wasm32-wasip1",
        "artifact":artifact(&wasm)?, "config":artifact(&supplied)?["fnv1a64"],
        "trust_root":artifact(&trust)?, "runtime":artifact(exe)?, "runtime_version":version,
    }))?;

    let log = run.join("runtime.log");
    let outcome = (|| -> Result<()> {
        let port = net::port()?;
        let mut command = fastly_command(exe, &supplied, &run, "record-contract-portable", port)?;
        // Only Viceroy trusts the test root, and only for this run.
        command.env("SSL_CERT_FILE", &trust);
        let mut process = Process::start(&mut command, &log, port)?;
        let ready = req(port, "/record-ready")?;
        validate_portable_ready(&ready)?;
        records.push(json!({
            "event":"portable-record-ready", "status":"pass", "client":ready,
            "qualification":false,
        }))?;
        for (id, label) in PORTABLE {
            let reply = req(port, &format!("/record-contract/{id}"))?;
            let receipt = guest(&reply)?;
            validate_portable_receipt(&receipt, label)?;
            records.push(json!({
                "event":"portable-record-contract", "adapter":"fastly", "status":"pass",
                "provider":label, "executed_suites":1, "receipt":receipt, "client":reply,
                "qualification":false,
            }))?;
            run_portable_cases(port, id, label, records)?;
        }
        process.finish()?;
        Ok(())
    })();
    // The launch configuration copies test credentials; do not keep it as evidence.
    for entry in fs::read_dir(&run)?.flatten() {
        if entry.file_name().to_string_lossy().starts_with("launch-") {
            fs::remove_file(entry.path())?;
        }
    }
    settle(
        records,
        &log,
        json!({"adapter":"fastly","suite":"portable-record-contract","qualification":false}),
        json!({"phase":"portable-record-contract"}),
        outcome,
    )?;
    // Judge the persisted evidence, not only the in-process control flow.
    validate_portable_events(&super::ci::read_events(
        &parent.join("events.jsonl"),
        super::ci::CI_ARTIFACT_RECORDS,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(status: u16, receipt: Value) -> Value {
        json!({"status":status,"body":receipt.to_string()})
    }

    #[test]
    fn readiness_is_distinct_from_mutating_contract_execution() {
        let ready = reply(200, json!({"record_ready":true,"provider":"fastly-native"}));
        assert!(validate_ready(&ready).is_ok());
        assert!(validate_reply(&ready).is_err());
        assert!(
            validate_ready(&reply(
                200,
                json!({
                    "record_ready":true,"provider":"fastly-native","executed_suites":1,
                })
            ))
            .is_err()
        );
        assert!(
            validate_ready(&reply(
                500,
                json!({"record_ready":true,"provider":"fastly-native"})
            ))
            .is_err()
        );
    }

    #[test]
    fn persisted_record_evidence_requires_one_native_suite() {
        let event = json!({
            "event":"fastly-record-contract", "status":"pass", "executed_suites":1,
            "receipt":{"record_contract":"passed","provider":"fastly-native","executed_suites":1},
        });
        let mut events = vec![event];
        for mode in ["add", "cas"] {
            events.push(
                json!({"event":"fastly-record-race","status":"pass","mode":mode,
                "independent_writers":2,"written":1,"precondition_failed":1}),
            );
        }
        events.push(
            json!({"event":"fastly-record-existence","status":"pass","receipt":{
                "descendants":1001,"native_pages":2,"exact_absent_before":true,
                "exact_present":true,"exact_absent_after":true,
            }}),
        );
        events.push(
            json!({"event":"fastly-record-cleanup","status":"pass","receipt":{"remaining_keys":0}}),
        );
        assert!(validate_events(&events).is_ok());
        assert!(validate_events(&[]).is_err());
        for index in 0..events.len() {
            let mut missing = events.clone();
            missing.remove(index);
            assert!(validate_events(&missing).is_err());
            let mut duplicate = events.clone();
            duplicate.push(events[index].clone());
            assert!(validate_events(&duplicate).is_err());
        }
        let mut failed = events.clone();
        failed.push(json!({"status":"fail"}));
        assert!(validate_events(&failed).is_err());
        let mut wrong = events;
        wrong[0]["receipt"]["executed_suites"] = json!(0);
        assert!(validate_events(&wrong).is_err());
    }

    #[test]
    fn receipt_requires_successful_native_nonzero_execution() {
        let receipt = json!({
            "record_contract":"passed", "provider":"fastly-native", "executed_suites":1,
        });
        assert!(validate_reply(&reply(200, receipt.clone())).is_ok());
        assert!(validate_reply(&reply(500, receipt.clone())).is_err());
        for (field, wrong) in [
            ("record_contract", json!("skipped")),
            ("provider", json!("stub")),
            ("executed_suites", json!(0)),
            ("executed_suites", json!(2)),
            ("executed_suites", json!("1")),
        ] {
            let mut invalid = receipt.clone();
            invalid[field] = wrong;
            assert!(validate_reply(&reply(200, invalid)).is_err());
        }
        assert!(validate_reply(&reply(200, json!({}))).is_err());
        assert!(validate_reply(&json!({"status":200,"body":"not json"})).is_err());
    }

    fn portable_events() -> Vec<Value> {
        let mut events = Vec::new();
        for (_, label) in PORTABLE {
            events.push(json!({"event":"portable-record-contract","status":"pass","provider":label,
                "executed_suites":1,"receipt":{"record_contract":"passed","provider":label,"executed_suites":1}}));
            for mode in ["add", "cas"] {
                events.push(
                    json!({"event":"portable-record-race","status":"pass","provider":label,
                    "mode":mode,"independent_writers":2,"written":1,"precondition_failed":1}),
                );
            }
            events.push(
                json!({"event":"portable-record-existence","status":"pass","provider":label,
                "receipt":{"descendants":1001,"native_pages":3,"exact_absent_before":true,
                    "exact_present":true,"exact_absent_after":true}}),
            );
            events.push(
                json!({"event":"portable-record-native-read","status":"pass","provider":label,
                "receipt":{"record_native":"passed","native_body_matched":true,
                    "native_generation_cas":true,"deleted":true}}),
            );
            events.push(
                json!({"event":"portable-record-cleanup","status":"pass","provider":label,
                "receipt":{"remaining_keys":0}}),
            );
        }
        events
    }

    #[test]
    fn portable_evidence_requires_every_provider_independently() {
        let events = portable_events();
        assert!(validate_portable_events(&events).is_ok());
        assert!(validate_portable_events(&[]).is_err());
        for index in 0..events.len() {
            let mut missing = events.clone();
            missing.remove(index);
            assert!(
                validate_portable_events(&missing).is_err(),
                "missing {index}"
            );
            let mut duplicate = events.clone();
            duplicate.push(events[index].clone());
            assert!(
                validate_portable_events(&duplicate).is_err(),
                "duplicate {index}"
            );
            // One provider's evidence relabelled as the other cannot satisfy both.
            let mut substituted = events.clone();
            let other = if events[index]["provider"] == "dynamodb-portable" {
                "celld-portable"
            } else {
                "dynamodb-portable"
            };
            substituted[index]["provider"] = json!(other);
            assert!(
                validate_portable_events(&substituted).is_err(),
                "substituted {index}"
            );
        }
        let mut failed = events.clone();
        failed.push(json!({"status":"fail"}));
        assert!(validate_portable_events(&failed).is_err());
        let mut foreign = events.clone();
        foreign.push(json!({"event":"client_completed","provider":"fastly-native"}));
        assert!(validate_portable_events(&foreign).is_err());
        for (field, value) in [
            ("executed_suites", json!(0)),
            ("executed_suites", json!("1")),
        ] {
            let mut wrong = events.clone();
            wrong[0]["receipt"][field] = value;
            assert!(validate_portable_events(&wrong).is_err());
        }
        let mut two_winners = events.clone();
        two_winners[1]["written"] = json!(2);
        two_winners[1]["precondition_failed"] = json!(0);
        assert!(validate_portable_events(&two_winners).is_err());
        let mut one_page = events.clone();
        one_page[3]["receipt"]["native_pages"] = json!(1);
        assert!(validate_portable_events(&one_page).is_err());
        let mut unread = events;
        unread[4]["receipt"]["native_body_matched"] = json!(false);
        assert!(validate_portable_events(&unread).is_err());
    }

    #[test]
    fn portable_readiness_names_both_providers_without_contract_fields() {
        let ready = |body: Value| json!({"status":200,"body":body.to_string()});
        assert!(
            validate_portable_ready(&ready(json!({"record_ready":true,
            "providers":["dynamodb-portable","celld-portable"]})))
            .is_ok()
        );
        for body in [
            json!({"record_ready":true,"providers":["dynamodb-portable"]}),
            json!({"record_ready":true,"providers":["dynamodb-portable","celld-portable"],"executed_suites":1}),
            json!({"record_ready":false,"providers":["dynamodb-portable","celld-portable"]}),
        ] {
            assert!(validate_portable_ready(&ready(body)).is_err());
        }
    }
}

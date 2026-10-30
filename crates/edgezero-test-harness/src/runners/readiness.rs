use super::*;
use crate::net::ReadinessOrigin;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReadinessSuite {
    ClientProgress,
    FullDiagnostic,
}

impl ReadinessSuite {
    pub(super) fn parse(name: &str) -> Option<Self> {
        match name {
            "outbound-client-progress" => Some(Self::ClientProgress),
            "outbound-readiness" => Some(Self::FullDiagnostic),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::ClientProgress => "outbound-client-progress",
            Self::FullDiagnostic => "outbound-readiness",
        }
    }
}

fn client_outcome(validation: Result<()>, mut failures: Vec<String>) -> Result<()> {
    if let Err(error) = validation {
        failures.push(error.to_string());
    }
    require(failures.is_empty(), failures.join("\n"))
}

fn timer_control(port: u16, source: &str, budget_ms: u64, revision: &str) -> Result<Value> {
    let token = token();
    let result = if source == "native-sleep" {
        let start = Instant::now();
        thread::sleep(Duration::from_millis(budget_ms));
        let elapsed_us = start.elapsed().as_micros() as u64;
        json!({"schema":1,"event":"readiness-timer-control","source":source,
            "budget_ms":budget_ms,"token":token,"revision":revision,"qualification":false,
            "elapsed_us":elapsed_us,"overrun_us":elapsed_us.saturating_sub(budget_ms * 1000),
            "within_five_ms":elapsed_us <= budget_ms * 1000 + 5000,
            "measurement_end":"primitive-return"})
    } else {
        guest(&net::request(
            port,
            &format!("/timer-control/{source}/{budget_ms}"),
            &token,
            None,
            None,
        )?)?
    };
    require(
        result["token"] == token
            && result["source"] == source
            && result["budget_ms"] == budget_ms
            && result["revision"] == revision,
        "foreign timer-control result",
    )?;
    Ok(result)
}

fn timer_controls(port: u16, revision: &str, records: &mut Records) -> Result<Vec<Value>> {
    for source in ["native-sleep", "empty-select", "wasi-sleep"] {
        let mut warmup = timer_control(port, source, 1, revision)?;
        warmup["event"] = json!("readiness-timer-warmup");
        records.push(warmup)?;
    }
    let mut controls = Vec::new();
    for leg in timer_legs() {
        let mut value = timer_control(port, leg.source, leg.budget_ms, revision)?;
        value["repetition"] = json!(leg.repetition);
        records.push(value.clone())?;
        controls.push(value);
    }
    records.push(json!({"event":"readiness-timer-inventory", "samples":controls.len(),
        "late_samples":controls.iter().filter(|c| c["within_five_ms"] == false).count(),
        "status":"unverified", "qualification":false,
        "reason":"timer controls characterize scheduling; no deterministic bound or G2 acceptance is inferred"}))?;
    Ok(controls)
}

fn client_probes(
    port: u16,
    revision: &str,
    records: &mut Records,
) -> Result<(Vec<Value>, Vec<String>)> {
    let mut probes = Vec::new();
    let mut failures = Vec::new();
    for leg in readiness_client_legs() {
        let token = token();
        let result = (|| -> Result<Value> {
            let origin = ReadinessOrigin::start(leg.case, &token)?;
            let reply = net::request(
                port,
                &format!(
                    "/{}/client/{}/{}",
                    leg.case, leg.consumption, leg.read_limit
                ),
                &token,
                Some(&origin.url(0)),
                None,
            );
            let snapshot = origin.finish();
            records.push(
                json!({"event":"readiness-client-origin", "leg":leg.key(), "origin":snapshot}),
            )?;
            let mut probe = guest(&reply?)?;
            require(probe.is_object(), "actual-client reply is not an object")?;
            probe["repetition"] = json!(leg.repetition);
            probe["origin"] = snapshot;
            records.push(probe.clone())?;
            Ok(probe)
        })();
        let validation = match result {
            Ok(probe) => {
                let validation = require(
                    probe["token"] == token
                        && probe["case"] == leg.case
                        && probe["consumption"] == leg.consumption,
                    "foreign actual-client reply",
                )
                .and_then(|()| validate_outbound_readiness_client_probe(&probe, revision));
                probes.push(probe);
                validation
            }
            Err(error) => Err(error),
        };
        if let Err(error) = validation {
            failures.push(format!("{}: {error}", leg.key()));
            records.push(
                json!({"event":"fastly-client-progress-failure", "status":"fail",
                "leg":leg.key(), "token":token, "case":leg.case, "consumption":leg.consumption,
                "repetition":leg.repetition, "error":error.to_string(), "qualification":false}),
            )?;
        }
    }
    Ok((probes, failures))
}

fn p5_probes(port: u16, revision: &str, records: &mut Records) -> Result<()> {
    let cases = p5_cases();
    let expected = cases.len();
    let mut probes = Vec::new();
    let mut failures = Vec::new();
    for case in cases {
        let token = token();
        let result = (|| -> Result<Value> {
            let a = Backend::start()?;
            let b = Backend::start()?;
            let shape = match case.as_str() {
                "two-origin" | "optimizer-next" => "redirect",
                "pending-head" => "pending",
                "optimizer-send" | "optimizer-initial" => "image",
                _ => &case,
            };
            let initial = a.url(&format!("/p5/{token}/{}/{shape}", b.port));
            let reply = net::request(port, &format!("/p5/{case}"), &token, Some(&initial), None);
            let origin = json!({"a_port":a.port,"b_port":b.port,
                "a":a.receipts.lock().unwrap().clone(),"b":b.receipts.lock().unwrap().clone(),
                "pending_released_before_reply":*a.release.0.lock().unwrap(),
                "pending_heads":a.progress.lock().unwrap().iter().filter(|event|
                    event["event"] == "p5-pending-head" && event["token"] == token).count()});
            // Never release a pending origin body until after the guest's final response.
            net::release(&a.release);
            let mut probe = guest(&reply?)?;
            require(
                probe["token"] == token && probe["case"] == case && probe["a_uri"] == initial,
                "foreign P5 guest response",
            )?;
            probe["origin"] = origin;
            records.push(probe.clone())?;
            Ok(probe)
        })();
        let validation = match result {
            Ok(probe) => {
                let validation = validate_outbound_readiness_client_probe(&probe, revision);
                probes.push(probe);
                validation
            }
            Err(error) => Err(error),
        };
        if let Err(error) = validation {
            failures.push(format!("{case}: {error}"));
            records.push(
                json!({"event":"fastly-p5-failure","status":"fail","case":case,
                "token":token,"error":error.to_string(),"qualification":false}),
            )?;
        }
    }
    let validation = client_outcome(
        validate_outbound_readiness_client(&probes, revision),
        failures,
    );
    records.push(json!({"event":"fastly-p5-inventory","status":if validation.is_ok(){"pass"}else{"fail"},
        "probes":probes.len(),"expected_probes":expected,
        "origin_requests":probes.iter().map(|probe| ["a","b"].into_iter().map(|origin|
            probe["origin"][origin].as_array().map_or(0, Vec::len)).sum::<usize>()).sum::<usize>(),
        "error":validation.as_ref().err().map(ToString::to_string),"qualification":false,
        "coding_evidence":"identity/gzip/unsupported-token passthrough with ResponseDecoding::Decode",
        "optimizer_evidence":"compositional native unavailable boundaries; no optimized-first-hop redirect, transformation or hosted performance proof"}))?;
    validation
}

pub(super) fn run(
    exe: &Path,
    run: &Path,
    records: &mut Records,
    suite: ReadinessSuite,
) -> Result<()> {
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(fixture_root())
        .output()?;
    require(revision.status.success(), "cannot record source revision")?;
    let revision = String::from_utf8(revision.stdout)?.trim().to_owned();
    checked(
        cargo_build()?
            .args([
                "build",
                "--offline",
                "--locked",
                "--release",
                "-p",
                "fixture-fastly",
                "--features",
                "readiness",
                "--bin",
                "fixture-fastly-outbound-readiness",
                "--target",
                "wasm32-wasip1",
            ])
            .env("EDGEZERO_QUALIFICATION_REVISION", &revision),
    )?;
    let binary = stage(
        &fixture_root().join("target/wasm32-wasip1/release/fixture-fastly-outbound-readiness.wasm"),
        run,
    )?;
    records.push(json!({"event":"readiness-build", "revision":revision,
        "source_state":"working-tree-characterization", "sdk":"0.13.1", "artifact":artifact(&binary)?}))?;
    let config = run.join("fastly.toml");
    fs::copy(
        fixture_root().join("crates/fixture-fastly/fastly.toml"),
        &config,
    )?;
    let log = run.join("readiness.log");
    let outcome = (|| -> Result<()> {
        let port = net::port()?;
        let _process = Process::start(
            &mut fastly_command(exe, &config, run, "outbound-readiness", port)?,
            &log,
            port,
        )?;
        let controls = match suite {
            ReadinessSuite::ClientProgress => Vec::new(),
            ReadinessSuite::FullDiagnostic => timer_controls(port, &revision, records)?,
        };
        let (clients, client_failures) = client_probes(port, &revision, records)?;
        let client_validation = client_outcome(
            validate_outbound_readiness_client(&clients, &revision),
            client_failures,
        );
        records.push(json!({"event":"fastly-client-progress-inventory",
            "status":if client_validation.is_ok() {"pass"} else {"fail"},
            "probes":clients.len(), "expected_probes":readiness_client_legs().len(),
            "origin_requests":clients.iter().map(|p| p["origin"]["accepted"].as_u64().unwrap_or(0)).sum::<u64>(),
            "error":client_validation.as_ref().err().map(ToString::to_string), "qualification":false}))?;
        if suite == ReadinessSuite::ClientProgress {
            let p5_validation = p5_probes(port, &revision, records);
            client_validation?;
            p5_validation?;
            return records.push(json!({"status":"unverified", "suite":suite.name(),
                "reason":"actual-client receive cases passed; SDK timing diagnostics, callback-wide progress, resource and hosted qualification remain open",
                "qualification":false}));
        }
        let mut probes = Vec::new();
        for leg in readiness_legs()
            .into_iter()
            .chain(readiness_progress_legs())
        {
            let token = token();
            let origin = ReadinessOrigin::start(leg.case, &token)?;
            let reply = net::request(
                port,
                &format!(
                    "/{}/{}/{}/{}",
                    leg.case, leg.mode, leg.consumption, leg.read_limit
                ),
                &token,
                Some(&origin.url(0)),
                None,
            );
            // Always stop, wake and join origin workers, including failed client reads.
            let snapshot = origin.finish();
            records
                .push(json!({"event":"readiness-origin", "leg":leg.key(), "origin":snapshot}))?;
            let reply = reply.map_err(|error| format!("readiness {}: {error}", leg.key()))?;
            let mut probe = guest(&reply)
                .map_err(|error| format!("readiness {}: {error}; reply={reply}", leg.key()))?;
            probe["repetition"] = json!(leg.repetition);
            probe["origin"] = snapshot;
            records.push(probe.clone())?;
            probes.push(probe);
        }
        for repetition in 0..3 {
            let control = probes
                .iter()
                .find(|probe| probe["repetition"] == repetition && probe["mode"] == "direct")
                .ok_or("missing direct control")?;
            let selected = probes
                .iter()
                .find(|probe| {
                    probe["repetition"] == repetition
                        && probe["case"] == "truncated"
                        && probe["mode"] == "selected"
                        && probe["consumption"] == "buffered"
                        && probe["read_limit"] == 1
                })
                .ok_or("missing selected control")?;
            let direct = &control["slots"][0]["error"];
            let selected = &selected["slots"][0]["error"];
            records.push(json!({"event":"readiness-error-preservation", "repetition":repetition,
                "direct":direct, "selected":selected,
                "status":if direct == "UnexpectedEof" && direct == selected {"pass"} else {"fail"}, "qualification":false}))?;
        }
        let (baseline, progress) = probes.split_at(readiness_legs().len());
        let failures: Vec<_> = [
            ("actual outbound client", client_validation),
            (
                "timer controls",
                validate_timer_controls(&controls, &revision),
            ),
            (
                "readiness",
                validate_outbound_readiness(baseline, &revision),
            ),
            (
                "mixed/full-buffer progress",
                validate_outbound_readiness_progress(progress, &revision),
            ),
        ]
        .into_iter()
        .filter_map(|(scope, result)| result.err().map(|error| format!("{scope}: {error}")))
        .collect();
        require(failures.is_empty(), failures.join("\n"))?;
        records.push(json!({"event":"readiness-inventory", "status":"pass", "probes":probes.len(),
            "origin_requests":probes.iter().map(|probe| probe["origin"]["accepted"].as_u64().unwrap_or(0)).sum::<u64>(),
            "baseline_probes":baseline.len(), "progress_probes":progress.len(), "qualification":false}))?;
        records.push(json!({"status":"unverified", "reason":"local SDK and client cases passed; full dispatch-mode coverage, callback ownership, resource and hosted qualification remain open", "qualification":false}))
    })();
    settle(
        records,
        &log,
        json!({"adapter":"fastly", "suite":suite.name()}),
        json!({"case":"readiness-inventory"}),
        outcome,
    )
}

#[cfg(test)]
mod tests {
    use super::{ReadinessSuite, client_outcome};

    #[test]
    fn functional_and_sdk_diagnostic_suites_have_distinct_explicit_names() {
        for suite in [
            ReadinessSuite::ClientProgress,
            ReadinessSuite::FullDiagnostic,
        ] {
            assert_eq!(ReadinessSuite::parse(suite.name()), Some(suite));
        }
        assert_ne!(
            ReadinessSuite::ClientProgress.name(),
            ReadinessSuite::FullDiagnostic.name()
        );
        assert_eq!(ReadinessSuite::parse("outbound-progress"), None);
        assert_eq!(ReadinessSuite::parse("smoke"), None);
    }

    #[test]
    fn client_outcome_retains_inventory_and_transport_failures() {
        let error = client_outcome(
            Err("missing client inventory".into()),
            vec![
                "first request failed".into(),
                "second reply malformed".into(),
            ],
        )
        .unwrap_err()
        .to_string();
        for diagnostic in [
            "missing client inventory",
            "first request failed",
            "second reply malformed",
        ] {
            assert!(error.contains(diagnostic));
        }
        assert!(client_outcome(Ok(()), vec!["request failed".into()]).is_err());
        assert!(client_outcome(Err("invalid inventory".into()), Vec::new()).is_err());
        assert!(client_outcome(Ok(()), Vec::new()).is_ok());
    }
}

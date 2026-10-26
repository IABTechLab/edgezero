use super::*;
use crate::net::ReadinessOrigin;

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

pub(super) fn run(exe: &Path, run: &Path, records: &mut Records) -> Result<()> {
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root())
        .output()?;
    require(revision.status.success(), "cannot record source revision")?;
    let revision = String::from_utf8(revision.stdout)?.trim().to_owned();
    checked(
        cargo_build()
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
        &root().join("target/wasm32-wasip1/release/fixture-fastly-outbound-readiness.wasm"),
        run,
    )?;
    records.push(json!({"event":"readiness-build", "revision":revision,
        "source_state":"working-tree-characterization", "sdk":"0.13.1", "artifact":artifact(&binary)?}))?;
    let config = run.join("fastly.toml");
    fs::copy(root().join("crates/fixture-fastly/fastly.toml"), &config)?;
    let log = run.join("readiness.log");
    let outcome = (|| -> Result<()> {
        let port = net::port()?;
        let _process = Process::start(
            &mut fastly_command(exe, &config, run, "outbound-readiness", port),
            &log,
            port,
        )?;
        let controls = timer_controls(port, &revision, records)?;
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
        records.push(json!({"status":"unverified", "reason":"local SDK primitive cases passed; production buffered/streamed paths, backend policy and hosted qualification remain open", "qualification":false}))
    })();
    settle(
        records,
        &log,
        json!({"adapter":"fastly", "suite":"outbound-readiness"}),
        json!({"case":"readiness-inventory"}),
        outcome,
    )
}

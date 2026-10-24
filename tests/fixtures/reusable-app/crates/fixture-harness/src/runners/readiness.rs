use super::*;
use crate::net::ReadinessOrigin;

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
        let mut probes = Vec::new();
        for leg in readiness_legs() {
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
        validate_outbound_readiness(&probes, &revision)?;
        records.push(json!({"event":"readiness-inventory", "status":"pass", "probes":probes.len(),
            "origin_requests":probes.iter().map(|probe| probe["origin"]["accepted"].as_u64().unwrap_or(0)).sum::<u64>(),
            "qualification":false}))?;
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

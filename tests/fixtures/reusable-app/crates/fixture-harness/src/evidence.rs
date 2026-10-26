use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};

mod readiness;
pub use readiness::{
    readiness_client_legs, readiness_legs, readiness_progress_legs, validate_outbound_readiness,
    validate_outbound_readiness_client, validate_outbound_readiness_client_probe,
    validate_outbound_readiness_progress,
};
mod timers;
pub use timers::{timer_legs, validate_timer_controls};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub fn require(condition: bool, message: impl Into<String>) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into().into())
    }
}
pub fn validate_outbound_progress(probe: &Value, receipts: &[Value]) -> Result<()> {
    require(
        probe["pause_ns"]
            .as_u64()
            .is_some_and(|ns| ns >= 100_000_000),
        "unrelated pause shorter than 100ms or unmeasured",
    )?;
    require(
        matches!(
            probe["adapter"].as_str(),
            Some("axum" | "cloudflare" | "fastly" | "spin")
        ),
        "unknown or missing progress adapter",
    )?;
    require(
        probe["protocol"] == "http1",
        "unsupported or unobserved protocol",
    )?;
    require(
        probe["consumer_polls_before_window_end"] == 0,
        "consumer polled during window",
    )?;
    let case = probe["case"].as_str().ok_or("missing progress case")?;
    let slots = match case {
        "eager-single" | "lazy-single" => 1,
        "eager-batch" | "response-owned" | "baseline-batch" => 2,
        _ => return Err("unknown progress case".into()),
    };
    require(probe["slots"] == slots, "wrong slot count")?;
    let token = probe["token"].as_str().ok_or("missing probe token")?;
    require(
        !token.is_empty()
            && token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "invalid probe token",
    )?;
    let prefix = format!("/g3/{token}/");
    require(
        receipts.len() == slots + 2,
        "missing or extra origin receipts",
    )?;
    let one = |suffix: &str| -> Result<u64> {
        let path = format!("{prefix}{suffix}");
        let matching = receipts
            .iter()
            .filter(|r| r["path"] == path)
            .collect::<Vec<_>>();
        require(
            matching.len() == 1,
            "missing, duplicate or foreign origin receipt",
        )?;
        matching[0]["received_ns"]
            .as_u64()
            .ok_or_else(|| "missing origin timestamp".into())
    };
    let start = one("start")?;
    let end = one("end")?;
    require(
        end.saturating_sub(start) >= 100_000_000,
        "unrelated wait shorter than 100ms",
    )?;
    let eager = case != "lazy-single" && (case != "baseline-batch" || probe["adapter"] == "fastly");
    for slot in 0..slots {
        let received = one(&format!("slot-{slot}"))?;
        require(
            if eager {
                received >= start && received - start < 100_000_000
            } else {
                received >= end
            },
            "origin receipt did not match eager/lazy dispatch window",
        )?;
    }
    Ok(())
}
pub fn validate_shared_state(guest: &Value, retained: bool) -> Result<()> {
    let expected = if retained {
        guest["ordinal"].as_u64()
    } else {
        Some(1)
    };
    require(
        expected.is_some() && guest["shared"].as_u64() == expected,
        format!("shared app state does not match its lifetime: {guest}"),
    )
}
pub fn validate_logging(guests: &[Value], delivered: &[String]) -> Result<&'static str> {
    let mut seen = HashMap::new();
    let mut reused = false;
    for guest in guests {
        if let Some(previous) = seen.insert(guest["instance"].to_string(), guest) {
            require(
                guest["ordinal"].as_u64() > previous["ordinal"].as_u64(),
                "logging ordinal did not increase",
            )?;
            require(
                guest["correlation"] != previous["correlation"],
                "logging correlation repeated",
            )?;
            reused = true;
        }
    }
    if !reused {
        return Ok("unverified");
    }
    require(
        guests.iter().all(|g| {
            delivered
                .iter()
                .any(|d| g["correlation"].as_str() == Some(d))
        }),
        "missing correlated endpoint receipt",
    )?;
    Ok("pass")
}
pub fn validate_negative_logging(replies: &[Value], starts: &[Value]) -> Result<&'static str> {
    let guests: HashMap<_, _> = starts
        .iter()
        .map(|v| (v["token"].to_string(), v["instance"].to_string()))
        .collect();
    let mut seen = HashSet::new();
    let mut reused = false;
    for reply in replies {
        let guest = guests
            .get(&reply["token"].to_string())
            .ok_or("missing negative logger request_start")?;
        let repeated = !seen.insert(guest);
        require(
            reply["status"] == if repeated { 500 } else { 200 },
            "unexpected negative logger status",
        )?;
        if repeated {
            require(
                reply["body"]
                    .as_str()
                    .is_some_and(|body| body.contains("already initialized")),
                "missing repeated logger initialization error",
            )?;
        }
        reused |= repeated;
    }
    Ok(if reused { "pass" } else { "unverified" })
}
pub fn validate_fastly_delivery_report(
    token: &str,
    events: &[Value],
    expected: &Value,
) -> Result<()> {
    let reports = events
        .iter()
        .filter(|event| event["event"] == "delivery_report" && event["token"] == token)
        .collect::<Vec<_>>();
    require(reports.len() == 1, "missing or duplicate delivery report")?;
    let report = reports[0];
    for key in [
        "disposition",
        "commitment",
        "body_kind",
        "outcome",
        "fallback",
        "may_send_native",
    ] {
        require(
            expected.get(key).is_some(),
            "incomplete delivery expectation",
        )?;
        require(
            report.get(key) == expected.get(key),
            format!("delivery report {key} mismatch"),
        )?;
    }
    if expected["disposition"] == "no-attempt" {
        require(
            expected.get("no_attempt_reason").is_some()
                && report.get("no_attempt_reason") == expected.get("no_attempt_reason"),
            "delivery report no-attempt reason mismatch",
        )?;
    }
    let attempted = matches!(
        expected["disposition"].as_str(),
        Some("routed" | "detached")
    );
    let terminals = events
        .iter()
        .filter(|event| event["event"] == "egress_terminal" && event["token"] == token)
        .collect::<Vec<_>>();
    require(
        terminals.len() == usize::from(attempted),
        "delivery completion count mismatch",
    )?;
    if attempted {
        for key in ["outcome", "body_kind", "fallback", "bytes_written"] {
            require(
                report.get(key).is_some() && report.get(key) == terminals[0].get(key),
                format!("delivery/completion {key} mismatch"),
            )?;
        }
    }
    Ok(())
}

pub fn validate_post_send(token: &str, receipts: &[String], events: &[Value]) -> Result<()> {
    require(
        receipts.contains(&format!("/post-send?token={token}")),
        "missing post-send backend receipt",
    )?;
    require(
        events
            .iter()
            .any(|e| e["event"] == "guest_completed" && e["token"] == token),
        "missing guest completion",
    )
}
pub fn validate_limit_summaries(events: &[Value]) -> Result<()> {
    let mut attempts = HashMap::new();
    for event in events.iter().filter(|e| e["event"] == "request_start") {
        *attempts
            .entry(event["instance"].to_string())
            .or_insert(0u64) += 1;
    }
    let summaries: Vec<_> = events
        .iter()
        .filter(|e| e["event"] == "sdk_summary")
        .collect();
    require(!attempts.is_empty(), "missing attempts")?;
    require(
        summaries.len() == attempts.len(),
        "missing or duplicate limit summaries",
    )?;
    let mut seen = HashSet::new();
    for summary in summaries {
        let instance = summary["instance"].to_string();
        require(seen.insert(instance.clone()), "duplicate summary instance")?;
        require(
            attempts.get(&instance) == Some(&1) && summary["attempted"] == 1,
            "limit summary attempts do not match",
        )?;
    }
    Ok(())
}
pub fn validate_overlap(left: &Value, right: &Value) -> Result<()> {
    require(
        left["instance"] == right["instance"],
        "different guests do not prove overlap",
    )?;
    require(
        left["before"]["inflight"]
            .as_u64()
            .unwrap_or(0)
            .max(right["before"]["inflight"].as_u64().unwrap_or(0))
            >= 2,
        "this pair did not observe overlapping callbacks",
    )
}
pub fn cookie_values(headers: &Value) -> Vec<&str> {
    headers
        .as_array()
        .into_iter()
        .flatten()
        .filter(|h| {
            h[0].as_str()
                .is_some_and(|s| s.eq_ignore_ascii_case("set-cookie"))
        })
        .filter_map(|h| h[1].as_str())
        .collect()
}
pub fn attempt_counts(events: &[Value]) -> (usize, usize) {
    (
        events
            .iter()
            .filter(|e| e["event"] == "request_start")
            .count(),
        events
            .iter()
            .filter(|e| e["event"] == "client_completed")
            .count(),
    )
}
fn quantiles(mut values: Vec<u64>) -> Value {
    values.sort_unstable();
    let mut result = json!({});
    for p in [50usize, 95, 99] {
        result[format!("p{p}")] = json!(values[(p * values.len()).div_ceil(100).saturating_sub(1)]);
    }
    result
}
/// Completion and first-byte quantiles for one cohort, or `None` without
/// completion samples.
fn cohort_stats(rows: &[&Value]) -> Option<Value> {
    let samples = |key: &str| {
        rows.iter()
            .filter_map(|r| r[key].as_u64())
            .collect::<Vec<_>>()
    };
    let completion = samples("complete_ns");
    if completion.is_empty() {
        return None;
    }
    let mut stats = json!({"samples":completion.len(), "completion_ns":quantiles(completion)});
    let first_bytes = samples("first_byte_ns");
    if !first_bytes.is_empty() {
        stats["first_byte_ns"] = quantiles(first_bytes);
    }
    Some(stats)
}
fn phase_delta(before: Option<f64>, after: Option<f64>) -> Value {
    match (before, after) {
        (Some(a), Some(b)) if a >= 0.0 && b >= a => {
            json!({"status":"observed", "value":b-a})
        }
        (Some(_), Some(_)) => json!({"status":"invalid", "value":null}),
        _ => json!({"status":"unknown", "value":null}),
    }
}
fn variant(record: &Value) -> String {
    record["variant"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| {
            format!(
                "{}:{}",
                record["adapter"].as_str().unwrap_or("unknown"),
                record["mode"].as_str().unwrap_or("unknown")
            )
        })
}
fn memory_analysis(events: &[&Value], phase: &str) -> Value {
    let snapshots = events
        .iter()
        .flat_map(|e| {
            ["heap_mib", "heap_before_mib", "heap_after_mib"]
                .into_iter()
                .filter_map(|key| e[key].as_f64().filter(|v| *v >= 0.0))
        })
        .collect::<Vec<_>>();
    let mut cleanup = BTreeMap::new();
    let mut invalid_cleanup = false;
    for event in events.iter().filter(|e| e["event"] == phase) {
        match (event["ordinal"].as_u64(), event["heap_mib"].as_f64()) {
            (Some(ordinal), Some(heap)) if heap >= 0.0 => {
                invalid_cleanup |= cleanup.insert(ordinal, heap).is_some();
            }
            _ => invalid_cleanup = true,
        }
    }
    let points = cleanup.into_iter().collect::<Vec<_>>();
    let slope = if points.len() >= 2 && !invalid_cleanup {
        let n = points.len() as f64;
        let x = points.iter().map(|(x, _)| *x as f64).sum::<f64>() / n;
        let y = points.iter().map(|(_, y)| y).sum::<f64>() / n;
        Some(
            points
                .iter()
                .map(|(a, b)| (*a as f64 - x) * (b - y))
                .sum::<f64>()
                / points
                    .iter()
                    .map(|(a, _)| (*a as f64 - x).powi(2))
                    .sum::<f64>(),
        )
    } else {
        None
    };
    let change = if points.len() >= 2 && !invalid_cleanup {
        Some(points.last().unwrap().1 - points[0].1)
    } else {
        None
    };
    let mut plateau = json!({"status":"unknown", "window_samples":3});
    if points.len() >= 3 && !invalid_cleanup {
        let tail = &points[points.len() - 3..];
        plateau = json!({"status":if tail.iter().all(|(_, heap)| *heap == tail[0].1) {
            "observed" } else { "not_observed" }, "window_samples":3,
            "first_ordinal":tail[0].0, "last_ordinal":tail[2].0});
    }
    json!({"sample_phase":phase,"source":"SDK host-inclusive rounded MiB snapshots",
        "scope":"observed request span only; rounded samples cannot establish absence of leaks; not guest linear-memory high-water or host RSS",
        "snapshot_samples":snapshots.len(),
        "peak_observed_mib":snapshots.into_iter().reduce(f64::max),
        "samples":points.len(), "samples_invalid_or_missing":invalid_cleanup,
        "samples_by_ordinal":points.iter().map(|(ordinal,heap)| json!({"ordinal":ordinal,"heap_mib":heap})).collect::<Vec<_>>(),
        "slope_mib_per_ordinal":slope, "slope_method":"least squares over observed phase ordinals",
        "change_mib":change, "plateau":plateau})
}
fn guest_analysis(records: &[Value]) -> Vec<Value> {
    let mut guests: BTreeMap<(String, String, String), Vec<&Value>> = BTreeMap::new();
    for event in records {
        if !event["instance"].is_null() {
            guests
                .entry((
                    variant(event),
                    event["repetition"].to_string(),
                    event["instance"].to_string(),
                ))
                .or_default()
                .push(event);
        }
    }
    guests.into_iter().map(|((variant, _, _), events)| {
        let count = |name: &str| events.iter().filter(|e| e["event"] == name).count();
        let summaries = events.iter().filter(|e| e["event"] == "sdk_summary").collect::<Vec<_>>();
        let attempted = if summaries.len() == 1 { summaries[0]["attempted"].as_u64() } else { None };
        let initializations = events.iter().filter(|e| e["event"] == "initialization")
            .map(|e| json!({"ordinal":e["ordinal"],"token":e["token"],"wall_ns":e["wall_ns"],
                "cpu_ms":phase_delta(e["cpu_before_ms"].as_f64(),e["cpu_after_ms"].as_f64()),
                "heap_before_mib":e["heap_before_mib"],"heap_after_mib":e["heap_after_mib"]}))
            .collect::<Vec<_>>();
        let mut requests: BTreeMap<(u64,String),Vec<&Value>> = BTreeMap::new();
        for event in &events {
            if let (Some(ordinal), Some(token)) = (event["ordinal"].as_u64(),event["token"].as_str()) {
                requests.entry((ordinal,token.to_owned())).or_default().push(event);
            }
        }
        let requests = requests.into_iter().map(|((ordinal,token),rows)| {
            let phase = |name: &str| {
                let matches = rows.iter().filter(|e| e["event"] == name).collect::<Vec<_>>();
                if matches.len() == 1 { matches[0]["cpu_ms"].as_f64() } else { None }
            };
            json!({"ordinal":ordinal,"token":token,
                "cpu_to_conversion_ms":phase_delta(phase("request_start"),phase("conversion_completed")),
                "cpu_to_commit_ms":phase_delta(phase("request_start"),phase("response_committed")),
                "cpu_after_commit_ms":phase_delta(phase("response_committed"),phase("guest_completed")),
                "cpu_request_ms":phase_delta(phase("request_start"),phase("guest_completed")),
                "response_committed":if rows.iter().any(|e| e["event"] == "response_committed") {json!(true)} else {Value::Null},
                "guest_completed":if rows.iter().any(|e| e["event"] == "guest_completed") {json!(true)} else {Value::Null},
                "terminal_error":rows.iter().any(|e| e["event"] == "terminal_error")})
        }).collect::<Vec<_>>();
        let client_completions = records.iter().filter(|r| r["event"] == "client_completed"
            && crate::evidence::variant(r) == variant && r["repetition"] == events[0]["repetition"]
            && (r["instance"] == events[0]["instance"] || r["guest"]["instance"] == events[0]["instance"]
                || requests.iter().any(|request| r["token"] == request["token"]))).count();
        json!({"variant":variant,"repetition":events[0]["repetition"],"instance":events[0]["instance"],
            "request_starts":count("request_start"),"response_commitments":count("response_committed"),
            "guest_completions":count("guest_completed"),"terminal_errors":count("terminal_error"),
            "client_completions":client_completions,"sdk_summary_records":summaries.len(),"sdk_attempted":attempted,
            "attempt_reconciliation":match attempted {Some(n) if n == count("request_start") as u64 => "match", Some(_) => "mismatch", None => "unknown"},
            "initialization_count":initializations.len(),"initialization":initializations,"requests":requests,
            "cpu_scope":"SDK samples between named observation points: standard request_start is after initialization; custom request_start is before initialization. Concurrent work may contribute. Not total request CPU or cross-run performance evidence.",
            "memory":memory_analysis(&events,"guest_completed"),
            "conversion_memory":memory_analysis(&events,"conversion_completed")})
    }).collect()
}
pub fn summarize(records: &[Value]) -> Value {
    let mut builds = BTreeMap::new();
    let mut mixed_builds = false;
    for record in records.iter().filter(|r| r["event"] == "build") {
        let key = variant(record);
        if let Some(previous) = builds.insert(key, record["artifact"]["fnv1a64"].clone()) {
            mixed_builds |= previous != record["artifact"]["fnv1a64"];
        }
    }
    let mut groups: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    let mut memory: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for r in records {
        if r["event"] == "client_completed"
            && r["workload"] == "probe"
            && r["status"] == 200
            && r.get("guest").is_some()
        {
            let cohort = if r["guest"]["ordinal"] == 1 {
                "cold"
            } else {
                "reused"
            };
            groups
                .entry(format!("{}:probe:{cohort}", variant(r)))
                .or_default()
                .push(r);
        }
        if let Some(heap) = r["heap_mib"].as_u64() {
            memory
                .entry(r["variant"].as_str().unwrap_or("unknown").to_owned())
                .or_default()
                .push(heap);
        }
    }
    let mut result = json!({"quantile_method":"nearest rank", "variants":{}, "memory_snapshots":{}, "cpu_comparison":"SDK phase readings only; cross-run performance unverified"});
    for (key, rows) in groups {
        let Some(mut stats) = cohort_stats(&rows) else {
            continue;
        };
        let mut repetitions: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
        for row in &rows {
            repetitions
                .entry(row["repetition"].to_string())
                .or_default()
                .push(row);
        }
        for (repetition, samples) in repetitions {
            if let Some(repetition_stats) = cohort_stats(&samples) {
                stats["repetitions"][&repetition] = repetition_stats;
            }
        }
        result["variants"][&key] = stats;
    }
    for (key, values) in memory {
        result["memory_snapshots"][key] = json!({"samples":values.len(),"min_mib":values.iter().min(),"max_mib":values.iter().max(),"source":"SDK host-inclusive rounded snapshot"});
    }
    result["build_identity"] = json!({"status":if mixed_builds {"invalid"} else if builds.is_empty() {"unknown"} else {"consistent"}});
    let (attempts, completions) = attempt_counts(records);
    result["guests"] = json!(guest_analysis(records));
    result["recorded_request_starts"] = json!(attempts);
    result["recorded_client_completions"] = json!(completions);
    result
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_evidence_assembly_retains_case_counts_and_hashes() -> Result<()> {
        let directory = std::env::temp_dir().join(format!("outbound-record-{}", crate::token()));
        std::fs::create_dir(&directory)?;
        let run = || -> Result<()> {
            let repository = crate::root().join("../../..").canonicalize()?;
            let script = repository.join("scripts/check_outbound_cutover_evidence.mjs");
            let inventory: Value = serde_json::from_slice(&std::fs::read(
                repository.join("tests/fixtures/reusable-app/outbound-cutover-cases.json"),
            )?)?;
            let candidate = "a".repeat(40);
            let build = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
            let raw_hash = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";
            std::fs::write(directory.join("app.bin"), b"abc")?;
            let mut results = Vec::new();
            for (index, case) in inventory["cases"]
                .as_array()
                .ok_or("missing inventory cases")?
                .iter()
                .take(2)
                .enumerate()
            {
                let path =
                    directory.join(case["artifact_path"].as_str().ok_or("missing artifact")?);
                std::fs::create_dir_all(path.parent().ok_or("missing artifact parent")?)?;
                std::fs::write(path, b"{}")?;
                results.push(json!({
                    "case_id":case["case_id"], "environment":case["environment"],
                    "candidate_sha":candidate, "build_id":build,
                    "source_digest":case["source_digest"], "command":case["command"],
                    "sentinel":case["sentinel"], "artifact_path":case["artifact_path"],
                    "status":if index == 0 {"failed"} else {"pending"},
                    "samples":if index == 0 {3} else {0},
                    "assertions":[{"name":"synthetic negative control", "passed":false}],
                    "measurements":[]
                }));
            }
            let input = json!({"schema":1, "candidate_sha":candidate,
                "source_state":"working-tree", "qualification":false,
                "builds":[{"build_id":build,"candidate_sha":candidate,"artifact_path":"app.bin"}],
                "results":results});
            let input_path = directory.join("input.json");
            let output_path = directory.join("draft.json");
            std::fs::write(&input_path, serde_json::to_vec(&input)?)?;
            let invoke = || {
                std::process::Command::new("node")
                    .arg(&script)
                    .args(["--assemble", "--candidate", &candidate, "--input"])
                    .arg(&input_path)
                    .arg("--artifacts")
                    .arg(&directory)
                    .arg("--output")
                    .arg(&output_path)
                    .output()
            };
            let output = invoke()?;
            require(
                output.status.code() == Some(1) && output_path.is_file(),
                format!(
                    "draft assembly did not expose blocked qualification: {}",
                    String::from_utf8_lossy(&output.stderr)
                ),
            )?;
            let bytes = std::fs::read(&output_path)?;
            let draft: Value = serde_json::from_slice(&bytes)?;
            require(
                draft["candidate_sha"] == candidate && draft["builds"][0]["build_id"] == build,
                "assembly changed candidate or build identity",
            )?;
            require(
                draft["qualified"] == false
                    && draft["stage"] == "pre-freeze"
                    && draft["qualification"] == false
                    && draft["source_state"] == "working-tree",
                "assembly promoted development evidence",
            )?;
            require(
                draft["assembly"]
                    == json!({"observed_legs":2,"passed":0,"failed":1,"pending":1,"samples":3}),
                "assembly lost outcome or sample counts",
            )?;
            for (index, result) in draft["results"]
                .as_array()
                .ok_or("missing results")?
                .iter()
                .enumerate()
            {
                require(
                    result["artifact_sha256"] == raw_hash
                        && result["build_id"] == build
                        && result["candidate_sha"] == candidate
                        && result["case_id"] == input["results"][index]["case_id"]
                        && result["samples"] == input["results"][index]["samples"],
                    "assembly changed case counts, provenance or raw hashes",
                )?;
            }
            require(
                !invoke()?.status.success() && std::fs::read(&output_path)? == bytes,
                "assembly overwrote existing evidence",
            )?;
            let report = std::process::Command::new("node")
                .arg(&script)
                .args(["--report", "--candidate", &candidate, "--record"])
                .arg(&output_path)
                .arg("--artifacts")
                .arg(&directory)
                .output()?;
            require(
                report.status.code() == Some(1),
                "report lost blocked exit status",
            )?;
            let report: Value = serde_json::from_slice(&report.stdout)?;
            require(
                report["status"] == "blocked"
                    && report["qualified"] == false
                    && report["passed"] == 0
                    && report["findings"]
                        .as_array()
                        .is_some_and(|findings| findings.len() > 2)
                    && std::fs::read(&output_path)? == bytes,
                "report omitted independent gates or modified evidence",
            )?;
            let release = std::process::Command::new("node")
                .arg(&script)
                .args(["--release", "--candidate", &candidate, "--record"])
                .arg(&output_path)
                .arg("--artifacts")
                .arg(&directory)
                .output()?;
            require(
                !release.status.success()
                    && String::from_utf8_lossy(&release.stderr).contains("pre-freeze"),
                "pre-freeze draft passed release validation",
            )
        };
        let result = run();
        std::fs::remove_dir_all(directory)?;
        result
    }

    #[test]
    fn missing_observation_is_unknown_and_mixed_builds_are_rejected() {
        let summary = summarize(&[
            json!({"event":"request_start","variant":"c","instance":"one","ordinal":1,"token":"a"}),
            json!({"event":"build","variant":"c","repetition":0,"artifact":{"fnv1a64":"a"}}),
            json!({"event":"build","variant":"c","repetition":1,"artifact":{"fnv1a64":"b"}}),
        ]);
        assert!(summary["guests"][0]["requests"][0]["response_committed"].is_null());
        assert_eq!(summary["build_identity"]["status"], "invalid");
    }
    #[test]
    fn equal_build_fingerprints_across_repetitions_stay_consistent() {
        let summary = summarize(&[
            json!({"event":"build","variant":"c","repetition":0,"artifact":{"fnv1a64":"a"}}),
            json!({"event":"build","variant":"c","repetition":1,"artifact":{"fnv1a64":"a"}}),
            json!({"event":"build","variant":"b","repetition":0,"artifact":{"fnv1a64":"b"}}),
        ]);
        assert_eq!(summary["build_identity"]["status"], "consistent");
    }
    fn measured(event: &str, ordinal: u64, cpu: u64, heap: u64) -> Value {
        json!({"event":event,"variant":"C","repetition":1,"instance":"same",
            "ordinal":ordinal,"token":format!("t{ordinal}"),"cpu_ms":cpu,"heap_mib":heap})
    }
    #[test]
    fn per_guest_phases_attempts_and_memory_are_bounded_observations() {
        let mut events = vec![
            json!({"event":"initialization","variant":"C","repetition":1,
            "instance":"same","ordinal":1,"token":"t1","cpu_before_ms":1,"cpu_after_ms":4,
            "heap_before_mib":5,"heap_after_mib":8}),
        ];
        for ordinal in 1..=4 {
            events.push(measured("request_start", ordinal, ordinal * 10, 8));
            events.push(measured(
                "response_committed",
                ordinal,
                ordinal * 10 + 2,
                10,
            ));
            events.push(measured("guest_completed", ordinal, ordinal * 10 + 5, 9));
        }
        events.push(json!({"event":"sdk_summary","variant":"C","repetition":1,"instance":"same","attempted":5}));
        let summary = summarize(&events);
        let guest = &summary["guests"][0];
        assert_eq!(guest["request_starts"], 4);
        assert_eq!(guest["guest_completions"], 4);
        assert_eq!(guest["sdk_attempted"], 5);
        assert_eq!(guest["attempt_reconciliation"], "mismatch");
        assert_eq!(guest["initialization"][0]["cpu_ms"]["value"], 3.0);
        assert_eq!(guest["requests"][0]["cpu_to_commit_ms"]["value"], 2.0);
        assert_eq!(guest["requests"][0]["cpu_after_commit_ms"]["value"], 3.0);
        assert_eq!(guest["memory"]["slope_mib_per_ordinal"], 0.0);
        assert_eq!(guest["memory"]["peak_observed_mib"], 10.0);
        assert_eq!(guest["memory"]["change_mib"], 0.0);
        assert_eq!(guest["memory"]["plateau"]["status"], "observed");
        assert_eq!(guest["memory"]["plateau"]["first_ordinal"], 2);
        assert_eq!(guest["memory"]["plateau"]["last_ordinal"], 4);
    }
    #[test]
    fn missing_and_decreasing_cpu_are_not_zero_or_success() {
        let mut events = vec![
            measured("request_start", 1, 10, 8),
            measured("response_committed", 1, 9, 8),
        ];
        events.push(measured("terminal_error", 1, 11, 8));
        let summary = summarize(&events);
        let guest = &summary["guests"][0];
        assert!(guest["sdk_attempted"].is_null());
        assert_eq!(guest["attempt_reconciliation"], "unknown");
        assert_eq!(guest["terminal_errors"], 1);
        assert_eq!(guest["guest_completions"], 0);
        assert_eq!(
            guest["requests"][0]["cpu_to_commit_ms"]["status"],
            "invalid"
        );
        assert_eq!(
            guest["requests"][0]["cpu_after_commit_ms"]["status"],
            "unknown"
        );
        assert!(guest["requests"][0]["cpu_to_commit_ms"]["value"].is_null());
        assert_eq!(guest["memory"]["plateau"]["status"], "unknown");
    }
    #[test]
    fn guest_identity_includes_variant_and_repetition_and_slope_uses_ordinals() {
        let mut events = vec![
            measured("guest_completed", 1, 1, 10),
            measured("guest_completed", 3, 2, 14),
        ];
        let mut other = events[0].clone();
        other["repetition"] = json!(2);
        events.push(other.clone());
        other["variant"] = json!("B");
        events.push(other);
        let summary = summarize(&events);
        let guests = summary["guests"].as_array().unwrap();
        assert_eq!(guests.len(), 3);
        let guest = guests
            .iter()
            .find(|g| g["variant"] == "C" && g["repetition"] == 1)
            .unwrap();
        assert_eq!(guest["memory"]["slope_mib_per_ordinal"], 2.0);
        assert_eq!(guest["memory"]["change_mib"], 4.0);
        assert_eq!(guest["memory"]["plateau"]["status"], "unknown");
    }
    #[test]
    fn conversion_samples_do_not_claim_guest_cleanup_or_commitment() {
        let events = vec![
            measured("conversion_completed", 1, 5, 8),
            measured("conversion_completed", 2, 6, 10),
        ];
        let summary = summarize(&events);
        let guest = &summary["guests"][0];
        assert_eq!(
            guest["conversion_memory"]["sample_phase"],
            "conversion_completed"
        );
        assert_eq!(guest["conversion_memory"]["slope_mib_per_ordinal"], 2.0);
        assert_eq!(guest["memory"]["samples"], 0);
        assert_eq!(guest["response_commitments"], 0);
        assert_eq!(guest["guest_completions"], 0);
        assert_eq!(guest["requests"][0]["cpu_request_ms"]["status"], "unknown");
    }
    #[test]
    fn failed_attempt_is_not_completion() {
        assert_eq!(
            attempt_counts(&[json!({"event":"request_start"}), json!({"event":"error"})]),
            (1, 0)
        );
    }
    #[test]
    fn duplicate_cookies() {
        assert_eq!(
            cookie_values(&json!([["Set-Cookie", "a=1"], ["Set-Cookie", "b=2"]])),
            vec!["a=1", "b=2"]
        );
    }
    #[test]
    fn overlap_requires_same_guest_and_concurrency() {
        let left = json!({"instance":"a","before":{"inflight":1}});
        for right in [
            json!({"instance":"b","before":{"inflight":2}}),
            json!({"instance":"a","before":{"inflight":1}}),
        ] {
            assert!(validate_overlap(&left, &right).is_err());
        }
        validate_overlap(&left, &json!({"instance":"a","before":{"inflight":2}})).unwrap();
    }
    #[test]
    fn unmatched_workloads_excluded() {
        let s = summarize(&[
            json!({"event":"client_completed","variant":"c","workload":"probe","status":200,"guest":{"ordinal":1},"complete_ns":10}),
            json!({"event":"client_completed","variant":"c","workload":"probe","status":200,"guest":{"ordinal":2},"complete_ns":20}),
            json!({"event":"client_completed","variant":"c","assertion":"idle","complete_ns":999}),
            json!({"event":"client_completed","variant":"c","guest":{"ordinal":2},"status":200,"complete_ns":999}),
            json!({"event":"client_completed","variant":"c","workload":"probe","guest":{"ordinal":2},"status":503,"complete_ns":999}),
        ]);
        assert_eq!(s["variants"]["c:probe:reused"]["samples"], 1);
        assert_eq!(s["variants"]["c:probe:cold"]["completion_ns"]["p50"], 10);
        assert_eq!(s["variants"]["c:probe:reused"]["completion_ns"]["p50"], 20);
    }
    #[test]
    fn provider_modes_separate() {
        let records=["retained","per-request"].map(|mode|json!({"event":"client_completed","adapter":"spin","mode":mode,"workload":"probe","status":200,"guest":{"ordinal":2},"complete_ns":10}));
        assert_eq!(
            summarize(&records)["variants"].as_object().unwrap().len(),
            2
        );
    }
    #[test]
    fn probe_quantiles_keep_repetitions_separate() {
        let records = [10, 100]
            .into_iter()
            .enumerate()
            .map(|(repetition, latency)| {
                json!({"event":"client_completed","variant":"c","workload":"probe","status":200,
                "guest":{"ordinal":2},"repetition":repetition,"complete_ns":latency})
            })
            .collect::<Vec<_>>();
        let summary = summarize(&records);
        let repetitions = &summary["variants"]["c:probe:reused"]["repetitions"];
        assert_eq!(repetitions["0"]["completion_ns"]["p50"], 10);
        assert_eq!(repetitions["1"]["completion_ns"]["p50"], 100);
    }
    #[test]
    fn shared_state_matches_retained_and_per_request_lifetimes() {
        let value = json!({"ordinal":3,"shared":3});
        validate_shared_state(&value, true).unwrap();
        assert!(validate_shared_state(&value, false).is_err());
        let fresh = json!({"ordinal":3,"shared":1});
        validate_shared_state(&fresh, false).unwrap();
        assert!(validate_shared_state(&fresh, true).is_err());
        assert!(validate_shared_state(&json!({}), true).is_err());
    }
    #[test]
    fn logging_requires_reuse_and_correlated_delivery() {
        let cold = (0..3)
            .map(|i| json!({"instance":i.to_string(),"ordinal":1,"correlation":i.to_string()}))
            .collect::<Vec<_>>();
        let delivered = (0..3).map(|i| i.to_string()).collect::<Vec<_>>();
        assert_eq!(validate_logging(&cold, &delivered).unwrap(), "unverified");
        let warm = (0..3)
            .map(|i| json!({"instance":"same","ordinal":i+1,"correlation":i.to_string()}))
            .collect::<Vec<_>>();
        assert!(validate_logging(&warm, &["0".into(), "0".into(), "0".into()]).is_err());
        assert_eq!(validate_logging(&warm, &delivered).unwrap(), "pass");
    }
    #[test]
    fn negative_logger_eviction() {
        let mut starts = vec![
            json!({"instance":"one","token":"a"}),
            json!({"instance":"two","token":"b"}),
        ];
        let mut replies = vec![
            json!({"token":"a","status":200}),
            json!({"token":"b","status":200}),
        ];
        assert_eq!(
            validate_negative_logging(&replies, &starts).unwrap(),
            "unverified"
        );
        starts[1]["instance"] = json!("one");
        assert!(validate_negative_logging(&replies, &starts).is_err());
        replies[1]["status"] = json!(500);
        replies[1]["body"] = json!("unrelated KV registry failure");
        assert!(validate_negative_logging(&replies, &starts).is_err());
        replies[1]["body"] = json!("logging system was already initialized");
        assert_eq!(
            validate_negative_logging(&replies, &starts).unwrap(),
            "pass"
        );
    }
    #[test]
    fn delivery_reports_require_exactly_one_matching_completion() {
        let expected = json!({"disposition":"routed","commitment":"Committed", "body_kind":"Application",
            "outcome":"SourceError","fallback":null,"may_send_native":false});
        let mut delivery = expected.clone();
        delivery["event"] = json!("delivery_report");
        delivery["token"] = json!("x");
        delivery["bytes_written"] = json!(7);
        let terminal = json!({"event":"egress_terminal","token":"x","body_kind":"Application",
            "outcome":"SourceError","fallback":null,"bytes_written":7});
        assert!(
            validate_fastly_delivery_report("x", &[delivery.clone(), terminal.clone()], &expected)
                .is_ok()
        );
        assert!(validate_fastly_delivery_report("x", &[delivery.clone()], &expected).is_err());
        assert!(
            validate_fastly_delivery_report(
                "x",
                &[delivery.clone(), delivery.clone(), terminal.clone()],
                &expected
            )
            .is_err()
        );
        for key in [
            "commitment",
            "body_kind",
            "outcome",
            "fallback",
            "may_send_native",
        ] {
            let mut changed = delivery.clone();
            changed[key] = json!("wrong");
            assert!(
                validate_fastly_delivery_report("x", &[changed, terminal.clone()], &expected)
                    .is_err()
            );
        }
        let expected = json!({"disposition":"no-attempt","commitment":null,"body_kind":null,
            "outcome":null,"fallback":null,"may_send_native":true,"no_attempt_reason":"required-store"});
        let mut no_attempt = expected.clone();
        no_attempt["event"] = json!("delivery_report");
        no_attempt["token"] = json!("x");
        assert!(validate_fastly_delivery_report("x", &[no_attempt.clone()], &expected).is_ok());
        let mut wrong_cause = no_attempt.clone();
        wrong_cause["no_attempt_reason"] = json!("request-head-conversion");
        assert!(validate_fastly_delivery_report("x", &[wrong_cause], &expected).is_err());
        assert!(validate_fastly_delivery_report("x", &[no_attempt, terminal], &expected).is_err());
    }

    #[test]
    fn post_send_requires_both_receipts() {
        assert!(validate_post_send("x", &[], &[]).is_err());
        let receipts = vec!["/post-send?token=x".into()];
        assert!(validate_post_send("x", &receipts, &[]).is_err());
        validate_post_send(
            "x",
            &receipts,
            &[json!({"event":"guest_completed","token":"x"})],
        )
        .unwrap();
    }
    #[test]
    fn limit_summaries_match_attempts() {
        let mut events = vec![json!({"event":"request_start","instance":"one"})];
        assert!(validate_limit_summaries(&events).is_err());
        for count in [0, 2] {
            events.truncate(1);
            events.push(json!({"event":"sdk_summary","instance":"one","attempted":count}));
            assert!(validate_limit_summaries(&events).is_err());
        }
        events[1]["attempted"] = json!(1);
        validate_limit_summaries(&events).unwrap();
        events[1]["instance"] = json!("other");
        assert!(validate_limit_summaries(&events).is_err());
        events[1]["instance"] = json!("one");
        events.push(events[1].clone());
        assert!(validate_limit_summaries(&events).is_err());
        events.push(json!({"event":"request_start","instance":"two"}));
        assert!(validate_limit_summaries(&events).is_err());
    }
}

#[cfg(test)]
mod progress_tests {
    use super::*;

    #[test]
    fn outbound_progress_probe_requires_origin_receipts() {
        let probe = json!({"adapter":"axum", "case":"eager-batch", "token":"one", "protocol":"http1",
            "slots":2, "consumer_polls_before_window_end":0,"pause_ns":100_000_000});
        assert!(validate_outbound_progress(&probe, &[]).is_err());
        let events = vec![
            json!({"path":"/g3/one/start", "received_ns":0}),
            json!({"path":"/g3/one/slot-0", "received_ns":10_000_000}),
            json!({"path":"/g3/one/slot-1", "received_ns":20_000_000}),
            json!({"path":"/g3/one/end", "received_ns":110_000_000}),
        ];
        validate_outbound_progress(&probe, &events).unwrap();
        for mutation in [
            "missing",
            "duplicate",
            "late",
            "foreign",
            "protocol",
            "polled",
            "adapter",
            "pause",
        ] {
            let mut events = events.clone();
            let mut probe = probe.clone();
            match mutation {
                "missing" => {
                    events.remove(1);
                }
                "duplicate" => events.push(events[1].clone()),
                "late" => events[1]["received_ns"] = json!(105_000_000),
                "foreign" => events[1]["path"] = json!("/g3/another/slot-0"),
                "protocol" => probe["protocol"] = json!("unsupported"),
                "polled" => probe["consumer_polls_before_window_end"] = json!(1),
                "adapter" => probe["adapter"] = json!("unknown"),
                "pause" => probe["pause_ns"] = json!(0),
                _ => unreachable!(),
            }
            assert!(
                validate_outbound_progress(&probe, &events).is_err(),
                "{mutation}"
            );
        }
        let mut lazy = probe;
        lazy["case"] = json!("lazy-single");
        lazy["slots"] = json!(1);
        let events = vec![
            events[0].clone(),
            events[3].clone(),
            json!({"path":"/g3/one/slot-0", "received_ns":120_000_000}),
        ];
        validate_outbound_progress(&lazy, &events).unwrap();
    }
}

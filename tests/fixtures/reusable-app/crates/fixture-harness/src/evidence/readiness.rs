use super::{Result, require};
use serde_json::Value;
use std::collections::HashSet;

const CASES: [&str; 11] = [
    "truncated",
    "silent-head",
    "pending-body",
    "trickle",
    "siblings",
    "exhausted",
    "eof",
    "empty-eof",
    "malformed-head",
    "malformed-body",
    "head-error",
];

#[derive(Clone, Copy, Debug)]
pub struct ReadinessLeg {
    pub case: &'static str,
    pub mode: &'static str,
    pub consumption: &'static str,
    pub read_limit: usize,
    pub repetition: usize,
}

impl ReadinessLeg {
    pub fn key(&self) -> String {
        format!(
            "{}/{}/{}/{}/{}",
            self.case, self.mode, self.consumption, self.read_limit, self.repetition
        )
    }
}

pub fn readiness_legs() -> Vec<ReadinessLeg> {
    let mut legs = Vec::new();
    for repetition in 0..3 {
        legs.push(ReadinessLeg {
            case: "truncated",
            mode: "direct",
            consumption: "buffered",
            read_limit: 1,
            repetition,
        });
        for case in CASES {
            for consumption in ["buffered", "streamed"] {
                for read_limit in [1, 16_384] {
                    legs.push(ReadinessLeg {
                        case,
                        mode: "selected",
                        consumption,
                        read_limit,
                        repetition,
                    });
                }
            }
        }
    }
    legs
}

fn payload(case: &str, slot: usize) -> &'static [u8] {
    match case {
        "truncated" | "exhausted" | "malformed-body" => b"x",
        "trickle" => b"xy",
        "eof" => b"xyz",
        "siblings" if slot == 1 => b"xyz",
        _ => b"",
    }
}

fn fingerprint(bytes: &[u8]) -> String {
    format!(
        "{:016x}",
        bytes
            .iter()
            .fold(0xcbf29ce484222325u64, |n, b| (n ^ u64::from(*b))
                .wrapping_mul(0x100000001b3))
    )
}

fn expected(case: &str, slot: usize) -> (&'static str, &'static str) {
    match case {
        "silent-head" => ("timeout", "head"),
        "pending-body" | "trickle" | "exhausted" => ("timeout", "body"),
        "siblings" if slot == 0 => ("timeout", "body"),
        "eof" | "empty-eof" | "siblings" => ("eof", "body"),
        "head-error" | "malformed-head" => ("error", "head"),
        _ => ("error", "body"),
    }
}

fn origin(probe: &Value, slots: usize) -> Result<()> {
    let origin = &probe["origin"];
    require(
        origin["case"] == probe["case"] && origin["token"] == probe["token"],
        "foreign readiness origin",
    )?;
    require(
        origin["accepted"] == slots
            && origin["workers_joined"] == slots
            && origin["max_workers"] == slots
            && origin["harness_shutdown"] == true
            && origin["watchdog"] == false
            && origin["errors"].as_array().is_some_and(Vec::is_empty),
        "origin workers, errors or containment invalid",
    )?;
    let requests = origin["requests"]
        .as_array()
        .ok_or("missing origin requests")?;
    require(requests.len() == slots, "wrong origin request count")?;
    let mut seen = HashSet::new();
    for request in requests {
        let slot = request["slot"].as_u64().ok_or("missing origin slot")? as usize;
        require(
            slot < slots
                && seen.insert(slot)
                && request["token"] == probe["token"]
                && request["path"]
                    == format!(
                        "/readiness/{}/{slot}",
                        probe["case"].as_str().unwrap_or_default()
                    )
                && request["received_us"].as_u64().is_some(),
            "wrong/duplicate origin request",
        )?;
    }
    let terminations = origin["terminations"]
        .as_array()
        .ok_or("missing origin terminations")?;
    require(
        terminations.len() == slots,
        "wrong origin termination count",
    )?;
    let mut terminated = HashSet::new();
    for termination in terminations {
        let slot = termination["slot"]
            .as_u64()
            .ok_or("missing termination slot")?;
        require(
            slot < slots as u64
                && terminated.insert(slot)
                && matches!(
                    termination["reason"].as_str(),
                    Some("peer-close" | "harness-stop" | "complete")
                ),
            "wrong/duplicate origin termination or failed worker",
        )?;
    }
    let writes = origin["writes"].as_array().ok_or("missing origin writes")?;
    for write in writes {
        require(
            write["slot"]
                .as_u64()
                .is_some_and(|slot| slot < slots as u64)
                && write["at_us"].as_u64().is_some_and(|at| at <= 3_000_000)
                && write["bytes"].as_u64().is_some_and(|bytes| bytes > 0)
                && matches!(write["kind"].as_str(), Some("head" | "body")),
            "invalid origin write",
        )?;
        require(
            !matches!(probe["case"].as_str(), Some("silent-head" | "head-error"))
                || write["kind"] != "head",
            "head written by silent/head-error origin",
        )?;
    }
    Ok(())
}

fn trace(probe: &Value, leg: &ReadinessLeg) -> Result<()> {
    let count = if leg.case == "siblings" { 2 } else { 1 };
    origin(probe, count)?;
    let samples = probe["samples"]
        .as_array()
        .ok_or("missing readiness samples")?;
    require(
        !samples.is_empty() && samples.len() <= 512,
        "empty/oversized readiness trace",
    )?;
    let mut phase = vec![None; count];
    let mut dispatched = vec![false; count];
    let mut finished = vec![false; count];
    let mut bytes = vec![0u64; count];
    let mut terminal = vec![None; count];
    let mut terminal_slot = None;
    let mut permission = None;
    let mut last_selection = Vec::new();
    let mut last_end = 0;
    let mut handoffs = 0;
    let mut read_count = 0;
    let mut nonzero_selection = false;
    let rows = probe["slots"]
        .as_array()
        .ok_or("missing readiness slot results")?;
    require(rows.len() == count, "wrong readiness slot count")?;
    for sample in samples {
        let operation = sample["operation"].as_str().ok_or("missing operation")?;
        let start = sample["start_us"]
            .as_u64()
            .ok_or("missing operation start")?;
        let end = sample["end_us"].as_u64().ok_or("missing operation end")?;
        require(
            last_end <= start && start <= end && end <= 505_000,
            "reordered or late operation",
        )?;
        last_end = end;
        require(
            operation == "finish" || start < 500_000,
            "operation starts after expiry",
        )?;
        if let Some(slot) = terminal_slot {
            require(
                operation == "finish" && sample["slot"] == slot,
                "source EOF/error must be followed by immediate same-slot finish",
            )?;
        }
        if let Some((slot, kind)) = permission {
            let matching = sample["slot"] == slot
                && operation == if kind == "head" { "head-poll" } else { "read" };
            require(
                matching
                    || (operation == "finish"
                        && sample["outcome"] == "timeout"
                        && start >= 500_000),
                "unused selection permits only matching I/O or deadline finish",
            )?;
        }
        if operation == "select" {
            require(
                permission.is_none() && dispatched.iter().all(|v| *v),
                "unused readiness or undispatched slot",
            )?;
            let heads_only = leg.case == "siblings" && phase.contains(&Some("head"));
            let live: Vec<_> = (0..count)
                .filter(|i| phase[*i].is_some_and(|p| !heads_only || p == "head"))
                .collect();
            let selected_slots = sample["slots"]
                .as_array()
                .ok_or("missing selection slots")?;
            let selected_phases = sample["phases"]
                .as_array()
                .ok_or("missing selection phases")?;
            require(
                !live.is_empty()
                    && selected_slots.len() == live.len()
                    && selected_phases.len() == live.len(),
                "wrong selection domain",
            )?;
            for (index, slot) in live.iter().enumerate() {
                require(
                    selected_slots[index] == *slot
                        && selected_phases[index].as_str() == phase[*slot],
                    "wrong selection map",
                )?;
            }
            let wait = ((500_000 - start) / 1000).max(1);
            require(
                sample["status"] == "FastlyStatus::OK" && sample["timeout_ms"] == wait,
                "invalid select status/remaining budget",
            )?;
            let selected = sample.get("selected").ok_or("missing selected outcome")?;
            if !selected.is_null() {
                let index = usize::try_from(selected.as_u64().ok_or("invalid selected index")?)?;
                require(index < live.len(), "selected index outside domain")?;
                if end < 500_000 {
                    let slot = live[index];
                    permission = Some((slot, phase[slot].unwrap()));
                    nonzero_selection |= index > 0 && phase[slot] == Some("body");
                }
            }
            last_selection = live
                .iter()
                .map(|slot| (*slot, phase[*slot].unwrap()))
                .collect();
            continue;
        }
        if operation == "handoff" {
            require(
                permission.is_none()
                    && leg.consumption == "streamed"
                    && handoffs == 0
                    && read_count == 0
                    && sample["body_reads"] == 0
                    && sample["deadline_us"] == 500_000,
                "streamed handoff changed deadline or already consumed the body",
            )?;
            handoffs += 1;
            last_selection.clear();
            continue;
        }
        let slot = sample["slot"].as_u64().ok_or("missing operation slot")? as usize;
        require(
            slot < count && !finished[slot],
            "invalid/completed operation slot",
        )?;
        match operation {
            "dispatch" => {
                require(
                    !dispatched[slot]
                        && sample["outcome"] == "pending"
                        && sample["error"].is_null()
                        && sample["bytes"].is_null(),
                    "bad or duplicate dispatch",
                )?;
                dispatched[slot] = true;
                phase[slot] = Some("head");
            }
            "head-poll" => {
                require(
                    permission.take() == Some((slot, "head")) && phase[slot] == Some("head"),
                    "head poll without matching readiness",
                )?;
                require(sample["bytes"].is_null(), "head poll contains body bytes")?;
                if end < 500_000 {
                    last_selection.clear();
                }
                match sample["outcome"].as_str() {
                    Some("ready") => {
                        require(
                            sample["error"].is_null(),
                            "ready head also contains an error",
                        )?;
                        if end < 500_000 {
                            phase[slot] = Some("body");
                        }
                    }
                    Some("error") => {
                        let error = sample["error"]
                            .as_str()
                            .filter(|e| !e.is_empty())
                            .ok_or("missing head error category")?;
                        terminal[slot] = Some(("error", "head", Some(error), end));
                        terminal_slot = Some(slot);
                    }
                    _ => return Err("selected head was not ready".into()),
                }
            }
            "read" => {
                require(
                    phase[slot] == Some("body") && sample["capacity"] == leg.read_limit,
                    "wrong read phase/capacity",
                )?;
                require(
                    leg.consumption != "streamed" || handoffs == 1,
                    "streamed read before handoff",
                )?;
                if leg.mode == "selected" {
                    require(
                        permission.take() == Some((slot, "body")),
                        "read without fresh same-slot readiness",
                    )?;
                } else {
                    require(
                        leg.case == "truncated",
                        "unbounded direct read outside finite control",
                    )?;
                }
                read_count += 1;
                if end < 500_000 {
                    last_selection.clear();
                }
                require(
                    sample.get("bytes").is_some() != sample.get("error").is_some(),
                    "contradictory read outcome",
                )?;
                if let Some(n) = sample["bytes"].as_u64() {
                    require(n <= leg.read_limit as u64, "oversized read")?;
                    if end < 500_000 {
                        bytes[slot] += n;
                    }
                    if n == 0 {
                        terminal[slot] = Some(("eof", "body", None, end));
                        terminal_slot = Some(slot);
                    }
                } else {
                    let error = sample["error"]
                        .as_str()
                        .filter(|e| !e.is_empty())
                        .ok_or("missing read error category")?;
                    terminal[slot] = Some(("error", "body", Some(error), end));
                    terminal_slot = Some(slot);
                }
            }
            "finish" => {
                let outcome = sample["outcome"]
                    .as_str()
                    .ok_or("missing terminal outcome")?;
                let actual_phase = sample["phase"].as_str().ok_or("missing terminal phase")?;
                require(Some(actual_phase) == phase[slot], "terminal phase mismatch")?;
                if outcome == "timeout" {
                    require(start >= 500_000, "premature deadline terminal")?;
                    require(
                        last_selection.contains(&(slot, actual_phase)),
                        "timeout lacks a final matching readiness wait",
                    )?;
                    require(
                        terminal[slot].is_none_or(|(_, _, _, at)| at >= 500_000),
                        "source terminal replaced by delayed timeout",
                    )?;
                } else {
                    last_selection.clear();
                    require(
                        end < 500_000
                            && terminal[slot].is_some_and(|(kind, phase, _, _)| {
                                (kind, phase) == (outcome, actual_phase)
                            }),
                        "invented or late terminal",
                    )?;
                }
                require(
                    (outcome, actual_phase) == expected(leg.case, slot),
                    "wrong case terminal",
                )?;
                require(
                    rows[slot]["outcome"] == outcome
                        && rows[slot]["phase"] == actual_phase
                        && rows[slot]["at_us"] == end,
                    "slot summary differs from trace",
                )?;
                if leg.case == "truncated" {
                    require(
                        sample["error"] == "UnexpectedEof",
                        "truncated source error was lost",
                    )?;
                }
                if let Some((_, _, Some(error), _)) = terminal[slot] {
                    require(
                        sample["error"].as_str() == Some(error)
                            && rows[slot]["error"].as_str() == Some(error),
                        "terminal error differs from source",
                    )?;
                } else {
                    require(
                        sample["error"].is_null() && rows[slot]["error"].is_null(),
                        "terminal contains an unobserved source error",
                    )?;
                }
                phase[slot] = None;
                finished[slot] = true;
                permission = None;
                terminal_slot = None;
            }
            _ => return Err("unknown readiness operation".into()),
        }
    }
    require(
        permission.is_none()
            && terminal_slot.is_none()
            && finished.iter().all(|v| *v)
            && handoffs == usize::from(leg.consumption == "streamed"),
        "missing terminal or streamed handoff",
    )?;
    require(
        leg.case != "siblings" || nonzero_selection,
        "ready sibling at nonzero index was not observed",
    )?;
    for (slot, row) in rows.iter().enumerate() {
        let wanted = payload(leg.case, slot);
        require(
            bytes[slot] == wanted.len() as u64
                && row["bytes"] == bytes[slot]
                && row["checksum"] == fingerprint(wanted)
                && row["buffered_bytes"]
                    == if leg.consumption == "buffered" {
                        wanted.len()
                    } else {
                        0
                    },
            "read bytes, checksum or consumption differs",
        )?;
    }
    require(
        probe["completed_us"]
            .as_u64()
            .is_some_and(|end| (last_end..=505_000).contains(&end)),
        "missing/late probe completion",
    )
}

pub fn validate_outbound_readiness(probes: &[Value], revision: &str) -> Result<()> {
    require(
        revision.len() == 40 && revision.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid expected revision",
    )?;
    let legs = readiness_legs();
    require(
        probes.len() == legs.len(),
        "missing readiness case inventory",
    )?;
    let mut identities = HashSet::new();
    let mut tokens = HashSet::new();
    let mut matched = Vec::with_capacity(legs.len());
    for probe in probes {
        let leg = legs
            .iter()
            .find(|leg| {
                probe["case"] == leg.case
                    && probe["mode"] == leg.mode
                    && probe["consumption"] == leg.consumption
                    && probe["read_limit"] == leg.read_limit
                    && probe["repetition"] == leg.repetition
            })
            .ok_or("unknown readiness leg")?;
        let token = probe["token"].as_str().ok_or("missing readiness token")?;
        require(
            probe["schema"] == 2
                && probe["revision"] == revision
                && probe["deadline_us"] == 500_000
                && probe["tolerance_us"] == 5000
                && probe["qualification"] == false
                && !token.is_empty()
                && token.len() <= 128
                && token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                && identities.insert(leg.key())
                && tokens.insert(token),
            "invalid/duplicate readiness identity",
        )?;
        matched.push((probe, leg));
    }
    let failures: Vec<_> = matched
        .into_iter()
        .filter_map(|(probe, leg)| {
            trace(probe, leg)
                .err()
                .map(|error| format!("{}: {error}", leg.key()))
        })
        .collect();
    require(
        failures.is_empty(),
        format!(
            "{} readiness leg(s) failed:\n{}",
            failures.len(),
            failures.join("\n")
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const REVISION: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    struct Trace {
        samples: Vec<Value>,
        phases: Vec<Option<&'static str>>,
        now: u64,
    }

    impl Trace {
        fn push(&mut self, mut sample: Value) {
            sample["start_us"] = json!(self.now);
            self.now += 1;
            sample["end_us"] = json!(self.now);
            self.samples.push(sample);
        }

        fn select(&mut self, case: &str, slot: Option<usize>) {
            let heads_only = case == "siblings" && self.phases.contains(&Some("head"));
            let live: Vec<_> = (0..self.phases.len())
                .filter(|i| self.phases[*i].is_some_and(|p| !heads_only || p == "head"))
                .collect();
            let selected = slot.map(|slot| live.iter().position(|i| *i == slot).unwrap());
            let phases: Vec<_> = live.iter().map(|i| self.phases[*i].unwrap()).collect();
            let start = self.now;
            let end = if slot.is_some() { start + 1 } else { 500_000 };
            self.samples
                .push(json!({"operation":"select", "start_us":start, "end_us":end,
                "slots":live, "phases":phases, "selected":selected,
                "status":"FastlyStatus::OK", "timeout_ms":((500_000 - start) / 1000).max(1)}));
            self.now = end;
        }
    }

    fn matrix() -> Vec<Value> {
        readiness_legs().iter().enumerate().map(|(index, leg)| {
            let count = if leg.case == "siblings" { 2 } else { 1 };
            let token = format!("readiness-{index}");
            let mut trace = Trace { samples: Vec::new(), phases: vec![Some("head"); count], now: 0 };
            for slot in 0..count {
                trace.push(json!({"operation":"dispatch", "slot":slot, "outcome":"pending"}));
            }
            if leg.consumption == "streamed" {
                trace.push(json!({"operation":"handoff", "body_reads":0, "deadline_us":500_000}));
            }
            if leg.case != "silent-head" {
                for slot in 0..count {
                    trace.select(leg.case, Some(slot));
                    let mut poll = json!({"operation":"head-poll", "slot":slot, "outcome":"ready"});
                    if expected(leg.case, slot) == ("error", "head") {
                        poll["outcome"] = json!("error");
                        poll["error"] = json!("HttpProtocolError");
                    } else {
                        trace.phases[slot] = Some("body");
                    }
                    trace.push(poll);
                }
            }
            let mut rows = vec![Value::Null; count];
            // Finish the ready sibling before observing the pending slot's deadline.
            for slot in (0..count).rev() {
                let wanted = payload(leg.case, slot);
                for chunk in wanted.chunks(leg.read_limit) {
                    if leg.mode == "selected" { trace.select(leg.case, Some(slot)); }
                    trace.push(json!({"operation":"read", "slot":slot, "capacity":leg.read_limit, "bytes":chunk.len()}));
                }
                let (outcome, phase) = expected(leg.case, slot);
                let error = match (outcome, phase) {
                    ("error", "head") => Some("HttpProtocolError"),
                    ("error", _) if leg.case == "truncated" => Some("UnexpectedEof"),
                    ("error", _) => Some("Other"),
                    _ => None,
                };
                if outcome == "timeout" {
                    trace.select(leg.case, None);
                } else if phase == "body" {
                    if leg.mode == "selected" { trace.select(leg.case, Some(slot)); }
                    let mut read = json!({"operation":"read", "slot":slot, "capacity":leg.read_limit});
                    if let Some(error) = error { read["error"] = json!(error); }
                    else { read["bytes"] = json!(0); }
                    trace.push(read);
                }
                let mut finish = json!({"operation":"finish", "slot":slot, "outcome":outcome, "phase":phase});
                if let Some(error) = error { finish["error"] = json!(error); }
                trace.push(finish);
                trace.phases[slot] = None;
                let mut row = json!({"outcome":outcome, "phase":phase, "at_us":trace.now,
                    "bytes":wanted.len(), "checksum":fingerprint(wanted),
                    "buffered_bytes":if leg.consumption == "buffered" { wanted.len() } else { 0 }});
                if let Some(error) = error { row["error"] = json!(error); }
                rows[slot] = row;
            }
            let requests: Vec<_> = (0..count).map(|slot| json!({"slot":slot, "token":token,
                "path":format!("/readiness/{}/{slot}", leg.case), "received_us":1})).collect();
            let terminations: Vec<_> = (0..count).map(|slot| json!({"slot":slot, "reason":"complete"})).collect();
            let writes: Vec<_> = if matches!(leg.case, "silent-head" | "head-error") {
                Vec::new()
            } else {
                (0..count).map(|slot| json!({"slot":slot, "kind":"head", "at_us":1, "bytes":1})).collect()
            };
            json!({"schema":2, "case":leg.case, "mode":leg.mode, "consumption":leg.consumption,
                "read_limit":leg.read_limit, "repetition":leg.repetition, "revision":REVISION,
                "token":token, "deadline_us":500_000, "tolerance_us":5000, "qualification":false,
                "samples":trace.samples, "slots":rows, "completed_us":trace.now,
                "origin":{"case":leg.case, "token":token, "accepted":count, "workers_joined":count,
                    "max_workers":count, "harness_shutdown":true, "watchdog":false, "errors":[],
                    "requests":requests, "terminations":terminations, "writes":writes}})
        }).collect()
    }

    fn leg<'a>(probes: &'a mut [Value], case: &str, consumption: &str) -> &'a mut Value {
        probes
            .iter_mut()
            .find(|p| {
                p["case"] == case
                    && p["consumption"] == consumption
                    && p["mode"] == "selected"
                    && p["read_limit"] == 1
                    && p["repetition"] == 0
            })
            .unwrap()
    }

    fn samples(probe: &mut Value) -> &mut Vec<Value> {
        probe["samples"].as_array_mut().unwrap()
    }

    fn operation<'a>(probe: &'a mut Value, name: &str) -> &'a mut Value {
        samples(probe)
            .iter_mut()
            .find(|s| s["operation"] == name)
            .unwrap()
    }

    #[test]
    fn readiness_accepts_complete_135_leg_matrix() {
        let probes = matrix();
        assert_eq!(probes.len(), 135);
        assert_eq!(probes.iter().filter(|p| p["mode"] == "direct").count(), 3);
        assert_eq!(
            probes
                .iter()
                .map(|p| p["slots"].as_array().unwrap().len())
                .sum::<usize>(),
            147
        );
        validate_outbound_readiness(&probes, REVISION).unwrap();
    }

    #[test]
    fn outbound_readiness_rejects_empty_or_unbounded_measurements() {
        assert!(validate_outbound_readiness(&[], REVISION).is_err());
        let baseline = matrix();
        validate_outbound_readiness(&baseline, REVISION).unwrap();
        for wait in [0, 501, u64::MAX] {
            let mut probes = baseline.clone();
            operation(&mut probes[0], "select")["timeout_ms"] = json!(wait);
            assert!(
                validate_outbound_readiness(&probes, REVISION).is_err(),
                "wait={wait}"
            );
        }
    }

    #[test]
    fn readiness_accepts_selection_abandoned_at_absolute_deadline() {
        let mut probes = matrix();
        let probe = leg(&mut probes, "silent-head", "buffered");
        let selection = operation(probe, "select");
        selection["selected"] = json!(0);
        selection["end_us"] = json!(499_999);
        validate_outbound_readiness(&probes, REVISION).unwrap();
    }

    #[test]
    fn readiness_rearms_early_timer_against_original_deadline() {
        let mut probes = matrix();
        let probe = leg(&mut probes, "silent-head", "buffered");
        let events = samples(probe);
        let selection = events
            .iter()
            .position(|s| s["operation"] == "select")
            .unwrap();
        events[selection]["end_us"] = json!(499_001);
        let mut rearmed = events[selection].clone();
        rearmed["start_us"] = json!(499_001);
        rearmed["end_us"] = json!(500_000);
        rearmed["timeout_ms"] = json!(1);
        events.insert(selection + 1, rearmed);
        validate_outbound_readiness(&probes, REVISION).unwrap();
    }

    #[test]
    fn readiness_aggregates_contextual_trace_failures() {
        let mut probes = matrix();
        operation(&mut probes[0], "finish")["error"] = json!("Other");
        leg(&mut probes, "pending-body", "streamed")["completed_us"] = json!(507_794);
        operation(leg(&mut probes, "malformed-head", "buffered"), "finish")["error"] =
            json!("Other");
        let error = validate_outbound_readiness(&probes, REVISION)
            .unwrap_err()
            .to_string();
        assert!(error.starts_with("3 readiness leg(s) failed:"), "{error}");
        for key in [
            "truncated/direct/buffered/1/0",
            "pending-body/selected/streamed/1/0",
            "malformed-head/selected/buffered/1/0",
        ] {
            assert!(error.contains(&format!("{key}: ")), "{error}");
        }
    }

    #[test]
    fn readiness_replays_retained_actual_negative_matrices() {
        use std::{
            collections::BTreeMap,
            fs::File,
            io::{BufRead, BufReader},
        };

        let path = crate::root()
            .join("../../../docs/audits/evidence/2026-10-23-fastly-readiness-matrix-probes.jsonl");
        let mut runs: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for line in BufReader::new(File::open(path).unwrap()).lines() {
            let row: Value = serde_json::from_str(&line.unwrap()).unwrap();
            let run = row["run"].as_str().unwrap().to_owned();
            runs.entry(run).or_default().push(row["probe"].clone());
        }
        assert_eq!(runs.len(), 4);
        assert_eq!(runs.values().map(Vec::len).sum::<usize>(), 540);
        let source_error_keys: std::collections::BTreeSet<_> = readiness_legs()
            .iter()
            .filter(|leg| {
                leg.mode == "selected" && matches!(leg.case, "truncated" | "malformed-body")
            })
            .map(ReadinessLeg::key)
            .collect();
        for (run, failed, late) in [
            ("repaired-before-trace-hardening", 1, 1),
            ("stock-before-trace-hardening", 28, 4),
            ("repaired-current", 1, 1),
            ("stock-current", 24, 0),
        ] {
            let probes = runs.get(run).unwrap();
            assert_eq!(probes.len(), 135, "{run}");
            assert_eq!(
                probes
                    .iter()
                    .map(|p| p["origin"]["accepted"].as_u64().unwrap())
                    .sum::<u64>(),
                147,
                "{run}"
            );
            let error =
                validate_outbound_readiness(probes, "d12730fda2704ba4c95e878dccd50aef8c4ec22b")
                    .expect_err("retained failed measurements must never pass")
                    .to_string();
            eprintln!("{run}: {error}");
            assert!(
                error.starts_with(&format!("{failed} readiness leg(s) failed:\n")),
                "{run}: {error}"
            );
            let failures: BTreeMap<_, _> = error
                .lines()
                .skip(1)
                .map(|line| line.split_once(": ").unwrap())
                .collect();
            assert_eq!(failures.len(), failed, "{run}: {error}");
            assert_eq!(
                failures
                    .values()
                    .filter(|reason| matches!(
                        **reason,
                        "reordered or late operation" | "missing/late probe completion"
                    ))
                    .count(),
                late,
                "{run}: {error}"
            );
            if run == "stock-current" {
                assert_eq!(
                    failures
                        .keys()
                        .map(|key| (*key).to_owned())
                        .collect::<std::collections::BTreeSet<_>>(),
                    source_error_keys
                );
                assert!(
                    failures
                        .values()
                        .all(|reason| *reason == "wrong case terminal")
                );
            } else if run.starts_with("repaired") {
                let (key, completed) = if run == "repaired-current" {
                    ("siblings/selected/streamed/1/1", 507_233)
                } else {
                    ("pending-body/selected/streamed/1/0", 507_794)
                };
                assert!(failures.contains_key(key), "{run}: {error}");
                let leg = readiness_legs()
                    .into_iter()
                    .find(|leg| leg.key() == key)
                    .unwrap();
                let probe = probes
                    .iter()
                    .find(|p| {
                        p["case"] == leg.case
                            && p["mode"] == leg.mode
                            && p["consumption"] == leg.consumption
                            && p["read_limit"] == leg.read_limit
                            && p["repetition"] == leg.repetition
                    })
                    .unwrap();
                assert_eq!(probe["completed_us"], completed, "{run}");
            }
        }
    }

    #[test]
    fn readiness_rejects_mutated_matrix() {
        type Mutation = (&'static str, fn(&mut Vec<Value>));
        let mutations: &[Mutation] = &[
            ("missing leg", |p| {
                p.pop();
            }),
            ("duplicate leg", |p| {
                p[1] = p[0].clone();
            }),
            ("duplicate identity with unique token", |p| {
                for key in ["case", "mode", "consumption", "read_limit", "repetition"] {
                    p[1][key] = p[0][key].clone();
                }
            }),
            ("foreign revision", |p| {
                p[0]["revision"] = json!("b".repeat(40));
            }),
            ("wrong token", |p| {
                p[0]["token"] = json!("foreign");
            }),
            ("duplicate token", |p| {
                p[1]["token"] = p[0]["token"].clone();
            }),
            ("invalid token", |p| {
                p[0]["token"] = json!("bad/token");
            }),
            ("wrong schema", |p| {
                p[0]["schema"] = json!(1);
            }),
            ("wrong deadline", |p| {
                p[0]["deadline_us"] = json!(500_001);
            }),
            ("widened tolerance", |p| {
                p[0]["tolerance_us"] = json!(5001);
            }),
            ("qualification promotion", |p| {
                p[0]["qualification"] = json!(true);
            }),
            ("unknown case", |p| {
                p[0]["case"] = json!("unknown");
            }),
            ("empty samples", |p| {
                p[0]["samples"] = json!([]);
            }),
            ("missing samples", |p| {
                p[0].as_object_mut().unwrap().remove("samples");
            }),
            ("late completion", |p| {
                p[0]["completed_us"] = json!(505_001);
            }),
            ("reordered sample", |p| {
                operation(&mut p[0], "select")["start_us"] = json!(0);
            }),
            ("missing read exit", |p| {
                operation(&mut p[0], "read")
                    .as_object_mut()
                    .unwrap()
                    .remove("end_us");
            }),
            ("late read", |p| {
                operation(&mut p[0], "read")["start_us"] = json!(500_000);
            }),
            ("wrong read capacity", |p| {
                operation(&mut p[0], "read")["capacity"] = json!(16_384);
            }),
            ("oversized read", |p| {
                operation(&mut p[0], "read")["bytes"] = json!(2);
            }),
            ("contradictory read", |p| {
                operation(&mut p[0], "read")["error"] = json!("Other");
            }),
            ("negative bytes", |p| {
                operation(&mut p[0], "read")["bytes"] = json!(-1);
            }),
            ("string bytes", |p| {
                operation(&mut p[0], "read")["bytes"] = json!("1");
            }),
            ("null bytes", |p| {
                operation(&mut p[0], "read")["bytes"] = Value::Null;
            }),
            ("missing read outcome", |p| {
                operation(&mut p[0], "read")
                    .as_object_mut()
                    .unwrap()
                    .remove("bytes");
            }),
            ("bad selection status", |p| {
                operation(&mut p[0], "select")["status"] = json!("FastlyStatus::BADF");
            }),
            ("out of range selection", |p| {
                operation(&mut p[0], "select")["selected"] = json!(1);
            }),
            ("missing selection outcome", |p| {
                operation(&mut p[0], "select")
                    .as_object_mut()
                    .unwrap()
                    .remove("selected");
            }),
            ("wrong selected slot", |p| {
                operation(&mut p[0], "select")["slots"] = json!([1]);
            }),
            ("wrong selected phase", |p| {
                operation(&mut p[0], "select")["phases"] = json!(["body"]);
            }),
            ("selected head pending", |p| {
                operation(&mut p[0], "head-poll")["outcome"] = json!("pending");
            }),
            ("ready head also errors", |p| {
                operation(&mut p[0], "head-poll")["error"] = json!("HttpProtocolError");
            }),
            ("missing streamed handoff", |p| {
                samples(leg(p, "eof", "streamed")).retain(|s| s["operation"] != "handoff");
            }),
            ("duplicate handoff", |p| {
                let probe = leg(p, "eof", "streamed");
                let mut handoff = operation(probe, "handoff").clone();
                handoff["end_us"] = handoff["start_us"].clone();
                samples(probe).insert(1, handoff);
            }),
            ("handoff retains selection", |p| {
                let probe = leg(p, "eof", "streamed");
                let events = samples(probe);
                let handoff = events
                    .iter()
                    .position(|s| s["operation"] == "handoff")
                    .unwrap();
                let mut event = events.remove(handoff);
                let select = events
                    .iter()
                    .position(|s| s["operation"] == "select")
                    .unwrap();
                event["start_us"] = events[select]["end_us"].clone();
                event["end_us"] = event["start_us"].clone();
                events.insert(select + 1, event);
            }),
            ("handoff moved after first read", |p| {
                let events = samples(leg(p, "eof", "streamed"));
                let handoff = events
                    .iter()
                    .position(|s| s["operation"] == "handoff")
                    .unwrap();
                let mut event = events.remove(handoff);
                let read = events
                    .iter()
                    .position(|s| s["operation"] == "read")
                    .unwrap();
                event["start_us"] = events[read]["end_us"].clone();
                event["end_us"] = event["start_us"].clone();
                events.insert(read + 1, event);
            }),
            ("source/terminal error mismatch", |p| {
                let probe = leg(p, "malformed-body", "buffered");
                operation(probe, "finish")["error"] = json!("UnexpectedEof");
                probe["slots"][0]["error"] = json!("UnexpectedEof");
            }),
            ("head source/terminal error mismatch", |p| {
                let probe = leg(p, "malformed-head", "buffered");
                operation(probe, "finish")["error"] = json!("HttpIncompleteResponse");
                probe["slots"][0]["error"] = json!("HttpIncompleteResponse");
            }),
            ("terminal/summary error mismatch", |p| {
                p[0]["slots"][0]["error"] = json!("Other");
            }),
            ("empty source error", |p| {
                let probe = leg(p, "malformed-body", "buffered");
                samples(probe)
                    .iter_mut()
                    .find(|s| s.get("error").is_some())
                    .unwrap()["error"] = json!("");
            }),
            ("malformed late head error", |p| {
                let probe = leg(p, "silent-head", "buffered");
                let events = samples(probe);
                let wait = events
                    .iter()
                    .position(|s| s["operation"] == "select")
                    .unwrap();
                events[wait]["selected"] = json!(0);
                events[wait]["end_us"] = json!(499_999);
                events.insert(
                    wait + 1,
                    json!({"operation":"head-poll", "slot":0,
                    "start_us":499_999, "end_us":500_000, "outcome":"error"}),
                );
            }),
            ("EOF read again", |p| {
                let probe = leg(p, "empty-eof", "buffered");
                let events = samples(probe);
                let eof = events
                    .iter()
                    .position(|s| s["operation"] == "read")
                    .unwrap();
                let at = events[eof]["end_us"].as_u64().unwrap();
                let selection = json!({"operation":"select", "slots":[0], "phases":["body"],
                    "start_us":at, "end_us":at, "timeout_ms":((500_000 - at) / 1000).max(1),
                    "status":"FastlyStatus::OK", "selected":0});
                let read = json!({"operation":"read", "slot":0, "capacity":1, "bytes":0, "start_us":at, "end_us":at});
                events.splice(eof + 1..eof + 1, [selection, read]);
            }),
            ("source error read again", |p| {
                let probe = &mut p[0];
                let events = samples(probe);
                let error = events
                    .iter()
                    .position(|s| s["operation"] == "read" && s.get("error").is_some())
                    .unwrap();
                let mut read = events[error].clone();
                read["start_us"] = read["end_us"].clone();
                events.insert(error + 1, read);
            }),
            ("EOF finishes another slot first", |p| {
                let probe = leg(p, "siblings", "buffered");
                let events = samples(probe);
                let terminal = events
                    .iter()
                    .position(|s| s["operation"] == "finish")
                    .unwrap();
                let eof_finish = events.remove(terminal);
                let timeout = events.last().unwrap()["start_us"].clone();
                let mut eof_finish = eof_finish;
                eof_finish["start_us"] = timeout.clone();
                eof_finish["end_us"] = timeout;
                events.push(eof_finish);
            }),
            ("premature timeout", |p| {
                operation(leg(p, "silent-head", "buffered"), "finish")["start_us"] = json!(499_999);
            }),
            ("silent head timeout without readiness", |p| {
                samples(leg(p, "silent-head", "buffered")).retain(|s| s["operation"] != "select");
            }),
            ("pending body timeout without body readiness", |p| {
                samples(leg(p, "pending-body", "buffered"))
                    .retain(|s| s["operation"] != "select" || s["phases"][0] != "body");
            }),
            ("exhausted timeout without another readiness wait", |p| {
                let events = samples(leg(p, "exhausted", "buffered"));
                let wait = events
                    .iter()
                    .rposition(|s| s["operation"] == "select")
                    .unwrap();
                events.remove(wait);
            }),
            ("bad origin token", |p| {
                p[0]["origin"]["requests"][0]["token"] = json!("foreign");
            }),
            ("origin watchdog", |p| {
                p[0]["origin"]["watchdog"] = json!(true);
            }),
            ("missing origin terminations", |p| {
                p[0]["origin"]
                    .as_object_mut()
                    .unwrap()
                    .remove("terminations");
            }),
            ("missing worker termination", |p| {
                p[0]["origin"]["terminations"] = json!([]);
            }),
            ("duplicate worker termination", |p| {
                let probe = leg(p, "siblings", "buffered");
                probe["origin"]["terminations"][1] = probe["origin"]["terminations"][0].clone();
            }),
            ("invalid termination slot", |p| {
                p[0]["origin"]["terminations"][0]["slot"] = json!(1);
            }),
            ("watchdog worker termination", |p| {
                p[0]["origin"]["terminations"][0]["reason"] = json!("watchdog");
            }),
            ("I/O error worker termination", |p| {
                p[0]["origin"]["terminations"][0]["reason"] = json!("io-error");
            }),
            ("unknown worker termination", |p| {
                p[0]["origin"]["terminations"][0]["reason"] = json!("unknown");
            }),
            ("missing origin writes", |p| {
                p[0]["origin"].as_object_mut().unwrap().remove("writes");
            }),
            ("invalid write slot", |p| {
                p[0]["origin"]["writes"][0]["slot"] = json!(1);
            }),
            ("late origin write", |p| {
                p[0]["origin"]["writes"][0]["at_us"] = json!(3_000_001);
            }),
            ("zero-byte origin write", |p| {
                p[0]["origin"]["writes"][0]["bytes"] = json!(0);
            }),
            ("silent origin head write", |p| {
                leg(p, "silent-head", "buffered")["origin"]["writes"] = json!([
                    {"slot":0, "kind":"head", "at_us":1, "bytes":1}
                ]);
            }),
            ("head-error origin head write", |p| {
                leg(p, "head-error", "buffered")["origin"]["writes"] = json!([
                    {"slot":0, "kind":"head", "at_us":1, "bytes":1}
                ]);
            }),
            ("missing origin receipt", |p| {
                p[0]["origin"]["requests"] = json!([]);
            }),
            ("wrong checksum", |p| {
                p[0]["slots"][0]["checksum"] = json!("0000000000000000");
            }),
            ("wrong consumption", |p| {
                leg(p, "eof", "streamed")["slots"][0]["buffered_bytes"] = json!(3);
            }),
        ];
        let baseline = matrix();
        validate_outbound_readiness(&baseline, REVISION).unwrap();
        let accepted: Vec<_> = mutations
            .iter()
            .filter_map(|(name, mutate)| {
                let mut probes = baseline.clone();
                mutate(&mut probes);
                validate_outbound_readiness(&probes, REVISION)
                    .is_ok()
                    .then_some(*name)
            })
            .collect();
        assert!(
            accepted.is_empty(),
            "accepted malformed traces: {accepted:?}"
        );
    }
}

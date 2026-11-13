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

pub fn readiness_progress_legs() -> Vec<ReadinessLeg> {
    let mut legs = Vec::new();
    for repetition in 0..3 {
        for case in ["mixed-head-body", "mixed-body-head", "full-body-error"] {
            for consumption in ["buffered", "streamed"] {
                legs.push(ReadinessLeg {
                    case,
                    mode: "selected",
                    consumption,
                    read_limit: 16_384,
                    repetition,
                });
            }
        }
    }
    legs
}

pub fn readiness_client_legs() -> Vec<ReadinessLeg> {
    let mut legs = Vec::new();
    for repetition in 0..3 {
        for (case, consumption) in [
            ("mixed-head-body", "buffered"),
            ("mixed-body-head", "buffered"),
            ("full-body-error", "buffered"),
            ("full-body-error", "streamed"),
        ] {
            legs.push(ReadinessLeg {
                case,
                mode: "client",
                consumption,
                read_limit: 16_384,
                repetition,
            });
        }
    }
    legs
}

fn client_leg(probe: &Value) -> Result<ReadinessLeg> {
    readiness_client_legs()
        .into_iter()
        .find(|leg| {
            probe["case"] == leg.case
                && probe["consumption"] == leg.consumption
                && probe["repetition"] == leg.repetition
        })
        .ok_or_else(|| "unknown actual-client leg".into())
}

pub fn validate_outbound_readiness_client_probe(probe: &Value, revision: &str) -> Result<()> {
    if probe["event"] == "fastly-p5-probe" {
        return validate_p5_probe(probe, revision);
    }
    let leg = client_leg(probe)?;
    let token = probe["token"]
        .as_str()
        .ok_or("missing actual-client token")?;
    require(
        revision.len() == 40
            && revision.bytes().all(|b| b.is_ascii_hexdigit())
            && probe["event"] == "fastly-client-progress"
            && probe["revision"] == revision
            && probe["qualification"] == false
            && !token.is_empty()
            && token.len() <= 128
            && token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "invalid actual-client identity",
    )?;
    origin(probe, slot_count(leg.case))?;
    let completed = probe["completed_us"]
        .as_u64()
        .ok_or("missing actual-client completion")?;
    let envelope = probe["result"]
        .as_object()
        .ok_or("missing actual-client result")?;
    require(envelope.len() == 1, "contradictory actual-client result")?;
    let result = envelope
        .get("Ok")
        .filter(|result| result.is_object())
        .ok_or("actual-client operation did not complete its evidence")?;
    if leg.case == "full-body-error" {
        require(
            result["kind"] == "error"
                && result["failure"]["category"] == "bad_gateway"
                && result["failure"]["source_unexpected_eof"] == true,
            "actual-client typed UnexpectedEof failure was lost",
        )?;
        let wanted = if leg.consumption == "streamed" {
            payload(leg.case, 0)
        } else {
            b""
        };
        require(
            result["bytes"] == wanted.len() && result["checksum"] == fingerprint(wanted),
            "wrong actual-client error prefix bytes/checksum",
        )?;
    } else {
        let slots = result["slots"]
            .as_array()
            .ok_or("missing actual-client slots")?;
        require(
            result["termination"] == "Cutoff" && slots.len() == 2,
            "wrong actual-client mixed termination/slot count",
        )?;
        require(
            slots[0] == serde_json::json!({"kind":"pending"}),
            "actual-client pending slot was replaced",
        )?;
        let ready = &slots[1];
        let wanted = payload(leg.case, 1);
        require(
            ready["kind"] == "ok"
                && ready["failure"].is_null()
                && ready["bytes"] == wanted.len()
                && ready["checksum"] == fingerprint(wanted)
                && ready["elapsed_us"]
                    .as_u64()
                    .is_some_and(|elapsed| elapsed < 500_000 && elapsed <= completed),
            "actual-client ready sibling is missing, late or has wrong bytes/checksum",
        )?;
    }
    Ok(())
}

pub fn validate_outbound_readiness_client(probes: &[Value], revision: &str) -> Result<()> {
    if probes
        .iter()
        .any(|probe| probe["event"] == "fastly-p5-probe")
    {
        require(
            probes.len() == p5_cases().len(),
            "missing P5 case inventory",
        )?;
        let mut cases = HashSet::new();
        let mut tokens = HashSet::new();
        for probe in probes {
            validate_p5_probe(probe, revision)?;
            require(
                cases.insert(probe["case"].as_str()) && tokens.insert(probe["token"].as_str()),
                "duplicate P5 case/token",
            )?;
        }
        return Ok(());
    }
    require(
        probes.len() == readiness_client_legs().len(),
        "missing actual-client case inventory",
    )?;
    let mut identities = HashSet::new();
    let mut tokens = HashSet::new();
    let failures: Vec<_> = probes
        .iter()
        .enumerate()
        .filter_map(|(index, probe)| {
            let validation = (|| -> Result<()> {
                validate_outbound_readiness_client_probe(probe, revision)?;
                require(
                    identities.insert(client_leg(probe)?.key())
                        && tokens.insert(
                            probe["token"]
                                .as_str()
                                .ok_or("missing actual-client token")?,
                        ),
                    "duplicate actual-client identity/token",
                )
            })();
            validation.err().map(|error| {
                let key =
                    client_leg(probe).map_or_else(|_| format!("row {index}"), |leg| leg.key());
                format!("{key}: {error}")
            })
        })
        .collect();
    require(
        failures.is_empty(),
        format!(
            "{} actual-client leg(s) failed:\n{}",
            failures.len(),
            failures.join("\n")
        ),
    )
}

pub fn p5_cases() -> Vec<String> {
    let mut cases: Vec<_> = [
        "two-origin",
        "pending-head",
        "optimizer-send",
        "optimizer-initial",
        "optimizer-next",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    for coding in ["identity", "gzip", "passthrough"] {
        for mode in ["buffered", "streamed"] {
            for size in ["ten", "eleven", "declared"] {
                cases.push(format!("parity/{coding}/{mode}/{size}"));
            }
        }
    }
    cases
}

fn validate_p5_probe(probe: &Value, revision: &str) -> Result<()> {
    let case = probe["case"].as_str().ok_or("missing P5 case")?;
    let token = probe["token"].as_str().ok_or("missing P5 token")?;
    require(
        probe["event"] == "fastly-p5-probe"
            && p5_cases().iter().any(|known| known == case)
            && revision.len() == 40
            && revision.bytes().all(|byte| byte.is_ascii_hexdigit())
            && probe["revision"] == revision
            && probe["qualification"] == false
            && probe["response_decoding"] == "Decode"
            && !token.is_empty()
            && token.len() <= 128
            && token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "invalid P5 identity",
    )?;
    let origin = &probe["origin"];
    let a = origin["a_port"]
        .as_u64()
        .filter(|port| (1..=65535).contains(port))
        .ok_or("invalid P5 origin A")?;
    let b = origin["b_port"]
        .as_u64()
        .filter(|port| (1..=65535).contains(port))
        .ok_or("invalid P5 origin B")?;
    require(a != b, "P5 origins are not distinct")?;
    let shape = match case {
        "two-origin" | "optimizer-next" => "redirect",
        "pending-head" => "pending",
        "optimizer-send" | "optimizer-initial" => "image",
        _ => case,
    };
    let path = format!("/p5/{token}/{b}/{shape}");
    let final_path = format!("/p5/{token}/{b}/final");
    let initial = format!("http://127.0.0.1:{a}{path}");
    let final_uri = format!("http://127.0.0.1:{b}{final_path}");
    require(
        probe["a_uri"] == initial && probe["b_uri"] == final_uri,
        "foreign P5 target",
    )?;
    let (a_count, b_count) = match case {
        "optimizer-send" | "optimizer-initial" => (0, 0),
        "optimizer-next" => (1, 0),
        "two-origin" | "pending-head" => (2, 1),
        _ => (2, 0),
    };
    require(
        origin["a"] == serde_json::json!(vec![&path; a_count])
            && origin["b"] == serde_json::json!(vec![&final_path; b_count])
            && origin["pending_released_before_reply"] == false
            && origin["pending_heads"] == if case == "pending-head" { 2 } else { 0 },
        "missing, duplicate or foreign P5 receipts/pending-head evidence",
    )?;
    let envelope = probe["result"].as_object().ok_or("missing P5 result")?;
    require(envelope.len() == 1, "contradictory P5 result")?;
    let result = envelope
        .get("Ok")
        .filter(|value| value.is_object())
        .ok_or("P5 guest failed")?;
    let follow_metadata = |value: &Value, redirects| -> Result<()> {
        require(
            value["final_uri"]
                == if redirects == 1 {
                    final_uri.as_str()
                } else {
                    initial.as_str()
                }
                && value["redirects"] == redirects
                && value.get("stop_reason") == Some(&Value::Null)
                && value["policy_calls"] == redirects
                && value.get("hop").is_none(),
            "wrong P5 follow metadata/policy count",
        )
    };
    let followed = |value: &Value, redirects, streamed| -> Result<()> {
        follow_metadata(value, redirects)?;
        let response = &value["response"];
        require(
            response["kind"] == "ok"
                && response["status"] == 200
                && response["bytes"] == serde_json::json!(b"0123456789")
                && response["streamed"] == streamed,
            "wrong P5 final bytes/mode/status",
        )
    };
    if case.starts_with("optimizer-") {
        let failure = &result["failure"];
        let hop = match case {
            "optimizer-send" => Value::Null,
            "optimizer-initial" => serde_json::json!(0),
            _ => serde_json::json!(1),
        };
        return require(
            failure["kind"] == "error"
                && failure["category"] == "bad_gateway"
                && failure["status"] == 502
                && failure["reason"] == "ImageOptimizerUnavailable"
                && failure["provider_optimizer_unsupported"] == true
                && failure.get("hop") == Some(&hop)
                && result["policy_calls"] == usize::from(case == "optimizer-next")
                && result["options_checked"] == true,
            "P5 optimizer did not reach the expected native unavailable boundary",
        );
    }
    if case == "two-origin" {
        require(
            result["send"]["kind"] == "ok"
                && result["send"]["status"] == 302
                && result["send"]["bytes"] == serde_json::json!([])
                && result["send"]["streamed"] == false,
            "P5 plain Send followed or changed the redirect",
        )?;
        return followed(&result["follow"], 1, false);
    }
    if case == "pending-head" {
        followed(&result["follow"], 1, true)?;
        return require(
            result["stop"]["response"]["kind"] == "error"
                && result["stop"]["response"]["category"] == "response_too_large"
                && result["stop"]["response"]["status"] == 502
                && result["stop"]["response"]["reason"] == "EncodedBody"
                && result["stop"]["hop"] == 0
                && result["stop"]["policy_calls"] == 1,
            "P5 stopped pending redirect bypassed final body limits",
        );
    }
    let follow = &result["follow"];
    require(
        result["send"] == follow["response"] && follow["policy_calls"] == 0,
        "P5 Send/Follow parity changed",
    )?;
    if case.ends_with("/ten") {
        followed(follow, 0, case.contains("/streamed/"))?;
        let headers = result["send"]["headers"]
            .as_array()
            .ok_or("missing P5 response headers")?;
        let encoding: Vec<_> = headers
            .iter()
            .filter(|field| field[0] == "content-encoding")
            .collect();
        require(
            if case.contains("/passthrough/") {
                encoding.len() == 1 && encoding[0][1] == "x-fixture-opaque"
            } else {
                encoding.is_empty()
            },
            "P5 coding normalization changed",
        )
    } else {
        // Identity's declared decoded length can reject even before a streamed head is returned.
        if case.contains("/buffered/") || case == "parity/identity/streamed/declared" {
            require(
                follow.get("hop") == Some(&serde_json::json!(0))
                    && ["final_uri", "redirects", "stop_reason"]
                        .iter()
                        .all(|field| follow.get(*field).is_none()),
                "wrong P5 finalization failure hop/metadata",
            )?;
        } else {
            follow_metadata(follow, 0)?;
        }
        // Passthrough skips the decoded cap; both modes still collect under the final 10-byte cap.
        let expected_reason = if case.contains("/passthrough/") {
            "BufferedBody"
        } else {
            "DecodedBody"
        };
        require(
            result["send"]["kind"] == "error"
                && result["send"]["category"] == "response_too_large"
                && result["send"]["status"] == 502
                && result["send"]["reason"] == expected_reason,
            "P5 overflow did not retain its typed response limit",
        )
    }
}

fn slot_count(case: &str) -> usize {
    if matches!(case, "siblings" | "mixed-head-body" | "mixed-body-head") {
        2
    } else {
        1
    }
}

fn payload(case: &str, slot: usize) -> &'static [u8] {
    match case {
        "mixed-head-body" | "mixed-body-head" if slot == 1 => &[b'x'; 32_769],
        "full-body-error" => &[b'x'; 32_769],
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
        "mixed-head-body" if slot == 0 => ("timeout", "head"),
        "mixed-body-head" if slot == 0 => ("timeout", "body"),
        "mixed-head-body" | "mixed-body-head" => ("eof", "body"),
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
            !(matches!(probe["case"].as_str(), Some("silent-head" | "head-error"))
                || (probe["case"] == "mixed-head-body" && write["slot"] == 0))
                || write["kind"] != "head",
            "head written by silent/head-error origin",
        )?;
    }
    let case = probe["case"].as_str().ok_or("missing origin case")?;
    if matches!(
        case,
        "mixed-head-body" | "mixed-body-head" | "full-body-error"
    ) {
        for slot in 0..slots {
            let body_bytes: u64 = writes
                .iter()
                .filter(|w| w["slot"] == slot && w["kind"] == "body")
                .try_fold(0u64, |sum, w| {
                    sum.checked_add(w["bytes"].as_u64().unwrap())
                        .ok_or("origin byte total overflow")
                })?;
            require(
                body_bytes == payload(case, slot).len() as u64,
                "wrong origin payload receipt",
            )?;
            let heads = writes
                .iter()
                .filter(|w| w["slot"] == slot && w["kind"] == "head")
                .count();
            require(
                heads == usize::from(case != "mixed-head-body" || slot != 0),
                "wrong origin head count",
            )?;
        }
        if case == "mixed-body-head" {
            let first = writes
                .iter()
                .position(|w| w["slot"] == 0 && w["kind"] == "head")
                .ok_or("missing pending-body head")?;
            let second = writes
                .iter()
                .position(|w| w["slot"] == 1 && w["kind"] == "head")
                .ok_or("missing delayed head")?;
            let first_at = writes[first]["at_us"].as_u64().unwrap();
            let second_at = writes[second]["at_us"].as_u64().unwrap();
            require(
                first < second && second_at >= first_at + 50_000,
                "delayed head control not observed",
            )?;
        }
    }
    Ok(())
}

fn trace(probe: &Value, leg: &ReadinessLeg) -> Result<()> {
    let count = slot_count(leg.case);
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
    let mut mixed_selection = false;
    let mut full_buffer_reads = 0;
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
            if sample.get("abi_enter_us").is_some() || sample.get("abi_return_us").is_some() {
                let entered = sample["abi_enter_us"]
                    .as_u64()
                    .ok_or("missing raw ABI entry")?;
                let returned = sample["abi_return_us"]
                    .as_u64()
                    .ok_or("missing raw ABI return")?;
                require(
                    start <= entered && entered <= returned && returned <= end,
                    "raw ABI timestamps outside observed selection",
                )?;
            }
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
            mixed_selection |= selected_phases.contains(&Value::from("head"))
                && selected_phases.contains(&Value::from("body"));
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
                        full_buffer_reads += usize::from(n == leg.read_limit as u64);
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
                if matches!(leg.case, "truncated" | "full-body-error") {
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
    if matches!(leg.case, "mixed-head-body" | "mixed-body-head") {
        require(
            mixed_selection,
            "mixed head/body selection was not observed",
        )?;
    }
    if matches!(
        leg.case,
        "mixed-head-body" | "mixed-body-head" | "full-body-error"
    ) {
        require(full_buffer_reads > 0, "full-capacity read was not observed")?;
    }
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
    validate_readiness(probes, revision, readiness_legs())
}

pub fn validate_outbound_readiness_progress(probes: &[Value], revision: &str) -> Result<()> {
    validate_readiness(probes, revision, readiness_progress_legs())
}

fn validate_readiness(probes: &[Value], revision: &str, legs: Vec<ReadinessLeg>) -> Result<()> {
    require(
        revision.len() == 40 && revision.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid expected revision",
    )?;
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
        matrix_for(readiness_legs())
    }

    fn matrix_for(legs: Vec<ReadinessLeg>) -> Vec<Value> {
        legs.iter().enumerate().map(|(index, leg)| {
            let count = slot_count(leg.case);
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
                    if leg.case == "mixed-head-body" && slot == 0 { continue; }
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
                    ("error", _) if matches!(leg.case, "truncated" | "full-body-error") => Some("UnexpectedEof"),
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
            let mut writes: Vec<_> = if matches!(leg.case, "silent-head" | "head-error") {
                Vec::new()
            } else {
                (0..count).filter(|slot| leg.case != "mixed-head-body" || *slot != 0)
                    .map(|slot| json!({"slot":slot, "kind":"head", "at_us":1, "bytes":1})).collect()
            };
            if leg.case == "mixed-body-head" {
                writes[1]["at_us"] = json!(50_001);
            }
            if matches!(leg.case, "mixed-head-body" | "mixed-body-head" | "full-body-error") {
                for slot in 0..count {
                    let bytes = payload(leg.case, slot).len();
                    if bytes > 0 { writes.push(json!({"slot":slot,"kind":"body","at_us":50_002,"bytes":bytes})); }
                }
            }
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

    fn client_matrix() -> Vec<Value> {
        matrix_for(readiness_client_legs())
            .into_iter()
            .map(|source| {
                let result = if source["case"] == "full-body-error" {
                    let bytes = if source["consumption"] == "streamed" {
                        payload("full-body-error", 0)
                    } else {
                        b""
                    };
                    json!({"kind":"error", "bytes":bytes.len(), "checksum":fingerprint(bytes),
                        "failure":{"category":"bad_gateway", "source_unexpected_eof":true}})
                } else {
                    json!({"termination":"Cutoff", "slots":[{"kind":"pending"},
                        {"kind":"ok", "bytes":32_769, "checksum":fingerprint(payload("full-body-error", 0)),
                        "elapsed_us":60_000}]})
                };
                json!({"event":"fastly-client-progress", "token":source["token"],
                    "case":source["case"], "consumption":source["consumption"],
                    "revision":REVISION, "qualification":false, "repetition":source["repetition"],
                    "completed_us":500_100, "origin":source["origin"], "result":{"Ok":result}})
            })
            .collect()
    }

    fn p5_matrix() -> Vec<Value> {
        p5_cases().into_iter().enumerate().map(|(index, case)| {
            let token = format!("p5-{index}");
            let shape = match case.as_str() {
                "two-origin" | "optimizer-next" => "redirect",
                "pending-head" => "pending",
                "optimizer-send" | "optimizer-initial" => "image",
                _ => &case,
            };
            let path = format!("/p5/{token}/8002/{shape}");
            let final_path = format!("/p5/{token}/8002/final");
            let initial = format!("http://127.0.0.1:8001{path}");
            let final_uri = format!("http://127.0.0.1:8002{final_path}");
            let response = json!({"kind":"ok", "status":200,
                "headers":[["content-type","application/octet-stream"],
                    ["date","Tue, 27 Oct 2026 00:00:00 GMT"]],
                "bytes":b"0123456789", "streamed":case == "pending-head" || case.contains("/streamed/")});
            let followed = |response: Value, redirects| json!({"response":response,
                "final_uri":if redirects == 1 { &final_uri } else { &initial },
                "redirects":redirects,"stop_reason":null,"policy_calls":redirects});
            let limit = json!({"kind":"error","category":"response_too_large","status":502,
                "reason":if case == "pending-head" {"EncodedBody"} else if case.contains("/passthrough/") {"BufferedBody"} else {"DecodedBody"},
                "provider_optimizer_unsupported":false,"hop":null});
            let mut a = vec![path.clone(), path.clone()];
            let mut b = Vec::new();
            let result = if case == "two-origin" {
                let mut sent = response.clone();
                sent["status"] = json!(302);
                sent["bytes"] = json!([]);
                b.push(final_path.clone());
                json!({"send":sent,"follow":followed(response,1)})
            } else if case == "pending-head" {
                b.push(final_path.clone());
                json!({"follow":followed(response,1),"stop":{"response":limit,"hop":0,"policy_calls":1}})
            } else if case.starts_with("optimizer-") {
                a.clear();
                let next = case == "optimizer-next";
                if next { a.push(path.clone()); }
                json!({"failure":{"kind":"error","category":"bad_gateway","status":502,
                    "reason":"ImageOptimizerUnavailable","provider_optimizer_unsupported":true,
                    "hop":if case == "optimizer-send" { Value::Null } else { json!(usize::from(next)) }},
                    "policy_calls":usize::from(next),"options_checked":true})
            } else {
                let mut response = response;
                if case.ends_with("/ten") {
                    if case.contains("/passthrough/") {
                        response["headers"].as_array_mut().unwrap().push(json!(["content-encoding","x-fixture-opaque"]));
                    }
                } else { response = limit; }
                let follow = if !case.ends_with("/ten")
                    && (case.contains("/buffered/") || case == "parity/identity/streamed/declared")
                {
                    json!({"response":response,"hop":0,"policy_calls":0})
                } else {
                    followed(response.clone(),0)
                };
                json!({"send":response,"follow":follow})
            };
            json!({"event":"fastly-p5-probe","case":case,"token":token,"revision":REVISION,
                "qualification":false,"a_uri":initial,"b_uri":final_uri,"response_decoding":"Decode","result":{"Ok":result},
                "origin":{"a_port":8001,"b_port":8002,"a":a,"b":b,
                    "pending_released_before_reply":false,
                    "pending_heads":if case == "pending-head" { 2 } else { 0 }}})
        }).collect()
    }

    #[test]
    fn p5_inventory_is_twenty_three_unique_cases() {
        let cases = p5_cases();
        assert_eq!(cases.len(), 23);
        assert_eq!(cases.iter().collect::<HashSet<_>>().len(), 23);
        for coding in ["identity", "gzip", "passthrough"] {
            assert_eq!(
                cases
                    .iter()
                    .filter(|case| case.starts_with(&format!("parity/{coding}/")))
                    .count(),
                6
            );
        }
        for case in [
            "two-origin",
            "pending-head",
            "optimizer-send",
            "optimizer-initial",
            "optimizer-next",
        ] {
            assert!(cases.iter().any(|known| known == case));
        }
    }

    #[test]
    fn p5_inventory_requires_complete_correlated_evidence() {
        let mut probes = p5_matrix();
        assert_eq!(probes.len(), 23);
        validate_outbound_readiness_client(&probes, REVISION).unwrap();
        probes.reverse();
        validate_outbound_readiness_client(&probes, REVISION).unwrap();
        validate_outbound_readiness_client(&client_matrix(), REVISION).unwrap();
    }

    #[test]
    fn p5_inventory_rejects_missing_duplicate_and_foreign_records() {
        let baseline = p5_matrix();
        assert!(validate_outbound_readiness_client(&[], REVISION).is_err());
        for index in 0..baseline.len() {
            let mut missing = baseline.clone();
            missing.remove(index);
            assert!(validate_outbound_readiness_client(&missing, REVISION).is_err());
            let mut duplicate = baseline.clone();
            duplicate[index] = baseline[(index + 1) % baseline.len()].clone();
            assert!(validate_outbound_readiness_client(&duplicate, REVISION).is_err());
            for field in [
                "event",
                "case",
                "token",
                "revision",
                "qualification",
                "response_decoding",
                "result",
                "origin",
                "a_uri",
                "b_uri",
            ] {
                let mut probes = baseline.clone();
                probes[index].as_object_mut().unwrap().remove(field);
                assert!(
                    validate_outbound_readiness_client(&probes, REVISION).is_err(),
                    "{index}: {field}"
                );
            }
        }
        for (field, value) in [
            ("case", json!("other")),
            ("revision", json!("foreign")),
            ("token", json!("invalid token")),
            ("qualification", json!(true)),
        ] {
            let mut probes = baseline.clone();
            probes[0][field] = value;
            assert!(
                validate_outbound_readiness_client(&probes, REVISION).is_err(),
                "{field}"
            );
        }
        let mut duplicate = baseline.clone();
        duplicate.push(baseline[0].clone());
        assert!(validate_outbound_readiness_client(&duplicate, REVISION).is_err());
        let mut foreign = baseline.clone();
        foreign[0] = client_matrix()[0].clone();
        assert!(validate_outbound_readiness_client(&foreign, REVISION).is_err());

        let mut reused_token = baseline.clone();
        let token = baseline[0]["token"].as_str().unwrap();
        let path = format!("/p5/{token}/8002/pending");
        let final_path = format!("/p5/{token}/8002/final");
        let probe = &mut reused_token[1];
        probe["token"] = json!(token);
        probe["a_uri"] = json!(format!("http://127.0.0.1:8001{path}"));
        probe["b_uri"] = json!(format!("http://127.0.0.1:8002{final_path}"));
        probe["origin"]["a"] = json!([path, path]);
        probe["origin"]["b"] = json!([final_path]);
        probe["result"]["Ok"]["follow"]["final_uri"] = probe["b_uri"].clone();
        validate_p5_probe(probe, REVISION).unwrap();
        assert!(validate_outbound_readiness_client(&reused_token, REVISION).is_err());
    }

    #[test]
    fn p5_inventory_requires_explicit_null_fields() {
        for (case, parent, field) in [
            ("two-origin", "/result/Ok/follow", "stop_reason"),
            ("optimizer-send", "/result/Ok/failure", "hop"),
        ] {
            let mut probes = p5_matrix();
            probes
                .iter_mut()
                .find(|probe| probe["case"] == case)
                .unwrap()
                .pointer_mut(parent)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(
                validate_outbound_readiness_client(&probes, REVISION).is_err(),
                "{case}: {field}"
            );
        }
    }

    #[test]
    fn p5_inventory_rejects_unrelated_limit_causes() {
        let baseline = p5_matrix();
        validate_outbound_readiness_client(&baseline, REVISION).unwrap();
        for (index, probe) in baseline.iter().enumerate() {
            let case = probe["case"].as_str().unwrap();
            let pointers: &[&str] = if case == "pending-head" {
                &["/result/Ok/stop/response/reason"]
            } else if case.starts_with("parity/") && !case.ends_with("/ten") {
                &[
                    "/result/Ok/send/reason",
                    "/result/Ok/follow/response/reason",
                ]
            } else {
                continue;
            };
            let expected = probe.pointer(pointers[0]).unwrap().as_str().unwrap();
            for reason in [
                "BrotliWindow",
                "BufferedBody",
                "DecodedBody",
                "DecoderMemory",
                "EncodedBody",
                "HeaderBytes",
                "HeaderCount",
                "Unspecified",
            ] {
                if reason == expected {
                    continue;
                }
                let mut probes = baseline.clone();
                for pointer in pointers {
                    *probes[index].pointer_mut(pointer).unwrap() = json!(reason);
                }
                assert!(
                    validate_outbound_readiness_client(&probes, REVISION).is_err(),
                    "{case}: accepted {reason} instead of {expected}"
                );
            }
        }
    }

    #[test]
    fn p5_inventory_rejects_original_mode_evidence() {
        let baseline = p5_matrix();
        for index in 0..baseline.len() {
            let mut probes = baseline.clone();
            probes[index]["response_decoding"] = json!("Original");
            assert!(validate_outbound_readiness_client(&probes, REVISION).is_err());
        }
    }

    #[test]
    fn p5_overflow_requires_correlated_follow_terminal_metadata() {
        let baseline = p5_matrix();
        validate_outbound_readiness_client(&baseline, REVISION).unwrap();
        for (index, probe) in baseline.iter().enumerate() {
            let case = probe["case"].as_str().unwrap();
            if !case.starts_with("parity/") || case.ends_with("/ten") {
                continue;
            }
            let original = probe.pointer("/result/Ok/follow").unwrap();
            let mutations = if original.get("hop").is_some() {
                vec![
                    ("hop", None),
                    ("hop", Some(json!(99))),
                    ("hop", Some(Value::Null)),
                    ("final_uri", Some(probe["a_uri"].clone())),
                    ("redirects", Some(json!(0))),
                    ("stop_reason", Some(Value::Null)),
                ]
            } else {
                vec![
                    ("final_uri", None),
                    ("redirects", None),
                    ("stop_reason", None),
                    ("final_uri", Some(json!("http://127.0.0.1:8001/p5/foreign"))),
                    ("redirects", Some(json!(20))),
                    ("stop_reason", Some(json!("LimitReached"))),
                    ("hop", Some(json!(99))),
                ]
            };
            for (field, replacement) in mutations {
                let mut probes = baseline.clone();
                let follow = probes[index]
                    .pointer_mut("/result/Ok/follow")
                    .unwrap()
                    .as_object_mut()
                    .unwrap();
                if let Some(replacement) = replacement {
                    follow.insert(field.to_owned(), replacement);
                } else {
                    follow.remove(field);
                }
                assert!(
                    validate_outbound_readiness_client(&probes, REVISION).is_err(),
                    "{case}: accepted invalid {field}"
                );
            }
        }
    }

    #[test]
    fn p5_inventory_rejects_receipt_and_outcome_substitution() {
        let baseline = p5_matrix();
        for (case, pointer, replacement) in [
            ("two-origin", "/origin/a", json!([])),
            ("two-origin", "/origin/b", json!([])),
            ("two-origin", "/origin/b", json!(["foreign"])),
            ("two-origin", "/result/Ok/follow/redirects", json!(0)),
            ("two-origin", "/result/Ok/follow/policy_calls", json!(0)),
            ("two-origin", "/result/Ok/send/status", json!(200)),
            (
                "pending-head",
                "/origin/pending_released_before_reply",
                json!(true),
            ),
            ("pending-head", "/origin/pending_heads", json!(0)),
            (
                "pending-head",
                "/result/Ok/follow/response/streamed",
                json!(false),
            ),
            (
                "pending-head",
                "/result/Ok/stop/response/category",
                json!("bad_gateway"),
            ),
            ("optimizer-send", "/origin/b", json!(["unexpected"])),
            ("optimizer-initial", "/result/Ok/failure/hop", json!(1)),
            ("optimizer-next", "/origin/a", json!([])),
            ("optimizer-next", "/result/Ok/failure/hop", json!(0)),
            (
                "optimizer-next",
                "/result/Ok/failure/provider_optimizer_unsupported",
                json!(false),
            ),
            (
                "optimizer-next",
                "/result/Ok/failure/reason",
                json!("Unspecified"),
            ),
            ("optimizer-next", "/result/Ok/options_checked", json!(false)),
            (
                "parity/gzip/streamed/ten",
                "/result/Ok/follow/response/bytes",
                json!([]),
            ),
            (
                "parity/identity/buffered/eleven",
                "/result/Ok/send/category",
                json!("bad_gateway"),
            ),
        ] {
            let mut probes = baseline.clone();
            let probe = probes
                .iter_mut()
                .find(|probe| probe["case"] == case)
                .unwrap();
            *probe.pointer_mut(pointer).unwrap() = replacement;
            assert!(
                validate_outbound_readiness_client(&probes, REVISION).is_err(),
                "{case}: {pointer}"
            );
        }
    }

    #[test]
    fn readiness_client_inventory_is_twelve_unique_legs() {
        let legs = readiness_client_legs();
        assert_eq!(legs.len(), 12);
        assert_eq!(
            legs.iter()
                .map(ReadinessLeg::key)
                .collect::<HashSet<_>>()
                .len(),
            12
        );
        for repetition in 0..3 {
            assert_eq!(
                legs.iter()
                    .filter(|leg| leg.repetition == repetition)
                    .count(),
                4
            );
        }
        assert!(
            legs.iter()
                .all(|leg| leg.mode == "client" && leg.read_limit == 16_384)
        );
    }

    #[test]
    fn readiness_client_accepts_complete_correlated_typed_evidence() {
        let mut probes = client_matrix();
        for probe in &probes {
            validate_outbound_readiness_client_probe(probe, REVISION).unwrap();
        }
        validate_outbound_readiness_client(&probes, REVISION).unwrap();
        probes.reverse();
        validate_outbound_readiness_client(&probes, REVISION).unwrap();
    }

    #[test]
    fn readiness_client_rejects_missing_duplicate_and_foreign_evidence() {
        let baseline = client_matrix();
        let mut missing = baseline.clone();
        missing.pop();
        assert!(validate_outbound_readiness_client(&missing, REVISION).is_err());
        let mut duplicate = baseline.clone();
        duplicate[1] = duplicate[0].clone();
        assert!(validate_outbound_readiness_client(&duplicate, REVISION).is_err());
        let mut duplicate_token = baseline.clone();
        duplicate_token[1]["token"] = duplicate_token[0]["token"].clone();
        duplicate_token[1]["origin"]["token"] = duplicate_token[0]["token"].clone();
        for request in duplicate_token[1]["origin"]["requests"]
            .as_array_mut()
            .unwrap()
        {
            request["token"] = baseline[0]["token"].clone();
        }
        assert!(validate_outbound_readiness_client(&duplicate_token, REVISION).is_err());
        for (field, value) in [
            ("event", json!("readiness-probe")),
            ("case", json!("eof")),
            ("consumption", json!("streamed")),
            (
                "revision",
                json!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            ),
            ("qualification", json!(true)),
            ("repetition", json!(3)),
            ("token", json!("foreign-token")),
            ("token", json!("invalid token")),
        ] {
            let mut probes = baseline.clone();
            probes[0][field] = value;
            assert!(
                validate_outbound_readiness_client(&probes, REVISION).is_err(),
                "{field}"
            );
        }
        assert!(validate_outbound_readiness_client(&baseline, "not-a-revision").is_err());
        for field in [
            "event",
            "token",
            "case",
            "consumption",
            "revision",
            "qualification",
            "repetition",
            "completed_us",
            "origin",
            "result",
        ] {
            let mut probes = baseline.clone();
            probes[0].as_object_mut().unwrap().remove(field);
            assert!(
                validate_outbound_readiness_client(&probes, REVISION).is_err(),
                "missing {field}"
            );
        }
    }

    #[test]
    fn readiness_client_rejects_collapsed_or_untyped_source_errors() {
        for consumption in ["buffered", "streamed"] {
            for mutation in ["eof", "category", "source", "outer-error"] {
                let mut probes = client_matrix();
                let probe = probes
                    .iter_mut()
                    .find(|p| p["case"] == "full-body-error" && p["consumption"] == consumption)
                    .unwrap();
                match mutation {
                    "eof" => {
                        probe["result"]["Ok"]["kind"] = json!("eof");
                        probe["result"]["Ok"]["failure"] = Value::Null;
                    }
                    "category" => probe["result"]["Ok"]["failure"]["category"] = json!("internal"),
                    "source" => {
                        probe["result"]["Ok"]["failure"]["source_unexpected_eof"] = json!(false)
                    }
                    _ => {
                        probe["result"] =
                            json!({"Err":{"category":"bad_gateway", "source_unexpected_eof":true}})
                    }
                }
                assert!(
                    validate_outbound_readiness_client(&probes, REVISION).is_err(),
                    "{consumption}/{mutation}"
                );
            }
        }
    }

    #[test]
    fn readiness_client_rejects_wrong_bytes_checksums_and_error_prefixes() {
        let baseline = client_matrix();
        for index in 0..4 {
            for field in ["bytes", "checksum"] {
                let mut probes = baseline.clone();
                let result = &mut probes[index]["result"]["Ok"];
                let body = if index < 2 {
                    &mut result["slots"][1]
                } else {
                    result
                };
                body[field] = if field == "bytes" {
                    json!(123)
                } else {
                    json!("0000000000000000")
                };
                assert!(
                    validate_outbound_readiness_client(&probes, REVISION).is_err(),
                    "{index}/{field}"
                );
            }
        }
        for (consumption, prefix) in [
            ("buffered", payload("full-body-error", 0)),
            ("streamed", b""),
            ("streamed", b"x"),
        ] {
            let mut probes = baseline.clone();
            let result = &mut probes
                .iter_mut()
                .find(|p| p["case"] == "full-body-error" && p["consumption"] == consumption)
                .unwrap()["result"]["Ok"];
            result["bytes"] = json!(prefix.len());
            result["checksum"] = json!(fingerprint(prefix));
            assert!(
                validate_outbound_readiness_client(&probes, REVISION).is_err(),
                "{consumption}/{}",
                prefix.len()
            );
        }
    }

    #[test]
    fn readiness_client_rejects_late_or_reassociated_mixed_results() {
        let baseline = client_matrix();
        for case in ["mixed-head-body", "mixed-body-head"] {
            for mutation in [
                "equal",
                "after",
                "missing-elapsed",
                "termination",
                "slot-order",
                "slot-count",
            ] {
                let mut probes = baseline.clone();
                let probe = probes.iter_mut().find(|p| p["case"] == case).unwrap();
                let result = &mut probe["result"]["Ok"];
                match mutation {
                    "equal" => result["slots"][1]["elapsed_us"] = json!(500_000),
                    "after" => result["slots"][1]["elapsed_us"] = json!(500_001),
                    "missing-elapsed" => {
                        result["slots"][1]
                            .as_object_mut()
                            .unwrap()
                            .remove("elapsed_us");
                    }
                    "termination" => result["termination"] = json!("Complete"),
                    "slot-order" => result["slots"].as_array_mut().unwrap().swap(0, 1),
                    _ => {
                        result["slots"].as_array_mut().unwrap().pop();
                    }
                }
                assert!(
                    validate_outbound_readiness_client(&probes, REVISION).is_err(),
                    "{case}/{mutation}"
                );
            }
        }
    }

    #[test]
    fn readiness_client_rejects_invalid_origin_receipts_and_aggregates_failures() {
        let baseline = client_matrix();
        for mutation in ["accepted", "body", "delay", "token"] {
            let mut probes = baseline.clone();
            let probe = probes
                .iter_mut()
                .find(|p| p["case"] == "mixed-body-head")
                .unwrap();
            match mutation {
                "accepted" => probe["origin"]["accepted"] = json!(1),
                "body" => probe["origin"]["writes"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|w| w["kind"] != "body"),
                "delay" => probe["origin"]["writes"][1]["at_us"] = json!(1),
                _ => probe["origin"]["requests"][0]["token"] = json!("foreign-token"),
            }
            assert!(
                validate_outbound_readiness_client(&probes, REVISION).is_err(),
                "{mutation}"
            );
        }
        let mut probes = baseline;
        probes[0]["result"]["Ok"]["slots"][1]["bytes"] = json!(0);
        probes[1]["result"]["Ok"]["slots"][1]["bytes"] = json!(0);
        let error = validate_outbound_readiness_client(&probes, REVISION)
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with("2 actual-client leg(s) failed:\n"),
            "{error}"
        );
        assert!(
            error.contains("mixed-head-body/client/buffered/16384/0"),
            "{error}"
        );
        assert!(
            error.contains("mixed-body-head/client/buffered/16384/0"),
            "{error}"
        );
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
    fn readiness_progress_inventory_requires_mixed_phases_and_full_buffers() {
        for case in ["mixed-head-body", "mixed-body-head", "full-body-error"] {
            let legs: Vec<_> = readiness_progress_legs()
                .into_iter()
                .filter(|leg| leg.case == case)
                .collect();
            assert_eq!(legs.len(), 6, "{case} must run both consumers three times");
            assert!(
                legs.iter()
                    .all(|leg| leg.mode == "selected" && leg.read_limit == 16_384)
            );
        }
    }

    #[test]
    fn readiness_progress_accepts_complete_matrix_and_rejects_lost_evidence() {
        let baseline = matrix_for(readiness_progress_legs());
        assert_eq!(baseline.len(), 18);
        assert_eq!(
            baseline
                .iter()
                .map(|p| p["slots"].as_array().unwrap().len())
                .sum::<usize>(),
            30
        );
        validate_outbound_readiness_progress(&baseline, REVISION).unwrap();
        let mut missing = baseline.clone();
        missing.pop();
        assert!(validate_outbound_readiness_progress(&missing, REVISION).is_err());
        let mut collapsed = baseline.clone();
        let probe = collapsed
            .iter_mut()
            .find(|p| p["case"] == "full-body-error")
            .unwrap();
        let error = operation(probe, "finish");
        error["outcome"] = json!("eof");
        error.as_object_mut().unwrap().remove("error");
        assert!(validate_outbound_readiness_progress(&collapsed, REVISION).is_err());
        let mut filtered = baseline;
        let probe = filtered
            .iter_mut()
            .find(|p| p["case"] == "mixed-head-body")
            .unwrap();
        let selection = samples(probe)
            .iter_mut()
            .find(|s| {
                s["phases"]
                    .as_array()
                    .is_some_and(|p| p.contains(&json!("head")) && p.contains(&json!("body")))
            })
            .unwrap();
        selection["slots"] = json!([1]);
        selection["phases"] = json!(["body"]);
        selection["selected"] = json!(0);
        let error = validate_outbound_readiness_progress(&filtered, REVISION)
            .unwrap_err()
            .to_string();
        assert!(error.contains("wrong selection domain"), "{error}");
    }

    #[test]
    fn readiness_progress_rejects_only_partial_buffer_reads_even_with_correct_payload() {
        let mut probes = matrix_for(readiness_progress_legs());
        let probe = probes
            .iter_mut()
            .find(|p| p["case"] == "full-body-error")
            .unwrap();
        let mut leg = readiness_progress_legs()
            .into_iter()
            .find(|leg| leg.case == "full-body-error")
            .unwrap();
        // Keep every actual byte/checksum/selection intact, but no read now fills the buffer.
        leg.read_limit = 32_770;
        for read in samples(probe)
            .iter_mut()
            .filter(|s| s["operation"] == "read")
        {
            read["capacity"] = json!(leg.read_limit);
        }
        assert_eq!(
            trace(probe, &leg).unwrap_err().to_string(),
            "full-capacity read was not observed"
        );
    }

    #[test]
    fn readiness_progress_rejects_missing_or_reordered_origin_writes() {
        let baseline = matrix_for(readiness_progress_legs());
        validate_outbound_readiness_progress(&baseline, REVISION).unwrap();
        for mutation in ["delay", "head-order", "body-receipt", "pending-body"] {
            let mut probes = baseline.clone();
            let probe = probes
                .iter_mut()
                .find(|p| p["case"] == "mixed-body-head")
                .unwrap();
            let writes = probe["origin"]["writes"].as_array_mut().unwrap();
            match mutation {
                "delay" => {
                    writes
                        .iter_mut()
                        .find(|w| w["slot"] == 1 && w["kind"] == "head")
                        .unwrap()["at_us"] = json!(1)
                }
                "head-order" => writes.swap(0, 1),
                "body-receipt" => writes.retain(|w| w["kind"] != "body"),
                _ => writes.push(json!({"slot":0,"kind":"body","at_us":50_002,"bytes":1})),
            }
            assert!(
                validate_outbound_readiness_progress(&probes, REVISION).is_err(),
                "{mutation}"
            );
        }
    }

    #[test]
    fn readiness_progress_rejects_consistent_eof_collapse_and_reused_readiness() {
        let baseline = matrix_for(readiness_progress_legs());
        for terminal in [false, true] {
            let mut probes = baseline.clone();
            let probe = probes
                .iter_mut()
                .find(|p| p["case"] == "full-body-error")
                .unwrap();
            let events = samples(probe);
            let read = if terminal {
                events
                    .iter()
                    .position(|s| s["operation"] == "read" && s["error"] == "UnexpectedEof")
                    .unwrap()
            } else {
                events
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| {
                        s["operation"] == "read" && s["bytes"].as_u64().is_some_and(|b| b > 0)
                    })
                    .nth(1)
                    .unwrap()
                    .0
            };
            assert_eq!(events[read - 1]["operation"], "select");
            events.remove(read - 1);
            let error = validate_outbound_readiness_progress(&probes, REVISION)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("read without fresh same-slot readiness"),
                "{error}"
            );
        }
        let mut collapsed = baseline.clone();
        let probe = collapsed
            .iter_mut()
            .find(|p| p["case"] == "full-body-error")
            .unwrap();
        for event in samples(probe) {
            if event["operation"] == "read" && event["error"] == "UnexpectedEof" {
                event.as_object_mut().unwrap().remove("error");
                event["bytes"] = json!(0);
            } else if event["operation"] == "finish" {
                event.as_object_mut().unwrap().remove("error");
                event["outcome"] = json!("eof");
            }
        }
        probe["slots"][0].as_object_mut().unwrap().remove("error");
        probe["slots"][0]["outcome"] = json!("eof");
        let error = validate_outbound_readiness_progress(&collapsed, REVISION)
            .unwrap_err()
            .to_string();
        assert!(error.contains("wrong case terminal"), "{error}");

        let mut head_only = baseline;
        let probe = head_only
            .iter_mut()
            .find(|p| p["case"] == "mixed-head-body")
            .unwrap();
        let selection = samples(probe)
            .iter_mut()
            .find(|s| {
                s["phases"]
                    .as_array()
                    .is_some_and(|p| p.contains(&json!("head")) && p.contains(&json!("body")))
            })
            .unwrap();
        selection["slots"] = json!([0]);
        selection["phases"] = json!(["head"]);
        selection["selected"] = json!(0);
        let error = validate_outbound_readiness_progress(&head_only, REVISION)
            .unwrap_err()
            .to_string();
        assert!(error.contains("wrong selection domain"), "{error}");
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

        let path = crate::repository_root()
            .join("docs/audits/evidence/2026-10-23-fastly-readiness-matrix-probes.jsonl");
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
    fn readiness_replays_raw_instrumented_matrices_without_overwriting_history() {
        use std::{
            collections::BTreeMap,
            fs::File,
            io::{BufRead, BufReader},
        };
        let path = crate::repository_root()
            .join("docs/audits/evidence/2026-10-23-fastly-timer-controls-probes.jsonl");
        let mut runs: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for line in BufReader::new(File::open(path).unwrap()).lines() {
            let row: Value = serde_json::from_str(&line.unwrap()).unwrap();
            if row["kind"] == "body-probe" {
                runs.entry(row["run"].as_str().unwrap().to_owned())
                    .or_default()
                    .push(row["probe"].clone());
            }
        }
        assert_eq!(runs.len(), 4);
        for (name, probes) in &runs {
            assert_eq!(probes.len(), 135);
            assert_eq!(
                probes
                    .iter()
                    .map(|p| p["origin"]["accepted"].as_u64().unwrap())
                    .sum::<u64>(),
                147
            );
            for probe in probes {
                for sample in probe["samples"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|s| s["operation"] == "select")
                {
                    assert!(sample["abi_enter_us"].is_u64());
                    assert!(sample["abi_return_us"].is_u64());
                }
                assert_eq!(probe["qualification"], false);
            }
            let result =
                validate_outbound_readiness(probes, "77819f712ad971ada5494c563dd86a3b64a2afa6");
            if name.starts_with("repaired") {
                result.unwrap();
            } else {
                let message = result.unwrap_err().to_string();
                assert!(
                    message.starts_with("24 readiness leg(s) failed:\n"),
                    "{message}"
                );
                assert_eq!(message.matches("wrong case terminal").count(), 24);
            }
        }
    }

    #[test]
    fn readiness_replays_local_cleanup_overrun_without_hiding_failures() {
        use std::{
            fs::File,
            io::{BufRead, BufReader},
        };
        let path = crate::repository_root()
            .join("docs/audits/evidence/2026-10-24-fastly-local-recheck-probes.jsonl");
        let rows: Vec<Value> = BufReader::new(File::open(path).unwrap())
            .lines()
            .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
            .collect();
        assert_eq!(rows.len(), 414);
        let revision = "98a1d0e8c3ac3e0bd9e3f0986ae0390527d7a4a8";
        for (run, late_controls, failures) in [("repaired", 2, 1), ("stock", 3, 24)] {
            let probes: Vec<Value> = rows
                .iter()
                .filter(|row| row["run"] == run && row["kind"] == "body-probe")
                .map(|row| row["probe"].clone())
                .collect();
            let controls: Vec<Value> = rows
                .iter()
                .filter(|row| row["run"] == run && row["kind"] == "timer-control")
                .map(|row| row["probe"].clone())
                .collect();
            assert_eq!(probes.len(), 135);
            assert_eq!(controls.len(), 72);
            assert_eq!(
                probes
                    .iter()
                    .map(|p| p["origin"]["accepted"].as_u64().unwrap())
                    .sum::<u64>(),
                147
            );
            crate::evidence::validate_timer_controls(&controls, revision).unwrap();
            assert_eq!(
                controls
                    .iter()
                    .filter(|p| p["within_five_ms"] == false)
                    .count(),
                late_controls
            );
            let message = validate_outbound_readiness(&probes, revision)
                .unwrap_err()
                .to_string();
            assert!(message.starts_with(&format!("{failures} readiness leg(s) failed:\n")));
            if run == "stock" {
                assert_eq!(message.matches("wrong case terminal").count(), 24);
                continue;
            }
            assert!(
                message.contains("trickle/selected/buffered/16384/1: reordered or late operation")
            );
            let probe = probes
                .iter()
                .find(|p| {
                    p["case"] == "trickle"
                        && p["mode"] == "selected"
                        && p["consumption"] == "buffered"
                        && p["read_limit"] == 16384
                        && p["repetition"] == 1
                })
                .unwrap();
            let samples = probe["samples"].as_array().unwrap();
            let selected = samples
                .iter()
                .rev()
                .find(|s| s["operation"] == "select")
                .unwrap();
            let finish = samples.iter().find(|s| s["operation"] == "finish").unwrap();
            let bound =
                probe["deadline_us"].as_u64().unwrap() + probe["tolerance_us"].as_u64().unwrap();
            assert!(selected["abi_return_us"].as_u64().unwrap() <= bound);
            assert!(finish["start_us"].as_u64().unwrap() <= bound);
            assert!(finish["end_us"].as_u64().unwrap() > bound);
            assert!(probe["completed_us"].as_u64().unwrap() > bound);
        }
    }

    #[test]
    fn readiness_replays_mixed_and_full_buffer_runtime_comparisons() {
        use std::{
            collections::BTreeMap,
            fs::File,
            io::{BufRead, BufReader},
        };
        let path = crate::repository_root()
            .join("docs/audits/evidence/2026-10-25-fastly-mixed-readiness-probes.jsonl");
        let rows: Vec<Value> = BufReader::new(File::open(path).unwrap())
            .lines()
            .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
            .collect();
        assert_eq!(rows.len(), 675);
        let revision = "92daf00ab8bb76b2d4791e727e698e3304cc15ca";
        for (run, baseline_failures, progress_failures, losses) in [
            ("repaired-before-formatting", 0, 2, 0),
            ("stock-formatted", 25, 7, 30),
            ("repaired-formatted", 0, 0, 0),
        ] {
            let probes: Vec<_> = rows
                .iter()
                .filter(|r| r["run"] == run && r["kind"] == "body-probe")
                .map(|r| r["probe"].clone())
                .collect();
            let controls: Vec<_> = rows
                .iter()
                .filter(|r| r["run"] == run && r["kind"] == "timer-control")
                .map(|r| r["probe"].clone())
                .collect();
            assert_eq!(probes.len(), 153);
            assert_eq!(controls.len(), 72);
            assert_eq!(
                probes
                    .iter()
                    .map(|p| p["origin"]["accepted"].as_u64().unwrap())
                    .sum::<u64>(),
                177
            );
            crate::evidence::validate_timer_controls(&controls, revision).unwrap();
            let (baseline, progress) = probes.split_at(135);
            for (result, count) in [
                (
                    validate_outbound_readiness(baseline, revision),
                    baseline_failures,
                ),
                (
                    validate_outbound_readiness_progress(progress, revision),
                    progress_failures,
                ),
            ] {
                if count == 0 {
                    result.unwrap();
                } else {
                    assert!(
                        result
                            .unwrap_err()
                            .to_string()
                            .starts_with(&format!("{count} readiness leg(s) failed:\n"))
                    );
                }
            }
            let actual_losses = probes
                .iter()
                .filter(|p| {
                    (p["case"] == "truncated" && p["mode"] == "selected")
                        || p["case"] == "malformed-body"
                        || p["case"] == "full-body-error"
                })
                .filter(|p| p["slots"][0]["outcome"] != "error")
                .count();
            assert_eq!(actual_losses, losses, "{run}");
            let groups: BTreeMap<_, Vec<_>> =
                progress.iter().fold(BTreeMap::new(), |mut groups, p| {
                    groups
                        .entry(p["case"].as_str().unwrap())
                        .or_default()
                        .push(p);
                    groups
                });
            assert_eq!(groups.len(), 3);
            for (case, samples) in groups {
                assert_eq!(samples.len(), 6, "{run}/{case}");
                assert!(samples.iter().all(|p| {
                    p["samples"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|s| s["operation"] == "read" && s["bytes"] == 16_384)
                }));
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
            ("missing raw return timestamp", |p| {
                let selection = operation(&mut p[0], "select");
                selection["abi_enter_us"] = selection["start_us"].clone();
            }),
            ("reversed raw interval", |p| {
                let selection = operation(&mut p[0], "select");
                selection["abi_enter_us"] = selection["end_us"].clone();
                selection["abi_return_us"] = selection["start_us"].clone();
            }),
            ("raw return outside observed interval", |p| {
                let selection = operation(&mut p[0], "select");
                selection["abi_enter_us"] = selection["start_us"].clone();
                selection["abi_return_us"] = json!(selection["end_us"].as_u64().unwrap() + 1);
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

use serde_json::{Value, json};
use socket2::{Domain, Protocol, Socket, Type};

const CASES: [&str; 6] = ["reuse", "variants", "head", "body", "slot", "settlement"];
struct NonListeningTargets {
    _owners: [Socket; 2],
    ports: [u16; 2],
}

impl NonListeningTargets {
    fn bind() -> crate::evidence::Result<Self> {
        let bind = || -> crate::evidence::Result<(Socket, u16)> {
            let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
            socket.bind(&std::net::SocketAddr::from(([127, 0, 0, 1], 0)).into())?;
            let port = socket
                .local_addr()?
                .as_socket()
                .ok_or("missing loopback target")?
                .port();
            Ok((socket, port))
        };
        let (first, first_port) = bind()?;
        let (second, second_port) = bind()?;
        Ok(Self {
            _owners: [first, second],
            ports: [first_port, second_port],
        })
    }
}

fn need(valid: bool, reason: &'static str) -> Result<(), &'static str> {
    if valid { Ok(()) } else { Err(reason) }
}

fn number(value: &Value, name: &str) -> Result<u64, &'static str> {
    value[name].as_u64().ok_or("missing budget measurement")
}

fn native(value: &Value, body: bool) -> Result<(), &'static str> {
    let source = &value["native"];
    need(
        source["invalid"] == false
            && source["attachments"] == 1
            && source["preparations"] == 1
            && source["operations"] == 1
            && source["operation_releases"] == 1
            && number(source, "heads")? <= 1
            && number(source, "sources")? <= number(source, "heads")?
            && source["releases"] == source["sources"],
        "missing actual SDK operation/release",
    )?;
    if body {
        need(
            source["heads"] == 1
                && source["sources"] == 1
                && source["releases"] == 1
                && number(source, "ready_reads")? > 0
                && number(source, "waits")? > 0,
            "missing actual SDK body read/release",
        )?;
        let wait = &source["last_timeout"];
        need(
            wait["kind"] == "BodyReadiness"
                && wait["outcome"] == "TimedOut"
                && number(wait, "started_us")? < number(value, "deadline_us")?
                && number(wait, "observed_us")? >= number(value, "deadline_us")?
                && number(wait, "observed_us")? <= number(value, "finished_us")?,
            "SDK wait did not span the original body deadline",
        )?;
    }
    Ok(())
}

fn timed(value: &Value, timeout: u64, body: bool) -> Result<(), &'static str> {
    let entered = number(value, "entered_us")?;
    let acquired = number(value, "pending_acquired_us")?;
    let finished = number(value, "finished_us")?;
    let target = timeout.checked_mul(1000).ok_or("budget overflow")?;
    let elapsed = finished
        .checked_sub(entered)
        .ok_or("backwards invocation clock")?;
    need(
        entered <= acquired
            && acquired <= finished
            && value["timeout_ms"] == timeout
            && number(value, "deadline_us")?.checked_sub(entered) == Some(target)
            && value["budget_source"] == "PerCallTimeout"
            && value["status"] == 504
            && value["error_kind"] == "gateway_timeout"
            && value["timeout_source"] == "PerCallTimeout"
            && (target..=target + 5000).contains(&elapsed),
        "wrong timeout, original budget or strict interval",
    )?;
    native(value, body)
}

fn success(value: &Value) -> Result<(), &'static str> {
    need(
        value["status"] == 200
            && value.get("error_kind") == Some(&Value::Null)
            && value.get("timeout_source") == Some(&Value::Null)
            && value["bytes"] == json!(b"ok".as_slice())
            && number(value, "entered_us")? <= number(value, "pending_acquired_us")?
            && number(value, "pending_acquired_us")? <= number(value, "finished_us")?,
        "wrong real origin success",
    )?;
    native(value, false)
}

fn non_listening(value: &Value) -> Result<(), &'static str> {
    native(value, false)?;
    let entered = number(value, "entered_us")?;
    let acquired = number(value, "pending_acquired_us")?;
    let finished = number(value, "finished_us")?;
    let source = &value["native"];
    let poll = &source["first_operation_poll"];
    let started = number(poll, "started_us")?;
    let observed = number(poll, "observed_us")?;
    let released = number(source, "operation_released_us")?;
    let polls = number(source, "operation_polls")?;
    let ready = number(source, "ready_operation_polls")?;
    let deadline = number(value, "deadline_us")?;
    let outcome = match value["sdk_outcome"].as_str() {
        Some("Unreachable") => {
            value["status"] == 502
                && value["error_kind"] == "bad_gateway"
                && value["bad_gateway_reason"] == "Unreachable"
                && value.get("timeout_source") == Some(&Value::Null)
                && finished < deadline
                && ready == 1
        }
        Some("PerCallTimeout" | "ConnectTimeout") => {
            value["status"] == 504
                && value["error_kind"] == "gateway_timeout"
                && value.get("bad_gateway_reason") == Some(&Value::Null)
                && value["timeout_source"] == value["sdk_outcome"]
                && if value["sdk_outcome"] == "PerCallTimeout" {
                    finished >= deadline
                } else {
                    value["label"] == "connect"
                        && value["request"]["connect_timeout_ms"] == 250
                        && finished < deadline
                        && ready == 1
                }
        }
        _ => false,
    };
    need(
        outcome
            && value["bytes"] == json!([])
            && value["mode"] == "Buffered"
            && value["timeout_ms"] == 500
            && value["budget_source"] == "PerCallTimeout"
            && deadline.checked_sub(entered) == Some(500_000)
            && entered <= acquired
            && acquired <= started
            && started <= observed
            && observed <= released
            && released <= finished
            && polls > 0
            && ready <= 1
            && ((poll["pending"] == false && polls == 1 && ready == 1)
                || (poll["pending"] == true && polls > ready))
            && source["heads"] == 0
            && source["sources"] == 0
            && source["releases"] == 0
            && source["reads"] == 0
            && source["ready_reads"] == 0
            && source["waits"] == 0
            && source.get("last_timeout") == Some(&Value::Null),
        "variant lacks a typed non-listening SDK outcome and operation evidence",
    )
}

fn variant_targets(
    event: &Value,
    report: &Value,
    origin_ports: [u16; 2],
) -> Result<(), &'static str> {
    let ports: [u16; 2] = serde_json::from_value(event["variant_ports"].clone())
        .map_err(|_| "missing owned variant targets")?;
    need(
        event["target_policy"] == "bound-non-listening"
            && report["target_policy"] == event["target_policy"]
            && ports[0] != ports[1]
            && ports
                .iter()
                .all(|port| *port > 0 && !origin_ports.contains(port)),
        "invalid owned variant targets",
    )?;
    let token = report["token"].as_str().ok_or("missing budget token")?;
    for value in report["observations"]
        .as_array()
        .ok_or("missing budget observations")?
    {
        let label = value["label"].as_str().ok_or("missing variant label")?;
        let scheme = if label == "scheme" { "https" } else { "http" };
        let port = ports[usize::from(label == "port")];
        let target = format!("{scheme}://127.0.0.1:{port}/budget/{token}/variants/identity");
        let settings = json!({"target":target,"method":"GET","scheme":scheme,"host":"127.0.0.1",
            "port":port,"authority":if label=="host" {"fixture.invalid".to_owned()} else {format!("127.0.0.1:{port}")},
            "host_override":label=="host","connect_timeout_ms":if label=="connect" {Some(250)} else {None},
            "cert_host":if label=="scheme" {Some("127.0.0.1")} else {None},"sni_hostname":null});
        need(
            value["request"] == settings
                && value["native"]["preparation_target"] == target
                && value["native"]["preparation_mode"] == "Send",
            "variant request settings or native preparation target changed",
        )?;
    }
    Ok(())
}

fn validate_report(case: &str, report: &Value) -> Result<(), &'static str> {
    need(
        CASES.contains(&case) && report["case"] == case && report["qualification"] == false,
        "unknown or missing budget case",
    )?;
    let values = report["observations"]
        .as_array()
        .ok_or("missing budget observations")?;
    need(values.len() <= 6, "unbounded budget observations")?;
    match case {
        "head" => {
            need(
                values.len() == 1 && values[0]["label"] == "head",
                "missing head probe",
            )?;
            timed(&values[0], 100, false)
        }
        "body" => {
            need(values.len() == 2, "missing body mode")?;
            for (value, label) in values.iter().zip(["buffered", "streamed"]) {
                need(
                    value["label"] == label
                        && value["mode"]
                            == if label == "buffered" {
                                "Buffered"
                            } else {
                                "Streamed"
                            },
                    "wrong body mode",
                )?;
                timed(value, 500, true)?;
            }
            Ok(())
        }
        "slot" => {
            need(
                values.len() == 3 && report["termination"] == "Completed",
                "incomplete batch",
            )?;
            need(
                values[0]["label"] == "silent" && values[0]["index"] == 0,
                "silent slot identity lost",
            )?;
            timed(&values[0], 200, false)?;
            need(
                (200_000..=205_000).contains(&number(&values[0], "arrival_elapsed_us")?),
                "silent arrival stamped at collection",
            )?;
            for (index, value) in values.iter().enumerate().skip(1) {
                need(
                    value["index"] == index && value["budget_source"] == "BatchCutoff",
                    "healthy slot identity/budget lost",
                )?;
                success(value)?;
            }
            need(
                number(report, "cutoff_us")?.checked_sub(number(report, "cutoff_anchor_us")?)
                    == Some(1_000_000),
                "batch cutoff changed",
            )
        }
        "settlement" => {
            need(
                values.len() == 1 && values[0]["timeout_ms"] == 1000,
                "wrong settlement budget",
            )?;
            let entered = number(&values[0], "entered_us")?;
            let settled = number(report, "settled_us")?;
            need(
                settled.checked_sub(entered).is_some_and(|v| v >= 26_000)
                    && settled <= number(&values[0], "pending_acquired_us")?
                    && number(&values[0], "deadline_us")?.checked_sub(entered) == Some(1_000_000),
                "settlement did not consume the original budget",
            )?;
            success(&values[0])
        }
        "reuse" => {
            let batches = report["batches"].as_array().ok_or("missing two batches")?;
            let stats = report["stats"]
                .as_array()
                .ok_or("missing callback observation")?;
            need(
                values.len() == 4 && batches.len() == 2 && stats.len() == 2,
                "incomplete reuse matrix",
            )?;
            for (index, remaining) in [640_000, 655_000].into_iter().enumerate() {
                let batch = &batches[index];
                need(
                    batch["termination"] == "Completed"
                        && number(batch, "cutoff_us")?
                            .checked_sub(number(batch, "cutoff_anchor_us")?)
                            == Some(remaining)
                        && stats[index]["configurations"] == 1
                        && stats[index]["registrations"] == 1,
                    "backend cache or aggregate cutoff changed",
                )?;
                for value in &values[index * 2..index * 2 + 2] {
                    need(
                        value["timeout_ms"] == 800
                            && value["budget_source"] == "BatchCutoff"
                            && value["deadline_us"] == batch["cutoff_us"],
                        "batch renewed the original cutoff",
                    )?;
                    success(value)?;
                }
            }
            Ok(())
        }
        "variants" => {
            let stats = report["stats"]
                .as_array()
                .ok_or("missing variant observations")?;
            need(
                values.len() == 5 && stats.len() == 5,
                "incomplete configuration variants",
            )?;
            for (index, label) in ["baseline", "scheme", "port", "host", "connect"]
                .into_iter()
                .enumerate()
            {
                need(
                    values[index]["label"] == label
                        && stats[index]["configurations"] == index + 1
                        && stats[index]["registrations"] == index + 1
                        && number(&stats[index], "max_registration_us")?
                            <= number(&stats[index], "total_registration_us")?
                        && (index == 0
                            || number(&values[index - 1], "finished_us")?
                                <= number(&values[index], "entered_us")?),
                    "variant reused a different backend configuration",
                )?;
                non_listening(&values[index])?;
            }
            Ok(())
        }
        _ => Err("unknown budget case"),
    }
}

fn expected_labels(case: &str) -> &'static [&'static str] {
    match case {
        "reuse" => &["a", "b", "a", "b"],
        "variants" => &[],
        "head" => &["head"],
        "body" => &["buffered", "streamed"],
        "slot" => &["silent", "a", "b"],
        "settlement" => &["settlement"],
        _ => &[],
    }
}

fn validate_case(event: &Value, ports: [u16; 2]) -> Result<(), &'static str> {
    let case = event["case"].as_str().ok_or("missing budget case")?;
    let token = event["token"].as_str().ok_or("missing budget token")?;
    need(
        !token.is_empty()
            && token.len() <= 64
            && token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "invalid budget token",
    )?;
    need(
        event["adapter"] == "fastly"
            && event["qualification"] == false
            && event["status"] == "pass"
            && event.get("error") == Some(&Value::Null)
            && ["client_error", "receipt_error", "report_error"]
                .iter()
                .all(|field| event.get(*field) == Some(&Value::Null)),
        "failed or partial budget case",
    )?;
    let client = &event["client"];
    let bytes: Vec<u8> = serde_json::from_value(client["body_bytes"].clone())
        .map_err(|_| "invalid budget response body")?;
    let report: Value =
        serde_json::from_slice(&bytes).map_err(|_| "invalid budget response JSON")?;
    need(
        client["status"] == 200 && report == event["report"] && report["token"] == token,
        "budget client and guest report disagree",
    )?;
    validate_report(case, &report)?;
    if case == "variants" {
        variant_targets(event, &report, ports)?;
    }
    let receipts = event["receipts"]
        .as_array()
        .ok_or("missing budget origin receipts")?;
    let expected = expected_labels(case);
    need(
        receipts.len() == expected.len(),
        "missing budget origin contact",
    )?;
    let mut labels = Vec::new();
    for receipt in receipts {
        let label = receipt["label"].as_str().ok_or("missing origin label")?;
        need(
            receipt["event"] == "budget-origin"
                && receipt["case"] == case
                && receipt["token"] == token
                && receipt["method"] == "GET"
                && receipt["target"] == format!("/budget/{token}/{case}/{label}")
                && receipt["port"] == ports[usize::from(case == "variants" && label == "port")]
                && receipt.get("error") == Some(&Value::Null)
                && matches!(
                    receipt["terminal"].as_str(),
                    Some("complete" | "peer-closed")
                ),
            "foreign, failed or stopped budget origin",
        )?;
        let chunks = receipt["chunks"]
            .as_array()
            .ok_or("missing origin chunks")?;
        if case == "slot" && label == "silent" {
            need(
                receipt.get("head_attempt_us") == Some(&Value::Null) && chunks.is_empty(),
                "silent origin wrote a response",
            )?;
        } else if case == "head" {
            need(
                number(receipt, "head_attempt_us")? >= 500_000,
                "slow head was not delayed 500ms",
            )?;
        } else if case == "body" {
            let head = number(receipt, "head_complete_us")?;
            need(
                !chunks.is_empty() && chunks.len() <= 3 && chunks[0]["written"] == true,
                "trickling origin never wrote a body byte",
            )?;
            for (index, chunk) in chunks.iter().enumerate() {
                let started = number(chunk, "started_us")?;
                need(
                    chunk["index"] == index
                        && started >= head + 300_000 * (index as u64 + 1)
                        && number(chunk, "completed_us")? >= started,
                    "origin trickle schedule changed",
                )?;
            }
            need(
                number(&chunks[0], "completed_us")? < head + 500_000,
                "origin did not progress before whole-body deadline",
            )?;
        } else {
            need(
                receipt["terminal"] == "complete" && receipt["head_complete_us"].as_u64().is_some(),
                "healthy origin did not finish",
            )?;
        }
        labels.push(label);
    }
    labels.sort_unstable();
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    need(
        labels == expected,
        "missing, repeated or substituted budget origin",
    )?;
    if case == "variants" {
        need(
            report["tls_policy"] == "fixed-strict"
                && report["configurable_tls_deferred"] == true
                && report.get("tls_variant").is_none()
                && report.get("unverified_reason").is_none(),
            "forged or missing fixed-strict TLS baseline",
        )?;
    }
    Ok(())
}

pub(super) fn validate_artifact(events: &[Value], adapter: &str) -> crate::evidence::Result<()> {
    use crate::evidence::require;
    require(
        adapter == "fastly",
        "Fastly budget evidence cannot qualify another adapter",
    )?;
    let cleanup = events
        .iter()
        .filter(|event| event["event"] == "fastly-budget-cleanup")
        .collect::<Vec<_>>();
    require(
        cleanup.len() == 1
            && cleanup[0]["adapter"] == adapter
            && cleanup[0]["qualification"] == false
            && cleanup[0]["status"] == "pass"
            && cleanup[0].get("error") == Some(&Value::Null),
        "missing or failed budget origin cleanup",
    )?;
    let ports: [u16; 2] = serde_json::from_value(cleanup[0]["ports"].clone())?;
    require(
        ports.iter().all(|port| *port > 0) && ports[0] != ports[1],
        "invalid budget origin identities",
    )?;
    let cases = events
        .iter()
        .filter(|event| event["event"] == "fastly-budget-case")
        .collect::<Vec<_>>();
    require(
        cases.len() == CASES.len(),
        "incomplete budget case inventory",
    )?;
    let mut tokens = std::collections::HashSet::new();
    for (event, case) in cases.iter().zip(CASES) {
        require(
            event["case"] == case,
            "budget cases reordered or duplicated",
        )?;
        validate_case(event, ports)?;
        require(
            tokens.insert(event["token"].as_str().ok_or("missing budget token")?),
            "duplicate budget token",
        )?;
    }
    let receipt_count = cases
        .iter()
        .map(|event| event["receipts"].as_array().map_or(0, Vec::len))
        .sum::<usize>();
    let inventory = events
        .iter()
        .filter(|event| event["event"] == "fastly-budget-inventory")
        .collect::<Vec<_>>();
    require(
        inventory.len() == 1
            && inventory[0]["adapter"] == adapter
            && inventory[0]["qualification"] == false
            && inventory[0]["status"] == "pass"
            && inventory[0]["cases"] == CASES.len()
            && inventory[0]["origin_requests"] == receipt_count
            && inventory[0]["ports"] == cleanup[0]["ports"],
        "missing or inconsistent six-case budget inventory",
    )?;
    require(
        cleanup[0]["accepted"] == receipt_count,
        "unaccounted budget origin connection",
    )?;
    Ok(())
}

fn retain_case_result(
    result: crate::evidence::Result<()>,
    first_hard: &mut Option<Box<dyn std::error::Error + Send + Sync>>,
) -> (&'static str, Option<String>) {
    match result {
        Ok(()) => ("pass", None),
        Err(error) => {
            let message = error.to_string();
            if first_hard.is_none() {
                *first_hard = Some(error);
            }
            ("fail", Some(message))
        }
    }
}

pub(super) fn run(
    adapter: &str,
    port: u16,
    records: &mut crate::Records,
) -> crate::evidence::Result<usize> {
    use crate::{
        evidence::{Result, require},
        net::{self, WireOrigins},
    };
    require(adapter == "fastly", "budget fixture requires Fastly")?;
    let first = records.values.len();
    let origins = WireOrigins::start()?;
    let ports = [origins.port(0), origins.port(1)];
    let mut first_hard = None;
    let execution = (|| -> Result<()> {
        for case in CASES {
            let token = crate::token();
            let non_listening = (case == "variants")
                .then(NonListeningTargets::bind)
                .transpose()?;
            let targets = non_listening
                .as_ref()
                .map_or(ports, |targets| targets.ports);
            let base = format!("http://127.0.0.1:{}/budget/{token}/{case}", targets[0]);
            let response = net::header_request(
                port,
                &format!("/progress/budgets/{case}"),
                &token,
                &[
                    ("x-fixture-backend", &base),
                    ("x-budget-second-port", &targets[1].to_string()),
                ],
                None,
            );
            let mut receipts = Vec::new();
            let mut receipt_error = None;
            for _ in expected_labels(case) {
                match origins.take_wire() {
                    Ok(receipt) => receipts.push(receipt),
                    Err(error) => {
                        receipt_error = Some(error);
                        break;
                    }
                }
            }
            let parsed: Result<Value> = match &response {
                Ok(client) => serde_json::from_value::<Vec<u8>>(client["body_bytes"].clone())
                    .map_err(Into::into)
                    .and_then(|bytes| Ok(serde_json::from_slice(&bytes)?)),
                Err(_) => Ok(Value::Null),
            };
            let mut event = json!({"event":"fastly-budget-case","adapter":adapter,"case":case,"token":token,
                "qualification":false,"status":"pass","error":null,"client":response.as_ref().ok(),
                "partial_client":response.as_ref().err().and_then(|error|error.downcast_ref::<net::InterruptedResponse>())
                    .map(|partial|json!({"status":partial.status,"headers":partial.headers,"body_bytes":partial.body,"cause":format!("{:?}",partial.cause)})),
                "client_error":response.as_ref().err().map(ToString::to_string),
                "receipts":receipts,"receipt_error":receipt_error.as_ref().map(ToString::to_string),
                "report":parsed.as_ref().ok(),"report_error":parsed.as_ref().err().map(ToString::to_string)});
            if case == "variants" {
                event["variant_ports"] = json!(targets);
                event["target_policy"] = json!("bound-non-listening");
            }
            let checked = response
                .map(|_| ())
                .and_then(|()| receipt_error.map_or(Ok(()), Err))
                .and_then(|()| parsed.map(|_| ()))
                .and_then(|()| validate_case(&event, ports).map_err(Into::into));
            let (status, error) = retain_case_result(checked, &mut first_hard);
            event["status"] = json!(status);
            event["error"] = json!(error);
            records.push(event)?;
            drop(non_listening);
        }
        Ok(())
    })();
    let cleanup = origins.finish_counted();
    records.push(
        json!({"event":"fastly-budget-cleanup","adapter":adapter,"ports":ports,
        "accepted":cleanup.as_ref().ok(),"status":if cleanup.is_ok(){"pass"}else{"fail"},
        "error":cleanup.as_ref().err().map(ToString::to_string),"qualification":false}),
    )?;
    retain_case_result(execution, &mut first_hard);
    retain_case_result(cleanup.map(|_| ()), &mut first_hard);
    records.push(json!({"event":"fastly-budget-inventory","adapter":adapter,
        "cases":CASES.len(),"origin_requests":CASES.iter().map(|case|expected_labels(case).len()).sum::<usize>(),
        "ports":ports,"status":"pass","qualification":false}))?;
    let persisted = match super::super::ci::read_events(&records.path) {
        Ok(events) => events,
        Err(error) => {
            retain_case_result(Err(error), &mut first_hard);
            Vec::new()
        }
    };
    let checked = persisted
        .get(first..)
        .ok_or_else(|| "truncated budget artifact".into())
        .and_then(|events| validate_artifact(events, adapter));
    retain_case_result(checked, &mut first_hard);
    if let Some(error) = first_hard {
        return Err(error);
    }
    Ok(CASES.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_budget_probes_keep_the_existing_qualification_owner() {
        let automatic = include_str!("../wire.rs")
            .split("fn progress_wire(")
            .nth(1)
            .expect("automatic wire owner")
            .split("pub(super) fn progress_suite")
            .next()
            .expect("wire owner boundary");
        assert!(!automatic.contains("budgets::run("));
        assert!(!automatic.contains("qualify_budgets("));
        let qualification = include_str!("../lifetimes.rs")
            .split("fn checks(")
            .nth(1)
            .expect("lifetime qualification owner")
            .split("\nfn ")
            .next()
            .expect("qualification owner boundary");
        let run = qualification
            .find("wire::qualify_budgets(")
            .expect("strict budget group remains reachable");
        let aggregate = qualification
            .rfind("require(failures.is_empty()")
            .expect("all case failures are retained");
        assert!(run < aggregate);
    }

    fn observation(label: &str, timeout: u64, elapsed: u64) -> Value {
        json!({"label":label,"entered_us":0,"pending_acquired_us":0,"finished_us":elapsed,
            "deadline_us":timeout*1000,"timeout_ms":timeout,"budget_source":"PerCallTimeout",
            "status":504,"error_kind":"gateway_timeout","timeout_source":"PerCallTimeout",
            "bytes":[],"mode":"Buffered","native":{"invalid":false,"attachments":1,
            "preparations":1,"operations":1,"operation_releases":1,"heads":0,"sources":0,
            "releases":0,"ready_reads":0,"waits":0,"last_timeout":null}})
    }

    fn healthy(label: &str, timeout: u64, deadline: u64) -> Value {
        let mut value = observation(label, timeout, 1000);
        value["status"] = json!(200);
        value["error_kind"] = Value::Null;
        value["timeout_source"] = Value::Null;
        value["bytes"] = json!(b"ok".as_slice());
        value["budget_source"] = json!("BatchCutoff");
        value["deadline_us"] = json!(deadline);
        value
    }

    fn complete_report(case: &str) -> Value {
        let mut value = report(case);
        match case {
            "reuse" => {
                value["observations"] = json!([
                    healthy("a", 800, 640_000),
                    healthy("b", 800, 640_000),
                    healthy("a", 800, 655_000),
                    healthy("b", 800, 655_000)
                ]);
                value["batches"] = json!([{"cutoff_anchor_us":0,"cutoff_us":640_000,"termination":"Completed"},
                    {"cutoff_anchor_us":0,"cutoff_us":655_000,"termination":"Completed"}]);
                value["stats"] = json!([{"configurations":1,"registrations":1},{"configurations":1,"registrations":1}]);
            }
            "slot" => {
                let mut values = vec![
                    observation("silent", 200, 200_000),
                    healthy("a", 1000, 1_000_000),
                    healthy("b", 1000, 1_000_000),
                ];
                for (index, value) in values.iter_mut().enumerate() {
                    value["index"] = json!(index);
                }
                values[0]["arrival_elapsed_us"] = json!(200_000);
                value["observations"] = json!(values);
                value["termination"] = json!("Completed");
                value["cutoff_anchor_us"] = json!(0);
                value["cutoff_us"] = json!(1_000_000);
            }
            "settlement" => {
                let mut result = healthy("settlement", 1000, 1_000_000);
                result["pending_acquired_us"] = json!(26_000);
                result["finished_us"] = json!(27_000);
                result["budget_source"] = json!("PerCallTimeout");
                value["observations"] = json!([result]);
                value["settled_us"] = json!(26_000);
            }
            _ => {}
        }
        value
    }

    fn report(case: &str) -> Value {
        let observations = match case {
            "head" => vec![observation("head", 100, 100_000)],
            "body" => ["buffered", "streamed"].into_iter().map(|label| {
                let mut value = observation(label, 500, 500_000);
                value["mode"] = json!(if label == "buffered" {"Buffered"} else {"Streamed"});
                value["native"]["heads"] = json!(1);
                value["native"]["sources"] = json!(1);
                value["native"]["releases"] = json!(1);
                value["native"]["ready_reads"] = json!(1);
                value["native"]["waits"] = json!(2);
                value["native"]["last_timeout"] = json!({"kind":"BodyReadiness","outcome":"TimedOut","started_us":300_000,"observed_us":500_000});
                value
            }).collect(),
            _ => Vec::new(),
        };
        json!({"case":case,"token":"owned","qualification":false,"observations":observations,
            "stats":[],"batches":[],"settled_us":null})
    }

    fn variant_report() -> Value {
        let mut report = report("variants");
        report["target_policy"] = json!("bound-non-listening");
        report["tls_policy"] = json!("fixed-strict");
        report["configurable_tls_deferred"] = json!(true);
        for (index, label) in ["baseline", "scheme", "port", "host", "connect"]
            .into_iter()
            .enumerate()
        {
            let scheme = if label == "scheme" { "https" } else { "http" };
            let port = if label == "port" { 4102 } else { 4101 };
            let target = format!("{scheme}://127.0.0.1:{port}/budget/owned/variants/identity");
            let mut value = observation(label, 500, index as u64 * 600_000 + 100);
            value["entered_us"] = json!(index * 600_000);
            value["deadline_us"] = json!(index * 600_000 + 500_000);
            value["pending_acquired_us"] = json!(index * 600_000 + 10);
            value["status"] = json!(502);
            value["error_kind"] = json!("bad_gateway");
            value["bad_gateway_reason"] = json!("Unreachable");
            value["sdk_outcome"] = json!("Unreachable");
            value["timeout_source"] = Value::Null;
            value["request"] = json!({"target":target,"method":"GET","scheme":scheme,
                "host":"127.0.0.1","port":port,
                "authority":if label=="host" {"fixture.invalid".to_owned()} else {format!("127.0.0.1:{port}")},
                "host_override":label=="host","connect_timeout_ms":if label=="connect" {Some(250)} else {None},
                "cert_host":if label=="scheme" {Some("127.0.0.1")} else {None},"sni_hostname":null});
            let native = &mut value["native"];
            native["preparation_target"] = json!(target);
            native["preparation_mode"] = json!("Send");
            native["operation_polls"] = json!(1);
            native["ready_operation_polls"] = json!(1);
            native["first_operation_poll"] = json!({"pending":false,
                "started_us":index*600_000+20,"observed_us":index*600_000+30});
            native["operation_released_us"] = json!(index * 600_000 + 40);
            native["reads"] = json!(0);
            report["observations"].as_array_mut().unwrap().push(value);
        }
        report["stats"] = json!(
            (1..=5)
                .map(|count| json!({"configurations":count,
            "registrations":count,"total_registration_us":count*10,"max_registration_us":10}))
                .collect::<Vec<_>>()
        );
        report
    }

    fn variant_event() -> Value {
        let report = variant_report();
        json!({"event":"fastly-budget-case","adapter":"fastly","case":"variants","token":"owned",
            "qualification":false,"status":"pass","error":null,"client_error":null,"receipt_error":null,"report_error":null,
            "client":{"status":200,"body_bytes":serde_json::to_vec(&report).unwrap()},"report":report,
            "variant_ports":[4101,4102],"target_policy":"bound-non-listening","receipts":[]})
    }

    #[test]
    fn fastly_budget_tls_baseline_is_complete_without_configurable_tls_claims() {
        assert_eq!(validate_case(&variant_event(), [4001, 4002]), Ok(()));
    }

    #[test]
    fn fastly_budget_tls_baseline_rejects_missing_or_forged_policy_and_getters() {
        for field in ["tls_policy", "configurable_tls_deferred"] {
            let mut event = variant_event();
            event["report"].as_object_mut().unwrap().remove(field);
            event["client"]["body_bytes"] = json!(serde_json::to_vec(&event["report"]).unwrap());
            assert!(validate_case(&event, [4001, 4002]).is_err(), "{field}");
        }
        for (field, value) in [
            ("tls_policy", json!("chain-only")),
            ("configurable_tls_deferred", json!(false)),
            ("tls_variant", json!(true)),
            ("tls_variant", json!(false)),
            ("unverified_reason", json!("legacy partial")),
        ] {
            let mut event = variant_event();
            event["report"][field] = value;
            event["client"]["body_bytes"] = json!(serde_json::to_vec(&event["report"]).unwrap());
            assert!(validate_case(&event, [4001, 4002]).is_err(), "{field}");
        }
        for index in 0..5 {
            for field in ["cert_host", "sni_hostname"] {
                let mut event = variant_event();
                event["report"]["observations"][index]["request"]
                    .as_object_mut()
                    .unwrap()
                    .remove(field);
                event["client"]["body_bytes"] =
                    json!(serde_json::to_vec(&event["report"]).unwrap());
                assert!(
                    validate_case(&event, [4001, 4002]).is_err(),
                    "{index} {field}"
                );
            }
        }
    }

    fn variant_timeout(value: &mut Value, cause: &str, ready: u64) {
        value["status"] = json!(504);
        value["error_kind"] = json!("gateway_timeout");
        value["bad_gateway_reason"] = Value::Null;
        value["timeout_source"] = json!(cause);
        value["sdk_outcome"] = json!(cause);
        let finished = if cause == "PerCallTimeout" {
            value["deadline_us"].clone()
        } else {
            json!(value["entered_us"].as_u64().unwrap() + 250_000)
        };
        value["finished_us"] = finished.clone();
        value["native"]["operation_released_us"] = finished;
        value["native"]["ready_operation_polls"] = json!(ready);
        value["native"]["first_operation_poll"]["pending"] = json!(ready == 0);
    }

    #[test]
    fn fastly_budget_non_listening_accepts_only_typed_portable_outcomes() {
        for (index, cause, ready) in [
            (0, "PerCallTimeout", 0),
            (1, "PerCallTimeout", 1),
            (4, "ConnectTimeout", 1),
        ] {
            let mut event = variant_event();
            variant_timeout(&mut event["report"]["observations"][index], cause, ready);
            event["client"]["body_bytes"] = json!(serde_json::to_vec(&event["report"]).unwrap());
            assert_eq!(
                validate_case(&event, [4001, 4002]),
                Ok(()),
                "{cause} {ready}"
            );
        }
        let mut event = variant_event();
        for value in event["report"]["observations"].as_array_mut().unwrap() {
            variant_timeout(value, "PerCallTimeout", 0);
        }
        event["report"]["observations"][1]["native"]["operation_polls"] = json!(2);
        event["report"]["observations"][1]["native"]["ready_operation_polls"] = json!(1);
        event["client"]["body_bytes"] = json!(serde_json::to_vec(&event["report"]).unwrap());
        assert_eq!(validate_case(&event, [4001, 4002]), Ok(()));
    }

    #[test]
    fn fastly_budget_non_listening_rejects_wrong_expiry_phase_and_poll_evidence() {
        for (index, cause, ready, field, replacement) in [
            (0, "PerCallTimeout", 0, "finished_us", json!(499_999)),
            (0, "ConnectTimeout", 1, "finished_us", json!(250_000)),
            (4, "ConnectTimeout", 0, "finished_us", json!(2_650_000)),
            (4, "ConnectTimeout", 1, "finished_us", json!(2_900_000)),
            (0, "Unspecified", 1, "finished_us", json!(500_000)),
            (0, "PerCallTimeout", 0, "sdk_outcome", json!("Unreachable")),
        ] {
            let mut event = variant_event();
            let value = &mut event["report"]["observations"][index];
            variant_timeout(value, cause, ready);
            value[field] = replacement;
            if field == "finished_us" {
                value["native"]["operation_released_us"] = value["finished_us"].clone();
            }
            event["client"]["body_bytes"] = json!(serde_json::to_vec(&event["report"]).unwrap());
            assert!(
                validate_case(&event, [4001, 4002]).is_err(),
                "{index} {cause} {ready} {field}"
            );
        }
        let mut event = variant_event();
        event["report"]["observations"][0]["finished_us"] = json!(500_000);
        event["client"]["body_bytes"] = json!(serde_json::to_vec(&event["report"]).unwrap());
        assert!(validate_case(&event, [4001, 4002]).is_err());
        let mut event = variant_event();
        variant_timeout(&mut event["report"]["observations"][4], "ConnectTimeout", 1);
        event["report"]["observations"][4]["request"]["connect_timeout_ms"] = Value::Null;
        event["client"]["body_bytes"] = json!(serde_json::to_vec(&event["report"]).unwrap());
        assert!(validate_case(&event, [4001, 4002]).is_err());
    }

    #[test]
    fn fastly_budget_rejects_obsolete_persisted_partial_coverage() {
        let mut events = baseline_artifact();
        events[1]["status"] = json!("unverified");
        events[1]["error"] = json!(
            "independent TLS settings are not expressible through the current public request API"
        );
        events[1]["report"]["tls_variant"] = json!(false);
        events[1]["report"]["unverified_reason"] = events[1]["error"].clone();
        events[1]["client"]["body_bytes"] =
            json!(serde_json::to_vec(&events[1]["report"]).unwrap());
        assert!(validate_artifact(&events, "fastly").is_err());
    }

    #[test]
    fn fastly_budget_aggregation_preserves_first_hard_error_in_both_success_orders() {
        for success_first in [false, true] {
            let mut first_hard = None;
            if success_first {
                assert_eq!(retain_case_result(Ok(()), &mut first_hard).0, "pass");
            }
            let hard = std::io::Error::from(std::io::ErrorKind::InvalidData);
            assert_eq!(
                retain_case_result(Err(hard.into()), &mut first_hard).0,
                "fail"
            );
            if !success_first {
                retain_case_result(Ok(()), &mut first_hard);
            }
            retain_case_result(
                Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe).into()),
                &mut first_hard,
            );
            assert_eq!(
                first_hard
                    .unwrap()
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .kind(),
                std::io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn fastly_budget_aggregation_keeps_cleanup_failure_after_valid_baseline() {
        let mut first_hard = None;
        retain_case_result(Ok(()), &mut first_hard);
        let cleanup = Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe).into());
        assert_eq!(retain_case_result(cleanup, &mut first_hard).0, "fail");
        let events = baseline_artifact();
        retain_case_result(validate_artifact(&events, "fastly"), &mut first_hard);
        assert_eq!(
            first_hard
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::BrokenPipe
        );
    }

    fn baseline_artifact() -> Vec<Value> {
        let mut events = Vec::new();
        for case in CASES {
            if case == "variants" {
                events.push(variant_event());
                continue;
            }
            let mut report = complete_report(case);
            report["token"] = json!(case);
            let receipts = expected_labels(case).iter().map(|label| {
                let silent = case == "slot" && *label == "silent";
                json!({"event":"budget-origin","case":case,"token":case,"label":label,"method":"GET",
                    "target":format!("/budget/{case}/{case}/{label}"),"port":4001,"error":null,
                    "terminal":if case=="head" {"peer-closed"} else {"complete"},
                    "head_attempt_us":if silent {None} else {Some(if case=="head" {500_000} else {0})},
                    "head_complete_us":if silent || case=="head" {None} else {Some(0)},
                    "chunks":if case=="body" {vec![json!({"index":0,"started_us":300_000,"completed_us":300_001,"written":true})]} else {vec![]}})
            }).collect::<Vec<_>>();
            events.push(json!({"event":"fastly-budget-case","adapter":"fastly","case":case,"token":case,
                "qualification":false,"status":"pass","error":null,"client_error":null,"receipt_error":null,"report_error":null,
                "client":{"status":200,"body_bytes":serde_json::to_vec(&report).unwrap()},"report":report,"receipts":receipts}));
        }
        events.push(
            json!({"event":"fastly-budget-cleanup","adapter":"fastly","ports":[4001,4002],
            "accepted":11,"status":"pass","error":null,"qualification":false}),
        );
        events.push(
            json!({"event":"fastly-budget-inventory","adapter":"fastly","ports":[4001,4002],
            "cases":6,"origin_requests":11,"status":"pass","qualification":false}),
        );
        events
    }

    #[test]
    fn fastly_budget_five_variants_validate_configuration_evidence() {
        validate_report("variants", &variant_report()).unwrap();
    }

    #[test]
    fn fastly_budget_non_listening_variants_require_no_http_receipts() {
        assert!(expected_labels("variants").is_empty());
        assert_eq!(
            CASES
                .iter()
                .map(|case| expected_labels(case).len())
                .sum::<usize>(),
            11
        );
    }

    #[test]
    fn fastly_budget_variants_reject_missing_native_or_typed_outcomes() {
        let valid = variant_report();
        validate_report("variants", &valid).unwrap();
        for path in [
            "/observations/0/status",
            "/observations/0/error_kind",
            "/observations/0/bad_gateway_reason",
            "/observations/0/timeout_source",
            "/observations/0/pending_acquired_us",
            "/observations/0/native/operation_polls",
            "/observations/0/native/ready_operation_polls",
            "/observations/0/native/first_operation_poll",
            "/observations/0/native/operation_released_us",
            "/observations/0/native/operations",
            "/observations/0/native/operation_releases",
            "/observations/0/native/heads",
            "/observations/0/native/sources",
            "/observations/0/native/releases",
            "/observations/0/native/reads",
            "/observations/0/native/ready_reads",
            "/observations/0/native/waits",
            "/observations/0/native/last_timeout",
        ] {
            let mut invalid = valid.clone();
            *invalid.pointer_mut(path).unwrap() = json!("invalid");
            assert!(validate_report("variants", &invalid).is_err(), "{path}");
        }
    }

    #[test]
    fn fastly_budget_variants_reject_legacy_tls_coverage_claims() {
        validate_case(&variant_event(), [4001, 4002]).unwrap();
        let mut forged = variant_event();
        forged["report"]["tls_variant"] = json!(true);
        forged["client"]["body_bytes"] = json!(serde_json::to_vec(&forged["report"]).unwrap());
        assert!(validate_case(&forged, [4001, 4002]).is_err());
        let mut missing = variant_event();
        missing["report"]
            .as_object_mut()
            .unwrap()
            .remove("tls_policy");
        missing["client"]["body_bytes"] = json!(serde_json::to_vec(&missing["report"]).unwrap());
        assert!(validate_case(&missing, [4001, 4002]).is_err());
    }

    #[test]
    fn fastly_budget_full_artifact_requires_all_evidence_after_baseline() {
        let valid = baseline_artifact();
        validate_artifact(&valid, "fastly").unwrap();
        for index in [0, 2, 3, 4, 5] {
            let mut invalid = valid.clone();
            invalid[index]["receipts"] = json!([]);
            assert!(validate_artifact(&invalid, "fastly").is_err(), "{index}");
        }
        let mut forged = valid.clone();
        forged[1]["report"]["tls_variant"] = json!(true);
        forged[1]["client"]["body_bytes"] =
            json!(serde_json::to_vec(&forged[1]["report"]).unwrap());
        assert!(validate_artifact(&forged, "fastly").is_err());
        let mut unaccounted = valid.clone();
        unaccounted[6]["accepted"] = json!(12);
        assert!(validate_artifact(&unaccounted, "fastly").is_err());
    }

    #[test]
    fn fastly_budget_variants_check_every_request_setting_and_native_target() {
        let valid = variant_event();
        for index in 0..5 {
            for field in [
                "target",
                "method",
                "scheme",
                "host",
                "port",
                "authority",
                "host_override",
                "connect_timeout_ms",
                "cert_host",
                "sni_hostname",
            ] {
                let mut invalid = valid.clone();
                invalid["report"]["observations"][index]["request"][field] = json!("foreign");
                invalid["client"]["body_bytes"] =
                    json!(serde_json::to_vec(&invalid["report"]).unwrap());
                assert!(
                    validate_case(&invalid, [4001, 4002]).is_err(),
                    "{index}: {field}"
                );
            }
            for field in ["preparation_target", "preparation_mode"] {
                let mut invalid = valid.clone();
                invalid["report"]["observations"][index]["native"][field] = json!("foreign");
                invalid["client"]["body_bytes"] =
                    json!(serde_json::to_vec(&invalid["report"]).unwrap());
                assert!(
                    validate_case(&invalid, [4001, 4002]).is_err(),
                    "{index}: {field}"
                );
            }
        }
    }

    #[test]
    fn fastly_budget_variants_reject_bad_counts_order_and_clocks() {
        let valid = variant_event();
        for (path, value) in [
            ("/observations/1/label", json!("baseline")),
            ("/observations/1/request/scheme", json!("http")),
            ("/observations/0/status", json!(200)),
            ("/observations/0/bad_gateway_reason", json!("Transport")),
            ("/observations/0/native/heads", json!(1)),
            ("/observations/0/native/sources", json!(1)),
            ("/observations/0/native/operation_polls", json!(0)),
            ("/observations/0/native/operation_releases", json!(0)),
            (
                "/observations/0/native/first_operation_poll/pending",
                json!(true),
            ),
            (
                "/observations/0/native/first_operation_poll/started_us",
                json!(9),
            ),
            (
                "/observations/0/native/first_operation_poll/observed_us",
                json!(19),
            ),
            ("/observations/0/native/operation_released_us", json!(29)),
            ("/observations/0/native/operation_released_us", json!(101)),
            ("/observations/0/deadline_us", json!(1_000_001)),
            ("/observations/0/finished_us", json!(1_000_000)),
            ("/observations/1/entered_us", json!(99)),
            ("/observations/0/bytes", json!([1])),
            ("/stats/1/configurations", json!(1)),
            ("/stats/1/registrations", json!(1)),
            ("/stats/0/max_registration_us", json!(11)),
        ] {
            let mut invalid = valid.clone();
            *invalid["report"].pointer_mut(path).unwrap() = value;
            invalid["client"]["body_bytes"] =
                json!(serde_json::to_vec(&invalid["report"]).unwrap());
            assert!(validate_case(&invalid, [4001, 4002]).is_err(), "{path}");
        }
        for index in 0..5 {
            let mut invalid = valid.clone();
            invalid["report"]["observations"]
                .as_array_mut()
                .unwrap()
                .remove(index);
            invalid["client"]["body_bytes"] =
                json!(serde_json::to_vec(&invalid["report"]).unwrap());
            assert!(validate_case(&invalid, [4001, 4002]).is_err());
        }
        let mut invalid = valid.clone();
        invalid["report"]["observations"]
            .as_array_mut()
            .unwrap()
            .swap(1, 2);
        invalid["client"]["body_bytes"] = json!(serde_json::to_vec(&invalid["report"]).unwrap());
        assert!(validate_case(&invalid, [4001, 4002]).is_err());
    }

    #[test]
    fn fastly_budget_variants_check_raw_envelope_receipts_and_owned_ports() {
        for (path, value) in [
            ("/receipts", json!([{"event":"budget-origin"}])),
            ("/receipts", Value::Null),
            ("/variant_ports", json!([4101, 4101])),
            ("/variant_ports", json!([0, 4102])),
            ("/variant_ports", json!([4001, 4102])),
            ("/variant_ports", json!([4101, 65536])),
            ("/variant_ports", json!([4101])),
            ("/client/status", json!(502)),
            ("/client/body_bytes", json!(b"not JSON".as_slice())),
            ("/report/token", json!("foreign")),
            ("/client_error", json!("failure")),
            ("/receipt_error", json!("failure")),
            ("/report_error", json!("failure")),
            ("/error", json!("failure")),
            ("/status", json!("unverified")),
        ] {
            let mut invalid = variant_event();
            *invalid.pointer_mut(path).unwrap() = value;
            assert!(validate_case(&invalid, [4001, 4002]).is_err(), "{path}");
        }
    }

    #[test]
    fn fastly_budget_owned_variant_ports_are_distinct_exclusive_loopback() {
        let targets = NonListeningTargets::bind().unwrap();
        assert_ne!(targets.ports[0], targets.ports[1]);
        for (owner, port) in targets._owners.iter().zip(targets.ports) {
            assert!(port > 0);
            let address = std::net::SocketAddr::from(([127, 0, 0, 1], port));
            assert_eq!(owner.local_addr().unwrap().as_socket(), Some(address));
            assert!(std::net::TcpListener::bind(address).is_err());
        }
    }

    #[test]
    fn fastly_budget_non_listening_ports_record_native_outcomes_while_reserved() {
        let targets = NonListeningTargets::bind().unwrap();
        assert_ne!(targets.ports[0], targets.ports[1]);
        for port in targets.ports {
            assert!(port > 0);
            let address = std::net::SocketAddr::from(([127, 0, 0, 1], port));
            let error = std::net::TcpStream::connect_timeout(
                &address,
                std::time::Duration::from_millis(250),
            )
            .unwrap_err();
            let outcome = match error.kind() {
                std::io::ErrorKind::ConnectionRefused => "ConnectionRefused",
                std::io::ErrorKind::TimedOut => "TimedOut",
                kind => panic!("unexpected native non-listening outcome: {kind:?}: {error}"),
            };
            println!(
                "{}",
                json!({"event":"budget-native-non-listening-probe","port":port,
                "target_policy":"bound-non-listening","native_outcome":outcome,"raw_os_error":error.raw_os_error()})
            );
            assert!(std::net::TcpListener::bind(address).is_err());
        }
        drop(targets);
    }

    #[test]
    fn fastly_budget_windows_reject_late_wrong_cause_and_renewed_deadlines() {
        for (case, limit) in [("head", 105_000), ("body", 505_000)] {
            let valid = report(case);
            validate_report(case, &valid).unwrap();
            for (field, value) in [
                ("finished_us", json!(limit + 1)),
                ("timeout_source", json!("BatchCutoff")),
                ("deadline_us", json!(999_999)),
                ("status", json!(500)),
                ("pending_acquired_us", Value::Null),
            ] {
                let mut invalid = valid.clone();
                invalid["observations"][0][field] = value;
                assert!(validate_report(case, &invalid).is_err(), "{case}: {field}");
            }
        }
    }

    #[test]
    fn fastly_budget_body_requires_actual_wait_read_and_release() {
        let valid = report("body");
        validate_report("body", &valid).unwrap();
        for (field, value) in [
            ("last_timeout", Value::Null),
            ("ready_reads", json!(0)),
            ("releases", json!(0)),
            ("invalid", json!(true)),
        ] {
            let mut invalid = valid.clone();
            invalid["observations"][1]["native"][field] = value;
            assert!(validate_report("body", &invalid).is_err(), "{field}");
        }
    }

    #[test]
    fn fastly_budget_variants_cannot_credit_http_only_or_missing_cases() {
        for case in CASES {
            assert!(validate_report(case, &json!({})).is_err(), "{case}");
        }
        assert!(validate_report("variants", &report("variants")).is_err());
    }

    #[test]
    fn fastly_budget_cache_slot_and_settlement_preserve_real_contracts() {
        for case in ["reuse", "slot", "settlement"] {
            let valid = complete_report(case);
            validate_report(case, &valid).unwrap();
            let mut invalid = valid.clone();
            invalid["observations"][0]["native"]["operations"] = json!(0);
            assert!(validate_report(case, &invalid).is_err(), "{case}");
            let mut invalid = valid.clone();
            match case {
                "reuse" => invalid["stats"][1]["registrations"] = json!(2),
                "slot" => invalid["observations"][0]["arrival_elapsed_us"] = json!(205_001),
                _ => invalid["settled_us"] = json!(25_000),
            }
            assert!(validate_report(case, &invalid).is_err(), "{case}");
        }
    }

    #[test]
    fn fastly_budget_case_rechecks_raw_response_and_partial_origin_evidence() {
        let report = report("head");
        let valid = json!({"event":"fastly-budget-case","adapter":"fastly","case":"head","token":"owned",
            "qualification":false,"status":"pass","error":null,"client_error":null,"receipt_error":null,"report_error":null,
            "client":{"status":200,"body_bytes":serde_json::to_vec(&report).unwrap()},"report":report,
            "receipts":[{"event":"budget-origin","case":"head","token":"owned","label":"head","method":"GET",
                "target":"/budget/owned/head/head","port":4001,"head_attempt_us":500_000,"head_complete_us":null,
                "chunks":[],"error":null,"terminal":"peer-closed"}]});
        validate_case(&valid, [4001, 4002]).unwrap();
        for field in ["client_error", "receipt_error", "report_error", "error"] {
            let mut invalid = valid.clone();
            invalid[field] = json!("retained failure");
            assert!(validate_case(&invalid, [4001, 4002]).is_err(), "{field}");
        }
        for (field, value) in [
            ("token", json!("foreign")),
            ("error", json!("write failed")),
            ("head_attempt_us", json!(499_999)),
            ("port", json!(4002)),
            ("terminal", json!("stopped")),
        ] {
            let mut invalid = valid.clone();
            invalid["receipts"][0][field] = value;
            assert!(validate_case(&invalid, [4001, 4002]).is_err(), "{field}");
        }
        let mut invalid = valid.clone();
        invalid["client"]["body_bytes"] = json!(b"not JSON".as_slice());
        assert!(validate_case(&invalid, [4001, 4002]).is_err());
        assert!(validate_artifact(&[valid], "fastly").is_err());
    }
}

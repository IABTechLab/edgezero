use super::*;

mod contract;

pub(super) fn policy_cases() -> impl Iterator<Item = (bool, &'static str, bool, u8)> {
    contract::policy_cases()
}

const PARITY: &[&str] = &[
    "parity-default",
    "parity-marker-off",
    "parity-marker-on",
    "parity-post",
    "parity-head",
    "parity-204",
    "parity-304",
    "parity-redirect",
    "parity-final-cap",
    "parity-decoded-cap",
    "parity-encoded-cap",
    "parity-gzip",
    "parity-original",
    "parity-opaque",
    "parity-upload-cap",
    "parity-zero-timeout",
    "parity-expired",
];
const GZIP: &[u8] = &[
    31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 75, 76, 132, 1, 0, 240, 205, 17, 76, 10, 0, 0, 0,
];

fn cases(adapter: &str) -> Result<Vec<&'static str>> {
    let mut result = match adapter {
        "axum" => vec![],
        "fastly" => vec![
            "optimizer",
            "post",
            "body",
            "bypass",
            "mixed",
            "identity",
            "target",
            "redirect-identity",
            "head-900",
        ],
        "cloudflare" => vec!["unsupported", "connect-short"],
        "spin" => vec!["unsupported", "denied"],
        _ => return Err("unknown native transport adapter".into()),
    };
    result.extend_from_slice(PARITY);
    Ok(result)
}

// (origin index, path case, operation), independent of the guest's report.
fn receipts(case: &str) -> Vec<(usize, &str, &str)> {
    match case {
        "optimizer"
        | "post"
        | "body"
        | "bypass"
        | "parity-upload-cap"
        | "parity-zero-timeout"
        | "parity-expired" => vec![],
        "mixed" | "identity" | "unsupported" => vec![(0, "plain", "plain")],
        "target" => vec![(0, "raw%2Ftarget", "target")],
        "redirect-identity" => vec![
            (0, "redirect", "single-a"),
            (1, "second", "single-b"),
            (0, "redirect", "follow"),
            (1, "second", "follow"),
        ],
        "denied" => vec![(0, "denied", "denied")],
        "connect-short" => vec![(0, "plain", "connect-short")],
        "head-900" => vec![(0, "head-900", "head-900")],
        _ => vec![(0, case, "send"), (0, case, "begin")],
    }
}

fn path(case: &str, token: &str) -> String {
    format!(
        "/transport/{case}/{token}{}",
        if case == "raw%2Ftarget" {
            "?a=%2F&b=two%20words&a=1"
        } else {
            ""
        }
    )
}

fn typed_error(
    outcome: &Value,
    kind: &str,
    status: u16,
    reason: Option<&str>,
    cause: Option<&str>,
    rejection: Option<&str>,
    provider: Option<&str>,
) -> Result<()> {
    let error = &outcome["error"];
    require(
        outcome.as_object().is_some_and(|v| v.len() == 1)
            && error.as_object().is_some_and(|v| {
                v.len() == 7
                    && [
                        "kind",
                        "status",
                        "reason",
                        "cause",
                        "rejection",
                        "upload_origin",
                        "provider",
                    ]
                    .iter()
                    .all(|field| v.contains_key(*field))
            })
            && error["kind"] == kind
            && error["status"] == status
            && error["reason"] == json!(reason)
            && error["cause"] == json!(cause)
            && error["rejection"] == json!(rejection)
            && error["upload_origin"]
                == if status == 413 {
                    json!("OutboundUpload")
                } else {
                    Value::Null
                }
            && match provider {
                Some(needle) => error["provider"]
                    .as_str()
                    .is_some_and(|p| p.contains(needle)),
                None => error.get("provider") == Some(&Value::Null),
            },
        "native transport typed error differs",
    )
}

fn success(
    outcome: &Value,
    status: u16,
    bytes: &[u8],
    marker: bool,
    adapter: &str,
    token: &str,
) -> Result<()> {
    require(
        outcome.as_object().is_some_and(|v| v.len() == 3)
            && outcome["status"] == status
            && outcome["bytes"] == json!(bytes),
        "native transport response differs",
    )?;
    let fields = outcome["headers"]
        .as_array()
        .ok_or("missing native transport headers")?;
    require(
        fields.iter().all(|f| {
            f.as_array().is_some_and(|p| p.len() == 2)
                && f[0].as_str().is_some_and(|n| n == n.to_ascii_lowercase())
                && serde_json::from_value::<Vec<u8>>(f[1].clone()).is_ok()
        }),
        "malformed native transport headers",
    )?;
    for (name, value) in [
        ("x-transport-fixture", Some(b"native".to_vec())),
        ("x-request-token", Some(token.as_bytes().to_vec())),
        (
            "x-edgezero-proxy",
            marker.then(|| adapter.as_bytes().to_vec()),
        ),
        ("connection", None),
        ("transfer-encoding", None),
    ] {
        let found = fields
            .iter()
            .filter(|f| f[0] == name)
            .map(|f| f[1].clone())
            .collect::<Vec<_>>();
        require(
            found == value.into_iter().map(|v| json!(v)).collect::<Vec<_>>(),
            "native transport response header differs",
        )?;
    }
    Ok(())
}

fn stats(value: &Value, configurations: u64) -> Result<()> {
    let registrations = value["registrations"]
        .as_u64()
        .ok_or("missing backend registrations")?;
    require(
        value.as_object().is_some_and(|v| v.len() == 2)
            && value["configurations"] == configurations
            && registrations <= configurations,
        "native backend identity or registration count differs",
    )
}

fn unsupported(value: &Value) -> Result<()> {
    typed_error(
        value,
        "bad_request",
        400,
        None,
        None,
        Some("unsupported_optimizer"),
        None,
    )
}

fn unavailable(value: &Value) -> Result<()> {
    typed_error(
        value,
        "bad_gateway",
        502,
        Some("ImageOptimizerUnavailable"),
        None,
        None,
        Some("ImageOptimizerUnsupported"),
    )
}

fn native_rejected(value: &Value) -> Result<()> {
    require(
        value
            == &json!({"attachments":1,"invalid":false,"preparations":0,"prepared":null,
        "operation_acquisitions":0,"operation_polls":0,"operation_releases":0,
        "source_acquisitions":0,"source_releases":0,"heads":0,"reads":0,"waits":0}),
        "unsupported optimizer prepared or acquired a native SDK operation",
    )
}

fn native_sibling(value: &Value, uri: &str) -> Result<()> {
    require(
        value.as_object().is_some_and(|v| v.len() == 12)
            && value["attachments"] == 1
            && value["invalid"] == false
            && value["preparations"] == 1
            && value["prepared"] == json!({"mode":"Batch","target":uri})
            && value["operation_acquisitions"] == 1
            && value["operation_releases"] == 1
            && value["operation_polls"].as_u64().is_some_and(|n| n > 0)
            && value["heads"] == 1
            && value["source_acquisitions"] == 1
            && value["source_releases"] == 1
            && value["reads"].as_u64().is_some_and(|n| n > 0)
            && value["waits"].as_u64().is_some(),
        "healthy optimizer sibling lacks actual SDK activity or resource release",
    )
}

fn validate_report(
    report: &Value,
    adapter: &str,
    case: &str,
    token: &str,
    ports: [u64; 2],
) -> Result<()> {
    require(
        report["adapter"] == adapter
            && report["case"] == case
            && report["token"] == token
            && report["qualification"] == false,
        "foreign native transport report",
    )?;
    if adapter == "fastly" {
        stats(&report["before"], 0)?;
    } else {
        require(
            report.get("before") == Some(&Value::Null) && report.get("after") == Some(&Value::Null),
            "foreign backend telemetry",
        )?;
    }
    let activity = &report["activity"];
    require(
        activity.as_object().is_some_and(|v| v.len() == 4)
            && activity["support"].is_array()
            && ["dispatches", "begins", "batches"]
                .iter()
                .all(|n| activity[n].as_u64().is_some()),
        "missing native entrypoint observations",
    )?;
    let steps = report["steps"]
        .as_array()
        .ok_or("missing native transport steps")?;
    let first = steps.first().ok_or("empty native transport steps")?;
    let outcome = &first["outcome"];
    let uri = |origin: usize, route: &str| {
        format!("http://127.0.0.1:{}{}", ports[origin], path(route, token))
    };
    if PARITY.contains(&case) {
        require(
            steps.len() == 1 && first["step"] == "parity" && first["send"] == first["begin"],
            "Send and Begin buffered parity differs",
        )?;
        let observed = &first["send"];
        match case {
            "parity-final-cap" | "parity-decoded-cap" | "parity-encoded-cap" => {
                let reason = match case {
                    "parity-final-cap" => "BufferedBody",
                    "parity-decoded-cap" => "DecodedBody",
                    _ => "EncodedBody",
                };
                typed_error(
                    observed,
                    "response_too_large",
                    502,
                    Some(reason),
                    None,
                    None,
                    None,
                )?;
            }
            "parity-upload-cap" => {
                typed_error(observed, "payload_too_large", 413, None, None, None, None)?
            }
            "parity-zero-timeout" | "parity-expired" => typed_error(
                observed,
                "gateway_timeout",
                504,
                None,
                Some(if case == "parity-expired" {
                    "RequestDeadline"
                } else {
                    "PerCallTimeout"
                }),
                None,
                None,
            )?,
            _ => {
                let status = match case {
                    "parity-204" => 204,
                    "parity-304" => 304,
                    "parity-redirect" => 302,
                    _ => 200,
                };
                let bytes = match case {
                    "parity-head" | "parity-204" | "parity-304" => b"".as_slice(),
                    "parity-post" => b"abc",
                    "parity-gzip" => b"aaaaaaaaaa",
                    "parity-original" => GZIP,
                    _ => b"ok",
                };
                success(
                    observed,
                    status,
                    bytes,
                    case != "parity-marker-off",
                    adapter,
                    token,
                )?;
                let encodings = observed["headers"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|f| f[0] == "content-encoding")
                    .map(|f| f[1].clone())
                    .collect::<Vec<_>>();
                let expected = match case {
                    "parity-original" => vec![json!(b"gzip".as_slice())],
                    "parity-opaque" => vec![json!(b"x-transport-opaque".as_slice())],
                    _ => vec![],
                };
                require(encodings == expected, "Send/Begin response coding differs")?;
            }
        }
    } else {
        require(
            steps.len()
                == match case {
                    "unsupported" | "identity" => 2,
                    "redirect-identity" => 3,
                    _ => 1,
                },
            "wrong native control step count",
        )?;
        match case {
            "unsupported" => {
                require(
                    first["step"] == "send" && first["uri"] == uri(0, "plain"),
                    "foreign optimizer send target",
                )?;
                unsupported(outcome)?;
                native_rejected(&first["native"])?;
                let batch = &steps[1];
                require(
                    batch["step"] == "batch"
                        && batch["termination"] == "Completed"
                        && batch["uris"] == json!([uri(0, "plain"), uri(0, "plain")])
                        && batch["slots"].as_array().is_some_and(|v| v.len() == 2)
                        && batch["slots"][0]["index"] == 0
                        && batch["slots"][1]["index"] == 1
                        && batch["native"].as_array().is_some_and(|v| v.len() == 2),
                    "unsupported optimizer batch differs",
                )?;
                unsupported(&batch["slots"][0]["outcome"])?;
                success(
                    &batch["slots"][1]["outcome"],
                    200,
                    b"ok",
                    true,
                    adapter,
                    token,
                )?;
                native_rejected(&batch["native"][0])?;
                native_sibling(&batch["native"][1], &uri(0, "plain"))?;
                require(
                    activity
                        == &json!({"support":[{"mode":"Send","optimizer":true,"accepted":false},{"mode":"Batch","optimizer":true,"accepted":false},{"mode":"Batch","optimizer":false,"accepted":true}],
                    "dispatches":0,"begins":0,"batches":1}),
                    "unsupported optimizer reached native I/O entrypoints",
                )?;
            }
            "optimizer" | "identity" => {
                require(
                    first["step"] == "optimizer" && first["uri"] == uri(0, "plain"),
                    "foreign optimizer target",
                )?;
                unavailable(outcome)?;
                stats(&first["before"], 0)?;
                stats(&first["after"], 1)?;
                if case == "identity" {
                    require(
                        steps[1]["step"] == "plain" && steps[1]["uri"] == uri(0, "plain"),
                        "optimizer/plain identity target differs",
                    )?;
                    success(&steps[1]["outcome"], 200, b"ok", true, adapter, token)?;
                    require(
                        steps[1]["before"] == first["after"] && steps[1]["after"] == first["after"],
                        "optimizer flag changed backend identity",
                    )?;
                }
            }
            "post" | "body" | "bypass" => {
                typed_error(
                    outcome,
                    "bad_request",
                    400,
                    None,
                    None,
                    Some(match case {
                        "post" => "optimizer_method",
                        "body" => "get_body",
                        _ => "optimizer_cache",
                    }),
                    None,
                )?;
                require(
                    activity["dispatches"] == 0
                        && activity["begins"] == 0
                        && activity["batches"] == 0,
                    "invalid optimizer request dispatched",
                )?;
            }
            "mixed" => {
                require(
                    first["step"] == "batch"
                        && first["termination"] == "Completed"
                        && first["uris"] == json!([uri(0, "plain"), uri(0, "plain")])
                        && first["slots"].as_array().is_some_and(|v| v.len() == 2)
                        && first["slots"][0]["index"] == 0
                        && first["slots"][1]["index"] == 1,
                    "mixed optimizer slot identity differs",
                )?;
                unsupported(&first["slots"][0]["outcome"])?;
                success(
                    &first["slots"][1]["outcome"],
                    200,
                    b"ok",
                    true,
                    adapter,
                    token,
                )?;
            }
            "target" => {
                require(
                    first["uri"] == uri(0, "raw%2Ftarget"),
                    "application target was rewritten",
                )?;
                success(outcome, 200, b"ok", true, adapter, token)?;
            }
            "redirect-identity" => {
                for (i, status, origin, route, label) in [
                    (0, 302, 0, "redirect", "single-a"),
                    (1, 200, 1, "second", "single-b"),
                    (2, 200, 1, "second", "follow"),
                ] {
                    require(
                        steps[i]["uri"] == uri(origin, route) && steps[i]["step"] == label,
                        "Follow backend comparison target differs",
                    )?;
                    success(&steps[i]["outcome"], status, b"ok", true, adapter, token)?;
                    stats(
                        &steps[i]["before"],
                        if i == 0 {
                            0
                        } else if i == 1 {
                            1
                        } else {
                            2
                        },
                    )?;
                    stats(&steps[i]["after"], if i == 0 { 1 } else { 2 })?;
                }
                require(
                    steps[2]["redirects"] == 1
                        && steps[2].get("stop") == Some(&Value::Null)
                        && steps[1]["after"] == steps[2]["after"],
                    "Follow added a backend or changed stop",
                )?;
                for step in steps {
                    require(
                        step["settings"] == json!({"timeout_ms":3000,"connect_ms":null}),
                        "single/Follow comparison settings differ",
                    )?;
                }
            }
            "denied" => {
                require(
                    first["step"] == "denied" && first["hop"] == 1,
                    "denied redirect hop index differs",
                )?;
                typed_error(
                    outcome,
                    "bad_request",
                    400,
                    None,
                    None,
                    None,
                    Some("HttpRequestDenied"),
                )?;
            }
            "connect-short" | "head-900" => {
                require(
                    first["step"] == case
                        && first["uri"] == uri(0, if case == "head-900" { case } else { "plain" })
                        && first["settings"]
                            == if case == "head-900" {
                                json!({"timeout_ms":1000,"connect_ms":null})
                            } else {
                                json!({"timeout_ms":3000,"connect_ms":1})
                            },
                    "connect control settings differ",
                )?;
                success(outcome, 200, b"ok", true, adapter, token)?;
            }
            _ => return Err("unknown native transport case".into()),
        }
    }
    if adapter == "fastly" {
        stats(
            &report["after"],
            match case {
                "post"
                | "body"
                | "bypass"
                | "parity-upload-cap"
                | "parity-zero-timeout"
                | "parity-expired" => 0,
                "redirect-identity" => 2,
                _ => 1,
            },
        )?;
    }
    Ok(())
}

pub(super) fn validate_artifact(events: &[Value], adapter: &str) -> Result<()> {
    require(
        events.iter().all(|e| e["status"] != "fail"),
        "failed native transport evidence",
    )?;
    let inventories = events
        .iter()
        .filter(|e| e["event"] == "native-transport-inventory")
        .collect::<Vec<_>>();
    let [inventory] = inventories.as_slice() else {
        return Err("missing or duplicate native transport inventory".into());
    };
    let ports: [u64; 2] = serde_json::from_value(inventory["origin_ports"].clone())?;
    require(
        ports[0] != ports[1] && ports.iter().all(|p| (1..=65535).contains(p)),
        "invalid native transport origins",
    )?;
    let expected = cases(adapter)?;
    let total = expected.iter().map(|c| receipts(c).len()).sum::<usize>();
    require(
        inventory["adapter"] == adapter
            && inventory["status"] == "pass"
            && inventory["qualification"] == false
            && inventory["cases"] == expected.len()
            && inventory["parity_cases"] == PARITY.len()
            && inventory["origin_requests"] == total
            && inventory["accepted_connections"] == total,
        "native transport inventory differs",
    )?;
    let mut seen = HashSet::new();
    let mut tokens = HashSet::new();
    for event in events
        .iter()
        .filter(|e| e["event"] == "native-transport-case")
    {
        let case = event["case"].as_str().ok_or("missing native case")?;
        let token = event["token"]
            .as_str()
            .filter(|t| {
                !t.is_empty()
                    && t.len() <= 64
                    && t.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
            .ok_or("invalid native token")?;
        require(
            expected.contains(&case)
                && seen.insert(case)
                && tokens.insert(token)
                && event["adapter"] == adapter
                && event["status"] == "pass"
                && event["qualification"] == false,
            "duplicate or foreign native transport case",
        )?;
        let wanted = receipts(case);
        let origins = event["origin"]
            .as_array()
            .ok_or("missing native receipts")?;
        require(
            origins.len() == wanted.len() && event["accepted_connections"] == wanted.len(),
            "native transport origin count differs",
        )?;
        for (receipt, (origin, route, step)) in origins.iter().zip(wanted) {
            let method = if case == "parity-post" {
                "POST"
            } else if case == "parity-head" {
                "HEAD"
            } else {
                "GET"
            };
            let payload = if method == "POST" { "abc" } else { "" };
            require(
                receipt["port"] == ports[origin]
                    && receipt["target"] == path(route, token)
                    && receipt["token"] == token
                    && receipt["method"] == method
                    && receipt["step"] == step
                    && receipt["payload"] == payload
                    && receipt["proof"]
                        == if case == "target" {
                            json!(["application-value"])
                        } else {
                            json!([])
                        }
                    && receipt["signature"]
                        == if case == "target" {
                            json!(["user-built-signature"])
                        } else {
                            json!([])
                        },
                "native transport origin receipt reassociated",
            )?;
            net::validate_forward_capture(receipt)?;
            let lengths = receipt["content_length"]
                .as_array()
                .ok_or("missing native framing")?;
            let encodings = receipt["transfer_encoding"]
                .as_array()
                .ok_or("missing native encoding")?;
            require(
                lengths.len() <= 1
                    && encodings.len() <= 1
                    && (lengths.is_empty() || encodings.is_empty())
                    && (method == "POST"
                        || (encodings.is_empty()
                            && lengths.iter().all(|v| v == "0")
                            && receipt["encoded_body_hex"] == "")),
                "native transport request framing differs",
            )?;
            require(
                if case == "head-900" {
                    receipt["head_delay_us"]
                        .as_u64()
                        .is_some_and(|us| us >= 900_000)
                } else {
                    receipt.get("head_delay_us") == Some(&Value::Null)
                },
                "native origin head delay differs",
            )?;
        }
        validate_report(&event["report"], adapter, case, token, ports)
            .map_err(|error| format!("{adapter}/{case}: {error}"))?;
        let observed: Value = serde_json::from_slice(&body(&event["response"])?)?;
        require(
            event["response"]["status"] == 200 && observed == event["report"],
            "native report and HTTP bytes disagree",
        )?;
    }
    require(
        seen.len() == expected.len(),
        "missing native transport cases",
    )?;
    contract::validate_artifact(events, adapter)
}

pub(super) fn run(adapter: &str, port: u16, records: &mut Records) -> Result<()> {
    let expected = cases(adapter)?;
    let origins = WireOrigins::start()?;
    let ports = [origins.port(0), origins.port(1)];
    let outcome = (|| -> Result<()> {
        for case in &expected {
            let token = token();
            let before = origins.accepted();
            let response = net::header_request(
                port,
                &format!("/progress/transport/{case}"),
                &token,
                &[
                    ("x-origin", &format!("http://127.0.0.1:{}/", ports[0])),
                    (
                        "x-origin-second",
                        &format!("http://127.0.0.1:{}/", ports[1]),
                    ),
                ],
                None,
            )?;
            let report: Value = serde_json::from_slice(&body(&response)?)?;
            let origin = (0..receipts(case).len())
                .map(|_| origins.take_wire())
                .collect::<Result<Vec<_>>>()?;
            records.push(json!({"event":"native-transport-case","adapter":adapter,"case":case,"token":token,
                "report":report,"response":response,"origin":origin,"accepted_connections":origins.accepted()-before,"status":"pass","qualification":false}))?;
        }
        Ok(())
    })();
    let cleanup = origins.finish_counted();
    if let Err(error) = &cleanup {
        records.push(json!({"adapter":adapter,"status":"fail","stage":"native-transport-origin-cleanup","error":error.to_string(),"qualification":false}))?;
    }
    outcome?;
    let accepted = cleanup?;
    records.push(json!({"event":"native-transport-inventory","adapter":adapter,"origin_ports":ports,"cases":expected.len(),
        "parity_cases":PARITY.len(),"origin_requests":expected.iter().map(|c|receipts(c).len()).sum::<usize>(),
        "accepted_connections":accepted,"status":"pass","qualification":false}))?;
    contract::run(adapter, port, records)?;
    validate_artifact(&super::super::ci::read_events(&records.path)?, adapter)?;
    println!("native_buffered_begin_send_parity_{adapter}: passed");
    match adapter {
        "fastly" => {
            for criterion in [
                "ez_8_acceptance_06_viceroy",
                "ez_8_acceptance_07_viceroy",
                "ez_9_acceptance_18_viceroy",
                "ez_4_acceptance_07_viceroy",
            ] {
                println!("{criterion}: passed");
            }
        }
        "cloudflare" => {
            println!("ez_8_acceptance_05_workerd: passed");
            println!("ez_4_acceptance_05_workerd: passed");
        }
        "spin" => {
            println!("ez_8_acceptance_05_spin: passed");
            println!("ez_9_acceptance_19_spin: passed");
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    fn failed(
        kind: &str,
        status: u16,
        reason: Option<&str>,
        cause: Option<&str>,
        rejection: Option<&str>,
        provider: Option<&str>,
    ) -> Value {
        json!({"error":{"kind":kind,"status":status,"reason":reason,"cause":cause,"rejection":rejection,
            "upload_origin":if status==413 {Some("OutboundUpload")} else {None},"provider":provider}})
    }

    fn rejected() -> Value {
        failed(
            "bad_request",
            400,
            None,
            None,
            Some("unsupported_optimizer"),
            None,
        )
    }
    fn no_optimizer() -> Value {
        failed(
            "bad_gateway",
            502,
            Some("ImageOptimizerUnavailable"),
            None,
            None,
            Some("Fastly { cause: ImageOptimizerUnsupported }"),
        )
    }
    fn backend(n: u64) -> Value {
        json!({"configurations":n,"registrations":n})
    }
    fn ok(adapter: &str, token: &str, status: u16, bytes: &[u8], marker: bool) -> Value {
        let mut headers = vec![
            json!(["x-transport-fixture", b"native".as_slice()]),
            json!(["x-request-token", token.as_bytes()]),
        ];
        if marker {
            headers.push(json!(["x-edgezero-proxy", adapter.as_bytes()]));
        }
        json!({"status":status,"bytes":bytes,"headers":headers})
    }

    pub(crate) fn artifact(adapter: &str) -> Vec<Value> {
        let mut events = Vec::new();
        let expected = cases(adapter).unwrap();
        for case in &expected {
            let token = format!("transport-{case}");
            let uri = |origin: usize, route: &str| {
                format!("http://127.0.0.1:{}{}", 4001 + origin, path(route, &token))
            };
            let settings = json!({"timeout_ms":3000,"connect_ms":null});
            let plain = ok(adapter, &token, 200, b"ok", true);
            let mut activity = json!({"support":[],"dispatches":1,"begins":0,"batches":0});
            let mut steps = match *case {
                "unsupported" => {
                    activity = json!({"support":[{"mode":"Send","optimizer":true,"accepted":false},{"mode":"Batch","optimizer":true,"accepted":false},{"mode":"Batch","optimizer":false,"accepted":true}],"dispatches":0,"begins":0,"batches":1});
                    let rejected_native = json!({"attachments":1,"invalid":false,"preparations":0,"prepared":null,
                        "operation_acquisitions":0,"operation_polls":0,"operation_releases":0,
                        "source_acquisitions":0,"source_releases":0,"heads":0,"reads":0,"waits":0});
                    let live_native = json!({"attachments":1,"invalid":false,"preparations":1,"prepared":{"mode":"Batch","target":uri(0,"plain")},
                        "operation_acquisitions":1,"operation_polls":1,"operation_releases":1,"source_acquisitions":1,"source_releases":1,"heads":1,"reads":1,"waits":0});
                    vec![
                        json!({"step":"send","uri":uri(0,"plain"),"settings":settings,"outcome":rejected(),"native":rejected_native}),
                        json!({"step":"batch","uris":[uri(0,"plain"),uri(0,"plain")],"termination":"Completed","slots":[{"index":0,"outcome":rejected()},{"index":1,"outcome":plain}],"native":[rejected_native,live_native]}),
                    ]
                }
                "optimizer" | "identity" => {
                    let mut steps = vec![
                        json!({"step":"optimizer","uri":uri(0,"plain"),"outcome":no_optimizer(),"before":backend(0),"after":backend(1)}),
                    ];
                    if *case == "identity" {
                        steps.push(json!({"step":"plain","uri":uri(0,"plain"),"outcome":plain,"before":backend(1),"after":backend(1)}));
                    }
                    steps
                }
                "post" | "body" | "bypass" => {
                    activity["dispatches"] = json!(0);
                    vec![
                        json!({"step":"optimizer","outcome":failed("bad_request",400,None,None,Some(match *case {"post"=>"optimizer_method","body"=>"get_body",_=>"optimizer_cache"}),None)}),
                    ]
                }
                "mixed" => vec![
                    json!({"step":"batch","uris":[uri(0,"plain"),uri(0,"plain")],"termination":"Completed","slots":[{"index":0,"outcome":rejected()},{"index":1,"outcome":plain}]}),
                ],
                "target" => {
                    vec![json!({"step":"target","uri":uri(0,"raw%2Ftarget"),"outcome":plain})]
                }
                "redirect-identity" => vec![
                    json!({"step":"single-a","uri":uri(0,"redirect"),"settings":settings,"outcome":ok(adapter,&token,302,b"ok",true),"before":backend(0),"after":backend(1)}),
                    json!({"step":"single-b","uri":uri(1,"second"),"settings":settings,"outcome":plain,"before":backend(1),"after":backend(2)}),
                    json!({"step":"follow","uri":uri(1,"second"),"redirects":1,"stop":null,"settings":settings,"outcome":plain,"before":backend(2),"after":backend(2)}),
                ],
                "denied" => vec![
                    json!({"step":"denied","hop":1,"outcome":failed("bad_request",400,None,None,None,Some("Spin { cause: HttpRequestDenied }"))}),
                ],
                "connect-short" | "head-900" => vec![
                    json!({"step":case,"uri":uri(0,if *case=="head-900" {case} else {"plain"}),
                    "settings":if *case=="head-900" {json!({"timeout_ms":1000,"connect_ms":null})} else {json!({"timeout_ms":3000,"connect_ms":1})},"outcome":plain}),
                ],
                _ => {
                    let mut result = match *case {
                        "parity-upload-cap" => {
                            failed("payload_too_large", 413, None, None, None, None)
                        }
                        "parity-zero-timeout" | "parity-expired" => failed(
                            "gateway_timeout",
                            504,
                            None,
                            Some(if *case == "parity-expired" {
                                "RequestDeadline"
                            } else {
                                "PerCallTimeout"
                            }),
                            None,
                            None,
                        ),
                        "parity-final-cap" | "parity-decoded-cap" | "parity-encoded-cap" => failed(
                            "response_too_large",
                            502,
                            Some(match *case {
                                "parity-final-cap" => "BufferedBody",
                                "parity-decoded-cap" => "DecodedBody",
                                _ => "EncodedBody",
                            }),
                            None,
                            None,
                            None,
                        ),
                        _ => ok(
                            adapter,
                            &token,
                            match *case {
                                "parity-204" => 204,
                                "parity-304" => 304,
                                "parity-redirect" => 302,
                                _ => 200,
                            },
                            match *case {
                                "parity-post" => b"abc",
                                "parity-head" | "parity-204" | "parity-304" => b"",
                                "parity-gzip" => b"aaaaaaaaaa",
                                "parity-original" => GZIP,
                                _ => b"ok",
                            },
                            *case != "parity-marker-off",
                        ),
                    };
                    if matches!(*case, "parity-original" | "parity-opaque") {
                        result["headers"].as_array_mut().unwrap().push(json!([
                            "content-encoding",
                            if *case == "parity-original" {
                                b"gzip".as_slice()
                            } else {
                                b"x-transport-opaque".as_slice()
                            }
                        ]));
                    }
                    vec![json!({"step":"parity","send":result,"begin":result})]
                }
            };
            if adapter != "fastly" {
                for step in &mut steps {
                    step["before"] = Value::Null;
                    step["after"] = Value::Null;
                }
            }
            let report = json!({"adapter":adapter,"case":case,"token":token,"steps":steps,"activity":activity,
                "before":if adapter=="fastly" {backend(0)} else {Value::Null},
                "after":if adapter=="fastly" {backend(match *case {"post"|"body"|"bypass"|"parity-upload-cap"|"parity-zero-timeout"|"parity-expired"=>0,"redirect-identity"=>2,_=>1})} else {Value::Null},"qualification":false});
            let origin=receipts(case).into_iter().map(|(index,route,step)|{
                let payload=if *case=="parity-post" {"abc"} else {""};
                json!({"port":4001+index,"target":path(route,&token),"token":token,"step":step,
                    "method":if *case=="parity-post" {"POST"} else if *case=="parity-head" {"HEAD"} else {"GET"},
                    "payload":payload,"encoded_body_hex":if payload.is_empty() {""} else {"616263"},"content_length":[payload.len().to_string()],"transfer_encoding":[],
                    "proof":if *case=="target" {vec!["application-value"]} else {vec![]},"signature":if *case=="target" {vec!["user-built-signature"]} else {vec![]},
                    "head_delay_us":if *case=="head-900" {Some(900_000)} else {None}})
            }).collect::<Vec<_>>();
            events.push(json!({"event":"native-transport-case","adapter":adapter,"case":case,"token":token,"origin":origin,
                "accepted_connections":origin.len(),"report":report,"response":{"status":200,"body_bytes":report.to_string().as_bytes()},"status":"pass","qualification":false}));
        }
        let total = expected.iter().map(|c| receipts(c).len()).sum::<usize>();
        events.push(json!({"event":"native-transport-inventory","adapter":adapter,"origin_ports":[4001,4002],"cases":expected.len(),
            "parity_cases":PARITY.len(),"origin_requests":total,"accepted_connections":total,"status":"pass","qualification":false}));
        events.extend(contract::tests::artifact(adapter));
        events
    }

    #[test]
    fn native_transport_evidence_rejects_missing_inventory() {
        for adapter in ["axum", "fastly", "cloudflare", "spin"] {
            assert!(validate_artifact(&[], adapter).is_err(), "{adapter}");
        }
    }

    #[test]
    fn native_parity_rejects_the_legacy_seventeen_vector_slice_as_full_contract_proof() {
        for adapter in ["axum", "cloudflare", "fastly", "spin"] {
            let mut events = artifact(adapter);
            events.retain(|event| {
                !event["case"]
                    .as_str()
                    .is_some_and(|name| name.starts_with("pair-"))
            });
            assert!(validate_artifact(&events, adapter).is_err(), "{adapter}");
        }
    }

    #[test]
    fn native_transport_errors_require_explicit_nullable_fields() {
        let valid = artifact("cloudflare");
        for field in ["reason", "cause", "rejection", "upload_origin", "provider"] {
            let mut changed = valid.clone();
            let error = changed[0]["report"]["steps"][0]["outcome"]["error"]
                .as_object_mut()
                .unwrap();
            let old = error.remove(field).unwrap();
            error.insert("foreign".to_owned(), old);
            changed[0]["response"]["body_bytes"] =
                json!(changed[0]["report"].to_string().as_bytes());
            assert!(
                validate_artifact(&changed, "cloudflare").is_err(),
                "missing {field}"
            );
        }
    }

    #[test]
    fn native_transport_evidence_requires_sdk_witnesses_for_unsupported_optimizer() {
        for adapter in ["cloudflare", "spin"] {
            let mut events = artifact(adapter);
            let report = &mut events[0]["report"];
            report["steps"][0].as_object_mut().unwrap().remove("native");
            report["steps"][1].as_object_mut().unwrap().remove("native");
            events[0]["response"]["body_bytes"] = json!(events[0]["report"].to_string().as_bytes());
            assert!(validate_artifact(&events, adapter).is_err(), "{adapter}");
        }
    }

    #[test]
    fn native_transport_evidence_rejects_preparation_or_operation_on_rejected_requests() {
        for adapter in ["cloudflare", "spin"] {
            let valid = artifact(adapter);
            for path in ["/steps/0/native", "/steps/1/native/0"] {
                for field in [
                    "attachments",
                    "preparations",
                    "operation_acquisitions",
                    "operation_polls",
                    "operation_releases",
                    "source_acquisitions",
                    "source_releases",
                    "heads",
                    "reads",
                    "waits",
                ] {
                    let mut changed = valid.clone();
                    changed[0]["report"].pointer_mut(path).unwrap()[field] =
                        json!(if field == "attachments" { 0 } else { 1 });
                    changed[0]["response"]["body_bytes"] =
                        json!(changed[0]["report"].to_string().as_bytes());
                    assert!(
                        validate_artifact(&changed, adapter).is_err(),
                        "{adapter}/{path}/{field}"
                    );
                }
            }
            for field in [
                "preparations",
                "operation_acquisitions",
                "operation_polls",
                "operation_releases",
                "source_acquisitions",
                "source_releases",
                "heads",
                "reads",
            ] {
                let mut changed = valid.clone();
                changed[0]["report"]["steps"][1]["native"][1][field] = json!(0);
                changed[0]["response"]["body_bytes"] =
                    json!(changed[0]["report"].to_string().as_bytes());
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "unobserved healthy sibling {adapter}/{field}"
                );
            }
        }
    }

    #[test]
    fn native_transport_evidence_rejects_incomplete_and_reassociated_records() {
        for adapter in ["axum", "fastly", "cloudflare", "spin"] {
            let valid = artifact(adapter);
            validate_artifact(&valid, adapter).unwrap();
            for index in 0..valid.len() {
                let mut changed = valid.clone();
                changed.remove(index);
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "{adapter}/{index}"
                );
            }
            for pointer in [
                "/0/token",
                "/0/adapter",
                "/0/status",
                "/0/qualification",
                "/0/accepted_connections",
                "/0/report/activity/dispatches",
                "/0/response/status",
            ] {
                let mut changed = valid.clone();
                *changed[0]
                    .pointer_mut(pointer.strip_prefix("/0").unwrap())
                    .unwrap() = Value::Null;
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "{adapter}/{pointer}"
                );
            }
            let index = valid
                .iter()
                .position(|e| e["origin"].as_array().is_some_and(|v| !v.is_empty()))
                .unwrap();
            for field in [
                "port",
                "method",
                "target",
                "token",
                "step",
                "payload",
                "content_length",
                "transfer_encoding",
                "encoded_body_hex",
                "proof",
                "signature",
                "head_delay_us",
            ] {
                let mut changed = valid.clone();
                changed[index]["origin"][0][field] = json!("foreign");
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "{adapter}/{field}"
                );
            }
            let mut changed = valid.clone();
            changed.push(json!({"status":"fail"}));
            assert!(validate_artifact(&changed, adapter).is_err());
            let mut changed = valid.clone();
            changed.push(valid[0].clone());
            assert!(validate_artifact(&changed, adapter).is_err());
            let mut changed = valid.clone();
            changed.push(valid.last().unwrap().clone());
            assert!(validate_artifact(&changed, adapter).is_err());
        }
    }

    #[test]
    fn native_transport_evidence_rejects_wrong_native_outcomes_even_when_both_parity_sides_match() {
        for adapter in ["axum", "fastly", "cloudflare", "spin"] {
            let valid = artifact(adapter);
            for index in
                (0..valid.len()).filter(|index| valid[*index]["event"] == "native-transport-case")
            {
                let mut changed = valid.clone();
                let step = &mut changed[index]["report"]["steps"][0];
                if step["step"] == "parity" {
                    step["send"] = json!({});
                    step["begin"] = json!({});
                } else if step["step"] == "batch" {
                    step["slots"][0]["outcome"] = json!({});
                } else {
                    step["outcome"] = json!({});
                }
                changed[index]["response"]["body_bytes"] =
                    json!(changed[index]["report"].to_string().as_bytes());
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "{adapter}/{index}"
                );
            }
        }
    }
}

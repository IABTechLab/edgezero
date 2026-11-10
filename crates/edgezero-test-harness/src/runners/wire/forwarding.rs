use super::*;
use crate::net::ForwardFraming as Framing;

struct Case {
    method: &'static str,
    framing: Framing,
    mode: &'static str,
}

fn unknown_late(case: &Case, adapter: &str, report: &Value) -> bool {
    case.mode == "late" && adapter == "cloudflare" && report["host_signal"]["body_absent"] == false
}

fn cases(adapter: &str) -> Vec<Case> {
    let mut cases = Vec::new();
    for method in ["GET", "HEAD"] {
        for framing in [Framing::Absent, Framing::Zero, Framing::ChunkedEmpty] {
            cases.push(Case {
                method,
                framing,
                mode: "send",
            });
        }
    }
    cases.push(Case {
        method: "GET",
        framing: Framing::FiveBytes,
        mode: "reject",
    });
    for method in ["POST", "PUT", "PATCH", "OPTIONS", "DELETE"] {
        cases.push(Case {
            method,
            framing: if matches!(method, "OPTIONS" | "DELETE") {
                Framing::Absent
            } else {
                Framing::ThreeBytes
            },
            mode: "send",
        });
    }
    for (method, framing, mode) in [
        ("GET", Framing::ChunkedEmpty, "begin"),
        ("POST", Framing::ThreeBytes, "begin-post"),
        ("GET", Framing::ChunkedEmpty, "batch"),
        ("GET", Framing::Pending, "unsupported"),
        ("GET", Framing::ChunkedEmpty, "follow"),
    ] {
        cases.push(Case {
            method,
            framing,
            mode,
        });
    }
    if adapter != "fastly" {
        cases.push(Case {
            method: "GET",
            framing: if adapter == "spin" {
                Framing::Zero
            } else {
                Framing::Absent
            },
            mode: "late",
        });
    }
    cases
}

fn report(response: &Value) -> Result<Value> {
    let headers = response["headers"]
        .as_array()
        .ok_or("missing forwarding response headers")?;
    let fields: Vec<_> = headers
        .iter()
        .filter(|field| {
            field[0]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("x-forward-report"))
        })
        .collect();
    let [field] = fields.as_slice() else {
        return Err("missing or duplicate forwarding report".into());
    };
    let field = field[1]
        .as_str()
        .filter(|field| field.len() <= 2048)
        .ok_or("invalid forwarding report")?;
    Ok(serde_json::from_str(field)?)
}

fn validate_case(event: &Value, adapter: &str, ports: [u64; 2]) -> Result<usize> {
    let index = event["case"]
        .as_u64()
        .and_then(|i| usize::try_from(i).ok())
        .ok_or("invalid forwarding index")?;
    let cases = cases(adapter);
    let case = cases.get(index).ok_or("unknown forwarding case")?;
    let token = event["token"]
        .as_str()
        .filter(|v| {
            !v.is_empty()
                && v.len() <= 64
                && v.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
        .ok_or("invalid forwarding token")?;
    let report = report(&event["response"])?;
    require(
        report == event["report"]
            && report["case"] == case.mode
            && report["token"] == token
            && report["qualification"] == false
            && report["before"] == "Initial"
            && event["adapter"] == adapter
            && event["status"] == "pass"
            && event["qualification"] == false
            && event["method"] == case.method
            && event["framing"] == format!("{:?}", case.framing),
        "forwarding identity or report differs",
    )?;
    let source = &report["source"];
    let polls = source["polls"]
        .as_u64()
        .ok_or("missing forwarding poll count")?;
    for field in ["bytes", "eofs", "drops", "nonempty"] {
        require(
            source[field].as_u64().is_some(),
            "missing forwarding source counter",
        )?;
    }
    let source_bytes = source["bytes"]
        .as_u64()
        .ok_or("invalid source byte count")?;
    let nonempty = source["nonempty"]
        .as_u64()
        .ok_or("invalid nonempty count")?;
    let eofs = source["eofs"].as_u64().ok_or("invalid EOF count")?;
    require(
        (source_bytes == 0) == (nonempty == 0)
            && source_bytes <= 8
            && nonempty <= source_bytes
            && eofs <= 1
            && nonempty
                .checked_add(eofs)
                .is_some_and(|items| items <= polls),
        "impossible forwarding source counters",
    )?;
    let rejected = matches!(case.mode, "reject" | "begin-post" | "unsupported");
    let unknown_late = unknown_late(case, adapter, &report);
    if adapter == "fastly" {
        require(
            report["backend_before"] == json!({"configurations":0,"registrations":0}),
            "preexisting forwarding backend work",
        )?;
        if rejected {
            require(
                report["backend_after"] == report["backend_before"],
                "rejected forwarding configured a backend",
            )?;
        } else {
            for field in ["configurations", "registrations"] {
                require(
                    report["backend_after"][field]
                        .as_u64()
                        .is_some_and(|n| n > 0 && n <= 2),
                    "missing forwarding registration observation",
                )?;
            }
        }
    }
    let expected_status = if rejected {
        400
    } else if unknown_late {
        408
    } else {
        201
    };
    require(
        event["response"]["status"] == expected_status,
        "forwarding status differs",
    )?;
    require(
        body(&event["response"])?
            == if rejected || unknown_late || case.method == "HEAD" {
                b"".as_slice()
            } else {
                b"end".as_slice()
            },
        "forwarding response bytes differ",
    )?;
    require(
        report["cached"] == (case.mode == "batch")
            && report["begun"] == (case.mode == "begin")
            && report["redirects"] == if case.mode == "follow" { 1 } else { 0 }
            && if unknown_late {
                report["error"] == "request_timeout"
            } else if rejected {
                report["error"] == "bad_request"
            } else {
                report["error"].is_null()
            },
        "wrong forwarding operation outcome",
    )?;
    require(
        report["rejection"]
            == match case.mode {
                "reject" => json!("get_body"),
                "begin-post" => json!("streamed_begin"),
                "unsupported" => json!("unsupported_option"),
                "late" if unknown_late => json!("inbound_timeout"),
                _ => Value::Null,
            },
        "wrong forwarding rejection reason",
    )?;
    let streamed = report["streamed"]
        .as_bool()
        .ok_or("missing body representation")?;
    require(
        if streamed {
            source["drops"] == 1
        } else {
            source == &json!({"polls":0,"bytes":0,"eofs":0,"drops":0,"nonempty":0})
        },
        "forwarded source not released exactly once",
    )?;
    if unknown_late {
        require(
            streamed
                && polls == 0
                && source["bytes"] == 0
                && source["eofs"] == 0
                && source["nonempty"] == 0,
            "unknown late body lost its expired read deadline",
        )?;
    } else if matches!(case.mode, "unsupported" | "begin-post") {
        require(
            streamed
                && polls == 0
                && source["bytes"] == 0
                && source["eofs"] == 0
                && source["nonempty"] == 0,
            "unsupported forwarding polled its pending source",
        )?;
    } else if case.mode == "reject" {
        require(
            streamed
                && polls > 0
                && source["bytes"]
                    .as_u64()
                    .is_some_and(|bytes| (1..=5).contains(&bytes))
                && source["eofs"] == 0
                && source["nonempty"] == 1,
            "nonempty GET did not reject its first byte",
        )?;
    } else if streamed {
        require(
            polls > 0
                && source["eofs"] == 1
                && source["bytes"]
                    == if matches!(case.method, "POST" | "PUT" | "PATCH") {
                        3
                    } else {
                        0
                    },
            "forwarding settlement/upload changed bytes or EOF",
        )?;
    }
    if case.mode == "batch" {
        require(!streamed, "batch did not use the cached inbound body")?;
    }
    if case.mode == "late" && !unknown_late {
        require(
            !streamed && polls == 0,
            "certified empty late forward read its body",
        )?;
    }
    match adapter {
        "cloudflare" => {
            require(
                report["host_signal"]["source"] == "workerd-before-conversion"
                    && report["host_signal"]["body_absent"].as_bool().is_some(),
                "missing original workerd body signal",
            )?;
            if report["host_signal"]["body_absent"] == true {
                require(
                    !streamed,
                    "workerd certified no body but conversion retained a stream",
                )?;
            }
        }
        "fastly" => require(
            report["host_signal"]["source"] == "fastly-sdk-before-conversion",
            "missing Fastly body signal",
        )?,
        "spin" => require(
            report["host_signal"]["source"] == "spin-sdk-before-conversion",
            "missing Spin body signal",
        )?,
        _ => return Err("unsupported forwarding adapter".into()),
    }
    let receipts = event["origin"]
        .as_array()
        .ok_or("missing forwarding origin receipts")?;
    let expected_count = if rejected || unknown_late {
        0
    } else if matches!(case.mode, "begin" | "follow") {
        2
    } else {
        1
    };
    require(
        receipts.len() == expected_count && event["accepted"] == expected_count,
        "unexpected forwarding origin connection",
    )?;
    for (hop, receipt) in receipts.iter().enumerate() {
        let operation = match (case.mode, hop) {
            ("begin", 0) => "begin",
            ("begin", 1) => "confirm",
            ("follow", 0) => "follow",
            ("follow", 1) => "final",
            _ => "send",
        };
        let payload = if matches!(case.method, "POST" | "PUT" | "PATCH") {
            "abc"
        } else {
            ""
        };
        require(
            receipt["port"] == ports[usize::from(operation == "final")]
                && receipt["method"]
                    == if operation == "confirm" {
                        "GET"
                    } else {
                        case.method
                    }
                && receipt["target"] == format!("/forward/{operation}/{token}")
                && receipt["token"] == token
                && receipt["operation"] == operation
                && receipt["payload"] == payload,
            "forwarding origin reassociation, method or payload changed",
        )?;
        let lengths = receipt["content_length"]
            .as_array()
            .ok_or("missing origin lengths")?;
        let encodings = receipt["transfer_encoding"]
            .as_array()
            .ok_or("missing origin encoding")?;
        require(
            lengths.len() <= 1
                && encodings.len() <= 1
                && (lengths.is_empty() || encodings.is_empty()),
            "ambiguous forwarded framing",
        )?;
        let encoded = receipt["encoded_body_hex"]
            .as_str()
            .ok_or("missing raw origin body")?;
        net::validate_forward_capture(receipt)?;
        if case.method == "GET" || case.method == "HEAD" || operation == "confirm" {
            require(
                encodings.is_empty() && lengths.iter().all(|v| v == "0") && encoded.is_empty(),
                "bodiless GET/HEAD carried framing or bytes",
            )?;
        }
    }
    Ok(receipts.len())
}

pub(super) fn validate_artifact(events: &[Value], adapter: &str) -> Result<()> {
    require(
        events.iter().all(|event| event["status"] != "fail"),
        "failed forwarding evidence",
    )?;
    let inventories: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "inbound-forwarding-inventory")
        .collect();
    let [inventory] = inventories.as_slice() else {
        return Err("missing or duplicate forwarding inventory".into());
    };
    let ports: Vec<_> = inventory["ports"]
        .as_array()
        .ok_or("missing forwarding ports")?
        .iter()
        .map(|p| {
            p.as_u64()
                .filter(|p| (1..=65535).contains(p))
                .ok_or("invalid forwarding port")
        })
        .collect::<std::result::Result<_, _>>()?;
    require(
        ports.len() == 2 && ports[0] != ports[1],
        "invalid forwarding origin pair",
    )?;
    let mut indices = Vec::new();
    let mut tokens = HashSet::new();
    let mut receipts = 0;
    for event in events
        .iter()
        .filter(|event| event["event"] == "inbound-forwarding-case")
    {
        receipts += validate_case(event, adapter, [ports[0], ports[1]])?;
        require(
            tokens.insert(event["token"].clone().to_string()),
            "duplicate forwarding token",
        )?;
        indices.push(event["case"].as_u64().ok_or("invalid forwarding index")?);
    }
    indices.sort_unstable();
    require(
        indices == (0..cases(adapter).len() as u64).collect::<Vec<_>>()
            && inventory["adapter"] == adapter
            && inventory["status"] == "pass"
            && inventory["qualification"] == false
            && inventory["cases"] == indices.len()
            && inventory["accepted"] == receipts
            && inventory["origin_requests"] == receipts,
        "incomplete forwarding cases or cleanup accounting",
    )
}

pub(super) fn run(adapter: &str, port: u16, records: &mut Records) -> Result<()> {
    let origins = WireOrigins::start()?;
    let ports = [origins.port(0), origins.port(1)];
    let mut total = 0;
    let outcome = (|| -> Result<()> {
        for (index, case) in cases(adapter).iter().enumerate() {
            let token = token();
            let operation = if matches!(case.mode, "begin" | "follow") {
                case.mode
            } else {
                "send"
            };
            let target = format!("http://127.0.0.1:{}/forward/{operation}/{token}", ports[0]);
            if case.mode == "begin" {
                origins.arm(&format!("forward-{token}"));
            }
            let before = origins.accepted();
            let response =
                net::forward_request(port, case.mode, case.method, case.framing, &token, &target)?;
            let observed_report = report(&response)?;
            let unknown_late = unknown_late(case, adapter, &observed_report);
            let expected_status = if unknown_late {
                408
            } else if matches!(case.mode, "reject" | "begin-post" | "unsupported") {
                400
            } else {
                201
            };
            if response["status"] != expected_status {
                records.push(json!({"event":"inbound-forwarding-failure","adapter":adapter,"case":index,"token":token,"response":response,"status":"fail","qualification":false}))?;
                return Err(format!(
                    "forwarding case {index} ({}) expected {expected_status}",
                    case.mode
                )
                .into());
            }
            let count =
                if unknown_late || matches!(case.mode, "reject" | "begin-post" | "unsupported") {
                    0
                } else if matches!(case.mode, "begin" | "follow") {
                    2
                } else {
                    1
                };
            let receipts = (0..count)
                .map(|_| origins.take_wire())
                .collect::<Result<Vec<_>>>()?;
            let event = json!({"event":"inbound-forwarding-case","adapter":adapter,"case":index,"method":case.method,
                "framing":format!("{:?}",case.framing),"token":token,"report":observed_report,"response":response,
                "origin":receipts,"accepted":origins.accepted()-before,"status":"pass","qualification":false});
            records.push(event.clone())?;
            total += validate_case(&event, adapter, ports.map(u64::from))?;
        }
        Ok(())
    })();
    let cleanup = origins.finish_counted();
    if let Err(error) = &cleanup {
        records.push(json!({"status":"fail","stage":"forwarding-origin-cleanup","error":error.to_string(),"qualification":false}))?;
    }
    outcome?;
    require(
        cleanup? == total,
        "late or partial forwarding origin connection",
    )?;
    records.push(json!({"event":"inbound-forwarding-inventory","adapter":adapter,"ports":ports,"cases":cases(adapter).len(),"origin_requests":total,"accepted":total,"status":"pass","qualification":false}))?;
    validate_artifact(&super::super::ci::read_events(&records.path)?, adapter)?;
    let environment = match adapter {
        "fastly" => "viceroy",
        "cloudflare" => "workerd",
        "spin" => "spin",
        _ => return Err("unsupported forwarding adapter".into()),
    };
    println!("ez_1_acceptance_01_{environment}_http1: passed");
    for criterion in ["04", "09", "10", "12", "14"] {
        println!("ez_1_acceptance_{criterion}_{environment}: passed");
    }
    if adapter == "cloudflare" {
        println!("ez_1_acceptance_03_workerd: passed");
        println!("ez_1_acceptance_08_workerd: passed");
    } else if adapter == "spin" {
        println!("ez_1_acceptance_08_spin_content_length_0: passed");
    }
    if adapter == "fastly" {
        println!("ez_1_acceptance_11_viceroy: passed");
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    fn rewrite_report(event: &mut Value, change: impl FnOnce(&mut Value)) {
        change(&mut event["report"]);
        event["response"]["headers"][0][1] = json!(event["report"].to_string());
    }

    pub(crate) fn artifact(adapter: &str) -> Vec<Value> {
        let mut events = Vec::new();
        let mut total = 0;
        for (index, case) in cases(adapter).iter().enumerate() {
            let token = format!("forward-{index}");
            let rejected = matches!(case.mode, "reject" | "begin-post" | "unsupported");
            let streamed = !matches!(case.mode, "batch" | "late");
            let idle = matches!(case.mode, "begin-post" | "unsupported") || !streamed;
            let source = json!({"polls":if idle {0} else if matches!(case.method,"POST"|"PUT"|"PATCH") {2} else {1},"bytes":if case.mode == "reject" {5} else if matches!(case.method,"POST"|"PUT"|"PATCH") && !idle {3} else {0},"eofs":u8::from(!idle && case.mode != "reject"),"drops":u8::from(streamed),"nonempty":u8::from(!idle && (case.mode=="reject" || matches!(case.method,"POST"|"PUT"|"PATCH")))});
            let report = json!({"case":case.mode,"token":token,"before":"Initial","cached":case.mode=="batch","streamed":streamed,
                "source":source,"begun":case.mode=="begin","redirects":u8::from(case.mode=="follow"),"error":rejected.then_some("bad_request"),
                "rejection":match case.mode {"reject"=>json!("get_body"),"begin-post"=>json!("streamed_begin"),"unsupported"=>json!("unsupported_option"),_=>Value::Null},
                "backend_before":if adapter=="fastly" {Some(json!({"configurations":0,"registrations":0}))} else {None},
                "backend_after":if adapter=="fastly" {Some(json!({"configurations":u8::from(!rejected),"registrations":u8::from(!rejected)}))} else {None},
                "host_signal":match adapter {"spin"=>json!({"source":"spin-sdk-before-conversion"}),"fastly"=>json!({"source":"fastly-sdk-before-conversion"}),_=>json!({"source":"workerd-before-conversion","body_absent":case.mode=="late"})},"qualification":false});
            let count = if rejected {
                0
            } else if matches!(case.mode, "begin" | "follow") {
                2
            } else {
                1
            };
            let origin: Vec<_> = (0..count).map(|hop| {
                let operation = match (case.mode,hop) {("begin",0)=>"begin",("begin",1)=>"confirm",("follow",0)=>"follow",("follow",1)=>"final",_=>"send"};
                let payload = if matches!(case.method,"POST"|"PUT"|"PATCH") {"abc"} else {""};
                json!({"port":if operation=="final" {4002} else {4001},"method":if operation=="confirm" {"GET"} else {case.method},"target":format!("/forward/{operation}/{token}"),"token":token,"operation":operation,
                    "content_length":[payload.len().to_string()],"transfer_encoding":[],"encoded_body_hex":payload.as_bytes().iter().map(|b|format!("{b:02x}")).collect::<String>(),"payload":payload})
            }).collect();
            total += count;
            let response = json!({"status":if rejected {400} else {201},"headers":[["x-forward-report",report.to_string()]],"body_bytes":if rejected || case.method=="HEAD" {b"".as_slice()} else {b"end".as_slice()}});
            events.push(json!({"event":"inbound-forwarding-case","adapter":adapter,"case":index,"method":case.method,"framing":format!("{:?}",case.framing),"token":token,"report":report,"response":response,"origin":origin,"accepted":count,"status":"pass","qualification":false}));
        }
        events.push(json!({"event":"inbound-forwarding-inventory","adapter":adapter,"ports":[4001,4002],"cases":cases(adapter).len(),"accepted":total,"origin_requests":total,"status":"pass","qualification":false}));
        events
    }

    #[test]
    fn forwarding_evidence_requires_all_real_ingress_outcomes_and_cleanup() {
        for adapter in ["fastly", "cloudflare", "spin"] {
            let valid = artifact(adapter);
            validate_artifact(&valid, adapter).unwrap();
            for index in 0..valid.len() {
                let mut changed = valid.clone();
                changed.remove(index);
                assert!(validate_artifact(&changed, adapter).is_err());
            }
            for index in 0..valid.len() {
                let mut changed = valid.clone();
                changed.push(valid[index].clone());
                assert!(validate_artifact(&changed, adapter).is_err());
            }
            for pointer in [
                "/0/origin/0/token",
                "/0/origin/0/target",
                "/0/origin/0/method",
                "/0/origin/0/payload",
                "/0/origin/0/encoded_body_hex",
                "/0/accepted",
                "/0/report/source/polls",
                "/0/response/body_bytes",
            ] {
                let mut changed = json!(valid);
                *changed.pointer_mut(pointer).unwrap() = json!("wrong");
                assert!(
                    validate_artifact(changed.as_array().unwrap(), adapter).is_err(),
                    "{pointer}"
                );
            }
            let mut changed = valid.clone();
            changed.push(json!({"status":"fail","stage":"late-cleanup"}));
            assert!(validate_artifact(&changed, adapter).is_err());
        }
    }

    #[test]
    fn forwarding_witnesses_reject_consistent_but_false_reports() {
        for adapter in ["fastly", "cloudflare", "spin"] {
            let valid = artifact(adapter);
            for (index, pointer, value) in [
                (0, "/source/polls", json!(0)),
                (0, "/source/bytes", json!(1)),
                (0, "/source/drops", json!(0)),
                (6, "/source/nonempty", json!(2)),
                (7, "/source/nonempty", json!(0)),
                (13, "/source/polls", json!(1)),
                (15, "/source/polls", json!(1)),
                (16, "/source/eofs", json!(2)),
                (15, "/rejection", json!("get_body")),
            ] {
                let mut changed = valid.clone();
                rewrite_report(&mut changed[index], |report| {
                    *report.pointer_mut(pointer).unwrap() = value
                });
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "{adapter}/{index}/{pointer}"
                );
            }
            let mut fragmented = valid.clone();
            rewrite_report(&mut fragmented[6], |report| {
                report["source"]["bytes"] = json!(1)
            });
            fragmented[7]["origin"][0]["content_length"] = json!([]);
            fragmented[7]["origin"][0]["transfer_encoding"] = json!(["chunked"]);
            fragmented[7]["origin"][0]["encoded_body_hex"] = json!(
                b"1\r\na\r\n2\r\nbc\r\n0\r\n\r\n"
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            );
            validate_artifact(&fragmented, adapter).unwrap();
            let mut unframed = valid.clone();
            unframed[7]["origin"][0]["content_length"] = json!([]);
            assert!(validate_artifact(&unframed, adapter).is_err());
        }
    }

    #[test]
    fn cloudflare_late_forward_records_unknown_body_without_native_credit() {
        let mut events = artifact("cloudflare");
        let late = events.len() - 2;
        rewrite_report(&mut events[late], |report| {
            report["host_signal"]["body_absent"] = json!(false);
            report["streamed"] = json!(true);
            report["source"]["drops"] = json!(1);
            report["error"] = json!("request_timeout");
            report["rejection"] = json!("inbound_timeout");
        });
        events[late]["response"]["status"] = json!(408);
        events[late]["response"]["body_bytes"] = json!([]);
        events[late]["origin"] = json!([]);
        events[late]["accepted"] = json!(0);
        let inventory = events.last_mut().unwrap();
        for field in ["accepted", "origin_requests"] {
            inventory[field] = json!(inventory[field].as_u64().unwrap() - 1);
        }
        validate_artifact(&events, "cloudflare").unwrap();
        rewrite_report(&mut events[late], |report| {
            report["host_signal"]["body_absent"] = json!(true)
        });
        assert!(validate_artifact(&events, "cloudflare").is_err());
    }
}

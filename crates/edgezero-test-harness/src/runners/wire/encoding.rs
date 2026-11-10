use super::*;

struct Case {
    kind: &'static str,
    original: bool,
    streamed: bool,
    explicit_accept: bool,
}

impl Case {
    fn new(adapter: &str, index: usize) -> Result<Self> {
        require(index < count(adapter), "unknown encoding case")?;
        Ok(Self {
            kind: if index == 8 {
                "batch"
            } else if index >= 9 {
                "relay"
            } else {
                "coding"
            },
            original: index >= 8 || index & 1 != 0,
            streamed: if index >= 9 {
                index == 10
            } else {
                index < 8 && index & 2 != 0
            },
            explicit_accept: index >= 8 || index & 4 != 0,
        })
    }

    fn name(&self) -> String {
        format!(
            "{}-gzip-{}-200-{}",
            self.kind,
            if self.streamed {
                "streamed"
            } else {
                "buffered"
            },
            if self.original { "original" } else { "decode" }
        )
    }

    fn request(&self) -> Value {
        json!({"kind":self.kind,"streamed":self.streamed,"original":self.original,
            "accept_control":if self.explicit_accept {"gzip, br"} else {"absent"}})
    }
}

fn count(adapter: &str) -> usize {
    if adapter == "cloudflare" { 11 } else { 9 }
}
fn dispatches(adapter: &str) -> usize {
    count(adapter) + 1
}

fn report(case: &Case, token: &str, original: bool, slot: Option<&str>) -> Result<Value> {
    let encoded = net::coding_body("gzip")?;
    Ok(
        json!({"case":case.name(),"status":200,"content_encoding":original.then_some("gzip"),
        "content_length":original.then(||encoded.len().to_string()),"etag":"\"coding-probe\"",
        "vary":"Accept-Encoding","token":token,"slot":slot,"streamed":case.streamed,
        "decoding":if original {"Original"}else{"Decode"},"bytes":if original {encoded}else{b"raw coding probe\n"},"qualification":false}),
    )
}

fn field_values(client: &Value, name: &str) -> Result<Vec<String>> {
    client["headers"]
        .as_array()
        .ok_or("missing encoding response headers")?
        .iter()
        .filter(|pair| {
            pair[0]
                .as_str()
                .is_some_and(|field| field.eq_ignore_ascii_case(name))
        })
        .map(|pair| {
            pair[1]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "nontext encoding response field".into())
        })
        .collect()
}

fn validate_case(event: &Value, adapter: &str, port: u64) -> Result<()> {
    let index = event["case"]
        .as_u64()
        .and_then(|v| usize::try_from(v).ok())
        .ok_or("missing encoding index")?;
    let case = Case::new(adapter, index)?;
    let token = event["token"]
        .as_str()
        .filter(|token| {
            !token.is_empty()
                && token.len() <= 64
                && token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
        .ok_or("invalid encoding token")?;
    require(
        event["adapter"] == adapter
            && event["qualification"] == false
            && event["request"] == case.request(),
        "foreign encoding case or effective settings",
    )?;
    let origins = event["origins"]
        .as_array()
        .ok_or("missing encoding receipts")?;
    require(
        origins.len() == if case.kind == "batch" { 2 } else { 1 },
        "wrong encoding dispatch count",
    )?;
    let mut slots = Vec::new();
    for receipt in origins {
        let slot: Vec<String> = serde_json::from_value(receipt["slot"].clone())?;
        require(
            receipt["port"] == port
                && receipt["method"] == "GET"
                && receipt["target"] == "/coding/gzip/200"
                && receipt["token"] == token
                && receipt["response_coding"] == "gzip"
                && receipt["response_status"] == 200
                && receipt["accept_encoding"]
                    == json!([if case.explicit_accept {
                        "gzip, br"
                    } else {
                        "identity"
                    }]),
            "encoding origin negotiation or identity changed",
        )?;
        slots.push(slot);
    }
    slots.sort();
    require(
        slots
            == if case.kind == "batch" {
                vec![vec!["0".to_owned()], vec!["1".to_owned()]]
            } else {
                vec![Vec::<String>::new()]
            },
        "encoding slot association changed",
    )?;
    let client = &event["client"];
    if case.kind == "relay" {
        super::super::progress::validate_relay_response(client, "gzip", 200, true)?;
        require(
            field_values(client, "x-coding-token")? == [token]
                && field_values(client, "x-coding-slot")?.is_empty()
                && event["report"].is_null()
                && event["origin_content_length"] == net::coding_body("gzip")?.len()
                && event["client_content_length"] == json!(field_values(client, "content-length")?),
            "relay correlation or observed length changed",
        )?;
        return Ok(());
    }
    let observed: Value = serde_json::from_slice(&body(client)?)?;
    let expected = if case.kind == "batch" {
        json!({"case":case.name(),"slots":[{"index":0,"response":report(&case,token,true,Some("0"))?},
            {"index":1,"response":report(&case,token,false,Some("1"))?}],"termination":"Completed","qualification":false})
    } else {
        report(&case, token, case.original, None)?
    };
    require(
        client["status"] == 200
            && observed == event["report"]
            && observed == expected
            && event["origin_content_length"].is_null()
            && event["client_content_length"].is_null(),
        "encoding bytes, mode, metadata or positional results differ",
    )
}

pub(super) fn validate_artifact(events: &[Value], adapter: &str) -> Result<()> {
    require(
        events.iter().all(|event| event["status"] != "fail"),
        "failed encoding or cleanup evidence",
    )?;
    require(
        ["axum", "fastly", "cloudflare", "spin"].contains(&adapter),
        "unknown encoding adapter",
    )?;
    let inventories: Vec<_> = events
        .iter()
        .filter(|e| e["event"] == "outbound-encoding-inventory")
        .collect();
    let [inventory] = inventories.as_slice() else {
        return Err("missing or duplicate encoding inventory".into());
    };
    let port = inventory["origin_port"]
        .as_u64()
        .filter(|v| (1..=u64::from(u16::MAX)).contains(v))
        .ok_or("invalid encoding origin port")?;
    require(
        inventory["adapter"] == adapter
            && inventory["cases"] == count(adapter)
            && inventory["origin_requests"] == dispatches(adapter)
            && inventory["accepted"] == dispatches(adapter)
            && inventory["cleanup"] == "passed"
            && inventory["status"] == "pass"
            && inventory["qualification"] == false,
        "invalid encoding inventory or cleanup",
    )?;
    let mut cases = Vec::new();
    let mut tokens = HashSet::new();
    for event in events
        .iter()
        .filter(|e| e["event"] == "outbound-encoding-case")
    {
        require(
            event["status"] == "pass"
                && [
                    "error",
                    "client_error",
                    "receipt_error",
                    "report_error",
                    "length_error",
                ]
                .iter()
                .all(|field| event[*field].is_null()),
            "failed encoding case",
        )?;
        validate_case(event, adapter, port)?;
        require(
            tokens.insert(event["token"].as_str().ok_or("missing encoding token")?),
            "duplicate encoding token",
        )?;
        cases.push(event["case"].as_u64().ok_or("missing encoding index")?);
    }
    cases.sort_unstable();
    require(
        cases == (0..count(adapter) as u64).collect::<Vec<_>>(),
        "incomplete or duplicate encoding cases",
    )
}

fn observation(
    adapter: &str,
    index: usize,
    token: &str,
    origin_port: u16,
    response: Result<Value>,
    mut receive: impl FnMut() -> Result<Value>,
) -> Result<(Value, Result<()>)> {
    let case = Case::new(adapter, index)?;
    let mut receipts = Vec::new();
    let mut receipt_error = None;
    for _ in 0..if case.kind == "batch" { 2 } else { 1 } {
        match receive() {
            Ok(receipt) => receipts.push(receipt),
            Err(error) => {
                receipt_error = Some(error);
                break;
            }
        }
    }
    let parsed: Result<Value> = match &response {
        Ok(client) if case.kind != "relay" => {
            body(client).and_then(|bytes| Ok(serde_json::from_slice(&bytes)?))
        }
        _ => Ok(Value::Null),
    };
    let length: Result<Value> = match &response {
        Ok(client) if case.kind == "relay" => {
            field_values(client, "content-length").map(|length| json!(length))
        }
        _ => Ok(Value::Null),
    };
    let mut event = json!({"event":"outbound-encoding-case","adapter":adapter,"case":index,"token":token,
        "request":case.request(),"origins":receipts,"client":response.as_ref().ok(),"report":parsed.as_ref().ok(),
        "client_error":response.as_ref().err().map(ToString::to_string),
        "partial_client":response.as_ref().err().and_then(|error|error.downcast_ref::<net::InterruptedResponse>())
            .map(|partial|json!({"status":partial.status,"headers":partial.headers,"body_bytes":partial.body,"cause":format!("{:?}",partial.cause)})),
        "receipt_error":receipt_error.as_ref().map(ToString::to_string),"report_error":parsed.as_ref().err().map(ToString::to_string),
        "length_error":length.as_ref().err().map(ToString::to_string),
        "origin_content_length":if case.kind=="relay" {Some(net::coding_body("gzip")?.len())}else{None},
        "client_content_length":length.as_ref().ok(),"qualification":false});
    let checked = response
        .map(|_| ())
        .and_then(|()| receipt_error.map_or(Ok(()), Err))
        .and_then(|()| parsed.map(|_| ()))
        .and_then(|()| length.map(|_| ()))
        .and_then(|()| validate_case(&event, adapter, u64::from(origin_port)));
    event["status"] = json!(if checked.is_ok() { "pass" } else { "fail" });
    event["error"] = json!(checked.as_ref().err().map(ToString::to_string));
    Ok((event, checked))
}

pub(super) fn run(adapter: &str, port: u16, records: &mut Records) -> Result<()> {
    let origins = WireOrigins::start()?;
    let origin_port = origins.port(0);
    let outcome = (|| -> Result<()> {
        for index in 0..count(adapter) {
            let case = Case::new(adapter, index)?;
            let token = token();
            let target = format!("http://127.0.0.1:{origin_port}/coding/gzip/200");
            let response = net::header_request(
                port,
                &format!("/progress/{}", case.name()),
                &token,
                &[
                    ("x-fixture-backend", &target),
                    (
                        "x-coding-accept",
                        if case.explicit_accept {
                            "gzip, br"
                        } else {
                            "absent"
                        },
                    ),
                ],
                None,
            );
            let (event, checked) =
                observation(adapter, index, &token, origin_port, response, || {
                    origins.take_wire()
                })?;
            records.push(event)?;
            checked?;
        }
        Ok(())
    })();
    let cleanup = origins.finish_counted();
    if let Err(error) = &cleanup {
        records.push(json!({"event":"outbound-encoding-cleanup","adapter":adapter,"status":"fail","error":error.to_string(),"qualification":false}))?;
    }
    outcome?;
    let accepted = cleanup?;
    records.push(json!({"event":"outbound-encoding-inventory","adapter":adapter,"origin_port":origin_port,
        "cases":count(adapter),"origin_requests":dispatches(adapter),"accepted":accepted,"cleanup":"passed","status":"pass","qualification":false}))?;
    validate_artifact(&super::super::ci::read_events(&records.path)?, adapter)?;
    let environment = match adapter {
        "fastly" => "viceroy",
        "cloudflare" => "workerd",
        other => other,
    };
    println!("ez_5_acceptance_04_{environment}: passed");
    println!("ez_5_acceptance_05_{environment}: passed");
    if adapter == "fastly" {
        println!("ez_5_acceptance_06_viceroy: passed");
    }
    if adapter == "cloudflare" {
        println!("ez_5_acceptance_07_workerd: passed");
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    pub(crate) fn artifact(adapter: &str) -> Vec<Value> {
        let mut records = Vec::new();
        for index in 0..count(adapter) {
            let case = Case::new(adapter, index).unwrap();
            let token = format!("encoding-{index}");
            let origins: Vec<_> = (0..if case.kind=="batch"{2}else{1}).map(|slot|json!({"port":4001,"method":"GET",
                "target":"/coding/gzip/200","token":token,"slot":if case.kind=="batch"{vec![slot.to_string()]}else{vec![]},
                "response_coding":"gzip","response_status":200,"accept_encoding":[if case.explicit_accept{"gzip, br"}else{"identity"}]})).collect();
            let observed = if case.kind == "batch" {
                json!({"case":case.name(),"slots":[{"index":0,"response":report(&case,&token,true,Some("0")).unwrap()},
                    {"index":1,"response":report(&case,&token,false,Some("1")).unwrap()}],"termination":"Completed","qualification":false})
            } else if case.kind == "relay" {
                Value::Null
            } else {
                report(&case, &token, case.original, None).unwrap()
            };
            let mut headers = net::coding_representation_headers("gzip", 200).unwrap();
            let length = net::coding_body("gzip").unwrap().len().to_string();
            headers.extend([
                ("content-encoding", "gzip".to_owned()),
                ("content-length", length.clone()),
                ("x-coding-token", token.clone()),
            ]);
            let client = if case.kind == "relay" {
                json!({"status":200,"headers":headers,"body_bytes":net::coding_body("gzip").unwrap()})
            } else {
                json!({"status":200,"body_bytes":serde_json::to_vec(&observed).unwrap()})
            };
            records.push(json!({"event":"outbound-encoding-case","adapter":adapter,"case":index,"token":token,
                "request":case.request(),"origins":origins,"report":observed,"client":client,
                "origin_content_length":if case.kind=="relay"{Some(length.parse::<usize>().unwrap())}else{None},
                "client_content_length":if case.kind=="relay"{json!([length])}else{Value::Null},
                "status":"pass","error":null,"qualification":false}));
        }
        records.push(json!({"event":"outbound-encoding-inventory","adapter":adapter,"origin_port":4001,"cases":count(adapter),
            "origin_requests":dispatches(adapter),"accepted":dispatches(adapter),"cleanup":"passed","status":"pass","qualification":false}));
        records
    }

    #[test]
    fn encoding_artifact_requires_complete_cases_receipts_and_cleanup() {
        for adapter in ["axum", "fastly", "cloudflare", "spin"] {
            let valid = artifact(adapter);
            validate_artifact(&valid, adapter).unwrap();
            for missing in 0..valid.len() {
                let mut changed = valid.clone();
                changed.remove(missing);
                assert!(validate_artifact(&changed, adapter).is_err());
            }
            let mut duplicate = valid.clone();
            duplicate[1] = duplicate[0].clone();
            assert!(validate_artifact(&duplicate, adapter).is_err());
            for (field, value) in [
                ("adapter", json!("other")),
                ("token", json!("foreign")),
                ("case", json!(99)),
                ("status", json!("fail")),
                ("qualification", json!(true)),
            ] {
                let mut changed = valid.clone();
                changed[0][field] = value;
                assert!(validate_artifact(&changed, adapter).is_err(), "{field}");
            }
            for (field, value) in [
                ("accept_encoding", json!(["gzip"])),
                ("token", json!("other")),
                ("port", json!(4002)),
                ("slot", json!(["0"])),
            ] {
                let mut changed = valid.clone();
                changed[0]["origins"][0][field] = value;
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "origin {field}"
                );
            }
            for (field, value) in [
                ("cleanup", json!("fail")),
                ("accepted", json!(0)),
                ("origin_requests", json!(0)),
            ] {
                let mut changed = valid.clone();
                changed.last_mut().unwrap()[field] = value;
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "inventory {field}"
                );
            }
        }
    }

    #[test]
    fn encoding_report_mutations_cannot_hide_in_matching_http_bytes() {
        let valid = artifact("spin");
        for (field, value) in [
            ("etag", json!("changed")),
            ("vary", json!(null)),
            ("token", json!("other")),
            ("streamed", json!(true)),
            ("bytes", json!([0])),
            ("content_encoding", json!("gzip")),
        ] {
            let mut changed = valid.clone();
            changed[0]["report"][field] = value;
            changed[0]["client"]["body_bytes"] =
                json!(serde_json::to_vec(&changed[0]["report"]).unwrap());
            assert!(validate_artifact(&changed, "spin").is_err(), "{field}");
        }
        let mut changed = valid.clone();
        changed[8]["report"]["slots"]
            .as_array_mut()
            .unwrap()
            .swap(0, 1);
        changed[8]["client"]["body_bytes"] =
            json!(serde_json::to_vec(&changed[8]["report"]).unwrap());
        assert!(validate_artifact(&changed, "spin").is_err());
        let mut relay = artifact("cloudflare");
        relay[9]["client"]["body_bytes"] = json!(b"decoded".as_slice());
        assert!(validate_artifact(&relay, "cloudflare").is_err());
    }

    #[test]
    fn encoding_artifact_rejects_an_appended_cleanup_failure() {
        let mut records = artifact("spin");
        records.push(json!({"event":"outbound-encoding-cleanup","adapter":"spin","status":"fail","qualification":false}));
        assert!(validate_artifact(&records, "spin").is_err());
    }

    #[test]
    fn encoding_observation_retains_interrupted_clients_and_partial_receipts() {
        let valid = artifact("spin");
        let mut receipts: std::collections::VecDeque<_> = vec![
            Ok(valid[8]["origins"][0].clone()),
            Err("second receipt failed".into()),
        ]
        .into();
        let client = Err(Box::new(net::InterruptedResponse {
            status: 200,
            headers: vec![],
            body: b"partial".to_vec(),
            reason: "fixture interruption".to_owned(),
            cause: net::ResponseInterruption::PrematureEof,
        }) as Box<dyn std::error::Error + Send + Sync>);
        let (event, result) = observation("spin", 8, "encoding-8", 4001, client, || {
            receipts.pop_front().unwrap()
        })
        .unwrap();
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("fixture interruption")
        );
        assert_eq!(event["origins"].as_array().unwrap().len(), 1);
        assert_eq!(
            event["partial_client"]["body_bytes"],
            json!(b"partial".as_slice())
        );
        assert_eq!(event["receipt_error"], "second receipt failed");
        assert_eq!(event["status"], "fail");
        let mut receipts = valid[0]["origins"].as_array().unwrap().clone().into_iter();
        let response = json!({"status":200,"body_bytes":b"not JSON".as_slice()});
        let (event, result) =
            observation("spin", 0, "encoding-0", 4001, Ok(response.clone()), || {
                Ok(receipts.next().unwrap())
            })
            .unwrap();
        assert!(result.is_err());
        assert_eq!(event["client"], response);
        assert!(event["report_error"].as_str().is_some());
    }
}

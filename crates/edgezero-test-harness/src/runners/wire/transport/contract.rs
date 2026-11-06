#![allow(
    clippy::duplicate_mod,
    reason = "private origin and evidence modules compile the same immutable fixture inventory without exporting either module tree"
)]

use super::*;

#[path = "../../../../../edgezero-core/src/outbound/tests/buffered_parity/oracle.rs"]
mod oracle;
#[path = "../../../../../edgezero-core/src/outbound/tests/buffered_parity/vectors.rs"]
pub(super) mod vectors;

const COHORT_PAIRS: usize = 32;

pub(super) fn policy_cases() -> impl Iterator<Item = (bool, &'static str, bool, u8)> {
    vectors::POLICY_CASES.iter().map(|case| {
        let vectors::Kind::Policy {
            coding,
            declared,
            size,
        } = case.kind
        else {
            unreachable!("policy inventory")
        };
        (
            case.streamed,
            if coding == vectors::Coding::Passthrough {
                "passthrough"
            } else {
                vectors::coding_name(coding)
            },
            declared,
            size,
        )
    })
}

fn validate_native(
    value: &Value,
    case: vectors::Case,
    adapter: &str,
    mode: &str,
    uri: &str,
) -> Result<()> {
    if !case.expects_origin(adapter) {
        return native_rejected(value);
    }
    require(
        value["attachments"] == 1
            && value["invalid"] == false
            && value["preparations"] == 1
            && value["prepared"] == json!({"mode":mode,"target":uri})
            && value["operation_acquisitions"] == 1
            && value["operation_releases"] == 1
            && value["operation_polls"]
                .as_u64()
                .is_some_and(|count| count > 0),
        "paired request lacks actual native preparation or operation release",
    )
}

fn validate_case(event: &Value, adapter: &str, index: usize, ports: [u64; 2]) -> Result<()> {
    let case = vectors::all()
        .nth(index)
        .ok_or("unknown paired contract index")?;
    require(
        vectors::find(case.name).is_some(),
        "missing paired case specification",
    )?;
    let token = event["token"]
        .as_str()
        .filter(|token| {
            !token.is_empty()
                && token.len() <= 64
                && token
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .ok_or("invalid paired contract token")?;
    let target = path(case.name, token);
    let uri = format!("http://127.0.0.1:{}{target}", ports[0]);
    require(
        event["event"] == "native-parity-case"
            && event["adapter"] == adapter
            && event["case"] == case.name
            && event["index"] == index
            && event["cohort"] == index / COHORT_PAIRS
            && event["status"] == "pass"
            && event["qualification"] == false
            && event["uri"] == uri,
        "paired case identity differs",
    )?;
    let report = &event["report"];
    require(
        report["adapter"] == adapter
            && report["case"] == case.name
            && report["token"] == token
            && report["qualification"] == false
            && report["steps"]
                .as_array()
                .is_some_and(|steps| steps.len() == 1),
        "foreign paired guest report",
    )?;
    let pair = &report["steps"][0];
    require(
        pair.as_object().is_some_and(|value| value.len() == 4)
            && pair["step"] == "paired-contract"
            && pair["schema"] == 1,
        "wrong paired report schema",
    )?;
    oracle::validate_pair(case, adapter, token, pair)?;
    for (label, mode) in [("send", "Send"), ("begin", "Begin")] {
        validate_native(&pair[label]["native"], case, adapter, mode, &uri)?;
    }
    let wanted = if case.expects_origin(adapter) { 2 } else { 0 };
    let receipts = event["origin"]
        .as_array()
        .ok_or("missing paired origin receipts")?;
    require(
        receipts.len() == wanted && event["accepted_connections"] == wanted,
        "paired origin acceptance differs",
    )?;
    for (receipt, step) in receipts.iter().zip(["send", "begin"]) {
        require(
            receipt["port"] == ports[0]
                && receipt["method"] == case.request_method()
                && receipt["target"] == target
                && receipt["token"] == token
                && receipt["step"] == step
                && receipt["payload"] == case.request_body()
                && receipt["proof"] == json!([])
                && receipt["signature"] == json!([])
                && receipt.get("head_delay_us") == Some(&Value::Null),
            "paired origin receipt reassociated",
        )?;
        net::validate_forward_capture(receipt)?;
        let lengths = receipt["content_length"]
            .as_array()
            .ok_or("missing paired origin lengths")?;
        let encodings = receipt["transfer_encoding"]
            .as_array()
            .ok_or("missing paired origin encodings")?;
        require(
            lengths.len() <= 1
                && encodings.len() <= 1
                && (lengths.is_empty() || encodings.is_empty())
                && (case.request_method() == "POST"
                    || encodings.is_empty()
                        && lengths.iter().all(|length| length == "0")
                        && receipt["encoded_body_hex"] == ""),
            "ambiguous paired origin framing",
        )?;
        let fields = &receipt["request_fields"];
        let target_authority = json!([format!("127.0.0.1:{}", ports[0])]);
        let override_authority = json!(["virtual.example:8443"]);
        let authority_matches = if case.kind == vectors::Kind::Authority {
            if adapter == "cloudflare" {
                // BestEffort accepts the setting, without promising Fetch honors Host.
                (fields["host"] == target_authority || fields["host"] == override_authority)
                    && fields["host"] == receipts[0]["request_fields"]["host"]
            } else {
                fields["host"] == override_authority
            }
        } else {
            fields["host"] == target_authority
        };
        require(
            authority_matches
                && fields["cache_control"]
                    == if adapter == "cloudflare" && case.kind == vectors::Kind::Cache {
                        json!(["no-cache"])
                    } else {
                        json!([])
                    }
                && fields["private"] == json!([])
                && fields["proxy_connection"] == json!([])
                && fields["accept_encoding"]
                    == if matches!(case.kind, vectors::Kind::Coding { .. }) {
                        json!(["gzip, deflate, br"])
                    } else {
                        json!(["identity"])
                    },
            "paired request fields or accepted options changed",
        )?;
        let response = &receipt["response"];
        let reply = case.reply(token);
        require(
            response["closed_before_head"] == reply.close_head
                && response["incomplete"] == reply.close_body
                && response["status"] == reply.status
                && response["cancelled"].is_boolean(),
            "paired origin response evidence differs",
        )?;
        if reply.close_head {
            require(
                response.get("head_at_us") == Some(&Value::Null) && response["body_written"] == 0,
                "native head error was not exercised",
            )?;
        } else {
            require(
                response["head_at_us"].as_u64().is_some(),
                "missing paired head-write anchor",
            )?;
            if reply.head_delay_ms != 0 {
                require(
                    response["head_at_us"]
                        .as_u64()
                        .is_some_and(|value| value >= reply.head_delay_ms * 1000),
                    "head timeout origin answered early",
                )?;
            }
            if reply.body_delay_ms != 0 {
                require(
                    response["body_at_us"]
                        .as_u64()
                        .is_some_and(|value| value >= reply.body_delay_ms * 1000),
                    "body timeout origin answered early",
                )?;
            }
            if !case.cancels_response() {
                require(
                    response["cancelled"] == false && response["body_written"] == reply.body.len(),
                    "paired origin lost response bytes",
                )?;
            }
        }
    }
    let observed: Value = serde_json::from_slice(&body(&event["response"])?)?;
    require(
        event["response"]["status"] == 200 && observed == *report,
        "paired HTTP bytes and report differ",
    )
}

pub(super) fn validate_artifact(events: &[Value], adapter: &str) -> Result<()> {
    require(
        events.iter().all(|event| event["status"] != "fail"),
        "failed paired evidence",
    )?;
    let inventories = events
        .iter()
        .filter(|event| event["event"] == "native-parity-inventory")
        .collect::<Vec<_>>();
    let [inventory] = inventories.as_slice() else {
        return Err("missing or duplicate full paired inventory".into());
    };
    let cases = vectors::all().collect::<Vec<_>>();
    let cohorts = inventory["cohorts"]
        .as_array()
        .ok_or("missing paired origin cohorts")?;
    let requests = cases
        .iter()
        .filter(|case| case.expects_origin(adapter))
        .count()
        * 2;
    require(
        inventory["adapter"] == adapter
            && inventory["status"] == "pass"
            && inventory["qualification"] == false
            && inventory["pairs"] == cases.len()
            && inventory["exchanges"] == cases.len() * 2
            && inventory["origin_requests"] == requests
            && inventory["accepted_connections"] == requests
            && inventory["base_policy_pairs"] == 18
            && inventory["cohort_limit"] == COHORT_PAIRS
            && cohorts.len() == cases.len().div_ceil(COHORT_PAIRS),
        "full paired inventory differs",
    )?;
    let mut ports = Vec::new();
    for (index, (cohort, chunk)) in cohorts.iter().zip(cases.chunks(COHORT_PAIRS)).enumerate() {
        let selected: [u64; 2] = serde_json::from_value(cohort["origin_ports"].clone())?;
        require(
            selected[0] != selected[1]
                && selected.iter().all(|port| (1..=65535).contains(port))
                && cohort["index"] == index
                && cohort["cases"] == json!(chunk.iter().map(|case| case.name).collect::<Vec<_>>())
                && cohort["origin_requests"]
                    == chunk
                        .iter()
                        .filter(|case| case.expects_origin(adapter))
                        .count()
                        * 2
                && cohort["accepted_connections"] == cohort["origin_requests"]
                && cohort["cleanup"] == "complete",
            "paired cohort identity or cleanup differs",
        )?;
        ports.push(selected);
    }
    let mut seen = HashSet::new();
    let mut tokens = HashSet::new();
    for event in events
        .iter()
        .filter(|event| event["event"] == "native-parity-case")
    {
        let index = event["index"]
            .as_u64()
            .and_then(|index| usize::try_from(index).ok())
            .ok_or("invalid paired index")?;
        require(
            index < cases.len()
                && seen.insert(index)
                && tokens.insert(event["token"].clone().to_string()),
            "duplicate or foreign paired case",
        )?;
        validate_case(event, adapter, index, ports[index / COHORT_PAIRS])
            .map_err(|error| format!("{adapter}/{}: {error}", cases[index].name))?;
    }
    require(seen.len() == cases.len(), "missing full paired cases")
}

pub(super) fn run(adapter: &str, port: u16, records: &mut Records) -> Result<()> {
    let cases = vectors::all().collect::<Vec<_>>();
    let mut cohorts = Vec::new();
    let mut accepted_total = 0;
    for (index, chunk) in cases.chunks(COHORT_PAIRS).enumerate() {
        let origins = WireOrigins::start()?;
        let ports = [origins.port(0), origins.port(1)];
        let outcome = (|| -> Result<()> {
            for (offset, case) in chunk.iter().enumerate() {
                let token = token();
                let before = origins.accepted();
                let uri = format!("http://127.0.0.1:{}{}", ports[0], path(case.name, &token));
                let response = net::header_request(
                    port,
                    &format!("/progress/transport/{}", case.name),
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
                let origin = (0..if case.expects_origin(adapter) { 2 } else { 0 })
                    .map(|_| origins.take_wire())
                    .collect::<Result<Vec<_>>>()?;
                let event = json!({"event":"native-parity-case","adapter":adapter,"case":case.name,"index":index*COHORT_PAIRS+offset,"cohort":index,
                    "token":token,"uri":uri,"report":report,"response":response,"origin":origin,
                    "accepted_connections":origins.accepted()-before,"status":"pass","qualification":false});
                records.push(event.clone())?;
                validate_case(
                    &event,
                    adapter,
                    index * COHORT_PAIRS + offset,
                    ports.map(u64::from),
                )
                .map_err(|error| format!("{adapter}/{}: {error}", case.name))?;
            }
            Ok(())
        })();
        let cleanup = origins.finish_counted();
        if let Err(error) = &cleanup {
            records.push(json!({"adapter":adapter,"status":"fail","stage":"paired-origin-cleanup","error":error.to_string(),"qualification":false}))?;
        }
        outcome?;
        let accepted = cleanup?;
        accepted_total += accepted;
        cohorts.push(json!({"index":index,"origin_ports":ports,"cases":chunk.iter().map(|case|case.name).collect::<Vec<_>>(),
            "origin_requests":chunk.iter().filter(|case|case.expects_origin(adapter)).count()*2,"accepted_connections":accepted,"cleanup":"complete"}));
    }
    records.push(json!({"event":"native-parity-inventory","adapter":adapter,"pairs":cases.len(),"exchanges":cases.len()*2,
        "base_policy_pairs":18,"cohort_limit":COHORT_PAIRS,"cohorts":cohorts,
        "origin_requests":cases.iter().filter(|case|case.expects_origin(adapter)).count()*2,"accepted_connections":accepted_total,"status":"pass","qualification":false}))?;
    validate_artifact(
        &super::super::super::ci::read_events(&records.path)?,
        adapter,
    )
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    fn headers(case: vectors::Case, adapter: &str, token: &str) -> Vec<(String, Vec<u8>)> {
        let reply = case.reply(token);
        let decoded = matches!(
            case.kind,
            vectors::Kind::Policy {
                coding: vectors::Coding::Gzip,
                declared: false,
                ..
            } | vectors::Kind::Coding {
                original: false,
                partial: false,
                ..
            } | vectors::Kind::Rechunk {
                coding: vectors::Coding::Gzip,
                original: false
            } | vectors::Kind::Error(
                vectors::ErrorCase::BrotliWindow
                    | vectors::ErrorCase::DecoderMemory
                    | vectors::ErrorCase::Gzip
            )
        );
        let mut fields = reply
            .fields
            .into_iter()
            .filter(|(name, _)| {
                !matches!(
                    *name,
                    "connection" | "proxy-connection" | "x-edgezero-proxy"
                ) && (*name != "x-private" || adapter == "spin")
                    && (!decoded || *name != "content-encoding")
            })
            .map(|(name, value)| (name.to_owned(), value.into_bytes()))
            .collect::<Vec<_>>();
        if let Some(length) = reply.declared.filter(|_| !decoded) {
            fields.push(("content-length".to_owned(), length.to_string().into_bytes()));
        }
        let marker = match case.kind {
            vectors::Kind::Marker(vectors::MarkerCase::Off | vectors::MarkerCase::InheritOff) => {
                Some("upstream")
            }
            vectors::Kind::Marker(vectors::MarkerCase::On) => Some(adapter),
            _ => case.client_marker().then_some(adapter),
        };
        if let Some(value) = marker {
            fields.push(("x-edgezero-proxy".to_owned(), value.as_bytes().to_vec()));
        }
        fields.sort_unstable();
        fields
    }

    fn native(case: vectors::Case, adapter: &str, uri: &str, begin: bool) -> Value {
        let eligible = case.expects_origin(adapter);
        json!({"attachments":1,"invalid":false,"preparations":u64::from(eligible),"prepared":if eligible {json!({"mode":if begin {"Begin"} else {"Send"},"target":uri})} else {Value::Null},
            "operation_acquisitions":u64::from(eligible),"operation_polls":u64::from(eligible),"operation_releases":u64::from(eligible),
            "source_acquisitions":u64::from(eligible),"source_releases":u64::from(eligible),"heads":u64::from(eligible),"reads":u64::from(eligible),"waits":0})
    }

    fn capture(case: vectors::Case, adapter: &str, token: &str, uri: &str, begin: bool) -> Value {
        let fields = headers(case, adapter, token);
        let reply = case.reply(token);
        let failure = oracle::error_spec(case, adapter);
        let body_error = failure.is_some() && oracle::body_error(case);
        let streamed = if failure.is_some() && !body_error {
            Value::Null
        } else {
            json!(case.streamed && case.kind != vectors::Kind::Bodyless205)
        };
        let outcome = if let Some((kind, status, reason, cause)) = failure {
            let provider = if matches!(
                case.kind,
                vectors::Kind::Error(vectors::ErrorCase::Head | vectors::ErrorCase::Body)
            ) {
                let name = match adapter {
                    "axum" => "Axum",
                    "cloudflare" => "Cloudflare",
                    "fastly" => "Fastly",
                    "spin" => "Spin",
                    _ => unreachable!(),
                };
                Some(
                    json!({"adapter":name,"phase":if case.kind == vectors::Kind::Error(vectors::ErrorCase::Head) {if adapter == "fastly" {"ResponseHeaders"} else {"Send"}} else {"ResponseBodyRead"},"cause":"Unknown"}),
                )
            } else {
                None
            };
            json!({"error":{"kind":kind,"status":status,"reason":if case.kind==vectors::Kind::Error(vectors::ErrorCase::Head)&&adapter!="cloudflare" {Some("Transport")} else {reason},"cause":cause,
                "provider":provider,"rejection":if case.kind==vectors::Kind::GetBody {Some("get_body")} else if case.kind==vectors::Kind::Authority&&adapter=="spin" {Some("unsupported_authority")} else {None},
                "upload_origin":if status==413 {Some("OutboundUpload")} else {None}}})
        } else {
            json!({"status":reply.status,"headers":fields,"bytes":oracle::expected_bytes(case)})
        };
        let head = if failure.is_some() && !body_error {
            Value::Null
        } else {
            json!({"status":reply.status,"headers":fields})
        };
        let bytes = oracle::expected_bytes(case);
        let chunks =
            if case.streamed && failure.is_none() && case.kind != vectors::Kind::Bodyless205 {
                bytes
                    .chunks(if matches!(case.kind, vectors::Kind::Rechunk { .. }) {
                        2
                    } else {
                        bytes.len()
                    })
                    .map(|chunk| chunk.to_vec())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
        json!({"outcome":outcome,"request":oracle::request_spec(case,adapter,token),"client_marker":case.client_marker(),
            "head":head,"head_us":if failure.is_some()&&!body_error {None} else {Some(1000_u64)},
            "terminal_us":if matches!(case.kind,vectors::Kind::Delay{..}) {150_000_u64} else if case.kind==vectors::Kind::StreamLive {250_000} else {1000},
            "reads_at_head":if failure.is_some()&&!body_error {None} else {Some(0_u64)},"streamed":streamed,
            "stage":if failure.is_none() {"complete"} else if body_error {"body"} else {"operation"},"chunks":chunks,"native":native(case,adapter,uri,begin)})
    }

    pub(crate) fn artifact(adapter: &str) -> Vec<Value> {
        let cases = vectors::all().collect::<Vec<_>>();
        let mut events = Vec::new();
        let mut cohorts = Vec::new();
        for (cohort, chunk) in cases.chunks(COHORT_PAIRS).enumerate() {
            let ports = [5001 + cohort * 2, 5002 + cohort * 2];
            for (offset, case) in chunk.iter().enumerate() {
                let index = cohort * COHORT_PAIRS + offset;
                let token = format!("paired-{index}");
                let target = path(case.name, &token);
                let uri = format!("http://127.0.0.1:{}{target}", ports[0]);
                let pair = json!({"step":"paired-contract","schema":1,"send":capture(*case,adapter,&token,&uri,false),"begin":capture(*case,adapter,&token,&uri,true)});
                let report = json!({"adapter":adapter,"case":case.name,"token":token,"qualification":false,"steps":[pair]});
                let reply = case.reply(&token);
                let origin = if case.expects_origin(adapter) {
                    ["send","begin"].into_iter().map(|step|json!({
                    "port":ports[0],"method":case.request_method(),"target":target,"token":token,"step":step,"payload":case.request_body(),
                    "content_length":[case.request_body().len().to_string()],"transfer_encoding":[],"encoded_body_hex":case.request_body().as_bytes().iter().map(|byte|format!("{byte:02x}")).collect::<String>(),
                    "proof":[],"signature":[],"head_delay_us":null,"request_fields":{"host":if case.kind==vectors::Kind::Authority {vec!["virtual.example:8443".to_owned()]} else {vec![format!("127.0.0.1:{}",ports[0])]},
                        "cache_control":if adapter=="cloudflare"&&case.kind==vectors::Kind::Cache {vec!["no-cache"]} else {vec![]},"private":[],"proxy_connection":[],"accept_encoding":if matches!(case.kind,vectors::Kind::Coding{..}) {vec!["gzip, deflate, br"]} else {vec!["identity"]}},
                    "response":{"head_at_us":if reply.close_head {None} else {Some(reply.head_delay_ms*1000+1)},"body_at_us":if reply.close_head {None} else {Some(reply.body_delay_ms*1000+1)},
                        "body_written":if reply.close_head {0} else {reply.body.len()},"closed_before_head":reply.close_head,"incomplete":reply.close_body,"cancelled":false,"status":reply.status}})).collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                events.push(json!({"event":"native-parity-case","adapter":adapter,"case":case.name,"index":index,"cohort":cohort,"token":token,"uri":uri,
                    "report":report,"response":{"status":200,"body_bytes":report.to_string().as_bytes()},"origin":origin,"accepted_connections":origin.len(),"status":"pass","qualification":false}));
            }
            let count = chunk
                .iter()
                .filter(|case| case.expects_origin(adapter))
                .count()
                * 2;
            cohorts.push(json!({"index":cohort,"origin_ports":ports,"cases":chunk.iter().map(|case|case.name).collect::<Vec<_>>(),"origin_requests":count,"accepted_connections":count,"cleanup":"complete"}));
        }
        let count = cases
            .iter()
            .filter(|case| case.expects_origin(adapter))
            .count()
            * 2;
        events.push(json!({"event":"native-parity-inventory","adapter":adapter,"pairs":cases.len(),"exchanges":cases.len()*2,"base_policy_pairs":18,
            "cohort_limit":COHORT_PAIRS,"cohorts":cohorts,"origin_requests":count,"accepted_connections":count,"status":"pass","qualification":false}));
        events
    }

    #[test]
    fn full_paired_contract_requires_every_vector_mode_and_exact_origin_cohort() {
        for adapter in ["axum", "cloudflare", "fastly", "spin"] {
            let valid = artifact(adapter);
            validate_artifact(&valid, adapter).unwrap();
            for omitted in 0..valid.len() {
                let mut changed = valid.clone();
                changed.remove(omitted);
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "{adapter}/{omitted}"
                );
            }
            for pointer in [
                "/report/steps/0/send",
                "/report/steps/0/begin",
                "/report/steps/0/begin/request/mode",
                "/origin/0/token",
                "/origin/0/port",
                "/origin/0/step",
            ] {
                let mut changed = valid.clone();
                *changed[0].pointer_mut(pointer).unwrap() = Value::Null;
                changed[0]["response"]["body_bytes"] =
                    json!(changed[0]["report"].to_string().as_bytes());
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "{adapter}/{pointer}"
                );
            }
            let mut changed = valid.clone();
            changed.push(json!({"status":"fail"}));
            assert!(validate_artifact(&changed, adapter).is_err());
        }
    }

    #[test]
    fn full_paired_contract_cache_bypass_keeps_provider_specific_wire_settings() {
        for adapter in ["axum", "cloudflare", "fastly", "spin"] {
            let mut changed = artifact(adapter);
            let index = vectors::all()
                .position(|case| case.name == "pair-cache-bypass")
                .unwrap();
            for receipt in changed[index]["origin"].as_array_mut().unwrap() {
                receipt["request_fields"]["cache_control"] = if adapter == "cloudflare" {
                    json!([])
                } else {
                    json!(["no-cache"])
                };
            }
            assert!(validate_artifact(&changed, adapter).is_err(), "{adapter}");
        }
    }

    #[test]
    fn full_paired_contract_requires_equal_besteffort_authority_and_rejects_foreign_hosts() {
        let valid = artifact("cloudflare");
        let index = vectors::all()
            .position(|case| case.name == "pair-authority")
            .unwrap();
        let target = valid[index]["uri"]
            .as_str()
            .unwrap()
            .split('/')
            .nth(2)
            .unwrap();
        let mut target_form = valid.clone();
        for receipt in target_form[index]["origin"].as_array_mut().unwrap() {
            receipt["request_fields"]["host"] = json!([target]);
        }
        validate_artifact(&target_form, "cloudflare").unwrap();
        let mut mixed = target_form;
        mixed[index]["origin"][1]["request_fields"]["host"] = json!(["virtual.example:8443"]);
        assert!(validate_artifact(&mixed, "cloudflare").is_err());
        let mut foreign = valid;
        for receipt in foreign[index]["origin"].as_array_mut().unwrap() {
            receipt["request_fields"]["host"] = json!(["foreign.example"]);
        }
        assert!(validate_artifact(&foreign, "cloudflare").is_err());
    }

    #[test]
    fn full_paired_contract_preserves_the_documented_spin_hidden_nomination_boundary() {
        for adapter in ["axum", "cloudflare", "fastly", "spin"] {
            let mut changed = artifact(adapter);
            let index = vectors::all()
                .position(|case| case.name == "pair-normalize-b")
                .unwrap();
            for mode in ["send", "begin"] {
                for pointer in ["outcome", "head"] {
                    let fields = changed[index]["report"]["steps"][0][mode][pointer]["headers"]
                        .as_array_mut()
                        .unwrap();
                    if adapter == "spin" {
                        fields.retain(|field| field[0] != "x-private");
                    } else {
                        fields.push(json!(["x-private", b"removed".as_slice()]));
                    }
                }
            }
            changed[index]["response"]["body_bytes"] =
                json!(changed[index]["report"].to_string().as_bytes());
            assert!(validate_artifact(&changed, adapter).is_err(), "{adapter}");
        }
    }

    #[test]
    fn full_paired_contract_rejects_forged_heads_and_typed_timing_evidence() {
        for adapter in ["axum", "cloudflare", "fastly", "spin"] {
            let valid = artifact(adapter);
            let mut changed = valid.clone();
            for mode in ["send", "begin"] {
                changed[0]["report"]["steps"][0][mode]["head"]["status"] = json!(201);
            }
            changed[0]["response"]["body_bytes"] =
                json!(changed[0]["report"].to_string().as_bytes());
            assert!(
                validate_artifact(&changed, adapter).is_err(),
                "{adapter}/forged matching heads"
            );
            let rejected = vectors::all()
                .position(|case| case.name == "pair-get-body")
                .unwrap();
            let mut changed = valid.clone();
            for mode in ["send", "begin"] {
                changed[rejected]["report"]["steps"][0][mode]["outcome"]["error"]["rejection"] =
                    Value::Null;
            }
            changed[rejected]["response"]["body_bytes"] =
                json!(changed[rejected]["report"].to_string().as_bytes());
            assert!(
                validate_artifact(&changed, adapter).is_err(),
                "{adapter}/missing matching GET body rejection"
            );
            for (name, pointer, value) in [
                (
                    "pair-policy-b-identity-10",
                    "/report/steps/0/send/head/status",
                    json!(201),
                ),
                (
                    "pair-policy-b-identity-10",
                    "/report/steps/0/begin/head/headers",
                    json!([]),
                ),
                (
                    "pair-head-call-timeout",
                    "/report/steps/0/send/outcome/error/cause",
                    json!("RequestDeadline"),
                ),
                (
                    "pair-body-s-deadline",
                    "/report/steps/0/begin/terminal_us",
                    json!(149_999),
                ),
                (
                    "pair-body-b-deadline",
                    "/report/steps/0/send/terminal_us",
                    json!(500_000),
                ),
                (
                    "pair-stream-live",
                    "/report/steps/0/send/head_us",
                    json!(250_000),
                ),
                (
                    "pair-error-head-b",
                    "/report/steps/0/send/outcome/error/provider/adapter",
                    json!("Wrong"),
                ),
                (
                    "pair-error-body-s",
                    "/report/steps/0/begin/outcome/error/provider/phase",
                    json!("Send"),
                ),
                (
                    "pair-error-body-b",
                    "/report/steps/0/send/outcome/error/provider",
                    Value::Null,
                ),
                (
                    "pair-get-body",
                    "/report/steps/0/send/outcome/error/rejection",
                    Value::Null,
                ),
                (
                    "pair-upload-over",
                    "/report/steps/0/send/outcome/error/rejection",
                    json!("get_body"),
                ),
                (
                    "pair-head-call-timeout",
                    "/origin/0/response/head_at_us",
                    json!(1000),
                ),
                (
                    "pair-body-b-call-timeout",
                    "/origin/1/response/body_at_us",
                    json!(1000),
                ),
            ] {
                let index = vectors::all().position(|case| case.name == name).unwrap();
                let mut changed = valid.clone();
                *changed[index].pointer_mut(pointer).unwrap() = value;
                changed[index]["response"]["body_bytes"] =
                    json!(changed[index]["report"].to_string().as_bytes());
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "{adapter}/{name}/{pointer}"
                );
            }
            let mut changed = valid.clone();
            changed.last_mut().unwrap()["cohorts"][0]["cleanup"] = json!("pending");
            assert!(validate_artifact(&changed, adapter).is_err());
        }
    }

    #[test]
    fn full_paired_contract_rejects_matching_but_wrong_outcomes_and_eager_streams() {
        for adapter in ["axum", "cloudflare", "fastly", "spin"] {
            let valid = artifact(adapter);
            for index in 0..valid.len() - 1 {
                let mut changed = valid.clone();
                for mode in ["send", "begin"] {
                    changed[index]["report"]["steps"][0][mode]["outcome"] = json!({});
                }
                changed[index]["response"]["body_bytes"] =
                    json!(changed[index]["report"].to_string().as_bytes());
                assert!(
                    validate_artifact(&changed, adapter).is_err(),
                    "{adapter}/{index}"
                );
            }
            let index = vectors::all()
                .position(|case| case.streamed && oracle::error_spec(case, adapter).is_none())
                .unwrap();
            let mut changed = valid.clone();
            for mode in ["send", "begin"] {
                changed[index]["report"]["steps"][0][mode]["reads_at_head"] = json!(1);
            }
            changed[index]["response"]["body_bytes"] =
                json!(changed[index]["report"].to_string().as_bytes());
            assert!(validate_artifact(&changed, adapter).is_err());
        }
    }
}

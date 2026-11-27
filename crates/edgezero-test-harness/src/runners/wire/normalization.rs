use super::*;

const REPRESENTATION_FIELDS: [(&str, &str); 9] = [
    ("content-encoding", "gzip"),
    ("content-length", "23"),
    ("content-range", "bytes 0-22/46"),
    ("etag", "\"strong\""),
    ("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
    ("vary", "Accept-Encoding"),
    ("accept-ranges", "bytes"),
    ("cache-control", "public, max-age=60"),
    (
        "content-digest",
        "sha-256=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:",
    ),
];

fn case_path(index: usize, token: &str) -> String {
    format!(
        "/p5/representation/{}/{token}",
        if index >= 4 { 206 } else { 200 }
    )
}

fn validate_report(index: usize, value: &Value) -> Result<()> {
    require(
        index < 8 && value["case"] == index,
        "invalid normalization case",
    )?;
    let report = &value["report"];
    let outcome = &report["outcome"];
    let raw = index & 2 != 0 || index >= 4;
    require(
        value["request"]
            == json!({
            "decoding":if index & 2 != 0 {"original"} else {"decode"},
            "streamed":index & 1 != 0,"buffered_cap":if index & 1 != 0 {None} else {Some(256)},
                    "decoded_cap":if raw {1} else {11},"encoded_cap":256,
                    "decoder_cap":if raw {0} else {32 * 1024 * 1024},
                }),
        "normalization effective request settings differ",
    )?;
    require(
        report["source"]["released"] == true
            && report["source"]["polls"]
                .as_u64()
                .is_some_and(|polls| polls > 0)
            && report["source"]["items"].as_u64().is_some_and(|items| {
                items > 0 && Some(items) <= report["source"]["polls"].as_u64()
            })
            && report.get("error_stage") == Some(&Value::Null)
            && outcome["status"] == if index >= 4 { 206 } else { 200 }
            && outcome["streamed"] == (index & 1 != 0)
            && outcome["bytes"]
                == json!(if raw {
                    POLICY_GZIP.to_vec()
                } else {
                    vec![b'a'; 11]
                })
            && outcome.get("error").is_none(),
        "normalization bytes, mode, status or source ownership differs",
    )?;
    let pairs: Vec<(String, Vec<u8>)> = serde_json::from_value(outcome["headers"].clone())?;
    let mut fields = std::collections::BTreeMap::new();
    for (name, bytes) in pairs {
        require(
            fields.insert(name.to_ascii_lowercase(), bytes).is_none(),
            "duplicate normalization field",
        )?;
    }
    let mut expected = std::collections::BTreeMap::new();
    expected.insert("date".to_owned(), b"Tue, 27 Oct 2026 00:00:00 GMT".to_vec());
    expected.insert(
        "content-type".to_owned(),
        b"application/octet-stream".to_vec(),
    );
    expected.insert("x-p5-fixture".to_owned(), b"spin-wire".to_vec());
    for (name, value) in REPRESENTATION_FIELDS {
        if raw || !matches!(name, "content-encoding" | "content-length") {
            expected.insert(name.to_owned(), value.as_bytes().to_vec());
        }
    }
    require(
        fields == expected,
        "normalized representation fields differ",
    )
}

pub(super) fn validate_artifact(events: &[Value], origin_port: u16) -> Result<()> {
    let mut cases = Vec::new();
    for event in events
        .iter()
        .filter(|event| event["event"] == "spin-normalization-case")
    {
        let index = event["case"]
            .as_u64()
            .and_then(|index| usize::try_from(index).ok())
            .ok_or("missing normalization index")?;
        let token = event["token"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or("missing normalization token")?;
        let path = case_path(index, token);
        let receipts: Vec<(u16, String)> =
            serde_json::from_value(event["origin_requests"].clone())?;
        require(
            event["qualification"] == false
                && event["path"] == path
                && receipts.len() == 1
                && origin_port > 0
                && receipts[0].0 == origin_port
                && receipts[0].1 == path,
            "foreign normalization receipt",
        )?;
        let response = &event["response"];
        let observed: Value = serde_json::from_slice(&body(response)?)?;
        require(
            response["status"] == 200 && observed == event["report"],
            "normalization HTTP response and report disagree",
        )?;
        validate_report(index, &event["report"])?;
        cases.push(index);
    }
    cases.sort_unstable();
    require(
        cases == (0..8).collect::<Vec<_>>(),
        "incomplete or duplicate normalization cases",
    )?;
    let inventories = events
        .iter()
        .filter(|event| event["event"] == "spin-normalization-inventory")
        .collect::<Vec<_>>();
    require(
        inventories.len() == 1
            && inventories[0]["status"] == "pass"
            && inventories[0]["qualification"] == false
            && inventories[0]["exchanges"] == 8
            && inventories[0]["origin_requests"] == 8,
        "invalid normalization inventory",
    )
}

pub(super) fn run(port: u16, origins: &WireOrigins, records: &mut Records) -> Result<()> {
    for index in 0..8 {
        require(
            origins.take_requests().is_empty(),
            "request escaped preceding normalization case",
        )?;
        let token = token();
        let path = case_path(index, &token);
        let uri = format!("http://127.0.0.1:{}{path}", origins.port(0));
        let response = net::header_request(
            port,
            "/normalization",
            &token,
            &[
                ("x-origin", &uri),
                ("x-normalization-case", &index.to_string()),
            ],
            None,
        )?;
        let observed = origins.take_requests();
        let report: Result<Value> =
            body(&response).and_then(|bytes| Ok(serde_json::from_slice(&bytes)?));
        records.push(json!({"event":"spin-normalization-case","case":index,"token":token,"path":path,"origin_requests":observed,"report":report.as_ref().ok(),"response":response,"qualification":false}))?;
        require(
            response["status"] == 200 && observed == vec![(origins.port(0), path)],
            "normalization exchange or receipts differ",
        )?;
        validate_report(index, &report?)?;
    }
    records.push(json!({"event":"spin-normalization-inventory","status":"pass","exchanges":8,"origin_requests":8,"qualification":false}))?;
    Ok(())
}

const TRAILER_CASES: [(&str, bool); 4] = [
    ("buffered", false),
    ("buffered", true),
    ("streamed", false),
    ("streamed", true),
];

fn trailer_limit(mode: &str, exceeded: bool, native: &Value) -> Result<u64> {
    let (field, added) = if mode == "buffered" {
        ("head_count", 2)
    } else {
        ("head_bytes", 24)
    };
    native[field]
        .as_u64()
        .and_then(|value| value.checked_add(added))
        .and_then(|value| value.checked_sub(u64::from(exceeded)))
        .ok_or_else(|| "invalid native trailer accounting".into())
}

pub(super) fn validate_trailers(events: &[Value]) -> Result<()> {
    let native = events
        .iter()
        .filter(|event| event["event"] == "spin-native-trailers")
        .collect::<Vec<_>>();
    require(
        native.len() == 1,
        "missing or duplicate native trailer control",
    )?;
    let native = native[0];
    let report = &native["report"];
    require(
        report["qualification"] == false
            && report["head_count"] == 2
            && report["head_bytes"] == 49
            && report["trailer_count"] == 2
            && report["trailer_bytes"] == 24
            && report["bytes"] == json!([b'x'])
            && report["trailers"] == json!([["x-trailer", b"one"], ["x-trailer", b"two"]]),
        "pinned Spin host did not expose the expected native trailer fields",
    )?;
    let origin = native["origin_requests"][0][0]
        .as_u64()
        .filter(|port| (1..=u64::from(u16::MAX)).contains(port))
        .ok_or("missing trailer origin port")?;
    let cases = events
        .iter()
        .filter(|event| event["event"] == "spin-trailer-limit")
        .collect::<Vec<_>>();
    require(
        cases.len() == TRAILER_CASES.len(),
        "incomplete trailer matrix",
    )?;
    for event in std::iter::once(native).chain(cases.iter().copied()) {
        let token = event["token"]
            .as_str()
            .filter(|token| !token.is_empty())
            .ok_or("missing trailer request identity")?;
        let path = format!("/p5/trailers/{token}");
        let observed: Value = serde_json::from_slice(&body(&event["response"])?)?;
        require(
            event["qualification"] == false
                && event["path"] == path
                && event["origin_requests"] == json!([[origin, path]])
                && event["response"]["status"] == 200
                && observed == event["report"],
            "trailer HTTP report, identity or receipts disagree",
        )?;
    }
    let mut tokens = std::collections::BTreeSet::new();
    tokens.insert(native["token"].as_str().ok_or("native identity")?);
    for (mode, exceeded) in TRAILER_CASES {
        let matching = cases
            .iter()
            .filter(|event| event["mode"] == mode && event["exceeded"] == exceeded)
            .collect::<Vec<_>>();
        require(
            matching.len() == 1,
            "missing or duplicate trailer policy case",
        )?;
        let event = matching[0];
        require(
            tokens.insert(event["token"].as_str().ok_or("trailer identity")?),
            "reused trailer request identity",
        )?;
        let actual = &event["report"];
        require(
            event["limit"] == trailer_limit(mode, exceeded, report)?
                && actual["source"]["released"] == true
                && actual["source"]["polls"]
                    .as_u64()
                    .is_some_and(|polls| polls > 0),
            "trailer effective policy or release differs",
        )?;
        if exceeded {
            require(
                actual["error_stage"] == if mode == "buffered" { "send" } else { "body" }
                    && actual["outcome"]["error"]["kind"] == "response_too_large"
                    && actual["outcome"]["error"]["status"] == 502
                    && actual["outcome"]["error"]["reason"]
                        == if mode == "buffered" {
                            "HeaderCount"
                        } else {
                            "HeaderBytes"
                        },
                "exposed trailers bypassed cumulative header accounting",
            )?;
        } else {
            require(
                actual["error_stage"] == Value::Null
                    && actual["outcome"]["status"] == 200
                    && actual["outcome"]["bytes"] == json!([b'x'])
                    && actual["outcome"]["streamed"] == (mode == "streamed")
                    && actual["outcome"].get("error").is_none(),
                "exact cumulative trailer cap rejected or changed payload",
            )?;
        }
    }
    let inventories = events
        .iter()
        .filter(|event| event["event"] == "spin-trailer-inventory")
        .collect::<Vec<_>>();
    require(
        inventories.len() == 1
            && inventories[0]["status"] == "pass"
            && inventories[0]["qualification"] == false
            && inventories[0]["exchanges"] == 5
            && inventories[0]["origin_requests"] == 5,
        "invalid trailer inventory",
    )
}

pub(super) fn run_trailers(port: u16, records: &mut Records) -> Result<()> {
    let start = records.values.len();
    let origins = WireOrigins::start()?;
    let outcome = (|| -> Result<()> {
        let native_token = token();
        let path = format!("/p5/trailers/{native_token}");
        let uri = format!("http://127.0.0.1:{}{path}", origins.port(0));
        let response = net::header_request(
            port,
            "/trailers-native",
            &native_token,
            &[("x-origin", &uri)],
            None,
        )?;
        let native: Value = serde_json::from_slice(&body(&response)?)?;
        let receipts = origins.take_requests();
        records.push(
            json!({"event":"spin-native-trailers","response":response,"token":native_token,
            "report":native,"path":path,"origin_requests":receipts,"qualification":false}),
        )?;
        require(
            response["status"] == 200
                && receipts == vec![(origins.port(0), path)]
                && native["trailer_count"] == 2
                && native["trailer_bytes"] == 24
                && native["bytes"] == json!([b'x']),
            "pinned Spin host did not expose the expected native trailer fields",
        )?;
        for (mode, exceeded) in TRAILER_CASES {
            let limit = trailer_limit(mode, exceeded, &native)?;
            let token = token();
            let path = format!("/p5/trailers/{token}");
            let uri = format!("http://127.0.0.1:{}{path}", origins.port(0));
            let response = net::header_request(
                port,
                &format!("/trailers-{mode}"),
                &token,
                &[("x-origin", &uri), ("x-trailer-limit", &limit.to_string())],
                None,
            )?;
            let report: Value = serde_json::from_slice(&body(&response)?)?;
            let receipts = origins.take_requests();
            records.push(json!({"event":"spin-trailer-limit","mode":mode,"limit":limit,"exceeded":exceeded,"token":token,
                "response":response,"report":report,"path":path,"origin_requests":receipts,"qualification":false}))?;
        }
        Ok(())
    })();
    let cleanup = origins.finish_counted();
    outcome?;
    require(
        cleanup? == 5,
        "trailer controls left extra or incomplete origin work",
    )?;
    records.push(
        json!({"event":"spin-trailer-inventory","exchanges":5,"origin_requests":5,
        "status":"pass","qualification":false}),
    )?;
    validate_trailers(&records.values[start..])
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    pub(crate) fn trailer_artifact() -> Vec<Value> {
        let mut events = Vec::new();
        let native = json!({"qualification":false,"head_count":2,"head_bytes":49,
            "trailer_count":2,"trailer_bytes":24,"bytes":[b'x'],
            "trailers":[["x-trailer",b"one"],["x-trailer",b"two"]]});
        for (index, (mode, exceeded)) in std::iter::once(("native", false))
            .chain(TRAILER_CASES)
            .enumerate()
        {
            let token = format!("trailer-{index}");
            let path = format!("/p5/trailers/{token}");
            let report = if mode == "native" {
                native.clone()
            } else {
                json!({"source":{"released":true,"polls":3},
                    "error_stage":if exceeded {Some(if mode == "buffered" {"send"}else{"body"})}else{None},
                    "outcome":if exceeded {json!({"error":{"kind":"response_too_large","status":502,
                        "reason":if mode == "buffered" {"HeaderCount"}else{"HeaderBytes"}}})}
                    else{json!({"status":200,"bytes":[b'x'],"streamed":mode == "streamed"})}})
            };
            events.push(json!({"event":if mode == "native" {"spin-native-trailers"}else{"spin-trailer-limit"},
                "mode":mode,"exceeded":exceeded,"limit":trailer_limit(mode,exceeded,&native).unwrap(),
                "token":token,"path":path,"origin_requests":[[1,path]],"report":report,
                "response":{"status":200,"body_bytes":serde_json::to_vec(&report).unwrap()},"qualification":false}));
        }
        events.push(
            json!({"event":"spin-trailer-inventory","status":"pass","qualification":false,
            "exchanges":5,"origin_requests":5}),
        );
        events
    }

    #[test]
    fn persisted_trailers_require_native_exposure_cumulative_limits_and_cleanup() {
        let events = trailer_artifact();
        validate_trailers(&events).unwrap();
        for index in 0..events.len() {
            let mut missing = events.clone();
            missing.remove(index);
            assert!(validate_trailers(&missing).is_err());
            let mut duplicate = events.clone();
            duplicate.push(events[index].clone());
            assert!(validate_trailers(&duplicate).is_err());
        }
        for (index, pointer, replacement) in [
            (0, "/report/trailer_count", json!(0)),
            (0, "/report/trailer_bytes", json!(23)),
            (0, "/report/trailers/1/1", json!(b"wrong")),
            (1, "/limit", json!(999)),
            (1, "/report/outcome/bytes", json!([])),
            (2, "/report/outcome/error/reason", json!("HeaderBytes")),
            (3, "/report/source/released", json!(false)),
            (4, "/report/error_stage", json!("send")),
            (4, "/report/outcome/error/kind", json!("internal")),
            (5, "/origin_requests", json!(4)),
        ] {
            let mut changed = events.clone();
            *changed[index].pointer_mut(pointer).unwrap() = replacement;
            if index < 5 {
                changed[index]["response"]["body_bytes"] =
                    json!(serde_json::to_vec(&changed[index]["report"]).unwrap());
            }
            assert!(validate_trailers(&changed).is_err(), "{index} {pointer}");
        }
    }

    pub(crate) fn artifact(origin_port: u16) -> Vec<Value> {
        let mut events = Vec::new();
        for index in 0..8 {
            let raw = index & 2 != 0 || index >= 4;
            let token = format!("case-{index}");
            let path = case_path(index, &token);
            let mut fields = vec![
                ("date", b"Tue, 27 Oct 2026 00:00:00 GMT".to_vec()),
                ("content-type", b"application/octet-stream".to_vec()),
                ("x-p5-fixture", b"spin-wire".to_vec()),
            ];
            fields.extend(
                REPRESENTATION_FIELDS
                    .iter()
                    .filter(|(name, _)| {
                        raw || !matches!(*name, "content-encoding" | "content-length")
                    })
                    .map(|(name, value)| (*name, value.as_bytes().to_vec())),
            );
            let report = json!({"case":index,"request":{
                "decoding":if index & 2 != 0 {"original"} else {"decode"},
                "streamed":index & 1 != 0,"buffered_cap":if index & 1 != 0 {None} else {Some(256)},
                "decoded_cap":if raw {1} else {11},"encoded_cap":256,
                "decoder_cap":if raw {0} else {32 * 1024 * 1024},
            },"report":{
                "outcome":{"status":if index >= 4 {206}else{200},"headers":fields,
                "bytes":if raw {POLICY_GZIP.to_vec()}else{vec![b'a';11]},"streamed":index & 1 != 0},
                "error_stage":null,"source":{"polls":2,"items":1,"released":true}}});
            events.push(
                json!({"event":"spin-normalization-case","case":index,"token":token,
                "path":path,"origin_requests":[[origin_port,path]],"report":report,
                "response":{"status":200,"body_bytes":serde_json::to_vec(&report).unwrap()},"qualification":false}),
            );
        }
        events.push(json!({"event":"spin-normalization-inventory","status":"pass","exchanges":8,"origin_requests":8,"qualification":false}));
        events
    }

    #[test]
    fn persisted_normalization_requires_complete_unique_cases_and_inventory() {
        let valid = artifact(1);
        validate_artifact(&valid, 1).unwrap();
        for missing in 0..valid.len() {
            let mut changed = valid.clone();
            changed.remove(missing);
            assert!(
                validate_artifact(&changed, 1).is_err(),
                "missing case {missing}"
            );
        }
        let mut duplicate = valid;
        duplicate[0] = duplicate[1].clone();
        assert!(validate_artifact(&duplicate, 1).is_err());
    }

    #[test]
    fn persisted_normalization_rejects_altered_ownership_fields_bytes_and_receipts() {
        let valid = artifact(1);
        for (pointer, value) in [
            ("/case", json!(8)),
            ("/report/case", json!(1)),
            ("/report/report/outcome/status", json!(206)),
            ("/report/report/outcome/streamed", json!(true)),
            ("/report/report/outcome/bytes", json!([0])),
            ("/report/report/outcome/headers", json!([])),
            ("/report/report/error_stage", json!("body")),
            ("/report/report/source/polls", json!(0)),
            ("/report/report/source/released", json!(false)),
            ("/origin_requests/0/0", json!(0)),
            ("/origin_requests/0/0", json!(2)),
            ("/origin_requests/0/1", json!("/foreign")),
            ("/path", json!("/foreign")),
            ("/qualification", json!(true)),
            ("/response/status", json!(500)),
            ("/response/body_bytes", json!([])),
            ("/report/request/decoding", json!("original")),
            ("/report/request/decoded_cap", json!(256)),
        ] {
            let mut changed = valid.clone();
            *changed[0].pointer_mut(pointer).unwrap() = value;
            if pointer.starts_with("/report/") {
                changed[0]["response"]["body_bytes"] =
                    json!(serde_json::to_vec(&changed[0]["report"]).unwrap());
            }
            assert!(
                validate_artifact(&changed, 1).is_err(),
                "accepted {pointer}"
            );
        }
        for mutation in ["drop", "duplicate", "change", "restored"] {
            let mut changed = valid.clone();
            let fields = changed[2]["report"]["report"]["outcome"]["headers"]
                .as_array_mut()
                .unwrap();
            match mutation {
                "drop" => {
                    fields.pop();
                }
                "duplicate" => {
                    fields.push(fields[0].clone());
                }
                "change" => {
                    fields[0][1] = json!([0]);
                }
                _ => {
                    fields.push(json!(["connection", b"close".as_slice()]));
                }
            }
            changed[2]["response"]["body_bytes"] =
                json!(serde_json::to_vec(&changed[2]["report"]).unwrap());
            assert!(
                validate_artifact(&changed, 1).is_err(),
                "accepted header {mutation}"
            );
        }
    }

    #[test]
    fn persisted_normalization_cannot_relabel_original_as_decode_for_partial_content() {
        let valid = artifact(1);
        for (decode, original) in [(4, 6), (5, 7)] {
            let mut changed = valid.clone();
            changed[decode]["report"] = valid[original]["report"].clone();
            changed[decode]["report"]["case"] = json!(decode);
            changed[decode]["response"]["body_bytes"] =
                json!(serde_json::to_vec(&changed[decode]["report"]).unwrap());
            assert!(validate_artifact(&changed, 1).is_err());
        }
    }
}

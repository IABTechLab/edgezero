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

#[cfg(test)]
pub(super) mod tests {
    use super::*;

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

use super::*;

const CASES: [(&str, u16); 5] = [
    ("GET", 200),
    ("GET", 204),
    ("HEAD", 200),
    ("GET", 304),
    ("GET", 302),
];

fn case(index: usize) -> Result<(&'static str, u16, &'static str, String, String)> {
    require(index < CASES.len() * 2, "unknown marker case")?;
    let (method, status) = CASES[index / 2];
    let setting = if index & 1 == 0 { "inherit" } else { "on" };
    let path = format!("/marker/{}/{status}/absent", method.to_ascii_lowercase());
    let name = format!(
        "marker-{}-{status}-absent-{setting}",
        method.to_ascii_lowercase()
    );
    Ok((method, status, setting, path, name))
}

pub(super) fn validate_artifact(events: &[Value], adapter: &str) -> Result<()> {
    require(
        events.iter().all(|event| event["status"] != "fail"),
        "failed marker evidence",
    )?;
    let inventories: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "outbound-marker-inventory")
        .collect();
    let [inventory] = inventories.as_slice() else {
        return Err("missing or duplicate marker inventory".into());
    };
    let origin = inventory["origin_port"]
        .as_u64()
        .filter(|port| (1..=u64::from(u16::MAX)).contains(port))
        .ok_or("invalid marker origin")?;
    require(
        inventory["adapter"] == adapter
            && inventory["qualification"] == false
            && inventory["status"] == "pass"
            && inventory["cases"] == 10
            && inventory["origin_requests"] == 10,
        "invalid marker inventory",
    )?;
    let mut cases = Vec::new();
    let mut tokens = HashSet::new();
    for event in events
        .iter()
        .filter(|event| event["event"] == "outbound-marker-case")
    {
        let index = event["case"]
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .ok_or("invalid marker index")?;
        let (method, status, _, path, name) = case(index)?;
        let token = event["token"]
            .as_str()
            .filter(|value| {
                !value.is_empty()
                    && value.len() <= 64
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
            .ok_or("invalid marker token")?;
        require(tokens.insert(token), "duplicate marker correlation")?;
        require(
            event["adapter"] == adapter
                && event["status"] == "pass"
                && event["qualification"] == false
                && event["path"] == path
                && event["origin"]
                    == json!({"port":origin,"method":method,"target":path,"token":token}),
            "foreign marker receipt",
        )?;
        let report = &event["report"];
        let enabled = index & 1 != 0;
        require(
            report
                == &json!({"case":name,"status":status,"markers":if enabled {vec![adapter]} else {vec![]},
            "enabled":enabled,"request":{"method":method,"setting":if enabled {"On"} else {"Inherit"},"client_default":false,"token":token},"qualification":false}),
            "marker status, values or effective policy differ",
        )?;
        let observed: Value = serde_json::from_slice(&body(&event["response"])?)?;
        require(
            event["response"]["status"] == 200 && observed == *report,
            "marker HTTP bytes and report disagree",
        )?;
        cases.push(index);
    }
    cases.sort_unstable();
    require(
        cases == (0..10).collect::<Vec<_>>(),
        "missing or duplicate marker case",
    )
}

pub(super) fn run(adapter: &str, port: u16, records: &mut Records) -> Result<()> {
    let origins = WireOrigins::start()?;
    let origin_port = origins.port(0);
    let outcome = (|| -> Result<()> {
        for index in 0..10 {
            let (method, _, setting, path, name) = case(index)?;
            let token = token();
            let uri = format!("http://127.0.0.1:{origin_port}{path}");
            let response = if adapter == "spin" {
                net::header_request(
                    port,
                    "/marker",
                    &token,
                    &[
                        ("x-origin", &uri),
                        ("x-marker-method", method),
                        ("x-marker-setting", setting),
                        ("x-marker-case", &name),
                    ],
                    None,
                )?
            } else {
                net::binary_request(port, &format!("/progress/{name}"), &token, Some(&uri))?
            };
            let origin = origins.take_wire()?;
            let report: Value = serde_json::from_slice(&body(&response)?)?;
            records.push(json!({"event":"outbound-marker-case","adapter":adapter,"case":index,"token":token,"path":path,
                "origin":origin,"report":report,"response":response,"status":"pass","qualification":false}))?;
        }
        Ok(())
    })();
    let cleanup = origins.finish();
    if let Err(error) = &cleanup {
        records.push(json!({"adapter":adapter,"status":"fail","stage":"marker-origin-cleanup","error":error.to_string(),"qualification":false}))?;
    }
    outcome?;
    cleanup?;
    records.push(json!({"event":"outbound-marker-inventory","adapter":adapter,"origin_port":origin_port,"cases":10,"origin_requests":10,"status":"pass","qualification":false}))?;
    validate_artifact(&super::super::ci::read_events(&records.path)?, adapter)?;
    println!("ez_7_acceptance_04_{adapter}_runtime: passed");
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    pub(crate) fn artifact(adapter: &str) -> Vec<Value> {
        let mut events = Vec::new();
        for index in 0..10 {
            let (method, status, _, path, name) = case(index).unwrap();
            let enabled = index & 1 != 0;
            let token = format!("marker-{index}");
            let report = json!({"case":name,"status":status,"markers":if enabled {vec![adapter]} else {vec![]},
                "enabled":enabled,"request":{"method":method,"setting":if enabled {"On"} else {"Inherit"},"client_default":false,"token":token},"qualification":false});
            events.push(json!({"event":"outbound-marker-case","adapter":adapter,"case":index,"token":token,"path":path,
                "origin":{"port":4001,"method":method,"target":path,"token":token},
                "report":report,"response":{"status":200,"body_bytes":report.to_string().as_bytes()},"status":"pass","qualification":false}));
        }
        events.push(json!({"event":"outbound-marker-inventory","adapter":adapter,"origin_port":4001,"cases":10,"origin_requests":10,"status":"pass","qualification":false}));
        events
    }

    #[test]
    fn marker_artifact_requires_exact_receipts_and_reports() {
        for adapter in ["axum", "fastly", "cloudflare", "spin"] {
            validate_artifact(&artifact(adapter), adapter).unwrap();
        }
        let valid = artifact("spin");
        validate_artifact(&valid, "spin").unwrap();
        for omitted in 0..valid.len() {
            let mut changed = valid.clone();
            changed.remove(omitted);
            assert!(validate_artifact(&changed, "spin").is_err());
        }
        for (field, replacement) in [
            ("case", json!(10)),
            ("token", json!("other")),
            ("path", json!("/other")),
            ("adapter", json!("fastly")),
            ("status", json!("fail")),
            ("qualification", json!(true)),
            (
                "response",
                json!({"status":200,"body_bytes":b"{}".as_slice()}),
            ),
        ] {
            let mut changed = valid.clone();
            changed[0][field] = replacement;
            assert!(
                validate_artifact(&changed, "spin").is_err(),
                "accepted {field}"
            );
        }
        for (field, replacement) in [
            ("port", json!(4002)),
            ("method", json!("HEAD")),
            ("target", json!("/other")),
            ("token", json!("other")),
        ] {
            let mut changed = valid.clone();
            changed[0]["origin"][field] = replacement;
            assert!(
                validate_artifact(&changed, "spin").is_err(),
                "accepted origin {field}"
            );
        }
        for index in 0..valid.len() {
            let mut changed = valid.clone();
            changed.push(valid[index].clone());
            assert!(validate_artifact(&changed, "spin").is_err());
        }
        for (field, replacement) in [
            ("cases", json!(9)),
            ("origin_requests", json!(11)),
            ("adapter", json!("fastly")),
            ("qualification", json!(true)),
            ("status", json!("ignored")),
            ("origin_port", json!(0)),
            ("origin_port", json!(65_536)),
            ("origin_port", Value::Null),
        ] {
            let mut changed = valid.clone();
            changed[10][field] = replacement;
            assert!(
                validate_artifact(&changed, "spin").is_err(),
                "accepted inventory {field}"
            );
        }
        for (field, replacement) in [
            ("status", json!(302)),
            ("enabled", json!(true)),
            ("markers", json!(["upstream"])),
            (
                "request",
                json!({"method":"HEAD","setting":"Inherit","client_default":false}),
            ),
        ] {
            let mut changed = valid.clone();
            changed[0]["report"][field] = replacement;
            changed[0]["response"]["body_bytes"] =
                json!(changed[0]["report"].to_string().as_bytes());
            assert!(
                validate_artifact(&changed, "spin").is_err(),
                "accepted report {field}"
            );
        }
        let mut changed = valid.clone();
        changed[1] = changed[0].clone();
        assert!(validate_artifact(&changed, "spin").is_err());
        for field in ["origin", "report", "response"] {
            for donor in [1, 4] {
                let mut reassociated = valid.clone();
                reassociated[0][field] = reassociated[donor][field].clone();
                assert!(
                    validate_artifact(&reassociated, "spin").is_err(),
                    "reassociated {field}"
                );
            }
        }
        for (container, fields) in [
            ("origin", &["method", "target", "port", "token"][..]),
            (
                "report",
                &[
                    "case",
                    "status",
                    "markers",
                    "enabled",
                    "request",
                    "qualification",
                ][..],
            ),
            ("response", &["status", "body_bytes"][..]),
        ] {
            for field in fields {
                let mut omitted = valid.clone();
                omitted[0][container]
                    .as_object_mut()
                    .unwrap()
                    .remove(*field);
                assert!(
                    validate_artifact(&omitted, "spin").is_err(),
                    "missing {container}.{field}"
                );
            }
        }
        let mut changed = valid;
        changed.push(json!({"status":"fail","stage":"marker-origin-cleanup"}));
        assert!(validate_artifact(&changed, "spin").is_err());
    }
}

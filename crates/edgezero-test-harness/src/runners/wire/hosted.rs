use super::*;

pub(super) fn run(port: u16, records: &mut Records) -> Result<()> {
    let revision = records
        .values
        .iter()
        .rev()
        .find(|record| record["event"] == "build" && record["variant"] == "fastly-qualification")
        .and_then(|record| record["revision"].as_str())
        .ok_or("missing qualification build identity")?
        .to_owned();
    let build_input = startup::build_id()?;
    let origin = Backend::start()?;
    let mut expected = Vec::new();
    for (driver, case, timeout, count) in [
        ("candidate", "long-budget", Some(90_000), 1),
        ("candidate", "body-buffered", Some(500), 1),
        ("candidate", "body-streamed", Some(500), 1),
        ("candidate", "connect-default", Some(2000), 1),
        ("candidate", "cache-send", Some(1000), 2),
        ("candidate", "cache-batch", Some(1000), 2),
        ("candidate", "reuse", Some(1000), 1),
        ("native", "reuse", Some(1000), 1),
    ] {
        let token = token();
        let path = format!("/p5/{token}/{}/final", origin.port);
        let target = origin.url(&path);
        let reply = net::header_request(
            port,
            &format!("/hosted/{driver}/{case}"),
            &token,
            &[("x-qualification-target", &target)],
            None,
        )?;
        require(
            reply["status"] == 200,
            "qualification package did not dispatch hosted profile",
        )?;
        let report = if driver == "native" {
            let fields = reply["headers"]
                .as_array()
                .ok_or("missing SDK control headers")?;
            let field = fields
                .iter()
                .find(|field| field[0] == "x-qualification-report")
                .ok_or("missing SDK control report")?;
            serde_json::from_str(field[1].as_str().ok_or("invalid SDK control report")?)?
        } else {
            serde_json::from_slice(&body(&reply)?)?
        };
        validate(
            &report,
            driver,
            case,
            &token,
            timeout,
            count,
            (&revision, &build_input),
        )?;
        expected.extend(std::iter::repeat_n(path, count));
        records.push(json!({"event":"qualification-package-local","driver":driver,"case":case,
            "report":report,"status":"pass","qualification":false,
            "limitations":"local settings/routing only; no hosted delay, caching, TLS reuse or optimizer parity credit"}))?;
    }
    let target = origin.url("/must-not-dispatch");
    for headers in [
        vec![(
            "x-qualification-target",
            "https://unapproved.invalid/private-token",
        )],
        vec![
            ("x-qualification-target", target.as_str()),
            ("x-qualification-target", target.as_str()),
        ],
    ] {
        let reply = net::header_request(port, "/hosted/candidate/reuse", &token(), &headers, None)?;
        require(
            reply["status"] == 400 && body(&reply)? == b"invalid hosted qualification probe\n",
            "qualification inputs did not fail closed",
        )?;
    }
    require(
        *origin.receipts.lock().unwrap() == expected,
        "qualification package origin receipts differ",
    )
}

fn validate(
    report: &Value,
    driver: &str,
    case: &str,
    token: &str,
    timeout: Option<u64>,
    count: usize,
    identity: (&str, &str),
) -> Result<()> {
    require(
        report["driver"] == driver
            && report["case"] == case
            && report["execution"] == token
            && report["qualification"] == false
            && report["blocking_horizon"] == "Unknown"
            && report["settings"]["timeout_ms"] == json!(timeout)
            && report["revision"] == identity.0
            && report["build_input_sha256"] == identity.1,
        "qualification package identity or settings differ",
    )?;
    if driver == "native" {
        return require(
            report["status"] == 200 && report["host_call"].is_object(),
            "SDK control did not execute",
        );
    }
    let outcomes = report["outcomes"]
        .as_array()
        .ok_or("missing qualification outcomes")?;
    require(
        outcomes.len() == count
            && outcomes.iter().all(|row| {
                row["status"] == 200
                    && row["bytes"] == 10
                    && row.get("error").is_none()
                    && row["preparation"].is_object()
                    && row["terminal_us"].is_number()
            }),
        "qualification package lost a terminal outcome",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn package_validation_requires_real_outcomes_and_identity_without_claiming_hosted_credit() {
        let report = json!({"driver":"candidate","case":"cache-batch","execution":"probe",
            "revision":"a".repeat(40),"build_input_sha256":"b".repeat(64),"qualification":false,
            "blocking_horizon":"Unknown","settings":{"timeout_ms":1000},
            "outcomes":[{"status":200,"bytes":10,"preparation":{},"terminal_us":1},{"status":200,"bytes":10,"preparation":{},"terminal_us":2}]});
        let revision = "a".repeat(40);
        let build_input = "b".repeat(64);
        let identity = (revision.as_str(), build_input.as_str());
        validate(
            &report,
            "candidate",
            "cache-batch",
            "probe",
            Some(1000),
            2,
            identity,
        )
        .unwrap();
        for field in ["outcomes", "revision", "settings"] {
            let mut invalid = report.clone();
            invalid[field] = Value::Null;
            assert!(
                validate(
                    &invalid,
                    "candidate",
                    "cache-batch",
                    "probe",
                    Some(1000),
                    2,
                    identity
                )
                .is_err()
            );
        }
        for (field, length) in [("revision", 40), ("build_input_sha256", 64)] {
            let mut invalid = report.clone();
            invalid[field] = json!("c".repeat(length));
            assert!(
                validate(
                    &invalid,
                    "candidate",
                    "cache-batch",
                    "probe",
                    Some(1000),
                    2,
                    identity
                )
                .is_err()
            );
        }
    }
}

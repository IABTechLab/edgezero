use super::*;

const MAX_BODY: u64 = 128 * 1024;
const MAX_HEAD: u64 = 16 * 1024;

pub(super) fn run(name: &str, parent: &Path, records: &mut Records) -> Result<()> {
    let run = parent.join(format!("framing-{name}"));
    fs::create_dir_all(&run)?;
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(fixture_root())
        .output()?;
    require(revision.status.success(), "missing framing source revision")?;
    let revision = String::from_utf8(revision.stdout)?.trim().to_owned();
    let Some((mut command, port)) =
        progress::build(name, &run, records, &revision, "response-framing")?
    else {
        return Ok(());
    };
    let log = run.join("runtime.log");
    let outcome = Process::start(&mut command, &log, port)
        .and_then(|_process| checks(name, port, &run, records));
    settle(
        records,
        &log,
        json!({"adapter":name,"suite":"response-framing"}),
        json!({"phase":"response-framing"}),
        outcome,
    )
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    require(
        fs::metadata(path)?.len() <= limit,
        "framing capture exceeds its fixed bound",
    )?;
    Ok(fs::read(path)?)
}

fn parse_head(bytes: &[u8]) -> Result<Value> {
    require(
        bytes.ends_with(b"\r\n\r\n"),
        "truncated framing header section",
    )?;
    let text = std::str::from_utf8(bytes)?;
    let mut lines = text.trim_end_matches("\r\n").split("\r\n");
    let mut status = lines
        .next()
        .ok_or("missing HTTP framing status")?
        .split_whitespace();
    let protocol = status.next().ok_or("missing HTTP framing version")?;
    let status: u16 = status
        .next()
        .ok_or("missing framing status code")?
        .parse()?;
    let mut headers = Vec::new();
    for line in lines {
        let (name, value) = line.split_once(':').ok_or("invalid framing header")?;
        headers.push(json!([name.to_ascii_lowercase(), value.trim()]));
    }
    let values = |key: &str| {
        headers
            .iter()
            .filter(|pair| pair[0] == key)
            .map(|pair| pair[1].clone())
            .collect::<Vec<_>>()
    };
    let lengths = values("content-length");
    let transfer = values("transfer-encoding");
    require(
        lengths.len() <= 1 && transfer.len() <= 1 && (lengths.is_empty() || transfer.is_empty()),
        "ambiguous response framing",
    )?;
    let length = if let Some(value) = lengths.first() {
        Some(
            value
                .as_str()
                .ok_or("non-text content-length")?
                .parse::<u64>()?,
        )
    } else {
        None
    };
    Ok(
        json!({"protocol":protocol,"status":status,"headers":headers,"length":length,"transfer":transfer.first()}),
    )
}

fn request(port: u16, path: &str, head: bool, h2: bool, run: &Path) -> Result<Value> {
    let budget = startup::remaining(port, Duration::from_secs(5))?;
    let id = token();
    let header_path = run.join(format!("{id}.headers"));
    let body_path = run.join(format!("{id}.body"));
    fs::write(&header_path, [])?;
    fs::write(&body_path, [])?;
    let mut command = Command::new(std::env::var_os("CURL_BIN").unwrap_or_else(|| "curl".into()));
    command
        .args([
            "--silent",
            "--show-error",
            "--noproxy",
            "*",
            "--max-time",
            &budget.as_secs_f64().to_string(),
            "--max-filesize",
            "131072",
        ])
        .arg(if h2 {
            "--http2-prior-knowledge"
        } else {
            "--http1.1"
        })
        .args(["--header", "Accept-Encoding: identity", "--dump-header"])
        .arg(&header_path)
        .arg("--output")
        .arg(&body_path);
    if head {
        command.arg("--head");
    }
    let output = command
        .arg(format!("http://127.0.0.1:{port}{path}"))
        .output()?;
    let mut response = capture(
        head,
        &header_path,
        &body_path,
        output.status.success(),
        &output.stderr,
    )?;
    response["curl_exit"] = json!(output.status.code());
    if startup::check(port)?.is_some() && !output.status.success() {
        startup::reject(port, "first prescribed curl response did not complete");
        startup::check(port)?;
    }
    startup::observe(
        port,
        path,
        response["headers"].as_array().unwrap_or(&Vec::new()),
    )?;
    startup::annotate_response(port, &mut response);
    Ok(response)
}

fn capture(
    head: bool,
    header_path: &Path,
    body_path: &Path,
    success: bool,
    diagnostics: &[u8],
) -> Result<Value> {
    let raw_head = read_bounded(header_path, MAX_HEAD)?;
    let mut response = match parse_head(&raw_head) {
        Ok(response) => response,
        Err(error) => {
            let mut incomplete = json!({"head_parse_error":error.to_string()});
            if raw_head.ends_with(b"\r\n") {
                let mut diagnostic = raw_head.clone();
                diagnostic.extend_from_slice(b"\r\n");
                if let Ok(partial) = parse_head(&diagnostic) {
                    incomplete["incomplete_head"] = partial;
                }
            }
            incomplete
        }
    };
    let bytes = read_bounded(body_path, MAX_BODY)?;
    response["body"] = if head {
        Value::Null
    } else {
        json!(std::str::from_utf8(&bytes)?)
    };
    response["body_observation"] = json!(if head { "header-only" } else { "captured" });
    response["curl_success"] = json!(success);
    response["head_artifact"] = artifact(header_path)?;
    response["capture_artifact"] = artifact(body_path)?;
    let diagnostic_path = header_path.with_extension("curl-stderr");
    fs::write(
        &diagnostic_path,
        &diagnostics[..diagnostics.len().min(4096)],
    )?;
    response["diagnostic_artifact"] = artifact(&diagnostic_path)?;
    response["diagnostic_truncated"] = json!(diagnostics.len() > 4096);
    Ok(response)
}

fn validate(response: &Value, adapter: &str, case: &str, h2: bool) -> Result<()> {
    if case == "short-stream" && response["head_artifact"]["bytes"] == 0 {
        return require(
            response["curl_success"] == false
                && response["curl_exit"] == if h2 { 92 } else { 52 }
                && response["body"] == "",
            "headless short-source abort lacks an empty-reply witness",
        );
    }
    require(
        response["protocol"] == if h2 { "HTTP/2" } else { "HTTP/1.1" },
        "framing probe negotiated the wrong protocol or lost its head",
    )?;
    if case == "short-stream" {
        let partial = response["body"]
            .as_str()
            .ok_or("missing partial body observation")?;
        return require(
            response["curl_success"] == false
                && response["curl_exit"] == if h2 { 92 } else { 18 }
                && response["status"] == 200
                && response["length"] == 7
                && b"abcdef".starts_with(partial.as_bytes()),
            "short source did not terminate as an incomplete response",
        );
    }
    require(
        response["curl_success"] == true,
        "controlled framing request failed; see bounded curl diagnostics",
    )?;
    let (status, body) = match case {
        "empty" => (200, ""),
        "trailers" | "mismatch" => (500, "internal server error"),
        "expired" => (504, "response write deadline exceeded"),
        "not-modified" | "not-modified-explicit" => (304, ""),
        "no-content" => (204, ""),
        "reset-content" => (205, ""),
        "head" | "head-explicit" => (200, ""),
        "rendered-not-found" => (
            404,
            response["body"]
                .as_str()
                .ok_or("missing rendered 404 body")?,
        ),
        _ => (200, "abcdef"),
    };
    require(
        response["status"] == status
            && if case.starts_with("head") {
                response["body"].is_null() && response["body_observation"] == "header-only"
            } else {
                response["body"] == body
            },
        "framing response status/body mismatch",
    )?;
    if case == "rendered-not-found" {
        let rendered: Value = serde_json::from_str(body)?;
        require(
            rendered["error"]["status"] == 404,
            "404 did not pass through framework rendering",
        )?;
    }
    if matches!(
        case,
        "head"
            | "head-explicit"
            | "not-modified"
            | "not-modified-explicit"
            | "no-content"
            | "reset-content"
    ) {
        if case == "no-content" {
            require(
                response["length"].is_null() && response["transfer"].is_null(),
                "204 carried forbidden transfer framing",
            )?;
        }
        if case == "reset-content" {
            require(
                (response["length"].is_null() || response["length"] == 0)
                    && (response["transfer"].is_null()
                        || (!h2 && response["transfer"] == "chunked")),
                "205 carried nonzero or invalid transfer framing",
            )?;
        }
        if adapter == "fastly" && case != "no-content" && case != "reset-content" {
            require(
                response["length"].is_null() || response["length"] == 6,
                "Fastly replaced a known representation length with the suppressed payload size",
            )?;
        }
        if adapter != "fastly" {
            let eligible = if case == "reset-content" {
                response["length"].is_null() || response["length"] == 0
            } else if case == "not-modified-explicit" {
                response["length"].is_null() || response["length"] == 6
            } else if case.ends_with("explicit") {
                response["length"] == 6
            } else {
                response["length"].is_null() || (case == "head" && response["length"] == 0)
            };
            require(
                eligible,
                "suppressed response gained or lost a representation length",
            )?;
        }
    } else if case == "streamed" {
        require(
            response["length"].is_null()
                && response["transfer"] == if h2 { Value::Null } else { json!("chunked") },
            "unknown stream acquired guessed framing",
        )?;
    } else {
        require(
            response["length"] == body.len() && response["transfer"].is_null(),
            "buffered/declared response lacks exact framing",
        )?;
    }
    Ok(())
}

fn checks(adapter: &str, port: u16, run: &Path, records: &mut Records) -> Result<()> {
    let curl = Command::new(std::env::var_os("CURL_BIN").unwrap_or_else(|| "curl".into()))
        .arg("--version")
        .output()?;
    require(
        curl.status.success(),
        "curl unavailable for response-framing probes",
    )?;
    let version = String::from_utf8(curl.stdout)?;
    require(
        adapter != "fastly" || version.contains("HTTP2"),
        "Fastly parity requires an HTTP/2 curl build",
    )?;
    records.push(
        json!({"event":"framing-client","adapter":adapter,"version":version,"qualification":false}),
    )?;
    let mut passed = true;
    let mut production_cases = 0;
    let mut sdk_parity_cases = 0;
    let mut sdk_references = 0;
    let mut sdk_failures = 0;
    let mut reference_differences = 0;
    let mut head_wire_cases = 0;
    for h2 in [false, true] {
        if h2 && adapter != "fastly" {
            continue;
        }
        for case in [
            "buffered",
            "empty",
            "explicit",
            "streamed",
            "declared-stream",
            "short-stream",
            "rendered-not-found",
            "mismatch",
            "trailers",
            "expired",
            "head",
            "head-explicit",
            "not-modified",
            "not-modified-explicit",
            "no-content",
            "reset-content",
        ] {
            let head = case.starts_with("head");
            let client = request(port, &format!("/progress/framing/{case}"), head, h2, run);
            let check = client
                .as_ref()
                .map_err(|error| error.to_string().into())
                .and_then(|response| validate(response, adapter, case, h2));
            records.push(json!({"event":"framing-case","adapter":adapter,"case":case,"http2":h2,"client":client.as_ref().ok(),"status":if check.is_ok(){"pass"}else{"fail"},"error":check.as_ref().err().map(ToString::to_string),"qualification":false}))?;
            passed &= check.is_ok();
            production_cases += 1;
            if head && !h2 {
                let wire = super::response_headers::assembly_request(
                    port,
                    &format!("/progress/framing/{case}"),
                    true,
                    run,
                    adapter == "cloudflare",
                );
                let check = wire
                    .as_ref()
                    .map_err(|error| error.to_string().into())
                    .and_then(|response| {
                        super::response_headers::validate_wire(response, true, 200, None)
                    });
                records.push(
                    json!({"event":"framing-head-wire","adapter":adapter,"case":case,
                    "client":wire.as_ref().ok(),"status":if check.is_ok(){"pass"}else{"fail"},
                    "error":check.as_ref().err().map(ToString::to_string),"qualification":false}),
                )?;
                passed &= check.is_ok();
                head_wire_cases += 1;
            }
            if adapter == "fastly"
                && !matches!(
                    case,
                    "streamed"
                        | "declared-stream"
                        | "short-stream"
                        | "rendered-not-found"
                        | "trailers"
                        | "mismatch"
                        | "expired"
                )
            {
                let reference =
                    request(port, &format!("/progress/reference/{case}"), head, h2, run);
                let parity = reference
                    .as_ref()
                    .map_err(|error| error.to_string().into())
                    .and_then(|reference| {
                        let client = client.as_ref().map_err(|error| error.to_string())?;
                        validate_sdk_parity(client, reference)
                    });
                let regression = client
                    .as_ref()
                    .map_err(|error| error.to_string().into())
                    .and_then(|candidate| {
                        let reference = reference.as_ref().map_err(|error| error.to_string())?;
                        sdk_regression(case, h2, candidate, reference)
                    });
                let parity_applies = sdk_parity_applies(case);
                if parity_applies {
                    sdk_parity_cases += 1;
                    sdk_failures += usize::from(parity.is_err());
                } else {
                    sdk_references += 1;
                    reference_differences +=
                        usize::from(matches!(regression, Ok("expected-failure")));
                }
                records.push(json!({"event":if parity_applies {"framing-sdk-parity"}else{"framing-sdk-reference"},
                    "adapter":adapter,"case":case,"http2":h2,"reference":reference.as_ref().ok(),
                    "status":if regression.is_ok(){"pass"}else{"fail"},"parity_applicable":parity_applies,
                    "matches_reference":parity.is_ok(),"reference_difference":parity.as_ref().err().map(ToString::to_string),
                    "regression_error":regression.as_ref().err().map(ToString::to_string),"qualification":false}))?;
                passed &= regression.is_ok();
            }
        }
    }
    records.push(json!({"event":"framing-regression-summary","adapter":adapter,"status":if passed {"pass"} else {"fail"},
        "production_cases":production_cases,"head_wire_cases":head_wire_cases,
        "sdk_parity_cases":sdk_parity_cases,"sdk_reference_observations":sdk_references,
        "sdk_reference_differences":reference_differences,"sdk_parity_failures":sdk_failures,
        "sdk_parity_status":if sdk_failures==0 {"pass"} else {"fail"},"qualification":false}))?;
    records.push(json!({"event":"framing-hosted-gates","adapter":adapter,"status":"open","qualification":false,"reason":"local runtime results do not qualify hosted compression, deployed header delivery, or consumer response parity"}))?;
    require(
        passed,
        "one or more independent response-framing cases failed",
    )
}

fn sdk_parity_applies(case: &str) -> bool {
    matches!(case, "buffered" | "empty" | "explicit")
}

fn validate_sdk_parity(candidate: &Value, reference: &Value) -> Result<()> {
    require(
        candidate["curl_success"] == true
            && reference["curl_success"] == true
            && candidate.get("head_parse_error").is_none()
            && reference.get("head_parse_error").is_none()
            && ["protocol", "status", "length", "transfer", "body"]
                .iter()
                .all(|key| reference[key] == candidate[key]),
        "Fastly SDK automatic framing parity mismatch",
    )
}

fn sdk_regression(
    case: &str,
    h2: bool,
    candidate: &Value,
    reference: &Value,
) -> Result<&'static str> {
    validate(candidate, "fastly", case, h2)?;
    let known = matches!(case, "head" | "head-explicit" | "reset-content")
        || (h2
            && matches!(
                case,
                "not-modified" | "not-modified-explicit" | "no-content"
            ));
    if !known {
        validate_sdk_parity(candidate, reference)?;
        return Ok("pass");
    }
    require(
        validate_sdk_parity(candidate, reference).is_err(),
        "SDK deviation improved; review the stale expectation",
    )?;
    require(
        candidate["length"].is_null()
            && candidate["transfer"]
                == if case == "reset-content" && !h2 {
                    json!("chunked")
                } else {
                    Value::Null
                },
        "suppression framing differs from the recorded deviation",
    )?;
    let failed_h2 = h2 && case != "reset-content";
    require(
        reference["curl_success"] == !failed_h2
            && if failed_h2 {
                matches!(reference["curl_exit"].as_u64(), Some(16 | 92))
            } else {
                reference["curl_exit"] == 0
            }
            && candidate["body_observation"]
                == if case.starts_with("head") {
                    json!("header-only")
                } else {
                    json!("captured")
                }
            && reference["body_observation"] == candidate["body_observation"]
            && reference["body"]
                == if case == "reset-content" {
                    json!("abcdef")
                } else {
                    candidate["body"].clone()
                },
        "SDK deviation has an unexpected transfer or payload",
    )?;
    if h2 && case == "no-content" {
        require(
            reference["head_parse_error"] == "truncated framing header section"
                && reference["incomplete_head"]["protocol"] == "HTTP/2"
                && reference["incomplete_head"]["status"] == 204
                && reference["incomplete_head"].get("length") == Some(&Value::Null)
                && reference["incomplete_head"].get("transfer") == Some(&Value::Null)
                && reference["incomplete_head"]["headers"]
                    .as_array()
                    .is_some_and(|fields| {
                        fields.len() == 1
                            && fields[0][0] == "date"
                            && fields[0][1].as_str().is_some_and(|value| !value.is_empty())
                    })
                && ["protocol", "status", "length", "transfer"]
                    .iter()
                    .all(|field| reference.get(field).is_none()),
            "SDK 204 deviation lacks the exact incomplete-head witness",
        )?;
    } else {
        require(
            reference.get("head_parse_error").is_none()
                && reference["protocol"] == candidate["protocol"]
                && reference["status"] == candidate["status"]
                && reference["length"] == 6
                && reference["transfer"].is_null(),
            "SDK deviation has an unexpected head",
        )?;
    }
    Ok("expected-failure")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sdk_parity_scope_excludes_all_twelve_bodiless_reference_observations() {
        let mut parity = 0;
        let mut references = 0;
        for h2 in [false, true] {
            for case in [
                "buffered",
                "empty",
                "explicit",
                "head",
                "head-explicit",
                "not-modified",
                "not-modified-explicit",
                "no-content",
                "reset-content",
            ] {
                if sdk_parity_applies(case) {
                    assert!(matches!(case, "buffered" | "empty" | "explicit"));
                    parity += 1;
                } else {
                    let (candidate, mut reference) = known_sdk_deviation(case, h2);
                    if !h2
                        && matches!(
                            case,
                            "not-modified" | "not-modified-explicit" | "no-content"
                        )
                    {
                        reference = candidate.clone();
                    }
                    assert!(sdk_regression(case, h2, &candidate, &reference).is_ok());
                    references += 1;
                }
            }
        }
        assert_eq!((parity, references), (6, 12));
    }

    #[test]
    fn framing_head_wire_requires_clean_eof_not_a_header_only_observation() {
        let valid = json!({"protocol":"HTTP/1.1","status":200,"headers":[],
            "body":"","eof":true,"capture_error":null});
        super::super::response_headers::validate_wire(&valid, true, 200, None).unwrap();
        for (field, wrong) in [
            ("body", json!("x")),
            ("body", Value::Null),
            ("eof", json!(false)),
            ("eof", Value::Null),
            ("capture_error", json!("deadline")),
        ] {
            let mut changed = valid.clone();
            changed[field] = wrong;
            assert!(
                super::super::response_headers::validate_wire(&changed, true, 200, None).is_err()
            );
        }
    }

    fn known_sdk_deviation(case: &str, h2: bool) -> (Value, Value) {
        let head = case.starts_with("head");
        let status = match case {
            "not-modified" | "not-modified-explicit" => 304,
            "no-content" => 204,
            "reset-content" => 205,
            _ => 200,
        };
        let candidate = json!({"protocol":if h2 {"HTTP/2"} else {"HTTP/1.1"},"status":status,
            "curl_success":true,"curl_exit":0,"length":null,
            "transfer":if case=="reset-content" && !h2 {json!("chunked")} else {Value::Null},
            "body":if head {Value::Null} else {json!("")},
            "body_observation":if head {"header-only"} else {"captured"}});
        let mut reference = candidate.clone();
        reference["transfer"] = Value::Null;
        reference["length"] = json!(6);
        if case == "reset-content" {
            reference["body"] = json!("abcdef");
        } else if h2 {
            reference["curl_success"] = json!(false);
            reference["curl_exit"] = json!(92);
            if case == "no-content" {
                for key in ["protocol", "status", "length", "transfer"] {
                    reference.as_object_mut().unwrap().remove(key);
                }
                reference["head_parse_error"] = json!("truncated framing header section");
                reference["incomplete_head"] = json!({"protocol":"HTTP/2","status":204,"headers":[["date","controlled date"]],"length":null,"transfer":null});
            }
        }
        (candidate, reference)
    }

    #[test]
    fn known_sdk_deviations_pass_regression_but_not_strict_parity() {
        let mut count = 0;
        for h2 in [false, true] {
            for case in [
                "head",
                "head-explicit",
                "not-modified",
                "not-modified-explicit",
                "no-content",
                "reset-content",
            ] {
                if !h2
                    && matches!(
                        case,
                        "not-modified" | "not-modified-explicit" | "no-content"
                    )
                {
                    continue;
                }
                let (candidate, reference) = known_sdk_deviation(case, h2);
                assert!(validate_sdk_parity(&candidate, &reference).is_err());
                assert_eq!(
                    sdk_regression(case, h2, &candidate, &reference).unwrap(),
                    "expected-failure"
                );
                count += 1;
            }
        }
        assert_eq!(count, 9);
    }

    #[test]
    fn known_sdk_cases_do_not_hide_unexpected_failures_or_improvements() {
        for (case, h2) in [
            ("head", false),
            ("head-explicit", true),
            ("not-modified", true),
            ("no-content", true),
            ("reset-content", false),
        ] {
            let (candidate, reference) = known_sdk_deviation(case, h2);
            assert!(
                sdk_regression(case, h2, &candidate, &candidate).is_err(),
                "review an improved reference instead of retaining stale expectations"
            );
            for (field, wrong) in [
                ("curl_exit", json!(28)),
                ("body", json!("foreign payload")),
                ("length", json!(7)),
                ("transfer", json!("chunked")),
                ("protocol", json!("HTTP/3")),
            ] {
                let mut changed = reference.clone();
                changed[field] = wrong;
                assert!(
                    sdk_regression(case, h2, &candidate, &changed).is_err(),
                    "accepted unexpected {case}/{h2}/{field}"
                );
            }
            let mut failed = candidate.clone();
            failed["curl_success"] = json!(false);
            failed["curl_exit"] = json!(28);
            assert!(sdk_regression(case, h2, &failed, &reference).is_err());
        }
        let matching = json!({"protocol":"HTTP/1.1","status":200,"curl_success":true,"curl_exit":0,"length":6,"transfer":null,"body":"abcdef","body_observation":"captured"});
        assert_eq!(
            sdk_regression("buffered", false, &matching, &matching).unwrap(),
            "pass"
        );
        let mut wrong = matching.clone();
        wrong["body"] = json!("wrong");
        assert!(sdk_regression("buffered", false, &matching, &wrong).is_err());
    }

    #[test]
    fn transport_timeouts_and_failed_candidates_cannot_pass_framing_gates() {
        let mut truncated = json!({"curl_success":false,"curl_exit":28,"protocol":"HTTP/1.1","status":200,"body":"abc","length":7,"transfer":null});
        assert!(validate(&truncated, "axum", "short-stream", false).is_err());
        truncated["curl_exit"] = json!(18);
        assert!(validate(&truncated, "axum", "short-stream", false).is_ok());
        truncated["protocol"] = json!("HTTP/2");
        truncated["curl_exit"] = json!(92);
        assert!(validate(&truncated, "fastly", "short-stream", true).is_ok());
        let headless =
            json!({"curl_success":false,"curl_exit":92,"head_artifact":{"bytes":0},"body":""});
        assert!(validate(&headless, "fastly", "short-stream", true).is_ok());
        let mut stalled = headless;
        stalled["curl_exit"] = json!(28);
        assert!(validate(&stalled, "fastly", "short-stream", true).is_err());
        let reference = json!({"curl_success":true,"protocol":"HTTP/1.1","status":200,"body":"abcdef","length":6,"transfer":null});
        let mut candidate = reference.clone();
        candidate["curl_success"] = json!(false);
        assert!(validate_sdk_parity(&candidate, &reference).is_err());
    }

    #[test]
    fn forbidden_204_framing_is_rejected_even_on_fastly() {
        for (length, transfer) in [
            (json!(0), Value::Null),
            (json!(6), Value::Null),
            (Value::Null, json!("chunked")),
        ] {
            let response = json!({"curl_success":true,"protocol":"HTTP/1.1","status":204,"body":"","length":length,"transfer":transfer});
            assert!(validate(&response, "fastly", "no-content", false).is_err());
        }
    }

    #[test]
    fn bodyless_wire_validation_distinguishes_host_framing_and_unobserved_head_body() {
        let mut response = json!({"curl_success":true,"protocol":"HTTP/1.1","status":205,"body":"","length":0,"transfer":null});
        assert!(validate(&response, "axum", "reset-content", false).is_ok());
        response["length"] = json!(6);
        assert!(validate(&response, "axum", "reset-content", false).is_err());
        response = json!({"curl_success":true,"protocol":"HTTP/1.1","status":200,"body":null,"body_observation":"header-only","length":6,"transfer":null});
        assert!(validate(&response, "axum", "head-explicit", false).is_ok());
        response["body"] = json!("");
        assert!(validate(&response, "axum", "head-explicit", false).is_err());
        response = json!({"curl_success":true,"protocol":"HTTP/2","status":304,"body":"","length":0,"transfer":null});
        assert!(validate(&response, "fastly", "not-modified-explicit", true).is_err());
    }

    #[test]
    fn parser_rejects_ambiguous_or_multiple_header_sections() {
        for text in [
            "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nContent-Length: 6\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nTransfer-Encoding: chunked\r\n\r\n",
            "HTTP/1.1 200 OK\r\n\r\nHTTP/1.1 200 OK\r\n\r\n",
            "HTTP/2 200\r\ncontent-length: 6\r\n",
        ] {
            assert!(parse_head(text.as_bytes()).is_err());
        }
        assert_eq!(
            parse_head(b"HTTP/2 200\r\ncontent-length: 6\r\n\r\n").unwrap()["length"],
            6
        );
    }

    #[test]
    fn failed_and_head_only_captures_retain_artifacts_without_claiming_payload() {
        let run = std::env::temp_dir().join(format!("framing-capture-{}", token()));
        fs::create_dir_all(&run).unwrap();
        let headers = run.join("capture.headers");
        let body = run.join("capture.body");
        fs::write(&headers, b"HTTP/2 200\r\ncontent-length: 6\r\n\r\n").unwrap();
        fs::write(&body, b"abc").unwrap();
        let failed = capture(
            false,
            &headers,
            &body,
            false,
            b"controlled transport failure",
        )
        .unwrap();
        assert_eq!(failed["curl_success"], false);
        assert_eq!(failed["body"], "abc");
        assert_eq!(failed["capture_artifact"]["bytes"], 3);
        assert!(failed["diagnostic_artifact"]["bytes"].as_u64().unwrap() > 0);
        assert!(validate(&failed, "fastly", "buffered", true).is_err());
        let head = capture(true, &headers, &body, true, b"").unwrap();
        assert!(head["body"].is_null());
        assert_eq!(head["body_observation"], "header-only");
        fs::write(&headers, b"HTTP/2 204\r\ndate: incomplete\r\n").unwrap();
        fs::write(&body, []).unwrap();
        let incomplete = capture(false, &headers, &body, false, b"HTTP/2 stream error").unwrap();
        assert_eq!(
            incomplete["head_parse_error"],
            "truncated framing header section"
        );
        assert_eq!(incomplete["incomplete_head"]["protocol"], "HTTP/2");
        assert_eq!(incomplete["incomplete_head"]["status"], 204);
        assert!(incomplete.get("status").is_none());
        assert!(validate(&incomplete, "fastly", "no-content", true).is_err());
        fs::write(&headers, b"HTTP/1.1 304 Not Modified\r\n\r\n").unwrap();
        fs::write(&body, []).unwrap();
        let empty = capture(false, &headers, &body, true, b"").unwrap();
        assert_eq!(empty["capture_artifact"]["bytes"], 0);
        assert!(validate(&empty, "axum", "not-modified", false).is_ok());
        fs::remove_dir_all(run).unwrap();
    }

    #[test]
    fn incomplete_sdk_204_capture_cannot_hide_unexpected_partial_headers() {
        let run = tempfile::tempdir().unwrap();
        let headers = run.path().join("capture.headers");
        let body = run.path().join("capture.body");
        fs::write(&body, []).unwrap();
        let (candidate, _) = known_sdk_deviation("no-content", true);
        for field in [
            "date: controlled",
            "content-length: 999",
            "transfer-encoding: chunked",
            "content-length: invalid",
            "x-unexpected: value",
            "malformed",
        ] {
            fs::write(&headers, format!("HTTP/2 204\r\n{field}\r\n")).unwrap();
            let mut reference =
                capture(false, &headers, &body, false, b"HTTP/2 stream error").unwrap();
            reference["curl_exit"] = json!(92);
            let regression = sdk_regression("no-content", true, &candidate, &reference);
            assert_eq!(regression.is_ok(), field == "date: controlled", "{field}");
            assert!(validate_sdk_parity(&candidate, &reference).is_err());
        }
    }
}

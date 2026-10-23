use super::*;

const MAX_BODY: u64 = 128 * 1024;
const MAX_HEAD: u64 = 16 * 1024;

pub(super) fn run(name: &str, parent: &Path, records: &mut Records) -> Result<()> {
    let run = parent.join(format!("framing-{name}"));
    fs::create_dir_all(&run)?;
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root())
        .output()?;
    require(revision.status.success(), "missing framing source revision")?;
    let revision = String::from_utf8(revision.stdout)?.trim().to_owned();
    let Some((mut command, port)) = progress::build(name, &run, records, &revision)? else {
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
            "5",
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
    Ok(response)
}

fn capture(
    head: bool,
    header_path: &Path,
    body_path: &Path,
    success: bool,
    diagnostics: &[u8],
) -> Result<Value> {
    let mut response = match parse_head(&read_bounded(header_path, MAX_HEAD)?) {
        Ok(response) => response,
        Err(error) => json!({"head_parse_error":error.to_string()}),
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
                records.push(json!({"event":"framing-sdk-parity","adapter":adapter,"case":case,"http2":h2,"reference":reference.as_ref().ok(),"status":if parity.is_ok(){"pass"}else{"fail"},"error":parity.as_ref().err().map(ToString::to_string),"qualification":false}))?;
                passed &= parity.is_ok();
            }
        }
    }
    records.push(json!({"event":"framing-hosted-gates","adapter":adapter,"status":"open","qualification":false,"reason":"local runtime results do not qualify hosted compression, deployed header delivery, or consumer response parity"}))?;
    require(
        passed,
        "one or more independent response-framing cases failed",
    )
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

#[cfg(test)]
mod tests {
    use super::*;

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
        fs::write(&headers, b"HTTP/1.1 304 Not Modified\r\n\r\n").unwrap();
        fs::write(&body, []).unwrap();
        let empty = capture(false, &headers, &body, true, b"").unwrap();
        assert_eq!(empty["capture_artifact"]["bytes"], 0);
        assert!(validate(&empty, "axum", "not-modified", false).is_ok());
        fs::remove_dir_all(run).unwrap();
    }
}

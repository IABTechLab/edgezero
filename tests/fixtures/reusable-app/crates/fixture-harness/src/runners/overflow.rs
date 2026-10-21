use super::*;
use crate::net::overflow::{self as wire, UploadOrigin};

pub(super) fn run(name: &str, parent: &Path, records: &mut Records) -> Result<()> {
    let run = parent.join(format!("overflow-{name}"));
    fs::create_dir_all(&run)?;
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root())
        .output()?;
    require(
        revision.status.success(),
        "missing overflow source revision",
    )?;
    let revision = String::from_utf8(revision.stdout)?.trim().to_owned();
    let Some((mut command, port)) = progress::build(name, &run, records, &revision)? else {
        return Ok(());
    };
    let log = run.join("runtime.log");
    let outcome = Process::start(&mut command, &log, port).and_then(|_process| {
        let checked = checks(name, port, records);
        let panic = fs::read_to_string(&log)?.contains("worker failed: worker panicked");
        records.push(json!({"event":"overflow-before-shutdown","adapter":name,"worker_panicked":panic,"qualification":false}))?;
        checked.and(require(!panic, "runtime worker panicked during overflow probes"))
    });
    if name == "spin" {
        for file in ["fixture_stdout.txt", "fixture_stderr.txt"] {
            for event in logs(&run.join(".spin/logs").join(file)) {
                records.push(annotate(
                    event,
                    json!({"adapter":name,"diagnostic_source":file}),
                ))?;
            }
        }
    }
    settle(
        records,
        &log,
        json!({"adapter":name,"suite":"outbound-overflow"}),
        json!({"phase":"outbound-overflow"}),
        outcome,
    )
}

fn validate_origin(snapshot: &Value, adapter: &str, token: &str, case: &str) -> Result<()> {
    require(
        snapshot["collection_complete"] == true && snapshot["overflow"] == false,
        "origin collection incomplete or exceeded connection bound",
    )?;
    let connections = snapshot["connections"]
        .as_array()
        .ok_or("missing origin receipts")?;
    require(
        snapshot["accepted"].as_u64() == Some(connections.len() as u64),
        "origin accepted/terminal count mismatch",
    )?;
    let over = case.ends_with("-9");
    if over && (case.starts_with("buffered") || matches!(adapter, "axum" | "cloudflare")) {
        return require(
            connections.is_empty(),
            "over-cap upload contacted the origin before rejection",
        );
    }
    if over && connections.is_empty() {
        return Ok(());
    }
    require(
        connections.len() == 1,
        "missing or duplicate upload origin request",
    )?;
    let receipt = &connections[0];
    if over && receipt["head"].is_null() {
        return require(
            receipt.get("head") == Some(&Value::Null)
                && receipt["head_state"] == "absent"
                && receipt["head_bytes"] == 0
                && receipt["body_bytes"] == 0
                && receipt["body_prefix"] == json!([])
                && matches!(receipt["terminal"].as_str(), Some("eof" | "reset")),
            "headless contact lacks an explicit zero-byte abort witness",
        );
    }
    require(
        receipt["head_state"] == "complete"
            && receipt["head_bytes"]
                .as_u64()
                .is_some_and(|bytes| bytes > 0 && bytes <= 16 * 1024)
            && receipt["head"]["method"] == "POST"
            && receipt["head"]["target"] == format!("/overflow/{token}/{case}"),
        "foreign or headless upload origin receipt",
    )?;
    let length = receipt["body_bytes"]
        .as_u64()
        .ok_or("missing upload byte count")?;
    let bytes = receipt["body_prefix"]
        .as_array()
        .ok_or("missing bounded upload bytes")?;
    require(
        length == bytes.len() as u64
            && bytes
                .iter()
                .all(|byte| byte.as_u64() == Some(u64::from(b'x'))),
        "upload content/count mismatch",
    )?;
    if over {
        return require(
            length <= 8 && matches!(receipt["terminal"].as_str(), Some("eof" | "reset")),
            "streamed overflow completed a truncated request or lacks abort evidence",
        );
    }
    let expected = if case.ends_with("-7") { 7 } else { 8 };
    require(
        receipt["terminal"] == "complete" && length == expected && receipt["reply_written"] == true,
        "valid upload did not complete exactly",
    )
}

fn validate_response(reply: &Value, case: &str, len: usize) -> Result<()> {
    let overflow = (case == "inbound" && len > 1024) || case.ends_with("-9");
    let expected = if overflow { 413 } else { 200 };
    require(
        reply["status"] == expected,
        "overflow probe status mismatch",
    )?;
    let headers = reply["headers"]
        .as_array()
        .ok_or("missing response headers")?;
    require(
        !headers.iter().any(|field| {
            field[0]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("retry-after"))
        }),
        "generated overflow/probe response gained Retry-After",
    )?;
    let report: Value =
        serde_json::from_str(reply["body"].as_str().ok_or("missing response body")?)?;
    if overflow {
        return require(
            report
                == json!({"error":{"status":413,"kind":"payload_too_large","message":"request body too large"}}),
            "generated overflow lost its fixed public category/body",
        );
    }
    if case == "inbound" {
        return require(
            report["case"] == case && report["received"] == len,
            "inbound cap boundary byte count mismatch",
        );
    }
    require(
        report["case"] == case
            && report["outcome"] == "response"
            && report["upstream_status"] == if case == "upstream-413" { 413 } else { 200 },
        "upstream status was not an ordinary response",
    )?;
    require(
        if case == "upstream-413" {
            report["retry_after"] == "120"
        } else {
            report["retry_after"].is_null()
        },
        "ordinary upstream Retry-After changed",
    )
}

fn checks(adapter: &str, port: u16, records: &mut Records) -> Result<()> {
    let mut failed = false;
    for len in [1023_usize, 1024, 1025] {
        let token = token();
        let response = wire::request(port, "inbound", &token, None, Some(len));
        let validation = response
            .as_ref()
            .map_err(|error| error.to_string().into())
            .and_then(|reply| validate_response(reply, "inbound", len));
        records.push(json!({"event":"overflow-case","adapter":adapter,"case":format!("inbound-{len}"),"token":token,
            "client":response.as_ref().ok(),"status":if validation.is_ok(){"pass"}else{"fail"},
            "error":validation.as_ref().err().map(ToString::to_string),"qualification":false}))?;
        failed |= validation.is_err();
    }
    for case in [
        "buffered-7",
        "buffered-8",
        "buffered-9",
        "streamed-7",
        "streamed-8",
        "streamed-9",
        "upstream-413",
    ] {
        let token = token();
        let origin = UploadOrigin::start()?;
        let response = wire::request(port, case, &token, Some(&origin.url(&token, case)), None);
        // Always close and join the origin collector, including failed client operations.
        let snapshot = origin.finish();
        let validation = (|| -> Result<()> {
            let reply = response.as_ref().map_err(|error| error.to_string())?;
            let receipt = snapshot.as_ref().map_err(|error| error.to_string())?;
            validate_response(reply, case, 0)?;
            validate_origin(receipt, adapter, &token, case)
        })();
        records.push(json!({"event":"overflow-case","adapter":adapter,"case":case,"token":token,
            "client":response.as_ref().ok(),"origin":snapshot.as_ref().ok(),
            "origin_collection_error":snapshot.as_ref().err().map(ToString::to_string),
            "status":if validation.is_ok(){"pass"}else{"fail"},"error":validation.as_ref().err().map(ToString::to_string),"qualification":false}))?;
        failed |= validation.is_err();
    }
    records.push(
        json!({"event":"overflow-coverage","adapter":adapter,"attempted":10,"qualification":false,
        "status":if failed{"fail"}else{"pass"}}),
    )?;
    require(!failed, "one or more overflow probes failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(terminal: &str, length: usize) -> Value {
        json!({"head":{"method":"POST","target":"/overflow/test/streamed-9"},"head_state":"complete","head_bytes":64,
            "terminal":terminal,"body_bytes":length,"body_prefix":vec![b'x';length]})
    }

    fn snapshot(receipts: Vec<Value>) -> Value {
        json!({"accepted":receipts.len(),"connections":receipts,"collection_complete":true,"overflow":false})
    }

    #[test]
    fn parsed_malformed_metadata_never_passes_as_an_upload_abort() {
        for tail in ["4\n", "4\r\nxxxxX", "Z", "4\r\nxxxx\r\n0\r\n:bad\r\n"] {
            let raw = format!(
                "POST /overflow/test/streamed-9 HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n{tail}"
            );
            let receipt = wire::inspect(&mut std::io::Cursor::new(raw));
            assert!(
                validate_origin(&snapshot(vec![receipt]), "fastly", "test", "streamed-9").is_err()
            );
        }
        let raw = "POST /overflow/test/streamed-9 HTTP/1.1\r\nX-Bad: a\0b\r\nContent-Length: 9\r\n\r\nxxxx";
        let receipt = wire::inspect(&mut std::io::Cursor::new(raw));
        assert!(validate_origin(&snapshot(vec![receipt]), "fastly", "test", "streamed-9").is_err());
        for raw in [
            "POST /overflow/test/streamed-9 HTTP/1.1\r\nContent-Length: 0\r\n\r\n",
            "POST /overflow/test/streamed-9 HTTP/1.1\r\n\r\n",
            "POST /overflow/test/streamed-9 HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        ] {
            let receipt = wire::inspect(&mut std::io::Cursor::new(raw));
            assert!(
                validate_origin(&snapshot(vec![receipt]), "fastly", "test", "streamed-9").is_err()
            );
        }
    }

    #[test]
    fn full_head_abort_requires_complete_numeric_head_evidence() {
        let raw = "POST /overflow/test/streamed-9 HTTP/1.1\r\nContent-Length: 9\r\n\r\nxxxx";
        let receipt = wire::inspect(&mut std::io::Cursor::new(raw));
        validate_origin(
            &snapshot(vec![receipt.clone()]),
            "fastly",
            "test",
            "streamed-9",
        )
        .unwrap();
        for field in ["head_state", "head_bytes"] {
            let mut missing = receipt.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                validate_origin(&snapshot(vec![missing]), "fastly", "test", "streamed-9").is_err()
            );
        }
        for (field, value) in [
            ("head_state", json!("partial")),
            ("head_state", json!("absent")),
            ("head_bytes", json!(0)),
            ("head_bytes", json!("64")),
        ] {
            let mut invalid = receipt.clone();
            invalid[field] = value;
            assert!(
                validate_origin(&snapshot(vec![invalid]), "fastly", "test", "streamed-9").is_err()
            );
        }
    }

    #[test]
    fn only_correlated_incomplete_prefixes_prove_streamed_upload_abort() {
        for adapter in ["fastly", "spin"] {
            for terminal in ["eof", "reset"] {
                validate_origin(
                    &snapshot(vec![receipt(terminal, 4)]),
                    adapter,
                    "test",
                    "streamed-9",
                )
                .unwrap();
            }
            for terminal in [
                "complete",
                "timeout",
                "io_error",
                "invalid_framing",
                "capture_limit",
                "reading",
            ] {
                assert!(
                    validate_origin(
                        &snapshot(vec![receipt(terminal, 4)]),
                        adapter,
                        "test",
                        "streamed-9"
                    )
                    .is_err()
                );
            }
            assert!(
                validate_origin(
                    &snapshot(vec![receipt("eof", 9)]),
                    adapter,
                    "test",
                    "streamed-9"
                )
                .is_err()
            );
            let duplicate = receipt("eof", 4);
            assert!(
                validate_origin(
                    &snapshot(vec![duplicate.clone(), duplicate]),
                    adapter,
                    "test",
                    "streamed-9"
                )
                .is_err()
            );
            let mut foreign = receipt("eof", 4);
            foreign["head"]["target"] = json!("/overflow/other/streamed-9");
            assert!(
                validate_origin(&snapshot(vec![foreign]), adapter, "test", "streamed-9").is_err()
            );
            let mut headless = receipt("eof", 4);
            headless["head"] = Value::Null;
            assert!(
                validate_origin(&snapshot(vec![headless]), adapter, "test", "streamed-9").is_err()
            );
            let empty_contact = json!({"head":null,"head_state":"absent","head_bytes":0,
                "terminal":"eof","body_bytes":0,"body_prefix":[]});
            validate_origin(
                &snapshot(vec![empty_contact.clone()]),
                adapter,
                "test",
                "streamed-9",
            )
            .unwrap();
            let mut partial = empty_contact.clone();
            partial["head_state"] = json!("partial");
            partial["head_bytes"] = json!(5);
            assert!(
                validate_origin(&snapshot(vec![partial]), adapter, "test", "streamed-9").is_err()
            );
            let mut unobserved = empty_contact;
            unobserved.as_object_mut().unwrap().remove("head_state");
            assert!(
                validate_origin(&snapshot(vec![unobserved]), adapter, "test", "streamed-9")
                    .is_err()
            );
        }
        for adapter in ["axum", "cloudflare"] {
            assert!(
                validate_origin(
                    &snapshot(vec![receipt("eof", 4)]),
                    adapter,
                    "test",
                    "streamed-9"
                )
                .is_err()
            );
            validate_origin(&snapshot(vec![]), adapter, "test", "streamed-9").unwrap();
        }
        let mut missing = snapshot(vec![]);
        missing["accepted"] = json!(1);
        assert!(validate_origin(&missing, "fastly", "test", "streamed-9").is_err());
        missing["accepted"] = json!(0);
        missing["collection_complete"] = json!(false);
        assert!(validate_origin(&missing, "fastly", "test", "streamed-9").is_err());
    }
}

use super::*;
use crate::net::WireOrigins;
use std::io::Read as _;

mod batch_recovery;
mod normalization;

pub(super) fn validate_batch_artifact(events: &[Value], adapter: &str) -> Result<()> {
    batch_recovery::validate_artifact(events, adapter)
}

#[cfg(test)]
pub(super) fn batch_recovery_artifact(adapter: &str) -> Vec<Value> {
    batch_recovery::tests::artifact(adapter)
}

fn body(reply: &Value) -> Result<Vec<u8>> {
    Ok(serde_json::from_value(reply["body_bytes"].clone())?)
}

fn handoff(bytes: &[u8]) -> Result<()> {
    require(
        bytes.len() == 21 && bytes.starts_with(b"origin"),
        "handoff origin bytes differ",
    )?;
    let mut bids = bytes[6..].as_chunks::<5>().0.to_vec();
    bids.sort_unstable();
    require(
        bids == [*b"bid-A", *b"bid-B", *b"bid-C"],
        "handoff lost or changed bidder bytes",
    )
}

fn error_is(report: &Value, kind: &str, reason: &str) -> Result<()> {
    let error = &report["outcome"]["error"];
    require(
        error["kind"] == kind
            && error["reason"] == reason
            && error["status"] == if kind == "gateway_timeout" { 504 } else { 502 },
        "wrong typed wire outcome",
    )
}

const POLICY_CASES: usize = 26;
const POLICY_GZIP: &[u8] = &[
    31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 75, 76, 132, 3, 0, 146, 93, 70, 85, 11, 0, 0, 0,
];

struct PolicyCase {
    gzip: bool,
    original: bool,
    streamed: bool,
    declared: bool,
    size: usize,
    encoded_cap: u64,
    final_cap: u64,
    reason: Option<&'static str>,
}

impl PolicyCase {
    fn new(index: usize) -> Result<Self> {
        require(index < POLICY_CASES, "unknown Spin policy case")?;
        let gzip = index >= 16;
        let variant = index.saturating_sub(16) % 5;
        let size = if gzip { 11 } else { 10 + (index & 1) };
        Ok(Self {
            gzip,
            original: gzip || index & 8 != 0,
            streamed: if gzip { variant >= 3 } else { index & 4 != 0 },
            declared: if gzip { index >= 21 } else { index & 2 != 0 },
            size,
            encoded_cap: if !gzip {
                20
            } else if matches!(variant, 1 | 4) {
                22
            } else {
                23
            },
            final_cap: if !gzip {
                20
            } else if variant == 2 {
                22
            } else {
                23
            },
            reason: if !gzip && size == 11 {
                Some("DecodedBody")
            } else if gzip && matches!(variant, 1 | 4) {
                Some("EncodedBody")
            } else if gzip && variant == 2 {
                Some("BufferedBody")
            } else {
                None
            },
        })
    }

    fn path(&self, token: &str) -> String {
        format!(
            "/p5/policy/{}/{}/{}/{}/{token}",
            if self.gzip { "gzip" } else { "identity" },
            if self.declared {
                "declared"
            } else {
                "observed"
            },
            self.size,
            if self.reason.is_some() {
                "reject"
            } else {
                "success"
            }
        )
    }
}

fn validate_policy(index: usize, report: &Value) -> Result<()> {
    let case = PolicyCase::new(index)?;
    require(
        report["case"] == index
            && report["request"]
                == json!({
            "decoding":if case.original {"original"} else {"decode"},"streamed":case.streamed,
            "decoded_cap":10,"encoded_cap":case.encoded_cap,
            "final_cap":if case.streamed {None} else {Some(case.final_cap)},"decoder_cap":0}),
        "wrong Spin policy case or effective configuration",
    )?;
    let polls = report["source"]["polls"]
        .as_u64()
        .ok_or("missing Spin source poll count")?;
    require(
        report["source"]["released"] == true
            && if case.declared && case.reason.is_some() {
                polls == 0
            } else {
                polls > 0
            },
        "Spin policy source polling or release differs",
    )?;
    if let Some(reason) = case.reason {
        error_is(report, "response_too_large", reason)?;
        return require(
            report["outcome"]
                .as_object()
                .is_some_and(|outcome| outcome.len() == 1)
                && report["outcome"]["error"].get("provider") == Some(&Value::Null)
                && report["error_stage"]
                    == if case.declared || !case.streamed {
                        "send"
                    } else {
                        "body"
                    },
            "wrong Spin policy error stage or provider",
        );
    }
    let bytes = if case.gzip {
        POLICY_GZIP.to_vec()
    } else {
        vec![b'a'; case.size]
    };
    let outcome = &report["outcome"];
    require(
        report.get("error_stage") == Some(&Value::Null)
            && outcome["status"] == 200
            && outcome["bytes"] == json!(bytes)
            && outcome["streamed"] == case.streamed
            && outcome.get("error").is_none(),
        "wrong Spin policy success bytes, mode or status",
    )?;
    let fields = outcome["headers"]
        .as_array()
        .ok_or("missing Spin policy headers")?;
    require(
        fields.iter().all(|field| {
            field.as_array().is_some_and(|pair| {
                pair.len() == 2
                    && pair[0].as_str().is_some()
                    && serde_json::from_value::<Vec<u8>>(pair[1].clone()).is_ok()
            })
        }),
        "invalid Spin policy header report",
    )?;
    for (name, expected) in [
        (
            "content-encoding",
            Some(if case.gzip {
                b"gzip".to_vec()
            } else {
                b"identity".to_vec()
            }),
        ),
        (
            "content-length",
            case.declared.then(|| bytes.len().to_string().into_bytes()),
        ),
        ("content-type", Some(b"application/octet-stream".to_vec())),
        ("x-p5-fixture", Some(b"spin-wire".to_vec())),
        ("transfer-encoding", None),
        ("connection", None),
    ] {
        let found: Vec<_> = fields
            .iter()
            .filter(|field| {
                field[0]
                    .as_str()
                    .is_some_and(|field| field.eq_ignore_ascii_case(name))
            })
            .collect();
        require(
            match expected {
                Some(expected) => found.len() == 1 && found[0][1] == json!(expected),
                None => found.is_empty(),
            },
            format!("wrong Spin policy {name} field"),
        )?;
    }
    Ok(())
}

fn validate_policy_inventory(cases: &[usize], receipts: usize) -> Result<()> {
    let mut observed = cases.to_vec();
    observed.sort_unstable();
    require(
        observed == (0..POLICY_CASES).collect::<Vec<_>>() && receipts == POLICY_CASES,
        "incomplete or duplicate Spin policy matrix",
    )
}

pub(super) fn validate_spin_artifact(events: &[Value]) -> Result<()> {
    require(
        !events.is_empty() && events.iter().all(|event| event["status"] != "fail"),
        "missing or failed Spin wire evidence",
    )?;
    for (name, exchanges, receipts) in [
        ("spin-wire-inventory", 68, 84),
        ("spin-policy-inventory", 26, 26),
    ] {
        let inventory = events
            .iter()
            .filter(|event| event["event"] == name)
            .collect::<Vec<_>>();
        require(
            inventory.len() == 1
                && inventory[0]["status"] == "pass"
                && inventory[0]["qualification"] == false
                && inventory[0]["exchanges"] == exchanges
                && inventory[0]["origin_requests"] == receipts,
            "invalid Spin wire inventory",
        )?;
    }
    let wire = events
        .iter()
        .filter(|event| event["event"] == "spin-wire-case")
        .collect::<Vec<_>>();
    require(wire.len() == 68, "incomplete Spin redirect matrix")?;
    let origin_port = wire[0]["origin_requests"][0][0]
        .as_u64()
        .and_then(|port| u16::try_from(port).ok())
        .ok_or("missing Spin origin port")?;
    replay_spin_wire(wire)?;
    let mut cases = Vec::new();
    let mut receipts = 0;
    for event in events
        .iter()
        .filter(|event| event["event"] == "spin-policy-case")
    {
        let index = event["case"]
            .as_u64()
            .and_then(|index| usize::try_from(index).ok())
            .ok_or("invalid Spin policy index")?;
        let token = event["token"]
            .as_str()
            .filter(|token| !token.is_empty())
            .ok_or("missing Spin policy token")?;
        let path = PolicyCase::new(index)?.path(token);
        let requests = event["origin_requests"]
            .as_array()
            .ok_or("missing Spin policy receipts")?;
        require(
            event["path"] == path
                && requests.len() == 1
                && requests[0].as_array().is_some_and(|pair| pair.len() == 2)
                && requests[0][0]
                    .as_u64()
                    .is_some_and(|port| (1..=u64::from(u16::MAX)).contains(&port))
                && requests[0][1] == path,
            "mismatched Spin policy receipt",
        )?;
        validate_policy(index, &event["report"])?;
        cases.push(index);
        receipts += requests.len();
    }
    validate_policy_inventory(&cases, receipts)?;
    normalization::validate_artifact(events, origin_port)?;
    batch_recovery::validate_artifact(events, "spin")?;
    Ok(())
}

fn spin_policy(port: u16, origins: &WireOrigins, records: &mut Records) -> Result<(usize, usize)> {
    let mut cases = Vec::new();
    let mut receipts = 0;
    for index in 0..POLICY_CASES {
        require(
            origins.take_requests().is_empty(),
            "request escaped the preceding Spin policy exchange",
        )?;
        let case = PolicyCase::new(index)?;
        let token = token();
        let path = case.path(&token);
        let uri = format!("http://127.0.0.1:{}{path}", origins.port(0));
        let response = net::header_request(
            port,
            "/policy",
            &token,
            &[("x-origin", &uri), ("x-policy-case", &index.to_string())],
            None,
        )?;
        let observed = origins.take_requests();
        let report: Result<Value> =
            body(&response).and_then(|bytes| Ok(serde_json::from_slice(&bytes)?));
        records.push(json!({"event":"spin-policy-case","case":index,"token":token,"path":path,
            "origin_requests":observed,"report":report.as_ref().ok(),"response":response,"qualification":false}))?;
        require(
            response["status"] == 200,
            "Spin policy guest rejected the exchange",
        )?;
        require(
            observed == vec![(origins.port(0), path)],
            "missing, extra or foreign Spin policy origin requests",
        )?;
        validate_policy(index, &report?)?;
        cases.push(index);
        receipts += observed.len();
    }
    validate_policy_inventory(&cases, receipts)?;
    Ok((cases.len(), receipts))
}

struct Matrix<'a> {
    adapter: &'static str,
    endpoint: &'static str,
    port: u16,
    origins: &'a WireOrigins,
    records: &'a mut Records,
    exchanges: usize,
    dispatches: usize,
}

impl MatrixContracts for Matrix<'_> {
    fn adapter(&self) -> &'static str {
        self.adapter
    }

    fn ports(&self) -> [u16; 2] {
        [self.origins.port(0), self.origins.port(1)]
    }

    fn inventory(&self) -> (usize, usize) {
        (self.exchanges, self.dispatches)
    }

    fn call(
        &mut self,
        path: &str,
        mode: &str,
        streamed: bool,
        encoded: bool,
        expected: Option<Vec<(u16, String)>>,
        gate: Option<&str>,
    ) -> Result<Value> {
        require(
            self.origins.take_requests().is_empty(),
            "request escaped the preceding wire exchange",
        )?;
        if let Some(name) = gate {
            self.origins.arm(name);
        }
        let uri = format!("http://127.0.0.1:{}{path}", self.origins.port(0));
        let response = net::header_request(
            self.port,
            self.endpoint,
            &token(),
            &[
                ("x-origin", &uri),
                ("x-p5-mode", mode),
                ("x-p5-stream", if streamed { "true" } else { "false" }),
                ("x-p5-encoded-cap", if encoded { "true" } else { "false" }),
            ],
            None,
        );
        if let Some(name) = gate {
            self.origins.release(name);
        }
        let response = response?;
        require(
            response["status"] == 200,
            "wire guest rejected the exchange",
        )?;
        let observed = self.origins.take_requests();
        let expected = expected.unwrap_or_else(|| vec![(self.origins.port(0), path.to_owned())]);
        if observed != expected {
            self.records.push(json!({"event":format!("{}-wire-receipt-failure", self.adapter),"status":"fail",
                "path":path,"mode":mode,"observed":observed,"expected":expected,"qualification":false}))?;
        }
        require(
            observed == expected,
            "missing, extra or foreign wire origin requests",
        )?;
        let report: Value = serde_json::from_slice(&body(&response)?)?;
        self.exchanges += 1;
        self.dispatches += observed.len();
        self.records.push(
            json!({"event":format!("{}-wire-case", self.adapter),"path":path,"mode":mode,
            "streamed":streamed,"encoded_cap":encoded,"origin_requests":observed,
            "report":report,"qualification":false}),
        )?;
        Ok(report)
    }
}

trait MatrixContracts {
    fn adapter(&self) -> &'static str;
    fn ports(&self) -> [u16; 2];
    fn inventory(&self) -> (usize, usize);
    fn call(
        &mut self,
        path: &str,
        mode: &str,
        streamed: bool,
        encoded: bool,
        expected: Option<Vec<(u16, String)>>,
        gate: Option<&str>,
    ) -> Result<Value>;

    fn parity(&mut self, path: &str, streamed: bool, encoded: bool) -> Result<Value> {
        let sent = self.call(path, "send", streamed, encoded, None, None)?;
        let followed = self.call(path, "follow", streamed, encoded, None, None)?;
        require(
            sent["outcome"] == followed["outcome"],
            "Send/Follow outcomes differ",
        )?;
        Ok(followed)
    }

    fn run(&mut self) -> Result<()> {
        let [first, second] = self.ports();
        let base = format!("http://127.0.0.1:{first}");
        for streamed in [false, true] {
            for coding in ["identity", "gzip", "passthrough"] {
                for size in [10, 11] {
                    let path = format!("/p5/body/{coding}/{size}");
                    let encoded = streamed && coding == "passthrough";
                    let followed = self.parity(&path, streamed, encoded)?;
                    require(
                        followed["policies"] == 0 && followed["hop"] == 0,
                        "unexpected no-redirect policy/hop",
                    )?;
                    if size == 10 {
                        let outcome = &followed["outcome"];
                        require(
                            outcome["status"] == 200
                                && outcome["bytes"] == json!(vec![b'a'; 10])
                                && followed["streamed"] == streamed
                                && followed["final_uri"] == format!("{base}{path}")
                                && followed["redirects"] == 0
                                && followed.get("stop") == Some(&Value::Null),
                            "wrong no-redirect final response",
                        )?;
                        let fields = outcome["headers"]
                            .as_array()
                            .ok_or("missing wire headers")?;
                        let field = |name: &str| {
                            fields
                                .iter()
                                .find(|entry| entry[0] == name)
                                .map(|entry| &entry[1])
                        };
                        require(
                            field("x-p5-fixture") == Some(&json!(b"spin-wire".as_slice())),
                            "missing wire fixture header",
                        )?;
                        if coding == "gzip" {
                            require(
                                field("content-encoding").is_none()
                                    && field("content-length").is_none(),
                                "decoded representation fields retained",
                            )?;
                        } else {
                            let value: &[u8] = if coding == "identity" {
                                b"identity"
                            } else {
                                b"x-p5-opaque"
                            };
                            require(
                                field("content-encoding") == Some(&json!(value)),
                                "coding field changed",
                            )?;
                        }
                    } else {
                        error_is(
                            &followed,
                            "response_too_large",
                            if encoded {
                                "EncodedBody"
                            } else if streamed {
                                "DecodedBody"
                            } else {
                                "BufferedBody"
                            },
                        )?;
                    }
                }
                let followed = self.parity(&format!("/p5/declared/{coding}"), streamed, true)?;
                error_is(&followed, "response_too_large", "EncodedBody")?;
            }
        }
        for kind in ["pending", "error", "emptybr", "oversized"] {
            let mut errors = Vec::new();
            for mode in ["follow", "send", "stop", "limit"] {
                let name = format!("{kind}-{mode}");
                let redirect = format!("/p5/redirect/{kind}/{name}");
                let path = if mode == "limit" {
                    format!("/p5/limit/{kind}/{name}")
                } else {
                    redirect.clone()
                };
                let mut expected = vec![(first, path.clone())];
                if mode == "follow" {
                    expected.push((first, format!("/p5/next/{name}")));
                } else if mode == "limit" {
                    expected.push((first, redirect));
                }
                let report = self.call(&path, mode, false, false, Some(expected), Some(&name))?;
                if mode == "follow" {
                    require(
                        report["outcome"]["status"] == 200
                            && report["outcome"]["bytes"] == json!(b"final".as_slice())
                            && report["redirects"] == 1
                            && report["policies"] == 1
                            && report.get("stop") == Some(&Value::Null)
                            && report["final_uri"] == format!("{base}/p5/next/{name}"),
                        "wrong intermediate-body follow result",
                    )?;
                } else {
                    require(
                        report["hop"] == usize::from(mode == "limit")
                            && report["policies"] == usize::from(mode != "send"),
                        "wrong stopped/limit hop or policy count",
                    )?;
                    match kind {
                        "pending" => error_is(&report, "gateway_timeout", "PerCallTimeout")?,
                        "error" => {
                            source_error(&report, self.adapter())?;
                        }
                        "emptybr" => error_is(&report, "bad_gateway", "Decode(Brotli)")?,
                        _ => error_is(&report, "response_too_large", "BufferedBody")?,
                    }
                    errors.push(report["outcome"].clone());
                }
            }
            require(
                errors.len() == 3 && errors.iter().all(|error| error == &errors[0]),
                "Send/stop/limit final errors differ",
            )?;
        }
        for mode in ["stop", "limit"] {
            let name = format!("clean-{mode}");
            let redirect = format!("/p5/redirect/clean/{name}");
            let path = if mode == "limit" {
                format!("/p5/limit/clean/{name}")
            } else {
                redirect.clone()
            };
            let mut expected = vec![(first, path.clone())];
            if mode == "limit" {
                expected.push((first, redirect));
            }
            let report = self.call(&path, mode, false, false, Some(expected), Some(&name))?;
            require(
                report["outcome"]["status"] == 302
                    && report["outcome"]["bytes"] == json!(b"found".as_slice())
                    && report["stop"]
                        == if mode == "limit" {
                            "LimitReached"
                        } else {
                            "StoppedByPolicy"
                        }
                    && report["redirects"] == usize::from(mode == "limit"),
                "wrong clean stop/limit response",
            )?;
        }
        let sent = self.call("/p5/cross", "send", false, false, None, None)?;
        require(
            sent["outcome"]["status"] == 302,
            "plain Send followed the cross-origin redirect",
        )?;
        let report = self.call(
            "/p5/cross",
            "follow",
            false,
            false,
            Some(vec![
                (first, "/p5/cross".to_owned()),
                (second, "/p5/final".to_owned()),
            ]),
            None,
        )?;
        require(
            report["outcome"]["status"] == 200
                && report["outcome"]["bytes"] == json!(b"final".as_slice())
                && report["redirects"] == 1
                && report["final_uri"] == format!("http://127.0.0.1:{second}/p5/final"),
            "wrong cross-origin final response",
        )?;
        for coding in ["identity", "gzip", "passthrough"] {
            for size in [10, 11] {
                let final_path = format!("/p5/body/{coding}/{size}");
                let path = format!("/p5/to-body/{coding}/{size}");
                let encoded = coding == "passthrough";
                let sent = self.call(&final_path, "send", true, encoded, None, None)?;
                let followed = self.call(
                    &path,
                    "follow",
                    true,
                    encoded,
                    Some(vec![(first, path.clone()), (first, final_path.clone())]),
                    None,
                )?;
                require(
                    sent["outcome"] == followed["outcome"]
                        && followed["streamed"] == true
                        && followed["redirects"] == 1
                        && followed["policies"] == 1
                        && followed["final_uri"] == format!("{base}{final_path}"),
                    "streamed final Send/Follow parity differs",
                )?;
            }
        }
        let (exchanges, dispatches) = self.inventory();
        require(
            exchanges == 68 && dispatches == 84,
            "incomplete provider wire matrix",
        )
    }
}

struct ReplayMatrix<'a> {
    ports: [u16; 2],
    events: std::vec::IntoIter<&'a Value>,
    exchanges: usize,
    dispatches: usize,
}

impl MatrixContracts for ReplayMatrix<'_> {
    fn adapter(&self) -> &'static str {
        "spin"
    }

    fn ports(&self) -> [u16; 2] {
        self.ports
    }

    fn inventory(&self) -> (usize, usize) {
        (self.exchanges, self.dispatches)
    }

    fn call(
        &mut self,
        path: &str,
        mode: &str,
        streamed: bool,
        encoded: bool,
        expected: Option<Vec<(u16, String)>>,
        _gate: Option<&str>,
    ) -> Result<Value> {
        let event = self.events.next().ok_or("missing Spin redirect case")?;
        require(
            event["path"] == path
                && event["mode"] == mode
                && event["streamed"] == streamed
                && event["encoded_cap"] == encoded
                && event["qualification"] == false,
            "Spin redirect case differs from ordered matrix",
        )?;
        let observed: Vec<(u16, String)> =
            serde_json::from_value(event["origin_requests"].clone())?;
        let expected = expected.unwrap_or_else(|| vec![(self.ports[0], path.to_owned())]);
        require(
            observed == expected,
            "missing, extra or foreign wire origin requests",
        )?;
        let report = event
            .get("report")
            .filter(|report| report.is_object())
            .ok_or("missing Spin redirect report")?
            .clone();
        self.exchanges += 1;
        self.dispatches += observed.len();
        Ok(report)
    }
}

fn replay_spin_wire(wire: Vec<&Value>) -> Result<()> {
    let receipt_port = |event: &Value, index: usize| {
        event["origin_requests"][index][0]
            .as_u64()
            .and_then(|port| u16::try_from(port).ok())
            .filter(|port| *port != 0)
            .ok_or("invalid Spin redirect origin port")
    };
    // Bind both origins once; every replayed receipt and final URI must agree.
    let first = receipt_port(wire.first().ok_or("missing Spin redirect case")?, 0)?;
    let cross = wire
        .iter()
        .find(|event| event["path"] == "/p5/cross" && event["mode"] == "follow")
        .ok_or("missing Spin cross-origin case")?;
    let second = receipt_port(cross, 1)?;
    require(first != second, "Spin redirect origins must be distinct")?;
    let mut matrix = ReplayMatrix {
        ports: [first, second],
        events: wire.into_iter(),
        exchanges: 0,
        dispatches: 0,
    };
    matrix.run()?;
    require(
        matrix.events.next().is_none(),
        "unexpected extra Spin redirect case",
    )
}

fn source_error_reason(adapter: &str) -> &'static str {
    if adapter == "spin" {
        "Protocol"
    } else {
        "Transport"
    }
}

fn source_error(report: &Value, adapter: &str) -> Result<()> {
    error_is(report, "bad_gateway", source_error_reason(adapter))?;
    let provider = report["outcome"]["error"]["provider"].as_str();
    require(
        provider.is_some_and(|detail| detail.contains("ResponseBodyRead")),
        "lost source-error provider phase",
    )?;
    require(
        adapter != "cloudflare"
            || provider == Some("Cloudflare { phase: ResponseBodyRead, cause: Unknown }"),
        "unexpected workerd source-error cause",
    )
}

fn component_text(directory: &Path) -> Result<String> {
    let mut text = String::new();
    let mut directories = vec![directory.to_owned()];
    let mut entries = 0;
    while let Some(directory) = directories.pop() {
        if !directory.exists() {
            continue;
        }
        for entry in fs::read_dir(directory)? {
            entries += 1;
            require(entries <= 128, "too many component log entries")?;
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                directories.push(entry.path());
            } else if kind.is_file() {
                fs::File::open(entry.path())?
                    .take(
                        (1024 * 1024_usize)
                            .saturating_sub(text.len())
                            .saturating_add(1) as u64,
                    )
                    .read_to_string(&mut text)?;
                require(text.len() <= 1024 * 1024, "component logs exceeded 1 MiB")?;
            }
        }
    }
    Ok(text)
}

fn controls(port: u16, origins: &WireOrigins, log_dir: &Path, process: &mut Process) -> Result<()> {
    for name in ["get", "head", "get-settled", "head-settled", "post"] {
        let uri = format!("http://127.0.0.1:{}/{name}", origins.port(0));
        let reply = net::header_request(
            port,
            &format!("/{name}"),
            &token(),
            &[("x-origin", &uri)],
            None,
        )?;
        require(reply["status"] == 200, "wire control guest failed")?;
        let captured = origins.take_wire()?;
        require(
            captured["method"]
                == if name == "post" {
                    "POST"
                } else if name.starts_with("head") {
                    "HEAD"
                } else {
                    "GET"
                }
                && captured["target"] == format!("/{name}"),
            "wire control method/target changed",
        )?;
        if name == "post" {
            require(
                captured["payload"] == "upload-control",
                "upload control bytes differ",
            )?;
        } else {
            require(
                captured["content_length"] == json!([])
                    && captured["transfer_encoding"] == json!([])
                    && captured["encoded_body_hex"] == "",
                "bodyless request carried framing or bytes",
            )?;
        }
    }
    require(
        net::header_request(port, "/scope", &token(), &[], None)?["status"] == 200,
        "canonical scope control failed",
    )?;
    let uri = format!("http://127.0.0.1:{}", origins.port(0));
    let reply = net::header_request(port, "/handoff", &token(), &[("x-origin", &uri)], None)?;
    require(reply["status"] == 200, "handoff guest failed")?;
    handoff(&body(&reply)?)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        process.check_running()?;
        let text = component_text(log_dir)?;
        if text.contains("PASS: Spin attached Begin/Batch handoff closed at SDK terminal") {
            return Ok(());
        }
        require(
            !text.contains("FAIL: Spin attached") && Instant::now() < deadline,
            "SDK-terminal ownership proof missing",
        )?;
        thread::sleep(Duration::from_millis(10));
    }
}

pub(super) fn run(name: &str, run: &Path, records: &mut Records, batch_only: bool) -> Result<()> {
    match name {
        "cloudflare" => return progress_wire("cloudflare", run, records, batch_only),
        "axum" => return progress_wire("axum", run, records, batch_only),
        "fastly" => return progress_wire("fastly", run, records, batch_only),
        _ => {}
    }
    require(name == "spin", "wire suite requires Spin")?;
    let repo = repository_root();
    let executable = executable("SPIN_BIN", "spin").ok_or("Spin runtime unavailable")?;
    let version = Command::new(&executable).arg("--version").output()?;
    require(version.status.success(), "Spin version probe failed")?;
    let version = String::from_utf8(version.stdout)?;
    let pins = fs::read_to_string(repo.join(".tool-versions"))?;
    let pin = pins
        .lines()
        .find_map(|line| line.strip_prefix("spin "))
        .ok_or("missing Spin pin")?;
    require(
        version.split_whitespace().nth(1) == Some(pin),
        "Spin runtime does not match repository pin",
    )?;
    records.push(json!({"event":"runtime","adapter":"spin","version":version.trim(),"executable":executable,"qualification":false}))?;
    checked(
        Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .current_dir(&repo)
            .env("CARGO_TARGET_DIR", fixture_root().join("target"))
            .env("EDGEZERO_FIXTURE_BUILD_ID", startup::build_id()?)
            .args(["build", "--offline", "--locked", "--manifest-path"])
            .arg(fixture_root().join("Cargo.toml"))
            .args(["-p", "fixture-spin-wire", "--target", "wasm32-wasip2"]),
    )?;
    let relative_wasm = "crates/edgezero-test-harness/fixtures/reusable-app/target/wasm32-wasip2/debug/fixture_spin_wire.wasm";
    let wasm = run.join(relative_wasm);
    copy_tree(&repo.join(relative_wasm), &wasm)?;
    let relative_manifest =
        "crates/edgezero-test-harness/fixtures/reusable-app/crates/fixture-spin-wire/spin.toml";
    let manifest = run.join(relative_manifest);
    copy_tree(&repo.join(relative_manifest), &manifest)?;
    records.push(json!({"event":"build","adapter":"spin","artifact":artifact(&wasm)?,"root_lockfile":artifact(&repo.join("Cargo.lock"))?,"qualification":false}))?;
    let origins = WireOrigins::start()?;
    let port = net::port()?;
    let state = run.join("spin-wire-state");
    let component_logs = run.join("component-logs");
    let log = run.join("spin-wire.log");
    let mut command = Command::new(executable);
    let nonce = startup::command(&mut command, "spin-wire", &startup::build_id()?);
    command
        .args(["up", "--from"])
        .arg(manifest)
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .arg("--state-dir")
        .arg(&state)
        .arg("--log-dir")
        .arg(&component_logs)
        .arg("--disable-cache");
    command.args(["--env", &format!("{}={nonce}", startup::NONCE)]);
    let mut process = None;
    let outcome = (|| -> Result<_> {
        let running = process.insert(Process::start(&mut command, &log, port)?);
        controls(port, &origins, &component_logs, running)?;
        if batch_only {
            batch_recovery::run("spin", port, records)?;
            running.check_running()?;
            return Ok(None);
        }
        let mut matrix = Matrix {
            adapter: "spin",
            endpoint: "/p5",
            port,
            origins: &origins,
            records,
            exchanges: 0,
            dispatches: 0,
        };
        matrix.run()?;
        let baseline = (matrix.exchanges, matrix.dispatches);
        let policy = spin_policy(port, &origins, matrix.records)?;
        normalization::run(port, &origins, matrix.records)?;
        batch_recovery::run("spin", port, matrix.records)?;
        running.check_running()?;
        Ok(Some((baseline, policy)))
    })();
    let result = finish_wire(
        "spin",
        &mut process,
        Some(origins),
        &state,
        records,
        outcome,
    )?;
    let Some(((exchanges, dispatches), (policy_cases, policy_receipts))) = result else {
        return Ok(());
    };
    records.push(json!({"event":"spin-wire-inventory","status":"pass","exchanges":exchanges,"origin_requests":dispatches,
        "bodyless_controls":4,"upload_controls":1,"scope_controls":1,"handoff_controls":1,"qualification":false}))?;
    records.push(
        json!({"event":"spin-policy-inventory","status":"pass","exchanges":policy_cases,
        "origin_requests":policy_receipts,"qualification":false}),
    )?;
    println!("ez_5_acceptance_10_unit_spin: passed");
    println!("ez_5_acceptance_11_wire_spin: passed");
    Ok(())
}

fn progress_wire(
    name: &'static str,
    run: &Path,
    records: &mut Records,
    batch_only: bool,
) -> Result<()> {
    let result = progress_suite(
        name,
        if batch_only {
            "outbound-batch"
        } else {
            "outbound-wire"
        },
        run,
        records,
        |port, origins, records| {
            if batch_only {
                batch_recovery::run(name, port, records)?;
                return Ok(None);
            }
            let mut matrix = Matrix {
                adapter: name,
                endpoint: "/progress/redirect",
                port,
                origins,
                records,
                exchanges: 0,
                dispatches: 0,
            };
            matrix.run()?;
            batch_recovery::run(name, port, matrix.records)?;
            Ok(Some((matrix.exchanges, matrix.dispatches)))
        },
    )?;
    if let Some(Some((exchanges, dispatches))) = result {
        records.push(
            json!({"event":format!("{name}-wire-inventory"),"status":"pass",
            "exchanges":exchanges,"origin_requests":dispatches,"qualification":false}),
        )?;
    }
    Ok(())
}

pub(super) fn progress_suite<T>(
    name: &str,
    suite: &str,
    run: &Path,
    records: &mut Records,
    check: impl FnOnce(u16, &WireOrigins, &mut Records) -> Result<T>,
) -> Result<Option<T>> {
    let workerd = if name == "cloudflare" {
        let Some(workerd) = executable("MINIFLARE_WORKERD_PATH", "workerd") else {
            records.push(json!({"status":"unsupported","adapter":"cloudflare",
                "reason":"set MINIFLARE_WORKERD_PATH to the local workerd executable","qualification":false}))?;
            return Ok(None);
        };
        let version = Command::new(&workerd).arg("--version").output()?;
        require(version.status.success(), "workerd version probe failed")?;
        records.push(json!({"event":"workerd-runtime","adapter":"cloudflare",
            "version":String::from_utf8(version.stdout)?.trim(),"artifact":artifact(&workerd)?,"qualification":false}))?;
        Some(workerd)
    } else {
        None
    };
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(fixture_root())
        .output()?;
    require(
        revision.status.success(),
        "cannot determine wire source revision",
    )?;
    let revision = String::from_utf8(revision.stdout)?;
    let state = if name == "axum" {
        run.join("axum-state")
    } else {
        run.join("retained/worker-state")
    };
    let log = run.join(format!("{name}-{suite}.log"));
    let mut process = None;
    let mut origins = None;
    let outcome: Result<Option<T>> = (|| {
        let Some((mut command, port)) =
            progress::build(name, run, records, revision.trim(), suite)?
        else {
            return Ok(None);
        };
        if let Some(workerd) = workerd {
            command
                .env("MINIFLARE_WORKERD_PATH", workerd)
                .env("WRANGLER_SEND_METRICS", "false")
                .env("WRANGLER_LOG_PATH", run.join("wrangler-logs"));
            records.push(
                json!({"event":"workerd-configuration","adapter":"cloudflare",
                "configuration":fs::read_to_string(run.join("wrangler.toml"))?,"qualification":false}),
            )?;
        }
        let origins = origins.insert(WireOrigins::start()?);
        let running = process.insert(Process::start(&mut command, &log, port)?);
        let result = check(port, origins, records)?;
        running.check_running()?;
        Ok(Some(result))
    })();
    let outcome = finish_wire(name, &mut process, origins, &state, records, outcome);
    settle(
        records,
        &log,
        json!({"adapter":name,"suite":suite}),
        json!({}),
        outcome,
    )
}

fn finish_wire<T>(
    adapter: &str,
    process: &mut Option<Process>,
    origins: Option<WireOrigins>,
    state: &Path,
    records: &mut Records,
    outcome: Result<T>,
) -> Result<T> {
    let process_result = process.as_mut().map_or(Ok(()), Process::finish);
    let origin_result = origins.map_or(Ok(()), WireOrigins::finish);
    let state_result: Result<()> = if state.exists() {
        fs::remove_dir_all(state).map_err(Into::into)
    } else {
        Ok(())
    };
    for (stage, result) in [
        ("process", &process_result),
        ("origin", &origin_result),
        ("state", &state_result),
    ] {
        if let Err(error) = result {
            records.push(json!({"status":"fail","adapter":adapter,"stage":stage,
                "error":error.to_string(),"qualification":false}))?;
        }
    }
    let inventory = outcome?;
    process_result.and(origin_result).and(state_result)?;
    Ok(inventory)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire_error(kind: &str, reason: &str) -> Value {
        json!({"error":{"kind":kind,"reason":reason,
            "status":if kind == "gateway_timeout" {504} else {502},"provider":null}})
    }

    fn wire_report(path: &str, streamed: bool, outcome: Value) -> Value {
        json!({"outcome":outcome,"streamed":streamed,"policies":0,"hop":0,
            "redirects":0,"stop":null,"final_uri":format!("http://127.0.0.1:4001{path}")})
    }

    fn wire_body_report(coding: &str, size: usize, streamed: bool, encoded: bool) -> Value {
        let path = format!("/p5/body/{coding}/{size}");
        let outcome = if size == 10 {
            let mut headers = vec![json!(["x-p5-fixture", b"spin-wire".as_slice()])];
            if coding != "gzip" {
                headers.push(json!([
                    "content-encoding",
                    if coding == "identity" {
                        b"identity".as_slice()
                    } else {
                        b"x-p5-opaque".as_slice()
                    }
                ]));
            }
            json!({"status":200,"bytes":vec![b'a';size],"headers":headers})
        } else {
            wire_error(
                "response_too_large",
                if encoded {
                    "EncodedBody"
                } else if streamed {
                    "DecodedBody"
                } else {
                    "BufferedBody"
                },
            )
        };
        wire_report(&path, streamed, outcome)
    }

    fn spin_artifact() -> Vec<Value> {
        let mut values = Vec::new();
        let first = 4001;
        let second = 4002;
        let mut push = |path: &str,
                        mode: &str,
                        streamed: bool,
                        encoded: bool,
                        requests: Vec<(u16, String)>,
                        report: Value| {
            values.push(json!({"event":"spin-wire-case","path":path,"mode":mode,
                "streamed":streamed,"encoded_cap":encoded,"origin_requests":requests,
                "report":report,"qualification":false}));
        };
        for streamed in [false, true] {
            for coding in ["identity", "gzip", "passthrough"] {
                for size in [10, 11] {
                    let path = format!("/p5/body/{coding}/{size}");
                    let encoded = streamed && coding == "passthrough";
                    for mode in ["send", "follow"] {
                        push(
                            &path,
                            mode,
                            streamed,
                            encoded,
                            vec![(first, path.clone())],
                            wire_body_report(coding, size, streamed, encoded),
                        );
                    }
                }
                let path = format!("/p5/declared/{coding}");
                for mode in ["send", "follow"] {
                    push(
                        &path,
                        mode,
                        streamed,
                        true,
                        vec![(first, path.clone())],
                        wire_report(
                            &path,
                            streamed,
                            wire_error("response_too_large", "EncodedBody"),
                        ),
                    );
                }
            }
        }
        for kind in ["pending", "error", "emptybr", "oversized"] {
            for mode in ["follow", "send", "stop", "limit"] {
                let name = format!("{kind}-{mode}");
                let redirect = format!("/p5/redirect/{kind}/{name}");
                let path = if mode == "limit" {
                    format!("/p5/limit/{kind}/{name}")
                } else {
                    redirect.clone()
                };
                let mut requests = vec![(first, path.clone())];
                let outcome = if mode == "follow" {
                    requests.push((first, format!("/p5/next/{name}")));
                    json!({"status":200,"bytes":b"final".as_slice()})
                } else {
                    if mode == "limit" {
                        requests.push((first, redirect));
                    }
                    match kind {
                        "pending" => wire_error("gateway_timeout", "PerCallTimeout"),
                        "error" => {
                            let mut error = wire_error("bad_gateway", "Protocol");
                            error["error"]["provider"] = json!("Spin { phase: ResponseBodyRead }");
                            error
                        }
                        "emptybr" => wire_error("bad_gateway", "Decode(Brotli)"),
                        _ => wire_error("response_too_large", "BufferedBody"),
                    }
                };
                let mut report = wire_report(&path, false, outcome);
                report["policies"] = json!(usize::from(mode != "send"));
                report["hop"] = json!(usize::from(mode == "limit"));
                if mode == "follow" {
                    report["redirects"] = json!(1);
                    report["final_uri"] = json!(format!("http://127.0.0.1:{first}/p5/next/{name}"));
                }
                push(&path, mode, false, false, requests, report);
            }
        }
        for mode in ["stop", "limit"] {
            let redirect = format!("/p5/redirect/clean/clean-{mode}");
            let path = if mode == "limit" {
                format!("/p5/limit/clean/clean-{mode}")
            } else {
                redirect.clone()
            };
            let mut requests = vec![(first, path.clone())];
            if mode == "limit" {
                requests.push((first, redirect));
            }
            let mut report = wire_report(
                &path,
                false,
                json!({"status":302,"bytes":b"found".as_slice()}),
            );
            report["stop"] = json!(if mode == "limit" {
                "LimitReached"
            } else {
                "StoppedByPolicy"
            });
            report["redirects"] = json!(usize::from(mode == "limit"));
            push(&path, mode, false, false, requests, report);
        }
        push(
            "/p5/cross",
            "send",
            false,
            false,
            vec![(first, "/p5/cross".into())],
            wire_report("/p5/cross", false, json!({"status":302})),
        );
        let mut report = wire_report(
            "/p5/final",
            false,
            json!({"status":200,"bytes":b"final".as_slice()}),
        );
        report["redirects"] = json!(1);
        report["final_uri"] = json!(format!("http://127.0.0.1:{second}/p5/final"));
        push(
            "/p5/cross",
            "follow",
            false,
            false,
            vec![(first, "/p5/cross".into()), (second, "/p5/final".into())],
            report,
        );
        for coding in ["identity", "gzip", "passthrough"] {
            for size in [10, 11] {
                let final_path = format!("/p5/body/{coding}/{size}");
                let path = format!("/p5/to-body/{coding}/{size}");
                let encoded = coding == "passthrough";
                let report = wire_body_report(coding, size, true, encoded);
                push(
                    &final_path,
                    "send",
                    true,
                    encoded,
                    vec![(first, final_path.clone())],
                    report.clone(),
                );
                let mut followed = report;
                followed["redirects"] = json!(1);
                followed["policies"] = json!(1);
                push(
                    &path,
                    "follow",
                    true,
                    encoded,
                    vec![(first, path.clone()), (first, final_path)],
                    followed,
                );
            }
        }
        values.extend((0..26).map(|index| {
            let token = format!("case-{index}");
            let path = PolicyCase::new(index).unwrap().path(&token);
            json!({"event":"spin-policy-case","case":index,"token":token,"path":path,
                "origin_requests":[[1,path]],"report":policy_report(index)})
        }));
        values.push(json!({"event":"spin-wire-inventory","status":"pass","exchanges":68,"origin_requests":84,"qualification":false}));
        values.push(json!({"event":"spin-policy-inventory","status":"pass","exchanges":26,"origin_requests":26,"qualification":false}));
        values.extend(normalization::tests::artifact(first));
        values.extend(batch_recovery::tests::artifact("spin"));
        values
    }

    #[test]
    fn persisted_spin_redirect_accepts_legitimate_repeated_cases() {
        let valid = spin_artifact();
        assert_eq!(valid[18], valid[56]);
        validate_spin_artifact(&valid).unwrap();
    }

    #[test]
    fn persisted_spin_redirect_replay_requires_exact_consumption() {
        let valid = spin_artifact();
        let wire = valid[..68].iter().collect::<Vec<_>>();
        replay_spin_wire(wire.clone()).unwrap();
        assert!(replay_spin_wire(wire[..67].to_vec()).is_err());
        let mut extra = wire;
        extra.push(&valid[0]);
        assert!(
            replay_spin_wire(extra)
                .unwrap_err()
                .to_string()
                .contains("unexpected extra Spin redirect case")
        );
    }

    #[test]
    fn persisted_spin_redirect_rejects_count_preserving_duplicates() {
        let valid = spin_artifact();
        for (missing, duplicate) in [(0, 2), (0, 1), (36, 39), (56, 58)] {
            let mut changed = valid.clone();
            assert_eq!(
                changed[missing]["origin_requests"]
                    .as_array()
                    .unwrap()
                    .len(),
                changed[duplicate]["origin_requests"]
                    .as_array()
                    .unwrap()
                    .len()
            );
            changed[missing] = changed[duplicate].clone();
            assert!(
                validate_spin_artifact(&changed).is_err(),
                "replacement {missing} with {duplicate} passed"
            );
        }
        let mut reordered = valid;
        reordered.swap(0, 2);
        assert!(validate_spin_artifact(&reordered).is_err());
    }

    #[test]
    fn persisted_spin_redirect_rejects_foreign_reports() {
        let valid = spin_artifact();
        let mut foreign = valid.clone();
        foreign[1]["report"] = valid[3]["report"].clone();
        assert!(validate_spin_artifact(&foreign).is_err());
        for (index, pointer, value) in [
            (
                1,
                "/report/final_uri",
                json!("http://127.0.0.1:4001/foreign"),
            ),
            (36, "/report/policies", json!(0)),
            (37, "/report/outcome/error/reason", json!("Protocol")),
            (
                41,
                "/report/outcome/error/provider",
                json!("Spin { phase: Dispatch }"),
            ),
            (55, "/report/redirects", json!(0)),
            (57, "/report/streamed", json!(false)),
        ] {
            let mut changed = valid.clone();
            *changed[index].pointer_mut(pointer).unwrap() = value;
            assert!(
                validate_spin_artifact(&changed).is_err(),
                "{index}: {pointer}"
            );
        }
        let mut changed = valid;
        for index in [0, 1] {
            changed[index]["report"]["outcome"]["bytes"] = json!(b"foreign".as_slice());
        }
        assert!(validate_spin_artifact(&changed).is_err());
    }

    #[test]
    fn persisted_spin_redirect_rejects_foreign_receipts_and_metadata() {
        let valid = spin_artifact();
        for (index, pointer, value) in [
            (0, "/origin_requests/0", json!("not a receipt")),
            (0, "/origin_requests/0/0", json!(0)),
            (0, "/origin_requests/0/0", json!(4003)),
            (2, "/origin_requests/0/0", json!(4003)),
            (2, "/origin_requests/0/0", json!(65536)),
            (2, "/origin_requests/0/1", json!("/foreign")),
            (55, "/origin_requests/1/0", json!(4001)),
            (55, "/origin_requests/1/0", json!(4003)),
            (55, "/origin_requests/1/1", json!("/foreign")),
            (0, "/path", json!("/foreign")),
            (0, "/mode", json!("follow")),
            (0, "/streamed", json!(true)),
            (0, "/encoded_cap", json!(true)),
            (0, "/qualification", json!(true)),
        ] {
            let mut changed = valid.clone();
            *changed[index].pointer_mut(pointer).unwrap() = value;
            assert!(
                validate_spin_artifact(&changed).is_err(),
                "{index}: {pointer}"
            );
        }
    }

    #[test]
    fn persisted_spin_inventory_rejects_missing_duplicate_failed_and_changed_records() {
        let valid = spin_artifact();
        validate_spin_artifact(&valid).unwrap();
        let mut mutations = vec![Vec::new(), valid[..valid.len() - 1].to_vec()];
        for index in [0, 67, 68, 93, 94, 95] {
            let mut missing = valid.clone();
            missing.remove(index);
            mutations.push(missing);
            let mut duplicate = valid.clone();
            duplicate.push(valid[index].clone());
            mutations.push(duplicate);
        }
        for (index, pointer, value) in [
            (94, "/qualification", json!(true)),
            (94, "/status", json!("fail")),
            (94, "/exchanges", json!(67)),
            (95, "/origin_requests", json!(25)),
            (68, "/case", json!(1)),
            (68, "/report/request/decoded_cap", json!(11)),
            (68, "/report/source/released", json!(false)),
            (68, "/origin_requests/0/1", json!("/foreign")),
            (68, "/token", json!("foreign")),
        ] {
            let mut changed = valid.clone();
            *changed[index].pointer_mut(pointer).unwrap() = value;
            mutations.push(changed);
        }
        let mut failed = valid;
        failed.push(json!({"status":"fail","stage":"late-cleanup"}));
        mutations.push(failed);
        for (index, changed) in mutations.into_iter().enumerate() {
            assert!(
                validate_spin_artifact(&changed).is_err(),
                "mutation {index} passed"
            );
        }
    }

    fn policy_report(index: usize) -> Value {
        let gzip = index >= 16;
        let variant = index.saturating_sub(16) % 5;
        let original = gzip || index & 8 != 0;
        let streamed = if gzip { variant >= 3 } else { index & 4 != 0 };
        let declared = if gzip { index >= 21 } else { index & 2 != 0 };
        let length = if gzip { 23 } else { 10 + (index & 1) };
        let reason = if !gzip && length == 11 {
            Some("DecodedBody")
        } else if gzip && matches!(variant, 1 | 4) {
            Some("EncodedBody")
        } else if gzip && variant == 2 {
            Some("BufferedBody")
        } else {
            None
        };
        let outcome = if let Some(reason) = reason {
            json!({"error":{"kind":"response_too_large","status":502,"reason":reason,"provider":null}})
        } else {
            let bytes = if gzip {
                vec![
                    31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 75, 76, 132, 3, 0, 146, 93, 70, 85, 11, 0, 0,
                    0,
                ]
            } else {
                vec![b'a'; length]
            };
            let mut headers = json!([
                [
                    "content-encoding",
                    if gzip {
                        b"gzip".as_slice()
                    } else {
                        b"identity".as_slice()
                    }
                ],
                ["content-type", b"application/octet-stream".as_slice()],
                ["x-p5-fixture", b"spin-wire".as_slice()]
            ]);
            if declared {
                headers
                    .as_array_mut()
                    .unwrap()
                    .push(json!(["content-length", length.to_string().as_bytes()]));
            }
            json!({"status":200,"headers":headers,"bytes":bytes,"streamed":streamed})
        };
        json!({"case":index,
            "request":{"decoding":if original {"original"} else {"decode"},"streamed":streamed,
                "decoded_cap":10,"encoded_cap":if gzip {if matches!(variant,1|4) {22} else {23}} else {20},
                "final_cap":if streamed {None} else {Some(if gzip {if variant==2 {22} else {23}} else {20})},"decoder_cap":0},
            "outcome":outcome,"error_stage":reason.map(|_| if declared || !streamed {"send"} else {"body"}),
            "source":{"polls":usize::from(!(declared && reason.is_some())),"released":true}})
    }

    #[test]
    fn policy_reports_validate_all_twenty_six_cases() {
        for index in 0..26 {
            validate_policy(index, &policy_report(index)).unwrap();
        }
        assert!(validate_policy(26, &policy_report(0)).is_err());
    }

    #[test]
    fn policy_reports_reject_mutated_configuration_source_and_outcomes() {
        for index in 0..26 {
            let valid = policy_report(index);
            validate_policy(index, &valid).unwrap();
            for (pointer, value) in [
                ("/case", json!(26)),
                ("/request/decoding", json!("other")),
                (
                    "/request/streamed",
                    json!(!valid["request"]["streamed"].as_bool().unwrap()),
                ),
                ("/request/decoded_cap", json!(11)),
                ("/request/encoded_cap", json!(99)),
                ("/request/final_cap", json!(99)),
                ("/request/decoder_cap", json!(1)),
                ("/source/polls", Value::Null),
                ("/source/polls", json!(-1)),
                ("/source/released", json!(false)),
                ("/error_stage", json!("reporter")),
            ] {
                let mut changed = valid.clone();
                *changed.pointer_mut(pointer).unwrap() = value;
                assert!(
                    validate_policy(index, &changed).is_err(),
                    "{index}: {pointer}"
                );
            }
            let mut changed = valid.clone();
            changed["source"]["polls"] = json!(usize::from(valid["source"]["polls"] == 0));
            assert!(validate_policy(index, &changed).is_err());
            for field in ["case", "request", "source", "outcome", "error_stage"] {
                let mut changed = valid.clone();
                changed.as_object_mut().unwrap().remove(field);
                assert!(
                    validate_policy(index, &changed).is_err(),
                    "{index}: missing {field}"
                );
            }
            if valid["outcome"].get("error").is_some() {
                for (field, value) in [
                    ("kind", json!("bad_gateway")),
                    ("status", json!(500)),
                    ("reason", json!("Protocol")),
                    ("provider", json!("source-error")),
                ] {
                    let mut changed = valid.clone();
                    changed["outcome"]["error"][field] = value;
                    assert!(
                        validate_policy(index, &changed).is_err(),
                        "{index}: {field}"
                    );
                }
                let mut changed = valid.clone();
                changed["error_stage"] = json!(if valid["error_stage"] == "send" {
                    "body"
                } else {
                    "send"
                });
                assert!(validate_policy(index, &changed).is_err());
            } else {
                for (pointer, value) in [
                    ("/outcome/status", json!(201)),
                    ("/outcome/bytes/0", json!(255)),
                    (
                        "/outcome/streamed",
                        json!(!valid["request"]["streamed"].as_bool().unwrap()),
                    ),
                    ("/outcome/headers/0/1", json!(b"wrong".as_slice())),
                    ("/error_stage", json!("body")),
                ] {
                    let mut changed = valid.clone();
                    *changed.pointer_mut(pointer).unwrap() = value;
                    assert!(
                        validate_policy(index, &changed).is_err(),
                        "{index}: {pointer}"
                    );
                }
                let mut changed = valid.clone();
                changed["outcome"]["headers"]
                    .as_array_mut()
                    .unwrap()
                    .push(valid["outcome"]["headers"][0].clone());
                assert!(validate_policy(index, &changed).is_err());
            }
        }
    }

    #[test]
    fn policy_inventory_requires_all_unique_cases_and_exact_receipts() {
        let cases: Vec<_> = (0..26).collect();
        validate_policy_inventory(&cases, 26).unwrap();
        for receipts in [0, 25, 27] {
            assert!(validate_policy_inventory(&cases, receipts).is_err());
        }
        assert!(validate_policy_inventory(&cases[..25], 26).is_err());
        let mut duplicate = cases.clone();
        duplicate[25] = 0;
        assert!(validate_policy_inventory(&duplicate, 26).is_err());
        duplicate[25] = 26;
        assert!(validate_policy_inventory(&duplicate, 26).is_err());
    }

    #[test]
    fn pre_startup_failure_removes_partial_state_and_preserves_primary_error() {
        let run = std::env::temp_dir().join(format!("wire-cleanup-{}", token()));
        let state = run.join("retained/worker-state");
        fs::create_dir_all(&state).unwrap();
        fs::write(state.join("partial-value"), b"synthetic").unwrap();
        let mut records = Records {
            path: run.join("events.jsonl"),
            values: Vec::new(),
        };
        let outcome: Result<()> = Err("build failed before runtime startup".into());
        let error =
            finish_wire("cloudflare", &mut None, None, &state, &mut records, outcome).unwrap_err();
        assert_eq!(error.to_string(), "build failed before runtime startup");
        assert!(!state.exists());
        assert!(records.values.is_empty());
        fs::remove_dir_all(run).unwrap();
    }

    #[test]
    fn late_origin_failure_is_not_hidden_by_success_or_primary_check_failure() {
        for primary in [None, Some("primary lifetime check failed")] {
            let run = std::env::temp_dir().join(format!("wire-late-cleanup-{}", token()));
            let state = run.join("worker-state");
            fs::create_dir_all(&state).unwrap();
            let mut records = Records {
                path: run.join("events.jsonl"),
                values: Vec::new(),
            };
            let origins = WireOrigins::start().unwrap();
            origins.arm("invalid++");
            let outcome = primary.map_or(Ok(()), |error| Err(error.into()));
            let error = finish_wire(
                "axum",
                &mut None,
                Some(origins),
                &state,
                &mut records,
                outcome,
            )
            .unwrap_err();
            assert_eq!(error.to_string(), primary.unwrap_or("invalid P5 gate name"));
            assert!(!state.exists());
            assert_eq!(records.values.len(), 1);
            assert_eq!(records.values[0]["stage"], "origin");
            assert_eq!(records.values[0]["status"], "fail");
            fs::remove_dir_all(run).unwrap();
        }
    }

    #[test]
    fn source_error_expectations_are_provider_specific() {
        assert_eq!(source_error_reason("spin"), "Protocol");
        assert_eq!(source_error_reason("cloudflare"), "Transport");
        assert_eq!(source_error_reason("axum"), "Transport");
        assert_eq!(source_error_reason("fastly"), "Transport");
    }

    #[test]
    fn workerd_source_errors_require_the_observed_typed_reason_phase_and_cause() {
        let valid = json!({"outcome":{"error":{"kind":"bad_gateway","status":502,
            "reason":"Transport","provider":"Cloudflare { phase: ResponseBodyRead, cause: Unknown }"}}});
        source_error(&valid, "cloudflare").unwrap();
        for (field, value) in [
            ("reason", json!("Protocol")),
            ("provider", Value::Null),
            (
                "provider",
                json!("Cloudflare { phase: Fetch, cause: Unknown }"),
            ),
            (
                "provider",
                json!("Cloudflare { phase: ResponseBodyRead, cause: TypeError }"),
            ),
            ("status", json!(500)),
        ] {
            let mut changed = valid.clone();
            changed["outcome"]["error"][field] = value;
            assert!(source_error(&changed, "cloudflare").is_err(), "{changed}");
        }
    }

    #[test]
    fn handoff_retains_exact_bytes_and_all_results() {
        for bytes in [b"originbid-Abid-Bbid-C", b"originbid-Cbid-Abid-B"] {
            handoff(bytes).unwrap();
        }
        for index in [0, 6, 11, 16] {
            let mut altered = b"originbid-Abid-Bbid-C".to_vec();
            altered[index] |= 0x80;
            assert!(handoff(&altered).is_err());
        }
        assert!(handoff(b"originbid-Abid-Abid-C").is_err());
        assert!(handoff(b"origin").is_err());
    }

    #[test]
    fn body_capture_does_not_clamp_or_erase_nontext_bytes() {
        assert_eq!(
            body(&json!({"body_bytes":[0,255,128]})).unwrap(),
            [0, 255, 128]
        );
        assert!(body(&json!({"body_bytes":[256]})).is_err());
        assert!(body(&json!({})).is_err());
    }
}

#![allow(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::wildcard_enum_match_arm,
    reason = "one independent oracle covers the finite fixture contract in request/error/success order across crates with different lint policies"
)]

use super::vectors::{
    Case, Coding, ErrorCase, HeaderCase, Kind, MarkerCase, coding_name, policy_payload,
};
use serde_json::{Value, json};

fn ensure(condition: bool, message: &str) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(message.to_owned())
    }
}

pub fn error_spec(
    case: Case,
    adapter: &str,
) -> Option<(
    &'static str,
    u16,
    Option<&'static str>,
    Option<&'static str>,
)> {
    match case.kind {
        Kind::Policy {
            coding,
            declared,
            size,
        } => {
            if declared {
                Some(("response_too_large", 502, Some("EncodedBody"), None))
            } else if size == 11 {
                Some((
                    "response_too_large",
                    502,
                    Some(if case.streamed {
                        if coding == Coding::Passthrough {
                            "EncodedBody"
                        } else {
                            "DecodedBody"
                        }
                    } else {
                        "BufferedBody"
                    }),
                    None,
                ))
            } else {
                None
            }
        }
        Kind::OriginalIdentity { size: 2.., .. } => {
            Some(("response_too_large", 502, Some("DecodedBody"), None))
        }
        Kind::Header(HeaderCase::Bytes | HeaderCase::MarkerBytesOn) => {
            Some(("response_too_large", 502, Some("HeaderBytes"), None))
        }
        Kind::Header(HeaderCase::Count | HeaderCase::Tie | HeaderCase::MarkerCountOn) => {
            Some(("response_too_large", 502, Some("HeaderCount"), None))
        }
        Kind::Delay { deadline, .. } => Some((
            "gateway_timeout",
            504,
            None,
            Some(if deadline {
                "RequestDeadline"
            } else {
                "PerCallTimeout"
            }),
        )),
        Kind::Error(ErrorCase::DecoderMemory) => {
            Some(("response_too_large", 502, Some("DecoderMemory"), None))
        }
        Kind::Error(ErrorCase::BrotliWindow) => {
            Some(("response_too_large", 502, Some("BrotliWindow"), None))
        }
        Kind::Error(ErrorCase::Gzip) => Some(("bad_gateway", 502, Some("Decode(Gzip)"), None)),
        Kind::Error(ErrorCase::Body) => Some((
            "bad_gateway",
            502,
            Some(if adapter == "spin" {
                "Protocol"
            } else {
                "Transport"
            }),
            None,
        )),
        Kind::Error(ErrorCase::Head) => Some((
            "bad_gateway",
            502,
            (adapter == "cloudflare").then_some("Unspecified"),
            None,
        )),
        Kind::GetBody => Some(("bad_request", 400, None, None)),
        Kind::Upload { cap: 0..=2 } => Some(("payload_too_large", 413, None, None)),
        Kind::Authority if adapter == "spin" => Some(("bad_request", 400, None, None)),
        _ => None,
    }
}

pub fn expected_bytes(case: Case) -> Vec<u8> {
    match case.kind {
        Kind::Policy { size, .. } | Kind::OriginalIdentity { size, .. } => {
            vec![b'a'; usize::from(size)]
        }
        Kind::Coding {
            coding,
            original,
            partial,
        } => {
            if original || partial {
                policy_payload(coding, 10, false).to_vec()
            } else {
                b"aaaaaaaaaa".to_vec()
            }
        }
        Kind::Rechunk { coding, original } => {
            if original {
                policy_payload(coding, 10, false).to_vec()
            } else {
                b"aaaaaaaaaa".to_vec()
            }
        }
        Kind::Passthrough => b"opaque".to_vec(),
        Kind::Bodyless205 => Vec::new(),
        _ => b"ok".to_vec(),
    }
}

pub fn body_error(case: Case) -> bool {
    case.streamed
        && matches!(
            case.kind,
            Kind::Policy {
                declared: false,
                size: 11,
                ..
            } | Kind::OriginalIdentity {
                declared: false,
                size: 2..,
                ..
            } | Kind::Delay { body: true, .. }
                | Kind::Error(ErrorCase::Body | ErrorCase::BrotliWindow | ErrorCase::Gzip)
        )
}

pub fn request_spec(case: Case, adapter: &str, token: &str) -> Value {
    let timeout: u64 = match case.kind {
        Kind::Delay {
            deadline: false, ..
        } => 150,
        Kind::Delay { deadline: true, .. } => 1000,
        Kind::Connect => 2000,
        _ => 3000,
    };
    let policy = matches!(case.kind, Kind::Policy { .. });
    let original = matches!(
        case.kind,
        Kind::Coding { original: true, .. }
            | Kind::OriginalIdentity { .. }
            | Kind::Rechunk { original: true, .. }
    );
    let raw = matches!(
        case.kind,
        Kind::Coding { original: true, .. } | Kind::Coding { partial: true, .. }
    );
    let decoded: u64 =
        if raw || matches!(case.kind, Kind::OriginalIdentity { .. } | Kind::Passthrough) {
            1
        } else if policy && case.streamed {
            10
        } else {
            64
        };
    let encoded: u64 = if matches!(case.kind, Kind::Policy { declared: true, .. })
        || case.streamed
            && matches!(
                case.kind,
                Kind::Policy {
                    coding: Coding::Passthrough,
                    ..
                }
            ) {
        10
    } else {
        256
    };
    let response: u64 = if policy { 10 } else { 64 };
    let header_bytes = match case.kind {
        Kind::Header(HeaderCase::Bytes | HeaderCase::Tie) => Some(0_u64),
        Kind::Header(HeaderCase::MarkerBytesOff | HeaderCase::MarkerBytesOn) => Some(
            case.reply(token)
                .fields
                .iter()
                .filter(|(name, _)| adapter != "spin" || *name != "connection")
                .map(|(name, value)| {
                    u64::try_from(name.len().saturating_add(value.len()))
                        .expect("bounded fixture field")
                })
                .sum::<u64>()
                .saturating_add(15),
        ),
        _ => None,
    };
    let header_count = match case.kind {
        Kind::Header(HeaderCase::Count | HeaderCase::Tie) => Some(0_u64),
        Kind::Header(HeaderCase::MarkerCountOff | HeaderCase::MarkerCountOn) => {
            Some(if adapter == "spin" { 5 } else { 6 })
        }
        Kind::Marker(MarkerCase::Off) => Some(if adapter == "spin" { 6 } else { 7 }),
        _ => None,
    };
    let upload: u64 = if let Kind::Upload { cap } = case.kind {
        cap
    } else {
        64
    };
    json!({"authority":(case.kind==Kind::Authority).then_some("virtual.example:8443"),
        "body_bytes":case.request_body().as_bytes(),"cache":if case.kind==Kind::Cache {"Bypass"} else {"PlatformDefault"},
        "chunk_bytes":matches!(case.kind,Kind::Rechunk{..}).then_some(2_u64),
        "connect_ms":(case.kind==Kind::Connect).then_some(200_u64),"decoding":if original {"Original"} else {"Decode"},
        "deadline":matches!(case.kind,Kind::Delay{deadline:true,..}),"decoded_cap":decoded,
        "decoder_cap":if raw||matches!(case.kind,Kind::Error(ErrorCase::DecoderMemory)) {0_u64} else {32*1024*1024},
        "encoded_cap":encoded,"header_bytes":header_bytes,"header_count":header_count,"method":case.request_method(),
        "mode":if case.streamed {"Streamed".to_owned()} else {format!("Buffered {{ max_bytes: {response} }}")},
        "proxy":match case.kind {Kind::Marker(MarkerCase::Off)=>"Off",Kind::Marker(MarkerCase::On)=>"On",_=>"Inherit"},
        "response_cap":if case.streamed {None} else {Some(response)},"timeout_ms":timeout,"upload_cap":upload,
        "window_bits":if matches!(case.kind,Kind::Error(ErrorCase::BrotliWindow)) {10_u64} else {30}})
}

fn field(fields: &Value, name: &str) -> Vec<Value> {
    fields.as_array().map_or_else(Vec::new, |values| {
        values
            .iter()
            .filter(|entry| entry[0] == name)
            .map(|entry| entry[1].clone())
            .collect()
    })
}

pub fn validate(case: Case, adapter: &str, token: &str, capture: &Value) -> Result<(), String> {
    ensure(
        capture.as_object().is_some_and(|value| {
            value.len() == 11
                && [
                    "outcome",
                    "request",
                    "client_marker",
                    "head",
                    "head_us",
                    "terminal_us",
                    "reads_at_head",
                    "streamed",
                    "stage",
                    "chunks",
                    "native",
                ]
                .iter()
                .all(|field| value.contains_key(*field))
        }),
        "missing paired capture field",
    )?;
    let outcome = &capture["outcome"];
    let request = &capture["request"];
    ensure(
        request == &request_spec(case, adapter, token),
        "paired effective request readback differs",
    )?;
    ensure(
        capture["client_marker"] == case.client_marker()
            && request["method"] == case.request_method()
            && request["body_bytes"] == json!(case.request_body().as_bytes())
            && request["deadline"] == matches!(case.kind, Kind::Delay { deadline: true, .. }),
        "paired request identity or defaults differ",
    )?;
    let expected_timeout: u64 = match case.kind {
        Kind::Delay {
            deadline: false, ..
        } => 150,
        Kind::Delay { deadline: true, .. } => 1000,
        Kind::Connect => 2000,
        _ => 3000,
    };
    ensure(
        request["timeout_ms"] == expected_timeout
            && request["connect_ms"]
                == if case.kind == Kind::Connect {
                    json!(200_u64)
                } else {
                    Value::Null
                }
            && request["cache"]
                == if case.kind == Kind::Cache {
                    "Bypass"
                } else {
                    "PlatformDefault"
                }
            && request["authority"]
                == if case.kind == Kind::Authority {
                    json!("virtual.example:8443")
                } else {
                    Value::Null
                },
        "paired accepted option readback differs",
    )?;
    let cap: u64 = if matches!(case.kind, Kind::Policy { .. }) {
        10
    } else {
        64
    };
    ensure(
        request["response_cap"]
            == if case.streamed {
                Value::Null
            } else {
                json!(cap)
            }
            && request["mode"]
                == if case.streamed {
                    "Streamed".to_owned()
                } else {
                    format!("Buffered {{ max_bytes: {cap} }}")
                },
        "paired response mode or buffered cap differs",
    )?;
    let terminal = capture["terminal_us"]
        .as_u64()
        .ok_or("missing paired terminal measurement")?;
    if matches!(case.kind, Kind::Delay { .. }) {
        ensure(
            (150_000..500_000).contains(&terminal),
            "paired timeout preceded its budget or waited for the withheld origin response",
        )?;
    }
    if let Some((kind, status, reason, cause)) = error_spec(case, adapter) {
        let error = &outcome["error"];
        ensure(
            outcome.as_object().is_some_and(|value| value.len() == 1)
                && error.as_object().is_some_and(|value| {
                    value.len() == 7
                        && [
                            "kind",
                            "status",
                            "reason",
                            "cause",
                            "provider",
                            "rejection",
                            "upload_origin",
                        ]
                        .iter()
                        .all(|name| value.contains_key(*name))
                })
                && error["kind"] == kind
                && error["status"] == status
                && error["cause"] == json!(cause),
            "wrong paired error category or timeout source",
        )?;
        if matches!(case.kind, Kind::Error(ErrorCase::Head)) && adapter != "cloudflare" {
            ensure(
                matches!(error["reason"].as_str(), Some("Transport" | "Protocol")),
                "wrong paired native head-error classification",
            )?;
        } else {
            ensure(
                error["reason"] == json!(reason),
                "wrong paired typed error reason",
            )?;
        }
        ensure(
            error["upload_origin"]
                == if status == 413 {
                    json!("OutboundUpload")
                } else {
                    Value::Null
                },
            "wrong paired upload origin",
        )?;
        let rejection = if case.kind == Kind::GetBody {
            Some("get_body")
        } else if case.kind == Kind::Authority && adapter == "spin" {
            Some("unsupported_authority")
        } else {
            None
        };
        ensure(
            error["rejection"] == json!(rejection),
            "wrong paired preflight rejection",
        )?;
        let native_error = matches!(case.kind, Kind::Error(ErrorCase::Head | ErrorCase::Body));
        if native_error {
            let platform = match adapter {
                "axum" | "core" => "Axum",
                "cloudflare" => "Cloudflare",
                "fastly" => "Fastly",
                "spin" => "Spin",
                _ => return Err("unknown paired adapter".to_owned()),
            };
            let phase = if matches!(case.kind, Kind::Error(ErrorCase::Head)) {
                if adapter == "fastly" {
                    "ResponseHeaders"
                } else {
                    "Send"
                }
            } else {
                "ResponseBodyRead"
            };
            let detail = &error["provider"];
            ensure(
                detail.as_object().is_some_and(|value| {
                    value.len() == 3
                        && ["adapter", "phase", "cause"]
                            .iter()
                            .all(|field| value.contains_key(*field))
                }) && detail["adapter"] == platform
                    && detail["phase"] == phase
                    && detail["cause"]
                        .as_str()
                        .is_some_and(|value| !value.is_empty() && value.len() <= 256),
                "lost paired typed provider detail",
            )?;
        } else {
            ensure(
                matches!(case.kind, Kind::Delay { .. })
                    || error.get("provider") == Some(&Value::Null),
                "invented paired provider detail",
            )?;
        }
        ensure(
            capture["stage"]
                == if body_error(case) {
                    "body"
                } else {
                    "operation"
                },
            "paired error occurred in wrong response stage",
        )?;
        ensure(
            capture["streamed"]
                == if body_error(case) {
                    json!(true)
                } else {
                    Value::Null
                },
            "paired error lost streamed response state",
        )?;
        if body_error(case) {
            ensure(
                capture["head"]["status"] == case.reply(token).status
                    && capture["head_us"].as_u64().is_some()
                    && capture["reads_at_head"] == 0_u64
                    && field(&capture["head"]["headers"], "x-request-token")
                        == vec![json!(token.as_bytes())],
                "paired body error lost its live response head",
            )?;
        } else {
            ensure(
                capture.get("head") == Some(&Value::Null)
                    && capture.get("head_us") == Some(&Value::Null)
                    && capture.get("reads_at_head") == Some(&Value::Null)
                    && capture["chunks"] == json!([]),
                "paired operation error exposed a false response head",
            )?;
        }
        return Ok(());
    }
    let status: u16 = match case.kind {
        Kind::Coding { partial: true, .. } => 206,
        Kind::Bodyless205 => 205,
        Kind::Upstream413 => 413,
        _ => 200,
    };
    ensure(
        outcome.as_object().is_some_and(|value| value.len() == 3)
            && outcome["status"] == status
            && outcome["bytes"] == json!(expected_bytes(case))
            && capture["head"] == json!({"status":status,"headers":outcome["headers"]})
            && capture["head_us"]
                .as_u64()
                .is_some_and(|head| head <= terminal)
            && capture["stage"] == "complete"
            && capture["streamed"] == (case.streamed && case.kind != Kind::Bodyless205),
        "paired status, bytes or streaming mode differ",
    )?;
    let fields = &outcome["headers"];
    ensure(
        fields.as_array().is_some_and(|values| {
            values.iter().all(|entry| {
                entry.as_array().is_some_and(|entry| entry.len() == 2)
                    && entry[0].as_str().is_some()
                    && serde_json::from_value::<Vec<u8>>(entry[1].clone()).is_ok()
            })
        }),
        "malformed paired response headers",
    )?;
    ensure(
        field(fields, "x-transport-fixture") == vec![json!(b"native".as_slice())]
            && field(fields, "x-request-token") == vec![json!(token.as_bytes())]
            && ["connection", "proxy-connection", "transfer-encoding"]
                .iter()
                .all(|name| field(fields, name).is_empty()),
        "paired normalization or correlation differs",
    )?;
    // Spin's documented WASI limitation hides Connection before EdgeZero sees it.
    // The core companion receives the nomination and must still remove the field.
    ensure(
        field(fields, "x-private")
            == if adapter == "spin" && case.kind == Kind::Header(HeaderCase::Normalize) {
                vec![json!(b"removed".as_slice())]
            } else {
                Vec::new()
            },
        "paired adapter-visible nomination behavior changed",
    )?;
    let marker = match case.kind {
        Kind::Marker(MarkerCase::Off | MarkerCase::InheritOff) => Some("upstream"),
        _ => case.client_marker().then_some(adapter),
    };
    let marker = if matches!(case.kind, Kind::Marker(MarkerCase::On)) {
        Some(adapter)
    } else {
        marker
    };
    ensure(
        field(fields, "x-edgezero-proxy")
            == marker
                .into_iter()
                .map(|value| json!(value.as_bytes()))
                .collect::<Vec<_>>(),
        "paired explicit marker policy differs",
    )?;
    if case.kind == Kind::Header(HeaderCase::Duplicate) {
        ensure(
            field(fields, "set-cookie") == vec![json!(b"a=1".as_slice()), json!(b"b=2".as_slice())],
            "paired duplicate fields changed",
        )?;
    }
    if let Kind::OriginalIdentity {
        declared,
        explicit,
        size,
    } = case.kind
    {
        ensure(
            field(fields, "content-encoding")
                == if explicit {
                    vec![json!(b"identity".as_slice())]
                } else {
                    Vec::new()
                }
                && field(fields, "content-length")
                    == if declared {
                        vec![json!(size.to_string().as_bytes())]
                    } else {
                        Vec::new()
                    },
            "paired Original identity fields changed",
        )?;
    }
    if let Kind::Coding {
        coding,
        original,
        partial,
    } = case.kind
    {
        if original || partial {
            ensure(
                field(fields, "content-encoding") == vec![json!(coding_name(coding).as_bytes())]
                    && field(fields, "content-length")
                        == vec![json!(
                            policy_payload(coding, 10, false)
                                .len()
                                .to_string()
                                .as_bytes()
                        )],
                "paired original representation fields changed",
            )?;
            if partial {
                ensure(
                    field(fields, "etag") == vec![json!(b"\"original\"".as_slice())]
                        && field(fields, "content-range") == vec![json!(b"bytes 0-3/8".as_slice())]
                        && field(fields, "vary") == vec![json!(b"Accept-Encoding".as_slice())],
                    "paired partial representation metadata changed",
                )?;
            }
        } else {
            ensure(
                field(fields, "content-encoding").is_empty()
                    && field(fields, "content-length").is_empty(),
                "paired decoded representation fields retained",
            )?;
        }
    }
    if case.streamed && case.kind != Kind::Bodyless205 {
        ensure(
            capture["reads_at_head"] == 0_u64,
            "paired streamed response was eagerly read",
        )?;
    }
    if case.kind == Kind::StreamLive {
        ensure(
            capture["head_us"]
                .as_u64()
                .is_some_and(|head| head < 250_000)
                && terminal >= 250_000,
            "paired streamed collection waited for the withheld body",
        )?;
    }
    if matches!(case.kind, Kind::Rechunk { .. }) {
        let chunks = capture["chunks"]
            .as_array()
            .ok_or("missing paired chunks")?;
        ensure(
            !chunks.is_empty()
                && chunks.iter().all(|chunk| {
                    chunk
                        .as_array()
                        .is_some_and(|bytes| !bytes.is_empty() && bytes.len() <= 2)
                }),
            "paired chunk cap or streaming evidence differs",
        )?;
        let bytes = chunks
            .iter()
            .flat_map(|chunk| chunk.as_array().unwrap().iter().cloned())
            .collect::<Vec<_>>();
        ensure(
            Value::Array(bytes) == outcome["bytes"],
            "paired rechunking changed byte order",
        )?;
    }
    Ok(())
}

pub fn validate_pair(case: Case, adapter: &str, token: &str, pair: &Value) -> Result<(), String> {
    for mode in ["send", "begin"] {
        validate(case, adapter, token, &pair[mode])?;
    }
    let mut sent = pair["send"]["outcome"].clone();
    let mut begun = pair["begin"]["outcome"].clone();
    // Deadline races can retain an SDK timeout diagnostic on one path. Its portable cause must match.
    if matches!(case.kind, Kind::Delay { .. }) {
        sent["error"]["provider"] = Value::Null;
        begun["error"]["provider"] = Value::Null;
    }
    ensure(
        sent == begun
            && pair["send"]["head"] == pair["begin"]["head"]
            && pair["send"]["request"] == pair["begin"]["request"]
            && pair["send"]["streamed"] == pair["begin"]["streamed"]
            && pair["send"]["stage"] == pair["begin"]["stage"],
        "paired Send/Begin outcomes differ",
    )
}

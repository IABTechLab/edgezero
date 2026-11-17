use super::*;

mod paired;
pub(super) use paired::policy_payload;

pub(super) struct Request<'a> {
    pub method: &'a str,
    pub target: &'a str,
    pub fields: &'a [(String, String)],
    pub port: u16,
    pub second: u16,
    pub head_received: Instant,
}

pub(super) fn serve(
    socket: &mut TcpStream,
    reader: &mut BufReader<DeadlineSocket>,
    request: Request<'_>,
    shared: &Shared,
) -> Result<()> {
    let Request {
        method,
        target,
        fields,
        port,
        second,
        head_received,
    } = request;
    let (path, query) = target
        .split_once('?')
        .map_or((target, None), |(p, q)| (p, Some(q)));
    let (case, token) = path
        .strip_prefix("/transport/")
        .and_then(|p| p.split_once('/'))
        .ok_or("invalid transport route")?;
    let paired_case = paired::vectors::find(case);
    require(
        !token.is_empty()
            && token.len() <= 64
            && token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "invalid transport token",
    )?;
    require(
        paired_case.is_some()
            || matches!(
                case,
                "plain"
                    | "second"
                    | "redirect"
                    | "denied"
                    | "raw%2Ftarget"
                    | "head-900"
                    | "parity-default"
                    | "parity-marker-off"
                    | "parity-marker-on"
                    | "parity-post"
                    | "parity-head"
                    | "parity-204"
                    | "parity-304"
                    | "parity-redirect"
                    | "parity-final-cap"
                    | "parity-decoded-cap"
                    | "parity-encoded-cap"
                    | "parity-gzip"
                    | "parity-original"
                    | "parity-opaque"
                    | "parity-empty-post"
                    | "parity-empty-delete"
                    | "parity-empty-options"
            ),
        "unknown transport route",
    )?;
    require(
        if case == "raw%2Ftarget" {
            query == Some("a=%2F&b=two%20words&a=1")
        } else {
            query.is_none()
        },
        "transport query changed",
    )?;
    let values = |field: &str| {
        fields
            .iter()
            .filter(|(name, _)| name == field)
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>()
    };
    let steps = values("x-transport-step");
    require(
        values("x-request-token") == [token]
            && steps.len() == 1
            && steps[0].len() <= 32
            && steps[0]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "transport correlation changed",
    )?;
    let head = super::super::overflow::Head {
        first: format!("{method} {target} HTTP/1.1"),
        fields: fields.to_vec(),
    };
    let (payload, encoded) = capture_wire_body(reader, &head).map_err(|error| error.name())?;
    no_trailing_bytes(reader)?;
    let expected_method = if let Some(spec) = paired_case {
        spec.request_method()
    } else if matches!(case, "parity-post" | "parity-empty-post") {
        "POST"
    } else if case == "parity-empty-delete" {
        "DELETE"
    } else if case == "parity-empty-options" {
        "OPTIONS"
    } else if case == "parity-head" {
        "HEAD"
    } else {
        "GET"
    };
    require(
        method == expected_method
            && payload
                == if let Some(spec) = paired_case {
                    spec.request_body().as_bytes()
                } else if case == "parity-post" {
                    b"abc".as_slice()
                } else {
                    b"".as_slice()
                },
        "transport method or payload changed",
    )?;
    if case == "head-900" {
        thread::sleep(
            (head_received + Duration::from_millis(900)).saturating_duration_since(Instant::now()),
        );
    }
    let receipt = json!({"port":port,"method":method,"target":target,"token":token,"step":steps[0],
        "head_delay_us":if case == "head-900" {Some(head_received.elapsed().as_micros())} else {None},
        "payload":String::from_utf8(payload)?,"encoded_body_hex":encoded.iter().map(|b|format!("{b:02x}")).collect::<String>(),
        "content_length":values("content-length"),"transfer_encoding":values("transfer-encoding"),
        "proof":values("x-transport-proof"),"signature":values("x-signature-test"),
        "request_fields":{"host":values("host"),"cache_control":values("cache-control"),"private":values("x-private"),"proxy_connection":values("proxy-connection"),"accept_encoding":values("accept-encoding")}});
    if let Some(spec) = paired_case {
        return paired::serve(socket, spec, token, receipt, head_received, shared);
    }
    shared.lock().wire.push_back(receipt);
    shared.wake.notify_all();
    let status = match case {
        "redirect" | "denied" | "parity-redirect" => 302,
        "parity-204" => 204,
        "parity-304" => 304,
        _ => 200,
    };
    let body = match case {
        "parity-post" => b"abc".as_slice(),
        "parity-gzip" | "parity-original" => GZIP_TEN,
        _ => b"ok".as_slice(),
    };
    write!(
        socket,
        "HTTP/1.1 {status} Fixture\r\nDate: Tue, 27 Oct 2026 00:00:00 GMT\r\nConnection: close\r\nContent-Type: application/octet-stream\r\nX-Transport-Fixture: native\r\nX-Request-Token: {token}\r\n"
    )?;
    if status != 204 {
        write!(socket, "Content-Length: {}\r\n", body.len())?;
    }
    if matches!(case, "parity-gzip" | "parity-original") {
        socket.write_all(b"Content-Encoding: gzip\r\n")?;
    }
    if case == "parity-opaque" {
        socket.write_all(b"Content-Encoding: x-transport-opaque\r\n")?;
    }
    if matches!(case, "redirect" | "denied") {
        write!(
            socket,
            "Location: http://{}:{second}/transport/second/{token}\r\n",
            if case == "denied" {
                "localhost"
            } else {
                "127.0.0.1"
            }
        )?;
    } else if case == "parity-redirect" {
        write!(socket, "Location: /transport/plain/{token}\r\n")?;
    }
    socket.write_all(b"\r\n")?;
    if method != "HEAD" && !matches!(status, 204 | 304) {
        socket.write_all(body)?;
    }
    socket.flush()?;
    Ok(())
}

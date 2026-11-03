use super::super::*;

#[path = "../../../../../edgezero-core/src/outbound/tests/buffered_parity/vectors.rs"]
#[allow(
    dead_code,
    reason = "the origin renderer shares the complete inventory without executing request options"
)]
pub(super) mod vectors;

pub fn policy_payload(coding: &str, size: &str, declared: bool) -> Result<&'static [u8]> {
    let coding = match coding {
        "gzip" => vectors::Coding::Gzip,
        "identity" => vectors::Coding::Identity,
        "passthrough" => vectors::Coding::Passthrough,
        _ => return Err("unknown policy coding".into()),
    };
    require(matches!(size, "10" | "11"), "unknown policy size")?;
    Ok(vectors::policy_payload(
        coding,
        if size == "10" { 10 } else { 11 },
        declared,
    ))
}

pub(super) fn serve(
    socket: &mut TcpStream,
    case: vectors::Case,
    token: &str,
    mut receipt: Value,
    received: Instant,
    shared: &Shared,
) -> Result<()> {
    let reply = case.reply(token);
    let deadline = received + IO_BUDGET;
    let mut head_at = None;
    let mut body_at = None;
    let mut written = 0;
    let mut cancelled = false;
    let result = (|| -> io::Result<()> {
        if reply.close_head {
            return Ok(());
        }
        if reply.head_delay_ms != 0 {
            thread::sleep(
                (received + Duration::from_millis(reply.head_delay_ms))
                    .saturating_duration_since(Instant::now()),
            );
        }
        head_at = Some(received.elapsed().as_micros());
        write!(socket, "HTTP/1.1 {} Fixture\r\n", reply.status)?;
        for (name, value) in &reply.fields {
            write!(socket, "{name}: {value}\r\n")?;
        }
        if let Some(length) = reply.declared {
            write!(socket, "Content-Length: {length}\r\n")?;
        } else {
            socket.write_all(b"Transfer-Encoding: chunked\r\n")?;
        }
        socket.write_all(b"\r\n")?;
        socket.flush()?;
        if reply.body_delay_ms != 0 {
            thread::sleep(
                (received + Duration::from_millis(reply.body_delay_ms))
                    .saturating_duration_since(Instant::now()),
            );
        }
        body_at = Some(received.elapsed().as_micros());
        if reply.declared.is_none() {
            write!(socket, "{:X}\r\n", reply.body.len())?;
        }
        socket.write_all(&reply.body)?;
        written = reply.body.len();
        if reply.declared.is_none() {
            socket.write_all(b"\r\n0\r\n\r\n")?;
        }
        socket.flush()
    })();
    if let Err(error) = result {
        if case.cancels_response()
            && matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
            )
        {
            cancelled = true;
        } else {
            return Err(error.into());
        }
    }
    require(
        Instant::now() < deadline,
        "paired origin exceeded its fixed I/O bound",
    )?;
    receipt["response"] = json!({"head_at_us":head_at,"body_at_us":body_at,"body_written":written,
        "closed_before_head":reply.close_head,"incomplete":reply.close_body,"cancelled":cancelled,"status":reply.status});
    require(receipt["token"] == token, "paired origin token changed")?;
    shared.lock().wire.push_back(receipt);
    shared.wake.notify_all();
    Ok(())
}

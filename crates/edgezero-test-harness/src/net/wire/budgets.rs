use super::*;

pub(super) struct Request<'a> {
    pub method: &'a str,
    pub target: &'a str,
    pub fields: &'a [(String, String)],
    pub port: u16,
}

fn wait_until(shared: &Shared, deadline: Instant) -> bool {
    let mut state = shared.lock();
    loop {
        if state.stop {
            return false;
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return true;
        };
        state = shared
            .wake
            .wait_timeout(state, remaining.min(Duration::from_millis(1)))
            .unwrap_or_else(PoisonError::into_inner)
            .0;
    }
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
    } = request;
    let parts = target
        .strip_prefix("/budget/")
        .ok_or("invalid budget origin")?
        .split('/')
        .collect::<Vec<_>>();
    require(
        parts.len() == 3
            && !parts[0].is_empty()
            && parts[0].len() <= 64
            && parts[0]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && matches!(
                parts[1],
                "reuse" | "variants" | "head" | "body" | "slot" | "settlement"
            )
            && !parts[2].is_empty()
            && parts[2].len() <= 32
            && parts[2]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "invalid budget path",
    )?;
    let (token, case, label) = (parts[0], parts[1], parts[2]);
    let values = |name: &str| {
        fields
            .iter()
            .filter(|(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
            .collect::<Vec<_>>()
    };
    require(
        method == "GET" && values("x-request-token") == [token],
        "budget request correlation changed",
    )?;
    let head = super::super::overflow::Head {
        first: format!("{method} {target} HTTP/1.1"),
        fields: fields.to_vec(),
    };
    let (payload, _) = capture_wire_body(reader, &head).map_err(|error| error.name())?;
    require(payload.is_empty(), "budget GET sent payload")?;
    let start = Instant::now();
    let at =
        || u64::try_from(start.elapsed().as_micros()).map_err(|_| "budget origin clock overflow");
    let mut record = json!({"event":"budget-origin","token":token,"case":case,"label":label,
        "port":port,"method":method,"target":target,"host":values("host"),
        "head_attempt_us":null,"head_complete_us":null,"chunks":[],"terminal":"complete"});
    let mut write = |bytes: &[u8]| -> Result<bool> {
        match socket.write_all(bytes).and_then(|()| socket.flush()) {
            Ok(()) => Ok(true),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    };
    let mut emit = || -> Result<()> {
        if case == "slot" && label == "silent" {
            if !wait_until(shared, start + Duration::from_secs(1)) {
                record["terminal"] = json!("stopped");
            }
            return Ok(());
        }
        if case == "head" && !wait_until(shared, start + Duration::from_millis(500)) {
            record["terminal"] = json!("stopped");
            return Ok(());
        }
        record["head_attempt_us"] = json!(at()?);
        let bytes: &[u8] = if case == "body" {
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\n"
        } else {
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n"
        };
        if !write(bytes)? {
            record["terminal"] = json!("peer-closed");
            return Ok(());
        }
        record["head_complete_us"] = json!(at()?);
        if case == "body" {
            let head_at = Instant::now();
            for index in 0..3 {
                if !wait_until(shared, head_at + Duration::from_millis(300 * (index + 1))) {
                    record["terminal"] = json!("stopped");
                    break;
                }
                let started = at()?;
                let written = write(&[b'A' + index as u8])?;
                record["chunks"].as_array_mut().ok_or("missing budget chunks")?.push(
                    json!({"index":index,"started_us":started,"completed_us":at()?,"written":written}));
                if !written {
                    record["terminal"] = json!("peer-closed");
                    break;
                }
            }
        } else if !write(b"ok")? {
            record["terminal"] = json!("peer-closed");
        }
        Ok(())
    };
    let outcome = emit();
    record["error"] = outcome
        .as_ref()
        .err()
        .map(ToString::to_string)
        .map_or(Value::Null, Value::String);
    shared.lock().wire.push_back(record);
    shared.wake.notify_all();
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fastly_budget_origin_wait_obeys_stop_and_absolute_expiry() {
        let shared = Shared::default();
        assert!(wait_until(&shared, Instant::now()));
        shared.lock().stop = true;
        assert!(!wait_until(
            &shared,
            Instant::now() + Duration::from_secs(5)
        ));
    }
}

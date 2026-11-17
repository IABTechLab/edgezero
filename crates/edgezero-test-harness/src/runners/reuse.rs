use super::lifetimes::protocol::{LogCursor, read_bytes};
use super::*;

pub(super) const WINDOW: Duration = Duration::from_millis(500);
pub(super) const CALLBACKS: usize = 5;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Guest {
    instance: String,
    span: u128,
}

#[derive(Debug)]
pub(super) struct Witness {
    pub(super) guest: Guest,
    pub(super) rejection: Option<Value>,
    pub(super) handle: Option<u32>,
}

pub(super) fn wait(
    log: &Path,
    cursor: &mut LogCursor,
    guest: Option<&Guest>,
    token: &str,
    ordinal: usize,
    deadline: Instant,
    mut now: impl FnMut() -> Instant,
) -> Result<Witness> {
    loop {
        remaining(deadline, now())?;
        let bytes = read_bytes(log, cursor)?;
        let result = inspect(&bytes, guest, token, ordinal)?;
        let remaining = remaining(deadline, now())?;
        if let Some(witness) = result {
            cursor.offset += bytes.len() as u64;
            return Ok(witness);
        }
        thread::sleep(remaining.min(Duration::from_millis(5)));
    }
}

pub(super) fn before_dispatch(deadline: Option<Instant>) -> Result<()> {
    if let Some(deadline) = deadline {
        remaining(deadline, Instant::now())?;
    }
    Ok(())
}

fn remaining(deadline: Instant, now: Instant) -> Result<Duration> {
    deadline
        .checked_duration_since(now)
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| "native reuse observation expired".into())
}

fn native_trace(line: &str) -> Result<Option<(u128, &str, &str)>> {
    let Some((timestamp, rest)) = line.split_once(" TRACE request{id=") else {
        return Ok(None);
    };
    let Some((span, rest)) =
        rest.split_once("}:wiggle abi{module=\"fastly_http_downstream\" function=\"")
    else {
        return Ok(None);
    };
    let (function, fields) = rest
        .split_once("\"}: ")
        .ok_or("malformed native reuse trace")?;
    if !matches!(
        function,
        "next_request" | "next_request_wait" | "next_request_abandon"
    ) {
        return Ok(None);
    }
    require(
        (20..=35).contains(&timestamp.len())
            && timestamp.ends_with('Z')
            && timestamp
                .bytes()
                .all(|byte| byte.is_ascii_digit() || b"-T:.Z".contains(&byte)),
        "invalid native reuse trace prefix",
    )?;
    Ok(Some((span.parse()?, function, fields)))
}

fn handle(fields: &str, prefix: &str, suffix: &str) -> Result<u32> {
    valid_handle(
        fields
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(suffix))
            .ok_or("invalid native reuse promise")?,
    )
}

fn valid_handle(token: &str) -> Result<u32> {
    require(
        !token.is_empty() && token.bytes().all(|byte| byte.is_ascii_digit()),
        "invalid native handle digits",
    )?;
    let value: u32 = token.parse()?;
    require(value != 4294967294, "invalid native handle sentinel")?;
    Ok(value)
}

fn returned(fields: &str) -> Result<()> {
    let handles = fields
        .strip_prefix("result=Ok((RequestHandle(")
        .and_then(|rest| rest.strip_suffix(")))"))
        .ok_or("native reuse wait failed or ended")?;
    let (request, body) = handles
        .split_once("), BodyHandle(")
        .ok_or("malformed native reuse return")?;
    valid_handle(request)?;
    valid_handle(body)?;
    Ok(())
}

fn inspect(
    bytes: &[u8],
    guest: Option<&Guest>,
    token: &str,
    ordinal: usize,
) -> Result<Option<Witness>> {
    require(
        (1..=CALLBACKS).contains(&ordinal),
        "invalid initialization callback ordinal",
    )?;
    require(
        guest.is_some() == (ordinal > 1),
        "missing original native guest anchor",
    )?;
    let mut anchor = guest.cloned();
    let mut started = false;
    let mut previous_returned = ordinal == 1;
    let mut rejection = None;
    let mut registration = None;
    let mut waiting = false;
    let mut ended = false;
    let mut partial = false;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if !line.ends_with(b"\n") {
            partial = true;
            break;
        }
        let line = std::str::from_utf8(&line[..line.len() - 1])?;
        if line.starts_with('{') {
            require(line.len() <= 4096, "initialization witness exceeds bound")?;
            let event: Value = serde_json::from_str(line)?;
            if event["event"] == "request_start" {
                require(
                    !started
                        && previous_returned
                        && event["token"] == token
                        && event["ordinal"] == ordinal
                        && event["native_id"] == true,
                    "foreign, duplicated or out-of-order initialization callback",
                )?;
                let instance = event["instance"]
                    .as_str()
                    .filter(|id| !id.is_empty())
                    .ok_or("missing initialization guest identity")?;
                let correlation = event["correlation"]
                    .as_str()
                    .filter(|id| id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
                    .ok_or("invalid native initialization correlation")?;
                let span = u128::from_str_radix(correlation, 16)?;
                if let Some(guest) = &anchor {
                    require(
                        guest.instance == instance,
                        "initialization callback changed guest",
                    )?;
                } else {
                    anchor = Some(Guest {
                        instance: instance.to_owned(),
                        span,
                    });
                }
                started = true;
            } else if event["event"] == "assembly-rejected" {
                require(
                    started
                        && ordinal == 2
                        && rejection.is_none()
                        && event["token"] == token
                        && event["ordinal"] == ordinal
                        && anchor
                            .as_ref()
                            .is_some_and(|guest| event["instance"] == guest.instance),
                    "foreign or duplicated initialization rejection",
                )?;
                rejection = Some(event);
            }
        } else if let Some((span, function, fields)) = native_trace(line)? {
            require(
                anchor.as_ref().is_some_and(|guest| guest.span == span),
                "native reuse witness changed original guest span",
            )?;
            if !started {
                if function == "next_request_wait" && fields.starts_with("result=") {
                    require(!previous_returned, "duplicate native reuse return")?;
                    returned(fields)?;
                    previous_returned = true;
                } else {
                    require(
                        !fields.starts_with("result=") && !fields.starts_with("handle="),
                        "stale native reuse witness",
                    )?;
                }
            } else {
                require(!ended, "native reuse record followed terminal wait")?;
                match function {
                    "next_request" if fields.starts_with("result=") => {
                        require(
                            registration.is_none(),
                            "duplicate native reuse registration",
                        )?;
                        registration =
                            Some(handle(fields, "result=Ok(RequestPromiseHandle(", "))")?);
                    }
                    "next_request_wait" if fields.starts_with("handle=") => {
                        let received = handle(fields, "handle=RequestPromiseHandle(", ")")?;
                        require(
                            !waiting && registration == Some(received),
                            "native reuse wait does not match registration",
                        )?;
                        waiting = true;
                    }
                    "next_request_wait" if fields.starts_with("result=") => {
                        require(
                            ordinal == CALLBACKS && waiting && fields == "result=Err(ValueAbsent)",
                            "native reuse wait ended before the next prescribed callback",
                        )?;
                        ended = true;
                    }
                    "next_request_abandon" => return Err("native reuse promise abandoned".into()),
                    "next_request" if fields.starts_with("options_mask=") => {}
                    _ => return Err("malformed native reuse fields".into()),
                }
            }
        }
    }
    if partial
        || !started
        || (ordinal == 2 && rejection.is_none())
        || (ordinal < CALLBACKS && !waiting)
    {
        return Ok(None);
    }
    Ok(Some(Witness {
        guest: anchor.ok_or("missing native guest anchor")?,
        rejection,
        handle: if ordinal < CALLBACKS {
            registration
        } else {
            None
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start(token: &str, instance: &str, ordinal: usize, correlation: &str) -> String {
        format!(
            "{}\n",
            json!({"event":"request_start", "token":token,
            "instance":instance, "ordinal":ordinal, "native_id":true,
            "correlation":correlation})
        )
    }

    fn trace(span: u128, function: &str, fields: &str) -> String {
        format!(
            "2026-11-08T20:36:38.911418Z TRACE request{{id={span}}}:wiggle abi{{module=\"fastly_http_downstream\" function=\"{function}\"}}: {fields}\n"
        )
    }

    fn registered(span: u128, handle: u32) -> String {
        trace(
            span,
            "next_request",
            &format!("result=Ok(RequestPromiseHandle({handle}))"),
        )
    }

    fn waiting(span: u128, handle: u32) -> String {
        trace(
            span,
            "next_request_wait",
            &format!("handle=RequestPromiseHandle({handle})"),
        )
    }

    fn previous_return(span: u128) -> String {
        trace(
            span,
            "next_request_wait",
            "result=Ok((RequestHandle(1), BodyHandle(3)))",
        )
    }

    fn rejection(token: &str, ordinal: usize) -> String {
        format!(
            "{}\n",
            json!({"event":"assembly-rejected", "token":token,
            "instance":"guest", "ordinal":ordinal,"attempts":1,"retained":false})
        )
    }

    #[test]
    fn client_completion_does_not_grant_reuse_without_matching_native_registration() {
        let header = start("one", "guest", 1, "0000000000000000000000000000000a");
        assert!(
            inspect(header.as_bytes(), None, "one", 1)
                .unwrap()
                .is_none()
        );
        let pending = header + &registered(10, 7);
        assert!(
            inspect(pending.as_bytes(), None, "one", 1)
                .unwrap()
                .is_none()
        );
        let ready = pending + &waiting(10, 7);
        let witness = inspect(ready.as_bytes(), None, "one", 1)
            .unwrap()
            .expect("native registration and matching SDK wait must grant this turn");
        assert_eq!(witness.guest.instance, "guest");
        assert_eq!(witness.guest.span, 10);
        assert_eq!(witness.handle, Some(7));
    }

    #[test]
    fn all_five_callbacks_keep_the_original_span_and_consume_only_four_permissions() {
        let mut guest = None;
        let mut permissions = 0;
        for ordinal in 1..=CALLBACKS {
            let token = format!("callback-{ordinal}");
            let mut bytes = if ordinal > 1 {
                previous_return(10)
            } else {
                String::new()
            };
            bytes += &start(&token, "guest", ordinal, &format!("{:032x}", ordinal + 9));
            if ordinal == 2 {
                bytes += &rejection(&token, ordinal);
            }
            // The final SDK registration is legal, but must not earn a fifth permission.
            bytes += &registered(10, ordinal as u32 + 100);
            bytes += &waiting(10, ordinal as u32 + 100);
            let witness = inspect(bytes.as_bytes(), guest.as_ref(), &token, ordinal)
                .unwrap()
                .expect("exact original callback");
            assert_eq!(witness.guest.span, 10);
            assert_eq!(witness.rejection.is_some(), ordinal == 2);
            permissions += usize::from(witness.handle.is_some());
            guest = Some(witness.guest);
        }
        assert_eq!(permissions, 4);
    }

    #[test]
    fn stale_foreign_duplicate_and_terminal_traces_cannot_grant_reuse() {
        let header = start("one", "guest", 1, "0000000000000000000000000000000a");
        let good = registered(10, 7) + &waiting(10, 7);
        for invalid in [
            registered(11, 7) + &waiting(11, 7),
            registered(10, 7) + &waiting(10, 8),
            registered(10, 7) + &registered(10, 7),
            good.clone() + &waiting(10, 7),
            good.clone() + &trace(10, "next_request_wait", "result=Err(InvalidHandle)"),
            good.clone() + &trace(10, "next_request_abandon", "handle=RequestPromiseHandle(7)"),
            trace(
                10,
                "next_request",
                "result=Ok(RequestPromiseHandle(4294967296))",
            ),
            trace(10, "next_request", "result=Err(LimitExceeded)"),
            registered(10, 4294967294) + &waiting(10, 4294967294),
            trace(10, "next_request", "result=Ok(RequestPromiseHandle(+7))"),
            good.clone() + &trace(10, "next_request_wait", "result =Err(ValueAbsent)"),
        ] {
            assert!(
                inspect((header.clone() + &invalid).as_bytes(), None, "one", 1).is_err(),
                "{invalid}"
            );
        }
        assert!(inspect((good + &header).as_bytes(), None, "one", 1).is_err());
    }

    #[test]
    fn final_callback_validates_but_does_not_require_or_credit_another_registration() {
        let anchor = Guest {
            instance: "guest".into(),
            span: 10,
        };
        let bytes =
            previous_return(10) + &start("last", "guest", 5, "0000000000000000000000000000000e");
        assert!(
            inspect(bytes.as_bytes(), Some(&anchor), "last", 5)
                .unwrap()
                .unwrap()
                .handle
                .is_none()
        );
        for invalid in [
            registered(10, 9) + &waiting(10, 8),
            registered(10, 9) + &registered(10, 9),
            trace(
                10,
                "next_request",
                "result=Ok(RequestPromiseHandle(invalid))",
            ),
        ] {
            assert!(
                inspect(
                    (bytes.clone() + &invalid).as_bytes(),
                    Some(&anchor),
                    "last",
                    5
                )
                .is_err()
            );
        }
        assert!(returned("result=Ok((RequestHandle(4294967294), BodyHandle(3)))").is_err());
        assert!(returned("result=Ok((RequestHandle(1), BodyHandle(4294967294)))").is_err());
    }

    #[test]
    fn only_the_final_matched_wait_may_expire_normally() {
        let anchor = Guest {
            instance: "guest".into(),
            span: 10,
        };
        let terminal = trace(10, "next_request_wait", "result=Err(ValueAbsent)");
        let body =
            previous_return(10) + &start("last", "guest", 5, "0000000000000000000000000000000e");
        let ready = body.clone() + &registered(10, 9) + &waiting(10, 9);
        assert!(
            inspect(
                (ready.clone() + &terminal).as_bytes(),
                Some(&anchor),
                "last",
                5
            )
            .unwrap()
            .unwrap()
            .handle
            .is_none()
        );
        for invalid in [
            body + &terminal,
            ready.clone() + &terminal + &terminal,
            ready + &trace(10, "next_request_wait", "result=Err(InvalidHandle)"),
        ] {
            assert!(inspect(invalid.as_bytes(), Some(&anchor), "last", 5).is_err());
        }
        let early = start("first", "guest", 1, "0000000000000000000000000000000a")
            + &registered(10, 9)
            + &waiting(10, 9)
            + &terminal;
        assert!(inspect(early.as_bytes(), None, "first", 1).is_err());
    }

    #[test]
    fn callback_and_rejection_evidence_must_be_exact() {
        let anchor = Guest {
            instance: "guest".into(),
            span: 10,
        };
        for header in [
            start("wrong", "guest", 2, "0000000000000000000000000000000b"),
            start("two", "other", 2, "0000000000000000000000000000000b"),
            start("two", "guest", 3, "0000000000000000000000000000000b"),
            start("two", "guest", 2, "not-a-native-request-id"),
            format!(
                "{}\n",
                json!({"event":"request_start", "token":"two",
                "instance":"guest", "ordinal":2,"native_id":false,
                "correlation":"0000000000000000000000000000000b"})
            ),
        ] {
            assert!(
                inspect(
                    (previous_return(10) + &header).as_bytes(),
                    Some(&anchor),
                    "two",
                    2
                )
                .is_err()
            );
        }
        let body =
            previous_return(10) + &start("two", "guest", 2, "0000000000000000000000000000000b");
        let ready = registered(10, 8) + &waiting(10, 8);
        assert!(
            inspect((body.clone() + &ready).as_bytes(), Some(&anchor), "two", 2)
                .unwrap()
                .is_none()
        );
        assert!(
            inspect(
                (body + &rejection("wrong", 2) + &ready).as_bytes(),
                Some(&anchor),
                "two",
                2
            )
            .is_err()
        );
    }

    #[test]
    fn partial_terminal_records_never_turn_into_a_ready_receipt() {
        let header = start("one", "guest", 1, "0000000000000000000000000000000a");
        let valid = header + &registered(10, 7) + &waiting(10, 7);
        let terminal = trace(10, "next_request_wait", "result=Err(Ended)");
        for partial in [registered(10, 7), terminal] {
            let bytes = valid.clone() + partial.trim_end_matches('\n');
            assert!(inspect(bytes.as_bytes(), None, "one", 1).unwrap().is_none());
            assert!(inspect((bytes + "\n").as_bytes(), None, "one", 1).is_err());
        }
    }

    #[test]
    fn expiration_before_or_after_reading_cannot_consume_a_receipt() {
        let output = tempfile::tempdir().unwrap();
        let log = output.path().join("reuse.log");
        let bytes = start("one", "guest", 1, "0000000000000000000000000000000a")
            + &registered(10, 7)
            + &waiting(10, 7);
        fs::write(&log, bytes).unwrap();
        let start = Instant::now();
        for readings in [[start + WINDOW, start + WINDOW], [start, start + WINDOW]] {
            let mut clock = readings.into_iter();
            let mut cursor = LogCursor::default();
            assert!(
                wait(&log, &mut cursor, None, "one", 1, start + WINDOW, || clock
                    .next()
                    .expect("bounded clock reads"))
                .is_err()
            );
            assert_eq!(cursor.offset, 0);
        }
        assert!(remaining(start, start).is_err());
        assert_eq!(remaining(start + WINDOW, start).unwrap(), WINDOW);
        assert!(before_dispatch(Some(start)).is_err());
    }

    #[test]
    fn cursor_retains_single_use_and_total_log_bounds() {
        let output = tempfile::tempdir().unwrap();
        let log = output.path().join("reuse.log");
        let bytes = start("one", "guest", 1, "0000000000000000000000000000000a")
            + &registered(10, 7)
            + &waiting(10, 7);
        fs::write(&log, &bytes).unwrap();
        let mut cursor = LogCursor::default();
        let witness = wait(
            &log,
            &mut cursor,
            None,
            "one",
            1,
            Instant::now() + WINDOW,
            Instant::now,
        )
        .unwrap();
        assert_eq!(cursor.offset, bytes.len() as u64);
        assert!(
            wait(
                &log,
                &mut cursor,
                Some(&witness.guest),
                "two",
                2,
                Instant::now(),
                Instant::now
            )
            .is_err()
        );
        fs::write(&log, b"shorter\n").unwrap();
        assert!(read_bytes(&log, &cursor).is_err());
        fs::write(&log, vec![b'x'; 1024 * 1024 + 1]).unwrap();
        assert!(read_bytes(&log, &LogCursor::default()).is_err());
    }
}

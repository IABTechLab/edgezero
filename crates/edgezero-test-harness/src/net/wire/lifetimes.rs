use super::*;
use std::{collections::HashSet, net::Shutdown};

const EVENT_LIMIT: usize = 32;

#[derive(Clone, Copy)]
enum Case {
    Slow,
    Fast,
    Begin,
    Batch,
    PendingHead,
    PendingBody,
    Cpu,
}

impl Case {
    fn parse(case: &str) -> Result<Self> {
        match case {
            "slow-origin" => Ok(Self::Slow),
            "fast-origin" => Ok(Self::Fast),
            "begin-progress" => Ok(Self::Begin),
            "batch-progress" => Ok(Self::Batch),
            "pending-head-cutoff" => Ok(Self::PendingHead),
            "pending-body-cutoff" => Ok(Self::PendingBody),
            "cpu-stretch" => Ok(Self::Cpu),
            _ => Err("unknown lifetime case".into()),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Slow => "slow-origin",
            Self::Fast => "fast-origin",
            Self::Begin => "begin-progress",
            Self::Batch => "batch-progress",
            Self::PendingHead => "pending-head-cutoff",
            Self::PendingBody => "pending-body-cutoff",
            Self::Cpu => "cpu-stretch",
        }
    }

    fn permits(self, role: &str) -> bool {
        match self {
            Self::Slow | Self::Cpu => matches!(role, "origin" | "a" | "b" | "c"),
            Self::Fast => matches!(role, "origin" | "a" | "b" | "c" | "mediator"),
            Self::Begin => role == "origin",
            Self::Batch => matches!(role, "a" | "b" | "c" | "quiet"),
            Self::PendingHead | Self::PendingBody => matches!(role, "a" | "b" | "c"),
        }
    }

    fn held(self) -> bool {
        matches!(self, Self::PendingHead | Self::PendingBody)
    }
}

pub(super) struct Lifetime {
    case: Case,
    anchor: Option<Instant>,
    scenario_anchor: Option<Instant>,
    roles: HashSet<String>,
    active: usize,
    events: Vec<Value>,
    released: bool,
}

impl Lifetime {
    pub(super) fn undrained(&self) -> usize {
        self.events.len()
    }
}

pub(super) fn register(shared: &Shared, token: &str, case: &str) -> Result<()> {
    require(
        !token.is_empty()
            && token.len() <= 64
            && token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "invalid lifetime token",
    )?;
    let case = Case::parse(case)?;
    let mut state = shared.lock();
    require(!state.stop, "wire origin stopped")?;
    require(
        !state.lifetimes.contains_key(token),
        "duplicate lifetime token",
    )?;
    require(
        state.lifetimes.len() < CONNECTION_LIMIT,
        "lifetime registration limit",
    )?;
    state.lifetimes.insert(
        token.to_owned(),
        Lifetime {
            case,
            anchor: None,
            scenario_anchor: None,
            roles: HashSet::new(),
            active: 0,
            events: Vec::new(),
            released: false,
        },
    );
    Ok(())
}

pub(super) fn release(shared: &Shared, token: &str, response_returned: Instant) -> Result<()> {
    let mut state = shared.lock();
    let lifetime = state
        .lifetimes
        .get_mut(token)
        .ok_or("unknown lifetime token")?;
    require(lifetime.case.held(), "lifetime does not use a release gate")?;
    let anchor = lifetime.anchor.ok_or("lifetime not started")?;
    require(!lifetime.released, "lifetime already released")?;
    require(lifetime.events.len() < EVENT_LIMIT, "lifetime event limit")?;
    let released_at = Instant::now();
    require(
        (anchor..=released_at).contains(&response_returned),
        "invalid response return boundary",
    )?;
    lifetime
        .events
        .push(json!({"op":"release","role":"control","token":token,
        "case":lifetime.case.name(),"at_us":offset(released_at,anchor),
        "response_returned_us":offset(response_returned,anchor)}));
    lifetime.released = true;
    shared.wake.notify_all();
    Ok(())
}

pub(super) fn mark_start(shared: &Shared, token: &str) -> Result<()> {
    let mut state = shared.lock();
    require(!state.stop, "wire origin stopped")?;
    let lifetime = state
        .lifetimes
        .get_mut(token)
        .ok_or("unknown lifetime token")?;
    require(lifetime.anchor.is_none(), "lifetime already started")?;
    lifetime.anchor = Some(Instant::now());
    Ok(())
}

pub(super) fn take_events(shared: &Shared, token: &str) -> Result<Value> {
    let deadline = Instant::now() + IO_BUDGET;
    let mut state = shared.lock();
    loop {
        let lifetime = state.lifetimes.get(token).ok_or("unknown lifetime token")?;
        require(lifetime.anchor.is_some(), "lifetime not started")?;
        if lifetime.active == 0 {
            let events = std::mem::take(&mut state.lifetimes.get_mut(token).unwrap().events);
            return Ok(Value::Array(events));
        }
        require(!state.stop, "wire origin stopped")?;
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or("lifetime event collection timed out")?;
        state = shared
            .wake
            .wait_timeout(state, remaining)
            .unwrap_or_else(PoisonError::into_inner)
            .0;
    }
}

struct Exchange<'a> {
    token: &'a str,
    role: &'a str,
    case: Case,
    anchor: Instant,
    scenario_anchor: Option<Instant>,
    received: Instant,
}

fn offset(instant: Instant, anchor: Instant) -> i64 {
    if instant >= anchor {
        instant.duration_since(anchor).as_micros() as i64
    } else {
        -(anchor.duration_since(instant).as_micros() as i64)
    }
}

impl Exchange<'_> {
    fn event(&self, op: &str, now: Instant, scheduled: Option<Instant>) -> Value {
        let mut event = json!({"op":op,"token":self.token,"case":self.case.name(),
            "role":self.role,"at_us":offset(now,self.anchor)});
        if let Some(scheduled) = scheduled {
            event["scheduled_us"] = json!(offset(scheduled, self.anchor));
            if let Some(anchor) = self.scenario_anchor {
                event["scenario_scheduled_us"] = json!(offset(scheduled, anchor));
                event["schedule_basis"] = json!("origin-receipt-minus-guest-at-us");
            } else {
                event["schedule_basis"] = json!("request-receipt");
            }
        }
        event
    }
}

fn record(shared: &Shared, exchange: &Exchange<'_>, event: Value) -> Result<()> {
    let mut state = shared.lock();
    let lifetime = state.lifetimes.get_mut(exchange.token).unwrap();
    require(lifetime.events.len() < EVENT_LIMIT, "lifetime event limit")?;
    lifetime.events.push(event);
    shared.wake.notify_all();
    Ok(())
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
            .wait_timeout(state, remaining)
            .unwrap_or_else(PoisonError::into_inner)
            .0;
    }
}

pub(super) fn serve(
    socket: &mut TcpStream,
    target: &str,
    fields: &[(String, String)],
    shared: &Shared,
    head_started: Instant,
    head_received: Instant,
) -> Result<()> {
    let parts: Vec<_> = target.split('/').collect();
    let ["", "lifetimes", token, case, role] = parts.as_slice() else {
        return Err("invalid lifetime route".into());
    };
    let mut exchange = {
        let mut state = shared.lock();
        require(!state.stop, "wire origin stopped")?;
        let lifetime = state
            .lifetimes
            .get_mut(*token)
            .ok_or("unknown lifetime token")?;
        require(*case == lifetime.case.name(), "lifetime case mismatch")?;
        require(lifetime.case.permits(role), "unknown lifetime role")?;
        let anchor = lifetime.anchor.ok_or("lifetime not started")?;
        let values: Vec<_> = fields
            .iter()
            .filter(|(name, _)| name == "x-fixture-at-us")
            .map(|(_, value)| value.as_str())
            .collect();
        require(
            values.len() == 1
                && !values[0].is_empty()
                && values[0].bytes().all(|byte| byte.is_ascii_digit()),
            "invalid lifetime guest clock",
        )?;
        let guest_at = values[0]
            .parse::<u64>()
            .map_err(|_| "invalid lifetime guest clock")?;
        require(guest_at <= 15_000_000, "invalid lifetime guest clock")?;
        require(!lifetime.roles.contains(*role), "duplicate lifetime role")?;
        if matches!(lifetime.case, Case::Slow | Case::Fast | Case::Cpu) && *role == "origin" {
            lifetime.scenario_anchor = Some(
                head_received
                    .checked_sub(Duration::from_micros(guest_at))
                    .ok_or("invalid lifetime guest clock")?,
            );
        }
        let exchange = Exchange {
            token,
            role,
            case: lifetime.case,
            anchor,
            scenario_anchor: lifetime.scenario_anchor,
            received: head_received,
        };
        let mut event = exchange.event("request", exchange.received, None);
        event["guest_at_us"] = json!(guest_at);
        if matches!(lifetime.case, Case::Slow | Case::Fast | Case::Cpu) && *role == "origin" {
            event["alignment"] = json!({
                "basis":"origin-receipt-minus-guest-at-us",
                "scenario_anchor_host_offset_us":offset(lifetime.scenario_anchor.unwrap(),anchor),
                "boundary_start_us":offset(head_started,anchor),
                "boundary_end_us":offset(head_received,anchor),
                "boundary_span_us":head_received.duration_since(head_started).as_micros() as u64,
                "transit_uncertainty_us":null,"clock_sync_proven":false
            });
        }
        lifetime.events.push(event);
        lifetime.roles.insert((*role).to_owned());
        lifetime.active += 1;
        shared.wake.notify_all();
        exchange
    };
    let outcome = align(shared, &mut exchange).and_then(|ready| {
        if ready {
            respond(socket, shared, &exchange)
        } else {
            Ok(false)
        }
    });
    let expected_close = matches!(exchange.case, Case::Slow | Case::Cpu)
        && exchange.role == "c"
        && exchange
            .scenario_anchor
            .is_some_and(|anchor| Instant::now() >= anchor + Duration::from_millis(534))
        && outcome
            .as_ref()
            .err()
            .and_then(|error| error.downcast_ref::<io::Error>())
            .is_some_and(|error| {
                matches!(
                    error.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                )
            });
    let close = socket.shutdown(Shutdown::Both);
    let scheduled_close = if matches!(exchange.case, Case::Fast) && exchange.role == "mediator" {
        exchange
            .scenario_anchor
            .map(|anchor| anchor + Duration::from_millis(1500))
    } else {
        None
    };
    let mut event = exchange.event("closed", Instant::now(), scheduled_close);
    event["reason"] = json!(match &outcome {
        Err(_) if expected_close => "peer_closed_after_cutoff",
        Ok(true) if exchange.role == "mediator" => "silent",
        Ok(true) => "complete",
        Ok(false) if exchange.case.held() && !shared.lock().stop => "fixture_released",
        Ok(false) => "stopped",
        Err(_) => "error",
    });
    if let Err(error) = &outcome {
        event["error"] = json!(error.to_string());
    }
    let recorded = record(shared, &exchange, event);
    {
        let mut state = shared.lock();
        state.lifetimes.get_mut(*token).unwrap().active -= 1;
        shared.wake.notify_all();
    }
    if !expected_close {
        outcome?;
    }
    recorded?;
    match close {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn align(shared: &Shared, exchange: &mut Exchange<'_>) -> Result<bool> {
    if !matches!(exchange.case, Case::Slow | Case::Fast | Case::Cpu) {
        return Ok(true);
    }
    let deadline = Instant::now() + IO_BUDGET;
    let mut state = shared.lock();
    loop {
        if state.stop {
            return Ok(false);
        }
        if let Some(anchor) = state.lifetimes[exchange.token].scenario_anchor {
            exchange.scenario_anchor = Some(anchor);
            return Ok(true);
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or("lifetime origin alignment timed out")?;
        state = shared
            .wake
            .wait_timeout(state, remaining)
            .unwrap_or_else(PoisonError::into_inner)
            .0;
    }
}

fn respond(socket: &mut TcpStream, shared: &Shared, exchange: &Exchange<'_>) -> Result<bool> {
    if exchange.case.held() {
        if shared.lock().lifetimes[exchange.token].released {
            return Ok(false);
        }
        if matches!(exchange.case, Case::PendingBody) {
            let started = Instant::now();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 1\r\n\r\n")?;
            socket.flush()?;
            let mut head = exchange.event("head", Instant::now(), None);
            head["write_started_us"] = json!(offset(started, exchange.anchor));
            head["content_length"] = json!(1);
            record(shared, exchange, head)?;
        }
        record(
            shared,
            exchange,
            exchange.event("waiting", Instant::now(), None),
        )?;
        shared.wait(GATE_BUDGET, |state| {
            state.stop || state.lifetimes[exchange.token].released
        })?;
        return Ok(false);
    }
    let ms = |value| Duration::from_millis(value);
    let absolute = |value| exchange.scenario_anchor.unwrap() + ms(value);
    let received = |value| exchange.received + ms(value);
    let (head_at, chunks): (Instant, Vec<(Instant, &[u8])>) = match (exchange.case, exchange.role) {
        (Case::Slow, "origin") => (
            absolute(700),
            vec![
                (absolute(700), b"O"),
                (absolute(900), b"o"),
                (absolute(1300), b"!"),
            ],
        ),
        (Case::Fast, "origin") => (absolute(120), vec![(absolute(260), b"O")]),
        (Case::Cpu, "origin") => (absolute(100), vec![(absolute(100), b"O")]),
        (Case::Begin, "origin") => (received(150), vec![(received(150), b"O")]),
        (Case::Batch, "quiet") => (received(300), Vec::new()),
        (Case::Fast, "mediator") => {
            // Stay silent beyond both the auction cutoff and the 1000 ms per-call timer.
            return Ok(wait_until(shared, absolute(1500)));
        }
        (case, role @ ("a" | "b" | "c")) => {
            let index = usize::from(role.as_bytes()[0] - b'a');
            let at = match case {
                Case::Slow => absolute([180, 320, 900][index]),
                Case::Fast => absolute([180, 320, 400][index]),
                Case::Cpu => absolute([180, 530, 900][index]),
                Case::Batch => received([20, 40, 60][index]),
                Case::Begin | Case::PendingHead | Case::PendingBody => unreachable!(),
            };
            (at, vec![(at, [b"A".as_slice(), b"B", b"C"][index])])
        }
        _ => unreachable!(),
    };
    if !wait_until(shared, head_at) {
        return Ok(false);
    }
    let chunked = exchange.role == "origin";
    let head = if chunked {
        b"HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n".as_slice()
    } else if chunks.is_empty() {
        b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    } else {
        b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 1\r\n\r\n"
    };
    let write_started = Instant::now();
    socket.write_all(head)?;
    socket.flush()?;
    let mut event = exchange.event("head", Instant::now(), Some(head_at));
    event["write_started_us"] = json!(offset(write_started, exchange.anchor));
    record(shared, exchange, event)?;
    for (at, bytes) in chunks {
        if !wait_until(shared, at) {
            return Ok(false);
        }
        if chunked {
            write!(socket, "{:X}\r\n", bytes.len())?;
        }
        socket.write_all(bytes)?;
        if chunked {
            socket.write_all(b"\r\n")?;
        }
        socket.flush()?;
        let mut event = exchange.event("chunk", Instant::now(), Some(at));
        event["bytes"] = json!(bytes);
        record(shared, exchange, event)?;
    }
    if chunked {
        socket.write_all(b"0\r\n\r\n")?;
        socket.flush()?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_accepts_the_same_ascii_token_alphabet_as_the_guest() {
        let shared = Shared::default();
        register(&shared, "Run_1-a", "begin-progress").unwrap();
    }

    fn connect(
        origins: &WireOrigins,
        token: &str,
        case: &str,
        role: &str,
        clock: &str,
    ) -> TcpStream {
        let mut socket = TcpStream::connect(("127.0.0.1", origins.port(0))).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        write!(
            socket,
            "GET /lifetimes/{token}/{case}/{role} HTTP/1.1\r\nx-fixture-at-us: {clock}\r\n\r\n"
        )
        .unwrap();
        socket
    }

    fn read(mut socket: TcpStream) -> Vec<u8> {
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).unwrap();
        bytes
    }

    fn event<'a>(events: &'a Value, role: &str, op: &str) -> &'a Value {
        events
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["role"] == role && event["op"] == op)
            .unwrap()
    }

    #[test]
    fn cpu_stretch_origin_uses_fixed_role_schedules_and_one_byte_bodies() {
        let origins = WireOrigins::start().unwrap();
        origins.register_lifetime("cpu", "cpu-stretch").unwrap();
        origins.mark_lifetime_start("cpu").unwrap();
        let sockets: Vec<_> = [
            ("origin", "2000"),
            ("a", "34000"),
            ("b", "34000"),
            ("c", "34000"),
        ]
        .into_iter()
        .map(|(role, at)| connect(&origins, "cpu", "cpu-stretch", role, at))
        .collect();
        for (index, socket) in sockets.into_iter().enumerate() {
            let bytes = read(socket);
            if index == 0 {
                assert_eq!(bytes, b"HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nO\r\n0\r\n\r\n");
            } else {
                let mut expected =
                    b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 1\r\n\r\n".to_vec();
                expected.push(b'A' + index as u8 - 1);
                assert_eq!(bytes, expected);
            }
        }
        let events = origins.take_lifetime_events("cpu").unwrap();
        for (role, at) in [
            ("origin", 100000),
            ("a", 180000),
            ("b", 530000),
            ("c", 900000),
        ] {
            assert_eq!(event(&events, role, "head")["scenario_scheduled_us"], at);
            assert!(
                event(&events, role, "head")["write_started_us"]
                    .as_i64()
                    .unwrap()
                    >= event(&events, role, "head")["scheduled_us"]
                        .as_i64()
                        .unwrap()
            );
            let phases: Vec<_> = events
                .as_array()
                .unwrap()
                .iter()
                .filter(|entry| entry["role"] == role)
                .map(|entry| entry["op"].as_str().unwrap())
                .collect();
            assert_eq!(phases, ["request", "head", "chunk", "closed"]);
            assert_eq!(event(&events, role, "closed")["reason"], "complete");
        }
        origins.finish().unwrap();
    }

    #[test]
    fn pending_origins_withhold_body_and_eof_until_explicit_release() {
        for case in ["pending-head-cutoff", "pending-body-cutoff"] {
            let origins = WireOrigins::start().unwrap();
            origins.register_lifetime("held", case).unwrap();
            origins.mark_lifetime_start("held").unwrap();
            let socket = connect(&origins, "held", case, "a", "34000");
            let (send, receive) = std::sync::mpsc::channel();
            let reader = thread::spawn(move || send.send(read(socket)).unwrap());
            origins
                .shared
                .wait(IO_BUDGET, |state| {
                    state.lifetimes["held"]
                        .events
                        .iter()
                        .any(|event| event["op"] == "waiting")
                })
                .unwrap();
            assert!(matches!(
                receive.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ));
            let response_returned = Instant::now();
            origins.release_lifetime("held", response_returned).unwrap();
            let bytes = receive.recv_timeout(IO_BUDGET).unwrap();
            reader.join().unwrap();
            if case == "pending-body-cutoff" {
                assert_eq!(
                    bytes,
                    b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 1\r\n\r\n"
                );
            } else {
                assert!(bytes.is_empty());
            }
            let events = origins.take_lifetime_events("held").unwrap();
            assert_eq!(event(&events, "a", "closed")["reason"], "fixture_released");
            assert_eq!(event(&events, "control", "release")["case"], case);
            let anchor = origins.shared.lock().lifetimes["held"].anchor.unwrap();
            assert_eq!(
                event(&events, "control", "release")["response_returned_us"],
                offset(response_returned, anchor),
            );
            assert!(origins.release_lifetime("held", Instant::now()).is_err());
            origins.finish().unwrap();
        }
    }

    #[test]
    fn pending_origin_release_rejects_boundaries_outside_the_active_lifetime() {
        for case in ["pending-head-cutoff", "pending-body-cutoff"] {
            let origins = WireOrigins::start().unwrap();
            origins.register_lifetime("boundary", case).unwrap();
            origins.mark_lifetime_start("boundary").unwrap();
            let anchor = origins.shared.lock().lifetimes["boundary"].anchor.unwrap();
            assert!(
                origins
                    .release_lifetime("boundary", anchor - Duration::from_millis(1))
                    .is_err()
            );
            assert!(
                origins
                    .release_lifetime("boundary", Instant::now() + Duration::from_secs(1))
                    .is_err()
            );
            origins
                .release_lifetime("boundary", Instant::now())
                .unwrap();
            let events = origins.take_lifetime_events("boundary").unwrap();
            assert_eq!(events.as_array().unwrap().len(), 1);
            origins.finish().unwrap();
        }
    }

    #[test]
    fn pending_origin_release_before_dispatch_and_shutdown_are_bounded() {
        for case in ["pending-head-cutoff", "pending-body-cutoff"] {
            let mut origins = WireOrigins::start().unwrap();
            origins.register_lifetime("early", case).unwrap();
            assert!(origins.release_lifetime("unknown", Instant::now()).is_err());
            assert!(origins.release_lifetime("early", Instant::now()).is_err());
            origins.mark_lifetime_start("early").unwrap();
            origins.release_lifetime("early", Instant::now()).unwrap();
            assert!(read(connect(&origins, "early", case, "a", "34000")).is_empty());
            let events = origins.take_lifetime_events("early").unwrap();
            assert_eq!(events[0]["op"], "release");
            assert_eq!(event(&events, "a", "closed")["reason"], "fixture_released");

            origins.register_lifetime("stop", case).unwrap();
            origins.mark_lifetime_start("stop").unwrap();
            let socket = connect(&origins, "stop", case, "b", "34000");
            origins
                .shared
                .wait(IO_BUDGET, |state| {
                    state.lifetimes["stop"]
                        .events
                        .iter()
                        .any(|event| event["op"] == "waiting")
                })
                .unwrap();
            origins.shutdown();
            read(socket);
            let events = origins.take_lifetime_events("stop").unwrap();
            assert_eq!(event(&events, "b", "closed")["reason"], "stopped");
            assert!(origins.shared.lock().workers.is_empty());
            origins.finish().unwrap();
        }
    }

    #[test]
    fn registration_and_start_reject_unknown_duplicate_and_unbounded_inputs() {
        let shared = Shared::default();
        for token in [
            "",
            "with/slash",
            "with?query",
            "with space",
            &"x".repeat(65),
        ] {
            assert!(register(&shared, token, "begin-progress").is_err());
        }
        assert!(register(&shared, "case", "unknown").is_err());
        assert!(mark_start(&shared, "unknown").is_err());
        assert!(take_events(&shared, "unknown").is_err());
        register(&shared, "case", "begin-progress").unwrap();
        assert!(register(&shared, "case", "begin-progress").is_err());
        assert!(take_events(&shared, "case").is_err());
        mark_start(&shared, "case").unwrap();
        assert!(mark_start(&shared, "case").is_err());
        assert_eq!(take_events(&shared, "case").unwrap(), json!([]));
        for index in 1..CONNECTION_LIMIT {
            register(&shared, &format!("case-{index}"), "batch-progress").unwrap();
        }
        assert!(register(&shared, "overflow", "batch-progress").is_err());
        shared.lock().stop = true;
        assert!(register(&shared, "stopped", "begin-progress").is_err());
        assert!(mark_start(&shared, "case-1").is_err());
    }

    #[test]
    fn begin_schedule_uses_receipt_and_only_requests_have_guest_clock() {
        let origins = WireOrigins::start().unwrap();
        origins
            .register_lifetime("begin", "begin-progress")
            .unwrap();
        origins.mark_lifetime_start("begin").unwrap();
        let reply = read(connect(
            &origins,
            "begin",
            "begin-progress",
            "origin",
            "2000",
        ));
        assert!(reply.ends_with(b"1\r\nO\r\n0\r\n\r\n"));
        let events = origins.take_lifetime_events("begin").unwrap();
        assert_eq!(events.as_array().unwrap().len(), 4);
        let request = event(&events, "origin", "request");
        assert_eq!(request["guest_at_us"], 2000);
        assert!(request.get("alignment").is_none());
        let head = event(&events, "origin", "head");
        let write_started = head["write_started_us"].as_u64().unwrap();
        assert!(write_started >= head["scheduled_us"].as_u64().unwrap());
        assert!(write_started <= head["at_us"].as_u64().unwrap());
        assert_eq!(
            head["scheduled_us"].as_u64().unwrap(),
            request["at_us"].as_u64().unwrap() + 150_000
        );
        assert!(head["at_us"].as_u64().unwrap() >= head["scheduled_us"].as_u64().unwrap());
        for entry in events.as_array().unwrap() {
            assert_eq!(entry["token"], "begin");
            assert_eq!(entry["case"], "begin-progress");
            assert_eq!(entry.get("guest_at_us").is_some(), entry["op"] == "request");
        }
        assert_eq!(origins.take_lifetime_events("begin").unwrap(), json!([]));
        assert!(origins.take_requests().is_empty());
        origins.finish().unwrap();
    }

    #[test]
    fn slow_schedule_keeps_host_anchor_and_explicitly_derives_scenario_anchor() {
        let origins = WireOrigins::start().unwrap();
        origins.register_lifetime("slow", "slow-origin").unwrap();
        origins.mark_lifetime_start("slow").unwrap();
        thread::sleep(Duration::from_millis(30));
        let origin = connect(&origins, "slow", "slow-origin", "origin", "2000");
        let bids =
            ["a", "b", "c"].map(|role| connect(&origins, "slow", "slow-origin", role, "34000"));
        let reply = read(origin);
        assert!(reply.ends_with(b"1\r\nO\r\n1\r\no\r\n1\r\n!\r\n0\r\n\r\n"));
        for (index, bid) in bids.into_iter().enumerate() {
            assert_eq!(*read(bid).last().unwrap(), b'A' + index as u8);
        }
        let events = origins.take_lifetime_events("slow").unwrap();
        let request = event(&events, "origin", "request");
        let alignment = &request["alignment"];
        assert_eq!(alignment["basis"], "origin-receipt-minus-guest-at-us");
        let offset = alignment["scenario_anchor_host_offset_us"]
            .as_i64()
            .unwrap();
        assert!(offset >= 28_000);
        assert_eq!(offset, request["at_us"].as_i64().unwrap() - 2000);
        assert_eq!(alignment["clock_sync_proven"], false);
        assert!(alignment["transit_uncertainty_us"].is_null());
        let span = alignment["boundary_span_us"].as_i64().unwrap();
        let boundary_delta = alignment["boundary_end_us"].as_i64().unwrap()
            - alignment["boundary_start_us"].as_i64().unwrap();
        // Offsets and the duration are independently truncated to whole microseconds.
        assert!((span..=span + 1).contains(&boundary_delta));
        for (role, expected) in [
            ("origin", 700_000),
            ("a", 180_000),
            ("b", 320_000),
            ("c", 900_000),
        ] {
            let head = event(&events, role, "head");
            assert_eq!(head["scenario_scheduled_us"], expected);
            assert_eq!(head["scheduled_us"].as_i64().unwrap(), offset + expected);
            assert!(head["at_us"].as_i64().unwrap() >= offset + expected);
        }
        let chunks: Vec<_> = events
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["role"] == "origin" && entry["op"] == "chunk")
            .map(|entry| entry["scenario_scheduled_us"].clone())
            .collect();
        assert_eq!(json!(chunks), json!([700_000, 900_000, 1_300_000]));
        origins.finish().unwrap();
    }

    #[test]
    fn fast_origin_and_silent_mediator_record_actual_completion() {
        let origins = WireOrigins::start().unwrap();
        origins.register_lifetime("fast", "fast-origin").unwrap();
        origins.mark_lifetime_start("fast").unwrap();
        let origin = connect(&origins, "fast", "fast-origin", "origin", "2000");
        let bids =
            ["a", "b", "c"].map(|role| connect(&origins, "fast", "fast-origin", role, "34000"));
        let mediator = connect(&origins, "fast", "fast-origin", "mediator", "400000");
        assert!(read(origin).ends_with(b"1\r\nO\r\n0\r\n\r\n"));
        for (index, bid) in bids.into_iter().enumerate() {
            assert_eq!(*read(bid).last().unwrap(), b'A' + index as u8);
        }
        assert!(read(mediator).is_empty());
        let events = origins.take_lifetime_events("fast").unwrap();
        for (role, expected) in [
            ("origin", 120_000),
            ("a", 180_000),
            ("b", 320_000),
            ("c", 400_000),
        ] {
            assert_eq!(
                event(&events, role, "head")["scenario_scheduled_us"],
                expected
            );
        }
        assert_eq!(
            event(&events, "origin", "chunk")["scenario_scheduled_us"],
            260_000
        );
        assert_eq!(event(&events, "mediator", "closed")["reason"], "silent");
        assert_eq!(
            event(&events, "mediator", "closed")["scenario_scheduled_us"],
            1_500_000
        );
        assert!(
            !events
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["role"] == "mediator"
                    && matches!(entry["op"].as_str(), Some("head" | "chunk")))
        );
        origins.finish().unwrap();
    }

    #[test]
    fn batch_and_quiet_schedules_are_relative_to_each_actual_receipt() {
        let origins = WireOrigins::start().unwrap();
        origins
            .register_lifetime("batch", "batch-progress")
            .unwrap();
        origins.mark_lifetime_start("batch").unwrap();
        let sockets = ["a", "b", "c", "quiet"]
            .map(|role| connect(&origins, "batch", "batch-progress", role, "34000"));
        for socket in sockets {
            read(socket);
        }
        let events = origins.take_lifetime_events("batch").unwrap();
        for (role, delay) in [
            ("a", 20_000),
            ("b", 40_000),
            ("c", 60_000),
            ("quiet", 300_000),
        ] {
            let request = event(&events, role, "request");
            let head = event(&events, role, "head");
            let write_started = head["write_started_us"].as_u64().unwrap();
            assert!(write_started >= head["scheduled_us"].as_u64().unwrap());
            assert!(write_started <= head["at_us"].as_u64().unwrap());
            assert_eq!(
                head["scheduled_us"].as_u64().unwrap(),
                request["at_us"].as_u64().unwrap() + delay
            );
            assert!(head.get("scenario_scheduled_us").is_none());
            assert!(request.get("alignment").is_none());
        }
        origins.finish().unwrap();
    }

    #[test]
    fn invalid_routes_clocks_and_duplicate_roles_remain_cleanup_failures() {
        for (case, role, clock, expected) in [
            ("wrong", "origin", "2", "lifetime case mismatch"),
            ("begin-progress", "a", "2", "unknown lifetime role"),
            (
                "begin-progress",
                "origin/extra",
                "2",
                "invalid lifetime route",
            ),
            (
                "begin-progress",
                "origin",
                "",
                "invalid lifetime guest clock",
            ),
            (
                "begin-progress",
                "origin",
                "-1",
                "invalid lifetime guest clock",
            ),
            (
                "begin-progress",
                "origin",
                "18446744073709551616",
                "invalid lifetime guest clock",
            ),
            (
                "begin-progress",
                "origin",
                "1\r\nx-fixture-at-us: 2",
                "invalid lifetime guest clock",
            ),
        ] {
            let origins = WireOrigins::start().unwrap();
            origins
                .register_lifetime("invalid", "begin-progress")
                .unwrap();
            origins.mark_lifetime_start("invalid").unwrap();
            assert!(read(connect(&origins, "invalid", case, role, clock)).is_empty());
            assert!(origins.finish().unwrap_err().to_string().contains(expected));
        }
        let origins = WireOrigins::start().unwrap();
        origins
            .register_lifetime("duplicate", "begin-progress")
            .unwrap();
        origins.mark_lifetime_start("duplicate").unwrap();
        read(connect(
            &origins,
            "duplicate",
            "begin-progress",
            "origin",
            "2",
        ));
        origins.take_lifetime_events("duplicate").unwrap();
        assert!(
            read(connect(
                &origins,
                "duplicate",
                "begin-progress",
                "origin",
                "2"
            ))
            .is_empty()
        );
        assert!(
            origins
                .finish()
                .unwrap_err()
                .to_string()
                .contains("duplicate lifetime role")
        );
    }

    #[test]
    fn undrained_lifetime_events_fail_finish() {
        let origins = WireOrigins::start().unwrap();
        origins
            .register_lifetime("undrained", "begin-progress")
            .unwrap();
        origins.mark_lifetime_start("undrained").unwrap();
        read(connect(
            &origins,
            "undrained",
            "begin-progress",
            "origin",
            "2",
        ));
        assert!(
            origins
                .finish()
                .unwrap_err()
                .to_string()
                .contains("undrained lifetime events")
        );
    }

    #[test]
    fn shutdown_interrupts_schedules_and_retains_late_closed_events() {
        let origins = WireOrigins::start().unwrap();
        origins.register_lifetime("stopped", "slow-origin").unwrap();
        origins.mark_lifetime_start("stopped").unwrap();
        let origin = connect(&origins, "stopped", "slow-origin", "origin", "2000");
        let shared = Arc::clone(&origins.shared);
        shared
            .wait(IO_BUDGET, |state| {
                !state.lifetimes["stopped"].events.is_empty()
            })
            .unwrap();
        let start = Instant::now();
        assert!(
            origins
                .finish()
                .unwrap_err()
                .to_string()
                .contains("undrained lifetime events")
        );
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(read(origin).is_empty());
        let state = shared.lock();
        assert_eq!(state.active, 0);
        assert!(state.workers.is_empty());
        assert!(
            state.lifetimes["stopped"]
                .events
                .iter()
                .any(|entry| entry["op"] == "closed" && entry["reason"] == "stopped")
        );
    }
}

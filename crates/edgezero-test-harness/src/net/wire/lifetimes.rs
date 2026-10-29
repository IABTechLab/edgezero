use super::*;
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use std::{collections::HashSet, net::Shutdown};

const EVENT_LIMIT: usize = 32;
const CAPS_BUFFER_BYTES: usize = 64 * 1024;
const CAPS_PIECE_BYTES: usize = 16 * 1024;
const CAPS_PIECES: usize = 8;

#[derive(Clone, Copy)]
enum Case {
    Slow,
    Timeout,
    Fast,
    Begin,
    Batch,
    PendingHead,
    PendingBody,
    Cpu,
    Caps,
    Handoff,
    Cleanup,
}

impl Case {
    fn parse(case: &str) -> Result<Self> {
        match case {
            "slow-origin" => Ok(Self::Slow),
            "timeout-before-cutoff" => Ok(Self::Timeout),
            "fast-origin" => Ok(Self::Fast),
            "begin-progress" => Ok(Self::Begin),
            "batch-progress" => Ok(Self::Batch),
            "pending-head-cutoff" => Ok(Self::PendingHead),
            "pending-body-cutoff" => Ok(Self::PendingBody),
            "cpu-stretch" => Ok(Self::Cpu),
            "caps-while-waiting" => Ok(Self::Caps),
            "response-handoff" => Ok(Self::Handoff),
            "response-cleanup" => Ok(Self::Cleanup),
            _ => Err("unknown lifetime case".into()),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Slow => "slow-origin",
            Self::Timeout => "timeout-before-cutoff",
            Self::Fast => "fast-origin",
            Self::Begin => "begin-progress",
            Self::Batch => "batch-progress",
            Self::PendingHead => "pending-head-cutoff",
            Self::PendingBody => "pending-body-cutoff",
            Self::Cpu => "cpu-stretch",
            Self::Caps => "caps-while-waiting",
            Self::Handoff => "response-handoff",
            Self::Cleanup => "response-cleanup",
        }
    }

    fn permits(self, role: &str) -> bool {
        match self {
            Self::Slow => matches!(role, "origin" | "a" | "b" | "c"),
            Self::Timeout => matches!(role, "origin" | "a" | "b" | "c" | "d"),
            Self::Cpu => matches!(role, "origin" | "a" | "b" | "c" | "calibration"),
            Self::Fast => matches!(role, "origin" | "a" | "b" | "c" | "mediator"),
            Self::Begin => role == "origin",
            Self::Batch => matches!(role, "a" | "b" | "c" | "quiet"),
            Self::PendingHead | Self::PendingBody => matches!(role, "a" | "b" | "c"),
            Self::Caps => matches!(role, "a" | "b" | "c"),
            Self::Handoff => matches!(role, "origin" | "a" | "b" | "c"),
            Self::Cleanup => matches!(
                role,
                "origin" | "a" | "b" | "c" | "callback" | "ready" | "post"
            ),
        }
    }

    fn held(self) -> bool {
        matches!(
            self,
            Self::PendingHead | Self::PendingBody | Self::Handoff | Self::Cleanup
        )
    }
}

pub(super) struct Lifetime {
    case: Case,
    anchor: Option<Instant>,
    scenario_anchor: Option<Instant>,
    calibration: Option<Calibration>,
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
            calibration: None,
            roles: HashSet::new(),
            active: 0,
            events: Vec::new(),
            released: false,
        },
    );
    Ok(())
}

pub(super) fn release(shared: &Shared, token: &str, observed: Instant) -> Result<()> {
    let mut state = shared.lock();
    let lifetime = state
        .lifetimes
        .get_mut(token)
        .ok_or("unknown lifetime token")?;
    require(lifetime.case.held(), "lifetime does not use a release gate")?;
    let anchor = lifetime.anchor.ok_or("lifetime not started")?;
    require(!lifetime.released, "lifetime already released")?;
    let handoff = matches!(lifetime.case, Case::Handoff);
    require(
        lifetime.events.len() + if handoff { 2 } else { 1 } <= EVENT_LIMIT,
        "lifetime event limit",
    )?;
    let released_at = Instant::now();
    require(
        (anchor..=released_at).contains(&observed),
        "invalid response return boundary",
    )?;
    if handoff {
        lifetime.events.push(
            json!({"op":"client-prefix-observed","role":"control","token":token,
            "case":lifetime.case.name(),"at_us":offset(observed,anchor)}),
        );
    }
    let mut event = json!({"op":"release","role":"control","token":token,
    "case":lifetime.case.name(),"at_us":offset(released_at,anchor),
    });
    event[if handoff {
        "prefix_observed_us"
    } else {
        "response_returned_us"
    }] = json!(offset(observed, anchor));
    lifetime.events.push(event);
    lifetime.released = true;
    shared.wake.notify_all();
    Ok(())
}

pub(super) fn mark_start(shared: &Shared, token: &str) -> Result<Instant> {
    let mut state = shared.lock();
    require(!state.stop, "wire origin stopped")?;
    let lifetime = state
        .lifetimes
        .get_mut(token)
        .ok_or("unknown lifetime token")?;
    require(lifetime.anchor.is_none(), "lifetime already started")?;
    let anchor = Instant::now();
    lifetime.anchor = Some(anchor);
    Ok(anchor)
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
    calibration_wait: Option<Instant>,
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
                event["schedule_basis"] =
                    json!(if matches!(self.case, Case::Cpu) && self.role != "origin" {
                        if self.role == "calibration" {
                            "calibrated-upper-bound"
                        } else {
                            "calibrated-causal-midpoint"
                        }
                    } else {
                        "origin-receipt-minus-guest-at-us"
                    });
                if self.role == "calibration" {
                    event["scenario_scheduled_us"] = json!(450000);
                }
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

#[derive(Clone, Copy)]
struct Calibration {
    anchor: Instant,
    wait_until: Instant,
}

fn calibrate(lifetime: &Lifetime) -> Result<Option<Calibration>> {
    let find = |role: &str, op: &str| {
        lifetime
            .events
            .iter()
            .find(|entry| entry["role"] == role && entry["op"] == op)
    };
    let (Some(head), Some(payload), Some(request), Some(initial)) = (
        find("origin", "head"),
        find("origin", "chunk"),
        find("calibration", "request"),
        find("origin", "request"),
    ) else {
        return Ok(None);
    };
    let number = |value: &Value, field: &str| {
        value[field]
            .as_i64()
            .map(i128::from)
            .ok_or("invalid calibration sample")
    };
    let lower =
        number(head, "write_started_us")? - number(request, "origin_head_observed_us")? - 1000;
    let upper = (number(request, "at_us")? - number(request, "guest_at_us")?)
        .min(number(initial, "at_us")? - number(initial, "guest_at_us")?)
        + 1000;
    require(
        number(request, "at_us")? >= number(payload, "write_started_us")?,
        "calibration precedes the origin payload write",
    )?;
    require(lower <= upper, "contradictory calibration clock bounds")?;
    let anchor = lifetime.anchor.ok_or("lifetime not started")?;
    let at = |micros: i128| -> Result<Instant> {
        let duration = Duration::from_micros(u64::try_from(micros.unsigned_abs())?);
        if micros >= 0 {
            anchor.checked_add(duration)
        } else {
            anchor.checked_sub(duration)
        }
        .ok_or_else(|| "invalid calibration clock offset".into())
    };
    Ok(Some(Calibration {
        anchor: at(lower + (upper - lower) / 2)?,
        wait_until: at(450000 + upper)?,
    }))
}

fn guest_clock(fields: &[(String, String)], name: &str) -> Result<u64> {
    let mut values = fields
        .iter()
        .filter(|(field, _)| field == name)
        .map(|(_, value)| value);
    let value = values.next().ok_or("missing lifetime guest clock")?;
    require(
        values.next().is_none()
            && !value.is_empty()
            && value.bytes().all(|byte| byte.is_ascii_digit()),
        "invalid lifetime guest clock",
    )?;
    let value = value
        .parse::<u64>()
        .map_err(|_| "invalid lifetime guest clock")?;
    require(value <= 15_000_000, "invalid lifetime guest clock")?;
    Ok(value)
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
        let guest_at = guest_clock(fields, "x-fixture-at-us")?;
        let observed = if matches!(lifetime.case, Case::Cpu) && *role == "calibration" {
            let observed = guest_clock(fields, "x-fixture-origin-head-us")?;
            require(
                observed <= guest_at,
                "calibration precedes head observation",
            )?;
            Some(observed)
        } else {
            None
        };
        require(!lifetime.roles.contains(*role), "duplicate lifetime role")?;
        if matches!(
            lifetime.case,
            Case::Slow | Case::Timeout | Case::Fast | Case::Cpu
        ) && *role == "origin"
        {
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
            calibration_wait: None,
            received: head_received,
        };
        let mut event = exchange.event("request", exchange.received, None);
        event["guest_at_us"] = json!(guest_at);
        if let Some(observed) = observed {
            event["origin_head_observed_us"] = json!(observed);
        }
        if matches!(
            lifetime.case,
            Case::Slow | Case::Timeout | Case::Fast | Case::Cpu
        ) && *role == "origin"
        {
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
    let expected_close = matches!(exchange.case, Case::Slow | Case::Timeout | Case::Cpu)
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
    let silent = matches!(
        (exchange.case, exchange.role),
        (Case::Fast, "mediator") | (Case::Timeout, "d")
    );
    let scheduled_close = if silent {
        exchange
            .scenario_anchor
            .map(|anchor| anchor + Duration::from_millis(1500))
    } else {
        None
    };
    let mut event = exchange.event("closed", Instant::now(), scheduled_close);
    event["reason"] = json!(match &outcome {
        Err(_) if expected_close => "peer_closed_after_cutoff",
        Ok(true) if silent => "silent",
        Ok(true) => "complete",
        Ok(false)
            if matches!(exchange.case, Case::Caps)
                && exchange.role == "a"
                && !shared.lock().stop =>
            "peer_closed_after_cap_prefix",
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
    if !matches!(
        exchange.case,
        Case::Slow | Case::Timeout | Case::Fast | Case::Cpu
    ) {
        return Ok(true);
    }
    let deadline = Instant::now() + IO_BUDGET;
    let mut state = shared.lock();
    loop {
        if state.stop {
            return Ok(false);
        }
        let lifetime = state.lifetimes.get_mut(exchange.token).unwrap();
        if matches!(exchange.case, Case::Cpu) && exchange.role != "origin" {
            if lifetime.calibration.is_none() {
                lifetime.calibration = calibrate(lifetime)?;
            }
            if let Some(calibration) = lifetime.calibration {
                exchange.scenario_anchor = Some(calibration.anchor);
                exchange.calibration_wait = Some(calibration.wait_until);
                shared.wake.notify_all();
                return Ok(true);
            }
        } else if let Some(anchor) = lifetime.scenario_anchor {
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

struct CapsPayload {
    encoded: Vec<u8>,
    ends: [usize; CAPS_PIECES],
}

impl CapsPayload {
    fn new() -> Result<Self> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::none());
        let mut ends = [0; CAPS_PIECES];
        for end in &mut ends {
            encoder.write_all(&[b'x'; CAPS_PIECE_BYTES])?;
            encoder.flush()?;
            *end = encoder.get_ref().len();
        }
        let encoded = encoder.finish()?;
        require(encoded.len() <= 129 * 1024, "caps source encoding bound")?;
        ends[CAPS_PIECES - 1] = encoded.len();
        Ok(Self { encoded, ends })
    }
}

fn decoded_prefix(encoded: &[u8]) -> Result<usize> {
    let mut decoder = GzDecoder::new(encoded);
    let mut scratch = [0; 8192];
    let mut decoded = 0;
    while decoded <= CAPS_BUFFER_BYTES {
        let limit = scratch.len().min(CAPS_BUFFER_BYTES + 1 - decoded);
        match decoder.read(&mut scratch[..limit]) {
            Ok(0) => break,
            Ok(count) => decoded += count,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(decoded)
}

fn counted_write(writer: &mut impl Write, bytes: &[u8]) -> (usize, io::Result<()>) {
    let mut written = 0;
    let mut interrupted = 0;
    while written < bytes.len() {
        match writer.write(&bytes[written..]) {
            Ok(0) => return (written, Err(io::ErrorKind::WriteZero.into())),
            Ok(count) if count <= bytes.len() - written => written += count,
            Ok(_) => return (written, Err(io::ErrorKind::InvalidData.into())),
            Err(error) if error.kind() == io::ErrorKind::Interrupted && interrupted < 32 => {
                interrupted += 1
            }
            Err(error) => return (written, Err(error)),
        }
    }
    (written, writer.flush())
}

fn respond_caps(socket: &mut TcpStream, shared: &Shared, exchange: &Exchange<'_>) -> Result<bool> {
    let payload = CapsPayload::new()?;
    let started = Instant::now();
    write!(
        socket,
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\n\r\n",
        payload.encoded.len()
    )?;
    socket.flush()?;
    let body_start = Instant::now();
    let mut head = exchange.event("head", body_start, None);
    head["write_started_us"] = json!(offset(started, exchange.anchor));
    head["body_start_us"] = json!(offset(body_start, exchange.anchor));
    head["content_encoding"] = json!("gzip");
    head["content_length"] = json!(payload.encoded.len());
    head["decoded_bytes"] = json!(CAPS_PIECE_BYTES * CAPS_PIECES);
    head["piece_ends"] = json!(payload.ends);
    record(shared, exchange, head)?;
    let mut start = 0;
    for (index, end) in payload.ends.into_iter().enumerate() {
        let scheduled =
            body_start + Duration::from_micros(index as u64 * 300_000 / (CAPS_PIECES as u64 - 1));
        if !wait_until(shared, scheduled) {
            return Ok(false);
        }
        let started = Instant::now();
        let (written, outcome) = counted_write(socket, &payload.encoded[start..end]);
        let completed = Instant::now();
        let decoded = decoded_prefix(&payload.encoded[..start + written])?;
        let mut event = exchange.event("chunk", completed, Some(scheduled));
        event["schedule_basis"] = json!("body-start");
        event["index"] = json!(index);
        event["write_started_us"] = json!(offset(started, exchange.anchor));
        event["attempted_bytes"] = json!(end - start);
        event["written_bytes"] = json!(written);
        event["encoded_end"] = json!(start + written);
        event["decoded_prefix_bytes"] = json!(decoded);
        event["io_error"] = json!(
            outcome
                .as_ref()
                .err()
                .map(|error| format!("{:?}", error.kind()))
        );
        record(shared, exchange, event)?;
        if let Err(error) = outcome {
            if decoded > CAPS_BUFFER_BYTES
                && matches!(
                    error.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                )
            {
                return Ok(false);
            }
            return Err(error.into());
        }
        start = end;
    }
    Ok(true)
}

fn respond(socket: &mut TcpStream, shared: &Shared, exchange: &Exchange<'_>) -> Result<bool> {
    if matches!(exchange.case, Case::Caps) && exchange.role == "a" {
        return respond_caps(socket, shared, exchange);
    }
    if matches!(exchange.case, Case::Cpu) && exchange.role == "calibration" {
        let scheduled = exchange
            .calibration_wait
            .ok_or("missing calibration wait")?;
        if !wait_until(shared, scheduled) {
            return Ok(false);
        }
        let started = Instant::now();
        socket.write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")?;
        socket.flush()?;
        let mut event = exchange.event("head", Instant::now(), Some(scheduled));
        event["write_started_us"] = json!(offset(started, exchange.anchor));
        record(shared, exchange, event)?;
        return Ok(true);
    }
    if matches!(exchange.case, Case::Cleanup) && exchange.role == "ready" {
        shared.wait(GATE_BUDGET, |state| {
            state.stop
                || state.lifetimes[exchange.token]
                    .events
                    .iter()
                    .filter(|event| event["op"] == "waiting")
                    .count()
                    == 5
        })?;
        require(!shared.lock().stop, "cleanup barrier stopped")?;
        let started = Instant::now();
        socket.write_all(
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        socket.flush()?;
        let mut event = exchange.event("head", Instant::now(), None);
        event["write_started_us"] = json!(offset(started, exchange.anchor));
        record(shared, exchange, event)?;
        return Ok(true);
    }
    if exchange.case.held() && !(matches!(exchange.case, Case::Cleanup) && exchange.role == "post")
    {
        let handoff = matches!(exchange.case, Case::Handoff);
        let cleanup = matches!(exchange.case, Case::Cleanup);
        if !handoff && !cleanup && shared.lock().lifetimes[exchange.token].released {
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
        if (!handoff && !(cleanup && exchange.role == "callback")) || shared.lock().stop {
            return Ok(false);
        }
    }
    let ms = |value| Duration::from_millis(value);
    let absolute = |value| exchange.scenario_anchor.unwrap() + ms(value);
    let received = |value| exchange.received + ms(value);
    let (head_at, chunks): (Instant, Vec<(Instant, &[u8])>) = match (exchange.case, exchange.role) {
        (Case::Cleanup, "callback" | "post") => {
            let at = Instant::now();
            (
                at,
                vec![(
                    at,
                    if exchange.role == "callback" {
                        b"K"
                    } else {
                        b"P"
                    },
                )],
            )
        }
        (Case::Handoff, role) => {
            let at = Instant::now();
            let bytes: &[u8] = match role {
                "origin" => b"O",
                "a" => b"A",
                "b" => b"B",
                "c" => b"C",
                _ => unreachable!(),
            };
            (at, vec![(at, bytes)])
        }
        (Case::Slow | Case::Timeout, "origin") => (
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
        (Case::Fast, "mediator") | (Case::Timeout, "d") => {
            // Stay silent beyond both the auction cutoff and the 1000 ms per-call timer.
            return Ok(wait_until(shared, absolute(1500)));
        }
        (case, role @ ("a" | "b" | "c")) => {
            let index = usize::from(role.as_bytes()[0] - b'a');
            let at = match case {
                Case::Slow | Case::Timeout => absolute([180, 320, 900][index]),
                Case::Fast => absolute([180, 320, 400][index]),
                Case::Cpu => absolute([180, 530, 900][index]),
                Case::Batch => received([20, 40, 60][index]),
                Case::Caps => received([0, 20, 40][index]),
                Case::Begin
                | Case::PendingHead
                | Case::PendingBody
                | Case::Handoff
                | Case::Cleanup => {
                    unreachable!()
                }
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
    let mut event = exchange.event(
        "head",
        Instant::now(),
        (!matches!(exchange.case, Case::Handoff | Case::Cleanup)).then_some(head_at),
    );
    event["write_started_us"] = json!(offset(write_started, exchange.anchor));
    record(shared, exchange, event)?;
    for (at, bytes) in chunks {
        if !wait_until(shared, at) {
            return Ok(false);
        }
        let write_started = Instant::now();
        if chunked {
            write!(socket, "{:X}\r\n", bytes.len())?;
        }
        socket.write_all(bytes)?;
        if chunked {
            socket.write_all(b"\r\n")?;
        }
        socket.flush()?;
        let mut event = exchange.event(
            "chunk",
            Instant::now(),
            (!matches!(exchange.case, Case::Handoff | Case::Cleanup)).then_some(at),
        );
        event["bytes"] = json!(bytes);
        event["write_started_us"] = json!(offset(write_started, exchange.anchor));
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
    fn cleanup_source_barrier_precedes_egress_and_only_callback_work_can_complete() {
        let origins = WireOrigins::start().unwrap();
        origins
            .register_lifetime("cleanup", "response-cleanup")
            .unwrap();
        origins.mark_lifetime_start("cleanup").unwrap();
        let pending = ["origin", "a", "b", "c", "callback"].map(|role| {
            (
                role,
                connect(&origins, "cleanup", "response-cleanup", role, "0"),
            )
        });
        let ready = read(connect(
            &origins,
            "cleanup",
            "response-cleanup",
            "ready",
            "0",
        ));
        assert!(ready.starts_with(b"HTTP/1.1 204"));
        assert!(ready.ends_with(b"\r\n\r\n"));
        for (_, socket) in &pending {
            socket
                .set_read_timeout(Some(Duration::from_millis(10)))
                .unwrap();
            assert!(
                matches!(socket.peek(&mut [0]), Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut))
            );
            socket.set_read_timeout(Some(IO_BUDGET)).unwrap();
        }
        origins.release_lifetime("cleanup", Instant::now()).unwrap();
        for (role, socket) in pending {
            let body = read(socket);
            if role == "callback" {
                assert!(body.ends_with(b"K"));
            } else {
                assert!(
                    body.is_empty(),
                    "body-owned work must not receive a response"
                );
            }
        }
        assert!(
            read(connect(
                &origins,
                "cleanup",
                "response-cleanup",
                "post",
                "0"
            ))
            .ends_with(b"P")
        );
        let events = origins.take_lifetime_events("cleanup").unwrap();
        assert_eq!(events.as_array().unwrap().len(), 25);
        let ready = event(&events, "ready", "head")["at_us"].as_u64().unwrap();
        for role in ["origin", "a", "b", "c", "callback"] {
            assert!(event(&events, role, "waiting")["at_us"].as_u64().unwrap() <= ready);
        }
        for role in ["origin", "a", "b", "c"] {
            assert_eq!(event(&events, role, "closed")["reason"], "fixture_released");
        }
        origins.finish().unwrap();
    }

    #[test]
    fn response_handoff_holds_all_heads_until_the_client_prefix_and_then_succeeds() {
        let origins = WireOrigins::start().unwrap();
        origins
            .register_lifetime("handoff", "response-handoff")
            .unwrap();
        origins.mark_lifetime_start("handoff").unwrap();
        let sockets = ["origin", "a", "b", "c"].map(|role| {
            (
                role,
                connect(
                    &origins,
                    "handoff",
                    "response-handoff",
                    role,
                    if role == "origin" { "2000" } else { "34000" },
                ),
            )
        });
        origins
            .shared
            .wait(IO_BUDGET, |state| {
                state.lifetimes["handoff"]
                    .events
                    .iter()
                    .filter(|event| event["op"] == "waiting")
                    .count()
                    == 4
            })
            .unwrap();
        for (_, socket) in &sockets {
            socket
                .set_read_timeout(Some(Duration::from_millis(10)))
                .unwrap();
            let error = socket.peek(&mut [0]).unwrap_err();
            assert!(matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ));
            socket.set_read_timeout(Some(IO_BUDGET)).unwrap();
        }
        origins.release_lifetime("handoff", Instant::now()).unwrap();
        for (role, socket) in sockets {
            let bytes = read(socket);
            if role == "origin" {
                assert!(bytes.ends_with(b"1\r\nO\r\n0\r\n\r\n"));
            } else {
                assert_eq!(
                    *bytes.last().unwrap(),
                    role.as_bytes()[0].to_ascii_uppercase()
                );
            }
        }
        let events = origins.take_lifetime_events("handoff").unwrap();
        assert_eq!(events.as_array().unwrap().len(), 22);
        let prefix = event(&events, "control", "client-prefix-observed");
        let release = event(&events, "control", "release");
        assert_eq!(release["prefix_observed_us"], prefix["at_us"]);
        assert!(release["response_returned_us"].is_null());
        for role in ["origin", "a", "b", "c"] {
            assert!(
                event(&events, role, "head")["write_started_us"]
                    .as_u64()
                    .unwrap()
                    >= release["at_us"].as_u64().unwrap()
            );
            assert_eq!(event(&events, role, "closed")["reason"], "complete");
        }
        origins.finish().unwrap();
    }

    #[test]
    fn caps_source_sends_one_bounded_gzip_member_over_the_full_trickle_window() {
        let origins = WireOrigins::start().unwrap();
        origins
            .register_lifetime("caps", "caps-while-waiting")
            .unwrap();
        origins.mark_lifetime_start("caps").unwrap();
        let mut reader = BufReader::new(connect(
            &origins,
            "caps",
            "caps-while-waiting",
            "a",
            "34000",
        ));
        let head = read_head(&mut reader, &mut 0, Profile::Wire).unwrap();
        assert_eq!(head.first, "HTTP/1.1 200 OK");
        let mut encoded = Vec::new();
        reader
            .take(129 * 1024 + 1)
            .read_to_end(&mut encoded)
            .unwrap();
        assert!(encoded.len() <= 129 * 1024);
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(encoded.as_slice())
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(decoded, vec![b'x'; 128 * 1024]);
        let events = origins.take_lifetime_events("caps").unwrap();
        let head = event(&events, "a", "head");
        assert_eq!(head["content_encoding"], "gzip");
        let chunks: Vec<_> = events
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["op"] == "chunk")
            .collect();
        assert_eq!(chunks.len(), 8);
        assert_eq!(chunks[0]["schedule_basis"], "body-start");
        assert_eq!(chunks[3]["decoded_prefix_bytes"], 64 * 1024);
        assert_eq!(chunks[4]["decoded_prefix_bytes"], 64 * 1024 + 1);
        assert_eq!(
            chunks[7]["scheduled_us"].as_u64().unwrap()
                - chunks[0]["scheduled_us"].as_u64().unwrap(),
            300_000
        );
        assert_eq!(event(&events, "a", "closed")["reason"], "complete");
        origins.finish().unwrap();
    }

    #[test]
    fn caps_counted_writes_retain_partial_progress_and_bound_interrupted_retries() {
        struct PrefixWriter {
            remaining: usize,
            interrupts: usize,
            calls: usize,
            error: io::ErrorKind,
        }
        impl Write for PrefixWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.calls += 1;
                if self.interrupts > 0 {
                    self.interrupts -= 1;
                    return Err(io::ErrorKind::Interrupted.into());
                }
                if self.remaining == 0 {
                    return Err(self.error.into());
                }
                let written = self.remaining.min(bytes.len());
                self.remaining -= written;
                Ok(written)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        for kind in [
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::TimedOut,
        ] {
            let mut writer = PrefixWriter {
                remaining: 5,
                interrupts: 1,
                calls: 0,
                error: kind,
            };
            let (written, outcome) = counted_write(&mut writer, b"1234567890");
            assert_eq!(written, 5);
            assert_eq!(outcome.unwrap_err().kind(), kind);
            assert_eq!(writer.calls, 3);
        }
        let mut writer = PrefixWriter {
            remaining: 10,
            interrupts: 100,
            calls: 0,
            error: io::ErrorKind::BrokenPipe,
        };
        let (written, outcome) = counted_write(&mut writer, b"1234567890");
        assert_eq!(written, 0);
        assert_eq!(outcome.unwrap_err().kind(), io::ErrorKind::Interrupted);
        assert_eq!(writer.calls, 33);
        let mut writer = std::io::Cursor::new([0_u8; 0]);
        let (written, outcome) = counted_write(&mut writer, b"x");
        assert_eq!(written, 0);
        assert_eq!(outcome.unwrap_err().kind(), io::ErrorKind::WriteZero);
    }

    #[test]
    fn caps_prefix_counts_exact_cap_and_first_excess_without_a_trailer() {
        let payload = CapsPayload::new().unwrap();
        assert_eq!(
            decoded_prefix(&payload.encoded[..payload.ends[3]]).unwrap(),
            65536
        );
        let crossing = payload.ends[4];
        assert_eq!(decoded_prefix(&payload.encoded[..crossing]).unwrap(), 65537);
        assert!(crossing < payload.encoded.len());
        let mut malformed = payload.encoded.clone();
        malformed[0] = 0;
        assert!(decoded_prefix(&malformed).is_err());
    }

    #[test]
    fn registration_accepts_the_same_ascii_token_alphabet_as_the_guest() {
        let shared = Shared::default();
        register(&shared, "Run_1-a", "begin-progress").unwrap();
    }

    #[test]
    fn timeout_before_cutoff_origin_never_sends_a_fourth_bidder_response() {
        let origins = WireOrigins::start().unwrap();
        origins
            .register_lifetime("timeout", "timeout-before-cutoff")
            .unwrap();
        origins.mark_lifetime_start("timeout").unwrap();
        let origin = connect(
            &origins,
            "timeout",
            "timeout-before-cutoff",
            "origin",
            "2000",
        );
        let silent = connect(&origins, "timeout", "timeout-before-cutoff", "d", "34000");
        assert!(read(origin).ends_with(b"1\r\n!\r\n0\r\n\r\n"));
        assert!(read(silent).is_empty());
        let events = origins.take_lifetime_events("timeout").unwrap();
        let phases: Vec<_> = events
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["role"] == "d")
            .map(|entry| entry["op"].as_str().unwrap())
            .collect();
        assert_eq!(phases, ["request", "closed"]);
        let closed = event(&events, "d", "closed");
        assert_eq!(closed["reason"], "silent");
        assert_eq!(closed["scenario_scheduled_us"], 1_500_000);
        assert!(closed["at_us"].as_i64().unwrap() >= closed["scheduled_us"].as_i64().unwrap());
        assert_eq!(
            event(&events, "origin", "head")["scenario_scheduled_us"],
            700_000
        );
        origins.finish().unwrap();
    }

    #[test]
    fn calibration_separates_midpoint_scheduling_from_the_upper_wait_bound() {
        let shared = Shared::default();
        register(&shared, "calibration", "cpu-stretch").unwrap();
        mark_start(&shared, "calibration").unwrap();
        let mut state = shared.lock();
        let lifetime = state.lifetimes.get_mut("calibration").unwrap();
        let anchor = lifetime.anchor.unwrap();
        lifetime.events = vec![
            json!({"op":"request","role":"origin","at_us":9000,"guest_at_us":2000}),
            json!({"op":"head","role":"origin","write_started_us":100000}),
            json!({"op":"chunk","role":"origin","write_started_us":100150}),
        ];
        assert!(calibrate(lifetime).unwrap().is_none());
        lifetime.events.push(json!({"op":"request","role":"calibration","at_us":101600,"guest_at_us":100600,"origin_head_observed_us":100500}));
        let calibration = calibrate(lifetime).unwrap().unwrap();
        assert_eq!(offset(calibration.anchor, anchor), 250);
        assert_eq!(offset(calibration.wait_until, anchor), 452000);
        lifetime.events[3]["at_us"] = json!(97000);
        assert!(calibrate(lifetime).is_err());
    }

    #[test]
    fn calibration_clock_fields_are_bounded_and_unambiguous() {
        for name in ["x-fixture-at-us", "x-fixture-origin-head-us"] {
            for value in ["0", "15000000"] {
                assert!(guest_clock(&[(name.into(), value.into())], name).is_ok());
            }
            for value in ["", "-1", "+1", "1,2", "15000001", "18446744073709551616"] {
                assert!(guest_clock(&[(name.into(), value.into())], name).is_err());
            }
            assert!(guest_clock(&[], name).is_err());
            assert!(
                guest_clock(
                    &[(name.into(), "1".into()), (name.into(), "1".into())],
                    name
                )
                .is_err()
            );
        }
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
        let mut sockets: Vec<_> = [
            ("origin", "0"),
            ("a", "34000"),
            ("b", "34000"),
            ("c", "34000"),
        ]
        .into_iter()
        .map(|(role, at)| connect(&origins, "cpu", "cpu-stretch", role, at))
        .collect();
        assert_eq!(read(sockets.remove(0)), b"HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nO\r\n0\r\n\r\n");
        let anchor = origins.shared.lock().lifetimes["cpu"].anchor.unwrap();
        let observed = offset(Instant::now(), anchor);
        let mut calibration = TcpStream::connect(("127.0.0.1", origins.port(0))).unwrap();
        calibration.set_read_timeout(Some(IO_BUDGET)).unwrap();
        write!(calibration, "GET /lifetimes/cpu/cpu-stretch/calibration HTTP/1.1\r\nx-fixture-at-us: {}\r\nx-fixture-origin-head-us: {observed}\r\n\r\n", offset(Instant::now(), anchor)).unwrap();
        assert_eq!(
            read(calibration),
            b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"
        );
        for (index, socket) in sockets.into_iter().enumerate() {
            let mut expected =
                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 1\r\n\r\n".to_vec();
            expected.push(b'A' + index as u8);
            assert_eq!(read(socket), expected);
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
        assert_eq!(events.as_array().unwrap().len(), 19);
        assert_eq!(
            event(&events, "calibration", "head")["scenario_scheduled_us"],
            450000
        );
        assert_eq!(
            event(&events, "b", "head")["schedule_basis"],
            "calibrated-causal-midpoint"
        );
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

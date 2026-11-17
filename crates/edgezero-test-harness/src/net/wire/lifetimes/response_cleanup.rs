use super::*;

pub(super) const CASES: [&str; 5] = [
    "response-owned-cleanup-complete",
    "response-owned-cleanup-disconnect",
    "response-owned-cleanup-write-deadline",
    "response-owned-cleanup-source-error",
    "response-owned-cleanup-cancel",
];

pub(super) fn permits(role: &str) -> bool {
    matches!(
        role,
        "origin"
            | "a"
            | "b"
            | "c"
            | "ready"
            | "ack"
            | "post"
            | "isolated-origin"
            | "isolated-ready"
    )
}

pub(super) fn respond(
    socket: &mut TcpStream,
    shared: &Shared,
    exchange: &Exchange<'_>,
) -> Result<bool> {
    require(
        permits(exchange.role),
        "invalid response-owned cleanup role",
    )?;
    if exchange.role == "post" {
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n")?;
        socket.flush()?;
        record(
            shared,
            exchange,
            exchange.event("head", Instant::now(), None),
        )?;
        socket.write_all(b"P")?;
        socket.flush()?;
        let mut chunk = exchange.event("chunk", Instant::now(), None);
        chunk["bytes"] = json!(b"P");
        record(shared, exchange, chunk)?;
        return Ok(true);
    }
    if matches!(exchange.role, "ready" | "isolated-ready" | "ack") {
        shared.wait(GATE_BUDGET, |state| {
            state.stop
                || state.lifetimes[exchange.token].released
                || if exchange.role == "ack" {
                    state.lifetimes[exchange.token].acknowledged
                } else {
                    let roles: &[&str] = if exchange.role == "isolated-ready" {
                        &["isolated-origin"]
                    } else {
                        &["origin", "a", "b", "c"]
                    };
                    roles.iter().all(|role| {
                        state.lifetimes[exchange.token]
                            .events
                            .iter()
                            .any(|event| event["op"] == "waiting" && event["role"] == *role)
                    })
                }
        })?;
        {
            let state = shared.lock();
            if state.stop || state.lifetimes[exchange.token].released {
                return Ok(false);
            }
        }
        socket.write_all(
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        socket.flush()?;
        record(
            shared,
            exchange,
            exchange.event("head", Instant::now(), None),
        )?;
        return Ok(true);
    }
    if matches!(exchange.role, "origin" | "isolated-origin") {
        let head_at = exchange.received + Duration::from_millis(2);
        if !wait_until(shared, head_at) {
            return Ok(false);
        }
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n")?;
        socket.flush()?;
        let mut head = exchange.event("head", Instant::now(), Some(head_at));
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
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::WireOrigins;

    fn request(origins: &WireOrigins, token: &str, role: &str) -> TcpStream {
        let mut socket =
            TcpStream::connect(("127.0.0.1", origins.port(0))).expect("source connection");
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("read bound");
        write!(socket,"GET /lifetimes/{token}/{}/{role} HTTP/1.1\r\nHost: localhost\r\nx-fixture-at-us: 0\r\nConnection: close\r\n\r\n",CASES[0]).expect("request");
        socket.flush().expect("request head");
        socket
    }

    #[test]
    fn cleanup_source_holds_original_body_and_heads_until_control_release() {
        let origins = WireOrigins::start().expect("origins");
        let token = "response_cleanup_native_shape";
        origins.register_lifetime(token, CASES[0]).expect("case");
        origins.mark_lifetime_start(token).expect("epoch");
        let mut origin = request(&origins, token, "origin");
        let expected = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n";
        let mut head = vec![0; expected.len()];
        origin.read_exact(&mut head).expect("origin head");
        assert_eq!(head, expected);
        let bids = ["a", "b", "c"].map(|role| request(&origins, token, role));
        let mut ready = request(&origins, token, "ready");
        let expected_ready =
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let mut bytes = vec![0; expected_ready.len()];
        ready
            .read_exact(&mut bytes)
            .expect("all four exchanges held");
        assert_eq!(bytes, expected_ready);
        origin
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("pending observation bound");
        let error = origin
            .read(&mut [0_u8])
            .expect_err("no origin body bytes before cleanup");
        assert!(matches!(
            error.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
        ));
        origins
            .acknowledge_cleanup_prefix(token, Instant::now())
            .expect("prefix control");
        let mut ack = request(&origins, token, "ack");
        ack.read_exact(&mut bytes).expect("acknowledgment");
        assert_eq!(bytes, expected_ready);
        let mut post = request(&origins, token, "post");
        let expected_post = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\nP";
        let mut healthy = vec![0; expected_post.len()];
        post.read_exact(&mut healthy).expect("healthy request");
        assert_eq!(healthy, expected_post);
        origins
            .release_lifetime(token, Instant::now())
            .expect("release");
        let events = origins
            .take_lifetime_events(token)
            .expect("joined source exchanges");
        assert!(
            events
                .as_array()
                .expect("events")
                .iter()
                .all(|event| event["role"] != "origin" || event["op"] != "chunk")
        );
        drop(bids);
    }

    #[test]
    fn isolated_origin_and_full_victim_leave_room_for_ready_controls() {
        let origins = WireOrigins::start().expect("origins");
        for token in ["isolated_owner", "victim_owner"] {
            origins.register_lifetime(token, CASES[0]).expect("case");
            origins.mark_lifetime_start(token).expect("epoch");
        }
        let expected_head = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n";
        let expected_ready =
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let mut isolated = request(&origins, "isolated_owner", "isolated-origin");
        let mut head = vec![0; expected_head.len()];
        isolated
            .read_exact(&mut head)
            .expect("one isolated native body source");
        assert_eq!(head, expected_head);
        let mut victim = request(&origins, "victim_owner", "origin");
        victim.read_exact(&mut head).expect("victim body source");
        let bids = ["a", "b", "c"].map(|role| request(&origins, "victim_owner", role));
        let mut first_ready = request(&origins, "isolated_owner", "isolated-ready");
        let mut next_ready = request(&origins, "victim_owner", "ready");
        let mut ready = vec![0; expected_ready.len()];
        first_ready
            .read_exact(&mut ready)
            .expect("isolated control fits global active limit");
        assert_eq!(ready, expected_ready);
        next_ready
            .read_exact(&mut ready)
            .expect("victim control fits global active limit");
        assert_eq!(ready, expected_ready);
        assert_eq!(
            origins.shared.lock().lifetimes["isolated_owner"]
                .roles
                .len(),
            2
        );
        assert_eq!(
            origins.shared.lock().lifetimes["victim_owner"].roles.len(),
            5
        );
        for token in ["isolated_owner", "victim_owner"] {
            origins
                .release_lifetime(token, Instant::now())
                .expect("release");
            origins.take_lifetime_events(token).expect("joined sources");
        }
        drop(bids);
    }

    #[test]
    fn response_cleanup_roles_are_exact_and_do_not_add_another_transport() {
        for role in ["origin", "a", "b", "c", "ready", "ack", "post"] {
            assert!(permits(role));
            assert!(!permits(&format!("{role}-other")));
        }
        assert!(!permits(""));
        assert_eq!(CASES.len(), 5);
    }
}

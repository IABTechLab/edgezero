//! Accept helper for test fixtures that poll a non-blocking std listener.

use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};

/// Accepts one connection and returns its stream in blocking mode.
///
/// Fixtures make the listener non-blocking so their accept loop can poll a deadline or stop
/// flag. macOS `accept(2)` copies `O_NONBLOCK` to the accepted socket (Linux does not), so a
/// read on the returned stream would otherwise fail with `WouldBlock` at once instead of
/// honouring its read timeout. `WouldBlock` from `accept` itself is still returned to the caller.
#[inline]
pub(crate) fn accept_blocking(listener: &TcpListener) -> io::Result<(TcpStream, SocketAddr)> {
    let (stream, peer) = listener.accept()?;
    stream.set_nonblocking(false)?;
    Ok((stream, peer))
}

#[cfg(test)]
mod tests {
    use super::accept_blocking;
    use std::io::{ErrorKind, Read as _};
    use std::net::{TcpListener, TcpStream};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn stream_accepted_from_nonblocking_listener_waits_for_read_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let address = listener.local_addr().expect("listener address");
        let _client = TcpStream::connect(address).expect("connect loopback client");
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(2))
            .expect("deadline fits the monotonic clock");
        let (mut stream, _peer) = loop {
            match accept_blocking(&listener) {
                Ok(accepted) => break accepted,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < deadline,
                        "loopback connection was not queued"
                    );
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accept failed: {error}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_millis(200)))
            .expect("read timeout");
        let started = Instant::now();
        let error = stream
            .read(&mut [0_u8; 1])
            .expect_err("the client sent nothing");
        let waited = started.elapsed();
        assert!(
            matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut),
            "unexpected read error: {error}"
        );
        assert!(
            waited >= Duration::from_millis(150),
            "read returned after {waited:?}, before its 200 ms timeout"
        );
    }
}

use mio::{Events, Interest, Poll, Token, Waker};
use std::{
    io,
    net::{TcpListener, TcpStream},
    sync::Arc,
    time::Duration,
};

const LISTENER: Token = Token(0);
const WAKE: Token = Token(1);
const STOP_FALLBACK: Duration = Duration::from_millis(50);

pub(super) struct Acceptor {
    listener: mio::net::TcpListener,
    poll: Poll,
    events: Events,
    waker: Arc<Waker>,
}

impl Acceptor {
    pub(super) fn new(listener: TcpListener) -> io::Result<Self> {
        listener.set_nonblocking(true)?;
        let mut listener = mio::net::TcpListener::from_std(listener);
        let poll = Poll::new()?;
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKE)?);
        Ok(Self {
            listener,
            poll,
            events: Events::with_capacity(2),
            waker,
        })
    }

    pub(super) fn waker(&self) -> Arc<Waker> {
        Arc::clone(&self.waker)
    }

    pub(super) fn accept(&self) -> io::Result<TcpStream> {
        self.listener.accept().map(|(socket, _)| socket.into())
    }

    pub(super) fn wait(&mut self) -> io::Result<()> {
        // Readiness wakes immediately; the timeout only backs up a missed stop wake.
        match self.poll.poll(&mut self.events, Some(STOP_FALLBACK)) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => Ok(()),
            result => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::TcpStream, sync::mpsc, thread, time::Instant};

    fn listener() -> (Acceptor, u16) {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        (Acceptor::new(listener).unwrap(), port)
    }

    fn wait_for_listener(acceptor: &mut Acceptor, deadline: Instant) {
        loop {
            assert!(Instant::now() < deadline, "listener readiness timed out");
            acceptor.wait().unwrap();
            if acceptor
                .events
                .iter()
                .any(|event| event.token() == LISTENER)
            {
                return;
            }
        }
    }

    fn accept_until(acceptor: &mut Acceptor, deadline: Instant) -> TcpStream {
        loop {
            assert!(Instant::now() < deadline, "queued accept timed out");
            match acceptor.accept() {
                Ok(stream) => return stream,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    wait_for_listener(acceptor, deadline)
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => panic!("queued accept failed: {error}"),
            }
        }
    }

    #[test]
    fn queued_connections_drain_and_new_arrivals_have_their_own_readiness() {
        let (mut acceptor, port) = listener();
        let clients: Vec<_> = (0..3)
            .map(|_| TcpStream::connect(("127.0.0.1", port)).unwrap())
            .collect();
        let deadline = Instant::now() + Duration::from_secs(2);
        wait_for_listener(&mut acceptor, deadline);
        let streams: Vec<_> = (0..clients.len())
            .map(|_| accept_until(&mut acceptor, deadline))
            .collect();
        assert_eq!(
            acceptor.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let next = TcpStream::connect(("127.0.0.1", port)).unwrap();
        wait_for_listener(&mut acceptor, deadline);
        let stream = accept_until(&mut acceptor, deadline);
        assert_eq!(stream.peer_addr().unwrap(), next.local_addr().unwrap());
        drop(streams);
    }

    #[test]
    fn wake_is_retained_before_poll_and_across_the_wait_race() {
        let (mut acceptor, _) = listener();
        let waker = acceptor.waker();
        waker.wake().unwrap();
        acceptor.wait().unwrap();
        assert!(acceptor.events.iter().any(|event| event.token() == WAKE));
        assert_eq!(
            acceptor.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let (ready_tx, ready_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < deadline {
                acceptor.wait().unwrap();
                if acceptor.events.iter().any(|event| event.token() == WAKE) {
                    done_tx.send(true).unwrap();
                    return;
                }
            }
            done_tx.send(false).unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        waker.wake().unwrap();
        let result = done_rx.recv_timeout(Duration::from_secs(3));
        worker.join().unwrap();
        assert!(result.unwrap());
    }

    #[test]
    fn fallback_wait_is_finite_without_any_socket_or_control_wake() {
        let (mut acceptor, _) = listener();
        let rescue = acceptor.waker();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            done_tx.send(acceptor.wait()).unwrap();
        });
        let result = done_rx.recv_timeout(Duration::from_secs(2));
        if result.is_err() {
            rescue.wake().unwrap();
        }
        worker.join().unwrap();
        result.unwrap().unwrap();
    }
}

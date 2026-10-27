"""Test-only wire repro; no alternate outbound transport.

The P5 origin gate proves next-hop progress without intermediate body data, not
the absence of provider prefetch or hosted cancellation guarantees.

From the worktree root (Rust 1.98.1, Spin 4.1.0, spin-sdk 7.0.0, Python 3.9.6):
  cargo build --locked -p edgezero-adapter-spin --no-default-features --features spin,test-utils --target wasm32-wasip2 --example outbound-bodyless-wire
  python3 crates/edgezero-adapter-spin/tests/outbound_bodyless_wire.py
"""

import gzip
import http.client
import io
import json
import os
from pathlib import Path
import queue
import signal
import socket
import socketserver
import subprocess
import tempfile
import threading
import time


def signal_spin_group(process, sig):
    try:
        os.killpg(process.pid, sig)
    except ProcessLookupError:
        pass


def stop_spin(process):
    try:
        signal_spin_group(process, signal.SIGTERM)
        try:
            return process.communicate(timeout=5)[0]
        except subprocess.TimeoutExpired:
            signal_spin_group(process, signal.SIGKILL)
            return process.communicate(timeout=5)[0]
    finally:
        signal_spin_group(process, signal.SIGKILL)


def p5_origin(handler, target, second_port, releases):
    parts = target.strip("/").split("/")
    status = 200
    fields = [
        ("Date", "Tue, 27 Oct 2026 00:00:00 GMT"),
        ("Connection", "close"),
        ("Content-Type", "application/octet-stream"),
        ("X-P5-Fixture", "spin-wire"),
    ]
    payload = b"final"
    pending = None
    if parts[1] == "body":
        coding, size = parts[2:]
        payload = b"a" * int(size)
        if coding == "gzip":
            payload = gzip.compress(payload, mtime=0)
        fields.append(("Content-Encoding", "x-p5-opaque" if coding == "passthrough" else coding))
        fields.append(("Transfer-Encoding", "chunked"))
        payload = f"{len(payload):X}\r\n".encode("ascii") + payload + b"\r\n0\r\n\r\n"
    elif parts[1] == "declared":
        coding = parts[2]
        fields.append(("Content-Encoding", "x-p5-opaque" if coding == "passthrough" else coding))
        fields.append(("Content-Length", "11"))
        payload = b"a" * 11
    elif parts[1] in ["cross", "limit", "to-body"]:
        status, payload = 302, b""
        if parts[1] == "cross":
            location = f"http://127.0.0.1:{second_port}/p5/final"
        elif parts[1] == "limit":
            location = f"/p5/redirect/{parts[2]}/{parts[3]}"
        else:
            location = f"/p5/body/{parts[2]}/{parts[3]}"
        fields.extend([("Location", location), ("Content-Length", "0")])
    elif parts[1] == "redirect":
        kind, case = parts[2:]
        status, payload = 302, b""
        fields.append(("Location", f"/p5/next/{case}"))
        if kind == "emptybr":
            fields.extend([("Content-Encoding", "br"), ("Content-Length", "0")])
        elif kind == "oversized":
            fields.append(("Content-Length", str(10 * 1024 * 1024)))
            pending = releases[case]
        elif kind in ["pending", "error"]:
            fields.append(("Transfer-Encoding", "chunked"))
            if kind == "pending":
                pending = releases[case]
            else:
                payload = b"3\r\nbad\r\n"  # Deliberately missing the terminal chunk.
        elif kind == "clean":
            payload = b"found"
            fields.append(("Content-Length", str(len(payload))))
        else:
            raise AssertionError(f"unknown P5 body kind: {kind}")
    elif parts[1] in ["next", "final"]:
        if parts[1] == "next":
            releases[parts[2]].set()
        fields.append(("Content-Length", str(len(payload))))
    else:
        raise AssertionError(f"unknown P5 origin route: {target}")
    head = f"HTTP/1.1 {status} Fixture\r\n" + "".join(f"{name}: {value}\r\n" for name, value in fields)
    handler.wfile.write(head.encode("ascii") + b"\r\n" + payload)
    handler.wfile.flush()
    if pending is not None and not pending.wait(timeout=6):
        raise TimeoutError(f"P5 body gate was not released: {target}")


def check_p5(spin_port, origin_port, second_port, records, releases):
    base = f"http://127.0.0.1:{origin_port}"
    exchanges = 0
    dispatches = 0

    def exchange(path, mode, streamed=False, encoded=False, expected=None, case=None):
        nonlocal exchanges, dispatches
        assert records.empty(), "P5 request escaped its preceding exchange"
        if case is not None:
            releases[case] = threading.Event()
        connection = http.client.HTTPConnection("127.0.0.1", spin_port, timeout=12)
        try:
            connection.request("GET", "/p5", headers={
                "x-origin": base + path,
                "x-p5-mode": mode,
                "x-p5-stream": str(streamed).lower(),
                "x-p5-encoded-cap": str(encoded).lower(),
            })
            response = connection.getresponse()
            body = response.read()
            assert response.status == 200, f"P5 {mode} {path}: HTTP {response.status}: {body!r}"
            report = json.loads(body)
        finally:
            connection.close()
            if case is not None:
                releases[case].set()
        captured = []
        while not records.empty():
            captured.append(records.get_nowait())
        wanted = expected if expected is not None else [(origin_port, path)]
        assert captured == wanted, f"P5 {mode} {path}: {captured!r} != {wanted!r}"
        exchanges += 1
        dispatches += len(captured)
        return report

    def assert_error(report, kind, reason):
        error = report["outcome"]["error"]
        assert error["kind"] == kind, report
        assert error["reason"] == reason, report
        assert error["status"] == (504 if kind == "gateway_timeout" else 502), report

    for streamed in [False, True]:
        for coding in ["identity", "gzip", "passthrough"]:
            for size in [10, 11]:
                path = f"/p5/body/{coding}/{size}"
                encoded = streamed and coding == "passthrough"
                sent = exchange(path, "send", streamed, encoded)
                followed = exchange(path, "follow", streamed, encoded)
                assert sent["outcome"] == followed["outcome"], (sent, followed)
                assert followed["policies"] == 0 and followed["hop"] == 0, followed
                if size == 10:
                    outcome = followed["outcome"]
                    assert outcome["status"] == 200 and outcome["bytes"] == list(b"a" * 10), followed
                    assert followed["streamed"] == streamed, followed
                    assert followed["final_uri"] == base + path, followed
                    assert followed["redirects"] == 0 and followed["stop"] is None, followed
                    fields = {name: bytes(value) for name, value in outcome["headers"]}
                    assert fields["x-p5-fixture"] == b"spin-wire", fields
                    if coding == "gzip":
                        assert "content-encoding" not in fields and "content-length" not in fields, fields
                    else:
                        assert fields["content-encoding"] == (b"identity" if coding == "identity" else b"x-p5-opaque"), fields
                else:
                    reason = "EncodedBody" if encoded else "DecodedBody" if streamed else "BufferedBody"
                    assert_error(followed, "response_too_large", reason)
            # Content-Length is encoded length; this cap precedes gzip decoding.
            path = f"/p5/declared/{coding}"
            sent = exchange(path, "send", streamed, True)
            followed = exchange(path, "follow", streamed, True)
            assert sent["outcome"] == followed["outcome"], (sent, followed)
            assert_error(followed, "response_too_large", "EncodedBody")
    print("PASS: P5 Send/Follow no-redirect identity/gzip/passthrough buffered/streamed 10/11 and declared caps", flush=True)

    for kind in ["pending", "error", "emptybr", "oversized"]:
        results = {}
        for mode in ["follow", "send", "stop", "limit"]:
            case = f"{kind}-{mode}"
            redirect = f"/p5/redirect/{kind}/{case}"
            path = f"/p5/limit/{kind}/{case}" if mode == "limit" else redirect
            expected = [(origin_port, path)]
            if mode == "follow":
                expected.append((origin_port, f"/p5/next/{case}"))
            elif mode == "limit":
                expected.append((origin_port, redirect))
            report = exchange(path, mode, expected=expected, case=case)
            if mode == "follow":
                assert report["outcome"]["status"] == 200, report
                assert report["outcome"]["bytes"] == list(b"final"), report
                assert report["redirects"] == 1 and report["policies"] == 1, report
                assert report["stop"] is None and report["final_uri"] == base + f"/p5/next/{case}", report
            else:
                results[mode] = report["outcome"]
                assert report["hop"] == (1 if mode == "limit" else 0), report
                assert report["policies"] == (0 if mode == "send" else 1), report
                if kind == "pending":
                    assert_error(report, "gateway_timeout", "PerCallTimeout")
                elif kind == "error":
                    assert_error(report, "bad_gateway", "Protocol")
                    assert "ResponseBodyRead" in report["outcome"]["error"]["provider"], report
                elif kind == "emptybr":
                    assert_error(report, "bad_gateway", "Decode(Brotli)")
                else:
                    assert_error(report, "response_too_large", "BufferedBody")
        assert results["send"] == results["stop"] == results["limit"], results
        print(f"PASS: P5 {kind} intermediate bypass and Send/stop/limit ordinary final error", flush=True)

    for mode, reason in [("stop", "StoppedByPolicy"), ("limit", "LimitReached")]:
        case = f"clean-{mode}"
        redirect = f"/p5/redirect/clean/{case}"
        path = f"/p5/limit/clean/{case}" if mode == "limit" else redirect
        expected = [(origin_port, path)] + ([(origin_port, redirect)] if mode == "limit" else [])
        report = exchange(path, mode, expected=expected, case=case)
        assert report["outcome"]["status"] == 302 and report["outcome"]["bytes"] == list(b"found"), report
        assert report["stop"] == reason and report["redirects"] == (1 if mode == "limit" else 0), report

    sent = exchange("/p5/cross", "send")
    followed = exchange("/p5/cross", "follow", expected=[
        (origin_port, "/p5/cross"), (second_port, "/p5/final"),
    ])
    assert sent["outcome"]["status"] == 302, sent
    assert followed["outcome"]["status"] == 200 and followed["outcome"]["bytes"] == list(b"final"), followed
    assert followed["redirects"] == 1 and followed["final_uri"] == f"http://127.0.0.1:{second_port}/p5/final", followed
    print("PASS: P5 two-origin plain 302 versus followed 200 and final URI", flush=True)

    for coding in ["identity", "gzip", "passthrough"]:
        for size in [10, 11]:
            final = f"/p5/body/{coding}/{size}"
            path = f"/p5/to-body/{coding}/{size}"
            encoded = coding == "passthrough"
            sent = exchange(final, "send", True, encoded)
            followed = exchange(path, "follow", True, encoded, expected=[
                (origin_port, path), (origin_port, final),
            ])
            assert sent["outcome"] == followed["outcome"], (sent, followed)
            assert followed["streamed"] and followed["redirects"] == 1, followed
            assert followed["policies"] == 1 and followed["final_uri"] == base + final, followed
    assert exchanges == 68, exchanges
    print(f"PASS: P5 streamed final-response parity; {exchanges} guest exchanges, {dispatches} real provider dispatches", flush=True)


def main():
    records = queue.Queue()
    p5_records = queue.Queue()
    p5_failures = queue.Queue()
    p5_releases = {}
    bidder_started = {name: threading.Event() for name in ["A", "B", "C"]}
    release_bidders = threading.Event()

    class Origin(socketserver.StreamRequestHandler):
        def handle(self):
            self.connection.settimeout(5)

            def line(limit):
                value = self.rfile.readline(limit + 1)
                if len(value) > limit or not value.endswith(b"\r\n"):
                    raise ValueError("origin fixture line exceeded its cap or ended early")
                return value

            method, target, _version = line(4096).decode("ascii").strip().split(" ")
            raw_headers = bytearray()
            while True:
                value = line(min(4096, 16384 - len(raw_headers)))
                raw_headers.extend(value)
                if value == b"\r\n":
                    break
            headers = http.client.parse_headers(io.BytesIO(raw_headers))
            if target.startswith("/p5/"):
                assert method == "GET" and not headers.get("transfer-encoding"), headers
                assert headers.get("content-length") in [None, "0"], headers
                p5_records.put((self.server.server_address[1], target))
                try:
                    p5_origin(self, target, second.server_address[1], p5_releases)
                except Exception as error:
                    p5_failures.put(repr(error))
                    raise
                return
            if target.startswith("/handoff-"):
                if method != "GET":
                    raise AssertionError("handoff transport changed the request method")
                if target == "/handoff-origin":
                    for started in bidder_started.values():
                        if not started.wait(timeout=5):
                            raise TimeoutError("bidder did not start before the origin head")
                    payload = b"origin"
                elif target == "/handoff-release":
                    release_bidders.set()
                    payload = b""
                elif target.startswith("/handoff-bid-"):
                    bidder = target.removeprefix("/handoff-bid-")
                    bidder_started[bidder].set()
                    if not release_bidders.wait(timeout=5):
                        raise TimeoutError("response body did not release the pending bidders")
                    payload = f"bid-{bidder}".encode("ascii")
                else:
                    raise AssertionError("unpolled retained Send performed provider work")
                self.wfile.write(
                    b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: "
                    + str(len(payload)).encode("ascii") + b"\r\n\r\n" + payload
                )
                return
            encoded = bytearray()
            payload = bytearray()
            transfer = headers.get("transfer-encoding", "").lower()
            if transfer and transfer != "chunked":
                raise ValueError("unsupported fixture transfer encoding")
            if transfer == "chunked":
                while True:
                    chunk_line = line(min(128, 4096 - len(encoded)))
                    encoded.extend(chunk_line)
                    count = int(chunk_line.split(b";", 1)[0], 16)
                    if not 0 <= count <= 1024 - len(payload):
                        raise ValueError("origin fixture payload exceeded 1024 bytes")
                    if len(encoded) + count + 2 > 4096:
                        raise ValueError("origin fixture framing exceeded 4096 bytes")
                    if not count:
                        ending = line(2)
                        if ending != b"\r\n":
                            raise ValueError("unexpected fixture trailers")
                        encoded.extend(ending)
                        break
                    chunk = self.rfile.read(count + 2)
                    if len(chunk) != count + 2 or not chunk.endswith(b"\r\n"):
                        raise ValueError("origin fixture chunk ended early")
                    payload.extend(chunk[:-2])
                    encoded.extend(chunk)
            elif headers.get("content-length"):
                count = int(headers["content-length"])
                if not 0 <= count <= 1024:
                    raise ValueError("origin fixture payload exceeded 1024 bytes")
                payload.extend(self.rfile.read(count))
                if len(payload) != count:
                    raise ValueError("origin fixture payload ended early")
                encoded.extend(payload)
            self.connection.settimeout(0.05)
            try:
                extra = self.rfile.read1(1)
            except (TimeoutError, socket.timeout):
                extra = b""
            self.connection.settimeout(5)
            records.put({
                "method": method,
                "target": target,
                "content_length": headers.get_all("content-length", []),
                "transfer_encoding": headers.get_all("transfer-encoding", []),
                "encoded_body_hex": bytes(encoded + extra).hex(),
                "payload": payload.decode("ascii"),
            })
            self.wfile.write(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")

    class OriginServer(socketserver.ThreadingTCPServer):
        daemon_threads = False
        block_on_close = True

    with OriginServer(("127.0.0.1", 0), Origin) as origin, OriginServer(("127.0.0.1", 0), Origin) as second:
        state = tempfile.TemporaryDirectory(prefix="edgezero-spin-wire-")
        listeners = []
        process = None
        failed = True
        try:
            for index, server in enumerate([origin, second]):
                listener = threading.Thread(
                    target=server.serve_forever, kwargs={"poll_interval": 0.1},
                    name=f"spin-wire-origin-{index}", daemon=False,
                )
                listener.start()
                listeners.append((server, listener))
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                spin_port = reservation.getsockname()[1]
            manifest = Path(__file__).resolve().parents[1] / "examples/outbound-bodyless-wire.spin.toml"
            process = subprocess.Popen([
                "spin", "up", "--from", str(manifest), "--listen", f"127.0.0.1:{spin_port}",
                "--state-dir", state.name, "--log-dir", str(Path(state.name) / "logs"), "--disable-cache",
            ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + 60
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"local Spin exited with status {process.returncode}")
                try:
                    with socket.create_connection(("127.0.0.1", spin_port), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("local Spin listener did not start")
                    time.sleep(0.05)
            failures = []
            for case in ["get", "head", "get-settled", "head-settled", "post"]:
                connection = http.client.HTTPConnection("127.0.0.1", spin_port, timeout=15)
                connection.request("GET", f"/{case}", headers={
                    "x-origin": f"http://127.0.0.1:{origin.server_address[1]}/{case}",
                })
                response = connection.getresponse()
                response.read()
                if response.status != 200:
                    raise AssertionError(f"{case}: Spin returned {response.status}")
                connection.close()
                captured = records.get(timeout=5)
                print(json.dumps(captured), flush=True)
                expected_method = "POST" if case == "post" else "HEAD" if case.startswith("head") else "GET"
                assert captured["method"] == expected_method
                assert captured["target"] == f"/{case}"
                if case == "post":
                    assert captured["payload"] == "upload-control"
                elif captured["content_length"] or captured["transfer_encoding"] or captured["encoded_body_hex"]:
                    failures.append(case)
            assert not failures, f"bodyless requests carried upstream framing or bytes: {failures}"
            print("PASS: 4 bodyless wire cases and 1 upload control", flush=True)
            connection = http.client.HTTPConnection("127.0.0.1", spin_port, timeout=15)
            connection.request("GET", "/scope")
            response = connection.getresponse()
            response.read()
            assert response.status == 200, f"canonical scope checks returned {response.status}"
            connection.close()
            print("PASS: canonical dispatch and assembly retained-handle cleanup", flush=True)
            connection = http.client.HTTPConnection("127.0.0.1", spin_port, timeout=15)
            connection.request("GET", "/handoff", headers={
                "x-origin": f"http://127.0.0.1:{origin.server_address[1]}",
            })
            response = connection.getresponse()
            page = response.read()
            assert response.status == 200, f"attached response returned {response.status}"
            assert page[:6] == b"origin", f"begun origin was lost: {page!r}"
            assert len(page) == 21, f"handoff lost or duplicated bidder bytes: {page!r}"
            assert {page[index:index + 5] for index in range(6, 21, 5)} == {
                b"bid-A", b"bid-B", b"bid-C",
            }, f"handoff lost bidder results: {page!r}"
            connection.close()
            marker = "PASS: Spin attached Begin/Batch handoff closed at SDK terminal"
            deadline = time.monotonic() + 10
            while True:
                component_logs = "\n".join(
                    path.read_text(errors="replace")
                    for path in (Path(state.name) / "logs").rglob("*") if path.is_file()
                )
                if marker in component_logs:
                    break
                if "FAIL: Spin attached" in component_logs or time.monotonic() >= deadline:
                    raise AssertionError(f"SDK-terminal ownership proof missing:\n{component_logs}")
                time.sleep(0.01)
            print(marker, flush=True)
            check_p5(spin_port, origin.server_address[1], second.server_address[1], p5_records, p5_releases)
            failed = False
        finally:
            release_bidders.set()
            for started in bidder_started.values():
                started.set()
            for release in p5_releases.values():
                release.set()
            try:
                if process is not None:
                    output = stop_spin(process)
                    if failed or process.returncode not in [0, -signal.SIGTERM]:
                        print(output.decode(errors="replace"), flush=True)
                        if not failed:
                            raise RuntimeError(f"local Spin exited unexpectedly: {process.returncode}")
            finally:
                for server, listener in listeners:
                    server.shutdown()
                    listener.join(timeout=1)
                state.cleanup()

    # Server context exit joins request handlers, including late failures after a reply.
    assert p5_failures.empty(), list(p5_failures.queue)
    assert p5_records.empty(), list(p5_records.queue)


if __name__ == "__main__":
    main()

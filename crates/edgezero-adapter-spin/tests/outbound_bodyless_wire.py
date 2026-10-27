"""Test-only wire repro; no alternate outbound transport.

From the worktree root (Rust 1.98.1, Spin 4.1.0, spin-sdk 7.0.0, Python 3.9.6):
  cargo build --locked -p edgezero-adapter-spin --no-default-features --features spin,test-utils --target wasm32-wasip2 --example outbound-bodyless-wire
  python3 crates/edgezero-adapter-spin/tests/outbound_bodyless_wire.py
"""

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


def main():
    records = queue.Queue()
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
        daemon_threads = True
        block_on_close = False

    with OriginServer(("127.0.0.1", 0), Origin) as origin:
        thread = threading.Thread(target=origin.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
        thread.start()
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            spin_port = reservation.getsockname()[1]
        manifest = Path(__file__).resolve().parents[1] / "examples/outbound-bodyless-wire.spin.toml"
        state = tempfile.TemporaryDirectory(prefix="edgezero-spin-wire-")
        process = None
        failed = True
        try:
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
            failed = False
        finally:
            try:
                if process is not None:
                    output = stop_spin(process)
                    if failed or process.returncode not in [0, -signal.SIGTERM]:
                        print(output.decode(errors="replace"), flush=True)
            finally:
                origin.shutdown()
                thread.join(timeout=1)
                state.cleanup()


if __name__ == "__main__":
    main()

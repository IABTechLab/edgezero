import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("geo_load", Path(__file__).with_name("load.py"))
loader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(loader)

FAKE_SPIN = '''#!/usr/bin/env python3
import http.server, json, os, sys, time
from pathlib import Path
Path(os.environ["GEO_TEST_PID"]).write_text(str(os.getpid()))
mode = os.environ["GEO_TEST_MODE"]
if mode == "readiness":
    time.sleep(60)
listen = sys.argv[sys.argv.index("--listen") + 1]
class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        if mode == "publication": time.sleep(60)
        if mode == "drip":
            self.send_response(200); self.send_header("Content-Length", "24"); self.end_headers()
            for _ in range(24):
                self.wfile.write(b" "); self.wfile.flush(); time.sleep(0.1)
            return
        expected = os.environ["SPIN_VARIABLE_LOADER_TOKEN"]
        if self.headers.get("x-edgezero-loader-token") != expected:
            self.send_response(401); self.end_headers(); return
        payload = json.dumps({"objects":1,"written":1,"identical":0,"pin":"test"}).encode()
        self.send_response(200); self.send_header("Content-Length", str(len(payload)))
        self.end_headers(); self.wfile.write(payload)
    def log_message(self, *_): pass
http.server.HTTPServer(("127.0.0.1", int(listen.split(":")[1])), Handler).serve_forever()
'''

class RunnerTests(unittest.TestCase):
    def run_case(self, mode):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            fake = root / "spin"
            fake.write_text(FAKE_SPIN)
            fake.chmod(0o700)
            wasm = root / "loader.wasm"
            wasm.write_bytes(b"test")
            artifact = root / "artifact"
            artifact.mkdir()
            config = root / "runtime.toml"
            config.write_text(f'[key_value_store.geo]\ntype="spin"\npath="{root}/db"\n')
            pid = root / "pid"
            old = {key: os.environ.get(key) for key in ["GEO_TEST_MODE", "GEO_TEST_PID"]}
            os.environ.update(GEO_TEST_MODE=mode, GEO_TEST_PID=str(pid))
            spawned = []
            original_popen = loader.subprocess.Popen
            def start(*args, **kwargs):
                child = original_popen(*args, **kwargs)
                spawned.append(child)
                return child
            try:
                with patch.object(loader.subprocess, "Popen", side_effect=start):
                    self.check_result(mode, fake, wasm, artifact, config)
                self.assertEqual(len(spawned), 1)
                self.assertIsNotNone(spawned[0].returncode, "runner must reap its child")
                with self.assertRaises(ProcessLookupError):
                    os.kill(spawned[0].pid, 0)
            finally:
                for key, value in old.items():
                    if value is None: os.environ.pop(key, None)
                    else: os.environ[key] = value

    def check_result(self, mode, fake, wasm, artifact, config):
                if mode in ("success", "relative"):
                    self.assertEqual(loader.publish(os.path.relpath(fake) if mode == "relative" else str(fake), wasm, artifact, config, "geo", 2, 2)["objects"], 1)
                else:
                    with self.assertRaises((TimeoutError, OSError)):
                        loader.publish(str(fake), wasm, artifact, config, "geo", 0.4 if mode == "readiness" else 2, 0.4)
    def test_relative_executable_survives_temporary_cwd(self): self.run_case("relative")
    def test_success_stops_child(self): self.run_case("success")
    def test_readiness_timeout_stops_child(self): self.run_case("readiness")
    def test_publication_timeout_stops_child(self): self.run_case("publication")
    def test_total_deadline_stops_a_dripping_response(self): self.run_case("drip")

if __name__ == "__main__": unittest.main()

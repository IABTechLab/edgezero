#!/usr/bin/env python3
"""Run a separate, authenticated loopback publisher and always stop its runtime."""
import argparse
import json
import math
import os
from pathlib import Path
import secrets
import signal
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import tomllib
import urllib.request


def publish(spin, wasm, artifact, runtime_config, store, readiness_timeout=30, publication_timeout=600):
    executable = shutil.which(spin)
    if executable is None:
        raise ValueError("Spin executable not found")
    spin = str(Path(executable).resolve())
    wasm, artifact, runtime_config = (Path(value).resolve() for value in (wasm, artifact, runtime_config))
    if not wasm.is_file() or not artifact.is_dir():
        raise ValueError("build the loader and supply an artifact directory")
    if not all(math.isfinite(value) and value > 0 for value in (readiness_timeout, publication_timeout)):
        raise ValueError("deadlines must be finite and positive")
    config = tomllib.loads(runtime_config.read_text())
    binding = config.get("key_value_store", {}).get(store)
    if not binding:
        raise ValueError("runtime configuration must explicitly define the selected store")
    if binding.get("type") == "spin" and not Path(binding.get("path", "")).is_absolute():
        raise ValueError("local Spin storage requires an absolute database path")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    token = secrets.token_hex(32)
    with tempfile.TemporaryDirectory(prefix="geo-spin-loader-") as raw:
        root = Path(raw)
        # Spin only maps files within its application directory. Stage an owned copy.
        shutil.copytree(artifact, root / "artifact")
        manifest = root / "spin.toml"
        quote = json.dumps
        manifest.write_text(f'''spin_manifest_version = 2
[application]
name = "geo-spin-loader"
version = "0.0.0"
[variables]
loader_token = {{ required = true, secret = true }}
loader_store = {{ required = true }}
[[trigger.http]]
route = "/..."
component = "loader"
[component.loader]
source = {quote(str(wasm))}
files = [{{ source = "artifact", destination = "/artifact" }}]
key_value_stores = [{quote(store)}]
[component.loader.variables]
loader_token = "{{{{ loader_token }}}}"
loader_store = "{{{{ loader_store }}}}"
''')
        env = dict(os.environ, SPIN_VARIABLE_LOADER_TOKEN=token, SPIN_VARIABLE_LOADER_STORE=store)
        process = None
        timer = None
        expired = threading.Event()
        with (root / "runtime.log").open("wb") as log:
            try:
                process = subprocess.Popen(
                    [spin, "up", "--from", str(manifest), "--listen", f"127.0.0.1:{port}",
                     "--runtime-config-file", str(runtime_config)],
                    cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT,
                )
                ready_by = time.monotonic() + readiness_timeout
                while True:
                    if process.poll() is not None:
                        raise OSError("loader runtime exited before readiness")
                    if time.monotonic() >= ready_by:
                        raise TimeoutError("loader readiness deadline exceeded")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                            break
                    except OSError:
                        time.sleep(0.025)
                def expire():
                    expired.set()
                    if process.poll() is None:
                        try:
                            process.kill()
                        except ProcessLookupError:
                            pass
                timer = threading.Timer(publication_timeout, expire)
                timer.daemon = True
                timer.start()
                request = urllib.request.Request(
                    f"http://127.0.0.1:{port}/publish", data=b"", method="POST",
                    headers={"x-edgezero-loader-token": token},
                )
                # The secret stays in process memory/environment, never argv or a log.
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                with opener.open(request, timeout=publication_timeout) as response:
                    payload = response.read(4097)
                    if len(payload) > 4096:
                        raise OSError("invalid publication report")
                    if expired.is_set():
                        raise TimeoutError("publication deadline exceeded")
                    return json.loads(payload)
            except Exception:
                if expired.is_set():
                    raise TimeoutError("publication deadline exceeded") from None
                raise
            finally:
                if timer is not None:
                    timer.cancel()
                    timer.join()
                if process is not None:
                    try:
                        process.terminate()
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spin", default="spin")
    parser.add_argument("--wasm", type=Path, default=Path(__file__).parent / "target/wasm32-wasip2/release/geo_spin_loader.wasm")
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--runtime-config", type=Path, required=True)
    parser.add_argument("--store", required=True)
    parser.add_argument("--readiness-timeout", type=float, default=30)
    parser.add_argument("--publication-timeout", type=float, default=600)
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        report = publish(args.spin, args.wasm, args.artifact, args.runtime_config, args.store,
                         args.readiness_timeout, args.publication_timeout)
        print(json.dumps(report, sort_keys=True))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except TimeoutError:
        parser.exit(1, "loader readiness or publication deadline exceeded\n")
    except urllib.error.HTTPError as error:
        category = "authentication rejected" if error.code == 401 else "artifact publication or verification rejected"
        parser.exit(1, f"loader: {category}\n")
    except ValueError:
        parser.exit(1, "invalid loader configuration or publication report\n")
    except OSError:
        parser.exit(1, "loader runtime startup or transport failed\n")
    except Exception:
        parser.exit(1, "dataset publication failed\n")


if __name__ == "__main__":
    main()

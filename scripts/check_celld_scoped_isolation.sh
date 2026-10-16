#!/usr/bin/env bash
# Explicit disposable namespace proof. No host network or production credentials.
set -euo pipefail
if [[ $# != 1 || ! -x $1 || $1 != /* ]]; then
  printf 'usage: %s /absolute/path/to/modified/celld\n' "$0" >&2
  exit 2
fi
for tool in unshare ip nsenter setpriv mount curl python3; do
  command -v "$tool" >/dev/null || { printf 'missing isolation prerequisite: %s\n' "$tool" >&2; exit 1; }
done
# Always create a private user/network/mount/PID world before any ip/mount work.
exec unshare --user --map-root-user --net --mount --pid --fork --mount-proc \
  env -i PATH="$PATH" bash -s -- "$1" <<'ISOLATED'
set -euo pipefail
binary=$1
root=$(mktemp -d /tmp/edgezero-celld-isolation.XXXXXX)
server_pid=
app_wrapper=
app_pid=
cleanup() {
  for pid in "$server_pid" "$app_wrapper" "$app_pid"; do
    if [[ -n $pid ]]; then kill -KILL "$pid" 2>/dev/null || true; fi
  done
  wait 2>/dev/null || true
  rm -rf -- "$root"
}
trap cleanup EXIT
mkdir -p "$root/service" "$root/control"
printf 'fixture-fleet-credential-not-a-real-secret\n' > "$root/service/fleet-credential"
ip link set lo up
ip link add scoped-server type veth peer name scoped-app
ip addr add 10.203.0.1/24 dev scoped-server
ip link set scoped-server up
cat > "$root/service/policy.json" <<'POLICY'
{"version":1,"listen":"10.203.0.1:7780","allow_remote_trusted_network":true,"stores":[{"alias":"state","namespace":"isolated-state"},{"alias":"config","namespace":"isolated-config"}],"access":{"mode":"trusted-network","grants":[{"store":"state","operations":["read","write","delete","list"]},{"store":"config","operations":["read"]}]}}
POLICY
env -i CELLD_TEST_DATA_DIR="$root/service/data" "$binary" \
  --listen 127.0.0.1:0 --internal-listen 127.0.0.1:0 \
  --store-api-config "$root/service/policy.json" > "$root/service/server.log" 2>&1 &
server_pid=$!
ready=false
for attempt in $(seq 1 100); do
  status=$(curl --noproxy '*' --max-time 1 -s -o /dev/null -w '%{http_code}' \
    'http://10.203.0.1:7780/v1/stores/state/value?key=probe' || true)
  if [[ $status == 404 ]]; then ready=true; break; fi
  kill -0 "$server_pid" 2>/dev/null || { printf 'celld startup failed\n' >&2; tail -40 "$root/service/server.log" >&2; exit 1; }
  sleep 0.1
done
[[ $ready == true ]] || { printf 'scoped listener startup deadline\n' >&2; exit 1; }
internal_port=$(python3 - "$root/service/server.log" <<'PY'
import re,sys
text=open(sys.argv[1]).read()
m=re.search(r'celld internal listening on 127\.0\.0\.1:(\d+)',text)
assert m, 'internal listener address absent'
print(m.group(1))
PY
)
# Separate app network, mounts and process visibility. Hide the entire service
# tree before dropping capabilities in the actual app probe, not just chmod it.
unshare --net --mount --pid --fork --mount-proc bash -c '
  set -e
  mount --make-rprivate /
  mount -t tmpfs -o size=1m,mode=000 none "$1/service"
  printf ready > "$1/control/app.ready"
  exec sleep 120
' bash "$root" &
app_wrapper=$!
for attempt in $(seq 1 100); do
  if [[ -f $root/control/app.ready ]]; then
    read -r app_pid < "/proc/$app_wrapper/task/$app_wrapper/children" || true
    if [[ -n $app_pid ]]; then break; fi
  fi
  sleep 0.05
done
[[ -n $app_pid ]] || { printf 'application namespace startup failed\n' >&2; exit 1; }
ip link set scoped-app netns "$app_pid"
nsenter --target "$app_pid" --net ip addr add 10.203.0.2/24 dev scoped-app
nsenter --target "$app_pid" --net ip link set scoped-app up
nsenter --target "$app_pid" --net ip link set lo up
nsenter --target "$app_pid" --net --mount --pid \
  setpriv --bounding-set=-all --inh-caps=-all --ambient-caps=-all \
  env -i PATH="$PATH" bash -s -- "$root" "$internal_port" <<'APP'
set -euo pipefail
root=$1
internal=$2
base=http://10.203.0.1:7780
response="$root/control/app-response"
request() { curl --disable --noproxy '*' --max-time 3 -s -o "$response" -w '%{http_code}' "$@"; }
[[ $(request -X PUT --data-binary 'isolated-app-value' "$base/v1/stores/state/value?key=probe") == 204 ]]
[[ $(request "$base/v1/stores/state/value?key=probe") == 200 ]]
[[ $(< "$response") == isolated-app-value ]]
[[ $(request -X PUT --data-binary forbidden "$base/v1/stores/config/value?key=probe") == 403 ]]
[[ $(request "$base/v1/stores/undeclared/value?key=probe") == 403 ]]
[[ $(request "$base/shutdown") == 404 ]]
if curl --noproxy '*' --max-time 1 -sf "http://10.203.0.1:$internal/state" >/dev/null; then
  printf 'application reached the internal listener by server address\n' >&2; exit 1
fi
if curl --noproxy '*' --max-time 1 -sf "http://127.0.0.1:$internal/state" >/dev/null; then
  printf 'application reached the server loopback listener\n' >&2; exit 1
fi
[[ ! -r $root/service/fleet-credential ]]
[[ ! -r $root/service/policy.json ]]
for comm in /proc/[0-9]*/comm; do
  if [[ $(< "$comm") == celld ]]; then
    printf 'application can inspect the server process through procfs\n' >&2; exit 1
  fi
done
rm -f -- "$response"
printf 'Scoped API reachable; admin listener, service files and server procfs isolated.\n'
APP
ISOLATED

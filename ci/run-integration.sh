#!/usr/bin/env bash
# Copyright 2026 Query Farm LLC - https://query.farm
#
# Run this repo's sqllogictest suite (test/sql/*.test) against the vgi-fhir
# VGI worker, using a prebuilt standalone `haybarn-unittest` and the signed
# community `vgi` extension — no C++ build from source. See ci/README.md.
#
# Multi-transport: the same suite runs over whichever transport the TRANSPORT
# env var selects, by changing what `VGI_FHIR_WORKER` resolves to (the vgi
# extension picks the transport from the ATTACH LOCATION string):
#
#   subprocess (default)  VGI_FHIR_WORKER = the stdio worker binary
#                         -> extension spawns it over stdin/stdout.
#   http                  start `<worker> --http` (prints "PORT:<n>"), parse the
#                         port, VGI_FHIR_WORKER = http://127.0.0.1:<port>.
#                         (The extension POSTs each RPC method at <LOCATION>/<method>,
#                         e.g. /catalog_attach; the SDK mounts them at the root.)
#   unix                  start `<worker> --unix /tmp/fhir.sock` (prints
#                         "UNIX:<path>"), VGI_FHIR_WORKER = unix:///tmp/fhir.sock.
#
# In every transport the fhir worker's table functions still query a FHIR R4
# server, so the suite ALWAYS needs the mock FHIR server: this script builds the
# repo's `mockserver`, starts it on a free port with a known bearer token, and
# points the tests at it via VGI_FHIR_TEST_URL + VGI_FHIR_TEST_TOKEN (mirroring
# `make test-sql`). No live external FHIR server is contacted. All started
# processes are trap-killed on exit.
#
# Required environment:
#   HAYBARN_UNITTEST   path to the haybarn-unittest binary
#   VGI_FHIR_WORKER    for TRANSPORT=subprocess: the worker LOCATION the .test
#                      files ATTACH (the built Go worker binary, spawned over
#                      stdio). For http/unix this is OVERRIDDEN by this script,
#                      but the binary it points at is reused to launch the
#                      out-of-band server, so it must still be the worker path.
# Optional:
#   TRANSPORT          subprocess (default) | http | unix
#   STAGE              scratch dir for the preprocessed test tree (default: mktemp)
set -euo pipefail

: "${HAYBARN_UNITTEST:?path to the haybarn-unittest binary}"
: "${VGI_FHIR_WORKER:?worker LOCATION (the built Go worker binary)}"

TRANSPORT="${TRANSPORT:-subprocess}"
case "$TRANSPORT" in
  subprocess|http|unix) ;;
  *) echo "ERROR: unknown TRANSPORT='$TRANSPORT' (expected subprocess|http|unix)" >&2; exit 2 ;;
esac

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
STAGE="${STAGE:-$(mktemp -d)}"

# The bearer token the mock FHIR server requires (must match
# internal/mockfhir.Token, and the Makefile's MOCK_TOKEN).
MOCK_TOKEN="test-token-123"

# The worker binary the subprocess transport ATTACHes to is also the binary we
# launch out-of-band for http/unix. Capture it before we possibly overwrite
# VGI_FHIR_WORKER with a URL.
WORKER_BIN="$VGI_FHIR_WORKER"

# Collected PIDs and paths to clean up on exit (mock + optional worker server).
MOCK_PID=""
WORKER_PID=""
UNIX_SOCK=""
cleanup() {
  # Preserve the script's exit status: this runs on EXIT, so its own last
  # command must not clobber the real exit code (a bare `[ -n "$x" ]` that is
  # false returns 1 and would turn a green run red).
  local rc=$?
  if [ -n "$WORKER_PID" ]; then kill "$WORKER_PID" 2>/dev/null || true; wait "$WORKER_PID" 2>/dev/null || true; fi
  if [ -n "$MOCK_PID" ]; then kill "$MOCK_PID" 2>/dev/null || true; wait "$MOCK_PID" 2>/dev/null || true; fi
  if [ -n "$UNIX_SOCK" ]; then rm -f "$UNIX_SOCK"; fi
  return "$rc"
}
trap cleanup EXIT

# --- Start the mock FHIR server (the .test files fetch from it; all transports) -
# Build + launch the repo's standalone mock server on a free port with a known
# bearer token; it prints "PORT:<n>" on stdout (see cmd/mockserver/main.go). We
# capture that, export VGI_FHIR_TEST_URL + VGI_FHIR_TEST_TOKEN. The mock is
# required for every transport — the worker still makes the FHIR HTTP calls.
MOCK_BIN="$STAGE/mockserver"
echo "Building mock FHIR server ..."
( cd "$REPO" && go build -o "$MOCK_BIN" ./cmd/mockserver )

MOCK_PORT_FILE="$(mktemp)"
"$MOCK_BIN" --addr 127.0.0.1:0 --token "$MOCK_TOKEN" >"$MOCK_PORT_FILE" 2>/dev/null &
MOCK_PID=$!

PORT=""
for _ in $(seq 1 30); do
  PORT="$(sed -n 's/^PORT:\([0-9][0-9]*\)$/\1/p' "$MOCK_PORT_FILE" 2>/dev/null | head -1)"
  [ -n "$PORT" ] && break
  sleep 0.2
done
if [ -z "$PORT" ]; then
  echo "ERROR: mock server did not report a port" >&2
  exit 1
fi
rm -f "$MOCK_PORT_FILE"
export VGI_FHIR_TEST_URL="http://127.0.0.1:$PORT"
export VGI_FHIR_TEST_TOKEN="$MOCK_TOKEN"
echo "Mock FHIR server listening on $VGI_FHIR_TEST_URL (pid $MOCK_PID)"

# --- Per-transport: resolve VGI_FHIR_WORKER (the ATTACH LOCATION) ------------
# subprocess keeps the binary path (extension spawns stdio). http/unix start the
# worker out-of-band and hand the extension a URL.
case "$TRANSPORT" in
  subprocess)
    echo "Transport: subprocess/stdio — VGI_FHIR_WORKER=$VGI_FHIR_WORKER"
    ;;

  http)
    # Start the worker in --http mode; it prints "PORT:<n>" once listening.
    WORKER_PORT_FILE="$(mktemp)"
    echo "Transport: http — starting '$WORKER_BIN --http' ..."
    "$WORKER_BIN" --http >"$WORKER_PORT_FILE" 2>/dev/null &
    WORKER_PID=$!
    WPORT=""
    for _ in $(seq 1 50); do
      WPORT="$(sed -n 's/^PORT:\([0-9][0-9]*\)$/\1/p' "$WORKER_PORT_FILE" 2>/dev/null | head -1)"
      [ -n "$WPORT" ] && break
      # Bail early if the worker died.
      kill -0 "$WORKER_PID" 2>/dev/null || { echo "ERROR: http worker exited before reporting a port" >&2; cat "$WORKER_PORT_FILE" >&2 || true; exit 1; }
      sleep 0.2
    done
    rm -f "$WORKER_PORT_FILE"
    if [ -z "$WPORT" ]; then
      echo "ERROR: http worker did not report a port" >&2
      exit 1
    fi
    # The extension treats the LOCATION as a base and POSTs each RPC method at
    # <LOCATION>/<method> (e.g. /catalog_attach). The SDK mounts those methods
    # at the server root (empty prefix), so the LOCATION must be the bare
    # scheme://host:port with NO path. Appending /vgi would make every method
    # 404 — which the runner silently skips as an error "matching 'HTTP'".
    export VGI_FHIR_WORKER="http://127.0.0.1:$WPORT"
    echo "HTTP worker listening on $VGI_FHIR_WORKER (pid $WORKER_PID)"
    ;;

  unix)
    # Start the worker on an AF_UNIX socket; it prints "UNIX:<path>" once
    # listening. idleTimeout is disabled (we own the process lifecycle).
    UNIX_SOCK="${TMPDIR:-/tmp}/fhir.$$.sock"
    rm -f "$UNIX_SOCK"
    WORKER_OUT_FILE="$(mktemp)"
    echo "Transport: unix — starting '$WORKER_BIN --unix $UNIX_SOCK' ..."
    "$WORKER_BIN" --unix "$UNIX_SOCK" >"$WORKER_OUT_FILE" 2>/dev/null &
    WORKER_PID=$!
    READY=""
    for _ in $(seq 1 50); do
      if grep -q '^UNIX:' "$WORKER_OUT_FILE" 2>/dev/null && [ -S "$UNIX_SOCK" ]; then
        READY=1; break
      fi
      kill -0 "$WORKER_PID" 2>/dev/null || { echo "ERROR: unix worker exited before the socket was ready" >&2; cat "$WORKER_OUT_FILE" >&2 || true; exit 1; }
      sleep 0.2
    done
    rm -f "$WORKER_OUT_FILE"
    if [ -z "$READY" ]; then
      echo "ERROR: unix worker did not report a ready socket at $UNIX_SOCK" >&2
      exit 1
    fi
    export VGI_FHIR_WORKER="unix://$UNIX_SOCK"
    echo "Unix worker listening on $VGI_FHIR_WORKER (pid $WORKER_PID)"
    ;;
esac

# --- Stage the preprocessed tests -------------------------------------------
# The FULL suite runs over every transport, including http. The fhir table
# functions (fhir_patients / fhir_search / fhir_observations / fhir_capabilities
# / fhir_read) work over the stateless HTTP transport because each function's
# state carries an explicit gob-encodable cursor (Rows + Offset) that the
# framework snapshots into the continuation token each tick — see the "WHY AN
# EXPLICIT CURSOR" comment in internal/fhirworker/functions.go. No tests are
# gated.
echo "Staging preprocessed tests into $STAGE ..."
mkdir -p "$STAGE/test/sql"
for f in "$REPO"/test/sql/*.test; do
  awk -f "$HERE/preprocess-require.awk" "$f" > "$STAGE/test/sql/$(basename "$f")"
done

# The HTTP transport needs DuckDB's HTTP client, which the vgi extension drives
# through DuckDB's HTTPUtil — that is only registered when the `httpfs`
# extension is loaded. The .test files only `LOAD vgi`, so over HTTP the
# worker-RPC POSTs fail with an "HTTP"-flavoured error (which the runner then
# silently skips). Inject an explicit signed `INSTALL httpfs FROM core; LOAD
# httpfs;` after each `LOAD vgi;` in the staged tests for the http transport
# only (subprocess/unix do not use the HTTP client, so they need nothing extra).
if [ "$TRANSPORT" = "http" ]; then
  echo "Transport http: injecting 'LOAD httpfs' (required for the worker HTTP RPC) ..."
  for f in "$STAGE"/test/sql/*.test; do
    awk '
      { print }
      /^LOAD[ \t]+vgi;[ \t]*$/ {
        print "";
        print "statement ok";
        print "INSTALL httpfs FROM core;";
        print "";
        print "statement ok";
        print "LOAD httpfs;";
      }
    ' "$f" > "$f.tmp" && mv "$f.tmp" "$f"
  done
fi

cd "$STAGE"

# Warm the extension cache once: vgi from the signed community channel. A miss
# here is only a warning — the per-test LOAD vgi; (the .test files load it
# explicitly) is what actually gates each file, and it needs vgi already
# INSTALLed into the runner's extension dir.
echo "Warming the extension cache (vgi from community) ..."
mkdir -p "$STAGE/test"
cat > "$STAGE/test/_warm.test" <<'EOF'
# name: test/_warm.test
# group: [warm]
statement ok
INSTALL vgi FROM community;
EOF
"$HAYBARN_UNITTEST" "test/_warm.test" >/dev/null 2>&1 || echo "::warning::extension warm step did not fully succeed"
rm -f "$STAGE/test/_warm.test"

# Run the whole suite in one invocation, capturing the runner's native
# sqllogictest report so we can both stream it AND guard against a silent skip.
#
# IMPORTANT: the DuckDB/Haybarn sqllogictest runner SKIPS (not fails, exit 0) a
# test whose error message matches a built-in network-error allowlist that
# includes the substring "HTTP". So a broken HTTP transport would otherwise show
# "All tests were skipped" and the job would go GREEN having run nothing — a
# fake pass. We detect that and fail explicitly. A real run prints
# "All tests passed (N assertions ...)".
echo "Running suite (transport: $TRANSPORT, worker: $VGI_FHIR_WORKER) ..."
RUN_LOG="$STAGE/run.log"
set +e
"$HAYBARN_UNITTEST" "test/sql/*" 2>&1 | tee "$RUN_LOG"
RUN_RC="${PIPESTATUS[0]}"
set -e

if [ "$RUN_RC" -ne 0 ]; then
  echo "ERROR: suite failed (transport: $TRANSPORT, rc=$RUN_RC)" >&2
  exit "$RUN_RC"
fi

# Guard against the silent-skip fake-pass (see comment above). If every test was
# skipped — and none ran — treat it as a failure for this transport, surfacing
# the skip reason the runner reported.
if grep -q 'All tests were skipped' "$RUN_LOG"; then
  echo "ERROR: every test was SKIPPED on transport '$TRANSPORT' (the runner's" >&2
  echo "       built-in network-error skip swallowed the real error). This is" >&2
  echo "       NOT a pass. Skip reason reported by the runner:" >&2
  grep -A3 'Skipped tests for the following reasons' "$RUN_LOG" >&2 || true
  exit 1
fi

#!/usr/bin/env bash
# Copyright 2026 Query Farm LLC - https://query.farm
#
# Run this repo's sqllogictest suite (test/sql/*.test) against the vgi-fhir
# VGI worker, using a prebuilt standalone `haybarn-unittest` and the signed
# community `vgi` extension — no C++ build from source. See ci/README.md.
#
# The fhir worker queries FHIR R4 REST servers, so the suite needs a FHIR
# server: this script builds the repo's `mockserver`, starts it on a free port
# with a known bearer token, and points the tests at it via VGI_FHIR_TEST_URL +
# VGI_FHIR_TEST_TOKEN (mirroring `make test-sql`). No live external FHIR server
# is contacted.
#
# Required environment:
#   HAYBARN_UNITTEST   path to the haybarn-unittest binary
#   VGI_FHIR_WORKER    worker LOCATION the .test files ATTACH (the built Go
#                      worker binary the vgi extension spawns over stdio)
# Optional:
#   STAGE              scratch dir for the preprocessed test tree (default: mktemp)
set -euo pipefail

: "${HAYBARN_UNITTEST:?path to the haybarn-unittest binary}"
: "${VGI_FHIR_WORKER:?worker LOCATION (the built Go worker binary)}"

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
STAGE="${STAGE:-$(mktemp -d)}"

# The bearer token the mock FHIR server requires (must match
# internal/mockfhir.Token, and the Makefile's MOCK_TOKEN).
MOCK_TOKEN="test-token-123"

# --- Start the mock FHIR server (the .test files fetch from it) -------------
# Build + launch the repo's standalone mock server on a free port; it prints
# "PORT:<n>" on stdout (see cmd/mockserver/main.go). We capture that, export
# VGI_FHIR_TEST_URL + VGI_FHIR_TEST_TOKEN, and kill the server on exit —
# exactly like `make test-sql`.
MOCK_BIN="$STAGE/mockserver"
echo "Building mock FHIR server ..."
( cd "$REPO" && go build -o "$MOCK_BIN" ./cmd/mockserver )

MOCK_PORT_FILE="$(mktemp)"
"$MOCK_BIN" --addr 127.0.0.1:0 --token "$MOCK_TOKEN" >"$MOCK_PORT_FILE" 2>/dev/null &
MOCK_PID=$!
cleanup() {
  kill "$MOCK_PID" 2>/dev/null || true
  wait "$MOCK_PID" 2>/dev/null || true
  rm -f "$MOCK_PORT_FILE"
}
trap cleanup EXIT

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
export VGI_FHIR_TEST_URL="http://127.0.0.1:$PORT"
export VGI_FHIR_TEST_TOKEN="$MOCK_TOKEN"
echo "Mock FHIR server listening on $VGI_FHIR_TEST_URL (pid $MOCK_PID)"

# --- Stage the preprocessed tests -------------------------------------------
echo "Staging preprocessed tests into $STAGE ..."
mkdir -p "$STAGE/test/sql"
for f in "$REPO"/test/sql/*.test; do
  awk -f "$HERE/preprocess-require.awk" "$f" > "$STAGE/test/sql/$(basename "$f")"
done

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

# Run the whole suite in one invocation, streaming the runner's native
# sqllogictest report. Any failed assertion exits non-zero and fails the job.
echo "Running suite (worker: $VGI_FHIR_WORKER) ..."
"$HAYBARN_UNITTEST" "test/sql/*"

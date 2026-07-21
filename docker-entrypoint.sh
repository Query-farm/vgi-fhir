#!/bin/sh
# Copyright 2026 Query Farm LLC - https://query.farm
#
# Dispatch the single vgi-fhir image into one of its transports:
#   http   (default) the HTTP server on $PORT (8000), bound 0.0.0.0 so a
#                    published host port and the HEALTHCHECK reach it. Serves
#                    /health.
#   stdio            a worker DuckDB spawns over stdio (on-host execution).
#   unix             the AF_UNIX launcher transport on $UNIX_SOCK (a socket path
#                    that must be shared with the host, e.g. a mounted volume).
# Any other first argument is exec'd verbatim (escape hatch for debugging).
#
# The worker is stateless (each function queries a live FHIR server given as an
# argument), so there is no /data to create and no state env to wire — each mode
# just exec's the binary.
set -e

case "${1:-http}" in
  http)
    shift 2>/dev/null || true
    # The worker's --http mode binds the address passed via --http-addr (default
    # 127.0.0.1:0, an ephemeral loopback port). In a container we must bind
    # 0.0.0.0 on a FIXED port so `-p $PORT:$PORT` and the HEALTHCHECK reach it.
    exec vgi-fhir-worker --http --http-addr "0.0.0.0:${PORT:-8000}" "$@"
    ;;
  stdio)
    shift 2>/dev/null || true
    exec vgi-fhir-worker "$@"
    ;;
  unix)
    shift 2>/dev/null || true
    exec vgi-fhir-worker --unix "${UNIX_SOCK:-/tmp/vgi-fhir.sock}" "$@"
    ;;
  *)
    exec "$@"
    ;;
esac

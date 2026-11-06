# Copyright 2026 Query Farm LLC - https://query.farm
#
# Single image that serves the network transports of the `vgi-fhir` VGI worker:
#   docker run ... IMG            -> HTTP server on $PORT       (default; Fly.io / local)
#   docker run -i ... IMG stdio   -> stdio worker DuckDB spawns on-host
#   docker run ... IMG unix       -> AF_UNIX launcher transport on $UNIX_SOCK
# See docker-entrypoint.sh.
#
# This worker is STATELESS: it holds no local data — every function queries a
# live HL7 FHIR R4 REST server given as an argument. So there is no /data volume,
# no state env, and no `farm.query.vgi.volumes` mount-discovery label. The image
# is just the binary + a tiny entrypoint.
# syntax=docker/dockerfile:1

# ---- build stage -----------------------------------------------------------
# CGO is REQUIRED: the vgi-go SDK links the Haybarn 2.0 engine statically via the haybarn-go
# bindings (a CGO_ENABLED=0 build fails with "build constraints exclude all Go
# files" in haybarn-go-bindings), so gcc/g++/libc6-dev must be present. Pinned
# glibc (bookworm) matches the slim runtime below. Native per-arch runners in CI
# build each platform — no cross-compilation.
FROM golang:1.26-bookworm AS build
WORKDIR /src
ENV CGO_ENABLED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc g++ libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Fetch modules first (cached across source-only changes). The Go module cache
# and build cache are BuildKit cache mounts, so the binary is copied OUT to a
# non-cache path (/worker) before the layer ends (cache mounts don't persist).
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /worker ./cmd/vgi-fhir-worker

# ---- runtime stage ---------------------------------------------------------
# debian-slim (not distroless) so the HEALTHCHECK below has a real `curl`.
# ca-certificates: the worker calls live FHIR R4 endpoints over HTTPS.
FROM debian:bookworm-slim

# Build metadata, wired from docker/metadata-action outputs in CI.
ARG VERSION=0.0.0
ARG GIT_COMMIT=unknown
ARG SOURCE_URL=https://github.com/Query-farm/vgi-fhir

# Standard OCI labels + the VGI transport-advertisement label. `transports`
# lists the NETWORK transports this image serves (http); stdio is a spawn mode
# and unix is an on-host launcher socket, so neither is a network transport.
LABEL org.opencontainers.image.title="vgi-fhir" \
      org.opencontainers.image.description="Query HL7 FHIR R4 REST servers from DuckDB/SQL as a VGI worker (stdio + HTTP)" \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${GIT_COMMIT}" \
      org.opencontainers.image.licenses="MIT" \
      farm.query.vgi.transports='["http"]'

ENV PORT=8000 \
    # Build provenance only; the version the worker advertises over VGI comes
    # from the compiled binary, not this.
    VGI_FHIR_GIT_COMMIT=${GIT_COMMIT}

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# `--chmod` sets the mode in the COPY layer itself (a separate RUN chmod would
# rewrite the whole binary into a second layer).
COPY --from=build --chmod=0755 /worker /usr/local/bin/vgi-fhir-worker
COPY --chmod=0755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# Run unprivileged. No state, no volume — there is nothing to own or persist.
RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8000

# Readiness probe for HTTP mode. Inert for a short-lived stdio container, which
# has no HTTP server (the probe just fails harmlessly there).
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS "http://localhost:${PORT:-8000}/health" || exit 1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["http"]

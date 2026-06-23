# vgi-fhir Makefile
#
# A VGI worker (Go) that queries HL7 FHIR R4 REST servers and returns resources
# as DuckDB rows. Targets:
#
#   make build       Build the worker + mock-server binaries
#   make test-unit   Run the pure-Go unit/integration tests (httptest)
#   make test-sql    Run the haybarn-unittest SQL E2E against a local mock server
#   make test        test-unit + test-sql
#   make fmt         gofmt -w
#   make vet         go vet
#   make lint        golangci-lint (if installed) else vet
#   make clean       Remove built binaries
#
# test-sql needs haybarn-unittest on PATH:
#   uv tool install haybarn-unittest
#   export PATH="$$HOME/.local/bin:$$PATH"

WORKER_BIN  := vgi-fhir-worker
MOCK_BIN    := mockserver
WORKER_CMD  := ./cmd/vgi-fhir-worker
MOCK_CMD    := ./cmd/mockserver

TEST_DIR     := .
TEST_PATTERN := test/sql/*

# Absolute path to the built worker (the VGI extension launches it via LOCATION).
WORKER_PATH := $(CURDIR)/$(WORKER_BIN)
MOCK_PATH   := $(CURDIR)/$(MOCK_BIN)

# The bearer token the mock FHIR server requires (must match
# internal/mockfhir.Token).
MOCK_TOKEN := test-token-123

.PHONY: build test test-unit test-sql fmt vet lint clean

build:
	go build -o $(WORKER_BIN) $(WORKER_CMD)
	go build -o $(MOCK_BIN) $(MOCK_CMD)

test: test-unit test-sql

test-unit:
	go test ./...

# Build both binaries, start the mock FHIR server on a free port (it prints
# PORT:<n>), point the worker's fhir functions at it via VGI_FHIR_TEST_URL +
# VGI_FHIR_TEST_TOKEN, run the SQL suite, then stop the mock server.
test-sql: build
	@set -e; \
	TMP_PORT_FILE=$$(mktemp); \
	$(MOCK_PATH) --addr 127.0.0.1:0 --token "$(MOCK_TOKEN)" >$$TMP_PORT_FILE 2>/dev/null & \
	MOCK_PID=$$!; \
	trap 'kill $$MOCK_PID 2>/dev/null; wait $$MOCK_PID 2>/dev/null; rm -f $$TMP_PORT_FILE' EXIT; \
	PORT=""; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do \
		PORT=$$(sed -n 's/^PORT:\([0-9][0-9]*\)$$/\1/p' $$TMP_PORT_FILE 2>/dev/null | head -1); \
		[ -n "$$PORT" ] && break; \
		sleep 0.2; \
	done; \
	if [ -z "$$PORT" ]; then echo "ERROR: mock server did not report a port" >&2; exit 1; fi; \
	echo "mock FHIR server listening on 127.0.0.1:$$PORT (pid $$MOCK_PID)"; \
	set +e; \
	VGI_FHIR_WORKER="$(WORKER_PATH)" \
	VGI_FHIR_TEST_URL="http://127.0.0.1:$$PORT" \
	VGI_FHIR_TEST_TOKEN="$(MOCK_TOKEN)" \
		haybarn-unittest --test-dir "$(TEST_DIR)" "$(TEST_PATTERN)"; \
	STATUS=$$?; \
	exit $$STATUS

fmt:
	gofmt -w .

vet:
	go vet ./...

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not found; running go vet instead"; \
		go vet ./...; \
	fi

clean:
	rm -f $(WORKER_BIN) $(MOCK_BIN)

# CI: the vgi-fhir worker integration suite

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs the Go unit
tests and this repo's sqllogictest suite (`test/sql/*.test`) against the
vgi-fhir VGI worker through the **real DuckDB `vgi` extension** on every push /
PR.

## How it works (no C++ build)

Rather than building the vgi DuckDB extension from source, CI drives a
**prebuilt** standalone `haybarn-unittest` (the DuckDB/Haybarn sqllogictest
runner, published in Haybarn's releases) and installs the **signed** `vgi`
extension from the Haybarn community channel:

1. **Build the worker** — `go build -o vgi-fhir-worker ./cmd/vgi-fhir-worker`.
   The resulting binary is a self-contained stdio worker the extension can
   spawn; `VGI_FHIR_WORKER` (an absolute path) is the ATTACH `LOCATION`.
2. **Download the runner** — the `haybarn_unittest-linux-amd64.zip` asset from
   the latest Haybarn release.
3. **Preprocess** — [`preprocess-require.awk`](preprocess-require.awk) rewrites
   any `require <ext>` gate into an explicit signed `INSTALL <ext> FROM
   {community,core}; LOAD <ext>;`. This repo's tests already use an explicit
   `LOAD vgi;` (haybarn silently *skips* `require vgi`), so the awk is mostly a
   pass-through here; `require-env` and everything else pass through untouched.
4. **Run** — [`run-integration.sh`](run-integration.sh) builds + starts the
   repo's `mockserver` (an in-memory FHIR R4 server) on a free port with a known
   bearer token (exporting `VGI_FHIR_TEST_URL` + `VGI_FHIR_TEST_TOKEN`), stages
   the preprocessed tree, points `VGI_FHIR_WORKER` at the built worker binary,
   warms the extension cache once (`INSTALL vgi FROM community`), then runs the
   suite in a single `haybarn-unittest` invocation. No live external FHIR server
   is contacted. Any failed assertion exits non-zero and fails the job. The mock
   server is killed on exit.

## Run it locally

```bash
go build -o vgi-fhir-worker ./cmd/vgi-fhir-worker
# point HAYBARN_UNITTEST at a haybarn-unittest binary (or a local DuckDB
# `unittest` built with the vgi extension):
HAYBARN_UNITTEST=/path/to/haybarn-unittest \
VGI_FHIR_WORKER="$PWD/vgi-fhir-worker" \
  ci/run-integration.sh
```

Or use the Makefile target (`make test-sql`), which builds both binaries,
starts the mock FHIR server, and points the worker at `$(CURDIR)/vgi-fhir-worker`
with `haybarn-unittest` on `PATH`.

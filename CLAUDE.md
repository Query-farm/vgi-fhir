# CLAUDE.md — vgi-fhir

Contributor/agent notes. User-facing docs live in `README.md`; this is the
"how it's built and where the sharp edges are" companion. It follows the same
Go SDK conventions as the reference Go workers `vgi-grpc`, `vgi-cve`, `vgi-scim`.

## What this is

A [VGI](https://query.farm) worker (Go) that queries **HL7 FHIR R4** REST
servers over plain REST/JSON and returns resources as DuckDB rows. Built on the
[`vgi-go`](https://github.com/Query-farm/vgi-go) SDK over stdio. Catalog name:
`fhir`. FHIR is just REST/JSON, so transport is **stdlib only** (`net/http` +
`encoding/json`) — no FHIR SDK. Scope: **FHIR R4** (`4.0.1`).

## Layout

```
cmd/vgi-fhir-worker/main.go   stdio entry point; assembles the worker + catalog
cmd/mockserver/main.go        standalone mock FHIR server for the SQL E2E
internal/fhirworker/
  client.go                   HTTP + Bundle pagination (SearchStartURL/SearchPage streaming, SearchAll/ReadOne/GetMetadata); FetchOptions
  fhir.go                     resource shapes + flatten (Flatten*/FetchPatients/Observations/Capabilities/SearchResources)
  functions.go                the five VGI table functions + Register(w)
  *_test.go                   httptest unit/integration tests
internal/mockfhir/server.go   in-memory FHIR handler (shared by tests + mockserver)
test/sql/*.test               haybarn-unittest sqllogictest — authoritative E2E
Makefile                      build / test-unit / test-sql / lint
```

To add a function: implement the FHIR fetch in `fhir.go` (or `client.go`), wrap
it as a `vgi.TypedTableFunc` in `functions.go`, and register it in `Register(w)`.

## The Go SDK worker pattern (read first)

A worker is `main()` assembling a `*vgi.Worker` and registering functions:

```go
w := vgi.NewWorker(
    vgi.WithCatalogName("fhir"),
    vgi.WithCatalogComment("..."),
)
fhirworker.Register(w)   // w.RegisterTable(NewXxxFunction()) for each fn
w.RunStdio()             // or w.RunHttp("127.0.0.1:0") behind a --http flag
```

A **table function** is a `vgi.TypedTableFunc[S]` (generic over a *state* type)
wrapped with `vgi.AsTableFunction[S](impl)`. Required methods:

- `Name() string`
- `Metadata() vgi.FunctionMetadata`
- `ArgumentSpecs() []vgi.ArgSpec` — `vgi.DeriveArgSpecs(argsStruct{})`.
- `OnBind(*vgi.BindParams) (*vgi.BindResponse, error)` — `vgi.BindSchema(schema)`.
- `NewState(*vgi.ProcessParams) (*S, error)` — `vgi.BindArgs(params.Args, &args)`
  and validate. Do a **single** request's network I/O here at most; a *paginated*
  fetch belongs in `Process`, one page per tick (sharp edge 3).
- `Process(ctx, *vgi.ProcessParams, *S, *vgirpc.OutputCollector) error` —
  `out.Emit(batch)`, then `out.Finish()`.

**Argument struct tags** (`vgi.DeriveArgSpecs` / `vgi.BindArgs`):

```go
type searchArgs struct {
    BaseURL      string `vgi:"pos=0,name=base_url,doc=FHIR R4 service base URL"`
    ResourceType string `vgi:"pos=1,name=resource_type,doc=FHIR resource type"`
    Query        string `vgi:"default=,doc=Raw search query string"`   // NAMED optional
    Token        string `vgi:"default=,doc=OAuth bearer token"`         // NAMED optional
    Count        int64  `vgi:"default=50,doc=Page size (_count)"`       // NAMED optional
}
```

- `pos=N` → positional; `name=` sets the wire name.
- A field **without** `pos` but **with** `default=` becomes a **named optional**
  argument (DuckDB `name := value`). Auth here is *optional* (many FHIR servers
  are open), so `token` is a named option, not positional.
- Go type → Arrow type is inferred (`string`→varchar, `int64`→bigint,
  `bool`→bool).

Build arrays in `Process` with `vgi.BuildStringArray` / `vgi.BuildBooleanArray`,
then `array.NewRecordBatch(schema, []arrow.Array{...}, n)`.

## Sharp edges (learned the hard way)

1. **Table-function state is `gob`-encoded by the SDK** between `NewState` and
   `Process` (it may cross a process/worker boundary). So **`S` must have
   exported, gob-encodable fields only** — no `arrow.Record`, no interfaces, no
   channels/funcs, no unexported fields (the SDK panics at registration
   otherwise). Every function stores plain exported Go slices and **rebuilds the
   Arrow batch in `Process`**. The flattened structs in `fhir.go` are
   deliberately all-exported for this reason. Optional numeric fields are
   `*float64` so a missing value can round-trip through gob and surface as SQL
   NULL.

   Two state shapes: single-request functions (`fhir_read`, `fhir_capabilities`)
   fetch in `NewState` and embed `Cursor[T]{ Rows []T; Offset int }`. The
   search-backed ones (`fhir_search`, `fhir_patients`, `fhir_observations`) do
   **no I/O in `NewState`** and embed `PageCursor[T]` — see sharp edge 3.

   **Streaming state MUST carry an explicit cursor, not a bare `Done bool`**
   (the HTTP-continuation invariant). Over the **stateless HTTP transport** the
   worker keeps no live state between `Process` ticks — the framework
   round-trips the producer state through a continuation token (gob-snapshotting
   the user state each tick, emitting ≤1 data batch per response, resuming from
   the token). A `Done` flag flipped *after* the single `Emit` observes the
   pre-`Emit` snapshot on resume, re-emits the same rows forever, and pins the
   worker in an infinite loop (subprocess/unix hold live state in memory, so they
   never hit it). `fhir_patients` / `fhir_search` emit MANY rows (Bundle
   pagination), so this is mandatory. The fix: the embedded `Cursor[T]` whose
   `Process` emits a bounded slice from `Offset`, advances `Offset` **before**
   yielding, and `out.Finish()`es when `Offset >= len(Rows)`. The framework
   snapshots `Offset` into the token, so HTTP resumes correctly and terminates.
   `TestCursorSurvivesContinuation` (gob round-trips between ticks) guards this.

2. **`haybarn-unittest` silently SKIPS `require vgi`.** Under haybarn the
   extension is not autoloaded for `require`, so a `.test` using `require vgi`
   is skipped (looks green but ran nothing). Use an explicit `statement ok` /
   `LOAD vgi;` instead — every `.test` here does.

3. **Bundle pagination STREAMS, and must terminate.** A search-backed table
   function must never crawl the whole result set before its first batch: a VGI
   scan blocked inside its first batch **cannot be interrupted** (vgi-lint
   VGI911), so a slow first batch wedges every client that touches the relation.
   That is exactly what the original design did — `NewState` called `SearchAll`,
   which followed every `next` link up to `max_results` (1000, i.e. ~20 HTTP
   round-trips at the default `_count=50`) before `Process` emitted one row.

   The fix is `PageCursor[T]` in `functions.go`: `NewState` only seeds
   `NextURL` (via `SearchStartURL`), and `Process` fetches ONE page
   (`client.go`'s `SearchPage`), flattens it, and emits it. First-row latency is
   one round-trip regardless of result-set size, and `LIMIT` stops the scan after
   a single page. `Rows`/`Offset`/`NextURL`/`Fetched` are all exported so the
   HTTP-continuation token round-trips them (sharp edge 1), and they advance
   **before** the batch is yielded.

   **Termination:** stop when there is no `next` link, when `max_results` is
   reached, **or when a page comes back with no resources**. That last one is
   load-bearing, not defensive dressing — the public HAPI R4 server
   (`hapi.fhir.org/baseR4`) answers an *unfiltered* search with a Bundle that has
   a `next` link and zero entries, on every page, forever; trusting `next` alone
   is an unbounded crawl that only stops on the 30s context deadline.
   `TestStreamingStopsOnEmptyPage` and `TestStreamingFirstBatchIsOneRequest`
   guard both halves. The eager `SearchAll` is kept for tests/non-streaming
   callers and has the same empty-page break.

   Note: the `next` URL is followed *verbatim* (it carries the server's own
   cursor) — don't re-derive query params from it.

4. **Examples point at `https://r4.smarthealthit.org`, not `hapi.fhir.org`.**
   `fhirworker.ExampleBaseURL` / `ExamplePatientID`. The public HAPI server is a
   shared scratchpad: anyone can POST to it, resources get deleted (the old
   `fhir_read` example's `Patient/example` now answers HTTP 404), and its
   unfiltered searches return nothing (above). The SMART Health IT R4 sandbox
   serves a synthetic Synthea dataset frozen since 2021, so pinned ids keep
   resolving and unfiltered searches return rows on the first page. Both are
   listed in the `fhir_servers` view. If you add an example, run
   `vgi-lint lint ./vgi-fhir-worker --execute --fail-on info` — it executes every
   example against the live endpoint.

5. **Nullable DOUBLE for `value`.** `vgi.BuildFloat64Array` can't emit NULLs, so
   `fhir_observations` builds its `value` column by hand with an
   `array.Float64Builder` that calls `AppendNull()` when `Observation.Value`
   (a `*float64`) is nil — a non-numeric Observation (no `valueQuantity`)
   surfaces a NULL, not 0.

6. **`VARCHAR[]` for `fhir_capabilities.interactions`.** The list column is a
   `arrow.ListOf(String)`; it's built with an `array.ListBuilder` whose
   `ValueBuilder()` appends the interaction codes per row
   (`buildStringListArray`). DuckDB `UNNEST(interactions)` then expands it.

7. **Errors are surfaced, never panics.** Non-2xx HTTP → a clean error; a FHIR
   `OperationOutcome` body is parsed into its severity/diagnostics
   (`describeError`); malformed JSON → a decode error. Every request is timeout
   bounded (`defaultTimeout`). The SQL E2E asserts `statement error` for a wrong
   token (401) and a missing resource (404).

## Mock-FHIR E2E (how `make test-sql` works)

Mirrors `vgi-scim`/`vgi-grpc`'s start/stop pattern:

1. `make build` compiles `vgi-fhir-worker` **and** `mockserver`.
2. `mockserver --addr 127.0.0.1:0 --token <tok>` binds a free port and prints
   `PORT:<n>`; the Makefile captures it. It serves the same in-memory FHIR
   dataset as the unit tests via `internal/mockfhir.NewHandler`, and **caps
   Patient pages at 2** so the E2E exercises real Bundle `next`-link pagination
   over the 3-patient seed set.
3. The Makefile exports `VGI_FHIR_WORKER` (the worker binary, used as the ATTACH
   `LOCATION`), `VGI_FHIR_TEST_URL` (`http://127.0.0.1:<n>`), and
   `VGI_FHIR_TEST_TOKEN` (the bearer token), all read by the `.test` files.
4. `haybarn-unittest --keep-home --test-dir . "test/sql/*"` runs the suite; the haybarn exit
   status is captured and returned, and a shell `trap` kills the mock server.

`cmd/mockserver` and the Go tests share `internal/mockfhir.NewHandler`, which
serves `/Patient` and `/Observation` (searchset Bundles, Patient paginated with
a `next` link), `/Patient/{id}` (single resource), and `/metadata`
(CapabilityStatement). The bearer token is required only when configured.

## Test inventory

- **Go (`make test-unit`)** — `internal/fhirworker/fhir_test.go` +
  `functions_test.go` spin up the FHIR server in-process via `httptest.Server`
  and assert: `fhir_search` follows `next` links across pages, `max_results`
  caps, `fhir_read` reads one (and 404 → error), Patient/Observation flatten
  (numeric value parsed, non-numeric → nil/NULL), capabilities list, bearer
  enforced (missing → error / correct → ok), HTTP 500 + OperationOutcome →
  error, malformed JSON → error, missing base_url → error, plus the VGI
  `NewState` data paths and NULL→no-rows.
- **SQL (`make test-sql`)** — `test/sql/fhir_patients.test` (patient count
  across pages, a family name, flattened fields, observation numeric value +
  NULL, `UNNEST(interactions)` from capabilities, wrong-token error) and
  `test/sql/fhir_search.test` (generic search across pages, read one, 404
  error).

## Conventions

- Source files start with `// Copyright 2026 Query Farm LLC - https://query.farm`.
- `gofmt`, `go vet`, and `go test ./...` must be clean before committing.

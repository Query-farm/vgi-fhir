<p align="center">
  <img src="https://raw.githubusercontent.com/Query-farm/vgi/main/docs/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

<p align="center"><em>A <a href="https://query.farm">Query.Farm</a> VGI worker for DuckDB.</em></p>

# vgi-fhir

[![CI](https://github.com/Query-farm/vgi-fhir/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-fhir/actions/workflows/ci.yml)

A [VGI](https://query.farm) worker, written in **Go**, that queries
**HL7 FHIR** REST servers and returns resources to DuckDB/SQL as rows. FHIR is
plain REST/JSON, so the worker uses only the Go standard library
(`net/http` + `encoding/json`) for transport; no FHIR SDK is required.
Multi-page searchset `Bundle`s are followed automatically via their
`link[].relation == "next"` links, up to a bounded `max_results`.

Built on the [`vgi-go`](https://github.com/Query-farm/vgi-go) SDK; it speaks the
VGI protocol over stdio. Catalog name: `fhir`. Targets **FHIR R4** (the common
version, `4.0.1`).

```sql
INSTALL vgi FROM community; LOAD vgi;

-- LOCATION is the path to the compiled worker binary.
ATTACH 'fhir' AS fhir (TYPE vgi, LOCATION '/path/to/vgi-fhir-worker');

-- All patients (core fields flattened, full JSON in `raw`).
SELECT id, family, given, gender, birth_date, active
FROM fhir.fhir_patients('https://hapi.fhir.org/baseR4');

-- All observations (valueQuantity flattened; non-numeric values are NULL).
SELECT code, code_display, value, unit, subject
FROM fhir.fhir_observations('https://hapi.fhir.org/baseR4');

-- Generic search of any resource type (id + full resource JSON).
SELECT id, resource
FROM fhir.fhir_search('https://hapi.fhir.org/baseR4', 'Condition');

-- A raw search query string + a custom page size.
SELECT id
FROM fhir.fhir_search(
  'https://hapi.fhir.org/baseR4', 'Patient',
  query := 'family=smith&gender=female',
  count := 50);

-- One resource by id.
SELECT resource
FROM fhir.fhir_read('https://hapi.fhir.org/baseR4', 'Patient', '12345');

-- What the server supports.
SELECT resource_type, interactions
FROM fhir.fhir_capabilities('https://hapi.fhir.org/baseR4');

-- A bearer token for protected servers (named option on every function).
SELECT id FROM fhir.fhir_patients('https://server/fhir', token := 'BEARER_TOKEN');
```

## Functions

All five are **table functions** (each makes one or more FHIR HTTP requests and
returns rows). The first argument is always `base_url` (the FHIR R4 service
base).

| Function | Returns | Description |
| --- | --- | --- |
| `fhir_search(base_url, resource_type)` | `id, resource VARCHAR` | One row per resource from a search of `{base}/{resourceType}`; `resource` is the full JSON. Follows `next` links up to `max_results` (1000). |
| `fhir_read(base_url, resource_type, id)` | `resource VARCHAR` | A single resource read from `{base}/{resourceType}/{id}` (one row). |
| `fhir_patients(base_url)` | `id, family, given, gender, birth_date VARCHAR, active BOOLEAN, raw VARCHAR` | All Patients, core fields flattened; `raw` is the full Patient JSON. |
| `fhir_observations(base_url)` | `id, status, code, code_display VARCHAR, value DOUBLE, unit, effective, subject, raw VARCHAR` | All Observations; `valueQuantity` flattened (non-numeric → NULL `value`); `raw` is the full JSON. |
| `fhir_capabilities(base_url)` | `resource_type VARCHAR, interactions VARCHAR[]` | The resource types and REST interactions from the server's CapabilityStatement (`{base}/metadata`). |

### Named options

These optional named arguments use DuckDB `name := value` syntax:

| Option | Applies to | Default | Meaning |
| --- | --- | --- | --- |
| `token` | all | _none_ | OAuth bearer token, sent as `Authorization: Bearer <token>`. Omit for open servers. |
| `query` | `fhir_search`, `fhir_patients`, `fhir_observations` | _none_ | Raw FHIR search query string (e.g. `family=smith&_count=50`), appended to the search URL verbatim. |
| `count` | `fhir_search`, `fhir_patients`, `fhir_observations` | `50` | Page size, sent as the `_count` search parameter. All pages are still followed (up to `max_results = 1000`). |

### Behaviour

- **Pagination** — searchset `Bundle`s are followed page by page via their
  `link[].relation == "next"` links, collecting up to `max_results` (1000)
  resources, then stopping.
- **Auth** — when a `token` is supplied it is sent as `Authorization: Bearer
  <token>`; otherwise no auth header is sent (many public FHIR servers are open).
- **Robustness** — non-2xx HTTP responses, FHIR `OperationOutcome` error bodies,
  and malformed JSON all become clear DuckDB errors with a short excerpt /
  diagnostics; the worker never crashes or hangs (every request is timeout
  bounded).
- **Flattening** — `fhir_patients`/`fhir_observations` lift the common fields;
  everything else stays available in `raw`. Observations without a numeric
  `valueQuantity` surface a NULL `value`.

## Building

```sh
make build        # builds vgi-fhir-worker + mockserver
make test-unit    # Go unit/integration tests (in-process httptest FHIR server)
make test-sql     # haybarn SQL end-to-end against a local mock FHIR server
make test         # both
```

## Licensing

This worker is licensed **MIT** (see [`LICENSE`](LICENSE)).

Dependencies: the Go **standard library** only for FHIR transport
(`net/http`, `encoding/json`) plus the VGI SDK stack —
[`vgi-go`](https://github.com/Query-farm/vgi-go),
[`vgi-rpc-go`](https://github.com/Query-farm/vgi-rpc-go), and
[`arrow-go`](https://github.com/apache/arrow-go) (Apache-2.0). No third-party
FHIR client library is used. Scope is **FHIR R4** (`4.0.1`); other versions are
not specifically supported.

---

## Authorship & License

Written by [Query.Farm](https://query.farm) — every VGI worker is designed and built by Query.Farm.

Copyright 2026 Query Farm LLC - https://query.farm


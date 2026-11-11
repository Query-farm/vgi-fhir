<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

<p align="center"><em>A <a href="https://query.farm">Query.Farm</a> VGI worker for DuckDB.</em></p>

# Query HL7 FHIR R4 Resources as Tables in DuckDB

> **vgi-fhir** · a [Query.Farm](https://query.farm) VGI worker

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

ATTACH 'fhir' AS fhir (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-fhir@v0.3.0');

-- Patients (core fields flattened, full JSON in `raw`). Every search-backed
-- function STREAMS: it fetches one Bundle page per batch, so `LIMIT` stops the
-- scan after a single HTTP round-trip instead of crawling every page first.
SELECT id, family, given, gender, birth_date, active
FROM fhir.fhir_patients('https://r4.smarthealthit.org');

-- All observations (valueQuantity flattened; non-numeric values are NULL).
SELECT code, code_display, value, unit, subject
FROM fhir.fhir_observations('https://r4.smarthealthit.org');

-- Generic search of any resource type (id + full resource JSON).
SELECT id, resource
FROM fhir.fhir_search('https://r4.smarthealthit.org', 'Condition');

-- A raw search query string + a custom page size.
SELECT id
FROM fhir.fhir_search(
  'https://r4.smarthealthit.org', 'Patient',
  query := 'family=smith&gender=female',
  count := 50);

-- One resource by id.
SELECT resource
FROM fhir.fhir_read('https://r4.smarthealthit.org', 'Patient',
                    '45a69bf0-7116-4f9f-a1d9-29700439b4a4');

-- What the server supports.
SELECT resource_type, interactions
FROM fhir.fhir_capabilities('https://r4.smarthealthit.org');

-- A bearer token for protected servers (named option on every function).
SELECT id FROM fhir.fhir_patients('https://server/fhir', token := 'BEARER_TOKEN');
```

## Quick start

Every [GitHub release](https://github.com/Query-farm/vgi-fhir/releases) ships a prebuilt worker binary. The `vgi` extension downloads the one for your platform, checks its SHA-256 and caches it, so there is nothing to install besides DuckDB:

```sql
INSTALL vgi FROM community;
LOAD vgi;

ATTACH 'fhir' AS fhir (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-fhir@v0.3.0');

-- List the curated public FHIR R4 test servers and whether each needs authentication.
SELECT name, base_url, requires_auth FROM fhir.main.fhir_servers ORDER BY name;

-- List Patients with their core demographic fields flattened into columns.
SELECT id, family, given, gender, birth_date FROM fhir.main.fhir_patients('https://r4.smarthealthit.org') LIMIT 20;
```

Every function fetches from the FHIR server URL you pass. Many public test servers are open; for a protected server pass an OAuth bearer token with `token := '…'` (see [Named options](#named-options) and [Behaviour](#behaviour)).

Prebuilt binaries cover macOS (Apple silicon), Linux (x86-64 and arm64) and Windows (x86-64). `github-auto://` needs an explicit release tag; change `@v0.3.0` to move to a newer release.

### Other ways to run it

- **Docker** (the image bundles the worker; needs a container runtime):

  ```sql
  ATTACH 'fhir' AS fhir (TYPE vgi, LOCATION 'docker://ghcr.io/query-farm/vgi-fhir:0.3.0');
  ```

- **Manual download.** Download the archive for your platform from the [releases page](https://github.com/Query-farm/vgi-fhir/releases), verify it against its `.sha256` file (a cosign `.cosign.bundle` is published alongside), unpack it, and set `LOCATION` to the path of the extracted worker executable.

### Build from source

To build the worker yourself, see [Building](#building).

## Functions

All five are **table functions** (each makes one or more FHIR HTTP requests and
returns rows). The first argument is always `base_url` (the FHIR R4 service
base).

| Function | Returns | Description |
| --- | --- | --- |
| `fhir_search(base_url, resource_type)` | `id, resource VARCHAR` | One row per resource from a search of `{base}/{resourceType}`; `resource` is the full JSON. Streams page by page, following `next` links up to `max_results` (1000) or until a page comes back empty. |
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

Written by [Query.Farm](https://query.farm).

Copyright 2026 Query Farm LLC - https://query.farm


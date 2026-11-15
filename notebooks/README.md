# FHIR examples

Two independent, local-only notebooks use authored synthetic resources:

- [Complete pages still need reference and quantity checks](01_complete_pages_do_not_make_quantities_comparable.ipynb) preserves the existing pagination story: reconcile every identifier, retain unresolved references and normalize only a narrowly supported quantity subset.
- [Read the resource behind a NULL](02_read_the_resource_behind_a_null.ipynb) compares generic search with flattened Observation rows, inspects the static endpoint directory without contacting it, checks advertised interactions and independently reads one selected resource. Numeric zero and a text-valued resource remain different states.

## Run locally or in Colab

Use either notebook’s Colab badge and Run All. The short bootstrap installs the released base shared package and retrieves three immutable, SHA-verified companions. For a local copy of this directory, install [uv](https://docs.astral.sh/uv/) and run:

```bash
uv sync --locked
uv run --locked vgi-notebooks 01_complete_pages_do_not_make_quantities_comparable.ipynb
uv run --locked vgi-notebooks 02_read_the_resource_behind_a_null.ipynb
```

The local environment uses the shared `[runner]` extra and Haybarn 2.0.0rc11. Colab installs only the base package, preserving preloaded kernel/Jupyter libraries. An optional editor kernel:

```bash
uv run --locked python -m ipykernel install --user --name vgi-fhir-notebooks --display-name 'Python (VGI FHIR examples)'
```

Visible SQL attaches the published native worker:

```sql
ATTACH 'fhir' (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-fhir@v0.4.0');
```

VGI downloads the platform release and verifies its published checksum. Release v0.4.0 peels to **ced609df56514c8a53a9a2929bd709d68e87feae**. First use needs public package, companion and release access; it does not require Go, a worker checkout, a clinical system, patient records or credentials. Upgrade pins deliberately and repeat the examples. SQL connections use fresh temporary empty secret directories with persistence disabled and UTC configured at connection creation.

## Separate experiments

```bash
uv run --locked vgi-notebooks 01_complete_pages_do_not_make_quantities_comparable.ipynb \
  --parameters '{"PAGE_SIZE":4}' --output-dir artifacts/page-4
uv run --locked vgi-notebooks 02_read_the_resource_behind_a_null.ipynb \
  --parameters '{"SELECTED_ID":"o04"}' --output-dir artifacts/text-resource
```

The first changes page size 2→4 while keeping all 16 resource IDs, policy categories, normalized values and analysis fingerprint fixed; search requests fall from eight to four, plus one metadata request in either run. The second switches o03→o04, changing the independent read from a deliberately numeric zero to NULL plus retained text. All twelve source rows, both complete searches, the shape matrix and the eight-request count stay fixed; only the selected resource/highlight changes.

The runner protects saved defaults, requires a separate experiment destination and rejects unknown parameters. Charts use native SVG output plus content-addressed GitHub SVG links. Ignored artifacts hold experiments, review PNGs and logs.

## Fixture scope and interpretation

`synthetic_resources.json` contains four authored Patient resources and twelve authored Observations. It is unchanged from the original notebook. These are not patient records or clinically validated measurements. `fixture_server.py` implements only a bounded, read-only FHIR subset on an ephemeral 127.0.0.1 port: metadata, Patient/Observation search with page sizes two/four and opaque next cursors, and single-resource reads. It rejects unsupported queries, routes, credentials and writes, records every response and never forwards requests. The actual unchanged worker performs HTTP, follows pages and flattens resources. Metadata advertises only implemented read/search interactions.

Completeness here is checked against the known finite inventory and exact request trace. The worker caps searches at 1,000 resources and stops on empty pages; page size or SQL LIMIT cannot prove completeness on an external server. The second notebook’s 25 deliveries are 24 resources delivered by two searches plus one independent read, representing only twelve distinct observations. Equality across search/read reflects this immutable fixture; it does not guarantee transactional consistency on a changing server. The static `fhir_servers` view contains published discovery metadata, not fresh endpoint availability, access authorization or content verification. None of its public endpoints is contacted.

The first notebook resolves only exact local `Patient/<id>` references. Missing-local and external absolute references remain unresolved; it never strips a base URL and assumes identity. Every authored Observation has one coding. The technical inclusion policy selects LOINC system/code 29463-7, final status, a numeric comparator-free quantity and UCUM system/code kg or g, converting grams by dividing by 1,000. Other codes/statuses, absent quantities, comparators and unsupported units/systems remain in an ordered reason table. This is neither a general FHIR/profile validator nor a clinical acceptance policy.

A deliberately zero-valued quantity stays zero; an absent numeric quantity stays NULL while alternative text remains in raw JSON. The shape matrix tests presence/matching fields, not validity or target-reference resolution. Synthetic UTC timestamps are authored fixture fields, not acquisition times. Neither notebook calculates clinical averages, diagnostic thresholds, normality or health conclusions. Authentication, histories, writes, arbitrary filters, multiple codings and other value choices are outside the fixture.

Official references: [FHIR R4 Observation](https://hl7.org/fhir/R4/observation.html), [references](https://hl7.org/fhir/R4/references.html), [search](https://hl7.org/fhir/R4/search.html), [read interaction](https://hl7.org/fhir/R4/http.html#read) and [CapabilityStatement](https://hl7.org/fhir/R4/capabilitystatement.html).

## Coverage and validation

The actual complete release catalog has **six logical surfaces**: five table functions plus the `fhir_servers` view, with no physical tables or COPY handlers. Both notebooks together meaningfully cover **6/6**. Full native arguments/result schemas and the view’s five columns are retained in [coverage.json](coverage.json) and its evidence. Current public source **90ec83458a72adbf02947b9ca206db9bbf6d899e** changes only metadata/examples/tests relative to v0.4.0; registrations, argument/output schemas, view and runtime behavior are unchanged. Coverage is **released exact 6/6, current source-compatible 6/6, exact current runtime 0/6**. No newer binary is built or substituted. Heading title/subtitle come from actual catalog metadata.

Each notebook includes ten SQL invariants plus complete ID/JSON/request reconciliation. Defaults and separate experiments execute the published native release. A fresh notebook-only Python 3.12 kernel preloads eight plotting/Jupyter libraries, installs only the base shared package, retrieves empty-cache companions and verifies unchanged library versions plus `pip check`. These are macOS ARM and Colab-compatible setup checks; the hosted Colab UI and other operating systems are not manually operated. The native release cache was already warm. All default/experiment charts are reviewed and published SVG bytes checked.

Worker code and root dependencies remain unchanged. Existing [source CI43568346131](https://github.com/Query-farm/vgi-fhir/actions/runs/43568346131) has green build/vet/fmt/unit and all three SQL transport jobs, but its metadata job fails with **VGI911 on fhir_capabilities** (first-row latency/hanging diagnostic; 98/100, L1). This pre-existing failure is preserved separately; the actual release returns fixture capabilities promptly in both notebooks. The prior [complete source CI43553728313](https://github.com/Query-farm/vgi-fhir/actions/runs/43553728313) passed. Notebook-only work does not rebuild the worker, repeat full suites, dispatch source/container CI or contact real FHIR services. [STANDARD.md](STANDARD.md) links the shared standard.

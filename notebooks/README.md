# FHIR notebooks

[01 · A complete FHIR search still needs reference and quantity checks](01_complete_pages_do_not_make_quantities_comparable.ipynb) uses explicit synthetic Patient/Observation resources to exercise Bundle pagination, preserve unresolved references and normalize a narrowly defined quantity subset. The experiment changes page size from 2 to 4.

## Run

Requirements: [uv](https://docs.astral.sh/uv/), Python 3.13+ (uv can install it), and internet access for dependency/native-worker downloads. No clinical system, patient data, credentials, Go build or worker checkout is needed.

```sh
cd notebooks
uv sync --locked
uv run --locked python execute.py --timeout 300
```

The locked environment uses Haybarn 2.0.0rc11. Visible SQL attaches `github-auto://Query-farm/vgi-fhir@v0.4.0`, release commit `ced609df56514c8a53a9a2929bd709d68e87feae`. VGI downloads the platform-native release, verifies its published SHA-256 and caches it. Upgrade the pin deliberately and rerun defaults and the experiment.

Optional editor kernel:

```sh
uv run --locked python -m ipykernel install --user --name vgi-fhir-notebooks --display-name "Python (vgi-fhir notebooks)"
```

Use this directory as working directory, then Restart Kernel → Run All. The runner creates a temporary kernel registration and replaces saved output only after every cell succeeds.

## Separate experiment

```sh
uv run --locked python execute.py --parameters '{"PAGE_SIZE":4}' --output-dir artifacts/page-4
```

Overrides require a separate destination. Branding, docs, fixture, server helper, runner and locked environment accompany the copy. Restore 2 for defaults. Generated logs, charts, experiments and virtual environments are ignored.

## Synthetic fixture and analysis policy

`synthetic_resources.json` contains four synthetic Patient resources and twelve synthetic Observations. They were authored for this notebook, not derived from patient records. Names and identifiers are explicit fixture labels. `fixture_server.py` serves a read-only subset of FHIR R4 on a local ephemeral port, including CapabilityStatement metadata and paged searchset Bundles. It honors `_count` 2 or 4 and emits opaque `next` cursors. The real unchanged worker performs HTTP retrieval, follows links and flattens resources. It never forwards fixture requests to an external service.

The notebook reconciles known identifiers and page counts before analysis. The worker caps a search at 1,000 resources and stops on empty pages; a real server's completeness must be established separately. Page size is not a total-result limit. No timestamp is invented for acquisition from a real server: this is a deterministic local fixture. Resource `effectiveDateTime` values are synthetic UTC values and are retained in raw JSON, not used for clinical trends.

The left join resolves only exact local `Patient/<id>` references. Absolute/external or missing-local references remain unresolved; stripping an external URL would assume an identity this notebook cannot establish. Every Observation has one coding. The technical inclusion policy selects LOINC system/code `29463-7`, final status, a numeric comparator-free quantity and UCUM system/code `kg` or `g`, then converts grams by dividing by 1,000. Other codes, non-final statuses, absent quantities, comparators and unsupported units/systems stay visible in an ordered reason table. This is not a general FHIR validator or clinical acceptance policy.

A deliberately zero-valued synthetic quantity demonstrates that zero survives flattening while a non-quantity value remains NULL. No claim of clinical plausibility, normality or patient health follows from the conversion. The notebook calculates no diagnostic thresholds or clinical averages.

Official specifications: [FHIR R4 Observation](https://hl7.org/fhir/R4/observation.html), [references](https://hl7.org/fhir/R4/references.html), [search](https://hl7.org/fhir/R4/search.html), and [CapabilityStatement](https://hl7.org/fhir/R4/capabilitystatement.html). Real deployments need explicit policies for other resource shapes, multiple codings, references and value types. Authentication is outside this local fixture; do not save real tokens or patient records in notebook outputs.

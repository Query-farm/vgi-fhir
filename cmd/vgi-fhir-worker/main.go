// Copyright 2026 Query Farm LLC - https://query.farm

// Command vgi-fhir-worker is a VGI worker that queries HL7 FHIR R4 REST servers
// over plain REST/JSON and returns resources as DuckDB rows. It speaks the VGI
// protocol over stdio.
package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/Query-farm/vgi-fhir/internal/fhirworker"
	"github.com/Query-farm/vgi-go/vgi"
)

func main() {
	// Accept --http for HTTP transport and --unix for the AF_UNIX launcher
	// transport; default is stdio. Unknown launcher flags are tolerated (the
	// VGI extension varies argv to key its worker cache), so we filter to flags
	// we actually define before parsing.
	httpMode := flag.Bool("http", false, "Run as an HTTP server instead of stdio")
	// The bind address used when --http is set. Default 127.0.0.1:0 (loopback +
	// ephemeral port) keeps dev/CI behaviour identical to the old hardcoded
	// value; the container entrypoint passes 0.0.0.0:$PORT so a published host
	// port (and the /health probe) can reach it.
	httpAddr := flag.String("http-addr", "127.0.0.1:0", "HTTP bind address (host:port) when --http is set")
	unixPath := flag.String("unix", "", "Serve the AF_UNIX launcher transport on this socket path instead of stdio")
	logFlags := vgi.RegisterLoggingFlags(flag.CommandLine)
	_ = flag.CommandLine.Parse(filterKnownFlags(os.Args[1:], map[string]bool{
		"log-level":  true,
		"log-format": true,
		"log-logger": true,
		"unix":       true,
		"http-addr":  true,
	}))
	if err := logFlags.Apply(); err != nil {
		log.Fatalf("logging flags: %v", err)
	}

	sourceURL := "https://github.com/Query-farm/vgi-fhir"
	w := vgi.NewWorker(
		vgi.WithCatalogName(fhirworker.CatalogName),
		vgi.WithCatalogComment("Query HL7 FHIR R4 REST servers and return resources as rows"),
		// Catalog-level discovery tags consumed by the vgi-lint metadata-quality
		// linter and surfaced via duckdb_databases().tags. The author/copyright/
		// license/support tags advertise provenance and support policy; the
		// description tags drive LLM/agent tool selection and listing pages.
		vgi.WithCatalogTags(map[string]string{
			"source":    "vgi-fhir",
			"vgi.title": "FHIR R4 Connector",
			// VGI148: one-sentence listing subtitle shown under the title.
			"vgi.subtitle": "Query HL7 FHIR R4 REST servers from SQL: patients, observations, generic search, reads by id, and server capabilities.",
			"vgi.keywords": `["fhir","hl7","fhir r4","healthcare","ehr","patient",` +
				`"observation","clinical","rest","json","capabilitystatement",` +
				`"interoperability","medical records"]`,
			"vgi.doc_llm": "Query live HL7 FHIR R4 REST servers from SQL. " +
				"List Patients and Observations with their core demographic and " +
				"vital-sign fields flattened into columns, run a generic search over " +
				"any FHIR resource type, read a single resource by id, and inspect a " +
				"server's CapabilityStatement (which resource types and REST " +
				"interactions it supports). Use to answer questions about patients, " +
				"clinical observations, and the capabilities of an EHR/FHIR endpoint " +
				"without leaving SQL; every function also returns the full resource JSON.",
			"vgi.doc_md": "# FHIR R4 in SQL\n\n" +
				"![HL7 FHIR logo](https://hl7.org/fhir/assets/images/fhir-logo-www.png)\n\n" +
				"**Query live HL7 FHIR R4 healthcare APIs directly from SQL** — turn any " +
				"FHIR REST server into DuckDB tables and run analytics over patients, " +
				"clinical observations, and server capabilities without writing a line " +
				"of integration code.\n\n" +
				"The `fhir` catalog connects to any [HL7 FHIR R4](https://hl7.org/fhir/R4/) " +
				"endpoint over plain REST/JSON and streams the resources back as DuckDB " +
				"rows over Apache Arrow. FHIR (Fast Healthcare Interoperability Resources) " +
				"is the modern HL7 standard that powers EHR, EMR, and health-data " +
				"interoperability across hospitals, payers, and digital-health platforms. " +
				"This connector is for analysts, data engineers, and clinical-informatics " +
				"teams who want to explore, validate, or extract FHIR data with familiar " +
				"SQL instead of bespoke API clients — point a function at a service base " +
				"URL (for example `https://hapi.fhir.org/baseR4`) and start querying.\n\n" +
				"Under the hood the worker speaks the FHIR R4 [RESTful API]" +
				"(https://hl7.org/fhir/R4/http.html) using nothing but the Go standard " +
				"library, so it stays light and dependency-free while remaining faithful " +
				"to the published [FHIR R4 specification](https://hl7.org/fhir/R4/) (build " +
				"source at [HL7/fhir on GitHub](https://github.com/HL7/fhir)). It follows " +
				"FHIR [search](https://hl7.org/fhir/R4/search.html) Bundle pagination " +
				"automatically — every `link[].relation == \"next\"` page is fetched up to " +
				"a safety cap — so a single query transparently spans many result pages. " +
				"An optional OAuth bearer `token` argument unlocks protected endpoints, " +
				"and non-2xx responses (including FHIR `OperationOutcome` bodies) surface " +
				"as clean SQL errors rather than panics.\n\n" +
				"The connector turns a FHIR endpoint's core capabilities into SQL: pull " +
				"clinical resources with their most-used fields flattened into typed " +
				"columns, run a generic search across any resource type (Condition, " +
				"Encounter, MedicationRequest, …) with an optional raw query string and " +
				"`_count` page size, read an individual resource by its logical id, and " +
				"discover which resource types and REST interactions a server supports " +
				"from its [CapabilityStatement](https://hl7.org/fhir/R4/capabilitystatement.html). " +
				"Flattened results always carry the untouched resource JSON alongside the " +
				"typed columns, so nothing is lost — anything not surfaced as a column is " +
				"still reachable with `json_extract`. Built on the " +
				"[vgi-go](https://github.com/Query-farm/vgi-go) SDK.\n\n" +
				"Not sure which endpoint to point at? Browse the built-in " +
				"`fhir_servers` directory view for a curated list of public FHIR R4 " +
				"base URLs, then hand one to any `fhir_*` function. Worked, runnable " +
				"statements ship as each function's examples.",
			"vgi.author":             "Query.Farm",
			"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
			"vgi.license":            "MIT",
			"vgi.support_contact":    "https://github.com/Query-farm/vgi-fhir/issues",
			"vgi.support_policy_url": "https://github.com/Query-farm/vgi-fhir/blob/main/README.md",
			// VGI152 agent-check suite: natural-language analyst tasks that
			// `vgi-lint simulate` runs to measure how well an agent uses this
			// worker. Each task names the SMART Health IT R4 sandbox as the base
			// URL (a frozen synthetic Synthea dataset, so the expected answers stay
			// true). Only name/prompt are published here: the answer SQL lives in
			// the private vgi-agent-tests.yaml sidecar (VGI416), keyed by name.
			"vgi.agent_test_tasks": `[` +
				`{"name":"has_ruecker_patient","prompt":"Using the FHIR R4 server at https://r4.smarthealthit.org, determine whether there is at least one Patient whose family name is Ruecker. Answer with a single boolean."},` +
				`{"name":"has_numeric_observation","prompt":"From the FHIR R4 server at https://r4.smarthealthit.org, determine whether any Observation carries a numeric measurement (a non-null value). Answer with a single boolean."},` +
				`{"name":"has_conditions","prompt":"Determine whether the FHIR R4 server at https://r4.smarthealthit.org has any Condition resources. Answer with a single boolean."},` +
				`{"name":"supports_patient_type","prompt":"Read the CapabilityStatement of the FHIR R4 server at https://r4.smarthealthit.org and determine whether it advertises support for the Patient resource type. Answer with a single boolean."},` +
				`{"name":"read_patient_by_id","prompt":"Read the Patient resource with logical id '45a69bf0-7116-4f9f-a1d9-29700439b4a4' from the FHIR R4 server at https://r4.smarthealthit.org and determine whether its family name is 'Ruecker'. Answer with a single boolean."},` +
				`{"name":"directory_lists_hapi","prompt":"The vgi-fhir connector exposes a directory of well-known public FHIR test servers. Determine whether it lists a server whose base URL is https://hapi.fhir.org/baseR4. Answer with a single boolean."},` +
				`{"name":"ruecker_active_condition_count","prompt":"On the FHIR R4 server at https://r4.smarthealthit.org, find the Patient whose family name is Ruecker and count that patient's Condition resources whose clinical status is 'active'. Answer with a single integer."},` +
				`{"name":"ruecker_weight_at_alzheimers_onset","prompt":"On the FHIR R4 server at https://r4.smarthealthit.org, the Patient with family name Ruecker has a Condition for familial Alzheimer's disease. What was that patient's body weight in kg (Body Weight Observation, LOINC 29463-7) at the first measurement taken on or after the condition's onset date? Answer with a single DOUBLE rounded to 1 decimal place."}` +
				`]`,
		}),
		vgi.WithCatalogInfo(vgi.CatalogInfo{
			Name:      fhirworker.CatalogName,
			SourceURL: &sourceURL,
		}),
		// Schema-level description tags (duckdb_schemas().tags) for the single
		// "main" schema that holds all FHIR table functions.
		vgi.WithSchemaComments(map[string]string{
			"main": "FHIR R4 query functions: patients, observations, generic search, single-resource read, and server capabilities.",
		}),
		vgi.WithSchemaTags(map[string]map[string]string{
			"main": {
				"vgi.title": "FHIR R4 Query Functions",
				// VGI413 ordered category registry; each function carries a
				// matching vgi.category tag naming one of these.
				"vgi.categories": `[` +
					`{"name":"Clinical Data","description":"Clinical resources (Patients, Observations) with their most-used fields flattened into typed columns."},` +
					`{"name":"Generic Resource Access","description":"Search any FHIR resource type or read a single resource by its logical id, returning the raw resource JSON."},` +
					`{"name":"Server Discovery","description":"Inspect a FHIR server's CapabilityStatement to see which resource types and REST interactions it supports."}` +
					`]`,
				"vgi.keywords": `["fhir","fhir r4","patient","observation","search",` +
					`"read","capabilities","capabilitystatement","resource",` +
					`"healthcare","ehr","clinical"]`,
				// VGI123 classifying tags use BARE keys (not vgi.-namespaced) so the
				// schema is findable by facet/topic.
				"domain":   "healthcare",
				"category": "interoperability",
				"topic":    "fhir-r4-rest",
				// Per-object vgi.source_url is intentionally omitted (VGI139):
				// source_url belongs only on the catalog object (CatalogInfo.SourceURL).
				"vgi.doc_llm": "The `main` schema holds the FHIR R4 query " +
					"functions of the vgi-fhir connector. Reach for it when a question is " +
					"about patients, clinical observations, or what a FHIR/EHR endpoint " +
					"can do. Every function's first argument is the FHIR R4 service base " +
					"URL (e.g. `https://hapi.fhir.org/baseR4`); an optional `token` " +
					"argument carries an OAuth bearer token. The functions cover the " +
					"common ways to pull data from a FHIR endpoint: flattening the " +
					"most-used fields of a clinical resource into typed columns (while " +
					"still returning the raw resource JSON), running a generic search " +
					"over any resource type, fetching a single resource by logical id, " +
					"and reporting the resource types and REST interactions a server " +
					"supports from its CapabilityStatement. Bundle pages are followed " +
					"automatically, so result sets span all `next` links.",
				"vgi.doc_md": "## FHIR R4 Query Functions\n\n" +
					"This schema groups every table function exposed by the **vgi-fhir** " +
					"connector. Each function takes a FHIR R4 service *base URL* as its " +
					"first argument and returns resources as DuckDB rows over Apache " +
					"Arrow.\n\n" +
					"The functions cover the common ways to pull data from a FHIR R4 " +
					"endpoint: listing clinical resources with their most-used fields " +
					"flattened into typed columns, running a generic search across any " +
					"resource type, reading an individual resource by its logical id, and " +
					"inspecting the server's CapabilityStatement for the resource types " +
					"and REST interactions it supports.\n\n" +
					"### Notes\n\n" +
					"- Bundle pagination (`link[].relation == \"next\"`) is followed " +
					"automatically up to a safety cap.\n" +
					"- The flattened functions also expose the untouched resource JSON " +
					"(`raw` / `resource`) so nothing is lost.\n" +
					"- An optional `token` named argument supplies an OAuth bearer token " +
					"for protected endpoints.",
				// VGI506/VGI515 representative example queries for the schema
				// (catalog-qualified). Each carries a human-readable description
				// via the described [{description,sql}] JSON list form.
				"vgi.example_queries": fhirworker.ExampleQueriesJSON(schemaExamples),
			},
		}),
	)
	fhirworker.Register(w)

	if *httpMode {
		if err := w.RunHttp(*httpAddr); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *unixPath != "" {
		// AF_UNIX launcher transport: serve on the given socket path. The SDK
		// prints "UNIX:<path>" once listening; idleTimeout=0 disables the
		// self-shutdown timer (the launcher/CI owns the process lifecycle).
		if err := w.RunUnix(*unixPath, 0); err != nil {
			log.Fatal(err)
		}
		return
	}
	w.RunStdio()
}

// schemaExamples is the schema-level vgi.example_queries list for "main": one
// representative query per object, then the composed multi-function examples
// (defined once in fhirworker, shared with each driving function's examples).
var schemaExamples = append([]vgi.CatalogExample{
	{
		Description: "List the curated public FHIR R4 test servers and whether each needs authentication.",
		SQL:         "SELECT name, base_url, requires_auth FROM fhir.main.fhir_servers ORDER BY name;",
	},
	{
		Description: "List Patients with their core demographic fields flattened into columns.",
		SQL:         "SELECT id, family, given, gender, birth_date FROM fhir.main.fhir_patients('https://r4.smarthealthit.org') LIMIT 20;",
	},
	{
		Description: "List numeric Observations with their code, value, and unit.",
		SQL:         "SELECT code, code_display, value, unit FROM fhir.main.fhir_observations('https://r4.smarthealthit.org') WHERE value IS NOT NULL LIMIT 20;",
	},
	{
		Description: "List the ids of Condition resources on a FHIR R4 server.",
		SQL:         "SELECT id FROM fhir.main.fhir_search('https://r4.smarthealthit.org', 'Condition') LIMIT 20;",
	},
	{
		Description: "Read a single Patient resource by logical id and return its full JSON.",
		SQL:         "SELECT resource FROM fhir.main.fhir_read('https://r4.smarthealthit.org', 'Patient', '45a69bf0-7116-4f9f-a1d9-29700439b4a4');",
	},
	{
		Description: "Expand a server's supported REST interactions to one row per (resource type, interaction).",
		SQL:         "SELECT resource_type, UNNEST(interactions) AS interaction FROM fhir.main.fhir_capabilities('https://r4.smarthealthit.org');",
	},
}, fhirworker.ComposedExamples...)

// filterKnownFlags drops argv tokens for flags this binary doesn't define, so
// launcher-injected differentiation flags don't abort flag parsing. Flags named
// in valueFlags consume the following token as their value.
func filterKnownFlags(args []string, valueFlags map[string]bool) []string {
	defined := map[string]bool{}
	flag.CommandLine.VisitAll(func(f *flag.Flag) { defined[f.Name] = true })
	var out []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") {
			continue
		}
		name := strings.TrimLeft(a, "-")
		hasInlineValue := strings.ContainsRune(name, '=')
		if eq := strings.IndexByte(name, '='); eq >= 0 {
			name = name[:eq]
		}
		if !defined[name] {
			continue
		}
		out = append(out, a)
		if valueFlags[name] && !hasInlineValue && i+1 < len(args) {
			i++
			out = append(out, args[i])
		}
	}
	return out
}

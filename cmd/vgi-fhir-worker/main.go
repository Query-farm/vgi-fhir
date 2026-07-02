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
	unixPath := flag.String("unix", "", "Serve the AF_UNIX launcher transport on this socket path instead of stdio")
	logFlags := vgi.RegisterLoggingFlags(flag.CommandLine)
	_ = flag.CommandLine.Parse(filterKnownFlags(os.Args[1:], map[string]bool{
		"log-level":  true,
		"log-format": true,
		"log-logger": true,
		"unix":       true,
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
			"vgi.title": "FHIR R4 REST Query Connector",
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
				"still reachable with `json_extract`. List the `main` schema to discover " +
				"the exact functions and their signatures. Built on the " +
				"[vgi-go](https://github.com/Query-farm/vgi-go) SDK.\n\n" +
				"```sql\n" +
				"SELECT id, family, given, gender, birth_date\n" +
				"FROM fhir.main.fhir_patients('https://hapi.fhir.org/baseR4');\n" +
				"```",
			"vgi.author":             "Query.Farm",
			"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
			"vgi.license":            "MIT",
			"vgi.support_contact":    "https://github.com/Query-farm/vgi-fhir/issues",
			"vgi.support_policy_url": "https://github.com/Query-farm/vgi-fhir/blob/main/README.md",
			// VGI152 agent-check suite: natural-language analyst tasks that
			// `vgi-lint simulate` runs to measure how well an agent uses this
			// worker. Each task names the public HAPI FHIR R4 test server as the
			// base URL and pairs a prompt with the canonical answer SQL.
			"vgi.agent_test_tasks": `[` +
				`{"name":"list_patient_names","prompt":"Using the FHIR R4 server at https://hapi.fhir.org/baseR4, list the family and given names of the patients whose family name is Chalmers.","reference_sql":"SELECT family, given FROM fhir.main.fhir_patients('https://hapi.fhir.org/baseR4', query := 'family=Chalmers');"},` +
				`{"name":"numeric_observations","prompt":"From the FHIR R4 server at https://hapi.fhir.org/baseR4, show the code, numeric value, and unit for observations that carry a numeric measurement.","reference_sql":"SELECT code, value, unit FROM fhir.main.fhir_observations('https://hapi.fhir.org/baseR4') WHERE value IS NOT NULL;"},` +
				`{"name":"search_conditions","prompt":"List the logical ids of the Condition resources available on the FHIR R4 server at https://hapi.fhir.org/baseR4.","reference_sql":"SELECT id FROM fhir.main.fhir_search('https://hapi.fhir.org/baseR4', 'Condition');"},` +
				`{"name":"server_resource_types","prompt":"Read the capability statement of the FHIR R4 server at https://hapi.fhir.org/baseR4 and list the resource types it advertises.","reference_sql":"SELECT resource_type FROM fhir.main.fhir_capabilities('https://hapi.fhir.org/baseR4');"}` +
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
					"automatically, so result sets span all `next` links. List the schema " +
					"to see the exact functions and their signatures.",
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
					"and REST interactions it supports. List the schema to see the exact " +
					"functions and their signatures.\n\n" +
					"### Notes\n\n" +
					"- Bundle pagination (`link[].relation == \"next\"`) is followed " +
					"automatically up to a safety cap.\n" +
					"- The flattened functions also expose the untouched resource JSON " +
					"(`raw` / `resource`) so nothing is lost.\n" +
					"- An optional `token` named argument supplies an OAuth bearer token " +
					"for protected endpoints.",
				// VGI506 representative example queries for the schema (catalog-qualified).
				"vgi.example_queries": "SELECT id, family, given, gender, birth_date FROM fhir.main.fhir_patients('https://hapi.fhir.org/baseR4');\n" +
					"SELECT code, code_display, value, unit FROM fhir.main.fhir_observations('https://hapi.fhir.org/baseR4') WHERE value IS NOT NULL;\n" +
					"SELECT id FROM fhir.main.fhir_search('https://hapi.fhir.org/baseR4', 'Condition');\n" +
					"SELECT resource FROM fhir.main.fhir_read('https://hapi.fhir.org/baseR4', 'Patient', 'example');\n" +
					"SELECT resource_type, UNNEST(interactions) AS interaction FROM fhir.main.fhir_capabilities('https://hapi.fhir.org/baseR4');",
			},
		}),
	)
	fhirworker.Register(w)

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
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

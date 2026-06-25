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
			"vgi.doc_md": "# fhir\n\n" +
				"Query [HL7 FHIR R4](https://hl7.org/fhir/R4/) REST servers over " +
				"plain REST/JSON and return resources as DuckDB rows.\n\n" +
				"Table functions: `fhir_patients`, `fhir_observations`, `fhir_search`, " +
				"`fhir_read`, `fhir_capabilities`. Bundle pagination (`next` links) is " +
				"followed automatically; flattened functions also expose the raw JSON.",
			"vgi.author":             "Query.Farm",
			"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
			"vgi.license":            "MIT",
			"vgi.support_contact":    "https://github.com/Query-farm/vgi-fhir/issues",
			"vgi.support_policy_url": "https://github.com/Query-farm/vgi-fhir/blob/main/README.md",
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
				"vgi.doc_llm": "The `main` schema contains the FHIR R4 query " +
					"functions of the vgi-fhir connector. Use it when a question is " +
					"about patients, clinical observations, or what a FHIR/EHR endpoint " +
					"can do. Every function's first argument is the FHIR R4 service base " +
					"URL (e.g. `https://hapi.fhir.org/baseR4`); an optional `token` " +
					"argument carries an OAuth bearer token. `fhir_patients` and " +
					"`fhir_observations` flatten the most-used fields into columns while " +
					"still returning the raw resource JSON; `fhir_search` works for any " +
					"resource type; `fhir_read` fetches one resource by id; and " +
					"`fhir_capabilities` lists the supported resource types and REST " +
					"interactions from the server's CapabilityStatement. Bundle pages are " +
					"followed automatically, so result sets span all `next` links.",
				"vgi.doc_md": "## FHIR R4 Query Functions\n\n" +
					"This schema groups every table function exposed by the **vgi-fhir** " +
					"connector. Each function takes a FHIR R4 service *base URL* as its " +
					"first argument and returns resources as DuckDB rows over Apache " +
					"Arrow.\n\n" +
					"| Function | Purpose |\n" +
					"|---|---|\n" +
					"| `fhir_patients` | List Patients with core demographics flattened. |\n" +
					"| `fhir_observations` | List Observations with `valueQuantity` flattened. |\n" +
					"| `fhir_search` | Generic search over any resource type. |\n" +
					"| `fhir_read` | Read a single resource by logical id. |\n" +
					"| `fhir_capabilities` | Inspect the server's CapabilityStatement. |\n\n" +
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

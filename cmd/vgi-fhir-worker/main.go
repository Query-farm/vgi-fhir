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
			"source": "vgi-fhir",
			"vgi.description_llm": "Query live HL7 FHIR R4 REST servers from SQL. " +
				"List Patients and Observations with their core demographic and " +
				"vital-sign fields flattened into columns, run a generic search over " +
				"any FHIR resource type, read a single resource by id, and inspect a " +
				"server's CapabilityStatement (which resource types and REST " +
				"interactions it supports). Use to answer questions about patients, " +
				"clinical observations, and the capabilities of an EHR/FHIR endpoint " +
				"without leaving SQL; every function also returns the full resource JSON.",
			"vgi.description_md": "# fhir\n\n" +
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
				"vgi.description_llm": "FHIR R4 query functions. List Patients and " +
					"Observations with core fields flattened, search any resource type, " +
					"read one resource by id, and read a server's CapabilityStatement.",
				"vgi.description_md": "FHIR R4 query functions over Apache Arrow: " +
					"`fhir_patients`, `fhir_observations`, `fhir_search`, `fhir_read`, " +
					"`fhir_capabilities`.",
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

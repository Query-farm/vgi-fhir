// Copyright 2026 Query Farm LLC - https://query.farm

package fhirworker

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
)

// TestExampleQueriesTagMatchesExamples checks every function's described
// vgi.example_queries tag decodes to exactly its native Examples (VGI515).
func TestExampleQueriesTagMatchesExamples(t *testing.T) {
	type named interface {
		Name() string
		Metadata() vgi.FunctionMetadata
	}
	for _, f := range []named{&SearchFunction{}, &ReadFunction{}, &PatientsFunction{}, &ObservationsFunction{}, &CapabilitiesFunction{}} {
		md := f.Metadata()
		var got []exampleQuery
		if err := json.Unmarshal([]byte(md.Tags["vgi.example_queries"]), &got); err != nil {
			t.Fatalf("%s: bad vgi.example_queries JSON: %v", f.Name(), err)
		}
		if len(got) != len(md.Examples) {
			t.Fatalf("%s: tag has %d examples, Examples has %d", f.Name(), len(got), len(md.Examples))
		}
		for i, e := range md.Examples {
			if got[i].SQL != e.SQL || got[i].Description != e.Description {
				t.Errorf("%s example %d: tag %+v != native %+v", f.Name(), i, got[i], e)
			}
		}
	}
}

// TestComposedExamplesOnDrivingFunction checks each composed example sits within
// the first 5 examples of the function that drives it (the agent simulator
// shows only those) and references at least two different fhir objects.
func TestComposedExamplesOnDrivingFunction(t *testing.T) {
	cases := []struct {
		ex     vgi.CatalogExample
		driver []vgi.CatalogExample
	}{
		{ExampleWeightTrend, observationsExamples},
		{ExampleProblemList, searchExamples},
		{ExampleLatestVitals, readExamples},
	}
	for _, c := range cases {
		found := false
		for i, e := range c.driver {
			if e == c.ex && i < 5 {
				found = true
			}
		}
		if !found {
			t.Errorf("composed example not in the first 5 of its driver: %q", c.ex.Description)
		}
		objs := map[string]bool{}
		for _, o := range []string{"fhir_search", "fhir_read", "fhir_patients", "fhir_observations", "fhir_capabilities", "fhir_servers"} {
			if strings.Contains(c.ex.SQL, "fhir.main."+o+"(") {
				objs[o] = true
			}
		}
		if len(objs) < 2 {
			t.Errorf("composed example combines %d objects, want >= 2: %q", len(objs), c.ex.SQL)
		}
	}
}

// TestExampleQueriesJSONNoHTMLEscape keeps `&` in search query strings literal.
func TestExampleQueriesJSONNoHTMLEscape(t *testing.T) {
	got := ExampleQueriesJSON([]vgi.CatalogExample{{Description: "d", SQL: "a=1&b=2"}})
	if want := `[{"description":"d","sql":"a=1&b=2"}]`; got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

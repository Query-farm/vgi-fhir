// Copyright 2026 Query Farm LLC - https://query.farm

package fhirworker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Query-farm/vgi-fhir/internal/mockfhir"
)

// startServer spins up the mock FHIR server with no auth and small pages so
// pagination is exercised across the 3-patient seed set.
func startServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(mockfhir.NewHandler(mockfhir.Config{PageSize: 2}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// startAuthServer spins up the mock FHIR server requiring the bearer token.
func startAuthServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(mockfhir.NewHandler(mockfhir.Config{Token: mockfhir.Token, PageSize: 2}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestSearchAllFollowsPages(t *testing.T) {
	base := startServer(t)
	res, err := SearchResources(context.Background(), FetchOptions{BaseURL: base, Count: 2}, "Patient")
	if err != nil {
		t.Fatalf("SearchResources: %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 patients across pages, got %d", len(res))
	}
	if res[0].ID != "p1" {
		t.Errorf("unexpected first id %q", res[0].ID)
	}
}

func TestSearchMaxResults(t *testing.T) {
	base := startServer(t)
	res, err := SearchResources(context.Background(),
		FetchOptions{BaseURL: base, Count: 1, MaxResults: 2}, "Patient")
	if err != nil {
		t.Fatalf("SearchResources: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("MaxResults=2 should cap at 2, got %d", len(res))
	}
}

func TestReadOne(t *testing.T) {
	base := startServer(t)
	raw, err := ReadOne(context.Background(), FetchOptions{BaseURL: base}, "Patient", "p2")
	if err != nil {
		t.Fatalf("ReadOne: %v", err)
	}
	if idOf(raw) != "p2" {
		t.Errorf("expected id p2, got %q", idOf(raw))
	}
}

func TestReadOne404(t *testing.T) {
	base := startServer(t)
	_, err := ReadOne(context.Background(), FetchOptions{BaseURL: base}, "Patient", "nope")
	if err == nil {
		t.Fatal("expected a 404 error")
	}
}

func TestFetchPatientsFlatten(t *testing.T) {
	base := startServer(t)
	ps, err := FetchPatients(context.Background(), FetchOptions{BaseURL: base, Count: 2})
	if err != nil {
		t.Fatalf("FetchPatients: %v", err)
	}
	if len(ps) != 3 {
		t.Fatalf("expected 3 patients, got %d", len(ps))
	}
	p1 := ps[0]
	if p1.Family != "Smith" || p1.Given != "Alice M" || p1.Gender != "female" || !p1.Active {
		t.Errorf("p1 flatten wrong: %+v", p1)
	}
	// p3 has no name → empty family/given.
	p3 := ps[2]
	if p3.Family != "" || p3.Given != "" {
		t.Errorf("p3 should have empty name fields: %+v", p3)
	}
}

func TestFetchObservationsFlatten(t *testing.T) {
	base := startServer(t)
	obs, err := FetchObservations(context.Background(), FetchOptions{BaseURL: base})
	if err != nil {
		t.Fatalf("FetchObservations: %v", err)
	}
	if len(obs) != 2 {
		t.Fatalf("expected 2 observations, got %d", len(obs))
	}
	o1 := obs[0]
	if o1.Value == nil || *o1.Value != 72.5 || o1.Unit != "kg" {
		t.Errorf("o1 numeric value wrong: %+v (value=%v)", o1, o1.Value)
	}
	if o1.Code != "29463-7" || o1.CodeDisplay != "Body Weight" {
		t.Errorf("o1 code wrong: %+v", o1)
	}
	if o1.Subject != "Patient/p1" {
		t.Errorf("o1 subject wrong: %q", o1.Subject)
	}
	// o2 has no valueQuantity → nil value (SQL NULL).
	if obs[1].Value != nil {
		t.Errorf("o2 should have nil value, got %v", *obs[1].Value)
	}
}

func TestFetchCapabilities(t *testing.T) {
	base := startServer(t)
	caps, err := FetchCapabilities(context.Background(), FetchOptions{BaseURL: base})
	if err != nil {
		t.Fatalf("FetchCapabilities: %v", err)
	}
	if len(caps) != 2 {
		t.Fatalf("expected 2 resource types, got %d", len(caps))
	}
	found := false
	for _, c := range caps {
		if c.ResourceType == "Patient" {
			found = true
			if len(c.Interactions) != 2 || c.Interactions[0] != "read" {
				t.Errorf("Patient interactions wrong: %+v", c.Interactions)
			}
		}
	}
	if !found {
		t.Error("Patient not listed in capabilities")
	}
}

func TestBearerEnforced(t *testing.T) {
	base := startAuthServer(t)
	// Missing token → error.
	if _, err := FetchPatients(context.Background(), FetchOptions{BaseURL: base, Count: 2}); err == nil {
		t.Fatal("expected an auth error without a token")
	}
	// Correct token → ok.
	ps, err := FetchPatients(context.Background(),
		FetchOptions{BaseURL: base, Token: mockfhir.Token, Count: 2})
	if err != nil {
		t.Fatalf("with token: %v", err)
	}
	if len(ps) != 3 {
		t.Fatalf("expected 3 patients with token, got %d", len(ps))
	}
}

func TestServerErrorSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"resourceType":"OperationOutcome","issue":[{"severity":"error","code":"exception","diagnostics":"boom"}]}`))
	}))
	defer srv.Close()
	_, err := SearchResources(context.Background(), FetchOptions{BaseURL: srv.URL}, "Patient")
	if err == nil {
		t.Fatal("expected a 500/OperationOutcome error")
	}
}

func TestBadJSONSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not json at all`))
	}))
	defer srv.Close()
	_, err := SearchResources(context.Background(), FetchOptions{BaseURL: srv.URL}, "Patient")
	if err == nil {
		t.Fatal("expected a decode error for bad JSON")
	}
}

func TestMissingBaseURL(t *testing.T) {
	if _, err := FetchPatients(context.Background(), FetchOptions{}); err == nil {
		t.Fatal("expected an error for missing base_url")
	}
}

// Copyright 2026 Query Farm LLC - https://query.farm

// Package mockfhir provides an in-memory HL7 FHIR R4 server used by both the Go
// unit tests (as an httptest.Server) and the standalone mockserver binary (for
// the haybarn SQL E2E). It serves /Patient and /Observation as searchset
// Bundles (with Patient paginated across two pages via a `next` link),
// /Patient/{id} as a single resource, and /metadata as a CapabilityStatement.
// When a non-empty bearer token is configured it is required on every request.
package mockfhir

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// Token is the bearer token the mock server requires. The standalone server can
// override it; the unit tests use this constant.
const Token = "test-token-123"

// seedPatients is the fixed Patient dataset (3 patients, served across 2 pages).
var seedPatients = []map[string]any{
	{
		"resourceType": "Patient",
		"id":           "p1",
		"active":       true,
		"gender":       "female",
		"birthDate":    "1980-05-12",
		"name": []map[string]any{
			{"use": "official", "family": "Smith", "given": []string{"Alice", "M"}},
		},
	},
	{
		"resourceType": "Patient",
		"id":           "p2",
		"active":       false,
		"gender":       "male",
		"birthDate":    "1975-11-30",
		"name": []map[string]any{
			{"use": "official", "family": "Jones", "given": []string{"Bob"}},
		},
	},
	{
		"resourceType": "Patient",
		"id":           "p3",
		"active":       true,
		"gender":       "other",
		"birthDate":    "1990-01-01",
		// No name: family/given should be empty.
	},
}

// seedObservations: one numeric valueQuantity and one non-numeric (no value).
var seedObservations = []map[string]any{
	{
		"resourceType": "Observation",
		"id":           "o1",
		"status":       "final",
		"code": map[string]any{
			"coding": []map[string]any{
				{"system": "http://loinc.org", "code": "29463-7", "display": "Body Weight"},
			},
			"text": "Body Weight",
		},
		"valueQuantity":     map[string]any{"value": 72.5, "unit": "kg", "code": "kg"},
		"effectiveDateTime": "2024-02-01T10:00:00Z",
		"subject":           map[string]any{"reference": "Patient/p1"},
	},
	{
		"resourceType": "Observation",
		"id":           "o2",
		"status":       "final",
		"code": map[string]any{
			"coding": []map[string]any{
				{"system": "http://loinc.org", "code": "72166-2", "display": "Tobacco smoking status"},
			},
			"text": "Tobacco smoking status",
		},
		// No valueQuantity (valueCodeableConcept instead) → value must be NULL.
		"valueCodeableConcept": map[string]any{"text": "Never smoker"},
		"effectiveDateTime":    "2024-02-01T10:05:00Z",
		"subject":              map[string]any{"reference": "Patient/p1"},
	},
}

// Config configures the mock server.
type Config struct {
	// Token, when non-empty, is required as `Authorization: Bearer <Token>`.
	Token string
	// PageSize forces Patient search to paginate (a `next` link is emitted
	// while more pages remain). Values <= 0 mean "all in one page".
	PageSize int
}

// NewHandler returns the FHIR HTTP handler.
func NewHandler(cfg Config) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/Patient/", singleHandler(cfg, seedPatients))
	mux.HandleFunc("/Patient", searchHandler(cfg, "Patient", seedPatients))
	mux.HandleFunc("/Observation", searchHandler(cfg, "Observation", seedObservations))
	mux.HandleFunc("/metadata", metadataHandler(cfg))
	return mux
}

// checkAuth enforces the bearer token when one is configured.
func checkAuth(cfg Config, w http.ResponseWriter, r *http.Request) bool {
	if cfg.Token == "" {
		return true
	}
	if r.Header.Get("Authorization") != "Bearer "+cfg.Token {
		writeOperationOutcome(w, http.StatusUnauthorized, "login", "invalid or missing bearer token")
		return false
	}
	return true
}

// searchHandler serves a searchset Bundle, paginating by `_count` / `_page`
// when cfg.PageSize forces small pages.
func searchHandler(cfg Config, resourceType string, resources []map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(cfg, w, r) {
			return
		}
		total := len(resources)

		// pageSize: server cap (cfg.PageSize) wins; otherwise honor _count.
		pageSize := total
		if c := r.URL.Query().Get("_count"); c != "" {
			if v, err := strconv.Atoi(c); err == nil && v > 0 {
				pageSize = v
			}
		}
		if cfg.PageSize > 0 && pageSize > cfg.PageSize {
			pageSize = cfg.PageSize
		}
		if pageSize <= 0 {
			pageSize = total
		}

		page := 0
		if p := r.URL.Query().Get("_page"); p != "" {
			if v, err := strconv.Atoi(p); err == nil && v >= 0 {
				page = v
			}
		}

		lo := page * pageSize
		hi := lo + pageSize
		var slice []map[string]any
		if lo < total {
			if hi > total {
				hi = total
			}
			slice = resources[lo:hi]
		}

		entries := make([]map[string]any, 0, len(slice))
		for _, res := range slice {
			id, _ := res["id"].(string)
			entries = append(entries, map[string]any{
				"fullUrl":  "urn:" + resourceType + "/" + id,
				"resource": res,
			})
		}

		links := []map[string]any{
			{"relation": "self", "url": r.URL.String()},
		}
		// Emit a `next` link while more pages remain.
		if hi < total {
			next := *r.URL
			q := next.Query()
			q.Set("_count", strconv.Itoa(pageSize))
			q.Set("_page", strconv.Itoa(page+1))
			next.RawQuery = q.Encode()
			// Build an absolute URL so the client follows it directly.
			scheme := "http"
			if r.TLS != nil {
				scheme = "https"
			}
			links = append(links, map[string]any{
				"relation": "next",
				"url":      scheme + "://" + r.Host + next.RequestURI(),
			})
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"resourceType": "Bundle",
			"type":         "searchset",
			"total":        total,
			"link":         links,
			"entry":        entries,
		})
	}
}

// singleHandler serves GET /Patient/{id} returning the single resource.
func singleHandler(cfg Config, resources []map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(cfg, w, r) {
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/Patient/")
		for _, res := range resources {
			if res["id"] == id {
				writeJSON(w, http.StatusOK, res)
				return
			}
		}
		writeOperationOutcome(w, http.StatusNotFound, "not-found", "Patient/"+id+" not found")
	}
}

// metadataHandler serves the CapabilityStatement.
func metadataHandler(cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(cfg, w, r) {
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"resourceType": "CapabilityStatement",
			"status":       "active",
			"fhirVersion":  "4.0.1",
			"rest": []map[string]any{
				{
					"mode": "server",
					"resource": []map[string]any{
						{
							"type": "Patient",
							"interaction": []map[string]any{
								{"code": "read"}, {"code": "search-type"},
							},
						},
						{
							"type": "Observation",
							"interaction": []map[string]any{
								{"code": "read"}, {"code": "search-type"},
							},
						},
					},
				},
			},
		})
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/fhir+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeOperationOutcome(w http.ResponseWriter, status int, code, diagnostics string) {
	writeJSON(w, status, map[string]any{
		"resourceType": "OperationOutcome",
		"issue": []map[string]any{
			{"severity": "error", "code": code, "diagnostics": diagnostics},
		},
	})
}

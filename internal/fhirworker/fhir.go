// Copyright 2026 Query Farm LLC - https://query.farm

package fhirworker

import (
	"context"
	"encoding/json"
)

// All result structs below are exported and gob-encodable: they are stored
// directly in table-function state, which the SDK gob-encodes between NewState
// and Process. Optional numeric fields use *float64 so "missing" surfaces as
// SQL NULL rather than 0.

// Resource is a generic FHIR resource: its id plus the full raw JSON.
type Resource struct {
	ID  string
	Raw string
}

// Patient is a FHIR Patient with the common core fields flattened. Raw holds
// the complete resource JSON so every other element remains accessible.
type Patient struct {
	ID        string
	Family    string
	Given     string
	Gender    string
	BirthDate string
	Active    bool
	Raw       string
}

// Observation is a FHIR Observation with common fields flattened. Value is nil
// when the observation has no numeric valueQuantity (so it becomes SQL NULL).
type Observation struct {
	ID          string
	Status      string
	Code        string
	CodeDisplay string
	Value       *float64
	Unit        string
	Effective   string
	Subject     string
	Raw         string
}

// CapabilityResource lists one resource type from a CapabilityStatement and the
// REST interactions the server supports on it.
type CapabilityResource struct {
	ResourceType string
	Interactions []string
}

// ---- raw JSON shapes (for flattening only) ----

type fhirCoding struct {
	System  string `json:"system"`
	Code    string `json:"code"`
	Display string `json:"display"`
}

type fhirCodeableConcept struct {
	Coding []fhirCoding `json:"coding"`
	Text   string       `json:"text"`
}

type fhirHumanName struct {
	Use    string   `json:"use"`
	Family string   `json:"family"`
	Given  []string `json:"given"`
}

type fhirReference struct {
	Reference string `json:"reference"`
	Display   string `json:"display"`
}

type fhirQuantity struct {
	Value *float64 `json:"value"`
	Unit  string   `json:"unit"`
	Code  string   `json:"code"`
}

type fhirPatient struct {
	ID        string          `json:"id"`
	Active    bool            `json:"active"`
	Gender    string          `json:"gender"`
	BirthDate string          `json:"birthDate"`
	Name      []fhirHumanName `json:"name"`
}

type fhirObservation struct {
	ID                string              `json:"id"`
	Status            string              `json:"status"`
	Code              fhirCodeableConcept `json:"code"`
	ValueQuantity     *fhirQuantity       `json:"valueQuantity"`
	EffectiveDateTime string              `json:"effectiveDateTime"`
	EffectiveInstant  string              `json:"effectiveInstant"`
	Subject           fhirReference       `json:"subject"`
}

type capabilityStatement struct {
	ResourceType string `json:"resourceType"`
	Rest         []struct {
		Mode     string `json:"mode"`
		Resource []struct {
			Type        string `json:"type"`
			Interaction []struct {
				Code string `json:"code"`
			} `json:"interaction"`
		} `json:"resource"`
	} `json:"rest"`
}

// firstName picks the official/usual name, falling back to the first one.
func firstName(names []fhirHumanName) fhirHumanName {
	if len(names) == 0 {
		return fhirHumanName{}
	}
	for _, n := range names {
		if n.Use == "official" || n.Use == "usual" || n.Use == "" {
			return n
		}
	}
	return names[0]
}

// firstGiven joins all given (first/middle) name parts with a space.
func firstGiven(n fhirHumanName) string {
	if len(n.Given) == 0 {
		return ""
	}
	out := n.Given[0]
	for _, g := range n.Given[1:] {
		out += " " + g
	}
	return out
}

// firstCoding returns the first coding of a CodeableConcept (code + display).
func firstCoding(c fhirCodeableConcept) (code, display string) {
	if len(c.Coding) > 0 {
		code = c.Coding[0].Code
		display = c.Coding[0].Display
	}
	if display == "" {
		display = c.Text
	}
	return code, display
}

// idOf extracts a resource's top-level id without a full decode.
func idOf(raw json.RawMessage) string {
	var idOnly struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(raw, &idOnly)
	return idOnly.ID
}

// SearchResources runs a generic search and returns each resource's id + raw.
func SearchResources(ctx context.Context, opts FetchOptions, resourceType string) ([]Resource, error) {
	raws, err := SearchAll(ctx, opts, resourceType)
	if err != nil {
		return nil, err
	}
	out := make([]Resource, 0, len(raws))
	for _, raw := range raws {
		out = append(out, Resource{ID: idOf(raw), Raw: string(raw)})
	}
	return out, nil
}

// FetchPatients searches {base}/Patient and flattens the core fields.
func FetchPatients(ctx context.Context, opts FetchOptions) ([]Patient, error) {
	raws, err := SearchAll(ctx, opts, "Patient")
	if err != nil {
		return nil, err
	}
	out := make([]Patient, 0, len(raws))
	for _, raw := range raws {
		var p fhirPatient
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, err
		}
		nm := firstName(p.Name)
		out = append(out, Patient{
			ID:        p.ID,
			Family:    nm.Family,
			Given:     firstGiven(nm),
			Gender:    p.Gender,
			BirthDate: p.BirthDate,
			Active:    p.Active,
			Raw:       string(raw),
		})
	}
	return out, nil
}

// FetchObservations searches {base}/Observation and flattens the core fields.
// Non-numeric observations (no valueQuantity) yield a nil Value (SQL NULL).
func FetchObservations(ctx context.Context, opts FetchOptions) ([]Observation, error) {
	raws, err := SearchAll(ctx, opts, "Observation")
	if err != nil {
		return nil, err
	}
	out := make([]Observation, 0, len(raws))
	for _, raw := range raws {
		var o fhirObservation
		if err := json.Unmarshal(raw, &o); err != nil {
			return nil, err
		}
		code, display := firstCoding(o.Code)
		eff := o.EffectiveDateTime
		if eff == "" {
			eff = o.EffectiveInstant
		}
		var value *float64
		var unit string
		if o.ValueQuantity != nil {
			value = o.ValueQuantity.Value
			unit = o.ValueQuantity.Unit
			if unit == "" {
				unit = o.ValueQuantity.Code
			}
		}
		out = append(out, Observation{
			ID:          o.ID,
			Status:      o.Status,
			Code:        code,
			CodeDisplay: display,
			Value:       value,
			Unit:        unit,
			Effective:   eff,
			Subject:     o.Subject.Reference,
			Raw:         string(raw),
		})
	}
	return out, nil
}

// FetchCapabilities reads {base}/metadata and lists each supported resource type
// with its REST interaction codes.
func FetchCapabilities(ctx context.Context, opts FetchOptions) ([]CapabilityResource, error) {
	raw, err := GetMetadata(ctx, opts)
	if err != nil {
		return nil, err
	}
	var cs capabilityStatement
	if err := json.Unmarshal(raw, &cs); err != nil {
		return nil, err
	}
	var out []CapabilityResource
	for _, rest := range cs.Rest {
		for _, res := range rest.Resource {
			ints := make([]string, 0, len(res.Interaction))
			for _, it := range res.Interaction {
				ints = append(ints, it.Code)
			}
			out = append(out, CapabilityResource{
				ResourceType: res.Type,
				Interactions: ints,
			})
		}
	}
	return out, nil
}

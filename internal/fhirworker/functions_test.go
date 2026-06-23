// Copyright 2026 Query Farm LLC - https://query.farm

package fhirworker

import (
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// strCol builds a 1-row string array (optionally NULL) for use as a positional
// argument value.
func strCol(v string, null bool) arrow.Array {
	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	if null {
		b.AppendNull()
	} else {
		b.Append(v)
	}
	return b.NewArray()
}

func argsWith(positional ...arrow.Array) *vgi.Arguments {
	return &vgi.Arguments{
		Positional: positional,
		Named:      map[string]arrow.Array{},
	}
}

func TestSearchNewStateData(t *testing.T) {
	base := startServer(t)
	f := &SearchFunction{}
	st, err := f.NewState(&vgi.ProcessParams{
		Args: argsWith(strCol(base, false), strCol("Patient", false)),
	})
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	if len(st.Resources) != 3 {
		t.Fatalf("expected 3 resources, got %d", len(st.Resources))
	}
	if st.Done {
		t.Error("state should not be marked done before Process")
	}
}

func TestSearchNullArgsNoRows(t *testing.T) {
	f := &SearchFunction{}
	st, err := f.NewState(&vgi.ProcessParams{
		Args: argsWith(strCol("", true), strCol("Patient", false)),
	})
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	if len(st.Resources) != 0 {
		t.Errorf("NULL base_url should yield no resources, got %d", len(st.Resources))
	}
}

func TestReadNewStateData(t *testing.T) {
	base := startServer(t)
	f := &ReadFunction{}
	st, err := f.NewState(&vgi.ProcessParams{
		Args: argsWith(strCol(base, false), strCol("Patient", false), strCol("p1", false)),
	})
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	if !st.Found || st.Resource == "" {
		t.Fatalf("expected a found resource, got %+v", st)
	}
}

func TestPatientsNewStateData(t *testing.T) {
	base := startServer(t)
	f := &PatientsFunction{}
	st, err := f.NewState(&vgi.ProcessParams{
		Args: argsWith(strCol(base, false)),
	})
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	if len(st.Patients) != 3 || st.Patients[0].Family != "Smith" {
		t.Fatalf("unexpected patients: %+v", st.Patients)
	}
}

func TestObservationsNewStateData(t *testing.T) {
	base := startServer(t)
	f := &ObservationsFunction{}
	st, err := f.NewState(&vgi.ProcessParams{
		Args: argsWith(strCol(base, false)),
	})
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	if len(st.Observations) != 2 {
		t.Fatalf("expected 2 observations, got %d", len(st.Observations))
	}
	if st.Observations[0].Value == nil || *st.Observations[0].Value != 72.5 {
		t.Errorf("o1 value wrong: %+v", st.Observations[0])
	}
}

func TestCapabilitiesNewStateData(t *testing.T) {
	base := startServer(t)
	f := &CapabilitiesFunction{}
	st, err := f.NewState(&vgi.ProcessParams{
		Args: argsWith(strCol(base, false)),
	})
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	if len(st.Resources) != 2 {
		t.Fatalf("expected 2 capability resources, got %d", len(st.Resources))
	}
}

func TestPatientsAuthError(t *testing.T) {
	base := startAuthServer(t)
	f := &PatientsFunction{}
	_, err := f.NewState(&vgi.ProcessParams{
		Args: argsWith(strCol(base, false)),
	})
	if err == nil {
		t.Fatal("expected an auth error without a token (server requires bearer)")
	}
}

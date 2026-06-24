// Copyright 2026 Query Farm LLC - https://query.farm

package fhirworker

import (
	"bytes"
	"encoding/gob"
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
	if len(st.Rows) != 3 {
		t.Fatalf("expected 3 resources, got %d", len(st.Rows))
	}
	if st.Offset != 0 {
		t.Error("cursor offset should be 0 before Process")
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
	if len(st.Rows) != 0 {
		t.Errorf("NULL base_url should yield no resources, got %d", len(st.Rows))
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
	if len(st.Rows) != 1 || st.Rows[0] == "" {
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
	if len(st.Rows) != 3 || st.Rows[0].Family != "Smith" {
		t.Fatalf("unexpected patients: %+v", st.Rows)
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
	if len(st.Rows) != 2 {
		t.Fatalf("expected 2 observations, got %d", len(st.Rows))
	}
	if st.Rows[0].Value == nil || *st.Rows[0].Value != 72.5 {
		t.Errorf("o1 value wrong: %+v", st.Rows[0])
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
	if len(st.Rows) != 2 {
		t.Fatalf("expected 2 capability resources, got %d", len(st.Rows))
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

// TestCursorSurvivesContinuation proves the streaming cursor round-trips through
// a gob snapshot between Process ticks — the exact path the stateless HTTP
// transport takes when it resumes a producer from its continuation token. A
// multi-row producer that advances Offset BEFORE yielding emits each row exactly
// once and terminates (the bug a bare Done flag re-emitted forever over HTTP).
func TestCursorSurvivesContinuation(t *testing.T) {
	const total = 200 // > rowsPerTick (64), so it spans several continuations
	rows := make([]Patient, total)
	for i := range rows {
		rows[i] = Patient{ID: string(rune('a' + i%26))}
	}
	st := &patientsState{Cursor[Patient]{Rows: rows}}

	emitted := 0
	for tick := 0; tick < total+5; tick++ {
		slice, done := st.nextSlice()
		if done {
			break
		}
		emitted += len(slice)
		// Simulate the HTTP continuation boundary: gob-encode then decode the
		// LIVE state, and resume from the snapshot (never the in-memory state).
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(st); err != nil {
			t.Fatalf("gob encode: %v", err)
		}
		var resumed patientsState
		if err := gob.NewDecoder(&buf).Decode(&resumed); err != nil {
			t.Fatalf("gob decode: %v", err)
		}
		st = &resumed
	}
	if emitted != total {
		t.Fatalf("cursor emitted %d rows across continuations, want %d", emitted, total)
	}
	if _, done := st.nextSlice(); !done {
		t.Fatal("cursor did not report done after draining all rows")
	}
}

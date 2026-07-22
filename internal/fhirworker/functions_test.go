// Copyright 2026 Query Farm LLC - https://query.farm

package fhirworker

import (
	"bytes"
	"encoding/gob"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Query-farm/vgi-fhir/internal/mockfhir"
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

// drainPages walks a streaming cursor to exhaustion the way Process does and
// returns every row it emitted, plus the number of ticks it took.
func drainPages[T any](t *testing.T, c *PageCursor[T], fetch func(string) ([]T, string, error)) ([]T, int) {
	t.Helper()
	var all []T
	for tick := 1; tick <= 1000; tick++ {
		slice, done, err := c.nextSlice(fetch)
		if err != nil {
			t.Fatalf("nextSlice: %v", err)
		}
		if done {
			return all, tick
		}
		all = append(all, slice...)
	}
	t.Fatal("streaming cursor did not terminate")
	return nil, 0
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
	// NewState must NOT have fetched anything: it only seeds the first page URL
	// (see PageCursor — the whole point is that the first batch costs one
	// round-trip, not the whole crawl).
	if len(st.Rows) != 0 || st.Offset != 0 {
		t.Errorf("NewState should not prefetch rows, got %d", len(st.Rows))
	}
	if st.NextURL == "" {
		t.Fatal("NewState should seed the first page URL")
	}
	rows, _ := drainPages(t, &st.PageCursor, st.fetchPage)
	if len(rows) != 3 {
		t.Fatalf("expected 3 resources across pages, got %d", len(rows))
	}
	if rows[0].ID != "p1" {
		t.Errorf("unexpected first id %q", rows[0].ID)
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
	if st.NextURL != "" {
		t.Error("NULL base_url should leave the cursor exhausted")
	}
	rows, _ := drainPages(t, &st.PageCursor, st.fetchPage)
	if len(rows) != 0 {
		t.Errorf("NULL base_url should yield no resources, got %d", len(rows))
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
	rows, _ := drainPages(t, &st.PageCursor, st.fetchPage)
	if len(rows) != 3 || rows[0].Family != "Smith" {
		t.Fatalf("unexpected patients: %+v", rows)
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
	rows, _ := drainPages(t, &st.PageCursor, st.fetchPage)
	if len(rows) != 2 {
		t.Fatalf("expected 2 observations, got %d", len(rows))
	}
	if rows[0].Value == nil || *rows[0].Value != 72.5 {
		t.Errorf("o1 value wrong: %+v", rows[0])
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
	st, err := f.NewState(&vgi.ProcessParams{
		Args: argsWith(strCol(base, false)),
	})
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	// The fetch now happens on the first Process tick, so that is where an auth
	// failure surfaces — still a clean error, never a panic.
	if _, _, err := st.nextSlice(st.fetchPage); err == nil {
		t.Fatal("expected an auth error without a token (server requires bearer)")
	}
}

// TestStreamingFirstBatchIsOneRequest is the VGI911 regression guard: a scan
// must be able to emit its first batch after ONE HTTP round-trip. The previous
// design crawled every page in NewState, so the first row of a large search cost
// the whole crawl — and a VGI scan blocked inside its first batch cannot be
// cancelled.
func TestStreamingFirstBatchIsOneRequest(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		mockfhir.NewHandler(mockfhir.Config{PageSize: 1}).ServeHTTP(w, r)
	}))
	defer srv.Close()

	f := &PatientsFunction{}
	st, err := f.NewState(&vgi.ProcessParams{Args: argsWith(strCol(srv.URL, false))})
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	if got := atomic.LoadInt32(&requests); got != 0 {
		t.Fatalf("NewState made %d HTTP requests, want 0", got)
	}
	rows, _, err := st.nextSlice(st.fetchPage)
	if err != nil {
		t.Fatalf("nextSlice: %v", err)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Fatalf("first batch cost %d HTTP requests, want 1", got)
	}
	if len(rows) != 1 {
		t.Fatalf("first batch returned %d rows, want the first page's 1", len(rows))
	}
}

// TestStreamingStopsOnEmptyPage covers the trap that wedged fhir_patients /
// fhir_observations / fhir_search against the public HAPI R4 server: it answers
// an unfiltered search with a Bundle that has NO entries and a `next` link, on
// every page, forever. Following `next` blindly is an unbounded crawl.
func TestStreamingStopsOnEmptyPage(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Header().Set("Content-Type", "application/fhir+json")
		_, _ = io.WriteString(w, `{"resourceType":"Bundle","type":"searchset","link":[`+
			`{"relation":"next","url":"http://`+r.Host+`/Patient?_page=next"}]}`)
	}))
	defer srv.Close()

	f := &PatientsFunction{}
	st, err := f.NewState(&vgi.ProcessParams{Args: argsWith(strCol(srv.URL, false))})
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	rows, ticks := drainPages(t, &st.PageCursor, st.fetchPage)
	if len(rows) != 0 {
		t.Fatalf("expected no rows from an always-empty server, got %d", len(rows))
	}
	if ticks != 1 {
		t.Errorf("expected the scan to finish on the first tick, took %d", ticks)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Fatalf("an empty page with a next link cost %d requests, want 1", got)
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
	// A single already-fetched page: NextURL is empty, so no fetch is attempted
	// and the test stays offline while still exercising the continuation path.
	st := &patientsState{PageCursor[Patient]{Rows: rows, Fetched: total, Max: defaultMaxResults}}
	noFetch := func(string) ([]Patient, string, error) {
		t.Fatal("cursor must not fetch when NextURL is empty")
		return nil, "", nil
	}

	emitted := 0
	for tick := 0; tick < total+5; tick++ {
		slice, done, err := st.nextSlice(noFetch)
		if err != nil {
			t.Fatalf("nextSlice: %v", err)
		}
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
	if _, done, err := st.nextSlice(noFetch); err != nil || !done {
		t.Fatalf("cursor did not report done after draining all rows (err=%v)", err)
	}
}

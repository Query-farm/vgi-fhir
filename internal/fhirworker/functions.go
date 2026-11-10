// Copyright 2026 Query Farm LLC - https://query.farm

package fhirworker

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CatalogName is the VGI catalog name advertised by this worker.
const CatalogName = "fhir"

// IMPORTANT (gob-state gotcha): table-function state is gob-encoded by the SDK
// between NewState and Process (it may cross a process/worker boundary). State
// structs must therefore hold only EXPORTED, gob-encodable fields — no
// arrow.Record, no interfaces, channels, funcs, or unexported fields. Every
// table function stores plain exported Go slices and rebuilds the Arrow batch in
// Process.
//
// The single-request functions (fhir_read, fhir_capabilities) fetch in NewState
// and embed Cursor[T]. The search-backed ones (fhir_search, fhir_patients,
// fhir_observations) do NO I/O in NewState — they embed PageCursor[T], which
// fetches one Bundle page at a time from Process, so the first batch costs one
// HTTP round-trip rather than the whole paginated crawl. See PageCursor below
// for why that matters (VGI911).
//
// WHY AN EXPLICIT CURSOR, NOT A bool Done (the HTTP-continuation fix):
//
// Over the stateless HTTP transport the worker keeps NO live state between
// Process ticks — the framework round-trips the producer state through an opaque
// continuation token: after each tick it gob-encodes the LIVE user state, the
// client returns the token, and the worker resumes by gob-decoding it. The HTTP
// server emits at most one data batch per response, so a producer with more to
// emit is always resumed mid-stream from its token.
//
// A bare `Done bool` flipped *after* the single Emit does not survive that
// limit-1 continuation: the resumed tick observes the pre-Emit snapshot,
// re-emits the same rows, and the scan never terminates — an infinite loop that
// pins the worker (subprocess/unix hold live state in memory, so they never hit
// it). The fix is an explicit cursor: each state embeds Cursor[T] carrying the
// fetched Rows plus the Offset of the next unemitted row. Process emits a
// bounded slice from Offset, advances Offset BEFORE yielding, and Finish()es
// once Offset >= len(Rows). The framework snapshots Offset into the token, so
// HTTP resumes correctly and terminates. This is the reference pattern for every
// streaming Go table function over HTTP. fhir_patients/fhir_search/etc. each
// emit MANY rows (Bundle pagination), so the cursor is mandatory, not cosmetic.

// rowsPerTick bounds how many rows each Process tick emits. Emitting a bounded
// slice and advancing the cursor is what makes the offset observable across the
// HTTP continuation boundary (and scales to large result sets).
const rowsPerTick = 64

// The four standard per-object discovery/description tags the vgi-lint strict
// profile expects on every function — vgi.title (VGI124), vgi.doc_llm
// (VGI112), vgi.doc_md (VGI113), and vgi.keywords (VGI126) — are set inline in
// each function's Metadata().Tags. Each vgi.title is a multi-word human display
// name so it does not normalize-equal the machine function name (VGI125).
// Per-object vgi.source_url is intentionally NOT set (VGI139): source_url
// belongs only on the catalog object (CatalogInfo.SourceURL in main.go).

// ExampleBaseURL is the public FHIR R4 endpoint most documented examples point
// at: the SMART Health IT R4 sandbox (also listed in the fhir_servers view).
// A few examples use the public HAPI R4 server instead, so the examples show
// that base_url is a per-call argument (VGI1003); those are always FILTERED
// searches or the CapabilityStatement, never an unfiltered scan or a pinned id.
//
// The unfiltered/pinned-id examples are deliberately NOT on the HAPI server. HAPI is a shared scratchpad
// — anyone can POST to it, resources are routinely deleted, and an example
// pinned to a logical id there rots (fhir_read's old `Patient/example` example
// answered HTTP 404). Worse, HAPI answers an UNFILTERED search with a Bundle
// that carries a `next` link and no entries, forever, so those examples returned
// nothing at all. The SMART sandbox serves a fixed synthetic Synthea dataset
// that has not changed since 2021, so a pinned id keeps resolving and an
// unfiltered search returns real rows on the first page.
const ExampleBaseURL = "https://r4.smarthealthit.org"

// ExamplePatientID is a Patient logical id that exists on ExampleBaseURL and is
// stable (part of the frozen Synthea seed set): family name "Ruecker".
const ExamplePatientID = "45a69bf0-7116-4f9f-a1d9-29700439b4a4"

// executableExamples (VGI509) is a JSON list of self-contained, catalog-qualified
// examples an agent can run as-is against an attached worker. Every list-style
// example carries an explicit LIMIT: these scans stream page by page over the
// network, so a bounded example is both faster and the behaviour we want to
// demonstrate. expected_result is omitted on purpose.
const executableExamples = `[
  {
    "description": "List Patients with core demographic fields flattened into columns.",
    "sql": "SELECT id, family, given, gender, birth_date FROM fhir.main.fhir_patients('` + ExampleBaseURL + `') LIMIT 20"
  },
  {
    "description": "List numeric Observations with their code, value, and unit.",
    "sql": "SELECT code, code_display, value, unit FROM fhir.main.fhir_observations('` + ExampleBaseURL + `') WHERE value IS NOT NULL LIMIT 20"
  },
  {
    "description": "Search Condition resources and return their logical ids.",
    "sql": "SELECT id FROM fhir.main.fhir_search('` + ExampleBaseURL + `', 'Condition') LIMIT 20"
  },
  {
    "description": "Expand a server's supported resource types and REST interactions.",
    "sql": "SELECT resource_type, UNNEST(interactions) AS interaction FROM fhir.main.fhir_capabilities('` + ExampleBaseURL + `')"
  }
]`

// Cursor is the shared streaming cursor embedded by every table-function state:
// the eagerly fetched rows plus the offset of the next unemitted row. Both
// fields are exported so gob round-trips them through the HTTP continuation
// token. The TYPE is exported (Cursor, not cursor) because the SDK counts a
// state struct's exported FIELDS at registration to verify it is gob-encodable —
// an embedded field named after an unexported type would not be counted and the
// worker would panic at startup.
type Cursor[T any] struct {
	Rows   []T
	Offset int
}

// nextSlice returns the next bounded slice of rows to emit and advances the
// cursor past them. It reports done=true once all rows have been consumed, at
// which point Process should call out.Finish().
func (c *Cursor[T]) nextSlice() (slice []T, done bool) {
	if c.Offset >= len(c.Rows) {
		return nil, true
	}
	end := c.Offset + rowsPerTick
	if end > len(c.Rows) {
		end = len(c.Rows)
	}
	slice = c.Rows[c.Offset:end]
	c.Offset = end
	return slice, false
}

// PageCursor is the STREAMING cursor used by every search-backed table function
// (fhir_search / fhir_patients / fhir_observations). Unlike Cursor, which is
// handed a fully materialised slice, it holds only the CURRENT Bundle page plus
// the URL of the next one, and fetches that next page lazily when the current
// page is drained.
//
// WHY (VGI911, the uncancellable-first-batch rule): the previous design fetched
// every page in NewState before Process emitted anything, so the first row of
// `SELECT * FROM fhir_patients(base) LIMIT 10` cost the WHOLE crawl — up to
// maxResults/_count round-trips (20 by default) against a public server, and
// against a server that advertises a `next` link on an empty page (the public
// HAPI R4 server does) it never finished at all and only stopped when the 30s
// context deadline fired. A VGI scan blocked inside its first batch cannot be
// interrupted, so that wedges the client. Streaming makes first-row latency one
// HTTP round-trip regardless of result-set size.
//
// Every field is exported and gob-encodable because the SDK round-trips this
// state through the stateless HTTP transport's continuation token — the same
// invariant Cursor documents above. Offset/NextURL/Fetched all advance BEFORE
// the batch is yielded, so a resumed tick never re-emits a page.
type PageCursor[T any] struct {
	// Rows is the current page, already flattened.
	Rows []T
	// Offset is the index of the next unemitted row within Rows.
	Offset int
	// NextURL is the page to fetch once Rows is drained ("" = search over).
	NextURL string
	// Fetched counts rows collected across pages, bounded by Max.
	Fetched int
	// Token is the bearer token to replay on each page request.
	Token string
	// Max bounds the total rows the scan will collect (<= 0 means the
	// package default).
	Max int
}

// newPageCursor seeds a streaming cursor at the first page of a search.
func newPageCursor[T any](opts FetchOptions, resourceType string) (PageCursor[T], error) {
	start, err := SearchStartURL(opts, resourceType)
	if err != nil {
		return PageCursor[T]{}, err
	}
	maxResults := opts.MaxResults
	if maxResults <= 0 {
		maxResults = defaultMaxResults
	}
	return PageCursor[T]{NextURL: start, Token: opts.Token, Max: maxResults}, nil
}

// nextSlice returns the next bounded slice of rows to emit, fetching the next
// page when the current one is drained, and advances the cursor past them. It
// reports done=true when the search is exhausted, at which point Process should
// call out.Finish().
//
// fetchPage does one HTTP round-trip and returns that page's rows plus the URL
// of the page after it; each table function supplies its own so the raw Bundle
// entries are flattened into its row type.
func (c *PageCursor[T]) nextSlice(fetchPage func(pageURL string) ([]T, string, error)) ([]T, bool, error) {
	for {
		if c.Offset < len(c.Rows) {
			end := c.Offset + rowsPerTick
			if end > len(c.Rows) {
				end = len(c.Rows)
			}
			slice := c.Rows[c.Offset:end]
			c.Offset = end
			return slice, false, nil
		}
		max := c.Max
		if max <= 0 {
			max = defaultMaxResults
		}
		if c.NextURL == "" || c.Fetched >= max {
			return nil, true, nil
		}
		rows, following, err := fetchPage(c.NextURL)
		if err != nil {
			return nil, false, err
		}
		// A page with no resources ends the search. Some servers (the public
		// HAPI R4 server among them) advertise a `next` link on an empty page,
		// so trusting `next` alone is an unbounded crawl.
		if len(rows) == 0 {
			c.Rows, c.Offset, c.NextURL = nil, 0, ""
			return nil, true, nil
		}
		if c.Fetched+len(rows) > max {
			rows = rows[:max-c.Fetched]
		}
		c.Fetched += len(rows)
		c.Rows, c.Offset, c.NextURL = rows, 0, following
	}
}

// optsFrom builds FetchOptions from the bound common arguments. query/token are
// the empty string when their named arg is NULL/absent.
func optsFrom(baseURL, token, query string, count int64) FetchOptions {
	return FetchOptions{
		BaseURL: baseURL,
		Token:   token,
		Query:   query,
		Count:   count,
	}
}

// isNullArg reports whether positional argument pos is present and NULL.
func isNullArg(args *vgi.Arguments, pos int) bool {
	if args == nil {
		return true
	}
	col, err := args.GetColumn(pos)
	if err != nil {
		return false
	}
	return col.Len() == 0 || col.IsNull(0)
}

// buildStringListArray builds a List<String> (VARCHAR[]) column; one list per
// row from the rows[i] string slice.
func buildStringListArray(n int64, fn func(i int64) []string) arrow.Array {
	b := array.NewListBuilder(memory.DefaultAllocator, arrow.BinaryTypes.String)
	defer b.Release()
	vb := b.ValueBuilder().(*array.StringBuilder)
	for i := int64(0); i < n; i++ {
		b.Append(true)
		for _, s := range fn(i) {
			vb.Append(s)
		}
	}
	return b.NewArray()
}

// ---------------------------------------------------------------------------
// fhir_search(base_url, resource_type) -> (id, resource VARCHAR)
// ---------------------------------------------------------------------------

var searchSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.BinaryTypes.String},
	{Name: "resource", Type: arrow.BinaryTypes.String},
}, nil)

type searchArgs struct {
	BaseURL      string `vgi:"pos=0,name=base_url,doc=FHIR R4 service base URL"`
	ResourceType string `vgi:"pos=1,name=resource_type,doc=FHIR resource type (e.g. Patient, Observation)"`
	Query        string `vgi:"default=,doc=Raw search query string (e.g. name=smith&_count=50)"`
	Token        string `vgi:"default=,doc=OAuth bearer token"`
	Count        int64  `vgi:"default=50,doc=Page size (_count search parameter)"`
}

type searchState struct {
	PageCursor[Resource]
}

// fetchPage reads one Bundle page of the generic search and flattens it.
func (s *searchState) fetchPage(pageURL string) ([]Resource, string, error) {
	raws, next, err := SearchPage(context.Background(), FetchOptions{Token: s.Token}, pageURL)
	if err != nil {
		return nil, "", err
	}
	return FlattenResources(raws), next, nil
}

// SearchFunction lists resources of one type via a FHIR search.
type SearchFunction struct{}

var _ vgi.TypedTableFunc[searchState] = (*SearchFunction)(nil)

func (f *SearchFunction) Name() string { return "fhir_search" }

func (f *SearchFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Search a FHIR R4 resource type (one row per resource; resource = full JSON; follows Bundle next links)",
		Stability:   vgi.StabilityVolatile,
		Categories:  []string{"fhir", "healthcare"},
		Tags: map[string]string{
			"vgi.title":    "Search FHIR Resource Type",
			"vgi.category": "Generic Resource Access",
			"vgi.doc_llm":  "Search a single FHIR R4 resource type via the server's search endpoint, following Bundle next links across pages. Returns one row per matched resource with its logical id and the full resource as a JSON string. Use for any resource type (Patient, Observation, Condition, ...) with an optional raw search query and page size.",
			"vgi.doc_md": "## fhir_search\n\n" +
				"Run a FHIR R4 [search](https://hl7.org/fhir/R4/search.html) over a " +
				"single resource type and return one row per matched resource.\n\n" +
				"### Usage\n\n" +
				"Call it with a FHIR R4 base URL and a resource type (for example " +
				"`Condition`) to return one row per matched resource; runnable " +
				"statements are attached as this function's examples.\n\n" +
				"Pass a raw `query` (e.g. `name=smith&_count=50`) and/or `count` to " +
				"refine the search, and `token` for protected servers.\n\n" +
				"### Notes\n\n" +
				"- Follows Bundle `next` links across pages until exhausted or the " +
				"safety cap is reached.\n" +
				"- The `resource` column holds the complete resource JSON, so any " +
				"field not surfaced by the flattened functions is still reachable via " +
				"`json_extract`.",
			"vgi.keywords": `["fhir","search","search resources","resource type","bundle","paging","query","patient","observation","condition"]`,
			"vgi.result_columns_schema": `[` +
				`{"name":"id","type":"VARCHAR","description":"Logical id of the matched FHIR resource."},` +
				`{"name":"resource","type":"VARCHAR","description":"The full matched resource as a JSON string."}` +
				`]`,
			// VGI515 described examples: the native duckdb_functions().examples
			// carrier drops descriptions, so the same examples are re-supplied here
			// as a described [{description,sql}] JSON list (byte-identical to Examples).
			"vgi.example_queries": `[` +
				`{"description":"List the ids of all Condition resources on a FHIR R4 server (paging through Bundle next links).",` +
				`"sql":"SELECT id FROM fhir.main.fhir_search('https://r4.smarthealthit.org', 'Condition') LIMIT 20;"},` +
				`{"description":"Search Patients named 'smith' on the public HAPI R4 server, 100 per page, returning id plus the raw resource JSON.",` +
				`"sql":"SELECT id, resource FROM fhir.main.fhir_search('https://hapi.fhir.org/baseR4', 'Patient', query := 'name=smith', count := 100);"}` +
				`]`,
		},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT id FROM fhir.main.fhir_search('https://r4.smarthealthit.org', 'Condition') LIMIT 20;",
				Description: "List the ids of all Condition resources on a FHIR R4 server (paging through Bundle next links).",
			},
			{
				SQL:         "SELECT id, resource FROM fhir.main.fhir_search('https://hapi.fhir.org/baseR4', 'Patient', query := 'name=smith', count := 100);",
				Description: "Search Patients named 'smith' on the public HAPI R4 server, 100 per page, returning id plus the raw resource JSON.",
			},
		},
	}
}

func (f *SearchFunction) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(searchArgs{}) }

func (f *SearchFunction) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(searchSchema)
}

func (f *SearchFunction) NewState(params *vgi.ProcessParams) (*searchState, error) {
	var args searchArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if isNullArg(params.Args, 0) || isNullArg(params.Args, 1) {
		return &searchState{}, nil
	}
	// No network I/O here: seed the cursor at the first page URL and let
	// Process fetch it, so the scan's first batch costs one round-trip.
	cur, err := newPageCursor[Resource](
		optsFrom(args.BaseURL, args.Token, args.Query, args.Count), args.ResourceType)
	if err != nil {
		return nil, err
	}
	return &searchState{cur}, nil
}

func (f *SearchFunction) Process(_ context.Context, _ *vgi.ProcessParams, state *searchState, out *vgirpc.OutputCollector) error {
	r, done, err := state.nextSlice(state.fetchPage)
	if err != nil {
		return err
	}
	if done {
		return out.Finish()
	}
	n := int64(len(r))
	batch := array.NewRecordBatch(searchSchema, []arrow.Array{
		vgi.BuildStringArray(n, func(i int64) string { return r[i].ID }),
		vgi.BuildStringArray(n, func(i int64) string { return r[i].Raw }),
	}, n)
	defer batch.Release()
	return out.Emit(batch)
}

// NewSearchFunction builds the registerable table function.
func NewSearchFunction() vgi.TableFunction {
	return vgi.AsTableFunction[searchState](&SearchFunction{})
}

// ---------------------------------------------------------------------------
// fhir_read(base_url, resource_type, id) -> (resource VARCHAR)
// ---------------------------------------------------------------------------

var readSchema = arrow.NewSchema([]arrow.Field{
	{Name: "resource", Type: arrow.BinaryTypes.String},
}, nil)

type readArgs struct {
	BaseURL      string `vgi:"pos=0,name=base_url,doc=FHIR R4 service base URL"`
	ResourceType string `vgi:"pos=1,name=resource_type,doc=FHIR resource type (e.g. Patient)"`
	ID           string `vgi:"pos=2,name=id,doc=Logical id of the resource to read"`
	Token        string `vgi:"default=,doc=OAuth bearer token"`
}

type readState struct {
	Cursor[string]
}

// ReadFunction reads a single resource by id.
type ReadFunction struct{}

var _ vgi.TypedTableFunc[readState] = (*ReadFunction)(nil)

func (f *ReadFunction) Name() string { return "fhir_read" }

func (f *ReadFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Read a single FHIR R4 resource by id (one row; resource = full JSON)",
		Stability:   vgi.StabilityVolatile,
		Categories:  []string{"fhir", "healthcare"},
		Tags: map[string]string{
			"vgi.title":    "Read FHIR Resource By Id",
			"vgi.category": "Generic Resource Access",
			"vgi.doc_llm":  "Read a single FHIR R4 resource by its resource type and logical id (a GET on /{type}/{id}). Returns one row with the full resource as a JSON string; a missing resource surfaces as an error.",
			"vgi.doc_md": "## fhir_read\n\n" +
				"Read a single FHIR R4 resource by its type and logical id — a direct " +
				"`GET /{type}/{id}` against the server (the FHIR " +
				"[read](https://hl7.org/fhir/R4/http.html#read) interaction).\n\n" +
				"### Usage\n\n" +
				"Call it with a FHIR R4 base URL, a resource type, and a logical id " +
				"(for example `Patient` / `example`) to fetch that one resource as " +
				"JSON; runnable statements are attached as this function's examples.\n\n" +
				"### Notes\n\n" +
				"- Returns exactly one row when the resource exists; a missing " +
				"resource (HTTP 404) surfaces as a query error rather than an empty " +
				"result.\n" +
				"- Supply `token` for endpoints that require authentication.",
			"vgi.keywords": `["fhir","read","read resource","get by id","single resource","logical id","resource json"]`,
			"vgi.result_columns_schema": `[` +
				`{"name":"resource","type":"VARCHAR","description":"The requested resource as a JSON string."}` +
				`]`,
			// VGI515 described examples (see fhir_search): the native examples
			// carrier drops descriptions, so re-supply them as a described list.
			"vgi.example_queries": `[` +
				`{"description":"Read the canonical FHIR example Patient (logical id 'example') and return its full JSON.",` +
				`"sql":"SELECT resource FROM fhir.main.fhir_read('https://r4.smarthealthit.org', 'Patient', '45a69bf0-7116-4f9f-a1d9-29700439b4a4');"}` +
				`]`,
		},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT resource FROM fhir.main.fhir_read('https://r4.smarthealthit.org', 'Patient', '45a69bf0-7116-4f9f-a1d9-29700439b4a4');",
				Description: "Read the canonical FHIR example Patient (logical id 'example') and return its full JSON.",
			},
		},
	}
}

func (f *ReadFunction) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(readArgs{}) }

func (f *ReadFunction) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(readSchema)
}

func (f *ReadFunction) NewState(params *vgi.ProcessParams) (*readState, error) {
	var args readArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if isNullArg(params.Args, 0) || isNullArg(params.Args, 1) || isNullArg(params.Args, 2) {
		return &readState{}, nil
	}
	raw, err := ReadOne(context.Background(),
		optsFrom(args.BaseURL, args.Token, "", 0), args.ResourceType, args.ID)
	if err != nil {
		return nil, err
	}
	return &readState{Cursor[string]{Rows: []string{string(raw)}}}, nil
}

func (f *ReadFunction) Process(_ context.Context, _ *vgi.ProcessParams, state *readState, out *vgirpc.OutputCollector) error {
	r, done := state.nextSlice()
	if done {
		return out.Finish()
	}
	n := int64(len(r))
	batch := array.NewRecordBatch(readSchema, []arrow.Array{
		vgi.BuildStringArray(n, func(i int64) string { return r[i] }),
	}, n)
	defer batch.Release()
	return out.Emit(batch)
}

// NewReadFunction builds the registerable table function.
func NewReadFunction() vgi.TableFunction {
	return vgi.AsTableFunction[readState](&ReadFunction{})
}

// ---------------------------------------------------------------------------
// fhir_patients(base_url) ->
//   (id, family, given, gender, birth_date VARCHAR, active BOOLEAN, raw VARCHAR)
// ---------------------------------------------------------------------------

var patientsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.BinaryTypes.String},
	{Name: "family", Type: arrow.BinaryTypes.String},
	{Name: "given", Type: arrow.BinaryTypes.String},
	{Name: "gender", Type: arrow.BinaryTypes.String},
	{Name: "birth_date", Type: arrow.BinaryTypes.String},
	{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "raw", Type: arrow.BinaryTypes.String},
}, nil)

type patientsArgs struct {
	BaseURL string `vgi:"pos=0,name=base_url,doc=FHIR R4 service base URL"`
	Query   string `vgi:"default=,doc=Raw search query string (e.g. family=smith)"`
	Token   string `vgi:"default=,doc=OAuth bearer token"`
	Count   int64  `vgi:"default=50,doc=Page size (_count search parameter)"`
}

type patientsState struct {
	PageCursor[Patient]
}

// fetchPage reads one Bundle page of the Patient search and flattens it.
func (s *patientsState) fetchPage(pageURL string) ([]Patient, string, error) {
	raws, next, err := SearchPage(context.Background(), FetchOptions{Token: s.Token}, pageURL)
	if err != nil {
		return nil, "", err
	}
	rows, err := FlattenPatients(raws)
	if err != nil {
		return nil, "", err
	}
	return rows, next, nil
}

// PatientsFunction lists Patients with core fields flattened.
type PatientsFunction struct{}

var _ vgi.TypedTableFunc[patientsState] = (*PatientsFunction)(nil)

func (f *PatientsFunction) Name() string { return "fhir_patients" }

func (f *PatientsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "List FHIR R4 Patients (core fields flattened; raw = full JSON)",
		Stability:   vgi.StabilityVolatile,
		Categories:  []string{"fhir", "healthcare"},
		Tags: map[string]string{
			"vgi.title":    "List FHIR Patient Resources",
			"vgi.category": "Clinical Data",
			"vgi.doc_llm":  "List FHIR R4 Patient resources with their core demographic fields (family/given name, gender, birth date, active) flattened into columns, plus the full Patient resource as JSON. Follows Bundle next links and accepts an optional raw search query.",
			"vgi.doc_md": "## fhir_patients\n\n" +
				"List [Patient](https://hl7.org/fhir/R4/patient.html) resources from a " +
				"FHIR R4 server with the most-used demographic fields flattened into " +
				"columns, while still returning the full resource JSON.\n\n" +
				"### Usage\n\n" +
				"Call it with a FHIR R4 base URL to list Patients with their core " +
				"demographic fields flattened into columns; runnable statements are " +
				"attached as this function's examples.\n\n" +
				"Filter server-side with a raw `query` (e.g. `family=smith`), set the " +
				"page size with `count`, and authenticate with `token`.\n\n" +
				"### Notes\n\n" +
				"- One row per Patient across all Bundle pages (`next` links are " +
				"followed automatically).\n" +
				"- The `raw` column carries the untouched Patient JSON for fields not " +
				"flattened here.",
			"vgi.keywords": `["fhir","patients","patient","demographics","name","gender","birth date","list patients","flatten","ehr"]`,
			"vgi.result_columns_schema": `[` +
				`{"name":"id","type":"VARCHAR","description":"Logical id of the Patient resource."},` +
				`{"name":"family","type":"VARCHAR","description":"Family (last) name from the first name entry."},` +
				`{"name":"given","type":"VARCHAR","description":"Given (first) name(s) from the first name entry."},` +
				`{"name":"gender","type":"VARCHAR","description":"Administrative gender: male, female, other, or unknown."},` +
				`{"name":"birth_date","type":"VARCHAR","description":"Date of birth (YYYY-MM-DD or a partial FHIR date)."},` +
				`{"name":"active","type":"BOOLEAN","description":"Whether the Patient record is in active use."},` +
				`{"name":"raw","type":"VARCHAR","description":"The full Patient resource as a JSON string."}` +
				`]`,
			// VGI515 described examples (see fhir_search): the native examples
			// carrier drops descriptions, so re-supply them as a described list.
			"vgi.example_queries": `[` +
				`{"description":"List Patients with their core demographic fields flattened into columns.",` +
				`"sql":"SELECT id, family, given, gender, birth_date FROM fhir.main.fhir_patients('https://r4.smarthealthit.org') LIMIT 20;"},` +
				`{"description":"List Patients with family name Smith on the public HAPI R4 server.",` +
				`"sql":"SELECT id, family, given, birth_date FROM fhir.main.fhir_patients('https://hapi.fhir.org/baseR4', query := 'family=Smith') LIMIT 20;"}` +
				`]`,
		},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT id, family, given, gender, birth_date FROM fhir.main.fhir_patients('https://r4.smarthealthit.org') LIMIT 20;",
				Description: "List Patients with their core demographic fields flattened into columns.",
			},
			{
				SQL:         "SELECT id, family, given, birth_date FROM fhir.main.fhir_patients('https://hapi.fhir.org/baseR4', query := 'family=Smith') LIMIT 20;",
				Description: "List Patients with family name Smith on the public HAPI R4 server.",
			},
		},
	}
}

func (f *PatientsFunction) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(patientsArgs{}) }

func (f *PatientsFunction) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(patientsSchema)
}

func (f *PatientsFunction) NewState(params *vgi.ProcessParams) (*patientsState, error) {
	var args patientsArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if isNullArg(params.Args, 0) {
		return &patientsState{}, nil
	}
	// No network I/O here: seed the cursor at the first page URL and let
	// Process fetch it, so the scan's first batch costs one round-trip.
	cur, err := newPageCursor[Patient](
		optsFrom(args.BaseURL, args.Token, args.Query, args.Count), "Patient")
	if err != nil {
		return nil, err
	}
	return &patientsState{cur}, nil
}

func (f *PatientsFunction) Process(_ context.Context, _ *vgi.ProcessParams, state *patientsState, out *vgirpc.OutputCollector) error {
	p, done, err := state.nextSlice(state.fetchPage)
	if err != nil {
		return err
	}
	if done {
		return out.Finish()
	}
	n := int64(len(p))
	batch := array.NewRecordBatch(patientsSchema, []arrow.Array{
		vgi.BuildStringArray(n, func(i int64) string { return p[i].ID }),
		vgi.BuildStringArray(n, func(i int64) string { return p[i].Family }),
		vgi.BuildStringArray(n, func(i int64) string { return p[i].Given }),
		vgi.BuildStringArray(n, func(i int64) string { return p[i].Gender }),
		vgi.BuildStringArray(n, func(i int64) string { return p[i].BirthDate }),
		vgi.BuildBooleanArray(n, func(i int64) bool { return p[i].Active }),
		vgi.BuildStringArray(n, func(i int64) string { return p[i].Raw }),
	}, n)
	defer batch.Release()
	return out.Emit(batch)
}

// NewPatientsFunction builds the registerable table function.
func NewPatientsFunction() vgi.TableFunction {
	return vgi.AsTableFunction[patientsState](&PatientsFunction{})
}

// ---------------------------------------------------------------------------
// fhir_observations(base_url) ->
//   (id, status, code, code_display VARCHAR, value DOUBLE, unit, effective,
//    subject, raw VARCHAR)
// ---------------------------------------------------------------------------

var observationsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.BinaryTypes.String},
	{Name: "status", Type: arrow.BinaryTypes.String},
	{Name: "code", Type: arrow.BinaryTypes.String},
	{Name: "code_display", Type: arrow.BinaryTypes.String},
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	{Name: "unit", Type: arrow.BinaryTypes.String},
	{Name: "effective", Type: arrow.BinaryTypes.String},
	{Name: "subject", Type: arrow.BinaryTypes.String},
	{Name: "raw", Type: arrow.BinaryTypes.String},
}, nil)

type observationsArgs struct {
	BaseURL string `vgi:"pos=0,name=base_url,doc=FHIR R4 service base URL"`
	Query   string `vgi:"default=,doc=Raw FHIR search query string appended to the request, such as a status or LOINC code filter"`
	Token   string `vgi:"default=,doc=OAuth bearer token"`
	Count   int64  `vgi:"default=50,doc=Page size (_count search parameter)"`
}

type observationsState struct {
	PageCursor[Observation]
}

// fetchPage reads one Bundle page of the Observation search and flattens it.
func (s *observationsState) fetchPage(pageURL string) ([]Observation, string, error) {
	raws, next, err := SearchPage(context.Background(), FetchOptions{Token: s.Token}, pageURL)
	if err != nil {
		return nil, "", err
	}
	rows, err := FlattenObservations(raws)
	if err != nil {
		return nil, "", err
	}
	return rows, next, nil
}

// ObservationsFunction lists Observations with core fields flattened.
type ObservationsFunction struct{}

var _ vgi.TypedTableFunc[observationsState] = (*ObservationsFunction)(nil)

func (f *ObservationsFunction) Name() string { return "fhir_observations" }

func (f *ObservationsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "List FHIR R4 Observations (valueQuantity flattened; non-numeric values NULL; raw = full JSON)",
		Stability:   vgi.StabilityVolatile,
		Categories:  []string{"fhir", "healthcare"},
		Tags: map[string]string{
			"vgi.title":    "List FHIR Observation Resources",
			"vgi.category": "Clinical Data",
			"vgi.doc_llm":  "List FHIR R4 Observation resources with their valueQuantity flattened into numeric value plus unit, alongside status, code, display, effective time, and subject reference. Non-numeric observations surface a NULL value. Follows Bundle next links and accepts an optional search query.",
			"vgi.doc_md": "## fhir_observations\n\n" +
				"List [Observation](https://hl7.org/fhir/R4/observation.html) resources " +
				"from a FHIR R4 server with the `valueQuantity` flattened into a numeric " +
				"`value` plus `unit`, alongside status, code, display, effective time, " +
				"and subject reference.\n\n" +
				"### Usage\n\n" +
				"Call it with a FHIR R4 base URL to list Observations with their " +
				"numeric `value` and `unit` flattened out; runnable statements are " +
				"attached as this function's examples.\n\n" +
				"Narrow to a single LOINC code with a raw `query` such as the heart-rate " +
				"code, and keep only rows whose `value` is not null for numeric " +
				"measurements.\n\n" +
				"### Notes\n\n" +
				"- Observations without a numeric `valueQuantity` (e.g. coded or " +
				"string results) surface a **NULL** `value` rather than `0`.\n" +
				"- The `raw` column preserves the full Observation JSON, including " +
				"components and reference ranges.",
			"vgi.keywords": `["fhir","observations","observation","vital signs","loinc","value","unit","measurement","lab","clinical"]`,
			"vgi.result_columns_schema": `[` +
				`{"name":"id","type":"VARCHAR","description":"Logical id of the Observation resource."},` +
				`{"name":"status","type":"VARCHAR","description":"Observation status such as final, preliminary, or amended."},` +
				`{"name":"code","type":"VARCHAR","description":"Primary observation code (e.g. the LOINC code from code.coding[0])."},` +
				`{"name":"code_display","type":"VARCHAR","description":"Human-readable display text for the code."},` +
				`{"name":"value","type":"DOUBLE","description":"Numeric valueQuantity.value; NULL for non-numeric observations."},` +
				`{"name":"unit","type":"VARCHAR","description":"Unit of the value (valueQuantity.unit)."},` +
				`{"name":"effective","type":"VARCHAR","description":"Effective date/time of the observation."},` +
				`{"name":"subject","type":"VARCHAR","description":"Reference to the subject (usually Patient/{id})."},` +
				`{"name":"raw","type":"VARCHAR","description":"The full Observation resource as a JSON string."}` +
				`]`,
			// VGI515 described examples (see fhir_search): the native examples
			// carrier drops descriptions, so re-supply them as a described list.
			"vgi.example_queries": `[` +
				`{"description":"List numeric Observations with their code, value, and unit.",` +
				`"sql":"SELECT code, code_display, value, unit FROM fhir.main.fhir_observations('https://r4.smarthealthit.org') WHERE value IS NOT NULL LIMIT 20;"},` +
				`{"description":"List body-weight Observations (LOINC 29463-7) on the public HAPI R4 server with their value, unit, and effective time.",` +
				`"sql":"SELECT id, value, unit, effective FROM fhir.main.fhir_observations('https://hapi.fhir.org/baseR4', query := 'code=29463-7') LIMIT 20;"}` +
				`]`,
		},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT code, code_display, value, unit FROM fhir.main.fhir_observations('https://r4.smarthealthit.org') WHERE value IS NOT NULL LIMIT 20;",
				Description: "List numeric Observations with their code, value, and unit.",
			},
			{
				SQL:         "SELECT id, value, unit, effective FROM fhir.main.fhir_observations('https://hapi.fhir.org/baseR4', query := 'code=29463-7') LIMIT 20;",
				Description: "List body-weight Observations (LOINC 29463-7) on the public HAPI R4 server with their value, unit, and effective time.",
			},
		},
	}
}

func (f *ObservationsFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(observationsArgs{})
}

func (f *ObservationsFunction) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(observationsSchema)
}

func (f *ObservationsFunction) NewState(params *vgi.ProcessParams) (*observationsState, error) {
	var args observationsArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if isNullArg(params.Args, 0) {
		return &observationsState{}, nil
	}
	// No network I/O here: seed the cursor at the first page URL and let
	// Process fetch it, so the scan's first batch costs one round-trip.
	cur, err := newPageCursor[Observation](
		optsFrom(args.BaseURL, args.Token, args.Query, args.Count), "Observation")
	if err != nil {
		return nil, err
	}
	return &observationsState{cur}, nil
}

func (f *ObservationsFunction) Process(_ context.Context, _ *vgi.ProcessParams, state *observationsState, out *vgirpc.OutputCollector) error {
	o, done, err := state.nextSlice(state.fetchPage)
	if err != nil {
		return err
	}
	if done {
		return out.Finish()
	}
	n := int64(len(o))
	batch := array.NewRecordBatch(observationsSchema, []arrow.Array{
		vgi.BuildStringArray(n, func(i int64) string { return o[i].ID }),
		vgi.BuildStringArray(n, func(i int64) string { return o[i].Status }),
		vgi.BuildStringArray(n, func(i int64) string { return o[i].Code }),
		vgi.BuildStringArray(n, func(i int64) string { return o[i].CodeDisplay }),
		buildNullableValue(o),
		vgi.BuildStringArray(n, func(i int64) string { return o[i].Unit }),
		vgi.BuildStringArray(n, func(i int64) string { return o[i].Effective }),
		vgi.BuildStringArray(n, func(i int64) string { return o[i].Subject }),
		vgi.BuildStringArray(n, func(i int64) string { return o[i].Raw }),
	}, n)
	defer batch.Release()
	return out.Emit(batch)
}

// buildNullableValue builds a Float64 array where a nil Value yields SQL NULL,
// so a non-numeric Observation surfaces a NULL value rather than 0.
func buildNullableValue(rows []Observation) arrow.Array {
	b := array.NewFloat64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Reserve(len(rows))
	for _, r := range rows {
		if r.Value == nil {
			b.AppendNull()
		} else {
			b.Append(*r.Value)
		}
	}
	return b.NewArray()
}

// NewObservationsFunction builds the registerable table function.
func NewObservationsFunction() vgi.TableFunction {
	return vgi.AsTableFunction[observationsState](&ObservationsFunction{})
}

// ---------------------------------------------------------------------------
// fhir_capabilities(base_url) -> (resource_type VARCHAR, interactions VARCHAR[])
// ---------------------------------------------------------------------------

var capabilitiesSchema = arrow.NewSchema([]arrow.Field{
	{Name: "resource_type", Type: arrow.BinaryTypes.String},
	{Name: "interactions", Type: arrow.ListOf(arrow.BinaryTypes.String)},
}, nil)

type capabilitiesArgs struct {
	BaseURL string `vgi:"pos=0,name=base_url,doc=FHIR R4 service base URL"`
	Token   string `vgi:"default=,doc=OAuth bearer token"`
}

type capabilitiesState struct {
	Cursor[CapabilityResource]
}

// CapabilitiesFunction parses the server CapabilityStatement.
type CapabilitiesFunction struct{}

var _ vgi.TypedTableFunc[capabilitiesState] = (*CapabilitiesFunction)(nil)

func (f *CapabilitiesFunction) Name() string { return "fhir_capabilities" }

func (f *CapabilitiesFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "List the resource types and REST interactions from a FHIR R4 server's CapabilityStatement (/metadata)",
		Stability:   vgi.StabilityVolatile,
		Categories:  []string{"fhir", "healthcare"},
		Tags: map[string]string{
			"vgi.title":    "Read FHIR Server Capabilities",
			"vgi.category": "Server Discovery",
			"vgi.doc_llm":  "Read a FHIR R4 server's CapabilityStatement (/metadata) and list each resource type it exposes together with the REST interactions supported for that type (read, search-type, create, ...). Use to discover what an EHR/FHIR endpoint can do before querying it.",
			"vgi.doc_md": "## fhir_capabilities\n\n" +
				"Read a server's " +
				"[CapabilityStatement](https://hl7.org/fhir/R4/capabilitystatement.html) " +
				"(`GET /metadata`) and list each resource type it exposes together with " +
				"the REST interactions supported for that type.\n\n" +
				"### Usage\n\n" +
				"Call it with a FHIR R4 base URL to list every resource type the " +
				"server exposes and the REST interactions it supports for each; " +
				"runnable statements are attached as this function's examples.\n\n" +
				"### Notes\n\n" +
				"- `interactions` is a `VARCHAR[]`; `UNNEST` it to get one row per " +
				"(resource type, interaction) pair.\n" +
				"- Use this first to discover what an unfamiliar EHR/FHIR endpoint " +
				"supports before querying specific resource types.",
			"vgi.keywords": `["fhir","capabilities","capabilitystatement","metadata","supported resources","rest interactions","conformance","server discovery"]`,
			"vgi.result_columns_schema": `[` +
				`{"name":"resource_type","type":"VARCHAR","description":"A FHIR resource type the server exposes (e.g. Patient)."},` +
				`{"name":"interactions","type":"VARCHAR[]","description":"REST interaction codes supported for that type (e.g. read, search-type, create)."}` +
				`]`,
			"vgi.executable_examples": executableExamples,
			// VGI515 described examples (see fhir_search): the native examples
			// carrier drops descriptions, so re-supply them as a described list.
			"vgi.example_queries": `[` +
				`{"description":"List every resource type a FHIR server supports and the REST interactions available for each.",` +
				`"sql":"SELECT resource_type, interactions FROM fhir.main.fhir_capabilities('https://hapi.fhir.org/baseR4');"},` +
				`{"description":"Expand the supported REST interactions to one row per (resource type, interaction).",` +
				`"sql":"SELECT resource_type, UNNEST(interactions) AS interaction FROM fhir.main.fhir_capabilities('https://r4.smarthealthit.org');"}` +
				`]`,
		},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT resource_type, interactions FROM fhir.main.fhir_capabilities('https://hapi.fhir.org/baseR4');",
				Description: "List every resource type a FHIR server supports and the REST interactions available for each.",
			},
			{
				SQL:         "SELECT resource_type, UNNEST(interactions) AS interaction FROM fhir.main.fhir_capabilities('https://r4.smarthealthit.org');",
				Description: "Expand the supported REST interactions to one row per (resource type, interaction).",
			},
		},
	}
}

func (f *CapabilitiesFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(capabilitiesArgs{})
}

func (f *CapabilitiesFunction) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(capabilitiesSchema)
}

func (f *CapabilitiesFunction) NewState(params *vgi.ProcessParams) (*capabilitiesState, error) {
	var args capabilitiesArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if isNullArg(params.Args, 0) {
		return &capabilitiesState{}, nil
	}
	res, err := FetchCapabilities(context.Background(),
		optsFrom(args.BaseURL, args.Token, "", 0))
	if err != nil {
		return nil, err
	}
	return &capabilitiesState{Cursor[CapabilityResource]{Rows: res}}, nil
}

func (f *CapabilitiesFunction) Process(_ context.Context, _ *vgi.ProcessParams, state *capabilitiesState, out *vgirpc.OutputCollector) error {
	r, done := state.nextSlice()
	if done {
		return out.Finish()
	}
	n := int64(len(r))
	batch := array.NewRecordBatch(capabilitiesSchema, []arrow.Array{
		vgi.BuildStringArray(n, func(i int64) string { return r[i].ResourceType }),
		buildStringListArray(n, func(i int64) []string { return r[i].Interactions }),
	}, n)
	defer batch.Release()
	return out.Emit(batch)
}

// NewCapabilitiesFunction builds the registerable table function.
func NewCapabilitiesFunction() vgi.TableFunction {
	return vgi.AsTableFunction[capabilitiesState](&CapabilitiesFunction{})
}

// serversViewDefinition backs the fhir_servers browsable registry view. It is a
// pure VALUES relation, so it scans with no arguments and no network access or
// credentials — an agent can list it to discover a real base URL to hand to the
// table functions (which take that base URL as their first argument). Every row
// is a well-known, publicly reachable FHIR R4 endpoint.
const serversViewDefinition = `SELECT name, base_url, fhir_version, requires_auth, description FROM (VALUES ` +
	`('HAPI FHIR R4 Public Test Server', 'https://hapi.fhir.org/baseR4', '4.0.1', false, ` +
	`'Open, unauthenticated public HAPI reference server hosting a large shared R4 dataset; ideal for exploration and demos.'), ` +
	`('Firely Public FHIR R4 Server', 'https://server.fire.ly', '4.0.1', false, ` +
	`'Firely public FHIR R4 test server (no authentication); a useful second endpoint for cross-server checks.'), ` +
	`('SMART Health IT R4 Sandbox', 'https://r4.smarthealthit.org', '4.0.1', false, ` +
	`'SMART Health IT open R4 sandbox seeded with synthetic Synthea patient data.'), ` +
	`('HL7 Terminology Server (R4)', 'https://tx.fhir.org/r4', '4.0.1', false, ` +
	`'HL7 public R4 terminology server exposing CodeSystem, ValueSet, and related conformance resources.')` +
	`) AS t(name, base_url, fhir_version, requires_auth, description)`

// registerServersView registers the fhir_servers browsable directory view: a
// static, credential-free list of public FHIR R4 endpoints that gives agents a
// concrete data surface (and a real base_url to pass to the table functions)
// without having to guess an endpoint first.
func registerServersView(w *vgi.Worker) {
	w.RegisterCatalogView("main", vgi.CatalogView{
		Name:       "fhir_servers",
		Definition: serversViewDefinition,
		Comment:    "Curated directory of public HL7 FHIR R4 endpoints and whether they require authentication.",
		ColumnComments: map[string]string{
			"name":          "Human-friendly name of the FHIR R4 server.",
			"base_url":      "Service base URL to pass as the first argument of every fhir_* table function.",
			"fhir_version":  "FHIR specification version the endpoint serves (all R4, 4.0.1).",
			"requires_auth": "Whether the endpoint requires an OAuth bearer token (all listed servers are open).",
			"description":   "What the endpoint is and what data it holds.",
		},
		Tags: map[string]string{
			"vgi.title":    "Public FHIR Servers",
			"vgi.category": "Server Discovery",
			"vgi.doc_llm": "A static, credential-free directory of well-known public HL7 FHIR R4 " +
				"endpoints. Each row pairs a human name with the service base_url to pass as the " +
				"first argument of the fhir_patients / fhir_observations / fhir_search / fhir_read / " +
				"fhir_capabilities table functions, the FHIR version served, whether the endpoint " +
				"needs authentication, and a short description. Query this first to discover a base " +
				"URL to point the other functions at, without having to know an endpoint up front.",
			"vgi.doc_md": "## fhir_servers\n\n" +
				"A curated, credential-free directory of well-known **public HL7 FHIR R4** " +
				"endpoints. It is a static list (no network access), so it always returns instantly " +
				"and is the natural starting point for discovery: pick a `base_url` here and hand it " +
				"to any `fhir_*` table function as its first argument.\n\n" +
				"### Columns\n\n" +
				"- `name` — human-friendly server name.\n" +
				"- `base_url` — the FHIR R4 service base URL (e.g. `https://hapi.fhir.org/baseR4`).\n" +
				"- `fhir_version` — the FHIR version served (all `4.0.1`).\n" +
				"- `requires_auth` — whether an OAuth bearer token is needed (all listed servers are open).\n" +
				"- `description` — what the endpoint is and what data it holds.",
			"vgi.keywords": `["fhir","fhir r4","servers","endpoints","directory","registry",` +
				`"base url","public test server","hapi","discovery"]`,
			"domain":   "healthcare",
			"category": "interoperability",
			"topic":    "fhir-r4-rest",
			"vgi.example_queries": `[` +
				`{"description":"List the curated public FHIR R4 test servers and whether each needs authentication.",` +
				`"sql":"SELECT name, base_url, requires_auth FROM fhir.main.fhir_servers ORDER BY name"}` +
				`]`,
		},
	})
}

// Register registers all FHIR table functions and the browsable directory view
// on the worker.
func Register(w *vgi.Worker) {
	w.RegisterTable(NewSearchFunction())
	w.RegisterTable(NewReadFunction())
	w.RegisterTable(NewPatientsFunction())
	w.RegisterTable(NewObservationsFunction())
	w.RegisterTable(NewCapabilitiesFunction())
	registerServersView(w)
}

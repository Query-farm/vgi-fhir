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
// arrow.Record, no interfaces, channels, funcs, or unexported fields. Each
// table function fetches its rows eagerly in NewState, stores plain exported Go
// slices, and rebuilds the Arrow batch in Process.
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

// sourceBase is the canonical GitHub blob URL for the file implementing every
// table function in this package; per-object vgi.source_url tags point at it.
const sourceBase = "https://github.com/Query-farm/vgi-fhir/blob/main/internal/fhirworker/functions.go"

// The five standard per-object discovery/description tags the vgi-lint strict
// profile expects on every function — vgi.title (VGI124), vgi.doc_llm
// (VGI112), vgi.doc_md (VGI113), vgi.keywords (VGI126), and
// vgi.source_url (VGI128) — are set inline in each function's Metadata().Tags
// (all point vgi.source_url at sourceBase). Each vgi.title is a multi-word human
// display name so it does not normalize-equal the machine function name (VGI125).

// executableExamples (VGI509) is a JSON list of self-contained, catalog-qualified
// examples an agent can run as-is against an attached worker. The base URL points
// at the public HAPI FHIR R4 test server; expected_result is omitted on purpose.
const executableExamples = `[
  {
    "description": "List Patients with core demographic fields flattened into columns.",
    "sql": "SELECT id, family, given, gender, birth_date FROM fhir.main.fhir_patients('https://hapi.fhir.org/baseR4')"
  },
  {
    "description": "List numeric Observations with their code, value, and unit.",
    "sql": "SELECT code, code_display, value, unit FROM fhir.main.fhir_observations('https://hapi.fhir.org/baseR4') WHERE value IS NOT NULL"
  },
  {
    "description": "Search Condition resources and return their logical ids.",
    "sql": "SELECT id FROM fhir.main.fhir_search('https://hapi.fhir.org/baseR4', 'Condition')"
  },
  {
    "description": "Expand a server's supported resource types and REST interactions.",
    "sql": "SELECT resource_type, UNNEST(interactions) AS interaction FROM fhir.main.fhir_capabilities('https://hapi.fhir.org/baseR4')"
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
	Cursor[Resource]
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
			"vgi.title":   "Search FHIR Resource Type",
			"vgi.doc_llm": "Search a single FHIR R4 resource type via the server's search endpoint, following Bundle next links across pages. Returns one row per matched resource with its logical id and the full resource as a JSON string. Use for any resource type (Patient, Observation, Condition, ...) with an optional raw search query and page size.",
			"vgi.doc_md": "## fhir_search\n\n" +
				"Run a FHIR R4 [search](https://hl7.org/fhir/R4/search.html) over a " +
				"single resource type and return one row per matched resource.\n\n" +
				"### Usage\n\n" +
				"```sql\n" +
				"SELECT id, resource\n" +
				"FROM fhir.main.fhir_search('https://hapi.fhir.org/baseR4', 'Condition');\n" +
				"```\n\n" +
				"Pass a raw `query` (e.g. `name=smith&_count=50`) and/or `count` to " +
				"refine the search, and `token` for protected servers.\n\n" +
				"### Notes\n\n" +
				"- Follows Bundle `next` links across pages until exhausted or the " +
				"safety cap is reached.\n" +
				"- The `resource` column holds the complete resource JSON, so any " +
				"field not surfaced by the flattened functions is still reachable via " +
				"`json_extract`.",
			"vgi.keywords":   "fhir search, search resources, resource type, bundle, paging, query, patient, observation, condition",
			"vgi.source_url": sourceBase,
			"vgi.result_columns_md": "| column | type | description |\n" +
				"|---|---|---|\n" +
				"| `id` | VARCHAR | Logical id of the matched FHIR resource. |\n" +
				"| `resource` | VARCHAR | The full resource as a JSON string. |",
		},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT id FROM fhir.main.fhir_search('https://hapi.fhir.org/baseR4', 'Condition');",
				Description: "List the ids of all Condition resources on a FHIR R4 server (paging through Bundle next links).",
			},
			{
				SQL:         "SELECT id, resource FROM fhir.main.fhir_search('https://hapi.fhir.org/baseR4', 'Patient', query := 'name=smith', count := 100);",
				Description: "Search Patients whose name contains 'smith', 100 per page, returning id plus the raw resource JSON.",
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
	res, err := SearchResources(context.Background(),
		optsFrom(args.BaseURL, args.Token, args.Query, args.Count), args.ResourceType)
	if err != nil {
		return nil, err
	}
	return &searchState{Cursor[Resource]{Rows: res}}, nil
}

func (f *SearchFunction) Process(_ context.Context, _ *vgi.ProcessParams, state *searchState, out *vgirpc.OutputCollector) error {
	r, done := state.nextSlice()
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
			"vgi.title":   "Read FHIR Resource By Id",
			"vgi.doc_llm": "Read a single FHIR R4 resource by its resource type and logical id (a GET on /{type}/{id}). Returns one row with the full resource as a JSON string; a missing resource surfaces as an error.",
			"vgi.doc_md": "## fhir_read\n\n" +
				"Read a single FHIR R4 resource by its type and logical id — a direct " +
				"`GET /{type}/{id}` against the server (the FHIR " +
				"[read](https://hl7.org/fhir/R4/http.html#read) interaction).\n\n" +
				"### Usage\n\n" +
				"```sql\n" +
				"SELECT resource\n" +
				"FROM fhir.main.fhir_read('https://hapi.fhir.org/baseR4', 'Patient', 'example');\n" +
				"```\n\n" +
				"### Notes\n\n" +
				"- Returns exactly one row when the resource exists; a missing " +
				"resource (HTTP 404) surfaces as a query error rather than an empty " +
				"result.\n" +
				"- Supply `token` for endpoints that require authentication.",
			"vgi.keywords":   "fhir read, read resource, get by id, single resource, logical id, resource json",
			"vgi.source_url": sourceBase,
			"vgi.result_columns_md": "| column | type | description |\n" +
				"|---|---|---|\n" +
				"| `resource` | VARCHAR | The requested resource as a JSON string. |",
		},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT resource FROM fhir.main.fhir_read('https://hapi.fhir.org/baseR4', 'Patient', 'example');",
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
	Cursor[Patient]
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
			"vgi.title":   "List FHIR Patient Resources",
			"vgi.doc_llm": "List FHIR R4 Patient resources with their core demographic fields (family/given name, gender, birth date, active) flattened into columns, plus the full Patient resource as JSON. Follows Bundle next links and accepts an optional raw search query.",
			"vgi.doc_md": "## fhir_patients\n\n" +
				"List [Patient](https://hl7.org/fhir/R4/patient.html) resources from a " +
				"FHIR R4 server with the most-used demographic fields flattened into " +
				"columns, while still returning the full resource JSON.\n\n" +
				"### Usage\n\n" +
				"```sql\n" +
				"SELECT id, family, given, gender, birth_date\n" +
				"FROM fhir.main.fhir_patients('https://hapi.fhir.org/baseR4');\n" +
				"```\n\n" +
				"Filter server-side with a raw `query` (e.g. `family=smith`), set the " +
				"page size with `count`, and authenticate with `token`.\n\n" +
				"### Notes\n\n" +
				"- One row per Patient across all Bundle pages (`next` links are " +
				"followed automatically).\n" +
				"- The `raw` column carries the untouched Patient JSON for fields not " +
				"flattened here.",
			"vgi.keywords":   "fhir patients, patient, demographics, name, gender, birth date, list patients, flatten, ehr",
			"vgi.source_url": sourceBase,
			"vgi.result_columns_md": "| column | type | description |\n" +
				"|---|---|---|\n" +
				"| `id` | VARCHAR | Logical id of the Patient resource. |\n" +
				"| `family` | VARCHAR | Family (last) name from the first `name` entry. |\n" +
				"| `given` | VARCHAR | Given (first) name(s) from the first `name` entry. |\n" +
				"| `gender` | VARCHAR | Administrative gender (`male`, `female`, `other`, `unknown`). |\n" +
				"| `birth_date` | VARCHAR | Date of birth (`YYYY-MM-DD` or partial FHIR date). |\n" +
				"| `active` | BOOLEAN | Whether the Patient record is in active use. |\n" +
				"| `raw` | VARCHAR | The full Patient resource as a JSON string. |",
		},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT id, family, given, gender, birth_date FROM fhir.main.fhir_patients('https://hapi.fhir.org/baseR4');",
				Description: "List Patients with their core demographic fields flattened into columns.",
			},
			{
				SQL:         "SELECT count(*) FROM fhir.main.fhir_patients('https://hapi.fhir.org/baseR4', query := 'family=smith');",
				Description: "Count Patients whose family name is 'smith' (across all Bundle pages).",
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
	patients, err := FetchPatients(context.Background(),
		optsFrom(args.BaseURL, args.Token, args.Query, args.Count))
	if err != nil {
		return nil, err
	}
	return &patientsState{Cursor[Patient]{Rows: patients}}, nil
}

func (f *PatientsFunction) Process(_ context.Context, _ *vgi.ProcessParams, state *patientsState, out *vgirpc.OutputCollector) error {
	p, done := state.nextSlice()
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
	Query   string `vgi:"default=,doc=Raw search query string (e.g. code=8867-4)"`
	Token   string `vgi:"default=,doc=OAuth bearer token"`
	Count   int64  `vgi:"default=50,doc=Page size (_count search parameter)"`
}

type observationsState struct {
	Cursor[Observation]
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
			"vgi.title":   "List FHIR Observation Resources",
			"vgi.doc_llm": "List FHIR R4 Observation resources with their valueQuantity flattened into numeric value plus unit, alongside status, code, display, effective time, and subject reference. Non-numeric observations surface a NULL value. Follows Bundle next links and accepts an optional search query.",
			"vgi.doc_md": "## fhir_observations\n\n" +
				"List [Observation](https://hl7.org/fhir/R4/observation.html) resources " +
				"from a FHIR R4 server with the `valueQuantity` flattened into a numeric " +
				"`value` plus `unit`, alongside status, code, display, effective time, " +
				"and subject reference.\n\n" +
				"### Usage\n\n" +
				"```sql\n" +
				"SELECT code, code_display, value, unit\n" +
				"FROM fhir.main.fhir_observations('https://hapi.fhir.org/baseR4')\n" +
				"WHERE value IS NOT NULL;\n" +
				"```\n\n" +
				"Narrow to a single LOINC code with a raw `query` (e.g. `code=8867-4` " +
				"for heart rate).\n\n" +
				"### Notes\n\n" +
				"- Observations without a numeric `valueQuantity` (e.g. coded or " +
				"string results) surface a **NULL** `value` rather than `0`.\n" +
				"- The `raw` column preserves the full Observation JSON, including " +
				"components and reference ranges.",
			"vgi.keywords":   "fhir observations, observation, vital signs, loinc, value, unit, measurement, lab, clinical",
			"vgi.source_url": sourceBase,
			"vgi.result_columns_md": "| column | type | description |\n" +
				"|---|---|---|\n" +
				"| `id` | VARCHAR | Logical id of the Observation resource. |\n" +
				"| `status` | VARCHAR | Observation status (`final`, `preliminary`, `amended`, …). |\n" +
				"| `code` | VARCHAR | Primary observation code (e.g. LOINC code from `code.coding[0]`). |\n" +
				"| `code_display` | VARCHAR | Human-readable display text for the code. |\n" +
				"| `value` | DOUBLE | Numeric `valueQuantity.value`; NULL for non-numeric observations. |\n" +
				"| `unit` | VARCHAR | Unit of the value (`valueQuantity.unit`). |\n" +
				"| `effective` | VARCHAR | Effective date/time of the observation. |\n" +
				"| `subject` | VARCHAR | Reference to the subject (usually `Patient/{id}`). |\n" +
				"| `raw` | VARCHAR | The full Observation resource as a JSON string. |",
		},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT code, code_display, value, unit FROM fhir.main.fhir_observations('https://hapi.fhir.org/baseR4') WHERE value IS NOT NULL;",
				Description: "List numeric Observations with their code, value, and unit.",
			},
			{
				SQL:         "SELECT id, value, unit, effective FROM fhir.main.fhir_observations('https://hapi.fhir.org/baseR4', query := 'code=8867-4');",
				Description: "List heart-rate Observations (LOINC 8867-4) with their value, unit, and effective time.",
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
	obs, err := FetchObservations(context.Background(),
		optsFrom(args.BaseURL, args.Token, args.Query, args.Count))
	if err != nil {
		return nil, err
	}
	return &observationsState{Cursor[Observation]{Rows: obs}}, nil
}

func (f *ObservationsFunction) Process(_ context.Context, _ *vgi.ProcessParams, state *observationsState, out *vgirpc.OutputCollector) error {
	o, done := state.nextSlice()
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
			"vgi.title":   "Read FHIR Server Capabilities",
			"vgi.doc_llm": "Read a FHIR R4 server's CapabilityStatement (/metadata) and list each resource type it exposes together with the REST interactions supported for that type (read, search-type, create, ...). Use to discover what an EHR/FHIR endpoint can do before querying it.",
			"vgi.doc_md": "## fhir_capabilities\n\n" +
				"Read a server's " +
				"[CapabilityStatement](https://hl7.org/fhir/R4/capabilitystatement.html) " +
				"(`GET /metadata`) and list each resource type it exposes together with " +
				"the REST interactions supported for that type.\n\n" +
				"### Usage\n\n" +
				"```sql\n" +
				"SELECT resource_type, UNNEST(interactions) AS interaction\n" +
				"FROM fhir.main.fhir_capabilities('https://hapi.fhir.org/baseR4');\n" +
				"```\n\n" +
				"### Notes\n\n" +
				"- `interactions` is a `VARCHAR[]`; `UNNEST` it to get one row per " +
				"(resource type, interaction) pair.\n" +
				"- Use this first to discover what an unfamiliar EHR/FHIR endpoint " +
				"supports before querying specific resource types.",
			"vgi.keywords":   "fhir capabilities, capabilitystatement, metadata, supported resources, rest interactions, conformance, server discovery",
			"vgi.source_url": sourceBase,
			"vgi.result_columns_md": "| column | type | description |\n" +
				"|---|---|---|\n" +
				"| `resource_type` | VARCHAR | A FHIR resource type the server exposes (e.g. `Patient`). |\n" +
				"| `interactions` | VARCHAR[] | REST interaction codes supported for that type (e.g. `read`, `search-type`, `create`). |",
			"vgi.executable_examples": executableExamples,
		},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT resource_type, interactions FROM fhir.main.fhir_capabilities('https://hapi.fhir.org/baseR4');",
				Description: "List every resource type a FHIR server supports and the REST interactions available for each.",
			},
			{
				SQL:         "SELECT resource_type, UNNEST(interactions) AS interaction FROM fhir.main.fhir_capabilities('https://hapi.fhir.org/baseR4');",
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

// Register registers all FHIR table functions on the worker.
func Register(w *vgi.Worker) {
	w.RegisterTable(NewSearchFunction())
	w.RegisterTable(NewReadFunction())
	w.RegisterTable(NewPatientsFunction())
	w.RegisterTable(NewObservationsFunction())
	w.RegisterTable(NewCapabilitiesFunction())
}

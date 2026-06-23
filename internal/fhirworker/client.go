// Copyright 2026 Query Farm LLC - https://query.farm

package fhirworker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// FetchOptions carries the parameters shared by every FHIR request.
type FetchOptions struct {
	// BaseURL is the FHIR R4 service base (e.g. https://server/fhir).
	BaseURL string
	// Token is an optional OAuth bearer token. Empty means send no
	// Authorization header (many public/demo FHIR servers are open).
	Token string
	// Query is an optional raw search query string (e.g.
	// `name=smith&_count=50`). It is appended verbatim to the search URL.
	Query string
	// Count is the page size, sent as the `_count` search parameter. Values
	// <= 0 fall back to defaultPageSize.
	Count int64
	// MaxResults bounds how many resources are collected across all pages.
	// Values <= 0 fall back to defaultMaxResults.
	MaxResults int
	// Timeout bounds the whole multi-page fetch. Zero uses defaultTimeout.
	Timeout time.Duration
}

const (
	defaultPageSize   = 50
	defaultMaxResults = 1000
	defaultTimeout    = 30 * time.Second
	// maxPages guards against a server that keeps returning a next link.
	maxPages = 10000
)

// bundle models a FHIR R4 Bundle (searchset). Entries are kept as raw JSON so
// each table function flattens only the fields it cares about while still
// exposing the full resource.
type bundle struct {
	ResourceType string       `json:"resourceType"`
	Type         string       `json:"type"`
	Total        int          `json:"total"`
	Link         []bundleLink `json:"link"`
	Entry        []struct {
		FullURL  string          `json:"fullUrl"`
		Resource json.RawMessage `json:"resource"`
	} `json:"entry"`
}

type bundleLink struct {
	Relation string `json:"relation"`
	URL      string `json:"url"`
}

// httpClient is the shared HTTP client. FHIR is plain REST/JSON, so the stdlib
// client is all that is needed.
var httpClient = &http.Client{}

// SearchAll performs a paginated FHIR search GET against
// {BaseURL}/{resourceType}, following Bundle `next` links until every page has
// been read or maxResults is reached. It returns the raw JSON of each resource.
func SearchAll(ctx context.Context, opts FetchOptions, resourceType string) ([]json.RawMessage, error) {
	if opts.BaseURL == "" {
		return nil, fmt.Errorf("fhir: base_url is required")
	}
	if resourceType == "" {
		return nil, fmt.Errorf("fhir: resource_type is required")
	}
	count := opts.Count
	if count <= 0 {
		count = defaultPageSize
	}
	maxResults := opts.MaxResults
	if maxResults <= 0 {
		maxResults = defaultMaxResults
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	next, err := searchURL(opts.BaseURL, resourceType, opts.Query, count)
	if err != nil {
		return nil, err
	}

	var all []json.RawMessage
	for page := 0; next != ""; page++ {
		if page > maxPages {
			return nil, fmt.Errorf("fhir: aborting after too many pages searching %s (possible pagination loop)", resourceType)
		}
		body, err := doGet(ctx, next, opts.Token)
		if err != nil {
			return nil, err
		}
		var b bundle
		if err := json.Unmarshal(body, &b); err != nil {
			return nil, fmt.Errorf("fhir: decode Bundle from %s: %w", next, err)
		}
		for _, e := range b.Entry {
			if len(e.Resource) == 0 {
				continue
			}
			all = append(all, e.Resource)
			if len(all) >= maxResults {
				return all, nil
			}
		}
		next = nextLink(b.Link)
	}
	return all, nil
}

// ReadOne performs GET {BaseURL}/{resourceType}/{id} and returns the resource's
// raw JSON (validated as a single FHIR resource, not a Bundle).
func ReadOne(ctx context.Context, opts FetchOptions, resourceType, id string) (json.RawMessage, error) {
	if opts.BaseURL == "" {
		return nil, fmt.Errorf("fhir: base_url is required")
	}
	if resourceType == "" || id == "" {
		return nil, fmt.Errorf("fhir: resource_type and id are required")
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	endpoint := strings.TrimRight(opts.BaseURL, "/") + "/" +
		url.PathEscape(resourceType) + "/" + url.PathEscape(id)
	body, err := doGet(ctx, endpoint, opts.Token)
	if err != nil {
		return nil, err
	}
	var probe json.RawMessage
	if err := json.Unmarshal(body, &probe); err != nil {
		return nil, fmt.Errorf("fhir: decode resource from %s: %w", endpoint, err)
	}
	return probe, nil
}

// GetMetadata fetches and decodes the server CapabilityStatement at
// {BaseURL}/metadata.
func GetMetadata(ctx context.Context, opts FetchOptions) (json.RawMessage, error) {
	if opts.BaseURL == "" {
		return nil, fmt.Errorf("fhir: base_url is required")
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	endpoint := strings.TrimRight(opts.BaseURL, "/") + "/metadata"
	body, err := doGet(ctx, endpoint, opts.Token)
	if err != nil {
		return nil, err
	}
	var probe json.RawMessage
	if err := json.Unmarshal(body, &probe); err != nil {
		return nil, fmt.Errorf("fhir: decode CapabilityStatement from %s: %w", endpoint, err)
	}
	return probe, nil
}

// searchURL builds the initial search URL: {base}/{resourceType}?_count=N plus
// any raw query string the caller supplied.
func searchURL(base, resourceType, rawQuery string, count int64) (string, error) {
	endpoint := strings.TrimRight(base, "/") + "/" + url.PathEscape(resourceType)
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("fhir: invalid URL %q: %w", endpoint, err)
	}
	q := u.Query()
	// Merge the caller's raw query string (e.g. "name=smith&_count=50").
	if rawQuery != "" {
		extra, err := url.ParseQuery(rawQuery)
		if err != nil {
			return "", fmt.Errorf("fhir: invalid query %q: %w", rawQuery, err)
		}
		for k, vs := range extra {
			for _, v := range vs {
				q.Add(k, v)
			}
		}
	}
	// Only set _count if the caller didn't already specify one.
	if q.Get("_count") == "" {
		q.Set("_count", strconv.FormatInt(count, 10))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// nextLink returns the URL of the Bundle link with relation "next", or "".
func nextLink(links []bundleLink) string {
	for _, l := range links {
		if l.Relation == "next" {
			return l.URL
		}
	}
	return ""
}

// doGet performs a single GET, sets the bearer token when present, and turns a
// non-2xx response (including a FHIR OperationOutcome body) into a clean error.
func doGet(ctx context.Context, endpoint, token string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("fhir: build request: %w", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Accept", "application/fhir+json, application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fhir: GET %s: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 256<<20))
	if err != nil {
		return nil, fmt.Errorf("fhir: read response from %s: %w", endpoint, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("fhir: %s returned HTTP %d: %s",
			endpoint, resp.StatusCode, describeError(body))
	}
	return body, nil
}

// describeError extracts a human-readable message from a FHIR OperationOutcome
// error body when present, otherwise returns a short excerpt of the body.
func describeError(body []byte) string {
	var oo struct {
		ResourceType string `json:"resourceType"`
		Issue        []struct {
			Severity    string `json:"severity"`
			Code        string `json:"code"`
			Diagnostics string `json:"diagnostics"`
			Details     struct {
				Text string `json:"text"`
			} `json:"details"`
		} `json:"issue"`
	}
	if err := json.Unmarshal(body, &oo); err == nil && oo.ResourceType == "OperationOutcome" && len(oo.Issue) > 0 {
		var parts []string
		for _, is := range oo.Issue {
			msg := is.Diagnostics
			if msg == "" {
				msg = is.Details.Text
			}
			if msg == "" {
				msg = is.Code
			}
			parts = append(parts, fmt.Sprintf("%s: %s", is.Severity, msg))
		}
		return "OperationOutcome[" + strings.Join(parts, "; ") + "]"
	}
	return snippet(body)
}

// snippet returns a short, single-line excerpt of a response body.
func snippet(body []byte) string {
	s := strings.TrimSpace(string(body))
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	if s == "" {
		return "(empty body)"
	}
	return s
}

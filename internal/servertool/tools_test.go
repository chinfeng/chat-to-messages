package servertool

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/chinfeng/chat-to-messages/internal/config"
	"github.com/chinfeng/chat-to-messages/internal/dump"
)

func testCfg() config.ServerToolConfig {
	return config.ServerToolConfig{
		WebSearch: true, WebFetch: true,
		WebSearchEngine:          "brave",
		WebFetchMaxContentTokens: 5000,
	}
}

func TestExecuteWebSearchBrave(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Subscription-Token") != "BST-key" {
			t.Errorf("missing token: %v", r.Header)
		}
		if !strings.Contains(r.URL.Path, "/res/v1/web/search") {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"web":{"results":[
			{"url":"https://a.com","title":"A","description":"desc A","page_age":"2026"},
			{"url":"https://b.com","title":"B"}
		]}}`))
	}))
	defer srv.Close()
	cfg := testCfg()
	cfg.WebSearchBaseURL = srv.URL
	cfg.WebSearchAPIKey = "BST-key"
	results := ExecuteWebSearch(context.Background(), "test query", cfg, nil)
	if len(results) != 2 {
		t.Fatalf("results = %d", len(results))
	}
	if results[0].URL != "https://a.com" || results[0].Title != "A" || results[0].Snippet != "desc A" || results[0].PageAge != "2026" {
		t.Errorf("r0 = %+v", results[0])
	}
	if results[1].Snippet != "" {
		t.Errorf("r1 snippet should be empty: %+v", results[1])
	}
}

func TestExecuteWebSearchSearxng(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("q")
		if r.URL.Query().Get("format") != "json" {
			t.Errorf("format = %q", r.URL.Query().Get("format"))
		}
		w.Write([]byte(`{"results":[{"url":"https://s.com","title":"S"}]}`))
	}))
	defer srv.Close()
	cfg := testCfg()
	cfg.WebSearchEngine = "searxng"
	cfg.WebSearchBaseURL = srv.URL
	results := ExecuteWebSearch(context.Background(), "hello world", cfg, nil)
	if gotQuery != "hello world" {
		t.Errorf("query = %q", gotQuery)
	}
	if len(results) != 1 || results[0].URL != "https://s.com" {
		t.Fatalf("results = %+v", results)
	}
}

func TestExecuteWebSearchEmptyQueryAndNoKey(t *testing.T) {
	if r := ExecuteWebSearch(context.Background(), "  ", testCfg(), nil); len(r) != 0 {
		t.Error("empty query should return nothing")
	}
	cfg := testCfg() // brave, no key
	if r := ExecuteWebSearch(context.Background(), "q", cfg, nil); len(r) != 0 {
		t.Error("no key should return nothing")
	}
}

// TestExecuteWebSearchSkipLogs pins the skipped entries (empty query / no key):
// full fields per ServerToolLogEntry, no status/duration (TS omits them).
func TestExecuteWebSearchSkipLogs(t *testing.T) {
	var entries []dump.ServerToolLogEntry
	ExecuteWebSearch(context.Background(), "  ", testCfg(), func(e dump.ServerToolLogEntry) { entries = append(entries, e) })
	if len(entries) != 1 {
		t.Fatalf("entries = %d", len(entries))
	}
	e := entries[0]
	if e.Tool != "web_search" || e.Input != "  " || e.Engine != "brave" || !e.Skipped || e.SkipReason != "empty query" {
		t.Errorf("entry = %+v", e)
	}
	if e.Status != nil || e.DurationMs != nil || e.ResultCount != nil {
		t.Errorf("skipped entry must not carry status/duration/resultCount: %+v", e)
	}
	if e.Timestamp == "" {
		t.Error("timestamp missing")
	}

	entries = nil
	cfg := testCfg() // brave, no key
	ExecuteWebSearch(context.Background(), "q", cfg, func(e dump.ServerToolLogEntry) { entries = append(entries, e) })
	if len(entries) != 1 {
		t.Fatalf("entries = %d", len(entries))
	}
	e = entries[0]
	if e.Engine != "brave" || !e.Skipped || e.SkipReason != "no API key configured" {
		t.Errorf("entry = %+v", e)
	}
}

// TestExecuteWebSearchHTTPErrorLogs pins the non-2xx log entry: status,
// truncated response body, error prefix, resultCount 0, duration.
func TestExecuteWebSearchHTTPErrorLogs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("boom error body"))
	}))
	defer srv.Close()
	cfg := testCfg()
	cfg.WebSearchBaseURL = srv.URL
	cfg.WebSearchAPIKey = "k"
	var entries []dump.ServerToolLogEntry
	results := ExecuteWebSearch(context.Background(), "q", cfg, func(e dump.ServerToolLogEntry) { entries = append(entries, e) })
	if len(results) != 0 {
		t.Fatalf("results = %+v", results)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d", len(entries))
	}
	e := entries[0]
	if e.Status == nil || *e.Status != 500 {
		t.Errorf("status = %+v", e.Status)
	}
	if !strings.Contains(e.Error, "boom error body") {
		t.Errorf("error = %q", e.Error)
	}
	if e.ResponseBody != "boom error body" {
		t.Errorf("responseBody = %q", e.ResponseBody)
	}
	if e.ResultCount == nil || *e.ResultCount != 0 {
		t.Errorf("resultCount = %+v", e.ResultCount)
	}
	if e.DurationMs == nil || *e.DurationMs < 0 {
		t.Errorf("durationMs = %+v", e.DurationMs)
	}
	if e.RequestURL == "" || len(e.RequestHeaders) == 0 {
		t.Errorf("request url/headers not logged: %+v", e)
	}
}

func TestExecuteWebFetchDomainRules(t *testing.T) {
	cfg := testCfg()
	cfg.WebFetchAllowedDomains = []string{"docs.example.com"}
	cfg.WebFetchBlockedDomains = []string{"bad.example.com"}
	if r := ExecuteWebFetch(context.Background(), "https://evil.com/x", cfg, nil); r.StatusCode != 403 {
		t.Errorf("not allowed: %+v", r)
	}
	if r := ExecuteWebFetch(context.Background(), "https://bad.example.com/x", cfg, nil); r.StatusCode != 403 {
		t.Errorf("blocked: %+v", r)
	}
	if r := ExecuteWebFetch(context.Background(), "not a url", cfg, nil); r.StatusCode != 400 {
		t.Errorf("invalid url: %+v", r)
	}
}

// TestExecuteWebFetchBlockedLogs pins the blocked log entry (skipReason,
// request headers incl. the new-project User-Agent).
func TestExecuteWebFetchBlockedLogs(t *testing.T) {
	cfg := testCfg()
	cfg.WebFetchBlockedDomains = []string{"bad.example.com"}
	var entries []dump.ServerToolLogEntry
	r := ExecuteWebFetch(context.Background(), "https://bad.example.com/x", cfg, func(e dump.ServerToolLogEntry) { entries = append(entries, e) })
	if r.StatusCode != 403 {
		t.Fatalf("status = %d", r.StatusCode)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d", len(entries))
	}
	e := entries[0]
	if e.Tool != "web_fetch" || !e.Skipped || e.SkipReason != "domain blocked: bad.example.com" || e.Status == nil || *e.Status != 403 {
		t.Errorf("entry = %+v", e)
	}
	if e.RequestHeaders["User-Agent"] != "chat-to-messages/1.0 (proxy; +https://github.com/chinfeng/chat-to-claude-code)" {
		t.Errorf("User-Agent = %q", e.RequestHeaders["User-Agent"])
	}
	if e.RequestHeaders["Accept"] != "text/html,application/json,text/plain,text/markdown" {
		t.Errorf("Accept = %q", e.RequestHeaders["Accept"])
	}
}

// TestExecuteWebFetchAllowsWildcard is the brief test with a hermetic fix:
// *.example.com is a reserved domain that does not resolve in this sandbox
// (DNS timeout), so the fetch to http://sub.example.com/x would fail with 502
// instead of 200. The assertion set is kept verbatim; the request is routed to
// the httptest server by rewriting the dial address for the sub.example.com
// host (the domain check and HTTP request otherwise behave exactly as written).
func TestExecuteWebFetchAllowsWildcard(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("hello"))
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	origTransport := httpClient.Transport
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, u.Host)
	}
	httpClient.Transport = tr
	defer func() { httpClient.Transport = origTransport }()

	cfg := testCfg()
	cfg.WebFetchAllowedDomains = []string{"*.example.com"}
	r := ExecuteWebFetch(context.Background(), "http://sub.example.com/x", cfg, nil)
	if r.StatusCode != 200 || r.Content != "hello" {
		t.Errorf("wildcard fetch: %+v", r)
	}
}

// TestExecuteWebFetchRedirectDomainRecheck (M-3): every redirect hop is
// re-checked against the allow/block lists. A 302 to a domain outside the
// allow list must fail the fetch (502, "Fetch failed") and never return the
// target content; a redirect staying within an allowed domain must succeed.
// The rejected target (evil.com) is never dialed — the check fires before the
// request is issued — so no DNS resolution is involved.
func TestExecuteWebFetchRedirectDomainRecheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect-to-evil":
			w.Header().Set("Location", "http://evil.com/secret")
			w.WriteHeader(http.StatusFound)
		case "/redirect-relative":
			w.Header().Set("Location", "/target")
			w.WriteHeader(http.StatusFound)
		default:
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte("SECRET TARGET CONTENT"))
		}
	}))
	defer srv.Close()

	cfg := testCfg()
	cfg.WebFetchAllowedDomains = []string{"127.0.0.1"}

	// Initial request allowed (127.0.0.1), redirect to evil.com rejected.
	r := ExecuteWebFetch(context.Background(), srv.URL+"/redirect-to-evil", cfg, nil)
	if r.StatusCode != 502 {
		t.Errorf("redirect rejection status = %d, want 502: %+v", r.StatusCode, r)
	}
	if !strings.HasPrefix(r.Content, "Fetch failed:") {
		t.Errorf("content = %q, want \"Fetch failed: ...\"", r.Content)
	}
	if strings.Contains(r.Content, "SECRET TARGET CONTENT") {
		t.Errorf("redirected content leaked: %q", r.Content)
	}
	if !strings.Contains(r.Content, "not in the allowed list") {
		t.Errorf("rejection reason missing: %q", r.Content)
	}

	// In-list redirect (same host, relative Location) still follows and succeeds.
	r = ExecuteWebFetch(context.Background(), srv.URL+"/redirect-relative", cfg, nil)
	if r.StatusCode != 200 || r.Content != "SECRET TARGET CONTENT" {
		t.Errorf("in-list redirect = %+v, want 200 with target content", r)
	}
}

func TestExecuteWebFetchHTMLToText(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<html><head><title>Page Title</title><script>var x=1;</script><style>body{}</style></head><body><h1>Hello</h1><p>World&nbsp;text</p></body></html>`))
	}))
	defer srv.Close()
	r := ExecuteWebFetch(context.Background(), srv.URL, testCfg(), nil)
	if r.StatusCode != 200 {
		t.Fatalf("status = %d", r.StatusCode)
	}
	if r.Title != "Page Title" {
		t.Errorf("title = %q", r.Title)
	}
	if strings.Contains(r.Content, "<script>") || strings.Contains(r.Content, "<style>") {
		t.Error("tags not stripped")
	}
	if !strings.Contains(r.Content, "Hello") || !strings.Contains(r.Content, "World text") {
		t.Errorf("content = %q", r.Content)
	}
}

func TestExecuteWebFetchTruncation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(strings.Repeat("a", 100000)))
	}))
	defer srv.Close()
	cfg := testCfg()
	cfg.WebFetchMaxContentTokens = 100
	r := ExecuteWebFetch(context.Background(), srv.URL, cfg, nil)
	if len(r.Content) > 100*4+50 {
		t.Errorf("content too long: %d", len(r.Content))
	}
	if !strings.Contains(r.Content, "[Content truncated]") {
		t.Error("missing truncation marker")
	}
}

// --- detection / strip / schema / format vectors ported from
// ../chat-to-claude-code/tests/server_tools.test.ts ---

func checkDetected(t *testing.T, got []DetectedTextToolCall, typ string, input map[string]any) {
	t.Helper()
	if len(got) != 1 {
		t.Fatalf("got %d results: %+v", len(got), got)
	}
	if got[0].Type != typ {
		t.Errorf("type = %q, want %q", got[0].Type, typ)
	}
	if !reflect.DeepEqual(got[0].Input, input) {
		t.Errorf("input = %v, want %v", got[0].Input, input)
	}
}

func TestDetectServerToolInTextPattern1(t *testing.T) {
	// <tool_use> web_search
	text := "I'll search for that.\n\n<tool_use>\n{\"name\": \"web_search\", \"input\": {\"query\": \"react-router v7 best practices\"}}\n</tool_use>"
	checkDetected(t, DetectServerToolInText(text), "web_search", map[string]any{"query": "react-router v7 best practices"})

	// <tool_use> web_fetch
	text = "<tool_use>\n{\"name\": \"web_fetch\", \"input\": {\"url\": \"https://example.com\"}}\n</tool_use>"
	checkDetected(t, DetectServerToolInText(text), "web_fetch", map[string]any{"url": "https://example.com"})

	// web_fetch with prompt
	text = "<tool_use>\n{\"name\": \"web_fetch\", \"input\": {\"url\": \"https://example.com\", \"prompt\": \"summarize\"}}\n</tool_use>"
	checkDetected(t, DetectServerToolInText(text), "web_fetch", map[string]any{"url": "https://example.com", "prompt": "summarize"})

	// multiple <tool_use> tags
	text = "<tool_use>\n{\"name\": \"web_search\", \"input\": {\"query\": \"first query\"}}\n</tool_use>\nSome text\n<tool_use>\n{\"name\": \"web_fetch\", \"input\": {\"url\": \"https://example.com\"}}\n</tool_use>"
	got := DetectServerToolInText(text)
	if len(got) != 2 {
		t.Fatalf("got %d results: %+v", len(got), got)
	}
	if got[0].Type != "web_search" || got[1].Type != "web_fetch" {
		t.Errorf("types = %q, %q", got[0].Type, got[1].Type)
	}

	// non-server tool ignored
	text = "<tool_use>\n{\"name\": \"read_file\", \"input\": {\"path\": \"/tmp/test\"}}\n</tool_use>"
	if got := DetectServerToolInText(text); len(got) != 0 {
		t.Errorf("non-server tool must be ignored: %+v", got)
	}
}

func TestDetectServerToolInTextPattern2(t *testing.T) {
	checkDetected(t, DetectServerToolInText(`WebSearch {"query": "test query"}`), "web_search", map[string]any{"query": "test query"})
	checkDetected(t, DetectServerToolInText(`WebFetch {"url": "https://example.com"}`), "web_fetch", map[string]any{"url": "https://example.com"})

	// case-insensitive
	checkDetected(t, DetectServerToolInText(`websearch {"query": "test"}`), "web_search", map[string]any{"query": "test"})

	// extra text around
	checkDetected(t, DetectServerToolInText(`Let me search for that. WebSearch {"query": "latest news"}`), "web_search", map[string]any{"query": "latest news"})

	// malformed JSON / missing key / plain text
	if got := DetectServerToolInText("WebSearch {broken"); len(got) != 0 {
		t.Errorf("malformed JSON must be ignored: %+v", got)
	}
	if got := DetectServerToolInText(`WebSearch {"other": "value"}`); len(got) != 0 {
		t.Errorf("WebSearch without query must be ignored: %+v", got)
	}
	if got := DetectServerToolInText(`WebFetch {"other": "value"}`); len(got) != 0 {
		t.Errorf("WebFetch without url must be ignored: %+v", got)
	}
	if got := DetectServerToolInText("Hello world"); len(got) != 0 {
		t.Errorf("plain text must yield nothing: %+v", got)
	}
	if got := DetectServerToolInText(`{"query": "test"}`); len(got) != 0 {
		t.Errorf("bare JSON must yield nothing: %+v", got)
	}
}

func TestDetectServerToolInTextGLMRealWorld(t *testing.T) {
	text := "I'll search for React Router v7 best practices for you.\n\n<tool_use>\n{\"name\": \"web_search\", \"input\": {\"query\": \"react-router v7 最佳实践 best practices\"}}\n</tool_use>\n\nBased on my search, here's a summary of React Router v7 best practices:\n\n---\n\n## React Router v7 Best Practices\n\n### 1. **Framework Mode vs Library Mode**"
	checkDetected(t, DetectServerToolInText(text), "web_search", map[string]any{"query": "react-router v7 最佳实践 best practices"})
}

func TestDetectServerToolInText(t *testing.T) {
	// brief vectors: 4 patterns + dedupe
	tc := DetectServerToolInText(`<tool_use>{"name":"web_search","input":{"query":"q1"}}</tool_use>`)
	if len(tc) != 1 || tc[0].Type != "web_search" {
		t.Errorf("pattern1: %+v", tc)
	}
	tc = DetectServerToolInText(`Use WebSearch {"query": "q2"}`)
	if len(tc) != 1 || tc[0].Type != "web_search" {
		t.Errorf("pattern2: %+v", tc)
	}
	tc = DetectServerToolInText(`<tool_call><tool_name>web_fetch</tool_name><parameter name="url">https://x.com</parameter></tool_call>`)
	if len(tc) != 1 || tc[0].Type != "web_fetch" || tc[0].Input["url"] != "https://x.com" {
		t.Errorf("pattern3: %+v", tc)
	}
	tc = DetectServerToolInText(`<tool_name>web_search</tool_name><parameter name="query">q3</parameter>`)
	if len(tc) != 1 || tc[0].Type != "web_search" || tc[0].Input["query"] != "q3" {
		t.Errorf("pattern4: %+v", tc)
	}
	tc = DetectServerToolInText(`<tool_use>{"name":"web_search","input":{"query":"q"}}</tool_use> Use WebSearch {"query": "q"}`)
	if len(tc) != 1 {
		t.Errorf("should dedupe: %+v", tc)
	}
}

func TestStripToolUseFromText(t *testing.T) {
	if got := StripToolUseFromText("intro <tool_use>{\"name\":\"web_search\"}</tool_use> fake results"); got != "intro" {
		t.Errorf("got %q", got)
	}
	if got := StripToolUseFromText("before <tool_call><tool_name>web_search</tool_name></tool_call> after"); got != "before" {
		t.Errorf("got %q", got)
	}
	if got := StripToolUseFromText("no tags here"); got != "no tags here" {
		t.Errorf("got %q", got)
	}
	// ported from server_tools.test.ts
	text := "I'll search for that.\n\n<tool_use>\n{\"name\": \"web_search\", \"input\": {\"query\": \"test\"}}\n</tool_use>\n\nBased on my search, here are the results..."
	if got := StripToolUseFromText(text); got != "I'll search for that." {
		t.Errorf("got %q", got)
	}
	text = "Just a normal response without tool calls."
	if got := StripToolUseFromText(text); got != text {
		t.Errorf("got %q", got)
	}
	text = "<tool_use>\n{\"name\": \"web_search\", \"input\": {\"query\": \"test\"}}\n</tool_use>\nHallucinated results"
	if got := StripToolUseFromText(text); got != "" {
		t.Errorf("got %q", got)
	}
	text = "Some text  \n\n<tool_use>\n...\n</tool_use>"
	if got := StripToolUseFromText(text); got != "Some text" {
		t.Errorf("got %q", got)
	}
	// unwrapped tool_name variant (brief pattern 4 tag)
	if got := StripToolUseFromText("pre <tool_name>web_fetch</tool_name> rest"); got != "pre" {
		t.Errorf("got %q", got)
	}
}

func TestIsServerToolTypeAndSchemas(t *testing.T) {
	for _, ty := range []string{"web_search", "web_search_20250305", "web_fetch", "web_fetch_20250305"} {
		if !IsServerToolType(ty) {
			t.Errorf("IsServerToolType(%q) = false", ty)
		}
	}
	if IsServerToolType("regular_tool") {
		t.Error("regular tool must not match")
	}
	for _, ty := range []string{"function", "text", ""} {
		if IsServerToolType(ty) {
			t.Errorf("IsServerToolType(%q) = true", ty)
		}
	}
	if s := BuildServerToolFunctionSchema("web_search_20250305", "web_search"); s == nil {
		t.Error("schema nil")
	}
	if s := BuildServerToolFunctionSchema("bogus", "x"); s != nil {
		t.Error("bogus schema should be nil")
	}
	suffix := BuildServerToolSystemPromptSuffix([]map[string]any{{"type": "web_search_20250305"}, {"type": "web_fetch_20250305"}})
	if !strings.Contains(suffix, "web_search") || !strings.Contains(suffix, "web_fetch") {
		t.Errorf("suffix = %q", suffix)
	}
}

func TestBuildServerToolFunctionSchemaWebSearch(t *testing.T) {
	got := BuildServerToolFunctionSchema("web_search_20250305", "web_search")
	want := map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        "web_search",
			"description": "Search the web for information. Use this tool when you need to find current information, look up facts, or research topics on the internet.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{
						"type":        "string",
						"description": "The search query string",
					},
				},
				"required": []string{"query"},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("schema = %v, want %v", got, want)
	}
}

func TestBuildServerToolFunctionSchemaWebFetch(t *testing.T) {
	got := BuildServerToolFunctionSchema("web_fetch_20250305", "web_fetch")
	want := map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        "web_fetch",
			"description": "Fetch the content of a web page. Use this tool when you need to read the content of a specific URL.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"url": map[string]any{
						"type":        "string",
						"description": "The URL to fetch",
					},
					"prompt": map[string]any{
						"type":        "string",
						"description": "What to look for or summarize from the page",
					},
				},
				"required": []string{"url"},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("schema = %v, want %v", got, want)
	}
	if got := BuildServerToolFunctionSchema("function", "read_file"); got != nil {
		t.Errorf("function schema = %v", got)
	}
	if got := BuildServerToolFunctionSchema("text", "something"); got != nil {
		t.Errorf("text schema = %v", got)
	}
}

func TestBuildServerToolSystemPromptSuffix(t *testing.T) {
	if suffix := BuildServerToolSystemPromptSuffix([]map[string]any{{"type": "web_search_20250305", "name": "web_search"}}); !strings.Contains(suffix, "web_search") || !strings.Contains(suffix, "query") {
		t.Errorf("search suffix = %q", suffix)
	}
	if suffix := BuildServerToolSystemPromptSuffix([]map[string]any{{"type": "web_fetch_20250305", "name": "web_fetch"}}); !strings.Contains(suffix, "web_fetch") || !strings.Contains(suffix, "url") {
		t.Errorf("fetch suffix = %q", suffix)
	}
	if suffix := BuildServerToolSystemPromptSuffix([]map[string]any{
		{"type": "web_search_20250305", "name": "web_search"},
		{"type": "web_fetch_20250305", "name": "web_fetch"},
	}); !strings.Contains(suffix, "web_search") || !strings.Contains(suffix, "web_fetch") {
		t.Errorf("combined suffix = %q", suffix)
	}
	if suffix := BuildServerToolSystemPromptSuffix([]map[string]any{}); suffix != "" {
		t.Errorf("empty suffix = %q", suffix)
	}
	if suffix := BuildServerToolSystemPromptSuffix([]map[string]any{{"type": "function", "name": "read_file"}}); suffix != "" {
		t.Errorf("non-server suffix = %q", suffix)
	}
}

func TestFormatWebSearchResultContent(t *testing.T) {
	got := FormatWebSearchResultContent([]WebSearchResult{{URL: "https://example.com", Title: "Example", Snippet: "A great example"}})
	want := []map[string]any{
		{"type": "web_search_result", "url": "https://example.com", "title": "Example", "snippet": "A great example"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	got = FormatWebSearchResultContent([]WebSearchResult{{URL: "https://example.com", Title: "Example"}})
	want = []map[string]any{{"type": "web_search_result", "url": "https://example.com", "title": "Example"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	got = FormatWebSearchResultContent([]WebSearchResult{{URL: "https://example.com", Title: "Example", PageAge: "2 days"}})
	want = []map[string]any{{"type": "web_search_result", "url": "https://example.com", "title": "Example", "page_age": "2 days"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	got = FormatWebSearchResultContent(nil)
	want = []map[string]any{}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestFormatWebFetchResultContent(t *testing.T) {
	got := FormatWebFetchResultContent(WebFetchResult{Content: "Page content", URL: "https://example.com", StatusCode: 200, Title: "Example Page"})
	want := []map[string]any{
		{"type": "text", "text": "Title: Example Page"},
		{"type": "text", "text": "URL: https://example.com"},
		{"type": "text", "text": "Page content"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	got = FormatWebFetchResultContent(WebFetchResult{Content: "Page content", URL: "https://example.com", StatusCode: 200})
	want = []map[string]any{
		{"type": "text", "text": "URL: https://example.com"},
		{"type": "text", "text": "Page content"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	got = FormatWebFetchResultContent(WebFetchResult{Content: "Not found", URL: "https://example.com/404", StatusCode: 404})
	found := false
	for _, b := range got {
		if b["type"] == "text" && b["text"] == "Status: 404" {
			found = true
		}
	}
	if !found {
		t.Errorf("missing status block: %v", got)
	}
}

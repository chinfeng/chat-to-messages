// Package servertool implements proxy-side execution of Anthropic server
// tools (web_search, web_fetch), ported from
// ../chat-to-claude-code/src/server/server_tools.ts.
package servertool

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"chat-to-messages/internal/config"
	"chat-to-messages/internal/dump"
)

// maxResponseBodyLog mirrors MAX_RESPONSE_BODY_LOG in server_tools.ts.
const maxResponseBodyLog = 10000

// LogFn receives one server-tool call's log entry, mirroring ServerToolLogFn
// in server_tools.ts. A nil LogFn disables logging.
type LogFn func(entry dump.ServerToolLogEntry)

// WebSearchResult is one search result, mirroring WebSearchResult in
// server_tools.ts. Snippet and PageAge are empty when absent.
type WebSearchResult struct {
	URL     string
	Title   string
	Snippet string
	PageAge string
}

// WebFetchResult mirrors WebFetchResult in server_tools.ts. Title is empty
// when the page has no extractable title.
type WebFetchResult struct {
	Content    string
	URL        string
	StatusCode int
	Title      string
}

// DetectedTextToolCall is a web_search/web_fetch call detected in upstream
// text output, mirroring DetectedTextToolCall in server_tools.ts.
type DetectedTextToolCall struct {
	Type  string
	Input map[string]any
}

// httpClient is the shared HTTP client for server-tool calls. Like TS
// fetch(), it has no timeout; cancellation flows through the request context.
var httpClient = &http.Client{}

// warn mirrors console.warn (stderr, no timestamp).
func warn(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

// nowISO renders the current time like TS new Date().toISOString().
func nowISO() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

// durationMs mirrors TS Date.now() - startMs via time.Since.
func durationMs(start time.Time) *int64 {
	ms := time.Since(start).Milliseconds()
	return &ms
}

// str converts an arbitrary JSON value to a string like TS String(v ?? "").
func str(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return fmt.Sprint(t)
	}
}

// isTruthy mirrors TS truthiness for values decoded from JSON.
func isTruthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case float64:
		return t != 0
	case json.Number:
		s := t.String()
		return s != "" && s != "0"
	default:
		return true
	}
}

// truncate mirrors TS truncate(): slice to maxLen plus a truncation marker.
// TS slices by UTF-16 code units; Go byte-slicing is the byte-level analog,
// except a partial trailing rune is dropped so the log stays valid UTF-8.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	cut := s[:maxLen]
	for !utf8.ValidString(cut) {
		_, size := utf8.DecodeLastRuneInString(cut)
		cut = cut[:len(cut)-size]
	}
	return cut + fmt.Sprintf("\n...[truncated, total %d bytes]", len(s))
}

// prefix returns at most maxLen characters (bytes in Go) of s, mirroring the
// inline s.slice(0, 500) used in error messages.
func prefix(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

// extractResponseHeaders mirrors extractResponseHeaders(): a plain
// key → value map. For multi-valued headers the last value wins, matching the
// forEach overwrite order in the TS original.
func extractResponseHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		if len(v) > 0 {
			out[k] = v[len(v)-1]
		}
	}
	return out
}

// readBody reads the response body, transparently decompressing gzip like
// undici's fetch does when Accept-Encoding: gzip was requested.
func readBody(res *http.Response) ([]byte, error) {
	if strings.EqualFold(res.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(res.Body)
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return io.ReadAll(zr)
	}
	return io.ReadAll(res.Body)
}

// doSearchRequest performs the search GET, mirroring the fetch() call plus
// res.text().catch(() => "") of executeWebSearch: a body read error yields an
// empty body instead of propagating.
func doSearchRequest(ctx context.Context, requestURL string, headers map[string]string) (status int, responseHeaders map[string]string, body string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return 0, nil, "", err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, "", err
	}
	defer res.Body.Close()
	bodyBytes, err := readBody(res)
	if err != nil {
		bodyBytes = nil
	}
	return res.StatusCode, extractResponseHeaders(res.Header), string(bodyBytes), nil
}

// searchEntry builds the base web_search log entry.
func searchEntry(query, engine string) dump.ServerToolLogEntry {
	return dump.ServerToolLogEntry{
		Tool: "web_search", Timestamp: nowISO(), Input: query, Engine: engine,
	}
}

func logSearch(log LogFn, e dump.ServerToolLogEntry) {
	if log != nil {
		log(e)
	}
}

// completedSearchEntry assembles the log entry for a non-skipped web_search
// call (status/resultCount/duration always present, mirroring the TS onLog
// calls on the non-skip paths).
func completedSearchEntry(query, engine string, requestURL string, requestHeaders map[string]string, status int, responseHeaders map[string]string, responseBody string, resultCount int, start time.Time, errMsg string) dump.ServerToolLogEntry {
	e := searchEntry(query, engine)
	e.RequestURL = requestURL
	e.RequestHeaders = requestHeaders
	e.Status = &status
	e.ResponseHeaders = responseHeaders
	e.ResponseBody = responseBody
	e.ResultCount = &resultCount
	e.Error = errMsg
	e.DurationMs = durationMs(start)
	return e
}

// searchCatch mirrors the outer catch of executeWebSearch: the error message
// is logged without request details and an empty result set is returned.
func searchCatch(log LogFn, query, engine string, err error, start time.Time) []WebSearchResult {
	msg := err.Error()
	warn("Web search failed: %s", msg)
	e := searchEntry(query, engine)
	zero := 0
	e.ResultCount = &zero
	e.Error = msg
	e.DurationMs = durationMs(start)
	logSearch(log, e)
	return nil
}

// mapSearchResults mirrors the final results.map of executeWebSearch.
func mapSearchResults(raw []any) []WebSearchResult {
	mapped := make([]WebSearchResult, 0, len(raw))
	for _, item := range raw {
		r, ok := item.(map[string]any)
		if !ok {
			r = map[string]any{}
		}
		res := WebSearchResult{
			URL:   str(r["url"]),
			Title: str(r["title"]),
		}
		if isTruthy(r["description"]) {
			res.Snippet = str(r["description"])
		}
		if isTruthy(r["page_age"]) {
			res.PageAge = str(r["page_age"])
		}
		mapped = append(mapped, res)
	}
	return mapped
}

// ExecuteWebSearch performs a web search using the Brave Search API or
// SearXNG, mirroring executeWebSearch() in server_tools.ts.
func ExecuteWebSearch(ctx context.Context, query string, cfg config.ServerToolConfig, log LogFn) []WebSearchResult {
	baseURL := strings.TrimRight(cfg.WebSearchBaseURL, "/")
	apiKey := cfg.WebSearchAPIKey
	engine := cfg.WebSearchEngine

	if strings.TrimSpace(query) == "" {
		e := searchEntry(query, engine)
		e.Skipped = true
		e.SkipReason = "empty query"
		logSearch(log, e)
		return nil
	}

	start := time.Now()

	if engine == "searxng" {
		requestURL := baseURL + "/search?q=" + url.QueryEscape(query) + "&format=json"
		headers := map[string]string{"Accept": "application/json"}
		if apiKey != "" {
			headers["Authorization"] = "Bearer " + apiKey
		}
		status, resHeaders, resBody, err := doSearchRequest(ctx, requestURL, headers)
		if err != nil {
			return searchCatch(log, query, "searxng", err, start)
		}
		if status < 200 || status >= 300 {
			warn("SearXNG search API returned %d: %s", status, resBody)
			logSearch(log, completedSearchEntry(query, "searxng", requestURL, headers, status, resHeaders, truncate(resBody, maxResponseBodyLog), 0, start, fmt.Sprintf("HTTP %d: %s", status, prefix(resBody, 500))))
			return nil
		}
		var data map[string]any
		if err := json.Unmarshal([]byte(resBody), &data); err != nil {
			logSearch(log, completedSearchEntry(query, "searxng", requestURL, headers, status, resHeaders, truncate(resBody, maxResponseBodyLog), 0, start, "Invalid JSON response"))
			return nil
		}
		raw, ok := data["results"].([]any)
		if !ok {
			logSearch(log, completedSearchEntry(query, "searxng", requestURL, headers, status, resHeaders, truncate(resBody, maxResponseBodyLog), 0, start, "Response results field is not an array"))
			return nil
		}
		logSearch(log, completedSearchEntry(query, "searxng", requestURL, headers, status, resHeaders, truncate(resBody, maxResponseBodyLog), len(raw), start, ""))
		return mapSearchResults(raw)
	}

	// Brave
	if apiKey == "" {
		e := searchEntry(query, "brave")
		e.Skipped = true
		e.SkipReason = "no API key configured"
		logSearch(log, e)
		return nil
	}
	requestURL := baseURL + "/res/v1/web/search?q=" + url.QueryEscape(query) + "&count=10"
	headers := map[string]string{
		"Accept":               "application/json",
		"Accept-Encoding":      "gzip",
		"X-Subscription-Token": apiKey,
	}
	status, resHeaders, resBody, err := doSearchRequest(ctx, requestURL, headers)
	if err != nil {
		return searchCatch(log, query, "brave", err, start)
	}
	if status < 200 || status >= 300 {
		warn("Brave search API returned %d: %s", status, resBody)
		logSearch(log, completedSearchEntry(query, "brave", requestURL, headers, status, resHeaders, truncate(resBody, maxResponseBodyLog), 0, start, fmt.Sprintf("HTTP %d: %s", status, prefix(resBody, 500))))
		return nil
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(resBody), &data); err != nil {
		logSearch(log, completedSearchEntry(query, "brave", requestURL, headers, status, resHeaders, truncate(resBody, maxResponseBodyLog), 0, start, "Invalid JSON response"))
		return nil
	}
	web, _ := data["web"].(map[string]any)
	raw, ok := web["results"].([]any)
	if !ok {
		logSearch(log, completedSearchEntry(query, "brave", requestURL, headers, status, resHeaders, truncate(resBody, maxResponseBodyLog), 0, start, "Response web.results field is not an array or missing"))
		return nil
	}
	logSearch(log, completedSearchEntry(query, "brave", requestURL, headers, status, resHeaders, truncate(resBody, maxResponseBodyLog), len(raw), start, ""))
	return mapSearchResults(raw)
}

// fetchEntry builds the base web_fetch log entry (status/duration present on
// every non-skip path, mirroring the TS onLog calls).
func fetchEntry(input string, requestHeaders map[string]string, status int, errMsg string, start time.Time) dump.ServerToolLogEntry {
	e := dump.ServerToolLogEntry{
		Tool: "web_fetch", Timestamp: nowISO(), Input: input,
		RequestURL: input, RequestHeaders: requestHeaders,
		Status: &status, Error: errMsg, DurationMs: durationMs(start),
	}
	return e
}

func logFetch(log LogFn, e dump.ServerToolLogEntry) {
	if log != nil {
		log(e)
	}
}

// fetchFailed mirrors the catch of executeWebFetch: a 502 result with
// "Fetch failed: <msg>" content.
func fetchFailed(log LogFn, input string, requestHeaders map[string]string, err error, start time.Time) WebFetchResult {
	msg := "Fetch failed: " + err.Error()
	logFetch(log, fetchEntry(input, requestHeaders, 502, msg, start))
	return WebFetchResult{Content: msg, URL: input, StatusCode: 502}
}

// ExecuteWebFetch performs a web fetch (HTTP GET) for a URL, mirroring
// executeWebFetch() in server_tools.ts.
func ExecuteWebFetch(ctx context.Context, input string, cfg config.ServerToolConfig, log LogFn) WebFetchResult {
	start := time.Now()

	reqHeaders := map[string]string{
		"User-Agent": "chat-to-messages/1.0 (proxy; +https://github.com/chinfeng/chat-to-claude-code)",
		"Accept":     "text/html,application/json,text/plain,text/markdown",
	}

	// Check domain restrictions
	parsed := parseDomain(input)
	if parsed == "" {
		logFetch(log, fetchEntry(input, reqHeaders, 400, "Invalid URL", start))
		return WebFetchResult{Content: "Invalid URL", URL: input, StatusCode: 400}
	}

	if len(cfg.WebFetchAllowedDomains) > 0 {
		allowed := false
		for _, d := range cfg.WebFetchAllowedDomains {
			if domainMatches(d, parsed) {
				allowed = true
				break
			}
		}
		if !allowed {
			msg := fmt.Sprintf("Domain %s is not in the allowed list", parsed)
			e := fetchEntry(input, reqHeaders, 403, msg, start)
			e.Skipped = true
			e.SkipReason = "domain not allowed: " + parsed
			logFetch(log, e)
			return WebFetchResult{Content: msg, URL: input, StatusCode: 403}
		}
	}

	if len(cfg.WebFetchBlockedDomains) > 0 {
		blocked := false
		for _, d := range cfg.WebFetchBlockedDomains {
			if domainMatches(d, parsed) {
				blocked = true
				break
			}
		}
		if blocked {
			msg := fmt.Sprintf("Domain %s is blocked", parsed)
			e := fetchEntry(input, reqHeaders, 403, msg, start)
			e.Skipped = true
			e.SkipReason = "domain blocked: " + parsed
			logFetch(log, e)
			return WebFetchResult{Content: msg, URL: input, StatusCode: 403}
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, input, nil)
	if err != nil {
		return fetchFailed(log, input, reqHeaders, err, start)
	}
	for k, v := range reqHeaders {
		req.Header.Set(k, v)
	}
	res, err := httpClient.Do(req) // redirect: "follow" is Go's default
	if err != nil {
		return fetchFailed(log, input, reqHeaders, err, start)
	}
	defer res.Body.Close()

	bodyBytes, err := readBody(res)
	if err != nil {
		return fetchFailed(log, input, reqHeaders, err, start)
	}
	body := string(bodyBytes)

	resHeaders := extractResponseHeaders(res.Header)
	contentType := res.Header.Get("Content-Type")

	// For HTML, strip tags to get plain text (simple approach)
	content := body
	if strings.Contains(contentType, "text/html") {
		content = htmlToPlainText(body)
	}

	// Truncate to approximate token limit (chars / 4)
	maxChars := cfg.WebFetchMaxContentTokens * 4
	if len(content) > maxChars {
		content = truncateContent(content, maxChars)
	}

	e := fetchEntry(input, reqHeaders, res.StatusCode, "", start)
	e.ResponseHeaders = resHeaders
	e.ResponseBody = truncate(body, maxResponseBodyLog)
	logFetch(log, e)

	return WebFetchResult{
		Content:    content,
		URL:        res.Request.URL.String(),
		StatusCode: res.StatusCode,
		Title:      extractTitle(body, contentType),
	}
}

// truncateContent truncates fetch content to maxChars bytes plus the TS
// "[Content truncated]" marker, dropping a partial trailing rune to keep the
// content valid UTF-8 (TS slices UTF-16 code units).
func truncateContent(s string, maxChars int) string {
	cut := s[:maxChars]
	for !utf8.ValidString(cut) {
		_, size := utf8.DecodeLastRuneInString(cut)
		cut = cut[:len(cut)-size]
	}
	return cut + "\n\n[Content truncated]"
}

// parseDomain mirrors parseDomain(): the URL's hostname, or "" when unparsable.
func parseDomain(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// domainMatches mirrors domainMatches(): exact match, or "*.suffix" wildcard
// matching the suffix itself and any subdomain.
func domainMatches(pattern, domain string) bool {
	if pattern == domain {
		return true
	}
	if strings.HasPrefix(pattern, "*.") {
		suffix := pattern[2:]
		return domain == suffix || strings.HasSuffix(domain, "."+suffix)
	}
	return false
}

var (
	scriptRE  = regexp.MustCompile(`(?i)<script[\s\S]*?</script>`)
	styleRE   = regexp.MustCompile(`(?i)<style[\s\S]*?</style>`)
	blockRE   = regexp.MustCompile(`(?i)</?(p|div|br|h[1-6]|li|tr|td|th)[^>]*>`)
	tagRE     = regexp.MustCompile(`<[^>]+>`)
	spaceRE   = regexp.MustCompile(`[ \t]+`)
	newlineRE = regexp.MustCompile(`\n{3,}`)
	titleRE   = regexp.MustCompile(`(?i)<title[^>]*>([\s\S]*?)</title>`)
	h1RE      = regexp.MustCompile(`(?m)^#\s+(.+)$`)
)

// htmlEntities mirrors the entity-decoding chain in htmlToPlainText.
var htmlEntities = strings.NewReplacer(
	"&amp;", "&",
	"&lt;", "<",
	"&gt;", ">",
	"&quot;", "\"",
	"&#39;", "'",
	"&nbsp;", " ",
)

// htmlToPlainText mirrors htmlToPlainText(): strip script/style, newline for
// block elements, drop remaining tags, decode common entities, collapse
// whitespace. Go regexp (RE2) supports the lazy quantifiers used here.
func htmlToPlainText(html string) string {
	text := scriptRE.ReplaceAllString(html, "")
	text = styleRE.ReplaceAllString(text, "")
	text = blockRE.ReplaceAllString(text, "\n")
	text = tagRE.ReplaceAllString(text, "")
	text = htmlEntities.Replace(text)
	text = spaceRE.ReplaceAllString(text, " ")
	text = newlineRE.ReplaceAllString(text, "\n\n")
	return strings.TrimSpace(text)
}

// extractTitle mirrors extractTitle(): title tag for HTML, first H1 for
// markdown; "" when there is no match.
func extractTitle(body, contentType string) string {
	if strings.Contains(contentType, "text/html") {
		if m := titleRE.FindStringSubmatch(body); m != nil {
			return strings.TrimSpace(tagRE.ReplaceAllString(m[1], ""))
		}
	}
	if strings.Contains(contentType, "text/markdown") {
		if m := h1RE.FindStringSubmatch(body); m != nil {
			return strings.TrimSpace(m[1])
		}
	}
	return ""
}

// FormatWebSearchResultContent mirrors formatWebSearchResultContent(): one
// web_search_result content block per result, omitting absent fields.
func FormatWebSearchResultContent(results []WebSearchResult) []map[string]any {
	out := make([]map[string]any, 0, len(results))
	for _, r := range results {
		block := map[string]any{"type": "web_search_result", "url": r.URL, "title": r.Title}
		if r.Snippet != "" {
			block["snippet"] = r.Snippet
		}
		if r.PageAge != "" {
			block["page_age"] = r.PageAge
		}
		out = append(out, block)
	}
	return out
}

// FormatWebFetchResultContent mirrors formatWebFetchResultContent(): text
// blocks for title/url/status (status only for >= 400)/content.
func FormatWebFetchResultContent(result WebFetchResult) []map[string]any {
	var blocks []map[string]any
	if result.Title != "" {
		blocks = append(blocks, map[string]any{"type": "text", "text": "Title: " + result.Title})
	}
	blocks = append(blocks, map[string]any{"type": "text", "text": "URL: " + result.URL})
	if result.StatusCode >= 400 {
		blocks = append(blocks, map[string]any{"type": "text", "text": "Status: " + strconv.Itoa(result.StatusCode)})
	}
	blocks = append(blocks, map[string]any{"type": "text", "text": result.Content})
	return blocks
}

var (
	// toolUseRE: pattern 1, Claude-style <tool_use> tags wrapping JSON.
	toolUseRE = regexp.MustCompile(`<tool_use>\s*(\{[\s\S]*?\})\s*</tool_use>`)
	// glmToolCallRE: patterns 3 & 4, GLM-style XML with an optional wrapper.
	glmToolCallRE = regexp.MustCompile(`(?i)(?:<tool_call>\s*)?<tool_name>\s*(web_search|web_fetch)\s*</tool_name>\s*((?:<parameter\s+name\s*=\s*"([^"]+)"\s*>([\s\S]*?)</parameter>\s*)+)(?:</tool_call>)?`)
	glmParamRE    = regexp.MustCompile(`(?i)<parameter\s+name\s*=\s*"([^"]+)"\s*>([\s\S]*?)</parameter>`)
	// webSearchRE / webFetchRE: pattern 2, natural-language calls.
	webSearchRE = regexp.MustCompile(`(?i)\bWebSearch\s*\{[^}]*"query"\s*:\s*"[^"]*"[^}]*\}`)
	webFetchRE  = regexp.MustCompile(`(?i)\bWebFetch\s*\{[^}]*"url"\s*:\s*"[^"]*"[^}]*\}`)
	// webSearchPrefixRE / webFetchPrefixRE strip the name prefix from a match.
	webSearchPrefixRE = regexp.MustCompile(`(?i)^\s*WebSearch\s*`)
	webFetchPrefixRE  = regexp.MustCompile(`(?i)^\s*WebFetch\s*`)
	// toolNameStripRE finds an unwrapped tool_name tag for stripping.
	toolNameStripRE = regexp.MustCompile(`<tool_name>\s*(?:web_search|web_fetch)\s*</tool_name>`)
)

// decodeJSONObject parses s as exactly one JSON object (like TS JSON.parse).
// Numbers are decoded with UseNumber so tool inputs preserve precision.
func decodeJSONObject(s string) (map[string]any, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if err := dec.Decode(&v); err != io.EOF {
		return nil, fmt.Errorf("trailing data after JSON value")
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("JSON value is not an object")
	}
	return obj, nil
}

// dedupe mirrors the results.some((r) => r.type === t && r.input[k] === v)
// checks in detectServerToolInText.
func dedupe(results []DetectedTextToolCall, toolType, key string, value any) bool {
	for _, r := range results {
		if r.Type == toolType && r.Input[key] == value {
			return true
		}
	}
	return false
}

// DetectServerToolInText mirrors detectServerToolInText(): detects web_search
// or web_fetch tool calls in upstream text output across four patterns
// (Claude tool_use JSON, WebSearch/WebFetch JSON, GLM XML with or without the
// tool_call wrapper), deduplicating by tool type and key input value.
func DetectServerToolInText(text string) []DetectedTextToolCall {
	var results []DetectedTextToolCall

	// Pattern 1: Claude-style tool_use tags wrapping JSON
	for _, m := range toolUseRE.FindAllStringSubmatch(text, -1) {
		parsed, err := decodeJSONObject(m[1])
		if err != nil {
			continue
		}
		name, _ := parsed["name"].(string)
		input, _ := parsed["input"].(map[string]any)
		if name == "web_search" && isTruthy(input["query"]) {
			results = append(results, DetectedTextToolCall{Type: "web_search", Input: input})
		} else if name == "web_fetch" && isTruthy(input["url"]) {
			results = append(results, DetectedTextToolCall{Type: "web_fetch", Input: input})
		}
	}

	// Patterns 3 & 4: GLM-style XML tags with tool_name and parameter sub-tags
	for _, m := range glmToolCallRE.FindAllStringSubmatch(text, -1) {
		toolName := strings.ToLower(m[1])
		paramsBlock := m[2]
		input := map[string]any{}
		for _, pm := range glmParamRE.FindAllStringSubmatch(paramsBlock, -1) {
			input[pm[1]] = strings.TrimSpace(pm[2])
		}
		if toolName == "web_search" && isTruthy(input["query"]) && !dedupe(results, "web_search", "query", input["query"]) {
			results = append(results, DetectedTextToolCall{Type: "web_search", Input: input})
		} else if toolName == "web_fetch" && isTruthy(input["url"]) && !dedupe(results, "web_fetch", "url", input["url"]) {
			results = append(results, DetectedTextToolCall{Type: "web_fetch", Input: input})
		}
	}

	// Pattern 2: WebSearch {"query": "..."} or WebFetch {"url": "..."}
	if sm := webSearchRE.FindString(text); sm != "" {
		jsonStr := webSearchPrefixRE.ReplaceAllString(sm, "")
		if input, err := decodeJSONObject(jsonStr); err == nil {
			if isTruthy(input["query"]) && !dedupe(results, "web_search", "query", input["query"]) {
				results = append(results, DetectedTextToolCall{Type: "web_search", Input: input})
			}
		}
	}
	if fm := webFetchRE.FindString(text); fm != "" {
		jsonStr := webFetchPrefixRE.ReplaceAllString(fm, "")
		if input, err := decodeJSONObject(jsonStr); err == nil {
			if isTruthy(input["url"]) && !dedupe(results, "web_fetch", "url", input["url"]) {
				results = append(results, DetectedTextToolCall{Type: "web_fetch", Input: input})
			}
		}
	}

	return results
}

// StripToolUseFromText mirrors stripToolUseFromText(): removes a tool call
// tag (and anything after it) from model output, returning the clean text
// that precedes the first tag. Trailing whitespace is trimmed like TS
// `replace(/\s+$/, "")`.
func StripToolUseFromText(text string) string {
	if idx := strings.Index(text, "<tool_use>"); idx != -1 {
		return strings.TrimRightFunc(text[:idx], isSpace)
	}
	if idx := strings.Index(text, "<tool_call>"); idx != -1 {
		return strings.TrimRightFunc(text[:idx], isSpace)
	}
	if m := toolNameStripRE.FindStringIndex(text); m != nil {
		return strings.TrimRightFunc(text[:m[0]], isSpace)
	}
	return text
}

// isSpace mirrors TS \s for the trailing-whitespace trim.
func isSpace(r rune) bool {
	switch r {
	case ' ', '\t', '\n', '\r', '\v', '\f', 0x85, 0xA0:
		return true
	}
	return false
}

// IsServerToolType mirrors isServerToolType(): true for web_search/web_fetch
// and their versioned variants.
func IsServerToolType(t string) bool {
	return t == "web_search" || strings.HasPrefix(t, "web_search_") ||
		t == "web_fetch" || strings.HasPrefix(t, "web_fetch_")
}

// BuildServerToolFunctionSchema mirrors buildServerToolFunctionSchema(): an
// OpenAI-compatible function schema for a server tool, nil for unrecognized
// types.
func BuildServerToolFunctionSchema(toolType, toolName string) map[string]any {
	if toolType == "web_search" || strings.HasPrefix(toolType, "web_search_") {
		return map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        toolName,
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
	}
	if toolType == "web_fetch" || strings.HasPrefix(toolType, "web_fetch_") {
		return map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        toolName,
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
	}
	return nil
}

// BuildServerToolSystemPromptSuffix mirrors buildServerToolSystemPromptSuffix():
// instructions for the upstream model, one paragraph per present server tool,
// joined with blank lines.
func BuildServerToolSystemPromptSuffix(serverTools []map[string]any) string {
	var parts []string
	for _, tool := range serverTools {
		type_, _ := tool["type"].(string)
		if type_ == "web_search" || strings.HasPrefix(type_, "web_search_") {
			parts = append(parts, "You have access to a web_search tool. When you need to search the web, call the web_search function with a JSON object containing a \"query\" field. Example: {\"query\": \"your search query\"}")
		}
		if type_ == "web_fetch" || strings.HasPrefix(type_, "web_fetch_") {
			parts = append(parts, "You have access to a web_fetch tool. When you need to fetch a web page, call the web_fetch function with a JSON object containing a \"url\" field and optionally a \"prompt\" field. Example: {\"url\": \"https://example.com\", \"prompt\": \"summarize the page\"}")
		}
	}
	return strings.Join(parts, "\n\n")
}

// Integration tests for the proxy HTTP layer, ported from
// chat-to-claude-code's tests/routes.test.ts and tests/sse_stream.test.ts.
// Upstream is mocked with httptest servers; downstream uses a recorder.
package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chinfeng/chat-to-messages/internal/config"
)

// mockUpstream returns an upstream server that writes body verbatim with the
// given status and closes cleanly.
func mockUpstream(t *testing.T, body string, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// mockUpstreamAbort writes prefix, flushes, then aborts the connection
// (panic(http.ErrAbortHandler) closes the connection without completing the
// chunked body — the proxy's next read sees a connection error).
func mockUpstreamAbort(t *testing.T, prefix string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, prefix)
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// mockUpstreamDelayed writes chunks with a small delay between them, flushing
// each chunk so the proxy's client.Do returns early and content flows
// incrementally. reachedChunk records how many chunks have been flushed
// (mutex-protected, for cancel-mid-stream tests).
func mockUpstreamDelayed(t *testing.T, chunks []string, delay time.Duration) (*httptest.Server, func() int) {
	t.Helper()
	var mu sync.Mutex
	flushed := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for _, c := range chunks {
			io.WriteString(w, c)
			fl.Flush()
			mu.Lock()
			flushed++
			mu.Unlock()
			time.Sleep(delay)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() int {
		mu.Lock()
		defer mu.Unlock()
		return flushed
	}
}

// mockUpstreamBlocking blocks until release is closed, then responds. Used to
// simulate an upstream that never responds (client-abort-before-response).
func mockUpstreamBlocking(t *testing.T) (*httptest.Server, chan struct{}, func() bool) {
	t.Helper()
	release := make(chan struct{})
	var mu sync.Mutex
	started := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		started = true
		mu.Unlock()
		<-release
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	return srv, release, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return started
	}
}

func testConfig(upstreamURL string) *config.Config {
	return &config.Config{
		UpstreamBaseURL: upstreamURL,
		UpstreamAPIKey:  "sk-upstream",
		Port:            8082,
		EnableThinking:  true,
	}
}

func postMessages(t *testing.T, h http.Handler, body string, headers map[string]string) *http.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Result()
}

// startPost starts POST /v1/messages with a cancellable context in a
// goroutine, returning the (thread-safe) recorder and a channel closed when
// the handler returns.
func startPost(t *testing.T, h http.Handler, body string, ctx context.Context) (*lockedRecorder, chan struct{}) {
	t.Helper()
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)
	rec := &lockedRecorder{ResponseRecorder: httptest.NewRecorder()}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(rec, req)
	}()
	return rec, done
}

// lockedRecorder wraps httptest.ResponseRecorder so the handler goroutine and
// the test goroutine can safely share it (cancel-mid-stream tests).
type lockedRecorder struct {
	mu sync.Mutex
	*httptest.ResponseRecorder
}

func (r *lockedRecorder) Write(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ResponseRecorder.Write(b)
}

func (r *lockedRecorder) WriteHeader(code int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ResponseRecorder.WriteHeader(code)
}

func (r *lockedRecorder) Flush() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ResponseRecorder.Flush()
}

func (r *lockedRecorder) body() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ResponseRecorder.Body.String()
}

type sseEvent struct{ event, data string }

func parseSSE(t *testing.T, body string) []sseEvent {
	t.Helper()
	var out []sseEvent
	for _, block := range strings.Split(body, "\n\n") {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		var ev sseEvent
		for _, line := range strings.Split(block, "\n") {
			if strings.HasPrefix(line, "event: ") {
				ev.event = strings.TrimPrefix(line, "event: ")
			} else if strings.HasPrefix(line, "data: ") {
				ev.data = strings.TrimPrefix(line, "data: ")
			}
		}
		if ev.event != "" {
			out = append(out, ev)
		}
	}
	return out
}

func eventTypes(events []sseEvent) []string {
	var out []string
	for _, e := range events {
		out = append(out, e.event)
	}
	return out
}

// eventData returns the parsed JSON data of the first event with the given
// type, or nil.
func eventData(events []sseEvent, eventType string) map[string]any {
	for _, e := range events {
		if e.event == eventType {
			var m map[string]any
			if err := json.Unmarshal([]byte(e.data), &m); err == nil {
				return m
			}
		}
	}
	return nil
}

// waitFor polls cond until it returns true or the deadline passes.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// readBody reads and returns the response body.
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

func TestHealthAndNotFound(t *testing.T) {
	h := NewHandler(testConfig("http://unused"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/health", nil))
	if rec.Code != 200 || rec.Body.String() != `{"status":"ok"}` {
		t.Fatalf("health: %d %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("health response missing CORS")
	}

	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("GET", "/nope", nil))
	if rec2.Code != 404 {
		t.Fatalf("404 expected, got %d", rec2.Code)
	}
	if !strings.Contains(rec2.Body.String(), "not_found_error") {
		t.Errorf("body = %s", rec2.Body.String())
	}
	if rec2.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("404 response missing CORS")
	}

	// Bare /models is NOT registered (only /v1/models); it falls through to the
	// default 404 case.
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, httptest.NewRequest("GET", "/models", nil))
	if rec3.Code != 404 {
		t.Fatalf("404 expected for bare /models, got %d", rec3.Code)
	}
}

func TestAuthTokenRequired(t *testing.T) {
	up := mockUpstream(t, "data: [DONE]\n\n", 200)
	cfg := testConfig(up.URL)
	cfg.AuthToken = "secret"
	h := NewHandler(cfg)

	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != 401 {
		t.Fatalf("401 expected, got %d", resp.StatusCode)
	}

	resp2 := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"x-api-key": "wrong"})
	if resp2.StatusCode != 401 {
		t.Fatalf("401 expected, got %d", resp2.StatusCode)
	}

	resp3 := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"x-api-key": "secret"})
	if resp3.StatusCode != 200 {
		t.Fatalf("200 expected, got %d", resp3.StatusCode)
	}

	// Authorization: Bearer form is also accepted (TS replace /^Bearer\s+/i).
	resp4 := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"Authorization": "Bearer secret"})
	if resp4.StatusCode != 200 {
		t.Fatalf("200 expected with Bearer auth, got %d", resp4.StatusCode)
	}
}

func TestMissingAPIKey(t *testing.T) {
	up := mockUpstream(t, "data: [DONE]\n\n", 200)
	cfg := testConfig(up.URL)
	cfg.UpstreamAPIKey = ""
	cfg.AuthToken = "t" // 非透传
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"x-api-key": "t"})
	if resp.StatusCode != 401 {
		t.Fatalf("401 expected, got %d", resp.StatusCode)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, "No API key provided") {
		t.Errorf("body = %s", body)
	}
}

func TestPassthroughForwardsClientKey(t *testing.T) {
	var gotAuth string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	cfg.UpstreamAPIKey = ""
	cfg.AuthToken = ""
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"x-api-key": "client-key"})
	if resp.StatusCode != 200 {
		t.Fatalf("200 expected, got %d", resp.StatusCode)
	}
	if !strings.Contains(gotAuth, "client-key") {
		t.Errorf("client key not forwarded: %q", gotAuth)
	}
}

func TestInvalidJSONAndModel(t *testing.T) {
	h := NewHandler(testConfig("http://unused"))
	if resp := postMessages(t, h, `not-json`, nil); resp.StatusCode != 400 {
		t.Errorf("bad json: %d", resp.StatusCode)
	}
	// Trailing garbage after a well-formed object (`{...}garbage`) must be
	// rejected like TS JSON.parse, not silently accepted.
	if resp := postMessages(t, h, `{"model":"m","messages":[]}garbage`, nil); resp.StatusCode != 400 {
		t.Errorf("trailing garbage: %d", resp.StatusCode)
	}
	if resp := postMessages(t, h, `{"messages":[]}`, nil); resp.StatusCode != 400 {
		t.Errorf("no model: %d", resp.StatusCode)
	}
	if resp := postMessages(t, h, `{"model":"m"}`, nil); resp.StatusCode != 400 {
		t.Errorf("no messages: %d", resp.StatusCode)
	}
	// `messages: []` is a valid array (non-nil) — passes validation, unlike a
	// missing/null messages field.
	up := mockUpstream(t, "data: [DONE]\n\n", 200)
	h2 := NewHandler(testConfig(up.URL))
	if resp := postMessages(t, h2, `{"model":"m","messages":[]}`, nil); resp.StatusCode != 200 {
		t.Errorf("empty messages array should reach upstream: %d", resp.StatusCode)
	}
}

func TestUpstreamErrorMapping(t *testing.T) {
	up := mockUpstream(t, `{"error":"rate limited"}`, 429)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != 429 {
		t.Fatalf("429 expected, got %d", resp.StatusCode)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, "Upstream error:") {
		t.Errorf("body = %s", body)
	}
	if !strings.Contains(body, "rate limited") {
		t.Errorf("body = %s", body)
	}

	up5xx := mockUpstream(t, "boom", 503)
	h2 := NewHandler(testConfig(up5xx.URL))
	resp2 := postMessages(t, h2, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp2.StatusCode != 502 {
		t.Fatalf("502 expected, got %d", resp2.StatusCode)
	}
}

func TestFullStreamingFlow(t *testing.T) {
	upstreamBody := "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n" +
		"data: [DONE]\n\n"
	up := mockUpstream(t, upstreamBody, 200)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("ct = %q", ct)
	}
	if resp.Header.Get("X-Accel-Buffering") != "no" {
		t.Error("X-Accel-Buffering missing")
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS missing")
	}

	body, _ := io.ReadAll(resp.Body)
	events := parseSSE(t, string(body))
	types := eventTypes(events)
	// The heuristic parser coalesces adjacent text chunks into ONE text delta
	// ("Hello world" at finalize flush) — verified against the TS reference
	// (bun run): TYPES = [message_start, content_block_start,
	// content_block_delta, content_block_stop, message_delta, message_stop].
	// The brief's inline expectation of two deltas does not match the
	// reference; the TS sse_stream.test.ts vector itself only checks content
	// integrity, not delta count.
	want := []string{"message_start", "content_block_start", "content_block_delta", "content_block_stop", "message_delta", "message_stop"}
	if strings.Join(types, ",") != strings.Join(want, ",") {
		t.Fatalf("types = %v\nbody = %s", types, body)
	}
	// All upstream text must appear downstream (coalesced or not).
	var textDeltas []string
	for _, e := range events {
		if e.event == "content_block_delta" && strings.Contains(e.data, "text_delta") {
			var d struct {
				Delta struct {
					Text string
				}
			}
			json.Unmarshal([]byte(e.data), &d)
			textDeltas = append(textDeltas, d.Delta.Text)
		}
	}
	if strings.Join(textDeltas, "") != "Hello world" {
		t.Errorf("text = %v", textDeltas)
	}
	// message_delta stop_reason
	for _, e := range events {
		if e.event == "message_delta" {
			if !strings.Contains(e.data, `"stop_reason":"end_turn"`) {
				t.Errorf("delta = %s", e.data)
			}
		}
	}
}

func TestUpstreamIncludesStreamOptions(t *testing.T) {
	var gotBody map[string]any
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		json.Unmarshal(b, &gotBody)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer up.Close()
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	so, _ := gotBody["stream_options"].(map[string]any)
	if so == nil || so["include_usage"] != true {
		t.Errorf("stream_options = %v", gotBody["stream_options"])
	}
	if gotBody["stream"] != true {
		t.Errorf("stream = %v", gotBody["stream"])
	}
}

func TestCORS(t *testing.T) {
	h := NewHandler(testConfig("http://unused"))
	req := httptest.NewRequest("OPTIONS", "/v1/messages", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 204 {
		t.Errorf("code = %d", rec.Code)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS origin missing")
	}
}

func TestDumpWiring(t *testing.T) {
	upstreamBody := "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n" +
		"data: [DONE]\n\n"
	up := mockUpstream(t, upstreamBody, 200)
	cfg := testConfig(up.URL)
	cfg.DumpDir = t.TempDir()
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	io.Copy(io.Discard, resp.Body)
	// dump 完成后应有 completed/ 目录
	entries, err := os.ReadDir(filepath.Join(cfg.DumpDir, "completed"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("completed dump missing: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Ported vectors from tests/sse_stream.test.ts
// ---------------------------------------------------------------------------

// typicalUpstreamSSE mirrors typicalUpstreamSse() in sse_stream.test.ts.
func typicalUpstreamSSE() string {
	return "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"!\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n" +
		"data: [DONE]\n\n"
}

func TestTypicalStreamFullLifecycle(t *testing.T) {
	up := mockUpstream(t, typicalUpstreamSSE(), 200)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	events := parseSSE(t, string(body))
	types := eventTypes(events)
	for _, want := range []string{"message_start", "content_block_start", "content_block_delta", "content_block_stop", "message_delta", "message_stop"} {
		found := false
		for _, ty := range types {
			if ty == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing event %q in %v", want, types)
		}
	}
	// Content integrity (the heuristic parser may coalesce deltas).
	for _, want := range []string{"Hello", " world", "!"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("content %q missing: %s", want, body)
		}
	}
}

func TestLongStreamNoDroppedEvents(t *testing.T) {
	var chunks []string
	chunks = append(chunks, "data: {\"id\":\"chatcmpl-long\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n")
	eventCount := 50
	for i := 0; i < eventCount; i++ {
		chunks = append(chunks, "data: {\"id\":\"chatcmpl-long\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"chunk"+strconv.Itoa(i)+" \"},\"finish_reason\":null}]}\n\n")
	}
	chunks = append(chunks, "data: {\"id\":\"chatcmpl-long\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":"+strconv.Itoa(eventCount+2)+"}}\n\n")
	chunks = append(chunks, "data: [DONE]\n\n")

	up := mockUpstream(t, strings.Join(chunks, ""), 200)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	body, _ := io.ReadAll(resp.Body)
	for i := 0; i < eventCount; i++ {
		if !strings.Contains(string(body), "chunk"+strconv.Itoa(i)) {
			t.Fatalf("chunk%d missing — events dropped", i)
		}
	}
	events := eventTypes(parseSSE(t, string(body)))
	hasStop := false
	for _, ty := range events {
		if ty == "message_stop" {
			hasStop = true
		}
	}
	if !hasStop {
		t.Errorf("message_stop missing: %v", events)
	}
}

func TestStreamCompletesNotStalls(t *testing.T) {
	up := mockUpstream(t, typicalUpstreamSSE(), 200)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	body, _ := io.ReadAll(resp.Body)
	events := parseSSE(t, string(body))
	if len(events) <= 3 {
		t.Fatalf("only %d events — stream stalled at message_start", len(events))
	}
	if events[len(events)-1].event != "message_stop" {
		t.Errorf("last event = %s", events[len(events)-1].event)
	}
	if delta := eventData(events, "message_delta"); delta != nil {
		usage, _ := delta["usage"].(map[string]any)
		if out, _ := usage["output_tokens"].(float64); out <= 1 {
			t.Errorf("output_tokens = %v (bug manifested as 1)", usage["output_tokens"])
		}
	}
}

func TestUpstreamAbortSurfacesStreamError(t *testing.T) {
	// Empty delta — no content is emitted before the abort, exercising the
	// no-content re-throw path (top-level event: error).
	goodPrefix := "data: {\"id\":\"chatcmpl-err\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":null}]}\n\n"
	up := mockUpstreamAbort(t, goodPrefix)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	events := parseSSE(t, string(body))
	types := eventTypes(events)

	if !contains(types, "message_start") {
		t.Errorf("message_start missing: %v", types)
	}
	if !contains(types, "error") {
		t.Fatalf("error event missing: %v\nbody = %s", types, body)
	}
	parsed := eventData(events, "error")
	errObj, _ := parsed["error"].(map[string]any)
	if errObj == nil || errObj["type"] != "overloaded_error" {
		t.Errorf("error.type = %v", errObj)
	}
		msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "Connection closed mid-response. The response above may be incomplete") {
		t.Errorf("message = %q", msg)
	}
	// The error must NOT be disguised as assistant text, and the turn must NOT
	// be reported as completed.
	if contains(types, "message_delta") || contains(types, "message_stop") {
		t.Errorf("no lifecycle events after error: %v", types)
	}
	if strings.Contains(string(body), "text_delta") {
		t.Errorf("error wrapped in text content: %s", body)
	}
	if strings.Contains(string(body), "[DONE]") {
		t.Errorf("body contains [DONE]: %s", body)
	}
}

func TestErrorEventAfterConnectionDrop(t *testing.T) {
	up := mockUpstreamAbort(t, "data: {\"id\":\"chatcmpl-conn\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n")
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if !strings.Contains(s, "Hello") {
		t.Errorf("content missing: %s", s)
	}
	types := eventTypes(parseSSE(t, s))
	if !contains(types, "error") {
		t.Fatalf("error event missing: %v\nbody = %s", types, s)
	}
	parsed := eventData(parseSSE(t, s), "error")
	errObj, _ := parsed["error"].(map[string]any)
	if errObj == nil || errObj["type"] != "overloaded_error" {
		t.Errorf("error.type = %v", errObj)
	}
	// No message_delta/message_stop after the error event.
	if contains(types, "message_delta") || contains(types, "message_stop") {
		t.Errorf("no lifecycle events after error: %v", types)
	}
}

func TestErrorEventAfterEmbeddedError(t *testing.T) {
	sseBody := "data: {\"id\":\"chatcmpl-err\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial output\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl-err\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"error\":{\"message\":\"Server overloaded\",\"code\":529},\"choices\":[]}\n\n"
	up := mockUpstream(t, sseBody, 200)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if !strings.Contains(s, "partial output") {
		t.Errorf("content missing: %s", s)
	}
	types := eventTypes(parseSSE(t, s))
	if !contains(types, "error") {
		t.Fatalf("error event missing: %v\nbody = %s", types, s)
	}
	parsed := eventData(parseSSE(t, s), "error")
	errObj, _ := parsed["error"].(map[string]any)
	if errObj == nil || errObj["type"] != "overloaded_error" {
		t.Errorf("error.type = %v", errObj)
	}
	// No message_delta/message_stop after the error event.
	if contains(types, "message_delta") || contains(types, "message_stop") {
		t.Errorf("no lifecycle events after error: %v", types)
	}
}

func TestErrorEventAfterStall(t *testing.T) {
	// Content, then a clean EOF with no finish_reason and no [DONE] — the
	// response_stalled path.
	up := mockUpstream(t, "data: {\"id\":\"chatcmpl-stall\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"stalled output\"},\"finish_reason\":null}]}\n\n", 200)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if !strings.Contains(s, "stalled output") {
		t.Errorf("content missing: %s", s)
	}
	types := eventTypes(parseSSE(t, s))
	if !contains(types, "error") {
		t.Fatalf("error event missing: %v\nbody = %s", types, s)
	}
	parsed := eventData(parseSSE(t, s), "error")
	errObj, _ := parsed["error"].(map[string]any)
	if errObj == nil || errObj["type"] != "overloaded_error" {
		t.Errorf("error.type = %v", errObj)
	}
	if contains(types, "message_delta") || contains(types, "message_stop") {
		t.Errorf("no lifecycle events after error: %v", types)
	}
}

func TestDoneWithoutFinishReasonGraceful(t *testing.T) {
	sseBody := "data: {\"id\":\"chatcmpl-done\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl-done\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi there\"},\"finish_reason\":null}]}\n\n" +
		"data: [DONE]\n\n"
	up := mockUpstream(t, sseBody, 200)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	body, _ := io.ReadAll(resp.Body)
	events := parseSSE(t, string(body))
	types := eventTypes(events)
	for _, want := range []string{"message_start", "message_delta", "message_stop"} {
		if !contains(types, want) {
			t.Errorf("missing %s in %v", want, types)
		}
	}
	if delta := eventData(events, "message_delta"); delta != nil {
		if !strings.Contains(string(body), `"stop_reason":"end_turn"`) {
			t.Errorf("delta = %s", string(body))
		}
	}
	if contains(types, "error") {
		t.Errorf("unexpected error event: %v", types)
	}
	if strings.Contains(string(body), "without a finish_reason") || strings.Contains(string(body), "Upstream stream ended") {
		t.Errorf("abort message surfaced: %s", body)
	}
}

func TestCRLFHandling(t *testing.T) {
	crlfSse := "data: {\"id\":\"crlf\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\r\n\r\n" +
		"data: {\"id\":\"crlf\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"CRLF works\"},\"finish_reason\":null}]}\r\n\r\n" +
		"data: {\"id\":\"crlf\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\r\n\r\n" +
		"data: [DONE]\r\n\r\n"
	up := mockUpstream(t, crlfSse, 200)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	body, _ := io.ReadAll(resp.Body)
	types := eventTypes(parseSSE(t, string(body)))
	if !contains(types, "message_start") || !contains(types, "message_stop") {
		t.Errorf("types = %v", types)
	}
	if !strings.Contains(string(body), "CRLF works") {
		t.Errorf("content missing: %s", body)
	}
}

func TestSplitReadFrames(t *testing.T) {
	full := typicalUpstreamSSE()
	mid := len(full) / 3
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		io.WriteString(w, full[:mid])
		fl.Flush()
		time.Sleep(5 * time.Millisecond)
		io.WriteString(w, full[mid:])
	}))
	t.Cleanup(srv.Close)
	h := NewHandler(testConfig(srv.URL))
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	body, _ := io.ReadAll(resp.Body)
	types := eventTypes(parseSSE(t, string(body)))
	if !contains(types, "message_stop") {
		t.Errorf("stream incomplete across split frames: %v", types)
	}
}

func TestDownstreamAbortStopsPump(t *testing.T) {
	var chunks []string
	chunks = append(chunks, "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n")
	for i := 0; i < 30; i++ {
		chunks = append(chunks, "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"chunk"+strconv.Itoa(i)+" \"},\"finish_reason\":null}]}\n\n")
	}
	up, flushed := mockUpstreamDelayed(t, chunks, 5*time.Millisecond)
	h := NewHandler(testConfig(up.URL))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec, done := startPost(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, ctx)

	// Wait for several chunks to be flushed (and thus written to the
	// recorder), then cancel like a disconnecting client.
	waitFor(t, "content to flow", 5*time.Second, func() bool {
		return flushed() >= 3 && strings.Contains(rec.body(), "message_start")
	})
	select {
	case <-done:
		t.Fatal("stream completed before cancel")
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after cancel")
	}

	partial := rec.body()
	if partial == "" {
		t.Fatal("no content received before abort")
	}
	if strings.Contains(partial, "event: message_stop") {
		t.Errorf("message_stop must not be emitted after downstream abort")
	}
}

func TestClientAbortBeforeUpstreamResponds(t *testing.T) {
	up, release, started := mockUpstreamBlocking(t)
	cfg := testConfig(up.URL)
	cfg.DumpDir = t.TempDir()
	h := NewHandler(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	rec, done := startPost(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, ctx)

	// Wait until the request is in-flight at the upstream, then abort the
	// client connection (cancels the request context, propagated to the
	// upstream fetch via NewRequestWithContext).
	waitFor(t, "upstream request to start", 5*time.Second, started)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after cancel")
	}
	close(release)

	if rec.Code != 499 {
		t.Fatalf("499 expected, got %d", rec.Code)
	}
	if !strings.Contains(rec.body(), "Client disconnected before upstream responded") {
		t.Errorf("body = %s", rec.body())
	}
	// The dump session must have been finalized (client-aborted bucket).
	entries, err := os.ReadDir(filepath.Join(cfg.DumpDir, "client-aborted"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("client-aborted dump missing: %v", err)
	}
}

// readDumpEntry returns the first renamed dump directory for the bucket.
func readDumpEntry(t *testing.T, dumpDir, bucket string) string {
	t.Helper()
	dir := filepath.Join(dumpDir, bucket)
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("no dump entry in %s: %v", bucket, err)
	}
	return filepath.Join(dir, entries[0].Name())
}

func TestDumpFinalizedOnMidStreamError(t *testing.T) {
	goodPrefix := "data: {\"id\":\"chatcmpl-dump-finalize\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":null}]}\n\n"
	up := mockUpstreamAbort(t, goodPrefix)
	cfg := testConfig(up.URL)
	cfg.DumpDir = t.TempDir()
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if !strings.Contains(s, "Connection closed mid-response. The response above may be incomplete") {
		t.Errorf("body = %s", s)
	}
	if !strings.Contains(s, `"type":"overloaded_error"`) {
		t.Errorf("body = %s", s)
	}

	// Dump finalized: renamed dir with all expected log files.
	renamedDir := readDumpEntry(t, cfg.DumpDir, "upstream-aborted")
	for _, f := range []string{"downstream-request.log", "upstream-request.log", "upstream-response.log", "downstream-response.log"} {
		if _, err := os.Stat(filepath.Join(renamedDir, f)); err != nil {
			t.Errorf("dump file %s missing: %v", f, err)
		}
	}
}

func TestUpstreamAbortWithContentCategorized(t *testing.T) {
	// Content, then a connection reset. The test client reads the response to
	// completion — a genuine upstream abort, NOT client-initiated.
	// With the fix, the hadContent path emits an error event (not a completed turn).
	up := mockUpstreamAbort(t, "data: {\"id\":\"chatcmpl-caseB\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n")
	cfg := testConfig(up.URL)
	cfg.DumpDir = t.TempDir()
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	body, _ := io.ReadAll(resp.Body)
	s := string(body)

	// Must contain the error event.
	if !contains(eventTypes(parseSSE(t, s)), "error") {
		t.Errorf("error event missing: %s", s)
	}
	// Must NOT contain message_delta or message_stop.
	if contains(eventTypes(parseSSE(t, s)), "message_delta") || contains(eventTypes(parseSSE(t, s)), "message_stop") {
		t.Errorf("no lifecycle events after error: %s", s)
	}

	// Root cause = upstream abort → upstream-aborted/ bucket.
	renamedDir := readDumpEntry(t, cfg.DumpDir, "upstream-aborted")
	// upstream-response.log records the TRUE upstream outcome.
	upResp, err := os.ReadFile(filepath.Join(renamedDir, "upstream-response.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(upResp), "Reason: upstream_abort") {
		t.Errorf("upstream-response.log = %s", upResp)
	}
	if !strings.Contains(string(upResp), "DisconnectTime:") {
		t.Errorf("upstream-response.log = %s", upResp)
	}
	// downstream-response.log records the downstream protocol outcome.
	downResp, err := os.ReadFile(filepath.Join(renamedDir, "downstream-response.log"))
	if err != nil {
		t.Fatal(err)
	}
	// Downstream also shows upstream_abort (error event, not a completed turn).
	if !strings.Contains(string(downResp), "Reason: upstream_abort") {
		t.Errorf("downstream-response.log = %s", downResp)
	}
	if strings.Contains(string(downResp), "Reason: completed") {
		t.Errorf("downstream-response.log = %s", downResp)
	}
}

func TestClientAbortCategorized(t *testing.T) {
	var chunks []string
	chunks = append(chunks, "data: {\"id\":\"chatcmpl-clientabort\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n")
	for i := 0; i < 50; i++ {
		chunks = append(chunks, "data: {\"id\":\"chatcmpl-clientabort\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"chunk"+strconv.Itoa(i)+" \"},\"finish_reason\":null}]}\n\n")
	}
	up, flushed := mockUpstreamDelayed(t, chunks, 5*time.Millisecond)
	cfg := testConfig(up.URL)
	cfg.DumpDir = t.TempDir()
	h := NewHandler(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec, done := startPost(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, ctx)

	waitFor(t, "content to flow", 5*time.Second, func() bool {
		return flushed() >= 3 && strings.Contains(rec.body(), "message_start")
	})
	select {
	case <-done:
		t.Fatal("stream completed before cancel")
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after cancel")
	}

	// Root cause = client disconnect → client-aborted/.
	renamedDir := readDumpEntry(t, cfg.DumpDir, "client-aborted")
	if _, err := os.Stat(filepath.Join(cfg.DumpDir, "upstream-aborted")); err == nil {
		t.Errorf("must NOT be categorized as upstream-aborted")
	}
	upResp, err := os.ReadFile(filepath.Join(renamedDir, "upstream-response.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(upResp), "Reason: client_abort") {
		t.Errorf("upstream-response.log = %s", upResp)
	}
	if strings.Contains(string(upResp), "Reason: upstream_abort") {
		t.Errorf("upstream-response.log = %s", upResp)
	}
}

func TestPreserveCallerStreamOptions(t *testing.T) {
	var gotBody map[string]any
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		json.Unmarshal(b, &gotBody)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, typicalUpstreamSSE())
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	cfg.ModelOverrides = []config.ModelOverride{{Pattern: "*", Extra: map[string]any{"stream_options": map[string]any{"count_tokens": true}}}}
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	io.Copy(io.Discard, resp.Body)
	so, _ := gotBody["stream_options"].(map[string]any)
	if so == nil || so["include_usage"] != true {
		t.Errorf("include_usage missing: %v", gotBody["stream_options"])
	}
	if so["count_tokens"] != true {
		t.Errorf("count_tokens clobbered: %v", gotBody["stream_options"])
	}
}

func TestCanonicalBodyPrivateParams(t *testing.T) {
	var gotBody map[string]any
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		json.Unmarshal(b, &gotBody)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, typicalUpstreamSSE())
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	// A `_`-prefixed private param from --upstream-extra-params must be
	// dropped by PrepareCanonicalBody.
	cfg.ModelOverrides = []config.ModelOverride{{Pattern: "*", Extra: map[string]any{"_private": "x"}}}
	h := NewHandler(cfg)
	reqBody := `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024,` +
		`"tools":[{"name":"t","description":"d","input_schema":{"type":"object","properties":{"_id":{"type":"string"},"name":{"type":"string"}}}}]}`
	resp := postMessages(t, h, reqBody, map[string]string{"x-api-key": "test-key"})
	io.Copy(io.Discard, resp.Body)
	if _, has := gotBody["_private"]; has {
		t.Errorf("private param not dropped: %v", gotBody)
	}
	tools, _ := gotBody["tools"].([]any)
	if len(tools) == 0 {
		t.Fatalf("tools missing: %v", gotBody)
	}
	tool, _ := tools[0].(map[string]any)
	fn, _ := tool["function"].(map[string]any)
	params, _ := fn["parameters"].(map[string]any)
	props, _ := params["properties"].(map[string]any)
	if props == nil {
		t.Fatalf("properties missing: %v", params)
	}
	// `_id` lives inside the JSON-Schema properties name map and must survive.
	if _, has := props["_id"]; !has {
		t.Errorf("_id schema property stripped: %v", props)
	}
	if _, has := props["name"]; !has {
		t.Errorf("name schema property missing: %v", props)
	}
}

func TestRealInputTokensEndToEnd(t *testing.T) {
	sseBody := "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":7,\"prompt_tokens_details\":{\"cached_tokens\":30,\"cache_write_tokens\":10}}}\n\n" +
		"data: [DONE]\n\n"
	up := mockUpstream(t, sseBody, 200)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "test-key"})
	body, _ := io.ReadAll(resp.Body)
	events := parseSSE(t, string(body))
	delta := eventData(events, "message_delta")
	if delta == nil {
		t.Fatalf("message_delta missing: %s", body)
	}
	usage, _ := delta["usage"].(map[string]any)
	// input = 100 - 30 (cached) - 10 (cache_write) = 60
	if usage["input_tokens"] != float64(60) {
		t.Errorf("input_tokens = %v", usage["input_tokens"])
	}
	if usage["cache_read_input_tokens"] != float64(30) {
		t.Errorf("cache_read_input_tokens = %v", usage["cache_read_input_tokens"])
	}
	if usage["cache_creation_input_tokens"] != float64(10) {
		t.Errorf("cache_creation_input_tokens = %v", usage["cache_creation_input_tokens"])
	}
	if usage["output_tokens"] != float64(7) {
		t.Errorf("output_tokens = %v", usage["output_tokens"])
	}
}

func TestServerToolsPassthroughNot400(t *testing.T) {
	up := mockUpstream(t, "data: [DONE]\n\n", 200)
	h := NewHandler(testConfig(up.URL))
	// server_tools with a server tool type; server tools are NOT enabled, so
	// the request must flow to the upstream (not 400).
	resp := postMessages(t, h, `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"server_tools":[{"type":"web_search_20250305","name":"web_search"}]}`, map[string]string{"x-api-key": "test-key"})
	if resp.StatusCode == 400 {
		t.Fatalf("server_tools request must not 400")
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestEmptyUpstreamBody500(t *testing.T) {
	up := mockUpstream(t, "", 200)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != 500 {
		t.Fatalf("500 expected, got %d", resp.StatusCode)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, "Upstream returned empty body.") {
		t.Errorf("body = %s", body)
	}
}

func TestChatCompletionsPassthroughVerbatim(t *testing.T) {
	body := `{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true,"extra":"保留"}` // 含非 ASCII + 未知 key
	var capturedBody string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		capturedBody = string(b)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n")
	}))
	defer up.Close()
	h := NewHandler(testConfig(up.URL))
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	// Body forwarded byte-for-byte (including the non-ASCII bytes).
	if capturedBody != body {
		t.Errorf("upstream body mismatch:\n got: %q\nwant: %q", capturedBody, body)
	}
	// Response relayed verbatim + CORS.
	out := rec.Body.String()
	if !strings.Contains(out, "chat.completion.chunk") || !strings.Contains(out, "[DONE]") {
		t.Errorf("response not relayed verbatim: %s", out)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS missing")
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}
	// SSE keep-alive headers present for event-stream responses.
	if rec.Header().Get("X-Accel-Buffering") != "no" {
		t.Error("X-Accel-Buffering missing on SSE passthrough")
	}
}

func TestChatCompletionsPassthroughNonStream(t *testing.T) {
	jsonBody := `{"id":"cmpl-1","object":"chat.completion","model":"m","choices":[{"message":{"role":"assistant","content":"hi"}}]}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, jsonBody)
	}))
	defer up.Close()
	h := NewHandler(testConfig(up.URL))
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[],"stream":false}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Body.String(); got != jsonBody {
		t.Errorf("non-stream body not relayed verbatim:\n got: %q\nwant: %q", got, jsonBody)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	// Non-event-stream responses must NOT carry SSE keep-alive headers.
	if rec.Header().Get("X-Accel-Buffering") != "" {
		t.Error("X-Accel-Buffering set on non-stream response")
	}
}

func TestModelsPassthrough(t *testing.T) {
	models := `{"object":"list","data":[{"id":"gpt-4o","object":"model","owned_by":"openai"}]}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/models" {
			t.Errorf("upstream got %s %s, want GET /models", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, models)
	}))
	defer up.Close()
	h := NewHandler(testConfig(up.URL))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/models", nil))

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Body.String(); got != models {
		t.Errorf("models body not relayed verbatim:\n got: %q\nwant: %q", got, models)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS missing")
	}
}

func TestChatCompletionsPassthroughForwardsClientKey(t *testing.T) {
	var gotAuth string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	cfg.UpstreamAPIKey = ""
	cfg.AuthToken = "" // 透传模式
	h := NewHandler(cfg)
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "client-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(gotAuth, "client-key") {
		t.Errorf("client key not forwarded: %q", gotAuth)
	}
}

func TestPassthroughAuthRequired(t *testing.T) {
	up := mockUpstream(t, "data: [DONE]\n\n", 200)
	cfg := testConfig(up.URL)
	cfg.AuthToken = "secret"
	h := NewHandler(cfg)

	// POST /v1/chat/completions without token → 401.
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Fatalf("chat completions 401 expected, got %d", rec.Code)
	}
	// OpenAI-format error body (passthrough clients parse this shape).
	if !strings.Contains(rec.Body.String(), `"error"`) || !strings.Contains(rec.Body.String(), "authentication_error") {
		t.Errorf("auth error body = %s", rec.Body.String())
	}

	// GET /v1/models without token → 401.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("GET", "/v1/models", nil))
	if rec2.Code != 401 {
		t.Fatalf("models 401 expected, got %d", rec2.Code)
	}

	// With correct x-api-key → 200.
	req2 := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("x-api-key", "secret")
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, req2)
	if rec3.Code != 200 {
		t.Fatalf("200 expected with token, got %d", rec3.Code)
	}
}

func TestModelsPassthroughMissingKey401(t *testing.T) {
	// Passthrough mode (no upstream key, no auth token) with no client key:
	// resolveAPIKey yields empty → 401.
	up := mockUpstream(t, "data: [DONE]\n\n", 200)
	cfg := testConfig(up.URL)
	cfg.UpstreamAPIKey = ""
	cfg.AuthToken = ""
	h := NewHandler(cfg)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/models", nil))
	if rec.Code != 401 {
		t.Fatalf("401 expected (passthrough mode, no client key), got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Multi-upstream routing: /v1/messages through the candidate chain.
// ---------------------------------------------------------------------------

func TestNoRouteForModel(t *testing.T) {
	var upstreamHit bool
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHit = true
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := &config.Config{
		Upstreams: []*config.Upstream{{Name: "a", BaseURL: up.URL}},
		Routes:    []config.Route{{Pattern: "glm-*", Names: []string{"a"}}},
	}
	h := NewHandler(cfg)
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"deepseek-v4","messages":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 400 || !strings.Contains(rec.Body.String(), "no route configured") {
		t.Fatalf("want 400 no route, got %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "glm-*") {
		t.Errorf("configured patterns missing from body: %s", rec.Body.String())
	}
	if upstreamHit {
		t.Error("upstream must not be reached when no route matches")
	}
}

func TestRoutedToConfiguredUpstream(t *testing.T) {
	var hitModel, gotAuth string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		hitModel, _ = b["model"].(string)
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, typicalUpstreamSSE())
	}))
	defer up.Close()
	cfg := &config.Config{
		Upstreams: []*config.Upstream{{Name: "z1", BaseURL: up.URL, APIKey: "k-z"}},
		Routes:    []config.Route{{Pattern: "*", Names: []string{"z1"}}},
	}
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"glm-4.7","messages":[{"role":"user","content":"Hello"}],"max_tokens":1024}`, map[string]string{"x-api-key": "client-key"})
	if resp.StatusCode != 200 {
		t.Fatalf("200 expected, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !contains(eventTypes(parseSSE(t, string(body))), "message_stop") {
		t.Errorf("message_stop missing: %s", body)
	}
	if hitModel != "glm-4.7" {
		t.Errorf("upstream model = %q, want glm-4.7", hitModel)
	}
	// effectiveKey: the upstream's own key wins over the client key.
	if !strings.Contains(gotAuth, "k-z") {
		t.Errorf("upstream key not used: %q", gotAuth)
	}
}

// ---------------------------------------------------------------------------
// Task 5: pre-stream failover across the candidate chain.
// ---------------------------------------------------------------------------

func TestFailoverOn500(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `{"error":{"message":"boom"}}`)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, typicalUpstreamSSE())
	}))
	defer second.Close()

	cfg := &config.Config{
		Upstreams: []*config.Upstream{
			{Name: "bad", BaseURL: first.URL},
			{Name: "good", BaseURL: second.URL},
		},
		Routes:  []config.Route{{Pattern: "*", Names: []string{"bad", "good"}}},
		DumpDir: t.TempDir(),
	}
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"x-api-key": "client-key"})
	if resp.StatusCode != 200 {
		t.Fatalf("200 expected after failover, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	events := eventTypes(parseSSE(t, string(body)))
	if !contains(events, "message_start") || !contains(events, "message_stop") {
		t.Errorf("good upstream stream incomplete: %v\nbody = %s", events, body)
	}
	// The skipped attempt must be in the dump attempt trail.
	renamedDir := readDumpEntry(t, cfg.DumpDir, "completed")
	data, err := os.ReadFile(filepath.Join(renamedDir, "upstream-attempts.log"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	if !strings.Contains(s, "bad") || !strings.Contains(s, "skipped") {
		t.Errorf("attempts log missing bad/skipped entry: %s", s)
	}
	if !strings.Contains(s, "Status: 500") {
		t.Errorf("attempts log missing 500 status: %s", s)
	}
}

func TestNoFailoverOn400(t *testing.T) {
	var secondHit bool
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"error":{"message":"not here"}}`)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHit = true
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer second.Close()

	cfg := &config.Config{
		Upstreams: []*config.Upstream{
			{Name: "bad", BaseURL: first.URL},
			{Name: "good", BaseURL: second.URL},
		},
		Routes: []config.Route{{Pattern: "*", Names: []string{"bad", "good"}}},
	}
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"x-api-key": "client-key"})
	if resp.StatusCode != 404 {
		t.Fatalf("404 expected (non-retryable passes through), got %d", resp.StatusCode)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, "Upstream returned 404") {
		t.Errorf("body = %s", body)
	}
	if secondHit {
		t.Error("failover must not happen on non-retryable status")
	}
}

func TestFailoverOnConnectRefused(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close() // port released → connection refused

	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, typicalUpstreamSSE())
	}))
	defer second.Close()

	cfg := &config.Config{
		Upstreams: []*config.Upstream{
			{Name: "dead", BaseURL: deadURL},
			{Name: "good", BaseURL: second.URL},
		},
		Routes: []config.Route{{Pattern: "*", Names: []string{"dead", "good"}}},
	}
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"x-api-key": "client-key"})
	if resp.StatusCode != 200 {
		t.Fatalf("200 expected after failover, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	events := eventTypes(parseSSE(t, string(body)))
	if !contains(events, "message_start") || !contains(events, "message_stop") {
		t.Errorf("good upstream stream incomplete: %v\nbody = %s", events, body)
	}
}

func TestRoundRobinDistribution(t *testing.T) {
	var hits1, hits2 atomic.Int64
	up1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits1.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer up1.Close()
	up2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits2.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer up2.Close()

	cfg := &config.Config{
		Upstreams: []*config.Upstream{
			{Name: "u1", BaseURL: up1.URL},
			{Name: "u2", BaseURL: up2.URL},
		},
		Routes: []config.Route{{Pattern: "*", Names: []string{"u1", "u2"}}},
	}
	h := NewHandler(cfg)
	for i := 0; i < 2; i++ {
		resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"x-api-key": "client-key"})
		if resp.StatusCode != 200 {
			t.Fatalf("request %d: 200 expected, got %d", i, resp.StatusCode)
		}
		io.Copy(io.Discard, resp.Body)
	}
	// Two requests over a two-upstream pool: the round-robin cursor alternates
	// the chain start, and both upstreams are healthy → one hit each.
	if hits1.Load() != 1 || hits2.Load() != 1 {
		t.Errorf("hits = %d/%d, want 1/1", hits1.Load(), hits2.Load())
	}
}

// ---------------------------------------------------------------------------
// Task 6: passthrough endpoints routed by body model / merged models.
// ---------------------------------------------------------------------------

func TestChatCompletionsRoutedByBodyModel(t *testing.T) {
	hitA, hitB := atomic.Bool{}, atomic.Bool{}
	var gotBodyA string
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBodyA = string(b)
		hitA.Store(true)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitB.Store(true)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer b.Close()
	cfg := &config.Config{
		Upstreams: []*config.Upstream{{Name: "a", BaseURL: a.URL, APIKey: "k-a"}, {Name: "b", BaseURL: b.URL, APIKey: "k-b"}},
		Routes: []config.Route{
			{Pattern: "glm-*", Names: []string{"a"}},
			{Pattern: "*", Names: []string{"b"}},
		},
	}
	h := NewHandler(cfg)

	post := func(body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// glm-4 matches glm-* → upstream a only; raw body forwarded verbatim.
	body1 := `{"model":"glm-4","messages":[]}`
	if rec := post(body1); rec.Code != 200 {
		t.Fatalf("200 expected, got %d %s", rec.Code, rec.Body.String())
	}
	if !hitA.Load() || hitB.Load() {
		t.Fatalf("hits = a:%v b:%v, want a:true b:false", hitA.Load(), hitB.Load())
	}
	if gotBodyA != body1 {
		t.Errorf("upstream body mismatch:\n got: %q\nwant: %q", gotBodyA, body1)
	}

	// other matches * → upstream b.
	if rec := post(`{"model":"other","messages":[]}`); rec.Code != 200 {
		t.Fatalf("200 expected, got %d %s", rec.Code, rec.Body.String())
	}
	if !hitB.Load() {
		t.Error("catch-all route did not reach upstream b")
	}

	// Missing model → 400 in OpenAI error format.
	rec := post(`{"messages":[]}`)
	if rec.Code != 400 {
		t.Fatalf("400 expected for missing model, got %d", rec.Code)
	}
	out := rec.Body.String()
	if !strings.Contains(out, `"error"`) || !strings.Contains(out, "invalid_request_error") {
		t.Errorf("body = %s", out)
	}

	// Non-string model is equally rejected.
	if rec := post(`{"model":123,"messages":[]}`); rec.Code != 400 {
		t.Errorf("400 expected for non-string model, got %d", rec.Code)
	}

	// No matching route → 400 listing configured patterns (no catch-all here).
	h2 := NewHandler(&config.Config{
		Upstreams: []*config.Upstream{{Name: "a", BaseURL: a.URL, APIKey: "k-a"}},
		Routes:    []config.Route{{Pattern: "glm-*", Names: []string{"a"}}},
	})
	req2 := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"unknown-model","messages":[]}`))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	h2.ServeHTTP(rec2, req2)
	if rec2.Code != 400 || !strings.Contains(rec2.Body.String(), "no route configured") {
		t.Fatalf("want 400 no route, got %d %s", rec2.Code, rec2.Body.String())
	}
	if !strings.Contains(rec2.Body.String(), "glm-*") {
		t.Errorf("configured patterns missing from body: %s", rec2.Body.String())
	}
}

func TestModelsMergedAcrossUpstreams(t *testing.T) {
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"object":"list","data":[{"id":"glm-4"},{"id":"shared"}]}`)
	}))
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"object":"list","data":[{"id":"deepseek-v4"},{"id":"shared"}]}`)
	}))
	cfg := &config.Config{
		Upstreams: []*config.Upstream{{Name: "a", BaseURL: a.URL, APIKey: "k-a"}, {Name: "b", BaseURL: b.URL, APIKey: "k-b"}},
		Routes: []config.Route{
			{Pattern: "glm-*", Names: []string{"a"}},
			{Pattern: "*", Names: []string{"b"}},
		},
	}
	h := NewHandler(cfg)

	getModels := func() (int, string, []string) {
		t.Helper()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/models", nil))
		var doc struct {
			Data []struct{ Id string `json:"id"` }
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &doc)
		var ids []string
		for _, m := range doc.Data {
			ids = append(ids, m.Id)
		}
		return rec.Code, rec.Body.String(), ids
	}

	code, body, ids := getModels()
	if code != 200 {
		t.Fatalf("200 expected, got %d", code)
	}
	want := []string{"glm-4", "deepseek-v4", "shared"} // 去重,a(声明序)的 shared 优先
	if strings.Join(ids, ",") != strings.Join(want, ",") {
		t.Errorf("ids = %v, want %v", ids, want)
	}

	// b goes down: partial degradation still serves a's models.
	b.Close()
	code, body, ids = getModels()
	if code != 200 {
		t.Fatalf("200 expected with one upstream down, got %d %s", code, body)
	}
	want = []string{"glm-4", "shared"}
	if strings.Join(ids, ",") != strings.Join(want, ",") {
		t.Errorf("degraded ids = %v, want %v", ids, want)
	}
}

func TestModelsSingleUpstreamVerbatim(t *testing.T) {
	modelsBody := `{"object":"list","data":[{"id":"m1","custom":true}],"owned_by":"x"}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/models" {
			t.Errorf("upstream got %s %s, want GET /models", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, modelsBody)
	}))
	defer up.Close()
	// File-mode style single upstream.
	cfg := &config.Config{
		Upstreams: []*config.Upstream{{Name: "f1", BaseURL: up.URL, APIKey: "k-f"}},
		Routes:    []config.Route{{Pattern: "*", Names: []string{"f1"}}},
	}
	h := NewHandler(cfg)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/models", nil))

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	// Single upstream: response must be byte-for-byte identical to the upstream's.
	if got := rec.Body.String(); got != modelsBody {
		t.Errorf("verbatim mismatch:\n got: %q\nwant: %q", got, modelsBody)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

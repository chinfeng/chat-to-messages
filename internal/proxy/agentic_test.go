// Integration tests for the server-tool agentic loop (task-11 brief inline
// vectors) plus unit vectors ported from routes_server_tools.test.ts.
//
// Note on the inline vectors: WebSearchBaseURL points at the same httptest
// server, so the proxy's web_search execution GETs /search?q=...&format=json
// on it — the mocks branch on the path and serve searxng-format results (the
// verbatim brief handler would panic on the empty GET body before the search
// could return results). The call counts count only /chat/completions POSTs:
// each loop round is one POST and the final streaming request is one more
// (the TS reference makes it unconditionally after the loop), so the tool →
// text scenario makes 3 POSTs (2 loop rounds + final), the never-ending
// scenario makes 6 (5 loop rounds + final).
package proxy

import (
	"encoding/json"
	"io"
	"iter"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/chinfeng/chat-to-messages/internal/config"
	"github.com/chinfeng/chat-to-messages/internal/openai"
)

func TestAgenticLoopToolThenText(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// The proxy executes web_search against WebSearchBaseURL (= this
		// server): serve searxng-format results so the tool message carries a
		// web_search_result block (searches are not counted below).
		if r.URL.Path == "/search" {
			io.WriteString(w, `{"results":[{"title":"Golang docs","url":"https://go.dev","description":"web_search_result"}]}`)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		json.Unmarshal(body, &req)
		msgs := req["messages"].([]any)
		n := calls.Add(1)
		if n == 1 {
			// 第一轮：返回 web_search tool_call
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"web_search\",\"arguments\":\"{\\\"query\\\":\\\"golang\\\"}\"}}]}}]}\n\n"+
				"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}}]}\n\n"+
				"data: [DONE]\n\n")
			return
		}
		// 第二轮：检查上游收到了 tool 结果消息
		found := false
		for _, m := range msgs {
			mm := m.(map[string]any)
			if mm["role"] == "tool" {
				content := mm["content"].(string)
				if strings.Contains(content, "web_search_result") {
					found = true
				}
			}
		}
		if !found {
			t.Error("upstream did not receive tool result")
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"final answer\"}}]}\n\n"+
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n"+
			"data: [DONE]\n\n")
	}))
	defer up.Close()

	cfg := testConfig(up.URL)
	cfg.ServerTools = config.ServerToolConfig{WebSearch: true, WebSearchEngine: "searxng", WebSearchBaseURL: up.URL}
	h := NewHandler(cfg)

	body := `{"model":"m","messages":[{"role":"user","content":"search golang"}],
		"server_tools":[{"type":"web_search_20250305","name":"web_search"}]}`
	resp := postMessages(t, h, body, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	events := parseSSE(t, string(raw))
	// 2 轮 loop POST + 1 final streaming POST（TS 在循环结束后无条件发 final）。
	if calls.Load() != 3 {
		t.Errorf("upstream calls = %d, want 3 (2 loop rounds + final)", calls.Load())
	}

	// 工具结果以文本块呈现（不出现 server_tool_use）
	joined := string(raw)
	if strings.Contains(joined, "server_tool_use") {
		t.Errorf("server_tool_use must not be emitted: %s", joined)
	}
	if !strings.Contains(joined, "[Web Search Results]") {
		t.Errorf("search results text missing: %s", joined)
	}
	if !strings.Contains(joined, "final answer") {
		t.Errorf("final text missing: %s", joined)
	}
	// 正常收尾
	types := eventTypes(events)
	if types[len(types)-1] != "message_stop" {
		t.Errorf("last = %v", types)
	}
}

// TestAgenticLoopSearchResultWithoutSnippet covers the two-line rendering when
// a search result has no snippet/description: the downstream text must be
// `[Web Search Results]\n<title>\n<url>` — never a literal "<nil>".
func TestAgenticLoopSearchResultWithoutSnippet(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if r.URL.Path == "/search" {
			// searxng 结果无 description 字段 → 无 snippet
			io.WriteString(w, `{"results":[{"title":"No snippet title","url":"https://example.com/page"}]}`)
			return
		}
		if calls.Add(1) == 1 {
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"web_search\",\"arguments\":\"{\\\"query\\\":\\\"q\\\"}\"}}]}}]}\n\n"+
				"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}}]}\n\n"+
				"data: [DONE]\n\n")
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\n"+
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n"+
			"data: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	cfg.ServerTools = config.ServerToolConfig{WebSearch: true, WebSearchEngine: "searxng", WebSearchBaseURL: up.URL}
	h := NewHandler(cfg)
	body := `{"model":"m","messages":[{"role":"user","content":"x"}],"server_tools":[{"type":"web_search_20250305","name":"web_search"}]}`
	resp := postMessages(t, h, body, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	joined := string(raw)
	// 两行格式：标题\nURL（TS：snippet 缺失时不输出第三行）。SSE 的 data 载荷
	// 把换行转义为字面 \n（反斜杠 n），断言用原始流中的转义形式。
	if !strings.Contains(joined, "[Web Search Results]\\nNo snippet title\\nhttps://example.com/page\"") {
		t.Errorf("two-line search result missing: %s", joined)
	}
	if strings.Contains(joined, "https://example.com/page\\n") {
		t.Errorf("three-line search result rendered with trailing newline: %s", joined)
	}
	if strings.Contains(joined, "<nil>") {
		t.Errorf("literal <nil> leaked into downstream: %s", joined)
	}
	if !strings.Contains(joined, "done") {
		t.Errorf("final text missing: %s", joined)
	}
}

func TestAgenticLoopMaxIterations(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if r.URL.Path == "/search" {
			io.WriteString(w, `{"results":[]}`)
			return
		}
		calls.Add(1)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c\",\"function\":{\"name\":\"web_search\",\"arguments\":\"{\\\"query\\\":\\\"q\\\"}\"}}]}}]}\n\n"+
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}}]}\n\n"+
			"data: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	cfg.ServerTools = config.ServerToolConfig{WebSearch: true, WebSearchEngine: "searxng", WebSearchBaseURL: up.URL}
	h := NewHandler(cfg)
	body := `{"model":"m","messages":[{"role":"user","content":"x"}],"server_tools":[{"type":"web_search_20250305","name":"web_search"}]}`
	resp := postMessages(t, h, body, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	// 5 次 loop 迭代（MAX_ITERATIONS）+ 1 次 final 流式请求 = 6 次上游 POST。
	if calls.Load() != 6 {
		t.Errorf("calls = %d, want 6 (5 loop iterations + 1 final request)", calls.Load())
	}
	io.Copy(io.Discard, resp.Body)
}

func TestAgenticLoopTextEmbeddedToolCall(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if r.URL.Path == "/search" {
			io.WriteString(w, `{"results":[{"title":"T","url":"https://go.dev","description":"snippet"}]}`)
			return
		}
		if calls.Add(1) == 1 {
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Let me search <tool_use>{\\\"name\\\":\\\"web_search\\\",\\\"input\\\":{\\\"query\\\":\\\"q\\\"}}</tool_use> fake result\"}}]}\n\n"+
				"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n"+
				"data: [DONE]\n\n")
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\n"+
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n"+
			"data: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	cfg.ServerTools = config.ServerToolConfig{WebSearch: true, WebSearchEngine: "searxng", WebSearchBaseURL: up.URL}
	h := NewHandler(cfg)
	body := `{"model":"m","messages":[{"role":"user","content":"x"}],"server_tools":[{"type":"web_search_20250305","name":"web_search"}]}`
	resp := postMessages(t, h, body, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3 (2 loop rounds + final)", calls.Load())
	}
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "done") {
		t.Errorf("final text missing: %s", raw)
	}
}

// TestAgenticLoopRealUsageInMessageDelta covers the tee mode: the final
// stream's trailing usage chunk (include_usage) must flow through the
// iter.Pull tee into the outer builder so message_delta reports REAL token
// counts (G1/G4) instead of the estimate.
func TestAgenticLoopRealUsageInMessageDelta(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if r.URL.Path == "/search" {
			io.WriteString(w, `{"results":[]}`)
			return
		}
		n := calls.Add(1)
		if n == 1 {
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c\",\"function\":{\"name\":\"web_search\",\"arguments\":\"{\\\"query\\\":\\\"q\\\"}\"}}]}}]}\n\n"+
				"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}}]}\n\n"+
				"data: [DONE]\n\n")
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"real usage\"}}]}\n\n"+
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":7,\"prompt_tokens_details\":{\"cached_tokens\":30}}}\n\n"+
			"data: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	cfg.ServerTools = config.ServerToolConfig{WebSearch: true, WebSearchEngine: "searxng", WebSearchBaseURL: up.URL}
	h := NewHandler(cfg)
	body := `{"model":"m","messages":[{"role":"user","content":"x"}],"server_tools":[{"type":"web_search_20250305","name":"web_search"}]}`
	resp := postMessages(t, h, body, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	events := parseSSE(t, string(raw))
	delta := eventData(events, "message_delta")
	if delta == nil {
		t.Fatalf("message_delta missing: %s", raw)
	}
	usage, _ := delta["usage"].(map[string]any)
	if usage["output_tokens"] != float64(7) {
		t.Errorf("output_tokens = %v (want real 7 from usage chunk)", usage["output_tokens"])
	}
	// input = 100 - 30 cached = 70 (three-bucket invariant).
	if usage["input_tokens"] != float64(70) {
		t.Errorf("input_tokens = %v (want 70 = 100 - 30 cached)", usage["input_tokens"])
	}
	if usage["cache_read_input_tokens"] != float64(30) {
		t.Errorf("cache_read_input_tokens = %v", usage["cache_read_input_tokens"])
	}
}

// TestAgenticLoopWebFetch covers the web_fetch branch of the native tool_calls
// path: a 404 page produces "Status: 404" in the fetch result text and the
// result is rendered downstream as a [Web Fetch Result] text block.
func TestAgenticLoopWebFetch(t *testing.T) {
	var calls atomic.Int32
	var up *httptest.Server
	up = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		switch r.URL.Path {
		case "/page":
			// 被 fetch 的页面返回 404（无 html 头）→ 状态块 "Status: 404"
			http.NotFound(w, r)
			return
		case "/search":
			io.WriteString(w, `{"results":[]}`)
			return
		}
		n := calls.Add(1)
		if n == 1 {
			args := `{"url":` + strconv.Quote(up.URL+"/page") + `}`
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"f\",\"function\":{\"name\":\"web_fetch\",\"arguments\":"+strconv.Quote(args)+"}}]}}]}\n\n"+
				"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}}]}\n\n"+
				"data: [DONE]\n\n")
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"fetch answer\"}}]}\n\n"+
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n"+
			"data: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	cfg.ServerTools = config.ServerToolConfig{WebFetch: true, WebFetchMaxContentTokens: 5000}
	h := NewHandler(cfg)
	body := `{"model":"m","messages":[{"role":"user","content":"x"}],"server_tools":[{"type":"web_fetch_20250124","name":"web_fetch"}]}`
	resp := postMessages(t, h, body, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	joined := string(raw)
	if !strings.Contains(joined, "[Web Fetch Result]") {
		t.Errorf("fetch result text missing: %s", joined)
	}
	if !strings.Contains(joined, "Status: 404") {
		t.Errorf("fetch 404 status missing: %s", joined)
	}
	if !strings.Contains(joined, "fetch answer") {
		t.Errorf("final text missing: %s", joined)
	}
}

// TestAgenticLoopFailsOver covers the candidate chain in the agentic loop:
// pool [bad(→500), good]. The good upstream answers with a plain text SSE
// stream (no tool call → one agentic round), the client still gets 200 +
// message_stop, and the dump attempt trail records the skipped bad upstream.
// Each of the two proxy POSTs (loop round + final streaming request) rides the
// chain, so bad and good each see 2 requests.
func TestAgenticLoopFailsOver(t *testing.T) {
	badCalls, goodCalls := atomic.Int32{}, atomic.Int32{}
	textSSE := "data: {\"choices\":[{\"delta\":{\"content\":\"failed-over answer\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n" +
		"data: [DONE]\n\n"
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badCalls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `{"error":{"message":"boom"}}`)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		n := goodCalls.Add(1)
		if n > 2 {
			t.Errorf("good upstream hit %d times, want <= 2 (loop round + final)", n)
		}
		io.WriteString(w, textSSE)
	}))
	defer good.Close()

	cfg := &config.Config{
		Upstreams: []*config.Upstream{
			{Name: "bad", BaseURL: bad.URL},
			{Name: "good", BaseURL: good.URL},
		},
		Routes:      []config.Route{{Pattern: "*", Names: []string{"bad", "good"}}},
		DumpDir:     t.TempDir(),
		ServerTools: config.ServerToolConfig{WebSearch: true},
	}
	h := NewHandler(cfg)
	body := `{"model":"m","messages":[{"role":"user","content":"x"}],"server_tools":[{"type":"web_search_20250305","name":"web_search"}]}`
	resp := postMessages(t, h, body, map[string]string{"x-api-key": "client-key"})
	if resp.StatusCode != 200 {
		t.Fatalf("200 expected after failover, got %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	events := eventTypes(parseSSE(t, string(raw)))
	if len(events) == 0 || events[len(events)-1] != "message_stop" {
		t.Fatalf("last event = %v\nbody = %s", events, raw)
	}
	if !strings.Contains(string(raw), "failed-over answer") {
		t.Errorf("final text missing: %s", raw)
	}
	if badCalls.Load() != 2 || goodCalls.Load() != 2 {
		t.Errorf("calls = bad:%d good:%d, want 2/2 (loop round + final, each riding the chain)", badCalls.Load(), goodCalls.Load())
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
}

// ---------------------------------------------------------------------------
// Unit vectors ported from tests/routes_server_tools.test.ts
// ---------------------------------------------------------------------------

func strPtr(s string) *string { return &s }

// chunksOf adapts a slice of chunks into an iter.Seq (the Go analog of the
// TS chunksToStream async generator in routes_server_tools.test.ts).
func chunksOf(chunks ...openai.Chunk) iter.Seq[openai.Chunk] {
	return func(yield func(openai.Chunk) bool) {
		for _, c := range chunks {
			if !yield(c) {
				return
			}
		}
	}
}

func TestIsServerToolCall(t *testing.T) {
	toolCfg := config.ServerToolConfig{
		WebSearch:                true,
		WebFetch:                 true,
		WebSearchEngine:          "brave",
		WebSearchBaseURL:         "https://api.search.brave.com",
		WebFetchMaxContentTokens: 5000,
	}
	if !isServerToolCall("web_search", toolCfg) {
		t.Error("web_search must be a server tool when enabled")
	}
	if !isServerToolCall("web_fetch", toolCfg) {
		t.Error("web_fetch must be a server tool when enabled")
	}
	disabled := toolCfg
	disabled.WebSearch = false
	disabled.WebFetch = false
	if isServerToolCall("web_search", disabled) {
		t.Error("web_search must not match when disabled")
	}
	if isServerToolCall("web_fetch", disabled) {
		t.Error("web_fetch must not match when disabled")
	}
	if isServerToolCall("bash", toolCfg) {
		t.Error("bash must not match")
	}
	if isServerToolCall("read_file", toolCfg) {
		t.Error("read_file must not match")
	}
}

func TestCollectToolCallArguments(t *testing.T) {
	// collects tool call info from upstream chunks
	result := collectToolCallArguments(chunksOf(
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{{Index: 0, ID: strPtr("call_1"), Function: openai.ToolCallFunction{Name: strPtr("web_search")}}}}}}},
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{{Index: 0, Function: openai.ToolCallFunction{Arguments: strPtr(`{"query":"test"}`)}}}}}}},
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: strPtr("tool_calls")}}},
	))
	if len(result.toolCalls) != 1 {
		t.Fatalf("toolCalls = %d", len(result.toolCalls))
	}
	if result.toolCalls[0].name != "web_search" {
		t.Errorf("name = %q", result.toolCalls[0].name)
	}
	if result.toolCalls[0].id != "call_1" {
		t.Errorf("id = %q", result.toolCalls[0].id)
	}
	if result.toolCalls[0].arguments != `{"query":"test"}` {
		t.Errorf("arguments = %q", result.toolCalls[0].arguments)
	}
	if result.finishReason != "tool_calls" {
		t.Errorf("finishReason = %q", result.finishReason)
	}
	if !result.hasServerToolCall {
		t.Error("hasServerToolCall = false")
	}

	// returns empty when no tool calls
	empty := collectToolCallArguments(chunksOf(
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr("Hello")}}}},
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: strPtr("stop")}}},
	))
	if len(empty.toolCalls) != 0 {
		t.Errorf("toolCalls = %d", len(empty.toolCalls))
	}
	if empty.hasServerToolCall {
		t.Error("hasServerToolCall = true")
	}
	if empty.textContent != "Hello" {
		t.Errorf("textContent = %q", empty.textContent)
	}

	// correctly buffers streaming tool call arguments
	buffered := collectToolCallArguments(chunksOf(
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{{Index: 0, ID: strPtr("tc_1"), Function: openai.ToolCallFunction{Name: strPtr("web_search")}}}}}}},
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{{Index: 0, Function: openai.ToolCallFunction{Arguments: strPtr(`{"qu`)}}}}}}},
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{{Index: 0, Function: openai.ToolCallFunction{Arguments: strPtr(`ery":"test"}`)}}}}}}},
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: strPtr("tool_calls")}}},
	))
	if len(buffered.toolCalls) != 1 {
		t.Fatalf("toolCalls = %d", len(buffered.toolCalls))
	}
	if buffered.toolCalls[0].name != "web_search" {
		t.Errorf("name = %q", buffered.toolCalls[0].name)
	}
	if buffered.toolCalls[0].arguments != `{"query":"test"}` {
		t.Errorf("arguments = %q", buffered.toolCalls[0].arguments)
	}
	if !buffered.hasServerToolCall {
		t.Error("hasServerToolCall = false")
	}
	if buffered.finishReason != "tool_calls" {
		t.Errorf("finishReason = %q", buffered.finishReason)
	}

	// handles multiple tool calls (insertion order preserved)
	multi := collectToolCallArguments(chunksOf(
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, ID: strPtr("tc_1"), Function: openai.ToolCallFunction{Name: strPtr("web_search"), Arguments: strPtr(`{"query":"a"}`)}},
			{Index: 1, ID: strPtr("tc_2"), Function: openai.ToolCallFunction{Name: strPtr("web_fetch"), Arguments: strPtr(`{"url":"http`)}},
		}}}}},
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{{Index: 1, Function: openai.ToolCallFunction{Arguments: strPtr(`s://x.com"}`)}}}}}}},
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: strPtr("tool_calls")}}},
	))
	if len(multi.toolCalls) != 2 {
		t.Fatalf("toolCalls = %d", len(multi.toolCalls))
	}
	if multi.toolCalls[0].name != "web_search" {
		t.Errorf("toolCalls[0].name = %q", multi.toolCalls[0].name)
	}
	if multi.toolCalls[1].name != "web_fetch" {
		t.Errorf("toolCalls[1].name = %q", multi.toolCalls[1].name)
	}
	if multi.toolCalls[1].arguments != `{"url":"https://x.com"}` {
		t.Errorf("toolCalls[1].arguments = %q", multi.toolCalls[1].arguments)
	}

	// returns textContent when no tool calls
	text := collectToolCallArguments(chunksOf(
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr("Here is ")}}}},
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr("the answer.")}}}},
		openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: strPtr("stop")}}},
	))
	if len(text.toolCalls) != 0 {
		t.Errorf("toolCalls = %d", len(text.toolCalls))
	}
	if text.hasServerToolCall {
		t.Error("hasServerToolCall = true")
	}
	if text.textContent != "Here is the answer." {
		t.Errorf("textContent = %q", text.textContent)
	}
	if text.finishReason != "stop" {
		t.Errorf("finishReason = %q", text.finishReason)
	}
}

func TestExecuteServerToolCall(t *testing.T) {
	toolCfg := config.ServerToolConfig{
		WebSearch:                true,
		WebFetch:                 true,
		WebSearchEngine:          "brave",
		WebSearchAPIKey:          "",
		WebSearchBaseURL:         "https://api.search.brave.com",
		WebFetchMaxContentTokens: 5000,
	}
	// web_search without an API key → empty results, still a tool message.
	res := executeServerToolCall("web_search", `{"query":"test"}`, toolCfg, nil)
	if res.Role != "tool" {
		t.Errorf("role = %q", res.Role)
	}
	if res.ToolCallID == "" {
		t.Error("tool_call_id missing")
	}
	// web_fetch of an unresolvable host → formatted failure result.
	fetchRes := executeServerToolCall("web_fetch", `{"url":"https://example.invalid/test"}`, toolCfg, nil)
	if fetchRes.Role != "tool" {
		t.Errorf("role = %q", fetchRes.Role)
	}
	// unknown tool name → error object content.
	unknown := executeServerToolCall("unknown_tool", `{}`, toolCfg, nil)
	if unknown.Role != "tool" {
		t.Errorf("role = %q", unknown.Role)
	}
	if !strings.Contains(unknown.Content, "Unknown server tool") {
		t.Errorf("content = %q", unknown.Content)
	}
}

package proxy

// End-to-end coverage of POST /v1/responses against a mocked chat/completions
// upstream: streaming text+tool round, previous_response_id chain (second
// turn carries function_call_output), non-stream aggregation, and the 400/404
// contract.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/chinfeng/chat-to-messages/internal/config"
)

// sseUpstreamScript serves each queued SSE body once per request, recording
// every upstream request body.
type sseUpstreamScript struct {
	mu     sync.Mutex
	bodies []string
	script []string
}

func (u *sseUpstreamScript) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		raw, _ := json.Marshal(req)
		u.mu.Lock()
		u.bodies = append(u.bodies, string(raw))
		idx := len(u.bodies) - 1
		if idx >= len(u.script) {
			idx = len(u.script) - 1
		}
		body := u.script[idx]
		u.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	})
}

func (u *sseUpstreamScript) lastBody(t *testing.T) map[string]any {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.bodies) == 0 {
		t.Fatal("no upstream body recorded")
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(u.bodies[len(u.bodies)-1]), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

const toolTurnSSE = `data: {"choices":[{"delta":{"content":"checking "}}]}

data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"chatcmpl-upstream-id","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}}]}}]}

data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}

data: {"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":6}}

data: [DONE]

`

const textTurnSSE = `data: {"choices":[{"delta":{"content":"Sunny, 24°C"}}]}

data: {"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4}}

data: [DONE]

`

func TestResponsesEndToEndToolChain(t *testing.T) {
	upstream := &sseUpstreamScript{script: []string{toolTurnSSE, textTurnSSE}}
	up := httptest.NewServer(upstream.handler())
	defer up.Close()
	cfg := &config.Config{
		UpstreamBaseURL:          up.URL,
		UpstreamAPIKey:           "sk-upstream",
		ResponsesStoreTTLMinutes: 1440,
	}
	h := NewHandler(cfg)

	// --- Turn 1: streaming, model answers with a tool call.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/responses", strings.NewReader(
		`{"model":"test-model","input":"weather in Paris?","stream":true}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("turn 1 status %d: %s", rec.Code, rec.Body.String())
	}
	stream := rec.Body.String()
	for _, want := range []string{
		"event: response.created", "event: response.output_item.added",
		"event: response.function_call_arguments.done", "event: response.completed",
	} {
		if !strings.Contains(stream, want) {
			t.Fatalf("turn 1 stream missing %q\n%s", want, stream)
		}
	}
	if strings.Contains(stream, "chatcmpl-upstream-id") {
		t.Fatal("upstream tool id leaked downstream (duplicate-id doom loop)")
	}
	respID := regexp.MustCompile(`"id":"(resp_[0-9a-f-]+)"`).FindStringSubmatch(stream)
	callID := regexp.MustCompile(`"call_id":"(call_[0-9a-f-]+)"`).FindStringSubmatch(stream)
	if respID == nil || callID == nil {
		t.Fatalf("ids missing from stream:\n%s", stream)
	}

	// --- Turn 2: client returns the tool result resolving by the minted ids,
	// non-stream response.
	turn2 := `{"model":"test-model","previous_response_id":"` + respID[1] + `","input":[{"type":"function_call_output","call_id":"` + callID[1] + `","output":"sunny"}]}`
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("POST", "/v1/responses", strings.NewReader(turn2)))
	if rec2.Code != http.StatusOK {
		t.Fatalf("turn 2 status %d: %s", rec2.Code, rec2.Body.String())
	}
	var final map[string]any
	if err := json.Unmarshal(rec2.Body.Bytes(), &final); err != nil {
		t.Fatalf("turn 2 must be JSON: %v", err)
	}
	if final["status"] != "completed" {
		t.Fatalf("turn 2 status field: %v", final)
	}

	// The upstream must have seen the joined chain: assistant tool_call id ==
	// OUR minted call_id, and the tool result message keyed by it.
	body := upstream.lastBody(t)
	msgs, _ := body["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("upstream turn 2 messages: %v", body["messages"])
	}
	assistant, _ := msgs[1].(map[string]any)
	toolCalls, _ := assistant["tool_calls"].([]any)
	if len(toolCalls) != 1 || toolCalls[0].(map[string]any)["id"] != callID[1] {
		t.Fatalf("assistant tool_call id must be the minted %s: %v", callID[1], toolCalls)
	}
	toolMsg, _ := msgs[2].(map[string]any)
	if toolMsg["role"] != "tool" || toolMsg["tool_call_id"] != callID[1] || toolMsg["content"] != "sunny" {
		t.Fatalf("tool result message broken: %v", toolMsg)
	}
}

func TestResponsesRejections(t *testing.T) {
	upstream := &sseUpstreamScript{script: []string{textTurnSSE}}
	up := httptest.NewServer(upstream.handler())
	defer up.Close()
	cfg := &config.Config{UpstreamBaseURL: up.URL, UpstreamAPIKey: "sk", ResponsesStoreTTLMinutes: 1440}
	h := NewHandler(cfg)

	post := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/responses", strings.NewReader(body)))
		return rec
	}

	if rec := post(`{"model":"m","input":"x","background":true}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("background must be 400, got %d", rec.Code)
	}
	if rec := post(`{"model":"m","input":"x","tools":[{"type":"web_search"}]}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("hosted tool must be 400, got %d", rec.Code)
	}
	rec := post(`{"model":"m","previous_response_id":"resp_nope","input":"x"}`)
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "not_found_error") {
		t.Fatalf("unknown previous_response_id must be 404 not_found_error: %d %s", rec.Code, rec.Body.String())
	}
	if rec := post(`{"input":"x"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing model must be 400, got %d", rec.Code)
	}
}

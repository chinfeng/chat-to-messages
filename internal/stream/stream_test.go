package stream

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"strings"
	"testing"

	"github.com/chinfeng/chat-to-messages/internal/convert"
	"github.com/chinfeng/chat-to-messages/internal/openai"
)

// ---- helpers (task-9 brief) ----

// req builds a minimal request.
func req(t *testing.T, model string) *convert.RequestData {
	t.Helper()
	return &convert.RequestData{Model: model}
}

// chunksFromSSE parses an OpenAI SSE body into chunks.
func chunksFromSSE(t *testing.T, body string) []openai.Chunk {
	t.Helper()
	var out []openai.Chunk
	for c := range openai.IterSSEChunks(context.Background(), strings.NewReader(body), nil) {
		out = append(out, c)
	}
	return out
}

// collect iterates the streamer and returns its events and termination error.
func collect(t *testing.T, st *Streamer) ([]string, error) {
	t.Helper()
	var events []string
	for ev := range st.Events() {
		events = append(events, ev)
	}
	return events, st.Err()
}

// allEvents runs a full stream over an SSE body and returns the events.
func allEvents(t *testing.T, body string, request *convert.RequestData) []string {
	t.Helper()
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), request, 10, true, nil, nil)
	events, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

// seqFromSlice wraps a slice into an iter.Seq[openai.Chunk].
func seqFromSlice(chunks []openai.Chunk) iter.Seq[openai.Chunk] {
	return func(yield func(openai.Chunk) bool) {
		for _, c := range chunks {
			if !yield(c) {
				return
			}
		}
	}
}

// eventTypes extracts the SSE event type names from a list of events.
func eventTypes(events []string) []string {
	var out []string
	for _, e := range events {
		for _, line := range strings.Split(e, "\n") {
			if strings.HasPrefix(line, "event: ") {
				out = append(out, strings.TrimPrefix(line, "event: "))
			}
		}
	}
	return out
}

// ---- task-9 brief golden vectors ----
//
// Note on brief deviations (all preserving the brief's intent and assertions):
//  1. The brief's finish_reason chunk bodies (`{"choices":[{"delta":{},"finish_reason":"stop"}}]}`)
//     carry an extra '}' — invalid JSON that IterSSEChunks silently drops, so
//     finish_reason never reaches the stream and tests fail spuriously. The
//     bodies here use the evident intended form `{"choices":[{"delta":{},"finish_reason":"stop"}]}`.
//  2. TestStreamNativeToolCall chunk 1 arguments were missing one escape: the
//     brief's assertion expects the first delta `{"path":"` (accumulating to
//     the valid `{"path":"/etc/hosts"}`), so the body sends `{"path":"`.
//  3. TestStreamEmptyOutputGetsSpace had the same extra-'}' typo (fixed below).

func TestStreamMessageLifecycle(t *testing.T) {
	ev := allEvents(t, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n", req(t, "m1"))
	types := eventTypes(ev)
	want := []string{"message_start", "content_block_start", "content_block_delta", "content_block_stop", "message_delta", "message_stop"}
	if strings.Join(types, ",") != strings.Join(want, ",") {
		t.Fatalf("types = %v", types)
	}
	if !strings.Contains(ev[len(ev)-2], `"stop_reason":"end_turn"`) {
		t.Errorf("delta = %s", ev[len(ev)-2])
	}
}

func TestStreamReasoningContent(t *testing.T) {
	// 用户裁决 2026-08-03（第三次确认跟随 TS）：thinking→text 切换经
	// ensure_text_block 直接 stop_thinking_block，不发 signature_delta；
	// 签名只在内容块收尾、thinking 仍打开时出现。brief 原断言（切换前
	// 必有签名）与 TS 不符，按裁决改为负向断言。
	body := "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think step 1\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	types := eventTypes(ev)
	want := []string{"message_start", "content_block_start", "content_block_delta", "content_block_stop", "content_block_start", "content_block_delta", "content_block_stop", "message_delta", "message_stop"}
	if strings.Join(types, ",") != strings.Join(want, ",") {
		t.Fatalf("types = %v\n%v", types, ev)
	}
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"delta":{"type":"thinking_delta","thinking":"think step 1"}`) {
		t.Errorf("thinking delta missing: %s", joined)
	}
	// 切换不发签名（TS 语义，裁决 2026-08-03）
	if strings.Contains(joined, "signature_delta") {
		t.Errorf("no signature on thinking→text switch: %s", joined)
	}
	// text 块
	if !strings.Contains(joined, `"delta":{"type":"text_delta","text":"answer"}`) {
		t.Errorf("text delta missing: %s", joined)
	}
}

// TestStreamReasoningAliasGLM: GLM-family upstreams stream the thinking field
// under the bare `reasoning` key (not `reasoning_content`). Regression for the
// silent-drop bug where downstream-response.log lost all thinking content.
func TestStreamReasoningAliasGLM(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"reasoning\":\"think step 1\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"delta":{"type":"thinking_delta","thinking":"think step 1"}`) {
		t.Errorf("GLM reasoning alias must produce a thinking delta: %s", joined)
	}
	if !strings.Contains(joined, `"delta":{"type":"text_delta","text":"answer"}`) {
		t.Errorf("text delta missing: %s", joined)
	}
}

func TestStreamSignatureAtEnd(t *testing.T) {
	// 纯 thinking 响应（裁决 2026-08-03 跟随 TS）。
	// 裁决指令初版预期"收尾 close_all 时 thinking 仍开 → 含 signature_delta"，
	// 但实测 TS 参考实现（bun 运行 stream.ts，纯 reasoning + finish stop）
	// 输出中签名数为 0：收尾前的 " " 占位文本块分支经 ensure_text_block
	// 关闭 thinking 块（无签名）。按裁决总原则"断言同步按 TS 行为修正并注释
	// 裁决"，此处修正为负向断言，并断言 TS 的 " " 占位块输出。
	body := "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think step 1\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if strings.Contains(joined, "signature_delta") {
		t.Errorf("pure-thinking finalize emits no signature (TS): %s", joined)
	}
	if !strings.Contains(joined, `"thinking":"think step 1"`) {
		t.Errorf("thinking missing: %s", joined)
	}
	if !strings.Contains(joined, `"text":" "`) {
		t.Errorf("space placeholder text block missing (TS): %s", joined)
	}
}

func TestStreamThinkTagsInContent(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"<think>hidden</think>visible\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"thinking":"hidden"`) {
		t.Errorf("thinking missing: %s", joined)
	}
	if !strings.Contains(joined, `"text":"visible"`) {
		t.Errorf("text missing: %s", joined)
	}
	// 用户消息里没有 <think> 文本
	if strings.Contains(joined, "<think>") {
		t.Errorf("raw tag leaked: %s", joined)
	}
}

func TestStreamThinkTagsDisabled(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"<think>hidden</think>visible\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, false, nil, nil)
	ev, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ev, "\n")
	if strings.Contains(joined, "thinking") {
		t.Errorf("thinking must be dropped: %s", joined)
	}
	if !strings.Contains(joined, `"text":"visible"`) {
		t.Errorf("text missing: %s", joined)
	}
}

// toolUseID extracts the id emitted by the single tool_use content_block_start
// event, failing the test when absent or not exactly one.
func toolUseID(t *testing.T, events []string) string {
	t.Helper()
	ids := []string{}
	for _, e := range events {
		for _, line := range strings.Split(e, "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var ev struct {
				Type         string `json:"type"`
				ContentBlock struct {
					Type string `json:"type"`
					ID   string `json:"id"`
				} `json:"content_block"`
			}
			if err := json.Unmarshal([]byte(line[6:]), &ev); err != nil {
				continue
			}
			if ev.Type == "content_block_start" && ev.ContentBlock.Type == "tool_use" {
				ids = append(ids, ev.ContentBlock.ID)
			}
		}
	}
	if len(ids) != 1 {
		t.Fatalf("want exactly 1 tool_use block, got %d (%v)", len(ids), ids)
	}
	return ids[0]
}

func TestStreamNativeToolCall(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"read_file\",\"arguments\":\"{\\\"path\\\":\\\"\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"/etc/hosts\\\"}\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	// The upstream id must NOT pass through: relays mint ids whose uniqueness
	// resets per response, and Claude Code drops duplicate-id tool_use blocks
	// across turns (dumped 2026-08-28, kimi-k3 Bash:0 doom loop).
	if strings.Contains(joined, `"id":"call_1"`) {
		t.Errorf("upstream id leaked downstream: %s", joined)
	}
	id := toolUseID(t, ev)
	if !strings.HasPrefix(id, "toolu_") {
		t.Errorf("tool id = %q, want toolu_ prefix", id)
	}
	if !strings.Contains(joined, `"partial_json":"{\"path\":\"`) {
		t.Errorf("first delta missing: %s", joined)
	}
	if !strings.Contains(joined, `"partial_json":"/etc/hosts\"}`) {
		t.Errorf("second delta missing: %s", joined)
	}
	if !strings.Contains(joined, `"stop_reason":"tool_use"`) {
		t.Errorf("stop reason: %s", joined)
	}
}

// TestStreamToolIDUniqueAcrossResponses pins the duplicate-id defense: two
// responses that both reuse the same upstream id ("Bash:0", the relay's
// Name:index scheme) must surface distinct downstream ids, or Claude Code
// drops the second tool_use and the turn degenerates into "(no content)"
// recovery loop.
func TestStreamToolIDUniqueAcrossResponses(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"Bash:0\",\"type\":\"function\",\"function\":{\"name\":\"Bash\",\"arguments\":\"{\\\"command\\\":\\\"ls\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	var ids []string
	for i := 0; i < 2; i++ {
		ev := allEvents(t, body, req(t, "m1"))
		id := toolUseID(t, ev)
		if strings.Contains(id, "Bash:0") {
			t.Errorf("upstream id leaked downstream: %q", id)
		}
		ids = append(ids, id)
	}
	if ids[0] == ids[1] {
		t.Errorf("ids across responses collide: %v", ids)
	}
}

func TestStreamRefusalAsText(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"refusal\":\"I cannot\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"content_filter\"}]}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"text":"I cannot"`) {
		t.Errorf("refusal text missing: %s", joined)
	}
	if !strings.Contains(joined, `"stop_reason":"refusal"`) {
		t.Errorf("stop reason: %s", joined)
	}
}

func TestStreamUsageBuckets(t *testing.T) {
	// TS spread 语义（裁决 2026-08-03 跟随 TS）：`{...first, ...extract(latest)}`
	// 中 extract 恒返回全字段，后到的裸 usage chunk 以 0 覆盖先前的缓存桶 →
	// message_delta 的 input_tokens = 100 - 0 - 0 = 100，且不含 cache 字段
	// （0 值经 omitempty 消失）。brief 原断言（缓存保留）与 TS 不符，按裁决
	// 改为负向断言。
	body := "data: {\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":20,\"cache_read_input_tokens\":30,\"cache_creation_input_tokens\":10}}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: {\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":20}}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"input_tokens":100`) {
		t.Errorf("input = prompt - cache (cache wiped by later bare chunk): %s", joined)
	}
	if strings.Contains(joined, "cache_read_input_tokens") || strings.Contains(joined, "cache_creation_input_tokens") {
		t.Errorf("cache buckets must be absent after bare usage chunk: %s", joined)
	}
	// message_start 里 output_tokens=1，message_delta 里真实值
	if !strings.Contains(joined, `"output_tokens":20`) {
		t.Errorf("output: %s", joined)
	}
}

func TestStreamUsageFallbackDetails(t *testing.T) {
	body := "data: {\"usage\":{\"prompt_tokens\":100,\"prompt_tokens_details\":{\"cached_tokens\":40,\"cache_write_tokens\":5}}}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"input_tokens":55`) {
		t.Errorf("prompt - cached - write = 55: %s", joined)
	}
	if !strings.Contains(joined, `"cache_read_input_tokens":40`) {
		t.Errorf("cache_read fallback: %s", joined)
	}
	if !strings.Contains(joined, `"cache_creation_input_tokens":5`) {
		t.Errorf("cache_creation fallback: %s", joined)
	}
}

func TestStreamUpstreamErrorObject(t *testing.T) {
	// Mid-stream error object after content: hadContent → Err() surfaces
	// UpstreamStreamError so the route layer emits `event: error` (stream_error)
	// per the Anthropic Messages streaming protocol. No message_delta/message_stop
	// — the error event terminates the stream.
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n" +
		"data: {\"error\":{\"message\":\"rate limited\",\"code\":429}}\n\n"
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err == nil {
		t.Fatal("hadContent → Err() must surface UpstreamStreamError")
	}
	var usErr *UpstreamStreamError
	if !errors.As(err, &usErr) || usErr.Code != 429 {
		t.Fatalf("err = %v", err)
	}
	// Events must NOT include message_delta or message_stop.
	joined := strings.Join(ev, "\n")
	if strings.Contains(joined, "message_delta") || strings.Contains(joined, "message_stop") {
		t.Errorf("no lifecycle events after error: %s", joined)
	}
}

func TestStreamUpstreamErrorNoContent(t *testing.T) {
	// error 对象作为第一个 chunk、无任何内容 → 无内容路径：Err() 携带
	// UpstreamStreamError，事件仅 message_start，无 notice（TS 语义，裁决
	// 2026-08-03；已用 bun 运行 TS 参考实现验证：rethrow "rate limited"）。
	body := "data: {\"error\":{\"message\":\"rate limited\",\"code\":429}}\n\n"
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err == nil {
		t.Fatal("expected UpstreamStreamError")
	}
	var usErr *UpstreamStreamError
	if !errors.As(err, &usErr) || usErr.Code != 429 {
		t.Fatalf("err = %v", err)
	}
	if len(ev) != 1 {
		t.Errorf("only message_start expected, got %d events", len(ev))
	}
	if strings.Contains(strings.Join(ev, "\n"), "API Error:") {
		t.Errorf("no notice when no content: %v", ev)
	}
}

// TestStreamUpstreamErrorStringCode: a non-numeric string code ("E429",
// OpenRouter/newapi/GLM style) must surface as an UpstreamStreamError with the
// TS fallback code 500 (`typeof err.code === "number" ? err.code : 500`).
func TestStreamUpstreamErrorStringCode(t *testing.T) {
	body := "data: {\"error\":{\"message\":\"rate limited\",\"code\":\"E429\"}}\n\n"
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err == nil {
		t.Fatal("expected UpstreamStreamError")
	}
	var usErr *UpstreamStreamError
	if !errors.As(err, &usErr) {
		t.Fatalf("err = %T, want *UpstreamStreamError", err)
	}
	if !strings.Contains(usErr.Message, "Server error mid-response") || usErr.Code != 500 {
		t.Errorf("stream error = %+v, want code 500 (string-code fallback)", usErr)
	}
	if len(ev) != 1 {
		t.Errorf("only message_start expected, got %d events", len(ev))
	}
}

// TestStreamUpstreamErrorNumericStringCode: a numeric STRING code ("429") is
// converted by openai.Error.UnmarshalJSON and surfaces as-is.
func TestStreamUpstreamErrorNumericStringCode(t *testing.T) {
	body := "data: {\"error\":{\"message\":\"rate limited\",\"code\":\"429\"}}\n\n"
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, nil)
	_, err := collect(t, st)
	if err == nil {
		t.Fatal("expected UpstreamStreamError")
	}
	var usErr *UpstreamStreamError
	if !errors.As(err, &usErr) || usErr.Code != 429 {
		t.Fatalf("err = %v, want UpstreamStreamError with code 429", err)
	}
}

func TestStreamAbortBeforeContent(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{}}]}\n\n" // 无 finish_reason、无 [DONE]
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err == nil {
		t.Fatal("expected UpstreamAbortedError")
	}
	var ab *UpstreamAbortedError
	if !errors.As(err, &ab) || ab.Subtype != "response_stalled" {
		t.Fatalf("err = %v", err)
	}
	if len(ev) != 1 {
		t.Errorf("only message_start expected, got %d events", len(ev))
	}
}

func TestStreamConnectionClosedMidStream(t *testing.T) {
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr("partial")}}}},
		{Err: errors.New("connection reset by peer")},
	}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err == nil {
		t.Fatal("hadContent → Err() must surface UpstreamAbortedError")
	}
	var ab *UpstreamAbortedError
	if !errors.As(err, &ab) || ab.Subtype != "connection_closed" {
		t.Fatalf("err = %v", err)
	}
	// Events must NOT include message_delta or message_stop.
	joined := strings.Join(ev, "\n")
	if strings.Contains(joined, "message_delta") || strings.Contains(joined, "message_stop") {
		t.Errorf("no lifecycle events after error: %s", joined)
	}
}

func TestStreamDoneWithoutFinishReason(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\n" +
		"data: [DONE]\n\n" // 无 finish_reason 但 [DONE]
	ev := allEvents(t, body, req(t, "m1"))
	if !strings.Contains(strings.Join(ev, "\n"), `"stop_reason":"end_turn"`) {
		t.Errorf("should be graceful stop: %v", ev)
	}
}

func TestStreamEmptyOutputGetsSpace(t *testing.T) {
	// note: brief's inline body had an extra '}' ("stop\"}}]}") which is invalid
	// JSON and silently drops the chunk; the corrected body is used here.
	body := "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"text":" "`) {
		t.Errorf("space text block missing: %s", joined)
	}
}

func TestStreamOrphanToolNameInference(t *testing.T) {
	request := req(t, "m1")
	request.Tools = []map[string]any{{"type": "custom", "name": "read_file", "input_schema": map[string]any{}}}
	body := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{}\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, request)
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"name":"read_file"`) {
		t.Errorf("inferred name missing: %s", joined)
	}
}

func TestStreamStopSequenceDetection(t *testing.T) {
	request := req(t, "m1")
	request.StopSequences = []string{"</output>"}
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"done</output>\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, request)
	if !strings.Contains(strings.Join(ev, "\n"), `"stop_sequence":"</output>"`) {
		t.Errorf("stop_sequence missing: %v", ev)
	}
}

func TestStreamSkipMessageLifecycle(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil,
		&Options{SkipMessageLifecycle: true, StartingBlockIndex: 3})
	ev, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ev {
		if strings.HasPrefix(e, "event: message_") {
			t.Errorf("lifecycle events must be skipped: %s", e)
		}
	}
	if !strings.Contains(strings.Join(ev, "\n"), `"index":3`) {
		t.Errorf("starting index: %v", ev)
	}
}

func TestStreamHeuristicToolParsing(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"● <function=read_file><parameter=path>/x</parameter>\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"content_block":{"type":"tool_use"`) {
		t.Errorf("heuristic tool block missing: %s", joined)
	}
	if !strings.Contains(joined, `"name":"read_file"`) {
		t.Errorf("name missing: %s", joined)
	}
}

func TestExtractUsageInfo(t *testing.T) {
	u := ExtractUsageInfo(&openai.Usage{PromptTokens: 100, CompletionTokens: 20,
		CacheReadInputTokens: int64Ptr(30), CacheCreationInputTokens: int64Ptr(10)})
	if u.PromptTokens != 100 || u.CacheReadInputTokens != 30 {
		t.Errorf("u = %+v", u)
	}
	u2 := ExtractUsageInfo(&openai.Usage{PromptTokens: 100, PromptTokensDetails: &openai.PromptTokensDetails{CachedTokens: int64Ptr(5), CacheWriteTokens: int64Ptr(2)}})
	if u2.CacheReadInputTokens != 5 || u2.CacheCreationInputTokens != 2 {
		t.Errorf("u2 = %+v", u2)
	}
	u3 := ExtractUsageInfo(&openai.Usage{PromptTokens: 100, PromptTokensDetails: &openai.PromptTokensDetails{CachedTokens: int64Ptr(0)}})
	if u3.CacheReadInputTokens != 0 {
		t.Errorf("0 should stay 0: %+v", u3)
	}
	if ExtractUsageInfo(nil) != nil {
		t.Error("nil usage → nil")
	}
}

func TestInferToolNameByIndex(t *testing.T) {
	request := req(t, "m")
	request.Tools = []map[string]any{{"name": "a"}, {"name": "b"}}
	if InferToolNameByIndex(request, 0) != "a" {
		t.Error("index 0")
	}
	if InferToolNameByIndex(request, 1) != "b" {
		t.Error("index 1")
	}
	if InferToolNameByIndex(request, 5) != "" {
		t.Error("out of range")
	}
	if InferToolNameByIndex(req(t, "m"), 0) != "" {
		t.Error("no tools")
	}
}

// accumulatedPartialJSON reconstructs the client-visible concatenation of all
// input_json_delta partial_json payloads (what a downstream client accumulates
// as the tool input).
func accumulatedPartialJSON(t *testing.T, events []string) string {
	t.Helper()
	var acc strings.Builder
	for _, e := range events {
		for _, line := range strings.Split(e, "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var d map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &d); err != nil {
				continue
			}
			delta, _ := d["delta"].(map[string]any)
			if pj, ok := delta["partial_json"].(string); ok {
				acc.WriteString(pj)
			}
		}
	}
	return acc.String()
}

// assertTaskToolInputValid asserts that the accumulated partial_json is valid
// JSON whose run_in_background is false (the Task-buffer ruling's core intent).
func assertTaskToolInputValid(t *testing.T, events []string) map[string]any {
	t.Helper()
	acc := accumulatedPartialJSON(t, events)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(acc), &parsed); err != nil {
		t.Fatalf("accumulated partial_json must be valid JSON: %q: %v", acc, err)
	}
	if parsed["run_in_background"] != false {
		t.Errorf("run_in_background = %v, want false (accumulated: %q)", parsed["run_in_background"], acc)
	}
	return parsed
}

func TestStreamTaskRunInBackgroundForced(t *testing.T) {
	// 多分片 Task 原生工具调用（评审修复 Important + 裁决意图）：参数跨 3 个
	// chunk 到达。缓冲未完成时原始分片被暂扣（不发，避免泄漏 run_in_background:true
	// 与重复/损坏拼接）；凑齐后只发一次 canonical 完整 JSON。
	body := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c1\",\"function\":{\"name\":\"Task\",\"arguments\":\"{\\\"run_in_background\\\":true,\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"description\\\":\\\"\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"d\\\"}\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if strings.Contains(joined, `"run_in_background":true`) {
		t.Errorf("run_in_background must never appear as true: %s", joined)
	}
	parsed := assertTaskToolInputValid(t, ev)
	if parsed["description"] != "d" {
		t.Errorf("description = %v, want d", parsed["description"])
	}
}

func TestStreamTaskPreStartArgsBuffered(t *testing.T) {
	// R1（评审）：Task 参数先于 name 到达——第一 chunk 只有 arguments（无
	// name/id），第二 chunk 才补 name "Task"。工具 start 时补发的 pre-start
	// 参数必须走 Task 缓冲路径（暂扣，凑齐后 canonical 一次发，run_in_background
	// 强制 false），不得原始照发。
	body := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"run_in_background\\\":true,\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c1\",\"function\":{\"name\":\"Task\",\"arguments\":\"\\\"description\\\":\\\"d\\\"}\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if strings.Contains(joined, `"run_in_background":true`) {
		t.Errorf("run_in_background must never appear as true: %s", joined)
	}
	parsed := assertTaskToolInputValid(t, ev)
	if parsed["description"] != "d" {
		t.Errorf("description = %v, want d", parsed["description"])
	}
}

func TestStreamOrphanTaskArgsBuffered(t *testing.T) {
	// R2（评审）：name 完全缺失的孤儿工具经 InferToolNameByIndex 推断为
	// "Task"（request.Tools[0]）→ preStartArgs 必须走 Task 缓冲/规范化路径
	// （解析成功 → run_in_background=false 的 canonical JSON；失败 → 留给
	// FlushTaskArgBuffers 发 "{}" + sha256 告警），而非 repair 后原始照发。
	request := req(t, "m1")
	request.Tools = []map[string]any{{"type": "custom", "name": "Task", "input_schema": map[string]any{}}}
	body := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"run_in_background\\\":true,\\\"description\\\":\\\"d\\\"}\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, request)
	joined := strings.Join(ev, "\n")
	if strings.Contains(joined, `"run_in_background":true`) {
		t.Errorf("run_in_background must never appear as true: %s", joined)
	}
	parsed := assertTaskToolInputValid(t, ev)
	if parsed["description"] != "d" {
		t.Errorf("description = %v, want d", parsed["description"])
	}
}

// ---- ported unit vectors from ../chat-to-claude-code/tests/stream.test.ts ----

func TestStreamCompleteTextChunks(t *testing.T) {
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr("Hello")}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr(" world")}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: strPtr("stop")}}, Usage: &openai.Usage{PromptTokens: 10, CompletionTokens: 5}},
	}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "gpt-4o"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ev, "\n")
	for _, want := range []string{"event: message_start", "event: content_block_start", "event: content_block_stop", "event: message_delta", "event: message_stop", "end_turn"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestStreamToolCallsChunks(t *testing.T) {
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, ID: strPtr("call_001"), Function: openai.ToolCallFunction{Name: strPtr("read_file")}},
		}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, Function: openai.ToolCallFunction{Arguments: strPtr(`{"path":"/tmp"}`)}},
		}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: strPtr("tool_calls")}}},
	}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "gpt-4o"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"type":"tool_use"`) {
		t.Errorf("tool_use missing: %s", joined)
	}
	if !strings.Contains(joined, `"name":"read_file"`) {
		t.Errorf("read_file missing: %s", joined)
	}
	if !strings.Contains(joined, "/tmp") {
		t.Errorf("/tmp missing: %s", joined)
	}
	if !strings.Contains(joined, `"stop_reason":"tool_use"`) {
		t.Errorf("stop_reason: %s", joined)
	}
}

func TestStreamErrorClosesOpenBlocks(t *testing.T) {
	// reasoning_content eagerly opens a thinking block; an upstream read error
	// must leave a well-formed block prefix (starts balanced by stops) and
	// surface the error.
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{ReasoningContent: strPtr("thinking...")}}}},
		{Err: errors.New("upstream disconnected")},
	}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "gpt-4o"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err == nil {
		t.Fatal("hadContent → Err() must surface error")
	}
	joined := strings.Join(ev, "\n")
	starts := strings.Count(joined, "event: content_block_start")
	stops := strings.Count(joined, "event: content_block_stop")
	if starts == 0 {
		t.Error("no block started")
	}
	if stops < starts {
		t.Errorf("unbalanced blocks: starts=%d stops=%d", starts, stops)
	}
}

func TestStreamNoFabricationOnImmediateError(t *testing.T) {
	// Error before any content delta: no fabricated text/end_turn/message_stop.
	chunks := []openai.Chunk{{Err: errors.New("upstream disconnected")}}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "gpt-4o"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err == nil {
		t.Fatal("expected UpstreamAbortedError")
	}
	var ab *UpstreamAbortedError
	if !errors.As(err, &ab) || ab.Subtype != "connection_closed" {
		t.Fatalf("err = %v", err)
	}
	joined := strings.Join(ev, "\n")
	for _, bad := range []string{"text_delta", "end_turn", "event: message_stop"} {
		if strings.Contains(joined, bad) {
			t.Errorf("must not contain %q: %s", bad, joined)
		}
	}
}

func TestStreamTextBeforeToolUse(t *testing.T) {
	// Some upstreams send tool_calls:[] alongside text, then native tool_calls
	// later — the text must appear BEFORE the tool_use block.
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr("Hello"), ToolCalls: []openai.ToolCallDelta{}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr(" World"), ToolCalls: []openai.ToolCallDelta{}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, ID: strPtr("call_001"), Function: openai.ToolCallFunction{Name: strPtr("read_file"), Arguments: strPtr(`{"path":"/tmp"}`)}},
		}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: strPtr("tool_calls")}}},
	}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "gpt-4o"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ev, "\n")
	textPos := strings.Index(joined, `"type":"text"`)
	toolPos := strings.Index(joined, `"type":"tool_use"`)
	if textPos == -1 || toolPos == -1 || textPos > toolPos {
		t.Errorf("text must precede tool_use: text@%d tool@%d\n%s", textPos, toolPos, joined)
	}
	if !strings.Contains(joined, "Hello World") {
		t.Errorf("text missing: %s", joined)
	}
	if !strings.Contains(joined, "read_file") {
		t.Errorf("tool name missing: %s", joined)
	}
}

func TestStreamIgnoresEmptyToolCalls(t *testing.T) {
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr("Just text"), ToolCalls: []openai.ToolCallDelta{}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: strPtr("stop")}}},
	}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "gpt-4o"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, "Just text") {
		t.Errorf("text missing: %s", joined)
	}
	if strings.Contains(joined, `"type":"tool_use"`) {
		t.Errorf("tool_use must be absent: %s", joined)
	}
}

func TestStreamWebSearchPassthrough(t *testing.T) {
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, ID: strPtr("call_ws_001"), Function: openai.ToolCallFunction{Name: strPtr("WebSearch"), Arguments: strPtr("")}},
		}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, Function: openai.ToolCallFunction{Arguments: strPtr(`{"query":"test search"}`)}},
		}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: strPtr("tool_calls")}}},
	}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "gpt-4o"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"type":"tool_use"`) {
		t.Errorf("must be tool_use: %s", joined)
	}
	if strings.Contains(joined, "server_tool_use") {
		t.Errorf("must not be server_tool_use: %s", joined)
	}
	if !strings.Contains(joined, `"name":"WebSearch"`) {
		t.Errorf("original name missing: %s", joined)
	}
	if !strings.Contains(joined, "test search") {
		t.Errorf("query missing: %s", joined)
	}
}

func TestStreamWebFetchPassthrough(t *testing.T) {
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, ID: strPtr("call_wf_001"), Function: openai.ToolCallFunction{Name: strPtr("WebFetch"), Arguments: strPtr("")}},
		}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, Function: openai.ToolCallFunction{Arguments: strPtr(`{"url":"https://example.com","prompt":"summarize"}`)}},
		}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: strPtr("tool_calls")}}},
	}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "gpt-4o"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"type":"tool_use"`) {
		t.Errorf("must be tool_use: %s", joined)
	}
	if strings.Contains(joined, "server_tool_use") {
		t.Errorf("must not be server_tool_use: %s", joined)
	}
	if !strings.Contains(joined, `"name":"WebFetch"`) {
		t.Errorf("original name missing: %s", joined)
	}
	if !strings.Contains(joined, "example.com") {
		t.Errorf("url missing: %s", joined)
	}
}

func TestStreamGLMIncompleteToolCalls(t *testing.T) {
	// GLM-5.1: tool_calls with only index + arguments; name inferred from the
	// request tools list by index.
	request := req(t, "z-ai/glm-5.1")
	request.Tools = []map[string]any{
		{"type": "custom", "name": "Read", "description": "Read a file", "input_schema": map[string]any{"type": "object", "properties": map[string]any{}}},
	}
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, Function: openai.ToolCallFunction{Arguments: strPtr("{}")}},
		}}, FinishReason: strPtr("tool_calls")}}},
	}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), request, 10, true, nil, nil)
	ev, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"type":"tool_use"`) {
		t.Errorf("tool_use missing: %s", joined)
	}
	if !strings.Contains(joined, `"name":"Read"`) {
		t.Errorf("inferred name missing: %s", joined)
	}
	if !strings.Contains(joined, `"stop_reason":"tool_use"`) {
		t.Errorf("stop_reason: %s", joined)
	}
}

func TestStreamGLMNoToolsDegrades(t *testing.T) {
	// No tools in the request → orphaned tool state must degrade to end_turn,
	// never a fake stop_reason tool_use.
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, Function: openai.ToolCallFunction{Arguments: strPtr("{}")}},
		}}, FinishReason: strPtr("tool_calls")}}},
	}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "gpt-4o"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ev, "\n")
	if strings.Contains(joined, `"stop_reason":"tool_use"`) {
		t.Errorf("must not emit tool_use stop reason: %s", joined)
	}
	if !strings.Contains(joined, `"stop_reason":"end_turn"`) {
		t.Errorf("must degrade to end_turn: %s", joined)
	}
}

func TestStreamGLMTextThenIncompleteTool(t *testing.T) {
	request := req(t, "z-ai/glm-5.1")
	request.Tools = []map[string]any{
		{"type": "custom", "name": "chrome-devtools-mcp:chrome-devtools", "description": "Chrome DevTools", "input_schema": map[string]any{"type": "object", "properties": map[string]any{}}},
	}
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr("让我"), ToolCalls: []openai.ToolCallDelta{}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr("用 Chrome DevTools 截图查看当前页面状态"), ToolCalls: []openai.ToolCallDelta{}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr("："), ToolCalls: []openai.ToolCallDelta{}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, Function: openai.ToolCallFunction{Arguments: strPtr("{}")}},
		}}, FinishReason: strPtr("tool_calls")}}},
	}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), request, 10, true, nil, nil)
	ev, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ev, "\n")
	textPos := strings.Index(joined, `"type":"text"`)
	toolPos := strings.Index(joined, `"type":"tool_use"`)
	if textPos == -1 || toolPos == -1 || textPos > toolPos {
		t.Errorf("text must precede tool_use: text@%d tool@%d", textPos, toolPos)
	}
	// The GLM text spanning chunks 1-3 must be preserved in full (账本记录缺失).
	if !strings.Contains(joined, "用 Chrome DevTools 截图查看当前页面状态") {
		t.Errorf("GLM text content missing: %s", joined)
	}
	if !strings.Contains(joined, `"name":"chrome-devtools-mcp:chrome-devtools"`) {
		t.Errorf("inferred name missing: %s", joined)
	}
}

// ---- usage accounting ports (G1+G3+G4) ----

func parseMessageDeltaUsage(t *testing.T, events []string) map[string]any {
	t.Helper()
	for _, ev := range events {
		if !strings.HasPrefix(ev, "event: message_delta") {
			continue
		}
		var data map[string]any
		for _, line := range strings.Split(ev, "\n") {
			if strings.HasPrefix(line, "data: ") {
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &data); err != nil {
					t.Fatal(err)
				}
			}
		}
		usage, _ := data["usage"].(map[string]any)
		return usage
	}
	t.Fatal("no message_delta event")
	return nil
}

func parseMessageStartUsage(t *testing.T, events []string) map[string]any {
	t.Helper()
	for _, ev := range events {
		if !strings.HasPrefix(ev, "event: message_start") {
			continue
		}
		var data map[string]any
		for _, line := range strings.Split(ev, "\n") {
			if strings.HasPrefix(line, "data: ") {
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &data); err != nil {
					t.Fatal(err)
				}
			}
		}
		msg, _ := data["message"].(map[string]any)
		usage, _ := msg["usage"].(map[string]any)
		return usage
	}
	t.Fatal("no message_start event")
	return nil
}

func numVal(v any) float64 {
	f, _ := v.(float64)
	return f
}

func TestStreamUsageFromPromptDetails(t *testing.T) {
	// GLM-5.2 / OpenAI surface cache hits in prompt_tokens_details.
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":5,\"prompt_tokens_details\":{\"cached_tokens\":30,\"cache_write_tokens\":10}}}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	usage := parseMessageDeltaUsage(t, ev)
	if numVal(usage["input_tokens"]) != 60 {
		t.Errorf("input_tokens = %v", usage["input_tokens"])
	}
	if numVal(usage["cache_read_input_tokens"]) != 30 {
		t.Errorf("cache_read = %v", usage["cache_read_input_tokens"])
	}
	if numVal(usage["cache_creation_input_tokens"]) != 10 {
		t.Errorf("cache_creation = %v", usage["cache_creation_input_tokens"])
	}
	if numVal(usage["output_tokens"]) != 5 {
		t.Errorf("output = %v", usage["output_tokens"])
	}
}

func TestStreamUsageDirectCacheFieldsWin(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":5,\"cache_read_input_tokens\":40,\"cache_creation_input_tokens\":20,\"prompt_tokens_details\":{\"cached_tokens\":30,\"cache_write_tokens\":10}}}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	usage := parseMessageDeltaUsage(t, ev)
	if numVal(usage["input_tokens"]) != 40 {
		t.Errorf("input_tokens = %v", usage["input_tokens"])
	}
	if numVal(usage["cache_read_input_tokens"]) != 40 {
		t.Errorf("cache_read = %v", usage["cache_read_input_tokens"])
	}
	if numVal(usage["cache_creation_input_tokens"]) != 20 {
		t.Errorf("cache_creation = %v", usage["cache_creation_input_tokens"])
	}
}

func TestStreamUsageRealInputTokensInDelta(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1234,\"completion_tokens\":7}}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	delta := parseMessageDeltaUsage(t, ev)
	if numVal(delta["input_tokens"]) != 1234 {
		t.Errorf("delta input_tokens = %v", delta["input_tokens"])
	}
	if numVal(delta["output_tokens"]) != 7 {
		t.Errorf("delta output_tokens = %v", delta["output_tokens"])
	}
	// message_start precedes the usage chunk → estimate (constructor input 10).
	start := parseMessageStartUsage(t, ev)
	if numVal(start["input_tokens"]) != 10 {
		t.Errorf("start input_tokens = %v", start["input_tokens"])
	}
}

func TestStreamUsageSaturatesInputTokens(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":1,\"prompt_tokens_details\":{\"cached_tokens\":100}}}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	usage := parseMessageDeltaUsage(t, ev)
	if numVal(usage["input_tokens"]) != 0 {
		t.Errorf("input_tokens = %v", usage["input_tokens"])
	}
}

func TestStreamUsageEstimateFallback(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 42, true, nil, nil)
	ev, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	usage := parseMessageDeltaUsage(t, ev)
	if numVal(usage["input_tokens"]) != 42 {
		t.Errorf("input_tokens = %v", usage["input_tokens"])
	}
}

func strPtr(s string) *string { return &s }
func int64Ptr(i int64) *int64 { return &i }

// ---- empty-turn guard (Go-only extension, dumped 2026-08-27) ----

const guardInvisibleSSE = "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"I will read routes.ts now\"}}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
	"data: [DONE]\n\n"

const guardRecoverySSE = "data: {\"choices\":[{\"delta\":{\"content\":\"Reading the TTL constants.\"}}]}\n\n" +
	"data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"Read:0\",\"type\":\"function\",\"function\":{\"name\":\"Read\",\"arguments\":\"{\\\"file_path\\\":\\\"x\\\"}\"}}]}}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
	"data: [DONE]\n\n"

func TestStreamEmptyTurnGuardRetriesAndContinues(t *testing.T) {
	chunks := chunksFromSSE(t, guardInvisibleSSE)
	recovery := chunksFromSSE(t, guardRecoverySSE)
	calls := 0
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, &Options{
		InvisibleTurnRetry: func(attempt int, info InvisibleTurnInfo) iter.Seq[openai.Chunk] {
			calls++
			if attempt != 1 || calls != 1 {
				t.Fatalf("unexpected call attempt=%d calls=%d", attempt, calls)
			}
			if info.Reasoning != "I will read routes.ts now" {
				t.Fatalf("info.Reasoning = %q", info.Reasoning)
			}
			return seqFromSlice(recovery)
		},
	})
	events, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	all := strings.Join(events, "")
	if !strings.Contains(all, "Reading the TTL constants.") {
		t.Fatal("retry continuation text missing")
	}
	if strings.Contains(all, `"text":" "`) {
		t.Fatal("placeholder space emitted despite real retry content")
	}
	if strings.Count(strings.Join(eventTypes(events), ","), "message_delta") != 1 {
		t.Fatalf("lifecycle events duplicated: %v", eventTypes(events))
	}
	if !strings.Contains(events[len(events)-2], `"stop_reason":"tool_use"`) {
		t.Fatalf("final delta = %s", events[len(events)-2])
	}
}

func TestStreamEmptyTurnGuardExhaustedFallsBack(t *testing.T) {
	chunks := chunksFromSSE(t, guardInvisibleSSE)
	stillInvisible := chunksFromSSE(t, "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"still thinking\"}}]}\n\n"+
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	var calls int
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, &Options{
		InvisibleTurnRetry: func(attempt int, info InvisibleTurnInfo) iter.Seq[openai.Chunk] {
			calls++
			if attempt == 1 {
				return seqFromSlice(stillInvisible)
			}
			return nil
		},
	})
	events, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d", calls)
	}
	all := strings.Join(events, "")
	if !strings.Contains(all, "still thinking") {
		t.Fatal("first retry reasoning missing from stream")
	}
	// Baseline invisible finalize preserved: placeholder text + clean end_turn.
	if !strings.Contains(all, `"text_delta","text":" "`) {
		t.Fatal("baseline placeholder missing after exhausted retries")
	}
	if !strings.Contains(events[len(events)-2], `"stop_reason":"end_turn"`) {
		t.Fatalf("final delta = %s", events[len(events)-2])
	}
}

func TestStreamEmptyTurnGuardRetryErrorFallsBackCleanly(t *testing.T) {
	chunks := chunksFromSSE(t, guardInvisibleSSE)
	failing := []openai.Chunk{{Err: errors.New("boom")}}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, &Options{
		InvisibleTurnRetry: func(attempt int, info InvisibleTurnInfo) iter.Seq[openai.Chunk] {
			return seqFromSlice(failing)
		},
	})
	events, err := collect(t, st)
	if err != nil {
		t.Fatalf("retry failure must not surface: %v", err)
	}
	if !strings.Contains(strings.Join(events, ""), `"text_delta","text":" "`) {
		t.Fatal("fallback placeholder missing")
	}
}

func TestStreamEmptyTurnGuardNotArmedKeepsTSParity(t *testing.T) {
	chunks := chunksFromSSE(t, guardInvisibleSSE)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, nil)
	events, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(events, ""), `"text_delta","text":" "`) {
		t.Fatal("placeholder missing without hook")
	}
}

func TestStreamEmptyTurnGuardSkipsLengthFinish(t *testing.T) {
	// A token-limit cut ("length") is the client's recovery path (thinking
	// resumption / max-output-tokens recovery), never a collapse signature.
	body := "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"partial plan\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n"
	chunks := chunksFromSSE(t, body)
	calls := 0
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, &Options{
		InvisibleTurnRetry: func(attempt int, info InvisibleTurnInfo) iter.Seq[openai.Chunk] {
			calls++
			return nil
		},
	})
	events, err := collect(t, st)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("hook invoked %d times for length finish", calls)
	}
	if !strings.Contains(events[len(events)-2], `"stop_reason":"max_tokens"`) {
		t.Fatalf("expected max_tokens mapping, got %s", events[len(events)-2])
	}
}

package responses

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/chinfeng/chat-to-messages/internal/openai"
)

func sp(s string) *string { return &s }

// runTranslator feeds chunks through a fresh translator and returns the full
// event list (Process events followed by Finish events).
func runTranslator(t *testing.T, req *Request, chunks []openai.Chunk) []string {
	t.Helper()
	tr := NewTranslator(req, "resp_test")
	var events []string
	for _, c := range chunks {
		events = append(events, tr.Process(c)...)
	}
	events = append(events, tr.Finish()...)
	return events
}

// eventNames extracts the "event:" names in order.
func eventNames(events []string) []string {
	var names []string
	for _, ev := range events {
		for _, line := range strings.Split(ev, "\n") {
			if strings.HasPrefix(line, "event: ") {
				names = append(names, strings.TrimPrefix(line, "event: "))
			}
		}
	}
	return names
}

func contains(events []string, subs ...string) bool {
	joined := strings.Join(events, "")
	for _, s := range subs {
		if !strings.Contains(joined, s) {
			return false
		}
	}
	return true
}

func TextDelta(text string) openai.Chunk {
	return openai.Chunk{Choices: []openai.Choice{{Delta: &openai.Delta{Content: sp(text)}}}}
}

func TestTranslatorTextOnly(t *testing.T) {
	var req Request
	req.Model = "m"
	events := runTranslator(t, &req, []openai.Chunk{
		TextDelta("Hello"),
		TextDelta(" world"),
		{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: sp("stop")}}, Usage: &openai.Usage{PromptTokens: 7, CompletionTokens: 2}},
		{Done: true},
	})
	names := eventNames(events)
	// NOTE: the heuristic tool parser buffers plain text until flush (TS
	// parity), so the two content chunks collapse into ONE flushed delta —
	// only the lifecycle + full-text invariants are asserted.
	want := []string{
		"response.created", "response.in_progress",
		"response.output_item.added", "response.content_part.added",
		"response.output_text.delta",
		"response.output_text.done", "response.content_part.done",
		"response.output_item.done", "response.completed",
	}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("event sequence:\nwant %v\ngot  %v", want, names)
	}
	if !contains(events, `"text":"Hello world"`) {
		t.Fatalf("text.done must carry the full text: %v", events)
	}
	// Usage lands on the terminal response.
	if !contains(events, `"input_tokens":7`, `"output_tokens":2`) {
		t.Fatalf("usage missing from completed response: %v", events)
	}
	// ids carry their prefixes
	if !contains(events, `resp_test`, `msg_`) {
		t.Fatalf("expected resp_ id echo and msg_ item id: %v", events)
	}
}

func TestTranslatorReasoningThenText(t *testing.T) {
	var req Request
	req.Model = "m"
	// GLM-style think tags inside content + native reasoning_content.
	events := runTranslator(t, &req, []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{ReasoningContent: sp("native thought ")}}}},
		TextDelta("<think>tagged thought</think>answer"),
		{Done: true},
	})
	names := eventNames(events)
	// reasoning item first (rs_), then message item.
	idx := func(n string) int {
		for i, x := range names {
			if x == n {
				return i
			}
		}
		return -1
	}
	if idx("response.reasoning_summary_text.delta") == -1 {
		t.Fatal("reasoning delta expected")
	}
	if !contains(events, "native thought tagged thought") && !contains(events, "native thought ") && !contains(events, "tagged thought") {
		t.Fatalf("reasoning text lost: %v", events)
	}
	if !contains(events, `"rs_`) {
		t.Fatalf("reasoning item must use rs_ prefix: %v", events)
	}
	var completed string
	for _, ev := range events {
		if strings.Contains(ev, "event: response.completed") {
			completed = ev
		}
	}
	var parsed struct {
		Response struct {
			Output []map[string]any `json:"output"`
		} `json:"response"`
	}
	dataStart := strings.Index(completed, "data: ")
	if err := json.Unmarshal([]byte(completed[dataStart+6:]), &parsed); err != nil {
		t.Fatalf("completed payload parse: %v", err)
	}
	if len(parsed.Response.Output) != 2 || parsed.Response.Output[0]["type"] != "reasoning" || parsed.Response.Output[1]["type"] != "message" {
		t.Fatalf("output item order/types wrong: %v", parsed.Response.Output)
	}
}

func TestTranslatorEncryptedContent(t *testing.T) {
	var req Request
	req.Model = "m"
	req.Include = []string{"reasoning.encrypted_content"}
	events := runTranslator(t, &req, []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{ReasoningContent: sp("hmm")}}}},
		{Done: true},
	})
	if !contains(events, "encrypted_content") {
		t.Fatalf("encrypted_content requested but absent: %v", events)
	}
	// Without the include flag it must NOT appear.
	req2 := Request{Model: "m"}
	events2 := runTranslator(t, &req2, []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{ReasoningContent: sp("hmm")}}}},
		{Done: true},
	})
	if contains(events2, "encrypted_content") {
		t.Fatal("encrypted_content must not appear without the include flag")
	}
}

func TestTranslatorNativeToolCall(t *testing.T) {
	var req Request
	req.Model = "m"
	chunks := []openai.Chunk{
		TextDelta("checking "),
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, ID: sp("upstream-minted-id"), Function: openai.ToolCallFunction{Name: sp("get_weather")}},
		}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, Function: openai.ToolCallFunction{Arguments: sp(`{"city":"`)}},
		}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, Function: openai.ToolCallFunction{Arguments: sp(`Paris"}`)}},
		}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: sp("tool_calls")}}},
		{Done: true},
	}
	events := runTranslator(t, &req, chunks)
	if !contains(events,
		"event: response.output_item.added\n",
		"function_call_arguments.delta",
		"function_call_arguments.done",
		`"arguments":"{\"city\":\"Paris\"}"`,
	) {
		t.Fatalf("tool-call lifecycle broken: %v", events)
	}
	if contains(events, "upstream-minted-id") {
		t.Fatal("the upstream tool-call id must NEVER leak downstream (duplicate-id doom loop)")
	}
	if !contains(events, `"call_id":"call_`) {
		t.Fatalf("call_id must be proxy-minted: %v", events)
	}
	// completed required_tool_calls? no — completed regardless; order matters.
}

func TestTranslatorToolCallSplitsMessagesAndTwoToolsSequential(t *testing.T) {
	var req Request
	req.Model = "m"
	// Two sequential tool calls on indices 0 then 1 (chat streams by index).
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 0, Function: openai.ToolCallFunction{Name: sp("a"), Arguments: sp(`{"x":1}`)}},
		}}}}},
		{Choices: []openai.Choice{{Delta: &openai.Delta{ToolCalls: []openai.ToolCallDelta{
			{Index: 1, Function: openai.ToolCallFunction{Name: sp("b"), Arguments: sp(`{"y":2}`)}},
		}}}}},
		{Done: true},
	}
	tr := NewTranslator(&req, "resp_seq")
	var events []string
	for _, c := range chunks {
		events = append(events, tr.Process(c)...)
	}
	events = append(events, tr.Finish()...)
	items := tr.OutputItems()
	if len(items) != 2 {
		t.Fatalf("want 2 tool items, got %d: %v", len(items), items)
	}
	a := items[0].(map[string]any)
	b := items[1].(map[string]any)
	if a["name"] != "a" || b["name"] != "b" {
		t.Fatalf("tool order broken: %v %v", a["name"], b["name"])
	}
	// output_index of the second item's added event must follow the first's done.
	joined := strings.Join(events, "")
	i1 := strings.Index(joined, `"name":"a"`)
	i2 := strings.Index(joined, `"name":"b"`)
	if i1 == -1 || i2 == -1 || i2 < i1 {
		t.Fatal("tool item order in events wrong")
	}
}

func TestTranslatorIncompleteOnLength(t *testing.T) {
	var req Request
	req.Model = "m"
	events := runTranslator(t, &req, []openai.Chunk{
		TextDelta("cut off"),
		{Choices: []openai.Choice{{Delta: &openai.Delta{}, FinishReason: sp("length")}}},
		{Done: true},
	})
	names := eventNames(events)
	if names[len(names)-1] != "response.incomplete" {
		t.Fatalf("want response.incomplete, got %v", names)
	}
	if !contains(events, `"reason":"max_output_tokens"`) {
		t.Fatal("incomplete_details.reason must be max_output_tokens")
	}
}

func TestTranslatorFailedOnUpstreamError(t *testing.T) {
	var req Request
	req.Model = "m"
	events := runTranslator(t, &req, []openai.Chunk{
		TextDelta("partial"),
		{Error: &openai.Error{Message: "boom"}},
	})
	names := eventNames(events)
	if names[len(names)-1] != "response.failed" {
		t.Fatalf("want response.failed, got %v", names)
	}
	if !contains(events, "boom") {
		t.Fatal("upstream error message must reach the failure")
	}
}

func TestTranslatorEmptyStreamStillWellFormed(t *testing.T) {
	var req Request
	req.Model = "m"
	events := runTranslator(t, &req, []openai.Chunk{{Done: true}})
	names := eventNames(events)
	want := []string{"response.created", "response.in_progress", "response.completed"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("empty stream events:\nwant %v\ngot  %v", want, names)
	}
}

func TestTranslatorUsageFallbackEstimate(t *testing.T) {
	var req Request
	req.Model = "m"
	events := runTranslator(t, &req, []openai.Chunk{TextDelta("abcd"), {Done: true}})
	// No upstream usage chunk: output_tokens estimated chars/4 = 1.
	if !contains(events, `"output_tokens":1`) {
		t.Fatalf("usage fallback estimate missing: %v", events)
	}
}

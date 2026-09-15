package sse

import (
	"encoding/json"
	"testing"
)

// AggregateMessage must reassemble the exact event sequences the builder
// emits (verified against dumped downstream traffic) into a complete
// non-streaming Message.
func TestAggregateMessage(t *testing.T) {
	b := NewBuilder("msg_test-1", "test-model", 100, nil)
	b.SetUsage(UsageInfo{PromptTokens: 100595, CompletionTokens: 50})
	var events []string
	events = append(events, b.MessageStart())
	events = append(events, b.StartThinkingBlock())
	events = append(events, b.EmitThinkingDelta("Let me check."))
	events = append(events, b.EmitSignatureDelta())
	events = append(events, b.StopThinkingBlock())
	events = append(events, b.StartTextBlock())
	events = append(events, b.TextDelta("Hello "))
	events = append(events, b.TextDelta("world"))
	events = append(events, b.StopTextBlock())
	// Tool args arrive as pure concatenation fragments.
	events = append(events, b.StartToolBlock(0, "toolu_1", "Bash"))
	events = append(events, b.ContentBlockDelta(2, "input_json_delta", `{"command":`))
	events = append(events, b.ContentBlockDelta(2, "input_json_delta", `"ls -la","timeout":120}`))
	events = append(events, b.StopToolBlock(0))
	out := int64(50)
	events = append(events, b.MessageDelta("tool_use", &out, nil))
	events = append(events, b.MessageStop())

	msg := AggregateMessage(events)
	if msg["id"] != "msg_test-1" || msg["model"] != "test-model" || msg["type"] != "message" || msg["role"] != "assistant" {
		t.Fatalf("bad identity: %v", msg)
	}
	if msg["stop_reason"] != "tool_use" {
		t.Errorf("stop_reason = %v, want tool_use", msg["stop_reason"])
	}
	usage := msg["usage"].(map[string]any)
	if !numEq(usage["input_tokens"], 100595) || !numEq(usage["output_tokens"], 50) {
		t.Errorf("usage = %v", usage)
	}
	content := msg["content"].([]any)
	if len(content) != 3 {
		t.Fatalf("content len = %d, want 3 blocks: %v", len(content), content)
	}
	if thinking := content[0].(thinkingBlock); thinking.Thinking != "Let me check." || thinking.Signature == "" {
		t.Errorf("thinking block = %+v", thinking)
	}
	if text := content[1].(textBlock); text.Text != "Hello world" {
		t.Errorf("text block = %+v", text)
	}
	tool := content[2].(toolUseBlock)
	if tool.ID != "toolu_1" || tool.Name != "Bash" {
		t.Errorf("tool ids = %+v", tool)
	}
	input := tool.Input.(map[string]any)
	if input["command"] != "ls -la" || !numEq(input["timeout"], 120) {
		t.Errorf("tool input = %v (duplicated partials?)", input)
	}
}

func TestAggregateMessageEmpty(t *testing.T) {
	msg := AggregateMessage(nil)
	usage := msg["usage"].(map[string]any)
	if !numEq(usage["input_tokens"], 0) {
		t.Errorf("usage = %v, want zeroed", usage)
	}
	if msg["content"] == nil {
		t.Error("content must be an array, not null")
	}
}

// numEq compares decoded JSON numbers (float64/json.Number/int) with an int64.
func numEq(v any, want int64) bool {
	switch n := v.(type) {
	case int:
		return int64(n) == want
	case int64:
		return n == want
	case float64:
		return int64(n) == want
	case json.Number:
		got, err := n.Int64()
		return err == nil && got == want
	}
	return false
}

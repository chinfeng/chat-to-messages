package responses

import (
	"strings"
	"testing"

	"github.com/chinfeng/chat-to-messages/internal/convert"
)

func messagesOf(t *testing.T, body map[string]any) []map[string]any {
	t.Helper()
	msgs, ok := body["messages"].([]map[string]any)
	if !ok {
		t.Fatalf("messages missing/not []map[string]any: %T", body["messages"])
	}
	return msgs
}

func TestBuildChatBodyStringInput(t *testing.T) {
	var req Request
	req.Model = "m"
	req.Input.IsString = true
	req.Input.Str = "hello"
	body, err := BuildChatBody(&req, nil, convert.ReplayDisabled)
	if err != nil {
		t.Fatal(err)
	}
	msgs := messagesOf(t, body)
	if len(msgs) != 1 {
		t.Fatalf("want 1 message, got %d", len(msgs))
	}
	m := msgs[0]
	if m["role"] != "user" || m["content"] != "hello" {
		t.Fatalf("bad message: %v", m)
	}
}

func TestBuildChatBodyInstructionsAndHistoryOrder(t *testing.T) {
	var req Request
	req.Model = "m"
	sys := "be terse"
	req.Instructions = &sys
	// History (previous_response_id expansion) then the new input items.
	history := []Item{
		{Type: "message", Role: "user", Content: ContentValue{IsString: true, Str: "old"}},
		{Type: "message", Role: "assistant", Content: ContentValue{Parts: []ContentPart{{Type: "output_text", Text: "older"}}}},
	}
	req.Input.Items = []Item{{Type: "message", Role: "user", Content: ContentValue{IsString: true, Str: "new"}}}
	body, err := BuildChatBody(&req, append(history, req.Input.Items...), convert.ReplayDisabled)
	if err != nil {
		t.Fatal(err)
	}
	msgs := messagesOf(t, body)
	if len(msgs) != 4 {
		t.Fatalf("want 4 messages, got %d: %v", len(msgs), msgs)
	}
	if msgs[0]["role"] != "system" {
		t.Fatal("instructions must be system first")
	}
	got := []string{
		msgs[1]["content"].(string),
		msgs[2]["content"].(string),
		msgs[3]["content"].(string),
	}
	if got[0] != "old" || got[1] != "older" || got[2] != "new" {
		t.Fatalf("order broken: %v", got)
	}
}

func TestBuildChatBodyAssistantTurnMerge(t *testing.T) {
	var req Request
	req.Model = "m"
	history := []Item{
		{Type: "reasoning", Summary: []SummaryPart{{Type: "summary_text", Text: "pondering"}}},
		{Type: "message", Role: "assistant", Content: ContentValue{Parts: []ContentPart{{Type: "output_text", Text: "calling"}}}},
		{Type: "function_call", CallID: "call_1", Name: "lookup", Arguments: `{"q":"x"}`},
		{Type: "function_call", CallID: "call_2", Name: "lookup", Arguments: `{"q":"y"}`},
		{Type: "function_call_output", CallID: "call_1", Output: ContentValue{IsString: true, Str: "res-a"}},
		{Type: "function_call_output", CallID: "call_2", Output: ContentValue{IsString: true, Str: "res-b"}},
	}
	body, err := BuildChatBody(&req, history, convert.ReplayThinkTags)
	if err != nil {
		t.Fatal(err)
	}
	msgs := messagesOf(t, body)
	if len(msgs) != 3 {
		t.Fatalf("want [assistant, tool, tool], got %d messages: %v", len(msgs), msgs)
	}
	assistant := msgs[0]
	content, _ := assistant["content"].(string)
	if !strings.Contains(content, "<think>") || !strings.Contains(content, "pondering") || !strings.Contains(content, "calling") {
		t.Fatalf("think_tags replay broken: %q", content)
	}
	calls, ok := assistant["tool_calls"].([]any)
	if !ok || len(calls) != 2 {
		t.Fatalf("want 2 tool_calls in the single assistant message, got %v", assistant["tool_calls"])
	}
	first := calls[0].(map[string]any)
	if first["id"] != "call_1" || first["function"].(map[string]any)["name"] != "lookup" {
		t.Fatalf("bad tool_call: %v", first)
	}
	tool1 := msgs[1]
	if tool1["role"] != "tool" || tool1["tool_call_id"] != "call_1" || tool1["content"] != "res-a" {
		t.Fatalf("bad tool message: %v", tool1)
	}
}

func TestBuildChatBodyReasoningReplayModes(t *testing.T) {
	history := []Item{{Type: "reasoning", EncryptedContent: EncryptReasoning("secret thought")}}
	var req Request
	req.Model = "m"

	body, err := BuildChatBody(&req, history, convert.ReplayReasoningContent)
	if err != nil {
		t.Fatal(err)
	}
	m := messagesOf(t, body)[0]
	if m["reasoning_content"] != "secret thought" {
		t.Fatalf("reasoning_content replay broken: %v", m)
	}

	body, err = BuildChatBody(&req, history, convert.ReplayDisabled)
	if err != nil {
		t.Fatal(err)
	}
	m = messagesOf(t, body)[0]
	if _, ok := m["reasoning_content"]; ok {
		t.Fatal("disabled replay must drop reasoning")
	}
	if c, _ := m["content"].(string); strings.Contains(c, "secret") {
		t.Fatalf("disabled replay leaked reasoning into content: %q", c)
	}
}

func TestBuildChatBodyRejections(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Request)
	}{
		{"background", func(r *Request) { r.Background = true }},
		{"hosted tool", func(r *Request) { r.Tools = []Tool{{Type: "web_search"}} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var req Request
			req.Model = "m"
			req.Input.IsString = true
			req.Input.Str = "hi"
			tc.mut(&req)
			_, err := BuildChatBody(&req, nil, convert.ReplayDisabled)
			ae, ok := err.(*APIError)
			if !ok || ae.HTTPStatus != 400 {
				t.Fatalf("want 400 APIError, got %v", err)
			}
		})
	}
}

func TestBuildChatBodyInputAudioAndFileIDRejected(t *testing.T) {
	var req Request
	req.Model = "m"
	audio := Item{Type: "message", Role: "user", Content: ContentValue{Parts: []ContentPart{{Type: "input_audio"}}}}
	if _, err := BuildChatBody(&req, []Item{audio}, convert.ReplayDisabled); err == nil {
		t.Fatal("input_audio must 400")
	}
	fileByID := Item{Type: "message", Role: "user", Content: ContentValue{Parts: []ContentPart{{Type: "input_file", FileID: "file-1"}}}}
	if _, err := BuildChatBody(&req, []Item{fileByID}, convert.ReplayDisabled); err == nil {
		t.Fatal("file_id-only input_file must 400")
	}
}

func TestBuildChatBodyFields(t *testing.T) {
	var req Request
	req.Model = "m"
	req.Input.IsString = true
	req.Input.Str = "hi"
	req.MaxOutputTokens = int64(128)
	tmp := 0.5
	req.Temperature = tmp
	req.ToolChoice = map[string]any{"type": "function", "name": "lookup"}
	req.Tools = []Tool{{Type: "function", Name: "lookup", Description: "finds"}}
	req.Text = &TextConfig{Format: TextFormat{Type: "json_schema", Name: "ans", Schema: map[string]any{"type": "object"}}}
	req.Reasoning = &ReasoningConfig{Effort: "low"}
	body, err := BuildChatBody(&req, nil, convert.ReplayDisabled)
	if err != nil {
		t.Fatal(err)
	}
	if body["max_completion_tokens"] != int64(128) {
		t.Fatalf("max_completion_tokens: %v", body["max_completion_tokens"])
	}
	if body["temperature"] != 0.5 {
		t.Fatalf("temperature: %v", body["temperature"])
	}
	tc, ok := body["tool_choice"].(map[string]any)
	if !ok || tc["type"] != "function" {
		t.Fatalf("tool_choice conversion: %v", body["tool_choice"])
	}
	rf, ok := body["response_format"].(map[string]any)
	if !ok || rf["type"] != "json_schema" {
		t.Fatalf("response_format: %v", body["response_format"])
	}
	if body["reasoning_effort"] != "low" {
		t.Fatalf("reasoning_effort: %v", body["reasoning_effort"])
	}
	tools := body["tools"].([]any)
	fn := tools[0].(map[string]any)["function"].(map[string]any)
	if fn["parameters"] == nil {
		t.Fatal("function tool without parameters must default to an empty object schema")
	}
}

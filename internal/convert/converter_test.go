package convert

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/chinfeng/chat-to-messages/internal/anthropic"
)

func decodeMessage(t *testing.T, data string) anthropic.Message {
	t.Helper()
	var m anthropic.Message
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		t.Fatal(err)
	}
	return m
}

func decodeMessages(t *testing.T, data string) []anthropic.Message {
	t.Helper()
	var msgs []anthropic.Message
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&msgs); err != nil {
		t.Fatal(err)
	}
	return msgs
}

func jsonStr(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func convert(t *testing.T, msgs []anthropic.Message, replay ReasoningReplayMode) []map[string]any {
	t.Helper()
	got, err := ConvertMessages(msgs, replay)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// --- brief inline vectors -------------------------------------------------

func TestConvertSystemPromptStripsBillingHeader(t *testing.T) {
	got := ConvertSystemPrompt("x-anthropic-billing-header: cch=abc;\nYou are a helper.")
	if got["role"] != "system" || got["content"] != "You are a helper." {
		t.Errorf("got %v", got)
	}
	// 整串仅表头 → nil
	if got := ConvertSystemPrompt("x-anthropic-billing-header: cch=abc;"); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
	// 数组形式
	got2 := ConvertSystemPrompt([]any{
		map[string]any{"type": "text", "text": "x-anthropic-billing-header: cch=z;\nPart A"},
		map[string]any{"type": "text", "text": "Part B"},
	})
	if got2["content"] != "Part A\n\nPart B" {
		t.Errorf("got %v", got2)
	}
}

func TestConvertThinkingThinkTags(t *testing.T) {
	msgs := []anthropic.Message{{
		Role: "assistant",
		Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "thinking", Thinking: "hmm", Signature: "sig123"},
			{Type: "text", Text: "answer"},
		}},
	}}
	got := convert(t, msgs, ReplayThinkTags)
	if len(got) != 1 {
		t.Fatalf("msgs = %d", len(got))
	}
	content := got[0]["content"].(string)
	if !strings.Contains(content, "<!--sig:sig123-->") || !strings.Contains(content, "<think>\nhmm\n</think>") || !strings.Contains(content, "answer") {
		t.Errorf("content = %q", content)
	}
}

func TestConvertThinkingDisabledDropsThinking(t *testing.T) {
	msgs := []anthropic.Message{{
		Role: "assistant",
		Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "thinking", Thinking: "hmm"},
			{Type: "text", Text: "answer"},
		}},
	}}
	got := convert(t, msgs, ReplayDisabled)
	if !strings.Contains(got[0]["content"].(string), "answer") {
		t.Errorf("content = %v", got[0]["content"])
	}
	if strings.Contains(got[0]["content"].(string), "hmm") {
		t.Error("thinking must be dropped")
	}
}

func TestConvertThinkingReasoningContent(t *testing.T) {
	msgs := []anthropic.Message{{
		Role: "assistant",
		Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "thinking", Thinking: "hmm"},
			{Type: "text", Text: "answer"},
		}},
	}}
	got := convert(t, msgs, ReplayReasoningContent)
	if got[0]["reasoning_content"] != "hmm" {
		t.Errorf("reasoning_content = %v", got[0]["reasoning_content"])
	}
	if got[0]["content"] != "answer" {
		t.Errorf("content = %v", got[0]["content"])
	}
}

func TestConvertToolUseCanonicalArgs(t *testing.T) {
	msgs := []anthropic.Message{{
		Role: "assistant",
		Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "tool_use", ID: "tu1", Name: "f",
				Input: json.RawMessage(`{"b":2,"a":1,"arr":[3,1]}`)},
		}},
	}}
	got := convert(t, msgs, ReplayThinkTags)
	tc := got[0]["tool_calls"].([]any)[0].(map[string]any)
	fn := tc["function"].(map[string]any)
	if fn["arguments"] != `{"a":1,"arr":[3,1],"b":2}` {
		t.Errorf("args = %v", fn["arguments"])
	}
}

func TestConvertToolResultAndMedia(t *testing.T) {
	msgs := []anthropic.Message{
		{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "tool_result", ToolUseID: "tu1", Content: json.RawMessage(`[{"type":"text","text":"ok"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}]`)},
		}}},
	}
	got := convert(t, msgs, ReplayThinkTags)
	// [tool 消息, 合成 user 消息]
	if len(got) != 2 {
		t.Fatalf("msgs = %d: %v", len(got), jsonStr(t, got))
	}
	toolMsg := got[0]
	if toolMsg["role"] != "tool" || toolMsg["tool_call_id"] != "tu1" {
		t.Errorf("tool msg = %v", toolMsg)
	}
	if !strings.Contains(toolMsg["content"].(string), "[tool result media moved to the following user message]") {
		t.Errorf("tool content = %v", toolMsg["content"])
	}
	userMsg := got[1]
	if userMsg["role"] != "user" {
		t.Errorf("user msg = %v", userMsg)
	}
	parts := userMsg["content"].([]any)
	if len(parts) != 2 {
		t.Fatalf("parts = %d", len(parts))
	}
	img := parts[1].(map[string]any)
	if img["type"] != "image_url" {
		t.Errorf("img = %v", img)
	}
	iu := img["image_url"].(map[string]any)
	if iu["url"] != "data:image/png;base64,AAAA" {
		t.Errorf("url = %v", iu["url"])
	}
}

func TestConvertToolErrorPrefix(t *testing.T) {
	msgs := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "tool_result", ToolUseID: "tu1", IsError: true, Content: json.RawMessage(`"boom"`)},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	if !strings.HasPrefix(got[0]["content"].(string), "[TOOL_ERROR] boom") {
		t.Errorf("content = %v", got[0]["content"])
	}
}

func TestConvertDeferredPostToolBlocks(t *testing.T) {
	msgs := decodeMessages(t, `[
		{"role":"assistant","content":[
			{"type":"text","text":"I will call"},
			{"type":"tool_use","id":"tu1","name":"f","input":{}},
			{"type":"text","text":"deferred text after tool"}
		]},
		{"role":"user","content":"continue"}
	]`)
	got := convert(t, msgs, ReplayThinkTags)
	// assistant(tool_calls) 先出；deferred text 在 user 消息之前注入
	if len(got) != 3 {
		t.Fatalf("msgs = %d: %v", len(got), jsonStr(t, got))
	}
	if got[0]["role"] != "assistant" {
		t.Errorf("m0 = %v", got[0])
	}
	if got[1]["role"] != "assistant" || !strings.Contains(got[1]["content"].(string), "deferred text after tool") {
		t.Errorf("m1 = %v", got[1])
	}
	if got[2]["role"] != "user" {
		t.Errorf("m2 = %v", got[2])
	}
}

func TestConvertUserImageMessage(t *testing.T) {
	msgs := decodeMessages(t, `[
		{"role":"user","content":[
			{"type":"text","text":"look at"},
			{"type":"image","source":{"type":"base64","media_type":"image/jpeg","data":"BBBB"},"detail":"low"}
		]}
	]`)
	got := convert(t, msgs, ReplayThinkTags)
	content := got[0]["content"].([]any)
	if len(content) != 2 {
		t.Fatalf("parts = %d: %v", len(content), jsonStr(t, content))
	}
	img := content[1].(map[string]any)
	iu := img["image_url"].(map[string]any)
	if iu["url"] != "data:image/jpeg;base64,BBBB" || iu["detail"] != "low" {
		t.Errorf("img = %v", img)
	}
}

func TestConvertDocumentBlock(t *testing.T) {
	msgs := decodeMessages(t, `[
		{"role":"user","content":[{"type":"document","title":"report.pdf","context":"about it","source":{"type":"base64","media_type":"application/pdf","data":"Q1JJ"}}]}
	]`)
	got := convert(t, msgs, ReplayThinkTags)
	content := got[0]["content"].(string)
	if !strings.Contains(content, "[Document: report.pdf (data:application/pdf;base64,Q1JJ)]") {
		t.Errorf("content = %q", content)
	}
	if !strings.Contains(content, "about it") {
		t.Errorf("context missing: %q", content)
	}
}

func TestConvertToolChoice(t *testing.T) {
	if got := ConvertToolChoice(map[string]any{"type": "tool", "name": "f"}); got.(map[string]any)["type"] != "function" {
		t.Errorf("tool → %v", got)
	}
	if got := ConvertToolChoice(map[string]any{"type": "any"}); got != "required" {
		t.Errorf("any → %v", got)
	}
	// TS returns the raw choiceType string for auto (identity mapping).
	if got := ConvertToolChoice(map[string]any{"type": "auto"}); got != "auto" {
		t.Errorf("auto → %v", got)
	}
	if !HasDisableParallelToolUse(map[string]any{"type": "auto", "disable_parallel_tool_use": true}) {
		t.Error("disable_parallel")
	}
	if HasDisableParallelToolUse(map[string]any{"type": "auto"}) {
		t.Error("must be false")
	}
}

func TestConvertToolsFiltersServerTools(t *testing.T) {
	tools := []map[string]any{
		{"type": "custom", "name": "t1", "description": "d", "input_schema": map[string]any{"type": "object", "properties": map[string]any{}}},
		{"type": "web_search_20250305", "name": "web_search"},
	}
	got := ConvertTools(tools)
	if len(got) != 1 {
		t.Fatalf("tools = %d: %v", len(got), got)
	}
	fn := got[0]["function"].(map[string]any)
	if fn["name"] != "t1" || fn["description"] != "d" {
		t.Errorf("fn = %v", fn)
	}
	if _, ok := fn["parameters"]; !ok {
		t.Error("parameters missing")
	}
}

func TestBuildBaseRequestBody(t *testing.T) {
	req := &RequestData{
		Model:     "m1",
		Messages:  []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{Str: "hi", IsString: true}}},
		System:    "x-anthropic-billing-header: cch=1;\nYou are helpful",
		MaxTokens: json.Number("4096"),
		Tools: []map[string]any{
			{"type": "custom", "name": "t1", "input_schema": map[string]any{"type": "object"}},
			{"type": "web_search_20250305", "name": "web_search"},
		},
		ToolChoice: map[string]any{"type": "tool", "name": "t1", "disable_parallel_tool_use": true},
	}
	body, err := BuildBaseRequestBody(req, nil, ReplayThinkTags)
	if err != nil {
		t.Fatal(err)
	}
	if body["model"] != "m1" {
		t.Errorf("model = %v", body["model"])
	}
	msgs := body["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("messages = %d", len(msgs))
	}
	sys := msgs[0].(map[string]any)
	// server tool suffix is appended after the stripped prompt
	if sys["role"] != "system" || !strings.HasPrefix(sys["content"].(string), "You are helpful") {
		t.Errorf("system = %v", sys)
	}
	// server tool schema 被附加到 tools
	tools := body["tools"].([]any)
	if len(tools) != 2 {
		t.Fatalf("tools = %d", len(tools))
	}
	// 系统提示后缀注入
	if !strings.Contains(sys["content"].(string), "web_search") {
		t.Errorf("server tool suffix missing: %v", sys["content"])
	}
	if body["parallel_tool_calls"] != false {
		t.Errorf("parallel = %v", body["parallel_tool_calls"])
	}
	if body["max_tokens"] != json.Number("4096") {
		t.Errorf("max_tokens = %v", body["max_tokens"])
	}
}

// --- ported converter.test.ts vectors -------------------------------------

// TS: converts a simple user message.
func TestConvertSimpleUserMessage(t *testing.T) {
	msgs := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{Str: "Hello", IsString: true}}}
	got := convert(t, msgs, ReplayThinkTags)
	want := []map[string]any{{"role": "user", "content": "Hello"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: converts user message with content blocks.
func TestConvertUserMessageContentBlocks(t *testing.T) {
	msgs := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "text", Text: "Hello"},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	want := []map[string]any{{"role": "user", "content": "Hello"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: converts assistant message with text.
func TestConvertAssistantTextMessage(t *testing.T) {
	msgs := []anthropic.Message{{Role: "assistant", Content: anthropic.ContentValue{Str: "Hi there", IsString: true}}}
	got := convert(t, msgs, ReplayThinkTags)
	want := []map[string]any{{"role": "assistant", "content": "Hi there"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: converts assistant message with thinking blocks (THINK_TAGS).
func TestConvertThinkingThinkTagsContent(t *testing.T) {
	msgs := []anthropic.Message{{Role: "assistant", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "thinking", Thinking: "Let me think..."},
		{Type: "text", Text: "Here is the answer."},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	if len(got) != 1 {
		t.Fatalf("len = %d", len(got))
	}
	content := got[0]["content"].(string)
	if !strings.Contains(content, "Let me think...") || !strings.Contains(content, "Here is the answer.") || !strings.Contains(content, "\nLet me think...\n") {
		t.Errorf("content = %q", content)
	}
}

// TS: skips thinking blocks when DISABLED.
func TestConvertThinkingDisabled(t *testing.T) {
	msgs := []anthropic.Message{{Role: "assistant", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "thinking", Thinking: "Internal thought"},
		{Type: "text", Text: "Public answer."},
	}}}}
	got := convert(t, msgs, ReplayDisabled)
	want := []map[string]any{{"role": "assistant", "content": "Public answer."}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: uses reasoning_content with REASONING_CONTENT mode (string content).
func TestConvertReasoningContentStringMessage(t *testing.T) {
	s := "My reasoning"
	msgs := []anthropic.Message{{Role: "assistant", Content: anthropic.ContentValue{Str: "Answer.", IsString: true}, ReasoningContent: &s}}
	got := convert(t, msgs, ReplayReasoningContent)
	if len(got) != 1 {
		t.Fatalf("len = %d", len(got))
	}
	if got[0]["reasoning_content"] != "My reasoning" {
		t.Errorf("reasoning_content = %v", got[0]["reasoning_content"])
	}
	if got[0]["content"] != "Answer." {
		t.Errorf("content = %v", got[0]["content"])
	}
}

// TS: converts tool_use blocks to tool_calls.
func TestConvertToolUseToToolCalls(t *testing.T) {
	msgs := []anthropic.Message{{Role: "assistant", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "text", Text: "Let me look that up."},
		{Type: "tool_use", ID: "tool_001", Name: "search", Input: json.RawMessage(`{"query":"test"}`)},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	if len(got) != 1 {
		t.Fatalf("len = %d", len(got))
	}
	msg := got[0]
	if msg["role"] != "assistant" {
		t.Errorf("role = %v", msg["role"])
	}
	calls := msg["tool_calls"].([]any)
	if len(calls) != 1 {
		t.Fatalf("calls = %d", len(calls))
	}
	call := calls[0].(map[string]any)
	if call["id"] != "tool_001" {
		t.Errorf("id = %v", call["id"])
	}
	fn := call["function"].(map[string]any)
	if fn["name"] != "search" {
		t.Errorf("name = %v", fn["name"])
	}
	if fn["arguments"] != `{"query":"test"}` {
		t.Errorf("arguments = %v", fn["arguments"])
	}
}

// TS: converts tool_result blocks to tool messages.
func TestConvertToolResultToToolMessage(t *testing.T) {
	msgs := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "tool_result", ToolUseID: "tool_001", Content: json.RawMessage(`"result data"`)},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	want := []map[string]any{{"role": "tool", "tool_call_id": "tool_001", "content": "result data"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: handles tool_result with array content.
func TestConvertToolResultArrayContent(t *testing.T) {
	msgs := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "tool_result", ToolUseID: "tool_001", Content: json.RawMessage(`[{"type":"text","text":"line 1"},{"type":"text","text":"line 2"}]`)},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	want := []map[string]any{{"role": "tool", "tool_call_id": "tool_001", "content": "line 1\nline 2"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: handles deferred post-tool blocks.
func TestConvertDeferredPostToolExplanation(t *testing.T) {
	msgs := []anthropic.Message{
		{Role: "assistant", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "tool_use", ID: "tool_001", Name: "read", Input: json.RawMessage(`{"path":"/a"}`)},
			{Type: "text", Text: "Now I can explain."},
		}}},
		{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "tool_result", ToolUseID: "tool_001", Content: json.RawMessage(`"file content"`)},
		}}},
	}
	got := convert(t, msgs, ReplayThinkTags)
	hasExplanation := false
	for _, m := range got {
		if m["role"] == "assistant" {
			if s, ok := m["content"].(string); ok && strings.Contains(s, "Now I can explain.") {
				hasExplanation = true
			}
		}
	}
	if !hasExplanation {
		t.Errorf("missing deferred explanation: %v", got)
	}
}

// TS: replays redacted_thinking as placeholder text.
func TestConvertRedactedThinking(t *testing.T) {
	msgs := []anthropic.Message{{Role: "assistant", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "redacted_thinking", Data: "..."},
		{Type: "text", Text: "Response."},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	want := []map[string]any{{"role": "assistant", "content": "[redacted thinking]\n\nResponse."}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: converts user image blocks to image_url format.
func TestConvertUserImageToImageURL(t *testing.T) {
	msgs := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "image", Source: &anthropic.Source{Type: "base64", MediaType: "image/png", Data: "YWJj"}},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	if len(got) != 1 {
		t.Fatalf("len = %d", len(got))
	}
	content := got[0]["content"].([]any)
	if len(content) != 1 {
		t.Fatalf("content = %d", len(content))
	}
	part := content[0].(map[string]any)
	if part["type"] != "image_url" {
		t.Errorf("type = %v", part["type"])
	}
	iu := part["image_url"].(map[string]any)
	if !reflect.DeepEqual(iu, map[string]any{"url": "data:image/png;base64,YWJj"}) {
		t.Errorf("image_url = %v", iu)
	}
}

// TS: handles server_tool_use blocks by skipping them (proxy-side only).
func TestConvertSkipsServerToolUse(t *testing.T) {
	msgs := []anthropic.Message{{Role: "assistant", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "text", Text: "Let me search for that."},
		{Type: "server_tool_use", ID: "st_1", Name: "web_search", Input: json.RawMessage(`{"query":"test"}`)},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	if len(got) != 1 {
		t.Fatalf("len = %d", len(got))
	}
	content := got[0]["content"].(string)
	if !strings.Contains(content, "Let me search for that.") {
		t.Errorf("content = %q", content)
	}
}

// TS: converts web_search_tool_result blocks as tool results.
func TestConvertWebSearchToolResult(t *testing.T) {
	msgs := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "web_search_tool_result", ToolUseID: "st_1", Content: json.RawMessage(`[{"type":"web_search_result","url":"https://example.com","title":"Example"}]`)},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	// Go maps have no insertion order; the textified JSON uses sorted keys.
	want := []map[string]any{{"role": "tool", "tool_call_id": "st_1",
		"content": `{"title":"Example","type":"web_search_result","url":"https://example.com"}`}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: converts web_fetch_tool_result blocks as tool results.
func TestConvertWebFetchToolResult(t *testing.T) {
	msgs := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "web_fetch_tool_result", ToolUseID: "st_2", Content: json.RawMessage(`[{"type":"text","text":"Page content here"}]`)},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	want := []map[string]any{{"role": "tool", "tool_call_id": "st_2", "content": "Page content here"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: converts Anthropic tools to OpenAI function format.
func TestConvertToolsRegular(t *testing.T) {
	tools := []map[string]any{
		{"name": "read_file", "description": "Read a file",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}}},
	}
	got := ConvertTools(tools)
	want := []map[string]any{{
		"type": "function",
		"function": map[string]any{
			"name":        "read_file",
			"description": "Read a file",
			"parameters":  map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}},
		},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: provides default schema when input_schema is missing.
func TestConvertToolsDefaultSchema(t *testing.T) {
	tools := []map[string]any{{"name": "noop", "description": "Does nothing"}}
	got := ConvertTools(tools)
	params := got[0]["function"].(map[string]any)["parameters"]
	want := map[string]any{"type": "object", "properties": map[string]any{}}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("parameters = %v", params)
	}
}

// TS: converts 'any' to 'required'.
func TestConvertToolChoiceAny(t *testing.T) {
	if got := ConvertToolChoice(map[string]any{"type": "any"}); got != "required" {
		t.Errorf("any → %v", got)
	}
}

// TS: converts 'tool' choice to function format.
func TestConvertToolChoiceTool(t *testing.T) {
	got := ConvertToolChoice(map[string]any{"type": "tool", "name": "search"})
	want := map[string]any{"type": "function", "function": map[string]any{"name": "search"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("tool → %v", got)
	}
}

// TS: passes through 'auto' and 'none'.
func TestConvertToolChoiceAutoNone(t *testing.T) {
	if got := ConvertToolChoice("auto"); got != "auto" {
		t.Errorf("auto → %v", got)
	}
	if got := ConvertToolChoice("none"); got != "none" {
		t.Errorf("none → %v", got)
	}
	if got := ConvertToolChoice(map[string]any{"type": "none"}); got != "none" {
		t.Errorf("none obj → %v", got)
	}
}

// TS: converts string system prompt.
func TestConvertSystemPromptString(t *testing.T) {
	got := ConvertSystemPrompt("You are helpful.")
	want := map[string]any{"role": "system", "content": "You are helpful."}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: converts array system prompt.
func TestConvertSystemPromptArray(t *testing.T) {
	got := ConvertSystemPrompt([]any{
		map[string]any{"type": "text", "text": "Part 1."},
		map[string]any{"type": "text", "text": "Part 2."},
	})
	want := map[string]any{"role": "system", "content": "Part 1.\n\nPart 2."}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: returns null for null input and empty string.
func TestConvertSystemPromptNullEmpty(t *testing.T) {
	if got := ConvertSystemPrompt(nil); got != nil {
		t.Errorf("nil → %v", got)
	}
	if got := ConvertSystemPrompt(""); got != nil {
		t.Errorf("empty → %v", got)
	}
}

// TS: strips a leading billing-header line from a string system prompt.
func TestConvertSystemPromptBillingHeaderString(t *testing.T) {
	sys := "x-anthropic-billing-header: cch=abc123; cc_version=1;\nYou are helpful."
	got := ConvertSystemPrompt(sys)
	want := map[string]any{"role": "system", "content": "You are helpful."}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: strips the leading billing-header line from each text block.
func TestConvertSystemPromptBillingHeaderArray(t *testing.T) {
	got := ConvertSystemPrompt([]any{
		map[string]any{"type": "text", "text": "x-anthropic-billing-header: cch=zzz;\nPart A."},
		map[string]any{"type": "text", "text": "Part B."},
	})
	want := map[string]any{"role": "system", "content": "Part A.\n\nPart B."}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: returns null when the system prompt is ONLY the billing header.
func TestConvertSystemPromptBillingHeaderOnly(t *testing.T) {
	if got := ConvertSystemPrompt("x-anthropic-billing-header: cch=rotating;"); got != nil {
		t.Errorf("got %v", got)
	}
}

// TS: leaves a billing-like string not at offset 0 untouched.
func TestConvertSystemPromptBillingNotAtStart(t *testing.T) {
	sys := "You are helpful.\nx-anthropic-billing-header: not at start"
	got := ConvertSystemPrompt(sys)
	want := map[string]any{"role": "system", "content": sys}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: handles CRLF after the billing header line.
func TestConvertSystemPromptBillingCRLF(t *testing.T) {
	sys := "x-anthropic-billing-header: cch=x;\r\nYou are helpful."
	got := ConvertSystemPrompt(sys)
	want := map[string]any{"role": "system", "content": "You are helpful."}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: serializes tool_use input with sorted keys (canonical args).
func TestToolUseArgumentsCanonical(t *testing.T) {
	msgs := []anthropic.Message{{Role: "assistant", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "tool_use", ID: "tu_1", Name: "search", Input: json.RawMessage(`{"z":1,"a":2,"m":0}`)},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	args := got[0]["tool_calls"].([]any)[0].(map[string]any)["function"].(map[string]any)["arguments"]
	if args != `{"a":2,"m":0,"z":1}` {
		t.Errorf("args = %v", args)
	}
}

// TS: produces stable arguments regardless of input key order.
func TestToolUseArgumentsStable(t *testing.T) {
	mk := func(input string) string {
		msgs := []anthropic.Message{{Role: "assistant", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "tool_use", ID: "t", Name: "f", Input: json.RawMessage(input)},
		}}}}
		got := convert(t, msgs, ReplayThinkTags)
		return got[0]["tool_calls"].([]any)[0].(map[string]any)["function"].(map[string]any)["arguments"].(string)
	}
	a := mk(`{"b":2,"a":1}`)
	b := mk(`{"a":1,"b":2}`)
	if a != b || a != `{"a":1,"b":2}` {
		t.Errorf("a = %s, b = %s", a, b)
	}
}

// TS: extracts an image block from tool_result into a synthetic user turn.
func TestToolResultMediaExtraction(t *testing.T) {
	msgs := []anthropic.Message{
		{Role: "assistant", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "tool_use", ID: "tu_1", Name: "screenshot", Input: json.RawMessage(`{}`)},
		}}},
		{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "tool_result", ToolUseID: "tu_1", Content: json.RawMessage(`[{"type":"text","text":"captured"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"YWJj"}}]`)},
		}}},
	}
	got := convert(t, msgs, ReplayThinkTags)

	var toolMsg map[string]any
	for _, m := range got {
		if m["role"] == "tool" {
			toolMsg = m
		}
	}
	if toolMsg == nil {
		t.Fatalf("no tool message: %v", got)
	}
	if toolMsg["tool_call_id"] != "tu_1" {
		t.Errorf("tool_call_id = %v", toolMsg["tool_call_id"])
	}
	content := toolMsg["content"].(string)
	if !strings.Contains(content, "captured") {
		t.Errorf("content = %q", content)
	}
	if strings.Contains(content, `"type":"image"`) {
		t.Errorf("image must not be stringified: %q", content)
	}
	if !strings.Contains(content, "[tool result media moved to the following user message]") {
		t.Errorf("content = %q", content)
	}

	var synthetic map[string]any
	for _, m := range got {
		if m["role"] == "user" {
			if parts, ok := m["content"].([]any); ok {
				for _, p := range parts {
					if pm, ok := p.(map[string]any); ok && pm["type"] == "image_url" {
						synthetic = m
					}
				}
			}
		}
	}
	if synthetic == nil {
		t.Fatalf("no synthetic user message: %v", got)
	}
	parts := synthetic["content"].([]any)
	hasTextMarker := false
	for _, p := range parts {
		if pm, ok := p.(map[string]any); ok && pm["type"] == "text" {
			hasTextMarker = true
		}
	}
	if !hasTextMarker {
		t.Error("synthetic message must carry the text marker")
	}
	var imgPart map[string]any
	for _, p := range parts {
		if pm, ok := p.(map[string]any); ok && pm["type"] == "image_url" {
			imgPart = pm
		}
	}
	if imgPart == nil {
		t.Fatal("no image_url part")
	}
	iu := imgPart["image_url"].(map[string]any)
	if iu["url"] != "data:image/png;base64,YWJj" {
		t.Errorf("url = %v", iu["url"])
	}
}

// TS: does NOT synthesize a media turn for text-only tool results.
func TestNoSyntheticMediaTurn(t *testing.T) {
	msgs := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "tool_result", ToolUseID: "t1", Content: json.RawMessage(`"plain result"`)},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	want := []map[string]any{{"role": "tool", "tool_call_id": "t1", "content": "plain result"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: keeps structured (web_search_result) tool content as textified JSON.
func TestStructuredToolContentUnchanged(t *testing.T) {
	msgs := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "web_search_tool_result", ToolUseID: "st_1", Content: json.RawMessage(`[{"type":"web_search_result","url":"https://example.com","title":"Example"}]`)},
	}}}}
	got := convert(t, msgs, ReplayThinkTags)
	want := []map[string]any{{"role": "tool", "tool_call_id": "st_1",
		"content": `{"title":"Example","type":"web_search_result","url":"https://example.com"}`}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// TS: user image block while deferred blocks are pending throws
// OpenAIConversionError (TS throws; Go returns the error).
func TestConvertUserImageWithPendingErrors(t *testing.T) {
	msgs := decodeMessages(t, `[
		{"role":"assistant","content":[
			{"type":"tool_use","id":"tu1","name":"f","input":{}},
			{"type":"text","text":"deferred"}
		]},
		{"role":"user","content":[
			{"type":"image","source":{"type":"base64","media_type":"image/png","data":"YWJj"}}
		]}
	]`)
	_, err := ConvertMessages(msgs, ReplayThinkTags)
	if err == nil {
		t.Fatal("expected error")
	}
	var convErr *OpenAIConversionError
	if !errors.As(err, &convErr) {
		t.Errorf("expected OpenAIConversionError, got %T: %v", err, err)
	}
}

// TS: builds a complete request body.
func TestBuildBaseRequestBodyComplete(t *testing.T) {
	req := &RequestData{
		Model:       "gpt-4o",
		Messages:    []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{Str: "Hi", IsString: true}}},
		System:      "You are helpful.",
		MaxTokens:   json.Number("1024"),
		Temperature: json.Number("0.7"),
		Tools: []map[string]any{{"name": "read", "description": "Read file",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{}}}},
		ToolChoice: map[string]any{"type": "auto"},
	}
	body, err := BuildBaseRequestBody(req, nil, ReplayThinkTags)
	if err != nil {
		t.Fatal(err)
	}
	if body["model"] != "gpt-4o" {
		t.Errorf("model = %v", body["model"])
	}
	if body["max_tokens"] != json.Number("1024") {
		t.Errorf("max_tokens = %v", body["max_tokens"])
	}
	if body["temperature"] != json.Number("0.7") {
		t.Errorf("temperature = %v", body["temperature"])
	}
	msgs := body["messages"].([]any)
	if len(msgs) != 2 {
		t.Errorf("messages = %d", len(msgs))
	}
}

// TS: uses default max_tokens when not provided.
func TestBuildBaseRequestBodyDefaultMaxTokens(t *testing.T) {
	req := &RequestData{
		Model:    "gpt-4o",
		Messages: []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{Str: "Hi", IsString: true}}},
	}
	body, err := BuildBaseRequestBody(req, json.Number("4096"), ReplayThinkTags)
	if err != nil {
		t.Fatal(err)
	}
	if body["max_tokens"] != json.Number("4096") {
		t.Errorf("max_tokens = %v", body["max_tokens"])
	}
}

// TS: omits max_tokens when null.
func TestBuildBaseRequestBodyOmitsNullMaxTokens(t *testing.T) {
	req := &RequestData{
		Model:     "gpt-4o",
		Messages:  []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{Str: "Hi", IsString: true}}},
		MaxTokens: nil,
	}
	body, err := BuildBaseRequestBody(req, nil, ReplayThinkTags)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := body["max_tokens"]; ok {
		t.Errorf("max_tokens must be omitted: %v", body)
	}
}

// TS: skips web_search_20250305 / web_fetch_20250305 type tools.
func TestConvertToolsSkipsServerTools(t *testing.T) {
	tools := []map[string]any{
		{"type": "web_search_20250305", "name": "web_search", "max_uses": 8},
		{"type": "custom", "name": "read_file", "input_schema": map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}}},
	}
	got := ConvertTools(tools)
	if len(got) != 1 {
		t.Fatalf("tools = %d: %v", len(got), got)
	}
	want := map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        "read_file",
			"description": "",
			"parameters":  map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}},
		},
	}
	if !reflect.DeepEqual(got[0], want) {
		t.Errorf("got %v", got[0])
	}

	tools2 := []map[string]any{{"type": "web_fetch_20250305", "name": "web_fetch"}}
	if got := ConvertTools(tools2); len(got) != 0 {
		t.Errorf("web_fetch tools must be filtered: %v", got)
	}

	tools3 := []map[string]any{{"name": "bash", "description": "Run a bash command",
		"input_schema": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}, "required": []string{"command"}}}}
	got3 := ConvertTools(tools3)
	if len(got3) != 1 || got3[0]["function"].(map[string]any)["name"] != "bash" {
		t.Errorf("regular tools must convert normally: %v", got3)
	}
}

// TS: injects server tool function schemas into tools array.
func TestBuildBaseRequestBodyServerToolsInjected(t *testing.T) {
	req := &RequestData{
		Model:       "test-model",
		Messages:    []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{Str: "search the web", IsString: true}}},
		Tools:       []map[string]any{{"type": "web_search_20250305", "name": "web_search", "max_uses": 8}},
		ServerTools: []map[string]any{{"type": "web_search_20250305", "name": "web_search"}},
	}
	body, err := BuildBaseRequestBody(req, json.Number("4096"), ReplayThinkTags)
	if err != nil {
		t.Fatal(err)
	}
	tools := body["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("tools = %d: %v", len(tools), tools)
	}
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
	if !reflect.DeepEqual(tools[0], want) {
		t.Errorf("got %v", tools[0])
	}
}

// TS: injects server tool usage instructions into system prompt.
func TestBuildBaseRequestBodyServerToolSystemPrompt(t *testing.T) {
	req := &RequestData{
		Model:       "test-model",
		Messages:    []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{Str: "search the web", IsString: true}}},
		System:      []any{map[string]any{"type": "text", "text": "You are a helpful assistant."}},
		ServerTools: []map[string]any{{"type": "web_search_20250305", "name": "web_search"}},
	}
	body, err := BuildBaseRequestBody(req, json.Number("4096"), ReplayThinkTags)
	if err != nil {
		t.Fatal(err)
	}
	msgs := body["messages"].([]any)
	var systemMsg map[string]any
	for _, m := range msgs {
		if mm := m.(map[string]any); mm["role"] == "system" {
			systemMsg = mm
		}
	}
	if systemMsg == nil {
		t.Fatalf("no system message: %v", msgs)
	}
	content := systemMsg["content"].(string)
	if !strings.Contains(content, "You are a helpful assistant.") {
		t.Errorf("content = %q", content)
	}
	if !strings.Contains(content, "web_search") {
		t.Errorf("content = %q", content)
	}
}

// TS: combines regular tools with server tool schemas.
func TestBuildBaseRequestBodyCombinesTools(t *testing.T) {
	req := &RequestData{
		Model:    "test-model",
		Messages: []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{Str: "test", IsString: true}}},
		Tools: []map[string]any{
			{"type": "web_search_20250305", "name": "web_search", "max_uses": 8},
			{"name": "bash", "description": "Run command", "input_schema": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}}},
		},
		ServerTools: []map[string]any{{"type": "web_search_20250305", "name": "web_search"}},
	}
	body, err := BuildBaseRequestBody(req, json.Number("4096"), ReplayThinkTags)
	if err != nil {
		t.Fatal(err)
	}
	tools := body["tools"].([]any)
	if len(tools) != 2 {
		t.Errorf("tools = %d: %v", len(tools), tools)
	}
}

// --- estimateInputTokens (core/tokens.ts) ---------------------------------

func TestEstimateTokens(t *testing.T) {
	if got := estimateTokens(""); got != 0 {
		t.Errorf("empty → %d", got)
	}
	if got := estimateTokens("hello"); got != 2 { // ceil(5/4)
		t.Errorf("hello → %d", got)
	}
	if got := estimateTokens("abcdefgh"); got != 2 { // ceil(8/4)
		t.Errorf("abcdefgh → %d", got)
	}
	if got := estimateTokens("a"); got != 1 {
		t.Errorf("a → %d", got)
	}
}

func TestEstimateInputTokens(t *testing.T) {
	msgs := []anthropic.Message{
		{Role: "user", Content: anthropic.ContentValue{Str: "Hello world", IsString: true}},
	}
	if got := EstimateInputTokens(msgs); got <= 0 {
		t.Errorf("string content → %d", got)
	}

	msgs2 := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "text", Text: "Hello world from blocks"},
	}}}}
	if got := EstimateInputTokens(msgs2); got <= 0 {
		t.Errorf("blocks content → %d", got)
	}

	if got := EstimateInputTokens(nil); got < 1 {
		t.Errorf("empty → %d", got)
	}
}

// char/4 per text block + tool input/name + tool_result text sub-blocks;
// +4 per message; max(total, 1).
func TestEstimateInputTokensCounts(t *testing.T) {
	msgs := []anthropic.Message{
		{Role: "user", Content: anthropic.ContentValue{Str: "hello", IsString: true}}, // ceil(5/4)=2 +4
		{Role: "assistant", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "text", Text: "0123456789"},                                        // ceil(10/4)=3
			{Type: "tool_use", ID: "t", Name: "f", Input: json.RawMessage(`{"a":1}`)}, // ceil(7/4)=2 + ceil(1/4)=1
			{Type: "thinking", Thinking: "0123"},                                      // ceil(4/4)=1
		}}}, // +4
		{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "tool_result", ToolUseID: "t", Content: json.RawMessage(`[{"type":"text","text":"1234"}]`)}, // ceil(4/4)=1
		}}}, // +4
	}
	// 6 + (3+2+1+1+4) + (1+4) = 22
	if got := EstimateInputTokens(msgs); got != 22 {
		t.Errorf("got %d", got)
	}
}

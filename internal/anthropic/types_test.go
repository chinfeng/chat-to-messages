package anthropic

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func mustDecode[T any](t *testing.T, data string) T {
	t.Helper()
	var v T
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestContentValueString(t *testing.T) {
	var m Message
	dec := json.NewDecoder(strings.NewReader(`{"role":"user","content":"hello"}`))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		t.Fatal(err)
	}
	s, ok := m.Content.String()
	if !ok || s != "hello" {
		t.Fatalf("content = %q, %v", s, ok)
	}
}

func TestContentValueBlocks(t *testing.T) {
	m := mustDecode[Message](t, `{"role":"assistant","content":[
		{"type":"text","text":"hi"},
		{"type":"tool_use","id":"tu1","name":"read_file","input":{"path":"/x"}}
	]}`)
	blocks, ok := m.Content.Blocks()
	if !ok || len(blocks) != 2 {
		t.Fatalf("blocks = %d, %v", len(blocks), ok)
	}
	if blocks[0].Type != "text" || blocks[0].Text != "hi" {
		t.Errorf("block0 = %+v", blocks[0])
	}
	if blocks[1].Type != "tool_use" || blocks[1].ID != "tu1" || blocks[1].Name != "read_file" {
		t.Errorf("block1 = %+v", blocks[1])
	}
	if v := blocks[1].InputValue(); !reflect.DeepEqual(v, map[string]any{"path": "/x"}) {
		t.Errorf("input = %v", v)
	}
}

func TestContentBlockExtraCapturesUnknown(t *testing.T) {
	b := mustDecode[ContentBlock](t, `{"type":"text","text":"hi","some_custom_attr":{"k":1},"another":true}`)
	if b.Extra["some_custom_attr"] == nil {
		t.Error("custom attr missing from Extra")
	}
	if b.Extra["another"] != true {
		t.Error("another missing")
	}
}

func TestContentBlockToolResult(t *testing.T) {
	b := mustDecode[ContentBlock](t, `{"type":"tool_result","tool_use_id":"tu1","is_error":true,
		"content":[{"type":"text","text":"failed"}]}`)
	if b.ToolUseID != "tu1" || !b.IsError {
		t.Errorf("tool_result fields: %+v", b)
	}
	v := b.ContentValue()
	arr, ok := v.([]any)
	if !ok || len(arr) != 1 {
		t.Fatalf("content = %T %v", v, v)
	}
	first := arr[0].(map[string]any)
	if first["type"] != "text" || first["text"] != "failed" {
		t.Errorf("first = %v", first)
	}
}

func TestContentBlockImageSource(t *testing.T) {
	b := mustDecode[ContentBlock](t, `{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}`)
	if b.Source == nil || b.Source.Type != "base64" || b.Source.MediaType != "image/png" || b.Source.Data != "AAAA" {
		t.Errorf("source = %+v", b.Source)
	}
}

func TestNumberFidelity(t *testing.T) {
	m := mustDecode[Message](t, `{"role":"assistant","content":[{"type":"tool_use","id":"t","name":"f","input":{"big":12345678901234567890,"x":1.5}}]}`)
	blocks, _ := m.Content.Blocks()
	in := blocks[0].InputValue().(map[string]any)
	if _, ok := in["big"].(json.Number); !ok {
		t.Errorf("big must be json.Number, got %T", in["big"])
	}
}

func TestMessagesRequestFull(t *testing.T) {
	req := mustDecode[MessagesRequest](t, `{
		"model":"m1",
		"messages":[{"role":"user","content":"hi"}],
		"system":"sys",
		"max_tokens":4096,
		"temperature":0.7,
		"top_p":1,
		"stop_sequences":["END"],
		"tools":[{"type":"function","name":"f"}],
		"tool_choice":{"type":"auto"},
		"server_tools":[{"type":"web_search_20250305","name":"web_search"}]
	}`)
	if req.Model != "m1" || len(req.Messages) != 1 {
		t.Fatalf("req = %+v", req)
	}
	if req.MaxTokens != json.Number("4096") {
		t.Errorf("max_tokens = %T %v", req.MaxTokens, req.MaxTokens)
	}
	if len(req.Tools) != 1 || req.Tools[0]["name"] != "f" {
		t.Errorf("tools = %v", req.Tools)
	}
	if len(req.ServerTools) != 1 {
		t.Errorf("server_tools = %v", req.ServerTools)
	}
	if req.StopSequences[0] != "END" {
		t.Errorf("stops = %v", req.StopSequences)
	}
	if req.System != "sys" {
		t.Errorf("system = %v", req.System)
	}
}

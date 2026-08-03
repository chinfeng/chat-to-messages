package sse

import (
	"strings"
	"testing"
)

func TestFormatEvent(t *testing.T) {
	got := FormatEvent("ping", map[string]any{})
	want := "event: ping\ndata: {}\n\n"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestMessageStart(t *testing.T) {
	b := NewBuilder("msg_1", "model-x", 100, nil)
	got := b.MessageStart()
	want := "event: message_start\n" +
		`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"model-x","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":100,"output_tokens":1}}}` + "\n\n"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestMessageStartWithCacheUsage(t *testing.T) {
	b := NewBuilder("msg_1", "model-x", 0, &UsageInfo{PromptTokens: 100, CacheReadInputTokens: 30, CacheCreationInputTokens: 10})
	got := b.MessageStart()
	if !strings.Contains(got, `"input_tokens":60`) {
		t.Errorf("input_tokens should be prompt-cache: %s", got)
	}
	if !strings.Contains(got, `"cache_read_input_tokens":30`) {
		t.Errorf("missing cache_read: %s", got)
	}
	if !strings.Contains(got, `"cache_creation_input_tokens":10`) {
		t.Errorf("missing cache_creation: %s", got)
	}
	if !strings.Contains(got, `"output_tokens":1`) {
		t.Errorf("output_tokens: %s", got)
	}
}

func TestMessageDeltaUsage(t *testing.T) {
	b := NewBuilder("msg_1", "m", 10, nil)
	out := int64(42)
	got := b.MessageDelta("end_turn", &out, nil)
	want := "event: message_delta\n" +
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":10,"output_tokens":42}}` + "\n\n"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestMessageDeltaThinkingTokens(t *testing.T) {
	b := NewBuilder("msg_1", "m", 10, nil)
	b.AddThinkingText("abcd") // 4 chars → 1 token
	out := int64(10)
	got := b.MessageDelta("end_turn", &out, nil)
	if !strings.Contains(got, `"thinking_tokens":1`) {
		t.Errorf("missing thinking_tokens: %s", got)
	}
}

func TestThinkingBlockSequence(t *testing.T) {
	b := NewBuilder("msg_1", "m", 10, nil)
	var events []string
	events = append(events, b.EnsureThinkingBlock()...)
	events = append(events, b.ContentBlockDelta(b.NextIndex()-1, "thinking_delta", "let me think"))
	events = append(events, b.CloseContentBlocks()...)
	// [content_block_start thinking, content_block_delta thinking_delta, signature_delta, content_block_stop]
	if len(events) != 4 {
		t.Fatalf("events = %d: %v", len(events), events)
	}
	if !strings.HasPrefix(events[0], "event: content_block_start") || !strings.Contains(events[0], `"content_block":{"type":"thinking","thinking":""}`) {
		t.Errorf("start = %s", events[0])
	}
	if !strings.Contains(events[1], `"delta":{"type":"thinking_delta","thinking":"let me think"}`) {
		t.Errorf("delta = %s", events[1])
	}
	if !strings.HasPrefix(events[2], "event: content_block_delta") || !strings.Contains(events[2], `"signature_delta"`) {
		t.Errorf("sig = %s", events[2])
	}
	if events[3] != "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" {
		t.Errorf("stop = %s", events[3])
	}
}

func TestSignatureDeterministic(t *testing.T) {
	b1 := NewBuilder("msg_same", "m", 0, nil)
	b2 := NewBuilder("msg_same", "m", 0, nil)
	b1.AddThinkingText("think hard")
	b2.AddThinkingText("think hard")
	sig1 := strings.Split(b1.EmitSignatureDelta(), "\n")[1]
	sig2 := strings.Split(b2.EmitSignatureDelta(), "\n")[1]
	if sig1 != sig2 {
		t.Errorf("signatures should match: %s vs %s", sig1, sig2)
	}
	b3 := NewBuilder("msg_diff", "m", 0, nil)
	b3.AddThinkingText("think hard")
	sig3 := strings.Split(b3.EmitSignatureDelta(), "\n")[1]
	if sig3 == sig1 {
		t.Error("different message_id must give different signature")
	}
}

func TestEnsureTextClosesThinking(t *testing.T) {
	b := NewBuilder("msg_1", "m", 0, nil)
	var events []string
	events = append(events, b.EnsureThinkingBlock()...)
	events = append(events, b.EnsureTextBlock()...)
	// thinking start, then (signature_delta + thinking stop) + text start
	if len(events) != 4 {
		t.Fatalf("events = %d", len(events))
	}
	if !strings.Contains(events[1], "signature_delta") {
		t.Errorf("e1 = %s", events[1])
	}
	if !strings.Contains(events[3], `"content_block":{"type":"text"`) {
		t.Errorf("e3 = %s", events[3])
	}
}

func TestToolBlockAndCloseAll(t *testing.T) {
	b := NewBuilder("msg_1", "m", 0, nil)
	var events []string
	events = append(events, b.StartToolBlock(0, "tool_001", "read_file"))
	events = append(events, b.EmitToolDelta(0, `{"path":`))
	events = append(events, b.CloseAllBlocks()...)
	if len(events) != 3 {
		t.Fatalf("events = %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], `"content_block":{"type":"tool_use","id":"tool_001","name":"read_file","input":{}`) {
		t.Errorf("start = %s", events[0])
	}
	if !strings.Contains(events[1], `"delta":{"type":"input_json_delta","partial_json":"{\"path\":"`) {
		t.Errorf("delta = %s", events[1])
	}
}

func TestEstimateOutputTokens(t *testing.T) {
	b := NewBuilder("msg_1", "m", 0, nil)
	b.AddThinkingText(strings.Repeat("a", 8)) // 2 tokens
	b.TextDelta(strings.Repeat("b", 16))      // 4 tokens
	if got := b.EstimateOutputTokens(); got != 2+4+2*4 {
		t.Errorf("estimate = %d", got)
	}
}

func TestWebToolResultBlocks(t *testing.T) {
	b := NewBuilder("msg_1", "m", 0, nil)
	ev := b.EmitWebSearchToolResult("tu1", []map[string]any{{"type": "web_search_result", "url": "https://x.com", "title": "X"}}, "")
	if len(ev) != 2 {
		t.Fatalf("events = %d", len(ev))
	}
	// Note: the content item is a caller-provided map[string]any, and Go's
	// encoding/json deterministically sorts map keys — so the item encodes as
	// {"title":...,"type":...,"url":...}, where the TS original preserved the
	// literal insertion order ({"type":...,"url":...,"title":...}). The
	// content_block wrapper itself (type, tool_use_id, content) keeps TS order.
	if !strings.Contains(ev[0], `"content_block":{"type":"web_search_tool_result","tool_use_id":"tu1","content":[{"title":"X","type":"web_search_result","url":"https://x.com"}]}`) {
		t.Errorf("start = %s", ev[0])
	}
}

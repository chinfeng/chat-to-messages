// AggregateMessage reassembles the builder's SSE event strings into one
// complete Anthropic Message — the response body for non-streaming
// /v1/messages clients (stream omitted or false). It consumes exactly the
// events the stream package emits, so the aggregation shares the stream
// path's behavior (guards, retries, repaired tool args) instead of
// duplicating it.
package sse

import (
	"encoding/json"
	"strings"
)

// msgBlock is one in-progress content block keyed by block index.
type msgBlock struct {
	typ       string
	text      string // text_delta / thinking_delta accumulation
	signature string // thinking signature_delta
	id, name  string // tool_use
	argBuf    string // input_json_delta accumulation, parsed at assembly
}

// AggregateMessage builds the Message JSON from emitted SSE events. The
// returned map marshals to the Anthropic non-streaming Message shape.
func AggregateMessage(events []string) map[string]any {
	var (
		id, model    string
		blocks       []*msgBlock
		stopReason   any
		stopSequence any
		usage        map[string]any
	)
	block := func(index int) *msgBlock {
		for len(blocks) <= index {
			blocks = append(blocks, &msgBlock{})
		}
		return blocks[index]
	}

	for _, ev := range events {
		name, data, ok := parseEvent(ev)
		if !ok {
			continue
		}
		switch name {
		case "message_start":
			msg, _ := data["message"].(map[string]any)
			if msg == nil {
				continue
			}
			id, _ = msg["id"].(string)
			model, _ = msg["model"].(string)
			if u, ok := msg["usage"].(map[string]any); ok {
				usage = u
			}
		case "content_block_start":
			idx := jsonInt(data["index"])
			cb, _ := data["content_block"].(map[string]any)
			if cb == nil {
				continue
			}
			b := block(idx)
			b.typ, _ = cb["type"].(string)
			b.id, _ = cb["id"].(string)
			b.name, _ = cb["name"].(string)
		case "content_block_delta":
			idx := jsonInt(data["index"])
			delta, _ := data["delta"].(map[string]any)
			if delta == nil {
				continue
			}
			b := block(idx)
			switch delta["type"] {
			case "text_delta":
				s, _ := delta["text"].(string)
				b.text += s
			case "thinking_delta":
				s, _ := delta["thinking"].(string)
				b.text += s
			case "signature_delta":
				b.signature, _ = delta["signature"].(string)
			case "input_json_delta":
				// Emitted deltas always concatenate (Task-tool args are held
				// upstream until complete, never re-emitted after partials),
				// so a plain accumulation parses once at assembly time.
				s, _ := delta["partial_json"].(string)
				b.argBuf += s
			}
		case "message_delta":
			if delta, ok := data["delta"].(map[string]any); ok {
				stopReason = delta["stop_reason"]
				if s, ok := delta["stop_sequence"]; ok {
					stopSequence = s
				}
			}
			if u, ok := data["usage"].(map[string]any); ok {
				usage = u
			}
		}
	}

	content := make([]any, 0, len(blocks))
	for _, b := range blocks {
		switch b.typ {
		case "text":
			content = append(content, textBlock{Type: "text", Text: b.text})
		case "thinking":
			content = append(content, thinkingBlock{Type: "thinking", Thinking: b.text, Signature: b.signature})
		case "tool_use", "server_tool_use":
			var input any = map[string]any{}
			if b.argBuf != "" {
				if err := json.Unmarshal([]byte(b.argBuf), &input); err != nil {
					input = map[string]any{}
				}
			}
			content = append(content, toolUseBlock{Type: "tool_use", ID: b.id, Name: b.name, Input: input})
		}
	}

	if usage == nil {
		usage = map[string]any{"input_tokens": 0, "output_tokens": 0}
	}
	return map[string]any{
		"id":            id,
		"type":          "message",
		"role":          "assistant",
		"model":         model,
		"content":       content,
		"stop_reason":   stopReason,
		"stop_sequence": stopSequence,
		"usage":         usage,
	}
}

// parseEvent splits one "event: NAME\ndata: JSON" payload (data may span
// lines; JSON itself has no raw newlines after encoding, so any data-line
// joining is a straight concat).
func parseEvent(ev string) (name string, data map[string]any, ok bool) {
	var dataBuf strings.Builder
	for _, line := range strings.Split(ev, "\n") {
		switch {
		case strings.HasPrefix(line, "event: "):
			name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			dataBuf.WriteString(strings.TrimPrefix(line, "data: "))
		}
	}
	if name == "" || dataBuf.Len() == 0 {
		return "", nil, false
	}
	if err := json.Unmarshal([]byte(dataBuf.String()), &data); err != nil {
		return "", nil, false
	}
	return name, data, true
}

// jsonInt extracts a numeric field as int (UseNumber or float64 decoded).
func jsonInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

// ---- Output shapes (Anthropic non-streaming Message) ----

type textBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type thinkingBlock struct {
	Type      string `json:"type"`
	Thinking  string `json:"thinking"`
	Signature string `json:"signature,omitempty"` // absent when no signature_delta was emitted (block closed by a switch)
}

type toolUseBlock struct {
	Type  string `json:"type"`
	ID    string `json:"id"`
	Name  string `json:"name"`
	Input any    `json:"input"`
}

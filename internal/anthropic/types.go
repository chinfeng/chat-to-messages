// Package anthropic provides typed protocol structures for the Anthropic
// Messages API, ported from chat-to-claude-code's src/protocol/anthropic.ts.
// All JSON decoding uses json.Number for round-trip fidelity.
package anthropic

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// ContentBlock is a tagged union of Anthropic content block types held in a
// single struct. Known keys map to named fields; unknown keys are kept
// verbatim (UseNumber-decoded) in Extra so they can round-trip.
type ContentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	Thinking  string          `json:"thinking,omitempty"`
	Signature string          `json:"signature,omitempty"`
	Data      string          `json:"data,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	Content   json.RawMessage `json:"content,omitempty"` // tool_result 内容
	IsError   bool            `json:"is_error,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Status    string          `json:"status,omitempty"`
	Source    *Source         `json:"source,omitempty"`
	Citations json.RawMessage `json:"citations,omitempty"`
	Detail    string          `json:"detail,omitempty"`
	Title     string          `json:"title,omitempty"`
	Context   string          `json:"context,omitempty"`
	Extra     map[string]any  `json:"-"` // UnmarshalJSON 捕获未知字段
}

// Source describes an image or document source block.
type Source struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type,omitempty"`
	MimeType  string `json:"mime_type,omitempty"`
	Data      string `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
}

// UnmarshalJSON decodes a content block with json.Number fidelity, mapping
// known keys to named fields and keeping every other key in Extra.
func (b *ContentBlock) UnmarshalJSON(data []byte) error {
	raw, err := decodeToMap(data)
	if err != nil {
		return err
	}
	var out ContentBlock
	for k, v := range raw {
		switch k {
		case "type":
			out.Type, _ = v.(string)
		case "text":
			out.Text, _ = v.(string)
		case "thinking":
			out.Thinking, _ = v.(string)
		case "signature":
			out.Signature, _ = v.(string)
		case "data":
			out.Data, _ = v.(string)
		case "id":
			out.ID, _ = v.(string)
		case "name":
			out.Name, _ = v.(string)
		case "input":
			out.Input, _ = rawMessage(v)
		case "content":
			out.Content, _ = rawMessage(v)
		case "is_error":
			out.IsError, _ = v.(bool)
		case "tool_use_id":
			out.ToolUseID, _ = v.(string)
		case "status":
			out.Status, _ = v.(string)
		case "source":
			out.Source, _ = decodeSource(v)
		case "citations":
			out.Citations, _ = rawMessage(v)
		case "detail":
			out.Detail, _ = v.(string)
		case "title":
			out.Title, _ = v.(string)
		case "context":
			out.Context, _ = v.(string)
		default:
			if out.Extra == nil {
				out.Extra = make(map[string]any, 1)
			}
			out.Extra[k] = v
		}
	}
	*b = out
	return nil
}

// InputValue decodes Input with json.Number fidelity, returning an empty map
// when Input is absent or not decodable.
func (b *ContentBlock) InputValue() any {
	if len(b.Input) == 0 {
		return map[string]any{}
	}
	var v any
	if err := decodeUseNumber(b.Input, &v); err != nil {
		return map[string]any{}
	}
	return v
}

// ContentValue decodes the tool_result content list with json.Number
// fidelity, returning nil when Content is absent.
func (b *ContentBlock) ContentValue() any {
	if len(b.Content) == 0 {
		return nil
	}
	var v any
	if err := decodeUseNumber(b.Content, &v); err != nil {
		return nil
	}
	return v
}

// ContentValue is the content field of a message: a string or a list of
// content blocks.
type ContentValue struct {
	IsString  bool
	Str       string
	BlocksVal []ContentBlock
}

// UnmarshalJSON dispatches on the first byte: `"` → string, `[` → blocks,
// anything else is an error.
func (v *ContentValue) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("anthropic: empty content value")
	}
	switch data[0] {
	case '"':
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		*v = ContentValue{IsString: true, Str: s}
		return nil
	case '[':
		var blocks []ContentBlock
		if err := decodeUseNumber(data, &blocks); err != nil {
			return err
		}
		*v = ContentValue{BlocksVal: blocks}
		return nil
	}
	return fmt.Errorf("anthropic: content must be a string or array, got %q", peek(data))
}

// String returns the string content when this value holds a string.
func (v ContentValue) String() (string, bool) {
	return v.Str, v.IsString
}

// Blocks returns the block list when this value holds a block array.
func (v ContentValue) Blocks() ([]ContentBlock, bool) {
	return v.BlocksVal, !v.IsString
}

// Message is a single conversation message.
type Message struct {
	Role             string
	Content          ContentValue
	ReasoningContent *string
}

// UnmarshalJSON decodes role, content (string | block list) and the optional
// OpenAI-style reasoning_content.
func (m *Message) UnmarshalJSON(data []byte) error {
	raw, err := decodeToMap(data)
	if err != nil {
		return err
	}
	var out Message
	out.Role, _ = raw["role"].(string)
	if v, ok := raw["content"]; ok {
		cd, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if err := out.Content.UnmarshalJSON(cd); err != nil {
			return err
		}
	}
	if v, ok := raw["reasoning_content"].(string); ok {
		out.ReasoningContent = &v
	}
	*m = out
	return nil
}

// MessagesRequest is the /v1/messages request body. Numbers are kept as
// json.Number for round-trip fidelity; fields absent from the body stay
// zero-valued.
type MessagesRequest struct {
	Model         string
	Messages      []Message
	System        any
	MaxTokens     any
	Temperature   any
	TopP          any
	StopSequences []string
	Tools         []map[string]any
	ToolChoice    any
	ServerTools   []map[string]any
}

// UnmarshalJSON decodes the whole request with UseNumber, extracts the typed
// fields and passes everything else through as-is.
func (r *MessagesRequest) UnmarshalJSON(data []byte) error {
	raw, err := decodeToMap(data)
	if err != nil {
		return err
	}
	var out MessagesRequest
	out.Model, _ = raw["model"].(string)
	out.System = raw["system"]
	out.MaxTokens = raw["max_tokens"]
	out.Temperature = raw["temperature"]
	out.TopP = raw["top_p"]
	out.ToolChoice = raw["tool_choice"]
	out.Messages, err = decodeMessages(raw["messages"])
	if err != nil {
		return err
	}
	out.StopSequences = toStringSlice(raw["stop_sequences"])
	out.Tools = toAnyMapSlice(raw["tools"])
	out.ServerTools = toAnyMapSlice(raw["server_tools"])
	*r = out
	return nil
}

// decodeToMap decodes JSON with UseNumber into a map.
func decodeToMap(data []byte) (map[string]any, error) {
	var m map[string]any
	if err := decodeUseNumber(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// decodeUseNumber decodes data into v with json.Number fidelity.
func decodeUseNumber(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(v)
}

// rawMessage re-encodes a decoded value into a RawMessage, preserving
// json.Number literals verbatim.
func rawMessage(v any) (json.RawMessage, error) {
	return json.Marshal(v)
}

// decodeSource re-marshals a decoded source value into a Source.
func decodeSource(v any) (*Source, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var s Source
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// decodeMessages converts a decoded messages array into []Message by
// re-marshaling each element and reusing Message.UnmarshalJSON.
func decodeMessages(v any) ([]Message, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, nil
	}
	out := make([]Message, 0, len(arr))
	for _, el := range arr {
		data, err := json.Marshal(el)
		if err != nil {
			return nil, err
		}
		var m Message
		if err := m.UnmarshalJSON(data); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// toStringSlice converts a decoded []any of strings into []string.
func toStringSlice(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, el := range arr {
		if s, ok := el.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// toAnyMapSlice converts a decoded []any of objects into []map[string]any.
func toAnyMapSlice(v any) []map[string]any {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(arr))
	for _, el := range arr {
		if m, ok := el.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// peek returns a short preview of data for error messages.
func peek(data []byte) string {
	if len(data) > 32 {
		return string(data[:32]) + "..."
	}
	return string(data)
}

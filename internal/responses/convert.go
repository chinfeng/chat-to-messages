package responses

import (
	"strings"

	"github.com/chinfeng/chat-to-messages/internal/convert"
)

// BuildChatBody converts a validated Responses request plus history items
// into the upstream chat/completions body (stream/stream_options are added by
// the route layer). history must already be the full logical input in wire
// order: store-expanded previous_response_id items FOLLOWED by the current
// input's items (req.Input.Items); a string input is appended last by this
// function.
//
// Request-level rejections (400): background=true, any non-function tool
// (hosted tools have no chat/completions counterpart — ADR-0001),
// input_audio parts, file_id-only media, bare item references.
func BuildChatBody(req *Request, history []Item, replayMode convert.ReasoningReplayMode) (map[string]any, error) {
	if req.Background {
		return nil, invalidRequest("background", "background mode is not supported by chat/completions upstreams")
	}
	if err := checkTools(req.Tools); err != nil {
		return nil, err
	}

	var messages []map[string]any
	if req.Instructions != nil && *req.Instructions != "" {
		messages = append(messages, map[string]any{"role": "system", "content": *req.Instructions})
	}

	// Responses items model an assistant turn as (reasoning?, message?,
	// function_call*) — chat merges those into ONE assistant message with
	// content + tool_calls. pending* buffers accumulate until a boundary
	// (user/system/tool-result item or end) flushes them.
	var pendingText []string
	var pendingCalls []map[string]any
	var pendingReasoning []string
	flush := func() {
		if len(pendingText) == 0 && len(pendingCalls) == 0 && len(pendingReasoning) == 0 {
			return
		}
		content := strings.Join(pendingText, "\n\n")
		msg := map[string]any{"role": "assistant"}
		switch replayMode {
		case convert.ReplayThinkTags:
			if r := strings.Join(pendingReasoning, "\n"); r != "" {
				if content != "" {
					content = "<think>\n" + r + "\n</think>\n\n" + content
				} else {
					content = "<think>\n" + r + "\n</think>"
				}
			}
		case convert.ReplayReasoningContent:
			if r := strings.Join(pendingReasoning, "\n"); r != "" {
				msg["reasoning_content"] = r
			}
		}
		// ReplayDisabled (or a mode with no reasoning buffered): drop.
		if content == "" && len(pendingCalls) == 0 && len(pendingReasoning) == 0 {
			content = " "
		}
		msg["content"] = content
		if len(pendingCalls) > 0 {
			msg["tool_calls"] = toAnySlice(pendingCalls)
		}
		messages = append(messages, msg)
		pendingText, pendingCalls, pendingReasoning = nil, nil, nil
	}

	for i := range history {
		it := &history[i]
		switch it.Type {
		case "message":
			if it.Role == "user" || it.Role == "" || it.Role == "developer" || it.Role == "system" {
				flush()
				msg, err := convertInboundMessage(it)
				if err != nil {
					return nil, err
				}
				messages = append(messages, msg)
			} else { // assistant
				for _, p := range assistantTextParts(it) {
					if p != "" {
						pendingText = append(pendingText, p)
					}
				}
			}
		case "function_call":
			callID := it.CallID
			if callID == "" {
				callID = newID("call_")
			}
			pendingCalls = append(pendingCalls, map[string]any{
				"id":   callID,
				"type": "function",
				"function": map[string]any{
					"name":      it.Name,
					"arguments": it.Arguments,
				},
			})
		case "function_call_output":
			flush()
			messages = append(messages, map[string]any{
				"role":         "tool",
				"tool_call_id": it.CallID,
				"content":      toolOutputText(it.Output),
			})
		case "reasoning":
			if text := reasoningText(it); text != "" {
				pendingReasoning = append(pendingReasoning, text)
			}
		case "":
			if it.ID != "" {
				return nil, invalidRequest("input", "item references ({id}) require previous_response_id expansion, which this proxy only resolves for ids it stores; resend the full input")
			}
			// Unknown bare object — skip (tolerant).
		default:
			// Hosted-tool call/result items from a foreign history cannot be
			// reconstructed against a chat upstream; skip rather than corrupt.
		}
	}
	flush()

	// String shorthand input is the NEWEST turn: it trails the item history
	// (with the pending assistant buffers flushed ahead of it).
	if req.Input.IsString {
		if t := strings.TrimSpace(req.Input.Str); t != "" {
			messages = append(messages, map[string]any{"role": "user", "content": req.Input.Str})
		}
	}

	if len(messages) == 0 {
		return nil, invalidRequest("input", "input is required (provide a string or at least one item)")
	}

	body := map[string]any{"model": req.Model, "messages": messages}
	if req.MaxOutputTokens != nil {
		// Modern chat field; upstreams that only know max_tokens can be
		// patched per-model via --upstream-extra-params.
		body["max_completion_tokens"] = req.MaxOutputTokens
	}
	if req.Temperature != nil {
		body["temperature"] = req.Temperature
	}
	if req.TopP != nil {
		body["top_p"] = req.TopP
	}
	if len(req.Tools) > 0 {
		tools := make([]map[string]any, 0, len(req.Tools))
		for _, t := range req.Tools {
			fn := map[string]any{"name": t.Name, "description": t.Description}
			if t.Parameters != nil {
				fn["parameters"] = t.Parameters
			} else {
				fn["parameters"] = map[string]any{"type": "object", "properties": map[string]any{}}
			}
			if t.Strict != nil {
				fn["strict"] = *t.Strict
			}
			tools = append(tools, map[string]any{"type": "function", "function": fn})
		}
		body["tools"] = toAnySlice(tools)
	}
	if req.ToolChoice != nil {
		body["tool_choice"] = convertToolChoice(req.ToolChoice)
	}
	if req.ParallelToolCalls != nil {
		body["parallel_tool_calls"] = *req.ParallelToolCalls
	}
	if req.Text != nil {
		if rf := convertTextFormat(&req.Text.Format); rf != nil {
			body["response_format"] = rf
		}
		if req.Text.Verbosity != "" {
			body["verbosity"] = req.Text.Verbosity
		}
	}
	if req.Reasoning != nil && req.Reasoning.Effort != "" {
		body["reasoning_effort"] = req.Reasoning.Effort
	}
	if u := req.User; u == "" {
		u = req.SafetyIdentifier
		if u != "" {
			body["user"] = u
		}
	} else {
		body["user"] = u
	}
	if req.Metadata != nil {
		body["metadata"] = req.Metadata
	}
	return body, nil
}

// checkTools rejects non-function tools: web_search / file_search /
// computer_use / code_interpreter / image_generation / mcp / custom etc. are
// OpenAI-server-side capabilities with no chat/completions equivalent
// (ADR-0001: fail fast, name the offending types).
func checkTools(tools []Tool) error {
	var bad []string
	seen := map[string]bool{}
	for _, t := range tools {
		if t.Type == "function" {
			continue
		}
		if !seen[t.Type] {
			seen[t.Type] = true
			bad = append(bad, t.Type)
		}
	}
	if len(bad) > 0 {
		return invalidRequest("tools", "unsupported tool type(s) for chat/completions upstreams: "+strings.Join(bad, ", "))
	}
	return nil
}

// convertInboundMessage converts a user/system/developer message item into a
// chat message. developer maps to system (broad upstream support).
func convertInboundMessage(it *Item) (map[string]any, error) {
	role := it.Role
	if role == "" || role == "developer" {
		role = map[string]string{"": "user", "developer": "system"}[role]
	}
	cv := it.Content
	if cv.IsString {
		return map[string]any{"role": role, "content": cv.Str}, nil
	}
	var parts []any
	var textOnly []string
	for _, p := range cv.Parts {
		switch p.Type {
		case "input_text", "output_text", "text":
			textOnly = append(textOnly, p.Text)
			parts = append(parts, map[string]any{"type": "text", "text": p.Text})
		case "input_image":
			if p.ImageURL == "" {
				if p.FileID != "" {
					return nil, invalidRequest("input", "input_image by file_id is not supported (no file store to fetch from); pass image_url / a data URL instead")
				}
				continue
			}
			img := map[string]any{"url": p.ImageURL}
			if p.Detail != "" {
				img["detail"] = p.Detail
			}
			parts = append(parts, map[string]any{"type": "image_url", "image_url": img})
		case "input_file":
			if p.FileData == "" {
				if p.FileID != "" {
					return nil, invalidRequest("input", "input_file by file_id is not supported (no file store to fetch from); pass file_data instead")
				}
				continue
			}
			file := map[string]any{}
			if p.Filename != "" {
				file["filename"] = p.Filename
			}
			file["file_data"] = p.FileData
			parts = append(parts, map[string]any{"type": "file", "file": file})
		case "input_audio":
			return nil, invalidRequest("input", "input_audio is not supported by chat/completions upstreams")
		default:
			// Unknown part — skip (tolerant).
		}
	}
	if len(parts) == len(textOnly) {
		return map[string]any{"role": role, "content": strings.Join(textOnly, "\n")}, nil
	}
	return map[string]any{"role": role, "content": parts}, nil
}

// assistantTextParts extracts visible text from an assistant message item
// (output_text + refusal parts, or a bare string content).
func assistantTextParts(it *Item) []string {
	if it.Content.IsString {
		return []string{it.Content.Str}
	}
	var out []string
	for _, p := range it.Content.Parts {
		switch p.Type {
		case "output_text", "text":
			out = append(out, p.Text)
		case "refusal":
			out = append(out, p.Refusal)
		}
	}
	return out
}

// toolOutputText renders function_call_output content as chat tool-message
// content: a string passes verbatim; parts join their text, non-text parts
// degrade to a marker.
func toolOutputText(cv ContentValue) string {
	if cv.IsString {
		return cv.Str
	}
	var parts []string
	for _, p := range cv.Parts {
		switch p.Type {
		case "input_text", "output_text", "text":
			parts = append(parts, p.Text)
		default:
			parts = append(parts, "["+p.Type+"]")
		}
	}
	return strings.Join(parts, "\n")
}

// reasoningText extracts replayable text from a reasoning item: summary text
// first, then our fabricated encrypted_content.
func reasoningText(it *Item) string {
	var parts []string
	for _, s := range it.Summary {
		if s.Text != "" {
			parts = append(parts, s.Text)
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, "\n")
	}
	if it.EncryptedContent != "" {
		if text, ok := DecryptReasoning(it.EncryptedContent); ok {
			return text
		}
	}
	return ""
}

// convertToolChoice maps Responses tool_choice onto the chat form.
func convertToolChoice(tc any) any {
	if m, ok := tc.(map[string]any); ok {
		if m["type"] == "function" {
			name, _ := m["name"].(string)
			return map[string]any{"type": "function", "function": map[string]any{"name": name}}
		}
	}
	return tc
}

// convertTextFormat maps text.format onto chat response_format.
func convertTextFormat(f *TextFormat) any {
	switch f.Type {
	case "json_object":
		return map[string]any{"type": "json_object"}
	case "json_schema":
		js := map[string]any{"name": f.Name}
		if f.Description != "" {
			js["description"] = f.Description
		}
		if f.Schema != nil {
			js["schema"] = f.Schema
		}
		if f.Strict != nil {
			js["strict"] = *f.Strict
		}
		return map[string]any{"type": "json_schema", "json_schema": js}
	default:
		return nil // "text": nothing to send
	}
}

// toAnySlice mirrors the convert package helper convention.
func toAnySlice(msgs []map[string]any) []any {
	out := make([]any, len(msgs))
	for i, m := range msgs {
		out[i] = m
	}
	return out
}

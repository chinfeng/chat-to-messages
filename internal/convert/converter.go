// Anthropic Messages API → OpenAI Chat Completions API conversion, ported
// from ../chat-to-claude-code/src/conversion/converter.ts.
package convert

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/chinfeng/chat-to-messages/internal/anthropic"
	"github.com/chinfeng/chat-to-messages/internal/servertool"
)

// OpenAIConversionError mirrors the TS error class of the same name.
type OpenAIConversionError struct {
	Message string
}

func (e *OpenAIConversionError) Error() string { return e.Message }

// ReasoningReplayMode mirrors the TS enum ReasoningReplayMode.
type ReasoningReplayMode string

const (
	ReplayDisabled         ReasoningReplayMode = "disabled"
	ReplayThinkTags        ReasoningReplayMode = "think_tags"
	ReplayReasoningContent ReasoningReplayMode = "reasoning_content"
)

// thinkTagContent mirrors thinkTagContent() in converter.ts.
func thinkTagContent(reasoning string) string {
	return "<think>\n" + reasoning + "\n</think>"
}

// sigMarkerRe matches the proxy-fabricated thinking-signature marker. These
// markers must never reach the upstream model: kimi-k3 imitates them when they
// appear in its own past turns (dumped 2026-08-26 — it emitted fresh
// <!--sig:<hex64>--> comments as literal text, then ended the turn with no
// tool call, breaking the agent loop).
var sigMarkerRe = regexp.MustCompile(`<!--sig:[0-9a-f]{64}-->`)

// interruptMarkerRe matches the two literal phrases Claude Code renders when a
// tool call is interrupted — `[Tool use interrupted]` (the self-contained
// assistant text turn it records on interrupt) and `No result from invoke.`
// (its mid-call tail). If these are replayed upstream they teach the model to
// emit them verbatim as its whole reply before end_turn with no tool call (the
// same imitation failure as the sig markers). They are unambiguous client-side
// rendering and never legitimate model content, so they are stripped as whole
// phrases.
//
// NOTE: do NOT add `</parameter>` / `</invoke>` (or any `<parameter>` XML) to
// this list — GLM-family upstreams use those tags as their NATIVE tool-call
// protocol (see servertool/tools.go glmToolCallRE), and stripping them here
// corrupts the arguments the model streams back to Claude Code.
var interruptMarkerRe = regexp.MustCompile(`\[Tool use interrupted\]|No result from invoke\.`)

// scrubSigMarkers removes signature markers from assistant text replayed
// upstream (both legacy injected ones and model-mimicked echoes).
func scrubSigMarkers(text string) string {
	return sigMarkerRe.ReplaceAllString(text, "")
}

// scrubClientControlMarkers removes Claude Code UI control markers (thinking
// signatures and tool-use interruption) from assistant text replayed upstream.
// The proxy re-submits prior assistant turns as model context; any client
// rendering marker in that context is imitated by GLM-family models and
// truncates the agent loop, so none may reach the upstream model.
func scrubClientControlMarkers(text string) string {
	if text == "" {
		return text
	}
	text = scrubSigMarkers(text)
	return interruptMarkerRe.ReplaceAllString(text, "")
}

// toolInputSchema mirrors toolInputSchema() in converter.ts.
func toolInputSchema(tool map[string]any) map[string]any {
	if s, ok := tool["input_schema"].(map[string]any); ok {
		return s
	}
	return map[string]any{"type": "object", "properties": map[string]any{}}
}

// stripTrailingNewline removes the leading segment through (and including)
// the line terminator at fromIndex. For "\r\n" the terminator is two chars;
// for "\r" or "\n" it is one char. Returns everything AFTER the terminator.
func stripTrailingNewline(text string, fromIndex int) string {
	if fromIndex+1 < len(text) && text[fromIndex] == '\r' && text[fromIndex+1] == '\n' {
		return text[fromIndex+2:]
	}
	return text[fromIndex+1:]
}

// newlineIndex returns the index of the first '\r' or '\n' in text, or -1,
// matching TS text.search(/\r\n|\r|\n/).
func newlineIndex(text string) int {
	for i := 0; i < len(text); i++ {
		if text[i] == '\r' || text[i] == '\n' {
			return i
		}
	}
	return -1
}

// stripLeadingAnthropicBillingHeader mirrors stripLeadingAnthropicBillingHeader():
// strip a leading `x-anthropic-billing-header:` line (with its single trailing
// terminator) from a system prompt text block so the prefix is stable for
// upstream prefix-cache reuse. Only the FIRST line, only at offset 0. Returns
// "" if the header line IS the entire string.
func stripLeadingAnthropicBillingHeader(text string) string {
	if !strings.HasPrefix(text, "x-anthropic-billing-header:") {
		return text
	}
	nl := newlineIndex(text)
	if nl == -1 {
		return "" // header line is the entire string
	}
	return stripTrailingNewline(text, nl)
}

// toolResultMediaMarker mirrors TOOL_RESULT_MEDIA_MARKER: OpenAI tool
// messages can only carry text, so media blocks inside a tool_result are
// extracted and re-emitted as a synthetic user turn.
const toolResultMediaMarker = "[tool result media moved to the following user message]"

// toolResultSerialization mirrors the ToolResultSerialization interface.
type toolResultSerialization struct {
	text   string
	images []map[string]any
}

// serializeToolResultContent mirrors serializeToolResultContent() in
// converter.ts. Go maps have no insertion order, so the object branch emits
// json.Marshal's sorted-key serialization.
func serializeToolResultContent(toolContent any) toolResultSerialization {
	if toolContent == nil {
		return toolResultSerialization{text: ""}
	}
	switch t := toolContent.(type) {
	case string:
		return toolResultSerialization{text: t}
	case map[string]any:
		return toolResultSerialization{text: jsonStringify(t)}
	case []any:
		var textParts []string
		var images []map[string]any
		for _, item := range t {
			if m, ok := item.(map[string]any); ok {
				switch m["type"] {
				case "text":
					textParts = append(textParts, attrString(m, "text"))
				case "image":
					if part := buildImagePartFromMap(m); part != nil {
						images = append(images, part)
					}
				default:
					// Structured blocks (web_search_result, etc.) — textified.
					textParts = append(textParts, jsonStringify(m))
				}
			} else {
				textParts = append(textParts, tsString(item))
			}
		}
		return toolResultSerialization{text: strings.Join(textParts, "\n"), images: images}
	default:
		return toolResultSerialization{text: tsString(toolContent)}
	}
}

// cleanReasoningContent mirrors cleanReasoningContent(): null for absent or
// empty reasoning_content, otherwise the string.
func cleanReasoningContent(value *string) (string, bool) {
	if value == nil || *value == "" {
		return "", false
	}
	return *value, true
}

// pendingAfterTools mirrors the PendingAfterTools interface.
type pendingAfterTools struct {
	remainingToolIds     map[string]bool
	deferredBlocks       []anthropic.ContentBlock
	topLevelReasoning    string
	hasTopLevelReasoning bool
	reasoningReplay      ReasoningReplayMode
	deferredEmitted      bool
}

// needsDeferred mirrors needsDeferred() in converter.ts.
func needsDeferred(p *pendingAfterTools) bool {
	return len(p.deferredBlocks) > 0 && !p.deferredEmitted
}

// assertNoForbiddenAssistantBlock mirrors assertNoForbiddenAssistantBlock():
// a text placeholder for tolerated-but-unsupported blocks (images →
// "[Image]"), or "" for blocks safe to skip silently (server_tool_use, etc.).
func assertNoForbiddenAssistantBlock(blockType string) string {
	if blockType == "image" {
		// OpenAI Chat does not support image blocks in assistant messages.
		return "[Image]"
	}
	return ""
}

// isToolResultBlockType mirrors isToolResultBlockType() in converter.ts.
func isToolResultBlockType(blockType string) bool {
	return blockType == "tool_result" ||
		blockType == "web_search_tool_result" ||
		blockType == "web_fetch_tool_result"
}

// indexFirstToolUse mirrors indexFirstToolUse(): the index of the first
// tool_use block, or -1.
func indexFirstToolUse(blocks []anthropic.ContentBlock) int {
	for i := range blocks {
		if blocks[i].Type == "tool_use" {
			return i
		}
	}
	return -1
}

// deferredPostToolBlocks mirrors deferredPostToolBlocks(): blocks after the
// first tool use, excluding further tool_use blocks.
func deferredPostToolBlocks(content []anthropic.ContentBlock, firstToolIndex int) []anthropic.ContentBlock {
	var out []anthropic.ContentBlock
	for i, b := range content {
		if i > firstToolIndex && b.Type != "tool_use" {
			out = append(out, b)
		}
	}
	return out
}

// toolCallFromBlock builds one OpenAI tool_calls entry from a tool_use block.
// Arguments use canonical (sorted-keys) serialization so the same tool
// invocation produces identical wire bytes regardless of input key order.
func toolCallFromBlock(block *anthropic.ContentBlock) map[string]any {
	args := toolInputArguments(block.InputValue())
	return map[string]any{
		"id":   block.ID,
		"type": "function",
		"function": map[string]any{
			"name":      block.Name,
			"arguments": args,
		},
	}
}

// iterToolUsesInOrder mirrors iterToolUsesInOrder() in converter.ts.
func iterToolUsesInOrder(blocks []anthropic.ContentBlock) []map[string]any {
	var toolCalls []map[string]any
	for i := range blocks {
		if blocks[i].Type != "tool_use" {
			continue
		}
		toolCalls = append(toolCalls, toolCallFromBlock(&blocks[i]))
	}
	return toolCalls
}

// convertAssistantMessage mirrors _convertAssistantMessage() in converter.ts.
func convertAssistantMessage(content []anthropic.ContentBlock, reasoningContent string, hasReasoning bool, reasoningReplay ReasoningReplayMode) []map[string]any {
	var contentParts []string
	var thinkingParts []string
	var toolCalls []map[string]any

	for i := range content {
		block := &content[i]
		switch block.Type {
		case "text":
			contentParts = append(contentParts, scrubClientControlMarkers(block.Text))
		case "thinking":
			if reasoningReplay == ReplayDisabled {
				continue
			}
			// The fabricated signature is proxy-internal state; injecting it
			// upstream teaches the model to emit <!--sig:...--> itself.
			if reasoningReplay == ReplayThinkTags {
				contentParts = append(contentParts, thinkTagContent(block.Thinking))
			} else if !hasReasoning {
				thinkingParts = append(thinkingParts, block.Thinking)
			}
		case "redacted_thinking":
			if reasoningReplay == ReplayDisabled {
				continue
			}
			if reasoningReplay == ReplayThinkTags {
				contentParts = append(contentParts, "[redacted thinking]")
			} else if !hasReasoning {
				thinkingParts = append(thinkingParts, "[redacted thinking]")
			}
		case "tool_use":
			toolCalls = append(toolCalls, toolCallFromBlock(block))
		default:
			if placeholder := assertNoForbiddenAssistantBlock(block.Type); placeholder != "" {
				contentParts = append(contentParts, placeholder)
			}
		}
	}

	contentStr := strings.Join(contentParts, "\n\n")
	if contentStr == "" && len(toolCalls) == 0 {
		contentStr = " "
	}

	msg := map[string]any{"role": "assistant", "content": contentStr}

	if len(toolCalls) > 0 {
		msg["tool_calls"] = toAnySlice(toolCalls)
	}

	if reasoningReplay == ReplayReasoningContent {
		replayReasoning := strings.Join(thinkingParts, "\n")
		if hasReasoning {
			replayReasoning = reasoningContent
		}
		if replayReasoning != "" {
			msg["reasoning_content"] = replayReasoning
		}
	}

	return []map[string]any{msg}
}

// deferredPostToolToMessages mirrors _deferredPostToolToMessages().
func deferredPostToolToMessages(pending *pendingAfterTools) []map[string]any {
	if len(pending.deferredBlocks) == 0 {
		return nil
	}
	return convertAssistantMessage(pending.deferredBlocks, pending.topLevelReasoning, pending.hasTopLevelReasoning, pending.reasoningReplay)
}

// convertAssistantMessageWithSplit mirrors _convertAssistantMessageWithSplit().
func convertAssistantMessageWithSplit(content []anthropic.ContentBlock, firstToolIndex int, reasoningContent string, hasReasoning bool, reasoningReplay ReasoningReplayMode) ([]map[string]any, *pendingAfterTools) {
	pre := content[:firstToolIndex]
	toolCalls := iterToolUsesInOrder(content)

	if len(toolCalls) == 0 {
		return convertAssistantMessage(content, reasoningContent, hasReasoning, reasoningReplay), nil
	}

	deferred := deferredPostToolBlocks(content, firstToolIndex)

	var preMsg map[string]any
	if len(pre) == 0 {
		preMsg = map[string]any{"role": "assistant", "content": ""}
		if reasoningReplay == ReplayReasoningContent && hasReasoning {
			preMsg["reasoning_content"] = reasoningContent
		}
	} else {
		preMsg = convertAssistantMessage(pre, reasoningContent, hasReasoning, reasoningReplay)[0]
	}

	preMsg["tool_calls"] = toAnySlice(toolCalls)
	if len(toolCalls) > 0 && preMsg["content"] == " " {
		preMsg["content"] = ""
	}

	var pnd *pendingAfterTools
	if len(deferred) > 0 {
		resIds := make(map[string]bool, len(toolCalls))
		for _, tc := range toolCalls {
			tid, ok := tc["id"].(string)
			if ok && strings.TrimSpace(tid) != "" {
				resIds[tid] = true
			}
		}
		pnd = &pendingAfterTools{
			remainingToolIds:     resIds,
			deferredBlocks:       deferred,
			topLevelReasoning:    reasoningContent,
			hasTopLevelReasoning: hasReasoning,
			reasoningReplay:      reasoningReplay,
			deferredEmitted:      false,
		}
	}

	return []map[string]any{preMsg}, pnd
}

// convertUserMessageWithInjection mirrors _convertUserMessageWithInjection():
// user turns that follow an assistant turn with deferred post-tool blocks get
// the deferred blocks injected once all pending tool ids have their results.
// Like TS, an image block on this path is an OpenAIConversionError.
func convertUserMessageWithInjection(content []anthropic.ContentBlock, pending *pendingAfterTools) ([]map[string]any, bool, error) {
	if !needsDeferred(pending) || len(pending.remainingToolIds) == 0 {
		return convertUserMessage(content), false, nil
	}

	var result []map[string]any
	var textParts []string
	var toolMedia []map[string]any
	cleared := false

	flushText := func() {
		if len(textParts) > 0 {
			result = append(result, map[string]any{"role": "user", "content": strings.Join(textParts, "\n")})
			textParts = nil
		}
	}

	for i := range content {
		block := &content[i]
		if block.Type == "text" {
			textParts = append(textParts, block.Text)
		} else if block.Type == "image" {
			return nil, false, &OpenAIConversionError{
				Message: "User message image blocks are not supported for OpenAI chat conversion.",
			}
		} else if isToolResultBlockType(block.Type) {
			flushText()
			serial := serializeToolResultContent(block.ContentValue())
			tuid := block.ToolUseID
			toolText := serial.text
			if len(serial.images) > 0 {
				if toolText != "" {
					toolText += "\n"
				}
				toolText += toolResultMediaMarker
			}
			finalContent := toolText
			if block.IsError {
				finalContent = "[TOOL_ERROR] " + finalContent
			}

			result = append(result, map[string]any{
				"role":         "tool",
				"tool_call_id": tuid,
				"content":      finalContent,
			})
			if len(serial.images) > 0 {
				toolMedia = append(toolMedia, serial.images...)
			}

			if pending.remainingToolIds[tuid] {
				delete(pending.remainingToolIds, tuid)
				if len(pending.remainingToolIds) == 0 {
					result = append(result, deferredPostToolToMessages(pending)...)
					pending.deferredEmitted = true
					cleared = true
				}
			}
		}
	}

	flushText()
	// Re-emit extracted tool_result media as a synthetic user turn after the
	// tool message(s) (+ any deferred assistant blocks).
	if len(toolMedia) > 0 {
		parts := make([]map[string]any, 0, len(toolMedia)+1)
		parts = append(parts, map[string]any{"type": "text", "text": toolResultMediaMarker})
		parts = append(parts, toolMedia...)
		result = append(result, map[string]any{"role": "user", "content": toAnySlice(parts)})
	}
	return result, cleared, nil
}

// convertUserMessage mirrors _convertUserMessage() in converter.ts.
func convertUserMessage(content []anthropic.ContentBlock) []map[string]any {
	var result []map[string]any
	var textParts []string
	var imageParts []map[string]any
	var toolMedia []map[string]any

	flushText := func() {
		if len(textParts) > 0 {
			result = append(result, map[string]any{"role": "user", "content": strings.Join(textParts, "\n")})
			textParts = nil
		}
	}

	flushImages := func() {
		// OpenAI supports mixed text + image in a single user message via a
		// content part array. Pull the last simple text-only "user" message
		// back into content-parts format; with tool messages in between we
		// can't merge, so images are emitted separately.
		if len(imageParts) == 0 {
			return
		}
		flushText()
		var contentParts []map[string]any
		for i := len(result) - 1; i >= 0; i-- {
			msg := result[i]
			if msg["role"] == "user" {
				if s, ok := msg["content"].(string); ok {
					contentParts = append(contentParts, map[string]any{"type": "text", "text": s})
					result = append(result[:i], result[i+1:]...)
					break
				}
			}
		}
		contentParts = append(contentParts, imageParts...)
		result = append(result, map[string]any{"role": "user", "content": toAnySlice(contentParts)})
		imageParts = nil
	}

	for i := range content {
		block := &content[i]
		switch block.Type {
		case "text":
			textParts = append(textParts, block.Text)
		case "image":
			if part := buildImagePartFromBlock(block); part != nil {
				imageParts = append(imageParts, part)
			}
		case "document":
			src := block.Source
			if src != nil {
				mediaType := src.MediaType
				if mediaType == "" {
					mediaType = src.MimeType
				}
				title := block.Title
				context := block.Context
				if src.Type == "base64" && isImageMimeType(mediaType) {
					// Image document → image_url content part (preserves data).
					data := src.Data
					if data != "" {
						flushText()
						url := data
						if !strings.HasPrefix(data, "data:") {
							url = "data:" + mediaType + ";base64," + data
						}
						imageParts = append(imageParts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": url}})
					}
				} else if src.Type == "base64" {
					// Non-image binary document → text reference with data URL.
					data := src.Data
					filename := title
					if filename == "" {
						filename = "document"
					}
					dataURL := mediaType
					if data != "" {
						dataURL = "data:" + mediaType + ";base64," + data
					}
					desc := filename + " (" + dataURL + ")"
					textParts = append(textParts, documentRef(desc, context))
				} else {
					// URL (or unknown) source → text reference.
					filename := title
					if filename == "" {
						if src.Type == "url" {
							filename = src.URL
						} else {
							filename = "document"
						}
					}
					desc := filename
					if mediaType != "" {
						desc = filename + " (" + mediaType + ")"
					}
					textParts = append(textParts, documentRef(desc, context))
				}
			}
		default:
			if isToolResultBlockType(block.Type) {
				flushImages()
				flushText()
				serial := serializeToolResultContent(block.ContentValue())
				toolText := serial.text
				if len(serial.images) > 0 {
					if toolText != "" {
						toolText += "\n"
					}
					toolText += toolResultMediaMarker
				}
				if block.IsError {
					toolText = "[TOOL_ERROR] " + toolText
				}
				result = append(result, map[string]any{
					"role":         "tool",
					"tool_call_id": block.ToolUseID,
					"content":      toolText,
				})
				if len(serial.images) > 0 {
					toolMedia = append(toolMedia, serial.images...)
				}
			}
		}
	}

	flushImages()
	flushText()
	if len(toolMedia) > 0 {
		parts := make([]map[string]any, 0, len(toolMedia)+1)
		parts = append(parts, map[string]any{"type": "text", "text": toolResultMediaMarker})
		parts = append(parts, toolMedia...)
		result = append(result, map[string]any{"role": "user", "content": toAnySlice(parts)})
	}
	return result
}

// documentRef mirrors the `[Document: desc]\ncontext` formatting in
// _convertUserMessage.
func documentRef(desc, context string) string {
	if context != "" {
		return "[Document: " + desc + "]\n" + context
	}
	return "[Document: " + desc + "]"
}

// isImageMimeType mirrors isImageMimeType() in converter.ts.
func isImageMimeType(mime string) bool {
	return strings.HasPrefix(strings.ToLower(mime), "image/")
}

// mergeImageDetail mirrors mergeImageDetail(): attaches a truthy `detail`
// hint to an OpenAI image_url object.
func mergeImageDetail(imageURL map[string]any, detail string) map[string]any {
	if detail != "" {
		imageURL["detail"] = detail
	}
	return imageURL
}

// buildImagePartFromBlock mirrors buildImagePartFromBlock() for typed
// anthropic.ContentBlock values (user-message path).
func buildImagePartFromBlock(block *anthropic.ContentBlock) map[string]any {
	if block.Source == nil {
		return nil
	}
	src := block.Source
	switch src.Type {
	case "base64":
		mediaType := src.MediaType
		if mediaType == "" {
			mediaType = src.MimeType
		}
		if mediaType == "" {
			mediaType = "image/png"
		}
		data := src.Data
		if data == "" || !isImageMimeType(mediaType) {
			return nil
		}
		url := data
		if !strings.HasPrefix(data, "data:") {
			url = "data:" + mediaType + ";base64," + data
		}
		return map[string]any{"type": "image_url", "image_url": mergeImageDetail(map[string]any{"url": url}, block.Detail)}
	case "url":
		url := src.URL
		if url == "" {
			return nil
		}
		return map[string]any{"type": "image_url", "image_url": mergeImageDetail(map[string]any{"url": url}, block.Detail)}
	}
	return nil
}

// buildImagePartFromMap is buildImagePartFromBlock for raw decoded blocks
// (tool_result media-extraction path).
func buildImagePartFromMap(block map[string]any) map[string]any {
	source, ok := block["source"].(map[string]any)
	if !ok {
		return nil
	}
	sourceType := attrString(source, "type")
	switch sourceType {
	case "base64":
		mediaType := attrString(source, "media_type")
		if mediaType == "" {
			mediaType = attrString(source, "mime_type")
		}
		if mediaType == "" {
			mediaType = "image/png"
		}
		data := attrString(source, "data")
		if data == "" || !isImageMimeType(mediaType) {
			return nil
		}
		url := data
		if !strings.HasPrefix(data, "data:") {
			url = "data:" + mediaType + ";base64," + data
		}
		return map[string]any{"type": "image_url", "image_url": mergeImageDetail(map[string]any{"url": url}, attrString(block, "detail"))}
	case "url":
		url := attrString(source, "url")
		if url == "" {
			return nil
		}
		return map[string]any{"type": "image_url", "image_url": mergeImageDetail(map[string]any{"url": url}, attrString(block, "detail"))}
	}
	return nil
}

// ConvertMessages mirrors AnthropicToOpenAIConverter.convertMessages(). The
// deferred post-tool state machine (PendingAfterTools) is ported as-is. Like
// the TS throw, an image block on the user-injection path surfaces as an
// *OpenAIConversionError.
func ConvertMessages(messages []anthropic.Message, reasoningReplay ReasoningReplayMode) ([]map[string]any, error) {
	if reasoningReplay == "" {
		reasoningReplay = ReplayThinkTags // TS default parameter
	}
	var result []map[string]any
	var pending *pendingAfterTools

	for mi := range messages {
		msg := &messages[mi]
		reasoningContent, hasReasoning := cleanReasoningContent(msg.ReasoningContent)

		str, isString := msg.Content.String()
		blocks, isBlocks := msg.Content.Blocks()

		if msg.Role == "assistant" && isBlocks && blocks != nil {
			if pending != nil && needsDeferred(pending) {
				result = append(result, deferredPostToolToMessages(pending)...)
				pending.deferredEmitted = true
				pending = nil
			}

			firstI := indexFirstToolUse(blocks)
			if firstI >= 0 {
				out, newPending := convertAssistantMessageWithSplit(blocks, firstI, reasoningContent, hasReasoning, reasoningReplay)
				result = append(result, out...)
				if newPending != nil {
					pending = newPending
				}
			} else {
				result = append(result, convertAssistantMessage(blocks, reasoningContent, hasReasoning, reasoningReplay)...)
			}
		} else if isString {
			if msg.Role == "user" && pending != nil && needsDeferred(pending) {
				result = append(result, deferredPostToolToMessages(pending)...)
				pending.deferredEmitted = true
				pending = nil
			}

			converted := map[string]any{"role": msg.Role, "content": str}

			if msg.Role == "assistant" && hasReasoning {
				if reasoningReplay == ReplayReasoningContent {
					converted["reasoning_content"] = reasoningContent
				} else if reasoningReplay == ReplayThinkTags {
					contentParts := []string{thinkTagContent(reasoningContent)}
					if str != "" {
						contentParts = append(contentParts, str)
					}
					converted["content"] = strings.Join(contentParts, "\n\n")
				}
			}

			result = append(result, converted)
		} else if isBlocks && blocks != nil {
			if msg.Role == "user" {
				if pending != nil && needsDeferred(pending) {
					if len(pending.remainingToolIds) == 0 {
						result = append(result, deferredPostToolToMessages(pending)...)
						pending.deferredEmitted = true
						pending = nil
					}
					if pending != nil {
						pieces, cleared, err := convertUserMessageWithInjection(blocks, pending)
						if err != nil {
							return nil, err
						}
						result = append(result, pieces...)
						if cleared {
							pending = nil
						}
					} else {
						result = append(result, convertUserMessage(blocks)...)
					}
				} else {
					result = append(result, convertUserMessage(blocks)...)
				}
			}
		} else {
			// Absent content: TS would treat this as the fallthrough
			// String(content) branch; Go has no undefined, so the value is
			// empty text (known deviation, matches the TS typeof/Array.isArray
			// discrimination intent).
			if msg.Role == "user" && pending != nil && needsDeferred(pending) {
				result = append(result, deferredPostToolToMessages(pending)...)
				pending.deferredEmitted = true
				pending = nil
			}
			result = append(result, map[string]any{"role": msg.Role, "content": ""})
		}
	}

	if pending != nil && needsDeferred(pending) {
		result = append(result, deferredPostToolToMessages(pending)...)
	}

	return result, nil
}

// ConvertSystemPrompt mirrors AnthropicToOpenAIConverter.convertSystemPrompt().
func ConvertSystemPrompt(system any) map[string]any {
	if s, ok := system.(string); ok {
		// Strip the rotating `x-anthropic-billing-header:` first line so the
		// system prompt has a stable prefix (upstream prefix-cache reuse).
		stripped := strings.TrimSpace(stripLeadingAnthropicBillingHeader(s))
		if stripped != "" {
			return map[string]any{"role": "system", "content": stripped}
		}
		return nil
	}
	if arr, ok := system.([]any); ok {
		var textParts []string
		for _, el := range arr {
			if m, ok := el.(map[string]any); ok {
				if m["type"] == "text" {
					text := strings.TrimSpace(stripLeadingAnthropicBillingHeader(attrString(m, "text")))
					if text != "" {
						textParts = append(textParts, text)
					}
				}
			}
		}
		if len(textParts) > 0 {
			return map[string]any{"role": "system", "content": strings.TrimSpace(strings.Join(textParts, "\n\n"))}
		}
	}
	return nil
}

// ConvertTools mirrors AnthropicToOpenAIConverter.convertTools(): server tool
// types are proxy-side only and filtered out.
func ConvertTools(tools []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		if servertool.IsServerToolType(attrString(tool, "type")) {
			continue
		}
		fn := map[string]any{
			"description": toolDescription(tool),
			"parameters":  toolInputSchema(tool),
		}
		if name, ok := tool["name"]; ok {
			fn["name"] = name
		}
		out = append(out, map[string]any{
			"type":     "function",
			"function": fn,
		})
	}
	return out
}

// toolDescription mirrors `tool.description || ""` in convertTools().
func toolDescription(tool map[string]any) string {
	if v, ok := tool["description"]; ok && isTruthy(v) {
		return tsString(v)
	}
	return ""
}

// ConvertToolChoice mirrors AnthropicToOpenAIConverter.convertToolChoice().
func ConvertToolChoice(toolChoice any) any {
	tc, ok := toolChoice.(map[string]any)
	if !ok {
		return toolChoice
	}
	choiceType := tc["type"]

	if choiceType == "tool" {
		if name := tc["name"]; isTruthy(name) {
			return map[string]any{"type": "function", "function": map[string]any{"name": name}}
		}
	}
	if choiceType == "any" {
		return "required"
	}
	if choiceType == "auto" || choiceType == "none" || choiceType == "required" {
		return choiceType
	}
	if choiceType == "function" {
		if fn, ok := tc["function"].(map[string]any); ok && fn != nil {
			return toolChoice
		}
	}
	return toolChoice
}

// HasDisableParallelToolUse mirrors hasDisableParallelToolUse(): true when the
// Anthropic tool_choice has `disable_parallel_tool_use: true`.
func HasDisableParallelToolUse(toolChoice any) bool {
	tc, ok := toolChoice.(map[string]any)
	if !ok {
		return false
	}
	return tc["disable_parallel_tool_use"] == true
}

// RequestData mirrors the RequestData interface in converter.ts.
type RequestData struct {
	Model         string
	Messages      []anthropic.Message
	System        any
	MaxTokens     any // nil when absent
	Temperature   any
	TopP          any
	StopSequences []string
	Tools         []map[string]any
	ToolChoice    any
	ServerTools   []map[string]any
}

// BuildBaseRequestBody mirrors buildBaseRequestBody() in converter.ts.
// Server tools are collected from BOTH the server_tools field and the tools
// array (Claude Code puts server tools in the tools array), deduplicated by
// (type, name), and re-exposed as OpenAI function schemas plus a system
// prompt suffix.
func BuildBaseRequestBody(req *RequestData, defaultMaxTokens any, reasoningReplay ReasoningReplayMode) (map[string]any, error) {
	messages, err := ConvertMessages(req.Messages, reasoningReplay)
	if err != nil {
		return nil, err
	}

	// Collect server tools from both sources, deduplicated by type+name.
	var allServerTools []map[string]any
	for _, st := range req.ServerTools {
		if !containsServerTool(allServerTools, st) {
			allServerTools = append(allServerTools, st)
		}
	}
	for _, tool := range req.Tools {
		if servertool.IsServerToolType(attrString(tool, "type")) {
			if !containsServerTool(allServerTools, tool) {
				allServerTools = append(allServerTools, tool)
			}
		}
	}
	serverToolPrompt := ""
	if len(allServerTools) > 0 {
		serverToolPrompt = servertool.BuildServerToolSystemPromptSuffix(allServerTools)
	}

	if req.System != nil {
		if systemMsg := ConvertSystemPrompt(req.System); systemMsg != nil {
			if serverToolPrompt != "" {
				systemMsg["content"] = systemMsg["content"].(string) + "\n\n" + serverToolPrompt
			}
			messages = append([]map[string]any{systemMsg}, messages...)
		}
	} else if serverToolPrompt != "" {
		messages = append([]map[string]any{{"role": "system", "content": serverToolPrompt}}, messages...)
	}

	body := map[string]any{"model": req.Model, "messages": toAnySlice(messages)}

	maxTokens := req.MaxTokens
	if maxTokens == nil {
		maxTokens = defaultMaxTokens
	}
	if maxTokens != nil {
		body["max_tokens"] = maxTokens
	}
	if req.Temperature != nil {
		body["temperature"] = req.Temperature
	}
	if req.TopP != nil {
		body["top_p"] = req.TopP
	}
	if len(req.StopSequences) > 0 {
		body["stop"] = req.StopSequences
	}

	// Build server tool function schemas from both sources.
	var serverToolSchemas []map[string]any
	seenServerToolTypes := make(map[string]bool)
	for _, st := range req.ServerTools {
		stType := attrString(st, "type")
		stName := attrString(st, "name")
		if schema := servertool.BuildServerToolFunctionSchema(stType, stName); schema != nil {
			serverToolSchemas = append(serverToolSchemas, schema)
			seenServerToolTypes[stType] = true
		}
	}
	for _, tool := range req.Tools {
		toolType := attrString(tool, "type")
		if servertool.IsServerToolType(toolType) && !seenServerToolTypes[toolType] {
			toolName := attrString(tool, "name")
			if schema := servertool.BuildServerToolFunctionSchema(toolType, toolName); schema != nil {
				serverToolSchemas = append(serverToolSchemas, schema)
				seenServerToolTypes[toolType] = true
			}
		}
	}

	if len(req.Tools) > 0 {
		regularTools := ConvertTools(req.Tools)
		allTools := append(regularTools, serverToolSchemas...)
		if len(allTools) > 0 {
			body["tools"] = toAnySlice(allTools)
		}
	} else if len(serverToolSchemas) > 0 {
		body["tools"] = toAnySlice(serverToolSchemas)
	}

	if req.ToolChoice != nil {
		body["tool_choice"] = ConvertToolChoice(req.ToolChoice)
		if HasDisableParallelToolUse(req.ToolChoice) {
			body["parallel_tool_calls"] = false
		}
	}

	return body, nil
}

// containsServerTool mirrors the allServerTools.some((t) => t.type === x.type
// && t.name === x.name) dedupe in buildBaseRequestBody().
func containsServerTool(tools []map[string]any, t map[string]any) bool {
	for _, x := range tools {
		if x["type"] == t["type"] && x["name"] == t["name"] {
			return true
		}
	}
	return false
}

// toAnySlice converts []map[string]any to []any so the body values match the
// TS Record<string, unknown>[] shapes in assertions.
func toAnySlice(msgs []map[string]any) []any {
	out := make([]any, len(msgs))
	for i, m := range msgs {
		out[i] = m
	}
	return out
}

// EstimateInputTokens mirrors estimateInputTokens() in core/tokens.ts:
// char/4 per text/thinking block, tool_use input+name, tool_result text
// sub-blocks; +4 per message; at least 1.
func EstimateInputTokens(messages []anthropic.Message) int64 {
	var total int64
	for mi := range messages {
		msg := &messages[mi]
		if s, ok := msg.Content.String(); ok {
			total += estimateTokens(s)
		} else if blocks, ok := msg.Content.Blocks(); ok && blocks != nil {
			for bi := range blocks {
				b := &blocks[bi]
				switch b.Type {
				case "text":
					total += estimateTokens(b.Text)
				case "thinking":
					total += estimateTokens(b.Thinking)
				case "tool_use":
					total += estimateTokens(jsonStringify(b.InputValue()))
					total += estimateTokens(b.Name)
				case "tool_result":
					if cv := b.ContentValue(); cv != nil {
						if s, ok := cv.(string); ok {
							total += estimateTokens(s)
						} else if arr, ok := cv.([]any); ok {
							for _, sub := range arr {
								if m, ok := sub.(map[string]any); ok {
									if m["type"] == "text" {
										if s, ok := m["text"].(string); ok {
											total += estimateTokens(s)
										}
									}
								}
							}
						}
					}
				}
			}
		}
		total += 4 // per-message overhead
	}
	if total < 1 {
		total = 1
	}
	return total
}

// estimateTokens mirrors estimateTokens() in core/tokens.ts: ceil(chars / 4).
// Rune count mirrors TS string length for BMP text (incl. CJK).
func estimateTokens(text string) int64 {
	if text == "" {
		return 0
	}
	n := int64(utf8.RuneCountInString(text))
	return (n + 3) / 4
}

// jsonStringify mirrors JSON.stringify for JSON-decoded values.
func jsonStringify(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// toolInputArguments mirrors the `typeof toolInput === "object" && ... ?
// canonicalJsonStringify(toolInput) : String(toolInput)` branch used for tool
// call arguments.
func toolInputArguments(input any) string {
	if m, ok := input.(map[string]any); ok {
		s, err := CanonicalJSONStringify(m)
		if err != nil {
			return ""
		}
		return s
	}
	return tsString(input)
}

// tsString approximates TS String() for JSON-decoded values: String(null) =
// "null", String(object) = "[object Object]", arrays join with ",".
func tsString(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case []any:
		parts := make([]string, len(t))
		for i, el := range t {
			parts[i] = tsString(el)
		}
		return strings.Join(parts, ",")
	case map[string]any:
		return "[object Object]"
	default:
		return jsonStringify(v)
	}
}

// attrString mirrors `String(getBlockAttr(block, key, "") ?? "")`: "" for a
// missing or null attribute, otherwise TS String() of the value.
func attrString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	return tsString(v)
}

// isTruthy mirrors TS truthiness for values decoded from JSON.
func isTruthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case float64:
		return t != 0
	case json.Number:
		return t.String() != "" && t.String() != "0"
	default:
		return true
	}
}

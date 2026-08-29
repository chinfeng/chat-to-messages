package responses

import (
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	"github.com/chinfeng/chat-to-messages/internal/convert"
	"github.com/chinfeng/chat-to-messages/internal/openai"
	"github.com/chinfeng/chat-to-messages/internal/parsers"
	"github.com/chinfeng/chat-to-messages/internal/sse"
)

// Translator converts one upstream chat/completions chunk stream into
// Responses-format SSE events (spec §出站). Items accumulate as they close so
// the terminal response object (and the response store) see the full output.
type Translator struct {
	responseID    string
	base          map[string]any // response skeleton echoed from the request
	wantEncrypted bool
	started       bool
	terminal      bool // a terminal event (completed/incomplete/failed) went out

	output []any // finalized output items, close order

	reasoningID      string
	reasoningOpen    bool
	reasoningSummary strings.Builder

	messageID   string
	messageOpen bool
	messageText strings.Builder

	tools     map[int]*toolState
	toolOrder []int
	openTool  *toolState // the one function_call item currently streaming

	thinkParser    *parsers.ThinkTagParser
	heuristicParse *parsers.HeuristicToolParser

	usage        *openai.Usage
	finishReason string
	sawDone      bool
	failure      *StreamFailure
}

type toolState struct {
	itemID   string
	callID   string
	name     string
	args     strings.Builder
	buffered string // args fragments that arrived before the item was opened
	opened   bool
	closed   bool // lifecycle already emitted; Finish must not re-emit
}

// StreamFailure is a mid-stream upstream failure (surfaced as
// response.failed downstream).
type StreamFailure struct{ Code, Message string }

// NewTranslator creates a translator for one request. responseID is the
// downstream-facing resp_ id; base echoes the request's response fields.
func NewTranslator(req *Request, responseID string) *Translator {
	t := &Translator{
		responseID:     responseID,
		wantEncrypted:  req.WantsEncrypted(),
		tools:          make(map[int]*toolState),
		thinkParser:    parsers.NewThinkTagParser(),
		heuristicParse: parsers.NewHeuristicToolParser(),
	}
	t.base = baseResponse(req, responseID)
	return t
}

// Process consumes one upstream chunk and yields the event strings it
// produces. Chunk.Error / Chunk.Err terminate the stream with response.failed.
func (t *Translator) Process(c openai.Chunk) []string {
	if t.terminal {
		return nil
	}
	var events []string
	emit := func(name string, data map[string]any) {
		events = append(events, sse.FormatEvent(name, data))
	}
	t.ensureStarted(emit)

	if c.Err != nil {
		t.failure = &StreamFailure{Code: "server_error", Message: "Connection closed mid-response. The response above may be incomplete."}
		return events
	}
	if c.Done {
		t.sawDone = true
		return events
	}
	if c.Error != nil {
		code := "server_error"
		msg := c.Error.Message
		if msg == "" {
			msg = "upstream error"
		}
		t.failure = &StreamFailure{Code: code, Message: "Server error mid-response. The response above may be incomplete. (upstream: " + msg + ")"}
		return events
	}
	if c.Usage != nil {
		t.usage = c.Usage
	}
	if len(c.Choices) == 0 {
		return events
	}
	choice := c.Choices[0]
	if choice.FinishReason != nil && *choice.FinishReason != "" {
		t.finishReason = *choice.FinishReason
	}
	delta := choice.Delta
	if delta == nil {
		return events
	}

	// Reasoning (native reasoning_content / GLM delta.reasoning alias).
	if delta.ReasoningContent != nil && *delta.ReasoningContent != "" {
		t.emitReasoningDelta(emit, *delta.ReasoningContent)
	}

	// Refusal streams as plain output text (same as the messages path).
	if delta.Refusal != nil && *delta.Refusal != "" {
		t.emitTextDelta(emit, *delta.Refusal)
	}

	// Text: think-tags route to reasoning; heuristic XML tool calls become
	// function_call items (GLM-family upstreams emit tools inside content).
	if delta.Content != nil && *delta.Content != "" {
		for _, part := range t.thinkParser.Feed(*delta.Content) {
			if part.Type == parsers.ThinkingContent {
				t.emitReasoningDelta(emit, part.Content)
				continue
			}
			filtered, toolUses := t.heuristicParse.Feed(part.Content)
			if filtered != "" {
				t.emitTextDelta(emit, filtered)
			}
			for _, toolUse := range toolUses {
				t.emitHeuristicToolCall(emit, toolUse)
			}
		}
	}

	// Native tool calls.
	for _, tc := range delta.ToolCalls {
		st := t.tool(tc.Index)
		if tc.Function.Name != nil && *tc.Function.Name != "" {
			st.name = *tc.Function.Name
		}
		if st.name != "" && !st.opened {
			t.openToolItem(emit, st)
		}
		if tc.Function.Arguments != nil && *tc.Function.Arguments != "" {
			st.args.WriteString(*tc.Function.Arguments)
			if st.opened {
				emit("response.function_call_arguments.delta", map[string]any{
					"item_id":      st.itemID,
					"output_index": len(t.output),
					"delta":        *tc.Function.Arguments,
				})
			} else {
				st.buffered += *tc.Function.Arguments
			}
		}
	}
	return events
}

// Finish flushes parser state, closes open items, and emits the terminal
// event (completed / incomplete / failed). It also fires created/in_progress
// when the stream produced no chunks at all.
func (t *Translator) Finish() []string {
	if t.terminal {
		return nil
	}
	t.terminal = true
	var events []string
	emit := func(name string, data map[string]any) {
		events = append(events, sse.FormatEvent(name, data))
	}
	t.ensureStarted(emit)

	// Parser flushes.
	if remaining := t.thinkParser.Flush(); remaining != nil {
		if remaining.Type == parsers.ThinkingContent {
			t.emitReasoningDelta(emit, remaining.Content)
		} else {
			t.emitTextDelta(emit, remaining.Content)
		}
	}
	if filtered, toolUses := t.heuristicParse.Flush(); filtered != "" || len(toolUses) > 0 {
		if filtered != "" {
			t.emitTextDelta(emit, filtered)
		}
		for _, toolUse := range toolUses {
			t.emitHeuristicToolCall(emit, toolUse)
		}
	}

	// Close open items, in stream order: reasoning, message, then tools (tool
	// items opened interleaved with the message close independently).
	t.closeReasoning(emit)
	t.closeMessage(emit)
	for _, idx := range t.toolOrder {
		t.closeToolItem(emit, t.tools[idx])
	}

	switch {
	case t.failure != nil:
		resp := t.responseObject("failed")
		resp["error"] = map[string]any{"code": t.failure.Code, "message": t.failure.Message}
		emit("response.failed", map[string]any{"type": "response.failed", "response": resp})
	case t.finishReason == "length":
		resp := t.responseObject("incomplete")
		resp["incomplete_details"] = map[string]any{"reason": "max_output_tokens"}
		emit("response.incomplete", map[string]any{"type": "response.incomplete", "response": resp})
	case t.finishReason == "content_filter":
		resp := t.responseObject("incomplete")
		resp["incomplete_details"] = map[string]any{"reason": "content_filter"}
		emit("response.incomplete", map[string]any{"type": "response.incomplete", "response": resp})
	default:
		emit("response.completed", map[string]any{"type": "response.completed", "response": t.responseObject("completed")})
	}
	return events
}

// FinalResponse assembles the response object after Finish (status matches
// the terminal event). Used for non-stream downstream answers and dump/store.
func (t *Translator) FinalResponse() map[string]any {
	switch {
	case t.failure != nil:
		resp := t.responseObject("failed")
		resp["error"] = map[string]any{"code": t.failure.Code, "message": t.failure.Message}
		return resp
	case t.finishReason == "length":
		resp := t.responseObject("incomplete")
		resp["incomplete_details"] = map[string]any{"reason": "max_output_tokens"}
		return resp
	case t.finishReason == "content_filter":
		resp := t.responseObject("incomplete")
		resp["incomplete_details"] = map[string]any{"reason": "content_filter"}
		return resp
	default:
		return t.responseObject("completed")
	}
}

// OutputItems returns the finalized output items (for the response store).
func (t *Translator) OutputItems() []any {
	out := make([]any, len(t.output))
	copy(out, t.output)
	return out
}

// ensureStarted emits response.created + response.in_progress exactly once.
func (t *Translator) ensureStarted(emit func(string, map[string]any)) {
	if t.started {
		return
	}
	t.started = true
	resp := t.responseSkeleton("in_progress")
	emit("response.created", map[string]any{"type": "response.created", "response": resp})
	emit("response.in_progress", map[string]any{"type": "response.in_progress", "response": resp})
}

// emitReasoningDelta opens the reasoning item (one summary part) on first
// reason chunk and streams the delta. Like every output item, opening it
// first closes whatever item is currently streaming (items never interleave;
// output_index counts closed items).
func (t *Translator) emitReasoningDelta(emit func(string, map[string]any), text string) {
	if !t.reasoningOpen {
		if t.openTool != nil {
			t.closeToolItem(emit, t.openTool)
		}
		if t.messageOpen {
			t.closeMessage(emit)
		}
		t.reasoningID = newID("rs_")
		t.reasoningOpen = true
		emit("response.output_item.added", map[string]any{
			"type":         "response.output_item.added",
			"output_index": len(t.output),
			"item": map[string]any{
				"type":    "reasoning",
				"id":      t.reasoningID,
				"summary": []any{},
				"status":  "in_progress",
			},
		})
		emit("response.reasoning_summary_part.added", map[string]any{
			"type":          "response.reasoning_summary_part.added",
			"item_id":       t.reasoningID,
			"output_index":  len(t.output),
			"summary_index": 0,
			"part":          map[string]any{"type": "summary_text", "text": ""},
		})
	}
	t.reasoningSummary.WriteString(text)
	emit("response.reasoning_summary_text.delta", map[string]any{
		"type":          "response.reasoning_summary_text.delta",
		"item_id":       t.reasoningID,
		"output_index":  len(t.output),
		"summary_index": 0,
		"delta":         text,
	})
}

func (t *Translator) closeReasoning(emit func(string, map[string]any)) {
	if !t.reasoningOpen {
		return
	}
	t.reasoningOpen = false
	full := t.reasoningSummary.String()
	emit("response.reasoning_summary_text.done", map[string]any{
		"type":          "response.reasoning_summary_text.done",
		"item_id":       t.reasoningID,
		"output_index":  len(t.output),
		"summary_index": 0,
		"text":          full,
	})
	part := map[string]any{"type": "summary_text", "text": full}
	emit("response.reasoning_summary_part.done", map[string]any{
		"type":          "response.reasoning_summary_part.done",
		"item_id":       t.reasoningID,
		"output_index":  len(t.output),
		"summary_index": 0,
		"part":          part,
	})
	t.output = append(t.output, reasoningItem(t.reasoningID, full, t.wantEncrypted))
	emit("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": len(t.output) - 1,
		"item":         t.output[len(t.output)-1],
	})
}

// reasoningItem is the finalized reasoning output item (also what the store
// replays).
func reasoningItem(id, summary string, wantEncrypted bool) map[string]any {
	item := map[string]any{
		"type":    "reasoning",
		"id":      id,
		"summary": []any{map[string]any{"type": "summary_text", "text": summary}},
		"status":  "completed",
	}
	if wantEncrypted {
		item["encrypted_content"] = EncryptReasoning(summary)
	}
	return item
}

// emitTextDelta opens the message item (with its lone output_text part) on
// first text and streams the delta.
func (t *Translator) emitTextDelta(emit func(string, map[string]any), text string) {
	// A message item cannot coexist with an open reasoning/tool item: close
	// whatever is streaming (mirrors the Anthropic side's block switch).
	if t.openTool != nil {
		t.closeToolItem(emit, t.openTool)
	}
	if t.reasoningOpen {
		t.closeReasoning(emit)
	}
	if !t.messageOpen {
		t.messageID = newID("msg_")
		t.messageOpen = true
		emit("response.output_item.added", map[string]any{
			"type":         "response.output_item.added",
			"output_index": len(t.output),
			"item": map[string]any{
				"type":    "message",
				"id":      t.messageID,
				"status":  "in_progress",
				"role":    "assistant",
				"content": []any{},
			},
		})
		emit("response.content_part.added", map[string]any{
			"type":          "response.content_part.added",
			"item_id":       t.messageID,
			"output_index":  len(t.output),
			"content_index": 0,
			"part":          map[string]any{"type": "output_text", "annotations": []any{}, "text": ""},
		})
	}
	t.messageText.WriteString(text)
	emit("response.output_text.delta", map[string]any{
		"type":          "response.output_text.delta",
		"item_id":       t.messageID,
		"output_index":  len(t.output),
		"content_index": 0,
		"delta":         text,
	})
}

func (t *Translator) closeMessage(emit func(string, map[string]any)) {
	if !t.messageOpen {
		return
	}
	t.messageOpen = false
	full := t.messageText.String()
	emit("response.output_text.done", map[string]any{
		"type":          "response.output_text.done",
		"item_id":       t.messageID,
		"output_index":  len(t.output),
		"content_index": 0,
		"text":          full,
	})
	part := map[string]any{"type": "output_text", "annotations": []any{}, "text": full}
	emit("response.content_part.done", map[string]any{
		"type":          "response.content_part.done",
		"item_id":       t.messageID,
		"output_index":  len(t.output),
		"content_index": 0,
		"part":          part,
	})
	t.output = append(t.output, map[string]any{
		"type":    "message",
		"id":      t.messageID,
		"status":  "completed",
		"role":    "assistant",
		"content": []any{part},
	})
	emit("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": len(t.output) - 1,
		"item":         t.output[len(t.output)-1],
	})
}

// tool returns (creating) the state for upstream tool-call index.
func (t *Translator) tool(index int) *toolState {
	if st, ok := t.tools[index]; ok {
		return st
	}
	st := &toolState{callID: newID("call_")}
	t.tools[index] = st
	t.toolOrder = append(t.toolOrder, index)
	return st
}

// openToolItem emits output_item.added for a native tool call. The call_id is
// ALWAYS proxy-minted (never the upstream tool_call id): upstreams mint ids
// that repeat across turns (the 2026-08-28 "Bash:0" duplicate-id doom loop),
// and the client joins function_call_output by call_id.
func (t *Translator) openToolItem(emit func(string, map[string]any), st *toolState) {
	// A function_call item cannot interleave inside another output item.
	// chat/completions streams tool_calls strictly by index, so a different
	// index opening means the previous tool finished — close it first.
	if t.openTool != nil && t.openTool != st {
		t.closeToolItem(emit, t.openTool)
	}
	if t.reasoningOpen {
		t.closeReasoning(emit)
	}
	if t.messageOpen {
		t.closeMessage(emit)
	}
	st.opened = true
	st.itemID = newID("fc_")
	t.openTool = st
	emit("response.output_item.added", map[string]any{
		"type":         "response.output_item.added",
		"output_index": len(t.output),
		"item": map[string]any{
			"type":      "function_call",
			"id":        st.itemID,
			"call_id":   st.callID,
			"name":      st.name,
			"arguments": "",
			"status":    "in_progress",
		},
	})
	if st.buffered != "" {
		emit("response.function_call_arguments.delta", map[string]any{
			"type":         "response.function_call_arguments.delta",
			"item_id":      st.itemID,
			"output_index": len(t.output),
			"delta":        st.buffered,
		})
		st.buffered = ""
	}
}

// closeToolItem emits arguments.done + output_item.done and finalizes the item.
func (t *Translator) closeToolItem(emit func(string, map[string]any), st *toolState) {
	if !st.opened || st.closed {
		// Never-opened: orphan (name never materialized) — drop, matching the
		// messages path. Already-closed: nothing to do (Finish re-walks all
		// indices after switching closes the previous one).
		return
	}
	st.closed = true
	t.openTool = nil
	full := st.args.String()
	emit("response.function_call_arguments.done", map[string]any{
		"type":         "response.function_call_arguments.done",
		"item_id":      st.itemID,
		"output_index": len(t.output),
		"arguments":    full,
	})
	t.output = append(t.output, map[string]any{
		"type":      "function_call",
		"id":        st.itemID,
		"call_id":   st.callID,
		"name":      st.name,
		"arguments": full,
		"status":    "completed",
	})
	emit("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": len(t.output) - 1,
		"item":         t.output[len(t.output)-1],
	})
}

// emitHeuristicToolCall emits a complete function_call lifecycle for a
// tool call parsed out of text content (GLM-family XML).
func (t *Translator) emitHeuristicToolCall(emit func(string, map[string]any), toolUse map[string]any) {
	if t.openTool != nil {
		t.closeToolItem(emit, t.openTool)
	}
	if t.reasoningOpen {
		t.closeReasoning(emit)
	}
	if t.messageOpen {
		t.closeMessage(emit)
	}
	name, _ := toolUse["name"].(string)
	args := "{}"
	if raw := toolUse["input"]; raw != nil {
		if s, err := convert.CanonicalJSONStringify(raw); err == nil {
			args = s
		}
	}
	itemID := newID("fc_")
	callID := newID("call_") // always proxy-minted; see openToolItem
	emit("response.output_item.added", map[string]any{
		"type":         "response.output_item.added",
		"output_index": len(t.output),
		"item": map[string]any{
			"type":      "function_call",
			"id":        itemID,
			"call_id":   callID,
			"name":      name,
			"arguments": "",
			"status":    "in_progress",
		},
	})
	emit("response.function_call_arguments.delta", map[string]any{
		"type":         "response.function_call_arguments.delta",
		"item_id":      itemID,
		"output_index": len(t.output),
		"delta":        args,
	})
	emit("response.function_call_arguments.done", map[string]any{
		"type":         "response.function_call_arguments.done",
		"item_id":      itemID,
		"output_index": len(t.output),
		"arguments":    args,
	})
	t.output = append(t.output, map[string]any{
		"type":      "function_call",
		"id":        itemID,
		"call_id":   callID,
		"name":      name,
		"arguments": args,
		"status":    "completed",
	})
	emit("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": len(t.output) - 1,
		"item":         t.output[len(t.output)-1],
	})
}

// responseSkeleton is the in_progress response object echoing the request.
func (t *Translator) responseSkeleton(status string) map[string]any {
	resp := make(map[string]any, len(t.base)+4)
	for k, v := range t.base {
		resp[k] = v
	}
	resp["status"] = status
	return resp
}

// responseObject is the terminal response object: skeleton + output + usage.
func (t *Translator) responseObject(status string) map[string]any {
	resp := t.responseSkeleton(status)
	output := t.output
	if output == nil {
		output = []any{}
	}
	resp["output"] = output
	resp["usage"] = t.usageObject()
	return resp
}

// usageObject maps upstream chat usage onto Responses usage; falls back to a
// char/4 estimate of what we streamed when the upstream sent no usage.
func (t *Translator) usageObject() map[string]any {
	if t.usage != nil {
		u := t.usage
		input := map[string]any{"cached_tokens": int64(0)}
		if u.CacheReadInputTokens != nil {
			input["cached_tokens"] = *u.CacheReadInputTokens
		} else if d := u.PromptTokensDetails; d != nil && d.CachedTokens != nil {
			input["cached_tokens"] = *d.CachedTokens
		}
		outputDetails := map[string]any{"reasoning_tokens": int64(0)}
		if d := u.CompletionTokensDetails; d != nil && d.ReasoningTokens != nil {
			outputDetails["reasoning_tokens"] = *d.ReasoningTokens
		}
		return map[string]any{
			"input_tokens":          u.PromptTokens,
			"input_tokens_details":  input,
			"output_tokens":         u.CompletionTokens,
			"output_tokens_details": outputDetails,
			"total_tokens":          u.PromptTokens + u.CompletionTokens,
		}
	}
	chars := t.messageText.Len() + t.reasoningSummary.Len()
	for _, idx := range t.toolOrder {
		st := t.tools[idx]
		chars += len(st.name) + st.args.Len()
	}
	est := int64(chars+3) / 4
	return map[string]any{
		"input_tokens":          int64(0),
		"input_tokens_details":  map[string]any{"cached_tokens": int64(0)},
		"output_tokens":         est,
		"output_tokens_details": map[string]any{"reasoning_tokens": int64(0)},
		"total_tokens":          est,
	}
}

// baseResponse builds the echo subset of the response skeleton from the
// request.
func baseResponse(req *Request, responseID string) map[string]any {
	store := true
	if req.Store != nil {
		store = *req.Store
	}
	parallel := true
	if req.ParallelToolCalls != nil {
		parallel = *req.ParallelToolCalls
	}
	text := map[string]any{"format": map[string]any{"type": "text"}}
	if req.Text != nil {
		f := map[string]any{"type": "text"}
		if req.Text.Format.Type != "" {
			f["type"] = req.Text.Format.Type
		}
		text = map[string]any{"format": f}
		if req.Text.Verbosity != "" {
			text["verbosity"] = req.Text.Verbosity
		}
	}
	truncation := req.Truncation
	if truncation == "" {
		truncation = "disabled"
	}
	var prevID any
	if req.PreviousResponseID != "" {
		prevID = req.PreviousResponseID
	}
	var instructions any
	if req.Instructions != nil {
		instructions = *req.Instructions
	}
	var maxOutput any
	if req.MaxOutputTokens != nil {
		maxOutput = req.MaxOutputTokens
	}
	reasoning := map[string]any{"effort": nil, "summary": nil}
	if req.Reasoning != nil {
		if req.Reasoning.Effort != "" {
			reasoning["effort"] = req.Reasoning.Effort
		}
		if req.Reasoning.Summary != "" {
			reasoning["summary"] = req.Reasoning.Summary
		}
	}
	tools := make([]any, 0, len(req.Tools))
	for _, tool := range req.Tools {
		tools = append(tools, map[string]any{
			"type": tool.Type, "name": tool.Name,
			"description": tool.Description, "parameters": tool.Parameters,
		})
	}
	var temperature, topP any
	if req.Temperature != nil {
		temperature = req.Temperature
	} else {
		temperature = 1.0
	}
	if req.TopP != nil {
		topP = req.TopP
	} else {
		topP = 1.0
	}
	var metadata any = map[string]any{}
	if req.Metadata != nil {
		metadata = req.Metadata
	}
	var toolChoice any = "auto"
	if req.ToolChoice != nil {
		toolChoice = req.ToolChoice
	}
	return map[string]any{
		"id":                   responseID,
		"object":               "response",
		"created_at":           time.Now().Unix(),
		"status":               "in_progress",
		"background":           false,
		"error":                nil,
		"incomplete_details":   nil,
		"instructions":         instructions,
		"max_output_tokens":    maxOutput,
		"max_tool_calls":       nil,
		"model":                req.Model,
		"output":               []any{},
		"parallel_tool_calls":  parallel,
		"previous_response_id": prevID,
		"prompt_cache_key":     nil,
		"reasoning":            reasoning,
		"safety_identifier":    nil,
		"service_tier":         "default",
		"store":                store,
		"stream":               req.Stream,
		"temperature":          temperature,
		"text":                 text,
		"tool_choice":          toolChoice,
		"tools":                tools,
		"top_p":                topP,
		"truncation":           truncation,
		"usage":                nil,
		"user":                 nil,
		"metadata":             metadata,
	}
}

// newID returns prefix + a UUID v4 (crypto/rand; panics on entropy failure,
// mirroring the stream package's uuidV4).
func newID(prefix string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("responses: crypto/rand unavailable: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s%x-%x-%x-%x-%x", prefix, b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// NewID mints an id with the Responses prefix conventions (resp_, msg_ …).
// Exported for the route layer, which owns the response id.
func NewID(prefix string) string { return newID(prefix) }

// Failure reports the mid-stream upstream failure, if the stream ended in
// response.failed (nil otherwise).
func (t *Translator) Failure() *StreamFailure { return t.failure }

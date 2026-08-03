// Package stream converts an OpenAI-style chat completions SSE stream into
// Anthropic-format SSE events, ported from chat-to-claude-code's
// src/transport/stream.ts. The Streamer drives a state machine over
// openai.Chunk values (text, reasoning, native tool calls, heuristic tool
// calls, usage) and emits the equivalent Anthropic event strings.
package stream

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"time"

	"chat-to-messages/internal/convert"
	"chat-to-messages/internal/dump"
	"chat-to-messages/internal/openai"
	"chat-to-messages/internal/parsers"
	"chat-to-messages/internal/sse"
)

// UpstreamStreamError mirrors the TS class of the same name: the upstream
// embedded an error object in the SSE stream (HTTP 200 with an error payload).
// Like the TS catch block, it is swallowed once output has been produced (the
// partial turn plus incomplete notice completes gracefully and Err() stays
// nil); with no output it propagates from Err().
type UpstreamStreamError struct {
	Message string
	Code    int64
}

func (e *UpstreamStreamError) Error() string { return e.Message }

// UpstreamAbortedError mirrors the TS class of the same name: thrown when the
// upstream CONNECTION terminated mid-stream — a read error/reset
// (connection_closed) or a clean EOF without finish_reason ([DONE] also
// absent, response_stalled). Carries no upstream error code. When output had
// already been produced the abort is swallowed (graceful completion with an
// incomplete notice); with no output it propagates from Err().
type UpstreamAbortedError struct {
	Message string
	Subtype string // "connection_closed" | "response_stalled"
}

func (e *UpstreamAbortedError) Error() string { return e.Message }

// Options mirrors the TS StreamOptions interface.
type Options struct {
	// SkipMessageLifecycle skips message_start/message_delta/message_stop.
	// Used when the agentic loop already emitted message_start and will
	// handle the message lifecycle events.
	SkipMessageLifecycle bool
	// StartingBlockIndex offsets the first content block index. Used when
	// server_tool_use blocks were already emitted before this stream starts.
	StartingBlockIndex int
	// IsDownstreamAborted reports whether the DOWNSTREAM client has already
	// disconnected. When provided and currently aborted, the stream layer
	// must NOT report an upstream termination to the dump (the disconnect is
	// downstream-initiated; the route's cancel() path owns that label). When
	// omitted, upstream-termination reporting is skipped entirely.
	IsDownstreamAborted func() bool
}

// BuildIncompleteNotice builds the Claude-standard incomplete-notice text
// appended when a streaming request fails mid-response AFTER the upstream has
// started producing output. Mirrors the documented strings at
// https://code.claude.com/docs/en/errors#the-response-above-may-be-incomplete.
// When no output was produced yet this notice is NOT used — the error
// surfaces from Err() instead.
func BuildIncompleteNotice(err error) string {
	var streamErr *UpstreamStreamError
	if errors.As(err, &streamErr) {
		return "\n\nAPI Error: Server error mid-response. The response above may be incomplete."
	}
	var abortedErr *UpstreamAbortedError
	if errors.As(err, &abortedErr) {
		if abortedErr.Subtype == "connection_closed" {
			return "\n\nAPI Error: Connection closed mid-response. The response above may be incomplete."
		}
		return "\n\nAPI Error: Response stalled mid-stream. The response above may be incomplete."
	}
	// Fallback for unknown errors
	return "\n\nAPI Error: Response stalled mid-stream. The response above may be incomplete."
}

// ExtractUsageInfo mirrors the TS extractUsageInfo(): Anthropic-compatible
// usage buckets with two-tier cache fallback:
//
//	cache_read     = usage.cache_read_input_tokens (Anthropic-compat direct)
//	               | prompt_tokens_details.cached_tokens (OpenAI-standard)
//	cache_creation = usage.cache_creation_input_tokens (Anthropic-compat direct)
//	               | prompt_tokens_details.cache_write_tokens (OpenAI-standard)
//
// The details fallback requires a value > 0 (TS `details?.cached_tokens &&
// details.cached_tokens > 0`); the direct fields win even when 0.
func ExtractUsageInfo(chunkUsage *openai.Usage) *sse.UsageInfo {
	if chunkUsage == nil {
		return nil
	}
	prompt := chunkUsage.PromptTokens
	completion := chunkUsage.CompletionTokens
	cacheRead := int64(0)
	if chunkUsage.CacheReadInputTokens != nil {
		cacheRead = *chunkUsage.CacheReadInputTokens
	} else if d := chunkUsage.PromptTokensDetails; d != nil && d.CachedTokens != nil && *d.CachedTokens > 0 {
		cacheRead = *d.CachedTokens
	}
	cacheCreate := int64(0)
	if chunkUsage.CacheCreationInputTokens != nil {
		cacheCreate = *chunkUsage.CacheCreationInputTokens
	} else if d := chunkUsage.PromptTokensDetails; d != nil && d.CacheWriteTokens != nil && *d.CacheWriteTokens > 0 {
		cacheCreate = *d.CacheWriteTokens
	}
	return &sse.UsageInfo{
		PromptTokens:             prompt,
		CompletionTokens:         completion,
		CacheReadInputTokens:     cacheRead,
		CacheCreationInputTokens: cacheCreate,
	}
}

// InferToolNameByIndex mirrors the TS inferToolNameByIndex(): try to infer a
// tool name from the request's tools list by tool call index. Returns "" when
// inference is not possible.
func InferToolNameByIndex(request *convert.RequestData, toolIndex int) string {
	tools := request.Tools
	if len(tools) == 0 {
		return ""
	}
	if toolIndex < len(tools) {
		if name, ok := tools[toolIndex]["name"].(string); ok {
			if trimmed := strings.TrimSpace(name); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

// Streamer converts one upstream OpenAI chunk stream into Anthropic SSE
// events. Iterate Events() to consume the events; after iteration completes,
// Err() reports the termination error (only set when the upstream failed
// before producing output, or for explicit upstream error objects).
type Streamer struct {
	ctx             context.Context
	chunks          iter.Seq[openai.Chunk]
	req             *convert.RequestData
	inputTokens     int64
	thinkingEnabled bool
	dump            *dump.Session
	opts            *Options

	builder         *sse.Builder
	thinkParser     *parsers.ThinkTagParser
	heuristicParser *parsers.HeuristicToolParser
	toolArgAccum    map[int]string
	finishReason    string
	seenDone        bool
	usageInfo       *openai.Usage // latest raw usage chunk
	err             error
}

// NewStreamer creates a Streamer. thinkingEnabledHint is the isThinkingEnabled
// hint (the TS default of true applies when the hint is absent; Go's bool is
// always present, so the hint is used verbatim). dump and opts may be nil.
func NewStreamer(ctx context.Context, chunks iter.Seq[openai.Chunk], request *convert.RequestData, inputTokens int64, thinkingEnabledHint bool, session *dump.Session, opts *Options) *Streamer {
	return &Streamer{
		ctx:             ctx,
		chunks:          chunks,
		req:             request,
		inputTokens:     inputTokens,
		thinkingEnabled: thinkingEnabledHint,
		dump:            session,
		opts:            opts,
	}
}

// Events returns the Anthropic SSE event strings for this stream, one per
// yield. Events() is single-iteration (like a TS generator).
func (s *Streamer) Events() iter.Seq[string] {
	return func(yield func(string) bool) {
		s.run(yield)
	}
}

// Err returns the termination error after Events() has been fully iterated,
// or nil when the turn completed (gracefully or after an abort swallowed by
// the had-content path).
func (s *Streamer) Err() error { return s.err }

// run is the main generator body (port of streamOpenAIChatToAnthropicSse).
func (s *Streamer) run(yield func(string) bool) {
	messageID := "msg_" + uuidV4()
	s.builder = sse.NewBuilder(messageID, s.req.Model, s.inputTokens, nil)
	if s.opts != nil && s.opts.StartingBlockIndex != 0 {
		s.builder.SetNextIndex(s.opts.StartingBlockIndex)
	}
	s.thinkParser = parsers.NewThinkTagParser()
	s.heuristicParser = parsers.NewHeuristicToolParser()
	s.toolArgAccum = make(map[int]string)

	stop := false
	emit := func(ev string) {
		if stop {
			return
		}
		if !yield(ev) {
			stop = true
		}
	}

	if s.opts == nil || !s.opts.SkipMessageLifecycle {
		emit(s.builder.MessageStart())
	}

	loopErr := s.iterate(emit, &stop)
	if stop {
		return
	}
	if loopErr != nil {
		s.handleError(emit, loopErr)
		return
	}
	s.finalize(emit)
}

// iterate is the for-await loop (TS try block). Returns nil on graceful end
// (including a [DONE]-terminated stream), or the error to route through
// handleError.
func (s *Streamer) iterate(emit func(string), stop *bool) error {
	for chunk := range s.chunks {
		if *stop {
			return nil
		}
		// Read errors surface as a final chunk carrying the error
		// (equivalent to the TS iterUpstreamChunks throw).
		if chunk.Err != nil {
			return &UpstreamAbortedError{
				Message: "Upstream stream ended without a finish_reason (connection terminated mid-generation).",
				Subtype: "connection_closed",
			}
		}
		// OpenAI stream-end marker: [DONE] is an explicit completion signal
		// even when no finish_reason chunk arrived.
		if chunk.Done {
			s.seenDone = true
			break
		}
		if chunk.Usage != nil {
			s.usageInfo = chunk.Usage
			// TS spread semantics (`{...first, ...extract(chunk.usage)}`):
			// extractUsageInfo always returns all four buckets (0 for absent
			// fields), so the fresh extraction replaces the previous usage
			// wholesale — a later bare usage chunk wipes earlier cache
			// buckets. User ruling 2026-08-03 (follow TS).
			if u := ExtractUsageInfo(chunk.Usage); u != nil {
				s.builder.SetUsage(*u)
			}
		}
		// Detect upstream error objects embedded in the SSE stream.
		if chunk.Error != nil {
			code := int64(500)
			if chunk.Error.Code != nil {
				code = *chunk.Error.Code
			}
			msg := chunk.Error.Message
			if msg == "" {
				msg = fmt.Sprintf("upstream error %d", code)
			}
			return &UpstreamStreamError{Message: msg, Code: code}
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		choice := chunk.Choices[0]
		delta := choice.Delta
		if delta == nil {
			continue
		}
		if choice.FinishReason != nil && *choice.FinishReason != "" {
			s.finishReason = *choice.FinishReason
		}

		// Handle reasoning_content (thinking).
		if s.thinkingEnabled && delta.ReasoningContent != nil && *delta.ReasoningContent != "" {
			for _, ev := range s.builder.EnsureThinkingBlock() {
				emit(ev)
			}
			emit(s.builder.EmitThinkingDelta(*delta.ReasoningContent))
		}

		// Handle refusal delta — forwarded as text content so the client
		// sees it (refusal supersedes other delta fields in this chunk).
		if delta.Refusal != nil && *delta.Refusal != "" {
			s.ensureTextBlock(emit)
			emit(s.builder.TextDelta(*delta.Refusal))
			continue
		}

		// Handle text content.
		if delta.Content != nil && *delta.Content != "" {
			for _, part := range s.thinkParser.Feed(*delta.Content) {
				if part.Type == parsers.ThinkingContent {
					if !s.thinkingEnabled {
						continue
					}
					for _, ev := range s.builder.EnsureThinkingBlock() {
						emit(ev)
					}
					emit(s.builder.EmitThinkingDelta(part.Content))
				} else {
					filteredText, detectedTools := s.heuristicParser.Feed(part.Content)
					if filteredText != "" {
						s.ensureTextBlock(emit)
						emit(s.builder.TextDelta(filteredText))
					}
					for _, toolUse := range detectedTools {
						s.iterHeuristicToolUseSse(emit, toolUse)
					}
				}
			}
		}

		// Handle native tool calls (accumulate args for JSON repair later).
		if len(delta.ToolCalls) > 0 {
			heuristicText, heuristicTools := s.heuristicParser.Flush()
			if heuristicText != "" {
				s.ensureTextBlock(emit)
				emit(s.builder.TextDelta(heuristicText))
			}
			for _, toolUse := range heuristicTools {
				s.iterHeuristicToolUseSse(emit, toolUse)
			}
			for _, ev := range s.builder.CloseContentBlocks() {
				emit(ev)
			}
			for _, tc := range delta.ToolCalls {
				if tc.Function.Arguments != nil && *tc.Function.Arguments != "" {
					s.toolArgAccum[tc.Index] += *tc.Function.Arguments
				}
				s.processToolCall(emit, tc)
			}
		}
	}

	// No finish_reason chunk was seen: [DONE] means graceful completion
	// (implicit stop); otherwise the upstream terminated without completing.
	if s.finishReason == "" {
		if s.seenDone {
			s.finishReason = "stop"
		} else {
			return &UpstreamAbortedError{
				Message: "Upstream stream ended without a finish_reason (connection terminated mid-generation).",
				Subtype: "response_stalled",
			}
		}
	}
	return nil
}

// handleError is the TS catch block: flush parser buffers, close open blocks,
// and either append a Claude-standard incomplete notice (output existed) or
// surface the error (no output yet).
func (s *Streamer) handleError(emit func(string), loopErr error) {
	// Flush any buffered parser content BEFORE checking hadContent.
	if remaining := s.thinkParser.Flush(); remaining != nil {
		if remaining.Type == parsers.ThinkingContent {
			if s.thinkingEnabled {
				for _, ev := range s.builder.EnsureThinkingBlock() {
					emit(ev)
				}
				emit(s.builder.EmitThinkingDelta(remaining.Content))
			}
		} else {
			s.ensureTextBlock(emit)
			emit(s.builder.TextDelta(remaining.Content))
		}
	}

	heuristicText, heuristicTools := s.heuristicParser.Flush()
	if heuristicText != "" {
		s.ensureTextBlock(emit)
		emit(s.builder.TextDelta(heuristicText))
	}
	for _, toolUse := range heuristicTools {
		s.iterHeuristicToolUseSse(emit, toolUse)
	}

	// Close any content blocks opened so far so the downstream prefix is
	// well-formed up to the failure point.
	for _, ev := range s.builder.CloseAllBlocks() {
		emit(ev)
	}

	hadContent := s.builder.AccumulatedText() != "" ||
		s.builder.AccumulatedReasoning() != "" ||
		s.builder.SetTextStarted() ||
		s.builder.SetThinkingStarted() ||
		s.builder.HasEmittedToolBlock()

	if !hadContent {
		// No output emitted yet — surface the failure (the route layer
		// emits a top-level `event: error`).
		s.err = loopErr
		return
	}

	// Append the Claude-standard incomplete notice as a text content block
	// and complete the turn — the partial output + notice is preserved.
	notice := BuildIncompleteNotice(loopErr)
	s.ensureTextBlock(emit)
	emit(s.builder.TextDelta(notice))
	for _, ev := range s.builder.CloseContentBlocks() {
		emit(ev)
	}

	completion := s.completionEstimate()
	reason := s.finishReason
	if reason == "" {
		reason = "stop"
	}
	if s.opts == nil || !s.opts.SkipMessageLifecycle {
		emit(s.builder.MessageDelta(sse.MapStopReason(reason), &completion, nil))
		emit(s.builder.MessageStop())
	}

	// The upstream aborted, but the downstream turn completed gracefully.
	// Record the TRUE upstream outcome to the dump — gated on the downstream
	// NOT having initiated the disconnect.
	checkDownstreamAborted := s.opts != nil && s.opts.IsDownstreamAborted != nil
	if s.dump != nil && checkDownstreamAborted && !s.opts.IsDownstreamAborted() {
		s.dump.RecordUpstreamTermination(dump.UpstreamAbort, time.Now().UTC().Format(time.RFC3339))
	}

	// The upstream failed, but the downstream turn completed gracefully —
	// both error kinds are treated identically (TS catch block): the partial
	// output plus notice is the complete story and the generator ends
	// normally (Err() stays nil). Only the no-output path surfaces the error
	// from Err(). User ruling 2026-08-03 (follow TS).
	s.err = nil
}

// finalize is the normal post-loop flush (TS lines after the try/catch):
// parser flushes, orphaned tool resolution, empty-content placeholder,
// task-args flush, block close, and the terminal message events.
func (s *Streamer) finalize(emit func(string)) {
	if remaining := s.thinkParser.Flush(); remaining != nil {
		if remaining.Type == parsers.ThinkingContent {
			if s.thinkingEnabled {
				for _, ev := range s.builder.EnsureThinkingBlock() {
					emit(ev)
				}
				emit(s.builder.EmitThinkingDelta(remaining.Content))
			}
		} else {
			s.ensureTextBlock(emit)
			emit(s.builder.TextDelta(remaining.Content))
		}
	}

	heuristicText, heuristicTools := s.heuristicParser.Flush()
	if heuristicText != "" {
		s.ensureTextBlock(emit)
		emit(s.builder.TextDelta(heuristicText))
	}
	for _, toolUse := range heuristicTools {
		s.iterHeuristicToolUseSse(emit, toolUse)
	}

	// Resolve orphaned tool states — tool_calls that arrived without a
	// name/id: infer the name from the request tools list by index, or drop
	// the state (forcing stop_reason "stop" rather than a fake tool_use).
	hasOrphanedToolStates := false
	for _, toolIndex := range s.builder.ToolIndices() {
		if s.builder.ToolStarted(toolIndex) {
			continue
		}
		preStartArgs := s.builder.ToolPreStartArgs(toolIndex)
		name := s.builder.ToolName(toolIndex)
		if preStartArgs == "" && name == "" {
			continue
		}

		inferredName := InferToolNameByIndex(s.req, toolIndex)
		if inferredName != "" {
			resolvedID := s.builder.ToolID(toolIndex)
			if resolvedID == "" {
				resolvedID = "tool_" + uuidV4()
			}
			for _, ev := range s.builder.CloseContentBlocks() {
				emit(ev)
			}
			emit(s.builder.StartToolBlock(toolIndex, resolvedID, inferredName))
			// Attempt JSON repair on pre-start args.
			if raw := preStartArgs; raw != "" {
				repaired := repairTruncatedJson(raw)
				s.toolArgAccum[toolIndex] = repaired
				emit(s.builder.EmitToolDelta(toolIndex, repaired))
				s.builder.SetToolPreStartArgs(toolIndex, "")
			}
		} else {
			hasOrphanedToolStates = true
			s.builder.SetToolPreStartArgs(toolIndex, "")
			s.builder.DeleteToolState(toolIndex)
		}
	}

	// Ensure at least one content block exists.
	hasStartedTool := s.builder.HasEmittedToolBlock()
	hasContentBlocks := s.builder.TextIndex() != -1 || s.builder.ThinkingIndex() != -1 || hasStartedTool

	if !hasContentBlocks {
		s.ensureTextBlock(emit)
		emit(s.builder.TextDelta(" "))
	} else if !hasStartedTool &&
		strings.TrimSpace(s.builder.AccumulatedText()) == "" &&
		strings.TrimSpace(s.builder.AccumulatedReasoning()) != "" {
		s.ensureTextBlock(emit)
		emit(s.builder.TextDelta(" "))
	}

	// Flush task arg buffers with JSON repair fallback.
	for _, out := range s.builder.FlushTaskArgBuffers() {
		repaired := repairTruncatedJson(out.Args)
		s.toolArgAccum[out.ToolIndex] = repaired
		emit(s.builder.EmitToolDelta(out.ToolIndex, repaired))
	}

	// Close all blocks — thinking blocks get signature_delta before stop.
	for _, ev := range s.builder.CloseAllBlocks() {
		emit(ev)
	}

	completion := s.completionEstimate()

	effectiveFinishReason := s.finishReason
	if hasOrphanedToolStates {
		effectiveFinishReason = "stop"
	}

	// Detect matching stop sequence from the accumulated text (best-effort).
	detectedStopSeq := detectStopSequence(s.builder.AccumulatedText(), s.req.StopSequences)

	if s.opts == nil || !s.opts.SkipMessageLifecycle {
		var seqPtr *string
		if detectedStopSeq != "" {
			seqPtr = &detectedStopSeq
		}
		emit(s.builder.MessageDelta(sse.MapStopReason(effectiveFinishReason), &completion, seqPtr))
		emit(s.builder.MessageStop())
	}
}

// ensureTextBlock emits the thinking→text switch, following TS exactly
// (Builder.EnsureTextBlock): an open thinking block is stopped WITHOUT a
// signature_delta — the signature only appears when content blocks close with
// thinking still open (CloseContentBlocks / CloseAllBlocks). Verified against
// the TS reference: even a pure-thinking finalize emits no signature (the " "
// placeholder-text branch closes the thinking block unsigned). User ruling
// 2026-08-03 (third confirmation, follow TS): no signature on the switch.
func (s *Streamer) ensureTextBlock(emit func(string)) {
	for _, ev := range s.builder.EnsureTextBlock() {
		emit(ev)
	}
}

// iterHeuristicToolUseSse emits a complete tool_use block for a
// heuristic-parsed tool call (port of iterHeuristicToolUseSse in stream.ts).
// Task tool inputs get run_in_background forced to false.
func (s *Streamer) iterHeuristicToolUseSse(emit func(string), toolUse map[string]any) {
	if name, _ := toolUse["name"].(string); name == "Task" {
		if input, ok := toolUse["input"].(map[string]any); ok && input != nil {
			if input["run_in_background"] != false {
				input["run_in_background"] = false
			}
		}
	}
	for _, ev := range s.builder.CloseContentBlocks() {
		emit(ev)
	}
	blockIdx := s.builder.AllocateIndex()
	emit(s.builder.ContentBlockStart(blockIdx, "tool_use", map[string]any{
		"id":   toolUse["id"],
		"name": toolUse["name"],
	}))
	// Canonical (sorted-key) serialization keeps heuristic tool inputs
	// deterministic; clients parse JSON without caring about key order.
	input := "{}"
	if raw := toolUse["input"]; raw != nil {
		if s, err := convert.CanonicalJSONStringify(raw); err == nil {
			input = s
		}
	}
	emit(s.builder.ContentBlockDelta(blockIdx, "input_json_delta", input))
	emit(s.builder.ContentBlockStop(blockIdx))
}

// processToolCall is the port of the TS processToolCall generator: accumulates
// stream tool state (id/name), starts the tool block when the name is known,
// and emits input_json_delta fragments. Task tool fragments are routed through
// BufferTaskArgs so run_in_background is forced to false once the args parse
// as JSON (the final flush handles never-completed buffers).
func (s *Streamer) processToolCall(emit func(string), tc openai.ToolCallDelta) {
	tcIndex := tc.Index
	var tcID string
	if tc.ID != nil {
		tcID = *tc.ID
	}
	var fnName string
	if tc.Function.Name != nil {
		fnName = *tc.Function.Name
	}
	args := ""
	if tc.Function.Arguments != nil {
		args = *tc.Function.Arguments
	}

	if tcID != "" {
		s.builder.SetStreamToolID(tcIndex, tcID)
	}
	if tc.Function.Name != nil {
		s.builder.RegisterToolName(tcIndex, fnName)
	}

	resolvedID := s.builder.ToolID(tcIndex)
	if resolvedID == "" {
		resolvedID = tcID
	}
	if resolvedID == "" {
		resolvedID = "tool_" + uuidV4()
	}
	resolvedName := strings.TrimSpace(s.builder.ToolName(tcIndex))

	if !s.builder.ToolStarted(tcIndex) && resolvedName != "" {
		emit(s.builder.StartToolBlock(tcIndex, resolvedID, resolvedName))
		if pre := s.builder.ToolPreStartArgs(tcIndex); pre != "" {
			s.builder.SetToolPreStartArgs(tcIndex, "")
			emit(s.builder.EmitToolDelta(tcIndex, pre))
		}
	}

	if args == "" {
		return
	}

	if !s.builder.ToolStarted(tcIndex) {
		if resolvedName == "" {
			s.builder.SetToolPreStartArgs(tcIndex, s.builder.ToolPreStartArgs(tcIndex)+args)
			return
		}
	}

	if resolvedName == "Task" {
		if argsJSON := s.builder.BufferTaskArgs(tcIndex, args); argsJSON != nil {
			if canonical, err := convert.CanonicalJSONStringify(argsJSON); err == nil {
				emit(s.builder.EmitToolDelta(tcIndex, canonical))
				return
			}
		}
	}
	emit(s.builder.EmitToolDelta(tcIndex, args))
}

// completionEstimate prefers the upstream completion_tokens count; falls back
// to the builder's char/4 estimate when no (or zero) completion count arrived.
func (s *Streamer) completionEstimate() int64 {
	if s.usageInfo != nil && s.usageInfo.CompletionTokens > 0 {
		return s.usageInfo.CompletionTokens
	}
	return s.builder.EstimateOutputTokens()
}

// detectStopSequence mirrors the TS detectStopSequence(): returns the stop
// sequence the accumulated text ends with, or "" when none matches. Some
// upstream APIs strip stop sequences; detection only fires when they do not.
func detectStopSequence(text string, sequences []string) string {
	if len(sequences) == 0 || text == "" {
		return ""
	}
	for _, seq := range sequences {
		if seq != "" && strings.HasSuffix(text, seq) {
			return seq
		}
	}
	return ""
}

// repairTruncatedJson mirrors the TS repairTruncatedJson(): auto-close /
// repair a truncated JSON string by patching unbalanced braces and quotes.
// Returns the repaired JSON if successful, otherwise the trimmed original.
func repairTruncatedJson(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "{}"
	}
	if json.Valid([]byte(raw)) {
		return raw
	}
	trimmed := strings.TrimSpace(raw)
	result := trimmed

	// Count open/close braces and brackets, tracking string state (escaped
	// quotes do not toggle it).
	braceDepth := 0
	bracketDepth := 0
	inString := false
	prevChar := byte(0)
	for i := 0; i < len(result); i++ {
		ch := result[i]
		if ch == '"' && prevChar != '\\' {
			inString = !inString
		}
		if inString {
			prevChar = ch
			continue
		}
		switch ch {
		case '{':
			braceDepth++
		case '}':
			braceDepth--
		case '[':
			bracketDepth++
		case ']':
			bracketDepth--
		}
		prevChar = ch
	}
	// Close unpaired quotes, then brackets first, then braces.
	if inString {
		result += `"`
	}
	for bracketDepth > 0 {
		result += "]"
		bracketDepth--
	}
	for braceDepth > 0 {
		result += "}"
		braceDepth--
	}
	if json.Valid([]byte(result)) {
		return result
	}
	return trimmed
}

// uuidV4 returns a crypto/rand UUID v4-shaped string. rand.Read failure
// panics, mirroring TS randomUUID throwing on entropy failure.
func uuidV4() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("stream: crypto/rand unavailable: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

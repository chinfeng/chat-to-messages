// Package sse builds Anthropic-format SSE events for streaming responses,
// ported from chat-to-claude-code's src/sse/builder.ts. Typed event structs
// keep output byte-deterministic: field order mirrors the TS construction
// order so the bytes match the reference implementation.
package sse

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

// PING_EVENT is a pre-built `event: ping` SSE event string (Anthropic
// keep-alive). Heartbeat pings are emitted by the route layer at
// DEFAULT_PING_INTERVAL; the stream generator does NOT produce pings.
const PING_EVENT = "event: ping\ndata: {}\n\n"

// DEFAULT_PING_INTERVAL is the heartbeat interval for `event: ping`.
const DEFAULT_PING_INTERVAL = 15 * time.Second

// UsageInfo carries upstream usage buckets consumed by the builder. The
// three-bucket invariant: Anthropic input_tokens = prompt_tokens minus the
// cache buckets already accounted separately (cache_read + cache_creation),
// saturated at 0.
type UsageInfo struct {
	PromptTokens             int64
	CompletionTokens         int64
	CacheReadInputTokens     int64
	CacheCreationInputTokens int64
}

// Builder assembles Anthropic SSE events for one streaming turn.
type Builder struct {
	messageID            string
	model                string
	inputTokens          int64
	usage                *UsageInfo
	accumulatedText      []string
	accumulatedReasoning []string
	thinkingAccum        string
	signingSecret        string
	blocks               *contentBlockManager
}

// TaskArgsResult is one flushed task-args JSON payload paired with its tool
// index, mirroring the TS `[toolIndex, jsonString]` pairs.
type TaskArgsResult struct {
	ToolIndex int
	Args      string
}

// NewBuilder creates a Builder for a single turn. inputTokens is the
// fallback `input_tokens` estimate used until an upstream usage chunk arrives
// (or forever, when none does).
func NewBuilder(messageID, model string, inputTokens int64, usage *UsageInfo) *Builder {
	return &Builder{
		messageID:   messageID,
		model:       model,
		inputTokens: inputTokens,
		usage:       usage,
		// Per-request secret for signing thinking content — derived from the
		// message_id so signatures are deterministic for the same thinking
		// text within a turn, but opaque to the client. The derivation string
		// matches TS exactly: hex(sha256("ts-proxy-think-sign:" + messageID)).
		signingSecret: hexEncode(sha256.Sum256([]byte("ts-proxy-think-sign:" + messageID))),
		blocks:        newContentBlockManager(),
	}
}

// SetUsage replaces the upstream usage buckets (used when a usage chunk
// arrives after message_start was already emitted).
func (b *Builder) SetUsage(u UsageInfo) {
	b.usage = &u
}

// computeInputTokens applies the three-bucket invariant: prompt minus cache
// buckets, saturated at 0. Falls back to the constructor estimate when no
// usage has been supplied.
func (b *Builder) computeInputTokens() int64 {
	if b.usage != nil {
		if n := b.usage.PromptTokens - b.usage.CacheReadInputTokens - b.usage.CacheCreationInputTokens; n > 0 {
			return n
		}
		return 0
	}
	return b.inputTokens
}

// MessageStart emits the `message_start` event with a fixed `output_tokens`
// of 1 (no output has been produced yet).
func (b *Builder) MessageStart() string {
	usage := messageStartUsage{InputTokens: b.computeInputTokens(), OutputTokens: 1}
	if b.usage != nil {
		if b.usage.CacheReadInputTokens > 0 {
			usage.CacheReadInputTokens = b.usage.CacheReadInputTokens
		}
		if b.usage.CacheCreationInputTokens > 0 {
			usage.CacheCreationInputTokens = b.usage.CacheCreationInputTokens
		}
	}
	return FormatEvent("message_start", messageStartData{
		Type: "message_start",
		Message: messageStartMessage{
			ID:           b.messageID,
			Type:         "message",
			Role:         "assistant",
			Content:      []any{},
			Model:        b.model,
			StopReason:   nil,
			StopSequence: nil,
			Usage:        usage,
		},
	})
}

// MessageDelta emits the `message_delta` event. outputTokens nil → 0;
// stopSequence nil → null. Includes thinking_tokens and the cache buckets
// when they are non-zero.
func (b *Builder) MessageDelta(stopReason string, outputTokens *int64, stopSequence *string) string {
	var out int64
	if outputTokens != nil {
		out = *outputTokens
	}
	usage := messageDeltaUsage{InputTokens: b.computeInputTokens(), OutputTokens: out}
	if think := b.EstimateThinkingTokens(); think > 0 {
		usage.ThinkingTokens = think
	}
	if b.usage != nil {
		if b.usage.CacheReadInputTokens > 0 {
			usage.CacheReadInputTokens = b.usage.CacheReadInputTokens
		}
		if b.usage.CacheCreationInputTokens > 0 {
			usage.CacheCreationInputTokens = b.usage.CacheCreationInputTokens
		}
	}
	var seq any
	if stopSequence != nil {
		seq = *stopSequence
	}
	return FormatEvent("message_delta", messageDeltaData{
		Type:  "message_delta",
		Delta: messageDelta{StopReason: stopReason, StopSequence: seq},
		Usage: usage,
	})
}

// MessageStop emits the `message_stop` event.
func (b *Builder) MessageStop() string {
	return FormatEvent("message_stop", messageStopData{Type: "message_stop"})
}

// Ping emits an SSE ping heartbeat event (Anthropic keep-alive).
func (b *Builder) Ping() string {
	return PING_EVENT
}

// ContentBlockStart emits a `content_block_start` event for any block type.
// kwargs keys mirror the TS kwargs object (thinking, text, id, name, input,
// tool_use_id, content, status); the content_block is marshaled per type in
// TS field order.
func (b *Builder) ContentBlockStart(index int, blockType string, kwargs map[string]any) string {
	cb := contentBlock{Type: blockType}
	switch blockType {
	case "thinking":
		cb.Thinking, _ = kwargs["thinking"].(string)
	case "text":
		cb.Text, _ = kwargs["text"].(string)
	case "tool_use", "server_tool_use":
		cb.ID, _ = kwargs["id"].(string)
		cb.Name, _ = kwargs["name"].(string)
		if v, ok := kwargs["input"].(map[string]any); ok {
			cb.Input = &v
		}
	case "web_search_tool_result", "web_fetch_tool_result":
		cb.ToolUseID, _ = kwargs["tool_use_id"].(string)
		cb.Content, _ = kwargs["content"].([]map[string]any)
		cb.Status, _ = kwargs["status"].(string)
	}
	return FormatEvent("content_block_start", contentBlockStartData{
		Type:         "content_block_start",
		Index:        index,
		ContentBlock: cb,
	})
}

// ContentBlockDelta emits a `content_block_delta` event. deltaType is one of
// thinking_delta, signature_delta, text_delta, input_json_delta.
func (b *Builder) ContentBlockDelta(index int, deltaType, content string) string {
	delta := contentBlockDelta{Type: deltaType}
	switch deltaType {
	case "thinking_delta":
		delta.Thinking = content
	case "signature_delta":
		delta.Signature = content
	case "text_delta":
		delta.Text = content
	case "input_json_delta":
		delta.PartialJSON = content
	}
	return FormatEvent("content_block_delta", contentBlockDeltaData{
		Type:  "content_block_delta",
		Index: index,
		Delta: delta,
	})
}

// ContentBlockStop emits a `content_block_stop` event.
func (b *Builder) ContentBlockStop(index int) string {
	return FormatEvent("content_block_stop", contentBlockStopData{
		Type:  "content_block_stop",
		Index: index,
	})
}

// StartThinkingBlock starts a thinking block: allocates the block index,
// marks thinking started and resets the thinking accumulator.
func (b *Builder) StartThinkingBlock() string {
	b.blocks.thinkingIndex = b.blocks.allocateIndex()
	b.blocks.thinkingStarted = true
	b.thinkingAccum = ""
	return b.ContentBlockStart(b.blocks.thinkingIndex, "thinking", map[string]any{"thinking": ""})
}

// EmitThinkingDelta emits a thinking_delta and feeds the thinking
// accumulator (for the signature) and reasoning accumulation.
func (b *Builder) EmitThinkingDelta(content string) string {
	b.thinkingAccum += content
	b.accumulatedReasoning = append(b.accumulatedReasoning, content)
	return b.ContentBlockDelta(b.blocks.thinkingIndex, "thinking_delta", content)
}

// EmitSignatureDelta emits a `signature_delta` carrying the SHA-256 of the
// accumulated thinking text. Called immediately before StopThinkingBlock to
// produce the signature_delta + content_block_stop sequence Claude Code
// expects for multi-turn thinking verification.
func (b *Builder) EmitSignatureDelta() string {
	return b.ContentBlockDelta(b.blocks.thinkingIndex, "signature_delta", b.computeThinkingSignature())
}

// StopThinkingBlock stops the current thinking block.
func (b *Builder) StopThinkingBlock() string {
	b.blocks.thinkingStarted = false
	return b.ContentBlockStop(b.blocks.thinkingIndex)
}

// CloseThinkingWithSignature closes a thinking block with its signature
// delta as one atomic sequence: signature_delta + content_block_stop.
func (b *Builder) CloseThinkingWithSignature() []string {
	return []string{b.EmitSignatureDelta(), b.StopThinkingBlock()}
}

// StartTextBlock starts a text block.
func (b *Builder) StartTextBlock() string {
	b.blocks.textIndex = b.blocks.allocateIndex()
	b.blocks.textStarted = true
	return b.ContentBlockStart(b.blocks.textIndex, "text", nil)
}

// TextDelta emits a text_delta and accumulates the text for token
// estimation.
func (b *Builder) TextDelta(content string) string {
	b.accumulatedText = append(b.accumulatedText, content)
	return b.ContentBlockDelta(b.blocks.textIndex, "text_delta", content)
}

// StopTextBlock stops the current text block.
func (b *Builder) StopTextBlock() string {
	b.blocks.textStarted = false
	return b.ContentBlockStop(b.blocks.textIndex)
}

// StartToolBlock starts a tool_use block at the allocated block index and
// records the stream tool state (by toolIndex).
func (b *Builder) StartToolBlock(toolIndex int, toolID, name string) string {
	blockIdx := b.blocks.allocateIndex()
	if state, ok := b.blocks.toolStates[toolIndex]; ok {
		state.blockIndex = blockIdx
		state.toolID = toolID
		state.started = true
	} else {
		b.blocks.setToolState(toolIndex, &toolCallState{
			blockIndex: blockIdx,
			toolID:     toolID,
			name:       name,
			started:    true,
		})
	}
	return b.ContentBlockStart(blockIdx, "tool_use", map[string]any{"id": toolID, "name": name})
}

// EmitToolDelta emits an input_json_delta for the tool block and accumulates
// the partial JSON.
func (b *Builder) EmitToolDelta(toolIndex int, partialJSON string) string {
	state := b.blocks.toolStates[toolIndex]
	state.contents = append(state.contents, partialJSON)
	return b.ContentBlockDelta(state.blockIndex, "input_json_delta", partialJSON)
}

// StopToolBlock stops the tool block for the stream tool index.
func (b *Builder) StopToolBlock(toolIndex int) string {
	return b.ContentBlockStop(b.blocks.toolStates[toolIndex].blockIndex)
}

// EnsureThinkingBlock yields a stop for an open text block, then a thinking
// block start when none is open.
func (b *Builder) EnsureThinkingBlock() []string {
	var events []string
	if b.blocks.textStarted {
		events = append(events, b.StopTextBlock())
	}
	if !b.blocks.thinkingStarted {
		events = append(events, b.StartThinkingBlock())
	}
	return events
}

// EnsureTextBlock yields a stop for an open thinking block (following TS:
// no signature_delta on the switch — the signature is only emitted when
// content blocks close), then a text block start when none is open.
func (b *Builder) EnsureTextBlock() []string {
	var events []string
	if b.blocks.thinkingStarted {
		events = append(events, b.StopThinkingBlock())
	}
	if !b.blocks.textStarted {
		events = append(events, b.StartTextBlock())
	}
	return events
}

// CloseContentBlocks closes open thinking (signature_delta first) and text
// blocks.
func (b *Builder) CloseContentBlocks() []string {
	var events []string
	if b.blocks.thinkingStarted {
		events = append(events, b.EmitSignatureDelta(), b.StopThinkingBlock())
	}
	if b.blocks.textStarted {
		events = append(events, b.StopTextBlock())
	}
	return events
}

// CloseAllBlocks closes all content blocks then every started tool block,
// in tool start order.
func (b *Builder) CloseAllBlocks() []string {
	var events []string
	events = append(events, b.CloseContentBlocks()...)
	for _, toolIndex := range b.blocks.toolOrder {
		if state := b.blocks.toolStates[toolIndex]; state.started {
			events = append(events, b.StopToolBlock(toolIndex))
		}
	}
	return events
}

// EmitServerToolUse emits a complete server_tool_use content block
// (non-streaming — all data at once).
func (b *Builder) EmitServerToolUse(toolID, toolName string, input map[string]any) []string {
	index := b.blocks.allocateIndex()
	return []string{
		b.ContentBlockStart(index, "server_tool_use", map[string]any{"id": toolID, "name": toolName, "input": input}),
		b.ContentBlockStop(index),
	}
}

// EmitWebSearchToolResult emits a complete web_search_tool_result content
// block (non-streaming).
func (b *Builder) EmitWebSearchToolResult(toolUseID string, content []map[string]any, status string) []string {
	index := b.blocks.allocateIndex()
	return []string{
		b.ContentBlockStart(index, "web_search_tool_result", map[string]any{"tool_use_id": toolUseID, "content": content, "status": status}),
		b.ContentBlockStop(index),
	}
}

// EmitWebFetchToolResult emits a complete web_fetch_tool_result content
// block (non-streaming).
func (b *Builder) EmitWebFetchToolResult(toolUseID string, content []map[string]any, status string) []string {
	index := b.blocks.allocateIndex()
	return []string{
		b.ContentBlockStart(index, "web_fetch_tool_result", map[string]any{"tool_use_id": toolUseID, "content": content, "status": status}),
		b.ContentBlockStop(index),
	}
}

// EmitTopLevelError emits a top-level `event: error` with the given error
// type (defaults to api_error).
func (b *Builder) EmitTopLevelError(message, errorType string) string {
	if errorType == "" {
		errorType = "api_error"
	}
	return FormatEvent("error", errorEventData{
		Type:  "error",
		Error: errorEventError{Type: errorType, Message: message},
	})
}

// AddThinkingText feeds accumulated reasoning text (used for the signature
// and token estimation). It does not emit events.
func (b *Builder) AddThinkingText(text string) {
	b.thinkingAccum += text
	b.accumulatedReasoning = append(b.accumulatedReasoning, text)
}

// AccumulatedText returns the joined text deltas.
func (b *Builder) AccumulatedText() string {
	return strings.Join(b.accumulatedText, "")
}

// AccumulatedReasoning returns the joined thinking/reasoning text.
func (b *Builder) AccumulatedReasoning() string {
	return strings.Join(b.accumulatedReasoning, "")
}

// EstimateOutputTokens estimates output tokens with the TS char/4 heuristic:
// text + reasoning + per-tool (name + contents + 15) + 4 per started/active
// block.
func (b *Builder) EstimateOutputTokens() int64 {
	accText := b.AccumulatedText()
	accReasoning := b.AccumulatedReasoning()
	textTokens := ceilDiv4(estChars(accText))
	reasoningTokens := ceilDiv4(estChars(accReasoning))
	var toolTokens int64
	startedToolCount := int64(0)
	for _, state := range b.blocks.toolStates {
		toolTokens += ceilDiv4(estChars(state.name))
		toolTokens += ceilDiv4(estChars(strings.Join(state.contents, "")))
		toolTokens += 15
		if state.started {
			startedToolCount++
		}
	}
	blockCount := int64(0)
	if accReasoning != "" {
		blockCount++
	}
	if accText != "" {
		blockCount++
	}
	blockCount += startedToolCount
	return textTokens + reasoningTokens + toolTokens + blockCount*4
}

// EstimateThinkingTokens estimates the tokens consumed by thinking content
// only (same char/4 heuristic) — reported as usage.thinking_tokens in
// message_delta.
func (b *Builder) EstimateThinkingTokens() int64 {
	return ceilDiv4(estChars(b.AccumulatedReasoning()))
}

// NextIndex returns the next free content block index.
func (b *Builder) NextIndex() int {
	return b.blocks.nextIndex
}

// SetNextIndex shifts the next content block index (used by the agentic
// loop to continue a prior stream's index space).
func (b *Builder) SetNextIndex(n int) {
	b.blocks.nextIndex = n
}

// SetThinkingStarted reports whether a thinking block is open (hadContent
// determination for the stream package).
func (b *Builder) SetThinkingStarted() bool {
	return b.blocks.thinkingStarted
}

// SetTextStarted reports whether a text block is open (hadContent
// determination for the stream package).
func (b *Builder) SetTextStarted() bool {
	return b.blocks.textStarted
}

// SetStreamToolID records the stream tool id for a tool index (empty is
// ignored, matching TS).
func (b *Builder) SetStreamToolID(index int, toolID string) {
	if toolID == "" {
		return
	}
	b.blocks.ensureToolState(index).toolID = toolID
}

// RegisterToolName accumulates the tool name progressively: takes the new
// name when it extends the previous one, otherwise appends (matching TS).
func (b *Builder) RegisterToolName(index int, name string) {
	if _, ok := b.blocks.toolStates[index]; !ok {
		b.blocks.setToolState(index, &toolCallState{name: name})
		return
	}
	state := b.blocks.toolStates[index]
	prev := state.name
	if prev == "" || strings.HasPrefix(name, prev) {
		state.name = name
	} else if !strings.HasPrefix(prev, name) {
		state.name = prev + name
	}
}

// BufferTaskArgs buffers a task-args fragment until the buffer parses as
// JSON; on success the parsed args are returned with run_in_background
// forced to false and the buffer is cleared. Returns nil while the buffer is
// still incomplete or after args were already emitted.
func (b *Builder) BufferTaskArgs(index int, args string) map[string]any {
	state := b.blocks.toolStates[index]
	if state == nil || state.taskArgsEmitted {
		return nil
	}
	state.taskArgBuffer += args
	var argsJSON map[string]any
	if err := json.Unmarshal([]byte(state.taskArgBuffer), &argsJSON); err != nil {
		return nil
	}
	normalizeTaskRunInBackground(argsJSON)
	state.taskArgsEmitted = true
	state.taskArgBuffer = ""
	return argsJSON
}

// HasEmittedToolBlock reports whether any tool block has been started.
func (b *Builder) HasEmittedToolBlock() bool {
	for _, state := range b.blocks.toolStates {
		if state.started {
			return true
		}
	}
	return false
}

// FlushTaskArgBuffers drains all buffered task args: valid JSON gets
// run_in_background forced to false and is re-serialized; invalid JSON emits
// a sha256-prefix warning to stderr and is replaced with "{}". Returns
// (toolIndex, argsJSON) pairs in tool start order.
func (b *Builder) FlushTaskArgBuffers() []TaskArgsResult {
	var results []TaskArgsResult
	for _, toolIndex := range b.blocks.toolOrder {
		state := b.blocks.toolStates[toolIndex]
		if state.taskArgBuffer == "" || state.taskArgsEmitted {
			continue
		}
		out := "{}"
		var argsJSON map[string]any
		if err := json.Unmarshal([]byte(state.taskArgBuffer), &argsJSON); err == nil {
			normalizeTaskRunInBackground(argsJSON)
			if data, err := json.Marshal(argsJSON); err == nil {
				out = string(data)
			}
		} else {
			sum := sha256.Sum256([]byte(state.taskArgBuffer))
			id := state.toolID
			if id == "" {
				id = "unknown"
			}
			fmt.Fprintf(os.Stderr, "Task args invalid JSON (id=%s len=%d buffer_sha256_prefix=%s)\n",
				id, len(state.taskArgBuffer), hex.EncodeToString(sum[:16]))
		}
		state.taskArgsEmitted = true
		state.taskArgBuffer = ""
		results = append(results, TaskArgsResult{ToolIndex: toolIndex, Args: out})
	}
	return results
}

// ToolPreStartArgs returns the buffered pre-start args for a tool index.
func (b *Builder) ToolPreStartArgs(index int) string {
	if state := b.blocks.toolStates[index]; state != nil {
		return state.preStartArgs
	}
	return ""
}

// SetToolPreStartArgs sets (or clears) the buffered pre-start args for a
// tool index.
func (b *Builder) SetToolPreStartArgs(index int, args string) {
	b.blocks.ensureToolState(index).preStartArgs = args
}

// DeleteToolState removes a tool state (used when an orphaned tool state is
// abandoned).
func (b *Builder) DeleteToolState(index int) {
	if _, ok := b.blocks.toolStates[index]; !ok {
		return
	}
	delete(b.blocks.toolStates, index)
	for i, t := range b.blocks.toolOrder {
		if t == index {
			b.blocks.toolOrder = append(b.blocks.toolOrder[:i], b.blocks.toolOrder[i+1:]...)
			break
		}
	}
}

// computeThinkingSignature hashes the per-request secret then the
// accumulated thinking text (same update order as TS).
func (b *Builder) computeThinkingSignature() string {
	h := sha256.New()
	h.Write([]byte(b.signingSecret))
	h.Write([]byte(b.thinkingAccum))
	return hex.EncodeToString(h.Sum(nil))
}

func hexEncode(sum [32]byte) string {
	return hex.EncodeToString(sum[:])
}

// estChars counts characters as TS `.length` does (UTF-16 code units
// approximating runes for the BMP).
func estChars(s string) int64 {
	return int64(utf8.RuneCountInString(s))
}

// ceilDiv4 is ceil(n/4) via integer arithmetic (n >= 0).
func ceilDiv4(n int64) int64 {
	return (n + 3) / 4
}

// normalizeTaskRunInBackground forces run_in_background to false unless it
// is already false.
func normalizeTaskRunInBackground(argsJSON map[string]any) {
	if argsJSON["run_in_background"] != false {
		argsJSON["run_in_background"] = false
	}
}

// contentBlockManager tracks content block indices and tool states for one
// builder (port of TS ContentBlockManager). thinking/text/tool indices are
// mutually exclusive per block position.
type contentBlockManager struct {
	nextIndex       int
	thinkingIndex   int
	textIndex       int
	thinkingStarted bool
	textStarted     bool
	toolStates      map[int]*toolCallState
	toolOrder       []int // insertion order, for deterministic iteration
}

func newContentBlockManager() *contentBlockManager {
	return &contentBlockManager{
		thinkingIndex: -1,
		textIndex:     -1,
		toolStates:    make(map[int]*toolCallState),
	}
}

func (m *contentBlockManager) allocateIndex() int {
	idx := m.nextIndex
	m.nextIndex++
	return idx
}

func (m *contentBlockManager) ensureToolState(index int) *toolCallState {
	if state, ok := m.toolStates[index]; ok {
		return state
	}
	state := &toolCallState{}
	m.setToolState(index, state)
	return state
}

// setToolState inserts a state, recording insertion order for deterministic
// iteration.
func (m *contentBlockManager) setToolState(index int, state *toolCallState) {
	m.toolStates[index] = state
	m.toolOrder = append(m.toolOrder, index)
}

// toolCallState holds the state of one stream tool call (port of TS
// ToolCallState).
type toolCallState struct {
	blockIndex      int
	toolID          string
	name            string
	contents        []string
	started         bool
	taskArgBuffer   string
	taskArgsEmitted bool
	preStartArgs    string
}

// ---- Typed event payloads (field order = TS construction order) ----

type messageStartData struct {
	Type    string              `json:"type"`
	Message messageStartMessage `json:"message"`
}

type messageStartMessage struct {
	ID           string            `json:"id"`
	Type         string            `json:"type"`
	Role         string            `json:"role"`
	Content      []any             `json:"content"`
	Model        string            `json:"model"`
	StopReason   any               `json:"stop_reason"`
	StopSequence any               `json:"stop_sequence"`
	Usage        messageStartUsage `json:"usage"`
}

type messageStartUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens,omitempty"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens,omitempty"`
}

type messageDeltaData struct {
	Type  string            `json:"type"`
	Delta messageDelta      `json:"delta"`
	Usage messageDeltaUsage `json:"usage"`
}

type messageDelta struct {
	StopReason   string `json:"stop_reason"`
	StopSequence any    `json:"stop_sequence"`
}

type messageDeltaUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	ThinkingTokens           int64 `json:"thinking_tokens,omitempty"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens,omitempty"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens,omitempty"`
}

type messageStopData struct {
	Type string `json:"type"`
}

type contentBlockStartData struct {
	Type         string       `json:"type"`
	Index        int          `json:"index"`
	ContentBlock contentBlock `json:"content_block"`
}

// contentBlock is a tagged union of content block types in a single struct
// (fields in TS order). MarshalJSON emits per-type key sets so output is
// byte-identical to the TS builder.
type contentBlock struct {
	Type      string           `json:"type"`
	Thinking  string           `json:"thinking"`
	Text      string           `json:"text"`
	ID        string           `json:"id"`
	Name      string           `json:"name"`
	Input     *map[string]any  `json:"input"`
	ToolUseID string           `json:"tool_use_id"`
	Content   []map[string]any `json:"content"`
	Status    string           `json:"status"`
}

func (c contentBlock) MarshalJSON() ([]byte, error) {
	switch c.Type {
	case "thinking":
		return json.Marshal(struct {
			Type     string `json:"type"`
			Thinking string `json:"thinking"`
		}{c.Type, c.Thinking})
	case "text":
		return json.Marshal(struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}{c.Type, c.Text})
	case "tool_use", "server_tool_use":
		input := c.Input
		if input == nil {
			empty := map[string]any{}
			input = &empty
		}
		return json.Marshal(struct {
			Type  string         `json:"type"`
			ID    string         `json:"id"`
			Name  string         `json:"name"`
			Input map[string]any `json:"input"`
		}{c.Type, c.ID, c.Name, *input})
	case "web_search_tool_result", "web_fetch_tool_result":
		out := struct {
			Type      string           `json:"type"`
			ToolUseID string           `json:"tool_use_id"`
			Content   []map[string]any `json:"content,omitempty"`
			Status    string           `json:"status,omitempty"`
		}{Type: c.Type, ToolUseID: c.ToolUseID, Content: c.Content}
		if c.Status == "error" {
			out.Status = "error"
		}
		return json.Marshal(out)
	}
	return json.Marshal(struct {
		Type string `json:"type"`
	}{c.Type})
}

type contentBlockDeltaData struct {
	Type  string            `json:"type"`
	Index int               `json:"index"`
	Delta contentBlockDelta `json:"delta"`
}

// contentBlockDelta is a tagged union of delta types in a single struct
// (fields in TS order). MarshalJSON emits only the field for the delta type.
type contentBlockDelta struct {
	Type        string `json:"type"`
	Thinking    string `json:"thinking"`
	Signature   string `json:"signature"`
	Text        string `json:"text"`
	PartialJSON string `json:"partial_json"`
}

func (d contentBlockDelta) MarshalJSON() ([]byte, error) {
	switch d.Type {
	case "thinking_delta":
		return json.Marshal(struct {
			Type     string `json:"type"`
			Thinking string `json:"thinking"`
		}{d.Type, d.Thinking})
	case "signature_delta":
		return json.Marshal(struct {
			Type      string `json:"type"`
			Signature string `json:"signature"`
		}{d.Type, d.Signature})
	case "text_delta":
		return json.Marshal(struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}{d.Type, d.Text})
	case "input_json_delta":
		return json.Marshal(struct {
			Type        string `json:"type"`
			PartialJSON string `json:"partial_json"`
		}{d.Type, d.PartialJSON})
	}
	return json.Marshal(struct {
		Type string `json:"type"`
	}{d.Type})
}

type contentBlockStopData struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
}

type errorEventData struct {
	Type  string          `json:"type"`
	Error errorEventError `json:"error"`
}

type errorEventError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

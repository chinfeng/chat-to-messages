// Package openai provides typed protocol structures for the OpenAI chat
// completions streaming API, ported from chat-to-claude-code's
// src/protocol/openai.ts, plus an SSE stream parser (IterSSEChunks).
package openai

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"strconv"
	"strings"
)

// Chunk is one parsed element of the OpenAI SSE stream. Done/Err are internal
// markers produced by IterSSEChunks, never present in upstream JSON.
type Chunk struct {
	Choices []Choice `json:"choices,omitempty"`
	Usage   *Usage   `json:"usage,omitempty"`
	Error   *Error   `json:"error,omitempty"`
	Done    bool     `json:"-"` // [DONE] 哨兵
	Err     error    `json:"-"` // 读错误（IterSSEChunks 产出）
}

type Choice struct {
	Delta        *Delta  `json:"delta,omitempty"`
	FinishReason *string `json:"finish_reason,omitempty"`
}

type Delta struct {
	Content          *string         `json:"content,omitempty"`
	ReasoningContent *string         `json:"reasoning_content,omitempty"`
	Refusal          *string         `json:"refusal,omitempty"`
	ToolCalls        []ToolCallDelta `json:"tool_calls,omitempty"`
}

type ToolCallDelta struct {
	Index    int              `json:"index"`
	ID       *string          `json:"id,omitempty"`
	Function ToolCallFunction `json:"function,omitempty"`
}

type ToolCallFunction struct {
	Name      *string `json:"name,omitempty"`
	Arguments *string `json:"arguments,omitempty"`
}

type Usage struct {
	PromptTokens             int64                `json:"prompt_tokens,omitempty"`
	CompletionTokens         int64                `json:"completion_tokens,omitempty"`
	CacheReadInputTokens     *int64               `json:"cache_read_input_tokens,omitempty"`
	CacheCreationInputTokens *int64               `json:"cache_creation_input_tokens,omitempty"`
	PromptTokensDetails      *PromptTokensDetails `json:"prompt_tokens_details,omitempty"`
}

type PromptTokensDetails struct {
	CachedTokens     *int64 `json:"cached_tokens,omitempty"`
	CacheWriteTokens *int64 `json:"cache_write_tokens,omitempty"`
}

type Error struct {
	Message string `json:"message,omitempty"`
	Code    *int64 `json:"code,omitempty"`
}

// UnmarshalJSON for Delta tolerates both `reasoning_content` (OpenAI/DeepSeek)
// and `reasoning` (GLM-family upstreams stream the thinking field under this
// bare name). Without this alias, the entire thinking trace is silently
// dropped on GLM upstreams and the downstream response loses all reasoning
// content. Both keys map to ReasoningContent; `reasoning_content` wins on
// collision (the OpenAI-standard field).
func (d *Delta) UnmarshalJSON(data []byte) error {
	type alias Delta
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*d = Delta(a)
	var raw struct {
		Reasoning *string `json:"reasoning,omitempty"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Reasoning != nil && *raw.Reasoning != "" && (d.ReasoningContent == nil || *d.ReasoningContent == "") {
		d.ReasoningContent = raw.Reasoning
	}
	return nil
}

// UnmarshalJSON tolerates both numeric and string error codes: OpenAI sends
// numbers, but OpenRouter / newapi / GLM-family upstreams emit string codes
// like "E429". A numeric string is converted to int64; a non-numeric string
// (and a missing or null code) yields nil — the stream layer then falls back
// to 500, mirroring the TS `typeof code === "number" ? code : 500` in
// stream.ts. Without this, a string code made the whole chunk fail JSON
// parsing and the error was silently dropped.
func (e *Error) UnmarshalJSON(data []byte) error {
	var raw struct {
		Message string          `json:"message"`
		Code    json.RawMessage `json:"code"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	e.Message = raw.Message
	e.Code = nil
	if len(raw.Code) > 0 {
		// Accept both `"code": 429` and `"code": "429"` (strip JSON quotes
		// for the string form); anything else parses to nil.
		if n, err := strconv.ParseInt(strings.Trim(string(raw.Code), `"`), 10, 64); err == nil {
			e.Code = &n
		}
	}
	return nil
}

const doneSentinel = "[DONE]"

// IterSSEChunks parses an OpenAI SSE stream: only "data:"-prefixed lines are
// processed; "[DONE]" yields Chunk{Done:true}; JSON parse failures are
// skipped; a read error yields one final Chunk{Err: err} and then stops.
// After [DONE] no further chunks are produced: a subsequent read error must
// not surface as Chunk{Err}, since the upstream response already completed.
// raw, when non-nil, accumulates the full original text (including non-data
// lines) so callers can dump the upstream stream verbatim; when raw is set,
// remaining text after [DONE] is still drained into it, but when raw is nil
// the iterator stops reading immediately after [DONE]. Line splitting accepts
// both \n and \r\n. A bufio.Reader is used instead of bufio.Scanner because
// Scanner truncates lines longer than 64KB.
func IterSSEChunks(ctx context.Context, r io.Reader, raw *strings.Builder) iter.Seq[Chunk] {
	return func(yield func(Chunk) bool) {
		br := bufio.NewReader(r)
		done := false // [DONE] 之后只累积 raw，不再产出任何 chunk（含 Err）
		for {
			line, err := br.ReadString('\n')
			if len(line) > 0 {
				if raw != nil {
					raw.WriteString(line)
				}
				if !done {
					if data, ok := parseDataLine(line); ok {
						if data == doneSentinel {
							if !yield(Chunk{Done: true}) {
								return
							}
							if raw == nil {
								return // 无 dump 需求：立即停止读取
							}
							done = true // 有 dump 需求：仅继续累积 raw
						} else {
							var c Chunk
							if json.Unmarshal([]byte(data), &c) == nil {
								if !yield(c) {
									return
								}
							}
						}
					}
				}
			}
			if err != nil {
				if !done && !errors.Is(err, io.EOF) {
					// 真实读错误：产出最后一个 chunk 后停止
					if !yield(Chunk{Err: err}) {
						return
					}
				}
				return
			}
		}
	}
}

// parseDataLine extracts the payload of an SSE "data:" line, trimming the
// line ending (CRLF/LF) and any whitespace around the value. ok is false for
// lines that are not data fields.
func parseDataLine(line string) (data string, ok bool) {
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "data:") {
		return "", false
	}
	return strings.TrimSpace(line[len("data:"):]), true
}

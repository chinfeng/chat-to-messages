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

const doneSentinel = "[DONE]"

// IterSSEChunks parses an OpenAI SSE stream: only "data:"-prefixed lines are
// processed; "[DONE]" yields Chunk{Done:true}; JSON parse failures are
// skipped; a read error yields one final Chunk{Err: err} and then stops.
// raw, when non-nil, accumulates the full original text (including non-data
// lines) so callers can dump the upstream stream verbatim. Line splitting
// accepts both \n and \r\n. A bufio.Reader is used instead of bufio.Scanner
// because Scanner truncates lines longer than 64KB.
func IterSSEChunks(ctx context.Context, r io.Reader, raw *strings.Builder) iter.Seq[Chunk] {
	return func(yield func(Chunk) bool) {
		br := bufio.NewReader(r)
		for {
			line, err := br.ReadString('\n')
			if len(line) > 0 {
				if raw != nil {
					raw.WriteString(line)
				}
				line = strings.TrimRight(line, "\r\n")
				if strings.HasPrefix(line, "data:") {
					data := strings.TrimSpace(line[len("data:"):])
					switch {
					case data == doneSentinel:
						if !yield(Chunk{Done: true}) {
							return
						}
					default:
						var c Chunk
						if json.Unmarshal([]byte(data), &c) != nil {
							continue // 非 JSON 内容（如 keepalive），跳过
						}
						if !yield(c) {
							return
						}
					}
				}
			}
			if err != nil {
				if !errors.Is(err, io.EOF) {
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

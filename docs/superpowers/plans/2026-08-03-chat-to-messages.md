# chat-to-messages (Go 迁移) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 chat-to-claude-code（TypeScript/Bun）完整迁移为 Go 实现 `chat-to-messages`，行为兼容（CLI 参数、SSE 事件序列、错误格式、dump 结构），内部彻底 Go 化。

**Architecture:** 分层包结构：`config`（CLI 解析/深度合并）→ `anthropic`/`openai`（协议 typed 类型）→ `sse`（事件构建器，typed struct 保证逐字节输出）→ `parsers`/`convert`（转换层）→ `servertool`/`dump`（工具与转储）→ `stream`（iter.Seq 流管线）→ `proxy`（HTTP 层 + agentic loop）→ `main`。每包独立测试周期，自底向上实现。

**Tech Stack:** Go 1.26，纯标准库（net/http、encoding/json、iter.Seq、crypto/sha256、crypto/rand）。无第三方依赖。

## Global Constraints

- **纯标准库**：禁止引入任何第三方 Go 模块。
- **go.mod**: `module chat-to-messages`、`go 1.26`。
- **JSON 数值保精度**：所有需原样往返的 JSON 解码（请求体、tool input、extra-params、上游 chunk 的 error 对象除外）必须用 `json.Decoder.UseNumber()`；禁止用 float64 承载透传数值。
- **SSE 逐字节输出**：sse 包用 typed struct（字段顺序 = TS 版插入顺序）构建事件，`data:` 行输出与 TS 版逐字节一致；上游请求体经 canonical 排序（Go map 编码天然排序，与 TS canonicalize 输出一致）。
- **行为参考**：TS 源码 `../chat-to-claude-code/src/**` 是行为规范；TS 测试 `../chat-to-claude-code/tests/**` 是测试向量来源。端口规则：TS 测试中每个 `test("...")` 案例 → Go 表驱动测试一行，期望值取 TS 文件中的字面量；TS 断言用 `toContain`/JSON.parse 处，Go 断言解析 `data:` JSON 后比较字段（语义相等）。
- **命名**：新项目一律 `chat-to-messages`（启动横幅、User-Agent、README）。
- **提交**：每任务一个 commit，`feat:`/`test:`/`docs:` 前缀 + `Co-Authored-By: Claude <noreply@anthropic.com>` 尾注。
- **错误类型**：`stream.UpstreamStreamError`（上游自报错误，不重试）与 `stream.UpstreamAbortedError`（连接中断，`Subtype` ∈ `connection_closed`/`response_stalled`，可重试）在 stream 包定义。
- **工作目录**：`C:\git\migrate-to-golang\chat-to-messages`（git 仓库已初始化，含设计文档）。

## File Structure

```
chat-to-messages/
├── go.mod                         # module chat-to-messages, go 1.26
├── .gitignore                     # 二进制、dump 目录等
├── main.go                        # 入口：config.Load → 横幅 → http.Server
├── Dockerfile                     # 多阶段构建（golang:1.26-alpine → alpine）
├── README.md / README-zh.md       # 新项目文档（仿 TS 版结构）
├── LICENSE                        # MIT
├── docs/superpowers/specs/2026-08-03-chat-to-messages-design.md  # 已提交
└── internal/
    ├── config/
    │   ├── config.go              # Config 结构 + Load(args) 迷你解析器
    │   ├── merge.go               # DeepMerge + ResolveModelExtra + GlobMatch
    │   └── config_test.go / merge_test.go
    ├── anthropic/
    │   ├── types.go               # ContentBlock(联合体+Extra) / ContentValue / Message / MessagesRequest
    │   └── types_test.go
    ├── openai/
    │   ├── types.go               # Chunk/Choice/Delta/ToolCallDelta/Usage/Error + IterSSEChunks
    │   └── types_test.go
    ├── sse/
    │   ├── builder.go             # Builder + 事件 typed struct + 块管理器
    │   ├── builder_test.go
    │   ├── errors.go              # MapStopReason/MapErrorType/BuildMidStreamErrorSse/BuildRetryableMidStreamErrorSse
    │   └── errors_test.go
    ├── parsers/
    │   ├── thinktag.go            # ThinkTagParser
    │   ├── heuristictool.go       # HeuristicToolParser
    │   ├── thinktag_test.go
    │   └── heuristictool_test.go
    ├── servertool/
    │   ├── tools.go               # web_search/web_fetch 执行 + 文本检测 + schema + 系统提示后缀
    │   └── tools_test.go
    ├── dump/
    │   ├── dump.go                # Session + 分类桶 + 终止优先级
    │   └── dump_test.go
    ├── convert/
    │   ├── converter.go           # ConvertMessages/ConvertTools/ConvertToolChoice/ConvertSystemPrompt/BuildBaseRequestBody + RequestData + AnthropicMessage
    │   ├── canonical.go           # FilterPrivateParams/Canonicalize/CanonicalJSONStringify/PrepareCanonicalBody
    │   ├── converter_test.go
    │   └── canonical_test.go
    ├── stream/
    │   ├── stream.go              # NewStreamer + Events()/Err() + 错误类型 + BuildIncompleteNotice + ExtractUsageInfo + InferToolNameByIndex
    │   └── stream_test.go
    └── proxy/
        ├── server.go              # NewHandler/路由/CORS/认证/透传/SSE 泵/dump 编排
        ├── server_test.go         # 集成测试（httptest）
        ├── agentic.go             # handleServerToolRequest（≤5 轮）
        └── agentic_test.go
```

## 接口契约（后续任务引用）

```go
// === internal/config ===
type Config struct {
    UpstreamBaseURL, UpstreamAPIKey, AuthToken string
    Port             int
    EnableThinking   bool
    DumpDir          string
    ModelOverrides   []ModelOverride
    ServerTools      ServerToolConfig
}
type ModelOverride struct { Pattern string; Extra map[string]any }
type ServerToolConfig struct {
    WebSearch, WebFetch bool
    WebSearchEngine     WebSearchEngine      // "brave" | "searxng"
    WebSearchAPIKey     string
    WebSearchBaseURL    string
    WebFetchAllowedDomains, WebFetchBlockedDomains []string
    WebFetchMaxContentTokens int
}
type WebSearchEngine string
func Load(args []string) *Config                       // args = os.Args[1:]
func GlobMatch(pattern, text string) bool
func ResolveModelExtra(model string, overrides []ModelOverride) map[string]any
func DeepMerge(target, source map[string]any) map[string]any   // 返回新对象，不修改入参

// === internal/anthropic ===
type ContentBlock struct {
    Type      string          `json:"type"`
    Text      string          `json:"text,omitempty"`
    Thinking  string          `json:"thinking,omitempty"`
    Signature string          `json:"signature,omitempty"`
    Data      string          `json:"data,omitempty"`
    ID        string          `json:"id,omitempty"`
    Name      string          `json:"name,omitempty"`
    Input     json.RawMessage `json:"input,omitempty"`
    Content   json.RawMessage `json:"content,omitempty"`   // tool_result 内容
    IsError   bool            `json:"is_error,omitempty"`
    ToolUseID string          `json:"tool_use_id,omitempty"`
    Status    string          `json:"status,omitempty"`
    Source    *Source         `json:"source,omitempty"`
    Citations json.RawMessage `json:"citations,omitempty"`
    Detail    string          `json:"detail,omitempty"`
    Title     string          `json:"title,omitempty"`
    Context   string          `json:"context,omitempty"`
    Extra     map[string]any  `json:"-"`   // UnmarshalJSON 捕获未知字段
}
func (b *ContentBlock) UnmarshalJSON(data []byte) error  // UseNumber；未知字段入 Extra
func (b *ContentBlock) InputValue() any                   // 解码 Input（UseNumber）；nil 时返回 map[string]any{}
func (b *ContentBlock) ContentValue() any                 // 解码 Content（UseNumber）

type Source struct {
    Type      string `json:"type"`
    MediaType string `json:"media_type,omitempty"`
    MimeType  string `json:"mime_type,omitempty"`
    Data      string `json:"data,omitempty"`
    URL       string `json:"url,omitempty"`
}

type ContentValue struct {        // string | []ContentBlock
    IsString bool
    Str      string
    Blocks   []ContentBlock
}
func (v *ContentValue) UnmarshalJSON(data []byte) error
func (v ContentValue) String() (string, bool)
func (v ContentValue) Blocks() ([]ContentBlock, bool)

type Message struct {
    Role             string
    Content          ContentValue
    ReasoningContent *string
}
func (m *Message) UnmarshalJSON(data []byte) error

type MessagesRequest struct {     // /v1/messages 请求体（UseNumber 解码）
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
func (r *MessagesRequest) UnmarshalJSON(data []byte) error

// === internal/openai ===
type Chunk struct {
    Choices []Choice      `json:"choices,omitempty"`
    Usage   *Usage        `json:"usage,omitempty"`
    Error   *Error        `json:"error,omitempty"`
    Done    bool          `json:"-"`   // [DONE] 哨兵
    Err     error         `json:"-"`   // 读错误（IterSSEChunks 产出）
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
    Index    int               `json:"index"`
    ID       *string           `json:"id,omitempty"`
    Function ToolCallFunction  `json:"function,omitempty"`
}
type ToolCallFunction struct {
    Name      *string `json:"name,omitempty"`
    Arguments *string `json:"arguments,omitempty"`
}
type Usage struct {
    PromptTokens            int64                 `json:"prompt_tokens,omitempty"`
    CompletionTokens        int64                 `json:"completion_tokens,omitempty"`
    CacheReadInputTokens    *int64                `json:"cache_read_input_tokens,omitempty"`
    CacheCreationInputTokens *int64               `json:"cache_creation_input_tokens,omitempty"`
    PromptTokensDetails     *PromptTokensDetails  `json:"prompt_tokens_details,omitempty"`
}
type PromptTokensDetails struct {
    CachedTokens     *int64 `json:"cached_tokens,omitempty"`
    CacheWriteTokens *int64 `json:"cache_write_tokens,omitempty"`
}
type Error struct {
    Message string `json:"message,omitempty"`
    Code    *int64 `json:"code,omitempty"`
}
// IterSSEChunks 解析 OpenAI SSE 流：只处理 "data: " 前缀行；"[DONE]" → Chunk{Done:true}；
// JSON 解析失败跳过；读错误 → 产出最后一个 Chunk{Err: 包装的 *stream.UpstreamAbortedError(connection_closed)}。
// raw 非 nil 时收集全部原始文本（dump 用）。行分割兼容 \n 与 \r\n。
func IterSSEChunks(ctx context.Context, r io.Reader, raw *strings.Builder) iter.Seq[Chunk]
// 注意：openai 包不 import stream 包（避免依赖环）——读错误先记入 Chunk.Err，
// 由 stream.Streamer 包装为 UpstreamAbortedError。Err 字段类型为 error。

// === internal/sse ===
type UsageInfo struct {
    PromptTokens, CompletionTokens int64
    CacheReadInputTokens, CacheCreationInputTokens int64
}
const PING_EVENT = "event: ping\ndata: {}\n\n"
const DEFAULT_PING_INTERVAL = 15 * time.Second
func MapStopReason(openaiReason string) string
func MapErrorType(code int) string
func BuildMidStreamErrorSse(message string) string
func BuildRetryableMidStreamErrorSse(message string) string
func FormatEvent(eventType string, data map[string]any) string

type Builder struct { /* message_id, model, input_tokens, usage, blocks(块管理器), 签名密钥, 累积文本 */ }
func NewBuilder(messageID, model string, inputTokens int64, usage *UsageInfo) *Builder
func (b *Builder) SetUsage(u UsageInfo)
func (b *Builder) MessageStart() string
func (b *Builder) MessageDelta(stopReason string, outputTokens *int64, stopSequence *string) string
func (b *Builder) MessageStop() string
func (b *Builder) ContentBlockStart(index int, blockType string, kwargs map[string]any) string
func (b *Builder) ContentBlockDelta(index int, deltaType, content string) string
func (b *Builder) ContentBlockStop(index int) string
func (b *Builder) StartThinkingBlock() string
func (b *Builder) StopThinkingBlock() string
func (b *Builder) EmitSignatureDelta() string
func (b *Builder) CloseThinkingWithSignature() []string
func (b *Builder) StartTextBlock() string
func (b *Builder) StopTextBlock() string
func (b *Builder) StartToolBlock(toolIndex int, toolID, name string) string
func (b *Builder) EmitToolDelta(toolIndex int, partialJSON string) string
func (b *Builder) StopToolBlock(toolIndex int) string
func (b *Builder) EnsureThinkingBlock() []string
func (b *Builder) EnsureTextBlock() []string
func (b *Builder) CloseContentBlocks() []string
func (b *Builder) CloseAllBlocks() []string
func (b *Builder) AddThinkingText(text string)
func (b *Builder) TextDelta(content string) string
func (b *Builder) EmitServerToolUse(toolID, toolName string, input map[string]any) []string
func (b *Builder) EmitWebSearchToolResult(toolUseID string, content []map[string]any, status string) []string
func (b *Builder) EmitWebFetchToolResult(toolUseID string, content []map[string]any, status string) []string
func (b *Builder) EmitTopLevelError(message, errorType string) string
func (b *Builder) AccumulatedText() string
func (b *Builder) AccumulatedReasoning() string
func (b *Builder) EstimateOutputTokens() int64
func (b *Builder) EstimateThinkingTokens() int64
func (b *Builder) NextIndex() int
func (b *Builder) SetNextIndex(n int)
func (b *Builder) SetThinkingStarted()/SetTextStarted()  // 供 block 状态查询（hadContent 判定用）——见 Task 9

// === internal/dump ===
type TerminationReason string
const (Completed TerminationReason = "completed"; ClientAbort = "client_abort";
      UpstreamTimeout = "upstream_timeout"; UpstreamError = "upstream_error"; UpstreamAbort = "upstream_abort")
type Termination struct { Reason TerminationReason; DisconnectTime string }
type ServerToolLogEntry struct {
    Tool, Timestamp, Input, Engine, RequestURL string
    RequestHeaders, ResponseHeaders map[string]string
    Status *int; ResultCount *int; DurationMs *int64
    Skipped bool; SkipReason, Error, ResponseBody string
}
type Session struct{ /* 见 Task 5 */ }
func NewSession(dir string) *Session          // dir=="" 时返回 noop 会话（所有方法空操作）
func (s *Session) WriteDownstreamRequest(headers map[string]string, datetime, body string)
func (s *Session) WriteUpstreamRequest(headers map[string]string, datetime, body string)
func (s *Session) WriteUpstreamResponse(headers map[string]string, status int, body string, termination *Termination)
func (s *Session) WriteDownstreamResponse(headers map[string]string, status int, body string, termination *Termination)
func (s *Session) SetTiming(ttfb, totalTime int64)
func (s *Session) LogServerTool(entry ServerToolLogEntry)
func (s *Session) RecordUpstreamTermination(reason TerminationReason, disconnectTime string)
func (s *Session) UpstreamTermination() *Termination
func (s *Session) Finish()

// === internal/parsers ===
type ContentType int
const (TextContent ContentType = iota; ThinkingContent)
type ContentChunk struct { Type ContentType; Content string }
type ThinkTagParser struct{ /* buffer, inThinkTag */ }
func NewThinkTagParser() *ThinkTagParser
func (p *ThinkTagParser) Feed(content string) []ContentChunk
func (p *ThinkTagParser) Flush() *ContentChunk
type HeuristicToolParser struct{ /* 状态机 + buffer + 当前工具 */ }
func NewHeuristicToolParser() *HeuristicToolParser
func (p *HeuristicToolParser) Feed(text string) (filtered string, tools []map[string]any)
func (p *HeuristicToolParser) Flush() (text string, tools []map[string]any)

// === internal/servertool ===
type LogFn func(dump.ServerToolLogEntry)
type WebSearchResult struct { URL, Title, Snippet, PageAge string }
type WebFetchResult struct { Content, URL string; StatusCode int; Title string }
type DetectedTextToolCall struct { Type string; Input map[string]any }
func ExecuteWebSearch(ctx context.Context, query string, cfg config.ServerToolConfig, log LogFn) []WebSearchResult
func ExecuteWebFetch(ctx context.Context, url string, cfg config.ServerToolConfig, log LogFn) WebFetchResult
func FormatWebSearchResultContent(results []WebSearchResult) []map[string]any
func FormatWebFetchResultContent(result WebFetchResult) []map[string]any
func DetectServerToolInText(text string) []DetectedTextToolCall
func StripToolUseFromText(text string) string
func IsServerToolType(t string) bool
func BuildServerToolFunctionSchema(toolType, toolName string) map[string]any
func BuildServerToolSystemPromptSuffix(serverTools []map[string]any) string

// === internal/convert ===
type ReasoningReplayMode int
const (ReplayDisabled ReasoningReplayMode = iota; ReplayThinkTags; ReplayReasoningContent)
type RequestData struct {
    Model         string
    Messages      []anthropic.Message
    System        any
    MaxTokens     any
    Temperature   any
    TopP          any
    StopSequences []string
    Tools         []map[string]any
    ToolChoice    any
    ServerTools   []map[string]any
}
type OpenAIConversionError struct{ Msg string }
func (e *OpenAIConversionError) Error() string
func ConvertMessages(messages []anthropic.Message, replay ReasoningReplayMode) []map[string]any
func ConvertTools(tools []map[string]any) []map[string]any
func ConvertToolChoice(tc any) any
func HasDisableParallelToolUse(tc any) bool
func ConvertSystemPrompt(system any) map[string]any
func BuildBaseRequestBody(req *RequestData, defaultMaxTokens any, replay ReasoningReplayMode) map[string]any
func FilterPrivateParams(v any) any
func Canonicalize(v any) any
func CanonicalJSONStringify(v any) (string, error)
func PrepareCanonicalBody(body map[string]any) map[string]any

// === internal/stream ===
type UpstreamStreamError struct { Code int64 }
func (e *UpstreamStreamError) Error() string
type UpstreamAbortedError struct { Subtype string }   // "connection_closed" | "response_stalled"
func (e *UpstreamAbortedError) Error() string
func BuildIncompleteNotice(err error) string
func ExtractUsageInfo(u *openai.Usage) *sse.UsageInfo
func InferToolNameByIndex(req *convert.RequestData, toolIndex int) string
type Options struct {
    SkipMessageLifecycle bool
    StartingBlockIndex   int
    IsDownstreamAborted  func() bool
}
type Streamer struct{ /* 见 Task 9 */ }
func NewStreamer(ctx context.Context, chunks iter.Seq[openai.Chunk], req *convert.RequestData,
    inputTokens int64, thinkingEnabled bool, d *dump.Session, opts *Options) *Streamer
func (s *Streamer) Events() iter.Seq[string]
func (s *Streamer) Err() error

// === internal/proxy ===
func NewHandler(cfg *config.Config) http.Handler
```

---

### Task 1: 脚手架 + config 包

**Files:**
- Create: `go.mod`, `.gitignore`
- Create: `internal/config/config.go`, `internal/config/merge.go`
- Test: `internal/config/config_test.go`, `internal/config/merge_test.go`

**Interfaces:** Produces: `config.Load(args []string) *Config`、`config.GlobMatch`、`config.ResolveModelExtra`、`config.DeepMerge`（签名见上文契约）。

- [ ] **Step 1: 创建 go.mod 与 .gitignore**

```bash
cd "C:\git\migrate-to-golang\chat-to-messages"
go mod init chat-to-messages
```

`.gitignore`：
```
/chat-to-messages
/chat-to-messages.exe
/dumps/
```

- [ ] **Step 2: 写失败的解析器测试**

`internal/config/config_test.go`（核心向量；完整向量集照抄 `../chat-to-claude-code/tests/config.test.ts` 的每个 `test()` 案例——其字面量就是期望值）：

```go
package config

import (
	"reflect"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	cfg := Load(nil)
	if cfg.UpstreamBaseURL != "https://api.openai.com/v1" { t.Errorf("UpstreamBaseURL = %q", cfg.UpstreamBaseURL) }
	if cfg.Port != 8082 { t.Errorf("Port = %d", cfg.Port) }
	if !cfg.EnableThinking { t.Error("EnableThinking should default true") }
	if cfg.ServerTools.WebSearchEngine != "brave" { t.Errorf("engine = %q", cfg.ServerTools.WebSearchEngine) }
	if cfg.ServerTools.WebFetchMaxContentTokens != 5000 { t.Errorf("max tokens = %d", cfg.ServerTools.WebFetchMaxContentTokens) }
}

func TestLoadArgForms(t *testing.T) {
	cfg := Load([]string{
		"--upstream-base-url", "https://example.com/v1",   // 空格分隔
		"--auth-token=secret",                             // = 分隔
		"--port", "9999",
		"--no-enable-thinking",                            // 否定式
		"--enable-web-search",                             // 纯开关
		"--enable-web-fetch=false",                        // = 假值
		"--web-fetch-allowed-domain", "a.com",
		"--web-fetch-allowed-domain=b.com",                // 可重复
		"--web-fetch-blocked-domain", "c.com",
		"--upstream-extra-params", `claude-*={"thinking":{"type":"enabled","budget_tokens":10000}}`,
	})
	if cfg.UpstreamBaseURL != "https://example.com/v1" { t.Errorf("base url = %q", cfg.UpstreamBaseURL) }
	if cfg.AuthToken != "secret" { t.Errorf("auth = %q", cfg.AuthToken) }
	if cfg.Port != 9999 { t.Errorf("port = %d", cfg.Port) }
	if cfg.EnableThinking { t.Error("thinking should be false") }
	if !cfg.ServerTools.WebSearch { t.Error("web search should be true") }
	if cfg.ServerTools.WebFetch { t.Error("web fetch should be false") }
	if !reflect.DeepEqual(cfg.ServerTools.WebFetchAllowedDomains, []string{"a.com", "b.com"}) { t.Error("allowed domains") }
	if !reflect.DeepEqual(cfg.ServerTools.WebFetchBlockedDomains, []string{"c.com"}) { t.Error("blocked domains") }
	if len(cfg.ModelOverrides) != 1 { t.Fatalf("overrides = %d", len(cfg.ModelOverrides)) }
	if cfg.ModelOverrides[0].Pattern != "claude-*" { t.Errorf("pattern = %q", cfg.ModelOverrides[0].Pattern) }
	if cfg.ModelOverrides[0].Extra["thinking"].(map[string]any)["budget_tokens"] != json.Number("10000") {
		t.Error("extra not parsed with UseNumber")
	}
}

func TestLoadSkipsInvalidExtraParams(t *testing.T) {
	cfg := Load([]string{
		"--upstream-extra-params", "no-equals-sign",
		"--upstream-extra-params", "pat=not-json",
		"--upstream-extra-params", "pat2=[1,2]",
	})
	if len(cfg.ModelOverrides) != 0 { t.Errorf("expected 0 overrides, got %d", len(cfg.ModelOverrides)) }
}

func TestGlobMatch(t *testing.T) {
	cases := []struct{ pattern, text string; want bool }{
		{"claude-sonnet-*", "claude-sonnet-4-20250514", true},
		{"claude-sonnet-*", "claude-opus-4", false},
		{"*", "anything", true},
		{"deepseek?", "deepseek1", true},
		{"deepseek?", "deepseek-r1", false},
	}
	for _, c := range cases {
		if got := GlobMatch(c.pattern, c.text); got != c.want {
			t.Errorf("GlobMatch(%q, %q) = %v, want %v", c.pattern, c.text, got, c.want)
		}
	}
}

func TestResolveModelExtraFirstMatchWins(t *testing.T) {
	overrides := []ModelOverride{
		{Pattern: "claude-sonnet-*", Extra: map[string]any{"a": 1}},
		{Pattern: "*", Extra: map[string]any{"b": 2}},
	}
	if got := ResolveModelExtra("claude-sonnet-4", overrides); got["a"] != 1 {
		t.Errorf("first match should win: %v", got)
	}
	if got := ResolveModelExtra("other-model", overrides); got["b"] != 2 {
		t.Errorf("catch-all should match: %v", got)
	}
	if got := ResolveModelExtra("x", nil); len(got) != 0 { t.Errorf("nil overrides: %v", got) }
}
```

`internal/config/merge_test.go`（完整向量另照抄 `../chat-to-claude-code/tests/config.test.ts` 中 deepMerge 各案例）：

```go
package config

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestDeepMergeBasics(t *testing.T) {
	// 常规合并：嵌套对象合并、数组替换、null 覆盖
	target := map[string]any{"a": 1, "nested": map[string]any{"x": 1, "y": 2}, "arr": []any{1, 2}}
	src := map[string]any{"a": 2, "nested": map[string]any{"y": 3}, "arr": []any{9}, "z": nil}
	got := DeepMerge(target, src)
	if got["a"] != 2 { t.Errorf("a = %v", got["a"]) }
	if got["nested"].(map[string]any)["x"] != 1 || got["nested"].(map[string]any)["y"] != 3 {
		t.Errorf("nested merge: %v", got["nested"])
	}
	if !reflect.DeepEqual(got["arr"], []any{9}) { t.Errorf("arr should be replaced: %v", got["arr"]) }
	if got["z"] != nil { t.Errorf("z should be nil: %v", got["z"]) }
}

func TestDeepMergeDeleteAndDefault(t *testing.T) {
	target := map[string]any{
		"temperature": 0.2,
		"thinking":    map[string]any{"type": "enabled", "budget_tokens": 20000},
		"user":        "existing",
		"seed":        1,
	}
	src := map[string]any{
		"$delete":  []any{"thinking.budget_tokens", "user", "seed", "missing.path"},
		"$default": map[string]any{"max_tokens": 4096, "thinking.budget_tokens": 10000},
		"temperature": 0.7,
	}
	got := DeepMerge(target, src)
	if got["temperature"] != 0.7 { t.Errorf("temperature = %v", got["temperature"]) }
	if got["max_tokens"] != 4096 { t.Errorf("max_tokens = %v", got["max_tokens"]) }
	if _, ok := got["user"]; ok { t.Error("user should be deleted") }
	if _, ok := got["seed"]; ok { t.Error("seed should be deleted") }
	th := got["thinking"].(map[string]any)
	if th["type"] != "enabled" { t.Errorf("thinking.type = %v", th["type"]) }
	// $default 的 thinking.budget_tokens 因 $delete 后缺失 → 应被填充
	if th["budget_tokens"] != 10000 { t.Errorf("thinking.budget_tokens = %v", th["budget_tokens"]) }
	// 入参不被修改
	if target["temperature"] != 0.2 { t.Error("target must not be mutated") }
}

func TestDeepMergeDefaultDoesNotOverwrite(t *testing.T) {
	target := map[string]any{"max_tokens": 2048}
	src := map[string]any{"$default": map[string]any{"max_tokens": 4096, "temperature": 0.7}}
	got := DeepMerge(target, src)
	if got["max_tokens"] != 2048 { t.Errorf("existing value must not be overwritten: %v", got["max_tokens"]) }
	if got["temperature"] != 0.7 { t.Errorf("missing should be set: %v", got["temperature"]) }
}

func TestDeepMergeJSONNumbers(t *testing.T) {
	var src map[string]any
	if err := json.Unmarshal([]byte(`{"$default":{"max_tokens":4096}}`), &src); err != nil { t.Fatal(err) }
	got := DeepMerge(map[string]any{}, src)
	if got["max_tokens"] != json.Number("4096") { t.Errorf("expected json.Number: %v (%T)", got["max_tokens"], got["max_tokens"]) }
}
```

- [ ] **Step 3: 运行确认失败**

Run: `go test ./internal/config/` — Expected: FAIL（包不存在/函数未定义）

- [ ] **Step 4: 实现 config.go**

端口 `../chat-to-claude-code/src/server/config.ts` 的 `parseArgs`（getArg/getBool/getMultiArg 语义）。要点：
- `Load(args []string) *Config`：遍历参数；`--name` 取下一个参数（`--x value`）或 `--x=` 前缀匹配；`getBool` 顺序：`--name` 存在→true，`--no-name` 存在→false，`--name=` 前缀→值 != "false"，否则 fallback；`getMultiArg` 收集所有出现。
- extra-params：`raw[0]=='='` 缺失 → 告警跳过；JSON 解析失败 → 告警跳过；非对象 → 告警跳过。JSON 解码用 `json.Decoder.UseNumber()`。
- `GlobMatch`：转义正则元字符（`regexp.QuoteMeta` 等价于 TS 的手动转义），`*`→`.*`、`?`→`.`，全串锚定。
- `ResolveModelExtra`：首个匹配返回，否则 `map[string]any{}`。
- `ServerToolConfig` 默认值按 Global Constraints 中 Config 表（brave / `https://api.search.brave.com` / 5000）。

- [ ] **Step 5: 实现 merge.go**

端口 `deepMerge`（config.ts:67-129）。结构：三阶段（分离 meta 键 → 常规深度合并 → `$default` → `$delete`）；点号路径助手 `pathGet/pathSet/pathDelete`（路径中间遇到非对象 → 跳过/覆盖为空对象）。**注意**：返回全新对象，绝不修改入参（`pathSet` 在需要时创建中间 map，但修改的是新 result 树——先浅拷贝 target，合并时嵌套路径在拷贝后的新 map 上操作）。

- [ ] **Step 6: 运行确认通过 + 补全向量**

Run: `go test ./internal/config/`
Expected: PASS。随后打开 `../chat-to-claude-code/tests/config.test.ts`，把其中每个 `test()`/`it()` 案例补充进对应的 Go 测试文件（期望值取其字面量），再次运行直到全部通过。

- [ ] **Step 7: 提交**

```bash
git add go.mod .gitignore internal/config
git commit -m "feat: config package with CLI parsing, glob and deep merge

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: anthropic 类型包

**Files:**
- Create: `internal/anthropic/types.go`
- Test: `internal/anthropic/types_test.go`

**Interfaces:** Consumes: 无（纯类型）。Produces: `anthropic.ContentBlock`/`ContentValue`/`Message`/`MessagesRequest`（签名见契约）。

- [ ] **Step 1: 写失败的测试**

```go
package anthropic

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func mustDecode[T any](t *testing.T, data string) T {
	t.Helper()
	var v T
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil { t.Fatal(err) }
	return v
}

func TestContentValueString(t *testing.T) {
	var m Message
	dec := json.NewDecoder(strings.NewReader(`{"role":"user","content":"hello"}`))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil { t.Fatal(err) }
	s, ok := m.Content.String()
	if !ok || s != "hello" { t.Fatalf("content = %q, %v", s, ok) }
}

func TestContentValueBlocks(t *testing.T) {
	m := mustDecode[Message](t, `{"role":"assistant","content":[
		{"type":"text","text":"hi"},
		{"type":"tool_use","id":"tu1","name":"read_file","input":{"path":"/x"}}
	]}`)
	blocks, ok := m.Content.Blocks()
	if !ok || len(blocks) != 2 { t.Fatalf("blocks = %d, %v", len(blocks), ok) }
	if blocks[0].Type != "text" || blocks[0].Text != "hi" { t.Errorf("block0 = %+v", blocks[0]) }
	if blocks[1].Type != "tool_use" || blocks[1].ID != "tu1" || blocks[1].Name != "read_file" {
		t.Errorf("block1 = %+v", blocks[1])
	}
	if v := blocks[1].InputValue(); !reflect.DeepEqual(v, map[string]any{"path": "/x"}) {
		t.Errorf("input = %v", v)
	}
}

func TestContentBlockExtraCapturesUnknown(t *testing.T) {
	b := mustDecode[ContentBlock](t, `{"type":"text","text":"hi","some_custom_attr":{"k":1},"another":true}`)
	if b.Extra["some_custom_attr"] == nil { t.Error("custom attr missing from Extra") }
	if b.Extra["another"] != true { t.Error("another missing") }
}

func TestContentBlockToolResult(t *testing.T) {
	b := mustDecode[ContentBlock](t, `{"type":"tool_result","tool_use_id":"tu1","is_error":true,
		"content":[{"type":"text","text":"failed"}]}`)
	if b.ToolUseID != "tu1" || !b.IsError { t.Errorf("tool_result fields: %+v", b) }
	v := b.ContentValue()
	arr, ok := v.([]any)
	if !ok || len(arr) != 1 { t.Fatalf("content = %T %v", v, v) }
	first := arr[0].(map[string]any)
	if first["type"] != "text" || first["text"] != "failed" { t.Errorf("first = %v", first) }
}

func TestContentBlockImageSource(t *testing.T) {
	b := mustDecode[ContentBlock](t, `{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}`)
	if b.Source == nil || b.Source.Type != "base64" || b.Source.MediaType != "image/png" || b.Source.Data != "AAAA" {
		t.Errorf("source = %+v", b.Source)
	}
}

func TestNumberFidelity(t *testing.T) {
	m := mustDecode[Message](t, `{"role":"assistant","content":[{"type":"tool_use","id":"t","name":"f","input":{"big":12345678901234567890,"x":1.5}}]}`)
	blocks, _ := m.Content.Blocks()
	in := blocks[0].InputValue().(map[string]any)
	if _, ok := in["big"].(json.Number); !ok {
		t.Errorf("big must be json.Number, got %T", in["big"])
	}
}

func TestMessagesRequestFull(t *testing.T) {
	req := mustDecode[MessagesRequest](t, `{
		"model":"m1",
		"messages":[{"role":"user","content":"hi"}],
		"system":"sys",
		"max_tokens":4096,
		"temperature":0.7,
		"top_p":1,
		"stop_sequences":["END"],
		"tools":[{"type":"function","name":"f"}],
		"tool_choice":{"type":"auto"},
		"server_tools":[{"type":"web_search_20250305","name":"web_search"}]
	}`)
	if req.Model != "m1" || len(req.Messages) != 1 { t.Fatalf("req = %+v", req) }
	if req.MaxTokens != json.Number("4096") { t.Errorf("max_tokens = %T %v", req.MaxTokens, req.MaxTokens) }
	if len(req.Tools) != 1 || req.Tools[0]["name"] != "f" { t.Errorf("tools = %v", req.Tools) }
	if len(req.ServerTools) != 1 { t.Errorf("server_tools = %v", req.ServerTools) }
	if req.StopSequences[0] != "END" { t.Errorf("stops = %v", req.StopSequences) }
	if req.System != "sys" { t.Errorf("system = %v", req.System) }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/anthropic/` — Expected: FAIL

- [ ] **Step 3: 实现 types.go**

- `ContentValue.UnmarshalJSON`：首字节判断——`"` → string；`[` → `[]ContentBlock`（逐块解码，UseNumber）；否则 error。
- `ContentBlock.UnmarshalJSON`：解码入 `map[string]any`（UseNumber）→ 映射到具名字段（type/text/thinking/signature/data/id/name/input/content/is_error/tool_use_id/status/source/citations/detail/title/context）→ 剩余键入 `Extra`。`input`/`content`/`citations` 以 `json.RawMessage` 原样保存。
- `InputValue`/`ContentValue`：对 RawMessage 用 `json.Decoder.UseNumber` 解码；nil/空 → `map[string]any{}` / nil。
- `Message.UnmarshalJSON`：role + content(ContentValue) + reasoning_content(*string)。
- `MessagesRequest.UnmarshalJSON`：整包 UseNumber 解码到 map，然后逐字段提取与转换：`messages` → `[]Message`（对每个元素 re-marshal 后调 Message.UnmarshalJSON，或直接类型断言后走同一路径）；`tools`/`server_tools` → `[]map[string]any`（`[]any` 断言转换）；`stop_sequences` → `[]string`；其余透传 `any`。
- 未出现的字段保持零值（TS 的 `undefined` ↔ Go 零值/nil 对等）。

- [ ] **Step 4: 运行确认通过**

Run: `go test ./internal/anthropic/` — Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/anthropic
git commit -m "feat: anthropic protocol types with content block union

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: openai 类型 + SSE 流解析

**Files:**
- Create: `internal/openai/types.go`
- Test: `internal/openai/types_test.go`

**Interfaces:** Consumes: 无。Produces: `openai.Chunk` 等类型 + `openai.IterSSEChunks(ctx, r, raw)`（签名见契约）。

- [ ] **Step 1: 写失败的测试**

```go
package openai

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestIterSSEChunksBasic(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\" there\"}}]}\n\ndata: [DONE]\n\n"
	var chunks []Chunk
	for c := range IterSSEChunks(t.Context(), strings.NewReader(body), nil) {
		chunks = append(chunks, c)
	}
	if len(chunks) != 3 { t.Fatalf("chunks = %d", len(chunks)) }
	if *chunks[0].Choices[0].Delta.Content != "hi" { t.Errorf("c0 = %+v", chunks[0]) }
	if !chunks[2].Done { t.Error("last should be [DONE] sentinel") }
}

func TestIterSSEChunksCRLFAndIgnoreNonData(t *testing.T) {
	body := ": keepalive comment\r\nevent: ping\r\ndata: {\"usage\":{\"prompt_tokens\":5}}\r\n\r\ndata: not-json\r\n\r\ndata: [DONE]\r\n\r\n"
	var chunks []Chunk
	for c := range IterSSEChunks(t.Context(), strings.NewReader(body), nil) {
		chunks = append(chunks, c)
	}
	if len(chunks) != 2 { t.Fatalf("chunks = %d: %+v", len(chunks), chunks) }
	if chunks[0].Usage == nil || chunks[0].Usage.PromptTokens != 5 { t.Errorf("usage = %+v", chunks[0].Usage) }
}

func TestIterSSEChunksSplitAcrossReads(t *testing.T) {
	// 分片读：data 行跨多个 Read
	reader := io.MultiReader(
		strings.NewReader("data: {\"choice"),
		strings.NewReader("s\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"),
		strings.NewReader("data: [DONE]\n\n"),
	)
	var chunks []Chunk
	for c := range IterSSEChunks(t.Context(), reader, nil) {
		chunks = append(chunks, c)
	}
	if len(chunks) != 2 || chunks[0].Choices == nil { t.Fatalf("chunks = %d: %+v", len(chunks), chunks) }
}

func TestIterSSEChunksRawCapture(t *testing.T) {
	var raw strings.Builder
	body := "data: {\"choices\":[]}\n\ndata: [DONE]\n\n"
	for range IterSSEChunks(t.Context(), strings.NewReader(body), &raw) {}
	if raw.String() != body { t.Errorf("raw = %q", raw.String()) }
}

type errReader struct{ n int }
func (r *errReader) Read(p []byte) (int, error) {
	if r.n > 0 { r.n--; return copy(p, "data: {\"choices\":[]}\n\n"), nil }
	return 0, errors.New("connection reset")
}

func TestIterSSEChunksReadError(t *testing.T) {
	var chunks []Chunk
	for c := range IterSSEChunks(t.Context(), &errReader{n: 1}, nil) {
		chunks = append(chunks, c)
	}
	if len(chunks) != 2 { t.Fatalf("chunks = %d", len(chunks)) }
	if chunks[1].Err == nil { t.Error("expected Err on last chunk") }
	if !strings.Contains(chunks[1].Err.Error(), "connection reset") { t.Errorf("err = %v", chunks[1].Err) }
}

func TestChunkErrorObject(t *testing.T) {
	var c Chunk
	if err := decodeChunk(&c, `{"error":{"message":"rate limited","code":429}}`); err != nil { t.Fatal(err) }
	if c.Error == nil || c.Error.Message != "rate limited" || *c.Error.Code != 429 { t.Errorf("err obj = %+v", c.Error) }
}

func TestChunkToolCallDelta(t *testing.T) {
	var c Chunk
	if err := decodeChunk(&c, `{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"f","arguments":"{\"a\":"}}],"finish_reason":null}}]}`); err != nil { t.Fatal(err) }
	tc := c.Choices[0].Delta.ToolCalls[0]
	if tc.Index != 0 || *tc.ID != "call_1" || *tc.Function.Name != "f" || *tc.Function.Arguments != `{"a":` {
		t.Errorf("tc = %+v", tc)
	}
}
```

辅助函数（测试文件内）：

```go
func decodeChunk(c *Chunk, data string) error {
	dec := json.NewDecoder(strings.NewReader(data))
	return dec.Decode(c)
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/openai/` — Expected: FAIL

- [ ] **Step 3: 实现 types.go**

- 类型定义按契约。`Chunk` 的 `Done`/`Err` 字段 `json:"-"`。
- `IterSSEChunks`：内部用 `bufio.Reader` 逐行读（`ReadString('\n')`，兼容 `\r\n`——`strings.TrimRight(line, "\r\n")`）；行不以 `data:` 开头跳过；`[DONE]` → `yield(Chunk{Done:true})`；JSON 解析失败跳过；读到错误 → `yield(Chunk{Err: err})` 后停止。`raw` 累积所有原始文本（含非 data 行——dump 需要完整上游文本，对应 TS `rawChunks.push(decoded)`）。
- 用 `bufio.Reader` 而非 `bufio.Scanner`（Scanner 有 64KB 行限制，长行会截断）。

- [ ] **Step 4: 运行确认通过**

Run: `go test ./internal/openai/` — Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/openai
git commit -m "feat: openai chunk types and SSE line parser

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: sse 事件构建器

**Files:**
- Create: `internal/sse/builder.go`
- Test: `internal/sse/builder_test.go`
- Create: `internal/sse/errors.go`
- Test: `internal/sse/errors_test.go`

**Interfaces:** Consumes: 无。Produces: `sse.Builder` 全套 + `PING_EVENT`/`DEFAULT_PING_INTERVAL`/`MapStopReason`/`MapErrorType`/`BuildMidStreamErrorSse`/`BuildRetryableMidStreamErrorSse`/`FormatEvent`/`UsageInfo`（签名见契约）。

- [ ] **Step 1: 写失败的测试**

`internal/sse/builder_test.go`（精确字符串断言——typed struct 保证输出确定性；字段顺序 = TS 版插入顺序）：

```go
package sse

import (
	"strings"
	"testing"
)

func TestFormatEvent(t *testing.T) {
	got := FormatEvent("ping", map[string]any{})
	want := "event: ping\ndata: {}\n\n"
	if got != want { t.Errorf("got %q want %q", got, want) }
}

func TestMessageStart(t *testing.T) {
	b := NewBuilder("msg_1", "model-x", 100, nil)
	got := b.MessageStart()
	want := "event: message_start\n" +
		`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"model-x","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":100,"output_tokens":1}}}` + "\n\n"
	if got != want { t.Errorf("got  %s\nwant %s", got, want) }
}

func TestMessageStartWithCacheUsage(t *testing.T) {
	b := NewBuilder("msg_1", "model-x", 0, &UsageInfo{PromptTokens: 100, CacheReadInputTokens: 30, CacheCreationInputTokens: 10})
	got := b.MessageStart()
	if !strings.Contains(got, `"input_tokens":60`) { t.Errorf("input_tokens should be prompt-cache: %s", got) }
	if !strings.Contains(got, `"cache_read_input_tokens":30`) { t.Errorf("missing cache_read: %s", got) }
	if !strings.Contains(got, `"cache_creation_input_tokens":10`) { t.Errorf("missing cache_creation: %s", got) }
	if !strings.Contains(got, `"output_tokens":1`) { t.Errorf("output_tokens: %s", got) }
}

func TestMessageDeltaUsage(t *testing.T) {
	b := NewBuilder("msg_1", "m", 10, nil)
	out := int64(42)
	got := b.MessageDelta("end_turn", &out, nil)
	want := "event: message_delta\n" +
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":10,"output_tokens":42}}` + "\n\n"
	if got != want { t.Errorf("got  %s\nwant %s", got, want) }
}

func TestMessageDeltaThinkingTokens(t *testing.T) {
	b := NewBuilder("msg_1", "m", 10, nil)
	b.AddThinkingText("abcd") // 4 chars → 1 token
	out := int64(10)
	got := b.MessageDelta("end_turn", &out, nil)
	if !strings.Contains(got, `"thinking_tokens":1`) { t.Errorf("missing thinking_tokens: %s", got) }
}

func TestThinkingBlockSequence(t *testing.T) {
	b := NewBuilder("msg_1", "m", 10, nil)
	var events []string
	events = append(events, b.EnsureThinkingBlock()...)
	events = append(events, b.ContentBlockDelta(b.NextIndex()-1, "thinking_delta", "let me think"))
	events = append(events, b.CloseContentBlocks()...)
	// [content_block_start thinking, content_block_delta thinking_delta, signature_delta, content_block_stop]
	if len(events) != 4 { t.Fatalf("events = %d: %v", len(events), events) }
	if !strings.HasPrefix(events[0], "event: content_block_start") || !strings.Contains(events[0], `"content_block":{"type":"thinking","thinking":""}`) {
		t.Errorf("start = %s", events[0])
	}
	if !strings.Contains(events[1], `"delta":{"type":"thinking_delta","thinking":"let me think"}`) { t.Errorf("delta = %s", events[1]) }
	if !strings.HasPrefix(events[2], "event: content_block_delta") || !strings.Contains(events[2], `"signature_delta"`) { t.Errorf("sig = %s", events[2]) }
	if events[3] != "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" { t.Errorf("stop = %s", events[3]) }
}

func TestSignatureDeterministic(t *testing.T) {
	b1 := NewBuilder("msg_same", "m", 0, nil)
	b2 := NewBuilder("msg_same", "m", 0, nil)
	b1.AddThinkingText("think hard")
	b2.AddThinkingText("think hard")
	sig1 := strings.Split(b1.EmitSignatureDelta(), "\n")[1]
	sig2 := strings.Split(b2.EmitSignatureDelta(), "\n")[1]
	if sig1 != sig2 { t.Errorf("signatures should match: %s vs %s", sig1, sig2) }
	b3 := NewBuilder("msg_diff", "m", 0, nil)
	b3.AddThinkingText("think hard")
	sig3 := strings.Split(b3.EmitSignatureDelta(), "\n")[1]
	if sig3 == sig1 { t.Error("different message_id must give different signature") }
}

func TestEnsureTextClosesThinking(t *testing.T) {
	b := NewBuilder("msg_1", "m", 0, nil)
	var events []string
	events = append(events, b.EnsureThinkingBlock()...)
	events = append(events, b.EnsureTextBlock()...)
	// thinking start, then (signature_delta + thinking stop) + text start
	if len(events) != 4 { t.Fatalf("events = %d", len(events)) }
	if !strings.Contains(events[1], "signature_delta") { t.Errorf("e1 = %s", events[1]) }
	if !strings.Contains(events[3], `"content_block":{"type":"text"`) { t.Errorf("e3 = %s", events[3]) }
}

func TestToolBlockAndCloseAll(t *testing.T) {
	b := NewBuilder("msg_1", "m", 0, nil)
	var events []string
	events = append(events, b.StartToolBlock(0, "tool_001", "read_file"))
	events = append(events, b.EmitToolDelta(0, `{"path":`))
	events = append(events, b.CloseAllBlocks()...)
	if len(events) != 3 { t.Fatalf("events = %d: %v", len(events), events) }
	if !strings.Contains(events[0], `"content_block":{"type":"tool_use","id":"tool_001","name":"read_file","input":{}` ) {
		t.Errorf("start = %s", events[0])
	}
	if !strings.Contains(events[1], `"delta":{"type":"input_json_delta","partial_json":"{\"path\":"` ) { t.Errorf("delta = %s", events[1]) }
}

func TestEstimateOutputTokens(t *testing.T) {
	b := NewBuilder("msg_1", "m", 0, nil)
	b.AddThinkingText(strings.Repeat("a", 8))  // 2 tokens
	b.TextDelta(strings.Repeat("b", 16))       // 4 tokens
	if got := b.EstimateOutputTokens(); got != 2+4+2*4 {
		t.Errorf("estimate = %d", got)
	}
}

func TestWebToolResultBlocks(t *testing.T) {
	b := NewBuilder("msg_1", "m", 0, nil)
	ev := b.EmitWebSearchToolResult("tu1", []map[string]any{{"type": "web_search_result", "url": "https://x.com", "title": "X"}}, "")
	if len(ev) != 2 { t.Fatalf("events = %d", len(ev)) }
	if !strings.Contains(ev[0], `"content_block":{"type":"web_search_tool_result","tool_use_id":"tu1","content":[{"type":"web_search_result","url":"https://x.com","title":"X"}]}`) {
		t.Errorf("start = %s", ev[0])
	}
}
```

`internal/sse/errors_test.go`：

```go
package sse

import (
	"strings"
	"testing"
)

func TestMapStopReason(t *testing.T) {
	cases := map[string]string{
		"stop": "end_turn", "length": "max_tokens", "tool_calls": "tool_use",
		"content_filter": "refusal", "unknown": "end_turn", "": "end_turn",
	}
	for in, want := range cases {
		if got := MapStopReason(in); got != want { t.Errorf("MapStopReason(%q) = %q", in, got) }
	}
}

func TestMapErrorType(t *testing.T) {
	if MapErrorType(429) != "overloaded_error" { t.Error("429") }
	if MapErrorType(529) != "overloaded_error" { t.Error("529") }
	if MapErrorType(400) != "invalid_request_error" { t.Error("400") }
	if MapErrorType(500) != "api_error" { t.Error("500") }
}

func TestBuildMidStreamErrorSse(t *testing.T) {
	got := BuildMidStreamErrorSse("boom")
	if !strings.Contains(got, "event: error\n") { t.Errorf("got %s", got) }
	if !strings.Contains(got, `"error":{"type":"stream_error","message":"boom"}`) { t.Errorf("got %s", got) }
	if strings.Contains(got, "overloaded_error") { t.Errorf("must not be retryable: %s", got) }
}

func TestBuildRetryableMidStreamErrorSse(t *testing.T) {
	got := BuildRetryableMidStreamErrorSse("boom")
	if !strings.Contains(got, `"message":"{\"type\":\"overloaded_error\"} boom"`) {
		t.Errorf("retryable prefix missing: %s", got)
	}
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/sse/` — Expected: FAIL

- [ ] **Step 3: 实现 builder.go**

- **typed 事件结构**（字段顺序 = TS 版构造顺序，全部 omitempty 按需）：

```go
type messageStartData struct {
	Type    string        `json:"type"`
	Message messageStartMessage `json:"message"`
}
type messageStartMessage struct {
	ID           string      `json:"id"`
	Type         string      `json:"type"`
	Role         string      `json:"role"`
	Content      []any       `json:"content"`
	Model        string      `json:"model"`
	StopReason   any         `json:"stop_reason"`
	StopSequence any         `json:"stop_sequence"`
	Usage        messageStartUsage `json:"usage"`
}
type messageStartUsage struct {
	InputTokens                int64 `json:"input_tokens"`
	OutputTokens               int64 `json:"output_tokens"`
	CacheReadInputTokens       int64 `json:"cache_read_input_tokens,omitempty"`
	CacheCreationInputTokens   int64 `json:"cache_creation_input_tokens,omitempty"`
}
```

message_delta、content_block_start（单结构体多类型：`Type, Thinking, Text, ID, Name, Input *map[string]any, ToolUseID, Content []map[string]any, Status string` 顺序）、content_block_delta（`Type, Thinking, Signature, Text, PartialJSON string`）、content_block_stop、errorEvent 同法定义。数值用 `json.Number` 编码（json.Marshal 对 json.Number 原样输出）——MessageDelta 的 usage 直接 int64 即可（TS 输出整数，相同）。
- `Builder` 内部状态：`messageID, model string, inputTokens int64, usage *UsageInfo, accumulatedText/Reasoning []string, thinkingAccum string, signingSecret string, blocks *contentBlockManager`。
- 块管理器（对应 TS `ContentBlockManager`）：`nextIndex, thinkingIndex, textIndex, thinkingStarted, textStarted int, toolStates map[int]*toolCallState`。`toolCallState{blockIndex, toolID, name string, contents []string, started bool, taskArgBuffer string, taskArgsEmitted bool, preStartArgs string}`。
- 注册工具名渐进拼接逻辑（registerToolName：`name.startsWith(prev)` 取新名，否则拼接）、`taskArgsEmitted`/`taskArgBuffer` 的 `bufferTaskArgs` 与 `flushTaskArgBuffers`（解析成功 → `run_in_background` 强制 false；失败 → 哈希告警 `fmt.Fprintf(os.Stderr, "Task args invalid JSON (id=%s len=%d buffer_sha256_prefix=%s)\n", ...)`）——这些方法供 stream 包使用，虽然 stream 也自带修复逻辑，先按 TS 端口。
- 签名：`sha256.Sum256(secret + thinkingAccum)` 十六进制（TS `createHash("sha256").update(secret).update(accum)` 顺序相同）。`signingSecret = hex(sha256("ts-proxy-think-sign:" + messageID))`——**保持与 TS 相同的密钥派生字符串**（同一 thinking 文本 + 同一 message_id 的签名跨实现一致，保证客户端多轮验证兼容）。
- `MessageStart`：usage 的 input = `usage.prompt_tokens - cache_read - cache_creation`（饱和 0）；无 usage → 构造时 inputTokens；cache 桶 >0 才出现。
- `MessageDelta`：同上 input 计算 + `thinking_tokens`（estimateThinkingTokens >0 时）+ cache 桶。
- `CloseAllBlocks` 顺序：CloseContentBlocks（thinking：signature → stop；text：stop）→ 各 started tool stop。
- `EstimateOutputTokens`：char/4 三部分（text、reasoning、每个 tool 的 name+contents+15）+ 块数×4（有 reasoning、有 text、started 工具数）。
- `SetThinkingStarted()`/`SetTextStarted()` 返回 `thinkingStarted`/`textStarted` 状态（供 hadContent 判定）。`NextIndex`/`SetNextIndex` 供 agentic 流偏移。

- [ ] **Step 4: 实现 errors.go**

- `MapStopReason`：stop→end_turn、length→max_tokens、tool_calls→tool_use、content_filter→refusal、其余→end_turn。
- `MapErrorType(code int)`：429/529→overloaded_error、400-499→invalid_request_error、其余→api_error。
- `BuildMidStreamErrorSse`：`event: error` + `{"type":"error","error":{"type":"stream_error","message":<msg>}}`。
- `BuildRetryableMidStreamErrorSse`：message 加前缀 `{"type":"overloaded_error"} `。
- `FormatEvent(eventType, data)`：`event: <type>\ndata: <json>\n\n`。

- [ ] **Step 5: 运行确认通过**

Run: `go test ./internal/sse/` — Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add internal/sse
git commit -m "feat: anthropic SSE event builder with byte-parity output

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: dump 会话

**Files:**
- Create: `internal/dump/dump.go`
- Test: `internal/dump/dump_test.go`

**Interfaces:** Consumes: 无。Produces: `dump.Session`/`TerminationReason`/`Termination`/`ServerToolLogEntry`（签名见契约）。

- [ ] **Step 1: 写失败的测试**

```go
package dump

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNoopSessionWhenDirEmpty(t *testing.T) {
	s := NewSession("")
	s.WriteDownstreamRequest(nil, "ts", "body") // 不得 panic
	s.Finish()
}

func TestSessionWritesFourLogsAndMovesToCompleted(t *testing.T) {
	dir := t.TempDir()
	s := NewSession(dir)
	s.WriteDownstreamRequest(map[string]string{"x-api-key": "k"}, "2026-08-03T00:00:00Z", `{"model":"m"}`)
	s.WriteUpstreamRequest(nil, "2026-08-03T00:00:00Z", `{"model":"m","stream":true}`)
	s.SetTiming(50, 120)
	s.WriteUpstreamResponse(nil, 200, "data: [DONE]", nil)
	s.WriteDownstreamResponse(nil, 200, "event: message_stop", &Termination{Reason: Completed})
	s.Finish()

	entries, err := os.ReadDir(filepath.Join(dir, "completed"))
	if err != nil { t.Fatal(err) }
	if len(entries) != 1 { t.Fatalf("completed entries = %d", len(entries)) }
	sub := filepath.Join(dir, "completed", entries[0].Name())
	for _, f := range []string{"downstream-request.log", "downstream-response.log", "upstream-request.log", "upstream-response.log"} {
		if _, err := os.Stat(filepath.Join(sub, f)); err != nil { t.Errorf("missing %s: %v", f, err) }
	}
	b, _ := os.ReadFile(filepath.Join(sub, "downstream-request.log"))
	if !strings.Contains(string(b), "[Request DateTime]") || !strings.Contains(string(b), "x-api-key: k") {
		t.Errorf("log content: %s", b)
	}
	b2, _ := os.ReadFile(filepath.Join(sub, "downstream-response.log"))
	if !strings.Contains(string(b2), "TTFB: 50ms") || !strings.Contains(string(b2), "Total: 120ms") {
		t.Errorf("timing: %s", b2)
	}
}

func TestTerminationBuckets(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		term *Termination
		want string
	}{
		{"completed", &Termination{Reason: Completed}, "completed"},
		{"client", &Termination{Reason: ClientAbort}, "client-aborted"},
		{"upstream_abort", &Termination{Reason: UpstreamAbort}, "upstream-aborted"},
		{"timeout", &Termination{Reason: UpstreamTimeout}, "failed"},
		{"upstream_error", &Termination{Reason: UpstreamError}, "failed"},
		{"none", nil, "completed"},
	}
	for _, c := range cases {
		s := NewSession(dir)
		s.WriteDownstreamResponse(nil, 200, "", c.term)
		s.Finish()
		entries, _ := os.ReadDir(filepath.Join(dir, c.want))
		if len(entries) != 1 { t.Errorf("%s: bucket %q has %d entries", c.name, c.want, len(entries)) }
	}
}

func TestPrecedenceClientAbortOverUpstream(t *testing.T) {
	dir := t.TempDir()
	s := NewSession(dir)
	s.RecordUpstreamTermination(UpstreamAbort, "2026-08-03T00:00:00Z")
	s.WriteDownstreamResponse(nil, 200, "", &Termination{Reason: ClientAbort})
	s.Finish()
	entries, _ := os.ReadDir(filepath.Join(dir, "client-aborted"))
	if len(entries) != 1 { t.Errorf("client-aborted entries = %d (client abort must win)", len(entries)) }
}

func TestPrecedenceUpstreamReportOverTracked(t *testing.T) {
	dir := t.TempDir()
	s := NewSession(dir)
	s.WriteDownstreamResponse(nil, 200, "", &Termination{Reason: Completed})
	s.RecordUpstreamTermination(UpstreamAbort, "2026-08-03T00:00:00Z")
	s.Finish()
	entries, _ := os.ReadDir(filepath.Join(dir, "upstream-aborted"))
	if len(entries) != 1 { t.Errorf("upstream-aborted entries = %d (upstream report must win over completed)", len(entries)) }
}

func TestRecordAfterFinishIsNoop(t *testing.T) {
	dir := t.TempDir()
	s := NewSession(dir)
	s.Finish()
	s.RecordUpstreamTermination(UpstreamAbort, "x") // 不得改变已完成的归类
	if s.UpstreamTermination() != nil { t.Errorf("upstream termination = %+v", s.UpstreamTermination()) }
}

func TestServerToolLog(t *testing.T) {
	dir := t.TempDir()
	s := NewSession(dir)
	rc := 10
	s.LogServerTool(ServerToolLogEntry{Tool: "web_search", Timestamp: "t", Input: "q", Engine: "brave", ResultCount: &rc})
	s.Finish()
	entries, _ := os.ReadDir(filepath.Join(dir, "completed"))
	if len(entries) != 1 { t.Fatal("no completed entry") }
	b, err := os.ReadFile(filepath.Join(filepath.Join(dir, "completed", entries[0].Name()), "server-tools.log"))
	if err != nil { t.Fatal(err) }
	if !strings.Contains(string(b), "[Tool]") || !strings.Contains(string(b), "Result Count: 10") {
		t.Errorf("server-tools.log: %s", b)
	}
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/dump/` — Expected: FAIL

- [ ] **Step 3: 实现 dump.go**

端口 `../chat-to-claude-code/src/core/dump.ts`。要点：
- `NewSession(dir)`：dir=="" → noop 会话（所有方法空操作）；否则创建 `<dir>/in-progress/<id>/`（`id` 为 crypto/rand 16 字节格式化 UUID v4 字符串，与 TS randomUUIDv7 形态一致：`xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`）。
- 日志格式 `[Section]\n<content>\n\n`：Request 日志 = DateTime + Headers（`k: v` 换行）+ Body；Response 日志 = Status + Headers + Body + 可选 Termination（`Reason: x\nDisconnectTime: y`）+ Timing（`TTFB: xms\nTotal: yms`）；server tool 条目各字段可选追加，尾部 `---`。
- `WriteUpstreamResponse`/`WriteDownstreamResponse`：`termination != nil` 时记录 `tracked`；`Finish()`：写 server-tools.log（如有）→ 计算 `finalName = id + "__START_" + formatTime(start) + "__END_" + formatTime(end)`（`formatTime` = ISO8601 替换 `:`/`.` 为 `-`）→ 按 `pickTerminationReason` 结果选桶（client_abort > 记录的 upstream > tracked；`getTargetSubdir`：completed→completed、client_abort→client-aborted、upstream_abort→upstream-aborted、upstream_timeout/upstream_error/缺省→failed）→ `os.MkdirAll` + `os.Rename`。所有文件操作失败静默忽略（TS `catch {}`）。
- `RecordUpstreamTermination`：finished 后 noop。
- 文件写入用 `os.WriteFile`，无锁（单请求单 goroutine 调用，agentic 与标准流不同时写——与 TS 相同假设）。

- [ ] **Step 4: 运行确认通过**

Run: `go test ./internal/dump/` — Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/dump
git commit -m "feat: request dump session with termination buckets

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 6: parsers 包

**Files:**
- Create: `internal/parsers/thinktag.go`
- Test: `internal/parsers/thinktag_test.go`
- Create: `internal/parsers/heuristictool.go`
- Test: `internal/parsers/heuristictool_test.go`

**Interfaces:** Consumes: 无。Produces: `ThinkTagParser`/`HeuristicToolParser`（签名见契约）。

- [ ] **Step 1: 写失败的测试**

`internal/parsers/thinktag_test.go`：

```go
package parsers

import "testing"

func feedAll(t *testing.T, p *ThinkTagParser, parts ...string) []ContentChunk {
	t.Helper()
	var out []ContentChunk
	for _, part := range parts {
		out = append(out, p.Feed(part)...)
	}
	return out
}

func TestThinkTagBasic(t *testing.T) {
	p := NewThinkTagParser()
	chunks := feedAll(t, p, "Hello <think>let me think</think> world")
	if len(chunks) != 3 { t.Fatalf("chunks = %d: %+v", len(chunks), chunks) }
	if chunks[0].Type != TextContent || chunks[0].Content != "Hello " { t.Errorf("c0 = %+v", chunks[0]) }
	if chunks[1].Type != ThinkingContent || chunks[1].Content != "let me think" { t.Errorf("c1 = %+v", chunks[1]) }
	if chunks[2].Type != TextContent || chunks[2].Content != " world" { t.Errorf("c2 = %+v", chunks[2]) }
}

func TestThinkTagSplitAcrossChunks(t *testing.T) {
	p := NewThinkTagParser()
	chunks := feedAll(t, p, "<th", "ink>", "deep", " thought", "</th", "ink>")
	if len(chunks) != 1 { t.Fatalf("chunks = %d: %+v", len(chunks), chunks) }
	if chunks[0].Type != ThinkingContent || chunks[0].Content != "deep thought" { t.Errorf("c0 = %+v", chunks[0]) }
}

func TestThinkTagOrphanClose(t *testing.T) {
	p := NewThinkTagParser()
	chunks := feedAll(t, p, "stray </think> tag")
	if len(chunks) != 1 { t.Fatalf("chunks = %d", len(chunks)) }
	if chunks[0].Content != "stray " { t.Errorf("c0 = %+v", chunks[0]) }
	// 残余 " tag" 在 flush 时作为文本
	fl := p.Flush()
	if fl == nil || fl.Content != " tag" || fl.Type != TextContent { t.Errorf("flush = %+v", fl) }
}

func TestThinkTagIncompleteTail(t *testing.T) {
	p := NewThinkTagParser()
	chunks := feedAll(t, p, "text before <th")
	if len(chunks) != 1 { t.Fatalf("chunks = %d", len(chunks)) }
	if chunks[0].Content != "text before " { t.Errorf("c0 = %+v", chunks[0]) }
	chunks = feedAll(t, p, "ink>inside</think>after")
	if len(chunks) != 3 { t.Fatalf("chunks2 = %d: %+v", len(chunks), chunks) }
	if chunks[0].Type != ThinkingContent || chunks[0].Content != "inside" { t.Errorf("c0 = %+v", chunks[0]) }
	if chunks[1].Content != "after" { t.Errorf("c1 = %+v", chunks[1]) }
}

func TestThinkTagFlushInsideTag(t *testing.T) {
	p := NewThinkTagParser()
	feedAll(t, p, "<think>unterminated")
	fl := p.Flush()
	if fl == nil || fl.Type != ThinkingContent || fl.Content != "unterminated" { t.Errorf("flush = %+v", fl) }
}

func TestThinkTagEmptyContent(t *testing.T) {
	p := NewThinkTagParser()
	chunks := feedAll(t, p, "<think></think>")
	if len(chunks) != 0 { t.Errorf("chunks = %d", len(chunks)) }
}
```

`internal/parsers/heuristictool_test.go`：

```go
package parsers

import (
	"reflect"
	"testing"
)

func TestHeuristicFunctionTool(t *testing.T) {
	p := NewHeuristicToolParser()
	filtered, tools := p.Feed("● <function=read_file><parameter=path>/etc/hosts</parameter>")
	if filtered != "" { t.Errorf("filtered = %q", filtered) }
	if len(tools) != 1 { t.Fatalf("tools = %d", len(tools)) }
	if tools[0]["name"] != "read_file" { t.Errorf("name = %v", tools[0]["name"]) }
	if tools[0]["type"] != "tool_use" { t.Errorf("type = %v", tools[0]["type"]) }
	in := tools[0]["input"].(map[string]any)
	if in["path"] != "/etc/hosts" { t.Errorf("input = %v", in) }
	if tools[0]["id"] == nil || tools[0]["id"] == "" { t.Errorf("id missing: %v", tools[0]) }
}

func TestHeuristicMultipleParameters(t *testing.T) {
	p := NewHeuristicToolParser()
	_, tools := p.Feed("● <function=write_file><parameter=path>/x</parameter><parameter=content>hello</parameter>")
	if len(tools) != 1 { t.Fatalf("tools = %d", len(tools)) }
	in := tools[0]["input"].(map[string]any)
	if in["path"] != "/x" || in["content"] != "hello" { t.Errorf("input = %v", in) }
}

func TestHeuristicTextBeforeAndAfter(t *testing.T) {
	p := NewHeuristicToolParser()
	filtered, tools := p.Feed("prefix ● <function=read_file><parameter=path>/a</parameter> suffix")
	if filtered != "prefix " && filtered != "prefix" { t.Errorf("filtered = %q", filtered) }
	if len(tools) != 1 { t.Fatalf("tools = %d", len(tools)) }
	// 尾部 " suffix" 由下一次 feed/flush 输出
	flText, flTools := p.Flush()
	if flText != " suffix" && flText != "suffix" { t.Errorf("flush text = %q", flText) }
	if len(flTools) != 0 { t.Errorf("flush tools = %d", len(flTools)) }
}

func TestHeuristicWebToolJSON(t *testing.T) {
	p := NewHeuristicToolParser()
	_, tools := p.Feed(`Sure! Use WebFetch {"url": "https://example.com"}`)
	if len(tools) != 1 { t.Fatalf("tools = %d", len(tools)) }
	if tools[0]["name"] != "WebFetch" { t.Errorf("name = %v", tools[0]["name"]) }
	// WebFetch 必须带 url
	_, tools2 := p.Feed(`Use WebSearch {"query": "test query"}`)
	if len(tools2) != 1 { t.Fatalf("tools2 = %d", len(tools2)) }
	if tools2[0]["name"] != "WebSearch" { t.Errorf("name2 = %v", tools2[0]["name"]) }
	_, tools3 := p.Feed(`Use WebFetch {"no":"url"}`)
	if len(tools3) != 0 { t.Errorf("WebFetch without url should not detect: %v", tools3) }
}

func TestControlTokenStrip(t *testing.T) {
	p := NewHeuristicToolParser()
	filtered, _ := p.Feed("<|begin_of_text|>hello")
	if filtered != "hello" { t.Errorf("filtered = %q", filtered) }
	// 未完成控制标记尾巴留缓冲
	filtered2, _ := p.Feed(" tail <|en")
	if filtered2 != " tail " { t.Errorf("filtered2 = %q", filtered2) }
	flText, _ := p.Flush()
	if flText != "<|en" { t.Errorf("flush = %q", flText) }
}

func TestHeuristicFunctionHeaderTooLong(t *testing.T) {
	p := NewHeuristicToolParser()
	longPrefix := "●"
	for i := 0; i < 110; i++ { longPrefix += "x" }
	filtered, tools := p.Feed(longPrefix)
	// 超过 100 字符仍未匹配 <function= → 逐字符转文本
	if filtered == "" { t.Error("should have drained to text") }
	if len(tools) != 0 { t.Errorf("tools = %d", len(tools)) }
}

func TestHeuristicFlushPartialParameters(t *testing.T) {
	p := NewHeuristicToolParser()
	p.Feed("● <function=my_tool><parameter=key>val")
	_, tools := p.Flush()
	if len(tools) != 1 { t.Fatalf("flush tools = %d", len(tools)) }
	in := tools[0]["input"].(map[string]any)
	if in["key"] != "val" { t.Errorf("input = %v", in) }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/parsers/` — Expected: FAIL

- [ ] **Step 3: 实现 thinktag.go**

端口 `../chat-to-claude-code/src/parsers/think_tag_parser.ts` 逐行翻译：
- 状态：`buffer string, inThinkTag bool`。
- `Feed`：追加 → 循环：非 think 态走 `parseOutside`（orphan `</think>` 优先、无 `<` 时整段输出、`<` 开头的潜在标签前缀保留、发现 `<think>` 进入 think 态并输出前缀文本）；think 态走 `parseInside`（`</think>` 截断、`<` 前缀分片保留、否则整段输出）；无进展即 break。
- 字符串搜索用 `strings.Index`、`strings.HasPrefix`（对应 `indexOf`/`startsWith`）；`OPEN.startsWith(potentialTag)` ↔ `strings.HasPrefix(OPEN, potential)`。
- `Flush`：buffer 非空 → 按 inThinkTag 决定类型输出。

- [ ] **Step 4: 实现 heuristictool.go**

端口 `../chat-to-claude-code/src/parsers/heuristic_tool_parser.ts`：
- 状态机枚举 `parserStateText / parserStateMatchingFunction / parserStateParsingParameters`。
- `Feed`：追加 → `stripControlTokens`（正则 `<\|[^|>]{1,80}\|>` → 空）→ `extractWebToolJSONCalls`（正则 `\b(?:use\s+)?(WebFetch|WebSearch)\b.*?(\{.*?\})` 忽略大小写 + dotall；JSON 解析失败/非对象/缺 url 或 query 跳过；命中则整个 buffer 清空并输出工具，id 形如 `toolu_heuristic_<8位hex>`）→ 主状态机循环（`●` 定位、`FUNC_START_PATTERN` = `●\s*<function=([^>]+)>`、`PARAM_PATTERN` = `<parameter=([^>]+)>([\s\S]*?)(?:</parameter>|$)`、buffer > 100 字符兜底逐字符转文本）→ 返回 `(filteredText, tools)`。
- Go 正则（RE2）注意：`[\s\S]*?` 在 RE2 中需用 `(?s:.*?)` 或 `[\s\S]*?`（Go 支持 `\s\S` 类，lazy 量词支持）；`(?<name>)` 具名组语法 Go 为 `(?P<name>)`。
- `Flush`：PARSING_PARAMETERS 且当前工具完整 → 用 `(?s)<parameter=([^>]+)>([\s\S]*)$` 收集尾参并产出工具；MATCHING_FUNCTION → 缓冲作文本；TEXT → 缓冲作文本。`id` 生成：crypto/rand 4 字节 hex。

- [ ] **Step 5: 运行确认通过 + 补全向量**

Run: `go test ./internal/parsers/` — Expected: PASS。补全向量：打开 `../chat-to-claude-code/tests/think_tag_parser.test.ts` 与 `heuristic_tool_parser.test.ts`，每个案例补进对应 Go 测试，全部通过为止。

- [ ] **Step 6: 提交**

```bash
git add internal/parsers
git commit -m "feat: think tag and heuristic tool parsers

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 7: servertool 包

**Files:**
- Create: `internal/servertool/tools.go`
- Test: `internal/servertool/tools_test.go`

**Interfaces:** Consumes: `config.ServerToolConfig`、`dump.ServerToolLogEntry`。Produces: `servertool.*`（签名见契约）。

- [ ] **Step 1: 写失败的测试**

```go
package servertool

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"chat-to-messages/internal/config"
)

func testCfg() config.ServerToolConfig {
	return config.ServerToolConfig{
		WebSearch: true, WebFetch: true,
		WebSearchEngine: "brave",
		WebFetchMaxContentTokens: 5000,
	}
}

func TestExecuteWebSearchBrave(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Subscription-Token") != "BST-key" { t.Errorf("missing token: %v", r.Header) }
		if !strings.Contains(r.URL.Path, "/res/v1/web/search") { t.Errorf("path = %s", r.URL.Path) }
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"web":{"results":[
			{"url":"https://a.com","title":"A","description":"desc A","page_age":"2026"},
			{"url":"https://b.com","title":"B"}
		]}}`))
	}))
	defer srv.Close()
	cfg := testCfg()
	cfg.WebSearchBaseURL = srv.URL
	cfg.WebSearchAPIKey = "BST-key"
	results := ExecuteWebSearch(context.Background(), "test query", cfg, nil)
	if len(results) != 2 { t.Fatalf("results = %d", len(results)) }
	if results[0].URL != "https://a.com" || results[0].Title != "A" || results[0].Snippet != "desc A" || results[0].PageAge != "2026" {
		t.Errorf("r0 = %+v", results[0])
	}
	if results[1].Snippet != "" { t.Errorf("r1 snippet should be empty: %+v", results[1]) }
}

func TestExecuteWebSearchSearxng(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("q")
		if r.URL.Query().Get("format") != "json" { t.Errorf("format = %q", r.URL.Query().Get("format")) }
		w.Write([]byte(`{"results":[{"url":"https://s.com","title":"S"}]}`))
	}))
	defer srv.Close()
	cfg := testCfg()
	cfg.WebSearchEngine = "searxng"
	cfg.WebSearchBaseURL = srv.URL
	results := ExecuteWebSearch(context.Background(), "hello world", cfg, nil)
	if gotQuery != "hello world" { t.Errorf("query = %q", gotQuery) }
	if len(results) != 1 || results[0].URL != "https://s.com" { t.Fatalf("results = %+v", results) }
}

func TestExecuteWebSearchEmptyQueryAndNoKey(t *testing.T) {
	if r := ExecuteWebSearch(context.Background(), "  ", testCfg(), nil); len(r) != 0 { t.Error("empty query should return nothing") }
	cfg := testCfg() // brave, no key
	if r := ExecuteWebSearch(context.Background(), "q", cfg, nil); len(r) != 0 { t.Error("no key should return nothing") }
}

func TestExecuteWebFetchDomainRules(t *testing.T) {
	cfg := testCfg()
	cfg.WebFetchAllowedDomains = []string{"docs.example.com"}
	cfg.WebFetchBlockedDomains = []string{"bad.example.com"}
	if r := ExecuteWebFetch(context.Background(), "https://evil.com/x", cfg, nil); r.StatusCode != 403 {
		t.Errorf("not allowed: %+v", r)
	}
	if r := ExecuteWebFetch(context.Background(), "https://bad.example.com/x", cfg, nil); r.StatusCode != 403 {
		t.Errorf("blocked: %+v", r)
	}
	if r := ExecuteWebFetch(context.Background(), "not a url", cfg, nil); r.StatusCode != 400 {
		t.Errorf("invalid url: %+v", r)
	}
}

func TestExecuteWebFetchAllowsWildcard(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("hello"))
	}))
	defer srv.Close()
	cfg := testCfg()
	cfg.WebFetchAllowedDomains = []string{"*.example.com"}
	r := ExecuteWebFetch(context.Background(), "http://sub.example.com/x", cfg, nil)
	if r.StatusCode != 200 || r.Content != "hello" { t.Errorf("wildcard fetch: %+v", r) }
}

func TestExecuteWebFetchHTMLToText(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<html><head><title>Page Title</title><script>var x=1;</script><style>body{}</style></head><body><h1>Hello</h1><p>World&nbsp;text</p></body></html>`))
	}))
	defer srv.Close()
	r := ExecuteWebFetch(context.Background(), srv.URL, testCfg(), nil)
	if r.StatusCode != 200 { t.Fatalf("status = %d", r.StatusCode) }
	if r.Title != "Page Title" { t.Errorf("title = %q", r.Title) }
	if strings.Contains(r.Content, "<script>") || strings.Contains(r.Content, "<style>") { t.Error("tags not stripped") }
	if !strings.Contains(r.Content, "Hello") || !strings.Contains(r.Content, "World text") { t.Errorf("content = %q", r.Content) }
}

func TestExecuteWebFetchTruncation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(strings.Repeat("a", 100000)))
	}))
	defer srv.Close()
	cfg := testCfg()
	cfg.WebFetchMaxContentTokens = 100
	r := ExecuteWebFetch(context.Background(), srv.URL, cfg, nil)
	if len(r.Content) > 100*4+50 { t.Errorf("content too long: %d", len(r.Content)) }
	if !strings.Contains(r.Content, "[Content truncated]") { t.Error("missing truncation marker") }
}

func TestDetectServerToolInText(t *testing.T) {
	// 4 种模式
	tc := DetectServerToolInText(`<tool_use>{"name":"web_search","input":{"query":"q1"}}</tool_use>`)
	if len(tc) != 1 || tc[0].Type != "web_search" { t.Errorf("pattern1: %+v", tc) }
	tc = DetectServerToolInText(`Use WebSearch {"query": "q2"}`)
	if len(tc) != 1 || tc[0].Type != "web_search" { t.Errorf("pattern2: %+v", tc) }
	tc = DetectServerToolInText(`<tool_call><tool_name>web_fetch</tool_name><parameter name="url">https://x.com</parameter></tool_call>`)
	if len(tc) != 1 || tc[0].Type != "web_fetch" || tc[0].Input["url"] != "https://x.com" { t.Errorf("pattern3: %+v", tc) }
	tc = DetectServerToolInText(`<tool_name>web_search</tool_name><parameter name="query">q3</parameter>`)
	if len(tc) != 1 || tc[0].Type != "web_search" || tc[0].Input["query"] != "q3" { t.Errorf("pattern4: %+v", tc) }
	// 去重
	tc = DetectServerToolInText(`<tool_use>{"name":"web_search","input":{"query":"q"}}</tool_use> Use WebSearch {"query": "q"}`)
	if len(tc) != 1 { t.Errorf("should dedupe: %+v", tc) }
}

func TestStripToolUseFromText(t *testing.T) {
	if got := StripToolUseFromText("intro <tool_use>{\"name\":\"web_search\"}</tool_use> fake results"); got != "intro" {
		t.Errorf("got %q", got)
	}
	if got := StripToolUseFromText("before <tool_call><tool_name>web_search</tool_name></tool_call> after"); got != "before" {
		t.Errorf("got %q", got)
	}
	if got := StripToolUseFromText("no tags here"); got != "no tags here" { t.Errorf("got %q", got) }
}

func TestIsServerToolTypeAndSchemas(t *testing.T) {
	for _, ty := range []string{"web_search", "web_search_20250305", "web_fetch", "web_fetch_20250305"} {
		if !IsServerToolType(ty) { t.Errorf("IsServerToolType(%q) = false", ty) }
	}
	if IsServerToolType("regular_tool") { t.Error("regular tool must not match") }
	if s := BuildServerToolFunctionSchema("web_search_20250305", "web_search"); s == nil { t.Error("schema nil") }
	if s := BuildServerToolFunctionSchema("bogus", "x"); s != nil { t.Error("bogus schema should be nil") }
	suffix := BuildServerToolSystemPromptSuffix([]map[string]any{{"type": "web_search_20250305"}, {"type": "web_fetch_20250305"}})
	if !strings.Contains(suffix, "web_search") || !strings.Contains(suffix, "web_fetch") { t.Errorf("suffix = %q", suffix) }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/servertool/` — Expected: FAIL

- [ ] **Step 3: 实现 tools.go**

端口 `../chat-to-claude-code/src/server/server_tools.ts` 逐段翻译：
- `ExecuteWebSearch`：brave 分支（无 key → 记 skip 日志并返回空；`<base>/res/v1/web/search?q=<url.QueryEscape>&count=10`，头 `Accept: application/json`、`Accept-Encoding: gzip`、`X-Subscription-Token`）；searxng 分支（`<base>/search?q=...&format=json`，可选 `Authorization: Bearer`）；非 2xx → 记日志（含截断 body ≤10000 字符）返回空；解析 `web.results` / `results` 数组，映射 url/title/description→snippet/page_age。HTTP 客户端：`&http.Client{Timeout: 0}` 用 ctx（`http.NewRequestWithContext`）。
- `ExecuteWebFetch`：UA `chat-to-messages/1.0 (proxy; +https://github.com/chinfeng/chat-to-messages)`（新项目名），`Accept: text/html,application/json,text/plain,text/markdown`；`parseDomain`（`url.Parse` 取 Hostname）；allow/block 通配 `*.` 匹配；`htmlToPlainText`（script/style 剥离、块元素换行、去标签、实体解码、空白折叠——Go 用 `regexp`，注意 RE2 惰性量词 OK）；`extractTitle`；截断 `maxChars = maxContentTokens*4` + `\n\n[Content truncated]`；响应 `res.StatusCode`、最终 URL `res.Request.URL.String()`；错误 → `{"content": "Fetch failed: ...", status_code: 502}`。日志条目同 TS（`ServerToolLogEntry` 全字段）。
- 工具检测/剥离/格式/后缀函数按 TS 逐行翻译。正则 `(?P<name>)` 具名组。

- [ ] **Step 4: 运行确认通过 + 补全向量**

Run: `go test ./internal/servertool/` — Expected: PASS。补全：`../chat-to-claude-code/tests/server_tools.test.ts` 的检测/剥离/格式案例。

- [ ] **Step 5: 提交**

```bash
git add internal/servertool
git commit -m "feat: proxy-side web search and web fetch tools

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 8: convert 包（canonical + converter）

**Files:**
- Create: `internal/convert/canonical.go`
- Test: `internal/convert/canonical_test.go`
- Create: `internal/convert/converter.go`
- Test: `internal/convert/converter_test.go`

**Interfaces:** Consumes: `anthropic.*`、`servertool.IsServerToolType/BuildServerToolFunctionSchema/BuildServerToolSystemPromptSuffix`。Produces: `convert.*`（签名见契约）。

- [ ] **Step 1: 写失败的 canonical 测试**

`internal/convert/canonical_test.go`：

```go
package convert

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestFilterPrivateParams(t *testing.T) {
	in := map[string]any{
		"_private": 1,
		"ok":       map[string]any{"_bad": 2, "good": 3, "nested": map[string]any{"_x": 4}},
		"schema": map[string]any{
			"properties": map[string]any{"_id": map[string]any{"type": "string"}},
		},
	}
	got := FilterPrivateParams(in)
	if _, ok := got["_private"]; ok { t.Error("_private must be stripped") }
	okMap := got["ok"].(map[string]any)
	if _, bad := okMap["_bad"]; bad { t.Error("_bad must be stripped") }
	if okMap["good"] != 3 { t.Error("good must remain") }
	if _, x := okMap["nested"].(map[string]any)["_x"]; x { t.Error("nested _x must be stripped") }
	if _, id := got["schema"].(map[string]any)["properties"].(map[string]any)["_id"]; !id {
		t.Error("_id in properties is a legit schema name and must be kept")
	}
}

func TestCanonicalizeAndStringify(t *testing.T) {
	in := map[string]any{"b": 2, "a": 1, "arr": []any{3, 1, 2}}
	got := Canonicalize(in)
	if !reflect.DeepEqual(got, map[string]any{"a": 1, "b": 2, "arr": []any{3, 1, 2}}) {
		t.Errorf("got %v", got)
	}
	s, err := CanonicalJSONStringify(map[string]any{"z": 1, "a": []any{"x", "y"}})
	if err != nil { t.Fatal(err) }
	if s != `{"a":["x","y"],"z":1}` { t.Errorf("s = %s", s) }
}

func TestCanonicalJSONStringifyNumberFidelity(t *testing.T) {
	var v map[string]any
	dec := json.NewDecoder(strings.NewReader(`{"big":12345678901234567890}`))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil { t.Fatal(err) }
	s, err := CanonicalJSONStringify(v)
	if err != nil { t.Fatal(err) }
	if s != `{"big":12345678901234567890}` { t.Errorf("s = %s", s) }
}

func TestPrepareCanonicalBody(t *testing.T) {
	body := map[string]any{"_x": 1, "b": 2, "a": 3}
	got := PrepareCanonicalBody(body)
	if _, ok := got["_x"]; ok { t.Error("_x must be stripped") }
	// 键排序由 json.Marshal 保证
	b, _ := json.Marshal(got)
	if string(b) != `{"a":3,"b":2}` { t.Errorf("b = %s", b) }
	// 不修改入参
	if _, ok := body["_x"]; !ok { t.Error("input must not be mutated") }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/convert/` — Expected: FAIL

- [ ] **Step 3: 实现 canonical.go**

端口 `../chat-to-claude-code/src/conversion/canonical.ts`：
- `FilterPrivateParams`：递归 map，键以 `_` 开头且在非 schema-name-map 上下文 → 跳过；`properties`/`patternProperties`/`definitions`/`$defs` 为直接键值时的子 map 是 name-map（不剥离其键），数组元素重置上下文。
- `Canonicalize`：递归 map（键排序）+ 数组逐元素 + 原样基础值。Go 的 map 无序遍历，排序键后插入 map 即可——`json.Marshal` 天然排序，Canonicalize 仅需递归复制（保结构）。
- `CanonicalJSONStringify(v) = json.Marshal(v)`（map 排序输出；json.Number 原样）。注意：TS `JSON.stringify(canonicalize(value))` 对 string/number/bool/null 输出一致；对 undefined 抛错——Go 无 undefined，nil map → `null`（TS 中工具输入非对象时为 String(toolInput)，不经过 stringify——行为已在 converter 分流）。
- `PrepareCanonicalBody` = `Canonicalize(FilterPrivateParams(body))`，不修改入参。

- [ ] **Step 4: 写失败的 converter 测试**

`internal/convert/converter_test.go`（核心案例内联；其余从 `../chat-to-claude-code/tests/converter.test.ts` 补全）：

```go
package convert

import (
	"encoding/json"
	"strings"
	"testing"

	"chat-to-messages/internal/anthropic"
)

func decodeMessage(t *testing.T, data string) anthropic.Message {
	t.Helper()
	var m anthropic.Message
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil { t.Fatal(err) }
	return m
}

func decodeMessages(t *testing.T, data string) []anthropic.Message {
	t.Helper()
	var msgs []anthropic.Message
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&msgs); err != nil { t.Fatal(err) }
	return msgs
}

func jsonStr(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil { t.Fatal(err) }
	return string(b)
}

func TestConvertSystemPromptStripsBillingHeader(t *testing.T) {
	got := ConvertSystemPrompt("x-anthropic-billing-header: cch=abc;\nYou are a helper.")
	if got["role"] != "system" || got["content"] != "You are a helper." {
		t.Errorf("got %v", got)
	}
	// 整串仅表头 → nil
	if got := ConvertSystemPrompt("x-anthropic-billing-header: cch=abc;"); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
	// 数组形式
	got2 := ConvertSystemPrompt([]any{
		map[string]any{"type": "text", "text": "x-anthropic-billing-header: cch=z;\nPart A"},
		map[string]any{"type": "text", "text": "Part B"},
	})
	if got2["content"] != "Part A\n\nPart B" { t.Errorf("got %v", got2) }
}

func TestConvertThinkingThinkTags(t *testing.T) {
	msgs := []anthropic.Message{{
		Role: "assistant",
		Content: anthropic.ContentValue{IsString: false, Blocks: []anthropic.ContentBlock{
			{Type: "thinking", Thinking: "hmm", Signature: "sig123"},
			{Type: "text", Text: "answer"},
		}},
	}}
	got := ConvertMessages(msgs, ReplayThinkTags)
	if len(got) != 1 { t.Fatalf("msgs = %d", len(got)) }
	content := got[0]["content"].(string)
	if !strings.Contains(content, "<!--sig:sig123-->") || !strings.Contains(content, "<think>\nhmm\n</think>") || !strings.Contains(content, "answer") {
		t.Errorf("content = %q", content)
	}
}

func TestConvertThinkingDisabledDropsThinking(t *testing.T) {
	msgs := []anthropic.Message{{
		Role: "assistant",
		Content: anthropic.ContentValue{Blocks: []anthropic.ContentBlock{
			{Type: "thinking", Thinking: "hmm"},
			{Type: "text", Text: "answer"},
		}},
	}}
	got := ConvertMessages(msgs, ReplayDisabled)
	if !strings.Contains(got[0]["content"].(string), "answer") { t.Errorf("content = %v", got[0]["content"]) }
	if strings.Contains(got[0]["content"].(string), "hmm") { t.Error("thinking must be dropped") }
}

func TestConvertThinkingReasoningContent(t *testing.T) {
	msgs := []anthropic.Message{{
		Role: "assistant",
		Content: anthropic.ContentValue{Blocks: []anthropic.ContentBlock{
			{Type: "thinking", Thinking: "hmm"},
			{Type: "text", Text: "answer"},
		}},
	}}
	got := ConvertMessages(msgs, ReplayReasoningContent)
	if got[0]["reasoning_content"] != "hmm" { t.Errorf("reasoning_content = %v", got[0]["reasoning_content"]) }
	if got[0]["content"] != "answer" { t.Errorf("content = %v", got[0]["content"]) }
}

func TestConvertToolUseCanonicalArgs(t *testing.T) {
	msgs := []anthropic.Message{{
		Role: "assistant",
		Content: anthropic.ContentValue{Blocks: []anthropic.ContentBlock{
			{Type: "tool_use", ID: "tu1", Name: "f",
				Input: json.RawMessage(`{"b":2,"a":1,"arr":[3,1]}`)},
		}},
	}}
	got := ConvertMessages(msgs, ReplayThinkTags)
	tc := got[0]["tool_calls"].([]any)[0].(map[string]any)
	fn := tc["function"].(map[string]any)
	if fn["arguments"] != `{"a":1,"arr":[3,1],"b":2}` { t.Errorf("args = %v", fn["arguments"]) }
}

func TestConvertToolResultAndMedia(t *testing.T) {
	msgs := []anthropic.Message{
		{Role: "user", Content: anthropic.ContentValue{Blocks: []anthropic.ContentBlock{
			{Type: "tool_result", ToolUseID: "tu1", Content: json.RawMessage(`[{"type":"text","text":"ok"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}]`)},
		}}},
	}
	got := ConvertMessages(msgs, ReplayThinkTags)
	// [tool 消息, 合成 user 消息]
	if len(got) != 2 { t.Fatalf("msgs = %d: %v", len(got), jsonStr(t, got)) }
	toolMsg := got[0]
	if toolMsg["role"] != "tool" || toolMsg["tool_call_id"] != "tu1" { t.Errorf("tool msg = %v", toolMsg) }
	if !strings.Contains(toolMsg["content"].(string), "[tool result media moved to the following user message]") {
		t.Errorf("tool content = %v", toolMsg["content"])
	}
	userMsg := got[1]
	if userMsg["role"] != "user" { t.Errorf("user msg = %v", userMsg) }
	parts := userMsg["content"].([]any)
	if len(parts) != 2 { t.Fatalf("parts = %d", len(parts)) }
	img := parts[1].(map[string]any)
	if img["type"] != "image_url" { t.Errorf("img = %v", img) }
	iu := img["image_url"].(map[string]any)
	if iu["url"] != "data:image/png;base64,AAAA" { t.Errorf("url = %v", iu["url"]) }
}

func TestConvertToolErrorPrefix(t *testing.T) {
	msgs := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{Blocks: []anthropic.ContentBlock{
		{Type: "tool_result", ToolUseID: "tu1", IsError: true, Content: json.RawMessage(`"boom"`)},
	}}}}
	got := ConvertMessages(msgs, ReplayThinkTags)
	if !strings.HasPrefix(got[0]["content"].(string), "[TOOL_ERROR] boom") { t.Errorf("content = %v", got[0]["content"]) }
}

func TestConvertDeferredPostToolBlocks(t *testing.T) {
	msgs := decodeMessages(t, `[
		{"role":"assistant","content":[
			{"type":"text","text":"I will call"},
			{"type":"tool_use","id":"tu1","name":"f","input":{}},
			{"type":"text","text":"deferred text after tool"}
		]},
		{"role":"user","content":"continue"}
	]`)
	got := ConvertMessages(msgs, ReplayThinkTags)
	// assistant(tool_calls) 先出；deferred text 在 user 消息之前注入
	if len(got) != 3 { t.Fatalf("msgs = %d: %v", len(got), jsonStr(t, got)) }
	if got[0]["role"] != "assistant" { t.Errorf("m0 = %v", got[0]) }
	if got[1]["role"] != "assistant" || !strings.Contains(got[1]["content"].(string), "deferred text after tool") {
		t.Errorf("m1 = %v", got[1])
	}
	if got[2]["role"] != "user" { t.Errorf("m2 = %v", got[2]) }
}

func TestConvertUserImageMessage(t *testing.T) {
	msgs := decodeMessages(t, `[
		{"role":"user","content":[
			{"type":"text","text":"look at"},
			{"type":"image","source":{"type":"base64","media_type":"image/jpeg","data":"BBBB"},"detail":"low"}
		]}
	]`)
	got := ConvertMessages(msgs, ReplayThinkTags)
	content := got[0]["content"].([]any)
	if len(content) != 2 { t.Fatalf("parts = %d: %v", len(content), jsonStr(t, content)) }
	img := content[1].(map[string]any)
	iu := img["image_url"].(map[string]any)
	if iu["url"] != "data:image/jpeg;base64,BBBB" || iu["detail"] != "low" { t.Errorf("img = %v", img) }
}

func TestConvertDocumentBlock(t *testing.T) {
	msgs := decodeMessages(t, `[
		{"role":"user","content":[{"type":"document","title":"report.pdf","context":"about it","source":{"type":"base64","media_type":"application/pdf","data":"Q1JJ"}}]}
	]`)
	got := ConvertMessages(msgs, ReplayThinkTags)
	content := got[0]["content"].(string)
	if !strings.Contains(content, "[Document: report.pdf (data:application/pdf;base64,Q1JJ)]") {
		t.Errorf("content = %q", content)
	}
	if !strings.Contains(content, "about it") { t.Errorf("context missing: %q", content) }
}

func TestConvertToolChoice(t *testing.T) {
	if got := ConvertToolChoice(map[string]any{"type": "tool", "name": "f"}); got.(map[string]any)["type"] != "function" {
		t.Errorf("tool → %v", got)
	}
	if got := ConvertToolChoice(map[string]any{"type": "any"}); got != "required" { t.Errorf("any → %v", got) }
	if got := ConvertToolChoice(map[string]any{"type": "auto"}); got.(map[string]any)["type"] != "auto" { t.Errorf("auto → %v", got) }
	if !HasDisableParallelToolUse(map[string]any{"type": "auto", "disable_parallel_tool_use": true}) { t.Error("disable_parallel") }
	if HasDisableParallelToolUse(map[string]any{"type": "auto"}) { t.Error("must be false") }
}

func TestConvertToolsFiltersServerTools(t *testing.T) {
	tools := []map[string]any{
		{"type": "custom", "name": "t1", "description": "d", "input_schema": map[string]any{"type": "object", "properties": map[string]any{}}},
		{"type": "web_search_20250305", "name": "web_search"},
	}
	got := ConvertTools(tools)
	if len(got) != 1 { t.Fatalf("tools = %d: %v", len(got), got) }
	fn := got[0]["function"].(map[string]any)
	if fn["name"] != "t1" || fn["description"] != "d" { t.Errorf("fn = %v", fn) }
	if _, ok := fn["parameters"]; !ok { t.Error("parameters missing") }
}

func TestBuildBaseRequestBody(t *testing.T) {
	req := &RequestData{
		Model: "m1",
		Messages: []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{Str: "hi", IsString: true}}},
		System: "x-anthropic-billing-header: cch=1;\nYou are helpful",
		MaxTokens: json.Number("4096"),
		Tools: []map[string]any{
			{"type": "custom", "name": "t1", "input_schema": map[string]any{"type": "object"}},
			{"type": "web_search_20250305", "name": "web_search"},
		},
		ToolChoice: map[string]any{"type": "tool", "name": "t1", "disable_parallel_tool_use": true},
	}
	body := BuildBaseRequestBody(req, nil, ReplayThinkTags)
	if body["model"] != "m1" { t.Errorf("model = %v", body["model"]) }
	msgs := body["messages"].([]any)
	if len(msgs) != 2 { t.Fatalf("messages = %d", len(msgs)) }
	sys := msgs[0].(map[string]any)
	if sys["role"] != "system" || sys["content"].(string) != "You are helpful" {
		t.Errorf("system = %v", sys)
	}
	// server tool schema 被附加到 tools
	tools := body["tools"].([]any)
	if len(tools) != 2 { t.Fatalf("tools = %d", len(tools)) }
	// 系统提示后缀注入
	if !strings.Contains(sys["content"].(string), "web_search") { t.Errorf("server tool suffix missing: %v", sys["content"]) }
	if body["parallel_tool_calls"] != false { t.Errorf("parallel = %v", body["parallel_tool_calls"]) }
	if body["max_tokens"] != json.Number("4096") { t.Errorf("max_tokens = %v", body["max_tokens"]) }
}
```

- [ ] **Step 5: 运行确认失败**

Run: `go test ./internal/convert/` — Expected: FAIL

- [ ] **Step 6: 实现 converter.go**

端口 `../chat-to-claude-code/src/conversion/converter.ts`。逐函数翻译，结构与 TS 对齐（关键状态机原样保留）：
- `stripLeadingAnthropicBillingHeader`（`\r\n`/`\r`/`\n` 终止符处理）、`stripTrailingNewline`。
- `thinkTagContent` = `<think>\n` + reasoning + `\n</think>`。
- `buildImagePartFromBlock`（base64 → data URL，`isImageMimeType` 检查，detail 透传；url → 直通）。
- `serializeToolResultContent`（string/object/array 分支；`TOOL_RESULT_MEDIA_MARKER`；非 text/image 结构化块 JSON 文本化）。
- `_convertAssistantMessage`（thinking 三模式、redacted_thinking、tool_use→tool_calls 规范参数、image→`[Image]`、空内容→`" "`）。
- `PendingAfterTools` 挂起状态机 + `_convertAssistantMessageWithSplit` + `_convertUserMessageWithInjection` + `_convertUserMessage`（图片合并回拉、文档块三策略、工具消息媒体拆出）。
- `ConvertSystemPrompt` / `ConvertTools`（过滤 server tool 类型）/ `ConvertToolChoice` / `HasDisableParallelToolUse`。
- `BuildBaseRequestBody`：双源收集 server tools（server_tools 字段 + tools 数组内 server tool 条目）去重 → 系统提示后缀 → 常规工具 + server tool schema 合成 `body.tools` → tool_choice 映射 + `parallel_tool_calls`。`defaultMaxTokens` 与 request 值都走 `any`（nil 判断）。
- 数组/对象判定用 `reflect` 辅助或类型断言；TS `Array.isArray` ↔ Go 类型断言 `[]any`；`typeof x === "object" && x !== null && !Array.isArray(x)` ↔ `map[string]any` 断言。
- `OpenAIConversionError` 在用户消息注入路径抛错（TS throw；Go 返回 error——此路径在 ConvertMessages 内部，设计为 panic 式不优雅。**决策**：`ConvertMessages` 在注入路径遇到 image 块时返回 `[]map[string]any` 前以 `(nil, *OpenAIConversionError)` 形式返回错误——签名改为 `ConvertMessages(msgs, replay) ([]map[string]any, error)`。检查调用点：BuildBaseRequestBody 和 stream.Streamer 使用，错误将 500 化。TS 该分支实际极少触发（injection 路径 images 来自 tool_result 媒体，不会被 user 层 image 触发——TS 里 `_convertUserMessageWithInjection` 对 image 块 throw）。为忠实，Go 同样在 `_convertUserMessageWithInjection` 遇 image 返回 `*OpenAIConversionError`，由 `ConvertMessages` 上抛。）

- [ ] **Step 7: 运行确认通过 + 补全向量**

Run: `go test ./internal/convert/` — Expected: PASS。补全：`../chat-to-claude-code/tests/converter.test.ts`（581 行）与 `canonical.test.ts` 全部案例。

- [ ] **Step 8: 提交**

```bash
git add internal/convert
git commit -m "feat: anthropic to openai conversion with canonical json

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 9: stream 包（流转换状态机）

**Files:**
- Create: `internal/stream/stream.go`
- Test: `internal/stream/stream_test.go`

**Interfaces:** Consumes: `openai.Chunk`/`IterSSEChunks`、`convert.RequestData`、`sse.Builder`/`sse.UsageInfo`、`dump.Session`。Produces: `stream.Streamer`/`UpstreamStreamError`/`UpstreamAbortedError`/`BuildIncompleteNotice`/`ExtractUsageInfo`/`InferToolNameByIndex`/`Options`（签名见契约）。

- [ ] **Step 1: 写失败的测试**

```go
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"chat-to-messages/internal/convert"
	"chat-to-messages/internal/openai"
	"chat-to-messages/internal/sse"
	"chat-to-messages/internal/anthropic"
)

// 构建请求辅助
func req(t *testing.T, model string) *convert.RequestData {
	t.Helper()
	return &convert.RequestData{Model: model}
}

func chunksFromSSE(t *testing.T, body string) []openai.Chunk {
	t.Helper()
	var out []openai.Chunk
	for c := range openai.IterSSEChunks(context.Background(), strings.NewReader(body), nil) {
		out = append(out, c)
	}
	return out
}

func collect(t *testing.T, st *Streamer) ([]string, error) {
	t.Helper()
	var events []string
	for ev := range st.Events() {
		events = append(events, ev)
	}
	return events, st.Err()
}

func allEvents(t *testing.T, body string, request *convert.RequestData) []string {
	t.Helper()
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), request, 10, true, nil, nil)
	events, err := collect(t, st)
	if err != nil { t.Fatal(err) }
	return events
}

// seqFromSlice 把切片包装成 iter.Seq[openai.Chunk]（测试辅助，见下方实现）
func seqFromSlice(chunks []openai.Chunk) iter.Seq[openai.Chunk] {
	return func(yield func(openai.Chunk) bool) {
		for _, c := range chunks {
			if !yield(c) { return }
		}
	}
}

func eventTypes(events []string) []string {
	var out []string
	for _, e := range events {
		for _, line := range strings.Split(e, "\n") {
			if strings.HasPrefix(line, "event: ") {
				out = append(out, strings.TrimPrefix(line, "event: "))
			}
		}
	}
	return out
}

func TestStreamMessageLifecycle(t *testing.T) {
	ev := allEvents(t, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n", req(t, "m1"))
	types := eventTypes(ev)
	want := []string{"message_start", "content_block_start", "content_block_delta", "content_block_stop", "message_delta", "message_stop"}
	if strings.Join(types, ",") != strings.Join(want, ",") { t.Fatalf("types = %v", types) }
	if !strings.Contains(ev[len(ev)-2], `"stop_reason":"end_turn"`) { t.Errorf("delta = %s", ev[len(ev)-2]) }
}

func TestStreamReasoningContent(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think step 1\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	types := eventTypes(ev)
	want := []string{"message_start", "content_block_start", "content_block_delta", "content_block_delta", "content_block_stop", "content_block_start", "content_block_delta", "content_block_stop", "message_delta", "message_stop"}
	if strings.Join(types, ",") != strings.Join(want, ",") { t.Fatalf("types = %v\n%v", types, ev) }
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"delta":{"type":"thinking_delta","thinking":"think step 1"}`) { t.Errorf("thinking delta missing: %s", joined) }
	// thinking 块关闭前有 signature_delta
	if !strings.Contains(joined, "signature_delta") { t.Errorf("signature missing: %s", joined) }
	// text 块
	if !strings.Contains(joined, `"delta":{"type":"text_delta","text":"answer"}`) { t.Errorf("text delta missing: %s", joined) }
}

func TestStreamThinkTagsInContent(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"<think>hidden</think>visible\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"thinking":"hidden"`) { t.Errorf("thinking missing: %s", joined) }
	if !strings.Contains(joined, `"text":"visible"`) { t.Errorf("text missing: %s", joined) }
	// 用户消息里没有 <think> 文本
	if strings.Contains(joined, "<think>") { t.Errorf("raw tag leaked: %s", joined) }
}

func TestStreamThinkTagsDisabled(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"<think>hidden</think>visible\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n"
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, false, nil, nil)
	ev, err := collect(t, st)
	if err != nil { t.Fatal(err) }
	joined := strings.Join(ev, "\n")
	if strings.Contains(joined, "thinking") { t.Errorf("thinking must be dropped: %s", joined) }
	if !strings.Contains(joined, `"text":"visible"`) { t.Errorf("text missing: %s", joined) }
}

func TestStreamNativeToolCall(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"read_file\",\"arguments\":\"{\\\"path\\\":\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"/etc/hosts\\\"}\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"content_block":{"type":"tool_use","id":"call_1","name":"read_file","input":{}`) {
		t.Errorf("tool start missing: %s", joined)
	}
	if !strings.Contains(joined, `"partial_json":"{\"path\":\"`) { t.Errorf("first delta missing: %s", joined) }
	if !strings.Contains(joined, `"partial_json":"/etc/hosts\"}`) { t.Errorf("second delta missing: %s", joined) }
	if !strings.Contains(joined, `"stop_reason":"tool_use"`) { t.Errorf("stop reason: %s", joined) }
}

func TestStreamRefusalAsText(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"refusal\":\"I cannot\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"content_filter\"}}]}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"text":"I cannot"`) { t.Errorf("refusal text missing: %s", joined) }
	if !strings.Contains(joined, `"stop_reason":"refusal"`) { t.Errorf("stop reason: %s", joined) }
}

func TestStreamUsageBuckets(t *testing.T) {
	body := "data: {\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":20,\"cache_read_input_tokens\":30,\"cache_creation_input_tokens\":10}}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n" +
		"data: {\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":20}}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"input_tokens":60`) { t.Errorf("input = prompt - cache: %s", joined) }
	if !strings.Contains(joined, `"cache_read_input_tokens":30`) { t.Errorf("cache_read: %s", joined) }
	if !strings.Contains(joined, `"cache_creation_input_tokens":10`) { t.Errorf("cache_creation: %s", joined) }
	// message_start 里 output_tokens=1，message_delta 里真实值
	if !strings.Contains(joined, `"output_tokens":20`) { t.Errorf("output: %s", joined) }
}

func TestStreamUsageFallbackDetails(t *testing.T) {
	body := "data: {\"usage\":{\"prompt_tokens\":100,\"prompt_tokens_details\":{\"cached_tokens\":40,\"cache_write_tokens\":5}}}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"input_tokens":55`) { t.Errorf("prompt - cached - write = 55: %s", joined) }
	if !strings.Contains(joined, `"cache_read_input_tokens":40`) { t.Errorf("cache_read fallback: %s", joined) }
	if !strings.Contains(joined, `"cache_creation_input_tokens":5`) { t.Errorf("cache_creation fallback: %s", joined) }
}

func TestStreamUpstreamErrorObject(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n" +
		"data: {\"error\":{\"message\":\"rate limited\",\"code\":429}}\n\n"
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err == nil { t.Fatal("expected UpstreamStreamError") }
	var usErr *UpstreamStreamError
	if !errors.As(err, &usErr) || usErr.Code != 429 { t.Fatalf("err = %v", err) }
	// 已有内容 → incomplete notice 分支
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, "API Error: Server error mid-response. The response above may be incomplete.") {
		t.Errorf("notice missing: %s", joined)
	}
}

func TestStreamAbortBeforeContent(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{}}]}\n\n" // 无 finish_reason、无 [DONE]
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err == nil { t.Fatal("expected UpstreamAbortedError") }
	var ab *UpstreamAbortedError
	if !errors.As(err, &ab) || ab.Subtype != "response_stalled" { t.Fatalf("err = %v", err) }
	if len(ev) != 1 { t.Errorf("only message_start expected, got %d events", len(ev)) }
}

func TestStreamConnectionClosedMidStream(t *testing.T) {
	chunks := []openai.Chunk{
		{Choices: []openai.Choice{{Delta: &openai.Delta{Content: strPtr("partial")}}}},
		{Err: errors.New("connection reset by peer")},
	}
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil, nil)
	ev, err := collect(t, st)
	if err != nil { t.Fatalf("content existed → graceful completion, err = %v", err) }
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, "API Error: Connection closed mid-response. The response above may be incomplete.") {
		t.Errorf("notice missing: %s", joined)
	}
	if !strings.Contains(joined, "message_stop") { t.Errorf("must complete gracefully: %s", joined) }
}

func TestStreamDoneWithoutFinishReason(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\n" +
		"data: [DONE]\n\n" // 无 finish_reason 但 [DONE]
	ev := allEvents(t, body, req(t, "m1"))
	if !strings.Contains(strings.Join(ev, "\n"), `"stop_reason":"end_turn"`) { t.Errorf("should be graceful stop: %v", ev) }
}

func TestStreamEmptyOutputGetsSpace(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"text":" "`) { t.Errorf("space text block missing: %s", joined) }
}

func TestStreamOrphanToolNameInference(t *testing.T) {
	request := req(t, "m1")
	request.Tools = []map[string]any{{"type": "custom", "name": "read_file", "input_schema": map[string]any{}}}
	body := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{}\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, request)
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"name":"read_file"`) { t.Errorf("inferred name missing: %s", joined) }
}

func TestStreamStopSequenceDetection(t *testing.T) {
	request := req(t, "m1")
	request.StopSequences = []string{"</output>"}
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"done</output>\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, request)
	if !strings.Contains(strings.Join(ev, "\n"), `"stop_sequence":"</output>"`) { t.Errorf("stop_sequence missing: %v", ev) }
}

func TestStreamSkipMessageLifecycle(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n" +
		"data: [DONE]\n\n"
	chunks := chunksFromSSE(t, body)
	st := NewStreamer(context.Background(), seqFromSlice(chunks), req(t, "m1"), 10, true, nil,
		&Options{SkipMessageLifecycle: true, StartingBlockIndex: 3})
	ev, err := collect(t, st)
	if err != nil { t.Fatal(err) }
	for _, e := range ev {
		if strings.HasPrefix(e, "event: message_") { t.Errorf("lifecycle events must be skipped: %s", e) }
	}
	if !strings.Contains(strings.Join(ev, "\n"), `"index":3`) { t.Errorf("starting index: %v", ev) }
}

func TestStreamHeuristicToolParsing(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"● <function=read_file><parameter=path>/x</parameter>\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"content_block":{"type":"tool_use"`) { t.Errorf("heuristic tool block missing: %s", joined) }
	if !strings.Contains(joined, `"name":"read_file"`) { t.Errorf("name missing: %s", joined) }
}

func TestExtractUsageInfo(t *testing.T) {
	u := ExtractUsageInfo(&openai.Usage{PromptTokens: 100, CompletionTokens: 20,
		CacheReadInputTokens: int64Ptr(30), CacheCreationInputTokens: int64Ptr(10)})
	if u.PromptTokens != 100 || u.CacheReadInputTokens != 30 { t.Errorf("u = %+v", u) }
	u2 := ExtractUsageInfo(&openai.Usage{PromptTokens: 100, PromptTokensDetails: &openai.PromptTokensDetails{CachedTokens: int64Ptr(5), CacheWriteTokens: int64Ptr(2)}})
	if u2.CacheReadInputTokens != 5 || u2.CacheCreationInputTokens != 2 { t.Errorf("u2 = %+v", u2) }
	u3 := ExtractUsageInfo(&openai.Usage{PromptTokens: 100, PromptTokensDetails: &openai.PromptTokensDetails{CachedTokens: int64Ptr(0)}})
	if u3.CacheReadInputTokens != 0 { t.Errorf("0 should stay 0: %+v", u3) }
	if ExtractUsageInfo(nil) != nil { t.Error("nil usage → nil") }
}

func TestBuildIncompleteNotice(t *testing.T) {
	if got := BuildIncompleteNotice(&UpstreamStreamError{}); !strings.Contains(got, "Server error mid-response") { t.Errorf("got %q", got) }
	if got := BuildIncompleteNotice(&UpstreamAbortedError{Subtype: "connection_closed"}); !strings.Contains(got, "Connection closed mid-response") { t.Errorf("got %q", got) }
	if got := BuildIncompleteNotice(&UpstreamAbortedError{Subtype: "response_stalled"}); !strings.Contains(got, "Response stalled mid-stream") { t.Errorf("got %q", got) }
	if got := BuildIncompleteNotice(errors.New("x")); !strings.Contains(got, "Response stalled mid-stream") { t.Errorf("got %q", got) }
}

func TestInferToolNameByIndex(t *testing.T) {
	request := req(t, "m")
	request.Tools = []map[string]any{{"name": "a"}, {"name": "b"}}
	if InferToolNameByIndex(request, 0) != "a" { t.Error("index 0") }
	if InferToolNameByIndex(request, 1) != "b" { t.Error("index 1") }
	if InferToolNameByIndex(request, 5) != "" { t.Error("out of range") }
	if InferToolNameByIndex(req(t, "m"), 0) != "" { t.Error("no tools") }
}

func TestStreamTaskRunInBackgroundForced(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c1\",\"function\":{\"name\":\"Task\",\"arguments\":\"{\\\"run_in_background\\\":true,\\\"description\\\":\\\"d\\\"}\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}}]}\n\n" +
		"data: [DONE]\n\n"
	ev := allEvents(t, body, req(t, "m1"))
	joined := strings.Join(ev, "\n")
	if !strings.Contains(joined, `"partial_json"`) { t.Errorf("task delta: %s", joined) }
}

func strPtr(s string) *string { return &s }
func int64Ptr(i int64) *int64 { return &i }
```

（需要 `import "iter"`；`t.Context()` 需 Go 1.24+，本机 1.26 OK。）

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/stream/` — Expected: FAIL

- [ ] **Step 3: 实现 stream.go**

端口 `../chat-to-claude-code/src/transport/stream.ts` 的结构。**核心骨架**：

```go
type Streamer struct {
	ctx             context.Context
	chunks          iter.Seq[openai.Chunk]
	req             *convert.RequestData
	inputTokens     int64
	thinkingEnabled bool
	dump            *dump.Session
	opts            *Options

	sse             *sse.Builder
	thinkParser     *parsers.ThinkTagParser
	heuristicParser *parsers.HeuristicToolParser
	toolArgAccum    map[int]string
	finishReason    string
	seenDone        bool
	usageInfo       *openai.Usage
	err             error
}

func NewStreamer(...) *Streamer {
	// messageID := "msg_" + uuidV4()
	// signing secret 派生在 sse.Builder 内部（NewBuilder 处理）
	...
}

func (s *Streamer) Events() iter.Seq[string] {
	return func(yield func(string) bool) {
		s.run(yield)
	}
}

func (s *Streamer) Err() error { return s.err }
```

`run(yield)` 结构（对照 stream.ts 主流程）：
1. `sse := sse.NewBuilder(messageID, req.Model, inputTokens, nil)`；`SetNextIndex(opts.StartingBlockIndex)`。
2. 非 skipMessageLifecycle → `yield(sse.MessageStart())`。
3. `for chunk := range s.chunks`：
   - `chunk.Err != nil` → 进入错误处理（等价 TS iterUpstreamChunks 抛 `UpstreamAbortedError(connection_closed)`）。
   - `chunk.Done` → `seenDone = true; break`。
   - `chunk.Usage != nil` → 合并 usageInfo（`ExtractUsageInfo` 结果合并进 sse.SetUsage，fresh wins）。
   - `chunk.Error != nil` → 抛 `UpstreamStreamError{Code: chunk.Error.Code}` 语义 → 进入错误处理分支。
   - `len(chunk.Choices) == 0` → continue；`finishReason` 记录；delta 处理：reasoning_content → EnsureThinkingBlock + AddThinkingText + ContentBlockDelta(thinking_delta)；refusal → EnsureTextBlock + TextDelta + continue；content → thinkParser.Feed → THINKING 走 thinking 块 / TEXT 走 heuristicParser.Feed → 文本与工具分流；tool_calls → heuristicParser.Flush 先冲刷 → CloseContentBlocks → 每 tc：累积 toolArgAccum、processToolCall（对应 TS processToolCall：setStreamToolId、registerToolName、preStartArgs 缓冲、未 started 且有名 → StartToolBlock、ensure 后发 delta）。
4. 循环后 `finishReason == ""`：`seenDone` → `finishReason = "stop"`；否则 → 错误处理（response_stalled）。
5. **错误处理分支**（TS catch 块逻辑）：thinkParser.Flush + heuristicParser.Flush 冲刷 → CloseAllBlocks → `hadContent` 判定（accumulatedText/Reasoning 非空、textStarted/thinkingStarted、hasEmittedToolBlock）→ 有内容：EnsureTextBlock + TextDelta(BuildIncompleteNotice(err)) + CloseContentBlocks → skipMessageLifecycle 或 MessageDelta(mapStopReason, completion, nil) + MessageStop → 若 dump != nil 且 opts.IsDownstreamAborted != nil 且 !IsDownstreamAborted() → `dump.RecordUpstreamTermination(UpstreamAbort, time.Now().UTC().Format(time.RFC3339))` → 返回（`s.err = nil`）。无内容：`s.err = err` → 返回。
6. 正常收尾：thinkParser.Flush、heuristicParser.Flush 冲刷 → 孤儿工具解析（遍历 `toolStates`：未 started 且有 preStartArgs 或 name → InferToolNameByIndex → 有名则 CloseContentBlocks + StartToolBlock + repairTruncatedJson(preStartArgs) 发 delta + 更新 toolArgAccum；无名 → 标记 hasOrphanedToolStates + 删除状态）→ 空内容补 `" "` 文本块 → flushTaskArgBuffers（JSON 修复 + run_in_background 强制 false + 无效 JSON 哈希告警）→ CloseAllBlocks → completion = usageInfo 或 EstimateOutputTokens → effectiveFinishReason = orphaned ? "stop" : finishReason → detectStopSequence → skipMessageLifecycle 或 MessageDelta + MessageStop。

**工具函数端口**（stream.ts 逐行）：
- `repairTruncatedJson`：括号/引号配平（含转义引号处理、`inString` 状态机）。
- `detectStopSequence`：`text` 以任一 stop_sequence 结尾。
- `extractUsageInfo` 按 TS（两个回退；cached 0 视为无——TS `details?.cached_tokens && >0`）。
- `inferToolNameByIndex`。
- `iterHeuristicToolUseSse`：Task 工具强制 `run_in_background=false` → CloseContentBlocks → ContentBlockStart(tool_use) + ContentBlockDelta(input_json_delta, JSON.stringify(input)) + ContentBlockStop——**注意**：JSON.stringify(input) 用 `json.Marshal`（Go map 排序，与 TS 插入序不同——但 TS 里 `JSON.stringify(toolUse.input)` 中 input 来自启发式解析的 JS 对象（`_currentParameters` 按插入序）。为行为一致，启发式工具 input 是 map[string]any——**决策**：此处输出用 `CanonicalJSONStringify`（排序键），与上游 canonical 策略一致；行为差异可接受（客户端解析 JSON 不关心键序）。
- `isThinkingEnabled`：hint 非空用之，否则 true（TS 默认注释掉了模型名启发——保持 true）。

**UUID 生成**：`msg_` 前缀用 crypto/rand 16 字节 → v4 格式字符串。`tool_` 同理。`toolu_heuristic_` 8 hex。

- [ ] **Step 4: 运行确认通过 + 补全向量**

Run: `go test ./internal/stream/` — Expected: PASS。补全：`../chat-to-claude-code/tests/stream.test.ts`（769 行，单元级 chunk 序列）与 `sse_stream.test.ts`（1239 行，路由集成级——后者归入 Task 10 的集成测试，本任务只补 stream.test.ts 的向量）。

- [ ] **Step 5: 提交**

```bash
git add internal/stream
git commit -m "feat: openai to anthropic SSE stream converter

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 10: proxy 服务器层

**Files:**
- Create: `internal/proxy/server.go`
- Test: `internal/proxy/server_test.go`

**Interfaces:** Consumes: `config.Config`、`anthropic.MessagesRequest`、`convert.*`、`openai.IterSSEChunks`、`stream.*`、`dump.Session`、`sse.*`。Produces: `proxy.NewHandler(cfg *config.Config) http.Handler`。

- [ ] **Step 1: 写失败的集成测试**

`internal/proxy/server_test.go`（httptest mock 上游；SSE 事件解析辅助与 TS 测试一致）：

```go
package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"chat-to-messages/internal/config"
)

// mockUpstream 返回吐 SSE 文本的上游服务器
func mockUpstream(t *testing.T, body string, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func testConfig(upstreamURL string) *config.Config {
	return &config.Config{
		UpstreamBaseURL: upstreamURL,
		UpstreamAPIKey:  "sk-upstream",
		Port:            8082,
		EnableThinking:  true,
	}
}

func postMessages(t *testing.T, h http.Handler, body string, headers map[string]string) *http.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Result()
}

type sseEvent struct{ event, data string }

func parseSSE(t *testing.T, body string) []sseEvent {
	t.Helper()
	var out []sseEvent
	for _, block := range strings.Split(body, "\n\n") {
		block = strings.TrimSpace(block)
		if block == "" { continue }
		var ev sseEvent
		for _, line := range strings.Split(block, "\n") {
			if strings.HasPrefix(line, "event: ") { ev.event = strings.TrimPrefix(line, "event: ") }
			else if strings.HasPrefix(line, "data: ") { ev.data = strings.TrimPrefix(line, "data: ") }
		}
		if ev.event != "" { out = append(out, ev) }
	}
	return out
}

func eventTypes(events []sseEvent) []string {
	var out []string
	for _, e := range events { out = append(out, e.event) }
	return out
}

func TestHealthAndNotFound(t *testing.T) {
	h := NewHandler(testConfig("http://unused"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/health", nil))
	if rec.Code != 200 || rec.Body.String() != `{"status":"ok"}` { t.Fatalf("health: %d %s", rec.Code, rec.Body.String()) }

	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("GET", "/nope", nil))
	if rec2.Code != 404 { t.Fatalf("404 expected, got %d", rec2.Code) }
	if !strings.Contains(rec2.Body.String(), "not_found_error") { t.Errorf("body = %s", rec2.Body.String()) }
}

func TestAuthTokenRequired(t *testing.T) {
	up := mockUpstream(t, "data: [DONE]\n\n", 200)
	cfg := testConfig(up.URL)
	cfg.AuthToken = "secret"
	h := NewHandler(cfg)

	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != 401 { t.Fatalf("401 expected, got %d", resp.StatusCode) }

	resp2 := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"x-api-key": "wrong"})
	if resp2.StatusCode != 401 { t.Fatalf("401 expected, got %d", resp2.StatusCode) }

	resp3 := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"x-api-key": "secret"})
	if resp3.StatusCode != 200 { t.Fatalf("200 expected, got %d", resp3.StatusCode) }
}

func TestMissingAPIKey(t *testing.T) {
	up := mockUpstream(t, "data: [DONE]\n\n", 200)
	cfg := testConfig(up.URL)
	cfg.UpstreamAPIKey = ""
	cfg.AuthToken = "t" // 非透传
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"x-api-key": "t"})
	if resp.StatusCode != 401 { t.Fatalf("401 expected, got %d", resp.StatusCode) }
	if !strings.Contains(resp.Body.String(), "No API key provided") { t.Errorf("body = %s", resp.Body.String()) }
}

func TestPassthroughForwardsClientKey(t *testing.T) {
	var gotAuth string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	cfg.UpstreamAPIKey = ""
	cfg.AuthToken = ""
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, map[string]string{"x-api-key": "client-key"})
	if resp.StatusCode != 200 { t.Fatalf("200 expected, got %d", resp.StatusCode) }
	if !strings.Contains(gotAuth, "client-key") { t.Errorf("client key not forwarded: %q", gotAuth) }
}

func TestInvalidJSONAndModel(t *testing.T) {
	h := NewHandler(testConfig("http://unused"))
	if resp := postMessages(t, h, `not-json`, nil); resp.StatusCode != 400 { t.Errorf("bad json: %d", resp.StatusCode) }
	if resp := postMessages(t, h, `{"messages":[]}`, nil); resp.StatusCode != 400 { t.Errorf("no model: %d", resp.StatusCode) }
	if resp := postMessages(t, h, `{"model":"m"}`, nil); resp.StatusCode != 400 { t.Errorf("no messages: %d", resp.StatusCode) }
}

func TestUpstreamErrorMapping(t *testing.T) {
	up := mockUpstream(t, `{"error":"rate limited"}`, 429)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != 429 { t.Fatalf("429 expected, got %d", resp.StatusCode) }
	if !strings.Contains(resp.Body.String(), "Upstream error:") { t.Errorf("body = %s", resp.Body.String()) }

	up5xx := mockUpstream(t, "boom", 503)
	h2 := NewHandler(testConfig(up5xx.URL))
	resp2 := postMessages(t, h2, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp2.StatusCode != 502 { t.Fatalf("502 expected, got %d", resp2.StatusCode) }
}

func TestFullStreamingFlow(t *testing.T) {
	upstreamBody := "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n" +
		"data: [DONE]\n\n"
	up := mockUpstream(t, upstreamBody, 200)
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" { t.Errorf("ct = %q", ct) }
	if resp.Header.Get("X-Accel-Buffering") != "no" { t.Error("X-Accel-Buffering missing") }
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" { t.Error("CORS missing") }

	body, _ := io.ReadAll(resp.Body)
	events := parseSSE(t, string(body))
	types := eventTypes(events)
	want := []string{"message_start", "content_block_start", "content_block_delta", "content_block_delta", "content_block_stop", "message_delta", "message_stop"}
	if strings.Join(types, ",") != strings.Join(want, ",") { t.Fatalf("types = %v\nbody = %s", types, body) }
	// 两段文本各一个 text_delta
	var textDeltas []string
	for _, e := range events {
		if e.event == "content_block_delta" && strings.Contains(e.data, "text_delta") {
			var d struct{ Delta struct{ Text string } }
			json.Unmarshal([]byte(e.data), &d)
			textDeltas = append(textDeltas, d.Delta.Text)
		}
	}
	if strings.Join(textDeltas, "") != "Hello world" { t.Errorf("text = %v", textDeltas) }
	// message_delta stop_reason
	for _, e := range events {
		if e.event == "message_delta" {
			if !strings.Contains(e.data, `"stop_reason":"end_turn"`) { t.Errorf("delta = %s", e.data) }
		}
	}
}

func TestUpstreamIncludesStreamOptions(t *testing.T) {
	var gotBody map[string]any
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		json.Unmarshal(b, &gotBody)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer up.Close()
	h := NewHandler(testConfig(up.URL))
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != 200 { t.Fatalf("status = %d", resp.StatusCode) }
	so, _ := gotBody["stream_options"].(map[string]any)
	if so == nil || so["include_usage"] != true { t.Errorf("stream_options = %v", gotBody["stream_options"]) }
	if gotBody["stream"] != true { t.Errorf("stream = %v", gotBody["stream"]) }
}

func TestCORS(t *testing.T) {
	h := NewHandler(testConfig("http://unused"))
	req := httptest.NewRequest("OPTIONS", "/v1/messages", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 204 { t.Errorf("code = %d", rec.Code) }
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" { t.Error("CORS origin missing") }
}

func TestDumpWiring(t *testing.T) {
	upstreamBody := "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n" +
		"data: [DONE]\n\n"
	up := mockUpstream(t, upstreamBody, 200)
	cfg := testConfig(up.URL)
	cfg.DumpDir = t.TempDir()
	h := NewHandler(cfg)
	resp := postMessages(t, h, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != 200 { t.Fatalf("status = %d", resp.StatusCode) }
	io.Copy(io.Discard, resp.Body)
	// dump 完成后应有 completed/ 目录
	entries, err := os.ReadDir(filepath.Join(cfg.DumpDir, "completed"))
	if err != nil || len(entries) != 1 { t.Fatalf("completed dump missing: %v", err) }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/proxy/` — Expected: FAIL（编译失败：NewHandler 未定义）

- [ ] **Step 3: 实现 server.go**

端口 `../chat-to-claude-code/src/server/routes.ts` + `index.ts` 的 HTTP 部分。**结构**：

```go
func NewHandler(cfg *config.Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions { // CORS 预检
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, x-api-key, anthropic-version")
			w.Header().Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		handleRequest(w, r, cfg)
	})
}
```

- `handleRequest`：路由 /health GET（`{"status":"ok"}`）、/v1/messages POST → handleMessages、其余 404 `{type:"error",error:{type:"not_found_error",message:"No route for GET /x"}}`。所有响应加 `Access-Control-Allow-Origin: *`。
- `handleMessages(w, r, cfg)`（对照 routes.ts handleMessages 顺序）：
  1. `dump := dump.NewSession(cfg.DumpDir)`；`requestStart := time.Now()`。
  2. 认证：`validateAuthToken`（cfg.AuthToken 为空放行；否则比较 `x-api-key` 或 `Authorization: Bearer`）失败 → 401 `authentication_error`；`resolveAPIKey`（透传：无上游 key 且无 authToken 且客户端带 key → 客户端 key；否则 cfg.UpstreamAPIKey）为空 → 401 "No API key provided. Set --upstream-api-key or enable passthrough mode (no upstream key and no auth token)."
  3. 读 body（`io.ReadAll` + `json.Decoder.UseNumber` 解到 `anthropic.MessagesRequest`）；解析失败 → 400 "Invalid JSON in request body."；`model` 缺失/非字符串 → 400；`messages` 非数组 → 400（MessagesRequest 类型直接承载校验：`req.Model == ""` / `len(req.Messages) == 0` 时构造对应错误——**注意** TS 校验 `Array.isArray(messages)` 区分"缺字段"与"空数组"：`messages: []` 是合法数组但无消息——TS 只校验 `Array.isArray`，空数组也接受（后续上游会报错）。Go 用 `[]anthropic.Message` 零值 nil 表示缺失——JSON 缺失与 `null` 都得到 nil。**忠实判定**：`MessagesRequest` 中 `Messages []Message` 为 nil 时 → 400。
  4. dump.WriteDownstreamRequest（headers 全量、datetime、body 原文 pretty 化——TS 用 `JSON.stringify(body, null, 2)`：Go 对原始 body 做 `json.MarshalIndent` 需先解码——**决策**：直接存原始 body 文本（行为改进，内容相同）。headers 用 `r.Header.Clone()` 转 map，小写键（Go 规范）。
  5. `inputTokens := estimateInputTokens(...)`（对应 core/tokens.ts：char/4 每内容块 + 每条消息 +4，最少 1——该函数放在 convert 包还是 proxy？**决策**：放 `convert` 包：`func EstimateInputTokens(messages []anthropic.Message) int64`）。
  6. server tools 检测（`servertool.IsServerToolType` 双源收集）→ 启用且存在 → `handleServerToolRequest`（Task 11 实现；本任务先留 stub 返回 501 并在 Task 11 替换）。
  7. 标准流：`buildUpstreamRequest`（BuildBaseRequestBody(4096, ReplayThinkTags) → `stream:true` → `resolveModelExtra`+`DeepMerge` → `stream_options.include_usage=true`（保留既有 stream_options 键）→ `PrepareCanonicalBody` → headers `Content-Type`/`Authorization: Bearer`）；`http.NewRequestWithContext(r.Context(), POST, baseURL+"/chat/completions", body)`；`client.Do`（`http.Client{}` 无超时——ctx 传播）。ttfb 记录。
  8. 连接失败 → 499（`r.Context().Err() != nil` 判定客户端断开——TS 用 `abortSignal.aborted || AbortError`）或 502 "Failed to connect to upstream"；dump 记 termination。
  9. 非 2xx → 读 body（截 500 字符）→ `mappedStatus = status >= 500 ? 502 : status` → `api_error` `"Upstream error: Upstream returned <status>: <body>"`。
  10. 空 body → 500 `serverError("Upstream returned empty body.")`。
  11. 成功 → SSE 输出：headers（`Content-Type: text/event-stream` + `X-Accel-Buffering: no` + `Cache-Control: no-cache` + `Connection: keep-alive` + CORS）；`w.WriteHeader(200)`；`flusher := w.(http.Flusher)`。
  12. **泵**：`streamer := stream.NewStreamer(r.Context(), openai.IterSSEChunks(r.Context(), upstreamBody, &rawBuilder), requestData, inputTokens, cfg.EnableThinking, dump, &stream.Options{IsDownstreamAborted: func() bool { return downstreamAborted }})`。单 goroutine 事件循环：

```go
evCh := make(chan string, 64)        // 泵 goroutine → 事件
go func() {                           // 泵：消费 streamer.Events()
	defer close(evCh)
	for ev := range streamer.Events() {
		select {
		case evCh <- ev:
		case <-r.Context().Done():
			return
		}
	}
	// 流结束：错误分类
	if err := streamer.Err(); err != nil && !downstreamAborted {
		terminationReason = dump.UpstreamAbort
		var line string
		switch err.(type) {
		case *stream.UpstreamAbortedError:
			line = sse.BuildRetryableMidStreamErrorSse(err.Error())
		default:
			line = sse.BuildMidStreamErrorSse(err.Error())
		}
		select {
		case evCh <- line:
		case <-r.Context().Done():
		}
	}
}()

ticker := time.NewTicker(sse.DEFAULT_PING_INTERVAL)
defer ticker.Stop()
for {
	select {
	case <-ticker.C:
		if downstreamAborted { continue }
		writeEvent(w, flusher, sse.PING_EVENT, &downstreamChunks)
	case ev, ok := <-evCh:
		if !ok { goto done }
		writeEvent(w, flusher, ev, &downstreamChunks)
	case <-r.Context().Done():
		downstreamAborted = true
		terminationReason = dump.ClientAbort
		goto done
	}
}
done:
	writeDumpFinal(...) // upstream response（原始文本）+ downstream response + timing + dump.Finish()
```

`writeEvent` 加互斥？单 goroutine 写 w——泵 goroutine 只写 channel，select 循环是唯一写 w 者 ✓。`downstreamAborted` 标志由 select 循环设置、泵 goroutine 读取——用 `atomic.Bool` 或 select 天然串行化（evCh 发送前检查）。**决策**：`atomic.Bool`。

dump 收尾（对照 finalizeDump）：guard 防双写（`sync.Once`）；`writeUpstreamResponse`（headers、status、rawBody、`dump.UpstreamTermination() ?? tracked`）；`writeDownstreamResponse`（200、下游拼接文本、tracked termination）；`SetTiming(ttfb, total)`；`Finish()`。`terminationReason` 变量在 select 循环与泵 goroutine 共享——用 `atomic.Value` 或再入 `sync.Mutex`。**决策**：泵 goroutine 在 close(evCh) 前写入自身 error 分支；select 循环在 ctx.Done 分支设置——两者竞态，用 `sync.Mutex` 保护 `terminationReason`。

- `estimateInputTokens`（convert 包）：按 tokens.ts 逐行端口（content string / blocks: text/thinking/tool_use(input+name)/tool_result(text 子块)；每消息 +4；`max(total,1)`）。

- [ ] **Step 4: 运行确认通过 + 补全集成向量**

Run: `go test ./internal/proxy/` — Expected: PASS。补全：`../chat-to-claude-code/tests/routes.test.ts`（115 行）与 `sse_stream.test.ts`（1239 行，其断言对事件序列/数据的检查 → 移植到 `parseSSE` 辅助之上；delayedReadableStream 模拟 → httptest 上游逐步 flush 或直接快照）。

- [ ] **Step 5: 提交**

```bash
git add internal/proxy/server.go internal/proxy/server_test.go
git commit -m "feat: proxy http layer with auth, passthrough and sse pump

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 11: agentic loop（服务器工具代理循环）

**Files:**
- Create: `internal/proxy/agentic.go`
- Modify: `internal/proxy/server.go`（handleMessages 中 server tools 分支接到 handleServerToolRequest，替换 Task 10 的 501 stub）
- Test: `internal/proxy/agentic_test.go`

**Interfaces:** Consumes: 全部现有包。Produces: `handleServerToolRequest`（包内函数）。

- [ ] **Step 1: 写失败的测试**

`internal/proxy/agentic_test.go`：

```go
package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"chat-to-messages/internal/config"
)

func TestAgenticLoopToolThenText(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		json.Unmarshal(body, &req)
		msgs := req["messages"].([]any)
		n := calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		if n == 1 {
			// 第一轮：返回 web_search tool_call
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"web_search\",\"arguments\":\"{\\\"query\\\":\\\"golang\\\"}\"}}]}}]}\n\n"+
				"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}}]}\n\n"+
				"data: [DONE]\n\n")
			return
		}
		// 第二轮：检查上游收到了 tool 结果消息
		found := false
		for _, m := range msgs {
			mm := m.(map[string]any)
			if mm["role"] == "tool" {
				content := mm["content"].(string)
				if strings.Contains(content, "web_search_result") { found = true }
			}
		}
		if !found { t.Error("upstream did not receive tool result") }
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"final answer\"}}]}\n\n"+
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n"+
			"data: [DONE]\n\n")
	}))
	defer up.Close()

	cfg := testConfig(up.URL)
	cfg.ServerTools = config.ServerToolConfig{WebSearch: true, WebSearchEngine: "searxng", WebSearchBaseURL: up.URL}
	h := NewHandler(cfg)

	body := `{"model":"m","messages":[{"role":"user","content":"search golang"}],
		"server_tools":[{"type":"web_search_20250305","name":"web_search"}]}`
	resp := postMessages(t, h, body, nil)
	if resp.StatusCode != 200 { t.Fatalf("status = %d", resp.StatusCode) }
	raw, _ := io.ReadAll(resp.Body)
	events := parseSSE(t, string(raw))
	if calls.Load() != 2 { t.Errorf("upstream calls = %d, want 2", calls.Load()) }

	// 工具结果以文本块呈现（不出现 server_tool_use）
	joined := string(raw)
	if strings.Contains(joined, "server_tool_use") { t.Errorf("server_tool_use must not be emitted: %s", joined) }
	if !strings.Contains(joined, "[Web Search Results]") { t.Errorf("search results text missing: %s", joined) }
	if !strings.Contains(joined, "final answer") { t.Errorf("final text missing: %s", joined) }
	// 正常收尾
	types := eventTypes(events)
	if types[len(types)-1] != "message_stop" { t.Errorf("last = %v", types) }
}

func TestAgenticLoopMaxIterations(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c\",\"function\":{\"name\":\"web_search\",\"arguments\":\"{\\\"query\\\":\\\"q\\\"}\"}}]}}]}\n\n"+
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}}]}\n\n"+
			"data: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	cfg.ServerTools = config.ServerToolConfig{WebSearch: true, WebSearchEngine: "searxng", WebSearchBaseURL: up.URL}
	h := NewHandler(cfg)
	body := `{"model":"m","messages":[{"role":"user","content":"x"}],"server_tools":[{"type":"web_search_20250305","name":"web_search"}]}`
	resp := postMessages(t, h, body, nil)
	if resp.StatusCode != 200 { t.Fatalf("status = %d", resp.StatusCode) }
	if calls.Load() != 5 { t.Errorf("calls = %d, want 5 (MAX_ITERATIONS)", calls.Load()) }
	io.Copy(io.Discard, resp.Body)
}

func TestAgenticLoopTextEmbeddedToolCall(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Let me search <tool_use>{\\\"name\\\":\\\"web_search\\\",\\\"input\\\":{\\\"query\\\":\\\"q\\\"}}</tool_use> fake result\"}}]}\n\n"+
				"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n"+
				"data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\n"+
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}}]}\n\n"+
			"data: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	cfg.ServerTools = config.ServerToolConfig{WebSearch: true, WebSearchEngine: "searxng", WebSearchBaseURL: up.URL}
	h := NewHandler(cfg)
	body := `{"model":"m","messages":[{"role":"user","content":"x"}],"server_tools":[{"type":"web_search_20250305","name":"web_search"}]}`
	resp := postMessages(t, h, body, nil)
	if resp.StatusCode != 200 { t.Fatalf("status = %d", resp.StatusCode) }
	if calls.Load() != 2 { t.Errorf("calls = %d, want 2", calls.Load()) }
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "done") { t.Errorf("final text missing: %s", raw) }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/proxy/ -run TestAgentic` — Expected: FAIL（501 stub）

- [ ] **Step 3: 实现 agentic.go**

端口 `../chat-to-claude-code/src/server/routes.ts` 的 `handleServerToolRequest`（697-1223 行）。要点：
- `MAX_ITERATIONS = 5`。
- 初始上游 body：`buildUpstreamRequestBodyOnly`（同标准流但不含 server tool schema 的 tools？——**不**：TS 的 `buildUpstreamRequestBodyOnly` 就是完整 body（含 server tool function schema，因为 Claude Code 的 server tools 在 tools 数组里）。保留。
- 解析初始 body 拿 `messages` 数组与 `tools`；循环内 `currentBody = prepareCanonicalBody({model, messages, max_tokens: req.MaxTokens ?? 32000, stream:true, stream_options:{include_usage:true}, tools(若有), thinking:{type:"enabled"}})`——注意 TS 在 loop 与 final 的 body 中**强制 thinking enabled**（与初始 body 无关的独立构造）。
- 每轮：POST → 非 2xx/连接失败 → dump 分类终止并返回错误；`collectToolCallArguments`（消费 IterSSEChunks，聚 index→{id,name,arguments}，记录 finishReason 与 textContent，**注意**：TS 的 collect 里 `chunk.usage` 也流动但未收集——Go 同样忽略）→ `serverToolCalls`（name ∈ web_search/web_fetch 且对应 enable）→ 若空则 `DetectServerToolInText(textContent)` → 都空 → break。
- 原生 tool_calls 路径：assistant 消息（content=textContent||null、tool_calls 全部）入历史；逐个 server tool 执行（`executeServerToolCall` → 解析 JSON → `servertool.ExecuteWebSearch/ExecuteWebFetch` → `FormatWebSearch/WebFetchResultContent` → tool 消息 `srvtool_<Date.Now()>` tool_call_id——**注意** TS 此处 tool_call_id 用 `srvtool_${Date.now()}`（毫秒时间戳），非 tc.id；而 tool 消息的 `tool_call_id` 用 `tc.id`。照抄。）；非 server tool → 占位 `"Tool execution not supported in server tool mode."`；serverToolEvents 记录（server_tool_use / web_search_tool_result / web_fetch_tool_result，web_fetch 的 status=error 判定：内容块含 `Status: 4` 前缀文本）。
- 文本嵌入路径：`StripToolUseFromText(textContent)` 作 assistant content；合成 tool_calls（`srvtool_<rand12>` id）；执行同前。
- 循环结束后 final 请求 → SSE 输出：`message_start` → 工具结果转文本块（web_search → `[Web Search Results]\n<title>\n<url>\n<snippet>` 序列、web_fetch → `[Web Fetch Result]\n<text>`）→ 内层 `NewStreamer(..., &Options{SkipMessageLifecycle:true, StartingBlockIndex: sse.NextIndex()})`（tee 捕获 usage：`iter.Pull` 包一层，记录 ExtractUsageInfo 最新值）→ 事件全部输出 → `sse.SetUsage(finalUsage)` → `message_delta("end_turn", completion)` + `message_stop` → dump 收尾（completed）。
- 流中断：错误分类同 Task 10（UpstreamAbortedError → retryable）；dump termination upstream_abort。
- `buildUpstreamRequestBodyOnly` 与 `buildUpstreamRequest` 共用一个 helper（`buildBaseBody` + merge + includeUsage + canonical）。

- [ ] **Step 4: 运行确认通过 + 补全向量**

Run: `go test ./internal/proxy/ -run TestAgentic -v` — Expected: PASS。补全：`../chat-to-claude-code/tests/routes_server_tools.test.ts`（155 行）案例。

- [ ] **Step 5: 提交**

```bash
git add internal/proxy/agentic.go internal/proxy/agentic_test.go
git commit -m "feat: server tool agentic loop with text fallback detection

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 12: main.go 入口 + 启动横幅

**Files:**
- Create: `main.go`
- Modify: 无
- Test: 手动冒烟（Go 集成测试不便覆盖 main；以构建 + 运行验证）

**Interfaces:** Consumes: `config.Load`、`proxy.NewHandler`。

- [ ] **Step 1: 实现 main.go**

```go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"chat-to-messages/internal/config"
	"chat-to-messages/internal/proxy"
)

func main() {
	cfg := config.Load(os.Args[1:])

	handler := proxy.NewHandler(cfg)
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: handler,
	}

	// 启动横幅（与 TS 版格式一致，名称改为 chat-to-messages）
	passthrough := cfg.UpstreamAPIKey == "" && cfg.AuthToken == ""
	fmt.Printf("chat-to-messages listening on http://localhost:%d\n", cfg.Port)
	fmt.Printf("  Upstream: %s\n", cfg.UpstreamBaseURL)
	fmt.Printf("  Upstream API key: %s\n", boolWord(cfg.UpstreamAPIKey != ""))
	fmt.Printf("  Auth token: %s\n", boolWord(cfg.AuthToken != ""))
	fmt.Printf("  Passthrough mode: %v\n", passthrough)
	fmt.Printf("  Thinking: %v\n", cfg.EnableThinking)
	fmt.Printf("  Dump: %s\n", orDisabled(cfg.DumpDir))
	if len(cfg.ModelOverrides) > 0 {
		fmt.Printf("  Model overrides:\n")
		for _, e := range cfg.ModelOverrides {
			extra, _ := json.Marshal(e.Extra)
			fmt.Printf("    %s -> %s\n", e.Pattern, extra)
		}
	}
	fmt.Printf("  Web Search: %v\n", cfg.ServerTools.WebSearch)
	fmt.Printf("  Web Fetch: %v\n", cfg.ServerTools.WebFetch)
	if cfg.ServerTools.WebSearch {
		fmt.Printf("    Search engine: %s\n", cfg.ServerTools.WebSearchEngine)
		fmt.Printf("    Search base URL: %s\n", cfg.ServerTools.WebSearchBaseURL)
		fmt.Printf("    Search API key: %s\n", boolWord(cfg.ServerTools.WebSearchAPIKey != ""))
	}
	if cfg.ServerTools.WebFetch {
		if len(cfg.ServerTools.WebFetchAllowedDomains) > 0 {
			fmt.Printf("    Allowed domains: %s\n", joinAll(cfg.ServerTools.WebFetchAllowedDomains))
		}
		if len(cfg.ServerTools.WebFetchBlockedDomains) > 0 {
			fmt.Printf("    Blocked domains: %s\n", joinAll(cfg.ServerTools.WebFetchBlockedDomains))
		}
		fmt.Printf("    Max content tokens: %d\n", cfg.ServerTools.WebFetchMaxContentTokens)
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		srv.Close()
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func boolWord(b bool) string { if b { return "configured" }; return "not set" }
func orDisabled(s string) string { if s == "" { return "disabled" }; return s }
func joinAll(s []string) string { return strings.Join(s, ", ") }
```

（TS 横幅中 Upstream API key/Auth token 输出 `configured`/`not set`、Dump 输出 `disabled`——保持。`syscall.SIGTERM` 在 Windows 可用但实际只触发 Interrupt；保留双监听无妨。）

- [ ] **Step 2: 构建 + 冒烟**

Run:
```bash
go build -o chat-to-messages.exe .
./chat-to-messages.exe --port 18082 &   # Windows 下另开终端或 run_in_background
curl http://localhost:18082/health
curl -s -X POST http://localhost:18082/v1/messages -H "Content-Type: application/json" -d '{"model":"m","messages":[]}'
```
Expected: 横幅输出、`/health` 返回 `{"status":"ok"}`、messages 缺 model 场景按上游行为（无上游响应 → 502 或连接失败路径）。冒烟后结束进程。

- [ ] **Step 3: 提交**

```bash
git add main.go
git commit -m "feat: main entry with startup banner and graceful shutdown

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 13: 交付文档 + Dockerfile + 终验

**Files:**
- Create: `README.md`、`README-zh.md`、`Dockerfile`、`LICENSE`
- Modify: 无

- [ ] **Step 1: Dockerfile**

```dockerfile
FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod ./
COPY main.go ./
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o chat-to-messages .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=builder /build/chat-to-messages /usr/local/bin/chat-to-messages
EXPOSE 8082
ENTRYPOINT ["chat-to-messages"]
```

- [ ] **Step 2: README.md / README-zh.md**

以 `../chat-to-claude-code/README.md` 与 `README-zh.md` 为模板改写：项目名/命令改为 `chat-to-messages`；Bun 安装/`bun run` 指令替换为 `go build`（单二进制 `chat-to-messages(.exe)`）；其余（设计哲学、CLI 参数表、passthrough、dump、extra-params、转换细节、SSE 映射表、Docker 部署、API 端点）原样保留。README-zh.md 对应中文版。

- [ ] **Step 3: LICENSE**

MIT（版权年份与原作者一致——从 chat-to-claude-code 仓库复制 LICENSE，作者名保留；新项目名与描述更新）。

- [ ] **Step 4: 终验**

Run:
```bash
go vet ./...
go test ./... -race
go build -o chat-to-messages.exe .
```
Expected: 全部 PASS、vet 干净、构建成功。

- [ ] **Step 5: 提交**

```bash
git add README.md README-zh.md Dockerfile LICENSE
git commit -m "docs: project docs, dockerfile and license

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## 自审记录

**1. Spec 覆盖核对**（对照设计文档各节）：
- §2.1 CLI 参数表 → Task 1 ✓；§2.2 端点/CORS → Task 10 ✓；§2.3 SSE 事件 → Task 4+9+10 ✓；§2.4 错误格式 → Task 8(错误)+10 ✓；§2.5 dump 结构 → Task 5+10 ✓
- §3.2 关键设计决策（typed 类型→Task 2、UseNumber→Global Constraints+各任务、iter.Seq→Task 3+9、单 goroutine 泵→Task 10、迷你解析器→Task 1、错误类型→Task 9）✓
- §3.3 数据流 → Task 10 覆盖 ✓；§3.4 错误处理 → Task 10/11 ✓；§3.5 转换层要点 → Task 8 ✓；§3.6 流层要点 → Task 9 ✓；§3.7 服务器工具 → Task 7+11 ✓
- §4 测试策略 → 各任务测试 + Task 3 说明（sse_stream.test.ts 向量归 Task 10）✓
- §5 交付物（README/Dockerfile/LICENSE/横幅）→ Task 12+13 ✓

**2. 占位符扫描**：无 TBD/TODO；所有步骤含实际代码或明确的端口指令（引用具体 TS 文件与行号）。

**3. 类型一致性**：`ConvertMessages` 在 Task 8 决策为 `([]map[string]any, error)` 双返回值——stream 包（Task 9）的 `BuildBaseRequestBody` 调用点同步处理 error（BuildBaseRequestBody 返回 `(map[string]any, error)`）；Task 9 测试签名一致；Task 10 的 buildUpstreamRequest 处理 error → 500。`ContentValue.String()/Blocks()` 方法在 Task 2 定义、Task 6+ 使用 ✓。`dump.ServerToolLogEntry` Task 5 定义、Task 7 servertool.LogFn 使用 ✓。`sse.UsageInfo` Task 4 定义、Task 9 ExtractUsageInfo 返回 ✓。`openai.IterSSEChunks` Task 3 定义、Task 9/10/11 使用 ✓。

**4. 已知偏差（记录在案）**：
- dump 的 downstream-request.log 保存原始 body 文本而非 pretty-print（内容相同，格式改进）。
- 启发式工具 input 的 JSON 序列化用规范键序（客户端解析不受影响）。
- `ConvertMessages`/`BuildBaseRequestBody` 以显式 error 替代 TS throw（行为等价：OpenAIConversionError → 500）。

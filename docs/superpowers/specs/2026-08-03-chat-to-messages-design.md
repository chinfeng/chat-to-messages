# chat-to-messages 设计文档（chat-to-claude-code 的 Go 迁移）

日期：2026-08-03
状态：已获用户批准（2026-08-03）

## 1. 背景与目标

将 [chat-to-claude-code](https://github.com/chinfeng/chat-to-claude-code)（TypeScript/Bun，零依赖）迁移为 Go 实现，新项目 `chat-to-messages` 建在 `C:\git\migrate-to-golang\chat-to-messages`，独立 git 仓库。

- 该工具将 Anthropic Messages API ↔ OpenAI Chat Completions 双向转换，使 Claude Code CLI 可直连任意 OpenAI 兼容上游。
- 上游参考实现约 5,000 行 TS 源码 + 4,400 行测试。

**用户已确认的决策：**

1. **完整功能对等** — 全部功能（协议转换、流式 SSE、thinking/tool 处理、web_search/web_fetch 代理工具 + agentic loop、`--dump` 转储、认证/透传、`--upstream-extra-params` 深度合并）都实现。
2. **行为兼容，允许改进** — CLI 参数、SSE 事件序列、错误格式、dump 目录结构与 TS 版一致；允许 Go 化内部实现与修复已知问题。
3. **方案 B：彻底 Go 化重设计** — 包结构按 Go 惯例设计，协议类型用 typed struct，流处理用 `iter.Seq`，并发用 context；行为契约不变。
4. **纯标准库** — 只用 Go 标准库（net/http、encoding/json、crypto/sha256 等），延续 TS 版零依赖哲学。
5. **命名** — 新项目统一使用 `chat-to-messages`（启动横幅、User-Agent 等一并更新）。

## 2. 兼容性契约（不可变行为）

以下行为与 TS 版对等，是验证迁移正确性的基准：

### 2.1 CLI 参数（名称/默认值/语义不变）

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--upstream-base-url` | `https://api.openai.com/v1` | 上游端点 |
| `--upstream-api-key` | `""` | 上游 API key |
| `--auth-token` | `""` | 下游认证 token |
| `--port` | `8082` | 监听端口 |
| `--enable-thinking` / `--no-enable-thinking` | `true` | thinking 转换 |
| `--upstream-extra-params` | — | `glob=JSON`，可重复，首个匹配生效 |
| `--dump` | `""` | 转储目录 |
| `--enable-web-search` / `--no-enable-web-search` | `false` | 代理侧 web_search |
| `--web-search-engine` | `brave` | `brave` 或 `searxng` |
| `--enable-web-fetch` / `--no-enable-web-fetch` | `false` | 代理侧 web_fetch |
| `--web-search-api-key` | `""` | Brave key（SearXNG 可选） |
| `--web-search-base-url` | `https://api.search.brave.com` | 搜索 API base URL |
| `--web-fetch-allowed-domain` | — | 可重复 |
| `--web-fetch-blocked-domain` | — | 可重复 |
| `--web-fetch-max-content-tokens` | `5000` | 抓取内容上限 |

语法支持：`--x value`、`--x=value`、`--no-x` 否定、可重复 flag。`--upstream-extra-params` 的 JSON 值必须是对象，解析失败时告警跳过（行为同 TS）。

### 2.2 端点

| 路径 | 方法 | 说明 |
|---|---|---|
| `/v1/messages` | POST | 核心代理端点 |
| `/health` | GET | 健康检查 `{"status":"ok"}` |
| 其他 | — | 404，Anthropic 格式错误体 |

CORS：OPTIONS 预检 204 + 全响应 `Access-Control-Allow-Origin: *`。

### 2.3 SSE 事件（下游，逐字节对等）

- 事件行格式 `event: <type>\ndata: <json>\n\n`。
- 事件序列与 TS 版一致：`message_start`（`output_tokens: 1`）→ `content_block_start/delta/stop`（thinking/text/tool_use）→ `message_delta`（usage 三桶 + thinking_tokens）→ `message_stop`。
- `event: ping` 心跳每 15s。
- thinking 块关闭前 `signature_delta`（SHA-256(per-request secret + 累积 thinking 文本)）。
- 流中错误：顶层 `event: error`，`error.type = "stream_error"`；连接中断类在 message 前嵌 `{"type":"overloaded_error"} ` 前缀触发客户端重试（claude-code `sym` 谓词匹配）。
- 停止原因映射：stop→end_turn、length→max_tokens、tool_calls→tool_use、content_filter→refusal、其他→end_turn。
- 无输出时补 `" "` 文本块；有输出时失败追加 Claude 标准 incomplete notice（`API Error: <Server error mid-response|Connection closed mid-response|Response stalled mid-stream>. The response above may be incomplete.`）后正常收尾。

### 2.4 错误响应（Anthropic 格式）

`{type:"error", error:{type, message}}`。类型：`invalid_request_error`(400)、`authentication_error`(401)、`not_found_error`(404)、`api_error`(500/502/499)；`upstreamError` message 带 `Upstream error: ` 前缀；`model` 缺失→400、`messages` 非数组→400。

### 2.5 dump 目录结构

`<dump>/in-progress/<id>/` 写入 4 个日志（`downstream-request.log`、`downstream-response.log`、`upstream-request.log`、`upstream-response.log`）+ 可选 `server-tools.log`，完成后改名到分类桶：

- `completed/`、`client-aborted/`、`upstream-aborted/`、`failed/`（upstream_timeout/upstream_error）
- 终止原因优先级：`client_abort` > 流层记录的上游真实终止（`recordUpstreamTermination`）> 追踪值
- 日志 `[Section]` 文本格式、Timing（TTFB/Total）、日志尾缀 `{id}__START_<ts>__END_<ts>` 与 TS 版对等。

## 3. 架构（Go 化重设计）

### 3.1 目录结构

```
chat-to-messages/
├── go.mod                     # module chat-to-messages, go 1.26, 纯标准库
├── main.go                    # CLI 入口：加载配置、启动横幅、http.Server
└── internal/
    ├── config/
    │   ├── config.go          # Config 结构体 + 迷你参数解析器（复刻 TS getArg/getBool/getMultiArg）
    │   └── merge.go           # deepMerge（$delete/$default 元操作）+ globMatch
    ├── anthropic/
    │   └── types.go           # 请求、消息、内容块联合体、SSE 事件结构
    ├── openai/
    │   └── types.go           # Chat Completions 请求、流式 chunk、usage、tool_call
    ├── convert/
    │   ├── converter.go       # Anthropic → OpenAI 消息/系统提示/tool_choice 转换
    │   └── canonical.go       # 私有参数过滤 + 规范 JSON 序列化
    ├── parsers/
    │   ├── thinktag.go        # <think> 流式标签解析
    │   └── heuristictool.go   # ●<function=> / WebSearch / WebFetch 文本工具解析
    ├── sse/
    │   └── builder.go         # Anthropic SSE 事件构建器（事件、块管理、签名、心跳）
    ├── stream/
    │   └── stream.go          # OpenAI 流 → Anthropic SSE 流（iter.Seq 管线）
    ├── servertool/
    │   └── tools.go           # web_search（Brave/SearXNG）/ web_fetch + 文本检测
    ├── proxy/
    │   ├── server.go          # 路由、CORS、认证/透传、SSE 输出、心跳、dump 编排
    │   └── agentic.go         # 服务器工具 agentic loop（≤5 轮）
    └── dump/
        └── dump.go            # 会话、终止原因分类桶、timing、工具日志
```

### 3.2 关键设计决策

1. **typed 协议类型**：内容块为带类型标签的联合体——单结构体 + 指针/omitempty + `Extra map[string]any`（自定义 `UnmarshalJSON` 捕获未知字段），兼顾类型安全与透传。
2. **JSON 保精度**：所有需原样往返的值（tool input、extra-params、upstream body）解码用 `json.Decoder.UseNumber()`；编码 `json.Number` 原样输出。Go 编码 map 天然按键排序 → 规范 JSON 低成本实现。
3. **流管线用 `iter.Seq`**：上游 SSE 解析 → `iter.Seq[openai.Chunk]`；转换 → `iter.Seq[string]`（SSE 事件）。`iter.Pull` 供 agentic loop tee usage 数据。
4. **SSE 输出单 goroutine + select**：`{pingTicker, eventChan, ctx.Done()}`，避免写交错；下游断连靠 `r.Context().Done()` → 取消上游 fetch → dump 记 `client_abort`。
5. **迷你 CLI 解析器**（~60 行）：Go flag 包不支持 `--no-x` 与重复 flag 的 TS 语义。
6. **错误类型**：`UpstreamStreamError`（上游自报错误，不重试）与 `UpstreamAbortedError`（连接中断，`connection_closed`/`response_stalled` 子类，可重试）区分，行为同 TS。

### 3.3 数据流

```
Claude Code ──POST /v1/messages──▶ proxy/server
  │ (Anthropic SSE)                 │ 认证校验 → 401
  ▼                                 │ JSON 解析 → 400
  convert：Anthropic 消息 → OpenAI 请求体（+server tool schema/系统提示后缀）
  ▼                                 │
  buildUpstreamRequest：stream:true、include_usage、模型 extra 合并、canonical body
  ▼                                 │
  上游 POST /chat/completions（ctx 取消传播）
  ▼                                 │
  iterUpstreamChunks：SSE 行解析（[DONE] 哨兵、\r\n 兼容、错误对象检测）
  ▼                                 │
  stream：chunk → SSE 事件（think_tag/启发式/原生 tool_calls/usage/错误分支）
  ▼                                 │
  单 goroutine select 泵：事件 + 15s ping → 下游 ResponseWriter
  ▼                                 │
  dump 会话：4 日志 → 分类桶改名
```

server tools 请求（含 `web_search_*`/`web_fetch_*` 类型）走 agentic loop：收集 tool_calls（原生 + 文本嵌入 4 模式）→ 拦截 → 代理执行 → 注入消息历史 → 重请求（≤5 轮）→ 最终文本流输出，工具结果以文本块呈现（`[Web Search Results]`/`[Web Fetch Result]`），不用 `server_tool_use`（避免 Claude Code 触 claude.ai 域安全验证）。内层流 `skipMessageLifecycle` + 起始块索引偏移，外层统一 message_delta/message_stop，usage 从 tee 的最终流回填。

### 3.4 错误处理

- 请求生命周期：认证失败 401、JSON 非法 400、缺 model/messages 400、上游连接失败 502/499、上游非 2xx → `≥500 ? 502 : 原状态`、空体 500。
- 流中：`UpstreamStreamError` → 非重试 `stream_error`；`UpstreamAbortedError` → 重试性（`overloaded_error` 子串）。已有输出 → incomplete notice + 正常收尾；无输出 → 顶层 `event: error`。
- 所有错误保持 Anthropic 错误格式；404 路由同格式。

### 3.5 转换层行为要点（全部对等）

- 系统提示：剥离首行 `x-anthropic-billing-header:`（含 `\r\n`/`\r`/`\n` 终止符处理）。
- thinking 回放三模式（disabled/think_tags/reasoning_content）；`redacted_thinking` → `[redacted thinking]`；签名 `<!--sig:...-->` 前缀往返。
- `tool_use` → `tool_calls`（参数规范 JSON）；`tool_result` → `role:tool`（`[TOOL_ERROR] ` 前缀、媒体提取 → 合成 user 轮 + 标记文本）。
- deferred post-tool blocks：首个 tool_use 后的非工具块延迟到随后的 user/tool 消息消费或收尾排空（PendingAfterTools 状态机）。
- 图片/文档：base64/url 图片 → `image_url`；非图片文档 → `[Document: <name> (<data-url|media-type>)]` 文本 + context。
- tool_choice 映射（tool→function、any→required、auto/none/required 直通、function 直通）；`disable_parallel_tool_use` → `parallel_tool_calls: false`。
- server tools 从 `server_tools` 字段和 `tools` 数组双源收集去重；OpenAI function schema 注入 + 系统提示后缀。
- canonical：`_` 前缀剥离（JSON Schema name-map `properties`/`patternProperties`/`definitions`/`$defs` 内保留）+ 键排序。

### 3.6 流层行为要点（全部对等）

- chunk 错误对象检测、`[DONE]` 哨兵（无 finish_reason 时视为隐式 stop）、无 finish_reason 且无 [DONE] → `UpstreamAbortedError(response_stalled)`。
- reasoning_content → thinking 块；refusal → 文本；content → think_tag 解析 → 启发式解析。
- 原生 tool_calls：preStartArgs 缓冲、名称/ID 缺失孤儿推断（按 tools 序号）、Task.run_in_background 强制 false、截断 JSON 修复（引号/括号配平）、flush 时无效 JSON 哈希告警。
- usage 双通道合并（`cache_read_input_tokens`/`cache_creation_input_tokens` 直读 + `prompt_tokens_details.cached_tokens/cache_write_tokens` 回退）；`message_start`/`message_delta` 三桶（input = prompt − cache_read − cache_write，饱和 0）。
- 输出 token 估计 char/4 + 块开销 +15/块 +4；thinking_tokens 子计数。
- 停止序列检测（`stop_sequences` 尾匹配）。
- 孤儿工具状态 → finish_reason 强转 `stop`。

### 3.7 服务器工具要点（全部对等）

- Brave：`/res/v1/web/search?q=...&count=10`，`X-Subscription-Token`；SearXNG：`/search?q=...&format=json`，可选 Bearer。
- web_fetch：域名 allow/block（`*.example.com` 通配）、UA、重定向跟随、HTML→纯文本、max content tokens 截断（`\n\n[Content truncated]`）、`Status: 4xx` 标记错误。
- 文本工具检测 4 模式：Claude `<tool_use>{json}</tool_use>`、`WebSearch/WebFetch {json}`、GLM `<tool_call><tool_name>...<parameter name="...">`（可无 tool_call 包裹）、剥离 `<tool_use>`/`<tool_call>` 及后续幻觉文本。

## 4. 测试策略

测试向量从 TS 版 13 个测试文件移植（TS 测试是行为规范来源）：

| Go 包 | 移植源 | 重点 |
|---|---|---|
| config | config.test.ts (363) | 参数解析、extra-params、glob、deepMerge 元操作 |
| convert | converter.test.ts (581) + canonical.test.ts (105) | 转换全矩阵、canonical |
| parsers | think_tag_parser.test.ts (89) + heuristic_tool_parser.test.ts (107) | 分片边界、状态机 |
| sse | sse_builder.test.ts (390) | 事件 golden、签名、usage |
| stream | stream.test.ts (769) + sse_stream.test.ts (1239) | mock chunk → golden SSE |
| servertool | server_tools.test.ts (365) | 双引擎 mock、域名、HTML、文本检测 |
| dump | 新写 | 终止优先级、分类桶、日志格式 |

集成测试（proxy 包，httptest）：完整链路 SSE 序列、认证/透传、agentic loop（mock 上游先 tool_calls 后文本）、断连取消。

质量门：`go test ./... -race`、`go vet ./...`、`go build`。实现顺序按依赖自底向上：config → anthropic/openai 类型 → sse → parsers → convert → stream → servertool → dump → proxy → main，每包先移植测试（红）→ 实现（绿）。

## 5. 交付物

- 完整 Go 实现（纯标准库），`go build` 产出单二进制
- 全部测试通过（`go test ./... -race`）
- README.md（英文）+ README-zh.md（中文）更新为新项目；Dockerfile（多阶段构建，distroless/scratch 镜像）
- 启动横幅与 TS 版格式一致（名称改为 chat-to-messages）
- MIT License

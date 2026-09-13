# chat-to-messages

将任意 OpenAI Chat Completions 兼容端点转为 Anthropic Messages API，让 Claude Code CLI 直接使用。

[English](README.md)

## 设计哲学

本项目遵循 Unix 哲学：**一个进程只对接一个上游端点**。若需同时使用多个上游（如不同模型或 provider），请启动多个进程，各自监听不同端口，由客户端或负载均衡器做路由选择。这样做的好处是：

- 每个进程简单、可预测、易调试
- 无内置路由状态，进程无状态，随起随停
- 可独立扩缩容、滚动升级，互不影响

## 项目动机

受 [free-claude-code](https://github.com/Alishahryar1/free-claude-code) 启发，本项目旨在提供一个**更轻量的部署方案**：

- **更小占用** — 纯 Go 标准库，零外部依赖，编译为单个静态二进制，磁盘和内存占用远低于 Python + FastAPI 方案
- **简化路由** — 完全去掉多上游转发，仅保留单上游的 OpenAI→Anthropic 协议中转
- **透传友好** — 支持 auth token 透传，无需硬编码上游密钥，适合部署在极轻量服务器（如 1C1G）上
- **多进程扩展** — 需要多个上游时，只需每个上游启动一个进程，监听不同端口，由反向代理或 DNS 做路由分发

### 多上游部署示例

```bash
# 进程 1：NVIDIA NIM，监听 8082 端口
./chat-to-messages \
  --upstream-base-url https://integrate.api.nvidia.com/v1 \
  --upstream-api-key nvapi-xxxx \
  --port 8082

# 进程 2：OpenRouter，监听 8083 端口
./chat-to-messages \
  --upstream-base-url https://openrouter.ai/api/v1 \
  --upstream-api-key sk-or-xxxx \
  --port 8083
```

然后将 Claude Code 指向所需的上游：

```bash
# 使用 NVIDIA NIM
ANTHROPIC_BASE_URL=http://localhost:8082 claude

# 使用 OpenRouter
ANTHROPIC_BASE_URL=http://localhost:8083 claude
```

或者将两个进程置于反向代理（nginx、Caddy 等）之后，按域名或路径做路由分发。

## 工作原理

```
Claude Code CLI
    │  Anthropic API (SSE)
    ▼
chat-to-messages ────► OpenAI /chat/completions (SSE)
                            │  (NVIDIA NIM / OpenAI / Ollama / LM Studio / ...)
    │  Anthropic SSE
    ▼
Claude Code CLI 收到标准 Anthropic 响应
```

核心能力：

- **协议转换** — Anthropic Messages API ↔ OpenAI Chat Completions 双向转换
- **流式 SSE** — OpenAI 流式 chunk 实时转为 Anthropic SSE 事件
- **Thinking 支持** — `reasoning_content` 和 thinking tag 两种推理格式均转 Anthropic thinking block
- **工具调用** — 原生 `tool_calls` 和文本形式的 `● <function=...>` 启发式解析均支持
- **下游鉴权** — 可选 `--auth-token` 对接入方进行 x-api-key 验证
- **请求转储** — 可选 `--dump <dir>` 记录完整请求/响应，便于调试
- **零依赖** — 纯 Go 标准库，无外部包

## 快速开始

### 1. 构建二进制

需要 [Go](https://go.dev/dl/) 1.26+：

```bash
go build -o chat-to-messages .
```

生成单个静态二进制（Linux/macOS 下为 `chat-to-messages`，Windows 下为 `chat-to-messages.exe`）。

### 2. 启动代理

所有配置通过 CLI 参数传入：

```bash
./chat-to-messages \
  --upstream-base-url https://integrate.api.nvidia.com/v1 \
  --upstream-api-key nvapi-xxxx
```

输出：

```
chat-to-messages listening on http://localhost:8082
  Upstream: https://integrate.api.nvidia.com/v1
  Upstream API key: configured
  Auth token: not set
  Passthrough mode: false
  Thinking: true
  Dump: disabled
  Web Search: false
  Web Fetch: false
```

### 3. 连接 Claude Code

```bash
export ANTHROPIC_BASE_URL="http://localhost:8082"
export ANTHROPIC_AUTH_TOKEN="your-token-here"
export ANTHROPIC_DEFAULT_OPUS_MODEL="deepseek-ai/deepseek-v4-pro"
export ANTHROPIC_DEFAULT_SONNET_MODEL="qwen/qwen3.5-397b-a17b"
export ANTHROPIC_DEFAULT_HAIKU_MODEL="minimaxai/minimax-m2.7"
claude
```

或在单行中启动：

```bash
ANTHROPIC_BASE_URL=http://localhost:8082 ANTHROPIC_AUTH_TOKEN=freecc claude
```

## CLI 参数参考

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--upstream-base-url` | `https://api.openai.com/v1` | 上游 OpenAI Chat Completions 兼容端点 |
| `--upstream-api-key` | `""` | 上游 API Key；用于请求上游端点时的鉴权 |
| `--auth-token` | `""` | 下游鉴权 Token；设置后客户端需在 x-api-key 头部提供匹配的值 |
| `--port` | `8082` | HTTP 监听端口 |
| `--enable-thinking` | `true` | 将上游推理内容转为 Anthropic thinking block |
| `--no-enable-thinking` | — | 禁用 thinking 转换 |
| `--sanitize-client-meta-turns` | `true` | 将 Claude Code 注入的合成用户轮（`[Your previous response had no visible output...]`、`(no content)`、中断标记）规约为中性续作指令后再重放给上游模型 |
| `--empty-turn-guard` | `true` | 空回合守卫：回合以推理结束但既无可见文本也无工具调用时，自动向上游发起有界重试（针对 kimi-k3/GLM 塌方模式） |
| `--empty-turn-retries` | `2` | 每个请求允许的空回合重试次数上限（0-5） |
| `--mid-stream-stall-timeout` | `60` | 上游静默超过该秒数后主动断开并重试一次、在同一 SSE message 内续流（针对 newapi/GLM 中转掐断长计划生成，2026-09-13 dump）。`0` 禁用 |
| `--max-upstream-images` | `7` | 每个请求发送到上游的图片数量上限；超出的较早图片（从最早开始）替换为文本占位符（z-ai 渠道 ≥8 张必挂）。`0` 禁用裁剪 |
| `--upstream-extra-params` | — | 按模型注入上游请求额外参数（可重复指定）；见下方说明 |
| `--reasoning-replay` | `think_tags` | 助手 thinking 块回放给上游的方式：`think_tags`、`reasoning_content` 或 `disabled`。接受裸模式（全局默认）或 `glob=mode` 按模型规则（可重复指定，首个匹配生效）；见下方说明 |
| `--dump` | `""` | 请求转储目录；启用后每个请求写入独立子目录 |
| `--enable-web-search` | `false` | 启用代理端 Web 搜索 |
| `--web-search-engine` | `brave` | 搜索引擎类型：`brave`（Brave Search API）或 `searxng`（SearXNG） |
| `--enable-web-fetch` | `false` | 启用代理端 Web 抓取（HTTP GET + 域名过滤） |
| `--web-search-api-key` | `""` | 搜索 API Key（Brave 必填，SearXNG 可选） |
| `--web-search-base-url` | `https://api.search.brave.com` | 搜索 API 基础 URL |
| `--web-fetch-allowed-domain` | — | Web 抓取允许的域名（可重复指定，如 `--web-fetch-allowed-domain example.com`） |
| `--web-fetch-blocked-domain` | — | Web 抓取屏蔽的域名（可重复指定） |
| `--web-fetch-max-content-tokens` | `5000` | Web 抓取结果的最大内容 token 数 |

### Server Tools（Web 搜索 & Web 抓取）

本代理可代替不支持原生 server tools 的上游模型执行 Anthropic 风格的 `web_search` 和 `web_fetch`。当模型在文本输出中发出匹配 `WebSearch` / `WebFetch` 的工具调用时，代理会拦截、执行，并将结果以 Anthropic `server_tool_use` / `web_search_tool_result` / `web_fetch_tool_result` 内容块返回。

#### Brave Search（默认）

```bash
./chat-to-messages \
  --upstream-base-url https://api.openai.com/v1 \
  --upstream-api-key sk-xxx \
  --enable-web-search \
  --web-search-api-key BST-xxxx
```

#### SearXNG（自建实例）

```bash
./chat-to-messages \
  --upstream-base-url https://api.openai.com/v1 \
  --upstream-api-key sk-xxx \
  --enable-web-search \
  --web-search-engine searxng \
  --web-search-base-url https://searxng.example.com
```

SearXNG 无需 `--web-search-api-key`，除非你的实例要求认证。

#### 同时启用搜索和抓取

```bash
./chat-to-messages \
  --upstream-base-url https://api.openai.com/v1 \
  --upstream-api-key sk-xxx \
  --enable-web-search \
  --web-search-engine searxng \
  --web-search-base-url https://searxng.example.com \
  --enable-web-fetch \
  --web-fetch-allowed-domain docs.example.com \
  --web-fetch-allowed-domain api.example.com
```

**工作流程：**

1. 客户端发送带有 `server_tools: [{type: "web_search_20250305"}, {type: "web_fetch_20250305"}]` 的请求
2. 代理检测到 server tools 类型，转发到上游前将其移除
3. 如果上游模型以启发式文本工具调用的形式调用 `WebSearch` / `WebFetch`，代理会拦截
4. 代理执行调用（Brave Search API 或 SearXNG 执行搜索，直接 HTTP 执行抓取），并将结果以 Anthropic SSE 事件返回给客户端

### 按模型注入额外参数

使用 `--upstream-extra-params` 根据请求中的模型名向上游请求体注入额外的 JSON 字段。格式为 `glob=JSON`：

```bash
--upstream-extra-params 'claude-*={"thinking":{"type":"enabled","budget_tokens":10000}}'
```

- **glob 模式** — `*` 匹配任意字符，`?` 匹配单个字符。第一个匹配的模式生效。
- **JSON 值** — 必须是 JSON 对象。会深层合并（deep merge）到上游请求体中：嵌套对象逐层合并，数组直接替换。
- **可重复指定** — 多次使用以配置不同模型。
- **处理顺序** — 常规合并 → `$default`（补默认值）→ `$delete`（删除属性）。

#### 元操作键

两个以 `$` 为前缀的保留键用于高级控制（其余键为标准 deep merge 行为：嵌套对象合并，数组替换，`null` 直接覆盖）：

| 键 | 类型 | 语义 |
|---|------|------|
| `$delete` | `string[]` | 从合并结果中删除指定**点号路径**的属性。路径不存在时静默忽略。 |
| `$default` | `Record<string, any>` | 仅在路径**缺失**（`undefined`）时才写入值。已有值不会被覆盖。支持点号路径设置嵌套默认值。 |

```bash
./chat-to-messages \
  --upstream-base-url https://api.openai.com/v1 \
  --upstream-api-key sk-xxx \
  --upstream-extra-params 'claude-sonnet-*={"thinking":{"type":"enabled","budget_tokens":10000}}' \
  --upstream-extra-params 'deepseek*={"reasoning_effort":"high"}' \
  --upstream-extra-params '*={"stream":true}'
```

当请求携带 `model: "claude-sonnet-4-20250514"` 到达时，匹配到 `claude-sonnet-*` 模式，其 JSON 会被合并到上游请求体中。`*` 通配符可作为所有未匹配模型的默认规则。

##### 示例

**删除顶层或嵌套参数：**

```bash
# 删除顶层 key
--upstream-extra-params 'deepseek*={"$delete":["top_p","presence_penalty"],"reasoning_effort":"high"}'

# 通过点号路径删除嵌套 key
--upstream-extra-params 'claude-*={"$delete":["thinking.budget_tokens"]}'

# 通过嵌套 $delete 删除（路径相对于当前层级）
--upstream-extra-params 'claude-*={"thinking":{"$delete":["budget_tokens"],"type":"enabled"}}'
```

**为缺失的参数提供默认值（不会覆盖已有值）：**

```bash
# 仅在上游请求体没有 max_tokens 时才设置
--upstream-extra-params '*={"$default":{"max_tokens":4096,"temperature":0.7}}'

# 通过点号路径设置嵌套默认值
--upstream-extra-params 'claude-*={"$default":{"thinking.budget_tokens":10000}}'

# 通过嵌套 $default 设置
--upstream-extra-params 'claude-*={"thinking":{"$default":{"budget_tokens":10000}}}'
```

**将参数设为 `null`（使用标准 JSON `null`）：**

```bash
--upstream-extra-params 'claude-*={"top_p":null}'
```

**混合使用 — 删除、默认值、覆盖三合一：**

```bash
--upstream-extra-params '*={"$delete":["user","seed"],"$default":{"max_tokens":4096},"temperature":0.2}'
```

### 推理回放（按模型）

控制代理在后续请求中如何把助手 thinking 块回放给上游模型。各家上游的 chat template 对"思考在历史中的位置"约定不一致，因此该模式按模型配置：

| 模式 | 行为 | 适用 |
|------|------|------|
| `think_tags`（默认） | thinking 以字面 `<think>...</think>` 文本嵌入助手消息 content | GLM 系及多数 OpenAI 兼容上游 |
| `reasoning_content` | thinking 以 OpenAI 风格的 `reasoning_content` 字段随助手消息发送 | Kimi K3（及 K2-thinking）thinking 模式 |
| `disabled` | 完全不回放 thinking 块 | 拒绝或忽略推理历史的上游 |

`--reasoning-replay` 接受裸模式（设置全局默认）或 `glob=mode`（添加按模型规则，首个匹配生效）：

```bash
./chat-to-messages \
  --upstream-base-url https://api.example.com/v1 \
  --upstream-api-key sk-xxx \
  --reasoning-replay 'moonshotai/kimi-k*=reasoning_content' \
  --reasoning-replay 'deepseek*=disabled'
```

**为什么按模型区分：** Kimi 官方要求 thinking 模式下 assistant 的 tool call 消息必须携带 `reasoning_content`；缺失该字段会让模型看到分布外的历史，表现为把推理当正文输出、宣布计划后直接结束回合而不发出任何工具调用。DeepSeek 恰好相反——输入 messages 中出现 `reasoning_content` 会直接返回 400 错误。GLM 系模型在训练中就使用上下文内的 `<think>` 标记，默认 `think_tags` 模式即其原生表达。除非模型文档另有约定，请保持默认值。

### 透传模式

当 `--upstream-api-key` 与 `--auth-token` 均未配置时，自动启用透传模式：客户端通过 `x-api-key` 或 `Authorization` 头部传入的 Key 将原样转发给上游端点。

### 下游鉴权

设置 `--auth-token` 后，客户端请求必须携带匹配的 `x-api-key` 或 `Authorization: Bearer xxx` 头部，否则返回 401。此功能用于保护代理不被未授权的客户端调用。

### 请求转储

启用 `--dump <dir>` 后，每个下游请求都会被记录。会话先写入 `<dir>/in-progress/<id>/`（`<id>` 为时间序 UUID v7），结束时按**根因**重命名到分类桶：

| 桶 | 含义 |
|----|------|
| `completed` | 正常完成（上游正常送达 finish） |
| `client-aborted` | 客户端中途断开 |
| `upstream-aborted` | 上游连接中断 / 未正常 finish 即中止 |
| `failed` | 上游超时、上游错误状态或空响应、未知终止 |

最终目录名为 `<id>__START_<开始时间>__END_<结束时间>`。`<id>` 为 UUID v7，其前 48 位编码了请求发起的 Unix 毫秒时间戳，因此按目录名字典序排序即可得到各桶内的请求时序（`__START_`/`__END_` 后缀仅便于人眼阅读）。客户端主动断开优先于一切；其次以上游自身的终止记录为准；最后才是下游侧记录的结果。

每个会话包含 `downstream-request.log`、`downstream-response.log`、`upstream-request.log` 和 `upstream-response.log` — 流式响应体即为完整 SSE 事件流。若调用了代理端 server tools（web_search / web_fetch / agentic loop），还会额外写入按调用逐条记录的 `server-tools.log`。

```bash
./chat-to-messages \
  --upstream-base-url https://integrate.api.nvidia.com/v1 \
  --upstream-api-key nvapi-xxxx \
  --dump /var/log/chat-to-messages
```

转储目录结构示例：

```
/var/log/chat-to-messages/
├── in-progress/
│   └── 3f1a2b4c-8d9e-7f5a-b6c7-8d9e0f1a2b3c/
├── completed/
│   └── 3f1a2b4c-8d9e-7f5a-b6c7-8d9e0f1a2b3c__START_2026-05-20T08-30-00-000Z__END_2026-05-20T08-30-05-123Z/
│       ├── downstream-request.log
│       ├── downstream-response.log
│       ├── upstream-request.log
│       ├── upstream-response.log
│       └── server-tools.log
├── client-aborted/
├── upstream-aborted/
└── failed/
```

## 交叉编译

纯 Go + `CGO_ENABLED=0`，可在任意平台交叉编译出完全静态、自包含的单文件二进制：

```bash
# 当前平台
go build -o chat-to-messages .

# Linux (amd64) — 常见服务器目标
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o chat-to-messages-linux-amd64 .

# Windows (amd64)
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o chat-to-messages.exe .

# macOS (arm64)
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o chat-to-messages-darwin-arm64 .
```

每个二进制独立运行 — 无需运行时、解释器或共享库。

## Docker 容器化部署

### 构建镜像

```bash
docker build -t chat-to-messages .
```

Dockerfile 采用多阶段构建：`golang:1.26-alpine` 构建阶段产出静态 `CGO_ENABLED=0` 二进制，再复制到带 CA 证书的精简 `alpine:3.22` 运行时镜像中。

### 运行容器

```bash
docker run -p 8082:8082 chat-to-messages \
  --upstream-base-url https://integrate.api.nvidia.com/v1 \
  --upstream-api-key nvapi-xxxx
```

带下游鉴权：

```bash
docker run -p 8082:8082 chat-to-messages \
  --upstream-base-url https://integrate.api.nvidia.com/v1 \
  --upstream-api-key nvapi-xxxx \
  --auth-token my-secret-token
```

透传模式（无需任何 Key）：

```bash
docker run -p 8082:8082 chat-to-messages \
  --upstream-base-url http://localhost:11434/v1
```

## API 端点

| 路径 | 方法 | 说明 |
|------|------|------|
| `/v1/messages` | POST | Anthropic Messages API 代理（核心端点） |
| `/v1/chat/completions` | POST | OpenAI Chat Completions 透传（原样转发，不转换） |
| `/v1/models` | GET | OpenAI Models 透传（原样转发） |
| `/health` | GET | 健康检查 |

未匹配路径返回 `404` + Anthropic 格式错误体。

**注意**：`model` 字段为必填，请求体中必须包含 `model` 字段，否则返回错误。

## 转换细节

### 消息转换

| Anthropic | OpenAI | 说明 |
|-----------|--------|------|
| `system`（字符串 / content blocks） | `{"role": "system", "content": "..."}` | 系统提示提取为 system 消息 |
| `tool_use` block | `tool_calls[i]` | 工具调用参数 JSON 序列化 |
| `tool_result` block | `{"role": "tool", "tool_call_id": "..."}` | 工具结果序列化为 tool 消息 |
| `thinking` block | thinking tag 嵌入 content 中 | 由 `ReasoningReplayMode` 控制 |
| `redacted_thinking` block | 重放为 `[redacted thinking]` 占位符 | 保留多轮推理链 |
| `thinking` block（带签名） | 以 `<!--sig:...-->` 前缀保留在 thinking tag 中 | 多轮验证的往返签名保留 |
| `document` block | 序列化为 `[Document: filename (media_type)]` 文本 | PDF/文档感知（二进制不转发） |
| `image` block（base64/url） | `image_url` content part | Anthropic 图片块转为 OpenAI 格式 |

### 流式 SSE 事件映射

| OpenAI chunk | Anthropic SSE event |
|-------------|-------------------|
| `delta.reasoning_content` | `content_block_start(thinking)` + `content_block_delta(thinking_delta)` |
| `delta.content`（含 thinking tag） | 解析后分发为 thinking / text delta |
| `delta.content`（纯文本） | `content_block_delta(text_delta)` |
| `delta.refusal` | `content_block_delta(text_delta)` — refusal 作为可见文本转发 |
| `delta.tool_calls` | `content_block_start(tool_use)` + `content_block_delta(input_json_delta)` |
| `delta.content`（WebSearch/WebFetch 文本） | `content_block_start(server_tool_use)` + `content_block_start(web_search_tool_result)` |
| `finish_reason: "stop"` | `message_delta(stop_reason: "end_turn")` |
| `finish_reason: "tool_calls"` | `message_delta(stop_reason: "tool_use")` |
| thinking block 关闭 | `content_block_delta(signature_delta)` + `content_block_stop` |
| `usage.prompt_tokens_details.cached_tokens` | `message_start.usage.cache_read_input_tokens` |
| `usage.prompt_tokens_details.cache_write_tokens` | `message_start.usage.cache_creation_input_tokens` |
| `event: ping`（心跳） | 长流期间每 15 秒注入 |
| 流内错误对象 | `event: error`，错误类型映射（`overloaded_error` / `invalid_request_error` / `api_error`） |
| `tool_choice.disable_parallel_tool_use: true` | 请求体根部写入 `parallel_tool_calls: false` |
| 被截断的 tool_use 输入 JSON | `content_block_stop` 前通过花括号配平自动修复 |

### Stop Reason 映射

| OpenAI | Anthropic |
|--------|-----------|
| `stop` | `end_turn` |
| `length` | `max_tokens` |
| `tool_calls` | `tool_use` |
| `content_filter` | `refusal` |
| 其他 | `end_turn` |

### 启发式工具调用解析

部分模型不返回原生 `tool_calls`，而是将工具调用以文本形式输出：

```
● <function=read_file><parameter=path>/etc/hosts</parameter>
```

解析器会将其转为结构化 `tool_use` block。同时支持 WebFetch / WebSearch 的 JSON 文本格式：

```
Use WebFetch {"url": "https://example.com"}
Use WebSearch {"query": "test query"}
```

## 常用 Provider 配置示例

### NVIDIA NIM

```bash
./chat-to-messages \
  --upstream-base-url https://integrate.api.nvidia.com/v1 \
  --upstream-api-key nvapi-your-key
```

### OpenAI

```bash
./chat-to-messages \
  --upstream-base-url https://api.openai.com/v1 \
  --upstream-api-key sk-your-key
```

### Ollama（本地）

```bash
./chat-to-messages \
  --upstream-base-url http://localhost:11434/v1
```

### LM Studio（本地）

```bash
./chat-to-messages \
  --upstream-base-url http://localhost:1234/v1
```

### OpenRouter

```bash
./chat-to-messages \
  --upstream-base-url https://openrouter.ai/api/v1 \
  --upstream-api-key sk-or-your-key
```

## 开发

### 项目结构

```
main.go                        # 入口：启动横幅、HTTP 服务、优雅退出
go.mod                         # module chat-to-messages（Go 1.26+）
internal/
├── anthropic/                 # Anthropic Messages API 类型定义
├── config/                    # CLI 参数解析、glob 匹配、extra-params 深层合并
├── convert/                   # Anthropic → OpenAI 消息/工具/系统提示转换
├── dump/                      # 请求/响应转储日志
├── openai/                    # OpenAI Chat Completions 类型 + SSE 流解析
├── parsers/                   # Think tag 流式解析器 + 启发式工具调用解析
├── proxy/                     # HTTP 路由、鉴权、CORS、SSE 泵、agentic loop
├── servertool/                # 代理端 web_search / web_fetch 执行
├── sse/                       # Anthropic SSE 事件构建器 + 错误响应
└── stream/                    # OpenAI 流 → Anthropic SSE 流式转换
```

### 运行测试

```bash
go test ./...
```

### 静态检查

```bash
go vet ./...
```

### 开发模式（源码直接运行）

```bash
go run . --upstream-base-url https://integrate.api.nvidia.com/v1 --upstream-api-key nvapi-xxxx
```

### 打包

```bash
go build -o chat-to-messages .
```

## 与 free-claude-code（Python 版）的差异

本项目是 [chat-to-claude-code](https://github.com/chinfeng/chat-to-claude-code)（[free-claude-code](https://github.com/chinfeng/free-claude-code) 的 TypeScript/Bun 移植）的 Go 移植，聚焦核心协议转换功能：

| 特性 | Python 版 | 本项目 (Go) |
|------|-----------|-------------|
| 运行时 | Python 3.14 + FastAPI | Go（单个静态二进制） |
| 外部依赖 | FastAPI, Pydantic, httpx, tiktoken 等 | 零（仅标准库） |
| Provider 数量 | 11（NIM, OpenRouter, DeepSeek, Kimi, Wafer, LM Studio, llama.cpp, Ollama, OpenCode, Z.ai, OpenAI） | 1（通用 OpenAI 兼容端点） |
| Model Router | Opus/Sonnet/Haiku 多 provider 路由 | 无（单一 upstream） |
| 配置方式 | 环境变量 | CLI 启动参数 |
| 下游鉴权 | 无 | AUTH_TOKEN 验证 |
| 可执行文件 | 无 | `go build` 单二进制 |
| 容器化 | 无 | Dockerfile（多阶段，alpine 运行时） |
| 请求转储 | 无 | --dump 顺序目录 |
| Admin UI | 本地 Web 配置界面 | 无 |
| 请求优化 | quota mock / title skip / prefix detection / filepath mock | 无 |
| Discord/Telegram Bot | 完整 bot 集成 | 无 |
| Web Server Tools | 代理端 web_search / web_fetch | 代理端 web_search（Brave / SearXNG）/ web_fetch |
| Rate Limiting | 令牌桶限速 | 无 |
| Token 计数 | tiktoken (cl100k_base) | char/4 估算 |
| 日志/追踪 | loguru + 结构化 trace | console + 可选 dump |

## License

MIT

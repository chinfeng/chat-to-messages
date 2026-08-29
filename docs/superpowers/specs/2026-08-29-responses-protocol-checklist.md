# Responses 协议实现清单(2026-08-29)

供人工核查协议完整度。标注:✅ 已实现并测试 / ⚠️ 已实现但有意的简化 / ❌ 未实现。传输 = HTTP(body/SSE)与 WS(text 帧,事件 JSON 与 SSE `data:` 载荷完全一致)。

## 1. 接入点

| 项 | 状态 | 说明 |
|---|---|---|
| `POST /v1/responses` | ✅ | `stream:true` → SSE;`stream:false` → 聚合 JSON(上游恒流式) |
| `GET /v1/responses`(WS upgrade) | ✅ | RFC 6455,`internal/ws` 手写;拒绝 permessage-deflate/子协议(RFC 省略式) |
| `GET /v1/responses`(非 upgrade) | ✅ 400 | 明确报错,不误路由 |
| 鉴权(x-api-key / Bearer) | ✅ | 与 /v1/messages 同一条 validateAuthToken/resolveAPIKey |

## 2. 请求字段 → 上游 chat/completions

| Responses 字段 | 状态 | 映射 |
|---|---|---|
| `model` | ✅ | `model`;缺失 → 400(param=model) |
| `input`: 字符串 | ✅ | 末尾追加 user 消息(历史之后) |
| `input`: items 数组 | ✅ | 见 §3 |
| `instructions` | ✅ | 首条 system 消息 |
| `tools[]`(type=function) | ✅ | chat tools;parameters 缺省补 `{}` |
| `tools[]`(hosted: web_search 等) | ✅ 400 | chat/completions 无对应物,fail fast |
| `tool_choice`: none/auto/required/function | ✅ | 同名/转 `{type:"function",...}` |
| `parallel_tool_calls` | ✅ | 透传 |
| `temperature` / `top_p` | ✅ | 透传(UseNumber 保精度) |
| `max_output_tokens` | ✅ | `max_completion_tokens` |
| `text.format`: text/json_object/json_schema | ✅ | `response_format` |
| `text.verbosity` | ✅ | 顶层 `verbosity` 透传 |
| `reasoning.effort` | ✅ | `reasoning_effort` |
| `reasoning.max_tokens` | ❌ | chat 无对应物,丢弃 |
| `stream` | ✅ | SSE 开关;WS 恒视为 true |
| `store` | ✅ | nil/true→写链;false→不写(HTTP);**WS 恒写**(连接级缓存语义,codex 依赖) |
| `previous_response_id` | ✅ | 代理内存 store;miss → 404(WS:error frame,`code=previous_response_not_found`) |
| `include: ["reasoning.encrypted_content"]` | ✅ | 代理侧 base64 收发(本代理即加密端) |
| `truncation` | ⚠️ | 仅回显;无本地截断(上游自行处理超长) |
| `background` | ✅ 400 | 无 chat 对应物 |
| `metadata` / `user` | ✅ | 透传上游 `metadata`/`user` |
| `safety_identifier` | ✅ | `user` 缺省时回填 |
| `service_tier` / `max_tool_calls` / `prompt_cache_key` | ❌ | 不进请求体(chat 无对应),响应对象中回显为 default(对齐 OpenAI 下游响应形状) |

## 3. input item 类型

| item 类型 | 状态 | 说明 |
|---|---|---|
| `message`(role=user/system/developer) | ✅ | developer→system;content 支持 string/parts |
| `message`(role=assistant) | ✅ | 与后续 function_call 合并为一条 assistant(content+tool_calls) |
| content part: input_text/output_text/text | ✅ | text part(纯文本时合并为 string) |
| input_image(image_url) | ✅ | image_url part;file_id → 400(无文件存储) |
| input_file(file_data) | ✅ | file part;file_id → 400 |
| input_audio | ✅ 400 | chat 无对应物 |
| `function_call` | ✅ | → assistant tool_calls;`call_id` 恒代理签发(不透传上游 id,0a4dae8 教训) |
| `function_call_output` | ✅ | → role:tool,按 call_id 归位 |
| `reasoning`(summary/encrypted_content) | ✅ | 按模型 replay:think_tags / reasoning_content / disabled(与 /v1/messages 同规则表) |
| `{id}` item_reference | ✅ 400 | 明确报错要求重发全量(不静默重启会话) |
| 未知 item 类型 | ⚠️ | 跳过(tolerant;对齐 openapi 异物容忍惯例) |

## 4. 下行事件(两传输相同 JSON)

| 事件 | 状态 |
|---|---|
| `response.created` / `response.in_progress` | ✅ 首个 chunk 前发出 |
| `response.output_item.added` / `.done`(reasoning/message/function_call) | ✅ |
| `response.content_part.added` / `.done`(output_text) | ✅ |
| `response.output_text.delta` / `.done` | ✅(启发式解析器缓冲,纯文本回合为单 delta——与 /v1/messages 同 parity) |
| `response.reasoning_summary_part.added` / `.done` | ✅ |
| `response.reasoning_summary_text.delta` / `.done` | ✅ |
| `response.function_call_arguments.delta` / `.done` | ✅(原生按 index 流式;GLM XML 启发式一次完整) |
| `response.completed`(usage 入终态对象) | ✅ |
| `response.incomplete`(reason=max_output_tokens/content_filter) | ✅ |
| `response.failed`(中途上游错误,error.code=server_error) | ✅ |
| `response.refusal.*` | ⚠️ refusal 按纯文本流(与 messages 路径一致) |
| `sequence_number` 字段 | ❌ 不携带(增强时补) |
| `response.output_item.*` 的 annotation/引用事件 | ❌ 上游无数据,annotations 恒 [] |
| OpenAI 专有事件(codex.*, rate_limits) | N/A | 上游 chat/completions 无此信息 |

## 5. usage 映射

| 字段 | 状态 |
|---|---|
| `input_tokens` ← prompt_tokens | ✅ |
| `output_tokens` ← completion_tokens | ✅ |
| `input_tokens_details.cached_tokens` ← cache_read_input_tokens ‖ prompt_tokens_details.cached_tokens | ✅ |
| `output_tokens_details.reasoning_tokens` ← completion_tokens_details.reasoning_tokens | ✅ |
| `total_tokens` | ✅ |
| 上游无 usage | ⚠️ 兜底:输出字符数/4 估算(input=0) |

## 6. 错误与状态码

HTTP:`400 invalid_request_error`(JSON 坏/model 缺/input 空/background/hosted tools/audio/file_id)· `404 not_found_error`(prev id)· `401 authentication_error` · 上游非 2xx → 5xx 映射 502、其余透传,body 截 500 字符 · 连接失败 502 · 建连前客户端断开 499 · 中途失败:流式发 `response.failed`,非流式 HTTP 500。

WS:error frame `{"type":"error","status":N,"error":{"type","code"?,"param"?,"message"}}`;code=`previous_response_not_found`(404,codex 识别并重发全量)、`response_in_progress`(400);二进制帧/坏 JSON/未知 type → 400;`response.cancel` → 协同取消,终态事件照发(codex 不用,直接断连);连接侧无 60 分钟上限(OpenAI 有,此处不需要)。

## 7. 存储(previous_response_id)

进程内 map,滑动 TTL(`--responses-store-ttl-minutes`,默认 1440);Get 命中续期;entry = 展开后的全量 input raw items + 本代理产出 output items(raw 保留未知字段)。HTTP 尊重 `store:false`;**WS 忽略 store:false**(OpenAI 连接级缓存语义)。重启全丢 → 客户端 404 后重发全量(codex 内建该路径)。`store:false` + HTTP + prev_id 组合 = 垂直到 404 是协议正确行为。

## 8. 与官方 Responses API 的已知差距(有意)

检索端点(GET/DELETE/list response、input_items)❌;background ❌;hosted tools 执行 ❌;conversation 对象 ❌;`prompt_cache_key`/`service_tier`/`max_tool_calls` 不进上游 ❌(chat 无对应);本地截断 ⚠️ 仅回显;`reasoning.max_tokens` ❌;`sequence_number` ❌。

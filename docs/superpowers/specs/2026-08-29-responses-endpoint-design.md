# /v1/responses 接入点设计

ADR-0001 已定方向；本文件是实施规格。`POST /v1/responses` 入站 → 转换为上游 `/v1/chat/completions`（恒 `stream: true`)→ 回译为 Responses 格式。新增包 `internal/responses`；路由挂 `internal/proxy/server.go`；上游调用、dump、鉴权与 messages 路径共享。

## 入站请求 → 上游 chat 请求

| Responses 字段 | 处理 |
|---|---|
| `model` | 直传；`-upstream-extra-params` / `--reasoning-replay` 按 model glob 照常生效 |
| `input` string | 单条 user 文本消息 |
| `input` items | 见下表 item 映射 |
| `instructions` | 置首条 system 消息 |
| `tools[]` function | → chat function tool(`description`/`parameters`/`strict` 直传） |
| `tools[]` hosted(web_search/file_search/computer_use/*/code_interpreter/image_generation/mcp/local_shell） | **400**,错误信息点名不支持的 type |
| `tool_choice` auto/none/required | 直传；`{type:"function",name}` → chat `{type:"function",function:{name}}`；其他对象原样直传 |
| `parallel_tool_calls` / `temperature` / `top_p` / `stop`(→`stop`) | 直传。Responses 用 `stop`？不——Responses 无 stop 字段；忽略 |
| `max_output_tokens` | → `max_completion_tokens`（不设默认；GLM/kimi 类上游不认识的话用 `--upstream-extra-params` 补） |
| `text.format` text | 省略；`json_object` → `response_format:{type:"json_object"}`;`json_schema` → `response_format:{type:"json_schema", json_schema:{name,description,schema,strict}}`；`text.verbosity` → `verbosity` 直传 |
| `reasoning.effort` | → `reasoning_effort` 直传；`reasoning.summary` 被本地消费（决定 reasoning item 是否带 summary 文本）,不下发 |
| `stream` | 下游语义。上游恒 stream；false 时服务端聚合后一次性返回 |
| `store` | 本地 response store 行为（默认存；false 不存）。不下发上游 |
| `previous_response_id` | store 展开（见下）；找不到 → 404 |
| `truncation`/`metadata`/`user`(`safety_identifier`) | `user`/`safety_identifier`→`user`;`metadata` 直传；`truncation` 接受但不做本地截断 |
| `background:true` | **400** |
| `include` | 仅识别 `reasoning.encrypted_content`（本地回放，无秘密要解）；其余忽略 |
| `service_tier`/`prompt_cache_key`/`max_tool_calls` | 忽略（不转发） |

### input item 映射

| item | 处理 |
|---|---|
| message(user/developer) | content parts:input_text→text;input_image(image_url)→image_url part(detail 直传）;input_file(file_data)→chat `file` part;input_audio→400;file_id→400（无法取回） |
| message(assistant) | output_text/refusal→assistant 文本；与相邻 function_call 组合进同一条 assistant 消息 |
| function_call | 累积进当前 assistant 消息的 `tool_calls`(id=本代理签发的 call_id，见 id 规则）|
| function_call_output | `{role:"tool", tool_call_id: call_id, content}`;output 为非字符串 parts 时 join 文本部分 |
| reasoning | 按 per-model replay:disabled→丢弃；think_tags→`<think>` 并入下一条 assistant 文本；reasoning_content→下一条 assistant 消息加 `reasoning_content`（来源为 summary 文本拼接；encrypted_content 不可解，仅原样留存在 store) |
| `{id}` item reference 无 previous_response_id | 400 |

## 出站（上游 chunk → Responses SSE)

事件序列（Codex 验收子集，+SDK 兼容）:

1. `response.created`(status=in_progress, 空 output,+ 请求回显字段）
2. `response.in_progress`
3. reasoning（仅当上游产出 thinking 时）:`output_item.added(type=reasoning, id=rs_…, summary=[])` → `reasoning_summary_part.added` → `reasoning_summary_text.delta`*→`…text.done` → `…part.done` → `output_item.done`（整段一个 summary part)
4. message:`output_item.added(type=message, id=msg_…, status=in_progress, role=assistant, content=[])` → `content_part.added(output_text)` → `output_text.delta`*→`output_text.done` → `content_part.done` → `output_item.done`
5. 每个工具调用：`output_item.added(type=function_call, id=fc_…, call_id=…, name, arguments:"", status=in_progress)` → `function_call_arguments.delta`*→`function_call_arguments.done`(full arguments)→`output_item.done`
6. 收尾：`response.completed`（最终 response 对象 + usage)；正常 stop 但无任何输出时仍需 completed（无效空 turn guard 不在本期范围）；上游 finish_reason=length → `response.incomplete`(incomplete_details.reason=max_output_tokens);length/content_filter 同；中途上游错误/断流且已有产出 → `response.failed`(error.code=server_error）后结束；无任何产出的致命失败 → 直接 HTTP 层错误（上游非 2xx 时本就未开始 SSE)

usage 映射：`input_tokens←prompt_tokens`,`output_tokens←completion_tokens`,`input_tokens_details.cached_tokens←(cache_read_input_tokens | prompt_tokens_details.cached_tokens)`,`output_tokens_details.reasoning_tokens←completion_tokens_details.reasoning_tokens`,`total_tokens`。

非流式下游：聚合上述状态产出最终 response JSON 对象。

## id 规则

- `call_id`：**总是重新签发** `call_<uuid>`，不透传上游 tool_call id(0a4dae8 教训：上游会发跨轮重复的 id，客户端按 id 去重即静默丢 tool)。签发的 id 写进回放的 assistant tool_calls；客户端 function_call_output 原样带回
- `msg_`/`fc_`/`rs_`/`resp_` 前缀 + uuid

## response store(Q5+Q10)

`internal/responses/store.go`：进程内 map,TTL 滑动（默认 24h,`--responses-store-ttl-minutes` 可配）。Entry = `{items: 展开后的完整 history items(input + output), createdAt, touchedAt}`,`Put` 在响应完成后记录；`Get(id)` 命中即续期。`store:false` 请求不写。dump 机制不可复用（非键值查找）。ponytail：纯内存；重启断链→客户端重发完整 input 或收到 404。

## 路由与复用

- `server.go` 新增 `case POST /v1/responses → handleResponses`(`internal/proxy/responses.go`)；鉴权/key 解析/dump session 与 handleMessages 同构；上游请求体 = 本包产物 + model extras + include_usage + PrepareCanonicalBody；SSE pump(单 select 循环 + ping）复用现有结构
- 上游错误/空体/连接错误的状态码映射与 handleMessages 一致，错误体为 OpenAI 形状（`{"error":{message,type,param,code}}`)

## 测试

- `convert_test.go`:input 各 item 映射（文本/图片/工具调用对/reasoning 三种 replay 模式/function_call_output 分组）、hosted tools 400、background 400、text.format 映射、previous_response_id 展开
- `stream_test.go`:fixture chunk 序列 → 事件序列断言（纯文本 / reasoning+text / 单工具 / 两工具并行 / usage / length→incomplete)
- `store_test.go`:TTL 滑动、store:false 不写、过期 404

## 不做（本期，ADR-0001 与 Q9 已决）

hosted tools 执行、background、本地截断、检索端点（GET/DELETE/list)、empty-turn guard 接入 responses 路径、server-tool agentic loop 接入


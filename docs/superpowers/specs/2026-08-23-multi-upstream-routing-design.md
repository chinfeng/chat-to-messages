# 多上游路由设计(Round-Robin + 故障转移)

日期:2026-08-23
状态:已确认(用户逐节批准)

## 背景与目标

chat-to-messages 现为"单进程单上游"架构:`--upstream-base-url` / `--upstream-api-key` 指定唯一上游,`cfg.UpstreamBaseURL` 仅在 3 处拼接触点使用(`buildUpstreamRequest`、`buildUpstreamRequestBodyOnly`、`forwardPassthrough`,见 `internal/proxy/server.go`)。多上游需求(如同供应商多账号分摊限流、异构模型聚合)目前靠跑多进程 + 外部反代解决。

目标:单进程内支持多个上游,按**模型名分组池内轮询**分发请求,池内**流开始前故障转移**。配置以 **JSON 文件**为主形态。

## 需求决策记录

| 决策点 | 结论 |
|---|---|
| 路由依据 | 按模型 glob 分组,组内轮询 |
| 池划分 | 路由规则 `pattern → [upstream...]`,首个匹配生效 |
| 失败处理 | 流开始前转移(连接失败 / 429 / 5xx);流中不转移 |
| 配置形态 | JSON 配置文件(`--config`),与 CLI 配置参数互斥;不传文件时行为与现在完全一致 |
| 模型名重写 | 不做。客户端应发上游真实模型名;别名映射需要时再加 |

## 方案选型

**采用:方案 A — 请求入口解析路由,选出候选链往下传。**

新增 `config.Router`,`Resolve(model)` 返回按轮询起点旋转后的 `[]*Upstream` 候选链;proxy 层把读 `cfg.UpstreamBaseURL` 的触点改为使用解析结果。改动集中在 config 与 proxy 触点签名,SSE 转换、dump、servertool 管线不动。

否决的备选:
- **B:每上游一个内部 sub-handler + mux** — 故障转移跨 handler 边界,dump session 归属混乱。
- **C:`httputil.ReverseProxy`** — 本项目核心是协议转换而非转发,过度设计。

## 配置文件格式

```json
{
  "port": 8082,
  "authToken": "",
  "enableThinking": true,
  "dumpDir": "",
  "upstreams": [
    {
      "name": "zai-1",
      "baseUrl": "https://api.z.ai/api/paas/v4",
      "apiKey": "xxx",
      "modelOverrides": [
        { "pattern": "glm-4.7", "extra": { "thinking": { "type": "enabled" } } }
      ]
    },
    { "name": "zai-2", "baseUrl": "https://api.z.ai/api/paas/v4", "apiKey": "yyy" }
  ],
  "routes": [
    { "pattern": "glm-*",      "upstreams": ["zai-1", "zai-2"] },
    { "pattern": "deepseek-*", "upstreams": ["nim"] },
    { "pattern": "*",          "upstreams": ["nim"] }
  ],
  "serverTools": {
    "webSearch": false,
    "webFetch": false,
    "webSearchEngine": "brave",
    "webSearchAPIKey": "",
    "webSearchBaseURL": "https://api.search.brave.com",
    "webFetchAllowedDomains": [],
    "webFetchBlockedDomains": [],
    "webFetchMaxContentTokens": 5000
  }
}
```

### 字段语义

- **顶层字段与现有 Config 一一对应**(port / authToken / enableThinking / dumpDir);server tools(web search/fetch)配置同样收入文件(`serverTools` 对象,字段同 `ServerToolConfig` 的 JSON 形态)。
- **upstreams[]**:命名上游。`name` 唯一;`baseUrl` 必填;`apiKey` 可空 → 该上游走 passthrough(客户端 key 原样转发给它)。`modelOverrides` 为该上游私有的 extra-params(glob → deep-merge,语义与现有 `--upstream-extra-params` 一致)。
- **routes[]**:声明序匹配,首个 `GlobMatch(pattern, model)` 生效;`upstreams` 引用上游 name 列表,即轮询池。
- **extra-params 归属**:文件模式下只有上游私有 `modelOverrides`,不再有全局层。构建某上游请求体时用该上游自己的 overrides 做匹配与 deep-merge(替代现在对全局列表的 `ResolveModelExtra` 调用),匹配与合并算法不变。CLI 单上游模式的全局 overrides 行为不变。

### 启动校验(违规即启动失败)

1. route 引用的上游名必须存在;
2. upstream `name` 不得重复;
3. upstream `baseUrl` 必填非空;
4. routes 中某路由的 `upstreams` 不得为空数组;
5. `--config` 与其他配置参数同时出现 → 报错退出。规则精确为:`--config` 存在时,命令行出现任何其他 `--` 参数即报错(当前所有 `--` 参数都是配置项,无例外)。

### 向后兼容

不传 `--config` → 现有单上游行为逐字节不变(CLI 参数路径原样保留)。

## 路由解析与轮询

```
Resolve("glm-4.7") →
  首个 GlobMatch 命中 {glm-* → [zai-1, zai-2]}
  counter++ (atomic) → 起点 = counter % len(pool)
  返回旋转后的候选链:[zai-2, zai-1]
```

- 计数器挂在 route 上(`atomic.Uint64`),进程内无其他状态。
- 无匹配路由 → 400 `invalid_request_error: no route configured for model "xxx"`,消息附已配置 pattern 列表。

## 故障转移语义

候选链顺序尝试,**全部发生在向客户端写出第一个字节之前**(现有代码先检查上游状态码再写响应头,转移窗口天然存在):

| 上游表现 | 行为 |
|---|---|
| 连接失败 / 超时 | 试下一个候选 |
| HTTP 429 | 试下一个候选 |
| HTTP 5xx | 试下一个候选 |
| 其他 4xx | 立即返回,不转移 |
| 全部候选失败 | 现有错误路径,返回最后一个的错误 |
| 流开始后中断 | 不变:`event:error`,不转移 |

实现形态:一个"带转移的上游 POST"辅助函数(输入候选链 + 请求构造器,输出最终 `*http.Response` 或错误),普通流式路径与 agentic loop 共用;agentic loop 每次循环调用独立享受转移。

## 各端点行为

| 端点 | 行为 |
|---|---|
| `POST /v1/messages` | 解析路由 → 候选链 → 带 POST 转移的流式管线(agentic loop 同一辅助函数) |
| `POST /v1/chat/completions` | 轻量解出 body `model` → 同一路由规则转发,含转移;缺 `model` → 400 |
| `GET /v1/models` | 并发 GET 所有去重上游,合并 `data` 按 id 去重;个别失败跳过(部分结果),全部失败 502 |
| `GET /health` | 不变 |

## Dump 集成

session 记录每次上游尝试:上游名、URL、结果(含被跳过的中间尝试——哪个 429 了、最后谁接的)。目录结构与 bucket 分类不变。

## 错误处理汇总

- 配置校验失败 → 启动即退出(stderr 明确指出违规项)。
- 无匹配路由 / 缺 model → 400 Anthropic 格式错误(OpenAI 端点用 OpenAI 格式)。
- 全候选失败 → 沿用现有映射(连接失败 502、非 2xx 映射规则不变)。
- `/v1/models` 部分失败 → 正常返回部分结果。

## 测试计划

- **config**:JSON 解析全字段;校验报错(重复名 / 悬空引用 / 空 baseUrl / 空池);`--config` 与 CLI 参数互斥报错;无 `--config` 时 CLI 路径回归。
- **router**:声明序首匹配;轮询序列(n 个上游 n 次请求均匀覆盖且循环);无匹配错误内容。
- **proxy(httptest 假上游)**:
  - 500 / 429 / 连接拒绝 → 转移到下一候选,客户端拿到正常流;
  - 400 → 不转移,原样返回;
  - 轮询分布:两上游池请求序列交替命中;
  - `/v1/models` 合并去重、部分失败降级;
  - `/v1/chat/completions` 按 body.model 路由;
  - dump 含尝试链记录。
- 现有 stream / convert / agentic 测试零改动通过。

## 明确不做(YAGNI)

- 模型名重写 / 别名映射
- 主动健康检查 / 后台探活
- 权重轮询、最少连接等高级均衡
- 流中断后自动换上游重发(无法安全回退)
- 配置热加载

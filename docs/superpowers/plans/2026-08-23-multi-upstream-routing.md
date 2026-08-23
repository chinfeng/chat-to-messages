# 多上游路由实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 单进程内支持多上游:按模型 glob 分组池内轮询,流开始前故障转移,JSON 配置文件驱动,CLI 单上游模式向后兼容。

**Architecture:** 新增 `config.Router`(每条路由持有已解析的 `[]*Upstream` 池 + atomic 轮询计数器),请求入口 `Resolve(model)` 返回旋转后的候选链;proxy 层共享 `postWithFailover` 辅助函数按候选链尝试(连接失败/429/5xx 换下一个,全部发生在向客户端写第一个字节之前)。CLI 模式在 handler 入口规范化为"单上游单路由",proxy 只有一条代码路径。

**Tech Stack:** Go 1.26 标准库(encoding/json、net/http、sync/atomic),零外部依赖。

**分支:** `feat/multi-upstream-routing`(所有 commit 在此分支)

## Global Constraints

- 零外部依赖:只用 Go 标准库
- 不传 `--config` 时现有行为不变;现有测试除 `config.Load` 签名变化外不改即绿(server_test 直接构造 `config.Config{}` 的用例必须继续工作)
- 错误格式沿用现有:Anthropic 格式给 `/v1/messages`,OpenAI 格式给 passthrough 端点
- JSON 配置字段名 camelCase,靠 encoding/json 大小写不敏感匹配,不加 json tag(`baseUrl`↔`BaseURL`)
- 每个 task 结束:`go build ./... && go vet ./... && go test ./...` 全绿再 commit
- Windows 开发机,测试命令:`go test ./internal/xxx -run TestName -v`

---

### Task 1: config 包 — Upstream/Route 类型与 Router

**Files:**
- Create: `internal/config/router.go`
- Create: `internal/config/router_test.go`

**Interfaces:**
- Consumes: 现有 `GlobMatch(pattern, text string) bool`
- Produces(Task 2/4/6/7/8 依赖):
  - `type Upstream struct { Name, BaseURL, APIKey string; ModelOverrides []ModelOverride }`
  - `type Route struct { Pattern string; Names []string }`(未导出:pool + `atomic.Uint64`)
  - `func NewRouter(upstreams []*Upstream, routes []Route) (*Router, error)`
  - `func (rt *Router) Resolve(model string) []*Upstream`(无匹配 → nil)
  - `func (rt *Router) Distinct() []*Upstream`(跨路由去重上游,声明序)
  - `func (rt *Router) Patterns() []string`
  - `func (rt *Router) Describe() []string`(横幅行 `"glm-* -> [zai-1 zai-2]"`)

- [ ] **Step 1: 写失败测试**

```go
package config

import "testing"

func mustRouter(t *testing.T, ups []*Upstream, routes []Route) *Router {
	t.Helper()
	rt, err := NewRouter(ups, routes)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return rt
}

func TestResolveFirstMatchAndRotation(t *testing.T) {
	ups := []*Upstream{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	rt := mustRouter(t, ups, []Route{
		{Pattern: "glm-*", Names: []string{"a", "b"}},
		{Pattern: "*", Names: []string{"c"}},
	})
	// 声明序首匹配;池内轮询:n 次请求均匀覆盖。
	got := rt.Resolve("glm-4.7")
	if len(got) != 2 || got[0].Name != "a" || got[1].Name != "b" {
		t.Fatalf("first resolve = [%s %s]", got[0].Name, got[1].Name)
	}
	got = rt.Resolve("glm-4.7")
	if got[0].Name != "b" || got[1].Name != "a" {
		t.Fatalf("second resolve should rotate: [%s %s]", got[0].Name, got[1].Name)
	}
	got = rt.Resolve("glm-4.7")
	if got[0].Name != "a" {
		t.Fatal("third resolve should wrap to start")
	}
	if name := rt.Resolve("other")[0].Name; name != "c" {
		t.Fatalf("catch-all route should hit c, got %s", name)
	}
}

func TestResolveNoMatch(t *testing.T) {
	rt := mustRouter(t, []*Upstream{{Name: "a"}}, []Route{{Pattern: "glm-*", Names: []string{"a"}}})
	if got := rt.Resolve("deepseek-v4"); got != nil {
		t.Fatalf("no match should return nil, got %v", got)
	}
}

func TestDistinct(t *testing.T) {
	a := &Upstream{Name: "a"}
	b := &Upstream{Name: "b"}
	rt := mustRouter(t, []*Upstream{a, b}, []Route{
		{Pattern: "x-*", Names: []string{"a"}},
		{Pattern: "*", Names: []string{"b", "a"}}, // a 已出现,不重复
	})
	d := rt.Distinct()
	if len(d) != 2 || d[0] != a || d[1] != b {
		t.Fatal("Distinct must dedupe by pointer, declaration order")
	}
}

func TestNewRouterValidation(t *testing.T) {
	cases := []struct {
		name      string
		ups       []*Upstream
		routes    []Route
		wantPanic string // err.Error() 包含
	}{
		{"dangling ref", []*Upstream{{Name: "a"}}, []Route{{Pattern: "*", Names: []string{"ghost"}}}, "unknown upstream"},
		{"dup name", []*Upstream{{Name: "a"}, {Name: "a"}}, []Route{{Pattern: "*", Names: []string{"a"}}}, "duplicate"},
		{"empty baseURL", []*Upstream{{Name: "a"}}, []Route{{Pattern: "*", Names: []string{"a"}}}, "baseUrl"},
		{"empty pool", []*Upstream{{Name: "a"}}, []Route{{Pattern: "*", Names: nil}}, "empty"},
	}
	for _, tc := range cases {
		_, err := NewRouter(tc.ups, tc.routes)
		if err == nil || !contains(err.Error(), tc.wantPanic) {
			t.Errorf("%s: want err containing %q, got %v", tc.name, tc.wantPanic, err)
		}
	}
}

func contains(s, sub string) bool { return len(s) >= len(sub) && stringsContains(s, sub) }
```

(`stringsContains` 用标准库 `strings.Contains` 直接包一层即可,或测试文件里直接 import "strings" 调用。)

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/config -run TestResolve -v`
Expected: FAIL(undefined: NewRouter)

- [ ] **Step 3: 最小实现**

```go
// Package config 内 router.go:模型 glob → 上游池 的轮询路由。
package config

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// Upstream is one named upstream endpoint.
type Upstream struct {
	Name           string
	BaseURL        string
	APIKey         string
	ModelOverrides []ModelOverride
}

// Route maps a model glob pattern to an ordered pool of upstream names.
type Route struct {
	Pattern string
	Names   []string

	pool    []*Upstream   // resolved by NewRouter
	counter atomic.Uint64 // round-robin cursor
}

// Router resolves request models to ordered candidate chains.
type Router struct {
	routes []*Route
}

// NewRouter validates and resolves routes against upstreams:
// names unique, baseUrl required, refs exist, pools non-empty.
func NewRouter(upstreams []*Upstream, routes []Route) (*Router, error) {
	byName := make(map[string]*Upstream, len(upstreams))
	for _, u := range upstreams {
		if u.Name == "" {
			return nil, fmt.Errorf("upstream name must not be empty")
		}
		if _, dup := byName[u.Name]; dup {
			return nil, fmt.Errorf("duplicate upstream name %q", u.Name)
		}
		if u.BaseURL == "" {
			return nil, fmt.Errorf("upstream %q: baseUrl is required", u.Name)
		}
		byName[u.Name] = u
	}
	rt := &Router{}
	for i := range routes {
		r := routes[i]
		if r.Pattern == "" {
			return nil, fmt.Errorf("route pattern must not be empty")
		}
		if len(r.Names) == 0 {
			return nil, fmt.Errorf("route %q: empty upstream pool", r.Pattern)
		}
		for _, n := range r.Names {
			u, ok := byName[n]
			if !ok {
				return nil, fmt.Errorf("route %q references unknown upstream %q", r.Pattern, n)
			}
			r.pool = append(r.pool, u)
		}
		rt.routes = append(rt.routes, &Route{Pattern: r.Pattern, Names: r.Names, pool: r.pool})
	}
	return rt, nil
}

// Resolve returns candidates for model: first matching route's pool rotated
// to start at the round-robin cursor. Nil when no route matches.
func (rt *Router) Resolve(model string) []*Upstream {
	for _, r := range rt.routes {
		if !GlobMatch(r.Pattern, model) {
			continue
		}
		n := len(r.pool)
		start := int((r.counter.Add(1) - 1) % uint64(n))
		out := make([]*Upstream, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, r.pool[(start+i)%n])
		}
		return out
	}
	return nil
}

// Patterns lists configured route patterns in order.
func (rt *Router) Patterns() []string {
	out := make([]string, 0, len(rt.routes))
	for _, r := range rt.routes {
		out = append(out, r.Pattern)
	}
	return out
}

// Distinct dedupes every route's pool by pointer, declaration order.
func (rt *Router) Distinct() []*Upstream {
	var out []*Upstream
	seen := make(map[*Upstream]bool)
	for _, r := range rt.routes {
		for _, u := range r.pool {
			if !seen[u] {
				seen[u] = true
				out = append(out, u)
			}
		}
	}
	return out
}

// Describe renders banner lines: "glm-* -> [zai-1 zai-2]".
func (rt *Router) Describe() []string {
	out := make([]string, 0, len(rt.routes))
	for _, r := range rt.routes {
		names := make([]string, 0, len(r.pool))
		for _, u := range r.pool {
			names = append(names, u.Name)
		}
		out = append(out, fmt.Sprintf("%s -> [%s]", r.Pattern, strings.Join(names, " ")))
	}
	return out
}
```

注意:`NewRouter` 必须复制 Route(counter 是值内嵌 atomic,不能拷贝——上面实现里新建 `&Route{}` 只搬 Pattern/Names/pool,规避 atomic 拷贝 vet 报错)。测试里的 `mustRouter` 传入的是字面量 Route,无碍。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/config -v`
Expected: PASS(原有 config 测试也须全绿)

- [ ] **Step 5: Commit**

```bash
git add internal/config/router.go internal/config/router_test.go
git commit -m "feat: config router with round-robin candidate resolution"
```

---

### Task 2: config 包 — LoadFile 与 --config 互斥

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`(追加用例)

**Interfaces:**
- Consumes: Task 1 的 `NewRouter`
- Produces(Task 4/6/8 依赖):
  - `Load(args []string) (*Config, error)`(**签名变化**,main.go 同步改)
  - `func LoadFile(path string) (*Config, error)`
  - Config 新增导出字段:`Upstreams []*Upstream`、`Routes []Route`(file 模式下填充,legacy 字段清零);legacy CLI 模式下两字段为 nil
  - `--config` 与任何其他 `--` 参数同现 → error

- [ ] **Step 1: 写失败测试**(追加到 config_test.go)

```go
func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routing.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadFileFullSchema(t *testing.T) {
	path := writeTempConfig(t, `{
	  "port": 9000,
	  "authToken": "tok",
	  "enableThinking": false,
	  "dumpDir": "/tmp/d",
	  "upstreams": [
	    {"name":"z1","baseUrl":"https://z.example/v4","apiKey":"k1",
	     "modelOverrides":[{"pattern":"glm-4.7","extra":{"thinking":{"type":"enabled"}}}]},
	    {"name":"n1","baseUrl":"https://n.example/v1"}
	  ],
	  "routes": [
	    {"pattern":"glm-*","upstreams":["z1","n1"]},
	    {"pattern":"*","upstreams":["n1"]}
	  ],
	  "serverTools": {"webSearch": true, "webSearchEngine": "searxng"}
	}`)
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.Port != 9000 || cfg.AuthToken != "tok" || cfg.EnableThinking || cfg.DumpDir != "/tmp/d" {
		t.Fatal("top-level fields mismatch")
	}
	if len(cfg.Upstreams) != 2 || cfg.Upstreams[0].APIKey != "k1" {
		t.Fatal("upstreams mismatch")
	}
	if len(cfg.Upstreams[0].ModelOverrides) != 1 || cfg.Upstreams[0].ModelOverrides[0].Pattern != "glm-4.7" {
		t.Fatal("modelOverrides mismatch")
	}
	if len(cfg.Routes) != 2 || cfg.Routes[0].Names[0] != "z1" {
		t.Fatal("routes mismatch")
	}
	if !cfg.ServerTools.WebSearch || cfg.ServerTools.WebSearchEngine != "searxng" {
		t.Fatal("serverTools mismatch")
	}
	// legacy 字段清零,file 模式只走 Upstreams/Routes。
	if cfg.UpstreamBaseURL != "" || cfg.UpstreamAPIKey != "" {
		t.Fatal("legacy fields must be zero in file mode")
	}
	// 默认值:未给出的 serverTools 子项沿用现有默认。
	cfg2, err := LoadFile(writeTempConfig(t, `{
	  "upstreams":[{"name":"a","baseUrl":"http://x/v1"}],
	  "routes":[{"pattern":"*","upstreams":["a"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg2.Port != 8082 || !cfg2.EnableThinking || cfg2.ServerTools.WebSearchEngine != "brave" ||
		cfg2.ServerTools.WebSearchBaseURL != "https://api.search.brave.com" ||
		cfg2.ServerTools.WebFetchMaxContentTokens != 5000 {
		t.Fatal("defaults not applied")
	}
}

func TestLoadFileValidationErrors(t *testing.T) {
	cases := map[string]string{
		`{"upstreams":[{"name":"a","baseUrl":""}],"routes":[{"pattern":"*","upstreams":["a"]}]}`: "baseUrl",
		`{"upstreams":[{"name":"a","baseUrl":"http://x"}],"routes":[{"pattern":"*","upstreams":["b"]}]}`: "unknown upstream",
	}
	for body, want := range cases {
		if _, err := LoadFile(writeTempConfig(t, body)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("want err containing %q, got %v", want, err)
		}
	}
}

func TestLoadMutualExclusion(t *testing.T) {
	path := writeTempConfig(t, `{}`)
	if _, err := Load([]string{"--config", path, "--port", "9999"}); err == nil {
		t.Fatal("--config plus another flag must error")
	}
	cfg, err := Load([]string{"--config", path})
	if err != nil || cfg.Port != 8082 {
		t.Fatalf("lone --config should load defaults, got %v", err)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/config -run 'TestLoad' -v`
Expected: FAIL(undefined: LoadFile / Load 签名不符编译错误)

- [ ] **Step 3: 实现**

config.go 改动:

```go
// Config 新增两个字段(file 模式):
type Config struct {
	// ... 现有字段不动 ...
	Upstreams []*Upstream // file mode only
	Routes    []Route     // file mode only
}

// Load 返回值加 error:--config 与其他参数互斥时返回错误。
func Load(args []string) (*Config, error) {
	// ... 现有 getArg/getBool/getMultiArg/parseInt 不动 ...
	configPath := ""
	rest := args[:0:0]
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--config" && i+1 < len(args):
			configPath = args[i+1]
			i++
		case strings.HasPrefix(args[i], "--config="):
			configPath = args[i][len("--config="):]
		default:
			rest = append(rest, args[i])
		}
	}
	if configPath != "" && len(rest) > 0 {
		return nil, fmt.Errorf("--config cannot be combined with other arguments (got %q)", rest[0])
	}
	if configPath != "" {
		return LoadFile(configPath)
	}
	// ... 现有解析逻辑,return 处改为 return &Config{...}, nil ...
}

// LoadFile reads a routing JSON file and applies the same defaults as the
// CLI path. Validation runs through NewRouter.
func LoadFile(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var doc struct {
		Port            int             `json:"port"`
		AuthToken       string          `json:"authToken"`
		EnableThinking  *bool           `json:"enableThinking"`
		DumpDir         string          `json:"dumpDir"`
		Upstreams       []*Upstream     `json:"upstreams"`
		Routes          []Route         `json:"routes"`
		ServerTools     *ServerToolJSON `json:"serverTools"`
	}
	// Route.Names 从 JSON 的 "upstreams" 键读 —— 需要别名,见下方说明。
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("invalid config file: %w", err)
	}
	if _, err := dec.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("invalid config file: trailing data after JSON object")
	}
	if doc.EnableThinking == nil {
		def := true
		doc.EnableThinking = &def
	}
	st := defaultServerTools() // 抽取现有默认值构造为函数,CLI 路径复用
	if doc.ServerTools != nil {
		st = *doc.ServerTools.materialize()
	}
	cfg := &Config{
		UpstreamBaseURL: "", // file 模式 legacy 字段清零
		AuthToken:       doc.AuthToken,
		Port:            doc.Port,
		EnableThinking:  *doc.EnableThinking,
		DumpDir:         doc.DumpDir,
		Upstreams:       doc.Upstreams,
		Routes:          doc.Routes,
		ServerTools:     st,
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		warn("Invalid port %d (must be between 1 and 65535); falling back to 8082", cfg.Port)
		cfg.Port = 8082
	}
	// 校验:构建一次 Router(丢弃实例,校验语义与运行期共用同一份代码)。
	if _, err := NewRouter(cfg.Upstreams, cfg.Routes); err != nil {
		return nil, err
	}
	return cfg, nil
}
```

两个实现细节:

1. **JSON `upstreams` 键 → `Route.Names`**:encoding/json 无字段别名。Route 加 json tag 不行(Names 要映射到键名 "upstreams")。解法:LoadFile 的 wire 结构体单独定义:

```go
var docRoutes []struct {
	Pattern   string   `json:"pattern"`
	Upstreams []string `json:"upstreams"`
}
```
把 `Routes []Route` 也从 doc 中拿出来,decode 到上述匿名结构再转成 `[]Route{{Pattern: p, Names: ns}}`。(wire 结构体是本文件局部类型,不外泄。)

2. **ServerToolJSON**:`ServerToolConfig` 里 `WebFetchMaxContentTokens int` 等可直接 decode;定义 `type ServerToolJSON ServerToolConfig` + `materialize()` 填默认(engine=brave、base URL、max tokens 5000)。现有 CLI 构造处同步改用 `defaultServerTools()` 再覆盖。

main.go 同步改动(本 task 内):

```go
cfg, err := config.Load(os.Args[1:])
if err != nil {
	log.Fatal(err)
}
```

同时更新 config_test.go 中所有 `config.Load(...)` 单返回值调用为双返回值(机械替换,断言不变)。

- [ ] **Step 4: 全包测试通过**

Run: `go test ./internal/config -v && go build ./...`
Expected: PASS,build 成功(main.go 编译过)

- [ ] **Step 5: Commit**

```bash
git add internal/config/ main.go
git commit -m "feat: --config JSON file loading with validation and mutual exclusion"
```

---

### Task 3: dump.Session — 上游尝试日志

**Files:**
- Modify: `internal/dump/dump.go`
- Modify: `internal/dump/dump_test.go`

**Interfaces:**
- Produces(Task 5/6/7 依赖):
  - `func (s *Session) LogUpstreamAttempt(name, url string, status int, outcome string)` — 追加一条;`Finish()` 时写入 `upstream-attempts.log`(与 toolLogs 同机制);noop 会话无操作

- [ ] **Step 1: 写失败测试**(追加到 dump_test.go)

```go
func TestLogUpstreamAttemptWrittenOnFinish(t *testing.T) {
	dir := t.TempDir()
	s := dump.NewSession(dir)
	s.LogUpstreamAttempt("zai-1", "https://z/v1/chat/completions", 429, "skipped: retryable status")
	s.LogUpstreamAttempt("zai-2", "https://y/v1/chat/completions", 200, "served")
	s.Finish()
	// 找到 bucket 目录里的 attempts 文件并断言内容包含两条记录。
	matches, _ := filepath.Glob(filepath.Join(dir, "*", "*__START_*", "upstream-attempts.log"))
	if len(matches) != 1 {
		t.Fatalf("attempts log missing: %v", matches)
	}
	data, _ := os.ReadFile(matches[0])
	text := string(data)
	if !strings.Contains(text, "zai-1") || !strings.Contains(text, "429") ||
		!strings.Contains(text, "skipped") || !strings.Contains(text, "zai-2") {
		t.Fatalf("unexpected attempts log:\n%s", text)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/dump -run TestLogUpstreamAttempt -v`
Expected: FAIL(undefined: LogUpstreamAttempt)

- [ ] **Step 3: 实现**(dump.go,仿 toolLogs)

```go
// Session struct 加一个字段:attemptLogs []string

// LogUpstreamAttempt records one upstream attempt (failover trail).
func (s *Session) LogUpstreamAttempt(name, url string, status int, outcome string) {
	if s.noop {
		return
	}
	statusStr := strconv.Itoa(status)
	if status == 0 {
		statusStr = "-"
	}
	s.attemptLogs = append(s.attemptLogs,
		fmt.Sprintf("[%s] %s (%s)\nOutcome: %s\n---\n", time.Now().UTC().Format(time.RFC3339), name, url, statusStr)+""+
			"Outcome: "+outcome+"\n---\n")
}

// Finish() 中 toolLogs 写入之后追加:
if len(s.attemptLogs) > 0 {
	_ = os.WriteFile(filepath.Join(s.tmpDir, "upstream-attempts.log"), []byte(strings.Join(s.attemptLogs, "\n")), 0o644)
}
```

(实现时把 LogUpstreamAttempt 里的字符串拼接理顺成一次 Sprintf:`fmt.Sprintf("[%s] %s (%s)\nStatus: %s\nOutcome: %s\n---\n", ...)`,status==0 时 Status 行为 "-"。)

- [ ] **Step 4: 测试通过**

Run: `go test ./internal/dump -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/dump/
git commit -m "feat: dump session upstream attempt trail log"
```

### Task 4: proxy — 候选链规范化(池=1 时行为与今天一致)

**Files:**
- Modify: `internal/proxy/server.go`
- Modify: `internal/proxy/server_test.go`(追加用例)

**Interfaces:**
- Consumes: Task 1 的 Router;Task 2 的 Config.Upstreams/Routes
- Produces(Task 5/6/7/8 依赖):
  - `func (c *config.Config) Router() *config.Router`(**落在 config 包**,proxy/main 共用;每次调用重建,成本可忽略):`len(c.Upstreams)>0` 时经 NewRouter 构造(失败返回 nil),否则用 `MustSyntheticRouter` 从 legacy 字段合成
  - `func MustSyntheticRouter(u *Upstream) *Router`(绕过校验的合成路径,router.go 内)
  - `func effectiveKey(u *config.Upstream, clientKey string) string` = `u.APIKey`,为空则 `clientKey`
  - `buildUpstreamRequest(u *config.Upstream, requestData, apiKey)` / `buildUpstreamRequestBodyOnly(...)` 签名改为接收 upstream(overrides 取 `u.ModelOverrides`,URL 拼 `u.BaseURL`)
  - `handleMessages` 内:`rt := cfg.Router(); candidates := rt.Resolve(req.Model)`;nil → 400 `no route configured for model %q (configured patterns: ...)`
  - 本 task 不加转移:取 `candidates[0]`,其余候选忽略(Task 5 启用)

- [ ] **Step 1: 写失败测试**(追加到 server_test.go)

```go
func TestNoRouteForModel(t *testing.T) {
	cfg := &config.Config{
		Upstreams: []*config.Upstream{{Name: "a", BaseURL: httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "unused") })).URL}},
		Routes: []config.Route{{Pattern: "glm-*", Names: []string{"a"}}},
	}
	h := proxy.NewHandler(cfg) // 包名按现有 server_test.go 实际 import 方式调整
	body := `{"model":"deepseek-v4","messages":[]}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 400 || !strings.Contains(rec.Body.String(), "no route configured") {
		t.Fatalf("want 400 no route, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestRoutedToConfiguredUpstream(t *testing.T) {
	var hitModel string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		hitModel, _ = b["model"].(string)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseMinimalResponse) // 复用现有测试里已有的最小 SSE 流常量/字面量
	}))
	defer up.Close()
	cfg := &config.Config{
		Upstreams: []*config.Upstream{{Name: "z1", BaseURL: up.URL, APIKey: "k-z"}},
		Routes:    []config.Route{{Pattern: "*", Names: []string{"z1"}}},
	}
	// ... 发送 {"model":"glm-4.7","messages":[]} 到 /v1/messages,断言 200 且 hitModel=="glm-4.7"
}
```

(实现时把第二个测试补完整:参照文件内现有流式测试的断言方式——读响应体含 `message_stop` 即可。)

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/proxy -run 'TestNoRoute|TestRouted' -v`
Expected: FAIL(Config 无 Upstreams 字段 → 编译错;Task 2 已解决,此时应为运行期失败)

- [ ] **Step 3: 实现**

server.go 关键改动(全部在现有函数体内,不新建文件):

```go
// (config 包 router.go)Router normalizes both modes. Called per request so
// tests may mutate the shared cfg pointer between requests.
func (c *Config) Router() *Router {
	if len(c.Upstreams) > 0 {
		rt, err := NewRouter(c.Upstreams, c.Routes)
		if err != nil {
			return nil // LoadFile 已校验过;防御性兜底
		}
		return rt
	}
	return MustSyntheticRouter(&Upstream{
		Name:           "default",
		BaseURL:        c.UpstreamBaseURL,
		APIKey:         c.UpstreamAPIKey,
		ModelOverrides: c.ModelOverrides,
	})
}

// MustSyntheticRouter builds a single-upstream catch-all router without
// validation (legacy CLI / direct-struct construction path).
func MustSyntheticRouter(u *Upstream) *Router {
	r := &Route{Pattern: "*", Names: []string{u.Name}, pool: []*Upstream{u}}
	return &Router{routes: []*Route{r}}
}
```

`effectiveKey` 与鉴权语义:

```go
// effectiveKey resolves the key for one candidate: its own key wins;
// empty → forward the client's key (per-upstream passthrough).
func effectiveKey(u *config.Upstream, clientKey string) string {
	if u.APIKey != "" {
		return u.APIKey
	}
	return clientKey
}
```

`handleMessages` 改动点:
1. 解析 body 后:`rt := cfg.Router()`;`candidates := rt.Resolve(req.Model)` 为 nil → `writeJSON(w, 400, invalidRequestError(fmt.Sprintf("no route configured for model %q (configured patterns: %s)", req.Model, strings.Join(rt.Patterns(), ", "))))`
2. `apiKey := resolveAPIKey(r, cfg)` 一行替换为 `clientKey := clientKey(r)`;后续 `apiKey := effectiveKey(candidates[0], clientKey)`;apiKey 为空 → 原 401 错误文案不变
3. `buildUpstreamRequest(cfg, requestData, apiKey)` → `buildUpstreamRequest(candidates[0], requestData, apiKey)`;内部 URL 拼 `u.BaseURL`、overrides 用 `u.ModelOverrides`(删掉 `config.ResolveModelExtra(requestData.Model, cfg.ModelOverrides)` 改为 `config.ResolveModelExtra(requestData.Model, u.ModelOverrides)`)
4. agentic 分支传参同步改(Task 7 深入,本 task 只保证编译:把 `handleServerToolRequest(w, r, cfg, session, requestStart, requestData, apiKey, inputTokens)` 的 apiKey 参数改为传 `candidates` 与 `clientKey`,函数内暂时仍取 `candidates[0]`)
5. `isPassthroughMode`/`resolveAPIKey` 删除,`validateAuthToken(r, cfg)` 保留(authToken 是全局下游鉴权)

`forwardPassthrough` 本 task 保持原样(继续读 legacy 字段;file 模式下 legacy 为空 → 它对 `/v1/models`、`/v1/chat/completions` 返回 401?)——不行,会破坏 file 模式。**最小修**:forwardPassthrough 开头也做规范化:`ups := cfg.Router().Distinct(); u := ups[0]`,用 `u.BaseURL`/`effectiveKey` 替换 cfg 直读。Task 6 再彻底拆分。

- [ ] **Step 4: 全量测试通过**

Run: `go test ./... && go vet ./...`
Expected: 全绿(**现有 server_test 全部不改即过**是本 task 的验收线)

- [ ] **Step 5: Commit**

```bash
git add internal/proxy/ internal/config/router.go
git commit -m "feat: route /v1/messages through candidate chain (single-candidate semantics)"
```

---

### Task 5: postWithFailover — 故障转移 + 尝试日志

**Files:**
- Modify: `internal/proxy/server.go`
- Modify: `internal/proxy/server_test.go`

**Interfaces:**
- Consumes: Task 4 的 `effectiveKey`/`buildUpstreamRequest`;Task 3 的 `LogUpstreamAttempt`
- Produces:
  - `func postWithFailover(ctx context.Context, candidates []*config.Upstream, clientKey string, mkReq func(u *config.Upstream, apiKey string) (*http.Request, error), session *dump.Session) (*http.Response, error)`
  - 重试判定:`err != nil || status == 429 || status >= 500`;跳过的尝试记 `LogUpstreamAttempt(name, url, status, "skipped: ...")`;最后一个候选的错误原样返回(连接错误由调用方走 handleUpstreamConnectError,非 2xx 走 handleUpstreamErrorStatus)

- [ ] **Step 1: 写失败测试**(追加到 server_test.go)

```go
// 两上游池:第一个返回 500,第二个正常 → 客户端拿到正常流。
func TestFailoverOn500(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"error":{"message":"boom"}}`)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: message_start\ndata: {}\n\n")
	}))
	defer second.Close()

	cfg := &config.Config{
		Upstreams: []*config.Upstream{
			{Name: "bad", BaseURL: first.URL},
			{Name: "good", BaseURL: second.URL},
		},
		Routes: []config.Route{{Pattern: "*", Names: []string{"bad", "good"}}},
	}
	// POST /v1/messages model=x 断言:200、body 含 message_start、
	// 且 dump 目录(设 cfg.DumpDir=t.TempDir())upstream-attempts.log 含 "bad" 与 "skipped"。
}

// 第一个 400 → 不转移,客户端收到映射后的错误。
func TestNoFailoverOn400(t *testing.T) {
	// bad-up handler 返回 404(不可重试类);second 若被命中会写标记。
	// 断言:响应含 "Upstream returned 404"、second 未被请求。
}

// 连接拒绝(关闭的端口)→ 转移。
func TestFailoverOnConnectRefused(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close() // 端口已释放
	// pool=[dead, good];断言客户端拿到 good 的正常流。
}

// 轮询分布:同一路由连发 2 个请求,起点交替。
func TestRoundRobinDistribution(t *testing.T) {
	// 两个都正常的上游,atomic 计数各收 1 个请求。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/proxy -run 'TestFailover|TestNoFailover|TestRoundRobinDist' -v`
Expected: FAIL(500 后直接把错误返给客户端,未尝试 good)

- [ ] **Step 3: 实现**

```go
// postWithFailover tries candidates in order until one responds without a
// retryable failure. Retryable: transport error, HTTP 429, HTTP 5xx.
// Skipped attempts land in the dump attempt trail. The returned response is
// the LAST candidate's outcome (caller maps errors as today).
func postWithFailover(ctx context.Context, candidates []*config.Upstream, clientKey string,
	mkReq func(u *config.Upstream, apiKey string) (*http.Request, error),
	session *dump.Session) (*http.Response, error) {

	client := &http.Client{}
	var lastRes *http.Response
	for i, u := range candidates {
		apiKey := effectiveKey(u, clientKey)
		if apiKey == "" {
			return nil, errNoAPIKey // sentinel;handleMessages 映射成现有 401 文案
		}
		req, err := mkReq(u, apiKey)
		if err != nil {
			return nil, err
		}
		res, err := client.Do(req)
		url := strings.TrimRight(u.BaseURL, "/") + "/chat/completions"
		if err != nil {
			if ctx.Err() != nil {
				return nil, err // 客户端已断开,不再转移
			}
			session.LogUpstreamAttempt(u.Name, url, 0, "skipped: connect failed ("+err.Error()+")")
			lastRes, lastErr = nil, err
		} else if res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500 {
			body, _ := io.ReadAll(io.LimitReaders(res.Body)) // 读掉并关闭,复用连接
			res.Body.Close()
			_ = body
			session.LogUpstreamAttempt(u.Name, url, res.StatusCode, fmt.Sprintf("skipped: retryable status %d", res.StatusCode))
			lastRes, lastErr = nil, nil
		} else {
			if i > 0 {
				session.LogUpstreamAttempt(u.Name, url, res.StatusCode, "served after failover")
			}
			return res, nil
		}
		if i == len(candidates)-1 {
			if lastRes != nil {
				return lastRes, nil
			}
			return nil, lastErr // 连接错误链;调用方按现有 connect-error 处理
		}
	}
	return nil, fmt.Errorf("no candidates")
}
```

(实现时理顺:循环里维护 `var lastErr error`;上面伪码的 `lastRes` 分支只在"最后候选是非连接类失败"时需要——实际上非 2xx 不可重试类在 else 已返回,可重试类的最后一次应把该 res 返回给调用方走 handleUpstreamErrorStatus。正确形态:可重试分支里保存 `lastRetryableRes = res`,循环结束返回它。)

handleMessages 集成:把"构建请求 → client.Do → 错误分流"整段替换为:

```go
mkReq := func(u *config.Upstream, apiKey string) (*http.Request, error) {
	_, wireBody, prettyBody, err := buildUpstreamRequest(u, requestData, apiKey)
	if err != nil { return nil, err }
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		strings.TrimRight(u.BaseURL, "/")+"/chat/completions", bytes.NewReader(wireBody))
	if err != nil { return nil, err }
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	session.WriteUpstreamRequest(headerMap(req.Header), time.Now().UTC().Format(time.RFC3339), string(prettyBody))
	return req, nil
}
upstreamRes, err := postWithFailover(r.Context(), candidates, clientKey, mkReq, session)
if err == errNoAPIKey { /* 现有 401 分支 */ }
```

注意:`session.WriteUpstreamRequest` 是覆盖写,每次尝试都会重写——最终文件内容即最后一次尝试的请求,配合 attempts 日志构成完整链条,符合 spec。

- [ ] **Step 4: 测试通过 + 回归**

Run: `go test ./... -v`
Expected: 全绿(含 Task 4 之前所有既有测试)

- [ ] **Step 5: Commit**

```bash
git add internal/proxy/
git commit -m "feat: pre-stream failover across candidate chain with attempt trail"
```

### Task 6: passthrough 端点 — chat/completions 路由化 + models 合并

**Files:**
- Modify: `internal/proxy/server.go`
- Modify: `internal/proxy/server_test.go`

**Interfaces:**
- Consumes: Task 5 的 `postWithFailover`;`Config.Router()`/`effectiveKey`
- Produces:
  - `handleChatCompletions(w, r, cfg)`:读 body → 轻量解出 `{"model": string}`(缺/非 string → OpenAI 格式 400)→ Resolve → `postWithFailover`(mkReq 原样转发 body 到 `u.BaseURL+/chat/completions`)→ 现有 relay 逻辑原样复用
  - `handleModels(w, r, cfg)`:`router.Distinct()`;恰 1 个 → 现有 verbatim 路径不变;>1 → 并发 GET 各上游 `/models`,`{"data": [...]}` 按 id 去重合并(声明序优先),个别上游失败跳过,全部失败 → OpenAI 格式 502

- [ ] **Step 1: 写失败测试**(追加到 server_test.go)

```go
func TestChatCompletionsRoutedByBodyModel(t *testing.T) {
	hitA, hitB := &atomic.Bool{}, &atomic.Bool{}
	a := httptest.NewServer(...) // 命中置 hitA,回 {"choices":[]}
	b := httptest.NewServer(...) // 命中置 hitB
	cfg := &config.Config{
		Upstreams: []*config.Upstream{{Name: "a", BaseURL: a.URL}, {Name: "b", BaseURL: b.URL}},
		Routes: []config.Route{
			{Pattern: "glm-*", Names: []string{"a"}},
			{Pattern: "*", Names: []string{"b"}},
		},
	}
	// POST /v1/chat/completions {"model":"glm-4","messages":[]} → hitA=true, hitB=false, 200
	// POST /v1/chat/completions {"model":"other","messages":[]} → hitB=true
	// POST body 缺 model → 400 且 body 是 OpenAI 错误格式({"error":{...}})
}

func TestModelsMergedAcrossUpstreams(t *testing.T) {
	a := httptest.NewServer(...回 `{"object":"list","data":[{"id":"glm-4"},{"id":"shared"}]}`...)
	b := httptest.NewServer(...回 `{"data":[{"id":"deepseek-v4"},{"id":"shared"}]}`...)
	// GET /v1/models → 200,data 恰好 3 个:glm-4、deepseek-v4、shared(去重,a 先出现)
	// b 关掉再请求 → 200 仅 a 的模型(部分失败降级)
}

func TestModelsSingleUpstreamVerbatim(t *testing.T) {
	// 单上游(file 或 legacy):响应体与上游逐字节一致(现有测试已覆盖 legacy;
	// 这里补 file 单上游一例)。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/proxy -run 'TestChatCompletions|TestModels' -v`
Expected: 相关新用例 FAIL

- [ ] **Step 3: 实现**

handleRequest 分发表改为:

```go
case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
	handleChatCompletions(w, r, cfg)
case r.Method == http.MethodGet && r.URL.Path == "/v1/models":
	handleModels(w, r, cfg)
```

```go
// handleChatCompletions relays an OpenAI chat completion request to the
// upstream selected by the body's model field (failover applies pre-stream).
func handleChatCompletions(w http.ResponseWriter, r *http.Request, cfg *config.Config) {
	if !validateAuthToken(r, cfg) {
		writeJSON(w, http.StatusUnauthorized, openAIError("authentication_error", "Invalid auth token. Provide correct x-api-key header."))
		return
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, openAIError("invalid_request_error", "Invalid request body."))
		return
	}
	var peek struct{ Model string }
	if err := json.Unmarshal(raw, &peek) || peek.Model == ""; err != nil {
		writeJSON(w, http.StatusBadRequest, openAIError("invalid_request_error", "`model` is required in request body."))
		return
	}
	router := cfg.Router()
	candidates := router.Resolve(peek.Model)
	if candidates == nil {
		writeJSON(w, http.StatusBadRequest, openAIError("invalid_request_error",
			fmt.Sprintf("no route configured for model %q (configured patterns: %s)", peek.Model, strings.Join(router.Patterns(), ", "))))
		return
	}
	mkReq := func(u *config.Upstream, apiKey string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(r.Context(), r.Method,
			strings.TrimRight(u.BaseURL, "/")+"/chat/completions", bytes.NewReader(raw))
		if err != nil { return nil, err }
		ct := r.Header.Get("Content-Type")
		if ct == "" { ct = "application/json" }
		req.Header.Set("Content-Type", ct)
		req.Header.Set("Authorization", "Bearer "+apiKey)
		return req, nil
	}
	res, err := postWithFailover(r.Context(), candidates, clientKey(r), mkReq, dump.NewSession(""))
	if err == errNoAPIKey {
		writeJSON(w, http.StatusUnauthorized, openAIError("authentication_error", "No API key provided. ..."))
		return
	}
	if err != nil {
		writeJSON(w, http.StatusBadGateway, openAIError("api_error", "Upstream error: "+err.Error()))
		return
	}
	defer res.Body.Close()
	relayResponse(w, res) // 从现 forwardPassthrough 尾部抽出:status+CT+SSE 头+io.Copy
}

// handleModels merges /models across all distinct upstreams.
func handleModels(w http.ResponseWriter, r *http.Request, cfg *config.Config) {
	if !validateAuthToken(r, cfg) { /* 同上 401 */ }
	ups := cfg.Router().Distinct()
	if len(ups) == 1 { // verbatim 兼容路径
		forwardPassthrough(w, r, cfg, "/models") // 内部改用 ups[0] 的 BaseURL/effectiveKey(Task 4 已做)
		return
	}
	type result struct {
		data []map[string]any
		ok   bool
	}
	results := make([]result, len(ups))
	var wg sync.WaitGroup
	for i, u := range ups {
		wg.Add(1)
		go func(i int, u *config.Upstream) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(r.Context(), http.MethodGet,
				strings.TrimRight(u.BaseURL, "/")+"/models", nil)
			if err != nil { return }
			req.Header.Set("Authorization", "Bearer "+effectiveKey(u, clientKey(r)))
			res, err := (&http.Client{}).Do(req)
			if err != nil { return }
			defer res.Body.Close()
			if res.StatusCode < 200 || res.StatusCode >= 300 { return }
			var doc struct{ Data []map[string]any }
			if json.NewDecoder(res.Body).Decode(&doc) != nil { return }
			results[i] = result{data: doc.Data, ok: true}
		}(i, u)
	}
	wg.Wait()
	seen := make(map[string]bool)
	var merged []map[string]any
	anyOK := false
	for _, res := range results {
		if !res.ok { continue }
		anyOK = true
		for _, m := range res.data {
			id, _ := m["id"].(string)
			if id == "" || seen[id] { continue }
			seen[id] = true
			merged = append(merged, m)
		}
	}
	if !anyOK {
		writeJSON(w, http.StatusBadGateway, openAIError("api_error", "Upstream error: all upstreams failed for /v1/models"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": merged})
}
```

(`errNoAPIKey` 在 Task 5 定义为包级 sentinel:`var errNoAPIKey = errors.New("no api key")`。)

- [ ] **Step 4: 测试通过**

Run: `go test ./internal/proxy -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/proxy/
git commit -m "feat: model-routed chat completions passthrough and merged models listing"
```

---

### Task 7: agentic loop 接入候选链

**Files:**
- Modify: `internal/proxy/agentic.go`
- Modify: `internal/proxy/server_test.go`(或新建 agentic_test 用例,按现有文件组织)

**Interfaces:**
- Consumes: Task 5 的 `postWithFailover`;Task 4 改过的 `handleServerToolRequest` 签名(candidates + clientKey)
- Produces: `handleServerToolRequest(w, r, cfg, session, requestStart, requestData, candidates []*config.Upstream, clientKey string, inputTokens int64)`;循环与最终流式请求都经 postWithFailover

- [ ] **Step 1: 写失败测试**(追加到 server_test.go 的 server-tool 测试区)

```go
// 启用 web_search 的请求,池第一个上游 500,第二个正常完成 agentic 回合。
func TestAgenticLoopFailsOver(t *testing.T) {
	// bad: 500;good: 返回一段含文本的 SSE 流(无 tool call → 一轮即终)。
	// 断言:客户端 200 收到 message_stop;attempts 日志含 bad skipped。
}
```

(参照文件内已有 agentic 测试构造 SSE 流的方式——`internal/proxy/agentic_test.go` 已存在,把用例放那里更合适。)

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/proxy -run TestAgenticLoopFailsOver -v`
Expected: FAIL(bad 的 500 直接变错误响应)

- [ ] **Step 3: 实现**

agentic.go 三处 `postUpstream(ctx, upstreamURL, upstreamHeadersObj, bodyXxx)` 全部替换:

```go
mkReq := func(u *config.Upstream, apiKey string) (*http.Request, error) {
	return postUpstreamReq(r.Context(), u.BaseURL, apiKey, currentBody) // 新小助手:NewRequest+headers
}
upstreamRes, err := postWithFailover(r.Context(), candidates, clientKey, mkReq, session)
```

初始请求日志 `session.WriteUpstreamRequest(upstreamHeadersObj, ...)` 删除(headers 里已无固定 apiKey,由 mkReq 内每候选设置;最终尝试的请求由 mkReq 里的 WriteUpstreamRequest 记录——把该调用移进 mkReq,同 Task 5 模式)。`buildUpstreamRequestBodyOnly(cfg, requestData)` → `buildUpstreamRequestBodyOnly(candidates[0], requestData)`(仅用于取初始 messages/tools 解析,URL 不再外泄)。函数签名按 Interfaces 声明改,server.go 调用点同步。

- [ ] **Step 4: 测试通过 + 全量回归**

Run: `go test ./... && go vet ./...`
Expected: 全绿

- [ ] **Step 5: Commit**

```bash
git add internal/proxy/
git commit -m "feat: agentic loop through candidate chain with failover"
```

---

### Task 8: 启动横幅路由表 + README + 收尾验证

**Files:**
- Modify: `main.go`
- Modify: `README.md`、`README-zh.md`

**Interfaces:**
- Consumes: Task 1 `Router.Describe()`;Task 2 Load 双返回值
- Produces: 无代码接口;文档收尾

- [ ] **Step 1: 横幅改动**

main.go 现横幅的 Upstream 行替换为:

```go
if len(cfg.Upstreams) > 0 {
	fmt.Printf("  Upstreams:\n")
	for _, u := range cfg.Upstreams {
		fmt.Printf("    %s -> %s (key: %s)\n", u.Name, u.BaseURL, boolWord(u.APIKey != ""))
	}
	fmt.Printf("  Routes:\n")
	rt := cfg.Router() // Task 4 落位于 config 包的规范化方法,proxy 与 main 共用
	for _, line := range rt.Describe() {
		fmt.Printf("    %s\n", line)
	}
} else {
	fmt.Printf("  Upstream: %s\n", cfg.UpstreamBaseURL)
	fmt.Printf("  Upstream API key: %s\n", boolWord(cfg.UpstreamAPIKey != ""))
}
```

注:`Config.Router()` 已在 Task 4 落位于 config 包(内部处理 legacy 合成),本 task 只是消费。

- [ ] **Step 2: 手动冒烟**

```powershell
go build -o chat-to-messages.exe .
# CLI 模式启动确认旧横幅正常
./chat-to-messages.exe --upstream-base-url https://api.openai.com/v1 --port 18082
# file 模式:写一个两上游临时配置,启动确认 Routes 表打印,Ctrl+C 退出
```

- [ ] **Step 3: README 更新**

README.md / README-zh.md:
- "Design Philosophy" 段落改写:单进程单上游仍是默认形态;新增 `--config` 多上游路由(轮询 + 流前故障转移),链接 spec
- CLI 参数表加 `--config` 行
- 新章节 "Multi-Upstream Routing"(JSON 示例直接抄 spec)、错误转移语义表
- API Endpoints 表 `/v1/models` 描述改为 "merged across upstreams when configured with multiple"

- [ ] **Step 4: 最终验证**

Run: `go build ./... ; go vet ./... ; go test ./... ; gofmt -l .`
Expected: 全绿,gofmt 无输出

- [ ] **Step 5: Commit**

```bash
git add main.go README.md README-zh.md
git commit -m "docs: multi-upstream routing banner and documentation"
```

---

## 任务依赖图

```
T1 ── T2 ── T4 ── T5 ── T7
            │     ├── T6
T3 ─────────┘─────┘(T3 只被 T5 的 attempts 日志消费)
                  T8(最后,消费 T1 Describe + 文档)
```

## Self-Review 结论

- Spec 覆盖:T1=轮询/校验,T2=JSON 配置/互斥/默认值,T3=dump 尝试链,T4=T5=路由管线/故障转移/鉴权语义,T6=passthrough 两端点,T7=agentic,T8=横幅/文档。spec 的"无匹配 400 附 pattern 列表"、"单上游 verbatim /v1/models"、"全候选失败走现有映射"均落在具体 task。
- 类型一致性:`Resolve` 返回 `[]*Upstream`、`postWithFailover` 五参签名、`Config.Router()` 方法(Task 4 落位于 config 包,Task 8 横幅直接复用)、`LogUpstreamAttempt(name, url, status, outcome)`。
- 遗留修正点(实现者注意):Task 5 伪码中 lastRes/lastErr 循环变量需理顺(注释已给正确形态)。



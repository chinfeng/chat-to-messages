package parsers

import (
	"testing"
)

func TestHeuristicFunctionTool(t *testing.T) {
	p := NewHeuristicToolParser()
	filtered, tools := p.Feed("● <function=read_file><parameter=path>/etc/hosts</parameter>")
	if filtered != "" {
		t.Errorf("filtered = %q", filtered)
	}
	// 裁决: 跟随 TS 逐行端口 — 参数耗尽且 buffer 为空时不立即完成工具，
	// 工具在 flush 时产出（bun 实证 TS: feed 返回 0 个工具）。
	if len(tools) != 0 {
		t.Fatalf("tools = %d", len(tools))
	}
	_, tools = p.Flush()
	if len(tools) != 1 {
		t.Fatalf("flush tools = %d", len(tools))
	}
	if tools[0]["name"] != "read_file" {
		t.Errorf("name = %v", tools[0]["name"])
	}
	if tools[0]["type"] != "tool_use" {
		t.Errorf("type = %v", tools[0]["type"])
	}
	in := tools[0]["input"].(map[string]any)
	if in["path"] != "/etc/hosts" {
		t.Errorf("input = %v", in)
	}
	if tools[0]["id"] == nil || tools[0]["id"] == "" {
		t.Errorf("id missing: %v", tools[0])
	}
}

func TestHeuristicMultipleParameters(t *testing.T) {
	p := NewHeuristicToolParser()
	_, tools := p.Feed("● <function=write_file><parameter=path>/x</parameter><parameter=content>hello</parameter>")
	// 裁决: 跟随 TS 逐行端口 — 工具在 flush 时产出（bun 实证 TS 行为）。
	if len(tools) != 0 {
		t.Fatalf("tools = %d", len(tools))
	}
	_, tools = p.Flush()
	if len(tools) != 1 {
		t.Fatalf("flush tools = %d", len(tools))
	}
	in := tools[0]["input"].(map[string]any)
	if in["path"] != "/x" || in["content"] != "hello" {
		t.Errorf("input = %v", in)
	}
}

func TestHeuristicTextBeforeAndAfter(t *testing.T) {
	p := NewHeuristicToolParser()
	filtered, tools := p.Feed("prefix ● <function=read_file><parameter=path>/a</parameter> suffix")
	// 裁决: 跟随 TS 逐行端口 — 尾部文本 " suffix" 在 feed 内随工具一起输出，
	// 故 filtered 为 "prefix  suffix"（两个空格；bun 实证 TS 行为）。
	if filtered != "prefix  suffix" {
		t.Errorf("filtered = %q", filtered)
	}
	if len(tools) != 1 {
		t.Fatalf("tools = %d", len(tools))
	}
	flText, flTools := p.Flush()
	if flText != "" {
		t.Errorf("flush text = %q", flText)
	}
	if len(flTools) != 0 {
		t.Errorf("flush tools = %d", len(flTools))
	}
}

func TestHeuristicWebToolJSON(t *testing.T) {
	p := NewHeuristicToolParser()
	_, tools := p.Feed(`Sure! Use WebFetch {"url": "https://example.com"}`)
	if len(tools) != 1 {
		t.Fatalf("tools = %d", len(tools))
	}
	if tools[0]["name"] != "WebFetch" {
		t.Errorf("name = %v", tools[0]["name"])
	}
	// WebFetch 必须带 url
	_, tools2 := p.Feed(`Use WebSearch {"query": "test query"}`)
	if len(tools2) != 1 {
		t.Fatalf("tools2 = %d", len(tools2))
	}
	if tools2[0]["name"] != "WebSearch" {
		t.Errorf("name2 = %v", tools2[0]["name"])
	}
	_, tools3 := p.Feed(`Use WebFetch {"no":"url"}`)
	if len(tools3) != 0 {
		t.Errorf("WebFetch without url should not detect: %v", tools3)
	}
}

func TestControlTokenStrip(t *testing.T) {
	p := NewHeuristicToolParser()
	filtered, _ := p.Feed("<|begin_of_text|>hello")
	// 裁决: 跟随 TS 逐行端口 — TEXT 态文本留缓冲到 flush；feed1 剥离控制
	// 标记后 buffer="hello" 无输出（bun 实证 TS 行为）。
	if filtered != "" {
		t.Errorf("filtered = %q", filtered)
	}
	// 未完成控制标记尾巴留缓冲，其前缀随 feed2 输出（含 feed1 的 "hello"）
	filtered2, _ := p.Feed(" tail <|en")
	if filtered2 != "hello tail " {
		t.Errorf("filtered2 = %q", filtered2)
	}
	flText, _ := p.Flush()
	if flText != "<|en" {
		t.Errorf("flush = %q", flText)
	}
}

func TestHeuristicFunctionHeaderTooLong(t *testing.T) {
	p := NewHeuristicToolParser()
	longPrefix := "●"
	for i := 0; i < 110; i++ {
		longPrefix += "x"
	}
	filtered, tools := p.Feed(longPrefix)
	// 超过 100 字符仍未匹配 <function= → 逐字符转文本
	if filtered == "" {
		t.Error("should have drained to text")
	}
	if len(tools) != 0 {
		t.Errorf("tools = %d", len(tools))
	}
}

func TestHeuristicFlushPartialParameters(t *testing.T) {
	p := NewHeuristicToolParser()
	p.Feed("● <function=my_tool><parameter=key>val")
	_, tools := p.Flush()
	if len(tools) != 1 {
		t.Fatalf("flush tools = %d", len(tools))
	}
	in := tools[0]["input"].(map[string]any)
	if in["key"] != "val" {
		t.Errorf("input = %v", in)
	}
}

// --- Ported from heuristic_tool_parser.test.ts ---

func TestHeuristicTSPassThroughText(t *testing.T) {
	p := NewHeuristicToolParser()
	text, tools := p.Feed("Hello, world!")
	if len(tools) != 0 {
		t.Errorf("tools = %+v", tools)
	}
	flushText, flushTools := p.Flush()
	if len(flushTools) != 0 {
		t.Errorf("flush tools = %+v", flushTools)
	}
	// In streaming mode text may come out on feed or on flush — combined must be intact.
	if text+flushText != "Hello, world!" {
		t.Errorf("text = %q, flush = %q", text, flushText)
	}
}

func TestHeuristicTSFunctionWithParameters(t *testing.T) {
	p := NewHeuristicToolParser()
	// The tool call may be emitted on feed or on flush — combined must be one.
	_, t1 := p.Feed("● <function=read_file><parameter=path>/etc/hosts</parameter>")
	_, t2 := p.Flush()
	all := append(t1, t2...)
	if len(all) != 1 {
		t.Fatalf("tools = %d: %+v", len(all), all)
	}
	if all[0]["name"] != "read_file" {
		t.Errorf("name = %v", all[0]["name"])
	}
}

func TestHeuristicTSMultipleCalls(t *testing.T) {
	p := NewHeuristicToolParser()
	_, t1 := p.Feed("● <function=read_file><parameter=path>/a</parameter>")
	_, t2 := p.Feed("● <function=write_file>")
	_, t3 := p.Feed("<parameter=path>/b</parameter><parameter=content>hello</parameter>")
	_, t4 := p.Feed(" end")
	all := append(append(append(t1, t2...), t3...), t4...)
	if len(all) != 2 {
		t.Fatalf("tools = %d: %+v", len(all), all)
	}
	if all[0]["name"] != "read_file" {
		t.Errorf("tools[0].name = %v", all[0]["name"])
	}
	if all[1]["name"] != "write_file" {
		t.Errorf("tools[1].name = %v", all[1]["name"])
	}
	// 裁决补强: 跨 feed 参数附着（TS _currentParameters 跨 feed 累积）—
	// write_file 的参数来自 feed3（bun 实证 TS 行为一致）。
	in := all[1]["input"].(map[string]any)
	if in["path"] != "/b" || in["content"] != "hello" {
		t.Errorf("write_file input = %v", in)
	}
}

func TestHeuristicTSWebFetch(t *testing.T) {
	p := NewHeuristicToolParser()
	_, tools := p.Feed(`Use WebFetch {"url": "https://example.com"}`)
	if len(tools) != 1 {
		t.Fatalf("tools = %d", len(tools))
	}
	if tools[0]["name"] != "WebFetch" {
		t.Errorf("name = %v", tools[0]["name"])
	}
	in := tools[0]["input"].(map[string]any)
	if in["url"] != "https://example.com" {
		t.Errorf("input = %v", in)
	}
}

func TestHeuristicTSWebSearch(t *testing.T) {
	p := NewHeuristicToolParser()
	_, tools := p.Feed(`Use WebSearch {"query": "test query"}`)
	if len(tools) != 1 {
		t.Fatalf("tools = %d", len(tools))
	}
	if tools[0]["name"] != "WebSearch" {
		t.Errorf("name = %v", tools[0]["name"])
	}
	in := tools[0]["input"].(map[string]any)
	if in["query"] != "test query" {
		t.Errorf("input = %v", in)
	}
}

func TestHeuristicTSStripControlTokens(t *testing.T) {
	p := NewHeuristicToolParser()
	text, _ := p.Feed("hello <|special|> world")
	flushText, _ := p.Flush()
	if text+flushText != "hello  world" {
		t.Errorf("got %q + %q", text, flushText)
	}
}

func TestHeuristicTSEmptyFeed(t *testing.T) {
	p := NewHeuristicToolParser()
	text, tools := p.Feed("")
	if text != "" {
		t.Errorf("text = %q", text)
	}
	if len(tools) != 0 {
		t.Errorf("tools = %+v", tools)
	}
}

func TestHeuristicTSFlushPartialTool(t *testing.T) {
	p := NewHeuristicToolParser()
	// The tool may be emitted on feed or on flush — combined must be one.
	_, t1 := p.Feed("● <function=read_file><parameter=path>/tmp</parameter>")
	_, t2 := p.Flush()
	all := append(t1, t2...)
	if len(all) != 1 {
		t.Fatalf("tools = %d: %+v", len(all), all)
	}
	if all[0]["name"] != "read_file" {
		t.Errorf("name = %v", all[0]["name"])
	}
}

func TestHeuristicTSFlushEmptyTools(t *testing.T) {
	p := NewHeuristicToolParser()
	p.Feed("plain text")
	p.Flush()
	text, tools := p.Flush()
	if len(tools) != 0 {
		t.Errorf("tools = %+v", tools)
	}
	if text != "" {
		t.Errorf("text = %q", text)
	}
}

func TestHeuristicTSWebFetchNoURL(t *testing.T) {
	p := NewHeuristicToolParser()
	_, tools := p.Feed(`Use WebFetch {"query": "bad"}`)
	if len(tools) != 0 {
		t.Errorf("tools = %+v", tools)
	}
}

func TestHeuristicTSWebSearchNoQuery(t *testing.T) {
	p := NewHeuristicToolParser()
	_, tools := p.Feed(`Use WebSearch {"url": "bad"}`)
	if len(tools) != 0 {
		t.Errorf("tools = %+v", tools)
	}
}

func TestHeuristicTSTextBeforeAndToolsAfter(t *testing.T) {
	p := NewHeuristicToolParser()
	text, t1 := p.Feed("Hello ● <function=echo><parameter=msg>hi</parameter>")
	// Text before ● should be in feed output
	if text != "Hello " {
		t.Errorf("text = %q", text)
	}
	// Tool may be in feed or flush
	_, t2 := p.Flush()
	all := append(t1, t2...)
	if len(all) != 1 {
		t.Fatalf("tools = %d: %+v", len(all), all)
	}
	if all[0]["name"] != "echo" {
		t.Errorf("name = %v", all[0]["name"])
	}
}

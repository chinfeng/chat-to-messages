package parsers

import "testing"

// 上游 DeepSeek V4 的 DSML 工具调用信封偶尔会作为普通文本漏进
// content/reasoning_content（dumped 2026-10-23，newapi → deepseek-v4.1-flash）。
// 这里覆盖日志中实际出现的变体：全角单竖线、双竖线、ASCII 竖线、以及缺
// "tool_" 的 mangled 形式。
func TestStripDSMLResidueVariants(t *testing.T) {
	cases := []struct{ in, want string }{
		{"\n\n<｜DSML｜ calls>\n", ""},
		{"<｜DSML｜tool_calls>", ""},
		{"</｜DSML｜tool_calls>", ""},
		{"<｜｜DSML｜｜tool_calls>", ""},
		{"<|DSML|tool_calls>", ""},
		{"</|DSML| calls>", ""},
		{"before<｜DSML｜tool_calls>after", "beforeafter"},
		{`hello <｜DSML｜invoke name="Bash"> world`, "hello  world"},
		{"plain <think>text</think> no marker", "plain <think>text</think> no marker"},
		{"a < b", "a < b"},
	}
	for _, c := range cases {
		if got := StripDSMLResidue(c.in); got != c.want {
			t.Errorf("StripDSMLResidue(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestDSMLFilterSplitAcrossChunks(t *testing.T) {
	f := NewDSMLFilter()
	if got := f.Feed("\n\n<｜DSML｜ calls"); got != "\n\n" {
		t.Fatalf("feed1 = %q, want %q", got, "\n\n")
	}
	if got := f.Feed(">\n"); got != "" {
		t.Fatalf("feed2 = %q, want %q", got, "")
	}
	if got := f.Flush(); got != "" {
		t.Fatalf("flush = %q, want %q", got, "")
	}
}

func TestDSMLFilterInlineSplit(t *testing.T) {
	f := NewDSMLFilter()
	if got := f.Feed("hello <｜DSML｜"); got != "hello " {
		t.Fatalf("feed1 = %q", got)
	}
	if got := f.Feed("tool_calls> world"); got != " world" {
		t.Fatalf("feed2 = %q", got)
	}
	if got := f.Flush(); got != "" {
		t.Fatalf("flush = %q", got)
	}
}

func TestDSMLFilterFlushDropsIncompleteMarker(t *testing.T) {
	f := NewDSMLFilter()
	if got := f.Feed("ok <｜DSML｜ calls"); got != "ok " {
		t.Fatalf("feed = %q", got)
	}
	if got := f.Flush(); got != "" {
		t.Fatalf("flush = %q, want marker residue dropped", got)
	}
}

func TestDSMLFilterKeepsOrdinaryTextWithAngleBracket(t *testing.T) {
	f := NewDSMLFilter()
	if got := f.Feed("a < b"); got != "a < b" {
		t.Fatalf("feed = %q", got)
	}
	if got := f.Flush(); got != "" {
		t.Fatalf("flush = %q", got)
	}
}

func TestDSMLFilterDoubleBarSplit(t *testing.T) {
	f := NewDSMLFilter()
	if got := f.Feed("x <｜｜DS"); got != "x " {
		t.Fatalf("feed1 = %q", got)
	}
	if got := f.Feed("ML｜｜tool_calls>y"); got != "y" {
		t.Fatalf("feed2 = %q", got)
	}
	if got := f.Flush(); got != "" {
		t.Fatalf("flush = %q", got)
	}
}

package parsers

import (
	"strings"
	"testing"
)

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
	if len(chunks) != 3 {
		t.Fatalf("chunks = %d: %+v", len(chunks), chunks)
	}
	if chunks[0].Type != TextContent || chunks[0].Content != "Hello " {
		t.Errorf("c0 = %+v", chunks[0])
	}
	if chunks[1].Type != ThinkingContent || chunks[1].Content != "let me think" {
		t.Errorf("c1 = %+v", chunks[1])
	}
	if chunks[2].Type != TextContent || chunks[2].Content != " world" {
		t.Errorf("c2 = %+v", chunks[2])
	}
}

func TestThinkTagSplitAcrossChunks(t *testing.T) {
	p := NewThinkTagParser()
	// 裁决: 跟随 TS 逐行端口 — think 态内容随到随发，跨 chunk 的标签内容
	// 分块输出（bun 实证 TS 产出 2 个 THINKING chunk: "deep" 与 " thought"）。
	chunks := feedAll(t, p, "<th", "ink>", "deep", " thought", "</th", "ink>")
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d: %+v", len(chunks), chunks)
	}
	if chunks[0].Type != ThinkingContent || chunks[0].Content != "deep" {
		t.Errorf("c0 = %+v", chunks[0])
	}
	if chunks[1].Type != ThinkingContent || chunks[1].Content != " thought" {
		t.Errorf("c1 = %+v", chunks[1])
	}
}

func TestThinkTagOrphanClose(t *testing.T) {
	p := NewThinkTagParser()
	// 裁决: 跟随 TS 逐行端口 — orphan </think> 残余文本在 feed 内即输出
	// （bun 实证 TS: feed 产出 ["stray ", " tag"]，flush 为 nil）。
	chunks := feedAll(t, p, "stray </think> tag")
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d", len(chunks))
	}
	if chunks[0].Content != "stray " {
		t.Errorf("c0 = %+v", chunks[0])
	}
	if chunks[1].Content != " tag" {
		t.Errorf("c1 = %+v", chunks[1])
	}
	if fl := p.Flush(); fl != nil {
		t.Errorf("flush = %+v", fl)
	}
}

func TestThinkTagIncompleteTail(t *testing.T) {
	p := NewThinkTagParser()
	chunks := feedAll(t, p, "text before <th")
	if len(chunks) != 1 {
		t.Fatalf("chunks = %d", len(chunks))
	}
	if chunks[0].Content != "text before " {
		t.Errorf("c0 = %+v", chunks[0])
	}
	chunks = feedAll(t, p, "ink>inside</think>after")
	// 裁决: 跟随 TS 逐行端口 — 本 feed 恰好产出 [THINKING "inside",
	// TEXT "after"] 两块（bun 实证 TS 行为；brief 原期望 3 块不可满足）。
	if len(chunks) != 2 {
		t.Fatalf("chunks2 = %d: %+v", len(chunks), chunks)
	}
	if chunks[0].Type != ThinkingContent || chunks[0].Content != "inside" {
		t.Errorf("c0 = %+v", chunks[0])
	}
	if chunks[1].Content != "after" {
		t.Errorf("c1 = %+v", chunks[1])
	}
}

func TestThinkTagFlushInsideTag(t *testing.T) {
	p := NewThinkTagParser()
	// 裁决: 跟随 TS 逐行端口 — 无 close 标签时 think 内容在 feed 内即输出
	// （bun 实证 TS: feed 产出 [THINKING "unterminated"]，flush 为 nil）。
	chunks := feedAll(t, p, "<think>unterminated")
	if len(chunks) != 1 {
		t.Fatalf("chunks = %d: %+v", len(chunks), chunks)
	}
	if chunks[0].Type != ThinkingContent || chunks[0].Content != "unterminated" {
		t.Errorf("c0 = %+v", chunks[0])
	}
	if fl := p.Flush(); fl != nil {
		t.Errorf("flush = %+v", fl)
	}
}

func TestThinkTagEmptyContent(t *testing.T) {
	p := NewThinkTagParser()
	chunks := feedAll(t, p, "<think></think>")
	if len(chunks) != 0 {
		t.Errorf("chunks = %d", len(chunks))
	}
}

// --- Ported from think_tag_parser.test.ts ---

func TestThinkTagTSNoTags(t *testing.T) {
	p := NewThinkTagParser()
	chunks := feedAll(t, p, "Hello world")
	if len(chunks) != 1 || chunks[0].Type != TextContent || chunks[0].Content != "Hello world" {
		t.Fatalf("chunks = %+v", chunks)
	}
}

func TestThinkTagTSInsideAndAfter(t *testing.T) {
	p := NewThinkTagParser()
	chunks := feedAll(t, p, "<think>my thoughts</think>answer")
	var thinking, text strings.Builder
	for _, c := range chunks {
		switch c.Type {
		case ThinkingContent:
			thinking.WriteString(c.Content)
		case TextContent:
			text.WriteString(c.Content)
		}
	}
	if thinking.Len() == 0 || !strings.Contains(thinking.String(), "my thoughts") {
		t.Errorf("thinking = %q", thinking.String())
	}
	if text.Len() == 0 || !strings.Contains(text.String(), "answer") {
		t.Errorf("text = %q", text.String())
	}
}

func TestThinkTagTSStreaming(t *testing.T) {
	p := NewThinkTagParser()
	chunks := feedAll(t, p, "<think>thinking", " more</think>text")
	var all strings.Builder
	for _, c := range chunks {
		all.WriteString(c.Content)
	}
	for _, want := range []string{"thinking", "more", "text"} {
		if !strings.Contains(all.String(), want) {
			t.Errorf("missing %q in %q", want, all.String())
		}
	}
}

func TestThinkTagTSEmptyFeed(t *testing.T) {
	p := NewThinkTagParser()
	if chunks := feedAll(t, p, ""); len(chunks) != 0 {
		t.Fatalf("chunks = %+v", chunks)
	}
}

func TestThinkTagTSFlushInsidePartial(t *testing.T) {
	p := NewThinkTagParser()
	// Feed content that ends with a partial close tag — buffer stays inside think mode
	feedAll(t, p, "<think>partial</th")
	fl := p.Flush()
	if fl == nil || fl.Type != ThinkingContent {
		t.Fatalf("flush = %+v", fl)
	}
}

func TestThinkTagTSFlushOutsidePartial(t *testing.T) {
	p := NewThinkTagParser()
	// Feed partial open tag at end — text before it stays in buffer
	feedAll(t, p, "before <th")
	fl := p.Flush()
	if fl == nil || fl.Type != TextContent {
		t.Fatalf("flush = %+v", fl)
	}
}

func TestThinkTagTSOrphanClose(t *testing.T) {
	p := NewThinkTagParser()
	chunks := feedAll(t, p, "before</think>after")
	var all strings.Builder
	for _, c := range chunks {
		all.WriteString(c.Content)
	}
	if !strings.Contains(all.String(), "before") || !strings.Contains(all.String(), "after") {
		t.Errorf("all = %q", all.String())
	}
}

func TestThinkTagTSPartialOpenAtEnd(t *testing.T) {
	p := NewThinkTagParser()
	// TS: feed(`hello ${OPEN.slice(0,-1)}`) then feed(`>thinking</think>text`)
	chunks1 := feedAll(t, p, "hello <think")
	chunks2 := feedAll(t, p, ">thinking</think>text")
	var all strings.Builder
	for _, c := range chunks1 {
		all.WriteString(c.Content)
	}
	for _, c := range chunks2 {
		all.WriteString(c.Content)
	}
	for _, want := range []string{"hello", "thinking", "text"} {
		if !strings.Contains(all.String(), want) {
			t.Errorf("missing %q in %q", want, all.String())
		}
	}
}

func TestThinkTagTSFlushEmpty(t *testing.T) {
	p := NewThinkTagParser()
	if fl := p.Flush(); fl != nil {
		t.Errorf("flush = %+v", fl)
	}
}

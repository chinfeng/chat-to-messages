package convert

import (
	"encoding/json"
	"testing"

	"github.com/chinfeng/chat-to-messages/internal/anthropic"
)

func userImageMsg(data string) anthropic.Message {
	return anthropic.Message{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "image", Source: &anthropic.Source{Type: "base64", MediaType: "image/png", Data: data}},
	}}}
}

func countRemainingImages(messages []anthropic.Message) int {
	n := 0
	for i := range messages {
		n += countMessageImages(&messages[i])
	}
	return n
}

func TestEvictOldImagesKeepsMostRecent(t *testing.T) {
	messages := make([]anthropic.Message, 0, 10)
	for i := 0; i < 10; i++ {
		messages = append(messages, userImageMsg(string(rune('A'+i))))
	}
	evicted := EvictOldImages(messages, 7)
	if evicted != 3 {
		t.Fatalf("evicted = %d, want 3", evicted)
	}
	if n := countRemainingImages(messages); n != 7 {
		t.Fatalf("remaining images = %d, want 7", n)
	}
	// The earliest three became placeholders; the last message's image survives.
	if got := messages[0].Content.BlocksVal[0]; got.Type != "text" || got.Text != imageEvictedPlaceholder {
		t.Fatalf("messages[0] = %+v, want text placeholder", got)
	}
	if got := messages[9].Content.BlocksVal[0]; got.Type != "image" || got.Source.Data != "J" {
		t.Fatalf("messages[9] = %+v, want image J intact", got)
	}
}

func TestEvictOldImagesToolResult(t *testing.T) {
	content := json.RawMessage(`[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAA"}},{"type":"text","text":"hello"}]`)
	messages := []anthropic.Message{
		{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "tool_result", ToolUseID: "tu_1", Content: content},
		}}},
		userImageMsg("BBB"),
	}
	evicted := EvictOldImages(messages, 1)
	if evicted != 1 {
		t.Fatalf("evicted = %d, want 1", evicted)
	}
	// tool_use_id pairing untouched; the image entry became text inside the
	// same tool_result content array.
	blocks := messages[0].Content.BlocksVal
	if blocks[0].ToolUseID != "tu_1" || blocks[0].Type != "tool_result" {
		t.Fatalf("tool_result block altered: %+v", blocks[0])
	}
	var entries []map[string]any
	if err := json.Unmarshal(blocks[0].Content, &entries); err != nil {
		t.Fatal(err)
	}
	if entries[0]["type"] != "text" || entries[0]["text"] != imageEvictedPlaceholder {
		t.Fatalf("entry 0 = %v, want placeholder text", entries[0])
	}
	if entries[1]["type"] != "text" || entries[1]["text"] != "hello" {
		t.Fatalf("entry 1 = %v, want text hello intact", entries[1])
	}
	if n := countRemainingImages(messages); n != 1 {
		t.Fatalf("remaining images = %d, want 1", n)
	}
}

func TestEvictOldImagesDisabled(t *testing.T) {
	messages := []anthropic.Message{userImageMsg("A"), userImageMsg("B")}
	if evicted := EvictOldImages(messages, 0); evicted != 0 {
		t.Fatalf("evicted = %d, want 0 (keep <= 0 disables)", evicted)
	}
	if n := countRemainingImages(messages); n != 2 {
		t.Fatalf("remaining images = %d, want 2", n)
	}
}

func TestEvictOldImagesUnderLimit(t *testing.T) {
	messages := []anthropic.Message{userImageMsg("A")}
	if evicted := EvictOldImages(messages, 7); evicted != 0 {
		t.Fatalf("evicted = %d, want 0", evicted)
	}
	if got := messages[0].Content.BlocksVal[0]; got.Type != "image" {
		t.Fatalf("image altered under limit: %+v", got)
	}
}

func TestEnumerateImageRefsOrderAndKinds(t *testing.T) {
	toolResultContent := json.RawMessage(`[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"TR"}},{"type":"text","text":"hi"}]`)
	messages := []anthropic.Message{
		{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "text", Text: "see this:"},
			{Type: "image", Source: &anthropic.Source{Type: "base64", MediaType: "image/png", Data: "AAA"}},
		}}},
		{Role: "assistant", Content: anthropic.ContentValue{IsString: true, Str: "ok"}},
		{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "tool_result", ToolUseID: "tu_1", Content: toolResultContent},
			{Type: "document", Source: &anthropic.Source{Type: "base64", MediaType: "image/jpeg", Data: "DOC"}},
			{Type: "document", Source: &anthropic.Source{Type: "base64", MediaType: "application/pdf", Data: "PDF"}},
			{Type: "image", Source: &anthropic.Source{Type: "url", URL: "https://example.com/a.png"}},
		}}},
	}

	refs := EnumerateImageRefs(messages)
	if len(refs) != 4 {
		t.Fatalf("refs = %d, want 4 (user image, tool_result image, image document, url image)", len(refs))
	}
	// Document order: image (msg 0), tool_result image (msg 2), image document, url image.
	if got := refs[0].Message; got != 0 || refs[0].Entry != -1 {
		t.Fatalf("refs[0] = %+v, want msg 0 top-level", refs[0])
	}
	if got := refs[1].Message; got != 2 || refs[1].Entry != 0 {
		t.Fatalf("refs[1] = %+v, want msg 2 tool_result entry 0", refs[1])
	}
	if got := refs[2].Message; got != 2 || refs[2].Block != 1 || refs[2].Entry != -1 {
		t.Fatalf("refs[2] = %+v, want msg 2 block 1 (image document)", refs[2])
	}
	if url := partURL(refs[3].Part); url != "https://example.com/a.png" {
		t.Fatalf("refs[3] url = %q, want the url source", url)
	}
	// The PDF document is not an upstream-visible image and must be skipped.
	for _, ref := range refs {
		if ref.Message == 2 && ref.Block == 2 {
			t.Fatalf("non-image document enumerated: %+v", ref)
		}
	}
}

func TestReplaceImageRefToolResult(t *testing.T) {
	content := json.RawMessage(`[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAA"}},{"type":"text","text":"keep"}]`)
	messages := []anthropic.Message{
		{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "tool_result", ToolUseID: "tu_1", Content: content},
		}}},
	}
	refs := EnumerateImageRefs(messages)
	if len(refs) != 1 {
		t.Fatalf("refs = %d, want 1", len(refs))
	}
	ReplaceImageRef(messages, refs[0], "captioned!")

	block := messages[0].Content.BlocksVal[0]
	if block.ToolUseID != "tu_1" || block.Type != "tool_result" {
		t.Fatalf("tool_result block altered: %+v", block)
	}
	var entries []map[string]any
	if err := json.Unmarshal(block.Content, &entries); err != nil {
		t.Fatal(err)
	}
	if entries[0]["type"] != "text" || entries[0]["text"] != "captioned!" {
		t.Fatalf("entry 0 = %v, want captioned text", entries[0])
	}
	if entries[1]["type"] != "text" || entries[1]["text"] != "keep" {
		t.Fatalf("entry 1 = %v, want untouched text", entries[1])
	}
}

func TestReplaceImageRefBounds(t *testing.T) {
	messages := []anthropic.Message{userImageMsg("A")}
	// Out-of-range refs must be no-ops, not panics.
	ReplaceImageRef(messages, ImageRef{Message: 5, Block: 0, Entry: -1}, "x")
	ReplaceImageRef(messages, ImageRef{Message: 0, Block: 5, Entry: -1}, "x")
	ReplaceImageRef(messages, ImageRef{Message: 0, Block: 0, Entry: 3}, "x")
	if got := messages[0].Content.BlocksVal[0]; got.Type != "image" || got.Source.Data != "A" {
		t.Fatalf("image altered by out-of-range ref: %+v", got)
	}
}

func partURL(part map[string]any) string {
	obj, ok := part["image_url"].(map[string]any)
	if !ok {
		return ""
	}
	s, _ := obj["url"].(string)
	return s
}

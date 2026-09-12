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

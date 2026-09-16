package caption

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chinfeng/chat-to-messages/internal/anthropic"
	"github.com/chinfeng/chat-to-messages/internal/config"
	"github.com/chinfeng/chat-to-messages/internal/hook"
)

// visionServer returns a fake chat/completions endpoint that records the
// last request body it received and replies with the given body.
func visionServer(t *testing.T, reply string) (*httptest.Server, *atomic.Int32, *atomic.Value) {
	t.Helper()
	var received atomic.Value
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		var parsed map[string]any
		if err := json.Unmarshal(body, &parsed); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		received.Store(parsed)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, reply)
	}))
	return srv, &calls, &received
}

func testHook(cfg *config.Config, srv *httptest.Server, now func() time.Time) *Hook {
	h := New(cfg)
	h.logf = func(string, ...any) {} // silence
	if srv != nil {
		h.client.http = srv.Client()
		// Point the caption upstream at the test server.
		h.cfg = cfg
	}
	h.cache.now = now
	return h
}

func baseCfg() *config.Config {
	return &config.Config{
		UpstreamBaseURL:         "https://main.example.com/v1",
		UpstreamAPIKey:          "main-key",
		ImageCaptionModel:       "glm-4v",
		ImageCaptionCacheTTL:    time.Hour,
		HookImageCaptionPatterns: []string{"deepseek*", "kimi*"},
	}
}

func imgMsg(data string) anthropic.Message {
	return anthropic.Message{Role: "user", Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
		{Type: "image", Source: &anthropic.Source{Type: "base64", MediaType: "image/png", Data: data}},
	}}}
}

func TestHookAppliesByGlob(t *testing.T) {
	h := testHook(baseCfg(), nil, time.Now)
	if !h.Applies("deepseek-r1") || !h.Applies("kimi-k2") {
		t.Fatal("configured globs must apply")
	}
	if h.Applies("glm-4.6") {
		t.Fatal("glm-4.6 is visual and must not be captioned")
	}
}

func TestApplyReplacesImagesWithCaptions(t *testing.T) {
	srv, calls, received := visionServer(t, `{"choices":[{"message":{"content":"[1] a red square\n[2] a blue circle"}}]}`)
	defer srv.Close()
	cfg := baseCfg()
	cfg.ImageCaptionBaseURL = srv.URL
	h := testHook(cfg, srv, time.Now)

	messages := []anthropic.Message{imgMsg("AAA"), imgMsg("BBB")}
	err := h.Apply(context.Background(), &hook.Request{Model: "deepseek-r1", Messages: messages})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("vision calls = %d, want 1 (batched)", calls.Load())
	}
	// The batched call carried both images in one message.
	req := received.Load().(map[string]any)
	msgs := req["messages"].([]any)
	content := msgs[0].(map[string]any)["content"].([]any)
	if len(content) != 3 {
		t.Fatalf("content parts = %d, want 2 images + 1 prompt", len(content))
	}
	if req["model"] != "glm-4v" {
		t.Fatalf("caption model = %v, want glm-4v", req["model"])
	}
	if req["stream"] != false {
		t.Fatalf("caption call must be non-streaming")
	}

	for i, want := range []string{"[Image 1/2 (image/png)] a red square", "[Image 2/2 (image/png)] a blue circle"} {
		got := messages[i].Content.BlocksVal[0]
		if got.Type != "text" || got.Text != want {
			t.Fatalf("messages[%d] = %+v, want %q", i, got, want)
		}
	}
}

func TestApplyCacheReusesCaptions(t *testing.T) {
	srv, calls, _ := visionServer(t, `{"choices":[{"message":{"content":"[1] red square"}}]}`)
	defer srv.Close()
	cfg := baseCfg()
	cfg.ImageCaptionBaseURL = srv.URL
	h := testHook(cfg, srv, time.Now)

	var msgs []anthropic.Message
	for i := 0; i < 3; i++ {
		// Fresh copy each time; same image content.
		m := []anthropic.Message{imgMsg("AAA")}
		if err := h.Apply(context.Background(), &hook.Request{Model: "deepseek-r1", Messages: m}); err != nil {
			t.Fatal(err)
		}
		msgs = m
	}
	if calls.Load() != 1 {
		t.Fatalf("vision calls = %d, want 1 (cache must serve the rest)", calls.Load())
	}
	if got := msgs[0].Content.BlocksVal[0].Text; !strings.Contains(got, "red square") {
		t.Fatalf("cached caption missing: %q", got)
	}
}

func TestApplyCacheExpiry(t *testing.T) {
	srv, calls, _ := visionServer(t, `{"choices":[{"message":{"content":"[1] red square"}}]}`)
	defer srv.Close()
	cfg := baseCfg()
	cfg.ImageCaptionBaseURL = srv.URL
	now := time.Now()
	h := testHook(cfg, srv, func() time.Time { return now })

	m := []anthropic.Message{imgMsg("AAA")}
	h.Apply(context.Background(), &hook.Request{Model: "deepseek-r1", Messages: m})
	now = now.Add(2 * time.Hour) // TTL (1h) expires
	m = []anthropic.Message{imgMsg("AAA")}
	h.Apply(context.Background(), &hook.Request{Model: "deepseek-r1", Messages: m})
	if calls.Load() != 2 {
		t.Fatalf("vision calls = %d, want 2 after TTL expiry", calls.Load())
	}
}

func TestApplyParseFailureDegradesToPlaceholder(t *testing.T) {
	srv, calls, _ := visionServer(t, `{"choices":[{"message":{"content":"the model ignored instructions"}}]}`)
	defer srv.Close()
	cfg := baseCfg()
	cfg.ImageCaptionBaseURL = srv.URL
	h := testHook(cfg, srv, time.Now)

	m := []anthropic.Message{imgMsg("AAA")}
	if err := h.Apply(context.Background(), &hook.Request{Model: "deepseek-r1", Messages: m}); err != nil {
		t.Fatal(err)
	}
	got := m[0].Content.BlocksVal[0]
	if got.Type != "text" || got.Text != placeholder {
		t.Fatalf("got = %+v, want placeholder on parse failure", got)
	}
	if calls.Load() != 1 {
		t.Fatalf("vision call count = %d, want 1", calls.Load())
	}
}

func TestApplyUpstreamFailureDegradesToPlaceholder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"error":{"message":"bad key","type":"invalid_request_error"}}`)
	}))
	defer srv.Close()
	cfg := baseCfg()
	cfg.ImageCaptionBaseURL = srv.URL
	h := testHook(cfg, srv, time.Now)

	m := []anthropic.Message{imgMsg("AAA")}
	if err := h.Apply(context.Background(), &hook.Request{Model: "deepseek-r1", Messages: m}); err != nil {
		t.Fatalf("caption failure must not fail the request: %v", err)
	}
	if got := m[0].Content.BlocksVal[0]; got.Text != placeholder {
		t.Fatalf("got = %+v, want placeholder on upstream failure", got)
	}
}

func TestApplyURImageAndDocument(t *testing.T) {
	srv, calls, _ := visionServer(t, `{"choices":[{"message":{"content":"[1] url image\n[2] doc image"}}]}`)
	defer srv.Close()
	cfg := baseCfg()
	cfg.ImageCaptionBaseURL = srv.URL
	h := testHook(cfg, srv, time.Now)

	m := []anthropic.Message{{
		Role: "user",
		Content: anthropic.ContentValue{BlocksVal: []anthropic.ContentBlock{
			{Type: "image", Source: &anthropic.Source{Type: "url", URL: "https://example.com/a.png"}},
			{Type: "document", Source: &anthropic.Source{Type: "base64", MediaType: "image/jpeg", Data: "DOC"}},
			{Type: "document", Source: &anthropic.Source{Type: "base64", MediaType: "application/pdf", Data: "PDF"}},
		}},
	}}
	if err := h.Apply(context.Background(), &hook.Request{Model: "kimi-k2", Messages: m}); err != nil {
		t.Fatal(err)
	}
	blocks := m[0].Content.BlocksVal
	if blocks[0].Type != "text" || !strings.Contains(blocks[0].Text, "url image") {
		t.Fatalf("url image not captioned: %+v", blocks[0])
	}
	if blocks[1].Type != "text" || !strings.Contains(blocks[1].Text, "doc image") {
		t.Fatalf("image document not captioned: %+v", blocks[1])
	}
	// Non-image documents are untouched by the caption hook.
	if blocks[2].Type != "document" {
		t.Fatalf("PDF document altered: %+v", blocks[2])
	}
	if calls.Load() != 1 {
		t.Fatalf("vision calls = %d, want 1", calls.Load())
	}
}

func TestApplyNoImagesIsNoop(t *testing.T) {
	srv, calls, _ := visionServer(t, `{"choices":[{"message":{"content":"[1] x"}}]}`)
	defer srv.Close()
	cfg := baseCfg()
	cfg.ImageCaptionBaseURL = srv.URL
	h := testHook(cfg, srv, time.Now)

	m := []anthropic.Message{{Role: "user", Content: anthropic.ContentValue{IsString: true, Str: "just text"}}}
	if err := h.Apply(context.Background(), &hook.Request{Model: "deepseek-r1", Messages: m}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("vision called for an image-free request: %d", calls.Load())
	}
}

func TestParseCaptionsStrictness(t *testing.T) {
	cases := []struct {
		name    string
		reply   string
		n       int
		want    []string
		wantOk  bool
	}{
		{"happy", "[1] alpha\n[2] beta\n", 2, []string{"alpha", "beta"}, true},
		{"reordered", "[2] beta\n[1] alpha\n", 2, []string{"alpha", "beta"}, true},
		{"missing index", "[1] alpha", 2, nil, false},
		{"out of range", "[1] alpha\n[2] beta\n[3] gamma", 2, nil, false},
		{"empty caption", "[1] alpha\n[2] \n", 2, nil, false},
		{"no marker", "alpha\nbeta", 2, nil, false},
		{"duplicate index", "[1] alpha\n[1] beta\n[2] gamma", 2, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseCaptions(c.reply, c.n)
			if ok != c.wantOk {
				t.Fatalf("ok = %v, want %v (reply %q)", ok, c.wantOk, c.reply)
			}
			if ok && (len(got) != len(c.want) || got[0] != c.want[0] || got[len(got)-1] != c.want[len(c.want)-1]) {
				t.Fatalf("got = %v, want %v", got, c.want)
			}
		})
	}
}

func TestPartMime(t *testing.T) {
	if got := partMime(map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/png;base64,AAA"}}); got != "image/png" {
		t.Fatalf("data URI mime = %q", got)
	}
	if got := partMime(map[string]any{"type": "image_url", "image_url": map[string]any{"url": "https://x.com/a.JPG?x=1"}}); got != "image/jpg" {
		t.Fatalf("url mime = %q", got)
	}
	if got := partMime(map[string]any{"type": "image_url", "image_url": map[string]any{"url": "https://x.com/image"}}); got != "image" {
		t.Fatalf("fallback mime = %q", got)
	}
}

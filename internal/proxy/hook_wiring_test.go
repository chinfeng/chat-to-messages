package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chinfeng/chat-to-messages/internal/config"
)

// TestHandleMessagesCaptionHookWired asserts the caption hook is reached by
// the real handler chain: a request for a caption-globbed model must carry
// the captioned text upstream, and the vision model must have been called.
func TestHandleMessagesCaptionHookWired(t *testing.T) {
	var visionCalls int
	var visionBody string
	vision := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		visionCalls++
		b, _ := io.ReadAll(r.Body)
		visionBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"[1] a diagram of the system"}}]}`)
	}))
	defer vision.Close()

	var upstreamBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		upstreamBody = string(b)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	cfg := &config.Config{
		UpstreamBaseURL:          upstream.URL,
		UpstreamAPIKey:           "k",
		MaxUpstreamImages:        7,
		HookImageCaptionPatterns: []string{"deepseek*"},
		ImageCaptionModel:        "glm-4v",
		ImageCaptionBaseURL:      vision.URL,
		ImageCaptionCacheTTL:     3600000000000,
	}
	h := NewHandler(cfg)

	body := `{"model":"deepseek-r1","max_tokens":1024,"messages":[{"role":"user","content":[` +
		`{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}},` +
		`{"type":"text","text":"what is this?"}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	req.Header.Set("x-api-key", "k")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if visionCalls != 1 {
		t.Fatalf("vision calls = %d, want 1", visionCalls)
	}
	if !strings.Contains(visionBody, `"model":"glm-4v"`) {
		t.Fatalf("vision call model missing: %s", visionBody)
	}
	if !strings.Contains(upstreamBody, "[Image 1/1 (image/png)] a diagram of the system") {
		t.Fatalf("upstream body missing caption: %s", upstreamBody)
	}
	if strings.Contains(upstreamBody, "image_url") {
		t.Fatalf("upstream body still carries image_url: %s", upstreamBody)
	}
}

// TestHandleMessagesCaptionHookSkippedForVisualModel asserts the hook does not
// touch a visual model sharing the upstream.
func TestHandleMessagesCaptionHookSkippedForVisualModel(t *testing.T) {
	var upstreamBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		upstreamBody = string(b)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	cfg := &config.Config{
		UpstreamBaseURL:          upstream.URL,
		UpstreamAPIKey:           "k",
		MaxUpstreamImages:        7,
		HookImageCaptionPatterns: []string{"deepseek*"},
		ImageCaptionModel:        "glm-4v",
	}
	h := NewHandler(cfg)

	body := `{"model":"glm-4.6","max_tokens":1024,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	req.Header.Set("x-api-key", "k")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(upstreamBody, "image_url") {
		t.Fatalf("visual model request lost its image: %s", upstreamBody)
	}
}

// TestHandleMessagesEvictionStillWired asserts the eviction hook still runs
// after it moved behind the registry.
func TestHandleMessagesEvictionStillWired(t *testing.T) {
	var upstreamBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		upstreamBody = string(b)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	cfg := &config.Config{
		UpstreamBaseURL:   upstream.URL,
		UpstreamAPIKey:    "k",
		MaxUpstreamImages: 1,
	}
	h := NewHandler(cfg)

	// Two images, cap 1: the oldest must become a placeholder.
	var images []string
	for i := 0; i < 2; i++ {
		images = append(images, `{"type":"image","source":{"type":"base64","media_type":"image/png","data":"IMG`+string(rune('A'+i))+`"}}`)
	}
	body := `{"model":"glm-4.6","max_tokens":1024,"messages":[{"role":"user","content":[` + strings.Join(images, ",") + `]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	req.Header.Set("x-api-key", "k")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(upstreamBody, "[Media removed from earlier context to reduce request size]") {
		t.Fatalf("eviction placeholder missing upstream: %s", upstreamBody)
	}
}

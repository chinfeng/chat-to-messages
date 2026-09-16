package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

// TestHandleResponsesCaptionHookWired asserts the caption hook also covers
// /v1/responses: an input_image part for a caption-globbed model must reach the
// upstream as captioned text, and the vision model must have been called once.
func TestHandleResponsesCaptionHookWired(t *testing.T) {
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

	body := `{"model":"deepseek-r1","input":[{"type":"message","role":"user","content":[` +
		`{"type":"input_image","image_url":"data:image/png;base64,AAAA"},` +
		`{"type":"input_text","text":"what is this?"}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
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

// TestHandleResponsesCaptionHookSkippedForVisualModel asserts the Responses path
// leaves a visual model's images intact.
func TestHandleResponsesCaptionHookSkippedForVisualModel(t *testing.T) {
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

	body := `{"model":"glm-4.6","input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
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

// TestHandleResponsesCaptionHookChainedFromStore asserts the hook covers the
// previous_response_id-expanded history: the store keeps the ORIGINAL image
// (not the caption), so turn 2 re-captions it — from the cache, so no second
// vision call — and the caption still reaches the upstream.
func TestHandleResponsesCaptionHookChainedFromStore(t *testing.T) {
	var visionCalls int
	vision := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		visionCalls++
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"[1] a diagram of the system"}}]}`)
	}))
	defer vision.Close()

	upstream := &sseUpstreamScript{script: []string{textTurnSSE, textTurnSSE}}
	up := httptest.NewServer(upstream.handler())
	defer up.Close()

	cfg := &config.Config{
		UpstreamBaseURL:          up.URL,
		UpstreamAPIKey:           "k",
		ResponsesStoreTTLMinutes: 1440,
		MaxUpstreamImages:        7,
		HookImageCaptionPatterns: []string{"deepseek*"},
		ImageCaptionModel:        "glm-4v",
		ImageCaptionBaseURL:      vision.URL,
		ImageCaptionCacheTTL:     time.Hour,
	}
	h := NewHandler(cfg)

	// Turn 1: image in. The hook captions it; the store keeps the original.
	turn1 := `{"model":"deepseek-r1","input":[{"type":"message","role":"user","content":[` +
		`{"type":"input_image","image_url":"data:image/png;base64,AAAA"},` +
		`{"type":"input_text","text":"describe this"}]}]}`
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(turn1)))
	if rec.Code != http.StatusOK {
		t.Fatalf("turn 1 status = %d, want 200", rec.Code)
	}
	if visionCalls != 1 {
		t.Fatalf("turn 1 vision calls = %d, want 1", visionCalls)
	}
	var final1 map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &final1); err != nil {
		t.Fatalf("turn 1 must be JSON: %v", err)
	}
	respID, _ := final1["id"].(string)
	if !strings.HasPrefix(respID, "resp_") {
		t.Fatalf("turn 1 missing response id: %v", final1)
	}

	// Turn 2: text only, but the stored turn still carries the image.
	turn2 := `{"model":"deepseek-r1","previous_response_id":"` + respID + `",` +
		`"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"again, what was it?"}]}]}`
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(turn2)))
	if rec2.Code != http.StatusOK {
		t.Fatalf("turn 2 status = %d, want 200", rec2.Code)
	}
	if visionCalls != 1 {
		t.Fatalf("turn 2 vision calls = %d, want 1 (cache hit)", visionCalls)
	}
	body2, _ := json.Marshal(upstream.lastBody(t))
	if !strings.Contains(string(body2), "[Image 1/1 (image/png)] a diagram of the system") {
		t.Fatalf("turn 2 upstream body missing the stored image's caption: %s", body2)
	}
	if strings.Contains(string(body2), "image_url") {
		t.Fatalf("turn 2 upstream body still carries image_url: %s", body2)
	}
}

// TestHandleResponsesWSCaptionHookWired asserts the websocket transport gets
// the same treatment: runResponsesWSJob shares buildResponsesUpstream, so an
// input_image in a response.create frame is captioned before conversion.
func TestHandleResponsesWSCaptionHookWired(t *testing.T) {
	var visionCalls int
	vision := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		visionCalls++
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"[1] a diagram of the system"}}]}`)
	}))
	defer vision.Close()

	upstream := &sseUpstreamScript{script: []string{textTurnSSE}}
	up := httptest.NewServer(upstream.handler())
	defer up.Close()

	cfg := &config.Config{
		UpstreamBaseURL:          up.URL,
		UpstreamAPIKey:           "k",
		MaxUpstreamImages:        7,
		HookImageCaptionPatterns: []string{"deepseek*"},
		ImageCaptionModel:        "glm-4v",
		ImageCaptionBaseURL:      vision.URL,
		ImageCaptionCacheTTL:     time.Hour,
	}
	proxy := httptest.NewServer(NewHandler(cfg))
	defer proxy.Close()

	c := dialProxyWS(t, proxy.URL)
	defer c.conn.Close()

	c.send(t, map[string]any{
		"type":  "response.create",
		"model": "deepseek-r1",
		"input": []map[string]any{{
			"type": "message", "role": "user",
			"content": []map[string]any{
				{"type": "input_image", "image_url": "data:image/png;base64,AAAA"},
				{"type": "input_text", "text": "what is this?"},
			},
		}},
	})
	events := c.collectEvents(t)
	if !wsHasEvent(events, "response.completed") {
		t.Fatalf("turn did not complete: %v", wsEventTypes(events))
	}
	if visionCalls != 1 {
		t.Fatalf("vision calls = %d, want 1", visionCalls)
	}
	body, _ := json.Marshal(upstream.lastBody(t))
	if !strings.Contains(string(body), "[Image 1/1 (image/png)] a diagram of the system") {
		t.Fatalf("upstream body missing caption: %s", body)
	}
	if strings.Contains(string(body), "image_url") {
		t.Fatalf("upstream body still carries image_url: %s", body)
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

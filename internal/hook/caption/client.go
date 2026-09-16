package caption

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/chinfeng/chat-to-messages/internal/config"
	"github.com/chinfeng/chat-to-messages/internal/openai"
)

// captionMaxTokens bounds the caption reply: one line per image, <= 80 words
// each — a description, not an essay.
const captionMaxTokens = 512

// captionPrompt asks the vision model for exactly N numbered lines so the
// reply can be split back into per-image captions. The framing ("a text-only
// model cannot see these") steers the description toward content a language
// model can act on: depicted objects, visible text, layout.
func captionPrompt(n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "You are captioning images for a text-only language model that cannot see them. "+
		"For each of the %d images provided in order, reply with exactly %d lines. ", n, n)
	b.WriteString("Line i starts with \"[i]\" followed by a concise factual description of image i: what it depicts, " +
		"any text or labels visible, layout and spatial relationships, and anything else needed to understand it " +
		"in context. Keep each line under 80 words. Output the lines only, nothing else.")
	return b.String()
}

// client calls the vision model over the OpenAI chat/completions dialect.
type client struct {
	http *http.Client
}

func newClient() *client {
	// No timeout: the request context bounds the call and cancels it when
	// the downstream client disconnects, mirroring the main upstream call.
	return &client{http: &http.Client{}}
}

// chatResponse is the subset of the non-streaming chat/completions reply this
// hook reads.
type chatResponse struct {
	Choices []struct {
		Message struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *openai.Error `json:"error,omitempty"`
}

// caption sends every part to the vision model in one batched call and
// returns the raw reply text. Failures (transport, non-200, upstream error,
// empty content) return an error — the hook degrades to placeholders rather
// than failing the user's request.
func (c *client) caption(ctx context.Context, cfg *config.Config, parts []map[string]any) (string, error) {
	base := cfg.ImageCaptionBaseURL
	if base == "" {
		base = cfg.UpstreamBaseURL
	}
	apiKey := cfg.ImageCaptionAPIKey
	if apiKey == "" {
		apiKey = cfg.UpstreamAPIKey
	}

	content := make([]map[string]any, 0, len(parts)+1)
	for _, p := range parts {
		content = append(content, p)
	}
	content = append(content, map[string]any{"type": "text", "text": captionPrompt(len(parts))})

	body := map[string]any{
		"model":       cfg.ImageCaptionModel,
		"messages":    []map[string]any{{"role": "user", "content": content}},
		"stream":      false,
		"temperature": 0,
		"max_tokens":  captionMaxTokens,
	}
	wire, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	url := strings.TrimRight(base, "/") + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(wire))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		var parsed chatResponse
		if json.Unmarshal(raw, &parsed) == nil && parsed.Error != nil {
			return "", fmt.Errorf("caption upstream %d: %s", resp.StatusCode, parsed.Error.Message)
		}
		return "", fmt.Errorf("caption upstream status %d", resp.StatusCode)
	}

	var parsed chatResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", err
	}
	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("caption reply has no choices")
	}
	return contentString(parsed.Choices[0].Message.Content)
}

// contentString extracts the assistant text from a choice's content field,
// which may be a plain string or a list of parts.
func contentString(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return "", fmt.Errorf("caption reply has empty content")
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return "", err
		}
		return s, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(trimmed, &parts); err != nil {
		return "", err
	}
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(p.Text)
	}
	return b.String(), nil
}

var captionLineRe = regexp.MustCompile(`^\[(\d{1,3})\]\s*(.*)$`)

// parseCaptions splits a numbered reply into one caption per image. Every
// image must receive a non-empty caption: a reply missing any index, or
// carrying extra/unordered indices, is treated as a failure so the hook
// degrades all images to placeholders together rather than giving the model a
// partial view.
func parseCaptions(reply string, n int) ([]string, bool) {
	out := make([]string, n)
	seen := 0
	for _, line := range strings.Split(reply, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		m := captionLineRe.FindStringSubmatch(line)
		if m == nil {
			return nil, false
		}
		idx, err := strconv.Atoi(m[1])
		if err != nil || idx < 1 || idx > n {
			return nil, false
		}
		text := strings.TrimSpace(m[2])
		if text == "" || out[idx-1] != "" {
			return nil, false
		}
		out[idx-1] = text
		seen++
	}
	if seen != n {
		return nil, false
	}
	return out, true
}

// partURL extracts the url of an OpenAI image_url content part.
func partURL(part map[string]any) string {
	obj, ok := part["image_url"].(map[string]any)
	if !ok {
		return ""
	}
	s, _ := obj["url"].(string)
	return s
}

// partMime extracts a best-effort MIME type from an image_url part: the
// data URI prefix for base64 images, the URL's file extension otherwise.
func partMime(part map[string]any) string {
	u := partURL(part)
	if s := strings.TrimPrefix(u, "data:"); s != u {
		if i := strings.Index(s, ";"); i > 0 {
			return s[:i]
		}
		return s
	}
	// Only the final path segment can carry an extension.
	seg := u
	if i := strings.LastIndex(u, "/"); i >= 0 {
		seg = u[i+1:]
	}
	if i := strings.LastIndex(seg, "."); i > 0 {
		ext := strings.ToLower(seg[i+1:])
		if j := strings.IndexAny(ext, "?#"); j >= 0 {
			ext = ext[:j]
		}
		if ext != "" {
			return "image/" + ext
		}
	}
	return "image"
}

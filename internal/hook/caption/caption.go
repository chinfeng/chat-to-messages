// Package caption implements the image-caption request hook: for models the
// operator rules non-visual, every image in the request is replaced with a
// text caption produced by a vision model before the upstream request is
// built, so a text-only upstream model still receives the image's content.
//
// The hook never fails the downstream request: a captioning failure degrades
// to the same placeholder text the eviction hook uses.
//
// Both downstream dialects are covered: Apply handles Anthropic Messages
// requests via the hook registry, CaptionItems handles OpenAI Responses input
// items (the Responses path cannot share the registry because its request
// shape is not Anthropic-canonical).
package caption

import (
	"context"
	"fmt"

	"github.com/chinfeng/chat-to-messages/internal/config"
	"github.com/chinfeng/chat-to-messages/internal/convert"
	"github.com/chinfeng/chat-to-messages/internal/hook"
	"github.com/chinfeng/chat-to-messages/internal/responses"
)

// placeholder mirrors the eviction hook's fallback so a captioning failure
// reads identically to a capped/evicted image.
const placeholder = "[Media removed from earlier context to reduce request size]"

// Hook captions images for models matching the operator's glob rules.
type Hook struct {
	patterns []string
	cfg      *config.Config
	client   *client
	cache    *cache
	logf     func(format string, args ...any)
}

// New builds a caption hook from cfg. The hook is inert when
// cfg.HookImageCaptionPatterns is empty — the registry simply never calls it.
func New(cfg *config.Config) *Hook {
	return &Hook{
		patterns: cfg.HookImageCaptionPatterns,
		cfg:      cfg,
		client:   newClient(),
		cache:    newCache(cfg.ImageCaptionCacheTTL),
		logf: func(format string, args ...any) {
			fmt.Printf("  [image-caption] "+format+"\n", args...)
		},
	}
}

// Name is "image-caption" (--hook-image-caption).
func (h *Hook) Name() string { return "image-caption" }

// Applies is true when the model matches any configured glob.
func (h *Hook) Applies(model string) bool {
	return hook.AnyMatch(h.patterns, model)
}

// Apply replaces every upstream-visible image in an Anthropic Messages request
// with a captioned text block: user image blocks, base64 image documents, and
// images nested in tool_result content (which the converter later moves to a
// synthetic user turn). Non-image documents are left to the converter's
// existing handling.
func (h *Hook) Apply(ctx context.Context, req *hook.Request) error {
	refs := convert.EnumerateImageRefs(req.Messages)
	if len(refs) == 0 {
		return nil
	}
	parts := make([]map[string]any, len(refs))
	for i, ref := range refs {
		parts[i] = ref.Part
	}
	captions := h.captionParts(ctx, parts)
	for i, ref := range refs {
		text := captionText(captions[i], i+1, len(refs), partMime(ref.Part))
		if captions[i] == "" {
			h.logf("model %s: caption failed for image %d/%d, using placeholder", req.Model, i+1, len(refs))
		}
		convert.ReplaceImageRef(req.Messages, ref, text)
	}
	return nil
}

// CaptionItems replaces every input_image part in an OpenAI Responses input
// item list with a captioned input_text part, in place. It covers both the
// current request's items and the previous_response_id-expanded history:
// buildResponsesUpstream runs it on the fully assembled history slice, before
// BuildChatBody turns the items into chat messages. Items without image parts
// are untouched; a caption failure degrades to the placeholder text.
//
// The store keeps the ORIGINAL items (with images), so a later turn
// re-captions from cache — or, if the operator later rules the model visual,
// replays the image untouched.
func (h *Hook) CaptionItems(ctx context.Context, model string, items []responses.Item) error {
	if !h.Applies(model) || len(items) == 0 {
		return nil
	}
	type imgPos struct {
		itemIdx int
		partIdx int
	}
	var positions []imgPos
	var parts []map[string]any
	for i := range items {
		it := &items[i]
		if it.Content.IsString {
			continue
		}
		for j := range it.Content.Parts {
			p := &it.Content.Parts[j]
			if p.Type == "input_image" && p.ImageURL != "" {
				img := map[string]any{"url": p.ImageURL}
				if p.Detail != "" {
					img["detail"] = p.Detail
				}
				positions = append(positions, imgPos{i, j})
				parts = append(parts, map[string]any{"type": "image_url", "image_url": img})
			}
		}
	}
	if len(positions) == 0 {
		return nil
	}

	captions := h.captionParts(ctx, parts)
	for k, pos := range positions {
		text := captionText(captions[k], k+1, len(positions), partMime(parts[k]))
		if captions[k] == "" {
			h.logf("model %s: caption failed for image %d/%d, using placeholder", model, k+1, len(positions))
		}
		items[pos.itemIdx].Content.Parts[pos.partIdx] = responses.ContentPart{
			Type: "input_text",
			Text: text,
		}
	}
	return nil
}

// captionText formats a caption into the replacement text block, or the
// placeholder when captioning failed.
func captionText(caption string, idx, total int, mime string) string {
	if caption == "" {
		return placeholder
	}
	return fmt.Sprintf("[Image %d/%d (%s)] %s", idx, total, mime, caption)
}

// captionParts returns one caption per OpenAI image_url part, "" for each
// image that could not be captioned. Cache misses are captioned in a single
// batched vision call; hits come from the cache.
func (h *Hook) captionParts(ctx context.Context, parts []map[string]any) []string {
	out := make([]string, len(parts))
	var missIdx []int
	var missParts []map[string]any
	for i, p := range parts {
		if cap, ok := h.cache.get(cacheKey(p)); ok {
			out[i] = cap
			continue
		}
		missIdx = append(missIdx, i)
		missParts = append(missParts, p)
	}
	if len(missParts) == 0 {
		return out
	}

	reply, err := h.client.caption(ctx, h.cfg, missParts)
	if err != nil {
		h.logf("vision call failed: %v", err)
		return out
	}
	parsed, ok := parseCaptions(reply, len(missParts))
	if !ok {
		h.logf("vision reply did not parse as %d numbered lines; reply: %q", len(missParts), truncate(reply, 200))
		return out
	}
	for j, idx := range missIdx {
		out[idx] = parsed[j]
		h.cache.set(cacheKey(missParts[j]), parsed[j])
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// compile-time guard: the hook must satisfy the registry interface.
var _ hook.Hook = (*Hook)(nil)

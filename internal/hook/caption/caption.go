// Package caption implements the image-caption request hook: for models the
// operator rules non-visual, every image in the request is replaced with a
// text caption produced by a vision model before the upstream request is
// built, so a text-only upstream model still receives the image's content.
//
// The hook never fails the downstream request: a captioning failure degrades
// to the same placeholder text the eviction hook uses.
package caption

import (
	"context"
	"fmt"

	"github.com/chinfeng/chat-to-messages/internal/config"
	"github.com/chinfeng/chat-to-messages/internal/convert"
	"github.com/chinfeng/chat-to-messages/internal/hook"
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

// Apply replaces every upstream-visible image in the request with a captioned
// text block: user image blocks, base64 image documents, and images nested in
// tool_result content (which the converter later moves to a synthetic user
// turn). Non-image documents are left to the converter's existing handling.
func (h *Hook) Apply(ctx context.Context, req *hook.Request) error {
	refs := convert.EnumerateImageRefs(req.Messages)
	if len(refs) == 0 {
		return nil
	}

	captions := h.captions(ctx, refs)
	for i, ref := range refs {
		text := captions[i]
		if text == "" {
			// Batch failure or a vision reply that did not parse: degrade
			// every image to the placeholder so the model gets a coherent
			// (if empty) view rather than half-captioned context.
			text = placeholder
			h.logf("model %s: caption failed for image %d/%d, using placeholder", req.Model, i+1, len(refs))
		} else {
			text = fmt.Sprintf("[Image %d/%d (%s)] %s", i+1, len(refs), partMime(ref.Part), text)
		}
		convert.ReplaceImageRef(req.Messages, ref, text)
	}
	return nil
}

// captions returns one caption per ref, "" for each image that could not be
// captioned. Misses are captioned in a single batched vision call; hits come
// from the cache.
func (h *Hook) captions(ctx context.Context, refs []convert.ImageRef) []string {
	out := make([]string, len(refs))
	var missIdx []int
	var missParts []map[string]any
	for i, ref := range refs {
		if cap, ok := h.cache.get(cacheKey(ref.Part)); ok {
			out[i] = cap
			continue
		}
		missIdx = append(missIdx, i)
		missParts = append(missParts, ref.Part)
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

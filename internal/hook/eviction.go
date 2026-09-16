package hook

import (
	"context"

	"github.com/chinfeng/chat-to-messages/internal/convert"
)

// EvictionHook caps the images sent upstream per request, replacing the
// oldest (document order) with text placeholders. It applies to every model;
// its cap comes from --max-upstream-images. This is the logic
// convert.EvictOldImages has always applied at this point in the pipeline —
// registered as a hook so every request transformation shares one entry
// point.
type EvictionHook struct {
	Keep int
}

// Name is "image-eviction".
func (h *EvictionHook) Name() string { return "image-eviction" }

// Applies is always true: image caps are a property of the upstream channel,
// not of the model.
func (h *EvictionHook) Applies(string) bool { return true }

// Apply runs convert.EvictOldImages; keep <= 0 disables it.
func (h *EvictionHook) Apply(_ context.Context, req *Request) error {
	convert.EvictOldImages(req.Messages, h.Keep)
	return nil
}

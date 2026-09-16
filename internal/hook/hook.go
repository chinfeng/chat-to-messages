// Package hook implements request hooks: transformations applied to a decoded
// downstream request before the upstream request is built, each gated by a
// model glob rule. The gateway never reasons about model capabilities — the
// operator decides which models a hook applies to via its --hook-<name> flag.
//
// Not to be confused with the stream hooks in internal/stream, which are
// mid-response retry callbacks on a completely different axis.
package hook

import (
	"context"

	"github.com/chinfeng/chat-to-messages/internal/anthropic"
	"github.com/chinfeng/chat-to-messages/internal/config"
)

// Request is the canonical view of a decoded downstream request handed to
// request hooks. Messages are mutated in place, before conversion.
type Request struct {
	Model    string
	Messages []anthropic.Message
}

// Hook is a named request transformation.
type Hook interface {
	// Name is the hook's unique id, matching its config flag
	// (--hook-<name>).
	Name() string
	// Applies reports whether this hook should run for model.
	Applies(model string) bool
	// Apply transforms req.Messages in place. A returned error fails the
	// downstream request; hooks that degrade gracefully (caption) never
	// return one.
	Apply(ctx context.Context, req *Request) error
}

// Registry runs its hooks in registration order. Eviction registers first so
// that image-removing hooks run before the caption hook sees the request.
type Registry struct {
	hooks []Hook
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register appends a hook.
func (r *Registry) Register(h Hook) {
	r.hooks = append(r.hooks, h)
}

// Apply runs every hook whose rule matches req.Model, in registration order.
func (r *Registry) Apply(ctx context.Context, req *Request) error {
	for _, h := range r.hooks {
		if !h.Applies(req.Model) {
			continue
		}
		if err := h.Apply(ctx, req); err != nil {
			return err
		}
	}
	return nil
}

// AnyMatch reports whether model matches any of the glob patterns (first
// match wins in the sense that the first hit short-circuits). An empty
// pattern list matches nothing.
func AnyMatch(patterns []string, model string) bool {
	for _, p := range patterns {
		if config.GlobMatch(p, model) {
			return true
		}
	}
	return false
}

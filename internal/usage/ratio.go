// Package usage maintains the in-memory per-model tokenizer ratio that
// corrects the char/4 input estimate toward the upstream's real token counts.
//
// Background: message_start must carry an input_tokens figure before any
// upstream usage exists, so it reports convert.EstimateInputTokens (char/4).
// Completed turns report the upstream's real prompt_tokens in message_delta —
// and the upstream tokenizer is denser than char/4 assumes (measured
// 2026-10-18 on z-ai/glm-5.3-flash: 163,049 real vs 106,228 estimated, 1.54×;
// every other upstream tokenizer needs its own measurement). The ratio learned
// from completed turns scales future message_start estimates into the same
// unit as message_delta, so the two stop disagreeing by the tokenizer gap.
//
// Ratio state is deliberately in-memory only (user ruling 2026-10-18): it
// resets to 1.0 on every restart and re-converges after one successful turn
// per model. Nothing here is persisted or configurable.
package usage

import "sync"

// Default is the process-wide tracker.
var Default = New()

const (
	// minEstimateTokens floors observations: below this the upstream's fixed
	// chat-template and system/tools overhead dominates the estimate (which
	// counts message content only) and the observed ratio is garbage.
	// ponytail: raise if small-request ratios prove noisy in practice.
	minEstimateTokens = 1000
	// Ratio clamp: observations outside the range are dropped as garbage
	// (mismatched units, relay bugs). Real ratios measured so far: ~0.65–1.6.
	minRatio = 0.25
	maxRatio = 5.0
)

// Ratio tracks upstream prompt_tokens ÷ estimated tokens per model.
type Ratio struct {
	mu  sync.Mutex
	obs map[string]float64
}

// New creates an empty tracker: every model starts at ratio 1.0.
func New() *Ratio { return &Ratio{obs: map[string]float64{}} }

// Get returns the current ratio for a model, 1.0 before any observation.
func (r *Ratio) Get(model string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ratio, ok := r.obs[model]; ok {
		return ratio
	}
	return 1.0
}

// Scaled returns estimate multiplied by the model's learned ratio.
func (r *Ratio) Scaled(model string, estimate int64) int64 {
	if estimate <= 0 {
		return estimate
	}
	return int64(float64(estimate)*r.Get(model) + 0.5)
}

// Observe folds one successfully completed turn into the ratio: estimate is
// the char/4 input estimate for the request, promptTokens the upstream's real
// prompt_tokens for the same request. The first observation sets the ratio
// directly; later ones blend 50/50 (the per-turn value varies with content
// mix; the blend damps that jitter without adding state).
func (r *Ratio) Observe(model string, estimate, promptTokens int64) {
	if estimate < minEstimateTokens || promptTokens <= 0 {
		return
	}
	obs := float64(promptTokens) / float64(estimate)
	if obs < minRatio || obs > maxRatio {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ratio, ok := r.obs[model]; ok {
		r.obs[model] = (ratio + obs) / 2
		return
	}
	r.obs[model] = obs
}

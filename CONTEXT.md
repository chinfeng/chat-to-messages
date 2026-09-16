# chat-to-messages

A Go (stdlib-only) API-dialect proxy: clients speak Anthropic Messages or OpenAI Responses; the proxy speaks OpenAI chat/completions to the actual provider.

## Language

**Upstream**:
The provider side the proxy calls. Speaks OpenAI-flavored `/v1/chat/completions`. Configured via CLI flags (single upstream on master; `feat/multi-upstream-routing` pending).
_Avoid_: 下游, backend, target API

**Downstream**:
The client side that calls the proxy. Two dialects: Anthropic Messages on `/v1/messages`, OpenAI Responses on `/v1/responses`.
_Avoid_: upstream, caller, 上游

**Response store**:
Proxy-side persistence of a response's input/output items, so a downstream `previous_response_id` can be expanded into full history for a stateless upstream. Owned by the proxy, never the upstream.
_Avoid_: session cache, conversation db

**Request hook**:
A transformation applied to a decoded downstream request before the upstream request is built, gated by a model glob rule. Registered in a code-level registry; each hook owns one config flag. The eviction logic behind `--max-upstream-images` is the first request hook.
_Avoid_: interceptor, middleware, filter

**Stream hook**:
A retry callback fired mid-stream on upstream failure (`InvisibleTurnRetry`, `MidStreamRetry` in `internal/stream`). Unrelated to request hooks despite the shared word.
_Avoid_: (do not call request hooks just "hooks")

**Image-caption hook**:
The first non-trivial request hook: for models the operator rules non-visual, replace each image block with a text block holding a caption produced by a vision model, so a text-only upstream model still receives image content.

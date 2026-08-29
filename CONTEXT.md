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

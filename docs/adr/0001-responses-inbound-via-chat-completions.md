# 0001 — Expose /v1/responses by translating to upstream chat/completions

Downstream clients (Codex CLI, OpenAI SDK) call `POST /v1/responses` on this proxy; the proxy translates to the upstream's `/v1/chat/completions` and translates the (streamed) reply back into Responses-format SSE. Upstream calls stay stateless: no `previous_response_id`, no `store` is ever sent upstream — statefulness is simulated by a proxy-side response store that expands `previous_response_id` into full message history. Hosted tools (`web_search`, `file_search`, `computer_use`, `code_interpreter`, `image_generation`) are rejected with 400 since no chat/completions upstream can execute them.

## Considered options

- **Reverse direction** (chat/completions inbound → responses outbound): rejected — the goal is serving Responses-speaking clients off existing chat/completions upstreams.
- **Rejecting `previous_response_id`** (pure stateless) or silently ignoring it: rejected — breaks real session-style clients and hides context loss.
- **Forwarding store/state upstream**: impossible — chat/completions has no such concept.

## Consequences

- The proxy needs a response store with TTL (24h sliding, configurable): reuse the dump-session machinery for disk persistence if cheap, otherwise start in-memory.
- Reasoning round-trip reuses the existing per-model replay mechanism instead of Responses' encrypted-content flow where possible.
- Acceptance clients: Codex CLI first, OpenAI SDK second — full tool-call + streaming round trips must pass.

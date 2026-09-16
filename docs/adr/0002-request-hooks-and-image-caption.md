# 0002 — Request hooks and the image-caption hook

Text-only upstream models (deepseek, kimi, qwen-text, …) reject requests carrying images with a hard 400, while the proxy's downstream clients (Claude Code) routinely send images. The proxy had no model-capability model at all: `/v1/models` is passthrough and the only per-model config is two glob rules (`--upstream-extra-params`, `--reasoning-replay`). One request transformation already existed — `convert.EvictOldImages` at `server.go:581-584`, called between request decode and upstream-request build to cap images at `--max-upstream-images`. That call site is a de-facto request hook.

Rather than bolt vision onto the config, we introduce a small **request-hook registry** (`internal/hook`): each hook is a named transformation plus a model-glob rule that says where it applies, applied in registration order at the single call site. The gateway does not reason about model capabilities at all — the operator decides which models a hook applies to. The eviction logic becomes the first registered hook (glob `*`, config flag `--max-upstream-images` unchanged). The first non-trivial hook is the **image-caption hook**: for models its glob rules match, every image block (user image blocks, base64 image documents, images nested in tool_result content) is replaced by a text block holding a caption produced by a vision model — single batched upstream call per request, `stream:false`, `temperature:0`, small `max_tokens`. Caption upstream defaults to the main upstream with only the model name overridden; optional `--image-caption-base-url` / `--image-caption-api-key` select a dedicated vision upstream. Captions are cached in memory by content hash (TTL, default 24h). A caption failure never fails the user's request: the image falls back to the existing placeholder and a warning is logged. URL-source images are passed to the vision model as-is; document blocks are untouched. Phase 1 applies hooks on `/v1/messages` only.

## Considered options

- **Reactive 400-then-strip retry** (like the existing stream retry hooks): rejected — every image-bearing first turn pays a full failed round trip, latency doubles, and it requires matching upstream error shapes per provider; proactive captioning is strictly better UX.
- **A static capability table keyed on `/v1/models`**: rejected — OpenAI-compatible relays do not expose vision capability, `/v1/models` is passthrough by design, and maintaining a table duplicates the operator's knowledge.
- **A "no-vision models" blacklist flag**: rejected as framing — the gateway should not model capabilities at all; the hook-application rule is the general mechanism and vision is just one hook.
- **Per-image caption calls**: rejected — one batched call keeps the added latency to a single hop (the vision model accepts the same ≤7-image cap eviction already defends against).
- **On-disk caption cache**: rejected — captions are keyed by content hash, in-memory + TTL is enough for the multi-turn/retry case; disk adds a persistence surface for no benefit.
- **Locally fetching URL images before captioning**: rejected for phase 1 — the vision call passes the URL through; if the upstream cannot fetch it, the placeholder fallback applies.
- **Generic `--hook name=glob` / `--hook-option` flags, or a config file**: rejected — with one hook these are over-design, and the project is CLI-flag-only by convention.
- **Captioning on `/v1/responses` in phase 1**: rejected — that path has no image handling at all yet (not even eviction); it gets hooks when it gets image support.

## Consequences

- New `internal/hook` package plus `internal/hook/caption`; `server.go:581-584` collapses to a single `hook.Apply(...)` call. Eviction moves behind the registry but keeps its flag, behaviour, and existing tests.
- The image-block enumeration and replacement primitives in `internal/convert/evict.go` are promoted to shared helpers — the caption hook reuses the exact tool_result marshal-back path that eviction already proved out.
- Matched requests pay one extra upstream round trip per image-bearing turn (cached on repeat hits within TTL); a vision model must be provisioned, either on the main upstream or a dedicated one.
- `config.Load` fails fast at startup when `--hook-image-caption` globs are set without `--image-caption-model`.
- README flag tables (en + zh) gain the five new flags and a hook section.
- `/v1/responses` remains hook-less until phase 2; the registry interface wraps Anthropic-canonical request types, so a second application point (or canonicalization of Responses input) is a phase-2 decision, not a blocker.
- Caption quality bounds utility: text models receive a vision model's description of e.g. a screenshot, not the pixels. Delimiter-based per-image parsing is deliberately strict, with placeholder fallback when the vision reply does not parse.

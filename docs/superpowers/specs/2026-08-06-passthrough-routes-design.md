# Passthrough Routes — Design Spec

Date: 2026-08-06
Status: Approved (design review)

## Goal

Make the proxy a dual-protocol gateway: Claude Code keeps using `/v1/messages`
(Anthropic format, converted to OpenAI upstream), while OpenAI-format clients
(`openai` SDK and other tools) can reach the **same** upstream through the same
process via two new passthrough routes — no conversion, byte-for-byte.

This supersedes an earlier implementation of the same feature that was reset off
the branch (commits `d9b610a`/`68d63a0` remain in the object DB). Decision:
implement fresh per this spec rather than recover those commits.

## Routes

| Path | Method | Behavior |
|------|--------|----------|
| `/v1/chat/completions` | POST | Body forwarded verbatim to `{UpstreamBaseURL}/chat/completions`; response (SSE or JSON) streamed back verbatim |
| `/v1/models` | GET | Forwarded to `{UpstreamBaseURL}/models`; response forwarded verbatim |
| `/models` | GET | 404 (not registered — falls through to existing `default:` case) |

Added as two `case` branches in `handleRequest` (server.go).

## Shared passthrough flow (one helper, both routes)

`forwardPassthrough(w, r, cfg, upstreamPath)` performs all steps; the routes only
differ in the upstream path suffix.

1. **Auth** — same policy as `/v1/messages`:
   - `validateAuthToken(r, cfg)`: 401 when `--auth-token` is set and the client
     key does not match.
   - `resolveAPIKey(r, cfg)`: 401 when empty (no `--upstream-api-key` configured
     and no client key in passthrough mode).
2. **Upstream request** — built with `r.Context()` (client disconnect cancels the
   upstream fetch), method and body taken from the client request. `Content-Type`
   forwarded from the client when present, else `application/json`.
   `Authorization: Bearer <resolvedKey>`. No `--upstream-extra-params`, no
   `stream_options.include_usage`, no canonicalization — the body is forwarded
   **verbatim**, including malformed payloads (the upstream reports those).
3. **Response** — forward the upstream **status code** and **Content-Type**
   unchanged, add `Access-Control-Allow-Origin: *`, and copy the body verbatim
   through a flush wrapper so SSE chunks reach the client incrementally. SSE
   keep-alive headers (`X-Accel-Buffering: no`, `Cache-Control: no-cache`,
   `Connection: keep-alive`) are set only when the upstream `Content-Type` is
   `text/event-stream`.

## Error handling

- Upstream non-2xx status → **forwarded verbatim** (upstream already returns
  OpenAI-format errors; no mapping).
- Proxy-side failures (auth failure, missing key, body read error, upstream
  connect error) → OpenAI-format error body
  (`{"error":{"message","type","param"}}`), since passthrough endpoints serve
  OpenAI-format clients. Status codes: 401 auth/missing-key, 400 body read, 500
  request construction, 502 upstream connect failure.

## Out of scope

- No dump sessions for passthrough routes (`--dump` untouched).
- No request-body processing of any kind.
- No change to the CORS preflight handler (existing OPTIONS already allows
  `GET, POST` and the `Content-Type`/`Authorization`/`x-api-key` headers).

## Files touched

- `internal/proxy/server.go` — two route cases + `forwardPassthrough` +
  OpenAI-format error body helper + `flushWriter`.
- `internal/proxy/server_test.go` — new tests (below).
- `main.go` — one banner line listing the passthrough endpoints.
- `README.md` + `README-zh.md` — API Endpoints table rows for the two routes.

## Testing

1. `/v1/chat/completions` forwards the request body and response body
   **byte-for-byte** (mock upstream echoes the captured body, returns SSE);
   status 200, CORS and `Content-Type: text/event-stream` set.
2. `/v1/chat/completions` with `stream: false` → upstream JSON forwarded
   verbatim.
3. `/v1/models` returns the upstream models JSON verbatim (status + body).
4. Auth: both routes return 401 without `--auth-token`; 200 with it; passthrough
   mode forwards the client key as `Authorization: Bearer`.
5. `/v1/models` returns 401 in non-passthrough mode when no key resolves
   (no upstream key, no client key).
6. `/models` (bare) returns 404 via the existing default case.

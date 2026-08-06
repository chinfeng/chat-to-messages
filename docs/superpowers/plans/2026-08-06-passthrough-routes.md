# Passthrough Routes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add transparent verbatim passthrough endpoints `POST /v1/chat/completions` and `GET /v1/models` so OpenAI-format clients can share the same upstream as Claude Code's converted `/v1/messages` flow.

**Architecture:** Two new `case` branches in `handleRequest` route to a single shared helper `forwardPassthrough(w, r, cfg, upstreamPath)` that performs auth + API-key resolution (same policy as `/v1/messages`), builds an upstream request from the client request verbatim, and relays the upstream response (status, Content-Type, body) back unchanged with a flush-per-write copy so SSE streams incrementally. No conversion, no `--upstream-extra-params`, no `include_usage`, no dump sessions.

**Tech Stack:** Go standard library only (`net/http`, `bytes`, `io`, `strings`). Zero external deps.

## Global Constraints

- No external dependencies — Go stdlib only (matches repo).
- Body must be forwarded **byte-for-byte**: no JSON parse, no re-marshal, no `--upstream-extra-params`, no `stream_options.include_usage`, no canonicalization.
- Auth policy identical to `/v1/messages`: `validateAuthToken` (401 on mismatch) then `resolveAPIKey` (401 if empty).
- Upstream non-2xx statuses are relayed verbatim — never mapped.
- Proxy-side failures on passthrough routes return **OpenAI-format** error bodies (`{"error":{...}}`), not the Anthropic format used by `/v1/messages`.
- No dump sessions for passthrough routes.
- SSE keep-alive headers (`X-Accel-Buffering`, `Cache-Control`, `Connection`) set only when upstream `Content-Type` is `text/event-stream`.
- Route `/models` (bare) must stay 404 (falls through to existing default).
- Commit docs + code together at the end (user decision); do not commit spec/plan separately.

---

### Task 1: Shared passthrough helper + route wiring in server.go

**Files:**
- Modify: `internal/proxy/server.go` (routing switch in `handleRequest` around line 133-142; add helpers near the other error helpers)

**Interfaces:**
- Consumes: existing `config.Config`, `validateAuthToken`, `resolveAPIKey`, `writeJSON` (all already in `server.go`).
- Produces: `forwardPassthrough(w http.ResponseWriter, r *http.Request, cfg *config.Config, upstreamPath string)` — used by Task 2's routes. Also produces `openAIErrorBody`/`openAIError(...)` and `flushWriter` used by the same helper.

- [ ] **Step 1: Add the routing cases**

In `handleRequest`, add two cases before the `default:` case:

```go
	case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		forwardPassthrough(w, r, cfg, "/chat/completions")
	case r.Method == http.MethodGet && r.URL.Path == "/v1/models":
		forwardPassthrough(w, r, cfg, "/models")
```

- [ ] **Step 2: Add the OpenAI-format error type**

Add near the existing `apiErrorBody` helpers (after `serverError`):

```go
// openAIErrorBody is the OpenAI-format error response body used by the
// passthrough routes (served to OpenAI-format clients, so the body must match
// the shape the OpenAI SDKs parse: {"error":{message,type,param}}).
type openAIErrorBody struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Param   any    `json:"param"`
	} `json:"error"`
}

func openAIError(errorType, message string) openAIErrorBody {
	var b openAIErrorBody
	b.Error.Type = errorType
	b.Error.Message = message
	return b
}
```

- [ ] **Step 3: Add the `flushWriter` and the passthrough handler**

Add at the end of `server.go`:

```go
// flushWriter wraps a ResponseWriter so every Write is followed by a Flush,
// delivering streamed upstream SSE to the client incrementally.
type flushWriter struct {
	w io.Writer
	f http.Flusher
}

func (fw flushWriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	fw.f.Flush()
	return n, err
}

// forwardPassthrough relays a request to the upstream verbatim: same method,
// same body, resolved Authorization header. The upstream response (status
// code, Content-Type, body) is relayed unchanged. Used by POST
// /v1/chat/completions and GET /v1/models.
func forwardPassthrough(w http.ResponseWriter, r *http.Request, cfg *config.Config, upstreamPath string) {
	if !validateAuthToken(r, cfg) {
		writeJSON(w, http.StatusUnauthorized, openAIError("authentication_error", "Invalid auth token. Provide correct x-api-key header."))
		return
	}
	apiKey := resolveAPIKey(r, cfg)
	if apiKey == "" {
		writeJSON(w, http.StatusUnauthorized, openAIError("authentication_error", "No API key provided. Set --upstream-api-key or enable passthrough mode (no upstream key and no auth token)."))
		return
	}

	var body io.Reader
	if r.Body != nil {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, openAIError("invalid_request_error", "Invalid request body."))
			return
		}
		body = bytes.NewReader(raw)
	}

	url := strings.TrimRight(cfg.UpstreamBaseURL, "/") + upstreamPath
	upstreamReq, err := http.NewRequestWithContext(r.Context(), r.Method, url, body)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, openAIError("api_error", err.Error()))
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		upstreamReq.Header.Set("Content-Type", ct)
	} else {
		upstreamReq.Header.Set("Content-Type", "application/json")
	}
	upstreamReq.Header.Set("Authorization", "Bearer "+apiKey)

	// The request context is the abort signal: client disconnect cancels the
	// upstream fetch (same contract as handleMessages).
	client := &http.Client{}
	upstreamRes, err := client.Do(upstreamReq)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, openAIError("api_error", "Upstream error: "+err.Error()))
		return
	}
	defer upstreamRes.Body.Close()

	// Relay the upstream response verbatim: status, Content-Type, body.
	ct := upstreamRes.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	if strings.Contains(ct, "text/event-stream") {
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(upstreamRes.StatusCode)
	if flusher, ok := w.(http.Flusher); ok {
		_, _ = io.Copy(flushWriter{w: w, f: flusher}, upstreamRes.Body)
	} else {
		_, _ = io.Copy(w, upstreamRes.Body)
	}
}
```

Note: `bytes`, `io`, `strings` are already imported in `server.go` (see imports at top: `bytes`, `io`, `strings`, `net/http`, `encoding/json`, `fmt`, `sync`, `sync/atomic`, `time`). No import changes needed.

- [ ] **Step 4: Build check**

Run: `go build ./...`
Expected: compiles clean.

- [ ] **Step 5: Commit**

```bash
git add internal/proxy/server.go
git commit -m "feat: shared verbatim passthrough helper for OpenAI routes"
```

---

### Task 2: Server tests for both passthrough routes

**Files:**
- Modify: `internal/proxy/server_test.go` (append tests before the `contains` helper at the bottom)

**Interfaces:**
- Consumes: `NewHandler`, `testConfig`, `mockUpstream`, `postMessages`, `readBody`, `parseSSE`, `eventTypes` (all existing in `server_test.go`).
- Produces: test coverage only (no new production interfaces).

- [ ] **Step 1: Add a helper to capture the upstream request body**

Add near `postMessages` (after line ~121):

```go
// passthroughPost posts to the given path with a verbatim body and returns the
// response plus the exact body bytes the mock upstream captured.
func passthroughPost(t *testing.T, h http.Handler, path, body string, headers map[string]string) (resp *http.Response, captured string) {
	t.Helper()
	var got string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	hh := NewHandler(cfg)
	req := httptest.NewRequest("POST", path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	hh.ServeHTTP(rec, req)
	return rec.Result(), got
}
```

- [ ] **Step 2: Write the failing tests**

Add these test functions at the end of `server_test.go` (before the `contains` helper):

```go
func TestChatCompletionsPassthroughVerbatim(t *testing.T) {
	body := `{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true,"extra":"保留"}` // include non-ASCII + unknown keys
	var capturedBody string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		capturedBody = string(b)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n")
	}))
	defer up.Close()
	h := NewHandler(testConfig(up.URL))
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	// Body forwarded byte-for-byte (including the non-ASCII bytes).
	if capturedBody != body {
		t.Errorf("upstream body mismatch:\n got: %q\nwant: %q", capturedBody, body)
	}
	// Response relayed verbatim + CORS.
	out := rec.Body.String()
	if !strings.Contains(out, "chat.completion.chunk") || !strings.Contains(out, "[DONE]") {
		t.Errorf("response not relayed verbatim: %s", out)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS missing")
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}
	// SSE keep-alive headers present for event-stream responses.
	if rec.Header().Get("X-Accel-Buffering") != "no" {
		t.Error("X-Accel-Buffering missing on SSE passthrough")
	}
}

func TestChatCompletionsPassthroughNonStream(t *testing.T) {
	jsonBody := `{"id":"cmpl-1","object":"chat.completion","model":"m","choices":[{"message":{"role":"assistant","content":"hi"}}]}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, jsonBody)
	}))
	defer up.Close()
	h := NewHandler(testConfig(up.URL))
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[],"stream":false}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Body.String(); got != jsonBody {
		t.Errorf("non-stream body not relayed verbatim:\n got: %q\nwant: %q", got, jsonBody)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	// Non-event-stream responses must NOT carry SSE keep-alive headers.
	if rec.Header().Get("X-Accel-Buffering") != "" {
		t.Error("X-Accel-Buffering set on non-stream response")
	}
}

func TestModelsPassthrough(t *testing.T) {
	models := `{"object":"list","data":[{"id":"gpt-4o","object":"model","owned_by":"openai"}]}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/models" {
			t.Errorf("upstream got %s %s, want GET /models", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, models)
	}))
	defer up.Close()
	h := NewHandler(testConfig(up.URL))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/models", nil))

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Body.String(); got != models {
		t.Errorf("models body not relayed verbatim:\n got: %q\nwant: %q", got, models)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS missing")
	}
}

func TestPassthroughForwardsClientKey(t *testing.T) {
	var gotAuth string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer up.Close()
	cfg := testConfig(up.URL)
	cfg.UpstreamAPIKey = ""
	cfg.AuthToken = "" // 透传模式
	h := NewHandler(cfg)
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "client-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(gotAuth, "client-key") {
		t.Errorf("client key not forwarded: %q", gotAuth)
	}
}

func TestPassthroughAuthRequired(t *testing.T) {
	up := mockUpstream(t, "data: [DONE]\n\n", 200)
	cfg := testConfig(up.URL)
	cfg.AuthToken = "secret"
	h := NewHandler(cfg)

	// POST /v1/chat/completions without token → 401.
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Fatalf("chat completions 401 expected, got %d", rec.Code)
	}
	// OpenAI-format error body (passthrough clients parse this shape).
	if !strings.Contains(rec.Body.String(), `"error"`) || !strings.Contains(rec.Body.String(), "authentication_error") {
		t.Errorf("auth error body = %s", rec.Body.String())
	}

	// GET /v1/models without token → 401.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("GET", "/v1/models", nil))
	if rec2.Code != 401 {
		t.Fatalf("models 401 expected, got %d", rec2.Code)
	}

	// With correct x-api-key → 200.
	req2 := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("x-api-key", "secret")
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, req2)
	if rec3.Code != 200 {
		t.Fatalf("200 expected with token, got %d", rec3.Code)
	}
}

func TestModelsPassthroughMissingKey401(t *testing.T) {
	// Non-passthrough mode: upstream key set, but client sends no key, so the
	// configured upstream key resolves (NOT a 401). So to exercise the 401
	// path we need passthrough mode with no client key.
	up := mockUpstream(t, "data: [DONE]\n\n", 200)
	cfg := testConfig(up.URL)
	cfg.UpstreamAPIKey = ""
	cfg.AuthToken = ""
	h := NewHandler(cfg)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/models", nil))
	if rec.Code != 401 {
		t.Fatalf("401 expected (passthrough mode, no client key), got %d", rec.Code)
	}
}
```

- [ ] **Step 3: Run the new tests**

Run: `go test ./internal/proxy/ -run 'Passthrough' -v`
Expected: all `Test*Passthrough*` tests PASS.

- [ ] **Step 4: Run the full suite**

Run: `go test ./...`
Expected: all packages PASS (existing tests unaffected — routing for `/v1/messages`, `/health`, and unknown paths is unchanged).

- [ ] **Step 5: Add a `/models` 404 assertion**

Extend `TestHealthAndNotFound` so it also asserts bare `/models` 404s (existing default case), then re-run:

```go
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, httptest.NewRequest("GET", "/models", nil))
	if rec3.Code != 404 {
		t.Fatalf("404 expected for bare /models, got %d", rec3.Code)
	}
```

Run: `go test ./internal/proxy/ -run TestHealthAndNotFound -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/proxy/server_test.go
git commit -m "test: passthrough /v1/chat/completions and /v1/models routes"
```

---

### Task 3: Banner line + README/README-zh rows + final verification

**Files:**
- Modify: `main.go` (after the "Thinking: %v" banner line, around line 33-34)
- Modify: `README.md` (API Endpoints table, lines ~372-378)
- Modify: `README-zh.md` (corresponding table)

**Interfaces:**
- Consumes: nothing new.
- Produces: docs.

- [ ] **Step 1: Add banner line**

In `main.go`, after the `fmt.Printf("  Thinking: %v\n", cfg.EnableThinking)` line, add:

```go
	fmt.Printf("  OpenAI passthrough: /v1/chat/completions, /v1/models\n")
```

- [ ] **Step 2: Update README.md API Endpoints table**

In the table at lines 372-378, change:

```markdown
| `/v1/messages` | POST | Anthropic Messages API proxy (core endpoint) |
| `/health` | GET | Health check |
```

to:

```markdown
| `/v1/messages` | POST | Anthropic Messages API proxy (core endpoint) |
| `/v1/chat/completions` | POST | OpenAI Chat Completions passthrough (verbatim, no conversion) |
| `/v1/models` | GET | OpenAI Models passthrough (verbatim) |
| `/health` | GET | Health check |
```

- [ ] **Step 3: Update README-zh.md API Endpoints table**

Find the corresponding table in README-zh.md and add the same two rows (mirror the Chinese wording used elsewhere in that file).

- [ ] **Step 4: Full verification**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: build clean, vet clean, all tests PASS.

- [ ] **Step 5: Final combined commit**

Since docs were held until the end (user decision), commit everything together. First check status shows only the intended files (spec/plan docs + code + READMEs):

```bash
git status --short
```

Then commit the remaining uncommitted docs + the banner/README changes. (If Tasks 1-2 were already committed individually, commit the remaining files in this one commit. If the user prefers a single squash commit for the whole feature, use `git add` for all feature files — including the spec/plan under `docs/superpowers/` — and commit once.)

```bash
git add main.go README.md README-zh.md docs/superpowers/
git commit -m "feat: add /v1/chat/completions and /v1/models passthrough endpoints"
```

---

## Self-Review

**Spec coverage:**
- Routes `/v1/chat/completions` + `/v1/models` → Task 1 (wiring) + Task 2 (tests). ✓
- Verbatim body (no processing) → Task 1 Step 3 (`bytes.NewReader(raw)`, no re-marshal). ✓
- Auth policy (`validateAuthToken` + `resolveAPIKey`, 401 empty) → Task 1 Step 3; tests Task 2 (`TestModelsPassthroughMissingKey401`, `TestPassthroughAuthRequired`). ✓
- Response relayed verbatim (status + Content-Type + body, flush) → Task 1 Step 3; tests Task 2. ✓
- SSE keep-alive headers only on `text/event-stream` → Task 1 Step 3 (`strings.Contains(ct, "text/event-stream")`); asserted in Task 2 tests (present on SSE, absent on JSON). ✓
- Upstream non-2xx relayed verbatim (not mapped) → `passthroughResponse`-equivalent `w.WriteHeader(upstreamRes.StatusCode)` in Task 1. ✓
- Proxy-side failures use OpenAI-format error body → Task 1 Step 2 (`openAIErrorBody`); asserted in Task 2. ✓
- `/models` (bare) 404 → Task 2 Step 5. ✓
- No dump for passthrough → `forwardPassthrough` never touches `dump.NewSession`. ✓
- Docs + code committed together → Task 3 Step 5. ✓

**Placeholder scan:** No TBD/TODO; every code step has full code. ✓

**Type consistency:** `forwardPassthrough(w, r, cfg, upstreamPath string)` defined in Task 1 and called in Task 1 Step 1 with the same signature; `openAIError(errorType, message string) openAIErrorBody` matches its usage in Task 1 Step 3; test helpers (`passthroughPost`) and tests reference only existing helpers (`testConfig`, `mockUpstream`, `NewHandler`, `httptest`). ✓

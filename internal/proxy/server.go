// Package proxy implements the HTTP server layer of chat-to-messages: CORS
// and routing, downstream auth and API-key resolution, the upstream
// OpenAI-compatible request, the SSE pump (single select loop + pump
// goroutine), and dump session orchestration. Ported from
// chat-to-claude-code's src/server/routes.ts and src/server/index.ts.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chinfeng/chat-to-messages/internal/anthropic"
	"github.com/chinfeng/chat-to-messages/internal/config"
	"github.com/chinfeng/chat-to-messages/internal/convert"
	"github.com/chinfeng/chat-to-messages/internal/dump"
	"github.com/chinfeng/chat-to-messages/internal/hook"
	"github.com/chinfeng/chat-to-messages/internal/hook/caption"
	"github.com/chinfeng/chat-to-messages/internal/openai"
	"github.com/chinfeng/chat-to-messages/internal/responses"
	"github.com/chinfeng/chat-to-messages/internal/servertool"
	"github.com/chinfeng/chat-to-messages/internal/sse"
	"github.com/chinfeng/chat-to-messages/internal/stream"
	"github.com/chinfeng/chat-to-messages/internal/ws"
)

// upstreamSSEHeaders mirrors ANTHROPIC_SSE_RESPONSE_HEADERS in the TS
// builder.ts (the sse package does not export it, so the values are written
// here directly): keep-alive plumbing for streaming responses.
var upstreamSSEHeaders = map[string]string{
	"X-Accel-Buffering": "no",
	"Cache-Control":     "no-cache",
	"Connection":        "keep-alive",
}

// apiErrorBody is the Anthropic-format error response body
// (src/core/errors.ts makeAnthropicError).
type apiErrorBody struct {
	Type  string `json:"type"`
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

func errorBody(errorType, message string) apiErrorBody {
	var b apiErrorBody
	b.Type = "error"
	b.Error.Type = errorType
	b.Error.Message = message
	return b
}

func invalidRequestError(message string) apiErrorBody {
	return errorBody("invalid_request_error", message)
}
func authenticationError(message string) apiErrorBody {
	return errorBody("authentication_error", message)
}
func notFoundError(message string) apiErrorBody { return errorBody("not_found_error", message) }
func serverError(message string) apiErrorBody   { return errorBody("api_error", message) }

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

// upstreamError prefixes the message with "Upstream error: " exactly like the
// TS upstreamError() helper.
func upstreamError(message string) apiErrorBody {
	return errorBody("api_error", "Upstream error: "+message)
}

// writeJSON writes an Anthropic-format JSON response with the CORS header the
// TS index.ts adds to every response. No trailing newline, no HTML escaping
// (matching TS JSON.stringify).
func writeJSON(w http.ResponseWriter, status int, body any) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
	data := bytes.TrimRight(buf.Bytes(), "\n")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

// jsonString renders v as compact JSON (for dump logs).
func jsonString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// truncate mirrors TS string.slice(0, max) for error-message truncation
// (runes approximate UTF-16 code units).
func truncate(s string, max int) string {
	runes := []rune(s)
	if len(runes) > max {
		return string(runes[:max])
	}
	return s
}

// headerMap converts an http.Header into a lowercase-key map for dump logs
// (Go canonicalizes header names on the wire; lowercase is the Go convention
// for the logs).
func headerMap(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, vs := range h {
		out[strings.ToLower(k)] = strings.Join(vs, ", ")
	}
	return out
}

// NewHandler returns the HTTP handler for the proxy: CORS preflight handling
// first (matching Bun.serve in index.ts), then routing. The /v1/responses
// request store (previous_response_id chains) is per-server state created
// here.
func NewHandler(cfg *config.Config) http.Handler {
	responsesStore := responses.NewStore(time.Duration(cfg.ResponsesStoreTTLMinutes) * time.Minute)
	hooks := buildHookRegistry(cfg)
	// The caption hook also serves the Responses dialect directly (the registry
	// is Anthropic-canonical); nil when the operator did not configure it.
	var imageCaption *caption.Hook
	if len(cfg.HookImageCaptionPatterns) > 0 {
		imageCaption = caption.New(cfg)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, x-api-key, anthropic-version")
			w.Header().Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		handleRequest(w, r, cfg, responsesStore, hooks, imageCaption)
	})
}

// buildHookRegistry wires the request hooks for the Anthropic Messages
// dialect: eviction first (it removes the oldest images so downstream image
// hooks see a bounded request), then the caption hook when the operator has
// configured non-visual model globs.
func buildHookRegistry(cfg *config.Config) *hook.Registry {
	r := hook.NewRegistry()
	r.Register(&hook.EvictionHook{Keep: cfg.MaxUpstreamImages})
	if len(cfg.HookImageCaptionPatterns) > 0 {
		r.Register(caption.New(cfg))
	}
	return r
}

// handleRequest routes a request (port of routeRequest() in routes.ts).
func handleRequest(w http.ResponseWriter, r *http.Request, cfg *config.Config, responsesStore *responses.Store, hooks *hook.Registry, imageCaption *caption.Hook) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/health":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/messages":
		handleMessages(w, r, cfg, hooks)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/responses":
		handleResponses(w, r, cfg, responsesStore, imageCaption)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/responses":
		// OpenAI websocket mode: same dialect, upgrade instead of POST.
		if ws.IsUpgrade(r) {
			handleResponsesWS(w, r, cfg, responsesStore, imageCaption)
		} else {
			writeJSON(w, http.StatusBadRequest, openAIError("invalid_request_error", "GET /v1/responses requires a websocket upgrade (use POST for SSE)."))
		}
	case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		forwardPassthrough(w, r, cfg, "/chat/completions")
	case r.Method == http.MethodGet && r.URL.Path == "/v1/models":
		forwardPassthrough(w, r, cfg, "/models")
	default:
		writeJSON(w, http.StatusNotFound, notFoundError(fmt.Sprintf("No route for %s %s", r.Method, r.URL.Path)))
	}
}

// clientKey extracts the client API key: x-api-key, or the Authorization
// Bearer token (TS: request.headers.get("x-api-key") ||
// authorization?.replace(/^Bearer\s+/i, "")).
func clientKey(r *http.Request) string {
	if k := r.Header.Get("x-api-key"); k != "" {
		return k
	}
	return stripBearer(r.Header.Get("Authorization"))
}

func stripBearer(auth string) string {
	if len(auth) >= len("Bearer ") && strings.EqualFold(auth[:len("Bearer ")], "Bearer ") {
		return strings.TrimLeft(auth[len("Bearer "):], " \t\r\n")
	}
	return auth
}

// isPassthroughMode mirrors isPassthroughMode(): no upstream key and no
// downstream auth token.
func isPassthroughMode(cfg *config.Config) bool {
	return cfg.UpstreamAPIKey == "" && cfg.AuthToken == ""
}

// validateAuthToken mirrors validateAuthToken(): passes when no token is
// configured; otherwise the client key must equal the configured token.
func validateAuthToken(r *http.Request, cfg *config.Config) bool {
	if cfg.AuthToken == "" {
		return true
	}
	return clientKey(r) == cfg.AuthToken
}

// resolveAPIKey mirrors resolveApiKey(): in passthrough mode the client key
// is forwarded; otherwise the configured upstream key is used.
func resolveAPIKey(r *http.Request, cfg *config.Config) string {
	key := clientKey(r)
	if isPassthroughMode(cfg) && key != "" {
		return key
	}
	return cfg.UpstreamAPIKey
}

// hasServerToolRequest reports whether the request contains any server tool
// entry (from the tools array or the server_tools field).
func hasServerToolRequest(req *anthropic.MessagesRequest) bool {
	for _, t := range req.Tools {
		if servertool.IsServerToolType(toolTypeString(t)) {
			return true
		}
	}
	for _, st := range req.ServerTools {
		if servertool.IsServerToolType(toolTypeString(st)) {
			return true
		}
	}
	return false
}

func toolTypeString(t map[string]any) string {
	if s, ok := t["type"].(string); ok {
		return s
	}
	return ""
}

// applyIncludeUsage mirrors applyIncludeUsage() in routes.ts: request
// OpenAI-standard SSE usage accounting from the upstream, preserving any
// other stream_options keys the caller set.
func applyIncludeUsage(body map[string]any) {
	if so, ok := body["stream_options"].(map[string]any); ok && so != nil {
		so["include_usage"] = true
	} else {
		body["stream_options"] = map[string]any{"include_usage": true}
	}
}

// buildBaseBody is the shared upstream body construction for both
// buildUpstreamRequest and buildUpstreamRequestBodyOnly (port of the body
// steps of buildUpstreamRequest() in routes.ts): base body, stream: true,
// model extra merged, include_usage applied, canonicalized.
func buildBaseBody(cfg *config.Config, requestData *convert.RequestData) (map[string]any, error) {
	// Replay mode is per-model: kimi-k3 requires assistant tool-call turns to
	// carry reasoning_content when thinking is enabled, while think_tags is
	// the native representation for GLM-family upstreams and DeepSeek rejects
	// reasoning_content in input messages outright.
	replayMode := convert.ReasoningReplayMode(config.ResolveReasoningReplay(
		requestData.Model, cfg.ReasoningReplayRules, cfg.DefaultReasoningReplay))
	body, err := convert.BuildBaseRequestBody(requestData, 4096, replayMode)
	if err != nil {
		return nil, err
	}
	body["stream"] = true

	extra := config.ResolveModelExtra(requestData.Model, cfg.ModelOverrides)
	if len(extra) > 0 {
		body = config.DeepMerge(body, extra)
	}
	applyIncludeUsage(body)
	return convert.PrepareCanonicalBody(body), nil
}

// buildUpstreamRequest mirrors buildUpstreamRequest() in routes.ts: builds
// the OpenAI-compatible request body (stream: true, model extra merged,
// include_usage applied, canonicalized) and the /chat/completions URL.
// Returns the wire body and the pretty-printed body for the dump.
func buildUpstreamRequest(cfg *config.Config, requestData *convert.RequestData, apiKey string) (url string, wire []byte, pretty []byte, err error) {
	body, err := buildBaseBody(cfg, requestData)
	if err != nil {
		return "", nil, nil, err
	}
	url = strings.TrimRight(cfg.UpstreamBaseURL, "/") + "/chat/completions"

	wire, err = json.Marshal(body)
	if err != nil {
		return "", nil, nil, err
	}
	// TS logs JSON.stringify(body, null, 2) for the dump.
	pretty, err = json.MarshalIndent(body, "", "  ")
	if err != nil {
		return "", nil, nil, err
	}
	return url, wire, pretty, nil
}

// buildUpstreamRequestBodyOnly mirrors buildUpstreamRequestBodyOnly() in
// routes.ts: the same body as buildUpstreamRequest (full body including the
// server tool function schemas — Claude Code puts server tools in the tools
// array), compact JSON for the wire.
func buildUpstreamRequestBodyOnly(cfg *config.Config, requestData *convert.RequestData) (url string, wire []byte, err error) {
	body, err := buildBaseBody(cfg, requestData)
	if err != nil {
		return "", nil, err
	}
	wire, err = json.Marshal(body)
	if err != nil {
		return "", nil, err
	}
	return strings.TrimRight(cfg.UpstreamBaseURL, "/") + "/chat/completions", wire, nil
}

// emptyTurnRetryHint is the corrective user cue appended after the abandoned
// turn when the empty-turn guard re-requests upstream. Deliberately free of
// accusation framing ("you produced nothing visible") — that framing fed
// kimi-k3's imitation collapse (dumped 2026-08-27).
const emptyTurnRetryHint = "Your previous turn ended without any tool call or visible answer. Continue your task now: emit the needed tool call(s), or a concise final answer."

// invisibleTurnRetryMessages appends the abandoned turn (replayed as its own
// assistant turn, reasoning in the think-tag form kimi upstreams emit natively)
// plus the corrective cue, so the retry request reads as an ordinary
// continuation ending on a user turn.
func invisibleTurnRetryMessages(req *convert.RequestData, info stream.InvisibleTurnInfo) []anthropic.Message {
	assistantText := ""
	if strings.TrimSpace(info.Reasoning) != "" {
		assistantText = "<think>\n" + info.Reasoning + "\n</think>\n\n"
	}
	assistantText += info.VisibleText
	out := make([]anthropic.Message, 0, len(req.Messages)+2)
	out = append(out, req.Messages...)
	out = append(out,
		anthropic.Message{
			Role:    "assistant",
			Content: anthropic.ContentValue{IsString: true, Str: assistantText},
		},
		anthropic.Message{
			Role:    "user",
			Content: anthropic.ContentValue{IsString: true, Str: emptyTurnRetryHint},
		},
	)
	return out
}

// emptyTurnRetryHook builds the stream.Options InvisibleTurnRetry callback for
// a /v1/messages request. The returned closure enforces the configured budget;
// each retry is logged to numbered dump attempt files. Returning nil (budget
// exhausted or any failure) degrades the stream layer to its baseline
// invisible-turn finalize.
func emptyTurnRetryHook(ctx context.Context, cfg *config.Config, session *dump.Session, requestData *convert.RequestData, apiKey string) func(int, stream.InvisibleTurnInfo) iter.Seq[openai.Chunk] {
	return func(attempt int, info stream.InvisibleTurnInfo) iter.Seq[openai.Chunk] {
		if !cfg.EmptyTurnGuard || attempt > cfg.EmptyTurnMaxRetries {
			return nil
		}
		attemptNo := attempt + 1 // dump numbering: primary request = attempt 1

		retryReq := *requestData
		retryReq.Messages = invisibleTurnRetryMessages(requestData, info)
		url, wire, pretty, err := buildUpstreamRequest(cfg, &retryReq, apiKey)
		if err != nil {
			return nil
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(wire))
		if err != nil {
			return nil
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+apiKey)
		session.WriteUpstreamAttemptRequest(attemptNo, headerMap(httpReq.Header), time.Now().UTC().Format(time.RFC3339), string(pretty))

		res, err := (&http.Client{}).Do(httpReq)
		if err != nil {
			fmt.Fprintln(os.Stderr, "chat-to-messages: empty-turn guard retry connect failed:", err)
			return nil
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
			res.Body.Close()
			session.WriteUpstreamAttemptResponse(attemptNo, headerMap(res.Header), res.StatusCode, string(body))
			fmt.Fprintln(os.Stderr, "chat-to-messages: empty-turn guard retry got status", res.StatusCode)
			return nil
		}

		var raw strings.Builder
		seq := openai.IterSSEChunks(ctx, res.Body, &raw)
		return func(yield func(openai.Chunk) bool) {
			defer res.Body.Close()
			for chunk := range seq {
				if !yield(chunk) {
					return
				}
			}
			session.WriteUpstreamAttemptResponse(attemptNo, headerMap(res.Header), res.StatusCode, raw.String())
		}
	}
}

// stallWatchdogReader closes the body when no bytes arrive within the window,
// breaking the blocked Read so the stream aborts and the mid-stream retry
// guard takes over. The watchdog halts itself when the body ends (EOF or any
// read error — including the error its own Close induced), so every exit path
// (clean end, abort, early abandon) stops it; closing an http response body
// twice is a no-op, so it may race the route's deferred Close.
type stallWatchdogReader struct {
	r      io.Reader
	body   io.Closer
	window time.Duration
	last   atomic.Int64 // unix nano of the last received byte
	stop   chan struct{}
	halted sync.Once
}

func newStallWatchdog(r io.Reader, body io.Closer, window time.Duration) io.Reader {
	if window <= 0 {
		return r
	}
	w := &stallWatchdogReader{r: r, body: body, window: window, stop: make(chan struct{})}
	w.last.Store(time.Now().UnixNano())
	go w.watch()
	return w
}

func (w *stallWatchdogReader) Read(p []byte) (int, error) {
	n, err := w.r.Read(p)
	if n > 0 {
		w.last.Store(time.Now().UnixNano())
	}
	if err != nil {
		w.halted.Do(func() { close(w.stop) })
	}
	return n, err
}

func (w *stallWatchdogReader) watch() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if time.Since(time.Unix(0, w.last.Load())) > w.window {
				_ = w.body.Close() // break the blocked Read
				return
			}
		case <-w.stop:
			return
		}
	}
}

// midStreamRetryHook mirrors emptyTurnRetryHook: bounded re-requests of the
// SAME conversation when the upstream connection dies mid-stream (the
// newapi/GLM relay stalls ~135s and kills long plan generations, dumped
// 2026-09-13). The continuation re-streams into the same downstream message;
// the stream layer drops buffered parser content that never reached the
// client. Budget is fixed at 2 (a wedged upstream burns tokens either way);
// cfg.MidStreamStallTimeout <= 0 disables the hook entirely.
func midStreamRetryHook(ctx context.Context, cfg *config.Config, session *dump.Session, requestData *convert.RequestData, apiKey string) func(int, error) iter.Seq[openai.Chunk] {
	window := time.Duration(cfg.MidStreamStallTimeout) * time.Second
	return func(attempt int, err error) iter.Seq[openai.Chunk] {
		if window <= 0 || attempt > 2 {
			return nil
		}
		fmt.Fprintln(os.Stderr, "chat-to-messages: mid-stream abort, retrying upstream (attempt", attempt, "):", err)
		attemptNo := attempt + 1 // dump numbering: primary request = attempt 1

		url, wire, pretty, err := buildUpstreamRequest(cfg, requestData, apiKey)
		if err != nil {
			return nil
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(wire))
		if err != nil {
			return nil
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+apiKey)
		session.WriteUpstreamAttemptRequest(attemptNo, headerMap(httpReq.Header), time.Now().UTC().Format(time.RFC3339), string(pretty))

		res, err := (&http.Client{}).Do(httpReq)
		if err != nil {
			fmt.Fprintln(os.Stderr, "chat-to-messages: mid-stream retry connect failed:", err)
			return nil
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
			res.Body.Close()
			session.WriteUpstreamAttemptResponse(attemptNo, headerMap(res.Header), res.StatusCode, string(body))
			fmt.Fprintln(os.Stderr, "chat-to-messages: mid-stream retry got status", res.StatusCode)
			return nil
		}

		var raw strings.Builder
		seq := openai.IterSSEChunks(ctx, newStallWatchdog(res.Body, res.Body, window), &raw)
		return func(yield func(openai.Chunk) bool) {
			defer res.Body.Close()
			for chunk := range seq {
				if !yield(chunk) {
					return
				}
			}
			session.WriteUpstreamAttemptResponse(attemptNo, headerMap(res.Header), res.StatusCode, raw.String())
		}
	}
}

// handleMessages handles POST /v1/messages (port of handleMessages() in
// routes.ts): auth, body validation, upstream request, and the SSE pump.
func handleMessages(w http.ResponseWriter, r *http.Request, cfg *config.Config, hooks *hook.Registry) {
	session := dump.NewSession(cfg.DumpDir)
	requestStart := time.Now()
	requestDatetime := time.Now().UTC().Format(time.RFC3339)

	// Validate downstream auth token when configured.
	if !validateAuthToken(r, cfg) {
		session.Finish()
		writeJSON(w, http.StatusUnauthorized, authenticationError("Invalid auth token. Provide correct x-api-key header."))
		return
	}

	apiKey := resolveAPIKey(r, cfg)
	if apiKey == "" {
		session.Finish()
		writeJSON(w, http.StatusUnauthorized, authenticationError("No API key provided. Set --upstream-api-key or enable passthrough mode (no upstream key and no auth token)."))
		return
	}

	// Parse the Anthropic /v1/messages body (UseNumber fidelity).
	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		session.Finish()
		writeJSON(w, http.StatusBadRequest, invalidRequestError("Invalid JSON in request body."))
		return
	}
	var req anthropic.MessagesRequest
	dec := json.NewDecoder(bytes.NewReader(rawBody))
	dec.UseNumber()
	if err := dec.Decode(&req); err != nil {
		session.Finish()
		writeJSON(w, http.StatusBadRequest, invalidRequestError("Invalid JSON in request body."))
		return
	}
	// Reject trailing garbage after the JSON object (`{...}garbage`), matching
	// TS JSON.parse: a well-formed single object decodes with a second decode
	// landing on EOF, anything else is a 400.
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		session.Finish()
		writeJSON(w, http.StatusBadRequest, invalidRequestError("Invalid JSON in request body."))
		return
	}

	// Log the downstream request. TS pretty-prints the body (2-space indent);
	// the raw client bytes are stored verbatim here (plan decision — same
	// content, no re-serialization).
	session.WriteDownstreamRequest(headerMap(r.Header), requestDatetime, string(rawBody))

	if req.Model == "" {
		session.Finish()
		writeJSON(w, http.StatusBadRequest, invalidRequestError("`model` is required and must be a string."))
		return
	}
	// A nil Messages slice means the JSON had no (or null) `messages` field —
	// an empty array decodes to a non-nil empty slice. Matches the TS
	// Array.isArray(messages) check (an empty array is valid and reaches the
	// upstream, which reports it).
	if req.Messages == nil {
		session.Finish()
		writeJSON(w, http.StatusBadRequest, invalidRequestError("`messages` is required and must be an array."))
		return
	}

	// Request hooks run after validation and before conversion: eviction
	// caps images for the upstream channel, then the caption hook replaces
	// images with vision-model captions for models the operator ruled
	// non-visual. A hook error fails the request.
	if err := hooks.Apply(r.Context(), &hook.Request{Model: req.Model, Messages: req.Messages}); err != nil {
		session.Finish()
		writeJSON(w, http.StatusInternalServerError, serverError(err.Error()))
		return
	}

	requestData := &convert.RequestData{
		Model:         req.Model,
		Messages:      req.Messages,
		System:        req.System,
		MaxTokens:     req.MaxTokens,
		Temperature:   req.Temperature,
		TopP:          req.TopP,
		StopSequences: req.StopSequences,
		Tools:         req.Tools,
		ToolChoice:    req.ToolChoice,
		ServerTools:   req.ServerTools,

		SanitizeClientMetaTurns: cfg.SanitizeClientMetaTurns,
	}
	inputTokens := convert.EstimateInputTokens(req.Messages)

	// --- Server tool agentic loop: requests containing server tool types are
	// intercepted and handled by the agentic loop in agentic.go (web_search /
	// web_fetch executed proxy-side, results injected, final text streamed). ---
	if (cfg.ServerTools.WebSearch || cfg.ServerTools.WebFetch) && hasServerToolRequest(&req) {
		handleServerToolRequest(w, r, cfg, session, requestStart, requestData, apiKey, inputTokens)
		return
	}

	// --- Standard streaming flow (no server tools) ---
	url, wireBody, prettyBody, err := buildUpstreamRequest(cfg, requestData, apiKey)
	if err != nil {
		session.Finish()
		writeJSON(w, http.StatusInternalServerError, serverError(err.Error()))
		return
	}
	upstreamReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, url, bytes.NewReader(wireBody))
	if err != nil {
		session.Finish()
		writeJSON(w, http.StatusInternalServerError, serverError(err.Error()))
		return
	}
	upstreamReq.Header.Set("Content-Type", "application/json")
	upstreamReq.Header.Set("Authorization", "Bearer "+apiKey)
	session.WriteUpstreamRequest(headerMap(upstreamReq.Header), time.Now().UTC().Format(time.RFC3339), string(prettyBody))

	// The request context is the abort signal: the client's disconnect cancels
	// the upstream fetch. No client timeout — the ctx propagates.
	client := &http.Client{}
	upstreamRes, err := client.Do(upstreamReq)
	if err != nil {
		handleUpstreamConnectError(w, session, r, requestStart, err)
		return
	}
	defer upstreamRes.Body.Close()
	ttfb := time.Since(requestStart).Milliseconds()

	upstreamHeaders := headerMap(upstreamRes.Header)
	upstreamStatus := upstreamRes.StatusCode

	if upstreamRes.StatusCode < 200 || upstreamRes.StatusCode >= 300 {
		handleUpstreamErrorStatus(w, session, requestStart, upstreamRes, upstreamHeaders, upstreamStatus, ttfb)
		return
	}

	if upstreamRes.Body == nil || upstreamRes.Body == http.NoBody {
		handleUpstreamEmptyBody(w, session, requestStart, upstreamHeaders, upstreamStatus, ttfb)
		return
	}

	// --- Success: translate the upstream stream for the downstream client ---
	var rawUpstream strings.Builder
	upstreamBody := newStallWatchdog(upstreamRes.Body, upstreamRes.Body, time.Duration(cfg.MidStreamStallTimeout)*time.Second)
	chunks := openai.IterSSEChunks(r.Context(), upstreamBody, &rawUpstream)
	var downstreamAborted atomic.Bool

	streamer := stream.NewStreamer(r.Context(), chunks, requestData, inputTokens, cfg.EnableThinking, session,
		&stream.Options{
			IsDownstreamAborted: func() bool { return downstreamAborted.Load() },
			InvisibleTurnRetry:  emptyTurnRetryHook(r.Context(), cfg, session, requestData, apiKey),
			MidStreamRetry:      midStreamRetryHook(r.Context(), cfg, session, requestData, apiKey),
		})

	// Non-streaming client (stream omitted or false — the Anthropic default):
	// aggregate into ONE Message JSON. Claude Code's model-validation probe and
	// side queries call create() non-streaming and read message.usage
	// directly; answering SSE here crashed them with "Unable to validate
	// model: undefined is not an object (evaluating 'xt.usage.input_tokens')"
	// (dumped 2026-09-15).
	if req.Stream == nil || !*req.Stream {
		writeNonStreamMessages(w, r, session, requestStart, ttfb, upstreamHeaders, upstreamStatus, streamer, &rawUpstream)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	for k, v := range upstreamSSEHeaders {
		w.Header().Set(k, v)
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	flusher, ok := w.(http.Flusher)
	if !ok {
		session.Finish()
		return
	}

	var downstreamChunks []string

	// terminationReason is written by the pump goroutine (stream error
	// branch) and the select loop (client-abort branch) and read by
	// finalizeDump — a mutex serializes check-and-set so the downstream-abort
	// gate and the termination reason are updated atomically.
	var terminationMu sync.Mutex
	terminationReason := dump.Completed

	var dumpOnce sync.Once
	finalizeDump := func() {
		dumpOnce.Do(func() {
			terminationMu.Lock()
			reason := terminationReason
			terminationMu.Unlock()
			var term *dump.Termination
			if reason != dump.Completed {
				term = &dump.Termination{Reason: reason, DisconnectTime: time.Now().UTC().Format(time.RFC3339)}
			} else {
				term = &dump.Termination{Reason: reason}
			}
			// The upstream log records the TRUE upstream outcome when the
			// stream layer reported one (an abort swallowed to complete the
			// turn gracefully); otherwise the tracked downstream outcome.
			upstreamTerm := session.UpstreamTermination()
			if upstreamTerm == nil {
				upstreamTerm = term
			}
			session.WriteUpstreamResponse(upstreamHeaders, upstreamStatus, rawUpstream.String(), upstreamTerm)
			downstreamHeaders := map[string]string{"Content-Type": "text/event-stream"}
			for k, v := range upstreamSSEHeaders {
				downstreamHeaders[k] = v
			}
			session.WriteDownstreamResponse(downstreamHeaders, http.StatusOK, strings.Join(downstreamChunks, ""), term)
			session.SetTiming(ttfb, time.Since(requestStart).Milliseconds())
			session.Finish()
		})
	}

	evCh := make(chan string, 64) // pump goroutine → select loop
	pumpDone := make(chan struct{})
	go func() { // pump: consume streamer.Events()
		defer close(pumpDone)
		defer close(evCh)
		for ev := range streamer.Events() {
			select {
			case evCh <- ev:
			case <-r.Context().Done():
				return
			}
		}
		// Stream ended: classify the termination (no-output rethrow only).
		if err := streamer.Err(); err != nil {
			terminationMu.Lock()
			aborted := downstreamAborted.Load()
			if !aborted {
				terminationReason = dump.UpstreamAbort
			}
			terminationMu.Unlock()
			if aborted {
				return
			}
			var line string
			switch e := err.(type) {
			case *stream.UpstreamAbortedError:
				line = sse.BuildMidStreamErrorSse("overloaded_error", e.Error())
			case *stream.UpstreamStreamError:
				line = sse.BuildMidStreamErrorSse(sse.MapErrorType(int(e.Code)), e.Error())
			default:
				line = sse.BuildMidStreamErrorSse("api_error", err.Error())
			}
			select {
			case evCh <- line:
			case <-r.Context().Done():
			}
		}
	}()

	ticker := time.NewTicker(sse.DEFAULT_PING_INTERVAL)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ticker.C:
			if downstreamAborted.Load() {
				continue
			}
			writeEvent(w, flusher, sse.PING_EVENT, &downstreamChunks)
		case ev, ok := <-evCh:
			if !ok {
				break loop
			}
			writeEvent(w, flusher, ev, &downstreamChunks)
		case <-r.Context().Done():
			terminationMu.Lock()
			downstreamAborted.Store(true)
			terminationReason = dump.ClientAbort
			terminationMu.Unlock()
			break loop
		}
	}
	// The select loop is the only goroutine that writes w; the pump only
	// writes the channel. In the abort path the pump may be blocked reading
	// the upstream body — close it (TS reader.cancel()) and wait for the pump
	// to exit so finalizeDump's reads of the raw upstream text and the
	// session's recorded upstream termination are race-free.
	_ = upstreamRes.Body.Close()
	<-pumpDone
	finalizeDump()
}

// writeEvent appends one SSE event to the downstream chunk log and writes it
// to the client, flushing. Only called from the select loop goroutine.
func writeEvent(w http.ResponseWriter, flusher http.Flusher, event string, downstreamChunks *[]string) {
	*downstreamChunks = append(*downstreamChunks, event)
	_, _ = io.WriteString(w, event)
	flusher.Flush()
}

// writeNonStreamMessages drains the translated stream and answers ONE
// Anthropic Message JSON for non-streaming /v1/messages clients. Upstream
// stays streaming (usage accounting needs it) — only the downstream side
// aggregates. A mid-stream failure becomes an Anthropic error JSON; partial
// content is dropped, same as an API failure after the fact.
func writeNonStreamMessages(w http.ResponseWriter, r *http.Request, session *dump.Session, requestStart time.Time, ttfb int64, upstreamHeaders map[string]string, upstreamStatus int, streamer *stream.Streamer, rawUpstream *strings.Builder) {
	var events []string
	for ev := range streamer.Events() {
		events = append(events, ev)
	}

	finish := func(status int, body any, reason dump.TerminationReason) {
		term := &dump.Termination{Reason: reason}
		if reason == dump.ClientAbort {
			term.DisconnectTime = time.Now().UTC().Format(time.RFC3339)
		}
		session.WriteUpstreamResponse(upstreamHeaders, upstreamStatus, rawUpstream.String(), term)
		session.WriteDownstreamResponse(nil, status, jsonString(body), term)
		session.SetTiming(ttfb, time.Since(requestStart).Milliseconds())
		session.Finish()
	}

	if r.Context().Err() != nil {
		finish(499, nil, dump.ClientAbort)
		return
	}
	if err := streamer.Err(); err != nil {
		errType := "api_error"
		switch e := err.(type) {
		case *stream.UpstreamAbortedError:
			errType = "overloaded_error"
		case *stream.UpstreamStreamError:
			errType = sse.MapErrorType(int(e.Code))
		}
		body := errorBody(errType, err.Error())
		finish(http.StatusBadGateway, body, dump.UpstreamAbort)
		writeJSON(w, http.StatusBadGateway, body)
		return
	}
	msg := sse.AggregateMessage(events)
	finish(http.StatusOK, msg, dump.Completed)
	writeJSON(w, http.StatusOK, msg)
}

// handleUpstreamConnectError handles a client.Do failure (port of the TS
// fetch catch): 499 when the client disconnected, 502 otherwise.
func handleUpstreamConnectError(w http.ResponseWriter, session *dump.Session, r *http.Request, requestStart time.Time, connectErr error) {
	msg := connectErr.Error()
	disconnectTime := time.Now().UTC().Format(time.RFC3339)
	var status int
	var reason dump.TerminationReason
	var errBody apiErrorBody
	if r.Context().Err() != nil {
		status = 499
		reason = dump.ClientAbort
		errBody = upstreamError(fmt.Sprintf("Client disconnected before upstream responded: %s", msg))
	} else {
		status = http.StatusBadGateway
		reason = dump.UpstreamTimeout
		errBody = upstreamError(fmt.Sprintf("Failed to connect to upstream: %s", msg))
	}
	termination := &dump.Termination{Reason: reason, DisconnectTime: disconnectTime}
	session.WriteUpstreamResponse(nil, 0, "", termination)
	session.WriteDownstreamResponse(nil, status, jsonString(errBody), termination)
	session.Finish()
	writeJSON(w, status, errBody)
}

// handleUpstreamErrorStatus handles a non-2xx upstream response: 5xx maps to
// 502, other statuses pass through; the error body is truncated at 500 chars
// in the message (the dump keeps the full body).
func handleUpstreamErrorStatus(w http.ResponseWriter, session *dump.Session, requestStart time.Time, upstreamRes *http.Response, upstreamHeaders map[string]string, status int, ttfb int64) {
	body, _ := io.ReadAll(upstreamRes.Body)
	fullBody := string(body)
	disconnectTime := time.Now().UTC().Format(time.RFC3339)
	termination := &dump.Termination{Reason: dump.UpstreamError, DisconnectTime: disconnectTime}
	session.WriteUpstreamResponse(upstreamHeaders, status, fullBody, termination)

	mapped := status
	if status >= 500 {
		mapped = http.StatusBadGateway
	}
	errBody := upstreamError(fmt.Sprintf("Upstream returned %d: %s", status, truncate(fullBody, 500)))
	session.WriteDownstreamResponse(nil, mapped, jsonString(errBody), termination)
	session.SetTiming(ttfb, time.Since(requestStart).Milliseconds())
	session.Finish()
	writeJSON(w, mapped, errBody)
}

// handleUpstreamEmptyBody handles an upstream 2xx response with no body.
func handleUpstreamEmptyBody(w http.ResponseWriter, session *dump.Session, requestStart time.Time, upstreamHeaders map[string]string, status int, ttfb int64) {
	disconnectTime := time.Now().UTC().Format(time.RFC3339)
	termination := &dump.Termination{Reason: dump.UpstreamError, DisconnectTime: disconnectTime}
	session.WriteUpstreamResponse(upstreamHeaders, status, "", termination)
	errBody := serverError("Upstream returned empty body.")
	session.WriteDownstreamResponse(nil, http.StatusInternalServerError, jsonString(errBody), termination)
	session.SetTiming(ttfb, time.Since(requestStart).Milliseconds())
	session.Finish()
	writeJSON(w, http.StatusInternalServerError, errBody)
}

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

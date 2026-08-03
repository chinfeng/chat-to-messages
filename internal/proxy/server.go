// Package proxy implements the HTTP server layer of chat-to-messages: CORS
// and routing, downstream auth and API-key resolution, the upstream
// OpenAI-compatible request, the SSE pump (single select loop + pump
// goroutine), and dump session orchestration. Ported from
// chat-to-claude-code's src/server/routes.ts and src/server/index.ts.
package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chinfeng/chat-to-messages/internal/anthropic"
	"github.com/chinfeng/chat-to-messages/internal/config"
	"github.com/chinfeng/chat-to-messages/internal/convert"
	"github.com/chinfeng/chat-to-messages/internal/dump"
	"github.com/chinfeng/chat-to-messages/internal/openai"
	"github.com/chinfeng/chat-to-messages/internal/servertool"
	"github.com/chinfeng/chat-to-messages/internal/sse"
	"github.com/chinfeng/chat-to-messages/internal/stream"
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
// first (matching Bun.serve in index.ts), then routing.
func NewHandler(cfg *config.Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, x-api-key, anthropic-version")
			w.Header().Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		handleRequest(w, r, cfg)
	})
}

// handleRequest routes a request (port of routeRequest() in routes.ts).
func handleRequest(w http.ResponseWriter, r *http.Request, cfg *config.Config) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/health":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/messages":
		handleMessages(w, r, cfg)
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
	body, err := convert.BuildBaseRequestBody(requestData, 4096, convert.ReplayThinkTags)
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

// handleMessages handles POST /v1/messages (port of handleMessages() in
// routes.ts): auth, body validation, upstream request, and the SSE pump.
func handleMessages(w http.ResponseWriter, r *http.Request, cfg *config.Config) {
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

	// --- Success: stream the upstream SSE to the downstream client ---
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

	var rawUpstream strings.Builder
	chunks := openai.IterSSEChunks(r.Context(), upstreamRes.Body, &rawUpstream)
	var downstreamAborted atomic.Bool
	var downstreamChunks []string

	streamer := stream.NewStreamer(r.Context(), chunks, requestData, inputTokens, cfg.EnableThinking, session,
		&stream.Options{IsDownstreamAborted: func() bool { return downstreamAborted.Load() }})

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
			switch err.(type) {
			case *stream.UpstreamAbortedError:
				line = sse.BuildRetryableMidStreamErrorSse(err.Error())
			default:
				line = sse.BuildMidStreamErrorSse(err.Error())
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

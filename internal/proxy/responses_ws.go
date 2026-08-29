package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/chinfeng/chat-to-messages/internal/config"
	"github.com/chinfeng/chat-to-messages/internal/dump"
	"github.com/chinfeng/chat-to-messages/internal/openai"
	"github.com/chinfeng/chat-to-messages/internal/responses"
	"github.com/chinfeng/chat-to-messages/internal/ws"
)

// wsClientEnvelope peeks at the control fields of one client text frame; the
// same payload is then decoded into responses.Request for response.create.
type wsClientEnvelope struct {
	Type       string `json:"type"`
	Generate   *bool  `json:"generate"`    // response.create warmup flag (false = prime only)
	ResponseID string `json:"response_id"` // response.cancel target
}

// handleResponsesWS serves GET /v1/responses with a websocket upgrade —
// OpenAI's websocket mode. Client frames: response.create (the normal
// Responses request body + envelope fields) and response.cancel. Server
// frames: the same event JSON the SSE transport puts in `data:` lines, one
// event per text frame, plus {type:"error",status,error:{...}} frames for
// request-level failures. One response at a time per connection (clients
// serialize turns; a second create while busy gets an error frame).
func handleResponsesWS(w http.ResponseWriter, r *http.Request, cfg *config.Config, store *responses.Store) {
	// Pre-upgrade failures answer plain HTTP, same as the SSE transport.
	if !validateAuthToken(r, cfg) {
		writeJSON(w, http.StatusUnauthorized, openAIError("authentication_error", "Invalid auth token. Provide correct x-api-key header."))
		return
	}
	apiKey := resolveAPIKey(r, cfg)
	if apiKey == "" {
		writeJSON(w, http.StatusUnauthorized, openAIError("authentication_error", "No API key provided. Set --upstream-api-key or enable passthrough mode (no upstream key and no auth token)."))
		return
	}
	conn, err := ws.Upgrade(w, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, openAIError("invalid_request_error", "WebSocket upgrade failed: "+err.Error()))
		return
	}
	defer conn.Close()

	// One job in flight; the flag and the cancel ride the same mutex. Clearing
	// also cancels (covers both the finished-job and conn-dead paths).
	var jobMu sync.Mutex
	jobRunning := false
	var jobCancel context.CancelFunc
	clearJob := func() {
		jobMu.Lock()
		if jobCancel != nil {
			jobCancel()
		}
		jobRunning = false
		jobCancel = nil
		jobMu.Unlock()
	}
	defer clearJob()

	for {
		msg, err := conn.Read()
		if err != nil {
			return // closed or protocol error: the job's ctx dies via clearJob
		}
		if msg.Op == ws.OpClose {
			return
		}
		if msg.Op != ws.OpText {
			writeWSErrorFrame(conn, http.StatusBadRequest, "invalid_request_error", "", "Binary websocket frames are not supported; send text frames.")
			continue
		}
		var env wsClientEnvelope
		if err := json.Unmarshal(msg.Payload, &env); err != nil {
			writeWSErrorFrame(conn, http.StatusBadRequest, "invalid_request_error", "", "Invalid JSON in websocket message.")
			continue
		}
		switch env.Type {
		case "response.create":
			jobMu.Lock()
			busy := jobRunning
			jobMu.Unlock()
			if busy {
				writeWSErrorFrame(conn, http.StatusBadRequest, "invalid_request_error", "response_in_progress",
					"A response is already in progress on this connection; wait for its terminal event.")
				continue
			}
			ctx, cancel := context.WithCancel(context.Background())
			jobMu.Lock()
			jobRunning = true
			jobCancel = cancel
			jobMu.Unlock()
			generate := env.Generate == nil || *env.Generate
			payload := msg.Payload
			go func() {
				defer clearJob()
				runResponsesWSJob(ctx, cfg, store, conn, payload, generate, apiKey)
			}()
		case "response.cancel":
			// Cooperative stop: the job's upstream read aborts and the
			// translator closes out the turn with its terminal event. Codex
			// never sends this (it drops the connection instead); clients
			// that do (pipecat-style) expect a drainable terminal event,
			// which the normal Finish path provides.
			jobMu.Lock()
			active := jobCancel
			jobMu.Unlock()
			if active != nil {
				active()
			} else {
				writeWSErrorFrame(conn, http.StatusNotFound, "not_found_error", "", "No active response to cancel.")
			}
		default:
			writeWSErrorFrame(conn, http.StatusBadRequest, "invalid_request_error", "", "Unknown websocket message type '"+env.Type+"'.")
		}
	}
}

// runResponsesWSJob executes one response.create: convert → upstream
// chat/completions → translate → one text frame per Responses event.
// Mirrors handleResponses with HTTP status/JSON answers swapped for error
// frames. Dump bookkeeping is per-create (one session each).
func runResponsesWSJob(ctx context.Context, cfg *config.Config, store *responses.Store, conn *ws.Conn, frame []byte, generate bool, apiKey string) {
	session := dump.NewSession(cfg.DumpDir)
	requestStart := time.Now()

	var req responses.Request
	dec := json.NewDecoder(bytes.NewReader(frame))
	dec.UseNumber()
	if err := dec.Decode(&req); err != nil {
		session.Finish()
		writeWSErrorFrame(conn, http.StatusBadRequest, "invalid_request_error", "", "Invalid JSON in response.create message.")
		return
	}
	session.WriteDownstreamRequest(map[string]string{"transport": "websocket"}, time.Now().UTC().Format(time.RFC3339), string(frame))
	req.Stream = true // the websocket transport always streams events

	if req.Model == "" {
		session.Finish()
		writeWSErrorFrameParam(conn, http.StatusBadRequest, "invalid_request_error", "model", "", "`model` is required and must be a string.")
		return
	}

	up, err := buildResponsesUpstream(ctx, cfg, store, &req, apiKey)
	if err != nil {
		session.Finish()
		if ae, ok := err.(*responses.APIError); ok {
			// Codex retries with the full input when the store miss carries
			// the previous_response_not_found code.
			code := ""
			if ae.HTTPStatus == http.StatusNotFound && ae.Param == "previous_response_id" {
				code = "previous_response_not_found"
			}
			writeWSErrorFrameParam(conn, ae.HTTPStatus, ae.Type, ae.Param, code, ae.Message)
			return
		}
		writeWSErrorFrame(conn, http.StatusInternalServerError, "api_error", "", err.Error())
		return
	}
	session.WriteUpstreamRequest(headerMap(up.HTTP.Header), time.Now().UTC().Format(time.RFC3339), up.Wire)

	responseID := responses.NewID("resp_")
	tr := responses.NewTranslator(&req, responseID)
	var downstreamChunks []string
	emit := func(sseEvent string) {
		downstreamChunks = append(downstreamChunks, sseEvent)
		_ = conn.WriteText([]byte(sseDataPayload(sseEvent)))
	}

	// Warmup (generate:false from codex-style clients) primes the chain
	// without spending an upstream generation: the response completes empty.
	if !generate {
		for _, ev := range tr.Finish() {
			emit(ev)
		}
		storeWSChain(cfg, store, responseID, up, tr.OutputItems())
		term := &dump.Termination{Reason: dump.Completed}
		session.WriteUpstreamResponse(nil, 0, "", term)
		session.WriteDownstreamResponse(map[string]string{"Content-Type": "websocket/text-frames"}, http.StatusSwitchingProtocols, strings.Join(downstreamChunks, ""), term)
		session.SetTiming(0, time.Since(requestStart).Milliseconds())
		session.Finish()
		return
	}

	client := &http.Client{}
	upstreamRes, err := client.Do(up.HTTP)
	if err != nil {
		msg := err.Error()
		status := http.StatusBadGateway
		reason := dump.UpstreamTimeout
		if ctx.Err() != nil {
			status = 499
			reason = dump.ClientAbort
			msg = "Client disconnected before upstream responded: " + msg
		} else {
			msg = "Failed to connect to upstream: " + msg
		}
		termination := &dump.Termination{Reason: reason, DisconnectTime: time.Now().UTC().Format(time.RFC3339)}
		session.WriteUpstreamResponse(nil, 0, "", termination)
		session.WriteDownstreamResponse(nil, status, msg, termination)
		session.SetTiming(0, time.Since(requestStart).Milliseconds())
		session.Finish()
		writeWSErrorFrame(conn, status, "api_error", "", msg)
		return
	}
	defer upstreamRes.Body.Close()
	ttfb := time.Since(requestStart).Milliseconds()
	upstreamHeaders := headerMap(upstreamRes.Header)
	upstreamStatus := upstreamRes.StatusCode

	if upstreamRes.StatusCode < 200 || upstreamRes.StatusCode >= 300 {
		fullBody, _ := io.ReadAll(upstreamRes.Body)
		termination := &dump.Termination{Reason: dump.UpstreamError, DisconnectTime: time.Now().UTC().Format(time.RFC3339)}
		session.WriteUpstreamResponse(upstreamHeaders, upstreamStatus, string(fullBody), termination)
		mapped := upstreamStatus
		if upstreamStatus >= 500 {
			mapped = http.StatusBadGateway
		}
		msg := fmt.Sprintf("Upstream returned %d: %s", upstreamStatus, truncate(string(fullBody), 500))
		session.WriteDownstreamResponse(nil, mapped, msg, termination)
		session.SetTiming(ttfb, time.Since(requestStart).Milliseconds())
		session.Finish()
		writeWSErrorFrame(conn, mapped, "api_error", "", "Upstream error: "+msg)
		return
	}
	if upstreamRes.Body == nil || upstreamRes.Body == http.NoBody {
		termination := &dump.Termination{Reason: dump.UpstreamError, DisconnectTime: time.Now().UTC().Format(time.RFC3339)}
		session.WriteUpstreamResponse(upstreamHeaders, upstreamStatus, "", termination)
		session.SetTiming(ttfb, time.Since(requestStart).Milliseconds())
		session.Finish()
		writeWSErrorFrame(conn, http.StatusInternalServerError, "api_error", "", "Upstream returned empty body.")
		return
	}

	var rawBuf strings.Builder
	for chunk := range openai.IterSSEChunks(ctx, upstreamRes.Body, &rawBuf) {
		for _, ev := range tr.Process(chunk) {
			emit(ev)
		}
	}
	clientAborted := ctx.Err() != nil
	for _, ev := range tr.Finish() {
		emit(ev)
	}

	storeWSChain(cfg, store, responseID, up, tr.OutputItems())

	termination := dump.Completed
	switch {
	case clientAborted:
		termination = dump.ClientAbort
	case tr.Failure() != nil:
		termination = dump.UpstreamAbort
	}
	term := &dump.Termination{Reason: termination}
	if termination == dump.ClientAbort {
		term.DisconnectTime = time.Now().UTC().Format(time.RFC3339)
	}
	session.WriteUpstreamResponse(upstreamHeaders, upstreamStatus, rawBuf.String(), term)
	session.WriteDownstreamResponse(map[string]string{"Content-Type": "websocket/text-frames"}, http.StatusSwitchingProtocols, strings.Join(downstreamChunks, ""), term)
	session.SetTiming(ttfb, time.Since(requestStart).Milliseconds())
	session.Finish()
}

// storeWSChain persists the chain for previous_response_id. Unlike the HTTP
// transport the client's store:false is NOT honored: OpenAI's websocket mode
// keeps the chain in a connection-local cache precisely so clients can pass
// store:false yet still chain, and codex relies on that with the proxy.
func storeWSChain(cfg *config.Config, store *responses.Store, responseID string, up *responsesUpstream, output []any) {
	if cfg.ResponsesStoreTTLMinutes <= 0 {
		return
	}
	items := append(up.StoredPrefix, up.CurrentRaw...)
	items = append(items, output...)
	store.Put(responseID, items)
}

// sseDataPayload extracts the JSON payload of one formatted SSE event — the
// websocket wire shape is exactly that JSON object as one text frame.
func sseDataPayload(sseEvent string) string {
	for _, line := range strings.Split(sseEvent, "\n") {
		if rest, ok := strings.CutPrefix(line, "data: "); ok {
			return rest
		}
	}
	return "{}"
}

// writeWSErrorFrame sends an error frame in the shape the reference clients
// parse: {"type":"error","status":N,"error":{"type":...,"code":...,"message":...}}.
func writeWSErrorFrame(conn *ws.Conn, status int, errType, code, message string) {
	writeWSErrorFrameParam(conn, status, errType, "", code, message)
}

// writeWSErrorFrameParam adds the optional param field (validation UIs key
// off it, same as the HTTP error body).
func writeWSErrorFrameParam(conn *ws.Conn, status int, errType, param, code, message string) {
	errObj := map[string]any{"type": errType, "message": message}
	if code != "" {
		errObj["code"] = code
	}
	if param != "" {
		errObj["param"] = param
	}
	frame, _ := json.Marshal(map[string]any{
		"type":   "error",
		"status": status,
		"error":  errObj,
	})
	_ = conn.WriteText(frame)
}

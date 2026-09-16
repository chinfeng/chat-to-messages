package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/chinfeng/chat-to-messages/internal/config"
	"github.com/chinfeng/chat-to-messages/internal/convert"
	"github.com/chinfeng/chat-to-messages/internal/dump"
	"github.com/chinfeng/chat-to-messages/internal/hook/caption"
	"github.com/chinfeng/chat-to-messages/internal/openai"
	"github.com/chinfeng/chat-to-messages/internal/responses"
)

// handleResponses handles POST /v1/responses (spec:
// docs/superpowers/specs/2026-08-29-responses-endpoint-design.md): the
// OpenAI Responses dialect in, chat/completions out, Responses SSE back.
// Error bodies are OpenAI-flavored ({"error":{message,type,param}}) since the
// client speaks that dialect here.
func handleResponses(w http.ResponseWriter, r *http.Request, cfg *config.Config, store *responses.Store, imageHook *caption.Hook) {
	session := dump.NewSession(cfg.DumpDir)
	requestStart := time.Now()
	requestDatetime := time.Now().UTC().Format(time.RFC3339)

	if !validateAuthToken(r, cfg) {
		session.Finish()
		writeJSON(w, http.StatusUnauthorized, openAIError("authentication_error", "Invalid auth token. Provide correct x-api-key header."))
		return
	}
	apiKey := resolveAPIKey(r, cfg)
	if apiKey == "" {
		session.Finish()
		writeJSON(w, http.StatusUnauthorized, openAIError("authentication_error", "No API key provided. Set --upstream-api-key or enable passthrough mode (no upstream key and no auth token)."))
		return
	}

	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		session.Finish()
		writeJSON(w, http.StatusBadRequest, openAIError("invalid_request_error", "Invalid JSON in request body."))
		return
	}
	var req responses.Request
	dec := json.NewDecoder(bytes.NewReader(rawBody))
	dec.UseNumber()
	if err := dec.Decode(&req); err != nil {
		session.Finish()
		writeJSON(w, http.StatusBadRequest, openAIError("invalid_request_error", "Invalid JSON in request body."))
		return
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		session.Finish()
		writeJSON(w, http.StatusBadRequest, openAIError("invalid_request_error", "Invalid JSON in request body."))
		return
	}
	session.WriteDownstreamRequest(headerMap(r.Header), requestDatetime, string(rawBody))

	if req.Model == "" {
		session.Finish()
		writeJSON(w, http.StatusBadRequest, openAIParamError("invalid_request_error", "model", "`model` is required and must be a string."))
		return
	}

	// Expand previous_response_id, convert, and build the upstream request
	// (shared with the websocket transport — see responses_ws.go).
	up, err := buildResponsesUpstream(r.Context(), cfg, store, &req, apiKey, imageHook)
	if err != nil {
		session.Finish()
		if ae, ok := err.(*responses.APIError); ok {
			writeOpenAIAPIError(w, ae)
			return
		}
		writeJSON(w, http.StatusInternalServerError, openAIError("api_error", err.Error()))
		return
	}
	session.WriteUpstreamRequest(headerMap(up.HTTP.Header), time.Now().UTC().Format(time.RFC3339), up.Wire)

	client := &http.Client{}
	upstreamRes, err := client.Do(up.HTTP)
	if err != nil {
		msg := err.Error()
		disconnectTime := time.Now().UTC().Format(time.RFC3339)
		var status int
		var reason dump.TerminationReason
		if r.Context().Err() != nil {
			status = 499
			reason = dump.ClientAbort
			msg = "Client disconnected before upstream responded: " + msg
		} else {
			status = http.StatusBadGateway
			reason = dump.UpstreamTimeout
			msg = "Failed to connect to upstream: " + msg
		}
		termination := &dump.Termination{Reason: reason, DisconnectTime: disconnectTime}
		session.WriteUpstreamResponse(nil, 0, "", termination)
		errBody := openAIError("api_error", msg)
		session.WriteDownstreamResponse(nil, status, jsonString(errBody), termination)
		session.Finish()
		writeJSON(w, status, errBody)
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
		errBody := openAIError("api_error", fmt.Sprintf("Upstream returned %d: %s", upstreamStatus, truncate(string(fullBody), 500)))
		session.WriteDownstreamResponse(nil, mapped, jsonString(errBody), termination)
		session.SetTiming(ttfb, time.Since(requestStart).Milliseconds())
		session.Finish()
		writeJSON(w, mapped, errBody)
		return
	}
	if upstreamRes.Body == nil || upstreamRes.Body == http.NoBody {
		termination := &dump.Termination{Reason: dump.UpstreamError, DisconnectTime: time.Now().UTC().Format(time.RFC3339)}
		session.WriteUpstreamResponse(upstreamHeaders, upstreamStatus, "", termination)
		errBody := openAIError("api_error", "Upstream returned empty body.")
		session.WriteDownstreamResponse(nil, http.StatusInternalServerError, jsonString(errBody), termination)
		session.SetTiming(ttfb, time.Since(requestStart).Milliseconds())
		session.Finish()
		writeJSON(w, http.StatusInternalServerError, errBody)
		return
	}

	// --- Success: translate the upstream chunk stream into Responses events.
	// No ping keepalives here (the real Responses stream never pings; unknown
	// event types are a needless risk against strict downstream parsers) and
	// no pump goroutine: the handler drains IterSSEChunks directly, the client
	// disconnect cancels the read through r.Context().
	responseID := responses.NewID("resp_")
	tr := responses.NewTranslator(&req, responseID)
	var downstreamChunks []string

	var rawUpstream string
	var rawBuf strings.Builder
	emit := func(ev string) {
		downstreamChunks = append(downstreamChunks, ev)
	}

	clientAborted := false
	streamHeaders := func() {
		w.Header().Set("Content-Type", "text/event-stream")
		for k, v := range upstreamSSEHeaders {
			w.Header().Set(k, v)
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
	}

	var flusher http.Flusher
	if req.Stream {
		streamHeaders()
		w.WriteHeader(http.StatusOK)
		f, ok := w.(http.Flusher)
		if !ok {
			f = nil
		}
		flusher = f
	}
	write := func(ev string) {
		emit(ev)
		if flusher != nil {
			_, _ = io.WriteString(w, ev)
			flusher.Flush()
		}
	}

	for chunk := range openai.IterSSEChunks(r.Context(), upstreamRes.Body, &rawBuf) {
		for _, ev := range tr.Process(chunk) {
			write(ev)
		}
	}
	if r.Context().Err() != nil {
		clientAborted = true
	}
	rawUpstream = rawBuf.String()
	// Finish closes open items and emits the terminal event even on abort —
	// the writes are lost but the translator state (and the stored chain) end
	// up well-formed.
	for _, ev := range tr.Finish() {
		write(ev)
	}

	// Persist the expanded chain (input + output) for previous_response_id.
	// Storing even on abort/failed keeps OpenAI's "responses are retained"
	// semantics; expiry is the TTL's job.
	if cfg.ResponsesStoreTTLMinutes > 0 && (req.Store == nil || *req.Store) {
		items := append(up.StoredPrefix, up.CurrentRaw...)
		items = append(items, tr.OutputItems()...)
		store.Put(responseID, items)
	}

	// Termination classification for the dump.
	termination := dump.Completed
	switch {
	case clientAborted:
		termination = dump.ClientAbort
	case tr.Failure() != nil:
		termination = dump.UpstreamAbort
	}

	if req.Stream {
		term := &dump.Termination{Reason: termination}
		if termination == dump.ClientAbort {
			term.DisconnectTime = time.Now().UTC().Format(time.RFC3339)
		}
		session.WriteUpstreamResponse(upstreamHeaders, upstreamStatus, rawUpstream, term)
		downstreamHeaders := map[string]string{"Content-Type": "text/event-stream"}
		for k, v := range upstreamSSEHeaders {
			downstreamHeaders[k] = v
		}
		session.WriteDownstreamResponse(downstreamHeaders, http.StatusOK, strings.Join(downstreamChunks, ""), term)
		session.SetTiming(ttfb, time.Since(requestStart).Milliseconds())
		session.Finish()
		return
	}

	// Non-stream: one JSON response carrying the finalized response object.
	final := tr.FinalResponse()
	status := http.StatusOK
	var respBody any = final
	if f := tr.Failure(); f != nil {
		// The upstream died mid-body with no events sent: the client only sees
		// HTTP — answer an error like a normal OpenAI failure would.
		status = http.StatusInternalServerError
		respBody = openAIError("api_error", "Upstream error: "+f.Message)
	}
	term := &dump.Termination{Reason: termination}
	if termination == dump.ClientAbort {
		term.DisconnectTime = time.Now().UTC().Format(time.RFC3339)
	}
	session.WriteUpstreamResponse(upstreamHeaders, upstreamStatus, rawUpstream, term)
	session.WriteDownstreamResponse(nil, status, jsonString(respBody), term)
	session.SetTiming(ttfb, time.Since(requestStart).Milliseconds())
	session.Finish()
	writeJSON(w, status, respBody)
}

// openAIParamError builds an OpenAI error body with the param field set when
// non-empty (OpenAI clients key validation UIs off it).
func openAIParamError(errType, param, message string) openAIErrorBody {
	b := openAIError(errType, message)
	if param != "" {
		b.Error.Param = param
	}
	return b
}

// writeOpenAIAPIError answers an responses.APIError (400/404 with typed
// param) in the OpenAI error shape.
func writeOpenAIAPIError(w http.ResponseWriter, ae *responses.APIError) {
	writeJSON(w, ae.HTTPStatus, openAIParamError(ae.Type, ae.Param, ae.Message))
}

// responsesUpstream is everything needed to fire the chat/completions call
// for one Responses request: the built http request plus the store-chain
// bookkeeping (StoredPrefix+CurrentRaw get persisted under the new response
// id together with the output items the translator produces).
type responsesUpstream struct {
	HTTP         *http.Request
	Wire         string // pretty-printed body, for the dump
	StoredPrefix []any  // raw items expanded from previous_response_id
	CurrentRaw   []any  // raw items of the current request
}

// buildResponsesUpstream expands previous_response_id into the history,
// converts the request to a chat/completions body (stream forced on — usage
// accounting needs it even for stream:false clients — model extras merged,
// include_usage applied, canonicalized) and wraps it in an http request.
// Missing/invalid client input comes back as *responses.APIError
// (400/404); anything else is a plain error the caller maps to 500.
// Shared by the SSE transport (handleResponses) and the websocket transport
// (responses_ws.go).
//
// imageHook, when non-nil and applicable to the request's model, captions the
// input_image parts across the assembled history (current input + stored
// chain) before conversion. It runs here so both transports get it.
func buildResponsesUpstream(ctx context.Context, cfg *config.Config, store *responses.Store, req *responses.Request, apiKey string, imageHook *caption.Hook) (*responsesUpstream, error) {
	// Expand previous_response_id through the proxy store. The full
	// conversation lives client-side otherwise; a missing entry is a 404,
	// never a silent restart (spec §响应存储).
	var history []responses.Item
	var storedPrefix []any
	if req.PreviousResponseID != "" {
		raw, ok := store.Get(req.PreviousResponseID)
		if !ok {
			return nil, &responses.APIError{HTTPStatus: http.StatusNotFound, Type: "not_found_error", Param: "previous_response_id",
				Message: "Previous response '" + req.PreviousResponseID + "' not found (expired, or stored=false, or minted by another proxy instance)."}
		}
		items, err := responses.ItemsFromRaw(raw)
		if err != nil {
			return nil, err
		}
		history = items
		storedPrefix = raw
	}
	// The current request's items trail the expanded history (BuildChatBody's
	// ordering contract).
	history = append(history, req.Input.Items...)

	// Caption images for non-visual models before conversion: the assembled
	// history is the full logical input, so one pass covers both the stored
	// chain and the current turn. Only `history` is mutated — ItemsFromRaw
	// hands back re-decoded copies, so the STORE keeps the original
	// image-bearing items (a later turn re-captions from cache, or replays the
	// image untouched if the model is later ruled visual) and CurrentRaw, which
	// is what the store persists for this turn, keeps its images too.
	if imageHook != nil {
		if err := imageHook.CaptionItems(ctx, req.Model, history); err != nil {
			return nil, err
		}
	}

	var currentRaw []any
	if req.Input.IsString {
		if strings.TrimSpace(req.Input.Str) != "" {
			currentRaw = []any{map[string]any{"type": "message", "role": "user", "content": req.Input.Str}}
		}
	} else {
		currentRaw = req.Input.Raw
	}

	// Reasoning replay is per-model, same rule table as /v1/messages.
	replayMode := convert.ReasoningReplayMode(config.ResolveReasoningReplay(req.Model, cfg.ReasoningReplayRules, cfg.DefaultReasoningReplay))
	body, err := responses.BuildChatBody(req, history, replayMode)
	if err != nil {
		return nil, err
	}
	body["stream"] = true // upstream always streams, even for stream:false clients (usage needs it)
	if extra := config.ResolveModelExtra(req.Model, cfg.ModelOverrides); len(extra) > 0 {
		body = config.DeepMerge(body, extra)
	}
	applyIncludeUsage(body)
	body = convert.PrepareCanonicalBody(body)

	wireBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	prettyBody, _ := json.MarshalIndent(body, "", "  ")

	url := strings.TrimRight(cfg.UpstreamBaseURL, "/") + "/chat/completions"
	upstreamReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(wireBody))
	if err != nil {
		return nil, err
	}
	upstreamReq.Header.Set("Content-Type", "application/json")
	upstreamReq.Header.Set("Authorization", "Bearer "+apiKey)
	return &responsesUpstream{
		HTTP:         upstreamReq,
		Wire:         string(prettyBody),
		StoredPrefix: storedPrefix,
		CurrentRaw:   currentRaw,
	}, nil
}

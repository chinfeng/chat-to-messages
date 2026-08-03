// Agentic loop for server-tool requests, ported from
// handleServerToolRequest() in ../chat-to-claude-code/src/server/routes.ts.
//
// When server tools (web_search / web_fetch) are enabled and the request
// contains server tool types, this flow iterates: sends the request upstream,
// intercepts web_search/web_fetch tool calls (native protocol or text-embedded
// fallback), executes them proxy-side, appends the results to the message
// history, and re-requests the upstream until the model produces a final text
// response. The final response is streamed downstream with the tool results
// rendered as TEXT content blocks — never as server_tool_use events — to avoid
// triggering Claude Code's domain-safety verification against claude.ai (which
// fails when claude.ai is unreachable).
package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"chat-to-messages/internal/config"
	"chat-to-messages/internal/convert"
	"chat-to-messages/internal/dump"
	"chat-to-messages/internal/openai"
	"chat-to-messages/internal/servertool"
	"chat-to-messages/internal/sse"
	"chat-to-messages/internal/stream"
)

// maxIterations bounds the agentic loop (MAX_ITERATIONS in routes.ts).
const maxIterations = 5

// nowISO renders the current time like TS new Date().toISOString().
func nowISO() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

// uuidV4 returns a crypto/rand UUID v4-shaped string (like TS randomUUID).
func uuidV4() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("proxy: crypto/rand unavailable: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// srvtoolRandID mirrors `srvtool_${randomUUID().slice(0, 12)}` (text-embedded
// fallback path).
func srvtoolRandID() string {
	return "srvtool_" + uuidV4()[:12]
}

// srvtoolNowID mirrors `srvtool_${Date.now()}` (millisecond timestamp, used in
// executeServerToolCall's returned message).
func srvtoolNowID() string {
	return "srvtool_" + strconv.FormatInt(time.Now().UnixMilli(), 10)
}

// tsString approximates TS String() for JSON-decoded values (callers apply the
// TS `?? ""` null-coercion before calling).
func tsString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return fmt.Sprint(t)
	}
}

// nilOrText mirrors the TS `textContent || null` coercion for assistant
// message content.
func nilOrText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// toAny converts []map[string]any to []any (TS arrays of objects).
func toAny(msgs []map[string]any) []any {
	out := make([]any, len(msgs))
	for i, m := range msgs {
		out[i] = m
	}
	return out
}

// isServerToolCall mirrors isServerToolCall() in routes.ts: whether a tool
// call name is a server tool the proxy should intercept (and is enabled).
func isServerToolCall(name string, cfg config.ServerToolConfig) bool {
	if name == "web_search" && cfg.WebSearch {
		return true
	}
	if name == "web_fetch" && cfg.WebFetch {
		return true
	}
	return false
}

// collectedToolCall mirrors the TS CollectedToolCall interface.
type collectedToolCall struct {
	index     int
	id        string
	name      string
	arguments string
}

// collectResult mirrors the TS CollectResult interface.
type collectResult struct {
	toolCalls         []collectedToolCall
	finishReason      string
	hasServerToolCall bool
	textContent       string
}

// collectToolCallArguments mirrors collectToolCallArguments() in routes.ts:
// consumes an upstream chunk stream and aggregates tool call info (index →
// id/name/arguments concatenation), the finish reason and the accumulated
// text. Usage chunks are ignored (the TS collect does not collect them
// either). Read-error chunks are treated as stream end (the TS throws and
// crashes the request — the Go analog degrades to whatever was collected).
func collectToolCallArguments(chunks iter.Seq[openai.Chunk]) collectResult {
	byIndex := make(map[int]*collectedToolCall)
	var order []int // insertion order, for deterministic results (TS Map)
	var finishReason string
	var textContent string

	for chunk := range chunks {
		if len(chunk.Choices) == 0 {
			continue
		}
		choice := chunk.Choices[0]
		delta := choice.Delta
		if delta == nil {
			continue
		}
		if choice.FinishReason != nil && *choice.FinishReason != "" {
			finishReason = *choice.FinishReason
		}
		if delta.Content != nil {
			textContent += *delta.Content
		}
		for _, tc := range delta.ToolCalls {
			existing, ok := byIndex[tc.Index]
			if !ok {
				existing = &collectedToolCall{index: tc.Index}
				byIndex[tc.Index] = existing
				order = append(order, tc.Index)
			}
			if tc.ID != nil {
				existing.id = *tc.ID
			}
			if tc.Function.Name != nil {
				existing.name = *tc.Function.Name
			}
			if tc.Function.Arguments != nil {
				existing.arguments += *tc.Function.Arguments
			}
		}
	}

	toolCalls := make([]collectedToolCall, 0, len(order))
	hasServerToolCall := false
	for _, idx := range order {
		tc := byIndex[idx]
		toolCalls = append(toolCalls, *tc)
		if tc.name == "web_search" || tc.name == "web_fetch" {
			hasServerToolCall = true
		}
	}
	return collectResult{toolCalls: toolCalls, finishReason: finishReason, hasServerToolCall: hasServerToolCall, textContent: textContent}
}

// serverToolMessage mirrors the return value of executeServerToolCall() in
// routes.ts (role/tool_call_id/content of an OpenAI-format tool message).
type serverToolMessage struct {
	Role       string
	ToolCallID string
	Content    string
}

// executeServerToolCall mirrors executeServerToolCall() in routes.ts: executes
// a web_search/web_fetch call proxy-side and formats the result as an
// OpenAI-format tool message; unknown tool names produce an error object.
// Tool execution uses context.Background() exactly like the TS fetch() calls
// (no abort signal).
func executeServerToolCall(toolName, argumentsJSON string, cfg config.ServerToolConfig, onLog servertool.LogFn) serverToolMessage {
	var input map[string]any
	if err := json.Unmarshal([]byte(argumentsJSON), &input); err != nil {
		input = map[string]any{}
	}

	if toolName == "web_search" && cfg.WebSearch {
		query := ""
		if v, ok := input["query"]; ok && v != nil {
			query = tsString(v)
		}
		results := servertool.ExecuteWebSearch(context.Background(), query, cfg, onLog)
		contentBlocks := servertool.FormatWebSearchResultContent(results)
		return serverToolMessage{Role: "tool", ToolCallID: srvtoolNowID(), Content: jsonString(contentBlocks)}
	}

	if toolName == "web_fetch" && cfg.WebFetch {
		url := ""
		if v, ok := input["url"]; ok && v != nil {
			url = tsString(v)
		}
		result := servertool.ExecuteWebFetch(context.Background(), url, cfg, onLog)
		contentBlocks := servertool.FormatWebFetchResultContent(result)
		return serverToolMessage{Role: "tool", ToolCallID: srvtoolNowID(), Content: jsonString(contentBlocks)}
	}

	return serverToolMessage{Role: "tool", ToolCallID: srvtoolNowID(), Content: jsonString(map[string]any{"error": "Unknown server tool: " + toolName})}
}

// serverToolEvent records one executed server tool and its result, mirroring
// the TS ServerToolEvent union. The events drive the downstream text-block
// conversion; server_tool_use entries are never emitted downstream.
type serverToolEvent struct {
	kind      string // "server_tool_use" | "web_search_tool_result" | "web_fetch_tool_result"
	toolUseID string
	toolName  string
	input     map[string]any
	content   []map[string]any
	status    string
}

// postUpstream POSTs a JSON body to the upstream chat/completions endpoint
// with the given headers and request context (port of the TS fetch call).
func postUpstream(ctx context.Context, url string, headers map[string]string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return (&http.Client{}).Do(req)
}

// handleAgenticLoopFetchError handles a connect failure inside the agentic
// loop (port of the TS loop fetch catch): 499 + client_abort when the client
// disconnected, 502 + upstream_timeout otherwise.
func handleAgenticLoopFetchError(w http.ResponseWriter, session *dump.Session, r *http.Request, iteration int, connectErr error) {
	msg := connectErr.Error()
	disconnectTime := nowISO()
	if r.Context().Err() != nil {
		termination := &dump.Termination{Reason: dump.ClientAbort, DisconnectTime: disconnectTime}
		errBody := upstreamError("Client disconnected: " + msg)
		session.WriteDownstreamResponse(nil, 499, jsonString(errBody), termination)
		session.Finish()
		writeJSON(w, 499, errBody)
		return
	}
	termination := &dump.Termination{Reason: dump.UpstreamTimeout, DisconnectTime: disconnectTime}
	session.WriteUpstreamResponse(nil, 0, "", termination)
	errBody := upstreamError(fmt.Sprintf("Upstream fetch failed (iteration %d): %s", iteration, msg))
	session.WriteDownstreamResponse(nil, http.StatusBadGateway, jsonString(errBody), termination)
	session.Finish()
	writeJSON(w, http.StatusBadGateway, errBody)
}

// handleAgenticLoopErrorStatus handles a non-2xx upstream response inside the
// agentic loop (port of the TS loop !upstreamRes.ok branch): 5xx maps to 502,
// other statuses pass through; the error body is truncated at 500 chars in the
// message (the dump keeps the full body).
func handleAgenticLoopErrorStatus(w http.ResponseWriter, session *dump.Session, upstreamRes *http.Response) {
	body, _ := io.ReadAll(upstreamRes.Body)
	upstreamRes.Body.Close()
	fullBody := string(body)
	disconnectTime := nowISO()
	termination := &dump.Termination{Reason: dump.UpstreamError, DisconnectTime: disconnectTime}
	session.WriteUpstreamResponse(headerMap(upstreamRes.Header), upstreamRes.StatusCode, fullBody, termination)

	mapped := upstreamRes.StatusCode
	if mapped >= 500 {
		mapped = http.StatusBadGateway
	}
	errBody := upstreamError(fmt.Sprintf("Upstream returned %d: %s", upstreamRes.StatusCode, truncate(fullBody, 500)))
	session.WriteDownstreamResponse(nil, mapped, jsonString(errBody), termination)
	session.Finish()
	writeJSON(w, mapped, errBody)
}

// handleServerToolRequest runs the agentic loop for requests that contain
// server tools (port of handleServerToolRequest() in routes.ts).
func handleServerToolRequest(w http.ResponseWriter, r *http.Request, cfg *config.Config, session *dump.Session, requestStart time.Time, requestData *convert.RequestData, apiKey string, inputTokens int64) {
	onLog := session.LogServerTool

	// Build the initial upstream request body (the full body — Claude Code
	// puts server tools in the tools array, so the schemas ride along).
	upstreamURL, initialBody, err := buildUpstreamRequestBodyOnly(cfg, requestData)
	if err != nil {
		session.Finish()
		writeJSON(w, http.StatusInternalServerError, serverError(err.Error()))
		return
	}
	upstreamHeadersObj := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + apiKey,
	}
	session.WriteUpstreamRequest(upstreamHeadersObj, nowISO(), string(initialBody))

	// Parse the initial body to get the messages array (we append to it in
	// the loop) and the tools array.
	var initialBodyParsed map[string]any
	if err := json.Unmarshal(initialBody, &initialBodyParsed); err != nil {
		session.Finish()
		writeJSON(w, http.StatusInternalServerError, serverError(err.Error()))
		return
	}
	upstreamMessages, _ := initialBodyParsed["messages"].([]any)
	upstreamTools := initialBodyParsed["tools"]

	var serverToolEvents []serverToolEvent
	iteration := 0

	// === Agentic loop ===
	for iteration < maxIterations {
		iteration++

		maxTokens := any(32000)
		if requestData.MaxTokens != nil {
			maxTokens = requestData.MaxTokens
		}
		// Loop bodies force thinking enabled and request include_usage,
		// independent of the initial body (TS prepareCanonicalBody call).
		loopBody := map[string]any{
			"model":          requestData.Model,
			"messages":       upstreamMessages,
			"max_tokens":     maxTokens,
			"stream":         true,
			"stream_options": map[string]any{"include_usage": true},
			"thinking":       map[string]any{"type": "enabled"},
		}
		if upstreamTools != nil {
			loopBody["tools"] = upstreamTools
		}
		loopBody = convert.PrepareCanonicalBody(loopBody)
		currentBody, err := json.Marshal(loopBody)
		if err != nil {
			session.Finish()
			writeJSON(w, http.StatusInternalServerError, serverError(err.Error()))
			return
		}

		// Log each iteration's upstream request.
		ms := time.Since(requestStart).Milliseconds()
		session.LogServerTool(dump.ServerToolLogEntry{
			Tool:       "agentic_loop",
			Timestamp:  nowISO(),
			Input:      fmt.Sprintf("iteration %d, messages: %d", iteration, len(upstreamMessages)),
			Engine:     "proxy",
			DurationMs: &ms,
		})

		upstreamRes, err := postUpstream(r.Context(), upstreamURL, upstreamHeadersObj, currentBody)
		if err != nil {
			handleAgenticLoopFetchError(w, session, r, iteration, err)
			return
		}
		if upstreamRes.StatusCode < 200 || upstreamRes.StatusCode >= 300 {
			handleAgenticLoopErrorStatus(w, session, upstreamRes)
			return
		}
		if upstreamRes.Body == nil || upstreamRes.Body == http.NoBody {
			// TS: dump.finish() + 500, no upstream/downstream response logs.
			session.Finish()
			writeJSON(w, http.StatusInternalServerError, serverError("Upstream returned empty body in agentic loop."))
			return
		}

		// Read the full upstream response and collect tool call info.
		chunks := openai.IterSSEChunks(r.Context(), upstreamRes.Body, nil)
		collect := collectToolCallArguments(chunks)
		_ = upstreamRes.Body.Close()

		// Path A: standard OpenAI tool_calls in the response.
		var serverToolCalls []collectedToolCall
		for _, tc := range collect.toolCalls {
			if isServerToolCall(tc.name, cfg.ServerTools) {
				serverToolCalls = append(serverToolCalls, tc)
			}
		}

		// Path B: fallback — detect tool calls embedded in text content
		// (some models, e.g. GLM, don't use the tool_calls protocol).
		var textToolCalls []servertool.DetectedTextToolCall
		if len(serverToolCalls) == 0 && collect.textContent != "" {
			textToolCalls = servertool.DetectServerToolInText(collect.textContent)
			if len(textToolCalls) > 0 {
				var types []string
				for _, tc := range textToolCalls {
					types = append(types, tc.Type)
				}
				ms := time.Since(requestStart).Milliseconds()
				session.LogServerTool(dump.ServerToolLogEntry{
					Tool:       "agentic_loop",
					Timestamp:  nowISO(),
					Input:      fmt.Sprintf("detected %d text-embedded tool call(s): %s", len(textToolCalls), strings.Join(types, ", ")),
					Engine:     "text_fallback",
					DurationMs: &ms,
				})
			}
		}

		// If neither path found server tool calls, this is the final text
		// response.
		if len(serverToolCalls) == 0 && len(textToolCalls) == 0 {
			break
		}

		// --- Execute standard (OpenAI protocol) server tool calls ---
		if len(serverToolCalls) > 0 {
			assistantToolCalls := make([]any, 0, len(collect.toolCalls))
			for _, tc := range collect.toolCalls {
				assistantToolCalls = append(assistantToolCalls, map[string]any{
					"id":   tc.id,
					"type": "function",
					"function": map[string]any{
						"name":      tc.name,
						"arguments": tc.arguments,
					},
				})
			}
			upstreamMessages = append(upstreamMessages, map[string]any{
				"role":       "assistant",
				"content":    nilOrText(collect.textContent),
				"tool_calls": assistantToolCalls,
			})

			for _, tc := range collect.toolCalls {
				if isServerToolCall(tc.name, cfg.ServerTools) {
					toolResult := executeServerToolCall(tc.name, tc.arguments, cfg.ServerTools, onLog)

					toolUseID := tc.id
					if toolUseID == "" {
						toolUseID = srvtoolRandID()
					}
					var input map[string]any
					if err := json.Unmarshal([]byte(tc.arguments), &input); err != nil {
						input = map[string]any{}
					}

					serverToolEvents = append(serverToolEvents, serverToolEvent{kind: "server_tool_use", toolUseID: toolUseID, toolName: tc.name, input: input})

					var contentBlocks []map[string]any
					if err := json.Unmarshal([]byte(toolResult.Content), &contentBlocks); err != nil {
						contentBlocks = nil
					}
					if tc.name == "web_search" {
						serverToolEvents = append(serverToolEvents, serverToolEvent{kind: "web_search_tool_result", toolUseID: toolUseID, content: contentBlocks})
					} else if tc.name == "web_fetch" {
						status := ""
						for _, b := range contentBlocks {
							if s, ok := b["text"].(string); ok && strings.HasPrefix(s, "Status: 4") {
								status = "error"
								break
							}
						}
						serverToolEvents = append(serverToolEvents, serverToolEvent{kind: "web_fetch_tool_result", toolUseID: toolUseID, content: contentBlocks, status: status})
					}

					upstreamMessages = append(upstreamMessages, map[string]any{
						"role":         "tool",
						"tool_call_id": tc.id,
						"content":      toolResult.Content,
					})
				} else {
					upstreamMessages = append(upstreamMessages, map[string]any{
						"role":         "tool",
						"tool_call_id": tc.id,
						"content":      "Tool execution not supported in server tool mode.",
					})
				}
			}
		}

		// --- Execute text-embedded (fallback) server tool calls ---
		if len(textToolCalls) > 0 {
			// The model output tool_use tags inline in its text, then
			// continued with hallucinated "results". Strip the tags and
			// everything after them, then inject real tool results.
			cleanText := servertool.StripToolUseFromText(collect.textContent)
			synthesizeToolCalls := make([]map[string]any, 0, len(textToolCalls))
			for _, tc := range textToolCalls {
				argsJSON, _ := json.Marshal(tc.Input)
				synthesizeToolCalls = append(synthesizeToolCalls, map[string]any{
					"id":   srvtoolRandID(),
					"type": "function",
					"function": map[string]any{
						"name":      tc.Type,
						"arguments": string(argsJSON),
					},
				})
			}
			upstreamMessages = append(upstreamMessages, map[string]any{
				"role":       "assistant",
				"content":    nilOrText(cleanText),
				"tool_calls": toAny(synthesizeToolCalls),
			})

			for i, tc := range textToolCalls {
				fakeID := synthesizeToolCalls[i]["id"].(string)
				argsJSON, _ := json.Marshal(tc.Input)
				toolResult := executeServerToolCall(tc.Type, string(argsJSON), cfg.ServerTools, onLog)

				serverToolEvents = append(serverToolEvents, serverToolEvent{kind: "server_tool_use", toolUseID: fakeID, toolName: tc.Type, input: tc.Input})

				var contentBlocks []map[string]any
				if err := json.Unmarshal([]byte(toolResult.Content), &contentBlocks); err != nil {
					contentBlocks = nil
				}
				if tc.Type == "web_search" {
					serverToolEvents = append(serverToolEvents, serverToolEvent{kind: "web_search_tool_result", toolUseID: fakeID, content: contentBlocks})
				} else if tc.Type == "web_fetch" {
					status := ""
					for _, b := range contentBlocks {
						if s, ok := b["text"].(string); ok && strings.HasPrefix(s, "Status: 4") {
							status = "error"
							break
						}
					}
					serverToolEvents = append(serverToolEvents, serverToolEvent{kind: "web_fetch_tool_result", toolUseID: fakeID, content: contentBlocks, status: status})
				}

				upstreamMessages = append(upstreamMessages, map[string]any{
					"role":         "tool",
					"tool_call_id": fakeID,
					"content":      toolResult.Content,
				})
			}
		}
	}

	// === Final upstream request to get the streaming text response ===
	maxTokens := any(32000)
	if requestData.MaxTokens != nil {
		maxTokens = requestData.MaxTokens
	}
	finalBody := map[string]any{
		"model":          requestData.Model,
		"messages":       upstreamMessages,
		"max_tokens":     maxTokens,
		"stream":         true,
		"stream_options": map[string]any{"include_usage": true},
		"thinking":       map[string]any{"type": "enabled"},
	}
	if upstreamTools != nil {
		finalBody["tools"] = upstreamTools
	}
	finalBody = convert.PrepareCanonicalBody(finalBody)
	finalWire, err := json.Marshal(finalBody)
	if err != nil {
		session.Finish()
		writeJSON(w, http.StatusInternalServerError, serverError(err.Error()))
		return
	}

	finalRes, err := postUpstream(r.Context(), upstreamURL, upstreamHeadersObj, finalWire)
	if err != nil {
		// TS: upstream_timeout dump + finish, then a plain 502 (no downstream
		// response log on this path).
		termination := &dump.Termination{Reason: dump.UpstreamTimeout, DisconnectTime: nowISO()}
		session.WriteUpstreamResponse(nil, 0, "", termination)
		session.Finish()
		errBody := upstreamError("Final upstream request failed: " + err.Error())
		writeJSON(w, http.StatusBadGateway, errBody)
		return
	}
	if finalRes.StatusCode < 200 || finalRes.StatusCode >= 300 {
		body, _ := io.ReadAll(finalRes.Body)
		finalRes.Body.Close()
		errBody := string(body)
		session.WriteUpstreamResponse(headerMap(finalRes.Header), finalRes.StatusCode, errBody, nil)
		short := upstreamError(fmt.Sprintf("Final upstream returned %d", finalRes.StatusCode))
		session.WriteDownstreamResponse(nil, http.StatusBadGateway, jsonString(short), nil)
		session.Finish()
		// The response actually returned to the client carries the truncated
		// upstream body (TS uses the long message here).
		full := upstreamError(fmt.Sprintf("Final upstream returned %d: %s", finalRes.StatusCode, truncate(errBody, 500)))
		writeJSON(w, http.StatusBadGateway, full)
		return
	}
	if finalRes.Body == nil || finalRes.Body == http.NoBody {
		session.Finish()
		writeJSON(w, http.StatusInternalServerError, serverError("Final upstream returned empty body."))
		return
	}
	defer finalRes.Body.Close()

	// === Stream the response to downstream ===
	downstreamHeaders := map[string]string{"Content-Type": "text/event-stream"}
	for k, v := range upstreamSSEHeaders {
		downstreamHeaders[k] = v
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
	var emitMu sync.Mutex
	emit := func(ev string) {
		emitMu.Lock()
		downstreamChunks = append(downstreamChunks, ev)
		_, _ = io.WriteString(w, ev)
		flusher.Flush()
		emitMu.Unlock()
	}

	// Ping heartbeat (TS pingInterval) interleaved with the stream; stopped
	// and joined before the handler returns so the chunk log is race-free.
	pingDone := make(chan struct{})
	var pingWG sync.WaitGroup
	pingWG.Add(1)
	go func() {
		defer pingWG.Done()
		ticker := time.NewTicker(sse.DEFAULT_PING_INTERVAL)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				emit(sse.PING_EVENT)
			case <-pingDone:
				return
			}
		}
	}()
	defer pingWG.Wait()
	defer close(pingDone)

	builder := sse.NewBuilder("msg_"+uuidV4(), requestData.Model, inputTokens, nil)

	// 1. message_start
	emit(builder.MessageStart())

	// 2. Tool results as text content blocks instead of server_tool_use +
	//    tool_result events (prevents Claude Code from verifying domain
	//    safety via claude.ai).
	var toolResultTexts []string
	for _, ev := range serverToolEvents {
		switch ev.kind {
		case "server_tool_use":
			// Skip — do NOT emit server_tool_use to downstream. The proxy has
			// already executed the tool; Claude Code must not try to verify or
			// execute it again.
		case "web_search_tool_result":
			var searchResults []string
			for _, b := range ev.content {
				if b["type"] == "web_search_result" {
					url := tsString(b["url"])
					title := tsString(b["title"])
					snippet := tsString(b["snippet"])
					if snippet != "" {
						searchResults = append(searchResults, title+"\n"+url+"\n"+snippet)
					} else {
						searchResults = append(searchResults, title+"\n"+url)
					}
				}
			}
			if joined := strings.Join(searchResults, "\n\n"); joined != "" {
				toolResultTexts = append(toolResultTexts, "[Web Search Results]\n"+joined)
			}
		case "web_fetch_tool_result":
			var fetchText []string
			for _, b := range ev.content {
				if b["type"] == "text" {
					if s, ok := b["text"].(string); ok {
						fetchText = append(fetchText, s)
					}
				}
			}
			if len(fetchText) > 0 {
				toolResultTexts = append(toolResultTexts, "[Web Fetch Result]\n"+strings.Join(fetchText, "\n"))
			}
		}
	}
	if len(toolResultTexts) > 0 {
		for _, ev := range builder.EnsureTextBlock() {
			emit(ev)
		}
		emit(builder.TextDelta(strings.Join(toolResultTexts, "\n\n")))
		for _, ev := range builder.CloseContentBlocks() {
			emit(ev)
		}
	}

	// 3. Stream the final upstream response (text content from the model).
	//    finalBody requests stream_options.include_usage, so the upstream sends
	//    a trailing usage chunk; tee the raw chunks to capture usage and feed
	//    REAL input/output token counts into the outer message_delta (G1+G4).
	var rawFinal strings.Builder
	finalChunks := openai.IterSSEChunks(r.Context(), finalRes.Body, &rawFinal)
	var finalUsage *sse.UsageInfo
	next, stop := iter.Pull(finalChunks)
	teeChunks := func(yield func(openai.Chunk) bool) {
		for {
			c, ok := next()
			if !ok {
				return
			}
			if c.Usage != nil {
				if u := stream.ExtractUsageInfo(c.Usage); u != nil {
					finalUsage = u
				}
			}
			if !yield(c) {
				stop()
				return
			}
		}
	}

	inner := stream.NewStreamer(r.Context(), teeChunks, requestData, inputTokens, cfg.EnableThinking, session,
		&stream.Options{SkipMessageLifecycle: true, StartingBlockIndex: builder.NextIndex()})
	for ev := range inner.Events() {
		emit(ev)
	}

	// Mid-stream failure: emit a top-level `event: error` (retry split by
	// abort kind) and record the upstream_abort dump — no
	// message_delta/message_stop, so the partial is discarded.
	if err := inner.Err(); err != nil {
		if r.Context().Err() == nil {
			var line string
			switch err.(type) {
			case *stream.UpstreamAbortedError:
				line = sse.BuildRetryableMidStreamErrorSse(err.Error())
			default:
				line = sse.BuildMidStreamErrorSse(err.Error())
			}
			emit(line)
		}
		session.WriteDownstreamResponse(downstreamHeaders, http.StatusOK, strings.Join(downstreamChunks, ""),
			&dump.Termination{Reason: dump.UpstreamAbort, DisconnectTime: nowISO()})
		session.Finish()
		return
	}

	// 4. message_delta and message_stop. With include_usage the message_delta
	//    reports REAL usage (G4); falls back to the estimate when the upstream
	//    sent no usage chunk.
	if finalUsage != nil {
		builder.SetUsage(*finalUsage)
	}
	completion := builder.EstimateOutputTokens()
	if finalUsage != nil {
		completion = finalUsage.CompletionTokens
	}
	emit(builder.MessageDelta("end_turn", &completion, nil))
	emit(builder.MessageStop())

	// 5. Finalize dump
	session.WriteUpstreamResponse(headerMap(finalRes.Header), finalRes.StatusCode, rawFinal.String(),
		&dump.Termination{Reason: dump.Completed})
	session.WriteDownstreamResponse(downstreamHeaders, http.StatusOK, strings.Join(downstreamChunks, ""),
		&dump.Termination{Reason: dump.Completed})
	elapsed := time.Since(requestStart).Milliseconds()
	session.SetTiming(elapsed, elapsed)
	session.Finish()
}

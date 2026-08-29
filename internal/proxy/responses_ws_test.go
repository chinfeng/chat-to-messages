package proxy

// End-to-end coverage of the websocket transport on /v1/responses: the wire
// shape (one event JSON per text frame), response.create chains over a
// single connection, error frames, cancel, and the generate:false warmup.
// The test client side is a hand-rolled masked-frame RFC 6455 client (the ws
// package only implements the server side).

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chinfeng/chat-to-messages/internal/config"
)

type wsTestClient struct {
	conn net.Conn
	br   *bufio.Reader
}

func dialProxyWS(t *testing.T, baseHTTPURL string) *wsTestClient {
	t.Helper()
	addr := strings.TrimPrefix(baseHTTPURL, "http://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString([]byte("proxy-test-key-1"))
	req := fmt.Sprintf("GET /v1/responses HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", addr, key)
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	status, _ := br.ReadString('\n')
	if !strings.Contains(status, "101") {
		t.Fatalf("handshake status: %q", status)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil || line == "\r\n" {
			break
		}
	}
	return &wsTestClient{conn: conn, br: br}
}

func (c *wsTestClient) send(t *testing.T, v any) {
	t.Helper()
	payload, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	c.sendRaw(t, payload)
}

// sendRaw writes one masked text frame (clients MUST mask, RFC 6455 §5.3).
func (c *wsTestClient) sendRaw(t *testing.T, payload []byte) {
	t.Helper()
	hdr := []byte{0x81}
	switch n := len(payload); {
	case n <= 125:
		hdr = append(hdr, 0x80|byte(n))
	case n <= 0xFFFF:
		hdr = append(hdr, 0x80|126, byte(n>>8), byte(n))
	default:
		hdr = append(hdr, 0x80|127)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		hdr = append(hdr, b[:]...)
	}
	mask := [4]byte{9, 8, 7, 6}
	hdr = append(hdr, mask[:]...)
	out := make([]byte, len(payload))
	for i := range payload {
		out[i] = payload[i] ^ mask[i&3]
	}
	if _, err := c.conn.Write(append(hdr, out...)); err != nil {
		t.Fatal(err)
	}
}

// recv reads one server text frame as decoded JSON.
func (c *wsTestClient) recv(t *testing.T) map[string]any {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var hdr [2]byte
	readN(t, c.br, hdr[:])
	if op := hdr[0] & 0x0f; op != 0x1 {
		t.Fatalf("expected text frame, got opcode %d", op)
	}
	n := uint64(hdr[1] & 0x7f)
	switch n {
	case 126:
		var ext [2]byte
		readN(t, c.br, ext[:])
		n = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		readN(t, c.br, ext[:])
		n = binary.BigEndian.Uint64(ext[:])
	}
	payload := make([]byte, n)
	readN(t, c.br, payload)
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatalf("frame is not JSON: %q", payload)
	}
	return m
}

func readN(t *testing.T, br *bufio.Reader, buf []byte) {
	t.Helper()
	got := 0
	for got < len(buf) {
		n, err := br.Read(buf[got:])
		got += n
		if err != nil {
			t.Fatal(err)
		}
	}
}

// collectEvents reads frames until a terminal response event (completed /
// incomplete / failed) arrives and returns them all.
func (c *wsTestClient) collectEvents(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for {
		m := c.recv(t)
		out = append(out, m)
		switch m["type"] {
		case "response.completed", "response.incomplete", "response.failed":
			return out
		}
	}
}

func wsEventTypes(events []map[string]any) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i], _ = e["type"].(string)
	}
	return out
}

func wsHasEvent(events []map[string]any, typ string) bool {
	for _, e := range events {
		if e["type"] == typ {
			return true
		}
	}
	return false
}

// TestResponsesWSEndToEnd runs the same two-turn tool chain as the SSE test
// but over a single websocket connection.
func TestResponsesWSEndToEnd(t *testing.T) {
	upstream := &sseUpstreamScript{script: []string{toolTurnSSE, textTurnSSE}}
	up := httptest.NewServer(upstream.handler())
	defer up.Close()
	cfg := &config.Config{
		UpstreamBaseURL:          up.URL,
		UpstreamAPIKey:           "sk-upstream",
		ResponsesStoreTTLMinutes: 1440,
	}
	proxy := httptest.NewServer(NewHandler(cfg))
	defer proxy.Close()

	c := dialProxyWS(t, proxy.URL)
	defer c.conn.Close()

	// Turn 1: tool call.
	c.send(t, map[string]any{"type": "response.create", "model": "test-model", "input": "weather in Paris?"})
	events := c.collectEvents(t)
	for _, want := range []string{
		"response.created", "response.in_progress", "response.output_item.added",
		"response.function_call_arguments.done", "response.output_item.done", "response.completed",
	} {
		if !wsHasEvent(events, want) {
			t.Fatalf("turn 1 missing %q in %v", want, wsEventTypes(events))
		}
	}
	completed := events[len(events)-1]
	respObj, _ := completed["response"].(map[string]any)
	respID, _ := respObj["id"].(string)
	if !strings.HasPrefix(respID, "resp_") || respObj["status"] != "completed" {
		t.Fatalf("turn 1 response object: %v", respObj)
	}
	var callID string
	for _, e := range events {
		if e["type"] != "response.output_item.done" {
			continue
		}
		item, _ := e["item"].(map[string]any)
		if item["type"] == "function_call" {
			callID, _ = item["call_id"].(string)
		}
	}
	if !strings.HasPrefix(callID, "call_") {
		t.Fatalf("minted call_id missing: %v", wsEventTypes(events))
	}
	raw := fmt.Sprintf("%v", events)
	if strings.Contains(raw, "chatcmpl-upstream-id") {
		t.Fatal("upstream tool id leaked into websocket frames")
	}

	// Turn 2: same connection, chain via previous_response_id + tool output.
	c.send(t, map[string]any{
		"type":                 "response.create",
		"model":                "test-model",
		"previous_response_id": respID,
		"input": []any{map[string]any{
			"type": "function_call_output", "call_id": callID, "output": "sunny",
		}},
	})
	events = c.collectEvents(t)
	if !wsHasEvent(events, "response.completed") {
		t.Fatalf("turn 2 did not complete: %v", wsEventTypes(events))
	}

	// The upstream must have seen the joined chain with the minted call_id.
	body := upstream.lastBody(t)
	msgs, _ := body["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("upstream turn 2 messages: %v", body["messages"])
	}
	toolCalls, _ := msgs[1].(map[string]any)["tool_calls"].([]any)
	if len(toolCalls) != 1 || toolCalls[0].(map[string]any)["id"] != callID {
		t.Fatalf("assistant tool_call id must be the minted %s: %v", callID, toolCalls)
	}
	toolMsg, _ := msgs[2].(map[string]any)
	if toolMsg["role"] != "tool" || toolMsg["tool_call_id"] != callID || toolMsg["content"] != "sunny" {
		t.Fatalf("tool result message broken: %v", toolMsg)
	}
}

func TestResponsesWSErrorFrames(t *testing.T) {
	upstream := &sseUpstreamScript{script: []string{textTurnSSE}}
	up := httptest.NewServer(upstream.handler())
	defer up.Close()
	cfg := &config.Config{UpstreamBaseURL: up.URL, UpstreamAPIKey: "sk", ResponsesStoreTTLMinutes: 1440}
	proxy := httptest.NewServer(NewHandler(cfg))
	defer proxy.Close()

	c := dialProxyWS(t, proxy.URL)
	defer c.conn.Close()

	expectError := func(frame map[string]any, status float64, code string) {
		t.Helper()
		if frame["type"] != "error" {
			t.Fatalf("want error frame, got %v", frame)
		}
		if frame["status"] != status {
			t.Fatalf("error status: got %v want %v (%v)", frame["status"], status, frame)
		}
		if code != "" {
			errObj, _ := frame["error"].(map[string]any)
			if errObj["code"] != code {
				t.Fatalf("error code: got %v want %q", errObj, code)
			}
		}
	}

	// Malformed JSON.
	c.sendRaw(t, []byte("{not json"))
	expectError(c.recv(t), 400.0, "")
	// Unknown message type.
	c.send(t, map[string]any{"type": "response.frobnicate"})
	expectError(c.recv(t), 400.0, "")
	// Unknown previous_response_id → retryable code codex keys on.
	c.send(t, map[string]any{"type": "response.create", "model": "m", "input": "x", "previous_response_id": "resp_nope"})
	expectError(c.recv(t), 404.0, "previous_response_not_found")
	// Missing model.
	c.send(t, map[string]any{"type": "response.create", "input": "x"})
	expectError(c.recv(t), 400.0, "")
}

// slowUpstream streams SSE chunks with delays so a response stays in flight
// long enough to test busy rejection and cancel below.
func slowUpstream(chunks int, delay time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < chunks; i++ {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(delay):
			}
			_, _ = fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"word%d \"}}]}\n\n", i)
			if flusher != nil {
				flusher.Flush()
			}
		}
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
		if flusher != nil {
			flusher.Flush()
		}
	})
}

func TestResponsesWSBusyAndCancel(t *testing.T) {
	up := httptest.NewServer(slowUpstream(20, 80*time.Millisecond))
	defer up.Close()
	cfg := &config.Config{UpstreamBaseURL: up.URL, UpstreamAPIKey: "sk", ResponsesStoreTTLMinutes: 1440}
	proxy := httptest.NewServer(NewHandler(cfg))
	defer proxy.Close()

	c := dialProxyWS(t, proxy.URL)
	defer c.conn.Close()

	create := map[string]any{"type": "response.create", "model": "m", "input": "say words"}
	c.send(t, create)
	c.send(t, create) // second create while the first streams → busy error

	// Expect the busy error frame somewhere in the first response's stream.
	sawBusy := false
	sawTerminal := false
	deadline := time.Now().Add(8 * time.Second)
	for !(sawBusy && sawTerminal) && time.Now().Before(deadline) {
		m := c.recv(t)
		if m["type"] == "error" {
			errObj, _ := m["error"].(map[string]any)
			if errObj["code"] == "response_in_progress" {
				sawBusy = true
			}
		}
		if m["type"] == "response.completed" || m["type"] == "response.incomplete" || m["type"] == "response.failed" {
			sawTerminal = true
		}
	}
	if !sawBusy || !sawTerminal {
		t.Fatalf("busy=%v terminal=%v", sawBusy, sawTerminal)
	}

	// Cancel a response mid-flight: the turn ends with a terminal event and
	// the connection must stay usable.
	c.send(t, create)
	time.Sleep(150 * time.Millisecond) // let it start streaming
	c.send(t, map[string]any{"type": "response.cancel", "response_id": "resp_anything"})
	sawTerminal = false
	deadline = time.Now().Add(8 * time.Second)
	for !sawTerminal && time.Now().Before(deadline) {
		m := c.recv(t)
		switch m["type"] {
		case "response.completed", "response.incomplete", "response.failed":
			sawTerminal = true
		}
	}
	if !sawTerminal {
		t.Fatal("cancelled response produced no terminal event")
	}

	// Connection still usable after the cancelled run.
	c.send(t, create)
	if events := c.collectEvents(t); !wsHasEvent(events, "response.completed") {
		t.Fatalf("post-cancel create did not complete: %v", wsEventTypes(events))
	}
}

// TestResponsesWSWarmup: generate:false completes without touching the
// upstream, yet the chain is stored so the next turn can reference it.
func TestResponsesWSWarmup(t *testing.T) {
	upstream := &sseUpstreamScript{script: []string{textTurnSSE}}
	up := httptest.NewServer(upstream.handler())
	defer up.Close()
	cfg := &config.Config{UpstreamBaseURL: up.URL, UpstreamAPIKey: "sk", ResponsesStoreTTLMinutes: 1440}
	proxy := httptest.NewServer(NewHandler(cfg))
	defer proxy.Close()

	c := dialProxyWS(t, proxy.URL)
	defer c.conn.Close()

	c.send(t, map[string]any{
		"type": "response.create", "model": "m",
		"input": "warmup context", "generate": false,
	})
	events := c.collectEvents(t)
	if !wsHasEvent(events, "response.completed") {
		t.Fatalf("warmup did not complete: %v", wsEventTypes(events))
	}
	if len(upstream.bodies) != 0 {
		t.Fatal("generate:false must not contact the upstream")
	}
	completed := events[len(events)-1]
	respID := completed["response"].(map[string]any)["id"].(string)

	// The next turn chains off the warmup response; the upstream body carries
	// the warmup input.
	c.send(t, map[string]any{
		"type": "response.create", "model": "m", "previous_response_id": respID, "input": "real question",
	})
	if events := c.collectEvents(t); !wsHasEvent(events, "response.completed") {
		t.Fatalf("chained create did not complete: %v", wsEventTypes(events))
	}
	body := upstream.lastBody(t)
	if !strings.Contains(fmt.Sprintf("%v", body["messages"]), "warmup context") {
		t.Fatalf("chained upstream body must include the warmup input: %v", body["messages"])
	}
}

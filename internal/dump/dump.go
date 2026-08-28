// Package dump implements request/response dump logging for debugging,
// ported from ../chat-to-claude-code/src/core/dump.ts.
//
// A Session writes one request/response cycle's logs under
// <dir>/in-progress/<id>/, then Finish() renames the directory into a
// classification bucket ("completed", "client-aborted", "upstream-aborted"
// or "failed") whose name reflects the ROOT CAUSE of the termination.
package dump

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// TerminationReason is the root cause of a request termination.
type TerminationReason string

const (
	Completed       TerminationReason = "completed"
	ClientAbort     TerminationReason = "client_abort"
	UpstreamTimeout TerminationReason = "upstream_timeout"
	UpstreamError   TerminationReason = "upstream_error"
	UpstreamAbort   TerminationReason = "upstream_abort"
)

// Termination records how a request ended.
type Termination struct {
	Reason         TerminationReason
	DisconnectTime string
}

// ServerToolLogEntry is one server-tool call (web_search / web_fetch /
// agentic_loop) logged to server-tools.log. Optional fields are only emitted
// when set (pointers/booleans follow the TS `!== undefined` semantics).
type ServerToolLogEntry struct {
	Tool            string
	Timestamp       string
	Input           string
	Engine          string
	RequestURL      string
	RequestHeaders  map[string]string
	Status          *int
	ResponseHeaders map[string]string
	ResponseBody    string
	ResultCount     *int
	Skipped         bool
	SkipReason      string
	Error           string
	DurationMs      *int64
}

type timing struct {
	ttfb      int64
	totalTime int64
}

// Session is the debug dump for a single request/response cycle. All file
// operations fail silently (TS `catch {}`). Like the TS original, it assumes
// single-request-single-goroutine use and is not mutex-protected.
type Session struct {
	noop      bool // dir == "" → every method is a no-op
	dir       string
	id        string
	tmpDir    string
	startTime time.Time
	finished  bool
	tracked   TerminationReason
	upstream  *Termination
	timing    *timing
	toolLogs  []string
}

// NewSession creates a dump session. A session writes to
// <dir>/in-progress/<id>/, where id is a time-ordered UUID v7
// (xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx). Sorting directory names therefore
// orders sessions by request start time. dir == "" returns a no-op session
// whose methods do nothing.
func NewSession(dir string) *Session {
	if dir == "" {
		return &Session{noop: true}
	}
	id := newID()
	tmpDir := filepath.Join(dir, "in-progress", id)
	// TS: mkdirSync(dumpDir, {recursive:true}) + mkdirSync(tmpDir, ...), both
	// errors swallowed. MkdirAll of the tmpDir covers the parent as well.
	_ = os.MkdirAll(tmpDir, 0o755)
	return &Session{dir: dir, id: id, tmpDir: tmpDir, startTime: time.Now()}
}

// WriteDownstreamRequest logs the downstream (client → proxy) request.
func (s *Session) WriteDownstreamRequest(headers map[string]string, datetime, body string) {
	if s.noop {
		return
	}
	_ = os.WriteFile(filepath.Join(s.tmpDir, "downstream-request.log"), []byte(formatRequestLog(headers, datetime, body)), 0o644)
}

// WriteUpstreamRequest logs the upstream (proxy → provider) request.
func (s *Session) WriteUpstreamRequest(headers map[string]string, datetime, body string) {
	if s.noop {
		return
	}
	_ = os.WriteFile(filepath.Join(s.tmpDir, "upstream-request.log"), []byte(formatRequestLog(headers, datetime, body)), 0o644)
}

// WriteUpstreamAttemptRequest logs the request of an empty-turn-guard retry
// attempt n (2-based: attempt 1 is the primary request already logged by
// WriteUpstreamRequest). Numbered sibling files keep the full retry history
// for forensics of this failure class.
func (s *Session) WriteUpstreamAttemptRequest(n int, headers map[string]string, datetime, body string) {
	if s.noop || n < 2 {
		return
	}
	name := fmt.Sprintf("upstream-request-attempt-%d.log", n)
	_ = os.WriteFile(filepath.Join(s.tmpDir, name), []byte(formatRequestLog(headers, datetime, body)), 0o644)
}

// WriteUpstreamAttemptResponse logs the response of an empty-turn-guard retry
// attempt n (same numbering as WriteUpstreamAttemptRequest).
func (s *Session) WriteUpstreamAttemptResponse(n int, headers map[string]string, status int, body string) {
	if s.noop || n < 2 {
		return
	}
	name := fmt.Sprintf("upstream-response-attempt-%d.log", n)
	_ = os.WriteFile(filepath.Join(s.tmpDir, name), []byte(formatResponseLog(headers, status, body, nil, nil)), 0o644)
}

// WriteUpstreamResponse logs the upstream response. A non-nil termination
// becomes the tracked downstream outcome.
func (s *Session) WriteUpstreamResponse(headers map[string]string, status int, body string, termination *Termination) {
	if termination != nil && termination.Reason != "" {
		s.tracked = termination.Reason
	}
	if s.noop {
		return
	}
	_ = os.WriteFile(filepath.Join(s.tmpDir, "upstream-response.log"), []byte(formatResponseLog(headers, status, body, termination, nil)), 0o644)
}

// WriteDownstreamResponse logs the downstream response, including the timing
// set via SetTiming (read at write time, like TS) and any termination.
func (s *Session) WriteDownstreamResponse(headers map[string]string, status int, body string, termination *Termination) {
	if termination != nil && termination.Reason != "" {
		s.tracked = termination.Reason
	}
	if s.noop {
		return
	}
	_ = os.WriteFile(filepath.Join(s.tmpDir, "downstream-response.log"), []byte(formatResponseLog(headers, status, body, termination, s.timing)), 0o644)
}

// SetTiming records response timing (ms) for the downstream response log.
func (s *Session) SetTiming(ttfb, totalTime int64) {
	if s.noop {
		return
	}
	s.timing = &timing{ttfb: ttfb, totalTime: totalTime}
}

// LogServerTool appends one server-tool call entry to server-tools.log.
func (s *Session) LogServerTool(entry ServerToolLogEntry) {
	if s.noop {
		return
	}
	s.toolLogs = append(s.toolLogs, formatServerToolEntry(entry))
}

// RecordUpstreamTermination records the TRUE upstream outcome, reported by
// the stream layer (e.g. an upstream abort the stream layer swallowed to
// complete the downstream turn gracefully). No-op once the session has
// finished, so a later cancel()/finish() cannot overwrite it.
func (s *Session) RecordUpstreamTermination(reason TerminationReason, disconnectTime string) {
	if s.noop || s.finished {
		return
	}
	s.upstream = &Termination{Reason: reason, DisconnectTime: disconnectTime}
}

// UpstreamTermination returns the recorded upstream outcome, if the stream
// layer reported one.
func (s *Session) UpstreamTermination() *Termination {
	return s.upstream
}

// Finish closes the session: writes server-tools.log if any tool entries were
// collected, then renames the in-progress directory into the classification
// bucket. The bucket reflects the ROOT CAUSE, which can differ from the
// tracked downstream outcome (see pickTerminationReason). Idempotent.
func (s *Session) Finish() {
	if s.noop || s.finished {
		return
	}
	s.finished = true
	if len(s.toolLogs) > 0 {
		_ = os.WriteFile(filepath.Join(s.tmpDir, "server-tools.log"), []byte(strings.Join(s.toolLogs, "\n")), 0o644)
	}
	endTime := time.Now()
	finalName := s.id + "__START_" + formatTime(s.startTime) + "__END_" + formatTime(endTime)
	targetDir := filepath.Join(s.dir, getTargetSubdir(pickTerminationReason(s.tracked, s.upstream)))
	_ = os.MkdirAll(targetDir, 0o755)
	_ = os.Rename(s.tmpDir, filepath.Join(targetDir, finalName))
}

// pickTerminationReason decides the dump bucket's root-cause termination. The
// reasons come from two places: the route's downstream outcome (`tracked` —
// what happened to the downstream turn) and the stream layer's separate
// report of what the UPSTREAM did (`upstream`). They describe different
// halves of the same request and can disagree:
//
//   - Upstream aborts after content: the stream layer completes the
//     downstream turn gracefully (partial preserved + notice + message_stop),
//     so `tracked` = "completed"; but the upstream never sent finish_reason /
//     [DONE], so `upstream` = upstream_abort. The root cause is the upstream
//     abort → the bucket must reflect `upstream`.
//   - Client disconnect (downstream cancels mid-stream): `tracked` =
//     "client_abort"; the stream-layer read teardown that follows may look
//     like an upstream abort, but the CLIENT initiated it → client_abort must
//     win over any upstream report.
//
// Precedence: a downstream-initiated client disconnect beats everything;
// then the upstream's own recorded outcome; then the tracked downstream
// outcome. When nothing was recorded at all, the zero value is returned
// (TS undefined) and getTargetSubdir's default branch lands the session in
// "failed" — verbatim TS behavior.
func pickTerminationReason(tracked TerminationReason, upstream *Termination) TerminationReason {
	if tracked == ClientAbort {
		return ClientAbort
	}
	if upstream != nil {
		return upstream.Reason
	}
	return tracked
}

func getTargetSubdir(reason TerminationReason) string {
	switch reason {
	case Completed:
		return "completed"
	case ClientAbort:
		return "client-aborted"
	case UpstreamAbort:
		return "upstream-aborted"
	case UpstreamTimeout, UpstreamError:
		return "failed"
	default:
		return "failed"
	}
}

// formatTime renders an ISO8601 UTC timestamp with ':' and '.' replaced by
// '-', matching TS `d.toISOString().replace(/[:.]/g, "-")`.
func formatTime(t time.Time) string {
	return strings.NewReplacer(":", "-", ".", "-").Replace(t.UTC().Format("2006-01-02T15:04:05.000Z"))
}

// formatHeaders renders "k: v" lines. Keys are sorted for deterministic
// output (TS preserves insertion order; Go maps are unordered — the ordering
// is a Go-inherent semantic equivalent, as noted in prior task reports).
func formatHeaders(headers map[string]string) string {
	if len(headers) == 0 {
		return ""
	}
	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(k)
		b.WriteString(": ")
		b.WriteString(headers[k])
	}
	return b.String()
}

func formatSection(label, content string) string {
	return fmt.Sprintf("[%s]\n%s\n\n", label, content)
}

func formatRequestLog(headers map[string]string, datetime, body string) string {
	var out string
	out += formatSection("Request DateTime", datetime)
	out += formatSection("Request Headers", formatHeaders(headers))
	out += formatSection("Request Body", body)
	return out
}

func formatTermination(t *Termination) string {
	out := "Reason: " + string(t.Reason)
	if t.DisconnectTime != "" {
		out += "\nDisconnectTime: " + t.DisconnectTime
	}
	return out
}

func formatResponseLog(headers map[string]string, status int, body string, termination *Termination, t *timing) string {
	var out string
	out += formatSection("Response Status", strconv.Itoa(status))
	out += formatSection("Response Headers", formatHeaders(headers))
	out += formatSection("Response Body", body)
	if termination != nil {
		out += formatSection("Termination", formatTermination(termination))
	}
	if t != nil {
		out += formatSection("Timing", fmt.Sprintf("TTFB: %dms\nTotal: %dms", t.ttfb, t.totalTime))
	}
	return out
}

func formatServerToolEntry(e ServerToolLogEntry) string {
	var out string
	out += formatSection("Tool", e.Tool)
	out += formatSection("Timestamp", e.Timestamp)
	out += formatSection("Input", e.Input)
	if e.Engine != "" {
		out += formatSection("Engine", e.Engine)
	}
	if e.RequestURL != "" {
		out += formatSection("Request URL", e.RequestURL)
	}
	if len(e.RequestHeaders) > 0 {
		out += formatSection("Request Headers", formatHeaders(e.RequestHeaders))
	}
	if e.Status != nil {
		out += formatSection("Status", strconv.Itoa(*e.Status))
	}
	if len(e.ResponseHeaders) > 0 {
		out += formatSection("Response Headers", formatHeaders(e.ResponseHeaders))
	}
	if e.ResponseBody != "" {
		out += formatSection("Response Body", e.ResponseBody)
	}
	if e.ResultCount != nil {
		out += formatSection("Result Count", strconv.Itoa(*e.ResultCount))
	}
	if e.Skipped {
		out += formatSection("Skipped", "true")
		if e.SkipReason != "" {
			out += formatSection("Skip Reason", e.SkipReason)
		}
	}
	if e.Error != "" {
		out += formatSection("Error", e.Error)
	}
	if e.DurationMs != nil {
		out += formatSection("Duration", strconv.FormatInt(*e.DurationMs, 10)+"ms")
	}
	out += "---\n"
	return out
}

// newID returns a time-ordered UUID v7 id (RFC 9562, method 1). The leading
// 48 bits are the Unix timestamp in milliseconds, so ids sort
// lexicographically into request-start order. rand.Read failure panics,
// mirroring TS randomUUIDv7 throwing on entropy failure.
func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("dump: crypto/rand unavailable: " + err.Error())
	}
	ts := uint64(time.Now().UnixMilli())
	b[0] = byte(ts >> 40)
	b[1] = byte(ts >> 32)
	b[2] = byte(ts >> 24)
	b[3] = byte(ts >> 16)
	b[4] = byte(ts >> 8)
	b[5] = byte(ts)
	b[6] = (b[6] & 0x0f) | 0x70 // version 7
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

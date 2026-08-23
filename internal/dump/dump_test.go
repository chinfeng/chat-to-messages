package dump

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNoopSessionWhenDirEmpty(t *testing.T) {
	s := NewSession("")
	s.WriteDownstreamRequest(nil, "ts", "body") // 不得 panic
	s.Finish()
}

func TestSessionWritesFourLogsAndMovesToCompleted(t *testing.T) {
	dir := t.TempDir()
	s := NewSession(dir)
	s.WriteDownstreamRequest(map[string]string{"x-api-key": "k"}, "2026-08-03T00:00:00Z", `{"model":"m"}`)
	s.WriteUpstreamRequest(nil, "2026-08-03T00:00:00Z", `{"model":"m","stream":true}`)
	s.SetTiming(50, 120)
	s.WriteUpstreamResponse(nil, 200, "data: [DONE]", nil)
	s.WriteDownstreamResponse(nil, 200, "event: message_stop", &Termination{Reason: Completed})
	s.Finish()

	entries, err := os.ReadDir(filepath.Join(dir, "completed"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("completed entries = %d", len(entries))
	}
	sub := filepath.Join(dir, "completed", entries[0].Name())
	for _, f := range []string{"downstream-request.log", "downstream-response.log", "upstream-request.log", "upstream-response.log"} {
		if _, err := os.Stat(filepath.Join(sub, f)); err != nil {
			t.Errorf("missing %s: %v", f, err)
		}
	}
	b, _ := os.ReadFile(filepath.Join(sub, "downstream-request.log"))
	if !strings.Contains(string(b), "[Request DateTime]") || !strings.Contains(string(b), "x-api-key: k") {
		t.Errorf("log content: %s", b)
	}
	b2, _ := os.ReadFile(filepath.Join(sub, "downstream-response.log"))
	if !strings.Contains(string(b2), "TTFB: 50ms") || !strings.Contains(string(b2), "Total: 120ms") {
		t.Errorf("timing: %s", b2)
	}
}

func TestTerminationBuckets(t *testing.T) {
	// Brief 原文把 dir := t.TempDir() 放在循环外，各 case 共享同一目录；
	// 但 timeout/upstream_error 均映射到 "failed"、completed/none 均映射到
	// "completed"，后续 case 的 len(entries) != 1 断言必然失败（无法满足）。
	// 按意图修正：每个 case 用独立目录，验证各自会话落到期望桶。
	cases := []struct {
		name string
		term *Termination
		want string
	}{
		{"completed", &Termination{Reason: Completed}, "completed"},
		{"client", &Termination{Reason: ClientAbort}, "client-aborted"},
		{"upstream_abort", &Termination{Reason: UpstreamAbort}, "upstream-aborted"},
		{"timeout", &Termination{Reason: UpstreamTimeout}, "failed"},
		{"upstream_error", &Termination{Reason: UpstreamError}, "failed"},
		// 裁决（用户 2026-08-03，跟随 TS）：未记录任何终止时 pick 返回零值
		// （对应 TS undefined），getTargetSubdir 缺省分支 → "failed"。
		{"none", nil, "failed"},
	}
	for _, c := range cases {
		dir := t.TempDir()
		s := NewSession(dir)
		s.WriteDownstreamResponse(nil, 200, "", c.term)
		s.Finish()
		entries, _ := os.ReadDir(filepath.Join(dir, c.want))
		if len(entries) != 1 {
			t.Errorf("%s: bucket %q has %d entries", c.name, c.want, len(entries))
		}
	}
}

func TestPrecedenceClientAbortOverUpstream(t *testing.T) {
	dir := t.TempDir()
	s := NewSession(dir)
	s.RecordUpstreamTermination(UpstreamAbort, "2026-08-03T00:00:00Z")
	s.WriteDownstreamResponse(nil, 200, "", &Termination{Reason: ClientAbort})
	s.Finish()
	entries, _ := os.ReadDir(filepath.Join(dir, "client-aborted"))
	if len(entries) != 1 {
		t.Errorf("client-aborted entries = %d (client abort must win)", len(entries))
	}
}

func TestPrecedenceUpstreamReportOverTracked(t *testing.T) {
	dir := t.TempDir()
	s := NewSession(dir)
	s.WriteDownstreamResponse(nil, 200, "", &Termination{Reason: Completed})
	s.RecordUpstreamTermination(UpstreamAbort, "2026-08-03T00:00:00Z")
	s.Finish()
	entries, _ := os.ReadDir(filepath.Join(dir, "upstream-aborted"))
	if len(entries) != 1 {
		t.Errorf("upstream-aborted entries = %d (upstream report must win over completed)", len(entries))
	}
}

func TestRecordAfterFinishIsNoop(t *testing.T) {
	dir := t.TempDir()
	s := NewSession(dir)
	s.Finish()
	s.RecordUpstreamTermination(UpstreamAbort, "x") // 不得改变已完成的归类
	if s.UpstreamTermination() != nil {
		t.Errorf("upstream termination = %+v", s.UpstreamTermination())
	}
}

func TestLogUpstreamAttemptWrittenOnFinish(t *testing.T) {
	dir := t.TempDir()
	s := NewSession(dir)
	s.LogUpstreamAttempt("zai-1", "https://z/v1/chat/completions", 429, "skipped: retryable status")
	s.LogUpstreamAttempt("zai-2", "https://y/v1/chat/completions", 200, "served")
	s.Finish()
	// 找到 bucket 目录里的 attempts 文件并断言内容包含两条记录。
	matches, _ := filepath.Glob(filepath.Join(dir, "*", "*__START_*", "upstream-attempts.log"))
	if len(matches) != 1 {
		t.Fatalf("attempts log missing: %v", matches)
	}
	data, _ := os.ReadFile(matches[0])
	text := string(data)
	if !strings.Contains(text, "zai-1") || !strings.Contains(text, "429") ||
		!strings.Contains(text, "skipped") || !strings.Contains(text, "zai-2") {
		t.Fatalf("unexpected attempts log:\n%s", text)
	}
}

func TestServerToolLog(t *testing.T) {
	dir := t.TempDir()
	s := NewSession(dir)
	rc := 10
	s.LogServerTool(ServerToolLogEntry{Tool: "web_search", Timestamp: "t", Input: "q", Engine: "brave", ResultCount: &rc})
	s.Finish()
	// 裁决（用户 2026-08-03，跟随 TS）：本会话未记录任何终止 → pick 返回零值
	// → getTargetSubdir 缺省 → "failed" 桶。
	entries, _ := os.ReadDir(filepath.Join(dir, "failed"))
	if len(entries) != 1 {
		t.Fatal("no completed entry")
	}
	b, err := os.ReadFile(filepath.Join(filepath.Join(dir, "failed", entries[0].Name()), "server-tools.log"))
	if err != nil {
		t.Fatal(err)
	}
	// 裁决（用户 2026-08-03，跟随 TS）：Result Count 节为
	// "[Result Count]\n<数字>\n\n"，非 "Result Count: 10" 行式格式。
	if !strings.Contains(string(b), "[Tool]") || !strings.Contains(string(b), "[Result Count]\n10") {
		t.Errorf("server-tools.log: %s", b)
	}
}

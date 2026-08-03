package sse

import (
	"strings"
	"testing"
)

func TestMapStopReason(t *testing.T) {
	cases := map[string]string{
		"stop": "end_turn", "length": "max_tokens", "tool_calls": "tool_use",
		"content_filter": "refusal", "unknown": "end_turn", "": "end_turn",
	}
	for in, want := range cases {
		if got := MapStopReason(in); got != want {
			t.Errorf("MapStopReason(%q) = %q", in, got)
		}
	}
}

func TestMapErrorType(t *testing.T) {
	if MapErrorType(429) != "overloaded_error" {
		t.Error("429")
	}
	if MapErrorType(529) != "overloaded_error" {
		t.Error("529")
	}
	if MapErrorType(400) != "invalid_request_error" {
		t.Error("400")
	}
	if MapErrorType(500) != "api_error" {
		t.Error("500")
	}
}

func TestBuildMidStreamErrorSse(t *testing.T) {
	got := BuildMidStreamErrorSse("boom")
	if !strings.Contains(got, "event: error\n") {
		t.Errorf("got %s", got)
	}
	if !strings.Contains(got, `"error":{"type":"stream_error","message":"boom"}`) {
		t.Errorf("got %s", got)
	}
	if strings.Contains(got, "overloaded_error") {
		t.Errorf("must not be retryable: %s", got)
	}
}

func TestBuildRetryableMidStreamErrorSse(t *testing.T) {
	got := BuildRetryableMidStreamErrorSse("boom")
	if !strings.Contains(got, `"message":"{\"type\":\"overloaded_error\"} boom"`) {
		t.Errorf("retryable prefix missing: %s", got)
	}
}

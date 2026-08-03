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
	// Non-retryable: api_error
	got := BuildMidStreamErrorSse("api_error", "something went wrong")
	if !strings.Contains(got, "event: error\n") {
		t.Errorf("missing event header: %s", got)
	}
	if !strings.Contains(got, `"error":{"type":"api_error","message":"something went wrong"}`) {
		t.Errorf("wrong payload: %s", got)
	}

	// Retryable: overloaded_error
	got2 := BuildMidStreamErrorSse("overloaded_error", "overloaded")
	if !strings.Contains(got2, `"error":{"type":"overloaded_error","message":"overloaded"}`) {
		t.Errorf("wrong retryable payload: %s", got2)
	}
}

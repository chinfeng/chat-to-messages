package sse

import "encoding/json"

// MapStopReason maps an OpenAI finish_reason to the Anthropic stop_reason
// vocabulary. Unknown or empty reasons fall back to "end_turn".
func MapStopReason(openaiReason string) string {
	if openaiReason == "" {
		return "end_turn"
	}
	switch openaiReason {
	case "stop":
		return "end_turn"
	case "length":
		return "max_tokens"
	case "tool_calls":
		return "tool_use"
	case "content_filter":
		return "refusal"
	}
	return "end_turn"
}

// MapErrorType maps an upstream HTTP status (or error code) to the
// Anthropic SSE error.error.type vocabulary: 429/529 → overloaded_error,
// other 4xx → invalid_request_error, everything else → api_error.
func MapErrorType(code int) string {
	if code == 429 || code == 529 {
		return "overloaded_error"
	}
	if code >= 400 && code < 500 {
		return "invalid_request_error"
	}
	return "api_error"
}

// BuildMidStreamErrorSse builds the downstream top-level `event: error` SSE
// line for a mid-stream failure: a stream_error-flavoured error carrying the
// upstream failure text. No message_delta/message_stop is emitted alongside
// it, so the turn is never disguised as completed; the client SDK throws on
// the error event and discards the partial. By design this does NOT trigger
// a claude-code client retry.
func BuildMidStreamErrorSse(message string) string {
	return FormatEvent("error", errorEventData{
		Type:  "error",
		Error: errorEventError{Type: "stream_error", Message: message},
	})
}

// BuildRetryableMidStreamErrorSse builds a stream_error-flavoured mid-stream
// `event: error` that DOES trigger a claude-code client retry (for upstream
// connection aborts). The overloaded_error marker is embedded into
// error.message as a prefix — claude-code's retry predicate matches the
// literal substring `{"type":"overloaded_error"}` in the SDK-built message —
// while error.type stays stream_error so dumps/logs show the true root cause.
func BuildRetryableMidStreamErrorSse(message string) string {
	return BuildMidStreamErrorSse(`{"type":"overloaded_error"} ` + message)
}

// FormatEvent renders one SSE event as `event: <type>\ndata: <json>\n\n`.
// data may be any JSON-marshalable value; a marshal failure falls back to
// `{}` rather than corrupting the stream.
func FormatEvent(eventType string, data any) string {
	payload, err := json.Marshal(data)
	if err != nil {
		payload = []byte("{}")
	}
	return "event: " + eventType + "\ndata: " + string(payload) + "\n\n"
}

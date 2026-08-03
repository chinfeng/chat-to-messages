package sse

import (
	"bytes"
	"encoding/json"
)

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

// BuildMidStreamErrorSse builds a mid-stream `event: error` SSE line with the
// given official Anthropic error type and message. errorType must be one of the
// documented error types (overloaded_error, api_error, invalid_request_error,
// rate_limit_error, etc.). No message_delta/message_stop follows the error
// event — the client SDK throws and discards the partial.
func BuildMidStreamErrorSse(errorType, message string) string {
	return FormatEvent("error", errorEventData{
		Type:  "error",
		Error: errorEventError{Type: errorType, Message: message},
	})
}

// FormatEvent renders one SSE event as `event: <type>\ndata: <json>\n\n`.
// data may be any JSON-marshalable value; a marshal failure falls back to
// `{}` rather than corrupting the stream. HTML escaping is disabled so the
// bytes match TS JSON.stringify (which leaves `<`, `>`, `&` literal); clients
// decode the JSON identically either way.
func FormatEvent(eventType string, data any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(data); err != nil {
		buf.Reset()
		buf.WriteString("{}")
	} else {
		// Encoder.Encode appends a trailing newline; strip it so the event
		// keeps the TS `event: <type>\ndata: <json>\n\n` shape.
		buf.Truncate(buf.Len() - 1)
	}
	return "event: " + eventType + "\ndata: " + buf.String() + "\n\n"
}

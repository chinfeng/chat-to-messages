// Package responses implements the downstream OpenAI Responses API dialect of
// the proxy: POST /v1/responses is translated to the upstream
// /v1/chat/completions and the (streamed) reply is translated back into
// Responses-format SSE. See docs/superpowers/specs/2026-08-29-responses-endpoint-design.md
// and docs/adr/0001.
package responses

import (
	"encoding/base64"
	"encoding/json"
)

// APIError is a client-facing request error. HTTPStatus carries the status to
// answer with (400 invalid request, 404 unknown previous_response_id).
type APIError struct {
	HTTPStatus int
	Type       string // "invalid_request_error" | "not_found_error"
	Param      string
	Message    string
}

func (e *APIError) Error() string { return e.Message }

func invalidRequest(param, message string) *APIError {
	return &APIError{HTTPStatus: 400, Type: "invalid_request_error", Param: param, Message: message}
}

// Request mirrors the POST /v1/responses body. Pointer/`any` fields preserve
// absence-vs-zero and numeric fidelity (decoded with UseNumber upstream).
type Request struct {
	Model              string           `json:"model"`
	Input              Input            `json:"input"`
	Instructions       *string          `json:"instructions"`
	Tools              []Tool           `json:"tools"`
	ToolChoice         any              `json:"tool_choice"`
	ParallelToolCalls  *bool            `json:"parallel_tool_calls"`
	Temperature        any              `json:"temperature"`
	TopP               any              `json:"top_p"`
	MaxOutputTokens    any              `json:"max_output_tokens"`
	Text               *TextConfig      `json:"text"`
	Reasoning          *ReasoningConfig `json:"reasoning"`
	Stream             bool             `json:"stream"`
	Store              *bool            `json:"store"`
	PreviousResponseID string           `json:"previous_response_id"`
	Include            []string         `json:"include"`
	Truncation         string           `json:"truncation"`
	Background         bool             `json:"background"`
	Metadata           any              `json:"metadata"`
	User               string           `json:"user"`
	SafetyIdentifier   string           `json:"safety_identifier"`
}

// Input is the `input` union: a shorthand string or an array of items. Items
// are decoded typed; Raw keeps the original decoded values verbatim (the
// response store replays raw items so unknown/passthrough fields such as
// encrypted_content survive round trips).
type Input struct {
	IsString bool
	Str      string
	Raw      []any
	Items    []Item
}

// UnmarshalJSON accepts a string (single user turn) or an item array.
func (in *Input) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		in.IsString = true
		in.Str = s
		return nil
	}
	var raw []any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	in.Raw = raw
	items, err := ItemsFromRaw(raw)
	if err != nil {
		return err
	}
	in.Items = items
	return nil
}

// Item is one input item (message / function_call / function_call_output /
// reasoning). Fields of other item kinds stay nil/empty.
type Item struct {
	Type   string `json:"type"`
	ID     string `json:"id,omitempty"`
	Status string `json:"status,omitempty"`

	// message
	Role    string       `json:"role,omitempty"`
	Content ContentValue `json:"content,omitempty"`

	// function_call / function_call_output
	CallID string `json:"call_id,omitempty"`
	Name   string `json:"name,omitempty"`

	// function_call
	Arguments string `json:"arguments,omitempty"`

	// function_call_output
	Output ContentValue `json:"output,omitempty"`

	// reasoning
	Summary          []SummaryPart `json:"summary,omitempty"`
	EncryptedContent string        `json:"encrypted_content,omitempty"`
}

// UnmarshalJSON decodes one item, tolerating non-string arguments and the
// content/output string-vs-parts unions.
func (it *Item) UnmarshalJSON(data []byte) error {
	var raw struct {
		Type      string          `json:"type"`
		ID        string          `json:"id"`
		Status    string          `json:"status"`
		Role      string          `json:"role"`
		Content   json.RawMessage `json:"content"`
		CallID    string          `json:"call_id"`
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
		Output    json.RawMessage `json:"output"`
		Summary   []SummaryPart   `json:"summary"`
		Encrypted string          `json:"encrypted_content"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	it.Type, it.ID, it.Status, it.Role = raw.Type, raw.ID, raw.Status, raw.Role
	it.CallID, it.Name = raw.CallID, raw.Name
	it.Summary, it.EncryptedContent = raw.Summary, raw.Encrypted
	if len(raw.Content) > 0 {
		if err := it.Content.UnmarshalJSON(raw.Content); err != nil {
			return err
		}
	}
	if len(raw.Output) > 0 {
		if err := it.Output.UnmarshalJSON(raw.Output); err != nil {
			return err
		}
	}
	if len(raw.Arguments) > 0 {
		var s string
		if err := json.Unmarshal(raw.Arguments, &s); err == nil {
			it.Arguments = s
		} else {
			// Tolerate object arguments (strict OpenAI sends a string).
			it.Arguments = string(raw.Arguments)
		}
	}
	return nil
}

// ItemsFromRaw re-decodes raw JSON-decoded values into typed Items (used for
// store entries replayed from a previous response).
func ItemsFromRaw(raw []any) ([]Item, error) {
	items := make([]Item, 0, len(raw))
	for i, v := range raw {
		data, err := json.Marshal(v)
		if err != nil {
			return nil, invalidRequest("input", "invalid item in input")
		}
		var it Item
		if err := json.Unmarshal(data, &it); err != nil {
			return nil, invalidRequest("input", "invalid item at index "+itoa(i)+": "+err.Error())
		}
		items = append(items, it)
	}
	return items, nil
}

// ContentValue is the message content / tool-output union: a bare string or
// an array of content parts.
type ContentValue struct {
	IsString bool
	Str      string
	Parts    []ContentPart
}

func (cv *ContentValue) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		cv.IsString = true
		cv.Str = s
		return nil
	}
	return json.Unmarshal(data, &cv.Parts)
}

// ContentPart is one typed part: input_text / output_text / input_image /
// input_file / refusal / input_audio (rejected downstream).
type ContentPart struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	Refusal  string `json:"refusal,omitempty"`
	ImageURL string `json:"image_url,omitempty"`
	FileID   string `json:"file_id,omitempty"`
	Detail   string `json:"detail,omitempty"`
	Filename string `json:"filename,omitempty"`
	FileData string `json:"file_data,omitempty"`
}

// SummaryPart is one reasoning summary part ({type:"summary_text", text}).
type SummaryPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// Tool is one entry of the tools array. Only function tools are convertible;
// hosted tool types are rejected before conversion.
type Tool struct {
	Type        string `json:"type"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters,omitempty"`
	Strict      *bool  `json:"strict,omitempty"`
}

// TextConfig mirrors `text` (format + verbosity).
type TextConfig struct {
	Format    TextFormat `json:"format"`
	Verbosity string     `json:"verbosity,omitempty"`
}

// TextFormat mirrors `text.format`: text | json_object | json_schema.
type TextFormat struct {
	Type        string `json:"type"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Schema      any    `json:"schema,omitempty"`
	Strict      *bool  `json:"strict,omitempty"`
}

// ReasoningConfig mirrors `reasoning` (effort / summary / max_tokens).
type ReasoningConfig struct {
	Effort    string `json:"effort,omitempty"`
	Summary   string `json:"summary,omitempty"`
	MaxTokens any    `json:"max_tokens,omitempty"`
}

// WantsEncrypted reports whether the request asked for reasoning.encrypted_content.
func (r *Request) WantsEncrypted() bool {
	for _, inc := range r.Include {
		if inc == "reasoning.encrypted_content" {
			return true
		}
	}
	return false
}

// EncryptReasoning fabricates the encrypted_content the proxy hands out for a
// reasoning item (base64 of the summary text — the proxy is both ends of the
// store, so a reversible encoding suffices; there is no secret to protect).
func EncryptReasoning(text string) string {
	return base64.StdEncoding.EncodeToString([]byte(text))
}

// DecryptReasoning reverses EncryptReasoning. ok=false when the value is not
// ours (cannot be decoded).
func DecryptReasoning(encrypted string) (string, bool) {
	b, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", false
	}
	return string(b), true
}

// itoa avoids importing strconv in this file for one call site.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	p := len(buf)
	for i > 0 {
		p--
		buf[p] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[p:])
}

package parsers

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"regexp"
	"strings"
)

// parserState is the state-machine state of HeuristicToolParser.
type parserState int

const (
	// parserStateText is plain text outside any tool call.
	parserStateText parserState = iota + 1
	// parserStateMatchingFunction waits for "<function=...>".
	parserStateMatchingFunction
	// parserStateParsingParameters collects "<parameter=...>" pairs.
	parserStateParsingParameters
)

const (
	controlTokenStart = "<|"
	controlTokenEnd   = "|>"
	bullet            = "●"
)

var (
	// controlTokenRE strips Llama-style control tokens like <|begin_of_text|>.
	controlTokenRE = regexp.MustCompile(`<\|[^|>]{1,80}\|>`)
	// funcStartPattern matches the start of a heuristic tool call.
	funcStartPattern = regexp.MustCompile(`●\s*<function=([^>]+)>`)
	// paramPattern matches one parameter element, or an unterminated
	// trailing one (the (?:</parameter>|$) alternative).
	paramPattern = regexp.MustCompile(`<parameter=([^>]+)>([\s\S]*?)(?:</parameter>|$)`)
	// webToolJSONPattern detects "Use WebFetch/WebSearch {json}" calls;
	// tool is group 1, json is group 2.
	webToolJSONPattern = regexp.MustCompile(`(?is)\b(?:use\s+)?(?P<tool>WebFetch|WebSearch)\b.*?(?P<json>\{.*?\})`)
	// flushParamPattern collects trailing unterminated parameters on Flush.
	flushParamPattern = regexp.MustCompile(`(?s)<parameter=([^>]+)>([\s\S]*)$`)
)

// HeuristicToolParser is a streaming parser that detects tool calls
// emitted as plain text: <|control tokens|> are stripped, ●
// <function=...> calls and inline WebFetch/WebSearch JSON calls are
// converted into tool_use records, and everything else passes through as
// text.
type HeuristicToolParser struct {
	state               parserState
	buffer              string
	currentToolID       string
	currentFunctionName string
	currentParameters   map[string]string
}

// NewHeuristicToolParser creates a HeuristicToolParser in TEXT state.
func NewHeuristicToolParser() *HeuristicToolParser {
	return &HeuristicToolParser{state: parserStateText}
}

// heuristicToolID returns a tool_use id of the form
// toolu_heuristic_<8 hex chars> from crypto/rand (4 bytes).
func heuristicToolID() string {
	var b [4]byte
	rand.Read(b[:]) // crypto/rand.Read is documented to never fail
	return "toolu_heuristic_" + hex.EncodeToString(b[:])
}

// extractWebToolJSONCalls scans the buffer for inline WebFetch/WebSearch
// JSON calls. When any are found the whole buffer is consumed and cleared;
// otherwise the buffer is returned unchanged.
func (p *HeuristicToolParser) extractWebToolJSONCalls() (filteredBuffer string, detectedTools []map[string]any) {
	for _, m := range webToolJSONPattern.FindAllStringSubmatch(p.buffer, -1) {
		// m[1] = tool name, m[2] = JSON payload.
		obj, ok := decodeJSONObject(m[2])
		if !ok {
			continue
		}
		toolName := m[1]
		if toolName == "WebFetch" {
			if _, has := obj["url"]; !has {
				continue
			}
		} else if toolName == "WebSearch" {
			if _, has := obj["query"]; !has {
				continue
			}
		}
		detectedTools = append(detectedTools, map[string]any{
			"type":  "tool_use",
			"id":    heuristicToolID(),
			"name":  toolName,
			"input": obj,
		})
	}
	if len(detectedTools) == 0 {
		return p.buffer, nil
	}
	return "", detectedTools
}

// decodeJSONObject parses s as a JSON object. Numbers are decoded with
// UseNumber so tool inputs preserve precision. The whole string must be
// exactly one JSON value (like TS JSON.parse).
func decodeJSONObject(s string) (map[string]any, bool) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	if err := dec.Decode(&v); err != io.EOF {
		return nil, false
	}
	obj, ok := v.(map[string]any)
	return obj, ok
}

// splitIncompleteControlTokenTail returns the text before an unclosed
// "<|" control-token start at the end of the buffer, leaving the
// incomplete token in the buffer for the next feed.
func (p *HeuristicToolParser) splitIncompleteControlTokenTail() string {
	start := strings.LastIndex(p.buffer, controlTokenStart)
	if start == -1 {
		return ""
	}
	if strings.Contains(p.buffer[start:], controlTokenEnd) {
		return ""
	}
	prefix := p.buffer[:start]
	p.buffer = p.buffer[start:]
	return prefix
}

// Feed ingests more text and returns the filtered (tool-stripped) text
// that is complete so far plus any fully detected tool calls.
func (p *HeuristicToolParser) Feed(text string) (string, []map[string]any) {
	p.buffer += text
	p.buffer = controlTokenRE.ReplaceAllString(p.buffer, "")

	filteredBuffer, detectedTools := p.extractWebToolJSONCalls()
	p.buffer = filteredBuffer

	var filteredOutputParts []string

	for {
		if p.state == parserStateText {
			if idx := strings.Index(p.buffer, bullet); idx != -1 {
				filteredOutputParts = append(filteredOutputParts, p.buffer[:idx])
				p.buffer = p.buffer[idx:]
				p.state = parserStateMatchingFunction
			} else {
				// No ●: everything is plain text. Keep only an unclosed
				// "<|" control-token tail in the buffer.
				if safePrefix := p.splitIncompleteControlTokenTail(); safePrefix != "" {
					filteredOutputParts = append(filteredOutputParts, safePrefix)
				} else if p.buffer != "" {
					filteredOutputParts = append(filteredOutputParts, p.buffer)
					p.buffer = ""
				}
				break
			}
		}

		if p.state == parserStateMatchingFunction {
			if m := funcStartPattern.FindStringSubmatchIndex(p.buffer); m != nil {
				// m[0] = match start, m[2:4] = function name group.
				p.currentFunctionName = strings.TrimSpace(p.buffer[m[2]:m[3]])
				p.currentToolID = heuristicToolID()
				p.currentParameters = map[string]string{}
				p.buffer = p.buffer[m[1]:]
				p.state = parserStateParsingParameters
			} else if len(p.buffer) > 100 {
				// Overlong header without <function= → drain one char to text.
				filteredOutputParts = append(filteredOutputParts, p.buffer[:1])
				p.buffer = p.buffer[1:]
				p.state = parserStateText
			} else {
				break
			}
		}

		if p.state == parserStateParsingParameters {
			finishedToolCall := false
			nextTool := false

			for {
				m := paramPattern.FindStringSubmatchIndex(p.buffer)
				if m == nil || !strings.Contains(p.buffer[m[0]:m[1]], "</parameter>") {
					break
				}
				if pre := p.buffer[:m[0]]; pre != "" {
					filteredOutputParts = append(filteredOutputParts, pre)
				}
				// m[2:4] = key, m[4:6] = value.
				p.currentParameters[strings.TrimSpace(p.buffer[m[2]:m[3]])] = strings.TrimSpace(p.buffer[m[4]:m[5]])
				p.buffer = p.buffer[m[1]:]
			}

			if idx := strings.Index(p.buffer, bullet); idx != -1 {
				// Another ● starts the next tool call; text before it is output.
				if idx > 0 {
					filteredOutputParts = append(filteredOutputParts, p.buffer[:idx])
				}
				p.buffer = p.buffer[idx:]
				finishedToolCall = true
				nextTool = true
			} else if p.buffer == "" {
				// All parameters consumed — the tool call is complete.
				finishedToolCall = true
			} else if !strings.HasPrefix(strings.TrimSpace(p.buffer), "<") && !strings.Contains(p.buffer, "<parameter=") {
				// Trailing plain text after a complete tool call stays
				// buffered and comes out on a later feed/flush.
				finishedToolCall = true
			}

			if finishedToolCall && p.currentToolID != "" && p.currentFunctionName != "" {
				detectedTools = append(detectedTools, p.buildTool())
				p.state = parserStateText
				if !nextTool {
					break
				}
			} else {
				break
			}
		}
	}

	return strings.Join(filteredOutputParts, ""), detectedTools
}

// buildTool assembles the current tool call as a tool_use record. Input
// keys/values are copied into a map[string]any (strings, per the TS
// Record<string, string> parameters).
func (p *HeuristicToolParser) buildTool() map[string]any {
	input := make(map[string]any, len(p.currentParameters))
	for k, v := range p.currentParameters {
		input[k] = v
	}
	return map[string]any{
		"type":  "tool_use",
		"id":    p.currentToolID,
		"name":  p.currentFunctionName,
		"input": input,
	}
}

// Flush finalizes any pending state: a tool call whose parameters were
// still being parsed is emitted (collecting any unterminated trailing
// parameter), and remaining buffer text is returned.
func (p *HeuristicToolParser) Flush() (string, []map[string]any) {
	p.buffer = controlTokenRE.ReplaceAllString(p.buffer, "")
	var detectedTools []map[string]any
	var remainingText string

	switch p.state {
	case parserStateParsingParameters:
		if p.currentToolID != "" && p.currentFunctionName != "" {
			for _, m := range flushParamPattern.FindAllStringSubmatch(p.buffer, -1) {
				p.currentParameters[strings.TrimSpace(m[1])] = strings.TrimSpace(m[2])
			}
			detectedTools = append(detectedTools, p.buildTool())
			p.state = parserStateText
		}
	case parserStateMatchingFunction:
		// Incomplete function header — emit as text.
		remainingText = p.buffer
	case parserStateText:
		if p.buffer != "" {
			remainingText = p.buffer
		}
	}

	p.buffer = ""
	return remainingText, detectedTools
}

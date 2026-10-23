package parsers

import (
	"regexp"
	"strings"
)

// dsmlTagRE matches a DeepSeek DSML control tag. DSML is the model's internal
// tool-call envelope markup; when a serving stack fails to swallow it, the tag
// leaks into visible content/reasoning text (dumped 2026-10-23: newapi →
// deepseek-v4.1-flash emitted "\n\n<｜DSML｜ calls>\n" right before a valid
// native tool_calls delta). Variants seen in the wild:
//
//	<｜DSML｜tool_calls>   </｜DSML｜tool_calls>
//	<｜｜DSML｜｜tool_calls>                       (double bars)
//	<｜DSML｜ calls>                              (mangled live V4 stream)
//	<|DSML|tool_calls>                           (ASCII-pipe proxies)
var dsmlTagRE = regexp.MustCompile(`(?i)<\s*/?\s*[|｜]{1,2}\s*DSML\s*[|｜]{1,2}[^<>]*>`)

// dsmlLoosePrefixRE matches a trailing fragment that could still grow into a
// DSML tag, including a bare "<" or "</" so a split immediately after the
// angle bracket does not leak half a marker. Ordinary text such as "a < b"
// does not match.
var dsmlLoosePrefixRE = regexp.MustCompile(`(?i)^<\s*/?\s*(?:[|｜]{1,2}\s*(?:D(?:S(?:M(?:L\s*(?:[|｜]{1,2}\s*[A-Za-z_]*)?)?)?)?)?)?$`)

// dsmlResidueRE is the strict form used at Flush: a tail that still looks like
// an unfinished DSML marker (bar required) is residue and is dropped. A bare
// "<" is ordinary text and is emitted.
var dsmlResidueRE = regexp.MustCompile(`(?i)^<\s*/?\s*[|｜]{1,2}\s*(?:D(?:S(?:M(?:L\s*(?:[|｜]{1,2}\s*[A-Za-z_]*)?)?)?)?)?$`)

// StripDSMLResidue removes leaked DSML control tags from a complete string.
// Marker-only residue that leaves nothing but whitespace collapses to "".
// Envelope bodies (invoke/parameter payloads) are not parsed or healed here.
func StripDSMLResidue(text string) string {
	if !strings.Contains(text, "DSML") && !strings.Contains(text, "dsml") {
		return text
	}
	stripped := dsmlTagRE.ReplaceAllString(text, "")
	if stripped != text && strings.TrimSpace(stripped) == "" {
		return ""
	}
	return stripped
}

// DSMLFilter is a streaming DSML residue filter. Feed returns the text that is
// safe to emit now; a trailing fragment that may become a marker is held until
// the next Feed or Flush. At Flush an unfinished marker is dropped.
type DSMLFilter struct {
	buf string
}

// NewDSMLFilter creates an empty DSMLFilter.
func NewDSMLFilter() *DSMLFilter {
	return &DSMLFilter{}
}

// Feed appends text, strips complete markers, and returns the safe prefix.
func (f *DSMLFilter) Feed(text string) string {
	if text == "" {
		return ""
	}
	f.buf += text
	stripped := dsmlTagRE.ReplaceAllString(f.buf, "")
	removed := stripped != f.buf
	f.buf = stripped

	// A marker was the only thing in the buffer: drop its leftover whitespace
	// instead of emitting a blank text block.
	if removed && strings.TrimSpace(f.buf) == "" {
		f.buf = ""
		return ""
	}

	// Hold a trailing fragment that might still become a marker.
	if i := strings.LastIndex(f.buf, "<"); i != -1 && !strings.Contains(f.buf[i:], ">") && dsmlLoosePrefixRE.MatchString(f.buf[i:]) {
		out := f.buf[:i]
		f.buf = f.buf[i:]
		return out
	}
	out := f.buf
	f.buf = ""
	return out
}

// Flush returns any remaining text. An unfinished DSML marker tail is residue
// and is dropped; ordinary trailing text (including a lone "<") is kept.
func (f *DSMLFilter) Flush() string {
	f.buf = dsmlTagRE.ReplaceAllString(f.buf, "")
	if i := strings.LastIndex(f.buf, "<"); i != -1 && dsmlResidueRE.MatchString(f.buf[i:]) {
		f.buf = f.buf[:i]
	}
	out := f.buf
	f.buf = ""
	return out
}

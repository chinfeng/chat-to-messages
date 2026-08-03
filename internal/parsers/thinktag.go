// Package parsers provides streaming parsers for provider-emitted text:
// a think-tag stream parser (ThinkTagParser) and a heuristic tool-call
// parser (HeuristicToolParser), ported from chat-to-claude-code's
// src/parsers/think_tag_parser.ts and src/parsers/heuristic_tool_parser.ts.
package parsers

import "strings"

// ContentType distinguishes ordinary assistant text from thinking
// (reasoning) content.
type ContentType int

const (
	// TextContent is ordinary assistant text.
	TextContent ContentType = iota
	// ThinkingContent is thinking/reasoning content.
	ThinkingContent
)

// ContentChunk is one streamed piece of content with its type.
type ContentChunk struct {
	Type    ContentType
	Content string
}

const (
	openTag  = "<think>"
	closeTag = "</think>"
)

// ThinkTagParser is a streaming parser that separates text outside
// <think>…</think> tags (TextContent) from the content inside them
// (ThinkingContent). Line-by-line port of chat-to-claude-code's
// think_tag_parser.ts: thinking content is emitted as it arrives, only a
// trailing fragment that could be the start of a tag is held back.
type ThinkTagParser struct {
	buffer     string
	inThinkTag bool
}

// NewThinkTagParser creates a ThinkTagParser with an empty buffer.
func NewThinkTagParser() *ThinkTagParser {
	return &ThinkTagParser{}
}

// Feed ingests content and returns the chunks that are complete after
// this batch. Residual partial tags stay buffered.
func (p *ThinkTagParser) Feed(content string) []ContentChunk {
	p.buffer += content
	var chunks []ContentChunk
	for p.buffer != "" {
		prevLen := len(p.buffer)
		var chunk *ContentChunk
		if !p.inThinkTag {
			chunk = p.parseOutsideThink()
		} else {
			chunk = p.parseInsideThink()
		}
		if chunk != nil {
			chunks = append(chunks, *chunk)
		} else if len(p.buffer) == prevLen {
			break
		}
	}
	return chunks
}

// parseOutsideThink processes the buffer while not inside a think tag.
func (p *ThinkTagParser) parseOutsideThink() *ContentChunk {
	thinkStart := strings.Index(p.buffer, openTag)
	orphanClose := strings.Index(p.buffer, closeTag)

	// An orphan close tag (no open tag before it) is dropped: emit the
	// text before it; whatever follows stays in the buffer and is
	// processed by the feed loop (or a later feed/flush).
	if orphanClose != -1 && (thinkStart == -1 || orphanClose < thinkStart) {
		preOrphan := p.buffer[:orphanClose]
		p.buffer = p.buffer[orphanClose+len(closeTag):]
		if preOrphan != "" {
			return &ContentChunk{Type: TextContent, Content: preOrphan}
		}
		return nil
	}

	if thinkStart == -1 {
		// No open tag: emit everything except a trailing fragment that
		// could be the start of "<think>" or "</think>".
		lastBracket := strings.LastIndex(p.buffer, "<")
		if lastBracket != -1 {
			potentialTag := p.buffer[lastBracket:]
			tagLen := len(potentialTag)
			if (tagLen < len(openTag) && strings.HasPrefix(openTag, potentialTag)) ||
				(tagLen < len(closeTag) && strings.HasPrefix(closeTag, potentialTag)) {
				emit := p.buffer[:lastBracket]
				p.buffer = p.buffer[lastBracket:]
				if emit != "" {
					return &ContentChunk{Type: TextContent, Content: emit}
				}
				return nil
			}
		}
		emit := p.buffer
		p.buffer = ""
		if emit != "" {
			return &ContentChunk{Type: TextContent, Content: emit}
		}
		return nil
	}

	preThink := p.buffer[:thinkStart]
	p.buffer = p.buffer[thinkStart+len(openTag):]
	p.inThinkTag = true
	if preThink != "" {
		return &ContentChunk{Type: TextContent, Content: preThink}
	}
	return nil
}

// parseInsideThink processes the buffer while inside a think tag.
// Thinking content is emitted as it arrives; only a trailing fragment
// that could be the start of "</think>" is held back.
func (p *ThinkTagParser) parseInsideThink() *ContentChunk {
	thinkEnd := strings.Index(p.buffer, closeTag)

	if thinkEnd == -1 {
		lastBracket := strings.LastIndex(p.buffer, "<")
		if lastBracket != -1 && len(p.buffer)-lastBracket < len(closeTag) {
			potentialTag := p.buffer[lastBracket:]
			if strings.HasPrefix(closeTag, potentialTag) {
				emit := p.buffer[:lastBracket]
				p.buffer = p.buffer[lastBracket:]
				if emit != "" {
					return &ContentChunk{Type: ThinkingContent, Content: emit}
				}
				return nil
			}
		}
		emit := p.buffer
		p.buffer = ""
		if emit != "" {
			return &ContentChunk{Type: ThinkingContent, Content: emit}
		}
		return nil
	}

	thinkingContent := p.buffer[:thinkEnd]
	p.buffer = p.buffer[thinkEnd+len(closeTag):]
	p.inThinkTag = false
	if thinkingContent != "" {
		return &ContentChunk{Type: ThinkingContent, Content: thinkingContent}
	}
	return nil
}

// Flush returns any remaining buffered content as one chunk, typed by the
// current mode (THINKING inside a tag, TEXT otherwise), or nil when the
// buffer is empty.
func (p *ThinkTagParser) Flush() *ContentChunk {
	if p.buffer == "" {
		return nil
	}
	t := TextContent
	if p.inThinkTag {
		t = ThinkingContent
	}
	content := p.buffer
	p.buffer = ""
	return &ContentChunk{Type: t, Content: content}
}

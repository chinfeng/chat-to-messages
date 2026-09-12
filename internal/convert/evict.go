package convert

import (
	"bytes"
	"encoding/json"

	"github.com/chinfeng/chat-to-messages/internal/anthropic"
)

// imageEvictedPlaceholder replaces images evicted by EvictOldImages — the
// same pattern Claude Code uses for media it strips from prior tool results.
const imageEvictedPlaceholder = "[Media removed from earlier context to reduce request size]"

// EvictOldImages replaces all but the most recent `keep` upstream-visible
// images with text placeholders, mutating messages in place before
// conversion. The z-ai upstream channel deterministically fails requests
// carrying >= 8 images (replay-bisected 2026-09-12), so the proxy caps them;
// the current turn's images are always the last ones and survive.
//
// Images are counted in document order: user-message image blocks, base64
// image documents, and images nested in tool_result content (which the
// converter later moves to a synthetic user turn). keep <= 0 disables.
// Returns the number of images evicted.
func EvictOldImages(messages []anthropic.Message, keep int) int {
	if keep <= 0 || len(messages) == 0 {
		return 0
	}
	total := 0
	for i := range messages {
		total += countMessageImages(&messages[i])
	}
	evict := total - keep
	if evict <= 0 {
		return 0
	}
	for i := range messages {
		if evict == 0 {
			break
		}
		evict -= evictMessageImages(&messages[i], evict)
	}
	return total - keep
}

// countMessageImages counts the image blocks in one message that would be
// emitted upstream as image_url parts.
func countMessageImages(m *anthropic.Message) int {
	blocks, ok := m.Content.Blocks()
	if !ok {
		return 0
	}
	n := 0
	for i := range blocks {
		n += countBlockImages(&blocks[i])
	}
	return n
}

func countBlockImages(block *anthropic.ContentBlock) int {
	switch block.Type {
	case "image":
		if buildImagePartFromBlock(block) != nil {
			return 1
		}
	case "document":
		if src := block.Source; src != nil && src.Type == "base64" && isImageBase64Source(src) {
			return 1
		}
	default:
		if isToolResultBlockType(block.Type) {
			return countToolResultImages(block)
		}
	}
	return 0
}

// countToolResultImages counts image entries nested in a tool_result content
// array, using the same criteria as the converter's media extraction.
func countToolResultImages(block *anthropic.ContentBlock) int {
	entries, ok := decodeToolResultEntries(block.Content)
	if !ok {
		return 0
	}
	n := 0
	for _, e := range entries {
		if m, ok := e.(map[string]any); ok && buildImagePartFromMap(m) != nil {
			n++
		}
	}
	return n
}

// evictMessageImages replaces up to budget images in one message with text
// placeholders, returning the number replaced.
func evictMessageImages(m *anthropic.Message, budget int) int {
	blocks, ok := m.Content.Blocks()
	if !ok {
		return 0
	}
	replaced := 0
	for i := range blocks {
		if replaced == budget {
			break
		}
		block := &blocks[i]
		switch block.Type {
		case "image":
			if buildImagePartFromBlock(block) != nil {
				*block = anthropic.ContentBlock{Type: "text", Text: imageEvictedPlaceholder}
				replaced++
			}
		case "document":
			if src := block.Source; src != nil && src.Type == "base64" && isImageBase64Source(src) {
				*block = anthropic.ContentBlock{Type: "text", Text: imageEvictedPlaceholder}
				replaced++
			}
		default:
			if isToolResultBlockType(block.Type) {
				replaced += evictToolResultImages(block, budget-replaced)
			}
		}
	}
	return replaced
}

// evictToolResultImages replaces up to budget image entries inside a
// tool_result content array with text entries, re-marshaling the content
// field. Returns the number replaced.
func evictToolResultImages(block *anthropic.ContentBlock, budget int) int {
	entries, ok := decodeToolResultEntries(block.Content)
	if !ok {
		return 0
	}
	replaced := 0
	for i, e := range entries {
		if replaced == budget {
			break
		}
		m, ok := e.(map[string]any)
		if !ok || buildImagePartFromMap(m) == nil {
			continue
		}
		entries[i] = map[string]any{"type": "text", "text": imageEvictedPlaceholder}
		replaced++
	}
	if replaced == 0 {
		return 0
	}
	out, err := json.Marshal(entries)
	if err != nil {
		return 0
	}
	block.Content = out
	return replaced
}

// decodeToolResultEntries decodes a tool_result content field into its
// entries with json.Number fidelity; ok is false for string or absent
// content (which carry no images).
func decodeToolResultEntries(raw json.RawMessage) ([]any, bool) {
	if len(raw) == 0 || raw[0] != '[' {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	entries, ok := v.([]any)
	return entries, ok
}

// isImageBase64Source reports whether a document source would be emitted as
// an image_url part (base64 + image MIME + data), per the converter's
// document branch.
func isImageBase64Source(src *anthropic.Source) bool {
	if src.Type != "base64" || src.Data == "" {
		return false
	}
	return isImageMimeType(sourceMediaType(src))
}

// sourceMediaType returns a source's media_type (mime_type fallback).
func sourceMediaType(src *anthropic.Source) string {
	if src.MediaType != "" {
		return src.MediaType
	}
	return src.MimeType
}

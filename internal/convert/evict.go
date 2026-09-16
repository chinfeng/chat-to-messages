package convert

import (
	"bytes"
	"encoding/json"
	"strings"

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
	refs := EnumerateImageRefs(messages)
	evict := len(refs) - keep
	if evict <= 0 {
		return 0
	}
	for _, ref := range refs[:evict] {
		ReplaceImageRef(messages, ref, imageEvictedPlaceholder)
	}
	return evict
}

// ImageRef locates one image that ConvertMessages would emit upstream as an
// image_url part. Part is that OpenAI content part (url + optional detail),
// ready to hand to a vision model.
type ImageRef struct {
	Part    map[string]any
	Message int // index into the messages slice
	Block   int // index into the message's block list
	Entry   int // index into a tool_result content array, -1 otherwise
}

// EnumerateImageRefs returns every image-bearing position in document order:
// user-message image blocks, base64 image documents, and images nested in
// tool_result content (which the converter later moves to a synthetic user
// turn). The slice is empty when the request carries no upstream-visible
// images.
func EnumerateImageRefs(messages []anthropic.Message) []ImageRef {
	var refs []ImageRef
	for mi := range messages {
		blocks, ok := messages[mi].Content.Blocks()
		if !ok {
			continue
		}
		for bi := range blocks {
			block := &blocks[bi]
			switch {
			case block.Type == "image":
				if part := buildImagePartFromBlock(block); part != nil {
					refs = append(refs, ImageRef{Part: part, Message: mi, Block: bi, Entry: -1})
				}
			case isImageDocumentBlock(block):
				refs = append(refs, ImageRef{
					Part:    documentImagePart(block),
					Message: mi, Block: bi, Entry: -1,
				})
			case isToolResultBlockType(block.Type):
				entries, ok := decodeToolResultEntries(block.Content)
				if !ok {
					continue
				}
				for ei, e := range entries {
					m, ok := e.(map[string]any)
					if !ok {
						continue
					}
					if part := buildImagePartFromMap(m); part != nil {
						refs = append(refs, ImageRef{Part: part, Message: mi, Block: bi, Entry: ei})
					}
				}
			}
		}
	}
	return refs
}

// ReplaceImageRef replaces the image at ref with a text block holding text,
// re-marshaling the tool_result content array when the image is nested. It is
// a no-op when ref no longer points at a valid position.
func ReplaceImageRef(messages []anthropic.Message, ref ImageRef, text string) {
	if ref.Message < 0 || ref.Message >= len(messages) {
		return
	}
	blocks, ok := messages[ref.Message].Content.Blocks()
	if !ok || ref.Block < 0 || ref.Block >= len(blocks) {
		return
	}
	block := &blocks[ref.Block]
	if ref.Entry < 0 {
		*block = anthropic.ContentBlock{Type: "text", Text: text}
		return
	}
	entries, ok := decodeToolResultEntries(block.Content)
	if !ok || ref.Entry < 0 || ref.Entry >= len(entries) {
		return
	}
	if _, ok := entries[ref.Entry].(map[string]any); !ok {
		return
	}
	entries[ref.Entry] = map[string]any{"type": "text", "text": text}
	out, err := json.Marshal(entries)
	if err != nil {
		return
	}
	block.Content = out
}

// isImageDocumentBlock reports whether a document block holds a base64 image
// the converter would emit as an image_url part.
func isImageDocumentBlock(block *anthropic.ContentBlock) bool {
	src := block.Source
	return src != nil && src.Type == "base64" && isImageBase64Source(src)
}

// documentImagePart builds the OpenAI image_url part the converter emits for
// a base64 image document block (no detail hint — the converter adds none).
func documentImagePart(block *anthropic.ContentBlock) map[string]any {
	src := block.Source
	mediaType := sourceMediaType(src)
	url := src.Data
	if !strings.HasPrefix(url, "data:") {
		url = "data:" + mediaType + ";base64," + url
	}
	return map[string]any{"type": "image_url", "image_url": map[string]any{"url": url}}
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

// Package convert implements Anthropic Messages API → OpenAI Chat
// Completions conversion and request-body canonicalization, ported from
// chat-to-claude-code's src/conversion/ (canonical.ts + converter.ts).
package convert

import (
	"encoding/json"
	"strings"
)

// schemaNameMapKeys mirrors SCHEMA_NAME_MAP_KEYS in canonical.ts: keys whose
// VALUES are JSON-Schema "name maps" — the keys directly beneath them are
// arbitrary names (property names, definition names, pattern names), not
// schema keywords. A `_`-prefixed name inside one of these maps is a
// legitimate schema property and must NOT be stripped.
var schemaNameMapKeys = map[string]bool{
	"properties":        true,
	"patternProperties": true,
	"definitions":       true,
	"$defs":             true,
}

// FilterPrivateParams recursively strips keys whose name starts with `_`,
// EXCEPT inside JSON-Schema name maps (properties/patternProperties/
// definitions/$defs), whose keys are arbitrary names that legitimately begin
// with `_`. Returns a new value; never mutates the input. The top level of a
// request body is always an object; the top-level map assertion mirrors that.
func FilterPrivateParams(value any) map[string]any {
	out, _ := filterPrivate(value, false).(map[string]any)
	return out
}

func filterPrivate(value any, insideSchemaNameMap bool) any {
	switch v := value.(type) {
	case []any:
		// Elements of an array reset the schema-name-map context — only the
		// object that is the direct value of a schema-name-map key has name-keys.
		out := make([]any, len(v))
		for i, el := range v {
			out[i] = filterPrivate(el, false)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, val := range v {
			if !insideSchemaNameMap && strings.HasPrefix(key, "_") {
				continue // drop private param
			}
			out[key] = filterPrivate(val, schemaNameMapKeys[key])
		}
		return out
	default:
		return value
	}
}

// Canonicalize recursively copies a JSON value to a stable wire form: all
// object keys sorted ascending, arrays preserved (order significant),
// primitives unchanged. Returns a NEW value; never mutates the input.
// (json.Marshal emits map keys sorted, so the recursive copy is sufficient —
// no explicit sorting needed.)
func Canonicalize(value any) any {
	switch v := value.(type) {
	case []any:
		out := make([]any, len(v))
		for i, el := range v {
			out[i] = Canonicalize(el)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, val := range v {
			out[key] = Canonicalize(val)
		}
		return out
	default:
		return value
	}
}

// CanonicalJSONStringify serializes a value with sorted keys and no
// incidental whitespace. Used for tool-call `arguments` so the same tool
// invocation produces the same bytes regardless of input key order —
// stable prefix for upstream cache reuse. json.Number values are emitted
// verbatim, preserving precision.
func CanonicalJSONStringify(value any) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// PrepareCanonicalBody prepares the final upstream request body: strip
// private `_`-prefixed params, then canonicalize key order. Always returns a
// fresh map; never mutates the input.
func PrepareCanonicalBody(body map[string]any) map[string]any {
	out, _ := Canonicalize(FilterPrivateParams(body)).(map[string]any)
	return out
}

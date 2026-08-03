package config

import "strings"

// DeepMerge merges src into target, mirroring deepMerge() in config.ts:
// nested plain objects are merged recursively, arrays are replaced (not
// concatenated), and the meta keys "$default" and "$delete" (dot-notation
// paths) are applied after the regular merge, in the order:
//
//	regular merge → $default → $delete
//
// A brand-new map is returned; neither input is ever modified (the TS
// reference shares references and can mutate inputs in edge cases; Go keeps
// them untouched, which is observable-equivalent on all ported vectors).
func DeepMerge(target, src map[string]any) map[string]any {
	// --- separate meta keys from regular data keys ---
	var deletePaths []string
	defaults := map[string]any{}
	sourceData := map[string]any{}

	for key, val := range src {
		switch key {
		case "$delete":
			if arr, ok := val.([]any); ok {
				for _, item := range arr {
					if s, ok := item.(string); ok {
						deletePaths = append(deletePaths, s)
					}
				}
			}
		case "$default":
			if m, ok := val.(map[string]any); ok {
				for k, v := range m {
					defaults[k] = v
				}
			}
		default:
			sourceData[key] = val
		}
	}

	// --- step 1: regular deep merge (sourceData only, no meta keys) ---
	result := make(map[string]any, len(target))
	for k, v := range target {
		result[k] = v
	}
	for key, sv := range sourceData {
		if tv, ok := result[key]; ok && isObject(tv) && isObject(sv) {
			result[key] = DeepMerge(tv.(map[string]any), sv.(map[string]any))
		} else {
			result[key] = sv
		}
	}

	// --- step 2: $default — only set paths that don't exist yet ---
	for path, value := range defaults {
		if _, exists := pathGet(result, path); !exists {
			pathSet(result, path, value)
		}
	}

	// --- step 3: $delete — remove specified paths ---
	for _, path := range deletePaths {
		pathDelete(result, path)
	}

	return result
}

// isObject reports whether v is a plain object (non-nil map), matching the TS
// check `v !== null && typeof v === "object" && !Array.isArray(v)`.
func isObject(v any) bool {
	_, ok := v.(map[string]any)
	return ok
}

// ---- dot-notation path helpers (operate on plain objects only) ----

// pathGet reads the value at a dot-notation path; exists is false when any
// segment is missing or the path traverses through a non-object.
func pathGet(obj map[string]any, path string) (value any, exists bool) {
	keys := strings.Split(path, ".")
	cur := any(obj)
	for _, key := range keys {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, exists = m[key]
		if !exists {
			return nil, false
		}
	}
	return cur, true
}

// pathSet sets a value at a dot-notation path, creating intermediate objects
// as needed. Existing maps along the path are cloned before being written
// through, so shared (input) objects are never modified — writes always land
// on fresh maps owned by the result tree.
func pathSet(obj map[string]any, path string, value any) {
	keys := strings.Split(path, ".")
	cur := obj
	for i := 0; i < len(keys)-1; i++ {
		k := keys[i]
		next, ok := cur[k]
		m, isMap := next.(map[string]any)
		if !ok || !isMap {
			m = map[string]any{}
			cur[k] = m
			cur = m
			continue
		}
		clone := make(map[string]any, len(m))
		for kk, vv := range m {
			clone[kk] = vv
		}
		cur[k] = clone
		cur = clone
	}
	cur[keys[len(keys)-1]] = value
}

// pathDelete removes the value at a dot-notation path, cloning maps along the
// way so shared (input) objects are never modified. Silently no-ops if any
// segment along the path is missing or is not a plain object.
func pathDelete(obj map[string]any, path string) {
	keys := strings.Split(path, ".")
	cur := obj
	for i := 0; i < len(keys)-1; i++ {
		k := keys[i]
		next, ok := cur[k]
		m, isMap := next.(map[string]any)
		if !ok || !isMap {
			return
		}
		clone := make(map[string]any, len(m))
		for kk, vv := range m {
			clone[kk] = vv
		}
		cur[k] = clone
		cur = clone
	}
	delete(cur, keys[len(keys)-1])
}

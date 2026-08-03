package config

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestDeepMergeBasics(t *testing.T) {
	// 常规合并：嵌套对象合并、数组替换、null 覆盖
	target := map[string]any{"a": 1, "nested": map[string]any{"x": 1, "y": 2}, "arr": []any{1, 2}}
	src := map[string]any{"a": 2, "nested": map[string]any{"y": 3}, "arr": []any{9}, "z": nil}
	got := DeepMerge(target, src)
	if got["a"] != 2 {
		t.Errorf("a = %v", got["a"])
	}
	if got["nested"].(map[string]any)["x"] != 1 || got["nested"].(map[string]any)["y"] != 3 {
		t.Errorf("nested merge: %v", got["nested"])
	}
	if !reflect.DeepEqual(got["arr"], []any{9}) {
		t.Errorf("arr should be replaced: %v", got["arr"])
	}
	if got["z"] != nil {
		t.Errorf("z should be nil: %v", got["z"])
	}
}

// Adapted from the brief: the brief expected $default to re-fill thinking.budget_tokens
// (10000) after $delete removed it, but the TS reference (config.ts) applies $default
// BEFORE $delete: the pre-existing 20000 blocks the default, then $delete removes it.
// Verified empirically against the TS implementation; the TS test "$delete can remove
// keys set by $default or merge" pins this order. TS behavior wins per the migration
// contract, so budget_tokens must be absent, not 10000.
func TestDeepMergeDeleteAndDefault(t *testing.T) {
	target := map[string]any{
		"temperature": 0.2,
		"thinking":    map[string]any{"type": "enabled", "budget_tokens": 20000},
		"user":        "existing",
		"seed":        1,
	}
	src := map[string]any{
		"$delete":     []any{"thinking.budget_tokens", "user", "seed", "missing.path"},
		"$default":    map[string]any{"max_tokens": 4096, "thinking.budget_tokens": 10000},
		"temperature": 0.7,
	}
	got := DeepMerge(target, src)
	if got["temperature"] != 0.7 {
		t.Errorf("temperature = %v", got["temperature"])
	}
	if got["max_tokens"] != 4096 {
		t.Errorf("max_tokens = %v", got["max_tokens"])
	}
	if _, ok := got["user"]; ok {
		t.Error("user should be deleted")
	}
	if _, ok := got["seed"]; ok {
		t.Error("seed should be deleted")
	}
	th := got["thinking"].(map[string]any)
	if th["type"] != "enabled" {
		t.Errorf("thinking.type = %v", th["type"])
	}
	if _, ok := th["budget_tokens"]; ok {
		t.Error("thinking.budget_tokens should be deleted ($default runs before $delete per TS)")
	}
	// 入参不被修改（TS 版会在这里修改 target.thinking；Go 版保证不动入参）
	if target["temperature"] != 0.2 {
		t.Error("target must not be mutated")
	}
	if target["thinking"].(map[string]any)["budget_tokens"] != 20000 {
		t.Error("target.thinking must not be mutated")
	}
}

func TestDeepMergeDefaultDoesNotOverwrite(t *testing.T) {
	target := map[string]any{"max_tokens": 2048}
	src := map[string]any{"$default": map[string]any{"max_tokens": 4096, "temperature": 0.7}}
	got := DeepMerge(target, src)
	if got["max_tokens"] != 2048 {
		t.Errorf("existing value must not be overwritten: %v", got["max_tokens"])
	}
	if got["temperature"] != 0.7 {
		t.Errorf("missing should be set: %v", got["temperature"])
	}
}

// The brief's json.Unmarshal (no UseNumber) would yield float64(4096), contradicting
// its own json.Number("4096") assertion and the global UseNumber constraint, so the
// input is decoded with a UseNumber decoder.
func TestDeepMergeJSONNumbers(t *testing.T) {
	dec := json.NewDecoder(strings.NewReader(`{"$default":{"max_tokens":4096}}`))
	dec.UseNumber()
	var src map[string]any
	if err := dec.Decode(&src); err != nil {
		t.Fatal(err)
	}
	got := DeepMerge(map[string]any{}, src)
	if got["max_tokens"] != json.Number("4096") {
		t.Errorf("expected json.Number: %v (%T)", got["max_tokens"], got["max_tokens"])
	}
}

// ---- ported from tests/config.test.ts "deepMerge" ----

// "merges flat objects"
func TestDeepMergeMergesFlatObjects(t *testing.T) {
	got := DeepMerge(map[string]any{"a": 1, "b": 2}, map[string]any{"b": 3, "c": 4})
	want := map[string]any{"a": 1, "b": 3, "c": 4}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "recursively merges nested objects"
func TestDeepMergeRecursivelyMergesNestedObjects(t *testing.T) {
	target := map[string]any{"thinking": map[string]any{"type": "disabled"}, "model": "x"}
	source := map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 10000}}
	got := DeepMerge(target, source)
	want := map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 10000}, "model": "x"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "replaces arrays instead of concatenating"
func TestDeepMergeReplacesArrays(t *testing.T) {
	got := DeepMerge(map[string]any{"tags": []any{1, 2}}, map[string]any{"tags": []any{3}})
	want := map[string]any{"tags": []any{3}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "handles null and primitive overrides"
func TestDeepMergeNullAndPrimitiveOverrides(t *testing.T) {
	got := DeepMerge(map[string]any{"a": map[string]any{"b": 1}}, map[string]any{"a": nil})
	want := map[string]any{"a": nil}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	got = DeepMerge(map[string]any{"a": map[string]any{"b": 1}}, map[string]any{"a": "string"})
	want = map[string]any{"a": "string"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "does not mutate target"
func TestDeepMergeDoesNotMutateTarget(t *testing.T) {
	target := map[string]any{"x": map[string]any{"y": 1}}
	got := DeepMerge(target, map[string]any{"x": map[string]any{"z": 2}})
	want := map[string]any{"x": map[string]any{"y": 1, "z": 2}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if !reflect.DeepEqual(target, map[string]any{"x": map[string]any{"y": 1}}) {
		t.Errorf("target must not be mutated: %v", target)
	}
}

// "$delete removes top-level keys"
func TestDeepMergeDeleteRemovesTopLevelKeys(t *testing.T) {
	got := DeepMerge(map[string]any{"a": 1, "b": 2, "c": 3}, map[string]any{"$delete": []any{"a", "c"}})
	want := map[string]any{"b": 2}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "$delete with dot-notation path removes nested keys"
func TestDeepMergeDeleteDotNotationPath(t *testing.T) {
	got := DeepMerge(
		map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 10000}},
		map[string]any{"$delete": []any{"thinking.budget_tokens"}},
	)
	want := map[string]any{"thinking": map[string]any{"type": "enabled"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "$delete silently no-ops when path does not exist"
func TestDeepMergeDeleteNoOpWhenMissing(t *testing.T) {
	got := DeepMerge(map[string]any{"a": 1}, map[string]any{"$delete": []any{"b", "c.d.e"}})
	want := map[string]any{"a": 1}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "$delete at nested level removes keys relative to that level"
func TestDeepMergeDeleteNestedLevel(t *testing.T) {
	got := DeepMerge(
		map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 10000, "extra": "x"}},
		map[string]any{"thinking": map[string]any{"$delete": []any{"budget_tokens", "extra"}}},
	)
	want := map[string]any{"thinking": map[string]any{"type": "enabled"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "$delete non-array values are ignored (no-op)"
func TestDeepMergeDeleteNonArrayIgnored(t *testing.T) {
	got := DeepMerge(map[string]any{"a": 1}, map[string]any{"$delete": "not-an-array"})
	want := map[string]any{"a": 1}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "$default sets missing top-level keys"
func TestDeepMergeDefaultSetsMissingTopLevel(t *testing.T) {
	got := DeepMerge(map[string]any{"existing": 1}, map[string]any{"$default": map[string]any{"max_tokens": 4096, "temperature": 0.7}})
	want := map[string]any{"existing": 1, "max_tokens": 4096, "temperature": 0.7}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "$default with dot-notation path sets nested defaults"
func TestDeepMergeDefaultDotNotationSetsNested(t *testing.T) {
	got := DeepMerge(
		map[string]any{"thinking": map[string]any{"type": "enabled"}},
		map[string]any{"$default": map[string]any{"thinking.budget_tokens": 10000}},
	)
	want := map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 10000}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "$default with dot-notation does not overwrite existing nested values"
func TestDeepMergeDefaultDotNotationNoOverwrite(t *testing.T) {
	got := DeepMerge(
		map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 5000}},
		map[string]any{"$default": map[string]any{"thinking.budget_tokens": 10000}},
	)
	want := map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 5000}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "$default creates intermediate objects for missing paths"
func TestDeepMergeDefaultCreatesIntermediateObjects(t *testing.T) {
	got := DeepMerge(map[string]any{"a": 1}, map[string]any{"$default": map[string]any{"thinking.type": "enabled"}})
	want := map[string]any{"a": 1, "thinking": map[string]any{"type": "enabled"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "$default nested level applies relative to that level"
func TestDeepMergeDefaultNestedLevel(t *testing.T) {
	got := DeepMerge(
		map[string]any{"thinking": map[string]any{"type": "enabled"}},
		map[string]any{"thinking": map[string]any{"$default": map[string]any{"budget_tokens": 10000}}},
	)
	want := map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 10000}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "mixes $delete, $default, and regular merge (order: merge → default → delete)"
func TestDeepMergeMixDeleteDefaultMerge(t *testing.T) {
	got := DeepMerge(
		map[string]any{"keep": 1, "user": "old", "seed": 42},
		map[string]any{"$delete": []any{"user", "seed"}, "$default": map[string]any{"max_tokens": 4096}, "temperature": 0.2},
	)
	want := map[string]any{"keep": 1, "temperature": 0.2, "max_tokens": 4096}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "$default sees values set by regular merge (won't overwrite them)"
func TestDeepMergeDefaultSeesMergeSetValues(t *testing.T) {
	got := DeepMerge(map[string]any{}, map[string]any{"max_tokens": 2048, "$default": map[string]any{"max_tokens": 4096}})
	want := map[string]any{"max_tokens": 2048}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "$delete can remove keys set by $default or merge"
func TestDeepMergeDeleteRemovesDefaultSetKeys(t *testing.T) {
	got := DeepMerge(map[string]any{}, map[string]any{"$default": map[string]any{"max_tokens": 4096}, "$delete": []any{"max_tokens"}})
	want := map[string]any{}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// "$delete and $default keys themselves do not appear in result"
func TestDeepMergeMetaKeysAbsentFromResult(t *testing.T) {
	got := DeepMerge(map[string]any{"a": 1}, map[string]any{"$delete": []any{"b"}, "$default": map[string]any{"c": 2}})
	if _, ok := got["$delete"]; ok {
		t.Error("$delete must not appear in result")
	}
	if _, ok := got["$default"]; ok {
		t.Error("$default must not appear in result")
	}
	want := map[string]any{"a": 1, "c": 2}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

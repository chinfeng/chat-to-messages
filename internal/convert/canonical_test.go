package convert

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestFilterPrivateParams(t *testing.T) {
	in := map[string]any{
		"_private": 1,
		"ok":       map[string]any{"_bad": 2, "good": 3, "nested": map[string]any{"_x": 4}},
		"schema": map[string]any{
			"properties": map[string]any{"_id": map[string]any{"type": "string"}},
		},
	}
	got := FilterPrivateParams(in)
	if _, ok := got["_private"]; ok {
		t.Error("_private must be stripped")
	}
	okMap := got["ok"].(map[string]any)
	if _, bad := okMap["_bad"]; bad {
		t.Error("_bad must be stripped")
	}
	if okMap["good"] != 3 {
		t.Error("good must remain")
	}
	if _, x := okMap["nested"].(map[string]any)["_x"]; x {
		t.Error("nested _x must be stripped")
	}
	if _, id := got["schema"].(map[string]any)["properties"].(map[string]any)["_id"]; !id {
		t.Error("_id in properties is a legit schema name and must be kept")
	}
}

// TS: drops `_`-prefixed keys recursively, including inside arrays.
func TestFilterPrivateParamsRecursiveAndArrays(t *testing.T) {
	in := map[string]any{
		"messages": []any{
			map[string]any{"role": "user", "_trace": "t", "content": "hi"},
		},
		"_meta": map[string]any{"keep": "no"},
	}
	got := FilterPrivateParams(in)
	if _, ok := got["_meta"]; ok {
		t.Error("_meta must be stripped")
	}
	msgs := got["messages"].([]any)
	m := msgs[0].(map[string]any)
	if _, ok := m["_trace"]; ok {
		t.Error("_trace must be stripped inside arrays")
	}
	if m["content"] != "hi" {
		t.Errorf("content = %v", m["content"])
	}
}

// TS: preserves `_`-prefixed names inside JSON-Schema name maps
// (properties/patternProperties/definitions/$defs).
func TestFilterPrivateParamsSchemaNameMaps(t *testing.T) {
	in := map[string]any{
		"properties": map[string]any{
			"_id":  map[string]any{"type": "string"},
			"name": map[string]any{"type": "string"},
		},
		"patternProperties": map[string]any{"^x_": map[string]any{"type": "number"}},
		"definitions":       map[string]any{"_Inner": map[string]any{"type": "object"}},
		"$defs":             map[string]any{"_Refd": map[string]any{"type": "string"}},
		"_internal":         "drop me",
	}
	got := FilterPrivateParams(in)
	if _, ok := got["_internal"]; ok {
		t.Error("_internal must be stripped")
	}
	props := got["properties"].(map[string]any)
	if _, ok := props["_id"]; !ok {
		t.Error("_id in properties is a legit schema name and must be kept")
	}
	if _, ok := props["name"]; !ok {
		t.Error("name in properties must be kept")
	}
	if _, ok := got["patternProperties"].(map[string]any)["^x_"]; !ok {
		t.Error("^x_ in patternProperties must be kept")
	}
	if _, ok := got["definitions"].(map[string]any)["_Inner"]; !ok {
		t.Error("_Inner in definitions must be kept")
	}
	if _, ok := got["$defs"].(map[string]any)["_Refd"]; !ok {
		t.Error("_Refd in $defs must be kept")
	}
	// The schema object directly under the name map is filtered normally again —
	// a `_`-prefixed annotation inside one property's schema is dropped.
	in2 := map[string]any{
		"properties": map[string]any{"_id": map[string]any{"_annot": 1, "type": "string"}},
	}
	got2 := FilterPrivateParams(in2)
	schema := got2["properties"].(map[string]any)["_id"].(map[string]any)
	if _, ok := schema["_annot"]; ok {
		t.Error("_annot inside a property schema must be stripped")
	}
}

// TS: returns a fresh value and does not mutate the input.
func TestFilterPrivateParamsDoesNotMutate(t *testing.T) {
	in := map[string]any{"_a": 1, "b": 2}
	out := FilterPrivateParams(in)
	if _, ok := in["_a"]; !ok {
		t.Error("input must not be mutated")
	}
	if !reflect.DeepEqual(out, map[string]any{"b": 2}) {
		t.Errorf("out = %v", out)
	}
}

func TestCanonicalizeAndStringify(t *testing.T) {
	in := map[string]any{"b": 2, "a": 1, "arr": []any{3, 1, 2}}
	got := Canonicalize(in)
	if !reflect.DeepEqual(got, map[string]any{"a": 1, "b": 2, "arr": []any{3, 1, 2}}) {
		t.Errorf("got %v", got)
	}
	s, err := CanonicalJSONStringify(map[string]any{"z": 1, "a": []any{"x", "y"}})
	if err != nil {
		t.Fatal(err)
	}
	if s != `{"a":["x","y"],"z":1}` {
		t.Errorf("s = %s", s)
	}
}

// TS: sorts object keys recursively (arrays preserved, primitives unchanged).
func TestCanonicalizeSortsKeysRecursively(t *testing.T) {
	in := map[string]any{"c": 1, "a": map[string]any{"z": 1, "b": 2}, "m": []any{3, 1, 2}}
	got := Canonicalize(in)
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"a":{"b":2,"z":1},"c":1,"m":[3,1,2]}` {
		t.Errorf("marshaled = %s", b)
	}
}

// TS: returns a fresh value and does not mutate the input.
func TestCanonicalizeDoesNotMutate(t *testing.T) {
	in := map[string]any{"b": 1, "a": 2}
	out := Canonicalize(in).(map[string]any)
	if len(in) != 2 {
		t.Error("input must not be mutated")
	}
	if len(out) != 2 {
		t.Errorf("out = %v", out)
	}
}

// TS: returns primitives/null/undefined unchanged (identity for scope types).
func TestCanonicalizePrimitivesIdentity(t *testing.T) {
	if got := Canonicalize(42); got != 42 {
		t.Errorf("42 → %v", got)
	}
	if got := Canonicalize("x"); got != "x" {
		t.Errorf("x → %v", got)
	}
	if got := Canonicalize(nil); got != nil {
		t.Errorf("nil → %v", got)
	}
	if got := Canonicalize(true); got != true {
		t.Errorf("true → %v", got)
	}
}

func TestCanonicalJSONStringifyNumberFidelity(t *testing.T) {
	var v map[string]any
	dec := json.NewDecoder(strings.NewReader(`{"big":12345678901234567890}`))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatal(err)
	}
	s, err := CanonicalJSONStringify(v)
	if err != nil {
		t.Fatal(err)
	}
	if s != `{"big":12345678901234567890}` {
		t.Errorf("s = %s", s)
	}
}

// TS: is stable regardless of input key order.
func TestCanonicalJSONStringifyStable(t *testing.T) {
	a, err := CanonicalJSONStringify(map[string]any{"b": 2, "a": 1})
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalJSONStringify(map[string]any{"a": 1, "b": 2})
	if err != nil {
		t.Fatal(err)
	}
	if a != b || a != `{"a":1,"b":2}` {
		t.Errorf("a = %s, b = %s", a, b)
	}
}

// TS: canonicalizes nested object keys inside arrays.
func TestCanonicalJSONStringifyNestedArrayObjects(t *testing.T) {
	s, err := CanonicalJSONStringify([]any{map[string]any{"y": 2, "x": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if s != `[{"x":1,"y":2}]` {
		t.Errorf("s = %s", s)
	}
}

func TestPrepareCanonicalBody(t *testing.T) {
	body := map[string]any{"_x": 1, "b": 2, "a": 3}
	got := PrepareCanonicalBody(body)
	if _, ok := got["_x"]; ok {
		t.Error("_x must be stripped")
	}
	// 键排序由 json.Marshal 保证
	b, _ := json.Marshal(got)
	if string(b) != `{"a":3,"b":2}` {
		t.Errorf("b = %s", b)
	}
	// 不修改入参
	if _, ok := body["_x"]; !ok {
		t.Error("input must not be mutated")
	}
}

// TS: filters private params then canonicalizes key order.
func TestPrepareCanonicalBodyFiltersThenCanonicalizes(t *testing.T) {
	got := PrepareCanonicalBody(map[string]any{"z": 1, "a": 2, "_hidden": 3})
	if len(got) != 2 {
		t.Fatalf("len = %d: %v", len(got), got)
	}
	if got["a"] != 2 {
		t.Errorf("a = %v", got["a"])
	}
}

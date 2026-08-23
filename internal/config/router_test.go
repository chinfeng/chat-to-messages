package config

import (
	"strings"
	"testing"
)

func mustRouter(t *testing.T, ups []*Upstream, routes []Route) *Router {
	t.Helper()
	rt, err := NewRouter(ups, routes)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return rt
}

func TestResolveFirstMatchAndRotation(t *testing.T) {
	ups := []*Upstream{{Name: "a", BaseURL: "http://a"}, {Name: "b", BaseURL: "http://b"}, {Name: "c", BaseURL: "http://c"}}
	rt := mustRouter(t, ups, []Route{
		{Pattern: "glm-*", Names: []string{"a", "b"}},
		{Pattern: "*", Names: []string{"c"}},
	})
	// 声明序首匹配;池内轮询:n 次请求均匀覆盖。
	got := rt.Resolve("glm-4.7")
	if len(got) != 2 || got[0].Name != "a" || got[1].Name != "b" {
		t.Fatalf("first resolve = [%s %s]", got[0].Name, got[1].Name)
	}
	got = rt.Resolve("glm-4.7")
	if got[0].Name != "b" || got[1].Name != "a" {
		t.Fatalf("second resolve should rotate: [%s %s]", got[0].Name, got[1].Name)
	}
	got = rt.Resolve("glm-4.7")
	if got[0].Name != "a" {
		t.Fatal("third resolve should wrap to start")
	}
	if name := rt.Resolve("other")[0].Name; name != "c" {
		t.Fatalf("catch-all route should hit c, got %s", name)
	}
}

func TestResolveNoMatch(t *testing.T) {
	rt := mustRouter(t, []*Upstream{{Name: "a", BaseURL: "http://a"}}, []Route{{Pattern: "glm-*", Names: []string{"a"}}})
	if got := rt.Resolve("deepseek-v4"); got != nil {
		t.Fatalf("no match should return nil, got %v", got)
	}
}

func TestDistinct(t *testing.T) {
	a := &Upstream{Name: "a", BaseURL: "http://a"}
	b := &Upstream{Name: "b", BaseURL: "http://b"}
	rt := mustRouter(t, []*Upstream{a, b}, []Route{
		{Pattern: "x-*", Names: []string{"a"}},
		{Pattern: "*", Names: []string{"b", "a"}}, // a 已出现,不重复
	})
	d := rt.Distinct()
	if len(d) != 2 || d[0] != a || d[1] != b {
		t.Fatal("Distinct must dedupe by pointer, declaration order")
	}
}

func TestNewRouterValidation(t *testing.T) {
	cases := []struct {
		name      string
		ups       []*Upstream
		routes    []Route
		wantPanic string // err.Error() 包含
	}{
		{"dangling ref", []*Upstream{{Name: "a", BaseURL: "http://a"}}, []Route{{Pattern: "*", Names: []string{"ghost"}}}, "unknown upstream"},
		{"dup name", []*Upstream{{Name: "a", BaseURL: "http://a"}, {Name: "a", BaseURL: "http://a"}}, []Route{{Pattern: "*", Names: []string{"a"}}}, "duplicate"},
		{"empty baseURL", []*Upstream{{Name: "a"}}, []Route{{Pattern: "*", Names: []string{"a"}}}, "baseUrl"},
		{"empty pool", []*Upstream{{Name: "a", BaseURL: "http://a"}}, []Route{{Pattern: "*", Names: nil}}, "empty"},
	}
	for _, tc := range cases {
		_, err := NewRouter(tc.ups, tc.routes)
		if err == nil || !strings.Contains(err.Error(), tc.wantPanic) {
			t.Errorf("%s: want err containing %q, got %v", tc.name, tc.wantPanic, err)
		}
	}
}

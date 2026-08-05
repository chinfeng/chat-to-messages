package config

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	cfg := Load(nil)
	if cfg.UpstreamBaseURL != "https://api.openai.com/v1" {
		t.Errorf("UpstreamBaseURL = %q", cfg.UpstreamBaseURL)
	}
	if cfg.Port != 8082 {
		t.Errorf("Port = %d", cfg.Port)
	}
	if !cfg.EnableThinking {
		t.Error("EnableThinking should default true")
	}
	if cfg.ServerTools.WebSearchEngine != "brave" {
		t.Errorf("engine = %q", cfg.ServerTools.WebSearchEngine)
	}
	if cfg.ServerTools.WebFetchMaxContentTokens != 5000 {
		t.Errorf("max tokens = %d", cfg.ServerTools.WebFetchMaxContentTokens)
	}
}

// Ported from tests/config.test.ts "loads default config values".
func TestLoadDefaultsAllFields(t *testing.T) {
	cfg := Load(nil)
	if cfg.UpstreamBaseURL != "https://api.openai.com/v1" {
		t.Errorf("upstreamBaseUrl = %q", cfg.UpstreamBaseURL)
	}
	if cfg.UpstreamAPIKey != "" {
		t.Errorf("upstreamApiKey = %q", cfg.UpstreamAPIKey)
	}
	if cfg.AuthToken != "" {
		t.Errorf("authToken = %q", cfg.AuthToken)
	}
	if cfg.Port != 8082 {
		t.Errorf("port = %d", cfg.Port)
	}
	if !cfg.EnableThinking {
		t.Error("enableThinking should be true")
	}
	if cfg.DumpDir != "" {
		t.Errorf("dumpDir = %q", cfg.DumpDir)
	}
	if len(cfg.ModelOverrides) != 0 {
		t.Errorf("modelOverrides = %v", cfg.ModelOverrides)
	}
	if cfg.ServerTools.WebSearch {
		t.Error("webSearch should be false")
	}
	if cfg.ServerTools.WebFetch {
		t.Error("webFetch should be false")
	}
	if cfg.ServerTools.WebSearchAPIKey != "" {
		t.Errorf("webSearchApiKey = %q", cfg.ServerTools.WebSearchAPIKey)
	}
	if cfg.ServerTools.WebSearchBaseURL != "https://api.search.brave.com" {
		t.Errorf("webSearchBaseUrl = %q", cfg.ServerTools.WebSearchBaseURL)
	}
	if len(cfg.ServerTools.WebFetchAllowedDomains) != 0 {
		t.Errorf("allowedDomains = %v", cfg.ServerTools.WebFetchAllowedDomains)
	}
	if len(cfg.ServerTools.WebFetchBlockedDomains) != 0 {
		t.Errorf("blockedDomains = %v", cfg.ServerTools.WebFetchBlockedDomains)
	}
	if cfg.ServerTools.WebFetchMaxContentTokens != 5000 {
		t.Errorf("maxContentTokens = %d", cfg.ServerTools.WebFetchMaxContentTokens)
	}
}

func TestLoadArgForms(t *testing.T) {
	cfg := Load([]string{
		"--upstream-base-url", "https://example.com/v1", // 空格分隔
		"--auth-token=secret", // = 分隔
		"--port", "9999",
		"--no-enable-thinking",     // 否定式
		"--enable-web-search",      // 纯开关
		"--enable-web-fetch=false", // = 假值
		"--web-fetch-allowed-domain", "a.com",
		"--web-fetch-allowed-domain=b.com", // 可重复
		"--web-fetch-blocked-domain", "c.com",
		"--upstream-extra-params", `claude-*={"thinking":{"type":"enabled","budget_tokens":10000}}`,
	})
	if cfg.UpstreamBaseURL != "https://example.com/v1" {
		t.Errorf("base url = %q", cfg.UpstreamBaseURL)
	}
	if cfg.AuthToken != "secret" {
		t.Errorf("auth = %q", cfg.AuthToken)
	}
	if cfg.Port != 9999 {
		t.Errorf("port = %d", cfg.Port)
	}
	if cfg.EnableThinking {
		t.Error("thinking should be false")
	}
	if !cfg.ServerTools.WebSearch {
		t.Error("web search should be true")
	}
	if cfg.ServerTools.WebFetch {
		t.Error("web fetch should be false")
	}
	if !reflect.DeepEqual(cfg.ServerTools.WebFetchAllowedDomains, []string{"a.com", "b.com"}) {
		t.Error("allowed domains")
	}
	if !reflect.DeepEqual(cfg.ServerTools.WebFetchBlockedDomains, []string{"c.com"}) {
		t.Error("blocked domains")
	}
	if len(cfg.ModelOverrides) != 1 {
		t.Fatalf("overrides = %d", len(cfg.ModelOverrides))
	}
	if cfg.ModelOverrides[0].Pattern != "claude-*" {
		t.Errorf("pattern = %q", cfg.ModelOverrides[0].Pattern)
	}
	if th, ok := cfg.ModelOverrides[0].Extra["thinking"].(map[string]any); !ok || th["budget_tokens"] != json.Number("10000") {
		t.Error("extra not parsed with UseNumber")
	}
}

// Ported from tests/config.test.ts "reads CLI arguments".
func TestLoadReadsCLIArguments(t *testing.T) {
	cfg := Load([]string{
		"--upstream-base-url", "https://custom.api/v1",
		"--upstream-api-key", "sk-test",
		"--auth-token", "my-token",
		"--port", "9090",
		"--no-enable-thinking",
		"--dump", "/tmp/dumps",
	})
	if cfg.UpstreamBaseURL != "https://custom.api/v1" {
		t.Errorf("base url = %q", cfg.UpstreamBaseURL)
	}
	if cfg.UpstreamAPIKey != "sk-test" {
		t.Errorf("api key = %q", cfg.UpstreamAPIKey)
	}
	if cfg.AuthToken != "my-token" {
		t.Errorf("auth = %q", cfg.AuthToken)
	}
	if cfg.Port != 9090 {
		t.Errorf("port = %d", cfg.Port)
	}
	if cfg.EnableThinking {
		t.Error("thinking should be false")
	}
	if cfg.DumpDir != "/tmp/dumps" {
		t.Errorf("dump = %q", cfg.DumpDir)
	}
}

// TestLoadPortRangeValidation: ports outside 1-65535 (including the 0 that
// parseInt yields for unparsable values) warn and fall back to the default
// 8082; in-range values are kept verbatim.
func TestLoadPortRangeValidation(t *testing.T) {
	for _, tt := range []struct {
		args []string
		want int
	}{
		{args: []string{"--port", "0"}, want: 8082},            // parseInt fallback / invalid low
		{args: []string{"--port", "-1"}, want: 8082},           // negative
		{args: []string{"--port", "65536"}, want: 8082},        // above the range
		{args: []string{"--port", "not-a-number"}, want: 8082}, // unparsable → 0 → fallback
		{args: []string{"--port", "1"}, want: 1},               // in-range boundary kept
		{args: []string{"--port", "65535"}, want: 65535},       // in-range boundary kept
		{args: nil, want: 8082},                                // default
	} {
		if got := Load(tt.args).Port; got != tt.want {
			t.Errorf("Load(%v).Port = %d, want %d", tt.args, got, tt.want)
		}
	}
}

// Ported from tests/config.test.ts "reads server tool CLI arguments".
func TestLoadServerToolArguments(t *testing.T) {
	cfg := Load([]string{
		"--enable-web-search",
		"--enable-web-fetch",
		"--web-search-api-key", "BST-xxx",
		"--web-search-base-url", "https://custom.search.api",
		"--web-fetch-allowed-domain", "example.com",
		"--web-fetch-allowed-domain", "docs.example.com",
		"--web-fetch-blocked-domain", "spam.com",
		"--web-fetch-max-content-tokens", "10000",
	})
	if !cfg.ServerTools.WebSearch {
		t.Error("web search should be true")
	}
	if !cfg.ServerTools.WebFetch {
		t.Error("web fetch should be true")
	}
	if cfg.ServerTools.WebSearchAPIKey != "BST-xxx" {
		t.Errorf("api key = %q", cfg.ServerTools.WebSearchAPIKey)
	}
	if cfg.ServerTools.WebSearchBaseURL != "https://custom.search.api" {
		t.Errorf("base url = %q", cfg.ServerTools.WebSearchBaseURL)
	}
	if !reflect.DeepEqual(cfg.ServerTools.WebFetchAllowedDomains, []string{"example.com", "docs.example.com"}) {
		t.Error("allowed domains")
	}
	if !reflect.DeepEqual(cfg.ServerTools.WebFetchBlockedDomains, []string{"spam.com"}) {
		t.Error("blocked domains")
	}
	if cfg.ServerTools.WebFetchMaxContentTokens != 10000 {
		t.Errorf("max tokens = %d", cfg.ServerTools.WebFetchMaxContentTokens)
	}
}

// Ported from tests/config.test.ts "parses --upstream-extra-params with glob=JSON".
func TestLoadExtraParamsGlobJSON(t *testing.T) {
	cfg := Load([]string{
		"--upstream-extra-params", `claude-*={"thinking":{"type":"enabled","budget_tokens":10000}}`,
		"--upstream-extra-params", `deepseek*={"reasoning_effort":"high"}`,
	})
	want := []ModelOverride{
		{Pattern: "claude-*", Extra: map[string]any{
			"thinking": map[string]any{"type": "enabled", "budget_tokens": json.Number("10000")},
		}},
		{Pattern: "deepseek*", Extra: map[string]any{"reasoning_effort": "high"}},
	}
	if !reflect.DeepEqual(cfg.ModelOverrides, want) {
		t.Errorf("overrides = %#v, want %#v", cfg.ModelOverrides, want)
	}
}

// Ported from tests/config.test.ts "supports --upstream-extra-params= format".
func TestLoadExtraParamsEqualsForm(t *testing.T) {
	cfg := Load([]string{`--upstream-extra-params=*={"stream":true}`})
	want := []ModelOverride{
		{Pattern: "*", Extra: map[string]any{"stream": true}},
	}
	if !reflect.DeepEqual(cfg.ModelOverrides, want) {
		t.Errorf("overrides = %#v, want %#v", cfg.ModelOverrides, want)
	}
}

func TestLoadSkipsInvalidExtraParams(t *testing.T) {
	cfg := Load([]string{
		"--upstream-extra-params", "no-equals-sign",
		"--upstream-extra-params", "pat=not-json",
		"--upstream-extra-params", "pat2=[1,2]",
	})
	if len(cfg.ModelOverrides) != 0 {
		t.Errorf("expected 0 overrides, got %d", len(cfg.ModelOverrides))
	}
}

// Ported from tests/config.test.ts "skips invalid --upstream-extra-params entries gracefully":
// good*={"ok":1} survives while no-equal-sign, bad={not json} and arr*=[1,2] are skipped.
func TestLoadSkipsInvalidExtraParamsKeepsValid(t *testing.T) {
	cfg := Load([]string{
		"--upstream-extra-params", "no-equal-sign",
		"--upstream-extra-params", `good*={"ok":1}`,
		"--upstream-extra-params", "bad={not json}",
		"--upstream-extra-params", `arr*=[1,2]`,
	})
	want := []ModelOverride{
		{Pattern: "good*", Extra: map[string]any{"ok": json.Number("1")}},
	}
	if !reflect.DeepEqual(cfg.ModelOverrides, want) {
		t.Errorf("overrides = %#v, want %#v", cfg.ModelOverrides, want)
	}
}

// GlobMatch: brief core vectors plus every case from tests/config.test.ts "globMatch".
func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, text string
		want          bool
	}{
		// brief core vectors
		{"claude-sonnet-*", "claude-sonnet-4-20250514", true},
		{"claude-sonnet-*", "claude-opus-4", false},
		{"*", "anything", true},
		{"deepseek?", "deepseek1", true},
		{"deepseek?", "deepseek-r1", false},
		// "matches exact strings"
		{"gpt-4o", "gpt-4o", true},
		{"gpt-4o", "gpt-4", false},
		// "matches wildcard *"
		{"claude-*", "claude-sonnet-4", true},
		{"claude-*", "claude-opus-4-20250514", true},
		{"claude-*", "gpt-4o", false},
		// "matches wildcard in the middle"
		{"deepseek-*-pro", "deepseek-v4-pro", true},
		{"deepseek-*-pro", "deepseek-v3-pro", true},
		{"deepseek-*-pro", "deepseek-v4-chat", false},
		// "matches multiple wildcards"
		{"*-*", "claude-sonnet", true},
		{"*-*", "single", false},
		// "matches ? as single char"
		{"model-?", "model-a", true},
		{"model-?", "model-ab", false},
		// "matches * as catch-all (including empty string)"
		{"*", "", true},
		// "escapes regex special chars in pattern"
		{"model.v2*", "model.v2-large", true},
		{"model.v2*", "modelXv2-large", false},
	}
	for _, c := range cases {
		if got := GlobMatch(c.pattern, c.text); got != c.want {
			t.Errorf("GlobMatch(%q, %q) = %v, want %v", c.pattern, c.text, got, c.want)
		}
	}
}

// ResolveModelExtra: brief vectors plus every case from tests/config.test.ts "resolveModelExtra".
func TestResolveModelExtraFirstMatchWins(t *testing.T) {
	overrides := []ModelOverride{
		{Pattern: "claude-sonnet-*", Extra: map[string]any{"a": 1}},
		{Pattern: "*", Extra: map[string]any{"b": 2}},
	}
	if got := ResolveModelExtra("claude-sonnet-4", overrides); got["a"] != 1 {
		t.Errorf("first match should win: %v", got)
	}
	if got := ResolveModelExtra("other-model", overrides); got["b"] != 2 {
		t.Errorf("catch-all should match: %v", got)
	}
	if got := ResolveModelExtra("x", nil); len(got) != 0 {
		t.Errorf("nil overrides: %v", got)
	}
}

func TestResolveModelExtraFullOverrides(t *testing.T) {
	overrides := []ModelOverride{
		{Pattern: "claude-sonnet-*", Extra: map[string]any{
			"thinking": map[string]any{"type": "enabled", "budget_tokens": 10000},
		}},
		{Pattern: "deepseek*", Extra: map[string]any{"reasoning_effort": "high"}},
		{Pattern: "*", Extra: map[string]any{"stream": true}},
	}

	if got := ResolveModelExtra("claude-sonnet-4", overrides); !reflect.DeepEqual(got, map[string]any{
		"thinking": map[string]any{"type": "enabled", "budget_tokens": 10000},
	}) {
		t.Errorf("first matching pattern's extra: %v", got)
	}
	if got := ResolveModelExtra("deepseek-v4-pro", overrides); !reflect.DeepEqual(got, map[string]any{"reasoning_effort": "high"}) {
		t.Errorf("deepseek* pattern: %v", got)
	}
	if got := ResolveModelExtra("gpt-4o", overrides); !reflect.DeepEqual(got, map[string]any{"stream": true}) {
		t.Errorf("catch-all * pattern: %v", got)
	}
	if got := ResolveModelExtra("anything", []ModelOverride{}); len(got) != 0 {
		t.Errorf("empty overrides: %v", got)
	}
	if got := ResolveModelExtra("anything", nil); len(got) != 0 {
		t.Errorf("nil overrides: %v", got)
	}
}

// --convert-model collects repeatable glob patterns in order (--name value
// and --name=value forms both supported), mirroring --web-fetch-allowed-domain.
func TestLoadConvertModels(t *testing.T) {
	cfg := Load([]string{
		"--convert-model", "claude-*",
		"--convert-model=gpt-4o",
		"--convert-model", "deepseek-*-pro",
	})
	want := []string{"claude-*", "gpt-4o", "deepseek-*-pro"}
	if !reflect.DeepEqual(cfg.ConvertModels, want) {
		t.Errorf("ConvertModels = %#v, want %#v", cfg.ConvertModels, want)
	}
}

// Default: no --convert-model → empty list (ShouldConvert then returns true
// for any model, i.e. convert all — backward compatible).
func TestLoadConvertModelsDefault(t *testing.T) {
	cfg := Load(nil)
	if len(cfg.ConvertModels) != 0 {
		t.Errorf("ConvertModels = %#v, want empty", cfg.ConvertModels)
	}
}

// ShouldConvert: empty patterns → convert all; glob match → convert; multiple
// patterns union; non-match → false (native passthrough).
func TestShouldConvert(t *testing.T) {
	// empty patterns → convert everything (default behavior)
	if !ShouldConvert("anything", nil) {
		t.Error("empty patterns should convert all")
	}
	if !ShouldConvert("anything", []string{}) {
		t.Error("empty (non-nil) patterns should convert all")
	}

	// single glob: match converts, non-match passes through
	if !ShouldConvert("claude-sonnet-4", []string{"claude-*"}) {
		t.Error("claude-* should convert claude-sonnet-4")
	}
	if ShouldConvert("llama-3", []string{"claude-*"}) {
		t.Error("claude-* should not convert llama-3 (passthrough)")
	}

	// multiple patterns union (any match → convert)
	if !ShouldConvert("gpt-4o", []string{"claude-*", "gpt-4o"}) {
		t.Error("gpt-4o should convert via second pattern")
	}
	if !ShouldConvert("claude-opus-4", []string{"claude-*", "gpt-4o"}) {
		t.Error("claude-opus-4 should convert via first pattern")
	}
	if ShouldConvert("llama-3", []string{"claude-*", "gpt-4o"}) {
		t.Error("llama-3 should not convert (no pattern matches)")
	}
}

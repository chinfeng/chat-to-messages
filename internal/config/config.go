// Package config implements the CLI-argument-based server configuration,
// ported from chat-to-claude-code's src/server/config.ts.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Reasoning replay modes accepted by --reasoning-replay. String values mirror
// convert.ReasoningReplayMode; duplicated here as plain strings so this
// package stays dependency-free (proxy casts on use).
const (
	ReplayModeThinkTags        = "think_tags"
	ReplayModeReasoningContent = "reasoning_content"
	ReplayModeDisabled         = "disabled"
)

var reasoningReplayModes = map[string]bool{
	ReplayModeThinkTags:        true,
	ReplayModeReasoningContent: true,
	ReplayModeDisabled:         true,
}

// ModelOverride maps a model glob pattern to extra upstream params.
type ModelOverride struct {
	Pattern string
	Extra   map[string]any
}

// ReasoningReplayRule maps a model glob pattern to a reasoning replay mode.
type ReasoningReplayRule struct {
	Pattern string
	Mode    string
}

// ServerToolConfig holds the server-side tool (web_search / web_fetch) settings.
type ServerToolConfig struct {
	WebSearch                bool
	WebFetch                 bool
	WebSearchEngine          string
	WebSearchAPIKey          string
	WebSearchBaseURL         string
	WebFetchAllowedDomains   []string
	WebFetchBlockedDomains   []string
	WebFetchMaxContentTokens int
}

// Config is the server configuration produced by Load.
type Config struct {
	UpstreamBaseURL string
	UpstreamAPIKey  string
	AuthToken       string
	Port            int
	EnableThinking  bool
	DumpDir         string
	ModelOverrides  []ModelOverride
	ServerTools     ServerToolConfig

	// DefaultReasoningReplay is the replay mode used when no
	// ReasoningReplayRules entry matches the request model.
	DefaultReasoningReplay string
	// ReasoningReplayRules are per-model replay overrides (first match wins).
	ReasoningReplayRules []ReasoningReplayRule

	// SanitizeClientMetaTurns normalizes Claude Code's synthetic user meta
	// turns ("[Your previous response had no visible output...]", "(no
	// content)", interruption markers) before they are replayed to the
	// upstream model. These recovery-loop turns are imitation poison for
	// kimi/GLM-family models (dumped 2026-08-27): repeated exposure teaches
	// them thinking-only / whitespace replies until the agent loop dies.
	SanitizeClientMetaTurns bool
	// EmptyTurnGuard retries the upstream request when a streaming turn ends
	// with reasoning but NO visible text and NO tool call while tools were
	// offered — the signature of an upstream model abandoning its turn
	// mid-plan (kimi-k3 collapse). The retry appends the abandoned turn plus
	// a corrective cue to the conversation and continues the same downstream
	// SSE message.
	EmptyTurnGuard bool
	// EmptyTurnMaxRetries bounds EmptyTurnGuard re-requests per downstream
	// request (clamped to 0-5).
	EmptyTurnMaxRetries int

	// ResponsesStoreTTLMinutes is the sliding TTL of the /v1/responses proxy
	// store (previous_response_id chains), in minutes; 0 disables the store
	// (any previous_response_id then 404s).
	ResponsesStoreTTLMinutes int

	// MaxUpstreamImages caps the number of images sent to the upstream per
	// request: older images (earliest first, document order) are replaced
	// with text placeholders before conversion. The z-ai channel
	// deterministically fails requests carrying >= 8 images (dumped
	// 2026-09-12); default 7, 0 disables eviction.
	MaxUpstreamImages int
}

// warn mirrors console.warn (stderr, no timestamp).
func warn(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

// Load parses CLI arguments into a *Config, mirroring loadConfig() in
// config.ts. Supported forms: --name value, --name=value, --no-name negation,
// and repeatable flags.
func Load(args []string) *Config {
	// getArg returns the value after the first exact "--name" flag, else the
	// value of the first "--name=" prefixed argument, else the fallback.
	getArg := func(name, fallback string) string {
		flag := "--" + name
		for i := 0; i < len(args); i++ {
			if args[i] == flag && i+1 < len(args) {
				return args[i+1]
			}
		}
		eqFlag := flag + "="
		for _, a := range args {
			if strings.HasPrefix(a, eqFlag) {
				return a[len(eqFlag):]
			}
		}
		return fallback
	}

	// getBool: --name present → true; --no-name present → false;
	// --name=<v> → v != "false"; otherwise fallback.
	getBool := func(name string, fallback bool) bool {
		flag := "--" + name
		for _, a := range args {
			if a == flag {
				return true
			}
		}
		noFlag := "--no-" + name
		for _, a := range args {
			if a == noFlag {
				return false
			}
		}
		eqFlag := flag + "="
		for _, a := range args {
			if strings.HasPrefix(a, eqFlag) {
				return a[len(eqFlag):] != "false"
			}
		}
		return fallback
	}

	// getMultiArg collects every occurrence of "--name <v>" or "--name=<v>".
	getMultiArg := func(name string) []string {
		flag := "--" + name
		eqPrefix := flag + "="
		var results []string
		for i := 0; i < len(args); i++ {
			if args[i] == flag && i+1 < len(args) {
				results = append(results, args[i+1])
			} else if strings.HasPrefix(args[i], eqPrefix) {
				results = append(results, args[i][len(eqPrefix):])
			}
		}
		return results
	}

	// parseInt mirrors parseInt(str, 10); unparsable values yield 0
	// (TS yields NaN, which is not representable in Go's int).
	parseInt := func(s string) int {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			return 0
		}
		return n
	}

	var modelOverrides []ModelOverride
	for _, raw := range getMultiArg("upstream-extra-params") {
		eqIdx := strings.Index(raw, "=")
		if eqIdx == -1 {
			warn("Skipping invalid --upstream-extra-params (missing '='): %s", raw)
			continue
		}
		pattern := strings.TrimSpace(raw[:eqIdx])
		jsonStr := strings.TrimSpace(raw[eqIdx+1:])
		dec := json.NewDecoder(strings.NewReader(jsonStr))
		dec.UseNumber()
		var extra any
		if err := dec.Decode(&extra); err != nil {
			warn("Skipping --upstream-extra-params with invalid JSON for pattern %q", pattern)
			continue
		}
		obj, ok := extra.(map[string]any)
		if !ok {
			warn("Skipping --upstream-extra-params: JSON value for %q must be an object", pattern)
			continue
		}
		modelOverrides = append(modelOverrides, ModelOverride{Pattern: pattern, Extra: obj})
	}

	// --reasoning-replay: a bare mode sets the global default; glob=mode adds
	// a per-model rule (first matching rule wins over the default).
	defaultReplay := ReplayModeThinkTags
	var replayRules []ReasoningReplayRule
	for _, raw := range getMultiArg("reasoning-replay") {
		if eqIdx := strings.Index(raw, "="); eqIdx != -1 {
			pattern := strings.TrimSpace(raw[:eqIdx])
			mode := strings.TrimSpace(raw[eqIdx+1:])
			if pattern == "" {
				warn("Skipping --reasoning-replay with empty pattern: %s", raw)
				continue
			}
			if !reasoningReplayModes[mode] {
				warn("Skipping --reasoning-replay with unknown mode for pattern %q: %s (want %s | %s | %s)",
					pattern, mode, ReplayModeThinkTags, ReplayModeReasoningContent, ReplayModeDisabled)
				continue
			}
			replayRules = append(replayRules, ReasoningReplayRule{Pattern: pattern, Mode: mode})
			continue
		}
		mode := strings.TrimSpace(raw)
		if !reasoningReplayModes[mode] {
			warn("Skipping --reasoning-replay with unknown mode: %s (want %s | %s | %s)",
				mode, ReplayModeThinkTags, ReplayModeReasoningContent, ReplayModeDisabled)
			continue
		}
		defaultReplay = mode
	}

	serverTools := ServerToolConfig{
		WebSearch:                getBool("enable-web-search", false),
		WebFetch:                 getBool("enable-web-fetch", false),
		WebSearchEngine:          getArg("web-search-engine", "brave"),
		WebSearchAPIKey:          getArg("web-search-api-key", ""),
		WebSearchBaseURL:         getArg("web-search-base-url", "https://api.search.brave.com"),
		WebFetchAllowedDomains:   getMultiArg("web-fetch-allowed-domain"),
		WebFetchBlockedDomains:   getMultiArg("web-fetch-blocked-domain"),
		WebFetchMaxContentTokens: parseInt(getArg("web-fetch-max-content-tokens", "5000")),
	}

	// Port: parseInt yields 0 for unparsable/absent values; a value outside
	// the valid port range (1-65535) cannot be bound — warn and fall back to
	// the default 8082 rather than failing the server later at listen time.
	port := parseInt(getArg("port", "8082"))
	if port <= 0 || port > 65535 {
		warn("Invalid --port %d (must be between 1 and 65535); falling back to 8082", port)
		port = 8082
	}

	// Empty-turn retry budget: clamped to 0-5 (a loop beyond that means the
	// upstream is wedged; more retries only burn tokens).
	emptyTurnRetries := parseInt(getArg("empty-turn-retries", "2"))
	if emptyTurnRetries < 0 || emptyTurnRetries > 5 {
		warn("Invalid --empty-turn-retries %d (must be between 0 and 5); using 2", emptyTurnRetries)
		emptyTurnRetries = 2
	}

	// Upstream image cap: negative is a typo — fall back to the default 7.
	maxUpstreamImages := parseInt(getArg("max-upstream-images", "7"))
	if maxUpstreamImages < 0 {
		warn("Invalid --max-upstream-images %d (must be >= 0); using 7", maxUpstreamImages)
		maxUpstreamImages = 7
	}

	return &Config{
		UpstreamBaseURL:          getArg("upstream-base-url", "https://api.openai.com/v1"),
		UpstreamAPIKey:           getArg("upstream-api-key", ""),
		AuthToken:                getArg("auth-token", ""),
		Port:                     port,
		EnableThinking:           getBool("enable-thinking", true),
		DumpDir:                  getArg("dump", ""),
		ModelOverrides:           modelOverrides,
		ServerTools:              serverTools,
		DefaultReasoningReplay:   defaultReplay,
		ReasoningReplayRules:     replayRules,
		SanitizeClientMetaTurns:  getBool("sanitize-client-meta-turns", true),
		EmptyTurnGuard:           getBool("empty-turn-guard", true),
		EmptyTurnMaxRetries:      emptyTurnRetries,
		ResponsesStoreTTLMinutes: parseInt(getArg("responses-store-ttl-minutes", "1440")),
		MaxUpstreamImages:        maxUpstreamImages,
	}
}

// GlobMatch implements the TS globMatch: `*` matches any characters (including
// none), `?` matches a single character, everything else is matched literally
// (regex metacharacters are escaped). The whole string must match.
func GlobMatch(pattern, text string) bool {
	quoted := regexp.QuoteMeta(pattern)
	quoted = strings.ReplaceAll(quoted, `\*`, ".*")
	quoted = strings.ReplaceAll(quoted, `\?`, ".")
	re := regexp.MustCompile("^" + quoted + "$")
	return re.MatchString(text)
}

// ResolveModelExtra returns the extra params of the first override whose
// pattern matches model; an empty map is returned when nothing matches or
// overrides is nil.
func ResolveModelExtra(model string, overrides []ModelOverride) map[string]any {
	if overrides == nil {
		return map[string]any{}
	}
	for _, entry := range overrides {
		if GlobMatch(entry.Pattern, model) {
			return entry.Extra
		}
	}
	return map[string]any{}
}

// ResolveReasoningReplay returns the reasoning replay mode for model: the mode
// of the first matching rule, else fallback.
func ResolveReasoningReplay(model string, rules []ReasoningReplayRule, fallback string) string {
	for _, rule := range rules {
		if GlobMatch(rule.Pattern, model) {
			return rule.Mode
		}
	}
	return fallback
}

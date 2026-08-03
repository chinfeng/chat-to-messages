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

// ModelOverride maps a model glob pattern to extra upstream params.
type ModelOverride struct {
	Pattern string
	Extra   map[string]any
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

	return &Config{
		UpstreamBaseURL: getArg("upstream-base-url", "https://api.openai.com/v1"),
		UpstreamAPIKey:  getArg("upstream-api-key", ""),
		AuthToken:       getArg("auth-token", ""),
		Port:            parseInt(getArg("port", "8082")),
		EnableThinking:  getBool("enable-thinking", true),
		DumpDir:         getArg("dump", ""),
		ModelOverrides:  modelOverrides,
		ServerTools:     serverTools,
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

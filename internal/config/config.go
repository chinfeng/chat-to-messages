// Package config implements the CLI-argument-based server configuration,
// ported from chat-to-claude-code's src/server/config.ts.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

	// Upstreams and Routes are populated only in file mode (--config);
	// in legacy CLI mode both are nil.
	Upstreams []*Upstream // file mode only
	Routes    []Route     // file mode only
}

// warn mirrors console.warn (stderr, no timestamp).
func warn(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

// defaultServerTools returns the built-in serverTools defaults shared by the
// CLI and file config paths.
func defaultServerTools() ServerToolConfig {
	return ServerToolConfig{
		WebSearchEngine:          "brave",
		WebSearchBaseURL:         "https://api.search.brave.com",
		WebFetchMaxContentTokens: 5000,
	}
}

// ServerToolJSON is the wire form of ServerToolConfig for JSON decoding.
// materialize fills in the defaults for fields the file left unset.
type ServerToolJSON ServerToolConfig

func (s *ServerToolJSON) materialize() ServerToolConfig {
	def := defaultServerTools()
	cfg := ServerToolConfig(*s)
	if cfg.WebSearchEngine == "" {
		cfg.WebSearchEngine = def.WebSearchEngine
	}
	if cfg.WebSearchBaseURL == "" {
		cfg.WebSearchBaseURL = def.WebSearchBaseURL
	}
	if cfg.WebFetchMaxContentTokens == 0 {
		cfg.WebFetchMaxContentTokens = def.WebFetchMaxContentTokens
	}
	return cfg
}

// Load parses CLI arguments into a *Config, mirroring loadConfig() in
// config.ts. Supported forms: --name value, --name=value, --no-name negation,
// and repeatable flags. --config <path> loads a routing JSON file instead and
// cannot be combined with any other argument.
func Load(args []string) (*Config, error) {
	configPath := ""
	rest := args[:0:0]
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--config" && i+1 < len(args):
			configPath = args[i+1]
			i++
		case strings.HasPrefix(args[i], "--config="):
			configPath = args[i][len("--config="):]
		default:
			rest = append(rest, args[i])
		}
	}
	if configPath != "" && len(rest) > 0 {
		return nil, fmt.Errorf("--config cannot be combined with other arguments (got %q)", rest[0])
	}
	if configPath != "" {
		return LoadFile(configPath)
	}

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

	serverTools := defaultServerTools()
	serverTools.WebSearch = getBool("enable-web-search", false)
	serverTools.WebFetch = getBool("enable-web-fetch", false)
	serverTools.WebSearchEngine = getArg("web-search-engine", serverTools.WebSearchEngine)
	serverTools.WebSearchAPIKey = getArg("web-search-api-key", serverTools.WebSearchAPIKey)
	serverTools.WebSearchBaseURL = getArg("web-search-base-url", serverTools.WebSearchBaseURL)
	serverTools.WebFetchAllowedDomains = getMultiArg("web-fetch-allowed-domain")
	serverTools.WebFetchBlockedDomains = getMultiArg("web-fetch-blocked-domain")
	serverTools.WebFetchMaxContentTokens = parseInt(getArg("web-fetch-max-content-tokens", strconv.Itoa(serverTools.WebFetchMaxContentTokens)))

	// Port: parseInt yields 0 for unparsable/absent values; a value outside
	// the valid port range (1-65535) cannot be bound — warn and fall back to
	// the default 8082 rather than failing the server later at listen time.
	port := parseInt(getArg("port", "8082"))
	if port <= 0 || port > 65535 {
		warn("Invalid --port %d (must be between 1 and 65535); falling back to 8082", port)
		port = 8082
	}

	return &Config{
		UpstreamBaseURL: getArg("upstream-base-url", "https://api.openai.com/v1"),
		UpstreamAPIKey:  getArg("upstream-api-key", ""),
		AuthToken:       getArg("auth-token", ""),
		Port:            port,
		EnableThinking:  getBool("enable-thinking", true),
		DumpDir:         getArg("dump", ""),
		ModelOverrides:  modelOverrides,
		ServerTools:     serverTools,
	}, nil
}

// LoadFile reads a routing JSON file and applies the same defaults as the
// CLI path. Validation runs through NewRouter.
func LoadFile(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var doc struct {
		Port           int         `json:"port"`
		AuthToken      string      `json:"authToken"`
		EnableThinking *bool       `json:"enableThinking"`
		DumpDir        string      `json:"dumpDir"`
		Upstreams      []*Upstream `json:"upstreams"`
		Routes         []struct {  // wire form: JSON key "upstreams" maps to Route.Names
			Pattern   string   `json:"pattern"`
			Upstreams []string `json:"upstreams"`
		} `json:"routes"`
		ServerTools *ServerToolJSON `json:"serverTools"`
	}
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("invalid config file: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("invalid config file: trailing data after JSON object")
	}
	routes := make([]Route, len(doc.Routes))
	for i, r := range doc.Routes {
		routes[i] = Route{Pattern: r.Pattern, Names: r.Upstreams}
	}
	if doc.EnableThinking == nil {
		def := true
		doc.EnableThinking = &def
	}
	st := defaultServerTools()
	if doc.ServerTools != nil {
		st = doc.ServerTools.materialize()
	}
	cfg := &Config{
		UpstreamBaseURL: "", // file mode legacy fields are zeroed
		UpstreamAPIKey:  "",
		AuthToken:       doc.AuthToken,
		Port:            doc.Port,
		EnableThinking:  *doc.EnableThinking,
		DumpDir:         doc.DumpDir,
		Upstreams:       doc.Upstreams,
		Routes:          routes,
		ServerTools:     st,
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		warn("Invalid port %d (must be between 1 and 65535); falling back to 8082", cfg.Port)
		cfg.Port = 8082
	}
	// Upstreams without any route can never serve a request (no route matches,
	// Distinct() is empty) — fail at load time rather than per request.
	if len(cfg.Upstreams) > 0 && len(routes) == 0 {
		return nil, fmt.Errorf(`config defines upstreams but no routes; add at least one "routes" entry`)
	}
	// Zero upstreams boots a healthy-looking server that 502s every request.
	// Also catches a typo'd top-level key ("upstream"): JSON decoding silently
	// leaves Upstreams nil, so say explicitly that "upstreams" is required.
	if len(cfg.Upstreams) == 0 {
		return nil, fmt.Errorf("config defines no upstreams; \"upstreams\" is required (check your config keys)")
	}
	// Validate by building a Router once (instance discarded): load-time and
	// runtime share the same validation code path.
	if _, err := NewRouter(cfg.Upstreams, cfg.Routes); err != nil {
		return nil, err
	}
	return cfg, nil
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

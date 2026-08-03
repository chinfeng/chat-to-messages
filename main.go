package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"chat-to-messages/internal/config"
	"chat-to-messages/internal/proxy"
)

func main() {
	cfg := config.Load(os.Args[1:])

	handler := proxy.NewHandler(cfg)
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: handler,
	}

	// 启动横幅（与 TS 版格式一致，名称改为 chat-to-messages）
	passthrough := cfg.UpstreamAPIKey == "" && cfg.AuthToken == ""
	fmt.Printf("chat-to-messages listening on http://localhost:%d\n", cfg.Port)
	fmt.Printf("  Upstream: %s\n", cfg.UpstreamBaseURL)
	fmt.Printf("  Upstream API key: %s\n", boolWord(cfg.UpstreamAPIKey != ""))
	fmt.Printf("  Auth token: %s\n", boolWord(cfg.AuthToken != ""))
	fmt.Printf("  Passthrough mode: %v\n", passthrough)
	fmt.Printf("  Thinking: %v\n", cfg.EnableThinking)
	fmt.Printf("  Dump: %s\n", orDisabled(cfg.DumpDir))
	if len(cfg.ModelOverrides) > 0 {
		fmt.Printf("  Model overrides:\n")
		for _, e := range cfg.ModelOverrides {
			extra, _ := json.Marshal(e.Extra)
			fmt.Printf("    %s -> %s\n", e.Pattern, extra)
		}
	}
	fmt.Printf("  Web Search: %v\n", cfg.ServerTools.WebSearch)
	fmt.Printf("  Web Fetch: %v\n", cfg.ServerTools.WebFetch)
	if cfg.ServerTools.WebSearch {
		fmt.Printf("    Search engine: %s\n", cfg.ServerTools.WebSearchEngine)
		fmt.Printf("    Search base URL: %s\n", cfg.ServerTools.WebSearchBaseURL)
		fmt.Printf("    Search API key: %s\n", boolWord(cfg.ServerTools.WebSearchAPIKey != ""))
	}
	if cfg.ServerTools.WebFetch {
		if len(cfg.ServerTools.WebFetchAllowedDomains) > 0 {
			fmt.Printf("    Allowed domains: %s\n", joinAll(cfg.ServerTools.WebFetchAllowedDomains))
		}
		if len(cfg.ServerTools.WebFetchBlockedDomains) > 0 {
			fmt.Printf("    Blocked domains: %s\n", joinAll(cfg.ServerTools.WebFetchBlockedDomains))
		}
		fmt.Printf("    Max content tokens: %d\n", cfg.ServerTools.WebFetchMaxContentTokens)
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		srv.Close()
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func boolWord(b bool) string {
	if b {
		return "configured"
	}
	return "not set"
}

func orDisabled(s string) string {
	if s == "" {
		return "disabled"
	}
	return s
}

func joinAll(s []string) string {
	return strings.Join(s, ", ")
}

// Package config 内 router.go:模型 glob → 上游池 的轮询路由。
package config

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// Upstream is one named upstream endpoint.
type Upstream struct {
	Name           string
	BaseURL        string
	APIKey         string
	ModelOverrides []ModelOverride
}

// Route maps a model glob pattern to an ordered pool of upstream names.
type Route struct {
	Pattern string
	Names   []string

	pool    []*Upstream   // resolved by NewRouter
	counter atomic.Uint64 // round-robin cursor
}

// Router resolves request models to ordered candidate chains.
type Router struct {
	routes []*Route
}

// NewRouter validates and resolves routes against upstreams:
// names unique, baseUrl required, refs exist, pools non-empty.
func NewRouter(upstreams []*Upstream, routes []Route) (*Router, error) {
	byName := make(map[string]*Upstream, len(upstreams))
	for _, u := range upstreams {
		if u.Name == "" {
			return nil, fmt.Errorf("upstream name must not be empty")
		}
		if _, dup := byName[u.Name]; dup {
			return nil, fmt.Errorf("duplicate upstream name %q", u.Name)
		}
		if u.BaseURL == "" {
			return nil, fmt.Errorf("upstream %q: baseUrl is required", u.Name)
		}
		byName[u.Name] = u
	}
	rt := &Router{}
	for i := range routes {
		src := &routes[i] // pointer into caller slice; avoid copying Route (atomic.Uint64)
		if src.Pattern == "" {
			return nil, fmt.Errorf("route pattern must not be empty")
		}
		if len(src.Names) == 0 {
			return nil, fmt.Errorf("route %q: empty upstream pool", src.Pattern)
		}
		pool := make([]*Upstream, 0, len(src.Names))
		for _, n := range src.Names {
			u, ok := byName[n]
			if !ok {
				return nil, fmt.Errorf("route %q references unknown upstream %q", src.Pattern, n)
			}
			pool = append(pool, u)
		}
		// New Route value: fresh zero counter, no atomic copy.
		rt.routes = append(rt.routes, &Route{Pattern: src.Pattern, Names: src.Names, pool: pool})
	}
	return rt, nil
}

// Resolve returns candidates for model: first matching route's pool rotated
// to start at the round-robin cursor. Nil when no route matches.
func (rt *Router) Resolve(model string) []*Upstream {
	for _, r := range rt.routes {
		if !GlobMatch(r.Pattern, model) {
			continue
		}
		n := len(r.pool)
		start := int((r.counter.Add(1) - 1) % uint64(n))
		out := make([]*Upstream, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, r.pool[(start+i)%n])
		}
		return out
	}
	return nil
}

// Patterns lists configured route patterns in order.
func (rt *Router) Patterns() []string {
	out := make([]string, 0, len(rt.routes))
	for _, r := range rt.routes {
		out = append(out, r.Pattern)
	}
	return out
}

// Distinct dedupes every route's pool by pointer, declaration order.
func (rt *Router) Distinct() []*Upstream {
	var out []*Upstream
	seen := make(map[*Upstream]bool)
	for _, r := range rt.routes {
		for _, u := range r.pool {
			if !seen[u] {
				seen[u] = true
				out = append(out, u)
			}
		}
	}
	return out
}

// Router normalizes both config modes into a *Router. Called per request so
// tests may mutate the shared cfg pointer between requests. File mode
// (--config) builds through NewRouter (nil on invalid data — LoadFile already
// validated, so this is defensive); legacy CLI / direct-struct construction
// synthesizes a single catch-all upstream from the legacy fields.
func (c *Config) Router() *Router {
	if len(c.Upstreams) > 0 {
		rt, err := NewRouter(c.Upstreams, c.Routes)
		if err != nil {
			return nil // LoadFile 已校验过;防御性兜底
		}
		return rt
	}
	return MustSyntheticRouter(&Upstream{
		Name:           "default",
		BaseURL:        c.UpstreamBaseURL,
		APIKey:         c.UpstreamAPIKey,
		ModelOverrides: c.ModelOverrides,
	})
}

// MustSyntheticRouter builds a single-upstream catch-all router without
// validation (legacy CLI / direct-struct construction path).
func MustSyntheticRouter(u *Upstream) *Router {
	r := &Route{Pattern: "*", Names: []string{u.Name}, pool: []*Upstream{u}}
	return &Router{routes: []*Route{r}}
}

// Describe renders banner lines: "glm-* -> [zai-1 zai-2]".
func (rt *Router) Describe() []string {
	out := make([]string, 0, len(rt.routes))
	for _, r := range rt.routes {
		names := make([]string, 0, len(r.pool))
		for _, u := range r.pool {
			names = append(names, u.Name)
		}
		out = append(out, fmt.Sprintf("%s -> [%s]", r.Pattern, strings.Join(names, " ")))
	}
	return out
}

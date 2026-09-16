package caption

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// cacheTTL entries are keyed by image content (sha256 of the OpenAI image
// url — the data URI for base64 images, the URL itself otherwise), so a
// multi-turn conversation, a retry, or a previous_response_id expansion that
// replays the same image pays for one caption.
const (
	defaultCacheTTL  = 24 * time.Hour
	cacheSafetyCap   = 4096
)

type cacheEntry struct {
	caption  string
	expires  time.Time
}

// cache is an in-memory caption memo with a per-entry TTL. A TTL <= 0
// disables memoization entirely.
type cache struct {
	mu      sync.Mutex
	entries map[string]cacheEntry
	ttl     time.Duration
	now     func() time.Time
}

func newCache(ttl time.Duration) *cache {
	return &cache{entries: make(map[string]cacheEntry), ttl: ttl, now: time.Now}
}

// get returns a non-expired caption for key. ok is false when the TTL is 0 or
// no live entry exists.
func (c *cache) get(key string) (string, bool) {
	if c.ttl <= 0 {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return "", false
	}
	if c.now().After(e.expires) {
		delete(c.entries, key)
		return "", false
	}
	return e.caption, true
}

// set stores caption under key with the TTL. The map is bounded by
// cacheSafetyCap: first expired entries are dropped, and if that is not
// enough the whole map is cleared (the cache is advisory, never a source of
// truth).
func (c *cache) set(key, caption string) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= cacheSafetyCap {
		now := c.now()
		for k, e := range c.entries {
			if now.After(e.expires) {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= cacheSafetyCap {
			clear(c.entries)
		}
	}
	c.entries[key] = cacheEntry{caption: caption, expires: c.now().Add(c.ttl)}
}

// cacheKey hashes the image url string: data URIs embed the content, and URLs
// identify their content by location.
func cacheKey(part map[string]any) string {
	url := partURL(part)
	h := sha256.Sum256([]byte(url))
	return hex.EncodeToString(h[:])
}

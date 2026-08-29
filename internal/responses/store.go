package responses

import (
	"sync"
	"time"
)

// StoredResponse is one response-store entry: the full item history of the
// turn (expanded input items + the output items the proxy produced), kept as
// raw JSON-decoded values so unknown fields survive replay.
type StoredResponse struct {
	Items []any
}

// Store is the proxy-side response store behind previous_response_id
// (ADR-0001). In-memory with a sliding TTL: Get renews the entry. A restart
// loses all chains — clients retry with full input (ponytail: disk persistence
// via the dump dir is the upgrade path if restart-survival becomes real).
type Store struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]*storeEntry
}

type storeEntry struct {
	items     []any
	expiresAt time.Time
}

// NewStore creates a store whose entries live ttl after last access.
func NewStore(ttl time.Duration) *Store {
	return &Store{ttl: ttl, entries: make(map[string]*storeEntry)}
}

// Put records items under id (a copy — later mutation of the caller's slice
// must not leak into history).
func (s *Store) Put(id string, items []any) {
	if id == "" {
		return
	}
	cp := make([]any, len(items))
	copy(cp, items)
	s.mu.Lock()
	s.evictExpired()
	s.entries[id] = &storeEntry{items: cp, expiresAt: time.Now().Add(s.ttl)}
	s.mu.Unlock()
}

// Get returns the stored items for id, sliding the TTL. Missing or expired →
// false.
func (s *Store) Get(id string) ([]any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[id]
	if !ok {
		return nil, false
	}
	if time.Now().After(e.expiresAt) {
		delete(s.entries, id)
		return nil, false
	}
	e.expiresAt = time.Now().Add(s.ttl)
	out := make([]any, len(e.items))
	copy(out, e.items)
	return out, true
}

// evictExpired drops expired entries; called under lock on Put (Get already
// evicts its own miss).
func (s *Store) evictExpired() {
	now := time.Now()
	for id, e := range s.entries {
		if now.After(e.expiresAt) {
			delete(s.entries, id)
		}
	}
}

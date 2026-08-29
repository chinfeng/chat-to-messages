package responses

import (
	"testing"
	"time"
)

func TestStorePutGetRoundtrip(t *testing.T) {
	s := NewStore(time.Hour)
	s.Put("resp_a", []any{map[string]any{"type": "message"}})
	got, ok := s.Get("resp_a")
	if !ok || len(got) != 1 {
		t.Fatalf("Get after Put: ok=%v len=%d", ok, len(got))
	}
	if _, ok := s.Get("resp_missing"); ok {
		t.Fatal("Get on unknown id must miss")
	}
}

func TestStoreExpires(t *testing.T) {
	s := NewStore(50 * time.Millisecond)
	s.Put("resp_a", []any{"x"})
	time.Sleep(80 * time.Millisecond)
	if _, ok := s.Get("resp_a"); ok {
		t.Fatal("expired entry must miss")
	}
}

func TestStoreSlidingTTL(t *testing.T) {
	s := NewStore(120 * time.Millisecond)
	s.Put("resp_a", []any{"x"})
	time.Sleep(80 * time.Millisecond)
	if _, ok := s.Get("resp_a"); !ok { // renews
		t.Fatal("entry must be live before TTL")
	}
	time.Sleep(80 * time.Millisecond)
	if _, ok := s.Get("resp_a"); !ok { // TTL slid past the first deadline
		t.Fatal("sliding TTL must renew on Get")
	}
}

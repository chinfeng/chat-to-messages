package hook

import (
	"context"
	"errors"
	"testing"

	"github.com/chinfeng/chat-to-messages/internal/anthropic"
)

type fakeHook struct {
	name    string
	applies func(string) bool
	apply   func(context.Context, *Request) error
	calls   int
}

func (h *fakeHook) Name() string { return h.name }
func (h *fakeHook) Applies(model string) bool {
	if h.applies == nil {
		return true
	}
	return h.applies(model)
}
func (h *fakeHook) Apply(ctx context.Context, req *Request) error {
	h.calls++
	if h.apply != nil {
		return h.apply(ctx, req)
	}
	return nil
}

func TestRegistryRunsInRegistrationOrder(t *testing.T) {
	var order []string
	r := NewRegistry()
	r.Register(&fakeHook{name: "a", apply: func(_ context.Context, _ *Request) error { order = append(order, "a"); return nil }})
	r.Register(&fakeHook{name: "b", apply: func(_ context.Context, _ *Request) error { order = append(order, "b"); return nil }})
	if err := r.Apply(context.Background(), &Request{Model: "x"}); err != nil {
		t.Fatal(err)
	}
	if got := len(order); got != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("order = %v, want [a b]", order)
	}
}

func TestRegistryStopsOnError(t *testing.T) {
	r := NewRegistry()
	r.Register(&fakeHook{name: "a", apply: func(_ context.Context, _ *Request) error { return errors.New("boom") }})
	b := &fakeHook{name: "b"}
	r.Register(b)
	err := r.Apply(context.Background(), &Request{Model: "x"})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("err = %v, want boom", err)
	}
	if b.calls != 0 {
		t.Fatalf("second hook ran after error: %d calls", b.calls)
	}
}

func TestRegistrySkipsNonApplicable(t *testing.T) {
	r := NewRegistry()
	a := &fakeHook{name: "a", applies: func(m string) bool { return m == "glm-4v" }}
	r.Register(a)
	if err := r.Apply(context.Background(), &Request{Model: "deepseek-r1", Messages: []anthropic.Message{}}); err != nil {
		t.Fatal(err)
	}
	if a.calls != 0 {
		t.Fatalf("hook applied to non-matching model: %d calls", a.calls)
	}
	if err := r.Apply(context.Background(), &Request{Model: "glm-4v"}); err != nil {
		t.Fatal(err)
	}
	if a.calls != 1 {
		t.Fatalf("hook not applied to matching model: %d calls", a.calls)
	}
}

func TestAnyMatch(t *testing.T) {
	if AnyMatch(nil, "anything") {
		t.Fatal("empty pattern list must not match")
	}
	if AnyMatch([]string{"deepseek*", "kimi*"}, "kimi-k2") == false {
		t.Fatal("kimi* should match kimi-k2")
	}
	if AnyMatch([]string{"deepseek*", "kimi*"}, "glm-4.6") {
		t.Fatal("glm-4.6 must not match deepseek*/kimi*")
	}
}

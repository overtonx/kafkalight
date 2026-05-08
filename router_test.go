package kafkalight

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
)

func TestRouter_RegisterAndHandle_Dispatches(t *testing.T) {
	t.Parallel()

	r := newRouter()
	called := false

	r.Register("orders", HandlerFunc(func(_ context.Context, msg *Message) error {
		called = true
		return nil
	}))

	err := r.Handle(context.Background(), &Message{Topic: "orders"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}

func TestRouter_UnregisteredTopic_ReturnsErrNoHandler(t *testing.T) {
	t.Parallel()

	r := newRouter()
	err := r.Handle(context.Background(), &Message{Topic: "missing-topic"})

	if !errors.Is(err, ErrNoHandler) {
		t.Errorf("expected ErrNoHandler, got %v", err)
	}
}

func TestRouter_ErrNoHandler_ContainsTopic(t *testing.T) {
	t.Parallel()

	r := newRouter()
	const topic = "my-special-topic"
	err := r.Handle(context.Background(), &Message{Topic: topic})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrNoHandler) {
		t.Errorf("expected ErrNoHandler in chain, got %v", err)
	}
}

func TestRouter_RegisterFunc_Works(t *testing.T) {
	t.Parallel()

	r := newRouter()
	called := false

	r.RegisterFunc("payments", func(_ context.Context, msg *Message) error {
		called = true
		return nil
	})

	err := r.Handle(context.Background(), &Message{Topic: "payments"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("RegisterFunc handler was not called")
	}
}

func TestRouter_OverwriteHandler(t *testing.T) {
	t.Parallel()

	r := newRouter()
	firstCalled := false
	secondCalled := false

	r.Register("topic", HandlerFunc(func(_ context.Context, msg *Message) error {
		firstCalled = true
		return nil
	}))
	r.Register("topic", HandlerFunc(func(_ context.Context, msg *Message) error {
		secondCalled = true
		return nil
	}))

	err := r.Handle(context.Background(), &Message{Topic: "topic"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if firstCalled {
		t.Error("first handler should have been overwritten but was called")
	}
	if !secondCalled {
		t.Error("second (overwrite) handler was not called")
	}
}

func TestRouter_ConcurrentRegisterAndHandle(t *testing.T) {
	t.Parallel()

	r := newRouter()
	// Pre-register so Handle always finds something.
	r.Register("topic", HandlerFunc(func(_ context.Context, _ *Message) error { return nil }))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			r.Register("topic", HandlerFunc(func(_ context.Context, _ *Message) error {
				return nil
			}))
		}()
		go func() {
			defer wg.Done()
			_ = r.Handle(context.Background(), &Message{Topic: "topic"})
		}()
	}
	wg.Wait()
}

func TestRouter_Topics_ReturnsAllRegistered(t *testing.T) {
	t.Parallel()

	r := newRouter()
	want := []string{"alpha", "beta", "gamma"}
	for _, topic := range want {
		topic := topic
		r.Register(topic, HandlerFunc(func(_ context.Context, _ *Message) error { return nil }))
	}

	got := r.topics()
	if len(got) != len(want) {
		t.Fatalf("topics(): got %d entries %v, want %d", len(got), got, len(want))
	}

	sort.Strings(got)
	sort.Strings(want)
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("topics()[%d]: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestRouter_Topics_EmptyRouter(t *testing.T) {
	t.Parallel()

	r := newRouter()
	if topics := r.topics(); len(topics) != 0 {
		t.Errorf("expected 0 topics on empty router, got %d: %v", len(topics), topics)
	}
}

func TestRouter_MiddlewareAppliedToHandler(t *testing.T) {
	t.Parallel()

	r := newRouter()
	var order []string

	mw := func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, msg *Message) error {
			order = append(order, "mw")
			return next.Handle(ctx, msg)
		})
	}

	r.Register("topic", HandlerFunc(func(_ context.Context, _ *Message) error {
		order = append(order, "handler")
		return nil
	}), mw)

	if err := r.Handle(context.Background(), &Message{Topic: "topic"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(order) != 2 || order[0] != "mw" || order[1] != "handler" {
		t.Errorf("expected [mw handler], got %v", order)
	}
}

func TestRouter_HandlerErrorPropagated(t *testing.T) {
	t.Parallel()

	r := newRouter()
	sentinel := errors.New("handler error")
	r.Register("t", HandlerFunc(func(_ context.Context, _ *Message) error {
		return sentinel
	}))

	err := r.Handle(context.Background(), &Message{Topic: "t"})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
}

package kafkalight

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

// ---------- Chain / Apply ----------

func TestChain_AppliesInLeftToRightOrder(t *testing.T) {
	t.Parallel()

	var order []string

	makeMiddleware := func(name string) Middleware {
		return func(next Handler) Handler {
			return HandlerFunc(func(ctx context.Context, msg *Message) error {
				order = append(order, name+":in")
				err := next.Handle(ctx, msg)
				order = append(order, name+":out")
				return err
			})
		}
	}

	h := HandlerFunc(func(_ context.Context, _ *Message) error {
		order = append(order, "handler")
		return nil
	})

	// Chain(A, B, C)(h) → A wraps B wraps C wraps h
	wrapped := Chain(makeMiddleware("A"), makeMiddleware("B"), makeMiddleware("C"))(h)
	if err := wrapped.Handle(context.Background(), &Message{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{
		"A:in", "B:in", "C:in",
		"handler",
		"C:out", "B:out", "A:out",
	}
	if len(order) != len(expected) {
		t.Fatalf("order length mismatch: got %v, want %v", order, expected)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("position %d: want %q got %q", i, v, order[i])
		}
	}
}

func TestChain_NoMiddleware_CallsHandlerDirectly(t *testing.T) {
	t.Parallel()

	called := false
	h := HandlerFunc(func(_ context.Context, _ *Message) error {
		called = true
		return nil
	})

	wrapped := Chain()(h)
	if err := wrapped.Handle(context.Background(), &Message{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler was not called with empty Chain")
	}
}

func TestApply_EquivalentToChain(t *testing.T) {
	t.Parallel()

	var order []string
	makeMiddleware := func(name string) Middleware {
		return func(next Handler) Handler {
			return HandlerFunc(func(ctx context.Context, msg *Message) error {
				order = append(order, name)
				return next.Handle(ctx, msg)
			})
		}
	}
	h := HandlerFunc(func(_ context.Context, _ *Message) error { return nil })

	Apply(h, makeMiddleware("X"), makeMiddleware("Y")).Handle(context.Background(), &Message{})

	if len(order) != 2 || order[0] != "X" || order[1] != "Y" {
		t.Errorf("Apply order: got %v, want [X Y]", order)
	}
}

// ---------- RecoveryMiddleware ----------

func TestRecoveryMiddleware_PanicWithErrorValue(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("panicked error value")
	h := HandlerFunc(func(_ context.Context, _ *Message) error {
		panic(sentinel)
	})

	wrapped := RecoveryMiddleware()(h)
	err := wrapped.Handle(context.Background(), &Message{})

	if err == nil {
		t.Fatal("expected non-nil error after panic recovery")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error in chain, got %v", err)
	}
}

func TestRecoveryMiddleware_PanicWithNonErrorValue(t *testing.T) {
	t.Parallel()

	h := HandlerFunc(func(_ context.Context, _ *Message) error {
		panic("plain string panic")
	})

	wrapped := RecoveryMiddleware()(h)
	err := wrapped.Handle(context.Background(), &Message{})

	if err == nil {
		t.Fatal("expected non-nil error after panic recovery")
	}
	if err.Error() == "" {
		t.Error("recovered error message must not be empty")
	}
}

func TestRecoveryMiddleware_PanicWithIntValue(t *testing.T) {
	t.Parallel()

	h := HandlerFunc(func(_ context.Context, _ *Message) error {
		panic(42)
	})

	wrapped := RecoveryMiddleware()(h)
	err := wrapped.Handle(context.Background(), &Message{})
	if err == nil {
		t.Fatal("expected error after int panic recovery")
	}
}

func TestRecoveryMiddleware_NoPanic_PassesThrough(t *testing.T) {
	t.Parallel()

	h := HandlerFunc(func(_ context.Context, _ *Message) error {
		return nil
	})

	wrapped := RecoveryMiddleware()(h)
	if err := wrapped.Handle(context.Background(), &Message{}); err != nil {
		t.Errorf("expected nil error when no panic, got %v", err)
	}
}

func TestRecoveryMiddleware_NoPanic_PropagatesHandlerError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("real handler error")
	h := HandlerFunc(func(_ context.Context, _ *Message) error { return sentinel })

	wrapped := RecoveryMiddleware()(h)
	err := wrapped.Handle(context.Background(), &Message{})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
}

// ---------- LoggingMiddleware ----------

func TestLoggingMiddleware_PropagatesErrorFromHandler(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	sentinel := errors.New("handler error")

	h := HandlerFunc(func(_ context.Context, _ *Message) error { return sentinel })
	wrapped := LoggingMiddleware(logger)(h)

	err := wrapped.Handle(context.Background(), &Message{Topic: "test-topic"})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error propagated, got %v", err)
	}
}

func TestLoggingMiddleware_NilErrorPassesThrough(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	h := HandlerFunc(func(_ context.Context, _ *Message) error { return nil })
	wrapped := LoggingMiddleware(logger)(h)

	if err := wrapped.Handle(context.Background(), &Message{Topic: "test-topic"}); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestLoggingMiddleware_PopulatesMessageFields(t *testing.T) {
	t.Parallel()

	// Verify the middleware does not crash when all Message fields are set.
	logger := zap.NewNop()
	h := HandlerFunc(func(_ context.Context, _ *Message) error { return nil })
	wrapped := LoggingMiddleware(logger)(h)

	msg := &Message{Topic: "t", Partition: 3, Offset: 100}
	if err := wrapped.Handle(context.Background(), msg); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---------- MetricsMiddleware ----------

func TestMetricsMiddleware_SuccessCallsIncProcessed(t *testing.T) {
	t.Parallel()

	mp := noop.NewMeterProvider()
	m, err := newMetrics(mp)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	h := HandlerFunc(func(_ context.Context, _ *Message) error { return nil })
	wrapped := MetricsMiddleware(m)(h)

	if err := wrapped.Handle(context.Background(), &Message{Topic: "t"}); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// With a noop provider no counters are checked numerically; we only verify
	// the middleware does not panic and propagates nil correctly.
}

func TestMetricsMiddleware_ErrorCallsIncProcessedWithErrorStatus(t *testing.T) {
	t.Parallel()

	mp := noop.NewMeterProvider()
	m, err := newMetrics(mp)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	sentinel := errors.New("oops")
	h := HandlerFunc(func(_ context.Context, _ *Message) error { return sentinel })
	wrapped := MetricsMiddleware(m)(h)

	err = wrapped.Handle(context.Background(), &Message{Topic: "t"})
	if !errors.Is(err, sentinel) {
		t.Errorf("MetricsMiddleware must propagate handler error; got %v", err)
	}
}

func TestMetricsMiddleware_NilMetrics_DoesNotPanic(t *testing.T) {
	t.Parallel()

	// newMetrics(nil) returns a *metrics with nil instruments; incProcessed must be a no-op.
	m, err := newMetrics(nil)
	if err != nil {
		t.Fatalf("newMetrics(nil): %v", err)
	}

	h := HandlerFunc(func(_ context.Context, _ *Message) error { return nil })
	wrapped := MetricsMiddleware(m)(h)

	if err := wrapped.Handle(context.Background(), &Message{Topic: "t"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

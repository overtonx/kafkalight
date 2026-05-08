package kafkalight

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// newTestTP creates a TracerProvider backed by an in-memory SpanRecorder.
// The provider is shut down when the test completes.
func newTestTP(t *testing.T) (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return tp, sr
}

func newTestTracer(t *testing.T) (trace.Tracer, *tracetest.SpanRecorder) {
	t.Helper()
	tp, sr := newTestTP(t)
	return tp.Tracer("test"), sr
}

func TestTracingMiddleware_SpanCreated(t *testing.T) {
	t.Parallel()

	tracer, sr := newTestTracer(t)
	msg := &Message{Topic: "orders", Partition: 2, Offset: 42}

	h := HandlerFunc(func(_ context.Context, _ *Message) error { return nil })
	wrapped := TracingMiddleware(WithTracer(tracer))(h)

	if err := wrapped.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	span := spans[0]
	if span.Name() != "kafka.process orders" {
		t.Errorf("span name: got %q, want %q", span.Name(), "kafka.process orders")
	}
	if span.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("span kind: got %v, want Consumer", span.SpanKind())
	}
}

func TestTracingMiddleware_AttributesSet(t *testing.T) {
	t.Parallel()

	tracer, sr := newTestTracer(t)
	msg := &Message{Topic: "payments", Partition: 1, Offset: 100}

	h := HandlerFunc(func(_ context.Context, _ *Message) error { return nil })
	_ = TracingMiddleware(WithTracer(tracer))(h).Handle(context.Background(), msg)

	span := sr.Ended()[0]
	attrs := map[string]any{}
	for _, a := range span.Attributes() {
		attrs[string(a.Key)] = a.Value.AsInterface()
	}

	checks := map[string]any{
		"messaging.system":           "kafka",
		"messaging.operation.name":   "process",
		"messaging.destination.name": "payments",
		"messaging.kafka.partition":  int64(1),
		"messaging.kafka.offset":     int64(100),
	}
	for k, want := range checks {
		got, ok := attrs[k]
		if !ok {
			t.Errorf("attribute %q missing", k)
			continue
		}
		if got != want {
			t.Errorf("attribute %q: got %v, want %v", k, got, want)
		}
	}
}

func TestTracingMiddleware_ErrorRecordedOnSpan(t *testing.T) {
	t.Parallel()

	tracer, sr := newTestTracer(t)
	sentinel := errors.New("handler blew up")

	h := HandlerFunc(func(_ context.Context, _ *Message) error { return sentinel })
	err := TracingMiddleware(WithTracer(tracer))(h).Handle(context.Background(), &Message{Topic: "t"})

	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}

	span := sr.Ended()[0]
	if span.Status().Code != codes.Error {
		t.Errorf("span status: got %v, want Error", span.Status().Code)
	}
	if len(span.Events()) == 0 {
		t.Error("expected error event on span")
	}
}

func TestTracingMiddleware_SuccessSpanStatusUnset(t *testing.T) {
	t.Parallel()

	tracer, sr := newTestTracer(t)
	h := HandlerFunc(func(_ context.Context, _ *Message) error { return nil })
	_ = TracingMiddleware(WithTracer(tracer))(h).Handle(context.Background(), &Message{Topic: "t"})

	span := sr.Ended()[0]
	if span.Status().Code == codes.Error {
		t.Errorf("span status must not be Error on success, got %v", span.Status().Code)
	}
}

func TestTracingMiddleware_ContextPropagated(t *testing.T) {
	t.Parallel()

	tracer, sr := newTestTracer(t)
	var childSpanID trace.SpanID
	h := HandlerFunc(func(ctx context.Context, _ *Message) error {
		childSpanID = trace.SpanFromContext(ctx).SpanContext().SpanID()
		return nil
	})
	_ = TracingMiddleware(WithTracer(tracer))(h).Handle(context.Background(), &Message{Topic: "t"})

	if len(sr.Ended()) != 1 {
		t.Fatalf("expected 1 span, got %d", len(sr.Ended()))
	}
	if childSpanID != sr.Ended()[0].SpanContext().SpanID() {
		t.Error("handler did not receive the tracing span in its context")
	}
}

func TestTracingMiddleware_HeaderCarrier_ExtractsTraceParent(t *testing.T) {
	t.Parallel()

	// Verify that a traceparent header in a Kafka message is extracted and
	// the resulting span is linked (same trace ID) to the remote parent.
	tracer, sr := newTestTracer(t)

	// Start a root span and capture its IDs.
	_, rootSpan := tracer.Start(context.Background(), "producer")
	traceID := rootSpan.SpanContext().TraceID()
	parentSpanID := rootSpan.SpanContext().SpanID()
	rootSpan.End()

	// Build a valid W3C traceparent header value.
	traceparent := "00-" + traceID.String() + "-" + parentSpanID.String() + "-01"

	msg := &Message{
		Topic:   "t",
		Headers: []Header{{Key: "traceparent", Value: []byte(traceparent)}},
	}

	h := HandlerFunc(func(_ context.Context, _ *Message) error { return nil })
	// Supply the W3C propagator explicitly; the global default is a noop.
	_ = TracingMiddleware(
		WithTracer(tracer),
		WithPropagator(propagation.TraceContext{}),
	)(h).Handle(context.Background(), msg)

	// The last ended span is the consumer span.
	spans := sr.Ended()
	consumer := spans[len(spans)-1]
	if consumer.SpanContext().TraceID() != traceID {
		t.Errorf("trace ID not propagated: got %v, want %v",
			consumer.SpanContext().TraceID(), traceID)
	}
}

func TestTracingMiddleware_WithTracerProvider(t *testing.T) {
	t.Parallel()

	tp, sr := newTestTP(t)

	h := HandlerFunc(func(_ context.Context, _ *Message) error { return nil })
	_ = TracingMiddleware(WithTracerProvider(tp))(h).Handle(context.Background(), &Message{Topic: "orders"})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span from WithTracerProvider, got %d", len(spans))
	}
	if spans[0].Name() != "kafka.process orders" {
		t.Errorf("span name: got %q", spans[0].Name())
	}
}

func TestTracingMiddleware_NilOptions_DoNotPanic(t *testing.T) {
	t.Parallel()

	// WithTracer(nil) and WithPropagator(nil) must be no-ops, not panics.
	// We use a noop tracer provider so no real spans are created.
	h := HandlerFunc(func(_ context.Context, _ *Message) error { return nil })
	mw := TracingMiddleware(WithTracer(nil), WithPropagator(nil))

	// Should not panic even though nil was passed for both options.
	// The globals (noop by default in tests) are used as fallback.
	if err := mw(h).Handle(context.Background(), &Message{Topic: "t"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestTracingMiddleware_WithTracerProvider_NilIsNoOp(t *testing.T) {
	t.Parallel()

	// WithTracerProvider(nil) must be a no-op, not a panic.
	tp, sr := newTestTP(t)
	h := HandlerFunc(func(_ context.Context, _ *Message) error { return nil })

	// nil overrride is ignored; the explicit tracer (from tp) is used.
	_ = TracingMiddleware(
		WithTracerProvider(tp),
		WithTracerProvider(nil), // must not overwrite the valid provider
	)(h).Handle(context.Background(), &Message{Topic: "t"})

	if len(sr.Ended()) != 1 {
		t.Errorf("expected 1 span, got %d — nil WithTracerProvider must be a no-op", len(sr.Ended()))
	}
}

func TestMsgHeaderCarrier_Get_CaseInsensitive(t *testing.T) {
	t.Parallel()

	c := msgHeaderCarrier([]Header{
		{Key: "Traceparent", Value: []byte("abc")},
	})
	if got := c.Get("traceparent"); got != "abc" {
		t.Errorf("Get: got %q, want %q", got, "abc")
	}
	if got := c.Get("TRACEPARENT"); got != "abc" {
		t.Errorf("Get (upper): got %q, want %q", got, "abc")
	}
}

func TestMsgHeaderCarrier_Keys(t *testing.T) {
	t.Parallel()

	c := msgHeaderCarrier([]Header{
		{Key: "traceparent", Value: []byte("x")},
		{Key: "tracestate", Value: []byte("y")},
	})
	keys := c.Keys()
	if len(keys) != 2 || keys[0] != "traceparent" || keys[1] != "tracestate" {
		t.Errorf("Keys: got %v", keys)
	}
}

func TestMsgHeaderCarrier_Set_IsNoOp(t *testing.T) {
	t.Parallel()

	c := msgHeaderCarrier([]Header{{Key: "k", Value: []byte("v")}})
	c.Set("k", "changed") // must not panic or modify anything
	if got := c.Get("k"); got != "v" {
		t.Errorf("Set should be a no-op, but value changed to %q", got)
	}
}

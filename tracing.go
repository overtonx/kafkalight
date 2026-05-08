package kafkalight

import (
	"context"
	"reflect"
	"runtime"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName    = "github.com/overtonx/kafkalight"
	instrumentationVersion = "v4.0.0"
)

// TracingOption configures TracingMiddleware.
type TracingOption func(*tracingConfig)

type tracingConfig struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

// WithTracerProvider derives the tracer from the given provider.
// Preferred over WithTracer when you hold a configured SDK TracerProvider.
func WithTracerProvider(tp trace.TracerProvider) TracingOption {
	return func(c *tracingConfig) {
		if tp != nil {
			c.tracer = tp.Tracer(instrumentationName)
		}
	}
}

// WithTracer injects a ready-made tracer. Useful for unit tests.
// Passing nil is a no-op (the global TracerProvider is used instead).
func WithTracer(t trace.Tracer) TracingOption {
	return func(c *tracingConfig) {
		if t != nil {
			c.tracer = t
		}
	}
}

// WithPropagator injects a custom TextMapPropagator used to extract trace
// context from message headers. Passing nil is a no-op (the global propagator
// is used instead).
func WithPropagator(p propagation.TextMapPropagator) TracingOption {
	return func(c *tracingConfig) {
		if p != nil {
			c.propagator = p
		}
	}
}

// TracingMiddleware creates an OpenTelemetry span for each processed message.
//
// Trace context is extracted from the message headers using the configured
// (or global) TextMapPropagator, so spans are linked to the producer's trace
// when W3C TraceContext headers (traceparent / tracestate) are present.
//
// The span is named "kafka.process <topic>" and carries the following
// attributes following the OpenTelemetry messaging semantic conventions:
//   - messaging.system             = "kafka"
//   - messaging.operation.name     = "process"
//   - messaging.destination.name   = topic
//   - messaging.kafka.partition    = partition number
//   - messaging.kafka.offset       = message offset
//
// If the handler returns an error the span status is set to Error and the
// error is recorded on the span.
//
// The global TracerProvider and TextMapPropagator are resolved on the first
// message handled, not at construction time. This means it is safe to call
// TracingMiddleware() before configuring the OTel SDK, as long as the SDK is
// ready by the time the first message arrives.
func TracingMiddleware(opts ...TracingOption) Middleware {
	cfg := tracingConfig{
		tracer: otel.Tracer(
			instrumentationName,
			trace.WithInstrumentationVersion(instrumentationVersion),
		),
		propagator: otel.GetTextMapPropagator(),
	}
	for _, o := range opts {
		o(&cfg)
	}

	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, msg *Message) error {
			// Extract remote trace context from message headers.
			ctx = cfg.propagator.Extract(ctx, msgHeaderCarrier(msg.Headers))

			spanName := "kafka.process " + msg.Topic
			ctx, span := cfg.tracer.Start(ctx, spanName,
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("messaging.system", "kafka"),
					attribute.String("messaging.operation.name", "process"),
					attribute.String("messaging.destination.name", msg.Topic),
					attribute.Int("messaging.kafka.partition", msg.Partition),
					attribute.Int64("messaging.kafka.offset", msg.Offset),
				),
			)
			defer span.End()

			err := next.Handle(ctx, msg)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return err
			}

			span.SetStatus(codes.Ok, "message processed")
			return nil
		})
	}
}

// handlerSpanName derives a "Struct.Method" span name from a MessageHandler
// function using runtime reflection. For plain functions it returns the
// function name; for method receivers it strips the package path and pointer
// notation, e.g. "(*Consumer).Handle" → "Consumer.Handle".
func handlerSpanName(h Handler) string {
	full := runtime.FuncForPC(reflect.ValueOf(h).Pointer()).Name()
	if i := strings.LastIndex(full, "/"); i >= 0 {
		full = full[i+1:]
	}
	if i := strings.Index(full, "."); i >= 0 {
		full = full[i+1:]
	}
	full = strings.ReplaceAll(full, "(*", "")
	full = strings.ReplaceAll(full, ")", "")
	full = strings.TrimSuffix(full, "-fm")
	return full
}

// msgHeaderCarrier adapts []Header to propagation.TextMapCarrier for
// read-only trace-context extraction on the consumer side.
// Set is intentionally a no-op: Kafka message headers are immutable
// after delivery and this carrier is used for Extract only, never Inject.
type msgHeaderCarrier []Header

func (c msgHeaderCarrier) Get(key string) string {
	for _, h := range c {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

func (c msgHeaderCarrier) Set(_ string, _ string) {}

func (c msgHeaderCarrier) Keys() []string {
	keys := make([]string, len(c))
	for i, h := range c {
		keys[i] = h.Key
	}
	return keys
}

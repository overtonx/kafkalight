package middleware

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/overtonx/kafkalight"
)

func TestTracing(t *testing.T) {
	// Setup
	tracerProvider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tracerProvider)
	propagator := propagation.TraceContext{}

	// Create a parent context
	tracer := otel.Tracer("test-tracer")
	parentCtx, parentSpan := tracer.Start(context.Background(), "parent")
	defer parentSpan.End()

	// Inject the parent context into a message
	msg := &kafkalight.Message{}
	carrier := kafkalight.NewMessageCarrier(msg)
	propagator.Inject(parentCtx, carrier)

	// The handler that will be called by the middleware
	var handlerCtx context.Context
	handler := kafkalight.MessageHandler(func(ctx context.Context, msg *kafkalight.Message) error {
		handlerCtx = ctx
		return nil
	})

	// Create and execute the middleware
	mw := Tracing(propagator)
	finalHandler := mw(handler)
	err := finalHandler(context.Background(), msg) // Pass a background context, not the parentCtx

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, handlerCtx)

	// Check that the context passed to the handler contains the propagated span context
	spanFromHandlerCtx := trace.SpanFromContext(handlerCtx)
	assert.True(t, spanFromHandlerCtx.SpanContext().IsValid())
	assert.Equal(t, parentSpan.SpanContext().TraceID(), spanFromHandlerCtx.SpanContext().TraceID())
	assert.Equal(t, parentSpan.SpanContext().SpanID(), spanFromHandlerCtx.SpanContext().SpanID())
}

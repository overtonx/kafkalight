package middleware

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/overtonx/kafkalight"
)

func Tracing(propagator propagation.TextMapPropagator) kafkalight.Middleware {
	return func(next kafkalight.MessageHandler) kafkalight.MessageHandler {
		return func(ctx context.Context, msg *kafkalight.Message) error {
			ctx = propagator.Extract(ctx, kafkalight.NewMessageCarrier(msg))

			span := trace.SpanFromContext(ctx)
			defer span.End()

			return next(ctx, msg)
		}
	}
}

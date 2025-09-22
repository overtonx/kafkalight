package middleware

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/overtonx/kafkalight"
)

func Tracing() kafkalight.Middleware {
	return func(next kafkalight.MessageHandler) kafkalight.MessageHandler {
		return func(ctx context.Context, msg *kafkalight.Message) error {
			propagator := otel.GetTextMapPropagator()
			ctx = propagator.Extract(ctx, kafkalight.NewMessageCarrier(msg))

			span := trace.SpanFromContext(ctx)
			defer span.End()

			return next(ctx, msg)
		}
	}
}

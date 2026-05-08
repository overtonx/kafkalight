package kafkalight

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// Middleware wraps a Handler to add cross-cutting behaviour.
type Middleware func(next Handler) Handler

// Chain applies middlewares in left-to-right order so that the first
// middleware in the list is the outermost wrapper.
//
//	Chain(A, B, C)(h)  →  A(B(C(h)))
func Chain(middlewares ...Middleware) Middleware {
	return func(next Handler) Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](next)
		}
		return next
	}
}

// Apply wraps handler with the provided middlewares (same order as Chain).
func Apply(h Handler, middlewares ...Middleware) Handler {
	return Chain(middlewares...)(h)
}

// RecoveryMiddleware catches panics in the handler and converts them to errors.
func RecoveryMiddleware() Middleware {
	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, msg *Message) (retErr error) {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						retErr = err
					} else {
						retErr = &panicError{value: r}
					}
				}
			}()
			return next.Handle(ctx, msg)
		})
	}
}

type panicError struct{ value any }

func (e *panicError) Error() string {
	return fmt.Sprintf("kafkalight: panic recovered: %v", e.value)
}

// LoggingMiddleware logs the start and outcome of every message processing.
func LoggingMiddleware(logger *zap.Logger) Middleware {
	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, msg *Message) error {
			start := time.Now()
			log := logger.With(
				zap.String("topic", msg.Topic),
				zap.Int("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
			)
			log.Debug("processing message")

			err := next.Handle(ctx, msg)

			elapsed := time.Since(start)
			if err != nil {
				log.Error("message processing failed",
					zap.Error(err),
					zap.Duration("elapsed", elapsed),
				)
			} else {
				log.Debug("message processed successfully",
					zap.Duration("elapsed", elapsed),
				)
			}
			return err
		})
	}
}

// MetricsMiddleware records OTel metrics for each processed message.
func MetricsMiddleware(m *metrics) Middleware {
	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, msg *Message) error {
			start := time.Now()
			err := next.Handle(ctx, msg)
			ms := float64(time.Since(start).Nanoseconds()) / 1e6

			status := "ok"
			if err != nil {
				status = "error"
			}
			m.incProcessed(ctx, msg.Topic, status)
			m.recordDuration(ctx, msg.Topic, ms)
			return err
		})
	}
}

package kafkalight

import (
	"time"

	"go.uber.org/zap"
)

type Option func(*KafkaRouter)

func WithErrorHandler(handler func(error)) Option {
	return func(r *KafkaRouter) {
		r.errorHandler = handler
	}
}

func WithReadTimeout(timeout time.Duration) Option {
	return func(r *KafkaRouter) {
		r.readTimeout = timeout
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(r *KafkaRouter) {
		r.logger = logger
	}
}

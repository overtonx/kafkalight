package kafkalight

import "time"

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

package kafkalight

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type Option func(*KafkaRouter)

func WithConsumerConfig(cfg *kafka.ConfigMap) Option {
	return func(r *KafkaRouter) {
		r.consumerConfig = cfg
	}
}

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
		r.logger = logger.With(zap.String("module", "kafka-light"))
	}
}

func WithCommitOnErrors(errs ...error) Option {
	return func(r *KafkaRouter) {
		r.commitWithErrors = append(r.commitWithErrors, errs...)
	}
}

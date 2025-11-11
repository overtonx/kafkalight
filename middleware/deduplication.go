package middleware

import (
	"context"
	"log"
	"time"

	"github.com/overtonx/kafkalight"
)

// KeyExtractor определяет функцию для извлечения ключа дедупликации из сообщения.
type KeyExtractor func(msg *kafkalight.Message) (string, error)

// deduplicationConfig хранит конфигурацию для middleware дедупликации.
type deduplicationConfig struct {
	extractor KeyExtractor
}

// DefaultKeyExtractor является реализацией KeyExtractor по умолчанию.
// Он использует ключ Kafka-сообщения.
func DefaultKeyExtractor(msg *kafkalight.Message) (string, error) {
	if !msg.Key.Exists() {
		return "", nil
	}
	return msg.Key.String(), nil
}

func Deduplication(store Deduplicator, ttl time.Duration, opts ...DeduplicationOption) kafkalight.Middleware {
	cfg := &deduplicationConfig{
		extractor: DefaultKeyExtractor,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	return func(next kafkalight.MessageHandler) kafkalight.MessageHandler {
		return func(ctx context.Context, msg *kafkalight.Message) error {
			key, err := cfg.extractor(msg)
			if err != nil {
				log.Printf("deduplication: failed to extract key: %v", err)
				return err
			}

			if key == "" {
				return next(ctx, msg)
			}

			isNew, err := store.SetIfNotExists(ctx, key, ttl)
			if err != nil {
				log.Printf("deduplication: store error for key '%s': %v", key, err)
				return err
			}

			if !isNew {
				log.Printf("deduplication: duplicate message skipped for key '%s'", key)
				return nil
			}

			return next(ctx, msg)
		}
	}
}

// DeduplicationOption определяет опцию для настройки middleware дедупликации.
type DeduplicationOption func(*deduplicationConfig)

// WithKeyExtractor устанавливает кастомную функцию для извлечения ключа.
func WithKeyExtractor(extractor KeyExtractor) DeduplicationOption {
	return func(c *deduplicationConfig) {
		c.extractor = extractor
	}
}

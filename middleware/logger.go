package middleware

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/overtonx/kafkalight"
)

func Logger(logger *zap.Logger) kafkalight.Middleware {
	return func(fn kafkalight.MessageHandler) kafkalight.MessageHandler {
		return func(ctx context.Context, msg *kafkalight.Message) error {
			start := time.Now()

			err := fn(ctx, msg)

			duration := time.Since(start)

			fields := []zap.Field{
				zap.String("topic", msg.TopicPartition.Topic),
				zap.Int32("partition", msg.TopicPartition.Partition),
				zap.Time("timestamp", start),
				zap.Duration("duration", duration),
			}

			if msg.Key.Exists() {
				fields = append(fields, zap.String("key", msg.Key.String()))
			}

			if err != nil {
				fields = append(fields, zap.String("status", "error"), zap.Error(err))
				logger.Error("processed message", fields...)
			} else {
				fields = append(fields, zap.String("status", "success"))
				logger.Info("processed message", fields...)
			}

			return err
		}
	}
}

package kafkalight

import "go.uber.org/zap"

// @todo create package for middleware

func LoggerMiddleware(logger *zap.Logger) Middleware {
	return func(fn MessageHandler) MessageHandler {
		return func(msg *Message) error {
			fields := make([]zap.Field, 0)
			fields = append(fields, zap.String("topic", msg.TopicPartition.Topic))
			fields = append(fields, zap.Int32("partition", msg.TopicPartition.Partition))
			if msg.Key.Exists() {
				fields = append(fields, zap.String("key", msg.Key.String()))
			}

			logger.Info("received message", fields...)

			return fn(msg)
		}
	}
}

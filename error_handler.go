package kafkalight

import (
	"errors"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type ErrorHandler func(error)

func errorHandler(logger *zap.Logger) ErrorHandler {
	return func(err error) {
		var kafkaErr kafka.Error
		if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
			return
		}

		logger.Error("handler error", zap.Error(err))
	}
}

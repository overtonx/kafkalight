package kafkalight

import "go.uber.org/zap"

type ErrorHandler func(error)

func errorHandler(logger *zap.Logger) ErrorHandler {
	return func(err error) {
		logger.Error("handler error", zap.Error(err))
	}
}

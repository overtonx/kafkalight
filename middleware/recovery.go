package middleware

import (
	"context"
	"fmt"
	"log"

	"github.com/overtonx/kafkalight"
)

// Recovery is a middleware that recovers from panics, logs the panic, and returns an error.
func Recovery() kafkalight.Middleware {
	return func(next kafkalight.MessageHandler) kafkalight.MessageHandler {
		return func(ctx context.Context, msg *kafkalight.Message) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
					log.Printf("recovered from panic: %v", r)
				}
			}()

			return next(ctx, msg)
		}
	}
}

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kafkalight/v4"
	"go.uber.org/zap"
)

func main() {
	// ── Logger ───────────────────────────────────────────────────────────────
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("build logger: %v", err)
	}
	defer logger.Sync() //nolint:errcheck

	// ── Consumer ──────────────────────────────────────────────────────────────
	// Pass a real metric.MeterProvider here (e.g. from go.opentelemetry.io/otel/sdk/metric)
	// to get Prometheus / OTLP metrics. nil falls back to a no-op provider.
	consumer, err := kafkalight.New(
		kafkalight.WithBrokers("localhost:9092"),
		kafkalight.WithGroupID("example-service"),
		kafkalight.WithConcurrency(4),
		kafkalight.WithLogger(logger),
		kafkalight.WithMeterProvider(nil), // replace with your sdk.MeterProvider
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  5,
			InitialDelay: 200 * time.Millisecond,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			Jitter:       0.2,
		}),
	)
	if err != nil {
		logger.Fatal("create consumer", zap.Error(err))
	}

	// ── Register handlers ─────────────────────────────────────────────────────
	consumer.Register("orders", kafkalight.HandlerFunc(handleOrder))

	// Multiple topics — each with its own handler and optional extra middleware.
	consumer.Register("payments", kafkalight.HandlerFunc(handlePayment))

	// ── Graceful shutdown via OS signals ──────────────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatal("consumer exited with error", zap.Error(err))
	}
}

// handleOrder processes messages from the "orders" topic.
func handleOrder(ctx context.Context, msg *kafkalight.Message) error {
	fmt.Printf("[order] topic=%s partition=%d offset=%d value=%s\n",
		msg.Topic, msg.Partition, msg.Offset, msg.Value)

	// Simulate processing. Return an error to trigger retry.
	if string(msg.Value) == "bad" {
		return errors.New("invalid order payload")
	}
	return nil
}

// handlePayment processes messages from the "payments" topic.
func handlePayment(ctx context.Context, msg *kafkalight.Message) error {
	fmt.Printf("[payment] topic=%s partition=%d offset=%d value=%s\n",
		msg.Topic, msg.Partition, msg.Offset, msg.Value)
	return nil
}

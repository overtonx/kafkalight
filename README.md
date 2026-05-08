# kafkalight

[![Go Reference](https://pkg.go.dev/badge/github.com/kafkalight/v4.svg)](https://pkg.go.dev/github.com/kafkalight/v4)
[![Go Report Card](https://goreportcard.com/badge/github.com/kafkalight/v4)](https://goreportcard.com/report/github.com/kafkalight/v4)

Production-ready Kafka consumer library for Go built on top
of [segmentio/kafka-go](https://github.com/segmentio/kafka-go).

## Features

- **Handler registration** per topic — clean, router-style API
- **Middleware chain** — Recovery, Logging (zap), Metrics (OTel) built-in; bring your own
- **Manual offset commit** — offset advances only after successful processing
- **Retry with exponential backoff + jitter** — configurable attempts, delays, multiplier
- **OpenTelemetry metrics** — counters and latency histogram out of the box
- **Graceful shutdown** — drains in-flight messages before stopping
- **Concurrency** — configurable worker pool per topic

## Installation

```bash
go get github.com/kafkalight/v4
```

Requires Go 1.21+.

## Quick start

```go
package main

import (
	"context"
	"os/signal"
	"syscall"

	kafkalight "github.com/kafkalight/v4"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	consumer, err := kafkalight.New(
		kafkalight.WithBrokers("localhost:9092"),
		kafkalight.WithGroupID("my-service"),
		kafkalight.WithConcurrency(4),
		kafkalight.WithLogger(logger),
	)
	if err != nil {
		logger.Fatal("create consumer", zap.Error(err))
	}

	consumer.Register("orders", kafkalight.HandlerFunc(func(ctx context.Context, msg *kafkalight.Message) error {
		// process msg.Value …
		return nil
	}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	consumer.Run(ctx) // blocks until ctx is cancelled
}
```

## Configuration

All options are passed via functional `WithXxx` helpers:

| Option                         | Default                       | Description                          |
|--------------------------------|-------------------------------|--------------------------------------|
| `WithBrokers(addrs...)`        | **required**                  | Kafka broker addresses               |
| `WithGroupID(id)`              | **required**                  | Consumer group ID                    |
| `WithConcurrency(n)`           | `1`                           | Workers per topic reader             |
| `WithRetryPolicy(p)`           | 3 attempts, 100ms→10s backoff | Retry behaviour                      |
| `WithGlobalMiddlewares(mw...)` | none                          | Middlewares applied to every handler |
| `WithLogger(l)`                | `zap.NewNop()`                | Zap logger                           |
| `WithMeterProvider(mp)`        | noop                          | OpenTelemetry `MeterProvider`        |
| `WithReadTimeout(d)`           | `10s`                         | Max idle wait for a Kafka message    |
| `WithCommitInterval(d)`        | `0` (per-message)             | Offset auto-commit interval          |
| `WithStartOffset(o)`           | `kafka.LastOffset`            | Starting offset (no committed state) |

## Handler registration

```go
// Via interface
consumer.Register("payments", myHandler)

// Via function literal
consumer.RegisterFunc("payments", func (ctx context.Context, msg *kafkalight.Message) error {
return processPayment(ctx, msg.Value)
})

// With topic-specific middlewares (applied after global ones)
consumer.Register("payments", myHandler, authMiddleware, tracingMiddleware)
```

## Middleware

```go
// Built-in middlewares (applied automatically in this order):
//   RecoveryMiddleware → LoggingMiddleware → MetricsMiddleware → your handlers

// Custom middleware:
func TimeoutMiddleware(d time.Duration) kafkalight.Middleware {
return func (next kafkalight.Handler) kafkalight.Handler {
return kafkalight.HandlerFunc(func (ctx context.Context, msg *kafkalight.Message) error {
ctx, cancel := context.WithTimeout(ctx, d)
defer cancel()
return next.Handle(ctx, msg)
})
}
}

consumer.Register("orders", handler, TimeoutMiddleware(5*time.Second))
```

## Retry policy

```go
consumer, _ := kafkalight.New(
kafkalight.WithBrokers("localhost:9092"),
kafkalight.WithGroupID("svc"),
kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
MaxAttempts:  5,
InitialDelay: 200 * time.Millisecond,
MaxDelay:     30 * time.Second,
Multiplier:   2.0,
Jitter:       0.2, // ±20% randomisation
}),
)
```

After all attempts are exhausted the offset is committed and the message is skipped.

## OpenTelemetry metrics

Pass any `metric.MeterProvider` (e.g. from the OTel SDK):

```go
import "go.opentelemetry.io/otel/sdk/metric"

provider := metric.NewMeterProvider( /* exporters … */)
consumer, _ := kafkalight.New(
kafkalight.WithMeterProvider(provider),
// …
)
```

Instruments registered:

| Name                                      | Type      | Labels                      |
|-------------------------------------------|-----------|-----------------------------|
| `kafka_consumer_messages_processed_total` | Counter   | `topic`, `status=ok\|error` |
| `kafka_consumer_retries_total`            | Counter   | `topic`                     |
| `kafka_consumer_processing_duration_ms`   | Histogram | `topic`                     |

## Graceful shutdown

```go
// Option 1: cancel the context passed to Run
ctx, cancel := context.WithCancel(context.Background())
go consumer.Run(ctx)
cancel() // triggers shutdown

// Option 2: call Shutdown from another goroutine
go consumer.Run(ctx)
consumer.Shutdown()
```

`Run` waits for all in-flight messages to complete processing before returning.

## Message type

```go
type Message struct {
Topic     string
Partition int
Offset    int64
Key       []byte
Value     []byte
Headers   []Header
Time      time.Time
}

// Helpers
msg.HeaderValue("x-correlation-id") // []byte or nil
msg.Raw() // underlying kafka.Message
```

## Error types

```go
errors.Is(err, kafkalight.ErrNoHandler) // no handler registered for topic
errors.Is(err, kafkalight.ErrMaxRetries) // all retry attempts exhausted
```

## License

MIT

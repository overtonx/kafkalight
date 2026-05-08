package kafkalight

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const (
	metricProcessed = "kafka_consumer_messages_processed_total"
	metricRetries   = "kafka_consumer_retries_total"
	metricDuration  = "kafka_consumer_processing_duration_ms"
)

// metrics holds all OTel instruments for the consumer.
type metrics struct {
	processed metric.Int64Counter
	retries   metric.Int64Counter
	duration  metric.Float64Histogram
}

func newMetrics(mp metric.MeterProvider) (*metrics, error) {
	if mp == nil {
		return &metrics{}, nil
	}

	meter := mp.Meter("github.com/kafkalight/v4")

	processed, err := meter.Int64Counter(metricProcessed,
		metric.WithDescription("Total number of processed Kafka messages"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	retries, err := meter.Int64Counter(metricRetries,
		metric.WithDescription("Total number of message processing retries"),
		metric.WithUnit("{retry}"),
	)
	if err != nil {
		return nil, err
	}

	hist, err := meter.Float64Histogram(metricDuration,
		metric.WithDescription("Message processing duration in milliseconds"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000),
	)
	if err != nil {
		return nil, err
	}

	return &metrics{
		processed: processed,
		retries:   retries,
		duration:  hist,
	}, nil
}

func (m *metrics) incProcessed(ctx context.Context, topic, status string) {
	if m.processed == nil {
		return
	}
	m.processed.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("status", status),
		),
	)
}

func (m *metrics) incRetries(ctx context.Context, topic string) {
	if m.retries == nil {
		return
	}
	m.retries.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic)))
}

func (m *metrics) recordDuration(ctx context.Context, topic string, ms float64) {
	if m.duration == nil {
		return
	}
	m.duration.Record(ctx, ms, metric.WithAttributes(attribute.String("topic", topic)))
}

// resolveMeterProvider returns mp if non-nil, otherwise a no-op provider.
func resolveMeterProvider(mp metric.MeterProvider) metric.MeterProvider {
	if mp == nil {
		return noop.NewMeterProvider()
	}
	return mp
}

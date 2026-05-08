package kafkalight

import (
	"errors"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Config holds all configuration for the Consumer.
type Config struct {
	// Brokers is the list of Kafka broker addresses (required).
	Brokers []string

	// GroupID is the consumer group identifier (required).
	GroupID string

	// Concurrency is the number of messages processed concurrently per reader.
	// Defaults to 1 (sequential processing).
	Concurrency int

	// RetryPolicy controls retry behaviour on handler errors.
	RetryPolicy RetryPolicy

	// GlobalMiddlewares are applied to every handler before topic-specific ones.
	GlobalMiddlewares []Middleware

	// Logger is used for internal logging. Defaults to zap.NewNop().
	Logger *zap.Logger

	// MeterProvider supplies OTel instruments. Defaults to a no-op provider.
	MeterProvider metric.MeterProvider

	// ReadTimeout is the maximum time to wait for a new message from Kafka.
	// Defaults to 10 seconds.
	ReadTimeout time.Duration

	// CommitInterval controls how often offsets are auto-committed.
	// Set to 0 to commit after every message (safest).
	CommitInterval time.Duration

	// StartOffset determines where to start reading when no committed offset
	// exists: kafka.FirstOffset (-2) or kafka.LastOffset (-1).
	// Defaults to kafka.LastOffset.
	StartOffset int64
}

// Option is a functional option for Config.
type Option func(*Config)

// WithBrokers sets the Kafka broker addresses.
func WithBrokers(brokers ...string) Option {
	return func(c *Config) { c.Brokers = brokers }
}

// WithGroupID sets the consumer group ID.
func WithGroupID(id string) Option {
	return func(c *Config) { c.GroupID = id }
}

// WithConcurrency sets the number of concurrent workers per reader.
func WithConcurrency(n int) Option {
	return func(c *Config) { c.Concurrency = n }
}

// WithRetryPolicy sets a custom retry policy.
func WithRetryPolicy(p RetryPolicy) Option {
	return func(c *Config) { c.RetryPolicy = p }
}

// WithGlobalMiddlewares prepends middlewares that wrap every handler.
func WithGlobalMiddlewares(mw ...Middleware) Option {
	return func(c *Config) { c.GlobalMiddlewares = append(c.GlobalMiddlewares, mw...) }
}

// WithLogger sets the zap logger.
func WithLogger(l *zap.Logger) Option {
	return func(c *Config) { c.Logger = l }
}

// WithMeterProvider sets the OpenTelemetry MeterProvider.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *Config) { c.MeterProvider = mp }
}

// WithReadTimeout sets the maximum idle wait for new messages.
func WithReadTimeout(d time.Duration) Option {
	return func(c *Config) { c.ReadTimeout = d }
}

// WithCommitInterval sets the offset commit interval (0 = per-message commit).
func WithCommitInterval(d time.Duration) Option {
	return func(c *Config) { c.CommitInterval = d }
}

// WithStartOffset sets the starting offset when no committed offset exists.
// Use kafka.FirstOffset or kafka.LastOffset from segmentio/kafka-go.
func WithStartOffset(offset int64) Option {
	return func(c *Config) { c.StartOffset = offset }
}

func defaultConfig() Config {
	return Config{
		Concurrency:    1,
		RetryPolicy:    DefaultRetryPolicy,
		Logger:         zap.NewNop(),
		ReadTimeout:    10 * time.Second,
		CommitInterval: 0,
		StartOffset:    -1, // kafka.LastOffset
	}
}

func (c *Config) validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("kafkalight: at least one broker is required")
	}
	if c.GroupID == "" {
		return errors.New("kafkalight: GroupID is required")
	}
	if c.Concurrency < 1 {
		c.Concurrency = 1
	}
	return nil
}

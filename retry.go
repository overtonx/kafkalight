package kafkalight

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// RetryPolicy defines how failed message processing is retried.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts (including the first one).
	// 0 or 1 means no retries.
	MaxAttempts int

	// InitialDelay is the wait time before the first retry.
	InitialDelay time.Duration

	// MaxDelay caps the exponential backoff.
	MaxDelay time.Duration

	// Multiplier is the exponential growth factor (e.g. 2.0 for doubling).
	Multiplier float64

	// Jitter adds a random fraction of the current delay to avoid thundering herd.
	// Value in [0, 1]: 0 = no jitter, 1 = up to 100% jitter.
	Jitter float64
}

// DefaultRetryPolicy is a sensible production default.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts:  3,
	InitialDelay: 100 * time.Millisecond,
	MaxDelay:     10 * time.Second,
	Multiplier:   2.0,
	Jitter:       0.3,
}

// delay returns the backoff duration for the given attempt index (0-based).
func (p *RetryPolicy) delay(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	d := float64(p.InitialDelay) * math.Pow(p.Multiplier, float64(attempt-1))
	if p.Jitter > 0 {
		d += d * p.Jitter * rand.Float64() //nolint:gosec
	}
	if d > float64(p.MaxDelay) {
		d = float64(p.MaxDelay)
	}
	return time.Duration(d)
}

// Execute runs fn up to MaxAttempts times.
// It respects context cancellation between retries.
// Returns ErrMaxRetries (wrapping the last error) when all attempts fail.
func (p *RetryPolicy) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	maxAttempts := p.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			wait := p.delay(attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}

		if err := fn(ctx); err != nil {
			lastErr = err
			// Do not retry on context cancellation.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			continue
		}
		return nil // success
	}

	return fmt.Errorf("%w: %w", ErrMaxRetries, lastErr)
}

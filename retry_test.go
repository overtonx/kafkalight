package kafkalight

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryPolicy_SuccessOnFirstAttempt(t *testing.T) {
	t.Parallel()

	p := RetryPolicy{MaxAttempts: 3, InitialDelay: time.Millisecond, Multiplier: 2.0}
	calls := 0
	err := p.Execute(context.Background(), func(_ context.Context) error {
		calls++
		return nil
	})

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestRetryPolicy_SuccessAfterNFailures(t *testing.T) {
	t.Parallel()

	p := RetryPolicy{MaxAttempts: 5, InitialDelay: time.Millisecond, Multiplier: 1.0, Jitter: 0}
	calls := 0
	err := p.Execute(context.Background(), func(_ context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient error")
		}
		return nil
	})

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestRetryPolicy_ExhaustedReturnsErrMaxRetries(t *testing.T) {
	t.Parallel()

	p := RetryPolicy{MaxAttempts: 3, InitialDelay: time.Millisecond, Multiplier: 1.0, Jitter: 0}
	sentinel := errors.New("persistent failure")
	err := p.Execute(context.Background(), func(_ context.Context) error {
		return sentinel
	})

	if !errors.Is(err, ErrMaxRetries) {
		t.Errorf("expected ErrMaxRetries, got %v", err)
	}
	// Execute now uses %w for both sentinels, so the underlying error is
	// accessible via errors.Is.
	if !errors.Is(err, sentinel) {
		t.Errorf("expected underlying sentinel error to be accessible via errors.Is, got %v", err)
	}
}

func TestRetryPolicy_ContextAlreadyCancelledBeforeRun(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	// MaxDelay must be > 0; a zero cap collapses all delays to 0 and makes
	// time.After(0) race with ctx.Done() non-deterministically.
	p := RetryPolicy{
		MaxAttempts:  5,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		Multiplier:   2.0,
		Jitter:       0,
	}
	calls := 0
	err := p.Execute(ctx, func(_ context.Context) error {
		calls++
		return errors.New("some error")
	})

	// First attempt always runs (no pre-attempt ctx check).
	// Second attempt hits the delay select which picks ctx.Done().
	if calls > 1 {
		t.Errorf("expected at most 1 call with pre-cancelled context, got %d", calls)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestRetryPolicy_ContextCancelledDuringRetryDelay(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	// MaxDelay must be > 0; a zero cap collapses all delays to 0 and makes
	// time.After(0) race with ctx.Done() non-deterministically.
	p := RetryPolicy{
		MaxAttempts:  5,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		Multiplier:   2.0,
		Jitter:       0,
	}
	calls := 0
	err := p.Execute(ctx, func(_ context.Context) error {
		calls++
		// Cancel after the first attempt; the delay select on attempt 2 fires ctx.Done.
		cancel()
		return errors.New("some error")
	})

	if calls > 1 {
		t.Errorf("expected at most 1 call after cancel-on-first-attempt, got %d", calls)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestRetryPolicy_MaxAttempts0TreatedAs1(t *testing.T) {
	t.Parallel()

	p := RetryPolicy{MaxAttempts: 0, InitialDelay: time.Millisecond, Multiplier: 2.0}
	calls := 0
	err := p.Execute(context.Background(), func(_ context.Context) error {
		calls++
		return errors.New("fail")
	})

	if calls != 1 {
		t.Errorf("expected 1 call with MaxAttempts=0, got %d", calls)
	}
	if p.MaxAttempts != 0 {
		t.Errorf("Execute must not mutate MaxAttempts field, got %d", p.MaxAttempts)
	}
	if !errors.Is(err, ErrMaxRetries) {
		t.Errorf("expected ErrMaxRetries, got %v", err)
	}
}

func TestRetryPolicy_MaxAttempts1TreatedAs1(t *testing.T) {
	t.Parallel()

	p := RetryPolicy{MaxAttempts: 1, InitialDelay: time.Millisecond, Multiplier: 2.0}
	calls := 0
	_ = p.Execute(context.Background(), func(_ context.Context) error {
		calls++
		return errors.New("fail")
	})

	if calls != 1 {
		t.Errorf("expected exactly 1 call with MaxAttempts=1, got %d", calls)
	}
}

func TestRetryPolicy_DelayCalculation_NoJitter(t *testing.T) {
	t.Parallel()

	p := RetryPolicy{
		InitialDelay: 100 * time.Millisecond,
		Multiplier:   2.0,
		MaxDelay:     10 * time.Second,
		Jitter:       0,
	}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 0},
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
	}

	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			t.Parallel()
			got := p.delay(tc.attempt)
			if got != tc.want {
				t.Errorf("delay(%d): got %v want %v", tc.attempt, got, tc.want)
			}
		})
	}
}

func TestRetryPolicy_DelayExponentialGrowth(t *testing.T) {
	t.Parallel()

	p := RetryPolicy{
		InitialDelay: 10 * time.Millisecond,
		Multiplier:   3.0,
		MaxDelay:     10 * time.Second,
		Jitter:       0,
	}

	d1 := p.delay(1)
	d2 := p.delay(2)
	d3 := p.delay(3)

	if d2 <= d1 {
		t.Errorf("delay should grow: delay(2)=%v <= delay(1)=%v", d2, d1)
	}
	if d3 <= d2 {
		t.Errorf("delay should grow: delay(3)=%v <= delay(2)=%v", d3, d2)
	}
}

func TestRetryPolicy_DelayMaxDelayCap(t *testing.T) {
	t.Parallel()

	p := RetryPolicy{
		InitialDelay: 1 * time.Second,
		Multiplier:   10.0,
		MaxDelay:     5 * time.Second,
		Jitter:       0,
	}

	// Without cap, delay(5) = 1s * 10^4 = 10000s.
	d := p.delay(5)
	if d > p.MaxDelay {
		t.Errorf("delay exceeded MaxDelay: got %v, max %v", d, p.MaxDelay)
	}
	if d != p.MaxDelay {
		t.Errorf("expected delay to equal MaxDelay %v, got %v", p.MaxDelay, d)
	}
}

func TestRetryPolicy_HandlerReturnsContextCanceledNotRetried(t *testing.T) {
	t.Parallel()

	p := RetryPolicy{MaxAttempts: 5, InitialDelay: time.Millisecond, Multiplier: 2.0}
	calls := 0
	err := p.Execute(context.Background(), func(_ context.Context) error {
		calls++
		return context.Canceled
	})

	if calls != 1 {
		t.Errorf("expected 1 call when handler returns context.Canceled, got %d", calls)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled propagated, got %v", err)
	}
}

func TestRetryPolicy_HandlerReturnsDeadlineExceededNotRetried(t *testing.T) {
	t.Parallel()

	p := RetryPolicy{MaxAttempts: 5, InitialDelay: time.Millisecond, Multiplier: 2.0}
	calls := 0
	err := p.Execute(context.Background(), func(_ context.Context) error {
		calls++
		return context.DeadlineExceeded
	})

	if calls != 1 {
		t.Errorf("expected 1 call when handler returns DeadlineExceeded, got %d", calls)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded propagated, got %v", err)
	}
}

func TestDefaultRetryPolicy_Values(t *testing.T) {
	t.Parallel()

	p := DefaultRetryPolicy

	if p.MaxAttempts != 3 {
		t.Errorf("MaxAttempts: got %d want 3", p.MaxAttempts)
	}
	if p.InitialDelay != 100*time.Millisecond {
		t.Errorf("InitialDelay: got %v want 100ms", p.InitialDelay)
	}
	if p.MaxDelay != 10*time.Second {
		t.Errorf("MaxDelay: got %v want 10s", p.MaxDelay)
	}
	if p.Multiplier != 2.0 {
		t.Errorf("Multiplier: got %v want 2.0", p.Multiplier)
	}
	if p.Jitter != 0.3 {
		t.Errorf("Jitter: got %v want 0.3", p.Jitter)
	}
}

func TestRetryPolicy_JitterIncreasesDelay(t *testing.T) {
	t.Parallel()

	base := RetryPolicy{
		InitialDelay: 100 * time.Millisecond,
		Multiplier:   2.0,
		MaxDelay:     10 * time.Second,
		Jitter:       0,
	}
	withJitter := RetryPolicy{
		InitialDelay: 100 * time.Millisecond,
		Multiplier:   2.0,
		MaxDelay:     10 * time.Second,
		Jitter:       1.0,
	}

	baseDelay := base.delay(1)
	// With full jitter the delay is in [baseDelay, 2*baseDelay].
	// Run multiple samples to confirm jitter is applied at all.
	foundAboveBase := false
	for i := 0; i < 100; i++ {
		d := withJitter.delay(1)
		if d < baseDelay {
			t.Errorf("jitter produced delay %v below base %v", d, baseDelay)
		}
		if d > baseDelay {
			foundAboveBase = true
		}
	}
	if !foundAboveBase {
		t.Error("jitter never produced a delay above the base delay in 100 samples")
	}
}

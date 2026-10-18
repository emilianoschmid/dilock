package postgres

import (
	"context"
	"time"

	dilock "example.invalid/dilock/core"
)

// TryAcquire returns nil,nil only for acknowledged contention.
func (h *Handle) TryAcquire(ctx context.Context, options dilock.Options) (*Lease, error) {
	return h.provider.tryAcquire(ctx, h.key, options)
}

// Acquire waits up to wait for acknowledged contention to clear.
func (h *Handle) Acquire(ctx context.Context, wait time.Duration, options dilock.Options) (*Lease, error) {
	if ctx == nil || wait <= 0 || wait > 24*time.Hour {
		return nil, dilock.ErrInvalidOptions
	}
	policy, _, _, err := options.Normalized()
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(wait)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lease, err := h.provider.tryAcquire(ctx, h.key, policy)
		if err != nil {
			return nil, err
		}
		if lease != nil {
			return lease, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, dilock.ErrWaitTimeout
		}
		pause := h.provider.options.PollInterval
		if pause > remaining {
			pause = remaining
		}
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// AcquireDefault uses the immutable provider policy.
func (h *Handle) AcquireDefault(ctx context.Context) (*Lease, error) {
	o := h.provider.options.DefaultAcquisition
	return h.Acquire(ctx, o.Wait, o.Lease)
}

// WithLease runs work and requires acknowledged same-session cleanup.
func (h *Handle) WithLease(ctx context.Context, work func(*Lease) error) error {
	if work == nil {
		return dilock.ErrInvalidOptions
	}
	lease, err := h.AcquireDefault(ctx)
	if err != nil {
		return err
	}
	workErr := work(lease)
	cleanupCtx, cancel := context.WithTimeout(context.Background(), h.provider.options.CleanupTimeout)
	outcome, cleanupErr := lease.Release(cleanupCtx)
	cancel()
	if workErr != nil {
		if outcome != dilock.ReleasedOwner {
			h.provider.observe(outcome, "body_failed")
		}
		return workErr
	}
	if outcome != dilock.ReleasedOwner {
		return &dilock.ScopeReleaseError{Outcome: outcome, Cause: cleanupErr}
	}
	return nil
}

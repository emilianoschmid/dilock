package redis

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"example.invalid/dilock/core"
)

// Handle is a reusable resource. Independent acquisitions are separate owner generations.
type Handle struct {
	provider *Provider
	key      string
}

type setResult struct {
	ok  bool
	err error
}

// TryAcquireDefault performs one SET NX PX attempt using the immutable lease
// policy captured by the provider. It does not wait for contention.
func (h *Handle) TryAcquireDefault(ctx context.Context) (*Lease, error) {
	return h.TryAcquire(ctx, h.provider.defaultAcquisition.Lease)
}

// TryAcquire performs one SET NX PX attempt. A dispatched request without an
// acknowledgement returns ErrAcquisitionUnknown; callers must not enter work.
func (h *Handle) TryAcquire(ctx context.Context, options dilock.Options) (*Lease, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	if !h.provider.beginOperation() {
		return nil, dilock.ErrProviderClosed
	}
	defer h.provider.endOperation()
	if err := options.ValidateComplete(); err != nil {
		return nil, err
	}
	_, ttl, margin, err := options.Complete()
	if err != nil {
		return nil, err
	}
	if options.RequireFencing {
		return nil, dilock.ErrUnsupported
	}
	p := h.provider
	if err := p.metadata(ctx, options.RPCTimeout); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	owner, err := token()
	if err != nil {
		return nil, fmt.Errorf("generate acquisition owner: %w", err)
	}
	sent := time.Now()
	rpcCtx, cancel := context.WithTimeout(ctx, options.RPCTimeout)
	cmd := p.client.SetNX(rpcCtx, h.key, owner, time.Duration(ttl)*time.Millisecond)
	result := make(chan setResult, 1)
	go func() { ok, commandErr := cmd.Result(); result <- setResult{ok: ok, err: commandErr} }()
	var response setResult
	select {
	case response = <-result:
		cancel()
	case <-rpcCtx.Done():
		cancel()
		// The command may already have committed. Keep owner-only cleanup tracked,
		// including a second pass after the command has finished in case it was late.
		p.trackCleanup(func() {
			p.cleanupOwner(h.key, owner, options.CleanupTimeout)
			<-result
			p.cleanupOwner(h.key, owner, options.CleanupTimeout)
		})
		return nil, &dilock.OperationError{Kind: dilock.ErrAcquisitionUnknown, Message: "SET has no ownership acknowledgement", Cause: rpcCtx.Err()}
	}
	if response.err != nil {
		if isServerError(response.err) {
			return nil, &dilock.OperationError{Kind: dilock.ErrBackendUnavailable, Message: "Redis rejected acquisition", Cause: response.err}
		}
		p.trackCleanup(func() {
			p.cleanupOwner(h.key, owner, options.CleanupTimeout)
			p.cleanupOwner(h.key, owner, options.CleanupTimeout)
		})
		return nil, &dilock.OperationError{Kind: dilock.ErrAcquisitionUnknown, Message: "SET has no ownership acknowledgement", Cause: response.err}
	}
	if !response.ok {
		// SET NX acknowledged an existing key. Classify its type with one
		// bounded, read-only command before reporting ordinary contention.
		typeCtx, typeCancel := context.WithTimeout(ctx, options.RPCTimeout)
		defer typeCancel()
		keyType, typeErr := p.client.Type(typeCtx, h.key).Result()
		if typeErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, mapBackend(typeErr, "Redis key type query failed after acknowledged contention")
		}
		if keyType != "string" && keyType != "none" {
			return nil, dilock.ErrProtocolMismatch
		}
		return nil, nil
	}
	deadline := sent.Add(time.Duration(ttl)*time.Millisecond - margin)
	if ctx.Err() != nil || time.Now().After(deadline) {
		p.trackCleanup(func() { p.cleanupOwner(h.key, owner, options.CleanupTimeout) })
		return nil, &dilock.OperationError{Kind: dilock.ErrAcquisitionUnknown, Message: "SET acknowledgement arrived after cancellation or conservative expiry", Cause: ctx.Err()}
	}
	lease := newLease(p, h.key, owner, options, ttl, margin, deadline)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		lease.markLost("provider_shutdown")
		if outcome := p.cleanupOwner(h.key, owner, options.CleanupTimeout); outcome != dilock.ReleaseUnknown {
			return nil, dilock.ErrProviderClosed
		}
		p.trackCleanup(func() { p.cleanupOwner(h.key, owner, options.CleanupTimeout) })
		return nil, &dilock.OperationError{
			Kind:    dilock.ErrAcquisitionUnknown,
			Message: "provider closed after Redis acknowledged acquisition; owner cleanup is unknown",
			Cause:   dilock.ErrProviderClosed,
		}
	}
	p.leases[lease] = struct{}{}
	p.mu.Unlock()
	return lease, nil
}

// Acquire retries only acknowledged contention. wait includes metadata, RPCs and backoff.
func (h *Handle) Acquire(ctx context.Context, wait time.Duration, options dilock.Options) (*Lease, error) {
	if ctx == nil || wait <= 0 || wait > 24*time.Hour {
		return nil, dilock.ErrInvalidOptions
	}
	deadline := time.Now().Add(wait)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, dilock.ErrWaitTimeout
		}
		attemptCtx, cancel := context.WithDeadline(ctx, deadline)
		lease, err := h.TryAcquire(attemptCtx, options)
		cancel()
		if err != nil {
			// A failure while classifying an acknowledged contender is a wait timeout
			// only when the caller is live and the overall acquisition deadline ended.
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil && time.Until(deadline) <= 0 {
				return nil, dilock.ErrWaitTimeout
			}
			return nil, err
		}
		if lease != nil {
			if time.Now().After(deadline) {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), options.CleanupTimeout)
				outcome, cleanupErr := lease.Release(cleanupCtx)
				cleanupCancel()
				return nil, lateAcquireFailure(outcome, cleanupErr)
			}
			return lease, nil
		}
		remaining = time.Until(deadline)
		if remaining <= 0 {
			return nil, dilock.ErrWaitTimeout
		}
		pause := time.Duration(rand.Int64N(int64(31*time.Millisecond))) + 20*time.Millisecond
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

// lateAcquireFailure preserves uncertainty when an acknowledged acquisition
// arrives after the caller's wait budget but owner-checked cleanup is unconfirmed.
func lateAcquireFailure(outcome dilock.ReleaseOutcome, cleanupErr error) error {
	if outcome == dilock.ReleaseUnknown || cleanupErr != nil {
		return &dilock.OperationError{
			Kind:    dilock.ErrAcquisitionUnknown,
			Message: "Redis acquired after the wait deadline and owner cleanup was not acknowledged",
			Cause:   cleanupErr,
		}
	}
	return dilock.ErrWaitTimeout
}

// AcquireDefault uses the immutable acquisition policy captured by the provider.
func (h *Handle) AcquireDefault(ctx context.Context) (*Lease, error) {
	policy := h.provider.defaultAcquisition
	return h.Acquire(ctx, policy.Wait, policy.Lease)
}

// WithLease acquires with provider defaults and releases using a fresh bounded context.
// Callback errors are preserved; uncertain scope cleanup is reported through the
// provider observer (or the standard logger when no observer is configured).
func (h *Handle) WithLease(ctx context.Context, work func(*Lease) error) (err error) {
	return h.WithLeaseOptions(ctx, h.provider.defaultAcquisition, work)
}

// WithLeaseOptions runs work exactly once under the complete explicit policy.
func (h *Handle) WithLeaseOptions(ctx context.Context, policy dilock.AcquireOptions, work func(*Lease) error) (err error) {
	if work == nil {
		return fmt.Errorf("work callback is required")
	}
	normErr := policy.ValidateComplete()
	if normErr != nil {
		return normErr
	}
	lease, acquireErr := h.Acquire(ctx, policy.Wait, policy.Lease)
	if acquireErr != nil {
		return acquireErr
	}
	reason := "scope_exit"
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), policy.Lease.CleanupTimeout)
		outcome, cleanupErr := lease.Release(cleanupCtx)
		cancel()
		if outcome != dilock.ReleasedOwner || cleanupErr != nil {
			h.provider.observeCleanup(outcome, reason)
			if err == nil {
				err = &dilock.ScopeReleaseError{Outcome: outcome, Cause: cleanupErr}
			}
		}
	}()
	err = work(lease)
	if err != nil {
		reason = "body_failed"
	}
	return err
}

func isServerError(err error) bool {
	type redisError interface {
		error
		RedisError()
	}
	var target redisError
	return errors.As(err, &target)
}

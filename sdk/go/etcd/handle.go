package etcd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"example.invalid/dilock/core"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handle is a reusable resource. Every call obtains a distinct server lease,
// so concurrent attempts are never reentrant.
type Handle struct {
	provider *Provider
	name     string
}

// TryAcquire performs a bounded native Lock call. A timeout is reported as
// ordinary contention only after the unique lease revoke is acknowledged;
// cancellation and unconfirmed cleanup remain errors. Etcd's Lock RPC queues
// waiters, so TryAcquire may wait up to Options.RPCTimeout before reporting busy.
func (h *Handle) TryAcquire(ctx context.Context, options dilock.Options) (*Lease, error) {
	return h.acquire(ctx, options, true)
}

// Acquire queues through the native Lock service until the caller's wait
// budget expires. wait bounds the entire acquisition wait, including metadata,
// grant and lock calls; renewal continues while queued.
func (h *Handle) Acquire(ctx context.Context, wait time.Duration, options dilock.Options) (*Lease, error) {
	if ctx == nil || wait <= 0 || wait > 24*time.Hour {
		return nil, dilock.ErrInvalidOptions
	}
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	waitDeadline, _ := waitCtx.Deadline()
	lease, err := h.acquire(waitCtx, options, false)
	if overallWaitExpired(err, ctx, waitDeadline) {
		return nil, dilock.ErrWaitTimeout
	}
	return lease, err
}

// AcquireDefault uses the immutable policy captured by the provider.
func (h *Handle) AcquireDefault(ctx context.Context) (*Lease, error) {
	policy := h.provider.defaultAcquisition
	return h.Acquire(ctx, policy.Wait, policy.Lease)
}

// WithLease acquires with provider defaults and releases with an independent,
// bounded cleanup context. The callback is invoked exactly once.
func (h *Handle) WithLease(ctx context.Context, work func(*Lease) error) (err error) {
	return h.WithLeaseOptions(ctx, h.provider.defaultAcquisition, work)
}

// WithLeaseOptions runs work once under a complete explicit policy. A callback
// error remains primary if cleanup also fails.
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

func overallWaitExpired(err error, caller context.Context, deadline time.Time) bool {
	return err != nil && !errors.Is(err, dilock.ErrAcquisitionUnknown) &&
		errors.Is(err, context.DeadlineExceeded) && caller.Err() == nil && time.Until(deadline) <= 0
}

func (h *Handle) acquire(ctx context.Context, options dilock.Options, try bool) (*Lease, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	p := h.provider
	if !p.begin() {
		return nil, dilock.ErrProviderClosed
	}
	defer p.ops.Done()
	operationCtx, operationCancel := context.WithCancel(ctx)
	stopOnClose := context.AfterFunc(p.closeCtx, operationCancel)
	defer stopOnClose()
	defer operationCancel()
	ctx = operationCtx
	if err := options.ValidateComplete(); err != nil {
		return nil, err
	}
	_, requestedTTL, _, err := options.Complete()
	if err != nil {
		return nil, err
	}
	if err := p.metadata(ctx, options.RPCTimeout); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ttlSeconds := int64(math.Ceil(float64(requestedTTL) / 1000.0))
	grantSent := time.Now()
	grantCtx, grantCancel := context.WithTimeout(ctx, options.RPCTimeout)
	grant, err := p.client.Grant(grantCtx, ttlSeconds)
	grantCancel()
	if err != nil {
		if ctx.Err() != nil {
			// Grant may have committed before its response was lost to caller or
			// provider cancellation. Without the returned lease ID there is no
			// owner-specific revoke we can safely issue.
			return nil, acquisitionUnknown("etcd lease grant was canceled before its acknowledgement", ctx.Err())
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, acquisitionUnknown("etcd lease grant was not acknowledged", err)
		}
		return nil, backendError("etcd lease grant failed", err)
	}
	if grant == nil || grant.ID == 0 || grant.TTL <= 0 {
		if grant != nil && grant.ID != 0 {
			if revokeErr := p.revoke(clientv3.LeaseID(grant.ID), options.CleanupTimeout); revokeErr != nil {
				return nil, acquisitionUnknown("etcd returned an invalid lease grant and owner cleanup was not acknowledged", errors.Join(dilock.ErrProtocolMismatch, revokeErr))
			}
		}
		return nil, dilock.ErrProtocolMismatch
	}
	effectiveTTL := time.Duration(grant.TTL) * time.Second
	margin := validityMargin(effectiveTTL)
	if options.RPCTimeout >= effectiveTTL-margin {
		if revokeErr := p.revoke(clientv3.LeaseID(grant.ID), options.CleanupTimeout); revokeErr != nil {
			return nil, acquisitionUnknown("etcd granted a lease too short for the configured RPC budget and cleanup was not acknowledged", errors.Join(dilock.ErrInvalidOptions, revokeErr))
		}
		return nil, dilock.ErrInvalidOptions
	}
	lease := newLease(p, clientv3.LeaseID(grant.ID), options, effectiveTTL, margin, grantSent.Add(effectiveTTL-margin))
	lease.startKeepAlive()

	lockCtx := ctx
	lockCancel := func() {}
	if try {
		lockCtx, lockCancel = context.WithTimeout(ctx, options.RPCTimeout)
	}
	lockedKey, lockErr := p.locks.Lock(lockCtx, h.name, lease.id)
	lockCancel()
	if lockErr != nil {
		cleanupErr := lease.abandon()
		if ctx.Err() != nil {
			if cleanupErr != nil {
				return nil, acquisitionUnknown("canceled etcd Lock waiter cleanup was not acknowledged", cleanupErr)
			}
			return nil, ctx.Err()
		}
		if errors.Is(lockErr, context.DeadlineExceeded) || errors.Is(lockErr, context.Canceled) || status.Code(lockErr) == codes.DeadlineExceeded || status.Code(lockErr) == codes.Canceled {
			if cleanupErr != nil {
				return nil, acquisitionUnknown("timed out etcd Lock waiter cleanup was not acknowledged", cleanupErr)
			}
			if try {
				busy, rangeErr := h.hasQueuedOwner(ctx, options.RPCTimeout)
				if rangeErr == nil && busy {
					return nil, nil
				}
				if rangeErr != nil {
					return nil, acquisitionUnknown("timed out etcd Lock call; contention Range was not confirmed", rangeErr)
				}
				return nil, acquisitionUnknown("timed out etcd Lock call without a confirmed competing waiter", lockErr)
			}
			return nil, dilock.ErrWaitTimeout
		}
		if cleanupErr != nil {
			return nil, acquisitionUnknown("etcd Lock outcome and waiter cleanup are unknown", lockErr)
		}
		return nil, backendError("etcd Lock failed; its lease was revoked", lockErr)
	}
	if len(lockedKey) == 0 {
		lease.setKey(nil)
		_ = lease.abandon()
		return nil, dilock.ErrProtocolMismatch
	}
	lease.setKey(lockedKey)
	if err := lease.verifyKey(ctx); err != nil {
		cleanupErr := lease.abandon()
		if cleanupErr != nil {
			return nil, acquisitionUnknown("etcd Lock returned a key but verification and cleanup are unknown", errors.Join(err, cleanupErr))
		}
		return nil, err
	}
	if !p.addLease(lease) {
		if cleanupErr := lease.abandon(); cleanupErr != nil {
			return nil, acquisitionUnknown("provider closed after etcd Lock succeeded; owner cleanup was not acknowledged", errors.Join(dilock.ErrProviderClosed, cleanupErr))
		}
		return nil, dilock.ErrProviderClosed
	}
	if !options.AutoRenew {
		cleanupAt := cleanupDeadline(options.CleanupTimeout)
		if !lease.stopKeeperUntil(cleanupAt) {
			if revokeErr := p.revokeUntil(lease.id, cleanupAt); revokeErr != nil {
				return nil, acquisitionUnknown("etcd keepalive shutdown and acquired lease revoke were not acknowledged", revokeErr)
			}
			lease.markLost("keepalive_shutdown_unconfirmed")
			p.removeLease(lease)
			return nil, dilock.ErrLeaseLost
		}
	}
	return lease, nil
}

func (h *Handle) hasQueuedOwner(ctx context.Context, rpcTimeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := h.provider.client.Get(ctx, h.name+"/", clientv3.WithPrefix())
	if err != nil {
		return false, err
	}
	return resp != nil && resp.Count > 0, nil
}

func validityMargin(ttl time.Duration) time.Duration {
	percent := ttl / 100
	if percent < 10*time.Millisecond {
		return 10 * time.Millisecond
	}
	return percent
}

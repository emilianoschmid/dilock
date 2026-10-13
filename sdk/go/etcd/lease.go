package etcd

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"example.invalid/dilock/core"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Fence is ordered only within Epoch and under the deployment's etcd restore
// policy. Protected resources must atomically register and compare both fields.
type Fence struct {
	Epoch          string
	CreateRevision int64
}

// Lease represents one distinct etcd lease and one native lock generation. Do
// not copy it after first use. Lost is local evidence only and does not cancel
// application work. Use Fence for a resource-side atomic stale-write guard.
type Lease struct {
	provider *Provider
	id       clientv3.LeaseID
	options  dilock.Options
	ttl      time.Duration
	margin   time.Duration

	opMu            sync.Mutex
	mu              sync.Mutex
	state           dilock.State
	lossReason      string
	deadline        time.Time
	expiry          *time.Timer
	key             []byte
	fence           Fence
	verified        bool
	lost            chan struct{}
	lostOnce        sync.Once
	keepCtx         context.Context
	keepCancel      context.CancelFunc
	keepDone        chan struct{}
	releaseStarted  bool
	releaseFinished bool
	releaseDone     chan struct{}
	releaseOutcome  dilock.ReleaseOutcome
	releaseErr      error
}

func newLease(p *Provider, id clientv3.LeaseID, options dilock.Options, ttl, margin time.Duration, deadline time.Time) *Lease {
	l := &Lease{provider: p, id: id, options: options, ttl: ttl, margin: margin,
		state: dilock.Held, deadline: deadline, lost: make(chan struct{})}
	l.expiry = time.AfterFunc(time.Until(deadline), func() { l.markLost("conservative_deadline_elapsed") })
	return l
}

func (l *Lease) startKeepAlive() {
	l.keepCtx, l.keepCancel = context.WithCancel(context.Background())
	l.keepDone = make(chan struct{})
	go func() {
		defer close(l.keepDone)
		for {
			l.mu.Lock()
			if l.state != dilock.Held {
				l.mu.Unlock()
				return
			}
			deadline := l.deadline
			l.mu.Unlock()
			wait := l.ttl / 3
			remaining := time.Until(deadline)
			if remaining <= 0 {
				l.markLost("conservative_deadline_elapsed")
				return
			}
			if wait > remaining/2 {
				wait = remaining / 2
			}
			timer := time.NewTimer(wait)
			select {
			case <-l.keepCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if renewed, err := l.renew(l.keepCtx); err != nil || !renewed {
				return
			}
		}
	}()
}

// State is a monotonic local observation, not authoritative etcd state.
func (l *Lease) State() dilock.State { l.expire(); l.mu.Lock(); defer l.mu.Unlock(); return l.state }

// Lost closes once when local reliance ends. It does not stop goroutines doing user work.
func (l *Lease) Lost() <-chan struct{} { return l.lost }

// LossReason returns the first local reason for loss, or an empty string while held.
func (l *Lease) LossReason() string { l.mu.Lock(); defer l.mu.Unlock(); return l.lossReason }

// EffectiveLeaseMilliseconds reports the server-granted TTL, rounded to the
// whole seconds accepted by the etcd Lease API, not remaining local validity.
func (l *Lease) EffectiveLeaseMilliseconds() int64 { return l.ttl.Milliseconds() }

// Fence returns the verified epoch and exact-key create_revision. The boolean
// is false until linearizable ownership verification completes.
func (l *Lease) Fence() (Fence, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fence, l.verified && l.state == dilock.Held
}

func (l *Lease) setKey(key []byte) {
	l.mu.Lock()
	l.key = append([]byte(nil), key...)
	l.mu.Unlock()
}

func (l *Lease) verifyKey(parent context.Context) error {
	l.mu.Lock()
	deadline := l.deadline
	key := append([]byte(nil), l.key...)
	l.mu.Unlock()
	budget := l.options.RPCTimeout
	if remaining := time.Until(deadline); remaining < budget {
		budget = remaining
	}
	if budget <= 0 {
		return acquisitionUnknown("native Lock ownership could not be verified within lease validity", dilock.ErrLeaseLost)
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	resp, err := l.provider.client.Get(ctx, string(key)) // Linearizable Range on the returned opaque key.
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return acquisitionUnknown("native Lock ownership Range was not acknowledged", err)
		}
		return backendError("native Lock ownership Range failed", err)
	}
	if resp == nil || resp.Count != 1 || len(resp.Kvs) != 1 {
		return dilock.ErrProtocolMismatch
	}
	kv := resp.Kvs[0]
	if string(kv.Key) != string(key) || kv.Lease != int64(l.id) || kv.CreateRevision <= 0 {
		return dilock.ErrProtocolMismatch
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != dilock.Held || !time.Now().Before(l.deadline) {
		return acquisitionUnknown("native Lock verification arrived after conservative lease validity", dilock.ErrLeaseLost)
	}
	l.fence = Fence{Epoch: l.provider.epoch, CreateRevision: kv.CreateRevision}
	l.verified = true
	return nil
}

func (l *Lease) expire() {
	l.mu.Lock()
	expired := l.state == dilock.Held && !time.Now().Before(l.deadline)
	l.mu.Unlock()
	if expired {
		l.markLost("conservative_deadline_elapsed")
	}
}

func (l *Lease) markLost(reason string) {
	l.mu.Lock()
	if l.state == dilock.Held {
		l.state = dilock.Lost
		l.lossReason = reason
		if l.expiry != nil {
			l.expiry.Stop()
		}
		l.lostOnce.Do(func() { close(l.lost) })
		if l.keepCancel != nil {
			l.keepCancel()
		}
	}
	l.mu.Unlock()
}

// stopKeeperUntil requests shutdown and bounds the wait for an in-flight
// KeepAliveOnce. A timeout does not prove the remote call stopped; callers
// must revoke this lease before abandoning it.
func (l *Lease) stopKeeperUntil(deadline time.Time) bool {
	l.mu.Lock()
	cancel, done := l.keepCancel, l.keepDone
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done == nil {
		return true
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}
	ctx, cancelWait := context.WithTimeout(context.Background(), remaining)
	defer cancelWait()
	return waitForKeeper(ctx, done)
}

func cleanupDeadline(timeout time.Duration) time.Time { return time.Now().Add(timeout) }

// cleanupWorkDeadline reserves the latter half of one cleanup budget for a
// lease Revoke attempt. Revoke is the fallback that removes every key attached
// to this lease when a keepalive or Unlock does not stop promptly.
func cleanupWorkDeadline(deadline time.Time) time.Time {
	remaining, ok := remainingCleanup(deadline)
	if !ok {
		return deadline
	}
	return deadline.Add(-remaining / 2)
}

func remainingCleanup(deadline time.Time) (time.Duration, bool) {
	return remainingCleanupAt(deadline, time.Now())
}

func remainingCleanupAt(deadline, now time.Time) (time.Duration, bool) {
	remaining := deadline.Sub(now)
	return remaining, remaining > 0
}

func (l *Lease) lockOperationsUntil(deadline time.Time) bool {
	for {
		if l.opMu.TryLock() {
			return true
		}
		remaining, ok := remainingCleanup(deadline)
		if !ok {
			return false
		}
		pause := time.Millisecond
		if remaining < pause {
			pause = remaining
		}
		timer := time.NewTimer(pause)
		<-timer.C
	}
}

// waitForKeeper stops waiting when the supplied cleanup budget expires. The
// lease must still be revoked after a timeout: dropping a goroutine that is
// waiting for KeepAliveOnce must not make Release block indefinitely.
func waitForKeeper(ctx context.Context, done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// Renew performs one bounded KeepAliveOnce. A missing, late or invalid reply
// ends local reliance; reconnecting never resurrects a lease declared lost.
func (l *Lease) Renew(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, errors.New("context is required")
	}
	return l.renew(ctx)
}

func (l *Lease) renew(parent context.Context) (bool, error) {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	l.expire()
	l.mu.Lock()
	if l.state != dilock.Held {
		l.mu.Unlock()
		return false, nil
	}
	oldDeadline := l.deadline
	l.mu.Unlock()
	if err := parent.Err(); err != nil {
		return false, err
	}
	budget := l.options.RPCTimeout
	if remaining := time.Until(oldDeadline); remaining < budget {
		budget = remaining
	}
	if budget <= 0 {
		l.markLost("conservative_deadline_elapsed")
		return false, dilock.ErrLeaseLost
	}
	sent := time.Now()
	ctx, cancel := context.WithTimeout(parent, budget)
	response, err := l.provider.client.KeepAliveOnce(ctx, l.id)
	cancel()
	if err != nil || response == nil || response.TTL <= 0 {
		l.markLost("keepalive_unconfirmed")
		if err == nil {
			err = dilock.ErrLeaseLost
		}
		return false, backendError("etcd keepalive was not confirmed; lease lost", err)
	}
	deadline := sent.Add(time.Duration(response.TTL)*time.Second - l.margin)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != dilock.Held || !l.deadline.Equal(oldDeadline) || !time.Now().Before(oldDeadline) || !time.Now().Before(deadline) {
		if l.state == dilock.Held {
			l.state = dilock.Lost
			l.lossReason = "keepalive_late_or_raced"
			l.expiry.Stop()
			l.lostOnce.Do(func() { close(l.lost) })
		}
		return false, dilock.ErrLeaseLost
	}
	l.deadline = deadline
	l.expiry.Reset(time.Until(deadline))
	return true, nil
}

func (l *Lease) abandon() error {
	l.markLost("acquisition_abandoned")
	deadline := cleanupDeadline(l.options.CleanupTimeout)
	workDeadline := cleanupWorkDeadline(deadline)
	if !l.stopKeeperUntil(workDeadline) {
		// Revocation is independent of the possibly stuck keepalive RPC. An
		// acknowledged revoke destroys the attempt lease and all attached lock
		// keys, so there is no need to wait for Unlock first.
		if err := l.provider.revokeUntil(l.id, deadline); err != nil {
			return acquisitionUnknown("etcd keepalive shutdown and lease revoke were not acknowledged", err)
		}
		return nil
	}
	l.mu.Lock()
	key := append([]byte(nil), l.key...)
	l.mu.Unlock()
	if len(key) > 0 {
		if remaining, ok := remainingCleanup(workDeadline); ok {
			unlockCtx, unlockCancel := context.WithTimeout(context.Background(), remaining)
			_ = l.provider.locks.Unlock(unlockCtx, key)
			unlockCancel()
		}
	}
	if err := l.provider.revokeUntil(l.id, deadline); err != nil {
		return err
	}
	return nil
}

// Release withdraws local permission, verifies ownership of the exact opaque
// etcd key, unlocks only that key and revokes only this acquisition's lease.
// Cleanup shares one monotonic CleanupTimeout deadline across keeper shutdown,
// Unlock and Revoke. ctx bounds
// this call's wait; cleanup continues independently if ctx is canceled.
// Repeated calls return the cached outcome. Local state never proves the
// server acknowledged release.
func (l *Lease) Release(ctx context.Context) (dilock.ReleaseOutcome, error) {
	if ctx == nil {
		return dilock.ReleaseUnknown, errors.New("context is required")
	}
	l.mu.Lock()
	if l.releaseStarted {
		done := l.releaseDone
		l.mu.Unlock()
		select {
		case <-done:
			l.mu.Lock()
			outcome := l.releaseOutcome
			l.mu.Unlock()
			return outcome, nil
		case <-ctx.Done():
			return dilock.ReleaseUnknown, ctx.Err()
		}
	}
	l.releaseStarted = true
	l.releaseDone = make(chan struct{})
	if l.state == dilock.Held {
		l.state = dilock.Releasing
	}
	if l.expiry != nil {
		l.expiry.Stop()
	}
	stopKeepalive := l.keepCancel
	l.mu.Unlock()
	if stopKeepalive != nil {
		stopKeepalive()
	}
	// Cleanup proceeds independently of the caller's context. The caller only
	// controls how long it waits for the outcome; a stuck keepalive must not
	// prevent lease revocation from being attempted.
	done := l.releaseDone
	go l.completeRelease()
	select {
	case <-done:
		l.mu.Lock()
		outcome := l.releaseOutcome
		err := l.releaseErr
		l.mu.Unlock()
		return outcome, err
	case <-ctx.Done():
		return dilock.ReleaseUnknown, ctx.Err()
	}
}

func (l *Lease) completeRelease() {
	deadline := cleanupDeadline(l.options.CleanupTimeout)
	workDeadline := cleanupWorkDeadline(deadline)
	keeperStopped := l.stopKeeperUntil(workDeadline)

	l.mu.Lock()
	key := append([]byte(nil), l.key...)
	l.mu.Unlock()

	// If KeepAliveOnce did not stop within budget, revoke first. A confirmed
	// revoke deletes every key attached to this generation and prevents a late
	// keepalive from extending it.
	if !keeperStopped {
		revokeErr := l.provider.revokeUntil(l.id, deadline)
		if revokeErr == nil {
			l.finishRelease(dilock.AlreadyNotOwned)
		} else {
			l.finishReleaseWithError(dilock.ReleaseUnknown, revokeErr)
		}
		return
	}

	// Serialize cleanup after the keeper has stopped so no renewal can race
	// past Unlock/Revoke and extend the lease after release begins. A concurrent
	// explicit Renew is bounded by the same deadline; if it does not finish,
	// revoke is attempted without waiting past the budget.
	if !l.lockOperationsUntil(workDeadline) {
		revokeErr := l.provider.revokeUntil(l.id, deadline)
		if revokeErr == nil {
			l.finishRelease(dilock.AlreadyNotOwned)
		} else {
			l.finishReleaseWithError(dilock.ReleaseUnknown, revokeErr)
		}
		return
	}
	defer l.opMu.Unlock()
	var ownershipKnown, owned bool
	if len(key) > 0 {
		if remaining, ok := remainingCleanup(workDeadline); ok {
			checkCtx, cancel := context.WithTimeout(context.Background(), remaining)
			observed, checkErr := l.provider.client.Get(checkCtx, string(key))
			cancel()
			if checkErr == nil && observed != nil {
				switch {
				case observed.Count == 0 && len(observed.Kvs) == 0:
					ownershipKnown = true
				case observed.Count == 1 && len(observed.Kvs) == 1:
					ownershipKnown = true
					owned = string(observed.Kvs[0].Key) == string(key) && observed.Kvs[0].Lease == int64(l.id)
				}
			}
		}
	}
	var unlockErr error
	if len(key) == 0 {
		unlockErr = dilock.ErrProtocolMismatch
	} else if ownershipKnown && !owned {
		// An absent or foreign exact key is positive evidence that this lease
		// no longer owns the native lock. Do not send an acknowledged no-op
		// Unlock and misreport it as a successful removal.
		unlockErr = dilock.ErrLeaseLost
	} else if remaining, ok := remainingCleanup(workDeadline); !ok {
		unlockErr = context.DeadlineExceeded
	} else {
		unlockCtx, cancel := context.WithTimeout(context.Background(), remaining)
		unlockErr = l.provider.locks.Unlock(unlockCtx, key)
		cancel()
	}
	revokeErr := l.provider.revokeUntil(l.id, deadline)
	outcome := dilock.ReleaseUnknown
	switch {
	case ownershipKnown && !owned:
		outcome = dilock.AlreadyNotOwned
	case owned && unlockErr == nil && revokeErr == nil:
		outcome = dilock.ReleasedOwner
	case owned && revokeErr == nil:
		outcome = dilock.AlreadyNotOwned
	}
	var releaseErr error
	if outcome == dilock.ReleaseUnknown {
		releaseErr = errors.Join(unlockErr, revokeErr)
	}
	l.finishReleaseWithError(outcome, releaseErr)
}

func (l *Lease) finishRelease(outcome dilock.ReleaseOutcome) {
	l.finishReleaseWithError(outcome, nil)
}

func (l *Lease) finishReleaseWithError(outcome dilock.ReleaseOutcome, releaseErr error) {
	l.mu.Lock()
	if l.releaseFinished {
		l.mu.Unlock()
		return
	}
	l.releaseFinished = true
	if l.state == dilock.Releasing {
		if outcome == dilock.ReleaseUnknown {
			l.state = dilock.ReleasedUnknown
		} else {
			l.state = dilock.Released
		}
	}
	l.releaseOutcome = outcome
	l.releaseErr = releaseErr
	done := l.releaseDone
	l.mu.Unlock()
	l.provider.removeLease(l)
	close(done)
}

func (l *Lease) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return fmt.Sprintf("etcd lease %d", l.id)
}

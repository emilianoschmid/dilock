package redis

import (
	"context"
	"sync"
	"time"

	"example.invalid/dilock/core"
)

// Lease is one acquisition generation. Do not copy it after first use. All
// mutating operations serialize; Lost is signaled once and never cancels user work.
type Lease struct {
	provider        *Provider
	key, owner      string
	options         dilock.Options
	ttl             time.Duration
	margin          time.Duration
	opMu            sync.Mutex
	mu              sync.Mutex
	state           dilock.State
	lossReason      string
	deadline        time.Time
	expiry          *time.Timer
	lost            chan struct{}
	lostOnce        sync.Once
	stop            chan struct{}
	stopOnce        sync.Once
	renewDone       chan struct{}
	releaseDone     chan struct{}
	releaseOutcome  dilock.ReleaseOutcome
	releaseStarted  bool
	releaseFinished bool
}

func newLease(p *Provider, key, owner string, options dilock.Options, ttl int64, margin time.Duration, deadline time.Time) *Lease {
	l := &Lease{provider: p, key: key, owner: owner, options: options, ttl: time.Duration(ttl) * time.Millisecond, margin: margin, state: dilock.Held, deadline: deadline, lost: make(chan struct{}), stop: make(chan struct{})}
	// Publish the timer while holding mu: an already elapsed deadline may run
	// the callback before AfterFunc returns, and markLost reads expiry under mu.
	l.mu.Lock()
	l.expiry = time.AfterFunc(time.Until(deadline), func() { l.markLost("conservative_deadline_elapsed") })
	l.mu.Unlock()
	if options.AutoRenew {
		l.renewDone = make(chan struct{})
		go l.renewLoop()
	}
	return l
}

// State returns a monotonic local observation, not authoritative Redis state.
func (l *Lease) State() dilock.State { l.expire(); l.mu.Lock(); defer l.mu.Unlock(); return l.state }

// Lost is closed once when local reliance ends. It does not stop goroutines doing user work.
func (l *Lease) Lost() <-chan struct{} { return l.lost }

// EffectiveLeaseMilliseconds reports server TTL, not remaining local validity.
func (l *Lease) EffectiveLeaseMilliseconds() int64 { return int64(l.ttl / time.Millisecond) }

func (l *Lease) expire() {
	l.mu.Lock()
	expired := l.state == dilock.Held && !time.Now().Before(l.deadline)
	l.mu.Unlock()
	if expired {
		l.markLost("conservative_deadline_elapsed")
	}
}

// LossReason reports the first local reason for loss, or an empty string while held.
func (l *Lease) LossReason() string { l.mu.Lock(); defer l.mu.Unlock(); return l.lossReason }

func (l *Lease) markLost(reason string) {
	l.mu.Lock()
	if l.state == dilock.Held {
		l.state = dilock.Lost
		l.lossReason = reason
		if l.expiry != nil {
			l.expiry.Stop()
		}
		l.lostOnce.Do(func() { close(l.lost) })
	}
	l.mu.Unlock()
}

func (l *Lease) renewLoop() {
	defer close(l.renewDone)
	for {
		l.mu.Lock()
		deadline := l.deadline
		held := l.state == dilock.Held
		l.mu.Unlock()
		if !held {
			return
		}
		wait := l.ttl / 3
		if until := time.Until(deadline); until <= 0 {
			l.markLost("conservative_deadline_elapsed")
			return
		} else if until < wait {
			wait = until
		}
		timer := time.NewTimer(wait)
		select {
		case <-l.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		if renewed, _ := l.renew(context.Background()); !renewed {
			return
		}
	}
}

// Renew performs one owner-checked renewal. Any uncertainty ends local reliance.
func (l *Lease) Renew(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, context.Canceled
	}
	return l.renew(ctx)
}

func (l *Lease) renew(ctx context.Context) (bool, error) {
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
	if err := ctx.Err(); err != nil {
		return false, err
	}
	sent := time.Now()
	budget := l.options.RPCTimeout
	if remain := time.Until(oldDeadline); remain < budget {
		budget = remain
	}
	if budget <= 0 {
		l.markLost("conservative_deadline_elapsed")
		return false, dilock.ErrLeaseLost
	}
	rpcCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	result := l.provider.client.Eval(rpcCtx, string(renewScript), []string{l.key}, l.owner, int64(l.ttl/time.Millisecond))
	ch := make(chan struct {
		n   int64
		err error
	}, 1)
	go func() {
		n, err := result.Int64()
		ch <- struct {
			n   int64
			err error
		}{n, err}
	}()
	var reply struct {
		n   int64
		err error
	}
	select {
	case reply = <-ch:
	case <-rpcCtx.Done():
		l.markLost("renewal_unconfirmed")
		l.provider.trackCleanup(func() { <-ch; l.provider.cleanupOwner(l.key, l.owner, l.options.CleanupTimeout) })
		return false, &dilock.OperationError{Kind: dilock.ErrBackendUnavailable, Message: "renewal acknowledgement missing; lease lost", Cause: rpcCtx.Err()}
	}
	if reply.err != nil {
		l.markLost("renewal_unconfirmed")
		return false, &dilock.OperationError{Kind: dilock.ErrBackendUnavailable, Message: "renewal failed; lease lost", Cause: reply.err}
	}
	deadline := sent.Add(l.ttl - l.margin)
	l.mu.Lock()
	stillHeld := l.state == dilock.Held && l.deadline.Equal(oldDeadline)
	l.mu.Unlock()
	if !stillHeld || !time.Now().Before(oldDeadline) || reply.n != 1 || !time.Now().Before(deadline) {
		l.markLost("renewal_not_owned_or_late")
		l.provider.trackCleanup(func() { l.provider.cleanupOwner(l.key, l.owner, l.options.CleanupTimeout) })
		return false, nil
	}
	l.mu.Lock()
	if l.state == dilock.Held && l.deadline.Equal(oldDeadline) {
		l.deadline = deadline
		l.expiry.Reset(time.Until(deadline))
	} else {
		l.mu.Unlock()
		l.markLost("renewal_raced_terminal_state")
		return false, nil
	}
	l.mu.Unlock()
	return true, nil
}

// Release withdraws local permission before owner-checked cleanup. Pass a fresh
// bounded cleanup context; never reuse a canceled acquisition context. Repeated
// calls return the cached outcome. The context bounds waiting and cleanup.
func (l *Lease) Release(ctx context.Context) (dilock.ReleaseOutcome, error) {
	if ctx == nil {
		return dilock.ReleaseUnknown, context.Canceled
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
	l.mu.Unlock()
	l.stopOnce.Do(func() { close(l.stop) })
	if l.renewDone != nil {
		select {
		case <-l.renewDone:
		case <-ctx.Done():
			l.provider.trackCleanup(func() {
				<-l.renewDone
				l.finishRelease(l.provider.cleanupOwner(l.key, l.owner, l.options.CleanupTimeout))
			})
			l.finishRelease(dilock.ReleaseUnknown)
			return dilock.ReleaseUnknown, ctx.Err()
		}
	}
	completed := make(chan dilock.ReleaseOutcome, 1)
	go func() {
		l.opMu.Lock()
		outcome := l.provider.cleanupOwner(l.key, l.owner, l.options.CleanupTimeout)
		l.opMu.Unlock()
		completed <- outcome
	}()
	select {
	case outcome := <-completed:
		l.finishRelease(outcome)
		return outcome, nil
	case <-ctx.Done():
		l.provider.trackCleanup(func() { outcome := <-completed; l.finishRelease(outcome) })
		return dilock.ReleaseUnknown, ctx.Err()
	}
}

func (l *Lease) finishRelease(outcome dilock.ReleaseOutcome) {
	l.mu.Lock()
	if l.releaseDone == nil {
		l.mu.Unlock()
		return
	}
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
	done := l.releaseDone
	l.mu.Unlock()
	l.provider.removeLease(l)
	select {
	case <-done:
	default:
		close(done)
	}
}

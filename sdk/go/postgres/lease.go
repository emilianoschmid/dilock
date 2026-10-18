package postgres

import (
	"context"
	"errors"
	"sync"

	dilock "example.invalid/dilock/core"
	"github.com/jackc/pgx/v5"
)

// Lease represents one PostgreSQL advisory lock and its private session.
type Lease struct {
	provider *Provider
	conn *pgx.Conn
	key int64
	options dilock.Options
	mu sync.Mutex
	state dilock.State
	releaseStarted bool
	releaseOutcome dilock.ReleaseOutcome
	releaseErr error
	releaseDone chan struct{}
}

func (l *Lease) State() dilock.State { l.mu.Lock(); defer l.mu.Unlock(); return l.state }

// Renew checks session liveness only; it cannot extend a PostgreSQL advisory lock.
func (l *Lease) Renew(ctx context.Context) (bool, error) {
	if ctx == nil { return false, dilock.ErrInvalidOptions }
	l.mu.Lock(); if l.state != dilock.Held { l.mu.Unlock(); return false, dilock.ErrLeaseLost }; l.mu.Unlock()
	var one int; if err := l.conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil { l.mu.Lock(); l.state=dilock.Lost; l.mu.Unlock(); return false, &dilock.OperationError{Kind:dilock.ErrBackendUnavailable, Message:"PostgreSQL session liveness failed", Cause:err} }
	return one == 1, nil
}

// Release unlocks on the same physical PostgreSQL session and then closes it.
func (l *Lease) Release(ctx context.Context) (dilock.ReleaseOutcome, error) {
	if ctx == nil { return dilock.ReleaseUnknown, dilock.ErrInvalidOptions }
	l.mu.Lock(); if l.releaseStarted { done:=l.releaseDone; l.mu.Unlock(); select { case <-done: l.mu.Lock(); defer l.mu.Unlock(); return l.releaseOutcome,l.releaseErr; case <-ctx.Done(): return dilock.ReleaseUnknown,ctx.Err() } }; l.releaseStarted=true; l.releaseDone=make(chan struct{}); l.mu.Unlock()
	var released bool; err := l.conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1::bigint)", l.key).Scan(&released); closeErr := l.conn.Close(ctx)
	outcome := dilock.ReleaseUnknown; var resultErr error
	if err == nil && closeErr == nil { if released { outcome=dilock.ReleasedOwner } else { outcome=dilock.AlreadyNotOwned } } else { resultErr=errors.Join(err,closeErr,lilockReleaseUnknown()); l.provider.observe(outcome,"release_unconfirmed") }
	l.provider.remove(l)
	l.mu.Lock(); if outcome == dilock.ReleaseUnknown { l.state=dilock.ReleasedUnknown } else { l.state=dilock.Released }; l.releaseOutcome=outcome; l.releaseErr=resultErr; close(l.releaseDone); l.mu.Unlock()
	return outcome,resultErr
}

func lilockReleaseUnknown() error { return dilock.ErrReleaseUnknown }

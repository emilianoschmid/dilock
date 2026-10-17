package consul

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	dilock "example.invalid/dilock/core"
)

// Lease represents one unique Consul session and its confirmed KV ownership.
type Lease struct {
	provider *Provider
	key, session string
	index uint64
	ttl, margin time.Duration
	deadline time.Time
	options dilock.Options

	mu sync.Mutex
	opMu sync.Mutex
	state dilock.State
	releaseStarted bool
	releaseDone chan struct{}
	releaseOutcome dilock.ReleaseOutcome
	releaseErr error
	renewTimer *time.Timer
}

func (l *Lease) activate() {
	if l.options.AutoRenew { l.renewTimer = time.AfterFunc(l.ttl/3, func(){ ctx,cancel:=context.WithTimeout(context.Background(),l.options.RPCTimeout); defer cancel(); _,_ = l.Renew(ctx); l.mu.Lock(); if l.state==dilock.Held { l.activate() }; l.mu.Unlock() }) }
}

// State returns the local conservative lease observation.
func (l *Lease) State() dilock.State { l.mu.Lock(); defer l.mu.Unlock(); if l.state==dilock.Held&&!time.Now().Before(l.deadline){l.state=dilock.Lost}; return l.state }

// Fence returns the verified epoch-scoped Consul LockIndex.
func (l *Lease) Fence() Fence { return Fence{Epoch:l.provider.options.Epoch,LockIndex:l.index} }

// Renew renews the same session and verifies this exact session and LockIndex.
func (l *Lease) Renew(ctx context.Context) (bool,error) {
	if ctx==nil{return false,dilock.ErrInvalidOptions}
	l.opMu.Lock(); defer l.opMu.Unlock()
	l.mu.Lock(); if l.state!=dilock.Held||!time.Now().Before(l.deadline){l.state=dilock.Lost;l.mu.Unlock();return false,dilock.ErrLeaseLost}; old:=l.deadline; l.mu.Unlock()
	sent:=time.Now(); if err:=l.provider.renewSession(ctx,l.session,l.options.RPCTimeout); err!=nil{l.markLost();return false,err}
	owner,index,err:=l.provider.owner(ctx,l.key,l.options.RPCTimeout); if err!=nil||owner!=l.session||index!=l.index{l.markLost();if err!=nil{return false,err};return false,dilock.ErrLeaseLost}
	deadline:=sent.Add(l.ttl-l.margin); l.mu.Lock(); defer l.mu.Unlock(); if l.state!=dilock.Held||!l.deadline.Equal(old)||!time.Now().Before(old)||!time.Now().Before(deadline){l.state=dilock.Lost;return false,dilock.ErrLeaseLost}; l.deadline=deadline; return true,nil
}

func (l *Lease) markLost(){l.mu.Lock();if l.state==dilock.Held{l.state=dilock.Lost};if l.renewTimer!=nil{l.renewTimer.Stop()};l.mu.Unlock()}

// Release removes only this session's ownership and destroys its session.
func (l *Lease) Release(ctx context.Context)(dilock.ReleaseOutcome,error){
	if ctx==nil{return dilock.ReleaseUnknown,dilock.ErrInvalidOptions}
	l.mu.Lock();if l.releaseStarted{done:=l.releaseDone;l.mu.Unlock();select{case <-done:l.mu.Lock();defer l.mu.Unlock();return l.releaseOutcome,l.releaseErr;case <-ctx.Done():return dilock.ReleaseUnknown,ctx.Err()}}
	l.releaseStarted=true;l.releaseDone=make(chan struct{});if l.state==dilock.Held{l.state=dilock.Releasing};if l.renewTimer!=nil{l.renewTimer.Stop()};done:=l.releaseDone;l.mu.Unlock()
	go l.completeRelease()
	select{case <-done:l.mu.Lock();defer l.mu.Unlock();return l.releaseOutcome,l.releaseErr;case <-ctx.Done():return dilock.ReleaseUnknown,ctx.Err()}
}

func (l *Lease) completeRelease(){
	ctx,cancel:=context.WithTimeout(context.Background(),l.options.CleanupTimeout)
	l.opMu.Lock()
	defer l.opMu.Unlock()
	value,err:=l.provider.request(ctx,http.MethodPut,"v1/kv/"+l.key,url.Values{"release":[]string{l.session}},[]byte{},l.options.CleanupTimeout,true)
	outcome:=dilock.ReleaseUnknown
	if err==nil{if released,ok:=value.(bool);ok&&released{outcome=dilock.ReleasedOwner}else if ok&&!released{owner,_,readErr:=l.provider.owner(ctx,l.key,l.options.CleanupTimeout);if readErr==nil&&owner!=l.session{outcome=dilock.AlreadyNotOwned}}}
	cancel()
	destroyCtx,destroyCancel:=context.WithTimeout(context.Background(),l.options.CleanupTimeout)
	destroyValue,destroyErr:=l.provider.request(destroyCtx,http.MethodPut,"v1/session/destroy/"+url.PathEscape(l.session),nil,nil,l.options.CleanupTimeout,true)
	destroyCancel()
	destroyed,ok:=destroyValue.(bool)
	if destroyErr!=nil||!ok||!destroyed{outcome=dilock.ReleaseUnknown}
	l.provider.removeLease(l)
	var resultErr error;if outcome==dilock.ReleaseUnknown{resultErr=errors.Join(errReleaseUnknown,err,destroyErr);l.provider.observe(outcome,"release_unconfirmed")}
	l.mu.Lock();if outcome==dilock.ReleaseUnknown{l.state=dilock.ReleasedUnknown}else{l.state=dilock.Released};l.releaseOutcome=outcome;l.releaseErr=resultErr;close(l.releaseDone);l.mu.Unlock()
}

// WithLease scopes one callback under provider defaults and reports unconfirmed cleanup.
func (h *Handle) WithLease(ctx context.Context, work func(*Lease) error) error {
	return h.WithLeaseOptions(ctx,h.provider.options.DefaultAcquisition,work)
}

// WithLeaseOptions uses a complete acquisition policy and preserves callback errors.
func (h *Handle) WithLeaseOptions(ctx context.Context, policy dilock.AcquireOptions, work func(*Lease)error)error{
	if work==nil{return dilock.ErrInvalidOptions};lease,err:=h.Acquire(ctx,policy.Wait,policy.Lease);if err!=nil{return err};workErr:=work(lease);cleanupCtx,cancel:=context.WithTimeout(context.Background(),policy.Lease.CleanupTimeout);outcome,cleanupErr:=lease.Release(cleanupCtx);cancel();if workErr!=nil{if outcome!=dilock.ReleasedOwner{h.provider.observe(outcome,"body_failed")};return workErr};if outcome!=dilock.ReleasedOwner{return &dilock.ScopeReleaseError{Outcome:outcome,Cause:cleanupErr}};return nil
}

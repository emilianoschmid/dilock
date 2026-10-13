package main

import (
	"context"
	"fmt"
	"time"

	"example.invalid/dilock/core"
	"example.invalid/dilock/etcd"
	"example.invalid/dilock/redis"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type lockProvider interface {
	lock(string) (lockHandle, error)
	hello(context.Context) (serverVersion, clientVersion string, capabilities []string, err error)
	close() error
	outstandingCleanup() int
}

type lockHandle interface {
	tryAcquire(context.Context, dilock.Options) (lockLease, error)
	acquire(context.Context, time.Duration, dilock.Options) (lockLease, error)
}

type lockLease interface {
	release(context.Context) (dilock.ReleaseOutcome, error)
	state() dilock.State
	lossReason() string
	renew(context.Context) (bool, error)
	effectiveLeaseMilliseconds() int64
	fence() (epoch, revision string, available bool)
}

type redisBackend struct{ provider *redis.Provider }
type redisHandle struct{ handle *redis.Handle }
type redisLease struct{ lease *redis.Lease }

func (p *redisBackend) lock(resource string) (lockHandle, error) {
	h, err := p.provider.Lock(resource)
	if err != nil {
		return nil, err
	}
	return redisHandle{handle: h}, nil
}
func (p *redisBackend) hello(ctx context.Context) (string, string, []string, error) {
	version, err := p.provider.ServerVersion(ctx)
	return version, "github.com/redis/go-redis/v9@v9.21.0", []string{"redis", "renewal"}, err
}
func (p *redisBackend) close() error            { return p.provider.Close() }
func (p *redisBackend) outstandingCleanup() int { return p.provider.OutstandingCleanup() }
func (h redisHandle) tryAcquire(ctx context.Context, options dilock.Options) (lockLease, error) {
	l, err := h.handle.TryAcquire(ctx, options)
	if l == nil || err != nil {
		return nil, err
	}
	return redisLease{lease: l}, nil
}
func (h redisHandle) acquire(ctx context.Context, wait time.Duration, options dilock.Options) (lockLease, error) {
	l, err := h.handle.Acquire(ctx, wait, options)
	if l == nil || err != nil {
		return nil, err
	}
	return redisLease{lease: l}, nil
}
func (l redisLease) release(ctx context.Context) (dilock.ReleaseOutcome, error) {
	return l.lease.Release(ctx)
}
func (l redisLease) state() dilock.State                     { return l.lease.State() }
func (l redisLease) lossReason() string                      { return l.lease.LossReason() }
func (l redisLease) renew(ctx context.Context) (bool, error) { return l.lease.Renew(ctx) }
func (l redisLease) effectiveLeaseMilliseconds() int64       { return l.lease.EffectiveLeaseMilliseconds() }
func (l redisLease) fence() (string, string, bool)           { return "", "", false }

type etcdBackend struct {
	provider *etcd.Provider
	client   *clientv3.Client
	endpoint string
}
type etcdHandle struct{ handle *etcd.Handle }
type etcdLease struct{ lease *etcd.Lease }

func (p *etcdBackend) lock(resource string) (lockHandle, error) {
	h, err := p.provider.Lock(resource)
	if err != nil {
		return nil, err
	}
	return etcdHandle{handle: h}, nil
}
func (p *etcdBackend) hello(ctx context.Context) (string, string, []string, error) {
	status, err := p.client.Status(ctx, p.endpoint)
	if err != nil {
		return "", "go.etcd.io/etcd/client/v3@v3.7.2", nil, err
	}
	return status.Version, "go.etcd.io/etcd/client/v3@v3.7.2", []string{"etcd-native-lock", "renewal", "fencing"}, nil
}
func (p *etcdBackend) close() error {
	err := p.provider.Close()
	if clientErr := p.client.Close(); err == nil {
		err = clientErr
	}
	return err
}
func (*etcdBackend) outstandingCleanup() int { return 0 }
func (h etcdHandle) tryAcquire(ctx context.Context, options dilock.Options) (lockLease, error) {
	l, err := h.handle.TryAcquire(ctx, options)
	if l == nil || err != nil {
		return nil, err
	}
	return etcdLease{lease: l}, nil
}
func (h etcdHandle) acquire(ctx context.Context, wait time.Duration, options dilock.Options) (lockLease, error) {
	l, err := h.handle.Acquire(ctx, wait, options)
	if l == nil || err != nil {
		return nil, err
	}
	return etcdLease{lease: l}, nil
}
func (l etcdLease) release(ctx context.Context) (dilock.ReleaseOutcome, error) {
	return l.lease.Release(ctx)
}
func (l etcdLease) state() dilock.State                     { return l.lease.State() }
func (l etcdLease) lossReason() string                      { return l.lease.LossReason() }
func (l etcdLease) renew(ctx context.Context) (bool, error) { return l.lease.Renew(ctx) }
func (l etcdLease) effectiveLeaseMilliseconds() int64       { return l.lease.EffectiveLeaseMilliseconds() }
func (l etcdLease) fence() (string, string, bool) {
	fence, ok := l.lease.Fence()
	if !ok {
		return "", "", false
	}
	return fence.Epoch, fmt.Sprint(fence.CreateRevision), true
}

func openBackend(ctx context.Context, cfg config) (lockProvider, error) {
	switch cfg.Backend {
	case "", "redis":
		provider, err := redis.OpenProvider(ctx, cfg.Endpoint, redis.ProviderOptions{Namespace: cfg.Namespace, Epoch: cfg.Epoch})
		if err != nil {
			return nil, err
		}
		return &redisBackend{provider: provider}, nil
	case "etcd":
		client, err := clientv3.New(clientv3.Config{Endpoints: []string{cfg.Endpoint}, DialTimeout: 2 * time.Second})
		if err != nil {
			return nil, err
		}
		locks, err := etcd.NewGRPCLockRPC(client.ActiveConnection())
		if err != nil {
			_ = client.Close()
			return nil, err
		}
		provider, err := etcd.NewProvider(client, locks, etcd.ProviderOptions{Namespace: cfg.Namespace, Epoch: cfg.Epoch})
		if err != nil {
			_ = client.Close()
			return nil, err
		}
		return &etcdBackend{provider: provider, client: client, endpoint: cfg.Endpoint}, nil
	default:
		return nil, fmt.Errorf("unsupported backend %q", cfg.Backend)
	}
}

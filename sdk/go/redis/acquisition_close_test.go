package redis

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"example.invalid/dilock/core"
	redisclient "github.com/redis/go-redis/v9"
)

type closeDuringAcquireClient struct {
	provider *Provider
	mu       sync.Mutex
	evalCall int
}

func (*closeDuringAcquireClient) Get(ctx context.Context, key string) *redisclient.StringCmd {
	cmd := redisclient.NewStringCmd(ctx, "GET", key)
	cmd.SetVal(`{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"test-epoch","fixed_lease_ms":null}`)
	return cmd
}

func (*closeDuringAcquireClient) Type(ctx context.Context, key string) *redisclient.StatusCmd {
	return redisclient.NewStatusCmd(ctx, "TYPE", key)
}

func (c *closeDuringAcquireClient) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redisclient.BoolCmd {
	cmd := redisclient.NewBoolCmd(ctx, "SET", key, value, "NX", "PX", expiration)
	cmd.SetVal(true)
	c.provider.mu.Lock()
	c.provider.closed = true
	c.provider.mu.Unlock()
	return cmd
}

func (c *closeDuringAcquireClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redisclient.Cmd {
	c.mu.Lock()
	c.evalCall++
	c.mu.Unlock()
	cmd := redisclient.NewCmd(ctx, "EVAL", script, keys, args)
	cmd.SetErr(errors.New("cleanup acknowledgement unavailable"))
	return cmd
}

func TestAcquisitionUnknownWhenProviderClosesAndOwnerCleanupIsUnconfirmed(t *testing.T) {
	client := &closeDuringAcquireClient{}
	provider, err := NewProvider(client, ProviderOptions{Namespace: "test", Epoch: "test-epoch"})
	if err != nil {
		t.Fatal(err)
	}
	client.provider = provider
	handle, err := provider.Lock("resource")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := handle.TryAcquire(context.Background(), dilock.Options{
		LeaseDuration: time.Second, RPCTimeout: 100 * time.Millisecond, CleanupTimeout: 100 * time.Millisecond,
	})
	if lease != nil || !errors.Is(err, dilock.ErrAcquisitionUnknown) {
		t.Fatalf("TryAcquire = %v, %v; want unknown acquisition after unconfirmed cleanup", lease, err)
	}
	deadline := time.Now().Add(time.Second)
	for provider.OutstandingCleanup() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if provider.OutstandingCleanup() != 0 {
		t.Fatal("owner cleanup retry did not finish")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.evalCall < 2 {
		t.Fatalf("owner-checked cleanup calls = %d; want initial cleanup and tracked retry", client.evalCall)
	}
}

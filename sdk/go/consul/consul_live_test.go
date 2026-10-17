package consul

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	dilock "example.invalid/dilock/core"
)

func TestLiveConsulContentionRenewalAndLockIndex(t *testing.T) {
	endpoint := os.Getenv("DILOCK_CONSUL_ENDPOINT")
	namespace := os.Getenv("DILOCK_CONSUL_NAMESPACE")
	epoch := os.Getenv("DILOCK_CONSUL_EPOCH")
	if endpoint == "" || namespace == "" || epoch == "" {
		t.Skip("set DILOCK_CONSUL_ENDPOINT, DILOCK_CONSUL_NAMESPACE and DILOCK_CONSUL_EPOCH")
	}

	ctx := context.Background()
	provider, err := NewProvider(ctx, endpoint, ProviderOptions{Namespace: namespace, Epoch: epoch})
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()

	handle, err := provider.Lock(fmt.Sprintf("go/live/%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	options := dilock.Options{
		LeaseDuration:  10 * time.Second,
		RPCTimeout:     2 * time.Second,
		CleanupTimeout: 2 * time.Second,
		RequireFencing: true,
	}
	first, err := handle.TryAcquire(ctx, options)
	if err != nil || first == nil {
		t.Fatalf("first acquisition = %v, %v", first, err)
	}
	firstFence := first.Fence()
	if firstFence.Epoch != epoch || firstFence.LockIndex == 0 {
		t.Fatalf("unexpected first fence: %#v", firstFence)
	}
	if contended, err := handle.TryAcquire(ctx, options); err != nil || contended != nil {
		t.Fatalf("contending acquisition = %v, %v; want acknowledged contention", contended, err)
	}
	if renewed, err := first.Renew(ctx); err != nil || !renewed {
		t.Fatalf("renewal = %v, %v", renewed, err)
	}
	if outcome, err := first.Release(ctx); err != nil || outcome != dilock.ReleasedOwner {
		t.Fatalf("first release = %v, %v", outcome, err)
	}

	next, err := handle.TryAcquire(ctx, options)
	if err != nil || next == nil {
		t.Fatalf("successor acquisition = %v, %v", next, err)
	}
	defer next.Release(context.Background())
	if fence := next.Fence(); fence.Epoch != epoch || fence.LockIndex <= firstFence.LockIndex {
		t.Fatalf("successor fence %#v did not advance from %#v", fence, firstFence)
	}
	if outcome, err := next.Release(ctx); err != nil || outcome != dilock.ReleasedOwner {
		t.Fatalf("successor release = %v, %v", outcome, err)
	}
}

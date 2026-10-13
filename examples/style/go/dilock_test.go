package style

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLifecycle(t *testing.T) {
	store := NewStore()
	provider, err := NewProvider(store)
	if err != nil {
		t.Fatal(err)
	}
	resource, err := provider.Lock("example/resource")
	if err != nil {
		t.Fatal(err)
	}
	first, err := resource.TryAcquire(context.Background())
	if err != nil || first == nil {
		t.Fatal("first", err)
	}
	busy, err := resource.TryAcquire(context.Background())
	if err != nil || busy != nil {
		t.Fatal("expected contention")
	}
	// Test-only expiry: never expose unconditional delete in the public SDK.
	store.mu.Lock()
	delete(store.owners, "example/resource")
	store.mu.Unlock()
	second, err := resource.TryAcquire(context.Background())
	if err != nil || second == nil {
		t.Fatal("second", err)
	}
	if outcome, err := first.Release(context.Background()); err != nil || outcome != AlreadyNotOwned {
		t.Fatal("stale release")
	}
	if busy, _ := resource.TryAcquire(context.Background()); busy != nil {
		t.Fatal("stale owner removed successor")
	}
	if outcome, _ := second.Release(context.Background()); outcome != Released {
		t.Fatal("release")
	}
	if outcome, _ := second.Release(context.Background()); outcome != AlreadyNotOwned {
		t.Fatal("idempotence")
	}
	func() {
		held, err := resource.TryAcquire(context.Background())
		if err != nil || held == nil {
			t.Fatal("scope acquire")
		}
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := held.Release(cleanup); err != nil {
				t.Error(err)
			}
		}()
	}()
	final, _ := resource.TryAcquire(context.Background())
	if final == nil {
		t.Fatal("defer failed")
	}
	_, _ = final.Release(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resource.TryAcquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation")
	}
	if _, err := provider.Lock(""); err == nil {
		t.Fatal("validation")
	}
}

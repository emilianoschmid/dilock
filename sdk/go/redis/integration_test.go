package redis

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"example.invalid/dilock/core"
)

// TestRedisOwnerCheckedLifecycle uses only an explicitly configured authority
// and a unique resource. Namespace metadata must already exist and match.
func TestRedisOwnerCheckedLifecycle(t *testing.T) {
	endpoint := os.Getenv("DILOCK_REDIS_URL")
	if endpoint == "" {
		t.Skip("DILOCK_REDIS_URL is unset; real Redis integration not run")
	}
	namespace, epoch := os.Getenv("DILOCK_NAMESPACE"), os.Getenv("DILOCK_EPOCH")
	if namespace == "" || epoch == "" {
		t.Fatal("DILOCK_NAMESPACE and DILOCK_EPOCH are required for real Redis integration")
	}
	defaults := dilock.AcquireOptions{Wait: 2 * time.Second, Lease: dilock.Options{
		LeaseDuration: 3 * time.Second, RPCTimeout: 250 * time.Millisecond,
		CleanupTimeout: time.Second,
	}}
	provider, err := OpenProvider(context.Background(), endpoint, ProviderOptions{
		Namespace: namespace, Epoch: epoch, DefaultAcquisition: defaults,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	resource := "go-integration/" + time.Now().UTC().Format("20060102T150405.000000000")
	h, err := provider.Lock(resource)
	if err != nil {
		t.Fatal(err)
	}
	o := dilock.Options{LeaseDuration: 500 * time.Millisecond, RPCTimeout: 100 * time.Millisecond, CleanupTimeout: 500 * time.Millisecond}
	first, err := h.TryAcquire(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil {
		t.Fatal("first acquisition unexpectedly busy")
	}
	second, err := h.TryAcquire(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if second != nil {
		t.Fatal("second acquisition entered while first held the key")
	}
	time.Sleep(530 * time.Millisecond)
	second, err = h.TryAcquire(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if second == nil {
		t.Fatal("acquisition remained busy after lease expiry")
	}
	lateCtx, cancel := context.WithTimeout(context.Background(), o.CleanupTimeout)
	defer cancel()
	outcome, err := first.Release(lateCtx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != dilock.AlreadyNotOwned {
		t.Fatalf("late first release = %v, want AlreadyNotOwned", outcome)
	}
	third, err := h.TryAcquire(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if third != nil {
		t.Fatal("late old-owner release removed successor")
	}
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), o.CleanupTimeout)
	defer releaseCancel()
	if _, err := second.Release(releaseCtx); err != nil {
		t.Fatal(err)
	}
	third, err = h.TryAcquire(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if third == nil {
		t.Fatal("acquisition remained busy after acknowledged release")
	}
	releaseThirdCtx, releaseThirdCancel := context.WithTimeout(context.Background(), o.CleanupTimeout)
	defer releaseThirdCancel()
	if _, err := third.Release(releaseThirdCtx); err != nil {
		t.Fatal(err)
	}

	daily, err := provider.Lock("go-quickstart/daily/" + time.Now().UTC().Format("150405.000000000"))
	if err != nil {
		t.Fatal(err)
	}
	weekly, err := provider.Lock("go-quickstart/weekly/" + time.Now().UTC().Format("150405.000000000"))
	if err != nil {
		t.Fatal(err)
	}
	if err := daily.WithLease(context.Background(), func(first *Lease) error {
		if first.EffectiveLeaseMilliseconds() != 3000 {
			t.Errorf("default effective TTL = %d", first.EffectiveLeaseMilliseconds())
		}
		return weekly.WithLease(context.Background(), func(second *Lease) error {
			if second.EffectiveLeaseMilliseconds() != 3000 {
				t.Errorf("second default effective TTL = %d", second.EffectiveLeaseMilliseconds())
			}
			if first.provider != second.provider {
				t.Error("two handles did not reuse the same provider")
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("nested WithLease: %v", err)
	}
	defaultLease, err := daily.AcquireDefault(context.Background())
	if err != nil {
		t.Fatalf("AcquireDefault: %v", err)
	}
	if outcome, err := defaultLease.Release(context.Background()); err != nil || outcome == dilock.ReleaseUnknown {
		t.Fatalf("AcquireDefault release = %v, %v", outcome, err)
	}
	bodyFailure := errors.New("primary body error")
	if err := weekly.WithLease(context.Background(), func(*Lease) error { return bodyFailure }); !errors.Is(err, bodyFailure) {
		t.Fatalf("WithLease returned %v; want primary callback error", err)
	}
}

func TestRedisWrongTypeContentionIsProtocolMismatchAndUntouched(t *testing.T) {
	endpoint := os.Getenv("DILOCK_REDIS_URL")
	if endpoint == "" {
		t.Skip("DILOCK_REDIS_URL is unset; real Redis integration not run")
	}
	namespace, epoch := os.Getenv("DILOCK_NAMESPACE"), os.Getenv("DILOCK_EPOCH")
	if namespace == "" || epoch == "" {
		t.Fatal("DILOCK_NAMESPACE and DILOCK_EPOCH are required for real Redis integration")
	}
	provider, err := OpenProvider(context.Background(), endpoint, ProviderOptions{Namespace: namespace, Epoch: epoch})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	h, err := provider.Lock("go-wrong-type/" + time.Now().UTC().Format("20060102T150405.000000000"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := provider.owned.RPush(ctx, h.key, "preserve-me").Err(); err != nil {
		t.Fatal(err)
	}
	_, err = h.TryAcquire(ctx, dilock.Options{LeaseDuration: time.Second, RPCTimeout: 100 * time.Millisecond, CleanupTimeout: time.Second})
	if !errors.Is(err, dilock.ErrProtocolMismatch) {
		t.Fatalf("TryAcquire error = %v, want ErrProtocolMismatch", err)
	}
	items, err := provider.owned.LRange(ctx, h.key, 0, -1).Result()
	if err != nil || len(items) != 1 || items[0] != "preserve-me" {
		t.Fatalf("wrong-type key after classification = %q, %v; want original list", items, err)
	}
}

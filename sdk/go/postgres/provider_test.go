package postgres

import (
	"context"
	"errors"
	"math"
	"os"
	"testing"
	"time"

	dilock "example.invalid/dilock/core"
)

func TestAdvisoryKeySignedDigestWord(t *testing.T) {
	for _, tc := range []struct {
		name, id string
		want     int64
	}{
		{"zero", "0000000000000000" + zeros48(), 0},
		{"max", "7fffffffffffffff" + zeros48(), math.MaxInt64},
		{"min", "8000000000000000" + zeros48(), math.MinInt64},
		{"minus one", "ffffffffffffffff" + zeros48(), -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := AdvisoryKey(tc.id)
			if err != nil || got != tc.want {
				t.Fatalf("got %d,%v want %d", got, err, tc.want)
			}
		})
	}
}
func zeros48() string { return "000000000000000000000000000000000000000000000000" }

func TestPostgresLiveContentionAndHandoff(t *testing.T) {
	dsn := os.Getenv("DILOCK_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DILOCK_POSTGRES_DSN is not set")
	}
	ctx := context.Background()
	options := ProviderOptions{
		Namespace:      "ci-postgres-go",
		Epoch:          "ci-postgres-go-epoch",
		RPCTimeout:     500 * time.Millisecond,
		CleanupTimeout: 500 * time.Millisecond,
		PollInterval:   10 * time.Millisecond,
		DefaultAcquisition: dilock.AcquireOptions{
			Wait:  250 * time.Millisecond,
			Lease: dilock.Options{LeaseDuration: 5 * time.Second, RPCTimeout: 500 * time.Millisecond, CleanupTimeout: 500 * time.Millisecond},
		},
	}
	if err := ProvisionNamespace(ctx, dsn, options); err != nil {
		t.Fatal(err)
	}
	first, err := NewProvider(ctx, dsn, options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewProvider(ctx, dsn, options)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	defer second.Close()

	firstLock, err := first.Lock("go-provider")
	if err != nil {
		t.Fatal(err)
	}
	secondLock, err := second.Lock("go-provider")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := firstLock.TryAcquire(ctx, options.DefaultAcquisition.Lease)
	if err != nil || lease == nil {
		t.Fatalf("first acquire: lease=%v err=%v", lease, err)
	}
	if ok, err := lease.Renew(ctx); err != nil || !ok {
		t.Fatalf("renew: ok=%v err=%v", ok, err)
	}
	if contender, err := secondLock.TryAcquire(ctx, options.DefaultAcquisition.Lease); err != nil || contender != nil {
		t.Fatalf("contention: lease=%v err=%v", contender, err)
	}
	different, err := second.Lock("go-different-resource")
	if err != nil {
		t.Fatal(err)
	}
	differentLease, err := different.TryAcquire(ctx, options.DefaultAcquisition.Lease)
	if err != nil || differentLease == nil {
		t.Fatalf("different resource: lease=%v err=%v", differentLease, err)
	}
	if outcome, err := differentLease.Release(ctx); err != nil || outcome != dilock.ReleasedOwner {
		t.Fatalf("different release: outcome=%v err=%v", outcome, err)
	}
	if _, err := secondLock.Acquire(ctx, 150*time.Millisecond, options.DefaultAcquisition.Lease); !errors.Is(err, dilock.ErrWaitTimeout) {
		t.Fatalf("timeout: %v", err)
	}
	if _, err := secondLock.TryAcquire(ctx, dilock.Options{LeaseDuration: 5 * time.Second, RPCTimeout: 500 * time.Millisecond, CleanupTimeout: 500 * time.Millisecond, RequireFencing: true}); !errors.Is(err, dilock.ErrUnsupported) {
		t.Fatalf("fencing capability: %v", err)
	}
	if outcome, err := lease.Release(ctx); err != nil || outcome != dilock.ReleasedOwner {
		t.Fatalf("release: outcome=%v err=%v", outcome, err)
	}
	handoff, err := secondLock.TryAcquire(ctx, options.DefaultAcquisition.Lease)
	if err != nil || handoff == nil {
		t.Fatalf("handoff: lease=%v err=%v", handoff, err)
	}
	if outcome, err := handoff.Release(ctx); err != nil || outcome != dilock.ReleasedOwner {
		t.Fatalf("handoff release: outcome=%v err=%v", outcome, err)
	}
}

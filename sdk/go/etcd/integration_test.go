package etcd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"example.invalid/dilock/core"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdNativeLockLifecycle(t *testing.T) {
	endpoint := os.Getenv("DILOCK_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("DILOCK_ETCD_ENDPOINT is unset; real etcd integration not run")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	namespace := os.Getenv("DILOCK_ETCD_NAMESPACE")
	if namespace == "" {
		namespace = "go-etcd-" + hex.EncodeToString(random[:])
	}
	epoch := "local-functional-test-v1"
	metadataKey := "/dilock/namespaces/" + namespace + "/meta"
	metadata, _ := json.Marshal(map[string]any{
		"protocol_major": 1, "identity_scheme": identityScheme,
		"profile": profile, "coordination_epoch": epoch, "fixed_lease_ms": nil,
	})
	_, err = client.Txn(ctx).If(clientv3.Compare(clientv3.Version(metadataKey), "=", 0)).Then(clientv3.OpPut(metadataKey, string(metadata))).Commit()
	if err != nil {
		t.Fatal(err)
	}
	locks, err := NewGRPCLockRPC(client.ActiveConnection())
	if err != nil {
		t.Fatal(err)
	}
	defaults := dilock.AcquireOptions{Wait: 2 * time.Second, Lease: dilock.Options{
		LeaseDuration: 3 * time.Second, RPCTimeout: 500 * time.Millisecond,
		CleanupTimeout: time.Second, RequireFencing: true,
	}}
	provider, err := NewProvider(client, locks, ProviderOptions{
		Namespace: namespace, Epoch: epoch, DefaultAcquisition: defaults,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	handle, err := provider.Lock("queue/test")
	if err != nil {
		t.Fatal(err)
	}
	opts := dilock.Options{LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second, AutoRenew: true, RequireFencing: true}
	first, err := handle.TryAcquire(ctx, opts)
	if err != nil || first == nil {
		t.Fatalf("first TryAcquire = %v, %v", first, err)
	}
	firstFence, ok := first.Fence()
	if !ok || firstFence.Epoch != epoch || firstFence.CreateRevision <= 0 {
		t.Fatalf("first fencing token = %+v, verified=%t", firstFence, ok)
	}
	resourceRun := "/dilock/test-resource-fence/" + namespace + "/" + hex.EncodeToString(random[:])
	fenceKey := resourceRun + "/current"
	resourceKey := resourceRun + "/value"
	firstFenceValue := firstFence.Epoch + ":" + strconv.FormatInt(firstFence.CreateRevision, 10)
	registered, err := client.Txn(ctx).If(clientv3.Compare(clientv3.Version(fenceKey), "=", 0)).Then(clientv3.OpPut(fenceKey, firstFenceValue)).Commit()
	if err != nil || !registered.Succeeded {
		t.Fatalf("register first resource fence = %v, %v", registered, err)
	}
	busy, err := handle.TryAcquire(ctx, opts)
	if err != nil || busy != nil {
		t.Fatalf("competing TryAcquire = %v, %v; want busy", busy, err)
	}

	// A queued native Lock must stay alive beyond its original one-second TTL.
	queuedCtx, queuedCancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer queuedCancel()
	queued := make(chan struct {
		lease *Lease
		err   error
	}, 1)
	go func() {
		lease, acquireErr := handle.Acquire(queuedCtx, 3500*time.Millisecond, opts)
		queued <- struct {
			lease *Lease
			err   error
		}{lease, acquireErr}
	}()
	time.Sleep(first.ttl + 250*time.Millisecond)
	if first.State() != dilock.Held {
		t.Fatalf("auto-renewed first lease state = %v, want Held", first.State())
	}
	if _, err := first.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-queued:
		if result.err != nil || result.lease == nil {
			t.Fatalf("queued Acquire = %v, %v", result.lease, result.err)
		}
		secondFence, verified := result.lease.Fence()
		if !verified || secondFence.CreateRevision <= firstFence.CreateRevision {
			t.Fatalf("successor fence = %+v, verified=%t; predecessor=%+v", secondFence, verified, firstFence)
		}
		if result.lease.id == first.id {
			t.Fatal("independent acquisitions shared one server lease")
		}
		secondFenceValue := secondFence.Epoch + ":" + strconv.FormatInt(secondFence.CreateRevision, 10)
		advanced, advanceErr := client.Txn(ctx).If(clientv3.Compare(clientv3.Value(fenceKey), "=", firstFenceValue)).Then(clientv3.OpPut(fenceKey, secondFenceValue)).Commit()
		if advanceErr != nil || !advanced.Succeeded {
			t.Fatalf("register successor resource fence = %v, %v", advanced, advanceErr)
		}
		staleWrite, staleErr := client.Txn(ctx).If(clientv3.Compare(clientv3.Value(fenceKey), "=", firstFenceValue)).Then(clientv3.OpPut(resourceKey, "stale-write")).Commit()
		if staleErr != nil || staleWrite.Succeeded {
			t.Fatalf("stale resource write = %v, %v; want atomic fence rejection", staleWrite, staleErr)
		}
		if _, err := result.lease.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	case <-queuedCtx.Done():
		t.Fatal("native waiter did not acquire after predecessor release")
	}

	// Cancellation while queued must cancel the native Lock and revoke its own lease.
	holder, err := handle.TryAcquire(ctx, opts)
	if err != nil || holder == nil {
		t.Fatalf("holder TryAcquire = %v, %v", holder, err)
	}
	cancelCtx, cancelWait := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() {
		_, acquireErr := handle.Acquire(cancelCtx, 5*time.Second, opts)
		canceled <- acquireErr
	}()
	if !waitForLockCount(t, client, handle.name+"/", 2, 2*time.Second) {
		cancelWait()
		t.Fatal("canceled waiter never appeared in the native queue")
	}
	cancelWait()
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Acquire error = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled native waiter did not return")
	}
	if !waitForLockCount(t, client, handle.name+"/", 1, 2*time.Second) {
		t.Fatal("canceled waiter left an etcd lock-queue key")
	}
	if _, err := holder.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	daily, err := provider.Lock("go-quickstart/daily/" + hex.EncodeToString(random[:]))
	if err != nil {
		t.Fatal(err)
	}
	weekly, err := provider.Lock("go-quickstart/weekly/" + hex.EncodeToString(random[:]))
	if err != nil {
		t.Fatal(err)
	}
	if err := daily.WithLease(ctx, func(first *Lease) error {
		if first.EffectiveLeaseMilliseconds() != 3000 {
			t.Errorf("default effective TTL = %d", first.EffectiveLeaseMilliseconds())
		}
		return weekly.WithLease(ctx, func(second *Lease) error {
			if second.EffectiveLeaseMilliseconds() != 3000 {
				t.Errorf("second default effective TTL = %d", second.EffectiveLeaseMilliseconds())
			}
			f1, ok1 := first.Fence()
			f2, ok2 := second.Fence()
			if !ok1 || !ok2 || f1.Epoch != epoch || f2.Epoch != epoch {
				t.Errorf("default fences = %+v/%t and %+v/%t", f1, ok1, f2, ok2)
			}
			if first.provider != second.provider {
				t.Error("two handles did not reuse the same provider")
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("nested WithLease: %v", err)
	}
	defaultLease, err := daily.AcquireDefault(ctx)
	if err != nil {
		t.Fatalf("AcquireDefault: %v", err)
	}
	if outcome, err := defaultLease.Release(ctx); err != nil || outcome == dilock.ReleaseUnknown {
		t.Fatalf("AcquireDefault release = %v, %v", outcome, err)
	}
	bodyFailure := errors.New("primary body error")
	if err := weekly.WithLease(ctx, func(*Lease) error { return bodyFailure }); !errors.Is(err, bodyFailure) {
		t.Fatalf("WithLease returned %v; want primary callback error", err)
	}
}

func waitForLockCount(t *testing.T, client *clientv3.Client, prefix string, count int64, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
		cancel()
		if err == nil && resp.Count == count {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		t.Logf("final lock queue Range: %v", err)
		return false
	}
	t.Logf("lock queue %s has %d entries, wanted %d", prefix, resp.Count, count)
	return false
}

func TestLockNameGoldenVectors(t *testing.T) {
	path := "../../../spec/vectors/identity.json"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Valid []struct {
			ID           string `json:"id"`
			Namespace    string `json:"namespace"`
			Resource     string `json:"resource"`
			EtcdLockName string `json:"etcd_lock_name"`
		} `json:"valid"`
	}
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors.Valid) != 13 {
		t.Fatalf("loaded %d valid vectors, want 13", len(vectors.Valid))
	}
	for _, vector := range vectors.Valid {
		t.Run(vector.ID, func(t *testing.T) {
			got, err := LockName(vector.Namespace, vector.Resource)
			if err != nil {
				t.Fatal(err)
			}
			if got != vector.EtcdLockName {
				t.Fatalf("LockName = %q, want %q", got, vector.EtcdLockName)
			}
		})
	}
}

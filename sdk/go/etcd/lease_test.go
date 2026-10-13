package etcd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"example.invalid/dilock/core"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type fakeClient struct {
	mu             sync.Mutex
	leaseID        clientv3.LeaseID
	leaseTTL       int64
	lockKey        []byte
	lockLease      int64
	revision       int64
	revokeErr      error
	keepalive      error
	metadata       []byte
	revokeCall     int
	onOwnershipGet func()
	grantStarted   chan struct{}
	blockGrant     bool
	probeRemaining time.Duration
}

func (c *fakeClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if key == "/dilock/namespaces/test/meta" {
		return &clientv3.GetResponse{Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte(key), Value: c.metadata}}}, nil
	}
	if len(opts) > 0 {
		if deadline, ok := ctx.Deadline(); ok {
			c.probeRemaining = time.Until(deadline)
		}
		for _, op := range c.allKeys() {
			if len(op.Key) >= len(key) && string(op.Key[:len(key)]) == key {
				return &clientv3.GetResponse{Count: 1, Kvs: []*mvccpb.KeyValue{op}}, nil
			}
		}
		return &clientv3.GetResponse{}, nil
	}
	if string(c.lockKey) == key {
		if c.onOwnershipGet != nil {
			c.onOwnershipGet()
		}
		return &clientv3.GetResponse{Count: 1, Kvs: []*mvccpb.KeyValue{{Key: c.lockKey, Lease: c.lockLease, CreateRevision: c.revision}}}, nil
	}
	return &clientv3.GetResponse{}, nil
}

func (c *fakeClient) allKeys() []*mvccpb.KeyValue {
	if len(c.lockKey) == 0 {
		return nil
	}
	return []*mvccpb.KeyValue{{Key: c.lockKey, Lease: c.lockLease, CreateRevision: c.revision}}
}

func (c *fakeClient) Grant(ctx context.Context, _ int64) (*clientv3.LeaseGrantResponse, error) {
	if c.blockGrant {
		close(c.grantStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leaseID++
	return &clientv3.LeaseGrantResponse{ID: c.leaseID, TTL: c.leaseTTL}, nil
}

func (c *fakeClient) KeepAliveOnce(context.Context, clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	if c.keepalive != nil {
		return nil, c.keepalive
	}
	return &clientv3.LeaseKeepAliveResponse{TTL: c.leaseTTL}, nil
}

func (c *fakeClient) Revoke(_ context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.revokeCall++
	if c.revokeErr != nil {
		return nil, c.revokeErr
	}
	if c.lockLease == int64(id) {
		c.lockKey = nil
	}
	return &clientv3.LeaseRevokeResponse{}, nil
}

type fakeLocks struct {
	mu      sync.Mutex
	key     []byte
	lockErr error
	unlock  error
	seenKey []byte
	unlockN int
	block   bool
	started chan struct{}
}

func (l *fakeLocks) Lock(ctx context.Context, _ string, id clientv3.LeaseID) ([]byte, error) {
	if l.block {
		close(l.started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if l.lockErr != nil {
		return nil, l.lockErr
	}
	return append([]byte(nil), l.key...), nil
}
func (l *fakeLocks) Unlock(_ context.Context, key []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.unlockN++
	l.seenKey = append([]byte(nil), key...)
	return l.unlock
}

func newFakeProvider(t *testing.T, client *fakeClient, locks *fakeLocks) *Provider {
	t.Helper()
	provider, err := NewProvider(client, locks, ProviderOptions{Namespace: "test", Epoch: "test-epoch"})
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func validMetadata() []byte {
	return []byte(`{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"etcd-native-lock-v1","coordination_epoch":"test-epoch","fixed_lease_ms":null}`)
}

func TestNamespaceMetadataRejectsDuplicateTopLevelFields(t *testing.T) {
	for _, metadata := range [][]byte{
		[]byte(`{"protocol_major":1,"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"etcd-native-lock-v1","coordination_epoch":"test-epoch","fixed_lease_ms":null}`),
		[]byte(`{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"etcd-native-lock-v1","coordination_epoch":"wrong","coordination_epoch":"test-epoch","fixed_lease_ms":null}`),
	} {
		provider := newFakeProvider(t, &fakeClient{metadata: metadata}, &fakeLocks{})
		if err := provider.metadata(context.Background(), time.Second); !errors.Is(err, dilock.ErrProtocolMismatch) {
			t.Fatalf("metadata(%s) = %v; want ErrProtocolMismatch", metadata, err)
		}
	}
}

func TestNamespaceMetadataAllowsNestedExtensionFields(t *testing.T) {
	metadata := []byte(`{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"etcd-native-lock-v1","coordination_epoch":"test-epoch","fixed_lease_ms":null,"extension":{"field":1,"field":2}}`)
	provider := newFakeProvider(t, &fakeClient{metadata: metadata}, &fakeLocks{})
	if err := provider.metadata(context.Background(), time.Second); err != nil {
		t.Fatalf("metadata with nested extension fields = %v; want success", err)
	}
}

func TestQueuedOwnerProbeUsesConfiguredRpcBudget(t *testing.T) {
	client := &fakeClient{metadata: validMetadata()}
	provider := newFakeProvider(t, client, &fakeLocks{})
	h, err := provider.Lock("resource")
	if err != nil {
		t.Fatal(err)
	}
	const budget = 40 * time.Millisecond

	if _, err := h.hasQueuedOwner(context.Background(), budget); err != nil {
		t.Fatalf("hasQueuedOwner = %v", err)
	}
	if client.probeRemaining <= 0 || client.probeRemaining > budget {
		t.Fatalf("contention probe deadline has %s remaining; want a positive budget no greater than %s", client.probeRemaining, budget)
	}
}

func TestReleaseUsesExactOpaqueKeyAndIsIdempotent(t *testing.T) {
	client := &fakeClient{leaseTTL: 2, metadata: validMetadata(), revision: 11}
	locks := &fakeLocks{key: []byte("/opaque/native/key/009f")}
	client.lockKey = append([]byte(nil), locks.key...)
	client.lockLease = 1
	provider := newFakeProvider(t, client, locks)
	h, err := provider.Lock("resource")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := h.TryAcquire(context.Background(), dilock.Options{LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second})
	if err != nil || lease == nil {
		t.Fatalf("TryAcquire = %v, %v", lease, err)
	}
	outcome, err := lease.Release(context.Background())
	if err != nil || outcome != dilock.ReleasedOwner {
		t.Fatalf("Release = %v, %v", outcome, err)
	}
	second, err := lease.Release(context.Background())
	if err != nil || second != outcome {
		t.Fatalf("repeat Release = %v, %v", second, err)
	}
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if string(locks.seenKey) != string(locks.key) || locks.unlockN != 1 {
		t.Fatalf("Unlock saw %q, calls=%d; want exact key %q once", locks.seenKey, locks.unlockN, locks.key)
	}
	if client.revokeCall != 1 {
		t.Fatalf("revoke calls = %d, want one", client.revokeCall)
	}
}

func TestReleaseRevokesWhenKeepaliveDoesNotStopWithinCleanupBudget(t *testing.T) {
	key := []byte("/opaque/stuck-keeper")
	client := &fakeClient{leaseID: 7, leaseTTL: 2, lockKey: key, lockLease: 7}
	locks := &fakeLocks{key: key}
	provider := newFakeProvider(t, client, locks)
	keeperDone := make(chan struct{}) // Simulates a client call that ignores cancellation.
	l := &Lease{
		provider: provider,
		id:       7,
		options: dilock.Options{
			CleanupTimeout: 200 * time.Millisecond,
			RPCTimeout:     20 * time.Millisecond,
		},
		state:       dilock.Held,
		key:         append([]byte(nil), key...),
		keepCancel:  func() {},
		keepDone:    keeperDone,
		lost:        make(chan struct{}),
		releaseDone: nil,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	outcome, err := l.Release(ctx)
	if err != nil || outcome != dilock.AlreadyNotOwned {
		t.Fatalf("Release with unresponsive keeper = %v, %v; want acknowledged revoke without Unlock", outcome, err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.revokeCall != 1 || len(client.lockKey) != 0 {
		t.Fatalf("revoke calls/key = %d/%q; want one revoke removing the lease key", client.revokeCall, client.lockKey)
	}
}

func TestCanceledReleaseWaiterDoesNotCancelCleanup(t *testing.T) {
	key := []byte("/opaque/canceled-release-waiter")
	client := &fakeClient{leaseID: 9, leaseTTL: 2, lockKey: key, lockLease: 9}
	provider := newFakeProvider(t, client, &fakeLocks{key: key})
	lease := &Lease{
		provider: provider,
		id:       9,
		options: dilock.Options{
			CleanupTimeout: 120 * time.Millisecond,
			RPCTimeout:     20 * time.Millisecond,
		},
		state:      dilock.Held,
		key:        append([]byte(nil), key...),
		keepCancel: func() {},
		keepDone:   make(chan struct{}), // Simulates a keeper RPC that ignores cancellation.
		lost:       make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	outcome, err := lease.Release(ctx)
	if !errors.Is(err, context.Canceled) || outcome != dilock.ReleaseUnknown {
		t.Fatalf("Release with canceled caller = %v, %v; want caller cancellation and unknown pending outcome", outcome, err)
	}
	if elapsed := time.Since(started); elapsed > 40*time.Millisecond {
		t.Fatalf("Release waited %s after caller cancellation; want prompt return while cleanup continues", elapsed)
	}

	select {
	case <-lease.releaseDone:
	case <-time.After(time.Second):
		t.Fatal("background cleanup did not finish after caller cancellation")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.revokeCall != 1 || len(client.lockKey) != 0 {
		t.Fatalf("background cleanup revoke/key = %d/%q; want one acknowledged revoke", client.revokeCall, client.lockKey)
	}
	if lease.releaseOutcome != dilock.AlreadyNotOwned || lease.releaseErr != nil {
		t.Fatalf("cached cleanup outcome = %v, %v; want acknowledged revoke", lease.releaseOutcome, lease.releaseErr)
	}
}

func TestReleaseClassifiesUnlockAndRevokeAcknowledgements(t *testing.T) {
	t.Run("revoke only", func(t *testing.T) {
		client := &fakeClient{leaseTTL: 2, metadata: validMetadata(), revision: 13}
		locks := &fakeLocks{key: []byte("/opaque/revoke-only"), unlock: errors.New("unlock not acknowledged")}
		client.lockKey, client.lockLease = append([]byte(nil), locks.key...), 1
		provider := newFakeProvider(t, client, locks)
		h, _ := provider.Lock("resource")
		lease, err := h.TryAcquire(context.Background(), dilock.Options{LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second})
		if err != nil || lease == nil {
			t.Fatalf("TryAcquire = %v, %v", lease, err)
		}
		outcome, err := lease.Release(context.Background())
		if err != nil || outcome != dilock.AlreadyNotOwned {
			t.Fatalf("Release = %v, %v; want AlreadyNotOwned", outcome, err)
		}
	})

	t.Run("unlock only", func(t *testing.T) {
		client := &fakeClient{leaseTTL: 2, metadata: validMetadata(), revision: 13, revokeErr: errors.New("revoke not acknowledged")}
		locks := &fakeLocks{key: []byte("/opaque/unlock-only")}
		client.lockKey, client.lockLease = append([]byte(nil), locks.key...), 1
		provider := newFakeProvider(t, client, locks)
		h, _ := provider.Lock("resource")
		lease, err := h.TryAcquire(context.Background(), dilock.Options{LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second})
		if err != nil || lease == nil {
			t.Fatalf("TryAcquire = %v, %v", lease, err)
		}
		outcome, err := lease.Release(context.Background())
		if outcome != dilock.ReleaseUnknown || !errors.Is(err, dilock.ErrBackendUnavailable) {
			t.Fatalf("Release = %v, %v; want ReleaseUnknown because Revoke was not acknowledged", outcome, err)
		}
	})
}

func TestReleaseAfterEtcdLeaseExpirySkipsAcknowledgedNoopUnlock(t *testing.T) {
	key := []byte("/opaque/expired-owner")
	client := &fakeClient{leaseTTL: 2, metadata: validMetadata(), revision: 14, lockKey: append([]byte(nil), key...), lockLease: 1}
	locks := &fakeLocks{key: key}
	provider := newFakeProvider(t, client, locks)
	h, _ := provider.Lock("resource")
	lease, err := h.TryAcquire(context.Background(), dilock.Options{LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second})
	if err != nil || lease == nil {
		t.Fatalf("TryAcquire = %v, %v", lease, err)
	}
	client.mu.Lock()
	client.lockKey = nil // etcd expired the lease and removed its native lock key.
	client.mu.Unlock()

	outcome, err := lease.Release(context.Background())
	if err != nil || outcome != dilock.AlreadyNotOwned {
		t.Fatalf("Release after expiry = %v, %v; want AlreadyNotOwned", outcome, err)
	}
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if locks.unlockN != 0 {
		t.Fatalf("Unlock calls = %d; expired key must skip native Unlock", locks.unlockN)
	}
}

func TestAbandonRevokesWhenKeepaliveDoesNotStopWithinCleanupBudget(t *testing.T) {
	key := []byte("/opaque/abandoned-waiter")
	client := &fakeClient{leaseID: 8, leaseTTL: 2, lockKey: key, lockLease: 8}
	locks := &fakeLocks{key: key}
	provider := newFakeProvider(t, client, locks)
	l := &Lease{
		provider: provider,
		id:       8,
		options: dilock.Options{
			CleanupTimeout: 200 * time.Millisecond,
			RPCTimeout:     20 * time.Millisecond,
		},
		state:      dilock.Held,
		key:        append([]byte(nil), key...),
		keepCancel: func() {},
		keepDone:   make(chan struct{}), // Simulates an RPC that ignores cancellation.
		lost:       make(chan struct{}),
	}
	if err := l.abandon(); err != nil {
		t.Fatalf("abandon with unresponsive keeper = %v; want acknowledged revoke", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.revokeCall != 1 || len(client.lockKey) != 0 {
		t.Fatalf("revoke calls/key = %d/%q; want one revoke removing the lock key", client.revokeCall, client.lockKey)
	}
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if locks.unlockN != 0 {
		t.Fatalf("Unlock calls = %d; revoke must clean the attempt lease without waiting for Unlock", locks.unlockN)
	}
}

func TestCleanupDeadlineSharesRemainingBudgetAcrossSteps(t *testing.T) {
	start := time.Unix(100, 0)
	deadline := start.Add(2 * time.Second)

	if remaining, ok := remainingCleanupAt(deadline, start); !ok || remaining != 2*time.Second {
		t.Fatalf("initial cleanup budget = (%v, %t), want (2s, true)", remaining, ok)
	}
	if remaining, ok := remainingCleanupAt(deadline, start.Add(1500*time.Millisecond)); !ok || remaining != 500*time.Millisecond {
		t.Fatalf("budget after keeper wait = (%v, %t), want (500ms, true)", remaining, ok)
	}
	if remaining, ok := remainingCleanupAt(deadline, deadline); ok || remaining != 0 {
		t.Fatalf("expired cleanup budget = (%v, %t), want (0, false)", remaining, ok)
	}

	lease := &Lease{}
	lease.opMu.Lock()
	if lease.lockOperationsUntil(time.Now()) {
		lease.opMu.Unlock()
		t.Fatal("cleanup acquired a renewal lock after its shared deadline")
	}
	lease.opMu.Unlock()
}

func TestReleaseUnknownWhenUnlockAndRevokeAreUnacknowledged(t *testing.T) {
	client := &fakeClient{leaseTTL: 2, metadata: validMetadata(), revision: 13, revokeErr: errors.New("revoke acknowledgement lost")}
	locks := &fakeLocks{key: []byte("/opaque/key"), unlock: errors.New("unlock acknowledgement lost")}
	client.lockKey, client.lockLease = append([]byte(nil), locks.key...), 1
	provider := newFakeProvider(t, client, locks)
	h, _ := provider.Lock("resource")
	lease, err := h.TryAcquire(context.Background(), dilock.Options{LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second})
	if err != nil || lease == nil {
		t.Fatalf("TryAcquire = %v, %v", lease, err)
	}
	// Keep the exact owner key present while both cleanup acknowledgements fail.
	// Without positive evidence of absence, release must stay Unknown.
	outcome, err := lease.Release(context.Background())
	if outcome != dilock.ReleaseUnknown || err == nil {
		t.Fatalf("Release = %v, %v; want unknown outcome and error", outcome, err)
	}
	second, secondErr := lease.Release(context.Background())
	if second != dilock.ReleaseUnknown || secondErr != nil {
		t.Fatalf("repeated Release = %v, %v; want cached unknown outcome", second, secondErr)
	}
	locks.mu.Lock()
	unlockCalls := locks.unlockN
	locks.mu.Unlock()
	client.mu.Lock()
	revokeCalls := client.revokeCall
	client.mu.Unlock()
	if unlockCalls != 1 || revokeCalls != 1 {
		t.Fatalf("repeated release retried cleanup: Unlock=%d Revoke=%d; want one each", unlockCalls, revokeCalls)
	}
}

func TestAcquirePreservesUnknownVerificationTimeoutBeforeOverallWaitDeadline(t *testing.T) {
	client := &fakeClient{leaseTTL: 2, metadata: validMetadata(), revokeErr: errors.New("revoke acknowledgement lost")}
	locks := &fakeLocks{lockErr: context.DeadlineExceeded}
	provider := newFakeProvider(t, client, locks)
	h, err := provider.Lock("resource")
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	lease, err := h.Acquire(context.Background(), 5*time.Second, dilock.Options{
		LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second,
	})
	if lease != nil || !errors.Is(err, dilock.ErrAcquisitionUnknown) || errors.Is(err, dilock.ErrWaitTimeout) {
		t.Fatalf("Acquire = %v, %v; want unknown outcome, not wait timeout", lease, err)
	}
	if time.Since(started) >= time.Second {
		t.Fatalf("Acquire took %v; inner RPC failure must return before its five-second wait budget", time.Since(started))
	}
	if client.revokeCall != 1 {
		t.Fatalf("revoke calls = %d; want one failed cleanup attempt", client.revokeCall)
	}
}

func TestOverallWaitClassificationDoesNotHideUnknownOutcome(t *testing.T) {
	caller := context.Background()
	deadline := time.Now().Add(-time.Millisecond)
	if overallWaitExpired(context.DeadlineExceeded, caller, deadline) != true {
		t.Fatal("expired overall wait was not classified as timeout")
	}
	unknown := acquisitionUnknown("verification and cleanup were not acknowledged", context.DeadlineExceeded)
	if overallWaitExpired(unknown, caller, deadline) {
		t.Fatal("uncertain acquisition was hidden as ordinary wait timeout")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if overallWaitExpired(context.DeadlineExceeded, canceled, deadline) {
		t.Fatal("caller cancellation was hidden as ordinary wait timeout")
	}
}

func TestCancellationDuringLeaseGrantIsUnknown(t *testing.T) {
	client := &fakeClient{
		leaseTTL: 3, metadata: validMetadata(), grantStarted: make(chan struct{}), blockGrant: true,
	}
	provider := newFakeProvider(t, client, &fakeLocks{})
	h, err := provider.Lock("cancel-grant")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, acquireErr := h.TryAcquire(ctx, dilock.Options{
			LeaseDuration: 2 * time.Second, RPCTimeout: time.Second, CleanupTimeout: time.Second,
		})
		result <- acquireErr
	}()
	select {
	case <-client.grantStarted:
	case <-time.After(time.Second):
		t.Fatal("LeaseGrant did not start")
	}
	cancel()
	select {
	case acquireErr := <-result:
		if !errors.Is(acquireErr, dilock.ErrAcquisitionUnknown) || !errors.Is(acquireErr, context.Canceled) {
			t.Fatalf("TryAcquire after dispatched Grant cancellation = %v; want acquisition unknown while preserving the cancellation cause", acquireErr)
		}
	case <-time.After(time.Second):
		t.Fatal("TryAcquire did not finish after Grant cancellation")
	}
}

func TestInvalidGrantWithUnconfirmedRevokeIsAcquisitionUnknown(t *testing.T) {
	cleanupFailure := errors.New("revoke acknowledgement unavailable")
	client := &fakeClient{leaseTTL: 0, metadata: validMetadata(), revokeErr: cleanupFailure}
	provider := newFakeProvider(t, client, &fakeLocks{})
	h, err := provider.Lock("invalid-grant")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := h.TryAcquire(context.Background(), dilock.Options{
		LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second,
	})
	if lease != nil || !errors.Is(err, dilock.ErrAcquisitionUnknown) || !errors.Is(err, dilock.ErrProtocolMismatch) || !errors.Is(err, cleanupFailure) {
		t.Fatalf("TryAcquire with invalid grant and unconfirmed revoke = %v, %v; want unknown outcome preserving protocol and cleanup errors", lease, err)
	}
	if client.revokeCall != 1 {
		t.Fatalf("lease revoke calls = %d, want one cleanup attempt", client.revokeCall)
	}
}

func TestExactKeyRangeRejectsWrongLease(t *testing.T) {
	client := &fakeClient{leaseTTL: 2, metadata: validMetadata(), revision: 15}
	locks := &fakeLocks{key: []byte("/opaque/wrong-lease")}
	client.lockKey, client.lockLease = append([]byte(nil), locks.key...), 999
	provider := newFakeProvider(t, client, locks)
	h, _ := provider.Lock("resource")
	lease, err := h.TryAcquire(context.Background(), dilock.Options{LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second})
	if lease != nil || !errors.Is(err, dilock.ErrProtocolMismatch) {
		t.Fatalf("TryAcquire = %v, %v; want protocol mismatch", lease, err)
	}
	if client.revokeCall != 1 {
		t.Fatalf("revoke calls = %d, want cleanup of the unverified lease", client.revokeCall)
	}
}

func TestProviderCloseCancelsInFlightWaiter(t *testing.T) {
	client := &fakeClient{leaseTTL: 2, metadata: validMetadata(), revision: 17}
	locks := &fakeLocks{block: true, started: make(chan struct{})}
	provider := newFakeProvider(t, client, locks)
	h, _ := provider.Lock("resource")
	result := make(chan error, 1)
	go func() {
		_, err := h.TryAcquire(context.Background(), dilock.Options{LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second})
		result <- err
	}()
	select {
	case <-locks.started:
	case <-time.After(time.Second):
		t.Fatal("native Lock waiter did not start")
	}
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("acquisition after Provider.Close = %v; want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Provider.Close did not cancel and join the in-flight waiter")
	}
	if client.revokeCall != 1 {
		t.Fatalf("revoke calls after provider shutdown = %d, want one", client.revokeCall)
	}
}

func TestProviderCloseAfterVerifiedLockReturnsAfterCleanup(t *testing.T) {
	client := &fakeClient{leaseTTL: 2, metadata: validMetadata(), revision: 19}
	locks := &fakeLocks{key: []byte("/opaque/verified/key")}
	client.lockKey, client.lockLease = append([]byte(nil), locks.key...), 1
	provider := newFakeProvider(t, client, locks)
	client.onOwnershipGet = func() {
		provider.mu.Lock()
		provider.closed = true
		provider.mu.Unlock()
	}
	h, err := provider.Lock("resource")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := h.TryAcquire(context.Background(), dilock.Options{LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second})
	if lease != nil || !errors.Is(err, dilock.ErrProviderClosed) || errors.Is(err, dilock.ErrAcquisitionUnknown) {
		t.Fatalf("TryAcquire = %v, %v; want provider closed after confirmed owner cleanup", lease, err)
	}
	if client.revokeCall != 1 {
		t.Fatalf("lease revoke calls = %d, want one confirmed cleanup", client.revokeCall)
	}
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if locks.unlockN != 1 || string(locks.seenKey) != string(locks.key) {
		t.Fatalf("Unlock calls/key = %d/%q; want exact key %q once", locks.unlockN, locks.seenKey, locks.key)
	}
}

func TestProviderCloseAfterVerifiedLockReportsUnknownCleanup(t *testing.T) {
	client := &fakeClient{leaseTTL: 2, metadata: validMetadata(), revision: 23, revokeErr: errors.New("revoke acknowledgement unavailable")}
	locks := &fakeLocks{key: []byte("/opaque/unconfirmed/key")}
	client.lockKey, client.lockLease = append([]byte(nil), locks.key...), 1
	provider := newFakeProvider(t, client, locks)
	client.onOwnershipGet = func() {
		provider.mu.Lock()
		provider.closed = true
		provider.mu.Unlock()
	}
	h, err := provider.Lock("resource")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := h.TryAcquire(context.Background(), dilock.Options{LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second})
	if lease != nil || !errors.Is(err, dilock.ErrAcquisitionUnknown) || !errors.Is(err, dilock.ErrBackendUnavailable) {
		t.Fatalf("TryAcquire = %v, %v; want unknown acquisition and cleanup error", lease, err)
	}
	if client.revokeCall != 1 {
		t.Fatalf("lease revoke calls = %d, want one failed cleanup attempt", client.revokeCall)
	}
}

func TestWithLeasePreservesBodyResultAndRequiresReleasedOutcome(t *testing.T) {
	bodyFailure := errors.New("protected work failed")
	cleanupFailure := errors.New("cleanup acknowledgement unavailable")
	for _, test := range []struct {
		name           string
		bodyErr        error
		observerPanics bool
		unlockErr      error
		revokeErr      error
		wantOutcome    dilock.ReleaseOutcome
		wantErr        error
		wantReason     string
	}{
		{name: "released", wantOutcome: dilock.ReleasedOwner},
		{name: "already not owned", unlockErr: cleanupFailure, wantOutcome: dilock.AlreadyNotOwned, wantErr: dilock.ErrLeaseLost, wantReason: "scope_exit"},
		{name: "unknown", unlockErr: cleanupFailure, revokeErr: cleanupFailure, wantOutcome: dilock.ReleaseUnknown, wantErr: dilock.ErrReleaseUnknown, wantReason: "scope_exit"},
		{name: "body error stays primary", bodyErr: bodyFailure, unlockErr: cleanupFailure, revokeErr: cleanupFailure, wantOutcome: dilock.ReleaseUnknown, wantErr: bodyFailure, wantReason: "body_failed"},
		{name: "throwing observer cannot replace body error", bodyErr: bodyFailure, observerPanics: true, unlockErr: cleanupFailure, revokeErr: cleanupFailure, wantOutcome: dilock.ReleaseUnknown, wantErr: bodyFailure, wantReason: "body_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := []byte("/opaque/scope-cleanup")
			client := &fakeClient{leaseTTL: 2, metadata: validMetadata(), revision: 31, revokeErr: test.revokeErr,
				lockKey: append([]byte(nil), key...), lockLease: 1}
			locks := &fakeLocks{key: append([]byte(nil), key...), unlock: test.unlockErr}
			var observation dilock.CleanupObservation
			observed := 0
			provider, err := NewProvider(client, locks, ProviderOptions{
				Namespace: "test", Epoch: "test-epoch",
				CleanupObserver: func(value dilock.CleanupObservation) {
					observed++
					observation = value
					if test.observerPanics {
						panic("observer failure")
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			h, err := provider.Lock("resource")
			if err != nil {
				t.Fatal(err)
			}
			policy := dilock.AcquireOptions{Wait: time.Second, Lease: dilock.Options{
				LeaseDuration: time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second,
			}}
			err = h.WithLeaseOptions(context.Background(), policy, func(*Lease) error { return test.bodyErr })
			if !errors.Is(err, test.wantErr) || (err == nil) != (test.wantErr == nil) {
				t.Fatalf("WithLeaseOptions error = %v; want %v", err, test.wantErr)
			}
			if test.wantOutcome == dilock.ReleasedOwner {
				if observed != 0 {
					t.Fatalf("cleanup observations = %d; want none", observed)
				}
			} else if observed != 1 || observation.Outcome != test.wantOutcome || observation.Reason != test.wantReason {
				t.Fatalf("cleanup observations = %d, %+v; want one %v observation with reason %q", observed, observation, test.wantOutcome, test.wantReason)
			}
			if err := provider.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

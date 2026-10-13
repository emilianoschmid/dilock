package etcd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	dilock "example.invalid/dilock/core"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// appliedUnlockAckLoss wraps the real native Lock RPC, applies Unlock to etcd,
// and then simulates losing its successful acknowledgement on the client path.
type appliedUnlockAckLoss struct {
	LockRPC
	calls atomic.Int32
}

func (l *appliedUnlockAckLoss) Unlock(ctx context.Context, key []byte) error {
	call := l.calls.Add(1)
	if err := l.LockRPC.Unlock(ctx, key); err != nil {
		return err
	}
	if call == 1 {
		return errors.New("injected lost acknowledgement after etcd applied Unlock")
	}
	return nil
}

// appliedRevokeAckLoss optionally applies Revoke and hides its successful
// acknowledgement. This lets the test distinguish a confirmed revoke from an
// outcome that remains unknown after both cleanup acknowledgements are lost.
type appliedRevokeAckLoss struct {
	Client
	dropAck bool
	calls   atomic.Int32
}

func (c *appliedRevokeAckLoss) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	call := c.calls.Add(1)
	response, err := c.Client.Revoke(ctx, id)
	if err != nil || !c.dropAck || call != 1 {
		return response, err
	}
	return nil, errors.New("injected lost acknowledgement after etcd applied Revoke")
}

func TestLiveAppliedUnlockAckLossClassifiesByRevokeAckAndPreservesSuccessor(t *testing.T) {
	endpoint := os.Getenv("DILOCK_ETCD_UNLOCK_ACK_LOSS_ENDPOINT")
	if endpoint == "" {
		t.Skip("DILOCK_ETCD_UNLOCK_ACK_LOSS_ENDPOINT is unset; live Unlock ACK-loss test not run")
	}
	base, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	namespace := "go-unlock-ack-loss-" + hex.EncodeToString(nonce[:])
	epoch := "go-unlock-ack-loss-v1"
	metadataKey := "/dilock/namespaces/" + namespace + "/meta"
	metadata, err := json.Marshal(map[string]any{
		"protocol_major": 1, "identity_scheme": identityScheme,
		"profile": profile, "coordination_epoch": epoch, "fixed_lease_ms": nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.Txn(ctx).If(clientv3.Compare(clientv3.Version(metadataKey), "=", 0)).Then(clientv3.OpPut(metadataKey, string(metadata))).Commit(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		_, _ = base.Delete(cleanupCtx, metadataKey)
		_ = base.Close()
	})

	rawLocks, err := NewGRPCLockRPC(base.ActiveConnection())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name          string
		dropRevokeAck bool
		want          dilock.ReleaseOutcome
	}{
		{name: "revoke-acknowledged", want: dilock.AlreadyNotOwned},
		{name: "revoke-ack-lost", dropRevokeAck: true, want: dilock.ReleaseUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			lossyClient := &appliedRevokeAckLoss{Client: base, dropAck: test.dropRevokeAck}
			lossyLocks := &appliedUnlockAckLoss{LockRPC: rawLocks}
			provider, err := NewProvider(lossyClient, lossyLocks, ProviderOptions{Namespace: namespace, Epoch: epoch})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = provider.Close() })
			handle, err := provider.Lock("same-resource")
			if err != nil {
				t.Fatal(err)
			}
			options := dilock.Options{LeaseDuration: 4 * time.Second, RPCTimeout: 500 * time.Millisecond, CleanupTimeout: 2 * time.Second, RequireFencing: true}
			owner, err := handle.TryAcquire(ctx, options)
			if err != nil || owner == nil {
				t.Fatalf("owner TryAcquire = %v, %v", owner, err)
			}
			ownerFence, verified := owner.Fence()
			if !verified || ownerFence.CreateRevision <= 0 {
				t.Fatalf("owner fence = %+v verified=%t", ownerFence, verified)
			}
			ownerKey := append([]byte(nil), owner.key...)
			outcome, releaseErr := owner.Release(ctx)
			if outcome != test.want || (outcome == dilock.ReleaseUnknown) != (releaseErr != nil) {
				t.Fatalf("owner Release = %v, %v; want %v and matching error state", outcome, releaseErr, test.want)
			}
			wantState := dilock.Released
			if test.want == dilock.ReleaseUnknown {
				wantState = dilock.ReleasedUnknown
			}
			if owner.State() != wantState {
				t.Fatalf("owner state = %v; want %v", owner.State(), wantState)
			}
			if lossyLocks.calls.Load() != 1 || lossyClient.calls.Load() != 1 {
				t.Fatalf("first cleanup calls Unlock/Revoke = %d/%d; want 1/1", lossyLocks.calls.Load(), lossyClient.calls.Load())
			}

			// The native Unlock was applied, so the same resource is available in
			// either classification case. The new generation has its own lease.
			successor, err := handle.TryAcquire(ctx, options)
			if err != nil || successor == nil {
				t.Fatalf("successor TryAcquire = %v, %v", successor, err)
			}
			successorFence, verified := successor.Fence()
			if !verified || successorFence.CreateRevision <= ownerFence.CreateRevision {
				t.Fatalf("successor fence = %+v verified=%t; owner fence = %+v", successorFence, verified, ownerFence)
			}
			second, secondErr := owner.Release(ctx)
			if second != test.want || secondErr != nil {
				t.Fatalf("repeated owner Release = %v, %v; want cached %v", second, secondErr, test.want)
			}
			if lossyLocks.calls.Load() != 1 || lossyClient.calls.Load() != 1 {
				t.Fatalf("repeated owner Release retried cleanup: Unlock/Revoke=%d/%d", lossyLocks.calls.Load(), lossyClient.calls.Load())
			}
			current, err := base.Get(ctx, string(successor.key))
			if err != nil || current.Count != 1 || current.Kvs[0].Lease != int64(successor.id) {
				t.Fatalf("old owner cleanup affected successor: Range=%v, err=%v", current, err)
			}
			if _, err := successor.Release(ctx); err != nil {
				t.Fatalf("successor Release: %v", err)
			}
			oldKey, err := base.Get(ctx, string(ownerKey))
			if err != nil || oldKey.Count != 0 {
				t.Fatalf("old exact key remains after successor cleanup: Range=%v, err=%v", oldKey, err)
			}
		})
	}
}

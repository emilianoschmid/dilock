// This consumer uses etcd's native FIFO Lock RPC through the public Dilock API.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"dilock"
	"dilock/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	endpoint, err := required("DILOCK_ETCD_ENDPOINT")
	if err != nil {
		return err
	}
	namespace, err := required("DILOCK_NAMESPACE")
	if err != nil {
		return err
	}
	epoch, err := required("DILOCK_EPOCH")
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 2 * time.Second})
	if err != nil {
		return fmt.Errorf("connect etcd client: %w", err)
	}
	defer client.Close()
	locks, err := etcd.NewGRPCLockRPC(client.ActiveConnection())
	if err != nil {
		return fmt.Errorf("create native Lock client: %w", err)
	}
	ownerProvider, err := etcd.NewProvider(client, locks, etcd.ProviderOptions{Namespace: namespace, Epoch: epoch})
	if err != nil {
		return fmt.Errorf("create owner provider: %w", err)
	}
	defer ownerProvider.Close()
	contenderProvider, err := etcd.NewProvider(client, locks, etcd.ProviderOptions{Namespace: namespace, Epoch: epoch})
	if err != nil {
		return fmt.Errorf("create contender provider: %w", err)
	}
	defer contenderProvider.Close()
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	resource := "docs-example/go-etcd/" + hex.EncodeToString(random[:])
	ownerHandle, err := ownerProvider.Lock(resource)
	if err != nil {
		return err
	}
	contenderHandle, err := contenderProvider.Lock(resource)
	if err != nil {
		return err
	}
	opts := dilock.Options{LeaseDuration: 5 * time.Second, RPCTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second, RequireFencing: true}
	owner, err := ownerHandle.Acquire(ctx, 3*time.Second, opts)
	if err != nil {
		return fmt.Errorf("owner acquisition: %w", err)
	}
	defer release(owner)
	fence, verified := owner.Fence()
	if !verified || fence.CreateRevision <= 0 {
		return errors.New("owner fencing token was not verified")
	}
	fmt.Printf("owner: acquired with epoch-scoped create_revision %s/%d\n", fence.Epoch, fence.CreateRevision)
	waitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, err = contenderHandle.Acquire(waitCtx, 250*time.Millisecond, opts)
	cancel()
	if !errors.Is(err, dilock.ErrWaitTimeout) {
		if err == nil {
			return errors.New("contender unexpectedly acquired while owner held the resource")
		}
		return fmt.Errorf("contender wait returned %w, expected ErrWaitTimeout", err)
	}
	fmt.Println("contender: bounded queue wait timed out under contention")
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
	_, err = owner.Release(cleanupCtx)
	cleanupCancel()
	if err != nil {
		return fmt.Errorf("owner release: %w", err)
	}
	successor, err := contenderHandle.Acquire(ctx, 3*time.Second, opts)
	if err != nil {
		return fmt.Errorf("successor acquisition: %w", err)
	}
	defer release(successor)
	next, verified := successor.Fence()
	if !verified || next.Epoch != fence.Epoch || next.CreateRevision <= fence.CreateRevision {
		return errors.New("successor fencing token did not advance within the configured epoch")
	}
	fmt.Printf("contender: acquired after release with create_revision %d\n", next.CreateRevision)
	cleanupCtx, cleanupCancel = context.WithTimeout(context.Background(), time.Second)
	_, err = successor.Release(cleanupCtx)
	cleanupCancel()
	if err != nil {
		return fmt.Errorf("successor release: %w", err)
	}
	return nil
}

func required(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("set %s for the pre-provisioned etcd namespace", name)
	}
	return value, nil
}

func release(lease interface {
	Release(context.Context) (dilock.ReleaseOutcome, error)
}) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := lease.Release(ctx); err != nil {
		log.Printf("best-effort lease cleanup: %v", err)
	}
}

// etcd quickstart: one provider and gRPC channel serve all lock handles.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	dilock "example.invalid/dilock/core"
	"example.invalid/dilock/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	ctx := context.Background()
	mode := "default"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if mode != "default" && mode != "--configured" && mode != "--shared-provider" {
		log.Fatal("Usage: go run . [--configured|--shared-provider]")
	}
	epoch := os.Getenv("DILOCK_EPOCH")
	if epoch == "" {
		log.Fatal("DILOCK_EPOCH is required")
	}
	policy := dilock.DefaultAcquireOptions()
	if mode == "--configured" {
		policy = dilock.AcquireOptions{Wait: 10 * time.Second, Lease: dilock.Options{
			LeaseDuration: 60 * time.Second, RPCTimeout: 2 * time.Second, CleanupTimeout: 2 * time.Second, RequireFencing: true,
		}}
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{env("DILOCK_ETCD_ENDPOINT", "127.0.0.1:2379")}, DialTimeout: 2 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	locks, err := etcd.NewGRPCLockRPC(client.ActiveConnection())
	if err != nil {
		log.Fatal(err)
	}
	provider, err := etcd.NewProvider(client, locks, etcd.ProviderOptions{
		Namespace: env("DILOCK_NAMESPACE", "default"), Epoch: epoch, DefaultAcquisition: policy,
		CleanupObserver: func(o dilock.CleanupObservation) {
			log.Printf("cleanup backend=%s outcome=%d reason=%s", o.Backend, o.Outcome, o.Reason)
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer provider.Close()
	daily, err := provider.Lock("reports/daily")
	if err != nil {
		log.Fatal(err)
	}
	if mode != "--shared-provider" {
		work := func(lease *etcd.Lease) error {
			fence, ok := lease.Fence()
			if !ok {
				return dilock.ErrProtocolMismatch
			}
			fmt.Printf("One etcd resource held; fence epoch %s revision %d.\n", fence.Epoch, fence.CreateRevision)
			return nil
		}
		if mode == "--configured" {
			err = daily.WithLeaseOptions(ctx, policy, work)
		} else {
			err = daily.WithLease(ctx, work)
		}
	} else {
		weekly, lockErr := provider.Lock("reports/weekly")
		if lockErr != nil {
			log.Fatal(lockErr)
		}
		err = daily.WithLease(ctx, func(first *etcd.Lease) error {
			f1, ok1 := first.Fence()
			if !ok1 {
				return dilock.ErrProtocolMismatch
			}
			return weekly.WithLease(ctx, func(second *etcd.Lease) error {
				f2, ok2 := second.Fence()
				if !ok2 {
					return dilock.ErrProtocolMismatch
				}
				fmt.Printf("Two distinct resources share this provider; fence revisions %d and %d.\n", f1.CreateRevision, f2.CreateRevision)
				return nil
			})
		})
	}
	if err != nil {
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

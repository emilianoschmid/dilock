// Redis quickstart: one provider owns the shared client used by all lock handles.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	dilock "example.invalid/dilock/core"
	"example.invalid/dilock/redis"
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
	epoch := os.Getenv("DILOCK_EPOCH") // Stable, operator-approved deployment value.
	if epoch == "" {
		log.Fatal("DILOCK_EPOCH is required")
	}
	policy := dilock.DefaultAcquireOptions()
	if mode == "--configured" {
		policy = dilock.AcquireOptions{Wait: 10 * time.Second, Lease: dilock.Options{
			LeaseDuration: 60 * time.Second, RPCTimeout: 2 * time.Second, CleanupTimeout: 2 * time.Second,
		}}
	}
	provider, err := redis.OpenProvider(ctx, env("DILOCK_REDIS_URL", "redis://127.0.0.1:6379"), redis.ProviderOptions{
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
		var work func(*redis.Lease) error
		work = func(lease *redis.Lease) error {
			fmt.Printf("One Redis resource held; effective TTL %d ms.\n", lease.EffectiveLeaseMilliseconds())
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
		err = daily.WithLease(ctx, func(first *redis.Lease) error {
			return weekly.WithLease(ctx, func(second *redis.Lease) error {
				fmt.Printf("Two distinct resources share this provider; TTLs %d ms and %d ms.\n", first.EffectiveLeaseMilliseconds(), second.EffectiveLeaseMilliseconds())
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

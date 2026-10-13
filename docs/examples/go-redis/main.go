// This consumer demonstrates the public Redis API against an operator-provisioned
// Dilock namespace. It never creates or repairs namespace metadata.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"dilock"
	"dilock/redis"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	endpoint, err := requiredEnvironment("DILOCK_REDIS_URL")
	if err != nil {
		return err
	}
	namespace, err := requiredEnvironment("DILOCK_NAMESPACE")
	if err != nil {
		return err
	}
	epoch, err := requiredEnvironment("DILOCK_EPOCH")
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	providerOptions := redis.ProviderOptions{Namespace: namespace, Epoch: epoch}
	ownerProvider, err := redis.OpenProvider(ctx, endpoint, providerOptions)
	if err != nil {
		return fmt.Errorf("open owner provider: %w", err)
	}
	defer closeProvider(ownerProvider)
	contenderProvider, err := redis.OpenProvider(ctx, endpoint, providerOptions)
	if err != nil {
		return fmt.Errorf("open contender provider: %w", err)
	}
	defer closeProvider(contenderProvider)

	// The unique resource avoids collisions between independent example runs.
	resource := fmt.Sprintf("docs-example/go-redis/%d", time.Now().UnixNano())
	ownerHandle, err := ownerProvider.Lock(resource)
	if err != nil {
		return fmt.Errorf("create owner handle: %w", err)
	}
	contenderHandle, err := contenderProvider.Lock(resource)
	if err != nil {
		return fmt.Errorf("create contender handle: %w", err)
	}

	options := dilock.Options{
		LeaseDuration:  3 * time.Second,
		RPCTimeout:     1 * time.Second,
		CleanupTimeout: 1 * time.Second,
		AutoRenew:      false,
	}
	ownerLease, err := ownerHandle.TryAcquire(ctx, options)
	if err != nil {
		return fmt.Errorf("owner acquisition: %w", err)
	}
	if ownerLease == nil {
		return errors.New("unique example resource was unexpectedly busy")
	}
	defer releaseLease(ownerLease)

	busyLease, err := contenderHandle.TryAcquire(ctx, options)
	if err != nil {
		return fmt.Errorf("contender try-acquire: %w", err)
	}
	if busyLease != nil {
		releaseLease(busyLease)
		return errors.New("contender acquired while the owner lease was active")
	}
	fmt.Println("contender: acknowledged busy")

	_, err = contenderHandle.Acquire(ctx, 250*time.Millisecond, options)
	if !errors.Is(err, dilock.ErrWaitTimeout) {
		if err == nil {
			return errors.New("contender unexpectedly acquired during the bounded wait")
		}
		return fmt.Errorf("contender wait ended with %w, expected ErrWaitTimeout", err)
	}
	fmt.Println("contender: bounded wait ended with ErrWaitTimeout")

	// Redis TTL is a server-side fallback. Expiry withdraws local reliance but
	// cannot stop this process or work already started by it.
	if err := waitContext(ctx, options.LeaseDuration+500*time.Millisecond); err != nil {
		return err
	}
	successorLease, err := contenderHandle.TryAcquire(ctx, options)
	if err != nil {
		return fmt.Errorf("successor acquisition: %w", err)
	}
	if successorLease == nil {
		return errors.New("contender remained busy after the original Redis TTL")
	}
	defer releaseLease(successorLease)
	fmt.Println("contender: acquired after Redis TTL")

	// A late owner-checked release cannot delete a successor with another token.
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), time.Second)
	outcome, err := ownerLease.Release(cleanupCtx)
	cancelCleanup()
	if err != nil {
		return fmt.Errorf("stale owner release: %w", err)
	}
	if outcome != dilock.AlreadyNotOwned {
		return fmt.Errorf("stale owner release returned %v, expected AlreadyNotOwned", outcome)
	}
	fmt.Println("original owner: stale release left successor untouched")

	cleanupCtx, cancelCleanup = context.WithTimeout(context.Background(), time.Second)
	outcome, err = successorLease.Release(cleanupCtx)
	cancelCleanup()
	if err != nil {
		return fmt.Errorf("successor release: %w", err)
	}
	if outcome != dilock.ReleasedOwner {
		return fmt.Errorf("successor release returned %v, expected ReleasedOwner", outcome)
	}
	fmt.Println("contender: successor release acknowledged")
	return nil
}

func requiredEnvironment(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("set %s for the pre-provisioned Redis namespace", name)
	}
	return value, nil
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func releaseLease(lease *redis.Lease) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := lease.Release(ctx); err != nil {
		log.Printf("best-effort lease release: %v", err)
	}
}

func closeProvider(provider *redis.Provider) {
	if err := provider.Close(); err != nil {
		log.Printf("provider close: %v", err)
	}
}

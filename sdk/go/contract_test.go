package dilock

import (
	"errors"
	"testing"
	"time"
)

func TestOptionsValidationCategories(t *testing.T) {
	tests := []struct {
		name    string
		options Options
	}{
		{name: "RPC consumes lease budget", options: Options{LeaseDuration: time.Second, RPCTimeout: 990 * time.Millisecond, CleanupTimeout: time.Second}},
		{name: "negative lease", options: Options{LeaseDuration: -time.Second, RPCTimeout: time.Millisecond, CleanupTimeout: time.Second}},
		{name: "cleanup timeout too large", options: Options{LeaseDuration: time.Second, RPCTimeout: time.Millisecond, CleanupTimeout: 25 * time.Hour}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := test.options.Validate(); !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("Validate() error = %v; want ErrInvalidOptions", err)
			}
			if _, _, _, err := test.options.Normalized(); !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("Normalized() error = %v; want ErrInvalidOptions", err)
			}
		})
	}
}

func TestOptionsNormalizedDefaults(t *testing.T) {
	got, ttl, margin, err := (Options{}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	if got.LeaseDuration != 30*time.Second || got.RPCTimeout != 2*time.Second || got.CleanupTimeout != 2*time.Second {
		t.Fatalf("normalized defaults = %+v", got)
	}
	if ttl != 30_000 || margin < 10*time.Millisecond {
		t.Fatalf("normalized lease budget = ttl %dms, margin %s", ttl, margin)
	}
}

func TestAcquireOptionsNormalizedDefaults(t *testing.T) {
	got, err := (AcquireOptions{}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	if got.Wait != 5*time.Second || got.Lease.LeaseDuration != 30*time.Second ||
		got.Lease.RPCTimeout != 2*time.Second || got.Lease.CleanupTimeout != 2*time.Second {
		t.Fatalf("normalized acquire defaults = %+v", got)
	}
}

func TestAcquireOptionsRejectsInvalidWait(t *testing.T) {
	for _, wait := range []time.Duration{-time.Second, 25 * time.Hour} {
		if _, err := (AcquireOptions{Wait: wait}).Normalized(); !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("wait %s error = %v; want ErrInvalidOptions", wait, err)
		}
	}
}

func TestExplicitAcquisitionPolicyMustBeComplete(t *testing.T) {
	if err := (AcquireOptions{}).ValidateComplete(); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("empty explicit policy error = %v; want ErrInvalidOptions", err)
	}
	if err := DefaultAcquireOptions().ValidateComplete(); err != nil {
		t.Fatalf("complete default policy rejected: %v", err)
	}
}

package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	dilock "example.invalid/dilock/core"
	redisclient "github.com/redis/go-redis/v9"
)

type scopeCleanupRedisClient struct {
	evalOutcome int64
	evalErr     error
}

func (scopeCleanupRedisClient) Get(context.Context, string) *redisclient.StringCmd {
	metadata := `{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"scope-cleanup-test","fixed_lease_ms":null}`
	return redisclient.NewStringResult(metadata, nil)
}
func (scopeCleanupRedisClient) Type(context.Context, string) *redisclient.StatusCmd {
	return redisclient.NewStatusResult("none", nil)
}
func (scopeCleanupRedisClient) SetNX(context.Context, string, interface{}, time.Duration) *redisclient.BoolCmd {
	return redisclient.NewBoolResult(true, nil)
}
func (c scopeCleanupRedisClient) Eval(context.Context, string, []string, ...interface{}) *redisclient.Cmd {
	if c.evalErr != nil {
		return redisclient.NewCmdResult(nil, c.evalErr)
	}
	return redisclient.NewCmdResult(c.evalOutcome, nil)
}

func TestWithLeasePreservesBodyResultWhenCleanupIsNotReleased(t *testing.T) {
	bodyFailure := errors.New("protected work failed")
	for _, test := range []struct {
		name           string
		bodyErr        error
		observerPanics bool
		useDefaults    bool
		client         scopeCleanupRedisClient
		wantOutcome    dilock.ReleaseOutcome
		wantErr        error
		wantReason     string
	}{
		{name: "body error stays primary for unknown", bodyErr: bodyFailure, client: scopeCleanupRedisClient{evalErr: errors.New("cleanup acknowledgement unavailable")}, wantOutcome: dilock.ReleaseUnknown, wantErr: bodyFailure, wantReason: "body_failed"},
		{name: "body error stays primary for already not owned", bodyErr: bodyFailure, client: scopeCleanupRedisClient{evalOutcome: 0}, wantOutcome: dilock.AlreadyNotOwned, wantErr: bodyFailure, wantReason: "body_failed"},
		{name: "throwing observer cannot replace body error", bodyErr: bodyFailure, observerPanics: true, client: scopeCleanupRedisClient{evalErr: errors.New("cleanup acknowledgement unavailable")}, wantOutcome: dilock.ReleaseUnknown, wantErr: bodyFailure, wantReason: "body_failed"},
		{name: "default scope preserves body error", useDefaults: true, bodyErr: bodyFailure, client: scopeCleanupRedisClient{evalErr: errors.New("cleanup acknowledgement unavailable")}, wantOutcome: dilock.ReleaseUnknown, wantErr: bodyFailure, wantReason: "body_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := 0
			var observation dilock.CleanupObservation
			provider, err := NewProvider(test.client, ProviderOptions{
				Namespace: "scope-test", Epoch: "scope-cleanup-test",
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
			if test.useDefaults {
				err = h.WithLease(context.Background(), func(*Lease) error { return test.bodyErr })
			} else {
				err = h.WithLeaseOptions(context.Background(), policy, func(*Lease) error { return test.bodyErr })
			}
			if !errors.Is(err, test.wantErr) || (err == nil) != (test.wantErr == nil) {
				t.Fatalf("WithLeaseOptions error = %v; want %v", err, test.wantErr)
			}
			if observed != 1 || observation.Outcome != test.wantOutcome || observation.Reason != test.wantReason {
				t.Fatalf("cleanup observations = %d, %+v; want one %v observation with reason %q", observed, observation, test.wantOutcome, test.wantReason)
			}
			if err := provider.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWithLeaseRequiresReleasedOutcomeAfterSuccessfulWork(t *testing.T) {
	for _, test := range []struct {
		name       string
		client     scopeCleanupRedisClient
		wantErr    error
		wantResult dilock.ReleaseOutcome
	}{
		{name: "released", client: scopeCleanupRedisClient{evalOutcome: 1}, wantResult: dilock.ReleasedOwner},
		{name: "already not owned", client: scopeCleanupRedisClient{evalOutcome: 0}, wantErr: dilock.ErrLeaseLost, wantResult: dilock.AlreadyNotOwned},
		{name: "unknown", client: scopeCleanupRedisClient{evalErr: errors.New("lost release acknowledgement")}, wantErr: dilock.ErrReleaseUnknown, wantResult: dilock.ReleaseUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			observations := 0
			var observation dilock.CleanupObservation
			provider, err := NewProvider(test.client, ProviderOptions{
				Namespace: "scope-test", Epoch: "scope-cleanup-test",
				CleanupObserver: func(value dilock.CleanupObservation) { observations++; observation = value },
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
			err = h.WithLeaseOptions(context.Background(), policy, func(*Lease) error { return nil })
			if test.wantErr == nil {
				if err != nil {
					t.Fatalf("WithLeaseOptions error = %v; want nil", err)
				}
			} else if !errors.Is(err, test.wantErr) {
				t.Fatalf("WithLeaseOptions error = %v; want errors.Is(_, %v)", err, test.wantErr)
			}
			if test.wantResult == dilock.ReleasedOwner {
				if observations != 0 {
					t.Fatalf("cleanup observations = %d; want none", observations)
				}
			} else if observations != 1 || observation.Outcome != test.wantResult || observation.Reason != "scope_exit" {
				t.Fatalf("cleanup = %d, %+v; want one %v scope_exit observation", observations, observation, test.wantResult)
			}
			if err := provider.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

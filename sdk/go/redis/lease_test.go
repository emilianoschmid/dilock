package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	dilock "example.invalid/dilock/core"
	redisclient "github.com/redis/go-redis/v9"
)

type releaseUnknownClient struct {
	evalCalls atomic.Int32
	started   chan struct{}
	unblock   chan struct{}
}

func (*releaseUnknownClient) Get(context.Context, string) *redisclient.StringCmd {
	return redisclient.NewStringResult(`{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"release-test","fixed_lease_ms":null}`, nil)
}
func (*releaseUnknownClient) Type(context.Context, string) *redisclient.StatusCmd {
	return redisclient.NewStatusResult("none", nil)
}
func (*releaseUnknownClient) SetNX(context.Context, string, interface{}, time.Duration) *redisclient.BoolCmd {
	return redisclient.NewBoolResult(true, nil)
}
func (c *releaseUnknownClient) Eval(context.Context, string, []string, ...interface{}) *redisclient.Cmd {
	c.evalCalls.Add(1)
	if c.started != nil {
		close(c.started)
		<-c.unblock
	}
	return redisclient.NewCmdResult(nil, errors.New("release acknowledgement unavailable"))
}

func TestReleaseUnknownIsCachedAndNotRetried(t *testing.T) {
	client := &releaseUnknownClient{}
	provider, err := NewProvider(client, ProviderOptions{Namespace: "test", Epoch: "release-test"})
	if err != nil {
		t.Fatal(err)
	}
	lease := &Lease{
		provider: provider, key: "resource", owner: "owner",
		options: dilock.Options{CleanupTimeout: time.Second},
		state:   dilock.Held, lost: make(chan struct{}), stop: make(chan struct{}),
	}
	first, err := lease.Release(context.Background())
	if first != dilock.ReleaseUnknown || err != nil {
		t.Fatalf("first Release = %v, %v; want cached unknown without transport error", first, err)
	}
	second, err := lease.Release(context.Background())
	if second != dilock.ReleaseUnknown || err != nil {
		t.Fatalf("repeated Release = %v, %v; want same unknown outcome", second, err)
	}
	if got := client.evalCalls.Load(); got != 1 {
		t.Fatalf("release EVAL calls = %d; want exactly one", got)
	}
}

func TestConcurrentReleaseSharesInFlightUnknown(t *testing.T) {
	client := &releaseUnknownClient{started: make(chan struct{}), unblock: make(chan struct{})}
	provider, err := NewProvider(client, ProviderOptions{Namespace: "test", Epoch: "release-test"})
	if err != nil {
		t.Fatal(err)
	}
	lease := &Lease{
		provider: provider, key: "resource", owner: "owner",
		options: dilock.Options{CleanupTimeout: time.Second},
		state:   dilock.Held, lost: make(chan struct{}), stop: make(chan struct{}),
	}
	results := make(chan dilock.ReleaseOutcome, 2)
	errorsCh := make(chan error, 2)
	go func() { outcome, err := lease.Release(context.Background()); results <- outcome; errorsCh <- err }()
	<-client.started
	go func() { outcome, err := lease.Release(context.Background()); results <- outcome; errorsCh <- err }()
	close(client.unblock)
	for range 2 {
		if outcome, err := <-results, <-errorsCh; outcome != dilock.ReleaseUnknown || err != nil {
			t.Fatalf("concurrent Release = %v, %v; want shared unknown outcome", outcome, err)
		}
	}
	if got := client.evalCalls.Load(); got != 1 {
		t.Fatalf("concurrent release EVAL calls = %d; want exactly one", got)
	}
}

func TestNewLeaseAlreadyElapsedDeadlineDoesNotRaceTimerPublication(t *testing.T) {
	for range 128 {
		lease := newLease(nil, "", "", dilock.Options{}, 1, 0, time.Now().Add(-time.Millisecond))
		select {
		case <-lease.Lost():
		case <-time.After(time.Second):
			t.Fatal("elapsed deadline did not signal lease loss")
		}
		if state := lease.State(); state != dilock.Lost {
			t.Fatalf("lease state = %v, want Lost", state)
		}
	}
}

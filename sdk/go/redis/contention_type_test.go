package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.invalid/dilock/core"
	redisclient "github.com/redis/go-redis/v9"
)

type contentionRedisClient struct {
	metadata     string
	keyType      string
	typeErr      error
	typeFn       func(context.Context) error
	setCalls     int
	setExpiry    time.Duration
	typeCalls    int
	typeDeadline time.Time
}

func (c *contentionRedisClient) Get(ctx context.Context, key string) *redisclient.StringCmd {
	cmd := redisclient.NewStringCmd(ctx, "GET", key)
	metadata := c.metadata
	if metadata == "" {
		metadata = `{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"test-epoch","fixed_lease_ms":null}`
	}
	cmd.SetVal(metadata)
	return cmd
}

func TestNamespaceMetadataRejectsDuplicateTopLevelFields(t *testing.T) {
	for _, metadata := range []string{
		`{"protocol_major":1,"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"test-epoch","fixed_lease_ms":null}`,
		`{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"wrong","coordination_epoch":"test-epoch","fixed_lease_ms":null}`,
	} {
		client := &contentionRedisClient{metadata: metadata}
		provider, err := NewProvider(client, ProviderOptions{Namespace: "test", Epoch: "test-epoch"})
		if err != nil {
			t.Fatal(err)
		}
		if err := provider.metadata(context.Background(), time.Second); !errors.Is(err, dilock.ErrProtocolMismatch) {
			t.Fatalf("metadata(%s) = %v; want ErrProtocolMismatch", metadata, err)
		}
	}
}

func TestNamespaceMetadataAllowsNestedExtensionFields(t *testing.T) {
	client := &contentionRedisClient{metadata: `{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"test-epoch","fixed_lease_ms":null,"extension":{"field":1,"field":2}}`}
	provider, err := NewProvider(client, ProviderOptions{Namespace: "test", Epoch: "test-epoch"})
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.metadata(context.Background(), time.Second); err != nil {
		t.Fatalf("metadata with nested extension fields = %v; want success", err)
	}
}

func (c *contentionRedisClient) Type(ctx context.Context, key string) *redisclient.StatusCmd {
	c.typeCalls++
	if deadline, ok := ctx.Deadline(); ok {
		c.typeDeadline = deadline
	}
	cmd := redisclient.NewStatusCmd(ctx, "TYPE", key)
	if c.typeFn != nil {
		cmd.SetErr(c.typeFn(ctx))
	} else if c.typeErr != nil {
		cmd.SetErr(c.typeErr)
	} else {
		cmd.SetVal(c.keyType)
	}
	return cmd
}

func TestContentionTypePreservesCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &contentionRedisClient{typeFn: func(context.Context) error {
		cancel()
		return context.Canceled
	}}
	provider, err := NewProvider(client, ProviderOptions{Namespace: "test", Epoch: "test-epoch"})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := provider.Lock("resource")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := handle.TryAcquire(ctx, dilock.Options{LeaseDuration: time.Second, RPCTimeout: 200 * time.Millisecond, CleanupTimeout: time.Second})
	if lease != nil || !errors.Is(err, context.Canceled) || errors.Is(err, dilock.ErrBackendUnavailable) {
		t.Fatalf("TryAcquire = %v, %v; want caller cancellation", lease, err)
	}
}

func TestContentionTypeWaitBudgetReturnsWaitTimeout(t *testing.T) {
	client := &contentionRedisClient{typeFn: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	provider, err := NewProvider(client, ProviderOptions{Namespace: "test", Epoch: "test-epoch"})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := provider.Lock("resource")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := handle.Acquire(context.Background(), 20*time.Millisecond, dilock.Options{LeaseDuration: time.Second, RPCTimeout: 200 * time.Millisecond, CleanupTimeout: time.Second})
	if lease != nil || !errors.Is(err, dilock.ErrWaitTimeout) {
		t.Fatalf("Acquire = %v, %v; want overall wait timeout", lease, err)
	}
}

func (c *contentionRedisClient) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redisclient.BoolCmd {
	c.setCalls++
	c.setExpiry = expiration
	cmd := redisclient.NewBoolCmd(ctx, "SET", key, value, "NX", "PX", expiration)
	cmd.SetVal(false)
	return cmd
}

func TestTryAcquireDefaultUsesProviderLeasePolicy(t *testing.T) {
	client := &contentionRedisClient{keyType: "string"}
	policy := dilock.AcquireOptions{
		Wait: time.Second,
		Lease: dilock.Options{
			LeaseDuration:  1500 * time.Millisecond,
			RPCTimeout:     250 * time.Millisecond,
			CleanupTimeout: 500 * time.Millisecond,
		},
	}
	provider, err := NewProvider(client, ProviderOptions{
		Namespace: "test", Epoch: "test-epoch", DefaultAcquisition: policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := provider.Lock("resource")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := handle.TryAcquireDefault(context.Background())
	if err != nil || lease != nil {
		t.Fatalf("TryAcquireDefault = %v, %v; want acknowledged contention", lease, err)
	}
	if client.setExpiry != policy.Lease.LeaseDuration {
		t.Fatalf("SET expiry = %v; want provider lease policy %v", client.setExpiry, policy.Lease.LeaseDuration)
	}
}

func (*contentionRedisClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redisclient.Cmd {
	return redisclient.NewCmd(ctx, "EVAL", script, keys, args)
}

func TestAcknowledgedContentionClassifiesKeyTypeOnce(t *testing.T) {
	for _, test := range []struct {
		name                 string
		typeName             string
		wantBusy             bool
		wantProtocolMismatch bool
	}{
		{name: "string", typeName: "string", wantBusy: true},
		{name: "expired before TYPE", typeName: "none", wantBusy: true},
		{name: "unexpected list", typeName: "list", wantProtocolMismatch: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &contentionRedisClient{keyType: test.typeName}
			provider, err := NewProvider(client, ProviderOptions{Namespace: "test", Epoch: "test-epoch"})
			if err != nil {
				t.Fatal(err)
			}
			handle, err := provider.Lock("resource")
			if err != nil {
				t.Fatal(err)
			}
			options := dilock.Options{LeaseDuration: time.Second, RPCTimeout: 200 * time.Millisecond, CleanupTimeout: time.Second}
			lease, err := handle.TryAcquire(context.Background(), options)
			if lease != nil {
				t.Fatal("contended acquisition returned a lease")
			}
			if test.wantProtocolMismatch {
				if !errors.Is(err, dilock.ErrProtocolMismatch) {
					t.Fatalf("error = %v, want ErrProtocolMismatch", err)
				}
			} else if !test.wantBusy || err != nil {
				t.Fatalf("TryAcquire = nil, %v; want acknowledged busy", err)
			}
			if client.setCalls != 1 || client.typeCalls != 1 {
				t.Fatalf("SET calls = %d, TYPE calls = %d; want exactly one each", client.setCalls, client.typeCalls)
			}
			if client.typeDeadline.IsZero() || time.Until(client.typeDeadline) > options.RPCTimeout {
				t.Fatalf("TYPE deadline = %v; want bounded by %v", client.typeDeadline, options.RPCTimeout)
			}
		})
	}
}

func TestContentionTypeFailureIsBackendUnavailable(t *testing.T) {
	client := &contentionRedisClient{typeErr: context.DeadlineExceeded}
	provider, err := NewProvider(client, ProviderOptions{Namespace: "test", Epoch: "test-epoch"})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := provider.Lock("resource")
	if err != nil {
		t.Fatal(err)
	}
	options := dilock.Options{LeaseDuration: time.Second, RPCTimeout: 200 * time.Millisecond, CleanupTimeout: time.Second}
	lease, err := handle.TryAcquire(context.Background(), options)
	if lease != nil || !errors.Is(err, dilock.ErrBackendUnavailable) {
		t.Fatalf("TryAcquire = %v, %v; want backend unavailable", lease, err)
	}
	if client.setCalls != 1 || client.typeCalls != 1 {
		t.Fatalf("SET calls = %d, TYPE calls = %d; want exactly one each", client.setCalls, client.typeCalls)
	}
}

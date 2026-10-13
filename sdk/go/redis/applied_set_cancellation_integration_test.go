package redis

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	dilock "example.invalid/dilock/core"
	redisclient "github.com/redis/go-redis/v9"
)

type appliedSetProxyStats struct {
	EvalCount       int    `json:"eval_count"`
	DroppedCount    int    `json:"dropped_count"`
	DelayedSetCount int    `json:"delayed_set_count"`
	LastKey         string `json:"last_key"`
}

func readAppliedSetProxyStats(controlEndpoint string) (appliedSetProxyStats, error) {
	var stats appliedSetProxyStats
	conn, err := net.DialTimeout("tcp", controlEndpoint, 500*time.Millisecond)
	if err != nil {
		return stats, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := fmt.Fprintln(conn, "STATS"); err != nil {
		return stats, err
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return stats, err
	}
	err = json.Unmarshal(line, &stats)
	return stats, err
}

func TestRedisCancellationAfterAppliedSetRemainsUnknownAndOwnerCleanupRemovesKey(t *testing.T) {
	if os.Getenv("DILOCK_REDIS_APPLIED_SET_CANCELLATION_INTEGRATION") != "1" {
		t.Skip("set DILOCK_REDIS_APPLIED_SET_CANCELLATION_INTEGRATION=1 with the deterministic RESP proxy to run")
	}
	proxyURL := os.Getenv("DILOCK_REDIS_URL")
	verifyURL := os.Getenv("DILOCK_REDIS_VERIFY_URL")
	controlEndpoint := os.Getenv("DILOCK_REDIS_PROXY_CONTROL")
	if proxyURL == "" || verifyURL == "" || controlEndpoint == "" {
		t.Fatal("DILOCK_REDIS_URL, DILOCK_REDIS_VERIFY_URL and DILOCK_REDIS_PROXY_CONTROL are required")
	}
	var suffixBytes [12]byte
	if _, err := rand.Read(suffixBytes[:]); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(suffixBytes[:])
	namespace := "go-cancel-applied-set-" + suffix
	epoch := "go-cancel-applied-set-epoch-" + suffix
	resource := "cancel-after-applied-set"
	metadataKey := "dilock:namespace:" + namespace + ":meta"
	metadata := `{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"` + epoch + `","fixed_lease_ms":null}`
	adminOptions, err := redisclient.ParseURL(verifyURL)
	if err != nil {
		t.Fatalf("parse disposable Redis verifier URL: %v", err)
	}
	admin := redisclient.NewClient(adminOptions)
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("connect to disposable Redis verifier: %v", err)
	}
	created, err := admin.SetNX(context.Background(), metadataKey, metadata, 0).Result()
	if err != nil || !created {
		t.Fatalf("create unique namespace metadata: created=%t err=%v", created, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = admin.Del(cleanupCtx, metadataKey).Err()
	})

	stats, err := readAppliedSetProxyStats(controlEndpoint)
	if err != nil {
		t.Fatalf("read proxy stats before reset: %v", err)
	}
	if stats.DelayedSetCount != 0 || stats.EvalCount != 0 || stats.DroppedCount != 0 {
		t.Fatalf("fault proxy counters were not clean before test: %+v", stats)
	}
	provider, err := OpenProvider(context.Background(), proxyURL, ProviderOptions{
		Namespace: namespace,
		Epoch:     epoch,
	})
	if err != nil {
		t.Fatalf("open provider through fault proxy: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	handle, err := provider.Lock(resource)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancelAcquire := context.WithCancel(context.Background())
	defer cancelAcquire()
	type acquireResult struct {
		lease *Lease
		err   error
	}
	result := make(chan acquireResult, 1)
	// A fractional-second lease makes go-redis encode the atomic acquisition as
	// SET key owner PX ttl NX, matching the protocol command under test.
	options := dilock.Options{LeaseDuration: 5500 * time.Millisecond, RPCTimeout: 2 * time.Second, CleanupTimeout: time.Second}
	go func() {
		lease, acquireErr := handle.TryAcquire(ctx, options)
		result <- acquireResult{lease: lease, err: acquireErr}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		stats, err = readAppliedSetProxyStats(controlEndpoint)
		if err == nil && stats.DelayedSetCount == 1 {
			break
		}
		if time.Now().After(deadline) {
			cancelAcquire()
			select {
			case early := <-result:
				t.Fatalf("proxy did not observe one applied SET; TryAcquire returned early: lease_returned=%t err=%v stats=%+v statsErr=%v", early.lease != nil, early.err, stats, err)
			case <-time.After(time.Second):
				t.Fatalf("proxy did not observe one applied SET before the deadline: stats=%+v err=%v", stats, err)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if value, getErr := admin.Get(context.Background(), handle.key).Result(); getErr != nil || value == "" {
		cancelAcquire()
		t.Fatalf("proxy reported delayed SET but owner key is absent: value=%q err=%v", value, getErr)
	}
	// The proxy increments DelayedSetCount only after Redis applied SET NX PX
	// and the proxy consumed its successful +OK, while withholding that ACK.
	cancelAcquire()
	var acquired acquireResult
	select {
	case acquired = <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("TryAcquire did not return promptly after caller cancellation")
	}
	if acquired.lease != nil || !errors.Is(acquired.err, dilock.ErrAcquisitionUnknown) || !errors.Is(acquired.err, context.Canceled) {
		t.Fatalf("canceled applied SET = lease %v, error %v; want no lease and acquisition-unknown wrapping context.Canceled", acquired.lease, acquired.err)
	}
	if errors.Is(acquired.err, dilock.ErrBackendUnavailable) {
		t.Fatalf("ambiguous acquisition was misclassified as a backend failure: %v", acquired.err)
	}

	cleanupDeadline := time.Now().Add(3 * time.Second)
	for provider.OutstandingCleanup() != 0 && time.Now().Before(cleanupDeadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if remaining := provider.OutstandingCleanup(); remaining != 0 {
		t.Fatalf("tracked owner cleanup still pending: %d", remaining)
	}
	stats, err = readAppliedSetProxyStats(controlEndpoint)
	if err != nil {
		t.Fatalf("read proxy stats after cleanup: %v", err)
	}
	if stats.DelayedSetCount != 1 || stats.DroppedCount != 0 || stats.EvalCount != 2 {
		t.Fatalf("unexpected SET/owner-cleanup traffic (retry/fallback forbidden): %+v; key=%q", stats, handle.key)
	}
	if _, getErr := admin.Get(context.Background(), handle.key).Result(); !errors.Is(getErr, redisclient.Nil) {
		t.Fatalf("owner-checked cleanup left acquired key: %v", getErr)
	}
	t.Logf("PASS: applied SET acknowledgment delayed; cancellation remained acquisition-unknown; one SET and two owner-checked cleanup EVALs; no lock key remains")
}

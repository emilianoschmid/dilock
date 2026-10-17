package consul

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	dilock "example.invalid/dilock/core"
)

func TestSessionLifecycleAndFence(t *testing.T) {
	metadata, _ := json.Marshal(map[string]any{
		"protocol_major": 1,
		"identity_scheme": "sha256-framed-utf8-v1",
		"profile": profile,
		"coordination_epoch": "epoch-test",
		"fixed_lease_ms": nil,
	})
	var mu sync.Mutex
	var owner string
	var index int
	var loseReleaseAck bool
	var destroyCalls int
	resourceID := testResourceID(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/kv/dilock/namespaces/default/meta":
			_, _ = fmt.Fprintf(w, `[{"Key":"dilock/namespaces/default/meta","Value":%q}]`, base64.StdEncoding.EncodeToString(metadata))
		case r.Method == http.MethodPut && r.URL.Path == "/v1/session/create":
			_, _ = fmt.Fprint(w, `{"ID":"session-test"}`)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/kv/dilock/locks/"+resourceID:
			mu.Lock()
			defer mu.Unlock()
			if r.URL.Query().Has("acquire") {
				if owner != "" { _, _ = fmt.Fprint(w, "false"); return }
				owner = r.URL.Query().Get("acquire")
				index++
				_, _ = fmt.Fprint(w, "true")
				return
			}
			if r.URL.Query().Has("release") && loseReleaseAck {
				loseReleaseAck = false
				http.Error(w, "injected lost release acknowledgement", http.StatusInternalServerError)
				return
			}
			if owner == r.URL.Query().Get("release") { owner = "" }
			_, _ = fmt.Fprint(w, "true")
		case r.Method == http.MethodGet && r.URL.Path == "/v1/kv/dilock/locks/"+resourceID:
			mu.Lock(); defer mu.Unlock()
			_, _ = fmt.Fprintf(w, `[{"Key":"dilock/locks/%s","Session":%q,"LockIndex":%d}]`, resourceID, owner, index)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/session/destroy/session-test":
			mu.Lock(); destroyCalls++; mu.Unlock()
			_, _ = fmt.Fprint(w, "true")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	provider, err := NewProvider(context.Background(), server.URL, ProviderOptions{Epoch: "epoch-test"})
	if err != nil { t.Fatal(err) }
	h, err := provider.Lock("invoice/42")
	if err != nil { t.Fatal(err) }
	lease, err := h.Acquire(context.Background(), time.Second, dilock.Options{LeaseDuration: 10*time.Second, RPCTimeout: time.Second, CleanupTimeout: time.Second})
	if err != nil { t.Fatal(err) }
	if fence := lease.Fence(); fence.Epoch != "epoch-test" || fence.LockIndex != 1 { t.Fatalf("unexpected fence: %#v", fence) }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second); defer cancel()
	outcome, err := lease.Release(ctx)
	if err != nil || outcome != dilock.ReleasedOwner { t.Fatalf("release outcome=%v err=%v", outcome, err) }
	mu.Lock(); loseReleaseAck = true; mu.Unlock()
	uncertain, err := h.TryAcquire(context.Background(), dilock.Options{LeaseDuration: 10*time.Second, RPCTimeout: time.Second, CleanupTimeout: time.Second})
	if err != nil || uncertain == nil { t.Fatalf("second acquire=%v err=%v", uncertain, err) }
	unknownOutcome, unknownErr := uncertain.Release(ctx)
	if unknownOutcome != dilock.ReleaseUnknown || unknownErr == nil { t.Fatalf("lost release outcome=%v err=%v", unknownOutcome, unknownErr) }
	mu.Lock(); destroyedAfterUnknown := destroyCalls; mu.Unlock()
	if destroyedAfterUnknown != 2 { t.Fatalf("destroy calls=%d; want one per release", destroyedAfterUnknown) }
	if err := provider.Close(); err != nil { t.Fatal(err) }
}

func TestRoundTTL(t *testing.T) {
	got, err := roundTTL(10*time.Second + time.Millisecond)
	if err != nil || got != 11*time.Second { t.Fatalf("got %v err=%v", got, err) }
	if _, err := roundTTL(9*time.Second); err == nil { t.Fatal("short session TTL accepted") }
}

func testResourceID(t *testing.T) string {
	t.Helper()
	id, err := dilock.Identity("default", "invoice/42")
	if err != nil { t.Fatal(err) }
	return id
}

// Command dilock-go-worker implements the shared JSON Lines conformance protocol.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"example.invalid/dilock/core"
)

type config struct {
	Backend   string `json:"backend"`
	Endpoint  string `json:"endpoint"`
	Namespace string `json:"namespace"`
	Epoch     string `json:"epoch"`
}
type request struct {
	ID        string `json:"id"`
	Op        string `json:"op"`
	Resource  string `json:"resource"`
	LeaseID   string `json:"lease_id"`
	TargetID  string `json:"target_id"`
	WaitMS    int64  `json:"wait_ms"`
	LeaseMS   int64  `json:"lease_ms"`
	RPCMS     int64  `json:"rpc_ms"`
	AutoRenew bool   `json:"auto_renew"`
}
type response map[string]any

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: dilock-go-worker CONFIG.json")
		os.Exit(2)
	}
	data, err := os.ReadFile(os.Args[1])
	fatalIf(err)
	var cfg config
	fatalIf(json.Unmarshal(data, &cfg))
	provider, err := openBackend(context.Background(), cfg)
	fatalIf(err)
	w := &worker{provider: provider, leases: map[string]lockLease{}, pending: map[string]context.CancelFunc{}, seen: map[string]struct{}{}}
	w.serve(os.Stdin)
}

type worker struct {
	provider lockProvider
	mu       sync.Mutex
	writeMu  sync.Mutex
	leases   map[string]lockLease
	pending  map[string]context.CancelFunc
	seen     map[string]struct{}
}

func (w *worker) write(r response) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(r)
}

func (w *worker) serve(input io.Reader) {
	s := bufio.NewScanner(input)
	s.Buffer(make([]byte, 4096), 65536)
	var all sync.WaitGroup
	shutdownID := ""
	for s.Scan() {
		line := append([]byte(nil), s.Bytes()...)
		var r request
		if err := decodeRequest(line, &r); err != nil || !validRequest(r) {
			w.write(response{"id": "", "status": "invalid_argument"})
			continue
		}
		w.mu.Lock()
		_, duplicate := w.seen[r.ID]
		if !duplicate {
			w.seen[r.ID] = struct{}{}
		}
		w.mu.Unlock()
		if duplicate {
			w.write(response{"id": r.ID, "status": "invalid_argument"})
			continue
		}
		if r.Op == "shutdown" {
			shutdownID = r.ID
			break
		}
		if r.Op == "cancel_request" {
			w.mu.Lock()
			cancel := w.pending[r.TargetID]
			w.mu.Unlock()
			if cancel != nil {
				cancel()
			}
			w.write(response{"id": r.ID, "status": "ok"})
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		w.mu.Lock()
		w.pending[r.ID] = cancel
		w.mu.Unlock()
		all.Add(1)
		go func() {
			defer all.Done()
			defer cancel()
			defer func() { w.mu.Lock(); delete(w.pending, r.ID); w.mu.Unlock() }()
			w.process(ctx, r)
		}()
	}
	if err := s.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "JSONL input failed:", err)
		w.write(response{"id": "", "status": "invalid_argument"})
	}
	w.mu.Lock()
	for _, cancel := range w.pending {
		cancel()
	}
	w.mu.Unlock()
	all.Wait()
	w.mu.Lock()
	leases := make([]lockLease, 0, len(w.leases))
	for _, l := range w.leases {
		leases = append(leases, l)
	}
	w.mu.Unlock()
	for _, l := range leases {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, _ = l.release(ctx)
		cancel()
	}
	_ = w.provider.close()
	if shutdownID != "" {
		w.write(response{"id": shutdownID, "status": "ok", "outstanding_cleanup": w.provider.outstandingCleanup()})
	}
}

func (w *worker) process(ctx context.Context, r request) {
	switch r.Op {
	case "hello":
		serverVersion, clientVersion, capabilities, err := w.provider.hello(ctx)
		if err != nil {
			w.failure(r.ID, err)
			return
		}
		w.write(response{"id": r.ID, "status": "ok", "sdk": "go", "version": "0.1.0-dev", "protocol": 1, "server_version": serverVersion, "client_version": clientVersion, "capabilities": capabilities})
	case "try_acquire", "acquire":
		leaseDuration := 30 * time.Second
		if r.LeaseMS > 0 {
			leaseDuration = time.Duration(r.LeaseMS) * time.Millisecond
		}
		rpc := 200 * time.Millisecond
		if r.RPCMS > 0 {
			rpc = time.Duration(r.RPCMS) * time.Millisecond
		}
		o := dilock.Options{LeaseDuration: leaseDuration, RPCTimeout: rpc, CleanupTimeout: 2 * time.Second, AutoRenew: r.AutoRenew}
		h, err := w.provider.lock(r.Resource)
		if err != nil {
			w.failure(r.ID, err)
			return
		}
		var lease lockLease
		if r.Op == "try_acquire" {
			lease, err = h.tryAcquire(ctx, o)
		} else {
			wait := 5 * time.Second
			if r.WaitMS > 0 {
				wait = time.Duration(r.WaitMS) * time.Millisecond
			}
			lease, err = h.acquire(ctx, wait, o)
		}
		if err != nil {
			w.failure(r.ID, err)
			return
		}
		if lease == nil {
			w.write(response{"id": r.ID, "status": "busy"})
			return
		}
		id, idErr := randomID()
		if idErr != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = lease.release(ctx)
			cancel()
			w.failure(r.ID, idErr)
			return
		}
		w.mu.Lock()
		w.leases[id] = lease
		w.mu.Unlock()
		acquired := response{"id": r.ID, "status": "acquired", "lease_id": id, "effective_lease_ms": lease.effectiveLeaseMilliseconds()}
		if epoch, revision, ok := lease.fence(); ok {
			acquired["fence"] = map[string]string{"epoch": epoch, "value": revision}
		}
		w.write(acquired)
	case "release":
		w.mu.Lock()
		lease := w.leases[r.LeaseID]
		w.mu.Unlock()
		if lease == nil {
			w.failure(r.ID, fmt.Errorf("unknown lease_id"))
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		outcome, err := lease.release(cleanup)
		cancel()
		if err != nil {
			w.failure(r.ID, err)
			return
		}
		status := map[dilock.ReleaseOutcome]string{dilock.ReleasedOwner: "released", dilock.AlreadyNotOwned: "already_not_owned", dilock.ReleaseUnknown: "release_unknown"}[outcome]
		w.write(response{"id": r.ID, "status": status})
	case "inspect":
		w.mu.Lock()
		lease := w.leases[r.LeaseID]
		w.mu.Unlock()
		if lease == nil {
			w.failure(r.ID, fmt.Errorf("unknown lease_id"))
			return
		}
		states := map[dilock.State]string{dilock.Held: "held", dilock.Releasing: "releasing", dilock.Released: "released", dilock.ReleasedUnknown: "released_unknown", dilock.Lost: "lost"}
		w.write(response{"id": r.ID, "status": "ok", "state": states[lease.state()], "loss_reason": lease.lossReason()})
	case "renew":
		w.mu.Lock()
		lease := w.leases[r.LeaseID]
		w.mu.Unlock()
		if lease == nil {
			w.failure(r.ID, fmt.Errorf("unknown lease_id"))
			return
		}
		ok, err := lease.renew(ctx)
		if err != nil {
			w.failure(r.ID, err)
			return
		}
		status := "lost"
		if ok {
			status = "renewed"
		}
		w.write(response{"id": r.ID, "status": status})
	default:
		w.write(response{"id": r.ID, "status": "invalid_argument"})
	}
}

func decodeRequest(line []byte, r *request) error {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()
	if err := dec.Decode(r); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("trailing JSON value")
	}
	return nil
}

func validRequest(r request) bool {
	if r.ID == "" || len(r.ID) > 128 {
		return false
	}
	switch r.Op {
	case "hello", "shutdown":
		return true
	case "acquire":
		return r.Resource != "" && r.WaitMS > 0
	case "try_acquire":
		return r.Resource != ""
	case "release", "inspect", "renew":
		return r.LeaseID != ""
	case "cancel_request":
		return r.TargetID != ""
	default:
		return false
	}
}

func (w *worker) failure(id string, err error) {
	switch {
	case errors.Is(err, dilock.ErrAcquisitionUnknown):
		w.write(response{"id": id, "status": "acquisition_unknown", "remote_outcome_unknown": true})
	case errors.Is(err, context.Canceled):
		w.write(response{"id": id, "status": "cancelled", "remote_outcome_unknown": false})
	case errors.Is(err, dilock.ErrBackendUnavailable):
		w.write(response{"id": id, "status": "backend_unavailable"})
	case errors.Is(err, dilock.ErrProtocolMismatch):
		w.write(response{"id": id, "status": "protocol_mismatch"})
	case errors.Is(err, dilock.ErrUnsupported):
		w.write(response{"id": id, "status": "unsupported_capability"})
	case errors.Is(err, dilock.ErrWaitTimeout), errors.Is(err, context.DeadlineExceeded):
		w.write(response{"id": id, "status": "timeout"})
	default:
		w.write(response{"id": id, "status": "invalid_argument"})
	}
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b[:]), nil
}

func fatalIf(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Package redis implements Dilock's Redis single-authority lease profile.
package redis

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"example.invalid/dilock/core"
	redisclient "github.com/redis/go-redis/v9"
)

//go:embed scripts/*.lua
var scriptFS embed.FS

var releaseScript, _ = scriptFS.ReadFile("scripts/release.lua")
var renewScript, _ = scriptFS.ReadFile("scripts/renew.lua")

// RedisClient is the narrow client surface used by the adapter. Implementations
// passed to NewProvider are borrowed and are never closed by the provider.
type RedisClient interface {
	Get(context.Context, string) *redisclient.StringCmd
	Type(context.Context, string) *redisclient.StatusCmd
	SetNX(context.Context, string, interface{}, time.Duration) *redisclient.BoolCmd
	Eval(context.Context, string, []string, ...interface{}) *redisclient.Cmd
}

// ProviderOptions identifies the shared namespace and stable coordinator epoch.
type ProviderOptions struct {
	Namespace, Epoch   string
	DefaultAcquisition dilock.AcquireOptions
	CleanupObserver    func(dilock.CleanupObservation)
}

// Provider owns handles, active leases and uncertain-command cleanup. Clients
// passed to NewProvider are borrowed. OpenProvider creates and owns its client.
type Provider struct {
	client             RedisClient
	owned              *redisclient.Client
	namespace, epoch   string
	defaultAcquisition dilock.AcquireOptions
	cleanupObserver    func(dilock.CleanupObservation)
	mu                 sync.Mutex
	closed             bool
	leases             map[*Lease]struct{}
	cleanupMu          sync.Mutex
	cleanupCount       int
	cleanupZero        chan struct{}
	operationMu        sync.Mutex
	operationCount     int
	operationZero      chan struct{}
}

// OpenProvider connects to exactly one Redis URL. The URL must use redis:// or
// rediss:// and must identify one standalone authority. Hidden retries and the
// offline queue are disabled; Redis failover cannot provide a strong fence.
func OpenProvider(ctx context.Context, rawURL string, options ProviderOptions) (*Provider, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	if _, err := dilock.Identity(options.Namespace, "validation"); err != nil {
		return nil, err
	}
	if err := validateEpoch(options.Epoch); err != nil {
		return nil, err
	}
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "redis" && u.Scheme != "rediss") || u.Host == "" {
		return nil, fmt.Errorf("invalid standalone Redis URL")
	}
	opt, err := redisclient.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis URL: %w", err)
	}
	opt.MaxRetries = -1
	opt.DialTimeout, opt.ReadTimeout, opt.WriteTimeout = 2*time.Second, 2*time.Second, 2*time.Second
	opt.PoolTimeout = 2 * time.Second
	client := redisclient.NewClient(opt)
	p, err := newProvider(client, options)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, mapBackend(err, "Redis connection failed")
	}
	p.owned = client
	return p, nil
}

// NewProvider borrows an established client. The caller retains responsibility
// for its lifetime and must configure it without command retries/offline queues.
func NewProvider(client RedisClient, options ProviderOptions) (*Provider, error) {
	if client == nil {
		return nil, fmt.Errorf("Redis client is required")
	}
	return newProvider(client, options)
}

func newProvider(client RedisClient, options ProviderOptions) (*Provider, error) {
	if options.Namespace == "" {
		options.Namespace = "default"
	}
	if _, err := dilock.Identity(options.Namespace, "validation"); err != nil {
		return nil, err
	}
	if err := validateEpoch(options.Epoch); err != nil {
		return nil, err
	}
	zero := make(chan struct{})
	close(zero)
	operationZero := make(chan struct{})
	close(operationZero)
	defaults, err := options.DefaultAcquisition.Normalized()
	if err != nil {
		return nil, err
	}
	return &Provider{client: client, namespace: options.Namespace, epoch: options.Epoch,
		defaultAcquisition: defaults, cleanupObserver: options.CleanupObserver,
		leases: make(map[*Lease]struct{}), cleanupZero: zero, operationZero: operationZero}, nil
}

func (p *Provider) observeCleanup(outcome dilock.ReleaseOutcome, reason string) {
	observation := dilock.CleanupObservation{Backend: "redis", Outcome: outcome, Reason: reason}
	if p.cleanupObserver != nil {
		func() { defer func() { _ = recover() }(); p.cleanupObserver(observation) }()
		return
	}
	log.Printf("dilock redis scope cleanup outcome=%d reason=%s", outcome, reason)
}

func validateEpoch(epoch string) error {
	if len(epoch) < 1 || len(epoch) > 1024 || !utf8.ValidString(epoch) {
		return fmt.Errorf("invalid coordination epoch")
	}
	for _, r := range epoch {
		if r == 0 {
			return fmt.Errorf("invalid coordination epoch")
		}
	}
	return nil
}

// Lock validates a resource and returns a reusable handle without network I/O.
func (p *Provider) Lock(resource string) (*Handle, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, dilock.ErrProviderClosed
	}
	key, err := dilock.RedisKey(p.namespace, resource)
	if err != nil {
		return nil, err
	}
	return &Handle{provider: p, key: key}, nil
}

// Close withdraws local lease permission, waits for bounded tracked cleanup,
// and closes only the connection created by OpenProvider.
func (p *Provider) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	leases := make([]*Lease, 0, len(p.leases))
	for l := range p.leases {
		leases = append(leases, l)
	}
	p.mu.Unlock()
	for _, l := range leases {
		l.markLost("provider_shutdown")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, _ = l.Release(ctx)
		cancel()
	}
	done := make(chan struct{})
	go func() { p.waitOperations(); p.waitCleanup(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	if p.owned != nil {
		return p.owned.Close()
	}
	return nil
}

func (p *Provider) removeLease(l *Lease) { p.mu.Lock(); delete(p.leases, l); p.mu.Unlock() }

// OutstandingCleanup reports owner-specific cleanup tasks still being observed.
func (p *Provider) OutstandingCleanup() int {
	p.cleanupMu.Lock()
	defer p.cleanupMu.Unlock()
	return p.cleanupCount
}

// ServerVersion returns Redis' server_version from INFO server.
func (p *Provider) ServerVersion(ctx context.Context) (string, error) {
	infoClient, ok := p.client.(interface {
		Info(context.Context, ...string) *redisclient.StringCmd
	})
	if !ok {
		return "", fmt.Errorf("borrowed Redis client does not expose INFO")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	raw, err := infoClient.Info(ctx, "server").Result()
	if err != nil {
		return "", mapBackend(err, "Redis version query failed")
	}
	for _, line := range strings.Split(raw, "\r\n") {
		if strings.HasPrefix(line, "redis_version:") {
			return strings.TrimPrefix(line, "redis_version:"), nil
		}
	}
	return "", dilock.ErrProtocolMismatch
}

func (p *Provider) metadata(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	raw, err := p.client.Get(ctx, "dilock:namespace:"+p.namespace+":meta").Bytes()
	if err != nil {
		if errors.Is(err, redisclient.Nil) {
			return dilock.ErrProtocolMismatch
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return mapBackend(err, "namespace read failed")
	}
	if len(raw) == 0 || len(raw) > 8192 {
		return dilock.ErrProtocolMismatch
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return dilock.ErrProtocolMismatch
	}
	if hasDuplicateTopLevelJSONField(raw) {
		return dilock.ErrProtocolMismatch
	}
	for _, name := range []string{"protocol_major", "identity_scheme", "profile", "coordination_epoch", "fixed_lease_ms"} {
		if _, ok := fields[name]; !ok {
			return dilock.ErrProtocolMismatch
		}
	}
	var m struct {
		ProtocolMajor  int    `json:"protocol_major"`
		IdentityScheme string `json:"identity_scheme"`
		Profile        string `json:"profile"`
		Epoch          string `json:"coordination_epoch"`
		FixedLeaseMS   *int64 `json:"fixed_lease_ms"`
	}
	// Unknown optional fields are ignored; all required draft 1 fields are checked above.
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	if err := dec.Decode(&m); err != nil {
		return dilock.ErrProtocolMismatch
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return dilock.ErrProtocolMismatch
	}
	if string(fields["fixed_lease_ms"]) != "null" || m.ProtocolMajor != 1 || m.IdentityScheme != "sha256-framed-utf8-v1" || m.Profile != "redis-single-authority-v1" || m.Epoch != p.epoch || m.FixedLeaseMS != nil {
		return dilock.ErrProtocolMismatch
	}
	return nil
}

func (p *Provider) cleanupOwner(key, owner string, timeout time.Duration) dilock.ReleaseOutcome {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	v, err := p.client.Eval(ctx, string(releaseScript), []string{key}, owner).Int64()
	if err != nil {
		return dilock.ReleaseUnknown
	}
	if v == 1 {
		return dilock.ReleasedOwner
	}
	if v == 0 {
		return dilock.AlreadyNotOwned
	}
	return dilock.ReleaseUnknown
}

func (p *Provider) trackCleanup(fn func()) {
	p.cleanupMu.Lock()
	if p.cleanupCount == 0 {
		p.cleanupZero = make(chan struct{})
	}
	p.cleanupCount++
	p.cleanupMu.Unlock()
	go func() {
		defer func() {
			p.cleanupMu.Lock()
			p.cleanupCount--
			if p.cleanupCount == 0 {
				close(p.cleanupZero)
			}
			p.cleanupMu.Unlock()
		}()
		fn()
	}()
}

func (p *Provider) beginOperation() bool {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return false
	}
	p.operationMu.Lock()
	if p.operationCount == 0 {
		p.operationZero = make(chan struct{})
	}
	p.operationCount++
	p.operationMu.Unlock()
	p.mu.Unlock()
	return true
}

func (p *Provider) endOperation() {
	p.operationMu.Lock()
	p.operationCount--
	if p.operationCount == 0 {
		close(p.operationZero)
	}
	p.operationMu.Unlock()
}

func (p *Provider) waitOperations() {
	for {
		p.operationMu.Lock()
		if p.operationCount == 0 {
			p.operationMu.Unlock()
			return
		}
		zero := p.operationZero
		p.operationMu.Unlock()
		<-zero
	}
}

func (p *Provider) waitCleanup() {
	for {
		p.cleanupMu.Lock()
		if p.cleanupCount == 0 {
			p.cleanupMu.Unlock()
			return
		}
		zero := p.cleanupZero
		p.cleanupMu.Unlock()
		<-zero
	}
}

func mapBackend(err error, action string) error {
	if err == nil {
		return nil
	}
	return &dilock.OperationError{Kind: dilock.ErrBackendUnavailable, Message: action, Cause: err}
}

func token() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "dilock/1/" + hex.EncodeToString(b[:]), nil
}

func scriptHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

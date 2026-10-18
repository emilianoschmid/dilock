// Package postgres implements Dilock's postgres-advisory-session-v1 profile.
package postgres

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	dilock "example.invalid/dilock/core"
	"github.com/jackc/pgx/v5"
)

const (
	profile = "postgres-advisory-session-v1"
	scheme  = "sha256-framed-utf8-v1"
)

// ProviderOptions fixes the provisioned namespace and coordinator epoch.
type ProviderOptions struct {
	Namespace, Epoch string
	RPCTimeout, CleanupTimeout time.Duration
	PollInterval time.Duration
	DefaultAcquisition dilock.AcquireOptions
	CleanupObserver func(dilock.CleanupObservation)
}

// Provider owns no pool. Every successful acquisition owns one pgx.Conn until
// its same-session advisory unlock completes.
type Provider struct {
	dsn string
	options ProviderOptions
	mu sync.Mutex
	closed bool
	leases map[*Lease]struct{}
}

// NewProvider opens one validation connection and then closes it. Runtime
// construction never creates or repairs metadata.
func NewProvider(ctx context.Context, dsn string, options ProviderOptions) (*Provider, error) {
	if ctx == nil || dsn == "" { return nil, dilock.ErrInvalidOptions }
	if options.Namespace == "" { options.Namespace = "default" }
	if _, err := dilock.Identity(options.Namespace, "validation"); err != nil { return nil, err }
	if options.Epoch == "" || len(options.Epoch) > 1024 || !utf8.ValidString(options.Epoch) { return nil, dilock.ErrInvalidOptions }
	if options.RPCTimeout == 0 { options.RPCTimeout = 2*time.Second }
	if options.CleanupTimeout == 0 { options.CleanupTimeout = 2*time.Second }
	if options.PollInterval == 0 { options.PollInterval = 25*time.Millisecond }
	if options.RPCTimeout <= 0 || options.CleanupTimeout <= 0 || options.PollInterval <= 0 || options.RPCTimeout > 24*time.Hour || options.CleanupTimeout > 24*time.Hour { return nil, dilock.ErrInvalidOptions }
	policy, err := options.DefaultAcquisition.Normalized(); if err != nil { return nil, err }
	options.DefaultAcquisition = policy
	p := &Provider{dsn: dsn, options: options, leases: make(map[*Lease]struct{})}
	conn, err := p.connect(ctx, options.RPCTimeout); if err != nil { return nil, err }
	defer conn.Close(context.Background())
	if err := p.readMetadata(ctx, conn); err != nil { return nil, err }
	return p, nil
}

func (p *Provider) connect(ctx context.Context, timeout time.Duration) (*pgx.Conn, error) {
	bounded, cancel := context.WithTimeout(ctx, timeout); defer cancel()
	config, err := pgx.ParseConfig(p.dsn); if err != nil { return nil, &dilock.OperationError{Kind:dilock.ErrInvalidOptions, Message:"invalid PostgreSQL DSN", Cause:err} }
	config.RuntimeParams["statement_timeout"] = fmt.Sprintf("%d", timeout.Milliseconds())
	conn, err := pgx.ConnectConfig(bounded, config)
	if err != nil { return nil, &dilock.OperationError{Kind:dilock.ErrBackendUnavailable, Message:"PostgreSQL connection failed", Cause:err} }
	return conn, nil
}

func (p *Provider) readMetadata(ctx context.Context, conn *pgx.Conn) error {
	bounded, cancel := context.WithTimeout(ctx, p.options.RPCTimeout); defer cancel()
	var major int; var gotScheme, gotProfile, gotEpoch string; var fixed any
	err := conn.QueryRow(bounded, "SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace=$1", p.options.Namespace).Scan(&major, &gotScheme, &gotProfile, &gotEpoch, &fixed)
	if errors.Is(err, pgx.ErrNoRows) { return dilock.ErrProtocolMismatch }
	if err != nil { return &dilock.OperationError{Kind:dilock.ErrBackendUnavailable, Message:"failed reading PostgreSQL namespace metadata", Cause:err} }
	if major != 1 || gotScheme != scheme || gotProfile != profile || gotEpoch != p.options.Epoch || fixed != nil { return dilock.ErrProtocolMismatch }
	return nil
}

// ProvisionNamespace creates exact metadata only when the namespace is absent.
func ProvisionNamespace(ctx context.Context, dsn string, options ProviderOptions) error {
	if ctx == nil || dsn == "" { return dilock.ErrInvalidOptions }
	if options.Namespace == "" { options.Namespace = "default" }
	if _, err := dilock.Identity(options.Namespace, "validation"); err != nil { return err }
	if options.Epoch == "" || len(options.Epoch) > 1024 || !utf8.ValidString(options.Epoch) { return dilock.ErrInvalidOptions }
	if options.RPCTimeout == 0 { options.RPCTimeout = 2*time.Second }
	if options.RPCTimeout <= 0 || options.RPCTimeout > 24*time.Hour { return dilock.ErrInvalidOptions }
	bounded, cancel := context.WithTimeout(ctx, options.RPCTimeout); defer cancel()
	conn, err := pgx.Connect(bounded, dsn); if err != nil { return &dilock.OperationError{Kind:dilock.ErrBackendUnavailable, Message:"PostgreSQL connection failed", Cause:err} }
	defer conn.Close(context.Background())
	tx, err := conn.Begin(bounded); if err != nil { return &dilock.OperationError{Kind:dilock.ErrBackendUnavailable, Message:"failed starting PostgreSQL namespace-provision transaction", Cause:err} }
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(bounded, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "dilock:namespace-meta:v1"); err != nil { return &dilock.OperationError{Kind:dilock.ErrBackendUnavailable, Message:"failed serializing PostgreSQL namespace provisioning", Cause:err} }
	if _, err = tx.Exec(bounded, "CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)"); err != nil { return &dilock.OperationError{Kind:dilock.ErrBackendUnavailable, Message:"failed creating PostgreSQL metadata table", Cause:err} }
	if _, err = tx.Exec(bounded, "INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES ($1,1,$2,$3,$4,NULL) ON CONFLICT (namespace) DO NOTHING", options.Namespace, scheme, profile, options.Epoch); err != nil { return &dilock.OperationError{Kind:dilock.ErrBackendUnavailable, Message:"failed provisioning PostgreSQL namespace", Cause:err} }
	if err = tx.Commit(bounded); err != nil { return &dilock.OperationError{Kind:dilock.ErrBackendUnavailable, Message:"failed committing PostgreSQL namespace provisioning", Cause:err} }
	var check ProviderOptions = options; check.RPCTimeout = options.RPCTimeout
	p := &Provider{dsn: dsn, options: check}; return p.readMetadata(bounded, conn)
}

// AdvisoryKey converts the first eight digest bytes to signed PostgreSQL bigint.
func AdvisoryKey(resourceID string) (int64, error) {
	if len(resourceID) != 64 { return 0, dilock.ErrInvalidIdentity }
	bytes, err := hex.DecodeString(resourceID[:16]); if err != nil { return 0, dilock.ErrInvalidIdentity }
	return int64(binary.BigEndian.Uint64(bytes)), nil
}

func (p *Provider) Lock(resource string) (*Handle, error) {
	p.mu.Lock(); closed := p.closed; p.mu.Unlock(); if closed { return nil, dilock.ErrProviderClosed }
	id, err := dilock.Identity(p.options.Namespace, resource); if err != nil { return nil, err }
	key, err := AdvisoryKey(id); if err != nil { return nil, err }
	return &Handle{provider:p, key:key}, nil
}

type Handle struct { provider *Provider; key int64 }

func (p *Provider) tryAcquire(ctx context.Context, key int64, options dilock.Options) (*Lease, error) {
	if ctx == nil { return nil, dilock.ErrInvalidOptions }
	if options.RequireFencing || options.AutoRenew { return nil, dilock.ErrUnsupported }
	policy, _, _, err := options.Normalized(); if err != nil { return nil, err }
	conn, err := p.connect(ctx, policy.RPCTimeout); if err != nil { return nil, err }
	bounded, cancel := context.WithTimeout(ctx, policy.RPCTimeout); defer cancel()
	var acquired bool
	err = conn.QueryRow(bounded, "SELECT pg_try_advisory_lock($1::bigint)", key).Scan(&acquired)
	if err != nil { conn.Close(context.Background()); return nil, &dilock.OperationError{Kind:dilock.ErrAcquisitionUnknown, Message:"PostgreSQL advisory-lock acknowledgement is unknown", Cause:err} }
	if !acquired { conn.Close(context.Background()); return nil, nil }
	lease := &Lease{provider:p, conn:conn, key:key, options:policy, state:dilock.Held}
	p.mu.Lock(); if p.closed { p.mu.Unlock(); conn.Close(context.Background()); return nil, dilock.ErrProviderClosed }; p.leases[lease]=struct{}{}; p.mu.Unlock()
	return lease, nil
}

// Close attempts bounded release of all tracked sessions.
func (p *Provider) Close() error {
	p.mu.Lock(); if p.closed { p.mu.Unlock(); return nil }; p.closed=true; leases:=make([]*Lease,0,len(p.leases)); for l:=range p.leases { leases=append(leases,l) }; p.mu.Unlock()
	for _, l := range leases { ctx,cancel:=context.WithTimeout(context.Background(),p.options.CleanupTimeout); _,_=l.Release(ctx); cancel() }
	return nil
}

func (p *Provider) remove(l *Lease) { p.mu.Lock(); delete(p.leases,l); p.mu.Unlock() }
func (p *Provider) observe(outcome dilock.ReleaseOutcome, reason string) { if p.options.CleanupObserver != nil { defer func(){ _=recover() }(); p.options.CleanupObserver(dilock.CleanupObservation{Backend:"postgres",Outcome:outcome,Reason:reason}) } }

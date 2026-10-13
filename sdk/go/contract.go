package dilock

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// AcquireOptions is the immutable provider policy or complete explicit policy
// for one waiting acquisition. Provider configuration may normalize zero fields;
// per-acquisition methods reject omitted budgets.
type AcquireOptions struct {
	Wait  time.Duration
	Lease Options
}

// DefaultAcquireOptions returns a finite five-second wait and standard lease budgets.
func DefaultAcquireOptions() AcquireOptions {
	return AcquireOptions{Wait: 5 * time.Second, Lease: DefaultOptions()}
}

// Normalized applies common defaults for provider configuration and validates the policy.
func (o AcquireOptions) Normalized() (AcquireOptions, error) {
	defaults := DefaultAcquireOptions()
	if o.Wait == 0 {
		o.Wait = defaults.Wait
	}
	lease, _, _, err := o.Lease.Normalized()
	if err != nil {
		return AcquireOptions{}, err
	}
	o.Lease = lease
	if o.Wait <= 0 || o.Wait > 24*time.Hour {
		return AcquireOptions{}, fmt.Errorf("%w: wait must be finite and positive", ErrInvalidOptions)
	}
	return o, nil
}

// ValidateComplete validates an explicit per-acquisition policy without filling omitted fields.
func (o AcquireOptions) ValidateComplete() error {
	if o.Wait <= 0 || o.Wait > 24*time.Hour {
		return fmt.Errorf("%w: wait must be finite and positive", ErrInvalidOptions)
	}
	if o.Lease.LeaseDuration <= 0 || o.Lease.RPCTimeout <= 0 || o.Lease.CleanupTimeout <= 0 {
		return fmt.Errorf("%w: explicit policy must set lease, RPC and cleanup budgets", ErrInvalidOptions)
	}
	_, _, err := o.Lease.validate()
	return err
}

// CleanupObservation contains safe scope-cleanup diagnostics, without owner tokens.
type CleanupObservation struct {
	Backend string
	Outcome ReleaseOutcome
	Reason  string
}

// These sentinel errors are stable categories for errors.Is.
var (
	ErrAcquisitionUnknown = errors.New("dilock: acquisition outcome unknown")
	ErrBackendUnavailable = errors.New("dilock: backend unavailable")
	ErrProtocolMismatch   = errors.New("dilock: incompatible namespace metadata or backend reply")
	ErrUnsupported        = errors.New("dilock: unsupported backend capability")
	ErrWaitTimeout        = errors.New("dilock: contention wait exhausted")
	ErrLeaseLost          = errors.New("dilock: lease no longer locally held")
	ErrReleaseUnknown     = errors.New("dilock: scope release acknowledgement unknown")
	ErrProviderClosed     = errors.New("dilock: provider closed")
	ErrInvalidOptions     = errors.New("dilock: invalid options")
)

// OperationError carries a stable category suitable for errors.Is.
type OperationError struct {
	Kind    error
	Message string
	Cause   error
}

func (e *OperationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *OperationError) Unwrap() error        { return e.Cause }
func (e *OperationError) Is(target error) bool { return target == e.Kind }

// Options controls one acquisition generation. Durations are finite; the safety
// margin reduces the local validity budget but cannot protect against suspension.
type Options struct {
	LeaseDuration  time.Duration
	RPCTimeout     time.Duration
	CleanupTimeout time.Duration
	AutoRenew      bool
	RequireFencing bool
}

// Complete validates a fully specified acquisition lease policy and returns its wire TTL and safety margin.
func (o Options) Complete() (Options, int64, time.Duration, error) {
	if err := o.ValidateComplete(); err != nil {
		return Options{}, 0, 0, err
	}
	ttl, margin, err := o.validate()
	return o, ttl, margin, err
}

// DefaultOptions returns the draft 1 starting budgets.
func DefaultOptions() Options {
	return Options{LeaseDuration: 30 * time.Second, RPCTimeout: 2 * time.Second, CleanupTimeout: 2 * time.Second}
}

// Normalized fills zero-valued fields with defaults and validates the result.
func (o Options) Normalized() (Options, int64, time.Duration, error) {
	if o.LeaseDuration == 0 {
		o.LeaseDuration = 30 * time.Second
	}
	if o.RPCTimeout == 0 {
		o.RPCTimeout = 2 * time.Second
	}
	if o.CleanupTimeout == 0 {
		o.CleanupTimeout = 2 * time.Second
	}
	ttl, margin, err := o.validate()
	return o, ttl, margin, err
}

// ValidateComplete validates an explicit lease policy without filling omitted fields.
func (o Options) ValidateComplete() error {
	if o.LeaseDuration <= 0 || o.RPCTimeout <= 0 || o.CleanupTimeout <= 0 {
		return fmt.Errorf("%w: explicit policy must set lease, RPC and cleanup budgets", ErrInvalidOptions)
	}
	_, _, err := o.validate()
	return err
}

func (o Options) validate() (int64, time.Duration, error) {
	if o.LeaseDuration == 0 {
		o.LeaseDuration = 30 * time.Second
	}
	if o.RPCTimeout == 0 {
		o.RPCTimeout = 2 * time.Second
	}
	if o.CleanupTimeout == 0 {
		o.CleanupTimeout = 2 * time.Second
	}
	if o.LeaseDuration < 100*time.Millisecond || o.LeaseDuration > 24*time.Hour || o.RPCTimeout <= 0 || o.CleanupTimeout <= 0 || o.CleanupTimeout > 24*time.Hour {
		return 0, 0, fmt.Errorf("%w: invalid duration", ErrInvalidOptions)
	}
	ttl := int64(o.LeaseDuration / time.Millisecond)
	margin := time.Duration(math.Max(10, float64(ttl)*.01)) * time.Millisecond
	if o.RPCTimeout >= time.Duration(ttl)*time.Millisecond-margin {
		return 0, 0, fmt.Errorf("%w: RPC timeout must leave room for lease safety margin", ErrInvalidOptions)
	}
	return ttl, margin, nil
}

// Validate checks duration constraints before any backend request.
func (o Options) Validate() (int64, time.Duration, error) { return o.validate() }

// State is a local lease observation, never proof that a backend still recognizes the lease.
type State uint32

const (
	// Held is the local state while conservative validity remains.
	Held State = iota
	// Releasing means local reliance ended and cleanup is in progress.
	Releasing
	// Released means owner-checked cleanup was acknowledged.
	Released
	// ReleasedUnknown means cleanup had no reliable acknowledgement.
	ReleasedUnknown
	// Lost means local reliance ended without an acknowledged release.
	Lost
)

// ReleaseOutcome reports the owner-checked Redis cleanup acknowledgement.
type ReleaseOutcome uint8

const (
	// ReleaseUnknown means the cleanup acknowledgement is unknown.
	ReleaseUnknown ReleaseOutcome = iota
	// ReleasedOwner means Redis removed this lease's owner value.
	ReleasedOwner
	// AlreadyNotOwned means the backend confirmed nothing belonging to this
	// acquisition remains; an absent etcd lock key alone is not sufficient.
	AlreadyNotOwned
)

// ScopeReleaseError means a protected callback completed, but its scope did
// not confirm owner release. The callback's own error remains primary when it
// failed; this error is returned only after a successful callback.
type ScopeReleaseError struct {
	Outcome ReleaseOutcome
	Cause   error
}

func (e *ScopeReleaseError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("dilock: scope release outcome %d: %v", e.Outcome, e.Cause)
	}
	return fmt.Sprintf("dilock: scope release outcome %d", e.Outcome)
}

func (e *ScopeReleaseError) Unwrap() error {
	category := ErrReleaseUnknown
	if e.Outcome == AlreadyNotOwned {
		category = ErrLeaseLost
	}
	if e.Cause == nil {
		return category
	}
	return errors.Join(category, e.Cause)
}

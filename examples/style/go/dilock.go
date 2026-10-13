// Package style demonstrates local ownership and native Go lifetime patterns.
// It is not a distributed lock: no TTL, network, fencing or renewal is implemented.
package style

import (
	"context"
	"errors"
	"sync"
)

type ReleaseOutcome uint8

const (
	Released ReleaseOutcome = iota
	AlreadyNotOwned
)

// owner is intentionally nonzero-sized: distinct pointers identify acquisitions.
type owner struct{ marker byte }

// Store is a thread-safe in-process test fixture, not a backend client.
type Store struct {
	mu     sync.Mutex
	owners map[string]*owner
}

func NewStore() *Store { return &Store{owners: make(map[string]*owner)} }

// Provider borrows a local store. A real provider declares connection ownership.
type Provider struct{ store *Store }

func NewProvider(store *Store) (*Provider, error) {
	if store == nil {
		return nil, errors.New("nil store")
	}
	return &Provider{store: store}, nil
}

// ResourceLock is reusable; it is not ownership of its resource.
type ResourceLock struct {
	store    *Store
	resource string
}

func (p *Provider) Lock(resource string) (ResourceLock, error) {
	if resource == "" {
		return ResourceLock{}, errors.New("empty resource")
	}
	return ResourceLock{store: p.store, resource: resource}, nil
}

// Lease must not be copied after first use. Methods serialize its local state.
type Lease struct {
	mu       sync.Mutex
	store    *Store
	resource string
	owner    *owner
	closed   bool
}

// TryAcquire returns nil,nil only for ordinary contention.
func (l ResourceLock) TryAcquire(ctx context.Context) (*Lease, error) {
	if ctx == nil {
		return nil, errors.New("nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if l.store == nil {
		return nil, errors.New("uninitialized resource lock")
	}
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	if _, exists := l.store.owners[l.resource]; exists {
		return nil, nil
	}
	id := &owner{marker: 1}
	l.store.owners[l.resource] = id
	return &Lease{store: l.store, resource: l.resource, owner: id}, nil
}

// Release uses a cleanup context independently of the acquisition context.
func (l *Lease) Release(ctx context.Context) (ReleaseOutcome, error) {
	if ctx == nil {
		return AlreadyNotOwned, errors.New("nil context")
	}
	if err := ctx.Err(); err != nil {
		return AlreadyNotOwned, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return AlreadyNotOwned, nil
	}
	l.closed = true
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	if l.store.owners[l.resource] != l.owner {
		return AlreadyNotOwned, nil
	}
	delete(l.store.owners, l.resource)
	return Released, nil
}

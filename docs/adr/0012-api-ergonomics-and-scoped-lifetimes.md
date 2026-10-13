# ADR 0012: Idiomatic defaults and scoped lock lifetimes

## Status

Accepted for implementation; the seven-SDK rollout is in progress.

## Context

The seven native SDKs share a protocol but expose language-specific lifetimes.
Common use should need little configuration while production callers must be
able to set the entire supported acquisition and transport policy. Resource
handles should reuse one provider and its backend client. Scope exit is useful
for cleanup, but it cannot prove that an unacknowledged remote release happened.

## Decision

- Use a five-second acquisition wait and a thirty-second lease by default.
  Backend RPC and cleanup limits default to two seconds where those options
  exist. Automatic renewal and fencing requests remain explicit capabilities;
  defaults do not silently enable them.
- Keep provider, reusable resource handle, and acquired lease as distinct
  objects. Handles from one provider reuse its connection, channel, or client
  state; acquiring two different resource names does not require two providers.
- Offer language-native scoped lifetime paths (`await using`, move-only RAII,
  context managers, `defer`, `try` resources, or callback helpers) that forward
  to the existing canonical acquire/release operations.
- Preserve explicit release as the acknowledgement-sensitive API. Scope
  cleanup is bounded best effort. Unknown cleanup is observable through an
  optional callback/diagnostic hook, whose failures never escape cleanup.
- Keep old explicit-policy entry points where practical. Reject unsupported
  backend capabilities explicitly rather than implying parity.

## Consequences

Minimal examples can show one provider, one resource handle, and one scoped
lease. Advanced examples can set provider and per-acquisition options without
changing ownership semantics. Synchronous destructors may block up to their
documented cleanup budget; asynchronous SDKs must not reuse an already-cancelled
operation token for cleanup. Body errors remain primary when cleanup is also
uncertain. Lease expiration does not stop a paused process or guarantee safe
protected-resource effects.

This decision changes no wire protocol, key identity, owner token, namespace
metadata, or golden vector. Each SDK rollout still requires native builds,
backend lifecycle tests, exact package consumers, and compatibility validation.
